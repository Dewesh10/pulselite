import requests
import json
import time
import fastavro
import io
import struct
from datetime import datetime, timezone
from confluent_kafka import Producer
import signal
import sys


def shutdown_handler(sig, frame):
    print("\n⚡ Shutting down producer gracefully...")
    producer.flush()
    print("✅ All messages flushed. Goodbye.")
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

SOURCE = "Hacker News"
NEW_STORIES_URL = "https://hacker-news.firebaseio.com/v0/newstories.json"
ITEM_URL = "https://hacker-news.firebaseio.com/v0/item/{}.json"
KAFKA_TOPIC = "hn-posts"
KAFKA_SERVER = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
DEAD_LETTER_FILE = "dead_letters_producer.jsonl"

producer = Producer({"bootstrap.servers": KAFKA_SERVER})

# Tracks post IDs already sent this run, so we don't resend on every 30s poll
seen_ids = set()

# Load Avro schema
with open("schemas/post_v1.avsc", "r") as f:
    SCHEMA = fastavro.parse_schema(json.load(f))


def register_schema():
    """Register schema with Confluent Schema Registry."""
    with open("schemas/post_v1.avsc", "r") as f:
        schema_str = f.read()
    response = requests.post(
        f"{SCHEMA_REGISTRY_URL}/subjects/{KAFKA_TOPIC}-value/versions",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        json={"schema": schema_str},
    )
    if response.status_code in [200, 201]:
        schema_id = response.json()["id"]
        print(f"✅ Schema registered with ID: {schema_id}")
        return schema_id
    else:
        print(f"⚠️ Schema registry error: {response.text}")
        return 1


def serialize_avro(record, schema_id):
    """Serialize record to Confluent Avro format (magic byte + schema ID + avro bytes)."""
    buf = io.BytesIO()
    buf.write(b"\x00")  # magic byte
    buf.write(struct.pack(">I", schema_id))  # 4-byte schema ID
    fastavro.schemaless_writer(buf, SCHEMA, record)
    return buf.getvalue()


def fetch_with_retry(url, retries=3, backoff=2):
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if attempt < retries - 1:
                wait = backoff**attempt
                print(f"⚠️ Request failed ({e}), retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"❌ Failed after {retries} attempts: {e}")
                return None


def fetch_posts(limit=25):
    story_ids = fetch_with_retry(NEW_STORIES_URL)
    if not story_ids:
        return []
    posts = []
    for sid in story_ids[:limit]:
        if sid in seen_ids:
            continue  # already sent, skip
        item = fetch_with_retry(ITEM_URL.format(sid))
        if item and "title" in item:
            posts.append(item)
    return posts


def produce_with_retry(topic, value, key, retries=3, backoff=2):
    """Produce to Kafka with retry. Returns True on success, False if it never succeeded."""
    for attempt in range(retries):
        try:
            producer.produce(topic, value=value, key=key)
            producer.poll(0)
            return True
        except BufferError:
            # local queue full, give it a moment and retry
            wait = backoff**attempt
            print(f"⚠️ Kafka local queue full, retrying in {wait}s...")
            time.sleep(wait)
        except Exception as e:
            print(f"⚠️ Kafka produce failed ({e}), attempt {attempt + 1}/{retries}")
            time.sleep(backoff**attempt)
    return False


def dead_letter(post, reason):
    """Log a message that couldn't be processed/sent, instead of silently dropping it."""
    entry = {
        "id": post.get("id"),
        "title": post.get("title", ""),
        "reason": reason,
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(DEAD_LETTER_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")
    print(f"  ⚠️ Dead-lettered post {post.get('id')}: {reason}")


def main():
    print(f"PulseLite producer started — watching {SOURCE}")
    print(f"Sending Avro messages to Kafka topic: {KAFKA_TOPIC}")
    print("-" * 50)

    # Wait for Schema Registry to be ready
    print("Waiting for Schema Registry...")
    for _ in range(10):
        try:
            r = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=3)
            if r.status_code == 200:
                print("✅ Schema Registry is ready")
                break
        except Exception:
            pass
        time.sleep(3)

    schema_id = register_schema()

    while True:
        print(
            f"\n[{datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC] Fetching posts..."
        )
        posts = fetch_posts()
        print(f"Fetched {len(posts)} new posts — sending as Avro...")

        for post in posts:
            try:
                now_utc = datetime.now(timezone.utc).isoformat()
                record = {
                    "id": post.get("id"),
                    "title": post.get("title", ""),
                    "score": post.get("score", 0),
                    "comments": post.get("descendants", 0),
                    "timestamp": now_utc,
                    "ingested_at": now_utc,
                }
                avro_bytes = serialize_avro(record, schema_id)
            except Exception as e:
                dead_letter(post, f"serialization error: {e}")
                continue

            success = produce_with_retry(
                KAFKA_TOPIC, value=avro_bytes, key=str(post["id"])
            )
            if success:
                seen_ids.add(post["id"])
                print(f"  ✓ Sent (Avro): {post['title'][:60]}")
            else:
                dead_letter(post, "kafka produce failed after retries")

        producer.flush()
        print("Batch done. Waiting 30 seconds...")
        time.sleep(30)


if __name__ == "__main__":
    main()
