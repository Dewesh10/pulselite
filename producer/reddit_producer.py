import requests
import json
import time
import fastavro
import io
import struct
from datetime import datetime
from confluent_kafka import Producer

SOURCE = "Hacker News"
NEW_STORIES_URL = "https://hacker-news.firebaseio.com/v0/newstories.json"
ITEM_URL = "https://hacker-news.firebaseio.com/v0/item/{}.json"
KAFKA_TOPIC = "hn-posts"
KAFKA_SERVER = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

producer = Producer({"bootstrap.servers": KAFKA_SERVER})

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
        json={"schema": schema_str}
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
    buf.write(b'\x00')  # magic byte
    buf.write(struct.pack('>I', schema_id))  # 4-byte schema ID
    fastavro.schemaless_writer(buf, SCHEMA, record)
    return buf.getvalue()


def fetch_posts(limit=25):
    story_ids = requests.get(NEW_STORIES_URL).json()[:limit]
    posts = []
    for sid in story_ids:
        item = requests.get(ITEM_URL.format(sid)).json()
        if item and "title" in item:
            posts.append(item)
    return posts


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
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Fetching posts...")
        posts = fetch_posts()
        print(f"Fetched {len(posts)} posts — sending as Avro...")

        for post in posts:
            record = {
                "id": post.get("id"),
                "title": post.get("title", ""),
                "score": post.get("score", 0),
                "comments": post.get("descendants", 0),
                "timestamp": datetime.now().isoformat(),
                "ingested_at": datetime.utcnow().isoformat()
            }
            avro_bytes = serialize_avro(record, schema_id)
            producer.produce(KAFKA_TOPIC, value=avro_bytes)
            print(f"  ✓ Sent (Avro): {post['title'][:60]}")

        producer.flush()
        print(f"All posts sent. Waiting 30 seconds...")
        time.sleep(30)


if __name__ == "__main__":
    main()