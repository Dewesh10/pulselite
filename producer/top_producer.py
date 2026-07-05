import requests
import json
import time
import fastavro
import io
import csv
import os
import struct
from datetime import datetime
from confluent_kafka import Producer

SOURCE = "Hacker News Top"
TOP_STORIES_URL = "https://hacker-news.firebaseio.com/v0/topstories.json"
ITEM_URL = "https://hacker-news.firebaseio.com/v0/item/{}.json"
KAFKA_TOPIC = "hn-top"
KAFKA_SERVER = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

producer = Producer({"bootstrap.servers": KAFKA_SERVER})

with open("schemas/post_v1.avsc", "r") as f:
    SCHEMA = fastavro.parse_schema(json.load(f))


def register_schema():
    with open("schemas/post_v1.avsc", "r") as f:
        schema_str = f.read()
    response = requests.post(
        f"{SCHEMA_REGISTRY_URL}/subjects/{KAFKA_TOPIC}-value/versions",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        json={"schema": schema_str}
    )
    schema_id = response.json().get("id", 1)
    print(f"✅ Schema registered for {KAFKA_TOPIC} with ID: {schema_id}")
    return schema_id


def serialize_avro(record, schema_id):
    buf = io.BytesIO()
    buf.write(b'\x00')
    buf.write(struct.pack('>I', schema_id))
    fastavro.schemaless_writer(buf, SCHEMA, record)
    return buf.getvalue()

CSV_TOP = "data_top.csv"

def init_top_csv():
    if not os.path.exists(CSV_TOP):
        with open(CSV_TOP, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["id", "title", "score", "comments", "timestamp"])

def save_top_post(post_id, title, score, comments, timestamp):
    with open(CSV_TOP, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([post_id, title, score, comments, timestamp])

def fetch_top_posts(limit=25):
    story_ids = requests.get(TOP_STORIES_URL).json()[:limit]
    posts = []
    for sid in story_ids:
        item = requests.get(ITEM_URL.format(sid)).json()
        if item and "title" in item:
            posts.append(item)
    return posts


def main():
    print(f"PulseLite TOP producer started — watching {SOURCE}")
    print(f"Sending to Kafka topic: {KAFKA_TOPIC}")
    print("-" * 50)

    time.sleep(5)
    schema_id = register_schema()
    init_top_csv()

    while True:
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Fetching top posts...")
        posts = fetch_top_posts()
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
            print(f"  ✓ Sent (Top): {post['title'][:60]}")
            save_top_post(post.get("id"), post.get("title",""), post.get("score",0), post.get("descendants",0), datetime.now().isoformat())

        producer.flush()
        print(f"All top posts sent. Waiting 30 seconds...")
        time.sleep(30)


if __name__ == "__main__":
    main()