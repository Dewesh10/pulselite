import requests
import json
import time
from datetime import datetime
from kafka import KafkaProducer

SOURCE = "Hacker News"
NEW_STORIES_URL = "https://hacker-news.firebaseio.com/v0/newstories.json"
ITEM_URL = "https://hacker-news.firebaseio.com/v0/item/{}.json"
KAFKA_TOPIC = "hn-posts"
KAFKA_SERVER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

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
    print(f"Sending to Kafka topic: {KAFKA_TOPIC}")
    print("-" * 50)

    while True:
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Fetching posts...")
        posts = fetch_posts()
        print(f"Fetched {len(posts)} posts — sending to Kafka...")

        for post in posts:
            message = {
                "id": post.get("id"),
                "title": post.get("title"),
                "score": post.get("score", 0),
                "comments": post.get("descendants", 0),
                "timestamp": datetime.now().isoformat()
            }
            producer.send(KAFKA_TOPIC, value=message)
            print(f"  ✓ Sent: {post['title'][:60]}")

        producer.flush()
        print(f"\nAll posts sent to Kafka. Waiting 30 seconds...")
        time.sleep(30)

if __name__ == "__main__":
    main()