import json
import duckdb
from confluent_kafka import Consumer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime
from collections import Counter
import re

KAFKA_TOPIC = "hn-posts"
KAFKA_SERVER = "localhost:9092"
DB_PATH = "pulselite.db"

# Setup
analyzer = SentimentIntensityAnalyzer()
con = duckdb.connect(DB_PATH)

# Create tables
con.execute("""
    CREATE TABLE IF NOT EXISTS posts (
        id INTEGER,
        title TEXT,
        score INTEGER,
        comments INTEGER,
        sentiment FLOAT,
        sentiment_label TEXT,
        timestamp TEXT
    )
""")

con.execute("""
    CREATE TABLE IF NOT EXISTS volume_per_minute (
        minute TEXT,
        post_count INTEGER
    )
""")

con.execute("""
    CREATE TABLE IF NOT EXISTS anomaly_alerts (
        timestamp TEXT,
        post_count INTEGER,
        rolling_avg FLOAT
    )
""")

def get_sentiment(text):
    score = analyzer.polarity_scores(text)["compound"]
    if score >= 0.05:
        return score, "positive"
    elif score <= -0.05:
        return score, "negative"
    else:
        return score, "neutral"

def check_anomaly(current_count):
    rows = con.execute("""
        SELECT post_count FROM volume_per_minute
        ORDER BY minute DESC LIMIT 5
    """).fetchall()
    
    if len(rows) < 3:
        return
    
    avg = sum(r[0] for r in rows) / len(rows)
    if current_count > 3 * avg:
        now = datetime.now().isoformat()
        con.execute(
            "INSERT INTO anomaly_alerts VALUES (?, ?, ?)",
            [now, current_count, avg]
        )
        print(f"🚨 ANOMALY DETECTED! Count: {current_count}, Avg: {avg:.1f}")

def main():
    print("PulseLite processor started")
    print(f"Reading from Kafka topic: {KAFKA_TOPIC}")
    print("-" * 50)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_SERVER,
        "group.id": "pulselite-processor",
        "auto.offset.reset": "earliest"
    })
    consumer.subscribe([KAFKA_TOPIC])

    minute_bucket = {}

    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        post = json.loads(msg.value().decode("utf-8"))
        title = post.get("title", "")
        score, label = get_sentiment(title)

        # Save post
        con.execute(
            "INSERT INTO posts VALUES (?, ?, ?, ?, ?, ?, ?)",
            [post.get("id"), title, post.get("score", 0),
             post.get("comments", 0), score, label, post.get("timestamp")]
        )

        # Volume per minute
        minute = datetime.now().strftime("%Y-%m-%d %H:%M")
        minute_bucket[minute] = minute_bucket.get(minute, 0) + 1
        count = minute_bucket[minute]

        con.execute("DELETE FROM volume_per_minute WHERE minute = ?", [minute])
        con.execute("INSERT INTO volume_per_minute VALUES (?, ?)", [minute, count])

        check_anomaly(count)

        print(f"  [{label.upper()}] {title[:55]} (score: {score:.2f})")

if __name__ == "__main__":
    main()