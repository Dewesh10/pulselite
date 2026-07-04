import json
import csv
import os
import duckdb
from confluent_kafka import Consumer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime

KAFKA_TOPIC = "hn-posts"
KAFKA_SERVER = "localhost:9092"
DB_PATH = "pulselite.db"
CSV_POSTS = "data_posts.csv"
CSV_VOLUME = "data_volume.csv"
CSV_ALERTS = "data_alerts.csv"

analyzer = SentimentIntensityAnalyzer()
seen_ids = set()


def get_sentiment(text):
    score = analyzer.polarity_scores(text)["compound"]
    if score >= 0.05:
        return score, "positive"
    elif score <= -0.05:
        return score, "negative"
    else:
        return score, "neutral"


def init_csvs():
    if not os.path.exists(CSV_POSTS):
        with open(CSV_POSTS, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["id", "title", "score", "comments", "sentiment", "sentiment_label", "timestamp"])
    if not os.path.exists(CSV_VOLUME):
        with open(CSV_VOLUME, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["minute", "post_count"])
    if not os.path.exists(CSV_ALERTS):
        with open(CSV_ALERTS, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["timestamp", "post_count", "rolling_avg"])


def save_post(post_id, title, hn_score, comments, sentiment, label, timestamp):
    if post_id in seen_ids:
        return
    seen_ids.add(post_id)
    with open(CSV_POSTS, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([post_id, title, hn_score, comments, sentiment, label, timestamp])


def update_volume(minute_bucket, minute):
    rows = []
    updated = False
    if os.path.exists(CSV_VOLUME):
        with open(CSV_VOLUME, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            for row in reader:
                if row and row[0] == minute:
                    rows.append([minute, minute_bucket[minute]])
                    updated = True
                else:
                    rows.append(row)
    if not updated:
        rows.append([minute, minute_bucket[minute]])
    with open(CSV_VOLUME, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["minute", "post_count"])
        writer.writerows(rows)


def check_anomaly(minute_bucket, current_minute, current_count):
    counts = []
    if os.path.exists(CSV_VOLUME):
        with open(CSV_VOLUME, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                if row and row[0] != current_minute:
                    try:
                        counts.append(int(row[1]))
                    except:
                        pass
    if len(counts) < 3:
        return
    avg = sum(counts[-5:]) / len(counts[-5:])
    if current_count > 3 * avg:
        now = datetime.now().isoformat()
        with open(CSV_ALERTS, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([now, current_count, round(avg, 2)])
        print(f"🚨 ANOMALY DETECTED! Count: {current_count}, Avg: {avg:.1f}")


def main():
    print("PulseLite processor started")
    print(f"Reading from Kafka topic: {KAFKA_TOPIC}")
    print("-" * 50)

    init_csvs()

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
        post_id = post.get("id")
        score, label = get_sentiment(title)

        save_post(post_id, title, post.get("score", 0), post.get("comments", 0), score, label, post.get("timestamp"))

        minute = datetime.now().strftime("%Y-%m-%d %H:%M")
        minute_bucket[minute] = minute_bucket.get(minute, 0) + 1
        count = minute_bucket[minute]

        update_volume(minute_bucket, minute)
        check_anomaly(minute_bucket, minute, count)

        print(f"  [{label.upper()}] {title[:55]} (score: {score:.2f})")


if __name__ == "__main__":
    main()