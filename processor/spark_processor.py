import json
import csv
import os
import struct
import io
import fastavro
import numpy as np
from confluent_kafka import Consumer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from sentence_transformers import SentenceTransformer
from datetime import datetime, timezone
import signal
import sys


KAFKA_TOPIC = "hn-posts"
KAFKA_SERVER = "localhost:9092"
CSV_POSTS = "data_posts.csv"
CSV_VOLUME = "data_volume.csv"
CSV_ALERTS = "data_alerts.csv"
DEAD_LETTER_FILE = "dead_letters_processor.jsonl"

analyzer = SentimentIntensityAnalyzer()
embedder = SentenceTransformer("all-MiniLM-L6-v2")
with open("schemas/post_v1.avsc", "r") as f:
    SCHEMA = fastavro.parse_schema(json.load(f))


def deserialize_avro(raw_bytes):
    try:
        buf = io.BytesIO(raw_bytes)
        magic = buf.read(1)
        if magic != b'\x00':
            return json.loads(raw_bytes.decode("utf-8"))
        struct.unpack('>I', buf.read(4))[0]
        record = fastavro.schemaless_reader(buf, SCHEMA)
        return record
    except Exception:
        return json.loads(raw_bytes.decode("utf-8"))


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
    if not os.path.exists("data_drift.csv"):
        with open("data_drift.csv", "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["timestamp", "drift_score", "sample_title", "before_titles", "after_titles"])


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
        now = datetime.now(timezone.utc).isoformat()
        with open(CSV_ALERTS, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([now, current_count, round(avg, 2)])
        print(f"🚨 ANOMALY DETECTED! Count: {current_count}, Avg: {avg:.1f}")


def compute_drift(current_window_titles, previous_window_titles, sample_title):
    if len(current_window_titles) < 3 or len(previous_window_titles) < 3:
        return

    current_embeddings = embedder.encode(current_window_titles)
    previous_embeddings = embedder.encode(previous_window_titles)

    current_centroid = np.mean(current_embeddings, axis=0)
    previous_centroid = np.mean(previous_embeddings, axis=0)

    similarity = np.dot(current_centroid, previous_centroid) / (
        np.linalg.norm(current_centroid) * np.linalg.norm(previous_centroid) + 1e-10
    )
    drift_score = round(float(1 - similarity), 4)

    before_titles = " | ".join(previous_window_titles[:3])
    after_titles = " | ".join(current_window_titles[:3])

    now = datetime.now(timezone.utc).isoformat()
    with open("data_drift.csv", "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([now, drift_score, sample_title[:80], before_titles[:200], after_titles[:200]])

    if drift_score > 0.3:
        print(f"🌊 TOPIC DRIFT DETECTED! Score: {drift_score:.3f}")


def dead_letter(raw_value, reason):
    entry = {
        "reason": reason,
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "raw_preview": str(raw_value)[:200]
    }
    with open(DEAD_LETTER_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")
    print(f"  ⚠️ Dead-lettered message: {reason}")


def main():
    print("PulseLite processor started")
    print(f"Reading from Kafka topic: {KAFKA_TOPIC}")
    print("-" * 50)

    init_csvs()

    consumer = Consumer({
        "bootstrap.servers": KAFKA_SERVER,
        "group.id": "pulselite-processor",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False  # we commit manually, only after processing succeeds
    })
    consumer.subscribe([KAFKA_TOPIC])

    def shutdown_handler(sig, frame):
        print("\n⚡ Shutting down processor gracefully...")
        consumer.close()
        print("✅ Kafka consumer closed. Offsets committed. Goodbye.")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    minute_bucket = {}
    minute_titles = {}

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"⚠️ Kafka error: {msg.error()}")
            continue

        try:
            post = deserialize_avro(msg.value())
            title = post.get("title", "")
            post_id = post.get("id")

            if post_id is None or not title:
                dead_letter(msg.value(), "missing id or title after deserialization")
                consumer.commit(msg)
                continue

            score, label = get_sentiment(title)

            save_post(post_id, title, post.get("score", 0), post.get("comments", 0), score, label, post.get("timestamp"))

            minute = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
            minute_bucket[minute] = minute_bucket.get(minute, 0) + 1
            count = minute_bucket[minute]

            update_volume(minute_bucket, minute)
            check_anomaly(minute_bucket, minute, count)

            if minute not in minute_titles:
                minute_titles[minute] = []
            minute_titles[minute].append(title)

            all_minutes = sorted(minute_titles.keys())
            if len(all_minutes) >= 2:
                current_min = all_minutes[-1]
                previous_min = all_minutes[-2]
                compute_drift(
                    minute_titles[current_min],
                    minute_titles[previous_min],
                    title
                )

            print(f"  [{label.upper()}] {title[:55]} (score: {score:.2f})")

            # Only commit the offset once we've fully processed this message.
            # If the process crashes before this line, this message gets
            # reprocessed on restart instead of being silently lost.
            consumer.commit(msg)

        except Exception as e:
            dead_letter(msg.value(), f"processing error: {e}")
            consumer.commit(msg)  # don't get stuck retrying a permanently broken message
            continue


if __name__ == "__main__":
    main()