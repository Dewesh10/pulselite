"""
PulseLite Demo Data Generator
Generates realistic historical data for demo/presentation mode.
Run this once to create demo CSV files that the dashboard can display
without needing the live pipeline running.
"""

import csv
import os
import random
import math
from datetime import datetime, timedelta

DEMO_TITLES = [
    "Show HN: I built a real-time Kafka pipeline for HN data",
    "Ask HN: What's the best way to learn data engineering?",
    "OpenAI releases GPT-5 with 10x better reasoning",
    "Rust is now the most loved language for 8th year in a row",
    "Show HN: PulseLite - Real-time HN Intelligence Dashboard",
    "DeepMind achieves breakthrough in protein structure prediction",
    "The death of the junior developer",
    "Why I left FAANG after 5 years",
    "Show HN: I built a compiler in a weekend",
    "Ask HN: How do you handle burnout as a developer?",
    "Bitcoin hits all time high as institutional adoption grows",
    "Anthropic releases Claude with extended context window",
    "The hidden costs of microservices architecture",
    "How I reduced our AWS bill by 80 percent",
    "Show HN: Open source alternative to Datadog",
    "Kafka vs RabbitMQ in 2026 - which should you choose?",
    "Why Python is still the king of data engineering",
    "The rise of streaming databases",
    "Show HN: I trained a model on HN comments",
    "Ask HN: Best resources for learning Kafka?",
    "Google announces new AI coding assistant",
    "The future of remote work in tech",
    "How Netflix handles 1 billion events per day",
    "Show HN: My open source Streamlit dashboard template",
    "The problem with modern JavaScript frameworks",
]

SENTIMENTS = [
    ("positive", 0.6),
    ("positive", 0.45),
    ("positive", 0.32),
    ("neutral", 0.0),
    ("neutral", 0.02),
    ("neutral", -0.01),
    ("negative", -0.28),
    ("negative", -0.45),
    ("negative", -0.61),
]


def generate_demo_data(hours_back=3):
    """Generate realistic demo data for the last N hours."""
    now = datetime.now()
    start_time = now - timedelta(hours=hours_back)

    os.makedirs("demo_data", exist_ok=True)

    # Generate posts
    posts = []
    post_id = 48000000
    current_time = start_time

    while current_time < now:
        # Vary volume — simulate spikes
        minute_of_hour = current_time.minute
        base_volume = 3
        if 10 <= minute_of_hour <= 20:
            base_volume = 15  # spike
        elif 35 <= minute_of_hour <= 45:
            base_volume = 12  # another spike
        volume = base_volume + random.randint(-1, 2)

        for _ in range(volume):
            title = random.choice(DEMO_TITLES)
            sentiment_label, sentiment_score = random.choice(SENTIMENTS)
            posts.append({
                "id": post_id,
                "title": title,
                "score": random.randint(1, 300),
                "comments": random.randint(0, 150),
                "sentiment": sentiment_score + random.uniform(-0.05, 0.05),
                "sentiment_label": sentiment_label,
                "timestamp": current_time.isoformat()
            })
            post_id += 1

        current_time += timedelta(minutes=1)

    # Write posts CSV
    with open("demo_data/data_posts.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "title", "score", "comments", "sentiment", "sentiment_label", "timestamp"])
        writer.writeheader()
        writer.writerows(posts)

    print(f"✅ Generated {len(posts)} demo posts")

    # Generate volume per minute
    volume_by_minute = {}
    for post in posts:
        minute = post["timestamp"][:16]
        volume_by_minute[minute] = volume_by_minute.get(minute, 0) + 1

    with open("demo_data/data_volume.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["minute", "post_count"])
        for minute, count in sorted(volume_by_minute.items()):
            writer.writerow([minute, count])

    print(f"✅ Generated {len(volume_by_minute)} volume buckets")

    # Generate some anomaly alerts
    with open("demo_data/data_alerts.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "post_count", "rolling_avg"])
        spike_time = start_time + timedelta(minutes=15)
        writer.writerow([spike_time.isoformat(), 18, 4.2])
        spike_time2 = start_time + timedelta(minutes=95)
        writer.writerow([spike_time2.isoformat(), 15, 3.8])

    print("✅ Generated anomaly alerts")

    # Generate drift data
    with open("demo_data/data_drift.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "drift_score", "sample_title", "before_titles", "after_titles"])
        drift_time = start_time
        while drift_time < now:
            score = 0.1 + 0.3 * abs(math.sin(drift_time.minute / 10))
            score += random.uniform(-0.05, 0.05)
            before = "OpenAI releases GPT-5 | DeepMind breakthrough | AI coding assistant"
            after = "Bitcoin hits ATH | Crypto winter | DeFi protocol hacked"
            writer.writerow([drift_time.isoformat(), round(score, 4), random.choice(DEMO_TITLES)[:80], before, after])
            drift_time += timedelta(minutes=2)

    print("✅ Generated drift scores")

    # Generate correlation data
    with open("demo_data/data_correlation.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["window_start", "window_end", "new_volume", "top_volume", "correlation"])
        corr_time = start_time
        while corr_time < now - timedelta(minutes=12):
            new_vol = random.randint(30, 80)
            top_vol = random.randint(20, 60)
            corr = random.uniform(-0.4, 0.8)
            writer.writerow([
                corr_time.isoformat(),
                (corr_time + timedelta(minutes=12)).isoformat(),
                new_vol, top_vol, round(corr, 4)
            ])
            corr_time += timedelta(minutes=5)

    print("✅ Generated correlation data")
    print("\n🎉 Demo data ready in demo_data/ folder!")
    print("Set DEMO_MODE=true in your environment to use it.")


if __name__ == "__main__":
    generate_demo_data(hours_back=3)