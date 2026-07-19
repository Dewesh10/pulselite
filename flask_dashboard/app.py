import sys
import os
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from flask import Flask, render_template, Response
from backend.components import (
    kpi_card,
    post_card,
    leaderboard_row,
    alert_card,
    pipeline_card,
    status_badge,
    digest_card,
)
from backend.data_loader import (
    load_posts,
    load_volume,
    load_alerts,
    load_sentiment_summary,
    load_table_counts,
    load_null_quality,
    load_digest,
    load_drift,
    load_correlation,
)
from backend.analytics import (
    current_velocity,
    sentiment_breakdown,
    pulse_score,
    pulse_label,
    momentum_pct,
    top_engaging_posts,
    rolling_sentiment,
    hourly_activity,
    pipeline_status,
    zscore_anomalies,
    top_terms,
    average_engagement,
)

app = Flask(__name__)

APP_NAME = "PulseLite"
APP_TAGLINE = "Real-Time Hacker News Intelligence"
APP_ICON = "🔴"
SOURCE_LABEL = "Hacker News"
PIPELINE_STAGES = [
    "Hacker News API",
    "Kafka",
    "Stream Processor (VADER)",
    "CSV Store",
    "PulseLite",
]

ARCHITECTURE_DIAGRAM = """Hacker News API
|  (poll new + top stories)
v
producer/reddit_producer.py  ·  producer/top_producer.py
|  (Avro-encoded, Confluent Schema Registry)
v
Kafka topics: hn-posts  ·  hn-top
|
v
processor/spark_processor.py  +  processor/stream_join.py
|  (VADER sentiment · windowed volume · anomaly detection · stream-stream join)
v
CSV store: data_posts.csv  ·  data_volume.csv  ·  data_alerts.csv
|
v
PulseLite Dashboard (you are here)"""


def _direction(val):
    if val is None:
        return "flat"
    return "up" if val > 0 else ("down" if val < 0 else "flat")


def _check_schema_registry():
    try:
        r = requests.get("http://localhost:8081/subjects", timeout=2)
        if r.status_code != 200:
            return None
        subjects = r.json()
        versions = None
        latest_id = None
        try:
            v = requests.get(
                "http://localhost:8081/subjects/hn-posts-value/versions", timeout=2
            ).json()
            versions = len(v)
            latest = requests.get(
                f"http://localhost:8081/subjects/hn-posts-value/versions/{v[-1]}",
                timeout=2,
            ).json()
            latest_id = latest.get("id")
        except Exception:
            pass
        return {
            "connected": True,
            "subjects": subjects,
            "versions": versions,
            "latest_id": latest_id,
        }
    except Exception:
        return {"connected": False}


@app.route("/")
def home():
    posts = load_posts()
    volume = load_volume(limit_minutes=90)
    alerts = load_alerts()
    sentiment_df = load_sentiment_summary()
    table_counts = load_table_counts()
    quality = load_null_quality()
    digest = load_digest()

    # ---- Core derived analytics ----
    velocity = current_velocity(volume)
    breakdown = sentiment_breakdown(sentiment_df)
    momentum = momentum_pct(volume)
    zscore_flags = zscore_anomalies(volume)
    score = pulse_score(velocity, breakdown.index, len(alerts), momentum)
    score_label, score_color = pulse_label(score)
    status = pipeline_status(posts)
    avg_engagement = average_engagement(posts)
    status_badge_html = status_badge(status.state, status.color_key)

    digest_html = None
    if digest:
        digest_html = digest_card(
            digest["text"], digest["mode"], digest["generated_at"]
        )

    momentum_text = (
        f"{momentum:+.1f}% vs prior window" if momentum is not None else "warming up..."
    )
    kpi_cards_html = [
        kpi_card("📊", "Total Posts Tracked", f"{table_counts.get('posts', 0):,}"),
        kpi_card(
            "⚡", "Velocity", f"{velocity:g}/min", momentum_text, _direction(momentum)
        ),
        kpi_card(
            "💓",
            "Pulse Score",
            f"{score}/100",
            score_label,
            score_color if score_color != "warning" else "flat",
        ),
        kpi_card(
            "🎭",
            "Sentiment Index",
            f"{breakdown.index:+.0f}",
            f"{breakdown.positive_pct}% positive",
            "up" if breakdown.index > 0 else "down" if breakdown.index < 0 else "flat",
        ),
        kpi_card(
            "🚨",
            "Active Anomalies",
            f"{len(alerts)}",
            f"{len(zscore_flags)} statistical flags",
            "down" if len(alerts) else "flat",
        ),
        kpi_card(
            "🔥", "Avg Engagement", f"{avg_engagement:g}", "score + comments", "flat"
        ),
    ]

    # ---- Overview tab charts ----
    volume_chart_data = {
        "x": (
            volume["minute_dt"].dt.strftime("%Y-%m-%d %H:%M").tolist()
            if not volume.empty
            else []
        ),
        "y": volume["post_count"].tolist() if not volume.empty else [],
        "rolling": (
            volume["post_count"]
            .rolling(window=min(5, max(len(volume), 1)), min_periods=1)
            .mean()
            .tolist()
            if not volume.empty
            else []
        ),
        "anomaly_x": (
            zscore_flags["minute_dt"].dt.strftime("%Y-%m-%d %H:%M").tolist()
            if not zscore_flags.empty
            else []
        ),
        "anomaly_y": (
            zscore_flags["post_count"].tolist() if not zscore_flags.empty else []
        ),
    }
    donut_data = {
        "positive": breakdown.positive,
        "negative": breakdown.negative,
        "neutral": breakdown.neutral,
        "index": breakdown.index,
    }

    terms = top_terms(posts["title"], top_n=12) if not posts.empty else []
    trending_terms_data = {
        "words": [t[0] for t in terms][::-1],
        "counts": [t[1] for t in terms][::-1],
    }

    pulse_gauge_data = {"score": score}

    # ---- Feed ----
    feed_posts = posts.sort_values("timestamp_dt", ascending=False).head(400)
    feed_items = [
        {
            "html": post_card(
                title=row["title"],
                score=row["score"],
                comments=row["comments"],
                sentiment=row["sentiment"],
                label=row["sentiment_label"],
                timestamp=row["timestamp"],
                post_id=row.get("id"),
            ),
            "title": str(row["title"]).lower(),
            "sentiment": row["sentiment_label"],
            "score": int(row["score"]),
            "comments": int(row["comments"]),
            "timestamp": str(row["timestamp"]),
        }
        for _, row in feed_posts.iterrows()
    ]
    feed_total = len(feed_items)

    # ---- Trends ----
    leaders = top_engaging_posts(posts, n=6)
    leaderboard_html = [
        leaderboard_row(i + 1, row.title, row.score, row.comments, row.sentiment_label)
        for i, row in enumerate(leaders.itertuples())
    ]

    roll = rolling_sentiment(posts)
    rolling_chart_data = {
        "timestamps": (
            roll["timestamp_dt"].dt.strftime("%Y-%m-%d %H:%M").tolist()
            if not roll.empty
            else []
        ),
        "sentiment": roll["sentiment"].tolist() if not roll.empty else [],
        "rolling": roll["rolling"].tolist() if not roll.empty else [],
    }

    scatter_chart_data = {
        "positive": {"x": [], "y": [], "text": []},
        "negative": {"x": [], "y": [], "text": []},
        "neutral": {"x": [], "y": [], "text": []},
    }
    for _, row in posts.iterrows():
        label = (
            row["sentiment_label"]
            if row["sentiment_label"] in scatter_chart_data
            else "neutral"
        )
        scatter_chart_data[label]["x"].append(float(row["score"]))
        scatter_chart_data[label]["y"].append(float(row["comments"]))
        scatter_chart_data[label]["text"].append(str(row["title"]))

    heat = hourly_activity(posts)
    heatmap_data = {
        "z": heat.values.tolist() if not heat.empty else [],
        "x": [f"{h:02d}:00" for h in heat.columns] if not heat.empty else [],
        "y": list(heat.index) if not heat.empty else [],
    }

    # ---- Drift (lives on Trends tab, matching original) ----
    drift_df = load_drift()
    drift_available = len(drift_df) > 1
    drift_chart_data = {"x": [], "y": []}
    drift_gauge_data = {"score": 0}
    drift_status = "STABLE"
    drift_before_after = []
    if drift_available:
        drift_chart_data = {
            "x": drift_df["ts"].dt.strftime("%Y-%m-%d %H:%M").tolist(),
            "y": drift_df["drift_score"].tolist(),
        }
        latest_drift = float(drift_df["drift_score"].iloc[-1])
        drift_gauge_data = {"score": round(latest_drift, 3)}
        drift_status = "DRIFTING" if latest_drift > 0.3 else "STABLE"
        if "before_titles" in drift_df.columns:
            high = drift_df[drift_df["drift_score"] > 0.3].tail(3)
            drift_before_after = [
                {
                    "ts": str(r["timestamp"])[:19],
                    "score": f"{r['drift_score']:.3f}",
                    "before": str(r.get("before_titles", ""))[:150],
                    "after": str(r.get("after_titles", ""))[:150],
                }
                for _, r in high.iterrows()
            ]

    # ---- Anomalies ----
    alerts_html = (
        [
            alert_card(row.timestamp, row.post_count, row.rolling_avg)
            for row in alerts.itertuples()
        ]
        if not alerts.empty
        else []
    )
    z_df = zscore_anomalies(volume, threshold=-999)
    zscore_chart_data = {
        "x": (
            z_df["minute_dt"].dt.strftime("%Y-%m-%d %H:%M").tolist()
            if not z_df.empty
            else []
        ),
        "y": z_df["zscore"].tolist() if not z_df.empty else [],
    }

    # ---- Correlation (rendered below all tabs, matching original quirk) ----
    corr_df = load_correlation()
    corr_available = len(corr_df) > 1
    corr_chart_data = {"x": [], "new_volume": [], "top_volume": []}
    corr_gauge_data = {"value": 0}
    corr_label = "Weak/No correlation"
    if corr_available:
        corr_chart_data = {
            "x": list(range(len(corr_df))),
            "new_volume": corr_df["new_volume"].tolist(),
            "top_volume": corr_df["top_volume"].tolist(),
        }
        latest_corr = float(corr_df["correlation"].iloc[-1])
        corr_gauge_data = {"value": round(latest_corr, 3)}
        corr_label = (
            "Strong positive"
            if latest_corr > 0.7
            else "Moderate" if latest_corr > 0.3 else "Weak/No correlation"
        )

    # ---- Pipeline ----
    has_data = table_counts.get("posts", 0) > 0
    stage_status = (
        "Streaming"
        if status.state == "LIVE"
        else ("Delayed" if status.state == "IDLE" else "Check process")
    )
    pipeline_cards_html = [
        pipeline_card("📡", "Hacker News API", "Source reachable"),
        pipeline_card("📨", "Kafka Topic: hn-posts", stage_status),
        pipeline_card("🧠", "VADER Processor", stage_status),
        pipeline_card("🗄️", "CSV Store", "Connected" if has_data else "Empty"),
    ]
    quality_cards_html = [
        kpi_card("🧪", "Total rows", str(quality.get("total", 0))),
        kpi_card("📝", "Missing titles", str(quality.get("missing_title", 0))),
        kpi_card("😊", "Missing sentiment", str(quality.get("missing_sentiment", 0))),
        kpi_card("⬆", "Missing score", str(quality.get("missing_score", 0))),
    ]
    schema_registry = _check_schema_registry()

    return render_template(
        "index.html",
        app_name=APP_NAME,
        app_tagline=APP_TAGLINE,
        app_icon=APP_ICON,
        source_label=SOURCE_LABEL,
        now_time=datetime.now().strftime("%H:%M:%S"),
        now_date=datetime.now().strftime("%d %b %Y"),
        status_badge_html=status_badge_html,
        digest_html=digest_html,
        kpi_cards_html=kpi_cards_html,
        volume_chart_data=json.dumps(volume_chart_data),
        donut_data=json.dumps(donut_data),
        trending_terms_data=json.dumps(trending_terms_data),
        pulse_gauge_data=json.dumps(pulse_gauge_data),
        score_label=score_label,
        velocity=velocity,
        breakdown_positive_pct=breakdown.positive_pct,
        feed_items=feed_items,
        feed_total=feed_total,
        leaderboard_html=leaderboard_html,
        rolling_chart_data=json.dumps(rolling_chart_data),
        scatter_chart_data=json.dumps(scatter_chart_data),
        heatmap_data=json.dumps(heatmap_data),
        drift_available=drift_available,
        drift_chart_data=json.dumps(drift_chart_data),
        drift_gauge_data=json.dumps(drift_gauge_data),
        drift_status=drift_status,
        drift_before_after=drift_before_after,
        alerts_html=alerts_html,
        zscore_chart_data=json.dumps(zscore_chart_data),
        corr_available=corr_available,
        corr_chart_data=json.dumps(corr_chart_data),
        corr_gauge_data=json.dumps(corr_gauge_data),
        corr_label=corr_label,
        pipeline_state=status.state,
        minutes_since_last=status.minutes_since_last,
        last_timestamp=status.last_timestamp,
        pipeline_cards_html=pipeline_cards_html,
        table_counts=table_counts,
        quality_cards_html=quality_cards_html,
        schema_registry=schema_registry,
        architecture_diagram=ARCHITECTURE_DIAGRAM,
    )


@app.route("/export/alerts")
def export_alerts():
    alerts = load_alerts()
    csv_data = alerts.drop(columns=["timestamp_dt"], errors="ignore").to_csv(
        index=False
    )
    return Response(
        csv_data,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=pulselite_alerts.csv"},
    )


if __name__ == "__main__":
    app.run(debug=True, port=5000)
