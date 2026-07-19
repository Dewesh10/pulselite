"""
PulseLite — Pulse Digest Generator
=======================================
Reads the current pipeline state (posts, volume, alerts, cross-topic
correlation, topic drift) and produces a short, human-readable narrative
summary — the kind of thing a human analyst would say if you asked them
"what's happening on Hacker News right now?" instead of making you read
six charts to find out yourself.

Design notes (see docs/adr/adr-006-pulse-digest.md for the full writeup):

- Runs as its own long-lived process, same pattern as spark_processor.py
  and stream_join.py — polls on an interval, writes its output to a CSV,
  the dashboard just reads the CSV. No new architecture, just another
  producer of a CSV the dashboard already knows how to display.
- Calls the Anthropic API by default, but never hard-fails without a key:
  if ANTHROPIC_API_KEY isn't set (or the API call fails), it falls back
  to a deterministic, rule-based digest built from the same stats. A
  demo/CI environment should never be blocked on having a paid API key.
- Retries the API call with exponential backoff, consistent with the
  retry/backoff pattern already used in producer/reddit_producer.py.
- Rate-limited to one call per DIGEST_INTERVAL_SECONDS (default 5 min)
  to keep API cost predictable regardless of how fast the dashboard
  itself refreshes.
"""

from __future__ import annotations

import csv
import os
import signal
import sys
import time
from datetime import datetime

import pandas as pd

DIGEST_INTERVAL_SECONDS = int(os.environ.get("DIGEST_INTERVAL_SECONDS", "300"))
DIGEST_MODEL = os.environ.get("DIGEST_MODEL", "claude-haiku-4-5-20251001")
DIGEST_CSV = "data_digest.csv"

CSV_POSTS = "data_posts.csv"
CSV_VOLUME = "data_volume.csv"
CSV_ALERTS = "data_alerts.csv"
CSV_CORRELATION = "data_correlation.csv"
CSV_DRIFT = "data_drift.csv"

_shutdown_requested = False


def _shutdown_handler(sig, frame):
    global _shutdown_requested
    print("\n⚡ Digest generator shutting down gracefully after this cycle...")
    _shutdown_requested = True


signal.signal(signal.SIGINT, _shutdown_handler)
signal.signal(signal.SIGTERM, _shutdown_handler)


# --------------------------------------------------------------------------
# Stats gathering — pulls the same signals a human would look at
# --------------------------------------------------------------------------


def _read_csv_safe(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        return df if not df.empty else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def gather_stats() -> dict:
    posts = _read_csv_safe(CSV_POSTS)
    volume = _read_csv_safe(CSV_VOLUME)
    alerts = _read_csv_safe(CSV_ALERTS)
    correlation = _read_csv_safe(CSV_CORRELATION)
    drift = _read_csv_safe(CSV_DRIFT)

    stats = {
        "total_posts": len(posts),
        "velocity": 0.0,
        "sentiment_positive_pct": 0.0,
        "sentiment_negative_pct": 0.0,
        "top_posts": [],
        "recent_anomaly": None,
        "latest_correlation": None,
        "top_drift": None,
    }

    if not posts.empty:
        posts["score"] = pd.to_numeric(posts.get("score"), errors="coerce").fillna(0)
        posts["comments"] = pd.to_numeric(
            posts.get("comments"), errors="coerce"
        ).fillna(0)
        total = len(posts)
        if "sentiment_label" in posts.columns and total:
            counts = posts["sentiment_label"].value_counts()
            stats["sentiment_positive_pct"] = round(
                counts.get("positive", 0) / total * 100, 1
            )
            stats["sentiment_negative_pct"] = round(
                counts.get("negative", 0) / total * 100, 1
            )
        posts["engagement"] = posts["score"] + posts["comments"] * 2
        top = posts.sort_values("engagement", ascending=False).head(5)
        stats["top_posts"] = [
            {"title": row.title, "score": int(row.score), "comments": int(row.comments)}
            for row in top.itertuples()
        ]

    if not volume.empty and "post_count" in volume.columns:
        stats["velocity"] = round(float(volume["post_count"].tail(5).mean()), 1)

    if not alerts.empty:
        last = alerts.iloc[-1]
        stats["recent_anomaly"] = {
            "post_count": float(last.get("post_count", 0)),
            "rolling_avg": float(last.get("rolling_avg", 0)),
        }

    if not correlation.empty and "correlation" in correlation.columns:
        stats["latest_correlation"] = round(
            float(correlation["correlation"].iloc[-1]), 3
        )

    if not drift.empty and "drift_score" in drift.columns:
        idx = drift["drift_score"].idxmax()
        row = drift.loc[idx]
        stats["top_drift"] = {
            "score": round(float(row["drift_score"]), 3),
            "sample_title": str(row.get("sample_title", "")),
        }

    return stats


# --------------------------------------------------------------------------
# Narrative generation — LLM primary, rule-based fallback
# --------------------------------------------------------------------------


def build_prompt(stats: dict) -> str:
    top_titles = (
        "\n".join(
            f"- \"{p['title']}\" ({p['score']} pts, {p['comments']} comments)"
            for p in stats["top_posts"]
        )
        or "- (no posts tracked yet)"
    )

    anomaly_line = "None currently active."
    if stats["recent_anomaly"]:
        anomaly_line = (
            f"Volume spike detected: {stats['recent_anomaly']['post_count']:.0f} posts/min "
            f"vs a rolling average of {stats['recent_anomaly']['rolling_avg']:.1f}."
        )

    drift_line = "No significant topic shift detected."
    if stats["top_drift"] and stats["top_drift"]["score"] >= 0.3:
        drift_line = (
            f"Topic drift score {stats['top_drift']['score']:.2f} — "
            f"conversation may be shifting, e.g. around: \"{stats['top_drift']['sample_title']}\"."
        )

    correlation_line = "Not enough data yet."
    if stats["latest_correlation"] is not None:
        correlation_line = f"{stats['latest_correlation']:+.2f} (new-story volume vs. top-story volume)"

    return f"""You are writing a short intelligence brief for a live Hacker News monitoring dashboard.

Data:
- Posts tracked: {stats['total_posts']}
- Current velocity: {stats['velocity']} posts/min
- Sentiment mix: {stats['sentiment_positive_pct']}% positive, {stats['sentiment_negative_pct']}% negative
- Top posts by engagement:
{top_titles}
- Anomaly status: {anomaly_line}
- Topic drift: {drift_line}
- New-vs-top story volume correlation: {correlation_line}

Write a 2-3 sentence summary a busy engineer could read in 5 seconds to
understand what's happening right now. Be specific (cite a real number or
title), not generic. No preamble, no "Here's a summary" — just the brief
itself, plain text."""


def generate_llm_digest(stats: dict) -> str | None:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    try:
        import anthropic
    except ImportError:
        print("⚠️  anthropic package not installed — falling back to rule-based digest.")
        return None

    client = anthropic.Anthropic(api_key=api_key)
    prompt = build_prompt(stats)

    backoff = 2
    for attempt in range(3):
        try:
            response = client.messages.create(
                model=DIGEST_MODEL,
                max_tokens=200,
                messages=[{"role": "user", "content": prompt}],
            )
            text = "".join(
                block.text
                for block in response.content
                if getattr(block, "type", None) == "text"
            ).strip()
            return text or None
        except anthropic.AuthenticationError as exc:
            # Retrying won't fix a bad/missing key — fail fast to the
            # rule-based fallback instead of burning 3 attempts on it.
            print(f"⚠️  Digest API auth failed (check ANTHROPIC_API_KEY): {exc}")
            return None
        except Exception as exc:
            print(f"⚠️  Digest API call failed (attempt {attempt + 1}/3): {exc}")
            if attempt < 2:
                time.sleep(backoff)
                backoff *= 2
    return None


def generate_fallback_digest(stats: dict) -> str:
    """
    Deterministic, template-based digest used when no API key is
    configured or the API call fails. Keeps the pipeline fully functional
    without any paid dependency — a demo, CI run, or offline reviewer
    should still see a real digest, just a simpler one.
    """
    if stats["total_posts"] == 0:
        return "Waiting for data — no posts tracked yet. Start the producer and processor to begin monitoring."

    mood = (
        "leaning positive"
        if stats["sentiment_positive_pct"] > stats["sentiment_negative_pct"]
        else (
            "leaning negative"
            if stats["sentiment_negative_pct"] > stats["sentiment_positive_pct"]
            else "mixed"
        )
    )
    parts = [
        f"Tracking {stats['total_posts']} posts at {stats['velocity']} posts/min, sentiment {mood} "
        f"({stats['sentiment_positive_pct']}% positive / {stats['sentiment_negative_pct']}% negative)."
    ]
    if stats["top_posts"]:
        top = stats["top_posts"][0]
        parts.append(
            f"Top story right now: \"{top['title']}\" ({top['score']} pts, {top['comments']} comments)."
        )
    if stats["recent_anomaly"]:
        parts.append(
            f"Volume anomaly active: {stats['recent_anomaly']['post_count']:.0f} vs "
            f"rolling average {stats['recent_anomaly']['rolling_avg']:.1f}."
        )
    if stats["top_drift"] and stats["top_drift"]["score"] >= 0.3:
        parts.append(
            f"Possible topic shift detected (drift score {stats['top_drift']['score']:.2f})."
        )
    return " ".join(parts)


def generate_digest(stats: dict) -> tuple[str, str]:
    """Returns (digest_text, mode) where mode is 'llm' or 'fallback'."""
    llm_text = generate_llm_digest(stats)
    if llm_text:
        return llm_text, "llm"
    return generate_fallback_digest(stats), "fallback"


# --------------------------------------------------------------------------
# CSV output
# --------------------------------------------------------------------------


def write_digest(digest_text: str, mode: str, posts_analyzed: int) -> None:
    file_exists = os.path.exists(DIGEST_CSV)
    with open(DIGEST_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["generated_at", "digest_text", "mode", "posts_analyzed"])
        writer.writerow([datetime.now().isoformat(), digest_text, mode, posts_analyzed])


# --------------------------------------------------------------------------
# Main loop
# --------------------------------------------------------------------------


def run_once() -> None:
    stats = gather_stats()
    digest_text, mode = generate_digest(stats)
    write_digest(digest_text, mode, stats["total_posts"])
    print(f"✅ [{mode}] {digest_text}")


def main() -> None:
    print(
        f"🧠 Pulse Digest generator started — refreshing every {DIGEST_INTERVAL_SECONDS}s "
        f"({'LLM mode' if os.environ.get('ANTHROPIC_API_KEY') else 'fallback mode, no ANTHROPIC_API_KEY set'})"
    )
    while not _shutdown_requested:
        try:
            run_once()
        except Exception as exc:
            print(f"⚠️  Digest cycle failed, will retry next interval: {exc}")
        for _ in range(DIGEST_INTERVAL_SECONDS):
            if _shutdown_requested:
                break
            time.sleep(1)
    print("✅ Digest generator stopped cleanly.")
    sys.exit(0)


if __name__ == "__main__":
    main()
