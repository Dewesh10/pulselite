# PulseLite — Design Doc
**Student:** Dewesh | **Problem:** H3 | **Date:** 24 June 2026

## What I'm building
A real-time Reddit post monitor that tracks volume, sentiment,
and top words — with an anomaly alert when volume spikes 3x above normal.

## Data source
Reddit public API via PRAW library. Subreddit: r/india
Chosen because it is free, public, and has consistent traffic.

## Architecture
Reddit API → Kafka → PySpark → DuckDB → Streamlit

## Tech stack
| Component | Choice | Why |
|-----------|--------|-----|
| Data source | Reddit (PRAW) | Free, public, no rate limit issues |
| Message queue | Kafka (Docker) | Industry standard |
| Processing | PySpark Structured Streaming | Required by problem statement |
| Storage | DuckDB | Lightweight, no server needed |
| Dashboard | Streamlit | Python-native, easy auto-refresh |
| Containers | Docker Compose | Runs Kafka without manual install |

## What the dashboard will show
- Posts per minute (live line chart)
- Sentiment score (positive / negative / neutral %)
- Top 10 words in last 5 minutes
- Red alert marker when volume is 3x rolling average (mini-extension)

## Known risks
- Reddit API rate limits if polling too fast — fix: poll every 10 seconds
- Kafka setup complexity — fix: use bitnami Docker image