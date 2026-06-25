# PulseLite — Design Doc
**Student:** Dewesh | **Problem:** H3 | **Date:** 24 June 2026

## What I'm building
A real-time Hacker News post monitor that tracks volume, sentiment,
and top words — with an anomaly alert when volume spikes 3x above normal.

## Data source
Hacker News public API (no API key required).
Free, stable, real-time tech posts with scores and comments.
Chosen because it is free, requires no auth, and works reliably from any region.

## Architecture
Hacker News API → Kafka → PySpark → DuckDB → Streamlit

## Tech stack
| Component | Choice | Why |
|-----------|--------|-----|
| Data source | Hacker News API | Free, no auth, reliable, real-time |
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
- HN API requires one request per post — fix: fetch top 25 only per poll
- Kafka setup complexity — fix: use bitnami Docker image