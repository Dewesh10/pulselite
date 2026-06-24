# pulselite


# PulseLite 🔴
> Real-time Reddit pulse tracker — know what the internet is talking about, the moment it happens.

## What it does
PulseLite monitors Reddit in real time and shows:
- 📈 Post volume per minute (live updating)
- 😊 Sentiment — are people positive, negative, or neutral?
- 🔤 Top 10 words trending right now
- 🚨 Anomaly alerts when volume suddenly spikes 3x above normal

## Architecture


## Tech Stack
| Component | Tool |
|-----------|------|
| Data source | Reddit API (PRAW) |
| Message queue | Apache Kafka (Docker) |
| Stream processor | PySpark Structured Streaming |
| Storage | DuckDB |
| Dashboard | Streamlit + Plotly |
| Containers | Docker Compose |

## Status
🚧 Week 1 — Setting up pipeline foundation

## Author
Dewesh | B.Tech CSE-AIDE | Internship 2026