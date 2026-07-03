# PulseLite 🔴
🌐 **Live Demo:** https://pulselite-dewesh.streamlit.app

> Real-time Hacker News pulse tracker — know what tech is talking about, the moment it happens.
## What it does

PulseLite monitors Hacker News in real time and shows:

- 📈 Post volume per minute (live updating)

- 😊 Sentiment — are people positive, negative, or neutral?

- 🔤 Top 10 words trending right now

- 🚨 Anomaly alerts when volume suddenly spikes 3x above normal
## Architecture

Hacker News API → Kafka → Processor + VADER → DuckDB → Streamlit Dashboard
## Tech Stack
ComponentToolWhyData sourceHacker News APIFree, no auth, real-timeMessage queueApache Kafka (Docker)Industry standard, decoupled pipelineStream processorPython + confluent-kafkaReads from Kafka, runs NLPSentiment analysisVADERLightweight, no model training neededStorageDuckDBFast, lightweight, no server neededDashboardStreamlit + PlotlyPython-native, easy auto-refreshContainersDocker ComposeRuns Kafka without manual install
## How to Run Locally

Start Kafka: docker-compose up -d
Activate venv: venv\Scripts\activate
Terminal 1 — Producer: python producer/reddit_producer.py
Terminal 2 — Processor: python processor/spark_processor.py
Terminal 3 — Dashboard: streamlit run dashboard/app.py

## Pipeline Status

- ✅ Data source — Hacker News API

- ✅ Kafka message queue — running in Docker

- ✅ Stream processor — VADER sentiment analysis

- ✅ DuckDB storage

- ⬜ Streamlit dashboard — in progress
## ADRs

- ADR 001 — Data Source Selection: docs/adr/adr-001-data-source.md

- ADR 002 — Message Queue Selection: docs/adr/adr-002-message-queue.md
## Author

Dewesh | B.Tech CSE-AIDE | Internship 2026