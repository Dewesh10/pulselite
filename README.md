# PulseLite 🔴

![CI](https://github.com/Dewesh10/pulselite/actions/workflows/test.yml/badge.svg)

🌐 **Live Demo:** https://pulselite-dewesh.streamlit.app *(runs in Demo Mode — see note below)*

> Real-time Hacker News intelligence — sentiment, volume anomalies, cross-topic
> correlation, and topic drift, the moment it happens.

## What it does

PulseLite ingests Hacker News in real time through Kafka and surfaces:

- 🧠 **Pulse Digest** — an LLM-generated (Claude Haiku) narrative summary of what's happening right now, refreshed every 5 minutes, with a deterministic rule-based fallback if no API key is configured
- 📈 **Post volume per minute**, live-updating with a rolling average
- 😊 **Sentiment analysis** (VADER) — positive / negative / neutral, plus a composite mood index
- 🔤 **Trending terms** — most frequent meaningful words across tracked titles
- 🚨 **Anomaly detection** — dual-layer: a 3× rolling-average rule from the stream processor, plus an independent statistical z-score overlay computed in the dashboard
- 🔗 **Cross-topic correlation** — a stream-stream join between "new stories" and "top stories" topics, correlating volume with Pearson's r
- 🧭 **Topic drift detection** — sentence-embedding comparison between time windows to catch when the conversation actually shifts topic, not just volume
- 💓 **Pulse Score** — a single 0–100 composite index blending velocity, sentiment, and stability
- ⚙️ **Pipeline health** — schema registry status, table row counts, data quality checks, freshness/liveness badge

## Architecture

```mermaid
graph LR
    HN[🌐 Hacker News API] -->|HTTP Poll every 30s| P1[Producer\nreddit_producer.py]
    HN -->|HTTP Poll every 30s| P2[Top Producer\ntop_producer.py]

    P1 -->|Avro messages| K1[Kafka Topic\nhn-posts]
    P2 -->|Avro messages| K2[Kafka Topic\nhn-top]

    SR[Schema Registry\n:8081] -.->|Schema validation| P1
    SR -.->|Schema validation| P2

    K1 -->|Consumer| PROC[Stream Processor\nspark_processor.py]
    K1 -->|Consumer| JOIN[Stream Join\nstream_join.py]
    K2 -->|Consumer| JOIN

    PROC -->|VADER Sentiment| CSV1[data_posts.csv]
    PROC -->|Volume counts| CSV2[data_volume.csv]
    PROC -->|Anomaly alerts| CSV3[data_alerts.csv]
    PROC -->|Sentence embeddings| CSV4[data_drift.csv]
    JOIN -->|Pearson correlation| CSV5[data_correlation.csv]

    CSV1 --> DASH[Streamlit Dashboard\ndashboard/app.py]
    CSV2 --> DASH
    CSV3 --> DASH
    CSV4 --> DASH
    CSV5 --> DASH

    DASH -->|Live URL| USER[👤 User]
```

Full component-by-component breakdown: [`docs/architecture.md`](docs/architecture.md)

## Tech Stack

| Component | Tool | Why |
|---|---|---|
| Data source | Hacker News API | Free, no auth, real-time |
| Message queue | Apache Kafka (Docker) | Industry-standard decoupled pipeline |
| Serialization | Avro + Confluent Schema Registry | Type-safe messages, backward-compatible schema evolution enforced at deploy time (see [ADR-004](docs/adr/adr-004-schema-registry.md)) |
| Stream processing | Python + confluent-kafka | Sentiment scoring, windowed volume, anomaly detection |
| Sentiment analysis | VADER | Lightweight, no model training needed |
| Drift detection | sentence-transformers | Embedding-based topic shift detection |
| Stream join | Python + NumPy | Cross-topic correlation (see [ADR-005](docs/adr/adr-005-stream-join.md)) |
| Narrative summary | Anthropic API (Claude Haiku) | LLM-generated Pulse Digest, rule-based fallback if no key (see [ADR-006](docs/adr/adr-006-pulse-digest.md)) |
| Storage | CSV | Lock-free handoff between processor and dashboard, no concurrent-writer issues |
| Dashboard | Streamlit + Plotly | Python-native, live auto-refresh via `st_autorefresh` |
| CI | GitHub Actions | Runs the test suite on every push |
| Containers | Docker Compose | Runs Kafka + Zookeeper + Schema Registry without manual install |
| Deployment | Streamlit Cloud | Public demo |

## How to Run Locally

```powershell
# 1. Start Kafka + Zookeeper + Schema Registry
docker-compose up -d

# 2. Activate your virtual environment
venv\Scripts\activate
pip install -r requirements.txt

# 3. Terminal 1 — new-stories producer
python producer/reddit_producer.py

# 4. Terminal 2 — top-stories producer
python producer/top_producer.py

# 5. Terminal 3 — stream processor (sentiment, volume, anomalies, drift)
python processor/spark_processor.py

# 6. Terminal 4 — stream join (cross-topic correlation)
python processor/stream_join.py

# 7. Terminal 5 — Pulse Digest generator (narrative summary, refreshes every 5 min)
# Optional: set ANTHROPIC_API_KEY first for LLM-generated digests, otherwise
# it uses a rule-based fallback automatically — see .env.example
python processor/digest_generator.py

# 8. Terminal 6 — dashboard
streamlit run dashboard/app.py
```

### Demo Mode (no pipeline required)

Don't want to spin up Kafka just to look around? The dashboard ships with a
replay mode using a pre-recorded snapshot — timestamps are shifted to "now"
on every load, so it always looks live even if you run it weeks from now.

```powershell
$env:DEMO_MODE="true"
streamlit run dashboard/app.py
```

> The hosted Streamlit Cloud demo runs in Demo Mode, since Cloud only hosts
> the dashboard script — it can't run Kafka/producers/processors in the
> background. Run locally with the full pipeline above to see real,
> currently-streaming data.

## Testing

```powershell
pip install -r requirements-test.txt
pytest tests/ -v
```

21 tests covering sentiment scoring, anomaly detection boundaries, producer
retry logic, and Pulse Digest generation (stats gathering, fallback digest
content, and CSV output). Runs automatically on every push via GitHub Actions.

## Pipeline Status

- ✅ Data source — Hacker News API (new stories + top stories)
- ✅ Kafka message queue — Avro-serialized, Schema Registry-validated
- ✅ Stream processor — VADER sentiment, volume, anomaly detection, drift detection
- ✅ Stream-stream join — cross-topic correlation
- ✅ Pulse Digest — LLM-generated narrative summary with rule-based fallback
- ✅ CSV storage — lock-free handoff
- ✅ Streamlit dashboard — 5 tabs (Overview, Live Feed, Trends & Analytics, Anomalies, Pipeline Health) + Demo Mode
- ✅ CI — automated test suite on every push

## Architecture Decision Records

- [ADR 001 — Data Source Selection](docs/adr/adr-001-data-source.md)
- [ADR 002 — Message Queue Selection](docs/adr/adr-002-message-queue.md)
- [ADR 003 — Dashboard Framework](docs/adr/adr-003-dashboard.md)
- [ADR 004 — Avro + Confluent Schema Registry](docs/adr/adr-004-schema-registry.md)
- [ADR 005 — Stream-Stream Join](docs/adr/adr-005-stream-join.md)
- [ADR 006 — LLM Pulse Digest](docs/adr/adr-006-pulse-digest.md)

Also see [`docs/design_doc.md`](docs/design_doc.md) for the original problem
statement and [`docs/learning-notes.md`](docs/learning-notes.md) for what
actually broke along the way.


## Author

Dewesh | B.Tech CSE-AIDE | Internship 2026