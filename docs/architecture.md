# PulseLite — Architecture

## System Architecture

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

## Component Details

| Component | Technology | Purpose |
|-----------|------------|---------|
| Data Source | Hacker News API | Real-time tech news posts |
| Message Queue | Apache Kafka (Docker) | Decoupled streaming pipeline |
| Schema Registry | Confluent Schema Registry | Avro schema versioning |
| Serialization | Avro (fastavro) | Type-safe message format |
| Sentiment Analysis | VADER | Post title mood scoring |
| Drift Detection | sentence-transformers | Embedding-based topic shift |
| Stream Join | Python + NumPy | Pearson correlation across topics |
| Storage | CSV files | Lightweight, lock-free storage |
| Dashboard | Streamlit + Plotly | Live intelligence interface |
| CI/CD | GitHub Actions | Automated test suite |
| Deployment | Streamlit Cloud | Public demo URL |

## Data Flow

1. **Ingest** — Two producers poll HN API every 30 seconds
2. **Serialize** — Messages serialized as Avro, validated by Schema Registry
3. **Stream** — Messages flow through Kafka topics hn-posts and hn-top
4. **Process** — Stream processor runs VADER sentiment, computes volume, detects anomalies, computes embeddings
5. **Join** — Stream join correlates volume across two topics using Pearson correlation
6. **Store** — Results written to CSV files (no locking issues)
7. **Visualize** — Dashboard reads CSVs and renders live charts with auto-refresh