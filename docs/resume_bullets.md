# PulseLite — Resume Bullets

- Built a real-time data pipeline (PulseLite) ingesting live Hacker News
  activity through Kafka with Avro serialization and Confluent Schema
  Registry, enforcing schema compatibility and processing posts through
  VADER sentiment analysis, rolling-average anomaly detection, and
  embedding-based topic drift detection.

- Engineered pipeline reliability with manual Kafka offset commits,
  retry/backoff logic, graceful shutdown handling, and dead-letter
  logging; validated crash recovery by force-killing the processor
  mid-run and confirming zero data loss on restart.

- Shipped two dashboards (Streamlit and Flask) over a shared backend,
  with real-time Pipeline Health metrics (consumer lag, throughput,
  end-to-end latency), Discord webhook alerting, 26 automated tests,
  and CI/CD via GitHub Actions.