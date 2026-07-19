# Changelog

All notable changes to PulseLite are documented here.

## [2026-07-19]
### Added
- Real Kafka consumer lag, throughput, and end-to-end latency metrics on the Pipeline Health tab
- Discord webhook alerts fired automatically when a volume anomaly is detected
- ADR-004 Avro schema compatibility tests (backward compatibility, breaking-change detection)
- Pre-commit hooks (black + ruff) for consistent code formatting and linting

## [2026-07-16]
### Added
- Dedup by post ID, retry/backoff, graceful shutdown, and dead-letter handling across both producers and the processor
- UTC timestamps standardized across the pipeline
- Manual Kafka offset commits in the processor — verified via a hard-kill/restart test that the pipeline resumes cleanly with no data loss

## [2026-07-15]
### Added
- Flask dashboard (`flask_dashboard/`) as an alternative to the Streamlit dashboard, sharing the same backend and pipeline
### Fixed
- Correlation chart no longer appears on every tab — scoped to Trends only
- Chart x-axis dates cleaned up with Plotly's native date formatting
- Drift "before/after" cards now show real per-window titles instead of duplicated hardcoded text
- Duplicate leaderboard posts removed via title-based deduplication
- Removed stale DuckDB references in code and UI labels, left over from the earlier switch to CSV storage
### Changed
- README rewritten to document both dashboards, with an accurate architecture diagram and pipeline status

## [2026-07-14]
### Added
- Migrated dashboard rendering from Streamlit to Flask, keeping the backend and Kafka pipeline untouched

## [2026-07-08]
### Added
- LLM-generated Pulse Digest (Claude Haiku), with a rule-based fallback when no API key is set

## [2026-07-05 to 2026-07-07]
### Added
- Demo Mode with pre-generated, timestamp-shifting data for Streamlit Cloud deployment
- Clickable HN links on post titles
- LICENSE and `.env.example`
- GitHub Actions CI running the test suite on every push
- Stream-stream join between "new stories" and "top stories" topics, with Pearson correlation (ADR-005)
- Avro serialization with Confluent Schema Registry (ADR-004), including a v2 schema demonstrating backward-compatible evolution
- Mermaid architecture diagram in the README

## [2026-07-03 to 2026-07-04]
### Added
- Topic drift detection using sentence embeddings, with before/after comparison and a STABLE/DRIFTING indicator
- Professional dashboard redesign — tabs, Pulse Score, auto-refresh, Pipeline Health
- First pytest test suite (9 tests) for sentiment and anomaly detection
- Live deployment to Streamlit Cloud
### Fixed
- Replaced DuckDB with CSV storage to resolve Windows file locking issues

## [2026-06-24 to 2026-06-29]
### Added
- Initial project structure, design doc, and Architecture Decision Records (data source, message queue selection)
- Hacker News producer sending posts to Kafka topic `hn-posts`
- Stream processor reading from Kafka and running VADER sentiment analysis
- Docker Compose setup with Kafka and Zookeeper
- First working live Streamlit dashboard