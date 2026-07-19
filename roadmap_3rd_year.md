# PulseLite — 3rd Year Extension Roadmap

## What this project is today

PulseLite is a real-time Hacker News intelligence pipeline: two Kafka
producers feed Avro-serialized, schema-validated messages through a stream
processor that runs sentiment analysis, anomaly detection, and topic drift
detection, plus a stream-stream join for cross-topic correlation. Two
dashboards (Streamlit and Flask) surface it live, backed by a reliability
layer (dedup, retry/backoff, graceful shutdown, dead-letter handling, and
crash-safe offset commits) and a full test/CI/pre-commit setup.

## The arc: where this could be by 3rd year internship (May 2027)

By the 3rd year internship, PulseLite should evolve from a single-node,
at-least-once pipeline into something closer to production streaming
infrastructure: exactly-once processing guarantees, proper handling of
late-arriving events via watermarks, a second real stream-stream join
extension, and a migration path off simple Kafka consumers toward Flink
for true stateful stream processing. This directly maps to the 3rd year
"B2 Clickstream Telemetry Pipeline" problem — same core skills, greater
scale and rigor.

## 3rd Year Semester Plan (Aug 2026 - Dec 2026)

### Milestone 1 (Aug-Sep 2026): Exactly-once processing
- What I'll add: idempotent producers, transactional writes so no message
  is processed twice even across crashes/restarts
- Tools I'll learn: Kafka transactions API, idempotent producer config
- Time commitment: 4-5 hours/week
- Done looks like: killing the processor mid-batch no longer risks even
  a single duplicate downstream, provable with a repeatable test

### Milestone 2 (Oct-Nov 2026): Late-arrival handling with watermarks
- What I'll add: proper event-time windowing that tolerates out-of-order
  HN API responses instead of assuming strict arrival order
- Tools I'll learn: watermarking concepts, event-time vs. processing-time
  semantics
- Time commitment: 4-5 hours/week
- Done looks like: a simulated late-arriving post still lands in the
  correct time window instead of being silently dropped or double-counted

### Milestone 3 (Nov-Dec 2026): Second stream-stream join + Flink migration groundwork
- What I'll add: a third Kafka topic (e.g., "ask HN" posts) joined against
  the existing two, plus a proof-of-concept processor rewritten in Flink
  for one pipeline stage
- Tools I'll learn: Flink basics, DataStream API
- Time commitment: 5-6 hours/week
- Done looks like: the new join produces a genuinely new metric, and the
  Flink proof-of-concept processes the same data as the current
  Python-based processor with equivalent output

## 3rd Year Internship Plan (Jun-Jul 2027)

This becomes the seed of **B2 Clickstream Telemetry Pipeline** — the
architecture is the same, but at 3rd year internship scale I'd aim for a
genuinely production-shaped system: full Flink migration (not just one
stage), proper schema evolution testing in CI, and observability with
Prometheus/Grafana instead of dashboard-embedded metrics.

## What I'll need from the placement / mentor ecosystem

Access to a mentor with real streaming infrastructure experience (ideally
someone who's run Kafka/Flink in production), a place to test Flink locally
without needing paid cloud infra, and code review specifically on the
exactly-once and watermarking implementations, since correctness bugs there
are easy to miss without expert eyes.

## Risks & open questions

The biggest risk is scope creep — exactly-once and Flink migration are both
individually substantial, and doing both well in one semester alongside
coursework is ambitious. I don't yet know how much Flink's learning curve
will eat into the timeline; if Milestone 3 slips, I'll prioritize the
Flink proof-of-concept over the second stream join, since it has more
direct value for the 3rd year internship problem statement.