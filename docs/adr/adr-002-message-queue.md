# ADR 002 — Message Queue Selection

## Decision
Use Apache Kafka as the message queue between producer and processor.

## Reason
- Industry standard for real-time data pipelines
- Decouples the producer and processor completely
  (if processor crashes, messages wait safely in Kafka)
- Supports high volume — can handle thousands of messages per second
- Required by the problem statement (H3)
- Bitnami images were removed from Docker Hub so switched to
  Confluent images (confluentinc/cp-kafka:7.4.0) which are 
  officially maintained and more stable

## Alternatives considered
- Direct Python queue (Queue module) — simpler but not persistent,
  messages lost if processor crashes
- RabbitMQ — good alternative but Kafka is more relevant for
  data engineering roles

## Consequences
- Requires Docker to run locally
- Adds operational complexity
- But gives us a production-grade pipeline that mirrors
  real company architectures
- Topic created: hn-posts (1 partition, replication factor 1)