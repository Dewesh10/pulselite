# ADR 004 — Avro + Confluent Schema Registry

## Decision
Replace JSON serialization in Kafka messages with Avro + Confluent Schema Registry.

## Reason
JSON serialization has no schema enforcement — a producer can change a field 
name or type and the consumer silently breaks. Schema Registry solves this by:
- Storing versioned schemas centrally
- Rejecting breaking schema changes at deploy time
- Ensuring producer and consumer always agree on message structure

## Schema Evolution Test Results

### Test 1 — Adding a field (backward compatible) ✅
Added `flair` field with a default value to post_v2.avsc.
Schema Registry accepted it — old consumers can still read new messages
because the field has a default.

### Test 2 — Removing a required field (breaking change) ❌
Attempted to remove the `title` field from the schema.
Schema Registry REJECTED it with:
"Schema being registered is incompatible with an earlier schema"
This is the expected behavior — breaking changes are caught at deploy time,
not at runtime 4 hours later when the consumer crashes.

## Architecture
- Schema stored at: schemas/post_v1.avsc
- Registry running at: http://localhost:8081
- Subject: hn-posts-value
- Schema ID assigned by registry: 1
- Serialization: Confluent wire format (magic byte + 4-byte schema ID + Avro bytes)

## Interview Talking Point
"I use Avro + Schema Registry for type safety across producers and consumers. 
Schemas are versioned, breaking changes are caught at deploy time, and backward 
compatibility is enforced by the registry. In my project, I tried to remove a 
required field and the registry rejected it before any consumer could be affected."

## Consequences
- Adds Schema Registry as a required infrastructure component
- Producer must register schema before sending messages
- Consumer must deserialize using the schema ID in each message
- Adds ~200ms startup latency for schema registration (acceptable)