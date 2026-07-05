# ADR 005 — Stream-Stream Join: Trend Correlation

## Decision
Join two Kafka streams (hn-posts: new stories, hn-top: top stories) on a 
sliding 12-minute window and compute Pearson correlation of volume.

## Why This Matters
Stream-stream joins are the hardest concept in stream processing. They require:
- Window alignment — both streams must use the same windowing strategy
- State management — join state grows with product of stream rates
- Out-of-order handling — one stream late, the other on time

## Implementation
- Topic 1: hn-posts (new stories — high volume, unproven)
- Topic 2: hn-top (top stories — lower volume, viral/proven)
- Window: 12-minute sliding window
- Metric: Pearson correlation coefficient of (volume_new, volume_top)
- Join processor: processor/stream_join.py runs every 60 seconds

## What We Found
Correlation between new stories and top stories is typically weak/negative
(-0.249 in testing). This makes sense:
- New stories flood in constantly regardless of quality
- Top stories are a curated subset that earned upvotes over hours
- When a major event breaks, new stories spike but top stories lag

## Edge Case: Empty Window on One Side
When one stream has no events in a window, the join drops that pair.
This is handled by defaulting missing minutes to 0 volume, which still
allows correlation to be computed but may artificially push it toward 0.
A production system would use outer joins with null-handling.

## Interview Talking Point
"I joined two Kafka streams on a sliding window to detect correlated trends.
The trickiest part was handling empty windows — when one stream had no events,
I default to 0 volume rather than dropping the window entirely. The correlation
between new and top HN stories is typically weak/negative because they represent
different stages of content lifecycle: unproven vs viral."

## Consequences
- Requires running a third process (stream_join.py) alongside producer and processor
- State is maintained in memory (minute_bucket dict) — acceptable for our scale
- Correlation recomputed every 60 seconds — sufficient for our use case