# ADR 001 — Data Source Selection

## Decision
Use Hacker News public API instead of Reddit.

## Reason
Reddit blocked new API app registrations in our region.
Hacker News API is free, requires no authentication, 
works from any region, and provides real-time tech posts
with scores and comment counts — same data shape as Reddit.

## Consequences
- No API key management needed
- More reliable for demo purposes
- Tech-focused content fits the PulseLite use case well