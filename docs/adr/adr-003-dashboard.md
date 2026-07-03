# ADR 003 — Dashboard Design Decision

## Decision
Build the dashboard using Streamlit with Plotly charts and custom CSS,
rather than a pre-built BI tool like Metabase or Grafana.

## Reason
- Streamlit is Python-native — no separate tool to install or configure
- Full control over layout, styling, and custom components
- Can add custom analytics logic (Pulse Score, momentum, z-score anomalies)
  that no BI tool supports out of the box
- Deploys as a single Python file — easy to ship and maintain
- Plotly gives interactive, professional-grade charts with minimal code

## Alternatives considered
- **Grafana** — powerful but requires a separate server, complex setup,
  and doesn't support custom Python analytics logic
- **Metabase** — good for SQL dashboards but no Python integration,
  can't compute derived scores like PulseLite's Pulse Score
- **Dash (Plotly)** — more flexible than Streamlit but more boilerplate,
  slower to build for a 5-week internship timeline

## What we built on top of Streamlit
- Custom CSS design system with Inter font, aurora gradient background,
  indigo-cyan brand colors, and JetBrains Mono for data values
- 5-tab layout: Overview, Live Feed, Trends, Anomalies, Pipeline Health
- Composite Pulse Score (velocity + sentiment + stability)
- Independent z-score anomaly overlay on top of the processor's rule
- Sidebar with real-time filters, search, sort, and refresh controls
- streamlit-autorefresh for 1-second clock updates

## Consequences
- Dashboard is tightly coupled to DuckDB schema — schema changes
  require dashboard updates
- Streamlit's re-render model means the full page refreshes on each
  auto-refresh cycle — acceptable for our use case
- Single-file deployment makes it easy to run anywhere Python runs