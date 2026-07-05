"""
PulseLite — Real-Time Hacker News Intelligence Dashboard
=============================================================
Single-file build. Run with:

    streamlit run dashboard/app.py

from the repository root (so pulselite.db on disk is found).
"""

from __future__ import annotations

import os
import re
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from streamlit_autorefresh import st_autorefresh
import duckdb
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def _raw(html: str) -> str:
    """Strip leading whitespace from every line of a multi-line HTML
    string. Streamlit's markdown renderer treats 4-space-indented lines
    as a fenced code block, which would otherwise print raw <div> tags
    instead of rendering them."""
    return "\n".join(line.strip() for line in html.strip("\n").splitlines())


# ============================================================================
# CONFIG MODULE (merged)
# ============================================================================
# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------

def resolve_db_path() -> str:
    """
    Resolve the DuckDB file regardless of whether Streamlit is launched
    from the repo root (`streamlit run dashboard/app.py`, the documented
    way) or from inside the dashboard/ folder itself.
    """
    candidates = [
        "pulselite.db",
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "pulselite.db"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "pulselite.db"),
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    # Fall back to the documented default even if it doesn't exist yet —
    # the UI has a graceful "waiting for data" state for this case.
    return "pulselite.db"


DB_PATH = resolve_db_path()
DASH_DB = DB_PATH

# --------------------------------------------------------------------------
# App metadata
# --------------------------------------------------------------------------

APP_NAME = "PulseLite"
APP_TAGLINE = "Real-Time Hacker News Intelligence"
APP_ICON = "🔴"
AUTHOR = "Dewesh · B.Tech CSE-AIDE · Internship 2026"
SOURCE_LABEL = "Hacker News"
PIPELINE = ["Hacker News API", "Kafka", "Stream Processor (VADER)", "DuckDB", "PulseLite"]

# --------------------------------------------------------------------------
# Refresh / freshness thresholds
# --------------------------------------------------------------------------

DEFAULT_REFRESH_SECONDS = 20
MIN_REFRESH_SECONDS = 5
MAX_REFRESH_SECONDS = 90

# A pipeline is considered "LIVE" if the newest post landed within this
# many minutes; "IDLE" up to the second threshold; otherwise "OFFLINE".
FRESHNESS_LIVE_MINUTES = 3
FRESHNESS_IDLE_MINUTES = 15

# z-score magnitude beyond which a volume bucket is flagged as anomalous
# in the dashboard's own (independent) anomaly overlay.
ZSCORE_ANOMALY_THRESHOLD = 2.0

# --------------------------------------------------------------------------
# Color system
# --------------------------------------------------------------------------
# A cohesive dark "aurora" palette: deep charcoal/navy base, an
# indigo -> cyan brand gradient, and semantic colors for sentiment/status.

COLORS = {
    # Base surfaces
    "bg": "#0a0e17",
    "bg_alt": "#0d1220",
    "surface": "#131a2b",
    "surface_alt": "#171f34",
    "border": "rgba(148, 163, 184, 0.12)",
    "border_strong": "rgba(148, 163, 184, 0.22)",

    # Text
    "text_primary": "#f1f5f9",
    "text_secondary": "#94a3b8",
    "text_muted": "#5b6a85",

    # Brand gradient
    "brand_start": "#6366f1",   # indigo
    "brand_mid": "#8b5cf6",     # violet
    "brand_end": "#22d3ee",     # cyan

    # Semantic
    "positive": "#22c55e",
    "positive_soft": "rgba(34, 197, 94, 0.14)",
    "negative": "#f43f5e",
    "negative_soft": "rgba(244, 63, 94, 0.14)",
    "neutral": "#60a5fa",
    "neutral_soft": "rgba(96, 165, 250, 0.14)",
    "warning": "#f59e0b",
    "warning_soft": "rgba(245, 158, 11, 0.14)",

    # Status
    "live": "#22c55e",
    "idle": "#f59e0b",
    "offline": "#f43f5e",
}

SENTIMENT_COLOR_MAP = {
    "positive": COLORS["positive"],
    "negative": COLORS["negative"],
    "neutral": COLORS["neutral"],
}

SENTIMENT_EMOJI = {
    "positive": "🟢",
    "negative": "🔴",
    "neutral": "🔵",
}

CHART_FONT = "Inter, -apple-system, sans-serif"
MONO_FONT = "'JetBrains Mono', 'Fira Code', monospace"

# --------------------------------------------------------------------------
# Pulse Score weighting
# --------------------------------------------------------------------------
# The Pulse Score is a composite 0-100 index PulseLite derives to answer
# "how much is the tech community buzzing right now, and how does it feel?"
# It blends three signals with the following weights:

PULSE_WEIGHTS = {
    "velocity": 0.40,   # how much discussion volume is happening
    "sentiment": 0.35,  # how positive/negative the mood is
    "stability": 0.25,  # penalized by recent anomalies/volatility
}


# ============================================================================
# DATA MODULE (merged)
# ============================================================================
def db_exists() -> bool:
    return os.path.exists(DB_PATH)


CSV_POSTS = "data_posts.csv"
CSV_VOLUME = "data_volume.csv"
CSV_ALERTS = "data_alerts.csv"

@st.cache_data(ttl=1, show_spinner=False)
def load_posts(limit: int = 1000) -> pd.DataFrame:
    if not os.path.exists(CSV_POSTS):
        return pd.DataFrame()
    try:
        df = pd.read_csv(CSV_POSTS, encoding="utf-8")
        if df.empty:
            return df
        df["timestamp_dt"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0)
        df["comments"] = pd.to_numeric(df["comments"], errors="coerce").fillna(0)
        df["sentiment"] = pd.to_numeric(df["sentiment"], errors="coerce").fillna(0.0)
        df["sentiment_label"] = df["sentiment_label"].fillna("neutral")
        df["engagement"] = df["score"] + df["comments"] * 2
        return df.tail(limit)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=1, show_spinner=False)
def load_volume(limit_minutes: int = 60) -> pd.DataFrame:
    if not os.path.exists(CSV_VOLUME):
        return pd.DataFrame()
    try:
        df = pd.read_csv(CSV_VOLUME, encoding="utf-8")
        if df.empty:
            return df
        df["minute_dt"] = pd.to_datetime(df["minute"], errors="coerce", format="%Y-%m-%d %H:%M")
        df["post_count"] = pd.to_numeric(df["post_count"], errors="coerce").fillna(0)
        return df.tail(limit_minutes)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=1, show_spinner=False)
def load_alerts(limit: int = 25) -> pd.DataFrame:
    if not os.path.exists(CSV_ALERTS):
        return pd.DataFrame()
    try:
        df = pd.read_csv(CSV_ALERTS, encoding="utf-8")
        if df.empty:
            return df
        df["timestamp_dt"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df["post_count"] = pd.to_numeric(df["post_count"], errors="coerce").fillna(0)
        df["rolling_avg"] = pd.to_numeric(df["rolling_avg"], errors="coerce").fillna(0.0)
        return df.tail(limit)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=1, show_spinner=False)
def load_sentiment_summary() -> pd.DataFrame:
    df = load_posts()
    if df.empty:
        return pd.DataFrame()
    return df.groupby("sentiment_label").size().reset_index(name="count")


@st.cache_data(ttl=1, show_spinner=False)
def load_table_counts() -> dict:
    posts = load_posts()
    volume = load_volume()
    alerts = load_alerts()
    return {
        "posts": len(posts),
        "volume_per_minute": len(volume),
        "anomaly_alerts": len(alerts)
    }


@st.cache_data(ttl=1, show_spinner=False)
def load_null_quality() -> dict:
    df = load_posts()
    if df.empty:
        return {"total": 0, "missing_title": 0, "missing_sentiment": 0, "missing_score": 0}
    return {
        "total": len(df),
        "missing_title": int(df["title"].isna().sum()),
        "missing_sentiment": int(df["sentiment_label"].isna().sum()),
        "missing_score": int(df["score"].isna().sum())
    }


def clear_all_caches() -> None:
    load_posts.clear()
    load_volume.clear()
    load_alerts.clear()
    load_sentiment_summary.clear()
    load_table_counts.clear()
    load_null_quality.clear()


def db_exists() -> bool:
    return os.path.exists(CSV_POSTS)


def clear_all_caches() -> None:
    load_posts.clear()
    load_volume.clear()
    load_alerts.clear()
    load_sentiment_summary.clear()
    load_table_counts.clear()
    load_null_quality.clear()


# ============================================================================
# ANALYTICS MODULE (merged)
# ============================================================================
STOPWORDS = {
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "is", "are", "was", "be", "it", "this", "that", "by",
    "from", "as", "not", "how", "why", "what", "i", "you", "we", "they",
    "he", "she", "my", "your", "its", "has", "have", "had", "do", "does",
    "did", "will", "would", "could", "should", "about", "after", "new",
    "use", "using", "than", "then", "into", "over", "can", "vs", "via",
    "up", "out", "if", "when", "where", "who", "which", "all", "one",
    "get", "make", "made", "just", "now", "still", "also", "off", "part",
}


# --------------------------------------------------------------------------
# Sentiment summary helpers
# --------------------------------------------------------------------------

@dataclass
class SentimentBreakdown:
    positive: int = 0
    negative: int = 0
    neutral: int = 0

    @property
    def total(self) -> int:
        return self.positive + self.negative + self.neutral

    @property
    def positive_pct(self) -> float:
        return round(self.positive / self.total * 100, 1) if self.total else 0.0

    @property
    def negative_pct(self) -> float:
        return round(self.negative / self.total * 100, 1) if self.total else 0.0

    @property
    def neutral_pct(self) -> float:
        return round(self.neutral / self.total * 100, 1) if self.total else 0.0

    @property
    def index(self) -> float:
        """A -100..+100 'mood index': positive share minus negative share."""
        if self.total == 0:
            return 0.0
        return round((self.positive - self.negative) / self.total * 100, 1)


def sentiment_breakdown(sentiment_df: pd.DataFrame) -> SentimentBreakdown:
    if sentiment_df is None or sentiment_df.empty:
        return SentimentBreakdown()
    lookup = dict(zip(sentiment_df["sentiment_label"], sentiment_df["count"]))
    return SentimentBreakdown(
        positive=int(lookup.get("positive", 0)),
        negative=int(lookup.get("negative", 0)),
        neutral=int(lookup.get("neutral", 0)),
    )


# --------------------------------------------------------------------------
# Volume / velocity / momentum
# --------------------------------------------------------------------------

def current_velocity(volume_df: pd.DataFrame, window: int = 5) -> float:
    """Average posts/minute over the most recent `window` buckets."""
    if volume_df is None or volume_df.empty:
        return 0.0
    recent = volume_df.tail(window)
    return round(float(recent["post_count"].mean()), 2)


def momentum_pct(volume_df: pd.DataFrame, window: int = 5) -> float | None:
    """
    % change of the most recent bucket vs. the average of the preceding
    `window` buckets. Returns None when there isn't enough history yet.
    """
    if volume_df is None or len(volume_df) < window + 1:
        return None
    recent = volume_df.iloc[-1]["post_count"]
    baseline = volume_df.iloc[-(window + 1):-1]["post_count"].mean()
    if baseline == 0:
        return None
    return round((recent - baseline) / baseline * 100, 1)


def zscore_anomalies(volume_df: pd.DataFrame, threshold: float = ZSCORE_ANOMALY_THRESHOLD) -> pd.DataFrame:
    """
    Independent statistical anomaly overlay (separate from the
    processor's simple 3x-rolling-average rule): flags buckets whose
    post_count is more than `threshold` standard deviations from the
    mean of the window.
    """
    if volume_df is None or len(volume_df) < 4:
        return pd.DataFrame(columns=["minute", "minute_dt", "post_count", "zscore"])
    df = volume_df.copy()
    mean = df["post_count"].mean()
    std = df["post_count"].std(ddof=0)
    if not std or np.isnan(std) or std == 0:
        df["zscore"] = 0.0
    else:
        df["zscore"] = (df["post_count"] - mean) / std
    return df[df["zscore"].abs() >= threshold]


# --------------------------------------------------------------------------
# Composite Pulse Score
# --------------------------------------------------------------------------

def pulse_score(
    velocity: float,
    sentiment_idx: float,
    anomaly_count: int,
    momentum: float | None,
    max_expected_velocity: float = 15.0,
) -> int:
    """
    A single 0-100 index answering "how much is the community buzzing,
    and how does it feel about it?" Blends:
      - velocity   (40%): normalized posts/minute, capped
      - sentiment  (35%): mood index rescaled from [-100,100] to [0,100]
      - stability  (25%): penalized by anomaly count and negative momentum swings
    """
    velocity_component = min(velocity / max_expected_velocity, 1.0) * 100
    sentiment_component = (sentiment_idx + 100) / 2  # rescale -100..100 -> 0..100
    stability_component = 100 - min(anomaly_count * 12, 60)
    if momentum is not None and momentum < -40:
        stability_component -= 15

    score = (
        velocity_component * PULSE_WEIGHTS["velocity"]
        + sentiment_component * PULSE_WEIGHTS["sentiment"]
        + max(stability_component, 0) * PULSE_WEIGHTS["stability"]
    )
    return int(round(max(0, min(100, score))))


def pulse_label(score: int) -> tuple[str, str]:
    """Returns (label, color-key) for a pulse score."""
    if score >= 75:
        return "Surging", "positive"
    if score >= 55:
        return "Active", "neutral"
    if score >= 35:
        return "Steady", "warning"
    return "Quiet", "negative"


# --------------------------------------------------------------------------
# Rolling sentiment over time
# --------------------------------------------------------------------------

def rolling_sentiment(posts_df: pd.DataFrame, window: int = 8) -> pd.DataFrame:
    if posts_df is None or posts_df.empty or "timestamp_dt" not in posts_df:
        return pd.DataFrame(columns=["timestamp_dt", "sentiment", "rolling"])
    df = posts_df.dropna(subset=["timestamp_dt"]).sort_values("timestamp_dt").copy()
    if df.empty:
        return pd.DataFrame(columns=["timestamp_dt", "sentiment", "rolling"])
    df["rolling"] = df["sentiment"].rolling(window=window, min_periods=1).mean()
    return df[["timestamp_dt", "sentiment", "rolling", "title"]]


# --------------------------------------------------------------------------
# Trending terms
# --------------------------------------------------------------------------

def top_terms(titles: pd.Series, top_n: int = 12, min_len: int = 3) -> list[tuple[str, int]]:
    counter: Counter = Counter()
    for title in titles.dropna():
        for word in re.findall(rf"\b[a-zA-Z]{{{min_len},}}\b", str(title).lower()):
            if word not in STOPWORDS:
                counter[word] += 1
    return counter.most_common(top_n)


# --------------------------------------------------------------------------
# Engagement
# --------------------------------------------------------------------------

def top_engaging_posts(posts_df: pd.DataFrame, n: int = 8) -> pd.DataFrame:
    if posts_df is None or posts_df.empty:
        return pd.DataFrame()
    return posts_df.sort_values("engagement", ascending=False).head(n)


def average_engagement(posts_df: pd.DataFrame) -> float:
    if posts_df is None or posts_df.empty:
        return 0.0
    return round(float(posts_df["score"].mean() + posts_df["comments"].mean()), 1)


# --------------------------------------------------------------------------
# Pipeline freshness / health
# --------------------------------------------------------------------------

@dataclass
class PipelineStatus:
    state: str          # "LIVE" | "IDLE" | "OFFLINE" | "NO DATA"
    color_key: str       # maps to config.COLORS
    minutes_since_last: float | None
    last_timestamp: str | None


def pipeline_status(posts_df: pd.DataFrame) -> PipelineStatus:
    if posts_df is None or posts_df.empty or "timestamp_dt" not in posts_df:
        return PipelineStatus("NO DATA", "offline", None, None)
    valid = posts_df.dropna(subset=["timestamp_dt"])
    if valid.empty:
        return PipelineStatus("NO DATA", "offline", None, None)
    latest = valid["timestamp_dt"].max()
    delta_minutes = (datetime.now() - latest.to_pydatetime()).total_seconds() / 60
    if delta_minutes <= FRESHNESS_LIVE_MINUTES:
        return PipelineStatus("LIVE", "live", round(delta_minutes, 1), str(latest))
    if delta_minutes <= FRESHNESS_IDLE_MINUTES:
        return PipelineStatus("IDLE", "idle", round(delta_minutes, 1), str(latest))
    return PipelineStatus("OFFLINE", "offline", round(delta_minutes, 1), str(latest))


def hourly_activity(posts_df: pd.DataFrame) -> pd.DataFrame:
    """Posts bucketed by hour-of-day and weekday for a heatmap. Returns an
    empty frame if there isn't enough time spread to be meaningful."""
    if posts_df is None or posts_df.empty or "timestamp_dt" not in posts_df:
        return pd.DataFrame()
    df = posts_df.dropna(subset=["timestamp_dt"]).copy()
    if df.empty:
        return pd.DataFrame()
    df["hour"] = df["timestamp_dt"].dt.hour
    df["weekday"] = df["timestamp_dt"].dt.day_name()
    pivot = df.pivot_table(index="weekday", columns="hour", values="id", aggfunc="count", fill_value=0)
    order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    pivot = pivot.reindex([d for d in order if d in pivot.index])
    return pivot


# ============================================================================
# THEME MODULE (merged)
# ============================================================================
CUSTOM_CSS = f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap');

:root {{
    --bg: {COLORS['bg']};
    --bg-alt: {COLORS['bg_alt']};
    --surface: {COLORS['surface']};
    --surface-alt: {COLORS['surface_alt']};
    --border: {COLORS['border']};
    --border-strong: {COLORS['border_strong']};
    --text-primary: {COLORS['text_primary']};
    --text-secondary: {COLORS['text_secondary']};
    --text-muted: {COLORS['text_muted']};
    --brand-start: {COLORS['brand_start']};
    --brand-mid: {COLORS['brand_mid']};
    --brand-end: {COLORS['brand_end']};
    --positive: {COLORS['positive']};
    --negative: {COLORS['negative']};
    --neutral: {COLORS['neutral']};
    --warning: {COLORS['warning']};
}}

/* ---------------------------------------------------------------- */
/* Global surface                                                    */
/* ---------------------------------------------------------------- */
html, body, [class*="css"] {{
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
}}

.stApp {{
    background:
        radial-gradient(ellipse 900px 500px at 8% -10%, rgba(99,102,241,0.16), transparent 60%),
        radial-gradient(ellipse 800px 500px at 100% 0%, rgba(34,211,238,0.10), transparent 55%),
        radial-gradient(ellipse 700px 600px at 50% 100%, rgba(139,92,246,0.08), transparent 55%),
        var(--bg);
}}

.block-container {{
    padding-top: 1.4rem;
    padding-bottom: 2rem;
    max-width: 1500px;
}}

::-webkit-scrollbar {{ width: 8px; height: 8px; }}
::-webkit-scrollbar-track {{ background: transparent; }}
::-webkit-scrollbar-thumb {{
    background: linear-gradient(var(--brand-start), var(--brand-end));
    border-radius: 8px;
}}

/* ---------------------------------------------------------------- */
/* Header / hero                                                     */
/* ---------------------------------------------------------------- */
.pl-hero {{
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    flex-wrap: wrap;
    gap: 12px;
    margin-bottom: 4px;
}}

.pl-title-row {{
    display: flex;
    align-items: center;
    gap: 14px;
}}

.pl-logo-mark {{
    width: 46px; height: 46px;
    border-radius: 13px;
    display: flex; align-items: center; justify-content: center;
    font-size: 22px;
    background: linear-gradient(135deg, var(--brand-start), var(--brand-mid) 55%, var(--brand-end));
    box-shadow: 0 8px 24px -6px rgba(99,102,241,0.55);
}}

.pl-title {{
    font-size: 2.15rem;
    font-weight: 800;
    letter-spacing: -0.03em;
    background: linear-gradient(90deg, #ffffff 15%, var(--brand-end) 120%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    line-height: 1.1;
    margin: 0;
}}

.pl-subtitle {{
    color: var(--text-secondary);
    font-size: 0.92rem;
    margin-top: 2px;
    font-weight: 500;
}}

.pl-meta-panel {{
    text-align: right;
    padding-top: 4px;
}}

.pl-clock {{
    font-family: 'JetBrains Mono', monospace;
    color: var(--text-primary);
    font-size: 0.95rem;
    font-weight: 600;
}}

.pl-clock-label {{
    color: var(--text-muted);
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}}

/* ---------------------------------------------------------------- */
/* Status badge / live pulse dot                                     */
/* ---------------------------------------------------------------- */
.pl-status-badge {{
    display: inline-flex;
    align-items: center;
    gap: 7px;
    padding: 5px 12px 5px 9px;
    border-radius: 999px;
    font-size: 0.75rem;
    font-weight: 700;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    border: 1px solid var(--border-strong);
    background: rgba(255,255,255,0.03);
}}

.pl-dot {{
    width: 8px; height: 8px;
    border-radius: 50%;
    flex-shrink: 0;
    box-shadow: 0 0 0 0 currentColor;
    animation: pl-pulse 1.8s infinite;
}}

@keyframes pl-pulse {{
    0%   {{ box-shadow: 0 0 0 0 rgba(34,197,94,0.55); }}
    70%  {{ box-shadow: 0 0 0 8px rgba(34,197,94,0); }}
    100% {{ box-shadow: 0 0 0 0 rgba(34,197,94,0); }}
}}

.pl-dot.live    {{ background: var(--positive); color: var(--positive); }}
.pl-dot.idle    {{ background: var(--warning); color: var(--warning); animation: none; }}
.pl-dot.offline {{ background: var(--negative); color: var(--negative); animation: none; }}

.pl-status-badge.live    {{ color: var(--positive); border-color: rgba(34,197,94,0.35); }}
.pl-status-badge.idle    {{ color: var(--warning); border-color: rgba(245,158,11,0.35); }}
.pl-status-badge.offline {{ color: var(--negative); border-color: rgba(244,63,94,0.35); }}

/* ---------------------------------------------------------------- */
/* KPI cards                                                          */
/* ---------------------------------------------------------------- */
.pl-kpi-grid {{
    display: grid;
    grid-template-columns: repeat(6, 1fr);
    gap: 12px;
    margin: 14px 0 6px 0;
}}

@media (max-width: 1100px) {{
    .pl-kpi-grid {{ grid-template-columns: repeat(3, 1fr); }}
}}

.pl-kpi-card {{
    position: relative;
    background: linear-gradient(160deg, var(--surface), var(--surface-alt));
    border: 1px solid var(--border);
    border-radius: 16px;
    padding: 16px 16px 14px 16px;
    overflow: hidden;
    transition: transform 0.18s ease, border-color 0.18s ease;
}}

.pl-kpi-card:hover {{
    transform: translateY(-3px);
    border-color: var(--border-strong);
}}

.pl-kpi-card::before {{
    content: "";
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    background: linear-gradient(90deg, var(--brand-start), var(--brand-end));
    opacity: 0.85;
}}

.pl-kpi-icon {{
    font-size: 1.05rem;
    opacity: 0.9;
    margin-bottom: 6px;
}}

.pl-kpi-label {{
    color: var(--text-secondary);
    font-size: 0.72rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-bottom: 6px;
}}

.pl-kpi-value {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 1.65rem;
    font-weight: 700;
    color: var(--text-primary);
    line-height: 1.1;
}}

.pl-kpi-delta {{
    margin-top: 6px;
    font-size: 0.76rem;
    font-weight: 600;
    display: inline-flex;
    align-items: center;
    gap: 4px;
}}

.pl-kpi-delta.up {{ color: var(--positive); }}
.pl-kpi-delta.down {{ color: var(--negative); }}
.pl-kpi-delta.flat {{ color: var(--text-muted); }}

/* ---------------------------------------------------------------- */
/* Section headers                                                    */
/* ---------------------------------------------------------------- */
.pl-section-title {{
    font-size: 1.02rem;
    font-weight: 700;
    color: var(--text-primary);
    display: flex;
    align-items: center;
    gap: 8px;
    margin: 6px 0 10px 0;
}}

.pl-section-sub {{
    color: var(--text-muted);
    font-size: 0.78rem;
    font-weight: 500;
    margin-top: -6px;
    margin-bottom: 10px;
}}

/* ---------------------------------------------------------------- */
/* Post / alert / leaderboard cards                                   */
/* ---------------------------------------------------------------- */
.pl-post-card {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-left: 3px solid var(--neutral);
    border-radius: 10px;
    padding: 11px 14px;
    margin-bottom: 8px;
    transition: border-color 0.15s ease, background 0.15s ease;
}}
.pl-post-card:hover {{ background: var(--surface-alt); }}
.pl-post-card.positive {{ border-left-color: var(--positive); }}
.pl-post-card.negative {{ border-left-color: var(--negative); }}
.pl-post-card.neutral  {{ border-left-color: var(--neutral); }}

.pl-post-title {{
    color: var(--text-primary);
    font-size: 0.92rem;
    font-weight: 600;
    line-height: 1.35;
}}

.pl-post-meta {{
    margin-top: 5px;
    color: var(--text-muted);
    font-size: 0.76rem;
    font-family: 'JetBrains Mono', monospace;
    display: flex;
    gap: 14px;
    flex-wrap: wrap;
}}

.pl-chip {{
    display: inline-block;
    padding: 1px 8px;
    border-radius: 6px;
    font-size: 0.68rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}}
.pl-chip.positive {{ background: {COLORS['positive_soft']}; color: var(--positive); }}
.pl-chip.negative {{ background: {COLORS['negative_soft']}; color: var(--negative); }}
.pl-chip.neutral  {{ background: {COLORS['neutral_soft']}; color: var(--neutral); }}

.pl-alert-card {{
    background: linear-gradient(135deg, rgba(244,63,94,0.14), rgba(244,63,94,0.04));
    border: 1px solid rgba(244,63,94,0.3);
    border-radius: 10px;
    padding: 12px 16px;
    margin-bottom: 8px;
    color: var(--text-primary);
    font-size: 0.85rem;
}}

.pl-rank-badge {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 22px; height: 22px;
    border-radius: 6px;
    font-size: 0.72rem;
    font-weight: 800;
    background: rgba(255,255,255,0.06);
    color: var(--text-secondary);
    margin-right: 8px;
    font-family: 'JetBrains Mono', monospace;
}}

/* ---------------------------------------------------------------- */
/* Pipeline health cards                                              */
/* ---------------------------------------------------------------- */
.pl-pipe-card {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 14px 16px;
    text-align: center;
}}
.pl-pipe-card .pl-pipe-icon {{ font-size: 1.4rem; margin-bottom: 6px; }}
.pl-pipe-card .pl-pipe-name {{ font-size: 0.8rem; font-weight: 700; color: var(--text-primary); }}
.pl-pipe-card .pl-pipe-status {{ font-size: 0.7rem; color: var(--text-muted); margin-top: 3px; text-transform: uppercase; letter-spacing: 0.05em;}}

.pl-diagram {{
    background: var(--bg-alt);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 18px;
    font-family: 'JetBrains Mono', monospace;
    color: var(--text-secondary);
    font-size: 0.82rem;
    line-height: 2;
    overflow-x: auto;
    white-space: pre;
}}

/* ---------------------------------------------------------------- */
/* Native widget re-skin                                              */
/* ---------------------------------------------------------------- */
section[data-testid="stSidebar"] {{
    background: linear-gradient(180deg, var(--bg-alt), var(--bg));
    border-right: 1px solid var(--border);
}}

div[data-testid="stMetric"] {{
    background: var(--surface);
    border-radius: 12px;
    padding: 14px 16px;
    border: 1px solid var(--border);
}}
div[data-testid="stMetric"] label {{ color: var(--text-secondary) !important; }}

button[data-baseweb="tab"] {{
    font-weight: 600;
    color: var(--text-secondary);
}}
button[data-baseweb="tab"][aria-selected="true"] {{
    color: var(--text-primary) !important;
}}
div[data-baseweb="tab-highlight"] {{
    background: linear-gradient(90deg, var(--brand-start), var(--brand-end)) !important;
}}
div[data-baseweb="tab-border"] {{ background: var(--border) !important; }}

.stButton > button {{
    background: linear-gradient(135deg, var(--brand-start), var(--brand-mid));
    color: white;
    border: none;
    border-radius: 10px;
    font-weight: 600;
    padding: 0.45rem 1rem;
    transition: opacity 0.15s ease;
}}
.stButton > button:hover {{ opacity: 0.88; color: white; }}

hr {{ border-color: var(--border) !important; margin: 1.1rem 0; }}

.pl-footer {{
    text-align: center;
    color: var(--text-muted);
    font-size: 0.78rem;
    padding-top: 4px;
}}
.pl-footer a {{ color: var(--text-secondary); text-decoration: none; }}

.pl-empty-state {{
    text-align: center;
    padding: 50px 20px;
    color: var(--text-secondary);
}}
.pl-empty-state .pl-empty-emoji {{ font-size: 2.4rem; margin-bottom: 10px; }}
.pl-empty-state code {{
    background: var(--surface);
    border: 1px solid var(--border);
    padding: 3px 8px;
    border-radius: 6px;
    color: var(--brand-end);
}}
</style>
"""


def inject_css(st_module) -> None:
    st_module.markdown(CUSTOM_CSS, unsafe_allow_html=True)


# ============================================================================
# COMPONENTS MODULE (merged)
# ============================================================================
def status_badge(state: str, color_key: str) -> str:
    return _raw(f"""
    <div class="pl-status-badge {color_key}">
        <span class="pl-dot {color_key}"></span>{state}
    </div>
    """)


def kpi_card(icon: str, label: str, value: str, delta: str | None = None, direction: str = "flat") -> str:
    delta_html = ""
    if delta:
        arrow = {"up": "▲", "down": "▼", "flat": "•"}.get(direction, "•")
        delta_html = f'<div class="pl-kpi-delta {direction}">{arrow} {delta}</div>'
    return _raw(f"""
    <div class="pl-kpi-card">
        <div class="pl-kpi-icon">{icon}</div>
        <div class="pl-kpi-label">{label}</div>
        <div class="pl-kpi-value">{value}</div>
        {delta_html}
    </div>
    """)


def section_title(icon: str, title: str, subtitle: str | None = None) -> str:
    sub = f'<div class="pl-section-sub">{subtitle}</div>' if subtitle else ""
    return f'<div class="pl-section-title">{icon} {title}</div>{sub}'


def post_card(title: str, score: int, comments: int, sentiment: float, label: str, timestamp: str) -> str:
    emoji = SENTIMENT_EMOJI.get(label, "🔵")
    short_title = title if len(title) <= 100 else title[:97] + "…"
    ts_display = timestamp[:16].replace("T", " ") if timestamp else "—"
    return _raw(f"""
    <div class="pl-post-card {label}">
        <div class="pl-post-title">{emoji} {short_title}</div>
        <div class="pl-post-meta">
            <span>⬆ {int(score)} pts</span>
            <span>💬 {int(comments)}</span>
            <span>🕐 {ts_display}</span>
            <span class="pl-chip {label}">{label}</span>
            <span>score {sentiment:+.2f}</span>
        </div>
    </div>
    """)


def leaderboard_row(rank: int, title: str, score: int, comments: int, label: str) -> str:
    emoji = SENTIMENT_EMOJI.get(label, "🔵")
    short_title = title if len(title) <= 78 else title[:75] + "…"
    return _raw(f"""
    <div class="pl-post-card {label}">
        <span class="pl-rank-badge">{rank}</span>
        <span class="pl-post-title">{emoji} {short_title}</span>
        <div class="pl-post-meta">
            <span>⬆ {int(score)} pts</span>
            <span>💬 {int(comments)}</span>
        </div>
    </div>
    """)


def alert_card(timestamp: str, post_count: int, rolling_avg: float) -> str:
    ts_display = str(timestamp)[:19].replace("T", " ")
    ratio = round(post_count / rolling_avg, 1) if rolling_avg else 0
    return _raw(f"""
    <div class="pl-alert-card">
        🚨 <b>Volume spike detected</b> at <code>{ts_display}</code><br>
        Observed <b>{int(post_count)}</b> posts vs a rolling average of <b>{rolling_avg:.1f}</b>
        &nbsp;(<b>{ratio}×</b> normal)
    </div>
    """)


def pipeline_card(icon: str, name: str, status: str) -> str:
    return _raw(f"""
    <div class="pl-pipe-card">
        <div class="pl-pipe-icon">{icon}</div>
        <div class="pl-pipe-name">{name}</div>
        <div class="pl-pipe-status">{status}</div>
    </div>
    """)


def empty_state(emoji: str, title: str, body_html: str) -> str:
    return _raw(f"""
    <div class="pl-empty-state">
        <div class="pl-empty-emoji">{emoji}</div>
        <h3>{title}</h3>
        <p>{body_html}</p>
    </div>
    """)
# ==========================================================================
# Page setup
# ==========================================================================

st.set_page_config(
    page_title=f"{APP_NAME} · Live Intelligence",
    page_icon=APP_ICON,
    layout="wide",
    initial_sidebar_state="expanded",
)
inject_css(st)

# ==========================================================================
# Session state defaults
# ==========================================================================

defaults = {
    "auto_refresh": True,
    "refresh_seconds": DEFAULT_REFRESH_SECONDS,
    "sentiment_filter": ["positive", "negative", "neutral"],
    "search_query": "",
    "min_score": 0,
    "sort_mode": "Newest",
    "post_limit": 400,
}
for key, val in defaults.items():
    st.session_state.setdefault(key, val)


# ==========================================================================
# Plotly base layout helper — keeps every chart visually consistent
# ==========================================================================

def apply_chart_theme(fig: go.Figure, height: int = 320, showlegend: bool = False) -> go.Figure:
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family=CHART_FONT, color=COLORS["text_secondary"], size=12),
        margin=dict(l=6, r=6, t=28, b=6),
        height=height,
        showlegend=showlegend,
        legend=dict(font=dict(color=COLORS["text_secondary"]), orientation="h", y=-0.18),
        hoverlabel=dict(bgcolor=COLORS["surface"], font_color=COLORS["text_primary"], bordercolor=COLORS["border"]),
    )
    fig.update_xaxes(showgrid=False, color=COLORS["text_secondary"], zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor=COLORS["border"], color=COLORS["text_secondary"], zeroline=False)
    return fig


# ==========================================================================
# Sidebar — controls, filters, about
# ==========================================================================

with st.sidebar:
    st.markdown(
        _raw(f"""
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:2px;">
            <div class="pl-logo-mark">{APP_ICON}</div>
            <div>
                <div style="font-weight:800;font-size:1.15rem;color:{COLORS['text_primary']};">{APP_NAME}</div>
                <div style="font-size:0.72rem;color:{COLORS['text_muted']};">Control Center</div>
            </div>
        </div>
        """),
        unsafe_allow_html=True,
    )
    st.divider()

    st.markdown("**⏱ Refresh**")
    st.session_state.auto_refresh = st.toggle("Auto-refresh", value=st.session_state.auto_refresh)
    st.session_state.refresh_seconds = st.slider(
        "Interval (seconds)",
        min_value=MIN_REFRESH_SECONDS,
        max_value=MAX_REFRESH_SECONDS,
        value=st.session_state.refresh_seconds,
        step=5,
        disabled=not st.session_state.auto_refresh,
    )
    if st.button("🔄 Refresh now", use_container_width=True):
        clear_all_caches()
        st.rerun()

    st.divider()
    st.markdown("**🔍 Filters**")
    st.session_state.sentiment_filter = st.multiselect(
        "Sentiment",
        options=["positive", "negative", "neutral"],
        default=st.session_state.sentiment_filter,
        format_func=lambda x: x.capitalize(),
    )
    st.session_state.search_query = st.text_input("Search titles", value=st.session_state.search_query, placeholder="e.g. rust, llm, security…")
    st.session_state.min_score = st.slider("Minimum HN score", 0, 500, st.session_state.min_score, step=10)
    st.session_state.sort_mode = st.selectbox(
        "Sort live feed by",
        ["Newest", "Highest score", "Most comments", "Most positive", "Most negative"],
        index=["Newest", "Highest score", "Most comments", "Most positive", "Most negative"].index(st.session_state.sort_mode),
    )
    st.session_state.post_limit = st.slider("Rows to analyze", 100, 1000, st.session_state.post_limit, step=100)

    st.divider()
    with st.expander("🏗️ Architecture", expanded=False):
        st.markdown(" → ".join(PIPELINE))
        st.caption(f"Source: {SOURCE_LABEL}  ·  Storage: DuckDB (`{DB_PATH}`)")

    with st.expander("ℹ️ About PulseLite", expanded=False):
        st.markdown(
            _raw(
                """
                PulseLite streams live Hacker News activity through Kafka,
                scores sentiment with VADER, and lands aggregates in DuckDB.
                This dashboard adds a derived-intelligence layer on top:
                a composite **Pulse Score**, momentum tracking, and an
                independent statistical anomaly overlay.
                """
            )
        )
        st.caption(AUTHOR)



# ==========================================================================
# Live section — everything here re-renders on every auto-refresh tick
# without reloading the whole page (via st.fragment), so KPI numbers,
# charts, and the clock genuinely update in place.
# ==========================================================================

_FRAGMENT = getattr(st, "fragment", None) or getattr(st, "experimental_fragment", None)


def _render_live_dashboard() -> None:
    # ==========================================================================
    # Load data
    # ==========================================================================
    st.sidebar.success(f"Refresh: {datetime.now().strftime('%H:%M:%S')}")
    st.sidebar.write(datetime.now())
    posts_all = load_posts(limit=st.session_state.post_limit)
    volume_df = load_volume(limit_minutes=90)
    alerts_df = load_alerts(limit=25)
    sentiment_summary = load_sentiment_summary()
    table_counts = load_table_counts()

    has_data = not posts_all.empty

    # Apply sidebar filters for feed-facing views (KPIs stay on the full set
    # so the headline numbers reflect the whole pipeline, not just the filter)
    filtered_posts = posts_all.copy()
    if has_data:
        if st.session_state.sentiment_filter:
            filtered_posts = filtered_posts[filtered_posts["sentiment_label"].isin(st.session_state.sentiment_filter)]
        if st.session_state.search_query:
            q = st.session_state.search_query.lower()
            filtered_posts = filtered_posts[filtered_posts["title"].str.lower().str.contains(q, na=False)]
        if st.session_state.min_score:
            filtered_posts = filtered_posts[filtered_posts["score"] >= st.session_state.min_score]

        sort_map = {
            "Newest": ("timestamp_dt", False),
            "Highest score": ("score", False),
            "Most comments": ("comments", False),
            "Most positive": ("sentiment", False),
            "Most negative": ("sentiment", True),
        }
        sort_col, ascending = sort_map[st.session_state.sort_mode]
        filtered_posts = filtered_posts.sort_values(sort_col, ascending=ascending)


    # ==========================================================================
    # Derived analytics
    # ==========================================================================

    breakdown = sentiment_breakdown(sentiment_summary)
    velocity = current_velocity(volume_df)
    momentum = momentum_pct(volume_df)
    zscore_flags = zscore_anomalies(volume_df)
    score = pulse_score(velocity, breakdown.index, len(alerts_df), momentum)
    score_label, score_color = pulse_label(score)
    status = pipeline_status(posts_all)
    avg_engagement = average_engagement(posts_all)


    # ==========================================================================
    # Header
    # ==========================================================================

    hdr_left, hdr_right = st.columns([3, 1])
    with hdr_left:
        st.markdown(
            _raw(f"""
            <div class="pl-title-row">
                <div class="pl-logo-mark">{APP_ICON}</div>
                <div>
                    <p class="pl-title">{APP_NAME}</p>
                    <p class="pl-subtitle">{APP_TAGLINE} &nbsp;·&nbsp; {SOURCE_LABEL}</p>
                </div>
            </div>
            """),
            unsafe_allow_html=True,
        )
    with hdr_right:
        st.markdown(
            _raw(f"""
            <div class="pl-meta-panel">
                <div style="margin-bottom:6px;">{status_badge(status.state, status.color_key)}</div>
                <div class="pl-clock">{datetime.now().strftime('%H:%M:%S')}</div>
                <div class="pl-clock-label">{datetime.now().strftime('%d %b %Y')}</div>
            </div>
            """),
            unsafe_allow_html=True,
        )

    st.divider()


    # ==========================================================================
    # Empty state (fresh clone / pipeline not started)
    # ==========================================================================

    if not has_data:
        st.markdown(
            empty_state(
                "⏳",
                "Waiting for data…",
                "Start the producer and processor, then this dashboard will come alive automatically."
                "<br><br><code>python producer/reddit_producer.py</code>"
                "<br><code>python processor/spark_processor.py</code>",
            ),
            unsafe_allow_html=True,
        )
        st.stop()


    # ==========================================================================
    # KPI row
    # ==========================================================================

    delta_dir = "up" if (momentum or 0) > 0 else ("down" if (momentum or 0) < 0 else "flat")
    momentum_text = f"{momentum:+.1f}% vs prior window" if momentum is not None else "warming up…"

    kpi_html = "".join(
        [
            kpi_card("📊", "Total Posts Tracked", f"{table_counts.get('posts', 0):,}"),
            kpi_card("⚡", "Velocity", f"{velocity:g}/min", momentum_text, delta_dir),
            kpi_card("💓", "Pulse Score", f"{score}/100", score_label, score_color if score_color != "warning" else "flat"),
            kpi_card("🎭", "Sentiment Index", f"{breakdown.index:+.0f}", f"{breakdown.positive_pct}% positive", "up" if breakdown.index > 0 else "down" if breakdown.index < 0 else "flat"),
            kpi_card("🚨", "Active Anomalies", f"{len(alerts_df)}", f"{len(zscore_flags)} statistical flags", "down" if len(alerts_df) else "flat"),
            kpi_card("🔥", "Avg Engagement", f"{avg_engagement:g}", "score + comments", "flat"),
        ]
    )
    st.markdown(f'<div class="pl-kpi-grid">{kpi_html}</div>', unsafe_allow_html=True)

    st.write("")


    # ==========================================================================
    # Tabs
    # ==========================================================================

    tab_overview, tab_feed, tab_trends, tab_anomalies, tab_pipeline = st.tabs(
        ["📊 Overview", "📰 Live Feed", "📈 Trends & Analytics", "🚨 Anomalies", "⚙️ Pipeline Health"]
    )

    # --------------------------------------------------------------------------
    # TAB 1 — Overview
    # --------------------------------------------------------------------------
    with tab_overview:
        col_vol, col_sent = st.columns([2, 1])

        with col_vol:
            st.markdown(section_title("📈", "Post Volume Timeline", "Posts per minute, with anomaly markers overlaid"), unsafe_allow_html=True)
            if len(volume_df) > 0:
                fig = go.Figure()
                fig.add_trace(
                    go.Scatter(
                        x=volume_df["minute_dt"],
                        y=volume_df["post_count"],
                        mode="lines",
                        name="Posts/min",
                        line=dict(color=COLORS["brand_end"], width=2.5, shape="spline"),
                        fill="tozeroy",
                        fillcolor="rgba(34,211,238,0.10)",
                    )
                )
                if len(volume_df) >= 3:
                    rolling = volume_df["post_count"].rolling(window=min(5, len(volume_df)), min_periods=1).mean()
                    fig.add_trace(
                        go.Scatter(
                            x=volume_df["minute_dt"], y=rolling, mode="lines", name="Rolling avg",
                            line=dict(color=COLORS["brand_mid"], width=1.5, dash="dot"),
                        )
                    )
                if len(zscore_flags) > 0:
                    fig.add_trace(
                        go.Scatter(
                            x=zscore_flags["minute_dt"], y=zscore_flags["post_count"], mode="markers", name="Anomaly",
                            marker=dict(size=11, color=COLORS["negative"], symbol="diamond", line=dict(width=1.5, color="white")),
                        )
                    )
                apply_chart_theme(fig, height=320, showlegend=True)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Accumulating volume buckets — check back in a minute.")

        with col_sent:
            st.markdown(section_title("🎭", "Sentiment Split"), unsafe_allow_html=True)
            fig2 = go.Figure(
                go.Pie(
                    labels=["Positive", "Negative", "Neutral"],
                    values=[breakdown.positive, breakdown.negative, breakdown.neutral],
                    hole=0.62,
                    marker=dict(colors=[COLORS["positive"], COLORS["negative"], COLORS["neutral"]], line=dict(color=COLORS["bg"], width=2)),
                    textinfo="percent",
                    textfont=dict(color="white", size=12),
                )
            )
            fig2.add_annotation(text=f"<b>{breakdown.index:+.0f}</b><br><span style='font-size:10px'>mood index</span>", showarrow=False, font=dict(color=COLORS["text_primary"], size=18))
            apply_chart_theme(fig2, height=300, showlegend=True)
            st.plotly_chart(fig2, use_container_width=True)

        st.markdown("<br>", unsafe_allow_html=True)
        col_words, col_gauge = st.columns([2, 1])

        with col_words:
            st.markdown(section_title("🔤", "Trending Terms", "Most frequent meaningful words across tracked titles"), unsafe_allow_html=True)
            terms = top_terms(posts_all["title"], top_n=12)
            if terms:
                words_df = pd.DataFrame(terms, columns=["word", "count"])
                fig3 = go.Figure(
                    go.Bar(
                        x=words_df["count"], y=words_df["word"], orientation="h",
                        marker=dict(color=words_df["count"], colorscale=[[0, COLORS["brand_start"]], [1, COLORS["brand_end"]]]),
                    )
                )
                fig3.update_layout(yaxis=dict(categoryorder="total ascending"))
                apply_chart_theme(fig3, height=360)
                st.plotly_chart(fig3, use_container_width=True)
            else:
                st.info("Not enough titles yet to surface trends.")

        with col_gauge:
            st.markdown(section_title("💓", "Pulse Score", "Composite: velocity + mood + stability"), unsafe_allow_html=True)
            gauge = go.Figure(
                go.Indicator(
                    mode="gauge+number",
                    value=score,
                    number=dict(suffix="", font=dict(color=COLORS["text_primary"], size=34)),
                    gauge=dict(
                        axis=dict(range=[0, 100], tickcolor=COLORS["text_muted"], tickfont=dict(color=COLORS["text_muted"], size=9)),
                        bar=dict(color=COLORS["brand_end"], thickness=0.28),
                        bgcolor="rgba(0,0,0,0)",
                        borderwidth=0,
                        steps=[
                            dict(range=[0, 35], color="rgba(244,63,94,0.18)"),
                            dict(range=[35, 55], color="rgba(245,158,11,0.18)"),
                            dict(range=[55, 75], color="rgba(96,165,250,0.18)"),
                            dict(range=[75, 100], color="rgba(34,197,94,0.18)"),
                        ],
                    ),
                )
            )
            apply_chart_theme(gauge, height=220)
            st.plotly_chart(gauge, use_container_width=True)
            st.caption(f"Status: **{score_label}** · velocity {velocity:g}/min · {breakdown.positive_pct}% positive mood")

    # --------------------------------------------------------------------------
    # TAB 2 — Live Feed
    # --------------------------------------------------------------------------
    with tab_feed:
        st.markdown(
            section_title("📰", "Live Feed", f"{len(filtered_posts)} posts match current filters · sorted by {st.session_state.sort_mode.lower()}"),
            unsafe_allow_html=True,
        )

        if filtered_posts.empty:
            st.markdown(empty_state("🔍", "No posts match these filters", "Try widening the sentiment filter, lowering the minimum score, or clearing your search."), unsafe_allow_html=True)
        else:
            page_size = 15
            total_pages = max(1, -(-len(filtered_posts) // page_size))
            page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1) if total_pages > 1 else 1
            start = (page - 1) * page_size
            page_df = filtered_posts.iloc[start:start + page_size]

            cards_html = "".join(
                post_card(row.title, row.score, row.comments, row.sentiment, row.sentiment_label, row.timestamp)
                for row in page_df.itertuples()
            )
            st.markdown(cards_html, unsafe_allow_html=True)

            csv = filtered_posts.drop(columns=["timestamp_dt", "engagement"], errors="ignore").to_csv(index=False).encode("utf-8")
            st.download_button("⬇️ Export filtered feed (CSV)", data=csv, file_name="pulselite_feed.csv", mime="text/csv")

    # --------------------------------------------------------------------------
    # TAB 3 — Trends & Analytics
    # --------------------------------------------------------------------------
    with tab_trends:
        col_a, col_b = st.columns(2)

        with col_a:
            st.markdown(section_title("📉", "Rolling Sentiment", "8-post rolling average, chronological"), unsafe_allow_html=True)
            roll = rolling_sentiment(posts_all)
            if not roll.empty:
                fig4 = go.Figure()
                fig4.add_trace(go.Scatter(x=roll["timestamp_dt"], y=roll["sentiment"], mode="markers", name="Post sentiment",
                                           marker=dict(size=5, color=COLORS["text_muted"], opacity=0.5)))
                fig4.add_trace(go.Scatter(x=roll["timestamp_dt"], y=roll["rolling"], mode="lines", name="Rolling avg",
                                           line=dict(color=COLORS["brand_end"], width=2.5)))
                fig4.add_hline(y=0, line_dash="dot", line_color=COLORS["text_muted"])
                apply_chart_theme(fig4, height=320, showlegend=True)
                st.plotly_chart(fig4, use_container_width=True)
            else:
                st.info("Need a few more timestamped posts to chart a trend.")

        with col_b:
            st.markdown(section_title("🎯", "Engagement Landscape", "Score vs. comments, colored by sentiment"), unsafe_allow_html=True)
            if not posts_all.empty:
                fig5 = go.Figure()
                for label, color in SENTIMENT_COLOR_MAP.items():
                    sub = posts_all[posts_all["sentiment_label"] == label]
                    if sub.empty:
                        continue
                    fig5.add_trace(
                        go.Scatter(
                            x=sub["score"], y=sub["comments"], mode="markers", name=label.capitalize(),
                            marker=dict(size=8, color=color, opacity=0.75, line=dict(width=1, color=COLORS["bg"])),
                            text=sub["title"], hovertemplate="%{text}<br>Score: %{x} · Comments: %{y}<extra></extra>",
                        )
                    )
                fig5.update_xaxes(title_text="HN Score")
                fig5.update_yaxes(title_text="Comments")
                apply_chart_theme(fig5, height=320, showlegend=True)
                st.plotly_chart(fig5, use_container_width=True)

        st.markdown("<br>", unsafe_allow_html=True)
        col_c, col_d = st.columns([1, 1])

        with col_c:
            st.markdown(section_title("🏆", "Top Engaging Posts", "Ranked by score + 2× comments"), unsafe_allow_html=True)
            leaders = top_engaging_posts(posts_all, n=6)
            if not leaders.empty:
                rows_html = "".join(
                    leaderboard_row(i + 1, row.title, row.score, row.comments, row.sentiment_label)
                    for i, row in enumerate(leaders.itertuples())
                )
                st.markdown(rows_html, unsafe_allow_html=True)
            else:
                st.info("No engagement data yet.")

        with col_d:
            st.markdown(section_title("🗓️", "Activity Heatmap", "Post volume by weekday & hour"), unsafe_allow_html=True)
            heat = hourly_activity(posts_all)
            if not heat.empty and heat.shape[1] > 1:
                fig6 = go.Figure(
                    go.Heatmap(
                        z=heat.values, x=[f"{h:02d}:00" for h in heat.columns], y=list(heat.index),
                        colorscale=[[0, COLORS["surface"]], [1, COLORS["brand_end"]]], showscale=False,
                    )
                )
                apply_chart_theme(fig6, height=320)
                st.plotly_chart(fig6, use_container_width=True)
            else:
                st.info("Heatmap needs activity spread across more hours — check back later.")

    # --------------------------------------------------------------------------
    # TAB 4 — Anomalies
    # --------------------------------------------------------------------------
        st.markdown('<br>', unsafe_allow_html=True)
        st.info('Drift detection: accumulating data - check back in 2 minutes.')

        st.markdown('<br>', unsafe_allow_html=True)
        st.markdown(section_title('🌊', 'Topic Drift Score', 'Cosine distance between consecutive 5-minute embedding centroids'), unsafe_allow_html=True)
        try:
            import pandas as _pd
            _drift = _pd.read_csv('data_drift.csv')
            if len(_drift) > 1:
                _drift['ts'] = _pd.to_datetime(_drift['timestamp'], errors='coerce')
                _latest_score = float(_drift['drift_score'].iloc[-1])
                _col1, _col2 = st.columns([2, 1])
                with _col1:
                    _fig_d = go.Figure()
                    _fig_d.add_trace(go.Scatter(
                        x=_drift['ts'], y=_drift['drift_score'],
                        mode='lines+markers',
                        line=dict(color=COLORS['brand_mid'], width=2.5),
                        fill='tozeroy', fillcolor='rgba(139,92,246,0.1)',
                        name='Drift Score'
                    ))
                    _fig_d.add_hline(y=0.3, line_dash='dot',
                        line_color=COLORS['negative'],
                        annotation_text='Threshold 0.3',
                        annotation_font_color=COLORS['negative'])
                    apply_chart_theme(_fig_d, height=280, showlegend=True)
                    st.plotly_chart(_fig_d, use_container_width=True)
                with _col2:
                    _gauge = go.Figure(go.Indicator(
                        mode='gauge+number',
                        value=round(_latest_score, 3),
                        number=dict(font=dict(color=COLORS['text_primary'], size=28)),
                        gauge=dict(
                            axis=dict(range=[0, 1]),
                            bar=dict(color=COLORS['brand_mid'] if _latest_score < 0.3 else COLORS['negative'], thickness=0.3),
                            bgcolor='rgba(0,0,0,0)', borderwidth=0,
                            steps=[
                                dict(range=[0, 0.3], color='rgba(34,197,94,0.15)'),
                                dict(range=[0.3, 0.6], color='rgba(245,158,11,0.15)'),
                                dict(range=[0.6, 1], color='rgba(244,63,94,0.15)')
                            ]
                        )
                    ))
                    _gauge.update_layout(
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)',
                        font=dict(color=COLORS['text_secondary']),
                        margin=dict(l=10, r=10, t=30, b=10),
                        height=220
                    )
                    st.plotly_chart(_gauge, use_container_width=True)
                    _status = 'DRIFTING' if _latest_score > 0.3 else 'STABLE'
                    _color = COLORS['negative'] if _latest_score > 0.3 else COLORS['positive']
                    st.markdown(f'<div style="text-align:center;font-weight:700;color:{_color};font-size:1.1rem">{_status}</div>', unsafe_allow_html=True)
                _high = _drift[_drift['drift_score'] > 0.3].tail(3)
                if not _high.empty and 'before_titles' in _drift.columns:
                    st.markdown('<br>', unsafe_allow_html=True)
                    st.markdown(section_title('🔀', 'What Changed — Before vs After Drift'), unsafe_allow_html=True)
                    for _, _row in _high.iterrows():
                        _before = str(_row.get('before_titles', ''))[:150]
                        _after = str(_row.get('after_titles', ''))[:150]
                        _ts = str(_row['timestamp'])[:19]
                        _sc = _row['drift_score']
                        st.markdown(f'''<div class="pl-alert-card">
                            <b>🕐 {_ts}</b> &nbsp;·&nbsp; Score: <b>{_sc:.3f}</b><br><br>
                            <b style="color:{COLORS['neutral']}">BEFORE:</b> {_before}<br>
                            <b style="color:{COLORS['brand_end']}">AFTER &nbsp;:</b> {_after}
                        </div>''', unsafe_allow_html=True)
            else:
                st.info('Accumulating drift data — needs 2+ minute windows.')
        except Exception as _e:
            st.info(f'Drift detection warming up...')
    with tab_anomalies:
        col_e, col_f = st.columns([1, 1])

        with col_e:
            st.markdown(section_title("🚨", "Processor-Flagged Alerts", "3× rolling-average rule from the stream processor"), unsafe_allow_html=True)
            if not alerts_df.empty:
                alerts_html = "".join(
                    alert_card(row.timestamp, row.post_count, row.rolling_avg) for row in alerts_df.itertuples()
                )
                st.markdown(alerts_html, unsafe_allow_html=True)
                csv_alerts = alerts_df.drop(columns=["timestamp_dt"], errors="ignore").to_csv(index=False).encode("utf-8")
                st.download_button("⬇️ Export alerts (CSV)", data=csv_alerts, file_name="pulselite_alerts.csv", mime="text/csv")
            else:
                st.markdown(empty_state("✅", "No anomalies detected", "Volume has stayed within normal bounds."), unsafe_allow_html=True)

        with col_f:
            st.markdown(section_title("📐", "Statistical Overlay", "z-score outliers (|z| ≥ 2) — independent of the processor rule"), unsafe_allow_html=True)
            if len(volume_df) >= 4:
                mean = volume_df["post_count"].mean()
                std = volume_df["post_count"].std(ddof=0) or 1
                zdf = volume_df.copy()
                zdf["zscore"] = (zdf["post_count"] - mean) / std
                colors = [COLORS["negative"] if abs(z) >= 2 else COLORS["brand_start"] for z in zdf["zscore"]]
                fig7 = go.Figure(go.Bar(x=zdf["minute_dt"], y=zdf["zscore"], marker_color=colors))
                fig7.add_hline(y=2, line_dash="dot", line_color=COLORS["negative"])
                fig7.add_hline(y=-2, line_dash="dot", line_color=COLORS["negative"])
                apply_chart_theme(fig7, height=320)
                st.plotly_chart(fig7, use_container_width=True)
            else:
                st.info("Need at least 4 volume buckets to compute z-scores.")

    # --------------------------------------------------------------------------
    # TAB 5 — Pipeline Health
    # --------------------------------------------------------------------------
    with tab_pipeline:
        st.markdown(section_title("⚙️", "Pipeline Components"), unsafe_allow_html=True)
        p1, p2, p3, p4 = st.columns(4)
        stage_status = "Streaming" if status.state == "LIVE" else ("Delayed" if status.state == "IDLE" else "Check process")
        with p1:
            st.markdown(pipeline_card("📡", "Hacker News API", "Source reachable"), unsafe_allow_html=True)
        with p2:
            st.markdown(pipeline_card("📨", "Kafka Topic: hn-posts", stage_status), unsafe_allow_html=True)
        with p3:
            st.markdown(pipeline_card("🧠", "VADER Processor", stage_status), unsafe_allow_html=True)
        with p4:
            st.markdown(pipeline_card("🗄️", "DuckDB Store", "Connected" if has_data else "Empty"), unsafe_allow_html=True)

        st.write("")
        col_g, col_h = st.columns([1, 1])

        with col_g:
            st.markdown(section_title("🕐", "Freshness"), unsafe_allow_html=True)
            st.metric("Pipeline state", status.state)
            if status.minutes_since_last is not None:
                st.metric("Minutes since last post", f"{status.minutes_since_last:g}")
            st.caption(f"Last post timestamp: `{status.last_timestamp}`")

        with col_h:
            st.markdown(section_title("📋", "Table Row Counts"), unsafe_allow_html=True)
            counts_df = pd.DataFrame(
                [{"table": k, "rows": v} for k, v in table_counts.items()]
            )
            st.dataframe(counts_df, use_container_width=True, hide_index=True)

        st.write("")
        st.markdown(section_title("🧪", "Data Quality"), unsafe_allow_html=True)
        quality = load_null_quality()
        q1, q2, q3, q4 = st.columns(4)
        q1.metric("Total rows", int(quality.get("total", 0)))
        q2.metric("Missing titles", int(quality.get("missing_title", 0) or 0))
        q3.metric("Missing sentiment", int(quality.get("missing_sentiment", 0) or 0))
        q4.metric("Missing score", int(quality.get("missing_score", 0) or 0))

        st.write("")
        st.write("")
        st.markdown(section_title("📋", "Schema Registry"), unsafe_allow_html=True)
        try:
            import requests as _req
            _sr = _req.get("http://localhost:8081/subjects", timeout=2)
            if _sr.status_code == 200:
                _subjects = _sr.json()
                st.success("✅ Schema Registry is connected")
                _col1, _col2 = st.columns(2)
                with _col1:
                    st.metric("Registered Subjects", len(_subjects))
                    st.caption(f"Subjects: {', '.join(_subjects)}")
                with _col2:
                    try:
                        _versions = _req.get("http://localhost:8081/subjects/hn-posts-value/versions", timeout=2).json()
                        st.metric("Schema Versions", len(_versions))
                        _latest = _req.get(f"http://localhost:8081/subjects/hn-posts-value/versions/{_versions[-1]}", timeout=2).json()
                        st.caption(f"Latest schema ID: {_latest.get('id', 'N/A')}")
                    except Exception:
                        st.caption("No versions yet")
            else:
                st.warning("⚠️ Schema Registry returned unexpected response")
        except Exception:
            st.warning("⚠️ Schema Registry not reachable — is it running?")
        st.markdown(section_title("🏗️", "Architecture"), unsafe_allow_html=True)
        st.markdown(
            _raw(f"""
            <div class="pl-diagram">{SOURCE_LABEL} API
       │  (poll new stories)
       ▼
    Kafka topic "hn-posts"
       │  (producer/reddit_producer.py)
       ▼
    Stream Processor
       │  (processor/spark_processor.py — VADER sentiment + windowed volume)
       ▼
    DuckDB  ({DB_PATH})
       │  tables: posts · volume_per_minute · anomaly_alerts
       ▼
    PulseLite Dashboard  (you are here)</div>
            """),
            unsafe_allow_html=True,
        )


    # ==========================================================================
    # Footer + auto-refresh
    # ==========================================================================

    st.divider()
    countdown = st.session_state.refresh_seconds
    st.markdown(
        _raw(f"""
        <div class="pl-footer">
            {"⏱ Auto-refreshing every " + str(countdown) + "s" if st.session_state.auto_refresh else "⏸ Auto-refresh paused"}
            &nbsp;·&nbsp; Built with Streamlit + Plotly + DuckDB
            &nbsp;·&nbsp; <a href="https://github.com/Dewesh10/pulselite" target="_blank">{APP_NAME} on GitHub</a>
            &nbsp;·&nbsp; {AUTHOR}
        </div>
        """),
        unsafe_allow_html=True,
    )



# Clock refreshes every second, data refreshes on the user-set interval
st_autorefresh(interval=1000, key="pulselite_clock")

if st.session_state.auto_refresh:
    st_autorefresh(
        interval=st.session_state.refresh_seconds * 1000,
        key="pulselite_refresh"
    )

clear_all_caches()

try:
    _render_live_dashboard()
except Exception as exc:
    st.error(f"Dashboard render error: {exc}")