import re
from collections import Counter
import numpy as np
from dataclasses import dataclass
import pandas as pd
from datetime import datetime

ZSCORE_ANOMALY_THRESHOLD = 2.0
PULSE_WEIGHTS = {
    "velocity": 0.40,
    "sentiment": 0.35,
    "stability": 0.25,
}
FRESHNESS_LIVE_MINUTES = 3
FRESHNESS_IDLE_MINUTES = 15

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
    sentiment_component = (sentiment_idx + 100) / 2
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


def rolling_sentiment(posts_df: pd.DataFrame, window: int = 8) -> pd.DataFrame:
    if posts_df is None or posts_df.empty or "timestamp_dt" not in posts_df:
        return pd.DataFrame(columns=["timestamp_dt", "sentiment", "rolling"])
    df = posts_df.dropna(subset=["timestamp_dt"]).sort_values("timestamp_dt").copy()
    if df.empty:
        return pd.DataFrame(columns=["timestamp_dt", "sentiment", "rolling"])
    df["rolling"] = df["sentiment"].rolling(window=window, min_periods=1).mean()
    return df[["timestamp_dt", "sentiment", "rolling", "title"]]


def top_terms(titles: pd.Series, top_n: int = 12, min_len: int = 3) -> list[tuple[str, int]]:
    counter: Counter = Counter()
    for title in titles.dropna():
        for word in re.findall(rf"\b[a-zA-Z]{{{min_len},}}\b", str(title).lower()):
            if word not in STOPWORDS:
                counter[word] += 1
    return counter.most_common(top_n)

def top_engaging_posts(posts_df: pd.DataFrame, n: int = 8) -> pd.DataFrame:
    if posts_df is None or posts_df.empty:
        return pd.DataFrame()
    ranked = posts_df.sort_values("engagement", ascending=False)
    deduped = ranked.drop_duplicates(subset="title", keep="first")
    return deduped.head(n)


def average_engagement(posts_df: pd.DataFrame) -> float:
    if posts_df is None or posts_df.empty:
        return 0.0
    return round(float(posts_df["score"].mean() + posts_df["comments"].mean()), 1)


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