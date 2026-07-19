import os
import pandas as pd

# --------------------------------------------------------------------------
# Paths / config (copied from dashboard/app.py)
# --------------------------------------------------------------------------

DEMO_MODE = os.environ.get("DEMO_MODE", "false").lower() == "true"
DEMO_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "demo_data"
)

CSV_POSTS = "data_posts.csv"
CSV_VOLUME = "data_volume.csv"
CSV_ALERTS = "data_alerts.csv"


def db_exists() -> bool:
    demo_path = os.path.join(DEMO_DATA_DIR, "data_posts.csv")
    return os.path.exists(demo_path) if DEMO_MODE else os.path.exists(CSV_POSTS)


def _shift_demo_time(
    df: pd.DataFrame, dt_col: str, str_col: str, str_fmt: str
) -> pd.DataFrame:
    if not DEMO_MODE or df.empty or df[dt_col].isna().all():
        return df
    offset = pd.Timestamp.now() - df[dt_col].max()
    df[dt_col] = df[dt_col] + offset
    df[str_col] = df[dt_col].dt.strftime(str_fmt)
    return df


def load_posts(limit: int = 1000) -> pd.DataFrame:
    path = os.path.join(DEMO_DATA_DIR, "data_posts.csv") if DEMO_MODE else CSV_POSTS
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, encoding="utf-8")
        if df.empty:
            return df
        df["timestamp_dt"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0)
        df["comments"] = pd.to_numeric(df["comments"], errors="coerce").fillna(0)
        df["sentiment"] = pd.to_numeric(df["sentiment"], errors="coerce").fillna(0.0)
        df["sentiment_label"] = df["sentiment_label"].fillna("neutral")
        df["engagement"] = df["score"] + df["comments"] * 2
        df = _shift_demo_time(df, "timestamp_dt", "timestamp", "%Y-%m-%dT%H:%M:%S.%f")
        return df.tail(limit)
    except Exception:
        return pd.DataFrame()


def load_volume(limit_minutes: int = 60) -> pd.DataFrame:
    path = os.path.join(DEMO_DATA_DIR, "data_volume.csv") if DEMO_MODE else CSV_VOLUME
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, encoding="utf-8")
        if df.empty:
            return df
        df["minute_dt"] = pd.to_datetime(df["minute"], errors="coerce")
        df["post_count"] = pd.to_numeric(df["post_count"], errors="coerce").fillna(0)
        df = _shift_demo_time(df, "minute_dt", "minute", "%Y-%m-%d %H:%M")
        return df.tail(limit_minutes)
    except Exception:
        return pd.DataFrame()


def load_alerts(limit: int = 25) -> pd.DataFrame:
    path = os.path.join(DEMO_DATA_DIR, "data_alerts.csv") if DEMO_MODE else CSV_ALERTS
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, encoding="utf-8")
        if df.empty:
            return df
        df["timestamp_dt"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df["post_count"] = pd.to_numeric(df["post_count"], errors="coerce").fillna(0)
        df["rolling_avg"] = pd.to_numeric(df["rolling_avg"], errors="coerce").fillna(
            0.0
        )
        df = _shift_demo_time(df, "timestamp_dt", "timestamp", "%Y-%m-%dT%H:%M:%S.%f")
        return df.tail(limit)
    except Exception:
        return pd.DataFrame()


def load_sentiment_summary() -> pd.DataFrame:
    df = load_posts()
    if df.empty:
        return pd.DataFrame()
    return df.groupby("sentiment_label").size().reset_index(name="count")


def load_table_counts() -> dict:
    posts = load_posts()
    volume = load_volume()
    alerts = load_alerts()
    return {
        "posts": len(posts),
        "volume_per_minute": len(volume),
        "anomaly_alerts": len(alerts),
    }


def load_null_quality() -> dict:
    df = load_posts()
    if df.empty:
        return {
            "total": 0,
            "missing_title": 0,
            "missing_sentiment": 0,
            "missing_score": 0,
        }
    return {
        "total": len(df),
        "missing_title": int(df["title"].isna().sum()),
        "missing_sentiment": int(df["sentiment_label"].isna().sum()),
        "missing_score": int(df["score"].isna().sum()),
    }


def load_digest() -> dict | None:
    """
    Reads the most recent row written by processor/digest_generator.py —
    a short LLM-generated (or rule-based fallback) narrative summary of
    what's currently happening in the pipeline.
    """
    path = (
        os.path.join(DEMO_DATA_DIR, "data_digest.csv")
        if DEMO_MODE
        else "data_digest.csv"
    )
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_csv(path, encoding="utf-8")
        if df.empty:
            return None
        df["generated_dt"] = pd.to_datetime(df["generated_at"], errors="coerce")
        df = _shift_demo_time(
            df, "generated_dt", "generated_at", "%Y-%m-%dT%H:%M:%S.%f"
        )
        last = df.iloc[-1]
        return {
            "text": str(last["digest_text"]),
            "mode": str(last.get("mode", "unknown")),
            "generated_at": str(last["generated_at"]),
        }
    except Exception:
        return None


def load_drift(limit: int = 200) -> pd.DataFrame:
    path = (
        os.path.join(DEMO_DATA_DIR, "data_drift.csv") if DEMO_MODE else "data_drift.csv"
    )
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, encoding="utf-8")
        if df.empty:
            return df
        df["ts"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = _shift_demo_time(df, "ts", "timestamp", "%Y-%m-%dT%H:%M:%S.%f")
        return df.tail(limit)
    except Exception:
        return pd.DataFrame()


def load_correlation(limit: int = 200) -> pd.DataFrame:
    path = (
        os.path.join(DEMO_DATA_DIR, "data_correlation.csv")
        if DEMO_MODE
        else "data_correlation.csv"
    )
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, encoding="utf-8")
        return df.tail(limit)
    except Exception:
        return pd.DataFrame()
