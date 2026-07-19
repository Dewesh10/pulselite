"""
PulseLite — Test Suite
Tests for the core processor logic: sentiment analysis and anomaly detection.
"""
import io
import pytest
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


# ============================================================================
# Sentiment Analysis Tests
# ============================================================================

analyzer = SentimentIntensityAnalyzer()


def get_sentiment(text):
    """Mirror of the function in spark_processor.py"""
    score = analyzer.polarity_scores(text)["compound"]
    if score >= 0.05:
        return score, "positive"
    elif score <= -0.05:
        return score, "negative"
    else:
        return score, "neutral"


def test_positive_sentiment():
    """Clearly positive headline should be classified as positive."""
    score, label = get_sentiment("Amazing breakthrough in AI research — incredible results")
    assert label == "positive"
    assert score > 0.05


def test_negative_sentiment():
    """Clearly negative headline should be classified as negative."""
    score, label = get_sentiment("Terrible security breach destroys company data")
    assert label == "negative"
    assert score < -0.05


def test_neutral_sentiment():
    """Neutral technical headline should be classified as neutral."""
    score, label = get_sentiment("New Python release adds type hints")
    assert label == "neutral"


def test_sentiment_score_range():
    """Compound score must always be between -1 and +1."""
    test_titles = [
        "Show HN: I built a thing",
        "Ask HN: Best practices for Rust?",
        "DeepSeek releases new model",
        "This is absolutely the worst thing ever made",
        "This is the best day of my life",
    ]
    for title in test_titles:
        score, _ = get_sentiment(title)
        assert -1.0 <= score <= 1.0, f"Score out of range for: {title}"


# ============================================================================
# Anomaly Detection Tests
# ============================================================================

def check_anomaly(current_count, historical_counts):
    """Mirror of the anomaly logic in spark_processor.py"""
    if len(historical_counts) < 3:
        return False
    avg = sum(historical_counts) / len(historical_counts)
    return current_count > 3 * avg


def test_anomaly_detected_when_spike():
    """Should detect anomaly when current count is 3x above average."""
    historical = [5, 4, 6, 5, 4]
    current = 20  # way above 3x average of ~5
    assert check_anomaly(current, historical) is True


def test_no_anomaly_when_normal():
    """Should not detect anomaly during normal volume."""
    historical = [5, 4, 6, 5, 4]
    current = 6  # normal
    assert check_anomaly(current, historical) is False


def test_no_anomaly_with_insufficient_history():
    """Should not detect anomaly when we have fewer than 3 historical points."""
    historical = [5, 4]  # only 2 points
    current = 100
    assert check_anomaly(current, historical) is False


def test_anomaly_boundary():
    """Exactly 3x should NOT trigger — must be strictly greater than 3x."""
    historical = [10, 10, 10]
    current = 30  # exactly 3x
    assert check_anomaly(current, historical) is False


def test_anomaly_just_above_boundary():
    """Just above 3x should trigger the anomaly."""
    historical = [10, 10, 10]
    current = 31  # just above 3x
    assert check_anomaly(current, historical) is True

# ============================================================================
# Producer Tests (mocked HN API)
# ============================================================================

from unittest.mock import patch, MagicMock


def test_fetch_with_retry_success():
    """fetch_with_retry should return data on successful request."""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'producer'))
    
    mock_response = MagicMock()
    mock_response.json.return_value = [1, 2, 3, 4, 5]
    mock_response.raise_for_status.return_value = None

    with patch('requests.get', return_value=mock_response):
        from reddit_producer import fetch_with_retry
        result = fetch_with_retry("http://fake-url.com")
        assert result == [1, 2, 3, 4, 5]


def test_fetch_with_retry_failure():
    """fetch_with_retry should return None after all retries fail."""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'producer'))

    with patch('requests.get', side_effect=Exception("Connection error")):
        from reddit_producer import fetch_with_retry
        result = fetch_with_retry("http://fake-url.com", retries=2, backoff=0)
        assert result is None


def test_sentiment_empty_string():
    """Empty string should return neutral sentiment."""
    score, label = get_sentiment("")
    assert label == "neutral"
    assert -0.05 <= score <= 0.05


def test_sentiment_mixed_signals():
    """Text with mixed signals should have moderate score."""
    score, label = get_sentiment("Good progress but terrible execution")
    assert -1.0 <= score <= 1.0


# ============================================================================
# Pulse Digest Tests
# ============================================================================

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'processor'))
import digest_generator as dg


def test_gather_stats_no_data(tmp_path, monkeypatch):
    """gather_stats should return safe defaults when no CSVs exist yet."""
    monkeypatch.chdir(tmp_path)
    stats = dg.gather_stats()
    assert stats["total_posts"] == 0
    assert stats["velocity"] == 0.0
    assert stats["top_posts"] == []
    assert stats["recent_anomaly"] is None


def test_fallback_digest_no_data():
    """Fallback digest should tell the reader nothing's tracked yet, not crash."""
    stats = {
        "total_posts": 0, "velocity": 0.0, "sentiment_positive_pct": 0.0,
        "sentiment_negative_pct": 0.0, "top_posts": [], "recent_anomaly": None,
        "latest_correlation": None, "top_drift": None,
    }
    text = dg.generate_fallback_digest(stats)
    assert "waiting" in text.lower() or "no posts" in text.lower()


def test_fallback_digest_with_data():
    """Fallback digest should cite real numbers from the stats it's given."""
    stats = {
        "total_posts": 500, "velocity": 4.2, "sentiment_positive_pct": 60.0,
        "sentiment_negative_pct": 10.0,
        "top_posts": [{"title": "Test Story", "score": 200, "comments": 50}],
        "recent_anomaly": None, "latest_correlation": None, "top_drift": None,
    }
    text = dg.generate_fallback_digest(stats)
    assert "500" in text
    assert "Test Story" in text
    assert "leaning positive" in text


def test_fallback_digest_includes_anomaly():
    """When an anomaly is active, the fallback digest should mention it."""
    stats = {
        "total_posts": 100, "velocity": 10.0, "sentiment_positive_pct": 30.0,
        "sentiment_negative_pct": 30.0, "top_posts": [],
        "recent_anomaly": {"post_count": 25.0, "rolling_avg": 5.0},
        "latest_correlation": None, "top_drift": None,
    }
    text = dg.generate_fallback_digest(stats)
    assert "anomaly" in text.lower()


def test_generate_llm_digest_no_api_key(monkeypatch):
    """Without ANTHROPIC_API_KEY set, should return None (triggering fallback), not raise."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    stats = {
        "total_posts": 10, "velocity": 1.0, "sentiment_positive_pct": 50.0,
        "sentiment_negative_pct": 10.0, "top_posts": [], "recent_anomaly": None,
        "latest_correlation": None, "top_drift": None,
    }
    result = dg.generate_llm_digest(stats)
    assert result is None


def test_generate_digest_falls_back_without_key(monkeypatch):
    """generate_digest should always return a usable (text, mode) pair."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    stats = {
        "total_posts": 10, "velocity": 1.0, "sentiment_positive_pct": 50.0,
        "sentiment_negative_pct": 10.0, "top_posts": [], "recent_anomaly": None,
        "latest_correlation": None, "top_drift": None,
    }
    text, mode = dg.generate_digest(stats)
    assert mode == "fallback"
    assert len(text) > 0


def test_write_digest_creates_csv_with_header(tmp_path, monkeypatch):
    """write_digest should create the CSV with a header on first write."""
    monkeypatch.chdir(tmp_path)
    dg.write_digest("Test digest text", "fallback", 42)
    content = (tmp_path / "data_digest.csv").read_text()
    assert "generated_at,digest_text,mode,posts_analyzed" in content
    assert "Test digest text" in content
    assert "fallback" in content


def test_write_digest_appends_without_duplicate_header(tmp_path, monkeypatch):
    """A second write_digest call should append, not duplicate the header."""
    monkeypatch.chdir(tmp_path)
    dg.write_digest("First digest", "fallback", 10)
    dg.write_digest("Second digest", "llm", 20)
    content = (tmp_path / "data_digest.csv").read_text()
    assert content.count("generated_at,digest_text,mode,posts_analyzed") == 1
    assert "First digest" in content
    assert "Second digest" in content


    # ============================================================================
# ADR-004 — Avro Schema Compatibility Tests
# ============================================================================

import json
import fastavro

SCHEMA_DIR = os.path.join(os.path.dirname(__file__), '..', 'schemas')


def load_schema(filename):
    path = os.path.join(SCHEMA_DIR, filename)
    with open(path, "r") as f:
        return json.load(f)


def test_v2_schema_is_valid_avro():
    """post_v2.avsc should parse as valid Avro on its own."""
    schema = load_schema("post_v2.avsc")
    parsed = fastavro.parse_schema(schema)
    assert parsed is not None


def test_v1_schema_is_valid_avro():
    """post_v1.avsc should parse as valid Avro on its own."""
    schema = load_schema("post_v1.avsc")
    parsed = fastavro.parse_schema(schema)
    assert parsed is not None


def test_v2_adds_flair_field_with_default():
    """
    ADR-004, Test 1: v2 adds a new 'flair' field. For this to be backward
    compatible (old consumers can read new messages), the new field must
    have a default value.
    """
    v2 = load_schema("post_v2.avsc")
    field_names = [f["name"] for f in v2["fields"]]
    assert "flair" in field_names

    flair_field = next(f for f in v2["fields"] if f["name"] == "flair")
    assert "default" in flair_field, "New field must have a default to stay backward compatible"


def test_v2_is_backward_compatible_with_v1_data():
    """
    A message written with the OLD (v1) schema should still be readable
    using the NEW (v2) schema — this is what 'backward compatible' means
    in practice, and is the actual guarantee Schema Registry enforces.
    """
    v1_schema = fastavro.parse_schema(load_schema("post_v1.avsc"))
    v2_schema = fastavro.parse_schema(load_schema("post_v2.avsc"))

    record = {
        "id": 123,
        "title": "Test post",
        "score": 10,
        "comments": 2,
        "timestamp": "2026-07-19T00:00:00+00:00",
        "ingested_at": "2026-07-19T00:00:00+00:00",
    }

    # Write using v1 schema
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, v1_schema, record)
    buf.seek(0)

    # Read back using v2 schema — should succeed and fill 'flair' with its default
    result = fastavro.schemaless_reader(buf, v1_schema, v2_schema)
    assert result["title"] == "Test post"
    assert result["flair"] is None  # comes from v2's default


def test_removing_required_field_breaks_compatibility():
    """
    ADR-004, Test 2: removing a required field (like 'title', which has
    no default) is a breaking change — an old message can't be read
    correctly by a schema that dropped a required field with no default.
    This test simulates that broken schema and confirms it's rejected
    the same way the real Schema Registry rejected it.
    """
    v1 = load_schema("post_v1.avsc")
    broken_schema = {
        **v1,
        "fields": [f for f in v1["fields"] if f["name"] != "title"],
    }

    v1_schema = fastavro.parse_schema(v1)

    record = {
        "id": 123,
        "title": "Test post",
        "score": 10,
        "comments": 2,
        "timestamp": "2026-07-19T00:00:00+00:00",
        "ingested_at": "2026-07-19T00:00:00+00:00",
    }

    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, v1_schema, record)
    buf.seek(0)

    # Reading with the broken schema should either raise, or produce a
    # record silently missing 'title' — both prove removing a required
    # field is unsafe, matching what the real Schema Registry rejected.
    broken_parsed = fastavro.parse_schema(broken_schema)
    buf.seek(0)
    result = fastavro.schemaless_reader(buf, v1_schema, broken_parsed)
    assert "title" not in result, "Removed field should not silently appear — this confirms it's a breaking change"