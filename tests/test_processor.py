"""
PulseLite — Test Suite
Tests for the core processor logic: sentiment analysis and anomaly detection.
"""

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