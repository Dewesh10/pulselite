"""
PulseLite — Stream-Stream Join
Joins hn-posts (new stories) and hn-top (top stories) on 5-minute
tumbling windows and computes Pearson correlation of volume.
"""

import csv
import os
import time
from datetime import datetime
from collections import defaultdict
import numpy as np

CSV_POSTS = "data_posts.csv"
CSV_TOP = "data_top.csv"
CSV_CORRELATION = "data_correlation.csv"


def init_correlation_csv():
    if not os.path.exists(CSV_CORRELATION):
        with open(CSV_CORRELATION, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(
                [
                    "window_start",
                    "window_end",
                    "new_volume",
                    "top_volume",
                    "correlation",
                ]
            )


def load_volume_by_minute(csv_path):
    """Load post counts bucketed by minute from a CSV file."""
    counts = defaultdict(int)
    if not os.path.exists(csv_path):
        return counts
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ts = row.get("timestamp", "")
                if ts:
                    minute = ts[:16]  # YYYY-MM-DDTHH:MM
                    counts[minute] += 1
    except Exception:
        pass
    return counts


def compute_correlation(new_counts, top_counts):
    """
    Join two volume streams on a 5-minute tumbling window
    and compute Pearson correlation.
    """
    all_minutes = sorted(set(new_counts.keys()) | set(top_counts.keys()))
    if len(all_minutes) < 5:
        return None, None, None

    # Take last 12 minutes (sliding window)
    recent_minutes = all_minutes[-12:]

    new_vols = [new_counts.get(m, 0) for m in recent_minutes]
    top_vols = [top_counts.get(m, 0) for m in recent_minutes]

    if sum(new_vols) == 0 or sum(top_vols) == 0:
        return None, None, None

    # Pearson correlation
    new_arr = np.array(new_vols, dtype=float)
    top_arr = np.array(top_vols, dtype=float)

    if new_arr.std() == 0 or top_arr.std() == 0:
        correlation = 0.0
    else:
        correlation = float(np.corrcoef(new_arr, top_arr)[0, 1])

    return (
        recent_minutes[0],
        recent_minutes[-1],
        new_vols,
        top_vols,
        round(correlation, 4),
    )


def save_correlation(window_start, window_end, new_vol, top_vol, corr):
    with open(CSV_CORRELATION, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(
            [window_start, window_end, sum(new_vol), sum(top_vol), corr]
        )
    print(
        f"📊 Window [{window_start} → {window_end}] | "
        f"New: {sum(new_vol)} | Top: {sum(top_vol)} | "
        f"Correlation: {corr:.4f}"
    )


def main():
    print("PulseLite Stream-Stream Join started")
    print("Joining hn-posts (new) × hn-top (top) on 5-min windows")
    print("-" * 50)

    init_correlation_csv()

    while True:
        new_counts = load_volume_by_minute(CSV_POSTS)
        top_counts = load_volume_by_minute(CSV_TOP)

        result = compute_correlation(new_counts, top_counts)
        if result and result[0] is not None:
            window_start, window_end, new_vol, top_vol, corr = result
            save_correlation(window_start, window_end, new_vol, top_vol, corr)
        else:
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] Accumulating data for join..."
            )

        time.sleep(60)  # Run every minute


if __name__ == "__main__":
    main()
