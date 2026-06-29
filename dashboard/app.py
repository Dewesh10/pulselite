import streamlit as st
import duckdb
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime
import time
import re
from collections import Counter

DB_PATH = "pulselite.db"

# Page config
st.set_page_config(
    page_title="PulseLite",
    page_icon="🔴",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS for professional look
st.markdown("""
<style>
    .main { background-color: #0e1117; }
    .block-container { padding-top: 1rem; padding-bottom: 1rem; }
    .metric-card {
        background: linear-gradient(135deg, #1e2130, #2d3250);
        border-radius: 12px;
        padding: 20px;
        border-left: 4px solid #FF4B4B;
        margin-bottom: 10px;
    }
    .alert-box {
        background: linear-gradient(135deg, #3d0000, #6b0000);
        border-radius: 8px;
        padding: 12px 16px;
        border-left: 4px solid #FF4B4B;
        margin: 6px 0;
        color: white;
    }
    .post-card {
        background: #1e2130;
        border-radius: 8px;
        padding: 12px 16px;
        margin: 6px 0;
        border-left: 3px solid #636EFA;
    }
    .post-card.positive { border-left-color: #00CC96; }
    .post-card.negative { border-left-color: #FF4B4B; }
    .post-card.neutral { border-left-color: #636EFA; }
    .header-title {
        font-size: 2.5rem;
        font-weight: 800;
        background: linear-gradient(90deg, #FF4B4B, #FF8C00);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .subtitle { color: #8b9dc3; font-size: 0.9rem; }
    div[data-testid="stMetric"] {
        background: #1e2130;
        border-radius: 12px;
        padding: 16px;
        border: 1px solid #2d3250;
    }
    div[data-testid="stMetric"] label { color: #8b9dc3 !important; }
    div[data-testid="stMetric"] div { color: white !important; }
</style>
""", unsafe_allow_html=True)

# Header
col_title, col_time = st.columns([3, 1])
with col_title:
    st.markdown('<p class="header-title">🔴 PulseLite</p>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Real-time Hacker News Intelligence Dashboard</p>', unsafe_allow_html=True)
with col_time:
    st.markdown(f"<br><p class='subtitle' style='text-align:right'>🕐 {datetime.now().strftime('%d %b %Y, %H:%M:%S')}</p>", unsafe_allow_html=True)

st.divider()

def get_data():
    con = duckdb.connect(DB_PATH, read_only=True)
    volume = con.execute("""
        SELECT minute, post_count 
        FROM volume_per_minute 
        ORDER BY minute ASC LIMIT 30
    """).df()
    sentiment = con.execute("""
        SELECT sentiment_label, COUNT(*) as count
        FROM posts
        GROUP BY sentiment_label
    """).df()
    recent = con.execute("""
        SELECT title, score, comments, sentiment_label, sentiment, timestamp
        FROM posts
        ORDER BY timestamp DESC LIMIT 20
    """).df()
    alerts = con.execute("""
        SELECT * FROM anomaly_alerts
        ORDER BY timestamp DESC LIMIT 5
    """).df()
    titles = con.execute("SELECT title FROM posts").df()
    total = con.execute("SELECT COUNT(*) as c FROM posts").fetchone()[0]
    con.close()
    return volume, sentiment, recent, alerts, titles, total

def get_top_words(titles_df):
    stopwords = {"the", "a", "an", "and", "or", "but", "in", "on", "at",
                 "to", "for", "of", "with", "is", "are", "was", "be", "it",
                 "this", "that", "by", "from", "as", "not", "how", "why",
                 "what", "i", "you", "we", "they", "he", "she", "my", "your",
                 "its", "has", "have", "had", "do", "does", "did", "will",
                 "would", "could", "should", "about", "after", "new", "use"}
    words = []
    for title in titles_df["title"].dropna():
        for word in re.findall(r'\b[a-zA-Z]{3,}\b', title.lower()):
            if word not in stopwords:
                words.append(word)
    return Counter(words).most_common(10)

try:
    volume, sentiment, recent, alerts, titles, total = get_data()

    # Metrics row
    pos_count = int(sentiment[sentiment["sentiment_label"] == "positive"]["count"].sum()) if len(sentiment) > 0 else 0
    neg_count = int(sentiment[sentiment["sentiment_label"] == "negative"]["count"].sum()) if len(sentiment) > 0 else 0
    neu_count = int(sentiment[sentiment["sentiment_label"] == "neutral"]["count"].sum()) if len(sentiment) > 0 else 0
    pos_pct = round((pos_count / total * 100), 1) if total > 0 else 0

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("📊 Total Posts", total)
    m2.metric("😊 Positive", pos_count, f"{pos_pct}%")
    m3.metric("😞 Negative", neg_count)
    m4.metric("😐 Neutral", neu_count)
    m5.metric("🚨 Anomalies", len(alerts))

    st.divider()

    # Volume chart + Sentiment pie
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("📈 Post Volume Per Minute")
        if len(volume) > 0:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=volume["minute"],
                y=volume["post_count"],
                mode="lines+markers",
                name="Posts",
                line=dict(color="#FF4B4B", width=2.5),
                marker=dict(size=6, color="#FF4B4B"),
                fill="tozeroy",
                fillcolor="rgba(255,75,75,0.1)"
            ))
            if len(alerts) > 0:
                for _, alert in alerts.iterrows():
                    fig.add_vline(
                        x=alert["timestamp"][:16],
                        line_dash="dash",
                        line_color="#FF8C00",
                        annotation_text="🚨 Spike",
                        annotation_font_color="#FF8C00"
                    )
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(showgrid=False, color="#8b9dc3"),
                yaxis=dict(showgrid=True, gridcolor="#2d3250", color="#8b9dc3"),
                margin=dict(l=0, r=0, t=10, b=0),
                showlegend=False,
                height=300
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Waiting for volume data...")

    with col2:
        st.subheader("😊 Sentiment Split")
        if len(sentiment) > 0:
            fig2 = go.Figure(go.Pie(
                labels=sentiment["sentiment_label"],
                values=sentiment["count"],
                hole=0.5,
                marker_colors=["#00CC96", "#FF4B4B", "#636EFA"]
            ))
            fig2.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                font_color="#8b9dc3",
                margin=dict(l=0, r=0, t=10, b=0),
                height=300,
                showlegend=True,
                legend=dict(font=dict(color="#8b9dc3"))
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Waiting for sentiment data...")

    st.divider()

    # Top words + Recent posts
    col1, col2 = st.columns([1, 2])

    with col1:
        st.subheader("🔤 Trending Words")
        top_words = get_top_words(titles)
        if top_words:
            words_df = pd.DataFrame(top_words, columns=["word", "count"])
            fig3 = go.Figure(go.Bar(
                x=words_df["count"],
                y=words_df["word"],
                orientation="h",
                marker_color="#636EFA",
                marker_line_color="rgba(0,0,0,0)"
            ))
            fig3.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(showgrid=False, color="#8b9dc3"),
                yaxis=dict(categoryorder="total ascending", color="#8b9dc3"),
                margin=dict(l=0, r=0, t=10, b=0),
                height=350
            )
            st.plotly_chart(fig3, use_container_width=True)

    with col2:
        st.subheader("📰 Latest Posts")
        if len(recent) > 0:
            for _, row in recent.head(8).iterrows():
                label = row["sentiment_label"]
                emoji = "😊" if label == "positive" else "😞" if label == "negative" else "😐"
                color = "positive" if label == "positive" else "negative" if label == "negative" else "neutral"
                st.markdown(f"""
                <div class="post-card {color}">
                    <b>{emoji} {row['title'][:75]}</b><br>
                    <small style="color:#8b9dc3">
                        ⬆ {row['score']} &nbsp;|&nbsp; 
                        💬 {row['comments']} &nbsp;|&nbsp; 
                        Sentiment: {row['sentiment']:.2f}
                    </small>
                </div>
                """, unsafe_allow_html=True)

    # Anomaly alerts section
    if len(alerts) > 0:
        st.divider()
        st.subheader("🚨 Anomaly Alerts")
        for _, alert in alerts.iterrows():
            st.markdown(f"""
            <div class="alert-box">
                🚨 <b>Volume Spike Detected</b> at {alert['timestamp'][:19]} — 
                Count: <b>{alert['post_count']}</b> vs Avg: <b>{alert['rolling_avg']:.1f}</b>
            </div>
            """, unsafe_allow_html=True)

except Exception as e:
    st.markdown("""
    <div style="text-align:center; padding:60px;">
        <h2>⏳ Waiting for data...</h2>
        <p style="color:#8b9dc3">Start the producer and processor first, then this dashboard will come alive.</p>
        <code>python producer/reddit_producer.py</code><br><br>
        <code>python processor/spark_processor.py</code>
    </div>
    """, unsafe_allow_html=True)

st.divider()
st.caption("⏱ Auto-refreshes every 30 seconds &nbsp;|&nbsp; Built with Streamlit + Plotly &nbsp;|&nbsp; PulseLite © 2026 Dewesh")
time.sleep(30)
st.rerun()