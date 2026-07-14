SENTIMENT_EMOJI = {
    "positive": "🟢",
    "negative": "🔴",
    "neutral": "🔵",
}


def _raw(html: str) -> str:
    """Strip leading whitespace from every line of a multi-line HTML string."""
    return "\n".join(line.strip() for line in html.strip("\n").splitlines())


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


def post_card(title: str, score: int, comments: int, sentiment: float, label: str, timestamp: str, post_id: int = None) -> str:
    emoji = SENTIMENT_EMOJI.get(label, "🔵")
    short_title = title if len(title) <= 100 else title[:97] + "…"
    ts_display = timestamp[:16].replace("T", " ") if timestamp else "—"
    hn_url = f"https://news.ycombinator.com/item?id={post_id}" if post_id else "#"
    return _raw(f"""
    <div class="pl-post-card {label}">
        <div class="pl-post-title">{emoji} <a href="{hn_url}" target="_blank" style="color:inherit;text-decoration:none;">{short_title}</a></div>
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


def digest_card(text: str, mode: str, generated_at: str) -> str:
    mode_label = "AI-generated" if mode == "llm" else "auto-generated (rule-based)"
    ts_display = str(generated_at)[:19].replace("T", " ")
    return _raw(f"""
    <div class="pl-digest-card">
        <div class="pl-digest-label">🧠 Pulse Digest</div>
        <div class="pl-digest-text">{text}</div>
        <div class="pl-digest-meta">{mode_label} · updated {ts_display}</div>
    </div>
    """)