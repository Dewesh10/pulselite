document.addEventListener("DOMContentLoaded", () => {
    // ---- Tab switching ----
    const buttons = document.querySelectorAll(".pl-tab-btn");
    const contents = document.querySelectorAll(".pl-tab-content");
    buttons.forEach(btn => {
        btn.addEventListener("click", () => {
            buttons.forEach(b => b.classList.remove("active"));
            contents.forEach(c => c.classList.remove("active"));
            btn.classList.add("active");
            document.getElementById("tab-" + btn.dataset.tab).classList.add("active");
        });
    });

    // ---- Feed filters ----
    const sentimentChecks = document.querySelectorAll(".sentiment-filter");
    const searchBox = document.getElementById("search-box");
    const minScoreSlider = document.getElementById("min-score");
    const minScoreLabel = document.getElementById("min-score-label");
    const feedItems = document.querySelectorAll(".feed-item");
    const noResultsMsg = document.getElementById("no-results-msg");

    function applyFilters() {
        const activeSentiments = Array.from(sentimentChecks).filter(c => c.checked).map(c => c.value);
        const searchTerm = searchBox.value.toLowerCase().trim();
        const minScore = parseInt(minScoreSlider.value, 10);
        minScoreLabel.textContent = minScore;

        let visibleCount = 0;
        feedItems.forEach(item => {
            const matchesSentiment = activeSentiments.includes(item.dataset.sentiment);
            const matchesSearch = !searchTerm || item.dataset.title.includes(searchTerm);
            const matchesScore = parseInt(item.dataset.score, 10) >= minScore;
            const visible = matchesSentiment && matchesSearch && matchesScore;
            item.style.display = visible ? "" : "none";
            if (visible) visibleCount++;
        });
        noResultsMsg.style.display = visibleCount === 0 ? "block" : "none";
    }

    sentimentChecks.forEach(c => c.addEventListener("change", applyFilters));
    searchBox.addEventListener("input", applyFilters);
    minScoreSlider.addEventListener("input", applyFilters);

    // ---- Auto-refresh ----
    const autoRefreshToggle = document.getElementById("auto-refresh-toggle");
    const refreshIntervalSlider = document.getElementById("refresh-interval");
    const refreshIntervalLabel = document.getElementById("refresh-interval-label");
    let refreshTimer = null;

    function startAutoRefresh() {
        stopAutoRefresh();
        const seconds = parseInt(refreshIntervalSlider.value, 10);
        refreshTimer = setInterval(() => window.location.reload(), seconds * 1000);
    }
    function stopAutoRefresh() {
        if (refreshTimer) clearInterval(refreshTimer);
    }

    autoRefreshToggle.addEventListener("change", () => {
        if (autoRefreshToggle.checked) startAutoRefresh();
        else stopAutoRefresh();
    });
    refreshIntervalSlider.addEventListener("input", () => {
        refreshIntervalLabel.textContent = refreshIntervalSlider.value;
        if (autoRefreshToggle.checked) startAutoRefresh();
    });
});