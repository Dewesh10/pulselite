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

    // ---- Sentiment chip filters ----
    const chipTags = document.querySelectorAll(".pl-chip-tag");
    function activeSentiments() {
        return Array.from(chipTags).filter(c => !c.classList.contains("off")).map(c => c.dataset.value);
    }
    chipTags.forEach(chip => {
        chip.addEventListener("click", () => {
            chip.classList.toggle("off");
            applyFilters();
        });
    });

    // ---- Feed filters, sort, rows-limit, pagination ----
    const searchBox = document.getElementById("search-box");
    const minScoreSlider = document.getElementById("min-score");
    const minScoreLabel = document.getElementById("min-score-label");
    const sortSelect = document.getElementById("sort-feed-by");
    const rowsLimitSlider = document.getElementById("rows-limit");
    const rowsLimitLabel = document.getElementById("rows-limit-label");
    const feedContainer = document.getElementById("feed-container");
    const allFeedItems = Array.from(document.querySelectorAll(".feed-item"));
    const noResultsMsg = document.getElementById("no-results-msg");
    const feedMetaText = document.getElementById("feed-meta-text");
    const feedPageLabel = document.getElementById("feed-page-label");
    const feedPrevBtn = document.getElementById("feed-prev-btn");
    const feedNextBtn = document.getElementById("feed-next-btn");
    const PAGE_SIZE = 20;
    let currentPage = 1;

    function getFilteredSorted() {
        const sentiments = activeSentiments();
        const searchTerm = searchBox.value.toLowerCase().trim();
        const minScore = parseInt(minScoreSlider.value, 10);
        const rowsLimit = parseInt(rowsLimitSlider.value, 10);

        let items = allFeedItems.slice(0, rowsLimit).filter(item => {
            const matchesSentiment = sentiments.includes(item.dataset.sentiment);
            const matchesSearch = !searchTerm || item.dataset.title.includes(searchTerm);
            const matchesScore = parseInt(item.dataset.score, 10) >= minScore;
            return matchesSentiment && matchesSearch && matchesScore;
        });

        const sortBy = sortSelect.value;
        if (sortBy === "newest") {
            items.sort((a, b) => b.dataset.timestamp.localeCompare(a.dataset.timestamp));
        } else if (sortBy === "oldest") {
            items.sort((a, b) => a.dataset.timestamp.localeCompare(b.dataset.timestamp));
        } else if (sortBy === "score") {
            items.sort((a, b) => parseInt(b.dataset.score, 10) - parseInt(a.dataset.score, 10));
        } else if (sortBy === "comments") {
            items.sort((a, b) => parseInt(b.dataset.comments, 10) - parseInt(a.dataset.comments, 10));
        }
        return items;
    }

    function applyFilters() {
        minScoreLabel.textContent = minScoreSlider.value;
        rowsLimitLabel.textContent = rowsLimitSlider.value;
        currentPage = 1;
        renderFeedPage();
    }

    function renderFeedPage() {
        allFeedItems.forEach(item => item.style.display = "none");
        const filtered = getFilteredSorted();
        const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
        if (currentPage > totalPages) currentPage = totalPages;

        const start = (currentPage - 1) * PAGE_SIZE;
        const pageItems = filtered.slice(start, start + PAGE_SIZE);

        pageItems.forEach(item => {
            item.style.display = "";
            feedContainer.appendChild(item);
        });

        noResultsMsg.style.display = filtered.length === 0 ? "block" : "none";
        feedMetaText.textContent = `${filtered.length} posts match current filters · sorted by ${sortSelect.options[sortSelect.selectedIndex].text.toLowerCase()}`;
        feedPageLabel.textContent = `Page ${currentPage} of ${totalPages}`;
        feedPrevBtn.disabled = currentPage <= 1;
        feedNextBtn.disabled = currentPage >= totalPages;
    }

    searchBox.addEventListener("input", applyFilters);
    minScoreSlider.addEventListener("input", applyFilters);
    sortSelect.addEventListener("change", applyFilters);
    rowsLimitSlider.addEventListener("input", applyFilters);
    feedPrevBtn.addEventListener("click", () => { currentPage--; renderFeedPage(); });
    feedNextBtn.addEventListener("click", () => { currentPage++; renderFeedPage(); });

    renderFeedPage();

    // ---- Feed CSV export (client-side, respects current filters) ----
    const feedExportBtn = document.getElementById("feed-export-btn");
    feedExportBtn.addEventListener("click", () => {
        const filtered = getFilteredSorted();
        const rows = [["title", "sentiment", "score", "comments", "timestamp"]];
        filtered.forEach(item => {
            rows.push([
                item.dataset.title.replace(/"/g, '""'),
                item.dataset.sentiment,
                item.dataset.score,
                item.dataset.comments,
                item.dataset.timestamp
            ]);
        });
        const csv = rows.map(r => r.map(v => `"${v}"`).join(",")).join("\n");
        const blob = new Blob([csv], { type: "text/csv" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = "pulselite_feed.csv";
        a.click();
        URL.revokeObjectURL(url);
    });

    // ---- Auto-refresh ----
    const autoRefreshToggle = document.getElementById("auto-refresh-toggle");
    const refreshIntervalSlider = document.getElementById("refresh-interval");
    const refreshIntervalLabel = document.getElementById("refresh-interval-label");
    const footerInterval = document.getElementById("footer-interval");
    const refreshNowBtn = document.getElementById("refresh-now-btn");
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
        if (footerInterval) footerInterval.textContent = refreshIntervalSlider.value;
        if (autoRefreshToggle.checked) startAutoRefresh();
    });
    refreshNowBtn.addEventListener("click", () => window.location.reload());
});