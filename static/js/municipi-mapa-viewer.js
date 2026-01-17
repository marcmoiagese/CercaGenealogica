(function () {
    const root = document.getElementById("municipiMapaViewer");
    if (!root) return;
    const versionId = root.dataset.versionId;
    const svg = document.getElementById("municipiMapaSvg");
    const statusEl = document.getElementById("municipiMapaStatus");
    const loadingLabel = statusEl ? statusEl.dataset.loading || "Loading..." : "";
    const emptyLabel = statusEl ? statusEl.dataset.empty || "Empty" : "";

    if (!versionId) {
        if (statusEl) statusEl.textContent = emptyLabel;
        return;
    }

    if (statusEl) statusEl.textContent = loadingLabel;

    fetch("/api/mapes/versions/" + versionId)
        .then((res) => {
            if (!res.ok) throw new Error("failed");
            return res.json();
        })
        .then((payload) => {
            if (!payload || !payload.data) {
                if (statusEl) statusEl.textContent = emptyLabel;
                return;
            }
            if (statusEl) statusEl.textContent = "";
            if (window.CGMap) {
                window.CGMap.render(svg, payload.data);
            }
        })
        .catch(() => {
            if (statusEl) statusEl.textContent = emptyLabel;
        });
})();
