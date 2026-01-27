(function () {
    const docLang = document.documentElement.getAttribute("lang") || "ca";
    const formatter = new Intl.NumberFormat(docLang);
    const formatNum = (val) => formatter.format(Number(val || 0));

    const addQuery = (base, params) => {
        const parts = [];
        Object.keys(params || {}).forEach((key) => {
            const value = params[key];
            if (value === "" || value === null || typeof value === "undefined") {
                return;
            }
            parts.push(encodeURIComponent(key) + "=" + encodeURIComponent(value));
        });
        if (!parts.length) return base;
        return base + (base.includes("?") ? "&" : "?") + parts.join("&");
    };

    const fetchJSON = (url) =>
        fetch(url).then((res) => {
            if (!res.ok) throw new Error("request failed");
            return res.json();
        });

    const labelForBucket = (bucket, value) => {
        if (bucket === "decade") {
            return value + "-" + (value + 9);
        }
        return String(value);
    };

    const renderTotals = (listEl, items, emptyLabel) => {
        if (!listEl) return;
        if (!items.length) {
            listEl.innerHTML = '<li class="muni-stats-empty">' + (emptyLabel || "-") + "</li>";
            return;
        }
        listEl.innerHTML = items
            .map((item) => {
                return (
                    '<li class="muni-stats-item">' +
                    '<span class="muni-stats-item-label">' +
                    (item.label || "-") +
                    "</span>" +
                    '<span class="muni-stats-item-value">' +
                    formatNum(item.total || 0) +
                    "</span>" +
                    "</li>"
                );
            })
            .join("");
    };

    const renderSeries = (holder, rows, bucket, emptyLabel) => {
        if (!holder) return;
        if (!rows.length) {
            holder.innerHTML = '<div class="muni-stats-empty">' + (emptyLabel || "-") + "</div>";
            return;
        }
        const values = rows.map((row) => Number(row.y || 0));
        const maxVal = Math.max(1, ...values);
        holder.innerHTML = rows
            .map((row) => {
                const val = Number(row.y || 0);
                const width = Math.max(0, (val / maxVal) * 100);
                return (
                    '<div class="muni-series-row">' +
                    '<span class="muni-series-label">' +
                    labelForBucket(bucket, Number(row.x || 0)) +
                    "</span>" +
                    '<div class="muni-series-bar"><span style="width: ' +
                    width.toFixed(1) +
                    '%"></span></div>' +
                    '<span class="muni-series-value">' +
                    formatNum(val) +
                    "</span>" +
                    "</div>"
                );
            })
            .join("");
    };

    const initTabs = (card) => {
        const tabs = Array.from(card.querySelectorAll(".muni-stats-tab"));
        const panels = Array.from(card.querySelectorAll(".muni-stats-panel"));
        tabs.forEach((tab) => {
            tab.addEventListener("click", () => {
                const target = tab.getAttribute("data-tab") || "";
                tabs.forEach((btn) => btn.classList.toggle("is-active", btn === tab));
                panels.forEach((panel) =>
                    panel.classList.toggle("is-active", panel.getAttribute("data-panel") === target)
                );
            });
        });
    };

    const initCard = (card) => {
        const baseUrl = card.getAttribute("data-base-url") || "";
        const seriesUrl = card.getAttribute("data-series-url") || "";
        const seriesParam = card.getAttribute("data-series-param") || "";
        const emptyTotals = card.getAttribute("data-empty-totals") || "";
        const emptySeries = card.getAttribute("data-empty-series") || "";
        const bucket = card.getAttribute("data-bucket") || "decade";
        const listEl = card.querySelector("[data-top-list]");
        const selectEl = card.querySelector("select");
        const seriesEl = card.querySelector("[data-series-chart]");
        const totalDistinctEl = card.querySelector("[data-total-distinct]");
        if (!baseUrl || !listEl || !selectEl || !seriesEl || !seriesParam) return;

        const loadSeries = (id) => {
            if (!seriesUrl || !id) {
                renderSeries(seriesEl, [], bucket, emptySeries);
                return;
            }
            const url = addQuery(seriesUrl, { [seriesParam]: id, bucket: bucket });
            fetchJSON(url)
                .then((payload) => {
                    const rows = Array.isArray(payload.items) ? payload.items : [];
                    renderSeries(seriesEl, rows, payload.bucket || bucket, emptySeries);
                })
                .catch(() => {
                    renderSeries(seriesEl, [], bucket, emptySeries);
                });
        };

        fetchJSON(addQuery(baseUrl, { limit: 10 }))
            .then((payload) => {
                const items = Array.isArray(payload.items) ? payload.items : [];
                renderTotals(listEl, items, emptyTotals);
                if (totalDistinctEl) {
                    totalDistinctEl.textContent = formatNum(payload.totalDistinct || 0);
                }
                if (!items.length) {
                    selectEl.innerHTML = "";
                    selectEl.disabled = true;
                    loadSeries("");
                    return;
                }
                selectEl.disabled = false;
                selectEl.innerHTML = items
                    .map((item) => '<option value="' + item.id + '">' + (item.label || "-") + "</option>")
                    .join("");
                selectEl.value = String(items[0].id);
                loadSeries(items[0].id);
            })
            .catch(() => {
                renderTotals(listEl, [], emptyTotals);
                if (totalDistinctEl) {
                    totalDistinctEl.textContent = "0";
                }
                selectEl.innerHTML = "";
                selectEl.disabled = true;
                renderSeries(seriesEl, [], bucket, emptySeries);
            });

        selectEl.addEventListener("change", () => {
            loadSeries(selectEl.value);
        });

        initTabs(card);
    };

    const cards = document.querySelectorAll("[data-stats-card]");
    cards.forEach(initCard);
})();
