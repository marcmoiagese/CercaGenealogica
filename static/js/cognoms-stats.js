(function () {
    const holder = document.getElementById("cognom-stats");
    if (!holder) {
        return;
    }

    const docLang = document.documentElement.getAttribute("lang") || "ca";
    const formatter = new Intl.NumberFormat(docLang);
    const formatNum = (val) => formatter.format(Number(val || 0));

    const totalUrl = holder.getAttribute("data-total-url") || "";
    const seriesUrl = holder.getAttribute("data-series-url") || "";
    const topUrl = holder.getAttribute("data-top-url") || "";

    const bucketSelect = holder.querySelector("[data-series-bucket]");
    const chartEl = holder.querySelector("[data-series-chart]");
    const topListEl = holder.querySelector("[data-top-list]");
    const topYearSelect = holder.querySelector("[data-top-year]");
    const topScopeSelect = holder.querySelector("[data-top-scope]");

    const allYearLabel = topYearSelect
        ? (topYearSelect.querySelector('option[value=""]') || {}).textContent || ""
        : "";

    const fetchJSON = (url) =>
        fetch(url, { credentials: "same-origin" }).then((res) => {
            if (!res.ok) {
                throw new Error("request failed");
            }
            return res.json();
        });

    const labelForBucket = (bucket, value) => {
        const num = Number(value || 0);
        if (!num) return "-";
        if (bucket === "decade") {
            return num + "-" + (num + 9);
        }
        return String(num);
    };

    const renderTotals = (payload) => {
        const personesEl = holder.querySelector('[data-stat="persones"]');
        const aparicionsEl = holder.querySelector('[data-stat="aparicions"]');
        if (personesEl) {
            personesEl.textContent = formatNum(payload.total_persones || 0);
        }
        if (aparicionsEl) {
            aparicionsEl.textContent = formatNum(payload.total_aparicions || 0);
        }
    };

    const renderChart = (rows, bucket, emptyLabel) => {
        if (!chartEl) {
            return;
        }
        if (!Array.isArray(rows) || !rows.length) {
            chartEl.innerHTML = '<div class="cognom-stats-empty">' + (emptyLabel || "-") + "</div>";
            return;
        }
        const width = Math.max(320, Math.round(chartEl.clientWidth || 520));
        const height = 190;
        const padding = 28;
        const values = rows.map((row) => Number(row.y || 0));
        const maxVal = Math.max(1, ...values);
        const points = rows.map((row, idx) => {
            const x = rows.length === 1
                ? width / 2
                : padding + (idx / (rows.length - 1)) * (width - padding * 2);
            const y = height - padding - (Number(row.y || 0) / maxVal) * (height - padding * 2);
            return [x, y];
        });
        const line = points.map((p) => p[0].toFixed(1) + "," + p[1].toFixed(1)).join(" ");
        const firstLabel = labelForBucket(bucket, rows[0].x);
        const lastLabel = labelForBucket(bucket, rows[rows.length - 1].x);
        chartEl.innerHTML =
            '<svg viewBox="0 0 ' +
            width +
            " " +
            height +
            '" class="cognom-stats-svg" role="img">' +
            '<defs><linearGradient id="cognomLineGradient" x1="0" y1="0" x2="1" y2="0">' +
            '<stop offset="0%" stop-color="#2563eb"/>' +
            '<stop offset="100%" stop-color="#38bdf8"/>' +
            "</linearGradient></defs>" +
            '<polyline class="cognom-stats-line" fill="none" stroke="url(#cognomLineGradient)" stroke-width="2.5" points="' +
            line +
            '"></polyline>' +
            points
                .map((p) => {
                    return (
                        '<circle class="cognom-stats-point" cx="' +
                        p[0].toFixed(1) +
                        '" cy="' +
                        p[1].toFixed(1) +
                        '" r="3"></circle>'
                    );
                })
                .join("") +
            '<text class="cognom-stats-axis" x="' +
            padding +
            '" y="' +
            (height - 8) +
            '">' +
            firstLabel +
            "</text>" +
            '<text class="cognom-stats-axis" x="' +
            (width - padding) +
            '" y="' +
            (height - 8) +
            '" text-anchor="end">' +
            lastLabel +
            "</text>" +
            "</svg>";
    };

    const renderTopList = (items, emptyLabel) => {
        if (!topListEl) {
            return;
        }
        if (!Array.isArray(items) || !items.length) {
            topListEl.innerHTML = '<li class="cognom-stats-empty">' + (emptyLabel || "-") + "</li>";
            return;
        }
        topListEl.innerHTML = items
            .map((item) => {
                return (
                    '<li class="cognom-stats-item">' +
                    '<span class="cognom-stats-item-label">' +
                    (item.label || "-") +
                    "</span>" +
                    '<span class="cognom-stats-item-value">' +
                    formatNum(item.total || 0) +
                    "</span>" +
                    "</li>"
                );
            })
            .join("");
    };

    const updateYearOptions = (rows) => {
        if (!topYearSelect) {
            return;
        }
        const years = Array.isArray(rows)
            ? rows.map((row) => Number(row.x || 0)).filter((y) => y > 0)
            : [];
        const uniqueYears = Array.from(new Set(years));
        uniqueYears.sort((a, b) => b - a);
        const options = [];
        options.push('<option value="">' + (allYearLabel || "-") + "</option>");
        uniqueYears.forEach((year) => {
            options.push('<option value="' + year + '">' + year + "</option>");
        });
        topYearSelect.innerHTML = options.join("");
        if (uniqueYears.length) {
            topYearSelect.value = String(uniqueYears[0]);
        }
    };

    const loadTotals = () => {
        if (!totalUrl) return;
        fetchJSON(totalUrl)
            .then(renderTotals)
            .catch(() => renderTotals({}));
    };

    const loadSeries = (bucket) => {
        if (!seriesUrl) return;
        const emptyLabel = chartEl ? chartEl.getAttribute("data-empty") : "";
        const params = new URLSearchParams();
        params.set("bucket", bucket || "year");
        fetchJSON(seriesUrl + "?" + params.toString())
            .then((payload) => {
                const rows = Array.isArray(payload.items) ? payload.items : [];
                renderChart(rows, payload.bucket || bucket || "year", emptyLabel);
                if ((bucket || "year") === "year") {
                    updateYearOptions(rows);
                    loadTop();
                }
            })
            .catch(() => {
                renderChart([], bucket || "year", emptyLabel);
                if ((bucket || "year") === "year") {
                    updateYearOptions([]);
                    loadTop();
                }
            });
    };

    const loadTop = () => {
        if (!topUrl) return;
        const emptyLabel = topListEl ? topListEl.getAttribute("data-empty") : "";
        const params = new URLSearchParams();
        if (topScopeSelect && topScopeSelect.value) {
            params.set("scope", topScopeSelect.value);
        }
        if (topYearSelect && topYearSelect.value) {
            params.set("any", topYearSelect.value);
        }
        params.set("limit", "12");
        fetchJSON(topUrl + "?" + params.toString())
            .then((payload) => {
                const items = Array.isArray(payload.items) ? payload.items : [];
                renderTopList(items, emptyLabel);
            })
            .catch(() => {
                renderTopList([], emptyLabel);
            });
    };

    if (bucketSelect) {
        bucketSelect.addEventListener("change", () => {
            loadSeries(bucketSelect.value || "year");
        });
    }
    if (topYearSelect) {
        topYearSelect.addEventListener("change", loadTop);
    }
    if (topScopeSelect) {
        topScopeSelect.addEventListener("change", loadTop);
    }

    loadTotals();
    loadSeries(bucketSelect ? bucketSelect.value : "year");
})();
