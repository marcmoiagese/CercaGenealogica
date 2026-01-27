(function () {
    const docLang = document.documentElement.getAttribute("lang") || "ca";
    const formatter = new Intl.NumberFormat(docLang);
    const formatNum = (val) => formatter.format(Number(val || 0));

    const addQuery = (base, params) => {
        const parts = [];
        Object.keys(params).forEach((key) => {
            if (params[key] === "" || params[key] === null || typeof params[key] === "undefined") {
                return;
            }
            parts.push(encodeURIComponent(key) + "=" + encodeURIComponent(params[key]));
        });
        if (!parts.length) return base;
        return base + (base.includes("?") ? "&" : "?") + parts.join("&");
    };

    const fetchJSON = (url) => {
        return fetch(url).then((res) => {
            if (!res.ok) throw new Error("request failed");
            return res.json();
        });
    };

    const buildPath = (values, width, height, pad, maxValue) => {
        if (!values.length) return "";
        const maxVal = Math.max(1, maxValue || 0, ...values);
        const span = Math.max(1, values.length - 1);
        const stepX = (width - pad * 2) / span;
        return values
            .map((val, idx) => {
                const x = pad + stepX * idx;
                const ratio = Math.max(0, val) / maxVal;
                const y = height - pad - ratio * (height - pad * 2);
                return (idx === 0 ? "M" : "L") + x.toFixed(1) + " " + y.toFixed(1);
            })
            .join(" ");
    };

    const renderSparkline = (holder, rows) => {
        if (!holder) return;
        const values = rows.map((row) => {
            const nat = Number(row.natalitat || 0);
            const mat = Number(row.matrimonis || 0);
            const def = Number(row.defuncions || 0);
            return nat + mat + def;
        });
        if (!values.length) {
            holder.innerHTML = "";
            return;
        }
        const width = 240;
        const height = 70;
        const pad = 6;
        const path = buildPath(values, width, height, pad);
        const color = getComputedStyle(holder).getPropertyValue("--demografia-natalitat").trim() || "#4fc3f7";
        holder.innerHTML =
            '<svg viewBox="0 0 ' +
            width +
            " " +
            height +
            '" aria-hidden="true">' +
            '<path d="' +
            path +
            '" fill="none" stroke="' +
            color +
            '" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" />' +
            "</svg>";
    };

    const renderChart = (svg, rows) => {
        if (!svg) return;
        const width = 640;
        const height = 240;
        const pad = 28;
        svg.innerHTML = "";
        const series = [
            { key: "natalitat", colorVar: "--demografia-natalitat" },
            { key: "matrimonis", colorVar: "--demografia-matrimonis" },
            { key: "defuncions", colorVar: "--demografia-defuncions" },
        ];
        const valuesBySeries = series.map((s) => rows.map((row) => Number(row[s.key] || 0)));
        const maxVal = Math.max(1, ...valuesBySeries.flat());

        for (let i = 1; i <= 3; i++) {
            const y = pad + ((height - pad * 2) * i) / 4;
            const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
            line.setAttribute("x1", String(pad));
            line.setAttribute("x2", String(width - pad));
            line.setAttribute("y1", String(y));
            line.setAttribute("y2", String(y));
            line.setAttribute("stroke", "#e3e9ef");
            line.setAttribute("stroke-width", "1");
            svg.appendChild(line);
        }

        series.forEach((serie, idx) => {
            const path = buildPath(valuesBySeries[idx], width, height, pad, maxVal);
            if (!path) return;
            const pathEl = document.createElementNS("http://www.w3.org/2000/svg", "path");
            const color = getComputedStyle(svg).getPropertyValue(serie.colorVar).trim();
            pathEl.setAttribute("d", path);
            pathEl.setAttribute("fill", "none");
            pathEl.setAttribute("stroke", color || "#2c3e50");
            pathEl.setAttribute("stroke-width", "2.4");
            pathEl.setAttribute("stroke-linecap", "round");
            pathEl.setAttribute("stroke-linejoin", "round");
            svg.appendChild(pathEl);
        });

        if (maxVal <= 0) {
            const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
            line.setAttribute("x1", String(pad));
            line.setAttribute("x2", String(width - pad));
            line.setAttribute("y1", String(height - pad));
            line.setAttribute("y2", String(height - pad));
            line.setAttribute("stroke", "#d6dde3");
            line.setAttribute("stroke-width", "2");
            svg.appendChild(line);
        }
    };

    const updateTable = (tbody, rows, bucket, emptyLabel) => {
        if (!tbody) return;
        if (!rows.length) {
            tbody.innerHTML = '<tr><td colspan="4">' + (emptyLabel || "-") + "</td></tr>";
            return;
        }
        tbody.innerHTML = rows
            .map((row) => {
                const key = Number(row.key || 0);
                const label = bucket === "decade" ? key + "-" + (key + 9) : String(key);
                return (
                    "<tr>" +
                    "<td>" +
                    label +
                    "</td>" +
                    "<td>" +
                    formatNum(row.natalitat) +
                    "</td>" +
                    "<td>" +
                    formatNum(row.matrimonis) +
                    "</td>" +
                    "<td>" +
                    formatNum(row.defuncions) +
                    "</td>" +
                    "</tr>"
                );
            })
            .join("");
    };

    const applyMetaToSummary = (summary, meta) => {
        if (!summary || !meta) return;
        const totals = meta.total || {};
        const nodes = summary.querySelectorAll("[data-demografia-total]");
        nodes.forEach((node) => {
            const key = node.getAttribute("data-demografia-total");
            if (key === "total") {
                node.textContent = formatNum(
                    Number(totals.natalitat || 0) + Number(totals.matrimonis || 0) + Number(totals.defuncions || 0)
                );
                return;
            }
            node.textContent = formatNum(totals[key] || 0);
        });
        const rangeEl = summary.querySelector(".demografia-range-value");
        if (rangeEl) {
            if (meta.any_min && meta.any_max) {
                rangeEl.textContent = meta.any_min === meta.any_max ? String(meta.any_min) : meta.any_min + " - " + meta.any_max;
            } else {
                rangeEl.textContent = "-";
            }
        }
    };

    const initSummarySparkline = () => {
        const cards = document.querySelectorAll("[data-demografia-summary]");
        cards.forEach((card) => {
            const holder = card.querySelector("[data-demografia-spark]");
            const base = card.getAttribute("data-series-url");
            if (!holder || !base) return;
            const url = addQuery(base, { bucket: "decade" });
            fetchJSON(url)
                .then((payload) => {
                    const rows = Array.isArray(payload.rows) ? payload.rows : [];
                    renderSparkline(holder, rows);
                })
                .catch(() => {});
        });
    };

    const initChart = () => {
        const card = document.querySelector("[data-demografia-chart]");
        if (!card) return;
        const metaUrl = card.getAttribute("data-meta-url") || "";
        const seriesUrl = card.getAttribute("data-series-url") || "";
        const emptyLabel = card.getAttribute("data-empty") || "";
        const bucketInput = document.getElementById("demografiaBucket");
        const fromInput = document.getElementById("demografiaFrom");
        const toInput = document.getElementById("demografiaTo");
        const applyBtn = document.getElementById("demografiaApply");
        const chart = document.getElementById("demografiaChart");
        const chartSvg = chart ? chart.querySelector("svg") : null;
        const chartEmpty = chart ? chart.querySelector(".demografia-chart-empty") : null;
        const tbody = document.getElementById("demografiaTableBody");
        const summary = document.querySelector(".demografia-summary-card");

        const loadSeries = () => {
            if (!seriesUrl) return;
            const bucket = bucketInput ? bucketInput.value : "year";
            const from = fromInput && fromInput.value ? fromInput.value : "";
            const to = toInput && toInput.value ? toInput.value : "";
            const url = addQuery(seriesUrl, { bucket: bucket, from: from, to: to });
            fetchJSON(url)
                .then((payload) => {
                    const rows = Array.isArray(payload.rows) ? payload.rows : [];
                    if (chartEmpty) {
                        const hasData = rows.some((row) => row.natalitat || row.matrimonis || row.defuncions);
                        chartEmpty.textContent = emptyLabel || "No data";
                        chartEmpty.hidden = hasData;
                    }
                    renderChart(chartSvg, rows);
                    updateTable(tbody, rows, bucket, emptyLabel);
                })
                .catch(() => {
                    if (chartEmpty) {
                        chartEmpty.textContent = emptyLabel || "No data";
                        chartEmpty.hidden = false;
                    }
                    updateTable(tbody, [], bucket, emptyLabel);
                });
        };

        if (metaUrl) {
            fetchJSON(metaUrl)
                .then((meta) => {
                    applyMetaToSummary(summary, meta);
                    if (fromInput && !fromInput.value && meta.any_min) {
                        fromInput.value = meta.any_min;
                    }
                    if (toInput && !toInput.value && meta.any_max) {
                        toInput.value = meta.any_max;
                    }
                    loadSeries();
                })
                .catch(() => {
                    loadSeries();
                });
        } else {
            loadSeries();
        }

        if (applyBtn) {
            applyBtn.addEventListener("click", loadSeries);
        }

        const rebuildBtn = card.querySelector("[data-demografia-rebuild]");
        if (rebuildBtn) {
            rebuildBtn.addEventListener("click", () => {
                const url = rebuildBtn.getAttribute("data-rebuild-url");
                if (!url) return;
                rebuildBtn.disabled = true;
                fetch(url, { method: "POST" })
                    .then((res) => {
                        if (!res.ok) throw new Error("failed");
                        return res.json();
                    })
                    .then(() => {
                        if (metaUrl) {
                            return fetchJSON(metaUrl).then((meta) => {
                                applyMetaToSummary(summary, meta);
                                if (fromInput && meta.any_min) fromInput.value = meta.any_min;
                                if (toInput && meta.any_max) toInput.value = meta.any_max;
                            });
                        }
                        return null;
                    })
                    .then(() => {
                        loadSeries();
                    })
                    .catch(() => {})
                    .finally(() => {
                        rebuildBtn.disabled = false;
                    });
            });
        }
    };

    initSummarySparkline();
    initChart();
})();
