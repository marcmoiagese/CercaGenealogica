(function () {
    const root = document.getElementById("municipiMapaViewer");
    if (!root) return;
    const versionId = root.dataset.versionId;
    const mapTitle = root.dataset.mapTitle || "mapa";
    const exportEmptyLabel = root.dataset.exportEmpty || "No hi ha mapa per exportar.";
    const exportUnavailableLabel = root.dataset.exportUnavailable || "No es pot exportar ara mateix.";
    const exportSvgDoneLabel = root.dataset.exportSvgDone || "SVG exportat.";
    const exportJsonDoneLabel = root.dataset.exportJsonDone || "Codi exportat.";
    const exportSvgBtn = document.getElementById("mapViewExportSvg");
    const exportJsonBtn = document.getElementById("mapViewExportJson");
    const svg = document.getElementById("municipiMapaSvg");
    const statusEl = document.getElementById("municipiMapaStatus");
    const loadingLabel = statusEl ? statusEl.dataset.loading || "Loading..." : "";
    const emptyLabel = statusEl ? statusEl.dataset.empty || "Empty" : "";

    let mapData = null;

    function sanitizeFileName(name) {
        const base = String(name || "mapa");
        const normalized = base.normalize ? base.normalize("NFD").replace(/[\u0300-\u036f]/g, "") : base;
        const cleaned = normalized.replace(/[^a-zA-Z0-9]+/g, "-").replace(/^-+|-+$/g, "").toLowerCase();
        return cleaned || "mapa";
    }

    function downloadBlob(filename, blob) {
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        link.remove();
        setTimeout(() => URL.revokeObjectURL(url), 0);
    }

    function exportStyleText() {
        return [
            ".cg-house{fill:#d9cfc6;stroke:#8f7c6b;stroke-width:2;}",
            ".cg-street{fill:none;stroke:#5c6b73;stroke-width:4;stroke-linecap:round;stroke-linejoin:round;}",
            ".cg-street--default{stroke:#5c6b73;stroke-width:4;}",
            ".cg-street--asfaltat{stroke:#3b4b55;stroke-width:5;}",
            ".cg-street--empedrat{stroke:#6c5b4c;stroke-width:5;stroke-dasharray:7 5;}",
            ".cg-street--terra{stroke:#a9714b;stroke-width:4;opacity:.7;}",
            ".cg-street--carretera{stroke:#1f2937;stroke-width:7;stroke-dasharray:10 6;stroke-linecap:round;}",
            ".cg-street--autopista{stroke:#0f172a;stroke-width:11;stroke-linecap:round;}",
            ".cg-river{fill:none;stroke:#3b82f6;stroke-width:6;opacity:.7;}",
            ".cg-river--riu{stroke:#2563eb;stroke-width:7;}",
            ".cg-river--riera{stroke:#38bdf8;stroke-width:5;stroke-dasharray:10 6;}",
            ".cg-river--rierol{stroke:#7dd3fc;stroke-width:4;stroke-dasharray:5 6;}",
            ".cg-street-label,.cg-house-label,.cg-toponym-label{font-size:18px;fill:#2b2b2b;}",
            ".cg-marker{fill:#3f7fbf;stroke:#2b567f;stroke-width:2;}",
            ".cg-element{fill:#3f7fbf;stroke:#1f3b57;stroke-width:2;}",
            ".cg-element--tree{fill:#2f855a;stroke:#1f5c3a;}",
            ".cg-element--fountain{fill:#0ea5e9;stroke:#0369a1;}",
            ".cg-element--well{fill:#6b7280;stroke:#374151;}",
            ".cg-element--bench{fill:#b45309;stroke:#78350f;}",
            ".cg-boundary{fill:none;stroke:#9c8b7a;stroke-width:3;stroke-dasharray:6 6;}",
        ].join("\n");
    }

    function exportSvg() {
        if (!mapData) {
            if (statusEl) statusEl.textContent = exportEmptyLabel;
            return;
        }
        if (!window.CGMap) {
            if (statusEl) statusEl.textContent = exportUnavailableLabel;
            return;
        }
        const NS = "http://www.w3.org/2000/svg";
        const exportSvgEl = document.createElementNS(NS, "svg");
        exportSvgEl.setAttribute("xmlns", "http://www.w3.org/2000/svg");
        if (Array.isArray(mapData.viewBox) && mapData.viewBox.length >= 4) {
            exportSvgEl.setAttribute("viewBox", mapData.viewBox.join(" "));
        } else {
            exportSvgEl.setAttribute("viewBox", "0 0 1000 700");
        }
        window.CGMap.render(exportSvgEl, mapData);
        const metadata = document.createElementNS(NS, "metadata");
        metadata.setAttribute("id", "cgmap-data");
        metadata.textContent = JSON.stringify(mapData);
        exportSvgEl.insertBefore(metadata, exportSvgEl.firstChild);
        const style = document.createElementNS(NS, "style");
        style.textContent = exportStyleText();
        exportSvgEl.insertBefore(style, metadata);
        const serializer = new XMLSerializer();
        const svgText = serializer.serializeToString(exportSvgEl);
        const payload = '<?xml version="1.0" encoding="UTF-8"?>\n' + svgText;
        downloadBlob(sanitizeFileName(mapTitle) + ".svg", new Blob([payload], { type: "image/svg+xml;charset=utf-8" }));
        if (statusEl) statusEl.textContent = exportSvgDoneLabel;
    }

    function exportJson() {
        if (!mapData) {
            if (statusEl) statusEl.textContent = exportEmptyLabel;
            return;
        }
        const payload = JSON.stringify(mapData, null, 2);
        downloadBlob(sanitizeFileName(mapTitle) + ".json", new Blob([payload], { type: "application/json;charset=utf-8" }));
        if (statusEl) statusEl.textContent = exportJsonDoneLabel;
    }

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
            mapData = payload.data;
            if (window.CGMap) {
                window.CGMap.render(svg, payload.data);
            }
        })
        .catch(() => {
            if (statusEl) statusEl.textContent = emptyLabel;
        });

    if (exportSvgBtn) exportSvgBtn.addEventListener("click", exportSvg);
    if (exportJsonBtn) exportJsonBtn.addEventListener("click", exportJson);
})();
