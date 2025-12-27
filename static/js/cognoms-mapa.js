(function () {
    var mapEl = document.getElementById("cognom-map");
    if (!mapEl || typeof L === "undefined") {
        return;
    }

    var cognomID = mapEl.getAttribute("data-cognom-id");
    if (!cognomID) {
        return;
    }

    var y0Input = document.getElementById("cognom-y0");
    var y1Input = document.getElementById("cognom-y1");
    var sliderEl = document.getElementById("cognom-range");
    var updateBtn = document.getElementById("cognom-update");
    var statusEl = document.getElementById("cognom-map-status");

    var map = L.map(mapEl, { scrollWheelZoom: true }).setView([41.7, 1.7], 6);
    map.createPane("cognomHeat");
    map.getPane("cognomHeat").style.zIndex = 410;
    map.createPane("cognomMarkers");
    map.getPane("cognomMarkers").style.zIndex = 420;
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
        maxZoom: 18,
        attribution: "&copy; OpenStreetMap contributors"
    }).addTo(map);

    var heat = L.heatLayer ? L.heatLayer([], {
        radius: 48,
        blur: 32,
        maxZoom: 11,
        minOpacity: 0.55,
        pane: "cognomHeat",
        gradient: {
            0.2: "#cfe6fb",
            0.4: "#93c5fd",
            0.6: "#3b82f6",
            0.8: "#1d4ed8",
            1.0: "#0f172a"
        }
    }).addTo(map) : null;
    var markerLayer = L.layerGroup().addTo(map);
    var mapReady = false;

    function setStatus(key, fallback) {
        if (!statusEl) {
            return;
        }
        var msg = "";
        if (key) {
            msg = statusEl.getAttribute("data-" + key) || fallback || "";
        }
        statusEl.textContent = msg;
        statusEl.style.display = msg ? "block" : "none";
    }

    var minYear = 1500;
    var maxYear = new Date().getFullYear();
    if (y0Input && y0Input.getAttribute("min")) {
        var minAttr = parseInt(y0Input.getAttribute("min"), 10);
        if (!Number.isNaN(minAttr)) {
            minYear = minAttr;
        }
    }
    if (y1Input && y1Input.getAttribute("max")) {
        var maxAttr = parseInt(y1Input.getAttribute("max"), 10);
        if (!Number.isNaN(maxAttr)) {
            maxYear = maxAttr;
        }
    }

    function clampYear(val, fallback) {
        var parsed = parseInt(val, 10);
        if (Number.isNaN(parsed)) {
            return fallback;
        }
        return Math.min(maxYear, Math.max(minYear, parsed));
    }

    function getYears() {
        var y0 = clampYear(y0Input && y0Input.value, 1800);
        var y1 = clampYear(y1Input && y1Input.value, maxYear);
        if (y0 > y1) {
            var tmp = y0;
            y0 = y1;
            y1 = tmp;
        }
        return { y0: y0, y1: y1 };
    }

    function syncInputs(y0, y1) {
        if (y0Input) {
            y0Input.value = y0;
        }
        if (y1Input) {
            y1Input.value = y1;
        }
    }

    function fetchHeatmap() {
        if (!mapReady) {
            return;
        }
        setStatus("loading", "Carregant...");
        var years = getYears();
        var params = new URLSearchParams();
        if (years.y0) {
            params.set("y0", years.y0);
        }
        if (years.y1) {
            params.set("y1", years.y1);
        }

        fetch("/api/cognoms/" + cognomID + "/heatmap?" + params.toString(), { credentials: "same-origin" })
            .then(function (res) { return res.json(); })
            .then(function (data) {
                var points = (data && data.points) ? data.points : [];
                if (!Array.isArray(points)) {
                    points = [];
                }
                var heatPoints = [];
                var bounds = [];
                var maxWeight = 0;
                var singlePoint = null;
                points.forEach(function (p) {
                    if (p.w > maxWeight) {
                        maxWeight = p.w;
                    }
                });
                var denom = maxWeight || 1;
                points.forEach(function (p) {
                    var lat = parseFloat(p.lat);
                    var lon = parseFloat(p.lon);
                    if (Number.isNaN(lat) || Number.isNaN(lon)) {
                        return;
                    }
                    var weight = p.w / denom;
                    if (weight < 0.5) {
                        weight = 0.75;
                    }
                    if (weight > 1) {
                        weight = 1;
                    }
                    heatPoints.push([lat, lon, weight]);
                    bounds.push([lat, lon]);
                    if (!singlePoint) {
                        singlePoint = [lat, lon];
                    }
                });
                if (heat) {
                    heat.setLatLngs(heatPoints);
                    if (typeof heat.redraw === "function") {
                        heat.redraw();
                    }
                }
                markerLayer.clearLayers();
                map.invalidateSize();
                if (bounds.length > 1) {
                    map.fitBounds(bounds, { padding: [20, 20] });
                    setStatus("");
                } else if (singlePoint) {
                    map.setView(singlePoint, 8);
                    setStatus("");
                } else {
                    setStatus("empty", "");
                }
                points.forEach(function (p) {
                    var lat = parseFloat(p.lat);
                    var lon = parseFloat(p.lon);
                    if (Number.isNaN(lat) || Number.isNaN(lon)) {
                        return;
                    }
                    var weight = p.w / denom;
                    if (weight < 0.5) {
                        weight = 0.75;
                    }
                    if (weight > 1) {
                        weight = 1;
                    }
                    var radius = 5 + Math.round(10 * weight);
                    L.circleMarker([lat, lon], {
                        pane: "cognomMarkers",
                        radius: radius,
                        color: "#0b4f75",
                        weight: 1,
                        fillColor: "#1f78b4",
                        fillOpacity: 0.25
                    }).addTo(markerLayer);
                    L.circle([lat, lon], {
                        pane: "cognomMarkers",
                        radius: 6000 + Math.round(9000 * weight),
                        color: "#fb923c",
                        weight: 1,
                        fillColor: "#fdba74",
                        fillOpacity: 0.08
                    }).addTo(markerLayer);
                });
            })
            .catch(function () {
                if (heat) {
                    heat.setLatLngs([]);
                }
                markerLayer.clearLayers();
                setStatus("error", "");
            });
    }

    if (sliderEl && window.noUiSlider) {
        var years = getYears();
        noUiSlider.create(sliderEl, {
            start: [years.y0, years.y1],
            connect: true,
            step: 1,
            range: { min: minYear, max: maxYear }
        });
        sliderEl.noUiSlider.on("update", function (values) {
            var y0 = Math.round(values[0]);
            var y1 = Math.round(values[1]);
            syncInputs(y0, y1);
        });
        sliderEl.noUiSlider.on("change", function () {
            fetchHeatmap();
        });
    }

    if (updateBtn) {
        updateBtn.addEventListener("click", function () {
            fetchHeatmap();
        });
    }

    if (y0Input) {
        y0Input.addEventListener("change", fetchHeatmap);
    }
    if (y1Input) {
        y1Input.addEventListener("change", fetchHeatmap);
    }

    map.whenReady(function () {
        mapReady = true;
        fetchHeatmap();
    });
})();
