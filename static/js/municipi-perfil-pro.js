(function () {
    const dataEl = document.getElementById("municipiProData");
    if (!dataEl) return;

    let data = {};
    try {
        data = JSON.parse(dataEl.textContent || "{}");
    } catch (err) {
        data = {};
    }

    const apiUrl = data.mapes_api || "";
    const viewport = document.getElementById("muniMapsViewport");
    const slidesEl = document.getElementById("muniMapsSlides");
    const chipText = document.getElementById("muniMapsChipText");
    const tooltip = document.getElementById("muniMapTooltip");
    const btnPrev = document.getElementById("muniMapPrev");
    const btnNext = document.getElementById("muniMapNext");
    const gear = document.getElementById("muniMapsGear");
    const popover = document.getElementById("muniMapsPopover");
    const popoverClose = document.getElementById("muniMapsPopoverClose");
    const optHouses = document.getElementById("optLayerHouses");
    const optStreets = document.getElementById("optLayerStreets");
    const optRivers = document.getElementById("optLayerRivers");
    const optToponyms = document.getElementById("optLayerToponyms");
    const optBounds = document.getElementById("optLayerBounds");
    const optElements = document.getElementById("optLayerElements");

    if (!slidesEl || !viewport || !apiUrl) return;

    const emptyLabel = viewport.dataset.empty || "";
    const loadingLabel = viewport.dataset.loading || "";
    const chipDefault = viewport.dataset.chipDefault || "";

    let slides = [];
    let currentIdx = 0;

    const showMessage = (text) => {
        slidesEl.innerHTML = "";
        const holder = document.createElement("div");
        holder.className = "muni-map-slide is-active";
        holder.innerHTML = `<div class="muni-map-empty">${text}</div>`;
        slidesEl.appendChild(holder);
        if (chipText) chipText.textContent = chipDefault || "";
    };

    const applyLayersTo = (svg) => {
        if (!svg) return;
        const layers = [
            { key: "houses", input: optHouses },
            { key: "streets", input: optStreets },
            { key: "rivers", input: optRivers },
            { key: "elements", input: optElements },
            { key: "toponyms", input: optToponyms },
            { key: "bounds", input: optBounds },
        ];
        layers.forEach((layer) => {
            const group = svg.querySelector(`g[data-layer="${layer.key}"]`);
            if (!group) return;
            const show = !layer.input || layer.input.checked;
            group.style.display = show ? "" : "none";
        });
    };

    const applyLayersAll = () => {
        slides.forEach((slide) => applyLayersTo(slide.svg));
    };

    const updateChip = (idx) => {
        if (!chipText) return;
        const info = slides[idx];
        if (!info) {
            chipText.textContent = chipDefault || "";
            return;
        }
        const parts = [];
        if (info.title) parts.push(info.title);
        if (info.period) parts.push(info.period);
        chipText.textContent = parts.length ? parts.join(" · ") : chipDefault;
    };

    const setActive = (idx) => {
        if (!slides.length) return;
        const safeIdx = ((idx % slides.length) + slides.length) % slides.length;
        slides.forEach((slide, i) => {
            slide.el.classList.toggle("is-active", i === safeIdx);
        });
        currentIdx = safeIdx;
        updateChip(currentIdx);
        applyLayersAll();
    };

    const setupTooltip = () => {
        if (!tooltip) return;
        const hideTooltip = () => {
            tooltip.classList.remove("is-visible");
        };
        viewport.addEventListener("mousemove", (evt) => {
            const target = evt.target.closest("[data-tooltip]");
            if (!target) {
                hideTooltip();
                return;
            }
            const rect = viewport.getBoundingClientRect();
            const x = evt.clientX - rect.left;
            const y = evt.clientY - rect.top;
            const label = target.getAttribute("data-tooltip") || "";
            const sub = target.getAttribute("data-sub") || "";
            tooltip.textContent = sub ? `${label} · ${sub}` : label;
            tooltip.style.left = `${x + 14}px`;
            tooltip.style.top = `${y + 14}px`;
            tooltip.classList.add("is-visible");
        });
        viewport.addEventListener("mouseleave", hideTooltip);
    };

    if (gear && popover) {
        const togglePopover = () => {
            popover.hidden = !popover.hidden;
        };
        gear.addEventListener("click", togglePopover);
        if (popoverClose) {
            popoverClose.addEventListener("click", () => {
                popover.hidden = true;
            });
        }
        document.addEventListener("click", (evt) => {
            if (popover.hidden) return;
            if (popover.contains(evt.target) || gear.contains(evt.target)) return;
            popover.hidden = true;
        });
    }

    [optHouses, optStreets, optRivers, optElements, optToponyms, optBounds].forEach((opt) => {
        if (!opt) return;
        opt.addEventListener("change", applyLayersAll);
    });

    if (btnPrev) {
        btnPrev.addEventListener("click", () => setActive(currentIdx - 1));
    }
    if (btnNext) {
        btnNext.addEventListener("click", () => setActive(currentIdx + 1));
    }

    if (loadingLabel) {
        showMessage(loadingLabel);
    }

    fetch(apiUrl)
        .then((res) => {
            if (!res.ok) throw new Error("failed");
            return res.json();
        })
        .then((payload) => {
            const items = Array.isArray(payload.items) ? payload.items : [];
            const withVersions = items.filter((item) => Number(item.current_version_id) > 0);
            if (!withVersions.length) {
                showMessage(emptyLabel || "Empty");
                return;
            }
            slides = withVersions.map((item, idx) => {
                const el = document.createElement("div");
                el.className = "muni-map-slide";
                el.dataset.index = String(idx);
                const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
                svg.setAttribute("aria-label", item.title || chipDefault || "map");
                el.appendChild(svg);
                slidesEl.appendChild(el);
                const periodParts = [];
                if (item.period_label) periodParts.push(item.period_label);
                if (item.period_start || item.period_end) {
                    const start = item.period_start ? String(item.period_start) : "";
                    const end = item.period_end ? String(item.period_end) : "";
                    periodParts.push([start, end].filter(Boolean).join("-"));
                }
                return {
                    el: el,
                    svg: svg,
                    title: item.title || "",
                    period: periodParts.join(" · "),
                    versionId: item.current_version_id,
                };
            });

            slides.forEach((slide) => {
                fetch(`/api/mapes/versions/${slide.versionId}`)
                    .then((res) => {
                        if (!res.ok) throw new Error("failed");
                        return res.json();
                    })
                    .then((version) => {
                        if (!version || !version.data || !window.CGMap) return;
                        window.CGMap.render(slide.svg, version.data);
                        applyLayersTo(slide.svg);
                    })
                    .catch(() => {});
            });

            setupTooltip();
            setActive(0);
        })
        .catch(() => {
            showMessage(emptyLabel || "Empty");
        });
})();
