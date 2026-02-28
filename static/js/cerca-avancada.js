document.addEventListener("DOMContentLoaded", () => {
    const form = document.getElementById("advancedSearchForm");
    if (!form) {
        return;
    }

    const api = form.dataset.api || "/api/search";
    const statusEl = document.getElementById("advancedSearchStatus");
    const resultsEl = document.getElementById("advancedSearchResults");
    const facetsEl = document.getElementById("advancedSearchFacets");
    const paginationEl = document.getElementById("advancedSearchPagination");
    const clearButtons = Array.from(form.querySelectorAll("[data-advanced-clear]"));
    const legacyClearBtn = document.getElementById("advancedSearchClear");
    const pageInput = form.querySelector("input[name='page']");
    const quickQueryInput = form.querySelector("input[name='q']");
    const entitySelect = document.getElementById("searchEntity");
    const tipusActeSelect = document.getElementById("tipusActe");
    const territoriInput = document.getElementById("territoriSearch");
    const territoriSuggestions = document.getElementById("territoriSuggestions");
    const territoriMunicipiId = document.getElementById("territoriMunicipiId");
    const territoriTypeInput = document.getElementById("ancestor_type");
    const territoriIdInput = document.getElementById("ancestor_id");
    const territoriLabelInput = document.getElementById("ancestor_label");
    const paisSelect = document.getElementById("searchPaisSelect");
    const levelSelects = Array.from(form.querySelectorAll("[data-territori-level]"));
    const advancedPanel = document.getElementById("advancedPanel");
    const advancedToggle = document.getElementById("btnAdvanced");
    const advancedClose = document.getElementById("btnAdvancedClose");

    const labels = {
        loading: form.dataset.labelLoading || "Loading...",
        error: form.dataset.labelError || "Error",
        empty: form.dataset.labelEmpty || "No results",
        total: form.dataset.labelTotal || "Total: %d",
        prev: form.dataset.labelPrev || "Prev",
        next: form.dataset.labelNext || "Next",
        all: form.dataset.labelAll || "All",
        entityPersona: form.dataset.labelEntityPersona || "Persona",
        entityRegistre: form.dataset.labelEntityRegistre || "Registre",
        entityEspai: form.dataset.labelEntityEspai || "Arbre",
        entityEspaiPersona: form.dataset.labelEntityEspaiPersona || "Persona d'arbre",
        facetEntity: form.dataset.labelFacetEntity || "Entity",
        facetTipus: form.dataset.labelFacetTipus || "Tipus",
    };

    let facetCache = null;
    let facetCacheKey = "";

    function formatLabel(template, value) {
        return String(template).replace("%d", value).replace("{total}", value);
    }

    function buildFacetKey(params) {
        const skipKeys = new Set(["page", "page_size", "entity", "tipus_acte"]);
        const entries = [];
        params.forEach((value, key) => {
            if (skipKeys.has(key)) {
                return;
            }
            entries.push([key, value]);
        });
        entries.sort((a, b) => {
            if (a[0] === b[0]) {
                return a[1].localeCompare(b[1]);
            }
            return a[0].localeCompare(b[0]);
        });
        return entries.map(([key, value]) => `${key}=${value}`).join("&");
    }

    function hasSearchFilters(params) {
        const keys = Array.from(params.keys());
        const skipKeys = new Set(["page", "page_size", "ancestor_label", "entitat_label", "arxiu_label", "llibre_label"]);
        for (const key of keys) {
            if (skipKeys.has(key)) {
                continue;
            }
            const value = (params.get(key) || "").trim();
            if (value !== "") {
                return true;
            }
        }
        if (quickQueryInput && quickQueryInput.value.trim() !== "") {
            return true;
        }
        return false;
    }

    function buildParams(overrides = {}) {
        const params = new URLSearchParams();
        const data = new FormData(form);
        data.forEach((value, key) => {
            const val = String(value).trim();
            if (val === "") {
                return;
            }
            params.set(key, val);
        });
        if (quickQueryInput) {
            const q = quickQueryInput.value.trim();
            if (q !== "") {
                params.set("q", q);
            } else {
                params.delete("q");
            }
        }
        Object.keys(overrides).forEach((key) => {
            if (overrides[key] === null || overrides[key] === undefined || overrides[key] === "") {
                params.delete(key);
                return;
            }
            params.set(key, String(overrides[key]));
        });
        applyTerritoryParams(params);
        return params;
    }

    function applyTerritoryParams(params) {
        const municipiId = (territoriMunicipiId && territoriMunicipiId.value || params.get("municipi_id") || "").trim();
        const paisId = (paisSelect && paisSelect.value || params.get("pais_id") || "").trim();
        const existingType = (territoriTypeInput && territoriTypeInput.value || params.get("ancestor_type") || "").trim();
        const existingId = (territoriIdInput && territoriIdInput.value || params.get("ancestor_id") || "").trim();
        let deepestLevel = "";
        levelSelects.forEach((select) => {
            const value = select.value.trim();
            if (value) {
                deepestLevel = value;
            }
        });
        if (municipiId) {
            params.set("ancestor_type", "municipi");
            params.set("ancestor_id", municipiId);
            if (territoriInput && territoriInput.value.trim()) {
                params.set("ancestor_label", territoriInput.value.trim());
            }
            return;
        }
        if (deepestLevel) {
            params.set("ancestor_type", "nivell");
            params.set("ancestor_id", deepestLevel);
            params.delete("ancestor_label");
            return;
        }
        if (paisId) {
            params.set("ancestor_type", "pais");
            params.set("ancestor_id", paisId);
            params.delete("ancestor_label");
            return;
        }
        if (existingType && existingId) {
            params.set("ancestor_type", existingType);
            params.set("ancestor_id", existingId);
            return;
        }
        params.delete("ancestor_type");
        params.delete("ancestor_id");
        params.delete("ancestor_label");
    }

    function updateURL(params) {
        const url = new URL(window.location.href);
        url.search = params.toString();
        history.replaceState(null, "", url.toString());
    }

    function setAdvanced(open, persist) {
        if (!advancedPanel) return;
        advancedPanel.classList.toggle("is-open", open);
        advancedPanel.setAttribute("aria-hidden", open ? "false" : "true");
        if (advancedToggle) {
            advancedToggle.classList.toggle("is-active", open);
        }
        if (persist) {
            try {
                if (open) {
                    sessionStorage.setItem("advancedSearchOpen", "1");
                } else {
                    sessionStorage.removeItem("advancedSearchOpen");
                }
            } catch (err) {
                return;
            }
        }
    }

    if (advancedToggle) {
        advancedToggle.addEventListener("click", () => {
            const open = !(advancedPanel && advancedPanel.classList.contains("is-open"));
            setAdvanced(open, true);
        });
    }
    if (advancedClose) {
        advancedClose.addEventListener("click", () => setAdvanced(false, true));
    }

    function clearResults() {
        resultsEl.innerHTML = "";
        facetsEl.innerHTML = "";
        paginationEl.innerHTML = "";
    }

    function tipusLabelMap() {
        const map = {};
        if (!tipusActeSelect) {
            return map;
        }
        Array.from(tipusActeSelect.options || []).forEach((opt) => {
            const value = String(opt.value || "").trim();
            const label = String(opt.textContent || "").trim();
            if (value) {
                map[value] = label;
            }
        });
        return map;
    }

    function renderFacetGroup(title, items) {
        if (!items || items.length === 0) {
            return null;
        }
        const group = document.createElement("div");
        group.className = "facet-group";
        if (title) {
            const heading = document.createElement("div");
            heading.className = "facet-group-title";
            heading.textContent = title;
            group.appendChild(heading);
        }
        const list = document.createElement("div");
        list.className = "facet-list";
        items.forEach((item) => {
            const button = document.createElement("button");
            button.type = "button";
            button.className = "facet-row";
            if (item.active) {
                button.classList.add("active");
            }
            const left = document.createElement("span");
            left.className = "facet-left";
            const icon = document.createElement("span");
            icon.className = "facet-icon";
            if (item.icon) {
                const i = document.createElement("i");
                i.className = `fas ${item.icon}`;
                icon.appendChild(i);
            }
            const name = document.createElement("span");
            name.className = "facet-name";
            name.textContent = item.label;
            left.appendChild(icon);
            left.appendChild(name);
            const count = document.createElement("span");
            count.className = "facet-count";
            count.textContent = String(item.count || 0);
            button.appendChild(left);
            button.appendChild(count);
            button.addEventListener("click", () => {
                if (item.onClick) {
                    item.onClick();
                }
            });
            list.appendChild(button);
        });
        group.appendChild(list);
        return group;
    }

    function renderFacets(facets, total) {
        facetsEl.innerHTML = "";
        if (!facets) {
            return;
        }
        const sourceFacets = facetCache ? facetCache.facets : facets;
        const sourceTotal = facetCache ? facetCache.total : total;
        const entityCounts = sourceFacets.entity_type || {};
        const tipusCounts = sourceFacets.tipus_acte || {};
        const tipusLabels = tipusLabelMap();
        const activeEntity = entitySelect ? entitySelect.value : "all";
        const activeTipus = tipusActeSelect ? tipusActeSelect.value : "";

        const entityItems = [];
        const applyEntityFilter = (entity) => {
            if (entitySelect) {
                entitySelect.value = entity;
            }
            if (tipusActeSelect) {
                tipusActeSelect.value = "";
            }
            runSearch(1);
        };
        const applyTipusFilter = (tipus) => {
            if (entitySelect) {
                entitySelect.value = "registre_raw";
            }
            if (tipusActeSelect) {
                tipusActeSelect.value = tipus;
            }
            runSearch(1);
        };

        entityItems.push({
            label: labels.all,
            count: sourceTotal || 0,
            active: activeEntity === "all" && !activeTipus,
            icon: "fa-layer-group",
            onClick: () => applyEntityFilter("all"),
        });
        entityItems.push({
            label: labels.entityPersona,
            count: entityCounts.persona || 0,
            active: activeEntity === "persona",
            icon: "fa-user",
            onClick: () => applyEntityFilter("persona"),
        });
        entityItems.push({
            label: labels.entityRegistre,
            count: entityCounts.registre_raw || 0,
            active: activeEntity === "registre_raw" && !activeTipus,
            icon: "fa-book-open",
            onClick: () => applyEntityFilter("registre_raw"),
        });
        entityItems.push({
            label: labels.entityEspai,
            count: entityCounts.espai_arbre || 0,
            active: activeEntity === "espai_arbre",
            icon: "fa-tree",
            onClick: () => applyEntityFilter("espai_arbre"),
        });
        entityItems.push({
            label: labels.entityEspaiPersona,
            count: entityCounts.espai_persona || 0,
            active: activeEntity === "espai_persona",
            icon: "fa-user",
            onClick: () => applyEntityFilter("espai_persona"),
        });

        const tipusItems = [];
        const tipusKeys = Object.keys(tipusCounts || {});
        if (tipusActeSelect) {
            Array.from(tipusActeSelect.options || []).forEach((opt) => {
                const key = String(opt.value || "").trim();
                if (!key) return;
                if (!tipusKeys.includes(key)) {
                    tipusKeys.push(key);
                }
            });
        }
        tipusKeys.forEach((key) => {
            const count = tipusCounts[key] || 0;
            if (!count && activeTipus !== key) {
                return;
            }
            const label = tipusLabels[key] || key;
            tipusItems.push({
                label: label,
                count: count,
                active: activeTipus === key,
                icon: "fa-file-lines",
                onClick: () => applyTipusFilter(key),
            });
        });

        const entityGroup = renderFacetGroup(labels.facetEntity, entityItems);
        const tipusGroup = renderFacetGroup(labels.facetTipus, tipusItems);
        if (entityGroup) {
            facetsEl.appendChild(entityGroup);
        }
        if (tipusGroup) {
            facetsEl.appendChild(tipusGroup);
        }
    }

    function renderResults(data) {
        clearResults();
        const items = Array.isArray(data.items) ? data.items : [];
        const total = data.total || 0;
        if (statusEl) {
            statusEl.textContent = total > 0 ? formatLabel(labels.total, total) : labels.empty;
        }
        const activeEntity = entitySelect ? entitySelect.value : "all";
        const activeTipus = tipusActeSelect ? tipusActeSelect.value : "";
        if (activeEntity === "all" && !activeTipus) {
            facetCache = { facets: data.facets || {}, total: total };
        }
        renderFacets(data.facets || {}, total);
        if (items.length === 0) {
            const empty = document.createElement("div");
            empty.className = "cerca-avancada-empty";
            empty.textContent = labels.empty;
            resultsEl.appendChild(empty);
            return;
        }

        const entity = (data.query && data.query.entity) || "";
        if (entity === "all") {
            const grouped = {
                persona: [],
                registre_raw: [],
                espai_arbre: [],
                espai_persona: [],
            };
            items.forEach((item) => {
                if (item.entity_type === "persona") {
                    grouped.persona.push(item);
                } else if (item.entity_type === "registre_raw") {
                    grouped.registre_raw.push(item);
                } else if (item.entity_type === "espai_arbre") {
                    grouped.espai_arbre.push(item);
                } else if (item.entity_type === "espai_persona") {
                    grouped.espai_persona.push(item);
                }
            });
            if (grouped.persona.length > 0) {
                resultsEl.appendChild(renderSection(labels.entityPersona, grouped.persona));
            }
            if (grouped.registre_raw.length > 0) {
                resultsEl.appendChild(renderSection(labels.entityRegistre, grouped.registre_raw));
            }
            if (grouped.espai_arbre.length > 0) {
                resultsEl.appendChild(renderSection(labels.entityEspai, grouped.espai_arbre));
            }
            if (grouped.espai_persona.length > 0) {
                resultsEl.appendChild(renderSection(labels.entityEspaiPersona, grouped.espai_persona));
            }
        } else {
            resultsEl.appendChild(renderSection("", items));
        }

        renderPagination(data.page || 1, data.total_pages || 1);
    }

    function renderSection(title, items) {
        const wrapper = document.createElement("div");
        if (title) {
            const heading = document.createElement("h3");
            heading.textContent = title;
            wrapper.appendChild(heading);
        }
        items.forEach((item) => {
            const card = document.createElement("div");
            card.className = "cerca-result-card";
            const header = document.createElement("div");
            header.className = "cerca-result-header";
            const badge = document.createElement("span");
            badge.className = "badge";
            badge.textContent = item.entity_type_label || item.entity_type || "";
            const link = document.createElement("a");
            link.className = "cerca-result-title";
            link.href = item.url || "#";
            link.textContent = item.title || "";
            header.appendChild(badge);
            header.appendChild(link);
            card.appendChild(header);
            if (item.subtitle) {
                const meta = document.createElement("div");
                meta.className = "cerca-result-meta";
                meta.textContent = item.subtitle;
                card.appendChild(meta);
            }
            if (item.match_info) {
                const match = document.createElement("div");
                match.className = "cerca-result-match";
                match.textContent = item.match_info;
                card.appendChild(match);
            }
            if (Array.isArray(item.reasons) && item.reasons.length > 0) {
                const reasons = document.createElement("div");
                reasons.className = "cerca-result-reasons";
                item.reasons.forEach((reason) => {
                    const span = document.createElement("span");
                    span.className = "badge badge-muted";
                    span.textContent = reason.label || reason.code || "";
                    reasons.appendChild(span);
                });
                card.appendChild(reasons);
            }
            wrapper.appendChild(card);
        });
        return wrapper;
    }

    function renderPagination(page, totalPages) {
        paginationEl.innerHTML = "";
        if (totalPages <= 1) {
            return;
        }
        const makeBtn = (label, targetPage, disabled) => {
            const btn = document.createElement("button");
            btn.type = "button";
            btn.className = "boto-secundari";
            btn.textContent = label;
            btn.disabled = disabled;
            btn.addEventListener("click", () => runSearch(targetPage));
            return btn;
        };
        paginationEl.appendChild(makeBtn(labels.prev, Math.max(1, page - 1), page <= 1));
        const info = document.createElement("span");
        info.className = "muted";
        info.textContent = `${page} / ${totalPages}`;
        paginationEl.appendChild(info);
        paginationEl.appendChild(makeBtn(labels.next, Math.min(totalPages, page + 1), page >= totalPages));
    }

    let territoriItems = [];
    let territoriIndex = -1;
    let territoriTimer = null;

    function clearTerritoriSuggestions() {
        if (!territoriSuggestions) return;
        territoriSuggestions.innerHTML = "";
        territoriSuggestions.classList.remove("is-open");
        territoriItems = [];
        territoriIndex = -1;
    }

    function setTerritoriActive(index) {
        if (!territoriSuggestions) return;
        const items = Array.from(territoriSuggestions.querySelectorAll("li"));
        items.forEach((el, idx) => {
            if (idx === index) {
                el.classList.add("is-active");
            } else {
                el.classList.remove("is-active");
            }
        });
        territoriIndex = index;
    }

    function selectHasOption(select, value) {
        if (!select || !value) return false;
        return Array.from(select.options || []).some((opt) => opt.value === String(value));
    }

    function applyTerritoriSuggestion(item) {
        if (!item) return;
        let needsReload = false;
        if (territoriMunicipiId) {
            if (item.id) {
                if (selectHasOption(territoriMunicipiId, item.id)) {
                    territoriMunicipiId.value = String(item.id);
                } else {
                    needsReload = true;
                }
            } else {
                territoriMunicipiId.value = "";
            }
        }
        if (territoriInput) {
            territoriInput.value = item.label || "";
        }
        if (territoriLabelInput) {
            territoriLabelInput.value = item.label || "";
        }
        if (territoriTypeInput) {
            territoriTypeInput.value = "municipi";
        }
        if (territoriIdInput) {
            territoriIdInput.value = item.id ? String(item.id) : "";
        }
        if (paisSelect) {
            if (item.pais_id) {
                if (selectHasOption(paisSelect, item.pais_id)) {
                    paisSelect.value = String(item.pais_id);
                } else {
                    needsReload = true;
                }
            } else {
                paisSelect.value = "";
            }
        }
        const levels = Array.isArray(item.nivells) ? item.nivells : [];
        if (levels.some((val) => val)) {
            if (levelSelects.length === 0) {
                needsReload = true;
            }
        }
        levelSelects.forEach((select) => {
            const level = parseInt(select.dataset.level || "0", 10);
            const desired = level > 0 ? levels[level - 1] : null;
            if (desired) {
                if (selectHasOption(select, desired)) {
                    select.value = String(desired);
                } else {
                    needsReload = true;
                }
            } else {
                select.value = "";
            }
        });
        if (needsReload) {
            const params = buildParams({ page: 1 });
            if (item.pais_id) {
                params.set("pais_id", String(item.pais_id));
            } else {
                params.delete("pais_id");
            }
            for (let i = 0; i < 7; i++) {
                const key = `nivell_id_${i + 1}`;
                const val = levels[i];
                if (val) {
                    params.set(key, String(val));
                } else {
                    params.delete(key);
                }
            }
            if (item.id) {
                params.set("municipi_id", String(item.id));
            }
            if (item.label) {
                params.set("ancestor_label", item.label);
            }
            window.location.href = `${window.location.pathname}?${params.toString()}`;
            return;
        }
        clearTerritoriSuggestions();
    }

    function renderTerritoriSuggestions(items) {
        if (!territoriSuggestions) return;
        const emptyLabel = territoriInput ? (territoriInput.dataset.emptyLabel || "No results") : "No results";
        territoriSuggestions.innerHTML = "";
        territoriItems = items || [];
        territoriIndex = -1;
        if (!items || items.length === 0) {
            const li = document.createElement("li");
            li.textContent = emptyLabel;
            li.className = "suggestion-empty";
            territoriSuggestions.appendChild(li);
            territoriSuggestions.classList.add("is-open");
            return;
        }
        items.forEach((item, idx) => {
            const li = document.createElement("li");
            li.dataset.index = String(idx);
            const title = document.createElement("span");
            title.className = "suggestion-title";
            title.textContent = item.label || "";
            li.appendChild(title);
            if (item.context) {
                const context = document.createElement("span");
                context.className = "suggestion-context";
                context.textContent = item.context;
                li.appendChild(context);
            }
            li.addEventListener("click", () => applyTerritoriSuggestion(item));
            territoriSuggestions.appendChild(li);
        });
        territoriSuggestions.classList.add("is-open");
    }

    function fetchTerritoriSuggestions(query) {
        if (!territoriInput) return;
        const api = territoriInput.dataset.api || "/api/scopes/search";
        const type = territoriInput.dataset.type || "municipi";
        const params = new URLSearchParams();
        params.set("q", query);
        params.set("type", type);
        params.set("limit", "10");
        fetch(`${api}?${params.toString()}`, { credentials: "same-origin" })
            .then((resp) => resp.json())
            .then((data) => {
                renderTerritoriSuggestions(data.items || []);
            })
            .catch(() => {
                clearTerritoriSuggestions();
            });
    }

    if (territoriInput) {
        territoriInput.addEventListener("input", () => {
            if (territoriMunicipiId) {
                territoriMunicipiId.value = "";
            }
            if (territoriLabelInput) {
                territoriLabelInput.value = "";
            }
            if (territoriTypeInput) {
                territoriTypeInput.value = "";
            }
            if (territoriIdInput) {
                territoriIdInput.value = "";
            }
            const value = territoriInput.value.trim();
            if (value.length < 1) {
                clearTerritoriSuggestions();
                return;
            }
            clearTimeout(territoriTimer);
            territoriTimer = setTimeout(() => fetchTerritoriSuggestions(value), 300);
        });
        territoriInput.addEventListener("keydown", (event) => {
            if (!territoriSuggestions || territoriSuggestions.children.length === 0) {
                return;
            }
            if (event.key === "ArrowDown") {
                event.preventDefault();
                const next = Math.min(territoriIndex + 1, territoriSuggestions.children.length - 1);
                setTerritoriActive(next);
            } else if (event.key === "ArrowUp") {
                event.preventDefault();
                const prev = Math.max(territoriIndex - 1, 0);
                setTerritoriActive(prev);
            } else if (event.key === "Enter") {
                if (territoriItems.length > 0) {
                    event.preventDefault();
                    const item = territoriItems[territoriIndex >= 0 ? territoriIndex : 0];
                    applyTerritoriSuggestion(item);
                }
            } else if (event.key === "Escape") {
                clearTerritoriSuggestions();
            }
        });
    }

    if (paisSelect) {
        paisSelect.addEventListener("change", () => {
            if (territoriMunicipiId) territoriMunicipiId.value = "";
            if (territoriInput) territoriInput.value = "";
            if (territoriLabelInput) territoriLabelInput.value = "";
            if (territoriTypeInput) territoriTypeInput.value = "";
            if (territoriIdInput) territoriIdInput.value = "";
            const params = buildParams({ page: 1 });
            window.location.href = `${window.location.pathname}?${params.toString()}`;
        });
    }
    if (territoriMunicipiId) {
        territoriMunicipiId.addEventListener("change", () => {
            if (territoriInput) territoriInput.value = "";
            if (territoriLabelInput) territoriLabelInput.value = "";
            if (territoriTypeInput) territoriTypeInput.value = "";
            if (territoriIdInput) territoriIdInput.value = "";
        });
    }
    if (levelSelects.length > 0) {
        levelSelects.forEach((select) => {
            select.addEventListener("change", () => {
                if (territoriMunicipiId) territoriMunicipiId.value = "";
                if (territoriInput) territoriInput.value = "";
                if (territoriLabelInput) territoriLabelInput.value = "";
                if (territoriTypeInput) territoriTypeInput.value = "";
                if (territoriIdInput) territoriIdInput.value = "";
                const params = buildParams({ page: 1 });
                window.location.href = `${window.location.pathname}?${params.toString()}`;
            });
        });
    }
    document.addEventListener("click", (event) => {
        if (!territoriSuggestions || !territoriInput) return;
        if (event.target === territoriInput || territoriSuggestions.contains(event.target)) {
            return;
        }
        clearTerritoriSuggestions();
    });

    async function runSearch(pageOverride) {
        if (pageInput && pageOverride) {
            pageInput.value = String(pageOverride);
        }
        const params = buildParams();
        const nextFacetKey = buildFacetKey(params);
        if (nextFacetKey !== facetCacheKey) {
            facetCache = null;
            facetCacheKey = nextFacetKey;
        }
        if (!hasSearchFilters(params)) {
            clearResults();
            if (statusEl) {
                statusEl.textContent = labels.empty;
            }
            updateURL(params);
            return;
        }
        if (statusEl) {
            statusEl.textContent = labels.loading;
        }
        updateURL(params);
        try {
            const resp = await fetch(`${api}?${params.toString()}`, { credentials: "same-origin" });
            if (!resp.ok) {
                throw new Error("bad response");
            }
            const data = await resp.json();
            renderResults(data);
        } catch (err) {
            clearResults();
            if (statusEl) {
                statusEl.textContent = labels.error;
            }
        }
    }

    form.addEventListener("submit", (event) => {
        event.preventDefault();
        if (pageInput) {
            pageInput.value = "1";
        }
        runSearch();
    });

    if (legacyClearBtn) {
        clearButtons.push(legacyClearBtn);
    }
    clearButtons.forEach((btn) => {
        btn.addEventListener("click", () => {
            form.reset();
            form.querySelectorAll("input[type='hidden']").forEach((input) => {
                if (input.name === "page") {
                    input.value = "1";
                    return;
                }
                input.value = "";
            });
            runSearch(1);
        });
    });

    const params = new URLSearchParams(window.location.search);
    if (hasSearchFilters(params)) {
        runSearch();
    }
    let shouldOpen = false;
    try {
        shouldOpen = sessionStorage.getItem("advancedSearchOpen") === "1";
    } catch (err) {
        shouldOpen = false;
    }
    setAdvanced(shouldOpen, false);
});
