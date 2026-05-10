document.addEventListener("DOMContentLoaded", function () {
    const RELIGIOUS_ENTITY_SUGGEST_API = "/api/documentals/llibres/entitats-religioses/suggest";
    const MUNICIPI_SUGGEST_API = "/api/documentals/llibres/municipis/suggest";
    const archiveInput = document.getElementById("arxiu_search");
    const archiveHidden = document.getElementById("arxiu_id");
    const entityInput = document.getElementById("entitat_religiosa_search");
    const entityHidden = document.getElementById("entitat_religiosa_id");
    const municipalityInput = document.getElementById("municipi_search");
    const municipalityHidden = document.getElementById("municipi_id");
    const entityHelper = document.getElementById("llibre-entitat-helper");
    const municipalityHelper = document.getElementById("llibre-municipi-helper");

    if (!archiveInput || !archiveHidden || !entityInput || !entityHidden || !municipalityInput || !municipalityHidden) {
        return;
    }

    const defaultEntityHelper = entityInput.dataset.helperDefault || (entityHelper ? entityHelper.textContent : "");
    const autoSelectedEntityHelper = entityInput.dataset.helperAutoSelected || defaultEntityHelper;
    const defaultMunicipalityHelper = municipalityInput.dataset.helperDefault || (municipalityHelper ? municipalityHelper.textContent : "");
    const emptyScopeMessage = municipalityInput.dataset.emptyScopeMessage || defaultMunicipalityHelper;

    let entityTouched = entityHidden.value.trim() !== "";
    let entitySelectOrigin = "user";
    let municipalityScopeRequest = 0;
    let relatedEntityRequest = 0;
    let municipalityScopeController = null;
    let relatedEntityController = null;

    function setInputValue(input, hidden, item, origin) {
        const detailOrigin = origin || "user";
        hidden.value = item && item.id ? String(item.id) : "";
        input.value = item && item.nom ? String(item.nom) : "";
        hidden.dispatchEvent(new Event("change", { bubbles: true }));
        input.dispatchEvent(new CustomEvent("suggest:select", { detail: { item: item || null, origin: detailOrigin } }));
    }

    function setEntityValue(item, origin) {
        entitySelectOrigin = origin || "user";
        setInputValue(entityInput, entityHidden, item, entitySelectOrigin);
        entitySelectOrigin = "user";
    }

    function clearMunicipalitySelection() {
        municipalityHidden.value = "";
        municipalityInput.value = "";
        municipalityHidden.dispatchEvent(new Event("change", { bubbles: true }));
    }

    function refreshMunicipalityApi() {
        let api = MUNICIPI_SUGGEST_API;
        const entityID = entityHidden.value.trim();
        if (entityID) {
            api += "?entitat_religiosa_id=" + encodeURIComponent(entityID);
        }
        municipalityInput.dataset.api = api;
    }

    function refreshEntityApi() {
        let api = RELIGIOUS_ENTITY_SUGGEST_API;
        const archiveID = archiveHidden.value.trim();
        if (archiveID) {
            api += "?arxiu_id=" + encodeURIComponent(archiveID);
        }
        entityInput.dataset.api = api;
    }

    function buildReligiousEntitySuggestURL(archiveID, limit) {
        const params = new URLSearchParams();
        if (archiveID) {
            params.set("arxiu_id", archiveID);
        }
        params.set("limit", String(limit));
        return RELIGIOUS_ENTITY_SUGGEST_API + "?" + params.toString();
    }

    function buildMunicipiSuggestURL(entityID, limit) {
        const params = new URLSearchParams();
        if (entityID) {
            params.set("entitat_religiosa_id", entityID);
        }
        params.set("limit", String(limit));
        return MUNICIPI_SUGGEST_API + "?" + params.toString();
    }

    function updateMunicipalityScopeState(payload) {
        const empty = !!(payload && payload.scope_empty);
        municipalityInput.disabled = empty;
        if (municipalityHelper) {
            municipalityHelper.textContent = empty ? (payload.scope_message || emptyScopeMessage) : defaultMunicipalityHelper;
        }
        if (empty) {
            clearMunicipalitySelection();
        }
    }

    function probeMunicipalityScope() {
        refreshMunicipalityApi();
        municipalityScopeRequest += 1;
        const requestID = municipalityScopeRequest;
        if (municipalityScopeController) {
            municipalityScopeController.abort();
        }
        municipalityScopeController = new AbortController();
        fetch(buildMunicipiSuggestURL(entityHidden.value.trim(), 10), {
            credentials: "same-origin",
            signal: municipalityScopeController.signal
        })
            .then(function (resp) { return resp.json(); })
            .then(function (data) {
                if (requestID !== municipalityScopeRequest) {
                    return;
                }
                updateMunicipalityScopeState(data || {});
            })
            .catch(function (err) {
                if (err && err.name === "AbortError") {
                    return;
                }
                if (requestID !== municipalityScopeRequest) {
                    return;
                }
                updateMunicipalityScopeState({});
            });
    }

    function applyArchiveRelatedEntities() {
        refreshEntityApi();
        relatedEntityRequest += 1;
        const requestID = relatedEntityRequest;
        const archiveID = archiveHidden.value.trim();
        if (relatedEntityController) {
            relatedEntityController.abort();
        }
        if (!archiveID) {
            if (!entityTouched) {
                setEntityValue(null, "auto-reset");
            }
            if (entityHelper) {
                entityHelper.textContent = defaultEntityHelper;
            }
            probeMunicipalityScope();
            return;
        }
        relatedEntityController = new AbortController();
        fetch(buildReligiousEntitySuggestURL(archiveID, 25), {
            credentials: "same-origin",
            signal: relatedEntityController.signal
        })
            .then(function (resp) { return resp.json(); })
            .then(function (data) {
                if (requestID !== relatedEntityRequest) {
                    return;
                }
                let autoSelected = false;
                if (!entityTouched && data && Array.isArray(data.items) && data.items.length === 1) {
                    setEntityValue(data.items[0], "auto");
                    autoSelected = true;
                }
                if (entityHelper) {
                    entityHelper.textContent = autoSelected ? autoSelectedEntityHelper : defaultEntityHelper;
                }
                probeMunicipalityScope();
            })
            .catch(function (err) {
                if (err && err.name === "AbortError") {
                    return;
                }
                if (requestID !== relatedEntityRequest) {
                    return;
                }
                if (entityHelper) {
                    entityHelper.textContent = defaultEntityHelper;
                }
                probeMunicipalityScope();
            });
    }

    archiveInput.addEventListener("input", function () {
        if (!archiveInput.value.trim()) {
            archiveHidden.value = "";
            archiveHidden.dispatchEvent(new Event("change", { bubbles: true }));
        }
    });
    archiveInput.addEventListener("suggest:select", function () {
        if (!entityTouched) {
            setEntityValue(null, "auto-reset");
        }
        clearMunicipalitySelection();
        applyArchiveRelatedEntities();
    });
    archiveHidden.addEventListener("change", function () {
        refreshEntityApi();
    });

    entityInput.addEventListener("input", function () {
        entityTouched = entityInput.value.trim() !== "";
        if (!entityInput.value.trim()) {
            entityHidden.value = "";
            entityHidden.dispatchEvent(new Event("change", { bubbles: true }));
        }
    });
    entityInput.addEventListener("suggest:select", function (event) {
        const detail = event.detail || {};
        const origin = detail.origin || entitySelectOrigin || "user";
        const hasItem = !!(detail.item && detail.item.id);
        entityTouched = origin === "user" ? hasItem : entityTouched;
        if (origin === "auto-reset") {
            entityTouched = false;
        }
        clearMunicipalitySelection();
        probeMunicipalityScope();
    });
    entityHidden.addEventListener("change", function () {
        refreshMunicipalityApi();
        probeMunicipalityScope();
    });

    refreshEntityApi();
    refreshMunicipalityApi();
    probeMunicipalityScope();
    if (!entityTouched) {
        applyArchiveRelatedEntities();
    }
});
