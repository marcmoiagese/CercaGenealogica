document.addEventListener("DOMContentLoaded", function () {
    var archiveInput = document.getElementById("arxiu_search");
    var archiveHidden = document.getElementById("arxiu_id");
    var entityInput = document.getElementById("entitat_religiosa_search");
    var entityHidden = document.getElementById("entitat_religiosa_id");
    var municipalityInput = document.getElementById("municipi_search");
    var municipalityHidden = document.getElementById("municipi_id");
    var entityHelper = document.getElementById("llibre-entitat-helper");
    var municipalityHelper = document.getElementById("llibre-municipi-helper");

    if (!archiveInput || !archiveHidden || !entityInput || !entityHidden || !municipalityInput || !municipalityHidden) {
        return;
    }

    var defaultEntityHelper = entityHelper ? entityHelper.textContent : "";
    var defaultMunicipalityHelper = municipalityHelper ? municipalityHelper.textContent : "";
    var entityTouched = entityHidden.value.trim() !== "";

    function setInputValue(input, hidden, item) {
        hidden.value = item && item.id ? String(item.id) : "";
        input.value = item && item.nom ? String(item.nom) : "";
        hidden.dispatchEvent(new Event("change", { bubbles: true }));
        input.dispatchEvent(new CustomEvent("suggest:select", { detail: { item: item || null } }));
    }

    function clearMunicipalitySelection() {
        municipalityHidden.value = "";
        municipalityInput.value = "";
        municipalityHidden.dispatchEvent(new Event("change", { bubbles: true }));
    }

    function refreshMunicipalityApi() {
        var api = "/api/documentals/llibres/municipis/suggest";
        var entityID = entityHidden.value.trim();
        if (entityID) {
            api += "?entitat_religiosa_id=" + encodeURIComponent(entityID);
        }
        municipalityInput.dataset.api = api;
    }

    function refreshEntityApi() {
        var api = "/api/documentals/llibres/entitats-religioses/suggest";
        var archiveID = archiveHidden.value.trim();
        if (archiveID) {
            api += "?arxiu_id=" + encodeURIComponent(archiveID);
        }
        entityInput.dataset.api = api;
    }

    function updateMunicipalityScopeState(payload) {
        var empty = !!(payload && payload.scope_empty);
        municipalityInput.disabled = empty;
        if (municipalityHelper) {
            municipalityHelper.textContent = empty && payload.scope_message ? payload.scope_message : defaultMunicipalityHelper;
        }
        if (empty) {
            clearMunicipalitySelection();
        }
    }

    function probeMunicipalityScope() {
        refreshMunicipalityApi();
        fetch(municipalityInput.dataset.api + (municipalityInput.dataset.api.indexOf("?") >= 0 ? "&" : "?") + "limit=10", {
            credentials: "same-origin"
        })
            .then(function (resp) { return resp.json(); })
            .then(function (data) {
                updateMunicipalityScopeState(data || {});
            })
            .catch(function () {
                updateMunicipalityScopeState({});
            });
    }

    function applyArchiveRelatedEntities() {
        refreshEntityApi();
        var archiveID = archiveHidden.value.trim();
        if (!archiveID) {
            if (!entityTouched) {
                setInputValue(entityInput, entityHidden, null);
            }
            if (entityHelper) {
                entityHelper.textContent = defaultEntityHelper;
            }
            probeMunicipalityScope();
            return;
        }
        fetch(entityInput.dataset.api + (entityInput.dataset.api.indexOf("?") >= 0 ? "&" : "?") + "limit=25", {
            credentials: "same-origin"
        })
            .then(function (resp) { return resp.json(); })
            .then(function (data) {
                if (entityHelper) {
                    entityHelper.textContent = data && data.archive_related_single ? "Preseleccionada des de l'arxiu perquè només hi ha una entitat religiosa publicada relacionada." : defaultEntityHelper;
                }
                if (!entityTouched && data && Array.isArray(data.items) && data.items.length === 1) {
                    setInputValue(entityInput, entityHidden, data.items[0]);
                }
                probeMunicipalityScope();
            })
            .catch(function () {
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
            setInputValue(entityInput, entityHidden, null);
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
    entityInput.addEventListener("suggest:select", function () {
        entityTouched = true;
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
