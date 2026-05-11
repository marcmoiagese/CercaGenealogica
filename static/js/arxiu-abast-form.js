document.addEventListener("DOMContentLoaded", () => {
    const kind = document.querySelector("[data-arxiu-abast-kind]");
    if (!kind) {
        return;
    }

    const search = document.getElementById("target_search");
    const targetID = document.getElementById("target_id");
    const targetCode = document.getElementById("target_code");
    const targetLabel = document.getElementById("target_label");
    const textInput = document.getElementById("target_label_text");
    const suggestWrap = document.querySelector("[data-arxiu-abast-suggest-wrap]");
    const textWrap = document.querySelector("[data-arxiu-abast-text-wrap]");
    const suggestions = document.getElementById("target_suggestions");

    function apiForKind(value) {
        switch (value) {
            case "religious_entity":
                return "/api/confessional/entitats/suggest";
            case "municipi":
                return "/api/territori/municipis/suggest";
            case "comarca":
                return "/api/territori/nivells/suggest?target_kind=comarca";
            case "provincia":
                return "/api/territori/nivells/suggest?target_kind=provincia";
            case "comunitat_autonoma":
                return "/api/territori/nivells/suggest?target_kind=comunitat_autonoma";
            case "estat":
                return "/api/territori/nivells/suggest?target_kind=estat";
            case "nivell_administratiu":
                return "/api/territori/nivells/suggest?target_kind=nivell_administratiu";
            default:
                return "";
        }
    }

    function placeholderForKind(value) {
        if (!search) {
            return "";
        }
        switch (value) {
            case "municipi":
                return search.dataset.placeholderMunicipi || search.dataset.placeholderDefault || "";
            case "comarca":
                return search.dataset.placeholderComarca || search.dataset.placeholderDefault || "";
            case "provincia":
                return search.dataset.placeholderProvincia || search.dataset.placeholderDefault || "";
            case "comunitat_autonoma":
                return search.dataset.placeholderComunitatAutonoma || search.dataset.placeholderDefault || "";
            case "estat":
                return search.dataset.placeholderEstat || search.dataset.placeholderDefault || "";
            case "nivell_administratiu":
                return search.dataset.placeholderNivellAdministratiu || search.dataset.placeholderDefault || "";
            case "religious_entity":
                return search.dataset.placeholderReligiousEntity || search.dataset.placeholderDefault || "";
            default:
                return search.dataset.placeholderDefault || "";
        }
    }

    function clearSuggestions() {
        if (!suggestions) {
            return;
        }
        suggestions.innerHTML = "";
        suggestions.classList.remove("is-open");
    }

    function syncMode(clearSelection) {
        const current = kind.value;
        const textMode = current === "institucio" || current === "free_text";
        if (suggestWrap) {
            suggestWrap.hidden = textMode;
        }
        if (textWrap) {
            textWrap.hidden = !textMode;
        }
        if (search) {
            search.dataset.api = apiForKind(current);
            search.placeholder = placeholderForKind(current);
        }
        if (clearSelection && targetID) {
            targetID.value = "";
        }
        if (clearSelection && targetCode) {
            targetCode.value = "";
        }
        if (clearSelection && targetLabel) {
            targetLabel.value = "";
        }
        if (clearSelection && search && document.activeElement !== search) {
            search.value = "";
        }
        if (clearSelection && textInput) {
            textInput.value = "";
        }
        if (clearSelection) {
            clearSuggestions();
        }
    }

    if (search) {
        search.addEventListener("suggest:select", (event) => {
            const detail = event.detail || {};
            const item = detail.item || {};
            if (targetLabel) {
                targetLabel.value = item.nom || "";
            }
            if (targetCode) {
                targetCode.value = item.code || item.codi || "";
            }
        });
    }

    if (textInput) {
        textInput.addEventListener("input", () => {
            if (targetLabel) {
                targetLabel.value = textInput.value.trim();
            }
        });
    }

    kind.addEventListener("change", () => syncMode(true));
    syncMode(false);

    if (textInput && targetLabel && textInput.value.trim() === "" && targetLabel.value.trim() !== "") {
        textInput.value = targetLabel.value;
    }
});
