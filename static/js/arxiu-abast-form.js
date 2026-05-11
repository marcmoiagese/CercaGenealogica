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

    function apiForKind(value) {
        switch (value) {
            case "religious_entity":
                return "/api/confessional/entitats/suggest";
            case "municipi":
                return "/api/territori/municipis/suggest";
            case "comarca":
                return "/api/territori/nivells/suggest?nivel=2";
            case "provincia":
                return "/api/territori/nivells/suggest?nivel=3";
            case "comunitat_autonoma":
                return "/api/territori/nivells/suggest?nivel=4";
            case "estat":
                return "/api/territori/nivells/suggest?nivel=1";
            default:
                return "";
        }
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
        }
        if (clearSelection && targetID) {
            targetID.value = "";
        }
        if (clearSelection && targetCode) {
            targetCode.value = "";
        }
        if (clearSelection && targetLabel && !textMode) {
            targetLabel.value = "";
        }
        if (clearSelection && search && document.activeElement !== search) {
            search.value = "";
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
