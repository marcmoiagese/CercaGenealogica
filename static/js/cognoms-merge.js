document.addEventListener("DOMContentLoaded", () => {
    const form = document.querySelector("[data-cognom-merge-form]");
    if (!form) {
        return;
    }

    const searchEndpoint = form.dataset.searchEndpoint || "/cognoms/cerca";
    const canonInput = document.getElementById("cognom-canon-search");
    const canonIdInput = document.getElementById("cognom-canon-id");
    const canonSuggestions = document.getElementById("cognom-canon-suggestions");
    const aliasInput = document.getElementById("cognom-alias-search");
    const aliasIdsInput = document.getElementById("cognom-alias-ids");
    const aliasSuggestions = document.getElementById("cognom-alias-suggestions");
    const aliasSelected = document.getElementById("cognom-alias-selected");
    const variantSuggestionsWrap = document.getElementById("cognom-variant-suggestions");
    const variantSuggestionsList = variantSuggestionsWrap ? variantSuggestionsWrap.querySelector(".cognoms-merge-suggestions-list") : null;
    const variantSuggestionsApi = variantSuggestionsWrap ? variantSuggestionsWrap.dataset.api || "" : "";

    if (!canonInput || !canonIdInput || !canonSuggestions || !aliasInput || !aliasIdsInput || !aliasSuggestions || !aliasSelected) {
        return;
    }

    const selectedAliases = new Map();
    let canonActiveIndex = -1;
    let canonItems = [];
    let aliasActiveIndex = -1;
    let aliasItems = [];
    let canonDebounce = null;
    let aliasDebounce = null;

    function clearSuggestions(list) {
        list.innerHTML = "";
        list.classList.remove("is-open");
    }

    function renderSuggestions(list, items, activeIndex) {
        list.innerHTML = "";
        if (!items || items.length === 0) {
            const li = document.createElement("li");
            li.className = "suggestion-empty";
            li.textContent = list.dataset.emptyLabel || "-";
            list.appendChild(li);
            list.classList.add("is-open");
            return;
        }
        items.forEach((item, idx) => {
            const li = document.createElement("li");
            li.dataset.index = String(idx);
            li.textContent = item.forma || "";
            if (idx === activeIndex) {
                li.classList.add("is-active");
            }
            list.appendChild(li);
        });
        list.classList.add("is-open");
    }

    function updateAliasHidden() {
        aliasIdsInput.value = Array.from(selectedAliases.keys()).join(",");
    }

    function renderAliasTags() {
        aliasSelected.innerHTML = "";
        selectedAliases.forEach((label, id) => {
            const tag = document.createElement("span");
            tag.className = "cognoms-merge-tag";
            tag.textContent = label;
            const btn = document.createElement("button");
            btn.type = "button";
            btn.className = "cognoms-merge-remove";
            btn.setAttribute("aria-label", "Eliminar");
            btn.textContent = "x";
            btn.addEventListener("click", () => {
                selectedAliases.delete(id);
                updateAliasHidden();
                renderAliasTags();
            });
            tag.appendChild(btn);
            aliasSelected.appendChild(tag);
        });
    }

    function clearVariantSuggestions() {
        if (!variantSuggestionsList) return;
        variantSuggestionsList.innerHTML = "";
    }

    function renderVariantSuggestions(items) {
        if (!variantSuggestionsList || !variantSuggestionsWrap) return;
        variantSuggestionsList.innerHTML = "";
        if (!Array.isArray(items) || items.length === 0) {
            const empty = document.createElement("p");
            empty.className = "muted";
            empty.textContent = variantSuggestionsWrap.dataset.emptyLabel || "-";
            variantSuggestionsList.appendChild(empty);
            return;
        }
        items.forEach((item) => {
            const row = document.createElement("div");
            row.className = "cognoms-merge-suggestion-row";
            const label = document.createElement("span");
            label.className = "cognoms-merge-suggestion-label";
            label.textContent = item.forma || "";
            const reason = document.createElement("span");
            reason.className = "badge badge-muted";
            reason.textContent = item.reason_label || item.reason || "";
            const btn = document.createElement("button");
            btn.type = "button";
            btn.className = "boto-secundari btn-mini";
            btn.textContent = "+";
            btn.addEventListener("click", () => applyAlias(item));
            row.appendChild(label);
            row.appendChild(reason);
            row.appendChild(btn);
            variantSuggestionsList.appendChild(row);
        });
    }

    function fetchVariantSuggestions() {
        if (!variantSuggestionsApi || !variantSuggestionsWrap) return;
        const canonID = canonIdInput.value;
        const canonLabel = canonInput.value.trim();
        if (!canonID && canonLabel.length < 2) {
            clearVariantSuggestions();
            return;
        }
        const params = new URLSearchParams();
        if (canonID) {
            params.set("cognom_id", canonID);
        } else {
            params.set("q", canonLabel);
        }
        fetch(`${variantSuggestionsApi}?${params.toString()}`, { credentials: "same-origin" })
            .then((resp) => resp.json())
            .then((data) => renderVariantSuggestions((data && data.items) || []))
            .catch(() => clearVariantSuggestions());
    }

    function fetchSuggestions(query, onDone) {
        const params = new URLSearchParams();
        params.set("q", query);
        fetch(`${searchEndpoint}?${params.toString()}`, { credentials: "same-origin" })
            .then((resp) => resp.json())
            .then((data) => {
                onDone(Array.isArray(data) ? data : []);
            })
            .catch(() => onDone([]));
    }

    function applyCanon(item) {
        if (!item) {
            return;
        }
        canonIdInput.value = item.id ? String(item.id) : "";
        canonInput.value = item.forma || "";
        if (item.id && selectedAliases.has(String(item.id))) {
            selectedAliases.delete(String(item.id));
            updateAliasHidden();
            renderAliasTags();
        }
        clearSuggestions(canonSuggestions);
        fetchVariantSuggestions();
    }

    function applyAlias(item) {
        if (!item || !item.id) {
            return;
        }
        const id = String(item.id);
        if (canonIdInput.value === id) {
            clearSuggestions(aliasSuggestions);
            aliasInput.value = "";
            return;
        }
        if (!selectedAliases.has(id)) {
            selectedAliases.set(id, item.forma || id);
            updateAliasHidden();
            renderAliasTags();
        }
        aliasInput.value = "";
        clearSuggestions(aliasSuggestions);
    }

    canonInput.addEventListener("input", () => {
        canonIdInput.value = "";
        const value = canonInput.value.trim();
        clearTimeout(canonDebounce);
        if (value.length < 1) {
            clearSuggestions(canonSuggestions);
            clearVariantSuggestions();
            return;
        }
        canonDebounce = setTimeout(() => {
            fetchSuggestions(value, (items) => {
                canonItems = items;
                canonActiveIndex = -1;
                renderSuggestions(canonSuggestions, canonItems, canonActiveIndex);
            });
            fetchVariantSuggestions();
        }, 250);
    });

    canonInput.addEventListener("keydown", (event) => {
        if (!canonItems.length) {
            return;
        }
        if (event.key === "ArrowDown") {
            event.preventDefault();
            canonActiveIndex = Math.min(canonActiveIndex + 1, canonItems.length - 1);
            renderSuggestions(canonSuggestions, canonItems, canonActiveIndex);
        } else if (event.key === "ArrowUp") {
            event.preventDefault();
            canonActiveIndex = Math.max(canonActiveIndex - 1, 0);
            renderSuggestions(canonSuggestions, canonItems, canonActiveIndex);
        } else if (event.key === "Enter") {
            event.preventDefault();
            const item = canonItems[canonActiveIndex >= 0 ? canonActiveIndex : 0];
            applyCanon(item);
        } else if (event.key === "Escape") {
            clearSuggestions(canonSuggestions);
        }
    });

    canonSuggestions.addEventListener("click", (event) => {
        const target = event.target.closest("li");
        if (!target || target.classList.contains("suggestion-empty")) {
            return;
        }
        const idx = Number(target.dataset.index || -1);
        if (idx >= 0 && canonItems[idx]) {
            applyCanon(canonItems[idx]);
        }
    });

    aliasInput.addEventListener("input", () => {
        const value = aliasInput.value.trim();
        clearTimeout(aliasDebounce);
        if (value.length < 1) {
            clearSuggestions(aliasSuggestions);
            return;
        }
        aliasDebounce = setTimeout(() => {
            fetchSuggestions(value, (items) => {
                aliasItems = items;
                aliasActiveIndex = -1;
                renderSuggestions(aliasSuggestions, aliasItems, aliasActiveIndex);
            });
        }, 250);
    });

    aliasInput.addEventListener("keydown", (event) => {
        if (!aliasItems.length) {
            return;
        }
        if (event.key === "ArrowDown") {
            event.preventDefault();
            aliasActiveIndex = Math.min(aliasActiveIndex + 1, aliasItems.length - 1);
            renderSuggestions(aliasSuggestions, aliasItems, aliasActiveIndex);
        } else if (event.key === "ArrowUp") {
            event.preventDefault();
            aliasActiveIndex = Math.max(aliasActiveIndex - 1, 0);
            renderSuggestions(aliasSuggestions, aliasItems, aliasActiveIndex);
        } else if (event.key === "Enter") {
            event.preventDefault();
            const item = aliasItems[aliasActiveIndex >= 0 ? aliasActiveIndex : 0];
            applyAlias(item);
        } else if (event.key === "Escape") {
            clearSuggestions(aliasSuggestions);
        }
    });

    aliasSuggestions.addEventListener("click", (event) => {
        const target = event.target.closest("li");
        if (!target || target.classList.contains("suggestion-empty")) {
            return;
        }
        const idx = Number(target.dataset.index || -1);
        if (idx >= 0 && aliasItems[idx]) {
            applyAlias(aliasItems[idx]);
        }
    });

    document.addEventListener("click", (event) => {
        if (!canonSuggestions.contains(event.target) && event.target !== canonInput) {
            clearSuggestions(canonSuggestions);
        }
        if (!aliasSuggestions.contains(event.target) && event.target !== aliasInput) {
            clearSuggestions(aliasSuggestions);
        }
    });
});
