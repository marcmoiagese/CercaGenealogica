document.addEventListener("DOMContentLoaded", () => {
    const modalTo = document.getElementById("cognoms-merge-to-modal");
    const modalFrom = document.getElementById("cognoms-merge-from-modal");
    const formTo = document.getElementById("cognoms-merge-to-form");
    const formFrom = document.getElementById("cognoms-merge-from-form");
    if (!modalTo || !modalFrom || !formTo || !formFrom) {
        return;
    }

    const subtitleTo = document.getElementById("cognoms-merge-to-subtitle");
    const subtitleFrom = document.getElementById("cognoms-merge-from-subtitle");
    const toCanonicalIdInput = document.getElementById("merge-to-canonical-id");
    const toAliasIdsInput = document.getElementById("merge-to-alias-ids");
    const toCanonicalDisplay = document.getElementById("merge-to-canonical-display");
    const toAliasSearch = document.getElementById("merge-to-alias-search");
    const toAliasSuggestions = document.getElementById("merge-to-alias-suggestions");
    const toAliasSelected = document.getElementById("merge-to-alias-selected");
    const toReasonSelect = document.getElementById("merge-to-reason-type");
    const toReasonDetail = document.getElementById("merge-to-reason");

    const fromCanonicalIdInput = document.getElementById("merge-from-canonical-id");
    const fromAliasIdsInput = document.getElementById("merge-from-alias-ids");
    const fromAliasDisplay = document.getElementById("merge-from-alias-display");
    const fromCanonicalSearch = document.getElementById("merge-from-canonical-search");
    const fromCanonicalSuggestions = document.getElementById("merge-from-canonical-suggestions");
    const fromReasonSelect = document.getElementById("merge-from-reason-type");
    const fromReasonDetail = document.getElementById("merge-from-reason");

    const searchEndpoint = formTo.dataset.searchEndpoint || "/cognoms/cerca";

    if (!toCanonicalIdInput || !toAliasIdsInput || !toCanonicalDisplay || !toAliasSearch || !toAliasSuggestions || !toAliasSelected || !fromCanonicalIdInput || !fromAliasIdsInput || !fromAliasDisplay || !fromCanonicalSearch || !fromCanonicalSuggestions) {
        return;
    }

    const selectedAliases = new Map();
    let aliasItems = [];
    let aliasActiveIndex = -1;
    let canonicalItems = [];
    let canonicalActiveIndex = -1;
    let aliasDebounce = null;
    let canonicalDebounce = null;

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
        toAliasIdsInput.value = Array.from(selectedAliases.keys()).join(",");
    }

    function renderAliasTags() {
        toAliasSelected.innerHTML = "";
        selectedAliases.forEach((label, id) => {
            const tag = document.createElement("span");
            tag.className = "cognoms-merge-tag";
            tag.textContent = label;
            const btn = document.createElement("button");
            btn.type = "button";
            btn.className = "cognoms-merge-remove";
            btn.textContent = "x";
            btn.addEventListener("click", () => {
                selectedAliases.delete(id);
                updateAliasHidden();
                renderAliasTags();
            });
            tag.appendChild(btn);
            toAliasSelected.appendChild(tag);
        });
    }

    function fetchSuggestions(query, onDone) {
        const params = new URLSearchParams();
        params.set("q", query);
        fetch(`${searchEndpoint}?${params.toString()}`, { credentials: "same-origin" })
            .then((resp) => resp.json())
            .then((data) => onDone(Array.isArray(data) ? data : []))
            .catch(() => onDone([]));
    }

    function applyAlias(item) {
        if (!item || !item.id) {
            return;
        }
        const id = String(item.id);
        if (toCanonicalIdInput.value === id) {
            toAliasSearch.value = "";
            clearSuggestions(toAliasSuggestions);
            return;
        }
        if (!selectedAliases.has(id)) {
            selectedAliases.set(id, item.forma || id);
            updateAliasHidden();
            renderAliasTags();
        }
        toAliasSearch.value = "";
        clearSuggestions(toAliasSuggestions);
    }

    function applyCanonical(item) {
        if (!item || !item.id) {
            return;
        }
        const id = String(item.id);
        if (fromAliasIdsInput.value === id) {
            fromCanonicalSearch.value = "";
            clearSuggestions(fromCanonicalSuggestions);
            return;
        }
        fromCanonicalIdInput.value = id;
        fromCanonicalSearch.value = item.forma || "";
        clearSuggestions(fromCanonicalSuggestions);
    }

    function resetToFields() {
        toCanonicalIdInput.value = "";
        toAliasIdsInput.value = "";
        selectedAliases.clear();
        renderAliasTags();
        clearSuggestions(toAliasSuggestions);
        toAliasSearch.value = "";
        if (toReasonSelect) {
            toReasonSelect.value = "";
        }
        if (toReasonDetail) {
            toReasonDetail.value = "";
        }
    }

    function resetFromFields() {
        fromCanonicalIdInput.value = "";
        fromAliasIdsInput.value = "";
        fromCanonicalSearch.value = "";
        clearSuggestions(fromCanonicalSuggestions);
        if (fromReasonSelect) {
            fromReasonSelect.value = "";
        }
        if (fromReasonDetail) {
            fromReasonDetail.value = "";
        }
    }

    function openModalTo(id, name) {
        resetToFields();
        toCanonicalDisplay.value = name || "";
        toCanonicalIdInput.value = String(id || "");
        formTo.action = `/cognoms/${id}/merge/to`;
        if (subtitleTo) {
            subtitleTo.textContent = name ? name : "";
        }
        modalTo.classList.add("is-open");
    }

    function openModalFrom(id, name) {
        resetFromFields();
        fromAliasDisplay.value = name || "";
        fromAliasIdsInput.value = String(id || "");
        formFrom.action = `/cognoms/${id}/merge/from`;
        if (subtitleFrom) {
            subtitleFrom.textContent = name ? name : "";
        }
        modalFrom.classList.add("is-open");
    }

    function closeModal(modalEl) {
        if (modalEl) {
            modalEl.classList.remove("is-open");
        }
    }

    document.querySelectorAll("[data-merge-mode]").forEach((btn) => {
        btn.addEventListener("click", () => {
            const mode = btn.dataset.mergeMode || "to";
            const id = btn.dataset.cognomId || "";
            const name = btn.dataset.cognomNom || "";
            if (mode === "from") {
                openModalFrom(id, name);
            } else {
                openModalTo(id, name);
            }
        });
    });

    document.querySelectorAll('[data-modal-close="cognoms-merge-to-modal"]').forEach((btn) => {
        btn.addEventListener("click", () => closeModal(modalTo));
    });
    document.querySelectorAll('[data-modal-close="cognoms-merge-from-modal"]').forEach((btn) => {
        btn.addEventListener("click", () => closeModal(modalFrom));
    });
    modalTo.addEventListener("click", (event) => {
        if (event.target === modalTo) {
            closeModal(modalTo);
        }
    });
    modalFrom.addEventListener("click", (event) => {
        if (event.target === modalFrom) {
            closeModal(modalFrom);
        }
    });

    toAliasSearch.addEventListener("input", () => {
        const value = toAliasSearch.value.trim();
        clearTimeout(aliasDebounce);
        if (value.length < 1) {
            clearSuggestions(toAliasSuggestions);
            return;
        }
        aliasDebounce = setTimeout(() => {
            fetchSuggestions(value, (items) => {
                aliasItems = items;
                aliasActiveIndex = -1;
                renderSuggestions(toAliasSuggestions, aliasItems, aliasActiveIndex);
            });
        }, 250);
    });

    toAliasSearch.addEventListener("keydown", (event) => {
        if (!aliasItems.length) {
            return;
        }
        if (event.key === "ArrowDown") {
            event.preventDefault();
            aliasActiveIndex = Math.min(aliasActiveIndex + 1, aliasItems.length - 1);
            renderSuggestions(toAliasSuggestions, aliasItems, aliasActiveIndex);
        } else if (event.key === "ArrowUp") {
            event.preventDefault();
            aliasActiveIndex = Math.max(aliasActiveIndex - 1, 0);
            renderSuggestions(toAliasSuggestions, aliasItems, aliasActiveIndex);
        } else if (event.key === "Enter") {
            event.preventDefault();
            const item = aliasItems[aliasActiveIndex >= 0 ? aliasActiveIndex : 0];
            applyAlias(item);
        } else if (event.key === "Escape") {
            clearSuggestions(toAliasSuggestions);
        }
    });

    toAliasSuggestions.addEventListener("click", (event) => {
        const target = event.target.closest("li");
        if (!target || target.classList.contains("suggestion-empty")) {
            return;
        }
        const idx = Number(target.dataset.index || -1);
        if (idx >= 0 && aliasItems[idx]) {
            applyAlias(aliasItems[idx]);
        }
    });

    fromCanonicalSearch.addEventListener("input", () => {
        const value = fromCanonicalSearch.value.trim();
        clearTimeout(canonicalDebounce);
        if (value.length < 1) {
            clearSuggestions(fromCanonicalSuggestions);
            return;
        }
        canonicalDebounce = setTimeout(() => {
            fetchSuggestions(value, (items) => {
                canonicalItems = items;
                canonicalActiveIndex = -1;
                renderSuggestions(fromCanonicalSuggestions, canonicalItems, canonicalActiveIndex);
            });
        }, 250);
    });

    fromCanonicalSearch.addEventListener("keydown", (event) => {
        if (!canonicalItems.length) {
            return;
        }
        if (event.key === "ArrowDown") {
            event.preventDefault();
            canonicalActiveIndex = Math.min(canonicalActiveIndex + 1, canonicalItems.length - 1);
            renderSuggestions(fromCanonicalSuggestions, canonicalItems, canonicalActiveIndex);
        } else if (event.key === "ArrowUp") {
            event.preventDefault();
            canonicalActiveIndex = Math.max(canonicalActiveIndex - 1, 0);
            renderSuggestions(fromCanonicalSuggestions, canonicalItems, canonicalActiveIndex);
        } else if (event.key === "Enter") {
            event.preventDefault();
            const item = canonicalItems[canonicalActiveIndex >= 0 ? canonicalActiveIndex : 0];
            applyCanonical(item);
        } else if (event.key === "Escape") {
            clearSuggestions(fromCanonicalSuggestions);
        }
    });

    fromCanonicalSuggestions.addEventListener("click", (event) => {
        const target = event.target.closest("li");
        if (!target || target.classList.contains("suggestion-empty")) {
            return;
        }
        const idx = Number(target.dataset.index || -1);
        if (idx >= 0 && canonicalItems[idx]) {
            applyCanonical(canonicalItems[idx]);
        }
    });

    document.addEventListener("click", (event) => {
        if (!toAliasSuggestions.contains(event.target) && event.target !== toAliasSearch) {
            clearSuggestions(toAliasSuggestions);
        }
        if (!fromCanonicalSuggestions.contains(event.target) && event.target !== fromCanonicalSearch) {
            clearSuggestions(fromCanonicalSuggestions);
        }
    });
});
