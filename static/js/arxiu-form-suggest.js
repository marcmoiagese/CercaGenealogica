document.addEventListener("DOMContentLoaded", () => {
    const inputs = Array.from(document.querySelectorAll("input[data-suggest='1']"));
    if (inputs.length === 0) {
        return;
    }

    function buildContext(item) {
        if (item && item.context) {
            return String(item.context);
        }
        const names = Array.isArray(item.nivells_nom) ? item.nivells_nom : [];
        const types = Array.isArray(item.nivells_tipus) ? item.nivells_tipus : [];
        const country = names[0] || "";
        const chain = [];
        for (let i = 1; i < names.length; i++) {
            const name = names[i];
            if (!name) {
                continue;
            }
            const label = types[i] ? `${types[i]}: ${name}` : name;
            chain.push(label);
        }
        const parts = [];
        if (country) {
            parts.push(country);
        }
        if (chain.length) {
            parts.push(chain.join(" / "));
        }
        return parts.join(" - ");
    }

    function setupSuggest(input) {
        const hiddenId = input.dataset.hidden || "";
        const hiddenTypeId = input.dataset.hiddenType || "";
        const suggestionsId = input.dataset.suggestions || "";
        if (!hiddenId || !suggestionsId) {
            return;
        }
        const hidden = document.getElementById(hiddenId);
        const hiddenType = hiddenTypeId ? document.getElementById(hiddenTypeId) : null;
        const suggestions = document.getElementById(suggestionsId);
        if (!hidden || !suggestions) {
            return;
        }
        const emptyLabel = input.dataset.emptyLabel || "No results";
        let lastItems = [];
        let activeIndex = -1;
        let debounceTimer = null;

        function clearSuggestions() {
            suggestions.innerHTML = "";
            suggestions.classList.remove("is-open");
            lastItems = [];
            activeIndex = -1;
        }

        function setActive(index) {
            const items = Array.from(suggestions.querySelectorAll("li"));
            items.forEach((el, idx) => {
                if (idx === index) {
                    el.classList.add("is-active");
                } else {
                    el.classList.remove("is-active");
                }
            });
            activeIndex = index;
        }

        function renderSuggestions(items) {
            suggestions.innerHTML = "";
            lastItems = items || [];
            activeIndex = -1;
            if (!items || items.length === 0) {
                const li = document.createElement("li");
                li.textContent = emptyLabel;
                li.className = "suggestion-empty";
                suggestions.appendChild(li);
                suggestions.classList.add("is-open");
                return;
            }
            items.forEach((item, idx) => {
                const li = document.createElement("li");
                li.dataset.index = String(idx);
                const title = document.createElement("span");
                title.className = "suggestion-title";
                title.textContent = item.nom || "";
                const contextText = buildContext(item);
                li.appendChild(title);
                if (contextText) {
                    const context = document.createElement("span");
                    context.className = "suggestion-context";
                    context.textContent = contextText;
                    li.appendChild(context);
                }
                li.addEventListener("click", () => applySuggestion(item));
                suggestions.appendChild(li);
            });
            suggestions.classList.add("is-open");
        }

        function applySuggestion(item) {
            if (!item) {
                return;
            }
            hidden.value = item.id ? String(item.id) : "";
            input.value = item.nom || "";
            hidden.dispatchEvent(new Event("change", { bubbles: true }));
            input.dispatchEvent(new Event("change", { bubbles: true }));
            input.dispatchEvent(new CustomEvent("suggest:select", { detail: { item } }));
            if (hiddenType && item.scope_type) {
                hiddenType.value = String(item.scope_type);
            }
            clearSuggestions();
        }

        function fetchSuggestions(query) {
            const apiAttr = input.dataset.api;
            const apiEndpoint = apiAttr !== undefined ? apiAttr : "";
            if (!apiEndpoint) {
                clearSuggestions();
                return;
            }
            const params = new URLSearchParams();
            params.set("q", query);
            params.set("limit", "10");
            const url = new URL(apiEndpoint, window.location.origin);
            params.forEach((value, key) => url.searchParams.set(key, value));
            fetch(url.toString(), { credentials: "same-origin" })
                .then((resp) => resp.json())
                .then((data) => {
                    renderSuggestions(data.items || []);
                })
                .catch(() => {
                    clearSuggestions();
                });
        }

        function handleInput() {
            hidden.value = "";
            if (hiddenType) {
                hiddenType.value = "";
            }
            hidden.dispatchEvent(new Event("change", { bubbles: true }));
            const value = input.value.trim();
            if (value.length < 1) {
                clearSuggestions();
                return;
            }
            clearTimeout(debounceTimer);
            debounceTimer = setTimeout(() => fetchSuggestions(value), 300);
        }

        input.addEventListener("input", handleInput);
        input.addEventListener("keydown", (event) => {
            if (suggestions.children.length === 0) {
                return;
            }
            if (event.key === "ArrowDown") {
                event.preventDefault();
                const next = Math.min(activeIndex + 1, suggestions.children.length - 1);
                setActive(next);
            } else if (event.key === "ArrowUp") {
                event.preventDefault();
                const prev = Math.max(activeIndex - 1, 0);
                setActive(prev);
            } else if (event.key === "Enter") {
                if (lastItems.length > 0) {
                    event.preventDefault();
                    const item = lastItems[activeIndex >= 0 ? activeIndex : 0];
                    applySuggestion(item);
                }
            } else if (event.key === "Escape") {
                clearSuggestions();
            }
        });

        document.addEventListener("click", (event) => {
            if (event.target === input || suggestions.contains(event.target)) {
                return;
            }
            clearSuggestions();
        });
    }

    inputs.forEach(setupSuggest);
});
