document.addEventListener("DOMContentLoaded", () => {
    function initScopeSuggest(wrapper) {
        if (!wrapper) return;
        const typeSelect = wrapper.querySelector("[data-scope-type]");
        const input = wrapper.querySelector("[data-scope-query]");
        const hiddenId = wrapper.querySelector("[data-scope-id]");
        const hiddenLabel = wrapper.querySelector("[data-scope-label]");
        const suggestions = wrapper.querySelector("[data-scope-suggestions]");
        if (!typeSelect || !input || !hiddenId || !suggestions) {
            return;
        }
        const api = wrapper.dataset.api || "/api/scopes/search";
        const emptyLabel = wrapper.dataset.emptyLabel || "No results";
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

        function applySuggestion(item) {
            if (!item) return;
            hiddenId.value = item.id ? String(item.id) : "";
            if (hiddenLabel) {
                hiddenLabel.value = item.label || "";
            }
            input.value = item.label || "";
            clearSuggestions();
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
                title.textContent = item.label || "";
                li.appendChild(title);
                if (item.context) {
                    const context = document.createElement("span");
                    context.className = "suggestion-context";
                    context.textContent = item.context;
                    li.appendChild(context);
                }
                li.addEventListener("click", () => applySuggestion(item));
                suggestions.appendChild(li);
            });
            suggestions.classList.add("is-open");
        }

        function fetchSuggestions(query) {
            const typeValue = typeSelect.value || "";
            if (!typeValue) {
                clearSuggestions();
                return;
            }
            const params = new URLSearchParams();
            params.set("q", query);
            params.set("type", typeValue);
            params.set("limit", "10");
            fetch(`${api}?${params.toString()}`, { credentials: "same-origin" })
                .then((resp) => resp.json())
                .then((data) => renderSuggestions(data.items || []))
                .catch(() => clearSuggestions());
        }

        function handleInput() {
            hiddenId.value = "";
            if (hiddenLabel) {
                hiddenLabel.value = "";
            }
            const value = input.value.trim();
            if (value.length < 1) {
                clearSuggestions();
                return;
            }
            clearTimeout(debounceTimer);
            debounceTimer = setTimeout(() => fetchSuggestions(value), 300);
        }

        input.addEventListener("input", handleInput);
        typeSelect.addEventListener("change", () => {
            hiddenId.value = "";
            if (hiddenLabel) {
                hiddenLabel.value = "";
            }
            input.value = "";
            clearSuggestions();
        });
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

    const wrappers = document.querySelectorAll("[data-scope-suggest]");
    wrappers.forEach(initScopeSuggest);

    const impacts = document.getElementById("eventImpacts");
    const addBtn = document.getElementById("impactAddBtn");
    const template = document.getElementById("impactRowTemplate");
    if (impacts && addBtn && template) {
        addBtn.addEventListener("click", () => {
            const node = template.content.firstElementChild.cloneNode(true);
            impacts.appendChild(node);
            const wrapper = node.querySelector("[data-scope-suggest]");
            initScopeSuggest(wrapper);
        });
        impacts.addEventListener("click", (event) => {
            const btn = event.target.closest("[data-impact-remove]");
            if (!btn) return;
            const row = btn.closest("[data-impact-row]");
            if (!row) return;
            const rows = impacts.querySelectorAll("[data-impact-row]");
            if (rows.length <= 1) {
                row.querySelectorAll("input, textarea, select").forEach((el) => {
                    if (el.tagName === "SELECT") {
                        el.selectedIndex = 0;
                    } else {
                        el.value = "";
                    }
                });
                return;
            }
            row.remove();
        });
    }
});
