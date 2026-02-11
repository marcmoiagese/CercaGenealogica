document.addEventListener("DOMContentLoaded", () => {
    const wrapper = document.querySelector("[data-media-book-search]");
    const pageRoot = document.querySelector("[data-media-page-link]");
    if (!wrapper && !pageRoot) {
        return;
    }

    if (pageRoot) {
        const pageEndpoint = pageRoot.dataset.pageEndpoint || "";
        const llibreId = pageRoot.dataset.llibreId || "";
        const forms = Array.from(pageRoot.querySelectorAll("[data-page-link-form]"));
        if (pageEndpoint && llibreId && forms.length > 0) {
            const closeSuggestions = (suggestions, eventTarget) => {
                if (!suggestions) {
                    return;
                }
                if (eventTarget && (suggestions.contains(eventTarget) || suggestions.previousElementSibling === eventTarget)) {
                    return;
                }
                suggestions.innerHTML = "";
                suggestions.classList.remove("is-open");
            };

            forms.forEach((form) => {
                const input = form.querySelector("[data-page-input]");
                const hidden = form.querySelector("[data-page-id]");
                const numHidden = form.querySelector("[data-page-num]");
                const suggestions = form.querySelector("[data-page-suggestions]");
                const submit = form.querySelector("button[type='submit']");
                if (!input || !hidden || !numHidden || !suggestions) {
                    return;
                }

                let items = [];
                let activeIndex = -1;
                let timer = null;

                const setSubmitState = () => {
                    if (submit) {
                        submit.disabled = hidden.value.trim() === "" && numHidden.value.trim() === "";
                    }
                };

                const clearSuggestions = () => {
                    suggestions.innerHTML = "";
                    suggestions.classList.remove("is-open");
                    items = [];
                    activeIndex = -1;
                };

                const setActive = (index) => {
                    const listItems = Array.from(suggestions.querySelectorAll("li"));
                    listItems.forEach((el, idx) => {
                        if (idx === index) {
                            el.classList.add("is-active");
                        } else {
                            el.classList.remove("is-active");
                        }
                    });
                    activeIndex = index;
                };

                const applySuggestion = (item) => {
                    if (!item) {
                        return;
                    }
                    hidden.value = item.id ? String(item.id) : "";
                    numHidden.value = item.num ? String(item.num) : "";
                    input.value = item.label || item.num || "";
                    clearSuggestions();
                    setSubmitState();
                };

                const renderSuggestions = (data) => {
                    suggestions.innerHTML = "";
                    items = Array.isArray(data) ? data : [];
                    activeIndex = -1;
                    if (items.length === 0) {
                        const li = document.createElement("li");
                        li.textContent = suggestions.dataset.emptyLabel || "No results";
                        li.className = "suggestion-empty";
                        suggestions.appendChild(li);
                        suggestions.classList.add("is-open");
                        return;
                    }
                    items.forEach((item, idx) => {
                        const li = document.createElement("li");
                        li.dataset.index = String(idx);
                        li.textContent = item.label || item.num || "";
                        li.addEventListener("click", () => applySuggestion(item));
                        suggestions.appendChild(li);
                    });
                    suggestions.classList.add("is-open");
                };

                const fetchSuggestions = (value) => {
                    if (!pageEndpoint) {
                        clearSuggestions();
                        return;
                    }
                    const url = new URL(pageEndpoint, window.location.origin);
                    url.searchParams.set("llibre_id", llibreId);
                    url.searchParams.set("limit", "10");
                    if (value) {
                        url.searchParams.set("q", value);
                    }
                    fetch(url.toString(), { credentials: "same-origin" })
                        .then((resp) => resp.json())
                        .then((data) => {
                            renderSuggestions(data.items || []);
                        })
                        .catch(() => {
                            clearSuggestions();
                        });
                };

                const handleInput = () => {
                    hidden.value = "";
                    numHidden.value = "";
                    const value = input.value.trim();
                    if (/^\d+$/.test(value)) {
                        numHidden.value = value;
                    }
                    setSubmitState();
                    if (value.length < 1) {
                        clearSuggestions();
                        return;
                    }
                    clearTimeout(timer);
                    timer = setTimeout(() => fetchSuggestions(value), 250);
                };

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
                        if (items.length > 0) {
                            event.preventDefault();
                            const item = items[activeIndex >= 0 ? activeIndex : 0];
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
                    closeSuggestions(suggestions, event.target);
                });

                form.addEventListener("submit", (event) => {
                    if (!hidden.value && !numHidden.value) {
                        event.preventDefault();
                    }
                });

                setSubmitState();
            });
        }
    }

    if (wrapper) {
        const endpoint = wrapper.dataset.bookEndpoint || "";
        const queryInput = wrapper.querySelector("[data-book-input]");
        const cronInput = wrapper.querySelector("[data-book-cronologia]");
        const resultsList = wrapper.querySelector("[data-book-results]");
        const resultsEmpty = wrapper.querySelector("[data-book-empty]");
        const resultsCount = wrapper.querySelector("[data-book-results-count]");
        const selectedWrap = wrapper.querySelector("[data-book-selected]");
        const selectedLabel = wrapper.querySelector("[data-book-selected-label]");
        const hiddenId = document.getElementById("media-book-id");
        const hiddenLabel = document.getElementById("media-book-label");
        const clearBtn = wrapper.querySelector("[data-book-clear]");

        if (queryInput && cronInput && resultsList && resultsEmpty && selectedLabel && hiddenId && hiddenLabel) {
            const filterConfigs = [
                { key: "arxiu", api: "/api/documentals/arxius/suggest" },
                { key: "ecles", api: "/api/territori/eclesiastic/suggest" },
                { key: "municipi", api: "/api/territori/municipis/suggest" }
            ];

            const filters = {};

            function buildContext(item) {
                if (!item) {
                    return "";
                }
                if (item.context) {
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

            function setSelected(item) {
                if (!item) {
                    return;
                }
                hiddenId.value = item.id ? String(item.id) : "";
                hiddenLabel.value = item.nom || "";
                selectedLabel.textContent = item.nom || selectedWrap.dataset.emptyLabel || "";
            }

            function clearSelected() {
                hiddenId.value = "";
                hiddenLabel.value = "";
                selectedLabel.textContent = selectedWrap.dataset.emptyLabel || "";
            }

            function hasActiveFilters() {
                if (queryInput.value.trim() !== "") {
                    return true;
                }
                if (cronInput.value.trim() !== "") {
                    return true;
                }
                return Object.values(filters).some((filter) => filter.hidden && filter.hidden.value);
            }

            let searchTimer = null;

            function clearResults() {
                resultsList.innerHTML = "";
                if (resultsCount) {
                    resultsCount.textContent = "";
                }
                resultsEmpty.style.display = "block";
            }

            function renderResults(items) {
                resultsList.innerHTML = "";
                if (!Array.isArray(items) || items.length === 0) {
                    clearResults();
                    return;
                }
                resultsEmpty.style.display = "none";
                if (resultsCount) {
                    resultsCount.textContent = `(${items.length})`;
                }
                items.forEach((item) => {
                    const li = document.createElement("li");
                    li.className = "media-book-result";
                    const title = document.createElement("span");
                    title.className = "suggestion-title";
                    title.textContent = item.nom || "";
                    li.appendChild(title);
                    const contextText = buildContext(item);
                    if (contextText) {
                        const context = document.createElement("span");
                        context.className = "suggestion-context";
                        context.textContent = contextText;
                        li.appendChild(context);
                    }
                    li.addEventListener("click", () => {
                        setSelected(item);
                    });
                    resultsList.appendChild(li);
                });
            }

            function runSearch() {
                if (!endpoint) {
                    return;
                }
                if (!hasActiveFilters()) {
                    clearResults();
                    return;
                }
                const params = new URLSearchParams();
                const query = queryInput.value.trim();
                const cronologia = cronInput.value.trim();
                if (query) {
                    params.set("q", query);
                }
                if (cronologia) {
                    params.set("cronologia", cronologia);
                }
                Object.values(filters).forEach((filter) => {
                    if (!filter.hidden || !filter.hidden.value) {
                        return;
                    }
                    params.set(`${filter.key}_id`, filter.hidden.value);
                });
                params.set("limit", "20");
                const url = new URL(endpoint, window.location.origin);
                params.forEach((value, key) => url.searchParams.set(key, value));
                fetch(url.toString(), { credentials: "same-origin" })
                    .then((resp) => resp.json())
                    .then((data) => {
                        renderResults(data.items || []);
                    })
                    .catch(() => {
                        clearResults();
                    });
            }

            function scheduleSearch() {
                clearTimeout(searchTimer);
                searchTimer = setTimeout(runSearch, 300);
            }

            function setupFilter(filter) {
                const input = wrapper.querySelector(`[data-book-filter="${filter.key}"]`);
                const hidden = wrapper.querySelector(`[data-book-filter-id="${filter.key}"]`);
                const suggestions = document.getElementById(`media-book-${filter.key}-suggestions`);
                if (!input || !hidden || !suggestions) {
                    return;
                }
                filter.input = input;
                filter.hidden = hidden;
                filter.suggestions = suggestions;
                filter.items = [];
                filter.activeIndex = -1;
                filters[filter.key] = filter;

                let timer = null;

                function clearSuggestions() {
                    suggestions.innerHTML = "";
                    suggestions.classList.remove("is-open");
                    filter.items = [];
                    filter.activeIndex = -1;
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
                    filter.activeIndex = index;
                }

                function applySuggestion(item) {
                    if (!item) {
                        return;
                    }
                    hidden.value = item.id ? String(item.id) : "";
                    input.value = item.nom || "";
                    clearSuggestions();
                    scheduleSearch();
                }

                function renderSuggestions(items) {
                    suggestions.innerHTML = "";
                    filter.items = items || [];
                    filter.activeIndex = -1;
                    if (!items || items.length === 0) {
                        const li = document.createElement("li");
                        li.textContent = suggestions.dataset.emptyLabel || "No results";
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

                function fetchSuggestions(value) {
                    if (!filter.api) {
                        clearSuggestions();
                        return;
                    }
                    const url = new URL(filter.api, window.location.origin);
                    url.searchParams.set("q", value);
                    url.searchParams.set("limit", "10");
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
                    scheduleSearch();
                    const value = input.value.trim();
                    if (value.length < 1) {
                        clearSuggestions();
                        return;
                    }
                    clearTimeout(timer);
                    timer = setTimeout(() => fetchSuggestions(value), 300);
                }

                input.addEventListener("input", handleInput);
                input.addEventListener("keydown", (event) => {
                    if (suggestions.children.length === 0) {
                        return;
                    }
                    if (event.key === "ArrowDown") {
                        event.preventDefault();
                        const next = Math.min(filter.activeIndex + 1, suggestions.children.length - 1);
                        setActive(next);
                    } else if (event.key === "ArrowUp") {
                        event.preventDefault();
                        const prev = Math.max(filter.activeIndex - 1, 0);
                        setActive(prev);
                    } else if (event.key === "Enter") {
                        if (filter.items.length > 0) {
                            event.preventDefault();
                            const item = filter.items[filter.activeIndex >= 0 ? filter.activeIndex : 0];
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

            filterConfigs.forEach(setupFilter);

            queryInput.addEventListener("input", scheduleSearch);
            cronInput.addEventListener("input", scheduleSearch);

            if (clearBtn) {
                clearBtn.addEventListener("click", () => {
                    clearSelected();
                });
            }

            if (hiddenId.value) {
                selectedLabel.textContent = hiddenLabel.value || selectedWrap.dataset.emptyLabel || "";
            }

            clearResults();
        }
    }
});
