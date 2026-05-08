(function () {
    document.querySelectorAll("[data-tabs]").forEach(function (container) {
        var buttons = Array.from(container.querySelectorAll(".tab-boto[data-tab]"))
            .filter(function (btn) { return btn.closest("[data-tabs]") === container; });
        var panes = Array.from(container.querySelectorAll(".tab-pane[data-tab-panel]"))
            .filter(function (pane) { return pane.closest("[data-tabs]") === container; });
        if (!buttons.length || !panes.length) {
            return;
        }

        function syncTabs(target) {
            buttons.forEach(function (button) {
                var activeButton = button.dataset.tab === target;
                button.classList.toggle("actiu", activeButton);
                button.setAttribute("aria-selected", activeButton ? "true" : "false");
            });
            panes.forEach(function (pane) {
                var activePane = pane.dataset.tabPanel === target;
                pane.classList.toggle("actiu", activePane);
                pane.hidden = !activePane;
            });
        }

        var activeButton = buttons.find(function (btn) {
            return btn.classList.contains("actiu") || btn.getAttribute("aria-selected") === "true";
        });
        var activePane = panes.find(function (pane) {
            return pane.classList.contains("actiu") || !pane.hidden;
        });
        var initialTarget = activeButton ? activeButton.dataset.tab : "";
        if (!initialTarget && activePane) {
            initialTarget = activePane.dataset.tabPanel || "";
        }
        if (!initialTarget && buttons[0]) {
            initialTarget = buttons[0].dataset.tab || "";
        }
        if (initialTarget) {
            syncTabs(initialTarget);
        }

        buttons.forEach(function (btn) {
            btn.addEventListener("click", function () {
                var target = btn.dataset.tab;
                if (!target) {
                    return;
                }
                syncTabs(target);
            });
        });
    });

    function setupLocalSuggest(input) {
        var hidden = document.getElementById(input.dataset.hidden || "");
        var optionsNode = document.getElementById(input.dataset.options || "");
        var suggestions = document.getElementById(input.dataset.suggestions || "");
        var selected = document.getElementById(input.dataset.selected || "");
        var clearButton = document.getElementById(input.dataset.clear || "");
        if (!hidden || !optionsNode || !suggestions || !selected) {
            return;
        }

        var options = Array.from(optionsNode.querySelectorAll("[data-suggest-option]")).map(function (node) {
            return {
                code: node.dataset.code || "",
                label: node.dataset.label || "",
                context: node.dataset.context || "",
                religionCode: node.dataset.religionCode || ""
            };
        }).filter(function (item) { return item.code && item.label; });
        var emptyText = selected.textContent || "";
        var noResults = input.dataset.emptyLabel || "No results";
        var activeIndex = -1;
        var currentItems = [];
        var instanceID = input.id || hidden.id || "local-suggest";

        function religionFilterValue() {
            var filterID = input.dataset.religionFilterHidden || "";
            var filter = filterID ? document.getElementById(filterID) : null;
            return filter ? filter.value : "";
        }

        function matchingOptions(query) {
            var normalized = String(query || "").trim().toLowerCase();
            var religionCode = religionFilterValue();
            return options.filter(function (item) {
                if (religionCode && item.religionCode && item.religionCode !== religionCode) {
                    return false;
                }
                if (!normalized) {
                    return true;
                }
                return item.label.toLowerCase().indexOf(normalized) !== -1 ||
                    item.code.toLowerCase().indexOf(normalized) !== -1 ||
                    item.context.toLowerCase().indexOf(normalized) !== -1;
            }).slice(0, 12);
        }

        function clearSuggestions() {
            suggestions.innerHTML = "";
            suggestions.classList.remove("is-open");
            input.setAttribute("aria-expanded", "false");
            input.removeAttribute("aria-activedescendant");
            activeIndex = -1;
            currentItems = [];
        }

        function setActive(index) {
            Array.from(suggestions.querySelectorAll("li[data-index]")).forEach(function (node, idx) {
                var active = idx === index;
                node.classList.toggle("is-active", active);
                node.setAttribute("aria-selected", active ? "true" : "false");
                if (active) {
                    input.setAttribute("aria-activedescendant", node.id);
                }
            });
            activeIndex = index;
        }

        function setSelection(item) {
            hidden.value = item ? item.code : "";
            input.value = item ? item.label : "";
            selected.textContent = item ? item.label : emptyText;
            selected.classList.toggle("has-selection", !!item);
            hidden.dispatchEvent(new Event("change", { bubbles: true }));
            clearSuggestions();
        }

        function render(items) {
            suggestions.innerHTML = "";
            currentItems = items;
            activeIndex = -1;
            if (!items.length) {
                var empty = document.createElement("li");
                empty.className = "suggestion-empty";
                empty.setAttribute("aria-disabled", "true");
                empty.textContent = noResults;
                suggestions.appendChild(empty);
                suggestions.classList.add("is-open");
                input.setAttribute("aria-expanded", "true");
                return;
            }
            items.forEach(function (item, index) {
                var li = document.createElement("li");
                li.id = instanceID + "-option-" + index;
                li.dataset.index = String(index);
                li.setAttribute("role", "option");
                li.setAttribute("aria-selected", "false");
                var title = document.createElement("span");
                title.className = "suggestion-title";
                title.textContent = item.label;
                li.appendChild(title);
                if (item.context) {
                    var context = document.createElement("span");
                    context.className = "suggestion-context";
                    context.textContent = item.context;
                    li.appendChild(context);
                }
                li.addEventListener("mousedown", function (event) {
                    event.preventDefault();
                    setSelection(item);
                });
                suggestions.appendChild(li);
            });
            suggestions.classList.add("is-open");
            input.setAttribute("aria-expanded", "true");
        }

        function clearSelection() {
            setSelection(null);
        }

        function openSuggestions() {
            render(matchingOptions(input.value));
        }

        input.addEventListener("focus", function () {
            openSuggestions();
        });
        input.addEventListener("click", function () {
            openSuggestions();
        });
        input.addEventListener("input", function () {
            hidden.value = "";
            selected.textContent = emptyText;
            selected.classList.remove("has-selection");
            hidden.dispatchEvent(new Event("change", { bubbles: true }));
            openSuggestions();
        });
        input.addEventListener("keydown", function (event) {
            if (event.key === "ArrowDown") {
                if (!suggestions.classList.contains("is-open")) {
                    openSuggestions();
                }
                event.preventDefault();
                setActive(Math.min(activeIndex + 1, currentItems.length - 1));
            } else if (event.key === "ArrowUp") {
                if (!suggestions.classList.contains("is-open")) {
                    openSuggestions();
                }
                event.preventDefault();
                setActive(Math.max(activeIndex - 1, 0));
            } else if (event.key === "Enter" && currentItems.length) {
                event.preventDefault();
                setSelection(currentItems[activeIndex >= 0 ? activeIndex : 0]);
            } else if (event.key === "Escape") {
                clearSuggestions();
            }
        });
        hidden.addEventListener("change", function () {
            if (hidden.value) {
                var current = options.find(function (item) { return item.code === hidden.value; });
                if (current) {
                    selected.textContent = current.label;
                    selected.classList.add("has-selection");
                }
            }
        });
        if (clearButton) {
            clearButton.addEventListener("click", clearSelection);
        }
        document.addEventListener("mousedown", function (event) {
            if (event.target === input || suggestions.contains(event.target)) {
                return;
            }
            clearSuggestions();
        });

        var filterID = input.dataset.religionFilterHidden || "";
        var filter = filterID ? document.getElementById(filterID) : null;
        if (filter) {
            filter.addEventListener("change", function () {
                if (!hidden.value) {
                    return;
                }
                var current = options.find(function (item) { return item.code === hidden.value; });
                if (current && current.religionCode && current.religionCode !== filter.value) {
                    clearSelection();
                }
            });
        }

        if (hidden.value) {
            hidden.dispatchEvent(new Event("change", { bubbles: true }));
        }
    }

    document.querySelectorAll("[data-local-suggest='1']").forEach(setupLocalSuggest);

    document.querySelectorAll("[data-confessional-export-form]").forEach(function (form) {
        form.addEventListener("submit", function () {
            form.querySelectorAll("[data-local-suggest='1']").forEach(function (input) {
                var hidden = document.getElementById(input.dataset.hidden || "");
                if (hidden && !hidden.value) {
                    input.value = "";
                }
            });
        });
    });
})();
