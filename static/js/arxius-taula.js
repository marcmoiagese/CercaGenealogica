document.addEventListener("DOMContentLoaded", () => {
    const table = document.getElementById("arxiusTable");
    if (!table) {
        return;
    }
    const filterRow = table.querySelector("#filtraFila");
    const headerRow = table.querySelector("thead tr:last-child");
    const tbody = document.getElementById("taulaDades");
    const inputs = filterRow ? Array.from(filterRow.querySelectorAll("input[data-key]")) : [];
    const filterPanel = document.getElementById("filtersPanel");
    const filterToggle = document.getElementById("toggleFilters");

    let filterOrder = [];
    const storedOrder = (table.dataset.filterOrder || "")
        .split(",")
        .map((val) => val.trim())
        .filter(Boolean);
    if (storedOrder.length) {
        filterOrder = storedOrder;
    }

    function updateFilterOrder(input) {
        const key = input.dataset.key || "";
        if (!key) {
            return;
        }
        const value = input.value.trim();
        const index = filterOrder.indexOf(key);
        if (value && index === -1) {
            filterOrder.push(key);
        }
        if (!value && index !== -1) {
            filterOrder.splice(index, 1);
        }
    }

    let debounceTimer = null;
    const triggerReload = () => {
        const params = new URLSearchParams(window.location.search);
        params.delete("page");
        const perPageSelect = document.getElementById("filesPerPagina");
        if (perPageSelect) {
            params.set("per_page", perPageSelect.value || "25");
        }
        inputs.forEach((input) => {
            updateFilterOrder(input);
            const key = input.dataset.key;
            const value = input.value.trim();
            const paramKey = `f_${key}`;
            if (value) {
                params.set(paramKey, value);
            } else {
                params.delete(paramKey);
            }
        });
        const ordered = filterOrder.filter((key) => params.get(`f_${key}`));
        if (ordered.length) {
            params.set("order", ordered.join(","));
        } else {
            params.delete("order");
        }
        const query = params.toString();
        const url = query ? `${window.location.pathname}?${query}` : window.location.pathname;
        window.location.assign(url);
    };

    inputs.forEach((input) => {
        input.addEventListener("input", () => {
            if (debounceTimer) {
                window.clearTimeout(debounceTimer);
            }
            debounceTimer = window.setTimeout(triggerReload, 350);
        });
    });

    const perPageSelect = document.getElementById("filesPerPagina");
    if (perPageSelect) {
        perPageSelect.addEventListener("change", triggerReload);
    }

    if (filterPanel && filterToggle) {
        const setOpen = (open) => {
            filterPanel.classList.toggle("is-open", open);
            filterToggle.classList.toggle("is-open", open);
            filterToggle.setAttribute("aria-expanded", open ? "true" : "false");
        };
        filterToggle.addEventListener("click", () => {
            const isOpen = filterPanel.classList.contains("is-open");
            setOpen(!isOpen);
        });
    }

    const openBtn = document.getElementById("configColumnes");
    const modal = document.getElementById("modalConfigColumnes");
    const closeBtn = modal ? modal.querySelector(".tanca-modal-config") : null;
    const activeSelect = document.getElementById("cols-actives");
    const hiddenSelect = document.getElementById("cols-hidden");
    const hideBtn = document.getElementById("cols-hide");
    const showBtn = document.getElementById("cols-show");
    const upBtn = document.getElementById("cols-up");
    const downBtn = document.getElementById("cols-down");
    const saveBtn = document.getElementById("cols-save");
    const saveStatus = document.getElementById("cols-save-status");

    if (!headerRow) {
        return;
    }

    const storageKey = `arxius.columns.${window.location.pathname}`;
    const columnLabels = {};
    const columnKeys = Array.from(headerRow.querySelectorAll("th[data-key]")).map((th) => {
        const key = th.dataset.key || "";
        columnLabels[key] = th.textContent.trim() || key;
        return key;
    });
    let columnOrder = columnKeys.slice();
    let hiddenSet = new Set();

    function normalizeOrder(order) {
        const seen = new Set();
        const normalized = [];
        order.forEach((key) => {
            if (columnKeys.includes(key) && !seen.has(key)) {
                normalized.push(key);
                seen.add(key);
            }
        });
        columnKeys.forEach((key) => {
            if (!seen.has(key)) {
                normalized.push(key);
            }
        });
        return normalized;
    }

    function loadConfig() {
        const stored = JSON.parse(localStorage.getItem(storageKey) || "{}");
        let order = columnKeys.slice();
        let hidden = {};
        if (stored && Array.isArray(stored.order)) {
            order = normalizeOrder(stored.order);
            hidden = stored.hidden || {};
        } else if (stored && typeof stored === "object" && Object.keys(stored).length) {
            const legacyHidden = {};
            Object.entries(stored).forEach(([key, visible]) => {
                const idx = parseInt(key, 10);
                if (!Number.isNaN(idx) && columnKeys[idx]) {
                    legacyHidden[columnKeys[idx]] = visible === false;
                }
            });
            hidden = legacyHidden;
        }
        return { order, hidden };
    }

    function saveConfig() {
        const hidden = {};
        hiddenSet.forEach((key) => {
            hidden[key] = true;
        });
        localStorage.setItem(
            storageKey,
            JSON.stringify({
                order: columnOrder,
                hidden,
            })
        );
    }

    function setColumnVisibility(key, visible) {
        table.querySelectorAll(`[data-key="${key}"]`).forEach((el) => {
            el.style.display = visible ? "" : "none";
        });
    }

    function reorderRow(row, order) {
        if (!row) {
            return;
        }
        const cellMap = {};
        Array.from(row.children).forEach((cell) => {
            const key = cell.dataset.key;
            if (key) {
                cellMap[key] = cell;
            }
        });
        order.forEach((key, index) => {
            const cell = cellMap[key];
            if (cell) {
                row.appendChild(cell);
                cell.dataset.col = index;
            }
        });
    }

    function applyColumns() {
        reorderRow(filterRow, columnOrder);
        reorderRow(headerRow, columnOrder);
        if (tbody) {
            Array.from(tbody.querySelectorAll("tr")).forEach((row) => {
                reorderRow(row, columnOrder);
            });
        }
        columnOrder.forEach((key) => {
            setColumnVisibility(key, !hiddenSet.has(key));
        });
    }

    function buildColumnLists() {
        if (!activeSelect || !hiddenSelect) {
            return;
        }
        activeSelect.innerHTML = "";
        hiddenSelect.innerHTML = "";
        columnOrder.forEach((key) => {
            const option = document.createElement("option");
            option.value = key;
            option.textContent = columnLabels[key] || key;
            if (hiddenSet.has(key)) {
                hiddenSelect.appendChild(option);
            } else {
                activeSelect.appendChild(option);
            }
        });
    }

    function updateConfigFromLists() {
        if (!activeSelect || !hiddenSelect) {
            return;
        }
        const activeKeys = Array.from(activeSelect.options).map((opt) => opt.value);
        const hiddenKeys = Array.from(hiddenSelect.options).map((opt) => opt.value);
        columnOrder = activeKeys.concat(hiddenKeys);
        hiddenSet = new Set(hiddenKeys);
        saveConfig();
        applyColumns();
    }

    function moveSelected(from, to) {
        Array.from(from.selectedOptions).forEach((opt) => {
            to.appendChild(opt);
        });
        updateConfigFromLists();
    }

    function moveOption(select, direction) {
        const options = Array.from(select.options);
        if (direction === "up") {
            for (let i = 1; i < options.length; i++) {
                if (options[i].selected && !options[i - 1].selected) {
                    select.insertBefore(options[i], options[i - 1]);
                }
            }
        } else {
            for (let i = options.length - 2; i >= 0; i--) {
                if (options[i].selected && !options[i + 1].selected) {
                    select.insertBefore(options[i + 1], options[i]);
                }
            }
        }
        updateConfigFromLists();
    }

    const storedConfig = loadConfig();
    columnOrder = storedConfig.order;
    hiddenSet = new Set(
        Object.entries(storedConfig.hidden || {})
            .filter(([, hidden]) => hidden)
            .map(([key]) => key)
    );
    applyColumns();

    if (openBtn && modal) {
        openBtn.addEventListener("click", (event) => {
            event.preventDefault();
            modal.style.display = "flex";
            buildColumnLists();
        });
        if (closeBtn) {
            closeBtn.addEventListener("click", () => {
                modal.style.display = "none";
            });
        }
        window.addEventListener("click", (event) => {
            if (event.target === modal) {
                modal.style.display = "none";
            }
        });
    }

    if (hideBtn && activeSelect && hiddenSelect) {
        hideBtn.addEventListener("click", () => {
            moveSelected(activeSelect, hiddenSelect);
        });
    }
    if (showBtn && activeSelect && hiddenSelect) {
        showBtn.addEventListener("click", () => {
            moveSelected(hiddenSelect, activeSelect);
        });
    }
    if (upBtn && activeSelect) {
        upBtn.addEventListener("click", () => {
            moveOption(activeSelect, "up");
        });
    }
    if (downBtn && activeSelect) {
        downBtn.addEventListener("click", () => {
            moveOption(activeSelect, "down");
        });
    }
    if (saveBtn) {
        saveBtn.addEventListener("click", () => {
            saveConfig();
            applyColumns();
            if (saveStatus) {
                saveStatus.textContent = saveBtn.dataset.savedLabel || "Saved";
                window.setTimeout(() => {
                    saveStatus.textContent = "";
                }, 2000);
            }
        });
    }
});
