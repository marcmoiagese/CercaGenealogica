document.addEventListener("DOMContentLoaded", () => {
    const table = document.getElementById("registresTable");
    const qualityModal = document.getElementById("modalQualitat");
    if (qualityModal) {
        const closeQualityBtn = qualityModal.querySelector(".tanca-modal-qualitat");
        document.addEventListener("click", (event) => {
            if (event.target.closest(".qualitat-icon")) {
                qualityModal.style.display = "flex";
            }
        });
        if (closeQualityBtn) {
            closeQualityBtn.addEventListener("click", () => {
                qualityModal.style.display = "none";
            });
        }
        window.addEventListener("click", (event) => {
            if (event.target === qualityModal) {
                qualityModal.style.display = "none";
            }
        });
    }

    const markModal = document.getElementById("modalMarcar");
    if (markModal) {
        const closeMarkBtn = markModal.querySelector(".tanca-modal-marcar");
        const markType = markModal.querySelector("#mark-type");
        const markPublic = markModal.querySelector("#mark-public");
        const markSave = markModal.querySelector("#mark-save");
        const markClear = markModal.querySelector("#mark-clear");
        const csrfToken = markModal.dataset.csrf || "";
        let currentMarkRow = null;
        let currentMarkID = 0;

        function clearRowMarks(row) {
            row.classList.remove("mark-consanguini", "mark-politic", "mark-interes");
        }

        function applyMark(row, markTypeValue) {
            clearRowMarks(row);
            if (!markTypeValue) {
                return;
            }
            row.classList.add(`mark-${markTypeValue}`);
        }

        function updateRowMark(row, mark) {
            row.dataset.markType = mark.type || "";
            row.dataset.markPublic = mark.is_public ? "1" : "0";
            row.dataset.markOwn = mark.own ? "1" : "0";
            applyMark(row, mark.type);
        }

        function openMarkModal(button) {
            currentMarkID = parseInt(button.dataset.registreId || "0", 10);
            currentMarkRow = button.closest("tr");
            if (!currentMarkRow) {
                currentMarkRow = document.querySelector("[data-mark-row='1']");
            }
            if (!currentMarkRow) {
                return;
            }
            const existingType = currentMarkRow.dataset.markType || "";
            const existingPublic = currentMarkRow.dataset.markPublic !== "0";
            if (markType) {
                markType.value = existingType;
            }
            if (markPublic) {
                markPublic.checked = existingType ? existingPublic : true;
            }
            if (markClear) {
                markClear.disabled = currentMarkRow.dataset.markOwn !== "1";
            }
            markModal.style.display = "flex";
        }

        function closeMarkModal() {
            markModal.style.display = "none";
        }

        document.addEventListener("click", (event) => {
            const btn = event.target.closest(".btn-marcar");
            if (!btn) {
                return;
            }
            openMarkModal(btn);
        });

        if (markSave) {
            markSave.addEventListener("click", () => {
                if (!currentMarkID || !currentMarkRow) {
                    return;
                }
                const typeValue = markType ? markType.value : "";
                if (!typeValue) {
                    return;
                }
                const body = new URLSearchParams();
                body.set("csrf_token", csrfToken);
                body.set("type", typeValue);
                body.set("public", markPublic && markPublic.checked ? "1" : "0");
                fetch(`/documentals/registres/${currentMarkID}/marcar`, {
                    method: "POST",
                    headers: { "Content-Type": "application/x-www-form-urlencoded" },
                    body: body.toString(),
                    credentials: "same-origin",
                })
                    .then((response) => {
                        if (!response.ok) {
                            throw new Error("mark_failed");
                        }
                        return response.json();
                    })
                    .then((data) => {
                        if (data && data.ok) {
                            updateRowMark(currentMarkRow, {
                                type: data.type || "",
                                is_public: data.is_public,
                                own: true,
                            });
                            closeMarkModal();
                        }
                    })
                    .catch(() => {});
            });
        }

        if (markClear) {
            markClear.addEventListener("click", () => {
                if (!currentMarkID || !currentMarkRow) {
                    return;
                }
                if (currentMarkRow.dataset.markOwn !== "1") {
                    closeMarkModal();
                    return;
                }
                const body = new URLSearchParams();
                body.set("csrf_token", csrfToken);
                fetch(`/documentals/registres/${currentMarkID}/desmarcar`, {
                    method: "POST",
                    headers: { "Content-Type": "application/x-www-form-urlencoded" },
                    body: body.toString(),
                    credentials: "same-origin",
                })
                    .then((response) => {
                        if (!response.ok) {
                            throw new Error("mark_clear_failed");
                        }
                        return response.json();
                    })
                    .then((data) => {
                        if (data && data.ok) {
                            updateRowMark(currentMarkRow, {
                                type: data.type || "",
                                is_public: data.is_public,
                                own: false,
                            });
                            closeMarkModal();
                        }
                    })
                    .catch(() => {});
            });
        }

        if (closeMarkBtn) {
            closeMarkBtn.addEventListener("click", closeMarkModal);
        }
        window.addEventListener("click", (event) => {
            if (event.target === markModal) {
                closeMarkModal();
            }
        });
    }
    if (!table) {
        return;
    }
    const filterRow = table.querySelector("#filtraFila");
    const headerRow = table.querySelector("thead tr:last-child");
    const tbody = document.getElementById("taulaDades");
    const paginador = document.getElementById("paginador");
    const inputs = filterRow ? Array.from(filterRow.querySelectorAll("input[data-key]")) : [];
    const allRows = tbody ? Array.from(tbody.querySelectorAll("tr")) : [];
    const serverMode = table.dataset.serverPagination === "1";
    const inlineBase = table.dataset.inlineUrl || "";
    const inlineCsrf = table.dataset.csrf || "";
    const selectLabel = table.dataset.selectLabel || "Selecciona";
    const editLabel = table.dataset.editLabel || "Editar";
    const confirmLabel = table.dataset.confirmLabel || "Guardar";
    const cancelLabel = table.dataset.cancelLabel || "Cancel·lar";
    const columnsMetaEl = document.getElementById("records-columns-meta");
    const statusLabelsEl = document.getElementById("records-status-labels");
    let columnsMeta = {};
    let statusLabels = {};
    try {
        columnsMeta = columnsMetaEl ? JSON.parse(columnsMetaEl.textContent) : {};
    } catch (err) {
        columnsMeta = {};
    }
    try {
        statusLabels = statusLabelsEl ? JSON.parse(statusLabelsEl.textContent) : {};
    } catch (err) {
        statusLabels = {};
    }

    let rowsPerPage = 25;
    let currentPage = 1;
    let currentRows = allRows.slice();
    let filterOrder = [];
    let applyFilters = null;

    const perPageSelect = document.getElementById("filesPerPagina");
    if (perPageSelect) {
        rowsPerPage = parseInt(perPageSelect.value || "25", 10);
        if (!serverMode) {
            perPageSelect.addEventListener("change", () => {
                rowsPerPage = parseInt(perPageSelect.value || "25", 10);
                currentPage = 1;
                paginate(currentRows);
            });
        }
    }

    function normalize(val) {
        return (val || "").toString().toLowerCase();
    }

    function qualityIconMarkup(quality) {
        if (!quality || quality === "clar") {
            return "";
        }
        if (quality === "dubtos") {
            return '<i class="fas fa-question-circle"></i>';
        }
        if (quality === "incomplet") {
            return '<i class="fas fa-file-excel"></i>';
        }
        if (quality === "illegible") {
            return '<i class="fas fa-eye-slash"></i>';
        }
        if (quality === "no_consta") {
            return '<i class="fas fa-minus-circle"></i>';
        }
        return "";
    }

    function renderEditableCell(cell, displayValue) {
        const quality = cell.dataset.quality || "";
        const icon = qualityIconMarkup(quality);
        const qualityButton = icon
            ? `<button type="button" class="qualitat-icon" data-quality="${quality}">${icon}</button>`
            : "";
        const safeText = displayValue || "";
        cell.innerHTML = `
            <div class="editable-cell">
                <span class="cell-text">${safeText}</span>
                ${qualityButton}
                <span class="edit-icons">
                    <button type="button" class="edit-icon" aria-label="${editLabel}">
                        <i class="fa fa-pencil"></i>
                    </button>
                </span>
            </div>
        `;
    }

    function buildInput(meta, value) {
        const inputType = meta && meta.input ? meta.input : "text";
        if (inputType === "select") {
            const select = document.createElement("select");
            const emptyOpt = document.createElement("option");
            emptyOpt.value = "";
            emptyOpt.textContent = selectLabel;
            select.appendChild(emptyOpt);
            if (meta && Array.isArray(meta.options)) {
                meta.options.forEach((opt) => {
                    const option = document.createElement("option");
                    option.value = opt.value;
                    option.textContent = opt.label;
                    if (opt.value === value) {
                        option.selected = true;
                    }
                    select.appendChild(option);
                });
            }
            return select;
        }
        const input = document.createElement("input");
        input.type = inputType === "number" ? "number" : "text";
        input.value = value || "";
        return input;
    }

    async function submitInlineEdit(cell, meta, value) {
        const row = cell.closest("tr");
        const registreId = row ? row.dataset.registreId : "";
        if (!registreId || !inlineBase) {
            return { ok: false };
        }
        const resp = await fetch(`${inlineBase}/${registreId}/inline`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "X-CSRF-Token": inlineCsrf,
            },
            credentials: "same-origin",
            body: JSON.stringify({
                key: cell.dataset.key,
                value,
            }),
        });
        if (!resp.ok) {
            return { ok: false };
        }
        return resp.json();
    }

    function updateStatusCell(row, status) {
        if (!row || !status) {
            return;
        }
        const statusCell = row.querySelector('td[data-key="_status"] .badge');
        if (statusCell) {
            statusCell.textContent = statusLabels[status] || status;
        }
    }

    function updateFilterOrder(input) {
        const col = input.dataset.key;
        const value = input.value.trim();
        const index = filterOrder.indexOf(col);
        if (value && index === -1) {
            filterOrder.push(col);
        }
        if (!value && index !== -1) {
            filterOrder.splice(index, 1);
        }
    }

    if (serverMode) {
        const storedOrder = (table.dataset.filterOrder || "").split(",").map((v) => v.trim()).filter(Boolean);
        if (storedOrder.length) {
            filterOrder = storedOrder;
        }
        let debounceTimer = null;
        const triggerReload = () => {
            const params = new URLSearchParams(window.location.search);
            params.delete("page");
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
            const url = `${window.location.pathname}?${params.toString()}`;
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
        if (perPageSelect) {
            perPageSelect.addEventListener("change", triggerReload);
        }
    } else {
        applyFilters = function applyFilters() {
            const filters = {};
            inputs.forEach((input) => {
                updateFilterOrder(input);
                const value = normalize(input.value.trim());
                if (value) {
                    filters[input.dataset.key] = value;
                }
            });
            currentRows = allRows.filter((row) => {
                for (const col of filterOrder) {
                    const cell = row.querySelector(`td[data-key="${col}"]`);
                    const text = normalize(cell ? cell.textContent : "");
                    if (!text.includes(filters[col])) {
                        return false;
                    }
                }
                return true;
            });
            currentPage = 1;
            paginate(currentRows);
        };

        function paginate(rows) {
            const totalPages = Math.max(1, Math.ceil(rows.length / rowsPerPage));
            if (currentPage > totalPages) {
                currentPage = totalPages;
            }
            const start = (currentPage - 1) * rowsPerPage;
            const end = start + rowsPerPage;
            allRows.forEach((row) => {
                row.style.display = "none";
            });
            rows.slice(start, end).forEach((row) => {
                row.style.display = "";
            });
            renderPagination(totalPages);
        }

        function renderPagination(totalPages) {
            if (!paginador) {
                return;
            }
            paginador.innerHTML = "";
            for (let i = 1; i <= totalPages; i++) {
                const btn = document.createElement("button");
                btn.type = "button";
                btn.textContent = i;
                if (i === currentPage) {
                    btn.classList.add("actiu");
                }
                btn.addEventListener("click", () => {
                    currentPage = i;
                    paginate(currentRows);
                });
                paginador.appendChild(btn);
            }
        }

        inputs.forEach((input) => {
            input.addEventListener("input", applyFilters);
        });
    }

    // Inline editing
    table.addEventListener("click", (event) => {
        const editButton = event.target.closest(".edit-icon");
        if (!editButton) {
            return;
        }
        const cell = editButton.closest("td[data-key]");
        if (!cell) {
            return;
        }
        const key = cell.dataset.key;
        const meta = columnsMeta[key];
        if (!meta) {
            return;
        }
        if (cell.dataset.editing === "1") {
            return;
        }
        cell.dataset.editing = "1";
        const originalDisplay = cell.querySelector(".cell-text") ? cell.querySelector(".cell-text").textContent.trim() : "";
        const originalValue = cell.dataset.value || originalDisplay;
        const input = buildInput(meta, originalValue);
        input.classList.add("edit-input");
        const actions = document.createElement("span");
        actions.className = "edit-icons";
        actions.innerHTML = `
            <button type="button" class="confirm-icon" aria-label="${confirmLabel}">
                <i class="fa fa-check"></i>
            </button>
            <button type="button" class="cancel-icon" aria-label="${cancelLabel}">
                <i class="fa fa-times"></i>
            </button>
        `;
        const wrapper = document.createElement("div");
        wrapper.className = "editable-cell is-editing";
        wrapper.appendChild(input);
        wrapper.appendChild(actions);
        cell.innerHTML = "";
        cell.appendChild(wrapper);

        const confirmBtn = actions.querySelector(".confirm-icon");
        const cancelBtn = actions.querySelector(".cancel-icon");
        cancelBtn.addEventListener("click", () => {
            cell.dataset.editing = "0";
            renderEditableCell(cell, originalDisplay);
        });
        confirmBtn.addEventListener("click", async () => {
            const newValue = input.value.trim();
            const result = await submitInlineEdit(cell, meta, newValue);
            if (!result || !result.ok) {
                cell.dataset.editing = "0";
                renderEditableCell(cell, originalDisplay);
                return;
            }
            cell.dataset.value = result.raw || newValue;
            cell.dataset.editing = "0";
            renderEditableCell(cell, result.value || newValue);
            updateStatusCell(cell.closest("tr"), result.status);
        });
    });

    // Column visibility modal
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
    const storageKey = `records.columns.${window.location.pathname}`;

    const columnLabels = {};
    const columnKeys = headerRow
        ? Array.from(headerRow.querySelectorAll("th[data-key]")).map((th) => {
              const key = th.dataset.key;
              columnLabels[key] = th.textContent.trim() || key;
              return key;
          })
        : [];
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
        openBtn.addEventListener("click", (e) => {
            e.preventDefault();
            modal.style.display = "flex";
            buildColumnLists();
        });
        if (closeBtn) {
            closeBtn.addEventListener("click", () => {
                modal.style.display = "none";
            });
        }
        window.addEventListener("click", (e) => {
            if (e.target === modal) {
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

    // Initialize
    if (applyFilters) {
        applyFilters();
    }

    // Link person modal
    const linkModal = document.getElementById("modalInterconnectar");
    if (linkModal) {
        const closeModalBtn = linkModal.querySelector(".tanca-modal-interconnexio");
        const typeSelect = document.getElementById("tipusConnexio");
        const formContainer = document.getElementById("formulariConnexio");
        const nameLabel = document.getElementById("link-person-name");
        const searchEndpoint = linkModal.dataset.searchEndpoint || "";
        const csrfToken = linkModal.dataset.csrf || "";
        const returnTo = linkModal.dataset.returnTo || window.location.pathname;
        const labelLink = linkModal.dataset.labelLink || "Link";
        const labelEmpty = linkModal.dataset.labelEmpty || "No results";
        const labelSearch = linkModal.dataset.labelSearch || "Search";
        const labelSearchPlaceholder = linkModal.dataset.labelSearchPlaceholder || "";
        const labelSelectRole = linkModal.dataset.labelSelectRole || "";
        const labelRawPerson = linkModal.dataset.labelRawPerson || "";
        const labelSelectPerson = linkModal.dataset.labelSelectPerson || "";

        let currentRegistreID = 0;
        let currentRawID = 0;
        let currentName = "";
        let currentMunicipi = "";
        let currentAny = "";
        let debounceTimer = null;
        let rawPeople = [];
        let primaryRole = "";
        let selectedPersonaID = 0;
        let selectedResult = null;

        function openModal(button) {
            currentRegistreID = parseInt(button.dataset.registreId || "0", 10);
            currentRawID = 0;
            currentName = button.dataset.rawName || "";
            currentMunicipi = button.dataset.rawMunicipi || "";
            currentAny = button.dataset.rawAny || "";
            primaryRole = button.dataset.primaryRole || "";
            rawPeople = [];
            selectedPersonaID = 0;
            selectedResult = null;
            if (nameLabel) {
                nameLabel.textContent = currentName;
            }
            if (typeSelect) {
                typeSelect.value = "";
            }
            if (formContainer) {
                formContainer.innerHTML = labelSelectRole ? `<p class="muted">${labelSelectRole}</p>` : "";
            }
            linkModal.style.display = "flex";
            if (currentRegistreID) {
                fetch(`/documentals/registres/${currentRegistreID}/persones`, {
                    headers: { "Accept": "application/json" },
                    credentials: "same-origin",
                })
                    .then((res) => res.json())
                    .then((payload) => {
                        rawPeople = payload.people || [];
                        if (payload.primary_role) {
                            primaryRole = payload.primary_role;
                        }
                        if (typeSelect && typeSelect.value) {
                            buildSearchForm(typeSelect.value);
                        }
                    })
                    .catch(() => {
                        rawPeople = [];
                    });
            }
        }

        function closeModal() {
            linkModal.style.display = "none";
        }

        function buildSearchForm(role) {
            if (!formContainer) {
                return;
            }
            selectedPersonaID = 0;
            selectedResult = null;
            const matchingRole = role === "persona_principal" ? primaryRole : role;
            if (!matchingRole) {
                formContainer.innerHTML = labelSelectRole ? `<p class="muted">${labelSelectRole}</p>` : "";
                return;
            }
            const candidates = rawPeople.filter((p) => p.Rol === matchingRole);
            if (candidates.length === 0) {
                formContainer.innerHTML = labelEmpty ? `<p class="muted">${labelEmpty}</p>` : "";
                return;
            }
            currentRawID = candidates[0].ID;
            const rawSelect =
                candidates.length > 1
                    ? `
                <div class="form-group">
                    <label for="raw-person-select">${labelRawPerson}</label>
                    <select id="raw-person-select">
                        ${candidates
                            .map((p) => {
                                const label = p.DisplayName || p.Nom || "-";
                                return `<option value="${p.ID}">${label}</option>`;
                            })
                            .join("")}
                    </select>
                </div>
            `
                    : "";
            formContainer.innerHTML = `
                ${rawSelect}
                <div class="form-group">
                    <label for="link-search-input">${labelSearch}</label>
                    <input type="text" id="link-search-input" class="input-text" placeholder="${labelSearchPlaceholder}">
                </div>
                <div class="link-results" id="link-results"></div>
                <button type="button" class="boto-primari" id="link-submit" disabled>${labelLink}</button>
            `;
            const input = formContainer.querySelector("#link-search-input");
            const resultsContainer = formContainer.querySelector("#link-results");
            const submitButton = formContainer.querySelector("#link-submit");
            const rawSelectEl = formContainer.querySelector("#raw-person-select");
            if (!input || !resultsContainer) {
                return;
            }
            if (rawSelectEl) {
                rawSelectEl.addEventListener("change", () => {
                    currentRawID = parseInt(rawSelectEl.value || "0", 10);
                });
            }
            if (submitButton) {
                submitButton.addEventListener("click", () => {
                    if (!selectedPersonaID || !currentRawID) {
                        return;
                    }
                    linkPersona(selectedPersonaID);
                });
            }
            input.value = currentName;
            input.addEventListener("input", () => {
                if (debounceTimer) {
                    window.clearTimeout(debounceTimer);
                }
                debounceTimer = window.setTimeout(() => {
                    searchPeople(input.value.trim(), resultsContainer);
                }, 300);
            });
            searchPeople(input.value.trim(), resultsContainer);
        }

        function renderResults(results, container) {
            container.innerHTML = "";
            if (!results || results.length === 0) {
                const empty = document.createElement("p");
                empty.className = "muted";
                empty.textContent = labelEmpty;
                container.appendChild(empty);
                return;
            }
            results.forEach((item) => {
                const row = document.createElement("div");
                row.className = "link-result";

                const info = document.createElement("div");
                const name = document.createElement("strong");
                name.textContent = item.Nom || "-";
                info.appendChild(name);
                if (item.Municipi) {
                    const mun = document.createElement("span");
                    mun.className = "muted";
                    mun.textContent = ` · ${item.Municipi}`;
                    info.appendChild(mun);
                }
                if (item.Any) {
                    const any = document.createElement("span");
                    any.className = "muted";
                    any.textContent = ` · ${item.Any}`;
                    info.appendChild(any);
                }

                const btn = document.createElement("button");
                btn.type = "button";
                btn.className = "boto-secundari";
                btn.textContent = labelSelectPerson || labelLink;
                btn.addEventListener("click", () => {
                    selectedPersonaID = item.ID;
                    if (selectedResult) {
                        selectedResult.classList.remove("selected");
                    }
                    row.classList.add("selected");
                    selectedResult = row;
                    const submitButton = formContainer ? formContainer.querySelector("#link-submit") : null;
                    if (submitButton) {
                        submitButton.disabled = false;
                    }
                });

                row.appendChild(info);
                row.appendChild(btn);
                container.appendChild(row);
            });
        }

        function searchPeople(query, container) {
            if (!searchEndpoint || !currentRegistreID || !currentRawID) {
                renderResults([], container);
                return;
            }
            const params = new URLSearchParams();
            if (query) {
                params.set("q", query);
            }
            params.set("registre_id", currentRegistreID.toString());
            fetch(`${searchEndpoint}?${params.toString()}`, {
                method: "GET",
                headers: { "Accept": "application/json" },
                credentials: "same-origin",
            })
                .then((response) => {
                    if (!response.ok) {
                        throw new Error("search_failed");
                    }
                    return response.json();
                })
                .then((data) => {
                    renderResults(data, container);
                })
                .catch(() => {
                    renderResults([], container);
                });
        }

        function linkPersona(personaID) {
            if (!currentRegistreID || !currentRawID) {
                return;
            }
            const body = new URLSearchParams();
            body.set("csrf_token", csrfToken);
            body.set("persona_id", personaID.toString());
            body.set("return_to", returnTo);
            fetch(`/documentals/registres/${currentRegistreID}/persones/${currentRawID}/enllacar`, {
                method: "POST",
                headers: { "Content-Type": "application/x-www-form-urlencoded" },
                body: body.toString(),
                credentials: "same-origin",
            }).then((response) => {
                if (response.ok) {
                    window.location.href = returnTo;
                }
            });
        }

        document.addEventListener("click", (event) => {
            const button = event.target.closest(".btn-interconnectar");
            if (!button) {
                return;
            }
            openModal(button);
        });

        if (typeSelect) {
            typeSelect.addEventListener("change", () => {
                buildSearchForm(typeSelect.value);
            });
        }

        if (closeModalBtn) {
            closeModalBtn.addEventListener("click", closeModal);
        }
        window.addEventListener("click", (event) => {
            if (event.target === linkModal) {
                closeModal();
            }
        });
    }

});
