(function () {
    const configEl = document.getElementById("indexer-config");
    const draftEl = document.getElementById("indexer-draft");
    const existingEl = document.getElementById("indexer-existing");
    const table = document.getElementById("indexer-table");
    const form = document.getElementById("indexer-form");
    const payloadInput = document.getElementById("indexer-payload");
    const addBtns = document.querySelectorAll(".indexer-add-row");
    const saveBtns = document.querySelectorAll(".indexer-save-draft");
    const clearBtns = document.querySelectorAll(".indexer-clear-draft");
    const statusEl = document.getElementById("indexer-status");
    const root = document.getElementById("indexer-root");
    if (!configEl || !table || !form || !root) {
        return;
    }

    let config = {};
    try {
        config = JSON.parse(configEl.textContent || "{}");
    } catch (e) {
        return;
    }
    const query = new URLSearchParams(window.location.search);
    const pageFieldKey = "pagina_llibre";
    const digitalFieldKey = "pagina_digital";
    function parsePageLimit(raw) {
        if (raw === undefined || raw === null || raw === "") {
            return null;
        }
        const n = parseInt(raw, 10);
        if (!Number.isFinite(n) || n < 0) {
            return null;
        }
        return n;
    }
    const datasetPage = (root.dataset.pageValue || "").trim();
    const datasetDigital = (root.dataset.pageDigital || "").trim();
    const datasetLimit = parsePageLimit(root.dataset.pageRemaining);
    const datasetTotal = parsePageLimit(root.dataset.pageTotal);
    const datasetInlineEdit = root.dataset.inlineEdit === "1";
    const queryPage = (query.get("pagina") || query.get("pagina_llibre") || query.get("num_pagina_text") || "").trim();
    const queryDigital = (query.get("pagina_digital") || "").trim();
    const queryLimit = parsePageLimit(query.get("page_remaining"));
    let context = {
        pageValue: datasetPage || queryPage || "",
        digitalValue: datasetDigital || queryDigital || "",
        pageLimit: datasetLimit !== null ? datasetLimit : queryLimit,
        pageTotal: datasetTotal,
        canInlineEdit: datasetInlineEdit
    };
    function applyContextDefaults(ctx) {
        if (!Array.isArray(config.fields)) {
            return;
        }
        config.fields.forEach((field) => {
            if (field.key === pageFieldKey) {
                field.default = ctx.pageValue || "";
            }
            if (field.key === digitalFieldKey) {
                field.default = ctx.digitalValue || "";
            }
        });
    }
    applyContextDefaults(context);
    const positionFieldKey = findPositionFieldKey();
    function filterRowsByPage(list, pageValue) {
        if (!pageValue || !pageFieldKey) {
            return list;
        }
        return (list || []).filter((row) => {
            const raw = row && row[pageFieldKey] !== undefined ? String(row[pageFieldKey]).trim() : "";
            return raw === "" || raw === pageValue;
        });
    }
    let draftRows = [];
    try {
        const parsed = JSON.parse(draftEl ? draftEl.textContent || "[]" : "[]");
        draftRows = Array.isArray(parsed) ? parsed : (parsed.rows || []);
    } catch (e) {
        draftRows = [];
    }
    draftRows = filterRowsByPage(draftRows, context.pageValue);
    let existingRows = [];
    try {
        const parsed = JSON.parse(existingEl ? existingEl.textContent || "[]" : "[]");
        existingRows = Array.isArray(parsed) ? parsed : (parsed && parsed.rows ? parsed.rows : []);
    } catch (e) {
        existingRows = [];
    }
    if (Array.isArray(existingRows)) {
        existingRows = existingRows.map((row) => {
            if (row && !row.__readonly) {
                row.__readonly = "1";
            }
            return row;
        });
    }

    const labels = {
        action: root.dataset.actionLabel || "Action",
        remove: root.dataset.removeLabel || "Remove",
        saved: root.dataset.draftSaved || "Saved",
        failed: root.dataset.draftFailed || "Failed",
        submitFailed: root.dataset.submitFailed || "Failed",
        empty: root.dataset.empty || "Empty",
        limitReached: root.dataset.limitReached || "Limit reached",
        limitExceeded: root.dataset.limitExceeded || "Limit exceeded",
        view: root.dataset.recordView || "View",
        edit: root.dataset.recordEdit || "Edit",
        save: root.dataset.recordSave || "Save",
        cancel: root.dataset.recordCancel || "Cancel"
    };

    const csrfToken = document.querySelector("meta[name='csrf-token']")?.content || "";
    const inlineBase = root.dataset.inlineUrl || "/documentals/registres";
    const draftUrl = root.dataset.draftUrl || (window.location.pathname + "/draft");
    const clearUrl = root.dataset.clearUrl || (window.location.pathname + "/clear");
    const tbody = table.querySelector("tbody");
    const thead = table.querySelector("thead");

    const rows = [];
    resetRows();

    let saveTimer;
    function scheduleSave() {
        if (saveTimer) {
            clearTimeout(saveTimer);
        }
        saveTimer = setTimeout(() => saveDraft(), 900);
    }

    function updateStatus(text, isError) {
        if (!statusEl) {
            return;
        }
        statusEl.textContent = text || "";
        statusEl.classList.toggle("error", !!isError);
    }

    function isReadonlyRow(row) {
        return row && row.__readonly === "1";
    }

    function shouldIgnoreForEmptyCheck(field) {
        if (!field) {
            return false;
        }
        if (field.key === pageFieldKey || field.key === digitalFieldKey || field.key === positionFieldKey) {
            return true;
        }
        if (field.key === "qualitat_general" || field.raw_field === "data_acte_estat") {
            return true;
        }
        if (field.target === "person" && field.person_field && field.person_field.endsWith("_estat")) {
            return true;
        }
        if (field.target === "attr" && field.attr_key === "pagina_digital") {
            return true;
        }
        return false;
    }

    function rowHasMeaningfulData(row) {
        if (!row || !Array.isArray(config.fields)) {
            return false;
        }
        for (const field of config.fields) {
            if (shouldIgnoreForEmptyCheck(field)) {
                continue;
            }
            const val = row[field.key];
            if (val !== undefined && val !== null && String(val).trim() !== "") {
                return true;
            }
        }
        return false;
    }

    function findPositionFieldKey() {
        if (!Array.isArray(config.fields)) {
            return "";
        }
        const match = config.fields.find((field) => field.target === "raw" && field.raw_field === "posicio_pagina");
        return match ? match.key : "";
    }

    function isFixedMode() {
        return context.pageTotal !== null && context.pageTotal > 0;
    }

    function canInlineEdit() {
        return !!context.canInlineEdit;
    }

    function getRemainingSlots() {
        if (!isFixedMode()) {
            return 0;
        }
        const existingCount = Array.isArray(existingRows) ? existingRows.length : 0;
        return Math.max(context.pageTotal - existingCount, 0);
    }

    function createEmptyRow(position) {
        const row = {};
        (config.fields || []).forEach((field) => {
            if (field.default) {
                row[field.key] = field.default;
            } else {
                row[field.key] = "";
            }
        });
        if (positionFieldKey && Number.isFinite(position) && position > 0) {
            row[positionFieldKey] = String(position);
        }
        return row;
    }

    function applyContextToRows(prevCtx, nextCtx) {
        if (!Array.isArray(rows)) {
            return;
        }
        rows.forEach((row) => {
            if (isReadonlyRow(row)) {
                return;
            }
            if (pageFieldKey && nextCtx.pageValue) {
                const current = row[pageFieldKey] || "";
                if (current === "" || current === prevCtx.pageValue) {
                    row[pageFieldKey] = nextCtx.pageValue;
                }
            }
            if (digitalFieldKey && nextCtx.digitalValue) {
                const current = row[digitalFieldKey] || "";
                if (current === "" || current === prevCtx.digitalValue) {
                    row[digitalFieldKey] = nextCtx.digitalValue;
                }
            }
        });
    }

    function buildFixedRows(list, limit, draft) {
        const slots = new Array(limit).fill(null);
        const extras = [];
        (list || []).forEach((row) => {
            let pos = 0;
            if (row.__position) {
                pos = parseInt(row.__position, 10);
            } else if (positionFieldKey && row[positionFieldKey]) {
                pos = parseInt(row[positionFieldKey], 10);
            }
            if (Number.isFinite(pos) && pos >= 1 && pos <= limit && !slots[pos - 1]) {
                slots[pos - 1] = row;
                return;
            }
            extras.push(row);
        });
        extras.forEach((row) => {
            const idx = slots.indexOf(null);
            if (idx !== -1) {
                slots[idx] = row;
            }
        });
        (draft || []).forEach((row) => {
            const idx = slots.indexOf(null);
            if (idx !== -1) {
                slots[idx] = row;
            }
        });
        for (let i = 0; i < slots.length; i++) {
            if (!slots[i]) {
                slots[i] = createEmptyRow(i + 1);
            }
        }
        return slots;
    }

    function buildInitialRows() {
        if (isFixedMode()) {
            return buildFixedRows(existingRows, context.pageTotal, draftRows);
        }
        if (draftRows.length) {
            return draftRows;
        }
        return [createEmptyRow()];
    }

    function resetRows() {
        rows.length = 0;
        buildInitialRows().forEach((row) => rows.push(row));
        applyContextToRows({ pageValue: "", digitalValue: "" }, context);
        normalizeRows();
        renderTable();
    }

    function getMaxRows() {
        if (isFixedMode()) {
            return context.pageTotal;
        }
        const baseLimit = config.max_rows || 0;
        if (context.pageLimit !== null) {
            if (baseLimit > 0) {
                return Math.min(baseLimit, context.pageLimit);
            }
            return context.pageLimit;
        }
        return baseLimit;
    }

    function isLimitReached() {
        const limit = getMaxRows();
        if (isFixedMode()) {
            return true;
        }
        if (context.pageLimit !== null) {
            return limit === 0 || rows.length >= limit;
        }
        return limit > 0 && rows.length >= limit;
    }

    function updateLimitState() {
        const limit = getMaxRows();
        const reached = isLimitReached();
        addBtns.forEach((btn) => {
            btn.disabled = reached;
            btn.style.display = isFixedMode() ? "none" : "";
        });
        if (isFixedMode() && Array.isArray(existingRows) && existingRows.length > limit) {
            updateStatus(labels.limitExceeded, true);
            return;
        }
        if (reached && limit > 0 && rows.length > limit) {
            updateStatus(labels.limitExceeded, true);
        }
    }

    function updateContextFromDetail(detail) {
        if (!detail) {
            return;
        }
        const wasFixed = isFixedMode();
        const prev = {
            pageValue: context.pageValue,
            digitalValue: context.digitalValue
        };
        const next = {
            pageValue: context.pageValue,
            digitalValue: context.digitalValue,
            pageLimit: context.pageLimit,
            pageTotal: context.pageTotal
        };
        if (typeof detail.pageValue === "string") {
            next.pageValue = detail.pageValue.trim();
        }
        if (typeof detail.digitalValue === "string") {
            next.digitalValue = detail.digitalValue.trim();
        }
        if ("pageLimit" in detail) {
            next.pageLimit = Number.isFinite(detail.pageLimit) ? Math.max(detail.pageLimit, 0) : null;
        }
        if ("pageTotal" in detail) {
            next.pageTotal = Number.isFinite(detail.pageTotal) ? Math.max(detail.pageTotal, 0) : null;
        }
        if ("canInlineEdit" in detail) {
            next.canInlineEdit = !!detail.canInlineEdit;
        }
        if (Array.isArray(detail.existingRows)) {
            existingRows = detail.existingRows.map((row) => {
                if (row && !row.__readonly) {
                    row.__readonly = "1";
                }
                return row;
            });
        }
        if (prev.pageValue !== next.pageValue) {
            draftRows = filterRowsByPage(draftRows, next.pageValue);
        }
        context = next;
        applyContextDefaults(context);
        if (wasFixed || isFixedMode() || Array.isArray(detail.existingRows)) {
            resetRows();
        } else {
            applyContextToRows(prev, context);
            renderTable();
        }
    }

    root.addEventListener("indexer:context", (event) => {
        updateContextFromDetail(event.detail);
    });

    function normalizeRows() {
        rows.forEach((row) => {
            (config.fields || []).forEach((field) => {
                if (!(field.key in row)) {
                    if (field.default) {
                        row[field.key] = field.default;
                    } else {
                        row[field.key] = "";
                    }
                }
            });
        });
    }

    function buildHeader() {
        thead.innerHTML = "";
        const tr = document.createElement("tr");
        (config.fields || []).forEach((field) => {
            const th = document.createElement("th");
            th.textContent = field.label || field.key;
            tr.appendChild(th);
        });
        const actionTh = document.createElement("th");
        actionTh.textContent = labels.action;
        tr.appendChild(actionTh);
        thead.appendChild(tr);
    }

    function formatValue(field, raw) {
        if (raw === undefined || raw === null) {
            return "";
        }
        const value = String(raw);
        if (field.input === "select" && Array.isArray(field.options)) {
            const opt = field.options.find((o) => String(o.value) === value);
            if (opt && opt.label) {
                return opt.label;
            }
        }
        return value;
    }

    function buildInput(field, rowIndex) {
        let input;
        if (field.input === "textarea") {
            input = document.createElement("textarea");
        } else if (field.input === "select") {
            input = document.createElement("select");
            (field.options || []).forEach((opt) => {
                const option = document.createElement("option");
                option.value = opt.value;
                option.textContent = opt.label || opt.value;
                input.appendChild(option);
            });
        } else {
            input = document.createElement("input");
            if (field.input === "number") {
                input.type = "number";
            } else if (field.input === "date") {
                input.type = "date";
            } else {
                input.type = "text";
            }
        }
        input.dataset.row = String(rowIndex);
        input.dataset.key = field.key;
        input.value = rows[rowIndex][field.key] || "";
        input.addEventListener("input", onInputChange);
        input.addEventListener("change", onInputChange);
        return input;
    }

    function buildRow(row, index) {
        const tr = document.createElement("tr");
        const readonly = isReadonlyRow(row);
        if (readonly) {
            tr.classList.add("indexer-row-readonly");
        }
        (config.fields || []).forEach((field) => {
            const td = document.createElement("td");
            if (readonly) {
                td.appendChild(buildReadonlyCell(field, row));
            } else {
                td.appendChild(buildInput(field, index));
            }
            tr.appendChild(td);
        });
        const tdActions = document.createElement("td");
        tdActions.className = "indexer-row-actions";
        if (readonly) {
            const recordID = row.__record_id;
            if (recordID) {
                const viewLink = document.createElement("a");
                viewLink.className = "indexer-row-link";
                viewLink.href = "/documentals/registres/" + recordID;
                viewLink.textContent = labels.view;
                tdActions.appendChild(viewLink);
                const editLink = document.createElement("a");
                editLink.className = "indexer-row-link";
                editLink.href = "/documentals/registres/" + recordID + "/editar";
                editLink.textContent = labels.edit;
                tdActions.appendChild(editLink);
            }
        } else if (!isFixedMode()) {
            const removeBtn = document.createElement("button");
            removeBtn.type = "button";
            removeBtn.className = "boto-eliminar-fila indexer-remove";
            removeBtn.innerHTML = `<i class="fas fa-trash"></i> ${labels.remove}`;
            removeBtn.setAttribute("aria-label", labels.remove);
            removeBtn.addEventListener("click", () => {
                rows.splice(index, 1);
                if (rows.length === 0) {
                    rows.push(createEmptyRow());
                }
                renderTable();
                scheduleSave();
            });
            tdActions.appendChild(removeBtn);
        }
        tr.appendChild(tdActions);
        return tr;
    }

    function buildReadonlyCell(field, row) {
        const recordId = row.__record_id || "";
        const wrap = document.createElement("div");
        wrap.className = "indexer-cell";
        const text = document.createElement("span");
        text.className = "indexer-cell-text";
        text.textContent = formatValue(field, row[field.key] || "") || "—";
        wrap.appendChild(text);
        if (!canInlineEdit() || !recordId) {
            return wrap;
        }
        const editBtn = document.createElement("button");
        editBtn.type = "button";
        editBtn.className = "indexer-cell-edit";
        editBtn.setAttribute("aria-label", labels.edit);
        editBtn.innerHTML = '<i class="fa fa-pencil"></i>';
        editBtn.addEventListener("click", () => {
            startInlineEdit(wrap, field, row, recordId);
        });
        wrap.appendChild(editBtn);
        return wrap;
    }

    function createInlineInput(field, value) {
        let input;
        if (field.input === "textarea") {
            input = document.createElement("textarea");
        } else if (field.input === "select") {
            input = document.createElement("select");
            const emptyOption = document.createElement("option");
            emptyOption.value = "";
            emptyOption.textContent = labels.empty;
            input.appendChild(emptyOption);
            (field.options || []).forEach((opt) => {
                const option = document.createElement("option");
                option.value = opt.value;
                option.textContent = opt.label || opt.value;
                input.appendChild(option);
            });
        } else {
            input = document.createElement("input");
            if (field.input === "number") {
                input.type = "number";
            } else if (field.input === "date") {
                input.type = "date";
            } else {
                input.type = "text";
            }
        }
        input.className = "indexer-cell-input";
        input.value = value || "";
        return input;
    }

    function startInlineEdit(wrapper, field, row, recordId) {
        if (!wrapper || wrapper.classList.contains("is-editing")) {
            return;
        }
        wrapper.classList.add("is-editing");
        wrapper.innerHTML = "";
        const input = createInlineInput(field, row[field.key] || "");
        wrapper.appendChild(input);
        const confirmBtn = document.createElement("button");
        confirmBtn.type = "button";
        confirmBtn.className = "indexer-cell-confirm";
        confirmBtn.setAttribute("aria-label", labels.save);
        confirmBtn.innerHTML = '<i class="fa fa-check"></i>';
        const cancelBtn = document.createElement("button");
        cancelBtn.type = "button";
        cancelBtn.className = "indexer-cell-cancel";
        cancelBtn.setAttribute("aria-label", labels.cancel);
        cancelBtn.innerHTML = '<i class="fa fa-times"></i>';
        wrapper.appendChild(confirmBtn);
        wrapper.appendChild(cancelBtn);
        const finish = (newValue, displayValue) => {
            wrapper.classList.remove("is-editing");
            wrapper.innerHTML = "";
            row[field.key] = newValue;
            const text = document.createElement("span");
            text.className = "indexer-cell-text";
            text.textContent = displayValue || "—";
            wrapper.appendChild(text);
            if (canInlineEdit()) {
                const editBtn = document.createElement("button");
                editBtn.type = "button";
                editBtn.className = "indexer-cell-edit";
                editBtn.setAttribute("aria-label", labels.edit);
                editBtn.innerHTML = '<i class="fa fa-pencil"></i>';
                editBtn.addEventListener("click", () => {
                    startInlineEdit(wrapper, field, row, recordId);
                });
                wrapper.appendChild(editBtn);
            }
        };
        const submit = () => {
            const newValue = input.value;
            fetch(`${inlineBase}/${recordId}/inline`, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "X-CSRF-Token": csrfToken
                },
                credentials: "same-origin",
                body: JSON.stringify({ key: field.key, value: newValue })
            })
                .then((res) => {
                    if (!res.ok) {
                        throw new Error("inline_failed");
                    }
                    return res.json();
                })
                .then((data) => {
                    const display = data && typeof data.value === "string" ? data.value : newValue;
                    const raw = data && typeof data.raw === "string" ? data.raw : newValue;
                    finish(raw, display);
                })
                .catch(() => {
                    updateStatus(labels.submitFailed, true);
                    finish(row[field.key] || "", formatValue(field, row[field.key] || "") || "—");
                });
        };
        confirmBtn.addEventListener("click", submit);
        cancelBtn.addEventListener("click", () => {
            finish(row[field.key] || "", formatValue(field, row[field.key] || "") || "—");
        });
        input.addEventListener("keydown", (event) => {
            if (event.key === "Enter" && input.tagName !== "TEXTAREA") {
                event.preventDefault();
                submit();
            } else if (event.key === "Escape") {
                event.preventDefault();
                finish(row[field.key] || "", formatValue(field, row[field.key] || "") || "—");
            }
        });
        input.focus();
    }

    function renderTable() {
        buildHeader();
        tbody.innerHTML = "";
        rows.forEach((row, index) => {
            tbody.appendChild(buildRow(row, index));
        });
        updateLimitState();
    }

    function onInputChange(e) {
        const rowIdx = parseInt(e.target.dataset.row, 10);
        const key = e.target.dataset.key;
        if (!Number.isInteger(rowIdx) || !key) {
            return;
        }
        rows[rowIdx][key] = e.target.value;
        scheduleSave();
    }

    function collectRows() {
        return rows.filter((row) => !isReadonlyRow(row)).map((row) => {
            const cleaned = {};
            Object.keys(row || {}).forEach((key) => {
                const val = row[key];
                if (val !== undefined && val !== null) {
                    cleaned[key] = String(val);
                }
            });
            return cleaned;
        });
    }

    function filterEmptyRows(list) {
        return list.filter((row) => rowHasMeaningfulData(row));
    }

    function saveDraft() {
        updateStatus("", false);
        const payload = { rows: filterEmptyRows(collectRows()) };
        fetch(draftUrl, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "X-CSRF-Token": csrfToken
            },
            body: JSON.stringify(payload)
        })
            .then((res) => {
                if (!res.ok) {
                    throw new Error("draft");
                }
                updateStatus(labels.saved, false);
            })
            .catch(() => {
                updateStatus(labels.failed, true);
            });
    }

    function clearDraft() {
        updateStatus("", false);
        fetch(clearUrl, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "X-CSRF-Token": csrfToken
            }
        })
            .then(() => {
                if (isFixedMode()) {
                    resetRows();
                } else {
                    rows.length = 0;
                    rows.push(createEmptyRow());
                    renderTable();
                }
                updateStatus("", false);
            })
            .catch(() => {
                updateStatus(labels.failed, true);
            });
    }

    addBtns.forEach((btn) => {
        btn.addEventListener("click", () => {
            if (isLimitReached()) {
                updateStatus(labels.limitReached, true);
                return;
            }
            rows.push(createEmptyRow());
            renderTable();
            scheduleSave();
        });
    });
    saveBtns.forEach((btn) => {
        btn.addEventListener("click", saveDraft);
    });
    clearBtns.forEach((btn) => {
        btn.addEventListener("click", clearDraft);
    });

    form.addEventListener("submit", (e) => {
        const payload = filterEmptyRows(collectRows());
        const limit = getMaxRows();
        if (isFixedMode()) {
            const remaining = getRemainingSlots();
            if (limit === 0 || payload.length > remaining) {
                e.preventDefault();
                updateStatus(labels.limitExceeded, true);
                return;
            }
        } else if (context.pageLimit !== null && (limit === 0 || payload.length > limit)) {
            e.preventDefault();
            updateStatus(labels.limitExceeded, true);
            return;
        }
        if (payload.length === 0) {
            e.preventDefault();
            updateStatus(labels.empty, true);
            return;
        }
        payloadInput.value = JSON.stringify({ rows: payload });
    });

    const errorParam = query.get("error");
    if (errorParam === "page_limit" || errorParam === "limit") {
        updateStatus(labels.limitExceeded, true);
    }
})();
