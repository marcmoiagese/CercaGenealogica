(function () {
    const configEl = document.getElementById("indexer-config");
    const draftEl = document.getElementById("indexer-draft");
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
    let draftRows = [];
    try {
        const parsed = JSON.parse(draftEl ? draftEl.textContent || "[]" : "[]");
        draftRows = Array.isArray(parsed) ? parsed : (parsed.rows || []);
    } catch (e) {
        draftRows = [];
    }

    const labels = {
        action: root.dataset.actionLabel || "Action",
        remove: root.dataset.removeLabel || "Remove",
        saved: root.dataset.draftSaved || "Saved",
        failed: root.dataset.draftFailed || "Failed",
        submitFailed: root.dataset.submitFailed || "Failed",
        empty: root.dataset.empty || "Empty"
    };

    const csrfToken = document.querySelector("meta[name='csrf-token']")?.content || "";
    const tbody = table.querySelector("tbody");
    const thead = table.querySelector("thead");

    const rows = draftRows.length ? draftRows : [createEmptyRow()];
    normalizeRows();
    renderTable();

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

    function createEmptyRow() {
        const row = {};
        (config.fields || []).forEach((field) => {
            if (field.default) {
                row[field.key] = field.default;
            } else {
                row[field.key] = "";
            }
        });
        return row;
    }

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
        (config.fields || []).forEach((field) => {
            const td = document.createElement("td");
            td.appendChild(buildInput(field, index));
            tr.appendChild(td);
        });
        const tdActions = document.createElement("td");
        tdActions.className = "indexer-row-actions";
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
        tr.appendChild(tdActions);
        return tr;
    }

    function renderTable() {
        buildHeader();
        tbody.innerHTML = "";
        rows.forEach((row, index) => {
            tbody.appendChild(buildRow(row, index));
        });
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
        return rows.map((row) => {
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
        return list.filter((row) => {
            return Object.values(row).some((v) => String(v).trim() !== "");
        });
    }

    function saveDraft() {
        updateStatus("", false);
        const payload = { rows: filterEmptyRows(collectRows()) };
        fetch(window.location.pathname + "/draft", {
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
        fetch(window.location.pathname + "/clear", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "X-CSRF-Token": csrfToken
            }
        })
            .then(() => {
                rows.length = 0;
                rows.push(createEmptyRow());
                renderTable();
                updateStatus("", false);
            })
            .catch(() => {
                updateStatus(labels.failed, true);
            });
    }

    addBtns.forEach((btn) => {
        btn.addEventListener("click", () => {
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
        if (payload.length === 0) {
            e.preventDefault();
            updateStatus(labels.empty, true);
            return;
        }
        payloadInput.value = JSON.stringify({ rows: payload });
    });
})();
