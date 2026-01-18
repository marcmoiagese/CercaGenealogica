(function () {
    const editor = document.getElementById("mapEditor");
    if (!editor) return;

    const mapId = editor.dataset.mapId;
    const csrf = editor.dataset.csrf;
    const mapTitle = editor.dataset.mapTitle || "mapa";
    const deleteConfirm = editor.dataset.deleteConfirm || "";
    const historicRemoveLabel = editor.dataset.historicRemove || "";
    const personSearchEndpoint = editor.dataset.personSearchEndpoint || "";
    const personEmptyLabel = editor.dataset.personEmpty || "";
    const personAddLabel = editor.dataset.personAdd || "";
    const exportEmptyLabel = editor.dataset.exportEmpty || "No hi ha mapa per exportar.";
    const exportUnavailableLabel = editor.dataset.exportUnavailable || "No es pot exportar ara mateix.";
    const exportSvgDoneLabel = editor.dataset.exportSvgDone || "SVG exportat.";
    const exportJsonDoneLabel = editor.dataset.exportJsonDone || "Codi exportat.";
    const importErrorLabel = editor.dataset.importError || "Codi invàlid.";
    const importSuccessLabel = editor.dataset.importSuccess || "Mapa carregat.";

    const svg = document.getElementById("mapEditorSvg");
    const overlay = document.getElementById("mapEditorOverlay");
    const statusEl = document.getElementById("mapEditorStatus");
    const hintEl = document.getElementById("mapEditorHint");
    const saveBtn = document.getElementById("mapSave");
    const submitBtn = document.getElementById("mapSubmit");
    const exportSvgBtn = document.getElementById("mapExportSvg");
    const exportJsonBtn = document.getElementById("mapExportJson");
    const importOpenBtn = document.getElementById("mapImportOpen");
    const streetTypeSelect = document.getElementById("mapEditorStreetType");
    const riverTypeSelect = document.getElementById("mapEditorRiverType");
    const elementTypeSelect = document.getElementById("mapEditorElementType");
    const labelInput = document.getElementById("mapEditorLabel");
    const notesInput = document.getElementById("mapEditorNotes");
    const peopleSearchInput = document.getElementById("mapEditorPeopleSearch");
    const peopleResults = document.getElementById("mapEditorPeopleResults");
    const peopleList = document.getElementById("mapEditorPeopleList");
    const houseCoordX = document.getElementById("mapEditorHouseCoordX");
    const houseCoordY = document.getElementById("mapEditorHouseCoordY");
    const historicList = document.getElementById("mapEditorHistoricList");
    const historicAddBtn = document.getElementById("mapEditorHistoricAdd");
    const deleteBtn = document.getElementById("mapEditorDelete");
    const emptyState = document.getElementById("mapEditorEmpty");
    const formEl = document.getElementById("mapEditorForm");
    const changelogInput = document.getElementById("mapEditorChangelog");
    const importModal = document.getElementById("mapEditorCodeModal");
    const importCloseBtn = document.getElementById("mapEditorCodeClose");
    const importCancelBtn = document.getElementById("mapEditorCodeCancel");
    const importApplyBtn = document.getElementById("mapEditorCodeApply");
    const importInput = document.getElementById("mapEditorCodeInput");
    const importFileInput = document.getElementById("mapEditorCodeFile");
    const importStatus = document.getElementById("mapEditorCodeStatus");
    const toolButtons = editor.querySelectorAll("[data-tool]");
    const fieldBlocks = editor.querySelectorAll(".map-editor-field[data-layer]");

    const baseHint = hintEl ? hintEl.textContent : "";

    const NS = "http://www.w3.org/2000/svg";
    let versionId = null;
    let lockVersion = 0;
    let model = null;
    let activeTool = "select";
    let drawing = null;
    let drag = null;
    let previewEl = null;
    let selected = null;
    let dragHandle = null;
    let dragHandleActive = false;
    let peopleSearchTimer = null;

    function setStatus(msg) {
        if (statusEl) statusEl.textContent = msg || "";
    }

    function setImportStatus(msg) {
        if (importStatus) importStatus.textContent = msg || "";
    }

    function sanitizeFileName(name) {
        const base = String(name || "mapa");
        const normalized = base.normalize ? base.normalize("NFD").replace(/[\u0300-\u036f]/g, "") : base;
        const cleaned = normalized.replace(/[^a-zA-Z0-9]+/g, "-").replace(/^-+|-+$/g, "").toLowerCase();
        return cleaned || "mapa";
    }

    function downloadBlob(filename, blob) {
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        link.remove();
        setTimeout(() => URL.revokeObjectURL(url), 0);
    }

    function exportStyleText() {
        return [
            ".cg-house{fill:#d9cfc6;stroke:#8f7c6b;stroke-width:2;}",
            ".cg-street{fill:none;stroke:#5c6b73;stroke-width:4;stroke-linecap:round;stroke-linejoin:round;}",
            ".cg-street--default{stroke:#5c6b73;stroke-width:4;}",
            ".cg-street--asfaltat{stroke:#3b4b55;stroke-width:5;}",
            ".cg-street--empedrat{stroke:#6c5b4c;stroke-width:5;stroke-dasharray:7 5;}",
            ".cg-street--terra{stroke:#a9714b;stroke-width:4;opacity:.7;}",
            ".cg-street--carretera{stroke:#1f2937;stroke-width:7;stroke-dasharray:10 6;stroke-linecap:round;}",
            ".cg-street--autopista{stroke:#0f172a;stroke-width:11;stroke-linecap:round;}",
            ".cg-river{fill:none;stroke:#3b82f6;stroke-width:6;opacity:.7;}",
            ".cg-river--riu{stroke:#2563eb;stroke-width:7;}",
            ".cg-river--riera{stroke:#38bdf8;stroke-width:5;stroke-dasharray:10 6;}",
            ".cg-river--rierol{stroke:#7dd3fc;stroke-width:4;stroke-dasharray:5 6;}",
            ".cg-street-label,.cg-house-label,.cg-toponym-label{font-size:18px;fill:#2b2b2b;}",
            ".cg-marker{fill:#3f7fbf;stroke:#2b567f;stroke-width:2;}",
            ".cg-element{fill:#3f7fbf;stroke:#1f3b57;stroke-width:2;}",
            ".cg-element--tree{fill:#2f855a;stroke:#1f5c3a;}",
            ".cg-element--fountain{fill:#0ea5e9;stroke:#0369a1;}",
            ".cg-element--well{fill:#6b7280;stroke:#374151;}",
            ".cg-element--bench{fill:#b45309;stroke:#78350f;}",
            ".cg-boundary{fill:none;stroke:#9c8b7a;stroke-width:3;stroke-dasharray:6 6;}",
        ].join("\n");
    }

    function exportSvg() {
        if (!model) {
            setStatus(exportEmptyLabel);
            return;
        }
        ensureLayers();
        if (!window.CGMap) {
            setStatus(exportUnavailableLabel);
            return;
        }
        const exportSvgEl = document.createElementNS(NS, "svg");
        exportSvgEl.setAttribute("xmlns", "http://www.w3.org/2000/svg");
        if (Array.isArray(model.viewBox) && model.viewBox.length >= 4) {
            exportSvgEl.setAttribute("viewBox", model.viewBox.join(" "));
        } else {
            exportSvgEl.setAttribute("viewBox", "0 0 1000 700");
        }
        window.CGMap.render(exportSvgEl, model);
        const metadata = document.createElementNS(NS, "metadata");
        metadata.setAttribute("id", "cgmap-data");
        metadata.textContent = JSON.stringify(model);
        exportSvgEl.insertBefore(metadata, exportSvgEl.firstChild);
        const style = document.createElementNS(NS, "style");
        style.textContent = exportStyleText();
        exportSvgEl.insertBefore(style, metadata);
        const serializer = new XMLSerializer();
        const svgText = serializer.serializeToString(exportSvgEl);
        const payload = '<?xml version="1.0" encoding="UTF-8"?>\n' + svgText;
        const fileName = sanitizeFileName(mapTitle) + ".svg";
        downloadBlob(fileName, new Blob([payload], { type: "image/svg+xml;charset=utf-8" }));
        setStatus(exportSvgDoneLabel);
    }

    function exportJson() {
        if (!model) {
            setStatus(exportEmptyLabel);
            return;
        }
        ensureLayers();
        const payload = JSON.stringify(model, null, 2);
        const fileName = sanitizeFileName(mapTitle) + ".json";
        downloadBlob(fileName, new Blob([payload], { type: "application/json;charset=utf-8" }));
        setStatus(exportJsonDoneLabel);
    }

    function openImportModal() {
        if (!importModal) return;
        importModal.classList.add("is-open");
        setImportStatus("");
        if (importInput) {
            importInput.value = "";
            importInput.focus();
        }
        if (importFileInput) {
            importFileInput.value = "";
        }
    }

    function closeImportModal() {
        if (!importModal) return;
        importModal.classList.remove("is-open");
        setImportStatus("");
    }

    function applyImportText(raw) {
        const text = (raw || "").trim();
        if (!text) {
            setImportStatus(importErrorLabel);
            return;
        }
        try {
            const parsed = JSON.parse(text);
            if (!parsed || typeof parsed !== "object") {
                throw new Error("invalid");
            }
            model = parsed;
            ensureLayers();
            clearSelection();
            render();
            setStatus(importSuccessLabel);
            closeImportModal();
        } catch (err) {
            setImportStatus(importErrorLabel);
        }
    }

    function applyImport() {
        if (!importInput) return;
        applyImportText(importInput.value);
    }

    function setHint(msg) {
        if (hintEl) hintEl.textContent = msg || "";
    }

    function ensureLayers() {
        if (!model) {
            model = { viewBox: [0, 0, 1000, 700], layers: { houses: [], streets: [], rivers: [], elements: [], toponyms: [], bounds: [] } };
            return;
        }
        if (!model.viewBox) model.viewBox = [0, 0, 1000, 700];
        if (!model.layers) model.layers = {};
        if (!Array.isArray(model.layers.houses)) model.layers.houses = [];
        if (!Array.isArray(model.layers.streets)) model.layers.streets = [];
        if (!Array.isArray(model.layers.rivers)) model.layers.rivers = [];
        if (!Array.isArray(model.layers.elements)) model.layers.elements = [];
        if (!Array.isArray(model.layers.toponyms)) model.layers.toponyms = [];
        if (!Array.isArray(model.layers.bounds)) model.layers.bounds = [];
    }

    function applySelection() {
        if (!svg) return;
        svg.querySelectorAll(".is-selected").forEach((node) => node.classList.remove("is-selected"));
        if (!selected) return;
        const selector = `[data-layer-item="${selected.layer}"][data-index="${selected.index}"]`;
        svg.querySelectorAll(selector).forEach((node) => node.classList.add("is-selected"));
    }

    function renderHandles() {
        if (!svg) return;
        const existing = svg.querySelector(".map-editor-handles");
        if (existing && existing.parentNode) {
            existing.parentNode.removeChild(existing);
        }
        const item = getSelectedItem();
        if (!item || !Array.isArray(item.points)) return;
        const group = document.createElementNS(NS, "g");
        group.setAttribute("class", "map-editor-handles");
        item.points.forEach((pt, idx) => {
            if (!Array.isArray(pt) || pt.length < 2) return;
            const cx = Number(pt[0]);
            const cy = Number(pt[1]);
            if (!Number.isFinite(cx) || !Number.isFinite(cy)) return;
            const handle = document.createElementNS(NS, "circle");
            handle.setAttribute("class", "map-editor-handle");
            handle.setAttribute("cx", cx);
            handle.setAttribute("cy", cy);
            handle.setAttribute("r", 6);
            handle.dataset.handleIndex = String(idx);
            group.appendChild(handle);
        });
        svg.appendChild(group);
    }

    function render() {
        ensureLayers();
        if (window.CGMap) {
            window.CGMap.render(svg, model);
        }
        applySelection();
        renderHandles();
    }

    function svgPoint(evt) {
        const rect = overlay.getBoundingClientRect();
        const vb = overlay.viewBox.baseVal;
        const x = (evt.clientX - rect.left) * vb.width / rect.width + vb.x;
        const y = (evt.clientY - rect.top) * vb.height / rect.height + vb.y;
        return [Math.round(x), Math.round(y)];
    }

    function clearPreview() {
        if (previewEl && previewEl.parentNode) {
            previewEl.parentNode.removeChild(previewEl);
        }
        previewEl = null;
    }

    function updatePreview(points, cursor) {
        if (!previewEl) {
            previewEl = document.createElementNS(NS, "polyline");
            previewEl.setAttribute("class", "map-editor-preview");
            overlay.appendChild(previewEl);
        }
        const all = points.slice();
        if (cursor) all.push(cursor);
        previewEl.setAttribute("points", all.map((p) => p[0] + "," + p[1]).join(" "));
    }

    function updateRectPreview(start, end) {
        if (!previewEl) {
            previewEl = document.createElementNS(NS, "rect");
            previewEl.setAttribute("class", "map-editor-preview");
            overlay.appendChild(previewEl);
        }
        const x = Math.min(start[0], end[0]);
        const y = Math.min(start[1], end[1]);
        const w = Math.abs(end[0] - start[0]);
        const h = Math.abs(end[1] - start[1]);
        previewEl.setAttribute("x", x);
        previewEl.setAttribute("y", y);
        previewEl.setAttribute("width", w);
        previewEl.setAttribute("height", h);
    }

    function normalizePersonEntry(entry) {
        if (!entry) return null;
        if (typeof entry === "string") {
            const name = entry.trim();
            return name ? { id: 0, name: name } : null;
        }
        if (typeof entry === "object") {
            const id = Number(entry.id || entry.ID || 0);
            const name = String(entry.name || entry.Nom || entry.label || "").trim();
            if (!name && !id) return null;
            return {
                id: Number.isFinite(id) ? id : 0,
                name: name,
                municipi: entry.municipi || entry.Municipi || "",
                any: entry.any || entry.Any || "",
            };
        }
        return null;
    }

    function ensureItemFields(layer, item) {
        if (!item || typeof item !== "object") return;
        if (typeof item.label !== "string") item.label = item.label ? String(item.label) : "";
        if (typeof item.notes !== "string") item.notes = item.notes ? String(item.notes) : "";
        if (layer === "streets") {
            if (!item.kind) item.kind = "asfaltat";
        }
        if (layer === "rivers") {
            if (!item.kind) item.kind = "riu";
        }
        if (layer === "elements") {
            if (!item.kind) item.kind = "tree";
        }
        if (layer === "houses") {
            if (!Array.isArray(item.people)) item.people = [];
            item.people = item.people.map(normalizePersonEntry).filter(Boolean);
            if (item.location && typeof item.location === "object") {
                const x = Number(item.location.x);
                const y = Number(item.location.y);
                item.location = {
                    x: Number.isFinite(x) ? x : "",
                    y: Number.isFinite(y) ? y : "",
                };
            } else {
                item.location = { x: "", y: "" };
            }
        }
        if (layer === "houses" || layer === "streets") {
            if (!Array.isArray(item.historic_names)) item.historic_names = [];
        }
    }

    function getSelectedItem() {
        if (!selected || !model || !model.layers) return null;
        const layer = model.layers[selected.layer];
        if (!Array.isArray(layer)) return null;
        return layer[selected.index] || null;
    }

    function updateFieldVisibility(layer) {
        fieldBlocks.forEach((block) => {
            const allowed = (block.dataset.layer || "").split(" ").filter(Boolean);
            const show = layer && (allowed.length === 0 || allowed.includes(layer));
            block.hidden = !show;
            block.style.display = show ? "" : "none";
        });
    }

    function renderHistoricList(entries) {
        if (!historicList) return;
        historicList.innerHTML = "";
        const list = Array.isArray(entries) ? entries : [];
        list.forEach((entry, idx) => {
            const row = document.createElement("div");
            row.className = "map-editor-historic-item";

            const nameInput = document.createElement("input");
            nameInput.type = "text";
            nameInput.value = entry && entry.name ? entry.name : "";

            const periodInput = document.createElement("input");
            periodInput.type = "text";
            periodInput.value = entry && entry.period ? entry.period : "";

            const removeBtn = document.createElement("button");
            removeBtn.type = "button";
            removeBtn.className = "map-editor-historic-remove";
            removeBtn.textContent = "X";
            if (historicRemoveLabel) {
                removeBtn.setAttribute("aria-label", historicRemoveLabel);
                removeBtn.title = historicRemoveLabel;
            }

            nameInput.addEventListener("input", () => {
                updateSelected((item) => {
                    if (!Array.isArray(item.historic_names)) item.historic_names = [];
                    while (item.historic_names.length <= idx) {
                        item.historic_names.push({ name: "", period: "" });
                    }
                    item.historic_names[idx].name = nameInput.value;
                }, false);
            });

            periodInput.addEventListener("input", () => {
                updateSelected((item) => {
                    if (!Array.isArray(item.historic_names)) item.historic_names = [];
                    while (item.historic_names.length <= idx) {
                        item.historic_names.push({ name: "", period: "" });
                    }
                    item.historic_names[idx].period = periodInput.value;
                }, false);
            });

            removeBtn.addEventListener("click", () => {
                updateSelected((item) => {
                    if (!Array.isArray(item.historic_names)) return;
                    item.historic_names.splice(idx, 1);
                }, false);
                const item = getSelectedItem();
                renderHistoricList(item ? item.historic_names : []);
            });

            row.appendChild(nameInput);
            row.appendChild(periodInput);
            row.appendChild(removeBtn);
            historicList.appendChild(row);
        });
    }

    function buildPersonLabel(person) {
        if (!person) return "";
        const parts = [];
        if (person.name) parts.push(person.name);
        const meta = [];
        if (person.municipi) meta.push(person.municipi);
        if (person.any) meta.push(person.any);
        if (meta.length) parts.push("(" + meta.join(" · ") + ")");
        return parts.join(" ");
    }

    function renderPeopleList(entries) {
        if (!peopleList) return;
        peopleList.innerHTML = "";
        const list = Array.isArray(entries) ? entries : [];
        list.forEach((person, idx) => {
            const item = document.createElement("div");
            item.className = "map-editor-people-item";
            const label = document.createElement("span");
            label.textContent = buildPersonLabel(person) || person.name || "";
            const removeBtn = document.createElement("button");
            removeBtn.type = "button";
            removeBtn.textContent = "X";
            removeBtn.addEventListener("click", () => {
                updateSelected((sel) => {
                    if (!Array.isArray(sel.people)) return;
                    sel.people.splice(idx, 1);
                }, false);
                const current = getSelectedItem();
                renderPeopleList(current ? current.people : []);
            });
            item.appendChild(label);
            item.appendChild(removeBtn);
            peopleList.appendChild(item);
        });
    }

    function renderPeopleResults(results) {
        if (!peopleResults) return;
        peopleResults.innerHTML = "";
        const list = Array.isArray(results) ? results : [];
        if (!list.length) {
            if (personEmptyLabel) {
                const empty = document.createElement("p");
                empty.className = "muted";
                empty.textContent = personEmptyLabel;
                peopleResults.appendChild(empty);
            }
            return;
        }
        list.forEach((person) => {
            const entry = normalizePersonEntry(person);
            if (!entry) return;
            const item = document.createElement("div");
            item.className = "map-editor-people-result";
            const label = document.createElement("span");
            label.textContent = buildPersonLabel(entry);
            const addBtn = document.createElement("button");
            addBtn.type = "button";
            addBtn.textContent = personAddLabel || "+";
            addBtn.addEventListener("click", () => {
                updateSelected((sel) => {
                    if (!Array.isArray(sel.people)) sel.people = [];
                    const exists = sel.people.some((p) => {
                        const pid = Number(p.id || 0);
                        if (entry.id && pid) return pid === entry.id;
                        return String(p.name || "").toLowerCase() === String(entry.name || "").toLowerCase();
                    });
                    if (!exists) sel.people.push(entry);
                }, false);
                const current = getSelectedItem();
                renderPeopleList(current ? current.people : []);
            });
            item.appendChild(label);
            item.appendChild(addBtn);
            peopleResults.appendChild(item);
        });
    }

    function searchPeople(query) {
        if (!peopleResults) return;
        if (!personSearchEndpoint) {
            peopleResults.innerHTML = "";
            return;
        }
        if (!selected || selected.layer !== "houses") {
            peopleResults.innerHTML = "";
            return;
        }
        const clean = String(query || "").trim();
        if (!clean) {
            peopleResults.innerHTML = "";
            return;
        }
        const url = `${personSearchEndpoint}?q=${encodeURIComponent(clean)}`;
        fetch(url, { credentials: "same-origin" })
            .then((res) => {
                if (!res.ok) throw new Error("search");
                return res.json();
            })
            .then((payload) => {
                renderPeopleResults(Array.isArray(payload) ? payload : []);
            })
            .catch(() => {
                renderPeopleResults([]);
            });
    }

    function minPointsForLayer(layer) {
        if (layer === "houses" || layer === "bounds") return 3;
        if (layer === "streets" || layer === "rivers") return 2;
        return 0;
    }

    function renderPointsList(layer, points) {
        const pointsList = document.getElementById("mapEditorPoints");
        if (!pointsList) return;
        pointsList.innerHTML = "";
        if (!Array.isArray(points)) return;
        const minPoints = minPointsForLayer(layer);
        points.forEach((pt, idx) => {
            const row = document.createElement("div");
            row.className = "map-editor-point-item";

            const xInput = document.createElement("input");
            xInput.type = "number";
            xInput.step = "1";
            xInput.value = Array.isArray(pt) ? pt[0] : 0;

            const yInput = document.createElement("input");
            yInput.type = "number";
            yInput.step = "1";
            yInput.value = Array.isArray(pt) ? pt[1] : 0;

            const removeBtn = document.createElement("button");
            removeBtn.type = "button";
            removeBtn.className = "map-editor-point-remove";
            removeBtn.textContent = "X";
            if (points.length <= minPoints) {
                removeBtn.disabled = true;
            }

            xInput.addEventListener("input", () => {
                updateSelected((item) => {
                    if (!Array.isArray(item.points)) item.points = [];
                    while (item.points.length <= idx) {
                        item.points.push([0, 0]);
                    }
                    const val = Number(xInput.value);
                    item.points[idx][0] = Number.isFinite(val) ? Math.round(val) : 0;
                }, true);
            });

            yInput.addEventListener("input", () => {
                updateSelected((item) => {
                    if (!Array.isArray(item.points)) item.points = [];
                    while (item.points.length <= idx) {
                        item.points.push([0, 0]);
                    }
                    const val = Number(yInput.value);
                    item.points[idx][1] = Number.isFinite(val) ? Math.round(val) : 0;
                }, true);
            });

            removeBtn.addEventListener("click", () => {
                updateSelected((item) => {
                    if (!Array.isArray(item.points)) return;
                    if (item.points.length <= minPoints) return;
                    item.points.splice(idx, 1);
                }, true);
                const item = getSelectedItem();
                renderPointsList(layer, item ? item.points : []);
            });

            row.appendChild(xInput);
            row.appendChild(yInput);
            row.appendChild(removeBtn);
            pointsList.appendChild(row);
        });
    }

    function syncPanel(layer, item) {
        if (!formEl || !emptyState) return;
        formEl.hidden = false;
        emptyState.hidden = true;
        updateFieldVisibility(layer);

        if (labelInput) labelInput.value = item.label || "";
        if (notesInput) notesInput.value = item.notes || "";
        if (streetTypeSelect) streetTypeSelect.value = item.kind || "asfaltat";
        if (riverTypeSelect) riverTypeSelect.value = item.kind || "riu";
        if (elementTypeSelect) elementTypeSelect.value = item.kind || "tree";
        if (peopleSearchInput) peopleSearchInput.value = "";
        renderPeopleResults([]);
        renderPeopleList(item.people || []);
        if (houseCoordX) houseCoordX.value = item.location && item.location.x !== "" ? item.location.x : "";
        if (houseCoordY) houseCoordY.value = item.location && item.location.y !== "" ? item.location.y : "";
        renderHistoricList(item.historic_names || []);
        renderPointsList(layer, item.points || []);
    }

    function clearSelection() {
        selected = null;
        applySelection();
        if (svg) {
            const handles = svg.querySelector(".map-editor-handles");
            if (handles && handles.parentNode) {
                handles.parentNode.removeChild(handles);
            }
        }
        dragHandle = null;
        dragHandleActive = false;
        if (formEl) formEl.hidden = true;
        if (emptyState) emptyState.hidden = false;
        if (deleteBtn) deleteBtn.disabled = true;
        const pointsList = document.getElementById("mapEditorPoints");
        if (pointsList) pointsList.innerHTML = "";
        if (peopleResults) peopleResults.innerHTML = "";
        if (peopleList) peopleList.innerHTML = "";
        if (peopleSearchInput) peopleSearchInput.value = "";
    }

    function setSelection(layer, index, focusLabel) {
        if (!model || !model.layers || !Array.isArray(model.layers[layer])) return;
        if (index < 0 || index >= model.layers[layer].length) return;
        selected = { layer: layer, index: index };
        const item = model.layers[layer][index];
        ensureItemFields(layer, item);
        applySelection();
        syncPanel(layer, item);
        renderHandles();
        if (deleteBtn) deleteBtn.disabled = false;
        if (focusLabel && labelInput) {
            labelInput.focus();
            labelInput.select();
        }
    }

    function updateSelected(handler, rerender) {
        const item = getSelectedItem();
        if (!item) return;
        handler(item);
        if (rerender) render();
    }

    function addHouse(points) {
        const item = { points: points, label: "", notes: "", people: [], historic_names: [], location: { x: "", y: "" } };
        model.layers.houses.push(item);
        render();
        setSelection("houses", model.layers.houses.length - 1, true);
    }

    function addStreet(points) {
        const kind = streetTypeSelect ? streetTypeSelect.value : "asfaltat";
        const item = { points: points, label: "", kind: kind, notes: "", historic_names: [] };
        model.layers.streets.push(item);
        render();
        setSelection("streets", model.layers.streets.length - 1, true);
    }

    function addRiver(points) {
        const kind = riverTypeSelect ? riverTypeSelect.value : "riu";
        const item = { points: points, label: "", notes: "", kind: kind };
        model.layers.rivers.push(item);
        render();
        setSelection("rivers", model.layers.rivers.length - 1, true);
    }

    function addElement(point) {
        const kind = elementTypeSelect ? elementTypeSelect.value : "tree";
        const item = { x: point[0], y: point[1], label: "", kind: kind, notes: "" };
        model.layers.elements.push(item);
        render();
        setSelection("elements", model.layers.elements.length - 1, true);
    }

    function addToponym(point, kind) {
        const item = { x: point[0], y: point[1], label: "", kind: kind, notes: "" };
        model.layers.toponyms.push(item);
        render();
        setSelection("toponyms", model.layers.toponyms.length - 1, true);
    }

    function finishDrawing() {
        if (!drawing || !drawing.points || drawing.points.length === 0) return;
        if (drawing.tool === "house-poly") {
            if (drawing.points.length >= 3) addHouse(drawing.points);
        } else if (drawing.tool === "street") {
            if (drawing.points.length >= 2) addStreet(drawing.points);
        } else if (drawing.tool === "river") {
            if (drawing.points.length >= 2) addRiver(drawing.points);
        }
        drawing = null;
        clearPreview();
    }

    function cancelDrawing() {
        drawing = null;
        clearPreview();
    }

    function setTool(tool) {
        activeTool = tool;
        editor.setAttribute("data-active-tool", tool);
        toolButtons.forEach((btn) => {
            const isActive = btn.dataset.tool === tool;
            btn.classList.toggle("is-active", isActive);
            btn.setAttribute("aria-pressed", isActive ? "true" : "false");
        });
        if (overlay) overlay.style.pointerEvents = tool === "select" ? "none" : "auto";
        const label = editor.querySelector(`[data-tool="${tool}"]`)?.dataset.toolLabel || tool;
        if (baseHint) {
            setHint(label ? label + " · " + baseHint : baseHint);
        } else {
            setHint(label ? "Eina activa: " + label : "");
        }
        cancelDrawing();
        drag = null;
        dragHandle = null;
        dragHandleActive = false;
    }

    toolButtons.forEach((btn) => {
        btn.addEventListener("click", () => setTool(btn.dataset.tool || "select"));
    });

    if (svg) {
        svg.addEventListener("click", (evt) => {
            if (activeTool !== "select") return;
            if (dragHandleActive) {
                dragHandleActive = false;
                return;
            }
            const target = evt.target.closest("[data-layer-item]");
            if (!target) {
                clearSelection();
                return;
            }
            const layer = target.getAttribute("data-layer-item");
            const index = Number(target.getAttribute("data-index"));
            if (!layer || Number.isNaN(index)) return;
            setSelection(layer, index, false);
        });

        svg.addEventListener("mousedown", (evt) => {
            if (activeTool !== "select") return;
            const handle = evt.target.closest(".map-editor-handle");
            if (!handle) return;
            const idx = Number(handle.dataset.handleIndex);
            if (Number.isNaN(idx)) return;
            evt.preventDefault();
            evt.stopPropagation();
            dragHandleActive = true;
            dragHandle = { index: idx };
        });
    }

    document.addEventListener("mousemove", (evt) => {
        if (!dragHandle || !selected) return;
        const point = svgPoint(evt);
        updateSelected((item) => {
            if (!Array.isArray(item.points)) return;
            if (dragHandle.index < 0 || dragHandle.index >= item.points.length) return;
            item.points[dragHandle.index] = [point[0], point[1]];
        }, true);
    });

    document.addEventListener("mouseup", () => {
        if (!dragHandle) return;
        dragHandle = null;
        dragHandleActive = false;
    });

    overlay.addEventListener("mousedown", (evt) => {
        if (activeTool !== "house-rect") return;
        drag = { start: svgPoint(evt) };
    });

    overlay.addEventListener("mousemove", (evt) => {
        if (activeTool === "house-poly" || activeTool === "street" || activeTool === "river") {
            if (drawing && drawing.points) {
                updatePreview(drawing.points, svgPoint(evt));
            }
            return;
        }
        if (activeTool === "house-rect" && drag) {
            updateRectPreview(drag.start, svgPoint(evt));
        }
    });

    overlay.addEventListener("mouseup", (evt) => {
        if (activeTool !== "house-rect" || !drag) return;
        const end = svgPoint(evt);
        const start = drag.start;
        drag = null;
        clearPreview();
        const x0 = Math.min(start[0], end[0]);
        const y0 = Math.min(start[1], end[1]);
        const x1 = Math.max(start[0], end[0]);
        const y1 = Math.max(start[1], end[1]);
        const points = [
            [x0, y0],
            [x1, y0],
            [x1, y1],
            [x0, y1],
        ];
        addHouse(points);
    });

    overlay.addEventListener("click", (evt) => {
        const point = svgPoint(evt);
        if (activeTool === "house-poly" || activeTool === "street" || activeTool === "river") {
            if (!drawing) drawing = { tool: activeTool, points: [] };
            drawing.points.push(point);
            updatePreview(drawing.points);
            return;
        }
        if (activeTool === "element") {
            addElement(point);
            return;
        }
        if (activeTool === "text") {
            addToponym(point, "text");
            return;
        }
        if (activeTool === "marker") {
            addToponym(point, "marker");
        }
    });

    overlay.addEventListener("dblclick", (evt) => {
        evt.preventDefault();
        finishDrawing();
    });

    document.addEventListener("keydown", (evt) => {
        const tag = evt.target && evt.target.tagName ? evt.target.tagName.toLowerCase() : "";
        if (tag === "input" || tag === "textarea") return;
        if (drawing) {
            if (evt.key === "Escape") {
                evt.preventDefault();
                cancelDrawing();
            }
            if (evt.key === "Backspace" || (evt.ctrlKey && evt.key.toLowerCase() === "z")) {
                evt.preventDefault();
                drawing.points.pop();
                if (drawing.points.length === 0) {
                    cancelDrawing();
                } else {
                    updatePreview(drawing.points);
                }
            }
            if (evt.key === "Enter") {
                evt.preventDefault();
                finishDrawing();
            }
            return;
        }
        if (selected && (evt.key === "Delete" || evt.key === "Backspace")) {
            evt.preventDefault();
            if (deleteConfirm && !window.confirm(deleteConfirm)) return;
            const layer = model.layers[selected.layer];
            if (!Array.isArray(layer)) return;
            layer.splice(selected.index, 1);
            clearSelection();
            render();
        }
    });

    if (labelInput) {
        labelInput.addEventListener("input", () => {
            updateSelected((item) => {
                item.label = labelInput.value;
            }, true);
        });
    }

    if (notesInput) {
        notesInput.addEventListener("input", () => {
            updateSelected((item) => {
                item.notes = notesInput.value;
            }, false);
        });
    }

    if (peopleSearchInput) {
        peopleSearchInput.addEventListener("input", () => {
            if (peopleSearchTimer) {
                window.clearTimeout(peopleSearchTimer);
            }
            const value = peopleSearchInput.value;
            peopleSearchTimer = window.setTimeout(() => {
                searchPeople(value);
            }, 250);
        });
    }

    if (streetTypeSelect) {
        streetTypeSelect.addEventListener("change", () => {
            updateSelected((item) => {
                item.kind = streetTypeSelect.value;
            }, true);
        });
    }

    if (riverTypeSelect) {
        riverTypeSelect.addEventListener("change", () => {
            updateSelected((item) => {
                item.kind = riverTypeSelect.value;
            }, true);
        });
    }

    if (elementTypeSelect) {
        elementTypeSelect.addEventListener("change", () => {
            updateSelected((item) => {
                item.kind = elementTypeSelect.value;
            }, true);
        });
    }

    if (houseCoordX) {
        houseCoordX.addEventListener("input", () => {
            updateSelected((item) => {
                if (!item.location || typeof item.location !== "object") {
                    item.location = { x: "", y: "" };
                }
                const val = Number(houseCoordX.value);
                item.location.x = Number.isFinite(val) ? Math.round(val) : "";
            }, false);
        });
    }

    if (houseCoordY) {
        houseCoordY.addEventListener("input", () => {
            updateSelected((item) => {
                if (!item.location || typeof item.location !== "object") {
                    item.location = { x: "", y: "" };
                }
                const val = Number(houseCoordY.value);
                item.location.y = Number.isFinite(val) ? Math.round(val) : "";
            }, false);
        });
    }

    if (historicAddBtn) {
        historicAddBtn.addEventListener("click", () => {
            updateSelected((item) => {
                if (!Array.isArray(item.historic_names)) item.historic_names = [];
                item.historic_names.push({ name: "", period: "" });
            }, false);
            const item = getSelectedItem();
            renderHistoricList(item ? item.historic_names : []);
        });
    }

    if (deleteBtn) {
        deleteBtn.addEventListener("click", () => {
            if (!selected || !model || !model.layers) return;
            if (deleteConfirm && !window.confirm(deleteConfirm)) return;
            const layer = model.layers[selected.layer];
            if (!Array.isArray(layer)) return;
            layer.splice(selected.index, 1);
            clearSelection();
            render();
        });
    }

    function createDraft() {
        setStatus("Creant esborrany...");
        fetch("/api/mapes/" + mapId + "/draft", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ csrf_token: csrf }),
        })
            .then((res) => {
                if (!res.ok) throw new Error("draft");
                return res.json();
            })
            .then((payload) => {
                versionId = payload.version_id;
                return fetch("/api/mapes/versions/" + versionId);
            })
            .then((res) => {
                if (!res.ok) throw new Error("load");
                return res.json();
            })
            .then((payload) => {
                model = payload.data || {};
                lockVersion = payload.lock_version || 0;
                setStatus("");
                render();
            })
            .catch(() => {
                setStatus("No s'ha pogut crear l'esborrany.");
            });
    }

    function saveDraft() {
        if (!versionId || !model) {
            return Promise.reject(new Error("No hi ha res per desar."));
        }
        const changelog = changelogInput ? changelogInput.value : "";
        return fetch("/api/mapes/versions/" + versionId, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                lock_version: lockVersion,
                changelog: changelog,
                data: model,
                csrf_token: csrf,
            }),
        })
            .then(async (res) => {
                if (res.status === 409) throw new Error("conflict");
                if (!res.ok) {
                    const text = await res.text();
                    throw new Error(text || "Error desant.");
                }
                return res.json();
            })
            .then((payload) => {
                lockVersion = payload.lock_version || lockVersion;
                return payload;
            });
    }

    function saveDraftWithStatus() {
        setStatus("Desant...");
        saveDraft()
            .then(() => {
                setStatus("Desat.");
            })
            .catch((err) => {
                if (err.message === "conflict") {
                    setStatus("Conflicte. Recarrega i torna a desar.");
                    return;
                }
                setStatus(err && err.message ? err.message : "Error desant.");
            });
    }

    function submitDraft() {
        if (!versionId) return;
        setStatus("Desant...");
        saveDraft()
            .then(() => {
                setStatus("Enviant a moderacio...");
                return fetch("/api/mapes/versions/" + versionId + "/submit", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ csrf_token: csrf }),
                });
            })
            .then(async (res) => {
                if (!res.ok) {
                    const text = await res.text();
                    throw new Error(text || "Error enviant.");
                }
                return res.json();
            })
            .then(() => {
                setStatus("Enviat a moderacio.");
            })
            .catch((err) => {
                if (err.message === "conflict") {
                    setStatus("Conflicte. Recarrega i torna a desar.");
                    return;
                }
                setStatus(err && err.message ? err.message : "Error enviant.");
            });
    }

    if (saveBtn) saveBtn.addEventListener("click", saveDraftWithStatus);
    if (submitBtn) submitBtn.addEventListener("click", submitDraft);
    if (exportSvgBtn) exportSvgBtn.addEventListener("click", exportSvg);
    if (exportJsonBtn) exportJsonBtn.addEventListener("click", exportJson);
    if (importOpenBtn) importOpenBtn.addEventListener("click", openImportModal);
    if (importCloseBtn) importCloseBtn.addEventListener("click", closeImportModal);
    if (importCancelBtn) importCancelBtn.addEventListener("click", closeImportModal);
    if (importApplyBtn) importApplyBtn.addEventListener("click", applyImport);
    if (importFileInput) {
        importFileInput.addEventListener("change", () => {
            const file = importFileInput.files && importFileInput.files[0];
            if (!file) return;
            const reader = new FileReader();
            reader.onload = () => {
                const text = typeof reader.result === "string" ? reader.result : "";
                if (importInput) importInput.value = text;
                applyImportText(text);
            };
            reader.onerror = () => {
                setImportStatus(importErrorLabel);
            };
            reader.readAsText(file);
        });
    }
    if (importModal) {
        importModal.addEventListener("click", (event) => {
            if (event.target === importModal) {
                closeImportModal();
            }
        });
    }
    document.addEventListener("keydown", (event) => {
        if (event.key === "Escape" && importModal && importModal.classList.contains("is-open")) {
            closeImportModal();
        }
    });

    clearSelection();
    setTool("select");
    createDraft();
})();
