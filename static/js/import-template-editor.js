(() => {
  const editor = document.getElementById("templateEditor");
  if (!editor) return;
  const csrfToken = editor.dataset.csrf || "";

  let editorData = {};
  const dataEl = document.getElementById("import-template-editor-data");
  if (dataEl) {
    try {
      editorData = JSON.parse(dataEl.textContent || "{}");
    } catch (_) {
      editorData = {};
    }
  }

  const locale = editorData.locale || "ca";
  const labels = Object.assign({}, editorData.labels || {});
  labels.addColumn = editor.dataset.labelAddColumn || labels.addColumn || "Afegir columna";
  labels.removeColumn = editor.dataset.labelRemoveColumn || labels.removeColumn || "Eliminar";
  labels.addMapping = editor.dataset.labelAddMapping || labels.addMapping || "Afegir mapping";
  labels.removeMapping = editor.dataset.labelRemoveMapping || labels.removeMapping || "Eliminar mapping";
  labels.addTransform = editor.dataset.labelAddTransform || labels.addTransform || "Afegir transform";
  labels.removeTransform = editor.dataset.labelRemoveTransform || labels.removeTransform || "Eliminar transform";
  labels.condition = editor.dataset.labelCondition || labels.condition || "Condicio";
  labels.conditionElse = editor.dataset.labelConditionElse || labels.conditionElse || "Else";

  const helpLabels = editorData.helpLabels || {};
  const similarityLabels = editorData.similarityLabels || {};
  const initialModel = editorData.initialModel || null;
  const transformHelp = editorData.transformHelp || {};
  const previewEmpty = editorData.previewEmpty || "";

  const targetCatalogDefaults = {
    common: { label: "", fields: [] },
    baptisme: { label: "", fields: [] },
    obit: { label: "", fields: [] },
    matrimoni: { label: "", fields: [] },
    padro: { label: "", fields: [] },
  };
  const targetCatalog = Object.assign({}, targetCatalogDefaults, editorData.targetCatalog || {});

  const transformOptions = [
    { value: "trim", label: "trim" },
    { value: "lower", label: "lower" },
    { value: "strip_diacritics", label: "strip_diacritics" },
    { value: "normalize_cronologia", label: "normalize_cronologia" },
    { value: "parse_ddmmyyyy_to_iso", label: "parse_ddmmyyyy_to_iso" },
    { value: "parse_date_flexible_to_base_data_acte", label: "parse_date_flexible_to_base_data_acte" },
    { value: "parse_date_flexible_to_date_or_text_with_quality", label: "parse_date_flexible_to_date_or_text_with_quality" },
    { value: "parse_person_from_cognoms", label: "parse_person_from_cognoms" },
    { value: "parse_person_from_nom", label: "parse_person_from_nom" },
    { value: "parse_person_from_cognoms_marcmoia_v2", label: "parse_person_from_cognoms_marcmoia_v2" },
    { value: "parse_person_from_nom_marcmoia_v2", label: "parse_person_from_nom_marcmoia_v2" },
    { value: "parse_person_from_cognoms_marcmoia_v2_maternal_first", label: "parse_person_from_cognoms_marcmoia_v2_maternal_first" },
    { value: "parse_person_from_nom_marcmoia_v2_maternal_first", label: "parse_person_from_nom_marcmoia_v2_maternal_first" },
    { value: "split_couple_i", label: "split_couple_i" },
    { value: "set_default", label: "set_default" },
    { value: "map_values", label: "map_values" },
    { value: "regex_extract", label: "regex_extract" },
    { value: "parse_marriage_order_int_nullable", label: "parse_marriage_order_int_nullable" },
    { value: "strip_marriage_order_text", label: "strip_marriage_order_text" },
    { value: "extract_parenthetical_last", label: "extract_parenthetical_last" },
    { value: "extract_parenthetical_all", label: "extract_parenthetical_all" },
    { value: "strip_parentheticals", label: "strip_parentheticals" },
  ];

  const applyHelpLabels = (root) => {
    if (!root) return;
    root.querySelectorAll("[data-help-key]").forEach((button) => {
      const key = button.dataset.helpKey;
      if (helpLabels[key]) {
        button.setAttribute("aria-label", helpLabels[key]);
      }
    });
  };

  const defaultModel = () => {
    return {
      metadata: { version: 1, kind: "transcripcions_raw", locale: locale, record_type: "baptisme" },
      book_resolution: {
        mode: "llibre_id",
        column: "llibre_id",
        cronologia_normalize: false,
        ambiguity_policy: "fail",
        scope_filters: true,
      },
      mapping: { columns: [] },
      policies: {
        moderation_status: "pendent",
        dedup: { within_file: true, key_fields: [] },
        merge_existing: {
          mode: "none",
          principal_roles: ["batejat"],
          update_missing_only: true,
          add_missing_people: true,
          add_missing_attrs: true,
        },
      },
    };
  };

  const transformNeedsParam = (name) => {
    return name === "set_default" || name === "map_values" || name === "regex_extract";
  };

  const parseMapValuesParam = (paramText) => {
    const parts = paramText
      .split(";")
      .map((part) => part.trim())
      .filter(Boolean);
    for (let i = 0; i < parts.length; i++) {
      const part = parts[i];
      let sep = null;
      if (part.includes("=>")) sep = "=>";
      else if (part.includes("=")) sep = "=";
      else if (part.includes(":")) sep = ":";
      if (!sep) continue;
      const pieces = part.split(sep).map((p) => p.trim());
      if (pieces.length >= 2 && pieces[0] && pieces[1]) {
        return { from: pieces[0], to: pieces.slice(1).join(sep).trim() };
      }
    }
    return null;
  };

  const formatLabel = (template, args) => {
    let result = template;
    (args || []).forEach((value) => {
      result = result.replace("%s", value);
    });
    return result;
  };

  const buildTransformContext = (colIndex, targetValue, state) => {
    const col = state.mapping.columns[colIndex];
    const columnName = col && col.header ? col.header : labels.header + " " + (colIndex + 1);
    const targetLabel = resolveTargetLabel(targetValue);
    if (targetLabel) {
      return (
        labels.transformContextColumn +
        ": " +
        columnName +
        " " +
        labels.transformContextSeparator +
        " " +
        labels.transformContextTarget +
        ": " +
        targetLabel
      );
    }
    return (
      labels.transformContextColumn +
      ": " +
      columnName +
      " " +
      labels.transformContextSeparator +
      " " +
      labels.transformContextTarget +
      ": " +
      labels.transformContextUnknown
    );
  };

  const buildTransformHelpText = (name, colIndex, targetValue, state) => {
    if (!name) return labels.transformHelpEmpty;
    const help = transformHelp[name] || name;
    const context = buildTransformContext(colIndex, targetValue, state);
    return help + " " + context;
  };

  const buildTransformExampleText = (name, paramText) => {
    if (!paramText) return "";
    if (name === "set_default") {
      return formatLabel(labels.transformExampleSetDefault, [paramText]);
    }
    if (name === "map_values") {
      const pair = parseMapValuesParam(paramText);
      if (pair) {
        return formatLabel(labels.transformExampleMapValues, [pair.from, pair.to]);
      }
      return formatLabel(labels.transformExampleMapValues, [paramText, paramText]);
    }
    if (name === "regex_extract") {
      return formatLabel(labels.transformExampleRegexExtract, [paramText]);
    }
    return "";
  };

  const buildTransformParamHint = (name) => {
    if (name === "set_default") return labels.transformParamHintSetDefault;
    if (name === "map_values") return labels.transformParamHintMapValues;
    if (name === "regex_extract") return labels.transformParamHintRegexExtract;
    return labels.transformParamHintNone;
  };

  const normalizeModel = (model) => {
    const base = model && typeof model === "object" ? model : defaultModel();
    if (!base.metadata) base.metadata = { version: 1, kind: "transcripcions_raw", locale: locale };
    if (!base.metadata.version) base.metadata.version = 1;
    if (!base.metadata.kind) base.metadata.kind = "transcripcions_raw";
    if (!base.metadata.locale) base.metadata.locale = locale;
    if (!base.metadata.record_type) base.metadata.record_type = "generic";
    if (!base.book_resolution) base.book_resolution = defaultModel().book_resolution;
    if (!base.mapping) base.mapping = { columns: [] };
    if (!Array.isArray(base.mapping.columns)) base.mapping.columns = [];
    if (!base.policies) base.policies = defaultModel().policies;
    base.mapping.columns = base.mapping.columns.map(normalizeColumn);
    return base;
  };

  const resolveTargetLabel = (value) => {
    if (!value) return "";
    const groups = Object.keys(targetCatalog);
    for (let i = 0; i < groups.length; i++) {
      const group = targetCatalog[groups[i]];
      const fields = group && Array.isArray(group.fields) ? group.fields : [];
      for (let j = 0; j < fields.length; j++) {
        if (fields[j].value === value) return fields[j].label;
      }
    }
    return value;
  };

  const updateTransformDescriptionsForMap = (mapRow, colIndex, targetValue, state) => {
    if (!mapRow) return;
    const rows = mapRow.querySelectorAll(".transform-row");
    rows.forEach((trRow) => {
      const select = trRow.querySelector('select[data-field="transform-name"]');
      const input = trRow.querySelector('input[data-field="transform-value"]');
      const desc = trRow.querySelector(".transform-desc");
      const hint = trRow.querySelector(".transform-param-hint");
      const example = trRow.querySelector(".transform-example");
      const paramWrap = trRow.querySelector(".transform-param");
      const name = select ? select.value : "";
      const param = input ? input.value : "";
      const needsParam = transformNeedsParam(name);
      if (paramWrap) {
        if (needsParam) {
          paramWrap.removeAttribute("hidden");
        } else {
          paramWrap.setAttribute("hidden", "");
        }
      }
      if (input) {
        if (needsParam) {
          input.removeAttribute("disabled");
        } else {
          input.value = "";
          input.setAttribute("disabled", "");
        }
      }
      if (hint) {
        if (needsParam) {
          hint.textContent = buildTransformParamHint(name);
          hint.removeAttribute("hidden");
        } else {
          hint.textContent = "";
          hint.setAttribute("hidden", "");
        }
      }
      if (desc) {
        desc.textContent = buildTransformHelpText(name, colIndex, targetValue, state);
      }
      if (example) {
        const exampleText = buildTransformExampleText(name, param);
        example.textContent = exampleText;
        if (exampleText) {
          example.removeAttribute("hidden");
        } else {
          example.setAttribute("hidden", "");
        }
      }
    });
  };

  const normalizeColumn = (col) => {
    const item = col && typeof col === "object" ? col : {};
    const aliases = Array.isArray(item.aliases) ? item.aliases : [];
    const mapTo = Array.isArray(item.map_to) ? item.map_to : [];
    const condition = item.condition && typeof item.condition === "object" ? item.condition : null;
    return {
      header: item.header || "",
      aliases: aliases,
      required: !!item.required,
      map_to: mapTo.map(normalizeMapTo),
      condition: normalizeCondition(condition),
      condition_enabled: !!condition,
    };
  };

  const normalizeCondition = (condition) => {
    if (!condition) return null;
    const thenBlock = condition.then && typeof condition.then === "object" ? condition.then : {};
    const elseBlock = condition.else && typeof condition.else === "object" ? condition.else : null;
    return {
      expr: condition.expr || "",
      then: {
        map_to: Array.isArray(thenBlock.map_to) ? thenBlock.map_to.map(normalizeMapTo) : [normalizeMapTo({})],
        transforms: Array.isArray(thenBlock.transforms) ? thenBlock.transforms.map(normalizeTransform) : [],
      },
      else: elseBlock
        ? {
            map_to: Array.isArray(elseBlock.map_to) ? elseBlock.map_to.map(normalizeMapTo) : [normalizeMapTo({})],
            transforms: Array.isArray(elseBlock.transforms) ? elseBlock.transforms.map(normalizeTransform) : [],
          }
        : null,
    };
  };

  const normalizeMapTo = (item) => {
    const m = item && typeof item === "object" ? item : {};
    return {
      target: m.target || "",
      transforms: Array.isArray(m.transforms) ? m.transforms.map(normalizeTransform) : [],
    };
  };

  const normalizeTransform = (t) => {
    if (!t) return { name: "", value: "" };
    if (typeof t === "string") return { name: t, value: "" };
    return { name: t.name || "", value: t.value || t.arg || "" };
  };

  const currentRecordType = (state) => {
    const val = state && state.metadata ? state.metadata.record_type : "";
    return (val || "generic").toLowerCase();
  };

  const buildTargetOptions = (state, currentValue) => {
    const recordType = currentRecordType(state);
    const groups = [targetCatalog.common];
    if (recordType === "generic") {
      groups.push(targetCatalog.baptisme, targetCatalog.obit, targetCatalog.matrimoni);
    } else if (targetCatalog[recordType]) {
      groups.push(targetCatalog[recordType]);
    }
    const known = new Set();
    let options = `<option value="">${escapeHTML(labels.targetPlaceholder)}</option>`;
    groups.forEach((group) => {
      if (!group || !group.fields || !group.fields.length) return;
      options += `<optgroup label="${escapeHTML(group.label || "")}">`;
      group.fields.forEach((field) => {
        const value = field.value || "";
        known.add(value);
        options += `<option value="${escapeHTML(value)}">${escapeHTML(field.label || value)}</option>`;
      });
      options += `</optgroup>`;
    });
    if (currentValue && !known.has(currentValue)) {
      const customLabel = labels.targetCustom ? labels.targetCustom + " " + currentValue : currentValue;
      options = `<option value="${escapeHTML(currentValue)}">${escapeHTML(customLabel)}</option>` + options;
    }
    return options;
  };

  const state = normalizeModel(initialModel);

  const columnsList = document.getElementById("columnsList");
  const addColumnButton = document.getElementById("addColumnButton");
  const modelInput = document.getElementById("templateModelJSON");
  const previewTable = document.getElementById("previewTable");
  const previewSamples = document.getElementById("previewSamples");
  const similarBox = document.getElementById("similarTemplates");
  const similarHint = document.getElementById("similarHint");
  let similarTimer = null;

  const createEmptyColumn = () => {
    return {
      header: "",
      aliases: [],
      required: false,
      map_to: [normalizeMapTo({})],
      condition: null,
      condition_enabled: false,
    };
  };

  const renderColumns = () => {
    columnsList.innerHTML = "";
    state.mapping.columns.forEach((col, idx) => {
      const card = document.createElement("div");
      card.className = "column-card";
      card.dataset.col = String(idx);
      card.innerHTML = `
                    <div class="column-header">
                        <h3>${labels.header} #${idx + 1}</h3>
                        <button type="button" class="boto-secundari btn-mini" data-action="remove-column">${labels.removeColumn}</button>
                    </div>
                    <div class="column-grid">
                        <div class="template-field">
                            <label>${labels.header} <button type="button" class="boto-icona ajuda-boto" aria-haspopup="dialog" aria-controls="ajuda-col-header" data-help-key="header">?</button></label>
                            <input type="text" data-field="header">
                        </div>
                        <div class="template-field">
                            <label>${labels.aliases} <button type="button" class="boto-icona ajuda-boto" aria-haspopup="dialog" aria-controls="ajuda-col-aliases" data-help-key="aliases">?</button></label>
                            <input type="text" data-field="aliases" placeholder="alias1, alias2">
                        </div>
                        <div class="template-field inline-field">
                            <label>
                                <input type="checkbox" data-field="required">
                                ${labels.required}
                            </label>
                            <button type="button" class="boto-icona ajuda-boto" aria-haspopup="dialog" aria-controls="ajuda-col-required" data-help-key="required">?</button>
                        </div>
                    </div>
                    <div class="map-to-block">
                        <div class="map-to-header">
                            <h4>${labels.mapping}</h4>
                            <button type="button" class="boto-secundari btn-mini" data-action="add-map">${labels.addMapping}</button>
                        </div>
                        <div class="map-to-list"></div>
                    </div>
                    <div class="condition-block">
                        <div class="condition-header">
                            <label class="inline-field">
                                <input type="checkbox" data-field="condition_enabled">
                                ${labels.condition}
                            </label>
                            <button type="button" class="boto-icona ajuda-boto" aria-haspopup="dialog" aria-controls="ajuda-col-condition" data-help-key="condition">?</button>
                        </div>
                        <div class="condition-body" ${col.condition_enabled ? "" : "hidden"}>
                            <div class="template-field">
                                <label>${labels.conditionExpr} <button type="button" class="boto-icona ajuda-boto" aria-haspopup="dialog" aria-controls="ajuda-col-condition-expr" data-help-key="conditionExpr">?</button></label>
                                <input type="text" data-field="condition.expr" placeholder="ex: value == 'X'">
                            </div>
                            <div class="condition-section">
                                <h5>IF</h5>
                                <div class="condition-map-list" data-branch="then"></div>
                                <button type="button" class="boto-secundari btn-mini" data-action="add-cond-map" data-branch="then">${labels.addMapping}</button>
                            </div>
                            <div class="condition-section">
                                <div class="condition-header">
                                    <label class="inline-field">
                                        <input type="checkbox" data-field="condition_else_enabled">
                                        ${labels.conditionElse}
                                    </label>
                                    <button type="button" class="boto-icona ajuda-boto" aria-haspopup="dialog" aria-controls="ajuda-col-condition-else" data-help-key="conditionElse">?</button>
                                </div>
                                <div class="condition-else" ${col.condition && col.condition.else ? "" : "hidden"}>
                                    <div class="condition-map-list" data-branch="else"></div>
                                    <button type="button" class="boto-secundari btn-mini" data-action="add-cond-map" data-branch="else">${labels.addMapping}</button>
                                </div>
                            </div>
                        </div>
                    </div>
                `;
      applyHelpLabels(card);
      const headerInput = card.querySelector('input[data-field="header"]');
      const aliasInput = card.querySelector('input[data-field="aliases"]');
      const requiredInput = card.querySelector('input[data-field="required"]');
      const conditionToggle = card.querySelector('input[data-field="condition_enabled"]');
      headerInput.value = col.header || "";
      aliasInput.value = (col.aliases || []).join(", ");
      requiredInput.checked = !!col.required;
      conditionToggle.checked = !!col.condition_enabled;
      const mapList = card.querySelector(".map-to-list");
      renderMapList(mapList, col.map_to, idx, null);
      const mapBlock = card.querySelector(".map-to-block");
      if (mapBlock) {
        if (col.condition_enabled) {
          mapBlock.setAttribute("hidden", "");
        } else {
          mapBlock.removeAttribute("hidden");
        }
      }
      if (col.condition_enabled && col.condition) {
        const thenList = card.querySelector('.condition-map-list[data-branch="then"]');
        renderMapList(thenList, col.condition.then.map_to, idx, "then");
        const elseList = card.querySelector('.condition-map-list[data-branch="else"]');
        if (col.condition.else) {
          renderMapList(elseList, col.condition.else.map_to, idx, "else");
        }
        const exprInput = card.querySelector('input[data-field="condition.expr"]');
        exprInput.value = col.condition.expr || "";
        const elseToggle = card.querySelector('input[data-field="condition_else_enabled"]');
        elseToggle.checked = !!col.condition.else;
      }
      columnsList.appendChild(card);
    });
  };

  const renderMapList = (container, maps, colIndex, branch) => {
    if (!container) return;
    container.innerHTML = "";
    (maps || []).forEach((mapItem, mapIndex) => {
      const row = document.createElement("div");
      row.className = "map-row";
      row.dataset.map = String(mapIndex);
      row.innerHTML = `
                    <div class="map-row-main">
                        <div class="template-field">
                            <label>${labels.target} <button type="button" class="boto-icona ajuda-boto" aria-haspopup="dialog" aria-controls="ajuda-col-target" data-help-key="target">?</button></label>
                            <select data-field="target">
                                ${buildTargetOptions(state, mapItem.target)}
                            </select>
                        </div>
                        <div class="transforms-block">
                            <div class="transforms-header">
                                <div class="transforms-title">
                                    <span>${labels.transforms}</span>
                                    <button type="button" class="boto-icona ajuda-boto" aria-haspopup="dialog" aria-controls="ajuda-col-transforms" data-help-key="transforms">?</button>
                                </div>
                            </div>
                            <div class="transforms-actions">
                                <button type="button" class="boto-secundari btn-mini" data-action="add-transform" data-map="${mapIndex}">${labels.addTransform}</button>
                            </div>
                            <div class="transforms-list"></div>
                        </div>
                    </div>
                    <div class="map-row-actions">
                        <button type="button" class="boto-secundari btn-mini" data-action="remove-map" data-map="${mapIndex}">${labels.removeMapping}</button>
                    </div>
                `;
      applyHelpLabels(row);
      const targetSelect = row.querySelector('select[data-field="target"]');
      targetSelect.value = mapItem.target || "";
      targetSelect.dataset.col = String(colIndex);
      targetSelect.dataset.map = String(mapIndex);
      if (branch) targetSelect.dataset.branch = branch;
      const tList = row.querySelector(".transforms-list");
      renderTransformList(tList, mapItem.transforms, colIndex, mapIndex, branch, mapItem.target);
      const removeBtn = row.querySelector('[data-action="remove-map"]');
      removeBtn.dataset.col = String(colIndex);
      if (branch) removeBtn.dataset.branch = branch;
      const addBtn = row.querySelector('[data-action="add-transform"]');
      addBtn.dataset.col = String(colIndex);
      if (branch) addBtn.dataset.branch = branch;
      container.appendChild(row);
    });
  };

  const renderTransformList = (container, transforms, colIndex, mapIndex, branch, targetValue) => {
    if (!container) return;
    container.innerHTML = "";
    (transforms || []).forEach((tr, idx) => {
      const row = document.createElement("div");
      row.className = "transform-row";
      row.dataset.transform = String(idx);
      const options = transformOptions
        .map((opt) => {
          return `<option value="${opt.value}">${opt.label}</option>`;
        })
        .join("");
      const helpText = buildTransformHelpText(tr.name || "", colIndex, targetValue, state);
      const needsParam = transformNeedsParam(tr.name || "");
      const hintText = needsParam ? buildTransformParamHint(tr.name || "") : "";
      const exampleText = buildTransformExampleText(tr.name || "", tr.value || "");
      row.innerHTML = `
                    <div class="transform-row-fields">
                        <select data-field="transform-name">${options}</select>
                        <div class="transform-param" ${needsParam ? "" : "hidden"}>
                            <input type="text" data-field="transform-value" placeholder="parametre (opcional)" ${needsParam ? "" : "disabled"}>
                        </div>
                        <button type="button" class="boto-secundari btn-mini" data-action="remove-transform">${labels.removeTransform}</button>
                    </div>
                    <div class="transform-meta">
                        <div class="transform-param-hint muted" ${needsParam ? "" : "hidden"}>${escapeHTML(hintText)}</div>
                        <div class="transform-desc muted">${escapeHTML(helpText)}</div>
                        <div class="transform-example muted" ${exampleText ? "" : "hidden"}>${escapeHTML(exampleText)}</div>
                    </div>
                `;
      const select = row.querySelector('select[data-field="transform-name"]');
      const input = row.querySelector('input[data-field="transform-value"]');
      select.value = tr.name || "";
      input.value = tr.value || "";
      select.dataset.col = String(colIndex);
      select.dataset.map = String(mapIndex);
      select.dataset.transform = String(idx);
      input.dataset.col = String(colIndex);
      input.dataset.map = String(mapIndex);
      input.dataset.transform = String(idx);
      if (branch) {
        select.dataset.branch = branch;
        input.dataset.branch = branch;
      }
      const removeBtn = row.querySelector('[data-action="remove-transform"]');
      removeBtn.dataset.col = String(colIndex);
      removeBtn.dataset.map = String(mapIndex);
      removeBtn.dataset.transform = String(idx);
      if (branch) removeBtn.dataset.branch = branch;
      container.appendChild(row);
    });
  };

  const updateConfigFromInputs = () => {
    const fields = document.querySelectorAll('[data-field^="metadata"], [data-field^="book_resolution"], [data-field^="policies"]');
    fields.forEach((field) => {
      const path = field.dataset.field || "";
      if (!path) return;
      const parts = path.split(".");
      let target = state;
      for (let i = 0; i < parts.length - 1; i++) {
        if (!target[parts[i]]) target[parts[i]] = {};
        target = target[parts[i]];
      }
      const key = parts[parts.length - 1];
      if (field.type === "checkbox") {
        target[key] = field.checked;
      } else if (key === "key_fields" || key === "principal_roles") {
        target[key] = field.value
          .split(",")
          .map((v) => v.trim())
          .filter(Boolean);
      } else {
        target[key] = field.value;
      }
    });
  };

  const applyStateToInputs = () => {
    setFieldValue("metadata.locale", state.metadata.locale || "");
    setFieldValue("metadata.record_type", state.metadata.record_type || "generic");
    setFieldValue("book_resolution.mode", state.book_resolution.mode || "llibre_id");
    setFieldValue("book_resolution.column", state.book_resolution.column || "llibre_id");
    setCheckbox("book_resolution.cronologia_normalize", !!state.book_resolution.cronologia_normalize);
    setFieldValue("book_resolution.ambiguity_policy", state.book_resolution.ambiguity_policy || "fail");
    setCheckbox("book_resolution.scope_filters", !!state.book_resolution.scope_filters);
    setCheckbox("policies.dedup.within_file", !!(state.policies.dedup && state.policies.dedup.within_file));
    setFieldValue("policies.dedup.key_fields", (state.policies.dedup && state.policies.dedup.key_fields || []).join(", "));
    setFieldValue("policies.merge_existing.mode", (state.policies.merge_existing && state.policies.merge_existing.mode) || "none");
    setFieldValue(
      "policies.merge_existing.principal_roles",
      (state.policies.merge_existing && state.policies.merge_existing.principal_roles || []).join(", ")
    );
    setCheckbox("policies.merge_existing.update_missing_only", !!(state.policies.merge_existing && state.policies.merge_existing.update_missing_only));
    setCheckbox("policies.merge_existing.add_missing_people", !!(state.policies.merge_existing && state.policies.merge_existing.add_missing_people));
    setCheckbox("policies.merge_existing.add_missing_attrs", !!(state.policies.merge_existing && state.policies.merge_existing.add_missing_attrs));
  };

  const setFieldValue = (fieldName, value) => {
    const field = document.querySelector('[data-field="' + fieldName + '"]');
    if (field) field.value = value || "";
  };

  const setCheckbox = (fieldName, value) => {
    const field = document.querySelector('[data-field="' + fieldName + '"]');
    if (field) field.checked = !!value;
  };

  const buildOutputModel = () => {
    updateConfigFromInputs();
    const model = JSON.parse(JSON.stringify(state));
    const columns = state.mapping.columns.map((col) => {
      const output = {
        header: col.header || "",
        aliases: (col.aliases || []).filter(Boolean),
        required: !!col.required,
      };
      if (col.condition_enabled && col.condition) {
        output.condition = {
          expr: col.condition.expr || "",
          then: {
            map_to: cleanMapTo(col.condition.then.map_to),
            transforms: cleanTransforms(col.condition.then.transforms),
          },
        };
        if (col.condition.else) {
          output.condition.else = {
            map_to: cleanMapTo(col.condition.else.map_to),
            transforms: cleanTransforms(col.condition.else.transforms),
          };
        }
      } else {
        output.map_to = cleanMapTo(col.map_to);
      }
      return output;
    });
    model.mapping.columns = columns;
    return model;
  };

  const cleanMapTo = (list) => {
    return (list || [])
      .map((item) => {
        return {
          target: item.target || "",
          transforms: cleanTransforms(item.transforms),
        };
      })
      .filter((item) => item.target || (item.transforms && item.transforms.length));
  };

  const cleanTransforms = (list) => {
    return (list || [])
      .map((t) => {
        if (!t || !t.name) return null;
        if (t.value) return { name: t.name, value: t.value };
        return { name: t.name };
      })
      .filter(Boolean);
  };

  const updateModelJSON = () => {
    const model = buildOutputModel();
    const json = JSON.stringify(model, null, 2);
    modelInput.value = json;
    scheduleSimilarTemplates(json);
  };

  const buildSampleRows = () => {
    const hasCondition = state.mapping.columns.some((c) => c.condition_enabled && c.condition);
    const rowsCount = hasCondition ? 3 : 2;
    const rows = [];
    for (let i = 0; i < rowsCount; i++) {
      rows.push([]);
    }
    state.mapping.columns.forEach((col) => {
      const baseSample = sampleForColumn(col, 0);
      if (col.condition_enabled && col.condition) {
        rows[0].push(sampleForConditionBranch(col.condition.then, 0));
        rows[1].push(col.condition.else ? sampleForConditionBranch(col.condition.else, 1) : baseSample);
        if (rowsCount > 2) rows[2].push(baseSample);
      } else {
        for (let i = 0; i < rowsCount; i++) {
          rows[i].push(sampleForColumn(col, i));
        }
      }
    });
    return rows;
  };

  const sampleForConditionBranch = (branch, idx) => {
    if (!branch) return "Exemple";
    const maps = branch.map_to || [];
    const transforms = maps.length ? maps[0].transforms || [] : branch.transforms || [];
    return sampleFromTransforms(transforms, idx);
  };

  const sampleForColumn = (col, idx) => {
    const maps = col.map_to || [];
    const transforms = maps.length ? maps[0].transforms || [] : [];
    const target = maps.length ? maps[0].target : "";
    return sampleFromTransforms(transforms, idx, target, col.header);
  };

  const sampleFromTransforms = (transforms, idx, target, header) => {
    const names = (transforms || []).map((t) => t.name || t);
    if (
      names.indexOf("parse_ddmmyyyy_to_iso") !== -1 ||
      names.indexOf("parse_date_flexible_to_base_data_acte") !== -1 ||
      names.indexOf("parse_date_flexible_to_date_or_text_with_quality") !== -1
    ) {
      if (idx % 3 === 1) return "??/??/1890";
      if (idx % 3 === 2) return "¿12/03/1890";
      return "12/03/1890";
    }
    if (
      names.indexOf("parse_person_from_cognoms") !== -1 ||
      names.indexOf("parse_person_from_nom") !== -1 ||
      names.indexOf("parse_person_from_cognoms_marcmoia_v2") !== -1 ||
      names.indexOf("parse_person_from_nom_marcmoia_v2") !== -1 ||
      names.indexOf("parse_person_from_cognoms_marcmoia_v2_maternal_first") !== -1 ||
      names.indexOf("parse_person_from_nom_marcmoia_v2_maternal_first") !== -1
    ) {
      if (idx % 2 === 1) return "¿Maria Puig (Valls)";
      return "Puig i Ferrer (Valls)";
    }
    if (names.indexOf("split_couple_i") !== -1) {
      return "Joan X i Maria Y";
    }
    if (names.indexOf("normalize_cronologia") !== -1) {
      return "1890-1891";
    }
    if (target && target.indexOf("llibre_id") !== -1) {
      return String(120 + idx);
    }
    if (target && target.indexOf("tipus_acte") !== -1) {
      return "baptisme";
    }
    if (target && target.indexOf("data") !== -1) {
      return "01/01/1890";
    }
    if (target && target.indexOf("cognom") !== -1) {
      return "Puig";
    }
    if (target && target.indexOf("nom") !== -1) {
      return "Joan";
    }
    if (header && header.toLowerCase().indexOf("data") !== -1) {
      return "01/01/1890";
    }
    return "Exemple " + (idx + 1);
  };

  const updatePreview = () => {
    const headers = state.mapping.columns.map((c) => c.header || "");
    previewTable.innerHTML = buildTable(headers, []);
    const rows = buildSampleRows();
    previewSamples.innerHTML = buildTable(headers, rows);
  };

  const scheduleSimilarTemplates = (modelJSON) => {
    if (!similarBox) return;
    if (similarTimer) clearTimeout(similarTimer);
    similarTimer = setTimeout(() => {
      fetchSimilarTemplates(modelJSON);
    }, 450);
  };

  const fetchSimilarTemplates = (modelJSON) => {
    if (!similarBox) return;
    if (similarHint) {
      similarHint.textContent = similarityLabels.loading;
    }
    fetch("/api/import-templates/similar", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRF-Token": csrfToken,
      },
      body: JSON.stringify({ model_json: modelJSON, limit: 8 }),
    })
      .then((res) => {
        if (!res.ok) throw new Error("failed");
        return res.json();
      })
      .then((payload) => {
        renderSimilarTemplates(payload.items || []);
      })
      .catch(() => {
        if (similarHint) {
          similarHint.textContent = similarityLabels.empty;
        }
        similarBox.innerHTML = "";
      });
  };

  const renderSimilarTemplates = (items) => {
    if (!similarBox) return;
    similarBox.innerHTML = "";
    if (!items.length) {
      if (similarHint) similarHint.textContent = similarityLabels.empty;
      return;
    }
    if (similarHint) similarHint.textContent = "";
    items.forEach((item) => {
      const row = document.createElement("div");
      row.className = "similar-item";
      const score = (item.score * 100).toFixed(0) + "%";
      row.innerHTML = `
                    <div class="similar-main">
                        <div class="similar-title">${escapeHTML(item.name || "")}</div>
                    </div>
                    <div class="similar-meta">${score}</div>
                    <div class="similar-actions"></div>
                `;
      const actions = row.querySelector(".similar-actions");
      if (item.can_edit) {
        const link = document.createElement("a");
        link.href = "/importador/plantilles/" + item.id + "/editar";
        link.className = "boto-secundari btn-mini";
        link.textContent = similarityLabels.open;
        actions.appendChild(link);
      }
      if (item.can_clone) {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "boto-secundari btn-mini";
        btn.textContent = similarityLabels.clone;
        btn.addEventListener("click", () => {
          cloneSimilarTemplate(item.id, btn);
        });
        actions.appendChild(btn);
      }
      similarBox.appendChild(row);
    });
  };

  const cloneSimilarTemplate = (id, button) => {
    if (!id) return;
    if (button) button.disabled = true;
    fetch("/api/import-templates/" + id + "/clone", {
      method: "POST",
      headers: { "X-CSRF-Token": csrfToken },
    })
      .then((res) => {
        if (!res.ok) throw new Error("failed");
        return res.json();
      })
      .then((payload) => {
        if (payload && payload.id) {
          window.location.assign("/importador/plantilles/" + payload.id + "/editar");
        }
      })
      .catch(() => {
        if (button) button.disabled = false;
      });
  };

  const buildTable = (headers, rows) => {
    if (!headers.length) {
      return `<div class="muted">${escapeHTML(previewEmpty)}</div>`;
    }
    let html = '<table class="taula preview-table"><thead><tr>';
    headers.forEach((h) => {
      html += "<th>" + escapeHTML(h || "-") + "</th>";
    });
    html += "</tr></thead>";
    if (rows.length) {
      html += "<tbody>";
      rows.forEach((row) => {
        html += "<tr>";
        row.forEach((cell) => {
          html += "<td>" + escapeHTML(cell || "") + "</td>";
        });
        html += "</tr>";
      });
      html += "</tbody>";
    }
    html += "</table>";
    return html;
  };

  const escapeHTML = (value) => {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  };

  const resolveMapList = (col, branch) => {
    if (!branch) return col.map_to;
    if (!col.condition) return null;
    if (branch === "then") return col.condition.then.map_to;
    if (branch === "else" && col.condition.else) return col.condition.else.map_to;
    return null;
  };

  columnsList.addEventListener("input", (e) => {
    const target = e.target;
    const card = target.closest(".column-card");
    const colIndex = card ? parseInt(card.dataset.col, 10) : NaN;
    if (isNaN(colIndex)) return;
    const col = state.mapping.columns[colIndex];
    if (!col) return;
    if (target.dataset.field === "header") {
      col.header = target.value;
    } else if (target.dataset.field === "aliases") {
      col.aliases = target.value
        .split(",")
        .map((v) => v.trim())
        .filter(Boolean);
    } else if (target.dataset.field === "required") {
      col.required = target.checked;
    } else if (target.dataset.field === "condition.expr" && col.condition) {
      col.condition.expr = target.value;
    } else if (target.dataset.field === "target") {
      const mapIndex = parseInt(target.dataset.map, 10);
      const branch = target.dataset.branch || "";
      const mapList = resolveMapList(col, branch);
      if (mapList && mapList[mapIndex]) {
        mapList[mapIndex].target = target.value;
        const mapRow = target.closest(".map-row");
        updateTransformDescriptionsForMap(mapRow, colIndex, target.value, state);
      }
    } else if (target.dataset.field === "transform-name" || target.dataset.field === "transform-value") {
      const mapIndex = parseInt(target.dataset.map, 10);
      const trIndex = parseInt(target.dataset.transform, 10);
      const branch = target.dataset.branch || "";
      const mapList = resolveMapList(col, branch);
      if (mapList && mapList[mapIndex]) {
        const transforms = mapList[mapIndex].transforms || [];
        const tr = transforms[trIndex];
        if (tr) {
          if (target.dataset.field === "transform-name") {
            tr.name = target.value;
            const row = target.closest(".map-row");
            updateTransformDescriptionsForMap(row, colIndex, mapList[mapIndex].target, state);
          } else {
            tr.value = target.value;
            const row = target.closest(".map-row");
            updateTransformDescriptionsForMap(row, colIndex, mapList[mapIndex].target, state);
          }
        }
      }
    }
    updateModelJSON();
    updatePreview();
  });

  columnsList.addEventListener("change", (e) => {
    const target = e.target;
    const card = target.closest(".column-card");
    const colIndex = card ? parseInt(card.dataset.col, 10) : NaN;
    if (isNaN(colIndex)) return;
    const col = state.mapping.columns[colIndex];
    if (!col) return;
    if (target.dataset.field === "target") {
      const mapIndex = parseInt(target.dataset.map, 10);
      const branch = target.dataset.branch || "";
      const mapList = resolveMapList(col, branch);
      if (mapList && mapList[mapIndex]) {
        mapList[mapIndex].target = target.value;
        const mapRow = target.closest(".map-row");
        updateTransformDescriptionsForMap(mapRow, colIndex, target.value, state);
      }
      updateModelJSON();
      updatePreview();
      return;
    }
    if (target.dataset.field === "condition_enabled") {
      col.condition_enabled = target.checked;
      if (col.condition_enabled && !col.condition) {
        col.condition = normalizeCondition({
          expr: "",
          then: { map_to: [normalizeMapTo({})], transforms: [] },
        });
      }
      renderColumns();
    }
    if (target.dataset.field === "condition_else_enabled" && col.condition) {
      if (target.checked) {
        col.condition.else = { map_to: [normalizeMapTo({})], transforms: [] };
      } else {
        col.condition.else = null;
      }
      renderColumns();
    }
    updateModelJSON();
    updatePreview();
  });

  columnsList.addEventListener("click", (e) => {
    const button = e.target.closest("button");
    if (!button) return;
    const action = button.dataset.action;
    const card = button.closest(".column-card");
    const colIndex = card ? parseInt(card.dataset.col, 10) : NaN;
    if (action === "remove-column") {
      if (!isNaN(colIndex)) {
        state.mapping.columns.splice(colIndex, 1);
        renderColumns();
        updateModelJSON();
        updatePreview();
      }
      return;
    }
    if (action === "add-map") {
      if (!isNaN(colIndex)) {
        const col = state.mapping.columns[colIndex];
        col.map_to.push(normalizeMapTo({}));
        renderColumns();
        updateModelJSON();
        updatePreview();
      }
      return;
    }
    if (action === "remove-map") {
      if (!isNaN(colIndex)) {
        const mapIndex = parseInt(button.dataset.map, 10);
        const branch = button.dataset.branch || "";
        const col = state.mapping.columns[colIndex];
        const mapList = resolveMapList(col, branch);
        if (mapList && mapIndex >= 0) {
          mapList.splice(mapIndex, 1);
          renderColumns();
          updateModelJSON();
          updatePreview();
        }
      }
      return;
    }
    if (action === "add-transform") {
      if (!isNaN(colIndex)) {
        const mapIndex = parseInt(button.dataset.map, 10);
        const branch = button.dataset.branch || "";
        const col = state.mapping.columns[colIndex];
        const mapList = resolveMapList(col, branch);
        if (mapList && mapList[mapIndex]) {
          mapList[mapIndex].transforms.push({ name: "", value: "" });
          renderColumns();
          updateModelJSON();
          updatePreview();
        }
      }
      return;
    }
    if (action === "remove-transform") {
      if (!isNaN(colIndex)) {
        const mapIndex = parseInt(button.dataset.map, 10);
        const trIndex = parseInt(button.dataset.transform, 10);
        const branch = button.dataset.branch || "";
        const col = state.mapping.columns[colIndex];
        const mapList = resolveMapList(col, branch);
        if (mapList && mapList[mapIndex] && trIndex >= 0) {
          mapList[mapIndex].transforms.splice(trIndex, 1);
          renderColumns();
          updateModelJSON();
          updatePreview();
        }
      }
      return;
    }
    if (action === "add-cond-map") {
      if (!isNaN(colIndex)) {
        const branch = button.dataset.branch || "then";
        const col = state.mapping.columns[colIndex];
        if (!col.condition) {
          col.condition = normalizeCondition({ expr: "", then: { map_to: [normalizeMapTo({})], transforms: [] } });
        }
        const mapList = resolveMapList(col, branch);
        if (mapList) {
          mapList.push(normalizeMapTo({}));
          renderColumns();
          updateModelJSON();
          updatePreview();
        }
      }
    }
  });

  addColumnButton.addEventListener("click", () => {
    state.mapping.columns.push(createEmptyColumn());
    renderColumns();
    updateModelJSON();
    updatePreview();
  });

  document
    .querySelectorAll('[data-field^="metadata"], [data-field^="book_resolution"], [data-field^="policies"]')
    .forEach((field) => {
      field.addEventListener("input", () => {
        updateModelJSON();
        updatePreview();
        if (field.dataset.field === "metadata.record_type") {
          renderColumns();
        }
      });
      field.addEventListener("change", () => {
        updateModelJSON();
        updatePreview();
        if (field.dataset.field === "metadata.record_type") {
          renderColumns();
        }
      });
    });

  const initHelpModals = () => {
    const modals = document.querySelectorAll(".modal-ajuda");
    document.addEventListener("click", (e) => {
      const helpButton = e.target.closest(".ajuda-boto");
      if (helpButton) {
        const targetId = helpButton.getAttribute("aria-controls");
        const modal = targetId ? document.getElementById(targetId) : null;
        if (modal) {
          modal.hidden = false;
          modal.focus();
        }
      }
      const closeButton = e.target.closest(".tancar-ajuda");
      if (closeButton) {
        const modal = closeButton.closest(".modal-ajuda");
        if (modal) modal.hidden = true;
      }
    });
    modals.forEach((modal) => {
      modal.addEventListener("click", (e) => {
        if (e.target === modal) modal.hidden = true;
      });
    });
  };

  applyStateToInputs();
  renderColumns();
  updateModelJSON();
  updatePreview();
  initHelpModals();
})();
