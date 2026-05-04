(() => {
  const activeTabInput = document.getElementById("policy-active-tab");

  document.querySelectorAll('.perm-grant-card input[type="checkbox"]').forEach((cb) => {
    const card = cb.closest(".perm-grant-card");
    if (!card) return;
    const toggle = () => card.classList.toggle("actiu", cb.checked);
    cb.addEventListener("change", () => {
      toggle();
      syncPolicyJSONFromVisualState();
    });
    toggle();
  });

  document.querySelectorAll(".policy-tabs .tab-boto").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".policy-tabs .tab-boto").forEach((b) => b.classList.remove("actiu"));
      document.querySelectorAll(".policy-tab").forEach((t) => t.classList.remove("actiu"));
      btn.classList.add("actiu");
      const target = document.getElementById(btn.dataset.target);
      if (target) target.classList.add("actiu");
      if (activeTabInput) {
        if (btn.dataset.target === "tab-grants") activeTabInput.value = "grants";
        else if (btn.dataset.target === "tab-json") activeTabInput.value = "json";
        else activeTabInput.value = "gui";
      }
    });
  });

  const policyJSON = document.getElementById("policy-json-editor");
  const copyJSON = document.getElementById("policy-json-copy");
  const formatJSON = document.getElementById("policy-json-format");
  const regenerateJSON = document.getElementById("policy-json-regenerate");
  const syncStatus = document.getElementById("policy-json-sync-status");
  const policyJSONForm = document.getElementById("policy-json-form");
  const policyName = document.getElementById("nom");
  const policyDescription = document.getElementById("descripcio");
  const policyJSONName = document.getElementById("policy-json-name");
  const policyJSONDescription = document.getElementById("policy-json-description");
  let manualJSONEdit = false;
  let internalJSONUpdate = false;

  const updatePolicyHiddenFields = () => {
    if (policyName && policyJSONName) policyJSONName.value = policyName.value || "";
    if (policyDescription && policyJSONDescription) policyJSONDescription.value = policyDescription.value || "";
  };

  const normalizeDataBool = (value) => value === true || value === "1" || value === "true";

  const grantSortValue = (grant) => [
    grant.perm_key,
    grant.scope_type,
    grant.scope_id === null ? 0 : grant.scope_id,
    grant.include_children ? 1 : 0,
  ];

  const sortGrants = (a, b) => {
    const av = grantSortValue(a);
    const bv = grantSortValue(b);
    for (let i = 0; i < av.length; i += 1) {
      if (av[i] < bv[i]) return -1;
      if (av[i] > bv[i]) return 1;
    }
    return 0;
  };

  const grantKey = (grant) =>
    `${grant.perm_key}|${grant.scope_type}|${grant.scope_id === null ? "" : grant.scope_id}|${grant.include_children ? "1" : "0"}`;

  const grantFromDataset = (dataset) => {
    const permKey = dataset.permKey || "";
    const scopeType = dataset.scopeType || "global";
    if (!permKey) return null;
    const rawScopeID = dataset.scopeId || "";
    const scopeID = scopeType === "global" ? null : Number.parseInt(rawScopeID, 10);
    if (scopeType !== "global" && (!Number.isFinite(scopeID) || scopeID <= 0)) return null;
    return {
      perm_key: permKey,
      scope_type: scopeType,
      scope_id: scopeID,
      include_children: normalizeDataBool(dataset.includeChildren),
    };
  };

  const collectGuidedGrants = () => {
    const grants = [];
    document.querySelectorAll('.perm-grant-card input[type="checkbox"]:checked').forEach((cb) => {
      const card = cb.closest(".perm-grant-card");
      const grant = grantFromDataset({
        permKey: cb.dataset.permKey || (card ? card.dataset.permKey : "") || cb.value,
        scopeType: cb.dataset.scopeType || (card ? card.dataset.scopeType : "") || "global",
        scopeId: cb.dataset.scopeId || (card ? card.dataset.scopeId : ""),
        includeChildren: cb.dataset.includeChildren || (card ? card.dataset.includeChildren : "0"),
      });
      if (grant) grants.push(grant);
    });
    return grants;
  };

  const guidedGlobalKeys = () => {
    const keys = new Set();
    document.querySelectorAll(".perm-grant-card").forEach((card) => {
      if ((card.dataset.scopeType || "global") === "global" && card.dataset.permKey) {
        keys.add(card.dataset.permKey);
      }
    });
    return keys;
  };

  const currentGrantFormGrantID = () => {
    const grantID = document.getElementById("grant-id");
    return grantID ? grantID.value || "" : "";
  };

  const collectExistingGrantRows = () => {
    const grants = [];
    const guidedKeys = guidedGlobalKeys();
    const editedGrantID = currentGrantFormGrantID();
    document.querySelectorAll(".policy-grant-row").forEach((row) => {
      if (editedGrantID && row.dataset.grantId === editedGrantID) return;
      if ((row.dataset.scopeType || "global") === "global" && guidedKeys.has(row.dataset.permKey || "")) return;
      const grant = grantFromDataset(row.dataset);
      if (grant) grants.push(grant);
    });
    return grants;
  };

  const collectGranularDraftGrant = () => {
    const grantPerm = document.getElementById("grant-perm-key");
    const grantScopeType = document.getElementById("grant-scope-type");
    const grantScopeTypeUI = document.getElementById("grant-scope-type-ui");
    const grantScopeId = document.getElementById("grant-scope-id");
    const grantInclude = document.getElementById("grant-include-children");
    if (!grantPerm || !grantPerm.value) return null;
    return grantFromDataset({
      permKey: grantPerm.value,
      scopeType: grantScopeType ? grantScopeType.value : grantScopeTypeUI ? grantScopeTypeUI.value : "global",
      scopeId: grantScopeId ? grantScopeId.value : "",
      includeChildren: grantInclude && grantInclude.checked ? "1" : "0",
    });
  };

  function collectVisualPolicyJSON() {
    const grantsByKey = new Map();
    const grants = collectExistingGrantRows().concat(collectGuidedGrants());
    const draftGrant = collectGranularDraftGrant();
    if (draftGrant) grants.push(draftGrant);
    grants.forEach((grant) => {
      grantsByKey.set(grantKey(grant), grant);
    });
    return {
      version: 1,
      policy: {
        name: policyName ? policyName.value || "" : "",
        description: policyDescription ? policyDescription.value || "" : "",
      },
      grants: Array.from(grantsByKey.values()).sort(sortGrants),
    };
  }

  function renderPolicyJSONFromVisualState() {
    return JSON.stringify(collectVisualPolicyJSON(), null, 2);
  }

  function syncPolicyJSONFromVisualState(force = false) {
    updatePolicyHiddenFields();
    if (!policyJSON || (manualJSONEdit && !force)) return;
    internalJSONUpdate = true;
    policyJSON.value = renderPolicyJSONFromVisualState();
    internalJSONUpdate = false;
    if (syncStatus) syncStatus.textContent = "";
  }

  const markManualJSONEdit = () => {
    if (internalJSONUpdate) return;
    manualJSONEdit = true;
    if (syncStatus) syncStatus.textContent = "JSON modificat manualment. Prem Regenerar des de visual per sincronitzar.";
  };

  if (policyName) policyName.addEventListener("input", () => syncPolicyJSONFromVisualState());
  if (policyDescription) policyDescription.addEventListener("input", () => syncPolicyJSONFromVisualState());
  if (policyJSON) policyJSON.addEventListener("input", markManualJSONEdit);
  if (regenerateJSON) {
    regenerateJSON.addEventListener("click", () => {
      manualJSONEdit = false;
      syncPolicyJSONFromVisualState(true);
      if (syncStatus) syncStatus.textContent = "JSON sincronitzat des de la vista visual.";
    });
  }

  if (policyJSONForm) {
    policyJSONForm.addEventListener("submit", () => {
      updatePolicyHiddenFields();
    });
  }
  if (copyJSON && policyJSON) {
    copyJSON.addEventListener("click", async () => {
      policyJSON.select();
      try {
        await navigator.clipboard.writeText(policyJSON.value);
      } catch (_) {
        document.execCommand("copy");
      }
    });
  }
  if (formatJSON && policyJSON) {
    formatJSON.addEventListener("click", () => {
      try {
        policyJSON.value = JSON.stringify(JSON.parse(policyJSON.value), null, 2);
        markManualJSONEdit();
      } catch (_) {
        policyJSON.focus();
      }
    });
  }

  const grantForm = document.getElementById("grant-form");
  if (grantForm) {
    const grantId = document.getElementById("grant-id");
    const grantPerm = document.getElementById("grant-perm-key");
    const grantScopeTypeUI = document.getElementById("grant-scope-type-ui");
    const grantScopeType = document.getElementById("grant-scope-type");
    const grantScopeId = document.getElementById("grant-scope-id");
    const grantScopeSearch = document.getElementById("grant-scope-search");
    const grantScopeRow = document.getElementById("grant-scope-row");
    const grantInclude = document.getElementById("grant-include-children");
    const grantSubmit = document.getElementById("grant-submit");
    const grantCancel = document.getElementById("grant-cancel");
    const deleteForm = document.getElementById("grant-delete-form");
    const deleteId = document.getElementById("grant-delete-id");
    const labelAdd = grantSubmit ? grantSubmit.dataset.labelAdd || grantSubmit.textContent : "";
    const labelUpdate = grantSubmit ? grantSubmit.dataset.labelUpdate || grantSubmit.textContent : "";
    let lastScopeType = grantScopeTypeUI ? grantScopeTypeUI.value : "global";

    const normalizeScopeTypeForUI = (value) => {
      const raw = (value || "").toLowerCase();
      if (raw === "provincia" || raw === "comarca" || raw === "municipi" || raw === "nivell") {
        return "nivell";
      }
      return raw || "global";
    };

    const scopeApis = grantScopeSearch
      ? {
          global: "",
          pais: grantScopeSearch.dataset.apiPais || "",
          nivell: grantScopeSearch.dataset.apiNivell || "",
          entitat_eclesiastica: grantScopeSearch.dataset.apiEntitatEclesiastica || "",
          arxiu: grantScopeSearch.dataset.apiArxiu || "",
          llibre: grantScopeSearch.dataset.apiLlibre || "",
        }
      : {};
    const scopePlaceholders = grantScopeSearch
      ? {
          global: "",
          pais: grantScopeSearch.dataset.placeholderPais || "",
          nivell: grantScopeSearch.dataset.placeholderNivell || "",
          entitat_eclesiastica: grantScopeSearch.dataset.placeholderEntitatEclesiastica || "",
          arxiu: grantScopeSearch.dataset.placeholderArxiu || "",
          llibre: grantScopeSearch.dataset.placeholderLlibre || "",
        }
      : {};

    const updateScopeVisibility = () => {
      if (!grantScopeTypeUI || !grantScopeRow || !grantScopeId) return;
      const isGlobal = grantScopeTypeUI.value === "global";
      grantScopeRow.classList.toggle("is-hidden", isGlobal);
      grantScopeId.disabled = isGlobal;
      if (grantInclude) grantInclude.disabled = isGlobal;
      if (grantScopeSearch) {
        grantScopeSearch.disabled = isGlobal;
        grantScopeSearch.dataset.api = scopeApis[grantScopeTypeUI.value] || "";
        grantScopeSearch.placeholder = scopePlaceholders[grantScopeTypeUI.value] || "";
      }
    };

    const initialState = {
      id: grantId ? grantId.value : "",
      permKey: grantPerm ? grantPerm.value : "",
      scopeType: grantScopeType ? grantScopeType.value : grantScopeTypeUI ? grantScopeTypeUI.value : "global",
      scopeId: grantScopeId ? grantScopeId.value : "",
      scopeLabel: grantScopeSearch ? grantScopeSearch.value : "",
      includeChildren: grantInclude ? grantInclude.checked : false,
    };

    const setGrantState = (state) => {
      const scopeTypeActual = state.scopeType || "global";
      const scopeTypeUI = normalizeScopeTypeForUI(scopeTypeActual);
      if (grantId) grantId.value = state.id || "";
      if (grantPerm) grantPerm.value = state.permKey || "";
      if (grantScopeTypeUI) grantScopeTypeUI.value = scopeTypeUI;
      if (grantScopeType) grantScopeType.value = scopeTypeActual;
      if (grantScopeId) grantScopeId.value = state.scopeId || "";
      if (grantScopeSearch) grantScopeSearch.value = state.scopeLabel || state.scopeId || "";
      if (grantInclude) grantInclude.checked = !!state.includeChildren;
      updateScopeVisibility();
      if (grantSubmit) grantSubmit.textContent = state.id ? labelUpdate : labelAdd;
      if (grantCancel) grantCancel.style.display = state.id ? "inline-flex" : "none";
      syncPolicyJSONFromVisualState();
    };

    setGrantState(initialState);
    lastScopeType = grantScopeTypeUI ? grantScopeTypeUI.value : "global";
    if (grantScopeTypeUI)
      grantScopeTypeUI.addEventListener("change", () => {
        if (lastScopeType !== grantScopeTypeUI.value) {
          if (grantScopeId) grantScopeId.value = "";
          if (grantScopeSearch) grantScopeSearch.value = "";
        }
        if (grantScopeType) {
          grantScopeType.value = grantScopeTypeUI.value === "nivell" ? "nivell" : grantScopeTypeUI.value;
        }
        lastScopeType = grantScopeTypeUI.value;
        updateScopeVisibility();
        syncPolicyJSONFromVisualState();
      });
    [grantPerm, grantScopeType, grantScopeId, grantScopeSearch, grantInclude].forEach((field) => {
      if (!field) return;
      field.addEventListener("input", () => syncPolicyJSONFromVisualState());
      field.addEventListener("change", () => syncPolicyJSONFromVisualState());
    });
    if (grantCancel) grantCancel.addEventListener("click", () => setGrantState(initialState));

    document.querySelectorAll(".grant-edit").forEach((btn) => {
      btn.addEventListener("click", () => {
        setGrantState({
          id: btn.dataset.grantId || "",
          permKey: btn.dataset.permKey || "",
          scopeType: btn.dataset.scopeType || "global",
          scopeId: btn.dataset.scopeId || "",
          scopeLabel: btn.dataset.scopeLabel || "",
          includeChildren: btn.dataset.includeChildren === "1",
        });
        syncPolicyJSONFromVisualState();
      });
    });

    document.querySelectorAll(".grant-delete").forEach((btn) => {
      btn.addEventListener("click", () => {
        if (!deleteForm || !deleteId) return;
        deleteId.value = btn.dataset.grantId || "";
        if (deleteForm.requestSubmit) deleteForm.requestSubmit();
        else deleteForm.submit();
      });
    });
  }
})();
