(() => {
  const activeTabInput = document.getElementById("policy-active-tab");

  document.querySelectorAll('.perm-grant-card input[type="checkbox"]').forEach((cb) => {
    const card = cb.closest(".perm-grant-card");
    if (!card) return;
    const toggle = () => card.classList.toggle("actiu", cb.checked);
    cb.addEventListener("change", toggle);
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
        else activeTabInput.value = "gui";
      }
    });
  });

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
