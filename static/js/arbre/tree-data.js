(() => {
  const readJSON = (id, fallback) => {
    const el = document.getElementById(id);
    if (!el) return fallback;
    try {
      return JSON.parse(el.textContent || "null");
    } catch (_) {
      return fallback;
    }
  };

  const familyData = readJSON("tree-family-data", null);
  if (familyData !== null) {
    window.familyData = familyData;
  }

  const familyLinks = readJSON("tree-family-links", null);
  if (familyLinks !== null) {
    window.familyLinks = familyLinks;
  }

  const rootPersonId = readJSON("tree-root-person-id", null);
  if (rootPersonId !== null) {
    window.rootPersonId = rootPersonId;
  }

  const datasetStats = readJSON("tree-dataset-stats", null);
  if (datasetStats !== null) {
    window.__DATASET_STATS = datasetStats;
  }

  const profileBase = readJSON("tree-profile-base", null);
  if (profileBase !== null) {
    window.treeProfileBase = profileBase;
  }

  const expandDisabled = readJSON("tree-expand-disabled", null);
  if (expandDisabled !== null) {
    window.treeExpandDisabled = !!expandDisabled;
  }

  const rootSelect = document.getElementById("rootSelect");
  if (rootSelect) {
    rootSelect.addEventListener("change", (e) => {
      const root = e.target.value;
      const params = new URLSearchParams(window.location.search);
      params.set("root_id", root);
      window.location.search = params.toString();
    });
  }
})();
