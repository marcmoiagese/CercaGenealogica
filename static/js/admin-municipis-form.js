(() => {
  const allLevelsInput = document.getElementById("all-levels");
  const prefillInput = document.getElementById("prefill-levels");
  const countrySelect = document.getElementById("pais_id");
  const levelSelects = Array.from(document.querySelectorAll('[id^="nivell_administratiu_id_"]'));

  if (!allLevelsInput || !prefillInput || !countrySelect || !levelSelects.length) {
    return;
  }

  let allLevels = [];
  let prefill = [];
  try {
    allLevels = JSON.parse(allLevelsInput.value || "[]");
  } catch (_) {
    allLevels = [];
  }
  try {
    prefill = JSON.parse(prefillInput.value || "[]");
  } catch (_) {
    prefill = [];
  }

  const clearFrom = (idx) => {
    for (let i = idx; i < levelSelects.length; i++) {
      const sel = levelSelects[i];
      const noneLabel = sel.dataset.noneLabel || "";
      sel.innerHTML = `<option value="">${noneLabel}</option>`;
      sel.disabled = true;
    }
  };

  const uniqueLevelNumbers = (levels, countryId) => {
    const nums = new Set();
    levels.forEach((l) => {
      if (l.PaisID === countryId) nums.add(l.Nivel);
    });
    return Array.from(nums).sort((a, b) => a - b);
  };

  const optionsFor = (levelNum, countryId, parentId) =>
    allLevels.filter(
      (l) => l.PaisID === countryId && l.Nivel === levelNum && (!parentId || (l.ParentID && l.ParentID === parentId))
    );

  const populateSelect = (sel, options, selectedVal) => {
    const noneLabel = sel.dataset.noneLabel || "";
    let html = `<option value="">${noneLabel}</option>`;
    options.forEach((o) => {
      const label = o.NomNivell + (o.TipusNivell ? ` (${o.TipusNivell})` : "");
      html += `<option value="${o.ID}" ${selectedVal === o.ID ? "selected" : ""}>${label}</option>`;
    });
    sel.innerHTML = html;
    sel.disabled = options.length === 0;
  };

  const findLevelByID = (id) => {
    for (const l of allLevels) {
      if (l.ID === id) return l;
    }
    return null;
  };

  const applyCascade = (countryId, prefillIDs) => {
    clearFrom(0);
    if (!countryId) return;
    const levelNums = uniqueLevelNumbers(allLevels, countryId);
    if (levelNums.length === 0) return;
    const startLevel = levelNums[0];
    const startSelect = levelSelects[startLevel - 1];
    const startPrefill = prefillIDs[startLevel - 1] && prefillIDs[startLevel - 1].Valid
      ? Number(prefillIDs[startLevel - 1].Int64)
      : null;
    populateSelect(startSelect, optionsFor(startLevel, countryId, null), startPrefill);
    startSelect.disabled = false;
    for (let i = startLevel; i < 7; i++) {
      const sel = levelSelects[i - 1];
      const val = sel && sel.value
        ? Number(sel.value)
        : prefillIDs[i - 1] && prefillIDs[i - 1].Valid
        ? Number(prefillIDs[i - 1].Int64)
        : null;
      if (!val) {
        clearFrom(i);
        break;
      }
      const nextLevel = i + 1;
      if (nextLevel > 7) break;
      const nextSel = levelSelects[nextLevel - 1];
      const opts = optionsFor(nextLevel, countryId, val);
      const nextPrefill = prefillIDs[nextLevel - 1] && prefillIDs[nextLevel - 1].Valid
        ? Number(prefillIDs[nextLevel - 1].Int64)
        : null;
      populateSelect(nextSel, opts, nextPrefill);
      if (opts.length === 0) {
        clearFrom(nextLevel);
        break;
      }
    }
  };

  countrySelect.addEventListener("change", () => {
    const cid = Number(countrySelect.value) || 0;
    applyCascade(cid, prefill);
  });

  levelSelects.forEach((sel, idx) => {
    sel.addEventListener("change", () => {
      const cid = Number(countrySelect.value) || 0;
      if (!cid) {
        clearFrom(0);
        return;
      }
      clearFrom(idx + 1);
      const val = sel.value ? Number(sel.value) : null;
      if (!val) return;
      const nextIdx = idx + 1;
      if (nextIdx >= levelSelects.length) return;
      const nextLevelNum = nextIdx + 1;
      const opts = optionsFor(nextLevelNum, cid, val);
      populateSelect(levelSelects[nextIdx], opts, null);
    });
  });

  const initialCountry = (() => {
    for (let i = 0; i < prefill.length; i++) {
      if (prefill[i] && prefill[i].Valid) {
        const l = findLevelByID(Number(prefill[i].Int64));
        if (l) return l.PaisID;
      }
    }
    return Number(countrySelect.value) || 0;
  })();

  if (initialCountry) {
    countrySelect.value = initialCountry;
    applyCascade(initialCountry, prefill);
  } else {
    clearFrom(0);
  }
})();
