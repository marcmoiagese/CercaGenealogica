(function () {
  function initConfessionalForm() {
    const religion = document.getElementById("religio_confessio_codi");
    const level = document.getElementById("nivell_confessional_codi");
    const parent = document.querySelector("input[name='parent_id']");
    const parentLabel = document.getElementById("parent_id_label");
    const parentSuggestions = document.getElementById("parent_id_suggestions");
    const formID = document.querySelector("input[name='id']");
    if (!religion || !level) {
      return;
    }

    let lastParentItems = [];
    let activeParentIndex = -1;
    let parentTimer = null;
    let parentSuggestionsController = null;
    let parentSuggestionsRequestID = 0;
    let parentCompatibilityMessage = "";
    let parentSearchHasResults = false;
    let parentSearchConfirmedEmpty = false;

    function selectedLevelOption() {
      if (level.selectedOptions.length === 0 || level.selectedOptions[0].disabled) {
        return null;
      }
      return level.selectedOptions[0];
    }

    function clearParentSuggestions() {
      if (!parentSuggestions) {
        return;
      }
      parentSuggestions.innerHTML = "";
      parentSuggestions.classList.remove("is-open");
      lastParentItems = [];
      activeParentIndex = -1;
    }

    function resetSelectedParentMetadata() {
      if (!parentLabel) {
        return;
      }
      parentLabel.dataset.selectedParentLevelCode = "";
      parentLabel.dataset.selectedParentReligionCode = "";
    }

    function storeSelectedParentMetadata(item) {
      if (!parentLabel) {
        return;
      }
      parentLabel.dataset.selectedParentLevelCode = item && item.nivell_confessional ? String(item.nivell_confessional) : "";
      parentLabel.dataset.selectedParentReligionCode = item && item.religio_confessio ? String(item.religio_confessio) : "";
    }

    function clearSelectedParent() {
      if (!parent || !parentLabel) {
        return;
      }
      parent.value = "";
      parentLabel.value = "";
      resetSelectedParentMetadata();
    }

    function abortParentSuggestions() {
      if (parentTimer !== null) {
        window.clearTimeout(parentTimer);
        parentTimer = null;
      }
      if (parentSuggestionsController) {
        parentSuggestionsController.abort();
        parentSuggestionsController = null;
      }
    }

    function setActiveParent(index) {
      if (!parentSuggestions) {
        return;
      }
      Array.prototype.forEach.call(parentSuggestions.querySelectorAll("li"), function (item, idx) {
        item.classList.toggle("is-active", idx === index);
      });
      activeParentIndex = index;
    }

    function renderParentSuggestions(items) {
      if (!parentSuggestions) {
        return;
      }
      parentSuggestions.innerHTML = "";
      lastParentItems = items || [];
      activeParentIndex = -1;
      if (!items || items.length === 0) {
        const empty = document.createElement("li");
        empty.className = "suggestion-empty";
        empty.textContent = parentLabel ? parentLabel.dataset.emptyLabel || "No results" : "No results";
        parentSuggestions.appendChild(empty);
        parentSuggestions.classList.add("is-open");
        return;
      }
      items.forEach(function (item, idx) {
        const li = document.createElement("li");
        li.className = "suggestion-option-item";
        li.dataset.index = String(idx);
        const button = document.createElement("button");
        button.type = "button";
        button.className = "suggestion-option";
        button.title = item.nom || "";
        button.setAttribute("aria-label", item.nom || "");
        button.addEventListener("mousedown", function (event) {
          event.preventDefault();
        });
        const title = document.createElement("span");
        title.className = "suggestion-title";
        title.textContent = item.nom || "";
        button.appendChild(title);
        if (item.context) {
          const context = document.createElement("span");
          context.className = "suggestion-context";
          context.textContent = item.context;
          button.appendChild(context);
        }
        button.addEventListener("click", function () {
          applyParentSuggestion(item);
        });
        li.appendChild(button);
        parentSuggestions.appendChild(li);
      });
      parentSuggestions.classList.add("is-open");
    }

    function applyParentSuggestion(item) {
      if (!item || !parent || !parentLabel) {
        return;
      }
      parentCompatibilityMessage = "";
      parent.value = item.id ? String(item.id) : "";
      parentLabel.value = item.nom || "";
      storeSelectedParentMetadata(item);
      parent.dispatchEvent(new Event("change", { bubbles: true }));
      parentLabel.dispatchEvent(new CustomEvent("suggest:select", { detail: { item: item } }));
      clearParentSuggestions();
      syncParentHelp();
    }

    function syncParentHelp() {
      const help = document.getElementById("parent_id_help");
      if (!help) {
        return;
      }
      if (parentCompatibilityMessage) {
        help.textContent = parentCompatibilityMessage;
        return;
      }
      const selectedLevel = selectedLevelOption();
      const parentLevelCodes = selectedLevel ? selectedLevel.dataset.parentLevelCodes || "" : "";
      if (!religion.value || !selectedLevel) {
        help.textContent = help.dataset.empty || "";
      } else if (parentLevelCodes === "") {
        help.textContent = help.dataset.none || "";
      } else if (parentSearchHasResults) {
        help.textContent = "";
      } else if (parentSearchConfirmedEmpty) {
        help.textContent = help.dataset.none || "";
      } else {
        help.textContent = "";
      }
    }

    function selectedParentCompatibilityState() {
      if (!parent || !parent.value || !parentLabel) {
        return { compatible: true, reason: "" };
      }
      const selectedLevel = selectedLevelOption();
      if (!selectedLevel || !religion.value) {
        return { compatible: true, reason: "" };
      }
      const parentReligionCode = parentLabel.dataset.selectedParentReligionCode || "";
      const parentLevelCode = parentLabel.dataset.selectedParentLevelCode || "";
      if (!parentReligionCode || !parentLevelCode) {
        return { compatible: false, reason: "incompatible" };
      }
      if (parentReligionCode !== religion.value) {
        return { compatible: false, reason: "incompatible" };
      }
      const parentLevelCodes = (selectedLevel.dataset.parentLevelCodes || "").split(",").filter(Boolean);
      if (parentLevelCodes.includes("*")) {
        return { compatible: true, reason: "" };
      }
      if (!parentLevelCodes.includes(parentLevelCode)) {
        return { compatible: false, reason: "incompatible" };
      }
      return { compatible: true, reason: "" };
    }

    function syncSelectedParentCompatibility() {
      const compatibility = selectedParentCompatibilityState();
      if (compatibility.compatible) {
        parentCompatibilityMessage = "";
        syncParentHelp();
        return true;
      }
      const help = document.getElementById("parent_id_help");
      parentCompatibilityMessage = help ? help.dataset.incompatible || "" : "";
      clearSelectedParent();
      clearParentSuggestions();
      syncParentHelp();
      return false;
    }

    function buildParentSuggestURL(query) {
      const apiAttr = parentLabel ? parentLabel.dataset.api || "" : "";
      if (!apiAttr) {
        return null;
      }
      const selectedLevel = selectedLevelOption();
      if (!religion.value || !selectedLevel) {
        return null;
      }
      const url = new URL(apiAttr, window.location.origin);
      const parentLevelCodes = selectedLevel.dataset.parentLevelCodes || "";
      if (parentLevelCodes === "") {
        return null;
      }
      url.searchParams.set("q", query);
      url.searchParams.set("limit", "10");
      url.searchParams.set("religio_confessio_codi", religion.value);
      url.searchParams.set("nivell_confessional_codi", level.value);
      if (formID && formID.value) {
        url.searchParams.set("child_id", formID.value);
        url.searchParams.set("exclude_id", formID.value);
      }
      return url;
    }

    function handleParentSuggestResponse(data, requestID) {
      if (requestID !== parentSuggestionsRequestID) {
        return;
      }
      renderParentSuggestions(data.items || []);
      parentSearchHasResults = Array.isArray(data.items) && data.items.length > 0;
      parentSearchConfirmedEmpty = Array.isArray(data.items) && data.items.length === 0;
      syncParentHelp();
    }

    function handleParentSuggestError(err, requestID) {
      if (requestID !== parentSuggestionsRequestID) {
        return;
      }
      if (err && err.name === "AbortError") {
        return;
      }
      parentSearchHasResults = false;
      clearParentSuggestions();
    }

    function fetchParentSuggestions(query) {
      if (!parentLabel) {
        return;
      }
      const url = buildParentSuggestURL(query);
      if (!url) {
        abortParentSuggestions();
        parentSearchHasResults = false;
        parentSearchConfirmedEmpty = false;
        clearParentSuggestions();
        syncParentHelp();
        return;
      }
      parentSuggestionsRequestID += 1;
      const requestID = parentSuggestionsRequestID;
      abortParentSuggestions();
      parentSuggestionsController = new AbortController();
      fetch(url.toString(), { credentials: "same-origin", signal: parentSuggestionsController.signal })
        .then(function (resp) { return resp.json(); })
        .then(function (data) {
          handleParentSuggestResponse(data, requestID);
        })
        .catch(function (err) {
          handleParentSuggestError(err, requestID);
        });
    }

    function syncConfessionalLevels(resetParent) {
      const selectedReligion = religion.value;
      let visibleLevels = 0;

      Array.prototype.forEach.call(level.options, function (option) {
        if (!option.value) {
          return;
        }
        const matchesReligion = !!selectedReligion && option.dataset.religionCode === selectedReligion;
        option.hidden = !matchesReligion;
        option.disabled = !matchesReligion;
        if (matchesReligion) {
          visibleLevels += 1;
        }
      });

      if (level.selectedOptions.length > 0 && level.selectedOptions[0].disabled) {
        level.value = "";
      }

      level.disabled = visibleLevels === 0;

      const help = document.getElementById("nivell_confessional_help");
      if (help) {
        if (!selectedReligion) {
          help.textContent = help.dataset.empty || "";
        } else if (visibleLevels === 0) {
          help.textContent = help.dataset.none || "";
        } else {
          help.textContent = "";
        }
      }

      if (resetParent && parent && parentLabel) {
        abortParentSuggestions();
        parentCompatibilityMessage = "";
        parentSearchHasResults = false;
        parentSearchConfirmedEmpty = false;
        clearSelectedParent();
        clearParentSuggestions();
      }
      syncParentHelp();
    }

    if (parentLabel && parent) {
      parentLabel.addEventListener("input", function () {
        parentCompatibilityMessage = "";
        parent.value = "";
        resetSelectedParentMetadata();
        parentSearchHasResults = false;
        parentSearchConfirmedEmpty = false;
        if (parentLabel.value.trim().length < 1) {
          abortParentSuggestions();
          clearParentSuggestions();
          syncParentHelp();
          return;
        }
        window.clearTimeout(parentTimer);
        parentTimer = window.setTimeout(function () {
          fetchParentSuggestions(parentLabel.value.trim());
        }, 250);
      });
      parentLabel.addEventListener("keydown", function (event) {
        if (!parentSuggestions || parentSuggestions.children.length === 0) {
          return;
        }
        if (event.key === "ArrowDown") {
          event.preventDefault();
          setActiveParent(Math.min(activeParentIndex + 1, parentSuggestions.children.length - 1));
        } else if (event.key === "ArrowUp") {
          event.preventDefault();
          setActiveParent(Math.max(activeParentIndex - 1, 0));
        } else if (event.key === "Enter" && lastParentItems.length > 0) {
          event.preventDefault();
          applyParentSuggestion(lastParentItems[activeParentIndex >= 0 ? activeParentIndex : 0]);
        } else if (event.key === "Escape") {
          abortParentSuggestions();
          parentSearchHasResults = false;
          clearParentSuggestions();
        }
      });
      document.addEventListener("click", function (event) {
        if (event.target === parentLabel || (parentSuggestions && parentSuggestions.contains(event.target))) {
          return;
        }
        abortParentSuggestions();
        parentSearchHasResults = false;
        clearParentSuggestions();
      });
    }

    const form = religion.form;
    if (form) {
      form.addEventListener("submit", function (event) {
        const selected = selectedLevelOption();
        if (!religion.value || !level.value || !selected || selected.dataset.religionCode !== religion.value) {
          event.preventDefault();
          syncConfessionalLevels(false);
          level.focus();
          return;
        }
        if (!syncSelectedParentCompatibility()) {
          event.preventDefault();
          parentLabel.focus();
          return;
        }
        if (parentLabel && parent && parentLabel.value.trim() !== "" && parent.value === "") {
          event.preventDefault();
          parentLabel.focus();
          clearParentSuggestions();
          fetchParentSuggestions(parentLabel.value.trim());
        }
      });
    }

    religion.addEventListener("change", function () {
      parentCompatibilityMessage = "";
      syncConfessionalLevels(true);
    });
    level.addEventListener("change", function () {
      abortParentSuggestions();
      parentSearchHasResults = false;
      parentSearchConfirmedEmpty = false;
      clearParentSuggestions();
      syncSelectedParentCompatibility();
    });
    syncConfessionalLevels(false);
    syncSelectedParentCompatibility();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initConfessionalForm);
  } else {
    initConfessionalForm();
  }
})();
