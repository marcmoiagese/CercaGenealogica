(function () {
  function initConfessionalForm() {
    var religion = document.getElementById("religio_confessio_codi");
    var level = document.getElementById("nivell_confessional_codi");
    var parent = document.getElementById("parent_id");
    var parentLabel = document.getElementById("parent_id_label");
    var parentSuggestions = document.getElementById("parent_id_suggestions");
    var formID = document.querySelector("input[name='id']");
    if (!religion || !level) {
      return;
    }

    var lastParentItems = [];
    var activeParentIndex = -1;
    var parentTimer = null;

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
        var empty = document.createElement("li");
        empty.className = "suggestion-empty";
        empty.textContent = parentLabel ? parentLabel.dataset.emptyLabel || "No results" : "No results";
        parentSuggestions.appendChild(empty);
        parentSuggestions.classList.add("is-open");
        return;
      }
      items.forEach(function (item, idx) {
        var li = document.createElement("li");
        li.dataset.index = String(idx);
        var title = document.createElement("span");
        title.className = "suggestion-title";
        title.textContent = item.nom || "";
        li.appendChild(title);
        if (item.context) {
          var context = document.createElement("span");
          context.className = "suggestion-context";
          context.textContent = item.context;
          li.appendChild(context);
        }
        li.addEventListener("click", function () {
          applyParentSuggestion(item);
        });
        parentSuggestions.appendChild(li);
      });
      parentSuggestions.classList.add("is-open");
    }

    function applyParentSuggestion(item) {
      if (!item || !parent || !parentLabel) {
        return;
      }
      parent.value = item.id ? String(item.id) : "";
      parentLabel.value = item.nom || "";
      parent.dispatchEvent(new Event("change", { bubbles: true }));
      parentLabel.dispatchEvent(new CustomEvent("suggest:select", { detail: { item: item } }));
      clearParentSuggestions();
    }

    function syncParentHelp() {
      var help = document.getElementById("parent_id_help");
      if (!help) {
        return;
      }
      var selectedLevel = selectedLevelOption();
      if (!religion.value || !selectedLevel) {
        help.textContent = help.getAttribute("data-empty") || "";
      } else {
        help.textContent = "";
      }
    }

    function fetchParentSuggestions(query) {
      if (!parentLabel) {
        return;
      }
      var apiAttr = parentLabel.dataset.api || "";
      if (!apiAttr) {
        clearParentSuggestions();
        return;
      }
      var selectedLevel = selectedLevelOption();
      if (!religion.value || !selectedLevel) {
        clearParentSuggestions();
        syncParentHelp();
        return;
      }
      var url = new URL(apiAttr, window.location.origin);
      url.searchParams.set("q", query);
      url.searchParams.set("limit", "10");
      url.searchParams.set("religio_confessio_codi", religion.value);
      url.searchParams.set("nivell_confessional_codi", level.value);
      if (formID && formID.value) {
        url.searchParams.set("child_id", formID.value);
        url.searchParams.set("exclude_id", formID.value);
      }
      fetch(url.toString(), { credentials: "same-origin" })
        .then(function (resp) { return resp.json(); })
        .then(function (data) {
          renderParentSuggestions(data.items || []);
          syncParentHelp();
        })
        .catch(function () {
          clearParentSuggestions();
        });
    }

    function syncConfessionalLevels(resetParent) {
      var selectedReligion = religion.value;
      var visibleLevels = 0;

      Array.prototype.forEach.call(level.options, function (option) {
        if (!option.value) {
          return;
        }
        var matchesReligion = !!selectedReligion && option.getAttribute("data-religion-code") === selectedReligion;
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

      var help = document.getElementById("nivell_confessional_help");
      if (help) {
        if (!selectedReligion) {
          help.textContent = help.getAttribute("data-empty") || "";
        } else if (visibleLevels === 0) {
          help.textContent = help.getAttribute("data-none") || "";
        } else {
          help.textContent = "";
        }
      }

      if (resetParent && parent && parentLabel) {
        parent.value = "";
        parentLabel.value = "";
        clearParentSuggestions();
      }
      syncParentHelp();
    }

    if (parentLabel && parent) {
      parentLabel.addEventListener("input", function () {
        parent.value = "";
        if (parentLabel.value.trim().length < 1) {
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
          clearParentSuggestions();
        }
      });
      document.addEventListener("click", function (event) {
        if (event.target === parentLabel || (parentSuggestions && parentSuggestions.contains(event.target))) {
          return;
        }
        clearParentSuggestions();
      });
    }

    var form = religion.form;
    if (form) {
      form.addEventListener("submit", function (event) {
        var selected = selectedLevelOption();
        if (!religion.value || !level.value || !selected || selected.getAttribute("data-religion-code") !== religion.value) {
          event.preventDefault();
          syncConfessionalLevels(false);
          level.focus();
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
      syncConfessionalLevels(true);
    });
    level.addEventListener("change", function () {
      if (parent && parentLabel) {
        parent.value = "";
        parentLabel.value = "";
      }
      clearParentSuggestions();
      syncParentHelp();
    });
    syncConfessionalLevels(false);
    if (parent && parent.value && parentLabel && parentLabel.value) {
      syncParentHelp();
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initConfessionalForm);
  } else {
    initConfessionalForm();
  }
})();
