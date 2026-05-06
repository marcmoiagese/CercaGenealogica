(function () {
  function initConfessionalHierarchy() {
    var form = document.getElementById("confessional-hierarchy-filters");
    if (!form) {
      return;
    }

    var search = document.getElementById("confessional-q");
    var religion = document.getElementById("confessional-religion");
    var level = document.getElementById("confessional-level");
    var parent = document.getElementById("confessional-parent");
    var debounceTimer = null;

    function selectedLevelOption() {
      if (!level || level.selectedOptions.length === 0 || level.selectedOptions[0].disabled) {
        return null;
      }
      return level.selectedOptions[0];
    }

    function allowedParentLevelCodes(option) {
      if (!option) {
        return [];
      }
      var raw = option.getAttribute("data-parent-level-codes") || "";
      if (raw === "*") {
        return ["*"];
      }
      return raw.split(",").filter(function (code) {
        return !!code;
      });
    }

    function parentAllowed(option) {
      if (!option.value) {
        return true;
      }
      var selectedReligion = religion ? religion.value : "";
      var selectedLevel = selectedLevelOption();
      if (selectedReligion && option.getAttribute("data-religion-code") !== selectedReligion) {
        return false;
      }
      if (option.getAttribute("data-can-have-children") !== "true") {
        return false;
      }
      if (!selectedLevel) {
        return true;
      }
      var allowed = allowedParentLevelCodes(selectedLevel);
      if (allowed.indexOf("*") >= 0) {
        return true;
      }
      return allowed.indexOf(option.getAttribute("data-level-code") || "") >= 0;
    }

    function syncLevelsAndParents() {
      var selectedReligion = religion ? religion.value : "";
      if (level) {
        Array.prototype.forEach.call(level.options, function (option) {
          if (!option.value) {
            return;
          }
          var visible = !selectedReligion || option.getAttribute("data-religion-code") === selectedReligion;
          option.hidden = !visible;
          option.disabled = !visible;
        });
        if (level.selectedOptions.length > 0 && level.selectedOptions[0].disabled) {
          level.value = "";
        }
      }
      if (parent) {
        Array.prototype.forEach.call(parent.options, function (option) {
          var visible = parentAllowed(option);
          option.hidden = !visible;
          option.disabled = !visible;
        });
        if (parent.selectedOptions.length > 0 && parent.selectedOptions[0].disabled) {
          parent.value = "";
        }
      }
    }

    function submitSoon(delay) {
      if (debounceTimer) {
        window.clearTimeout(debounceTimer);
      }
      debounceTimer = window.setTimeout(function () {
        syncLevelsAndParents();
        form.submit();
      }, delay);
    }

    form.querySelectorAll("[data-submit-form='true']").forEach(function (el) {
      el.addEventListener("change", function () {
        if (el === religion && parent) {
          parent.value = "";
        }
        if (el === religion && level) {
          syncLevelsAndParents();
        }
        if (el === level && parent) {
          parent.value = "";
        }
        submitSoon(0);
      });
    });

    if (search) {
      search.addEventListener("input", function () {
        submitSoon(350);
      });
    }

    syncLevelsAndParents();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initConfessionalHierarchy);
  } else {
    initConfessionalHierarchy();
  }
})();
