(function () {
  function initConfessionalForm() {
    var religion = document.getElementById("religio_confessio_codi");
    var level = document.getElementById("nivell_confessional_codi");
    var parent = document.getElementById("parent_id");
    if (!religion || !level) {
      return;
    }

    function allowedParentLevelCodes(selectedLevel) {
      if (!selectedLevel) {
        return [];
      }
      var raw = selectedLevel.getAttribute("data-parent-level-codes") || "";
      if (raw === "*") {
        return ["*"];
      }
      return raw.split(",").filter(function (code) {
        return !!code;
      });
    }

    function parentLevelAllowed(option, selectedReligion, selectedLevel) {
      if (!option.value || !selectedReligion || !selectedLevel) {
        return false;
      }
      if (option.getAttribute("data-religion-code") !== selectedReligion) {
        return false;
      }
      if (option.getAttribute("data-can-have-children") !== "true") {
        return false;
      }
      var parentLevel = option.getAttribute("data-level-code") || "";
      var allowedLevels = allowedParentLevelCodes(selectedLevel);
      if (allowedLevels.indexOf("*") >= 0) {
        return !!parentLevel;
      }
      return allowedLevels.indexOf(parentLevel) >= 0;
    }

    function syncConfessionalParents() {
      if (!parent) {
        return;
      }
      var selectedReligion = religion.value;
      var selectedLevel = level.selectedOptions.length > 0 && !level.selectedOptions[0].disabled ? level.selectedOptions[0] : null;
      var visibleParents = 0;

      Array.prototype.forEach.call(parent.options, function (option) {
        if (!option.value) {
          return;
        }
        var allowed = parentLevelAllowed(option, selectedReligion, selectedLevel);
        option.hidden = !allowed;
        option.disabled = !allowed;
        if (allowed) {
          visibleParents += 1;
        }
      });

      if (parent.selectedOptions.length > 0 && parent.selectedOptions[0].disabled) {
        parent.value = "";
      }

      var help = document.getElementById("parent_id_help");
      if (help) {
        if (!selectedReligion || !selectedLevel) {
          help.textContent = help.getAttribute("data-empty") || "";
        } else if (visibleParents === 0) {
          help.textContent = help.getAttribute("data-none") || "";
        } else {
          help.textContent = "";
        }
      }
    }

    function syncConfessionalLevels() {
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

      if (parent) {
        syncConfessionalParents();
      }
    }

    var form = religion.form;
    if (form) {
      form.addEventListener("submit", function (event) {
        var selected = level.selectedOptions.length > 0 ? level.selectedOptions[0] : null;
        if (!religion.value || !level.value || !selected || selected.disabled || selected.getAttribute("data-religion-code") !== religion.value) {
          event.preventDefault();
          syncConfessionalLevels();
          level.focus();
        }
      });
    }

    religion.addEventListener("change", syncConfessionalLevels);
    level.addEventListener("change", syncConfessionalParents);
    syncConfessionalLevels();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initConfessionalForm);
  } else {
    initConfessionalForm();
  }
})();
