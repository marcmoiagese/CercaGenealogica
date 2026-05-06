(function () {
  function initConfessionalForm() {
    var religion = document.getElementById("religio_confessio_codi");
    var level = document.getElementById("nivell_confessional_codi");
    var parent = document.getElementById("parent_id");
    if (!religion || !level) {
      return;
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
        Array.prototype.forEach.call(parent.options, function (option) {
          if (!option.value) {
            return;
          }
          var matchesParentReligion = !selectedReligion || option.getAttribute("data-religion-code") === selectedReligion;
          option.hidden = !matchesParentReligion;
          option.disabled = !matchesParentReligion;
        });
        if (parent.selectedOptions.length > 0 && parent.selectedOptions[0].disabled) {
          parent.value = "";
        }
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
    syncConfessionalLevels();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initConfessionalForm);
  } else {
    initConfessionalForm();
  }
})();
