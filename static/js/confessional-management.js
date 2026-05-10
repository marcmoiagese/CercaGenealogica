(function () {
  function initConfessionalManagement() {
    var form = document.getElementById("nivellsFilterForm");
    var religion = document.getElementById("confessional-religion");
    var level = document.getElementById("confessional-level");
    var parentHidden = document.getElementById("confessional-parent-filter");
    var parentLabel = document.getElementById("confessional-parent-filter-label");
    if (!form) {
      return;
    }

    function syncLevels() {
      if (!religion || !level) {
        return;
      }
      var selectedReligion = religion.value;
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

    if (religion) {
      religion.addEventListener("change", function () {
        syncLevels();
        if (parentHidden && parentLabel) {
          parentHidden.value = "";
          parentLabel.value = "";
        }
      });
    }

    if (parentLabel && parentHidden) {
      parentLabel.addEventListener("input", function () {
        parentHidden.value = "";
      });
    }

    syncLevels();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initConfessionalManagement);
  } else {
    initConfessionalManagement();
  }
})();
