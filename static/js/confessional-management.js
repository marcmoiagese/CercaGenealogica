(function () {
  function initConfessionalManagement() {
    const form = document.getElementById("nivellsFilterForm");
    const religion = document.getElementById("confessional-religion");
    const level = document.getElementById("confessional-level");
    const parentHidden = document.getElementById("confessional-parent-filter");
    const parentLabel = document.getElementById("confessional-parent-filter-label");
    if (!form) {
      return;
    }

    function syncLevels() {
      if (!religion || !level) {
        return;
      }
      const selectedReligion = religion.value;
      Array.prototype.forEach.call(level.options, function (option) {
        if (!option.value) {
          return;
        }
        const visible = !selectedReligion || option.dataset.religionCode === selectedReligion;
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
