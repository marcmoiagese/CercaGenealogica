(() => {
  const recordSelect = document.getElementById("wizardRecordType");
  const groups = document.querySelectorAll(".wizard-mapping-panel optgroup[data-group]");
  const targetSelects = document.querySelectorAll('select[name^="col_target_"]');

  const isPersonRoleTarget = (value) => {
    if (!value || value.indexOf("person.") !== 0) return false;
    const rest = value.slice("person.".length);
    return rest !== "" && rest.indexOf(".") === -1;
  };

  const applyFilters = () => {
    if (!recordSelect) return;
    const value = recordSelect.value || "baptisme";
    groups.forEach((group) => {
      const key = group.getAttribute("data-group") || "";
      const enable = value === "generic" || key === "common" || key === value;
      group.disabled = !enable;
    });
  };

  const updateNameOrderForSelect = (select) => {
    const card = select.closest(".wizard-column-card");
    if (!card) return;
    const field = card.querySelector(".wizard-name-order-field");
    if (!field) return;
    const orderSelect = field.querySelector("select");
    const show = isPersonRoleTarget(select.value);
    field.style.display = show ? "" : "none";
    if (orderSelect) {
      orderSelect.disabled = !show;
    }
  };

  const updateAllNameOrders = () => {
    targetSelects.forEach((select) => {
      updateNameOrderForSelect(select);
    });
  };

  if (recordSelect) {
    recordSelect.addEventListener("change", applyFilters);
    applyFilters();
  }

  targetSelects.forEach((select) => {
    select.addEventListener("change", () => {
      updateNameOrderForSelect(select);
    });
  });
  updateAllNameOrders();
})();
