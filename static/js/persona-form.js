(() => {
  const modal = document.getElementById("persona-link-modal");
  if (!modal) {
    return;
  }
  const fieldInput = document.getElementById("persona-link-field");
  const fieldLabel = document.getElementById("persona-link-field-label");
  const registreInput = document.getElementById("persona-link-registre");
  const openButtons = document.querySelectorAll("[data-persona-link]");
  const closeButtons = modal.querySelectorAll("[data-close-persona-link]");

  const openModal = (btn) => {
    if (fieldInput) {
      fieldInput.value = btn.dataset.fieldKey || "";
    }
    if (fieldLabel) {
      fieldLabel.textContent = btn.dataset.fieldLabel || "—";
    }
    if (registreInput) {
      registreInput.value = "";
      registreInput.focus();
    }
    modal.classList.add("is-open");
  };

  const closeModal = () => {
    modal.classList.remove("is-open");
  };

  openButtons.forEach((btn) => {
    btn.addEventListener("click", () => openModal(btn));
  });
  closeButtons.forEach((btn) => {
    btn.addEventListener("click", closeModal);
  });
  modal.addEventListener("click", (event) => {
    if (event.target === modal) {
      closeModal();
    }
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeModal();
    }
  });
})();
