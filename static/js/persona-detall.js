(() => {
  const optionsBtn = document.getElementById("botoOpcions");
  const dropdown = document.getElementById("dropdownOpcions");
  if (optionsBtn && dropdown) {
    optionsBtn.addEventListener("click", (event) => {
      event.preventDefault();
      dropdown.style.display = dropdown.style.display === "block" ? "none" : "block";
    });
    document.addEventListener("click", (event) => {
      if (!event.target.closest(".opcions-dropdown")) {
        dropdown.style.display = "none";
      }
    });
  }

  const linkModal = document.getElementById("persona-link-modal");
  if (linkModal) {
    const form = document.getElementById("persona-link-form");
    const fieldInput = document.getElementById("persona-link-field");
    const fieldLabel = document.getElementById("persona-link-field-label");
    const openButtons = document.querySelectorAll("[data-persona-link]");
    const closeButtons = linkModal.querySelectorAll("[data-close-persona-link]");
    const archiveInput = document.getElementById("persona-link-archive-search");
    const archiveIdInput = document.getElementById("persona-link-archive-id");
    const bookInput = document.getElementById("persona-link-book-search");
    const bookIdInput = document.getElementById("persona-link-book-id");
    const registreInput = document.getElementById("persona-link-registre-search");
    const registreIdInput = document.getElementById("persona-link-registre-id");
    const registreWrap = document.getElementById("persona-link-registre-wrap");
    const submitBtn = document.getElementById("persona-link-submit");

    const updateBookApi = () => {
      if (!bookInput) {
        return;
      }
      let api = "/api/documentals/llibres/suggest";
      if (archiveIdInput && archiveIdInput.value) {
        api = `/api/documentals/llibres/suggest?arxiu_id=${encodeURIComponent(archiveIdInput.value)}`;
      }
      bookInput.dataset.api = api;
    };

    const clearBook = () => {
      if (bookInput) {
        bookInput.value = "";
      }
      if (bookIdInput) {
        bookIdInput.value = "";
      }
    };

    const clearRegistre = () => {
      if (registreInput) {
        registreInput.value = "";
        registreInput.dataset.api = "";
      }
      if (registreIdInput) {
        registreIdInput.value = "";
      }
      if (registreWrap) {
        registreWrap.style.display = "none";
      }
    };

    const updateRegistreApi = () => {
      if (!bookIdInput || !bookIdInput.value) {
        clearRegistre();
        return;
      }
      if (registreInput) {
        registreInput.dataset.api = `/api/documentals/registres/suggest?llibre_id=${encodeURIComponent(bookIdInput.value)}`;
      }
      if (registreWrap) {
        registreWrap.style.display = "";
      }
    };

    const updateSubmitState = () => {
      if (submitBtn) {
        submitBtn.disabled = !(registreIdInput && registreIdInput.value);
      }
    };

    const resetForm = () => {
      if (archiveInput) {
        archiveInput.value = "";
      }
      if (archiveIdInput) {
        archiveIdInput.value = "";
      }
      clearBook();
      clearRegistre();
      updateBookApi();
      updateSubmitState();
    };

    const openModal = (btn) => {
      if (fieldInput) {
        fieldInput.value = btn.dataset.fieldKey || "";
      }
      if (fieldLabel) {
        fieldLabel.textContent = btn.dataset.fieldLabel || "—";
      }
      resetForm();
      linkModal.classList.add("is-open");
      if (archiveInput) {
        archiveInput.focus();
      }
    };

    const closeModal = () => {
      linkModal.classList.remove("is-open");
    };

    openButtons.forEach((btn) => {
      btn.addEventListener("click", () => openModal(btn));
    });
    closeButtons.forEach((btn) => {
      btn.addEventListener("click", closeModal);
    });
    linkModal.addEventListener("click", (event) => {
      if (event.target === linkModal) {
        closeModal();
      }
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        closeModal();
      }
    });

    if (archiveIdInput) {
      archiveIdInput.addEventListener("change", () => {
        updateBookApi();
        clearBook();
        clearRegistre();
        updateSubmitState();
      });
    }

    if (bookIdInput) {
      bookIdInput.addEventListener("change", () => {
        updateRegistreApi();
        updateSubmitState();
      });
    }

    if (registreIdInput) {
      registreIdInput.addEventListener("change", updateSubmitState);
    }

    if (form) {
      form.addEventListener("submit", (event) => {
        if (!registreIdInput || !registreIdInput.value) {
          event.preventDefault();
          if (registreInput) {
            registreInput.focus();
          }
        }
      });
    }
  }

  const markState = document.getElementById("persona-mark-state");
  const modal = document.getElementById("modalMarcarPersona");
  const openBtn = document.querySelector(".btn-marcar");
  const closeBtns = document.querySelectorAll('[data-modal-close="modalMarcarPersona"]');
  if (!markState || !modal) {
    return;
  }
  const markType = modal.querySelector("#persona-mark-type");
  const markPublic = modal.querySelector("#persona-mark-public");
  const markSave = modal.querySelector("#persona-mark-save");
  const markClear = modal.querySelector("#persona-mark-clear");
  const personaID = parseInt(markState.dataset.personaId || "0", 10);
  const csrfToken = markState.dataset.csrf || "";
  const basePath = markState.dataset.basePath || "/persones";

  const openModal = () => {
    const existingType = markState.dataset.markType || "";
    const existingPublic = markState.dataset.markPublic !== "0";
    const own = markState.dataset.markOwn === "1";
    if (markType) {
      markType.value = existingType;
    }
    if (markPublic) {
      markPublic.checked = existingType ? existingPublic : true;
    }
    if (markClear) {
      markClear.disabled = !own;
    }
    modal.classList.add("is-open");
  };

  const closeModal = () => {
    modal.classList.remove("is-open");
  };

  if (openBtn) {
    openBtn.addEventListener("click", (event) => {
      event.preventDefault();
      if (!personaID) {
        return;
      }
      openModal();
    });
  }
  closeBtns.forEach((btn) => {
    btn.addEventListener("click", closeModal);
  });

  if (markSave) {
    markSave.addEventListener("click", () => {
      if (!personaID) {
        return;
      }
      const typeValue = markType ? markType.value : "";
      if (!typeValue) {
        return;
      }
      const body = new URLSearchParams();
      body.set("csrf_token", csrfToken);
      body.set("type", typeValue);
      body.set("public", markPublic && markPublic.checked ? "1" : "0");
      fetch(`${basePath}/${personaID}/marcar`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: body.toString(),
        credentials: "same-origin",
      })
        .then((response) => {
          if (!response.ok) {
            throw new Error("mark_failed");
          }
          return response.json();
        })
        .then((data) => {
          if (data && data.ok) {
            markState.dataset.markType = data.type || "";
            markState.dataset.markPublic = data.is_public ? "1" : "0";
            markState.dataset.markOwn = "1";
            closeModal();
          }
        })
        .catch(() => {});
    });
  }

  if (markClear) {
    markClear.addEventListener("click", () => {
      if (!personaID) {
        return;
      }
      if (markState.dataset.markOwn !== "1") {
        closeModal();
        return;
      }
      const body = new URLSearchParams();
      body.set("csrf_token", csrfToken);
      fetch(`${basePath}/${personaID}/desmarcar`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: body.toString(),
        credentials: "same-origin",
      })
        .then((response) => {
          if (!response.ok) {
            throw new Error("mark_clear_failed");
          }
          return response.json();
        })
        .then((data) => {
          if (data && data.ok) {
            markState.dataset.markType = "";
            markState.dataset.markPublic = "0";
            markState.dataset.markOwn = "0";
            closeModal();
          }
        })
        .catch(() => {});
    });
  }
})();
