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

  const markState = document.getElementById("cognom-mark-state");
  const modal = document.getElementById("modalMarcarCognom");
  const openBtn = document.querySelector(".btn-marcar");
  const closeBtns = document.querySelectorAll('[data-modal-close="modalMarcarCognom"]');
  if (!markState || !modal) {
    return;
  }
  const markType = modal.querySelector("#cognom-mark-type");
  const markPublic = modal.querySelector("#cognom-mark-public");
  const markSave = modal.querySelector("#cognom-mark-save");
  const markClear = modal.querySelector("#cognom-mark-clear");
  const cognomID = parseInt(markState.dataset.cognomId || "0", 10);
  const csrfToken = markState.dataset.csrf || "";

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
      if (!cognomID) {
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
      if (!cognomID) {
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
      fetch(`/cognoms/${cognomID}/marcar`, {
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
      if (!cognomID) {
        return;
      }
      if (markState.dataset.markOwn !== "1") {
        closeModal();
        return;
      }
      const body = new URLSearchParams();
      body.set("csrf_token", csrfToken);
      fetch(`/cognoms/${cognomID}/desmarcar`, {
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
