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

  const markState = document.getElementById("event-mark-state");
  const modal = document.getElementById("modalMarcarEvent");
  const openBtn = document.querySelector(".btn-marcar");
  const closeBtns = document.querySelectorAll('[data-modal-close="modalMarcarEvent"]');
  if (!markState || !modal) {
    return;
  }
  const markType = modal.querySelector("#event-mark-type");
  const markPublic = modal.querySelector("#event-mark-public");
  const markSave = modal.querySelector("#event-mark-save");
  const markClear = modal.querySelector("#event-mark-clear");
  const eventID = parseInt(markState.dataset.eventId || "0", 10);
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
    modal.classList.add("active");
  };

  const closeModal = () => {
    modal.classList.remove("active");
  };

  if (openBtn) {
    openBtn.addEventListener("click", (event) => {
      event.preventDefault();
      openModal();
    });
  }
  closeBtns.forEach((btn) => {
    btn.addEventListener("click", closeModal);
  });
  modal.addEventListener("click", (event) => {
    if (event.target === modal) {
      closeModal();
    }
  });

  if (markSave) {
    markSave.addEventListener("click", () => {
      if (!eventID || !markType) {
        return;
      }
      const typeValue = markType.value || "";
      if (!typeValue) {
        return;
      }
      const body = new URLSearchParams();
      body.set("type", typeValue);
      body.set("public", markPublic && markPublic.checked ? "1" : "0");
      fetch(`/historia/events/${eventID}/marcar`, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          "X-CSRF-Token": csrfToken,
        },
        body: body.toString(),
      })
        .then((resp) => resp.json())
        .then((data) => {
          if (!data || !data.ok) {
            throw new Error("mark_failed");
          }
          markState.dataset.markType = data.type || "";
          markState.dataset.markPublic = data.is_public ? "1" : "0";
          markState.dataset.markOwn = "1";
          closeModal();
        })
        .catch(() => {});
    });
  }
  if (markClear) {
    markClear.addEventListener("click", () => {
      if (!eventID) {
        return;
      }
      if (markState.dataset.markOwn !== "1") {
        return;
      }
      fetch(`/historia/events/${eventID}/desmarcar`, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          "X-CSRF-Token": csrfToken,
        },
        body: "",
      })
        .then((resp) => resp.json())
        .then((data) => {
          if (!data || !data.ok) {
            throw new Error("mark_clear_failed");
          }
          markState.dataset.markType = "";
          markState.dataset.markPublic = "0";
          markState.dataset.markOwn = "0";
          closeModal();
        })
        .catch(() => {});
    });
  }
})();
