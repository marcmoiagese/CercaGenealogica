(() => {
  const timeline = document.getElementById("history-timeline");
  const checks = Array.from(document.querySelectorAll(".revisio-check"));
  const countEl = document.getElementById("history-selected-count");
  const compareBtn = document.getElementById("history-compare");
  const compareOne = document.querySelectorAll(".history-compare-one");
  const revertButtons = document.querySelectorAll(".history-revert");
  const modal = document.getElementById("revert-modal");
  const modalClose = document.querySelectorAll('[data-modal-close="revert-modal"]');
  const revertChangeInput = document.getElementById("revert-change-id");
  const historyUrl = timeline ? timeline.dataset.historyUrl || "" : "";

  if (!timeline) {
    return;
  }

  const updateCount = () => {
    const selected = checks.filter((el) => el.checked && el.dataset.hasSnapshot === "1");
    if (countEl) {
      countEl.textContent = String(selected.length);
    }
    if (compareBtn) {
      compareBtn.disabled = selected.length < 2;
    }
  };

  checks.forEach((el) => el.addEventListener("change", updateCount));
  updateCount();

  if (compareBtn) {
    compareBtn.addEventListener("click", () => {
      const selected = checks
        .filter((el) => el.checked && el.dataset.hasSnapshot === "1")
        .map((el) => el.dataset.changeId);
      if (selected.length < 2 || !historyUrl) {
        return;
      }
      const params = new URLSearchParams();
      params.set("compare", selected.join(","));
      window.location.href = `${historyUrl}?${params.toString()}`;
    });
  }

  compareOne.forEach((btn) => {
    btn.addEventListener("click", () => {
      if (!historyUrl) {
        return;
      }
      const compareId = btn.dataset.compareId;
      if (!compareId) {
        return;
      }
      const params = new URLSearchParams();
      params.set("compare", `current,${compareId}`);
      window.location.href = `${historyUrl}?${params.toString()}`;
    });
  });

  revertButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      if (revertChangeInput) {
        revertChangeInput.value = btn.dataset.changeId || "";
      }
      if (modal) {
        modal.classList.add("is-open");
      }
    });
  });

  modalClose.forEach((btn) => {
    btn.addEventListener("click", () => {
      if (modal) {
        modal.classList.remove("is-open");
      }
    });
  });
})();
