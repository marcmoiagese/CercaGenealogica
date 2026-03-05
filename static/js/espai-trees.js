(() => {
  const modal = document.getElementById("espai-tree-edit-modal");
  if (modal) {
    const closeUrl = modal.getAttribute("data-close-url") || "/espai/arbres";
    const closeButtons = modal.querySelectorAll('[data-modal-close="espai-tree-edit-modal"]');
    const closeModal = () => {
      window.location.href = closeUrl;
    };
    closeButtons.forEach((btn) => {
      btn.addEventListener("click", closeModal);
    });
    modal.addEventListener("click", (event) => {
      if (event.target === modal) {
        closeModal();
      }
    });
  }

  const table = document.querySelector(".espai-trees-table");
  if (!table) {
    return;
  }

  const labelsEl = document.getElementById("espai-status-labels");
  let statusLabels = {};
  if (labelsEl) {
    try {
      statusLabels = JSON.parse(labelsEl.textContent || "{}");
    } catch (_) {
      statusLabels = {};
    }
  }

  const activeStatuses = new Set(["queued", "parsing", "normalizing", "persisted"]);
  const rows = new Map();
  table.querySelectorAll("tbody tr[data-tree-id]").forEach((row) => {
    rows.set(row.getAttribute("data-tree-id"), row);
  });

  const updateRow = (row, status) => {
    const badge = row.querySelector(".espai-import-state");
    const icon = row.querySelector(".espai-tree-sync-icon");
    const normalized = (status || "").toLowerCase();
    if (badge) {
      if (activeStatuses.has(normalized)) {
        badge.textContent = statusLabels[normalized] || normalized;
        badge.className = `espai-status espai-status--${normalized} espai-import-state`;
        badge.style.display = "inline-flex";
      } else {
        badge.textContent = "";
        badge.style.display = "none";
      }
    }
    if (icon) {
      icon.classList.toggle("is-spinning", activeStatuses.has(normalized));
    }
    row.setAttribute("data-import-status", normalized);
  };

  rows.forEach((row) => {
    updateRow(row, row.getAttribute("data-import-status"));
  });

  const poll = async () => {
    const hasActive = Array.from(rows.values()).some((row) =>
      activeStatuses.has((row.getAttribute("data-import-status") || "").toLowerCase())
    );
    if (!hasActive) return;
    try {
      const resp = await fetch("/api/espai/gedcom/imports", { credentials: "same-origin" });
      if (!resp.ok) {
        setTimeout(poll, 5000);
        return;
      }
      const data = await resp.json();
      if (!data || !Array.isArray(data.items)) {
        setTimeout(poll, 5000);
        return;
      }
      const latest = new Map();
      data.items.forEach((item) => {
        if (!item || !item.arbre_id) return;
        const key = String(item.arbre_id);
        if (!latest.has(key)) {
          latest.set(key, item);
        }
      });
      latest.forEach((item, key) => {
        const row = rows.get(key);
        if (row) {
          updateRow(row, item.status);
        }
      });
    } catch (_) {
    }
    setTimeout(poll, 5000);
  };

  poll();
})();
