(() => {
  const labelsEl = document.getElementById("policy-perm-labels");
  let permLabels = {};
  if (labelsEl) {
    try {
      permLabels = JSON.parse(labelsEl.textContent || "{}");
    } catch (_) {
      permLabels = {};
    }
  }

  document.querySelectorAll(".perms-badges").forEach((el) => {
    const raw = el.getAttribute("data-perms") || "{}";
    let data = {};
    try {
      data = JSON.parse(raw);
    } catch (_) {
      return;
    }
    el.innerHTML = "";
    Object.entries(data).forEach(([k, v]) => {
      if (!v) return;
      const span = document.createElement("span");
      span.className = "perms-pill";
      span.textContent = permLabels[k] || k;
      el.appendChild(span);
    });
    if (!el.childElementCount) {
      const span = document.createElement("span");
      span.className = "code-fallback";
      span.textContent = "—";
      el.appendChild(span);
    }
  });
})();
