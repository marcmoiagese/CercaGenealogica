/*
  Arbre genealògic vectorial (SVG) amb pan/zoom, col·lapse/desplegament i fitxa lateral.
*/

(function () {
  "use strict";

  const i18nEl = document.getElementById("tree-i18n");
  let I18N = {};
  if (i18nEl) {
    try {
      I18N = JSON.parse(i18nEl.textContent || "{}");
    } catch (_) {
      I18N = {};
    }
  }
  const t = (key, vars) => {
    let str = (I18N && I18N[key]) || key;
    if (vars) {
      Object.keys(vars).forEach((k) => {
        str = str.replaceAll(`{${k}}`, vars[k]);
      });
    }
    return str;
  };

  if (typeof window.d3 === "undefined") {
    console.error(t("tree.error.d3"));
    return;
  }

  const NODE_W = 200;
  const NODE_H = 74;
  const STRIP_W = 6;
  const GAP_X = 70;
  const GAP_Y = 18;

  const svg = d3.select("#treeSvg");
  const drawer = document.getElementById("personDrawer");
  const drawerName = document.getElementById("drawerName");
  const drawerSub = document.getElementById("drawerSub");
  const drawerBody = document.getElementById("drawerBody");
  const drawerClose = document.getElementById("drawerClose");

  const btnZoomIn = document.getElementById("zoomIn");
  const btnZoomOut = document.getElementById("zoomOut");
  const btnFit = document.getElementById("fitView");
  const btnToggleDrawer = document.getElementById("toggleDrawer");
  const generationsSelect = document.getElementById("generacionsSelect");
  const viewPersonBtn = document.getElementById("viewPersonBtn");
  const hasProfileBase = Object.prototype.hasOwnProperty.call(window, "treeProfileBase");
  const profileBaseRaw =
    hasProfileBase && typeof window.treeProfileBase === "string"
      ? window.treeProfileBase.trim()
      : "";
  const profileBase = hasProfileBase ? profileBaseRaw : "/persones";
  const canOpenProfile = profileBase !== "";

  const familyDataRef = Array.isArray(window.familyData) ? window.familyData : [];
  const familyLinksRef = Array.isArray(window.familyLinks) ? window.familyLinks : [];

  const persons = new Map(familyDataRef.map((p) => [p.id, p]));
  const links = familyLinksRef;

  const linkMap = new Map();
  const linkKeySet = new Set();
  const expandCache = new Set();
  const expandInFlight = new Set();
  const EXPAND_GENS = 2;

  function linkKey(child, fatherId, motherId) {
    return `${child || ""}:${fatherId || ""}:${motherId || ""}`;
  }

  function addPerson(person) {
    if (!person || person.id == null) return false;
    if (persons.has(person.id)) return false;
    persons.set(person.id, person);
    familyDataRef.push(person);
    return true;
  }

  function upsertParentMap(child, fatherId, motherId) {
    if (child == null) return;
    const key = String(child);
    const existing = linkMap.get(key);
    if (!existing) {
      linkMap.set(key, { fatherId: fatherId ?? null, motherId: motherId ?? null });
      return;
    }
    if (fatherId && !existing.fatherId) existing.fatherId = fatherId;
    if (motherId && !existing.motherId) existing.motherId = motherId;
  }

  function addLinkRecord(record) {
    if (!record) return false;
    const child = record.child ?? record.Child ?? record.id ?? null;
    if (child == null) return false;
    const fatherId = record.father ?? record.Father ?? null;
    const motherId = record.mother ?? record.Mother ?? null;
    const key = linkKey(child, fatherId, motherId);
    if (linkKeySet.has(key)) {
      upsertParentMap(child, fatherId, motherId);
      return false;
    }
    linkKeySet.add(key);
    links.push({ child, father: fatherId ?? null, mother: motherId ?? null });
    upsertParentMap(child, fatherId, motherId);
    return true;
  }

  function markNoParents(child) {
    if (child == null) return;
    const key = String(child);
    if (!linkMap.has(key)) {
      linkMap.set(key, { fatherId: null, motherId: null });
    }
  }

  function syncDatasetInfo() {
    try {
      const info = document.getElementById("datasetInfo");
      window.__DATASET_STATS = { people: persons.size, links: linkKeySet.size };
      if (info && window.__DATASET_STATS) {
        info.textContent = t("tree.dataset", {
          people: window.__DATASET_STATS.people || 0,
          links: window.__DATASET_STATS.links || 0,
        });
      }
    } catch (_) {}
  }

  function mergeDataset(people, newLinks) {
    let addedPeople = 0;
    let addedLinks = 0;
    if (Array.isArray(people)) {
      for (const p of people) {
        if (addPerson(p)) addedPeople += 1;
      }
    }
    if (Array.isArray(newLinks)) {
      for (const l of newLinks) {
        if (addLinkRecord(l)) addedLinks += 1;
      }
    }
    if (addedPeople || addedLinks) syncDatasetInfo();
    return { addedPeople, addedLinks };
  }

  const initialLinks = links.slice();
  links.length = 0;
  for (const r of initialLinks) {
    addLinkRecord(r);
  }

  syncDatasetInfo();

  function initRootSelect() {
    const sel = document.getElementById("rootSelect");
    if (!sel || !Array.isArray(window.familyData)) return;

    if (!sel.__filled) {
      const data = window.familyData
        .slice()
        .sort((a, b) => (a.name || "").localeCompare(b.name || "", "ca"));
      sel.innerHTML = "";
      for (const p of data) {
        if (!p || p.hidden) continue;
        const opt = document.createElement("option");
        opt.value = String(p.id);
        opt.textContent = `${p.name || "#" + p.id}`;
        sel.appendChild(opt);
      }
      sel.__filled = true;

      sel.addEventListener("change", () => {
        const newId = Number(sel.value);
        if (!newId) return;
        window.rootPersonId = newId;
        treeDataRoot = null;
        expandedIds.clear();
        selectedId = null;
        render({ fit: true });
      });
    }

    try {
      sel.value = String(window.rootPersonId || "");
    } catch (_) {}
  }

  function setVisibleInfo(count) {
    try {
      const el = document.getElementById("visibleInfo");
      if (el) el.textContent = t("tree.visible", { count: count || 0 });
    } catch (_) {}
  }

  function isHiddenId(id) {
    if (id == null) return false;
    const p = persons.get(id);
    return !!(p && p.hidden);
  }

  function getParentIds(childId) {
    return linkMap.get(String(childId)) || { fatherId: null, motherId: null };
  }

  function sexClass(sex) {
    if (sex === 0) return "is-male";
    if (sex === 1) return "is-female";
    return "is-unknown";
  }

  function truncate(str, max) {
    const s = String(str || "").trim();
    if (s.length <= max) return s;
    return s.slice(0, Math.max(0, max - 1)) + "…";
  }

  function fmtLifespan(p) {
    const b = (p.birth || "").trim();
    const d = (p.death || "").trim();
    if (!b && !d) return "";
    if (b && d) return `${b} – ${d}`;
    if (b) return `${b} –`;
    return `– ${d}`;
  }

  function escapeHtml(str) {
    return String(str)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function isNumericId(id) {
    return id != null && /^\d+$/.test(String(id));
  }

  function buildProfileURL(id) {
    const base = profileBase.endsWith("/") ? profileBase.slice(0, -1) : profileBase;
    return `${base}/${id}`;
  }

  function setViewPersonTarget(personLike) {
    if (!viewPersonBtn) return;
    if (!canOpenProfile) {
      viewPersonBtn.removeAttribute("href");
      viewPersonBtn.classList.add("is-disabled");
      viewPersonBtn.setAttribute("aria-disabled", "true");
      viewPersonBtn.style.display = "none";
      return;
    }
    const rootId = window.rootPersonId;
    if (personLike && isNumericId(personLike.id)) {
      viewPersonBtn.href = buildProfileURL(personLike.id);
      viewPersonBtn.classList.remove("is-disabled");
      viewPersonBtn.setAttribute("aria-disabled", "false");
      return;
    }
    if (rootId && isNumericId(rootId)) {
      viewPersonBtn.href = buildProfileURL(rootId);
      viewPersonBtn.classList.remove("is-disabled");
      viewPersonBtn.setAttribute("aria-disabled", "false");
      return;
    }
    viewPersonBtn.removeAttribute("href");
    viewPersonBtn.classList.add("is-disabled");
    viewPersonBtn.setAttribute("aria-disabled", "true");
  }

  let drawerEnabled = true;
  let lastSelectedPersonLike = null;

  function openDrawer(personLike) {
    lastSelectedPersonLike = personLike || null;

    if (!drawerEnabled) return;

    const p = personLike || {};
    setViewPersonTarget(p);
    drawer.classList.add("is-open");
    drawer.setAttribute("aria-hidden", "false");

    drawerName.textContent = p.name || t("tree.unknown.name");
    const subtitleBits = [];
    if (p.occupation) subtitleBits.push(p.occupation);
    const life = fmtLifespan(p);
    if (life) subtitleBits.push(life);
    drawerSub.textContent = subtitleBits.join(" · ");

    const rows = [
      [t("tree.drawer.birth"), p.birth || ""],
      [t("tree.drawer.birth_place"), p.birth_place || ""],
      [t("tree.drawer.death"), p.death || ""],
      [t("tree.drawer.death_place"), p.death_place || ""],
    ].filter(([, v]) => (v || "").trim() !== "");

    const actionLink = canOpenProfile && isNumericId(p.id)
      ? `<div class="drawer-actions"><a class="drawer-link" href="${buildProfileURL(p.id)}">${t("tree.drawer.open_profile")}</a></div>`
      : "";

    if (rows.length === 0) {
      drawerBody.innerHTML = '<div class="drawer-empty">' + t("tree.drawer.empty") + "</div>" + actionLink;
      return;
    }

    drawerBody.innerHTML = `
      <div class="drawer-block">
        <div class="block-title">${t("tree.drawer.section")}</div>
        <div class="block-content">
          <div class="drawer-kv">
            ${rows
              .map(
                ([k, v]) => `
              <div class="kv">
                <div class="k">${k}</div>
                <div class="v">${escapeHtml(v)}</div>
              </div>
            `
              )
              .join("")}
          </div>
        </div>
      </div>
      ${actionLink}
    `;
  }

  function closeDrawer() {
    drawer.classList.remove("is-open");
    drawer.setAttribute("aria-hidden", "true");
  }

  function setDrawerEnabled(enabled) {
    drawerEnabled = !!enabled;
    btnToggleDrawer?.classList.toggle("is-off", !drawerEnabled);
    drawer?.classList.toggle("is-disabled", !drawerEnabled);

    if (!drawerEnabled) {
      closeDrawer();
      return;
    }

    if (lastSelectedPersonLike) openDrawer(lastSelectedPersonLike);
  }

  function mkPlaceholder(kind, childId) {
    return {
      id: `ph-${kind}-${childId}`,
      person: {
        id: `ph-${kind}-${childId}`,
        name: kind === "father" ? t("tree.placeholder.father") : t("tree.placeholder.mother"),
        sex: kind === "father" ? 0 : 1,
        placeholder: true,
      },
      placeholder: true,
      parentIds: { fatherId: null, motherId: null },
      children: null,
      _children: null,
    };
  }

  const nodeById = new Map();
  function getOrCreateNode(personId) {
    const key = String(personId);
    if (nodeById.has(key)) return nodeById.get(key);

    const p = persons.get(personId) || { id: personId, name: t("tree.unknown.person"), sex: 2 };
    const node = {
      id: personId,
      person: p,
      placeholder: false,
      parentIds: getParentIds(personId),
      children: null,
      _children: null,
    };
    nodeById.set(key, node);
    return node;
  }

  function hasExpandable(node) {
    if (!node || node.placeholder) return false;
    const { fatherId, motherId } = node.parentIds || {};
    if ((node.children && node.children.length) || (node._children && node._children.length)) return true;
    if (linkMap.has(String(node.id))) {
      return !!((fatherId && !isHiddenId(fatherId)) || (motherId && !isHiddenId(motherId)));
    }
    return true;
  }

  async function fetchAncestorsFor(personId, gens) {
    const key = `${personId}:${gens}`;
    if (expandCache.has(key) || expandInFlight.has(key)) return false;
    expandInFlight.add(key);
    try {
      const url = `/api/arbre/expand?person_id=${encodeURIComponent(personId)}&gens=${gens}&mode=ancestors`;
      const res = await fetch(url, { credentials: "same-origin" });
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      const payload = await res.json();
      const people = payload.people || payload.familyData || [];
      const newLinks = payload.links || payload.familyLinks || [];
      mergeDataset(people, newLinks);
      if (!linkMap.has(String(personId))) markNoParents(personId);
      expandCache.add(key);
      return true;
    } catch (err) {
      console.error(t("tree.error.expand"), err);
      expandCache.add(key);
      return false;
    } finally {
      expandInFlight.delete(key);
    }
  }

  function ensureChildrenLoaded(node) {
    if (!node || node.placeholder) return;
    if (node.children || node._children) return;

    const { fatherId, motherId } = node.parentIds || { fatherId: null, motherId: null };
    const fOk = fatherId && !isHiddenId(fatherId);
    const mOk = motherId && !isHiddenId(motherId);

    if (!fOk && !mOk) {
      node.children = [];
      return;
    }

    const children = [];
    if (fOk) children.push(getOrCreateNode(fatherId));
    else children.push(mkPlaceholder("father", node.id));

    if (mOk) children.push(getOrCreateNode(motherId));
    else children.push(mkPlaceholder("mother", node.id));

    node.children = children;
  }

  function collapse(node) {
    if (!node || !node.children || node.children.length === 0) return;
    node._children = node.children;
    node.children = null;
  }

  function expand(node) {
    if (!node) return;
    if (node._children) {
      node.children = node._children;
      node._children = null;
      return;
    }
    if (!node.children) ensureChildrenLoaded(node);
  }

  function toggle(node) {
    if (!node || node.placeholder) return;
    if (node.children && node.children.length) {
      collapse(node);
      return;
    }
    if (!linkMap.has(String(node.id))) {
      fetchAncestorsFor(node.id, EXPAND_GENS).then((updated) => {
        if (updated) {
          node.parentIds = getParentIds(node.id);
          node.children = null;
          node._children = null;
          ensureChildrenLoaded(node);
          render({ fit: false });
        }
      });
    }
    expand(node);
  }

  function buildInitialTree(rootId, initialDepth) {
    nodeById.clear();

    const root = getOrCreateNode(rootId);

    function walk(n, depth) {
      if (!n || n.placeholder) return;
      if (depth + 1 < initialDepth) {
        ensureChildrenLoaded(n);
        for (const c of n.children || []) walk(c, depth + 1);
      } else {
        if (n.children && n.children.length) collapse(n);
      }
    }

    walk(root, 0);
    return root;
  }

  let gRoot;
  let gLinks;
  let gNodes;
  let zoomBehavior;

  let currentSelectionId = null;
  let lastHierarchyRoot = null;
  let didInitialFit = false;

  function setupSvgLayers() {
    svg.selectAll("*").remove();

    gRoot = svg.append("g").attr("class", "tree-root");
    gLinks = gRoot.append("g").attr("class", "tree-links");
    gNodes = gRoot.append("g").attr("class", "tree-nodes");

    zoomBehavior = d3.zoom()
      .scaleExtent([0.25, 2.75])
      .on("zoom", (event) => gRoot.attr("transform", event.transform));

    svg.call(zoomBehavior);
  }

  function fitToView(root) {
    if (!root) return;

    const nodes = root.descendants();
    if (!nodes.length) return;

    const extraRight = drawerEnabled ? 420 : 80;

    const minX = d3.min(nodes, (d) => d.x) - (NODE_H / 2 + 40);
    const maxX = d3.max(nodes, (d) => d.x) + (NODE_H / 2 + 40);
    const minY = d3.min(nodes, (d) => d.y) - (NODE_W / 2 + 60);
    const maxY = d3.max(nodes, (d) => d.y) + (NODE_W / 2 + extraRight);

    const width = maxY - minY;
    const height = maxX - minX;

    const svgEl = svg.node();
    const bbox = svgEl.getBoundingClientRect();
    const vw = Math.max(1, bbox.width);
    const vh = Math.max(1, bbox.height);

    const scale = Math.min(2.2, Math.max(0.25, 0.92 * Math.min(vw / width, vh / height)));
    const tx = vw / 2 - scale * (minY + width / 2);
    const ty = vh / 2 - scale * (minX + height / 2);

    svg.transition()
      .duration(220)
      .call(zoomBehavior.transform, d3.zoomIdentity.translate(tx, ty).scale(scale));
  }

  function highlightSelection() {
    gNodes.selectAll("g.tree-node")
      .classed("is-selected", (d) => String(d.data.id) === currentSelectionId);
  }

  let treeDataRoot = null;
  const expandedIds = new Set();
  let selectedId = null;

  function computeHierarchy() {
    const root = d3.hierarchy(treeDataRoot, (d) => d.children);
    const tree = d3.tree().nodeSize([NODE_H + GAP_Y, NODE_W + GAP_X]);
    tree(root);
    lastHierarchyRoot = root;
    return root;
  }

  function render({ fit = false } = {}) {
    const rootId = window.rootPersonId;
    if (!rootId) {
      console.error(t("tree.error.root"));
      return;
    }

    if (!treeDataRoot) {
      const initialDepth = Math.max(1, Number(generationsSelect?.value || 3));
      treeDataRoot = buildInitialTree(rootId, initialDepth);
      didInitialFit = false;
    }

    const root = computeHierarchy();

    const nodeByIdH = new Map();
    root.descendants().forEach((n) => nodeByIdH.set(n.data.id, n));

    const linkItems = [];
    for (const child of root.descendants()) {
      if (!child || !child.data || child.data.placeholder) continue;

      const { fatherId, motherId } = child.data.parentIds || { fatherId: null, motherId: null };

      const father = fatherId != null ? nodeByIdH.get(fatherId) : null;
      const mother = motherId != null ? nodeByIdH.get(motherId) : null;

      const childRightH = child.y + NODE_W / 2;
      const childV = child.x;

      const fatherLeftH = father ? father.y - NODE_W / 2 : null;
      const motherLeftH = mother ? mother.y - NODE_W / 2 : null;

      if (father && mother) {
        const stemH = Math.min(fatherLeftH, motherLeftH) - 16;
        const topV = Math.min(father.x, mother.x);
        const botV = Math.max(father.x, mother.x);
        const midV = (topV + botV) / 2;

        const d = [
          `M ${childRightH} ${childV}`,
          `L ${stemH} ${childV}`,
          `L ${stemH} ${midV}`,
          `M ${stemH} ${topV}`,
          `L ${stemH} ${botV}`,
          `M ${stemH} ${father.x}`,
          `L ${fatherLeftH} ${father.x}`,
          `M ${stemH} ${mother.x}`,
          `L ${motherLeftH} ${mother.x}`,
        ].join(" ");

        linkItems.push({ key: `cpl:${child.data.id}`, d });
      } else if (father || mother) {
        const p = father || mother;
        const pLeftH = p.y - NODE_W / 2;
        const stemH = pLeftH - 16;

        const d = [
          `M ${childRightH} ${childV}`,
          `L ${stemH} ${childV}`,
          `L ${stemH} ${p.x}`,
          `L ${pLeftH} ${p.x}`,
        ].join(" ");

        linkItems.push({ key: `one:${child.data.id}`, d });
      }
    }

    gLinks.selectAll("path")
      .data(linkItems, (d) => d.key)
      .join(
        (enter) => enter.append("path").attr("class", "tree-link").attr("d", (d) => d.d),
        (update) => update.attr("d", (d) => d.d),
        (exit) => exit.remove()
      );

    const nodesSel = gNodes.selectAll("g.tree-node")
      .data(root.descendants(), (d) => String(d.data.id));

    const nodesEnter = nodesSel.enter().append("g")
      .attr("class", (d) => {
        const node = d.data;
        return `tree-node ${sexClass(node.person.sex)} ${node.placeholder ? "is-placeholder" : ""} ${hasExpandable(node) ? "has-expand" : ""}`;
      })
      .attr("transform", (d) => `translate(${d.y},${d.x})`)
      .style("cursor", "pointer");

    nodesEnter.on("click", (event, d) => {
      event.stopPropagation();

      currentSelectionId = String(d.data.id);
      highlightSelection();

      if (d.data.placeholder) {
        openDrawer({
          name: d.data.person.name,
          occupation: "",
          birth: "",
          death: "",
          birth_place: "",
          death_place: "",
        });
        return;
      }

      openDrawer(d.data.person);
      toggle(d.data);
      render({ fit: false });
    });

    nodesEnter.append("rect")
      .attr("class", "node-card")
      .attr("x", -NODE_W / 2)
      .attr("y", -NODE_H / 2)
      .attr("rx", 10)
      .attr("ry", 10)
      .attr("width", NODE_W)
      .attr("height", NODE_H);

    nodesEnter.append("rect")
      .attr("class", "node-strip")
      .attr("x", -NODE_W / 2)
      .attr("y", -NODE_H / 2)
      .attr("rx", 10)
      .attr("ry", 10)
      .attr("width", STRIP_W)
      .attr("height", NODE_H);

    nodesEnter.append("text")
      .attr("class", "node-glyph")
      .attr("x", -NODE_W / 2 + STRIP_W + 14)
      .attr("y", -NODE_H / 2 + 26)
      .text((d) => {
        const node = d.data;
        const sex = node.person.sex;
        if (node.placeholder) return "+";
        if (sex === 0) return "♂";
        if (sex === 1) return "♀";
        return "?";
      });

    nodesEnter.append("text")
      .attr("class", "node-name")
      .attr("x", -NODE_W / 2 + STRIP_W + 36)
      .attr("y", -NODE_H / 2 + 26)
      .text((d) => truncate(d.data.person.name, 24));

    nodesEnter.append("text")
      .attr("class", "node-dates")
      .attr("x", -NODE_W / 2 + STRIP_W + 36)
      .attr("y", -NODE_H / 2 + 48)
      .text((d) => {
        const node = d.data;
        if (node.placeholder) return "";
        const life = fmtLifespan(node.person);
        return truncate(life, 28);
      });

    nodesSel.merge(nodesEnter)
      .attr("class", (d) => {
        const node = d.data;
        return `tree-node ${sexClass(node.person.sex)} ${node.placeholder ? "is-placeholder" : ""} ${hasExpandable(node) ? "has-expand" : ""} ${String(node.id) === currentSelectionId ? "is-selected" : ""}`;
      })
      .transition()
      .duration(180)
      .attr("transform", (d) => `translate(${d.y},${d.x})`);

    nodesSel.exit().remove();

    svg.on("click", () => {
      currentSelectionId = null;
      highlightSelection();
      closeDrawer();
      setViewPersonTarget({ id: window.rootPersonId });
    });

    if (fit || !didInitialFit) {
      fitToView(root);
      didInitialFit = true;
    }

    try {
      const visibleCount = root.descendants().filter((d) => d && d.data && !isHiddenId(d.data.id)).length;
      setVisibleInfo(visibleCount);
    } catch (_) {}
  }

  function bindUi() {
    drawerClose?.addEventListener("click", (e) => {
      e.preventDefault();
      closeDrawer();
    });

    generationsSelect?.addEventListener("change", () => {
      treeDataRoot = null;
      render({ fit: true });
    });

    btnZoomIn?.addEventListener("click", () => {
      svg.transition().duration(120).call(zoomBehavior.scaleBy, 1.2);
    });

    btnZoomOut?.addEventListener("click", () => {
      svg.transition().duration(120).call(zoomBehavior.scaleBy, 1 / 1.2);
    });

    btnFit?.addEventListener("click", () => fitToView(lastHierarchyRoot));

    btnToggleDrawer?.addEventListener("click", () => {
      setDrawerEnabled(!drawerEnabled);
      fitToView(lastHierarchyRoot);
    });

    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape") closeDrawer();
    });
  }

  setupSvgLayers();
  bindUi();
  initRootSelect();
  setDrawerEnabled(true);
  setViewPersonTarget({ id: window.rootPersonId });
  render({ fit: true });
})();
