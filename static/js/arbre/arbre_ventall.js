/* arbre_ventall.js
 * Vista "Ventall" (Fan chart) d'ancestres.
 * - Mateix dataset que la resta de vistes: window.familyData, window.familyLinks, window.rootPersonId
 * - Dibuix SVG (D3) amb pan/zoom, ajustar a pantalla, selector de generacions
 * - Clic sobre segment: selecciona i obre/actualitza la fitxa lateral
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

  // --- Dependencies
  if (typeof d3 === "undefined") {
    console.error(t("tree.error.d3"));
    return;
  }

  // --- DOM
  const svgEl = document.getElementById("fanSvg");
  const containerEl = document.getElementById("arbreShell");
  const drawerEl = document.getElementById("personDrawer");
  const drawerNameEl = document.getElementById("drawerName");
  const drawerSubEl = document.getElementById("drawerSub");
  const drawerBodyEl = document.getElementById("drawerBody");
  const drawerCloseEl = document.getElementById("drawerClose");
  const toggleDrawerBtn = document.getElementById("toggleDrawer");
  const viewPersonBtn = document.getElementById("viewPersonBtn");

  // Drawer state
  let drawerEnabled = true;
  let lastSelectedPerson = null;
  const datasetInfoEl = document.getElementById("datasetInfo");

  // Controls (mateixos ids que a Pedigree)
  const zoomInBtn = document.getElementById("zoomIn");
  const zoomOutBtn = document.getElementById("zoomOut");
  const fitBtn = document.getElementById("fitView");
  const generacionsSelect = document.getElementById("generacionsSelect");

  // --- Dataset basics
  const people = Array.isArray(window.familyData) ? window.familyData.slice() : [];
  const links = Array.isArray(window.familyLinks) ? window.familyLinks.slice() : [];
  let rootId = window.rootPersonId;

  const peopleById = new Map();
  for (let i = 0; i < people.length; i++) peopleById.set(people[i].id, people[i]);

  const parentsByChild = new Map(); // childId -> {father, mother}
  for (let j = 0; j < links.length; j++) {
    const l = links[j];
    parentsByChild.set(l.child, { father: l.father || null, mother: l.mother || null });
  }

  function getPerson(id) {
    return peopleById.get(id) || null;
  }

  function safeText(s) {
    return (s == null ? "" : String(s)).trim();
  }

  function isNumericId(id) {
    return id != null && /^\d+$/.test(String(id));
  }

  function setViewPersonTarget(personLike) {
    if (!viewPersonBtn) return;
    const rootId = window.rootPersonId;
    if (personLike && isNumericId(personLike.id)) {
      viewPersonBtn.href = `/persones/${personLike.id}`;
      viewPersonBtn.classList.remove("is-disabled");
      viewPersonBtn.setAttribute("aria-disabled", "false");
      return;
    }
    if (rootId && isNumericId(rootId)) {
      viewPersonBtn.href = `/persones/${rootId}`;
      viewPersonBtn.classList.remove("is-disabled");
      viewPersonBtn.setAttribute("aria-disabled", "false");
      return;
    }
    viewPersonBtn.removeAttribute("href");
    viewPersonBtn.classList.add("is-disabled");
    viewPersonBtn.setAttribute("aria-disabled", "true");
  }

  // IMPORTANT: al projecte fem servir:
  //   0 = Home, 1 = Dona, 2 = Desconegut
  function sexCls(sex) {
    if (sex === 0) return "is-male";
    if (sex === 1) return "is-female";
    return "is-unknown";
  }

  function formatYears(p) {
    if (!p) return "";
    const b = safeText(p.birth_year || p.birthYear || p.birth);
    const d = safeText(p.death_year || p.deathYear || p.death);

    function lastYear(x) {
      if (!x) return "";
      const m = x.match(/(\d{4})/g);
      return m ? m[m.length - 1] : "";
    }

    const by = lastYear(b);
    const dy = lastYear(d);
    if (by && dy) return by + "–" + dy;
    if (by) return t("tree.fan.birth_prefix", { year: by });
    if (dy) return t("tree.fan.death_prefix", { year: dy });
    return "";
  }

  function personSubtitle(p) {
    if (!p) return "";
    const parts = [];
    const yrs = formatYears(p);
    if (yrs) parts.push(yrs);
    const place = safeText(p.birth_place || p.birthPlace || p.birth_place_name);
    if (place) parts.push(place);
    return parts.join(" · ");
  }

  function updateDrawer(p) {
    lastSelectedPerson = p || null;
    if (!drawerEl) return;
    if (!drawerEnabled) return;

    drawerEl.setAttribute("aria-hidden", "false");
    drawerEl.classList.add("is-open");

    if (!p) {
      setViewPersonTarget({ id: rootId });
      drawerNameEl.textContent = t("tree.drawer.select_person");
      drawerSubEl.textContent = "";
      drawerBodyEl.innerHTML = '<div class="drawer-empty">' + t("tree.drawer.segment_hint") + "</div>";
      return;
    }

    setViewPersonTarget(p);
    drawerNameEl.textContent = safeText(p.name) || t("tree.unknown.name");
    drawerSubEl.textContent = personSubtitle(p);

    const rows = [];
    function row(label, value) {
      const v = safeText(value);
      if (!v) return;
      rows.push('<div class="drawer-row"><div class="drawer-label">' + label + '</div><div class="drawer-value">' + v + '</div></div>');
    }

    row(
      t("tree.drawer.birth"),
      safeText(p.birth || p.birth_date) + (safeText(p.birth_place) ? " · " + safeText(p.birth_place) : "")
    );
    row(
      t("tree.drawer.death"),
      safeText(p.death || p.death_date) + (safeText(p.death_place) ? " · " + safeText(p.death_place) : "")
    );
    row(t("tree.drawer.occupation"), safeText(p.occupation));
    row(
      t("tree.drawer.sex"),
      p.sex === 0 ? t("tree.sex.male") : p.sex === 1 ? t("tree.sex.female") : t("tree.sex.unknown")
    );

    const actionLink = isNumericId(p.id)
      ? '<div class="drawer-actions"><a class="drawer-link" href="/persones/' + p.id + '">' + t("tree.drawer.open_profile") + "</a></div>"
      : "";

    if (rows.length === 0) {
      drawerBodyEl.innerHTML = '<div class="drawer-empty">' + t("tree.drawer.no_extra") + "</div>" + actionLink;
    } else {
      drawerBodyEl.innerHTML = rows.join("") + actionLink;
    }
  }

  function closeDrawer() {
    if (!drawerEl) return;
    drawerEl.setAttribute("aria-hidden", "true");
    drawerEl.classList.remove("is-open");
  }

  if (drawerCloseEl) drawerCloseEl.addEventListener("click", closeDrawer);
  if (toggleDrawerBtn) {
    toggleDrawerBtn.addEventListener("click", function () {
      drawerEnabled = !drawerEnabled;
      if (!drawerEnabled) {
        closeDrawer();
        return;
      }
      updateDrawer(lastSelectedPerson);
    });
  }

  // --- Dataset info
  if (datasetInfoEl) {
    const stats = window.__DATASET_STATS || { people: people.length, links: links.length };
    datasetInfoEl.textContent = t("tree.dataset", {
      people: stats.people || 0,
      links: stats.links || 0,
    });
  }

  // --- Validate root
  let rootPerson = getPerson(rootId);
  if (!rootPerson) {
    console.error(t("tree.error.root"));
    lastSelectedPerson = null;
    closeDrawer();
    return;
  }

  // --- SVG setup
  const svg = d3.select(svgEl);
  svg.attr("width", "100%").attr("height", "100%");

  // Zoom/pan group
  const gZoom = svg.append("g").attr("class", "fan-zoom");
  const g = gZoom.append("g").attr("class", "fan-root");

  // Background
  g.append("rect").attr("class", "fan-bg").attr("x", -5000).attr("y", -5000).attr("width", 10000).attr("height", 10000);

  // Defs: filters + patterns
  const defs = svg.append("defs");

  const filter = defs.append("filter").attr("id", "softShadow").attr("x", "-20%")
    .attr("y", "-20%")
    .attr("width", "140%")
    .attr("height", "140%");
  filter.append("feDropShadow").attr("dx", 0).attr("dy", 2).attr("stdDeviation", 3).attr("flood-opacity", 0.18);

  const hatch = defs.append("pattern")
    .attr("id", "hatch")
    .attr("patternUnits", "userSpaceOnUse")
    .attr("width", 8)
    .attr("height", 8)
    .attr("patternTransform", "rotate(25)");
  hatch.append("rect").attr("width", 8).attr("height", 8).attr("fill", "transparent");
  hatch.append("line").attr("x1", 0).attr("y1", 0).attr("x2", 0).attr("y2", 8).attr("stroke-width", 2).attr("class", "fan-hatch-line");

  // Soft palette per generation
  const ringFills = [
    "rgba(255,255,255,0.95)",
    "rgba(244,248,255,0.95)",
    "rgba(245,255,250,0.95)",
    "rgba(255,249,244,0.95)",
    "rgba(250,245,255,0.95)",
    "rgba(245,252,255,0.95)",
    "rgba(255,245,249,0.95)",
  ];

  // Zoom behavior
  const zoom = d3
    .zoom()
    .scaleExtent([0.2, 4])
    .on("zoom", function (event) {
      gZoom.attr("transform", event.transform);
    });
  svg.call(zoom);

  function zoomBy(factor) {
    svg.transition().duration(140).call(zoom.scaleBy, factor);
  }
  if (zoomInBtn) zoomInBtn.addEventListener("click", function () { zoomBy(1.2); });
  if (zoomOutBtn) zoomOutBtn.addEventListener("click", function () { zoomBy(1 / 1.2); });

  // --- Build slots (2^g) from root
  function buildAncestorSlots(rootIdLocal, gens) {
    // Return array of levels: levels[g] = [{slot, id, person, hidden, fatherId, motherId}]
    const levels = [];
    levels.push([{ slot: 0, id: rootIdLocal, person: getPerson(rootIdLocal), hidden: false }]);

    for (let gen = 1; gen <= gens; gen++) {
      const prev = levels[gen - 1];
      const curr = [];
      for (let k = 0; k < prev.length; k++) {
        const node = prev[k];
        const p = node && node.person ? node.person : null;
        const par = p ? parentsByChild.get(p.id) : null;
        const fatherId = par ? par.father : null;
        const motherId = par ? par.mother : null;

        // if father/mother hidden, treat as empty slot
        let father = fatherId ? getPerson(fatherId) : null;
        if (father && father.hidden) father = null;
        let mother = motherId ? getPerson(motherId) : null;
        if (mother && mother.hidden) mother = null;

        // slot mapping: father left, mother right
        curr.push({
          slot: node.slot * 2,
          id: father ? father.id : null,
          person: father,
          hidden: !father,
        });
        curr.push({
          slot: node.slot * 2 + 1,
          id: mother ? mother.id : null,
          person: mother,
          hidden: !mother,
        });
      }
      levels.push(curr);
    }

    return levels;
  }

  function setVisibleInfo(count) {
    try {
      const el = document.getElementById("visibleInfo");
      if (el) el.textContent = t("tree.visible", { count: count || 0 });
    } catch (_) {}
  }

  // --- Layout/render
  function render() {
    g.selectAll(".fan-layer").remove();

    let gens = 4;
    if (generacionsSelect) gens = parseInt(generacionsSelect.value, 10) || 4;

    const bbox = containerEl ? containerEl.getBoundingClientRect() : { width: 900, height: 700 };
    const w = Math.max(600, bbox.width);
    const h = Math.max(520, bbox.height);

    // bottom-center
    const cx = w / 2;
    const cy = h * 0.88;

    // max radius
    const maxR = Math.min(w * 0.48, h * 0.82);

    // root card space
    const rootCardH = Math.max(64, Math.min(90, maxR * 0.22));
    const rootCardW = Math.max(240, Math.min(340, w * 0.42));
    const innerStart = rootCardH * 0.62;

    // ring width
    const ringW = (maxR - innerStart) / Math.max(1, gens);

    // viewBox
    svg.attr("viewBox", "0 0 " + w + " " + h);

    const layer = g.append("g").attr("class", "fan-layer").attr("transform", "translate(" + cx + "," + cy + ")");

    // background radial gradient
    const gradId = "bgRadial";
    defs.select("#" + gradId).remove();
    const rg = defs.append("radialGradient").attr("id", gradId).attr("cx", "50%").attr("cy", "50%").attr("r", "70%");
    rg.append("stop").attr("offset", "0%").attr("stop-color", "rgba(255,255,255,0.85)");
    rg.append("stop").attr("offset", "100%").attr("stop-color", "rgba(245,247,252,1)");
    layer.append("rect")
      .attr("x", -w).attr("y", -h).attr("width", 2 * w).attr("height", 2 * h)
      .attr("fill", "url(#" + gradId + ")");

    // Fan angles: -90 to +90 degrees
    const a0 = -Math.PI / 2;
    const a1 = Math.PI / 2;

    const levels = buildAncestorSlots(rootPerson.id, gens);

    const visibleIds = new Set();
    levels.forEach((level) => {
      level.forEach((n) => {
        if (n && n.person && !n.person.hidden) visibleIds.add(String(n.person.id));
      });
    });

    // Root card
    const rootG = layer.append("g").attr("class", "fan-root-card").attr("filter", "url(#softShadow)");
    rootG.append("rect")
      .attr("x", -rootCardW / 2).attr("y", -rootCardH / 2 + 10)
      .attr("rx", 16).attr("ry", 16)
      .attr("width", rootCardW).attr("height", rootCardH)
      .attr("class", "fan-card");
    rootG.append("text")
      .attr("x", 0).attr("y", 18)
      .attr("text-anchor", "middle")
      .attr("class", "fan-card-name")
      .text(safeText(rootPerson.name) || t("tree.unknown.name"));
    const sub = personSubtitle(rootPerson);
    if (sub) {
      rootG.append("text")
        .attr("x", 0).attr("y", 40)
        .attr("text-anchor", "middle")
        .attr("class", "fan-card-sub")
        .text(sub);
    }
    rootG.style("cursor", "pointer").on("click", function () {
      updateDrawer(rootPerson);
    });

    // Segments (generations 1..gens)
    for (let gen = 1; gen <= gens; gen++) {
      const nodes = levels[gen];
      const count = nodes.length; // 2^gen
      const innerR = innerStart + (gen - 1) * ringW;
      const outerR = innerR + ringW * 0.96;

      const pad = 0.005 + Math.min(0.01, 0.002 * gen);
      const arc = d3
        .arc()
        .innerRadius(innerR)
        .outerRadius(outerR)
        .cornerRadius(Math.max(4, ringW * 0.12))
        .padAngle(pad)
        .padRadius(outerR);

      const sexArc = d3
        .arc()
        .innerRadius(innerR + 1)
        .outerRadius(innerR + Math.max(3, ringW * 0.1))
        .padAngle(pad)
        .padRadius(innerR + Math.max(3, ringW * 0.1));

      const genG = layer.append("g").attr("class", "fan-gen fan-gen-" + gen);
      const ringFill = ringFills[gen % ringFills.length];

      for (let s = 0; s < count; s++) {
        const n = nodes[s];
        const start = a0 + (a1 - a0) * (s / count);
        const end = a0 + (a1 - a0) * ((s + 1) / count);
        const d = { startAngle: start, endAngle: end };

        const hasPerson = !!(n && n.person);
        const p = hasPerson ? n.person : null;

        const seg = genG
          .append("path")
          .attr("d", arc(d))
          .attr("class", hasPerson ? "fan-seg" : "fan-seg fan-seg-empty")
          .attr("fill", hasPerson ? ringFill : "url(#hatch)")
          .attr("tabindex", -1)
          .attr("focusable", "false");

        seg.attr("stroke", "rgba(20,35,55,0.08)").attr("stroke-width", 1);

        if (hasPerson && p) {
          genG
            .append("path")
            .attr("d", sexArc(d))
            .attr("class", "fan-sexline " + sexCls(p.sex))
            .attr("fill", "currentColor")
            .style("opacity", 0.85);
        }

        (function (segSel, personObj) {
          segSel
            .on("mousedown", function (event) {
              if (event && event.preventDefault) event.preventDefault();
            })
            .on("mouseenter", function () {
              d3.select(this).classed("is-hover", true);
            })
            .on("mouseleave", function () {
              d3.select(this).classed("is-hover", false);
            })
            .on("click", function (event) {
              if (event && event.preventDefault) event.preventDefault();
              if (event && event.stopPropagation) event.stopPropagation();
              if (!personObj) return;
              updateDrawer(personObj);
              genG.selectAll(".fan-seg").classed("is-selected", false);
              d3.select(this).classed("is-selected", true);
            });
        })(seg, p);

        if (hasPerson) {
          const mid = (start + end) / 2;
          const deg = (mid * 180) / Math.PI;
          const c = arc.centroid(d);
          const rotate = Math.max(-55, Math.min(55, deg));
          const tg = genG
            .append("g")
            .attr("transform", "translate(" + c[0] + "," + c[1] + ") rotate(" + rotate + ")")
            .attr("class", "fan-label");

          const name = safeText(p.name);
          const years = formatYears(p);

          function trunc(str, max) {
            const s = safeText(str);
            if (s.length <= max) return s;
            return s.slice(0, Math.max(0, max - 1)) + "…";
          }

          tg.append("text")
            .attr("text-anchor", "middle")
            .attr("class", "fan-label-name")
            .text(trunc(name, gen >= 4 ? 18 : 22));

          if (years && gen <= 5) {
            tg.append("text")
              .attr("text-anchor", "middle")
              .attr("dy", "1.25em")
              .attr("class", "fan-label-sub")
              .text(years);
          }
        }
      }
    }

    fitToView();

    try {
      setVisibleInfo(visibleIds.size);
    } catch (_) {}
  }

  function fitToView() {
    const tform = d3.zoomIdentity.translate(0, 0).scale(1);
    svg.transition().duration(180).call(zoom.transform, tform);
  }

  if (fitBtn) fitBtn.addEventListener("click", function () {
    render();
  });

  if (generacionsSelect) {
    generacionsSelect.addEventListener("change", function () {
      render();
    });
  }

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
        opt.textContent = `${p.name || ("#" + p.id)}`;
        sel.appendChild(opt);
      }
      sel.__filled = true;

      sel.addEventListener("change", () => {
        const newId = Number(sel.value);
        if (!newId) return;
        rootId = newId;
        rootPerson = getPerson(rootId);
        if (!rootPerson) return;
        render();
      });
    }

    try {
      sel.value = String(window.rootPersonId || "");
    } catch (_) {}
  }

  initRootSelect();
  render();
  updateDrawer(rootPerson);
})();
