/*
  arbre-familiar.js  — Motor de layout v2 (reescriptura definitiva)
  ===================================================================

  ARQUITECTURA CLAU:
  ──────────────────
  1. UNIONS com a entitats de primera classe
     Cada parella (pare+mare, o un sol progenitor) és una "Union".
     Els fills pertanyen a UNA Union concreta. Mai es barregen.

  2. LAYOUT per RANGS (Sugiyama simplificat)
     - Rang 0 = focus. Rang -N = avantpassats. Rang +N = descendents.
     - Per cada generació es construeixen "clusters de parella":
       dues persones d'una mateixa Union sempre apareixen adjacents.
     - Compactació iterativa amb 4 passades baricèntriques.

  3. CONNECTORS ORTOGONALS per Union
     - Baixada pare → punt matrimoni
     - Baixada mare → punt matrimoni
     - Línia horitzontal de matrimoni
     - Troncal → bus de germans → baixades individuals

  4. RENDIMENT
     - Només renderitza el subgraf visible
     - Fetch lazy d'ancestres per API
     - D3 join eficient (enter/update/exit)

  5. NAVEGACIÓ
     - Clic sobre persona diferent al focus → canvi de focus
     - Toggle expand/collapse per veure fills
     - Clic sobre progenitor del tronc → canvi de llinatge (patern/matern)
*/

(function () {
  "use strict";

  // ─── i18n ──────────────────────────────────────────────────────────────────
  const i18nEl = document.getElementById("tree-i18n");
  let I18N = {};
  try { I18N = JSON.parse(i18nEl?.textContent || "{}"); } catch (_) {}
  const t = (key, vars) => {
    let s = I18N[key] || key;
    if (vars) for (const k of Object.keys(vars)) s = s.replaceAll(`{${k}}`, vars[k]);
    return s;
  };

  if (typeof window.d3 === "undefined") { console.error("d3 no trobat"); return; }

  // ─── Constants de mida ─────────────────────────────────────────────────────
  const NW         = 200;  // node width
  const NH         = 74;   // node height
  const SW         = 6;    // strip de color (sexe)
  const GAP_X      = 56;   // separació horitzontal mínima entre nodes
  const GAP_Y      = 50;   // separació vertical entre generacions
  const SPOUSE_GAP = 16;   // gap extra entre membres d'un cluster parella
  const MARRY_DY   = 14;   // desplaçament Y punt matrimoni sota pares
  const BUS_DY     = 14;   // desplaçament Y bus sobre fills

  // ─── DOM ──────────────────────────────────────────────────────────────────
  const svg        = d3.select("#treeSvg");
  const drawer     = document.getElementById("personDrawer");
  const drawerName = document.getElementById("drawerName");
  const drawerSub  = document.getElementById("drawerSub");
  const drawerBody = document.getElementById("drawerBody");
  const drawerClose= document.getElementById("drawerClose");
  const btnZoomIn  = document.getElementById("zoomIn");
  const btnZoomOut = document.getElementById("zoomOut");
  const btnFit     = document.getElementById("fitView");
  const btnToggle  = document.getElementById("toggleDrawer");
  const genSelect  = document.getElementById("generacionsSelect");
  const viewBtn    = document.getElementById("viewPersonBtn");

  const profileBase = (() => {
    if (!Object.prototype.hasOwnProperty.call(window, "treeProfileBase")) return "/persones";
    return (window.treeProfileBase || "").trim();
  })();
  const canOpenProfile = profileBase !== "";
  const expandDisabled = !!window.treeExpandDisabled;

  // ─── Dades: persones ───────────────────────────────────────────────────────
  const familyDataRef  = Array.isArray(window.familyData)  ? window.familyData  : [];
  const familyLinksRef = Array.isArray(window.familyLinks) ? window.familyLinks : [];

  const persons = new Map(familyDataRef.map(p => [String(p.id), p]));

  // ─── Dades: unions i parentesc ─────────────────────────────────────────────
  /*
    Union = { id: string, fId: string|null, mId: string|null, children: Set<string> }
    Clau canònica: "<menor>:<major>" (o "~" per null)
  */
  const unions           = new Map();   // unionId  → Union
  const personToUnions   = new Map();   // personId → Set<unionId>
  const childToUnion     = new Map();   // childId  → unionId
  const parentsByChild   = new Map();   // childId  → { fId, mId }
  const childrenByParent = new Map();   // parentId → Set<childId>
  const noParentsSet     = new Set();   // fills sense pares coneguts

  function makeUid(a, b) {
    const sa = a ? String(a) : "~";
    const sb = b ? String(b) : "~";
    return sa <= sb ? `${sa}:${sb}` : `${sb}:${sa}`;
  }

  function ensureUnion(fIdRaw, mIdRaw) {
    const fId = fIdRaw ? String(fIdRaw) : null;
    const mId = mIdRaw ? String(mIdRaw) : null;
    const uid = makeUid(fId, mId);
    if (!unions.has(uid)) unions.set(uid, { id: uid, fId, mId, children: new Set() });
    for (const pid of [fId, mId]) {
      if (!pid) continue;
      if (!personToUnions.has(pid)) personToUnions.set(pid, new Set());
      personToUnions.get(pid).add(uid);
    }
    return uid;
  }

  function normalizeId(v) {
    if (v == null) return null;
    const n = Number(v);
    return Number.isFinite(n) && n !== 0 ? String(Math.round(n)) : null;
  }

  function ingestLink(rec) {
    if (!rec) return;
    const cid = normalizeId(rec.child ?? rec.Child ?? rec.id ?? null);
    if (!cid) return;
    const fId = normalizeId(rec.father ?? rec.Father ?? null);
    const mId = normalizeId(rec.mother ?? rec.Mother ?? null);

    if (!fId && !mId) { noParentsSet.add(cid); return; }

    if (!parentsByChild.has(cid)) parentsByChild.set(cid, { fId: null, mId: null });
    const pb = parentsByChild.get(cid);
    if (fId && !pb.fId) pb.fId = fId;
    if (mId && !pb.mId) pb.mId = mId;

    for (const pid of [fId, mId]) {
      if (!pid) continue;
      if (!childrenByParent.has(pid)) childrenByParent.set(pid, new Set());
      childrenByParent.get(pid).add(cid);
    }

    const uid = ensureUnion(fId, mId);
    const u   = unions.get(uid);
    u.children.add(cid);
    // Preferim unió amb ambdós pares
    if (!childToUnion.has(cid) || (fId && mId)) childToUnion.set(cid, uid);
  }

  function addPerson(p) {
    if (!p?.id) return false;
    const k = String(p.id);
    if (persons.has(k)) return false;
    persons.set(k, p);
    familyDataRef.push(p);
    return true;
  }

  function mergeDataset(people, newLinks) {
    let ap = 0, al = 0;
    if (Array.isArray(people))   for (const p of people)   { if (addPerson(p)) ap++; }
    if (Array.isArray(newLinks)) for (const l of newLinks) { ingestLink(l); al++; }
    if (ap || al) syncDatasetInfo();
  }

  function syncDatasetInfo() {
    try {
      const el = document.getElementById("datasetInfo");
      if (el) el.textContent = t("tree.dataset", { people: persons.size, links: unions.size });
    } catch (_) {}
  }

  // Ingesta inicial
  for (const rec of familyLinksRef) ingestLink(rec);
  syncDatasetInfo();

  // ─── Helpers ───────────────────────────────────────────────────────────────
  function getPerson(id)    { return persons.get(String(id)) || { id: String(id), name: t("tree.unknown.person"), sex: 2 }; }
  function getParents(cid)  { return parentsByChild.get(String(cid)) || { fId: null, mId: null }; }
  function getChildren(pid) { const s = childrenByParent.get(String(pid)); return s ? Array.from(s) : []; }
  function isHidden(id)     { const p = persons.get(String(id)); return !!(p?.hidden); }

  function sexClass(sex) { return sex === 0 ? "is-male" : sex === 1 ? "is-female" : "is-unknown"; }
  function truncate(str, max) {
    const s = String(str || "").trim();
    return s.length <= max ? s : s.slice(0, max - 1) + "…";
  }
  function fmtLifespan(p) {
    const b = (p?.birth || "").trim(), d = (p?.death || "").trim();
    if (!b && !d) return "";
    if (b && d) return `${b} – ${d}`;
    if (b) return `${b} –`;
    return `– ${d}`;
  }
  function escapeHtml(s) {
    return String(s).replaceAll("&","&amp;").replaceAll("<","&lt;")
      .replaceAll(">","&gt;").replaceAll('"',"&quot;").replaceAll("'","&#39;");
  }
  function buildProfileURL(id) {
    const base = profileBase.endsWith("/") ? profileBase.slice(0,-1) : profileBase;
    return `${base}/${id}`;
  }
  function isNumericId(id) { return /^\d+$/.test(String(id || "")); }

  // ─── Estat global de la vista ─────────────────────────────────────────────
  let focusId       = window.rootPersonId ? String(window.rootPersonId) : null;
  let selectionId   = focusId;
  let drawerEnabled = true;
  let lastFitBounds = null;
  let lastSelectedP = null;

  const expanded = new Set();  // nodes amb fills visibles
  const lineage  = new Map();  // nodeId → "father"|"mother" (llinatge escollit)
  let didAutoExpand = false;

  // ─── Fetch lazy ────────────────────────────────────────────────────────────
  const expandCache  = new Set();
  const expandFlight = new Set();

  async function fetchAncestors(pid, gens) {
    if (expandDisabled) return false;
    const key = `${pid}:${gens}`;
    if (expandCache.has(key) || expandFlight.has(key)) return false;
    expandFlight.add(key);
    try {
      const res = await fetch(
        `/api/arbre/expand?person_id=${encodeURIComponent(pid)}&gens=${gens}&mode=ancestors`,
        { credentials: "same-origin" }
      );
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const d = await res.json();
      mergeDataset(d.people || d.familyData || [], d.links || d.familyLinks || []);
      if (!parentsByChild.has(String(pid))) noParentsSet.add(pid);
      expandCache.add(key);
      return true;
    } catch (err) {
      console.error("fetchAncestors:", err);
      expandCache.add(key);
      return false;
    } finally {
      expandFlight.delete(key);
    }
  }

  // ─── Drawer ────────────────────────────────────────────────────────────────
  function openDrawer(p) {
    lastSelectedP = p || null;
    if (!drawerEnabled) return;
    setViewTarget(p);
    drawer.classList.add("is-open");
    drawer.setAttribute("aria-hidden","false");
    drawerName.textContent = p?.name || t("tree.unknown.name");
    const bits = [];
    if (p?.occupation) bits.push(p.occupation);
    const life = fmtLifespan(p || {});
    if (life) bits.push(life);
    drawerSub.textContent = bits.join(" · ");
    const rows = [
      [t("tree.drawer.birth"),       p?.birth       || ""],
      [t("tree.drawer.birth_place"), p?.birth_place || ""],
      [t("tree.drawer.death"),       p?.death       || ""],
      [t("tree.drawer.death_place"), p?.death_place || ""],
    ].filter(([,v]) => (v || "").trim());
    const actionLink = canOpenProfile && isNumericId(p?.id)
      ? `<div class="drawer-actions"><a class="drawer-link" href="${buildProfileURL(p.id)}">${t("tree.drawer.open_profile")}</a></div>`
      : "";
    if (!rows.length) {
      drawerBody.innerHTML = `<div class="drawer-empty">${t("tree.drawer.empty")}</div>${actionLink}`;
      return;
    }
    drawerBody.innerHTML = `
      <div class="drawer-block">
        <div class="block-title">${t("tree.drawer.section")}</div>
        <div class="block-content"><div class="drawer-kv">
          ${rows.map(([k,v]) => `<div class="kv"><div class="k">${k}</div><div class="v">${escapeHtml(v)}</div></div>`).join("")}
        </div></div>
      </div>${actionLink}`;
  }

  function closeDrawer() {
    drawer.classList.remove("is-open");
    drawer.setAttribute("aria-hidden","true");
  }

  function setDrawerEnabled(on) {
    drawerEnabled = !!on;
    btnToggle?.classList.toggle("is-off", !drawerEnabled);
    drawer?.classList.toggle("is-disabled", !drawerEnabled);
    if (!drawerEnabled) { closeDrawer(); return; }
    if (lastSelectedP) openDrawer(lastSelectedP);
  }

  function setViewTarget(pLike) {
    if (!viewBtn) return;
    if (!canOpenProfile) { viewBtn.style.display = "none"; return; }
    const id = pLike?.id || focusId;
    if (id && isNumericId(id)) {
      viewBtn.href = buildProfileURL(id);
      viewBtn.classList.remove("is-disabled");
    } else {
      viewBtn.removeAttribute("href");
      viewBtn.classList.add("is-disabled");
    }
  }

  // ─── Canvi de focus ────────────────────────────────────────────────────────
  function updateUrl(id) {
    try {
      const url = new URL(window.location.href);
      const m = url.pathname.match(/^(.*\/(?:espai\/persones|persones|public\/persones)\/)(\d+)(\/arbre.*)$/);
      if (!m) return;
      url.pathname = `${m[1]}${id}${m[3]}`;
      window.history.replaceState({}, "", url.toString());
    } catch (_) {}
  }

  function setFocus(newId) {
    if (!newId) return false;
    const s = String(newId);
    if (s === focusId) return false;
    focusId = s;
    window.rootPersonId = s;
    selectionId = s;
    expanded.clear();
    lineage.clear();
    didAutoExpand = false;
    updateUrl(s);
    return true;
  }

  // ════════════════════════════════════════════════════════════════════════════
  //  PART 1: SUBGRAF VISIBLE
  // ════════════════════════════════════════════════════════════════════════════

  function computeVisible(rootId, depth) {
    // Compte d'ancestres per escollir llinatge
    function countAnc(id, d) {
      if (!id || d <= 0) return 0;
      let n = 0, fr = [String(id)];
      for (let l = 0; l < d && fr.length; l++) {
        const nx = [];
        for (const x of fr) {
          const { fId, mId } = getParents(x);
          if (fId && !isHidden(fId)) { n++; nx.push(fId); }
          if (mId && !isHidden(mId)) { n++; nx.push(mId); }
        }
        fr = nx;
      }
      return n;
    }

    function preferSide(nodeId, remain) {
      const pref = lineage.get(String(nodeId));
      if (pref === "father" || pref === "mother") return pref;
      const { fId, mId } = getParents(nodeId);
      const f = fId && !isHidden(fId) ? fId : null;
      const m = mId && !isHidden(mId) ? mId : null;
      if (f && !m) return "father";
      if (m && !f) return "mother";
      if (!f && !m) return null;
      return countAnc(m, remain - 1) > countAnc(f, remain - 1) ? "mother" : "father";
    }

    const visible   = new Set([rootId]);
    const trunkSet  = new Set([rootId]);
    const switchMap = new Map();

    // BFS ascendent fins a `depth` generacions
    const queue = [{ id: rootId, level: 0 }];
    while (queue.length) {
      const { id, level } = queue.shift();
      if (level + 1 >= depth) continue;
      const { fId, mId } = getParents(id);
      const f = fId && !isHidden(fId) ? String(fId) : null;
      const m = mId && !isHidden(mId) ? String(mId) : null;

      // Mostrem AMBDÓS progenitors (per germans, oncles/ties, etc.)
      if (f) { visible.add(f); switchMap.set(f, { childId: String(id), side: "father" }); }
      if (m) { visible.add(m); switchMap.set(m, { childId: String(id), side: "mother" }); }

      const side = preferSide(String(id), depth - level);
      const next = side === "father" ? f : side === "mother" ? m : (f || m);
      if (next) { trunkSet.add(next); queue.push({ id: next, level: level + 1 }); }
    }

    // Auto-expand inicial (focus + pares + avis)
    if (!didAutoExpand) {
      expanded.add(rootId);
      const { fId: pf, mId: pm } = getParents(rootId);
      for (const pid of [pf, pm].filter(Boolean).filter(id => !isHidden(id)).map(String)) {
        expanded.add(pid);
        const { fId: gf, mId: gm } = getParents(pid);
        for (const gid of [gf, gm].filter(Boolean).filter(id => !isHidden(id)).map(String)) {
          expanded.add(gid);
        }
      }
      didAutoExpand = true;
    }

    // Expansions: afegim fills dels nodes desplegats + els seus co-progenitors directes
    let changed = true, guard = 0;
    while (changed && guard++ < 60) {
      changed = false;
      for (const pid of Array.from(expanded)) {
        if (!visible.has(pid) || isHidden(pid)) continue;
        for (const cid of getChildren(pid).map(String)) {
          if (isHidden(cid)) continue;
          if (!visible.has(cid)) { visible.add(cid); changed = true; }
          // Co-progenitor directe del fill (per la línia de matrimoni)
          const { fId: cf, mId: cm } = getParents(cid);
          for (const cop of [cf, cm].filter(Boolean).map(String)) {
            if (!isHidden(cop) && !visible.has(cop)) { visible.add(cop); changed = true; }
          }
        }
      }
      if (!visible.has(rootId)) { visible.add(rootId); changed = true; }
    }

    return { visible, trunkSet, switchMap };
  }

  // ════════════════════════════════════════════════════════════════════════════
  //  PART 2: RANGS
  // ════════════════════════════════════════════════════════════════════════════

  function computeRanks(rootId, visible) {
    const rank = new Map([[rootId, 0]]);
    const q    = [rootId];
    let guard  = 0;

    while (q.length && guard++ < 20000) {
      const id = q.shift();
      const r  = rank.get(id);

      const { fId, mId } = getParents(id);
      for (const pid of [fId, mId].filter(Boolean).map(String)) {
        if (!visible.has(pid) || isHidden(pid)) continue;
        const cand = r - 1;
        if (!rank.has(pid) || rank.get(pid) > cand) { rank.set(pid, cand); q.push(pid); }
      }

      for (const cid of getChildren(id).map(String)) {
        if (!visible.has(cid) || isHidden(cid)) continue;
        const cand = r + 1;
        if (!rank.has(cid) || rank.get(cid) < cand) { rank.set(cid, cand); q.push(cid); }

        const { fId: cf, mId: cm } = getParents(cid);
        for (const coParent of [cf, cm].filter(Boolean).map(String)) {
          if (coParent === id) continue;
          if (!visible.has(coParent) || isHidden(coParent)) continue;
          if (!rank.has(coParent) || rank.get(coParent) !== r) {
            rank.set(coParent, r);
            q.push(coParent);
          }
        }
      }
    }

    for (const id of visible) { if (!rank.has(id)) rank.set(id, 0); }
    return rank;
  }

  // ════════════════════════════════════════════════════════════════════════════
  //  PART 3: LAYOUT
  // ════════════════════════════════════════════════════════════════════════════

  function computeLayout(rootId, visible, rankMap) {
    const pos   = new Map();
    const SSTEP = NW + SPOUSE_GAP;  // amplada d'un cluster de parella
    const STEP  = NW + GAP_X;       // pas mínim entre clusters individuals

    // Agrupar ids per rang
    const byRank = new Map();
    for (const id of visible) {
      const r = rankMap.get(id) ?? 0;
      if (!byRank.has(r)) byRank.set(r, []);
      byRank.get(r).push(String(id));
    }
    const allRanks = Array.from(byRank.keys()).sort((a,b) => a-b);
    if (!allRanks.length) return pos;
    const minR = allRanks[0], maxR = allRanks[allRanks.length - 1];

    // ── Construir clusters de parella per a un rang ──────────────────────────
    function buildClusters(ids, r) {
      const sortedIds = [...ids].sort((a, b) => String(a).localeCompare(String(b)));
      const assigned = new Set();
      const clusters = [];

      // Recorre totes les unions per trobar parelles al rang r
      const unionList = Array.from(unions.values()).sort((a, b) => String(a.id).localeCompare(String(b.id)));
      for (const u of unionList) {
        const fOk = u.fId && visible.has(u.fId) && (rankMap.get(u.fId) ?? NaN) === r && !isHidden(u.fId);
        const mOk = u.mId && visible.has(u.mId) && (rankMap.get(u.mId) ?? NaN) === r && !isHidden(u.mId);
        if (!fOk || !mOk) continue;
        if (assigned.has(u.fId) || assigned.has(u.mId)) continue;

        // Ordre visual: home a l'esquerra, dona a la dreta
        const pf = getPerson(u.fId), pm = getPerson(u.mId);
        let left = u.fId, right = u.mId;
        if (pf.sex === 1 && pm.sex === 0) { left = u.mId; right = u.fId; }

        clusters.push({ members: [left, right], unionId: u.id });
        assigned.add(u.fId);
        assigned.add(u.mId);
      }

      // Singletons (no assignats a cap cluster de parella)
      for (const id of sortedIds) {
        if (assigned.has(id)) continue;
        clusters.push({ members: [id], unionId: null });
        assigned.add(id);
      }

      return clusters;
    }

    // ── Amplada d'un cluster ─────────────────────────────────────────────────
    function clusterW(c) { return c.members.length === 2 ? SSTEP + NW : NW; }
    function minStepBetween(ca, cb) { return clusterW(ca)/2 + GAP_X + clusterW(cb)/2; }

    // ── Baricèntre d'un cluster respecte al rang de referència ───────────────
    function bary(cluster, refR) {
      const xs = [];
      for (const id of cluster.members) {
        const { fId, mId } = getParents(id);
        for (const pid of [fId, mId].filter(Boolean).map(String)) {
          if ((rankMap.get(pid) ?? NaN) === refR && visible.has(pid) && pos.has(pid)) {
            xs.push(pos.get(pid).x);
          }
        }
        for (const cid of getChildren(id).map(String)) {
          if ((rankMap.get(cid) ?? NaN) === refR && visible.has(cid) && pos.has(cid)) {
            xs.push(pos.get(cid).x);
          }
        }
      }
      return xs.length ? xs.reduce((a,b) => a+b, 0) / xs.length : null;
    }

    // ── Compactació i assignació de coordenades ───────────────────────────────
    function pack(clusters, r, desired) {
      const n = clusters.length;
      if (!n) return;
      const widths  = clusters.map(clusterW);
      const centers = desired.map((d, i) => d ?? (i * STEP));

      // Endavant: mínima separació
      for (let i = 1; i < n; i++) {
        const min = centers[i-1] + minStepBetween(clusters[i-1], clusters[i]);
        if (centers[i] < min) centers[i] = min;
      }
      // Enrere: no apilar cap a la dreta
      for (let i = n-2; i >= 0; i--) {
        const max = centers[i+1] - minStepBetween(clusters[i], clusters[i+1]);
        if (centers[i] > max) centers[i] = max;
      }
      // Endavant final
      for (let i = 1; i < n; i++) {
        const min = centers[i-1] + minStepBetween(clusters[i-1], clusters[i]);
        if (centers[i] < min) centers[i] = min;
      }

      // Centra només el rang del focus; la resta manté alineació amb el rang de referència
      if (r === 0) {
        const fi = clusters.findIndex(c => c.members.includes(rootId));
        if (fi >= 0) {
          const off = -centers[fi];
          for (let i = 0; i < n; i++) centers[i] += off;
        } else {
          const mid = (centers[0] + centers[n-1]) / 2;
          for (let i = 0; i < n; i++) centers[i] -= mid;
        }
      }

      const y = r * (NH + GAP_Y);
      for (let i = 0; i < n; i++) {
        const cx = centers[i];
        if (clusters[i].members.length === 2) {
          const left = clusters[i].members[0];
          const right = clusters[i].members[1];
          const anchor = clusters[i].anchorId;
          if (anchor === left) {
            pos.set(left, { x: cx, y });
            pos.set(right, { x: cx + SSTEP, y });
          } else if (anchor === right) {
            pos.set(left, { x: cx - SSTEP, y });
            pos.set(right, { x: cx, y });
          } else {
            pos.set(left, { x: cx - SSTEP/2, y });
            pos.set(right, { x: cx + SSTEP/2, y });
          }
        } else {
          pos.set(clusters[i].members[0], { x: cx, y });
        }
      }
    }

    function orderAndPack(ids, r, refR) {
      const clusters = buildClusters(ids, r);
      for (const c of clusters) {
        if (c.members.length !== 2) continue;
        let anchor = null;
        for (const id of c.members) {
          const { fId, mId } = getParents(id);
          if ((rankMap.get(fId) ?? NaN) === refR || (rankMap.get(mId) ?? NaN) === refR) {
            if (anchor && anchor !== id) {
              anchor = null;
              break;
            }
            anchor = id;
          }
        }
        c.anchorId = anchor;
      }
      const baryList = clusters.map(c => bary(c, refR));
      const desired = baryList.slice();

      if (r > 0 && refR === r - 1) {
        const grouped = new Map();
        clusters.forEach((c, idx) => {
          let childId = null;
          if (c.members.length === 1) {
            childId = c.members[0];
          } else if (c.anchorId) {
            childId = String(c.anchorId);
          } else {
            for (const id of c.members) {
              const { fId, mId } = getParents(id);
              if ((rankMap.get(fId) ?? NaN) === refR || (rankMap.get(mId) ?? NaN) === refR) {
                childId = String(id);
                break;
              }
            }
          }
          if (!childId) return;
          const uid = childToUnion.get(String(childId));
          if (!uid) return;
          const u = unions.get(uid);
          if (!u) return;
          const fOk = u.fId && (rankMap.get(u.fId) ?? NaN) === refR && pos.has(u.fId);
          const mOk = u.mId && (rankMap.get(u.mId) ?? NaN) === refR && pos.has(u.mId);
          if (!fOk && !mOk) return;
          if (!grouped.has(uid)) grouped.set(uid, []);
          grouped.get(uid).push(idx);
        });

        for (const [uid, idxList] of grouped) {
          if (!idxList.length) continue;
          idxList.sort((a, b) => {
            const ak = clusters[a].members.join(":");
            const bk = clusters[b].members.join(":");
            return ak.localeCompare(bk);
          });

          const u = unions.get(uid);
          const fx = u.fId && pos.has(u.fId) ? pos.get(u.fId).x : null;
          const mx = u.mId && pos.has(u.mId) ? pos.get(u.mId).x : null;
          const unionX = (fx != null && mx != null) ? (fx + mx) / 2 : (fx != null ? fx : mx);
          if (unionX == null) continue;

          const localCenters = [0];
          for (let i = 1; i < idxList.length; i++) {
            const prev = clusters[idxList[i - 1]];
            const cur = clusters[idxList[i]];
            localCenters[i] = localCenters[i - 1] + minStepBetween(prev, cur);
          }
          const groupMid = (localCenters[0] + localCenters[localCenters.length - 1]) / 2;
          for (let i = 0; i < idxList.length; i++) {
            desired[idxList[i]] = unionX + (localCenters[i] - groupMid);
          }
        }
      }

      // Ordena per posició desitjada (null al final, estable per índex original)
      const idx = clusters.map((c, i) => i);
      idx.sort((a, b) => {
        const da = desired[a], db = desired[b];
        if (da != null && db != null && da !== db) return da - db;
        const ba = baryList[a], bb = baryList[b];
        if (ba == null && bb == null) return a - b;
        if (ba == null) return 1;
        if (bb == null) return -1;
        return ba - bb;
      });
      pack(idx.map(i => clusters[i]), r, idx.map(i => desired[i]));
    }

    // ── Rang 0 (focus) ────────────────────────────────────────────────────────
    {
      const ids = byRank.get(0) || [rootId];
      pack(buildClusters(ids, 0), 0, ids.map(() => null));
    }

    // ── Ancestres (rang negatiu) ───────────────────────────────────────────────
    for (let r = -1; r >= minR; r--) {
      const ids = byRank.get(r);
      if (!ids?.length) continue;
      orderAndPack(ids, r, r + 1);
    }

    // ── Descendents (rang positiu) ────────────────────────────────────────────
    for (let r = 1; r <= maxR; r++) {
      const ids = byRank.get(r);
      if (!ids?.length) continue;
      orderAndPack(ids, r, r - 1);
    }

    // ── 4 passades baricèntriques per reduir creuaments ───────────────────────
    for (let pass = 0; pass < 4; pass++) {
      for (let r = 1; r <= maxR; r++) {
        const ids = byRank.get(r);
        if (!ids?.length) continue;
        orderAndPack(ids, r, r - 1);
      }
      for (let r = -1; r >= minR; r--) {
        const ids = byRank.get(r);
        if (!ids?.length) continue;
        orderAndPack(ids, r, r + 1);
      }
    }

    return pos;
  }

  // ════════════════════════════════════════════════════════════════════════════
  //  PART 4: CONNECTORS
  // ════════════════════════════════════════════════════════════════════════════

  function buildPaths(visible, rankMap, pos, expandedSet) {
    const paths = [];

    for (const [uid, u] of unions) {
      const fOk = u.fId && visible.has(u.fId) && !isHidden(u.fId) && pos.has(u.fId);
      const mOk = u.mId && visible.has(u.mId) && !isHidden(u.mId) && pos.has(u.mId);
      if (!fOk && !mOk) continue;
      const parentExpanded = (u.fId && expandedSet.has(u.fId)) || (u.mId && expandedSet.has(u.mId));

      // Fills d'AQUESTA unió (mai barrejats amb d'altres)
      const kids = Array.from(u.children).map(String)
        .filter(cid => visible.has(cid) && !isHidden(cid) && pos.has(cid));

      const pf = fOk ? pos.get(u.fId) : null;
      const pm = mOk ? pos.get(u.mId) : null;

      const yf = pf ? pf.y + NH/2 : null;
      const ym = pm ? pm.y + NH/2 : null;
      const parentBaseY = Math.max(yf ?? -Infinity, ym ?? -Infinity);
      const marriageY   = parentBaseY + MARRY_DY;

      let cx;
      if (pf && pm) cx = (pf.x + pm.x) / 2;
      else if (pf)  cx = pf.x;
      else           cx = pm.x;

      const showCouple = pf && pm;
      const showChildren = kids.length > 0 && parentExpanded;
      if (!showCouple && !showChildren) continue;

      // Baixades progenitors → matrimoni
      if (pf && (showCouple || showChildren)) paths.push({ id: `${uid}:vf`, d: `M ${pf.x},${yf} V ${marriageY}` });
      if (pm && (showCouple || showChildren)) paths.push({ id: `${uid}:vm`, d: `M ${pm.x},${ym} V ${marriageY}` });

      // Línia horitzontal de matrimoni
      if (showCouple) {
        const x1 = Math.min(pf.x, pm.x), x2 = Math.max(pf.x, pm.x);
        paths.push({ id: `${uid}:mar`, d: `M ${x1},${marriageY} H ${x2}` });
      }

      if (!showChildren) continue;

      // Filtra fills al rang esperat (pares+1)
      const pRank = Math.min(
        fOk ? (rankMap.get(u.fId) ?? Infinity) : Infinity,
        mOk ? (rankMap.get(u.mId) ?? Infinity) : Infinity
      );
      const expRank = Number.isFinite(pRank) ? pRank + 1 : null;
      const busKids = expRank != null
        ? kids.filter(cid => (rankMap.get(cid) ?? 0) === expRank)
        : kids;
      const finalKids = busKids.length ? busKids : kids;

      const kidTops = finalKids.map(cid => pos.get(cid).y - NH/2);
      const busY    = Math.min(...kidTops) - BUS_DY;

      if (finalKids.length === 1) {
        const cp = pos.get(finalKids[0]);
        paths.push({
          id: `${uid}:dir`,
          d:  `M ${cx},${marriageY} V ${busY} H ${cp.x} V ${cp.y - NH/2}`
        });
      } else {
        const xs = finalKids.map(cid => pos.get(cid).x);
        paths.push({ id: `${uid}:stem`, d: `M ${cx},${marriageY} V ${busY}` });
        paths.push({ id: `${uid}:bus`,  d: `M ${Math.min(...xs)},${busY} H ${Math.max(...xs)}` });
        for (const cid of finalKids) {
          const cp = pos.get(cid);
          paths.push({ id: `${uid}:drop:${cid}`, d: `M ${cp.x},${busY} V ${cp.y - NH/2}` });
        }
      }
    }

    return paths;
  }

  // ════════════════════════════════════════════════════════════════════════════
  //  PART 5: SVG + ZOOM + RENDER
  // ════════════════════════════════════════════════════════════════════════════

  let gRoot, gLinks, gNodes, zoom;

  function setupSvg() {
    svg.selectAll("*").remove();
    gRoot  = svg.append("g").attr("class","tree-root");
    gLinks = gRoot.append("g").attr("class","tree-links");
    gNodes = gRoot.append("g").attr("class","tree-nodes");
    zoom   = d3.zoom().scaleExtent([0.08, 3.5])
      .on("zoom", e => gRoot.attr("transform", e.transform));
    svg.call(zoom);

    const svgNode = svg.node();
    if (svgNode && !svgNode.__gestureZoomBound) {
      let gestureBase = null;
      svgNode.addEventListener("gesturestart", (e) => {
        e.preventDefault();
        gestureBase = svgNode.__zoom?.k || 1;
      }, { passive: false });
      svgNode.addEventListener("gesturechange", (e) => {
        if (gestureBase == null) gestureBase = svgNode.__zoom?.k || 1;
        e.preventDefault();
        const target = Math.max(0.08, Math.min(3.5, gestureBase * e.scale));
        const pt = d3.pointer(e, svgNode);
        svg.call(zoom.scaleTo, target, pt);
      }, { passive: false });
      svgNode.addEventListener("gestureend", () => { gestureBase = null; });
      svgNode.__gestureZoomBound = true;
    }
  }

  function fitToView(bounds) {
    if (!bounds) return;
    const extraR = drawerEnabled ? 380 : 80;
    const px = NW/2 + 40, py = NH/2 + 60;
    const W  = (bounds.maxX - bounds.minX) + 2*px + extraR;
    const H  = (bounds.maxY - bounds.minY) + 2*py;
    const el = svg.node();
    const { width: vw, height: vh } = el.getBoundingClientRect();
    if (!vw || !vh) return;
    const scale = Math.min(2.5, Math.max(0.08, 0.9 * Math.min(vw/W, vh/H)));
    const cx = (bounds.minX + bounds.maxX) / 2;
    const cy = (bounds.minY + bounds.maxY) / 2;
    svg.transition().duration(260).call(
      zoom.transform,
      d3.zoomIdentity.translate(vw/2 - scale*cx, vh/2 - scale*cy).scale(scale)
    );
  }

  function hasExpandable(id) {
    if (isHidden(id)) return false;
    if (getChildren(id).length > 0) return true;
    const str = String(id);
    return !parentsByChild.has(str) && !noParentsSet.has(str) && !expandDisabled;
  }

  function ncls(d) {
    return ["tree-node", sexClass(d.person.sex),
      d.isTrunk         ? "is-trunk"    : "",
      d.id === selectionId ? "is-selected" : "",
      hasExpandable(d.id) ? "has-expand"  : "",
    ].filter(Boolean).join(" ");
  }

  function setVisibleInfo(n) {
    try {
      const el = document.getElementById("visibleInfo");
      if (el) el.textContent = t("tree.visible", { count: n || 0 });
    } catch (_) {}
  }

  function render({ fit = false } = {}) {
    if (!focusId) { console.error("focusId no definit"); return; }

    const depth = Math.max(1, Number(genSelect?.value || 3));
    const { visible, trunkSet, switchMap } = computeVisible(focusId, depth);
    const rankMap = computeRanks(focusId, visible);
    const pos     = computeLayout(focusId, visible, rankMap);

    // Bounds
    let minX=Infinity, maxX=-Infinity, minY=Infinity, maxY=-Infinity;
    for (const [id, p] of pos) {
      if (!visible.has(id)) continue;
      minX=Math.min(minX,p.x); maxX=Math.max(maxX,p.x);
      minY=Math.min(minY,p.y); maxY=Math.max(maxY,p.y);
    }
    if (!isFinite(minX)) minX=maxX=minY=maxY=0;
    lastFitBounds = { minX, maxX, minY, maxY };

    // ── Connectors ──────────────────────────────────────────────────────────
    gLinks.selectAll("path")
      .data(buildPaths(visible, rankMap, pos, expanded), d => d.id)
      .join(
        en => en.append("path").attr("class","tree-link").attr("d", d => d.d),
        up => up.attr("d", d => d.d),
        ex => ex.remove()
      );

    // ── Nodes ───────────────────────────────────────────────────────────────
    const nodeData = Array.from(visible)
      .filter(id => pos.has(id))
      .map(id => ({ id, person: getPerson(id), isTrunk: trunkSet.has(id) }));

    const sel = gNodes.selectAll("g.tree-node").data(nodeData, d => d.id);

    const en = sel.enter().append("g")
      .attr("class", d => ncls(d))
      .attr("transform", d => `translate(${pos.get(d.id).x},${pos.get(d.id).y})`)
      .style("cursor","pointer")
      .on("click", makeClickHandler(switchMap));

    en.append("rect").attr("class","node-card")
      .attr("x",-NW/2).attr("y",-NH/2).attr("rx",10).attr("ry",10)
      .attr("width",NW).attr("height",NH);
    en.append("rect").attr("class","node-strip")
      .attr("x",-NW/2).attr("y",-NH/2).attr("rx",10).attr("ry",10)
      .attr("width",SW).attr("height",NH);
    en.append("text").attr("class","node-glyph")
      .attr("x",-NW/2+SW+14).attr("y",-NH/2+26)
      .text(d => d.person.sex===0?"♂":d.person.sex===1?"♀":"?");
    en.append("text").attr("class","node-name")
      .attr("x",-NW/2+SW+36).attr("y",-NH/2+26)
      .text(d => truncate(d.person.name, 24));
    en.append("text").attr("class","node-dates")
      .attr("x",-NW/2+SW+36).attr("y",-NH/2+48)
      .text(d => truncate(fmtLifespan(d.person), 28));

    sel.merge(en)
      .attr("class", d => ncls(d))
      .transition().duration(160)
      .attr("transform", d => {
        const p = pos.get(d.id) || { x:0, y:0 };
        return `translate(${p.x},${p.y})`;
      });

    sel.exit().remove();

    svg.on("click", () => {
      selectionId = null;
      closeDrawer();
      gNodes.selectAll("g.tree-node").classed("is-selected", false);
      setViewTarget({ id: focusId });
    });

    if (fit) fitToView(lastFitBounds);
    setVisibleInfo(visible.size);
  }

  function makeClickHandler(switchMap) {
    return function(event, d) {
      if (event.defaultPrevented) return;
      event.stopPropagation();

      selectionId   = d.id;
      lastSelectedP = d.person;
      if (drawerEnabled) openDrawer(d.person);

      // Nova persona focus → reinicia i redibuja
      if (setFocus(d.id)) { render({ fit: false }); return; }

      // Canvi de llinatge (clic sobre progenitor del tronc)
      const sw = switchMap.get(String(d.id));
      let didSwitch = false;
      if (sw?.childId) { lineage.set(String(sw.childId), sw.side); didSwitch = true; }

      // Fetch lazy d'ancestres si no sabem si n'hi ha
      const str = String(d.id);
      if (!expandDisabled && !parentsByChild.has(str) && !noParentsSet.has(str)) {
        fetchAncestors(d.id, 2).then(ok => { if (ok) render({ fit: false }); });
      }

      // Toggle expand/collapse
      if (!didSwitch && hasExpandable(d.id)) {
        if (expanded.has(d.id)) expanded.delete(d.id);
        else expanded.add(d.id);
      }

      render({ fit: false });
    };
  }

  // ─── Controls UI ──────────────────────────────────────────────────────────
  function bindUi() {
    drawerClose?.addEventListener("click", e => { e.preventDefault(); closeDrawer(); });
    genSelect?.addEventListener("change", () => render({ fit: true }));
    btnZoomIn?.addEventListener("click",  () => svg.transition().duration(120).call(zoom.scaleBy, 1.25));
    btnZoomOut?.addEventListener("click", () => svg.transition().duration(120).call(zoom.scaleBy, 0.8));
    btnFit?.addEventListener("click",     () => fitToView(lastFitBounds));
    btnToggle?.addEventListener("click",  () => { setDrawerEnabled(!drawerEnabled); fitToView(lastFitBounds); });
    document.addEventListener("keydown",  e => { if (e.key === "Escape") closeDrawer(); });
  }

  // ─── Arrencada ────────────────────────────────────────────────────────────
  setupSvg();
  bindUi();
  setDrawerEnabled(true);
  setViewTarget({ id: focusId });
  render({ fit: true });

})();
