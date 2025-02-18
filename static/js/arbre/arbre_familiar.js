/*
  Vista "Familiar" (de baix cap amunt) basada en:
    - window.familyData: persones
    - window.familyLinks: {child, father, mother}

  Idees clau:
    - Mostra el focus (rootPersonId) i els ancestres segons el selector.
    - Per defecte desplega els pares del focus perquè apareguin els germans.
    - Clic a una persona: selecciona + obre fitxa + desplega/replega (mostra els seus fills + co-progenitors).
    - Per veure cosins: desplega els avis i després desplega els oncles/ties.

  Aquesta vista està pensada per escalar amb dades grans perquè no pinta tot l'univers,
  sinó el subgraf visible segons expansions.
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

  // Mides
  const NODE_W = 200;
  const NODE_H = 74;
  const STRIP_W = 6;
  const GAP_X = 54;
  const GAP_Y = 44; // separacio vertical entre generacions

  // DOM
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
  const expandDisabled = !!window.treeExpandDisabled;

  // Dades
  const familyDataRef = Array.isArray(window.familyData) ? window.familyData : [];
  const familyLinksRef = Array.isArray(window.familyLinks) ? window.familyLinks : [];

  const persons = new Map(familyDataRef.map((p) => [String(p.id), p]));
  const links = familyLinksRef;

  const linkKeySet = new Set();
  const expandCache = new Set();
  const expandInFlight = new Set();
  const EXPAND_GENS = 2;

  function linkKey(child, fatherId, motherId) {
    return `${child || ""}:${fatherId || ""}:${motherId || ""}`;
  }

  function normalizeTreeId(val) {
    if (val == null) return null;
    const num = Number(val);
    if (!Number.isFinite(num) || num === 0) return null;
    return String(num);
  }

  function addPerson(person) {
    if (!person || person.id == null) return false;
    const key = String(person.id);
    if (persons.has(key)) return false;
    persons.set(key, person);
    familyDataRef.push(person);
    return true;
  }

  // child -> {fatherId, motherId}
  const parentsByChild = new Map();
  // parent -> Set(child)
  const childrenByParent = new Map();

  function upsertParentMaps(childId, fatherId, motherId) {
    const childKey = String(childId);
    const existing = parentsByChild.get(childKey) || { fatherId: null, motherId: null };
    if (fatherId && !existing.fatherId) existing.fatherId = fatherId;
    if (motherId && !existing.motherId) existing.motherId = motherId;
    parentsByChild.set(childKey, existing);

    if (fatherId) {
      const fKey = String(fatherId);
      if (!childrenByParent.has(fKey)) childrenByParent.set(fKey, new Set());
      childrenByParent.get(fKey).add(childKey);
    }
    if (motherId) {
      const mKey = String(motherId);
      if (!childrenByParent.has(mKey)) childrenByParent.set(mKey, new Set());
      childrenByParent.get(mKey).add(childKey);
    }
  }

  function addLinkRecord(record) {
    if (!record) return false;
    const child = normalizeTreeId(record.child ?? record.Child ?? record.id ?? null);
    if (!child) return false;
    const fatherId = normalizeTreeId(record.father ?? record.Father ?? null);
    const motherId = normalizeTreeId(record.mother ?? record.Mother ?? null);
    if (!fatherId && !motherId) {
      markNoParents(child);
      return false;
    }
    const key = linkKey(child, fatherId, motherId);
    if (linkKeySet.has(key)) {
      upsertParentMaps(child, fatherId, motherId);
      return false;
    }
    linkKeySet.add(key);
    links.push({ child, father: fatherId, mother: motherId });
    upsertParentMaps(child, fatherId, motherId);
    return true;
  }

  function markNoParents(childId) {
    const childKey = String(childId);
    if (!parentsByChild.has(childKey)) {
      parentsByChild.set(childKey, { fatherId: null, motherId: null });
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

  // Helpers
  function isHidden(id) {
    if (id == null) return false;
    const p = persons.get(String(id));
    return !!(p && p.hidden);
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
    const rootId = focusPersonId || window.rootPersonId;
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

  function getPerson(id) {
    const key = String(id);
    return persons.get(key) || { id: key, name: t("tree.unknown.person"), sex: 2 };
  }

  function getParents(childId) {
    return parentsByChild.get(String(childId)) || { fatherId: null, motherId: null };
  }

  function getChildren(parentId) {
    const s = childrenByParent.get(String(parentId));
    return s ? Array.from(s) : [];
  }

  function getSpouses(personId) {
    const pid = String(personId);
    const spouses = new Set();
    for (const r of links) {
      const f = r.father != null ? String(r.father) : null;
      const m = r.mother != null ? String(r.mother) : null;
      if (f === pid && m) spouses.add(m);
      if (m === pid && f) spouses.add(f);
    }
    return Array.from(spouses);
  }

  async function fetchAncestorsFor(personId, gens) {
    if (expandDisabled) return false;
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
      if (!parentsByChild.has(String(personId))) markNoParents(personId);
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

  // Drawer (fitxa)
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
      const empty = '<div class="drawer-empty">' + t("tree.drawer.empty") + "</div>";
      drawerBody.innerHTML = empty + actionLink;
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

  // Estat de desplegament: quan una persona es "expanded" mostra els seus fills (i l'altre progenitor).
  const expanded = new Set();
  let didAutoExpandParents = false;
  let currentSelectionId = null;
  let focusPersonId = window.rootPersonId ? String(window.rootPersonId) : null;

  // Seleccio de llinatge (estil Geneanet):
  // per cada "fill" (node del tronc), triem si seguim la linia paterna o materna.
  // Aixo permet mostrar els dos pares pero nomes desplegar els avis d'una de les dues branques.
  const lineageChoice = new Map(); // childId -> "father" | "mother"

  // Mapa generat a cada render per poder canviar el llinatge fent clic sobre un progenitor.
  // parentId -> { childId, side }
  let lastLineageSwitchMap = new Map();
  let lastTrunkSet = new Set();

  function updateFocusUrl(id) {
    if (!id || !window.history || !window.history.replaceState) return;
    const url = new URL(window.location.href);
    const path = url.pathname;
    const match = path.match(/^(.*\/(?:espai\/persones|persones|public\/persones)\/)(\d+)(\/arbre.*)$/);
    if (!match) return;
    url.pathname = `${match[1]}${id}${match[3]}`;
    window.history.replaceState({}, "", url.toString());
  }

  function resetFocusState() {
    expanded.clear();
    lineageChoice.clear();
    lastLineageSwitchMap = new Map();
    lastTrunkSet = new Set();
    didAutoExpandParents = false;
  }

  function setFocusPerson(newId) {
    if (!newId) return false;
    const idStr = String(newId);
    if (focusPersonId === idStr) return false;
    focusPersonId = idStr;
    window.rootPersonId = idStr;
    currentSelectionId = idStr;
    resetFocusState();
    updateFocusUrl(idStr);
    return true;
  }

  function toggleExpanded(id) {
    const k = String(id);
    if (expanded.has(k)) expanded.delete(k);
    else expanded.add(k);
  }

  function countKnownAncestors(startId, depth) {
    const sid = String(startId);
    if (!sid || depth <= 0) return 0;

    let count = 0;
    let frontier = [sid];
    for (let level = 0; level < depth; level++) {
      const next = [];
      for (const id of frontier) {
        const p = getPerson(id);
        // Si es un placeholder "hidden", no ens aporta informacio (i no seguim mes amunt)
        if (p && p.hidden) continue;

        const { fatherId, motherId } = getParents(id);
        if (fatherId) {
          const pf = getPerson(fatherId);
          if (!(pf && pf.hidden)) count += 1;
          next.push(String(fatherId));
        }
        if (motherId) {
          const pm = getPerson(motherId);
          if (!(pm && pm.hidden)) count += 1;
          next.push(String(motherId));
        }
      }
      frontier = next;
      if (!frontier.length) break;
    }
    return count;
  }

  function defaultLineSide(childId, remainingDepth) {
    const { fatherId, motherId } = getParents(childId);
    const f = fatherId ? String(fatherId) : null;
    const m = motherId ? String(motherId) : null;

    if (f && !m) return "father";
    if (m && !f) return "mother";

    // Heuristica: seguim la branca amb mes ancestres "coneguts" dins el limit restant.
    const scoreF = f ? countKnownAncestors(f, Math.max(0, remainingDepth - 1)) : 0;
    const scoreM = m ? countKnownAncestors(m, Math.max(0, remainingDepth - 1)) : 0;

    if (scoreM > scoreF) return "mother";
    return "father";
  }

  function buildAncestorSet(focusId, depth) {
    const focus = String(focusId);

    const set = new Set([focus]);
    const switchMap = new Map(); // parentId -> { childId, side }
    const trunkSet = new Set([focus]);

    let frontier = [{ id: focus, level: 0 }];

    while (frontier.length) {
      const { id, level } = frontier.shift();
      const nextLevel = level + 1;
      if (nextLevel >= depth) continue;

      const { fatherId, motherId } = getParents(id);
      const f = fatherId ? String(fatherId) : null;
      const m = motherId ? String(motherId) : null;

      // Sempre fem visibles els dos progenitors (si existeixen),
      // pero nomes continuem "amunt" per una branca (seleccionada).
      if (f && !isHidden(f)) {
        set.add(f);
        switchMap.set(f, { childId: String(id), side: "father" });
      }
      if (m && !isHidden(m)) {
        set.add(m);
        switchMap.set(m, { childId: String(id), side: "mother" });
      }

      // Decideix quina branca segueix a partir d'aquest node
      let chosen = lineageChoice.get(String(id));
      if (chosen !== "father" && chosen !== "mother") {
        chosen = defaultLineSide(String(id), depth - level);
      }

      // Fallback si la branca triada no existeix
      let nextId = null;
      if (chosen === "father" && f && !isHidden(f)) nextId = f;
      else if (chosen === "mother" && m && !isHidden(m)) nextId = m;
      else if (f && !isHidden(f)) nextId = f;
      else if (m && !isHidden(m)) nextId = m;

      if (nextId) {
        trunkSet.add(nextId);
        frontier.push({ id: nextId, level: nextLevel });
      }
    }

    return { set, switchMap, trunkSet };
  }

  function computeVisible(focusId, ancestorDepth) {
    const focus = String(focusId);

    // Ancestres (amb seleccio de llinatge)
    const built = buildAncestorSet(focus, ancestorDepth);
    const mandatory = built.set;

    // Exposa el mapa per canviar de branca fent clic sobre el pare/mare visible
    lastLineageSwitchMap = built.switchMap;
    lastTrunkSet = built.trunkSet;

    // Per defecte: desplega el focus + pares + avis (nomes el primer cop)
    // perque la vista tingui mes informacio immediata (germans, oncles/ties, cosins...).
    // En arbres grans aixo segueix sent acotat perque nomes desplega el voltant del focus.
    if (!didAutoExpandParents) {
      expanded.add(String(focus));

      const { fatherId, motherId } = getParents(focus);
      const f = fatherId ? String(fatherId) : null;
      const m = motherId ? String(motherId) : null;

      if (f && !(getPerson(f)?.hidden)) expanded.add(f);
      if (m && !(getPerson(m)?.hidden)) expanded.add(m);

      // Avis paterns/materns
      for (const pid of [f, m].filter(Boolean)) {
        const { fatherId: gf, motherId: gm } = getParents(pid);
        const gfs = gf ? String(gf) : null;
        const gms = gm ? String(gm) : null;
        if (gfs && !(getPerson(gfs)?.hidden)) expanded.add(gfs);
        if (gms && !(getPerson(gms)?.hidden)) expanded.add(gms);
      }

      didAutoExpandParents = true;
    }

    const visible = new Set(mandatory);

    // Expansions (iteracio fins a estabilitzar)
    let changed = true;
    let guard = 0;
    while (changed && guard < 35) {
      guard += 1;
      changed = false;

      for (const pidRaw of expanded) {
        const pid = String(pidRaw);
        if (getPerson(pid)?.hidden) continue;

        if (!visible.has(pid)) {
          visible.add(pid);
          changed = true;
        }
        const kids = getChildren(pid);
        for (const kid of kids) {
          if (!visible.has(kid)) {
            visible.add(kid);
            changed = true;
          }
          // Co-progenitors (parelles) perque els enllacos pare/mare siguin coherents
          const par = getParents(kid);
          if (par.fatherId && !getPerson(par.fatherId)?.hidden && !visible.has(par.fatherId)) {
            visible.add(par.fatherId);
            changed = true;
          }
          if (par.motherId && !getPerson(par.motherId)?.hidden && !visible.has(par.motherId)) {
            visible.add(par.motherId);
            changed = true;
          }
        }
        // Si te fills amb diferents parelles, les fem visibles quan esta desplegat
        for (const sp of getSpouses(pid)) {
          if (!getPerson(sp)?.hidden && !visible.has(sp)) {
            visible.add(sp);
            changed = true;
          }
        }
      }
      if (!visible.has(focus)) {
        visible.add(focus);
        changed = true;
      }
    }
    return visible;
  }

  function computeGenerations(focusId, visible, ancestorDepth) {
    const focus = String(focusId);
    const gen = new Map([[focus, 0]]);

    const minAllowed = -Math.max(1, ancestorDepth) + 1;
    const maxDown = Math.max(3, Math.max(1, ancestorDepth) + 2); // limit suau per no baixar infinitament

    const q = [focus];
    let guard = 0;

    while (q.length && guard < 8000) {
      guard += 1;
      const id = q.shift();
      const g = gen.get(id) ?? 0;

      // Pujar (pares)
      if (g - 1 >= minAllowed) {
        const { fatherId, motherId } = getParents(id);
        for (const pidRaw of [fatherId, motherId]) {
          if (!pidRaw) continue;
          const pid = String(pidRaw);
          if (!visible.has(pid)) continue;
          if (getPerson(pid)?.hidden) continue;

          const cand = g - 1;
          if (!gen.has(pid) || gen.get(pid) > cand) {
            gen.set(pid, cand);
            q.push(pid);
          }
        }
      }

      // Baixar (fills)
      if (g + 1 <= maxDown) {
        const kids = getChildren(id);
        for (const cidRaw of kids) {
          const cid = String(cidRaw);
          if (!visible.has(cid)) continue;
          if (getPerson(cid)?.hidden) continue;

          const cand = g + 1;
          // Evita inversions: un fill no pot quedar per sobre del seu progenitor.
          if (!gen.has(cid)) {
            gen.set(cid, cand);
            q.push(cid);
          } else {
            const cur = gen.get(cid);
            if (cur < cand) continue;
          }
        }
      }
    }

    // Per a nodes visibles sense generacio (casos rars), assigna 0
    for (const id of visible) {
      if (!gen.has(id)) gen.set(id, 0);
    }

    return gen;
  }

  function computeSideMap(focusId, visible) {
    const focus = String(focusId);
    const sideMap = new Map();
    const distMap = new Map();

    sideMap.set(focus, "center");
    distMap.set(focus, 0);

    const queue = [];

    function seed(idRaw, side) {
      if (!idRaw) return;
      const id = String(idRaw);
      if (!visible.has(id)) return;
      if (isHidden(id)) return;

      const existingDist = distMap.get(id);
      if (existingDist == null || existingDist > 1) {
        distMap.set(id, 1);
        sideMap.set(id, side);
        queue.push(id);
        return;
      }
      if (existingDist === 1) {
        const existingSide = sideMap.get(id);
        if (existingSide && existingSide !== side) {
          sideMap.set(id, "center");
        }
      }
    }

    const { fatherId, motherId } = getParents(focus);
    if (fatherId) seed(fatherId, "left");
    if (motherId) seed(motherId, "right");

    for (const kid of getChildren(focus)) {
      seed(kid, "center");
    }

    while (queue.length) {
      const id = queue.shift();
      const curDist = distMap.get(id) ?? 0;
      const curSide = sideMap.get(id) || "center";

      const neighbors = [];
      const { fatherId: f, motherId: m } = getParents(id);
      if (f) neighbors.push(String(f));
      if (m) neighbors.push(String(m));
      for (const kid of getChildren(id)) neighbors.push(String(kid));

      for (const nid of neighbors) {
        if (nid === focus) continue;
        if (!visible.has(nid)) continue;
        if (isHidden(nid)) continue;

        const nextDist = curDist + 1;
        const existingDist = distMap.get(nid);
        if (existingDist == null || nextDist < existingDist) {
          distMap.set(nid, nextDist);
          sideMap.set(nid, curSide);
          queue.push(nid);
        } else if (existingDist === nextDist) {
          const existingSide = sideMap.get(nid);
          if (existingSide && existingSide !== curSide && existingSide !== "center") {
            sideMap.set(nid, "center");
            queue.push(nid);
          }
        }
      }
    }

    return { sideMap, distMap };
  }

  function computeLayout(focusId, visible, genMap) {
    // Layout per "clusters" a cada generacio: persona + totes les seves parelles visibles al costat.
    // Aixo fa que, quan desplegues algu amb parella(es), la fila es reordeni i faci lloc (com Geneanet).

    const SPOUSE_GAP = 18; // espai entre parelles dins del mateix "cluster"

    // Agrupa per generacio
    const byGen = new Map();
    let minG = 0;
    let maxG = 0;
    for (const id of visible) {
      const g = genMap.get(id) ?? 0;
      minG = Math.min(minG, g);
      maxG = Math.max(maxG, g);
      if (!byGen.has(g)) byGen.set(g, []);
      byGen.get(g).push(String(id));
    }

    const pos = new Map();
    const sideInfo = computeSideMap(focusId, visible);
    const sideMap = sideInfo.sideMap;
    const distMap = sideInfo.distMap;

    function unionKey(a, b) {
      if (!a || !b) return null;
      return a < b ? `u:${a}:${b}` : `u:${b}:${a}`;
    }

    const unionChildrenMap = new Map();
    for (const r of links) {
      const child = String(r.child);
      if (!visible.has(child)) continue;
      if (isHidden(child)) continue;
      const fatherId = r.father != null ? String(r.father) : null;
      const motherId = r.mother != null ? String(r.mother) : null;
      if (!fatherId || !motherId) continue;
      if (!visible.has(fatherId) || !visible.has(motherId)) continue;
      if (isHidden(fatherId) || isHidden(motherId)) continue;
      const key = unionKey(fatherId, motherId);
      if (!key) continue;
      let set = unionChildrenMap.get(key);
      if (!set) {
        set = new Set();
        unionChildrenMap.set(key, set);
      }
      set.add(child);
    }

    function getUnionChildren(a, b) {
      const key = unionKey(a, b);
      if (!key) return [];
      const set = unionChildrenMap.get(key);
      return set ? Array.from(set) : [];
    }

    const primarySpouseByPerson = new Map();

    function visibleSpouses(pid, genValue) {
      return getSpouses(pid)
        .map(String)
        .filter((s) => visible.has(s))
        .filter((s) => (genMap.get(s) ?? 0) === genValue)
        .filter((s) => !(getPerson(s)?.hidden));
    }

    function spouseScore(pid, spouseId) {
      let score = Number.POSITIVE_INFINITY;
      const spouseDist = distMap.get(spouseId);
      if (spouseDist != null) score = Math.min(score, spouseDist);
      const kids = getUnionChildren(pid, spouseId);
      if (kids.length) {
        let bestChild = Number.POSITIVE_INFINITY;
        for (const kid of kids) {
          const d = distMap.get(kid);
          if (d != null && d < bestChild) bestChild = d;
        }
        if (bestChild < score) score = bestChild;
      }
      return score;
    }

    for (const pid of visible) {
      const genValue = genMap.get(pid) ?? 0;
      const spouses = visibleSpouses(pid, genValue);
      if (!spouses.length) continue;
      if (spouses.length === 1) {
        primarySpouseByPerson.set(String(pid), spouses[0]);
        continue;
      }
      let best = null;
      let bestScore = Number.POSITIVE_INFINITY;
      let bestKids = -1;
      let bestName = "";
      for (const spouseId of spouses) {
        const score = spouseScore(pid, spouseId);
        const kidsCount = getUnionChildren(pid, spouseId).length;
        const name = String(getPerson(spouseId).name || "");
        if (
          score < bestScore ||
          (score === bestScore && kidsCount > bestKids) ||
          (score === bestScore && kidsCount === bestKids && name.localeCompare(bestName) < 0)
        ) {
          best = spouseId;
          bestScore = score;
          bestKids = kidsCount;
          bestName = name;
        }
      }
      if (best) primarySpouseByPerson.set(String(pid), best);
    }

    function clusterWidth(members) {
      if (!members.length) return 0;
      return members.length * NODE_W + (members.length - 1) * SPOUSE_GAP;
    }

    function primarySpouse(pid, genValue) {
      const spouseId = primarySpouseByPerson.get(String(pid));
      if (!spouseId) return null;
      if (!visible.has(spouseId)) return null;
      if ((genMap.get(spouseId) ?? 0) !== genValue) return null;
      if (getPerson(spouseId)?.hidden) return null;
      return spouseId;
    }

    function buildClustersForGen(ids, genValue, focus) {
      const assigned = new Set();
      const clusters = [];

      const wantCluster = (id) => id === focus || expanded.has(id) || lastTrunkSet.has(id);

      // Primer: clusters per focus/tronc/expandits
      for (const id of ids) {
        const pid = String(id);
        if (assigned.has(pid)) continue;
        if (!wantCluster(pid)) continue;

        const members = [pid];
        const spouseId = primarySpouse(pid, genValue);
        if (spouseId && !assigned.has(spouseId)) {
          const spousePrimary = primarySpouseByPerson.get(String(spouseId));
          if (spousePrimary === pid) {
            members.push(spouseId);
          }
        }
        for (const m of members) assigned.add(m);
        clusters.push({ root: pid, members });
      }

      // Resta: singletons
      for (const id of ids) {
        const pid = String(id);
        if (assigned.has(pid)) continue;
        assigned.add(pid);
        clusters.push({ root: pid, members: [pid] });
      }

      return clusters;
    }

    function clusterSide(cluster) {
      let hasLeft = false;
      let hasRight = false;
      let hasCenter = false;

      for (const id of cluster.members) {
        const side = sideMap.get(String(id));
        if (side === "left") hasLeft = true;
        else if (side === "right") hasRight = true;
        else hasCenter = true;
      }

      if (hasLeft && hasRight) return "center";
      if (hasCenter) return "center";
      if (hasLeft) return "left";
      if (hasRight) return "right";
      return "center";
    }

    function clusterDisplayName(cluster) {
      const p = getPerson(cluster.root);
      return String(p.name || "");
    }

    function clusterName(item) {
      return clusterDisplayName(item.cluster);
    }

    function compareByTargetThenName(a, b) {
      const aHas = typeof a.target === "number";
      const bHas = typeof b.target === "number";
      if (aHas && bHas && a.target !== b.target) return a.target - b.target;
      if (aHas !== bHas) return aHas ? -1 : 1;
      return clusterName(a).localeCompare(clusterName(b));
    }

    function sideRank(side) {
      if (side === "left") return -1;
      if (side === "right") return 1;
      return 0;
    }

    function clusterParentInfo(cluster, refGen) {
      let best = null;
      for (const id of cluster.members) {
        const { fatherId, motherId } = getParents(id);
        if (!fatherId || !motherId) continue;
        const f = String(fatherId);
        const m = String(motherId);
        if (!visible.has(f) || !visible.has(m)) continue;
        if ((genMap.get(f) ?? 0) !== refGen || (genMap.get(m) ?? 0) !== refGen) continue;
        const fPos = pos.get(f);
        const mPos = pos.get(m);
        if (!fPos || !mPos) continue;
        const key = unionKey(f, m);
        if (!key) continue;
        const center = (fPos.x + mPos.x) / 2;
        const score = Math.min(
          distMap.get(f) ?? Number.POSITIVE_INFINITY,
          distMap.get(m) ?? Number.POSITIVE_INFINITY,
          distMap.get(String(id)) ?? Number.POSITIVE_INFINITY
        );
        if (!best || score < best.score) {
          best = { key, center, score };
        }
      }
      return best;
    }

    function clusterRefs(cluster, refGen, clusterGen) {
      // Referencia per ordenar el cluster segons els nodes ja collocats a la generacio adjacent.
      const refs = [];
      if (refGen > clusterGen && cluster.members.length === 2) {
        const a = String(cluster.members[0]);
        const b = String(cluster.members[1]);
        const kids = getUnionChildren(a, b)
          .map(String)
          .filter((k) => visible.has(k))
          .filter((k) => !(getPerson(k)?.hidden))
          .filter((k) => (genMap.get(k) ?? 0) === refGen);
        if (kids.length) {
          refs.push(...kids);
          return refs;
        }
      }

      for (const id of cluster.members) {
        if (refGen > clusterGen) {
          // cap avall: mira fills
          const kids = getChildren(id)
            .map(String)
            .filter((k) => visible.has(k))
            .filter((k) => (genMap.get(k) ?? 0) === refGen);
          refs.push(...kids);
        } else {
          // cap amunt: mira pares
          const { fatherId, motherId } = getParents(id);
          const ps = [fatherId, motherId]
            .filter(Boolean)
            .map(String)
            .filter((p) => visible.has(p))
            .filter((p) => (genMap.get(p) ?? 0) === refGen);
          refs.push(...ps);
        }
      }
      return refs;
    }

    function orderClusters(clusters, clusterGen, refGen, focus) {
      // Si estem a gen 0, garantim que el cluster del focus queda al centre.
      const idx = clusters.findIndex((c) => c.members.includes(String(focus)));
      const focusCluster = idx >= 0 ? clusters.splice(idx, 1)[0] : null;

      const groupTargets = new Map();
      if (refGen < clusterGen) {
        const groups = new Map();
        for (const c of clusters) {
          const info = clusterParentInfo(c, refGen);
          if (!info) continue;
          let group = groups.get(info.key);
          if (!group) {
            group = { center: info.center, clusters: [] };
            groups.set(info.key, group);
          }
          group.clusters.push(c);
        }

        for (const group of groups.values()) {
          if (group.clusters.length < 2) continue;
          const ordered = group.clusters
            .slice()
            .sort((a, b) => clusterDisplayName(a).localeCompare(clusterDisplayName(b)));
          const widths = ordered.map((c) => clusterWidth(c.members));
          const total = widths.reduce((a, b) => a + b, 0) + Math.max(0, ordered.length - 1) * GAP_X;
          let cursor = group.center - total / 2;
          for (let i = 0; i < ordered.length; i++) {
            const w = widths[i];
            const center = cursor + w / 2;
            groupTargets.set(ordered[i], center);
            cursor += w + GAP_X;
          }
        }
      }

      const scored = clusters.map((c) => {
        const refs = clusterRefs(c, refGen, clusterGen);
        const xs = refs.map((r) => pos.get(String(r))?.x).filter((v) => typeof v === "number");
        const fallback = xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null;
        const target = groupTargets.has(c) ? groupTargets.get(c) : fallback;
        return { cluster: c, target, side: clusterSide(c) };
      });

      if (clusterGen === 0) {
        const left = [];
        const right = [];
        const center = [];

        for (const item of scored) {
          if (item.side === "left") left.push(item);
          else if (item.side === "right") right.push(item);
          else center.push(item);
        }

        left.sort(compareByTargetThenName);
        right.sort(compareByTargetThenName);
        center.sort(compareByTargetThenName);

        const centerLeft = [];
        const centerRight = [];
        for (let i = 0; i < center.length; i++) {
          if (i % 2 === 0) centerLeft.push(center[i]);
          else centerRight.push(center[i]);
        }
        centerLeft.reverse();

        const ordered = [...left, ...centerLeft];
        if (focusCluster) {
          ordered.push({ cluster: focusCluster, target: 0, side: "center" });
        }
        ordered.push(...centerRight, ...right);
        return ordered;
      }

      scored.sort((a, b) => {
        const ra = sideRank(a.side);
        const rb = sideRank(b.side);
        if (ra !== rb) return ra - rb;
        return compareByTargetThenName(a, b);
      });

      return scored;
    }

    function packClusters(ordered, genValue) {
      if (!ordered.length) return;
      const items = ordered.map((item) =>
        item && item.cluster ? item : { cluster: item, target: null, side: "center" }
      );
      const widths = items.map((item) => clusterWidth(item.cluster.members));
      const totalW = widths.reduce((a, b) => a + b, 0) + Math.max(0, items.length - 1) * GAP_X;
      let cursor = -totalW / 2;
      const y = genValue * (NODE_H + GAP_Y);
      const fallbackCenters = new Array(items.length);

      for (let i = 0; i < items.length; i++) {
        const w = widths[i];
        fallbackCenters[i] = cursor + w / 2;
        cursor += w + GAP_X;
      }

      const centers = fallbackCenters.slice();
      const desired = items.map((item, i) =>
        typeof item.target === "number" ? item.target : fallbackCenters[i]
      );

      for (let i = 0; i < items.length; i++) {
        let c = desired[i];
        if (i > 0) {
          const minCenter = centers[i - 1] + widths[i - 1] / 2 + GAP_X + widths[i] / 2;
          if (c < minCenter) c = minCenter;
        }
        centers[i] = c;
      }

      for (let i = items.length - 2; i >= 0; i--) {
        const maxCenter = centers[i + 1] - widths[i + 1] / 2 - GAP_X - widths[i] / 2;
        if (centers[i] > maxCenter) centers[i] = maxCenter;
      }

      for (let i = 1; i < items.length; i++) {
        const minCenter = centers[i - 1] + widths[i - 1] / 2 + GAP_X + widths[i] / 2;
        if (centers[i] < minCenter) centers[i] = minCenter;
      }

      for (let i = 0; i < items.length; i++) {
        const c = items[i].cluster;
        const w = widths[i];
        const left = centers[i] - w / 2;
        for (let j = 0; j < c.members.length; j++) {
          const id = c.members[j];
          const x = left + NODE_W / 2 + j * (NODE_W + SPOUSE_GAP);
          pos.set(String(id), { x, y });
        }
      }
    }

    const focus = String(focusId);

    // Gen 0
    {
      const ids = byGen.get(0) || [focus];
      const clusters = buildClustersForGen(ids, 0, focus);
      const ordered = orderClusters(clusters, 0, 1, focus);
      packClusters(ordered, 0);
    }

    // Ancestres: g = -1, -2...
    for (let g = -1; g >= minG; g--) {
      const ids = byGen.get(g) || [];
      const clusters = buildClustersForGen(ids, g, focus);
      const ordered = orderClusters(clusters, g, g + 1, focus);
      packClusters(ordered, g);
    }

    // Descendents: g = +1, +2...
    for (let g = 1; g <= maxG; g++) {
      const ids = byGen.get(g) || [];
      const clusters = buildClustersForGen(ids, g, focus);
      const ordered = orderClusters(clusters, g, g - 1, focus);
      packClusters(ordered, g);
    }

    return pos;
  }

  // SVG layers + zoom
  let gRoot;
  let gLinks;
  let gNodes;
  let zoomBehavior;
  let lastFitBounds = null;

  function setupSvgLayers() {
    svg.selectAll("*").remove();

    gRoot = svg.append("g").attr("class", "tree-root");
    gLinks = gRoot.append("g").attr("class", "tree-links");
    gNodes = gRoot.append("g").attr("class", "tree-nodes");

    zoomBehavior = d3
      .zoom()
      .scaleExtent([0.25, 2.75])
      .on("zoom", (event) => gRoot.attr("transform", event.transform));

    svg.call(zoomBehavior);
  }

  function fitToView(bounds) {
    if (!bounds) return;

    const extraRight = drawerEnabled ? 420 : 80;
    const minX = bounds.minX - (NODE_W / 2 + 40);
    const maxX = bounds.maxX + (NODE_W / 2 + extraRight);
    const minY = bounds.minY - (NODE_H / 2 + 60);
    const maxY = bounds.maxY + (NODE_H / 2 + 60);
    const width = maxX - minX;
    const height = maxY - minY;

    const svgEl = svg.node();
    const bbox = svgEl.getBoundingClientRect();
    const vw = Math.max(1, bbox.width);
    const vh = Math.max(1, bbox.height);

    const scale = Math.min(2.2, Math.max(0.25, 0.92 * Math.min(vw / width, vh / height)));
    const tx = vw / 2 - scale * (minX + width / 2);
    const ty = vh / 2 - scale * (minY + height / 2);

    svg.transition().duration(220).call(zoomBehavior.transform, d3.zoomIdentity.translate(tx, ty).scale(scale));
  }

  function hasExpandable(id) {
    const p = getPerson(id);
    if (p && p.hidden) return false;
    const kids = getChildren(id);
    if (kids.length > 0) return true;
    if (!parentsByChild.has(String(id))) return !expandDisabled;
    return false;
  }

  function setVisibleInfo(count) {
    try {
      const el = document.getElementById("visibleInfo");
      if (el) el.textContent = t("tree.visible", { count: count || 0 });
    } catch (_) {}
  }

  function render({ fit = false } = {}) {
    const focusIdRaw = focusPersonId;
    if (!focusIdRaw) {
      console.error(t("tree.error.root"));
      return;
    }

    const focusId = String(focusIdRaw);
    const ancestorDepth = Math.max(1, Number(generationsSelect?.value || 3));

    const visible = computeVisible(focusId, ancestorDepth);
    const genMap = computeGenerations(focusId, visible, ancestorDepth);
    const pos = computeLayout(focusId, visible, genMap);

    // Bounds
    let minX = Infinity;
    let maxX = -Infinity;
    let minY = Infinity;
    let maxY = -Infinity;
    for (const id of visible) {
      const p = pos.get(id);
      if (!p) continue;
      minX = Math.min(minX, p.x);
      maxX = Math.max(maxX, p.x);
      minY = Math.min(minY, p.y);
      maxY = Math.max(maxY, p.y);
    }
    lastFitBounds = { minX, maxX, minY, maxY };

    function unionKeyForParents(fatherId, motherId) {
      if (fatherId && motherId) {
        const a = fatherId < motherId ? fatherId : motherId;
        const b = fatherId < motherId ? motherId : fatherId;
        return `u:${a}:${b}`;
      }
      if (fatherId) return `u:${fatherId}:_`;
      if (motherId) return `u:_:${motherId}`;
      return null;
    }

    const trunkUnionKeys = new Set();
    for (const cidRaw of lastTrunkSet) {
      const cid = String(cidRaw);
      const { fatherId, motherId } = getParents(cid);
      const key = unionKeyForParents(fatherId ? String(fatherId) : null, motherId ? String(motherId) : null);
      if (key) trunkUnionKeys.add(key);
    }

    // Links estil Geneanet: ortogonals + segments compartits
    const unions = new Map(); // key -> { key, fatherId, motherId, fatherRawId, motherRawId, children:Set }

    for (const r of links) {
      const child = String(r.child);
      if (!visible.has(child)) continue;

      const fatherIdRaw = r.father != null ? String(r.father) : null;
      const motherIdRaw = r.mother != null ? String(r.mother) : null;

      const fatherId = fatherIdRaw && visible.has(fatherIdRaw) && !isHidden(fatherIdRaw) ? fatherIdRaw : null;
      const motherId = motherIdRaw && visible.has(motherIdRaw) && !isHidden(motherIdRaw) ? motherIdRaw : null;

      if (!fatherId && !motherId) continue;

      const key = unionKeyForParents(fatherId, motherId);
      if (!key) continue;

      if (!unions.has(key)) {
        unions.set(key, {
          key,
          fatherId,
          motherId,
          fatherRawId: fatherIdRaw,
          motherRawId: motherIdRaw,
          children: new Set(),
        });
      }
      unions.get(key).children.add(child);
    }

    const pathData = [];
    const pushPath = (id, d) => {
      if (d) pathData.push({ id, d });
    };

    for (const u of unions.values()) {
      const fatherPos = u.fatherId ? pos.get(u.fatherId) : null;
      const motherPos = u.motherId ? pos.get(u.motherId) : null;

      // punts d'ancoratge dels progenitors (centre inferior de la targeta)
      const fA = fatherPos ? { x: fatherPos.x, y: fatherPos.y + NODE_H / 2 } : null;
      const mA = motherPos ? { x: motherPos.x, y: motherPos.y + NODE_H / 2 } : null;

      const parentsY = Math.max(fA?.y ?? -Infinity, mA?.y ?? -Infinity);
      const marriageY = parentsY + 14;

      let centerX = 0;
      if (fA && mA) centerX = (fA.x + mA.x) / 2;
      else if (fA) centerX = fA.x;
      else if (mA) centerX = mA.x;

      const fatherTrunk = u.fatherRawId && lastTrunkSet.has(String(u.fatherRawId));
      const motherTrunk = u.motherRawId && lastTrunkSet.has(String(u.motherRawId));
      const trunkSideCount = (fatherTrunk ? 1 : 0) + (motherTrunk ? 1 : 0);
      const isPrimaryUnion = trunkUnionKeys.has(u.key);
      const isSecondaryUnion = trunkSideCount === 1 && !isPrimaryUnion;

      // baixades dels progenitors fins a la linia de parella
      if (fA) pushPath(`${u.key}:pf`, `M ${fA.x},${fA.y} V ${marriageY}`);
      if (mA) pushPath(`${u.key}:pm`, `M ${mA.x},${mA.y} V ${marriageY}`);

      // linia de parella (si hi ha dos progenitors)
      if (fA && mA) {
        pushPath(`${u.key}:mar`, `M ${fA.x},${marriageY} H ${mA.x}`);
      }

      // fills visibles d'aquesta unio
      let children = Array.from(u.children).filter((cid) => pos.get(cid));
      if (!children.length) continue;

      // intenta limitar els fills a la generacio immediatament inferior
      const parentGen = Math.min(
        u.fatherId ? genMap.get(u.fatherId) ?? Infinity : Infinity,
        u.motherId ? genMap.get(u.motherId) ?? Infinity : Infinity
      );
      if (Number.isFinite(parentGen)) {
        const expected = parentGen + 1;
        const filtered = children.filter((cid) => (genMap.get(cid) ?? 0) === expected);
        if (filtered.length) children = filtered;
      }

      const childTop = (cid) => pos.get(cid).y - NODE_H / 2;
      const minTopY = Math.min(...children.map(childTop));
      const busY = minTopY - 14;

      const useSharedBus = children.length > 1 && !isSecondaryUnion;

      if (!useSharedBus) {
        for (const cid of children) {
          const cPos = pos.get(cid);
          const topY = childTop(cid);
          const elbowY = Math.min(busY, topY - 12);
          pushPath(`${u.key}:to:${cid}`, `M ${centerX},${marriageY} V ${elbowY} H ${cPos.x} V ${topY}`);
        }
        continue;
      }

      // Cas 2: multiples fills -> troncal + bus de germans + baixades curtes
      pushPath(`${u.key}:trunk`, `M ${centerX},${marriageY} V ${busY}`);

      const xs = children.map((cid) => pos.get(cid).x);
      const xMin = Math.min(...xs);
      const xMax = Math.max(...xs);
      pushPath(`${u.key}:bus`, `M ${xMin},${busY} H ${xMax}`);

      for (const cid of children) {
        const cPos = pos.get(cid);
        const topY = childTop(cid);
        pushPath(`${u.key}:drop:${cid}`, `M ${cPos.x},${busY} V ${topY}`);
      }
    }

    gLinks
      .selectAll("path")
      .data(pathData, (d) => d.id)
      .join(
        (enter) => enter.append("path").attr("class", "tree-link").attr("d", (d) => d.d),
        (update) => update.attr("d", (d) => d.d),
        (exit) => exit.remove()
      );

    // Nodes
    const nodeData = Array.from(visible).map((id) => ({
      id: String(id),
      person: getPerson(id),
      expanded: expanded.has(String(id)),
      isTrunk: lastTrunkSet.has(String(id)),
      selectable: true,
    }));

    const nodesSel = gNodes.selectAll("g.tree-node").data(nodeData, (d) => d.id);

    const nodesEnter = nodesSel
      .enter()
      .append("g")
      .attr(
        "class",
        (d) => `tree-node ${sexClass(d.person.sex)} ${hasExpandable(d.id) ? "has-expand" : ""} ${
          d.isTrunk ? "is-trunk" : ""
        }`
      )
      .attr("transform", (d) => {
        const p = pos.get(d.id) || { x: 0, y: 0 };
        return `translate(${p.x},${p.y})`;
      })
      .style("cursor", "pointer")
      .attr("tabindex", -1)
      .attr("focusable", "false");

    nodesEnter.on("mousedown", (event) => {
      if (event && event.preventDefault) event.preventDefault();
    });

    nodesEnter.on("click", (event, d) => {
      if (event.defaultPrevented) return;
      event.stopPropagation();

      currentSelectionId = d.id;
      lastSelectedPersonLike = d.person;
      if (drawerEnabled) openDrawer(d.person);

      if (setFocusPerson(d.id)) {
        render({ fit: false });
        return;
      }

      // Si cliques sobre un progenitor del tronc (pare/mare visible),
      // canviem la branca (paterna/materna) al nivell corresponent.
      const sw = lastLineageSwitchMap.get(String(d.id));
      let didSwitchLineage = false;
      if (sw && sw.childId && (sw.side === "father" || sw.side === "mother")) {
        lineageChoice.set(String(sw.childId), sw.side);
        didSwitchLineage = true;
      }

      if (!expandDisabled && !parentsByChild.has(String(d.id))) {
        fetchAncestorsFor(d.id, EXPAND_GENS).then((updated) => {
          if (updated) render({ fit: false });
        });
      }

      // Toggle desplegament amb clic (fills / cosins, etc.)
      // IMPORTANT: si aquest clic s'ha usat per canviar la branca (pare/mare del tronc),
      // no collapsarem/expandirem aqui per evitar desaparicions inesperades de fills.
      if (!didSwitchLineage && hasExpandable(d.id)) {
        toggleExpanded(d.id);
      }

      render({ fit: false });
    });

    // Card + strip
    nodesEnter
      .append("rect")
      .attr("class", "node-card")
      .attr("x", -NODE_W / 2)
      .attr("y", -NODE_H / 2)
      .attr("rx", 10)
      .attr("ry", 10)
      .attr("width", NODE_W)
      .attr("height", NODE_H);

    nodesEnter
      .append("rect")
      .attr("class", "node-strip")
      .attr("x", -NODE_W / 2)
      .attr("y", -NODE_H / 2)
      .attr("rx", 10)
      .attr("ry", 10)
      .attr("width", STRIP_W)
      .attr("height", NODE_H);

    // Glyph (♂/♀)
    nodesEnter
      .append("text")
      .attr("class", "node-glyph")
      .attr("x", -NODE_W / 2 + STRIP_W + 14)
      .attr("y", -NODE_H / 2 + 26)
      .text((d) => {
        const sex = d.person.sex;
        if (sex === 0) return "♂";
        if (sex === 1) return "♀";
        return "?";
      });

    // Name
    nodesEnter
      .append("text")
      .attr("class", "node-name")
      .attr("x", -NODE_W / 2 + STRIP_W + 36)
      .attr("y", -NODE_H / 2 + 26)
      .text((d) => truncate(d.person.name, 24));

    // Dates
    nodesEnter
      .append("text")
      .attr("class", "node-dates")
      .attr("x", -NODE_W / 2 + STRIP_W + 36)
      .attr("y", -NODE_H / 2 + 48)
      .text((d) => truncate(fmtLifespan(d.person), 28));

    // Update
    nodesSel
      .merge(nodesEnter)
      .attr("class", (d) => {
        const sel = String(d.id) === String(currentSelectionId) ? "is-selected" : "";
        return `tree-node ${sexClass(d.person.sex)} ${hasExpandable(d.id) ? "has-expand" : ""} ${
          d.isTrunk ? "is-trunk" : ""
        } ${sel}`;
      })
      .transition()
      .duration(180)
      .attr("transform", (d) => {
        const p = pos.get(d.id) || { x: 0, y: 0 };
        return `translate(${p.x},${p.y})`;
      });

    nodesSel.exit().remove();

    // Fons: des-selecciona
    svg.on("click", () => {
      currentSelectionId = null;
      closeDrawer();
      gNodes.selectAll("g.tree-node").classed("is-selected", false);
      setViewPersonTarget({ id: focusPersonId || window.rootPersonId });
    });

    if (fit) fitToView(lastFitBounds);

    // Info de visibles
    try {
      const visibleCount = typeof visible !== "undefined" && visible && visible.size != null ? visible.size : nodeData.length;
      setVisibleInfo(visibleCount);
    } catch (_) {}
  }

  function bindUi() {
    drawerClose?.addEventListener("click", (e) => {
      e.preventDefault();
      closeDrawer();
    });

    generationsSelect?.addEventListener("change", () => {
      render({ fit: true });
    });

    btnZoomIn?.addEventListener("click", () => {
      svg.transition().duration(120).call(zoomBehavior.scaleBy, 1.2);
    });

    btnZoomOut?.addEventListener("click", () => {
      svg.transition().duration(120).call(zoomBehavior.scaleBy, 1 / 1.2);
    });

    btnFit?.addEventListener("click", () => {
      fitToView(lastFitBounds);
    });

    btnToggleDrawer?.addEventListener("click", () => {
      setDrawerEnabled(!drawerEnabled);
      fitToView(lastFitBounds);
    });

    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape") closeDrawer();
    });
  }

  // Arrencada
  setupSvgLayers();
  bindUi();
  setDrawerEnabled(true);
  setViewPersonTarget({ id: focusPersonId || window.rootPersonId });
  render({ fit: true });
})();
