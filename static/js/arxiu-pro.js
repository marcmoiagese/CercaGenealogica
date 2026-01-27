(() => {
  const dataEl = document.getElementById("arxiuProData");
  if (!dataEl) return;

  const $ = (sel, el=document) => el.querySelector(sel);
  const $$ = (sel, el=document) => Array.from(el.querySelectorAll(sel));

  const fmtInt = (n) => (n ?? n === 0) ? n.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".") : "-";

  const parseYears = (cron) => {
    if (!cron) return {min:null, max:null};
    const m = cron.match(/(\d{4})/g);
    if (!m || !m.length) return {min:null, max:null};
    const ys = m.map(x => parseInt(x,10)).filter(Boolean);
    return {min: Math.min(...ys), max: Math.max(...ys)};
  };

  const normalizeBook = (b) => {
    const titol = (b?.titol || b?.nom_esglesia || "").toString().trim();
    const pagines = (typeof b?.pagines === "number")
      ? b.pagines
      : (b?.pagines != null ? parseInt(b.pagines, 10) : null);
    return {
      ...b,
      titol: titol || "-",
      tipus: (b?.tipus || "").toString().trim(),
      municipi: (b?.municipi || "").toString().trim(),
      cronologia: (b?.cronologia || "").toString().trim(),
      pagines: Number.isFinite(pagines) ? pagines : null
    };
  };

  const buildFacets = (books) => {
    const munMap = new Map();
    const tipMap = new Map();
    books.forEach((b) => {
      if (b.municipi) munMap.set(b.municipi, (munMap.get(b.municipi) || 0) + 1);
      if (b.tipus) tipMap.set(b.tipus, (tipMap.get(b.tipus) || 0) + 1);
    });
    const toSorted = (map) => Array.from(map, ([nom, count]) => ({nom, count}))
      .sort((a, b) => (b.count - a.count) || a.nom.localeCompare(b.nom, "ca", {sensitivity:"base"}));
    return {
      municipis: toSorted(munMap),
      tipus: toSorted(tipMap)
    };
  };

  const buildTotals = (books) => {
    const muni = new Set();
    let pagesSum = 0;
    let pagesCount = 0;
    let minYear = null;
    let maxYear = null;
    books.forEach((b) => {
      if (b.municipi) muni.add(b.municipi);
      if (typeof b.pagines === "number") {
        pagesSum += b.pagines;
        pagesCount += 1;
      }
      const yrs = parseYears(b.cronologia);
      if (yrs.min != null) minYear = (minYear == null) ? yrs.min : Math.min(minYear, yrs.min);
      if (yrs.max != null) maxYear = (maxYear == null) ? yrs.max : Math.max(maxYear, yrs.max);
    });
    return {
      llibres: books.length,
      municipis: muni.size,
      pagines: pagesCount ? pagesSum : null,
      any_min: minYear,
      any_max: maxYear
    };
  };

  const buildCobertura = (books) => {
    const segleMap = new Map();
    const typeMap = new Map();
    books.forEach((b) => {
      const yrs = parseYears(b.cronologia);
      if (yrs.min != null) {
        const segle = Math.floor((yrs.min - 1) / 100) + 1;
        segleMap.set(segle, (segleMap.get(segle) || 0) + 1);
      }
      const label = b.tipus || "Sense categoria";
      const entry = typeMap.get(label) || {tipus: label, count: 0, pages: 0, min: null, max: null};
      entry.count += 1;
      if (typeof b.pagines === "number") entry.pages += b.pagines;
      if (yrs.min != null) entry.min = (entry.min == null) ? yrs.min : Math.min(entry.min, yrs.min);
      if (yrs.max != null) entry.max = (entry.max == null) ? yrs.max : Math.max(entry.max, yrs.max);
      typeMap.set(label, entry);
    });
    const perSegle = Array.from(segleMap, ([segle, count]) => ({segle, count}))
      .sort((a, b) => a.segle - b.segle);
    const perTipus = Array.from(typeMap.values());
    return { per_segle: perSegle, per_tipus: perTipus };
  };

  const state = {
    data: null,
    filtered: [],
    q: "",
    municipi: "",
    tipus: "",
    sort: "titol",
    dir: "asc",
    limit: 25,
    offset: 0,
    showActions: false,
    labels: {
      edit: "Editar",
      pages: "Pagines",
      link: "Enllac"
    }
  };

  function setLoading(on) {
    const box = $("#booksTableBody");
    const meta = $("#booksMeta");
    const more = $("#btnMore");
    if (!box || !meta || !more) return;
    if (on) {
      meta.innerHTML = `<span class="count skeleton" style="display:inline-block;width:240px;height:16px;"></span>`;
      box.innerHTML = "";
      for (let i=0; i<8; i++) {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td><div class="skeleton" style="height:14px;width:${55 + Math.random()*35}%;"></div></td>
          <td><div class="skeleton" style="height:14px;width:80px;"></div></td>
          <td><div class="skeleton" style="height:14px;width:130px;"></div></td>
          <td><div class="skeleton" style="height:14px;width:120px;"></div></td>
          <td><div class="skeleton" style="height:14px;width:70px;"></div></td>
          <td><div class="skeleton" style="height:14px;width:70px;"></div></td>
          <td><div class="skeleton" style="height:14px;width:90px;"></div></td>`;
        box.appendChild(tr);
      }
      more.disabled = true;
    } else {
      more.disabled = false;
    }
  }

  function badgeClass(estat) {
    const s = (estat || "").toLowerCase();
    if (s.includes("public")) return "verd";
    if (s.includes("pendent") || s.includes("revis")) return "groc";
    return "gris";
  }

  function renderHero() {
    const a = state.data.arxiu || {};
    const estatEl = $("#arxiuEstat");
    const estatLabel = a.estat_label || a.estat || "-";

    $("#arxiuNom").textContent = a.nom || "Arxiu";
    const subEl = $("#arxiuSub");
    const subText = [
      a.tipus ? `${a.tipus}` : null,
      a.entitat_eclesiastica ? `${a.entitat_eclesiastica}` : null
    ].filter(Boolean).join(" / ");
    if (subEl) {
      subEl.textContent = subText || subEl.textContent || "Informacio de l'arxiu";
    }

    if (estatEl) {
      estatEl.textContent = estatLabel;
      estatEl.classList.remove("verd", "groc", "gris");
      estatEl.classList.add(badgeClass(a.estat || estatLabel));
    }

    $("#chipTipus").textContent = a.tipus || "-";
    $("#chipAcces").textContent = a.acces || "-";
    $("#chipCobertura").textContent = (a.totals?.any_min && a.totals?.any_max) ? `${a.totals.any_min}-${a.totals.any_max}` : "-";

    $("#kpiLlibres").textContent = fmtInt(a.totals?.llibres);
    $("#kpiMunicipis").textContent = fmtInt(a.totals?.municipis);
    $("#kpiPagines").textContent = fmtInt(a.totals?.pagines);
    $("#kpiAnys").textContent = (a.totals?.any_min && a.totals?.any_max) ? `${a.totals.any_min}-${a.totals.any_max}` : "-";

    // Sidebar
    $("#metaTipus").textContent = a.tipus || "-";
    $("#metaAcces").textContent = a.acces || "-";
    $("#metaEntitat").textContent = a.entitat_eclesiastica || "-";
    $("#metaMunicipi").textContent = a.municipi_nom || (a.municipi_id ? `#${a.municipi_id}` : "-");
    $("#metaAdreca").textContent = a.adreca || "-";
    $("#metaUbicacio").textContent = a.ubicacio || "-";
    const w3w = $("#metaWhat3Words");
    if (w3w) {
      w3w.textContent = a.what3words || "-";
    }

    const web = $("#metaWeb");
    if (web) {
      if (a.web) {
        web.innerHTML = `<a href="${a.web}" target="_blank" rel="noopener">${escapeHtml(a.web)}</a>`;
      } else {
        web.textContent = "-";
      }
    }

    const notes = $("#metaNotes");
    if (notes) notes.textContent = (a.notes && a.notes.trim()) ? a.notes.trim() : "-";
  }

  function buildSelectOptions(selectEl, items, placeholder) {
    selectEl.innerHTML = "";
    const opt0 = document.createElement("option");
    opt0.value = "";
    opt0.textContent = placeholder;
    selectEl.appendChild(opt0);

    items.forEach(it => {
      const opt = document.createElement("option");
      opt.value = it.nom;
      opt.textContent = `${it.nom} (${fmtInt(it.count)})`;
      selectEl.appendChild(opt);
    });
  }

  function normalize(s) {
    return (s || "")
      .toString()
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "");
  }

  function applyFilters() {
    const all = state.data.llibres || [];
    const qn = normalize(state.q);
    const mun = state.municipi;
    const tip = state.tipus;

    let res = all.filter(b => {
      if (mun && b.municipi !== mun) return false;
      if (tip && b.tipus !== tip) return false;
      if (qn) {
        const hay = normalize(`${b.titol} ${b.municipi} ${b.tipus} ${b.cronologia} ${b.signatura || ""}`);
        if (!hay.includes(qn)) return false;
      }
      return true;
    });

    const key = state.sort;
    const dir = state.dir === "desc" ? -1 : 1;
    res.sort((a,b) => {
      const av = (a[key] ?? "").toString();
      const bv = (b[key] ?? "").toString();
      if (key === "pagines") return dir * ((a.pagines ?? -1) - (b.pagines ?? -1));
      return dir * av.localeCompare(bv, "ca", {numeric:true, sensitivity:"base"});
    });

    state.filtered = res;
    state.offset = 0;
  }

  function setActiveFilterPills() {
    const host = $("#activeFilters");
    host.innerHTML = "";
    const pills = [];

    if (state.q) pills.push({k:"Cerca", v: state.q, clear: () => { $("#q").value=""; state.q=""; }});
    if (state.municipi) pills.push({k:"Municipi", v: state.municipi, clear: () => { $("#filtreMunicipi").value=""; state.municipi=""; }});
    if (state.tipus) pills.push({k:"Categoria", v: state.tipus, clear: () => { $("#filtreTipus").value=""; state.tipus=""; }});

    pills.forEach(p => {
      const el = document.createElement("span");
      el.className = "filter-pill";
      el.innerHTML = `${p.k}: <span class="muted">${escapeHtml(p.v)}</span> <button type="button" aria-label="Treure filtre">x</button>`;
      el.querySelector("button").addEventListener("click", () => {
        p.clear();
        applyFilters();
        renderBooks(true);
        setActiveFilterPills();
      });
      host.appendChild(el);
    });

    $("#btnClear").disabled = pills.length === 0;
  }

  function escapeHtml(s) {
    return (s || "").toString()
      .replaceAll("&","&amp;")
      .replaceAll("<","&lt;")
      .replaceAll(">","&gt;")
      .replaceAll("\"","&quot;")
      .replaceAll("'","&#039;");
  }

  function renderBooks(reset=false) {
    const body = $("#booksTableBody");
    const meta = $("#booksMeta");
    const btnMore = $("#btnMore");

    if (reset) body.innerHTML = "";

    const total = state.filtered.length;
    const start = state.offset;
    const end = Math.min(start + state.limit, total);
    const slice = state.filtered.slice(start, end);

    const frag = document.createDocumentFragment();
    slice.forEach(b => {
      const tr = document.createElement("tr");
      const link = b.href ? `<a href="${b.href}">${escapeHtml(b.titol)}</a>` : escapeHtml(b.titol);
      const ext = b.url ? `<a href="${b.url}" target="_blank" rel="noopener">${state.labels.link}</a>` : `<span style="color:var(--color-text-clar);">-</span>`;
      const sig = b.signatura ? escapeHtml(b.signatura) : `<span style="color:var(--color-text-clar);">-</span>`;
      tr.innerHTML = `
        <td>${link}</td>
        <td>${escapeHtml(b.tipus || "-")}</td>
        <td>${escapeHtml(b.cronologia || "-")}</td>
        <td>${escapeHtml(b.municipi || "-")}</td>
        <td>${b.pagines != null ? fmtInt(b.pagines) : "-"}</td>
        <td>${sig}</td>
        <td>${ext}</td>
      `;
      if (state.showActions) {
        const actions = [];
        if (b.can_edit) actions.push(`<a href=\"/documentals/llibres/${b.id}/edit\">${state.labels.edit}</a>`);
        if (b.can_view) actions.push(`<a href=\"/documentals/llibres/${b.id}/pagines\">${state.labels.pages}</a>`);
        const actionHtml = actions.length ? actions.join(" | ") : `<span style=\"color:var(--color-text-clar);\">-</span>`;
        const td = document.createElement("td");
        td.className = "accions";
        td.innerHTML = actionHtml;
        tr.appendChild(td);
      }
      frag.appendChild(tr);
    });
    body.appendChild(frag);

    meta.innerHTML = `
      <div class="count">${fmtInt(total)} resultats - mostrant ${fmtInt(end)} de ${fmtInt(total)}</div>
    `;

    state.offset = end;
    btnMore.style.display = (end < total) ? "inline-flex" : "none";
  }

  function renderMunicipis() {
    const host = $("#muniList");
    host.innerHTML = "";
    const items = state.data.facets?.municipis || [];
    const max = Math.max(1, ...items.map(i => i.count));

    items.slice(0, 24).forEach(it => {
      const row = document.createElement("div");
      row.className = "muni-row";
      const pct = Math.round((it.count / max) * 100);
      row.innerHTML = `
        <div>
          <strong>${escapeHtml(it.nom)}</strong><div style="color:var(--color-text-clar);font-size:0.85rem;">${fmtInt(it.count)} llibres</div>
        </div>
        <div class="mini-bar" aria-hidden="true"><span style="width:${pct}%"></span></div>
        <div class="muni-actions"><button class="boto-secundari" type="button">Filtra</button></div>
      `;
      row.querySelector("button").addEventListener("click", () => {
        $("#filtreMunicipi").value = it.nom;
        state.municipi = it.nom;
        applyFilters();
        renderBooks(true);
        setActiveFilterPills();
        activateTab("llibres");
      });
      host.appendChild(row);
    });
  }

  function renderCobertura() {
    const host = $("#coverageBars");
    host.innerHTML = "";
    const items = state.data.cobertura?.per_segle || [];
    const max = Math.max(1, ...items.map(i => i.count));
    items.slice(0, 18).forEach(it => {
      const row = document.createElement("div");
      row.className = "coverage-bar";
      const pct = Math.round((it.count / max) * 100);
      row.innerHTML = `
        <div><strong>S. ${it.segle}</strong></div>
        <div class="bar" aria-hidden="true"><span style="width:${pct}%"></span></div>
        <div style="text-align:right;color:var(--color-text-clar);font-weight:800;">${fmtInt(it.count)}</div>
      `;
      host.appendChild(row);
    });

    const hostTypes = $("#coverageTypeList");
    const btnMore = $("#coverageTypeMore");
    hostTypes.innerHTML = "";

    const titems = (state.data.cobertura?.per_tipus || []).slice();
    titems.sort((a,b) => (b.count||0) - (a.count||0));

    const INITIAL = 10;
    let expanded = false;

    const maxCount = Math.max(1, ...titems.map(x => x.count || 0));

    function renderList() {
      hostTypes.innerHTML = "";
      const show = expanded ? titems : titems.slice(0, INITIAL);

      show.forEach(it => {
        const span = (it.min && it.max) ? `${it.min}-${it.max}` : "-";
        const pct = Math.round(((it.count||0) / maxCount) * 100);

        const row = document.createElement("div");
        row.className = "cov-type-item";
        row.innerHTML = `
          <div class="cov-type-head">
            <div class="cov-type-name" title="${escapeHtml(it.tipus)}">${escapeHtml(it.tipus)}</div>
            <div class="cov-type-metrics">
              <span class="chip chip-muted" title="Periode">${escapeHtml(span)}</span>
              <span class="metric" title="Llibres"><strong>${fmtInt(it.count)}</strong> llibres</span>
              <span class="metric" title="Pagines"><strong>${fmtInt(it.pages)}</strong> pag.</span>
            </div>
          </div>
          <div class="bar" aria-hidden="true"><span style="width:${pct}%"></span></div>
        `;
        hostTypes.appendChild(row);
      });

      const hasMore = titems.length > INITIAL;
      if (btnMore) btnMore.style.display = hasMore ? "inline-flex" : "none";
      if (btnMore) btnMore.textContent = expanded ? "Mostra'n menys" : "Mostra'n mes";
      if (btnMore) btnMore.setAttribute("aria-expanded", expanded ? "true" : "false");
    }

    if (btnMore) {
      btnMore.onclick = () => { expanded = !expanded; renderList(); };
    }

    renderList();
  }

  function activateTab(id) {
    $$(".tab").forEach(btn => {
      const on = btn.dataset.tab === id;
      btn.setAttribute("aria-selected", on ? "true" : "false");
    });
    $$(".tabpanel").forEach(p => {
      p.classList.toggle("active", p.id === `tab-${id}`);
    });
  }

  function setTabVisible(tabId, visible) {
    const btn = document.querySelector(`.tab[data-tab="${tabId}"]`);
    const panel = document.getElementById(`tab-${tabId}`);
    if (btn) btn.style.display = visible ? "" : "none";
    if (panel) panel.style.display = visible ? "" : "none";
  }

  function applyConditionals() {
    const d = state.data || {};

    const donate = document.getElementById("donateCard");
    if (donate) {
      const ok = !!d.accepta_donacions;
      donate.style.display = ok ? "" : "none";

      if (ok) {
        const cfg = d.donacions || {};
        const title = donate.querySelector(".donate-title");
        const sub = donate.querySelector(".donate-sub");
        const link = donate.querySelector(".donate-link");

        if (title && cfg.title) title.textContent = cfg.title;
        if (sub && cfg.sub) sub.textContent = cfg.sub;
        if (link && cfg.url) link.href = cfg.url;
      }
    }

    const hasMunicipis =
      (d.facets?.municipis?.length || 0) > 0 ||
      (d.municipis?.length || 0) > 0;

    const hasCobertura =
      (d.cobertura?.per_segle?.length || 0) > 0 ||
      (d.cobertura?.per_tipus?.length || 0) > 0;

    setTabVisible("municipis", hasMunicipis);
    setTabVisible("cobertura", hasCobertura);

    const active = document.querySelector('.tab[aria-selected="true"]');
    if (active && active.style.display === "none") activateTab("llibres");
  }

  function setupTabs() {
    $$(".tab").forEach(btn => {
      btn.addEventListener("click", () => activateTab(btn.dataset.tab));
    });
  }

  function setupFilters() {
    const q = $("#q");
    const selM = $("#filtreMunicipi");
    const selT = $("#filtreTipus");
    const selS = $("#filtreSort");

    let t = null;
    q.addEventListener("input", () => {
      clearTimeout(t);
      t = setTimeout(() => {
        state.q = q.value.trim();
        applyFilters();
        renderBooks(true);
        setActiveFilterPills();
      }, 180);
    });

    selM.addEventListener("change", () => {
      state.municipi = selM.value;
      applyFilters();
      renderBooks(true);
      setActiveFilterPills();
    });

    selT.addEventListener("change", () => {
      state.tipus = selT.value;
      applyFilters();
      renderBooks(true);
      setActiveFilterPills();
    });

    selS.addEventListener("change", () => {
      const [sort, dir] = selS.value.split(":");
      state.sort = sort;
      state.dir = dir || "asc";
      applyFilters();
      renderBooks(true);
    });

    $("#btnClear").addEventListener("click", () => {
      q.value = "";
      selM.value = "";
      selT.value = "";
      state.q = "";
      state.municipi = "";
      state.tipus = "";
      applyFilters();
      renderBooks(true);
      setActiveFilterPills();
    });

    $("#btnMore").addEventListener("click", () => renderBooks(false));
  }

  function initLabels() {
    const table = document.querySelector(".taula");
    if (!table) return;
    state.showActions = table.dataset.showActions === "1" || state.data.show_actions === true;
    if (table.dataset.labelEdit) state.labels.edit = table.dataset.labelEdit;
    if (table.dataset.labelPages) state.labels.pages = table.dataset.labelPages;
    if (table.dataset.labelLink) state.labels.link = table.dataset.labelLink;
  }

  function init() {
    setLoading(true);

    let payload = {};
    try {
      payload = JSON.parse(dataEl.textContent || "{}");
    } catch (e) {
      console.error(e);
      $("#booksMeta").innerHTML = `<div class="count" style="color:#c0392b;font-weight:800;">No s'han pogut carregar les dades.</div>`;
      setLoading(false);
      return;
    }

    state.data = payload || {};
    state.data.llibres = Array.isArray(state.data.llibres) ? state.data.llibres.map(normalizeBook) : [];
    state.data.facets = state.data.facets || buildFacets(state.data.llibres);
    state.data.cobertura = state.data.cobertura || buildCobertura(state.data.llibres);
    state.data.arxiu = state.data.arxiu || {};
    if (!state.data.arxiu.totals) {
      state.data.arxiu.totals = buildTotals(state.data.llibres);
    }

    initLabels();
    renderHero();
    applyConditionals();
    setupTabs();

    buildSelectOptions($("#filtreMunicipi"), state.data.facets?.municipis || [], "Tots els municipis");
    buildSelectOptions($("#filtreTipus"), state.data.facets?.tipus || [], "Totes les categories");

    setupFilters();
    applyFilters();
    setActiveFilterPills();

    activateTab("llibres");
    setLoading(false);
    renderBooks(true);

    renderMunicipis();
    renderCobertura();
  }

  document.addEventListener("DOMContentLoaded", init);
})();
