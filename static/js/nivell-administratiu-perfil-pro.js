(function () {
  'use strict';

  const $ = (s, root = document) => root.querySelector(s);

  function num(v) {
    return (typeof v === 'number' ? v : Number(v || 0)).toLocaleString('ca-ES');
  }

  function pct(v) {
    const n = typeof v === 'number' ? v : Number(v || 0);
    return `${n.toFixed(1).replace('.', ',')}%`;
  }

  function setText(id, value) {
    const el = document.getElementById(id);
    if (el) el.textContent = value ?? '—';
  }

  function setLink(id, href) {
    const el = document.getElementById(id);
    if (el && href) el.href = href;
  }

  function escapeHtml(s) {
    return String(s ?? '')
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#039;');
  }

  function parsePeriod(period) {
    const m = String(period || '').match(/(\d{4}).*?(\d{4})/);
    if (!m) return null;
    return { from: Number(m[1]), to: Number(m[2]) };
  }

  function renderKpis(kpis) {
    const wrap = $('#adminKpis');
    if (!wrap) return;
    wrap.innerHTML = '';

    (kpis || []).forEach(item => {
      const el = document.createElement('article');
      el.className = 'kpi-item';
      el.innerHTML = `
        <div class="kpi-icon"><i class="fas ${item.icon || 'fa-chart-bar'}"></i></div>
        <div>
          <div class="kpi-label">${item.label || '—'}</div>
          <div class="kpi-value">${typeof item.value === 'number' ? num(item.value) : (item.value || '—')}</div>
        </div>
      `;
      wrap.appendChild(el);
    });
  }

  function getTopIcons(data) {
    if (Array.isArray(data.top_icons) && data.top_icons.length) return data.top_icons;

    const totalBooks = (data.books || []).reduce((a, b) => a + Number(b.volums || 0), 0);
    const bookTypes = (data.books || []).length;
    const periods = (data.books || []).map(b => parsePeriod(b.period)).filter(Boolean);
    let from = null, to = null;
    if (periods.length) {
      from = Math.min(...periods.map(p => p.from));
      to = Math.max(...periods.map(p => p.to));
    }

    return [
      { icon: 'fa-book', label: 'Llibres', value: totalBooks || 0 },
      { icon: 'fa-layer-group', label: 'Tipus de llibre', value: bookTypes || 0 },
      { icon: 'fa-calendar', label: 'Període cobert', value: from && to ? `${from}–${to}` : '—' },
      { icon: 'fa-clipboard-check', label: 'Indexació', value: data.indexing?.total_pct != null ? pct(Number(data.indexing.total_pct)) : '—' }
    ];
  }

  function renderTopIcons(data) {
    const wrap = $('#adminTopIcons');
    if (!wrap) return;

    const icons = getTopIcons(data);
    wrap.innerHTML = '';

    icons.forEach(item => {
      const el = document.createElement('article');
      el.className = 'admin-icon-item';
      el.innerHTML = `
        <div class="admin-icon"><i class="fas ${item.icon || 'fa-circle'}"></i></div>
        <div>
          <div class="admin-icon-label">${item.label || '—'}</div>
          <div class="admin-icon-value">${typeof item.value === 'number' ? num(item.value) : (item.value || '—')}</div>
        </div>
      `;
      wrap.appendChild(el);
    });
  }

  function renderHierarchy(rows) {
    const ol = $('#adminHierarchy');
    if (!ol) return;
    ol.innerHTML = '';

    (rows || []).forEach(r => {
      const li = document.createElement('li');
      li.innerHTML = `
        <div class="admin-h-label">${r.label || ''}</div>
        <div class="admin-h-value">${r.href ? `<a href="${r.href}">${r.value || '—'}</a>` : (r.value || '—')}</div>
      `;
      ol.appendChild(li);
    });
  }

  function renderIndex(index) {
    const box = $('#indexGlobal');
    const list = $('#indexByType');
    if (!box || !list) return;

    const totalPct = Number(index?.total_pct || 0);
    const indexed = Number(index?.indexed || 0);
    const estimated = Number(index?.estimated || 0);

    box.innerHTML = `
      <div class="index-top">
        <strong>${pct(totalPct)}</strong>
        <span class="index-meta">${num(indexed)} / ${num(estimated)} registres</span>
      </div>
      <div class="progress"><span style="width:${Math.max(0, Math.min(100, totalPct))}%"></span></div>
    `;

    list.innerHTML = '';
    (index?.by_type || []).forEach(row => {
      const el = document.createElement('div');
      el.className = 'index-item';
      el.innerHTML = `
        <div class="index-row">
          <span class="name">${row.name || '—'}</span>
          <span class="pct">${pct(Number(row.pct || 0))}</span>
        </div>
        <div class="progress"><span style="width:${Math.max(0, Math.min(100, Number(row.pct || 0)))}%"></span></div>
        <div class="index-row"><span class="meta">${num(row.indexed || 0)} / ${num(row.estimated || 0)}</span></div>
      `;
      list.appendChild(el);
    });
  }

  function renderUnitsTable(rows) {
    const tbody = $('#unitsTbody');
    if (!tbody) return;

    const normalized = (rows || []).map(r => ({ ...r }));
    const input = $('#unitsFilter');
    const prevBtn = $('#unitsPrev');
    const nextBtn = $('#unitsNext');
    const pageInfo = $('#unitsPageInfo');
    const pager = $('#unitsPagination');
    const pageSize = 10;
    let currentPage = 1;

    function filteredRows(filterText) {
      const q = filterText.trim().toLowerCase();
      if (!q) return normalized;
      return normalized.filter(r => {
        return String(r.name || '').toLowerCase().includes(q)
          || String(r.type || '').toLowerCase().includes(q)
          || String(r.level || '').toLowerCase().includes(q);
      });
    }

    function updatePager(total, totalPages) {
      if (pager) {
        pager.style.display = total > pageSize ? 'flex' : 'none';
      }
      if (pageInfo) {
        pageInfo.textContent = total > 0 ? `Pàgina ${currentPage} de ${totalPages} · ${num(total)} municipis` : '—';
      }
      if (prevBtn) prevBtn.disabled = currentPage <= 1;
      if (nextBtn) nextBtn.disabled = currentPage >= totalPages;
    }

    function paint(filterText = '') {
      const filtered = filteredRows(filterText);
      const totalPages = Math.max(1, Math.ceil(filtered.length / pageSize));
      if (currentPage > totalPages) currentPage = totalPages;
      const start = (currentPage - 1) * pageSize;
      const visible = filtered.slice(start, start + pageSize);

      tbody.innerHTML = '';
      if (!filtered.length) {
        tbody.innerHTML = `<tr><td colspan="7" class="muted">No hi ha resultats amb aquest filtre.</td></tr>`;
        updatePager(0, 1);
        return;
      }

      visible.forEach(r => {
        const tr = document.createElement('tr');
        const p = Number(r.index_pct || 0);
        tr.innerHTML = `
          <td><a class="link-inline" href="${r.href || '#'}">${r.name || '—'}</a></td>
          <td>${r.type || '—'}</td>
          <td><span class="level-tag">N${r.level || '—'}</span></td>
          <td>${num(r.municipis || 0)}</td>
          <td>${num(r.books || 0)}</td>
          <td>
            <div class="pct-cell">
              <span>${pct(p)}</span>
              <span class="micro"><span style="width:${Math.max(0, Math.min(100, p))}%"></span></span>
            </div>
          </td>
          <td><a class="enllaç" href="${r.href || '#'}">Obrir</a></td>
        `;
        tbody.appendChild(tr);
      });

      updatePager(filtered.length, totalPages);
    }

    if (input) {
      input.addEventListener('input', (e) => {
        currentPage = 1;
        paint(e.target.value);
      });
    }
    if (prevBtn) {
      prevBtn.addEventListener('click', () => {
        if (currentPage > 1) {
          currentPage -= 1;
          paint(input ? input.value : '');
        }
      });
    }
    if (nextBtn) {
      nextBtn.addEventListener('click', () => {
        currentPage += 1;
        paint(input ? input.value : '');
      });
    }
    paint('');
  }

  function renderBooks(rows) {
    const tbody = $('#booksTbody');
    if (!tbody) return;
    tbody.innerHTML = '';

    (rows || []).forEach(r => {
      const est = Number(r.estimated || 0);
      const idx = Number(r.indexed || 0);
      const p = est > 0 ? (idx / est) * 100 : 0;

      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${r.cat || '—'}</td>
        <td>${num(r.volums || 0)}</td>
        <td>${r.period || '—'}</td>
        <td>${num(est)}</td>
        <td>${num(idx)}</td>
        <td>
          <div class="pct-cell">
            <span>${pct(p)}</span>
            <span class="micro"><span style="width:${Math.max(0, Math.min(100, p))}%"></span></span>
          </div>
        </td>
      `;
      tbody.appendChild(tr);
    });
  }

  function normalizeDemoSeries(data) {
    if (data.demography_series?.births?.length) return data.demography_series;
    const rows = Array.isArray(data.demography) ? data.demography : [];

    const births = [];
    const marriages = [];
    const deaths = [];

    rows.forEach((r, idx) => {
      const lbl = String(r.label || '');
      const yearMatch = lbl.match(/(\d{4})/);
      const x = yearMatch ? Number(yearMatch[1]) : (idx + 1);
      births.push({ x, y: Number(r.births || 0) });
      marriages.push({ x, y: Number(r.marriages || 0) });
      deaths.push({ x, y: Number(r.deaths || 0) });
    });

    return { births, marriages, deaths };
  }

  function metric(label, value) {
    return `<div class="admin-metric"><div class="label">${escapeHtml(label)}</div><div class="value">${num(value || 0)}</div></div>`;
  }

  function sumSeries(series){
    if (!Array.isArray(series)) return 0;
    return series.reduce((a, p) => a + (Number(p.y) || 0), 0);
  }

  function labelFor(key){
    if (key === 'births') return 'Natalitat per període';
    if (key === 'marriages') return 'Matrimonis per període';
    if (key === 'deaths') return 'Defuncions per període';
    return 'Sèrie';
  }

  // Gràfic demogràfic gran (amb àrea suau)
  function renderLine(container, series, title) {
    if (!container) return;
    if (!Array.isArray(series) || series.length < 2) {
      container.innerHTML = `<div class="muted" style="padding:1rem;">Sense dades per dibuixar el gràfic.</div>`;
      return;
    }

    const rect = container.getBoundingClientRect();
    const W = Math.max(740, Math.min(1400, Math.round(rect.width || 980)));
    const H = 340;
    const padL = 56, padR = 24, padT = 44, padB = 36;

    const xs = series.map(p => Number(p.x));
    const ys = series.map(p => Number(p.y));
    const minY = 0;
    const maxY = Math.max(...ys, 1);
    const minX = Math.min(...xs);
    const maxX = Math.max(...xs);

    const x = (v) => padL + ((v - minX) / (maxX - minX || 1)) * (W - padL - padR);
    const y = (v) => (H - padB) - ((v - minY) / (maxY - minY || 1)) * (H - padT - padB);

    const points = series.map((p) => ({ x: x(Number(p.x)), y: y(Number(p.y)), xv: Number(p.x), yv: Number(p.y) }));
    const d = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x.toFixed(1)} ${p.y.toFixed(1)}`).join(' ');
    const areaD = `${d} L ${points[points.length - 1].x.toFixed(1)} ${(H - padB).toFixed(1)} L ${points[0].x.toFixed(1)} ${(H - padB).toFixed(1)} Z`;

    const gridLines = [0, .25, .5, .75, 1].map((t) => {
      const yy = padT + (1 - t) * (H - padT - padB);
      const val = Math.round(maxY * t);
      return `
        <line x1="${padL}" y1="${yy}" x2="${W - padR}" y2="${yy}" stroke="rgba(0,0,0,.08)" />
        <text x="${padL - 8}" y="${yy + 4}" text-anchor="end" font-size="12" fill="rgba(0,0,0,.46)">${num(val)}</text>
      `;
    }).join('');

    const circles = points.map((p) => `
      <circle cx="${p.x}" cy="${p.y}" r="3.6" fill="var(--secondary-color)">
        <title>${p.xv}: ${num(p.yv)}</title>
      </circle>
    `).join('');

    container.innerHTML = `
      <svg viewBox="0 0 ${W} ${H}" width="100%" height="336" role="img" aria-label="${escapeHtml(title)}" style="display:block">
        <defs>
          <linearGradient id="admArea" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stop-color="rgba(79,195,247,.35)" />
            <stop offset="100%" stop-color="rgba(79,195,247,.03)" />
          </linearGradient>
        </defs>
        ${gridLines}
        <path d="${areaD}" fill="url(#admArea)"></path>
        <path d="${d}" fill="none" stroke="var(--secondary-color)" stroke-width="3"></path>
        ${circles}
        <text x="${padL}" y="28" font-size="16" fill="rgba(0,0,0,.58)" font-weight="900">${escapeHtml(title)}</text>
        <text x="${W - padR}" y="28" font-size="15" fill="rgba(0,0,0,.45)" text-anchor="end">${minX}–${maxX}</text>
      </svg>
    `;
  }

  function normalizeTop(items, fallbackPrefix){
    return (items || []).map((it, idx) => {
      const label = it.label || it.name || `${fallbackPrefix} ${idx+1}`;
      const value = Number(it.value ?? it.count ?? 0);
      const series = Array.isArray(it.series) ? it.series : (Array.isArray(it.timeline) ? it.timeline : []);
      return { label, value, series };
    });
  }

  function renderTrendRows(container, rows, labelKey, valueKey, title) {
    if (!container) return;
    if (!Array.isArray(rows) || !rows.length) {
      container.innerHTML = `<div class="muted" style="padding:.9rem;">Sense dades.</div>`;
      return;
    }

    const max = Math.max(...rows.map(r => Number(r[valueKey]) || 0), 1);
    container.innerHTML = `
      <div class="admin-trend-panel" aria-label="${escapeHtml(title || '')}">
        ${rows.map(r => {
          const val = Number(r[valueKey]) || 0;
          const w = (val / max) * 100;
          return `
            <div class="admin-trend-row">
              <div class="admin-trend-label">${escapeHtml(r[labelKey] || '—')}</div>
              <div class="admin-trend-track"><span class="admin-trend-fill" style="width:${Math.max(2,w)}%"></span></div>
              <div class="admin-trend-value">${num(val)}</div>
            </div>
          `;
        }).join('')}
      </div>
    `;
  }

  function buildPeriodRows(series){
    const map = new Map();
    (series || []).forEach((p, idx) => {
      const rawX = p?.x;
      const y = Number(p?.y || 0);

      if (Number.isFinite(Number(rawX))) {
        const year = Number(rawX);
        const decade = Math.floor(year / 10) * 10;
        const key = String(decade);
        const label = `${decade}-${decade + 9}`;
        const curr = map.get(key) || { sort: decade, label, value: 0 };
        curr.value += y;
        map.set(key, curr);
      } else {
        const key = `${idx}-${rawX ?? 'període'}`;
        map.set(key, { sort: idx, label: String(rawX || 'Període'), value: y });
      }
    });

    return Array.from(map.values()).sort((a,b)=>a.sort-b.sort);
  }

  function setupTopBlock({rootId, totalsId, seriesId, selectId, items, title}) {
    const root = document.getElementById(rootId);
    const totalsEl = document.getElementById(totalsId);
    const seriesEl = document.getElementById(seriesId);
    const selectEl = document.getElementById(selectId);
    if (!root || !totalsEl || !seriesEl || !selectEl) return;

    if (!Array.isArray(items) || items.length === 0) {
      totalsEl.innerHTML = `<div class="muted" style="padding:.8rem 0;">Sense dades.</div>`;
      seriesEl.innerHTML = `<div class="muted" style="padding:.8rem 0;">Sense dades.</div>`;
      return;
    }

    // Totals (mateix patró visual amb barres i valor)
    const topTotals = items.slice(0, 12).map(it => ({ label: it.label, value: it.value }));
    renderTrendRows(totalsEl, topTotals, 'label', 'value', `${title} totals`);

    // Evolució (selector + barres per període)
    selectEl.innerHTML = items.slice(0, 40).map((it, i) => `<option value="${i}">${escapeHtml(it.label || '—')}</option>`).join('');

    function paintSeries() {
      const idx = Number(selectEl.value) || 0;
      const selected = items[idx] || items[0];
      const periodRows = buildPeriodRows(selected?.series || []);
      renderTrendRows(seriesEl, periodRows, 'label', 'value', `${title} evolució`);
    }

    paintSeries();
    selectEl.addEventListener('change', paintSeries);

    const tabTotals = root.querySelector('[data-top="totals"]');
    const tabSeries = root.querySelector('[data-top="series"]');
    const selRow = root.querySelector('.admin-select-row');

    if (tabTotals && tabSeries && selRow) {
      tabTotals.addEventListener('click', () => {
        tabTotals.classList.add('is-active');
        tabSeries.classList.remove('is-active');
        totalsEl.style.display = 'block';
        selRow.style.display = 'none';
        seriesEl.style.display = 'none';
      });
      tabSeries.addEventListener('click', () => {
        tabSeries.classList.add('is-active');
        tabTotals.classList.remove('is-active');
        totalsEl.style.display = 'none';
        selRow.style.display = 'flex';
        seriesEl.style.display = 'block';
      });

      // Per defecte: Totals (mateixa lògica del perfil municipi)
      totalsEl.style.display = 'block';
      selRow.style.display = 'none';
      seriesEl.style.display = 'none';
    }

    return {
      rerender: () => {
        renderTrendRows(totalsEl, topTotals, 'label', 'value', `${title} totals`);
        if (seriesEl.style.display !== 'none') paintSeries();
      }
    };
  }

  function renderEvents(rows) {
    const wrap = $('#historyEvents');
    if (!wrap) return;
    wrap.innerHTML = '';

    (rows || []).forEach(r => {
      const el = document.createElement('article');
      el.className = 't-item';
      el.innerHTML = `
        <div class="t-year">${r.year || '—'}</div>
        <div>
          <div class="t-title">${r.title || '—'}</div>
          <div class="t-text">${r.text || ''}</div>
          <span class="t-tag">${r.tag || 'general'}</span>
        </div>
      `;
      wrap.appendChild(el);
    });
  }

  function renderHistory(paragraphs) {
    const box = $('#historyText');
    if (!box) return;
    box.innerHTML = '';

    (paragraphs || []).forEach(t => {
      const p = document.createElement('p');
      p.textContent = t;
      box.appendChild(p);
    });
  }

  function renderMemo(rows) {
    const box = $('#memoList');
    if (!box) return;
    box.innerHTML = '';

    (rows || []).forEach(r => {
      const el = document.createElement('article');
      el.className = 'memo-item';
      el.innerHTML = `
        <h3>${r.title || '—'}</h3>
        <p>${r.text || ''}</p>
        <div class="memo-source">${r.source || ''}</div>
      `;
      box.appendChild(el);
    });
  }

  function boot() {
    const raw = $('#adminLevelData');
    if (!raw) return;

    let data;
    try {
      data = JSON.parse(raw.textContent || '{}');
    } catch {
      return;
    }

    setText('adminName', data.name || '—');
    setText('adminSubtitle', data.subtitle || '—');
    setText('adminLevel', data.level || '—');
    setText('adminStatus', data.status || '—');
    setText('adminCountry', data.country || '—');

    setText('kvLevel', data.level || '—');
    setText('kvCode', data.code || '—');
    setText('kvMunicipis', num(data.municipis || 0));
    setText('kvUpdated', data.updated_at || '—');

    setLink('ctaLlibres', data.links?.llibres);
    setLink('ctaMunicipis', data.links?.municipis);
    setLink('ctaHist', data.links?.historia);

    renderKpis(data.kpis || []);
    renderTopIcons(data);
    renderHierarchy(data.hierarchy || []);
    renderIndex(data.indexing || {});
    renderUnitsTable(data.units || []);
    renderBooks(data.books || []);

    const demoSeries = normalizeDemoSeries(data);
    let activeSeriesKey = 'births';

    const lineEl = document.getElementById('adminLineChart');
    const metricsEl = document.getElementById('adminMetrics');

    function paintDemography() {
      if (metricsEl) {
        metricsEl.innerHTML = [
          metric('Naixements', sumSeries(demoSeries.births || [])),
          metric('Matrimonis', sumSeries(demoSeries.marriages || [])),
          metric('Defuncions', sumSeries(demoSeries.deaths || [])),
          metric('Períodes', Math.max((demoSeries.births || []).length, (demoSeries.marriages || []).length, (demoSeries.deaths || []).length))
        ].join('');
      }
      renderLine(lineEl, demoSeries[activeSeriesKey] || [], labelFor(activeSeriesKey));
    }

    document.querySelectorAll('.admin-tab[data-series]').forEach((btn) => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.admin-tab[data-series]').forEach(b => b.classList.remove('is-active'));
        btn.classList.add('is-active');
        activeSeriesKey = btn.dataset.series || 'births';
        paintDemography();
      });
    });

    const names = normalizeTop(data.top_names || data.topNames || [], 'Nom');
    const surnames = normalizeTop(data.top_surnames || data.topSurnames || [], 'Cognom');

    const topNamesController = setupTopBlock({
      rootId: 'topNamesBlock',
      totalsId: 'adminTopNamesTotals',
      seriesId: 'adminTopNamesSeries',
      selectId: 'adminTopNamesSelect',
      items: names,
      title: 'Noms'
    });

    const topSurnamesController = setupTopBlock({
      rootId: 'topSurnamesBlock',
      totalsId: 'adminTopSurnamesTotals',
      seriesId: 'adminTopSurnamesSeries',
      selectId: 'adminTopSurnamesSelect',
      items: surnames,
      title: 'Cognoms'
    });

    renderEvents(data.events || []);
    renderHistory(data.history || []);
    renderMemo(data.memo || []);
    paintDemography();

    let resizeT = null;
    window.addEventListener('resize', () => {
      clearTimeout(resizeT);
      resizeT = setTimeout(() => {
        paintDemography();
        if (topNamesController?.rerender) topNamesController.rerender();
        if (topSurnamesController?.rerender) topSurnamesController.rerender();
      }, 140);
    });
  }

  document.addEventListener('DOMContentLoaded', boot);
})();
