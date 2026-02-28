document.addEventListener('DOMContentLoaded', () => {
  const extList = document.getElementById('externalTreesList');
  if (!extList) {
    return;
  }

  const extSummary = document.getElementById('externalTreesSummary');
  const extEmpty = document.getElementById('externalTreesEmpty');
  const extSearch = document.querySelector('.ext-search');
  const extSelect = document.querySelector('.ext-select');
  const feedback = document.getElementById('externalTreesFeedback');
  const feedbackIcon = feedback?.querySelector('i');
  const feedbackText = feedback?.querySelector('[data-feedback-text]');

  const api = extList.dataset.api || '';
  const postUrl = extList.dataset.post || '';
  const csrf = extList.dataset.csrf || '';
  const iconBase = (extList.dataset.iconBase || '/static/img/ext-sites').replace(/\/+$/, '');
  const summaryTemplate = extList.dataset.summaryTemplate || '{total}';
  const labels = {
    open: extList.dataset.labelOpen || 'Obrir',
    public: extList.dataset.labelPublic || 'Visible',
    account: extList.dataset.labelAccount || 'Compte',
    private: extList.dataset.labelPrivate || 'Privat',
    premium: extList.dataset.labelPremium || 'Premium',
    mixed: extList.dataset.labelMixed || 'Mixt',
    linkOne: extList.dataset.labelLinkOne || 'enllac',
    linkMany: extList.dataset.labelLinkMany || 'enllacos',
    appearanceOne: extList.dataset.labelAppearanceOne || 'aparicio',
    appearanceMany: extList.dataset.labelAppearanceMany || 'aparicions',
    uniqueOne: extList.dataset.labelUniqueOne || 'unic',
    uniqueMany: extList.dataset.labelUniqueMany || 'unics',
    siteOne: extList.dataset.labelSiteOne || 'web',
    siteMany: extList.dataset.labelSiteMany || 'webs',
  };
  const messages = {
    pending: extList.dataset.msgPending || '',
    dup: extList.dataset.msgDup || '',
    invalid: extList.dataset.msgInvalid || '',
    error: extList.dataset.msgError || '',
    login: extList.dataset.msgLogin || '',
  };

  let allGroups = [];

  function normalizeAccess(raw) {
    const val = String(raw || '').toLowerCase().trim();
    if (val === 'public') return 'public';
    if (val === 'account') return 'account';
    if (val === 'private') return 'private';
    if (val === 'premium') return 'premium';
    if (val === 'mixed') return 'mixed';
    return 'mixed';
  }

  function accessIcon(cat) {
    switch (cat) {
      case 'public': return 'fa-unlock';
      case 'account': return 'fa-user';
      case 'private': return 'fa-user-lock';
      case 'premium': return 'fa-crown';
      default: return 'fa-circle-half-stroke';
    }
  }

  function accessLabel(cat) {
    switch (cat) {
      case 'public': return labels.public;
      case 'account': return labels.account;
      case 'private': return labels.private;
      case 'premium': return labels.premium;
      default: return labels.mixed;
    }
  }

  function pluralize(val, one, many) {
    return val === 1 ? one : many;
  }

  function normalizeGroup(group) {
    const site = group?.site || {};
    const slug = String(site.slug || 'unknown');
    const name = String(site.name || slug);
    const accessMode = normalizeAccess(site.access_mode || 'mixed');
    let icon = String(site.icon_url || '');
    if (!icon) {
      icon = `${iconBase}/${slug}.svg`;
      if (slug === 'unknown') {
        icon = `${iconBase}/unknown.svg`;
      }
    } else if (!icon.startsWith('/') && !icon.startsWith('http://') && !icon.startsWith('https://')) {
      icon = `/${icon}`;
    }
    const items = Array.isArray(group?.items) ? group.items : [];
    const normalizedItems = items.map((item) => {
      const count = Number(item?.count || 0);
      return {
        url: String(item?.url || '').trim(),
        title: String(item?.title || '').trim(),
        meta: String(item?.meta || '').trim(),
        count: count > 0 ? count : 1,
      };
    }).filter((item) => item.url);

    return {
      site: {
        slug,
        name,
        accessMode,
        icon,
      },
      items: normalizedItems,
    };
  }

  function updateSummary(groups) {
    if (!extSummary) {
      return;
    }
    const totalLinks = groups.reduce((sum, group) => sum + group.items.length, 0);
    const totalAppearances = groups.reduce((sum, group) => {
      return sum + group.items.reduce((acc, item) => acc + (item.count || 0), 0);
    }, 0);
    const sites = groups.length;
    const linksLabel = pluralize(totalAppearances, labels.linkOne, labels.linkMany);
    const uniqueLabel = pluralize(totalLinks, labels.uniqueOne, labels.uniqueMany);
    const sitesLabel = pluralize(sites, labels.siteOne, labels.siteMany);
    extSummary.textContent = summaryTemplate
      .replace('{total}', String(totalAppearances))
      .replace('{linksLabel}', linksLabel)
      .replace('{unique}', String(totalLinks))
      .replace('{uniqueLabel}', uniqueLabel)
      .replace('{sites}', String(sites))
      .replace('{sitesLabel}', sitesLabel);
  }

  function render(groups) {
    extList.innerHTML = '';
    if (!groups.length) {
      updateSummary([]);
      if (extEmpty) extEmpty.style.display = '';
      return;
    }
    if (extEmpty) extEmpty.style.display = 'none';
    updateSummary(groups);

    groups.forEach((group) => {
      const details = document.createElement('details');
      details.className = 'ext-site';
      details.open = true;

      const summary = document.createElement('summary');
      summary.className = 'ext-site__summary';

      const logo = document.createElement('img');
      logo.className = 'ext-site__logo';
      logo.src = group.site.icon;
      logo.alt = '';
      logo.loading = 'lazy';

      const meta = document.createElement('div');
      meta.className = 'ext-site__meta';

      const name = document.createElement('div');
      name.className = 'ext-site__name';
      name.textContent = group.site.name || group.site.slug;

      const counts = document.createElement('div');
      counts.className = 'ext-site__counts';
      const appearances = group.items.reduce((sum, item) => sum + item.count, 0);
      const linkLabel = pluralize(group.items.length, labels.linkOne, labels.linkMany);
      const appearanceLabel = pluralize(appearances, labels.appearanceOne, labels.appearanceMany);
      counts.textContent = `${group.items.length} ${linkLabel} - ${appearances} ${appearanceLabel}`;

      meta.appendChild(name);
      meta.appendChild(counts);

      const badges = document.createElement('div');
      badges.className = 'ext-site__badges';

      const access = group.site.accessMode;
      const badge = document.createElement('span');
      badge.className = 'ext-badge';
      if (access === 'private') badge.classList.add('ext-badge--warn');
      if (access === 'premium') badge.classList.add('ext-badge--pay');
      if (access === 'mixed') badge.classList.add('ext-badge--muted');
      badge.innerHTML = `<i class="fas ${accessIcon(access)}"></i> ${accessLabel(access)}`;
      badges.appendChild(badge);

      const chev = document.createElement('i');
      chev.className = 'fas fa-chevron-down ext-site__chev';

      summary.appendChild(logo);
      summary.appendChild(meta);
      summary.appendChild(badges);
      summary.appendChild(chev);

      const ul = document.createElement('ul');
      ul.className = 'ext-links';

      group.items.forEach((item) => {
        const li = document.createElement('li');
        li.className = 'ext-link';

        const a = document.createElement('a');
        a.className = 'ext-link__a';
        a.href = item.url;
        a.target = '_blank';
        a.rel = 'noopener noreferrer';
        a.title = item.url;

        const t = document.createElement('div');
        t.className = 'ext-link__title';
        t.textContent = item.title || labels.open;

        const m = document.createElement('div');
        m.className = 'ext-link__meta';
        m.textContent = item.meta || '';

        a.appendChild(t);
        if (item.meta) a.appendChild(m);

        const right = document.createElement('div');
        right.className = 'ext-link__right';

        const openBtn = document.createElement('a');
        openBtn.className = 'ext-pill ext-pill--open';
        openBtn.href = item.url;
        openBtn.target = '_blank';
        openBtn.rel = 'noopener noreferrer';
        openBtn.innerHTML = `<i class="fas fa-arrow-up-right-from-square"></i> ${labels.open}`;
        right.appendChild(openBtn);

        if (item.count > 1) {
          const countPill = document.createElement('span');
          countPill.className = 'ext-pill ext-pill--count';
          countPill.textContent = `\u00d7${item.count}`;
          right.appendChild(countPill);
        }

        li.appendChild(a);
        li.appendChild(right);
        ul.appendChild(li);
      });

      details.appendChild(summary);
      details.appendChild(ul);
      extList.appendChild(details);
    });
  }

  function filterGroups() {
    const q = (extSearch?.value || '').toLowerCase().trim();
    const f = (extSelect?.value || 'all').toLowerCase().trim();
    const filtered = [];

    allGroups.forEach((group) => {
      if (f !== 'all' && group.site.accessMode !== f) {
        return;
      }
      const items = group.items.filter((item) => {
        if (!q) return true;
        const text = `${group.site.name} ${item.title} ${item.meta} ${item.url}`.toLowerCase();
        return text.includes(q);
      });
      if (items.length) {
        filtered.push({
          site: group.site,
          items,
        });
      }
    });

    render(filtered);
  }

  function loadExternalLinks() {
    if (!api) {
      render([]);
      return;
    }
    fetch(api, { headers: { 'Accept': 'application/json' }, credentials: 'same-origin' })
      .then(res => res.ok ? res.json() : Promise.reject(res))
      .then(data => {
        const raw = Array.isArray(data?.groups) ? data.groups : [];
        allGroups = raw.map(normalizeGroup);
        filterGroups();
      })
      .catch(() => {
        allGroups = [];
        render([]);
      });
  }

  function showFeedback(text, isError) {
    if (!feedback || !feedbackText) {
      return;
    }
    feedbackText.textContent = text;
    if (isError) {
      feedback.classList.add('ext-feedback--error');
      if (feedbackIcon) feedbackIcon.className = 'fas fa-triangle-exclamation';
    } else {
      feedback.classList.remove('ext-feedback--error');
      if (feedbackIcon) feedbackIcon.className = 'fas fa-circle-check';
    }
    feedback.style.display = '';
  }

  function hideFeedback() {
    if (!feedback) {
      return;
    }
    feedback.style.display = 'none';
  }

  extSearch?.addEventListener('input', filterGroups);
  extSelect?.addEventListener('change', filterGroups);

  const modal = document.getElementById('external-link-modal');
  const openButtons = document.querySelectorAll('[data-open-external-link]');
  const closeButtons = modal?.querySelectorAll('[data-close-external-link]');
  const form = modal?.querySelector('form');
  const urlInput = modal?.querySelector('#external-link-url');
  const titleInput = modal?.querySelector('#external-link-title');
  const modalError = modal?.querySelector('[data-external-link-error]');
  const submitBtn = modal?.querySelector('[data-external-link-submit]');

  function clearModalError() {
    if (!modalError) {
      return;
    }
    modalError.textContent = '';
    modalError.style.display = 'none';
  }

  function showModalError(text) {
    if (!modalError) {
      return;
    }
    modalError.textContent = text;
    modalError.style.display = '';
  }

  function openModal() {
    if (!modal) {
      return;
    }
    clearModalError();
    if (urlInput) urlInput.value = '';
    if (titleInput) titleInput.value = '';
    modal.classList.add('is-open');
    urlInput?.focus();
  }

  function closeModal() {
    if (!modal) {
      return;
    }
    modal.classList.remove('is-open');
  }

  openButtons.forEach((btn) => btn.addEventListener('click', openModal));
  closeButtons?.forEach((btn) => btn.addEventListener('click', closeModal));
  modal?.addEventListener('click', (event) => {
    if (event.target === modal) {
      closeModal();
    }
  });

  form?.addEventListener('submit', (event) => {
    event.preventDefault();
    if (!postUrl) {
      return;
    }
    hideFeedback();
    clearModalError();

    const payload = {
      url: urlInput?.value?.trim() || '',
      title: titleInput?.value?.trim() || '',
    };

    if (!payload.url) {
      showModalError(messages.invalid || 'URL no valida.');
      return;
    }

    if (submitBtn) submitBtn.disabled = true;

    fetch(postUrl, {
      method: 'POST',
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'X-CSRF-Token': csrf,
      },
      credentials: 'same-origin',
      body: JSON.stringify(payload),
    })
      .then(async (res) => {
        let data = {};
        try {
          data = await res.json();
        } catch (_) {
          data = {};
        }
        if (!res.ok) {
          if (res.status === 401 && messages.login) {
            showModalError(messages.login);
          } else if (data?.status === 'invalid' && messages.invalid) {
            showModalError(messages.invalid);
          } else {
            showModalError(messages.error || 'Error');
          }
          return;
        }
        if (data?.status === 'dup') {
          showModalError(messages.dup || 'Duplicat');
          return;
        }
        closeModal();
        if (messages.pending) {
          showFeedback(messages.pending, false);
        }
        loadExternalLinks();
      })
      .catch(() => {
        showModalError(messages.error || 'Error');
      })
      .finally(() => {
        if (submitBtn) submitBtn.disabled = false;
      });
  });

  loadExternalLinks();
});
