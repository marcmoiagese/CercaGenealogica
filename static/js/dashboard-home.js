document.addEventListener('DOMContentLoaded', function () {
  const grid = document.getElementById('dashGrid');
  const modal = document.getElementById('dashCustomizeModal');
  const list = document.getElementById('dashWidgetList');
  const openBtn = document.getElementById('dashCustomizeBtn');
  const closeBtn = document.getElementById('dashCustomizeClose');
  const saveBtn = document.getElementById('dashCustomizeSave');
  const resetBtn = document.getElementById('dashCustomizeReset');
  const csrfInput = document.getElementById('dashCsrfToken');
  const quickInput = document.getElementById('dashQuickSearch');
  const quickBtn = document.getElementById('dashQuickSearchBtn');
  const csrfToken = csrfInput ? csrfInput.value : '';

  if (!grid || !modal || !list || !openBtn) {
    return;
  }

  const getWidgets = () => Array.from(grid.querySelectorAll('.widget[data-widget-id]'));

  const getWidgetMeta = (el) => {
    const id = (el.getAttribute('data-widget-id') || '').trim();
    const title = (el.getAttribute('data-widget-title') || '').trim();
    const orderVal = el.getAttribute('data-order') || el.style.order || '0';
    const defaultOrderVal = el.getAttribute('data-default-order') || orderVal || '0';
    const order = parseInt(orderVal, 10) || 0;
    const defaultOrder = parseInt(defaultOrderVal, 10) || order;
    const defaultHidden = el.getAttribute('data-default-hidden') === '1';
    const hidden = el.classList.contains('is-hidden');
    return { id, title: title || id, order, defaultOrder, defaultHidden, hidden, el };
  };

  const widgetMap = () => {
    const map = new Map();
    getWidgets().forEach((el) => {
      const meta = getWidgetMeta(el);
      map.set(meta.id, meta);
    });
    return map;
  };

  const sortedWidgets = () => {
    const items = getWidgets().map(getWidgetMeta);
    items.sort((a, b) => {
      if (a.order === b.order) {
        return a.id.localeCompare(b.id);
      }
      return a.order - b.order;
    });
    return items;
  };

  const buildList = () => {
    list.innerHTML = '';
    sortedWidgets().forEach((meta) => {
      const li = document.createElement('li');
      li.className = 'widget-item';
      li.setAttribute('data-widget-id', meta.id);
      li.setAttribute('draggable', 'true');

      const left = document.createElement('div');
      left.className = 'left';

      const handle = document.createElement('span');
      handle.className = 'drag-handle';
      handle.innerHTML = '<i class="fas fa-grip-vertical"></i>';

      const label = document.createElement('span');
      label.textContent = meta.title;

      left.appendChild(handle);
      left.appendChild(label);

      const toggle = document.createElement('input');
      toggle.type = 'checkbox';
      toggle.checked = !meta.hidden;

      li.appendChild(left);
      li.appendChild(toggle);
      list.appendChild(li);
    });
  };

  const applyLayout = (items) => {
    const map = widgetMap();
    items.forEach((item) => {
      const meta = map.get(item.id);
      if (!meta) {
        return;
      }
      meta.el.dataset.order = String(item.order);
      meta.el.style.order = item.order;
      meta.el.classList.toggle('is-hidden', item.hidden);
    });
  };

  const collectLayout = () => {
    return Array.from(list.children).map((li, index) => {
      const id = li.getAttribute('data-widget-id') || '';
      const checkbox = li.querySelector('input[type="checkbox"]');
      const hidden = checkbox ? !checkbox.checked : false;
      return { id, order: index + 1, hidden };
    });
  };

  const defaultsLayout = () => {
    const items = getWidgets().map(getWidgetMeta);
    items.sort((a, b) => a.defaultOrder - b.defaultOrder);
    return items.map((item, index) => ({
      id: item.id,
      order: item.defaultOrder || index + 1,
      hidden: item.defaultHidden,
    }));
  };

  const persistLayout = async (payload) => {
    const resp = await fetch('/api/dashboard/widgets', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRF-Token': csrfToken,
      },
      credentials: 'same-origin',
      body: JSON.stringify(payload),
    });
    if (!resp.ok) {
      throw new Error('request failed');
    }
    return resp.json();
  };

  const openModal = () => {
    buildList();
    modal.classList.add('open');
  };

  const closeModal = () => {
    modal.classList.remove('open');
  };

  openBtn.addEventListener('click', openModal);
  if (closeBtn) {
    closeBtn.addEventListener('click', closeModal);
  }
  modal.addEventListener('click', (e) => {
    if (e.target === modal) {
      closeModal();
    }
  });

  if (saveBtn) {
    saveBtn.addEventListener('click', async () => {
      const layout = collectLayout();
      applyLayout(layout);
      try {
        await persistLayout({ widgets: layout });
        closeModal();
      } catch (err) {
        // no-op: keep UI state, allow retry
      }
    });
  }

  if (resetBtn) {
    resetBtn.addEventListener('click', async () => {
      const layout = defaultsLayout();
      applyLayout(layout);
      buildList();
      try {
        await persistLayout({ reset: true });
      } catch (err) {
        // no-op: keep UI state, allow retry
      }
    });
  }

  let dragging = null;
  list.addEventListener('dragstart', (e) => {
    const item = e.target.closest('.widget-item');
    if (!item) {
      return;
    }
    dragging = item;
    item.classList.add('is-dragging');
    if (e.dataTransfer) {
      e.dataTransfer.effectAllowed = 'move';
    }
  });

  list.addEventListener('dragover', (e) => {
    if (!dragging) {
      return;
    }
    e.preventDefault();
    const target = e.target.closest('.widget-item');
    if (!target || target === dragging) {
      return;
    }
    const rect = target.getBoundingClientRect();
    const after = e.clientY - rect.top > rect.height / 2;
    list.insertBefore(dragging, after ? target.nextSibling : target);
  });

  list.addEventListener('dragend', () => {
    if (dragging) {
      dragging.classList.remove('is-dragging');
    }
    dragging = null;
  });

  list.addEventListener('drop', (e) => {
    if (dragging) {
      e.preventDefault();
    }
  });

  if (quickBtn && quickInput) {
    quickInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        quickBtn.click();
      }
    });
    quickBtn.addEventListener('click', () => {
      const q = quickInput.value.trim();
      if (!q) {
        return;
      }
      const url = '/cerca-avancada?entity=all&sort=relevance&page=1&page_size=25&q=' + encodeURIComponent(q);
      window.location.href = url;
    });
  }
});
