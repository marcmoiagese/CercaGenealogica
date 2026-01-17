(function() {
    const searchInput = document.getElementById('municipi-search');
    const suggestionsList = document.getElementById('municipi-suggestions');
    const typeLabelsEl = document.getElementById('municipi-type-labels');
    const levelTypesEl = document.getElementById('municipi-level-types');
    const typeLabels = typeLabelsEl ? JSON.parse(typeLabelsEl.textContent || '{}') : {};
    const levelTypes = levelTypesEl ? JSON.parse(levelTypesEl.textContent || '[]') : [];

    let lastItems = [];
    let activeIndex = -1;
    let debounceTimer;

    function clearSuggestions() {
        if (!suggestionsList) return;
        suggestionsList.innerHTML = '';
        suggestionsList.classList.remove('is-open');
        lastItems = [];
        activeIndex = -1;
    }

    function setActive(index) {
        if (!suggestionsList) return;
        const items = Array.from(suggestionsList.querySelectorAll('li'));
        items.forEach((el, idx) => {
            if (idx === index) {
                el.classList.add('is-active');
            } else {
                el.classList.remove('is-active');
            }
        });
        activeIndex = index;
    }

    function buildContext(item) {
        const names = Array.isArray(item.nivells_nom) ? item.nivells_nom : [];
        const types = Array.isArray(item.nivells_tipus) ? item.nivells_tipus : [];
        const country = names[0] || '';
        const chain = [];
        for (let i = 1; i < names.length; i++) {
            const name = names[i];
            if (!name) continue;
            const label = types[i] ? types[i] + ': ' + name : name;
            chain.push(label);
        }
        const parts = [];
        if (country) parts.push(country);
        if (chain.length) parts.push(chain.join(' / '));
        if (item.tipus && typeLabels[item.tipus]) {
            parts.push(typeLabels[item.tipus]);
        }
        return parts.join(' - ');
    }

    function renderSuggestions(items) {
        if (!suggestionsList || !searchInput) return;
        const emptyLabel = searchInput.dataset.emptyLabel || 'No results';
        suggestionsList.innerHTML = '';
        lastItems = items || [];
        activeIndex = -1;
        if (!items || items.length === 0) {
            const li = document.createElement('li');
            li.textContent = emptyLabel;
            li.className = 'suggestion-empty';
            suggestionsList.appendChild(li);
            suggestionsList.classList.add('is-open');
            return;
        }
        items.forEach((item, idx) => {
            const li = document.createElement('li');
            li.dataset.index = String(idx);
            const title = document.createElement('span');
            title.className = 'suggestion-title';
            let titleText = item.nom || '';
            if (item.tipus && typeLabels[item.tipus]) {
                titleText += ' (' + typeLabels[item.tipus] + ')';
            }
            title.textContent = titleText;
            const context = document.createElement('span');
            context.className = 'suggestion-context';
            context.textContent = buildContext(item);
            li.appendChild(title);
            if (context.textContent) {
                li.appendChild(context);
            }
            li.addEventListener('click', () => applySuggestion(item));
            suggestionsList.appendChild(li);
        });
        suggestionsList.classList.add('is-open');
    }

    function applySuggestion(item) {
        if (!item) return;
        const url = new URL(window.location.href);
        const params = url.searchParams;
        params.set('page', '1');
        if (searchInput && searchInput.dataset.perPage) {
            params.set('per_page', searchInput.dataset.perPage);
        }
        params.delete('q');
        params.delete('municipi_id');
        if (item.pais_id) {
            params.set('pais_id', String(item.pais_id));
        } else {
            params.delete('pais_id');
        }
        if (item.tipus) {
            params.set('tipus', item.tipus);
        } else {
            params.delete('tipus');
        }
        if (Array.isArray(item.nivells)) {
            for (let i = 0; i < 7; i++) {
                const key = 'nivell_id_' + (i + 1);
                const val = item.nivells[i];
                if (val) {
                    params.set(key, String(val));
                } else {
                    params.delete(key);
                }
            }
        }
        url.search = params.toString();
        window.location.href = url.pathname + url.search;
    }

    function fetchSuggestions(query) {
        if (!searchInput) return;
        const api = searchInput.dataset.api || '';
        if (!api) return;
        const countrySelect = document.getElementById('pais-select');
        const country = countrySelect && countrySelect.value ? countrySelect.value : '';
        const params = new URLSearchParams();
        params.set('q', query);
        params.set('limit', '10');
        if (country) {
            params.set('pais_id', country);
        }
        fetch(api + '?' + params.toString(), { credentials: 'same-origin' })
            .then(resp => resp.json())
            .then(data => {
                renderSuggestions(data.items || []);
            })
            .catch(() => {
                clearSuggestions();
            });
    }

    function handleInput() {
        if (!searchInput) return;
        const value = searchInput.value.trim();
        if (value.length < 1) {
            clearSuggestions();
            return;
        }
        clearTimeout(debounceTimer);
        debounceTimer = setTimeout(() => fetchSuggestions(value), 300);
    }

    if (searchInput) {
        searchInput.addEventListener('input', handleInput);
        searchInput.addEventListener('keydown', (event) => {
            if (!suggestionsList || suggestionsList.children.length === 0) return;
            if (event.key === 'ArrowDown') {
                event.preventDefault();
                const next = Math.min(activeIndex + 1, suggestionsList.children.length - 1);
                setActive(next);
            } else if (event.key === 'ArrowUp') {
                event.preventDefault();
                const prev = Math.max(activeIndex - 1, 0);
                setActive(prev);
            } else if (event.key === 'Enter') {
                if (lastItems.length > 0) {
                    event.preventDefault();
                    const item = lastItems[activeIndex >= 0 ? activeIndex : 0];
                    applySuggestion(item);
                }
            } else if (event.key === 'Escape') {
                clearSuggestions();
            }
        });
    }

    document.addEventListener('click', (event) => {
        if (!suggestionsList || !searchInput) return;
        if (event.target === searchInput || suggestionsList.contains(event.target)) {
            return;
        }
        clearSuggestions();
    });

    const infoModal = document.getElementById('municipi-info-modal');
    const infoTitle = document.getElementById('municipi-info-title');
    const infoType = document.getElementById('municipi-info-type');
    const infoList = document.getElementById('municipi-info-list');
    const googleLink = document.getElementById('municipi-map-google');
    const osmLink = document.getElementById('municipi-map-osm');
    const hereLink = document.getElementById('municipi-map-here');
    const mapyLink = document.getElementById('municipi-map-mapy');

    function buildQuery(name, country, levels) {
        const parts = [];
        if (name) parts.push(name);
        if (Array.isArray(levels)) {
            levels.forEach((val, idx) => {
                if (idx === 0) return;
                if (val) parts.push(val);
            });
        }
        if (country) parts.push(country);
        return parts.join(', ');
    }

    function setMapLinks(lat, lon, query) {
        const q = encodeURIComponent(query || '');
        if (lat && lon) {
            const coords = lat + ',' + lon;
            if (googleLink) googleLink.href = 'https://www.google.com/maps?q=' + coords;
            if (osmLink) osmLink.href = 'https://www.openstreetmap.org/?mlat=' + lat + '&mlon=' + lon + '#map=12/' + lat + '/' + lon;
            if (hereLink) hereLink.href = 'https://wego.here.com/?map=' + lat + ',' + lon + ',14,normal';
            if (mapyLink) mapyLink.href = 'https://mapy.com/zakladni?q=' + encodeURIComponent(coords);
        } else {
            if (googleLink) googleLink.href = 'https://www.google.com/maps/search/?api=1&query=' + q;
            if (osmLink) osmLink.href = 'https://www.openstreetmap.org/search?query=' + q;
            if (hereLink) hereLink.href = 'https://wego.here.com/search/' + q;
            if (mapyLink) mapyLink.href = 'https://mapy.com/zakladni?q=' + q;
        }
    }

    function openInfoModal(button) {
        if (!infoModal || !button) return;
        const name = button.dataset.name || '';
        const type = button.dataset.type || '';
        const country = button.dataset.pais || '';
        let levels = [];
        try {
            levels = JSON.parse(button.dataset.levels || '[]');
        } catch (e) {
            levels = [];
        }
        if (!Array.isArray(levels)) {
            levels = [];
        }
        const labelCountry = infoModal.dataset.labelCountry || 'Country';
        const labelType = infoModal.dataset.labelType || 'Type';
        const labelLevel = infoModal.dataset.labelLevel || 'Level';
        infoTitle.textContent = name;
        const typeLabel = typeLabels[type] || type;
        infoType.textContent = typeLabel ? labelType + ': ' + typeLabel : '';
        if (infoList) {
            infoList.innerHTML = '';
            if (country) {
                const dt = document.createElement('dt');
                dt.textContent = labelCountry;
                const dd = document.createElement('dd');
                dd.textContent = country;
                infoList.appendChild(dt);
                infoList.appendChild(dd);
            }
            levels.forEach((val, idx) => {
                if (!val || idx === 0) return;
                const dt = document.createElement('dt');
                const label = levelTypes[idx] || (labelLevel + ' ' + (idx + 1));
                dt.textContent = label;
                const dd = document.createElement('dd');
                dd.textContent = val;
                infoList.appendChild(dt);
                infoList.appendChild(dd);
            });
        }
        const lat = button.dataset.lat ? Number(button.dataset.lat) : 0;
        const lon = button.dataset.lon ? Number(button.dataset.lon) : 0;
        const query = buildQuery(name, country, levels);
        setMapLinks(lat || null, lon || null, query);
        infoModal.classList.add('is-open');
    }

    if (infoModal) {
        document.querySelectorAll('[data-municipi-info]').forEach(button => {
            button.addEventListener('click', () => openInfoModal(button));
        });
        const closeBtn = infoModal.querySelector('[data-municipi-info-close]');
        if (closeBtn) {
            closeBtn.addEventListener('click', () => infoModal.classList.remove('is-open'));
        }
        infoModal.addEventListener('click', (event) => {
            if (event.target === infoModal) {
                infoModal.classList.remove('is-open');
            }
        });
    }

    const deleteModal = document.getElementById('municipi-delete-modal');
    const deleteName = document.getElementById('municipi-delete-name');
    const deleteForm = document.getElementById('municipi-delete-form');

    function openDeleteModal(button) {
        if (!deleteModal || !deleteForm || !button) return;
        const id = button.dataset.id;
        const name = button.dataset.name || '';
        deleteForm.action = '/territori/municipis/' + id + '/delete';
        if (deleteName) {
            deleteName.textContent = name;
        }
        deleteModal.classList.add('is-open');
    }

    if (deleteModal) {
        document.querySelectorAll('[data-municipi-delete]').forEach(button => {
            button.addEventListener('click', () => openDeleteModal(button));
        });
        const closeBtn = deleteModal.querySelector('[data-municipi-delete-close]');
        const cancelBtn = deleteModal.querySelector('[data-municipi-delete-cancel]');
        [closeBtn, cancelBtn].forEach(btn => {
            if (!btn) return;
            btn.addEventListener('click', () => deleteModal.classList.remove('is-open'));
        });
        deleteModal.addEventListener('click', (event) => {
            if (event.target === deleteModal) {
                deleteModal.classList.remove('is-open');
            }
        });
    }
})();
