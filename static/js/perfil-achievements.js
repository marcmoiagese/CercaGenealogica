document.addEventListener('DOMContentLoaded', function () {
    const panel = document.querySelector('.achievements-panel');
    if (!panel) return;

    const showcaseEl = document.getElementById('achievements-showcase');
    const galleryEl = document.getElementById('achievements-gallery');
    const emptyShowcaseEl = document.getElementById('achievements-showcase-empty');
    const emptyGalleryEl = document.getElementById('achievements-gallery-empty');
    const loadingEl = document.getElementById('achievements-loading');
    const errorEl = document.getElementById('achievements-error');

    const slotCount = parseInt(showcaseEl ? showcaseEl.dataset.slotCount : "6", 10) || 6;
    const csrfToken = panel.dataset.csrf || "";
    const emptyOptionLabel = panel.dataset.emptyOption || "None";
    const selectLabel = panel.dataset.selectLabel || "Select";
    const errorLabel = panel.dataset.errorLabel || "Error";

    function setLoading(isLoading) {
        if (!loadingEl) return;
        loadingEl.style.display = isLoading ? 'block' : 'none';
    }

    function showError(message) {
        if (!errorEl) return;
        errorEl.textContent = message || errorLabel;
        errorEl.style.display = 'block';
    }

    function clearError() {
        if (!errorEl) return;
        errorEl.textContent = '';
        errorEl.style.display = 'none';
    }

    function formatDate(value) {
        if (!value) return "";
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) {
            return value;
        }
        return date.toLocaleDateString();
    }

    function buildIcon(url) {
        if (url) {
            const img = document.createElement('img');
            img.src = url;
            img.alt = "";
            img.className = 'achievement-icon';
            return img;
        }
        const placeholder = document.createElement('div');
        placeholder.className = 'achievement-placeholder';
        placeholder.textContent = "+";
        return placeholder;
    }

    function updateEmptyState(container, emptyEl) {
        if (!emptyEl || !container) return;
        const hasChildren = container.children.length > 0;
        emptyEl.style.display = hasChildren ? 'none' : 'block';
    }

    function setShowcaseSlot(slotEl, item) {
        const header = slotEl.querySelector('header');
        if (!header) return;
        header.innerHTML = "";
        header.appendChild(buildIcon(item ? item.icon_url : ""));
        const titleWrap = document.createElement('div');
        const title = document.createElement('strong');
        title.textContent = item ? item.name : selectLabel;
        titleWrap.appendChild(title);
        if (item && item.rarity) {
            const meta = document.createElement('div');
            meta.className = 'achievement-meta';
            meta.textContent = item.rarity;
            titleWrap.appendChild(meta);
        }
        header.appendChild(titleWrap);
    }

    function renderShowcase(items, showcase) {
        if (!showcaseEl) return;
        showcaseEl.innerHTML = "";
        const bySlot = {};
        (showcase || []).forEach(function (entry) {
            if (entry && entry.slot) {
                bySlot[entry.slot] = entry;
            }
        });
        for (let slot = 1; slot <= slotCount; slot++) {
            const slotEl = document.createElement('div');
            slotEl.className = 'achievement-slot';
            const header = document.createElement('header');
            slotEl.appendChild(header);
            const select = document.createElement('select');
            const emptyOption = document.createElement('option');
            emptyOption.value = "0";
            emptyOption.textContent = emptyOptionLabel;
            select.appendChild(emptyOption);
            items.forEach(function (item) {
                const opt = document.createElement('option');
                opt.value = String(item.id);
                opt.textContent = item.name;
                select.appendChild(opt);
            });
            const current = bySlot[slot] || null;
            if (current && current.id) {
                select.value = String(current.id);
            }
            setShowcaseSlot(slotEl, current);
            select.addEventListener('change', function () {
                const chosen = parseInt(select.value, 10) || 0;
                const target = items.find(i => i.id === chosen);
                setShowcaseSlot(slotEl, target || null);
                saveShowcaseSlot(slot, chosen);
            });
            slotEl.appendChild(select);
            showcaseEl.appendChild(slotEl);
        }
        if (emptyShowcaseEl) {
            emptyShowcaseEl.style.display = Object.keys(bySlot).length === 0 ? 'block' : 'none';
        }
    }

    function renderGallery(items) {
        if (!galleryEl) return;
        galleryEl.innerHTML = "";
        items.forEach(function (item) {
            const card = document.createElement('article');
            card.className = 'achievement-card';
            const header = document.createElement('header');
            header.appendChild(buildIcon(item.icon_url));
            const titleWrap = document.createElement('div');
            const title = document.createElement('strong');
            title.textContent = item.name;
            titleWrap.appendChild(title);
            if (item.rarity) {
                const meta = document.createElement('div');
                meta.className = 'achievement-meta';
                meta.textContent = item.rarity;
                titleWrap.appendChild(meta);
            }
            header.appendChild(titleWrap);
            card.appendChild(header);
            if (item.description) {
                const desc = document.createElement('p');
                desc.textContent = item.description;
                card.appendChild(desc);
            }
            if (item.awarded_at) {
                const awarded = document.createElement('div');
                awarded.className = 'achievement-meta';
                awarded.textContent = formatDate(item.awarded_at);
                card.appendChild(awarded);
            }
            galleryEl.appendChild(card);
        });
        updateEmptyState(galleryEl, emptyGalleryEl);
    }

    function saveShowcaseSlot(slot, achievementID) {
        if (!csrfToken) return;
        fetch('/api/perfil/achievements/showcase', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRF-Token': csrfToken,
            },
            credentials: 'same-origin',
            body: JSON.stringify({
                slot: slot,
                achievement_id: achievementID,
            }),
        }).then(function (res) {
            if (!res.ok) {
                throw new Error('save failed');
            }
        }).catch(function () {
            showError(errorLabel);
        });
    }

    setLoading(true);
    clearError();
    fetch('/api/perfil/achievements', { credentials: 'same-origin' })
        .then(function (res) {
            if (!res.ok) {
                throw new Error('load failed');
            }
            return res.json();
        })
        .then(function (payload) {
            const items = Array.isArray(payload.items) ? payload.items : [];
            const showcase = Array.isArray(payload.showcase) ? payload.showcase : [];
            renderShowcase(items, showcase);
            renderGallery(items);
        })
        .catch(function () {
            showError(errorLabel);
        })
        .finally(function () {
            setLoading(false);
        });
});
