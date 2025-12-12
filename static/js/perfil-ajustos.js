document.addEventListener('DOMContentLoaded', function () {
    // Gestió de tabs
    const tabButtons = document.querySelectorAll('.perfil-ajustos-tabs .tab-boto');
    const tabPanes = document.querySelectorAll('.perfil-ajustos-tabs .tab-pane');

    tabButtons.forEach(function (button) {
        button.addEventListener('click', function () {
            const target = this.dataset.tab;
            if (!target) return;

            // Amaga alertes quan canvies de pestanya
            document.querySelectorAll('.alert').forEach(a => a.style.display = 'none');

            // Actualitza la URL perquè reflecteixi la pestanya activa
            const url = new URL(window.location.href);
            url.searchParams.set('tab', target);
            window.history.replaceState({}, '', url.toString());

            tabButtons.forEach(b => {
                b.classList.remove('actiu');
                b.setAttribute('aria-selected', 'false');
            });
            this.classList.add('actiu');
            this.setAttribute('aria-selected', 'true');

            tabPanes.forEach(pane => pane.classList.remove('actiu'));
            const paneActiu = document.getElementById('tab-' + target);
            if (paneActiu) {
                paneActiu.classList.add('actiu');
            }
        });
    });

    // Mostrar/ocultar contrasenya
    document.querySelectorAll('[data-toggle-password]').forEach(function (toggleBtn) {
        const targetId = toggleBtn.getAttribute('data-toggle-password');
        const input = document.getElementById(targetId);
        if (!input) return;

        toggleBtn.addEventListener('click', function () {
            const isPassword = input.type === 'password';
            input.type = isPassword ? 'text' : 'password';
            const icon = this.querySelector('i');
            if (icon) {
                icon.classList.toggle('fa-eye');
                icon.classList.toggle('fa-eye-slash');
            }
            this.classList.toggle('is-visible', isPassword);
        });
    });

    // Validació de coincidència de contrasenyes
    const passwordForm = document.getElementById('form-contrasenya');
    if (passwordForm) {
        const nova = document.getElementById('nova-contrasenya');
        const confirmar = document.getElementById('confirmar-contrasenya');
        const errorBox = document.getElementById('contrasenya-error');
        const submitBtn = passwordForm.querySelector('button[type="submit"]');

        function validarContrasenyes() {
            if (!nova.value && !confirmar.value) {
                errorBox.textContent = "";
                submitBtn.disabled = false;
                return;
            }

            if (nova.value !== confirmar.value) {
                errorBox.textContent = "Les contrasenyes no coincideixen.";
                submitBtn.disabled = true;
            } else {
                errorBox.textContent = "";
                submitBtn.disabled = false;
            }
        }

        nova.addEventListener('input', validarContrasenyes);
        confirmar.addEventListener('input', validarContrasenyes);

        passwordForm.addEventListener('submit', function (e) {
            if (nova.value !== confirmar.value) {
                e.preventDefault();
                errorBox.textContent = "Les contrasenyes no coincideixen.";
            }
        });
    }

    // Formulari d'eliminar compte
    const deleteForm = document.getElementById('form-eliminar-compte');
    if (deleteForm) {
        const passwordField = document.getElementById('eliminar-contrasenya');
        const checkbox = document.getElementById('confirmar-eliminacio');
        const submitDelete = deleteForm.querySelector('button[type="submit"]');

        function actualitzarBotoEliminar() {
            const habilitat = passwordField.value.trim() !== "" && checkbox.checked;
            submitDelete.disabled = !habilitat;
        }

        passwordField.addEventListener('input', actualitzarBotoEliminar);
        checkbox.addEventListener('change', actualitzarBotoEliminar);

        actualitzarBotoEliminar();
    }

    // Auto-amagar alertes i netejar query params de success/error
    const alerts = document.querySelectorAll('.alert');
    if (alerts.length) {
        setTimeout(() => {
            alerts.forEach(a => a.style.display = 'none');
        }, 4000);

        const url = new URL(window.location.href);
        url.searchParams.delete('success');
        url.searchParams.delete('error');
        window.history.replaceState({}, '', url.toString());
    }

    // Gestor de tags per a idiomes parlats
    const tagsContainer = document.getElementById('tags-idiomes');
    const tagsList = tagsContainer ? tagsContainer.querySelector('.tags-llista') : null;
    const tagsInput = document.getElementById('idiomes_parla_input');
    const hiddenInput = document.getElementById('idiomes_parla_hidden');
    const suggestionsList = tagsContainer ? tagsContainer.querySelector('.tags-suggestions') : null;
    // TODO: Omple aquesta llista amb tots els idiomes acceptats (codis o noms), un per string.
    const LANGUAGE_OPTIONS = [
        "Français",
        "Occitan",
        "Breton",
        "Corse",
        "Alsacien",
        "Español",
        "Català",
        "Gallego",
        "Euskara",
        "Aranés",
        "Italiano",
        "Tedesco",
        "Ladin",
        "Slovenščina",
        "Franco-provençal",
        "Sardo",
        "Deutsch",
        "Dänisch",
        "Friesisch",
        "Sorbi",
        "Nederlands",
        "Fries",
        "Português",
        "Mirandês",
        "English",
        "Cymraeg",
        "Gàidhlig",
        "Scots",
        "Gaelg",
        "Kernewek",
        "Irish",
        "Ulster Scots"
    ];
    if (tagsContainer && tagsList && tagsInput && hiddenInput && suggestionsList) {
        let tags = [];
        let suggestions = [];
        const renderTags = () => {
            tagsList.innerHTML = '';
            tags.forEach((tag, idx) => {
                const li = document.createElement('li');
                li.className = 'tag-item';
                li.textContent = tag;
                const btn = document.createElement('button');
                btn.type = 'button';
                btn.className = 'tag-remove';
                btn.innerHTML = '&times;';
                btn.addEventListener('click', () => removeTag(idx));
                li.appendChild(btn);
                tagsList.appendChild(li);
            });
            hiddenInput.value = tags.join(',');
        };
        const addTag = (value) => {
            const clean = value.trim();
            if (!clean) return;
            const exists = LANGUAGE_OPTIONS.some(opt => opt.toLowerCase() === clean.toLowerCase());
            if (!exists) return;
            // Normalitza al cas original de la llista
            const canonical = LANGUAGE_OPTIONS.find(opt => opt.toLowerCase() === clean.toLowerCase()) || clean;
            if (!tags.includes(canonical)) {
                tags.push(canonical);
                renderTags();
            }
            tagsInput.value = '';
            renderSuggestions('');
        };
        const removeTag = (idx) => {
            tags.splice(idx, 1);
            renderTags();
        };
        const renderSuggestions = (term) => {
            suggestionsList.innerHTML = '';
            if (!term) return;
            const lower = term.toLowerCase();
            suggestions = LANGUAGE_OPTIONS
                .filter(opt => opt.toLowerCase().includes(lower) && !tags.includes(opt))
                .slice(0, 3);
            suggestions.forEach(opt => {
                const li = document.createElement('li');
                li.textContent = opt;
                li.addEventListener('click', () => addTag(opt));
                suggestionsList.appendChild(li);
            });
        };
        // Inicialitza des de hidden
        if (hiddenInput.value) {
            hiddenInput.value.split(',').forEach(v => addTag(v));
        }
        tagsInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' || e.key === ',') {
                e.preventDefault();
                addTag(tagsInput.value);
            } else if (e.key === 'Backspace' && tagsInput.value === '') {
                tags.pop();
                renderTags();
            }
        });
        tagsInput.addEventListener('input', () => renderSuggestions(tagsInput.value));
        tagsInput.addEventListener('blur', () => {
            addTag(tagsInput.value);
            setTimeout(() => suggestionsList.innerHTML = '', 200);
        });
    }
});
