document.addEventListener('DOMContentLoaded', function () {
    // Gestió de tabs
    const tabButtons = document.querySelectorAll('.perfil-ajustos-tabs .tab-boto');
    const tabPanes = document.querySelectorAll('.perfil-ajustos-tabs .tab-pane');

    tabButtons.forEach(function (button) {
        button.addEventListener('click', function () {
            const target = this.dataset.tab;
            if (!target) return;

            // Actualitza estat visual dels botons
            tabButtons.forEach(b => {
                b.classList.remove('actiu');
                b.setAttribute('aria-selected', 'false');
            });
            this.classList.add('actiu');
            this.setAttribute('aria-selected', 'true');

            // Mostra/oculta panells
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
});
