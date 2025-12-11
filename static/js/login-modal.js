document.addEventListener('DOMContentLoaded', function () {
    const modalLogin = document.getElementById('modalLogin');
    const modalRegistre = document.getElementById('modalRegistre');
    const modalRecuperar = document.getElementById('modalRecuperar');
    const botoLogin = document.getElementById('botoLogin');
    const botoRegistre = document.getElementById('botoRegistre');
    const tancaLogin = document.getElementById('tancaModal');
    const tancaRegistre = document.getElementById('tancaModalRegistre');
    const tancaRecuperar = document.getElementById('tancaModalRecuperar');
    const mostrarRegistre = document.getElementById('mostrarRegistre');
    const mostrarRecuperar = document.getElementById('mostrarRecuperar');
    const tornaLogin = document.getElementById('tornaLogin');
    const tornaLoginRecuperar = document.getElementById('tornaLoginRecuperar');

    // Obre el modal d'inici de sessió
    if (botoLogin && modalLogin) {
        botoLogin.addEventListener('click', function (e) {
            e.preventDefault();
            modalLogin.style.display = 'flex';
        });
    }

    // Obre el modal de registre directament
    if (botoRegistre && modalRegistre) {
        botoRegistre.addEventListener('click', function (e) {
            e.preventDefault();
            modalLogin.style.display = 'none';
            modalRegistre.style.display = 'flex';
        });
    }

    // Tanca els modals amb la X
    [tancaLogin, tancaRegistre, tancaRecuperar].forEach(tanca => {
        if (tanca) {
            tanca.addEventListener('click', function () {
                modalLogin.style.display = 'none';
                modalRegistre.style.display = 'none';
                modalRecuperar.style.display = 'none';
            });
        }
    });

    // Mostra el modal de registre des del login
    if (mostrarRegistre) {
        mostrarRegistre.addEventListener('click', function (e) {
            e.preventDefault();
            modalLogin.style.display = 'none';
            modalRegistre.style.display = 'flex';
        });
    }

    // Torna al modal d'inici de sessió des del registre
    if (tornaLogin) {
        tornaLogin.addEventListener('click', function (e) {
            e.preventDefault();
            modalRegistre.style.display = 'none';
            modalLogin.style.display = 'flex';
        });
    }

    // Mostra el modal de recuperar contrasenya
    if (mostrarRecuperar) {
        mostrarRecuperar.addEventListener('click', function (e) {
            e.preventDefault();
            modalLogin.style.display = 'none';
            modalRecuperar.style.display = 'flex';
        });
    }

    // Torna al login des del recuperar
    if (tornaLoginRecuperar) {
        tornaLoginRecuperar.addEventListener('click', function (e) {
            e.preventDefault();
            modalRecuperar.style.display = 'none';
            modalLogin.style.display = 'flex';
        });
    }

    // Solicitud de recuperació de contrasenya
    const formRecuperar = document.getElementById('formRecuperar');
    const recoverMessage = document.getElementById('recoverMessage');
    if (formRecuperar) {
        formRecuperar.addEventListener('submit', async function (e) {
            e.preventDefault();
            const csrfInput = formRecuperar.querySelector('input[name="csrf_token"]') || document.querySelector('input[name="csrf_token"]');
            const formData = new FormData(formRecuperar);
            if (csrfInput) {
                formData.set('csrf_token', csrfInput.value);
            }

            const defaultSuccess = formRecuperar.dataset.success || '';
            const defaultError = formRecuperar.dataset.error || defaultSuccess;

            try {
                const resp = await fetch(formRecuperar.action || '/recuperar', {
                    method: 'POST',
                    body: formData,
                    credentials: 'same-origin',
                    headers: Object.assign(
                        { 'Accept': 'application/json' },
                        csrfInput ? { 'X-CSRF-Token': csrfInput.value } : {}
                    )
                });
                let data = {};
                try {
                    data = await resp.json();
                } catch (err) {
                    data = {};
                }
                const msg = data.message || data.error || (resp.ok ? defaultSuccess : defaultError);
                if (recoverMessage) {
                    recoverMessage.textContent = msg || '';
                    recoverMessage.className = resp.ok ? 'alert alert-success' : 'alert alert-error';
                    recoverMessage.style.display = 'block';
                }
            } catch (err) {
                if (recoverMessage) {
                    recoverMessage.textContent = defaultError || 'Error';
                    recoverMessage.className = 'alert alert-error';
                    recoverMessage.style.display = 'block';
                }
            }
        });
    }

    // Tanca modals fent clic fora
    window.addEventListener('click', function (event) {
        if (event.target === modalLogin) {
            modalLogin.style.display = 'none';
        } else if (event.target === modalRegistre) {
            modalRegistre.style.display = 'none';
        } else if (event.target === modalRecuperar) {
            modalRecuperar.style.display = 'none';
        }
    });
});

// Validació del formulari de registre
document.addEventListener('DOMContentLoaded', function() {
    const formRegistre = document.getElementById('formRegistre');
    const inputUsuari = document.getElementById('registre_usuari');
    const inputEmail = document.getElementById('registre_email');
    const statusUsuari = document.getElementById('statusUsuari');
    const statusEmail = document.getElementById('statusEmail');
    const errorUsuari = document.getElementById('errorUsuari');
    const errorEmail = document.getElementById('errorEmail');
    const csrfTokenInput = document.querySelector('input[name="csrf_token"]');

    async function checkAvailability(field, value) {
        if (!value) return null;
        const form = new FormData();
        form.append(field, value);
        if (csrfTokenInput) {
            form.append('csrf_token', csrfTokenInput.value);
        }
        try {
            const resp = await fetch('/api/check-availability', {
                method: 'POST',
                body: form,
                credentials: 'same-origin',
                headers: {
                    'X-CSRF-Token': csrfTokenInput ? csrfTokenInput.value : ''
                }
            });
            if (!resp.ok) return null;
            return await resp.json();
        } catch (err) {
            console.warn('No s\'ha pogut validar disponibilitat', err);
            return null;
        }
    }

    function setStatus(elStatus, elError, ok, msg) {
        if (!elStatus || !elError) return;
        elStatus.className = 'status-icon';
        elError.textContent = '';
        if (ok === true) {
            elStatus.classList.add('ok');
            elStatus.textContent = '✔';
        } else if (ok === false) {
            elStatus.classList.add('error');
            elStatus.textContent = '✖';
            elError.textContent = msg || '';
        } else {
            elStatus.textContent = '';
        }
    }

    if (inputUsuari) {
        inputUsuari.addEventListener('blur', async () => {
            const val = inputUsuari.value.trim();
            if (!val) {
                setStatus(statusUsuari, errorUsuari, null);
                return;
            }
            const data = await checkAvailability('username', val);
            if (data && data.usernameTaken) {
                setStatus(statusUsuari, errorUsuari, false, 'Usuari no disponible');
            } else {
                setStatus(statusUsuari, errorUsuari, true);
            }
        });
    }

    if (inputEmail) {
        inputEmail.addEventListener('blur', async () => {
            const val = inputEmail.value.trim();
            if (!val) {
                setStatus(statusEmail, errorEmail, null);
                return;
            }
            const data = await checkAvailability('email', val);
            if (data && data.emailTaken) {
                setStatus(statusEmail, errorEmail, false, 'Email ja registrat');
            } else {
                setStatus(statusEmail, errorEmail, true);
            }
        });
    }

    if (formRegistre) {
        formRegistre.addEventListener('submit', function(e) {
            // No prevenir l'enviament per defecte, només validar
            const emailInput = document.getElementById('registre_email');
            
            // Validar que s'acceptin les condicions
            const acceptaCondicions = document.getElementById('registre_accepta_condicions');
            if (!acceptaCondicions.checked) {
                e.preventDefault();
                console.log('Error: No s\'han acceptat les condicions');
                
                // Mostrar error
                const checkboxGrup = acceptaCondicions.closest('.checkbox-grup');
                checkboxGrup.classList.add('error');
                
                // Crear missatge d'error si no existeix
                let errorMsg = checkboxGrup.querySelector('.error-missatge');
                if (!errorMsg) {
                    errorMsg = document.createElement('div');
                    errorMsg.className = 'error-missatge';
                    errorMsg.style.color = '#e74c3c';
                    errorMsg.style.fontSize = '0.8rem';
                    errorMsg.style.marginTop = '0.5rem';
                    errorMsg.textContent = 'Has d\'acceptar les condicions d\'ús per continuar';
                    checkboxGrup.appendChild(errorMsg);
                }
                
                // Scroll suau cap al checkbox
                acceptaCondicions.scrollIntoView({ behavior: 'smooth', block: 'center' });
                return;
            }
            
            // Eliminar error si existeix
            const checkboxGrup = acceptaCondicions.closest('.checkbox-grup');
            checkboxGrup.classList.remove('error');
            const errorMsg = checkboxGrup.querySelector('.error-missatge');
            if (errorMsg) {
                errorMsg.remove();
            }

            // Validar format d'email senzill
            if (emailInput) {
                const emailVal = emailInput.value.trim();
                const emailRegex = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;
                if (!emailRegex.test(emailVal)) {
                    e.preventDefault();
                    console.log('Error: email invàlid');
                    alert('Introdueix un correu electrònic vàlid');
                    emailInput.focus();
                    return;
                }
            }
            
            // Validar contrasenyes
            const contrasenya = document.getElementById('registre_contrassenya').value;
            const confirmarContrasenya = document.getElementById('registre_confirmar_contrasenya').value;

            if (contrasenya !== confirmarContrasenya) {
                e.preventDefault();
                alert('Les contrasenyes no coincideixen');
                return;
            }
            // El formulari s'enviarà automàticament
        });
        
        // Eliminar error quan es marca el checkbox
        const acceptaCondicions = document.getElementById('registre_accepta_condicions');
        if (acceptaCondicions) {
            acceptaCondicions.addEventListener('change', function() {
                const checkboxGrup = this.closest('.checkbox-grup');
                if (this.checked) {
                    checkboxGrup.classList.remove('error');
                    const errorMsg = checkboxGrup.querySelector('.error-missatge');
                    if (errorMsg) {
                        errorMsg.remove();
                    }
                }
            });
        }
    }
});

// Gestió del formulari d'inici de sessió
document.addEventListener('DOMContentLoaded', function() {
    console.log('[DEBUG] DOM carregat, buscant formulari de login...');
    
    const formLogin = document.getElementById('formLogin');
    if (formLogin) {
        console.log('[DEBUG] Formulari de login trobat!');
        console.log('[DEBUG] Action del formulari:', formLogin.action);
        console.log('[DEBUG] Method del formulari:', formLogin.method);
        
        // TEMPORAL: Desactivem la validació per veure si el problema està aquí
        /*
        formLogin.addEventListener('submit', function(e) {
            console.log('[DEBUG] Event submit disparat!');
            
            // Validar CAPTCHA
            const captcha = document.getElementById('login_captcha');
            const usuari = document.getElementById('login_usuari');
            const contrasenya = document.getElementById('login_contrassenya');
            
            console.log('[DEBUG] Element captcha:', captcha);
            console.log('[DEBUG] Element usuari:', usuari);
            console.log('[DEBUG] Element contrasenya:', contrasenya);
            
            if (captcha && captcha.value !== '8') {
                e.preventDefault();
                alert('CAPTCHA incorrecte. La resposta és 8.');
                return;
            }
            
            console.log('[DEBUG] CAPTCHA correcte, enviant formulari...');
            // No fem preventDefault() - deixem que el formulari s'enviï normalment
        });
        */
        console.log('[DEBUG] JavaScript del formulari desactivat temporalment per debug');
    } else {
        console.log('[DEBUG] ERROR: No s\'ha trobat el formulari amb ID formLogin');
        console.log('[DEBUG] Elements disponibles amb "form":');
        const forms = document.querySelectorAll('form');
        forms.forEach((form, index) => {
            console.log(`  Form ${index}: id="${form.id}", action="${form.action}"`);
        });
    }
});
