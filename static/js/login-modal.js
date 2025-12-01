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
    if (formRegistre) {
        formRegistre.addEventListener('submit', function(e) {
            // No prevenir l'enviament per defecte, només validar
            console.log('Validant formulari de registre...');
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
            
            console.log('Contrasenya:', contrasenya);
            console.log('Confirmar contrasenya:', confirmarContrasenya);
            console.log('Coincideixen:', contrasenya === confirmarContrasenya);
            
            if (contrasenya !== confirmarContrasenya) {
                e.preventDefault();
                console.log('Error: Les contrasenyes no coincideixen');
                alert('Les contrasenyes no coincideixen');
                return;
            }
            
            console.log('Formulari vàlid, enviant...');
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
