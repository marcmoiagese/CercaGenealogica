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