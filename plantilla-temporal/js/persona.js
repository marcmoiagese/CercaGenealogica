document.addEventListener('DOMContentLoaded', function () {
    const botoOpcions = document.getElementById('botoOpcions');
    const dropdownOpcions = document.getElementById('dropdownOpcions');

    if (botoOpcions && dropdownOpcions) {
        botoOpcions.addEventListener('click', function (e) {
            e.preventDefault();
            e.stopPropagation();
            dropdownOpcions.style.display = dropdownOpcions.style.display === 'block' ? 'none' : 'block';
        });

        document.addEventListener('click', function (event) {
            if (!botoOpcions.contains(event.target) && !dropdownOpcions.contains(event.target)) {
                dropdownOpcions.style.display = 'none';
            }
        });
    } else {
        console.warn("No s'han trobat els elements del desplegable d'opcions.");
    }

    /* Modal */
    const modal = document.getElementById('modalSeguirPersona');
    const botoTanca = document.getElementById('tancaModalSeguir');

    // Funció per obrir el modal
    function obrirModalSeguir(event) {
        event.preventDefault();
        if (modal) modal.style.display = 'flex';
    }

    // Registra-la globalment per poder usar-la a l'HTML
    window.obrirModalSeguir = obrirModalSeguir;

    // Funció per tancar-lo amb la X
    if (botoTanca) {
        botoTanca.addEventListener('click', function () {
            if (modal) modal.style.display = 'none';
        });
    }

    // Tancar fent clic fora
    window.addEventListener('click', function (event) {
        if (event.target === modal) {
            modal.style.display = 'none';
        }
    });

    // Opcional: mostrar confirmació al enviar formulari
    const form = document.getElementById('formSeguirPersona');
    if (form) {
        form.addEventListener('submit', function (e) {
            e.preventDefault();
            alert("Persona afegida als seguiments!");
            modal.style.display = 'none';
        });
    }
});