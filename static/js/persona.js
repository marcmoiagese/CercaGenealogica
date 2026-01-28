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
    }

    const modal = document.getElementById('modalSeguirPersona');
    const botoTanca = document.getElementById('tancaModalSeguir');

    function obrirModalSeguir(event) {
        if (event) event.preventDefault();
        if (modal) modal.style.display = 'flex';
    }

    window.obrirModalSeguir = obrirModalSeguir;

    if (botoTanca) {
        botoTanca.addEventListener('click', function () {
            if (modal) modal.style.display = 'none';
        });
    }

    window.addEventListener('click', function (event) {
        if (event.target === modal) {
            modal.style.display = 'none';
        }
    });

    const form = document.getElementById('formSeguirPersona');
    if (form) {
        form.addEventListener('submit', function (e) {
            e.preventDefault();
            if (modal) modal.style.display = 'none';
        });
    }
});
