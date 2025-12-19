document.addEventListener('DOMContentLoaded', function () {
    const botoPerfil = document.getElementById('botoPerfil');
    const dropdownPerfil = document.getElementById('dropdownPerfil');

    if (botoPerfil && dropdownPerfil) {
        botoPerfil.addEventListener('click', function (e) {
            e.preventDefault();
            e.stopPropagation();
            dropdownPerfil.style.display = dropdownPerfil.style.display === 'block' ? 'none' : 'block';
        });

        document.addEventListener('click', function (event) {
            if (!botoPerfil.contains(event.target) && !dropdownPerfil.contains(event.target)) {
                dropdownPerfil.style.display = 'none';
            }
        });
    }
});