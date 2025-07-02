document.addEventListener('DOMContentLoaded', function () {
    const botoSelector = document.getElementById('botoSelectorIdioma');
    const dropdown = document.getElementById('dropdownIdiomes');

    if (botoSelector && dropdown) {
        // Obre/tanca el desplegable en fer clic al botó
        botoSelector.addEventListener('click', function (e) {
            e.preventDefault();
            e.stopPropagation();
            dropdown.style.display = dropdown.style.display === 'block' ? 'none' : 'block';
        });

        // Tanca el desplegable si es fa clic fora
        document.addEventListener('click', function (event) {
            if (!botoSelector.contains(event.target) && !dropdown.contains(event.target)) {
                dropdown.style.display = 'none';
            }
        });

        // Canvia el text del botó i manté l'icona
        dropdown.querySelectorAll('a').forEach(link => {
            link.addEventListener('click', function (e) {
                e.preventDefault();
                dropdown.style.display = 'none';
                botoSelector.innerHTML = this.textContent + ' <i class="fas fa-chevron-down"></i>';
            });
        });
    } else {
        console.warn("No s'han trobat els elements del desplegable d'idioma.");
    }
});