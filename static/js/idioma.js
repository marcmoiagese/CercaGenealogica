document.addEventListener('DOMContentLoaded', function () {
    const botoSelector = document.getElementById('botoSelectorIdioma');
    const dropdown = document.getElementById('dropdownIdiomes');

    console.log('[idioma] init selector...');

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

        // Canvia el text del botó i desa cookie, deixant que l'enllaç navegui
        dropdown.querySelectorAll('a').forEach(link => {
            link.addEventListener('click', function () {
                console.log('[idioma] clic idioma', this.dataset.lang, this.getAttribute('href'));
                dropdown.style.display = 'none';

                const lang = this.dataset.lang || this.textContent.trim().toLowerCase().slice(0, 3);
                const expiresDays = 365;
                const expires = new Date(Date.now() + expiresDays * 24 * 60 * 60 * 1000).toUTCString();

                // Desa cookie d'idioma per coherència immediata
                const secure = window.location.protocol === 'https:' ? '; Secure' : '';
                document.cookie = `lang=${lang}; path=/; expires=${expires}; SameSite=Strict${secure}`;
                botoSelector.innerHTML = this.textContent + ' <i class="fas fa-chevron-down"></i>';
                // Deixem que l'enllaç continuï amb la navegació (rutes /cat/, /en/, /oc/)
            });
        });
    } else {
        console.warn("No s'han trobat els elements del desplegable d'idioma.");
    }
});
