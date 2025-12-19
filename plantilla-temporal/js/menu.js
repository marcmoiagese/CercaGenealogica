document.addEventListener('DOMContentLoaded', function() {
    const botoMenu = document.getElementById('botoMenu');
    const menuLateral = document.getElementById('menuLateral');
    const overlayMenu = document.getElementById('overlayMenu');
    const cercaMenuInput = document.getElementById('cercaMenuInput');
    const llistaFavorits = document.getElementById('llistaFavorits');
    //const mostrarFavoritsBtn = document.getElementById('mostrarFavorits');
    //const favoritsSeccio = document.getElementById('favoritsSeccio');
    //const tancaFavoritsBtn = document.getElementById('tancaFavorits');
    const botoSelector = document.getElementById('botoSelectorIdioma');
    const dropdown = document.getElementById('dropdownIdiomes');

    // Funció per alternar el menú
    function toggleMenu() {
        menuLateral.classList.toggle('obert');
        overlayMenu.style.display = menuLateral.classList.contains('obert') ? 'block' : 'none';
        
        // Canviar l'icona
        const icona = botoMenu.querySelector('i');
        if (menuLateral.classList.contains('obert')) {
            icona.classList.replace('fa-bars', 'fa-times');
        } else {
            icona.classList.replace('fa-times', 'fa-bars');
        }
    }
    
    // Event listeners
    botoMenu.addEventListener('click', function(e) {
        e.preventDefault();
        e.stopPropagation();
        toggleMenu();
    });
    
    overlayMenu.addEventListener('click', function() {
        toggleMenu();
    });
    
    // Tancar menú en fer clic fora
    document.addEventListener('click', function(e) {
        if (!menuLateral.contains(e.target) && e.target !== botoMenu) {
            menuLateral.classList.remove('obert');
            overlayMenu.style.display = 'none';
            const icona = botoMenu.querySelector('i');
            if (icona) icona.classList.replace('fa-times', 'fa-bars');
        }
    });

    // Cerca al menú
    cercaMenuInput.addEventListener('input', function () {
        const term = this.value.toLowerCase();
        const opcions = document.querySelectorAll('.menu-opcio');

        opcions.forEach(opcio => {
            const text = opcio.textContent.toLowerCase();
            opcio.closest('li').style.display = text.includes(term) ? '' : 'none';
        });
    });

    // Favorits
    const botonsFavorit = document.querySelectorAll('.boto-afegir-favorit');

    function toggleFavorit(boto) {
        const nom = boto.dataset.nom;
        const itemExisteix = Array.from(llistaFavorits.children).some(li => li.dataset.nom === nom);

        if (boto.classList.contains('favorit')) {
            boto.classList.remove('favorit');
            boto.querySelector('i').classList.replace('fas', 'far');

            if (itemExisteix) {
                llistaFavorits.querySelector(`[data-nom="${nom}"]`).remove();
            }
        } else {
            boto.classList.add('favorit');
            boto.querySelector('i').classList.replace('far', 'fas');

            if (!itemExisteix) {
                const li = document.createElement('li');
                li.dataset.nom = nom;
                li.textContent = nom;
                llistaFavorits.appendChild(li);
            }
        }

        // Amaga o mostra la secció de favorits si hi ha elements
        favoritsSeccio.style.display = llistaFavorits.children.length > 0 ? 'block' : 'none';
    }

    botonsFavorit.forEach(boto => {
        boto.addEventListener('click', function (e) {
            e.stopPropagation(); // Evita obrir/tancar el menú
            toggleFavorit(this);
        });
    });


    // MENU IDIOMA
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

        // Opcional: tanca el desplegable quan es selecciona un idioma
        dropdown.querySelectorAll('a').forEach(link => {
            link.addEventListener('click', function () {
                dropdown.style.display = 'none';
                // Aquí pots afegir codi per canviar l'idioma visualment si vols
                botoSelector.innerHTML = this.textContent + ' <i class="fas fa-chevron-down"></i>';
            });
        });
    } else {
        console.warn("No s'han trobat els elements del desplegable d'idioma.");
    }

});