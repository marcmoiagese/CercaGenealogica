// favorits.js - Versió revisada
document.addEventListener('DOMContentLoaded', function() {
    // Inicialitzar favorits
    let favorits = JSON.parse(localStorage.getItem('favoritsMenu')) || [];
    const llistaFavorits = document.getElementById('llistaFavorits');
    
    // Actualitzar llista de favorits
    function actualitzarFavorits() {
        llistaFavorits.innerHTML = '';
        
        if (favorits.length === 0) {
            const placeholder = document.createElement('li');
            placeholder.className = 'placeholder-favorit';
            placeholder.textContent = 'No tens cap favorit encara';
            llistaFavorits.appendChild(placeholder);
            return;
        }
        
        favorits.forEach(favorit => {
            const item = document.createElement('li');
            item.className = 'favorit-item';
            
            const link = document.createElement('a');
            link.href = favorit.link || '#';
            link.className = 'menu-opcio';
            link.innerHTML = `<i class="${favorit.icon}"></i> ${favorit.text}`;
            
            const botoEliminar = document.createElement('button');
            botoEliminar.className = 'boto-eliminar-favorit';
            botoEliminar.innerHTML = '<i class="fas fa-trash"></i>';
            botoEliminar.addEventListener('click', function(e) {
                e.preventDefault();
                eliminarFavorit(favorit.id);
            });
            
            link.appendChild(botoEliminar);
            item.appendChild(link);
            llistaFavorits.appendChild(item);
        });
    }
    
    // Afegir favorit
    function afegirFavorit(id, text, link, icon) {
        if (!favorits.some(f => f.id === id)) {
            favorits.push({ id, text, link, icon });
            localStorage.setItem('favoritsMenu', JSON.stringify(favorits));
            actualitzarFavorits();
            
            // Actualitzar l'estrella al menú
            const botoEstrella = document.querySelector(`.menu-opcio[data-id="${id}"] .boto-afegir-favorit i`);
            if (botoEstrella) {
                botoEstrella.classList.replace('far', 'fas');
            }
        }
    }
    
    // Eliminar favorit
    function eliminarFavorit(id) {
        favorits = favorits.filter(f => f.id !== id);
        localStorage.setItem('favoritsMenu', JSON.stringify(favorits));
        actualitzarFavorits();
        
        // Actualitzar l'estrella al menú
        const botoEstrella = document.querySelector(`.menu-opcio[data-id="${id}"] .boto-afegir-favorit i`);
        if (botoEstrella) {
            botoEstrella.classList.replace('fas', 'far');
        }
    }
    
    // Configurar botons d'afegir a favorits
    document.querySelectorAll('.boto-afegir-favorit').forEach(boto => {
        boto.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            
            const opcio = this.closest('.menu-opcio');
            const id = opcio.getAttribute('data-id') || Math.random().toString(36).substr(2, 9);
            const text = opcio.textContent.trim();
            const link = opcio.getAttribute('href') || '#';
            const icon = opcio.querySelector('i').className;
            
            // Si ja és favorit, l'eliminem
            if (opcio.getAttribute('data-favorit') === 'true') {
                eliminarFavorit(id);
                opcio.setAttribute('data-favorit', 'false');
            } else {
                afegirFavorit(id, text, link, icon);
                opcio.setAttribute('data-favorit', 'true');
                opcio.setAttribute('data-id', id);
            }
        });
    });
    
    // Inicialitzar llista de favorits
    actualitzarFavorits();
});