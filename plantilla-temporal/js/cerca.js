// Funcionalitat de cerca i filtre
document.addEventListener('DOMContentLoaded', function() {
    // Variables
    const taula = document.getElementById('taula-persones');
    const files = taula.querySelectorAll('tbody tr');
    const filtreNom = document.getElementById('filtre-nom');
    const filtreCognom = document.getElementById('filtre-cognom');
    const filtrePoblacio = document.getElementById('filtre-poblacio');
    const botonsPagina = document.querySelectorAll('.boto-pagina');
    
    // Funció per filtrar les files
    function filtrarFiles() {
        const valorNom = filtreNom.value.toLowerCase();
        const valorCognom = filtreCognom.value.toLowerCase();
        const valorPoblacio = filtrePoblacio.value;
        
        files.forEach(fila => {
            const nom = fila.cells[0].textContent.toLowerCase();
            const cognom = fila.cells[1].textContent.toLowerCase();
            const poblacio = fila.cells[5].textContent.toLowerCase();
            
            const coincideixNom = nom.includes(valorNom);
            const coincideixCognom = cognom.includes(valorCognom);
            const coincideixPoblacio = valorPoblacio === '' || poblacio.includes(valorPoblacio);
            
            if (coincideixNom && coincideixCognom && coincideixPoblacio) {
                fila.style.display = '';
            } else {
                fila.style.display = 'none';
            }
        });
    }
    
    // Event listeners pels filtres
    filtreNom.addEventListener('input', filtrarFiles);
    filtreCognom.addEventListener('input', filtrarFiles);
    filtrePoblacio.addEventListener('change', filtrarFiles);
    
    // Funcionalitat d'ordenació
    const capçaleres = taula.querySelectorAll('th[data-sort]');
    
    capçaleres.forEach(capçalera => {
        capçalera.addEventListener('click', function() {
            const tipusOrdenacio = this.getAttribute('data-sort');
            const ordreActual = this.getAttribute('data-ordre') || 'asc';
            const nouOrdre = ordreActual === 'asc' ? 'desc' : 'asc';
            
            // Netegem les classes d'ordenació
            capçaleres.forEach(c => {
                c.removeAttribute('data-ordre');
                c.classList.remove('asc', 'desc');
            });
            
            // Establim el nou ordre
            this.setAttribute('data-ordre', nouOrdre);
            this.classList.add(nouOrdre);
            
            // Ordenem les files
            ordenarFiles(tipusOrdenacio, nouOrdre);
        });
    });
    
    function ordenarFiles(tipus, ordre) {
        const filesArray = Array.from(files);
        const cosTaula = taula.querySelector('tbody');
        
        filesArray.sort((a, b) => {
            const valorA = a.querySelector(`td:nth-child(${getIndexColumna(tipus) + 1})`).textContent;
            const valorB = b.querySelector(`td:nth-child(${getIndexColumna(tipus) + 1})`).textContent;
            
            if (tipus === 'naixement' || tipus === 'defuncio') {
                return ordre === 'asc' 
                    ? new Date(valorA.split('/').reverse().join('-')) - new Date(valorB.split('/').reverse().join('-'))
                    : new Date(valorB.split('/').reverse().join('-')) - new Date(valorA.split('/').reverse().join('-'));
            } else {
                return ordre === 'asc' 
                    ? valorA.localeCompare(valorB) 
                    : valorB.localeCompare(valorA);
            }
        });
        
        // Eliminem les files existents
        files.forEach(fila => cosTaula.removeChild(fila));
        
        // Afegim les files ordenades
        filesArray.forEach(fila => cosTaula.appendChild(fila));
    }
    
    function getIndexColumna(tipus) {
        const capçaleres = taula.querySelectorAll('th[data-sort]');
        for (let i = 0; i < capçaleres.length; i++) {
            if (capçaleres[i].getAttribute('data-sort') === tipus) {
                return i;
            }
        }
        return 0;
    }
    
    // Paginació
    botonsPagina.forEach(boto => {
        boto.addEventListener('click', function() {
            botonsPagina.forEach(b => b.classList.remove('active'));
            this.classList.add('active');
            // Aquí aniria la lògica per canviar de pàgina
        });
    });
});