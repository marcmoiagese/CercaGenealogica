document.addEventListener('DOMContentLoaded', function () {
    let draggingEl = null;
    let filesPerPagina = 25; // Per la paginacio
    let paginaActual = 1; // Per la paginacio

    function enableDragSort(table) {
        const theadRow = table.querySelector('thead tr:nth-child(2)');
        const filterRow = table.querySelector('thead tr:first-child');
        const tbody = table.querySelector('tbody');

        theadRow.querySelectorAll('th[data-col]').forEach(th => {
            th.setAttribute('draggable', true);

            th.addEventListener('dragstart', () => {
                draggingEl = th;
            });

            th.addEventListener('dragover', e => {
                e.preventDefault();
                e.dataTransfer.dropEffect = 'move';

                const target = e.target.closest('th[data-col]');
                if (!draggingEl || !target || target === draggingEl) return;

                const dragCol = draggingEl.dataset.col;
                const targetCol = target.dataset.col;

                if (dragCol === targetCol) return;

                // Movem les capçaleres segons l'ordre actual
                moveColumnGroup(dragCol, targetCol, filterRow, theadRow, tbody);
            });
        });
    }

    function moveColumnGroup(dragCol, targetCol, filterRow, headerRow, tbody) {
        const getElementsByCol = (parent, col) =>
            Array.from(parent.children).find(el => el.dataset.col === col);

        const dragHeader = getElementsByCol(headerRow, dragCol);
        const targetHeader = getElementsByCol(headerRow, targetCol);

        const dragFilter = getElementsByCol(filterRow, dragCol);
        const targetFilter = getElementsByCol(filterRow, targetCol);

        // Movem les capçaleres
        headerRow.insertBefore(dragHeader, targetHeader);
        filterRow.insertBefore(dragFilter, targetFilter);

        // Movem cada cel·la de la columna
        tbody.querySelectorAll('tr').forEach(row => {
            const dragTd = getElementsByCol(row, dragCol);
            const targetTd = getElementsByCol(row, targetCol);
            row.insertBefore(dragTd, targetTd);
        });
    }

    // Inicialitza
    const taula = document.querySelector('.taula-container');
    if (taula) {
        enableDragSort(taula);
    }

    /*******************************/
    /* Aqui comensa el menu modal */
    /*******************************/
    const configBtn = document.getElementById('configColumnes');
    const modal = document.getElementById('modalConfigColumnes');
    const tancaModal = modal.querySelector('.tanca-modal');
    //const llistaColumnes = document.getElementById('llistaColumnes');

    // Generar llista de columnes al modal
    function generarLlistaColumnes() {
        const headers = document.querySelectorAll('thead tr:nth-child(2) th[data-col]');
        const llistaColumnes = document.getElementById('llistaColumnes');
        llistaColumnes.innerHTML = ''; // Netegem abans d'afegir-hi coses

        document.getElementById('filesPerPagina').value = filesPerPagina;
            document.getElementById('filesPerPagina').addEventListener('change', function () {
                filesPerPagina = parseInt(this.value);
                paginaActual = 1;
                actualitzarPaginacio();
            });

        headers.forEach((th, idx) => {
            if (idx === headers.length - 1) return; // Omet "Opcions"

            const colElement = th;
            const colInput = document.querySelector(`#filtraFila [data-col="${idx}"]`);
            const colCell = document.querySelector(`#taulaDades tr td[data-col="${idx}"]`);

            // Comprova si la columna està visible
            const isVisible = colElement && colInput && colCell &&
                window.getComputedStyle(colElement).display !== 'none' &&
                window.getComputedStyle(colInput).display !== 'none' &&
                window.getComputedStyle(colCell).display !== 'none';

            const li = document.createElement('li');
            li.innerHTML = `
                <label>
                    <input type="checkbox" ${isVisible ? 'checked' : ''} data-col="${idx}">
                    ${th.textContent}
                </label>
            `;
            llistaColumnes.appendChild(li);
        });

        // Afegeix events als checkbox recién generats
        llistaColumnes.querySelectorAll('input[type="checkbox"]').forEach(input => {
            input.addEventListener('change', function () {
                const colIndex = parseInt(this.dataset.col);

                // Mostra/Oculta header th
                document.querySelectorAll(`th[data-col="${colIndex}"]`).forEach(el => {
                    el.style.display = this.checked ? '' : 'none';
                });

                // Mostra/Oculta filtre input
                document.querySelectorAll(`#filtraFila [data-col="${colIndex}"]`).forEach(el => {
                    el.style.display = this.checked ? 'table-cell' : 'none';
                });

                // Mostra/Oculta cel·les de dades
                document.querySelectorAll(`#taulaDades tr`).forEach(row => {
                    const cell = row.querySelector(`td[data-col="${colIndex}"]`);
                    if (cell) {
                        cell.style.display = this.checked ? '' : 'none';
                    }
                });
            });
        });
    }

    document.getElementById('configColumnes').addEventListener('click', (e) => {
        e.preventDefault();
        document.getElementById('modalConfigColumnes').style.display = 'flex';
        generarLlistaColumnes(); // Genera llista cada vegada que s'obre
    });

    // Mostrar/Ocultar modal
    if (configBtn && modal) {
        configBtn.addEventListener('click', (e) => {
            e.preventDefault();
            modal.style.display = 'flex';
            generarLlistaColumnes(); // Actualitza l'estat de les checkboxes
        });

        tancaModal.addEventListener('click', () => {
            modal.style.display = 'none';
        });

        window.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.style.display = 'none';
            }
        });
    }
    /**************/
    /* Filtratge */
    /**************/
    const inputs = document.querySelectorAll('#filtraFila [data-col] input');
    const files = Array.from(document.querySelectorAll('#taulaDades tr'));

    // Funció principal de filtratge
    function aplicarFiltres() {
        const valors = {};

        // Obtenim tots els valors dels inputs
        inputs.forEach(input => {
            const col = input.dataset.col;
            const valor = input.value.trim().toLowerCase();
            if (valor) {
                valors[col] = valor;
            }
        });

        files.forEach(fila => {
            let visible = true;

            for (const col in valors) {
                const cel·la = fila.querySelector(`td[data-col="${col}"]`);
                const text = cel·la?.textContent.toLowerCase() || '';
                if (!text.includes(valors[col])) {
                    visible = false;
                    break; // Ja no cal continuar revisant
                }
            }

            fila.style.display = visible ? '' : 'none';
        });
    }

    // Afegeix events als inputs
    inputs.forEach(input => {
        input.addEventListener('input', aplicarFiltres);
    });

    


});