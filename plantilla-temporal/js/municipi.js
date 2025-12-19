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
    /* Modal de Configuració de Columnes */
    /*******************************/
    const botoConfigColumnes = document.getElementById('configColumnes');
    const modalConfig = document.getElementById('modalConfigColumnes');
    const tancaModalConfig = modalConfig.querySelector('.tanca-modal-config');

    function generarLlistaColumnes() {
        const headers = document.querySelectorAll('thead tr:nth-child(2) th[data-col]');
        const llistaColumnes = document.getElementById('llistaColumnes');
        llistaColumnes.innerHTML = ''; // Netegem abans d'afegir-hi coses

        // Assigna valor actual al selector de files per pàgina
        document.getElementById('filesPerPagina').value = filesPerPagina;

        // Event de canvi de files per pàgina
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

        // Event per mostrar/ocultar columnes
        llistaColumnes.querySelectorAll('input[type="checkbox"]').forEach(input => {
            input.addEventListener('change', function () {
                const colIndex = parseInt(this.dataset.col);

                document.querySelectorAll(`th[data-col="${colIndex}"]`).forEach(el => {
                    el.style.display = this.checked ? '' : 'none';
                });

                document.querySelectorAll(`#filtraFila [data-col="${colIndex}"]`).forEach(el => {
                    el.style.display = this.checked ? 'table-cell' : 'none';
                });

                document.querySelectorAll(`#taulaDades tr`).forEach(row => {
                    const cell = row.querySelector(`td[data-col="${colIndex}"]`);
                    if (cell) {
                        cell.style.display = this.checked ? '' : 'none';
                    }
                });
            });
        });
    }

    // Obrir el modal quan es fa clic al botó
    if (botoConfigColumnes && modalConfig) {
        botoConfigColumnes.addEventListener('click', (e) => {
            e.preventDefault();
            modalConfig.style.display = 'flex';
            generarLlistaColumnes();
        });

        tancaModalConfig.addEventListener('click', () => {
            modalConfig.style.display = 'none';
        });

        window.addEventListener('click', (e) => {
            if (e.target === modalConfig) {
                modalConfig.style.display = 'none';
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

    /******************************/
    /*          PAGINACIO         */
    /******************************/
    let filesOriginals = Array.from(document.querySelectorAll('#taulaDades tr'));

    function mostrarPaginacio(totalPagines) {
        const paginador = document.getElementById('paginador');
        paginador.innerHTML = '';

        const crearBoton = (text, pagina) => {
            const b = document.createElement('span');
            b.textContent = text;
            if (pagina === paginaActual) b.classList.add('actiu');
            b.addEventListener('click', () => {
                if (pagina !== paginaActual) {
                    paginaActual = pagina;
                    actualitzarPaginacio();
                }
            });
            return b;
        };

        if (paginaActual > 1) paginador.appendChild(crearBoton('<', paginaActual - 1));
        for (let i = 1; i <= totalPagines; i++) {
            paginador.appendChild(crearBoton(i, i));
        }
        if (paginaActual < totalPagines) paginador.appendChild(crearBoton('>', paginaActual + 1));
    }

    function actualitzarPaginacio() {
        const visibles = obtenirFilesFiltrades();
        const totalPagines = Math.ceil(visibles.length / filesPerPagina);
        const start = (paginaActual - 1) * filesPerPagina;
        const end = start + filesPerPagina;

        // Amaga totes
        filesOriginals.forEach(fila => fila.style.display = 'none');

        // Mostra només les filtrades i paginades
        visibles.slice(start, end).forEach(fila => {
            fila.style.display = '';
        });

        mostrarPaginacio(totalPagines || 1);
    }

    // crida després d'aplicar filtres
    function aplicarFiltresAmbPaginacio() {
        aplicarFiltres();
        paginaActual = 1;
        actualitzarPaginacio();
    }

    inputs.forEach(input => {
        input.addEventListener('input', () => {
            paginaActual = 1;
            actualitzarPaginacio();
        });
    });

    document.getElementById('filesPerPagina').addEventListener('change', function () {
        filesPerPagina = parseInt(this.value);
        paginaActual = 1;
        actualitzarPaginacio();
    });

    function obtenirFilesFiltrades() {
        const valors = {};
        inputs.forEach(input => {
            const col = input.dataset.col;
            const valor = input.value.trim().toLowerCase();
            if (valor) valors[col] = valor;
        });

        return filesOriginals.filter(fila => {
            return Object.entries(valors).every(([col, valor]) => {
                const cel·la = fila.querySelector(`td[data-col="${col}"]`);
                const text = cel·la?.textContent.toLowerCase() || '';
                return text.includes(valor);
            });
        });
    }

    actualitzarPaginacio();

    /*******************************/
    /*    EDICIO DEL CONTINGUT     */
    /*******************************/
    // Columnes editables segons data-col
    const columnesEditables = [0, 1, 8, 9, 11, 14]; // camps que volem poder editar

    document.querySelectorAll('#taulaDades td').forEach(td => {
        const col = parseInt(td.dataset.col);
        if (columnesEditables.includes(col)) {
            const text = td.textContent.trim();
            td.innerHTML = `
                <div class="editable-cell">
                    <span class="cell-text">${text}</span>
                    <span class="edit-icons">
                        <i class="fa fa-pencil edit-icon"></i>
                    </span>
                </div>
            `;

            const cellDiv = td.querySelector('.editable-cell');
            const editIcon = cellDiv.querySelector('.edit-icon');

            editIcon.addEventListener('click', () => {
                const currentValue = cellDiv.querySelector('.cell-text').textContent;

                cellDiv.innerHTML = `
                    <input type="text" class="edit-input" value="${currentValue}">
                    <span class="edit-icons">
                        <i class="fa fa-check confirm-icon"></i>
                        <i class="fa fa-times cancel-icon"></i>
                    </span>
                `;

                const input = cellDiv.querySelector('.edit-input');
                const confirmIcon = cellDiv.querySelector('.confirm-icon');
                const cancelIcon = cellDiv.querySelector('.cancel-icon');

                cancelIcon.addEventListener('click', () => {
                    cellDiv.innerHTML = `
                        <span class="cell-text">${currentValue}</span>
                        <span class="edit-icons">
                            <i class="fa fa-pencil edit-icon"></i>
                        </span>
                    `;
                    // Reafegim l’event
                    cellDiv.querySelector('.edit-icon').addEventListener('click', editIcon.click);
                });

                confirmIcon.addEventListener('click', () => {
                    const nouValor = input.value;
                    // Aquí podràs fer l'AJAX més endavant

                    cellDiv.innerHTML = `
                        <span class="cell-text">${nouValor}</span>
                        <span class="edit-icons">
                            <i class="fa fa-pencil edit-icon"></i>
                        </span>
                    `;
                    // Reafegim l’event
                    cellDiv.querySelector('.edit-icon').addEventListener('click', editIcon.click);
                });
            });
        }
    });

    /*********************************/
    /*         CONECTADOR            */
    /*********************************/

    const modalInterconnexio = document.getElementById('modalInterconnectar');
    const tancaModalInterconnexio = modalInterconnexio.querySelector('.tanca-modal-interconnexio');
    const selectTipusConnexio = document.getElementById('tipusConnexio');
    const formContainer = document.getElementById('formulariConnexio');

    // Obrir modal
    document.querySelectorAll('.btn-interconnectar').forEach(btn => {
        btn.addEventListener('click', () => {
            modalInterconnexio.style.display = 'flex';
            selectTipusConnexio.value = '';
            formContainer.innerHTML = '';
        });
    });

    // Tancar modal
    tancaModalInterconnexio.addEventListener('click', () => {
        modalInterconnexio.style.display = 'none';
    });

    window.addEventListener('click', (e) => {
        if (e.target === modalInterconnexio) {
            modalInterconnexio.style.display = 'none';
        }
    });

    // Actualitzar contingut segons connexió
    selectTipusConnexio.addEventListener('change', () => {
        const tipus = selectTipusConnexio.value;
        formContainer.innerHTML = '';

        if (!tipus) return;

        const buscador = `
            <div class="form-group">
                <label for="buscador">Cercar persona:</label>
                <input type="text" id="buscador" placeholder="Nom, Cognoms..." />
            </div>
        `;

        if (tipus === 'parella') {
            formContainer.innerHTML = `
                ${buscador}
                <div class="form-group">
                    <label for="dataMatrimoni">Data del matrimoni:</label>
                    <input type="date" id="dataMatrimoni" />
                </div>
                <div class="form-group">
                    <label for="municipiMatrimoni">Municipi del matrimoni:</label>
                    <input type="text" id="municipiMatrimoni" placeholder="Municipi" />
                </div>
            `;
        } else {
            formContainer.innerHTML = buscador;
        }
    });
});