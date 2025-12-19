document.addEventListener('DOMContentLoaded', function () {
    const taula = document.getElementById('registres-taula');
    const botoAfegir = document.getElementById('afegirFila');
    let comptadorFiles = 0;

    // Generar fila nova
    function generarFila(index) {
        return `
            <tr>
                <!-- Persona principal -->
                <td><input type="text" name="cognom1[${index}]" class="input-nom persona-exist" data-tipus="persona" placeholder="Cognom 1"></td>
                <td><input type="text" name="cognom2[${index}]" class="input-nom persona-exist" data-tipus="persona" placeholder="Cognom 2"></td>
                <td><input type="text" name="nom[${index}]" class="input-nom persona-exist" data-tipus="persona" placeholder="Nom"></td>

                <!-- Dades bateig -->
                <td><input type="date" name="data_naixament[${index}]"></td>
                <td><input type="date" name="data_baptisme[${index}]"></td>
                <td><input type="number" name="pagina_llibre[${index}]" min="1"></td>
                <td><input type="number" name="pagina_digital[${index}]" min="1"></td>
                <td><input type="number" name="any[${index}]" min="1700" max="2100" value="${new Date().getFullYear()}"></td>

                <!-- PARE -->
                <td><input type="text" name="cognom1_pare[${index}]" class="input-nom persona-exist" data-tipus="pare" placeholder="Pare Cognom 1"></td>
                <td><input type="text" name="cognom2_pare[${index}]" class="input-nom persona-exist" data-tipus="pare" placeholder="Pare Cognom 2"></td>
                <td><input type="text" name="nom_pare[${index}]" class="input-nom persona-exist" data-tipus="pare" placeholder="Pare Nom"></td>

                <!-- MARE -->
                <td><input type="text" name="cognom1_mare[${index}]" class="input-nom persona-exist" data-tipus="mare" placeholder="Mare Cognom 1"></td>
                <td><input type="text" name="cognom2_mare[${index}]" class="input-nom persona-exist" data-tipus="mare" placeholder="Mare Cognom 2"></td>
                <td><input type="text" name="nom_mare[${index}]" class="input-nom persona-exist" data-tipus="mare" placeholder="Mare Nom"></td>

                <!-- AVI PATERN -->
                <td><input type="text" name="cognom1_avi_patern[${index}]" class="input-nom persona-exist" data-tipus="avi_patern" placeholder="Avi Patern Cognom 1"></td>
                <td><input type="text" name="cognom2_avi_patern[${index}]" class="input-nom persona-exist" data-tipus="avi_patern" placeholder="Avi Patern Cognom 2"></td>
                <td><input type="text" name="nom_avi_patern[${index}]" class="input-nom persona-exist" data-tipus="avi_patern" placeholder="Avi Patern Nom"></td>

                <!-- AVIA PATERNA -->
                <td><input type="text" name="cognom1_avia_patern[${index}]" class="input-nom persona-exist" data-tipus="avia_patern" placeholder="Avia Paterna Cognom 1"></td>
                <td><input type="text" name="cognom2_avia_patern[${index}]" class="input-nom persona-exist" data-tipus="avia_patern" placeholder="Avia Paterna Cognom 2"></td>
                <td><input type="text" name="nom_avia_patern[${index}]" class="input-nom persona-exist" data-tipus="avia_patern" placeholder="Avia Paterna Nom"></td>

                <!-- AVI MATERN -->
                <td><input type="text" name="cognom1_avi_matern[${index}]" class="input-nom persona-exist" data-tipus="avi_matern" placeholder="Avi Matern Cognom 1"></td>
                <td><input type="text" name="cognom2_avi_matern[${index}]" class="input-nom persona-exist" data-tipus="avi_matern" placeholder="Avi Matern Cognom 2"></td>
                <td><input type="text" name="nom_avi_matern[${index}]" class="input-nom persona-exist" data-tipus="avi_matern" placeholder="Avi Matern Nom"></td>

                <!-- AVIA MATERNA -->
                <td><input type="text" name="cognom1_avia_materna[${index}]" class="input-nom persona-exist" data-tipus="avia_materna" placeholder="Avia Materna Cognom 1"></td>
                <td><input type="text" name="cognom2_avia_materna[${index}]" class="input-nom persona-exist" data-tipus="avia_materna" placeholder="Avia Materna Cognom 2"></td>
                <td><input type="text" name="nom_avia_materna[${index}]" class="input-nom persona-exist" data-tipus="avia_materna" placeholder="Avia Materna Nom"></td>

                <!-- PADRÍ -->
                <td><input type="text" name="cognom1_padri[${index}]" class="input-nom persona-exist" data-tipus="padri" placeholder="Padrí Cognom 1"></td>
                <td><input type="text" name="cognom2_padri[${index}]" class="input-nom persona-exist" data-tipus="padri" placeholder="Padrí Cognom 2"></td>
                <td><input type="text" name="nom_padri[${index}]" class="input-nom persona-exist" data-tipus="padri" placeholder="Padrí Nom"></td>
                <td><textarea name="notes_padri[${index}]" rows="1" placeholder="Notes padrí..."></textarea></td>

                <!-- PADRINA -->
                <td><input type="text" name="cognom1_padrina[${index}]" class="input-nom persona-exist" data-tipus="padrina" placeholder="Padrina Cognom 1"></td>
                <td><input type="text" name="cognom2_padrina[${index}]" class="input-nom persona-exist" data-tipus="padrina" placeholder="Padrina Cognom 2"></td>
                <td><input type="text" name="nom_padrina[${index}]" class="input-nom persona-exist" data-tipus="padrina" placeholder="Padrina Nom"></td>
                <td><textarea name="notes_padrina[${index}]" rows="1" placeholder="Notes padrina..."></textarea></td>

                <!-- DATA DEFUNCIÓ -->
                <td><input type="date" name="data_defuncio[${index}]"></td>

                <!-- CAMPS OCULTS -->
                <td style="display:none;">
                    <input type="hidden" name="id_persona[${index}]" class="id-persona">
                    <input type="hidden" name="id_pare[${index}]" class="id-pare">
                    <input type="hidden" name="id_mare[${index}]" class="id-mare">
                    <input type="hidden" name="id_avi_patern[${index}]" class="id-avi_patern">
                    <input type="hidden" name="id_avia_patern[${index}]" class="id-avia_patern">
                    <input type="hidden" name="id_avi_matern[${index}]" class="id-avi_matern">
                    <input type="hidden" name="id_avia_materna[${index}]" class="id-avia_materna">
                    <input type="hidden" name="id_padri[${index}]" class="id-padri">
                    <input type="hidden" name="id_padrina[${index}]" class="id-padrina">
                </td>

                <!-- Acció -->
                <td><button type="button" class="boto-eliminar-fila"><i class="fas fa-trash"></i> Eliminar</button></td>
            </tr>`;
    }

    // Afegir fila nova
    botoAfegir.addEventListener('click', function () {
        taula.insertAdjacentHTML('beforeend', generarFila(comptadorFiles));
        const filaNova = taula.querySelector('tr:last-child');

        observarCanvis(filaNova, 'persona');
        observarCanvis(filaNova, 'pare');
        observarCanvis(filaNova, 'mare');
        observarCanvis(filaNova, 'avi_patern');
        observarCanvis(filaNova, 'avia_patern');
        observarCanvis(filaNova, 'avi_matern');
        observarCanvis(filaNova, 'avia_materna');
        observarCanvis(filaNova, 'padri');
        observarCanvis(filaNova, 'padrina');

        comptadorFiles++;
    });

    // Inicialitzar amb 3 files
    for (let i = 0; i < 3; i++) {
        taula.insertAdjacentHTML('beforeend', generarFila(i));
        const filaNova = taula.querySelector('tr:last-child');

        observarCanvis(filaNova, 'persona');
        observarCanvis(filaNova, 'pare');
        observarCanvis(filaNova, 'mare');
        observarCanvis(filaNova, 'avi_patern');
        observarCanvis(filaNova, 'avia_patern');
        observarCanvis(filaNova, 'avi_matern');
        observarCanvis(filaNova, 'avia_materna');
        observarCanvis(filaNova, 'padri');
        observarCanvis(filaNova, 'padrina');

        comptadorFiles++;
    }

    // AJAX - Cerca persona
    taula.addEventListener('input', async function (e) {
        const input = e.target;
        if (!input.classList.contains('persona-exist')) return;

        const valor = input.value.trim();
        if (valor.length < 2) return;

        try {
            const res = await fetch('/persones.json');
            const persones = await res.json();

            const coincidencies = persones.filter(p =>
                p.nom.toLowerCase().includes(valor.toLowerCase()) ||
                p.cognoms.toLowerCase().includes(valor.toLowerCase())
            );

            mostrarResultats(input, coincidencies);

        } catch (err) {
            console.error("Error llegint persones.json", err);
        }
    });

    // Mostrar suggeriments
    function mostrarResultats(input, resultats) {
        const divSuggeriments = document.getElementById('div-suggeriments');
        if (divSuggeriments) divSuggeriments.remove();

        if (resultats.length === 0) return;

        const div = document.createElement('div');
        div.id = 'div-suggeriments';
        div.style.position = 'absolute';
        div.style.border = '1px solid #ccc';
        div.style.background = 'white';
        div.style.zIndex = '9999';

        const rect = input.getBoundingClientRect();
        div.style.top = window.scrollY + rect.bottom + 'px';
        div.style.left = window.scrollX + rect.left + 'px';
        div.style.width = rect.width + 'px';

        resultats.forEach(p => {
            const opcio = document.createElement('div');
            opcio.textContent = `${p.nom} ${p.cognoms}`;
            opcio.style.padding = '8px';
            opcio.style.cursor = 'pointer';

            opcio.addEventListener('click', function () {
                const fila = input.closest('tr');
                const tipusRelacio = input.dataset.tipus || 'persona';
                const cognoms = p.cognoms.split(' ');

                switch (tipusRelacio) {
                    case 'persona':
                        omplirPersona(fila, p.nom, cognoms, p.id);
                        break;
                    case 'pare':
                        omplirPare(fila, p.nom, cognoms, p.id);
                        break;
                    case 'mare':
                        omplirMare(fila, p.nom, cognoms, p.id);
                        break;
                    case 'avi_patern':
                        omplirAviPatern(fila, p.nom, cognoms, p.id);
                        break;
                    case 'avia_patern':
                        omplirAviaPatern(fila, p.nom, cognoms, p.id);
                        break;
                    case 'avi_matern':
                        omplirAviMatern(fila, p.nom, cognoms, p.id);
                        break;
                    case 'avia_materna':
                        omplirAviaMaterna(fila, p.nom, cognoms, p.id);
                        break;
                    case 'padri':
                        omplirPadri(fila, p.nom, cognoms, p.id);
                        break;
                    case 'padrina':
                        omplirPadrina(fila, p.nom, cognoms, p.id);
                        break;
                }

                const divSuggeriments = document.getElementById('div-suggeriments');
                if (divSuggeriments) divSuggeriments.remove();
            });

            div.appendChild(opcio);
        });

        document.body.appendChild(div);

        // Tancar suggeriment si es fa clic fora
        document.addEventListener('click', function tancar(e) {
            if (!div.contains(e.target) && e.target !== input) {
                div.remove();
                document.removeEventListener('click', tancar);
            }
        });
    }

    // Observar modificacions manuals
    function observarCanvis(fila, tipusRelacio) {
        const inputs = fila.querySelectorAll(`[data-tipus="${tipusRelacio}"]`);
        const inputId = fila.querySelector(`.id-${tipusRelacio}`);

        if (!inputId) {
            console.warn(`No s'ha trobat el camp .id-${tipusRelacio}`);
            return;
        }

        inputs.forEach(input => {
            input.addEventListener('input', function () {
                if (inputId && inputId.value !== '') {
                    inputId.value = '';
                    desmarcaComOmplit(fila, tipusRelacio);
                    console.log(`${tipusRelacio} modificat manualment. ID eliminat.`);
                }
            });
        });
    }

    // Marcar com seleccionat
    function marcaComOmplit(fila, tipusRelacio) {
        fila.querySelectorAll('.relacionat').forEach(el => el.classList.remove('relacionat'));
        fila.querySelectorAll(`[data-tipus="${tipusRelacio}"]`).forEach(el => {
            el.classList.add('relacionat');
            el.classList.add('is-valid');
        });
    }

    // Desmarcar quan es modifica
    function desmarcaComOmplit(fila, tipusRelacio) {
        fila.querySelectorAll(`[data-tipus="${tipusRelacio}"]`).forEach(el => {
            el.classList.remove('relacionat');
            el.classList.remove('is-valid');
        });
    }

    // Funcions d'emplenat
    function omplirPersona(fila, nom, cognoms, id) {
        const inputCognom1 = fila.querySelector(`[name^="cognom1["]`);
        const inputCognom2 = fila.querySelector(`[name^="cognom2["]`);
        const inputNom = fila.querySelector(`[name^="nom["]`);
        const inputId = fila.querySelector('.id-persona');

        if (inputCognom1) inputCognom1.value = cognoms[0] || '';
        if (inputCognom2) inputCognom2.value = cognoms.slice(1).join(' ');
        if (inputNom) inputNom.value = nom;
        if (inputId) inputId.value = id;

        marcaComOmplit(fila, 'persona');
    }

    function omplirPare(fila, nom, cognoms, id) {
        const inputCognom1 = fila.querySelector(`[name^="cognom1_pare["]`);
        const inputCognom2 = fila.querySelector(`[name^="cognom2_pare["]`);
        const inputNom = fila.querySelector(`[name^="nom_pare["]`);
        const inputId = fila.querySelector('.id-pare');

        if (inputCognom1) inputCognom1.value = cognoms[0] || '';
        if (inputCognom2) inputCognom2.value = cognoms.slice(1).join(' ');
        if (inputNom) inputNom.value = nom;
        if (inputId) inputId.value = id;

        marcaComOmplit(fila, 'pare');
    }

    function omplirMare(fila, nom, cognoms, id) {
        const inputCognom1 = fila.querySelector(`[name^="cognom1_mare["]`);
        const inputCognom2 = fila.querySelector(`[name^="cognom2_mare["]`);
        const inputNom = fila.querySelector(`[name^="nom_mare["]`);
        const inputId = fila.querySelector('.id-mare');

        if (inputCognom1) inputCognom1.value = cognoms[0] || '';
        if (inputCognom2) inputCognom2.value = cognoms.slice(1).join(' ');
        if (inputNom) inputNom.value = nom;
        if (inputId) inputId.value = id;

        marcaComOmplit(fila, 'mare');
    }

    function omplirAviPatern(fila, nom, cognoms, id) {
        const inputCognom1 = fila.querySelector(`[name^="cognom1_avi_patern["]`);
        const inputCognom2 = fila.querySelector(`[name^="cognom2_avi_patern["]`);
        const inputNom = fila.querySelector(`[name^="nom_avi_patern["]`);
        const inputId = fila.querySelector('.id-avi_patern');

        if (inputCognom1) inputCognom1.value = cognoms[0] || '';
        if (inputCognom2) inputCognom2.value = cognoms.slice(1).join(' ');
        if (inputNom) inputNom.value = nom;
        if (inputId) inputId.value = id;

        marcaComOmplit(fila, 'avi_patern');
    }

    function omplirAviaPatern(fila, nom, cognoms, id) {
        const inputCognom1 = fila.querySelector(`[name^="cognom1_avia_patern["]`);
        const inputCognom2 = fila.querySelector(`[name^="cognom2_avia_patern["]`);
        const inputNom = fila.querySelector(`[name^="nom_avia_patern["]`);
        const inputId = fila.querySelector('.id-avia_patern');

        if (inputCognom1) inputCognom1.value = cognoms[0] || '';
        if (inputCognom2) inputCognom2.value = cognoms.slice(1).join(' ');
        if (inputNom) inputNom.value = nom;
        if (inputId) inputId.value = id;

        marcaComOmplit(fila, 'avia_patern');
    }

    function omplirAviMatern(fila, nom, cognoms, id) {
        const inputCognom1 = fila.querySelector(`[name^="cognom1_avi_matern["]`);
        const inputCognom2 = fila.querySelector(`[name^="cognom2_avi_matern["]`);
        const inputNom = fila.querySelector(`[name^="nom_avi_matern["]`);
        const inputId = fila.querySelector('.id-avi_matern');

        if (inputCognom1) inputCognom1.value = cognoms[0] || '';
        if (inputCognom2) inputCognom2.value = cognoms.slice(1).join(' ');
        if (inputNom) inputNom.value = nom;
        if (inputId) inputId.value = id;

        marcaComOmplit(fila, 'avi_matern');
    }

    function omplirAviaMaterna(fila, nom, cognoms, id) {
        const inputCognom1 = fila.querySelector(`[name^="cognom1_avia_materna["]`);
        const inputCognom2 = fila.querySelector(`[name^="cognom2_avia_materna["]`);
        const inputNom = fila.querySelector(`[name^="nom_avia_materna["]`);
        const inputId = fila.querySelector('.id-avia_materna');

        if (inputCognom1) inputCognom1.value = cognoms[0] || '';
        if (inputCognom2) inputCognom2.value = cognoms.slice(1).join(' ');
        if (inputNom) inputNom.value = nom;
        if (inputId) inputId.value = id;

        marcaComOmplit(fila, 'avia_materna');
    }

    function omplirPadri(fila, nom, cognoms, id) {
        const inputCognom1 = fila.querySelector(`[name^="cognom1_padri["]`);
        const inputCognom2 = fila.querySelector(`[name^="cognom2_padri["]`);
        const inputNom = fila.querySelector(`[name^="nom_padri["]`);
        const inputNotes = fila.querySelector(`[name^="notes_padri["]`);
        const inputId = fila.querySelector('.id-padri');

        if (inputCognom1) inputCognom1.value = cognoms[0] || '';
        if (inputCognom2) inputCognom2.value = cognoms.slice(1).join(' ');
        if (inputNom) inputNom.value = nom;
        if (inputNotes) inputNotes.value = "Seleccionat";
        if (inputId) inputId.value = id;

        marcaComOmplit(fila, 'padri');
    }

    function omplirPadrina(fila, nom, cognoms, id) {
        const inputCognom1 = fila.querySelector(`[name^="cognom1_padrina["]`);
        const inputCognom2 = fila.querySelector(`[name^="cognom2_padrina["]`);
        const inputNom = fila.querySelector(`[name^="nom_padrina["]`);
        const inputNotes = fila.querySelector(`[name^="notes_padrina["]`);
        const inputId = fila.querySelector('.id-padrina');

        if (inputCognom1) inputCognom1.value = cognoms[0] || '';
        if (inputCognom2) inputCognom2.value = cognoms.slice(1).join(' ');
        if (inputNom) inputNom.value = nom;
        if (inputNotes) inputNotes.value = "Seleccionada";
        if (inputId) inputId.value = id;

        marcaComOmplit(fila, 'padrina');
    }

    // Eliminar fila
    taula.addEventListener('click', function (e) {
        if (e.target.closest('.boto-eliminar-fila')) {
            e.target.closest('tr').remove();
        }
    });
});