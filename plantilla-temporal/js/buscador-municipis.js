document.addEventListener('DOMContentLoaded', function () {
    // Elements DOM
    const paisSelect = document.getElementById('paisSelect');
    const comunitatGrup = document.getElementById('comunitatGrup');
    const comunitatSelect = document.getElementById('comunitatSelect');
    const provinciaGrup = document.getElementById('provinciaGrup');
    const provinciaSelect = document.getElementById('provinciaSelect');
    const mancomunitatGrup = document.getElementById('mancomunititatsGrup');
    const municipiGrup = document.getElementById('municipiGrup');
    const municipiSelect = document.getElementById('municipiSelect');
    const taulaBody = document.getElementById('taulaBuscadorMunicipisBody');
    const grupBotonsPagina = document.getElementById('grupBotonsPagina');

    // Cerca avançada lliure
    const campCercaAvançada = document.getElementById('campCercaAvançada');
    const resultatsCercaAvançada = document.getElementById('resultatsCercaAvançada');

    let paginaActual = 1;
    let registresPerPagina = 10;

    // Ordenació
    let columnaOrdenada = null;
    let ordreAscendent = true;

    // Valors normalitzats
    function normalitzar(text) {
        if (!text || typeof text !== 'string') return ''; // Retorna cadena buida si text no és vàlid
        return text.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().trim();
    }

    if (!grupBotonsPagina) {
        console.error("No s'ha trobat l'element #grupBotonsPagina");
        return;
    }

    // Control del nombre de registres per pàgina
    const selectRegistres = document.getElementById('registresPerPaginaSelect');
    if (selectRegistres) {
        selectRegistres.addEventListener('change', function () {
            registresPerPagina = parseInt(this.value);
            paginaActual = 1;
            carregarTaula();
        });
    }

    function omplirSelect(selectElement, valors) {
        selectElement.innerHTML = '<option value="">-- Selecciona opció --</option>';
        if (!valors) return;
        valors.forEach(item => {
            const option = document.createElement('option');
            option.value = item.nom;
            option.textContent = item.nom;
            selectElement.appendChild(option);
        });
    }

    // Dades del buscador municipal - jerarquia completa
    const dadesLocalitzacio = {
        espanya: {
            nom: "Espanya",
            comunitats: {
                Catalunya: {
                    Barcelona: [
                        {"codi_ine": "08001", "nom": "Abrera"},
                        {"codi_ine": "08002", "nom": "Aguilar de Segarra"},
                        {"codi_ine": "08003", "nom": "Aiguafreda"}
                    ],
                    Girona: [
                        {"codi_ine": "17001", "nom": "Agullana"},
                        {"codi_ine": "17002", "nom": "Aiguaviva"},
                        {"codi_ine": "17003", "nom": "Albanyà"}
                    ],
                    Lleida: [
                        {"codi_ine": "25001", "nom": "Abella de la Conca"},
                        {"codi_ine": "25002", "nom": "Àger"},
                        {"codi_ine": "25003", "nom": "Agramunt"},
                        {"codi_ine": "25004", "nom": "Alamús"},
                        {"codi_ine": "25005", "nom": "Alàs i Cerc"},
                        {"codi_ine": "25006", "nom": "Albagés"},
                        {"codi_ine": "25007", "nom": "Albatàrrec"},
                        {"codi_ine": "25008", "nom": "Albesa"},
                    ],
                    Tarragona: [
                        {"codi_ine": "43001", "nom": "Aiguamúrcia"},
                        {"codi_ine": "43002", "nom": "Albinyana"},
                        {"codi_ine": "43003", "nom": "Albiol"},
                        {"codi_ine": "43004", "nom": "Alcanar"},
                        {"codi_ine": "43005", "nom": "Alcover"},
                        {"codi_ine": "43006", "nom": "Aldover"},
                        {"codi_ine": "43007", "nom": "Alfara de Carles"},
                        {"codi_ine": "43008", "nom": "Alforja"},
                        {"codi_ine": "43009", "nom": "Alió"},
                    ]
                }
            }
        }
    };

    function generarDadesMunicipis() {
        const municipis = [];
        for (const [comunitat, provincies] of Object.entries(dadesLocalitzacio.espanya.comunitats)) {
            for (const [provincia, pobles] of Object.entries(provincies)) {
                pobles.forEach(poble => {
                    const registres = Math.floor(Math.random() * 100);
                    const indexat = Math.min(100, Math.round((Math.random() * 100) + 30));
                    municipis.push({
                        pais: "Espanya",
                        comunitat,
                        provincia,
                        poble: poble.nom,
                        registres,
                        indexat
                    });
                });
            }
        }
        return municipis;
    }

    let dadesTaula = generarDadesMunicipis();
    
    //let dadesTaula = [];
    let dadesFiltrades = [];

    function filtrarDades() {
        const pais = paisSelect.value === 'espanya' ? 'Espanya' : '';
        const comunitat = normalitzar(comunitatSelect.value);
        const provincia = normalitzar(provinciaSelect.value);
        const municipi = normalitzar(municipiSelect.value);

        return dadesTaula.filter(reg => {
            const regNorm = {
                pais: normalitzar(reg.pais),
                comunitat: normalitzar(reg.comunitat),
                provincia: normalitzar(reg.provincia),
                poble: normalitzar(reg.poble)
            };
            return (
                (!pais || regNorm.pais.includes(normalitzar(pais))) &&
                (!comunitat || regNorm.comunitat.includes(comunitat)) &&
                (!provincia || regNorm.provincia.includes(provincia)) &&
                (!municipi || regNorm.poble.includes(municipi))
            );
        });
    }

    function carregarTaula(pagina = 1) {
        dadesFiltrades = filtrarDades();

        if (!dadesFiltrades.length) {
            taulaBody.innerHTML = '<tr><td colspan="7" style="text-align:center;">No hi ha dades disponibles</td></tr>';
            grupBotonsPagina.innerHTML = '';
            const infoPagina = document.getElementById('infoPagina');
            if (infoPagina) infoPagina.textContent = '0/0';
            return;
        }

        // Ordenació si cal
        if (columnaOrdenada) {
            dadesFiltrades.sort((a, b) => {
                const valA = a[columnaOrdenada]?.toLowerCase() || '';
                const valB = b[columnaOrdenada]?.toLowerCase() || '';
                return ordreAscendent ? valA.localeCompare(valB) : valB.localeCompare(valA);
            });
        }

        const totalPagines = Math.ceil(dadesFiltrades.length / registresPerPagina);
        paginaActual = Math.min(Math.max(pagina, 1), totalPagines);

        // Mostra info de pàgina
        const infoPagina = document.getElementById('infoPagina');
        if (infoPagina) {
            infoPagina.textContent = `${paginaActual}/${totalPagines}`;
        }

        taulaBody.innerHTML = '';
        const inici = (paginaActual - 1) * registresPerPagina;
        const fi = Math.min(inici + registresPerPagina, dadesFiltrades.length);

        for (let i = inici; i < fi; i++) {
            const { pais, comunitat, provincia, mancomunitat, poble, registres, indexat } = dadesFiltrades[i];

            // Càlcul percentatge indexat
            let percentatgeIndexat = 0;
            if (registres > 0) {
                percentatgeIndexat = Math.min(100, Math.round((indexat * 100) / registres));
            }

            // Assignar classe de color segons el percentatge
            let classeColor = 'verd';
            if (percentatgeIndexat < 30) classeColor = 'rosa';
            else if (percentatgeIndexat < 60) classeColor = 'taronja';
            else if (percentatgeIndexat < 90) classeColor = 'groc';

            // Crear fila
            const fila = document.createElement('tr');
            fila.innerHTML = `
                <td>${pais}</td>
                <td>${comunitat}</td>
                <td>${provincia}</td>
                <td>${poble}</td>
                <td class="cel·la-registres">
                    ${registres}
                    <div class="barra-progres">
                        <div class="progres ${classeColor}" style="width: ${percentatgeIndexat}%"></div>
                    </div>
                    <span class="percentatge-text">${percentatgeIndexat}%</span>
                </td>
                <td class="opcions-mapa">
                    <a href="#" title="Detall"><i class="fas fa-info-circle"></i></a>
                    <a href="https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(poble + ', ' + provincia + ', ' + comunitat)}" target="_blank" title="Google Maps"><i class="fas fa-map-marked-alt"></i></a>
                    <a href="https://www.openstreetmap.org/search?query=${encodeURIComponent(poble + ', ' + provincia + ', ' + comunitat)}" target="_blank" title="OpenStreetMap"><i class="fas fa-map-signs"></i></a>
                </td>
            `;
            taulaBody.appendChild(fila);
        }

        generarBotonsPagina(totalPagines);
    }

    function generarBotonsPagina(totalPagines) {
        if (!totalPagines || totalPagines < 1) return;

        const maxBotons = 10;
        let inici = Math.max(1, paginaActual - Math.floor(maxBotons / 2));
        let fi = Math.min(totalPagines, inici + maxBotons - 1);

        if (fi - inici + 1 < maxBotons && totalPagines > maxBotons) {
            inici = Math.max(1, fi - maxBotons + 1);
        }

        grupBotonsPagina.innerHTML = ''; // Neteja prèvia

        // Botó anterior
        const botoAnterior = document.createElement('button');
        botoAnterior.classList.add('boto-pagina');
        botoAnterior.innerHTML = '<i class="fas fa-chevron-left"></i>';
        botoAnterior.disabled = paginaActual === 1;
        botoAnterior.addEventListener('click', () => {
            if (paginaActual > 1) {
                paginaActual--;
                carregarTaula(paginaActual);
            }
        });
        grupBotonsPagina.appendChild(botoAnterior);

        // Botons pàgines
        for (let i = inici; i <= fi; i++) {
            const boto = document.createElement('button');
            boto.textContent = i;
            boto.dataset.pagina = i;
            boto.classList.add('boto-pagina');
            if (i === paginaActual) boto.classList.add('active');
            boto.addEventListener('click', () => {
                const pagina = parseInt(boto.dataset.pagina);
                carregarTaula(pagina);
            });
            grupBotonsPagina.appendChild(boto);
        }

        // Botó següent
        const botoSeguent = document.createElement('button');
        botoSeguent.classList.add('boto-pagina');
        botoSeguent.innerHTML = '<i class="fas fa-chevron-right"></i>';
        botoSeguent.disabled = paginaActual === totalPagines;
        botoSeguent.addEventListener('click', () => {
            if (paginaActual < totalPagines) {
                paginaActual++;
                carregarTaula(paginaActual);
            }
        });
        grupBotonsPagina.appendChild(botoSeguent);
    }

    // Funcions de selecció
    function mostrarComunitats() {
        const pais = paisSelect.value;
        if (pais !== 'espanya') {
            comunitatGrup.style.display = 'none';
            provinciaGrup.style.display = 'none';
            municipiGrup.style.display = 'none';
            comunitatSelect.innerHTML = '<option value="">-- Selecciona comunitat --</option>';
            provinciaSelect.innerHTML = '<option value="">-- Selecciona provincia --</option>';
            municipiSelect.innerHTML = '<option value="">-- Selecciona municipi --</option>';
            paginaActual = 1;
            carregarTaula();
            return;
        }

        const comunitats = Object.keys(dadesLocalitzacio.espanya.comunitats).map(c => ({ nom: c }));
        omplirSelect(comunitatSelect, comunitats);
        comunitatGrup.style.display = 'block';
        provinciaGrup.style.display = 'none';
        municipiGrup.style.display = 'none';
        provinciaSelect.innerHTML = '<option value="">-- Selecciona provincia --</option>';
        municipiSelect.innerHTML = '<option value="">-- Selecciona municipi --</option>';
        paginaActual = 1;
        carregarTaula();
    }


    function mostrarProvincies() {
        const comunitat = comunitatSelect.value;
        if (!comunitat) {
            provinciaGrup.style.display = 'none';
            municipiGrup.style.display = 'none';
            provinciaSelect.innerHTML = '<option value="">-- Selecciona provincia --</option>';
            municipiSelect.innerHTML = '<option value="">-- Selecciona municipi --</option>';
            paginaActual = 1;
            carregarTaula();
            return;
        }

        const provincies = Object.keys(dadesLocalitzacio.espanya.comunitats[comunitat]).map(p => ({ nom: p }));
        omplirSelect(provinciaSelect, provincies);
        provinciaGrup.style.display = 'block';
        municipiGrup.style.display = 'none';
        municipiSelect.innerHTML = '<option value="">-- Selecciona municipi --</option>';
        paginaActual = 1;
        carregarTaula();
    }

    function mostrarMancomunitats() {
        const comunitatId = comunitatSelect.value;
        const provinciaId = provinciaSelect.value;

        const comunitat = Object.values(dadesLocalitzacio.espanya.comunitats).find(c => 
            normalitzar(c.nom) === normalitzar(comunitatId)
        );

        if (!comunitat || !comunitat.provincies) {
            mancomunitatGrup.style.display = 'none';
            municipiGrup.style.display = 'none';
            municipiSelect.innerHTML = '<option value="">-- Selecciona municipi --</option>';
            paginaActual = 1;
            carregarTaula();
            return;
        }

        const provincia = Object.values(comunitat.provincies).find(p => 
            normalitzar(p.nom) === normalitzar(provinciaId)
        );

        if (!provincia) {
            mancomunitatGrup.style.display = 'none';
            municipiGrup.style.display = 'none';
            municipiSelect.innerHTML = '<option value="">-- Selecciona municipi --</option>';
            paginaActual = 1;
            carregarTaula();
            return;
        }

        // Detectem quin nivell tenim (comarques, mancomunitats, concellos...)
        let mancomunitats = [];

        for (const nivell of ['mancomunitats', 'comarques', 'illots', 'concellos']) {
            if (Array.isArray(provincia[nivell])) {
                mancomunitats = provincia[nivell];
                break;
            }
        }

        if (!mancomunitats.length) {
            mancomunitatGrup.style.display = 'none';
            municipiGrup.style.display = 'none';
            municipiSelect.innerHTML = '<option value="">-- Selecciona municipi --</option>';
            paginaActual = 1;
            carregarTaula();
            return;
        }

        mancomunitatGrup.style.display = 'block';
        municipiGrup.style.display = 'none';
        municipiSelect.innerHTML = '<option value="">-- Selecciona municipi --</option>';
        paginaActual = 1;
        carregarTaula();
    }

    function mostrarMunicipis() {
        const comunitat = comunitatSelect.value;
        const provincia = provinciaSelect.value;

        if (!comunitat || !provincia) {
            municipiGrup.style.display = 'none';
            paginaActual = 1;
            carregarTaula();
            return;
        }

        const municipis = dadesLocalitzacio.espanya.comunitats[comunitat][provincia];
        omplirSelect(municipiSelect, municipis.map(m => ({ nom: m.nom })));
        municipiGrup.style.display = 'block';
        paginaActual = 1;
        carregarTaula();
    }

    // Listeners
    paisSelect.addEventListener('change', () => {
        mostrarComunitats();
        carregarTaula();
    });


    comunitatSelect.addEventListener('change', () => {
        mostrarProvincies();
        carregarTaula();
    });

    provinciaSelect.addEventListener('change', () => {
        mostrarMunicipis();
        carregarTaula();
    });

    municipiSelect.addEventListener('change', () => {
        carregarTaula();
    });

    // Listener ordenació per columna
    document.querySelectorAll('#taulaBuscadorMunicipis thead th[data-column]:not([data-column="registres"])').forEach(th => {
        th.addEventListener('click', () => {
            const col = th.getAttribute('data-column');
            ordreAscendent = columnaOrdenada === col ? !ordreAscendent : true;
            columnaOrdenada = col;
            carregarTaula();
        });
    });

    // Control del nombre de registres per pàgina
    document.getElementById('registresPerPaginaSelect').addEventListener('change', function () {
        registresPerPagina = parseInt(this.value);
        paginaActual = 1;
        carregarTaula();
    });

    // Càrrega inicial
    dadesTaula = generarDadesMunicipis();
    carregarTaula();

    function aplicarResultatCerca(resultat) {
        // Restableix tots els camps menys el país (que ja sabem que és Espanya)
        paisSelect.value = 'espanya';
        mostrarComunitats();

        // Aplica segons el tipus de resultat
        if (resultat.tipus === 'comunitat') {
            comunitatSelect.value = resultat.nom;
            mostrarProvincies();
        } else if (resultat.tipus === 'provincia') {
            comunitatSelect.value = resultat.comunitatId;
            mostrarProvincies();
            provinciaSelect.value = resultat.nom;
            mostrarMunicipis();
        } else if (resultat.tipus === 'municipi') {
            comunitatSelect.value = resultat.comunitatId;
            mostrarProvincies();
            provinciaSelect.value = resultat.provinciaId;
            mostrarMunicipis();
            municipiSelect.value = resultat.nom;
        }

        // Actualitza la pàgina i carrega la taula
        paginaActual = 1;
        carregarTaula(paginaActual);

        // Amaga la llista de resultats després de seleccionar
        resultatsCercaAvançada.style.display = 'none';
    }

    function buscarResultats(term) {
        const resultats = [];

        // Buscar en comunitats
        Object.entries(dadesLocalitzacio.espanya.comunitats).forEach(([comunitatId, comunitat]) => {
            if (normalitzar(comunitatId).includes(term)) {
                resultats.push({
                    tipus: 'comunitat',
                    nom: comunitatId,
                    valor: comunitatId
                });
            }

            // Buscar en províncies
            Object.entries(comunitat).forEach(([provinciaId, provincia]) => {
                if (normalitzar(provinciaId).includes(term)) {
                    resultats.push({
                        tipus: 'provincia',
                        nom: provinciaId,
                        valor: provinciaId,
                        comunitatId: comunitatId
                    });
                }

                // Buscar en municipis
                provincia.forEach(municipi => {
                    if (normalitzar(municipi.nom).includes(term)) {
                        resultats.push({
                            tipus: 'municipi',
                            nom: municipi.nom,
                            valor: municipi.nom,
                            comunitatId: comunitatId,
                            provinciaId: provinciaId
                        });
                    }
                });
            });
        });

        return resultats;
    }

    function mostrarResultats(resultats) {
        if (!resultats.length) {
            console.log("Cap resultat trobat."); // Log per verificar si no hi ha resultats
            resultatsCercaAvançada.style.display = 'none'; // Amaga si no hi ha resultats
            return;
        }

        console.log("Mostrant resultats..."); // Log per verificar que es mostren els resultats
        resultatsCercaAvançada.innerHTML = ''; // Neteja els resultats anteriors
        resultatsCercaAvançada.style.display = 'block'; // Mostra la llista

        resultats.forEach(res => {
            const li = document.createElement('li');
            li.innerHTML = `<span>${res.nom}</span><small>${res.tipus}</small>`;
            li.addEventListener('click', () => {
                aplicarResultatCerca(res); // Funció que aplica el resultat seleccionat
            });
            resultatsCercaAvançada.appendChild(li);
        });
    }
    
    if (campCercaAvançada && resultatsCercaAvançada) {
        campCercaAvançada.addEventListener('input', function () {
            console.log("Event 'input' detectat"); // Log per verificar que l'event s'executa
            const term = normalitzar(this.value);
            console.log("Terme introduït:", term); // Log per verificar el terme introduït

            resultatsCercaAvançada.innerHTML = ''; // Neteja els resultats anteriors

            if (term.length < 2) {
                console.log("El terme és massa curt. Amagant resultats.");
                resultatsCercaAvançada.style.display = 'none'; // Amaga si el terme és massa curt
                return;
            }

            const resultats = buscarResultats(term); // Funció que busca coincidències
            console.log("Resultats trobats:", resultats); // Log per verificar els resultats

            mostrarResultats(resultats); // Funció que mostra els resultats
        });
    } else {
        console.error("Els elements #campCercaAvançada o #resultatsCercaAvançada no són vàlids.");
    }
});