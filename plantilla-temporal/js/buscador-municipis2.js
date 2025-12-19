document.addEventListener('DOMContentLoaded', function () {
    // Elements DOM
    const paisSelect = document.getElementById('paisSelect');
    const comunitatGrup = document.getElementById('comunitatGrup');
    const comunitatSelect = document.getElementById('comunitatSelect');
    const provinciaGrup = document.getElementById('provinciaGrup');
    const provinciaSelect = document.getElementById('provinciaSelect');
    const mancomunitatGrup = document.getElementById('mancomunititatsGrup');
    const mancomunitatSelect = document.getElementById('mancomunitatSelect');
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
        if (!text || typeof text !== 'string') return '';
        return text.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().replace(/\s+/g, '');
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
            const valorNet = normalitzar(item.nom);
            option.value = valorNet;
            option.textContent = item.nom;
            selectElement.appendChild(option);
        });
    }

    // Dades del buscador municipal - jerarquia completa
    const dadesLocalitzacio = {
        espanya: {
            nom: "Espanya",
            comunitats: {
                andalusia: { 
                    nom: "Andalusia", 
                    columnes: ["Pais", "Comunitat Autonoma", "Provincia", "Mancomunitat", "Poble", "Registres", "Opcions"],
                    provincies: {
                        almeria: {
                            nom: "Almería",
                            mancomunitats: [
                                {
                                    nom: "l'Alpujarra Almeriense",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Alboloduy", "Alcolea", "Alhabia", "Alhama de Almería", "Alicún", 
                                        "Almócita", "Alsodux", "Bayárcal", "Beires", "Bentarique", 
                                        "Canjáyar", "Fondón", "Huécija", "Íllar", "Instinción", 
                                        "Laujar de Andarax", "Ohanes", "Padules", "Paterna del Río", 
                                        "Rágol", "Santa Cruz de Marchena", "Terque"
                                    ]
                                },
                                {
                                    nom: "Valle del Almanzora",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Albox", "Alcóntar", "Arboleas", "Armuña de Almanzora", "Bacares", 
                                        "Bayarque", "Cantoria", "Chercos", "Cóbdar", "Fines", 
                                        "Laroya", "Líjar", "Lúcar", "Macael", "Olula del Río", 
                                        "Oria", "Partaloa", "Purchena", "Serón", "Sierro", 
                                        "Somontín", "Suflí", "Taberno", "Tíjola", "Urrácal", "Zurgena"
                                    ]
                                },
                                {
                                    nom: "La Mojonera",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "La Mojonera", "Venta del Viso", "Urbanización Felix", 
                                        "San Nicolás Bajo", "Venta del Cosario", "San Nicolás Alto", 
                                        "Las Cantinas"
                                    ]
                                }
                            ]
                        },
                        cadiz: {
                            nom: "Cádiz",
                            mancomunitats: [
                                {
                                    nom: "Campo de Gibraltar",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Algeciras", "La Línea de la Concepción", "San Roque", 
                                        "Los Barrios", "Tarifa", "Castellar de la Frontera", 
                                        "Jimena de la Frontera"
                                    ]
                                },
                                {
                                    nom: "Costa Noroeste de Cádiz",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Chipiona", "Sanlúcar de Barrameda", "Trebujena", 
                                        "Rota", "El Puerto de Santa María"
                                    ]
                                },
                                {
                                    nom: "La Janda",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Barbate", "Conil de la Frontera", "Medina-Sidonia", 
                                        "Paterna de Rivera", "San José del Valle", 
                                        "Vejer de la Frontera", "Benalup-Casas Viejas"
                                    ]
                                },
                                {
                                    nom: "Sierra de Cádiz",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Arcos de la Frontera", "Bornos", "Espera", 
                                        "Villamartín", "Prado del Rey", "El Bosque", 
                                        "Ubrique", "Benaocaz", "Grazalema", "Villaluenga del Rosario"
                                    ]
                                }
                            ]
                        },
                        cordoba: {
                            nom: "Còrdova",
                            mancomunitats: [
                                {
                                    nom: "Alto Guadalquivir",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Villa del Río", "Bujalance", "Cañete de las Torres", 
                                        "El Carpio", "Pedro Abad", "Adamuz", "Montoro"
                                    ]
                                },
                                {
                                    nom: "Subbética Cordobesa",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Cabra", "Priego de Córdoba", "Lucena", "Rute", 
                                        "Zuheros", "Iznájar", "Encinas Reales", "Almedinilla"
                                    ]
                                },
                                {
                                    nom: "Valle del Guadiato",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Peñarroya-Pueblonuevo", "Belmez", "Espiel", 
                                        "Villanueva del Rey", "Obejo", "Valsequillo"
                                    ]
                                },
                                {
                                    nom: "Campiña Sur Cordobesa",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Aguilar de la Frontera", "Montilla", "Monturque", 
                                        "Moriles", "Puente Genil", "La Rambla", "Santaella"
                                    ]
                                },
                                {
                                    nom: "Los Pedroches",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Pozoblanco", "Villanueva de Córdoba", "Hinojosa del Duque", 
                                        "Añora", "Dos Torres", "Alcaracejos", "Cardeña"
                                    ]
                                }
                            ]
                        },
                        granada: {
                            nom: "Granada",
                            mancomunitats: [
                                {
                                    nom: "Alpujarra Granadina",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Órgiva", "Lanjarón", "Pampaneira", "Bubión", "Capileira",
                                        "Trevélez", "Ugíjar", "Cádiar", "Alpujarra de la Sierra",
                                        "Soportújar", "La Taha"
                                    ]
                                },
                                {
                                    nom: "Comarca de Guadix",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Guadix", "Purullena", "Benalúa", "Ferreira", "La Calahorra",
                                        "Aldeire", "Alquife", "Jérez del Marquesado", "Valle del Zalabí"
                                    ]
                                },
                                {
                                    nom: "Comarca de Baza",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Baza", "Caniles", "Cúllar", "Freila", "Zújar",
                                        "Benamaurel", "Cortes de Baza", "Hernán-Valle"
                                    ]
                                },
                                {
                                    nom: "Vega de Granada",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Santa Fe", "Atarfe", "Las Gabias", "Churriana de la Vega",
                                        "Cenes de la Vega", "Huétor Vega", "Ogíjares", "Armilla",
                                        "Albolote", "Maracena"
                                    ]
                                },
                                {
                                    nom: "Costa Tropical",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Motril", "Almuñécar", "Salobreña", "Gualchos", "Polopos",
                                        "Lújar", "Sorvilán", "Molvízar"
                                    ]
                                },
                                {
                                    nom: "Los Montes",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Iznalloz", "Deifontes", "Montillana", "Piñar", "Morelábor",
                                        "Guadahortuna", "Pedro Martínez"
                                    ]
                                },
                                {
                                    nom: "Valle de Lecrín",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Dúrcal", "Nigüelas", "Villamena", "Lecrín", "El Padul",
                                        "El Pinar", "Restábal"
                                    ]
                                }
                            ]
                        },
                        huelva: {
                            nom: "Huelva",
                            mancomunitats: [
                                {
                                    nom: "Andévalo",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Valverde del Camino", "Minas de Riotinto", "Nerva", 
                                        "El Campillo", "Zalamea la Real", "Puebla de Guzmán",
                                        "Santa Bárbara de Casa", "Cabezas Rubias", "Alosno"
                                    ]
                                },
                                {
                                    nom: "Costa Occidental",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Lepe", "Isla Cristina", "Ayamonte", "Cartaya", 
                                        "Villablanca", "La Antilla", "Punta Umbría"
                                    ]
                                },
                                {
                                    nom: "Cuenca Minera",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Tharsis", "Calañas", "Almonaster la Real", 
                                        "El Cerro de Andévalo", "Villanueva de las Cruces"
                                    ]
                                },
                                {
                                    nom: "Sierra de Aracena y Picos de Aroche",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Aracena", "Jabugo", "Cortegana", "Almonaster la Real",
                                        "Linares de la Sierra", "Alájar", "Santa Ana la Real",
                                        "Los Marines", "Fuenteheridos", "Galaroza"
                                    ]
                                },
                                {
                                    nom: "Condado de Huelva",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "La Palma del Condado", "Bollullos Par del Condado",
                                        "Villalba del Alcor", "Almonte", "Rociana del Condado",
                                        "Moguer", "Niebla", "Bonares", "Lucena del Puerto"
                                    ]
                                },
                                {
                                    nom: "Metropolitana de Huelva",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Gibraleón", "San Bartolomé de la Torre", 
                                        "Trigueros", "Beas", "San Juan del Puerto"
                                    ]
                                }
                            ]
                        },
                        jaen: {
                            nom: "Jaén",
                            mancomunitats: [
                                {
                                    nom: "Sierra de Cazorla",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Cazorla", "La Iruela", "Hinojares", 
                                        "Huesa", "Peal de Becerro", "Quesada"
                                    ]
                                },
                                {
                                    nom: "Sierra de Segura",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Santiago-Pontones", "Beas de Segura", "Orcera",
                                        "Segura de la Sierra", "Hornos", "Benatae",
                                        "Villarrodrigo", "Génave"
                                    ]
                                },
                                {
                                    nom: "Sierra Mágina",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Jódar", "Bedmar y Garcíez", "Huelma",
                                        "Jimena", "Pegalajar", "Cambil",
                                        "Mancha Real", "Torres"
                                    ]
                                },
                                {
                                    nom: "La Loma",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Úbeda", "Baeza", "Vilches",
                                        "Rus", "Sabiote", "Canena",
                                        "Ibros", "Lupión"
                                    ]
                                },
                                {
                                    nom: "Campo de Jaén",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Jaén", "Mengíbar", "Torre del Campo",
                                        "Torredonjimeno", "Jamilena", "Fuerte del Rey",
                                        "Los Villares", "Valdepeñas de Jaén"
                                    ]
                                },
                                {
                                    nom: "Sierra Sur",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Alcalá la Real", "Alcaudete", "Castillo de Locubín",
                                        "Frailes", "Valdepeñas de Jaén", "Noalejo",
                                        "Martos", "Fuensanta de Martos"
                                    ]
                                },
                                {
                                    nom: "El Condado",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Santisteban del Puerto", "Navas de San Juan",
                                        "Villacarrillo", "Iznatoraf", "Sorihuela del Guadalimar",
                                        "Chiclana de Segura", "Arroyo del Ojanco"
                                    ]
                                }
                            ]
                        },
                        malaga: {
                            nom: "Màlaga",
                            mancomunitats: [
                                {
                                    nom: "Costa del Sol Occidental",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Marbella", "Estepona", "Benahavís", "Casares", 
                                        "Manilva", "Fuengirola", "Mijas", "Torremolinos"
                                    ]
                                },
                                {
                                    nom: "Costa del Sol Oriental",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Nerja", "Torrox", "Vélez-Málaga", "Algarrobo",
                                        "Rincón de la Victoria", "Macharaviaya", "Benamargosa"
                                    ]
                                },
                                {
                                    nom: "Valle del Guadalhorce",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Coín", "Alhaurín el Grande", "Alhaurín de la Torre",
                                        "Cártama", "Pizarra", "Almogía", "Valle de Abdalajís"
                                    ]
                                },
                                {
                                    nom: "Serranía de Ronda",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Ronda", "Arriate", "Atajate", "Benadalid",
                                        "Gaucín", "Jimera de Líbar", "Montejaque",
                                        "Benaoján", "Cortes de la Frontera"
                                    ]
                                },
                                {
                                    nom: "Axarquía",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Comares", "Cútar", "El Borge", "Almáchar",
                                        "Benamocarra", "Iznate", "Periana", "Riogordo"
                                    ]
                                },
                                {
                                    nom: "Antequera",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Antequera", "Campillos", "Archidona", "Humilladero",
                                        "Mollina", "Alameda", "Sierra de Yeguas", "Villanueva de la Concepción"
                                    ]
                                },
                                {
                                    nom: "Nororma",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Alfarnate", "Alfarnatejo", "Colmenar", "Riogordo",
                                        "Villanueva del Rosario", "Villanueva del Trabuco"
                                    ]
                                },
                                {
                                    nom: "Sierra de las Nieves",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Tolox", "Parauta", "El Burgo", "Yunquera",
                                        "Ojén", "Istán", "Monda", "Guaro"
                                    ]
                                }
                            ]
                        },
                        sevilla: {
                            nom: "Sevilla",
                            mancomunitats: [
                                {
                                    nom: "Aljarafe",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Mairena del Aljarafe", "Bormujos", "Tomares", 
                                        "Gines", "Valencina de la Concepción", "Castilleja de la Cuesta",
                                        "Sanlúcar la Mayor", "Olivares", "Albaida del Aljarafe"
                                    ]
                                },
                                {
                                    nom: "Campiña de Carmona",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Carmona", "La Campana", "Fuentes de Andalucía",
                                        "La Luisiana", "Écija", "Cañada Rosal"
                                    ]
                                },
                                {
                                    nom: "Comarca Metropolitana de Sevilla",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Dos Hermanas", "Alcalá de Guadaíra", "Utrera",
                                        "Los Palacios y Villafranca", "Camas", "Santiponce",
                                        "La Rinconada", "Brenes"
                                    ]
                                },
                                {
                                    nom: "Sierra Norte",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Cazalla de la Sierra", "Constantina", "Alanís",
                                        "Guadalcanal", "San Nicolás del Puerto", "Las Navas de la Concepción",
                                        "El Pedroso", "La Puebla de los Infantes"
                                    ]
                                },
                                {
                                    nom: "Vega del Guadalquivir",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Lora del Río", "Villaverde del Río", "Alcolea del Río",
                                        "Burguillos", "Tocina", "Cantillana",
                                        "Villanueva del Río y Minas"
                                    ]
                                },
                                {
                                    nom: "Bajo Guadalquivir",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Lebrija", "Las Cabezas de San Juan", "Los Molares",
                                        "El Coronil", "Utrera", "Los Palacios y Villafranca"
                                    ]
                                },
                                {
                                    nom: "Écija",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Écija", "Cañada Rosal", "La Luisiana",
                                        "Fuentes de Andalucía", "La Campana"
                                    ]
                                },
                                {
                                    nom: "Estepa",
                                    tipus: "Mancomunitat",
                                    municipis: [
                                        "Estepa", "Osuna", "Marinaleda", "Gilena",
                                        "Pedrera", "Lora de Estepa", "La Roda de Andalucía"
                                    ]
                                }
                            ]
                        }
                    }
                },
                aragon: {
                    nom: "Aragón",
                    columnes: ["Pais", "Comunitat Autonoma", "Provincia", "Comarca", "Poble", "Registres", "Opcions"],
                    provincies: {
                        osca: {
                            nom: "Osca",
                            comarques: [
                                {
                                    nom: "Ribagorça",
                                    tipus: "comarca",
                                    municipis: [
                                        "Benavarri", "Graus", "Camporrells", 
                                        "Estopanyà", "El Pont de Suert", "Viacamp i Lliterà",
                                        "Sopeira", "Arén"
                                    ]
                                },
                                {
                                    nom: "Somontano de Barbastre",
                                    tipus: "comarca",
                                    municipis: [
                                        "Barbastre", "El Grado", "Ilche", 
                                        "Peralta de Calasanz", "Azanuy-Alins", "Estada",
                                        "La Puebla de Castro", "Salas Altas"
                                    ]
                                },
                                {
                                    nom: "Alt Pirineu",
                                    tipus: "comarca",
                                    municipis: [
                                        "Bielsa", "Sallent de Gállego", "Jaca", 
                                        "Canfranc", "Torla-Ordesa", "Broto",
                                        "Plan", "Tella-Sin"
                                    ]
                                },
                                {
                                    nom: "Foia d'Osca",
                                    tipus: "comarca",
                                    municipis: [
                                        "Osca", "Almudévar", "Monflorite-Lascasas", 
                                        "Tierz", "Quicena", "Lupiñén-Ortilla",
                                        "Siétamo", "Nueno"
                                    ]
                                },
                                {
                                    nom: "Monegros",
                                    tipus: "comarca",
                                    municipis: [
                                        "Sariñena", "Lanaja", "Grañén", 
                                        "Albalatillo", "La Almolda", "Capdesaso",
                                        "Lalueza", "Poleñino"
                                    ]
                                },
                                {
                                    nom: "Cinca Mitjà",
                                    tipus: "comarca",
                                    municipis: [
                                        "Monzón", "Binéfar", "Alcampell", 
                                        "Altorricón", "Tamarit de Llitera", "Esplús",
                                        "Belver de Cinca", "Alcolea de Cinca"
                                    ]
                                },
                                {
                                    nom: "Baix Cinca",
                                    tipus: "comarca",
                                    municipis: [
                                        "Fraga", "Zaidín", "Torrent de Cinca", 
                                        "Ventoses", "Ballobar", "Belver de Cinca",
                                        "Osso de Cinca", "Velilla de Cinca"
                                    ]
                                },
                                {
                                    nom: "La Llitera",
                                    tipus: "comarca",
                                    municipis: [
                                        "Tamarit de Llitera", "Campell", "Almacelles", 
                                        "Albelda", "Esplús", "Castillonroy",
                                        "San Esteban de Llitera", "Viacamp i Lliterà"
                                    ]
                                }
                            ]
                        },
                        saragossa: {
                            nom: "Saragossa",
                            comarques: [
                                {
                                    nom: "Comarca de Saragossa",
                                    tipus: "comarca",
                                    municipis: [
                                        "Saragossa", "Utebo", "Cuarte de Huerva", 
                                        "La Muela", "Villanueva de Gállego", "San Mateo de Gállego",
                                        "Zuera", "Pastriz"
                                    ]
                                },
                                {
                                    nom: "Camp de Belchit",
                                    tipus: "comarca",
                                    municipis: [
                                        "Belchite", "Lécera", "Letux", 
                                        "Almonacid de la Cuba", "Azaila", "Codo",
                                        "Lagata", "Samper del Salz"
                                    ]
                                },
                                {
                                    nom: "Camp de Carinyena",
                                    tipus: "comarca",
                                    municipis: [
                                        "Carinyena", "Paniza", "Encinacorba",
                                        "Vistabella", "Cosuenda", "Aladrén",
                                        "Tosos", "Aguilón"
                                    ]
                                },
                                {
                                    nom: "Baix Aragó-Casp",
                                    tipus: "comarca",
                                    municipis: [
                                        "Casp", "Maella", "Fabara",
                                        "Nonaspe", "Fayón", "Mequinensa",
                                        "Torre del Comte", "Val de Pilas"
                                    ]
                                },
                                {
                                    nom: "Aranda",
                                    tipus: "comarca",
                                    municipis: [
                                        "Illueca", "Gotor", "Breya de Aragón",
                                        "Mesones de Isuela", "Calcena", "Oseja",
                                        "Tierga", "Pomer"
                                    ]
                                },
                                {
                                    nom: "Cinco Villas",
                                    tipus: "comarca",
                                    municipis: [
                                        "Exeya d'os Caballers", "Taust", "Sadaba",
                                        "Uncastillo", "Biota", "Luesia",
                                        "Orés", "Biel"
                                    ]
                                },
                                {
                                    nom: "Ribera Alta de l'Ebre",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alagón", "Figueruelas", "Pedrola",
                                        "Pradilla de Ebro", "Remolinos", "Alcalá de Ebro",
                                        "Cabañas de Ebro", "Grisén"
                                    ]
                                },
                                {
                                    nom: "Tarassona i el Moncayo",
                                    tipus: "comarca",
                                    municipis: [
                                        "Tarassona", "Ágreda", "Los Fayos",
                                        "Vozmediano", "Añón de Moncayo", "Litago",
                                        "Trasmoz", "San Martín de la Virgen de Moncayo"
                                    ]
                                },
                                {
                                    nom: "Valdejalón",
                                    tipus: "comarca",
                                    municipis: [
                                        "La Almunia de Doña Godina", "Épila", "Calatorao",
                                        "Ricla", "Salillas de Jalón", "Urrea de Jalón",
                                        "Chodes", "Lucena de Jalón"
                                    ]
                                }
                            ]
                        },
                        teruel: {
                            nom: "Terol",
                            comarques: [
                                {
                                    nom: "Baix Aragó",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alcanyís", "Calanda", "Alcorisa",
                                        "Valdealgorfa", "Torrevelilla", "La Ginebrosa",
                                        "Foz-Calanda", "Torredarques"
                                    ]
                                },
                                {
                                    nom: "Matarranya",
                                    tipus: "comarca",
                                    municipis: [
                                        "Valderrobres", "Calaceit", "Arens de Lledó",
                                        "Cretes", "Torre del Comte", "La Fresneda",
                                        "Mazaleón", "Peñarroya de Tastavins"
                                    ]
                                },
                                {
                                    nom: "Gúdar-Javalambre",
                                    tipus: "comarca",
                                    municipis: [
                                        "Mora de Rubielos", "Rubiols", "Llinars de Mora",
                                        "Alcalá de la Selva", "Formiche Alto", "Cabra de Mora",
                                        "Valbona", "Arcos de las Salinas"
                                    ]
                                },
                                {
                                    nom: "Comunidad de Teruel",
                                    tipus: "comarca",
                                    municipis: [
                                        "Terol", "Cella", "Villel",
                                        "Albalat del Arzobispo", "Cascante del Río", "Corbalán",
                                        "San Blas", "El Cuervo"
                                    ]
                                },
                                {
                                    nom: "Serrans Baixos",
                                    tipus: "comarca",
                                    municipis: [
                                        "Mont-roig de Tastavins", "Rafel del Maestrat", "Morella",
                                        "Portell de Morella", "Olocau del Rey", "Castellfort",
                                        "Cinctorres", "Forcall"
                                    ]
                                },
                                {
                                    nom: "Andorra-Sierra de Arcos",
                                    tipus: "comarca",
                                    municipis: [
                                        "Andorra", "Alacó", "Crivillén",
                                        "Ejulve", "Gargallo", "Estercuel",
                                        "Oliete", "Ariño"
                                    ]
                                },
                                {
                                    nom: "Maestrat",
                                    tipus: "comarca",
                                    municipis: [
                                        "Cantavella", "La Cuba", "Mirambel",
                                        "Todolella", "Villores", "La Mata de Morella",
                                        "Ortells", "Portellada"
                                    ]
                                },
                                {
                                    nom: "Albarracín",
                                    tipus: "comarca",
                                    municipis: [
                                        "Albarracín", "Gea de Albarracín", "Torres de Albarracín",
                                        "Tramacastiel", "Moscardón", "Royuela",
                                        "Calomarde", "Frías de Albarracín"
                                    ]
                                }
                            ]
                        }
                    }
                },
                asturias: {
                    nom: "Asturias",
                    columnes: ["Pais", "Comunitat Autonoma", "Provincia", "Conceyo", "Poble", "Registres", "Opcions"],
                    provincies: {
                        asturies: {
                            nom: "Asturies",
                            comarques: [
                                {
                                    nom: "Avilés",
                                    tipus: "conceyo",
                                    conceyos: [
                                        "Avilés", "Castrillón", "Corvera", "Illas", 
                                        "Llanera", "Muros", "Gozón", "Carreño"
                                    ]
                                },
                                {
                                    nom: "Caudal",
                                    tipus: "conceyo",
                                    conceyos: [
                                        "Lena", "Mieres", "Aller", "Riosa", 
                                        "Morcín", "Ribera de Arriba", "Uviéu"
                                    ]
                                },
                                {
                                    nom: "Eo-Navia",
                                    tipus: "conceyo",
                                    conceyos: [
                                        "Navia", "Valdés", "Cuaña", "El Franco", 
                                        "Tapia", "Castropol", "Vegadeo", "Santalla d'Ozcos", 
                                        "Samartín d'Ozcos", "Vilanova d'Ozcos", "Eilao", "Pezós"
                                    ]
                                },
                                {
                                    nom: "Xixón",
                                    tipus: "conceyo",
                                    conceyos: [
                                        "Xixón", "Villaviciosa", "Sariego", 
                                        "Nava", "Bimenes", "Colunga", "Caravia"
                                    ]
                                },
                                {
                                    nom: "Nalón",
                                    tipus: "conceyo",
                                    conceyos: [
                                        "Llangréu", "Llaviana", "Samartín del Rei Aurelio", 
                                        "Casorvia", "Sobrescobiu", "Llatres"
                                    ]
                                },
                                {
                                    nom: "Narcea",
                                    tipus: "conceyo",
                                    conceyos: [
                                        "Cangas del Narcea", "Degaña", "Ibias"
                                    ]
                                },
                                {
                                    nom: "Oriente",
                                    tipus: "conceyo",
                                    conceyos: [
                                        "Llanes", "Ribeseya", "Cangues d'Onís", 
                                        "Parres", "Onís", "Cabrales", 
                                        "Peñamellera Alta", "Peñamellera Baxa", "Amieva", "Ponga"
                                    ]
                                },
                                {
                                    nom: "Uviéu",
                                    tipus: "conceyo",
                                    conceyos: [
                                        "Uviéu", "Siero", "Llanera (part)", 
                                        "Santo Adriano", "Morcín (part)", "Ribera de Arriba (part)", 
                                        "Les Regueres", "Grau", "Yernes y Tameza", "Proaza", "Teberga", "Quirós"
                                    ]
                                }
                            ]

                        }
                    }
                },
                illesbalears: {
                    nom: "Illes Balears", // No va per que el nom te aquest espai, pero noesecito l'espai per que es el que apareix al select
                    columnes: ["Pais", "Comunitat Autonoma", "Provincia", "Illa", "Poble", "Registres", "Opcions"],
                    provincies: {
                        illesbalears: {
                            nom: "Illes Balears",
                            illes: [
                                {
                                    nom: "Mallorca",
                                    tipus: "illa",
                                    municipis: [
                                        "Palma", "Manacor", "Inca", "Llucmajor",
                                        "Sóller", "Pollença", "Felanitx", "Alcúdia",
                                        "Marratxí", "Santanyí", "Pollença", "Binissalem",
                                        "Santa Margalida", "Capdepera", "Consell", "Porreres"
                                    ]
                                },
                                {
                                    nom: "Menorca",
                                    tipus: "illa",
                                    municipis: [
                                        "Maó", "Ciutadella", "Alaior", "Sant Lluís",
                                        "Es Mercadal", "Ferreries", "Es Castell", "Es Migjorn Gran"
                                    ]
                                },
                                {
                                    nom: "Eivissa",
                                    tipus: "illa",
                                    municipis: [
                                        "Eivissa", "Sant Antoni de Portmany", "Santa Eulària des Riu",
                                        "Sant Josep de sa Talaia", "Sant Joan de Labritja"
                                    ]
                                },
                                {
                                    nom: "Formentera",
                                    tipus: "illa",
                                    municipis: [
                                        "Sant Francesc Xavier", "Sant Ferran de ses Roques",
                                        "El Pilar de la Mola", "Es Caló"
                                    ]
                                }
                            ]
                        }
                    }
                },
                canarias: {
                    nom: "Canarias",
                    columnes: ["Pais", "Comunitat Autonoma", "Provincia", "Illa", "Poble", "Registres", "Opcions"],
                    provincies: {
                        las_palmas: {
                            nom: "Las Palmas",
                            illes: [
                                {
                                    nom: "Gran Canaria",
                                    tipus: "illa",
                                    municipis: [
                                        "Las Palmas de Gran Canaria", "Telde", "Santa Lucía de Tirajana",
                                        "San Bartolomé de Tirajana", "Arucas", "Ingenio",
                                        "Agüimes", "Gáldar", "Mogán", "Teror",
                                        "Valsequillo", "Santa Brígida", "San Mateo", "Firgas",
                                        "Moya", "Valleseco", "Tejeda", "Artenara"
                                    ]
                                },
                                {
                                    nom: "Lanzarote",
                                    tipus: "illa",
                                    municipis: [
                                        "Arrecife", "Tías", "Yaiza", "San Bartolomé",
                                        "Teguise", "Tinajo", "Haría"
                                    ]
                                },
                                {
                                    nom: "Fuerteventura",
                                    tipus: "illa",
                                    municipis: [
                                        "Puerto del Rosario", "La Oliva", "Pájara",
                                        "Tuineje", "Antigua", "Betancuria"
                                    ]
                                },
                                {
                                    nom: "Illes Menors",
                                    tipus: "illa",
                                    municipis: [
                                        "La Graciosa", "Alegranza", "Montaña Clara",
                                        "Roque del Este", "Roque del Oeste", "Lobos"
                                    ]
                                }
                            ]
                        },
                        santa_cruz_tenerife: {
                            nom: "Santa Cruz de Tenerife",
                            illes: [
                                {
                                    nom: "Tenerife",
                                    tipus: "illa",
                                    municipis: [
                                        "Santa Cruz de Tenerife", "San Cristóbal de La Laguna", 
                                        "Arona", "Adeje", "Puerto de la Cruz", 
                                        "Los Realejos", "Granadilla de Abona", "Guía de Isora",
                                        "Candelaria", "Tacoronte", "Icod de los Vinos", 
                                        "La Orotava", "El Rosario", "Güímar", "Los Silos",
                                        "Buenavista del Norte", "San Miguel de Abona", "Santiago del Teide"
                                    ]
                                },
                                {
                                    nom: "La Palma",
                                    tipus: "illa",
                                    municipis: [
                                        "Santa Cruz de La Palma", "Los Llanos de Aridane", 
                                        "El Paso", "Breña Alta", "Breña Baja", 
                                        "Tazacorte", "San Andrés y Sauces", "Puntagorda",
                                        "Garafía", "Mazo", "Fuencaliente", "Villa de Mazo"
                                    ]
                                },
                                {
                                    nom: "La Gomera",
                                    tipus: "illa",
                                    municipis: [
                                        "San Sebastián de La Gomera", "Vallehermoso", 
                                        "Hermigua", "Alajeró", "Valle Gran Rey", 
                                        "Agulo"
                                    ]
                                },
                                {
                                    nom: "El Hierro",
                                    tipus: "illa",
                                    municipis: [
                                        "Valverde", "Frontera", "El Pinar"
                                    ]
                                }
                            ]
                        }
                    }
                },
                cantabria: {
                    nom: "Cantabria",
                    columnes: ["Pais", "Comunitat Autonoma", "Provincia", "Comarca", "Poble", "Registres", "Opcions"],
                    provincies: {
                        cantabria: {
                            nom: "Cantàbria",
                            valls: [
                                {
                                    nom: "Vall de Liébana",
                                    tipus: "comarca",
                                    municipis: [
                                        "Cabezón de Liébana", "Camaleño", "Cillórigo de Liébana", 
                                        "Pesaguero", "Potes", "Tresviso", "Vega de Liébana"
                                    ]
                                },
                                {
                                    nom: "Vall del Nansa",
                                    tipus: "comarca",
                                    municipis: [
                                        "Herrerías", "Lamasón", "Peñarrubia", 
                                        "Polaciones", "Rionansa", "Tudanca", "Valdáliga"
                                    ]
                                },
                                {
                                    nom: "Vall del Pas",
                                    tipus: "comarca",
                                    municipis: [
                                        "Corvera de Toranzo", "Luena", "Puente Viesgo", 
                                        "San Pedro del Romeral", "San Roque de Riomiera", 
                                        "Santa María de Cayón", "Santiurde de Toranzo", 
                                        "Vega de Pas", "Villacarriedo", "Villafufre"
                                    ]
                                },
                                {
                                    nom: "Vall del Saja",
                                    tipus: "comarca",
                                    municipis: [
                                        "Arenas de Iguña", "Cabezón de la Sal", "Cabuérniga", 
                                        "Herrerías", "Mazcuerras", "Reocín", "Ruente", 
                                        "Los Tojos", "Udías"
                                    ]
                                },
                                {
                                    nom: "Vall del Besaya",
                                    tipus: "comarca",
                                    municipis: [
                                        "Anievas", "Arenas de Iguña", "Bárcena de Pie de Concha", 
                                        "Cieza", "Los Corrales de Buelna", "Molledo", 
                                        "San Miguel de Aguayo", "Santiurde de Reinosa"
                                    ]
                                },
                                {
                                    nom: "Vall del Asón",
                                    tipus: "comarca",
                                    municipis: [
                                        "Ampuero", "Arredondo", "Guriezo", 
                                        "Hazas de Cesto", "Limpias", "Liendo", 
                                        "Rasines", "Ruesga", "Soba", "Voto"
                                    ]
                                },
                                {
                                    nom: "Vall del Miera",
                                    tipus: "comarca",
                                    municipis: [
                                        "Miera", "San Roque de Riomiera", "Santa María de Cayón"
                                    ]
                                },
                                {
                                    nom: "Costa Oriental",
                                    tipus: "comarca",
                                    municipis: [
                                        "Castro-Urdiales", "Colindres", "Laredo", 
                                        "Noja", "Santoña", "Bárcena de Cicero", 
                                        "Escalante", "Argoños", "Voto"
                                    ]
                                },
                                {
                                    nom: "Costa Occidental",
                                    tipus: "comarca",
                                    municipis: [
                                        "San Vicente de la Barquera", "Val de San Vicente", 
                                        "Valdáliga", "Comillas", "Udías", "Alfoz de Lloredo"
                                    ]
                                },
                                {
                                    nom: "Campoo-Los Valles",
                                    tipus: "comarca",
                                    municipis: [
                                        "Campoo de Enmedio", "Campoo de Yuso", "Hermandad de Campoo de Suso", 
                                        "Las Rozas de Valdearroyo", "Reinosa", "Valdeolea", 
                                        "Valdeprado del Río", "Valderredible"
                                    ]
                                }
                            ]
                        }
                    }
                },
                "castilla y leon": {
                    nom: "Castilla y León", // No va per que el nom te espais, pero noesecito l'espai per que es el que apareix al select
                    columnes: ["Pais", "Comunitat Autonoma", "Provincia", "Comarca", "Poble", "Registres", "Opcions"],
                    provincies: {
                        avila: {
                            nom: "Àvila",
                            comarques: [
                                {
                                    nom: "Valle del Tiétar",
                                    tipus: "comarca",
                                    municipis: [
                                        "Arenas de San Pedro", "Candeleda", "Sotillo de la Adrada", 
                                        "El Tiemblo", "La Adrada", "Casavieja", "Piedralaves",
                                        "Guisando", "Hoyos del Espino", "Lanzahíta", "Mombeltrán",
                                        "Navahondilla", "Navalperal de Tormes", "Navarredonda de Gredos",
                                        "San Esteban del Valle", "Santa Cruz del Valle", "Santa María del Tiétar",
                                        "Serranillos", "Villarejo del Valle"
                                    ]
                                },
                                {
                                    nom: "Valle del Alberche",
                                    tipus: "comarca",
                                    municipis: [
                                        "Burgohondo", "El Barco de Ávila", "El Barraco", 
                                        "Navaluenga", "Navarredondilla", "Navatalgordo",
                                        "Navatejares", "San Juan de la Nava", "San Juan del Molinillo",
                                        "Santa Cruz de Pinares", "Santiago del Collado", "Serranillos",
                                        "Solana de Ávila", "Villafranca de la Sierra", "Villanueva de Ávila"
                                    ]
                                },
                                {
                                    nom: "La Moraña",
                                    tipus: "comarca",
                                    municipis: [
                                        "Arévalo", "Madrigal de las Altas Torres", "Fontiveros", 
                                        "San Pascual", "Donvidas", "Espinosa de los Caballeros",
                                        "Hernansancho", "Horcajo de las Torres", "Langayo",
                                        "Mamblas", "Mingorría", "Narros del Castillo",
                                        "Palacios de Goda", "Papatrigo", "Riocabado",
                                        "San Vicente de Arévalo", "Sinlabajos", "Tiñosillos"
                                    ]
                                },
                                {
                                    nom: "Tierra de Ávila",
                                    tipus: "comarca",
                                    municipis: [
                                        "Ávila", "Martiherrero", "Mingorría", 
                                        "Mironcillo", "Ojos-Albos", "Padiernos",
                                        "Pradosegar", "Riofrío", "Salobral",
                                        "San Esteban de los Patos", "San Pedro del Arroyo", "Santa María del Arroyo",
                                        "Solosancho", "Villanueva de Gómez", "Villatoro"
                                    ]
                                },
                                {
                                    nom: "Sierra de Gredos",
                                    tipus: "comarca",
                                    municipis: [
                                        "Bohoyo", "Candeleda", "Gil García", 
                                        "Guisando", "Hoyos del Espino", "Hoyos del Collado",
                                        "Navadijos", "Navarredonda de Gredos", "Puerto Castilla",
                                        "San Juan de Gredos", "Santiago del Collado", "Solana de Ávila",
                                        "Umbrías", "Zapardiel de la Ribera"
                                    ]
                                },
                                {
                                    nom: "Tierra de Piedrahíta",
                                    tipus: "comarca",
                                    municipis: [
                                        "Piedrahíta", "Bonilla de la Sierra", "Malpartida de Corneja", 
                                        "Mesegar de Corneja", "Narrillos del Álamo", "Narrillos del Rebollar",
                                        "San Bartolomé de Corneja", "San Miguel de Corneja", "Santa María de los Caballeros",
                                        "Santiago de Corneja", "Tórtoles", "Villafranca de la Sierra"
                                    ]
                                },
                                {
                                    nom: "Tierra de Arévalo",
                                    tipus: "comarca",
                                    municipis: [
                                        "Arévalo", "Donvidas", "Espinosa de los Caballeros", 
                                        "Hernansancho", "Horcajo de las Torres", "Langayo",
                                        "Mamblas", "Palacios de Goda", "Papatrigo",
                                        "Riocabado", "San Vicente de Arévalo", "Sinlabajos"
                                    ]
                                }
                            ]
                        },
                        burgos: {
                            nom: "Burgos",
                            comarques: [
                                {
                                    nom: "Comarca de Burgos",
                                    tipus: "comarca",
                                    municipis: [
                                        "Burgos", "Villagonzalo Pedernales", "Villalbilla de Burgos", 
                                        "Cardeñadijo", "Cardeñajimeno", "Castrillo del Val", 
                                        "Modúbar de la Emparedada", "Sotragero", "Villariezo",
                                        "Quintanadueñas", "Sarracín", "Cogollos", 
                                        "Arcos", "Villangómez", "Cayuela"
                                    ]
                                },
                                {
                                    nom: "La Bureba",
                                    tipus: "comarca",
                                    municipis: [
                                        "Briviesca", "Oña", "Poza de la Sal", 
                                        "Los Barrios de Bureba", "Belorado", "Quintanabureba",
                                        "Rojas", "Viloria de Rioja", "Santa María Rivarredonda",
                                        "Galbarros", "Grisaleña", "Padrones de Bureba",
                                        "Ruiforco de Bureba", "Zuñeda"
                                    ]
                                },
                                {
                                    nom: "Valle del Arlanza",
                                    tipus: "comarca",
                                    municipis: [
                                        "Lerma", "Covarrubias", "Santo Domingo de Silos", 
                                        "Quintanilla del Agua", "Tordómar", "Villalmanzo",
                                        "Villamayor de los Montes", "Villangómez", "Santa María del Campo",
                                        "Mahamud", "Torrepadre", "Cogollos"
                                    ]
                                },
                                {
                                    nom: "Valle del Esgueva",
                                    tipus: "comarca",
                                    municipis: [
                                        "Villanueva de Gumiel", "Terradillos de Esgueva", "Cabañes de Esgueva", 
                                        "Pinilla-Trasmonte", "Santibáñez de Esgueva", "Tórtoles de Esgueva",
                                        "Villatuelda", "Valles de Palenzuela", "Torrepadre"
                                    ]
                                },
                                {
                                    nom: "Sierra de la Demanda",
                                    tipus: "comarca",
                                    municipis: [
                                        "Salas de los Infantes", "Quintanar de la Sierra", "Canicosa de la Sierra", 
                                        "Regumiel de la Sierra", "Vilviestre del Pinar", "Monterrubio de la Demanda",
                                        "Barbadillo del Mercado", "Huerta de Rey", "Rabanera del Pinar"
                                    ]
                                },
                                {
                                    nom: "Arlanza",
                                    tipus: "comarca",
                                    municipis: [
                                        "Lerma", "Covarrubias", "Santo Domingo de Silos", 
                                        "Quintanilla del Agua", "Tordómar", "Villalmanzo",
                                        "Villamayor de los Montes", "Villangómez", "Santa María del Campo",
                                        "Mahamud", "Torrepadre", "Cogollos"
                                    ]
                                },
                                {
                                    nom: "Ribera del Duero",
                                    tipus: "comarca",
                                    municipis: [
                                        "Aranda de Duero", "Roa", "Peñaranda de Duero", 
                                        "La Vid y Barrios", "Villalba de Duero", "Gumiel de Izán",
                                        "Sotillo de la Ribera", "Valdezate", "Hontangas"
                                    ]
                                },
                                {
                                    nom: "Páramos",
                                    tipus: "comarca",
                                    municipis: [
                                        "Castrojeriz", "Melgar de Fernamental", "Padilla de Abajo", 
                                        "Villadiego", "Sasamón", "Cayuela",
                                        "Estépar", "Hontanas", "Itero del Castillo"
                                    ]
                                },
                                {
                                    nom: "Ebro",
                                    tipus: "comarca",
                                    municipis: [
                                        "Miranda de Ebro", "Villarcayo", "Medina de Pomar", 
                                        "Espinosa de los Monteros", "Oña", "Frias",
                                        "Trespaderne", "Valle de Tobalina", "Valle de Mena"
                                    ]
                                }
                            ]
                        },
                        lleo: {
                            nom: "Lleó",
                            comarques: [
                                {
                                    nom: "Tierras de Llión",
                                    tipus: "comarca",
                                    municipis: [
                                        "Llión", "San Andrés del Rabanedo", "Villaquilambre", 
                                        "Sariegos", "Valverde de la Virgen", "Chozas de Abajo",
                                        "Cuadros", "Garfe", "Valdefresno",
                                        "Villaturiel", "Santovenia de la Valdoncina"
                                    ]
                                },
                                {
                                    nom: "El Bierzu",
                                    tipus: "comarca",
                                    municipis: [
                                        "Ponferrada", "Bembibre", "Villafranca del Bierzu", 
                                        "Cacabelos", "Toreno", "Camponaraya",
                                        "Carracedelo", "Torre del Bierzu", "Folgoso de la Ribera",
                                        "Castropodame", "Congostu"
                                    ]
                                },
                                {
                                    nom: "La Montaña Occidental",
                                    tipus: "comarca",
                                    municipis: [
                                        "Villablino", "Palacios del Sil", "Páramu del Sil", 
                                        "Igüeña", "Murias de Paredes", "Riello",
                                        "Valdesamariu", "Vega de Espinareda", "Candín",
                                        "Peranzanes", "Fabero"
                                    ]
                                },
                                {
                                    nom: "La Cabreira",
                                    tipus: "comarca",
                                    municipis: [
                                        "Truchillas", "Castrillo de Cabreira", "Castrocontrigo", 
                                        "Encinedo", "Marrubiu", "Posada de Valdeón"
                                    ]
                                },
                                {
                                    nom: "Valle del Órbigo",
                                    tipus: "comarca",
                                    municipis: [
                                        "La Bañeza", "Santa María del Páramu", "Villareyu d'Órbigu", 
                                        "Veguellina de Órbigo", "Carrizo de la Ribera", "Hospital de Órbigo",
                                        "Benavides", "Turcia", "Santas Martas"
                                    ]
                                },
                                {
                                    nom: "Tierras de Sahagún",
                                    tipus: "comarca",
                                    municipis: [
                                        "Sahagún", "Gradefes", "El Burgo Ranero", 
                                        "Calzada del Coto", "Gordaliza del Pino", "Joarilla de las Matas",
                                        "Vallecillo", "Villamol", "Villamontán de la Valduerna"
                                    ]
                                },
                                {
                                    nom: "Maragatería",
                                    tipus: "comarca",
                                    municipis: [
                                        "Astorga", "Santa Colomba de Somoza", "Val de San Lloriente", 
                                        "Luyego", "Brazuelo", "Lucillo",
                                        "Castrillo de los Polvazares", "Santiago Millas"
                                    ]
                                },
                                {
                                    nom: "Valle del Luna",
                                    tipus: "comarca",
                                    municipis: [
                                        "Sena de Luna", "Barrios de Luna", "Los Barrios de Luna", 
                                        "Riello", "Soto y Amío", "Villamanín",
                                        "La Pola de Gordón"
                                    ]
                                },
                                {
                                    nom: "Tierra de La Bañeza",
                                    tipus: "comarca",
                                    municipis: [
                                        "La Bañeza", "Santa Elena de Jamuz", "Palacios de la Valduerna", 
                                        "Quintana del Marco", "Roperuelos del Páramo", "San Cristóbal de la Polantera",
                                        "Villademor de la Vega"
                                    ]
                                }
                            ]
                        },
                        palencia: {
                            nom: "Palença",
                            comarques: [
                                {
                                    nom: "Tierra de Campos",
                                    tipus: "comarca",
                                    municipis: [
                                        "Palença", "Dueñas", "Venta de Baños",
                                        "Villarramiel", "Frechilla", "Fuentes de Nava",
                                        "Grijota", "Husillos", "Autilla del Pino",
                                        "Becerril de Campos", "Paredes de Nava", "Cisneros",
                                        "Capillas", "Castil de Vela", "Castrillo de Villavega"
                                    ]
                                },
                                {
                                    nom: "El Cerrato",
                                    tipus: "comarca",
                                    municipis: [
                                        "Baltanás", "Dueñas", "Hornillos de Cerrato",
                                        "Torquemada", "Venta de Baños", "Villaconancio",
                                        "Villaviudas", "Cevico Navero", "Cevico de la Torre",
                                        "Antigüedad", "Cubillas de Cerrato", "Valdecañas de Cerrato"
                                    ]
                                },
                                {
                                    nom: "La Montaña Palentina",
                                    tipus: "comarca",
                                    municipis: [
                                        "Aguilar de Campoo", "Cervera de Pisuerga", "Guardo",
                                        "Salinas de Pisuerga", "Brañosera", "Barruelo de Santullán",
                                        "Santibáñez de la Peña", "Castrejón de la Peña", "Dehesa de Montejo",
                                        "La Pernía", "Polentinos", "Respenda de la Peña"
                                    ]
                                },
                                {
                                    nom: "Vega-Valdavia",
                                    tipus: "comarca",
                                    municipis: [
                                        "Saldaña", "Carrión de los Condes", "Loma de Ucieza",
                                        "Villamuera de la Cueza", "Villoldo", "Pino del Río",
                                        "Villalobón", "Villamuriel de Cerrato", "San Cebrián de Campos",
                                        "Villamartín de Campos", "Villacidaler", "Boadilla del Camino"
                                    ]
                                },
                                {
                                    nom: "Boedo-Ojeda",
                                    tipus: "comarca",
                                    municipis: [
                                        "Herrera de Pisuerga", "Alar del Rey", "Olmos de Ojeda",
                                        "Dehesa de Romanos", "Nogales de Pisuerga", "San Andrés de Arroyo",
                                        "Santibáñez de Ecla", "Villabasta de Valdavia", "Villaeles de Valdavia",
                                        "Villanuño de Valdavia", "Congosto de Valdavia", "Buenavista de Valdavia"
                                    ]
                                },
                                {
                                    nom: "Camino de Santiago",
                                    tipus: "comarca",
                                    municipis: [
                                        "Carrión de los Condes", "Frómista", "Itero de la Vega",
                                        "Boadilla del Camino", "Villalcázar de Sirga", "Lédigos",
                                        "San Nicolás del Real Camino", "Calzada de los Molinos", "Villovieco",
                                        "Villotilla", "Revenga de Campos", "Villarmentero de Campos"
                                    ]
                                }
                            ]
                        },
                        salamanca: {
                            nom: "Salamanca",
                            comarques: [
                                {
                                    nom: "Camp Charro",
                                    tipus: "comarca",
                                    municipis: [
                                        "Salamanca", "Santa Marta de Tormes", "Villamayor", 
                                        "Carbajosa de la Sagrada", "Doñinos de Salamanca", 
                                        "Aldeatejada", "Arapiles", "Calvarrasa de Abajo",
                                        "Carrascal de Barregas", "Machacón", "Pelabravo",
                                        "San Cristóbal de la Cuesta", "Terradillos", "Villoría"
                                    ]
                                },
                                {
                                    nom: "La Armuña",
                                    tipus: "comarca",
                                    municipis: [
                                        "Topas", "Calzada de Valdunciel", "Valdunciel", 
                                        "Villares de la Reina", "Aldealengua", 
                                        "Aldearrubia", "Moriscos", "San Morales",
                                        "Villaverde de Guareña", "Zorita de la Frontera",
                                        "El Pedroso de la Armuña", "Peleas de Abajo",
                                        "Palencia de Negrilla", "Negrilla de Palencia"
                                    ]
                                },
                                {
                                    nom: "Las Arribes",
                                    tipus: "comarca",
                                    municipis: [
                                        "Vilvestre", "Saucelle", "Masueco", 
                                        "Aldeadávila de la Ribera", "Mieza", 
                                        "La Fregeneda", "Hinojosa de Duero", 
                                        "Sobradillo", "Ahigal de los Aceiteros",
                                        "Barruecopardo", "Saldeana", "La Zarza de Pumareda"
                                    ]
                                },
                                {
                                    nom: "Sierra de Francia",
                                    tipus: "comarca",
                                    municipis: [
                                        "La Alberca", "Miranda del Castañar", "Mogarraz", 
                                        "San Martín del Castañar", "Sequeros", 
                                        "Villanueva del Conde", "Sotoserrano", 
                                        "Madroñal", "Monforte de la Sierra",
                                        "Casares de las Hurdes", "El Tornadizo",
                                        "San Esteban de la Sierra", "Valero"
                                    ]
                                },
                                {
                                    nom: "Comarca de Ciudad Rodrigo",
                                    tipus: "comarca",
                                    municipis: [
                                        "Ciudad Rodrigo", "Fuenteguinaldo", "Ituero de Azaba", 
                                        "Campillo de Azaba", "Espeja", 
                                        "Gallegos de Argañán", "Martiago", 
                                        "Pastores", "La Alameda de Gardón",
                                        "Saelices el Chico", "Villar de Argañán",
                                        "Villar de la Yegua", "Zamarra"
                                    ]
                                },
                                {
                                    nom: "Tierra de Peñaranda",
                                    tipus: "comarca",
                                    municipis: [
                                        "Peñaranda de Bracamonte", "Macotera", "Alaraz", 
                                        "Aldeaseca de la Armuña", "Bóveda del Río Almar", 
                                        "Cantalpino", "El Campo de Peñaranda", 
                                        "Nava de Sotrobal", "Parada de Arriba",
                                        "Rágama", "Salvatierra de Tormes",
                                        "Tordillos", "Ventosa del Río Almar"
                                    ]
                                },
                                {
                                    nom: "Valle del Tormes",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alba de Tormes", "Guijuelo", "Beleña", 
                                        "Encinas de Abajo", "Galinduste", 
                                        "Galisancho", "Huerta", 
                                        "Pedrosillo el Ralo", "Pitiegua",
                                        "Valdecarros", "Valverdón",
                                        "Villoria", "Villoruela"
                                    ]
                                },
                                {
                                    nom: "Tierra de Ledesma",
                                    tipus: "comarca",
                                    municipis: [
                                        "Ledesma", "Aldearrodrigo", "Doñinos de Ledesma", 
                                        "Gejuelo del Barro", "Golpejas", 
                                        "Juzbado", "La Mata de Ledesma", 
                                        "Palacios del Arzobispo", "Sardón de los Frailes",
                                        "Tabera de Abajo", "Villar de Gallimazo",
                                        "Villar de Peralonso", "Zamayón"
                                    ]
                                }
                            ]
                        },
                        segovia: {
                            nom: "Segòvia",
                            comarques: [
                                {
                                    nom: "Capital i Alfoz",
                                    tipus: "comarca",
                                    municipis: [
                                        "Segòvia", "La Lastrilla", "San Cristóbal de Segovia", 
                                        "Palazuelos de Eresma", "Trescasas", "Torrecaballeros",
                                        "Espirdo", "Bernuy de Porreros", "Cabañas de Polendos",
                                        "Valverde del Majano", "Hontanares de Eresma", "Valseca"
                                    ]
                                },
                                {
                                    nom: "Tierra de Pinares",
                                    tipus: "comarca",
                                    municipis: [
                                        "Cuéllar", "Navas de Oro", "Carbonero el Mayor", 
                                        "Cantimpalos", "Sanchonuño", "Fresneda de Cuéllar",
                                        "Chatún", "Campo de Cuéllar", "Mozoncillo",
                                        "Pinarejos", "Samboal", "Chañe"
                                    ]
                                },
                                {
                                    nom: "Valle del Eresma",
                                    tipus: "comarca",
                                    municipis: [
                                        "Villacastín", "Navas de San Antonio", "Ortigosa del Monte", 
                                        "Valdeprados", "Sotosalbos", "Añe",
                                        "Basardilla", "Bercial", "Cabañas de Polendos",
                                        "Cobos de Fuentidueña", "Fuentepelayo", "Juarros de Voltoya"
                                    ]
                                },
                                {
                                    nom: "Sierra de Guadarrama",
                                    tipus: "comarca",
                                    municipis: [
                                        "Real Sitio de San Ildefonso", "Rascafría", "Alpedrete", 
                                        "Collado Hermoso", "Sotosalbos", "Otero de Herreros",
                                        "El Espinar", "La Losa", "Navas de Riofrío",
                                        "Palazuelos de Eresma", "Revenga", "Veganzones"
                                    ]
                                },
                                {
                                    nom: "Tierra de Ayllón",
                                    tipus: "comarca",
                                    municipis: [
                                        "Ayllón", "Riaza", "Cantalejo", 
                                        "Sebúlcor", "Fresno de Cantespino", "Aldealcorvo",
                                        "Alconada de Maderuelo", "Bercimuel", "Corral de Ayllón",
                                        "Estebanvela", "Francos", "Santibañez de Ayllón"
                                    ]
                                },
                                {
                                    nom: "Campo de Sepúlveda",
                                    tipus: "comarca",
                                    municipis: [
                                        "Sepúlveda", "Cantalejo", "Sebúlcor", 
                                        "Duruelo", "Castroserna de Abajo", "Condado de Castilnovo",
                                        "Aldealcorvo", "Aldeonte", "Barbolla",
                                        "Carrascal del Río", "Castillejo de Mesleón", "Castrojimeno"
                                    ]
                                },
                                {
                                    nom: "Tierra de Fuentidueña",
                                    tipus: "comarca",
                                    municipis: [
                                        "Fuentidueña", "Castro de Fuentidueña", "Cobos de Fuentidueña", 
                                        "Fuenterrebollo", "Adrados", "Bercimuel",
                                        "Castroserracín", "Cozuelos de Fuentidueña", "Fuentesoto",
                                        "Lastras de Cuéllar", "Mata de Cuéllar", "San Miguel de Bernuy"
                                    ]
                                },
                                {
                                    nom: "Valle del Duratón",
                                    tipus: "comarca",
                                    municipis: [
                                        "Sebúlcor", "Carrascal del Río", "San Miguel de Bernuy", 
                                        "Valleruela de Pedraza", "Urueñas", "Valdevacas y Guijar",
                                        "Aldealcorvo", "Aldeonte", "Barbolla",
                                        "Castroserna de Abajo", "Condado de Castilnovo", "Duruelo"
                                    ]
                                }
                            ]
                        },
                        soria: {
                            nom: "Sòria",
                            comarques: [
                                {
                                    nom: "Comarca de Sòria",
                                    tipus: "comarca",
                                    municipis: [
                                        "Sòria", "Almazán", "Garray", 
                                        "Los Rábanos", "Tardelcuende", "Fuentepinilla",
                                        "Buitrago", "Cidones", "Cubilla",
                                        "Fuentelsaz de Soria", "Golmayo", "Quintana Redonda"
                                    ]
                                },
                                {
                                    nom: "Tierras Altas",
                                    tipus: "comarca",
                                    municipis: [
                                        "San Pedro Manrique", "Yanguas", "Oncala", 
                                        "Villar del Río", "Las Aldehuelas", "Arévalo de la Sierra",
                                        "Ausejo de la Sierra", "Carrascosa de la Sierra", "Castilfrío de la Sierra",
                                        "Cerbón", "Estepa de San Juan", "Fuentes de Magaña"
                                    ]
                                },
                                {
                                    nom: "Valle del Duero",
                                    tipus: "comarca",
                                    municipis: [
                                        "Berlanga de Duero", "Almazán", "Bayubas de Abajo", 
                                        "Bayubas de Arriba", "Fuentepinilla", "Matamala de Almazán",
                                        "Nepas", "Quintanas de Gormaz", "Recuerda",
                                        "Rioseco de Soria", "Tajueco", "Valdenebro"
                                    ]
                                },
                                {
                                    nom: "Comarca de Pinares",
                                    tipus: "comarca",
                                    municipis: [
                                        "Covaleda", "Duruelo de la Sierra", "San Leonardo de Yagüe", 
                                        "Abejar", "Casarejos", "Molinos de Duero",
                                        "Salduero", "Vinuesa", "Navaleno",
                                        "Cubilla", "Espeja de San Marcelino", "Talveila"
                                    ]
                                },
                                {
                                    nom: "Campo de Gómara",
                                    tipus: "comarca",
                                    municipis: [
                                        "Gómara", "Aliud", "Almenar de Soria", 
                                        "Cabrejas del Campo", "Cihuela", "Deza",
                                        "Mazalvete", "Portillo de Soria", "Serón de Nágima",
                                        "Torrubia de Soria", "Villaseca de Arciel", "Velilla de la Sierra"
                                    ]
                                },
                                {
                                    nom: "Tierras del Burgo",
                                    tipus: "comarca",
                                    municipis: [
                                        "El Burgo de Osma", "Berzosa", "Calatañazor", 
                                        "Carrascosa de Abajo", "Fuentearmegil", "Langa de Duero",
                                        "Liceras", "Miño de San Esteban", "Nafría de Ucero",
                                        "San Esteban de Gormaz", "Ucero", "Valdemaluque"
                                    ]
                                },
                                {
                                    nom: "Moncayo",
                                    tipus: "comarca",
                                    municipis: [
                                        "Ágreda", "Ólvega", "Borobia", 
                                        "Cueva de Ágreda", "Dévanos", "Matalebreras",
                                        "Noviercas", "Vozmediano", "Aldehuela de Ágreda",
                                        "Beratón", "Castilruiz", "Fuentestrún"
                                    ]
                                },
                                {
                                    nom: "Valle del Tera",
                                    tipus: "comarca",
                                    municipis: [
                                        "Almarza", "Arévalo de la Sierra", "Ausejo de la Sierra", 
                                        "Carrascosa de la Sierra", "Castilfrío de la Sierra", "Cirujales del Río",
                                        "Estepa de San Juan", "Fuentelsaz de Soria", "Gallinero", 
                                        "Hinojosa del Campo", "La Losilla", "Las Aldehuelas"
                                    ]
                                }
                            ]
                        },
                        valladolid: {
                            nom: "Valladolid",
                            comarques: [
                                {
                                    nom: "Tierra de Campos",
                                    tipus: "comarca",
                                    municipis: [
                                        "Medina de Rioseco", "Villalón de Campos", "Mayorga", 
                                        "Cuenca de Campos", "Villabrágima", "Castromonte",
                                        "Villafrechós", "Villagarcía de Campos", "Villalba de la Loma",
                                        "Villanueva de los Caballeros", "Villavicencio de los Caballeros",
                                        "Becilla de Valderaduey", "Castroponce", "Castroverde de Cerrato"
                                    ]
                                },
                                {
                                    nom: "Campo de Peñafiel",
                                    tipus: "comarca",
                                    municipis: [
                                        "Peñafiel", "Pesquera de Duero", "Quintanilla de Onésimo", 
                                        "Curiel de Duero", "San Llorente", "Manzanillo",
                                        "Olmos de Peñafiel", "Padilla de Duero", "Piñel de Abajo",
                                        "Piñel de Arriba", "Rábano", "Roturas",
                                        "Valbuena de Duero", "San Martín de Rubios"
                                    ]
                                },
                                {
                                    nom: "Tierra de Pinares",
                                    tipus: "comarca",
                                    municipis: [
                                        "Íscar", "Portillo", "Cogeces de Íscar", 
                                        "Megeces", "Pedrajas de San Esteban", "Alcazarén",
                                        "Aguasal", "Boecillo", "La Parrilla",
                                        "Montemayor de Pililla", "Santovenia de Pisuerga", "Traspinedo"
                                    ]
                                },
                                {
                                    nom: "Páramos del Esgueva",
                                    tipus: "comarca",
                                    municipis: [
                                        "Renedo de Esgueva", "Villabáñez", "Castrillo-Tejeriego", 
                                        "Amusquillo", "Esguevillas de Esgueva", "Población de Cerrato",
                                        "Torrelobatón", "Villafuerte", "Villaco",
                                        "San Salvador", "Villafrades de Campos", "Villán de Tordesillas"
                                    ]
                                },
                                {
                                    nom: "Tierras del Cerrato",
                                    tipus: "comarca",
                                    municipis: [
                                        "Baltanás", "Hornillos de Cerrato", "Tariego de Cerrato", 
                                        "Cevico Navero", "Antigüedad", "Cubillas de Cerrato",
                                        "Valdecañas de Cerrato", "Vertavillo", "Castrillo de Onielo",
                                        "Palenzuela", "Tabanera de Cerrato", "Villaconancio"
                                    ]
                                },
                                {
                                    nom: "Ribera del Duero",
                                    tipus: "comarca",
                                    municipis: [
                                        "Tudela de Duero", "Tordesillas", "La Seca", 
                                        "Rueda", "Serrada", "Villanueva de Duero",
                                        "Viana de Cega", "San Miguel del Pino", "Geria",
                                        "Villamarciel", "Villalbarba", "Vega de Valdetronco"
                                    ]
                                },
                                {
                                    nom: "Área Metropolitana de Valladolid",
                                    tipus: "comarca",
                                    municipis: [
                                        "Valladolid", "Laguna de Duero", "Arroyo de la Encomienda", 
                                        "Cistérniga", "Santovenia de Pisuerga", "Simancas",
                                        "Viana de Cega", "Zaratán", "Fuensaldaña",
                                        "Villanubla", "Cabezón de Pisuerga", "Bocigas"
                                    ]
                                },
                                {
                                    nom: "Tierra de Medina",
                                    tipus: "comarca",
                                    municipis: [
                                        "Medina del Campo", "Rubí de Bracamonte", "Ramiro", 
                                        "Velascálvaro", "Villaverde de Medina", "Nava del Rey",
                                        "Fresno el Viejo", "Alaejos", "Siete Iglesias de Trabancos",
                                        "Castrejón de Trabancos", "Villalar de los Comuneros", "Torrecilla de la Orden"
                                    ]
                                }
                            ]
                        },
                        zamora: {
                            nom: "Zamora",
                            comarques: [
                                {
                                    nom: "Tierra del Pan",
                                    tipus: "comarca",
                                    municipis: [
                                        "Abezames", "Algodre", "Almaraz de Duero",
                                        "Andavías", "Arquillinos", "Benegiles",
                                        "Bustillo del Oro", "Carbajosa de Alba", "Coreses",
                                        "Cubillos", "La Hiniesta", "Manganeses de la Lampreana",
                                        "Matilla la Seca", "Molacillos", "Monfarracinos",
                                        "Montamarta", "Morales del Vino", "Moreruela de los Infanzones",
                                        "Muelas del Pan", "Pajares de la Lampreana", "Palacios del Pan",
                                        "Piedrahita de Castro", "Roales del Pan", "San Cebrián de Castro",
                                        "San Pelayo", "Santa Clara de Avedillo", "Santa Eufemia del Barco",
                                        "Toro", "Valcabado", "Villalazán",
                                        "Villalube", "Villanueva de Campeán", "Villaralbo",
                                        "Villardeciervos", "Villarrín de Campos", "Zamora"
                                    ]
                                },
                                {
                                    nom: "Tierra de Campos",
                                    tipus: "comarca",
                                    municipis: [
                                        "Belver de los Montes", "Cañizo", "Castroverde de Campos",
                                        "Cerecinos de Campos", "Cotanes del Monte", "Granja de Moreruela",
                                        "Prado", "Revellinos", "San Agustín del Pozo",
                                        "San Cebrián de Castro", "San Esteban del Molar", "San Martín de Valderaduey",
                                        "Tapioles", "Vidayanes", "Villafáfila",
                                        "Villalobos", "Villalpando", "Villamayor de Campos",
                                        "Villanueva del Campo", "Villárdiga", "Villardondiego"
                                    ]
                                },
                                {
                                    nom: "Benavente y Los Valles",
                                    tipus: "comarca",
                                    municipis: [
                                        "Arcos de la Polvorosa", "Bretó", "Bretocino",
                                        "Burganes de Valverde", "Bustillo del Oro", "Camarzana de Tera",
                                        "Castronuevo", "Castroverde de Campos", "Coomonte",
                                        "Cubo de Benavente", "Fresno de la Polvorosa", "Fuentes de Ropel",
                                        "Milles de la Polvorosa", "Manganeses de la Polvorosa", "Navianos de Valverde",
                                        "Pobladura del Valle", "Quintanilla de Urz", "Quiruelas de Vidriales",
                                        "Santa Clara de Avedillo", "Santa Colomba de las Monjas", "Santa Cristina de la Polvorosa",
                                        "Santa Croya de Tera", "Santovenia", "Benavente",
                                        "Villabrázaro", "Villanázar", "Villaveza del Agua",
                                        "Villaveza de Valverde"
                                    ]
                                },
                                {
                                    nom: "La Guareña",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alfaraz de Sayago", "Cañizal", "Cuelgamures",
                                        "El Maderal", "Fuentelapeña", "Fuentesaúco",
                                        "Guarrate", "Peleas de Abajo", "Peleas de Arriba",
                                        "Pozoantiguo", "Torrecilla de la Orden", "Vadillo de la Guareña",
                                        "Vallesa de la Guareña", "Villaescusa", "Villamor de los Escuderos",
                                        "Villanueva de Campeán", "Villaralbo"
                                    ]
                                },
                                {
                                    nom: "Sayago",
                                    tipus: "comarca",
                                    municipis: [
                                        "Almeida de Sayago", "Argañín", "Bermillo de Sayago",
                                        "Carbellino", "Fermoselle", "Figueruela de Sayago",
                                        "Fresno de Sayago", "Gamones", "Luelmo",
                                        "Mayalde", "Moral de Sayago", "Moralina",
                                        "Muga de Sayago", "Peñausende", "Pereruela",
                                        "Roelos", "Salce", "Samir de los Caños",
                                        "San Román de los Infantes", "Torregamones", "Villadepera",
                                        "Villar del Buey", "Villardiegua de la Ribera", "Zamarra"
                                    ]
                                },
                                {
                                    nom: "Aliste",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alcañices", "Brandilanes", "Cabañas de Aliste",
                                        "Campogrande de Aliste", "Castro de Alcañices", "Ceadea",
                                        "Fonfría", "Forfoleda", "Gallegos del Río",
                                        "Lober", "Mahíde", "Mellanes",
                                        "Palazuelo de las Cuevas", "Pino del Oro", "Rabanales",
                                        "Riofrío de Aliste", "Samir de los Caños", "San Blas",
                                        "San Cristóbal de Aliste", "San Juan del Rebollar", "San Vitero",
                                        "Tola", "Trabazos", "Vega de Nuez",
                                        "Villalcampo", "Villarino Tras la Sierra"
                                    ]
                                },
                                {
                                    nom: "Sanabria",
                                    tipus: "comarca",
                                    municipis: [
                                        "Asturianos", "Cobreros", "Galende",
                                        "Hermisende", "Lubián", "Palacios de Sanabria",
                                        "Pedralba de la Pradería", "Pías", "Porto",
                                        "Puebla de Sanabria", "Requejo", "Robleda-Cervantes",
                                        "Rosinos de la Requejada", "San Justo", "Trefacio",
                                        "Lobeznos", "Sampil", "Santa Colomba de Sanabria",
                                        "Triufé", "Villardeciervos", "Villarino de Sanabria"
                                    ]
                                },
                                {
                                    nom: "Tierra de Tábara",
                                    tipus: "comarca",
                                    municipis: [
                                        "Bercianos de Valverde", "Ferreras de Abajo", "Ferreras de Arriba",
                                        "Ferreruela", "Escober de Tábara", "Fresno de la Ribera",
                                        "Friera de Valverde", "Morales de Valverde", "Moreruela de Tábara",
                                        "Olmillos de Castro", "Pereña de la Ribera", "Pozuelo de Tábara",
                                        "Tábara", "Villanueva de las Peras", "Villaveza de Valverde"
                                    ]
                                }
                            ]
                        }
                    }
                },
                "castilla - la mancha": {
                    nom: "Castilla - La Mancha",
                    columnes: ["Pais", "Comunitat Autonoma", "Provincia", "Comarca", "Poble", "Registres", "Opcions"],
                    provincies: {
                        albacete: {
                            nom: "Albacete",
                            comarques: [
                                {
                                    nom: "Llanos de Albacete",
                                    tipus: "comarca",
                                    municipis: [
                                        "Albacete", "Barrax", "Chinchilla de Monte-Aragón",
                                        "La Gineta", "Hoya-Gonzalo", "Pozohondo",
                                        "Pozuelo", "Tarazona de la Mancha", "Valdeganga"
                                    ]
                                },
                                {
                                    nom: "La Mancha del Júcar-Centro",
                                    tipus: "comarca",
                                    municipis: [
                                        "Casas de Juan Núñez", "Casasimarro", "Cenizate",
                                        "Fuentealbilla", "Golosalvo", "Madrigueras",
                                        "Mahora", "Motilleja", "Navas de Jorquera",
                                        "Pozo-Lorente", "Valdeganga", "Villamalea",
                                        "Villalgordo del Júcar", "Villavaliente"
                                    ]
                                },
                                {
                                    nom: "La Manchuela",
                                    tipus: "comarca",
                                    municipis: [
                                        "Abengibre", "Alatoz", "Alcalá del Júcar",
                                        "Balsa de Ves", "Carcelén", "Casas de Ves",
                                        "Villatoya", "Villa de Ves", "El Herrumblar",
                                        "Higueruela", "Hoya-Gonzalo", "Jorquera",
                                        "Pétrola", "Tobarra", "Villapalacios"
                                    ]
                                },
                                {
                                    nom: "Sierra de Alcaraz y Campo de Montiel",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alcaraz", "Balazote", "Bienservida",
                                        "Bogarra", "Casas de Lázaro", "Cotillas",
                                        "El Ballestero", "Lezuza", "Masegoso",
                                        "Peñascosa", "Povedilla", "Robledo",
                                        "Salobre", "San Pedro", "Vianos",
                                        "Villaverde de Guadalimar", "Viveros"
                                    ]
                                },
                                {
                                    nom: "Sierra del Segura",
                                    tipus: "comarca",
                                    municipis: [
                                        "Ayna", "Bogarra", "Elche de la Sierra",
                                        "Férez", "Letur", "Lietor",
                                        "Molinicos", "Nerpio", "Paterna del Madera",
                                        "Riópar", "Socovos", "Yeste"
                                    ]
                                },
                                {
                                    nom: "Campos de Hellín",
                                    tipus: "comarca",
                                    municipis: [
                                        "Hellín", "Tobarra", "Ontur",
                                        "Alborea", "Corral-Rubio", "Fuente-Álamo",
                                        "Montealegre del Castillo", "Minaya"
                                    ]
                                },
                                {
                                    nom: "Manchuela Albaceteña",
                                    tipus: "comarca",
                                    municipis: [
                                        "Almansa", "Alpera", "Bonete",
                                        "Caudete", "Fuentealbilla", "Higueruela",
                                        "Hoya-Gonzalo", "Pétrola", "Villarrobledo"
                                    ]
                                }
                            ]
                        },
                        ciudad_real: {
                            nom: "Ciudad Real",
                            comarques: [
                                {
                                    nom: "Campo de Calatrava",
                                    tipus: "comarca",
                                    municipis: [
                                        "Almagro", "Aldea del Rey", "Argamasilla de Calatrava",
                                        "Ballesteros de Calatrava", "Bolaños de Calatrava", "Carrión de Calatrava",
                                        "Cañada de Calatrava", "Corral de Calatrava", "Granátula de Calatrava",
                                        "Miguelturra", "Pozuelo de Calatrava", "Torralba de Calatrava",
                                        "Valenzuela de Calatrava", "Villar del Pozo", "Ciudad Real"
                                    ]
                                },
                                {
                                    nom: "La Mancha",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alcázar de San Juan", "Campo de Criptana", "Herencia",
                                        "Pedro Muñoz", "Puerto Lápice", "Las Labores",
                                        "Santa Cruz de los Cáñamos", "Villarta de San Juan", "Villarrubia de los Ojos",
                                        "Arenales de San Gregorio", "Daimiel", "Manzanares",
                                        "Llanos del Caudillo", "Membrilla", "La Solana",
                                        "Tomelloso", "Socuéllamos", "Alameda de Cervera"
                                    ]
                                },
                                {
                                    nom: "Montes Norte",
                                    tipus: "comarca",
                                    municipis: [
                                        "Piedrabuena", "Porzuna", "Los Cortijos",
                                        "Fontanarejo", "Horcajo de los Montes", "Las Navas de Estena",
                                        "Navalpotro", "Retuerta del Bullaque", "Alcoba",
                                        "Anchuras", "Santa Quiteria", "El Robledo"
                                    ]
                                },
                                {
                                    nom: "Sierra Morena",
                                    tipus: "comarca",
                                    municipis: [
                                        "Almadén", "Almodóvar del Campo", "Brazatortas",
                                        "Cabezarados", "Hinojosas de Calatrava", "Mestanza",
                                        "San Lorenzo de Calatrava", "Solana del Pino", "Abenójar",
                                        "Agudo", "Alamillo", "Guadalmez",
                                        "Saceruela", "Valdemanco del Esteras", "Villamayor de Calatrava"
                                    ]
                                },
                                {
                                    nom: "Valle de Alcudia",
                                    tipus: "comarca",
                                    municipis: [
                                        "Almodóvar del Campo", "Cabezarrubias del Puerto", "Fuencaliente",
                                        "Hinojosas de Calatrava", "Mestanza", "San Lorenzo de Calatrava",
                                        "Solana del Pino", "Brazatortas", "Cabezarados"
                                    ]
                                },
                                {
                                    nom: "Campo de Montiel",
                                    tipus: "comarca",
                                    municipis: [
                                        "Albaladejo", "Alhambra", "Carrizosa",
                                        "Castellar de Santiago", "Cózar", "Infantes",
                                        "Montiel", "Ossa de Montiel", "Puebla del Príncipe",
                                        "Ruidera", "San Carlos del Valle", "Santa Cruz de los Cáñamos",
                                        "Terrinches", "Torre de Juan Abad", "Villahermosa",
                                        "Villamanrique", "Villanueva de la Fuente"
                                    ]
                                },
                                {
                                    nom: "Puertollano",
                                    tipus: "comarca",
                                    municipis: [
                                        "Puertollano", "Alcolea de Calatrava", "Argamasilla de Calatrava",
                                        "Cabezarados", "Hinojosas de Calatrava", "Mestanza",
                                        "Villanueva de San Carlos", "Villar del Pozo"
                                    ]
                                }
                            ]
                        },
                        conca: {
                            nom: "Cuenca",
                            comarques: [
                                {
                                    nom: "Alcarria Conquense",
                                    tipus: "comarca",
                                    municipis: [
                                        "Albalate de las Nogueras", "Alcantud", "Alcázar del Rey", 
                                        "Alcohujate", "Almendros", "Barajas de Melo",
                                        "Belinchón", "Buenache de Alarcón", "Campillos-Paravientos",
                                        "Cañamares", "Cañaveras", "Cañaveruelas",
                                        "El Hito", "Huete", "Mondejar",
                                        "Palomares del Campo", "Paredes", "Portalrubio de Guadamejud",
                                        "Priego", "Provencio", "Salmeroncillos",
                                        "San Pedro Palmiches", "Santa María del Campo Rus", "Tresjuncos",
                                        "Valdeolivas", "Vellisca", "Villar de Domingo García",
                                        "Villar y Velasco"
                                    ]
                                },
                                {
                                    nom: "La Mancha de Conca",
                                    tipus: "comarca",
                                    municipis: [
                                        "Belmonte", "El Pedernoso", "Las Mesas",
                                        "Las Pedroñeras", "Los Hinojosos", "Mota del Cuervo",
                                        "Monreal del Llano", "Osa de la Vega", "Pedroñeras",
                                        "San Clemente", "Santa María de los Llanos", "Villaescusa de Haro",
                                        "Villamayor de Santiago", "Villar de Cañas", "Zafra de Záncara"
                                    ]
                                },
                                {
                                    nom: "Serranía Alta",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alcalá de la Vega", "Algarra", "Aliaguilla",
                                        "Arcas del Villar", "Arcos de la Sierra", "Arguisuelas",
                                        "Beamud", "Boniches", "Buenache de la Sierra",
                                        "Campillos-Sierra", "Cañada del Hoyo", "Cañete",
                                        "Carboneras de Guadazaón", "Castillejo-Sierra", "Cuenca",
                                        "Chillarón de Cuenca", "Chumillas", "Enguídanos",
                                        "Fresneda de la Sierra", "Fuentenava de Jábaga", "Huerta del Marquesado",
                                        "Huérguina", "Laguna del Marquesado", "Landete",
                                        "Minglanilla", "Narboneta", "Palomera",
                                        "Paracuellos", "Poyatos", "Salinas del Manzano",
                                        "San Martín de Boniches", "Santa Cruz de Moya", "Talayuelas",
                                        "Tejadillos", "Valdemeca", "Valdemoro-Sierra",
                                        "Valle de Altomira", "Valsalobre", "Vega del Codorno",
                                        "Villar del Humo", "Zafrilla"
                                    ]
                                },
                                {
                                    nom: "Serranía Media-Campichuelo",
                                    tipus: "comarca",
                                    municipis: [
                                        "Abia de la Obispalía", "Albaladejo del Cuende", "Almendros",
                                        "Altarejos", "Arandilla del Arroyo", "Barchín del Hoyo",
                                        "Buenache de la Sierra", "Castillejo de Iniesta", "Cervera del Llano",
                                        "Fresneda de Altarejos", "Fuentes", "Fuentelespino de Haro",
                                        "Horcajo de Santiago", "Huelves", "Huerta de la Obispalía",
                                        "Iniesta", "Ledaña", "Leganiel",
                                        "Montalbo", "Monteagudo de las Salinas", "Olivares de Júcar",
                                        "Olmeda del Rey", "Parra de las Vegas", "Piqueras del Castillo",
                                        "Pozorrubielos de la Mancha", "Pozuelo", "Puebla de Almenara",
                                        "Puebla del Salvador", "Reillo", "Rozalén del Monte",
                                        "Saelices", "Salinas del Manzano", "San Lorenzo de la Parrilla",
                                        "Santa Cruz de Moya", "Solera de Gabaldón", "Tarancón",
                                        "Torrubia del Campo", "Valhermoso de la Fuente", "Valle de Altomira",
                                        "Villar de la Encina", "Villar del Humo", "Villarejo de Fuentes",
                                        "Villarejo-Periesteban", "Villares del Saz", "Villarrubio"
                                    ]
                                },
                                {
                                    nom: "Serranía Baja",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alarcón", "Almodóvar del Pinar", "Barchín del Hoyo",
                                        "Buenache de Alarcón", "Campillo de Altobuey", "Casas de Benítez",
                                        "Casas de Guijarro", "Casasimarro", "Castillejo de Iniesta",
                                        "Castejón", "Caudete de las Fuentes", "Cervera del Llano",
                                        "Enguídanos", "Gabaldón", "Graja de Iniesta",
                                        "Henarejos", "Honrubia", "Horcajada de la Torre",
                                        "Iniesta", "Ledaña", "Minglanilla",
                                        "Mota de Altarejos", "Motilla del Palancar", "Olmedilla de Alarcón",
                                        "Paracuellos", "Piqueras del Castillo", "Pozoamargo",
                                        "Pozuelo", "Puebla de Don Fadrique", "Puebla del Salvador",
                                        "Quintanar del Rey", "Sisante", "Tébar",
                                        "Valhermoso de la Fuente", "Valverdejo", "Villar de Cañas",
                                        "Villar del Humo", "Villarejo de Fuentes", "Villares del Saz",
                                        "Villarta", "Villas Viejas"
                                    ]
                                }
                            ]
                        },
                        guadalajara: {
                            nom: "Guadalajara",
                            comarques: [
                                {
                                    nom: "La Alcarria",
                                    tipus: "comarca",
                                    municipis: [
                                        "Guadalajara", "Albalate de Zorita", "Alcocer", 
                                        "Almonacid de Zorita", "Almoguera", "Alocén",
                                        "Auñón", "Berninches", "Budia",
                                        "Chiloeches", "Cifuentes", "Driebes",
                                        "Durón", "El Olivar", "El Recuenco",
                                        "Escamilla", "Escariche", "Fuentelencina",
                                        "Fuentelviejo", "Hita", "Horche",
                                        "Hueva", "Illana", "Loranca de Tajuña",
                                        "Lupiana", "Mazuecos", "Miralrío",
                                        "Mondéjar", "Moratilla de los Meleros", "Pastrana",
                                        "Pioz", "Pozo de Almoguera", "Renera",
                                        "Romanones", "Sacedón", "Sayatón",
                                        "Tendilla", "Trillo", "Valdeconcha",
                                        "Valdegrudas", "Valfermoso de Tajuña", "Yebra",
                                        "Zaorejas", "Zorita de los Canes"
                                    ]
                                },
                                {
                                    nom: "La Campiña",
                                    tipus: "comarca",
                                    municipis: [
                                        "Azuqueca de Henares", "Alovera", "Cabanillas del Campo",
                                        "Camarma de Esteruelas", "Marchamalo", "Fontanar",
                                        "Galápagos", "Humanes", "Loeches",
                                        "Los Santos de la Humosa", "Meco", "Pezuela de las Torres",
                                        "Quer", "Ribatejada", "Talamanca de Jarama",
                                        "Torrejón del Rey", "Valdeavero", "Valdeaveruelo",
                                        "Valdeolmos-Alalpardo", "Villalbilla", "Yunquera de Henares"
                                    ]
                                },
                                {
                                    nom: "La Serranía",
                                    tipus: "comarca",
                                    municipis: [
                                        "Atienza", "Aragoncillo", "Anguita",
                                        "Arbeteta", "Argecilla", "Armallones",
                                        "Baides", "Baños de Tajo", "Canredondo",
                                        "Cifuentes", "Cogollor", "Cortes de Tajuña",
                                        "Cuevas Labradas", "Embid", "Esplegares",
                                        "Establés", "Gajanejos", "Henche",
                                        "Herrería", "Hita", "Hontoba",
                                        "Horna", "Huertahernando", "Jadraque",
                                        "Ledanca", "Luzón", "Maranchón",
                                        "Masegoso de Tajuña", "Mazarete", "Medranda",
                                        "Miedes de Atienza", "Milmarcos", "Mirabueno",
                                        "Monasterio", "Molina de Aragón", "Morenilla",
                                        "Negredo", "Ocentejo", "Olmeda de Cobeta",
                                        "Paredes de Sigüenza", "Peralveche", "Pinilla de Jadraque",
                                        "Poveda de la Sierra", "Riba de Saelices", "Rillo de Gallo",
                                        "Sacecorbo", "Saelices de la Sal", "Saúca",
                                        "Sigüenza", "Solanillos del Extremo", "Taragudo",
                                        "Tartanedo", "Tierzo", "Torre del Burgo",
                                        "Torremocha del Campo", "Torrubia", "Tortuera",
                                        "Tortuero", "Traíd", "Valhermoso",
                                        "Viana de Jadraque", "Villanueva de Alcorón", "Villares de Jadraque",
                                        "Yélamos de Abajo", "Yélamos de Arriba", "Yunquera de Henares"
                                    ]
                                },
                                {
                                    nom: "Senyoriu de Molina-Alto Tajo",
                                    tipus: "comarca",
                                    municipis: [
                                        "Ablanque", "Adobes", "Alcoroches",
                                        "Alustante", "Anguita", "Anquela del Ducado",
                                        "Anquela del Pedregal", "Arandilla", "Arbancón",
                                        "Armallones", "Azañón", "Baños de Tajo",
                                        "Canales del Ducado", "Cantalojas", "Castellar de la Muela",
                                        "Castilnuevo", "Checa", "Chequilla",
                                        "Cifuentes", "Cobeta", "Corduente",
                                        "El Cardoso de la Sierra", "El Ordial", "Establés",
                                        "Fuembellida", "Fuentelsaz", "Herrería",
                                        "Hiendelaencina", "Hombrados", "Hontoba",
                                        "Hueva", "Labros", "Luzaga",
                                        "Luzón", "Maranchón", "Mazarete",
                                        "Megina", "Milmarcos", "Molina de Aragón",
                                        "Morenilla", "Mochales", "Negredo",
                                        "Ocentejo", "Olmeda de Cobeta", "Pardos",
                                        "Peñalén", "Peralejos de las Truchas", "Piqueras",
                                        "Poveda de la Sierra", "Rillo de Gallo", "Riba de Saelices",
                                        "Selas", "Setiles", "Taragudo",
                                        "Tartanedo", "Terzaga", "Tierzo",
                                        "Tordellego", "Tordelrábano", "Torrecuadrada de Molina",
                                        "Torremocha del Pinar", "Torrubia", "Tortuera",
                                        "Tortuero", "Traíd", "Valdeavellano",
                                        "Valhermoso", "Villanueva de Alcorón", "Villar de Cobeta",
                                        "Yélamos de Abajo", "Yélamos de Arriba", "Zaorejas"
                                    ]
                                },
                                {
                                    nom: "L'Alt Henares",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alarilla", "Albolleque", "Alcolea del Pinar",
                                        "Algora", "Alhóndiga", "Almadrones",
                                        "Alocén", "Alovera", "Arbancón",
                                        "Argecilla", "Atanzón", "Brihuega",
                                        "Budia", "Cabanillas del Campo", "Campillo de Dueñas",
                                        "Canredondo", "Casar de Talamanca", "Casas de San Galindo",
                                        "Castilforte", "Cerezo de Mohernando", "Ciruelas",
                                        "Cogolludo", "Copernal", "Driebes",
                                        "Espinosa de Henares", "Fuentelahiguera de Albatages", "Fuentelencina",
                                        "Fuentelsaz", "Galápagos", "Gascueña de Bornova",
                                        "Guadalajara", "Hita", "Horche",
                                        "Hortezuela de Océn", "Hueva", "Humanes",
                                        "Jadraque", "Ledanca", "Lupiana",
                                        "Luzón", "Malaguilla", "Mandayona",
                                        "Marchamalo", "Masegoso de Tajuña", "Matarrubia",
                                        "Matillas", "Mazuecos", "Medranda",
                                        "Miralrío", "Mohernando", "Muduex",
                                        "Navalpotro", "Pálmaces de Jadraque", "Pioz",
                                        "Pozo de Guadalajara", "Quer", "Renera",
                                        "Retiendas", "Riba de Saelices", "Romanones",
                                        "Sacedón", "Saelices de la Sal", "Salmerón",
                                        "San Andrés del Rey", "Santiuste", "Sayatón",
                                        "Tajueco", "Taracena", "Tartanedo",
                                        "Tendilla", "Tierzo", "Torija",
                                        "Torre del Burgo", "Torrecuadrada de Molina", "Torremocha del Campo",
                                        "Tórtola de Henares", "Valdearenas", "Valdeavellano",
                                        "Valdegrudas", "Valfermoso de Tajuña", "Villanueva de la Torre",
                                        "Villares de Jadraque", "Yebes", "Yunquera de Henares",
                                        "Zaorejas", "Zorita de los Canes"
                                    ]
                                }
                            ]
                        },
                        toledo: {
                            nom: "Toledo",
                            comarques: [
                                {
                                    nom: "La Sagra",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alameda de la Sagra", "Añover de Tajo", "Arcicóllar", "Bargas",
                                        "Borox", "Cabañas de Yepes", "Carranque", "Casarrubios del Monte",
                                        "Cobeja", "Chozas de Canales", "Esquivias", "Fuensalida", "Gerindote",
                                        "Illescas", "Lominchar", "Magán", "Mocejón", "Numancia de la Sagra",
                                        "Ocaña", "Olías del Rey", "Palomeque", "Pantoja", "Recas",
                                        "Seseña", "Torrejón de la Calzada", "Ugena", "Valdemoro",
                                        "Villaluenga de la Sagra", "Villaseca de la Sagra", "Yeles",
                                        "Yepes", "Yuncler", "Yunclillos"
                                    ]
                                },
                                {
                                    nom: "Mesa de Ocaña",
                                    tipus: "comarca",
                                    municipis: [
                                        "Cabañas de Yepes", "Ciruelos", "Corral de Almaguer", "Dosbarrios",
                                        "La Guardia", "Noblejas", "Ocaña", "Ontígola", "Santa Cruz de la Zarza",
                                        "Villatobas"
                                    ]
                                },
                                {
                                    nom: "Montes de Toledo",
                                    tipus: "comarca",
                                    municipis: [
                                        "Ajofrín", "Almonacid de Toledo", "Arges", "Burguillos de Toledo",
                                        "Casasbuenas", "Cobisa", "Chueca", "Guadamur", "Hontanar",
                                        "Las Ventas con Peña Aguilera", "Los Navalucillos", "Lucillos", "Mascaraque",
                                        "Mazarambroz", "Menasalbas", "Nambroca", "Noez", "Polán",
                                        "Pulgar", "San Martín de Montalbán", "Sonseca", "Totanés",
                                        "Turleque", "Ventosa", "Villaminaya"
                                    ]
                                },
                                {
                                    nom: "La Jara",
                                    tipus: "comarca",
                                    municipis: [
                                        "Aldeanueva de Barbarroya", "Azután", "Belvís de la Jara", "Calera y Chozas",
                                        "Campillo de la Jara", "El Puente del Arzobispo", "Espinoso del Rey", "Garciotum",
                                        "La Nava de Ricomalillo", "Las Herencias", "Los Navalmorales", "Menasalbas",
                                        "Mohedas de la Jara", "Navalcán", "Navalucillos", "Puerto de San Vicente",
                                        "Retamoso de la Jara", "San Bartolomé de las Abiertas", "San Martín de Pusa",
                                        "Sevilleja de la Jara", "Torrecilla de la Jara", "Villanueva de Bogas",
                                        "Villarejo de Montalbán"
                                    ]
                                },
                                {
                                    nom: "Sierra de San Vicente",
                                    tipus: "comarca",
                                    municipis: [
                                        "Almendral de la Cañada", "Cardiel de los Montes", "Castillo de Bayuela",
                                        "Cazalegas", "Cerro Gordo", "Espinoso del Rey", "Garciotum",
                                        "Hinojosa de San Vicente", "Hormigos", "Iglesuela del Tiétar", "La Iglesuela",
                                        "Marrupe", "Navamorcuende", "Nuño Gómez", "Pelahustán",
                                        "Pelahustán", "El Real de San Vicente", "San Román de los Montes",
                                        "Sartajada"
                                    ]
                                },
                                {
                                    nom: "Torrijos",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alcabón", "Alcañizo", "Almorox", "Arcicóllar", "Barcience",
                                        "Carmena", "Carriches", "Domingo Pérez", "Erustes", "Escalonilla",
                                        "Fuensalida", "Gerindote", "Hinojosa de San Vicente", "Hormigos",
                                        "Huecas", "Maqueda", "Méntrida", "Nombela", "Novés",
                                        "Otero", "Portillo de Toledo", "Puebla de Montalbán", "Quismondo",
                                        "Santa Olalla", "Santo Domingo-Caudilla", "Torrijos", "Val de Santo Domingo",
                                        "Valmojado", "Ventas de Retamosa", "Villamiel de Toledo"
                                    ]
                                },
                                {
                                    nom: "Mancha Alta de Toledo",
                                    tipus: "comarca",
                                    municipis: [
                                        "Cabezamesada", "Corral de Almaguer", "Huerta de Valdecarábanos", "La Guardia",
                                        "Lillo", "Los Yébenes", "Mora", "Ocaña", "Ontígola",
                                        "Quero", "Tembleque", "Villacañas", "Villafranca de los Caballeros"
                                    ]
                                },
                                {
                                    nom: "Mancha de Toledo",
                                    tipus: "comarca",
                                    municipis: [
                                        "Consuegra", "Herencia", "Madridejos", "Puerto Lápice",
                                        "Urda", "Villacañas"
                                    ]
                                },
                                {
                                    nom: "Talavera",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alcaudete de la Jara", "Alcolea de Tajo", "Aldeanueva de Barbarroya", "Aldeanueva de San Bartolomé",
                                        "Almendra", "Azután", "Belvís de la Jara", "Buenaventura", "Calera y Chozas",
                                        "Caleruela", "Campillo de la Jara", "Cardiel de los Montes", "Casar de Escalona",
                                        "Castillo de Bayuela", "Cazalegas", "Cebolla", "Cervera de los Montes",
                                        "El Puente del Arzobispo", "El Real de San Vicente", "Espinoso del Rey", "Gargantilla",
                                        "Guadamur", "Hinojosa de San Vicente", "Hontanar", "Hormigos",
                                        "Iglesuela del Tiétar", "La Iglesuela", "La Nava de Ricomalillo", "Las Herencias",
                                        "Las Ventas de Retamosa", "Las Ventas con Peña Aguilera", "Los Navalmorales", "Lucillos",
                                        "Malpica de Tajo", "Marrupe", "Méntrida", "Mesegar de Tajo",
                                        "Montesclaros", "Navalcán", "Navalmoralejo", "Navalucillos", "Navamorcuende",
                                        "Otero", "Parrillas", "Pelahustán", "Pepino",
                                        "Puebla de Montalbán", "Puente del Arzobispo", "Puerto de San Vicente", "Retamoso de la Jara",
                                        "San Bartolomé de las Abiertas", "San Martín de Pusa", "San Román de los Montes", "Santa Ana de Pusa",
                                        "Sartajada", "Segurilla", "Sevilleja de la Jara", "Sotillo de las Palomas",
                                        "Talavera de la Reina", "Tembleque", "Tietar", "Torralba de Oropesa",
                                        "Torrecilla de la Jara", "Torrico", "Valverde de Burguillos", "Velada",
                                        "Villanueva de Alcardete", "Villanueva de Bogas", "Villarejo de Montalbán"
                                    ]
                                }
                            ]
                        }
                    }

                },
                catalunya: {
                    nom: "Catalunya",
                    columnes: ["Pais", "Comunitat Autonoma", "Provincia", "Comarca", "Poble", "Registres", "Opcions"],
                    provincies: {
                        barcelona: {
                            nom: "Barcelona",
                            comarques: [
                                {
                                    nom: "Alt Camp",
                                    tipus: "comarca",
                                    municipis: [
                                        "Aiguamúrcia", "Alcover", "Alió", "Bràfim", "Cabra del Camp",
                                        "Figuerola del Camp", "Els Garidells", "La Masó", "El Milà",
                                        "Montferri", "Nulles", "El Pla de Santa Maria", "Puigpelat",
                                        "Querol", "La Riba", "Rodonyà", "El Rourell", "Vallmoll",
                                        "Valls", "Vila-rodona"
                                        ]
                                },
                                {
                                    nom: "Anoia",
                                    tipus: "comarca",
                                    municipis: [
                                        "Argençola", "Bellprat", "El Bruc", "Cabrera d'Igualada", "Calaf",
                                        "Calonge de Segarra", "Capellades", "Carme", "Castellfollit de Riubregós",
                                        "Castellolí", "Copons", "Els Hostalets de Pierola", "Igualada", "Jorba",
                                        "La Llacuna", "Masquefa", "Montmaneu", "Òdena", "Orpí",
                                        "Piera", "La Pobla de Claramunt", "Els Prats de Rei", "Puigdalber",
                                        "Rubió", "Sant Martí Sesgueioles", "Sant Pere Sallavinera", "Santa Margarida de Montbui",
                                        "Santa Maria de Miralles", "La Torre de Claramunt", "Vallbona d'Anoia"
                                        ]
                                },
                                {
                                    nom: "Bages",
                                    tipus: "comarca",
                                    municipis: [
                                        "Aguilar de Segarra", "Artés", "Avinyó", "Balsareny", "Calders",
                                        "Callús", "Cardona", "Castellbell i el Vilar", "Castellgalí", "Castellnou de Bages",
                                        "L'Estany", "Fonollosa", "Gaià", "Manresa", "Marganell",
                                        "Monistrol de Calders", "Monistrol de Montserrat", "Mura", "Navarcles",
                                        "Navàs", "Rajadell", "Sallent", "Sant Feliu Sasserra", "Sant Fruitós de Bages",
                                        "Sant Joan de Vilatorrada", "Sant Mateu de Bages", "Sant Vicenç de Castellet", "Santa Maria d'Oló",
                                        "Santpedor", "Súria", "Talamanca"
                                        ]
                                },
                                {
                                    nom: "Baix Camp",
                                    tipus: "comarca",
                                    municipis: [
                                        "L'Aleixar", "Alforja", "Almoster", "Arbolí", "L'Argentera",
                                        "Les Borges del Camp", "Botarell", "Cambrils", "Capçanes", "Castelldans",
                                        "Castellvell del Camp", "Colldejou", "Duesaigües", "Falset", "Figuerola del Camp",
                                        "Mas de Barberans", "Masllorenç", "Montbrió del Camp", "Mont-roig del Camp", "Pratdip",
                                        "Reus", "Riudecanyes", "Riudecols", "Riudoms", "La Selva del Camp",
                                        "Tivissa", "Torredembarra", "Vandellòs i l'Hospitalet de l'Infant", "Vilanova d'Escornalbou", "Vilaplana",
                                        "Vinyols i els Arcs"
                                        ]
                                },
                                {
                                    nom: "Baix Llobregat",
                                    tipus: "comarca",
                                    municipis: [
                                        "Abrera", "Begues", "Castelldefels", "Castellví de Rosanes", "Cervelló",
                                        "Collbató", "Corbera de Llobregat", "Cornellà de Llobregat", "Esparreguera", "Esplugues de Llobregat",
                                        "Gavà", "Martorell", "Molins de Rei", "Olesa de Montserrat", "Pallejà",
                                        "El Papiol", "Prat de Llobregat", "Sant Andreu de la Barca", "Sant Boi de Llobregat", "Sant Climent de Llobregat",
                                        "Sant Esteve Sesrovires", "Sant Feliu de Llobregat", "Sant Joan Despí", "Sant Just Desvern", "Sant Vicenç dels Horts",
                                        "Santa Coloma de Cervelló", "Torrelles de Llobregat", "Vallirana"
                                        ]
                                },
                                {
                                    nom: "Barcelonès",
                                    tipus: "comarca",
                                    municipis: [
                                        "Badalona", "Barcelona", "L'Hospitalet de Llobregat", "Sant Adrià de Besòs",
                                        "Santa Coloma de Gramenet"
                                        ]
                                },
                                {
                                    nom: "Berguedà",
                                    tipus: "comarca",
                                    municipis: [
                                        "Avià", "Bagà", "Berga", "Borredà", "Capolat",
                                        "Casserres", "Castell de l'Areny", "Castellar de N'Hug", "Castellar del Riu", "Cercs",
                                        "L'Espunyola", "Fígols", "Gironella", "Gisclareny", "Gósol",
                                        "Guardiola de Berguedà", "Montclar", "Montmajor", "La Nou de Berguedà", "Olvan",
                                        "La Pobla de Lillet", "Puig-reig", "Queralbs", "Saldes", "Sant Jaume de Frontanyà",
                                        "Sant Julià de Cerdanyola", "Santa Maria de Merlès", "Vallcebre", "Vilada", "Viver i Serrateix"
                                        ]
                                },
                                {
                                    nom: "Garraf",
                                    tipus: "comarca",
                                    municipis: [
                                        "Canyelles", "Cubelles", "Olivella", "Sant Pere de Ribes",
                                        "Sitges", "Vilanova i la Geltrú"
                                        ]
                                },
                                {
                                    nom: "Maresme",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alella", "Argentona", "Badalona", "Cabrera de Mar", "Cabrils",
                                        "Calella", "Canet de Mar", "Dosrius", "Malgrat de Mar", "El Masnou",
                                        "Mataró", "Montgat", "Òrrius", "Palafolls", "Pineda de Mar",
                                        "Premià de Dalt", "Premià de Mar", "Sant Andreu de Llavaneres", "Sant Cebrià de Vallalta", "Sant Iscle de Vallalta",
                                        "Sant Pol de Mar", "Sant Vicenç de Montalt", "Santa Susanna", "Teià",
                                        "Tiana", "Tordera", "Vilassar de Dalt", "Vilassar de Mar"
                                        ]
                                },
                                {
                                    nom: "Osona",
                                    tipus: "comarca",
                                    municipis: [
                                        "Aiguafreda", "Balenyà", "El Brull", "Calldetenes", "Centelles",
                                        "Espinelves", "Folgueroles", "Gurb", "Lluçà", "Malla",
                                        "Manlleu", "Les Masies de Roda", "Les Masies de Voltregà", "Montesquiu", "Muntanyola",
                                        "Olost", "Orís", "Oristà", "Perafita", "Prats de Lluçanès",
                                        "Roda de Ter", "Rupit i Pruit", "Sant Agustí de Lluçanès", "Sant Bartomeu del Grau", "Sant Boi de Lluçanès",
                                        "Sant Hipòlit de Voltregà", "Sant Julià de Vilatorta", "Sant Martí d'Albars", "Sant Martí de Centelles", "Sant Pere de Torelló",
                                        "Sant Quirze de Besora", "Sant Sadurní d'Osormort", "Sant Vicenç de Torelló", "Santa Cecília de Voltregà", "Santa Eugènia de Berga",
                                        "Santa Eulàlia de Riuprimer", "Santa Maria de Besora", "Santa Maria de Corcó", "Seva", "Sobremunt",
                                        "Sora", "Taradell", "Tavèrnoles", "Tavertet", "Tona",
                                        "Vic", "Vidrà", "Viladrau"
                                    ]
                                },
                                {
                                    nom: "Pallars Sobirà",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alins", "Alt Àneu", "Baix Pallars", "Espot", "Esterri d'Àneu",
                                        "Esterri de Cardós", "Farrera", "La Guingueta d'Àneu", "Lladorre", "Llavorsí",
                                        "Rialp", "Soriguera", "Sort", "Tírvia", "Vall de Cardós"
                                    ]
                                },
                                {
                                    nom: "Penedès",
                                    tipus: "comarca",
                                    municipis: [
                                        "Avinyonet del Penedès", "Castellet i la Gornal", "Castellví de la Marca", "Font-rubí",
                                        "Gelida", "La Granada", "Mediona", "Olèrdola", "Pacs del Penedès",
                                        "Puigdàlber", "Sant Cugat Sesgarrigues", "Sant Llorenç d'Hortons", "Sant Martí Sarroca", "Sant Pere de Riudebitlles",
                                        "Sant Quintí de Mediona", "Sant Sadurní d'Anoia", "Santa Fe del Penedès", "Santa Margarida i els Monjos", "Subirats",
                                        "Torrelavit", "Torrelles de Foix", "Vilafranca del Penedès"
                                        ]
                                },
                                {
                                    nom: "Segarra",
                                    tipus: "comarca",
                                    municipis: [
                                        "Biosca", "Cervera", "Estaràs", "Granyanella", "Granyena de Segarra",
                                        "Guissona", "Ivorra", "Massoteres", "Montoliu de Segarra", "Montornès de Segarra",
                                        "Les Oluges", "Els Plans de Sió", "Ribera d'Ondara", "Sanaüja", "Sant Guim de Freixenet",
                                        "Sant Guim de la Plana", "Tora"
                                        ]
                                },
                                {
                                    nom: "Solsonès",
                                    tipus: "comarca",
                                    municipis: [
                                        "Castellar de la Ribera", "Clariana de Cardener", "La Coma i la Pedra", "Guixers",
                                        "Lladurs", "Llobera", "La Molsosa", "Navès", "Odèn",
                                        "Olius", "Pinell de Solsonès", "Riner", "Sant Llorenç de Morunys", "Solsona"
                                        ]
                                },
                                {
                                    nom: "Vallès Occidental",
                                    tipus: "comarca",
                                    municipis: [
                                        "Badia del Vallès", "Barberà del Vallès", "Castellar del Vallès", "Castellbisbal",
                                        "Cerdanyola del Vallès", "Gallifa", "Matadepera", "Montcada i Reixac", "Palau-solità i Plegamans",
                                        "Polinyà", "Ripollet", "Rubí", "Sabadell", "Sant Cugat del Vallès",
                                        "Sant Llorenç Savall", "Sant Quirze del Vallès", "Santa Perpètua de Mogoda", "Sentmenat", "Terrassa",
                                        "Ullastrell", "Vacarisses", "Viladecavalls"
                                        ]
                                },
                                {
                                    nom: "Vallès Oriental",
                                    tipus: "comarca",
                                    municipis: [
                                        "Aiguafreda", "Bigues i Riells del Fai", "Caldes de Montbui", "Campins",
                                        "Canovelles", "Cànoves i Samalús", "Cardedeu", "Castellcir", "Castellterçol",
                                        "Figaró", "Fogars de Montclús", "Les Franqueses del Vallès", "La Garriga", "Granollers",
                                        "Gualba", "L'Ametlla del Vallès", "La Llagosta", "Llinars del Vallès", "Lliçà d'Amunt",
                                        "Lliçà de Vall", "Martorelles", "Mollet del Vallès", "Montmeló", "Montornès del Vallès",
                                        "Parets del Vallès", "Sant Antoni de Vilamajor", "Sant Celoni", "Sant Esteve de Palautordera", "Sant Feliu de Codines",
                                        "Sant Fost de Campsentelles", "Sant Iscle i Sant Victorià", "Sant Martí de Centelles", "Sant Martí de Tous",
                                        "Sant Pere de Vilamajor", "Santa Eulàlia de Ronçana", "Santa Maria de Martorelles", "Santa Maria de Palautordera", "Tagamanent",
                                        "Vallgorguina", "Vallromanes", "Vilalba Sasserra", "Vilanova del Vallès"
                                        ]
                                }
                            ]
                        },
                        girona: {
                            nom: "Girona",
                            comarques: [
                                {
                                    nom: "Alt Empordà",
                                    tipus: "comarca",
                                    municipis: [
                                        "Agullana", "Albanyà", "L'Armentera", "Avinyonet de Puigventós", "Bàscara",
                                        "Biure", "Boadella i les Escaules", "Borrassà", "Breda", "Cabanes",
                                        "Cadaqués", "Cantallops", "Capmany", "Castelló d'Empúries", "Cistella",
                                        "Colera", "Darnius", "L'Escala", "Espolla", "Figueres",
                                        "Fortià", "Garriguella", "La Jonquera", "Llançà", "Llers",
                                        "Maçanet de Cabrenys", "Masarac", "Mollet de Peralada", "Navata", "Ordis",
                                        "Palau de Santa Eulàlia", "Palau-saverdera", "Pau", "Pedret i Marzà", "La Pera",
                                        "Peralada", "Pont de Molins", "Rabós", "Riumors", "Roses",
                                        "Sant Climent Sescebes", "Sant Llorenç de la Muga", "Sant Miquel de Fluvià", "Sant Mori", "Sant Pere Pescador",
                                        "Santa Llogaia d'Àlguema", "Saus, Camallera i Llampaies", "La Selva de Mar", "Siurana", "Terrades",
                                        "Torroella de Fluvià", "Vajol", "Ventalló", "Vilabertran", "Viladamat",
                                        "Vilafant", "Vilajuïga"
                                    ]
                                },
                                {
                                    nom: "Baix Empordà",
                                    tipus: "comarca",
                                    municipis: [
                                        "Albons", "Begur", "Bellcaire d'Empordà", "Bisbal d'Empordà", "Calonge i Sant Antoni",
                                        "Castell-Platja d'Aro", "Colomers", "Corçà", "Cruïlles, Monells i Sant Sadurní de l'Heura", "Foixà",
                                        "Fontanilles", "Forallac", "Garrigoles", "Gualta", "Jafre",
                                        "Mont-ras", "Palafrugell", "Palamós", "Palau-sator", "Pals",
                                        "Parlavà", "Regencós", "Rupià", "Sant Feliu de Boada", "Sant Jordi Desvalls",
                                        "Sant Joan de Montdarn", "Sant Julià de Boada", "Sant Martí Vell", "Santa Cristina d'Aro", "Serra de Daró",
                                        "Tallada d'Empordà", "Torrent", "Torroella de Montgrí", "Ullà", "Ullastret",
                                        "Ultramort", "Vall-llobrega", "Verges"
                                    ]
                                },
                                {
                                    nom: "Garrotxa",
                                    tipus: "comarca",
                                    municipis: [
                                        "Argelaguer", "Besalú", "Beuda", "Castellfollit de la Roca", "Les Planes d'Hostoles",
                                        "Les Preses", "Maià de Montcal", "Mieres", "Montagut i Oix", "Olot",
                                        "Planes d'Hostoles", "Riudaura", "Sales de Llierca", "Sant Aniol de Finestres", "Sant Feliu de Pallerols",
                                        "Sant Ferriol", "Sant Joan les Fonts", "Santa Pau", "Tortellà", "La Vall de Bianya",
                                        "La Vall d'en Bas"
                                    ]
                                },
                                {
                                    nom: "Gironès",
                                    tipus: "comarca",
                                    municipis: [
                                        "Bescanó", "Bordils", "Celrà", "Cervià de Ter", "Flaçà",
                                        "Girona", "Juià", "Llagostera", "Llambilles", "Madremanya",
                                        "Quart", "Salt", "Sant Gregori", "Sant Joan de Mollet", "Sant Jordi Desvalls",
                                        "Sant Julià de Ramis", "Sarrià de Ter", "Vilablareix"
                                    ]
                                },
                                {
                                    nom: "Pla de l'Estany",
                                    tipus: "comarca",
                                    municipis: [
                                        "Banyoles", "Camós", "Cornellà del Terri", "Crespià", "Esponellà",
                                        "Fontcoberta", "Palol de Revardit", "Porqueres", "Sant Miquel de Campmajor", "Serinyà",
                                        "Vilademuls"
                                    ]
                                },
                                {
                                    nom: "Ripollès",
                                    tipus: "comarca",
                                    municipis: [
                                        "Campdevànol", "Campelles", "Camprodon", "Gombrèn", "Llanars",
                                        "Les Llosses", "Molló", "Ogassa", "Pardines", "Planoles",
                                        "Queralbs", "Ribes de Freser", "Ripoll", "Sant Joan de les Abadesses", "Sant Pau de Segúries",
                                        "Setcases", "Toses", "Vallfogona de Ripollès", "Vidrà", "Vilallonga de Ter"
                                    ]
                                },
                                {
                                    nom: "Selva",
                                    tipus: "comarca",
                                    municipis: [
                                        "Amer", "Anglès", "Arbúcies", "Blanes", "Breda",
                                        "Brunyola i Sant Martí Sapresa", "Caldes de Malavella", "Cellera de Ter", "Fogars de la Selva", "Hostalric",
                                        "Lloret de Mar", "Massanes", "Massanet de la Selva", "Osor", "Riells i Viabrea",
                                        "Riudarenes", "Riudellots de la Selva", "Sant Feliu de Buixalleu", "Sant Hilari Sacalm", "Sant Julià del Llor i Bonmatí",
                                        "Santa Coloma de Farners", "Sils", "Susqueda", "Tossa de Mar", "Vidreres",
                                        "Vilobí d'Onyar"
                                    ]
                                }
                            ]
                        },
                        lleida: {
                            nom: "Lleida",
                            comarques: [
                                {
                                    nom: "Alta Ribagorça",
                                    tipus: "comarca",
                                    municipis: [
                                        "El Pont de Suert", "La Vall de Boí"
                                    ]
                                },
                                {
                                    nom: "Alt Urgell",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alàs i Cerc", "Arsèguel", "Bassella", "Cava", "Coll de Nargó",
                                        "Estamariu", "Fígols i Alinyà", "Josa i Tuixén", "Montferrer i Castellbò", "Oliana",
                                        "Organyà", "Peramola", "El Pla de Sant Tirs", "Ribera d'Urgellet", "Seu d'Urgell",
                                        "Les Valls d'Aguilar", "Les Valls de Valira"
                                    ]
                                },
                                {
                                    nom: "Baix Cerdanya",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alp", "Baga", "Bellver de Cerdanya", "Bolvir", "Das",
                                        "Ger", "Guils de Cerdanya", "Isòvol", "Lles de Cerdanya", "Martinet",
                                        "Meranges", "Prats i Sansor", "Prullans", "Puigcerdà", "Riu de Cerdanya",
                                        "Urús"
                                    ]
                                },
                                {
                                    nom: "Garrigues",
                                    tipus: "comarca",
                                    municipis: [
                                        "L'Albi", "Arbeca", "Bellaguarda", "Les Borges Blanques", "Bovera",
                                        "Castelldans", "Cervià de les Garrigues", "El Cogul", "L'Espluga Calba", "Floresta",
                                        "Fulleda", "Granyena de les Garrigues", "Juncosa", "Margalef", "Els Omellons",
                                        "La Pobla de Cérvoles", "Puiggròs", "El Soleràs", "Tarrés", "Els Torms",
                                        "Vinaixa"
                                    ]
                                },
                                {
                                    nom: "Noguera",
                                    tipus: "comarca",
                                    municipis: [
                                        "Àger", "Albesa", "Algerri", "Alòs de Balaguer", "Artesa de Segre",
                                        "Balaguer", "Baronia de Rialb", "Bellcaire d'Urgell", "Bellmunt d'Urgell", "Benavent de Tremp",
                                        "Cabanabona", "Camarasa", "Castelló de Farfanya", "Cubells", "Foradada",
                                        "Ivars de Noguera", "Menàrguens", "Montgai", "Oliola", "Os de Balaguer",
                                        "Penelles", "Ponts", "Preixens", "La Sentiu de Sió", "Tiurana",
                                        "Torrelameu", "Tornabous", "Vallfogona de Balaguer", "Vilanova de Meià"
                                    ]
                                },
                                {
                                    nom: "Pallars Jussà",
                                    tipus: "comarca",
                                    municipis: [
                                        "Abella de la Conca", "Castelldans", "Conca de Dalt", "Gavet de la Conca", "Isona i Conca Dellà",
                                        "Llimiana", "La Pobla de Segur", "Salàs de Pallars", "Sant Esteve de la Sarga", "Senterada",
                                        "Talarn", "La Torre de Cabdella", "Tremp"
                                    ]
                                },
                                {
                                    nom: "Pla d'Urgell",
                                    tipus: "comarca",
                                    municipis: [
                                        "Barbens", "Bell-lloc d'Urgell", "Bellvís", "Castelldans", "Fondarella",
                                        "Golmés", "Ivars d'Urgell", "Linyola", "Miralcamp", "Mollerussa",
                                        "El Palau d'Anglesola", "Poal", "Sidamon", "Torregrossa", "Vilanova de Bellpuig",
                                        "Vila-sana"
                                    ]
                                },
                                {
                                    nom: "Segarra",
                                    tipus: "comarca",
                                    municipis: [
                                        "Biosca", "Cervera", "Estaràs", "Granyanella", "Granyena de Segarra",
                                        "Guissona", "Ivorra", "Massoteres", "Montoliu de Segarra", "Montornès de Segarra",
                                        "Les Oluges", "Els Plans de Sió", "Ribera d'Ondara", "Sanaüja", "Sant Guim de Freixenet",
                                        "Sant Guim de la Plana", "Tora"
                                    ]
                                },
                                {
                                    nom: "Segrià",
                                    tipus: "comarca",
                                    municipis: [
                                        "Aitona", "Els Alamús", "Albatàrrec", "Alcarràs", "Alcoletge",
                                        "Alfarràs", "Alfés", "Alguaire", "Almacelles", "Almatret",
                                        "Alpicat", "Artesa de Lleida", "Aspa", "Benavent de Segrià", "Corbins",
                                        "Gimenells i el Pla de la Font", "Granja d'Escarp", "Lleida", "Llardecans",
                                        "Maials", "Massalcoreig", "Montoliu de Lleida", "La Portella", "Puigverd de Lleida",
                                        "Rosselló", "Sarroca de Lleida", "Seròs", "Soses", "Sutxelles",
                                        "Torrebesses", "Torrefarrera", "Torres de Segre", "Torreserona", "Vilanova de la Barca"
                                    ]
                                },
                                {
                                    nom: "Solsonès",
                                    tipus: "comarca",
                                    municipis: [
                                        "Castellar de la Ribera", "Clariana de Cardener", "La Coma i la Pedra", "Guixers",
                                        "Lladurs", "Llobera", "La Molsosa", "Navès", "Odèn",
                                        "Olius", "Pinell de Solsonès", "Riner", "Sant Llorenç de Morunys", "Solsona"
                                    ]
                                },
                                {
                                    nom: "Urgell",
                                    tipus: "comarca",
                                    municipis: [
                                        "Agramunt", "Anglesola", "Belianes", "Bellpuig", "Castellserà",
                                        "Ciutadilla", "La Fuliola", "Guimerà", "Maldà", "Nalec",
                                        "Preixana", "Puigverd d'Agramunt", "Sant Martí de Riucorb", "Tàrrega", "Tornabous",
                                        "Vallbona de les Monges", "Verdú", "Vila-rodona"
                                    ]
                                },
                                {
                                    nom: "Val d'Aran",
                                    tipus: "comarca",
                                    municipis: [
                                        "Arres", "Bausen", "Bossòst", "Canejan", "Les",
                                        "Naut Aran", "Vielha e Mijaran"
                                    ]
                                },
                                {
                                    nom: "Pallars Sobirà",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alins", "Alt Àneu", "Baix Pallars", "Espot", "Esterri d'Àneu",
                                        "Esterri de Cardós", "Farrera", "La Guingueta d'Àneu", "Lladorre", "Llavorsí",
                                        "Rialp", "Soriguera", "Sort", "Tírvia", "Vall de Cardós"
                                    ]
                                }
                            ]
                        },
                        tarragona: {
                            nom: "Tarragona",
                            comarques: [
                                {
                                    nom: "Alt Camp",
                                    tipus: "comarca",
                                    municipis: [
                                        "Aiguamúrcia", "Alcover", "Alió", "Bràfim", "Cabra del Camp",
                                        "Figuerola del Camp", "Els Garidells", "La Masó", "El Milà",
                                        "Montferri", "Nulles", "El Pla de Santa Maria", "Puigpelat",
                                        "Querol", "La Riba", "Rodonyà", "El Rourell", "Vallmoll",
                                        "Valls", "Vila-rodona"
                                    ]
                                },
                                {
                                    nom: "Baix Camp",
                                    tipus: "comarca",
                                    municipis: [
                                        "L'Aleixar", "Alforja", "Almoster", "Arbolí", "L'Argentera",
                                        "Les Borges del Camp", "Botarell", "Cambrils", "Capçanes", "Castelldans",
                                        "Castellvell del Camp", "Colldejou", "Duesaigües", "Falset", "Figuerola del Camp",
                                        "Mas de Barberans", "Masllorenç", "Montbrió del Camp", "Mont-roig del Camp", "Pratdip",
                                        "Reus", "Riudecanyes", "Riudecols", "Riudoms", "La Selva del Camp",
                                        "Tivissa", "Torredembarra", "Vandellòs i l'Hospitalet de l'Infant", "Vilanova d'Escornalbou", "Vilaplana",
                                        "Vinyols i els Arcs"
                                    ]
                                },
                                {
                                    nom: "Baix Ebre",
                                    tipus: "comarca",
                                    municipis: [
                                        "L'Aldea", "L'Ametlla de Mar", "L'Ampolla", "Benifallet", "Camarles",
                                        "Deltebre", "Paüls", "El Perelló", "Roquetes", "Tivenys",
                                        "Tortosa", "Xerta"
                                    ]
                                },
                                {
                                    nom: "Baix Penedès",
                                    tipus: "comarca",
                                    municipis: [
                                        "L'Arboç", "Banyeres del Penedès", "Bellvei", "La Bisbal del Penedès", "Bonastre",
                                        "Calafell", "Cunit", "Llorenç del Penedès", "Masllorenç", "El Montmell",
                                        "Sant Jaume dels Domenys", "Santa Oliva"
                                    ]
                                },
                                {
                                    nom: "Conca de Barberà",
                                    tipus: "comarca",
                                    municipis: [
                                        "Barberà de la Conca", "Blancafort", "Conesa", "L'Espluga de Francolí", "Forès",
                                        "Llorac", "Montblanc", "Passanant i Belltall", "Pira", "Pontils",
                                        "Rocafort de Queralt", "Santa Coloma de Queralt", "Sarral", "Senan",
                                        "Solivella", "Vallclara", "Vilanova de Prades", "Vilaverd"
                                    ]
                                },
                                {
                                    nom: "Montsià",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alcanar", "Amposta", "Benicarló", "Càlig", "Canet lo Roig",
                                        "Castell de Cabres", "La Jana", "Mas de Barberans", "Peníscola", "Rossell",
                                        "Sant Carles de la Ràpita", "Sant Jordi", "Sant Mateu", "Santa Magdalena de Polpís",
                                        "Traiguera", "Ulldecona", "Vinaròs"
                                    ]
                                },
                                {
                                    nom: "Priorat",
                                    tipus: "comarca",
                                    municipis: [
                                        "Bellmunt del Priorat", "La Bisbal de Falset", "Cabacés", "Capçanes",
                                        "Cornudella de Montsant", "Falset", "Gratallops", "Lloar", "Margalef",
                                        "Marçà", "Molar", "La Morera de Montsant", "Poboleda", "Porrera",
                                        "Pradell de la Teixeta", "La Torre de Fontaubella", "Torroja del Priorat", "Ulldemolins",
                                        "La Vilella Alta", "La Vilella Baixa"
                                    ]
                                },
                                {
                                    nom: "Ribera d'Ebre",
                                    tipus: "comarca",
                                    municipis: [
                                        "Ascó", "Benissanet", "Benissanet", "Flix", "Garcia",
                                        "Ginestar", "Miravet", "Móra d'Ebre", "Móra la Nova", "La Palma d'Ebre",
                                        "Rasquera", "Tivissa", "Torre de l'Espanyol"
                                    ]
                                },
                                {
                                    nom: "Tarragonès",
                                    tipus: "comarca",
                                    municipis: [
                                        "Altafulla", "Banyeres del Penedès", "La Canonja", "Catllar", "Constantí",
                                        "Creixell", "El Morell", "Nou de Gaià", "Els Pallaresos", "Perafort",
                                        "La Pobla de Mafumet", "La Riera de Gaià", "Salomó", "Salou",
                                        "Secuita", "Tarragona", "Torredembarra", "Vespella de Gaià", "Vila-seca"
                                    ]
                                },
                                {
                                    nom: "Terra Alta",
                                    tipus: "comarca",
                                    municipis: [
                                        "Arnes", "Batea", "Bot", "Corbera d'Ebre", "Fatarella",
                                        "Gandesa", "Horta de Sant Joan", "Pinell de Brai"
                                    ]
                                }
                            ]
                        }
                    }
                },
                valencia: {
                    nom: "Valencia",
                    columnes: ["Pais", "Comunitat Autonoma", "Provincia", "Comarca", "Poble", "Registres", "Opcions"],
                    provincies: {
                        castello: {
                            nom: "Castelló",
                            comarques: [
                                {
                                    nom: "Alcalatén",
                                    tipus: "comarca",
                                    municipis: [
                                        "L'Alcora", "Atzeneta del Maestrat", "Benafer", "Figueroles",
                                        "Lucena del Cid", "Useras", "Vistabella del Maestrat"
                                    ]
                                },
                                {
                                    nom: "Alt Maestrat",
                                    tipus: "comarca",
                                    municipis: [
                                        "Albocàsser", "Ares del Maestrat", "Benassal", "Catí",
                                        "Culla", "Tírig", "La Torre d'en Besora", "Vilafranca"
                                    ]
                                },
                                {
                                    nom: "Alto Mijares",
                                    tipus: "comarca",
                                    municipis: [
                                        "Arañuel", "Aras de los Olmos", "Castellnovo", "Cirat",
                                        "Cortes de Arenoso", "Espadilla", "Fanzara", "Fuentes de Ayódar",
                                        "Ludiente", "Montán", "Montanejos", "Puebla de Arenoso", "Toga",
                                        "Torrechiva", "Vallat", "Villahermosa del Río", "Villamalur", "Villanueva de Viver",
                                        "Zucaina"
                                    ]
                                },
                                {
                                    nom: "Baix Maestrat",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alcalà de Xivert", "Benicarló", "Càlig", "Canet lo Roig",
                                        "Castell de Cabres", "Cervera del Maestre", "La Jana", "Peñíscola",
                                        "Rossell", "Sant Jordi", "Sant Mateu", "Santa Magdalena de Polpís",
                                        "Traiguera", "Vinaròs"
                                    ]
                                },
                                {
                                    nom: "Els Ports",
                                    tipus: "comarca",
                                    municipis: [
                                        "Castellfort", "Cinctorres", "Forcall", "Herbés",
                                        "Morella", "Olocau del Rey", "Palanques", "Portell de Morella",
                                        "Todolella", "La Todolella", "Villores"
                                    ]
                                },
                                {
                                    nom: "Plana Alta",
                                    tipus: "comarca",
                                    municipis: [
                                        "Almassora", "Benicàssim", "Borriol", "Cabanes",
                                        "Castelló de la Plana", "Les Coves de Vinromà", "Orpesa", "Pobla de Tornesa",
                                        "Sant Joan de Moró", "Torreblanca", "Vall d'Alba", "Vilafamés",
                                        "Vilanova d'Alcolea"
                                    ]
                                },
                                {
                                    nom: "Plana Baixa",
                                    tipus: "comarca",
                                    municipis: [
                                        "Aín", "Alcudia de Veo", "Alfondeguilla", "Artana",
                                        "Betxí", "Borriana", "Eslida", "Moncofa",
                                        "Nules", "Onda", "Suera", "Tales",
                                        "La Vall d'Uixó", "Vila-real", "Xilxes"
                                    ]
                                }
                            ]
                        },
                        valencia: {
                            nom: "València",
                            comarques: [
                                {
                                    nom: "Camp de Túria",
                                    tipus: "comarca",
                                    municipis: [
                                        "Benaguasil", "Benissanó", "Bétera", "Casinos",
                                        "Gàtova", "Llíria", "Loriguilla", "Losa del Obispo",
                                        "Montcada", "Nàquera", "Olocau", "La Pobla de Vallbona",
                                        "Riba-roja de Túria", "San Antonio de Benagéber", "Serra", "Vilamarxant"
                                    ]
                                },
                                {
                                    nom: "Camp de Morvedre",
                                    tipus: "comarca",
                                    municipis: [
                                        "Albalat dels Tarongers", "Alfara de la Baronia", "Algímia d'Alfara", "Benavites",
                                        "Benifairó de les Valls", "Canet d'en Berenguer", "Estivella", "Faura",
                                        "Gilet", "Petrés", "Quart de les Valls", "Quartell",
                                        "Sagunt", "Segart"
                                    ]
                                },
                                {
                                    nom: "Canal de Navarrés",
                                    tipus: "comarca",
                                    municipis: [
                                        "Anna", "Bicorp", "Bolbaite", "Chella",
                                        "Enguera", "Millares", "Navarrés", "Quesa"
                                    ]
                                },
                                {
                                    nom: "Costera",
                                    tipus: "comarca",
                                    municipis: [
                                        "L'Alcúdia de Crespins", "Barxeta", "Bèlgida", "Bellús",
                                        "Benigànim", "Benimodo", "Canals", "Cerdà",
                                        "L'Ènova", "Genovés", "Guadassèquies", "Llocnou d'en Fenollet",
                                        "La Llosa de Ranes", "Moixent", "Montesa", "Novetlè",
                                        "Olleria", "Ottos", "Quesa", "Rotglà i Corberà",
                                        "Torres Torres", "Vallada", "Vallés", "Xàtiva"
                                    ]
                                },
                                {
                                    nom: "Foia de Bunyol",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alborache", "Buñol", "Cheste", "Chiva",
                                        "Dos Aguas", "Godelleta", "Macastre", "Siete Aguas",
                                        "Turís", "Yátova"
                                    ]
                                },
                                {
                                    nom: "Horta Nord",
                                    tipus: "comarca",
                                    municipis: [
                                        "Albalat dels Sorells", "Albuixech", "Alfara del Patriarca", "Almàssera",
                                        "Bonrepòs i Mirambell", "Burjassot", "Emperador", "Foios",
                                        "Godella", "Massalfassar", "Massamagrell", "Meliana",
                                        "Moncada", "Museros", "Paterna", "La Pobla de Farnals",
                                        "Puig", "Rafelbunyol", "Rocafort", "Tavernes Blanques",
                                        "Vinalesa"
                                    ]
                                },
                                {
                                    nom: "Horta Oest",
                                    tipus: "comarca",
                                    municipis: [
                                        "Aldaia", "Alaquàs", "Almussafes", "Benifaió",
                                        "Beniparrell", "Boixar", "Bunyol", "Quart de Poblet",
                                        "Sedaví", "Silla", "Torrent", "Xirivella"
                                    ]
                                },
                                {
                                    nom: "Horta Sud",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alcàsser", "Alfafar", "Benetússer", "Beniparrell",
                                        "Catarroja", "Llocnou de la Corona", "Manises", "Massanassa",
                                        "Paiporta", "Picanya", "Picassent", "Silla"
                                    ]
                                },
                                {
                                    nom: "Requena-Utiel",
                                    tipus: "comarca",
                                    municipis: [
                                        "Camporrobles", "Caudete de las Fuentes", "Fuenterrobles", "Requena",
                                        "Sinarcas", "Utiel", "Venta del Moro", "Villargordo del Cabriel"
                                    ]
                                },
                                {
                                    nom: "Ribera Alta",
                                    tipus: "comarca",
                                    municipis: [
                                        "L'Alcúdia", "Alfarp", "Algemesí", "Alginet",
                                        "Alzira", "Antella", "Benifaió", "Benimuslem",
                                        "Carcaixent", "Càrcer", "Carlet", "Catadau",
                                        "Gavarda", "Guadassuar", "Llaurí", "Llombai",
                                        "Manuel", "Massalavés", "Montserrat", "La Pobla Llarga",
                                        "Rafelguaraf", "Real", "Sant Joan d'Ènova", "Senyera",
                                        "Sumacàrcer", "Tavernes Blanques"
                                    ]
                                },
                                {
                                    nom: "Ribera Baixa",
                                    tipus: "comarca",
                                    municipis: [
                                        "Albalat de la Ribera", "Almussafes", "Corbera", "Cullera",
                                        "Favara", "Fortaleny", "Llaurí", "Polinyà de Xúquer",
                                        "Riola", "Sueca"
                                    ]
                                },
                                {
                                    nom: "Safor",
                                    tipus: "comarca",
                                    municipis: [
                                        "Ador", "Alfauir", "Almiserà", "Beniarjó",
                                        "Benifairó de la Valldigna", "Beniflà", "Benirredrà", "Castellonet de la Conquesta",
                                        "Daimús", "Gandia", "Guardamar de la Safor", "Llocnou de Sant Jeroni",
                                        "Miramar", "Oliva", "Palma de Gandia", "Piles",
                                        "Potries", "Rafelcofer", "Real de Gandia", "Ròtova",
                                        "Simat de la Valldigna", "Tavernes de la Valldigna", "Xeraco", "Xeresa"
                                    ]
                                },
                                {
                                    nom: "Serrans",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alcublas", "Alpuente", "Andilla", "Aras de los Olmos",
                                        "Benagéber", "Bugarra", "Calles", "Chelva",
                                        "Chulilla", "Domeño", "Gestalgar", "Higueruelas",
                                        "Losa del Obispo", "Pedralba", "Sot de Chera", "Tuéjar",
                                        "Villar del Arzobispo"
                                    ]
                                },
                                {
                                    nom: "Vall d'Albaida",
                                    tipus: "comarca",
                                    municipis: [
                                        "L'Alqueria d'Asnar", "Albaida", "Alfarrasí", "Aielo de Malferit",
                                        "Aielo de Rugat", "Bellús", "Beniatjar", "Benicolet",
                                        "Benigànim", "Benissoda", "Benissuera", "Bocairent",
                                        "Bufali", "Carrícola", "Castelló de Rugat", "Fontanars dels Alforins",
                                        "Guadasséquies", "Llutxent", "Moixent", "Montaverner",
                                        "Ontinyent", "Otos", "Palomar", "Pinet",
                                        "La Pobla del Duc", "Quatretonda", "Ràfol de Salem", "Rugat",
                                        "Sempere", "Terrateig"
                                    ]
                                },
                                {
                                    nom: "Vall d'Aiora",
                                    tipus: "comarca",
                                    municipis: [
                                        "Aiora", "Cofrentes", "Corrales", "Jarafuel",
                                        "Teresa de Cofrentes", "Zarra"
                                    ]
                                }
                            ]
                        },
                        alacant: {
                            nom: "Alacant",
                            comarques: [
                                {
                                    nom: "Alacantí",
                                    tipus: "comarca",
                                    municipis: [
                                        "Agost", "Alacant", "Busot", "Campello",
                                        "Mutxamel", "Sant Joan d'Alacant", "Sant Vicent del Raspeig"
                                    ]
                                },
                                {
                                    nom: "Alcoià",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alcoi", "Banyeres de Mariola", "Benifallim", "Castalla",
                                        "Ibi", "Onil", "Tibi"
                                    ]
                                },
                                {
                                    nom: "Alto Vinalopó",
                                    tipus: "comarca",
                                    municipis: [
                                        "Beneixama", "Biar", "Camp de Mirra", "Cañada",
                                        "Elda", "Monòver", "Petrer", "Salinas"
                                    ]
                                },
                                {
                                    nom: "Baix Vinalopó",
                                    tipus: "comarca",
                                    municipis: [
                                        "Crevillent", "Elx", "Santa Pola"
                                    ]
                                },
                                {
                                    nom: "Comtat",
                                    tipus: "comarca",
                                    municipis: [
                                        "Agres", "Alcocer de Planes", "Alfafara", "Almudaina",
                                        "Balones", "Benasau", "Benilloba", "Benimassot",
                                        "Cocentaina", "Gaianes", "Gorga", "Millena",
                                        "Muro d'Alcoi"
                                    ]
                                },
                                {
                                    nom: "Marina Alta",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alcalalí", "Beniarbeig", "Benidoleig", "Benigembla",
                                        "Benissa", "Calp", "Dénia", "Gata de Gorgos",
                                        "Jesús Pobre (Entitat Local Menor)", "Llíber", "Murla",
                                        "Ondara", "Orba", "Parcent", "Pedreguer",
                                        "El Poble Nou de Benitatxell", "Els Poblets", "Sagra", "Sanet i els Negrals",
                                        "Senija", "Teulada", "Tormos", "La Vall d'Alcalà",
                                        "La Vall d'Ebo", "La Vall de Gallinera", "La Vall de Laguar", "Xàbia"
                                    ]
                                },
                                {
                                    nom: "Marina Baixa",
                                    tipus: "comarca",
                                    municipis: [
                                        "L'Alfàs del Pi", "Altea", "Benidorm", "Benimantell",
                                        "Bolulla", "Callosa d'en Sarrià", "Confrides", "Finestrat",
                                        "Guadalest", "La Nucia", "Orxeta", "Polop",
                                        "Relleu", "Sella", "Tàrbena", "La Vila Joiosa"
                                    ]
                                },
                                {
                                    nom: "Vega Baja del Segura",
                                    tipus: "comarca",
                                    municipis: [
                                        "Albatera", "Algorfa", "Almoradí", "Benejúzar",
                                        "Benferri", "Benijófar", "Bigastro", "Callosa de Segura",
                                        "Catral", "Cox", "Daya Nueva", "Daya Vieja",
                                        "Dolores", "Formentera del Segura", "Granja de Rocamora", "Guardamar del Segura",
                                        "Jacarilla", "Los Montesinos", "Orihuela", "Rafal",
                                        "Redován", "Rojales", "San Fulgencio", "San Isidro",
                                        "San Miguel de Salinas", "Torrevieja"
                                    ]
                                },
                                {
                                    nom: "Vinalopó Mitjà",
                                    tipus: "comarca",
                                    municipis: [
                                        "L'Alguenya", "Aspe", "Elda", "Hondón de las Nieves",
                                        "Hondón de los Frailes", "Monforte del Cid", "Novelda", "Petrer",
                                        "Pinoso", "La Romana"
                                    ]
                                }
                            ]
                        }
                    }
                },
                extremadura: {
                    nom: "Extremadura",
                    columnes: ["Pais", "Comunitat Autonoma", "Provincia", "Mancomunitat", "Poble", "Registres", "Opcions"],
                    provincies: {
                        caceres: {
                            nom: "Càceres",
                            mancomunitats: [
                                {
                                    nom: "Mancomunidad de la Comarca de Trujillo",
                                    tipus: "mancomunitat",
                                    municipis: [
                                        "Aldea del Obispo", "Belvís de Monroy", "Campo Lugar", "Conquista de la Sierra",
                                        "Escurial", "Huertas de la Magdalena", "Ibahernando", "Jaraicejo",
                                        "La Cumbre", "Madroñera", "Miajadas", "Plasenzuela",
                                        "Robledillo de Trujillo", "Ruanes", "Salvatierra de Santiago", "Santa Ana",
                                        "Santa Cruz de la Sierra", "Torrecillas de la Tiesa", "Trujillo", "Villamesías"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Sierra de Gata",
                                    tipus: "mancomunitat",
                                    municipis: [
                                        "Acebo", "Cilleros", "Eljas", "Gata",
                                        "Hernán-Pérez", "Hoyos", "Perales del Puerto", "Robledillo de Gata",
                                        "San Martín de Trevejo", "Torrecilla de los Ángeles", "Valverde del Fresno"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad Integral Sierra de Montánchez y Tamuja",
                                    tipus: "mancomunitat",
                                    municipis: [
                                        "Albalá", "Alcuéscar", "Aldea del Cano", "Almoharín",
                                        "Arroyo de la Luz", "Benquerencia", "Botija", "Cáceres",
                                        "Carmonita", "Casar de Miajadas", "Casas de Don Antonio", "Casas de Juan Gil",
                                        "Casas de Millán", "Garrovillas de Alconétar", "Hinojal", "Malpartida de Cáceres",
                                        "Montánchez", "Plasenzuela", "Riolobos", "Salvatierra de Santiago",
                                        "Santa Marta de Magasca", "Torre de Santa María", "Torremocha", "Torrequemada",
                                        "Valdefuentes", "Valdemorales", "Zarza de Montánchez"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad Intermunicipal de las Villuercas-Ibores-Jara",
                                    tipus: "mancomunitat",
                                    municipis: [
                                        "Aldeacentenera", "Alía", "Berzocana", "Cabañas del Castillo",
                                        "Cañamero", "Castañar de Ibor", "Guadalupe", "Logrosán",
                                        "Navalvillar de Ibor", "Navezuelas", "Robledollano", "Ruanes",
                                        "Villar del Pedroso"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios del Valle del Jerte",
                                    tipus: "mancomunitat",
                                    municipis: [
                                        "Barrado", "Cabezuela del Valle", "Cabrero", "Casas del Castañar",
                                        "El Torno", "Jerte", "Navaconcejo", "Piornal",
                                        "Rebollar", "Tornavacas", "Valdastillas"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de La Vera",
                                    tipus: "mancomunitat",
                                    municipis: [
                                        "Aldeanueva de la Vera", "Arroyomolinos de la Vera", "Collado", "Cuacos de Yuste",
                                        "Garganta la Olla", "Gargüera", "Guijo de Santa Bárbara", "Jaraíz de la Vera",
                                        "Losar de la Vera", "Madrigal de la Vera", "Navalmoral de la Mata", "Pasarón de la Vera",
                                        "Pinofranqueado", "Robledillo de la Vera", "Talaveruela de la Vera", "Tejeda de Tiétar",
                                        "Torremenga", "Valverde de la Vera", "Viandar de la Vera"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Riberos del Tajo",
                                    tipus: "mancomunitat",
                                    municipis: [
                                        "Alcántara", "Brozas", "Ceclavín", "Estorninos",
                                        "Garrovillas de Alconétar", "Herrera de Alcántara", "Malpartida de Alcántara", "Navas del Madroño",
                                        "Piedras Albas", "Portaje", "Santiago del Campo", "Villa del Rey"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad Tajo-Salor",
                                    tipus: "mancomunitat",
                                    municipis: [
                                        "Aliseda", "Arroyo de la Luz", "Cáceres", "Casar de Cáceres",
                                        "Malpartida de Cáceres", "Monroy", "Navas del Madroño", "Torrejoncillo"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Trasierra-Tierras de Granadilla",
                                    tipus: "mancomunitat",
                                    municipis: [
                                        "Ahigal", "Aldeanueva del Camino", "Baños de Montemayor", "Carcaboso",
                                        "Casar de Palomero", "Casas del Monte", "Cerezo", "Gargantilla",
                                        "Granadilla", "Guijo de Granadilla", "Herguijuela", "La Pesga",
                                        "Ladrillar", "Marchagaz", "Mohedas de Granadilla", "Oliva de Plasencia", "Palomero",
                                        "Pinofranqueado", "Plasencia", "Pozuelo de Zarzón", "Riomalo de Abajo",
                                        "Santibáñez el Alto", "Serradilla", "Torrecilla de los Ángeles", "Torremenga",
                                        "Valdastillas", "Valdecañas de Tajo", "Valdehúncar", "Valdelacasa de Tajo",
                                        "Valdemorales", "Zarza de Granadilla"
                                    ]
                                },
                                {
                                    nom: "Altres",
                                    tipus: "mancomunitat",
                                    municipis: [
                                        "Abadía", "Almaraz", "Arroyo de San Serván", "Berrocalejo",
                                        "Canchales", "Casatejada", "Guijo de Galisteo", "Hervás",
                                        "Holguera", "Madrigal de la Vera", "Navalmoral de la Mata", "Navalvillar de Pela",
                                        "Peraleda de la Mata", "Plasencia", "Talayuela", "Tejeda de Tiétar",
                                        "Toril", "Valdecañas de Tajo", "Villanueva de la Vera", "Villarreal de San Carlos"
                                    ]
                                }
                            ]
                        },
                        badajoz: {
                            nom: "Badajoz",
                            mancomunitats: [
                                {
                                    nom: "Mancomunidad Integral de Olivenza",
                                    tipus: "mancomunitat",
                                    municipis: [
                                        "Alconchel", "Cheles", "Olivenza", "Táliga",
                                        "Villarreal"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de la Comarca de Zafra - Río Bodión",
                                    tipus: "mancomunitat",
                                    municipis: [
                                        "Almendral", "Bienvenida", "Burguillos del Cerro", "Calzadilla de los Barros",
                                        "Feria", "Fuente del Maestre", "La Lapa", "Los Santos de Maimona",
                                        "Medina de las Torres", "Puebla de Sancho Pérez", "Valencia del Mombuey", "Zafra"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios Sierra Suroeste",
                                    tipus: "mancomunitat",
                                    municipis: [
                                        "Aceuchal", "Barcarrota", "Higuera de Vargas", "Jerez de los Caballeros",
                                        "Oliva de la Frontera", "Salvatierra de los Barros", "Valverde de Burguillos", "Valverde de Leganés"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad Integral Lácara - Los Baldíos",
                                    tipus: "mancomunitat",
                                    municipis: [
                                        "Alburquerque", "La Codosera", "Puebla de Obando", "San Vicente de Alcántara"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad Integral de Servicios Vegas Bajas",
                                    tipus: "mancomunitat",
                                    municipis: [
                                        "Alvarado", "Badajoz", "Balboa", "Gévora",
                                        "Guadiana del Caudillo", "Puebla de la Calzada", "Sagrajas", "San Juan Bautista",
                                        "Torremayor", "Valdelacalzada", "Villar del Rey"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Tierra de Barros",
                                    tipus: "mancomunitat",
                                    municipis: [
                                        "Aceuchal", "Almendralejo", "Fuente del Maestre", "Hinojosa del Valle",
                                        "Hornachos", "Palomas", "Puebla de la Reina", "Ribera del Fresno",
                                        "Santa Marta", "Torremejía", "Villalba de los Barros"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de La Serena",
                                    tipus: "mancomunitat",
                                    municipis: [
                                        "Benquerencia de la Serena", "Cabeza del Buey", "Capilla", "Castuera",
                                        "Esparragosa de Lares", "Higuera de la Serena", "Malpartida de la Serena", "Monterrubio de la Serena",
                                        "Peñalsordo", "Puebla de Alcocer", "Quintana de la Serena", "Siruela",
                                        "Valdemanco", "Zarza Capilla"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Tentudía",
                                    tipus: "mancomunitat",
                                    municipis: [
                                        "Bienvenida", "Bodonal de la Sierra", "Cabeza la Vaca", "Calera de León",
                                        "Fuente de Cantos", "Fuentes de León", "Higuera la Real", "Monesterio",
                                        "Montemolín", "Pallares", "Segura de León"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Vegas Altas",
                                    tipus: "mancomunitat",
                                    municipis: [
                                        "Acedera", "Baterno", "Casas de Don Pedro", "Esparragosa de Lares",
                                        "Garbayuela", "Garlitos", "Navalvillar de Pela", "Orellana de la Sierra",
                                        "Orellana la Vieja", "Peraleda del Zaucejo", "Puebla de Alcocer", "Talarrubias",
                                        "Villanueva de la Serena"
                                    ]
                                },
                                {
                                    nom: "Altres",
                                    tipus: "mancomunitat",
                                    municipis: [
                                        "Azuaga", "Berlanga", "Campanario", "La Coronada",
                                        "Don Benito", "Entrín Bajo", "Esparragalejo", "Fregenal de la Sierra",
                                        "Guareña", "Magacela", "Malcocinado", "Mengabril",
                                        "Obando", "Peraleda del Zaucejo", "Puebla del Prior", "Salvaleón",
                                        "San Pedro de Mérida", "Santa Amalia", "Trasierra", "Valdecaballos",
                                        "Valdetorres", "Villagonzalo", "Villanueva del Fresno", "Villar de Rena"
                                    ]
                                }
                            ]
                        }
                    }
                },
                galicia: {
                    nom: "Galicia",
                    columnes: ["Pais", "Comunitat Autonoma", "Provincia", "Comarca", "Concello", "Registres", "Opcions"],
                    provincies: {
                        a_corunya: {
                            nom: "A Coruña",
                            concello: [
                                {
                                    nom: "Mancomunidad de Municipios de A Coruña",
                                    tipus: "concello",
                                    municipis: [
                                        "A Coruña", "Arteixo", "Bergondo", "Betanzos",
                                        "Cambre", "Carral", "Culleredo", "Oleiros",
                                        "Sada"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad Intermunicipal de Ferrolterra",
                                    tipus: "concello",
                                    municipis: [
                                        "Ares", "Cabanas", "Fene", "Ferrol",
                                        "Mugardos", "Narón", "Neda", "Pontedeume",
                                        "Valdoviño"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Barbanza Arousa",
                                    tipus: "concello",
                                    municipis: [
                                        "A Pobra do Caramiñal", "Boiro", "Rianxo"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Noia",
                                    tipus: "concello",
                                    municipis: [
                                        "Lousame", "Noia", "Outes", "Porto do Son"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Ordes",
                                    tipus: "concello",
                                    municipis: [
                                        "Cerceda", "Frades", "Mesía", "Ordes",
                                        "Oroso", "Tordoia"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Santiago",
                                    tipus: "concello",
                                    municipis: [
                                        "Ames", "Boqueixón", "Brión", "Santiago de Compostela",
                                        "Teo", "Val do Dubra"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Arzúa",
                                    tipus: "concello",
                                    municipis: [
                                        "Arzúa", "Boimorto", "O Pino", "Touro"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Bergantiños",
                                    tipus: "concello",
                                    municipis: [
                                        "Cabana de Bergantiños", "Carballo", "Coristanco", "Laxe",
                                        "Malpica de Bergantiños", "Ponteceso"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios da Comarca da Terra de Melide",
                                    tipus: "concello",
                                    municipis: [
                                        "Melide", "Santiso", "Sobrado"
                                    ]
                                },
                                {
                                    nom: "Otros Municipios (sin mancomunidad específica o información no detallada)",
                                    tipus: "concello",
                                    municipis: [
                                        "Abegondo", "Aranga", "Boiro", "Cee",
                                        "Curtis", "Dodro", "Dumbría", "Irixoa",
                                        "Mazaricos", "Muros", "Muxía", "Padrón",
                                        "Ribeira", "Santa Comba", "Vedra"
                                    ]
                                }
                            ]
                        },
                        lugo: {
                            nom: "Lugo",
                            comarques: [
                                {
                                    nom: "Mancomunidad de Municipios de la Comarca de Lugo",
                                    tipus: "concello",
                                    municipis: [
                                        "Castro de Rei", "Friol", "Guntín", "Lugo",
                                        "O Corgo", "Outeiro de Rei", "Portomarín", "Rábade"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de A Mariña Central",
                                    tipus: "concello",
                                    municipis: [
                                        "Alfoz", "Burela", "Foz", "Mondoñedo",
                                        "O Valadouro"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de A Mariña Occidental",
                                    tipus: "concello",
                                    municipis: [
                                        "Cervo", "Ourol", "Viveiro", "Xove"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de A Mariña Oriental",
                                    tipus: "concello",
                                    municipis: [
                                        "Barreiros", "Lourenzá", "Ribadeo", "Trabada"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Chantada",
                                    tipus: "concello",
                                    municipis: [
                                        "Carballedo", "Chantada", "Taboada"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Monforte de Lemos",
                                    tipus: "concello",
                                    municipis: [
                                        "Bóveda", "Monforte de Lemos", "Pantón", "Sober"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Sarria",
                                    tipus: "concello",
                                    municipis: [
                                        "O Incio", "Láncara", "Paradela", "Samos",
                                        "Sarria"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Terra Chá",
                                    tipus: "concello",
                                    municipis: [
                                        "Abadín", "Begonte", "Castro de Rei", "Cospeito",
                                        "Guitiriz", "Muras", "Vilalba", "Xermade"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Os Ancares",
                                    tipus: "concello",
                                    municipis: [
                                        "Baralla", "Becerreá", "Cervantes", "Navia de Suarna",
                                        "Pedrafita do Cebreiro"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de A Fonsagrada",
                                    tipus: "concello",
                                    municipis: [
                                        "A Fonsagrada", "Baleira", "Negueira de Muñiz"
                                    ]
                                },
                                {
                                    nom: "Otros Municipios (sin mancomunidad específica o información no detallada)",
                                    tipus: "concello",
                                    municipis: [
                                        "Antas de Ulla", "Folgoso do Courel", "Monterroso",
                                        "Palas de Rei", "Quiroga"
                                    ]
                                }
                            ]
                        },
                        pontevedra: {
                            nom: "Pontevedra",
                            mancomunitats: [
                                {
                                    nom: "Mancomunidad Intermunicipal de Arousa Norte",
                                    tipus: "concello",
                                    municipis: [
                                        "A Illa de Arousa", "Cambados", "O Grove", "Vilanova de Arousa"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad Intermunicipal de O Salnés",
                                    tipus: "concello",
                                    municipis: [
                                        "Meaño", "Ribadumia", "Sanxenxo", "Vilagarcía de Arousa"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Pontevedra",
                                    tipus: "concello",
                                    municipis: [
                                        "Barro", "Campo Lameiro", "Cerdedo-Cotobade", "Marín",
                                        "Poio", "Ponte Caldelas", "Pontevedra"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de O Morrazo",
                                    tipus: "concello",
                                    municipis: [
                                        "Bueu", "Cangas", "Moaña"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios del Área Intermunicipal de Vigo",
                                    tipus: "concello",
                                    municipis: [
                                        "Baiona", "Fornelos de Montes", "Gondomar", "Mos",
                                        "Nigrán", "O Porriño", "Redondela", "Salceda de Caselas",
                                        "Soutomaior", "Vigo"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Caldas",
                                    tipus: "concello",
                                    municipis: [
                                        "Caldas de Reis", "Catoira", "Moraña", "Pontecesures",
                                        "Valga"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Deza",
                                    tipus: "concello",
                                    municipis: [
                                        "Agolada", "Dozón", "Lalín", "Rodeiro",
                                        "Silleda", "Vila de Cruces"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Tabeirós - Terra de Montes",
                                    tipus: "concello",
                                    municipis: [
                                        "A Estrada", "Forcarei"
                                    ]
                                },
                                {
                                    nom: "Otros Municipios (sin mancomunidad específica o información no detallada)",
                                    tipus: "concello",
                                    municipis: [
                                        "Arbo", "As Neves", "Covelo", "Crecente",
                                        "A Guarda", "Mondariz", "Mondariz-Balneario", "Oia",
                                        "Pazos de Borbén", "Salvatierra de Miño"
                                    ]
                                }
                            ]
                        },
                        ourense: {
                            nom: "Ourense",
                            mancomunitats: [
                                {
                                    nom: "Mancomunidad Intermunicipal de la Comarca de Ourense",
                                    tipus: "concello",
                                    municipis: [
                                        "Barbadás", "Coles", "Esgos", "Nogueira de Ramuín",
                                        "Ourense", "Paderne de Allariz", "Pereiro de Aguiar", "San Cibrao das Viñas"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de A Limia",
                                    tipus: "concello",
                                    municipis: [
                                        "Bande", "Calvos de Randín", "Entrimo", "Lobeira",
                                        "Lobios", "Muíños", "Porqueira", "Rairiz de Veiga",
                                        "Sandias", "Sarreaus", "Trasmiras", "Vilar de Santos"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Verín",
                                    tipus: "concello",
                                    municipis: [
                                        "Castrelo do Val", "Cualedro", "Laza", "Monterrei",
                                        "Oímbra", "Riós", "Verín", "Vilardevós"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Valdeorras",
                                    tipus: "concello",
                                    municipis: [
                                        "A Rúa", "Carballeda de Valdeorras", "Larouco", "Petín",
                                        "Rubiá", "Vilamartín de Valdeorras"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Terra de Celanova",
                                    tipus: "concello",
                                    municipis: [
                                        "A Bola", "Cartelle", "Celanova", "Ramirás",
                                        "Verea"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de Allariz-Maceda",
                                    tipus: "concello",
                                    municipis: [
                                        "Allariz", "Maceda", "Xunqueira de Ambía"
                                    ]
                                },
                                {
                                    nom: "Mancomunidad de Municipios de A Baixa Limia",
                                    tipus: "concello",
                                    municipis: [
                                        "Bande", "Entrimo", "Lobios", "Muíños"
                                    ]
                                },
                                {
                                    nom: "Otros Municipios (sin mancomunidad específica o información no detallada)",
                                    tipus: "concello",
                                    municipis: [
                                        "A Arnoia", "Avión", "Baltar", "Beariz",
                                        "Blancos", "Boborás", "Castro Caldelas", "Chandrexa de Queixa",
                                        "Coles", "Cortegada", "Laza", "Leiro",
                                        "Maside", "Melón", "Montederramo", "Parada de Sil",
                                        "Peroxa", "Pobra de Trives", "Pontedeva", "Punxín",
                                        "San Cristovo de Cea", "Taboadela", "Teixeira", "Toén",
                                        "Viana do Bolo", "Vilar de Barrio"
                                    ]
                                }
                            ]
                        }
                    }
                },
                madrid: {
                    nom: "Madrid",
                    columnes: ["Pais", "Comunitat Autonoma", "Provincia", "Comarca", "Poble", "Registres", "Opcions"],
                    provincies: {
                        madrid: {
                            nom: "Madrid",
                            comarques: [
                                {
                                    nom: "Madrid Capital",
                                    tipus: "comarca",
                                    municipis: [
                                        "Madrid"
                                    ]
                                },
                                {
                                    nom: "Sierra Norte",
                                    tipus: "comarca",
                                    municipis: [
                                        "Acebeda", "Ajalvir", "Alameda del Valle", "El Atazar",
                                        "El Berrueco", "Berzosa del Lozoya", "Boadilla del Monte", "Braojos",
                                        "Bustarviejo", "Cabanillas de la Sierra", "La Cabrera", "Canencia",
                                        "Carabaña", "Casarrubuelos", "Cenicientos", "Cercedilla",
                                        "Cervera de Buitrago", "Chapinería", "Ciempozuelos", "Colmenar de Oreja",
                                        "Colmenar del Arroyo", "Colmenar Viejo", "Corpa", "Coslada",
                                        "Cubas de la Sagra", "Daganzo de Arriba", "El Escorial", "Estremera",
                                        "Fresnedillas de la Oliva", "Fuente el Saz de Jarama", "Fuenlabrada", "Galapagar",
                                        "Garganta de los Montes", "Gargantilla del Lozoya y Pinilla de Buitrago", "Gascones", "Griñón",
                                        "Guadalix de la Sierra", "Guadarrama", "Horcajo de la Sierra-Aoslos", "Horcajuelo de la Sierra",
                                        "Hoyo de Manzanares", "Humanes de Madrid", "Leganés", "Loeches",
                                        "Lozoya", "Madarcos", "Majadahonda", "Manzanares el Real",
                                        "Meco", "Mejorada del Campo", "Miraflores de la Sierra", "Molar",
                                        "Los Molinos", "Montejo de la Sierra", "Navacerrada", "Navalafuente",
                                        "Navalagamella", "Navalcarnero", "Navas del Rey", "Nuevo Baztán",
                                        "Olmeda de las Fuentes", "Orusco de Tajuña", "Paracuellos de Jarama", "Parla",
                                        "Patones", "Pedrezuela", "Pelayos de la Presa", "Perales de Tajuña",
                                        "Pezuela de las Torres", "Pinilla del Valle", "Pinto", "Piñuécar-Gandullas",
                                        "Pozuelo de Alarcón", "Pozuelo del Rey", "Prádena del Rincón", "Puebla de la Sierra",
                                        "Puentes Viejas", "Quijorna", "Rascafría", "Redueña",
                                        "Ribatejada", "Rivas-Vaciamadrid", "Robledillo de la Jara", "Robledo de Chavela",
                                        "Robregordo", "Las Rozas de Madrid", "San Agustín del Guadalix", "San Fernando de Henares",
                                        "San Lorenzo de El Escorial", "San Martín de la Vega", "San Martín de Valdeiglesias", "Santa María de la Alameda",
                                        "Santorcaz", "Los Santos de la Humosa", "Serranillos del Valle", "Sevilla la Nueva",
                                        "Somosierra", "Soto del Real", "Talamanca de Jarama", "Tielmes",
                                        "Titulcia", "Torrejón de Ardoz", "Torrejón de la Calzada", "Torrejón de Velasco", "Torrelaguna",
                                        "Torrelodones", "Torremocha de Jarama", "Torres de la Alameda", "Tres Cantos", "Valdaracete",
                                        "Valdeavero", "Valdelaguna", "Valdemanco", "Valdemaqueda",
                                        "Valdemorillo", "Valdemoro", "Valdeolmos-Alalpardo", "Valdepiélagos", "Valdetorres de Jarama",
                                        "Valdilecha", "Villaconejos", "Villalbilla", "Villamanrique de Tajo", "Villamanta",
                                        "Villamantilla", "Villanueva de la Cañada", "Villanueva de Perales", "Villanueva del Pardillo", "Villar del Olmo"
                                    ]
                                },
                                {
                                    nom: "Cuenca del Guadarrama",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alpedrete", "Becerril de la Sierra", "Boadilla del Monte", "Brunete",
                                        "Cercedilla", "Collado Mediano", "Colmenarejo", "El Escorial",
                                        "Galapagar", "Guadarrama", "Hoyo de Manzanares", "Las Rozas de Madrid",
                                        "Majadahonda", "Moralzarzal", "Navacerrada", "Quijorna",
                                        "San Lorenzo de El Escorial", "Torrelodones", "Valdemorillo", "Villanueva de la Cañada",
                                        "Villanueva del Pardillo"
                                    ]
                                },
                                {
                                    nom: "Campiña",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alcalá de Henares", "Coslada", "Daganzo de Arriba", "Fuente el Saz de Jarama",
                                        "Loeches", "Meco", "Mejorada del Campo", "Paracuellos de Jarama",
                                        "Pezuela de las Torres", "San Fernando de Henares", "Torrejón de Ardoz", "Torres de la Alameda",
                                        "Villalbilla"
                                    ]
                                },
                                {
                                    nom: "Sur",
                                    tipus: "comarca",
                                    municipis: [
                                        "Aranjuez", "Batres", "Casarrubuelos", "Ciempozuelos",
                                        "Colmenar de Oreja", "Chinchón", "Getafe", "Griñón",
                                        "Humanes de Madrid", "Leganés", "Moraleja de Enmedio", "Móstoles",
                                        "Parla", "Pinto", "San Martín de la Vega", "Serranillos del Valle",
                                        "Titulcia", "Torrejón de la Calzada", "Torrejón de Velasco", "Valdemoro",
                                        "Valdilecha", "Villaconejos", "Villamanrique de Tajo", "Villamanta",
                                        "Villamantilla"
                                    ]
                                },
                                {
                                    nom: "Este",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alcalá de Henares", "Campo Real", "Carabaña", "Corpa",
                                        "Estremera", "Fuentidueña de Tajo", "Loeches", "Mejorada del Campo",
                                        "Nuevo Baztán", "Olmeda de las Fuentes", "Orusco de Tajuña", "Perales de Tajuña",
                                        "Pezuela de las Torres", "Pozo de Guadalajara", "Rivas-Vaciamadrid", "Santorcaz",
                                        "Tielmes", "Torrejón de Ardoz", "Torres de la Alameda", "Valdaracete",
                                        "Valdilecha", "Villar del Olmo"
                                    ]
                                }
                            ]
                        }
                    }
                },
                murcia: {
                    nom: "Murcia",
                    columnes: ["Pais", "Comunitat Autonoma", "Provincia", "Comarca", "Poble", "Registres", "Opcions"],
                    provincies: {
                        murcia: {
                            nom: "Murcia",
                            comarques: [
                                {
                                    nom: "Alto Guadalentín",
                                    tipus: "comarca",
                                    municipis: [
                                        "Águilas", "Lorca", "Puerto Lumbreras"
                                    ]
                                },
                                {
                                    nom: "Bajo Guadalentín",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alhama de Murcia", "Fuente Álamo de Murcia", "Totana"
                                    ]
                                },
                                {
                                    nom: "Campo de Cartagena",
                                    tipus: "comarca",
                                    municipis: [
                                        "Cartagena", "La Unión", "Los Alcázares", "San Javier",
                                        "San Pedro del Pinatar", "Torre-Pacheco"
                                    ]
                                },
                                {
                                    nom: "Huerta de Murcia",
                                    tipus: "comarca",
                                    municipis: [
                                        "Alcantarilla", "Beniel", "Murcia", "Santomera"
                                    ]
                                },
                                {
                                    nom: "Región del Noroeste",
                                    tipus: "comarca",
                                    municipis: [
                                        "Bullas", "Caravaca de la Cruz", "Cehegín", "Moratalla"
                                    ]
                                },
                                {
                                    nom: "Vega Alta del Segura",
                                    tipus: "comarca",
                                    municipis: [
                                        "Abarán", "Blanca", "Cieza"
                                    ]
                                },
                                {
                                    nom: "Vega Media del Segura",
                                    tipus: "comarca",
                                    municipis: [
                                        "Archena", "Ceutí", "Lorquí", "Molina de Segura"
                                    ]
                                },
                                {
                                    nom: "Valle de Ricote",
                                    tipus: "comarca",
                                    municipis: [
                                        "Arboleas", "Ojós", "Ricote", "Ulea",
                                        "Villanueva del Río Segura"
                                    ]
                                },
                                {
                                    nom: "Comarca Oriental",
                                    tipus: "comarca",
                                    municipis: [
                                        "Abanilla", "Fortuna"
                                    ]
                                }
                            ]
                        }
                    }
                },
                navarra: {
                    nom: "Navarra",
                    columnes: ["Pais", "Comunitat Autonoma", "Provincia", "Merindades", "Poble", "Registres", "Opcions"],
                    provincies: {
                        navarra: {
                            nom: "Navarra",
                            comarques: [
                                {
                                    nom: "Pamplona",
                                    tipus: "Merinda",
                                    municipis: [
                                        "Ansoáin", "Aranguren", "Atarrabia", "Barañáin",
                                        "Berrioplano", "Berriozar", "Burlada/Burlata", "Egüés",
                                        "Huarte/Uharte", "Noáin (Valle de Elorz)/Noain (Elortzibar)", "Orcoyen", "Pamplona/Iruña",
                                        "Villava/Atarrabia"
                                    ]
                                },
                                {
                                    nom: "Comarca de Puente la Reina/Gares",
                                    tipus: "Merinda",
                                    municipis: [
                                        "Adiós", "Campanas", "Eneriz", "Garínoain",
                                        "Guirguillano", "Legarda", "Mañeru", "Obanos",
                                        "Puente la Reina/Gares", "Tiebas-Muruarte de Reta", "Uterga", "Valdizarbe"
                                    ]
                                },
                                {
                                    nom: "Comarca de Estella Occidental",
                                    tipus: "Merinda",
                                    municipis: [
                                        "Allín", "Allo", "Améscoa Baja", "Ancín",
                                        "Arellano", "Arroniz", "Ayegui/Aiegi", "Barásoain",
                                        "Cabredo", "Desojo", "Dicastillo", "Espronceda",
                                        "Estella/Lizarra", "Eulate", "Genevilla", "Guesálaz",
                                        "Igúzquiza", "Larraga", "Lapoblación", "Los Arcos",
                                        "Luquin", "Marañón", "Mendavia", "Metauten",
                                        "Morentin", "Mues", "Oco", "Oteiza",
                                        "Sartaguda", "Sesma", "Torralba del Río", "Torres del Río",
                                        "Viana", "Villamayor de Monjardín", "Villatuerta", "Yerri"
                                    ]
                                },
                                {
                                    nom: "Comarca de Estella Oriental",
                                    tipus: "Merinda",
                                    municipis: [
                                        "Abáigar", "Abárzuza/Abartzuza", "Aieguí", "Allín",
                                        "Améscoa Alta", "Anue", "Arakil", "Aranarache",
                                        "Arbizu", "Areso", "Arizala", "Arruazu",
                                        "Bakaiku", "Basaburua", "Beintza-Labaien", "Bera",
                                        "Betelu", "Burguete/Burgelu", "Burlada/Burlata", "Donamaria",
                                        "Echarri/Etxarri", "Etxalar", "Ezkurra", "Garaioa",
                                        "Goizueta", "Imotz", "Irurtzun", "Ituren",
                                        "Iturmendi", "Lakuntza", "Lantz", "Larraun",
                                        "Leitza", "Lekunberri", "Lesaka", "Lizasoáin",
                                        "Olazti/Olazagutia", "Ollaran", "Ollo", "Orbaizeta",
                                        "Orbara", "Urdiain", "Urdazubi/Urdax", "Ultzama",
                                        "Zubieta", "Zugarramurdi"
                                    ]
                                },
                                {
                                    nom: "Comarca de Sangüesa",
                                    tipus: "Merinda",
                                    municipis: [
                                        "Aibar/Oibar", "Cáseda", "Eslava", "Ezprogui",
                                        "Gallués/Galoze", "Javier", "Liédena", "Lumbier",
                                        "Romanzado", "Sangüesa/Zangoza", "Yesa"
                                    ]
                                },
                                {
                                    nom: "Comarca de Tudela",
                                    tipus: "Merinda",
                                    municipis: [
                                        "Ablitas", "Arguedas", "Barillas", "Bera",
                                        "Buñuel", "Cabanas de Tudela", "Carcar", "Cascante",
                                        "Castejón", "Cintruénigo", "Corella", "Cortes",
                                        "Fitero", "Fontellas", "Fustiñana", "Murchante",
                                        "Ribaforada", "Tudela", "Valtierra"
                                    ]
                                },
                                {
                                    nom: "Pirinio Navarro",
                                    tipus: "Merinda",
                                    municipis: [
                                        "Abaurregaina/Abaurrea Alta", "Abaurrepea/Abaurrea Baja", "Auritz/Burguete", "Esparza de Salazar",
                                        "Ezcároz/Ezkaroze", "Gallués/Galoze", "Isaba/Izaba", "Jaurrieta",
                                        "Ochagavía/Otsagabia", "Roncal/Erronkari", "Urzainqui/Urzainki", "Vidángoz/Bidankoze"
                                    ]
                                },
                                {
                                    nom: "Zona Media Oriental",
                                    tipus: "Merinda",
                                    municipis: [
                                        "Artajona", "Barásoain", "Beire", "Berbinzana",
                                        "Cárcar", "Falces", "Garínoain", "Larraga",
                                        "Lerín", "Lumbier", "Miranda de Arga", "Olite/Erriberri",
                                        "Pitillas", "San Adrián", "Tafalla"
                                    ]
                                },
                                {
                                    nom: "Zona Media Occidental",
                                    tipus: "Merinda",
                                    municipis: [
                                        "Adiós", "Biurrun-Olcoz", "Enériz", "Guesálaz",
                                        "Guirguillano", "Leoz", "Muruzábal", "Obanos",
                                        "Olóriz", "Orísoain", "Puente la Reina/Gares", "Tiebas-Muruarte de Reta",
                                        "Unzué", "Uterga"
                                    ]
                                }
                            ]
                        }
                    }
                },
                paísbasc: {
                    nom: "País Basc",
                    columnes: ["Pais", "Comunitat Autonoma", "Provincia", "Comarca", "Poble", "Registres", "Opcions"],
                    provincies: {
                        alava: {
                            nom: "Álava",
                            comarques: [
                                {
                                    nom: "Vitoria-Gasteiz",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Vitoria-Gasteiz"
                                    ]
                                },
                                {
                                    nom: "Cuadrilla de Ayala/Aiarako Kuadrilla",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Amurrio", "Artziniega", "Aiara/Ayala", "Laudio/Llodio",
                                        "Okondo"
                                    ]
                                },
                                {
                                    nom: "Cuadrilla de Añana",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Añana", "Armiñón", "Berantevilla", "Erriberagoitia/Ribera Alta",
                                        "Iruraiz-Gauna", "Kuartango", "Zambrana"
                                    ]
                                },
                                {
                                    nom: "Cuadrilla de Campezo/Kanpezu",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Arraia-Maeztu", "Bernedo", "Campezo/Kanpezu", "Lagrán"
                                    ]
                                },
                                {
                                    nom: "Cuadrilla de Gorbeialdea",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Aramaio", "Arratzua-Ubarrundia", "Legutio", "Urkabustaiz",
                                        "Zigoitia", "Zuia"
                                    ]
                                },
                                {
                                    nom: "Cuadrilla de Salvatierra/Agurain",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Agurain/Salvatierra", "Alegría-Dulantzi", "Asparrena", "Barrundia",
                                        "Elburgo/Burgelu", "Iruraiz-Gauna", "San Millán/Donemiliaga"
                                    ]
                                },
                                {
                                    nom: "Cuadrilla de la Llanada Alavesa",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Arrazua-Ubarrundia", "Barrundia", "Elburgo/Burgelu", "Iruraiz-Gauna",
                                        "San Millán/Donemiliaga"
                                    ]
                                },
                                {
                                    nom: "Cuadrilla de Rioja Alavesa",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Baños de Ebro/Mañueta", "Elciego", "Elvillar/Bilar", "Kripan",
                                        "Labastida/Bastida", "Laguaridia", "Lanciego/Lantziego", "Leza",
                                        "Moreda de Álava/Moreda Araba", "Navaridas", "Oyón-Oion", "Samaniego",
                                        "Villabuena de Álava/Eskuernaga", "Yécora/Iekora"
                                    ]
                                },
                                {
                                    nom: "Cuadrilla de Zuia",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Zuia"
                                    ]
                                }
                            ]
                        },
                        gipuzkoa: {
                            nom: "Gipuzkoa",
                            comarques: [
                                {
                                    nom: "Donostialdea",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Astigarraga", "Donostia/San Sebastián", "Errenteria", "Hernani",
                                        "Lasarte-Oria", "Lezo", "Oiartzun", "Pasaia",
                                        "Usurbil"
                                    ]
                                },
                                {
                                    nom: "Bidasoa-Txingudi",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Hondarribia", "Irun"
                                    ]
                                },
                                {
                                    nom: "Debabarrena",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Deba", "Eibar", "Elgoibar", "Mendaro",
                                        "Mutriku", "Soraluze-Placencia de las Armas"
                                    ]
                                },
                                {
                                    nom: "Debagoiena",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Antzuola", "Aretxabaleta", "Arrasate/Mondragón", "Bergara",
                                        "Eskoriatza", "Legazpi", "Oñati"
                                    ]
                                },
                                {
                                    nom: "Goierri",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Altzaga", "Arama", "Ataun", "Beasain",
                                        "Beizama", "Ezkio-Itsaso", "Gabiria", "Gaintza",
                                        "Idiazabal", "Itsasondo", "Lazkao", "Legorreta",
                                        "Mutiloa", "Olaberria", "Ordizia", "Ormaiztegi",
                                        "Segura", "Zaldibia", "Zerain", "Zumarraga"
                                    ]
                                },
                                {
                                    nom: "Tolosaldea",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Alegia", "Alkiza", "Amezketa", "Anoeta",
                                        "Asteasu", "Baliarrain", "Berastegi", "Elduain",
                                        "Gaztelu", "Hernialde", "Ibarra", "Ikaztegieta",
                                        "Irura", "Larraul", "Leaburu", "Lizartza",
                                        "Orendain", "Orexa", "Tolosa", "Zizurkil"
                                    ]
                                },
                                {
                                    nom: "Urola Erdia",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Azkoitia", "Azpeitia", "Beizama", "Errezil",
                                        "Zestoa", "Zumaia"
                                    ]
                                },
                                {
                                    nom: "Urola Kosta",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Aia", "Getaria", "Orio", "Zarautz",
                                        "Zumaia"
                                    ]
                                }
                            ]
                        },
                        bizkaia: {
                            nom: "Bizkaia",
                            comarques: [
                                {
                                    nom: "Gran Bilbao",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Abanto y Ciérvana-Abanto Zierbena", "Alonsotegi", "Arrigorriaga", "Barakaldo",
                                        "Basauri", "Bedia", "Berango", "Bilbao",
                                        "Derio", "Erandio", "Etxebarri", "Galdakao",
                                        "Getxo", "Leioa", "Loiu", "Muskiz",
                                        "Ortuella", "Portugalete", "Santurtzi", "Sestao",
                                        "Sondika", "Sopela", "Zalla", "Zamudio"
                                    ]
                                },
                                {
                                    nom: "Margen Izquierda",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Barakaldo", "Erandio", "Leioa", "Portugalete",
                                        "Santurtzi", "Sestao", "Sopela"
                                    ]
                                },
                                {
                                    nom: "Margen Derecha-Uribe Kosta",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Barrika", "Berango", "Getxo", "Gorliz",
                                        "Lemoiz", "Leioa", "Plentzia", "Sopela"
                                    ]
                                },
                                {
                                    nom: "Encartaciones",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Artzentales", "Balmaseda", "Gordexola", "Güeñes",
                                        "Karrantza Harana/Valle de Carranza", "Lanestosa", "Sopuerta", "Trucios-Turtzioz",
                                        "Zalla"
                                    ]
                                },
                                {
                                    nom: "Duranguesado",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Abadiño", "Amorebieta-Etxano", "Atxondo", "Berriz",
                                        "Durango", "Elorrio", "Iurreta", "Izurtza",
                                        "Mañaria", "Otxandio", "Zaldibar"
                                    ]
                                },
                                {
                                    nom: "Busturialdea-Urdaibai",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Ajangiz", "Arratzu", "Bakio", "Bermeo",
                                        "Busturia", "Ea", "Elantxobe", "Errigoiti",
                                        "Forua", "Gautegiz Arteaga", "Gernika-Lumo", "Ibarrangelu",
                                        "Kortezubi", "Mendata", "Morga", "Mundaka",
                                        "Murueta", "Muxika", "Sukarrieta"
                                    ]
                                },
                                {
                                    nom: "Arratia-Nervión",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Arakaldo", "Arantzazu", "Areatza", "Artea",
                                        "Bedia", "Dima", "Igorre", "Lemoa",
                                        "Orozko", "Zeanuri", "Zeberio"
                                    ]
                                }
                            ]
                        }
                    }
                },
                larioja: {
                    nom: "La Rioja",
                    columnes: ["Pais", "Comunitat Autonoma", "Provincia", "Comarca", "Poble", "Registres", "Opcions"],
                    provincies: {
                        la_rioja: {
                            nom: "La Rioja",
                            comarques: [
                                {
                                    nom: "Rioja Alta",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Ábalos", "Alesanco", "Alesón", "Alfaro",
                                        "Anguciana", "Anguiano", "Arenzana de Abajo", "Arenzana de Arriba",
                                        "Azofra", "Badarán", "Bañares", "Baños de Río Tobía",
                                        "Berceo", "Bobadilla", "Briñas", "Briones",
                                        "Casalarreina", "Castañares de Rioja", "Cenicero", "Cidamón",
                                        "Cirueña", "Cordovín", "Ezcaray", "Foncea",
                                        "Fonzaleche", "Galbárruli", "Gimileo", "Haro",
                                        "Herce", "Herramélluri", "Hormilla", "Hormilleja",
                                        "Huércanos", "Laguardia", "Lardero", "Leiva",
                                        "Logroño", "Manzanares de Rioja", "Navarrete", "Nájera",
                                        "Ochánduri", "Ollauri", "Oyón", "Rodezno",
                                        "Sajazarra", "San Asensio", "San Millán de la Cogolla", "San Vicente de la Sonsierra",
                                        "Santo Domingo de la Calzada", "Sojuela", "Sorzano", "Tirgo",
                                        "Tormantos", "Treviana", "Valgañón", "Ventosa",
                                        "Villalba de Rioja", "Zarratón"
                                    ]
                                },
                                {
                                    nom: "Rioja Media",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Agoncillo", "Alcanadre", "Aldeanueva de Ebro", "Arnedo",
                                        "Arrúbal", "Ausejo", "Autol", "Ausejo",
                                        "Calahorra", "Corera", "El Villar de Arnedo", "Elciego",
                                        "El Redal", "Fuenmayor", "Galilea", "Huércanos",
                                        "Lagunilla del Jubera", "Lardero", "Logroño", "Nalda",
                                        "Navarrete", "Pradejón", "Ribafrecha", "Rincón de Soto",
                                        "San Asensio", "Soto en Cameros", "Torremontalbo", "Uruñuela",
                                        "Ventosa", "Villamediana de Iregua"
                                    ]
                                },
                                {
                                    nom: "Rioja Baja",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Aguilar del Río Alhama", "Alfaro", "Arnedo", "Autol",
                                        "Calahorra", "Cervera del Río Alhama", "Cornago", "Grávalos",
                                        "Igea", "Pradejón", "Rincón de Soto", "Tudelilla"
                                    ]
                                },
                                {
                                    nom: "Comarca de Logroño",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Agoncillo", "Albelda de Iregua", "Alberite", "Alcanadre",
                                        "Arrúbal", "Ausejo", "Cenicero", "Clavijo",
                                        "Corera", "El Redal", "Fuenmayor", "Galilea",
                                        "Lagunilla del Jubera", "Lardero", "Logroño", "Medrano",
                                        "Murillo de Río Leza", "Nalda", "Navarrete", "Ocón",
                                        "Ribafrecha", "Robres del Castillo", "San Asensio", "Sojuela",
                                        "Sorzano", "Torremontalbo", "Ventosa", "Villamediana de Iregua"
                                    ]
                                },
                                {
                                    nom: "Comarca de Nájera",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Alesanco", "Alesón", "Arenzana de Abajo", "Arenzana de Arriba",
                                        "Azofra", "Badarán", "Bañares", "Baños de Río Tobía",
                                        "Berceo", "Bobadilla", "Camprovín", "Canales de la Sierra",
                                        "Cañas", "Cárdenas", "Castroviejo", "Cordovín",
                                        "Hormilla", "Hormilleja", "Huércanos", "Manzanares de Rioja",
                                        "Matute", "Nájera", "Navarrete", "Pedroso",
                                        "San Millán de la Cogolla", "Santa Coloma", "Tricio", "Villar de Torre",
                                        "Villarejo"
                                    ]
                                },
                                {
                                    nom: "Comarca de Santo Domingo de la Calzada",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Ajamil de Cameros", "Anguiano", "Baños de Río Tobía", "Berceo",
                                        "Brieva de Cameros", "Canales de la Sierra", "Castroviejo", "Ezcaray",
                                        "Gallinero de Cameros", "Gimileo", "Grañón", "Herce",
                                        "Herramélluri", "Leiva", "Lumbreras", "Manzanares de Rioja",
                                        "Mansilla de la Sierra", "Matute", "Ochánduri", "Pazuengos",
                                        "Pinillos", "Pradillo", "Rabanera", "Regumiel de la Sierra",
                                        "Sajazarra", "San Millán de la Cogolla", "San Torcuato", "Santo Domingo de la Calzada",
                                        "Tobía", "Valgañón", "Villarroya"
                                    ]
                                },
                                {
                                    nom: "Comarca de Cervera",
                                    tipus: "Comarca",
                                    municipis: [
                                        "Aguilar del Río Alhama", "Alfaro", "Arnedo", "Autol",
                                        "Calahorra", "Cervera del Río Alhama", "Cornago", "Grávalos",
                                        "Igea", "Pradejón", "Rincón de Soto", "Tudelilla"
                                    ]
                                }
                            ]
                        }
                    }
                },
                ceuta: {
                    nom: "Ceuta",
                    columnes: ["Pais", "Comunitat Autonoma", "Provincia", "Comarca", "Poble", "Registres", "Opcions"],
                    provincies: {
                        ceuta:{
                            nom: "Ceuta",
                            tipus: "Comarca",
                            comarques: [
                                {
                                    nom: "Ceuta",
                                    municipis: [
                                        "Ceuta"
                                    ]
                                }
                            ]
                        }
                    }
                },
                melilla: {
                    nom: "Melilla",
                    columnes: ["Pais", "Comunitat Autonoma", "Provincia", "Comarca", "Poble", "Registres", "Opcions"],
                    provincies: {
                        melilla: {
                            nom: "Melilla",
                            tipus: "Comarca",
                            comarques: [
                                {
                                    nom: "Melilla",
                                    municipis: [
                                        "Melilla"
                                    ]
                                }
                            ]
                        }
                    }
                }
            }
        }
    };

    function generarDadesMunicipis() {
        const municipis = [];

        Object.values(dadesLocalitzacio.espanya.comunitats).forEach(comunitat => {
            if (!comunitat.provincies) return;

            Object.entries(comunitat.provincies).forEach(([provKey, provincia]) => {
                // Detectem quin tipus de nivell hi ha sota de cada provincia
                let nivells = [];

                if (provincia.mancomunitats) nivells = [...nivells, ...provincia.mancomunitats];
                if (provincia.comarques) nivells = [...nivells, ...provincia.comarques];
                if (provincia.concellos) nivells = [...nivells, ...provincia.concellos];
                if (provincia.illots) nivells = [...nivells, ...provincia.illots];

                nivells.forEach(nivell => {
                    if (!Array.isArray(nivell.municipis)) return;
                    nivell.municipis.forEach(poble => {
                        const registres = Math.floor(Math.random() * 100);
                        const indexat = Math.min(100, Math.round((Math.random() * 100) + 30));

                        municipis.push({
                            pais: "Espanya",
                            comunitat: comunitat.nom,
                            provincia: provincia.nom,
                            mancomunitat: nivell.nom,
                            tipus_nivell: nivell.tipus || 'mancomunitat',
                            poble: poble,
                            registres: registres,
                            indexat: indexat
                        });
                    });
                });
            });
        });

        return municipis;
    }

    let dadesTaula = [];
    let dadesFiltrades = [];

    function filtrarDades() {
        const pais = paisSelect.value === 'espanya' ? 'Espanya' : '';
        const comunitat = normalitzar(comunitatSelect.value);
        const provincia = normalitzar(provinciaSelect.value);
        const mancomunitat = normalitzar(mancomunitatSelect.value);
        const municipi = normalitzar(municipiSelect.value);

        return dadesTaula.filter(reg => {
            const regNorm = {
                pais: normalitzar(reg.pais),
                comunitat: normalitzar(reg.comunitat),
                provincia: normalitzar(reg.provincia),
                mancomunitat: normalitzar(reg.mancomunitat),
                poble: normalitzar(reg.poble),
            };

            return (
                (!pais || regNorm.pais.includes(normalitzar(pais))) &&
                (!comunitat || regNorm.comunitat.includes(comunitat)) &&
                (!provincia || regNorm.provincia.includes(provincia)) &&
                (!mancomunitat || regNorm.mancomunitat.includes(mancomunitat)) &&
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
                <td>${mancomunitat}</td>
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
                    <a href="https://www.google.com/maps/search/?api=1&query= ${encodeURIComponent(poble + ', ' + provincia + ', ' + comunitat)}" target="_blank" title="Google Maps"><i class="fas fa-map-marked-alt"></i></a>
                    <a href="https://www.openstreetmap.org/search?query= ${encodeURIComponent(poble + ', ' + provincia + ', ' + comunitat)}" target="_blank" title="OpenStreetMap"><i class="fas fa-map-signs"></i></a>
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
                carregarTaula(paginaActual); // Passa explícitament la pàgina
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
                carregarTaula(paginaActual); // Passa explícitament la pàgina
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
            mancomunitatGrup.style.display = 'none';
            municipiGrup.style.display = 'none';
            comunitatSelect.innerHTML = '<option value="">-- Selecciona comunitat --</option>';
            provinciaSelect.innerHTML = '<option value="">-- Selecciona provincia --</option>';
            mancomunitatSelect.innerHTML = '<option value="">-- Selecciona opció --</option>';
            municipiSelect.innerHTML = '<option value="">-- Selecciona municipi --</option>';
            paginaActual = 1;
            carregarTaula();
            return;
        }

        const comunitats = Object.values(dadesLocalitzacio.espanya.comunitats).map(c => ({ nom: c.nom }));
        omplirSelect(comunitatSelect, comunitats);
        comunitatGrup.style.display = 'block';
        provinciaGrup.style.display = 'none';
        mancomunitatGrup.style.display = 'none';
        municipiGrup.style.display = 'none';
        provinciaSelect.innerHTML = '<option value="">-- Selecciona provincia --</option>';
        mancomunitatSelect.innerHTML = '<option value="">-- Selecciona opció --</option>';
        municipiSelect.innerHTML = '<option value="">-- Selecciona municipi --</option>';
        paginaActual = 1;
        carregarTaula();
    }

    function mostrarProvincies() {
        const comunitatId = comunitatSelect.value;
        const comunitat = Object.values(dadesLocalitzacio.espanya.comunitats).find(c => 
            normalitzar(c.nom) === normalitzar(comunitatId)
        );
        if (!comunitat || !comunitat.provincies) {
            provinciaGrup.style.display = 'none';
            mancomunitatGrup.style.display = 'none';
            municipiGrup.style.display = 'none';
            provinciaSelect.innerHTML = '<option value="">-- Selecciona provincia --</option>';
            mancomunitatSelect.innerHTML = '<option value="">-- Selecciona opció --</option>';
            municipiSelect.innerHTML = '<option value="">-- Selecciona municipi --</option>';
            paginaActual = 1;
            carregarTaula();
            return;
        }

        const provincies = Object.values(comunitat.provincies).map(p => ({ nom: p.nom }));
        omplirSelect(provinciaSelect, provincies);
        provinciaGrup.style.display = 'block';
        mancomunitatGrup.style.display = 'none';
        municipiGrup.style.display = 'none';
        mancomunitatSelect.innerHTML = '<option value="">-- Selecciona opció --</option>';
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
            mancomunitatSelect.innerHTML = '<option value="">-- Selecciona opció --</option>';
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
            mancomunitatSelect.innerHTML = '<option value="">-- Selecciona opció --</option>';
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

        omplirSelect(mancomunitatSelect, mancomunitats.map(mc => ({ nom: mc.nom })));
        mancomunitatGrup.style.display = 'block';
        municipiGrup.style.display = 'none';
        municipiSelect.innerHTML = '<option value="">-- Selecciona municipi --</option>';
        paginaActual = 1;
        carregarTaula();
    }

    function mostrarMunicipis() {
        const comunitatId = comunitatSelect.value;
        const provinciaId = provinciaSelect.value;
        const mancomunitatId = normalitzar(mancomunitatSelect.value);

        const comunitat = Object.values(dadesLocalitzacio.espanya.comunitats).find(c => 
            normalitzar(c.nom) === normalitzar(comunitatId)
        );

        if (!comunitat || !comunitat.provincies) {
            municipiGrup.style.display = 'none';
            paginaActual = 1;
            carregarTaula();
            return;
        }

        const provincia = Object.values(comunitat.provincies).find(p => 
            normalitzar(p.nom) === normalitzar(provinciaId)
        );

        if (!provincia) {
            municipiGrup.style.display = 'none';
            paginaActual = 1;
            carregarTaula();
            return;
        }

        // Busquem a quin nivell pertany la selecció actual
        let mancomunitatData = null;

        for (const nivell of ['mancomunitats', 'comarques', 'illots', 'concellos']) {
            if (Array.isArray(provincia[nivell])) {
                mancomunitatData = provincia[nivell].find(m => 
                    normalitzar(m.nom) === mancomunitatId
                );
                if (mancomunitatData) break;
            }
        }

        if (!mancomunitatData || !Array.isArray(mancomunitatData.municipis)) {
            municipiGrup.style.display = 'none';
            paginaActual = 1;
            carregarTaula();
            return;
        }

        omplirSelect(municipiSelect, mancomunitatData.municipis.map(m => ({ nom: m })));
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
        mostrarMancomunitats();
        carregarTaula();
    });

    mancomunitatSelect.addEventListener('change', () => {
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
        // Restableix tots els camps menys el pais (que ja sabem que és Espanya)
        paisSelect.value = 'espanya';
        mostrarComunitats();

        // Aplica segons el tipus de resultat
        if (resultat.tipus === 'comunitat') {
            comunitatSelect.value = resultat.valor;
            mostrarProvincies();
        }

        if (resultat.tipus === 'provincia') {
            comunitatSelect.value = resultat.comunitatId;
            mostrarProvincies();
            provinciaSelect.value = resultat.valor;
            mostrarMancomunitats();
        }

        if (resultat.tipus === 'mancomunitat') {
            comunitatSelect.value = resultat.comunitatId;
            mostrarProvincies();
            provinciaSelect.value = resultat.provinciaId;
            mostrarMancomunitats();
            mancomunitatSelect.value = normalitzar(resultat.nom);
            mostrarMunicipis();
        }

        if (resultat.tipus === 'municipi') {
            comunitatSelect.value = resultat.comunitatId;
            mostrarProvincies();
            provinciaSelect.value = resultat.provinciaId;
            mostrarMancomunitats();
            mancomunitatSelect.value = normalitzar(resultat.mancomunitatNom);
            mostrarMunicipis();
            municipiSelect.value = normalitzar(resultat.nom);
        }

        paginaActual = 1;
        carregarTaula(paginaActual);
    }

    if (campCercaAvançada && resultatsCercaAvançada) {
        campCercaAvançada.addEventListener('input', function () {
            const term = normalitzar(this.value);
            resultatsCercaAvançada.innerHTML = '';
            if (term.length < 2) return;

            const resultats = [];

            // Buscar en comunitats
            Object.entries(dadesLocalitzacio.espanya.comunitats).forEach(([id, comunitat]) => {
                if (normalitzar(comunitat.nom).includes(term)) {
                    resultats.push({ tipus: 'comunitat', nom: comunitat.nom, valor: id });
                }

                // Buscar en provincies
                if (comunitat.provincies) {
                    Object.entries(comunitat.provincies).forEach(([idProv, provincia]) => {
                        if (normalitzar(provincia.nom).includes(term)) {
                            resultats.push({
                                tipus: 'provincia',
                                nom: provincia.nom,
                                valor: idProv,
                                comunitatId: id
                            });
                        }

                        // Buscar en mancomunitats
                        if (provincia.mancomunitats) {
                            provincia.mancomunitats.forEach(mancomunitat => {
                                if (normalitzar(mancomunitat.nom).includes(term)) {
                                    resultats.push({
                                        tipus: 'mancomunitat',
                                        nom: mancomunitat.nom,
                                        comunitatId: id,
                                        provinciaId: idProv
                                    });
                                }

                                // Buscar en municipis
                                if (Array.isArray(mancomunitat.municipis)) {
                                    mancomunitat.municipis.forEach(municipi => {
                                        if (normalitzar(municipi).includes(term)) {
                                            resultats.push({
                                                tipus: 'municipi',
                                                nom: municipi,
                                                comunitatId: id,
                                                provinciaId: idProv,
                                                mancomunitatNom: mancomunitat.nom
                                            });
                                        }
                                    });
                                }
                            });
                        }
                    });
                }
            });

            // Mostrar resultats
            if (resultats.length > 0) {
                resultatsCercaAvançada.style.display = 'block';
                resultats.forEach(res => {
                    const li = document.createElement('li');
                    li.innerHTML = `<span>${res.nom}</span><small>${res.tipus}</small>`;
                    li.addEventListener('click', () => {
                        aplicarResultatCerca(res);
                        resultatsCercaAvançada.style.display = 'none';
                    });
                    resultatsCercaAvançada.appendChild(li);
                });
            } else {
                resultatsCercaAvançada.style.display = 'none';
            }
        });

        // Tancar si es fa clic fora
        document.addEventListener('click', function (e) {
            if (!campCercaAvançada.contains(e.target) && !resultatsCercaAvançada.contains(e.target)) {
                resultatsCercaAvançada.style.display = 'none';
            }
        });
    }
});