-- SQLite.sql
CREATE TABLE IF NOT EXISTS usuaris (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom TEXT NOT NULL,
    cognom1 TEXT,
    cognom2 TEXT,
    municipi TEXT,
    arquevisbat TEXT,
    nom_complet TEXT,
    pagina TEXT,
    llibre TEXT,
    any TEXT,
    data_naixement DATE,
    data_bateig DATE,
    data_defuncio DATE,
    ofici TEXT,
    estat_civil TEXT
);

CREATE TABLE IF NOT EXISTS relacions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    usuari_id INTEGER NOT NULL,
    tipus_relacio TEXT NOT NULL, -- ex: "pare", "mare", "casat", etc.
    nom TEXT,
    cognom1 TEXT,
    cognom2 TEXT,
    municipi TEXT,
    ofici TEXT,
    data_matrimoni TEXT,
    FOREIGN KEY(usuari_id) REFERENCES usuaris(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS usuaris_possibles_duplicats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom TEXT NOT NULL,
    cognom1 TEXT NOT NULL,
    cognom2 TEXT NOT NULL,
    municipi TEXT,
    arquevisbat TEXT,
    nom_complet TEXT,
    pagina TEXT,
    llibre TEXT,
    any TEXT
);

-- TAULA PAISOS
CREATE TABLE IF NOT EXISTS paisos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom TEXT NOT NULL, -- Nom oficial del país ex: "Espanya", "França"
    codi_iso2 TEXT(2) UNIQUE, -- Codi ISO 3166-1 alpha-2 ex: ES, FR, US, CA...
    codi_iso3 TEXT(3) UNIQUE, -- Codi ISO 3166-1 alpha-3 ex: ESP, FRA, USA
    codi_pais_num TEXT, -- Codi numèric ISO 3166-1
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Data de creació del registre
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Última modificació del registre
);

-- TAULA NIVELLS ADMINISTRATIUS
CREATE TABLE IF NOT EXISTS nivells_administratius (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pais_id INTEGER REFERENCES paisos(id),  -- País al qual pertany aquest nivell
    nivel INTEGER CHECK(nivel BETWEEN 1 AND 7),   -- Jerarquia del nivell (ex: 1=continent, 2=país, 3=comunitat, 4=província, 5=comarca, 6=municipi, 7=barri)
    nom_nivell TEXT,                          -- Ex: Catalunya, Lleida, Urgell, etc.
    tipus_nivell TEXT,                        -- Tipus específic: Regió, Província, Comarca, Municipi, Barri, etc.
    codi_oficial TEXT,                        -- Codi oficial local (ex: INE, NUTS, etc.)
    altres TEXT, -- Informació addicional en format JSON (ex: {"codi_INE": "25098", "codi_NUTS": "ES511"}
    estat TEXT CHECK(estat IN ('actiu', 'inactiu', 'fusionat', 'abolit')) DEFAULT 'actiu',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Quan s'ha creat el nivell
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Última vegada que ha canviat
);

-- TAULA MUNICIPIS
CREATE TABLE IF NOT EXISTS municipis (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom TEXT NOT NULL, -- Nom oficial actual del municipi
    municipi_id INTEGER REFERENCES municipis(id), -- Opcional: si el poble pertany a un altre municipi (ex: barri d'un municipi gran)
    tipus TEXT NOT NULL,                        -- Tipus de territori: poblatge, ciutat, barri, nucli, etc.
    -- Relacions jeràrquiques amb nivells administratius (de 1 a 7)
    nivell_administratiu_id_1 INTEGER REFERENCES nivells_administratius(id), -- País
    nivell_administratiu_id_2 INTEGER REFERENCES nivells_administratius(id), -- Regió / Comunitat Autònoma
    nivell_administratiu_id_3 INTEGER REFERENCES nivells_administratius(id), -- Província
    nivell_administratiu_id_4 INTEGER REFERENCES nivells_administratius(id), -- Comarca / àrea local
    nivell_administratiu_id_5 INTEGER REFERENCES nivells_administratius(id), -- Àrea local
    nivell_administratiu_id_6 INTEGER REFERENCES nivells_administratius(id), -- Municipi
    nivell_administratiu_id_7 INTEGER REFERENCES nivells_administratius(id), -- Barri
    codi_postal TEXT, -- Codi postal associat
    latitud REAL, -- Latitud GPS
    longitud REAL, -- Longitud GPS
    what3words TEXT, -- Ex: ///three.words.example
    web TEXT, -- URL de l’ajuntament o entitat local
    wikipedia TEXT, -- URL o títol de pàgina Wikipedia
    altres TEXT,                               -- JSON amb informació adicional
    estat TEXT CHECK(estat IN ('actiu', 'inactiu', 'abandonat')) DEFAULT 'actiu',
    data_creacio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ultima_modificacio TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS noms_historics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_municipi INTEGER NOT NULL REFERENCES municipis(id),
    nom TEXT NOT NULL, -- Nom antic o anterior
    any_inici INTEGER, -- Any d'inici del nom (opcional)
    any_fi INTEGER, -- Any final del nom (null = encara vigent)
    pais_regne TEXT, -- Regne o estat al moment del nom ex: "Regne d'Aragó", "Imperi Romà"
    distribucio_geografica TEXT, -- Sistema administratiu anterior: vegueria, diòcesi, baronia, etc.
    font TEXT, -- Font o documentació on s'ha trobat aquest nom
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Moment de gravació d'aquest registre
);

CREATE TABLE IF NOT EXISTS arquebisbats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom TEXT NOT NULL UNIQUE,
    tipus_entitat TEXT CHECK(tipus_entitat IN ('arquebisbat', 'bisbat', 'diocesi', 'viscomtat', 'vegueria', 'altres')), -- Tipus d'entitat eclesiàstica
    web TEXT,
    web_arxiu TEXT,
    web_wikipedia TEXT,
    territori TEXT, -- Ex: Àmbit geogràfic (ex: Catalunya Nord, Catalunya del Sud)
    autoritat_superior TEXT, -- Bisbat superior o arquebisbat pare
    observacions TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS arquebisbats_municipi (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_municipi INTEGER NOT NULL REFERENCES municipis(id),
    id_arquevisbat INTEGER NOT NULL REFERENCES arquebisbats(id),
    any_inici INTEGER, -- Quan va passar a formar part d’aquest arquebisbat
    any_fi INTEGER, -- Si va deixar de pertanyer-hi
    motiu TEXT, -- Motiu del canvi (ex: reforma administrativa, decrets reials, etc.)
    font TEXT, -- Font del canvi (ex: document eclesiàstic, decrets, arxius)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS codis_postals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_municipi INTEGER NOT NULL REFERENCES municipis(id),
    codi_postal TEXT NOT NULL,
    zona TEXT, -- Opcional: barri, sector, etc.
    desde DATE,
    fins DATE,
    font TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS relacio_comarca_provincia (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_municipi INTEGER NOT NULL REFERENCES municipis(id),
    comarca TEXT,
    provincia TEXT,
    any_inici INTEGER,
    any_fi INTEGER,
    font TEXT,
    observacions TEXT
);

CREATE TABLE IF NOT EXISTS llibres (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Relació amb l'arquevisbat (mitjançant FK)
    arquevisbat_id INTEGER NOT NULL,
    FOREIGN KEY(arquevisbat_id) REFERENCES arquebisbats(id) ON DELETE CASCADE,

    -- Relació amb el municipi (si hi ha una taula 'municipis')
    municipi_id INTEGER NOT NULL,
    FOREIGN KEY(municipi_id) REFERENCES municipis(id) ON DELETE RESTRICT,

    nom_esglesia TEXT,                     -- ex: "Sant Jaume Apòstol"
    
    -- Codi identificador únic (de cada sistema)
    codi_digital TEXT,                    -- ex: "0000013893" (Tarragona)
    codi_fisic TEXT,                      -- ex: "UD: 05 / UI: 423" (Urgell)
    
    -- Metadades del llibre
    titol TEXT,
    cronologia TEXT,                      -- ex: "21.10.1600 - 10.01.1663"
    volum TEXT,                           -- ex: "Foli partit de gran tamany"
    abat TEXT,
    contingut TEXT,
    llengua TEXT,                          -- ex: "Llatí", "Català", "Castellà"
    requeriments_tecnics TEXT,
    unitat_catalogacio TEXT,               -- ex: "P-ABR-123"
    unitat_instalacio TEXT,                -- ex: "CAIXA-45"
    pagines INT, -- numero de pagines totals del llibre
    
    -- URL digital
    url_base TEXT,                         -- ex: "https://arxiuenlinia.ahat.cat/Document/ "
    url_imatge_prefix TEXT DEFAULT "#imatge-", -- prefix comú per afegir pàgina
    url_digital TEXT AS (url_base || codi_digital || url_imatge_prefix || pagina) STORED, -- generat automàticament
    
    -- Pàgina específica (si es vol navegar directe a una pàgina concreta)
    pagina TEXT                            -- ex: "7", "05-0023" (Urgell)

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index per accelerar busquedes

-- Per buscar ràpidament pel codi postal
CREATE INDEX IF NOT EXISTS idx_codi_postal ON municipis(codi_postal);
-- Per cercar pel nom
CREATE INDEX IF NOT EXISTS idx_nom_municipi ON municipis(nom);
-- Per buscar nivells pel seu tipus
CREATE INDEX IF NOT EXISTS idx_tipus_nivell ON nivells_administratius(tipus_nivell);

CREATE INDEX IF NOT EXISTS idx_llibres_arquevisbat ON llibres(arquevisbat_id);
CREATE INDEX IF NOT EXISTS idx_llibres_municipi ON llibres(municipi_id);

-- Índex compost per millorar la cerca de duplicats i cerques combinades
CREATE INDEX idx_usuaris_cognoms_any_llibre_pagina ON usuaris(cognom1, cognom2, any, llibre, pagina);

-- Cerca per cognoms i nom (per coincidències exactes)
CREATE INDEX idx_usuaris_nom_complet ON usuaris(nom_complet);

-- Útil per cerca de persones per municipi i any (ex: nascuts al mateix lloc i època)
CREATE INDEX idx_usuaris_municipi_any ON usuaris(municipi, any);

-- Cercar per ofici o estat civil
CREATE INDEX idx_usuaris_ofici ON usuaris(ofici);
CREATE INDEX idx_usuaris_estat_civil ON usuaris(estat_civil);


-- Dades preinsertades
-- Inserció de països a la taula 'paisos'
INSERT INTO paisos (nom, codi_iso2, codi_iso3, codi_pais_num, capital, poblacio) VALUES
('Espanya', 'ES', 'ESP', '724', 'Madrid', 47500297),
('França', 'FR', 'FRA', '250', 'París', 67405000),
('Itàlia', 'IT', 'ITA', '380', 'Roma', 60244639),
('Portugal', 'PT', 'PRT', '620', 'Lisboa', 10309538),
('Alemanya', 'DE', 'DEU', '276', 'Berlín', 83122573),
('Regne Unit', 'GB', 'GBR', '826', 'Londres', 67081234),
('Suïssa', 'CH', 'CHE', '756', 'Berna', 8601566),
('Andorra', 'AD', 'AND', '20', 'Andorra la Vella', 77265),
('Belgica', 'BE', 'BEL', '56', 'Brussel·les', 11555995),
('Països Baixos', 'NL', 'NLD', '528', 'Amsterdam', 17450028),
('Luxemburg', 'LU', 'LUX', '442', 'Luxemburg', 6261000),
('Polònia', 'PL', 'POL', '616', 'Warsaw', 37974903),
('República Txeca', 'CZ', 'CZE', '203', 'Praga', 10708981),
('Slovàquia', 'SK', 'SVK', '703', 'Bratislava', 5459642),
('Hongria', 'HU', 'HUN', '348', 'Budapest', 9749790),
('Àustria', 'AT', 'AUT', '40', 'Viena', 9043056),
('Suècia', 'SE', 'SWE', '752', 'Estocolm', 10437732),
('Noruega', 'NO', 'NOR', '578', 'Oslo', 5408443),
('Dinamarca', 'DK', 'DNK', '208', 'Copenhaguen', 5818514),
('Finlàndia', 'FI', 'FIN', '246', 'Hèlsinki', 5539413),
('Grècia', 'GR', 'GRC', '300', 'Atenes', 10724599),
('Irlanda', 'IE', 'IRL', '372', 'Dublín', 5105097),
('Bulgària', 'BG', 'BGR', '100', 'Sofia', 6948445),
('Romania', 'RO', 'ROU', '642', 'Bucarest', 19286805),
('Croàcia', 'HR', 'HRV', '191', 'Zagreb', 4067340),
('Eslovènia', 'SI', 'SVN', '705', 'Liubliana', 2103767),
('Eslovàquia', 'SK', 'SVK', '703', 'Bratislava', 5459642),
('Turquia', 'TR', 'TUR', '792', 'Ankara', 84680273),
('Canadà', 'CA', 'CAN', '124', 'Ottawa', 38005238),
('Estats Units', 'US', 'USA', '840', 'Washington DC', 331449237),
('Mèxic', 'MX', 'MEX', '484', 'Ciutat de Mèxic', 128932753),
('Brasil', 'BR', 'BRA', '76', 'Brasília', 212559417),
('Argentina', 'AR', 'ARG', '32', 'Buenos Aires', 45195508),
('Xile', 'CL', 'CHL', '152', 'Santiago', 19116209),
('Perú', 'PE', 'PER', '604', 'Lima', 32971854),
('Colòmbia', 'CO', 'COL', '170', 'Bogotà', 50882891),
('Uruguai', 'UY', 'URY', '858', 'Montevideo', 3470472),
('Paraguai', 'PY', 'PRY', '600', 'Asunción', 7132538),
('Veneçuela', 'VE', 'VEN', '862', 'Caracas', 28515829),
('Rússia', 'RU', 'RUS', '643', 'Moscou', 144103215),
('Japó', 'JP', 'JPN', '392', 'Tòquio', 125847412),
('Xina', 'CN', 'CHN', '156', 'Pequin', 1439323776),
('Índia', 'IN', 'IND', '356', 'Nova Delhi', 1393409098),
('Marroc', 'MA', 'MAR', '504', 'Rabat', 37800000),
('França', 'FR', 'FRA', '250', 'París', 67405000);