-- Desactivo les claus foranes per pervindre errors durant la creació
-- PRAGMA foreign_keys = OFF;

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
    altres TEXT,                              -- Informació addicional en format JSON (ex: {"codi_INE": "25098", "codi_NUTS": "ES511"}
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
    arquevisbat_id INTEGER NOT NULL,
    municipi_id INTEGER NOT NULL,
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
    pagines INT,                           -- numero de pagines totals del llibre
    -- URL digital
    url_base TEXT,                         -- ex: "https://arxiuenlinia.ahat.cat/Document/ "
    url_imatge_prefix TEXT DEFAULT "#imatge-", -- prefix comú per afegir pàgina
    pagina TEXT,                            -- Pàgina específica (si es vol navegar directe a una pàgina concreta) ex: "7", "05-0023" (Urgell)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(arquevisbat_id) REFERENCES arquebisbats(id) ON DELETE CASCADE,
    FOREIGN KEY(municipi_id) REFERENCES municipis(id) ON DELETE RESTRICT
);

-- Index per accelerar busquedes

-- Per buscar ràpidament pel codi postal
CREATE INDEX IF NOT EXISTS idx_codi_postal ON municipis(codi_postal);
-- Per cercar pel nom
CREATE INDEX IF NOT EXISTS idx_nom_municipi ON municipis(nom);
-- Per buscar nivells pel seu tipus
CREATE INDEX IF NOT EXISTS idx_tipus_nivell ON nivells_administratius(tipus_nivell);

--CREATE INDEX IF NOT EXISTS idx_llibres_arquevisbat ON llibres(arquevisbat_id);
--CREATE INDEX IF NOT EXISTS idx_llibres_municipi ON llibres(municipi_id);

-- Índex compost per millorar la cerca de duplicats i cerques combinades
-- CREATE INDEX idx_usuaris_cognoms_any_llibre_pagina ON usuaris(cognom1, cognom2, any, llibre, pagina); -- error executant SQLite.sql: index idx_usuaris_cognoms_any_llibre_pagina already exists

-- Cerca per cognoms i nom (per coincidències exactes)
CREATE INDEX IF NOT EXISTS idx_usuaris_nom_complet ON usuaris(nom_complet);

-- Útil per cerca de persones per municipi i any (ex: nascuts al mateix lloc i època)
CREATE INDEX IF NOT EXISTS idx_usuaris_municipi_any ON usuaris(municipi, any);

-- Cercar per ofici o estat civil
CREATE INDEX IF NOT EXISTS idx_usuaris_ofici ON usuaris(ofici);
CREATE INDEX IF NOT EXISTS idx_usuaris_estat_civil ON usuaris(estat_civil);

-- Reactivo les claus foranes per pervindre errors durant la creació
-- PRAGMA foreign_keys = ON;

-- Dades preinsertades
-- Inserció de països a la taula 'paisos'
INSERT INTO paisos (codi_iso2, codi_iso3, codi_pais_num) VALUES
('AF', 'AFG', '004'),
('AL', 'ALB', '008'),
('DE', 'DEU', '276'),
('AD', 'AND', '020'),
('AO', 'AGO', '024'),
('AG', 'ATG', '028'),
('SA', 'SAU', '682'),
('AR', 'ARG', '032'),
('AM', 'ARM', '051'),
('AU', 'AUS', '036'),
('AT', 'AUT', '040'),
('AZ', 'AZE', '031'),
('BS', 'BHS', '044'),
('BH', 'BHR', '048'),
('BD', 'BGD', '050'),
('BB', 'BRB', '052'),
('BE', 'BEL', '056'),
('BZ', 'BLZ', '084'),
('BJ', 'BEN', '204'),
('BY', 'BLR', '112'),
('MM', 'MMR', '104'),
('BO', 'BOL', '068'),
('BA', 'BIH', '070'),
('BW', 'BWA', '072'),
('BR', 'BRA', '076'),
('BN', 'BRN', '096'),
('BG', 'BGR', '100'),
('BF', 'BFA', '854'),
('BI', 'BDI', '108'),
('BT', 'BTN', '064'),
('CV', 'CPV', '132'),
('KH', 'KHM', '116'),
('CM', 'CMR', '120'),
('CA', 'CAN', '124'),
('QA', 'QAT', '634'),
('TD', 'TCD', '148'),
('CL', 'CHL', '152'),
('CN', 'CHN', '156'),
('CY', 'CYP', '196'),
('CO', 'COL', '170'),
('KM', 'COM', '174'),
('CG', 'COG', '178'),
('CD', 'COD', '180'),
('KP', 'PRK', '408'),
('KR', 'KOR', '410'),
('CI', 'CIV', '384'),
('CR', 'CRI', '188'),
('HR', 'HRV', '191'),
('CU', 'CUB', '192'),
('DK', 'DNK', '208'),
('DM', 'DMA', '212'),
('DO', 'DOM', '214'),
('EC', 'ECU', '218'),
('EG', 'EGY', '818'),
('SV', 'SLV', '222'),
('ER', 'ERI', '232'),
('SK', 'SVK', '703'),
('SI', 'SVN', '705'),
('ES', 'ESP', '724'),
('US', 'USA', '840'),
('EE', 'EST', '233'),
('SZ', 'SWZ', '748'),
('ET', 'ETH', '231'),
('FJ', 'FJI', '242'),
('PH', 'PHL', '608'),
('FI', 'FIN', '246'),
('FR', 'FRA', '250'),
('GA', 'GAB', '266'),
('GM', 'GMB', '270'),
('GE', 'GEO', '268'),
('GH', 'GHA', '288'),
('GR', 'GRC', '300'),
('GD', 'GRD', '308'),
('GT', 'GTM', '320'),
('GN', 'GIN', '324'),
('GW', 'GNB', '624'),
('GQ', 'GNQ', '226'),
('GY', 'GUY', '328'),
('HT', 'HTI', '332'),
('HN', 'HND', '340'),
('HU', 'HUN', '348'),
('IN', 'IND', '356'),
('ID', 'IDN', '360'),
('IR', 'IRN', '364'),
('IQ', 'IRQ', '368'),
('IE', 'IRL', '372'),
('IS', 'ISL', '352'),
('IL', 'ISR', '376'),
('IT', 'ITA', '380'),
('JM', 'JAM', '388'),
('JP', 'JPN', '392'),
('JO', 'JOR', '400'),
('KZ', 'KAZ', '398'),
('KE', 'KEN', '404'),
('KI', 'KIR', '296'),
('KW', 'KWT', '414'),
('KG', 'KGZ', '417'),
('LA', 'LAO', '418'),
('LS', 'LSO', '426'),
('LV', 'LVA', '428'),
('LB', 'LBN', '422'),
('LR', 'LBR', '430'),
('LY', 'LBY', '434'),
('LI', 'LIE', '438'),
('LT', 'LTU', '440'),
('LU', 'LUX', '442'),
('MK', 'MKD', '807'),
('MG', 'MDG', '450'),
('MW', 'MWI', '454'),
('MY', 'MYS', '458'),
('MV', 'MDV', '462'),
('ML', 'MLI', '466'),
('MT', 'MLT', '470'),
('MH', 'MHL', '584'),
('MR', 'MRT', '478'),
('MU', 'MUS', '480'),
('MX', 'MEX', '484'),
('FM', 'FSM', '583'),
('MD', 'MDA', '498'),
('MC', 'MCO', '492'),
('MN', 'MNG', '496'),
('ME', 'MNE', '499'),
('MZ', 'MOZ', '508'),
('NA', 'NAM', '516'),
('NR', 'NRU', '520'),
('NP', 'NPL', '524'),
('NL', 'NLD', '528'),
('NZ', 'NZL', '554'),
('NI', 'NIC', '558'),
('NE', 'NER', '562'),
('NG', 'NGA', '566'),
('NO', 'NOR', '578'),
('OM', 'OMN', '512'),
('PK', 'PAK', '586'),
('PW', 'PLW', '585'),
('PA', 'PAN', '591'),
('PG', 'PNG', '598'),
('PY', 'PRY', '600'),
('PE', 'PER', '604'),
('PL', 'POL', '616'),
('PT', 'PRT', '620'),
('GB', 'GBR', '826'),
('CF', 'CAF', '140'),
('CZ', 'CZE', '203'),
('RO', 'ROU', '642'),
('RU', 'RUS', '643'),
('RW', 'RWA', '646'),
('KN', 'KNA', '659'),
('LC', 'LCA', '662'),
('VC', 'VCT', '670'),
('WS', 'WSM', '882'),
('SM', 'SMR', '674'),
('ST', 'STP', '678'),
('SN', 'SEN', '686'),
('RS', 'SRB', '688'),
('SC', 'SYC', '690'),
('SL', 'SLE', '694'),
('SG', 'SGP', '702'),
('SY', 'SYR', '760'),
('SO', 'SOM', '706'),
('LK', 'LKA', '144'),
('ZA', 'ZAF', '710'),
('SD', 'SDN', '729'),
('SS', 'SSD', '728'),
('SE', 'SWE', '752'),
('CH', 'CHE', '756'),
('SR', 'SUR', '740'),
('TH', 'THA', '764'),
('TJ', 'TJK', '762'),
('TL', 'TLS', '626'),
('TG', 'TGO', '768'),
('TO', 'TON', '776'),
('TT', 'TTO', '780'),
('TN', 'TUN', '788'),
('TR', 'TUR', '792'),
('TM', 'TKM', '795'),
('TV', 'TUV', '798'),
('UG', 'UGA', '800'),
('UA', 'UKR', '804'),
('AE', 'ARE', '784'),
('UY', 'URY', '858'),
('UZ', 'UZB', '860'),
('VU', 'VUT', '548'),
('VA', 'VAT', '336'),
('VE', 'VEN', '862'),
('VN', 'VNM', '704'),
('YE', 'YEM', '887'),
('ZM', 'ZMB', '894'),
('ZW', 'ZWE', '716'),
('TW', 'TWN', '158'),
('MA', 'MAR', '504'),
('PS', 'PSE', '275'),
('EH', 'ESH', '732'),
('KY', 'CYM', '136'),
('FK', 'FLK', '238'),
('GI', 'GIB', '292'),
('GL', 'GRL', '304'),
('GP', 'GLP', '312'),
('GU', 'GUM', '316'),
('HK', 'HKG', '344'),
('MO', 'MAC', '446'),
('MQ', 'MTQ', '474'),
('NC', 'NCL', '540'),
('PF', 'PYF', '258'),
('PR', 'PRI', '630'),
('RE', 'REU', '638'),
('BL', 'BLM', '652'),
('MF', 'MAF', '663'),
('PM', 'SPM', '666'),
('SX', 'SXM', '534'),
('TC', 'TCA', '796'),
('VG', 'VGB', '092'),
('VI', 'VIR', '850'),
('WF', 'WLF', '876');