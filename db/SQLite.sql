BEGIN IMMEDIATE;
PRAGMA foreign_keys = ON;
-- Desactivo les claus foranes per pervindre errors durant la creació
-- PRAGMA foreign_keys = OFF;

CREATE TABLE IF NOT EXISTS usuaris (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom TEXT NOT NULL,
    cognoms TEXT NOT NULL,
    usuari TEXT NOT NULL UNIQUE,
    contrasenya TEXT NOT NULL,  -- Guardarà el hash de la contrasenya
    correu TEXT NOT NULL UNIQUE,
    data_naixement DATE,
    pais TEXT,
    estat TEXT,
    provincia TEXT,
    poblacio TEXT,
    codi_postal TEXT,
    data_creacio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    token_activacio TEXT,
    expira_token DATETIME,
    actiu BOOLEAN DEFAULT 1
);

CREATE TABLE IF NOT EXISTS grups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom TEXT NOT NULL UNIQUE,
    descripcio TEXT,
    data_creacio TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS usuaris_grups (
    usuari_id INTEGER NOT NULL,
    grup_id INTEGER NOT NULL,
    data_afegit TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (usuari_id, grup_id),
    FOREIGN KEY (usuari_id) REFERENCES usuaris(id) ON DELETE CASCADE,
    FOREIGN KEY (grup_id) REFERENCES grups(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS politiques (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom TEXT NOT NULL UNIQUE,
    descripcio TEXT,
    permisos TEXT NOT NULL,  -- JSON o text amb els permisos específics
    data_creacio TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS usuaris_politiques (
    usuari_id INTEGER NOT NULL,
    politica_id INTEGER NOT NULL,
    data_assignacio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (usuari_id, politica_id),
    FOREIGN KEY (usuari_id) REFERENCES usuaris(id) ON DELETE CASCADE,
    FOREIGN KEY (politica_id) REFERENCES politiques(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS grups_politiques (
    grup_id INTEGER NOT NULL,
    politica_id INTEGER NOT NULL,
    data_assignacio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (grup_id, politica_id),
    FOREIGN KEY (grup_id) REFERENCES grups(id) ON DELETE CASCADE,
    FOREIGN KEY (politica_id) REFERENCES politiques(id) ON DELETE CASCADE
);

-- SQLite.sql
CREATE TABLE IF NOT EXISTS persona (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom TEXT NOT NULL,
    cognom1 TEXT,
    cognom2 TEXT,
    municipi TEXT,
    arquevisbat TEXT,
    nom_complet TEXT,
    pagina TEXT,
    llibre TEXT,
    quinta TEXT,
    data_naixement DATE,
    data_bateig DATE,
    data_defuncio DATE,
    ofici TEXT,
    estat_civil TEXT
);

CREATE TABLE IF NOT EXISTS relacions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    persona_id INTEGER NOT NULL,
    tipus_relacio TEXT NOT NULL, -- ex: "pare", "mare", "casat", etc.
    nom TEXT,
    cognom1 TEXT,
    cognom2 TEXT,
    municipi TEXT,
    ofici TEXT,
    data_matrimoni TEXT,
    FOREIGN KEY(persona_id) REFERENCES persona(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS persona_possibles_duplicats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom TEXT NOT NULL,
    cognom1 TEXT NOT NULL,
    cognom2 TEXT NOT NULL,
    municipi TEXT,
    arquevisbat TEXT,
    nom_complet TEXT,
    pagina TEXT,
    llibre TEXT,
    quinta TEXT
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

-- Taula de sessions (mapa token_hash -> usuari)
CREATE TABLE IF NOT EXISTS sessions (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  usuari_id    INTEGER NOT NULL,
  token_hash   TEXT    NOT NULL UNIQUE, -- SHA-256 o HMAC-SHA-256 en hex/base64
  expira       TIMESTAMP, -- data d'expiració (nullable per compatibilitat amb versions anteriors)
  creat        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  revocat      INTEGER NOT NULL DEFAULT 0 CHECK (revocat IN (0,1)),
  FOREIGN KEY (usuari_id) REFERENCES usuaris(id) ON DELETE CASCADE
);

-- Registre d’accessos (IP + timestamp vinculats a la sessió)
CREATE TABLE IF NOT EXISTS session_access_log (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id INTEGER  NOT NULL,
  ts         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ip         TEXT     NOT NULL,     -- guarda IPv4/IPv6 en text
  FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
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
-- CREATE INDEX idx_persona_cognoms_any_llibre_pagina ON persona(cognom1, cognom2, quinta, llibre, pagina); -- error executant SQLite.sql: index idx_persona_cognoms_any_llibre_pagina already exists

-- Cerca per cognoms i nom (per coincidències exactes)
CREATE INDEX IF NOT EXISTS idx_persona_nom_complet ON persona(nom_complet);

-- Útil per cerca de persones per municipi i any (ex: nascuts al mateix lloc i època)
CREATE INDEX IF NOT EXISTS idx_persona_municipi_quinta ON persona(municipi, quinta);

-- Cercar per ofici o estat civil
CREATE INDEX IF NOT EXISTS idx_persona_ofici ON persona(ofici);
-- CREATE INDEX IF NOT EXISTS idx_persona_estat_civil ON persona(estat_civil);

CREATE INDEX IF NOT EXISTS idx_usuaris_correu ON usuaris(correu);
CREATE INDEX IF NOT EXISTS idx_usuaris_data_creacio ON usuaris(data_creacio);
CREATE INDEX IF NOT EXISTS idx_grups_nom ON grups(nom);
CREATE INDEX IF NOT EXISTS idx_politiques_nom ON politiques(nom);

-- Index taula sessions
CREATE INDEX IF NOT EXISTS idx_sessions_user    ON sessions(usuari_id);
CREATE INDEX IF NOT EXISTS idx_sessions_revocat ON sessions(revocat);

-- Index taula sessions_access_log
CREATE INDEX IF NOT EXISTS idx_access_session_ts ON session_access_log(session_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_access_ip_ts      ON session_access_log(ip, ts DESC);

-- Reactivo les claus foranes per pervindre errors durant la creació
-- PRAGMA foreign_keys = ON;
COMMIT;
