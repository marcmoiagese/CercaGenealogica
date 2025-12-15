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
    address TEXT,
    employment_status TEXT,
    profession TEXT,
    phone TEXT,
    preferred_lang TEXT,
    spoken_langs TEXT,
    data_creacio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    token_activacio TEXT,
    expira_token DATETIME,
    actiu INTEGER NOT NULL DEFAULT 1 CHECK (actiu IN (0,1))
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
    nom TEXT NOT NULL UNIQUE, -- admin, moderador, confiança, usuari
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
    estat_civil TEXT,
    created_by INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_by INTEGER,
    moderated_by INTEGER,
    moderated_at TIMESTAMP,
    FOREIGN KEY(created_by) REFERENCES usuaris(id) ON DELETE SET NULL,
    FOREIGN KEY(updated_by) REFERENCES usuaris(id) ON DELETE SET NULL,
    FOREIGN KEY(moderated_by) REFERENCES usuaris(id) ON DELETE SET NULL
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
    parent_id INTEGER REFERENCES nivells_administratius(id), -- Referència al nivell superior (null si és el nivell més alt)
    any_inici INTEGER,  -- quan apareix aquest nivell (si es coneix)
    any_fi    INTEGER,   -- quan deixa d’existir / canvia (null = vigent)
    estat TEXT CHECK(estat IN ('actiu', 'inactiu', 'fusionat', 'abolit')) DEFAULT 'actiu',
    created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderation_status TEXT CHECK(moderation_status IN ('pendent','publicat','rebutjat')) DEFAULT 'pendent',
    moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderated_at TIMESTAMP,
    moderation_notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Quan s'ha creat el nivell
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Última vegada que ha canviat
);

-- TAULA MUNICIPIS
-- Aquesta taula conté qualsevol nucli de població (ciutat, poble, barri, llogaret, etc.)
CREATE TABLE IF NOT EXISTS municipis (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom TEXT NOT NULL, -- Nom oficial actual del municipi
    municipi_id INTEGER REFERENCES municipis(id) ON DELETE SET NULL, -- Opcional: si el poble pertany a un altre municipi (ex: barri d'un municipi gran)
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
    created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderation_status TEXT CHECK(moderation_status IN ('pendent','publicat','rebutjat')) DEFAULT 'pendent',
    moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderated_at TIMESTAMP,
    moderation_notes TEXT,
    data_creacio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ultima_modificacio TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Recorda: aquí no podem posar FKs condicionals segons entitat_tipus en SQLite,
-- la coherència l’asseguraràs des del codi Go (validant abans de fer INSERT/UPDATE).
CREATE TABLE IF NOT EXISTS noms_historics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Tipus d'entitat a la qual fa referència aquest nom històric:
    --   municipi       -> entitat_id apunta a municipis.id
    --   nivell_admin   -> entitat_id apunta a nivells_administratius.id
    --   eclesiastic    -> entitat_id apunta a arquebisbats.id
    entitat_tipus TEXT NOT NULL CHECK(entitat_tipus IN ('municipi', 'nivell_admin', 'eclesiastic')),
    entitat_id INTEGER NOT NULL,

    nom TEXT NOT NULL,          -- Nom antic o anterior
    any_inici INTEGER,          -- Any d'inici del nom (opcional)
    any_fi INTEGER,             -- Any final del nom (NULL = encara vigent)

    pais_regne TEXT,            -- Regne o estat al moment del nom ("Regne d'Aragó", "Imperi Romà"...)
    distribucio_geografica TEXT,-- Sistema administratiu anterior: vegueria, diòcesi, baronia, etc.
    font TEXT,                  -- Font o documentació on s'ha trobat aquest nom

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Moment de gravació del registre
);

CREATE TABLE IF NOT EXISTS arquebisbats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom TEXT NOT NULL UNIQUE,   -- Nom oficial actual de l'entitat eclesiàstica
    tipus_entitat TEXT CHECK(tipus_entitat IN ('arquebisbat', 'bisbat', 'diocesi', 'viscomtat', 'vegueria', 'altres')), -- Tipus d'entitat eclesiàstica
    pais_id INTEGER REFERENCES paisos(id),  -- Enllaç opcional amb un país (si aplica)
    -- Jerarquia interna eclesiàstica
    nivell INTEGER,             -- 1=arquebisbat, 2=bisbat, 3=arxiprestat, 4=parròquia, etc.
    parent_id INTEGER REFERENCES arquebisbats(id), -- Entitat eclesiàstica superior
    -- Vigència històrica (opcional)
    any_inici INTEGER,          -- Any en què comença a existir aquesta entitat
    any_fi INTEGER,             -- Any en què deixa d'existir / canvia (NULL = encara vigent)
    web TEXT,
    web_arxiu TEXT,
    web_wikipedia TEXT,
    territori TEXT, -- Ex: Àmbit geogràfic (ex: Catalunya Nord, Catalunya del Sud)
    observacions TEXT,
    created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderation_status TEXT CHECK(moderation_status IN ('pendent','publicat','rebutjat')) DEFAULT 'pendent',
    moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderated_at TIMESTAMP,
    moderation_notes TEXT,
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
    created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderation_status TEXT CHECK(moderation_status IN ('pendent','publicat','rebutjat')) DEFAULT 'pendent',
    moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderated_at TIMESTAMP,
    moderation_notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(arquevisbat_id) REFERENCES arquebisbats(id) ON DELETE CASCADE,
    FOREIGN KEY(municipi_id) REFERENCES municipis(id) ON DELETE RESTRICT
);



-- =====================================================================
-- Arxius / Custòdia (físic o digital) + Estat d'indexació per pàgina
-- =====================================================================

CREATE TABLE IF NOT EXISTS arxius (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom TEXT NOT NULL UNIQUE,
    tipus TEXT,                 -- parroquia, arxiu_diocesa, portal_digital, etc.

    municipi_id INTEGER REFERENCES municipis(id) ON DELETE SET NULL,

    -- Nova FK opcional: a quina entitat eclesiàstica està vinculat l'arxiu
    entitat_eclesiastica_id INTEGER REFERENCES arquebisbats(id) ON DELETE SET NULL,

    adreca TEXT,
    ubicacio TEXT,              -- (legacy) municipi/adreça en text lliure
    web TEXT,
    acces TEXT,                 -- online, presencial, mixt
    notes TEXT,
    created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderation_status TEXT CHECK(moderation_status IN ('pendent','publicat','rebutjat')) DEFAULT 'pendent',
    moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderated_at TIMESTAMP,
    moderation_notes TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS arxius_llibres (
  arxiu_id INTEGER NOT NULL REFERENCES arxius(id) ON DELETE CASCADE,
  llibre_id INTEGER NOT NULL REFERENCES llibres(id) ON DELETE CASCADE,
  signatura TEXT,          -- signatura específica a aquell arxiu
  url_override TEXT,       -- si l’URL depèn de l’arxiu/portal
  PRIMARY KEY (arxiu_id, llibre_id)
);

CREATE TABLE IF NOT EXISTS llibre_pagines (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  llibre_id INTEGER NOT NULL REFERENCES llibres(id) ON DELETE CASCADE,
  num_pagina INTEGER NOT NULL,
  estat TEXT NOT NULL CHECK (estat IN ('pendent','indexant','indexada','revisio','error')) DEFAULT 'pendent',
  indexed_at TIMESTAMP,
  indexed_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  notes TEXT,
  UNIQUE (llibre_id, num_pagina)
);

-- Índexs per accelerar consultes habituals
CREATE INDEX IF NOT EXISTS idx_arxius_llibres_arxiu  ON arxius_llibres(arxiu_id);
CREATE INDEX IF NOT EXISTS idx_arxius_llibres_llibre ON arxius_llibres(llibre_id);
CREATE INDEX IF NOT EXISTS idx_llibre_pagines_estat  ON llibre_pagines(llibre_id, estat);

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

-- Recuperació de contrasenya
CREATE TABLE IF NOT EXISTS password_resets (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  usuari_id  INTEGER NOT NULL,
  token      TEXT    NOT NULL UNIQUE,
  expira     DATETIME NOT NULL,
  lang       TEXT,
  used       INTEGER NOT NULL DEFAULT 0 CHECK (used IN (0,1)),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (usuari_id) REFERENCES usuaris(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS user_privacy (
  usuari_id INTEGER PRIMARY KEY,
  nom_visibility TEXT DEFAULT 'private',
  cognoms_visibility TEXT DEFAULT 'private',
  email_visibility TEXT DEFAULT 'private',
  birth_visibility TEXT DEFAULT 'private',
  pais_visibility TEXT DEFAULT 'public',
  estat_visibility TEXT DEFAULT 'private',
  provincia_visibility TEXT DEFAULT 'private',
  poblacio_visibility TEXT DEFAULT 'private',
  postal_visibility TEXT DEFAULT 'private',
  address_visibility TEXT DEFAULT 'private',
  employment_visibility TEXT DEFAULT 'private',
  profession_visibility TEXT DEFAULT 'private',
  phone_visibility TEXT DEFAULT 'private',
  preferred_lang_visibility TEXT DEFAULT 'private',
  spoken_langs_visibility TEXT DEFAULT 'private',
  show_activity INTEGER NOT NULL DEFAULT 1 CHECK (show_activity IN (0,1)),
  profile_public INTEGER NOT NULL DEFAULT 1 CHECK (profile_public IN (0,1)),
  notify_email INTEGER NOT NULL DEFAULT 1 CHECK (notify_email IN (0,1)),
  allow_contact INTEGER NOT NULL DEFAULT 1 CHECK (allow_contact IN (0,1)),
  FOREIGN KEY (usuari_id) REFERENCES usuaris(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS email_changes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  usuari_id INTEGER NOT NULL,
  old_email TEXT NOT NULL,
  new_email TEXT NOT NULL,
  token_confirm TEXT NOT NULL UNIQUE,
  exp_confirm DATETIME NOT NULL,
  token_revert TEXT NOT NULL UNIQUE,
  exp_revert DATETIME NOT NULL,
  lang TEXT,
  confirmed INTEGER NOT NULL DEFAULT 0 CHECK (confirmed IN (0,1)),
  reverted INTEGER NOT NULL DEFAULT 0 CHECK (reverted IN (0,1)),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (usuari_id) REFERENCES usuaris(id) ON DELETE CASCADE
);

-- =====================================================================
-- Sistema de punts per activitats dels usuaris
-- =====================================================================

CREATE TABLE IF NOT EXISTS punts_regles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    codi TEXT NOT NULL UNIQUE,   -- Clau interna: 'persona_create', 'pagina_indexada', 'moderacio_aprovada', etc.
    nom TEXT NOT NULL,           -- Nom humà: "Crear registre de persona", "Indexar pàgina", etc.
    descripcio TEXT,             -- Explicació més llarga de la regla

    punts INTEGER NOT NULL,      -- Punts que atorga aquesta acció (pot ser negatiu, si vols penalitzacions)
    actiu INTEGER NOT NULL DEFAULT 1 CHECK (actiu IN (0,1)),

    data_creacio TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS usuaris_activitat (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    usuari_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,

    -- Regla aplicada (opcional, però molt útil per saber d'on surten els punts)
    regla_id INTEGER REFERENCES punts_regles(id),

    -- Tipus d'acció genèric: 'crear', 'editar', 'moderar_aprovar', 'moderar_rebutjar', 'corregir', etc.
    accio TEXT NOT NULL,

    -- Sobre QUIN objecte s'ha fet l'acció
    objecte_tipus TEXT NOT NULL,   -- 'persona', 'relacio', 'llibre_pagines', 'arxiu', 'comentari', etc.
    objecte_id INTEGER,            -- ID dins de la taula corresponent (no podem posar FK forta perquè apunta a moltes taules diferents)

    -- Punts aplicats en el moment de l’acció (pot ser 0, positiu o negatiu)
    punts INTEGER NOT NULL DEFAULT 0,

    -- Estat de l'activitat, pensant en moderació:
    --   pendent   = a l'espera de revisió (el contingut existeix però no és "validat")
    --   validat   = acceptat per algú amb permís
    --   anulat    = acció revertida / punts retirats
    estat TEXT NOT NULL DEFAULT 'validat'
        CHECK (estat IN ('pendent','validat','anulat')),

    -- Si hi ha moderació, qui la valida/rebutja
    moderat_per INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,

    -- JSON o text amb detalls extra (IP, resum dels canvis, etc.)
    detalls TEXT,

    data_creacio TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS usuaris_punts (
    usuari_id INTEGER PRIMARY KEY
        REFERENCES usuaris(id) ON DELETE CASCADE,

    punts_total INTEGER NOT NULL DEFAULT 0,

    ultima_actualitzacio TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

-- Index taula password_resets
CREATE INDEX IF NOT EXISTS idx_password_resets_token ON password_resets(token);
CREATE INDEX IF NOT EXISTS idx_password_resets_expira ON password_resets(expira);

CREATE UNIQUE INDEX IF NOT EXISTS idx_nivell_pais_nom
ON nivells_administratius(pais_id, nivel, nom_nivell);


-- Índexs útils per consultes habituals
CREATE INDEX IF NOT EXISTS idx_usuaris_activitat_usuari_data
    ON usuaris_activitat(usuari_id, data_creacio DESC);

CREATE INDEX IF NOT EXISTS idx_usuaris_activitat_objecte
    ON usuaris_activitat(objecte_tipus, objecte_id);

CREATE INDEX IF NOT EXISTS idx_persona_cognoms_quinta_llibre_pagina
  ON persona(cognom1, cognom2, quinta, llibre, pagina);
  
-- Reactivo les claus foranes per pervindre errors durant la creació
-- PRAGMA foreign_keys = ON;
COMMIT;
