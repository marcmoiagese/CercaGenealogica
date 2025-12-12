BEGIN;

-- Taules de GESTIÓ D'USUARIS I PERMISOS
------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS usuaris (
    id SERIAL PRIMARY KEY,
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
    data_creacio TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    token_activacio TEXT,
    expira_token TIMESTAMP WITHOUT TIME ZONE,
    actiu BOOLEAN DEFAULT TRUE -- En PostgreSQL, s'utilitza TRUE/FALSE, no 1/0
);

CREATE TABLE IF NOT EXISTS grups (
    id SERIAL PRIMARY KEY,
    nom TEXT NOT NULL UNIQUE,
    descripcio TEXT,
    data_creacio TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS usuaris_grups (
    usuari_id INTEGER NOT NULL,
    grup_id INTEGER NOT NULL,
    data_afegit TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (usuari_id, grup_id),
    FOREIGN KEY (usuari_id) REFERENCES usuaris(id) ON DELETE CASCADE,
    FOREIGN KEY (grup_id) REFERENCES grups(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS politiques (
    id SERIAL PRIMARY KEY,
    nom TEXT NOT NULL UNIQUE,
    descripcio TEXT,
    permisos TEXT NOT NULL,  -- JSON o text amb els permisos específics
    data_creacio TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS usuaris_politiques (
    usuari_id INTEGER NOT NULL,
    politica_id INTEGER NOT NULL,
    data_assignacio TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (usuari_id, politica_id),
    FOREIGN KEY (usuari_id) REFERENCES usuaris(id) ON DELETE CASCADE,
    FOREIGN KEY (politica_id) REFERENCES politiques(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS grups_politiques (
    grup_id INTEGER NOT NULL,
    politica_id INTEGER NOT NULL,
    data_assignacio TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (grup_id, politica_id),
    FOREIGN KEY (grup_id) REFERENCES grups(id) ON DELETE CASCADE,
    FOREIGN KEY (politica_id) REFERENCES politiques(id) ON DELETE CASCADE
);

-- Taules de DADES DE PERSONES
------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS persona (
    id SERIAL PRIMARY KEY,
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
    id SERIAL PRIMARY KEY,
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
    id SERIAL PRIMARY KEY,
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

-- Taules de DADES GEOGRÀFIQUES I HISTÒRIQUES
------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS paisos (
    id SERIAL PRIMARY KEY,
    codi_iso2 VARCHAR(2) UNIQUE, -- Codi ISO 3166-1 alpha-2 ex: ES, FR, US, CA... (VARCHAR en lloc de TEXT(2))
    codi_iso3 VARCHAR(3) UNIQUE, -- Codi ISO 3166-1 alpha-3 ex: ESP, FRA, USA (VARCHAR en lloc de TEXT(3))
    codi_pais_num TEXT, -- Codi numèric ISO 3166-1
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS nivells_administratius (
    id SERIAL PRIMARY KEY,
    pais_id INTEGER REFERENCES paisos(id),
    nivel INTEGER CHECK(nivel BETWEEN 1 AND 7),
    nom_nivell TEXT,
    tipus_nivell TEXT,
    codi_oficial TEXT,
    altres TEXT,
    estat TEXT CHECK(estat IN ('actiu', 'inactiu', 'fusionat', 'abolit')) DEFAULT 'actiu',
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS municipis (
    id SERIAL PRIMARY KEY,
    nom TEXT NOT NULL,
    municipi_id INTEGER REFERENCES municipis(id),
    tipus TEXT NOT NULL,
    nivell_administratiu_id_1 INTEGER REFERENCES nivells_administratius(id), -- País
    nivell_administratiu_id_2 INTEGER REFERENCES nivells_administratius(id), -- Regió / Comunitat Autònoma
    nivell_administratiu_id_3 INTEGER REFERENCES nivells_administratius(id), -- Província
    nivell_administratiu_id_4 INTEGER REFERENCES nivells_administratius(id), -- Comarca / àrea local
    nivell_administratiu_id_5 INTEGER REFERENCES nivells_administratius(id), -- Àrea local
    nivell_administratiu_id_6 INTEGER REFERENCES nivells_administratius(id), -- Municipi
    nivell_administratiu_id_7 INTEGER REFERENCES nivells_administratius(id), -- Barri
    codi_postal TEXT,
    latitud REAL,
    longitud REAL,
    what3words TEXT,
    web TEXT,
    wikipedia TEXT,
    altres TEXT,
    estat TEXT CHECK(estat IN ('actiu', 'inactiu', 'abandonat')) DEFAULT 'actiu',
    data_creacio TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ultima_modificacio TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS noms_historics (
    id SERIAL PRIMARY KEY,
    id_municipi INTEGER NOT NULL REFERENCES municipis(id),
    nom TEXT NOT NULL,
    any_inici INTEGER,
    any_fi INTEGER,
    pais_regne TEXT,
    distribucio_geografica TEXT,
    font TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS arquebisbats (
    id SERIAL PRIMARY KEY,
    nom TEXT NOT NULL UNIQUE,
    tipus_entitat TEXT CHECK(tipus_entitat IN ('arquebisbat', 'bisbat', 'diocesi', 'viscomtat', 'vegueria', 'altres')),
    web TEXT,
    web_arxiu TEXT,
    web_wikipedia TEXT,
    territori TEXT,
    autoritat_superior TEXT,
    observacions TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS arquebisbats_municipi (
    id SERIAL PRIMARY KEY,
    id_municipi INTEGER NOT NULL REFERENCES municipis(id),
    id_arquevisbat INTEGER NOT NULL REFERENCES arquebisbats(id),
    any_inici INTEGER,
    any_fi INTEGER,
    motiu TEXT,
    font TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS codis_postals (
    id SERIAL PRIMARY KEY,
    id_municipi INTEGER NOT NULL REFERENCES municipis(id),
    codi_postal TEXT NOT NULL,
    zona TEXT,
    desde DATE,
    fins DATE,
    font TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS relacio_comarca_provincia (
    id SERIAL PRIMARY KEY,
    id_municipi INTEGER NOT NULL REFERENCES municipis(id),
    comarca TEXT,
    provincia TEXT,
    any_inici INTEGER,
    any_fi INTEGER,
    font TEXT,
    observacions TEXT
);

CREATE TABLE IF NOT EXISTS llibres (
    id SERIAL PRIMARY KEY,
    arquevisbat_id INTEGER NOT NULL,
    municipi_id INTEGER NOT NULL,
    nom_esglesia TEXT,
    codi_digital TEXT,
    codi_fisic TEXT,
    titol TEXT,
    cronologia TEXT,
    volum TEXT,
    abat TEXT,
    contingut TEXT,
    llengua TEXT,
    requeriments_tecnics TEXT,
    unitat_catalogacio TEXT,
    unitat_instalacio TEXT,
    pagines INT,
    url_base TEXT,
    url_imatge_prefix TEXT DEFAULT '#imatge-',
    pagina TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(arquevisbat_id) REFERENCES arquebisbats(id) ON DELETE CASCADE,
    FOREIGN KEY(municipi_id) REFERENCES municipis(id) ON DELETE RESTRICT
);




-- =====================================================================
-- Arxius / Custòdia (físic o digital) + Estat d'indexació per pàgina
-- =====================================================================

CREATE TABLE IF NOT EXISTS arxius (
  id SERIAL PRIMARY KEY,
  nom TEXT NOT NULL UNIQUE,
  tipus TEXT,                 -- parroquia, arxiu_diocesa, portal_digital, etc.
  municipi_id INTEGER REFERENCES municipis(id) ON DELETE SET NULL,
  adreca TEXT,
  ubicacio TEXT,              -- (legacy) municipi/adreça en text lliure
  web TEXT,
  acces TEXT,                 -- online, presencial, mixt
  notes TEXT,
  created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS arxius_llibres (
  arxiu_id INTEGER NOT NULL REFERENCES arxius(id) ON DELETE CASCADE,
  llibre_id INTEGER NOT NULL REFERENCES llibres(id) ON DELETE CASCADE,
  signatura TEXT,          -- signatura específica a aquell arxiu
  url_override TEXT,       -- si l’URL depèn de l’arxiu/portal
  PRIMARY KEY (arxiu_id, llibre_id)
);

CREATE TABLE IF NOT EXISTS llibre_pagines (
  id SERIAL PRIMARY KEY,
  llibre_id INTEGER NOT NULL REFERENCES llibres(id) ON DELETE CASCADE,
  num_pagina INTEGER NOT NULL,
  estat TEXT NOT NULL CHECK (estat IN ('pendent','indexant','indexada','revisio','error')) DEFAULT 'pendent',
  indexed_at TIMESTAMP WITHOUT TIME ZONE,
  indexed_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  notes TEXT,
  UNIQUE (llibre_id, num_pagina)
);

-- Índexs per accelerar consultes habituals
CREATE INDEX IF NOT EXISTS idx_arxius_llibres_arxiu  ON arxius_llibres(arxiu_id);
CREATE INDEX IF NOT EXISTS idx_arxius_llibres_llibre ON arxius_llibres(llibre_id);
CREATE INDEX IF NOT EXISTS idx_llibre_pagines_estat  ON llibre_pagines(llibre_id, estat);

-- Taules de GESTIÓ DE SESSIONS
------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sessions (
  id           SERIAL PRIMARY KEY,
  usuari_id    INTEGER NOT NULL,
  token_hash   TEXT    NOT NULL UNIQUE,
  expira       TIMESTAMP WITHOUT TIME ZONE,
  creat        TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  revocat      BOOLEAN NOT NULL DEFAULT FALSE, -- BOOLEAN en lloc de INTEGER amb CHECK
  FOREIGN KEY (usuari_id) REFERENCES usuaris(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS session_access_log (
  id         SERIAL PRIMARY KEY,
  session_id INTEGER  NOT NULL,
  ts         TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ip         TEXT     NOT NULL,
  FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS password_resets (
  id         SERIAL PRIMARY KEY,
  usuari_id  INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
  token      TEXT    NOT NULL UNIQUE,
  expira     TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  lang       TEXT,
  used       BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_privacy (
  usuari_id INTEGER PRIMARY KEY REFERENCES usuaris(id) ON DELETE CASCADE,
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
  show_activity BOOLEAN NOT NULL DEFAULT TRUE,
  profile_public BOOLEAN NOT NULL DEFAULT TRUE,
  notify_email BOOLEAN NOT NULL DEFAULT TRUE,
  allow_contact BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS email_changes (
  id SERIAL PRIMARY KEY,
  usuari_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
  old_email TEXT NOT NULL,
  new_email TEXT NOT NULL,
  token_confirm TEXT NOT NULL UNIQUE,
  exp_confirm TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  token_revert TEXT NOT NULL UNIQUE,
  exp_revert TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  lang TEXT,
  confirmed BOOLEAN NOT NULL DEFAULT FALSE,
  reverted BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Índexs
------------------------------------------------------------------------------------------------------------------------

-- Per buscar ràpidament pel codi postal
CREATE INDEX IF NOT EXISTS idx_codi_postal ON municipis(codi_postal);
-- Per cercar pel nom
CREATE INDEX IF NOT EXISTS idx_nom_municipi ON municipis(nom);
-- Per buscar nivells pel seu tipus
CREATE INDEX IF NOT EXISTS idx_tipus_nivell ON nivells_administratius(tipus_nivell);

-- Índexs de la taula 'persona'
-- Índex compost per millorar la cerca de duplicats i cerques combinades
CREATE INDEX IF NOT EXISTS idx_persona_cognoms_quinta_llibre_pagina ON persona(cognom1, cognom2, quinta, llibre, pagina);
-- Cerca per cognoms i nom (per coincidències exactes)
CREATE INDEX IF NOT EXISTS idx_persona_nom_complet  ON persona(nom_complet);
-- Útil per cerca de persones per municipi i any (ex: nascuts al mateix lloc i època)
CREATE INDEX IF NOT EXISTS idx_persona_municipi_quinta ON persona(municipi, quinta);
-- Cercar per ofici o estat civil
CREATE INDEX IF NOT EXISTS idx_persona_ofici ON persona(ofici);
-- CREATE INDEX IF NOT EXISTS idx_persona_estat_civil ON persona(estat_civil); -- Es manté el comentari

-- Índexs de la taula 'usuaris' i permisos
CREATE INDEX IF NOT EXISTS idx_usuaris_correu ON usuaris(correu);
CREATE INDEX IF NOT EXISTS idx_usuaris_data_creacio ON usuaris(data_creacio);
CREATE INDEX IF NOT EXISTS idx_grups_nom ON grups(nom);
CREATE INDEX IF NOT EXISTS idx_politiques_nom ON politiques(nom);

-- Índexs de la taula 'sessions'
CREATE INDEX IF NOT EXISTS idx_sessions_user    ON sessions(usuari_id);
CREATE INDEX IF NOT EXISTS idx_sessions_revocat ON sessions(revocat);

-- Índexs de la taula 'session_access_log'
CREATE INDEX IF NOT EXISTS idx_access_session_ts ON session_access_log(session_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_access_ip_ts      ON session_access_log(ip, ts DESC);

-- Índexs de la taula 'password_resets'
CREATE INDEX IF NOT EXISTS idx_password_resets_token ON password_resets(token);
CREATE INDEX IF NOT EXISTS idx_password_resets_expira ON password_resets(expira);

COMMIT;
