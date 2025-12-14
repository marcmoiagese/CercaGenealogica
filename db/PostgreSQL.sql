BEGIN;

-- Taules de GESTIÓ D'USUARIS I PERMISOS
------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS usuaris (
    id SERIAL PRIMARY KEY,
    nom TEXT NOT NULL,
    cognoms TEXT NOT NULL,
    usuari TEXT NOT NULL UNIQUE,
    contrasenya TEXT NOT NULL,
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
    actiu BOOLEAN DEFAULT TRUE
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
    permisos TEXT NOT NULL,
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
    estat_civil TEXT,
    created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS relacions (
    id SERIAL PRIMARY KEY,
    persona_id INTEGER NOT NULL,
    tipus_relacio TEXT NOT NULL,
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
    codi_iso2 VARCHAR(2) UNIQUE,
    codi_iso3 VARCHAR(3) UNIQUE,
    codi_pais_num TEXT,
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
    parent_id INTEGER REFERENCES nivells_administratius(id),
    any_inici INTEGER,
    any_fi INTEGER,
    estat TEXT CHECK(estat IN ('actiu', 'inactiu', 'fusionat', 'abolit')) DEFAULT 'actiu',
    created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderation_status TEXT CHECK(moderation_status IN ('pendent','publicat','rebutjat')) DEFAULT 'pendent',
    moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderated_at TIMESTAMP WITHOUT TIME ZONE,
    moderation_notes TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS municipis (
    id SERIAL PRIMARY KEY,
    nom TEXT NOT NULL,
    municipi_id INTEGER REFERENCES municipis(id),
    tipus TEXT NOT NULL,
    nivell_administratiu_id_1 INTEGER REFERENCES nivells_administratius(id),
    nivell_administratiu_id_2 INTEGER REFERENCES nivells_administratius(id),
    nivell_administratiu_id_3 INTEGER REFERENCES nivells_administratius(id),
    nivell_administratiu_id_4 INTEGER REFERENCES nivells_administratius(id),
    nivell_administratiu_id_5 INTEGER REFERENCES nivells_administratius(id),
    nivell_administratiu_id_6 INTEGER REFERENCES nivells_administratius(id),
    nivell_administratiu_id_7 INTEGER REFERENCES nivells_administratius(id),
    codi_postal TEXT,
    latitud REAL,
    longitud REAL,
    what3words TEXT,
    web TEXT,
    wikipedia TEXT,
    altres TEXT,
    estat TEXT CHECK(estat IN ('actiu', 'inactiu', 'abandonat')) DEFAULT 'actiu',
    created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderation_status TEXT CHECK(moderation_status IN ('pendent','publicat','rebutjat')) DEFAULT 'pendent',
    moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderated_at TIMESTAMP WITHOUT TIME ZONE,
    moderation_notes TEXT,
    data_creacio TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ultima_modificacio TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS noms_historics (
    id SERIAL PRIMARY KEY,

    -- Tipus d'entitat a la qual fa referència aquest nom històric:
    --   municipi       -> entitat_id apunta a municipis.id
    --   nivell_admin   -> entitat_id apunta a nivells_administratius.id
    --   eclesiastic    -> entitat_id apunta a arquebisbats.id
    entitat_tipus TEXT NOT NULL CHECK (entitat_tipus IN ('municipi', 'nivell_admin', 'eclesiastic')),
    entitat_id INTEGER NOT NULL,

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
    pais_id INTEGER REFERENCES paisos(id),
    nivell INTEGER,
    parent_id INTEGER REFERENCES arquebisbats(id),
    any_inici INTEGER,
    any_fi INTEGER,
    web TEXT,
    web_arxiu TEXT,
    web_wikipedia TEXT,
    territori TEXT,
    observacions TEXT,
    created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderation_status TEXT CHECK(moderation_status IN ('pendent','publicat','rebutjat')) DEFAULT 'pendent',
    moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderated_at TIMESTAMP WITHOUT TIME ZONE,
    moderation_notes TEXT,
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

CREATE TABLE IF NOT EXISTS llibres (
    id SERIAL PRIMARY KEY,
    arquevisbat_id INTEGER NOT NULL REFERENCES arquebisbats(id) ON DELETE CASCADE,
    municipi_id INTEGER NOT NULL REFERENCES municipis(id) ON DELETE RESTRICT,
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
    created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderation_status TEXT CHECK(moderation_status IN ('pendent','publicat','rebutjat')) DEFAULT 'pendent',
    moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderated_at TIMESTAMP WITHOUT TIME ZONE,
    moderation_notes TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================================
-- Arxius / Custòdia (físic o digital) + Estat d'indexació per pàgina
-- =====================================================================

CREATE TABLE IF NOT EXISTS arxius (
  id SERIAL PRIMARY KEY,
  nom TEXT NOT NULL UNIQUE,
  tipus TEXT,
  municipi_id INTEGER REFERENCES municipis(id) ON DELETE SET NULL,
  entitat_eclesiastica_id INTEGER REFERENCES arquebisbats(id) ON DELETE SET NULL,
  adreca TEXT,
  ubicacio TEXT,
  web TEXT,
  acces TEXT,
  notes TEXT,
  created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  moderation_status TEXT CHECK(moderation_status IN ('pendent','publicat','rebutjat')) DEFAULT 'pendent',
  moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  moderated_at TIMESTAMP WITHOUT TIME ZONE,
  moderation_notes TEXT,
  created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS arxius_llibres (
  arxiu_id INTEGER NOT NULL REFERENCES arxius(id) ON DELETE CASCADE,
  llibre_id INTEGER NOT NULL REFERENCES llibres(id) ON DELETE CASCADE,
  signatura TEXT,
  url_override TEXT,
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

-- Taules de GESTIÓ DE SESSIONS
------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sessions (
  id           SERIAL PRIMARY KEY,
  usuari_id    INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
  token_hash   TEXT    NOT NULL UNIQUE,
  expira       TIMESTAMP WITHOUT TIME ZONE,
  creat        TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  revocat      BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS session_access_log (
  id         SERIAL PRIMARY KEY,
  session_id INTEGER  NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  ts         TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ip         TEXT     NOT NULL
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

-- =====================================================================
-- Sistema de punts per activitats dels usuaris
-- =====================================================================

CREATE TABLE IF NOT EXISTS punts_regles (
    id SERIAL PRIMARY KEY,
    codi TEXT NOT NULL UNIQUE,
    nom TEXT NOT NULL,
    descripcio TEXT,
    punts INTEGER NOT NULL,
    actiu BOOLEAN NOT NULL DEFAULT TRUE,
    data_creacio TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS usuaris_activitat (
    id SERIAL PRIMARY KEY,
    usuari_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
    regla_id INTEGER REFERENCES punts_regles(id),
    accio TEXT NOT NULL,
    objecte_tipus TEXT NOT NULL,
    objecte_id INTEGER,
    punts INTEGER NOT NULL DEFAULT 0,
    estat TEXT NOT NULL DEFAULT 'validat' CHECK (estat IN ('pendent','validat','anulat')),
    moderat_per INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    detalls TEXT,
    data_creacio TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS usuaris_punts (
    usuari_id INTEGER PRIMARY KEY REFERENCES usuaris(id) ON DELETE CASCADE,
    punts_total INTEGER NOT NULL DEFAULT 0,
    ultima_actualitzacio TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Índexs
------------------------------------------------------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_codi_postal ON municipis(codi_postal);
CREATE INDEX IF NOT EXISTS idx_nom_municipi ON municipis(nom);
CREATE INDEX IF NOT EXISTS idx_tipus_nivell ON nivells_administratius(tipus_nivell);

CREATE INDEX IF NOT EXISTS idx_persona_cognoms_quinta_llibre_pagina ON persona(cognom1, cognom2, quinta, llibre, pagina);
CREATE INDEX IF NOT EXISTS idx_persona_nom_complet  ON persona(nom_complet);
CREATE INDEX IF NOT EXISTS idx_persona_municipi_quinta ON persona(municipi, quinta);
CREATE INDEX IF NOT EXISTS idx_persona_ofici ON persona(ofici);

CREATE INDEX IF NOT EXISTS idx_usuaris_correu ON usuaris(correu);
CREATE INDEX IF NOT EXISTS idx_usuaris_data_creacio ON usuaris(data_creacio);
CREATE INDEX IF NOT EXISTS idx_grups_nom ON grups(nom);
CREATE INDEX IF NOT EXISTS idx_politiques_nom ON politiques(nom);

CREATE INDEX IF NOT EXISTS idx_sessions_user    ON sessions(usuari_id);
CREATE INDEX IF NOT EXISTS idx_sessions_revocat ON sessions(revocat);

CREATE INDEX IF NOT EXISTS idx_access_session_ts ON session_access_log(session_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_access_ip_ts      ON session_access_log(ip, ts DESC);

CREATE INDEX IF NOT EXISTS idx_password_resets_token ON password_resets(token);
CREATE INDEX IF NOT EXISTS idx_password_resets_expira ON password_resets(expira);

-- Índexs útils per consultes habituals
CREATE UNIQUE INDEX IF NOT EXISTS idx_nivell_pais_nom
    ON nivells_administratius(pais_id, nivel, nom_nivell);

CREATE INDEX IF NOT EXISTS idx_usuaris_activitat_usuari_data
    ON usuaris_activitat(usuari_id, data_creacio DESC);

CREATE INDEX IF NOT EXISTS idx_usuaris_activitat_objecte
    ON usuaris_activitat(objecte_tipus, objecte_id);

CREATE INDEX IF NOT EXISTS idx_arxius_llibres_arxiu
    ON arxius_llibres(arxiu_id);

CREATE INDEX IF NOT EXISTS idx_arxius_llibres_llibre
    ON arxius_llibres(llibre_id);

CREATE INDEX IF NOT EXISTS idx_llibre_pagines_estat
    ON llibre_pagines(llibre_id, estat);

COMMIT;
