SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0; -- Desactivem temporalment la verificació de claus foranes per permetre la creació

-- Inici de la transacció (MySQL ignora BEGIN/COMMIT en mode Autocommit per defecte, però és bona pràctica)
START TRANSACTION;

-- Taules de GESTIÓ D'USUARIS I PERMISOS
------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS usuaris (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    nom VARCHAR(255) NOT NULL,
    cognoms VARCHAR(255) NOT NULL,
    usuari VARCHAR(255) NOT NULL UNIQUE,
    contrasenya VARCHAR(255) NOT NULL,  -- Guardarà el hash de la contrasenya
    correu VARCHAR(255) NOT NULL UNIQUE,
    data_naixement DATE,
    pais VARCHAR(255),
    estat VARCHAR(255),
    provincia VARCHAR(255),
    poblacio VARCHAR(255),
    codi_postal VARCHAR(10),
    address TEXT,
    employment_status VARCHAR(50),
    profession VARCHAR(255),
    phone VARCHAR(50),
    preferred_lang VARCHAR(10),
    spoken_langs TEXT,
    data_creacio DATETIME DEFAULT CURRENT_TIMESTAMP,
    token_activacio TEXT,
    expira_token DATETIME,
    actiu BOOLEAN DEFAULT TRUE, -- BOOLEAN es mapeja a TINYINT(1)
    INDEX idx_usuaris_correu (correu),
    INDEX idx_usuaris_data_creacio (data_creacio)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS grups (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    nom VARCHAR(255) NOT NULL UNIQUE,
    descripcio TEXT,
    data_creacio DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_grups_nom (nom)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS usuaris_grups (
    usuari_id INT UNSIGNED NOT NULL,
    grup_id INT UNSIGNED NOT NULL,
    data_afegit DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (usuari_id, grup_id),
    FOREIGN KEY (usuari_id) REFERENCES usuaris(id) ON DELETE CASCADE,
    FOREIGN KEY (grup_id) REFERENCES grups(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS politiques (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    nom VARCHAR(255) NOT NULL UNIQUE,
    descripcio TEXT,
    permisos TEXT NOT NULL,  -- JSON o text amb els permisos específics
    data_creacio DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_politiques_nom (nom)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS usuaris_politiques (
    usuari_id INT UNSIGNED NOT NULL,
    politica_id INT UNSIGNED NOT NULL,
    data_assignacio DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (usuari_id, politica_id),
    FOREIGN KEY (usuari_id) REFERENCES usuaris(id) ON DELETE CASCADE,
    FOREIGN KEY (politica_id) REFERENCES politiques(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS grups_politiques (
    grup_id INT UNSIGNED NOT NULL,
    politica_id INT UNSIGNED NOT NULL,
    data_assignacio DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (grup_id, politica_id),
    FOREIGN KEY (grup_id) REFERENCES grups(id) ON DELETE CASCADE,
    FOREIGN KEY (politica_id) REFERENCES politiques(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Taules de DADES DE PERSONES
------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS persona (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    nom VARCHAR(255) NOT NULL,
    cognom1 VARCHAR(255),
    cognom2 VARCHAR(255),
    municipi VARCHAR(255),
    arquevisbat VARCHAR(255),
    nom_complet TEXT,
    pagina VARCHAR(50),
    llibre VARCHAR(50),
    quinta VARCHAR(4),
    data_naixement DATE,
    data_bateig DATE,
    data_defuncio DATE,
    ofici VARCHAR(255),
    estat_civil VARCHAR(50),
    created_by INT UNSIGNED NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    updated_by INT UNSIGNED NULL,
    moderated_by INT UNSIGNED NULL,
    moderated_at DATETIME NULL,
    INDEX idx_persona_cognoms_quinta_llibre_pagina (cognom1, cognom2, quinta, llibre, pagina),
    FULLTEXT INDEX idx_persona_nom_complet (nom_complet),
    INDEX idx_persona_municipi_quinta (municipi, quinta),
    INDEX idx_persona_ofici (ofici),
    CONSTRAINT fk_persona_created_by FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL,
    CONSTRAINT fk_persona_updated_by FOREIGN KEY (updated_by) REFERENCES usuaris(id) ON DELETE SET NULL,
    CONSTRAINT fk_persona_moderated_by FOREIGN KEY (moderated_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS relacions (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    persona_id INT UNSIGNED NOT NULL,
    tipus_relacio VARCHAR(50) NOT NULL, -- ex: "pare", "mare", "casat", etc.
    nom VARCHAR(255),
    cognom1 VARCHAR(255),
    cognom2 VARCHAR(255),
    municipi VARCHAR(255),
    ofici VARCHAR(255),
    data_matrimoni VARCHAR(50),
    FOREIGN KEY(persona_id) REFERENCES persona(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS persona_possibles_duplicats (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    nom VARCHAR(255) NOT NULL,
    cognom1 VARCHAR(255) NOT NULL,
    cognom2 VARCHAR(255) NOT NULL,
    municipi VARCHAR(255),
    arquevisbat VARCHAR(255),
    nom_complet TEXT,
    pagina VARCHAR(50),
    llibre VARCHAR(50),
    quinta VARCHAR(4)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Taules de DADES GEOGRÀFIQUES I HISTÒRIQUES
------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS paisos (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    codi_iso2 VARCHAR(2) UNIQUE,
    codi_iso3 VARCHAR(3) UNIQUE,
    codi_pais_num VARCHAR(10),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS nivells_administratius (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    pais_id INT UNSIGNED,
    nivel TINYINT CHECK(nivel BETWEEN 1 AND 7),
    nom_nivell VARCHAR(255),
    tipus_nivell VARCHAR(50),
    codi_oficial VARCHAR(50),
    altres TEXT,
    parent_id INT UNSIGNED,
    any_inici SMALLINT,
    any_fi SMALLINT,
    estat ENUM('actiu', 'inactiu', 'fusionat', 'abolit') DEFAULT 'actiu',
    created_by INT UNSIGNED NULL,
    moderation_status ENUM('pendent','publicat','rebutjat') DEFAULT 'pendent',
    moderated_by INT UNSIGNED NULL,
    moderated_at DATETIME,
    moderation_notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pais_id) REFERENCES paisos(id),
    FOREIGN KEY (parent_id) REFERENCES nivells_administratius(id),
    FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL,
    FOREIGN KEY (moderated_by) REFERENCES usuaris(id) ON DELETE SET NULL,
    INDEX idx_tipus_nivell (tipus_nivell),
    UNIQUE KEY idx_nivell_pais_nom (pais_id, nivel, nom_nivell)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS municipis (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    nom VARCHAR(255) NOT NULL,
    municipi_id INT UNSIGNED REFERENCES municipis(id),
    tipus VARCHAR(50) NOT NULL,
    nivell_administratiu_id_1 INT UNSIGNED REFERENCES nivells_administratius(id),
    nivell_administratiu_id_2 INT UNSIGNED REFERENCES nivells_administratius(id),
    nivell_administratiu_id_3 INT UNSIGNED REFERENCES nivells_administratius(id),
    nivell_administratiu_id_4 INT UNSIGNED REFERENCES nivells_administratius(id),
    nivell_administratiu_id_5 INT UNSIGNED REFERENCES nivells_administratius(id),
    nivell_administratiu_id_6 INT UNSIGNED REFERENCES nivells_administratius(id),
    nivell_administratiu_id_7 INT UNSIGNED REFERENCES nivells_administratius(id),
    codi_postal VARCHAR(10),
    latitud DECIMAL(10, 6),
    longitud DECIMAL(10, 6),
    what3words VARCHAR(255),
    web VARCHAR(255),
    wikipedia VARCHAR(255),
    altres TEXT,
    estat ENUM('actiu', 'inactiu', 'abandonat') DEFAULT 'actiu',
    created_by INT UNSIGNED NULL,
    moderation_status ENUM('pendent','publicat','rebutjat') DEFAULT 'pendent',
    moderated_by INT UNSIGNED NULL,
    moderated_at DATETIME,
    moderation_notes TEXT,
    data_creacio DATETIME DEFAULT CURRENT_TIMESTAMP,
    ultima_modificacio DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_codi_postal (codi_postal),
    INDEX idx_nom_municipi (nom),
    FOREIGN KEY (nivell_administratiu_id_1) REFERENCES nivells_administratius(id),
    FOREIGN KEY (nivell_administratiu_id_2) REFERENCES nivells_administratius(id),
    FOREIGN KEY (nivell_administratiu_id_3) REFERENCES nivells_administratius(id),
    FOREIGN KEY (nivell_administratiu_id_4) REFERENCES nivells_administratius(id),
    FOREIGN KEY (nivell_administratiu_id_5) REFERENCES nivells_administratius(id),
    FOREIGN KEY (nivell_administratiu_id_6) REFERENCES nivells_administratius(id),
    FOREIGN KEY (nivell_administratiu_id_7) REFERENCES nivells_administratius(id),
    FOREIGN KEY (municipi_id) REFERENCES municipis(id),
    FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL,
    FOREIGN KEY (moderated_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS noms_historics (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,

    -- Tipus d'entitat a la qual fa referència aquest nom històric:
    --   municipi       -> entitat_id apunta a municipis.id
    --   nivell_admin   -> entitat_id apunta a nivells_administratius.id
    --   eclesiastic    -> entitat_id apunta a arquebisbats.id
    entitat_tipus ENUM('municipi', 'nivell_admin', 'eclesiastic') NOT NULL,
    entitat_id INT UNSIGNED NOT NULL,

    nom VARCHAR(255) NOT NULL,
    any_inici SMALLINT,
    any_fi SMALLINT,

    pais_regne VARCHAR(255),
    distribucio_geografica VARCHAR(255),
    font TEXT,

    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS arquebisbats (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    nom VARCHAR(255) NOT NULL UNIQUE,
    tipus_entitat ENUM('arquebisbat', 'bisbat', 'diocesi', 'viscomtat', 'vegueria', 'altres'),
    pais_id INT UNSIGNED,
    nivell TINYINT,
    parent_id INT UNSIGNED,
    any_inici SMALLINT,
    any_fi SMALLINT,
    web VARCHAR(255),
    web_arxiu VARCHAR(255),
    web_wikipedia VARCHAR(255),
    territori TEXT,
    observacions TEXT,
    created_by INT UNSIGNED NULL,
    moderation_status ENUM('pendent','publicat','rebutjat') DEFAULT 'pendent',
    moderated_by INT UNSIGNED NULL,
    moderated_at DATETIME,
    moderation_notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pais_id) REFERENCES paisos(id),
    FOREIGN KEY (parent_id) REFERENCES arquebisbats(id),
    FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL,
    FOREIGN KEY (moderated_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS arquebisbats_municipi (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    id_municipi INT UNSIGNED NOT NULL,
    id_arquevisbat INT UNSIGNED NOT NULL,
    any_inici SMALLINT,
    any_fi SMALLINT,
    motiu TEXT,
    font TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_municipi) REFERENCES municipis(id),
    FOREIGN KEY (id_arquevisbat) REFERENCES arquebisbats(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS codis_postals (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    id_municipi INT UNSIGNED NOT NULL,
    codi_postal VARCHAR(10) NOT NULL,
    zona VARCHAR(255),
    desde DATE,
    fins DATE,
    font TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_municipi) REFERENCES municipis(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS llibres (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    arquevisbat_id INT UNSIGNED NOT NULL,
    municipi_id INT UNSIGNED NOT NULL,
    nom_esglesia VARCHAR(255),
    codi_digital VARCHAR(50),
    codi_fisic VARCHAR(50),
    titol VARCHAR(255),
    tipus_llibre VARCHAR(50),
    cronologia VARCHAR(255),
    volum VARCHAR(255),
    abat VARCHAR(255),
    contingut TEXT,
    llengua VARCHAR(50),
    requeriments_tecnics TEXT,
    unitat_catalogacio VARCHAR(50),
    unitat_instalacio VARCHAR(50),
    pagines INT,
    url_base VARCHAR(255),
    url_imatge_prefix VARCHAR(50) DEFAULT '#imatge-',
    pagina VARCHAR(50),
    created_by INT UNSIGNED NULL,
    moderation_status ENUM('pendent','publicat','rebutjat') DEFAULT 'pendent',
    moderated_by INT UNSIGNED NULL,
    moderated_at DATETIME,
    moderation_notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(arquevisbat_id) REFERENCES arquebisbats(id) ON DELETE CASCADE,
    FOREIGN KEY(municipi_id) REFERENCES municipis(id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL,
    FOREIGN KEY (moderated_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Taules de GESTIÓ DE SESSIONS
------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sessions (
  id           INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  usuari_id    INT UNSIGNED NOT NULL,
  token_hash   VARCHAR(64) NOT NULL UNIQUE,
  expira       DATETIME,
  creat        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  revocat      BOOLEAN NOT NULL DEFAULT FALSE,
  FOREIGN KEY (usuari_id) REFERENCES usuaris(id) ON DELETE CASCADE,
  INDEX idx_sessions_user (usuari_id),
  INDEX idx_sessions_revocat (revocat)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS session_access_log (
  id         INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  session_id INT UNSIGNED NOT NULL,
  ts         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ip         VARCHAR(45) NOT NULL,
  FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
  INDEX idx_access_session_ts (session_id, ts DESC),
  INDEX idx_access_ip_ts (ip, ts DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS password_resets (
  id         INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  usuari_id  INT UNSIGNED NOT NULL,
  token      VARCHAR(128) NOT NULL UNIQUE,
  expira     DATETIME NOT NULL,
  lang       VARCHAR(10),
  used       BOOLEAN NOT NULL DEFAULT FALSE,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (usuari_id) REFERENCES usuaris(id) ON DELETE CASCADE,
  INDEX idx_password_resets_token (token),
  INDEX idx_password_resets_expira (expira)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS user_privacy (
  usuari_id INT UNSIGNED NOT NULL PRIMARY KEY,
  nom_visibility VARCHAR(10) DEFAULT 'private',
  cognoms_visibility VARCHAR(10) DEFAULT 'private',
  email_visibility VARCHAR(10) DEFAULT 'private',
  birth_visibility VARCHAR(10) DEFAULT 'private',
  pais_visibility VARCHAR(10) DEFAULT 'public',
  estat_visibility VARCHAR(10) DEFAULT 'private',
  provincia_visibility VARCHAR(10) DEFAULT 'private',
  poblacio_visibility VARCHAR(10) DEFAULT 'private',
  postal_visibility VARCHAR(10) DEFAULT 'private',
  address_visibility VARCHAR(10) DEFAULT 'private',
  employment_visibility VARCHAR(10) DEFAULT 'private',
  profession_visibility VARCHAR(10) DEFAULT 'private',
  phone_visibility VARCHAR(10) DEFAULT 'private',
  preferred_lang_visibility VARCHAR(10) DEFAULT 'private',
  spoken_langs_visibility VARCHAR(10) DEFAULT 'private',
  show_activity BOOLEAN NOT NULL DEFAULT TRUE,
  profile_public BOOLEAN NOT NULL DEFAULT TRUE,
  notify_email BOOLEAN NOT NULL DEFAULT TRUE,
  allow_contact BOOLEAN NOT NULL DEFAULT TRUE,
  FOREIGN KEY (usuari_id) REFERENCES usuaris(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS email_changes (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  usuari_id INT UNSIGNED NOT NULL,
  old_email VARCHAR(255) NOT NULL,
  new_email VARCHAR(255) NOT NULL,
  token_confirm VARCHAR(128) NOT NULL UNIQUE,
  exp_confirm DATETIME NOT NULL,
  token_revert VARCHAR(128) NOT NULL UNIQUE,
  exp_revert DATETIME NOT NULL,
  lang VARCHAR(10),
  confirmed BOOLEAN NOT NULL DEFAULT FALSE,
  reverted BOOLEAN NOT NULL DEFAULT FALSE,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (usuari_id) REFERENCES usuaris(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =====================================================================
-- Sistema de punts per activitats dels usuaris
-- =====================================================================

CREATE TABLE IF NOT EXISTS punts_regles (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    codi VARCHAR(100) NOT NULL UNIQUE,
    nom VARCHAR(255) NOT NULL,
    descripcio TEXT,

    punts INT NOT NULL,
    actiu BOOLEAN NOT NULL DEFAULT TRUE,

    data_creacio DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS usuaris_activitat (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,

    usuari_id INT UNSIGNED NOT NULL,
    regla_id INT UNSIGNED NULL,

    accio VARCHAR(50) NOT NULL,

    objecte_tipus VARCHAR(50) NOT NULL,
    objecte_id INT UNSIGNED NULL,

    punts INT NOT NULL DEFAULT 0,

    estat ENUM('pendent','validat','anulat') NOT NULL DEFAULT 'validat',

    moderat_per INT UNSIGNED NULL,

    detalls TEXT,

    data_creacio DATETIME DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (usuari_id) REFERENCES usuaris(id) ON DELETE CASCADE,
    FOREIGN KEY (regla_id) REFERENCES punts_regles(id),
    FOREIGN KEY (moderat_per) REFERENCES usuaris(id) ON DELETE SET NULL,
    INDEX idx_usuaris_activitat_usuari_data (usuari_id, data_creacio DESC),
    INDEX idx_usuaris_activitat_objecte (objecte_tipus, objecte_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS usuaris_punts (
    usuari_id INT UNSIGNED NOT NULL PRIMARY KEY,
    punts_total INT NOT NULL DEFAULT 0,
    ultima_actualitzacio DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (usuari_id) REFERENCES usuaris(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =====================================================================
-- Arxius / Custòdia (físic o digital) + Estat d'indexació per pàgina
-- =====================================================================

CREATE TABLE IF NOT EXISTS arxius (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  nom VARCHAR(255) NOT NULL UNIQUE,
  tipus VARCHAR(50),
  municipi_id INT UNSIGNED NULL,
  entitat_eclesiastica_id INT UNSIGNED NULL,
  adreca TEXT,
  ubicacio TEXT,
  web VARCHAR(255),
  acces VARCHAR(20),
  notes TEXT,
  created_by INT UNSIGNED NULL,
  moderation_status ENUM('pendent','publicat','rebutjat') DEFAULT 'pendent',
  moderated_by INT UNSIGNED NULL,
  moderated_at DATETIME,
  moderation_notes TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (municipi_id) REFERENCES municipis(id) ON DELETE SET NULL,
  FOREIGN KEY (entitat_eclesiastica_id) REFERENCES arquebisbats(id) ON DELETE SET NULL,
  FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL,
  FOREIGN KEY (moderated_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS arxius_llibres;
CREATE TABLE IF NOT EXISTS arxius_llibres (
  arxiu_id INT UNSIGNED NOT NULL,
  llibre_id INT UNSIGNED NOT NULL,
  signatura VARCHAR(255),
  url_override TEXT,
  PRIMARY KEY (arxiu_id, llibre_id),
  FOREIGN KEY (arxiu_id) REFERENCES arxius(id) ON DELETE CASCADE,
  FOREIGN KEY (llibre_id) REFERENCES llibres(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS llibre_pagines;
CREATE TABLE IF NOT EXISTS llibre_pagines (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  llibre_id INT UNSIGNED NOT NULL,
  num_pagina INT NOT NULL,
  estat VARCHAR(10) NOT NULL DEFAULT 'pendent',
  indexed_at DATETIME,
  indexed_by INT UNSIGNED NULL,
  notes TEXT,
  UNIQUE KEY uq_llibre_pagines (llibre_id, num_pagina),
  CONSTRAINT chk_llibre_pagines_estat CHECK (estat IN ('pendent','indexant','indexada','revisio','error')),
  FOREIGN KEY (llibre_id) REFERENCES llibres(id) ON DELETE CASCADE,
  FOREIGN KEY (indexed_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Índexs per accelerar consultes habituals
CREATE INDEX idx_arxius_llibres_arxiu  ON arxius_llibres(arxiu_id);
CREATE INDEX idx_arxius_llibres_llibre ON arxius_llibres(llibre_id);
CREATE INDEX idx_llibre_pagines_estat  ON llibre_pagines(llibre_id, estat);

-- Transcripcions RAW de registres
DROP TABLE IF EXISTS transcripcions_atributs_raw;
DROP TABLE IF EXISTS transcripcions_persones_raw;
DROP TABLE IF EXISTS transcripcions_raw;
CREATE TABLE IF NOT EXISTS transcripcions_raw (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  llibre_id INT UNSIGNED NOT NULL,
  pagina_id INT UNSIGNED NULL,
  num_pagina_text VARCHAR(50),
  posicio_pagina INT,
  tipus_acte VARCHAR(50),
  any_doc INT,
  data_acte_text TEXT,
  data_acte_iso DATE,
  data_acte_estat VARCHAR(20),
  transcripcio_literal TEXT,
  notes_marginals TEXT,
  observacions_paleografiques TEXT,
  moderation_status ENUM('pendent','publicat','rebutjat') DEFAULT 'pendent',
  moderated_by INT UNSIGNED NULL,
  moderated_at DATETIME,
  moderation_notes TEXT,
  created_by INT UNSIGNED NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT chk_transcripcions_raw_estat CHECK (data_acte_estat IN ('clar','dubtos','incomplet','illegible','no_consta')),
  FOREIGN KEY (llibre_id) REFERENCES llibres(id) ON DELETE CASCADE,
  FOREIGN KEY (pagina_id) REFERENCES llibre_pagines(id) ON DELETE SET NULL,
  FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL,
  FOREIGN KEY (moderated_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS transcripcions_persones_raw (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  transcripcio_id INT UNSIGNED NOT NULL,
  rol VARCHAR(50),
  nom VARCHAR(255),
  nom_estat VARCHAR(20),
  cognom1 VARCHAR(255),
  cognom1_estat VARCHAR(20),
  cognom2 VARCHAR(255),
  cognom2_estat VARCHAR(20),
  sexe VARCHAR(10),
  sexe_estat VARCHAR(20),
  edat_text VARCHAR(100),
  edat_estat VARCHAR(20),
  estat_civil_text VARCHAR(100),
  estat_civil_estat VARCHAR(20),
  municipi_text VARCHAR(255),
  municipi_estat VARCHAR(20),
  ofici_text VARCHAR(255),
  ofici_estat VARCHAR(20),
  casa_nom VARCHAR(255),
  casa_estat VARCHAR(20),
  persona_id INT UNSIGNED NULL,
  linked_by INT UNSIGNED NULL,
  linked_at TIMESTAMP NULL,
  notes TEXT,
  FOREIGN KEY (transcripcio_id) REFERENCES transcripcions_raw(id) ON DELETE CASCADE,
  FOREIGN KEY (persona_id) REFERENCES persona(id) ON DELETE SET NULL,
  FOREIGN KEY (linked_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS transcripcions_atributs_raw (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  transcripcio_id INT UNSIGNED NOT NULL,
  clau VARCHAR(100),
  tipus_valor VARCHAR(20),
  valor_text TEXT,
  valor_int INT,
  valor_date DATE,
  valor_bool TINYINT(1),
  estat VARCHAR(20),
  notes TEXT,
  FOREIGN KEY (transcripcio_id) REFERENCES transcripcions_raw(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS transcripcions_raw_drafts (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  llibre_id INT UNSIGNED NOT NULL,
  user_id INT UNSIGNED NOT NULL,
  payload MEDIUMTEXT NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uniq_transcripcions_raw_drafts (llibre_id, user_id),
  FOREIGN KEY (llibre_id) REFERENCES llibres(id) ON DELETE CASCADE,
  FOREIGN KEY (user_id) REFERENCES usuaris(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_transcripcions_raw_llibre_pagina
  ON transcripcions_raw(llibre_id, pagina_id, posicio_pagina);
CREATE INDEX idx_transcripcions_raw_llibre_tipus_any
  ON transcripcions_raw(llibre_id, tipus_acte, any_doc);
CREATE INDEX idx_transcripcions_raw_status
  ON transcripcions_raw(moderation_status);

CREATE INDEX idx_transcripcions_persones_raw_rol
  ON transcripcions_persones_raw(transcripcio_id, rol);
CREATE INDEX idx_transcripcions_persones_raw_nom
  ON transcripcions_persones_raw(cognom1, cognom2, nom);

CREATE INDEX idx_transcripcions_atributs_raw_clau
  ON transcripcions_atributs_raw(clau);
CREATE INDEX idx_transcripcions_atributs_raw_transcripcio
  ON transcripcions_atributs_raw(transcripcio_id, clau);

-- Cerca per cognoms i nom (per coincidències exactes)
-- CREATE INDEX idx_persona_nom_complet ON persona(nom_complet);
-- Útil per cerca de persones per municipi i quinta (ex: nascuts al mateix lloc i època)
-- CREATE INDEX idx_persona_municipi_quinta ON persona(municipi, quinta);
-- Cercar per ofici o estat civil
-- CREATE INDEX idx_persona_ofici ON persona(ofici);

COMMIT;

SET FOREIGN_KEY_CHECKS = 1; -- Tornem a activar la verificació de claus foranes
