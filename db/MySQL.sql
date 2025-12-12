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
    INDEX idx_persona_cognoms_quinta_llibre_pagina (cognom1, cognom2, quinta, llibre, pagina),
    FULLTEXT INDEX idx_persona_nom_complet (nom_complet),
    INDEX idx_persona_municipi_quinta (municipi, quinta),
    INDEX idx_persona_ofici (ofici)
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
    codi_iso2 VARCHAR(2) UNIQUE, -- Codi ISO 3166-1 alpha-2 ex: ES, FR, US, CA... (VARCHAR en lloc de TEXT(2))
    codi_iso3 VARCHAR(3) UNIQUE, -- Codi ISO 3166-1 alpha-3 ex: ESP, FRA, USA (VARCHAR en lloc de TEXT(3))
    codi_pais_num VARCHAR(10), -- Codi numèric ISO 3166-1
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
    estat ENUM('actiu', 'inactiu', 'fusionat', 'abolit') DEFAULT 'actiu', -- ENUM per als CHECKS
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pais_id) REFERENCES paisos(id),
    INDEX idx_tipus_nivell (tipus_nivell)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS municipis (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    nom VARCHAR(255) NOT NULL,
    municipi_id INT UNSIGNED REFERENCES municipis(id), -- Opcional: si el poble pertany a un altre municipi
    tipus VARCHAR(50) NOT NULL,
    nivell_administratiu_id_1 INT UNSIGNED REFERENCES nivells_administratius(id), -- País
    nivell_administratiu_id_2 INT UNSIGNED REFERENCES nivells_administratius(id), -- Regió / Comunitat Autònoma
    nivell_administratiu_id_3 INT UNSIGNED REFERENCES nivells_administratius(id), -- Província
    nivell_administratiu_id_4 INT UNSIGNED REFERENCES nivells_administratius(id), -- Comarca / àrea local
    nivell_administratiu_id_5 INT UNSIGNED REFERENCES nivells_administratius(id), -- Àrea local
    nivell_administratiu_id_6 INT UNSIGNED REFERENCES nivells_administratius(id), -- Municipi
    nivell_administratiu_id_7 INT UNSIGNED REFERENCES nivells_administratius(id), -- Barri
    codi_postal VARCHAR(10),
    latitud DECIMAL(10, 6), -- REAL canviat a DECIMAL per precisió
    longitud DECIMAL(10, 6), -- REAL canviat a DECIMAL per precisió
    what3words VARCHAR(255),
    web VARCHAR(255),
    wikipedia VARCHAR(255),
    altres TEXT,
    estat ENUM('actiu', 'inactiu', 'abandonat') DEFAULT 'actiu',
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
    FOREIGN KEY (nivell_administratiu_id_7) REFERENCES nivells_administratius(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS noms_historics (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    id_municipi INT UNSIGNED NOT NULL,
    nom VARCHAR(255) NOT NULL, -- Nom antic o anterior
    any_inici YEAR, -- INTEGER canviat a YEAR per a anys
    any_fi YEAR, -- INTEGER canviat a YEAR per a anys
    pais_regne VARCHAR(255),
    distribucio_geografica VARCHAR(255),
    font TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_municipi) REFERENCES municipis(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS arquebisbats (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    nom VARCHAR(255) NOT NULL UNIQUE,
    tipus_entitat ENUM('arquebisbat', 'bisbat', 'diocesi', 'viscomtat', 'vegueria', 'altres'),
    web VARCHAR(255),
    web_arxiu VARCHAR(255),
    web_wikipedia VARCHAR(255),
    territori TEXT,
    autoritat_superior VARCHAR(255),
    observacions TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS arquebisbats_municipi (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    id_municipi INT UNSIGNED NOT NULL,
    id_arquevisbat INT UNSIGNED NOT NULL,
    any_inici YEAR,
    any_fi YEAR,
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

CREATE TABLE IF NOT EXISTS relacio_comarca_provincia (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    id_municipi INT UNSIGNED NOT NULL,
    comarca VARCHAR(255),
    provincia VARCHAR(255),
    any_inici YEAR,
    any_fi YEAR,
    font TEXT,
    observacions TEXT,
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
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(arquevisbat_id) REFERENCES arquebisbats(id) ON DELETE CASCADE,
    FOREIGN KEY(municipi_id) REFERENCES municipis(id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Taules de GESTIÓ DE SESSIONS
------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sessions (
  id           INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  usuari_id    INT UNSIGNED NOT NULL,
  token_hash   VARCHAR(64) NOT NULL UNIQUE, -- S'assumeix un hash de longitud fixa
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
  ip         VARCHAR(45) NOT NULL,     -- Admet IPv6
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

-- Índexs de la taula 'persona' (Els índexs ja s'han definit dins del CREATE TABLE per claredat en MySQL)
------------------------------------------------------------------------------------------------------------------------

-- Índex compost per millorar la cerca de duplicats i cerques combinades
-- CREATE INDEX idx_persona_cognoms_any_llibre_pagina ON persona(cognom1, cognom2, quinta, llibre, pagina);
-- Cerca per cognoms i nom (per coincidències exactes)
-- CREATE INDEX idx_persona_nom_complet ON persona(nom_complet);
-- Útil per cerca de persones per municipi i quinta (ex: nascuts al mateix lloc i època)
-- CREATE INDEX idx_persona_municipi_quinta ON persona(municipi, quinta);
-- Cercar per ofici o estat civil
-- CREATE INDEX idx_persona_ofici ON persona(ofici);

COMMIT;

SET FOREIGN_KEY_CHECKS = 1; -- Tornem a activar la verificació de claus foranes
