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
    banned BOOLEAN DEFAULT FALSE,
    permissions_version INT NOT NULL DEFAULT 0,
    INDEX idx_usuaris_correu (correu),
    INDEX idx_usuaris_data_creacio (data_creacio)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS user_dashboard_widgets (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id INT UNSIGNED NOT NULL,
    widget_id VARCHAR(120) NOT NULL,
    position INT NOT NULL DEFAULT 0,
    is_hidden BOOLEAN NOT NULL DEFAULT 0,
    settings_json TEXT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_user_widget (user_id, widget_id),
    FOREIGN KEY (user_id) REFERENCES usuaris(id) ON DELETE CASCADE,
    INDEX idx_user_dashboard_widgets_user (user_id),
    INDEX idx_user_dashboard_widgets_order (user_id, position)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS platform_settings (
    setting_key VARCHAR(191) NOT NULL PRIMARY KEY,
    setting_value TEXT NOT NULL,
    updated_by INT UNSIGNED NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS maintenance_windows (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    message TEXT NOT NULL,
    severity VARCHAR(32) NOT NULL DEFAULT 'info',
    show_from DATETIME NOT NULL,
    starts_at DATETIME NOT NULL,
    ends_at DATETIME NOT NULL,
    cta_label VARCHAR(255),
    cta_url VARCHAR(512),
    is_enabled TINYINT(1) NOT NULL DEFAULT 1,
    dismissible TINYINT(1) NOT NULL DEFAULT 1,
    created_by INT UNSIGNED NULL,
    updated_by INT UNSIGNED NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_maintenance_windows_enabled (is_enabled),
    INDEX idx_maintenance_windows_show (show_from, ends_at),
    INDEX idx_maintenance_windows_starts (starts_at, ends_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS transparency_settings (
    setting_key VARCHAR(191) NOT NULL PRIMARY KEY,
    setting_value TEXT NOT NULL,
    updated_by INT UNSIGNED NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS transparency_contributors (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(32) NOT NULL DEFAULT 'other',
    description TEXT,
    amount DECIMAL(12,2) NULL,
    currency VARCHAR(16),
    url VARCHAR(512),
    is_public TINYINT(1) NOT NULL DEFAULT 1,
    sort_order INT NOT NULL DEFAULT 0,
    created_by INT UNSIGNED NULL,
    updated_by INT UNSIGNED NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_transparency_contributors_public (is_public),
    INDEX idx_transparency_contributors_sort (sort_order, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS admin_import_runs (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    import_type VARCHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL,
    created_by INT UNSIGNED NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_admin_import_runs_created (created_at),
    INDEX idx_admin_import_runs_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS admin_jobs (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    kind VARCHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL,
    progress_total INT NOT NULL DEFAULT 0,
    progress_done INT NOT NULL DEFAULT 0,
    payload_json LONGTEXT,
    result_json LONGTEXT,
    error_text TEXT,
    started_at DATETIME NULL,
    finished_at DATETIME NULL,
    created_by INT UNSIGNED NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_admin_jobs_kind (kind),
    INDEX idx_admin_jobs_status (status),
    INDEX idx_admin_jobs_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS admin_audit (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    actor_id INT UNSIGNED NULL,
    action VARCHAR(128) NOT NULL,
    object_type VARCHAR(64) NULL,
    object_id INT NULL,
    metadata_json LONGTEXT,
    ip VARCHAR(45),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_admin_audit_created (created_at),
    INDEX idx_admin_audit_action (action),
    INDEX idx_admin_audit_actor (actor_id),
    INDEX idx_admin_audit_object (object_type, object_id)
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

CREATE TABLE IF NOT EXISTS politica_grants (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    politica_id INT UNSIGNED NOT NULL,
    perm_key VARCHAR(255) NOT NULL,
    scope_type VARCHAR(50) NOT NULL,
    scope_id INT NULL,
    include_children BOOLEAN NOT NULL DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (politica_id) REFERENCES politiques(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_politica_grants_politica ON politica_grants(politica_id);
CREATE INDEX idx_politica_grants_perm ON politica_grants(perm_key);
CREATE INDEX idx_politica_grants_perm_scope ON politica_grants(perm_key, scope_type, scope_id);

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
    municipi_naixement VARCHAR(255),
    municipi_defuncio VARCHAR(255),
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

CREATE TABLE IF NOT EXISTS persona_field_links (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    persona_id INT UNSIGNED NOT NULL,
    field_key VARCHAR(100) NOT NULL,
    registre_id INT UNSIGNED NOT NULL,
    created_by INT UNSIGNED NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_persona_field (persona_id, field_key),
    INDEX idx_persona_field_links_persona (persona_id),
    INDEX idx_persona_field_links_registre (registre_id),
    CONSTRAINT fk_persona_field_links_persona FOREIGN KEY (persona_id) REFERENCES persona(id) ON DELETE CASCADE,
    CONSTRAINT fk_persona_field_links_registre FOREIGN KEY (registre_id) REFERENCES transcripcions_raw(id) ON DELETE CASCADE,
    CONSTRAINT fk_persona_field_links_created_by FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL
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

-- Anecdotari de persona
CREATE TABLE IF NOT EXISTS persona_anecdotari (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    persona_id INT UNSIGNED NOT NULL,
    user_id INT UNSIGNED,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    tag VARCHAR(100),
    status VARCHAR(20) NOT NULL DEFAULT 'pendent',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_persona_anecdotari_persona (persona_id, created_at),
    INDEX idx_persona_anecdotari_status (status, created_at),
    FOREIGN KEY (persona_id) REFERENCES persona(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES usuaris(id) ON DELETE SET NULL
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
    UNIQUE KEY idx_nivell_scope_nom (nivel, (IFNULL(parent_id, -pais_id)), nom_nivell)
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

CREATE TABLE IF NOT EXISTS admin_closure (
    descendant_municipi_id INT UNSIGNED NOT NULL,
    ancestor_type ENUM('pais','nivell','municipi') NOT NULL,
    ancestor_id INT UNSIGNED NOT NULL,
    PRIMARY KEY (descendant_municipi_id, ancestor_type, ancestor_id),
    INDEX idx_admin_closure_ancestor (ancestor_type, ancestor_id),
    INDEX idx_admin_closure_descendant (descendant_municipi_id),
    FOREIGN KEY (descendant_municipi_id) REFERENCES municipis(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS municipi_mapes (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    municipi_id INT UNSIGNED NOT NULL,
    group_type ENUM('actual','historic','community') NOT NULL,
    title VARCHAR(255) NOT NULL,
    period_label VARCHAR(64) NULL,
    period_start INT NULL,
    period_end INT NULL,
    topic VARCHAR(64) NULL,
    current_version_id INT UNSIGNED NULL,
    created_by INT UNSIGNED NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_municipi_mapes_municipi_group (municipi_id, group_type),
    INDEX idx_municipi_mapes_updated (municipi_id, updated_at),
    FOREIGN KEY (municipi_id) REFERENCES municipis(id) ON DELETE CASCADE,
    FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS municipi_mapa_versions (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    mapa_id INT UNSIGNED NOT NULL,
    version INT UNSIGNED NOT NULL,
    status ENUM('draft','pendent','publicat','rebutjat') DEFAULT 'draft',
    data_json LONGTEXT NOT NULL,
    changelog TEXT NOT NULL,
    lock_version INT UNSIGNED NOT NULL DEFAULT 0,
    created_by INT UNSIGNED NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    moderated_by INT UNSIGNED NULL,
    moderated_at DATETIME,
    moderation_notes TEXT,
    UNIQUE KEY idx_municipi_mapa_versions_unique (mapa_id, version),
    INDEX idx_municipi_mapa_versions_status (status, created_at),
    INDEX idx_municipi_mapa_versions_mapa_status (mapa_id, status),
    FOREIGN KEY (mapa_id) REFERENCES municipi_mapes(id) ON DELETE CASCADE,
    FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL,
    FOREIGN KEY (moderated_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Historia del municipi
CREATE TABLE IF NOT EXISTS municipi_historia (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    municipi_id INT UNSIGNED NOT NULL,
    current_general_version_id INT UNSIGNED NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_municipi_historia_municipi (municipi_id),
    FOREIGN KEY (municipi_id) REFERENCES municipis(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS municipi_historia_general_versions (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    historia_id INT UNSIGNED NOT NULL,
    version INT UNSIGNED NOT NULL,
    titol VARCHAR(255) NULL,
    resum TEXT,
    cos_text LONGTEXT NOT NULL,
    tags_json TEXT,
    status ENUM('draft','pendent','publicat','rebutjat') DEFAULT 'draft',
    moderation_notes TEXT,
    lock_version INT UNSIGNED NOT NULL DEFAULT 0,
    created_by INT UNSIGNED NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    moderated_by INT UNSIGNED NULL,
    moderated_at DATETIME,
    UNIQUE KEY idx_municipi_historia_general_unique (historia_id, version),
    INDEX idx_municipi_historia_general_status (status, created_at),
    INDEX idx_municipi_historia_general_historia (historia_id, version),
    FOREIGN KEY (historia_id) REFERENCES municipi_historia(id) ON DELETE CASCADE,
    FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL,
    FOREIGN KEY (moderated_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS municipi_historia_fets (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    municipi_id INT UNSIGNED NOT NULL,
    current_version_id INT UNSIGNED NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_municipi_historia_fets_municipi (municipi_id),
    FOREIGN KEY (municipi_id) REFERENCES municipis(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS municipi_historia_fet_versions (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    fet_id INT UNSIGNED NOT NULL,
    version INT UNSIGNED NOT NULL,
    any_inici INT NULL,
    any_fi INT NULL,
    data_inici VARCHAR(32) NULL,
    data_fi VARCHAR(32) NULL,
    data_display VARCHAR(64) NULL,
    titol VARCHAR(255) NOT NULL,
    resum TEXT,
    cos_text LONGTEXT NOT NULL,
    tags_json TEXT,
    fonts_json TEXT,
    status ENUM('draft','pendent','publicat','rebutjat') DEFAULT 'draft',
    moderation_notes TEXT,
    lock_version INT UNSIGNED NOT NULL DEFAULT 0,
    created_by INT UNSIGNED NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    moderated_by INT UNSIGNED NULL,
    moderated_at DATETIME,
    UNIQUE KEY idx_municipi_historia_fet_unique (fet_id, version),
    INDEX idx_municipi_historia_fet_status (status, created_at),
    INDEX idx_municipi_historia_fet_fet (fet_id, version),
    INDEX idx_municipi_historia_fet_any (any_inici, any_fi),
    FOREIGN KEY (fet_id) REFERENCES municipi_historia_fets(id) ON DELETE CASCADE,
    FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL,
    FOREIGN KEY (moderated_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Anecdotari del municipi
CREATE TABLE IF NOT EXISTS municipi_anecdotari_items (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    municipi_id INT UNSIGNED NOT NULL,
    current_version_id INT UNSIGNED NULL,
    created_by INT UNSIGNED NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_municipi_anecdotari_items_municipi (municipi_id, updated_at),
    FOREIGN KEY (municipi_id) REFERENCES municipis(id) ON DELETE CASCADE,
    FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS municipi_anecdotari_versions (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    item_id INT UNSIGNED NOT NULL,
    version INT UNSIGNED NOT NULL,
    status ENUM('draft','pendent','publicat','rebutjat') DEFAULT 'draft',
    titol VARCHAR(255) NOT NULL,
    tag VARCHAR(64) NOT NULL,
    data_ref VARCHAR(32) NULL,
    text LONGTEXT NOT NULL,
    font_url TEXT,
    moderation_notes TEXT,
    lock_version INT UNSIGNED NOT NULL DEFAULT 0,
    created_by INT UNSIGNED NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    moderated_by INT UNSIGNED NULL,
    moderated_at DATETIME,
    UNIQUE KEY idx_municipi_anecdotari_versions_unique (item_id, version),
    INDEX idx_municipi_anecdotari_versions_status (status, created_at),
    INDEX idx_municipi_anecdotari_versions_item (item_id, version),
    FOREIGN KEY (item_id) REFERENCES municipi_anecdotari_items(id) ON DELETE CASCADE,
    FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL,
    FOREIGN KEY (moderated_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS municipi_anecdotari_comments (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    item_id INT UNSIGNED NOT NULL,
    user_id INT UNSIGNED NOT NULL,
    body TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_municipi_anecdotari_comments_item (item_id, created_at),
    FOREIGN KEY (item_id) REFERENCES municipi_anecdotari_items(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES usuaris(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Demografia del municipi (rollups)
CREATE TABLE IF NOT EXISTS municipi_demografia_any (
    municipi_id INT UNSIGNED NOT NULL,
    `any` INT NOT NULL,
    natalitat INT NOT NULL DEFAULT 0,
    matrimonis INT NOT NULL DEFAULT 0,
    defuncions INT NOT NULL DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (municipi_id, `any`),
    INDEX idx_municipi_demografia_any_municipi_any (municipi_id, `any`),
    FOREIGN KEY (municipi_id) REFERENCES municipis(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS municipi_demografia_meta (
    municipi_id INT UNSIGNED NOT NULL PRIMARY KEY,
    any_min INT NULL,
    any_max INT NULL,
    total_natalitat INT NOT NULL DEFAULT 0,
    total_matrimonis INT NOT NULL DEFAULT 0,
    total_defuncions INT NOT NULL DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (municipi_id) REFERENCES municipis(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Demografia per nivell administratiu (rollups)
CREATE TABLE IF NOT EXISTS nivell_demografia_any (
    nivell_id INT UNSIGNED NOT NULL,
    `any` INT NOT NULL,
    natalitat INT NOT NULL DEFAULT 0,
    matrimonis INT NOT NULL DEFAULT 0,
    defuncions INT NOT NULL DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (nivell_id, `any`),
    INDEX idx_nivell_demografia_any_nivell_any (nivell_id, `any`),
    FOREIGN KEY (nivell_id) REFERENCES nivells_administratius(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS nivell_demografia_meta (
    nivell_id INT UNSIGNED NOT NULL PRIMARY KEY,
    any_min INT NULL,
    any_max INT NULL,
    total_natalitat INT NOT NULL DEFAULT 0,
    total_matrimonis INT NOT NULL DEFAULT 0,
    total_defuncions INT NOT NULL DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (nivell_id) REFERENCES nivells_administratius(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS demografia_queue (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    municipi_id INT UNSIGNED NOT NULL,
    tipus ENUM('natalitat','matrimonis','defuncions') NOT NULL,
    `any` INT NOT NULL,
    delta INT NOT NULL,
    source VARCHAR(64),
    source_id VARCHAR(64),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    processed_at DATETIME NULL,
    UNIQUE KEY idx_demografia_queue_unique (source, source_id, delta),
    INDEX idx_demografia_queue_pending (processed_at),
    FOREIGN KEY (municipi_id) REFERENCES municipis(id) ON DELETE CASCADE
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
    arquevisbat_id INT UNSIGNED NULL,
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
    indexacio_completa TINYINT(1) NOT NULL DEFAULT 0,
    created_by INT UNSIGNED NULL,
    moderation_status ENUM('pendent','publicat','rebutjat') DEFAULT 'pendent',
    moderated_by INT UNSIGNED NULL,
    moderated_at DATETIME,
    moderation_notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(arquevisbat_id) REFERENCES arquebisbats(id) ON DELETE SET NULL,
    FOREIGN KEY(municipi_id) REFERENCES municipis(id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL,
    FOREIGN KEY (moderated_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_llibres_municipi ON llibres(municipi_id);

CREATE TABLE IF NOT EXISTS llibres_indexacio_stats (
    llibre_id INT UNSIGNED NOT NULL PRIMARY KEY,
    total_registres INT NOT NULL DEFAULT 0,
    total_camps INT NOT NULL DEFAULT 0,
    camps_emplenats INT NOT NULL DEFAULT 0,
    percentatge INT NOT NULL DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (llibre_id) REFERENCES llibres(id) ON DELETE CASCADE
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
    INDEX idx_usuaris_activitat_usuari_data (usuari_id, data_creacio),
    INDEX idx_usuaris_activitat_usuari_status_data (usuari_id, estat, data_creacio),
    INDEX idx_usuaris_activitat_usuari_regla_status_data (usuari_id, regla_id, estat, data_creacio),
    INDEX idx_usuaris_activitat_objecte (objecte_tipus, objecte_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS usuaris_punts (
    usuari_id INT UNSIGNED NOT NULL PRIMARY KEY,
    punts_total INT NOT NULL DEFAULT 0,
    ultima_actualitzacio DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (usuari_id) REFERENCES usuaris(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Achievements
CREATE TABLE IF NOT EXISTS achievements (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(100) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    rarity VARCHAR(50) NOT NULL,
    visibility VARCHAR(50) NOT NULL,
    domain VARCHAR(100) NOT NULL,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    is_repeatable BOOLEAN NOT NULL DEFAULT FALSE,
    icon_media_item_id INT UNSIGNED NULL,
    rule_json TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_achievements_domain_enabled (domain, is_enabled),
    INDEX idx_achievements_icon (icon_media_item_id),
    FOREIGN KEY (icon_media_item_id) REFERENCES media_items(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS achievement_events (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(100) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    start_at DATETIME NOT NULL,
    end_at DATETIME NOT NULL,
    scope VARCHAR(50) NOT NULL DEFAULT 'global',
    scope_id INT UNSIGNED NULL,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_achievement_events_code_window (code, is_enabled, start_at, end_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS achievements_user (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id INT UNSIGNED NOT NULL,
    achievement_id INT UNSIGNED NOT NULL,
    awarded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    meta_json TEXT,
    UNIQUE KEY uniq_achievements_user (user_id, achievement_id),
    INDEX idx_achievements_user_user (user_id, awarded_at DESC),
    INDEX idx_achievements_user_achievement (achievement_id),
    FOREIGN KEY (user_id) REFERENCES usuaris(id) ON DELETE CASCADE,
    FOREIGN KEY (achievement_id) REFERENCES achievements(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS achievements_showcase (
    user_id INT UNSIGNED NOT NULL,
    achievement_id INT UNSIGNED NOT NULL,
    slot INT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, slot),
    UNIQUE KEY uniq_achievements_showcase (user_id, achievement_id),
    FOREIGN KEY (user_id) REFERENCES usuaris(id) ON DELETE CASCADE,
    FOREIGN KEY (achievement_id) REFERENCES achievements(id) ON DELETE CASCADE
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
  what3words VARCHAR(255),
  web VARCHAR(255),
  acces VARCHAR(20),
  notes TEXT,
  accepta_donacions TINYINT(1) NOT NULL DEFAULT 0,
  donacions_url VARCHAR(500),
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

CREATE TABLE IF NOT EXISTS arxius_donacions_clicks (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  arxiu_id INT UNSIGNED NOT NULL,
  user_id INT UNSIGNED NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (arxiu_id) REFERENCES arxius(id) ON DELETE CASCADE,
  FOREIGN KEY (user_id) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_arxiu_donacions_clicks_arxiu ON arxius_donacions_clicks(arxiu_id);
CREATE INDEX idx_arxiu_donacions_clicks_created ON arxius_donacions_clicks(created_at);

CREATE TABLE IF NOT EXISTS arxius_llibres (
  arxiu_id INT UNSIGNED NOT NULL,
  llibre_id INT UNSIGNED NOT NULL,
  signatura VARCHAR(255),
  url_override TEXT,
  PRIMARY KEY (arxiu_id, llibre_id),
  FOREIGN KEY (arxiu_id) REFERENCES arxius(id) ON DELETE CASCADE,
  FOREIGN KEY (llibre_id) REFERENCES llibres(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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

CREATE TABLE IF NOT EXISTS llibres_urls (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  llibre_id INT UNSIGNED NOT NULL,
  arxiu_id INT UNSIGNED NULL,
  llibre_ref_id INT UNSIGNED NULL,
  url TEXT NOT NULL,
  tipus VARCHAR(50),
  descripcio TEXT,
  created_by INT UNSIGNED NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_llibres_urls_llibre FOREIGN KEY (llibre_id) REFERENCES llibres(id) ON DELETE CASCADE,
  CONSTRAINT fk_llibres_urls_arxiu FOREIGN KEY (arxiu_id) REFERENCES arxius(id) ON DELETE SET NULL,
  CONSTRAINT fk_llibres_urls_llibre_ref FOREIGN KEY (llibre_ref_id) REFERENCES llibres(id) ON DELETE SET NULL,
  CONSTRAINT fk_llibres_urls_created_by FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL
);
CREATE INDEX idx_llibres_urls_llibre ON llibres_urls(llibre_id);
CREATE INDEX idx_llibres_urls_arxiu ON llibres_urls(arxiu_id);
CREATE INDEX idx_llibres_urls_llibre_ref ON llibres_urls(llibre_ref_id);
CREATE INDEX idx_llibre_pagines_estat  ON llibre_pagines(llibre_id, estat);

-- Media (àlbums + ítems)
CREATE TABLE IF NOT EXISTS media_albums (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  public_id VARCHAR(64) NOT NULL,
  title VARCHAR(255) NOT NULL,
  description TEXT,
  album_type VARCHAR(20) NOT NULL DEFAULT 'other',
  owner_user_id INT UNSIGNED NOT NULL,
  llibre_id INT UNSIGNED NULL,
  moderation_status VARCHAR(20) NOT NULL DEFAULT 'pending',
  visibility VARCHAR(30) NOT NULL DEFAULT 'private',
  restricted_group_id INT UNSIGNED NULL,
  access_policy_id INT UNSIGNED NULL,
  credit_cost INT NOT NULL DEFAULT 0,
  difficulty_score INT NOT NULL DEFAULT 0,
  source_type VARCHAR(30) DEFAULT 'online',
  moderated_by INT UNSIGNED NULL,
  moderated_at DATETIME,
  moderation_notes TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY idx_media_albums_public_id (public_id),
  INDEX idx_media_albums_owner (owner_user_id),
  INDEX idx_media_albums_moderation (moderation_status),
  INDEX idx_media_albums_llibre (llibre_id),
  CONSTRAINT chk_media_album_type CHECK (album_type IN ('book','memorial','photo','achievement_icon','other')),
  CONSTRAINT chk_media_album_status CHECK (moderation_status IN ('pending','approved','rejected')),
  CONSTRAINT chk_media_album_visibility CHECK (visibility IN ('private','registered','public','restricted_group','admins_only','custom_policy')),
  CONSTRAINT chk_media_album_source CHECK (source_type IN ('online','offline_archive','family_private','other')),
  FOREIGN KEY (owner_user_id) REFERENCES usuaris(id) ON DELETE CASCADE,
  FOREIGN KEY (llibre_id) REFERENCES llibres(id) ON DELETE SET NULL,
  FOREIGN KEY (restricted_group_id) REFERENCES grups(id) ON DELETE SET NULL,
  FOREIGN KEY (access_policy_id) REFERENCES politiques(id) ON DELETE SET NULL,
  FOREIGN KEY (moderated_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS media_items (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  public_id VARCHAR(64) NOT NULL,
  album_id INT UNSIGNED NOT NULL,
  title VARCHAR(255),
  original_filename VARCHAR(255),
  mime_type VARCHAR(100),
  byte_size BIGINT,
  width INT,
  height INT,
  checksum_sha256 VARCHAR(64),
  storage_key_original VARCHAR(512) NOT NULL,
  thumb_path VARCHAR(512),
  derivatives_status VARCHAR(20) NOT NULL DEFAULT 'pending',
  moderation_status VARCHAR(20) NOT NULL DEFAULT 'pending',
  moderated_by INT UNSIGNED NULL,
  moderated_at DATETIME,
  moderation_notes TEXT,
  credit_cost INT NOT NULL DEFAULT 0,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY idx_media_items_public_id (public_id),
  INDEX idx_media_items_album (album_id),
  INDEX idx_media_items_moderation (moderation_status),
  CONSTRAINT chk_media_item_status CHECK (derivatives_status IN ('pending','ready','failed')),
  CONSTRAINT chk_media_item_moderation CHECK (moderation_status IN ('pending','approved','rejected')),
  FOREIGN KEY (album_id) REFERENCES media_albums(id) ON DELETE CASCADE,
  FOREIGN KEY (moderated_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS media_item_pages (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  media_item_id INT UNSIGNED NOT NULL,
  llibre_id INT UNSIGNED NULL,
  pagina_id INT UNSIGNED NULL,
  page_order INT DEFAULT 0,
  notes TEXT,
  UNIQUE KEY idx_media_item_pages_unique (media_item_id, pagina_id),
  INDEX idx_media_item_pages_item (media_item_id),
  INDEX idx_media_item_pages_pagina (pagina_id),
  FOREIGN KEY (media_item_id) REFERENCES media_items(id) ON DELETE CASCADE,
  FOREIGN KEY (llibre_id) REFERENCES llibres(id) ON DELETE SET NULL,
  FOREIGN KEY (pagina_id) REFERENCES llibre_pagines(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Media credits + grants
CREATE TABLE IF NOT EXISTS user_credits_ledger (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  user_id INT UNSIGNED NOT NULL,
  delta INT NOT NULL,
  reason VARCHAR(100) NOT NULL,
  ref_type VARCHAR(50) NULL,
  ref_id INT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_user_credits_ledger_user (user_id),
  INDEX idx_user_credits_ledger_ref (ref_type, ref_id),
  FOREIGN KEY (user_id) REFERENCES usuaris(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS media_access_grants (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  user_id INT UNSIGNED NOT NULL,
  media_item_id INT UNSIGNED NOT NULL,
  grant_token VARCHAR(255) NOT NULL,
  expires_at DATETIME NOT NULL,
  credits_spent INT NOT NULL DEFAULT 0,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY idx_media_access_grants_token (grant_token),
  INDEX idx_media_access_grants_lookup (user_id, media_item_id, expires_at),
  FOREIGN KEY (user_id) REFERENCES usuaris(id) ON DELETE CASCADE,
  FOREIGN KEY (media_item_id) REFERENCES media_items(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS media_access_logs (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  user_id INT UNSIGNED NOT NULL,
  media_item_id INT UNSIGNED NOT NULL,
  access_type VARCHAR(20) NOT NULL,
  credits_spent INT NOT NULL DEFAULT 0,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_media_access_logs_user (user_id),
  INDEX idx_media_access_logs_item (media_item_id),
  FOREIGN KEY (user_id) REFERENCES usuaris(id) ON DELETE CASCADE,
  FOREIGN KEY (media_item_id) REFERENCES media_items(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Transcripcions RAW de registres
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

CREATE TABLE IF NOT EXISTS search_docs (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  entity_type ENUM('persona','registre_raw','espai_arbre','espai_persona') NOT NULL,
  entity_id INT UNSIGNED NOT NULL,
  published TINYINT(1) NOT NULL DEFAULT 1,
  municipi_id INT UNSIGNED NULL,
  arxiu_id INT UNSIGNED NULL,
  llibre_id INT UNSIGNED NULL,
  entitat_eclesiastica_id INT UNSIGNED NULL,
  data_acte DATE NULL,
  any_acte INT NULL,
  person_nom_norm TEXT,
  person_cognoms_norm TEXT,
  person_full_norm TEXT,
  person_tokens_norm TEXT,
  cognoms_tokens_norm TEXT,
  person_phonetic TEXT,
  cognoms_phonetic TEXT,
  cognoms_canon TEXT,
  UNIQUE KEY idx_search_docs_entity (entity_type, entity_id),
  INDEX idx_search_docs_any (any_acte),
  INDEX idx_search_docs_data (data_acte),
  INDEX idx_search_docs_municipi (municipi_id),
  INDEX idx_search_docs_arxiu (arxiu_id),
  INDEX idx_search_docs_llibre (llibre_id),
  INDEX idx_search_docs_entitat (entitat_eclesiastica_id),
  INDEX idx_search_docs_full (person_full_norm(191)),
  INDEX idx_search_docs_cognoms_canon (cognoms_canon(191)),
  FOREIGN KEY (municipi_id) REFERENCES municipis(id) ON DELETE SET NULL,
  FOREIGN KEY (arxiu_id) REFERENCES arxius(id) ON DELETE SET NULL,
  FOREIGN KEY (llibre_id) REFERENCES llibres(id) ON DELETE SET NULL,
  FOREIGN KEY (entitat_eclesiastica_id) REFERENCES arquebisbats(id) ON DELETE SET NULL
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
  cognom_soltera VARCHAR(255),
  cognom_soltera_estat VARCHAR(20),
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

CREATE TABLE IF NOT EXISTS transcripcions_raw_page_stats (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  llibre_id INT UNSIGNED NOT NULL,
  pagina_id INT UNSIGNED NULL,
  num_pagina_text VARCHAR(255),
  tipus_pagina ENUM('normal','portada','index','visita','altres') NOT NULL DEFAULT 'normal',
  exclosa TINYINT(1) NOT NULL DEFAULT 0,
  indexacio_completa TINYINT(1) NOT NULL DEFAULT 0,
  duplicada_de VARCHAR(255),
  total_registres INT NOT NULL DEFAULT 0,
  computed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uniq_transcripcions_raw_page_stats (llibre_id, pagina_id, num_pagina_text),
  FOREIGN KEY (llibre_id) REFERENCES llibres(id) ON DELETE CASCADE,
  FOREIGN KEY (pagina_id) REFERENCES llibre_pagines(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS transcripcions_raw_marques (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  transcripcio_id INT UNSIGNED NOT NULL,
  user_id INT UNSIGNED NOT NULL,
  tipus ENUM('consanguini','politic','interes') NOT NULL,
  is_public TINYINT(1) NOT NULL DEFAULT 1,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uniq_transcripcions_raw_marques (transcripcio_id, user_id),
  FOREIGN KEY (transcripcio_id) REFERENCES transcripcions_raw(id) ON DELETE CASCADE,
  FOREIGN KEY (user_id) REFERENCES usuaris(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS transcripcions_raw_canvis (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  transcripcio_id INT UNSIGNED NOT NULL,
  change_type VARCHAR(50) NOT NULL,
  field_key VARCHAR(100) NOT NULL,
  old_value TEXT,
  new_value TEXT,
  metadata TEXT,
  moderation_status ENUM('pendent','publicat','rebutjat') NOT NULL DEFAULT 'pendent',
  moderated_by INT UNSIGNED NULL,
  moderated_at DATETIME NULL,
  moderation_notes TEXT,
  changed_by INT UNSIGNED NULL,
  changed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (transcripcio_id) REFERENCES transcripcions_raw(id) ON DELETE CASCADE,
  FOREIGN KEY (moderated_by) REFERENCES usuaris(id) ON DELETE SET NULL,
  FOREIGN KEY (changed_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS wiki_marques (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  object_type VARCHAR(32) NOT NULL,
  object_id INT UNSIGNED NOT NULL,
  user_id INT UNSIGNED NOT NULL,
  tipus ENUM('consanguini','politic','interes') NOT NULL,
  is_public TINYINT(1) NOT NULL DEFAULT 1,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uniq_wiki_marques (object_type, object_id, user_id),
  INDEX idx_wiki_marques_object (object_type, object_id),
  INDEX idx_wiki_marques_user (user_id),
  FOREIGN KEY (user_id) REFERENCES usuaris(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS wiki_marks_stats (
  object_type VARCHAR(32) NOT NULL,
  object_id INT UNSIGNED NOT NULL,
  tipus ENUM('consanguini','politic','interes') NOT NULL,
  public_count INT UNSIGNED NOT NULL DEFAULT 0,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (object_type, object_id, tipus),
  INDEX idx_wiki_marks_stats_object (object_type, object_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS wiki_canvis (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  object_type VARCHAR(32) NOT NULL,
  object_id INT UNSIGNED NOT NULL,
  change_type VARCHAR(50) NOT NULL,
  field_key VARCHAR(100) NOT NULL,
  old_value TEXT,
  new_value TEXT,
  metadata TEXT,
  moderation_status ENUM('pendent','publicat','rebutjat') NOT NULL DEFAULT 'pendent',
  moderated_by INT UNSIGNED NULL,
  moderated_at DATETIME NULL,
  moderation_notes TEXT,
  changed_by INT UNSIGNED NULL,
  changed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_wiki_canvis_object (object_type, object_id, changed_at),
  INDEX idx_wiki_canvis_status_changed (moderation_status, changed_at),
  FOREIGN KEY (moderated_by) REFERENCES usuaris(id) ON DELETE SET NULL,
  FOREIGN KEY (changed_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS wiki_pending_queue (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  change_id INT UNSIGNED NOT NULL,
  object_type VARCHAR(32) NOT NULL,
  object_id INT UNSIGNED NOT NULL,
  changed_at DATETIME NOT NULL,
  changed_by INT UNSIGNED NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uniq_wiki_pending_change (change_id),
  INDEX idx_wiki_pending_changed_at (changed_at),
  INDEX idx_wiki_pending_object (object_type, object_id),
  FOREIGN KEY (change_id) REFERENCES wiki_canvis(id) ON DELETE CASCADE,
  FOREIGN KEY (changed_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS csv_import_templates (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(200) NOT NULL,
  description TEXT NULL,
  owner_user_id INT UNSIGNED NULL,
  visibility ENUM('private','public') NOT NULL DEFAULT 'private',
  default_separator VARCHAR(8) NULL,
  model_json TEXT NOT NULL,
  signature VARCHAR(128) NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uniq_csv_import_templates_owner_name (owner_user_id, name),
  INDEX idx_csv_import_templates_owner_visibility (owner_user_id, visibility),
  INDEX idx_csv_import_templates_visibility_created (visibility, created_at),
  INDEX idx_csv_import_templates_signature (signature),
  FOREIGN KEY (owner_user_id) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Cognoms (forma canònica)
CREATE TABLE IF NOT EXISTS cognoms (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  forma VARCHAR(255) NOT NULL,
  `key` VARCHAR(255) NOT NULL,
  origen TEXT,
  notes TEXT,
  created_by INT UNSIGNED,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_cognoms_key (`key`),
  INDEX idx_cognoms_forma (forma),
  INDEX idx_cognoms_updated_at (updated_at),
  FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Noms (forma canònica)
CREATE TABLE IF NOT EXISTS noms (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  forma VARCHAR(255) NOT NULL,
  `key` VARCHAR(255) NOT NULL,
  notes TEXT,
  created_by INT UNSIGNED,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_noms_key (`key`),
  INDEX idx_noms_forma (forma),
  INDEX idx_noms_updated_at (updated_at),
  FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Variants de cognom (moderables)
CREATE TABLE IF NOT EXISTS cognom_variants (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  cognom_id INT UNSIGNED NOT NULL,
  variant VARCHAR(255) NOT NULL,
  `key` VARCHAR(255) NOT NULL,
  llengua VARCHAR(20),
  any_inici INT,
  any_fi INT,
  pais_id INT UNSIGNED,
  municipi_id INT UNSIGNED,
  moderation_status VARCHAR(20) DEFAULT 'pendent',
  moderated_by INT UNSIGNED,
  moderated_at DATETIME,
  moderation_notes TEXT,
  created_by INT UNSIGNED,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_cognom_variants (cognom_id, `key`),
  INDEX idx_cognom_variants_status (cognom_id, moderation_status),
  INDEX idx_cognom_variants_key (`key`),
  FOREIGN KEY (cognom_id) REFERENCES cognoms(id) ON DELETE CASCADE,
  FOREIGN KEY (pais_id) REFERENCES paisos(id) ON DELETE SET NULL,
  FOREIGN KEY (municipi_id) REFERENCES municipis(id) ON DELETE SET NULL,
  FOREIGN KEY (moderated_by) REFERENCES usuaris(id) ON DELETE SET NULL,
  FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Redirects de cognoms (alias -> canònic)
CREATE TABLE IF NOT EXISTS cognoms_redirects (
  from_cognom_id INT UNSIGNED NOT NULL PRIMARY KEY,
  to_cognom_id INT UNSIGNED NOT NULL,
  reason TEXT,
  created_by INT UNSIGNED NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_cognoms_redirects_to (to_cognom_id),
  FOREIGN KEY (from_cognom_id) REFERENCES cognoms(id) ON DELETE CASCADE,
  FOREIGN KEY (to_cognom_id) REFERENCES cognoms(id) ON DELETE CASCADE,
  FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Propostes d'unificació de cognoms (moderables)
CREATE TABLE IF NOT EXISTS cognoms_redirects_suggestions (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  from_cognom_id INT UNSIGNED NOT NULL,
  to_cognom_id INT UNSIGNED NOT NULL,
  reason TEXT,
  moderation_status VARCHAR(20) DEFAULT 'pendent',
  moderated_by INT UNSIGNED NULL,
  moderated_at DATETIME NULL,
  moderation_notes TEXT,
  created_by INT UNSIGNED NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_cognoms_redirects_suggestions_status (moderation_status),
  INDEX idx_cognoms_redirects_suggestions_from (from_cognom_id),
  INDEX idx_cognoms_redirects_suggestions_to (to_cognom_id),
  FOREIGN KEY (from_cognom_id) REFERENCES cognoms(id) ON DELETE CASCADE,
  FOREIGN KEY (to_cognom_id) REFERENCES cognoms(id) ON DELETE CASCADE,
  FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL,
  FOREIGN KEY (moderated_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Referències de cognoms (moderables)
CREATE TABLE IF NOT EXISTS cognoms_referencies (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  cognom_id INT UNSIGNED NOT NULL,
  kind VARCHAR(20) NOT NULL,
  ref_id INT UNSIGNED NULL,
  url TEXT,
  titol TEXT,
  descripcio TEXT,
  pagina TEXT,
  moderation_status VARCHAR(20) DEFAULT 'pendent',
  moderated_by INT UNSIGNED NULL,
  moderated_at DATETIME,
  moderation_notes TEXT,
  created_by INT UNSIGNED NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_cognoms_ref_cognom_status (cognom_id, moderation_status),
  INDEX idx_cognoms_ref_kind (kind),
  FOREIGN KEY (cognom_id) REFERENCES cognoms(id) ON DELETE CASCADE,
  FOREIGN KEY (moderated_by) REFERENCES usuaris(id) ON DELETE SET NULL,
  FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Estadístiques pre-agregades per cognom/municipi/any
CREATE TABLE IF NOT EXISTS cognoms_freq_municipi_any (
  cognom_id INT UNSIGNED NOT NULL,
  municipi_id INT UNSIGNED NOT NULL,
  any_doc INT NOT NULL,
  freq INT NOT NULL DEFAULT 0,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (cognom_id, municipi_id, any_doc),
  INDEX idx_cognoms_freq_cognom_any (cognom_id, any_doc),
  INDEX idx_cognoms_freq_municipi_any (municipi_id, any_doc),
  FOREIGN KEY (cognom_id) REFERENCES cognoms(id) ON DELETE CASCADE,
  FOREIGN KEY (municipi_id) REFERENCES municipis(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Totals per cognom/municipi
CREATE TABLE IF NOT EXISTS cognoms_freq_municipi_total (
  cognom_id INT UNSIGNED NOT NULL,
  municipi_id INT UNSIGNED NOT NULL,
  total_freq INT NOT NULL DEFAULT 0,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (cognom_id, municipi_id),
  INDEX idx_cognoms_freq_municipi_total_municipi (municipi_id, total_freq),
  FOREIGN KEY (cognom_id) REFERENCES cognoms(id) ON DELETE CASCADE,
  FOREIGN KEY (municipi_id) REFERENCES municipis(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Estadístiques pre-agregades per cognom/nivell/any
CREATE TABLE IF NOT EXISTS cognoms_freq_nivell_any (
  cognom_id INT UNSIGNED NOT NULL,
  nivell_id INT UNSIGNED NOT NULL,
  any_doc INT NOT NULL,
  freq INT NOT NULL DEFAULT 0,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (cognom_id, nivell_id, any_doc),
  INDEX idx_cognoms_freq_nivell_any (nivell_id, any_doc),
  INDEX idx_cognoms_freq_nivell_any_cognom (cognom_id, any_doc),
  FOREIGN KEY (cognom_id) REFERENCES cognoms(id) ON DELETE CASCADE,
  FOREIGN KEY (nivell_id) REFERENCES nivells_administratius(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Totals per cognom/nivell
CREATE TABLE IF NOT EXISTS cognoms_freq_nivell_total (
  cognom_id INT UNSIGNED NOT NULL,
  nivell_id INT UNSIGNED NOT NULL,
  total_freq INT NOT NULL DEFAULT 0,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (cognom_id, nivell_id),
  INDEX idx_cognoms_freq_nivell_total (nivell_id, total_freq),
  FOREIGN KEY (cognom_id) REFERENCES cognoms(id) ON DELETE CASCADE,
  FOREIGN KEY (nivell_id) REFERENCES nivells_administratius(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Estadístiques globals per cognom
CREATE TABLE IF NOT EXISTS cognoms_stats_total (
  cognom_id INT UNSIGNED NOT NULL,
  total_persones INT NOT NULL DEFAULT 0,
  total_aparicions INT NOT NULL DEFAULT 0,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (cognom_id),
  INDEX idx_cognoms_stats_total_aparicions (total_aparicions),
  FOREIGN KEY (cognom_id) REFERENCES cognoms(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Estadístiques per any per cognom
CREATE TABLE IF NOT EXISTS cognoms_stats_any (
  cognom_id INT UNSIGNED NOT NULL,
  `any` INT NOT NULL,
  total INT NOT NULL DEFAULT 0,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (cognom_id, `any`),
  INDEX idx_cognoms_stats_any_any (`any`),
  FOREIGN KEY (cognom_id) REFERENCES cognoms(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Estadístiques per ancestre (municipi/nivell) i any
CREATE TABLE IF NOT EXISTS cognoms_stats_ancestor_any (
  cognom_id INT UNSIGNED NOT NULL,
  ancestor_type VARCHAR(32) NOT NULL,
  ancestor_id INT UNSIGNED NOT NULL,
  `any` INT NOT NULL,
  total INT NOT NULL DEFAULT 0,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (cognom_id, ancestor_type, ancestor_id, `any`),
  INDEX idx_cognoms_stats_ancestor_cognom_any (cognom_id, ancestor_type, `any`),
  INDEX idx_cognoms_stats_ancestor_id (ancestor_type, ancestor_id),
  FOREIGN KEY (cognom_id) REFERENCES cognoms(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Estadístiques pre-agregades per nom/municipi/any
CREATE TABLE IF NOT EXISTS noms_freq_municipi_any (
  nom_id INT UNSIGNED NOT NULL,
  municipi_id INT UNSIGNED NOT NULL,
  any_doc INT NOT NULL,
  freq INT NOT NULL DEFAULT 0,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (nom_id, municipi_id, any_doc),
  INDEX idx_noms_freq_municipi_any_municipi_any (municipi_id, any_doc),
  INDEX idx_noms_freq_municipi_any_nom_any (nom_id, any_doc),
  FOREIGN KEY (nom_id) REFERENCES noms(id) ON DELETE CASCADE,
  FOREIGN KEY (municipi_id) REFERENCES municipis(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Totals per nom/municipi
CREATE TABLE IF NOT EXISTS noms_freq_municipi_total (
  nom_id INT UNSIGNED NOT NULL,
  municipi_id INT UNSIGNED NOT NULL,
  total_freq INT NOT NULL DEFAULT 0,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (nom_id, municipi_id),
  INDEX idx_noms_freq_municipi_total_municipi (municipi_id, total_freq),
  FOREIGN KEY (nom_id) REFERENCES noms(id) ON DELETE CASCADE,
  FOREIGN KEY (municipi_id) REFERENCES municipis(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Estadístiques pre-agregades per nom/nivell/any
CREATE TABLE IF NOT EXISTS noms_freq_nivell_any (
  nom_id INT UNSIGNED NOT NULL,
  nivell_id INT UNSIGNED NOT NULL,
  any_doc INT NOT NULL,
  freq INT NOT NULL DEFAULT 0,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (nom_id, nivell_id, any_doc),
  INDEX idx_noms_freq_nivell_any (nivell_id, any_doc),
  INDEX idx_noms_freq_nivell_any_nom (nom_id, any_doc),
  FOREIGN KEY (nom_id) REFERENCES noms(id) ON DELETE CASCADE,
  FOREIGN KEY (nivell_id) REFERENCES nivells_administratius(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Totals per nom/nivell
CREATE TABLE IF NOT EXISTS noms_freq_nivell_total (
  nom_id INT UNSIGNED NOT NULL,
  nivell_id INT UNSIGNED NOT NULL,
  total_freq INT NOT NULL DEFAULT 0,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (nom_id, nivell_id),
  INDEX idx_noms_freq_nivell_total (nivell_id, total_freq),
  FOREIGN KEY (nom_id) REFERENCES noms(id) ON DELETE CASCADE,
  FOREIGN KEY (nivell_id) REFERENCES nivells_administratius(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =====================================================================
-- Esdeveniments historics
-- =====================================================================

CREATE TABLE IF NOT EXISTS events_historics (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  titol VARCHAR(255) NOT NULL,
  slug VARCHAR(255) NOT NULL,
  tipus VARCHAR(64) NOT NULL,
  resum TEXT,
  descripcio TEXT,
  data_inici DATE,
  data_fi DATE,
  data_inici_aprox TINYINT(1) NOT NULL DEFAULT 0,
  data_fi_aprox TINYINT(1) NOT NULL DEFAULT 0,
  precisio VARCHAR(20),
  fonts TEXT,
  created_by INT UNSIGNED,
  moderation_status VARCHAR(20) NOT NULL DEFAULT 'pendent',
  moderated_by INT UNSIGNED,
  moderated_at DATETIME,
  moderation_notes TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_events_historics_slug (slug),
  INDEX idx_events_historics_tipus_data_inici (tipus, data_inici),
  INDEX idx_events_historics_moderation (moderation_status, created_at),
  CONSTRAINT chk_events_historics_precisio CHECK (precisio IN ('dia','mes','any','decada')),
  CONSTRAINT chk_events_historics_moderation CHECK (moderation_status IN ('pendent','publicat','rebutjat')),
  FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL,
  FOREIGN KEY (moderated_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS events_historics_impactes (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  event_id INT UNSIGNED NOT NULL,
  scope_type VARCHAR(32) NOT NULL,
  scope_id INT UNSIGNED NOT NULL,
  impacte_tipus VARCHAR(20) NOT NULL,
  intensitat TINYINT NOT NULL,
  notes TEXT,
  created_by INT UNSIGNED,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_events_historics_impactes_event (event_id),
  INDEX idx_events_historics_impactes_scope (scope_type, scope_id),
  INDEX idx_events_historics_impactes_scope_intensitat (scope_type, scope_id, intensitat),
  CONSTRAINT chk_events_historics_impactes_scope CHECK (scope_type IN ('pais','nivell_admin','municipi','entitat_eclesiastica')),
  CONSTRAINT chk_events_historics_impactes_tipus CHECK (impacte_tipus IN ('directe','indirecte','transit','rumor')),
  CONSTRAINT chk_events_historics_impactes_intensitat CHECK (intensitat BETWEEN 1 AND 5),
  FOREIGN KEY (event_id) REFERENCES events_historics(id) ON DELETE CASCADE,
  FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dm_threads (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  user_low_id INT UNSIGNED NOT NULL,
  user_high_id INT UNSIGNED NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  last_message_at DATETIME NULL,
  last_message_id INT UNSIGNED NULL,
  UNIQUE KEY uq_dm_threads_users (user_low_id, user_high_id),
  INDEX idx_dm_threads_user_low (user_low_id),
  INDEX idx_dm_threads_user_high (user_high_id),
  INDEX idx_dm_threads_last_message_at (last_message_at DESC),
  CONSTRAINT chk_dm_threads_order CHECK (user_low_id < user_high_id),
  FOREIGN KEY (user_low_id) REFERENCES usuaris(id) ON DELETE CASCADE,
  FOREIGN KEY (user_high_id) REFERENCES usuaris(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dm_thread_state (
  thread_id INT UNSIGNED NOT NULL,
  user_id INT UNSIGNED NOT NULL,
  last_read_message_id INT UNSIGNED NULL,
  archived TINYINT(1) NOT NULL DEFAULT 0,
  muted TINYINT(1) NOT NULL DEFAULT 0,
  deleted TINYINT(1) NOT NULL DEFAULT 0,
  folder VARCHAR(120) NULL,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (thread_id, user_id),
  INDEX idx_dm_thread_state_user_archived (user_id, archived, updated_at),
  INDEX idx_dm_thread_state_user_deleted (user_id, deleted, updated_at),
  INDEX idx_dm_thread_state_user_folder (user_id, folder),
  FOREIGN KEY (thread_id) REFERENCES dm_threads(id) ON DELETE CASCADE,
  FOREIGN KEY (user_id) REFERENCES usuaris(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dm_messages (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  thread_id INT UNSIGNED NOT NULL,
  sender_id INT UNSIGNED NOT NULL,
  body TEXT NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_dm_messages_thread_created (thread_id, created_at DESC),
  INDEX idx_dm_messages_sender_created (sender_id, created_at DESC),
  FOREIGN KEY (thread_id) REFERENCES dm_threads(id) ON DELETE CASCADE,
  FOREIGN KEY (sender_id) REFERENCES usuaris(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS user_blocks (
  blocker_id INT UNSIGNED NOT NULL,
  blocked_id INT UNSIGNED NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (blocker_id, blocked_id),
  INDEX idx_user_blocks_blocker (blocker_id, created_at),
  INDEX idx_user_blocks_blocked (blocked_id, created_at),
  FOREIGN KEY (blocker_id) REFERENCES usuaris(id) ON DELETE CASCADE,
  FOREIGN KEY (blocked_id) REFERENCES usuaris(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Espai personal
CREATE TABLE IF NOT EXISTS espai_arbres (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  owner_user_id INT UNSIGNED NOT NULL,
  nom VARCHAR(255) NOT NULL,
  descripcio TEXT,
  visibility VARCHAR(20) NOT NULL DEFAULT 'private',
  status VARCHAR(20) NOT NULL DEFAULT 'active',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_espai_arbres_owner FOREIGN KEY (owner_user_id) REFERENCES usuaris(id) ON DELETE CASCADE,
  CONSTRAINT chk_espai_arbres_visibility CHECK (visibility IN ('private','public','restricted')),
  CONSTRAINT chk_espai_arbres_status CHECK (status IN ('active','archived')),
  UNIQUE KEY idx_espai_arbres_owner_name (owner_user_id, nom)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_espai_arbres_owner ON espai_arbres(owner_user_id);
CREATE INDEX idx_espai_arbres_status ON espai_arbres(status);
CREATE INDEX idx_espai_arbres_updated ON espai_arbres(updated_at);

CREATE TABLE IF NOT EXISTS espai_fonts_importacio (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  owner_user_id INT UNSIGNED NOT NULL,
  source_type VARCHAR(20) NOT NULL,
  nom VARCHAR(255),
  original_filename VARCHAR(255),
  storage_path TEXT,
  checksum_sha256 VARCHAR(128),
  size_bytes BIGINT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_espai_fonts_owner FOREIGN KEY (owner_user_id) REFERENCES usuaris(id) ON DELETE CASCADE,
  CONSTRAINT chk_espai_fonts_type CHECK (source_type IN ('gedcom','gramps','manual'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_espai_fonts_owner ON espai_fonts_importacio(owner_user_id);
CREATE INDEX idx_espai_fonts_type ON espai_fonts_importacio(source_type);

CREATE TABLE IF NOT EXISTS espai_imports (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  owner_user_id INT UNSIGNED NOT NULL,
  arbre_id INT UNSIGNED NOT NULL,
  font_id INT UNSIGNED NULL,
  import_type VARCHAR(20) NOT NULL,
  import_mode VARCHAR(20) NOT NULL DEFAULT 'full',
  status VARCHAR(20) NOT NULL,
  progress_total INT NOT NULL DEFAULT 0,
  progress_done INT NOT NULL DEFAULT 0,
  summary_json TEXT,
  error_text TEXT,
  started_at DATETIME NULL,
  finished_at DATETIME NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_espai_imports_owner FOREIGN KEY (owner_user_id) REFERENCES usuaris(id) ON DELETE CASCADE,
  CONSTRAINT fk_espai_imports_arbre FOREIGN KEY (arbre_id) REFERENCES espai_arbres(id) ON DELETE CASCADE,
  CONSTRAINT fk_espai_imports_font FOREIGN KEY (font_id) REFERENCES espai_fonts_importacio(id) ON DELETE SET NULL,
  CONSTRAINT chk_espai_imports_type CHECK (import_type IN ('gedcom','gramps')),
  CONSTRAINT chk_espai_imports_status CHECK (status IN ('queued','parsing','normalizing','persisted','done','error','cancelled'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_espai_imports_owner ON espai_imports(owner_user_id);
CREATE INDEX idx_espai_imports_arbre ON espai_imports(arbre_id);
CREATE INDEX idx_espai_imports_status ON espai_imports(status);
CREATE INDEX idx_espai_imports_updated ON espai_imports(updated_at);
CREATE INDEX idx_espai_imports_type ON espai_imports(import_type);

CREATE TABLE IF NOT EXISTS espai_persones (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  owner_user_id INT UNSIGNED NOT NULL,
  arbre_id INT UNSIGNED NOT NULL,
  external_id VARCHAR(255),
  nom VARCHAR(255),
  cognom1 VARCHAR(255),
  cognom2 VARCHAR(255),
  nom_complet TEXT,
  sexe VARCHAR(20),
  data_naixement VARCHAR(50),
  data_defuncio VARCHAR(50),
  lloc_naixement VARCHAR(255),
  lloc_defuncio VARCHAR(255),
  notes TEXT,
  has_media TINYINT(1) NOT NULL DEFAULT 0,
  visibility VARCHAR(12) NOT NULL DEFAULT 'visible',
  status VARCHAR(20) NOT NULL DEFAULT 'active',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_espai_persones_owner FOREIGN KEY (owner_user_id) REFERENCES usuaris(id) ON DELETE CASCADE,
  CONSTRAINT fk_espai_persones_arbre FOREIGN KEY (arbre_id) REFERENCES espai_arbres(id) ON DELETE CASCADE,
  CONSTRAINT chk_espai_persones_status CHECK (status IN ('active','archived')),
  CONSTRAINT chk_espai_persones_sexe CHECK (sexe IN ('male','female','unknown')),
  CONSTRAINT chk_espai_persones_visibility CHECK (visibility IN ('visible','hidden'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_espai_persones_owner ON espai_persones(owner_user_id);
CREATE INDEX idx_espai_persones_arbre ON espai_persones(arbre_id);
CREATE INDEX idx_espai_persones_visibility ON espai_persones(visibility);
CREATE INDEX idx_espai_persones_status ON espai_persones(status);
CREATE INDEX idx_espai_persones_updated ON espai_persones(updated_at);

CREATE TABLE IF NOT EXISTS espai_relacions (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  arbre_id INT UNSIGNED NOT NULL,
  persona_id INT UNSIGNED NOT NULL,
  related_persona_id INT UNSIGNED NOT NULL,
  relation_type VARCHAR(20) NOT NULL,
  notes TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_espai_relacions_arbre FOREIGN KEY (arbre_id) REFERENCES espai_arbres(id) ON DELETE CASCADE,
  CONSTRAINT fk_espai_relacions_persona FOREIGN KEY (persona_id) REFERENCES espai_persones(id) ON DELETE CASCADE,
  CONSTRAINT fk_espai_relacions_related FOREIGN KEY (related_persona_id) REFERENCES espai_persones(id) ON DELETE CASCADE,
  CONSTRAINT chk_espai_relacions_type CHECK (relation_type IN ('parent','mother','father','spouse','child','sibling'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_espai_relacions_arbre ON espai_relacions(arbre_id);
CREATE INDEX idx_espai_relacions_persona ON espai_relacions(persona_id);
CREATE INDEX idx_espai_relacions_related ON espai_relacions(related_persona_id);

CREATE TABLE IF NOT EXISTS espai_events (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  arbre_id INT UNSIGNED NOT NULL,
  persona_id INT UNSIGNED NOT NULL,
  external_id VARCHAR(255),
  event_type VARCHAR(64) NOT NULL,
  event_role VARCHAR(64),
  event_date VARCHAR(255),
  event_place VARCHAR(255),
  description TEXT,
  source VARCHAR(64),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_espai_events_arbre FOREIGN KEY (arbre_id) REFERENCES espai_arbres(id) ON DELETE CASCADE,
  CONSTRAINT fk_espai_events_persona FOREIGN KEY (persona_id) REFERENCES espai_persones(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_espai_events_arbre ON espai_events(arbre_id);
CREATE INDEX idx_espai_events_persona ON espai_events(persona_id);
CREATE INDEX idx_espai_events_type ON espai_events(event_type);
CREATE INDEX idx_espai_events_source ON espai_events(source);

CREATE TABLE IF NOT EXISTS espai_coincidencies (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  owner_user_id INT UNSIGNED NOT NULL,
  arbre_id INT UNSIGNED NOT NULL,
  persona_id INT UNSIGNED NOT NULL,
  target_type VARCHAR(20) NOT NULL,
  target_id INT UNSIGNED NOT NULL,
  score DOUBLE,
  reason_json TEXT,
  status VARCHAR(20) NOT NULL DEFAULT 'pending',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_espai_coincidencies_owner FOREIGN KEY (owner_user_id) REFERENCES usuaris(id) ON DELETE CASCADE,
  CONSTRAINT fk_espai_coincidencies_arbre FOREIGN KEY (arbre_id) REFERENCES espai_arbres(id) ON DELETE CASCADE,
  CONSTRAINT fk_espai_coincidencies_persona FOREIGN KEY (persona_id) REFERENCES espai_persones(id) ON DELETE CASCADE,
  CONSTRAINT chk_espai_coincidencies_status CHECK (status IN ('pending','accepted','rejected','ignored')),
  CONSTRAINT chk_espai_coincidencies_target CHECK (target_type IN ('persona','registre_raw'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_espai_coincidencies_owner ON espai_coincidencies(owner_user_id);
CREATE INDEX idx_espai_coincidencies_arbre ON espai_coincidencies(arbre_id);
CREATE INDEX idx_espai_coincidencies_status ON espai_coincidencies(status);
CREATE INDEX idx_espai_coincidencies_updated ON espai_coincidencies(updated_at);
CREATE INDEX idx_espai_coincidencies_target ON espai_coincidencies(target_type, target_id);

CREATE TABLE IF NOT EXISTS espai_decisions_coincidencia (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  coincidencia_id INT UNSIGNED NOT NULL,
  decision VARCHAR(20) NOT NULL,
  decided_by INT UNSIGNED NULL,
  notes TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_espai_decisions_coincidencia FOREIGN KEY (coincidencia_id) REFERENCES espai_coincidencies(id) ON DELETE CASCADE,
  CONSTRAINT fk_espai_decisions_user FOREIGN KEY (decided_by) REFERENCES usuaris(id) ON DELETE SET NULL,
  CONSTRAINT chk_espai_decisions_decision CHECK (decision IN ('accept','reject','ignore','undo'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_espai_decisions_match ON espai_decisions_coincidencia(coincidencia_id);
CREATE INDEX idx_espai_decisions_user ON espai_decisions_coincidencia(decided_by);

CREATE TABLE IF NOT EXISTS espai_integracions_gramps (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  owner_user_id INT UNSIGNED NOT NULL,
  arbre_id INT UNSIGNED NOT NULL,
  base_url TEXT NOT NULL,
  username VARCHAR(255) NULL,
  token TEXT,
  status VARCHAR(20) NOT NULL DEFAULT 'connected',
  last_sync_at DATETIME NULL,
  last_error TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_espai_integracions_owner FOREIGN KEY (owner_user_id) REFERENCES usuaris(id) ON DELETE CASCADE,
  CONSTRAINT fk_espai_integracions_arbre FOREIGN KEY (arbre_id) REFERENCES espai_arbres(id) ON DELETE CASCADE,
  CONSTRAINT chk_espai_integracions_status CHECK (status IN ('connected','error','disabled'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_espai_integracions_owner ON espai_integracions_gramps(owner_user_id);
CREATE INDEX idx_espai_integracions_arbre ON espai_integracions_gramps(arbre_id);
CREATE INDEX idx_espai_integracions_status ON espai_integracions_gramps(status);

CREATE TABLE IF NOT EXISTS espai_integracions_gramps_logs (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  integracio_id INT UNSIGNED NOT NULL,
  status VARCHAR(20) NOT NULL,
  message TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_espai_gramps_logs_integracio FOREIGN KEY (integracio_id) REFERENCES espai_integracions_gramps(id) ON DELETE CASCADE,
  CONSTRAINT chk_espai_gramps_logs_status CHECK (status IN ('success','error'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_espai_gramps_logs_integracio ON espai_integracions_gramps_logs(integracio_id);
CREATE INDEX idx_espai_gramps_logs_created ON espai_integracions_gramps_logs(created_at);

CREATE TABLE IF NOT EXISTS espai_privacy_audit (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  owner_user_id INT UNSIGNED NOT NULL,
  arbre_id INT UNSIGNED NOT NULL,
  persona_id INT UNSIGNED NULL,
  action VARCHAR(32) NOT NULL,
  from_visibility VARCHAR(12),
  to_visibility VARCHAR(12),
  ip VARCHAR(45),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_espai_privacy_owner FOREIGN KEY (owner_user_id) REFERENCES usuaris(id) ON DELETE CASCADE,
  CONSTRAINT fk_espai_privacy_arbre FOREIGN KEY (arbre_id) REFERENCES espai_arbres(id) ON DELETE CASCADE,
  CONSTRAINT fk_espai_privacy_persona FOREIGN KEY (persona_id) REFERENCES espai_persones(id) ON DELETE SET NULL,
  CONSTRAINT chk_espai_privacy_action CHECK (action IN ('tree_visibility','person_visibility'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_espai_privacy_owner ON espai_privacy_audit(owner_user_id);
CREATE INDEX idx_espai_privacy_arbre ON espai_privacy_audit(arbre_id);
CREATE INDEX idx_espai_privacy_persona ON espai_privacy_audit(persona_id);
CREATE INDEX idx_espai_privacy_created ON espai_privacy_audit(created_at);

CREATE TABLE IF NOT EXISTS espai_grups (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  owner_user_id INT UNSIGNED NOT NULL,
  nom VARCHAR(255) NOT NULL,
  descripcio TEXT,
  status VARCHAR(20) NOT NULL DEFAULT 'active',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_espai_grups_owner FOREIGN KEY (owner_user_id) REFERENCES usuaris(id) ON DELETE CASCADE,
  CONSTRAINT chk_espai_grups_status CHECK (status IN ('active','archived')),
  UNIQUE KEY idx_espai_grups_owner_name (owner_user_id, nom)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_espai_grups_owner ON espai_grups(owner_user_id);
CREATE INDEX idx_espai_grups_status ON espai_grups(status);
CREATE INDEX idx_espai_grups_updated ON espai_grups(updated_at);

CREATE TABLE IF NOT EXISTS espai_grups_membres (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  grup_id INT UNSIGNED NOT NULL,
  user_id INT UNSIGNED NOT NULL,
  role VARCHAR(20) NOT NULL,
  status VARCHAR(20) NOT NULL DEFAULT 'active',
  joined_at DATETIME NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_espai_grups_membres_grup FOREIGN KEY (grup_id) REFERENCES espai_grups(id) ON DELETE CASCADE,
  CONSTRAINT fk_espai_grups_membres_user FOREIGN KEY (user_id) REFERENCES usuaris(id) ON DELETE CASCADE,
  CONSTRAINT chk_espai_grups_membres_role CHECK (role IN ('owner','admin','member','viewer')),
  CONSTRAINT chk_espai_grups_membres_status CHECK (status IN ('active','invited','removed')),
  UNIQUE KEY idx_espai_grups_membres_unique (grup_id, user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_espai_grups_membres_grup ON espai_grups_membres(grup_id);
CREATE INDEX idx_espai_grups_membres_user ON espai_grups_membres(user_id);
CREATE INDEX idx_espai_grups_membres_status ON espai_grups_membres(status);

CREATE TABLE IF NOT EXISTS espai_grups_arbres (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  grup_id INT UNSIGNED NOT NULL,
  arbre_id INT UNSIGNED NOT NULL,
  status VARCHAR(20) NOT NULL DEFAULT 'active',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_espai_grups_arbres_grup FOREIGN KEY (grup_id) REFERENCES espai_grups(id) ON DELETE CASCADE,
  CONSTRAINT fk_espai_grups_arbres_arbre FOREIGN KEY (arbre_id) REFERENCES espai_arbres(id) ON DELETE CASCADE,
  CONSTRAINT chk_espai_grups_arbres_status CHECK (status IN ('active','removed')),
  UNIQUE KEY idx_espai_grups_arbres_unique (grup_id, arbre_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_espai_grups_arbres_grup ON espai_grups_arbres(grup_id);
CREATE INDEX idx_espai_grups_arbres_arbre ON espai_grups_arbres(arbre_id);
CREATE INDEX idx_espai_grups_arbres_status ON espai_grups_arbres(status);

CREATE TABLE IF NOT EXISTS espai_grups_conflictes (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  grup_id INT UNSIGNED NOT NULL,
  arbre_id INT UNSIGNED NOT NULL,
  conflict_type VARCHAR(20) NOT NULL,
  object_id INT UNSIGNED NULL,
  status VARCHAR(20) NOT NULL DEFAULT 'pending',
  summary TEXT,
  details_json TEXT,
  resolved_at DATETIME NULL,
  resolved_by INT UNSIGNED NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_espai_grups_conflictes_grup FOREIGN KEY (grup_id) REFERENCES espai_grups(id) ON DELETE CASCADE,
  CONSTRAINT fk_espai_grups_conflictes_arbre FOREIGN KEY (arbre_id) REFERENCES espai_arbres(id) ON DELETE CASCADE,
  CONSTRAINT fk_espai_grups_conflictes_resolved_by FOREIGN KEY (resolved_by) REFERENCES usuaris(id) ON DELETE SET NULL,
  CONSTRAINT chk_espai_grups_conflictes_status CHECK (status IN ('pending','resolved')),
  CONSTRAINT chk_espai_grups_conflictes_type CHECK (conflict_type IN ('persona','relacio','event','camp'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_espai_grups_conflictes_grup ON espai_grups_conflictes(grup_id);
CREATE INDEX idx_espai_grups_conflictes_status ON espai_grups_conflictes(status);
CREATE INDEX idx_espai_grups_conflictes_updated ON espai_grups_conflictes(updated_at);
CREATE INDEX idx_espai_grups_conflictes_type ON espai_grups_conflictes(conflict_type);

CREATE TABLE IF NOT EXISTS external_sites (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  slug VARCHAR(120) NOT NULL UNIQUE,
  name VARCHAR(255) NOT NULL,
  domains TEXT NOT NULL,
  icon_path TEXT,
  access_mode VARCHAR(20) NOT NULL,
  is_active TINYINT(1) NOT NULL DEFAULT 1,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS external_links (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  persona_id INT UNSIGNED NOT NULL,
  site_id INT UNSIGNED NULL,
  url TEXT NOT NULL,
  url_norm TEXT NOT NULL,
  title TEXT,
  meta TEXT,
  status VARCHAR(20) NOT NULL,
  created_by_user_id INT UNSIGNED NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY idx_external_links_persona_url (persona_id, url_norm),
  KEY idx_external_links_persona_status (persona_id, status),
  KEY idx_external_links_site (site_id),
  CONSTRAINT fk_external_links_persona FOREIGN KEY (persona_id) REFERENCES persona(id) ON DELETE CASCADE,
  CONSTRAINT fk_external_links_site FOREIGN KEY (site_id) REFERENCES external_sites(id) ON DELETE SET NULL,
  CONSTRAINT fk_external_links_user FOREIGN KEY (created_by_user_id) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS espai_notifications (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  user_id INT UNSIGNED NOT NULL,
  kind VARCHAR(40) NOT NULL,
  title TEXT,
  body TEXT,
  url TEXT,
  status VARCHAR(20) NOT NULL DEFAULT 'unread',
  object_type VARCHAR(40),
  object_id INT UNSIGNED NULL,
  group_id INT UNSIGNED NULL,
  tree_id INT UNSIGNED NULL,
  dedupe_key VARCHAR(191),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  read_at DATETIME NULL,
  CONSTRAINT fk_espai_notifications_user FOREIGN KEY (user_id) REFERENCES usuaris(id) ON DELETE CASCADE,
  CONSTRAINT fk_espai_notifications_group FOREIGN KEY (group_id) REFERENCES espai_grups(id) ON DELETE SET NULL,
  CONSTRAINT fk_espai_notifications_tree FOREIGN KEY (tree_id) REFERENCES espai_arbres(id) ON DELETE SET NULL,
  CONSTRAINT chk_espai_notifications_status CHECK (status IN ('unread','read','archived')),
  UNIQUE KEY uniq_espai_notifications_dedupe (user_id, dedupe_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_espai_notifications_user ON espai_notifications(user_id);
CREATE INDEX idx_espai_notifications_status ON espai_notifications(status);
CREATE INDEX idx_espai_notifications_created ON espai_notifications(created_at);

CREATE TABLE IF NOT EXISTS espai_notification_prefs (
  user_id INT UNSIGNED NOT NULL PRIMARY KEY,
  freq VARCHAR(20) NOT NULL DEFAULT 'instant',
  types_json TEXT,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_espai_notification_prefs_user FOREIGN KEY (user_id) REFERENCES usuaris(id) ON DELETE CASCADE,
  CONSTRAINT chk_espai_notification_prefs_freq CHECK (freq IN ('instant','daily','weekly','off'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS espai_grups_canvis (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  grup_id INT UNSIGNED NOT NULL,
  actor_id INT UNSIGNED NULL,
  action VARCHAR(255) NOT NULL,
  object_type VARCHAR(100),
  object_id INT UNSIGNED NULL,
  payload_json TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_espai_grups_canvis_grup FOREIGN KEY (grup_id) REFERENCES espai_grups(id) ON DELETE CASCADE,
  CONSTRAINT fk_espai_grups_canvis_actor FOREIGN KEY (actor_id) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_espai_grups_canvis_grup ON espai_grups_canvis(grup_id);
CREATE INDEX idx_espai_grups_canvis_created ON espai_grups_canvis(created_at);
CREATE INDEX idx_espai_grups_canvis_actor ON espai_grups_canvis(actor_id);

CREATE INDEX idx_transcripcions_raw_llibre_pagina
  ON transcripcions_raw(llibre_id, pagina_id, posicio_pagina);
CREATE INDEX idx_transcripcions_raw_llibre_tipus_any
  ON transcripcions_raw(llibre_id, tipus_acte, any_doc);
CREATE INDEX idx_transcripcions_raw_status
  ON transcripcions_raw(moderation_status);
CREATE INDEX idx_transcripcions_raw_status_sort
  ON transcripcions_raw(moderation_status, any_doc, pagina_id, posicio_pagina, id);
CREATE INDEX idx_transcripcions_raw_marques_transcripcio
  ON transcripcions_raw_marques(transcripcio_id);
CREATE INDEX idx_transcripcions_raw_marques_user
  ON transcripcions_raw_marques(user_id);
CREATE INDEX idx_transcripcions_raw_canvis_transcripcio
  ON transcripcions_raw_canvis(transcripcio_id);
CREATE INDEX idx_transcripcions_raw_canvis_changed_by
  ON transcripcions_raw_canvis(changed_by);

CREATE INDEX idx_transcripcions_persones_raw_rol
  ON transcripcions_persones_raw(transcripcio_id, rol);
CREATE INDEX idx_transcripcions_persones_raw_nom
  ON transcripcions_persones_raw(cognom1, cognom2, nom);

CREATE INDEX idx_transcripcions_atributs_raw_clau
  ON transcripcions_atributs_raw(clau);
CREATE INDEX idx_transcripcions_atributs_raw_transcripcio
  ON transcripcions_atributs_raw(transcripcio_id, clau);
CREATE INDEX idx_transcripcions_atributs_raw_clau_transcripcio
  ON transcripcions_atributs_raw(clau, transcripcio_id);

-- Cerca per cognoms i nom (per coincidències exactes)
-- CREATE INDEX idx_persona_nom_complet ON persona(nom_complet);
-- Útil per cerca de persones per municipi i quinta (ex: nascuts al mateix lloc i època)
-- CREATE INDEX idx_persona_municipi_quinta ON persona(municipi, quinta);
-- Cercar per ofici o estat civil
-- CREATE INDEX idx_persona_ofici ON persona(ofici);

COMMIT;

SET FOREIGN_KEY_CHECKS = 1; -- Tornem a activar la verificació de claus foranes
