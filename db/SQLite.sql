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
    actiu INTEGER NOT NULL DEFAULT 1 CHECK (actiu IN (0,1)),
    banned INTEGER NOT NULL DEFAULT 0 CHECK (banned IN (0,1)),
    permissions_version INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS user_dashboard_widgets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
    widget_id TEXT NOT NULL,
    position INTEGER NOT NULL DEFAULT 0,
    is_hidden INTEGER NOT NULL DEFAULT 0,
    settings_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (user_id, widget_id)
);
CREATE INDEX IF NOT EXISTS idx_user_dashboard_widgets_user ON user_dashboard_widgets(user_id);
CREATE INDEX IF NOT EXISTS idx_user_dashboard_widgets_order ON user_dashboard_widgets(user_id, position);

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

CREATE TABLE IF NOT EXISTS politica_grants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    politica_id INTEGER NOT NULL REFERENCES politiques(id) ON DELETE CASCADE,
    perm_key TEXT NOT NULL,
    scope_type TEXT NOT NULL,
    scope_id INTEGER,
    include_children INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_politica_grants_politica ON politica_grants(politica_id);
CREATE INDEX IF NOT EXISTS idx_politica_grants_perm ON politica_grants(perm_key);
CREATE INDEX IF NOT EXISTS idx_politica_grants_perm_scope ON politica_grants(perm_key, scope_type, scope_id);

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
    municipi_naixement TEXT,
    municipi_defuncio TEXT,
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

CREATE TABLE IF NOT EXISTS persona_field_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    persona_id INTEGER NOT NULL,
    field_key TEXT NOT NULL,
    registre_id INTEGER NOT NULL,
    created_by INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(persona_id, field_key),
    FOREIGN KEY(persona_id) REFERENCES persona(id) ON DELETE CASCADE,
    FOREIGN KEY(registre_id) REFERENCES transcripcions_raw(id) ON DELETE CASCADE,
    FOREIGN KEY(created_by) REFERENCES usuaris(id) ON DELETE SET NULL
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

-- Anecdotari de persona
CREATE TABLE IF NOT EXISTS persona_anecdotari (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    persona_id INTEGER NOT NULL REFERENCES persona(id) ON DELETE CASCADE,
    user_id INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    tag TEXT,
    status TEXT CHECK(status IN ('pendent','publicat','rebutjat')) DEFAULT 'pendent',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_persona_anecdotari_persona
  ON persona_anecdotari(persona_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_persona_anecdotari_status
  ON persona_anecdotari(status, created_at DESC);

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

-- TAULA CLOSURE TERRITORIAL (PER FILTRAR PER QUALSEVOL NIVELL ADMINISTRATIU)
CREATE TABLE IF NOT EXISTS admin_closure (
    descendant_municipi_id INTEGER NOT NULL REFERENCES municipis(id) ON DELETE CASCADE,
    ancestor_type TEXT NOT NULL CHECK (ancestor_type IN ('pais','nivell','municipi')),
    ancestor_id INTEGER NOT NULL,
    PRIMARY KEY (descendant_municipi_id, ancestor_type, ancestor_id)
);

CREATE INDEX IF NOT EXISTS idx_admin_closure_ancestor
  ON admin_closure(ancestor_type, ancestor_id);

CREATE INDEX IF NOT EXISTS idx_admin_closure_descendant
  ON admin_closure(descendant_municipi_id);

-- Mapes de municipi
CREATE TABLE IF NOT EXISTS municipi_mapes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    municipi_id INTEGER NOT NULL REFERENCES municipis(id) ON DELETE CASCADE,
    group_type TEXT NOT NULL CHECK(group_type IN ('actual','historic','community')),
    title TEXT NOT NULL,
    period_label TEXT,
    period_start INTEGER,
    period_end INTEGER,
    topic TEXT,
    current_version_id INTEGER,
    created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS municipi_mapa_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mapa_id INTEGER NOT NULL REFERENCES municipi_mapes(id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('draft','pendent','publicat','rebutjat')) DEFAULT 'draft',
    data_json TEXT NOT NULL,
    changelog TEXT NOT NULL DEFAULT '',
    lock_version INTEGER NOT NULL DEFAULT 0,
    created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderated_at TIMESTAMP,
    moderation_notes TEXT,
    UNIQUE (mapa_id, version)
);

CREATE INDEX IF NOT EXISTS idx_municipi_mapes_municipi_group ON municipi_mapes(municipi_id, group_type);
CREATE INDEX IF NOT EXISTS idx_municipi_mapes_updated ON municipi_mapes(municipi_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_municipi_mapa_versions_status ON municipi_mapa_versions(status, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_municipi_mapa_versions_mapa_status ON municipi_mapa_versions(mapa_id, status);

-- Historia del municipi
CREATE TABLE IF NOT EXISTS municipi_historia (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    municipi_id INTEGER NOT NULL REFERENCES municipis(id) ON DELETE CASCADE,
    current_general_version_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (municipi_id)
);

CREATE TABLE IF NOT EXISTS municipi_historia_general_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    historia_id INTEGER NOT NULL REFERENCES municipi_historia(id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    titol TEXT,
    resum TEXT,
    cos_text TEXT NOT NULL,
    tags_json TEXT,
    status TEXT NOT NULL CHECK(status IN ('draft','pendent','publicat','rebutjat')) DEFAULT 'draft',
    moderation_notes TEXT,
    lock_version INTEGER NOT NULL DEFAULT 0,
    created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderated_at TIMESTAMP,
    UNIQUE (historia_id, version)
);

CREATE TABLE IF NOT EXISTS municipi_historia_fets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    municipi_id INTEGER NOT NULL REFERENCES municipis(id) ON DELETE CASCADE,
    current_version_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS municipi_historia_fet_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fet_id INTEGER NOT NULL REFERENCES municipi_historia_fets(id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    any_inici INTEGER,
    any_fi INTEGER,
    data_inici TEXT,
    data_fi TEXT,
    data_display TEXT,
    titol TEXT NOT NULL,
    resum TEXT,
    cos_text TEXT NOT NULL,
    tags_json TEXT,
    fonts_json TEXT,
    status TEXT NOT NULL CHECK(status IN ('draft','pendent','publicat','rebutjat')) DEFAULT 'draft',
    moderation_notes TEXT,
    lock_version INTEGER NOT NULL DEFAULT 0,
    created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderated_at TIMESTAMP,
    UNIQUE (fet_id, version)
);

CREATE INDEX IF NOT EXISTS idx_municipi_historia_general_status ON municipi_historia_general_versions(status, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_municipi_historia_general_historia ON municipi_historia_general_versions(historia_id, version);
CREATE INDEX IF NOT EXISTS idx_municipi_historia_fets_municipi ON municipi_historia_fets(municipi_id);
CREATE INDEX IF NOT EXISTS idx_municipi_historia_fet_versions_status ON municipi_historia_fet_versions(status, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_municipi_historia_fet_versions_fet ON municipi_historia_fet_versions(fet_id, version);
CREATE INDEX IF NOT EXISTS idx_municipi_historia_fet_versions_any ON municipi_historia_fet_versions(any_inici, any_fi);

-- Anecdotari del municipi
CREATE TABLE IF NOT EXISTS municipi_anecdotari_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    municipi_id INTEGER NOT NULL REFERENCES municipis(id) ON DELETE CASCADE,
    current_version_id INTEGER,
    created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS municipi_anecdotari_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id INTEGER NOT NULL REFERENCES municipi_anecdotari_items(id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('draft','pendent','publicat','rebutjat')) DEFAULT 'draft',
    titol TEXT NOT NULL,
    tag TEXT NOT NULL,
    data_ref TEXT,
    text TEXT NOT NULL,
    font_url TEXT,
    moderation_notes TEXT,
    lock_version INTEGER NOT NULL DEFAULT 0,
    created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderated_at TIMESTAMP,
    UNIQUE (item_id, version)
);

CREATE TABLE IF NOT EXISTS municipi_anecdotari_comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id INTEGER NOT NULL REFERENCES municipi_anecdotari_items(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
    body TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_municipi_anecdotari_items_municipi ON municipi_anecdotari_items(municipi_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_municipi_anecdotari_versions_status ON municipi_anecdotari_versions(status, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_municipi_anecdotari_versions_item ON municipi_anecdotari_versions(item_id, version);
CREATE INDEX IF NOT EXISTS idx_municipi_anecdotari_comments_item ON municipi_anecdotari_comments(item_id, created_at ASC);

-- Demografia del municipi (rollups)
CREATE TABLE IF NOT EXISTS municipi_demografia_any (
    municipi_id INTEGER NOT NULL REFERENCES municipis(id) ON DELETE CASCADE,
    any INTEGER NOT NULL,
    natalitat INTEGER NOT NULL DEFAULT 0,
    matrimonis INTEGER NOT NULL DEFAULT 0,
    defuncions INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (municipi_id, any)
);

CREATE TABLE IF NOT EXISTS municipi_demografia_meta (
    municipi_id INTEGER NOT NULL PRIMARY KEY REFERENCES municipis(id) ON DELETE CASCADE,
    any_min INTEGER,
    any_max INTEGER,
    total_natalitat INTEGER NOT NULL DEFAULT 0,
    total_matrimonis INTEGER NOT NULL DEFAULT 0,
    total_defuncions INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Demografia per nivell administratiu (rollups)
CREATE TABLE IF NOT EXISTS nivell_demografia_any (
    nivell_id INTEGER NOT NULL REFERENCES nivells_administratius(id) ON DELETE CASCADE,
    any INTEGER NOT NULL,
    natalitat INTEGER NOT NULL DEFAULT 0,
    matrimonis INTEGER NOT NULL DEFAULT 0,
    defuncions INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (nivell_id, any)
);

CREATE TABLE IF NOT EXISTS nivell_demografia_meta (
    nivell_id INTEGER NOT NULL PRIMARY KEY REFERENCES nivells_administratius(id) ON DELETE CASCADE,
    any_min INTEGER,
    any_max INTEGER,
    total_natalitat INTEGER NOT NULL DEFAULT 0,
    total_matrimonis INTEGER NOT NULL DEFAULT 0,
    total_defuncions INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS demografia_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    municipi_id INTEGER NOT NULL REFERENCES municipis(id) ON DELETE CASCADE,
    tipus TEXT NOT NULL CHECK(tipus IN ('natalitat','matrimonis','defuncions')),
    any INTEGER NOT NULL,
    delta INTEGER NOT NULL,
    source TEXT,
    source_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    UNIQUE (source, source_id, delta)
);

CREATE INDEX IF NOT EXISTS idx_municipi_demografia_any_municipi_any ON municipi_demografia_any(municipi_id, any);
CREATE INDEX IF NOT EXISTS idx_nivell_demografia_any_nivell_any ON nivell_demografia_any(nivell_id, any);
CREATE INDEX IF NOT EXISTS idx_demografia_queue_pending ON demografia_queue(processed_at);


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
    arquevisbat_id INTEGER,
    municipi_id INTEGER NOT NULL,
    nom_esglesia TEXT,                     -- ex: "Sant Jaume Apòstol"
    -- Codi identificador únic (de cada sistema)
    codi_digital TEXT,                    -- ex: "0000013893" (Tarragona)
    codi_fisic TEXT,                      -- ex: "UD: 05 / UI: 423" (Urgell)
    -- Metadades del llibre
    titol TEXT,
    tipus_llibre TEXT,
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
    indexacio_completa INTEGER NOT NULL DEFAULT 0,
    created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderation_status TEXT CHECK(moderation_status IN ('pendent','publicat','rebutjat')) DEFAULT 'pendent',
    moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderated_at TIMESTAMP,
    moderation_notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(arquevisbat_id) REFERENCES arquebisbats(id) ON DELETE SET NULL,
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
    what3words TEXT,
    web TEXT,
    acces TEXT,                 -- online, presencial, mixt
    notes TEXT,
    accepta_donacions INTEGER NOT NULL DEFAULT 0,
    donacions_url TEXT,
    created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderation_status TEXT CHECK(moderation_status IN ('pendent','publicat','rebutjat')) DEFAULT 'pendent',
    moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderated_at TIMESTAMP,
    moderation_notes TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS arxius_donacions_clicks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  arxiu_id INTEGER NOT NULL REFERENCES arxius(id) ON DELETE CASCADE,
  user_id INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_arxiu_donacions_clicks_arxiu ON arxius_donacions_clicks(arxiu_id);
CREATE INDEX IF NOT EXISTS idx_arxiu_donacions_clicks_created ON arxius_donacions_clicks(created_at);

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

-- Enllaços alternatius per llibres
CREATE TABLE IF NOT EXISTS llibres_urls (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  llibre_id INTEGER NOT NULL REFERENCES llibres(id) ON DELETE CASCADE,
  arxiu_id INTEGER REFERENCES arxius(id) ON DELETE SET NULL,
  llibre_ref_id INTEGER REFERENCES llibres(id) ON DELETE SET NULL,
  url TEXT NOT NULL,
  tipus TEXT,
  descripcio TEXT,
  created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_llibres_urls_llibre ON llibres_urls(llibre_id);
CREATE INDEX IF NOT EXISTS idx_llibres_urls_arxiu ON llibres_urls(arxiu_id);
CREATE INDEX IF NOT EXISTS idx_llibres_urls_llibre_ref ON llibres_urls(llibre_ref_id);
CREATE INDEX IF NOT EXISTS idx_llibre_pagines_estat  ON llibre_pagines(llibre_id, estat);

-- Media (àlbums + ítems)
CREATE TABLE IF NOT EXISTS media_albums (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  public_id TEXT NOT NULL UNIQUE,
  title TEXT NOT NULL,
  description TEXT,
  album_type TEXT NOT NULL DEFAULT 'other' CHECK (album_type IN ('book','memorial','photo','achievement_icon','other')),
  owner_user_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
  llibre_id INTEGER REFERENCES llibres(id) ON DELETE SET NULL,
  moderation_status TEXT NOT NULL DEFAULT 'pending' CHECK (moderation_status IN ('pending','approved','rejected')),
  visibility TEXT NOT NULL DEFAULT 'private' CHECK (visibility IN ('private','registered','public','restricted_group','admins_only','custom_policy')),
  restricted_group_id INTEGER REFERENCES grups(id) ON DELETE SET NULL,
  access_policy_id INTEGER REFERENCES politiques(id) ON DELETE SET NULL,
  credit_cost INTEGER NOT NULL DEFAULT 0,
  difficulty_score INTEGER NOT NULL DEFAULT 0,
  source_type TEXT DEFAULT 'online' CHECK (source_type IN ('online','offline_archive','family_private','other')),
  moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  moderated_at TIMESTAMP,
  moderation_notes TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS media_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  public_id TEXT NOT NULL UNIQUE,
  album_id INTEGER NOT NULL REFERENCES media_albums(id) ON DELETE CASCADE,
  title TEXT,
  original_filename TEXT,
  mime_type TEXT,
  byte_size INTEGER,
  width INTEGER,
  height INTEGER,
  checksum_sha256 TEXT,
  storage_key_original TEXT NOT NULL,
  thumb_path TEXT,
  derivatives_status TEXT NOT NULL DEFAULT 'pending' CHECK (derivatives_status IN ('pending','ready','failed')),
  moderation_status TEXT NOT NULL DEFAULT 'pending' CHECK (moderation_status IN ('pending','approved','rejected')),
  moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  moderated_at TIMESTAMP,
  moderation_notes TEXT,
  credit_cost INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS media_item_pages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  media_item_id INTEGER NOT NULL REFERENCES media_items(id) ON DELETE CASCADE,
  llibre_id INTEGER REFERENCES llibres(id) ON DELETE SET NULL,
  pagina_id INTEGER REFERENCES llibre_pagines(id) ON DELETE SET NULL,
  page_order INTEGER DEFAULT 0,
  notes TEXT
);

-- Media credits + grants
CREATE TABLE IF NOT EXISTS user_credits_ledger (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
  delta INTEGER NOT NULL,
  reason TEXT NOT NULL,
  ref_type TEXT,
  ref_id INTEGER,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS media_access_grants (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
  media_item_id INTEGER NOT NULL REFERENCES media_items(id) ON DELETE CASCADE,
  grant_token TEXT NOT NULL UNIQUE,
  expires_at TIMESTAMP NOT NULL,
  credits_spent INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS media_access_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
  media_item_id INTEGER NOT NULL REFERENCES media_items(id) ON DELETE CASCADE,
  access_type TEXT NOT NULL,
  credits_spent INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_user_credits_ledger_user ON user_credits_ledger(user_id);
CREATE INDEX IF NOT EXISTS idx_user_credits_ledger_ref ON user_credits_ledger(ref_type, ref_id);
CREATE INDEX IF NOT EXISTS idx_media_access_grants_lookup ON media_access_grants(user_id, media_item_id, expires_at);
CREATE INDEX IF NOT EXISTS idx_media_access_logs_user ON media_access_logs(user_id);
CREATE INDEX IF NOT EXISTS idx_media_access_logs_item ON media_access_logs(media_item_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_media_item_pages_unique ON media_item_pages(media_item_id, pagina_id);
CREATE INDEX IF NOT EXISTS idx_media_item_pages_pagina ON media_item_pages(pagina_id);
CREATE INDEX IF NOT EXISTS idx_media_items_album ON media_items(album_id);
CREATE INDEX IF NOT EXISTS idx_media_items_moderation ON media_items(moderation_status);
CREATE INDEX IF NOT EXISTS idx_media_albums_owner ON media_albums(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_media_albums_moderation ON media_albums(moderation_status);
CREATE INDEX IF NOT EXISTS idx_media_albums_llibre ON media_albums(llibre_id);

-- Transcripcions RAW de registres
CREATE TABLE IF NOT EXISTS transcripcions_raw (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  llibre_id INTEGER NOT NULL REFERENCES llibres(id) ON DELETE CASCADE,
  pagina_id INTEGER REFERENCES llibre_pagines(id) ON DELETE SET NULL,
  num_pagina_text TEXT,
  posicio_pagina INTEGER,
  tipus_acte TEXT,
  any_doc INTEGER,
  data_acte_text TEXT,
  data_acte_iso DATE,
  data_acte_estat TEXT CHECK(data_acte_estat IN ('clar','dubtos','incomplet','illegible','no_consta')) DEFAULT 'clar',
  transcripcio_literal TEXT,
  notes_marginals TEXT,
  observacions_paleografiques TEXT,
  moderation_status TEXT CHECK(moderation_status IN ('pendent','publicat','rebutjat')) DEFAULT 'pendent',
  moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  moderated_at TIMESTAMP,
  moderation_notes TEXT,
  created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- TAULA D'INDEX DE CERCA UNIFICAT
CREATE TABLE IF NOT EXISTS search_docs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  entity_type TEXT NOT NULL CHECK (entity_type IN ('persona','registre_raw')),
  entity_id INTEGER NOT NULL,
  published INTEGER NOT NULL DEFAULT 1 CHECK (published IN (0,1)),
  municipi_id INTEGER NULL REFERENCES municipis(id) ON DELETE SET NULL,
  arxiu_id INTEGER NULL REFERENCES arxius(id) ON DELETE SET NULL,
  llibre_id INTEGER NULL REFERENCES llibres(id) ON DELETE SET NULL,
  entitat_eclesiastica_id INTEGER NULL REFERENCES arquebisbats(id) ON DELETE SET NULL,
  data_acte DATE NULL,
  any_acte INTEGER NULL,
  person_nom_norm TEXT,
  person_cognoms_norm TEXT,
  person_full_norm TEXT,
  person_tokens_norm TEXT,
  cognoms_tokens_norm TEXT,
  person_phonetic TEXT,
  cognoms_phonetic TEXT,
  cognoms_canon TEXT,
  UNIQUE (entity_type, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_search_docs_entity ON search_docs(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_search_docs_any ON search_docs(any_acte);
CREATE INDEX IF NOT EXISTS idx_search_docs_data ON search_docs(data_acte);
CREATE INDEX IF NOT EXISTS idx_search_docs_municipi ON search_docs(municipi_id);
CREATE INDEX IF NOT EXISTS idx_search_docs_arxiu ON search_docs(arxiu_id);
CREATE INDEX IF NOT EXISTS idx_search_docs_llibre ON search_docs(llibre_id);
CREATE INDEX IF NOT EXISTS idx_search_docs_entitat ON search_docs(entitat_eclesiastica_id);
CREATE INDEX IF NOT EXISTS idx_search_docs_full ON search_docs(person_full_norm);
CREATE INDEX IF NOT EXISTS idx_search_docs_cognoms_canon ON search_docs(cognoms_canon);

CREATE TABLE IF NOT EXISTS llibres_indexacio_stats (
  llibre_id INTEGER NOT NULL REFERENCES llibres(id) ON DELETE CASCADE,
  total_registres INTEGER NOT NULL DEFAULT 0,
  total_camps INTEGER NOT NULL DEFAULT 0,
  camps_emplenats INTEGER NOT NULL DEFAULT 0,
  percentatge INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (llibre_id)
);

CREATE TABLE IF NOT EXISTS transcripcions_persones_raw (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  transcripcio_id INTEGER NOT NULL REFERENCES transcripcions_raw(id) ON DELETE CASCADE,
  rol TEXT,
  nom TEXT,
  nom_estat TEXT,
  cognom1 TEXT,
  cognom1_estat TEXT,
  cognom2 TEXT,
  cognom2_estat TEXT,
  cognom_soltera TEXT,
  cognom_soltera_estat TEXT,
  sexe TEXT,
  sexe_estat TEXT,
  edat_text TEXT,
  edat_estat TEXT,
  estat_civil_text TEXT,
  estat_civil_estat TEXT,
  municipi_text TEXT,
  municipi_estat TEXT,
  ofici_text TEXT,
  ofici_estat TEXT,
  casa_nom TEXT,
  casa_estat TEXT,
  persona_id INTEGER REFERENCES persona(id) ON DELETE SET NULL,
  linked_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  linked_at TIMESTAMP,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS transcripcions_atributs_raw (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  transcripcio_id INTEGER NOT NULL REFERENCES transcripcions_raw(id) ON DELETE CASCADE,
  clau TEXT,
  tipus_valor TEXT,
  valor_text TEXT,
  valor_int INTEGER,
  valor_date DATE,
  valor_bool INTEGER,
  estat TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS transcripcions_raw_drafts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  llibre_id INTEGER NOT NULL REFERENCES llibres(id) ON DELETE CASCADE,
  user_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
  payload TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (llibre_id, user_id)
);

CREATE TABLE IF NOT EXISTS transcripcions_raw_page_stats (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  llibre_id INTEGER NOT NULL REFERENCES llibres(id) ON DELETE CASCADE,
  pagina_id INTEGER REFERENCES llibre_pagines(id) ON DELETE SET NULL,
  num_pagina_text TEXT,
  tipus_pagina TEXT NOT NULL DEFAULT 'normal' CHECK (tipus_pagina IN ('normal','portada','index','visita','altres')),
  exclosa INTEGER NOT NULL DEFAULT 0 CHECK (exclosa IN (0,1)),
  indexacio_completa INTEGER NOT NULL DEFAULT 0 CHECK (indexacio_completa IN (0,1)),
  duplicada_de TEXT,
  total_registres INTEGER NOT NULL DEFAULT 0,
  computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (llibre_id, pagina_id, num_pagina_text)
);

CREATE TABLE IF NOT EXISTS transcripcions_raw_marques (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  transcripcio_id INTEGER NOT NULL REFERENCES transcripcions_raw(id) ON DELETE CASCADE,
  user_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
  tipus TEXT CHECK(tipus IN ('consanguini','politic','interes')) NOT NULL,
  is_public INTEGER NOT NULL DEFAULT 1 CHECK (is_public IN (0,1)),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (transcripcio_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_transcripcions_raw_marques_transcripcio
  ON transcripcions_raw_marques(transcripcio_id);
CREATE INDEX IF NOT EXISTS idx_transcripcions_raw_marques_user
  ON transcripcions_raw_marques(user_id);

CREATE TABLE IF NOT EXISTS transcripcions_raw_canvis (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  transcripcio_id INTEGER NOT NULL REFERENCES transcripcions_raw(id) ON DELETE CASCADE,
  change_type TEXT NOT NULL,
  field_key TEXT NOT NULL,
  old_value TEXT,
  new_value TEXT,
  metadata TEXT,
  moderation_status TEXT CHECK(moderation_status IN ('pendent','publicat','rebutjat')) DEFAULT 'pendent',
  moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  moderated_at TIMESTAMP,
  moderation_notes TEXT,
  changed_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_transcripcions_raw_canvis_transcripcio
  ON transcripcions_raw_canvis(transcripcio_id);
CREATE INDEX IF NOT EXISTS idx_transcripcions_raw_canvis_changed_by
  ON transcripcions_raw_canvis(changed_by);

CREATE TABLE IF NOT EXISTS wiki_marques (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  object_type TEXT NOT NULL CHECK(object_type IN ('municipi','arxiu','llibre','persona','cognom')),
  object_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
  tipus TEXT NOT NULL CHECK(tipus IN ('consanguini','politic','interes')),
  is_public INTEGER NOT NULL DEFAULT 1 CHECK (is_public IN (0,1)),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (object_type, object_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_wiki_marques_object
  ON wiki_marques(object_type, object_id);
CREATE INDEX IF NOT EXISTS idx_wiki_marques_user
  ON wiki_marques(user_id);

CREATE TABLE IF NOT EXISTS wiki_marks_stats (
  object_type TEXT NOT NULL CHECK(object_type IN ('municipi','arxiu','llibre','persona','cognom')),
  object_id INTEGER NOT NULL,
  tipus TEXT NOT NULL CHECK(tipus IN ('consanguini','politic','interes')),
  public_count INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (object_type, object_id, tipus)
);
CREATE INDEX IF NOT EXISTS idx_wiki_marks_stats_object
  ON wiki_marks_stats(object_type, object_id);

CREATE TABLE IF NOT EXISTS wiki_canvis (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  object_type TEXT NOT NULL CHECK(object_type IN ('municipi','arxiu','llibre','persona','cognom')),
  object_id INTEGER NOT NULL,
  change_type TEXT NOT NULL,
  field_key TEXT NOT NULL,
  old_value TEXT,
  new_value TEXT,
  metadata TEXT,
  moderation_status TEXT CHECK(moderation_status IN ('pendent','publicat','rebutjat')) DEFAULT 'pendent',
  moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  moderated_at TIMESTAMP,
  moderation_notes TEXT,
  changed_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_wiki_canvis_object
  ON wiki_canvis(object_type, object_id, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_wiki_canvis_status_changed
  ON wiki_canvis(moderation_status, changed_at DESC);

CREATE TABLE IF NOT EXISTS wiki_pending_queue (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  change_id INTEGER NOT NULL UNIQUE REFERENCES wiki_canvis(id) ON DELETE CASCADE,
  object_type TEXT NOT NULL CHECK(object_type IN ('municipi','arxiu','llibre','persona','cognom')),
  object_id INTEGER NOT NULL,
  changed_at TIMESTAMP NOT NULL,
  changed_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_wiki_pending_changed_at
  ON wiki_pending_queue(changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_wiki_pending_object
  ON wiki_pending_queue(object_type, object_id);

CREATE TABLE IF NOT EXISTS csv_import_templates (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  description TEXT,
  owner_user_id INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  visibility TEXT NOT NULL DEFAULT 'private' CHECK(visibility IN ('private','public')),
  default_separator TEXT,
  model_json TEXT NOT NULL,
  signature TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (owner_user_id, name)
);
CREATE INDEX IF NOT EXISTS idx_csv_import_templates_owner_visibility
  ON csv_import_templates(owner_user_id, visibility);
CREATE INDEX IF NOT EXISTS idx_csv_import_templates_visibility_created
  ON csv_import_templates(visibility, created_at);
CREATE INDEX IF NOT EXISTS idx_csv_import_templates_signature
  ON csv_import_templates(signature);

-- Cognoms (forma canònica)
CREATE TABLE IF NOT EXISTS cognoms (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  forma TEXT NOT NULL,
  key TEXT NOT NULL,
  origen TEXT,
  notes TEXT,
  created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (key)
);
CREATE INDEX IF NOT EXISTS idx_cognoms_forma ON cognoms(forma);
CREATE INDEX IF NOT EXISTS idx_cognoms_updated_at ON cognoms(updated_at);

-- Noms (forma canònica)
CREATE TABLE IF NOT EXISTS noms (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  forma TEXT NOT NULL,
  key TEXT NOT NULL,
  notes TEXT,
  created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (key)
);
CREATE INDEX IF NOT EXISTS idx_noms_forma ON noms(forma);
CREATE INDEX IF NOT EXISTS idx_noms_updated_at ON noms(updated_at);

-- Variants de cognom (moderables)
CREATE TABLE IF NOT EXISTS cognom_variants (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  cognom_id INTEGER NOT NULL REFERENCES cognoms(id) ON DELETE CASCADE,
  variant TEXT NOT NULL,
  key TEXT NOT NULL,
  llengua TEXT,
  any_inici INTEGER,
  any_fi INTEGER,
  pais_id INTEGER REFERENCES paisos(id) ON DELETE SET NULL,
  municipi_id INTEGER REFERENCES municipis(id) ON DELETE SET NULL,
  moderation_status TEXT CHECK(moderation_status IN ('pendent','publicat','rebutjat')) DEFAULT 'pendent',
  moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  moderated_at TIMESTAMP,
  moderation_notes TEXT,
  created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (cognom_id, key)
);
CREATE INDEX IF NOT EXISTS idx_cognom_variants_status ON cognom_variants(cognom_id, moderation_status);
CREATE INDEX IF NOT EXISTS idx_cognom_variants_key ON cognom_variants(key);

-- Redirects de cognoms (alias -> canònic)
CREATE TABLE IF NOT EXISTS cognoms_redirects (
  from_cognom_id INTEGER PRIMARY KEY REFERENCES cognoms(id) ON DELETE CASCADE,
  to_cognom_id INTEGER NOT NULL REFERENCES cognoms(id) ON DELETE CASCADE,
  reason TEXT,
  created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_cognoms_redirects_to ON cognoms_redirects(to_cognom_id);

-- Propostes d'unificació de cognoms (moderables)
CREATE TABLE IF NOT EXISTS cognoms_redirects_suggestions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  from_cognom_id INTEGER NOT NULL REFERENCES cognoms(id) ON DELETE CASCADE,
  to_cognom_id INTEGER NOT NULL REFERENCES cognoms(id) ON DELETE CASCADE,
  reason TEXT,
  moderation_status TEXT CHECK(moderation_status IN ('pendent','publicat','rebutjat')) DEFAULT 'pendent',
  moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  moderated_at TIMESTAMP,
  moderation_notes TEXT,
  created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_cognoms_redirects_suggestions_status ON cognoms_redirects_suggestions(moderation_status);
CREATE INDEX IF NOT EXISTS idx_cognoms_redirects_suggestions_from ON cognoms_redirects_suggestions(from_cognom_id);
CREATE INDEX IF NOT EXISTS idx_cognoms_redirects_suggestions_to ON cognoms_redirects_suggestions(to_cognom_id);

-- Referències de cognoms (moderables)
CREATE TABLE IF NOT EXISTS cognoms_referencies (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  cognom_id INTEGER NOT NULL REFERENCES cognoms(id) ON DELETE CASCADE,
  kind TEXT NOT NULL,
  ref_id INTEGER,
  url TEXT,
  titol TEXT,
  descripcio TEXT,
  pagina TEXT,
  moderation_status TEXT DEFAULT 'pendent',
  moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  moderated_at TIMESTAMP,
  moderation_notes TEXT,
  created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_cognoms_ref_cognom_status ON cognoms_referencies(cognom_id, moderation_status);
CREATE INDEX IF NOT EXISTS idx_cognoms_ref_kind ON cognoms_referencies(kind);

-- Estadístiques pre-agregades per cognom/municipi/any
CREATE TABLE IF NOT EXISTS cognoms_freq_municipi_any (
  cognom_id INTEGER NOT NULL REFERENCES cognoms(id) ON DELETE CASCADE,
  municipi_id INTEGER NOT NULL REFERENCES municipis(id) ON DELETE CASCADE,
  any_doc INTEGER NOT NULL,
  freq INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (cognom_id, municipi_id, any_doc)
);
CREATE INDEX IF NOT EXISTS idx_cognoms_freq_cognom_any
  ON cognoms_freq_municipi_any(cognom_id, any_doc);
CREATE INDEX IF NOT EXISTS idx_cognoms_freq_municipi_any
  ON cognoms_freq_municipi_any(municipi_id, any_doc);

-- Totals per cognom/municipi
CREATE TABLE IF NOT EXISTS cognoms_freq_municipi_total (
  cognom_id INTEGER NOT NULL REFERENCES cognoms(id) ON DELETE CASCADE,
  municipi_id INTEGER NOT NULL REFERENCES municipis(id) ON DELETE CASCADE,
  total_freq INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (cognom_id, municipi_id)
);
CREATE INDEX IF NOT EXISTS idx_cognoms_freq_municipi_total_municipi
  ON cognoms_freq_municipi_total(municipi_id, total_freq);

-- Estadístiques pre-agregades per cognom/nivell/any
CREATE TABLE IF NOT EXISTS cognoms_freq_nivell_any (
  cognom_id INTEGER NOT NULL REFERENCES cognoms(id) ON DELETE CASCADE,
  nivell_id INTEGER NOT NULL REFERENCES nivells_administratius(id) ON DELETE CASCADE,
  any_doc INTEGER NOT NULL,
  freq INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (cognom_id, nivell_id, any_doc)
);
CREATE INDEX IF NOT EXISTS idx_cognoms_freq_nivell_any
  ON cognoms_freq_nivell_any(nivell_id, any_doc);
CREATE INDEX IF NOT EXISTS idx_cognoms_freq_nivell_any_cognom
  ON cognoms_freq_nivell_any(cognom_id, any_doc);

-- Totals per cognom/nivell
CREATE TABLE IF NOT EXISTS cognoms_freq_nivell_total (
  cognom_id INTEGER NOT NULL REFERENCES cognoms(id) ON DELETE CASCADE,
  nivell_id INTEGER NOT NULL REFERENCES nivells_administratius(id) ON DELETE CASCADE,
  total_freq INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (cognom_id, nivell_id)
);
CREATE INDEX IF NOT EXISTS idx_cognoms_freq_nivell_total
  ON cognoms_freq_nivell_total(nivell_id, total_freq);

-- Estadístiques globals per cognom
CREATE TABLE IF NOT EXISTS cognoms_stats_total (
  cognom_id INTEGER NOT NULL REFERENCES cognoms(id) ON DELETE CASCADE,
  total_persones INTEGER NOT NULL DEFAULT 0,
  total_aparicions INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (cognom_id)
);
CREATE INDEX IF NOT EXISTS idx_cognoms_stats_total_aparicions
  ON cognoms_stats_total(total_aparicions);

-- Estadístiques per any per cognom
CREATE TABLE IF NOT EXISTS cognoms_stats_any (
  cognom_id INTEGER NOT NULL REFERENCES cognoms(id) ON DELETE CASCADE,
  any INTEGER NOT NULL,
  total INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (cognom_id, any)
);
CREATE INDEX IF NOT EXISTS idx_cognoms_stats_any_any
  ON cognoms_stats_any(any);

-- Estadístiques per ancestre (municipi/nivell) i any
CREATE TABLE IF NOT EXISTS cognoms_stats_ancestor_any (
  cognom_id INTEGER NOT NULL REFERENCES cognoms(id) ON DELETE CASCADE,
  ancestor_type TEXT NOT NULL,
  ancestor_id INTEGER NOT NULL,
  any INTEGER NOT NULL,
  total INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (cognom_id, ancestor_type, ancestor_id, any)
);
CREATE INDEX IF NOT EXISTS idx_cognoms_stats_ancestor_cognom_any
  ON cognoms_stats_ancestor_any(cognom_id, ancestor_type, any);
CREATE INDEX IF NOT EXISTS idx_cognoms_stats_ancestor_id
  ON cognoms_stats_ancestor_any(ancestor_type, ancestor_id);

-- Estadístiques pre-agregades per nom/municipi/any
CREATE TABLE IF NOT EXISTS noms_freq_municipi_any (
  nom_id INTEGER NOT NULL REFERENCES noms(id) ON DELETE CASCADE,
  municipi_id INTEGER NOT NULL REFERENCES municipis(id) ON DELETE CASCADE,
  any_doc INTEGER NOT NULL,
  freq INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (nom_id, municipi_id, any_doc)
);
CREATE INDEX IF NOT EXISTS idx_noms_freq_municipi_any_municipi_any
  ON noms_freq_municipi_any(municipi_id, any_doc);
CREATE INDEX IF NOT EXISTS idx_noms_freq_municipi_any_nom_any
  ON noms_freq_municipi_any(nom_id, any_doc);

-- Totals per nom/municipi
CREATE TABLE IF NOT EXISTS noms_freq_municipi_total (
  nom_id INTEGER NOT NULL REFERENCES noms(id) ON DELETE CASCADE,
  municipi_id INTEGER NOT NULL REFERENCES municipis(id) ON DELETE CASCADE,
  total_freq INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (nom_id, municipi_id)
);
CREATE INDEX IF NOT EXISTS idx_noms_freq_municipi_total_municipi
  ON noms_freq_municipi_total(municipi_id, total_freq);

-- Estadístiques pre-agregades per nom/nivell/any
CREATE TABLE IF NOT EXISTS noms_freq_nivell_any (
  nom_id INTEGER NOT NULL REFERENCES noms(id) ON DELETE CASCADE,
  nivell_id INTEGER NOT NULL REFERENCES nivells_administratius(id) ON DELETE CASCADE,
  any_doc INTEGER NOT NULL,
  freq INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (nom_id, nivell_id, any_doc)
);
CREATE INDEX IF NOT EXISTS idx_noms_freq_nivell_any
  ON noms_freq_nivell_any(nivell_id, any_doc);
CREATE INDEX IF NOT EXISTS idx_noms_freq_nivell_any_nom
  ON noms_freq_nivell_any(nom_id, any_doc);

-- Totals per nom/nivell
CREATE TABLE IF NOT EXISTS noms_freq_nivell_total (
  nom_id INTEGER NOT NULL REFERENCES noms(id) ON DELETE CASCADE,
  nivell_id INTEGER NOT NULL REFERENCES nivells_administratius(id) ON DELETE CASCADE,
  total_freq INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (nom_id, nivell_id)
);
CREATE INDEX IF NOT EXISTS idx_noms_freq_nivell_total
  ON noms_freq_nivell_total(nivell_id, total_freq);

CREATE INDEX IF NOT EXISTS idx_transcripcions_raw_llibre_pagina
  ON transcripcions_raw(llibre_id, pagina_id, posicio_pagina);
CREATE INDEX IF NOT EXISTS idx_transcripcions_raw_llibre_tipus_any
  ON transcripcions_raw(llibre_id, tipus_acte, any_doc);
CREATE INDEX IF NOT EXISTS idx_transcripcions_raw_status
  ON transcripcions_raw(moderation_status);
CREATE INDEX IF NOT EXISTS idx_transcripcions_raw_status_sort
  ON transcripcions_raw(moderation_status, any_doc, pagina_id, posicio_pagina, id);

CREATE INDEX IF NOT EXISTS idx_transcripcions_persones_raw_rol
  ON transcripcions_persones_raw(transcripcio_id, rol);
CREATE INDEX IF NOT EXISTS idx_transcripcions_persones_raw_nom
  ON transcripcions_persones_raw(cognom1, cognom2, nom);

CREATE INDEX IF NOT EXISTS idx_transcripcions_atributs_raw_clau
  ON transcripcions_atributs_raw(clau);
CREATE INDEX IF NOT EXISTS idx_transcripcions_atributs_raw_transcripcio
  ON transcripcions_atributs_raw(transcripcio_id, clau);
CREATE INDEX IF NOT EXISTS idx_transcripcions_atributs_raw_clau_transcripcio
  ON transcripcions_atributs_raw(clau, transcripcio_id);

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

-- Achievements
CREATE TABLE IF NOT EXISTS achievements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    rarity TEXT NOT NULL CHECK(rarity IN ('common','rare','epic','legendary')),
    visibility TEXT NOT NULL CHECK(visibility IN ('visible','hidden','seasonal')),
    domain TEXT NOT NULL,
    is_enabled INTEGER NOT NULL DEFAULT 1,
    is_repeatable INTEGER NOT NULL DEFAULT 0,
    icon_media_item_id INTEGER REFERENCES media_items(id) ON DELETE SET NULL,
    rule_json TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS achievement_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    start_at TIMESTAMP NOT NULL,
    end_at TIMESTAMP NOT NULL,
    scope TEXT NOT NULL DEFAULT 'global',
    scope_id INTEGER,
    is_enabled INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS achievements_user (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
    achievement_id INTEGER NOT NULL REFERENCES achievements(id) ON DELETE CASCADE,
    awarded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT NOT NULL CHECK(status IN ('active','revoked','hidden_by_user')) DEFAULT 'active',
    meta_json TEXT,
    UNIQUE (user_id, achievement_id)
);

CREATE TABLE IF NOT EXISTS achievements_showcase (
    user_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
    achievement_id INTEGER NOT NULL REFERENCES achievements(id) ON DELETE CASCADE,
    slot INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, slot),
    UNIQUE (user_id, achievement_id)
);

CREATE INDEX IF NOT EXISTS idx_achievements_domain_enabled ON achievements(domain, is_enabled);
CREATE INDEX IF NOT EXISTS idx_achievements_icon ON achievements(icon_media_item_id);
CREATE INDEX IF NOT EXISTS idx_achievements_user_user ON achievements_user(user_id, awarded_at DESC);
CREATE INDEX IF NOT EXISTS idx_achievements_user_achievement ON achievements_user(achievement_id);
CREATE INDEX IF NOT EXISTS idx_achievement_events_code_window ON achievement_events(code, is_enabled, start_at, end_at);
CREATE INDEX IF NOT EXISTS idx_usuaris_activitat_user_created ON usuaris_activitat(usuari_id, data_creacio);
CREATE INDEX IF NOT EXISTS idx_usuaris_activitat_user_status_created ON usuaris_activitat(usuari_id, estat, data_creacio);
CREATE INDEX IF NOT EXISTS idx_usuaris_activitat_user_regla_status_created ON usuaris_activitat(usuari_id, regla_id, estat, data_creacio);

-- =====================================================================
-- Esdeveniments historics
-- =====================================================================

CREATE TABLE IF NOT EXISTS events_historics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  titol TEXT NOT NULL,
  slug TEXT NOT NULL UNIQUE,
  tipus TEXT NOT NULL,
  resum TEXT,
  descripcio TEXT,
  data_inici DATE,
  data_fi DATE,
  data_inici_aprox INTEGER NOT NULL DEFAULT 0 CHECK (data_inici_aprox IN (0,1)),
  data_fi_aprox INTEGER NOT NULL DEFAULT 0 CHECK (data_fi_aprox IN (0,1)),
  precisio TEXT CHECK (precisio IN ('dia','mes','any','decada')),
  fonts TEXT,
  created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  moderation_status TEXT NOT NULL DEFAULT 'pendent' CHECK (moderation_status IN ('pendent','publicat','rebutjat')),
  moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  moderated_at TIMESTAMP,
  moderation_notes TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS events_historics_impactes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id INTEGER NOT NULL REFERENCES events_historics(id) ON DELETE CASCADE,
  scope_type TEXT NOT NULL CHECK (scope_type IN ('pais','nivell_admin','municipi','entitat_eclesiastica')),
  scope_id INTEGER NOT NULL,
  impacte_tipus TEXT NOT NULL CHECK (impacte_tipus IN ('directe','indirecte','transit','rumor')),
  intensitat INTEGER NOT NULL CHECK (intensitat BETWEEN 1 AND 5),
  notes TEXT,
  created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_events_historics_tipus_data_inici
  ON events_historics(tipus, data_inici);
CREATE INDEX IF NOT EXISTS idx_events_historics_moderation
  ON events_historics(moderation_status, created_at);
CREATE INDEX IF NOT EXISTS idx_events_historics_impactes_event
  ON events_historics_impactes(event_id);
CREATE INDEX IF NOT EXISTS idx_events_historics_impactes_scope
  ON events_historics_impactes(scope_type, scope_id);
CREATE INDEX IF NOT EXISTS idx_events_historics_impactes_scope_intensitat
  ON events_historics_impactes(scope_type, scope_id, intensitat);

CREATE TABLE IF NOT EXISTS dm_threads (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_low_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
  user_high_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  last_message_at TIMESTAMP,
  last_message_id INTEGER,
  CHECK (user_low_id < user_high_id),
  UNIQUE (user_low_id, user_high_id)
);
CREATE INDEX IF NOT EXISTS idx_dm_threads_user_low
  ON dm_threads(user_low_id);
CREATE INDEX IF NOT EXISTS idx_dm_threads_user_high
  ON dm_threads(user_high_id);
CREATE INDEX IF NOT EXISTS idx_dm_threads_last_message_at
  ON dm_threads(last_message_at DESC);

CREATE TABLE IF NOT EXISTS dm_thread_state (
  thread_id INTEGER NOT NULL REFERENCES dm_threads(id) ON DELETE CASCADE,
  user_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
  last_read_message_id INTEGER,
  archived INTEGER NOT NULL DEFAULT 0 CHECK (archived IN (0,1)),
  muted INTEGER NOT NULL DEFAULT 0 CHECK (muted IN (0,1)),
  deleted INTEGER NOT NULL DEFAULT 0 CHECK (deleted IN (0,1)),
  folder TEXT,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (thread_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_dm_thread_state_user_archived
  ON dm_thread_state(user_id, archived, updated_at);
CREATE INDEX IF NOT EXISTS idx_dm_thread_state_user_deleted
  ON dm_thread_state(user_id, deleted, updated_at);
CREATE INDEX IF NOT EXISTS idx_dm_thread_state_user_folder
  ON dm_thread_state(user_id, folder);

CREATE TABLE IF NOT EXISTS dm_messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  thread_id INTEGER NOT NULL REFERENCES dm_threads(id) ON DELETE CASCADE,
  sender_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
  body TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_dm_messages_thread_created
  ON dm_messages(thread_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dm_messages_sender_created
  ON dm_messages(sender_id, created_at DESC);

CREATE TABLE IF NOT EXISTS user_blocks (
  blocker_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
  blocked_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (blocker_id, blocked_id)
);
CREATE INDEX IF NOT EXISTS idx_user_blocks_blocker
  ON user_blocks(blocker_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_user_blocks_blocked
  ON user_blocks(blocked_id, created_at DESC);

-- Index per accelerar busquedes

-- Per buscar ràpidament pel codi postal
CREATE INDEX IF NOT EXISTS idx_codi_postal ON municipis(codi_postal);
-- Per cercar pel nom
CREATE INDEX IF NOT EXISTS idx_nom_municipi ON municipis(nom);
-- Per buscar nivells pel seu tipus
CREATE INDEX IF NOT EXISTS idx_tipus_nivell ON nivells_administratius(tipus_nivell);

--CREATE INDEX IF NOT EXISTS idx_llibres_arquevisbat ON llibres(arquevisbat_id);
CREATE INDEX IF NOT EXISTS idx_llibres_municipi ON llibres(municipi_id);

-- Índex compost per millorar la cerca de duplicats i cerques combinades
-- CREATE INDEX idx_persona_cognoms_any_llibre_pagina ON persona(cognom1, cognom2, quinta, llibre, pagina); -- error executant SQLite.sql: index idx_persona_cognoms_any_llibre_pagina already exists

-- Cerca per cognoms i nom (per coincidències exactes)
CREATE INDEX IF NOT EXISTS idx_persona_nom_complet ON persona(nom_complet);

-- Útil per cerca de persones per municipi i any (ex: nascuts al mateix lloc i època)
CREATE INDEX IF NOT EXISTS idx_persona_municipi_quinta ON persona(municipi, quinta);

-- Cercar per ofici o estat civil
CREATE INDEX IF NOT EXISTS idx_persona_ofici ON persona(ofici);
-- CREATE INDEX IF NOT EXISTS idx_persona_estat_civil ON persona(estat_civil);

CREATE INDEX IF NOT EXISTS idx_persona_field_links_persona ON persona_field_links(persona_id);
CREATE INDEX IF NOT EXISTS idx_persona_field_links_registre ON persona_field_links(registre_id);

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

CREATE UNIQUE INDEX IF NOT EXISTS idx_nivell_scope_nom
ON nivells_administratius(nivel, COALESCE(parent_id, -pais_id), nom_nivell);


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
