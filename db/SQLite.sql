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