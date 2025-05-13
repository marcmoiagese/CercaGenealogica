-- Taula d'Usuaris
CREATE TABLE Usuari (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT UNIQUE NOT NULL,
    contrasenya_hash TEXT NOT NULL,
    nom TEXT,
    data_registre TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nivell_acces TEXT CHECK (nivell_acces IN ('VISITANT', 'COLABORADOR', 'MODERADOR', 'ADMIN')) DEFAULT 'VISITANT',
    estat TEXT CHECK (estat IN ('ACTIU', 'SUSPES', 'INACTIU')) DEFAULT 'ACTIU'
);

CREATE INDEX idx_usuari_email ON Usuari(email);

-- Taula de Llocs
CREATE TABLE Lloc (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom TEXT NOT NULL,
    tipus TEXT CHECK (tipus IN ('POBLE', 'CIUTAT', 'REGIO', 'PAIS', 'ALTRES')) DEFAULT 'POBLE',
    coordenades TEXT,
    codi_postal TEXT,
    pais TEXT,
    descripcio TEXT
);

CREATE INDEX idx_lloc_nom ON Lloc(nom);

-- Taula de Llibres
CREATE TABLE Llibre (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    titol TEXT NOT NULL,
    tipus TEXT CHECK (tipus IN ('BATEIG', 'MATRIMONI', 'OBIT', 'CENS', 'NOTARIAL', 'ALTRES')) DEFAULT 'ALTRES',
    any_inici INTEGER,
    any_fi INTEGER,
    ubicacio_fisica TEXT,
    referencia_arxiu TEXT,
    descripcio TEXT,
    estat_validacio TEXT CHECK (estat_validacio IN ('PENDENT', 'VALIDAT', 'REBUTJAT')) DEFAULT 'VALIDAT'
);

CREATE INDEX idx_llibre_titol ON Llibre(titol);
CREATE INDEX idx_llibre_any_inici ON Llibre(any_inici);
CREATE INDEX idx_llibre_any_fi ON Llibre(any_fi);

-- Taula de Persona
CREATE TABLE Persona (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nom TEXT,
    cognoms TEXT NOT NULL,
    sexe TEXT CHECK (sexe IN ('M', 'F', 'D', 'DESCONEGUT')) DEFAULT 'DESCONEGUT',
    data_naixement_aproximada TEXT,
    data_naixement DATE,
    data_defuncio DATE,
    notes_biografiques TEXT,
    data_creacio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_actualitzacio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    usuari_creador INTEGER REFERENCES Usuari(id) ON DELETE SET NULL,
    estat_validacio TEXT CHECK (estat_validacio IN ('PENDENT', 'VALIDAT', 'REBUTJAT', 'REVISIO')) DEFAULT 'PENDENT'
);

CREATE INDEX idx_persona_cognoms ON Persona(cognoms);
CREATE INDEX idx_persona_nom ON Persona(nom);
CREATE INDEX idx_persona_data_naixement ON Persona(data_naixement);
CREATE INDEX idx_persona_data_defuncio ON Persona(data_defuncio);

-- Taula d'Esdeveniment
CREATE TABLE Esdeveniment (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tipus TEXT CHECK (tipus IN ('BATEIG', 'MATRIMONI', 'OBIT', 'NAIXEMENT', 'CENS', 'ALTRES')) NOT NULL,
    data_exacta DATE,
    data_aproximada TEXT,
    any INTEGER,
    llibre_id INTEGER REFERENCES Llibre(id) ON DELETE SET NULL,
    pagina TEXT,
    pagina_real TEXT,
    lloc_id INTEGER REFERENCES Lloc(id) ON DELETE SET NULL,
    notes TEXT,
    url_document TEXT,
    estat_validacio TEXT CHECK (estat_validacio IN ('PENDENT', 'VALIDAT', 'REBUTJAT')) DEFAULT 'PENDENT',
    usuari_creador INTEGER REFERENCES Usuari(id) ON DELETE SET NULL,
    data_creacio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_moderacio TIMESTAMP
);

CREATE INDEX idx_esdeveniment_tipus ON Esdeveniment(tipus);
CREATE INDEX idx_esdeveniment_any ON Esdeveniment(any);
CREATE INDEX idx_esdeveniment_data_exacta ON Esdeveniment(data_exacta);

-- Taula de Bateig
CREATE TABLE Bateig (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    esdeveniment_id INTEGER NOT NULL REFERENCES Esdeveniment(id) ON DELETE CASCADE,
    padri_id INTEGER REFERENCES Persona(id) ON DELETE SET NULL,
    padrina_id INTEGER REFERENCES Persona(id) ON DELETE SET NULL,
    info_padri TEXT,
    info_padrina TEXT,
    ofici_pare TEXT,
    data_matrimoni_pares DATE
);

-- Taula de Matrimoni
CREATE TABLE Matrimoni (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    esdeveniment_id INTEGER NOT NULL REFERENCES Esdeveniment(id) ON DELETE CASCADE,
    nom_casa TEXT
);

-- Taula d'Òbit
CREATE TABLE Obit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    esdeveniment_id INTEGER NOT NULL REFERENCES Esdeveniment(id) ON DELETE CASCADE,
    causa_mort TEXT,
    classe_enterro TEXT,
    edat INTEGER
);

-- Taula de PersonaEsdeveniment
CREATE TABLE PersonaEsdeveniment (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    persona_id INTEGER NOT NULL REFERENCES Persona(id) ON DELETE CASCADE,
    esdeveniment_id INTEGER NOT NULL REFERENCES Esdeveniment(id) ON DELETE CASCADE,
    rol TEXT CHECK (rol IN ('BATEJAT', 'PARE', 'MARE', 'AVI', 'ÀVIA', 'PADRI', 'PADRINA', 'DIFUNT', 'MARIT', 'ESPOSA', 'TESTIMONI', 'ALTRES')),
    notes TEXT
);

CREATE INDEX idx_persona_esdeveniment_persona ON PersonaEsdeveniment(persona_id);
CREATE INDEX idx_persona_esdeveniment_esdeveniment ON PersonaEsdeveniment(esdeveniment_id);

-- Taula de RelacioFamiliar
CREATE TABLE RelacioFamiliar (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    persona1_id INTEGER NOT NULL REFERENCES Persona(id) ON DELETE CASCADE,
    persona2_id INTEGER NOT NULL REFERENCES Persona(id) ON DELETE CASCADE,
    tipus_relacio TEXT CHECK (tipus_relacio IN ('PARE', 'MARE', 'FILL', 'FILLA', 'AVI', 'ÀVIA', 'NÉT', 'NÉTA', 'CÒNJUGE', 'GERMÀ', 'GERMANA', 'ONCLE', 'TIA', 'NEBOT', 'NEBODA', 'COSÍ', 'COSINA', 'ALTRE')),
    certesa TEXT CHECK (certesa IN ('CONFIRMADA', 'SUPOSADA', 'INCERTA')) DEFAULT 'SUPOSADA',
    notes TEXT,
    estat_validacio TEXT CHECK (estat_validacio IN ('PENDENT', 'VALIDAT', 'REBUTJAT')) DEFAULT 'PENDENT',
    usuari_creador INTEGER REFERENCES Usuari(id) ON DELETE SET NULL,
    data_creacio TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_relacio_persona1 ON RelacioFamiliar(persona1_id);
CREATE INDEX idx_relacio_persona2 ON RelacioFamiliar(persona2_id);

-- Taula de Document
CREATE TABLE Document (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    persona_id INTEGER REFERENCES Persona(id) ON DELETE SET NULL,
    esdeveniment_id INTEGER REFERENCES Esdeveniment(id) ON DELETE SET NULL,
    tipus TEXT CHECK (tipus IN ('ARXIU', 'EXTERN', 'FOTO', 'REGISTRE')),
    titol TEXT NOT NULL,
    descripcio TEXT,
    url TEXT,
    data_creacio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    usuari_creador INTEGER REFERENCES Usuari(id) ON DELETE SET NULL,
    estat_validacio TEXT CHECK (estat_validacio IN ('PENDENT', 'VALIDAT', 'REBUTJAT')) DEFAULT 'PENDENT'
);

CREATE INDEX idx_document_persona ON Document(persona_id);
CREATE INDEX idx_document_esdeveniment ON Document(esdeveniment_id);

-- Taula de DocumentExtern
CREATE TABLE DocumentExtern (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id INTEGER NOT NULL REFERENCES Document(id) ON DELETE CASCADE,
    tipus TEXT CHECK (tipus IN ('PARES', 'MEMORIAL', 'MEMORIA_DEMOCRATICA', 'CENS', 'ALTRES')),
    referencia TEXT,
    data_consulta DATE
);

-- Taula de Contribucio
CREATE TABLE Contribucio (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    usuari_id INTEGER NOT NULL REFERENCES Usuari(id) ON DELETE CASCADE,
    tipus_entitat TEXT CHECK (tipus_entitat IN ('PERSONA', 'ESDEVENIMENT', 'RELACIO', 'DOCUMENT', 'LLIBRE', 'LLOC')),
    entitat_id INTEGER NOT NULL,
    accio TEXT CHECK (accio IN ('CREACIO', 'MODIFICACIO', 'ELIMINACIO')),
    dades_anteriors TEXT, -- SQLite no té tipus JSON natiu, s'emmagatzema com TEXT
    dades_noves TEXT,
    data_accio TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_contribucio_usuari ON Contribucio(usuari_id);
CREATE INDEX idx_contribucio_tipus_entitat ON Contribucio(tipus_entitat, entitat_id);

-- Taula de Moderacio
CREATE TABLE Moderacio (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contribucio_id INTEGER NOT NULL REFERENCES Contribucio(id) ON DELETE CASCADE,
    moderador_id INTEGER NOT NULL REFERENCES Usuari(id) ON DELETE CASCADE,
    estat TEXT CHECK (estat IN ('PENDENT', 'APROVAT', 'REBUTJAT')),
    comentaris TEXT,
    data_moderacio TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_moderacio_contribucio ON Moderacio(contribucio_id);
CREATE INDEX idx_moderacio_estat ON Moderacio(estat);

-- Disparador per actualitzar data_actualitzacio automàticament
CREATE TRIGGER persona_update_timestamp
AFTER UPDATE ON Persona
BEGIN
    UPDATE Persona SET data_actualitzacio = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Disparador per actualitzar data_actualitzacio en Esdeveniment
CREATE TRIGGER esdeveniment_update_timestamp
AFTER UPDATE ON Esdeveniment
BEGIN
    UPDATE Esdeveniment SET data_actualitzacio = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;