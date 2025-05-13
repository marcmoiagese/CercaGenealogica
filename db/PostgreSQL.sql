-- Creació d'extensions necessàries
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Taula d'Usuaris
CREATE TABLE Usuari (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    contrasenya_hash VARCHAR(255) NOT NULL,
    nom VARCHAR(100),
    data_registre TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    nivell_acces VARCHAR(20) CHECK (nivell_acces IN ('VISITANT', 'COLABORADOR', 'MODERADOR', 'ADMIN')) DEFAULT 'VISITANT',
    estat VARCHAR(10) CHECK (estat IN ('ACTIU', 'SUSPES', 'INACTIU')) DEFAULT 'ACTIU'
);

CREATE INDEX idx_usuari_email ON Usuari(email);

-- Taula de Llocs
CREATE TABLE Lloc (
    id SERIAL PRIMARY KEY,
    nom VARCHAR(255) NOT NULL,
    tipus VARCHAR(10) CHECK (tipus IN ('POBLE', 'CIUTAT', 'REGIO', 'PAIS', 'ALTRES')) DEFAULT 'POBLE',
    coordenades VARCHAR(100),
    codi_postal VARCHAR(20),
    pais VARCHAR(100),
    descripcio TEXT
);

CREATE INDEX idx_lloc_nom ON Lloc(nom);

-- Taula de Llibres
CREATE TABLE Llibre (
    id SERIAL PRIMARY KEY,
    titol VARCHAR(255) NOT NULL,
    tipus VARCHAR(20) CHECK (tipus IN ('BATEIG', 'MATRIMONI', 'OBIT', 'CENS', 'NOTARIAL', 'ALTRES')) DEFAULT 'ALTRES',
    any_inici INTEGER,
    any_fi INTEGER,
    ubicacio_fisica VARCHAR(255),
    referencia_arxiu VARCHAR(255),
    descripcio TEXT,
    estat_validacio VARCHAR(10) CHECK (estat_validacio IN ('PENDENT', 'VALIDAT', 'REBUTJAT')) DEFAULT 'VALIDAT'
);

CREATE INDEX idx_llibre_titol ON Llibre(titol);
CREATE INDEX idx_llibre_any_inici ON Llibre(any_inici);
CREATE INDEX idx_llibre_any_fi ON Llibre(any_fi);

-- Taula de Persona
CREATE TABLE Persona (
    id SERIAL PRIMARY KEY,
    nom VARCHAR(100),
    cognoms VARCHAR(255) NOT NULL,
    sexe VARCHAR(12) CHECK (sexe IN ('M', 'F', 'D', 'DESCONEGUT')) DEFAULT 'DESCONEGUT',
    data_naixement_aproximada VARCHAR(100),
    data_naixement DATE,
    data_defuncio DATE,
    notes_biografiques TEXT,
    data_creacio TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data_actualitzacio TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    usuari_creador INTEGER REFERENCES Usuari(id) ON DELETE SET NULL,
    estat_validacio VARCHAR(10) CHECK (estat_validacio IN ('PENDENT', 'VALIDAT', 'REBUTJAT', 'REVISIO')) DEFAULT 'PENDENT'
);

CREATE INDEX idx_persona_cognoms ON Persona(cognoms);
CREATE INDEX idx_persona_nom ON Persona(nom);
CREATE INDEX idx_persona_data_naixement ON Persona(data_naixement);
CREATE INDEX idx_persona_data_defuncio ON Persona(data_defuncio);

-- Taula d'Esdeveniment
CREATE TABLE Esdeveniment (
    id SERIAL PRIMARY KEY,
    tipus VARCHAR(20) CHECK (tipus IN ('BATEIG', 'MATRIMONI', 'OBIT', 'NAIXEMENT', 'CENS', 'ALTRES')) NOT NULL,
    data_exacta DATE,
    data_aproximada VARCHAR(100),
    any INTEGER,
    llibre_id INTEGER REFERENCES Llibre(id) ON DELETE SET NULL,
    pagina VARCHAR(50),
    pagina_real VARCHAR(50),
    lloc_id INTEGER REFERENCES Lloc(id) ON DELETE SET NULL,
    notes TEXT,
    url_document VARCHAR(255),
    estat_validacio VARCHAR(10) CHECK (estat_validacio IN ('PENDENT', 'VALIDAT', 'REBUTJAT')) DEFAULT 'PENDENT',
    usuari_creador INTEGER REFERENCES Usuari(id) ON DELETE SET NULL,
    data_creacio TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data_moderacio TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_esdeveniment_tipus ON Esdeveniment(tipus);
CREATE INDEX idx_esdeveniment_any ON Esdeveniment(any);
CREATE INDEX idx_esdeveniment_data_exacta ON Esdeveniment(data_exacta);

-- Taula de Bateig
CREATE TABLE Bateig (
    id SERIAL PRIMARY KEY,
    esdeveniment_id INTEGER NOT NULL REFERENCES Esdeveniment(id) ON DELETE CASCADE,
    padri_id INTEGER REFERENCES Persona(id) ON DELETE SET NULL,
    padrina_id INTEGER REFERENCES Persona(id) ON DELETE SET NULL,
    info_padri TEXT,
    info_padrina TEXT,
    ofici_pare VARCHAR(100),
    data_matrimoni_pares DATE
);

-- Taula de Matrimoni
CREATE TABLE Matrimoni (
    id SERIAL PRIMARY KEY,
    esdeveniment_id INTEGER NOT NULL REFERENCES Esdeveniment(id) ON DELETE CASCADE,
    nom_casa VARCHAR(100)
);

-- Taula d'Òbit
CREATE TABLE Obit (
    id SERIAL PRIMARY KEY,
    esdeveniment_id INTEGER NOT NULL REFERENCES Esdeveniment(id) ON DELETE CASCADE,
    causa_mort VARCHAR(255),
    classe_enterro VARCHAR(100),
    edat INTEGER
);

-- Taula de PersonaEsdeveniment
CREATE TABLE PersonaEsdeveniment (
    id SERIAL PRIMARY KEY,
    persona_id INTEGER NOT NULL REFERENCES Persona(id) ON DELETE CASCADE,
    esdeveniment_id INTEGER NOT NULL REFERENCES Esdeveniment(id) ON DELETE CASCADE,
    rol VARCHAR(20) CHECK (rol IN ('BATEJAT', 'PARE', 'MARE', 'AVI', 'ÀVIA', 'PADRI', 'PADRINA', 'DIFUNT', 'MARIT', 'ESPOSA', 'TESTIMONI', 'ALTRES')),
    notes TEXT
);

CREATE INDEX idx_persona_esdeveniment_persona ON PersonaEsdeveniment(persona_id);
CREATE INDEX idx_persona_esdeveniment_esdeveniment ON PersonaEsdeveniment(esdeveniment_id);

-- Taula de RelacioFamiliar
CREATE TABLE RelacioFamiliar (
    id SERIAL PRIMARY KEY,
    persona1_id INTEGER NOT NULL REFERENCES Persona(id) ON DELETE CASCADE,
    persona2_id INTEGER NOT NULL REFERENCES Persona(id) ON DELETE CASCADE,
    tipus_relacio VARCHAR(20) CHECK (tipus_relacio IN ('PARE', 'MARE', 'FILL', 'FILLA', 'AVI', 'ÀVIA', 'NÉT', 'NÉTA', 'CÒNJUGE', 'GERMÀ', 'GERMANA', 'ONCLE', 'TIA', 'NEBOT', 'NEBODA', 'COSÍ', 'COSINA', 'ALTRE')),
    certesa VARCHAR(10) CHECK (certesa IN ('CONFIRMADA', 'SUPOSADA', 'INCERTA')) DEFAULT 'SUPOSADA',
    notes TEXT,
    estat_validacio VARCHAR(10) CHECK (estat_validacio IN ('PENDENT', 'VALIDAT', 'REBUTJAT')) DEFAULT 'PENDENT',
    usuari_creador INTEGER REFERENCES Usuari(id) ON DELETE SET NULL,
    data_creacio TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_relacio_persona1 ON RelacioFamiliar(persona1_id);
CREATE INDEX idx_relacio_persona2 ON RelacioFamiliar(persona2_id);

-- Taula de Document
CREATE TABLE Document (
    id SERIAL PRIMARY KEY,
    persona_id INTEGER REFERENCES Persona(id) ON DELETE SET NULL,
    esdeveniment_id INTEGER REFERENCES Esdeveniment(id) ON DELETE SET NULL,
    tipus VARCHAR(20) CHECK (tipus IN ('ARXIU', 'EXTERN', 'FOTO', 'REGISTRE')),
    titol VARCHAR(255) NOT NULL,
    descripcio TEXT,
    url VARCHAR(255),
    data_creacio TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    usuari_creador INTEGER REFERENCES Usuari(id) ON DELETE SET NULL,
    estat_validacio VARCHAR(10) CHECK (estat_validacio IN ('PENDENT', 'VALIDAT', 'REBUTJAT')) DEFAULT 'PENDENT'
);

CREATE INDEX idx_document_persona ON Document(persona_id);
CREATE INDEX idx_document_esdeveniment ON Document(esdeveniment_id);

-- Taula de DocumentExtern
CREATE TABLE DocumentExtern (
    id SERIAL PRIMARY KEY,
    document_id INTEGER NOT NULL REFERENCES Document(id) ON DELETE CASCADE,
    tipus VARCHAR(30) CHECK (tipus IN ('PARES', 'MEMORIAL', 'MEMORIA_DEMOCRATICA', 'CENS', 'ALTRES')),
    referencia VARCHAR(255),
    data_consulta DATE
);

-- Taula de Contribucio
CREATE TABLE Contribucio (
    id SERIAL PRIMARY KEY,
    usuari_id INTEGER NOT NULL REFERENCES Usuari(id) ON DELETE CASCADE,
    tipus_entitat VARCHAR(20) CHECK (tipus_entitat IN ('PERSONA', 'ESDEVENIMENT', 'RELACIO', 'DOCUMENT', 'LLIBRE', 'LLOC')),
    entitat_id INTEGER NOT NULL,
    accio VARCHAR(20) CHECK (accio IN ('CREACIO', 'MODIFICACIO', 'ELIMINACIO')),
    dades_anteriors JSONB,
    dades_noves JSONB,
    data_accio TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_contribucio_usuari ON Contribucio(usuari_id);
CREATE INDEX idx_contribucio_tipus_entitat ON Contribucio(tipus_entitat, entitat_id);

-- Taula de Moderacio
CREATE TABLE Moderacio (
    id SERIAL PRIMARY KEY,
    contribucio_id INTEGER NOT NULL REFERENCES Contribucio(id) ON DELETE CASCADE,
    moderador_id INTEGER NOT NULL REFERENCES Usuari(id) ON DELETE CASCADE,
    estat VARCHAR(10) CHECK (estat IN ('PENDENT', 'APROVAT', 'REBUTJAT')),
    comentaris TEXT,
    data_moderacio TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_moderacio_contribucio ON Moderacio(contribucio_id);
CREATE INDEX idx_moderacio_estat ON Moderacio(estat);

-- Vistes

-- Vista per a cerca ràpida de persones
CREATE OR REPLACE VIEW CercaPersones AS
SELECT p.id, p.nom, p.cognoms, p.data_naixement, p.data_defuncio,
       STRING_AGG(DISTINCT e.tipus, ', ') AS esdeveniments,
       COUNT(DISTINCT r.id) AS num_relacions
FROM Persona p
LEFT JOIN PersonaEsdeveniment pe ON p.id = pe.persona_id
LEFT JOIN Esdeveniment e ON pe.esdeveniment_id = e.id
LEFT JOIN RelacioFamiliar r ON p.id = r.persona1_id OR p.id = r.persona2_id
WHERE p.estat_validacio = 'VALIDAT'
GROUP BY p.id;

-- Vista per a relacions familiars
CREATE OR REPLACE VIEW ArbreGenealogic AS
SELECT 
  p1.id AS id1, p1.nom AS nom1, p1.cognoms AS cognoms1,
  p2.id AS id2, p2.nom AS nom2, p2.cognoms AS cognoms2,
  rf.tipus_relacio, rf.certesa
FROM RelacioFamiliar rf
JOIN Persona p1 ON rf.persona1_id = p1.id
JOIN Persona p2 ON rf.persona2_id = p2.id
WHERE rf.estat_validacio = 'VALIDAT';

-- Vista per a esdeveniments pendents de moderació
CREATE OR REPLACE VIEW PendentsModeracio AS
SELECT c.id, u.nom AS usuari, c.tipus_entitat, c.accio, c.data_accio
FROM Contribucio c
JOIN Usuari u ON c.usuari_id = u.id
LEFT JOIN Moderacio m ON c.id = m.contribucio_id
WHERE m.id IS NULL OR m.estat = 'PENDENT';

-- Funció per actualitzar data_actualitzacio automàticament
CREATE OR REPLACE FUNCTION update_actualitzacio_timestamp()
RETURNS TRIGGER AS $$
BEGIN
   NEW.data_actualitzacio = CURRENT_TIMESTAMP;
   RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger per a Persona
CREATE TRIGGER persona_update_timestamp
BEFORE UPDATE ON Persona
FOR EACH ROW
EXECUTE FUNCTION update_actualitzacio_timestamp();

-- Trigger per a Esdeveniment
CREATE TRIGGER esdeveniment_update_timestamp
BEFORE UPDATE ON Esdeveniment
FOR EACH ROW
EXECUTE FUNCTION update_actualitzacio_timestamp();