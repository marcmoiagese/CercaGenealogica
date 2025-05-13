# Base de Dades Genealògica - Documentació Tècnica Completa

![Diagrama Esquemàtic](Schema.png)

## 1. Taula `Usuari`

### 📝 Descripció
Gestiona l'accés i permisos de tots els usuaris del sistema.

### 🗂 Estructura

| Camp                | Tipus          | Descripció                                                                                     | Restriccions                                  |
|---------------------|----------------|------------------------------------------------------------------------------------------------|-----------------------------------------------|
| `id`               | SERIAL/INTEGER | Identificador únic autoincremental                                                             | **PK**                                       |
| `email`            | VARCHAR(255)   | Adreça electrònica única                                                                       | `UNIQUE`, `NOT NULL`                         |
| `contrasenya_hash` | VARCHAR(255)   | Hash de contrasenya (bcrypt/scrypt)                                                           | `NOT NULL`                                   |
| `data_registre`    | TIMESTAMP      | Data de registre automàtica                                                                    | `DEFAULT CURRENT_TIMESTAMP`                  |
| `nivell_acces`     | ENUM           | `VISITANT` (només lectura)<br>`COLABORADOR` (afegir dades)<br>`MODERADOR`<br>`ADMIN`          | `DEFAULT 'VISITANT'`                         |
| `estat`           | ENUM           | `ACTIU`<br>`SUSPES` (temporal)<br>`INACTIU` (definitiu)                                        | `DEFAULT 'ACTIU'`                            |

### 🔗 Relacions
- **1:N** amb `Persona`, `Esdeveniment`, `Document`
- **1:1** amb `Moderacio` (com a moderador)

### ⚙️ Configuració PostgreSQL
```sql
CREATE TYPE nivell_acces_type AS ENUM ('VISITANT', 'COLABORADOR', 'MODERADOR', 'ADMIN');
CREATE TYPE estat_usuari_type AS ENUM ('ACTIU', 'SUSPES', 'INACTIU');
```

## ⚙️ Configuració Específica per SQLite

### 1.1. Simulació de Tipus ENUM
Per a les columnes que requereixen valors predefinits:

```sql
-- Per a la taula Usuari
CREATE TABLE Usuari (
    nivell_acces TEXT CHECK (nivell_acces IN ('VISITANT', 'COLABORADOR', 'MODERADOR', 'ADMIN')),
    estat TEXT CHECK (estat IN ('ACTIU', 'SUSPES', 'INACTIU'))
);

-- Per a la taula Esdeveniment
CREATE TABLE Esdeveniment (
    tipus TEXT CHECK (tipus IN ('BATEIG', 'MATRIMONI', 'OBIT', 'NAIXEMENT', 'CENS', 'ALTRES'))
);
```

## 1.2. Habilitació de Claus Foranes

**Necessari executar en cada connexió**:
```sql
PRAGMA foreign_keys = ON;
```

### 1.3. Gestió de Timestamps Automàtics
```sql
-- Trigger per actualitzar data_actualitzacio en Persona
CREATE TRIGGER persona_update_timestamp
AFTER UPDATE ON Persona
BEGIN
    UPDATE Persona SET data_actualitzacio = datetime('now') WHERE id = NEW.id;
END;

-- Trigger equivalent per Esdeveniment
CREATE TRIGGER esdeveniment_update_timestamp
AFTER UPDATE ON Esdeveniment
BEGIN
    UPDATE Esdeveniment SET data_actualitzacio = datetime('now') WHERE id = NEW.id;
END;
```

### 1.4. Gestió de JSON (emmagatzematge com TEXT)
```sql
-- A la taula Contribucio
CREATE TABLE Contribucio (
    dades_anteriors TEXT,  -- Emmagatzema JSON com a text
    dades_noves TEXT,
    
    -- Validació bàsica de format JSON
    CHECK (json_valid(dades_anteriors)),
    CHECK (json_valid(dades_noves))
);
```

### 1.5. Índexs per a Millorar Rendiment
    ```sql
    -- Índex per cerca de cognoms (sensibilitat a majúscules/minúscules)
    CREATE INDEX idx_persona_cognoms ON Persona(cognoms COLLATE NOCASE);

    -- Índex compost per cerques freqüents
    CREATE INDEX idx_esdeveniment_dates ON Esdeveniment(tipus, any, data_exacta);
    ```

### 1.6. Restriccions Addicionals
    ```sql
    -- Evitar auto-relacions en RelacioFamiliar
    CREATE TRIGGER prevent_self_relation
    BEFORE INSERT ON RelacioFamiliar
    BEGIN
        SELECT CASE
            WHEN NEW.persona1_id = NEW.persona2_id THEN
                RAISE(ABORT, 'No es permeten auto-relacions')
        END;
    END;

    -- Validació de dates coherents
    CREATE TRIGGER validate_persona_dates
    BEFORE INSERT ON Persona
    BEGIN
        SELECT CASE
            WHEN NEW.data_naixement > date('now') THEN
                RAISE(ABORT, 'Data de naixement futura no vàlida')
            WHEN NEW.data_defuncio > date('now') THEN
                RAISE(ABORT, 'Data de defunció futura no vàlida')
            WHEN NEW.data_naixement > NEW.data_defuncio THEN
                RAISE(ABORT, 'Data de naixement posterior a defunció')
        END;
    END;
    ```

### 1.7. Inicialització Recomanada
    ```sql
    -- Script d'inici recomanat
    PRAGMA journal_mode = WAL;  -- Millor rendiment en escriptures
    PRAGMA synchronous = NORMAL; -- Equilibri entre seguretat i rendiment
    PRAGMA foreign_keys = ON; -- Habilitar claus foranes
    PRAGMA busy_timeout = 30000;  -- 30 segons d'espera en bloquejos
    ```

## Nota Important sobre SQLite

### 🔴 Limitacions a tenir en compte

1. **Suport de tipus de dades**:
   - No suporta veritables tipus ENUM (s'han de simular amb constraints CHECK)
   - Tipus bàsics implementats:
     - `INTEGER`: Valors numèrics sencers
     - `REAL`: Nombres en coma flotant
     - `TEXT`: Cadenes de caràcters
     - `BLOB`: Dades binàries
     - `NULL`: Valor nul

2. **Gestió de JSON**:
   ```sql
   -- S'emmagatzema com TEXT amb validació opcional
   CREATE TABLE exemple_json (
       dades TEXT CHECK(json_valid(dades))
   );
   ```



## 2. Taula `Lloc`

### 📝 Descripció
Emmagatzema informació geogràfica sobre tots els llocs rellevants per als registres genealògics (pobles, ciutats, regions, països).

### 🗂 Estructura

| Camp            | Tipus          | Descripció                                                                                     | Restriccions                                  |
|-----------------|----------------|------------------------------------------------------------------------------------------------|-----------------------------------------------|
| `id`           | SERIAL/INTEGER | Identificador únic autoincremental                                                             | **PK**                                       |
| `nom`          | VARCHAR(255)   | Nom complet del lloc                                                                           | `NOT NULL`                                   |
| `tipus`        | VARCHAR(10)    | Categorització del lloc:<br>`POBLE`<br>`CIUTAT`<br>`REGIO`<br>`PAIS`<br>`ALTRES`              | `DEFAULT 'POBLE'`                            |
| `coordenades`  | VARCHAR(100)   | Coordenades geogràfiques (format lliure: "41.3825, 2.1769" o "40°25'N 3°42'W")                |                                               |
| `codi_postal`  | VARCHAR(20)    | Codi postal associat (per localitzacions actuals)                                              |                                               |
| `pais`         | VARCHAR(100)   | País al qual pertany el lloc                                                                   |                                               |
| `descripcio`   | TEXT           | Informació addicional:<br>- Noms històrics<br>- Canvis territorials<br>- Característiques      |                                               |

### 🔗 Relacions
- **1:N** amb `Esdeveniment` (lloc on ocorren els esdeveniments)
- **1:N** amb `Persona` (lloc de naixement/defunció)

### ⚙️ Configuració SQLite
```sql
   CHECK (tipus IN ('POBLE', 'CIUTAT', 'REGIO', 'PAIS', 'ALTRES'))
```

🗃️ Normalització Geogràfica

    Jerarquia recomanada:
    PAIS → REGIO → CIUTAT/POBLE

    Consistència de noms:
    - Utilitzar sempre la forma oficial actual
    - Registrar variants històriques a descripcio

🏷️ Casos Especials

    Llocs desapareguts: Marcar com tipus = 'ALTRES' i detallar a descripcio
    Fronteres històriques: Indicar període temporal a descripcio

## 3. Taula `Llibre`

### 📝 Descripció
Registra tots els llibres i fonts documentals que contenen la informació genealògica (registres parroquials, arxius notarials, censos, etc.).

### 🗂 Estructura

| Camp                | Tipus          | Descripció                                                                                     | Restriccions                                  |
|---------------------|----------------|------------------------------------------------------------------------------------------------|-----------------------------------------------|
| `id`               | SERIAL/INTEGER | Identificador únic autoincremental                                                             | **PK**                                       |
| `titol`            | VARCHAR(255)   | Títol descriptiu del llibre:<br>"Llibre de Baptismes 1801-1825"<br>"Registre de Defuncions 1890-1910" | `NOT NULL`                                   |
| `tipus`            | VARCHAR(20)    | Tipologia documental:<br>`BATEIG`<br>`MATRIMONI`<br>`OBIT`<br>`CENS`<br>`NOTARIAL`<br>`ALTRES` | `DEFAULT 'ALTRES'`                           |
| `any_inici`        | INTEGER        | Any d'inici del període documentat (formato YYYY)                                              |                                               |
| `any_fi`           | INTEGER        | Any de finalització del període documentat (format YYYY)                                       |                                               |
| `ubicacio_fisica`  | VARCHAR(255)   | Localització física actual:<br>"Arxiu Diocesà de Barcelona, secció 12, prestatge 34"           |                                               |
| `referencia_arxiu` | VARCHAR(255)   | Codi de referència de l'arxiu:<br>"ADB-BA-1801-1825"                                           |                                               |
| `descripcio`       | TEXT           | Detalls addicionals:<br>- Estat de conservació<br>- Notes paleogràfiques<br>- Limitacions d'accés |                                               |
| `estat_validacio`  | VARCHAR(10)    | Estat de verificació:<br>`PENDENT`<br>`VALIDAT`<br>`REBUTJAT`                                  | `DEFAULT 'VALIDAT'`                          |

### 🔗 Relacions
- **1:N** amb `Esdeveniment` (tots els esdeveniments registrats en aquest llibre)
- **1:N** amb `Document` (per a versions digitalitzades o annexos)

### ⚙️ Configuració PostgreSQL
```sql
CREATE TYPE tipus_llibre AS ENUM ('BATEIG', 'MATRIMONI', 'OBIT', 'CENS', 'NOTARIAL', 'ALTRES');
CREATE TYPE estat_validacio_type AS ENUM ('PENDENT', 'VALIDAT', 'REBUTJAT');
```

### ⚙️ Configuració SQLite
```sql
CHECK (tipus IN ('BATEIG', 'MATRIMONI', 'OBIT', 'CENS', 'NOTARIAL', 'ALTRES'))
CHECK (estat_validacio IN ('PENDENT', 'VALIDAT', 'REBUTJAT'))
```

📊 Exemple d'Ús
```sql
-- Cerca de llibres de bateigs del segle XIX
SELECT titol, any_inici, any_fi, ubicacio_fisica 
FROM Llibre
WHERE tipus = 'BATEIG' 
AND any_inici BETWEEN 1801 AND 1900
ORDER BY any_inici;

-- Comptatge d'esdeveniments per llibre
SELECT l.titol, COUNT(e.id) AS num_esdeveniments
FROM Llibre l
LEFT JOIN Esdeveniment e ON l.id = e.llibre_id
GROUP BY l.titol
ORDER BY num_esdeveniments DESC;
```

## 📚 Bones Pràctiques per a la Taula `Llibre`

### 1. Convencions de Títols
**Estructura recomanada**:
```plaintext
[Tipus d'esdeveniment] + [Localització] + [Període]
```

Exemples:
    - "Bateigs Parròquia Santa Maria del Mar 1720-1750"
    - "Cens Municipal de Barcelona 1857"
    - "Registre Notarial de Girona Lligall 12 (1601-1605)"

🏷️ Casos Especials
    - Llibres fragmentats: Registrar com a ítems separats amb notes a descripcio
    - Documents sense data: Deixar any_inici i any_fi com NULL i detallar a descripcio
    - Còpies digitals: Registrar a la taula Document amb referència al llibre_id