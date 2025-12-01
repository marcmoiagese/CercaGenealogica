
# CercaGenealogica

Aplicació web en Go per gestionar l’autenticació d’usuaris i posar les bases d’un futur cercador genealògic (llibres, persones, municipis, etc.).  
Ara mateix el projecte se centra en:

- Registre i activació de comptes d’usuari.
- Inici i tancament de sessió amb cookies segures.
- Internacionalització bàsica (català, anglès i occità).
- Estructura de base de dades pensada per a funcionalitats genealògiques futures.
- Capes clares per a front-end, back-end i accessos a BD.
- Suite de tests (unitaris i d’integració, alguns encara pendents).

> **Nota:** Aquest README descriu l’estat actual del projecte (Fases F0, F1 i inici de F2 del roadmap intern a `.codex/`).

---

## 1. Arquitectura del projecte

Arrel del repositori:

```text
.
├── cnf/              # Càrrega de configuració bàsica (config.cfg)
├── core/             # Lògica principal (i18n, logger, plantilles, usuaris, webserver)
├── db/               # Capa d’accés a dades (interfície DB + SQLite/MySQL/PostgreSQL)
├── locales/          # Fitxers d’idioma (cat, en, oc)
├── static/           # CSS, JS i imatges estàtiques
├── templates/        # Plantilles HTML (públiques i privades)
├── tests/            # Tests unitaris i d’integració
├── tools/            # Scripts d’utilitat (p. ex. test.sh)
├── main.go           # Punt d’entrada de l’aplicació web
├── go.mod / go.sum   # Dependències Go dels diferents paquets
└── .codex/           # Documentació interna, roadmap i instruccions de treball
```

### 1.1. `cnf/`

- `cnf.go`: llegeix `config.cfg` i el carrega en un `map[string]string`.
- `config.cfg`: fitxer clau=valor amb la configuració bàsica de l’app  
  (motor de BD, ruta del fitxer SQLite, entorn, etc.).

### 1.2. `core/`

- `i18n.go`  
  Carrega els fitxers `locales/*.json` i manté els textos traduïts en memòria.  
  Proporciona la funció `T(lang, key)` i la resolució d’idioma a partir de la cookie `lang`.

- `logger.go`  
  Logger senzill amb nivells `silent`, `error`, `info`, `debug`, configurats via `LOG_LEVEL`.

- `templates.go`  
  Registra i renderitza plantilles HTML, amb un `DataContext` comú que inclou:
  - si l’usuari està loguejat,
  - idioma actual,
  - dades específiques de cada pàgina.

- `usuaris.go`  
  Conté la lògica d’autenticació:
  - Registre d’usuari (amb validacions bàsiques, CAPTCHA simple i token d’activació).
  - Activació de compte via token.
  - Inici i tancament de sessió (cookie `cg_session` + registre a BD).
  - Regeneració de token d’activació.

- `webserver.go`  
  Defineix:
  - capçaleres de seguretat (CSP, HSTS, X-Frame-Options, X-Content-Type-Options, etc.),
  - rate limiting per IP/sessió i per ruta (token bucket),
  - servei controlat de `/static/`,
  - funcions d’ajut per obtenir IP real.

### 1.3. `db/`

- `motor.go`  
  Defineix la interfície genèrica:

  ```go
  type DB interface {
      Connect() error
      Close()
      Exec(query string, args ...interface{}) (sql.Result, error)
      Query(query string, args ...interface{}) (*sql.Rows, error)

      InsertUser(user *User) (int64, error)
      SaveActivationToken(userID int64, token string, expiresAt time.Time) error
      GetUserByEmail(email string) (*User, error)
      ActivateUser(token string, now time.Time) (*User, error)
      AuthenticateUser(identifier, password string) (*User, error)

      SaveSession(userID int64, token string, expiresAt time.Time) error
      GetSessionUser(token string, now time.Time) (*User, error)
      DeleteSession(token string) error
  }
  ```

  i s’encarrega de seleccionar i inicialitzar el motor concret (`sqlite`, `mysql`, `postgres`) en funció de la configuració (`DB_ENGINE`). També pot recrear l’esquema de la BD a partir del fitxer SQL corresponent.

- `sqlite.go`, `mysql.go`, `postgres.go`  
  Implementacions específiques per a cada motor, compartint la mateixa interfície `DB`.

- `SQLite.sql`  
  Esquema complet per SQLite:
  - usuaris, sessions i logs d’accés,
  - taules de grups i polítiques,
  - estructures per a persones, relacions i entitats territorials,
  - taules de llibres i metadades genealògiques.

### 1.4. `locales/`

Fitxers JSON d’idioma:

- `cat.json` – català  
- `en.json` – anglès  
- `oc.json` – occità

Cada fitxer defineix les mateixes claus (textos de portada, login, registre, regenerar token, espai privat, footer, etc.) perquè les plantilles puguin cridar `{{ t .Lang "clau" }}` sense dependre de l’idioma concret.

### 1.5. `static/`

- `static/css/`  
  - `estils.css`: estils base del lloc (layout, capçalera, peu, etc.).
  - `login-modal.css`: estils pels modals d’inici de sessió / registre / recuperar contrasenya.
  - `menu.css`: estils del menú lateral de l’espai privat.
  - `perfil-dropdown.css`: estils del desplegable de perfil d’usuari.
  - `regenerar-token.css`: pàgina específica de regeneració de token.
  - `registre.css`: pàgina específica de resultat del registre (èxit/error).

- `static/js/`  
  - `login-modal.js`: control dels modals de login/registre/recuperar contrasenya.
  - `menu.js`: gestió del menú lateral i overlay.
  - `perfil-dropdown.js`: desplegable del perfil d’usuari.
  - `idioma.js`: selector d’idioma (cookie `lang` + canvi visual d’idioma).

- `static/img/`  
  - `logo.png`: logo actual del projecte.

### 1.6. `templates/`

- `templates/layouts/`
  - `base.html`: layout base amb `header`, `footer`, menú privat i blocs per estils / scripts / contingut.
  - `header-public.html`: capçalera per usuaris no autenticats (logo, selector d’idioma, botons de login/registre).
  - `header-private.html`: capçalera per usuaris autenticats (botó de menú, perfil, etc.).
  - `menu-private.html`: menú lateral de l’espai privat.
  - `footer.html`: peu de pàgina comú.
  - `styles-*.html` / `scripts-*.html`: agrupació d’estils i scripts per context (públic vs loguejat).
  - `modals-public.html`: inclou els tres modals principals de login/registre/recuperar.
  - `modal-login.html`, `modal-registre.html`, `modal-inici-sesio.html`: definició dels modals.

- Plantilles principals
  - `index.html`: landing pública amb accés a login/registre.
  - `index-logedin.html`: pàgina d’inici per a usuaris autenticats.
  - `activat-user.html`: resultat d’activació del compte (èxit/error).
  - `regenerar-token.html`: formulari per demanar un nou correu d’activació.
  - `registre-correcte.html`: pàgina mostrada després d’un registre exitós.
  - `registre-incorrecte.html`: pàgina d’error amb possibles causes i instruccions.
  - `condicions-us.html`: condicions d’ús (actualment pàgina fora del layout general).

### 1.7. `tests/`

- `tests/unit/`
  - `db_test.go`: comprova la connexió SQLite in-memory.
  - `usuaris_test.go`: comprova la coherència bàsica del hashing de contrasenya amb bcrypt.
  - `sessions_test.go`: esquelet per a futurs tests de sessions.
  - `test_helpers.go`: helper `Pending(t, "...")` per marcar tests pendents.

- `tests/integration/`
  - `login_test.go`, `registre_test.go`, `perfil_test.go`, `contrasenya_test.go`:  
    Esquelets de tests per a fluxos complets (registre/login/perfil/canvi contrasenya), actualment marcats com a pendents.

- `tests/common/`
  - `pending.go`: versió compartida del helper `Pending`.

- `tests/README.md`: explica com estructurar i executar els tests.

### 1.8. `tools/`

- `tools/test.sh`: script per executar tots els tests amb cobertura:

  ```bash
  go test ./... -cover -count=1
  ```

---

## 2. Configuració

El fitxer `cnf/config.cfg` segueix un format senzill `clau=valor`.  
Exemple d’esquelet (els valors reals dependran del teu entorn):

```ini
# Motor de base de dades: sqlite | mysql | postgres
DB_ENGINE=sqlite

# Exemple per SQLite
DB_SQLITE_PATH=database.db

# Exemple per MySQL
# DB_MYSQL_USER=usuari
# DB_MYSQL_PASSWORD=contrasenya
# DB_MYSQL_HOST=127.0.0.1
# DB_MYSQL_PORT=3306
# DB_MYSQL_NAME=cercagenealogica

# Exemple per PostgreSQL
# DB_PG_HOST=127.0.0.1
# DB_PG_PORT=5432
# DB_PG_USER=usuari
# DB_PG_PASSWORD=contrasenya
# DB_PG_NAME=cercagenealogica
# DB_PG_SSLMODE=disable

# Recrear esquema de BD a l’inici (ATENCIÓ: esborrarà dades)
RECREADB=false

# Nivell de log: silent | error | info | debug
LOG_LEVEL=info

# Entorn: development | production
ENV=development
```

> **Advertiment:**  
> Si `RECREADB=true`, en arrencar l’aplicació es tornarà a aplicar l’esquema SQL i podries perdre dades existents.

---

## 3. Requisits

- **Go** ≥ 1.21 (recomanat).
- Drivers de BD inclosos com a dependències Go:
  - `github.com/mattn/go-sqlite3`
  - `github.com/go-sql-driver/mysql`
  - `github.com/lib/pq`
- Compilador C disponible (per al driver SQLite, que utilitza CGO).

---

## 4. Execució en entorn de desenvolupament

1. **Clonar el repositori** i entrar al directori:

   ```bash
   git@github.com:marcmoiagese/CercaGenealogica.git
   cd CercaGenealogica
   ```

2. **revisar el fitxer de configuració** `cnf/config.cfg`:

   ```bash
   vi cnf/config.cfg
   ```

3. **Assegurar que les dependències estan descarregades:**

   ```bash
   go mod tidy
   ```

4. **Inicialitzar la BD (opcional)**:

   - Si fas servir SQLite i vols crear l’esquema des de zero, pots posar:
     ```ini
     DB_ENGINE=sqlite
     DB_SQLITE_PATH=database.db
     RECREADB=true
     ```
   - Arrencar l’app una vegada (es crearà `database.db` amb l’esquema de `db/SQLite.sql`) i després tornar a posar `RECREADB=false`.

5. **Arrencar el servidor:**

   ```bash
   go run ./...
   ```

   Per defecte, el servidor escolta a:

   ```text
   http://localhost:8080
   ```

---

## 5. Ús bàsic de l’aplicació

### 5.1. Pàgines públiques

- `GET /`  
  Mostra la landing amb:
  - text d’introducció,
  - botó de login,
  - botó de registre,
  - selector d’idioma.

- `GET /condicions-us`  
  Mostra les condicions d’ús (actualment en català).

- Canvi d’idioma:
  - `GET /cat/`
  - `GET /en/`
  - `GET /oc/`

  Aquestes rutes estableixen la cookie `lang` i redirigeixen a `/`.

### 5.2. Registre i activació

- `POST /registre`  
  Des del modal de registre:
  - valida dades bàsiques (format d’email, contrasenya, acceptació de condicions, CAPTCHA simple),
  - crea l’usuari a BD,
  - genera el token d’activació,
  - mostra una pàgina de “Registre correcte” amb instruccions.

- `GET /activar?token=...`  
  Handler d’activació:
  - valida el token,
  - marca l’usuari com a actiu,
  - mostra una pàgina d’èxit o error.

- `GET /regenerar-token`  
  Mostra el formulari per regenerar el correu d’activació.

- `POST /regenerar-token`  
  Processa la petició per enviar un nou correu d’activació (lògica interna per fer match amb l’usuari i regenerar el token).

### 5.3. Login i espai privat

- `POST /login`  
  Valida les credencials:
  - comprova usuari/correu + contrasenya,
  - que el compte estigui actiu,
  - aplica un CAPTCHA simple, S'ha de cambiar amb un amb cara i ulls en algun moment
  - registra i desa una sessió a BD,
  - crea una cookie `cg_session` amb `HttpOnly` i `SameSite=Lax`.

- `GET /inici`  
  Requereix sessió vàlida:
  - comprova la cookie `cg_session`,
  - busca la sessió a BD,
  - mostra la pàgina `index-logedin.html` amb informació bàsica de benvinguda.

- `GET /logout`  
  Tanca la sessió actual:
  - marca la sessió com a revocada a BD,
  - esborra la cookie `cg_session`,
  - redirigeix a `/`.

---

## 6. Execució de tests

Per executar tots els tests (unitaris + integració):

```bash
./tools/test.sh
```

Això equival a:

```bash
go test ./... -cover -count=1
```

- Alguns tests d’integració estan marcats com a **pendents**, mitjançant el helper:

  ```go
  common.Pending(t, "Descripció del que falta")
  ```

- Els tests existents comproven:
  - connexió bàsica amb SQLite,
  - coherència del hashing de contrasenyes amb bcrypt.

---

## 7. Roadmap i documentació interna

La carpeta `.codex/` conté:

- `overview/` – descripció del projecte, arquitectura, backend, frontend, base de dades i fluxos.
- `prompts/roadmap/` – fases planificades (F0, F1, F2, …) amb objectius:
  - F0: internacionalització bàsica,
  - F1: fonaments (autenticació, estructura de BD, seguretat mínima),
  - F2: espai d’usuari,
  - fases posteriors: funcionalitats genealògiques completes.
- `project_structure.md` – estructura actual del projecte, per tenir una visió ràpida.

Aquesta documentació és especialment útil per:

- entendre en quina fase es troba el projecte,
- decidir quines parts calen abans d’afegir nova funcionalitat,
- i mantenir una línia coherent de treball a mesura que el projecte creix.

---

## 8. Estat actual i línies de millora

A dia d’avui, l’aplicació proporciona una base sòlida per:

- autenticació d’usuaris,
- gestió de sessions i seguretat bàsica,
- internacionalització i estructura de plantilles,
- esquema de BD adaptat a futurs mòduls genealògics.

Línies de treball previstes (no exhaustives):

- Millorar la gestió de configuració i reutilització de la instància de BD.
- Enfortir la protecció CSRF als formularis.
- Completar i activar els tests d’integració (registre, login, canvi de contrasenya, perfil).
- Desplegar les funcionalitats específiques de genealogia (cerca, llistats, fitxes de persones i llibres).
