# CercaGenealogica

Aplicació web en **Go** orientada a construir una plataforma de recerca i col·laboració genealògica.

## Què inclou aquesta versió

- **Autenticació completa**: registre, activació per correu, inici/tancament de sessió, recuperació de contrasenya.
- **Perfil d’usuari**: dades personals, canvi d’email amb confirmació/reversió, privacitat per camp, preferència d’idioma.
- **Perfils públics** (`/u/...`) i **ranking** per punts.
- **Punts i activitat**: regles de punts, registre d’accions, recalcul i rànquing.
- **Moderació**: cua d’elements pendents i accions d’aprovar/rebutjar.
- **Administració** (segons permisos):
  - Territori: països, nivells administratius, municipis i noms/codis històrics.
  - Eclesiàstic: arquebisbats/bisbats/arxiprestats i assignacions.
  - Documentals: arxius i llibres (CRUD, enllaços arxiu↔llibre, pàgines).
  - Polítiques i permisos, i regles de punts.
- **Internacionalització (i18n)**: català, anglès i occità (cookie `lang`, `Accept-Language` i preferència d’usuari).

## Estructura del repositori (resum)

```text
.
├── cnf/        Configuració (config.cfg) + parser tipat
├── core/       Lògica web (handlers, seguretat, plantilles, permisos, mail, etc.)
├── db/         Capa de dades (DB interface + SQLite/MySQL/PostgreSQL + esquemes SQL)
├── locales/    Traduccions (cat/en/oc en JSON)
├── static/     CSS/JS/imatges + dades JSON (p.ex. countries.json)
├── templates/  Plantilles HTML (públiques, privades i admin)
├── tests/      Unit tests i integration tests
├── tools/      Scripts d’utilitat (p.ex. tools/test.sh)
└── main.go     Entrypoint: rutes HTTP i inicialització d’App/DB
```

## Requisits

- Go (recomanat 1.21 o superior).
- Si uses SQLite: entorn amb **CGO** habilitat (dependència habitual del driver SQLite).

## Configuració

La configuració es carrega des de `cnf/config.cfg` (format `CLAU=valor`).

Claus principals:

- `DB_ENGINE`: `sqlite` | `postgres` | `mysql`
- `DB_PATH`: ruta del fitxer SQLite (quan `DB_ENGINE=sqlite`)
- `DB_HOST`, `DB_USR`, `DB_PASS`, `DB_PORT`, `DB_NAME`: connexió Postgres/MySQL
- `RECREADB`: `true|false` (si és `true`, aplica l’SQL de l’esquema a l’arrencada)
- `REGISTERD`: `true|false` (habilita fluxos relacionats amb registre)
- `MAIL_ENABLED`, `MAIL_FROM`, `MAIL_SMTP_HOST`, `MAIL_SMTP_PORT`
- `LOG_LEVEL`: `silent|error` (només errors), `info`, `debug`
- `ENVIRONMENT`: `development|production` (si no hi és, es mira la variable d’entorn `ENVIRONMENT`)

Veure `cnf/README.md` per exemples complets.

## Execució

```bash
go run ./...
```

El servidor arrenca a:

```text
http://localhost:8080
```

## Rutes principals

### Públiques

- `GET /` landing
- `GET /condicions-us`
- Canvi d’idioma: `GET /cat/`, `GET /en/`, `GET /oc/` (posa cookie i redirigeix al referer)
- `POST /registre` (registre)
- `GET /activar?token=...` (activació)
- `GET|POST /regenerar-token` (regeneració de token d’activació)
- `POST /login` (login)
- `GET /recuperar` / `POST /recuperar` (recuperació de contrasenya)
- `GET /u/...` (perfil públic)

### Autenticades

- `GET /inici`
- `GET /logout`
- `GET /perfil` i actualitzacions:
  - `POST /perfil/dades`
  - `POST /perfil/privacitat`
  - `POST /perfil/contrasenya`
  - `GET /perfil/email-confirm` i `GET /perfil/email-revert`
- `GET /ranking`
- Persones:
  - `GET /persones` (llista, requereix login)
  - `GET /persones/new`, `GET /persones/{id}`, `GET /persones/{id}/edit`
  - `POST /persones` / `POST /persones/save` / `POST|PUT /persones/{id}`
- Arxius (lectura): `GET /arxius` i `GET /arxius/{id}`

### Administració (segons permisos)

- Països: `/admin/paisos` …
- Nivells administratius: `/territori/nivells` … i `/territori/paisos/{id}/nivells` …
- Municipis: `/territori/municipis` … (codis postals, noms històrics, assignació eclesiàstica)
- Eclesiàstic: `/territori/eclesiastic` …
- Polítiques i assignacions: `/admin/politiques` …
- Punts: `/admin/punts/regles` … + recalcul
- Moderació: `/moderacio` …
- Documentals:
  - Arxius: `/documentals/arxius` …
  - Llibres: `/documentals/llibres` … (inclou pàgines i relació arxiu↔llibre)

## Base de dades

El paquet `db/` proporciona un contracte `DB` i implementacions per SQLite/PostgreSQL/MySQL.

- Esquemes:
  - `db/SQLite.sql`
  - `db/PostgreSQL.sql`
  - `db/MySQL.sql`

En arrencar, si `RECREADB=true`, l’aplicació aplica l’esquema segons el motor seleccionat.

## Seguretat i hardening (resum)

- **CSRF**: patró *double-submit* amb cookie HttpOnly `cg_csrf` + token al formulari.
- **Rate limit**: *token bucket* per ruta i per IP (o sessió si existeix).
- **Bloqueig per IP**: `BLOCKED_IPS` (llista separada per comes) a `config.cfg`.
- **Static files**: servei controlat sota `/static/` (evita traversal i llistats, i filtra rutes).

## Tests

Per executar tota la suite amb cobertura:

```bash
./tools/test.sh
```

Documentació detallada a `tests/README.md`.

## Documentació interna

La carpeta `.codex/` conté documentació de treball i materials interns (estructura, guies, prompts/roadmap, etc.).
