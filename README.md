# CercaGenealogica

Aplicació web en **Go** per a **recerca i col·laboració genealògica**, centrada en la **indexació de documentació històrica** (llibres, pàgines i registres) i la seva **vinculació amb persones**.

> Servei local per defecte: `http://localhost:8080`

## Què hi trobaràs (estat actual)

### Usuari, seguretat i perfils
- Registre (opcional), activació per correu, login/logout i recuperació de contrasenya.
- Perfil d’usuari (incloent privacitat per camp).
- Perfils públics (`/u/...`) i rànquing per punts.
- Mesures bàsiques: rate limit, bloqueig d’IP, CSRF, sessions.

### Documentals (arxius → llibres → pàgines → registres)
- CRUD d’**arxius** i **llibres** (amb relacions i metadades).
- Gestió de **pàgines**.
- **Registres**: llistat, formularis, vista detall i taula de cerca.
- Import/export (segons mòduls disponibles) i utilitats d’indexació.

### Indexació literal (RAW) i moderació
- Indexació RAW a taules `transcripcions_*`.
- Vinculació registre ⇄ persona (segons fluxos actuals).
- Moderació: cua d’elements pendents i accions d’aprovar/rebutjar (segons permisos).

### Punts i rànquing
- Regles de punts, registre d’activitat i recalcul.
- Vista de rànquing.

### Cognoms
- Llistat/cerca i pàgina de detall.
- Import i estadístiques.
- **Mapa (heatmap)** per distribució.

## Estructura del repositori

- `main.go` — rutes principals i arrencada del servidor
- `cnf/` — configuració (fitxer `cnf/config.cfg`)
- `core/` — handlers, seguretat, render de plantilles, permisos, i18n
- `db/` — capa d’accés a dades + esquemes `SQLite.sql`, `PostgreSQL.sql`, `MySQL.sql`
- `templates/` — plantilles HTML (layouts i vistes)
- `static/` — CSS/JS/assets
- `locales/` — traduccions JSON (`cat`, `en`, `oc`)
- `tests/` — tests unit i integració
- `tools/` — scripts auxiliars (p. ex. `test.sh`)
- `plantilla-temporal/` — maquetes/plantilles de referència (disseny)

## Requisits

- **Go 1.23** (el `go.mod` inclou `toolchain go1.23.10`)
- Compilació amb CGO (necessari per `github.com/mattn/go-sqlite3`)
  - Ubuntu/Debian: `sudo apt-get install -y build-essential`

## Arrencada ràpida (SQLite)

1) Configura `cnf/config.cfg` (valors per defecte recomanats en local):
- `DB_ENGINE=sqlite`
- `DB_PATH=./database.db`
- `RECREADB=true` (només desenvolupament; recrea l’esquema a l’arrencada)
- `MAIL_ENABLED=false` (si no vols SMTP en local)

2) Engega:
```bash
go run .
```

3) Obre:
- `http://localhost:8080`

### Correu en local (opcional)
Si vols provar activacions i recuperació de contrasenya, pots aixecar un SMTP de desenvolupament (exemple amb Mailpit):

```bash
docker run --rm -p 1025:1025 -p 8025:8025 axllent/mailpit
```

I a `cnf/config.cfg`:
- `MAIL_ENABLED=true`
- `MAIL_SMTP_HOST=localhost`
- `MAIL_SMTP_PORT=1025`

UI de Mailpit: `http://localhost:8025`

## Configuració (`cnf/config.cfg`)

Claus principals:
- `DB_ENGINE`: `sqlite` | `postgres` | `mysql`
- `DB_PATH`: path del fitxer SQLite
- `DB_HOST`, `DB_PORT`, `DB_USR`, `DB_PASS`, `DB_NAME`: per Postgres/MySQL
- `RECREADB`: si és `true`, aplica l’esquema del motor a l’arrencada (**compte en entorns amb dades**)
- `REGISTERD`: activa/desactiva el registre d’usuaris
- `MAIL_ENABLED`, `MAIL_FROM`, `MAIL_SMTP_HOST`, `MAIL_SMTP_PORT`
- `LOG_LEVEL`: `silent/error`, `info`, `debug`
- `ENVIRONMENT`: si no es defineix, `development`

## Tests

Executar tota la suite:
```bash
./tools/test.sh
```

> Nota: el repositori treballa amb SQLite i pot tenir proves d’integració multi-DB segons la configuració de tests.

## Esquema SQL i compatibilitat entre motors

Quan es toca l’esquema, cal mantenir homogeni:
- `db/SQLite.sql`
- `db/PostgreSQL.sql`
- `db/MySQL.sql`

## Documentació interna i roadmap

A `.codex/` tens:
- `overview/` — visió global i decisions
- `prompts/roadmap/` — fases implementables (prompts per Codex)
- `instructions/` — regles de treball per evitar regressions

## Pendents destacats (backlog)

- Fer visible l’**historial de canvis** i comparació de versions en registres (estil wiki/confluence).
- Tancar el **fix de permisos** (evitar qualsevol elevació per defecte i garantir que assignar polítiques té efecte).
- Evolució de la part genealògica (cerques i arbres amb càrrega progressiva).

---
