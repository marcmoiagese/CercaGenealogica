El paquet `db/` encapsula l’accés a dades i ofereix una interfície comuna (`db.DB`) amb implementacions per:

- **SQLite** (`db/sqlite.go`)
- **PostgreSQL** (`db/postgres.go`)
- **MySQL** (`db/mysql.go`)

## Esquemes SQL

Els esquemes estan versionats per motor:

- `SQLite.sql`
- `PostgreSQL.sql`
- `MySQL.sql`

Quan l’aplicació arrenca amb `RECREADB=true`, s’aplica l’esquema corresponent.

## Compatibilitat de placeholders

Per mantenir consultes comunes, una part de l’SQL es defineix amb `?` i es transforma quan cal:

- SQLite / MySQL: `?`
- PostgreSQL: `$1, $2, ...`

Això es gestiona a `db/sqlcommon.go`.

## Bootstrap de dades (per defecte)

Després de connectar, el `main.go` executa:

- `EnsureDefaultPolicies()` → crea polítiques base (p.ex. `admin`, `moderador`, `confiança`, `usuari`).
- `EnsureDefaultPointsRules()` → crea regles de punts base.

> Important: si la BD està buida o no té polítiques assignades, la capa `core/permissions.go` dona permisos d’admin per defecte per evitar bloquejos en entorns de desenvolupament.

## Models principals

Alguns tipus rellevants exposats pel paquet:

- `User`, `PrivacySettings`, `PasswordReset`, `EmailChange`
- `Politica`, `PolicyPermissions`, `Group`
- `PointsRule`, `UserActivity`, `UserPoints` (punts/rànquing)
- Entitats de territori (països, nivells, municipis, noms/codis històrics)
- Entitats documentals (`Arxiu`, `Llibre`, pàgines i relacions)
- Persones (amb estat de moderació)

## Tests

La major part del paquet està coberta per unit tests i integration tests (incloent proves multi-DB). Veure `tests/README.md`.
