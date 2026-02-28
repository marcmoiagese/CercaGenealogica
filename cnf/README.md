# Configuració de la base de dades

L’aplicació pot treballar amb tres motors de base de dades:

- `sqlite`
- `postgres`
- `mysql`

La configuració es controla des del fitxer `config.cfg` i, opcionalment, pels fitxers SQL de creació d’esquema:

- `db/SQLite.sql`
- `db/PostgreSQL.sql`
- `db/MySQL.sql`

A continuació es detallen totes les combinacions típiques.

---

## 1. Paràmetres comuns de `config.cfg`

```ini
DB_ENGINE=sqlite        # sqlite | postgres | mysql
RECREADB=true           # true = aplica l'esquema des del fitxer SQL (no esborra dades)
RECREADB_RESET=false    # true = esborra i recrea la BD (nomes si RECREADB=true)
REGISTERD=true          # comportament funcional (registre d’usuaris, etc.)

# Per SQLite
DB_PATH=./database.db   # només s’usa si DB_ENGINE=sqlite

# Per Postgres / MySQL
DB_HOST=...
DB_USR=...
DB_PASS=...
DB_PORT=...
DB_NAME=...

MAIL_ENABLED=true       # habilita l'enviament de correus
MAIL_FROM=no-reply@localhost
MAIL_SMTP_HOST=localhost
MAIL_SMTP_PORT=25

LOG_LEVEL=debug         # silent/error | info | debug

# Espai personal
ESP_TREE_LIMIT=0        # 0 = sense limit d'arbres per usuari
```

L’enviament de correus intenta primer el binari `sendmail` del sistema i, si no està disponible, prova via SMTP a `MAIL_SMTP_HOST:MAIL_SMTP_PORT` (per defecte `localhost:25`).

> `RECREADB=true` fa que, a l’arrencada, s’apliqui el fitxer SQL corresponent al motor:
> - `sqlite`  → `db/SQLite.sql`
> - `postgres` → `db/PostgreSQL.sql`
> - `mysql`  → `db/MySQL.sql`

---

## 2. Mode SQLite (entorn local / per defecte)

### 2.1. Dev ràpid amb recreació automàtica

Ideal per a desenvolupament local, quan no cal conservar dades.

```ini
DB_ENGINE=sqlite
DB_PATH=./database.db
RECREADB=true
RECREADB_RESET=true
REGISTERD=true

LOG_LEVEL=debug
```

- A cada arrencada:
  - El fitxer `database.db` es recrea des de `db/SQLite.sql`.
  - Es perden totes les dades anteriors (entorn “fresh” per a tests).

### 2.2. SQLite persistent (sense recreació)

Quan vols mantenir dades entre arrencades.

```ini
DB_ENGINE=sqlite
DB_PATH=./database.db
RECREADB=false
REGISTERD=true

LOG_LEVEL=info
```

- Només es fa servir `db/SQLite.sql` quan tu l’executes manualment per inicialitzar la BD.
- L’aplicació no toca l’esquema (només fa servir la BD existent).

---

## 3. Mode PostgreSQL (contenidor `dev-postgres`)

Al `docker-compose.yml` tens definit:

```yaml
postgres:
  image: postgres:16
  container_name: dev-postgres
  restart: unless-stopped
  environment:
    POSTGRES_USER: recerca
    POSTGRES_PASSWORD: d3l$4b4n7p4$$a7$
    POSTGRES_DB: cerca_genealogica
  ports:
    - "5432:5432"
  volumes:
    - ./pgdata:/var/lib/postgresql/data
```

Això et dóna:

- Host (des de fora del servidor): `ip o fqdn`
- Host (des de dins del mateix servidor): `localhost` o `127.0.0.1`
- Port: `5432`
- Usuari: `recerca`
- Contrasenya: `d3l$4b4n7p4$$a7$`
- Base de dades: `cerca_genealogica`

> **Nota:** el codi ja força `sslmode=disable` a la cadena de connexió, així que no cal cap paràmetre extra per SSL.

### 3.1. Postgres amb recreació automàtica (entorn de test)

Requereix tenir `db/PostgreSQL.sql` preparat amb l’esquema equivalent al de SQLite.

```ini
DB_ENGINE=postgres
RECREADB=true
RECREADB_RESET=true
REGISTERD=true

DB_HOST=dev.marc.cat   # o localhost si l’app corre al mateix host
DB_USR=recerca
DB_PASS=d3l$4b4n7p4$$a7$
DB_PORT=5432
DB_NAME=cerca_genealogica

LOG_LEVEL=debug
```

- A l’arrencada:
  - Es connecta a la BD `cerca_genealogica`.
  - Executa el contingut de `db/PostgreSQL.sql`.
  - Amb `RECREADB_RESET=true`, es neteja l’esquema abans d’aplicar-lo.
- Útil per tests d’integració que necessiten un entorn net.

### 3.2. Postgres sense recreació (entorn compartit / semi-prod)

Aquí assumeixes que l’esquema ja existeix i no vols tocar-lo.

```ini
DB_ENGINE=postgres
RECREADB=false
REGISTERD=true

DB_HOST=devstack.marc.cat
DB_USR=recerca
DB_PASS=d3l$4b4n7p4$$a7$
DB_PORT=5432
DB_NAME=cerca_genealogica

LOG_LEVEL=info
```

- L’app simplement es connecta i treballa amb les taules existents.
- Esquema inicial creat prèviament amb `db/PostgreSQL.sql` (manualment).

---

## 4. Mode MySQL (contenidor `dev-mysql`)

Al `docker-compose.yml` tens:

```yaml
mysql:
  image: mysql:8.4
  container_name: dev-mysql
  restart: unless-stopped
  environment:
    MYSQL_ROOT_PASSWORD: rootpass
    MYSQL_DATABASE: cerca_genealogica
    MYSQL_USER: recerca
    MYSQL_PASSWORD: d3l$4b4n7p4$$a7$
  ports:
    - "3306:3306"
  volumes:
    - ./mysqldata:/var/lib/mysql
```

Paràmetres resultants:

- Host (des de fora del servidor): `ip o fqdn`
- Host (des de dins del mateix servidor): `localhost` o `127.0.0.1`
- Port: `3306`
- Usuari: `recerca`
- Contrasenya: `d3l$4b4n7p4$$a7$`
- Base de dades: `cerca_genealogica`

### 4.1. MySQL amb recreació automàtica

Requereix `db/MySQL.sql` amb l’esquema adaptat (tipus, AUTO_INCREMENT, etc.).

```ini
DB_ENGINE=mysql
RECREADB=true
RECREADB_RESET=true
REGISTERD=true

DB_HOST=dev.marc.cat
DB_USR=recerca
DB_PASS=d3l$4b4n7p4$$a7$
DB_PORT=3306
DB_NAME=cerca_genealogica

LOG_LEVEL=debug
```

- A l’arrencada:
  - Es connecta a `cerca_genealogica`.
  - Executa `db/MySQL.sql` per crear/actualitzar l’esquema.
  - Amb `RECREADB_RESET=true`, es neteja l’esquema abans d’aplicar-lo.

### 4.2. MySQL sense recreació

Quan la BD ja està inicialitzada i només vols que l’app la faci servir.

```ini
DB_ENGINE=mysql
RECREADB=false
REGISTERD=true

DB_HOST=dev.marc.cat
DB_USR=recerca
DB_PASS=d3l$4b4n7p4$$a7$
DB_PORT=3306
DB_NAME=cerca_genealogica

LOG_LEVEL=info
```

---

## 5. Taula resum de combinacions

| Ús                                 | DB_ENGINE | RECREADB | RECREADB_RESET | Fitxer SQL necessari         | Exemple de destí              |
|------------------------------------|-----------|----------|----------------|------------------------------|-------------------------------|
| Dev local ràpid (fitxer únic)     | sqlite    | true     | true           | `db/SQLite.sql`              | `./database.db`               |
| SQLite amb dades persistents      | sqlite    | false    | false          | (execució manual opcional)   | `./database.db`               |
| Tests amb Postgres “net”          | postgres  | true     | true           | `db/PostgreSQL.sql`          | `devstack.marc.cat:5432`      |
| Postgres compartit / semi-prod    | postgres  | false    | false          | (esquema creat abans)        | `devstack.marc.cat:5432`      |
| Tests amb MySQL “net”             | mysql     | true     | true           | `db/MySQL.sql`               | `devstack.marc.cat:3306`      |
| MySQL compartit / semi-prod       | mysql     | false    | false          | (esquema creat abans)        | `devstack.marc.cat:3306`      |

---

## 6. Notes pràctiques

- **Contrasenyes amb `$`**  
  En `config.cfg` no cal escapar `$`, però si mai passes la contrasenya com a variable d’entorn en un shell, caldrà:
  - posar-la entre cometes simples `'...'`, o  
  - escapar els `$` (`\$`).

- **Canviar host segons on corre l’aplicació**  
  - Si l’aplicació Go corre a la mateixa màquina que Docker:
    - Pots usar `DB_HOST=localhost`.
  - Si la llances des d’una altra màquina:
    - Usa el DNS: `DB_HOST=dev.marc.cat`.

- **REGISTERD**  
  Aquest paràmetre afecta funcionalitat de l’aplicació (registre d’usuaris, etc.) però és independent del motor de BD; pots combinar-lo amb qualsevol de les configuracions anteriors.
