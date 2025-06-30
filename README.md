## Cercador genealogic

## Estructura de directoris

```plaintext
app/
├── cnf/
│   └── config.cfg
├── db/
│   ├── sqlite.go
│   ├── SQLite.sql
│   └── motor.go
├── core/
│   ├── cerca/
│   │   └──cerca.go
│   └── mon/
│       ├── handler.go 
│       ├── model.go
│       └── service.go
├── modules/
│       └── Importacio
│           └── Arquevisbats
│               └── impbisbats.go
├── web/
│   ├── css/
│   ├── handlers/
│   │   ├── cerca.go
│   │   ├── mon.go
│   │   ├── pendents.go
│   │   ├── static.go
│   │   └── upload.go
│   ├── static/
│   │   ├── duplicats.js
│   │   └── cercador.js
│   └── templates/
│       ├── index.html
│       ├── municipis.html
│       ├── pendents.html
│       ├── revisio_duplicats.html
│       └── upload.html
├── .gitignore
├── go.mod
├── go.sum
└── main.go
```

```bash
go get github.com/mattn/go-sqlite3
go get github.com/lib/pq         # Postgres
go get github.com/go-sql-driver/mysql
```