## Cercador genealogic

## Estructura de directoris

```plaintext
app/
├── main.go
├── go.mod
├── cnf/
│   └── config.cfg
├── db/
│   ├── sqlite.go
│   ├── SQLite.sql
│   └── motor.go
├── core/
│   └── cerca/
│       ├──cerca.go
│       └── static/
│           └── script.js
├── modules/
│       └── Importacio
│           └── Arquevisbats
│               └── impbisbats.go
├── web/
│   ├── css/
│   ├── handlers/
│   │   ├── cerca.go
│   │   ├── pendents.go
│   │   ├── static.go
│   │   └── upload.go
│   ├── static/
│   │   ├── duplicats.js
│   │   └── cercador.js
│   └── templates/
│       ├── index.html
│       ├── pendents.html
│       ├── revisio_duplicats.html
│       └── upload.html
├── .gitignore
├── go.mod
├── go.sum
└── main.go
```