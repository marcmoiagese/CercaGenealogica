


## Estructura de directoris

```plaintext
CercaGenealogica/
├── go.mod
├── go.sum
├── main.go
├── cnf/
│   └── config.yaml
├── db/
│   ├── postgresql.go
│   ├── sqlite.go
│   ├── database.go
│   └── sql/
│       ├── postgresql.sql
│       └── sqlite.sql
└── core/
```


## Pre-Requisits
Assegura't que tens instal·lades aquestes dependències del sistema:


# Per a SQLite (requereix CGO)
```bash
sudo apt-get install gcc
```

# Per a PostgreSQL (opcional)
```bash
sudo apt-get install libpq-dev
```

## Primeres passes

Inicialitza el mòdul:
```bash
go mod init github.com/marcmoiagese/CercaGenealogica
```

Instal·la dependències:
```bash
go mod tidy
```

Compila i executa:
```bash
go run main.go
```

