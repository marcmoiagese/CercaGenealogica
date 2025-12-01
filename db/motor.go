package db

import (
	"fmt"
	"os"
	"strings"
)

type DB interface {
	Connect() error
	Close()
	Exec(query string, args ...interface{}) (int64, error)
	Query(query string, args ...interface{}) ([]map[string]interface{}, error)
	InsertUser(user *User) error
	SaveActivationToken(email, token string) error
	GetUserByEmail(email string) (*User, error)
	ActivateUser(token string) error
	AuthenticateUser(usernameOrEmail, password string) (*User, error)
	SaveSession(sessionID string, userID int, expiry string) error
	GetSessionUser(sessionID string) (*User, error)
	DeleteSession(sessionID string) error
}

// Tipus comú d'usuari al paquet `db`
type User struct {
	ID            int
	Usuari        string
	Name          string
	Surname       string
	Email         string
	Password      []byte
	DataNaixament string
	Active        bool
	CreatedAt     string
	Pais          string
	Estat         string
	Provincia     string
	Poblacio      string
	CodiPostal    string
}

// Funció principal per obtenir una connexió i recrear BD si cal
func NewDB(config map[string]string) (DB, error) {
	var dbInstance DB
	engine := config["DB_ENGINE"]

	switch engine {
	case "sqlite":
		dbInstance = &SQLite{Path: config["DB_PATH"]}
	case "postgres":
		dbInstance = &PostgreSQL{
			Host:   config["DB_HOST"],
			Port:   config["DB_PORT"],
			User:   config["DB_USR"],
			Pass:   config["DB_PASS"],
			DBName: config["DB_NAME"],
		}
	case "mysql":
		dbInstance = &MySQL{
			Host:   config["DB_HOST"],
			Port:   config["DB_PORT"],
			User:   config["DB_USR"],
			Pass:   config["DB_PASS"],
			DBName: config["DB_NAME"],
		}
	default:
		return nil, fmt.Errorf("motor de BD desconegut: %s", engine)
	}

	// Connectem primer
	if err := dbInstance.Connect(); err != nil {
		return nil, err
	}

	// Si cal, recrearem la BD
	if config["RECREADB"] == "true" {
		sqlFile := getSQLFilePath(engine)
		if err := CreateDatabaseFromSQL(sqlFile, dbInstance); err != nil {
			return nil, fmt.Errorf("error recreant BD amb %s: %v", engine, err)
		}
	}

	return dbInstance, nil
}

// Obtenir el path del fitxer SQL segons el motor
func getSQLFilePath(engine string) string {
	switch engine {
	case "sqlite":
		return "db/SQLite.sql"
	case "postgres":
		return "db/PostgreSQL.sql"
	case "mysql":
		return "db/MySQL.sql"
	default:
		return ""
	}
}

// Funció genèrica per executar totes les sentències SQL d'un fitxer
func CreateDatabaseFromSQL(sqlFile string, db DB) error {
	logInfof("Recreant BD des de: %s", sqlFile)
	data, err := os.ReadFile(sqlFile)
	if err != nil {
		return fmt.Errorf("no s'ha pogut llegir el fitxer SQL: %w", err)
	}

	raw := string(data)

	// 1) Elimina línies de comentari i línies buides,
	//    però conserva el SQL que vingui després en altres línies
	var b strings.Builder
	for _, line := range strings.Split(raw, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") || trimmed == "" {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	cleanSQL := b.String()

	// 2) Separa per ';' i neteja espais. (Semicolons al final del statement)
	parts := strings.Split(cleanSQL, ";")

	// 3) Executa-ho dins d’una única transacció perquè ningú vegi mig esquema
	if _, err := db.Exec("BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("no s’ha pogut començar transacció: %w", err)
	}
	defer func() {
		// en cas d’error, el caller retornarà; aquí fem un ROLLBACK best-effort
		_, _ = db.Exec("ROLLBACK")
	}()

	// 4) Activa FKs (per si el fitxer no ho fa)
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		return fmt.Errorf("error activant foreign_keys: %w", err)
	}

	// 5) Executa cada statement
	for _, stmt := range parts {
		q := strings.TrimSpace(stmt)
		if q == "" {
			continue
		}
		// Evita BEGIN/COMMIT del fitxer, si n’hi hagués
		low := strings.ToLower(q)
		if low == "begin" || low == "commit" || strings.HasPrefix(low, "begin ") || strings.HasPrefix(low, "commit ") {
			continue
		}

		if _, err := db.Exec(q); err != nil {
			// Mostra un tros de l’SQL per facilitar el debug
			snip := q
			if len(snip) > 120 {
				snip = snip[:120] + " ..."
			}
			return fmt.Errorf("error executant '%s': %w", snip, err)
		}
	}

	// 6) Commit final
	if _, err := db.Exec("COMMIT"); err != nil {
		return fmt.Errorf("error fent COMMIT: %w", err)
	}

	logInfof("BD recreada correctament")
	return nil
}
