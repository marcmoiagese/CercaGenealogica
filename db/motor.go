package db

import (
	"fmt"
	"os"
	"strings"
)

type DB interface {
	Connect() error
	Query(query string, args ...interface{}) ([]map[string]interface{}, error)
	Exec(query string, args ...interface{}) (int64, error)
	Close()
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

	// Connectem
	if err := dbInstance.Connect(); err != nil {
		return nil, err
	}

	// Recrear BD si cal
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
	data, err := os.ReadFile(sqlFile)
	if err != nil {
		return fmt.Errorf("no s'ha pogut llegir el fitxer SQL: %v", err)
	}

	queries := strings.Split(string(data), ";")

	for _, q := range queries {
		trimmed := strings.TrimSpace(q)
		if trimmed == "" {
			continue
		}

		if _, err := db.Exec(trimmed); err != nil {
			return fmt.Errorf("error executant '%s': %v", trimmed, err)
		}
	}

	return nil
}
