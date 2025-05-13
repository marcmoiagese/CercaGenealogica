package db

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Database interface defineix els mètodes comuns per a totes les bases de dades
type Database interface {
	Connect() error
	Close() error
	Migrate() error
	Exec(query string, args ...interface{}) error
	Query(query string, args ...interface{}) (Rows, error)
	QueryRow(query string, args ...interface{}) Row
	// Afegir més mètodes segons necessitis
}

// Row interface per a una fila de resultats
type Row interface {
	Scan(dest ...interface{}) error
}

// Rows interface per a múltiples files de resultats
type Rows interface {
	Scan(dest ...interface{}) error
	Next() bool
	Close() error
}

// Config estructura per a la configuració de la base de dades
type Config struct {
	Type     string
	Postgres PostgreSQLConfig
	SQLite   SQLiteConfig
}

// PostgreSQLConfig configuració específica de PostgreSQL
type PostgreSQLConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
}

// SQLiteConfig configuració específica de SQLite
type SQLiteConfig struct {
	Path string
}

// NewDatabase crea una nova instància de base de dades segons la configuració
func NewDatabase(config Config) (Database, error) {
	switch config.Type {
	case "postgresql":
		return NewPostgreSQL(config.Postgres)
	case "sqlite":
		return NewSQLite(config.SQLite)
	default:
		return nil, fmt.Errorf("tipus de base de dades no suportat: %s", config.Type)
	}
}

// LoadConfig carrega la configuració des d'un fitxer YAML
func LoadConfig(path string) (Config, error) {
	var config Config

	data, err := os.ReadFile(path)
	if err != nil {
		return config, fmt.Errorf("error llegint fitxer de configuració: %v", err)
	}

	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return config, fmt.Errorf("error deserialitzant configuració: %v", err)
	}

	return config, nil
}
