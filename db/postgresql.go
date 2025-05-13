package db

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

// PostgreSQL implementa la interfície Database per a PostgreSQL
type PostgreSQL struct {
	config PostgreSQLConfig
	db     *sql.DB
}

// NewPostgreSQL crea una nova instància de PostgreSQL
func NewPostgreSQL(config PostgreSQLConfig) (*PostgreSQL, error) {
	return &PostgreSQL{config: config}, nil
}

func (p *PostgreSQL) Connect() error {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		p.config.Host, p.config.Port, p.config.User, p.config.Password, p.config.DBName, p.config.SSLMode)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("error connectant a PostgreSQL: %v", err)
	}

	p.db = db
	return nil
}

func (p *PostgreSQL) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

func (p *PostgreSQL) Migrate() error {
	// Implementar migracions des de postgresql.sql
	// Pots llegir el fitxer SQL i executar les consultes
	return nil
}

func (p *PostgreSQL) Exec(query string, args ...interface{}) error {
	_, err := p.db.Exec(query, args...)
	return err
}

func (p *PostgreSQL) Query(query string, args ...interface{}) (Rows, error) {
	return p.db.Query(query, args...)
}

func (p *PostgreSQL) QueryRow(query string, args ...interface{}) Row {
	return p.db.QueryRow(query, args...)
}
