package db

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

// SQLite implementa la interfície Database per a SQLite
type SQLite struct {
	config SQLiteConfig
	db     *sql.DB
}

// NewSQLite crea una nova instància de SQLite
func NewSQLite(config SQLiteConfig) (*SQLite, error) {
	return &SQLite{config: config}, nil
}

func (s *SQLite) Connect() error {
	db, err := sql.Open("sqlite3", s.config.Path)
	if err != nil {
		return fmt.Errorf("error connectant a SQLite: %v", err)
	}

	s.db = db
	return nil
}

func (s *SQLite) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *SQLite) Migrate() error {
	// Implementar migracions des de sqlite.sql
	// Pots llegir el fitxer SQL i executar les consultes
	return nil
}

func (s *SQLite) Exec(query string, args ...interface{}) error {
	_, err := s.db.Exec(query, args...)
	return err
}

func (s *SQLite) Query(query string, args ...interface{}) (Rows, error) {
	return s.db.Query(query, args...)
}

func (s *SQLite) QueryRow(query string, args ...interface{}) Row {
	return s.db.QueryRow(query, args...)
}
