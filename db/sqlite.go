package db

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

type SQLite struct {
	Path string
	Conn *sql.DB
}

func (s *SQLite) Connect() error {
	var err error
	s.Conn, err = sql.Open("sqlite3", s.Path)
	if err != nil {
		return err
	}

	return s.Conn.Ping()
}

func (s *SQLite) Query(query string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := s.Conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Processa resultats
	columns, _ := rows.Columns()
	results := []map[string]interface{}{}

	for rows.Next() {
		scanArgs := make([]interface{}, len(columns))
		values := make([]interface{}, len(columns))

		for i := range values {
			scanArgs[i] = &values[i]
		}

		rows.Scan(scanArgs...)

		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}

		results = append(results, row)
	}

	return results, nil
}

func (s *SQLite) Exec(query string, args ...interface{}) (int64, error) {
	res, err := s.Conn.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *SQLite) Close() {
	s.Conn.Close()
}
