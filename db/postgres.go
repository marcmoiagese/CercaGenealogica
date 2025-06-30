package db

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type PostgreSQL struct {
	Host   string
	Port   string
	User   string
	Pass   string
	DBName string
	Conn   *sql.DB
}

func (p *PostgreSQL) Connect() error {
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		p.Host, p.Port, p.User, p.Pass, p.DBName)

	var err error
	p.Conn, err = sql.Open("postgres", connStr)
	if err != nil {
		return err
	}

	return p.Conn.Ping()
}

func (p *PostgreSQL) Query(query string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := p.Conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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

func (p *PostgreSQL) Exec(query string, args ...interface{}) (int64, error) {
	res, err := p.Conn.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (p *PostgreSQL) Close() {
	if p.Conn != nil {
		p.Conn.Close()
	}
}
