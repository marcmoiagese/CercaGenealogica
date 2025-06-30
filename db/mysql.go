package db

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type MySQL struct {
	Host   string
	Port   string
	User   string
	Pass   string
	DBName string
	Conn   *sql.DB
}

func (m *MySQL) Connect() error {
	var err error
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		m.User, m.Pass, m.Host, m.Port, m.DBName)

	m.Conn, err = sql.Open("mysql", connStr)
	if err != nil {
		return err
	}

	return m.Conn.Ping()
}

func (m *MySQL) Query(query string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := m.Conn.Query(query, args...)
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

func (m *MySQL) Exec(query string, args ...interface{}) (int64, error) {
	res, err := m.Conn.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (m *MySQL) Close() {
	if m.Conn != nil {
		m.Conn.Close()
	}
}
