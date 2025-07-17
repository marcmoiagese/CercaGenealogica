package db

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/mattn/go-sqlite3"
)

type SQLite struct {
	Path string
	Conn *sql.DB
}

func (d *SQLite) Connect() error {
	conn, err := sql.Open("sqlite3", d.Path)
	if err != nil {
		return fmt.Errorf("error connectant a SQLite: %w", err)
	}
	d.Conn = conn
	log.Println("Conectat a SQLite")
	return nil
}

func (d *SQLite) Close() {
	if d.Conn != nil {
		d.Conn.Close()
	}
}

func (d *SQLite) Query(query string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := d.Conn.Query(query, args...)
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

func (d *SQLite) Exec(query string, args ...interface{}) (int64, error) {
	res, err := d.Conn.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	id, _ := res.LastInsertId()
	return id, nil
}

func (d *SQLite) InsertUser(user *User) error {
	stmt := `INSERT INTO usuaris 
    (usuari, nom, cognoms, correu, contrasenya, data_naixement, pais, estat, provincia, poblacio, codi_postal, data_creacio, actiu) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?)`

	res, err := d.Conn.Exec(stmt,
		user.Usuari,
		user.Name,
		user.Surname,
		user.Email,
		user.Password,
		user.DataNaixament,
		user.Pais,
		user.Estat,
		user.Provincia,
		user.Poblacio,
		user.CodiPostal,
		user.Active,
	)
	if err != nil {
		log.Printf("[SQLite] Error a InsertUser: %v", err)
		return err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	user.ID = int(id)
	return nil
}

func (d *SQLite) GetUserByEmail(email string) (*User, error) {
	row := d.Conn.QueryRow(`
        SELECT id, nom, cognoms, correu, contrasenya, data_naixement, pais, estat, provincia, poblacio, codi_postal, data_creacio, actiu 
        FROM usuaris 
        WHERE correu = ?`, email)

	u := new(User)
	err := row.Scan(
		&u.ID,
		&u.Name,
		&u.Surname,
		&u.Email,
		&u.Password,
		&u.DataNaixament,
		&u.Pais,
		&u.Estat,
		&u.Provincia,
		&u.Poblacio,
		&u.CodiPostal,
		&u.CreatedAt,
		&u.Active,
	)
	if err != nil {
		return nil, err
	}
	return u, nil
}

func (s *SQLite) SaveActivationToken(email, token string) error {
	stmt := `UPDATE usuaris SET actiu = 0, token_activacio = ?, expira_token = datetime('now', '+48 hours') WHERE correu = ?`
	_, err := s.Conn.Exec(stmt, token, email)
	return err
}

func (d *SQLite) ActivateUser(token string) error {
	row := d.Conn.QueryRow("SELECT correu FROM usuaris WHERE token_activacio = ? AND expira_token > datetime('now')", token)
	var email string
	if err := row.Scan(&email); err != nil {
		return err
	}
	_, err := d.Conn.Exec("UPDATE usuaris SET actiu = 1, token_activacio = NULL, expira_token = NULL WHERE correu = ?", email)
	return err
}
