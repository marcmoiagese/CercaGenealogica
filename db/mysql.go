package db

import (
	"database/sql"
	"fmt"
	"log"

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

func (d *MySQL) Connect() error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", d.User, d.Pass, d.Host, d.Port, d.DBName)
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("error connectant a MySQL: %w", err)
	}
	d.Conn = conn
	log.Println("Conectat a MySQL")
	return nil
}

func (d *MySQL) Close() {
	if d.Conn != nil {
		d.Conn.Close()
	}
}

func (d *MySQL) Exec(query string, args ...interface{}) (int64, error) {
	res, err := d.Conn.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	id, _ := res.LastInsertId()
	return id, nil
}

func (d *MySQL) Query(query string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := d.Conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		columns, _ := rows.Columns()
		scanArgs := make([]interface{}, len(columns))
		values := make([]interface{}, len(columns))

		for i := range values {
			scanArgs[i] = &values[i]
		}

		err := rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}
		results = append(results, row)
	}
	return results, nil
}

func (d *MySQL) InsertUser(user *User) error {
	stmt := `INSERT INTO usuaris 
    (usuari, nom, cognoms, correu, contrasenya, data_naixement, pais, estat, provincia, poblacio, codi_postal, data_creacio, actiu) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), ?)`

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
		return err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	user.ID = int(id)
	return nil
}

func (d *MySQL) GetUserByEmail(email string) (*User, error) {
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
	return u, err
}

func (m *MySQL) SaveActivationToken(email, token string) error {
	stmt := `UPDATE usuaris SET actiu = 0, token_activacio = ?, expira_token = NOW() + INTERVAL 48 HOUR WHERE correu = ?`
	_, err := m.Conn.Exec(stmt, token, email)
	return err
}

func (d *MySQL) ActivateUser(token string) error {
	row := d.Conn.QueryRow("SELECT correu FROM usuaris WHERE token_activacio = ? AND expira_token > NOW()", token)
	var email string
	if err := row.Scan(&email); err != nil {
		return err
	}
	_, err := d.Conn.Exec("UPDATE usuaris SET actiu = 1, token_activacio = NULL, expira_token = NULL WHERE correu = ?", email)
	return err
}
