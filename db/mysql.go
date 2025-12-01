package db

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/crypto/bcrypt"
)

type MySQL struct {
	Host   string
	Port   string
	User   string
	Pass   string
	DBName string
	Conn   *sql.DB
	help   sqlHelper
}

func (d *MySQL) Connect() error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", d.User, d.Pass, d.Host, d.Port, d.DBName)
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("error connectant a MySQL: %w", err)
	}
	d.Conn = conn
	d.help = newSQLHelper(conn, "mysql", "NOW()")
	logInfof("Conectat a MySQL")
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
	return d.help.insertUser(user)
}

func (d *MySQL) GetUserByEmail(email string) (*User, error) {
	return d.help.getUserByEmail(email)
}

func (m *MySQL) SaveActivationToken(email, token string) error {
	return m.help.saveActivationToken(email, token)
}

func (d *MySQL) ExistsUserByUsername(username string) (bool, error) {
	return d.help.existsUserByUsername(username)
}

func (d *MySQL) ExistsUserByEmail(email string) (bool, error) {
	return d.help.existsUserByEmail(email)
}

func (d *MySQL) ActivateUser(token string) error {
	return d.help.activateUser(token)
}

func (d *MySQL) AuthenticateUser(usernameOrEmail, password string) (*User, error) {
	u, err := d.help.authenticateUser(usernameOrEmail, password)
	if err != nil {
		return nil, fmt.Errorf("usuari no trobat o no actiu")
	}
	if err := bcrypt.CompareHashAndPassword(u.Password, []byte(password)); err != nil {
		return nil, fmt.Errorf("contrasenya incorrecta")
	}
	return u, nil
}

// Gestió de sessions - adaptat a MySQL
func (d *MySQL) SaveSession(sessionID string, userID int, expiry string) error {
	if err := d.help.saveSession(sessionID, userID, expiry); err != nil {
		logErrorf("[MySQL] Error guardant sessió: %v", err)
		return err
	}
	return nil
}

func (d *MySQL) GetSessionUser(sessionID string) (*User, error) {
	return d.help.getSessionUser(sessionID)
}

func (d *MySQL) DeleteSession(sessionID string) error {
	return d.help.deleteSession(sessionID)
}
