package db

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
	"golang.org/x/crypto/bcrypt"
)

type PostgreSQL struct {
	Host   string
	Port   string
	User   string
	Pass   string
	DBName string
	Conn   *sql.DB
}

func (d *PostgreSQL) Connect() error {
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		d.Host, d.Port, d.User, d.Pass, d.DBName)

	conn, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return fmt.Errorf("error connectant a PostgreSQL: %w", err)
	}
	d.Conn = conn
	log.Println("Conectat a PostgreSQL")
	return nil
}

func (d *PostgreSQL) Close() {
	if d.Conn != nil {
		d.Conn.Close()
	}
}

func (d *PostgreSQL) Exec(query string, args ...interface{}) (int64, error) {
	res, err := d.Conn.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	id, _ := res.LastInsertId()
	return id, nil
}

func (d *PostgreSQL) Query(query string, args ...interface{}) ([]map[string]interface{}, error) {
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

		if err := rows.Scan(scanArgs...); err != nil {
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

func (d *PostgreSQL) InsertUser(user *User) error {
	stmt := `INSERT INTO usuaris 
    (usuari, nom, cognoms, correu, contrasenya, data_naixement, pais, estat, provincia, poblacio, codi_postal, data_creacio, actiu) 
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW(), $12)`

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

func (d *PostgreSQL) GetUserByEmail(email string) (*User, error) {
	row := d.Conn.QueryRow(`
        SELECT id, nom, cognoms, correu, contrasenya, data_naixement, pais, estat, provincia, poblacio, codi_postal, data_creacio, actiu 
        FROM usuaris 
        WHERE correu = $1`, email)

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

func (p *PostgreSQL) SaveActivationToken(email, token string) error {
	stmt := `UPDATE usuaris SET actiu = false, token_activacio = $1, expira_token = NOW() + INTERVAL '48 hour' WHERE correu = $2`
	_, err := p.Conn.Exec(stmt, token, email)
	return err
}

func (d *PostgreSQL) ActivateUser(token string) error {
	row := d.Conn.QueryRow("SELECT correu FROM usuaris WHERE token_activacio = $1 AND expira_token > NOW()", token)
	var email string
	if err := row.Scan(&email); err != nil {
		return err
	}
	_, err := d.Conn.Exec("UPDATE usuaris SET actiu = true, token_activacio = NULL, expira_token = NULL WHERE correu = $1", email)
	return err
}

func (d *PostgreSQL) AuthenticateUser(usernameOrEmail, password string) (*User, error) {
	// Buscar usuari per nom d'usuari o correu electrònic
	row := d.Conn.QueryRow(`
        SELECT id, usuari, nom, cognoms, correu, contrasenya, data_naixement, pais, estat, provincia, poblacio, codi_postal, data_creacio, actiu 
        FROM usuaris 
        WHERE (usuari = $1 OR correu = $2) AND actiu = true`, usernameOrEmail, usernameOrEmail)

	u := new(User)
	err := row.Scan(
		&u.ID,
		&u.Usuari,
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
		return nil, fmt.Errorf("usuari no trobat o no actiu")
	}

	// Verificar contrasenya (assumim que està hashejada amb bcrypt)
	if err := bcrypt.CompareHashAndPassword(u.Password, []byte(password)); err != nil {
		return nil, fmt.Errorf("contrasenya incorrecta")
	}

	return u, nil
}

// Gestió de sessions - adaptat a PostgreSQL
func (d *PostgreSQL) SaveSession(sessionID string, userID int, expiry string) error {
	stmt := `INSERT INTO sessions (usuari_id, token_hash, creat, revocat) VALUES ($1, $2, NOW(), 0) ON CONFLICT (token_hash) DO UPDATE SET revocat = 0, creat = NOW()`
	_, err := d.Conn.Exec(stmt, userID, sessionID)
	if err != nil {
		log.Printf("[PostgreSQL] Error guardant sessió: %v", err)
	}
	return err
}

func (d *PostgreSQL) GetSessionUser(sessionID string) (*User, error) {
	row := d.Conn.QueryRow(`
        SELECT u.id, u.usuari, u.nom, u.cognoms, u.correu, u.contrasenya, u.data_naixement, u.pais, u.estat, u.provincia, u.poblacio, u.codi_postal, u.data_creacio, u.actiu
        FROM usuaris u
        INNER JOIN sessions s ON u.id = s.usuari_id
        WHERE s.token_hash = $1 AND s.revocat = 0`, sessionID)

	u := new(User)
	err := row.Scan(
		&u.ID,
		&u.Usuari,
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

func (d *PostgreSQL) DeleteSession(sessionID string) error {
	_, err := d.Conn.Exec("UPDATE sessions SET revocat = 1 WHERE token_hash = $1", sessionID)
	return err
}
