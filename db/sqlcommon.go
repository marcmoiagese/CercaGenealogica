package db

import (
	"database/sql"
	"fmt"
	"strings"
)

// formatPlaceholders converteix '?' a placeholders de l'estil PostgreSQL ($1, $2...) si cal.
func formatPlaceholders(style, query string) string {
	if strings.ToLower(style) != "postgres" {
		return query
	}
	var b strings.Builder
	idx := 1
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			b.WriteString(fmt.Sprintf("$%d", idx))
			idx++
		} else {
			b.WriteByte(query[i])
		}
	}
	return b.String()
}

type sqlHelper struct {
	db     *sql.DB
	style  string
	nowFun string
}

func newSQLHelper(db *sql.DB, style, nowFun string) sqlHelper {
	return sqlHelper{db: db, style: strings.ToLower(style), nowFun: nowFun}
}

func (h sqlHelper) insertUser(user *User) error {
	stmt := fmt.Sprintf(`INSERT INTO usuaris 
    (usuari, nom, cognoms, correu, contrasenya, data_naixement, pais, estat, provincia, poblacio, codi_postal, data_creacio, actiu) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, %s, ?)`, h.nowFun)

	stmt = formatPlaceholders(h.style, stmt)

	res, err := h.db.Exec(stmt,
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
	if err == nil {
		user.ID = int(id)
	}
	return nil
}

func (h sqlHelper) getUserByEmail(email string) (*User, error) {
	query := formatPlaceholders(h.style, `
        SELECT id, nom, cognoms, correu, contrasenya, data_naixement, pais, estat, provincia, poblacio, codi_postal, data_creacio, actiu 
        FROM usuaris 
        WHERE correu = ?`)

	row := h.db.QueryRow(query, email)

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

func (h sqlHelper) saveActivationToken(email, token string) error {
	// Manté una finestra de 48h com en la implementació original
	stmt := formatPlaceholders(h.style, `UPDATE usuaris SET token_activacio = ?, expira_token = datetime('now', '+48 hours') WHERE correu = ?`)
	if h.style == "mysql" || h.style == "postgres" {
		stmt = formatPlaceholders(h.style, `UPDATE usuaris SET token_activacio = ?, expira_token = NOW() + INTERVAL '48 HOURS' WHERE correu = ?`)
		if h.style == "mysql" {
			stmt = formatPlaceholders(h.style, `UPDATE usuaris SET token_activacio = ?, expira_token = DATE_ADD(NOW(), INTERVAL 48 HOUR) WHERE correu = ?`)
		}
	}
	_, err := h.db.Exec(stmt, token, email)
	return err
}

func (h sqlHelper) activateUser(token string) error {
	stmt := formatPlaceholders(h.style, `
        UPDATE usuaris 
        SET actiu = 1, token_activacio = NULL, expira_token = NULL 
        WHERE token_activacio = ? AND (expira_token IS NULL OR expira_token > %s)
    `)
	nowExpr := "datetime('now')"
	if h.style == "mysql" {
		nowExpr = "NOW()"
	} else if h.style == "postgres" {
		nowExpr = "NOW()"
	}
	stmt = fmt.Sprintf(stmt, nowExpr)
	res, err := h.db.Exec(stmt, token)
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return fmt.Errorf("token invàlid o expirat")
	}
	return nil
}

func (h sqlHelper) authenticateUser(usernameOrEmail, password string) (*User, error) {
	query := formatPlaceholders(h.style, `
        SELECT id, usuari, nom, cognoms, correu, contrasenya, actiu 
        FROM usuaris 
        WHERE (usuari = ? OR correu = ?) AND actiu = 1`)

	row := h.db.QueryRow(query, usernameOrEmail, usernameOrEmail)

	u := new(User)
	if err := row.Scan(&u.ID, &u.Usuari, &u.Name, &u.Surname, &u.Email, &u.Password, &u.Active); err != nil {
		return nil, err
	}

	return u, nil
}

func (h sqlHelper) saveSession(sessionID string, userID int, expiry string) error {
	stmt := formatPlaceholders(h.style, `INSERT INTO sessions (token_hash, usuari_id, expira, revocat) VALUES (?, ?, ?, 0)`)
	_, err := h.db.Exec(stmt, sessionID, userID, expiry)
	return err
}

func (h sqlHelper) getSessionUser(sessionID string) (*User, error) {
	query := formatPlaceholders(h.style, `
        SELECT u.id, u.usuari, u.nom, u.cognoms, u.correu, u.contrasenya, u.data_naixement, u.pais, u.estat, u.provincia, u.poblacio, u.codi_postal, u.data_creacio, u.actiu
        FROM usuaris u
        INNER JOIN sessions s ON u.id = s.usuari_id
        WHERE s.token_hash = ? AND s.revocat = 0`)

	row := h.db.QueryRow(query, sessionID)

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

func (h sqlHelper) deleteSession(sessionID string) error {
	stmt := formatPlaceholders(h.style, `UPDATE sessions SET revocat = 1 WHERE token_hash = ?`)
	_, err := h.db.Exec(stmt, sessionID)
	return err
}

func (h sqlHelper) existsUserByUsername(username string) (bool, error) {
	query := formatPlaceholders(h.style, `SELECT 1 FROM usuaris WHERE usuari = ? LIMIT 1`)
	row := h.db.QueryRow(query, username)
	var tmp int
	err := row.Scan(&tmp)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (h sqlHelper) existsUserByEmail(email string) (bool, error) {
	query := formatPlaceholders(h.style, `SELECT 1 FROM usuaris WHERE correu = ? LIMIT 1`)
	row := h.db.QueryRow(query, email)
	var tmp int
	err := row.Scan(&tmp)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
