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

func (h sqlHelper) columnExists(table, column string) bool {
	var query string
	switch h.style {
	case "mysql":
		query = `SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?`
	case "postgres":
		query = `SELECT 1 FROM information_schema.columns WHERE table_name = $1 AND column_name = $2`
	default: // sqlite
		query = fmt.Sprintf(`SELECT 1 FROM pragma_table_info('%s') WHERE name = ?`, table)
	}
	row := h.db.QueryRow(query, table, column)
	var tmp int
	if err := row.Scan(&tmp); err != nil {
		return false
	}
	return true
}

func (h sqlHelper) ensureUserExtraColumns() {
	stmts := []string{}
	switch h.style {
	case "mysql":
		if !h.columnExists("usuaris", "address") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN address TEXT")
		}
		if !h.columnExists("usuaris", "employment_status") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN employment_status VARCHAR(50)")
		}
		if !h.columnExists("usuaris", "profession") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN profession VARCHAR(255)")
		}
		if !h.columnExists("usuaris", "phone") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN phone VARCHAR(50)")
		}
		if !h.columnExists("usuaris", "preferred_lang") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN preferred_lang VARCHAR(10)")
		}
		if !h.columnExists("usuaris", "spoken_langs") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN spoken_langs TEXT")
		}
	case "postgres":
		if !h.columnExists("usuaris", "address") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN IF NOT EXISTS address TEXT")
		}
		if !h.columnExists("usuaris", "employment_status") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN IF NOT EXISTS employment_status TEXT")
		}
		if !h.columnExists("usuaris", "profession") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN IF NOT EXISTS profession TEXT")
		}
		if !h.columnExists("usuaris", "phone") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN IF NOT EXISTS phone TEXT")
		}
		if !h.columnExists("usuaris", "preferred_lang") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN IF NOT EXISTS preferred_lang TEXT")
		}
		if !h.columnExists("usuaris", "spoken_langs") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN IF NOT EXISTS spoken_langs TEXT")
		}
	default: // sqlite
		if !h.columnExists("usuaris", "address") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN address TEXT")
		}
		if !h.columnExists("usuaris", "employment_status") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN employment_status TEXT")
		}
		if !h.columnExists("usuaris", "profession") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN profession TEXT")
		}
		if !h.columnExists("usuaris", "phone") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN phone TEXT")
		}
		if !h.columnExists("usuaris", "preferred_lang") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN preferred_lang TEXT")
		}
		if !h.columnExists("usuaris", "spoken_langs") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN spoken_langs TEXT")
		}
	}
	for _, stmt := range stmts {
		_, _ = h.db.Exec(stmt)
	}
}

func (h sqlHelper) ensurePrivacyExtraColumns() {
	stmts := []string{}
	switch h.style {
	case "mysql":
		if !h.columnExists("user_privacy", "address_visibility") {
			stmts = append(stmts, "ALTER TABLE user_privacy ADD COLUMN address_visibility VARCHAR(10) DEFAULT 'private'")
		}
		if !h.columnExists("user_privacy", "employment_visibility") {
			stmts = append(stmts, "ALTER TABLE user_privacy ADD COLUMN employment_visibility VARCHAR(10) DEFAULT 'private'")
		}
		if !h.columnExists("user_privacy", "profession_visibility") {
			stmts = append(stmts, "ALTER TABLE user_privacy ADD COLUMN profession_visibility VARCHAR(10) DEFAULT 'private'")
		}
		if !h.columnExists("user_privacy", "phone_visibility") {
			stmts = append(stmts, "ALTER TABLE user_privacy ADD COLUMN phone_visibility VARCHAR(10) DEFAULT 'private'")
		}
		if !h.columnExists("user_privacy", "preferred_lang_visibility") {
			stmts = append(stmts, "ALTER TABLE user_privacy ADD COLUMN preferred_lang_visibility VARCHAR(10) DEFAULT 'private'")
		}
		if !h.columnExists("user_privacy", "spoken_langs_visibility") {
			stmts = append(stmts, "ALTER TABLE user_privacy ADD COLUMN spoken_langs_visibility VARCHAR(10) DEFAULT 'private'")
		}
	case "postgres":
		if !h.columnExists("user_privacy", "address_visibility") {
			stmts = append(stmts, "ALTER TABLE user_privacy ADD COLUMN IF NOT EXISTS address_visibility TEXT DEFAULT 'private'")
		}
		if !h.columnExists("user_privacy", "employment_visibility") {
			stmts = append(stmts, "ALTER TABLE user_privacy ADD COLUMN IF NOT EXISTS employment_visibility TEXT DEFAULT 'private'")
		}
		if !h.columnExists("user_privacy", "profession_visibility") {
			stmts = append(stmts, "ALTER TABLE user_privacy ADD COLUMN IF NOT EXISTS profession_visibility TEXT DEFAULT 'private'")
		}
		if !h.columnExists("user_privacy", "phone_visibility") {
			stmts = append(stmts, "ALTER TABLE user_privacy ADD COLUMN IF NOT EXISTS phone_visibility TEXT DEFAULT 'private'")
		}
		if !h.columnExists("user_privacy", "preferred_lang_visibility") {
			stmts = append(stmts, "ALTER TABLE user_privacy ADD COLUMN IF NOT EXISTS preferred_lang_visibility TEXT DEFAULT 'private'")
		}
		if !h.columnExists("user_privacy", "spoken_langs_visibility") {
			stmts = append(stmts, "ALTER TABLE user_privacy ADD COLUMN IF NOT EXISTS spoken_langs_visibility TEXT DEFAULT 'private'")
		}
	default: // sqlite
		if !h.columnExists("user_privacy", "address_visibility") {
			stmts = append(stmts, "ALTER TABLE user_privacy ADD COLUMN address_visibility TEXT DEFAULT 'private'")
		}
		if !h.columnExists("user_privacy", "employment_visibility") {
			stmts = append(stmts, "ALTER TABLE user_privacy ADD COLUMN employment_visibility TEXT DEFAULT 'private'")
		}
		if !h.columnExists("user_privacy", "profession_visibility") {
			stmts = append(stmts, "ALTER TABLE user_privacy ADD COLUMN profession_visibility TEXT DEFAULT 'private'")
		}
		if !h.columnExists("user_privacy", "phone_visibility") {
			stmts = append(stmts, "ALTER TABLE user_privacy ADD COLUMN phone_visibility TEXT DEFAULT 'private'")
		}
		if !h.columnExists("user_privacy", "preferred_lang_visibility") {
			stmts = append(stmts, "ALTER TABLE user_privacy ADD COLUMN preferred_lang_visibility TEXT DEFAULT 'private'")
		}
		if !h.columnExists("user_privacy", "spoken_langs_visibility") {
			stmts = append(stmts, "ALTER TABLE user_privacy ADD COLUMN spoken_langs_visibility TEXT DEFAULT 'private'")
		}
	}
	for _, stmt := range stmts {
		_, _ = h.db.Exec(stmt)
	}
}

func (h sqlHelper) insertUser(user *User) error {
	h.ensureUserExtraColumns()
	stmt := fmt.Sprintf(`INSERT INTO usuaris 
    (usuari, nom, cognoms, correu, contrasenya, data_naixement, pais, estat, provincia, poblacio, codi_postal, address, employment_status, profession, phone, preferred_lang, spoken_langs, data_creacio, actiu) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, %s, ?)`, h.nowFun)

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
		user.Address,
		user.Employment,
		user.Profession,
		user.Phone,
		user.PreferredLang,
		user.SpokenLangs,
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
	h.ensureUserExtraColumns()
	query := formatPlaceholders(h.style, `
        SELECT id, nom, cognoms, correu, contrasenya, data_naixement, pais, estat, provincia, poblacio, codi_postal, address, employment_status, profession, phone, preferred_lang, spoken_langs, data_creacio, actiu 
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
		&u.Address,
		&u.Employment,
		&u.Profession,
		&u.Phone,
		&u.PreferredLang,
		&u.SpokenLangs,
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
	h.ensureUserExtraColumns()
	query := formatPlaceholders(h.style, `
        SELECT id, usuari, nom, cognoms, correu, contrasenya, data_naixement, pais, estat, provincia, poblacio, codi_postal, address, employment_status, profession, phone, preferred_lang, spoken_langs, actiu 
        FROM usuaris 
        WHERE (usuari = ? OR correu = ?) AND actiu = 1`)

	row := h.db.QueryRow(query, usernameOrEmail, usernameOrEmail)

	u := new(User)
	if err := row.Scan(&u.ID, &u.Usuari, &u.Name, &u.Surname, &u.Email, &u.Password,
		&u.DataNaixament, &u.Pais, &u.Estat, &u.Provincia, &u.Poblacio, &u.CodiPostal,
		&u.Address, &u.Employment, &u.Profession, &u.Phone, &u.PreferredLang, &u.SpokenLangs, &u.Active); err != nil {
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
	h.ensureUserExtraColumns()
	query := formatPlaceholders(h.style, `
        SELECT u.id, u.usuari, u.nom, u.cognoms, u.correu, u.contrasenya, u.data_naixement, u.pais, u.estat, u.provincia, u.poblacio, u.codi_postal, u.address, u.employment_status, u.profession, u.phone, u.preferred_lang, u.spoken_langs, u.data_creacio, u.actiu
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
		&u.Address,
		&u.Employment,
		&u.Profession,
		&u.Phone,
		&u.PreferredLang,
		&u.SpokenLangs,
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

func (h sqlHelper) createPasswordReset(email, token, expiry, lang string) (bool, error) {
	// Comprova si l'usuari existeix
	var userID int
	q := formatPlaceholders(h.style, `SELECT id FROM usuaris WHERE correu = ?`)
	err := h.db.QueryRow(q, email).Scan(&userID)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	stmt := formatPlaceholders(h.style, `
        INSERT INTO password_resets (usuari_id, token, expira, lang, used)
        VALUES (?, ?, ?, ?, 0)`)
	_, err = h.db.Exec(stmt, userID, token, expiry, lang)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (h sqlHelper) getPasswordReset(token string) (*PasswordReset, error) {
	nowExpr := "datetime('now')"
	if h.style == "mysql" || h.style == "postgres" {
		nowExpr = "NOW()"
	}

	stmt := formatPlaceholders(h.style, `
        SELECT pr.id, pr.usuari_id, pr.lang, u.correu
        FROM password_resets pr
        INNER JOIN usuaris u ON u.id = pr.usuari_id
        WHERE pr.token = ? AND pr.used = 0 AND pr.expira > `+nowExpr+``)

	row := h.db.QueryRow(stmt, token)
	var pr PasswordReset
	err := row.Scan(&pr.ID, &pr.UserID, &pr.Lang, &pr.Email)
	if err != nil {
		return nil, err
	}
	return &pr, nil
}

func (h sqlHelper) markPasswordResetUsed(id int) error {
	stmt := formatPlaceholders(h.style, `UPDATE password_resets SET used = 1 WHERE id = ?`)
	_, err := h.db.Exec(stmt, id)
	return err
}

func (h sqlHelper) updateUserPassword(userID int, passwordHash []byte) error {
	stmt := formatPlaceholders(h.style, `UPDATE usuaris SET contrasenya = ? WHERE id = ?`)
	_, err := h.db.Exec(stmt, passwordHash, userID)
	return err
}

func (h sqlHelper) updateUserProfile(u *User) error {
	h.ensureUserExtraColumns()
	stmt := formatPlaceholders(h.style, `
        UPDATE usuaris
        SET nom = ?, cognoms = ?, correu = ?, data_naixement = ?, pais = ?, estat = ?, provincia = ?, poblacio = ?, codi_postal = ?, address = ?, employment_status = ?, profession = ?, phone = ?, preferred_lang = ?, spoken_langs = ?
        WHERE id = ?`)
	_, err := h.db.Exec(stmt,
		u.Name,
		u.Surname,
		u.Email,
		u.DataNaixament,
		u.Pais,
		u.Estat,
		u.Provincia,
		u.Poblacio,
		u.CodiPostal,
		u.Address,
		u.Employment,
		u.Profession,
		u.Phone,
		u.PreferredLang,
		u.SpokenLangs,
		u.ID,
	)
	return err
}

func (h sqlHelper) updateUserEmail(userID int, newEmail string) error {
	stmt := formatPlaceholders(h.style, `UPDATE usuaris SET correu = ? WHERE id = ?`)
	_, err := h.db.Exec(stmt, newEmail, userID)
	return err
}

func (h sqlHelper) savePrivacySettings(userID int, p *PrivacySettings) error {
	h.ensurePrivacyExtraColumns()
	stmt := formatPlaceholders(h.style, `
        INSERT INTO user_privacy (
            usuari_id, nom_visibility, cognoms_visibility, email_visibility, birth_visibility,
            pais_visibility, estat_visibility, provincia_visibility, poblacio_visibility, postal_visibility,
            address_visibility, employment_visibility, profession_visibility, phone_visibility, preferred_lang_visibility, spoken_langs_visibility,
            show_activity, profile_public, notify_email, allow_contact
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(usuari_id) DO UPDATE SET
            nom_visibility=excluded.nom_visibility,
            cognoms_visibility=excluded.cognoms_visibility,
            email_visibility=excluded.email_visibility,
            birth_visibility=excluded.birth_visibility,
            pais_visibility=excluded.pais_visibility,
            estat_visibility=excluded.estat_visibility,
            provincia_visibility=excluded.provincia_visibility,
            poblacio_visibility=excluded.poblacio_visibility,
            postal_visibility=excluded.postal_visibility,
            address_visibility=excluded.address_visibility,
            employment_visibility=excluded.employment_visibility,
            profession_visibility=excluded.profession_visibility,
            phone_visibility=excluded.phone_visibility,
            preferred_lang_visibility=excluded.preferred_lang_visibility,
            spoken_langs_visibility=excluded.spoken_langs_visibility,
            show_activity=excluded.show_activity,
            profile_public=excluded.profile_public,
            notify_email=excluded.notify_email,
            allow_contact=excluded.allow_contact
    `)
	if h.style == "mysql" {
		stmt = `
        INSERT INTO user_privacy (
            usuari_id, nom_visibility, cognoms_visibility, email_visibility, birth_visibility,
            pais_visibility, estat_visibility, provincia_visibility, poblacio_visibility, postal_visibility,
            address_visibility, employment_visibility, profession_visibility, phone_visibility, preferred_lang_visibility, spoken_langs_visibility,
            show_activity, profile_public, notify_email, allow_contact
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            nom_visibility=VALUES(nom_visibility),
            cognoms_visibility=VALUES(cognoms_visibility),
            email_visibility=VALUES(email_visibility),
            birth_visibility=VALUES(birth_visibility),
            pais_visibility=VALUES(pais_visibility),
            estat_visibility=VALUES(estat_visibility),
            provincia_visibility=VALUES(provincia_visibility),
            poblacio_visibility=VALUES(poblacio_visibility),
            postal_visibility=VALUES(postal_visibility),
            address_visibility=VALUES(address_visibility),
            employment_visibility=VALUES(employment_visibility),
            profession_visibility=VALUES(profession_visibility),
            phone_visibility=VALUES(phone_visibility),
            preferred_lang_visibility=VALUES(preferred_lang_visibility),
            spoken_langs_visibility=VALUES(spoken_langs_visibility),
            show_activity=VALUES(show_activity),
            profile_public=VALUES(profile_public),
            notify_email=VALUES(notify_email),
            allow_contact=VALUES(allow_contact)
        `
	}
	_, err := h.db.Exec(stmt,
		userID,
		p.NomVisibility,
		p.CognomsVisibility,
		p.EmailVisibility,
		p.BirthVisibility,
		p.PaisVisibility,
		p.EstatVisibility,
		p.ProvinciaVisibility,
		p.PoblacioVisibility,
		p.PostalVisibility,
		p.AddressVisibility,
		p.EmploymentVisibility,
		p.ProfessionVisibility,
		p.PhoneVisibility,
		p.PreferredLangVisibility,
		p.SpokenLangsVisibility,
		p.ShowActivity,
		p.ProfilePublic,
		p.NotifyEmail,
		p.AllowContact,
	)
	return err
}

func (h sqlHelper) createEmailChange(userID int, newEmail, tokenConfirm, expConfirm, tokenRevert, expRevert, lang string) error {
	stmt := formatPlaceholders(h.style, `
        INSERT INTO email_changes (
            usuari_id, old_email, new_email, token_confirm, exp_confirm, token_revert, exp_revert, lang, confirmed, reverted
        )
        SELECT id, correu, ?, ?, ?, ?, ?, ?, 0, 0 FROM usuaris WHERE id = ?`)
	_, err := h.db.Exec(stmt, newEmail, tokenConfirm, expConfirm, tokenRevert, expRevert, lang, userID)
	return err
}

func (h sqlHelper) confirmEmailChange(token string) (*EmailChange, error) {
	nowExpr := "datetime('now')"
	if h.style == "mysql" || h.style == "postgres" {
		nowExpr = "NOW()"
	}
	stmt := formatPlaceholders(h.style, `
        SELECT id, usuari_id, old_email, new_email, token_confirm, exp_confirm, token_revert, exp_revert, lang, confirmed, reverted
        FROM email_changes
        WHERE token_confirm = ? AND confirmed = 0 AND exp_confirm > `+nowExpr+``)
	row := h.db.QueryRow(stmt, token)
	var c EmailChange
	if err := row.Scan(&c.ID, &c.UserID, &c.OldEmail, &c.NewEmail, &c.TokenConfirm, &c.ExpConfirm, &c.TokenRevert, &c.ExpRevert, &c.Lang, &c.Confirmed, &c.Reverted); err != nil {
		return nil, err
	}
	return &c, nil
}

func (h sqlHelper) revertEmailChange(token string) (*EmailChange, error) {
	nowExpr := "datetime('now')"
	if h.style == "mysql" || h.style == "postgres" {
		nowExpr = "NOW()"
	}
	stmt := formatPlaceholders(h.style, `
        SELECT id, usuari_id, old_email, new_email, token_confirm, exp_confirm, token_revert, exp_revert, lang, confirmed, reverted
        FROM email_changes
        WHERE token_revert = ? AND reverted = 0 AND exp_revert > `+nowExpr+``)
	row := h.db.QueryRow(stmt, token)
	var c EmailChange
	if err := row.Scan(&c.ID, &c.UserID, &c.OldEmail, &c.NewEmail, &c.TokenConfirm, &c.ExpConfirm, &c.TokenRevert, &c.ExpRevert, &c.Lang, &c.Confirmed, &c.Reverted); err != nil {
		return nil, err
	}
	return &c, nil
}

func (h sqlHelper) markEmailChangeConfirmed(id int) error {
	stmt := formatPlaceholders(h.style, `UPDATE email_changes SET confirmed = 1 WHERE id = ?`)
	_, err := h.db.Exec(stmt, id)
	return err
}

func (h sqlHelper) markEmailChangeReverted(id int) error {
	stmt := formatPlaceholders(h.style, `UPDATE email_changes SET reverted = 1 WHERE id = ?`)
	_, err := h.db.Exec(stmt, id)
	return err
}

func (h sqlHelper) createPrivacyDefaults(userID int) error {
	h.ensurePrivacyExtraColumns()
	stmt := formatPlaceholders(h.style, `
        INSERT INTO user_privacy (
            usuari_id, nom_visibility, cognoms_visibility, email_visibility, birth_visibility,
            pais_visibility, estat_visibility, provincia_visibility, poblacio_visibility, postal_visibility,
            address_visibility, employment_visibility, profession_visibility, phone_visibility, preferred_lang_visibility, spoken_langs_visibility,
            show_activity, profile_public, notify_email, allow_contact
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, 1, 1)
        ON CONFLICT (usuari_id) DO NOTHING
    `)
	if h.style == "mysql" {
		stmt = `
        INSERT IGNORE INTO user_privacy (
            usuari_id, nom_visibility, cognoms_visibility, email_visibility, birth_visibility,
            pais_visibility, estat_visibility, provincia_visibility, poblacio_visibility, postal_visibility,
            address_visibility, employment_visibility, profession_visibility, phone_visibility, preferred_lang_visibility, spoken_langs_visibility,
            show_activity, profile_public, notify_email, allow_contact
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, 1, 1)
        `
	}
	_, err := h.db.Exec(stmt,
		userID,
		"private", // nom
		"private", // cognoms
		"private", // email
		"private", // birth
		"public",  // país
		"private", // estat
		"private", // provincia
		"private", // poblacio
		"private", // postal
		"private", // address
		"private", // employment
		"private", // profession
		"private", // phone
		"private", // preferred lang
		"private", // spoken langs
	)
	return err
}

func (h sqlHelper) getPrivacySettings(userID int) (*PrivacySettings, error) {
	h.ensurePrivacyExtraColumns()
	stmt := formatPlaceholders(h.style, `
        SELECT usuari_id, nom_visibility, cognoms_visibility, email_visibility, birth_visibility,
               pais_visibility, estat_visibility, provincia_visibility, poblacio_visibility, postal_visibility,
               address_visibility, employment_visibility, profession_visibility, phone_visibility, preferred_lang_visibility, spoken_langs_visibility,
               show_activity, profile_public, notify_email, allow_contact
        FROM user_privacy
        WHERE usuari_id = ?
    `)
	row := h.db.QueryRow(stmt, userID)
	var p PrivacySettings
	err := row.Scan(
		&p.UserID,
		&p.NomVisibility,
		&p.CognomsVisibility,
		&p.EmailVisibility,
		&p.BirthVisibility,
		&p.PaisVisibility,
		&p.EstatVisibility,
		&p.ProvinciaVisibility,
		&p.PoblacioVisibility,
		&p.PostalVisibility,
		&p.AddressVisibility,
		&p.EmploymentVisibility,
		&p.ProfessionVisibility,
		&p.PhoneVisibility,
		&p.PreferredLangVisibility,
		&p.SpokenLangsVisibility,
		&p.ShowActivity,
		&p.ProfilePublic,
		&p.NotifyEmail,
		&p.AllowContact,
	)
	if err != nil {
		return nil, err
	}
	return &p, nil
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
