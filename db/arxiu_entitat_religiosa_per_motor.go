package db

import "strings"

func sqliteListArxiuEntitatsReligioses(d *SQLite, arxiuID int, entitatReligiosaID int, status string) ([]ArxiuEntitatReligiosa, error) {
	query := `SELECT id, arxiu_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, COALESCE(observacions, ''), estat, moderation_status, COALESCE(moderation_notes, ''), created_by, updated_by, moderated_by, moderated_at, created_at, updated_at FROM arxiu_entitat_religiosa WHERE 1=1`
	args := []interface{}{}
	if arxiuID > 0 {
		query += ` AND arxiu_id = ?`
		args = append(args, arxiuID)
	}
	if entitatReligiosaID > 0 {
		query += ` AND entitat_religiosa_id = ?`
		args = append(args, entitatReligiosaID)
	}
	if strings.TrimSpace(status) != "" {
		query += ` AND moderation_status = ?`
		args = append(args, strings.TrimSpace(status))
	}
	query += ` ORDER BY arxiu_id, entitat_religiosa_id, any_inici, id`
	rows, err := d.Conn.Query(query, args...)
	if err != nil {
		return nil, arxiuEntitatReligiosaWrap("sqlite", "list_arxiu_entitat_religiosa", 0, err)
	}
	defer rows.Close()
	out := []ArxiuEntitatReligiosa{}
	for rows.Next() {
		var rel ArxiuEntitatReligiosa
		if err := scanArxiuEntitatReligiosa(rows, &rel); err != nil {
			return nil, arxiuEntitatReligiosaWrap("sqlite", "scan_arxiu_entitat_religiosa", 0, err)
		}
		out = append(out, rel)
	}
	if err := rows.Err(); err != nil {
		return nil, arxiuEntitatReligiosaWrap("sqlite", "rows_arxiu_entitat_religiosa", 0, err)
	}
	return out, nil
}

func sqliteGetArxiuEntitatReligiosa(d *SQLite, id int) (*ArxiuEntitatReligiosa, error) {
	row := d.Conn.QueryRow(`SELECT id, arxiu_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, COALESCE(observacions, ''), estat, moderation_status, COALESCE(moderation_notes, ''), created_by, updated_by, moderated_by, moderated_at, created_at, updated_at FROM arxiu_entitat_religiosa WHERE id = ?`, id)
	var rel ArxiuEntitatReligiosa
	if err := scanArxiuEntitatReligiosa(row, &rel); err != nil {
		return nil, arxiuEntitatReligiosaWrap("sqlite", "get_arxiu_entitat_religiosa", id, err)
	}
	return &rel, nil
}

func sqliteSaveArxiuEntitatReligiosa(d *SQLite, rel *ArxiuEntitatReligiosa) (int, error) {
	if rel == nil {
		return 0, nil
	}
	if rel.ID == 0 {
		res, err := d.Conn.Exec(`INSERT INTO arxiu_entitat_religiosa (arxiu_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, observacions, estat, moderation_status, moderation_notes, created_by, updated_by, moderated_by, moderated_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))`, rel.ArxiuID, rel.EntitatReligiosaID, rel.TipusRelacio, rel.AnyInici, rel.AnyFi, rel.Observacions, rel.Estat, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt)
		if err != nil {
			return 0, arxiuEntitatReligiosaWrap("sqlite", "create_arxiu_entitat_religiosa", 0, err)
		}
		if id, err := res.LastInsertId(); err == nil {
			rel.ID = int(id)
		}
		return rel.ID, nil
	}
	if _, err := d.Conn.Exec(`UPDATE arxiu_entitat_religiosa SET arxiu_id=?, entitat_religiosa_id=?, tipus_relacio=?, any_inici=?, any_fi=?, observacions=?, estat=?, moderation_status=?, moderation_notes=?, created_by=?, updated_by=?, moderated_by=?, moderated_at=?, updated_at=datetime('now') WHERE id=?`, rel.ArxiuID, rel.EntitatReligiosaID, rel.TipusRelacio, rel.AnyInici, rel.AnyFi, rel.Observacions, rel.Estat, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt, rel.ID); err != nil {
		return 0, arxiuEntitatReligiosaWrap("sqlite", "update_arxiu_entitat_religiosa", rel.ID, err)
	}
	return rel.ID, nil
}

func sqliteDeleteArxiuEntitatReligiosa(d *SQLite, id int) error {
	if _, err := d.Conn.Exec(`DELETE FROM arxiu_entitat_religiosa WHERE id = ?`, id); err != nil {
		return arxiuEntitatReligiosaWrap("sqlite", "delete_arxiu_entitat_religiosa", id, err)
	}
	return nil
}

func sqliteUpdateArxiuEntitatReligiosaModeracio(d *SQLite, id int, estat, motiu string, moderatorID int) error {
	if _, err := d.Conn.Exec(`UPDATE arxiu_entitat_religiosa SET moderation_status=?, moderation_notes=?, moderated_by=?, moderated_at=datetime('now'), updated_at=datetime('now') WHERE id=?`, estat, motiu, moderatorID, id); err != nil {
		return arxiuEntitatReligiosaWrap("sqlite", "update_arxiu_entitat_religiosa_moderacio", id, err)
	}
	return nil
}

func postgresListArxiuEntitatsReligioses(d *PostgreSQL, arxiuID int, entitatReligiosaID int, status string) ([]ArxiuEntitatReligiosa, error) {
	query := `SELECT id, arxiu_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, COALESCE(observacions, ''), estat, moderation_status, COALESCE(moderation_notes, ''), created_by, updated_by, moderated_by, moderated_at, created_at, updated_at FROM arxiu_entitat_religiosa WHERE 1=1`
	args := []interface{}{}
	next := 1
	if arxiuID > 0 {
		query += ` AND arxiu_id = $1`
		args = append(args, arxiuID)
		next++
	}
	if entitatReligiosaID > 0 {
		if next == 1 {
			query += ` AND entitat_religiosa_id = $1`
		} else {
			query += ` AND entitat_religiosa_id = $2`
		}
		args = append(args, entitatReligiosaID)
		next++
	}
	if strings.TrimSpace(status) != "" {
		switch next {
		case 1:
			query += ` AND moderation_status = $1`
		case 2:
			query += ` AND moderation_status = $2`
		default:
			query += ` AND moderation_status = $3`
		}
		args = append(args, strings.TrimSpace(status))
	}
	query += ` ORDER BY arxiu_id, entitat_religiosa_id, any_inici, id`
	rows, err := d.Conn.Query(query, args...)
	if err != nil {
		return nil, arxiuEntitatReligiosaWrap("postgres", "list_arxiu_entitat_religiosa", 0, err)
	}
	defer rows.Close()
	out := []ArxiuEntitatReligiosa{}
	for rows.Next() {
		var rel ArxiuEntitatReligiosa
		if err := scanArxiuEntitatReligiosa(rows, &rel); err != nil {
			return nil, arxiuEntitatReligiosaWrap("postgres", "scan_arxiu_entitat_religiosa", 0, err)
		}
		out = append(out, rel)
	}
	if err := rows.Err(); err != nil {
		return nil, arxiuEntitatReligiosaWrap("postgres", "rows_arxiu_entitat_religiosa", 0, err)
	}
	return out, nil
}

func postgresGetArxiuEntitatReligiosa(d *PostgreSQL, id int) (*ArxiuEntitatReligiosa, error) {
	row := d.Conn.QueryRow(`SELECT id, arxiu_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, COALESCE(observacions, ''), estat, moderation_status, COALESCE(moderation_notes, ''), created_by, updated_by, moderated_by, moderated_at, created_at, updated_at FROM arxiu_entitat_religiosa WHERE id = $1`, id)
	var rel ArxiuEntitatReligiosa
	if err := scanArxiuEntitatReligiosa(row, &rel); err != nil {
		return nil, arxiuEntitatReligiosaWrap("postgres", "get_arxiu_entitat_religiosa", id, err)
	}
	return &rel, nil
}

func postgresSaveArxiuEntitatReligiosa(d *PostgreSQL, rel *ArxiuEntitatReligiosa) (int, error) {
	if rel == nil {
		return 0, nil
	}
	if rel.ID == 0 {
		if err := d.Conn.QueryRow(`INSERT INTO arxiu_entitat_religiosa (arxiu_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, observacions, estat, moderation_status, moderation_notes, created_by, updated_by, moderated_by, moderated_at, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW(), NOW()) RETURNING id`, rel.ArxiuID, rel.EntitatReligiosaID, rel.TipusRelacio, rel.AnyInici, rel.AnyFi, rel.Observacions, rel.Estat, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt).Scan(&rel.ID); err != nil {
			return 0, arxiuEntitatReligiosaWrap("postgres", "create_arxiu_entitat_religiosa", 0, err)
		}
		return rel.ID, nil
	}
	if _, err := d.Conn.Exec(`UPDATE arxiu_entitat_religiosa SET arxiu_id=$1, entitat_religiosa_id=$2, tipus_relacio=$3, any_inici=$4, any_fi=$5, observacions=$6, estat=$7, moderation_status=$8, moderation_notes=$9, created_by=$10, updated_by=$11, moderated_by=$12, moderated_at=$13, updated_at=NOW() WHERE id=$14`, rel.ArxiuID, rel.EntitatReligiosaID, rel.TipusRelacio, rel.AnyInici, rel.AnyFi, rel.Observacions, rel.Estat, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt, rel.ID); err != nil {
		return 0, arxiuEntitatReligiosaWrap("postgres", "update_arxiu_entitat_religiosa", rel.ID, err)
	}
	return rel.ID, nil
}

func postgresDeleteArxiuEntitatReligiosa(d *PostgreSQL, id int) error {
	if _, err := d.Conn.Exec(`DELETE FROM arxiu_entitat_religiosa WHERE id = $1`, id); err != nil {
		return arxiuEntitatReligiosaWrap("postgres", "delete_arxiu_entitat_religiosa", id, err)
	}
	return nil
}

func postgresUpdateArxiuEntitatReligiosaModeracio(d *PostgreSQL, id int, estat, motiu string, moderatorID int) error {
	if _, err := d.Conn.Exec(`UPDATE arxiu_entitat_religiosa SET moderation_status=$1, moderation_notes=$2, moderated_by=$3, moderated_at=NOW(), updated_at=NOW() WHERE id=$4`, estat, motiu, moderatorID, id); err != nil {
		return arxiuEntitatReligiosaWrap("postgres", "update_arxiu_entitat_religiosa_moderacio", id, err)
	}
	return nil
}

func mysqlListArxiuEntitatsReligioses(d *MySQL, arxiuID int, entitatReligiosaID int, status string) ([]ArxiuEntitatReligiosa, error) {
	query := `SELECT id, arxiu_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, COALESCE(observacions, ''), estat, moderation_status, COALESCE(moderation_notes, ''), created_by, updated_by, moderated_by, moderated_at, created_at, updated_at FROM arxiu_entitat_religiosa WHERE 1=1`
	args := []interface{}{}
	if arxiuID > 0 {
		query += ` AND arxiu_id = ?`
		args = append(args, arxiuID)
	}
	if entitatReligiosaID > 0 {
		query += ` AND entitat_religiosa_id = ?`
		args = append(args, entitatReligiosaID)
	}
	if strings.TrimSpace(status) != "" {
		query += ` AND moderation_status = ?`
		args = append(args, strings.TrimSpace(status))
	}
	query += ` ORDER BY arxiu_id, entitat_religiosa_id, any_inici, id`
	rows, err := d.Conn.Query(query, args...)
	if err != nil {
		return nil, arxiuEntitatReligiosaWrap("mysql", "list_arxiu_entitat_religiosa", 0, err)
	}
	defer rows.Close()
	out := []ArxiuEntitatReligiosa{}
	for rows.Next() {
		var rel ArxiuEntitatReligiosa
		if err := scanArxiuEntitatReligiosa(rows, &rel); err != nil {
			return nil, arxiuEntitatReligiosaWrap("mysql", "scan_arxiu_entitat_religiosa", 0, err)
		}
		out = append(out, rel)
	}
	if err := rows.Err(); err != nil {
		return nil, arxiuEntitatReligiosaWrap("mysql", "rows_arxiu_entitat_religiosa", 0, err)
	}
	return out, nil
}

func mysqlGetArxiuEntitatReligiosa(d *MySQL, id int) (*ArxiuEntitatReligiosa, error) {
	row := d.Conn.QueryRow(`SELECT id, arxiu_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, COALESCE(observacions, ''), estat, moderation_status, COALESCE(moderation_notes, ''), created_by, updated_by, moderated_by, moderated_at, created_at, updated_at FROM arxiu_entitat_religiosa WHERE id = ?`, id)
	var rel ArxiuEntitatReligiosa
	if err := scanArxiuEntitatReligiosa(row, &rel); err != nil {
		return nil, arxiuEntitatReligiosaWrap("mysql", "get_arxiu_entitat_religiosa", id, err)
	}
	return &rel, nil
}

func mysqlSaveArxiuEntitatReligiosa(d *MySQL, rel *ArxiuEntitatReligiosa) (int, error) {
	if rel == nil {
		return 0, nil
	}
	if rel.ID == 0 {
		res, err := d.Conn.Exec(`INSERT INTO arxiu_entitat_religiosa (arxiu_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, observacions, estat, moderation_status, moderation_notes, created_by, updated_by, moderated_by, moderated_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())`, rel.ArxiuID, rel.EntitatReligiosaID, rel.TipusRelacio, rel.AnyInici, rel.AnyFi, rel.Observacions, rel.Estat, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt)
		if err != nil {
			return 0, arxiuEntitatReligiosaWrap("mysql", "create_arxiu_entitat_religiosa", 0, err)
		}
		if id, err := res.LastInsertId(); err == nil {
			rel.ID = int(id)
		}
		return rel.ID, nil
	}
	if _, err := d.Conn.Exec(`UPDATE arxiu_entitat_religiosa SET arxiu_id=?, entitat_religiosa_id=?, tipus_relacio=?, any_inici=?, any_fi=?, observacions=?, estat=?, moderation_status=?, moderation_notes=?, created_by=?, updated_by=?, moderated_by=?, moderated_at=?, updated_at=NOW() WHERE id=?`, rel.ArxiuID, rel.EntitatReligiosaID, rel.TipusRelacio, rel.AnyInici, rel.AnyFi, rel.Observacions, rel.Estat, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt, rel.ID); err != nil {
		return 0, arxiuEntitatReligiosaWrap("mysql", "update_arxiu_entitat_religiosa", rel.ID, err)
	}
	return rel.ID, nil
}

func mysqlDeleteArxiuEntitatReligiosa(d *MySQL, id int) error {
	if _, err := d.Conn.Exec(`DELETE FROM arxiu_entitat_religiosa WHERE id = ?`, id); err != nil {
		return arxiuEntitatReligiosaWrap("mysql", "delete_arxiu_entitat_religiosa", id, err)
	}
	return nil
}

func mysqlUpdateArxiuEntitatReligiosaModeracio(d *MySQL, id int, estat, motiu string, moderatorID int) error {
	if _, err := d.Conn.Exec(`UPDATE arxiu_entitat_religiosa SET moderation_status=?, moderation_notes=?, moderated_by=?, moderated_at=NOW(), updated_at=NOW() WHERE id=?`, estat, motiu, moderatorID, id); err != nil {
		return arxiuEntitatReligiosaWrap("mysql", "update_arxiu_entitat_religiosa_moderacio", id, err)
	}
	return nil
}

type arxiuEntitatReligiosaScanner interface {
	Scan(dest ...interface{}) error
}

func scanArxiuEntitatReligiosa(scanner arxiuEntitatReligiosaScanner, rel *ArxiuEntitatReligiosa) error {
	return scanner.Scan(&rel.ID, &rel.ArxiuID, &rel.EntitatReligiosaID, &rel.TipusRelacio, &rel.AnyInici, &rel.AnyFi, &rel.Observacions, &rel.Estat, &rel.ModeracioEstat, &rel.ModeracioMotiu, &rel.CreatedBy, &rel.UpdatedBy, &rel.ModeratedBy, &rel.ModeratedAt, &rel.CreatedAt, &rel.UpdatedAt)
}

func arxiuEntitatReligiosaWrap(engine string, op string, id int, err error) error {
	if err == nil {
		return nil
	}
	return WrapSQLError(SQLErrorContext{Engine: engine, Component: "arxiu_entitat_religiosa", Op: op, Object: "arxiu_entitat_religiosa", ObjectID: id}, err)
}
