package db

import "strings"

func sqliteListArxiuAbasts(d *SQLite, arxiuID int, targetKind string, status string) ([]ArxiuAbast, error) {
	query := `SELECT id, arxiu_id, target_kind, target_id, COALESCE(target_code, ''), COALESCE(target_label, ''), relation_kind, COALESCE(notes, ''), estat, moderation_status, COALESCE(moderation_notes, ''), created_by, updated_by, moderated_by, moderated_at, created_at, updated_at FROM arxiu_abast WHERE 1=1`
	args := []interface{}{}
	if arxiuID > 0 {
		query += ` AND arxiu_id = ?`
		args = append(args, arxiuID)
	}
	if strings.TrimSpace(targetKind) != "" {
		query += ` AND target_kind = ?`
		args = append(args, strings.TrimSpace(targetKind))
	}
	if strings.TrimSpace(status) != "" {
		query += ` AND moderation_status = ?`
		args = append(args, strings.TrimSpace(status))
	}
	query += ` ORDER BY arxiu_id, target_kind, target_label, id`
	rows, err := d.Conn.Query(query, args...)
	if err != nil {
		return nil, arxiuAbastWrap("sqlite", "list_arxiu_abast", 0, err)
	}
	defer rows.Close()
	out := []ArxiuAbast{}
	for rows.Next() {
		var rel ArxiuAbast
		if err := scanArxiuAbast(rows, &rel); err != nil {
			return nil, arxiuAbastWrap("sqlite", "scan_arxiu_abast", 0, err)
		}
		out = append(out, rel)
	}
	if err := rows.Err(); err != nil {
		return nil, arxiuAbastWrap("sqlite", "rows_arxiu_abast", 0, err)
	}
	return out, nil
}

func sqliteGetArxiuAbast(d *SQLite, id int) (*ArxiuAbast, error) {
	row := d.Conn.QueryRow(`SELECT id, arxiu_id, target_kind, target_id, COALESCE(target_code, ''), COALESCE(target_label, ''), relation_kind, COALESCE(notes, ''), estat, moderation_status, COALESCE(moderation_notes, ''), created_by, updated_by, moderated_by, moderated_at, created_at, updated_at FROM arxiu_abast WHERE id = ?`, id)
	var rel ArxiuAbast
	if err := scanArxiuAbast(row, &rel); err != nil {
		return nil, arxiuAbastWrap("sqlite", "get_arxiu_abast", id, err)
	}
	return &rel, nil
}

func sqliteSaveArxiuAbast(d *SQLite, rel *ArxiuAbast) (int, error) {
	if rel == nil {
		return 0, nil
	}
	if rel.ID == 0 {
		res, err := d.Conn.Exec(`INSERT INTO arxiu_abast (arxiu_id, target_kind, target_id, target_code, target_label, relation_kind, notes, estat, moderation_status, moderation_notes, created_by, updated_by, moderated_by, moderated_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))`, rel.ArxiuID, rel.TargetKind, rel.TargetID, rel.TargetCode, rel.TargetLabel, rel.RelationKind, rel.Notes, rel.Estat, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt)
		if err != nil {
			return 0, arxiuAbastWrap("sqlite", "create_arxiu_abast", 0, err)
		}
		if id, err := res.LastInsertId(); err == nil {
			rel.ID = int(id)
		}
		return rel.ID, nil
	}
	if _, err := d.Conn.Exec(`UPDATE arxiu_abast SET arxiu_id=?, target_kind=?, target_id=?, target_code=?, target_label=?, relation_kind=?, notes=?, estat=?, moderation_status=?, moderation_notes=?, created_by=?, updated_by=?, moderated_by=?, moderated_at=?, updated_at=datetime('now') WHERE id=?`, rel.ArxiuID, rel.TargetKind, rel.TargetID, rel.TargetCode, rel.TargetLabel, rel.RelationKind, rel.Notes, rel.Estat, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt, rel.ID); err != nil {
		return 0, arxiuAbastWrap("sqlite", "update_arxiu_abast", rel.ID, err)
	}
	return rel.ID, nil
}

func sqliteDeleteArxiuAbast(d *SQLite, id int) error {
	if _, err := d.Conn.Exec(`DELETE FROM arxiu_abast WHERE id = ?`, id); err != nil {
		return arxiuAbastWrap("sqlite", "delete_arxiu_abast", id, err)
	}
	return nil
}

func sqliteUpdateArxiuAbastModeracio(d *SQLite, id int, estat, motiu string, moderatorID int) error {
	if _, err := d.Conn.Exec(`UPDATE arxiu_abast SET moderation_status=?, moderation_notes=?, moderated_by=?, moderated_at=datetime('now'), updated_at=datetime('now') WHERE id=?`, estat, motiu, moderatorID, id); err != nil {
		return arxiuAbastWrap("sqlite", "update_arxiu_abast_moderacio", id, err)
	}
	return nil
}

func postgresListArxiuAbasts(d *PostgreSQL, arxiuID int, targetKind string, status string) ([]ArxiuAbast, error) {
	query := `SELECT id, arxiu_id, target_kind, target_id, COALESCE(target_code, ''), COALESCE(target_label, ''), relation_kind, COALESCE(notes, ''), estat, moderation_status, COALESCE(moderation_notes, ''), created_by, updated_by, moderated_by, moderated_at, created_at, updated_at FROM arxiu_abast WHERE 1=1`
	args := []interface{}{}
	next := 1
	if arxiuID > 0 {
		query += ` AND arxiu_id = $1`
		args = append(args, arxiuID)
		next++
	}
	if strings.TrimSpace(targetKind) != "" {
		if next == 1 {
			query += ` AND target_kind = $1`
		} else {
			query += ` AND target_kind = $2`
		}
		args = append(args, strings.TrimSpace(targetKind))
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
	query += ` ORDER BY arxiu_id, target_kind, target_label, id`
	rows, err := d.Conn.Query(query, args...)
	if err != nil {
		return nil, arxiuAbastWrap("postgres", "list_arxiu_abast", 0, err)
	}
	defer rows.Close()
	out := []ArxiuAbast{}
	for rows.Next() {
		var rel ArxiuAbast
		if err := scanArxiuAbast(rows, &rel); err != nil {
			return nil, arxiuAbastWrap("postgres", "scan_arxiu_abast", 0, err)
		}
		out = append(out, rel)
	}
	if err := rows.Err(); err != nil {
		return nil, arxiuAbastWrap("postgres", "rows_arxiu_abast", 0, err)
	}
	return out, nil
}

func postgresGetArxiuAbast(d *PostgreSQL, id int) (*ArxiuAbast, error) {
	row := d.Conn.QueryRow(`SELECT id, arxiu_id, target_kind, target_id, COALESCE(target_code, ''), COALESCE(target_label, ''), relation_kind, COALESCE(notes, ''), estat, moderation_status, COALESCE(moderation_notes, ''), created_by, updated_by, moderated_by, moderated_at, created_at, updated_at FROM arxiu_abast WHERE id = $1`, id)
	var rel ArxiuAbast
	if err := scanArxiuAbast(row, &rel); err != nil {
		return nil, arxiuAbastWrap("postgres", "get_arxiu_abast", id, err)
	}
	return &rel, nil
}

func postgresSaveArxiuAbast(d *PostgreSQL, rel *ArxiuAbast) (int, error) {
	if rel == nil {
		return 0, nil
	}
	if rel.ID == 0 {
		if err := d.Conn.QueryRow(`INSERT INTO arxiu_abast (arxiu_id, target_kind, target_id, target_code, target_label, relation_kind, notes, estat, moderation_status, moderation_notes, created_by, updated_by, moderated_by, moderated_at, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW(), NOW()) RETURNING id`, rel.ArxiuID, rel.TargetKind, rel.TargetID, rel.TargetCode, rel.TargetLabel, rel.RelationKind, rel.Notes, rel.Estat, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt).Scan(&rel.ID); err != nil {
			return 0, arxiuAbastWrap("postgres", "create_arxiu_abast", 0, err)
		}
		return rel.ID, nil
	}
	if _, err := d.Conn.Exec(`UPDATE arxiu_abast SET arxiu_id=$1, target_kind=$2, target_id=$3, target_code=$4, target_label=$5, relation_kind=$6, notes=$7, estat=$8, moderation_status=$9, moderation_notes=$10, created_by=$11, updated_by=$12, moderated_by=$13, moderated_at=$14, updated_at=NOW() WHERE id=$15`, rel.ArxiuID, rel.TargetKind, rel.TargetID, rel.TargetCode, rel.TargetLabel, rel.RelationKind, rel.Notes, rel.Estat, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt, rel.ID); err != nil {
		return 0, arxiuAbastWrap("postgres", "update_arxiu_abast", rel.ID, err)
	}
	return rel.ID, nil
}

func postgresDeleteArxiuAbast(d *PostgreSQL, id int) error {
	if _, err := d.Conn.Exec(`DELETE FROM arxiu_abast WHERE id = $1`, id); err != nil {
		return arxiuAbastWrap("postgres", "delete_arxiu_abast", id, err)
	}
	return nil
}

func postgresUpdateArxiuAbastModeracio(d *PostgreSQL, id int, estat, motiu string, moderatorID int) error {
	if _, err := d.Conn.Exec(`UPDATE arxiu_abast SET moderation_status=$1, moderation_notes=$2, moderated_by=$3, moderated_at=NOW(), updated_at=NOW() WHERE id=$4`, estat, motiu, moderatorID, id); err != nil {
		return arxiuAbastWrap("postgres", "update_arxiu_abast_moderacio", id, err)
	}
	return nil
}

func mysqlListArxiuAbasts(d *MySQL, arxiuID int, targetKind string, status string) ([]ArxiuAbast, error) {
	query := `SELECT id, arxiu_id, target_kind, target_id, COALESCE(target_code, ''), COALESCE(target_label, ''), relation_kind, COALESCE(notes, ''), estat, moderation_status, COALESCE(moderation_notes, ''), created_by, updated_by, moderated_by, moderated_at, created_at, updated_at FROM arxiu_abast WHERE 1=1`
	args := []interface{}{}
	if arxiuID > 0 {
		query += ` AND arxiu_id = ?`
		args = append(args, arxiuID)
	}
	if strings.TrimSpace(targetKind) != "" {
		query += ` AND target_kind = ?`
		args = append(args, strings.TrimSpace(targetKind))
	}
	if strings.TrimSpace(status) != "" {
		query += ` AND moderation_status = ?`
		args = append(args, strings.TrimSpace(status))
	}
	query += ` ORDER BY arxiu_id, target_kind, target_label, id`
	rows, err := d.Conn.Query(query, args...)
	if err != nil {
		return nil, arxiuAbastWrap("mysql", "list_arxiu_abast", 0, err)
	}
	defer rows.Close()
	out := []ArxiuAbast{}
	for rows.Next() {
		var rel ArxiuAbast
		if err := scanArxiuAbast(rows, &rel); err != nil {
			return nil, arxiuAbastWrap("mysql", "scan_arxiu_abast", 0, err)
		}
		out = append(out, rel)
	}
	if err := rows.Err(); err != nil {
		return nil, arxiuAbastWrap("mysql", "rows_arxiu_abast", 0, err)
	}
	return out, nil
}

func mysqlGetArxiuAbast(d *MySQL, id int) (*ArxiuAbast, error) {
	row := d.Conn.QueryRow(`SELECT id, arxiu_id, target_kind, target_id, COALESCE(target_code, ''), COALESCE(target_label, ''), relation_kind, COALESCE(notes, ''), estat, moderation_status, COALESCE(moderation_notes, ''), created_by, updated_by, moderated_by, moderated_at, created_at, updated_at FROM arxiu_abast WHERE id = ?`, id)
	var rel ArxiuAbast
	if err := scanArxiuAbast(row, &rel); err != nil {
		return nil, arxiuAbastWrap("mysql", "get_arxiu_abast", id, err)
	}
	return &rel, nil
}

func mysqlSaveArxiuAbast(d *MySQL, rel *ArxiuAbast) (int, error) {
	if rel == nil {
		return 0, nil
	}
	if rel.ID == 0 {
		res, err := d.Conn.Exec(`INSERT INTO arxiu_abast (arxiu_id, target_kind, target_id, target_code, target_label, relation_kind, notes, estat, moderation_status, moderation_notes, created_by, updated_by, moderated_by, moderated_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())`, rel.ArxiuID, rel.TargetKind, rel.TargetID, rel.TargetCode, rel.TargetLabel, rel.RelationKind, rel.Notes, rel.Estat, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt)
		if err != nil {
			return 0, arxiuAbastWrap("mysql", "create_arxiu_abast", 0, err)
		}
		if id, err := res.LastInsertId(); err == nil {
			rel.ID = int(id)
		}
		return rel.ID, nil
	}
	if _, err := d.Conn.Exec(`UPDATE arxiu_abast SET arxiu_id=?, target_kind=?, target_id=?, target_code=?, target_label=?, relation_kind=?, notes=?, estat=?, moderation_status=?, moderation_notes=?, created_by=?, updated_by=?, moderated_by=?, moderated_at=?, updated_at=NOW() WHERE id=?`, rel.ArxiuID, rel.TargetKind, rel.TargetID, rel.TargetCode, rel.TargetLabel, rel.RelationKind, rel.Notes, rel.Estat, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt, rel.ID); err != nil {
		return 0, arxiuAbastWrap("mysql", "update_arxiu_abast", rel.ID, err)
	}
	return rel.ID, nil
}

func mysqlDeleteArxiuAbast(d *MySQL, id int) error {
	if _, err := d.Conn.Exec(`DELETE FROM arxiu_abast WHERE id = ?`, id); err != nil {
		return arxiuAbastWrap("mysql", "delete_arxiu_abast", id, err)
	}
	return nil
}

func mysqlUpdateArxiuAbastModeracio(d *MySQL, id int, estat, motiu string, moderatorID int) error {
	if _, err := d.Conn.Exec(`UPDATE arxiu_abast SET moderation_status=?, moderation_notes=?, moderated_by=?, moderated_at=NOW(), updated_at=NOW() WHERE id=?`, estat, motiu, moderatorID, id); err != nil {
		return arxiuAbastWrap("mysql", "update_arxiu_abast_moderacio", id, err)
	}
	return nil
}

type arxiuAbastScanner interface {
	Scan(dest ...interface{}) error
}

func scanArxiuAbast(scanner arxiuAbastScanner, rel *ArxiuAbast) error {
	return scanner.Scan(&rel.ID, &rel.ArxiuID, &rel.TargetKind, &rel.TargetID, &rel.TargetCode, &rel.TargetLabel, &rel.RelationKind, &rel.Notes, &rel.Estat, &rel.ModeracioEstat, &rel.ModeracioMotiu, &rel.CreatedBy, &rel.UpdatedBy, &rel.ModeratedBy, &rel.ModeratedAt, &rel.CreatedAt, &rel.UpdatedAt)
}

func arxiuAbastWrap(engine string, op string, id int, err error) error {
	if err == nil {
		return nil
	}
	return WrapSQLError(SQLErrorContext{Engine: engine, Component: "arxiu_abast", Op: op, Object: "arxiu_abast", ObjectID: id}, err)
}
