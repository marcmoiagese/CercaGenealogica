package db

import (
	"database/sql"
	"strings"
)

type arxiuEntitatReligiosaQueries struct {
	engine          string
	list            string
	get             string
	insert          string
	update          string
	delete          string
	updateModeracio string
	returningID     bool
}

func sqliteArxiuEntitatReligiosaQueries() arxiuEntitatReligiosaQueries {
	return arxiuEntitatReligiosaQueries{
		engine:          "sqlite",
		list:            `SELECT id, arxiu_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, COALESCE(observacions, ''), estat, moderation_status, COALESCE(moderation_notes, ''), created_by, updated_by, moderated_by, moderated_at, created_at, updated_at FROM arxiu_entitat_religiosa`,
		get:             `SELECT id, arxiu_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, COALESCE(observacions, ''), estat, moderation_status, COALESCE(moderation_notes, ''), created_by, updated_by, moderated_by, moderated_at, created_at, updated_at FROM arxiu_entitat_religiosa WHERE id = ?`,
		insert:          `INSERT INTO arxiu_entitat_religiosa (arxiu_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, observacions, estat, moderation_status, moderation_notes, created_by, updated_by, moderated_by, moderated_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))`,
		update:          `UPDATE arxiu_entitat_religiosa SET arxiu_id=?, entitat_religiosa_id=?, tipus_relacio=?, any_inici=?, any_fi=?, observacions=?, estat=?, moderation_status=?, moderation_notes=?, created_by=?, updated_by=?, moderated_by=?, moderated_at=?, updated_at=datetime('now') WHERE id=?`,
		delete:          `DELETE FROM arxiu_entitat_religiosa WHERE id = ?`,
		updateModeracio: `UPDATE arxiu_entitat_religiosa SET moderation_status=?, moderation_notes=?, moderated_by=?, moderated_at=datetime('now'), updated_at=datetime('now') WHERE id=?`,
	}
}

func mysqlArxiuEntitatReligiosaQueries() arxiuEntitatReligiosaQueries {
	q := sqliteArxiuEntitatReligiosaQueries()
	q.engine = "mysql"
	q.insert = `INSERT INTO arxiu_entitat_religiosa (arxiu_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, observacions, estat, moderation_status, moderation_notes, created_by, updated_by, moderated_by, moderated_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())`
	q.update = `UPDATE arxiu_entitat_religiosa SET arxiu_id=?, entitat_religiosa_id=?, tipus_relacio=?, any_inici=?, any_fi=?, observacions=?, estat=?, moderation_status=?, moderation_notes=?, created_by=?, updated_by=?, moderated_by=?, moderated_at=?, updated_at=NOW() WHERE id=?`
	q.updateModeracio = `UPDATE arxiu_entitat_religiosa SET moderation_status=?, moderation_notes=?, moderated_by=?, moderated_at=NOW(), updated_at=NOW() WHERE id=?`
	return q
}

func postgresArxiuEntitatReligiosaQueries() arxiuEntitatReligiosaQueries {
	return arxiuEntitatReligiosaQueries{
		engine: "postgres", returningID: true,
		list:            `SELECT id, arxiu_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, COALESCE(observacions, ''), estat, moderation_status, COALESCE(moderation_notes, ''), created_by, updated_by, moderated_by, moderated_at, created_at, updated_at FROM arxiu_entitat_religiosa`,
		get:             `SELECT id, arxiu_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, COALESCE(observacions, ''), estat, moderation_status, COALESCE(moderation_notes, ''), created_by, updated_by, moderated_by, moderated_at, created_at, updated_at FROM arxiu_entitat_religiosa WHERE id = $1`,
		insert:          `INSERT INTO arxiu_entitat_religiosa (arxiu_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, observacions, estat, moderation_status, moderation_notes, created_by, updated_by, moderated_by, moderated_at, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW(), NOW()) RETURNING id`,
		update:          `UPDATE arxiu_entitat_religiosa SET arxiu_id=$1, entitat_religiosa_id=$2, tipus_relacio=$3, any_inici=$4, any_fi=$5, observacions=$6, estat=$7, moderation_status=$8, moderation_notes=$9, created_by=$10, updated_by=$11, moderated_by=$12, moderated_at=$13, updated_at=NOW() WHERE id=$14`,
		delete:          `DELETE FROM arxiu_entitat_religiosa WHERE id = $1`,
		updateModeracio: `UPDATE arxiu_entitat_religiosa SET moderation_status=$1, moderation_notes=$2, moderated_by=$3, moderated_at=NOW(), updated_at=NOW() WHERE id=$4`,
	}
}

func listArxiuEntitatsReligioses(conn *sql.DB, q arxiuEntitatReligiosaQueries, arxiuID int, entitatReligiosaID int, status string) ([]ArxiuEntitatReligiosa, error) {
	clauses := []string{"1=1"}
	args := []interface{}{}
	if arxiuID > 0 {
		clauses = append(clauses, "arxiu_id = ?")
		args = append(args, arxiuID)
	}
	if entitatReligiosaID > 0 {
		clauses = append(clauses, "entitat_religiosa_id = ?")
		args = append(args, entitatReligiosaID)
	}
	if strings.TrimSpace(status) != "" {
		clauses = append(clauses, "moderation_status = ?")
		args = append(args, strings.TrimSpace(status))
	}
	query := q.list + " WHERE " + strings.Join(clauses, " AND ") + " ORDER BY arxiu_id, entitat_religiosa_id, any_inici, id"
	query = formatPlaceholders(q.engine, query)
	rows, err := conn.Query(query, args...)
	if err != nil {
		return nil, arxiuEntitatReligiosaWrap(q, "list_arxiu_entitat_religiosa", 0, err)
	}
	defer rows.Close()
	out := []ArxiuEntitatReligiosa{}
	for rows.Next() {
		var rel ArxiuEntitatReligiosa
		if err := scanArxiuEntitatReligiosa(rows, &rel); err != nil {
			return nil, arxiuEntitatReligiosaWrap(q, "scan_arxiu_entitat_religiosa", 0, err)
		}
		out = append(out, rel)
	}
	if err := rows.Err(); err != nil {
		return nil, arxiuEntitatReligiosaWrap(q, "rows_arxiu_entitat_religiosa", 0, err)
	}
	return out, nil
}

func getArxiuEntitatReligiosa(conn *sql.DB, q arxiuEntitatReligiosaQueries, id int) (*ArxiuEntitatReligiosa, error) {
	row := conn.QueryRow(q.get, id)
	var rel ArxiuEntitatReligiosa
	if err := scanArxiuEntitatReligiosa(row, &rel); err != nil {
		return nil, arxiuEntitatReligiosaWrap(q, "get_arxiu_entitat_religiosa", id, err)
	}
	return &rel, nil
}

func saveArxiuEntitatReligiosa(conn *sql.DB, q arxiuEntitatReligiosaQueries, rel *ArxiuEntitatReligiosa) (int, error) {
	if rel == nil {
		return 0, nil
	}
	args := []interface{}{rel.ArxiuID, rel.EntitatReligiosaID, rel.TipusRelacio, rel.AnyInici, rel.AnyFi, rel.Observacions, rel.Estat, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt}
	if rel.ID == 0 {
		if q.returningID {
			if err := conn.QueryRow(q.insert, args...).Scan(&rel.ID); err != nil {
				return 0, arxiuEntitatReligiosaWrap(q, "create_arxiu_entitat_religiosa", 0, err)
			}
			return rel.ID, nil
		}
		res, err := conn.Exec(q.insert, args...)
		if err != nil {
			return 0, arxiuEntitatReligiosaWrap(q, "create_arxiu_entitat_religiosa", 0, err)
		}
		if id, err := res.LastInsertId(); err == nil {
			rel.ID = int(id)
		}
		return rel.ID, nil
	}
	args = append(args, rel.ID)
	if _, err := conn.Exec(q.update, args...); err != nil {
		return 0, arxiuEntitatReligiosaWrap(q, "update_arxiu_entitat_religiosa", rel.ID, err)
	}
	return rel.ID, nil
}

func deleteArxiuEntitatReligiosa(conn *sql.DB, q arxiuEntitatReligiosaQueries, id int) error {
	if _, err := conn.Exec(q.delete, id); err != nil {
		return arxiuEntitatReligiosaWrap(q, "delete_arxiu_entitat_religiosa", id, err)
	}
	return nil
}

func updateArxiuEntitatReligiosaModeracio(conn *sql.DB, q arxiuEntitatReligiosaQueries, id int, estat, motiu string, moderatorID int) error {
	if _, err := conn.Exec(q.updateModeracio, estat, motiu, moderatorID, id); err != nil {
		return arxiuEntitatReligiosaWrap(q, "update_arxiu_entitat_religiosa_moderacio", id, err)
	}
	return nil
}

type arxiuEntitatReligiosaScanner interface {
	Scan(dest ...interface{}) error
}

func scanArxiuEntitatReligiosa(scanner arxiuEntitatReligiosaScanner, rel *ArxiuEntitatReligiosa) error {
	return scanner.Scan(&rel.ID, &rel.ArxiuID, &rel.EntitatReligiosaID, &rel.TipusRelacio, &rel.AnyInici, &rel.AnyFi, &rel.Observacions, &rel.Estat, &rel.ModeracioEstat, &rel.ModeracioMotiu, &rel.CreatedBy, &rel.UpdatedBy, &rel.ModeratedBy, &rel.ModeratedAt, &rel.CreatedAt, &rel.UpdatedAt)
}

func arxiuEntitatReligiosaWrap(q arxiuEntitatReligiosaQueries, op string, id int, err error) error {
	if err == nil {
		return nil
	}
	return WrapSQLError(SQLErrorContext{Engine: q.engine, Component: "arxiu_entitat_religiosa", Op: op, Object: "arxiu_entitat_religiosa", ObjectID: id}, err)
}

func sqliteListArxiuEntitatsReligioses(d *SQLite, arxiuID int, entitatReligiosaID int, status string) ([]ArxiuEntitatReligiosa, error) {
	return listArxiuEntitatsReligioses(d.Conn, sqliteArxiuEntitatReligiosaQueries(), arxiuID, entitatReligiosaID, status)
}
func postgresListArxiuEntitatsReligioses(d *PostgreSQL, arxiuID int, entitatReligiosaID int, status string) ([]ArxiuEntitatReligiosa, error) {
	return listArxiuEntitatsReligioses(d.Conn, postgresArxiuEntitatReligiosaQueries(), arxiuID, entitatReligiosaID, status)
}
func mysqlListArxiuEntitatsReligioses(d *MySQL, arxiuID int, entitatReligiosaID int, status string) ([]ArxiuEntitatReligiosa, error) {
	return listArxiuEntitatsReligioses(d.Conn, mysqlArxiuEntitatReligiosaQueries(), arxiuID, entitatReligiosaID, status)
}
func sqliteGetArxiuEntitatReligiosa(d *SQLite, id int) (*ArxiuEntitatReligiosa, error) {
	return getArxiuEntitatReligiosa(d.Conn, sqliteArxiuEntitatReligiosaQueries(), id)
}
func postgresGetArxiuEntitatReligiosa(d *PostgreSQL, id int) (*ArxiuEntitatReligiosa, error) {
	return getArxiuEntitatReligiosa(d.Conn, postgresArxiuEntitatReligiosaQueries(), id)
}
func mysqlGetArxiuEntitatReligiosa(d *MySQL, id int) (*ArxiuEntitatReligiosa, error) {
	return getArxiuEntitatReligiosa(d.Conn, mysqlArxiuEntitatReligiosaQueries(), id)
}
func sqliteSaveArxiuEntitatReligiosa(d *SQLite, rel *ArxiuEntitatReligiosa) (int, error) {
	return saveArxiuEntitatReligiosa(d.Conn, sqliteArxiuEntitatReligiosaQueries(), rel)
}
func postgresSaveArxiuEntitatReligiosa(d *PostgreSQL, rel *ArxiuEntitatReligiosa) (int, error) {
	return saveArxiuEntitatReligiosa(d.Conn, postgresArxiuEntitatReligiosaQueries(), rel)
}
func mysqlSaveArxiuEntitatReligiosa(d *MySQL, rel *ArxiuEntitatReligiosa) (int, error) {
	return saveArxiuEntitatReligiosa(d.Conn, mysqlArxiuEntitatReligiosaQueries(), rel)
}
func sqliteDeleteArxiuEntitatReligiosa(d *SQLite, id int) error {
	return deleteArxiuEntitatReligiosa(d.Conn, sqliteArxiuEntitatReligiosaQueries(), id)
}
func postgresDeleteArxiuEntitatReligiosa(d *PostgreSQL, id int) error {
	return deleteArxiuEntitatReligiosa(d.Conn, postgresArxiuEntitatReligiosaQueries(), id)
}
func mysqlDeleteArxiuEntitatReligiosa(d *MySQL, id int) error {
	return deleteArxiuEntitatReligiosa(d.Conn, mysqlArxiuEntitatReligiosaQueries(), id)
}
func sqliteUpdateArxiuEntitatReligiosaModeracio(d *SQLite, id int, estat, motiu string, moderatorID int) error {
	return updateArxiuEntitatReligiosaModeracio(d.Conn, sqliteArxiuEntitatReligiosaQueries(), id, estat, motiu, moderatorID)
}
func postgresUpdateArxiuEntitatReligiosaModeracio(d *PostgreSQL, id int, estat, motiu string, moderatorID int) error {
	return updateArxiuEntitatReligiosaModeracio(d.Conn, postgresArxiuEntitatReligiosaQueries(), id, estat, motiu, moderatorID)
}
func mysqlUpdateArxiuEntitatReligiosaModeracio(d *MySQL, id int, estat, motiu string, moderatorID int) error {
	return updateArxiuEntitatReligiosaModeracio(d.Conn, mysqlArxiuEntitatReligiosaQueries(), id, estat, motiu, moderatorID)
}
