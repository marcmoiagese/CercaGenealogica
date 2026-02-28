package db

import (
	"database/sql"
	"strings"
)

func (h sqlHelper) createEspaiArbre(a *EspaiArbre) (int, error) {
	if a == nil {
		return 0, nil
	}
	visibility := strings.TrimSpace(a.Visibility)
	if visibility == "" {
		visibility = "private"
	}
	status := strings.TrimSpace(a.Status)
	if status == "" {
		status = "active"
	}
	stmt := `INSERT INTO espai_arbres (owner_user_id, nom, descripcio, visibility, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	if h.style == "postgres" {
		if err := h.db.QueryRow(stmt, a.OwnerUserID, a.Nom, a.Descripcio, visibility, status).Scan(&a.ID); err != nil {
			return 0, err
		}
		return a.ID, nil
	}
	res, err := h.db.Exec(stmt, a.OwnerUserID, a.Nom, a.Descripcio, visibility, status)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		a.ID = int(id)
	}
	return a.ID, nil
}

func (h sqlHelper) updateEspaiArbre(a *EspaiArbre) error {
	if a == nil {
		return nil
	}
	stmt := `UPDATE espai_arbres
        SET nom = ?, descripcio = ?, visibility = ?, status = ?, updated_at = ` + h.nowFun + `
        WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, a.Nom, a.Descripcio, a.Visibility, a.Status, a.ID)
	return err
}

func (h sqlHelper) getEspaiArbre(id int) (*EspaiArbre, error) {
	query := `SELECT id, owner_user_id, nom, descripcio, visibility, status, created_at, updated_at
        FROM espai_arbres WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	var a EspaiArbre
	if err := h.db.QueryRow(query, id).Scan(&a.ID, &a.OwnerUserID, &a.Nom, &a.Descripcio, &a.Visibility, &a.Status, &a.CreatedAt, &a.UpdatedAt); err != nil {
		return nil, err
	}
	return &a, nil
}

func (h sqlHelper) listEspaiArbresByOwner(ownerID int) ([]EspaiArbre, error) {
	query := `SELECT id, owner_user_id, nom, descripcio, visibility, status, created_at, updated_at
        FROM espai_arbres WHERE owner_user_id = ? ORDER BY updated_at DESC, id DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiArbre
	for rows.Next() {
		var a EspaiArbre
		if err := rows.Scan(&a.ID, &a.OwnerUserID, &a.Nom, &a.Descripcio, &a.Visibility, &a.Status, &a.CreatedAt, &a.UpdatedAt); err != nil {
			return nil, err
		}
		res = append(res, a)
	}
	return res, nil
}

func (h sqlHelper) listEspaiArbresPublic() ([]EspaiArbre, error) {
	query := `SELECT id, owner_user_id, nom, descripcio, visibility, status, created_at, updated_at
        FROM espai_arbres WHERE visibility = 'public' AND status = 'active' ORDER BY updated_at DESC, id DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiArbre
	for rows.Next() {
		var a EspaiArbre
		if err := rows.Scan(&a.ID, &a.OwnerUserID, &a.Nom, &a.Descripcio, &a.Visibility, &a.Status, &a.CreatedAt, &a.UpdatedAt); err != nil {
			return nil, err
		}
		res = append(res, a)
	}
	return res, nil
}

func (h sqlHelper) deleteEspaiArbre(ownerID, treeID int) error {
	stmt := `DELETE FROM espai_arbres WHERE id = ? AND owner_user_id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	res, err := h.db.Exec(stmt, treeID, ownerID)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err == nil && affected == 0 {
		return sql.ErrNoRows
	}
	return err
}

func (h sqlHelper) createEspaiFontImportacio(f *EspaiFontImportacio) (int, error) {
	if f == nil {
		return 0, nil
	}
	sourceType := strings.TrimSpace(f.SourceType)
	if sourceType == "" {
		sourceType = "gedcom"
	}
	stmt := `INSERT INTO espai_fonts_importacio (owner_user_id, source_type, nom, original_filename, storage_path, checksum_sha256, size_bytes, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	args := []interface{}{f.OwnerUserID, sourceType, f.Nom, f.OriginalFilename, f.StoragePath, f.ChecksumSHA256, f.SizeBytes}
	if h.style == "postgres" {
		if err := h.db.QueryRow(stmt, args...).Scan(&f.ID); err != nil {
			return 0, err
		}
		return f.ID, nil
	}
	res, err := h.db.Exec(stmt, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		f.ID = int(id)
	}
	return f.ID, nil
}

func (h sqlHelper) updateEspaiFontImportacio(f *EspaiFontImportacio) error {
	if f == nil || f.ID == 0 {
		return nil
	}
	stmt := `UPDATE espai_fonts_importacio
        SET original_filename = ?, storage_path = ?, checksum_sha256 = ?, size_bytes = ?, updated_at = ` + h.nowFun + `
        WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, f.OriginalFilename, f.StoragePath, f.ChecksumSHA256, f.SizeBytes, f.ID)
	return err
}

func (h sqlHelper) getEspaiFontImportacio(id int) (*EspaiFontImportacio, error) {
	query := `SELECT id, owner_user_id, source_type, nom, original_filename, storage_path, checksum_sha256, size_bytes, created_at, updated_at
        FROM espai_fonts_importacio WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	var f EspaiFontImportacio
	if err := h.db.QueryRow(query, id).Scan(&f.ID, &f.OwnerUserID, &f.SourceType, &f.Nom, &f.OriginalFilename, &f.StoragePath, &f.ChecksumSHA256, &f.SizeBytes, &f.CreatedAt, &f.UpdatedAt); err != nil {
		return nil, err
	}
	return &f, nil
}

func (h sqlHelper) deleteEspaiFontImportacio(id int) error {
	stmt := `DELETE FROM espai_fonts_importacio WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	res, err := h.db.Exec(stmt, id)
	if err != nil {
		return err
	}
	if rows, err := res.RowsAffected(); err == nil && rows == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (h sqlHelper) getEspaiFontImportacioByChecksum(ownerID int, checksum string) (*EspaiFontImportacio, error) {
	query := `SELECT id, owner_user_id, source_type, nom, original_filename, storage_path, checksum_sha256, size_bytes, created_at, updated_at
        FROM espai_fonts_importacio WHERE owner_user_id = ? AND checksum_sha256 = ?`
	query = formatPlaceholders(h.style, query)
	var f EspaiFontImportacio
	if err := h.db.QueryRow(query, ownerID, checksum).Scan(&f.ID, &f.OwnerUserID, &f.SourceType, &f.Nom, &f.OriginalFilename, &f.StoragePath, &f.ChecksumSHA256, &f.SizeBytes, &f.CreatedAt, &f.UpdatedAt); err != nil {
		return nil, err
	}
	return &f, nil
}

func (h sqlHelper) listEspaiFontsImportacioByOwner(ownerID int) ([]EspaiFontImportacio, error) {
	query := `SELECT id, owner_user_id, source_type, nom, original_filename, storage_path, checksum_sha256, size_bytes, created_at, updated_at
        FROM espai_fonts_importacio WHERE owner_user_id = ? ORDER BY created_at DESC, id DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiFontImportacio
	for rows.Next() {
		var f EspaiFontImportacio
		if err := rows.Scan(&f.ID, &f.OwnerUserID, &f.SourceType, &f.Nom, &f.OriginalFilename, &f.StoragePath, &f.ChecksumSHA256, &f.SizeBytes, &f.CreatedAt, &f.UpdatedAt); err != nil {
			return nil, err
		}
		res = append(res, f)
	}
	return res, nil
}

func (h sqlHelper) createEspaiImport(i *EspaiImport) (int, error) {
	if i == nil {
		return 0, nil
	}
	importType := strings.TrimSpace(i.ImportType)
	if importType == "" {
		importType = "gedcom"
	}
	importMode := strings.TrimSpace(i.ImportMode)
	if importMode == "" {
		importMode = "full"
	}
	status := strings.TrimSpace(i.Status)
	if status == "" {
		status = "queued"
	}
	stmt := `INSERT INTO espai_imports (owner_user_id, arbre_id, font_id, import_type, import_mode, status, progress_total, progress_done, summary_json, error_text, started_at, finished_at, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	args := []interface{}{i.OwnerUserID, i.ArbreID, i.FontID, importType, importMode, status, i.ProgressTotal, i.ProgressDone, i.SummaryJSON, i.ErrorText, i.StartedAt, i.FinishedAt}
	if h.style == "postgres" {
		if err := h.db.QueryRow(stmt, args...).Scan(&i.ID); err != nil {
			return 0, err
		}
		return i.ID, nil
	}
	res, err := h.db.Exec(stmt, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		i.ID = int(id)
	}
	return i.ID, nil
}

func (h sqlHelper) updateEspaiImportStatus(id int, status string, errorText, summaryJSON string) error {
	stmt := `UPDATE espai_imports SET status = ?, error_text = ?, summary_json = ?, updated_at = ` + h.nowFun + ` WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, status, nullableString(errorText), nullableString(summaryJSON), id)
	return err
}

func (h sqlHelper) updateEspaiImportProgress(id int, done, total int) error {
	stmt := `UPDATE espai_imports SET progress_done = ?, progress_total = ?, updated_at = ` + h.nowFun + ` WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, done, total, id)
	return err
}

func (h sqlHelper) getEspaiImport(id int) (*EspaiImport, error) {
	query := `SELECT id, owner_user_id, arbre_id, font_id, import_type, import_mode, status, progress_total, progress_done, summary_json, error_text, started_at, finished_at, created_at, updated_at
        FROM espai_imports WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	var i EspaiImport
	if err := h.db.QueryRow(query, id).Scan(&i.ID, &i.OwnerUserID, &i.ArbreID, &i.FontID, &i.ImportType, &i.ImportMode, &i.Status, &i.ProgressTotal, &i.ProgressDone, &i.SummaryJSON, &i.ErrorText, &i.StartedAt, &i.FinishedAt, &i.CreatedAt, &i.UpdatedAt); err != nil {
		return nil, err
	}
	return &i, nil
}

func (h sqlHelper) getLatestEspaiImportByFont(ownerID, fontID int) (*EspaiImport, error) {
	query := `SELECT id, owner_user_id, arbre_id, font_id, import_type, import_mode, status, progress_total, progress_done, summary_json, error_text, started_at, finished_at, created_at, updated_at
        FROM espai_imports WHERE owner_user_id = ? AND font_id = ? ORDER BY created_at DESC, id DESC LIMIT 1`
	query = formatPlaceholders(h.style, query)
	var i EspaiImport
	if err := h.db.QueryRow(query, ownerID, fontID).Scan(&i.ID, &i.OwnerUserID, &i.ArbreID, &i.FontID, &i.ImportType, &i.ImportMode, &i.Status, &i.ProgressTotal, &i.ProgressDone, &i.SummaryJSON, &i.ErrorText, &i.StartedAt, &i.FinishedAt, &i.CreatedAt, &i.UpdatedAt); err != nil {
		return nil, err
	}
	return &i, nil
}

func (h sqlHelper) listEspaiImportsByOwner(ownerID int) ([]EspaiImport, error) {
	query := `SELECT id, owner_user_id, arbre_id, font_id, import_type, import_mode, status, progress_total, progress_done, summary_json, error_text, started_at, finished_at, created_at, updated_at
        FROM espai_imports WHERE owner_user_id = ? ORDER BY created_at DESC, id DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiImport
	for rows.Next() {
		var i EspaiImport
		if err := rows.Scan(&i.ID, &i.OwnerUserID, &i.ArbreID, &i.FontID, &i.ImportType, &i.ImportMode, &i.Status, &i.ProgressTotal, &i.ProgressDone, &i.SummaryJSON, &i.ErrorText, &i.StartedAt, &i.FinishedAt, &i.CreatedAt, &i.UpdatedAt); err != nil {
			return nil, err
		}
		res = append(res, i)
	}
	return res, nil
}

func (h sqlHelper) listEspaiImportsByArbre(arbreID int) ([]EspaiImport, error) {
	query := `SELECT id, owner_user_id, arbre_id, font_id, import_type, import_mode, status, progress_total, progress_done, summary_json, error_text, started_at, finished_at, created_at, updated_at
        FROM espai_imports WHERE arbre_id = ? ORDER BY created_at DESC, id DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, arbreID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiImport
	for rows.Next() {
		var i EspaiImport
		if err := rows.Scan(&i.ID, &i.OwnerUserID, &i.ArbreID, &i.FontID, &i.ImportType, &i.ImportMode, &i.Status, &i.ProgressTotal, &i.ProgressDone, &i.SummaryJSON, &i.ErrorText, &i.StartedAt, &i.FinishedAt, &i.CreatedAt, &i.UpdatedAt); err != nil {
			return nil, err
		}
		res = append(res, i)
	}
	return res, nil
}

func (h sqlHelper) listEspaiImportsByStatus(status string, limit int) ([]EspaiImport, error) {
	status = strings.TrimSpace(status)
	if status == "" {
		return nil, nil
	}
	query := `SELECT id, owner_user_id, arbre_id, font_id, import_type, import_mode, status, progress_total, progress_done, summary_json, error_text, started_at, finished_at, created_at, updated_at
        FROM espai_imports WHERE status = ? ORDER BY created_at ASC, id ASC`
	args := []interface{}{status}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiImport
	for rows.Next() {
		var i EspaiImport
		if err := rows.Scan(&i.ID, &i.OwnerUserID, &i.ArbreID, &i.FontID, &i.ImportType, &i.ImportMode, &i.Status, &i.ProgressTotal, &i.ProgressDone, &i.SummaryJSON, &i.ErrorText, &i.StartedAt, &i.FinishedAt, &i.CreatedAt, &i.UpdatedAt); err != nil {
			return nil, err
		}
		res = append(res, i)
	}
	return res, nil
}

func (h sqlHelper) deleteEspaiImportsByArbre(arbreID int) error {
	stmt := `DELETE FROM espai_imports WHERE arbre_id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, arbreID)
	return err
}

func (h sqlHelper) countEspaiImportsByFont(fontID int) (int, error) {
	query := `SELECT COUNT(*) FROM espai_imports WHERE font_id = ?`
	query = formatPlaceholders(h.style, query)
	var total int
	if err := h.db.QueryRow(query, fontID).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (h sqlHelper) clearEspaiTreeData(arbreID int) error {
	stmts := []string{
		`DELETE FROM espai_events WHERE arbre_id = ?`,
		`DELETE FROM espai_relacions WHERE arbre_id = ?`,
		`DELETE FROM espai_coincidencies WHERE arbre_id = ?`,
		`DELETE FROM espai_persones WHERE arbre_id = ?`,
	}
	for _, stmt := range stmts {
		stmt = formatPlaceholders(h.style, stmt)
		if _, err := h.db.Exec(stmt, arbreID); err != nil {
			return err
		}
	}
	return nil
}

func (h sqlHelper) createEspaiPersona(p *EspaiPersona) (int, error) {
	if p == nil {
		return 0, nil
	}
	status := strings.TrimSpace(p.Status)
	if status == "" {
		status = "active"
	}
	visibility := strings.TrimSpace(p.Visibility)
	if visibility == "" {
		visibility = "visible"
	}
	if !p.NomComplet.Valid {
		full := strings.TrimSpace(strings.Join([]string{p.Nom.String, p.Cognom1.String, p.Cognom2.String}, " "))
		if full != "" {
			p.NomComplet = sql.NullString{String: full, Valid: true}
		}
	}
	stmt := `INSERT INTO espai_persones (owner_user_id, arbre_id, external_id, nom, cognom1, cognom2, nom_complet, sexe, data_naixement, data_defuncio, lloc_naixement, lloc_defuncio, notes, has_media, visibility, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	args := []interface{}{p.OwnerUserID, p.ArbreID, p.ExternalID, p.Nom, p.Cognom1, p.Cognom2, p.NomComplet, p.Sexe, p.DataNaixement, p.DataDefuncio, p.LlocNaixement, p.LlocDefuncio, p.Notes, p.HasMedia, visibility, status}
	if h.style == "postgres" {
		if err := h.db.QueryRow(stmt, args...).Scan(&p.ID); err != nil {
			return 0, err
		}
		return p.ID, nil
	}
	res, err := h.db.Exec(stmt, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		p.ID = int(id)
	}
	return p.ID, nil
}

func (h sqlHelper) updateEspaiPersona(p *EspaiPersona) error {
	if p == nil || p.ID == 0 {
		return nil
	}
	if !p.NomComplet.Valid {
		full := strings.TrimSpace(strings.Join([]string{p.Nom.String, p.Cognom1.String, p.Cognom2.String}, " "))
		if full != "" {
			p.NomComplet = sql.NullString{String: full, Valid: true}
		}
	}
	stmt := `UPDATE espai_persones
        SET nom = ?, cognom1 = ?, cognom2 = ?, nom_complet = ?, sexe = ?, data_naixement = ?, data_defuncio = ?, lloc_naixement = ?, lloc_defuncio = ?, notes = ?, has_media = ?, updated_at = ` + h.nowFun + `
        WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, p.Nom, p.Cognom1, p.Cognom2, p.NomComplet, p.Sexe, p.DataNaixement, p.DataDefuncio, p.LlocNaixement, p.LlocDefuncio, p.Notes, p.HasMedia, p.ID)
	return err
}

func (h sqlHelper) getEspaiPersona(id int) (*EspaiPersona, error) {
	query := `SELECT id, owner_user_id, arbre_id, external_id, nom, cognom1, cognom2, nom_complet, sexe, data_naixement, data_defuncio, lloc_naixement, lloc_defuncio, notes, has_media, visibility, status, created_at, updated_at
        FROM espai_persones WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	var p EspaiPersona
	if err := h.db.QueryRow(query, id).Scan(&p.ID, &p.OwnerUserID, &p.ArbreID, &p.ExternalID, &p.Nom, &p.Cognom1, &p.Cognom2, &p.NomComplet, &p.Sexe, &p.DataNaixement, &p.DataDefuncio, &p.LlocNaixement, &p.LlocDefuncio, &p.Notes, &p.HasMedia, &p.Visibility, &p.Status, &p.CreatedAt, &p.UpdatedAt); err != nil {
		return nil, err
	}
	return &p, nil
}

func (h sqlHelper) listEspaiPersonesByArbre(arbreID int) ([]EspaiPersona, error) {
	query := `SELECT id, owner_user_id, arbre_id, external_id, nom, cognom1, cognom2, nom_complet, sexe, data_naixement, data_defuncio, lloc_naixement, lloc_defuncio, notes, has_media, visibility, status, created_at, updated_at
        FROM espai_persones WHERE arbre_id = ? ORDER BY id DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, arbreID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiPersona
	for rows.Next() {
		var p EspaiPersona
		if err := rows.Scan(&p.ID, &p.OwnerUserID, &p.ArbreID, &p.ExternalID, &p.Nom, &p.Cognom1, &p.Cognom2, &p.NomComplet, &p.Sexe, &p.DataNaixement, &p.DataDefuncio, &p.LlocNaixement, &p.LlocDefuncio, &p.Notes, &p.HasMedia, &p.Visibility, &p.Status, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, err
		}
		res = append(res, p)
	}
	return res, nil
}

func (h sqlHelper) countEspaiPersonesByArbre(arbreID int) (int, int, error) {
	query := `SELECT COUNT(*), COALESCE(SUM(CASE WHEN visibility = 'hidden' THEN 1 ELSE 0 END), 0)
        FROM espai_persones WHERE arbre_id = ?`
	query = formatPlaceholders(h.style, query)
	var total, hidden int
	if err := h.db.QueryRow(query, arbreID).Scan(&total, &hidden); err != nil {
		return 0, 0, err
	}
	return total, hidden, nil
}

func (h sqlHelper) countEspaiPersonesByArbreQuery(arbreID int, queryText string) (int, error) {
	query := `SELECT COUNT(*) FROM espai_persones WHERE arbre_id = ?`
	args := []interface{}{arbreID}
	queryText = strings.TrimSpace(queryText)
	if queryText != "" {
		like := "%" + strings.ToLower(queryText) + "%"
		query += " AND (LOWER(nom_complet) LIKE ? OR LOWER(nom) LIKE ? OR LOWER(cognom1) LIKE ? OR LOWER(cognom2) LIKE ?)"
		args = append(args, like, like, like, like)
	}
	query = formatPlaceholders(h.style, query)
	var total int
	if err := h.db.QueryRow(query, args...).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (h sqlHelper) listEspaiPersonesByArbreQuery(arbreID int, queryText string, limit, offset int) ([]EspaiPersona, error) {
	query := `SELECT id, owner_user_id, arbre_id, external_id, nom, cognom1, cognom2, nom_complet, sexe, data_naixement, data_defuncio, lloc_naixement, lloc_defuncio, notes, has_media, visibility, status, created_at, updated_at
        FROM espai_persones WHERE arbre_id = ?`
	args := []interface{}{arbreID}
	queryText = strings.TrimSpace(queryText)
	if queryText != "" {
		like := "%" + strings.ToLower(queryText) + "%"
		query += " AND (LOWER(nom_complet) LIKE ? OR LOWER(nom) LIKE ? OR LOWER(cognom1) LIKE ? OR LOWER(cognom2) LIKE ?)"
		args = append(args, like, like, like, like)
	}
	query += " ORDER BY id DESC"
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
		if offset > 0 {
			query += " OFFSET ?"
			args = append(args, offset)
		}
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiPersona
	for rows.Next() {
		var p EspaiPersona
		if err := rows.Scan(&p.ID, &p.OwnerUserID, &p.ArbreID, &p.ExternalID, &p.Nom, &p.Cognom1, &p.Cognom2, &p.NomComplet, &p.Sexe, &p.DataNaixement, &p.DataDefuncio, &p.LlocNaixement, &p.LlocDefuncio, &p.Notes, &p.HasMedia, &p.Visibility, &p.Status, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, err
		}
		res = append(res, p)
	}
	return res, nil
}

func (h sqlHelper) countEspaiPersonesByOwnerFilters(ownerID int, nameFilter, treeFilter, visibility string) (int, error) {
	query := `SELECT COUNT(*)
        FROM espai_persones p
        JOIN espai_arbres a ON a.id = p.arbre_id
        WHERE p.owner_user_id = ? AND a.owner_user_id = ?`
	args := []interface{}{ownerID, ownerID}
	nameFilter = strings.TrimSpace(nameFilter)
	if nameFilter != "" {
		like := "%" + strings.ToLower(nameFilter) + "%"
		query += " AND (LOWER(p.nom_complet) LIKE ? OR LOWER(p.nom) LIKE ? OR LOWER(p.cognom1) LIKE ? OR LOWER(p.cognom2) LIKE ?)"
		args = append(args, like, like, like, like)
	}
	treeFilter = strings.TrimSpace(treeFilter)
	if treeFilter != "" {
		like := "%" + strings.ToLower(treeFilter) + "%"
		query += " AND LOWER(a.nom) LIKE ?"
		args = append(args, like)
	}
	visibility = strings.TrimSpace(visibility)
	if visibility != "" {
		query += " AND p.visibility = ?"
		args = append(args, visibility)
	}
	query = formatPlaceholders(h.style, query)
	var total int
	if err := h.db.QueryRow(query, args...).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (h sqlHelper) listEspaiPersonesByOwnerFilters(ownerID int, nameFilter, treeFilter, visibility string, limit, offset int) ([]EspaiPersonaTreeRow, error) {
	query := `SELECT p.id, p.owner_user_id, p.arbre_id, p.external_id, p.nom, p.cognom1, p.cognom2, p.nom_complet, p.sexe, p.data_naixement, p.data_defuncio, p.lloc_naixement, p.lloc_defuncio, p.notes, p.has_media, p.visibility, p.status, p.created_at, p.updated_at, a.nom
        FROM espai_persones p
        JOIN espai_arbres a ON a.id = p.arbre_id
        WHERE p.owner_user_id = ? AND a.owner_user_id = ?`
	args := []interface{}{ownerID, ownerID}
	nameFilter = strings.TrimSpace(nameFilter)
	if nameFilter != "" {
		like := "%" + strings.ToLower(nameFilter) + "%"
		query += " AND (LOWER(p.nom_complet) LIKE ? OR LOWER(p.nom) LIKE ? OR LOWER(p.cognom1) LIKE ? OR LOWER(p.cognom2) LIKE ?)"
		args = append(args, like, like, like, like)
	}
	treeFilter = strings.TrimSpace(treeFilter)
	if treeFilter != "" {
		like := "%" + strings.ToLower(treeFilter) + "%"
		query += " AND LOWER(a.nom) LIKE ?"
		args = append(args, like)
	}
	visibility = strings.TrimSpace(visibility)
	if visibility != "" {
		query += " AND p.visibility = ?"
		args = append(args, visibility)
	}
	query += " ORDER BY p.id DESC"
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
		if offset > 0 {
			query += " OFFSET ?"
			args = append(args, offset)
		}
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiPersonaTreeRow
	for rows.Next() {
		var row EspaiPersonaTreeRow
		if err := rows.Scan(&row.ID, &row.OwnerUserID, &row.ArbreID, &row.ExternalID, &row.Nom, &row.Cognom1, &row.Cognom2, &row.NomComplet, &row.Sexe, &row.DataNaixement, &row.DataDefuncio, &row.LlocNaixement, &row.LlocDefuncio, &row.Notes, &row.HasMedia, &row.Visibility, &row.Status, &row.CreatedAt, &row.UpdatedAt, &row.TreeName); err != nil {
			return nil, err
		}
		res = append(res, row)
	}
	return res, nil
}

func (h sqlHelper) buildEspaiPersonaDataFilters(f EspaiPersonaDataFilter) ([]string, []interface{}) {
	clauses := []string{}
	args := []interface{}{}
	addLike := func(val string, clause string) {
		val = strings.TrimSpace(val)
		if val == "" {
			return
		}
		clauses = append(clauses, clause)
		args = append(args, "%"+strings.ToLower(val)+"%")
	}
	addExact := func(val string, clause string) {
		val = strings.TrimSpace(val)
		if val == "" {
			return
		}
		clauses = append(clauses, clause)
		args = append(args, val)
	}
	addLike(f.Name, "LOWER(p.nom) LIKE ?")
	addLike(f.Surname1, "LOWER(p.cognom1) LIKE ?")
	addLike(f.Surname2, "LOWER(p.cognom2) LIKE ?")
	addLike(f.FullName, "LOWER(p.nom_complet) LIKE ?")
	addLike(f.BirthDate, "LOWER(p.data_naixement) LIKE ?")
	addLike(f.DeathDate, "LOWER(p.data_defuncio) LIKE ?")
	addLike(f.BirthPlace, "LOWER(p.lloc_naixement) LIKE ?")
	addLike(f.DeathPlace, "LOWER(p.lloc_defuncio) LIKE ?")
	addLike(f.Notes, "LOWER(p.notes) LIKE ?")
	addLike(f.Tree, "LOWER(a.nom) LIKE ?")
	addLike(f.ExternalID, "LOWER(p.external_id) LIKE ?")
	sex := strings.TrimSpace(strings.ToLower(f.Sex))
	if sex != "" {
		if sex == "unknown" {
			clauses = append(clauses, "(p.sexe IS NULL OR TRIM(p.sexe) = '')")
		} else {
			clauses = append(clauses, "LOWER(p.sexe) = ?")
			args = append(args, sex)
		}
	}
	addExact(f.Visibility, "p.visibility = ?")
	if f.HasMedia != nil {
		clauses = append(clauses, "p.has_media = ?")
		args = append(args, *f.HasMedia)
	}
	if f.Linked != nil {
		if *f.Linked {
			clauses = append(clauses, "EXISTS (SELECT 1 FROM espai_coincidencies c WHERE c.owner_user_id = p.owner_user_id AND c.persona_id = p.id AND c.target_type = 'persona' AND c.status = 'accepted')")
		} else {
			clauses = append(clauses, "NOT EXISTS (SELECT 1 FROM espai_coincidencies c WHERE c.owner_user_id = p.owner_user_id AND c.persona_id = p.id AND c.target_type = 'persona' AND c.status = 'accepted')")
		}
	}
	return clauses, args
}

func (h sqlHelper) countEspaiPersonesByOwnerDataFilters(ownerID int, filter EspaiPersonaDataFilter) (int, error) {
	query := `SELECT COUNT(*) FROM espai_persones p
        JOIN espai_arbres a ON a.id = p.arbre_id
        WHERE p.owner_user_id = ? AND a.owner_user_id = ?`
	args := []interface{}{ownerID, ownerID}
	clauses, moreArgs := h.buildEspaiPersonaDataFilters(filter)
	if len(clauses) > 0 {
		query += " AND " + strings.Join(clauses, " AND ")
		args = append(args, moreArgs...)
	}
	query = formatPlaceholders(h.style, query)
	var total int
	if err := h.db.QueryRow(query, args...).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (h sqlHelper) listEspaiPersonesByOwnerDataFilters(ownerID int, filter EspaiPersonaDataFilter, limit, offset int) ([]EspaiPersonaTreeRow, error) {
	query := `SELECT p.id, p.owner_user_id, p.arbre_id, p.external_id, p.nom, p.cognom1, p.cognom2, p.nom_complet, p.sexe, p.data_naixement, p.data_defuncio, p.lloc_naixement, p.lloc_defuncio, p.notes, p.has_media, p.visibility, p.status, p.created_at, p.updated_at, a.nom
        FROM espai_persones p
        JOIN espai_arbres a ON a.id = p.arbre_id
        WHERE p.owner_user_id = ? AND a.owner_user_id = ?`
	args := []interface{}{ownerID, ownerID}
	clauses, moreArgs := h.buildEspaiPersonaDataFilters(filter)
	if len(clauses) > 0 {
		query += " AND " + strings.Join(clauses, " AND ")
		args = append(args, moreArgs...)
	}
	sortKey := strings.TrimSpace(filter.Sort)
	sortDir := strings.ToLower(strings.TrimSpace(filter.SortDir))
	if sortDir != "asc" && sortDir != "desc" {
		sortDir = "asc"
	}
	order := ""
	switch sortKey {
	case "name":
		order = "p.nom " + sortDir
	case "surname1":
		order = "p.cognom1 " + sortDir
	case "surname2":
		order = "p.cognom2 " + sortDir
	case "full_name":
		order = "p.nom_complet " + sortDir
	case "sex":
		order = "p.sexe " + sortDir
	case "birth_date":
		order = "p.data_naixement " + sortDir
	case "death_date":
		order = "p.data_defuncio " + sortDir
	case "birth_place":
		order = "p.lloc_naixement " + sortDir
	case "death_place":
		order = "p.lloc_defuncio " + sortDir
	case "tree":
		order = "a.nom " + sortDir
	case "visibility":
		order = "p.visibility " + sortDir
	case "has_media":
		order = "p.has_media " + sortDir
	case "external_id":
		order = "p.external_id " + sortDir
	case "updated":
		order = "p.updated_at " + sortDir
	}
	if order == "" {
		order = "p.id DESC"
	} else {
		order = order + ", p.id DESC"
	}
	query += " ORDER BY " + order
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
		if offset > 0 {
			query += " OFFSET ?"
			args = append(args, offset)
		}
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiPersonaTreeRow
	for rows.Next() {
		var row EspaiPersonaTreeRow
		if err := rows.Scan(&row.ID, &row.OwnerUserID, &row.ArbreID, &row.ExternalID, &row.Nom, &row.Cognom1, &row.Cognom2, &row.NomComplet, &row.Sexe, &row.DataNaixement, &row.DataDefuncio, &row.LlocNaixement, &row.LlocDefuncio, &row.Notes, &row.HasMedia, &row.Visibility, &row.Status, &row.CreatedAt, &row.UpdatedAt, &row.TreeName); err != nil {
			return nil, err
		}
		res = append(res, row)
	}
	return res, nil
}

func (h sqlHelper) updateEspaiPersonaVisibility(id int, visibility string) error {
	visibility = strings.TrimSpace(visibility)
	if visibility == "" {
		visibility = "visible"
	}
	stmt := `UPDATE espai_persones SET visibility = ?, updated_at = ` + h.nowFun + ` WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, visibility, id)
	return err
}

func (h sqlHelper) createEspaiPrivacyAudit(a *EspaiPrivacyAudit) (int, error) {
	if a == nil {
		return 0, nil
	}
	action := strings.TrimSpace(a.Action)
	if action == "" {
		return 0, nil
	}
	stmt := `INSERT INTO espai_privacy_audit (owner_user_id, arbre_id, persona_id, action, from_visibility, to_visibility, ip, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	args := []interface{}{a.OwnerUserID, a.ArbreID, a.PersonaID, action, a.FromVisibility, a.ToVisibility, a.IP}
	if h.style == "postgres" {
		if err := h.db.QueryRow(stmt, args...).Scan(&a.ID); err != nil {
			return 0, err
		}
		return a.ID, nil
	}
	res, err := h.db.Exec(stmt, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		a.ID = int(id)
	}
	return a.ID, nil
}

func (h sqlHelper) createEspaiRelacio(r *EspaiRelacio) (int, error) {
	if r == nil {
		return 0, nil
	}
	stmt := `INSERT INTO espai_relacions (arbre_id, persona_id, related_persona_id, relation_type, notes, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	if h.style == "postgres" {
		if err := h.db.QueryRow(stmt, r.ArbreID, r.PersonaID, r.RelatedPersonaID, r.RelationType, r.Notes).Scan(&r.ID); err != nil {
			return 0, err
		}
		return r.ID, nil
	}
	res, err := h.db.Exec(stmt, r.ArbreID, r.PersonaID, r.RelatedPersonaID, r.RelationType, r.Notes)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		r.ID = int(id)
	}
	return r.ID, nil
}

func (h sqlHelper) listEspaiRelacionsByArbre(arbreID int) ([]EspaiRelacio, error) {
	query := `SELECT id, arbre_id, persona_id, related_persona_id, relation_type, notes, created_at, updated_at
        FROM espai_relacions WHERE arbre_id = ? ORDER BY id DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, arbreID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiRelacio
	for rows.Next() {
		var r EspaiRelacio
		if err := rows.Scan(&r.ID, &r.ArbreID, &r.PersonaID, &r.RelatedPersonaID, &r.RelationType, &r.Notes, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}

func (h sqlHelper) countEspaiRelacionsByArbre(arbreID int) (int, error) {
	query := `SELECT COUNT(*) FROM espai_relacions WHERE arbre_id = ?`
	query = formatPlaceholders(h.style, query)
	var total int
	if err := h.db.QueryRow(query, arbreID).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (h sqlHelper) countEspaiRelacionsByArbreType(arbreID int, relationType string) (int, error) {
	relationType = strings.TrimSpace(relationType)
	query := `SELECT COUNT(*) FROM espai_relacions WHERE arbre_id = ? AND relation_type = ?`
	query = formatPlaceholders(h.style, query)
	var total int
	if err := h.db.QueryRow(query, arbreID, relationType).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (h sqlHelper) createEspaiEvent(ev *EspaiEvent) (int, error) {
	if ev == nil {
		return 0, nil
	}
	stmt := `INSERT INTO espai_events (arbre_id, persona_id, external_id, event_type, event_role, event_date, event_place, description, source, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	args := []interface{}{ev.ArbreID, ev.PersonaID, ev.ExternalID, ev.EventType, ev.EventRole, ev.EventDate, ev.EventPlace, ev.Description, ev.Source}
	if h.style == "postgres" {
		if err := h.db.QueryRow(stmt, args...).Scan(&ev.ID); err != nil {
			return 0, err
		}
		return ev.ID, nil
	}
	res, err := h.db.Exec(stmt, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		ev.ID = int(id)
	}
	return ev.ID, nil
}

func (h sqlHelper) listEspaiEventsByPersona(personaID int) ([]EspaiEvent, error) {
	query := `SELECT id, arbre_id, persona_id, external_id, event_type, event_role, event_date, event_place, description, source, created_at, updated_at
        FROM espai_events WHERE persona_id = ? ORDER BY id DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, personaID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiEvent
	for rows.Next() {
		var ev EspaiEvent
		if err := rows.Scan(&ev.ID, &ev.ArbreID, &ev.PersonaID, &ev.ExternalID, &ev.EventType, &ev.EventRole, &ev.EventDate, &ev.EventPlace, &ev.Description, &ev.Source, &ev.CreatedAt, &ev.UpdatedAt); err != nil {
			return nil, err
		}
		res = append(res, ev)
	}
	return res, nil
}

func (h sqlHelper) deleteEspaiEventsByArbreSource(arbreID int, source string) error {
	source = strings.TrimSpace(source)
	stmt := `DELETE FROM espai_events WHERE arbre_id = ?`
	args := []interface{}{arbreID}
	if source != "" {
		stmt += " AND source = ?"
		args = append(args, source)
	}
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, args...)
	return err
}

func (h sqlHelper) createEspaiCoincidencia(c *EspaiCoincidencia) (int, error) {
	if c == nil {
		return 0, nil
	}
	status := strings.TrimSpace(c.Status)
	if status == "" {
		status = "pending"
	}
	stmt := `INSERT INTO espai_coincidencies (owner_user_id, arbre_id, persona_id, target_type, target_id, score, reason_json, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	args := []interface{}{c.OwnerUserID, c.ArbreID, c.PersonaID, c.TargetType, c.TargetID, c.Score, c.ReasonJSON, status}
	if h.style == "postgres" {
		if err := h.db.QueryRow(stmt, args...).Scan(&c.ID); err != nil {
			return 0, err
		}
		return c.ID, nil
	}
	res, err := h.db.Exec(stmt, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		c.ID = int(id)
	}
	return c.ID, nil
}

func (h sqlHelper) updateEspaiCoincidenciaStatus(id int, status string) error {
	stmt := `UPDATE espai_coincidencies SET status = ?, updated_at = ` + h.nowFun + ` WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, status, id)
	return err
}

func (h sqlHelper) getEspaiCoincidencia(id int) (*EspaiCoincidencia, error) {
	query := `SELECT id, owner_user_id, arbre_id, persona_id, target_type, target_id, score, reason_json, status, created_at, updated_at
        FROM espai_coincidencies WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	var c EspaiCoincidencia
	if err := h.db.QueryRow(query, id).Scan(&c.ID, &c.OwnerUserID, &c.ArbreID, &c.PersonaID, &c.TargetType, &c.TargetID, &c.Score, &c.ReasonJSON, &c.Status, &c.CreatedAt, &c.UpdatedAt); err != nil {
		return nil, err
	}
	return &c, nil
}

func (h sqlHelper) getEspaiCoincidenciaByTarget(ownerID, personaID int, targetType string, targetID int) (*EspaiCoincidencia, error) {
	query := `SELECT id, owner_user_id, arbre_id, persona_id, target_type, target_id, score, reason_json, status, created_at, updated_at
        FROM espai_coincidencies WHERE owner_user_id = ? AND persona_id = ? AND target_type = ? AND target_id = ?`
	query = formatPlaceholders(h.style, query)
	var c EspaiCoincidencia
	if err := h.db.QueryRow(query, ownerID, personaID, targetType, targetID).Scan(&c.ID, &c.OwnerUserID, &c.ArbreID, &c.PersonaID, &c.TargetType, &c.TargetID, &c.Score, &c.ReasonJSON, &c.Status, &c.CreatedAt, &c.UpdatedAt); err != nil {
		return nil, err
	}
	return &c, nil
}

func (h sqlHelper) listEspaiCoincidenciesByOwner(ownerID int) ([]EspaiCoincidencia, error) {
	query := `SELECT id, owner_user_id, arbre_id, persona_id, target_type, target_id, score, reason_json, status, created_at, updated_at
        FROM espai_coincidencies WHERE owner_user_id = ? ORDER BY updated_at DESC, id DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiCoincidencia
	for rows.Next() {
		var c EspaiCoincidencia
		if err := rows.Scan(&c.ID, &c.OwnerUserID, &c.ArbreID, &c.PersonaID, &c.TargetType, &c.TargetID, &c.Score, &c.ReasonJSON, &c.Status, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, err
		}
		res = append(res, c)
	}
	return res, nil
}

func (h sqlHelper) createEspaiCoincidenciaDecision(d *EspaiCoincidenciaDecision) (int, error) {
	if d == nil {
		return 0, nil
	}
	stmt := `INSERT INTO espai_decisions_coincidencia (coincidencia_id, decision, decided_by, notes, created_at)
        VALUES (?, ?, ?, ?, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	if h.style == "postgres" {
		if err := h.db.QueryRow(stmt, d.CoincidenciaID, d.Decision, d.DecidedBy, d.Notes).Scan(&d.ID); err != nil {
			return 0, err
		}
		return d.ID, nil
	}
	res, err := h.db.Exec(stmt, d.CoincidenciaID, d.Decision, d.DecidedBy, d.Notes)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		d.ID = int(id)
	}
	return d.ID, nil
}

func (h sqlHelper) listEspaiCoincidenciaDecisions(coincidenciaID int) ([]EspaiCoincidenciaDecision, error) {
	query := `SELECT id, coincidencia_id, decision, decided_by, notes, created_at
        FROM espai_decisions_coincidencia WHERE coincidencia_id = ? ORDER BY created_at DESC, id DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, coincidenciaID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiCoincidenciaDecision
	for rows.Next() {
		var d EspaiCoincidenciaDecision
		if err := rows.Scan(&d.ID, &d.CoincidenciaID, &d.Decision, &d.DecidedBy, &d.Notes, &d.CreatedAt); err != nil {
			return nil, err
		}
		res = append(res, d)
	}
	return res, nil
}

func (h sqlHelper) createEspaiIntegracioGramps(i *EspaiIntegracioGramps) (int, error) {
	if i == nil {
		return 0, nil
	}
	status := strings.TrimSpace(i.Status)
	if status == "" {
		status = "connected"
	}
	stmt := `INSERT INTO espai_integracions_gramps (owner_user_id, arbre_id, base_url, username, token, status, last_sync_at, last_error, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	args := []interface{}{i.OwnerUserID, i.ArbreID, i.BaseURL, i.Username, i.Token, status, i.LastSyncAt, i.LastError}
	if h.style == "postgres" {
		if err := h.db.QueryRow(stmt, args...).Scan(&i.ID); err != nil {
			return 0, err
		}
		return i.ID, nil
	}
	res, err := h.db.Exec(stmt, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		i.ID = int(id)
	}
	return i.ID, nil
}

func (h sqlHelper) updateEspaiIntegracioGramps(i *EspaiIntegracioGramps) error {
	if i == nil {
		return nil
	}
	stmt := `UPDATE espai_integracions_gramps
        SET arbre_id = ?, base_url = ?, username = ?, token = ?, status = ?, last_sync_at = ?, last_error = ?, updated_at = ` + h.nowFun + `
        WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, i.ArbreID, i.BaseURL, i.Username, i.Token, i.Status, i.LastSyncAt, i.LastError, i.ID)
	return err
}

func (h sqlHelper) getEspaiIntegracioGramps(id int) (*EspaiIntegracioGramps, error) {
	query := `SELECT id, owner_user_id, arbre_id, base_url, username, token, status, last_sync_at, last_error, created_at, updated_at
        FROM espai_integracions_gramps WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	var i EspaiIntegracioGramps
	if err := h.db.QueryRow(query, id).Scan(&i.ID, &i.OwnerUserID, &i.ArbreID, &i.BaseURL, &i.Username, &i.Token, &i.Status, &i.LastSyncAt, &i.LastError, &i.CreatedAt, &i.UpdatedAt); err != nil {
		return nil, err
	}
	return &i, nil
}

func (h sqlHelper) listEspaiIntegracionsGrampsByOwner(ownerID int) ([]EspaiIntegracioGramps, error) {
	query := `SELECT id, owner_user_id, arbre_id, base_url, username, token, status, last_sync_at, last_error, created_at, updated_at
        FROM espai_integracions_gramps WHERE owner_user_id = ? ORDER BY updated_at DESC, id DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiIntegracioGramps
	for rows.Next() {
		var i EspaiIntegracioGramps
		if err := rows.Scan(&i.ID, &i.OwnerUserID, &i.ArbreID, &i.BaseURL, &i.Username, &i.Token, &i.Status, &i.LastSyncAt, &i.LastError, &i.CreatedAt, &i.UpdatedAt); err != nil {
			return nil, err
		}
		res = append(res, i)
	}
	return res, nil
}

func (h sqlHelper) listEspaiIntegracionsGramps() ([]EspaiIntegracioGramps, error) {
	query := `SELECT id, owner_user_id, arbre_id, base_url, username, token, status, last_sync_at, last_error, created_at, updated_at
        FROM espai_integracions_gramps ORDER BY updated_at DESC, id DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiIntegracioGramps
	for rows.Next() {
		var i EspaiIntegracioGramps
		if err := rows.Scan(&i.ID, &i.OwnerUserID, &i.ArbreID, &i.BaseURL, &i.Username, &i.Token, &i.Status, &i.LastSyncAt, &i.LastError, &i.CreatedAt, &i.UpdatedAt); err != nil {
			return nil, err
		}
		res = append(res, i)
	}
	return res, nil
}

func (h sqlHelper) createEspaiIntegracioGrampsLog(l *EspaiIntegracioGrampsLog) (int, error) {
	if l == nil {
		return 0, nil
	}
	stmt := `INSERT INTO espai_integracions_gramps_logs (integracio_id, status, message, created_at)
        VALUES (?, ?, ?, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	if h.style == "postgres" {
		if err := h.db.QueryRow(stmt, l.IntegracioID, l.Status, l.Message).Scan(&l.ID); err != nil {
			return 0, err
		}
		return l.ID, nil
	}
	res, err := h.db.Exec(stmt, l.IntegracioID, l.Status, l.Message)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		l.ID = int(id)
	}
	return l.ID, nil
}

func (h sqlHelper) listEspaiIntegracioGrampsLogs(integracioID int, limit int) ([]EspaiIntegracioGrampsLog, error) {
	query := `SELECT id, integracio_id, status, message, created_at
        FROM espai_integracions_gramps_logs WHERE integracio_id = ? ORDER BY created_at DESC, id DESC`
	args := []interface{}{integracioID}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiIntegracioGrampsLog
	for rows.Next() {
		var l EspaiIntegracioGrampsLog
		if err := rows.Scan(&l.ID, &l.IntegracioID, &l.Status, &l.Message, &l.CreatedAt); err != nil {
			return nil, err
		}
		res = append(res, l)
	}
	return res, nil
}

func (h sqlHelper) createEspaiNotification(n *EspaiNotification) (int, error) {
	if n == nil {
		return 0, nil
	}
	status := strings.TrimSpace(n.Status)
	if status == "" {
		status = "unread"
	}
	stmt := `INSERT INTO espai_notifications (user_id, kind, title, body, url, status, object_type, object_id, group_id, tree_id, dedupe_key, created_at, read_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ?)`
	if h.style == "postgres" {
		stmt += " ON CONFLICT (user_id, dedupe_key) DO NOTHING RETURNING id"
	} else if h.style == "mysql" {
		stmt = `INSERT IGNORE INTO espai_notifications (user_id, kind, title, body, url, status, object_type, object_id, group_id, tree_id, dedupe_key, created_at, read_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ?)`
	} else {
		stmt += " ON CONFLICT(user_id, dedupe_key) DO NOTHING"
	}
	stmt = formatPlaceholders(h.style, stmt)
	args := []interface{}{n.UserID, n.Kind, n.Title, n.Body, n.URL, status, n.ObjectType, n.ObjectID, n.GroupID, n.TreeID, n.DedupeKey, n.ReadAt}
	if h.style == "postgres" {
		if err := h.db.QueryRow(stmt, args...).Scan(&n.ID); err != nil {
			if err == sql.ErrNoRows {
				return 0, nil
			}
			return 0, err
		}
		return n.ID, nil
	}
	res, err := h.db.Exec(stmt, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		n.ID = int(id)
	}
	return n.ID, nil
}

func (h sqlHelper) listEspaiNotificationsByUser(userID int, status string, limit int) ([]EspaiNotification, error) {
	query := `SELECT id, user_id, kind, title, body, url, status, object_type, object_id, group_id, tree_id, dedupe_key, created_at, read_at
        FROM espai_notifications WHERE user_id = ?`
	args := []interface{}{userID}
	if strings.TrimSpace(status) != "" {
		query += " AND status = ?"
		args = append(args, status)
	}
	query += " ORDER BY created_at DESC, id DESC"
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiNotification
	for rows.Next() {
		var n EspaiNotification
		if err := rows.Scan(&n.ID, &n.UserID, &n.Kind, &n.Title, &n.Body, &n.URL, &n.Status, &n.ObjectType, &n.ObjectID, &n.GroupID, &n.TreeID, &n.DedupeKey, &n.CreatedAt, &n.ReadAt); err != nil {
			return nil, err
		}
		res = append(res, n)
	}
	return res, nil
}

func (h sqlHelper) markEspaiNotificationRead(id int, userID int) error {
	stmt := `UPDATE espai_notifications SET status = 'read', read_at = ` + h.nowFun + ` WHERE id = ? AND user_id = ? AND status != 'read'`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, id, userID)
	return err
}

func (h sqlHelper) markEspaiNotificationsReadAll(userID int) error {
	stmt := `UPDATE espai_notifications SET status = 'read', read_at = ` + h.nowFun + ` WHERE user_id = ? AND status = 'unread'`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, userID)
	return err
}

func (h sqlHelper) getEspaiNotificationPref(userID int) (*EspaiNotificationPref, error) {
	query := `SELECT user_id, freq, types_json, updated_at FROM espai_notification_prefs WHERE user_id = ?`
	query = formatPlaceholders(h.style, query)
	var p EspaiNotificationPref
	if err := h.db.QueryRow(query, userID).Scan(&p.UserID, &p.Freq, &p.TypesJSON, &p.UpdatedAt); err != nil {
		return nil, err
	}
	return &p, nil
}

func (h sqlHelper) upsertEspaiNotificationPref(p *EspaiNotificationPref) error {
	if p == nil {
		return nil
	}
	stmt := `INSERT INTO espai_notification_prefs (user_id, freq, types_json, updated_at)
        VALUES (?, ?, ?, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " ON CONFLICT (user_id) DO UPDATE SET freq = excluded.freq, types_json = excluded.types_json, updated_at = " + h.nowFun
	} else if h.style == "mysql" {
		stmt += " ON DUPLICATE KEY UPDATE freq = VALUES(freq), types_json = VALUES(types_json), updated_at = " + h.nowFun
	} else {
		stmt += " ON CONFLICT(user_id) DO UPDATE SET freq = excluded.freq, types_json = excluded.types_json, updated_at = " + h.nowFun
	}
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, p.UserID, p.Freq, p.TypesJSON)
	return err
}

func (h sqlHelper) createEspaiGrup(g *EspaiGrup) (int, error) {
	if g == nil {
		return 0, nil
	}
	status := strings.TrimSpace(g.Status)
	if status == "" {
		status = "active"
	}
	stmt := `INSERT INTO espai_grups (owner_user_id, nom, descripcio, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	if h.style == "postgres" {
		if err := h.db.QueryRow(stmt, g.OwnerUserID, g.Nom, g.Descripcio, status).Scan(&g.ID); err != nil {
			return 0, err
		}
		return g.ID, nil
	}
	res, err := h.db.Exec(stmt, g.OwnerUserID, g.Nom, g.Descripcio, status)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		g.ID = int(id)
	}
	return g.ID, nil
}

func (h sqlHelper) getEspaiGrup(id int) (*EspaiGrup, error) {
	query := `SELECT id, owner_user_id, nom, descripcio, status, created_at, updated_at
        FROM espai_grups WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	var g EspaiGrup
	if err := h.db.QueryRow(query, id).Scan(&g.ID, &g.OwnerUserID, &g.Nom, &g.Descripcio, &g.Status, &g.CreatedAt, &g.UpdatedAt); err != nil {
		return nil, err
	}
	return &g, nil
}

func (h sqlHelper) listEspaiGrupsByOwner(ownerID int) ([]EspaiGrup, error) {
	query := `SELECT id, owner_user_id, nom, descripcio, status, created_at, updated_at
        FROM espai_grups WHERE owner_user_id = ? ORDER BY updated_at DESC, id DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiGrup
	for rows.Next() {
		var g EspaiGrup
		if err := rows.Scan(&g.ID, &g.OwnerUserID, &g.Nom, &g.Descripcio, &g.Status, &g.CreatedAt, &g.UpdatedAt); err != nil {
			return nil, err
		}
		res = append(res, g)
	}
	return res, nil
}

func (h sqlHelper) listEspaiGrupsByUser(userID int) ([]EspaiGrup, error) {
	query := `SELECT g.id, g.owner_user_id, g.nom, g.descripcio, g.status, g.created_at, g.updated_at
        FROM espai_grups g
        JOIN espai_grups_membres m ON m.grup_id = g.id
        WHERE m.user_id = ? AND m.status != 'removed'
        ORDER BY g.updated_at DESC, g.id DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiGrup
	for rows.Next() {
		var g EspaiGrup
		if err := rows.Scan(&g.ID, &g.OwnerUserID, &g.Nom, &g.Descripcio, &g.Status, &g.CreatedAt, &g.UpdatedAt); err != nil {
			return nil, err
		}
		res = append(res, g)
	}
	return res, nil
}

func (h sqlHelper) getEspaiGrupMembre(grupID, userID int) (*EspaiGrupMembre, error) {
	query := `SELECT id, grup_id, user_id, role, status, joined_at, created_at
        FROM espai_grups_membres WHERE grup_id = ? AND user_id = ?`
	query = formatPlaceholders(h.style, query)
	var m EspaiGrupMembre
	if err := h.db.QueryRow(query, grupID, userID).Scan(&m.ID, &m.GrupID, &m.UserID, &m.Role, &m.Status, &m.JoinedAt, &m.CreatedAt); err != nil {
		return nil, err
	}
	return &m, nil
}

func (h sqlHelper) updateEspaiGrupMembre(m *EspaiGrupMembre) error {
	if m == nil {
		return nil
	}
	stmt := `UPDATE espai_grups_membres
        SET role = ?, status = ?, joined_at = ?
        WHERE grup_id = ? AND user_id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, m.Role, m.Status, m.JoinedAt, m.GrupID, m.UserID)
	return err
}

func (h sqlHelper) addEspaiGrupMembre(m *EspaiGrupMembre) (int, error) {
	if m == nil {
		return 0, nil
	}
	role := strings.TrimSpace(m.Role)
	if role == "" {
		role = "member"
	}
	status := strings.TrimSpace(m.Status)
	if status == "" {
		status = "active"
	}
	stmt := `INSERT INTO espai_grups_membres (grup_id, user_id, role, status, joined_at, created_at)
        VALUES (?, ?, ?, ?, ?, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	args := []interface{}{m.GrupID, m.UserID, role, status, m.JoinedAt}
	if h.style == "postgres" {
		if err := h.db.QueryRow(stmt, args...).Scan(&m.ID); err != nil {
			return 0, err
		}
		return m.ID, nil
	}
	res, err := h.db.Exec(stmt, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		m.ID = int(id)
	}
	return m.ID, nil
}

func (h sqlHelper) listEspaiGrupMembres(grupID int) ([]EspaiGrupMembre, error) {
	query := `SELECT id, grup_id, user_id, role, status, joined_at, created_at
        FROM espai_grups_membres WHERE grup_id = ? ORDER BY created_at DESC, id DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, grupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiGrupMembre
	for rows.Next() {
		var m EspaiGrupMembre
		if err := rows.Scan(&m.ID, &m.GrupID, &m.UserID, &m.Role, &m.Status, &m.JoinedAt, &m.CreatedAt); err != nil {
			return nil, err
		}
		res = append(res, m)
	}
	return res, nil
}

func (h sqlHelper) addEspaiGrupArbre(a *EspaiGrupArbre) (int, error) {
	if a == nil {
		return 0, nil
	}
	status := strings.TrimSpace(a.Status)
	if status == "" {
		status = "active"
	}
	stmt := `INSERT INTO espai_grups_arbres (grup_id, arbre_id, status, created_at)
        VALUES (?, ?, ?, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	if h.style == "postgres" {
		if err := h.db.QueryRow(stmt, a.GrupID, a.ArbreID, status).Scan(&a.ID); err != nil {
			return 0, err
		}
		return a.ID, nil
	}
	res, err := h.db.Exec(stmt, a.GrupID, a.ArbreID, status)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		a.ID = int(id)
	}
	return a.ID, nil
}

func (h sqlHelper) listEspaiGrupArbres(grupID int) ([]EspaiGrupArbre, error) {
	query := `SELECT id, grup_id, arbre_id, status, created_at
        FROM espai_grups_arbres WHERE grup_id = ? ORDER BY created_at DESC, id DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, grupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiGrupArbre
	for rows.Next() {
		var a EspaiGrupArbre
		if err := rows.Scan(&a.ID, &a.GrupID, &a.ArbreID, &a.Status, &a.CreatedAt); err != nil {
			return nil, err
		}
		res = append(res, a)
	}
	return res, nil
}

func (h sqlHelper) updateEspaiGrupArbreStatus(grupID, arbreID int, status string) error {
	stmt := `UPDATE espai_grups_arbres SET status = ? WHERE grup_id = ? AND arbre_id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, status, grupID, arbreID)
	return err
}

func (h sqlHelper) createEspaiGrupConflicte(c *EspaiGrupConflicte) (int, error) {
	if c == nil {
		return 0, nil
	}
	status := strings.TrimSpace(c.Status)
	if status == "" {
		status = "pending"
	}
	stmt := `INSERT INTO espai_grups_conflictes (grup_id, arbre_id, conflict_type, object_id, status, summary, details_json, resolved_at, resolved_by, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	args := []interface{}{c.GrupID, c.ArbreID, c.ConflictType, c.ObjectID, status, c.Summary, c.DetailsJSON, c.ResolvedAt, c.ResolvedBy}
	if h.style == "postgres" {
		if err := h.db.QueryRow(stmt, args...).Scan(&c.ID); err != nil {
			return 0, err
		}
		return c.ID, nil
	}
	res, err := h.db.Exec(stmt, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		c.ID = int(id)
	}
	return c.ID, nil
}

func (h sqlHelper) updateEspaiGrupConflicteStatus(id int, status string, resolvedBy *int) error {
	var resolvedByVal sql.NullInt64
	if resolvedBy != nil {
		resolvedByVal = sql.NullInt64{Int64: int64(*resolvedBy), Valid: true}
	}
	stmt := `UPDATE espai_grups_conflictes
        SET status = ?, resolved_by = ?, resolved_at = CASE WHEN ? = 'resolved' THEN ` + h.nowFun + ` ELSE NULL END, updated_at = ` + h.nowFun + `
        WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, status, resolvedByVal, status, id)
	return err
}

func (h sqlHelper) listEspaiGrupConflictes(grupID int) ([]EspaiGrupConflicte, error) {
	query := `SELECT id, grup_id, arbre_id, conflict_type, object_id, status, summary, details_json, resolved_at, resolved_by, created_at, updated_at
        FROM espai_grups_conflictes WHERE grup_id = ? ORDER BY updated_at DESC, id DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, grupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiGrupConflicte
	for rows.Next() {
		var c EspaiGrupConflicte
		if err := rows.Scan(&c.ID, &c.GrupID, &c.ArbreID, &c.ConflictType, &c.ObjectID, &c.Status, &c.Summary, &c.DetailsJSON, &c.ResolvedAt, &c.ResolvedBy, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, err
		}
		res = append(res, c)
	}
	return res, nil
}

func (h sqlHelper) createEspaiGrupCanvi(c *EspaiGrupCanvi) (int, error) {
	if c == nil {
		return 0, nil
	}
	stmt := `INSERT INTO espai_grups_canvis (grup_id, actor_id, action, object_type, object_id, payload_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	args := []interface{}{c.GrupID, c.ActorID, c.Action, c.ObjectType, c.ObjectID, c.PayloadJSON}
	if h.style == "postgres" {
		if err := h.db.QueryRow(stmt, args...).Scan(&c.ID); err != nil {
			return 0, err
		}
		return c.ID, nil
	}
	res, err := h.db.Exec(stmt, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		c.ID = int(id)
	}
	return c.ID, nil
}

func (h sqlHelper) listEspaiGrupCanvis(grupID int, limit int) ([]EspaiGrupCanvi, error) {
	query := `SELECT id, grup_id, actor_id, action, object_type, object_id, payload_json, created_at
        FROM espai_grups_canvis WHERE grup_id = ? ORDER BY created_at DESC, id DESC`
	args := []interface{}{grupID}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EspaiGrupCanvi
	for rows.Next() {
		var c EspaiGrupCanvi
		if err := rows.Scan(&c.ID, &c.GrupID, &c.ActorID, &c.Action, &c.ObjectType, &c.ObjectID, &c.PayloadJSON, &c.CreatedAt); err != nil {
			return nil, err
		}
		res = append(res, c)
	}
	return res, nil
}

func nullableString(val string) sql.NullString {
	if strings.TrimSpace(val) == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: val, Valid: true}
}
