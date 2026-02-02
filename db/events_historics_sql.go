package db

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

type eventHistoricScanner interface {
	Scan(dest ...interface{}) error
}

const eventHistoricSelectFields = `
        e.id, e.titol, e.slug, e.tipus, e.resum, e.descripcio,
        e.data_inici, e.data_fi, e.data_inici_aprox, e.data_fi_aprox,
        e.precisio, e.fonts, e.created_by, e.moderation_status,
        e.moderated_by, e.moderated_at, e.moderation_notes,
        e.created_at, e.updated_at`

func scanEventHistoric(scanner eventHistoricScanner) (EventHistoric, error) {
	var e EventHistoric
	var resum sql.NullString
	var descripcio sql.NullString
	var dataInici sql.NullString
	var dataFi sql.NullString
	var precisio sql.NullString
	var fonts sql.NullString
	var notes sql.NullString
	if err := scanner.Scan(
		&e.ID,
		&e.Titol,
		&e.Slug,
		&e.Tipus,
		&resum,
		&descripcio,
		&dataInici,
		&dataFi,
		&e.DataIniciAprox,
		&e.DataFiAprox,
		&precisio,
		&fonts,
		&e.CreatedBy,
		&e.ModerationStatus,
		&e.ModeratedBy,
		&e.ModeratedAt,
		&notes,
		&e.CreatedAt,
		&e.UpdatedAt,
	); err != nil {
		return e, err
	}
	e.Resum = resum.String
	e.Descripcio = descripcio.String
	e.DataInici = dataInici.String
	e.DataFi = dataFi.String
	e.Precisio = precisio.String
	e.Fonts = fonts.String
	e.ModerationNotes = notes.String
	return e, nil
}

func toNullString(value string) sql.NullString {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: trimmed, Valid: true}
}

func appendEventHistoricFilterClauses(style string, clauses []string, args []interface{}, filter EventHistoricFilter) ([]string, []interface{}) {
	if strings.TrimSpace(filter.Tipus) != "" {
		clauses = append(clauses, "e.tipus = ?")
		args = append(args, strings.TrimSpace(filter.Tipus))
	}
	if strings.TrimSpace(filter.Status) != "" {
		clauses = append(clauses, "e.moderation_status = ?")
		args = append(args, strings.TrimSpace(filter.Status))
	}
	if strings.TrimSpace(filter.Query) != "" {
		q := "%" + strings.TrimSpace(filter.Query) + "%"
		if style == "postgres" {
			clauses = append(clauses, "(e.titol ILIKE ? OR e.resum ILIKE ? OR e.descripcio ILIKE ?)")
			args = append(args, q, q, q)
		} else {
			q = strings.ToLower(q)
			clauses = append(clauses, "(LOWER(e.titol) LIKE ? OR LOWER(e.resum) LIKE ? OR LOWER(e.descripcio) LIKE ?)")
			args = append(args, q, q, q)
		}
	}
	if !filter.From.IsZero() {
		clauses = append(clauses, "COALESCE(e.data_inici, e.data_fi) >= ?")
		args = append(args, filter.From.Format("2006-01-02"))
	}
	if !filter.To.IsZero() {
		clauses = append(clauses, "COALESCE(e.data_inici, e.data_fi) <= ?")
		args = append(args, filter.To.Format("2006-01-02"))
	}
	if filter.OnlyWithDates {
		clauses = append(clauses, "(e.data_inici IS NOT NULL OR e.data_fi IS NOT NULL)")
	}
	return clauses, args
}

func eventHistoricOrderBy(filter EventHistoricFilter, hasIntensity bool) string {
	order := strings.TrimSpace(filter.OrderBy)
	switch order {
	case "data_asc":
		return " ORDER BY CASE WHEN COALESCE(e.data_inici, e.data_fi) IS NULL THEN 1 ELSE 0 END, COALESCE(e.data_inici, e.data_fi) ASC, e.id ASC"
	case "data_desc":
		return " ORDER BY COALESCE(e.data_inici, e.data_fi) DESC, e.id DESC"
	case "intensitat_desc":
		if hasIntensity {
			return " ORDER BY i.max_intensitat DESC, e.id DESC"
		}
	case "recent":
		return " ORDER BY e.created_at DESC, e.id DESC"
	}
	return " ORDER BY COALESCE(e.data_inici, e.data_fi) DESC, e.id DESC"
}

func (h sqlHelper) createEventHistoric(e *EventHistoric) (int, error) {
	if e == nil {
		return 0, errors.New("event nil")
	}
	e.Titol = strings.TrimSpace(e.Titol)
	e.Slug = strings.TrimSpace(e.Slug)
	e.Tipus = strings.TrimSpace(e.Tipus)
	if strings.TrimSpace(e.ModerationStatus) == "" {
		e.ModerationStatus = "pendent"
	}
	if err := ValidateEventHistoric(e); err != nil {
		return 0, err
	}
	query := `
        INSERT INTO events_historics (
            titol, slug, tipus, resum, descripcio, data_inici, data_fi,
            data_inici_aprox, data_fi_aprox, precisio, fonts, created_by,
            moderation_status, moderated_by, moderated_at, moderation_notes,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		query += " RETURNING id"
	}
	query = formatPlaceholders(h.style, query)
	args := []interface{}{
		e.Titol,
		e.Slug,
		e.Tipus,
		toNullString(e.Resum),
		toNullString(e.Descripcio),
		toNullString(e.DataInici),
		toNullString(e.DataFi),
		e.DataIniciAprox,
		e.DataFiAprox,
		toNullString(e.Precisio),
		toNullString(e.Fonts),
		e.CreatedBy,
		e.ModerationStatus,
		e.ModeratedBy,
		e.ModeratedAt,
		toNullString(e.ModerationNotes),
	}
	if h.style == "postgres" {
		if err := h.db.QueryRow(query, args...).Scan(&e.ID); err != nil {
			return 0, err
		}
		return e.ID, nil
	}
	res, err := h.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		e.ID = int(id)
	}
	return e.ID, nil
}

func (h sqlHelper) getEventHistoric(id int) (*EventHistoric, error) {
	query := `SELECT ` + eventHistoricSelectFields + ` FROM events_historics e WHERE e.id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, id)
	item, err := scanEventHistoric(row)
	if err != nil {
		return nil, err
	}
	return &item, nil
}

func (h sqlHelper) getEventHistoricBySlug(slug string) (*EventHistoric, error) {
	query := `SELECT ` + eventHistoricSelectFields + ` FROM events_historics e WHERE e.slug = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, strings.TrimSpace(slug))
	item, err := scanEventHistoric(row)
	if err != nil {
		return nil, err
	}
	return &item, nil
}

func (h sqlHelper) updateEventHistoric(e *EventHistoric) error {
	if e == nil {
		return errors.New("event nil")
	}
	e.Titol = strings.TrimSpace(e.Titol)
	e.Slug = strings.TrimSpace(e.Slug)
	e.Tipus = strings.TrimSpace(e.Tipus)
	if strings.TrimSpace(e.ModerationStatus) == "" {
		e.ModerationStatus = "pendent"
	}
	if err := ValidateEventHistoric(e); err != nil {
		return err
	}
	query := `
        UPDATE events_historics
        SET titol = ?, slug = ?, tipus = ?, resum = ?, descripcio = ?, data_inici = ?, data_fi = ?,
            data_inici_aprox = ?, data_fi_aprox = ?, precisio = ?, fonts = ?,
            moderation_status = ?, moderated_by = ?, moderated_at = ?, moderation_notes = ?,
            updated_at = ` + h.nowFun + `
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(
		query,
		e.Titol,
		e.Slug,
		e.Tipus,
		toNullString(e.Resum),
		toNullString(e.Descripcio),
		toNullString(e.DataInici),
		toNullString(e.DataFi),
		e.DataIniciAprox,
		e.DataFiAprox,
		toNullString(e.Precisio),
		toNullString(e.Fonts),
		e.ModerationStatus,
		e.ModeratedBy,
		e.ModeratedAt,
		toNullString(e.ModerationNotes),
		e.ID,
	)
	return err
}

func (h sqlHelper) listEventsHistoric(filter EventHistoricFilter) ([]EventHistoric, error) {
	query := `SELECT ` + eventHistoricSelectFields + ` FROM events_historics e`
	clauses := []string{}
	args := []interface{}{}
	useImpact := strings.TrimSpace(filter.ImpacteTipus) != "" || filter.IntensitatMin > 0 || strings.TrimSpace(filter.OrderBy) == "intensitat_desc"
	if useImpact {
		subClauses := []string{}
		subArgs := []interface{}{}
		if strings.TrimSpace(filter.ImpacteTipus) != "" {
			subClauses = append(subClauses, "impacte_tipus = ?")
			subArgs = append(subArgs, strings.TrimSpace(filter.ImpacteTipus))
		}
		if filter.IntensitatMin > 0 {
			subClauses = append(subClauses, "intensitat >= ?")
			subArgs = append(subArgs, filter.IntensitatMin)
		}
		subQuery := "SELECT event_id, MAX(intensitat) AS max_intensitat FROM events_historics_impactes"
		if len(subClauses) > 0 {
			subQuery += " WHERE " + strings.Join(subClauses, " AND ")
		}
		subQuery += " GROUP BY event_id"
		query += " JOIN (" + subQuery + ") i ON i.event_id = e.id"
		args = append(args, subArgs...)
	}
	clauses, args = appendEventHistoricFilterClauses(h.style, clauses, args, filter)
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += eventHistoricOrderBy(filter, useImpact)
	if filter.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, filter.Limit)
		if filter.Offset > 0 {
			query += " OFFSET ?"
			args = append(args, filter.Offset)
		}
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EventHistoric
	for rows.Next() {
		item, err := scanEventHistoric(rows)
		if err != nil {
			return nil, err
		}
		res = append(res, item)
	}
	return res, nil
}

func (h sqlHelper) updateEventHistoricModeracio(id int, estat, notes string, moderatorID int) error {
	estat = strings.TrimSpace(estat)
	if estat == "" {
		return fmt.Errorf("moderation_status required")
	}
	if _, ok := eventHistoricModerationStatus[estat]; !ok {
		return fmt.Errorf("moderation_status invalid")
	}
	stmt := `UPDATE events_historics SET moderation_status = ?, moderation_notes = ?, moderated_by = ?, moderated_at = ?, updated_at = ? WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	now := time.Now()
	_, err := h.db.Exec(stmt, estat, strings.TrimSpace(notes), moderatorID, now, now, id)
	return err
}

func (h sqlHelper) listEventImpacts(eventID int) ([]EventHistoricImpact, error) {
	query := `
        SELECT id, event_id, scope_type, scope_id, impacte_tipus, intensitat, notes, created_by, created_at, updated_at
        FROM events_historics_impactes
        WHERE event_id = ?
        ORDER BY id`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EventHistoricImpact
	for rows.Next() {
		var item EventHistoricImpact
		var notes sql.NullString
		if err := rows.Scan(
			&item.ID,
			&item.EventID,
			&item.ScopeType,
			&item.ScopeID,
			&item.ImpacteTipus,
			&item.Intensitat,
			&notes,
			&item.CreatedBy,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		item.Notes = notes.String
		res = append(res, item)
	}
	return res, nil
}

func (h sqlHelper) eventHistoricScopeExists(scopeType string, scopeID int) (bool, error) {
	if scopeID <= 0 {
		return false, nil
	}
	table := ""
	switch scopeType {
	case "pais":
		table = "paisos"
	case "nivell_admin":
		table = "nivells_administratius"
	case "municipi":
		table = "municipis"
	case "entitat_eclesiastica":
		table = "arquebisbats"
	default:
		return false, fmt.Errorf("scope_type invalid")
	}
	query := fmt.Sprintf("SELECT 1 FROM %s WHERE id = ? LIMIT 1", table)
	query = formatPlaceholders(h.style, query)
	var dummy int
	if err := h.db.QueryRow(query, scopeID).Scan(&dummy); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (h sqlHelper) replaceEventImpacts(eventID int, impacts []EventHistoricImpact) error {
	if eventID <= 0 {
		return fmt.Errorf("event_id invalid")
	}
	for idx := range impacts {
		impact := impacts[idx]
		if err := ValidateEventHistoricImpact(&impact); err != nil {
			return err
		}
		ok, err := h.eventHistoricScopeExists(strings.TrimSpace(impact.ScopeType), impact.ScopeID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("scope_id not found")
		}
	}
	tx, err := h.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	delStmt := formatPlaceholders(h.style, `DELETE FROM events_historics_impactes WHERE event_id = ?`)
	if _, err := tx.Exec(delStmt, eventID); err != nil {
		return err
	}
	if len(impacts) == 0 {
		return tx.Commit()
	}
	insertStmt := `
        INSERT INTO events_historics_impactes
            (event_id, scope_type, scope_id, impacte_tipus, intensitat, notes, created_by, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	insertStmt = formatPlaceholders(h.style, insertStmt)
	for _, impact := range impacts {
		if _, err := tx.Exec(
			insertStmt,
			eventID,
			strings.TrimSpace(impact.ScopeType),
			impact.ScopeID,
			strings.TrimSpace(impact.ImpacteTipus),
			impact.Intensitat,
			toNullString(impact.Notes),
			impact.CreatedBy,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (h sqlHelper) listEventsByScope(scopeType string, scopeID int, filter EventHistoricFilter) ([]EventHistoric, error) {
	scopeType = strings.TrimSpace(scopeType)
	if scopeType == "" {
		return nil, fmt.Errorf("scope_type required")
	}
	if !isEventHistoricScopeType(scopeType) {
		return nil, fmt.Errorf("scope_type invalid")
	}
	if scopeID <= 0 {
		return nil, fmt.Errorf("scope_id invalid")
	}
	subClauses := []string{"scope_type = ?", "scope_id = ?"}
	args := []interface{}{scopeType, scopeID}
	if strings.TrimSpace(filter.ImpacteTipus) != "" {
		subClauses = append(subClauses, "impacte_tipus = ?")
		args = append(args, strings.TrimSpace(filter.ImpacteTipus))
	}
	if filter.IntensitatMin > 0 {
		subClauses = append(subClauses, "intensitat >= ?")
		args = append(args, filter.IntensitatMin)
	}
	subQuery := `SELECT event_id, MAX(intensitat) AS max_intensitat FROM events_historics_impactes WHERE ` + strings.Join(subClauses, " AND ") + ` GROUP BY event_id`
	query := `SELECT ` + eventHistoricSelectFields + `
        FROM events_historics e
        JOIN (` + subQuery + `) i
          ON i.event_id = e.id`
	clauses := []string{}
	clauses, args = appendEventHistoricFilterClauses(h.style, clauses, args, filter)
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += eventHistoricOrderBy(filter, true)
	if filter.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, filter.Limit)
		if filter.Offset > 0 {
			query += " OFFSET ?"
			args = append(args, filter.Offset)
		}
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []EventHistoric
	for rows.Next() {
		item, err := scanEventHistoric(rows)
		if err != nil {
			return nil, err
		}
		res = append(res, item)
	}
	return res, nil
}
