package db

import (
	"database/sql"
	"strings"
)

func (h sqlHelper) listExternalSitesActive() ([]ExternalSite, error) {
	active := true
	return h.listExternalSites(&active)
}

func (h sqlHelper) listExternalSitesAll() ([]ExternalSite, error) {
	return h.listExternalSites(nil)
}

func (h sqlHelper) listExternalSites(isActive *bool) ([]ExternalSite, error) {
	query := `SELECT id, slug, name, domains, icon_path, access_mode, is_active, created_at, updated_at
        FROM external_sites`
	args := []interface{}{}
	if isActive != nil {
		query += " WHERE is_active = ?"
		args = append(args, *isActive)
	}
	query += " ORDER BY name, id"
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []ExternalSite
	for rows.Next() {
		var s ExternalSite
		if err := rows.Scan(&s.ID, &s.Slug, &s.Name, &s.Domains, &s.IconPath, &s.AccessMode, &s.IsActive, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return nil, err
		}
		res = append(res, s)
	}
	return res, nil
}

func (h sqlHelper) upsertExternalSite(site *ExternalSite) (int, error) {
	if site == nil {
		return 0, nil
	}
	slug := strings.TrimSpace(site.Slug)
	if slug == "" {
		return 0, nil
	}
	site.Slug = slug
	if site.ID > 0 {
		stmt := `UPDATE external_sites
            SET slug = ?, name = ?, domains = ?, icon_path = ?, access_mode = ?, is_active = ?, updated_at = ` + h.nowFun + `
            WHERE id = ?`
		stmt = formatPlaceholders(h.style, stmt)
		_, err := h.db.Exec(stmt, site.Slug, site.Name, site.Domains, nullStringValue(site.IconPath), site.AccessMode, site.IsActive, site.ID)
		return site.ID, err
	}
	var existingID int
	query := `SELECT id FROM external_sites WHERE slug = ?`
	query = formatPlaceholders(h.style, query)
	err := h.db.QueryRow(query, slug).Scan(&existingID)
	if err == nil {
		stmt := `UPDATE external_sites
            SET name = ?, domains = ?, icon_path = ?, access_mode = ?, is_active = ?, updated_at = ` + h.nowFun + `
            WHERE id = ?`
		stmt = formatPlaceholders(h.style, stmt)
		_, err = h.db.Exec(stmt, site.Name, site.Domains, nullStringValue(site.IconPath), site.AccessMode, site.IsActive, existingID)
		if err != nil {
			return 0, err
		}
		site.ID = existingID
		return existingID, nil
	}
	if err != sql.ErrNoRows {
		return 0, err
	}
	stmt := `INSERT INTO external_sites (slug, name, domains, icon_path, access_mode, is_active, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	args := []interface{}{slug, site.Name, site.Domains, nullStringValue(site.IconPath), site.AccessMode, site.IsActive}
	if h.style == "postgres" {
		if err := h.db.QueryRow(stmt, args...).Scan(&site.ID); err != nil {
			return 0, err
		}
		return site.ID, nil
	}
	res, err := h.db.Exec(stmt, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		site.ID = int(id)
	}
	return site.ID, nil
}

func (h sqlHelper) toggleExternalSiteActive(id int) error {
	stmt := `UPDATE external_sites SET is_active = NOT is_active, updated_at = ` + h.nowFun + ` WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, id)
	return err
}

func (h sqlHelper) listExternalLinksByPersona(personaID int, statusFilter string) ([]ExternalLinkRow, error) {
	query := `SELECT l.id, l.persona_id, l.site_id, l.url, l.url_norm, l.title, l.meta, l.status, l.created_by_user_id, l.created_at, l.updated_at,
        s.slug, s.name, s.icon_path, s.access_mode
        FROM external_links l
        LEFT JOIN external_sites s ON s.id = l.site_id
        WHERE l.persona_id = ?`
	args := []interface{}{personaID}
	statusFilter = strings.TrimSpace(statusFilter)
	if statusFilter != "" {
		query += " AND l.status = ?"
		args = append(args, statusFilter)
	}
	query += " ORDER BY l.created_at DESC, l.id DESC"
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []ExternalLinkRow
	for rows.Next() {
		var row ExternalLinkRow
		if err := rows.Scan(&row.ID, &row.PersonaID, &row.SiteID, &row.URL, &row.URLNorm, &row.Title, &row.Meta, &row.Status, &row.CreatedByUserID, &row.CreatedAt, &row.UpdatedAt,
			&row.SiteSlug, &row.SiteName, &row.SiteIconPath, &row.SiteAccessMode); err != nil {
			return nil, err
		}
		res = append(res, row)
	}
	return res, nil
}

func (h sqlHelper) createExternalLinkPending(personaID int, userID int, url, title string) (int, error) {
	cleanURL, normURL, host, err := NormalizeExternalURL(url)
	if err != nil {
		return 0, err
	}
	query := `SELECT id FROM external_links WHERE persona_id = ? AND url_norm = ?`
	query = formatPlaceholders(h.style, query)
	var existingID int
	if err := h.db.QueryRow(query, personaID, normURL).Scan(&existingID); err == nil {
		return existingID, nil
	} else if err != sql.ErrNoRows {
		return 0, err
	}

	siteID := sql.NullInt64{}
	if sites, err := h.listExternalSitesActive(); err == nil {
		if matchID := resolveExternalSiteID(host, sites); matchID > 0 {
			siteID = sql.NullInt64{Int64: int64(matchID), Valid: true}
		}
	}
	createdBy := sql.NullInt64{}
	if userID > 0 {
		createdBy = sql.NullInt64{Int64: int64(userID), Valid: true}
	}
	title = strings.TrimSpace(title)
	stmt := `INSERT INTO external_links (persona_id, site_id, url, url_norm, title, meta, status, created_by_user_id, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	args := []interface{}{
		personaID,
		nullInt64Value(siteID),
		cleanURL,
		normURL,
		nullStringValue(nullableString(title)),
		nil,
		nullInt64Value(createdBy),
	}
	if h.style == "postgres" {
		var id int
		if err := h.db.QueryRow(stmt, args...).Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}
	res, err := h.db.Exec(stmt, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		return int(id), nil
	}
	return 0, nil
}

func (h sqlHelper) updateExternalLinkStatus(id int, status string) error {
	stmt := `UPDATE external_links SET status = ?, updated_at = ` + h.nowFun + ` WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, status, id)
	return err
}

func (h sqlHelper) listExternalLinksByStatus(status string) ([]ExternalLinkAdminRow, error) {
	query := `SELECT l.id, l.persona_id, l.site_id, l.url, l.url_norm, l.title, l.meta, l.status, l.created_by_user_id, l.created_at, l.updated_at,
        s.slug, s.name, s.icon_path, s.access_mode,
        p.nom, p.cognom1, p.cognom2, p.nom_complet
        FROM external_links l
        LEFT JOIN external_sites s ON s.id = l.site_id
        LEFT JOIN persona p ON p.id = l.persona_id
        WHERE l.status = ?
        ORDER BY l.created_at DESC, l.id DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []ExternalLinkAdminRow
	for rows.Next() {
		var row ExternalLinkAdminRow
		if err := rows.Scan(&row.ID, &row.PersonaID, &row.SiteID, &row.URL, &row.URLNorm, &row.Title, &row.Meta, &row.Status, &row.CreatedByUserID, &row.CreatedAt, &row.UpdatedAt,
			&row.SiteSlug, &row.SiteName, &row.SiteIconPath, &row.SiteAccessMode,
			&row.PersonaNom, &row.PersonaCognom1, &row.PersonaCognom2, &row.PersonaNomComplet); err != nil {
			return nil, err
		}
		res = append(res, row)
	}
	return res, nil
}

func resolveExternalSiteID(host string, sites []ExternalSite) int {
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return 0
	}
	for _, site := range sites {
		for _, domain := range ParseExternalDomains(site.Domains) {
			if MatchExternalDomain(host, domain) {
				return site.ID
			}
		}
	}
	return 0
}

func nullStringValue(val sql.NullString) interface{} {
	if val.Valid {
		return val.String
	}
	return nil
}

func nullInt64Value(val sql.NullInt64) interface{} {
	if val.Valid {
		return val.Int64
	}
	return nil
}
