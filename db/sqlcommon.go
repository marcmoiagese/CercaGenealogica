package db

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
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

func buildInPlaceholders(style string, count int) string {
	if count <= 0 {
		return ""
	}
	switch strings.ToLower(style) {
	case "postgres":
		parts := make([]string, count)
		for i := 0; i < count; i++ {
			parts[i] = fmt.Sprintf("$%d", i+1)
		}
		return strings.Join(parts, ",")
	default:
		return strings.TrimRight(strings.Repeat("?,", count), ",")
	}
}

func demografiaYearColumn(style string) string {
	switch strings.ToLower(style) {
	case "mysql":
		return "`any`"
	case "postgres":
		return "\"any\""
	default:
		return "any"
	}
}

func demografiaYearExpr(style string) string {
	switch strings.ToLower(style) {
	case "mysql":
		return "COALESCE(t.any_doc, YEAR(t.data_acte_iso))"
	case "postgres":
		return "COALESCE(t.any_doc, EXTRACT(YEAR FROM t.data_acte_iso)::INT)"
	default:
		return "COALESCE(t.any_doc, CAST(strftime('%Y', t.data_acte_iso) AS INTEGER))"
	}
}

func parseBoolValue(val interface{}) bool {
	switch v := val.(type) {
	case bool:
		return v
	case int:
		return v != 0
	case int64:
		return v != 0
	case int32:
		return v != 0
	case uint:
		return v != 0
	case uint64:
		return v != 0
	case []byte:
		s := strings.ToLower(strings.TrimSpace(string(v)))
		return s == "1" || s == "t" || s == "true" || s == "yes" || s == "y"
	case string:
		s := strings.ToLower(strings.TrimSpace(v))
		return s == "1" || s == "t" || s == "true" || s == "yes" || s == "y"
	default:
		return false
	}
}

func parseIntValue(val interface{}) int {
	switch v := val.(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case uint:
		return int(v)
	case uint64:
		return int(v)
	case float64:
		return int(v)
	case []byte:
		n, err := strconv.Atoi(strings.TrimSpace(string(v)))
		if err != nil {
			return 0
		}
		return n
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return 0
		}
		return n
	default:
		return 0
	}
}

func dbTimeString(val interface{}) string {
	if val == nil {
		return ""
	}
	switch v := val.(type) {
	case time.Time:
		if v.IsZero() {
			return ""
		}
		return v.Format("2006-01-02 15:04:05")
	case sql.NullTime:
		if !v.Valid {
			return ""
		}
		return v.Time.Format("2006-01-02 15:04:05")
	case []byte:
		return strings.TrimSpace(string(v))
	case string:
		return strings.TrimSpace(v)
	default:
		return fmt.Sprint(v)
	}
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
	var args []interface{}
	switch h.style {
	case "mysql":
		query = `SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?`
		args = []interface{}{table, column}
	case "postgres":
		query = `SELECT 1 FROM information_schema.columns WHERE table_name = $1 AND column_name = $2`
		args = []interface{}{table, column}
	default: // sqlite
		query = fmt.Sprintf(`SELECT 1 FROM pragma_table_info('%s') WHERE name = ?`, table)
		args = []interface{}{column}
	}
	row := h.db.QueryRow(query, args...)
	var tmp int
	if err := row.Scan(&tmp); err != nil {
		return false
	}
	return true
}

func (h sqlHelper) tableExists(table string) bool {
	table = strings.TrimSpace(table)
	if table == "" {
		return false
	}
	var query string
	var args []interface{}
	switch h.style {
	case "mysql":
		query = `SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`
		args = []interface{}{table}
	case "postgres":
		query = `SELECT 1 FROM information_schema.tables WHERE table_name = $1`
		args = []interface{}{table}
	default: // sqlite
		query = `SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?`
		args = []interface{}{table}
	}
	row := h.db.QueryRow(query, args...)
	var tmp int
	if err := row.Scan(&tmp); err != nil {
		return false
	}
	return true
}

// Policies
func (h sqlHelper) ensureDefaultPolicies() error {
	h.ensurePermissionsSchema()
	defaultPerms := map[string]PolicyPermissions{
		"admin": {
			Admin:              true,
			CanManageUsers:     true,
			CanManageTerritory: true,
			CanManageEclesia:   true,
			CanManageArchives:  true,
			CanCreatePerson:    true,
			CanEditAnyPerson:   true,
			CanModerate:        true,
			CanManagePolicies:  true,
		},
		"moderador": {
			CanModerate: true,
		},
		"confiança": {
			CanCreatePerson: true,
		},
		"usuari": {},
	}
	for name, perms := range defaultPerms {
		permsJSON, _ := json.Marshal(perms)
		stmt := `INSERT INTO politiques (nom, descripcio, permisos, data_creacio) VALUES (?, ?, ?, ` + h.nowFun + `)`
		if h.style == "postgres" {
			stmt = formatPlaceholders(h.style, `INSERT INTO politiques (nom, descripcio, permisos, data_creacio) VALUES (?, ?, ?, `+h.nowFun+`) ON CONFLICT (nom) DO NOTHING`)
		} else if h.style == "mysql" {
			stmt += " ON DUPLICATE KEY UPDATE permisos=VALUES(permisos), descripcio=VALUES(descripcio)"
		} else { // sqlite
			stmt += " ON CONFLICT(nom) DO NOTHING"
		}
		if h.style != "postgres" {
			stmt = formatPlaceholders(h.style, stmt)
		}
		_, _ = h.db.Exec(stmt, name, "", string(permsJSON))
		// Update perms if entry already exists but empty
		upd := formatPlaceholders(h.style, `UPDATE politiques SET permisos = ? WHERE nom = ? AND (permisos IS NULL OR permisos = '' OR permisos = '{}' )`)
		_, _ = h.db.Exec(upd, string(permsJSON), name)
	}
	var adminID int
	_ = h.db.QueryRow(formatPlaceholders(h.style, "SELECT id FROM politiques WHERE nom = ?"), "admin").Scan(&adminID)
	var userPolicyID int
	_ = h.db.QueryRow(formatPlaceholders(h.style, "SELECT id FROM politiques WHERE nom = ?"), "usuari").Scan(&userPolicyID)

	// Assigna política base als usuaris sense cap assignació directa.
	if userPolicyID > 0 {
		stmt := `
            INSERT INTO usuaris_politiques (usuari_id, politica_id)
            SELECT u.id, ?
            FROM usuaris u
            LEFT JOIN usuaris_politiques up ON up.usuari_id = u.id
            WHERE up.usuari_id IS NULL`
		stmt = formatPlaceholders(h.style, stmt)
		_, _ = h.db.Exec(stmt, userPolicyID)
	}

	// Assegura com a mínim un admin assignat explícitament.
	if adminID > 0 {
		var adminCount int
		countStmt := formatPlaceholders(h.style, "SELECT COUNT(*) FROM usuaris_politiques WHERE politica_id = ?")
		if err := h.db.QueryRow(countStmt, adminID).Scan(&adminCount); err == nil && adminCount == 0 {
			var userID int
			if err := h.db.QueryRow("SELECT id FROM usuaris ORDER BY id ASC LIMIT 1").Scan(&userID); err == nil {
				stmt := formatPlaceholders(h.style, "INSERT INTO usuaris_politiques (usuari_id, politica_id) VALUES (?, ?)")
				_, _ = h.db.Exec(stmt, userID, adminID)
			}
		}
	}
	return nil
}

func (h sqlHelper) ensureDefaultPointsRules() error {
	defaults := []PointsRule{
		{Code: "persona_create", Name: "Crear persona", Description: "Alta de fitxa de persona", Points: 5, Active: true},
		{Code: "persona_update", Name: "Editar persona", Description: "Edició/correcció de persona", Points: 2, Active: true},
		{Code: "llibre_pagina_index", Name: "Indexar pàgina", Description: "Indexar pàgina de llibre", Points: 3, Active: true},
		{Code: "moderacio_approve", Name: "Aprovar com a moderador", Description: "Aprovar contingut pendent", Points: 1, Active: true},
		{Code: "moderacio_reject", Name: "Rebutjar com a moderador", Description: "Rebutjar contingut pendent", Points: 0, Active: true},
		{Code: "arxiu_create", Name: "Crear arxiu", Description: "Alta d'arxiu", Points: 2, Active: true},
		{Code: "arxiu_update", Name: "Editar arxiu", Description: "Edició d'arxiu", Points: 1, Active: true},
		{Code: "llibre_create", Name: "Crear llibre", Description: "Alta de llibre", Points: 3, Active: true},
		{Code: "llibre_update", Name: "Editar llibre", Description: "Edició de llibre", Points: 1, Active: true},
		{Code: "nivell_create", Name: "Crear nivell administratiu", Description: "Alta de nivell administratiu", Points: 2, Active: true},
		{Code: "nivell_update", Name: "Editar nivell administratiu", Description: "Edició de nivell administratiu", Points: 1, Active: true},
		{Code: "municipi_create", Name: "Crear municipi", Description: "Alta de municipi/localitat", Points: 3, Active: true},
		{Code: "municipi_update", Name: "Editar municipi", Description: "Edició de municipi/localitat", Points: 1, Active: true},
		{Code: "eclesiastic_create", Name: "Crear entitat eclesiàstica", Description: "Alta d'entitat eclesiàstica", Points: 2, Active: true},
		{Code: "eclesiastic_update", Name: "Editar entitat eclesiàstica", Description: "Edició d'entitat eclesiàstica", Points: 1, Active: true},
		{Code: "llibre_page_stats_update", Name: "Registres per pàgina", Description: "Actualitzar registres per pàgina d'un llibre", Points: 1, Active: true},
		{Code: "cognom_variant_create", Name: "Proposar variant de cognom", Description: "Aportar una nova variació (pendent de moderació)", Points: 1, Active: true},
		{Code: "municipi_mapa_submit", Name: "Proposar mapa", Description: "Enviar un mapa a moderació", Points: 15, Active: true},
		{Code: "municipi_mapa_approve", Name: "Aprovar mapa", Description: "Aprovar un mapa pendent", Points: 3, Active: true},
		{Code: "municipi_mapa_reject", Name: "Rebutjar mapa", Description: "Rebutjar un mapa pendent", Points: 0, Active: true},
		{Code: "municipi_historia_general_submit", Name: "Proposar història general", Description: "Enviar història general a moderació", Points: 5, Active: true},
		{Code: "municipi_historia_fet_submit", Name: "Proposar fet històric", Description: "Enviar un fet històric a moderació", Points: 3, Active: true},
		{Code: "municipi_anecdota_publicada", Name: "Proposar anècdota", Description: "Enviar una anècdota a moderació", Points: 5, Active: true},
		{Code: "event_historic_submit", Name: "Proposar esdeveniment històric", Description: "Enviar un esdeveniment històric a moderació", Points: 0, Active: true},
		{Code: "event_historic_approve", Name: "Aprovar esdeveniment històric", Description: "Aprovar un esdeveniment històric pendent", Points: 5, Active: true},
		{Code: "event_historic_reject", Name: "Rebutjar esdeveniment històric", Description: "Rebutjar un esdeveniment històric pendent", Points: 0, Active: true},
	}
	for _, r := range defaults {
		stmt := `INSERT INTO punts_regles (codi, nom, descripcio, punts, actiu, data_creacio) VALUES (?, ?, ?, ?, ?, ` + h.nowFun + `)`
		if h.style == "postgres" {
			stmt += " ON CONFLICT (codi) DO NOTHING"
		} else if h.style == "mysql" {
			stmt += " ON DUPLICATE KEY UPDATE nom=VALUES(nom), descripcio=VALUES(descripcio), punts=VALUES(punts), actiu=VALUES(actiu)"
		} else { // sqlite
			stmt += " ON CONFLICT(codi) DO NOTHING"
		}
		stmt = formatPlaceholders(h.style, stmt)
		h.db.Exec(stmt, r.Code, r.Name, r.Description, r.Points, r.Active)
	}
	return nil
}

func (h sqlHelper) ensureDefaultAchievements() error {
	type ruleFilters struct {
		RuleCodes   []string `json:"rule_codes,omitempty"`
		Actions     []string `json:"actions,omitempty"`
		ObjectTypes []string `json:"object_types,omitempty"`
		Status      []string `json:"status,omitempty"`
	}
	type ruleSeed struct {
		Type      string      `json:"type"`
		Filters   ruleFilters `json:"filters,omitempty"`
		Threshold int         `json:"threshold,omitempty"`
		Window    string      `json:"window,omitempty"`
		MinDays   int         `json:"min_days,omitempty"`
	}
	type achievementSeed struct {
		Code        string
		Name        string
		Description string
		Rarity      string
		Visibility  string
		Domain      string
		Repeatable  bool
		Rule        ruleSeed
	}
	seeds := []achievementSeed{
		{
			Code:        "ACH_FIRST_STEP",
			Name:        "First step",
			Description: "First validated contribution",
			Rarity:      "common",
			Visibility:  "visible",
			Domain:      "general",
			Rule: ruleSeed{
				Type:      "count",
				Filters:   ruleFilters{Status: []string{"validat"}},
				Threshold: 1,
			},
		},
		{
			Code:        "ACH_STREAK_7",
			Name:        "Seven day streak",
			Description: "Seven days with validated activity",
			Rarity:      "rare",
			Visibility:  "visible",
			Domain:      "general",
			Rule: ruleSeed{
				Type:    "streak_days",
				Filters: ruleFilters{Status: []string{"validat"}},
				MinDays: 7,
			},
		},
		{
			Code:        "ACH_INDEXER_10",
			Name:        "Indexer",
			Description: "Index 10 pages",
			Rarity:      "common",
			Visibility:  "visible",
			Domain:      "llibres",
			Rule: ruleSeed{
				Type:      "count",
				Filters:   ruleFilters{RuleCodes: []string{"llibre_pagina_index"}, Status: []string{"validat"}},
				Threshold: 10,
			},
		},
		{
			Code:        "ACH_LIBRE_1",
			Name:        "Llibre creator",
			Description: "Create one book",
			Rarity:      "rare",
			Visibility:  "visible",
			Domain:      "llibres",
			Rule: ruleSeed{
				Type:      "count",
				Filters:   ruleFilters{RuleCodes: []string{"llibre_create"}, Status: []string{"validat"}},
				Threshold: 1,
			},
		},
		{
			Code:        "ACH_MUN_1",
			Name:        "Municipi founder",
			Description: "Create one municipality",
			Rarity:      "rare",
			Visibility:  "visible",
			Domain:      "municipis",
			Rule: ruleSeed{
				Type:      "count",
				Filters:   ruleFilters{RuleCodes: []string{"municipi_create"}, Status: []string{"validat"}},
				Threshold: 1,
			},
		},
		{
			Code:        "ACH_MUN_BURST_5",
			Name:        "Rapid founder",
			Description: "Create 5 municipalities in 24h",
			Rarity:      "epic",
			Visibility:  "hidden",
			Domain:      "municipis",
			Rule: ruleSeed{
				Type:      "burst_count",
				Filters:   ruleFilters{RuleCodes: []string{"municipi_create"}, Status: []string{"validat"}},
				Threshold: 5,
				Window:    "24h",
			},
		},
		{
			Code:        "ACH_MAP_SUBMIT",
			Name:        "Cartographer",
			Description: "Submit a municipality map",
			Rarity:      "rare",
			Visibility:  "visible",
			Domain:      "municipis",
			Rule: ruleSeed{
				Type:      "count",
				Filters:   ruleFilters{RuleCodes: []string{"municipi_mapa_submit"}, Status: []string{"validat"}},
				Threshold: 1,
			},
		},
		{
			Code:        "ACH_HISTORIA_SUBMIT",
			Name:        "Storyteller",
			Description: "Submit municipality history",
			Rarity:      "rare",
			Visibility:  "visible",
			Domain:      "municipis",
			Rule: ruleSeed{
				Type:      "count",
				Filters:   ruleFilters{RuleCodes: []string{"municipi_historia_general_submit", "municipi_historia_fet_submit"}, Status: []string{"validat"}},
				Threshold: 1,
			},
		},
		{
			Code:        "ACH_ANECDOTE",
			Name:        "Anecdote maker",
			Description: "Submit a local anecdote",
			Rarity:      "common",
			Visibility:  "visible",
			Domain:      "municipis",
			Rule: ruleSeed{
				Type:      "count",
				Filters:   ruleFilters{RuleCodes: []string{"municipi_anecdota_publicada"}, Status: []string{"validat"}},
				Threshold: 1,
			},
		},
		{
			Code:        "ACH_MOD_10",
			Name:        "Moderator",
			Description: "Approve 10 items",
			Rarity:      "rare",
			Visibility:  "visible",
			Domain:      "moderacio",
			Rule: ruleSeed{
				Type:      "count",
				Filters:   ruleFilters{RuleCodes: []string{"moderacio_approve"}, Status: []string{"validat"}},
				Threshold: 10,
			},
		},
		{
			Code:        "ACH_POINTS_100",
			Name:        "Century",
			Description: "Earn 100 points",
			Rarity:      "epic",
			Visibility:  "visible",
			Domain:      "general",
			Rule: ruleSeed{
				Type:      "sum_points",
				Filters:   ruleFilters{Status: []string{"validat"}},
				Threshold: 100,
			},
		},
	}
	for _, seed := range seeds {
		enabled := true
		for _, code := range seed.Rule.Filters.RuleCodes {
			if _, err := h.getPointsRuleByCode(code); err != nil {
				enabled = false
				break
			}
		}
		ruleJSON, err := json.Marshal(seed.Rule)
		if err != nil {
			continue
		}
		stmt := `INSERT INTO achievements (code, name, description, rarity, visibility, domain, is_enabled, is_repeatable, icon_media_item_id, rule_json, created_at, updated_at)
		         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
		if h.style == "postgres" {
			stmt += " ON CONFLICT (code) DO NOTHING"
		} else if h.style == "mysql" {
			stmt += " ON DUPLICATE KEY UPDATE name=VALUES(name), description=VALUES(description), rarity=VALUES(rarity), visibility=VALUES(visibility), domain=VALUES(domain), is_enabled=VALUES(is_enabled), is_repeatable=VALUES(is_repeatable), rule_json=VALUES(rule_json)"
		} else {
			stmt += " ON CONFLICT(code) DO NOTHING"
		}
		stmt = formatPlaceholders(h.style, stmt)
		_, _ = h.db.Exec(stmt, seed.Code, seed.Name, seed.Description, seed.Rarity, seed.Visibility, seed.Domain, enabled, seed.Repeatable, nil, string(ruleJSON))
	}
	return nil
}

func (h sqlHelper) userHasAnyPolicy(userID int, policies []string) (bool, error) {
	if len(policies) == 0 {
		return false, nil
	}
	inPlaceholders := strings.TrimRight(strings.Repeat("?,", len(policies)), ",")
	query := `
        SELECT 1
        FROM usuaris_politiques up
        INNER JOIN politiques p ON p.id = up.politica_id
        WHERE up.usuari_id = ? AND p.nom IN (` + inPlaceholders + `)
        LIMIT 1`
	query = formatPlaceholders(h.style, query)
	args := make([]interface{}, 0, len(policies)+1)
	args = append(args, userID)
	for _, p := range policies {
		args = append(args, p)
	}
	row := h.db.QueryRow(query, args...)
	var tmp int
	if err := row.Scan(&tmp); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (h sqlHelper) listPolitiques() ([]Politica, error) {
	rows, err := h.db.Query(`SELECT id, nom, descripcio, permisos FROM politiques ORDER BY nom`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []Politica
	for rows.Next() {
		var p Politica
		if err := rows.Scan(&p.ID, &p.Nom, &p.Descripcio, &p.Permisos); err != nil {
			return nil, err
		}
		res = append(res, p)
	}
	return res, nil
}

func (h sqlHelper) getPolitica(id int) (*Politica, error) {
	row := h.db.QueryRow(`SELECT id, nom, descripcio, permisos FROM politiques WHERE id = ?`, id)
	p := &Politica{}
	if err := row.Scan(&p.ID, &p.Nom, &p.Descripcio, &p.Permisos); err != nil {
		return nil, err
	}
	return p, nil
}

func (h sqlHelper) savePolitica(p *Politica) (int, error) {
	if p.ID == 0 {
		stmt := `INSERT INTO politiques (nom, descripcio, permisos, data_creacio) VALUES (?, ?, ?, ` + h.nowFun + `)`
		if h.style == "postgres" {
			stmt += " RETURNING id"
		}
		stmt = formatPlaceholders(h.style, stmt)
		if h.style == "postgres" {
			if err := h.db.QueryRow(stmt, p.Nom, p.Descripcio, p.Permisos).Scan(&p.ID); err != nil {
				return 0, err
			}
			return p.ID, nil
		}
		res, err := h.db.Exec(stmt, p.Nom, p.Descripcio, p.Permisos)
		if err != nil {
			return 0, err
		}
		if id, err := res.LastInsertId(); err == nil {
			p.ID = int(id)
		}
		return p.ID, nil
	}
	stmt := `UPDATE politiques SET nom=?, descripcio=?, permisos=? WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, p.Nom, p.Descripcio, p.Permisos, p.ID)
	return p.ID, err
}

func (h sqlHelper) listPoliticaGrants(politicaID int) ([]PoliticaGrant, error) {
	query := `
        SELECT id, politica_id, perm_key, scope_type, scope_id, include_children
        FROM politica_grants
        WHERE politica_id = ?
        ORDER BY id`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, politicaID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []PoliticaGrant
	for rows.Next() {
		var g PoliticaGrant
		var includeRaw interface{}
		if err := rows.Scan(&g.ID, &g.PoliticaID, &g.PermKey, &g.ScopeType, &g.ScopeID, &includeRaw); err != nil {
			return nil, err
		}
		g.IncludeChildren = parseBoolValue(includeRaw)
		res = append(res, g)
	}
	return res, nil
}

func (h sqlHelper) savePoliticaGrant(g *PoliticaGrant) (int, error) {
	if g.ID == 0 {
		stmt := `INSERT INTO politica_grants (politica_id, perm_key, scope_type, scope_id, include_children)
                 VALUES (?, ?, ?, ?, ?)`
		stmt = formatPlaceholders(h.style, stmt)
		if h.style == "postgres" {
			stmt += " RETURNING id"
			if err := h.db.QueryRow(stmt, g.PoliticaID, g.PermKey, g.ScopeType, g.ScopeID, g.IncludeChildren).Scan(&g.ID); err != nil {
				return 0, err
			}
			return g.ID, nil
		}
		res, err := h.db.Exec(stmt, g.PoliticaID, g.PermKey, g.ScopeType, g.ScopeID, g.IncludeChildren)
		if err != nil {
			return 0, err
		}
		if id, err := res.LastInsertId(); err == nil {
			g.ID = int(id)
		}
		return g.ID, nil
	}
	stmt := `UPDATE politica_grants
             SET politica_id = ?, perm_key = ?, scope_type = ?, scope_id = ?, include_children = ?
             WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, g.PoliticaID, g.PermKey, g.ScopeType, g.ScopeID, g.IncludeChildren, g.ID)
	return g.ID, err
}

func (h sqlHelper) deletePoliticaGrant(id int) error {
	stmt := formatPlaceholders(h.style, `DELETE FROM politica_grants WHERE id = ?`)
	_, err := h.db.Exec(stmt, id)
	return err
}

func (h sqlHelper) listUserPolitiques(userID int) ([]Politica, error) {
	query := `
        SELECT p.id, p.nom, p.descripcio, p.permisos
        FROM usuaris_politiques up
        INNER JOIN politiques p ON p.id = up.politica_id
        WHERE up.usuari_id = ?
        ORDER BY p.nom`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []Politica
	for rows.Next() {
		var p Politica
		if err := rows.Scan(&p.ID, &p.Nom, &p.Descripcio, &p.Permisos); err != nil {
			return nil, err
		}
		res = append(res, p)
	}
	return res, nil
}

func (h sqlHelper) addUserPolitica(userID, politicaID int) error {
	stmt := formatPlaceholders(h.style, `INSERT INTO usuaris_politiques (usuari_id, politica_id, data_assignacio) VALUES (?, ?, `+h.nowFun+`) ON CONFLICT DO NOTHING`)
	if h.style == "mysql" {
		stmt = formatPlaceholders(h.style, `INSERT INTO usuaris_politiques (usuari_id, politica_id, data_assignacio) VALUES (?, ?, `+h.nowFun+`) ON DUPLICATE KEY UPDATE usuari_id=VALUES(usuari_id)`)
	}
	_, err := h.db.Exec(stmt, userID, politicaID)
	return err
}

func (h sqlHelper) removeUserPolitica(userID, politicaID int) error {
	stmt := formatPlaceholders(h.style, `DELETE FROM usuaris_politiques WHERE usuari_id = ? AND politica_id = ?`)
	_, err := h.db.Exec(stmt, userID, politicaID)
	return err
}

func (h sqlHelper) listGroupPolitiques(groupID int) ([]Politica, error) {
	query := `
        SELECT p.id, p.nom, p.descripcio, p.permisos
        FROM grups_politiques gp
        INNER JOIN politiques p ON p.id = gp.politica_id
        WHERE gp.grup_id = ?
        ORDER BY p.nom`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []Politica
	for rows.Next() {
		var p Politica
		if err := rows.Scan(&p.ID, &p.Nom, &p.Descripcio, &p.Permisos); err != nil {
			return nil, err
		}
		res = append(res, p)
	}
	return res, nil
}

func (h sqlHelper) addGroupPolitica(groupID, politicaID int) error {
	stmt := formatPlaceholders(h.style, `INSERT INTO grups_politiques (grup_id, politica_id, data_assignacio) VALUES (?, ?, `+h.nowFun+`) ON CONFLICT DO NOTHING`)
	if h.style == "mysql" {
		stmt = formatPlaceholders(h.style, `INSERT INTO grups_politiques (grup_id, politica_id, data_assignacio) VALUES (?, ?, `+h.nowFun+`) ON DUPLICATE KEY UPDATE grup_id=VALUES(grup_id)`)
	}
	_, err := h.db.Exec(stmt, groupID, politicaID)
	return err
}

func (h sqlHelper) removeGroupPolitica(groupID, politicaID int) error {
	stmt := formatPlaceholders(h.style, `DELETE FROM grups_politiques WHERE grup_id = ? AND politica_id = ?`)
	_, err := h.db.Exec(stmt, groupID, politicaID)
	return err
}

func (h sqlHelper) getUserPermissionsVersion(userID int) (int, error) {
	stmt := formatPlaceholders(h.style, `SELECT permissions_version FROM usuaris WHERE id = ?`)
	var val sql.NullInt64
	if err := h.db.QueryRow(stmt, userID).Scan(&val); err != nil {
		return 0, err
	}
	if !val.Valid {
		return 0, nil
	}
	return int(val.Int64), nil
}

func (h sqlHelper) bumpUserPermissionsVersion(userID int) error {
	stmt := formatPlaceholders(h.style, `UPDATE usuaris SET permissions_version = COALESCE(permissions_version, 0) + 1 WHERE id = ?`)
	_, err := h.db.Exec(stmt, userID)
	return err
}

func (h sqlHelper) bumpGroupPermissionsVersion(groupID int) error {
	stmt := formatPlaceholders(h.style, `UPDATE usuaris SET permissions_version = COALESCE(permissions_version, 0) + 1 WHERE id IN (SELECT usuari_id FROM usuaris_grups WHERE grup_id = ?)`)
	_, err := h.db.Exec(stmt, groupID)
	return err
}

func (h sqlHelper) bumpPolicyPermissionsVersion(politicaID int) error {
	if politicaID <= 0 {
		return nil
	}
	stmt := `
        UPDATE usuaris
        SET permissions_version = COALESCE(permissions_version, 0) + 1
        WHERE id IN (SELECT usuari_id FROM usuaris_politiques WHERE politica_id = ?)
           OR id IN (
                SELECT ug.usuari_id
                FROM usuaris_grups ug
                INNER JOIN grups_politiques gp ON gp.grup_id = ug.grup_id
                WHERE gp.politica_id = ?
           )`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, politicaID, politicaID)
	return err
}

func (h sqlHelper) listGroups() ([]Group, error) {
	rows, err := h.db.Query(`SELECT id, nom, descripcio FROM grups ORDER BY nom`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []Group
	for rows.Next() {
		var g Group
		if err := rows.Scan(&g.ID, &g.Nom, &g.Descripcio); err != nil {
			return nil, err
		}
		res = append(res, g)
	}
	return res, nil
}

func (h sqlHelper) listUserGroups(userID int) ([]Group, error) {
	query := `
        SELECT g.id, g.nom, g.descripcio
        FROM usuaris_grups ug
        INNER JOIN grups g ON g.id = ug.grup_id
        WHERE ug.usuari_id = ?`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []Group
	for rows.Next() {
		var g Group
		if err := rows.Scan(&g.ID, &g.Nom, &g.Descripcio); err != nil {
			return nil, err
		}
		res = append(res, g)
	}
	return res, nil
}

func (h sqlHelper) getEffectivePoliticaPerms(userID int) (PolicyPermissions, error) {
	combined := PolicyPermissions{}
	// Direct policies
	userPolicies, err := h.listUserPolitiques(userID)
	if err != nil {
		return combined, err
	}
	// Group policies
	groupRows, err := h.listUserGroups(userID)
	if err != nil {
		return combined, err
	}
	groupPolicies := []Politica{}
	for _, g := range groupRows {
		ps, err := h.listGroupPolitiques(g.ID)
		if err == nil {
			groupPolicies = append(groupPolicies, ps...)
		}
	}
	all := append(userPolicies, groupPolicies...)
	for _, p := range all {
		var perms PolicyPermissions
		if err := json.Unmarshal([]byte(p.Permisos), &perms); err != nil {
			continue
		}
		combined = combinePermissions(combined, perms)
	}
	return combined, nil
}

func combinePermissions(base, add PolicyPermissions) PolicyPermissions {
	base.Admin = base.Admin || add.Admin
	base.CanManageUsers = base.CanManageUsers || add.CanManageUsers
	base.CanManageTerritory = base.CanManageTerritory || add.CanManageTerritory
	base.CanManageEclesia = base.CanManageEclesia || add.CanManageEclesia
	base.CanManageArchives = base.CanManageArchives || add.CanManageArchives
	base.CanCreatePerson = base.CanCreatePerson || add.CanCreatePerson
	base.CanEditAnyPerson = base.CanEditAnyPerson || add.CanEditAnyPerson
	base.CanModerate = base.CanModerate || add.CanModerate
	base.CanManagePolicies = base.CanManagePolicies || add.CanManagePolicies
	return base
}

// Persones (moderació bàsica)
func (h sqlHelper) listPersones(f PersonaFilter) ([]Persona, error) {
	query := `
        SELECT id, nom, cognom1, cognom2, municipi, arquevisbat, nom_complet, pagina, llibre, quinta,
               data_naixement, data_bateig, data_defuncio, ofici, estat_civil,
               created_by, created_at, updated_at, updated_by, moderated_by, moderated_at
        FROM persona`
	var args []interface{}
	where := []string{}
	if f.Estat != "" {
		where = append(where, "estat_civil = ?")
		args = append(args, f.Estat)
	}
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY id DESC"
	if f.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, f.Limit)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []Persona
	for rows.Next() {
		var p Persona
		if err := rows.Scan(&p.ID, &p.Nom, &p.Cognom1, &p.Cognom2, &p.Municipi, &p.Arquebisbat, &p.NomComplet, &p.Pagina, &p.Llibre, &p.Quinta, &p.DataNaixement, &p.DataBateig, &p.DataDefuncio, &p.Ofici, &p.ModeracioEstat, &p.CreatedBy, &p.CreatedAt, &p.UpdatedAt, &p.UpdatedBy, &p.ModeratedBy, &p.ModeratedAt); err != nil {
			return nil, err
		}
		// Guardem el motiu de moderació (si s'ha usat) dins de quinta per no ampliar esquema
		p.ModeracioMotiu = p.Quinta
		res = append(res, p)
	}
	return res, nil
}

func (h sqlHelper) getPersona(id int) (*Persona, error) {
	row := h.db.QueryRow(`SELECT id, nom, cognom1, cognom2, municipi, arquevisbat, nom_complet, pagina, llibre, quinta,
        data_naixement, data_bateig, data_defuncio, ofici, estat_civil, created_by, created_at, updated_at, updated_by, moderated_by, moderated_at FROM persona WHERE id = ?`, id)
	var p Persona
	if err := row.Scan(&p.ID, &p.Nom, &p.Cognom1, &p.Cognom2, &p.Municipi, &p.Arquebisbat, &p.NomComplet, &p.Pagina, &p.Llibre, &p.Quinta, &p.DataNaixement, &p.DataBateig, &p.DataDefuncio, &p.Ofici, &p.ModeracioEstat, &p.CreatedBy, &p.CreatedAt, &p.UpdatedAt, &p.UpdatedBy, &p.ModeratedBy, &p.ModeratedAt); err != nil {
		return nil, err
	}
	p.ModeracioMotiu = p.Quinta
	return &p, nil
}

func (h sqlHelper) createPersona(p *Persona) (int, error) {
	status := p.ModeracioEstat
	if status == "" {
		status = "pendent"
	}
	nomComplet := p.NomComplet
	if strings.TrimSpace(nomComplet) == "" {
		nomComplet = strings.TrimSpace(strings.Join([]string{p.Nom, p.Cognom1, p.Cognom2}, " "))
	}
	stmt := `INSERT INTO persona (nom, cognom1, cognom2, municipi, arquevisbat, nom_complet, pagina, llibre, quinta, data_naixement, data_bateig, data_defuncio, ofici, estat_civil, created_by, updated_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	if h.style == "postgres" {
		if err := h.db.QueryRow(stmt, p.Nom, p.Cognom1, p.Cognom2, p.Municipi, p.Arquebisbat, nomComplet, p.Pagina, p.Llibre, p.ModeracioMotiu, p.DataNaixement, p.DataBateig, p.DataDefuncio, p.Ofici, status, p.CreatedBy, p.UpdatedBy).Scan(&p.ID); err != nil {
			return 0, err
		}
		return p.ID, nil
	}
	res, err := h.db.Exec(stmt, p.Nom, p.Cognom1, p.Cognom2, p.Municipi, p.Arquebisbat, nomComplet, p.Pagina, p.Llibre, p.ModeracioMotiu, p.DataNaixement, p.DataBateig, p.DataDefuncio, p.Ofici, status, p.CreatedBy, p.UpdatedBy)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		p.ID = int(id)
	}
	return p.ID, nil
}

func (h sqlHelper) listPersonaAnecdotes(personaID int, userID int) ([]PersonaAnecdote, error) {
	query := `
        SELECT a.id, a.persona_id, a.user_id, u.usuari, a.title, a.body, COALESCE(a.tag, ''), a.status, a.created_at, a.updated_at
        FROM persona_anecdotari a
        LEFT JOIN usuaris u ON u.id = a.user_id
        WHERE a.persona_id = ?`
	args := []interface{}{personaID}
	if userID > 0 {
		query += " AND (a.status = 'publicat' OR a.user_id = ?)"
		args = append(args, userID)
	} else {
		query += " AND a.status = 'publicat'"
	}
	query += " ORDER BY a.created_at DESC"
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []PersonaAnecdote
	for rows.Next() {
		var a PersonaAnecdote
		if err := rows.Scan(&a.ID, &a.PersonaID, &a.UserID, &a.UserName, &a.Title, &a.Body, &a.Tag, &a.Status, &a.CreatedAt, &a.UpdatedAt); err != nil {
			return nil, err
		}
		res = append(res, a)
	}
	return res, nil
}

func (h sqlHelper) createPersonaAnecdote(a *PersonaAnecdote) (int, error) {
	status := strings.TrimSpace(a.Status)
	if status == "" {
		status = "pendent"
	}
	stmt := `INSERT INTO persona_anecdotari (persona_id, user_id, title, body, tag, status)
            VALUES (?, ?, ?, ?, ?, ?)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	if h.style == "postgres" {
		if err := h.db.QueryRow(stmt, a.PersonaID, a.UserID, a.Title, a.Body, a.Tag, status).Scan(&a.ID); err != nil {
			return 0, err
		}
		return a.ID, nil
	}
	res, err := h.db.Exec(stmt, a.PersonaID, a.UserID, a.Title, a.Body, a.Tag, status)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		a.ID = int(id)
	}
	return a.ID, nil
}

// Punts i activitat
func (h sqlHelper) listPointsRules() ([]PointsRule, error) {
	rows, err := h.db.Query(`SELECT id, codi, nom, descripcio, punts, actiu, data_creacio FROM punts_regles ORDER BY codi`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []PointsRule
	for rows.Next() {
		var r PointsRule
		if err := rows.Scan(&r.ID, &r.Code, &r.Name, &r.Description, &r.Points, &r.Active, &r.CreatedAt); err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}

func (h sqlHelper) listUserIDs(limit, offset int) ([]int, error) {
	if limit <= 0 {
		return nil, errors.New("limit invalid")
	}
	if offset < 0 {
		offset = 0
	}
	query := "SELECT id FROM usuaris ORDER BY id LIMIT ?"
	args := []interface{}{limit}
	if offset > 0 {
		query += " OFFSET ?"
		args = append(args, offset)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := []int{}
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		res = append(res, id)
	}
	return res, nil
}

func (h sqlHelper) getPointsRuleByCode(code string) (*PointsRule, error) {
	row := h.db.QueryRow(`SELECT id, codi, nom, descripcio, punts, actiu, data_creacio FROM punts_regles WHERE codi = ?`, code)
	var r PointsRule
	if err := row.Scan(&r.ID, &r.Code, &r.Name, &r.Description, &r.Points, &r.Active, &r.CreatedAt); err != nil {
		return nil, err
	}
	return &r, nil
}

func (h sqlHelper) getPointsRule(id int) (*PointsRule, error) {
	row := h.db.QueryRow(`SELECT id, codi, nom, descripcio, punts, actiu, data_creacio FROM punts_regles WHERE id = ?`, id)
	var r PointsRule
	if err := row.Scan(&r.ID, &r.Code, &r.Name, &r.Description, &r.Points, &r.Active, &r.CreatedAt); err != nil {
		return nil, err
	}
	return &r, nil
}

func (h sqlHelper) savePointsRule(r *PointsRule) (int, error) {
	if r.ID == 0 {
		stmt := `INSERT INTO punts_regles (codi, nom, descripcio, punts, actiu, data_creacio) VALUES (?, ?, ?, ?, ?, ` + h.nowFun + `)`
		if h.style == "postgres" {
			stmt += " RETURNING id"
		}
		stmt = formatPlaceholders(h.style, stmt)
		if h.style == "postgres" {
			if err := h.db.QueryRow(stmt, r.Code, r.Name, r.Description, r.Points, r.Active).Scan(&r.ID); err != nil {
				return 0, err
			}
			return r.ID, nil
		}
		res, err := h.db.Exec(stmt, r.Code, r.Name, r.Description, r.Points, r.Active)
		if err != nil {
			return 0, err
		}
		id, _ := res.LastInsertId()
		r.ID = int(id)
		return r.ID, nil
	}
	stmt := formatPlaceholders(h.style, `UPDATE punts_regles SET codi = ?, nom = ?, descripcio = ?, punts = ?, actiu = ? WHERE id = ?`)
	if _, err := h.db.Exec(stmt, r.Code, r.Name, r.Description, r.Points, r.Active, r.ID); err != nil {
		return 0, err
	}
	return r.ID, nil
}

func (h sqlHelper) getUserActivity(id int) (*UserActivity, error) {
	row := h.db.QueryRow(`SELECT id, usuari_id, regla_id, accio, objecte_tipus, objecte_id, punts, estat, moderat_per, detalls, data_creacio FROM usuaris_activitat WHERE id = ?`, id)
	var a UserActivity
	if err := row.Scan(&a.ID, &a.UserID, &a.RuleID, &a.Action, &a.ObjectType, &a.ObjectID, &a.Points, &a.Status, &a.ModeratedBy, &a.Details, &a.CreatedAt); err != nil {
		return nil, err
	}
	return &a, nil
}

func (h sqlHelper) insertUserActivity(a *UserActivity) (int, error) {
	stmt := `INSERT INTO usuaris_activitat (usuari_id, regla_id, accio, objecte_tipus, objecte_id, punts, estat, moderat_per, detalls, data_creacio)
	         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	var ruleID interface{}
	if a.RuleID.Valid {
		ruleID = a.RuleID.Int64
	}
	var objID interface{}
	if a.ObjectID.Valid {
		objID = a.ObjectID.Int64
	}
	var mod interface{}
	if a.ModeratedBy.Valid {
		mod = a.ModeratedBy.Int64
	}
	if h.style == "postgres" {
		if err := h.db.QueryRow(stmt, a.UserID, ruleID, a.Action, a.ObjectType, objID, a.Points, a.Status, mod, a.Details).Scan(&a.ID); err != nil {
			return 0, err
		}
		return a.ID, nil
	}
	res, err := h.db.Exec(stmt, a.UserID, ruleID, a.Action, a.ObjectType, objID, a.Points, a.Status, mod, a.Details)
	if err != nil {
		return 0, err
	}
	id, _ := res.LastInsertId()
	a.ID = int(id)
	return a.ID, nil
}

func (h sqlHelper) updateUserActivityStatus(id int, status string, moderatedBy *int) error {
	stmt := formatPlaceholders(h.style, `UPDATE usuaris_activitat SET estat = ?, moderat_per = ? WHERE id = ?`)
	var mod interface{}
	if moderatedBy != nil {
		mod = *moderatedBy
	}
	_, err := h.db.Exec(stmt, status, mod, id)
	return err
}

func (h sqlHelper) listUserActivityByUser(userID int, f ActivityFilter) ([]UserActivity, error) {
	where := []string{"usuari_id = ?"}
	args := []interface{}{userID}
	if f.Status != "" {
		where = append(where, "estat = ?")
		args = append(args, f.Status)
	}
	if f.ObjectType != "" {
		where = append(where, "objecte_tipus = ?")
		args = append(args, f.ObjectType)
	}
	if !f.From.IsZero() {
		where = append(where, "data_creacio >= ?")
		args = append(args, f.From)
	}
	if !f.To.IsZero() {
		where = append(where, "data_creacio <= ?")
		args = append(args, f.To)
	}
	query := `SELECT id, usuari_id, regla_id, accio, objecte_tipus, objecte_id, punts, estat, moderat_per, detalls, data_creacio
	          FROM usuaris_activitat`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY data_creacio DESC"
	if f.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", f.Limit)
	}
	if f.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", f.Offset)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []UserActivity
	for rows.Next() {
		var a UserActivity
		if err := rows.Scan(&a.ID, &a.UserID, &a.RuleID, &a.Action, &a.ObjectType, &a.ObjectID, &a.Points, &a.Status, &a.ModeratedBy, &a.Details, &a.CreatedAt); err != nil {
			return nil, err
		}
		res = append(res, a)
	}
	return res, nil
}

func (h sqlHelper) listActivityByObject(objectType string, objectID int, status string) ([]UserActivity, error) {
	where := []string{"objecte_tipus = ?", "objecte_id = ?"}
	args := []interface{}{objectType, objectID}
	if status != "" {
		where = append(where, "estat = ?")
		args = append(args, status)
	}
	query := `SELECT id, usuari_id, regla_id, accio, objecte_tipus, objecte_id, punts, estat, moderat_per, detalls, data_creacio
	          FROM usuaris_activitat WHERE ` + strings.Join(where, " AND ") + ` ORDER BY data_creacio DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []UserActivity
	for rows.Next() {
		var a UserActivity
		if err := rows.Scan(&a.ID, &a.UserID, &a.RuleID, &a.Action, &a.ObjectType, &a.ObjectID, &a.Points, &a.Status, &a.ModeratedBy, &a.Details, &a.CreatedAt); err != nil {
			return nil, err
		}
		res = append(res, a)
	}
	return res, nil
}

func (h sqlHelper) addPointsToUser(userID int, delta int) error {
	switch h.style {
	case "mysql":
		stmt := `INSERT INTO usuaris_punts (usuari_id, punts_total, ultima_actualitzacio)
		         VALUES (?, ?, ` + h.nowFun + `)
		         ON DUPLICATE KEY UPDATE punts_total = punts_total + VALUES(punts_total), ultima_actualitzacio = ` + h.nowFun
		_, err := h.db.Exec(formatPlaceholders(h.style, stmt), userID, delta)
		return err
	case "postgres":
		stmt := `INSERT INTO usuaris_punts (usuari_id, punts_total, ultima_actualitzacio)
		         VALUES ($1, $2, ` + h.nowFun + `)
		         ON CONFLICT (usuari_id) DO UPDATE SET punts_total = usuaris_punts.punts_total + EXCLUDED.punts_total, ultima_actualitzacio = ` + h.nowFun
		_, err := h.db.Exec(stmt, userID, delta)
		return err
	default: // sqlite
		stmt := `INSERT INTO usuaris_punts (usuari_id, punts_total, ultima_actualitzacio)
		         VALUES (?, ?, ` + h.nowFun + `)
		         ON CONFLICT(usuari_id) DO UPDATE SET punts_total = punts_total + excluded.punts_total, ultima_actualitzacio = ` + h.nowFun
		_, err := h.db.Exec(formatPlaceholders(h.style, stmt), userID, delta)
		return err
	}
}

func (h sqlHelper) getUserPoints(userID int) (*UserPoints, error) {
	row := h.db.QueryRow(`SELECT usuari_id, punts_total, ultima_actualitzacio FROM usuaris_punts WHERE usuari_id = ?`, userID)
	var up UserPoints
	if err := row.Scan(&up.UserID, &up.Total, &up.UltimaActualitzacio); err != nil {
		return nil, err
	}
	return &up, nil
}

func (h sqlHelper) recalcUserPoints() error {
	if _, err := h.db.Exec(`DELETE FROM usuaris_punts`); err != nil {
		return err
	}
	stmt := `
	    INSERT INTO usuaris_punts (usuari_id, punts_total, ultima_actualitzacio)
	    SELECT usuari_id, COALESCE(SUM(punts),0) AS total, ` + h.nowFun + `
	    FROM usuaris_activitat
	    WHERE estat = 'validat'
	    GROUP BY usuari_id`
	_, err := h.db.Exec(stmt)
	return err
}

func (h sqlHelper) getRanking(f RankingFilter) ([]UserPoints, error) {
	query := `SELECT u.id, COALESCE(up.punts_total, 0) AS total, COALESCE(up.ultima_actualitzacio, u.data_creacio) AS ultima
			  FROM usuaris u
			  LEFT JOIN usuaris_punts up ON u.id = up.usuari_id
			  LEFT JOIN user_privacy pr ON pr.usuari_id = u.id`
	args := []interface{}{}
	clauses := []string{}
	if f.PublicOnly {
		publicCond := "(pr.profile_public IS NULL OR pr.profile_public = 1)"
		if h.style == "postgres" {
			publicCond = "(pr.profile_public IS NULL OR pr.profile_public = TRUE)"
		}
		clauses = append(clauses, publicCond)
	}
	if strings.TrimSpace(f.PreferredLang) != "" {
		clauses = append(clauses, "u.preferred_lang = ?")
		args = append(args, f.PreferredLang)
	}
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY total DESC, u.id ASC"
	if f.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, f.Limit)
		if f.Offset > 0 {
			query += " OFFSET ?"
			args = append(args, f.Offset)
		}
	} else if f.Offset > 0 {
		// offset without limit can be problematic; set a large limit to allow offset usage
		query += " LIMIT -1 OFFSET ?"
		args = append(args, f.Offset)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []UserPoints
	for rows.Next() {
		var up UserPoints
		var ts sql.NullString
		if err := rows.Scan(&up.UserID, &up.Total, &ts); err != nil {
			return nil, err
		}
		if ts.Valid {
			if parsed, perr := time.Parse(time.RFC3339, ts.String); perr == nil {
				up.UltimaActualitzacio = parsed
			} else if parsed, perr2 := time.Parse("2006-01-02 15:04:05", ts.String); perr2 == nil {
				up.UltimaActualitzacio = parsed
			}
		}
		res = append(res, up)
	}
	return res, nil
}

func (h sqlHelper) listEnabledAchievements() ([]Achievement, error) {
	query := `SELECT id, code, name, description, rarity, visibility, domain, is_enabled, is_repeatable, icon_media_item_id, rule_json, created_at, updated_at
	          FROM achievements WHERE is_enabled = 1 ORDER BY id`
	rows, err := h.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []Achievement
	for rows.Next() {
		var a Achievement
		if err := rows.Scan(&a.ID, &a.Code, &a.Name, &a.Description, &a.Rarity, &a.Visibility, &a.Domain,
			&a.IsEnabled, &a.IsRepeatable, &a.IconMediaItemID, &a.RuleJSON, &a.CreatedAt, &a.UpdatedAt); err != nil {
			return nil, err
		}
		res = append(res, a)
	}
	return res, nil
}

func (h sqlHelper) isAchievementEventActive(code string, at time.Time) (bool, error) {
	if strings.TrimSpace(code) == "" {
		return false, errors.New("code invalid")
	}
	if at.IsZero() {
		at = time.Now()
	}
	activeExpr := "is_enabled = 1"
	if h.style == "postgres" {
		activeExpr = "is_enabled = TRUE"
	}
	query := `SELECT COUNT(*) FROM achievement_events WHERE code = ? AND ` + activeExpr + ` AND start_at <= ? AND end_at >= ?`
	query = formatPlaceholders(h.style, query)
	var total int
	if err := h.db.QueryRow(query, code, at, at).Scan(&total); err != nil {
		return false, err
	}
	return total > 0, nil
}

func (h sqlHelper) listAchievements() ([]Achievement, error) {
	query := `SELECT id, code, name, description, rarity, visibility, domain, is_enabled, is_repeatable, icon_media_item_id, rule_json, created_at, updated_at
	          FROM achievements ORDER BY id`
	rows, err := h.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []Achievement
	for rows.Next() {
		var a Achievement
		if err := rows.Scan(&a.ID, &a.Code, &a.Name, &a.Description, &a.Rarity, &a.Visibility, &a.Domain,
			&a.IsEnabled, &a.IsRepeatable, &a.IconMediaItemID, &a.RuleJSON, &a.CreatedAt, &a.UpdatedAt); err != nil {
			return nil, err
		}
		res = append(res, a)
	}
	return res, nil
}

func (h sqlHelper) getAchievement(id int) (*Achievement, error) {
	query := `SELECT id, code, name, description, rarity, visibility, domain, is_enabled, is_repeatable, icon_media_item_id, rule_json, created_at, updated_at
	          FROM achievements WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, id)
	var a Achievement
	if err := row.Scan(&a.ID, &a.Code, &a.Name, &a.Description, &a.Rarity, &a.Visibility, &a.Domain,
		&a.IsEnabled, &a.IsRepeatable, &a.IconMediaItemID, &a.RuleJSON, &a.CreatedAt, &a.UpdatedAt); err != nil {
		return nil, err
	}
	return &a, nil
}

func (h sqlHelper) getAchievementByCode(code string) (*Achievement, error) {
	query := `SELECT id, code, name, description, rarity, visibility, domain, is_enabled, is_repeatable, icon_media_item_id, rule_json, created_at, updated_at
	          FROM achievements WHERE code = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, code)
	var a Achievement
	if err := row.Scan(&a.ID, &a.Code, &a.Name, &a.Description, &a.Rarity, &a.Visibility, &a.Domain,
		&a.IsEnabled, &a.IsRepeatable, &a.IconMediaItemID, &a.RuleJSON, &a.CreatedAt, &a.UpdatedAt); err != nil {
		return nil, err
	}
	return &a, nil
}

func (h sqlHelper) saveAchievement(a *Achievement) (int, error) {
	if a == nil {
		return 0, errors.New("achievement nil")
	}
	if a.ID == 0 {
		stmt := `INSERT INTO achievements (code, name, description, rarity, visibility, domain, is_enabled, is_repeatable, icon_media_item_id, rule_json, created_at, updated_at)
		         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
		if h.style == "postgres" {
			stmt += " RETURNING id"
		}
		stmt = formatPlaceholders(h.style, stmt)
		var icon interface{}
		if a.IconMediaItemID.Valid {
			icon = a.IconMediaItemID.Int64
		}
		if h.style == "postgres" {
			if err := h.db.QueryRow(stmt, a.Code, a.Name, a.Description, a.Rarity, a.Visibility, a.Domain, a.IsEnabled, a.IsRepeatable, icon, a.RuleJSON).Scan(&a.ID); err != nil {
				return 0, err
			}
			return a.ID, nil
		}
		res, err := h.db.Exec(stmt, a.Code, a.Name, a.Description, a.Rarity, a.Visibility, a.Domain, a.IsEnabled, a.IsRepeatable, icon, a.RuleJSON)
		if err != nil {
			return 0, err
		}
		id, _ := res.LastInsertId()
		a.ID = int(id)
		return a.ID, nil
	}
	stmt := formatPlaceholders(h.style, `UPDATE achievements SET code = ?, name = ?, description = ?, rarity = ?, visibility = ?, domain = ?, is_enabled = ?, is_repeatable = ?, icon_media_item_id = ?, rule_json = ?, updated_at = `+h.nowFun+` WHERE id = ?`)
	var icon interface{}
	if a.IconMediaItemID.Valid {
		icon = a.IconMediaItemID.Int64
	}
	if _, err := h.db.Exec(stmt, a.Code, a.Name, a.Description, a.Rarity, a.Visibility, a.Domain, a.IsEnabled, a.IsRepeatable, icon, a.RuleJSON, a.ID); err != nil {
		return 0, err
	}
	return a.ID, nil
}

func (h sqlHelper) awardAchievement(userID, achievementID int, status, metaJSON string) (bool, error) {
	if userID <= 0 || achievementID <= 0 {
		return false, errors.New("invalid ids")
	}
	if status == "" {
		status = "active"
	}
	meta := interface{}(nil)
	if strings.TrimSpace(metaJSON) != "" {
		meta = metaJSON
	}
	stmt := `INSERT INTO achievements_user (user_id, achievement_id, awarded_at, status, meta_json)
	         VALUES (?, ?, ` + h.nowFun + `, ?, ?)`
	if h.style == "postgres" {
		stmt += " ON CONFLICT (user_id, achievement_id) DO NOTHING"
	} else if h.style == "mysql" {
		stmt = "INSERT IGNORE INTO achievements_user (user_id, achievement_id, awarded_at, status, meta_json) VALUES (?, ?, " + h.nowFun + ", ?, ?)"
	} else {
		stmt = "INSERT OR IGNORE INTO achievements_user (user_id, achievement_id, awarded_at, status, meta_json) VALUES (?, ?, " + h.nowFun + ", ?, ?)"
	}
	stmt = formatPlaceholders(h.style, stmt)
	res, err := h.db.Exec(stmt, userID, achievementID, status, meta)
	if err != nil {
		return false, err
	}
	if rows, err := res.RowsAffected(); err == nil && rows > 0 {
		return true, nil
	}
	return false, nil
}

func (h sqlHelper) listUserAchievements(userID int) ([]AchievementUserView, error) {
	query := `
        SELECT a.id, a.code, a.name, a.description, a.rarity, a.visibility, a.domain, a.icon_media_item_id,
               m.public_id, au.awarded_at, au.status, au.meta_json
        FROM achievements_user au
        INNER JOIN achievements a ON a.id = au.achievement_id
        LEFT JOIN media_items m ON m.id = a.icon_media_item_id
        WHERE au.user_id = ?
        ORDER BY au.awarded_at DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []AchievementUserView
	for rows.Next() {
		var row AchievementUserView
		if err := rows.Scan(&row.AchievementID, &row.Code, &row.Name, &row.Description, &row.Rarity, &row.Visibility, &row.Domain,
			&row.IconMediaItemID, &row.IconPublicID, &row.AwardedAt, &row.Status, &row.MetaJSON); err != nil {
			return nil, err
		}
		res = append(res, row)
	}
	return res, nil
}

func (h sqlHelper) listUserShowcase(userID int) ([]AchievementShowcaseView, error) {
	query := `
        SELECT s.slot, a.id, a.code, a.name, a.description, a.rarity, a.visibility, a.domain, a.icon_media_item_id,
               m.public_id, au.awarded_at, au.status, au.meta_json
        FROM achievements_showcase s
        INNER JOIN achievements_user au ON au.user_id = s.user_id AND au.achievement_id = s.achievement_id
        INNER JOIN achievements a ON a.id = s.achievement_id
        LEFT JOIN media_items m ON m.id = a.icon_media_item_id
        WHERE s.user_id = ?
        ORDER BY s.slot ASC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []AchievementShowcaseView
	for rows.Next() {
		var row AchievementShowcaseView
		if err := rows.Scan(&row.Slot, &row.AchievementID, &row.Code, &row.Name, &row.Description, &row.Rarity, &row.Visibility, &row.Domain,
			&row.IconMediaItemID, &row.IconPublicID, &row.AwardedAt, &row.Status, &row.MetaJSON); err != nil {
			return nil, err
		}
		res = append(res, row)
	}
	return res, nil
}

func (h sqlHelper) setUserShowcaseSlot(userID, achievementID, slot int) error {
	if userID <= 0 || achievementID <= 0 || slot <= 0 {
		return errors.New("invalid ids")
	}
	clearStmt := formatPlaceholders(h.style, `DELETE FROM achievements_showcase WHERE user_id = ? AND achievement_id = ?`)
	if _, err := h.db.Exec(clearStmt, userID, achievementID); err != nil {
		return err
	}
	stmt := `INSERT INTO achievements_showcase (user_id, achievement_id, slot, created_at)
	         VALUES (?, ?, ?, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " ON CONFLICT (user_id, slot) DO UPDATE SET achievement_id = excluded.achievement_id, created_at = " + h.nowFun
	} else if h.style == "mysql" {
		stmt += " ON DUPLICATE KEY UPDATE achievement_id = VALUES(achievement_id), created_at = " + h.nowFun
	} else {
		stmt += " ON CONFLICT(user_id, slot) DO UPDATE SET achievement_id = excluded.achievement_id, created_at = " + h.nowFun
	}
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, userID, achievementID, slot)
	return err
}

func (h sqlHelper) clearUserShowcaseSlot(userID, slot int) error {
	if userID <= 0 || slot <= 0 {
		return errors.New("invalid ids")
	}
	stmt := formatPlaceholders(h.style, `DELETE FROM achievements_showcase WHERE user_id = ? AND slot = ?`)
	_, err := h.db.Exec(stmt, userID, slot)
	return err
}

func (h sqlHelper) achievementActivityFilters(f AchievementActivityFilter) (string, []string, []interface{}) {
	join := ""
	where := []string{"ua.usuari_id = ?"}
	args := []interface{}{f.UserID}
	if len(f.RuleCodes) > 0 {
		join = "INNER JOIN punts_regles pr ON pr.id = ua.regla_id"
		placeholders := strings.TrimRight(strings.Repeat("?,", len(f.RuleCodes)), ",")
		where = append(where, "pr.codi IN ("+placeholders+")")
		for _, code := range f.RuleCodes {
			args = append(args, code)
		}
	}
	if len(f.Actions) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(f.Actions)), ",")
		where = append(where, "ua.accio IN ("+placeholders+")")
		for _, action := range f.Actions {
			args = append(args, action)
		}
	}
	if len(f.ObjectTypes) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(f.ObjectTypes)), ",")
		where = append(where, "ua.objecte_tipus IN ("+placeholders+")")
		for _, typ := range f.ObjectTypes {
			args = append(args, typ)
		}
	}
	if len(f.Statuses) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(f.Statuses)), ",")
		where = append(where, "ua.estat IN ("+placeholders+")")
		for _, status := range f.Statuses {
			args = append(args, status)
		}
	}
	if !f.From.IsZero() {
		where = append(where, "ua.data_creacio >= ?")
		args = append(args, f.From)
	}
	if !f.To.IsZero() {
		where = append(where, "ua.data_creacio <= ?")
		args = append(args, f.To)
	}
	return join, where, args
}

func (h sqlHelper) countUserActivities(f AchievementActivityFilter) (int, error) {
	if f.UserID <= 0 {
		return 0, errors.New("user_id invalid")
	}
	join, where, args := h.achievementActivityFilters(f)
	query := `SELECT COUNT(*) FROM usuaris_activitat ua ` + join + ` WHERE ` + strings.Join(where, " AND ")
	query = formatPlaceholders(h.style, query)
	var total int
	if err := h.db.QueryRow(query, args...).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (h sqlHelper) countUserActivitiesDistinctObject(f AchievementActivityFilter) (int, error) {
	if f.UserID <= 0 {
		return 0, errors.New("user_id invalid")
	}
	join, where, args := h.achievementActivityFilters(f)
	where = append(where, "ua.objecte_id IS NOT NULL")
	distinctExpr := "COUNT(DISTINCT ua.objecte_tipus || ':' || ua.objecte_id)"
	if h.style == "mysql" || h.style == "postgres" {
		distinctExpr = "COUNT(DISTINCT CONCAT(ua.objecte_tipus, ':', ua.objecte_id))"
	}
	query := `SELECT ` + distinctExpr + ` FROM usuaris_activitat ua ` + join + ` WHERE ` + strings.Join(where, " AND ")
	query = formatPlaceholders(h.style, query)
	var total int
	if err := h.db.QueryRow(query, args...).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (h sqlHelper) sumUserActivityPoints(f AchievementActivityFilter) (int, error) {
	if f.UserID <= 0 {
		return 0, errors.New("user_id invalid")
	}
	join, where, args := h.achievementActivityFilters(f)
	query := `SELECT COALESCE(SUM(ua.punts), 0) FROM usuaris_activitat ua ` + join + ` WHERE ` + strings.Join(where, " AND ")
	query = formatPlaceholders(h.style, query)
	var total int
	if err := h.db.QueryRow(query, args...).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (h sqlHelper) listUserActivityDays(f AchievementActivityFilter) ([]time.Time, error) {
	if f.UserID <= 0 {
		return nil, errors.New("user_id invalid")
	}
	join, where, args := h.achievementActivityFilters(f)
	query := `SELECT DISTINCT DATE(ua.data_creacio) as day FROM usuaris_activitat ua ` + join + ` WHERE ` + strings.Join(where, " AND ") + ` ORDER BY day`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []time.Time
	for rows.Next() {
		var dayStr string
		if err := rows.Scan(&dayStr); err != nil {
			return nil, err
		}
		day, err := time.Parse("2006-01-02", dayStr)
		if err != nil {
			continue
		}
		res = append(res, day)
	}
	return res, nil
}

func (h sqlHelper) countRanking(f RankingFilter) (int, error) {
	query := `SELECT COUNT(1)
			  FROM usuaris u
			  LEFT JOIN user_privacy pr ON pr.usuari_id = u.id`
	args := []interface{}{}
	clauses := []string{}
	if f.PublicOnly {
		publicCond := "(pr.profile_public IS NULL OR pr.profile_public = 1)"
		if h.style == "postgres" {
			publicCond = "(pr.profile_public IS NULL OR pr.profile_public = TRUE)"
		}
		clauses = append(clauses, publicCond)
	}
	if strings.TrimSpace(f.PreferredLang) != "" {
		clauses = append(clauses, "u.preferred_lang = ?")
		args = append(args, f.PreferredLang)
	}
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, args...)
	var total int
	if err := row.Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (h sqlHelper) updatePersona(p *Persona) error {
	nomComplet := p.NomComplet
	if strings.TrimSpace(nomComplet) == "" {
		nomComplet = strings.TrimSpace(strings.Join([]string{p.Nom, p.Cognom1, p.Cognom2}, " "))
	}
	stmt := `
        UPDATE persona
        SET nom=?, cognom1=?, cognom2=?, municipi=?, arquevisbat=?, nom_complet=?, pagina=?, llibre=?, quinta=?, data_naixement=?, data_bateig=?, data_defuncio=?, ofici=?, estat_civil=?, updated_at=?, updated_by=?
        WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, p.Nom, p.Cognom1, p.Cognom2, p.Municipi, p.Arquebisbat, nomComplet, p.Pagina, p.Llibre, p.ModeracioMotiu, p.DataNaixement, p.DataBateig, p.DataDefuncio, p.Ofici, p.ModeracioEstat, time.Now(), p.UpdatedBy, p.ID)
	return err
}

func (h sqlHelper) updatePersonaModeracio(id int, estat, motiu string, moderatorID int) error {
	stmt := `UPDATE persona SET estat_civil = ?, quinta = ?, updated_at = ?, moderated_by = ?, moderated_at = ? WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	now := time.Now()
	_, err := h.db.Exec(stmt, estat, motiu, now, moderatorID, now, id)
	return err
}

func (h sqlHelper) updateArxiuModeracio(id int, estat, motiu string, moderatorID int) error {
	stmt := `UPDATE arxius SET moderation_status = ?, moderation_notes = ?, moderated_by = ?, moderated_at = ?, updated_at = ? WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	now := time.Now()
	_, err := h.db.Exec(stmt, estat, motiu, moderatorID, now, now, id)
	return err
}

func (h sqlHelper) updateLlibreModeracio(id int, estat, motiu string, moderatorID int) error {
	stmt := `UPDATE llibres SET moderation_status = ?, moderation_notes = ?, moderated_by = ?, moderated_at = ?, updated_at = ? WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	now := time.Now()
	_, err := h.db.Exec(stmt, estat, motiu, moderatorID, now, now, id)
	return err
}

func (h sqlHelper) updateNivellModeracio(id int, estat, motiu string, moderatorID int) error {
	stmt := `UPDATE nivells_administratius SET moderation_status = ?, moderation_notes = ?, moderated_by = ?, moderated_at = ?, updated_at = ? WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	now := time.Now()
	_, err := h.db.Exec(stmt, estat, motiu, moderatorID, now, now, id)
	return err
}

func (h sqlHelper) updateMunicipiModeracio(id int, estat, motiu string, moderatorID int) error {
	stmt := `UPDATE municipis SET moderation_status = ?, moderation_notes = ?, moderated_by = ?, moderated_at = ?, ultima_modificacio = ? WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	now := time.Now()
	_, err := h.db.Exec(stmt, estat, motiu, moderatorID, now, now, id)
	return err
}

func (h sqlHelper) updateArquebisbatModeracio(id int, estat, motiu string, moderatorID int) error {
	stmt := `UPDATE arquebisbats SET moderation_status = ?, moderation_notes = ?, moderated_by = ?, moderated_at = ?, updated_at = ? WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	now := time.Now()
	_, err := h.db.Exec(stmt, estat, motiu, moderatorID, now, now, id)
	return err
}

func (h sqlHelper) updateTranscripcioModeracio(id int, estat, motiu string, moderatorID int) error {
	stmt := `UPDATE transcripcions_raw SET moderation_status = ?, moderation_notes = ?, moderated_by = ?, moderated_at = ?, updated_at = ? WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	now := time.Now()
	_, err := h.db.Exec(stmt, estat, motiu, moderatorID, now, now, id)
	return err
}

func (h sqlHelper) updateTranscripcioModeracioWithDemografia(id int, estat, motiu string, moderatorID int, municipiID, year int, tipus string, delta int) error {
	if delta == 0 || municipiID <= 0 || year <= 0 || strings.TrimSpace(tipus) == "" {
		return h.updateTranscripcioModeracio(id, estat, motiu, moderatorID)
	}
	tx, err := h.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt := `UPDATE transcripcions_raw SET moderation_status = ?, moderation_notes = ?, moderated_by = ?, moderated_at = ?, updated_at = ? WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	now := time.Now()
	if _, err := tx.Exec(stmt, estat, motiu, moderatorID, now, now, id); err != nil {
		return err
	}
	if err := h.applyMunicipiDemografiaDeltaTx(tx, municipiID, year, tipus, delta); err != nil {
		return err
	}
	return tx.Commit()
}

// Paisos
func (h sqlHelper) listPaisos() ([]Pais, error) {
	query := `SELECT id, codi_iso2, codi_iso3, codi_pais_num FROM paisos ORDER BY codi_iso2`
	rows, err := h.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []Pais
	for rows.Next() {
		var p Pais
		if err := rows.Scan(&p.ID, &p.CodiISO2, &p.CodiISO3, &p.CodiPaisNum); err != nil {
			return nil, err
		}
		res = append(res, p)
	}
	return res, nil
}

func (h sqlHelper) getPais(id int) (*Pais, error) {
	query := formatPlaceholders(h.style, `SELECT id, codi_iso2, codi_iso3, codi_pais_num FROM paisos WHERE id = ?`)
	row := h.db.QueryRow(query, id)
	var p Pais
	if err := row.Scan(&p.ID, &p.CodiISO2, &p.CodiISO3, &p.CodiPaisNum); err != nil {
		return nil, err
	}
	return &p, nil
}

func (h sqlHelper) createPais(p *Pais) (int, error) {
	query := `
        INSERT INTO paisos (codi_iso2, codi_iso3, codi_pais_num, created_at, updated_at)
        VALUES (?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		query += ` RETURNING id`
	}
	query = formatPlaceholders(h.style, query)

	if h.style == "postgres" {
		if err := h.db.QueryRow(query, p.CodiISO2, p.CodiISO3, p.CodiPaisNum).Scan(&p.ID); err != nil {
			return 0, err
		}
		return p.ID, nil
	}
	res, err := h.db.Exec(query, p.CodiISO2, p.CodiISO3, p.CodiPaisNum)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		p.ID = int(id)
	}
	return p.ID, nil
}

func (h sqlHelper) updatePais(p *Pais) error {
	query := `
        UPDATE paisos
        SET codi_iso2 = ?, codi_iso3 = ?, codi_pais_num = ?, updated_at = ` + h.nowFun + `
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query, p.CodiISO2, p.CodiISO3, p.CodiPaisNum, p.ID)
	return err
}

// Nivells administratius
func (h sqlHelper) listNivells(f NivellAdminFilter) ([]NivellAdministratiu, error) {
	where := "1=1"
	args := []interface{}{}
	inClause := func(column string, ids []int) {
		if len(ids) == 0 {
			return
		}
		placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
		where += " AND " + column + " IN (" + placeholders + ")"
		for _, id := range ids {
			args = append(args, id)
		}
	}
	if f.PaisID > 0 {
		where += " AND n.pais_id = ?"
		args = append(args, f.PaisID)
	}
	if f.Nivel > 0 {
		where += " AND n.nivel = ?"
		args = append(args, f.Nivel)
	}
	if strings.TrimSpace(f.Text) != "" {
		where += " AND lower(n.nom_nivell) LIKE ?"
		args = append(args, "%"+strings.ToLower(strings.TrimSpace(f.Text))+"%")
	}
	if strings.TrimSpace(f.Estat) != "" {
		where += " AND n.estat = ?"
		args = append(args, strings.TrimSpace(f.Estat))
	}
	if strings.TrimSpace(f.Status) != "" {
		where += " AND n.moderation_status = ?"
		args = append(args, strings.TrimSpace(f.Status))
	}
	inClause("n.pais_id", f.AllowedPaisIDs)
	query := `
        SELECT n.id, n.pais_id, pi.codi_iso2, n.nivel, n.nom_nivell, n.tipus_nivell, n.codi_oficial, n.altres,
               n.parent_id, p.nom_nivell as parent_nom, n.any_inici, n.any_fi, n.estat,
               n.created_by, n.moderation_status, n.moderated_by, n.moderated_at, n.moderation_notes
        FROM nivells_administratius n
        LEFT JOIN nivells_administratius p ON p.id = n.parent_id
        LEFT JOIN paisos pi ON pi.id = n.pais_id
        WHERE ` + where + `
        ORDER BY n.nivel, n.nom_nivell`
	if f.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, f.Limit)
		if f.Offset > 0 {
			query += " OFFSET ?"
			args = append(args, f.Offset)
		}
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []NivellAdministratiu
	for rows.Next() {
		var n NivellAdministratiu
		if err := rows.Scan(&n.ID, &n.PaisID, &n.PaisISO2, &n.Nivel, &n.NomNivell, &n.TipusNivell, &n.CodiOficial, &n.Altres, &n.ParentID, &n.ParentNom, &n.AnyInici, &n.AnyFi, &n.Estat,
			&n.CreatedBy, &n.ModeracioEstat, &n.ModeratedBy, &n.ModeratedAt, &n.ModeracioMotiu); err != nil {
			return nil, err
		}
		res = append(res, n)
	}
	return res, nil
}

func (h sqlHelper) countNivells(f NivellAdminFilter) (int, error) {
	where := "1=1"
	args := []interface{}{}
	inClause := func(column string, ids []int) {
		if len(ids) == 0 {
			return
		}
		placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
		where += " AND " + column + " IN (" + placeholders + ")"
		for _, id := range ids {
			args = append(args, id)
		}
	}
	if f.PaisID > 0 {
		where += " AND n.pais_id = ?"
		args = append(args, f.PaisID)
	}
	if f.Nivel > 0 {
		where += " AND n.nivel = ?"
		args = append(args, f.Nivel)
	}
	if strings.TrimSpace(f.Text) != "" {
		where += " AND lower(n.nom_nivell) LIKE ?"
		args = append(args, "%"+strings.ToLower(strings.TrimSpace(f.Text))+"%")
	}
	if strings.TrimSpace(f.Estat) != "" {
		where += " AND n.estat = ?"
		args = append(args, strings.TrimSpace(f.Estat))
	}
	if strings.TrimSpace(f.Status) != "" {
		where += " AND n.moderation_status = ?"
		args = append(args, strings.TrimSpace(f.Status))
	}
	inClause("n.pais_id", f.AllowedPaisIDs)
	query := `SELECT COUNT(*) FROM nivells_administratius n WHERE ` + where
	query = formatPlaceholders(h.style, query)
	var total int
	if err := h.db.QueryRow(query, args...).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (h sqlHelper) getNivell(id int) (*NivellAdministratiu, error) {
	query := `
        SELECT n.id, n.pais_id, pi.codi_iso2, n.nivel, n.nom_nivell, n.tipus_nivell, n.codi_oficial, n.altres,
               n.parent_id, p.nom_nivell as parent_nom, n.any_inici, n.any_fi, n.estat,
               n.created_by, n.moderation_status, n.moderated_by, n.moderated_at, n.moderation_notes
        FROM nivells_administratius n
        LEFT JOIN nivells_administratius p ON p.id = n.parent_id
        LEFT JOIN paisos pi ON pi.id = n.pais_id
        WHERE n.id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, id)
	var n NivellAdministratiu
	if err := row.Scan(&n.ID, &n.PaisID, &n.PaisISO2, &n.Nivel, &n.NomNivell, &n.TipusNivell, &n.CodiOficial, &n.Altres, &n.ParentID, &n.ParentNom, &n.AnyInici, &n.AnyFi, &n.Estat,
		&n.CreatedBy, &n.ModeracioEstat, &n.ModeratedBy, &n.ModeratedAt, &n.ModeracioMotiu); err != nil {
		return nil, err
	}
	return &n, nil
}

func (h sqlHelper) createNivell(n *NivellAdministratiu) (int, error) {
	query := `
        INSERT INTO nivells_administratius
            (pais_id, nivel, nom_nivell, tipus_nivell, codi_oficial, altres, parent_id, any_inici, any_fi, estat,
             created_by, moderation_status, moderated_by, moderated_at, moderation_notes, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		query += ` RETURNING id`
	}
	query = formatPlaceholders(h.style, query)
	args := []interface{}{n.PaisID, n.Nivel, n.NomNivell, n.TipusNivell, n.CodiOficial, n.Altres, n.ParentID, n.AnyInici, n.AnyFi, n.Estat,
		n.CreatedBy, n.ModeracioEstat, n.ModeratedBy, n.ModeratedAt, n.ModeracioMotiu}
	if h.style == "postgres" {
		if err := h.db.QueryRow(query, args...).Scan(&n.ID); err != nil {
			return 0, err
		}
		return n.ID, nil
	}
	res, err := h.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		n.ID = int(id)
	}
	return n.ID, nil
}

func (h sqlHelper) updateNivell(n *NivellAdministratiu) error {
	query := `
        UPDATE nivells_administratius
        SET pais_id = ?, nivel = ?, nom_nivell = ?, tipus_nivell = ?, codi_oficial = ?, altres = ?, parent_id = ?, any_inici = ?, any_fi = ?, estat = ?,
            moderation_status = ?, moderated_by = ?, moderated_at = ?, moderation_notes = ?, updated_at = ` + h.nowFun + `
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query, n.PaisID, n.Nivel, n.NomNivell, n.TipusNivell, n.CodiOficial, n.Altres, n.ParentID, n.AnyInici, n.AnyFi, n.Estat,
		n.ModeracioEstat, n.ModeratedBy, n.ModeratedAt, n.ModeracioMotiu, n.ID)
	return err
}

// Municipis
func (h sqlHelper) listMunicipis(f MunicipiFilter) ([]MunicipiRow, error) {
	where := "1=1"
	args := []interface{}{}
	inClause := func(column string, ids []int) {
		if len(ids) == 0 {
			return
		}
		placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
		where += " AND " + column + " IN (" + placeholders + ")"
		for _, id := range ids {
			args = append(args, id)
		}
	}
	if strings.TrimSpace(f.Text) != "" {
		where += " AND lower(m.nom) LIKE ?"
		args = append(args, "%"+strings.ToLower(strings.TrimSpace(f.Text))+"%")
	}
	if strings.TrimSpace(f.Estat) != "" {
		where += " AND m.estat = ?"
		args = append(args, strings.TrimSpace(f.Estat))
	}
	if strings.TrimSpace(f.Status) != "" {
		where += " AND m.moderation_status = ?"
		args = append(args, strings.TrimSpace(f.Status))
	}
	if f.PaisID > 0 {
		where += " AND na1.id = ?"
		args = append(args, f.PaisID)
	}
	if f.NivellID > 0 {
		where += " AND (m.nivell_administratiu_id_1 = ? OR m.nivell_administratiu_id_2 = ? OR m.nivell_administratiu_id_3 = ? OR m.nivell_administratiu_id_4 = ? OR m.nivell_administratiu_id_5 = ? OR m.nivell_administratiu_id_6 = ? OR m.nivell_administratiu_id_7 = ?)"
		for i := 0; i < 7; i++ {
			args = append(args, f.NivellID)
		}
	}
	inClause("m.id", f.AllowedMunicipiIDs)
	inClause("m.nivell_administratiu_id_3", f.AllowedProvinciaIDs)
	inClause("m.nivell_administratiu_id_4", f.AllowedComarcaIDs)
	inClause("na1.id", f.AllowedPaisIDs)
	query := `
        SELECT m.id, m.nom, m.tipus, m.estat, m.codi_postal,
               m.nivell_administratiu_id_1,
               m.nivell_administratiu_id_3,
               m.nivell_administratiu_id_4,
               na1.nom_nivell AS pais_nom,
               na3.nom_nivell AS provincia_nom,
               na4.nom_nivell AS comarca_nom,
               m.moderation_status,
               m.created_by,
               m.data_creacio
        FROM municipis m
        LEFT JOIN nivells_administratius na1 ON na1.id = m.nivell_administratiu_id_1
        LEFT JOIN nivells_administratius na3 ON na3.id = m.nivell_administratiu_id_3
        LEFT JOIN nivells_administratius na4 ON na4.id = m.nivell_administratiu_id_4
        WHERE ` + where + `
        ORDER BY m.nom`
	if f.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, f.Limit)
		if f.Offset > 0 {
			query += " OFFSET ?"
			args = append(args, f.Offset)
		}
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []MunicipiRow
	for rows.Next() {
		var r MunicipiRow
		if err := rows.Scan(
			&r.ID, &r.Nom, &r.Tipus, &r.Estat, &r.CodiPostal,
			&r.PaisID, &r.ProvinciaID, &r.ComarcaID,
			&r.PaisNom, &r.ProvNom, &r.Comarca, &r.ModeracioEstat,
			&r.CreatedBy, &r.CreatedAt,
		); err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}

func (h sqlHelper) countMunicipis(f MunicipiFilter) (int, error) {
	where := "1=1"
	args := []interface{}{}
	inClause := func(column string, ids []int) {
		if len(ids) == 0 {
			return
		}
		placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
		where += " AND " + column + " IN (" + placeholders + ")"
		for _, id := range ids {
			args = append(args, id)
		}
	}
	if strings.TrimSpace(f.Text) != "" {
		where += " AND lower(m.nom) LIKE ?"
		args = append(args, "%"+strings.ToLower(strings.TrimSpace(f.Text))+"%")
	}
	if strings.TrimSpace(f.Estat) != "" {
		where += " AND m.estat = ?"
		args = append(args, strings.TrimSpace(f.Estat))
	}
	if strings.TrimSpace(f.Status) != "" {
		where += " AND m.moderation_status = ?"
		args = append(args, strings.TrimSpace(f.Status))
	}
	if f.PaisID > 0 {
		where += " AND na1.id = ?"
		args = append(args, f.PaisID)
	}
	if f.NivellID > 0 {
		where += " AND (m.nivell_administratiu_id_1 = ? OR m.nivell_administratiu_id_2 = ? OR m.nivell_administratiu_id_3 = ? OR m.nivell_administratiu_id_4 = ? OR m.nivell_administratiu_id_5 = ? OR m.nivell_administratiu_id_6 = ? OR m.nivell_administratiu_id_7 = ?)"
		for i := 0; i < 7; i++ {
			args = append(args, f.NivellID)
		}
	}
	inClause("m.id", f.AllowedMunicipiIDs)
	inClause("m.nivell_administratiu_id_3", f.AllowedProvinciaIDs)
	inClause("m.nivell_administratiu_id_4", f.AllowedComarcaIDs)
	inClause("na1.id", f.AllowedPaisIDs)
	query := `
        SELECT COUNT(*)
        FROM municipis m
        LEFT JOIN nivells_administratius na1 ON na1.id = m.nivell_administratiu_id_1
        WHERE ` + where
	query = formatPlaceholders(h.style, query)
	var total int
	if err := h.db.QueryRow(query, args...).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (h sqlHelper) listMunicipisBrowse(f MunicipiBrowseFilter) ([]MunicipiBrowseRow, error) {
	where := "1=1"
	args := []interface{}{}
	inClause := func(column string, ids []int) {
		if len(ids) == 0 {
			return
		}
		placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
		where += " AND " + column + " IN (" + placeholders + ")"
		for _, id := range ids {
			args = append(args, id)
		}
	}
	if strings.TrimSpace(f.Text) != "" {
		text := "%" + strings.ToLower(strings.TrimSpace(f.Text)) + "%"
		where += " AND (lower(m.nom) LIKE ? OR lower(na1.nom_nivell) LIKE ? OR lower(na2.nom_nivell) LIKE ? OR lower(na3.nom_nivell) LIKE ? OR lower(na4.nom_nivell) LIKE ? OR lower(na5.nom_nivell) LIKE ? OR lower(na6.nom_nivell) LIKE ? OR lower(na7.nom_nivell) LIKE ?)"
		for i := 0; i < 8; i++ {
			args = append(args, text)
		}
	}
	if strings.TrimSpace(f.Estat) != "" {
		where += " AND m.estat = ?"
		args = append(args, strings.TrimSpace(f.Estat))
	}
	if strings.TrimSpace(f.Status) != "" {
		where += " AND m.moderation_status = ?"
		args = append(args, strings.TrimSpace(f.Status))
	}
	if f.PaisID > 0 {
		where += " AND na1.pais_id = ?"
		args = append(args, f.PaisID)
	}
	if f.MunicipiID > 0 {
		where += " AND m.id = ?"
		args = append(args, f.MunicipiID)
	}
	if f.NivellID > 0 {
		where += " AND (m.nivell_administratiu_id_1 = ? OR m.nivell_administratiu_id_2 = ? OR m.nivell_administratiu_id_3 = ? OR m.nivell_administratiu_id_4 = ? OR m.nivell_administratiu_id_5 = ? OR m.nivell_administratiu_id_6 = ? OR m.nivell_administratiu_id_7 = ?)"
		for i := 0; i < 7; i++ {
			args = append(args, f.NivellID)
		}
	}
	if strings.TrimSpace(f.Tipus) != "" {
		where += " AND m.tipus = ?"
		args = append(args, strings.TrimSpace(f.Tipus))
	}
	for i, id := range f.LevelIDs {
		if id <= 0 {
			continue
		}
		where += " AND m.nivell_administratiu_id_" + strconv.Itoa(i+1) + " = ?"
		args = append(args, id)
	}
	inClause("m.id", f.AllowedMunicipiIDs)
	inClause("m.nivell_administratiu_id_3", f.AllowedProvinciaIDs)
	inClause("m.nivell_administratiu_id_4", f.AllowedComarcaIDs)
	inClause("na1.pais_id", f.AllowedPaisIDs)
	orderBy := "m.nom"
	switch strings.TrimSpace(f.Sort) {
	case "pais":
		orderBy = "na1.nom_nivell"
	case "level1":
		orderBy = "na1.nom_nivell"
	case "level2":
		orderBy = "na2.nom_nivell"
	case "level3":
		orderBy = "na3.nom_nivell"
	case "level4":
		orderBy = "na4.nom_nivell"
	case "level5":
		orderBy = "na5.nom_nivell"
	case "level6":
		orderBy = "na6.nom_nivell"
	case "level7":
		orderBy = "na7.nom_nivell"
	case "nom":
		orderBy = "m.nom"
	}
	dir := "ASC"
	if strings.EqualFold(strings.TrimSpace(f.SortDir), "desc") {
		dir = "DESC"
	}
	query := `
		SELECT m.id, m.nom, m.tipus, m.estat, m.codi_postal, m.moderation_status,
		       m.nivell_administratiu_id_1, m.nivell_administratiu_id_2, m.nivell_administratiu_id_3,
		       m.nivell_administratiu_id_4, m.nivell_administratiu_id_5, m.nivell_administratiu_id_6, m.nivell_administratiu_id_7,
		       na1.nom_nivell, na2.nom_nivell, na3.nom_nivell, na4.nom_nivell, na5.nom_nivell, na6.nom_nivell, na7.nom_nivell,
		       m.latitud, m.longitud,
		       COUNT(l.id) AS llibres_total,
		       COALESCE(CAST(ROUND(AVG(CASE WHEN s.percentatge IS NOT NULL THEN s.percentatge ELSE CASE WHEN l.indexacio_completa = 1 THEN 100 ELSE 0 END END)) AS INTEGER), 0) AS index_percent
		FROM municipis m
		LEFT JOIN nivells_administratius na1 ON na1.id = m.nivell_administratiu_id_1
		LEFT JOIN nivells_administratius na2 ON na2.id = m.nivell_administratiu_id_2
		LEFT JOIN nivells_administratius na3 ON na3.id = m.nivell_administratiu_id_3
		LEFT JOIN nivells_administratius na4 ON na4.id = m.nivell_administratiu_id_4
		LEFT JOIN nivells_administratius na5 ON na5.id = m.nivell_administratiu_id_5
		LEFT JOIN nivells_administratius na6 ON na6.id = m.nivell_administratiu_id_6
		LEFT JOIN nivells_administratius na7 ON na7.id = m.nivell_administratiu_id_7
		LEFT JOIN llibres l ON l.municipi_id = m.id
		LEFT JOIN llibres_indexacio_stats s ON s.llibre_id = l.id
		WHERE ` + where + `
		GROUP BY m.id, m.nom, m.tipus, m.estat, m.codi_postal, m.moderation_status,
		         m.nivell_administratiu_id_1, m.nivell_administratiu_id_2, m.nivell_administratiu_id_3,
		         m.nivell_administratiu_id_4, m.nivell_administratiu_id_5, m.nivell_administratiu_id_6, m.nivell_administratiu_id_7,
		         na1.nom_nivell, na2.nom_nivell, na3.nom_nivell, na4.nom_nivell, na5.nom_nivell, na6.nom_nivell, na7.nom_nivell,
		         m.latitud, m.longitud
		ORDER BY ` + orderBy + ` ` + dir + `, m.nom`
	if f.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, f.Limit)
		if f.Offset > 0 {
			query += " OFFSET ?"
			args = append(args, f.Offset)
		}
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []MunicipiBrowseRow
	for rows.Next() {
		var r MunicipiBrowseRow
		if err := rows.Scan(
			&r.ID, &r.Nom, &r.Tipus, &r.Estat, &r.CodiPostal, &r.ModeracioEstat,
			&r.LevelIDs[0], &r.LevelIDs[1], &r.LevelIDs[2], &r.LevelIDs[3], &r.LevelIDs[4], &r.LevelIDs[5], &r.LevelIDs[6],
			&r.LevelNames[0], &r.LevelNames[1], &r.LevelNames[2], &r.LevelNames[3], &r.LevelNames[4], &r.LevelNames[5], &r.LevelNames[6],
			&r.Latitud, &r.Longitud,
			&r.RegistresTotal, &r.RegistresIndexats,
		); err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}

func (h sqlHelper) countMunicipisBrowse(f MunicipiBrowseFilter) (int, error) {
	where := "1=1"
	args := []interface{}{}
	inClause := func(column string, ids []int) {
		if len(ids) == 0 {
			return
		}
		placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
		where += " AND " + column + " IN (" + placeholders + ")"
		for _, id := range ids {
			args = append(args, id)
		}
	}
	if strings.TrimSpace(f.Text) != "" {
		text := "%" + strings.ToLower(strings.TrimSpace(f.Text)) + "%"
		where += " AND (lower(m.nom) LIKE ? OR lower(na1.nom_nivell) LIKE ? OR lower(na2.nom_nivell) LIKE ? OR lower(na3.nom_nivell) LIKE ? OR lower(na4.nom_nivell) LIKE ? OR lower(na5.nom_nivell) LIKE ? OR lower(na6.nom_nivell) LIKE ? OR lower(na7.nom_nivell) LIKE ?)"
		for i := 0; i < 8; i++ {
			args = append(args, text)
		}
	}
	if strings.TrimSpace(f.Estat) != "" {
		where += " AND m.estat = ?"
		args = append(args, strings.TrimSpace(f.Estat))
	}
	if strings.TrimSpace(f.Status) != "" {
		where += " AND m.moderation_status = ?"
		args = append(args, strings.TrimSpace(f.Status))
	}
	if f.PaisID > 0 {
		where += " AND na1.pais_id = ?"
		args = append(args, f.PaisID)
	}
	if f.MunicipiID > 0 {
		where += " AND m.id = ?"
		args = append(args, f.MunicipiID)
	}
	if f.NivellID > 0 {
		where += " AND (m.nivell_administratiu_id_1 = ? OR m.nivell_administratiu_id_2 = ? OR m.nivell_administratiu_id_3 = ? OR m.nivell_administratiu_id_4 = ? OR m.nivell_administratiu_id_5 = ? OR m.nivell_administratiu_id_6 = ? OR m.nivell_administratiu_id_7 = ?)"
		for i := 0; i < 7; i++ {
			args = append(args, f.NivellID)
		}
	}
	if strings.TrimSpace(f.Tipus) != "" {
		where += " AND m.tipus = ?"
		args = append(args, strings.TrimSpace(f.Tipus))
	}
	for i, id := range f.LevelIDs {
		if id <= 0 {
			continue
		}
		where += " AND m.nivell_administratiu_id_" + strconv.Itoa(i+1) + " = ?"
		args = append(args, id)
	}
	inClause("m.id", f.AllowedMunicipiIDs)
	inClause("m.nivell_administratiu_id_3", f.AllowedProvinciaIDs)
	inClause("m.nivell_administratiu_id_4", f.AllowedComarcaIDs)
	inClause("na1.pais_id", f.AllowedPaisIDs)
	query := `
		SELECT COUNT(*)
		FROM municipis m
		LEFT JOIN nivells_administratius na1 ON na1.id = m.nivell_administratiu_id_1
		LEFT JOIN nivells_administratius na2 ON na2.id = m.nivell_administratiu_id_2
		LEFT JOIN nivells_administratius na3 ON na3.id = m.nivell_administratiu_id_3
		LEFT JOIN nivells_administratius na4 ON na4.id = m.nivell_administratiu_id_4
		LEFT JOIN nivells_administratius na5 ON na5.id = m.nivell_administratiu_id_5
		LEFT JOIN nivells_administratius na6 ON na6.id = m.nivell_administratiu_id_6
		LEFT JOIN nivells_administratius na7 ON na7.id = m.nivell_administratiu_id_7
		WHERE ` + where
	query = formatPlaceholders(h.style, query)
	var total int
	if err := h.db.QueryRow(query, args...).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (h sqlHelper) suggestMunicipis(f MunicipiBrowseFilter) ([]MunicipiSuggestRow, error) {
	where := "1=1"
	args := []interface{}{}
	inClause := func(column string, ids []int) {
		if len(ids) == 0 {
			return
		}
		placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
		where += " AND " + column + " IN (" + placeholders + ")"
		for _, id := range ids {
			args = append(args, id)
		}
	}
	if strings.TrimSpace(f.Text) != "" {
		text := "%" + strings.ToLower(strings.TrimSpace(f.Text)) + "%"
		where += " AND (lower(m.nom) LIKE ? OR lower(na1.nom_nivell) LIKE ? OR lower(na2.nom_nivell) LIKE ? OR lower(na3.nom_nivell) LIKE ? OR lower(na4.nom_nivell) LIKE ? OR lower(na5.nom_nivell) LIKE ? OR lower(na6.nom_nivell) LIKE ? OR lower(na7.nom_nivell) LIKE ?)"
		for i := 0; i < 8; i++ {
			args = append(args, text)
		}
	}
	if strings.TrimSpace(f.Estat) != "" {
		where += " AND m.estat = ?"
		args = append(args, strings.TrimSpace(f.Estat))
	}
	if strings.TrimSpace(f.Status) != "" {
		where += " AND m.moderation_status = ?"
		args = append(args, strings.TrimSpace(f.Status))
	}
	if f.PaisID > 0 {
		where += " AND na1.pais_id = ?"
		args = append(args, f.PaisID)
	}
	if f.MunicipiID > 0 {
		where += " AND m.id = ?"
		args = append(args, f.MunicipiID)
	}
	if f.NivellID > 0 {
		where += " AND (m.nivell_administratiu_id_1 = ? OR m.nivell_administratiu_id_2 = ? OR m.nivell_administratiu_id_3 = ? OR m.nivell_administratiu_id_4 = ? OR m.nivell_administratiu_id_5 = ? OR m.nivell_administratiu_id_6 = ? OR m.nivell_administratiu_id_7 = ?)"
		for i := 0; i < 7; i++ {
			args = append(args, f.NivellID)
		}
	}
	if strings.TrimSpace(f.Tipus) != "" {
		where += " AND m.tipus = ?"
		args = append(args, strings.TrimSpace(f.Tipus))
	}
	for i, id := range f.LevelIDs {
		if id <= 0 {
			continue
		}
		where += " AND m.nivell_administratiu_id_" + strconv.Itoa(i+1) + " = ?"
		args = append(args, id)
	}
	inClause("m.id", f.AllowedMunicipiIDs)
	inClause("m.nivell_administratiu_id_3", f.AllowedProvinciaIDs)
	inClause("m.nivell_administratiu_id_4", f.AllowedComarcaIDs)
	inClause("na1.pais_id", f.AllowedPaisIDs)
	query := `
		SELECT m.id, m.nom, m.tipus,
		       COALESCE(na1.pais_id, na2.pais_id, na3.pais_id, na4.pais_id, na5.pais_id, na6.pais_id, na7.pais_id, 0) AS pais_id,
		       m.nivell_administratiu_id_1, m.nivell_administratiu_id_2, m.nivell_administratiu_id_3,
		       m.nivell_administratiu_id_4, m.nivell_administratiu_id_5, m.nivell_administratiu_id_6, m.nivell_administratiu_id_7,
		       na1.nom_nivell, na2.nom_nivell, na3.nom_nivell, na4.nom_nivell, na5.nom_nivell, na6.nom_nivell, na7.nom_nivell,
		       na1.tipus_nivell, na2.tipus_nivell, na3.tipus_nivell, na4.tipus_nivell, na5.tipus_nivell, na6.tipus_nivell, na7.tipus_nivell,
		       m.latitud, m.longitud
		FROM municipis m
		LEFT JOIN nivells_administratius na1 ON na1.id = m.nivell_administratiu_id_1
		LEFT JOIN nivells_administratius na2 ON na2.id = m.nivell_administratiu_id_2
		LEFT JOIN nivells_administratius na3 ON na3.id = m.nivell_administratiu_id_3
		LEFT JOIN nivells_administratius na4 ON na4.id = m.nivell_administratiu_id_4
		LEFT JOIN nivells_administratius na5 ON na5.id = m.nivell_administratiu_id_5
		LEFT JOIN nivells_administratius na6 ON na6.id = m.nivell_administratiu_id_6
		LEFT JOIN nivells_administratius na7 ON na7.id = m.nivell_administratiu_id_7
		WHERE ` + where + `
		ORDER BY m.nom`
	limit := f.Limit
	if limit <= 0 {
		limit = 10
	}
	query += " LIMIT ?"
	args = append(args, limit)
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []MunicipiSuggestRow
	for rows.Next() {
		var r MunicipiSuggestRow
		if err := rows.Scan(
			&r.ID, &r.Nom, &r.Tipus, &r.PaisID,
			&r.LevelIDs[0], &r.LevelIDs[1], &r.LevelIDs[2], &r.LevelIDs[3], &r.LevelIDs[4], &r.LevelIDs[5], &r.LevelIDs[6],
			&r.LevelNames[0], &r.LevelNames[1], &r.LevelNames[2], &r.LevelNames[3], &r.LevelNames[4], &r.LevelNames[5], &r.LevelNames[6],
			&r.LevelTypes[0], &r.LevelTypes[1], &r.LevelTypes[2], &r.LevelTypes[3], &r.LevelTypes[4], &r.LevelTypes[5], &r.LevelTypes[6],
			&r.Latitud, &r.Longitud,
		); err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}

func (h sqlHelper) getMunicipi(id int) (*Municipi, error) {
	query := `
        SELECT id, nom, municipi_id, tipus,
               nivell_administratiu_id_1, nivell_administratiu_id_2, nivell_administratiu_id_3,
               nivell_administratiu_id_4, nivell_administratiu_id_5, nivell_administratiu_id_6, nivell_administratiu_id_7,
               codi_postal, latitud, longitud, what3words, web, wikipedia, altres, estat,
               created_by, moderation_status, moderated_by, moderated_at, moderation_notes
        FROM municipis WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, id)
	var m Municipi
	if err := row.Scan(
		&m.ID, &m.Nom, &m.MunicipiID, &m.Tipus,
		&m.NivellAdministratiuID[0], &m.NivellAdministratiuID[1], &m.NivellAdministratiuID[2],
		&m.NivellAdministratiuID[3], &m.NivellAdministratiuID[4], &m.NivellAdministratiuID[5], &m.NivellAdministratiuID[6],
		&m.CodiPostal, &m.Latitud, &m.Longitud, &m.What3Words, &m.Web, &m.Wikipedia, &m.Altres, &m.Estat,
		&m.CreatedBy, &m.ModeracioEstat, &m.ModeratedBy, &m.ModeratedAt, &m.ModeracioMotiu,
	); err != nil {
		return nil, err
	}
	return &m, nil
}

func (h sqlHelper) createMunicipi(m *Municipi) (int, error) {
	query := `
        INSERT INTO municipis (
            nom,
            municipi_id,
            tipus,
            nivell_administratiu_id_1,
            nivell_administratiu_id_2,
            nivell_administratiu_id_3,
            nivell_administratiu_id_4,
            nivell_administratiu_id_5,
            nivell_administratiu_id_6,
            nivell_administratiu_id_7,
            codi_postal,
            latitud,
            longitud,
            what3words,
            web,
            wikipedia,
            altres,
            estat,
            created_by,
            moderation_status,
            moderated_by,
            moderated_at,
            moderation_notes,
            data_creacio,
            ultima_modificacio
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `
        )`
	if h.style == "postgres" {
		query += ` RETURNING id`
	}
	query = formatPlaceholders(h.style, query)
	args := []interface{}{
		m.Nom, m.MunicipiID, m.Tipus,
		m.NivellAdministratiuID[0], m.NivellAdministratiuID[1], m.NivellAdministratiuID[2],
		m.NivellAdministratiuID[3], m.NivellAdministratiuID[4], m.NivellAdministratiuID[5], m.NivellAdministratiuID[6],
		m.CodiPostal, m.Latitud, m.Longitud, m.What3Words, m.Web, m.Wikipedia, m.Altres, m.Estat, m.CreatedBy, m.ModeracioEstat, m.ModeratedBy, m.ModeratedAt, m.ModeracioMotiu,
	}
	if h.style == "postgres" {
		if err := h.db.QueryRow(query, args...).Scan(&m.ID); err != nil {
			fmt.Printf("[DB][createMunicipi][%s] err=%v cols=25 args=%d query=%s\n", h.style, err, len(args), query)
			return 0, err
		}
		return m.ID, nil
	}
	res, err := h.db.Exec(query, args...)
	if err != nil {
		fmt.Printf("[DB][createMunicipi][%s] err=%v cols=25 args=%d query=%s\n", h.style, err, len(args), query)
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		m.ID = int(id)
	}
	return m.ID, nil
}

func (h sqlHelper) updateMunicipi(m *Municipi) error {
	query := `
        UPDATE municipis SET
            nom=?, municipi_id=?, tipus=?,
            nivell_administratiu_id_1=?, nivell_administratiu_id_2=?, nivell_administratiu_id_3=?,
            nivell_administratiu_id_4=?, nivell_administratiu_id_5=?, nivell_administratiu_id_6=?, nivell_administratiu_id_7=?,
            codi_postal=?, latitud=?, longitud=?, what3words=?, web=?, wikipedia=?, altres=?, estat=?,
            moderation_status=?, moderated_by=?, moderated_at=?, moderation_notes=?,
            ultima_modificacio=` + h.nowFun + `
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query,
		m.Nom, m.MunicipiID, m.Tipus,
		m.NivellAdministratiuID[0], m.NivellAdministratiuID[1], m.NivellAdministratiuID[2],
		m.NivellAdministratiuID[3], m.NivellAdministratiuID[4], m.NivellAdministratiuID[5], m.NivellAdministratiuID[6],
		m.CodiPostal, m.Latitud, m.Longitud, m.What3Words, m.Web, m.Wikipedia, m.Altres, m.Estat,
		m.ModeracioEstat, m.ModeratedBy, m.ModeratedAt, m.ModeracioMotiu,
		m.ID)
	return err
}

func (h sqlHelper) listCodisPostals(municipiID int) ([]CodiPostal, error) {
	query := formatPlaceholders(h.style, `
        SELECT id, municipi_id, codi_postal, zona, desde, fins
        FROM codis_postals
        WHERE municipi_id = ? ORDER BY codi_postal`)
	rows, err := h.db.Query(query, municipiID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []CodiPostal
	for rows.Next() {
		var cp CodiPostal
		if err := rows.Scan(&cp.ID, &cp.MunicipiID, &cp.CodiPostal, &cp.Zona, &cp.Desde, &cp.Fins); err != nil {
			return nil, err
		}
		res = append(res, cp)
	}
	return res, nil
}

func (h sqlHelper) saveCodiPostal(cp *CodiPostal) (int, error) {
	if cp.ID == 0 {
		query := `
            INSERT INTO codis_postals (municipi_id, codi_postal, zona, desde, fins)
            VALUES (?, ?, ?, ?, ?)`
		query = formatPlaceholders(h.style, query)
		if h.style == "postgres" {
			query += ` RETURNING id`
			if err := h.db.QueryRow(query, cp.MunicipiID, cp.CodiPostal, cp.Zona, cp.Desde, cp.Fins).Scan(&cp.ID); err != nil {
				return 0, err
			}
			return cp.ID, nil
		}
		res, err := h.db.Exec(query, cp.MunicipiID, cp.CodiPostal, cp.Zona, cp.Desde, cp.Fins)
		if err != nil {
			return 0, err
		}
		if id, err := res.LastInsertId(); err == nil {
			cp.ID = int(id)
		}
		return cp.ID, nil
	}
	query := `
        UPDATE codis_postals
        SET codi_postal=?, zona=?, desde=?, fins=?
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query, cp.CodiPostal, cp.Zona, cp.Desde, cp.Fins, cp.ID)
	return cp.ID, err
}

// Noms històrics
func (h sqlHelper) listNomsHistorics(entitatTipus string, entitatID int) ([]NomHistoric, error) {
	query := `
        SELECT id, entitat_tipus, entitat_id, nom, any_inici, any_fi, pais_regne, distribucio_geografica, font
        FROM noms_historics
        WHERE entitat_tipus = ? AND entitat_id = ?
        ORDER BY any_inici`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, entitatTipus, entitatID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []NomHistoric
	for rows.Next() {
		var nh NomHistoric
		if err := rows.Scan(&nh.ID, &nh.EntitatTipus, &nh.EntitatID, &nh.Nom, &nh.AnyInici, &nh.AnyFi, &nh.PaisRegne, &nh.DistribucioGeografica, &nh.Font); err != nil {
			return nil, err
		}
		res = append(res, nh)
	}
	return res, nil
}

func (h sqlHelper) saveNomHistoric(nh *NomHistoric) (int, error) {
	if nh.ID == 0 {
		query := `
            INSERT INTO noms_historics (entitat_tipus, entitat_id, nom, any_inici, any_fi, pais_regne, distribucio_geografica, font)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
		query = formatPlaceholders(h.style, query)
		if h.style == "postgres" {
			query += " RETURNING id"
			if err := h.db.QueryRow(query, nh.EntitatTipus, nh.EntitatID, nh.Nom, nh.AnyInici, nh.AnyFi, nh.PaisRegne, nh.DistribucioGeografica, nh.Font).Scan(&nh.ID); err != nil {
				return 0, err
			}
			return nh.ID, nil
		}
		res, err := h.db.Exec(query, nh.EntitatTipus, nh.EntitatID, nh.Nom, nh.AnyInici, nh.AnyFi, nh.PaisRegne, nh.DistribucioGeografica, nh.Font)
		if err != nil {
			return 0, err
		}
		if id, err := res.LastInsertId(); err == nil {
			nh.ID = int(id)
		}
		return nh.ID, nil
	}
	query := `
        UPDATE noms_historics
        SET nom=?, any_inici=?, any_fi=?, pais_regne=?, distribucio_geografica=?, font=?
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query, nh.Nom, nh.AnyInici, nh.AnyFi, nh.PaisRegne, nh.DistribucioGeografica, nh.Font, nh.ID)
	return nh.ID, err
}

// Entitats eclesiàstiques
func (h sqlHelper) listArquebisbats(f ArquebisbatFilter) ([]ArquebisbatRow, error) {
	where := "1=1"
	args := []interface{}{}
	inClause := func(column string, ids []int) {
		if len(ids) == 0 {
			return
		}
		placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
		where += " AND " + column + " IN (" + placeholders + ")"
		for _, id := range ids {
			args = append(args, id)
		}
	}
	if strings.TrimSpace(f.Text) != "" {
		where += " AND lower(a.nom) LIKE ?"
		args = append(args, "%"+strings.ToLower(strings.TrimSpace(f.Text))+"%")
	}
	if f.PaisID > 0 {
		where += " AND a.pais_id = ?"
		args = append(args, f.PaisID)
	}
	if strings.TrimSpace(f.Status) != "" {
		where += " AND a.moderation_status = ?"
		args = append(args, strings.TrimSpace(f.Status))
	}
	inClause("a.id", f.AllowedEclesIDs)
	inClause("a.pais_id", f.AllowedPaisIDs)
	query := `
        SELECT a.id, a.nom, a.tipus_entitat, a.pais_id, p.codi_iso3, a.nivell, parent.nom as parent_nom, a.any_inici, a.any_fi,
               a.moderation_status
        FROM arquebisbats a
        LEFT JOIN paisos p ON p.id = a.pais_id
        LEFT JOIN arquebisbats parent ON parent.id = a.parent_id
        WHERE ` + where + `
        ORDER BY a.nom`
	if f.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, f.Limit)
		if f.Offset > 0 {
			query += " OFFSET ?"
			args = append(args, f.Offset)
		}
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []ArquebisbatRow
	for rows.Next() {
		var r ArquebisbatRow
		if err := rows.Scan(&r.ID, &r.Nom, &r.TipusEntitat, &r.PaisID, &r.PaisNom, &r.Nivell, &r.ParentNom, &r.AnyInici, &r.AnyFi, &r.ModeracioEstat); err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}

func (h sqlHelper) countArquebisbats(f ArquebisbatFilter) (int, error) {
	where := "1=1"
	args := []interface{}{}
	inClause := func(column string, ids []int) {
		if len(ids) == 0 {
			return
		}
		placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
		where += " AND " + column + " IN (" + placeholders + ")"
		for _, id := range ids {
			args = append(args, id)
		}
	}
	if strings.TrimSpace(f.Text) != "" {
		where += " AND lower(a.nom) LIKE ?"
		args = append(args, "%"+strings.ToLower(strings.TrimSpace(f.Text))+"%")
	}
	if f.PaisID > 0 {
		where += " AND a.pais_id = ?"
		args = append(args, f.PaisID)
	}
	if strings.TrimSpace(f.Status) != "" {
		where += " AND a.moderation_status = ?"
		args = append(args, strings.TrimSpace(f.Status))
	}
	inClause("a.id", f.AllowedEclesIDs)
	inClause("a.pais_id", f.AllowedPaisIDs)
	query := `SELECT COUNT(*) FROM arquebisbats a WHERE ` + where
	query = formatPlaceholders(h.style, query)
	var total int
	if err := h.db.QueryRow(query, args...).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (h sqlHelper) getArquebisbat(id int) (*Arquebisbat, error) {
	query := `
        SELECT id, nom, tipus_entitat, pais_id, nivell, parent_id, any_inici, any_fi,
               web, web_arxiu, web_wikipedia, territori, observacions,
               created_by, moderation_status, moderated_by, moderated_at, moderation_notes
        FROM arquebisbats WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, id)
	var a Arquebisbat
	if err := row.Scan(&a.ID, &a.Nom, &a.TipusEntitat, &a.PaisID, &a.Nivell, &a.ParentID, &a.AnyInici, &a.AnyFi, &a.Web, &a.WebArxiu, &a.WebWikipedia, &a.Territori, &a.Observacions,
		&a.CreatedBy, &a.ModeracioEstat, &a.ModeratedBy, &a.ModeratedAt, &a.ModeracioMotiu); err != nil {
		return nil, err
	}
	return &a, nil
}

func (h sqlHelper) createArquebisbat(ae *Arquebisbat) (int, error) {
	query := `
        INSERT INTO arquebisbats
            (nom, tipus_entitat, pais_id, nivell, parent_id, any_inici, any_fi, web, web_arxiu, web_wikipedia, territori, observacions,
             created_by, moderation_status, moderated_by, moderated_at, moderation_notes, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		query += ` RETURNING id`
	}
	query = formatPlaceholders(h.style, query)
	args := []interface{}{ae.Nom, ae.TipusEntitat, ae.PaisID, ae.Nivell, ae.ParentID, ae.AnyInici, ae.AnyFi, ae.Web, ae.WebArxiu, ae.WebWikipedia, ae.Territori, ae.Observacions,
		ae.CreatedBy, ae.ModeracioEstat, ae.ModeratedBy, ae.ModeratedAt, ae.ModeracioMotiu}
	if h.style == "postgres" {
		if err := h.db.QueryRow(query, args...).Scan(&ae.ID); err != nil {
			return 0, err
		}
		return ae.ID, nil
	}
	res, err := h.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		ae.ID = int(id)
	}
	return ae.ID, nil
}

func (h sqlHelper) updateArquebisbat(ae *Arquebisbat) error {
	query := `
        UPDATE arquebisbats
        SET nom=?, tipus_entitat=?, pais_id=?, nivell=?, parent_id=?, any_inici=?, any_fi=?, web=?, web_arxiu=?, web_wikipedia=?, territori=?, observacions=?,
            moderation_status=?, moderated_by=?, moderated_at=?, moderation_notes=?, updated_at=` + h.nowFun + `
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query, ae.Nom, ae.TipusEntitat, ae.PaisID, ae.Nivell, ae.ParentID, ae.AnyInici, ae.AnyFi, ae.Web, ae.WebArxiu, ae.WebWikipedia, ae.Territori, ae.Observacions,
		ae.ModeracioEstat, ae.ModeratedBy, ae.ModeratedAt, ae.ModeracioMotiu, ae.ID)
	return err
}

func (h sqlHelper) listArquebisbatMunicipis(munID int) ([]ArquebisbatMunicipi, error) {
	query := `
        SELECT am.id, am.id_municipi, am.id_arquevisbat, am.any_inici, am.any_fi, am.motiu, am.font, a.nom
        FROM arquebisbats_municipi am
        INNER JOIN arquebisbats a ON a.id = am.id_arquevisbat
        WHERE am.id_municipi = ? ORDER BY am.any_inici`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, munID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []ArquebisbatMunicipi
	for rows.Next() {
		var am ArquebisbatMunicipi
		if err := rows.Scan(&am.ID, &am.MunicipiID, &am.ArquebisbatID, &am.AnyInici, &am.AnyFi, &am.Motiu, &am.Font, &am.NomEntitat); err != nil {
			return nil, err
		}
		res = append(res, am)
	}
	return res, nil
}

func (h sqlHelper) saveArquebisbatMunicipi(am *ArquebisbatMunicipi) (int, error) {
	if am.ID == 0 {
		query := `
            INSERT INTO arquebisbats_municipi (id_municipi, id_arquevisbat, any_inici, any_fi, motiu, font)
            VALUES (?, ?, ?, ?, ?, ?)`
		query = formatPlaceholders(h.style, query)
		if h.style == "postgres" {
			query += ` RETURNING id`
			if err := h.db.QueryRow(query, am.MunicipiID, am.ArquebisbatID, am.AnyInici, am.AnyFi, am.Motiu, am.Font).Scan(&am.ID); err != nil {
				return 0, err
			}
			return am.ID, nil
		}
		res, err := h.db.Exec(query, am.MunicipiID, am.ArquebisbatID, am.AnyInici, am.AnyFi, am.Motiu, am.Font)
		if err != nil {
			return 0, err
		}
		if id, err := res.LastInsertId(); err == nil {
			am.ID = int(id)
		}
		return am.ID, nil
	}
	query := `
        UPDATE arquebisbats_municipi
        SET id_arquevisbat=?, any_inici=?, any_fi=?, motiu=?, font=?
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query, am.ArquebisbatID, am.AnyInici, am.AnyFi, am.Motiu, am.Font, am.ID)
	return am.ID, err
}

func (h sqlHelper) ensurePermissionsSchema() {
	h.ensureUserExtraColumns()
	h.ensurePolicyGrantsTable()
	h.ensureMediaModerationColumns()
	h.ensureMediaCreditsTables()
}

func (h sqlHelper) ensurePolicyGrantsTable() {
	var stmt string
	switch h.style {
	case "mysql":
		stmt = `CREATE TABLE IF NOT EXISTS politica_grants (
            id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            politica_id INT UNSIGNED NOT NULL,
            perm_key VARCHAR(255) NOT NULL,
            scope_type VARCHAR(50) NOT NULL,
            scope_id INT NULL,
            include_children BOOLEAN NOT NULL DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            FOREIGN KEY (politica_id) REFERENCES politiques(id) ON DELETE CASCADE
        )`
	case "postgres":
		stmt = `CREATE TABLE IF NOT EXISTS politica_grants (
            id SERIAL PRIMARY KEY,
            politica_id INTEGER NOT NULL REFERENCES politiques(id) ON DELETE CASCADE,
            perm_key TEXT NOT NULL,
            scope_type TEXT NOT NULL,
            scope_id INTEGER,
            include_children BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`
	default: // sqlite
		stmt = `CREATE TABLE IF NOT EXISTS politica_grants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            politica_id INTEGER NOT NULL REFERENCES politiques(id) ON DELETE CASCADE,
            perm_key TEXT NOT NULL,
            scope_type TEXT NOT NULL,
            scope_id INTEGER,
            include_children INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`
	}
	if stmt != "" {
		_, _ = h.db.Exec(stmt)
	}
	indexStmts := []string{}
	switch h.style {
	case "mysql":
		indexStmts = []string{
			"CREATE INDEX idx_politica_grants_politica ON politica_grants(politica_id)",
			"CREATE INDEX idx_politica_grants_perm ON politica_grants(perm_key)",
			"CREATE INDEX idx_politica_grants_perm_scope ON politica_grants(perm_key, scope_type, scope_id)",
		}
	case "postgres":
		indexStmts = []string{
			"CREATE INDEX IF NOT EXISTS idx_politica_grants_politica ON politica_grants(politica_id)",
			"CREATE INDEX IF NOT EXISTS idx_politica_grants_perm ON politica_grants(perm_key)",
			"CREATE INDEX IF NOT EXISTS idx_politica_grants_perm_scope ON politica_grants(perm_key, scope_type, scope_id)",
		}
	default: // sqlite
		indexStmts = []string{
			"CREATE INDEX IF NOT EXISTS idx_politica_grants_politica ON politica_grants(politica_id)",
			"CREATE INDEX IF NOT EXISTS idx_politica_grants_perm ON politica_grants(perm_key)",
			"CREATE INDEX IF NOT EXISTS idx_politica_grants_perm_scope ON politica_grants(perm_key, scope_type, scope_id)",
		}
	}
	for _, idx := range indexStmts {
		_, _ = h.db.Exec(idx)
	}
}

func (h sqlHelper) ensureMediaModerationColumns() {
	if !h.tableExists("media_items") {
		return
	}
	switch h.style {
	case "mysql":
		if !h.columnExists("media_items", "moderation_status") {
			_, _ = h.db.Exec("ALTER TABLE media_items ADD COLUMN moderation_status VARCHAR(20) NOT NULL DEFAULT 'pending'")
		}
		if !h.columnExists("media_items", "moderated_by") {
			_, _ = h.db.Exec("ALTER TABLE media_items ADD COLUMN moderated_by INT UNSIGNED NULL")
		}
		if !h.columnExists("media_items", "moderated_at") {
			_, _ = h.db.Exec("ALTER TABLE media_items ADD COLUMN moderated_at DATETIME")
		}
		if !h.columnExists("media_items", "moderation_notes") {
			_, _ = h.db.Exec("ALTER TABLE media_items ADD COLUMN moderation_notes TEXT")
		}
	case "postgres":
		if !h.columnExists("media_items", "moderation_status") {
			_, _ = h.db.Exec("ALTER TABLE media_items ADD COLUMN moderation_status TEXT NOT NULL DEFAULT 'pending'")
		}
		if !h.columnExists("media_items", "moderated_by") {
			_, _ = h.db.Exec("ALTER TABLE media_items ADD COLUMN moderated_by INTEGER")
		}
		if !h.columnExists("media_items", "moderated_at") {
			_, _ = h.db.Exec("ALTER TABLE media_items ADD COLUMN moderated_at TIMESTAMP")
		}
		if !h.columnExists("media_items", "moderation_notes") {
			_, _ = h.db.Exec("ALTER TABLE media_items ADD COLUMN moderation_notes TEXT")
		}
	default: // sqlite
		if !h.columnExists("media_items", "moderation_status") {
			_, _ = h.db.Exec("ALTER TABLE media_items ADD COLUMN moderation_status TEXT NOT NULL DEFAULT 'pending'")
		}
		if !h.columnExists("media_items", "moderated_by") {
			_, _ = h.db.Exec("ALTER TABLE media_items ADD COLUMN moderated_by INTEGER")
		}
		if !h.columnExists("media_items", "moderated_at") {
			_, _ = h.db.Exec("ALTER TABLE media_items ADD COLUMN moderated_at TIMESTAMP")
		}
		if !h.columnExists("media_items", "moderation_notes") {
			_, _ = h.db.Exec("ALTER TABLE media_items ADD COLUMN moderation_notes TEXT")
		}
	}

	indexStmts := []string{}
	switch h.style {
	case "mysql":
		indexStmts = []string{
			"CREATE INDEX idx_media_items_moderation ON media_items(moderation_status)",
			"CREATE INDEX idx_media_albums_moderation ON media_albums(moderation_status)",
		}
	case "postgres":
		indexStmts = []string{
			"CREATE INDEX IF NOT EXISTS idx_media_items_moderation ON media_items(moderation_status)",
			"CREATE INDEX IF NOT EXISTS idx_media_albums_moderation ON media_albums(moderation_status)",
		}
	default: // sqlite
		indexStmts = []string{
			"CREATE INDEX IF NOT EXISTS idx_media_items_moderation ON media_items(moderation_status)",
			"CREATE INDEX IF NOT EXISTS idx_media_albums_moderation ON media_albums(moderation_status)",
		}
	}
	for _, stmt := range indexStmts {
		_, _ = h.db.Exec(stmt)
	}
}

func (h sqlHelper) ensureMediaCreditsTables() {
	var ledgerStmt string
	var grantsStmt string
	var logsStmt string
	switch h.style {
	case "mysql":
		ledgerStmt = `CREATE TABLE IF NOT EXISTS user_credits_ledger (
            id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            user_id INT UNSIGNED NOT NULL,
            delta INT NOT NULL,
            reason VARCHAR(100) NOT NULL,
            ref_type VARCHAR(50) NULL,
            ref_id INT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES usuaris(id) ON DELETE CASCADE
        )`
		grantsStmt = `CREATE TABLE IF NOT EXISTS media_access_grants (
            id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            user_id INT UNSIGNED NOT NULL,
            media_item_id INT UNSIGNED NOT NULL,
            grant_token VARCHAR(255) NOT NULL UNIQUE,
            expires_at DATETIME NOT NULL,
            credits_spent INT NOT NULL DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES usuaris(id) ON DELETE CASCADE,
            FOREIGN KEY (media_item_id) REFERENCES media_items(id) ON DELETE CASCADE
        )`
		logsStmt = `CREATE TABLE IF NOT EXISTS media_access_logs (
            id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            user_id INT UNSIGNED NOT NULL,
            media_item_id INT UNSIGNED NOT NULL,
            access_type VARCHAR(20) NOT NULL,
            credits_spent INT NOT NULL DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES usuaris(id) ON DELETE CASCADE,
            FOREIGN KEY (media_item_id) REFERENCES media_items(id) ON DELETE CASCADE
        )`
	case "postgres":
		ledgerStmt = `CREATE TABLE IF NOT EXISTS user_credits_ledger (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
            delta INTEGER NOT NULL,
            reason TEXT NOT NULL,
            ref_type TEXT,
            ref_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`
		grantsStmt = `CREATE TABLE IF NOT EXISTS media_access_grants (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
            media_item_id INTEGER NOT NULL REFERENCES media_items(id) ON DELETE CASCADE,
            grant_token TEXT NOT NULL UNIQUE,
            expires_at TIMESTAMP NOT NULL,
            credits_spent INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`
		logsStmt = `CREATE TABLE IF NOT EXISTS media_access_logs (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
            media_item_id INTEGER NOT NULL REFERENCES media_items(id) ON DELETE CASCADE,
            access_type TEXT NOT NULL,
            credits_spent INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`
	default: // sqlite
		ledgerStmt = `CREATE TABLE IF NOT EXISTS user_credits_ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
            delta INTEGER NOT NULL,
            reason TEXT NOT NULL,
            ref_type TEXT,
            ref_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`
		grantsStmt = `CREATE TABLE IF NOT EXISTS media_access_grants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
            media_item_id INTEGER NOT NULL REFERENCES media_items(id) ON DELETE CASCADE,
            grant_token TEXT NOT NULL UNIQUE,
            expires_at TIMESTAMP NOT NULL,
            credits_spent INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`
		logsStmt = `CREATE TABLE IF NOT EXISTS media_access_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES usuaris(id) ON DELETE CASCADE,
            media_item_id INTEGER NOT NULL REFERENCES media_items(id) ON DELETE CASCADE,
            access_type TEXT NOT NULL,
            credits_spent INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`
	}
	if ledgerStmt != "" {
		_, _ = h.db.Exec(ledgerStmt)
	}
	if grantsStmt != "" {
		_, _ = h.db.Exec(grantsStmt)
	}
	if logsStmt != "" {
		_, _ = h.db.Exec(logsStmt)
	}

	indexStmts := []string{}
	switch h.style {
	case "mysql":
		indexStmts = []string{
			"CREATE INDEX idx_user_credits_ledger_user ON user_credits_ledger(user_id)",
			"CREATE INDEX idx_user_credits_ledger_ref ON user_credits_ledger(ref_type, ref_id)",
			"CREATE INDEX idx_media_access_grants_lookup ON media_access_grants(user_id, media_item_id, expires_at)",
			"CREATE INDEX idx_media_access_logs_user ON media_access_logs(user_id)",
			"CREATE INDEX idx_media_access_logs_item ON media_access_logs(media_item_id)",
		}
	case "postgres":
		indexStmts = []string{
			"CREATE INDEX IF NOT EXISTS idx_user_credits_ledger_user ON user_credits_ledger(user_id)",
			"CREATE INDEX IF NOT EXISTS idx_user_credits_ledger_ref ON user_credits_ledger(ref_type, ref_id)",
			"CREATE INDEX IF NOT EXISTS idx_media_access_grants_lookup ON media_access_grants(user_id, media_item_id, expires_at)",
			"CREATE INDEX IF NOT EXISTS idx_media_access_logs_user ON media_access_logs(user_id)",
			"CREATE INDEX IF NOT EXISTS idx_media_access_logs_item ON media_access_logs(media_item_id)",
		}
	default: // sqlite
		indexStmts = []string{
			"CREATE INDEX IF NOT EXISTS idx_user_credits_ledger_user ON user_credits_ledger(user_id)",
			"CREATE INDEX IF NOT EXISTS idx_user_credits_ledger_ref ON user_credits_ledger(ref_type, ref_id)",
			"CREATE INDEX IF NOT EXISTS idx_media_access_grants_lookup ON media_access_grants(user_id, media_item_id, expires_at)",
			"CREATE INDEX IF NOT EXISTS idx_media_access_logs_user ON media_access_logs(user_id)",
			"CREATE INDEX IF NOT EXISTS idx_media_access_logs_item ON media_access_logs(media_item_id)",
		}
	}
	for _, stmt := range indexStmts {
		_, _ = h.db.Exec(stmt)
	}
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
		if !h.columnExists("usuaris", "banned") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN banned BOOLEAN DEFAULT 0")
		}
		if !h.columnExists("usuaris", "permissions_version") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN permissions_version INT NOT NULL DEFAULT 0")
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
		if !h.columnExists("usuaris", "banned") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN IF NOT EXISTS banned BOOLEAN DEFAULT FALSE")
		}
		if !h.columnExists("usuaris", "permissions_version") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN IF NOT EXISTS permissions_version INTEGER NOT NULL DEFAULT 0")
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
		if !h.columnExists("usuaris", "banned") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN banned INTEGER NOT NULL DEFAULT 0")
		}
		if !h.columnExists("usuaris", "permissions_version") {
			stmts = append(stmts, "ALTER TABLE usuaris ADD COLUMN permissions_version INTEGER NOT NULL DEFAULT 0")
		}
	}
	for _, stmt := range stmts {
		_, _ = h.db.Exec(stmt)
	}
}

func (h sqlHelper) ensureArxiuExtraColumns() {
	stmts := []string{}
	switch h.style {
	case "mysql":
		if !h.columnExists("arxius", "what3words") {
			stmts = append(stmts, "ALTER TABLE arxius ADD COLUMN what3words VARCHAR(255)")
		}
	case "postgres":
		if !h.columnExists("arxius", "what3words") {
			stmts = append(stmts, "ALTER TABLE arxius ADD COLUMN IF NOT EXISTS what3words TEXT")
		}
	default: // sqlite
		if !h.columnExists("arxius", "what3words") {
			stmts = append(stmts, "ALTER TABLE arxius ADD COLUMN what3words TEXT")
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

func (h sqlHelper) getUserByID(id int) (*User, error) {
	h.ensureUserExtraColumns()
	query := formatPlaceholders(h.style, `
        SELECT id, usuari, nom, cognoms, correu, contrasenya, data_naixement, pais, estat, provincia, poblacio, codi_postal, address, employment_status, profession, phone, preferred_lang, spoken_langs, data_creacio, actiu 
        FROM usuaris 
        WHERE id = ?`)
	row := h.db.QueryRow(query, id)
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
        SET actiu = %s, token_activacio = NULL, expira_token = NULL 
        WHERE token_activacio = ? AND (expira_token IS NULL OR expira_token > %s)
    `)
	nowExpr := "datetime('now')"
	actiuExpr := "1"
	if h.style == "mysql" {
		nowExpr = "NOW()"
	} else if h.style == "postgres" {
		nowExpr = "NOW()"
		actiuExpr = "TRUE"
	}
	stmt = fmt.Sprintf(stmt, actiuExpr, nowExpr)
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
        WHERE (usuari = ? OR correu = ?) AND actiu = 1 AND (banned = 0 OR banned IS NULL)`)

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
        WHERE s.token_hash = ? AND s.revocat = 0 AND u.actiu = 1 AND (u.banned = 0 OR u.banned IS NULL)`)

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

func (h sqlHelper) listUsersAdmin() ([]UserAdminRow, error) {
	h.ensureUserExtraColumns()
	query := `
        SELECT u.id, u.usuari, u.nom, u.cognoms, u.correu, u.data_creacio,
               CASE WHEN u.actiu THEN 1 ELSE 0 END AS actiu_val,
               CASE WHEN u.banned THEN 1 ELSE 0 END AS banned_val,
               MAX(COALESCE(sal.ts, s.creat)) AS last_login
        FROM usuaris u
        LEFT JOIN sessions s ON s.usuari_id = u.id
        LEFT JOIN session_access_log sal ON sal.session_id = s.id
        GROUP BY u.id, u.usuari, u.nom, u.cognoms, u.correu, u.data_creacio, u.actiu, u.banned
        ORDER BY u.data_creacio DESC, u.id DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []UserAdminRow
	for rows.Next() {
		var row UserAdminRow
		var createdRaw interface{}
		var lastRaw interface{}
		var actiuVal int
		var bannedVal int
		if err := rows.Scan(&row.ID, &row.Usuari, &row.Nom, &row.Cognoms, &row.Email, &createdRaw, &actiuVal, &bannedVal, &lastRaw); err != nil {
			return nil, err
		}
		row.CreatedAt = dbTimeString(createdRaw)
		row.LastLogin = dbTimeString(lastRaw)
		row.Active = actiuVal == 1
		row.Banned = bannedVal == 1
		res = append(res, row)
	}
	return res, nil
}

func (h sqlHelper) listUsersAdminFiltered(filter UserAdminFilter) ([]UserAdminRow, error) {
	h.ensureUserExtraColumns()
	clauses := []string{"1=1"}
	args := []interface{}{}
	if filter.UserID > 0 {
		clauses = append(clauses, "u.id = ?")
		args = append(args, filter.UserID)
	}
	if q := strings.TrimSpace(filter.Query); q != "" {
		like := "%" + strings.ToLower(q) + "%"
		idExpr := "CAST(u.id AS TEXT)"
		if h.style == "mysql" {
			idExpr = "CAST(u.id AS CHAR)"
		}
		clauses = append(clauses, "(LOWER(COALESCE(u.usuari,'')) LIKE ? OR LOWER(COALESCE(u.nom,'')) LIKE ? OR LOWER(COALESCE(u.cognoms,'')) LIKE ? OR LOWER(COALESCE(u.correu,'')) LIKE ? OR "+idExpr+" LIKE ?)")
		args = append(args, like, like, like, like, like)
	}
	if filter.Active != nil {
		clauses = append(clauses, "u.actiu = ?")
		if h.style == "postgres" {
			args = append(args, *filter.Active)
		} else {
			if *filter.Active {
				args = append(args, 1)
			} else {
				args = append(args, 0)
			}
		}
	}
	if filter.Banned != nil {
		clauses = append(clauses, "u.banned = ?")
		if h.style == "postgres" {
			args = append(args, *filter.Banned)
		} else {
			if *filter.Banned {
				args = append(args, 1)
			} else {
				args = append(args, 0)
			}
		}
	}
	limit := filter.Limit
	offset := filter.Offset
	if limit <= 0 {
		limit = 25
	}
	if offset < 0 {
		offset = 0
	}
	args = append(args, limit, offset)
	query := `
        SELECT u.id, u.usuari, u.nom, u.cognoms, u.correu, u.data_creacio,
               CASE WHEN u.actiu THEN 1 ELSE 0 END AS actiu_val,
               CASE WHEN u.banned THEN 1 ELSE 0 END AS banned_val,
               last.last_login
        FROM usuaris u
        LEFT JOIN (
            SELECT s.usuari_id AS uid, MAX(COALESCE(sal.ts, s.creat)) AS last_login
            FROM sessions s
            LEFT JOIN session_access_log sal ON sal.session_id = s.id
            GROUP BY s.usuari_id
        ) last ON last.uid = u.id
        WHERE ` + strings.Join(clauses, " AND ") + `
        ORDER BY u.data_creacio DESC, u.id DESC
        LIMIT ? OFFSET ?`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []UserAdminRow
	for rows.Next() {
		var row UserAdminRow
		var createdRaw interface{}
		var lastRaw interface{}
		var actiuVal int
		var bannedVal int
		if err := rows.Scan(&row.ID, &row.Usuari, &row.Nom, &row.Cognoms, &row.Email, &createdRaw, &actiuVal, &bannedVal, &lastRaw); err != nil {
			return nil, err
		}
		row.CreatedAt = dbTimeString(createdRaw)
		row.LastLogin = dbTimeString(lastRaw)
		row.Active = actiuVal == 1
		row.Banned = bannedVal == 1
		res = append(res, row)
	}
	return res, nil
}

func (h sqlHelper) countUsersAdmin(filter UserAdminFilter) (int, error) {
	h.ensureUserExtraColumns()
	clauses := []string{"1=1"}
	args := []interface{}{}
	if filter.UserID > 0 {
		clauses = append(clauses, "u.id = ?")
		args = append(args, filter.UserID)
	}
	if q := strings.TrimSpace(filter.Query); q != "" {
		like := "%" + strings.ToLower(q) + "%"
		idExpr := "CAST(u.id AS TEXT)"
		if h.style == "mysql" {
			idExpr = "CAST(u.id AS CHAR)"
		}
		clauses = append(clauses, "(LOWER(COALESCE(u.usuari,'')) LIKE ? OR LOWER(COALESCE(u.nom,'')) LIKE ? OR LOWER(COALESCE(u.cognoms,'')) LIKE ? OR LOWER(COALESCE(u.correu,'')) LIKE ? OR "+idExpr+" LIKE ?)")
		args = append(args, like, like, like, like, like)
	}
	if filter.Active != nil {
		clauses = append(clauses, "u.actiu = ?")
		if h.style == "postgres" {
			args = append(args, *filter.Active)
		} else {
			if *filter.Active {
				args = append(args, 1)
			} else {
				args = append(args, 0)
			}
		}
	}
	if filter.Banned != nil {
		clauses = append(clauses, "u.banned = ?")
		if h.style == "postgres" {
			args = append(args, *filter.Banned)
		} else {
			if *filter.Banned {
				args = append(args, 1)
			} else {
				args = append(args, 0)
			}
		}
	}
	query := `SELECT COUNT(*) FROM usuaris u WHERE ` + strings.Join(clauses, " AND ")
	query = formatPlaceholders(h.style, query)
	var total int
	if err := h.db.QueryRow(query, args...).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (h sqlHelper) setUserActive(userID int, active bool) error {
	stmt := formatPlaceholders(h.style, `UPDATE usuaris SET actiu = ? WHERE id = ?`)
	if h.style == "postgres" {
		_, err := h.db.Exec(stmt, active, userID)
		return err
	}
	val := 0
	if active {
		val = 1
	}
	_, err := h.db.Exec(stmt, val, userID)
	return err
}

func (h sqlHelper) setUserBanned(userID int, banned bool) error {
	stmt := formatPlaceholders(h.style, `UPDATE usuaris SET banned = ? WHERE id = ?`)
	if h.style == "postgres" {
		_, err := h.db.Exec(stmt, banned, userID)
		return err
	}
	val := 0
	if banned {
		val = 1
	}
	_, err := h.db.Exec(stmt, val, userID)
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

// Arxius
func (h sqlHelper) listArxius(filter ArxiuFilter) ([]ArxiuWithCount, error) {
	h.ensureUserExtraColumns() // no-op but keeps migrations consistent
	h.ensureArxiuExtraColumns()
	args := []interface{}{}
	clauses := []string{"1=1"}
	if filter.Text != "" {
		clauses = append(clauses, "a.nom LIKE ?")
		args = append(args, "%"+filter.Text+"%")
	}
	if filter.Tipus != "" {
		clauses = append(clauses, "a.tipus = ?")
		args = append(args, filter.Tipus)
	}
	if filter.Acces != "" {
		clauses = append(clauses, "a.acces = ?")
		args = append(args, filter.Acces)
	}
	if filter.EntitatID > 0 {
		clauses = append(clauses, "a.entitat_eclesiastica_id = ?")
		args = append(args, filter.EntitatID)
	}
	if filter.MunicipiID > 0 {
		clauses = append(clauses, "a.municipi_id = ?")
		args = append(args, filter.MunicipiID)
	}
	if strings.TrimSpace(filter.Status) != "" {
		clauses = append(clauses, "a.moderation_status = ?")
		args = append(args, strings.TrimSpace(filter.Status))
	}
	allowedClauses := []string{}
	allowedArgs := []interface{}{}
	inClause := func(column string, ids []int) {
		if len(ids) == 0 {
			return
		}
		placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
		allowedClauses = append(allowedClauses, column+" IN ("+placeholders+")")
		for _, id := range ids {
			allowedArgs = append(allowedArgs, id)
		}
	}
	inClause("a.id", filter.AllowedArxiuIDs)
	inClause("a.municipi_id", filter.AllowedMunicipiIDs)
	inClause("a.entitat_eclesiastica_id", filter.AllowedEclesIDs)
	inClause("m.nivell_administratiu_id_3", filter.AllowedProvinciaIDs)
	inClause("m.nivell_administratiu_id_4", filter.AllowedComarcaIDs)
	inClause("na1.pais_id", filter.AllowedPaisIDs)
	if len(allowedClauses) > 0 {
		clauses = append(clauses, "("+strings.Join(allowedClauses, " OR ")+")")
		args = append(args, allowedArgs...)
	}
	limit := 50
	offset := 0
	applyLimit := true
	if filter.Limit < 0 {
		applyLimit = false
	} else if filter.Limit > 0 {
		limit = filter.Limit
	}
	if filter.Offset > 0 {
		offset = filter.Offset
	}
	query := `
        SELECT a.id, a.nom, a.tipus, a.municipi_id, a.entitat_eclesiastica_id, a.adreca, a.ubicacio, COALESCE(a.what3words, ''), a.web, a.acces, a.notes, a.accepta_donacions, COALESCE(a.donacions_url, ''),
               a.created_by, a.moderation_status, a.moderated_by, a.moderated_at, a.moderation_notes,
               m.nom as municipi_nom, ae.nom as entitat_nom,
               COALESCE(cnt.total, 0) AS llibres
        FROM arxius a
        LEFT JOIN municipis m ON m.id = a.municipi_id
        LEFT JOIN nivells_administratius na1 ON na1.id = m.nivell_administratiu_id_1
        LEFT JOIN arquebisbats ae ON ae.id = a.entitat_eclesiastica_id
        LEFT JOIN (
            SELECT arxiu_id, COUNT(*) as total FROM arxius_llibres GROUP BY arxiu_id
        ) cnt ON cnt.arxiu_id = a.id
        WHERE ` + strings.Join(clauses, " AND ") + `
        ORDER BY a.nom ASC`
	if applyLimit {
		query += " LIMIT ? OFFSET ?"
		args = append(args, limit, offset)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []ArxiuWithCount
	for rows.Next() {
		var a ArxiuWithCount
		if err := rows.Scan(&a.ID, &a.Nom, &a.Tipus, &a.MunicipiID, &a.EntitatEclesiasticaID, &a.Adreca, &a.Ubicacio, &a.What3Words, &a.Web, &a.Acces, &a.Notes, &a.AcceptaDonacions, &a.DonacionsURL,
			&a.CreatedBy, &a.ModeracioEstat, &a.ModeratedBy, &a.ModeratedAt, &a.ModeracioMotiu,
			&a.MunicipiNom, &a.EntitatNom, &a.Llibres); err != nil {
			return nil, err
		}
		res = append(res, a)
	}
	return res, nil
}

func (h sqlHelper) countArxius(filter ArxiuFilter) (int, error) {
	h.ensureUserExtraColumns()
	args := []interface{}{}
	clauses := []string{"1=1"}
	if filter.Text != "" {
		clauses = append(clauses, "a.nom LIKE ?")
		args = append(args, "%"+filter.Text+"%")
	}
	if filter.Tipus != "" {
		clauses = append(clauses, "a.tipus = ?")
		args = append(args, filter.Tipus)
	}
	if filter.Acces != "" {
		clauses = append(clauses, "a.acces = ?")
		args = append(args, filter.Acces)
	}
	if filter.EntitatID > 0 {
		clauses = append(clauses, "a.entitat_eclesiastica_id = ?")
		args = append(args, filter.EntitatID)
	}
	if filter.MunicipiID > 0 {
		clauses = append(clauses, "a.municipi_id = ?")
		args = append(args, filter.MunicipiID)
	}
	if strings.TrimSpace(filter.Status) != "" {
		clauses = append(clauses, "a.moderation_status = ?")
		args = append(args, strings.TrimSpace(filter.Status))
	}
	allowedClauses := []string{}
	allowedArgs := []interface{}{}
	inClause := func(column string, ids []int) {
		if len(ids) == 0 {
			return
		}
		placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
		allowedClauses = append(allowedClauses, column+" IN ("+placeholders+")")
		for _, id := range ids {
			allowedArgs = append(allowedArgs, id)
		}
	}
	inClause("a.id", filter.AllowedArxiuIDs)
	inClause("a.municipi_id", filter.AllowedMunicipiIDs)
	inClause("a.entitat_eclesiastica_id", filter.AllowedEclesIDs)
	inClause("m.nivell_administratiu_id_3", filter.AllowedProvinciaIDs)
	inClause("m.nivell_administratiu_id_4", filter.AllowedComarcaIDs)
	inClause("na1.pais_id", filter.AllowedPaisIDs)
	if len(allowedClauses) > 0 {
		clauses = append(clauses, "("+strings.Join(allowedClauses, " OR ")+")")
		args = append(args, allowedArgs...)
	}
	query := `
        SELECT COUNT(*)
        FROM arxius a
        LEFT JOIN municipis m ON m.id = a.municipi_id
        LEFT JOIN nivells_administratius na1 ON na1.id = m.nivell_administratiu_id_1
        WHERE ` + strings.Join(clauses, " AND ")
	query = formatPlaceholders(h.style, query)
	var total int
	if err := h.db.QueryRow(query, args...).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (h sqlHelper) getArxiu(id int) (*Arxiu, error) {
	h.ensureArxiuExtraColumns()
	query := formatPlaceholders(h.style, `
        SELECT id, nom, tipus, municipi_id, entitat_eclesiastica_id, adreca, ubicacio, COALESCE(what3words, ''), web, acces, notes, accepta_donacions, COALESCE(donacions_url, ''),
               created_by, moderation_status, moderated_by, moderated_at, moderation_notes
        FROM arxius WHERE id = ?`)
	row := h.db.QueryRow(query, id)
	var a Arxiu
	if err := row.Scan(&a.ID, &a.Nom, &a.Tipus, &a.MunicipiID, &a.EntitatEclesiasticaID, &a.Adreca, &a.Ubicacio, &a.What3Words, &a.Web, &a.Acces, &a.Notes, &a.AcceptaDonacions, &a.DonacionsURL,
		&a.CreatedBy, &a.ModeracioEstat, &a.ModeratedBy, &a.ModeratedAt, &a.ModeracioMotiu); err != nil {
		return nil, err
	}
	return &a, nil
}

func (h sqlHelper) createArxiu(a *Arxiu) (int, error) {
	h.ensureArxiuExtraColumns()
	args := []interface{}{a.Nom, a.Tipus, a.MunicipiID, a.EntitatEclesiasticaID, a.Adreca, a.Ubicacio, a.What3Words, a.Web, a.Acces, a.Notes, a.AcceptaDonacions, a.DonacionsURL, a.CreatedBy, a.ModeracioEstat, a.ModeratedBy, a.ModeratedAt, a.ModeracioMotiu}
	if h.style == "postgres" {
		query := `
            INSERT INTO arxius (nom, tipus, municipi_id, entitat_eclesiastica_id, adreca, ubicacio, what3words, web, acces, notes, accepta_donacions, donacions_url, created_by, moderation_status, moderated_by, moderated_at, moderation_notes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)
            RETURNING id`
		query = formatPlaceholders(h.style, query)
		if err := h.db.QueryRow(query, args...).Scan(&a.ID); err != nil {
			return 0, err
		}
		return a.ID, nil
	}

	query := `
        INSERT INTO arxius (nom, tipus, municipi_id, entitat_eclesiastica_id, adreca, ubicacio, what3words, web, acces, notes, accepta_donacions, donacions_url, created_by, moderation_status, moderated_by, moderated_at, moderation_notes, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	query = formatPlaceholders(h.style, query)
	res, err := h.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		a.ID = int(id)
	}
	return a.ID, nil
}

func (h sqlHelper) updateArxiu(a *Arxiu) error {
	h.ensureArxiuExtraColumns()
	query := `
        UPDATE arxius
        SET nom = ?, tipus = ?, municipi_id = ?, entitat_eclesiastica_id = ?, adreca = ?, ubicacio = ?, what3words = ?, web = ?, acces = ?, notes = ?, accepta_donacions = ?, donacions_url = ?,
            moderation_status = ?, moderated_by = ?, moderated_at = ?, moderation_notes = ?, updated_at = ` + h.nowFun + `
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query, a.Nom, a.Tipus, a.MunicipiID, a.EntitatEclesiasticaID, a.Adreca, a.Ubicacio, a.What3Words, a.Web, a.Acces, a.Notes, a.AcceptaDonacions, a.DonacionsURL, a.ModeracioEstat, a.ModeratedBy, a.ModeratedAt, a.ModeracioMotiu, a.ID)
	return err
}

func (h sqlHelper) deleteArxiu(id int) error {
	stmt := formatPlaceholders(h.style, `DELETE FROM arxius WHERE id = ?`)
	_, err := h.db.Exec(stmt, id)
	return err
}

func (h sqlHelper) insertArxiuDonacioClick(arxiuID int, userID *int) error {
	var uid interface{} = nil
	if userID != nil {
		uid = *userID
	}
	query := `
        INSERT INTO arxius_donacions_clicks (arxiu_id, user_id, created_at)
        VALUES (?, ?, ` + h.nowFun + `)`
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query, arxiuID, uid)
	return err
}

func (h sqlHelper) countArxiuDonacioClicks(arxiuID int) (int, error) {
	query := formatPlaceholders(h.style, `SELECT COUNT(*) FROM arxius_donacions_clicks WHERE arxiu_id = ?`)
	var total int
	if err := h.db.QueryRow(query, arxiuID).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (h sqlHelper) listArxiuLlibres(arxiuID int) ([]ArxiuLlibreDetail, error) {
	query := `
        SELECT al.arxiu_id, al.llibre_id, al.signatura, al.url_override,
               l.titol, l.tipus_llibre, l.nom_esglesia, l.cronologia, m.nom as municipi, a.nom as arxiu_nom,
               l.pagines
        FROM arxius_llibres al
        INNER JOIN llibres l ON l.id = al.llibre_id
        LEFT JOIN municipis m ON m.id = l.municipi_id
        LEFT JOIN arxius a ON a.id = al.arxiu_id
        WHERE al.arxiu_id = ?`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, arxiuID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []ArxiuLlibreDetail
	for rows.Next() {
		var d ArxiuLlibreDetail
		if err := rows.Scan(&d.ArxiuID, &d.LlibreID, &d.Signatura, &d.URLOverride, &d.Titol, &d.TipusLlibre, &d.NomEsglesia, &d.Cronologia, &d.Municipi, &d.ArxiuNom, &d.Pagines); err != nil {
			return nil, err
		}
		res = append(res, d)
	}
	return res, nil
}

func (h sqlHelper) listLlibreArxius(llibreID int) ([]ArxiuLlibreDetail, error) {
	query := `
        SELECT al.arxiu_id,
               al.llibre_id,
               MAX(al.signatura) as signatura,
               MAX(al.url_override) as url_override,
               a.nom as arxiu_nom,
               m.nom as municipi
        FROM arxius_llibres al
        INNER JOIN arxius a ON a.id = al.arxiu_id
        LEFT JOIN municipis m ON m.id = a.municipi_id
        WHERE al.llibre_id = ?
        GROUP BY al.arxiu_id, al.llibre_id, a.nom, m.nom
        ORDER BY a.nom`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, llibreID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []ArxiuLlibreDetail
	for rows.Next() {
		var d ArxiuLlibreDetail
		if err := rows.Scan(&d.ArxiuID, &d.LlibreID, &d.Signatura, &d.URLOverride, &d.ArxiuNom, &d.Municipi); err != nil {
			return nil, err
		}
		res = append(res, d)
	}
	return res, nil
}

func (h sqlHelper) addArxiuLlibre(arxiuID, llibreID int, signatura, urlOverride string) error {
	stmt := formatPlaceholders(h.style, `
        INSERT INTO arxius_llibres (arxiu_id, llibre_id, signatura, url_override)
        VALUES (?, ?, ?, ?)`)
	_, err := h.db.Exec(stmt, arxiuID, llibreID, signatura, urlOverride)
	return err
}

func (h sqlHelper) updateArxiuLlibre(arxiuID, llibreID int, signatura, urlOverride string) error {
	stmt := formatPlaceholders(h.style, `
        UPDATE arxius_llibres SET signatura = ?, url_override = ?
        WHERE arxiu_id = ? AND llibre_id = ?`)
	_, err := h.db.Exec(stmt, signatura, urlOverride, arxiuID, llibreID)
	return err
}

func (h sqlHelper) listLlibreURLs(llibreID int) ([]LlibreURL, error) {
	query := `
        SELECT lu.id, lu.llibre_id, lu.arxiu_id, lu.url, lu.tipus, lu.descripcio,
               lu.created_by, lu.created_at, a.nom as arxiu_nom
        FROM llibres_urls lu
        LEFT JOIN arxius a ON a.id = lu.arxiu_id
        WHERE lu.llibre_id = ?
        ORDER BY lu.id DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, llibreID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []LlibreURL
	for rows.Next() {
		var d LlibreURL
		if err := rows.Scan(&d.ID, &d.LlibreID, &d.ArxiuID, &d.URL, &d.Tipus, &d.Descripcio, &d.CreatedBy, &d.CreatedAt, &d.ArxiuNom); err != nil {
			return nil, err
		}
		res = append(res, d)
	}
	return res, nil
}

func (h sqlHelper) addLlibreURL(link *LlibreURL) error {
	stmt := formatPlaceholders(h.style, `
        INSERT INTO llibres_urls (llibre_id, arxiu_id, url, tipus, descripcio, created_by, created_at)
        VALUES (?, ?, ?, ?, ?, ?, `+h.nowFun+`)`)
	_, err := h.db.Exec(stmt, link.LlibreID, link.ArxiuID, link.URL, link.Tipus, link.Descripcio, link.CreatedBy)
	return err
}

func (h sqlHelper) deleteLlibreURL(id int) error {
	stmt := formatPlaceholders(h.style, `DELETE FROM llibres_urls WHERE id = ?`)
	_, err := h.db.Exec(stmt, id)
	return err
}

func (h sqlHelper) deleteArxiuLlibre(arxiuID, llibreID int) error {
	stmt := formatPlaceholders(h.style, `DELETE FROM arxius_llibres WHERE arxiu_id = ? AND llibre_id = ?`)
	_, err := h.db.Exec(stmt, arxiuID, llibreID)
	return err
}

func (h sqlHelper) searchLlibresSimple(q string, limit int) ([]LlibreSimple, error) {
	if limit <= 0 {
		limit = 20
	}
	args := []interface{}{}
	where := "1=1"
	if strings.TrimSpace(q) != "" {
		where = "(l.titol LIKE ? OR l.nom_esglesia LIKE ? OR l.cronologia LIKE ?)"
		like := "%" + q + "%"
		args = append(args, like, like, like)
	}
	args = append(args, limit)
	query := `
        SELECT l.id, l.titol, l.nom_esglesia, l.cronologia, m.nom as municipi
        FROM llibres l
        LEFT JOIN municipis m ON m.id = l.municipi_id
        WHERE ` + where + `
        ORDER BY l.titol ASC
        LIMIT ?`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []LlibreSimple
	for rows.Next() {
		var l LlibreSimple
		if err := rows.Scan(&l.ID, &l.Titol, &l.NomEsglesia, &l.Cronologia, &l.Municipi); err != nil {
			return nil, err
		}
		res = append(res, l)
	}
	return res, nil
}

func (h sqlHelper) listLlibres(filter LlibreFilter) ([]LlibreRow, error) {
	args := []interface{}{}
	clauses := []string{"1=1"}
	if strings.TrimSpace(filter.Text) != "" {
		like := "%" + strings.TrimSpace(filter.Text) + "%"
		clauses = append(clauses, "(l.titol LIKE ? OR l.nom_esglesia LIKE ?)")
		args = append(args, like, like)
	}
	if filter.ArquebisbatID > 0 {
		clauses = append(clauses, "l.arquevisbat_id = ?")
		args = append(args, filter.ArquebisbatID)
	}
	if filter.MunicipiID > 0 {
		clauses = append(clauses, "l.municipi_id = ?")
		args = append(args, filter.MunicipiID)
	}
	if filter.ArxiuID > 0 {
		clauses = append(clauses, "EXISTS (SELECT 1 FROM arxius_llibres al WHERE al.llibre_id = l.id AND al.arxiu_id = ?)")
		args = append(args, filter.ArxiuID)
	}
	if strings.TrimSpace(filter.ArxiuTipus) != "" {
		clauses = append(clauses, "EXISTS (SELECT 1 FROM arxius_llibres al INNER JOIN arxius ax ON ax.id = al.arxiu_id WHERE al.llibre_id = l.id AND ax.tipus = ?)")
		args = append(args, strings.TrimSpace(filter.ArxiuTipus))
	}
	if strings.TrimSpace(filter.TipusLlibre) != "" {
		clauses = append(clauses, "l.tipus_llibre = ?")
		args = append(args, strings.TrimSpace(filter.TipusLlibre))
	}
	if strings.TrimSpace(filter.Status) != "" {
		clauses = append(clauses, "l.moderation_status = ?")
		args = append(args, strings.TrimSpace(filter.Status))
	}
	allowedClauses := []string{}
	allowedArgs := []interface{}{}
	inClause := func(column string, ids []int) {
		if len(ids) == 0 {
			return
		}
		placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
		allowedClauses = append(allowedClauses, column+" IN ("+placeholders+")")
		for _, id := range ids {
			allowedArgs = append(allowedArgs, id)
		}
	}
	inClause("l.id", filter.AllowedLlibreIDs)
	inClause("l.municipi_id", filter.AllowedMunicipiIDs)
	inClause("l.arquevisbat_id", filter.AllowedEclesIDs)
	inClause("m.nivell_administratiu_id_3", filter.AllowedProvinciaIDs)
	inClause("m.nivell_administratiu_id_4", filter.AllowedComarcaIDs)
	inClause("na1.pais_id", filter.AllowedPaisIDs)
	if len(filter.AllowedArxiuIDs) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(filter.AllowedArxiuIDs)), ",")
		allowedClauses = append(allowedClauses, "EXISTS (SELECT 1 FROM arxius_llibres al WHERE al.llibre_id = l.id AND al.arxiu_id IN ("+placeholders+"))")
		for _, id := range filter.AllowedArxiuIDs {
			allowedArgs = append(allowedArgs, id)
		}
	}
	if len(allowedClauses) > 0 {
		clauses = append(clauses, "("+strings.Join(allowedClauses, " OR ")+")")
		args = append(args, allowedArgs...)
	}
	query := `
        SELECT l.id, l.arquevisbat_id, l.municipi_id, l.nom_esglesia, l.codi_digital, l.codi_fisic,
               l.titol, l.tipus_llibre, l.cronologia, l.volum, l.abat, l.contingut, l.llengua,
               l.requeriments_tecnics, l.unitat_catalogacio, l.unitat_instalacio, l.pagines,
               l.url_base, l.url_imatge_prefix, l.pagina, l.indexacio_completa,
               l.created_by, l.moderation_status, l.moderated_by, l.moderated_at, l.moderation_notes,
               ae.nom as arquebisbat_nom, m.nom as municipi_nom
        FROM llibres l
        LEFT JOIN arquebisbats ae ON ae.id = l.arquevisbat_id
        LEFT JOIN municipis m ON m.id = l.municipi_id
        LEFT JOIN nivells_administratius na1 ON na1.id = m.nivell_administratiu_id_1
        WHERE ` + strings.Join(clauses, " AND ") + `
        ORDER BY l.titol`
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
	var res []LlibreRow
	for rows.Next() {
		var lr LlibreRow
		if err := rows.Scan(
			&lr.ID, &lr.ArquebisbatID, &lr.MunicipiID, &lr.NomEsglesia, &lr.CodiDigital, &lr.CodiFisic,
			&lr.Titol, &lr.TipusLlibre, &lr.Cronologia, &lr.Volum, &lr.Abat, &lr.Contingut, &lr.Llengua,
			&lr.Requeriments, &lr.UnitatCatalogacio, &lr.UnitatInstalacio, &lr.Pagines,
			&lr.URLBase, &lr.URLImatgePrefix, &lr.Pagina, &lr.IndexacioCompleta,
			&lr.CreatedBy, &lr.ModeracioEstat, &lr.ModeratedBy, &lr.ModeratedAt, &lr.ModeracioMotiu,
			&lr.ArquebisbatNom, &lr.MunicipiNom,
		); err != nil {
			return nil, err
		}
		res = append(res, lr)
	}
	return res, nil
}

func (h sqlHelper) countLlibres(filter LlibreFilter) (int, error) {
	args := []interface{}{}
	clauses := []string{"1=1"}
	if strings.TrimSpace(filter.Text) != "" {
		like := "%" + strings.TrimSpace(filter.Text) + "%"
		clauses = append(clauses, "(l.titol LIKE ? OR l.nom_esglesia LIKE ?)")
		args = append(args, like, like)
	}
	if filter.ArquebisbatID > 0 {
		clauses = append(clauses, "l.arquevisbat_id = ?")
		args = append(args, filter.ArquebisbatID)
	}
	if filter.MunicipiID > 0 {
		clauses = append(clauses, "l.municipi_id = ?")
		args = append(args, filter.MunicipiID)
	}
	if filter.ArxiuID > 0 {
		clauses = append(clauses, "EXISTS (SELECT 1 FROM arxius_llibres al WHERE al.llibre_id = l.id AND al.arxiu_id = ?)")
		args = append(args, filter.ArxiuID)
	}
	if strings.TrimSpace(filter.ArxiuTipus) != "" {
		clauses = append(clauses, "EXISTS (SELECT 1 FROM arxius_llibres al INNER JOIN arxius ax ON ax.id = al.arxiu_id WHERE al.llibre_id = l.id AND ax.tipus = ?)")
		args = append(args, strings.TrimSpace(filter.ArxiuTipus))
	}
	if strings.TrimSpace(filter.TipusLlibre) != "" {
		clauses = append(clauses, "l.tipus_llibre = ?")
		args = append(args, strings.TrimSpace(filter.TipusLlibre))
	}
	if strings.TrimSpace(filter.Status) != "" {
		clauses = append(clauses, "l.moderation_status = ?")
		args = append(args, strings.TrimSpace(filter.Status))
	}
	allowedClauses := []string{}
	allowedArgs := []interface{}{}
	inClause := func(column string, ids []int) {
		if len(ids) == 0 {
			return
		}
		placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
		allowedClauses = append(allowedClauses, column+" IN ("+placeholders+")")
		for _, id := range ids {
			allowedArgs = append(allowedArgs, id)
		}
	}
	inClause("l.id", filter.AllowedLlibreIDs)
	inClause("l.municipi_id", filter.AllowedMunicipiIDs)
	inClause("l.arquevisbat_id", filter.AllowedEclesIDs)
	inClause("m.nivell_administratiu_id_3", filter.AllowedProvinciaIDs)
	inClause("m.nivell_administratiu_id_4", filter.AllowedComarcaIDs)
	inClause("na1.pais_id", filter.AllowedPaisIDs)
	if len(filter.AllowedArxiuIDs) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(filter.AllowedArxiuIDs)), ",")
		allowedClauses = append(allowedClauses, "EXISTS (SELECT 1 FROM arxius_llibres al WHERE al.llibre_id = l.id AND al.arxiu_id IN ("+placeholders+"))")
		for _, id := range filter.AllowedArxiuIDs {
			allowedArgs = append(allowedArgs, id)
		}
	}
	if len(allowedClauses) > 0 {
		clauses = append(clauses, "("+strings.Join(allowedClauses, " OR ")+")")
		args = append(args, allowedArgs...)
	}
	query := `
        SELECT COUNT(*)
        FROM llibres l
        LEFT JOIN municipis m ON m.id = l.municipi_id
        LEFT JOIN nivells_administratius na1 ON na1.id = m.nivell_administratiu_id_1
        WHERE ` + strings.Join(clauses, " AND ")
	query = formatPlaceholders(h.style, query)
	var total int
	if err := h.db.QueryRow(query, args...).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (h sqlHelper) getLlibre(id int) (*Llibre, error) {
	query := `
        SELECT id, arquevisbat_id, municipi_id, nom_esglesia, codi_digital, codi_fisic,
               titol, tipus_llibre, cronologia, volum, abat, contingut, llengua,
               requeriments_tecnics, unitat_catalogacio, unitat_instalacio, pagines,
               url_base, url_imatge_prefix, pagina, indexacio_completa,
               created_by, moderation_status, moderated_by, moderated_at, moderation_notes
        FROM llibres WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, id)
	var l Llibre
	if err := row.Scan(
		&l.ID, &l.ArquebisbatID, &l.MunicipiID, &l.NomEsglesia, &l.CodiDigital, &l.CodiFisic,
		&l.Titol, &l.TipusLlibre, &l.Cronologia, &l.Volum, &l.Abat, &l.Contingut, &l.Llengua,
		&l.Requeriments, &l.UnitatCatalogacio, &l.UnitatInstalacio, &l.Pagines,
		&l.URLBase, &l.URLImatgePrefix, &l.Pagina, &l.IndexacioCompleta,
		&l.CreatedBy, &l.ModeracioEstat, &l.ModeratedBy, &l.ModeratedAt, &l.ModeracioMotiu,
	); err != nil {
		return nil, err
	}
	return &l, nil
}

func (h sqlHelper) createLlibre(l *Llibre) (int, error) {
	query := `
        INSERT INTO llibres
            (arquevisbat_id, municipi_id, nom_esglesia, codi_digital, codi_fisic, titol, tipus_llibre, cronologia, volum, abat, contingut, llengua,
             requeriments_tecnics, unitat_catalogacio, unitat_instalacio, pagines, url_base, url_imatge_prefix, pagina, indexacio_completa,
             created_by, moderation_status, moderated_by, moderated_at, moderation_notes, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		query += ` RETURNING id`
	}
	query = formatPlaceholders(h.style, query)
	args := []interface{}{
		l.ArquebisbatID, l.MunicipiID, l.NomEsglesia, l.CodiDigital, l.CodiFisic, l.Titol, l.TipusLlibre, l.Cronologia, l.Volum, l.Abat, l.Contingut, l.Llengua,
		l.Requeriments, l.UnitatCatalogacio, l.UnitatInstalacio, l.Pagines, l.URLBase, l.URLImatgePrefix, l.Pagina, l.IndexacioCompleta,
		l.CreatedBy, l.ModeracioEstat, l.ModeratedBy, l.ModeratedAt, l.ModeracioMotiu,
	}
	if h.style == "postgres" {
		if err := h.db.QueryRow(query, args...).Scan(&l.ID); err != nil {
			return 0, err
		}
		return l.ID, nil
	}
	res, err := h.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		l.ID = int(id)
	}
	return l.ID, nil
}

func (h sqlHelper) updateLlibre(l *Llibre) error {
	query := `
        UPDATE llibres
        SET arquevisbat_id=?, municipi_id=?, nom_esglesia=?, codi_digital=?, codi_fisic=?, titol=?, tipus_llibre=?, cronologia=?, volum=?, abat=?, contingut=?, llengua=?,
            requeriments_tecnics=?, unitat_catalogacio=?, unitat_instalacio=?, pagines=?, url_base=?, url_imatge_prefix=?, pagina=?, indexacio_completa=?,
            moderation_status=?, moderated_by=?, moderated_at=?, moderation_notes=?, updated_at=` + h.nowFun + `
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query,
		l.ArquebisbatID, l.MunicipiID, l.NomEsglesia, l.CodiDigital, l.CodiFisic, l.Titol, l.TipusLlibre, l.Cronologia, l.Volum, l.Abat, l.Contingut, l.Llengua,
		l.Requeriments, l.UnitatCatalogacio, l.UnitatInstalacio, l.Pagines, l.URLBase, l.URLImatgePrefix, l.Pagina, l.IndexacioCompleta,
		l.ModeracioEstat, l.ModeratedBy, l.ModeratedAt, l.ModeracioMotiu, l.ID)
	return err
}

func (h sqlHelper) getLlibresIndexacioStats(ids []int) (map[int]LlibreIndexacioStats, error) {
	res := map[int]LlibreIndexacioStats{}
	if len(ids) == 0 {
		return res, nil
	}
	placeholders := buildInPlaceholders(h.style, len(ids))
	query := `
        SELECT llibre_id, total_registres, total_camps, camps_emplenats, percentatge, updated_at
        FROM llibres_indexacio_stats
        WHERE llibre_id IN (` + placeholders + `)`
	args := make([]interface{}, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var s LlibreIndexacioStats
		if err := rows.Scan(&s.LlibreID, &s.TotalRegistres, &s.TotalCamps, &s.CampsEmplenats, &s.Percentatge, &s.UpdatedAt); err != nil {
			return nil, err
		}
		res[s.LlibreID] = s
	}
	return res, nil
}

func (h sqlHelper) upsertLlibreIndexacioStats(s *LlibreIndexacioStats) error {
	switch h.style {
	case "mysql":
		stmt := `INSERT INTO llibres_indexacio_stats (llibre_id, total_registres, total_camps, camps_emplenats, percentatge, updated_at)
		         VALUES (?, ?, ?, ?, ?, ` + h.nowFun + `)
		         ON DUPLICATE KEY UPDATE total_registres = VALUES(total_registres), total_camps = VALUES(total_camps),
		         camps_emplenats = VALUES(camps_emplenats), percentatge = VALUES(percentatge), updated_at = ` + h.nowFun
		_, err := h.db.Exec(formatPlaceholders(h.style, stmt), s.LlibreID, s.TotalRegistres, s.TotalCamps, s.CampsEmplenats, s.Percentatge)
		return err
	case "postgres":
		stmt := `INSERT INTO llibres_indexacio_stats (llibre_id, total_registres, total_camps, camps_emplenats, percentatge, updated_at)
		         VALUES ($1, $2, $3, $4, $5, ` + h.nowFun + `)
		         ON CONFLICT (llibre_id) DO UPDATE SET total_registres = EXCLUDED.total_registres, total_camps = EXCLUDED.total_camps,
		         camps_emplenats = EXCLUDED.camps_emplenats, percentatge = EXCLUDED.percentatge, updated_at = ` + h.nowFun
		_, err := h.db.Exec(stmt, s.LlibreID, s.TotalRegistres, s.TotalCamps, s.CampsEmplenats, s.Percentatge)
		return err
	default: // sqlite
		stmt := `INSERT INTO llibres_indexacio_stats (llibre_id, total_registres, total_camps, camps_emplenats, percentatge, updated_at)
		         VALUES (?, ?, ?, ?, ?, ` + h.nowFun + `)
		         ON CONFLICT(llibre_id) DO UPDATE SET total_registres = excluded.total_registres, total_camps = excluded.total_camps,
		         camps_emplenats = excluded.camps_emplenats, percentatge = excluded.percentatge, updated_at = ` + h.nowFun
		_, err := h.db.Exec(formatPlaceholders(h.style, stmt), s.LlibreID, s.TotalRegistres, s.TotalCamps, s.CampsEmplenats, s.Percentatge)
		return err
	}
}

func (h sqlHelper) hasLlibreDuplicate(municipiID int, tipus, cronologia, codiDigital, codiFisic string, excludeID int) (bool, error) {
	if municipiID <= 0 {
		return false, nil
	}
	tipus = strings.TrimSpace(tipus)
	cronologia = strings.TrimSpace(cronologia)
	if tipus == "" || cronologia == "" {
		return false, nil
	}
	codes := []string{}
	args := []interface{}{municipiID, tipus, cronologia}
	if cd := strings.TrimSpace(codiDigital); cd != "" {
		codes = append(codes, "codi_digital = ?")
		args = append(args, cd)
	}
	if cf := strings.TrimSpace(codiFisic); cf != "" {
		codes = append(codes, "codi_fisic = ?")
		args = append(args, cf)
	}
	if len(codes) == 0 {
		return false, nil
	}
	query := `
        SELECT COUNT(1)
        FROM llibres
        WHERE municipi_id = ? AND tipus_llibre = ? AND cronologia = ? AND (` + strings.Join(codes, " OR ") + `)`
	if excludeID > 0 {
		query += " AND id <> ?"
		args = append(args, excludeID)
	}
	query = formatPlaceholders(h.style, query)
	var count int
	if err := h.db.QueryRow(query, args...).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func (h sqlHelper) listLlibrePagines(llibreID int) ([]LlibrePagina, error) {
	query := `
        SELECT id, llibre_id, num_pagina, estat, indexed_at, indexed_by, notes
        FROM llibre_pagines
        WHERE llibre_id = ?
        ORDER BY num_pagina`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, llibreID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []LlibrePagina
	for rows.Next() {
		var p LlibrePagina
		if err := rows.Scan(&p.ID, &p.LlibreID, &p.NumPagina, &p.Estat, &p.IndexedAt, &p.IndexedBy, &p.Notes); err != nil {
			return nil, err
		}
		res = append(res, p)
	}
	return res, nil
}

func (h sqlHelper) getLlibrePaginaByID(id int) (*LlibrePagina, error) {
	query := `
        SELECT id, llibre_id, num_pagina, estat, indexed_at, indexed_by, notes
        FROM llibre_pagines
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, id)
	var p LlibrePagina
	if err := row.Scan(&p.ID, &p.LlibreID, &p.NumPagina, &p.Estat, &p.IndexedAt, &p.IndexedBy, &p.Notes); err != nil {
		return nil, err
	}
	return &p, nil
}

func (h sqlHelper) saveLlibrePagina(p *LlibrePagina) (int, error) {
	if p.ID == 0 {
		query := `
            INSERT INTO llibre_pagines (llibre_id, num_pagina, estat, indexed_at, indexed_by, notes)
            VALUES (?, ?, ?, ?, ?, ?)`
		query = formatPlaceholders(h.style, query)
		if h.style == "postgres" {
			query += ` RETURNING id`
			if err := h.db.QueryRow(query, p.LlibreID, p.NumPagina, p.Estat, p.IndexedAt, p.IndexedBy, p.Notes).Scan(&p.ID); err != nil {
				return 0, err
			}
			return p.ID, nil
		}
		res, err := h.db.Exec(query, p.LlibreID, p.NumPagina, p.Estat, p.IndexedAt, p.IndexedBy, p.Notes)
		if err != nil {
			return 0, err
		}
		if id, err := res.LastInsertId(); err == nil {
			p.ID = int(id)
		}
		return p.ID, nil
	}
	query := `
        UPDATE llibre_pagines
        SET num_pagina=?, estat=?, indexed_at=?, indexed_by=?, notes=?
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query, p.NumPagina, p.Estat, p.IndexedAt, p.IndexedBy, p.Notes, p.ID)
	return p.ID, err
}

// Transcripcions RAW
func (h sqlHelper) transcripcionsRawFilters(llibreID int, f TranscripcioFilter) (string, []interface{}, string) {
	clauses := []string{}
	args := []interface{}{}
	join := ""
	if llibreID > 0 {
		clauses = append(clauses, "t.llibre_id = ?")
		args = append(args, llibreID)
	} else if f.LlibreID > 0 {
		clauses = append(clauses, "t.llibre_id = ?")
		args = append(args, f.LlibreID)
	}
	if strings.TrimSpace(f.TipusActe) != "" {
		clauses = append(clauses, "t.tipus_acte = ?")
		args = append(args, strings.TrimSpace(f.TipusActe))
	}
	if f.AnyDoc > 0 {
		clauses = append(clauses, "t.any_doc = ?")
		args = append(args, f.AnyDoc)
	}
	if f.PaginaID > 0 {
		clauses = append(clauses, "t.pagina_id = ?")
		args = append(args, f.PaginaID)
	}
	if strings.TrimSpace(f.Status) != "" {
		clauses = append(clauses, "t.moderation_status = ?")
		args = append(args, strings.TrimSpace(f.Status))
	}
	if strings.TrimSpace(f.Qualitat) != "" {
		clauses = append(clauses, "t.data_acte_estat = ?")
		args = append(args, strings.TrimSpace(f.Qualitat))
	}
	if strings.TrimSpace(f.Search) != "" {
		search := strings.TrimSpace(f.Search)
		if f.UseFullText {
			joinClause, searchClause, searchArgs := h.fullTextSearchClause(search)
			if joinClause != "" {
				join = joinClause
			}
			if searchClause != "" {
				clauses = append(clauses, searchClause)
				args = append(args, searchArgs...)
			}
		} else {
			join = "LEFT JOIN transcripcions_persones_raw p ON p.transcripcio_id = t.id"
			search = "%" + search + "%"
			likeOp := "LIKE"
			if h.style == "postgres" {
				likeOp = "ILIKE"
			}
			clauses = append(clauses, "(t.transcripcio_literal "+likeOp+" ? OR t.notes_marginals "+likeOp+" ? OR t.observacions_paleografiques "+likeOp+" ? OR p.nom "+likeOp+" ? OR p.cognom1 "+likeOp+" ? OR p.cognom2 "+likeOp+" ?)")
			args = append(args, search, search, search, search, search, search)
		}
	}
	if len(clauses) == 0 {
		clauses = append(clauses, "1=1")
	}
	return strings.Join(clauses, " AND "), args, join
}

func (h sqlHelper) fullTextSearchClause(search string) (string, string, []interface{}) {
	join := "LEFT JOIN transcripcions_persones_raw p ON p.transcripcio_id = t.id"
	if strings.TrimSpace(search) == "" {
		return "", "", nil
	}
	if h.style == "postgres" {
		clause := "(to_tsvector('simple', coalesce(t.transcripcio_literal,'') || ' ' || coalesce(t.notes_marginals,'') || ' ' || coalesce(t.observacions_paleografiques,'') || ' ' || coalesce(p.nom,'') || ' ' || coalesce(p.cognom1,'') || ' ' || coalesce(p.cognom2,'')) @@ plainto_tsquery('simple', ?))"
		return join, clause, []interface{}{search}
	}
	search = "%" + search + "%"
	likeOp := "LIKE"
	if h.style == "postgres" {
		likeOp = "ILIKE"
	}
	clause := "(t.transcripcio_literal " + likeOp + " ? OR t.notes_marginals " + likeOp + " ? OR t.observacions_paleografiques " + likeOp + " ? OR p.nom " + likeOp + " ? OR p.cognom1 " + likeOp + " ? OR p.cognom2 " + likeOp + " ?)"
	return join, clause, []interface{}{search, search, search, search, search, search}
}

func (h sqlHelper) listTranscripcionsRaw(llibreID int, f TranscripcioFilter) ([]TranscripcioRaw, error) {
	where, args, join := h.transcripcionsRawFilters(llibreID, f)
	limit := 50
	offset := 0
	withLimit := true
	if f.Limit == -1 {
		withLimit = false
	}
	if f.Limit > 0 {
		limit = f.Limit
	}
	if f.Offset > 0 {
		offset = f.Offset
	}
	query := `
        SELECT DISTINCT t.id, t.llibre_id, t.pagina_id, t.num_pagina_text, t.posicio_pagina, t.tipus_acte, t.any_doc,
               t.data_acte_text, t.data_acte_iso, t.data_acte_estat, t.transcripcio_literal, t.notes_marginals, t.observacions_paleografiques,
               t.moderation_status, t.moderated_by, t.moderated_at, t.moderation_notes, t.created_by, t.created_at, t.updated_at
        FROM transcripcions_raw t
        ` + join + `
        WHERE ` + where + `
        ORDER BY t.any_doc, t.pagina_id, t.posicio_pagina, t.id`
	if withLimit {
		query += `
        LIMIT ? OFFSET ?`
		args = append(args, limit, offset)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []TranscripcioRaw
	for rows.Next() {
		var t TranscripcioRaw
		if err := rows.Scan(
			&t.ID, &t.LlibreID, &t.PaginaID, &t.NumPaginaText, &t.PosicioPagina, &t.TipusActe, &t.AnyDoc,
			&t.DataActeText, &t.DataActeISO, &t.DataActeEstat, &t.TranscripcioLiteral, &t.NotesMarginals, &t.ObservacionsPaleografiques,
			&t.ModeracioEstat, &t.ModeratedBy, &t.ModeratedAt, &t.ModeracioMotiu, &t.CreatedBy, &t.CreatedAt, &t.UpdatedAt,
		); err != nil {
			return nil, err
		}
		res = append(res, t)
	}
	return res, nil
}

func (h sqlHelper) listTranscripcionsRawGlobal(f TranscripcioFilter) ([]TranscripcioRaw, error) {
	return h.listTranscripcionsRaw(0, f)
}

func (h sqlHelper) countTranscripcionsRaw(llibreID int, f TranscripcioFilter) (int, error) {
	where, args, join := h.transcripcionsRawFilters(llibreID, f)
	query := `
        SELECT COUNT(DISTINCT t.id)
        FROM transcripcions_raw t
        ` + join + `
        WHERE ` + where
	query = formatPlaceholders(h.style, query)
	var total int
	if err := h.db.QueryRow(query, args...).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (h sqlHelper) countTranscripcionsRawGlobal(f TranscripcioFilter) (int, error) {
	return h.countTranscripcionsRaw(0, f)
}

func (h sqlHelper) recalcTranscripcionsRawPageStats(llibreID int) error {
	resetStmt := formatPlaceholders(h.style, `UPDATE transcripcions_raw_page_stats SET total_registres = 0, computed_at = `+h.nowFun+` WHERE llibre_id = ?`)
	if _, err := h.db.Exec(resetStmt, llibreID); err != nil {
		return err
	}
	insertStmt := `
        INSERT INTO transcripcions_raw_page_stats (
            llibre_id, pagina_id, num_pagina_text, total_registres, computed_at
        )
        SELECT
            t.llibre_id,
            t.pagina_id,
            COALESCE(pd.valor_text, NULLIF(TRIM(t.num_pagina_text), '')) AS num_pagina_text,
            COUNT(*),
            ` + h.nowFun + `
        FROM transcripcions_raw t
        LEFT JOIN (
            SELECT
                pd.transcripcio_id,
                MAX(NULLIF(TRIM(pd.valor_text), '')) AS valor_text
            FROM transcripcions_atributs_raw pd
            JOIN transcripcions_raw t2
                ON t2.id = pd.transcripcio_id
            WHERE pd.clau = 'pagina_digital'
              AND t2.llibre_id = ?
              AND pd.valor_text IS NOT NULL
              AND TRIM(pd.valor_text) <> ''
              AND pd.valor_text NOT LIKE '%-%'
            GROUP BY pd.transcripcio_id
        ) pd
            ON pd.transcripcio_id = t.id
        WHERE t.llibre_id = ?
          AND COALESCE(pd.valor_text, NULLIF(TRIM(t.num_pagina_text), '')) IS NOT NULL
          AND COALESCE(pd.valor_text, NULLIF(TRIM(t.num_pagina_text), '')) <> ''
          AND COALESCE(pd.valor_text, NULLIF(TRIM(t.num_pagina_text), '')) NOT LIKE '%-%'
        GROUP BY t.llibre_id, t.pagina_id, COALESCE(pd.valor_text, NULLIF(TRIM(t.num_pagina_text), ''))`
	switch h.style {
	case "mysql":
		insertStmt += `
        ON DUPLICATE KEY UPDATE
            total_registres = VALUES(total_registres),
            computed_at = VALUES(computed_at)`
	case "postgres":
		insertStmt += `
        ON CONFLICT (llibre_id, pagina_id, num_pagina_text)
        DO UPDATE SET
            total_registres = EXCLUDED.total_registres,
            computed_at = EXCLUDED.computed_at`
	default:
		insertStmt += `
        ON CONFLICT (llibre_id, pagina_id, num_pagina_text)
        DO UPDATE SET
            total_registres = excluded.total_registres,
            computed_at = excluded.computed_at`
	}
	insertStmt = formatPlaceholders(h.style, insertStmt)
	_, err := h.db.Exec(insertStmt, llibreID, llibreID)
	return err
}

func (h sqlHelper) setTranscripcionsRawPageStatsIndexacio(llibreID int, value int) error {
	query := `UPDATE transcripcions_raw_page_stats
        SET indexacio_completa = ?, computed_at = ` + h.nowFun + `
        WHERE llibre_id = ?`
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query, value, llibreID)
	return err
}

func (h sqlHelper) listTranscripcionsRawPageStats(llibreID int) ([]TranscripcioRawPageStat, error) {
	query := `
        SELECT id, llibre_id, pagina_id, num_pagina_text,
               COALESCE(tipus_pagina, 'normal') AS tipus_pagina,
               COALESCE(exclosa, 0) AS exclosa,
               COALESCE(indexacio_completa, 0) AS indexacio_completa,
               duplicada_de,
               total_registres, computed_at
        FROM transcripcions_raw_page_stats
        WHERE llibre_id = ?
        ORDER BY num_pagina_text, pagina_id, id`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, llibreID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []TranscripcioRawPageStat
	for rows.Next() {
		var stat TranscripcioRawPageStat
		if err := rows.Scan(
			&stat.ID,
			&stat.LlibreID,
			&stat.PaginaID,
			&stat.NumPaginaText,
			&stat.TipusPagina,
			&stat.Exclosa,
			&stat.IndexacioCompleta,
			&stat.DuplicadaDe,
			&stat.TotalRegistres,
			&stat.ComputedAt,
		); err != nil {
			return nil, err
		}
		res = append(res, stat)
	}
	return res, nil
}

func (h sqlHelper) updateTranscripcionsRawPageStat(stat *TranscripcioRawPageStat) error {
	if stat.TipusPagina == "" {
		stat.TipusPagina = "normal"
	}
	if stat.ID == 0 {
		query := `
            INSERT INTO transcripcions_raw_page_stats
                (llibre_id, pagina_id, num_pagina_text, tipus_pagina, exclosa, indexacio_completa, duplicada_de, total_registres, computed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `)`
		switch h.style {
		case "mysql":
			query += `
            ON DUPLICATE KEY UPDATE
                tipus_pagina = VALUES(tipus_pagina),
                exclosa = VALUES(exclosa),
                indexacio_completa = VALUES(indexacio_completa),
                duplicada_de = VALUES(duplicada_de),
                total_registres = VALUES(total_registres),
                computed_at = VALUES(computed_at)`
		case "postgres":
			query += `
            ON CONFLICT (llibre_id, pagina_id, num_pagina_text)
            DO UPDATE SET
                tipus_pagina = EXCLUDED.tipus_pagina,
                exclosa = EXCLUDED.exclosa,
                indexacio_completa = EXCLUDED.indexacio_completa,
                duplicada_de = EXCLUDED.duplicada_de,
                total_registres = EXCLUDED.total_registres,
                computed_at = EXCLUDED.computed_at`
		default:
			query += `
            ON CONFLICT (llibre_id, pagina_id, num_pagina_text)
            DO UPDATE SET
                tipus_pagina = excluded.tipus_pagina,
                exclosa = excluded.exclosa,
                indexacio_completa = excluded.indexacio_completa,
                duplicada_de = excluded.duplicada_de,
                total_registres = excluded.total_registres,
                computed_at = excluded.computed_at`
		}
		query = formatPlaceholders(h.style, query)
		_, err := h.db.Exec(query, stat.LlibreID, stat.PaginaID, stat.NumPaginaText, stat.TipusPagina, stat.Exclosa, stat.IndexacioCompleta, stat.DuplicadaDe, stat.TotalRegistres)
		return err
	}
	query := `
        UPDATE transcripcions_raw_page_stats
        SET pagina_id = ?, num_pagina_text = ?, tipus_pagina = ?, exclosa = ?, indexacio_completa = ?, duplicada_de = ?, total_registres = ?, computed_at = ` + h.nowFun + `
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query, stat.PaginaID, stat.NumPaginaText, stat.TipusPagina, stat.Exclosa, stat.IndexacioCompleta, stat.DuplicadaDe, stat.TotalRegistres, stat.ID)
	return err
}

func (h sqlHelper) getTranscripcioRaw(id int) (*TranscripcioRaw, error) {
	query := `
        SELECT id, llibre_id, pagina_id, num_pagina_text, posicio_pagina, tipus_acte, any_doc,
               data_acte_text, data_acte_iso, data_acte_estat, transcripcio_literal, notes_marginals, observacions_paleografiques,
               moderation_status, moderated_by, moderated_at, moderation_notes, created_by, created_at, updated_at
        FROM transcripcions_raw
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, id)
	var t TranscripcioRaw
	if err := row.Scan(
		&t.ID, &t.LlibreID, &t.PaginaID, &t.NumPaginaText, &t.PosicioPagina, &t.TipusActe, &t.AnyDoc,
		&t.DataActeText, &t.DataActeISO, &t.DataActeEstat, &t.TranscripcioLiteral, &t.NotesMarginals, &t.ObservacionsPaleografiques,
		&t.ModeracioEstat, &t.ModeratedBy, &t.ModeratedAt, &t.ModeracioMotiu, &t.CreatedBy, &t.CreatedAt, &t.UpdatedAt,
	); err != nil {
		return nil, err
	}
	return &t, nil
}

func (h sqlHelper) createTranscripcioRaw(t *TranscripcioRaw) (int, error) {
	status := strings.TrimSpace(t.ModeracioEstat)
	if status == "" {
		status = "pendent"
	}
	query := `
        INSERT INTO transcripcions_raw (
            llibre_id, pagina_id, num_pagina_text, posicio_pagina, tipus_acte, any_doc, data_acte_text, data_acte_iso, data_acte_estat,
            transcripcio_literal, notes_marginals, observacions_paleografiques,
            moderation_status, moderated_by, moderated_at, moderation_notes, created_by, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		query += " RETURNING id"
	}
	query = formatPlaceholders(h.style, query)
	args := []interface{}{
		t.LlibreID, t.PaginaID, t.NumPaginaText, t.PosicioPagina, t.TipusActe, t.AnyDoc, t.DataActeText, t.DataActeISO, t.DataActeEstat,
		t.TranscripcioLiteral, t.NotesMarginals, t.ObservacionsPaleografiques,
		status, t.ModeratedBy, t.ModeratedAt, t.ModeracioMotiu, t.CreatedBy,
	}
	if h.style == "postgres" {
		if err := h.db.QueryRow(query, args...).Scan(&t.ID); err != nil {
			return 0, err
		}
		return t.ID, nil
	}
	res, err := h.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		t.ID = int(id)
	}
	return t.ID, nil
}

func (h sqlHelper) updateTranscripcioRaw(t *TranscripcioRaw) error {
	status := strings.TrimSpace(t.ModeracioEstat)
	if status == "" {
		status = "pendent"
	}
	query := `
        UPDATE transcripcions_raw
        SET llibre_id = ?, pagina_id = ?, num_pagina_text = ?, posicio_pagina = ?, tipus_acte = ?, any_doc = ?, data_acte_text = ?, data_acte_iso = ?, data_acte_estat = ?,
            transcripcio_literal = ?, notes_marginals = ?, observacions_paleografiques = ?,
            moderation_status = ?, moderated_by = ?, moderated_at = ?, moderation_notes = ?, updated_at = ` + h.nowFun + `
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query,
		t.LlibreID, t.PaginaID, t.NumPaginaText, t.PosicioPagina, t.TipusActe, t.AnyDoc, t.DataActeText, t.DataActeISO, t.DataActeEstat,
		t.TranscripcioLiteral, t.NotesMarginals, t.ObservacionsPaleografiques,
		status, t.ModeratedBy, t.ModeratedAt, t.ModeracioMotiu, t.ID)
	return err
}

func (h sqlHelper) deleteTranscripcioRaw(id int) error {
	stmt := formatPlaceholders(h.style, `DELETE FROM transcripcions_raw WHERE id = ?`)
	_, err := h.db.Exec(stmt, id)
	return err
}

func (h sqlHelper) deleteTranscripcionsByLlibre(llibreID int) error {
	if llibreID == 0 {
		return nil
	}
	stmtDrafts := formatPlaceholders(h.style, `DELETE FROM transcripcions_raw_drafts WHERE llibre_id = ?`)
	if _, err := h.db.Exec(stmtDrafts, llibreID); err != nil {
		return err
	}
	stmt := formatPlaceholders(h.style, `DELETE FROM transcripcions_raw WHERE llibre_id = ?`)
	_, err := h.db.Exec(stmt, llibreID)
	return err
}

func (h sqlHelper) createTranscripcioRawChange(c *TranscripcioRawChange) (int, error) {
	estado := c.ModeracioEstat
	if strings.TrimSpace(estado) == "" {
		estado = "publicat"
	}
	query := `
        INSERT INTO transcripcions_raw_canvis (
            transcripcio_id, change_type, field_key, old_value, new_value, metadata,
            moderation_status, moderated_by, moderated_at, moderation_notes,
            changed_by, changed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `)`
	if h.style == "postgres" {
		query += " RETURNING id"
	}
	query = formatPlaceholders(h.style, query)
	args := []interface{}{
		c.TranscripcioID,
		c.ChangeType,
		c.FieldKey,
		c.OldValue,
		c.NewValue,
		c.Metadata,
		estado,
		c.ModeratedBy,
		c.ModeratedAt,
		c.ModeracioMotiu,
		c.ChangedBy,
	}
	if h.style == "postgres" {
		if err := h.db.QueryRow(query, args...).Scan(&c.ID); err != nil {
			return 0, err
		}
		return c.ID, nil
	}
	res, err := h.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		c.ID = int(id)
	}
	return c.ID, nil
}

func (h sqlHelper) listTranscripcioRawChanges(transcripcioID int) ([]TranscripcioRawChange, error) {
	query := `
        SELECT id, transcripcio_id, change_type, field_key, old_value, new_value, metadata,
               moderation_status, moderated_by, moderated_at, moderation_notes,
               changed_by, changed_at
        FROM transcripcions_raw_canvis
        WHERE transcripcio_id = ?
        ORDER BY changed_at DESC, id DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, transcripcioID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []TranscripcioRawChange
	for rows.Next() {
		var c TranscripcioRawChange
		if err := rows.Scan(
			&c.ID,
			&c.TranscripcioID,
			&c.ChangeType,
			&c.FieldKey,
			&c.OldValue,
			&c.NewValue,
			&c.Metadata,
			&c.ModeracioEstat,
			&c.ModeratedBy,
			&c.ModeratedAt,
			&c.ModeracioMotiu,
			&c.ChangedBy,
			&c.ChangedAt,
		); err != nil {
			return nil, err
		}
		res = append(res, c)
	}
	return res, rows.Err()
}

func (h sqlHelper) getTranscripcioRawChange(id int) (*TranscripcioRawChange, error) {
	query := `
        SELECT id, transcripcio_id, change_type, field_key, old_value, new_value, metadata,
               moderation_status, moderated_by, moderated_at, moderation_notes,
               changed_by, changed_at
        FROM transcripcions_raw_canvis
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	var c TranscripcioRawChange
	if err := h.db.QueryRow(query, id).Scan(
		&c.ID,
		&c.TranscripcioID,
		&c.ChangeType,
		&c.FieldKey,
		&c.OldValue,
		&c.NewValue,
		&c.Metadata,
		&c.ModeracioEstat,
		&c.ModeratedBy,
		&c.ModeratedAt,
		&c.ModeracioMotiu,
		&c.ChangedBy,
		&c.ChangedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &c, nil
}

func (h sqlHelper) listTranscripcioRawChangesPending() ([]TranscripcioRawChange, error) {
	query := `
        SELECT id, transcripcio_id, change_type, field_key, old_value, new_value, metadata,
               moderation_status, moderated_by, moderated_at, moderation_notes,
               changed_by, changed_at
        FROM transcripcions_raw_canvis
        WHERE moderation_status = 'pendent'
        ORDER BY changed_at DESC, id DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []TranscripcioRawChange
	for rows.Next() {
		var c TranscripcioRawChange
		if err := rows.Scan(
			&c.ID,
			&c.TranscripcioID,
			&c.ChangeType,
			&c.FieldKey,
			&c.OldValue,
			&c.NewValue,
			&c.Metadata,
			&c.ModeracioEstat,
			&c.ModeratedBy,
			&c.ModeratedAt,
			&c.ModeracioMotiu,
			&c.ChangedBy,
			&c.ChangedAt,
		); err != nil {
			return nil, err
		}
		res = append(res, c)
	}
	return res, rows.Err()
}

func (h sqlHelper) updateTranscripcioRawChangeModeracio(id int, estat, motiu string, moderatorID int) error {
	stmt := `UPDATE transcripcions_raw_canvis SET moderation_status = ?, moderation_notes = ?, moderated_by = ?, moderated_at = ? WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	now := time.Now()
	_, err := h.db.Exec(stmt, estat, motiu, moderatorID, now, id)
	return err
}

func (h sqlHelper) listTranscripcioPersones(transcripcioID int) ([]TranscripcioPersonaRaw, error) {
	query := `
        SELECT id, transcripcio_id, rol, nom, nom_estat, cognom1, cognom1_estat, cognom2, cognom2_estat, sexe, sexe_estat,
               edat_text, edat_estat, estat_civil_text, estat_civil_estat, municipi_text, municipi_estat, ofici_text, ofici_estat,
               casa_nom, casa_estat, persona_id, linked_by, linked_at, notes
        FROM transcripcions_persones_raw
        WHERE transcripcio_id = ?
        ORDER BY id`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, transcripcioID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []TranscripcioPersonaRaw
	for rows.Next() {
		var p TranscripcioPersonaRaw
		if err := rows.Scan(
			&p.ID, &p.TranscripcioID, &p.Rol, &p.Nom, &p.NomEstat, &p.Cognom1, &p.Cognom1Estat, &p.Cognom2, &p.Cognom2Estat, &p.Sexe, &p.SexeEstat,
			&p.EdatText, &p.EdatEstat, &p.EstatCivilText, &p.EstatCivilEstat, &p.MunicipiText, &p.MunicipiEstat, &p.OficiText, &p.OficiEstat,
			&p.CasaNom, &p.CasaEstat, &p.PersonaID, &p.LinkedBy, &p.LinkedAt, &p.Notes,
		); err != nil {
			return nil, err
		}
		res = append(res, p)
	}
	return res, nil
}

func (h sqlHelper) createTranscripcioPersona(p *TranscripcioPersonaRaw) (int, error) {
	query := `
        INSERT INTO transcripcions_persones_raw (
            transcripcio_id, rol, nom, nom_estat, cognom1, cognom1_estat, cognom2, cognom2_estat, sexe, sexe_estat,
            edat_text, edat_estat, estat_civil_text, estat_civil_estat, municipi_text, municipi_estat, ofici_text, ofici_estat,
            casa_nom, casa_estat, persona_id, linked_by, linked_at, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if h.style == "postgres" {
		query += " RETURNING id"
	}
	query = formatPlaceholders(h.style, query)
	args := []interface{}{
		p.TranscripcioID, p.Rol, p.Nom, p.NomEstat, p.Cognom1, p.Cognom1Estat, p.Cognom2, p.Cognom2Estat, p.Sexe, p.SexeEstat,
		p.EdatText, p.EdatEstat, p.EstatCivilText, p.EstatCivilEstat, p.MunicipiText, p.MunicipiEstat, p.OficiText, p.OficiEstat,
		p.CasaNom, p.CasaEstat, p.PersonaID, p.LinkedBy, p.LinkedAt, p.Notes,
	}
	if h.style == "postgres" {
		if err := h.db.QueryRow(query, args...).Scan(&p.ID); err != nil {
			return 0, err
		}
		return p.ID, nil
	}
	res, err := h.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		p.ID = int(id)
	}
	return p.ID, nil
}

func (h sqlHelper) linkTranscripcioPersona(personaRawID int, personaID int, linkedBy int) error {
	query := `
        UPDATE transcripcions_persones_raw
        SET persona_id = ?, linked_by = ?, linked_at = ` + h.nowFun + `
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query, personaID, linkedBy, personaRawID)
	return err
}

func (h sqlHelper) unlinkTranscripcioPersona(personaRawID int, linkedBy int) error {
	query := `
        UPDATE transcripcions_persones_raw
        SET persona_id = NULL, linked_by = ?, linked_at = ` + h.nowFun + `
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query, linkedBy, personaRawID)
	return err
}

func (h sqlHelper) deleteTranscripcioPersones(transcripcioID int) error {
	stmt := formatPlaceholders(h.style, `DELETE FROM transcripcions_persones_raw WHERE transcripcio_id = ?`)
	_, err := h.db.Exec(stmt, transcripcioID)
	return err
}

func (h sqlHelper) listTranscripcioAtributs(transcripcioID int) ([]TranscripcioAtributRaw, error) {
	query := `
        SELECT id, transcripcio_id, clau, tipus_valor, valor_text, valor_int, valor_date, valor_bool, estat, notes
        FROM transcripcions_atributs_raw
        WHERE transcripcio_id = ?
        ORDER BY id`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, transcripcioID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []TranscripcioAtributRaw
	for rows.Next() {
		var a TranscripcioAtributRaw
		if err := rows.Scan(&a.ID, &a.TranscripcioID, &a.Clau, &a.TipusValor, &a.ValorText, &a.ValorInt, &a.ValorDate, &a.ValorBool, &a.Estat, &a.Notes); err != nil {
			return nil, err
		}
		res = append(res, a)
	}
	return res, nil
}

func (h sqlHelper) createTranscripcioAtribut(a *TranscripcioAtributRaw) (int, error) {
	query := `
        INSERT INTO transcripcions_atributs_raw (transcripcio_id, clau, tipus_valor, valor_text, valor_int, valor_date, valor_bool, estat, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if h.style == "postgres" {
		query += " RETURNING id"
	}
	query = formatPlaceholders(h.style, query)
	if h.style == "postgres" {
		if err := h.db.QueryRow(query, a.TranscripcioID, a.Clau, a.TipusValor, a.ValorText, a.ValorInt, a.ValorDate, a.ValorBool, a.Estat, a.Notes).Scan(&a.ID); err != nil {
			return 0, err
		}
		return a.ID, nil
	}
	res, err := h.db.Exec(query, a.TranscripcioID, a.Clau, a.TipusValor, a.ValorText, a.ValorInt, a.ValorDate, a.ValorBool, a.Estat, a.Notes)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		a.ID = int(id)
	}
	return a.ID, nil
}

func (h sqlHelper) deleteTranscripcioAtributs(transcripcioID int) error {
	stmt := formatPlaceholders(h.style, `DELETE FROM transcripcions_atributs_raw WHERE transcripcio_id = ?`)
	_, err := h.db.Exec(stmt, transcripcioID)
	return err
}

func (h sqlHelper) getTranscripcioDraft(userID, llibreID int) (*TranscripcioDraft, error) {
	query := `
        SELECT id, llibre_id, user_id, payload, updated_at
        FROM transcripcions_raw_drafts
        WHERE llibre_id = ? AND user_id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, llibreID, userID)
	var d TranscripcioDraft
	if err := row.Scan(&d.ID, &d.LlibreID, &d.UserID, &d.Payload, &d.UpdatedAt); err != nil {
		return nil, err
	}
	return &d, nil
}

func (h sqlHelper) saveTranscripcioDraft(userID, llibreID int, payload string) error {
	query := `
        INSERT INTO transcripcions_raw_drafts (llibre_id, user_id, payload, created_at, updated_at)
        VALUES (?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	switch h.style {
	case "postgres", "sqlite":
		query += ` ON CONFLICT (llibre_id, user_id) DO UPDATE SET payload = excluded.payload, updated_at = ` + h.nowFun
	case "mysql":
		query += ` ON DUPLICATE KEY UPDATE payload = VALUES(payload), updated_at = ` + h.nowFun
	}
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query, llibreID, userID, payload)
	return err
}

func (h sqlHelper) deleteTranscripcioDraft(userID, llibreID int) error {
	stmt := formatPlaceholders(h.style, `DELETE FROM transcripcions_raw_drafts WHERE llibre_id = ? AND user_id = ?`)
	_, err := h.db.Exec(stmt, llibreID, userID)
	return err
}

func (h sqlHelper) listTranscripcioMarks(transcripcioIDs []int) ([]TranscripcioRawMark, error) {
	if len(transcripcioIDs) == 0 {
		return []TranscripcioRawMark{}, nil
	}
	placeholders := buildInPlaceholders(h.style, len(transcripcioIDs))
	query := fmt.Sprintf(`
        SELECT id, transcripcio_id, user_id, tipus, is_public, created_at, updated_at
        FROM transcripcions_raw_marques
        WHERE transcripcio_id IN (%s)`, placeholders)
	args := make([]interface{}, len(transcripcioIDs))
	for i, id := range transcripcioIDs {
		args[i] = id
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []TranscripcioRawMark
	for rows.Next() {
		var m TranscripcioRawMark
		var isPublic interface{}
		if err := rows.Scan(&m.ID, &m.TranscripcioID, &m.UserID, &m.Tipus, &isPublic, &m.CreatedAt, &m.UpdatedAt); err != nil {
			return nil, err
		}
		m.IsPublic = parseBoolValue(isPublic)
		res = append(res, m)
	}
	return res, nil
}

func (h sqlHelper) upsertTranscripcioMark(m *TranscripcioRawMark) error {
	query := `
        INSERT INTO transcripcions_raw_marques (transcripcio_id, user_id, tipus, is_public, created_at, updated_at)
        VALUES (?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	switch h.style {
	case "postgres", "sqlite":
		query += ` ON CONFLICT (transcripcio_id, user_id)
        DO UPDATE SET tipus = excluded.tipus, is_public = excluded.is_public, updated_at = ` + h.nowFun
	case "mysql":
		query += ` ON DUPLICATE KEY UPDATE tipus = VALUES(tipus), is_public = VALUES(is_public), updated_at = ` + h.nowFun
	}
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query, m.TranscripcioID, m.UserID, m.Tipus, m.IsPublic)
	return err
}

func (h sqlHelper) deleteTranscripcioMark(transcripcioID, userID int) error {
	stmt := formatPlaceholders(h.style, `DELETE FROM transcripcions_raw_marques WHERE transcripcio_id = ? AND user_id = ?`)
	_, err := h.db.Exec(stmt, transcripcioID, userID)
	return err
}

func (h sqlHelper) getWikiMark(objectType string, objectID int, userID int) (*WikiMark, error) {
	if strings.TrimSpace(objectType) == "" || objectID == 0 || userID == 0 {
		return nil, nil
	}
	query := `
        SELECT id, object_type, object_id, user_id, tipus, is_public, created_at, updated_at
        FROM wiki_marques
        WHERE object_type = ? AND object_id = ? AND user_id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, objectType, objectID, userID)
	var m WikiMark
	var isPublic interface{}
	if err := row.Scan(&m.ID, &m.ObjectType, &m.ObjectID, &m.UserID, &m.Tipus, &isPublic, &m.CreatedAt, &m.UpdatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	m.IsPublic = parseBoolValue(isPublic)
	return &m, nil
}

func (h sqlHelper) listWikiMarks(objectType string, objectIDs []int) ([]WikiMark, error) {
	if strings.TrimSpace(objectType) == "" || len(objectIDs) == 0 {
		return []WikiMark{}, nil
	}
	placeholders := buildInPlaceholders(h.style, len(objectIDs))
	query := fmt.Sprintf(`
        SELECT id, object_type, object_id, user_id, tipus, is_public, created_at, updated_at
        FROM wiki_marques
        WHERE object_type = ? AND object_id IN (%s)`, placeholders)
	args := make([]interface{}, 0, len(objectIDs)+1)
	args = append(args, objectType)
	for _, id := range objectIDs {
		args = append(args, id)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []WikiMark
	for rows.Next() {
		var m WikiMark
		var isPublic interface{}
		if err := rows.Scan(&m.ID, &m.ObjectType, &m.ObjectID, &m.UserID, &m.Tipus, &isPublic, &m.CreatedAt, &m.UpdatedAt); err != nil {
			return nil, err
		}
		m.IsPublic = parseBoolValue(isPublic)
		res = append(res, m)
	}
	return res, rows.Err()
}

func (h sqlHelper) upsertWikiMark(m *WikiMark) error {
	query := `
        INSERT INTO wiki_marques (object_type, object_id, user_id, tipus, is_public, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	switch h.style {
	case "postgres", "sqlite":
		query += ` ON CONFLICT (object_type, object_id, user_id)
        DO UPDATE SET tipus = excluded.tipus, is_public = excluded.is_public, updated_at = ` + h.nowFun
	case "mysql":
		query += ` ON DUPLICATE KEY UPDATE tipus = VALUES(tipus), is_public = VALUES(is_public), updated_at = ` + h.nowFun
	}
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query, m.ObjectType, m.ObjectID, m.UserID, m.Tipus, m.IsPublic)
	return err
}

func (h sqlHelper) deleteWikiMark(objectType string, objectID int, userID int) error {
	stmt := formatPlaceholders(h.style, `DELETE FROM wiki_marques WHERE object_type = ? AND object_id = ? AND user_id = ?`)
	_, err := h.db.Exec(stmt, objectType, objectID, userID)
	return err
}

func (h sqlHelper) incWikiPublicCount(objectType string, objectID int, tipus string, delta int) error {
	if strings.TrimSpace(objectType) == "" || objectID == 0 || strings.TrimSpace(tipus) == "" || delta == 0 {
		return nil
	}
	if delta < 0 {
		stmt := `
            UPDATE wiki_marks_stats
            SET public_count = CASE WHEN public_count + ? < 0 THEN 0 ELSE public_count + ? END,
                updated_at = ` + h.nowFun + `
            WHERE object_type = ? AND object_id = ? AND tipus = ?`
		stmt = formatPlaceholders(h.style, stmt)
		_, err := h.db.Exec(stmt, delta, delta, objectType, objectID, tipus)
		return err
	}
	query := `
        INSERT INTO wiki_marks_stats (object_type, object_id, tipus, public_count, updated_at)
        VALUES (?, ?, ?, ?, ` + h.nowFun + `)`
	switch h.style {
	case "postgres", "sqlite":
		query += ` ON CONFLICT (object_type, object_id, tipus)
        DO UPDATE SET public_count = wiki_marks_stats.public_count + excluded.public_count,
                      updated_at = ` + h.nowFun
	case "mysql":
		query += ` ON DUPLICATE KEY UPDATE public_count = public_count + VALUES(public_count),
                      updated_at = ` + h.nowFun
	}
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query, objectType, objectID, tipus, delta)
	return err
}

func (h sqlHelper) getWikiPublicCounts(objectType string, objectID int) (map[string]int, error) {
	if strings.TrimSpace(objectType) == "" || objectID == 0 {
		return map[string]int{}, nil
	}
	query := `
        SELECT tipus, public_count
        FROM wiki_marks_stats
        WHERE object_type = ? AND object_id = ?`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, objectType, objectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := map[string]int{}
	for rows.Next() {
		var tipus string
		var raw interface{}
		if err := rows.Scan(&tipus, &raw); err != nil {
			return nil, err
		}
		res[tipus] = parseIntValue(raw)
	}
	return res, rows.Err()
}

func (h sqlHelper) createWikiChange(c *WikiChange) (int, error) {
	estado := strings.TrimSpace(c.ModeracioEstat)
	if estado == "" {
		estado = "pendent"
	}
	query := `
        INSERT INTO wiki_canvis (
            object_type, object_id, change_type, field_key, old_value, new_value, metadata,
            moderation_status, moderated_by, moderated_at, moderation_notes,
            changed_by, changed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `)`
	if h.style == "postgres" {
		query += " RETURNING id"
	}
	query = formatPlaceholders(h.style, query)
	args := []interface{}{
		c.ObjectType,
		c.ObjectID,
		c.ChangeType,
		c.FieldKey,
		c.OldValue,
		c.NewValue,
		c.Metadata,
		estado,
		c.ModeratedBy,
		c.ModeratedAt,
		c.ModeracioMotiu,
		c.ChangedBy,
	}
	if h.style == "postgres" {
		if err := h.db.QueryRow(query, args...).Scan(&c.ID); err != nil {
			return 0, err
		}
		if estado == "pendent" {
			if err := h.enqueueWikiPending(c); err != nil {
				_, _ = h.db.Exec(formatPlaceholders(h.style, `DELETE FROM wiki_canvis WHERE id = ?`), c.ID)
				return 0, fmt.Errorf("wiki_pending_enqueue: %w", err)
			}
		}
		return c.ID, nil
	}
	res, err := h.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		c.ID = int(id)
	}
	if estado == "pendent" {
		if err := h.enqueueWikiPending(c); err != nil {
			_, _ = h.db.Exec(formatPlaceholders(h.style, `DELETE FROM wiki_canvis WHERE id = ?`), c.ID)
			return 0, fmt.Errorf("wiki_pending_enqueue: %w", err)
		}
	}
	return c.ID, nil
}

func (h sqlHelper) getWikiChange(id int) (*WikiChange, error) {
	query := `
        SELECT id, object_type, object_id, change_type, field_key, old_value, new_value, metadata,
               moderation_status, moderated_by, moderated_at, moderation_notes,
               changed_by, changed_at
        FROM wiki_canvis
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	var c WikiChange
	if err := h.db.QueryRow(query, id).Scan(
		&c.ID,
		&c.ObjectType,
		&c.ObjectID,
		&c.ChangeType,
		&c.FieldKey,
		&c.OldValue,
		&c.NewValue,
		&c.Metadata,
		&c.ModeracioEstat,
		&c.ModeratedBy,
		&c.ModeratedAt,
		&c.ModeracioMotiu,
		&c.ChangedBy,
		&c.ChangedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &c, nil
}

func (h sqlHelper) listWikiChanges(objectType string, objectID int) ([]WikiChange, error) {
	query := `
        SELECT id, object_type, object_id, change_type, field_key, old_value, new_value, metadata,
               moderation_status, moderated_by, moderated_at, moderation_notes,
               changed_by, changed_at
        FROM wiki_canvis
        WHERE object_type = ? AND object_id = ?
        ORDER BY changed_at DESC, id DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, objectType, objectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []WikiChange
	for rows.Next() {
		var c WikiChange
		if err := rows.Scan(
			&c.ID,
			&c.ObjectType,
			&c.ObjectID,
			&c.ChangeType,
			&c.FieldKey,
			&c.OldValue,
			&c.NewValue,
			&c.Metadata,
			&c.ModeracioEstat,
			&c.ModeratedBy,
			&c.ModeratedAt,
			&c.ModeracioMotiu,
			&c.ChangedBy,
			&c.ChangedAt,
		); err != nil {
			return nil, err
		}
		res = append(res, c)
	}
	return res, rows.Err()
}

func (h sqlHelper) listWikiChangesPending(objectType string, limit int) ([]WikiChange, error) {
	where := `moderation_status = 'pendent'`
	args := []interface{}{}
	if strings.TrimSpace(objectType) != "" {
		where += ` AND object_type = ?`
		args = append(args, objectType)
	}
	query := `
        SELECT id, object_type, object_id, change_type, field_key, old_value, new_value, metadata,
               moderation_status, moderated_by, moderated_at, moderation_notes,
               changed_by, changed_at
        FROM wiki_canvis
        WHERE ` + where + `
        ORDER BY changed_at DESC, id DESC`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []WikiChange
	for rows.Next() {
		var c WikiChange
		if err := rows.Scan(
			&c.ID,
			&c.ObjectType,
			&c.ObjectID,
			&c.ChangeType,
			&c.FieldKey,
			&c.OldValue,
			&c.NewValue,
			&c.Metadata,
			&c.ModeracioEstat,
			&c.ModeratedBy,
			&c.ModeratedAt,
			&c.ModeracioMotiu,
			&c.ChangedBy,
			&c.ChangedAt,
		); err != nil {
			return nil, err
		}
		res = append(res, c)
	}
	return res, rows.Err()
}

func (h sqlHelper) updateWikiChangeModeracio(id int, estat, motiu string, moderatorID int) error {
	stmt := `UPDATE wiki_canvis SET moderation_status = ?, moderation_notes = ?, moderated_by = ?, moderated_at = ? WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	now := time.Now()
	if _, err := h.db.Exec(stmt, estat, motiu, moderatorID, now, id); err != nil {
		return err
	}
	if strings.TrimSpace(estat) != "pendent" {
		if err := h.dequeueWikiPending(id); err != nil {
			return fmt.Errorf("wiki_pending_dequeue: %w", err)
		}
	}
	return nil
}

// CSV import templates
func (h sqlHelper) createCSVImportTemplate(t *CSVImportTemplate) (int, error) {
	if t == nil {
		return 0, fmt.Errorf("template invalid")
	}
	name := strings.TrimSpace(t.Name)
	if name == "" {
		return 0, fmt.Errorf("name invalid")
	}
	modelJSON := strings.TrimSpace(t.ModelJSON)
	if modelJSON == "" {
		return 0, fmt.Errorf("model_json invalid")
	}
	signature := ComputeTemplateSignature(modelJSON)
	t.Signature = signature
	visibility := strings.TrimSpace(t.Visibility)
	if visibility == "" {
		visibility = "private"
	}
	var owner interface{}
	if t.OwnerUserID.Valid {
		owner = t.OwnerUserID.Int64
	}
	var description interface{}
	if strings.TrimSpace(t.Description) != "" {
		description = strings.TrimSpace(t.Description)
	}
	var sep interface{}
	if strings.TrimSpace(t.DefaultSeparator) != "" {
		sep = strings.TrimSpace(t.DefaultSeparator)
	}
	query := `
        INSERT INTO csv_import_templates
            (name, description, owner_user_id, visibility, default_separator, model_json, signature, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		query += " RETURNING id"
	}
	query = formatPlaceholders(h.style, query)
	args := []interface{}{name, description, owner, visibility, sep, modelJSON, signature}
	if h.style == "postgres" {
		if err := h.db.QueryRow(query, args...).Scan(&t.ID); err != nil {
			return 0, err
		}
		return t.ID, nil
	}
	res, err := h.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		t.ID = int(id)
	}
	return t.ID, nil
}

func (h sqlHelper) updateCSVImportTemplate(t *CSVImportTemplate) error {
	if t == nil || t.ID == 0 {
		return fmt.Errorf("template invalid")
	}
	name := strings.TrimSpace(t.Name)
	if name == "" {
		return fmt.Errorf("name invalid")
	}
	modelJSON := strings.TrimSpace(t.ModelJSON)
	if modelJSON == "" {
		return fmt.Errorf("model_json invalid")
	}
	signature := ComputeTemplateSignature(modelJSON)
	t.Signature = signature
	visibility := strings.TrimSpace(t.Visibility)
	if visibility == "" {
		visibility = "private"
	}
	var description interface{}
	if strings.TrimSpace(t.Description) != "" {
		description = strings.TrimSpace(t.Description)
	}
	var sep interface{}
	if strings.TrimSpace(t.DefaultSeparator) != "" {
		sep = strings.TrimSpace(t.DefaultSeparator)
	}
	query := `
        UPDATE csv_import_templates
        SET name = ?, description = ?, visibility = ?, default_separator = ?, model_json = ?, signature = ?, updated_at = ` + h.nowFun + `
        WHERE id = ?`
	args := []interface{}{name, description, visibility, sep, modelJSON, signature, t.ID}
	if t.OwnerUserID.Valid {
		query += " AND owner_user_id = ?"
		args = append(args, t.OwnerUserID.Int64)
	}
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query, args...)
	return err
}

func (h sqlHelper) getCSVImportTemplate(id int) (*CSVImportTemplate, error) {
	query := `
        SELECT id, name, COALESCE(description, ''), owner_user_id, visibility,
               COALESCE(default_separator, ''), model_json, signature, created_at, updated_at
        FROM csv_import_templates
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, id)
	var t CSVImportTemplate
	if err := row.Scan(&t.ID, &t.Name, &t.Description, &t.OwnerUserID, &t.Visibility, &t.DefaultSeparator, &t.ModelJSON, &t.Signature, &t.CreatedAt, &t.UpdatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &t, nil
}

func (h sqlHelper) listCSVImportTemplates(filter CSVImportTemplateFilter) ([]CSVImportTemplate, error) {
	where := []string{"1=1"}
	args := []interface{}{}
	if filter.OwnerUserID > 0 {
		if filter.IncludePublic {
			where = append(where, "(owner_user_id = ? OR visibility = 'public')")
			args = append(args, filter.OwnerUserID)
		} else {
			where = append(where, "owner_user_id = ?")
			args = append(args, filter.OwnerUserID)
		}
	} else if filter.IncludePublic {
		where = append(where, "visibility = 'public'")
	} else {
		return []CSVImportTemplate{}, nil
	}
	if q := strings.TrimSpace(filter.Query); q != "" {
		likeOp := "LIKE"
		if h.style == "postgres" {
			likeOp = "ILIKE"
		}
		where = append(where, "(name "+likeOp+" ? OR description "+likeOp+" ?)")
		q = "%" + q + "%"
		args = append(args, q, q)
	}
	query := `
        SELECT id, name, COALESCE(description, ''), owner_user_id, visibility,
               COALESCE(default_separator, ''), model_json, signature, created_at, updated_at
        FROM csv_import_templates
        WHERE ` + strings.Join(where, " AND ") + `
        ORDER BY created_at DESC`
	if filter.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", filter.Limit)
	}
	if filter.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", filter.Offset)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []CSVImportTemplate
	for rows.Next() {
		var t CSVImportTemplate
		if err := rows.Scan(&t.ID, &t.Name, &t.Description, &t.OwnerUserID, &t.Visibility, &t.DefaultSeparator, &t.ModelJSON, &t.Signature, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		res = append(res, t)
	}
	return res, rows.Err()
}

func (h sqlHelper) deleteCSVImportTemplate(id int) error {
	stmt := formatPlaceholders(h.style, `DELETE FROM csv_import_templates WHERE id = ?`)
	_, err := h.db.Exec(stmt, id)
	return err
}

func (h sqlHelper) enqueueWikiPending(change *WikiChange) error {
	if change == nil || change.ID == 0 {
		return fmt.Errorf("change invàlid")
	}
	changedAt := change.ChangedAt
	if changedAt.IsZero() {
		changedAt = time.Now()
	}
	createdAt := time.Now()
	stmt := `
        INSERT INTO wiki_pending_queue (change_id, object_type, object_id, changed_at, changed_by, created_at)
        VALUES (?, ?, ?, ?, ?, ?)`
	switch h.style {
	case "postgres", "sqlite":
		stmt += ` ON CONFLICT (change_id) DO NOTHING`
	case "mysql":
		stmt += ` ON DUPLICATE KEY UPDATE change_id = change_id`
	}
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, change.ID, change.ObjectType, change.ObjectID, changedAt, change.ChangedBy, createdAt)
	return err
}

func (h sqlHelper) dequeueWikiPending(changeID int) error {
	stmt := formatPlaceholders(h.style, `DELETE FROM wiki_pending_queue WHERE change_id = ?`)
	_, err := h.db.Exec(stmt, changeID)
	return err
}

func (h sqlHelper) listWikiPending(limit int) ([]WikiPendingItem, error) {
	query := `
        SELECT change_id, object_type, object_id, changed_at, changed_by, created_at
        FROM wiki_pending_queue
        ORDER BY changed_at DESC, change_id DESC`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []WikiPendingItem
	for rows.Next() {
		var item WikiPendingItem
		if err := rows.Scan(&item.ChangeID, &item.ObjectType, &item.ObjectID, &item.ChangedAt, &item.ChangedBy, &item.CreatedAt); err != nil {
			return nil, err
		}
		res = append(res, item)
	}
	return res, rows.Err()
}

func (h sqlHelper) searchPersones(f PersonaSearchFilter) ([]PersonaSearchResult, error) {
	where := []string{"1=1"}
	args := []interface{}{}
	likeOp := "LIKE"
	if h.style == "postgres" {
		likeOp = "ILIKE"
	}
	expandedClause, expandedArgs := buildExpandedCognomClause(likeOp, f.UseCognomDictionary, f.ExpandedCognoms)
	if strings.TrimSpace(f.Query) != "" {
		q := "%" + strings.TrimSpace(f.Query) + "%"
		queryClause := "(nom " + likeOp + " ? OR cognom1 " + likeOp + " ? OR cognom2 " + likeOp + " ? OR nom_complet " + likeOp + " ?)"
		queryArgs := []interface{}{q, q, q, q}
		if expandedClause != "" {
			queryClause = "(" + queryClause + " OR " + expandedClause + ")"
			queryArgs = append(queryArgs, expandedArgs...)
		}
		where = append(where, queryClause)
		args = append(args, queryArgs...)
	} else if expandedClause != "" {
		where = append(where, expandedClause)
		args = append(args, expandedArgs...)
	}
	if strings.TrimSpace(f.Nom) != "" {
		q := "%" + strings.TrimSpace(f.Nom) + "%"
		where = append(where, "nom "+likeOp+" ?")
		args = append(args, q)
	}
	if strings.TrimSpace(f.Cognom1) != "" {
		q := "%" + strings.TrimSpace(f.Cognom1) + "%"
		where = append(where, "cognom1 "+likeOp+" ?")
		args = append(args, q)
	}
	if strings.TrimSpace(f.Cognom2) != "" {
		q := "%" + strings.TrimSpace(f.Cognom2) + "%"
		where = append(where, "cognom2 "+likeOp+" ?")
		args = append(args, q)
	}
	if strings.TrimSpace(f.Municipi) != "" {
		q := "%" + strings.TrimSpace(f.Municipi) + "%"
		where = append(where, "municipi "+likeOp+" ?")
		args = append(args, q)
	}
	if f.AnyMin > 0 || f.AnyMax > 0 {
		minYear := f.AnyMin
		maxYear := f.AnyMax
		if minYear == 0 {
			minYear = maxYear
		}
		if maxYear == 0 {
			maxYear = minYear
		}
		start := fmt.Sprintf("%04d-01-01", minYear)
		end := fmt.Sprintf("%04d-12-31", maxYear)
		where = append(where, "((data_naixement BETWEEN ? AND ?) OR (data_bateig BETWEEN ? AND ?) OR (data_defuncio BETWEEN ? AND ?))")
		args = append(args, start, end, start, end, start, end)
	}
	limit := 10
	if f.Limit > 0 {
		limit = f.Limit
	}
	query := `
        SELECT id, nom, cognom1, cognom2, municipi, data_naixement, data_bateig, data_defuncio, ofici, estat_civil
        FROM persona
        WHERE ` + strings.Join(where, " AND ") + `
        ORDER BY id DESC
        LIMIT ?`
	args = append(args, limit)
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []PersonaSearchResult
	for rows.Next() {
		var p PersonaSearchResult
		if err := rows.Scan(&p.ID, &p.Nom, &p.Cognom1, &p.Cognom2, &p.Municipi, &p.DataNaixement, &p.DataBateig, &p.DataDefuncio, &p.Ofici, &p.EstatCivil); err != nil {
			return nil, err
		}
		res = append(res, p)
	}
	return res, nil
}

func buildExpandedCognomClause(likeOp string, enabled bool, forms []string) (string, []interface{}) {
	if !enabled || len(forms) == 0 {
		return "", nil
	}
	const maxForms = 60
	seen := map[string]struct{}{}
	var parts []string
	var args []interface{}
	for _, form := range forms {
		form = strings.TrimSpace(form)
		if form == "" {
			continue
		}
		key := strings.ToLower(form)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		parts = append(parts, "(cognom1 "+likeOp+" ? OR cognom2 "+likeOp+" ?)")
		args = append(args, form, form)
		if len(args)/2 >= maxForms {
			break
		}
	}
	if len(parts) == 0 {
		return "", nil
	}
	return "(" + strings.Join(parts, " OR ") + ")", args
}

func (h sqlHelper) listRegistresByPersona(personaID int, tipus string) ([]PersonaRegistreRow, error) {
	where := []string{"p.persona_id = ?"}
	args := []interface{}{personaID}
	if strings.TrimSpace(tipus) != "" {
		where = append(where, "t.tipus_acte = ?")
		args = append(args, strings.TrimSpace(tipus))
	}
	query := `
        SELECT t.id, p.id, t.llibre_id, l.titol, l.nom_esglesia, t.tipus_acte, t.any_doc, t.data_acte_text,
               t.pagina_id, t.num_pagina_text, t.posicio_pagina, p.rol, t.moderation_status
        FROM transcripcions_persones_raw p
        JOIN transcripcions_raw t ON t.id = p.transcripcio_id
        JOIN llibres l ON l.id = t.llibre_id
        WHERE ` + strings.Join(where, " AND ") + `
        ORDER BY t.any_doc, t.id`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []PersonaRegistreRow
	for rows.Next() {
		var row PersonaRegistreRow
		if err := rows.Scan(&row.RegistreID, &row.PersonaRawID, &row.LlibreID, &row.LlibreTitol, &row.LlibreNom, &row.TipusActe, &row.AnyDoc, &row.DataActeText, &row.PaginaID, &row.NumPaginaText, &row.PosicioPagina, &row.Rol, &row.ModeracioEstat); err != nil {
			return nil, err
		}
		res = append(res, row)
	}
	return res, nil
}

func (h sqlHelper) recalcLlibrePagines(llibreID, total int) error {
	if total <= 0 {
		return nil
	}
	tx, err := h.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	delStmt := formatPlaceholders(h.style, `DELETE FROM llibre_pagines WHERE llibre_id = ?`)
	if _, err := tx.Exec(delStmt, llibreID); err != nil {
		return err
	}
	insertStmt := formatPlaceholders(h.style, `
        INSERT INTO llibre_pagines (llibre_id, num_pagina, estat)
        VALUES (?, ?, 'pendent')`)
	for i := 1; i <= total; i++ {
		if _, err := tx.Exec(insertStmt, llibreID, i); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Cognoms
func (h sqlHelper) listCognoms(q string, limit, offset int) ([]Cognom, error) {
	keyCol := "key"
	if h.style == "mysql" {
		keyCol = "`key`"
	}
	query := fmt.Sprintf(`
        SELECT id, forma, %s, origen, notes, created_by, created_at, updated_at
        FROM cognoms`, keyCol)
	var where []string
	var args []interface{}
	if strings.TrimSpace(q) != "" {
		likeOp := "LIKE"
		if h.style == "postgres" {
			likeOp = "ILIKE"
		}
		where = append(where, "(forma "+likeOp+" ? OR "+keyCol+" "+likeOp+" ?)")
		qLike := "%" + strings.TrimSpace(q) + "%"
		args = append(args, qLike, qLike)
	}
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY forma"
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	if offset > 0 {
		query += " OFFSET ?"
		args = append(args, offset)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []Cognom
	for rows.Next() {
		var c Cognom
		var origen sql.NullString
		var notes sql.NullString
		if err := rows.Scan(&c.ID, &c.Forma, &c.Key, &origen, &notes, &c.CreatedBy, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, err
		}
		c.Origen = origen.String
		c.Notes = notes.String
		res = append(res, c)
	}
	return res, nil
}

func (h sqlHelper) getCognom(id int) (*Cognom, error) {
	keyCol := "key"
	if h.style == "mysql" {
		keyCol = "`key`"
	}
	query := fmt.Sprintf("SELECT id, forma, %s, origen, notes, created_by, created_at, updated_at FROM cognoms WHERE id = ?", keyCol)
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, id)
	var c Cognom
	var origen sql.NullString
	var notes sql.NullString
	if err := row.Scan(&c.ID, &c.Forma, &c.Key, &origen, &notes, &c.CreatedBy, &c.CreatedAt, &c.UpdatedAt); err != nil {
		return nil, err
	}
	c.Origen = origen.String
	c.Notes = notes.String
	return &c, nil
}

func (h sqlHelper) updateCognom(c *Cognom) error {
	stmt := `UPDATE cognoms SET origen = ?, notes = ?, updated_at = ? WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, c.Origen, c.Notes, time.Now(), c.ID)
	return err
}

func (h sqlHelper) upsertCognom(forma, key, origen, notes string, createdBy *int) (int, error) {
	keyCol := "key"
	if h.style == "mysql" {
		keyCol = "`key`"
	}
	var createdByVal interface{}
	if createdBy != nil {
		createdByVal = *createdBy
	}
	if h.style == "postgres" {
		stmt := fmt.Sprintf(`
            INSERT INTO cognoms (forma, %s, origen, notes, created_by, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, %s, %s)
            ON CONFLICT (%s) DO UPDATE
            SET forma = EXCLUDED.forma, origen = EXCLUDED.origen, notes = EXCLUDED.notes, updated_at = %s
            RETURNING id`, keyCol, h.nowFun, h.nowFun, keyCol, h.nowFun)
		stmt = formatPlaceholders(h.style, stmt)
		var id int
		if err := h.db.QueryRow(stmt, forma, key, origen, notes, createdByVal).Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}

	stmt := fmt.Sprintf(`
        INSERT INTO cognoms (forma, %s, origen, notes, created_by, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, %s, %s)`, keyCol, h.nowFun, h.nowFun)
	if h.style == "mysql" {
		stmt += " ON DUPLICATE KEY UPDATE forma=VALUES(forma), origen=VALUES(origen), notes=VALUES(notes), updated_at=" + h.nowFun
	} else {
		stmt += " ON CONFLICT(" + keyCol + ") DO UPDATE SET forma=excluded.forma, origen=excluded.origen, notes=excluded.notes, updated_at=" + h.nowFun
	}
	stmt = formatPlaceholders(h.style, stmt)
	if _, err := h.db.Exec(stmt, forma, key, origen, notes, createdByVal); err != nil {
		return 0, err
	}
	selectStmt := fmt.Sprintf("SELECT id FROM cognoms WHERE %s = ?", keyCol)
	selectStmt = formatPlaceholders(h.style, selectStmt)
	row := h.db.QueryRow(selectStmt, key)
	var id int
	if err := row.Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

func (h sqlHelper) getNom(id int) (*Nom, error) {
	keyCol := "key"
	if h.style == "mysql" {
		keyCol = "`key`"
	}
	query := fmt.Sprintf("SELECT id, forma, %s, notes, created_by, created_at, updated_at FROM noms WHERE id = ?", keyCol)
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, id)
	var n Nom
	var notes sql.NullString
	if err := row.Scan(&n.ID, &n.Forma, &n.Key, &notes, &n.CreatedBy, &n.CreatedAt, &n.UpdatedAt); err != nil {
		return nil, err
	}
	n.Notes = notes.String
	return &n, nil
}

func (h sqlHelper) upsertNom(forma, key, notes string, createdBy *int) (int, error) {
	keyCol := "key"
	if h.style == "mysql" {
		keyCol = "`key`"
	}
	var createdByVal interface{}
	if createdBy != nil {
		createdByVal = *createdBy
	}
	if h.style == "postgres" {
		stmt := fmt.Sprintf(`
            INSERT INTO noms (forma, %s, notes, created_by, created_at, updated_at)
            VALUES (?, ?, ?, ?, %s, %s)
            ON CONFLICT (%s) DO UPDATE
            SET forma = EXCLUDED.forma, notes = EXCLUDED.notes, updated_at = %s
            RETURNING id`, keyCol, h.nowFun, h.nowFun, keyCol, h.nowFun)
		stmt = formatPlaceholders(h.style, stmt)
		var id int
		if err := h.db.QueryRow(stmt, forma, key, notes, createdByVal).Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}

	stmt := fmt.Sprintf(`
        INSERT INTO noms (forma, %s, notes, created_by, created_at, updated_at)
        VALUES (?, ?, ?, ?, %s, %s)`, keyCol, h.nowFun, h.nowFun)
	if h.style == "mysql" {
		stmt += " ON DUPLICATE KEY UPDATE forma=VALUES(forma), notes=VALUES(notes), updated_at=" + h.nowFun
	} else {
		stmt += " ON CONFLICT(" + keyCol + ") DO UPDATE SET forma=excluded.forma, notes=excluded.notes, updated_at=" + h.nowFun
	}
	stmt = formatPlaceholders(h.style, stmt)
	if _, err := h.db.Exec(stmt, forma, key, notes, createdByVal); err != nil {
		return 0, err
	}
	selectStmt := fmt.Sprintf("SELECT id FROM noms WHERE %s = ?", keyCol)
	selectStmt = formatPlaceholders(h.style, selectStmt)
	row := h.db.QueryRow(selectStmt, key)
	var id int
	if err := row.Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

func (h sqlHelper) resolveNomByForma(forma string) (int, string, bool, error) {
	key := normalizeNomKey(forma)
	if key == "" {
		return 0, "", false, nil
	}
	keyCol := "key"
	if h.style == "mysql" {
		keyCol = "`key`"
	}
	query := fmt.Sprintf("SELECT id, forma FROM noms WHERE %s = ? LIMIT 1", keyCol)
	query = formatPlaceholders(h.style, query)
	var id int
	var canon string
	err := h.db.QueryRow(query, key).Scan(&id, &canon)
	if err == nil {
		return id, canon, true, nil
	}
	if err != sql.ErrNoRows {
		return 0, "", false, err
	}
	cleanForma := strings.TrimSpace(forma)
	id, err = h.upsertNom(cleanForma, key, "", nil)
	if err != nil {
		return 0, "", false, err
	}
	return id, cleanForma, true, nil
}

func (h sqlHelper) listCognomVariants(f CognomVariantFilter) ([]CognomVariant, error) {
	keyCol := "key"
	if h.style == "mysql" {
		keyCol = "`key`"
	}
	query := fmt.Sprintf(`
        SELECT id, cognom_id, variant, %s, llengua, any_inici, any_fi, pais_id, municipi_id,
               moderation_status, moderated_by, moderated_at, moderation_notes, created_by, created_at, updated_at
        FROM cognom_variants`, keyCol)
	var where []string
	var args []interface{}
	if f.CognomID > 0 {
		where = append(where, "cognom_id = ?")
		args = append(args, f.CognomID)
	}
	if strings.TrimSpace(f.Status) != "" {
		where = append(where, "moderation_status = ?")
		args = append(args, strings.TrimSpace(f.Status))
	}
	if strings.TrimSpace(f.Q) != "" {
		likeOp := "LIKE"
		if h.style == "postgres" {
			likeOp = "ILIKE"
		}
		where = append(where, "(variant "+likeOp+" ? OR "+keyCol+" "+likeOp+" ?)")
		qLike := "%" + strings.TrimSpace(f.Q) + "%"
		args = append(args, qLike, qLike)
	}
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY id DESC"
	if f.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, f.Limit)
	}
	if f.Offset > 0 {
		query += " OFFSET ?"
		args = append(args, f.Offset)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []CognomVariant
	for rows.Next() {
		var v CognomVariant
		var llengua sql.NullString
		var motiu sql.NullString
		if err := rows.Scan(&v.ID, &v.CognomID, &v.Variant, &v.Key, &llengua, &v.AnyInici, &v.AnyFi, &v.PaisID, &v.MunicipiID, &v.ModeracioEstat, &v.ModeratedBy, &v.ModeratedAt, &motiu, &v.CreatedBy, &v.CreatedAt, &v.UpdatedAt); err != nil {
			return nil, err
		}
		v.Llengua = llengua.String
		v.ModeracioMotiu = motiu.String
		res = append(res, v)
	}
	return res, nil
}

func (h sqlHelper) resolveCognomPublicatByForma(forma string) (int, string, bool, error) {
	key := normalizeCognomKey(forma)
	if key == "" {
		return 0, "", false, nil
	}
	keyCol := "key"
	if h.style == "mysql" {
		keyCol = "`key`"
	}
	query := fmt.Sprintf("SELECT id, forma FROM cognoms WHERE %s = ? LIMIT 1", keyCol)
	query = formatPlaceholders(h.style, query)
	var id int
	var canon string
	err := h.db.QueryRow(query, key).Scan(&id, &canon)
	if err == nil {
		return id, canon, true, nil
	}
	if err != sql.ErrNoRows {
		return 0, "", false, err
	}
	variantKey := "v." + keyCol
	query = fmt.Sprintf(`
        SELECT c.id, c.forma
        FROM cognom_variants v
        JOIN cognoms c ON c.id = v.cognom_id
        WHERE v.moderation_status = 'publicat' AND %s = ?
        LIMIT 1`, variantKey)
	query = formatPlaceholders(h.style, query)
	err = h.db.QueryRow(query, key).Scan(&id, &canon)
	if err == nil {
		return id, canon, true, nil
	}
	if err == sql.ErrNoRows {
		return 0, "", false, nil
	}
	return 0, "", false, err
}

func (h sqlHelper) listCognomFormesPublicades(cognomID int) ([]string, error) {
	query := "SELECT forma FROM cognoms WHERE id = ?"
	query = formatPlaceholders(h.style, query)
	var canon string
	if err := h.db.QueryRow(query, cognomID).Scan(&canon); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	forms := []string{}
	seen := map[string]struct{}{}
	canon = strings.TrimSpace(canon)
	if canon != "" {
		forms = append(forms, canon)
		seen[strings.ToLower(canon)] = struct{}{}
	}
	query = `
        SELECT variant
        FROM cognom_variants
        WHERE cognom_id = ? AND moderation_status = 'publicat'
        ORDER BY variant
        LIMIT 100`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, cognomID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var variant string
		if err := rows.Scan(&variant); err != nil {
			return nil, err
		}
		variant = strings.TrimSpace(variant)
		if variant == "" {
			continue
		}
		key := strings.ToLower(variant)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		forms = append(forms, variant)
	}
	return forms, nil
}

func (h sqlHelper) createCognomVariant(v *CognomVariant) (int, error) {
	keyCol := "key"
	if h.style == "mysql" {
		keyCol = "`key`"
	}
	status := v.ModeracioEstat
	if strings.TrimSpace(status) == "" {
		status = "pendent"
	}
	if h.style == "postgres" {
		stmt := fmt.Sprintf(`
            INSERT INTO cognom_variants (cognom_id, variant, %s, llengua, any_inici, any_fi, pais_id, municipi_id,
                moderation_status, moderated_by, moderated_at, moderation_notes, created_by, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, %s, %s)
            RETURNING id`, keyCol, h.nowFun, h.nowFun)
		stmt = formatPlaceholders(h.style, stmt)
		var id int
		if err := h.db.QueryRow(stmt, v.CognomID, v.Variant, v.Key, v.Llengua, v.AnyInici, v.AnyFi, v.PaisID, v.MunicipiID, status, v.ModeratedBy, v.ModeratedAt, v.ModeracioMotiu, v.CreatedBy).Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}
	stmt := fmt.Sprintf(`
        INSERT INTO cognom_variants (cognom_id, variant, %s, llengua, any_inici, any_fi, pais_id, municipi_id,
            moderation_status, moderated_by, moderated_at, moderation_notes, created_by, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, %s, %s)`, keyCol, h.nowFun, h.nowFun)
	stmt = formatPlaceholders(h.style, stmt)
	res, err := h.db.Exec(stmt, v.CognomID, v.Variant, v.Key, v.Llengua, v.AnyInici, v.AnyFi, v.PaisID, v.MunicipiID, status, v.ModeratedBy, v.ModeratedAt, v.ModeracioMotiu, v.CreatedBy)
	if err != nil {
		return 0, err
	}
	id, _ := res.LastInsertId()
	if id == 0 {
		row := h.db.QueryRow(formatPlaceholders(h.style, "SELECT id FROM cognom_variants WHERE cognom_id = ? AND "+keyCol+" = ?"), v.CognomID, v.Key)
		if err := row.Scan(&id); err != nil {
			return 0, err
		}
	}
	return int(id), nil
}

func normalizeCognomKey(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	s = strings.ToLower(s)
	s = stripDiacritics(s)
	s = strings.ReplaceAll(s, "’", "")
	s = strings.ReplaceAll(s, "'", "")
	s = strings.ReplaceAll(s, "-", " ")
	s = strings.ReplaceAll(s, ".", " ")
	s = strings.ReplaceAll(s, ",", " ")
	s = strings.Join(strings.Fields(s), " ")
	s = strings.ReplaceAll(s, " ", "")
	return strings.ToUpper(s)
}

func normalizeNomKey(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	s = strings.ToLower(s)
	s = stripDiacritics(s)
	s = strings.ReplaceAll(s, "’", "")
	s = strings.ReplaceAll(s, "'", "")
	s = strings.ReplaceAll(s, "-", " ")
	s = strings.ReplaceAll(s, ".", " ")
	s = strings.ReplaceAll(s, ",", " ")
	s = strings.Join(strings.Fields(s), " ")
	s = strings.ReplaceAll(s, " ", "")
	return strings.ToUpper(s)
}

func stripDiacritics(val string) string {
	replacer := strings.NewReplacer(
		"à", "a", "á", "a", "â", "a", "ä", "a", "ã", "a", "å", "a",
		"è", "e", "é", "e", "ê", "e", "ë", "e",
		"ì", "i", "í", "i", "î", "i", "ï", "i",
		"ò", "o", "ó", "o", "ô", "o", "ö", "o", "õ", "o",
		"ù", "u", "ú", "u", "û", "u", "ü", "u",
		"ç", "c", "ñ", "n",
		"·", "",
	)
	return replacer.Replace(val)
}

func (h sqlHelper) updateCognomVariantModeracio(id int, estat, motiu string, moderatorID int) error {
	stmt := `UPDATE cognom_variants SET moderation_status = ?, moderation_notes = ?, moderated_by = ?, moderated_at = ?, updated_at = ? WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	now := time.Now()
	_, err := h.db.Exec(stmt, estat, motiu, moderatorID, now, now, id)
	return err
}

func (h sqlHelper) listCognomImportRows(limit, offset int) ([]CognomImportRow, error) {
	query := `
        SELECT p.cognom1, p.cognom1_estat, p.cognom2, p.cognom2_estat
        FROM transcripcions_persones_raw p
        JOIN transcripcions_raw r ON r.id = p.transcripcio_id
        WHERE r.moderation_status = 'publicat'
        ORDER BY p.id`
	var args []interface{}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	if offset > 0 {
		query += " OFFSET ?"
		args = append(args, offset)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []CognomImportRow
	for rows.Next() {
		var row CognomImportRow
		if err := rows.Scan(&row.Cognom1, &row.Cognom1Estat, &row.Cognom2, &row.Cognom2Estat); err != nil {
			return nil, err
		}
		res = append(res, row)
	}
	return res, nil
}

func (h sqlHelper) listCognomStatsRows(limit, offset int) ([]CognomStatsRow, error) {
	query := `
        SELECT p.cognom1, p.cognom1_estat, p.cognom2, p.cognom2_estat, r.any_doc, l.municipi_id
        FROM transcripcions_persones_raw p
        JOIN transcripcions_raw r ON r.id = p.transcripcio_id
        JOIN llibres l ON l.id = r.llibre_id
        WHERE r.moderation_status = 'publicat'
          AND r.any_doc BETWEEN 0 AND 2025
          AND l.municipi_id IS NOT NULL
        ORDER BY r.id`
	var args []interface{}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	if offset > 0 {
		query += " OFFSET ?"
		args = append(args, offset)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []CognomStatsRow
	for rows.Next() {
		var row CognomStatsRow
		if err := rows.Scan(&row.Cognom1, &row.Cognom1Estat, &row.Cognom2, &row.Cognom2Estat, &row.AnyDoc, &row.MunicipiID); err != nil {
			return nil, err
		}
		res = append(res, row)
	}
	return res, nil
}

func (h sqlHelper) upsertCognomFreqMunicipiAny(cognomID, municipiID, anyDoc, freq int) error {
	stmt := fmt.Sprintf(`
        INSERT INTO cognoms_freq_municipi_any (cognom_id, municipi_id, any_doc, freq, updated_at)
        VALUES (?, ?, ?, ?, %s)`, h.nowFun)
	if h.style == "mysql" {
		stmt += " ON DUPLICATE KEY UPDATE freq = VALUES(freq), updated_at = " + h.nowFun
	} else {
		stmt += " ON CONFLICT (cognom_id, municipi_id, any_doc) DO UPDATE SET freq = excluded.freq, updated_at = " + h.nowFun
	}
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, cognomID, municipiID, anyDoc, freq)
	return err
}

func (h sqlHelper) applyCognomFreqMunicipiAnyDelta(cognomID, municipiID, anyDoc, delta int) error {
	if cognomID <= 0 || municipiID <= 0 || anyDoc <= 0 {
		return errors.New("invalid ids")
	}
	if delta == 0 {
		return nil
	}
	stmt := fmt.Sprintf(`
        INSERT INTO cognoms_freq_municipi_any (cognom_id, municipi_id, any_doc, freq, updated_at)
        VALUES (?, ?, ?, ?, %s)`, h.nowFun)
	if h.style == "mysql" {
		stmt += " ON DUPLICATE KEY UPDATE freq = freq + VALUES(freq), updated_at = " + h.nowFun
	} else {
		stmt += " ON CONFLICT (cognom_id, municipi_id, any_doc) DO UPDATE SET freq = cognoms_freq_municipi_any.freq + excluded.freq, updated_at = " + h.nowFun
	}
	stmt = formatPlaceholders(h.style, stmt)
	if _, err := h.db.Exec(stmt, cognomID, municipiID, anyDoc, delta); err != nil {
		return err
	}
	cleanup := formatPlaceholders(h.style, `DELETE FROM cognoms_freq_municipi_any WHERE cognom_id = ? AND municipi_id = ? AND any_doc = ? AND freq <= 0`)
	if res, err := h.db.Exec(cleanup, cognomID, municipiID, anyDoc); err == nil && delta < 0 {
		if rows, err := res.RowsAffected(); err == nil && rows > 0 {
			logInfof("clamp cognoms_freq_municipi_any cognom=%d municipi=%d any=%d delta=%d", cognomID, municipiID, anyDoc, delta)
		}
	}
	return nil
}

func (h sqlHelper) upsertNomFreqMunicipiAny(nomID, municipiID, anyDoc, delta int) error {
	if nomID <= 0 || municipiID <= 0 || anyDoc <= 0 {
		return errors.New("invalid ids")
	}
	if delta == 0 {
		return nil
	}
	stmt := fmt.Sprintf(`
        INSERT INTO noms_freq_municipi_any (nom_id, municipi_id, any_doc, freq, updated_at)
        VALUES (?, ?, ?, ?, %s)`, h.nowFun)
	if h.style == "mysql" {
		stmt += " ON DUPLICATE KEY UPDATE freq = freq + VALUES(freq), updated_at = " + h.nowFun
	} else {
		stmt += " ON CONFLICT (nom_id, municipi_id, any_doc) DO UPDATE SET freq = noms_freq_municipi_any.freq + excluded.freq, updated_at = " + h.nowFun
	}
	stmt = formatPlaceholders(h.style, stmt)
	if _, err := h.db.Exec(stmt, nomID, municipiID, anyDoc, delta); err != nil {
		return err
	}
	cleanup := formatPlaceholders(h.style, `DELETE FROM noms_freq_municipi_any WHERE nom_id = ? AND municipi_id = ? AND any_doc = ? AND freq <= 0`)
	if res, err := h.db.Exec(cleanup, nomID, municipiID, anyDoc); err == nil && delta < 0 {
		if rows, err := res.RowsAffected(); err == nil && rows > 0 {
			logInfof("clamp noms_freq_municipi_any nom=%d municipi=%d any=%d delta=%d", nomID, municipiID, anyDoc, delta)
		}
	}
	return nil
}

func (h sqlHelper) upsertNomFreqMunicipiTotal(nomID, municipiID, delta int) error {
	if nomID <= 0 || municipiID <= 0 {
		return errors.New("invalid ids")
	}
	if delta == 0 {
		return nil
	}
	stmt := fmt.Sprintf(`
        INSERT INTO noms_freq_municipi_total (nom_id, municipi_id, total_freq, updated_at)
        VALUES (?, ?, ?, %s)`, h.nowFun)
	if h.style == "mysql" {
		stmt += " ON DUPLICATE KEY UPDATE total_freq = total_freq + VALUES(total_freq), updated_at = " + h.nowFun
	} else {
		stmt += " ON CONFLICT (nom_id, municipi_id) DO UPDATE SET total_freq = noms_freq_municipi_total.total_freq + excluded.total_freq, updated_at = " + h.nowFun
	}
	stmt = formatPlaceholders(h.style, stmt)
	if _, err := h.db.Exec(stmt, nomID, municipiID, delta); err != nil {
		return err
	}
	cleanup := formatPlaceholders(h.style, `DELETE FROM noms_freq_municipi_total WHERE nom_id = ? AND municipi_id = ? AND total_freq <= 0`)
	if res, err := h.db.Exec(cleanup, nomID, municipiID); err == nil && delta < 0 {
		if rows, err := res.RowsAffected(); err == nil && rows > 0 {
			logInfof("clamp noms_freq_municipi_total nom=%d municipi=%d delta=%d", nomID, municipiID, delta)
		}
	}
	return nil
}

func (h sqlHelper) upsertCognomFreqMunicipiTotal(cognomID, municipiID, delta int) error {
	if cognomID <= 0 || municipiID <= 0 {
		return errors.New("invalid ids")
	}
	if delta == 0 {
		return nil
	}
	stmt := fmt.Sprintf(`
        INSERT INTO cognoms_freq_municipi_total (cognom_id, municipi_id, total_freq, updated_at)
        VALUES (?, ?, ?, %s)`, h.nowFun)
	if h.style == "mysql" {
		stmt += " ON DUPLICATE KEY UPDATE total_freq = total_freq + VALUES(total_freq), updated_at = " + h.nowFun
	} else {
		stmt += " ON CONFLICT (cognom_id, municipi_id) DO UPDATE SET total_freq = cognoms_freq_municipi_total.total_freq + excluded.total_freq, updated_at = " + h.nowFun
	}
	stmt = formatPlaceholders(h.style, stmt)
	if _, err := h.db.Exec(stmt, cognomID, municipiID, delta); err != nil {
		return err
	}
	cleanup := formatPlaceholders(h.style, `DELETE FROM cognoms_freq_municipi_total WHERE cognom_id = ? AND municipi_id = ? AND total_freq <= 0`)
	if res, err := h.db.Exec(cleanup, cognomID, municipiID); err == nil && delta < 0 {
		if rows, err := res.RowsAffected(); err == nil && rows > 0 {
			logInfof("clamp cognoms_freq_municipi_total cognom=%d municipi=%d delta=%d", cognomID, municipiID, delta)
		}
	}
	return nil
}

func (h sqlHelper) listTopNomsByMunicipi(municipiID, limit int) ([]NomTotalRow, error) {
	if municipiID <= 0 {
		return nil, errors.New("municipi_id invalid")
	}
	query := `
        SELECT n.id, t.municipi_id, t.total_freq, n.forma
        FROM noms_freq_municipi_total t
        JOIN noms n ON n.id = t.nom_id
        WHERE t.municipi_id = ?
        ORDER BY t.total_freq DESC, n.forma`
	args := []interface{}{municipiID}
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
	var res []NomTotalRow
	for rows.Next() {
		var row NomTotalRow
		if err := rows.Scan(&row.NomID, &row.MunicipiID, &row.TotalFreq, &row.Forma); err != nil {
			return nil, err
		}
		res = append(res, row)
	}
	return res, nil
}

func (h sqlHelper) listTopCognomsByMunicipi(municipiID, limit int) ([]CognomTotalRow, error) {
	if municipiID <= 0 {
		return nil, errors.New("municipi_id invalid")
	}
	query := `
        SELECT c.id, t.municipi_id, t.total_freq, c.forma
        FROM cognoms_freq_municipi_total t
        JOIN cognoms c ON c.id = t.cognom_id
        WHERE t.municipi_id = ?
        ORDER BY t.total_freq DESC, c.forma`
	args := []interface{}{municipiID}
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
	var res []CognomTotalRow
	for rows.Next() {
		var row CognomTotalRow
		if err := rows.Scan(&row.CognomID, &row.MunicipiID, &row.TotalFreq, &row.Forma); err != nil {
			return nil, err
		}
		res = append(res, row)
	}
	return res, nil
}

func (h sqlHelper) listNomSeries(municipiID, nomID int, bucket string) ([]NomFreqRow, error) {
	if municipiID <= 0 || nomID <= 0 {
		return nil, errors.New("invalid ids")
	}
	if bucket == "decade" {
		query := `
        SELECT (any_doc - (any_doc % 10)) AS decade, SUM(freq) AS freq
        FROM noms_freq_municipi_any
        WHERE municipi_id = ? AND nom_id = ?
        GROUP BY decade
        ORDER BY decade`
		query = formatPlaceholders(h.style, query)
		rows, err := h.db.Query(query, municipiID, nomID)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var res []NomFreqRow
		for rows.Next() {
			var decade int
			var freq int
			if err := rows.Scan(&decade, &freq); err != nil {
				return nil, err
			}
			res = append(res, NomFreqRow{
				NomID:      nomID,
				MunicipiID: municipiID,
				AnyDoc:     decade,
				Freq:       freq,
			})
		}
		return res, nil
	}
	query := `
        SELECT any_doc, freq
        FROM noms_freq_municipi_any
        WHERE municipi_id = ? AND nom_id = ?
        ORDER BY any_doc`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, municipiID, nomID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []NomFreqRow
	for rows.Next() {
		var year int
		var freq int
		if err := rows.Scan(&year, &freq); err != nil {
			return nil, err
		}
		res = append(res, NomFreqRow{
			NomID:      nomID,
			MunicipiID: municipiID,
			AnyDoc:     year,
			Freq:       freq,
		})
	}
	return res, nil
}

func (h sqlHelper) listCognomSeries(municipiID, cognomID int, bucket string) ([]CognomFreqRow, error) {
	if municipiID <= 0 || cognomID <= 0 {
		return nil, errors.New("invalid ids")
	}
	if bucket == "decade" {
		query := `
        SELECT (any_doc - (any_doc % 10)) AS decade, SUM(freq) AS freq
        FROM cognoms_freq_municipi_any
        WHERE municipi_id = ? AND cognom_id = ?
        GROUP BY decade
        ORDER BY decade`
		query = formatPlaceholders(h.style, query)
		rows, err := h.db.Query(query, municipiID, cognomID)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var res []CognomFreqRow
		for rows.Next() {
			var decade int
			var freq int
			if err := rows.Scan(&decade, &freq); err != nil {
				return nil, err
			}
			res = append(res, CognomFreqRow{
				MunicipiID: municipiID,
				AnyDoc:     decade,
				Freq:       freq,
			})
		}
		return res, nil
	}
	query := `
        SELECT any_doc, freq
        FROM cognoms_freq_municipi_any
        WHERE municipi_id = ? AND cognom_id = ?
        ORDER BY any_doc`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, municipiID, cognomID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []CognomFreqRow
	for rows.Next() {
		var year int
		var freq int
		if err := rows.Scan(&year, &freq); err != nil {
			return nil, err
		}
		res = append(res, CognomFreqRow{
			MunicipiID: municipiID,
			AnyDoc:     year,
			Freq:       freq,
		})
	}
	return res, nil
}

func (h sqlHelper) countNomTotalsByMunicipi(municipiID int) (int, error) {
	if municipiID <= 0 {
		return 0, errors.New("municipi_id invalid")
	}
	query := `SELECT COUNT(*) FROM noms_freq_municipi_total WHERE municipi_id = ? AND total_freq > 0`
	query = formatPlaceholders(h.style, query)
	var total int
	if err := h.db.QueryRow(query, municipiID).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (h sqlHelper) countCognomTotalsByMunicipi(municipiID int) (int, error) {
	if municipiID <= 0 {
		return 0, errors.New("municipi_id invalid")
	}
	query := `SELECT COUNT(*) FROM cognoms_freq_municipi_total WHERE municipi_id = ? AND total_freq > 0`
	query = formatPlaceholders(h.style, query)
	var total int
	if err := h.db.QueryRow(query, municipiID).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (h sqlHelper) clearNomCognomStatsByMunicipi(municipiID int) error {
	if municipiID <= 0 {
		return errors.New("municipi_id invalid")
	}
	queries := []string{
		`DELETE FROM noms_freq_municipi_any WHERE municipi_id = ?`,
		`DELETE FROM noms_freq_municipi_total WHERE municipi_id = ?`,
		`DELETE FROM cognoms_freq_municipi_any WHERE municipi_id = ?`,
		`DELETE FROM cognoms_freq_municipi_total WHERE municipi_id = ?`,
	}
	for _, query := range queries {
		stmt := formatPlaceholders(h.style, query)
		if _, err := h.db.Exec(stmt, municipiID); err != nil {
			return err
		}
	}
	return nil
}

func (h sqlHelper) queryCognomHeatmap(cognomID int, anyStart, anyEnd int) ([]CognomFreqRow, error) {
	query := `
        SELECT c.municipi_id, m.nom, m.latitud, m.longitud, 0, SUM(c.freq) as freq
        FROM cognoms_freq_municipi_any c
        JOIN municipis m ON m.id = c.municipi_id
        WHERE c.cognom_id = ?`
	args := []interface{}{cognomID}
	if anyStart > 0 {
		query += " AND c.any_doc >= ?"
		args = append(args, anyStart)
	}
	if anyEnd > 0 {
		query += " AND c.any_doc <= ?"
		args = append(args, anyEnd)
	}
	query += " GROUP BY c.municipi_id, m.nom, m.latitud, m.longitud"
	query += " ORDER BY m.nom"
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []CognomFreqRow
	for rows.Next() {
		var r CognomFreqRow
		if err := rows.Scan(&r.MunicipiID, &r.MunicipiNom, &r.Latitud, &r.Longitud, &r.AnyDoc, &r.Freq); err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}

// Media
func (h sqlHelper) listMediaAlbumsByOwner(userID int) ([]MediaAlbum, error) {
	query := `
        SELECT a.id, a.public_id, a.title, COALESCE(a.description, ''), a.album_type, a.owner_user_id,
               a.moderation_status, a.visibility, a.restricted_group_id, a.access_policy_id,
               a.credit_cost, a.difficulty_score, COALESCE(a.source_type, ''), a.moderated_by, a.moderated_at,
               COALESCE(a.moderation_notes, ''), COALESCE(cnt.total, 0) as items_count
        FROM media_albums a
        LEFT JOIN (
            SELECT album_id, COUNT(*) as total FROM media_items GROUP BY album_id
        ) cnt ON cnt.album_id = a.id
        WHERE a.owner_user_id = ?
        ORDER BY a.created_at DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []MediaAlbum
	for rows.Next() {
		var a MediaAlbum
		if err := rows.Scan(
			&a.ID, &a.PublicID, &a.Title, &a.Description, &a.AlbumType, &a.OwnerUserID,
			&a.ModerationStatus, &a.Visibility, &a.RestrictedGroupID, &a.AccessPolicyID,
			&a.CreditCost, &a.DifficultyScore, &a.SourceType, &a.ModeratedBy, &a.ModeratedAt,
			&a.ModerationNotes, &a.ItemsCount,
		); err != nil {
			return nil, err
		}
		res = append(res, a)
	}
	return res, nil
}

func (h sqlHelper) getMediaAlbumByID(id int) (*MediaAlbum, error) {
	query := `
        SELECT id, public_id, title, COALESCE(description, ''), album_type, owner_user_id,
               moderation_status, visibility, restricted_group_id, access_policy_id,
               credit_cost, difficulty_score, COALESCE(source_type, ''), moderated_by, moderated_at,
               COALESCE(moderation_notes, '')
        FROM media_albums WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, id)
	var a MediaAlbum
	if err := row.Scan(
		&a.ID, &a.PublicID, &a.Title, &a.Description, &a.AlbumType, &a.OwnerUserID,
		&a.ModerationStatus, &a.Visibility, &a.RestrictedGroupID, &a.AccessPolicyID,
		&a.CreditCost, &a.DifficultyScore, &a.SourceType, &a.ModeratedBy, &a.ModeratedAt,
		&a.ModerationNotes,
	); err != nil {
		return nil, err
	}
	return &a, nil
}

func (h sqlHelper) getMediaAlbumByPublicID(publicID string) (*MediaAlbum, error) {
	query := `
        SELECT id, public_id, title, COALESCE(description, ''), album_type, owner_user_id,
               moderation_status, visibility, restricted_group_id, access_policy_id,
               credit_cost, difficulty_score, COALESCE(source_type, ''), moderated_by, moderated_at,
               COALESCE(moderation_notes, '')
        FROM media_albums WHERE public_id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, publicID)
	var a MediaAlbum
	if err := row.Scan(
		&a.ID, &a.PublicID, &a.Title, &a.Description, &a.AlbumType, &a.OwnerUserID,
		&a.ModerationStatus, &a.Visibility, &a.RestrictedGroupID, &a.AccessPolicyID,
		&a.CreditCost, &a.DifficultyScore, &a.SourceType, &a.ModeratedBy, &a.ModeratedAt,
		&a.ModerationNotes,
	); err != nil {
		return nil, err
	}
	return &a, nil
}

func (h sqlHelper) createMediaAlbum(a *MediaAlbum) (int, error) {
	query := `
        INSERT INTO media_albums
            (public_id, title, description, album_type, owner_user_id, moderation_status, visibility, restricted_group_id, access_policy_id,
             credit_cost, difficulty_score, source_type, moderated_by, moderated_at, moderation_notes, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		query += " RETURNING id"
	}
	query = formatPlaceholders(h.style, query)
	args := []interface{}{
		a.PublicID, a.Title, a.Description, a.AlbumType, a.OwnerUserID, a.ModerationStatus, a.Visibility, a.RestrictedGroupID, a.AccessPolicyID,
		a.CreditCost, a.DifficultyScore, a.SourceType, a.ModeratedBy, a.ModeratedAt, a.ModerationNotes,
	}
	if h.style == "postgres" {
		if err := h.db.QueryRow(query, args...).Scan(&a.ID); err != nil {
			return 0, err
		}
		return a.ID, nil
	}
	res, err := h.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		a.ID = int(id)
	}
	return a.ID, nil
}

func (h sqlHelper) listMediaItemsByAlbum(albumID int) ([]MediaItem, error) {
	query := `
        SELECT id, public_id, album_id, COALESCE(title, ''), COALESCE(original_filename, ''), COALESCE(mime_type, ''),
               COALESCE(byte_size, 0), COALESCE(width, 0), COALESCE(height, 0), COALESCE(checksum_sha256, ''),
               storage_key_original, COALESCE(thumb_path, ''), derivatives_status, moderation_status,
               moderated_by, moderated_at, COALESCE(moderation_notes, ''), credit_cost
        FROM media_items WHERE album_id = ?
        ORDER BY id ASC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, albumID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []MediaItem
	for rows.Next() {
		var item MediaItem
		if err := rows.Scan(
			&item.ID, &item.PublicID, &item.AlbumID, &item.Title, &item.OriginalFilename, &item.MimeType,
			&item.ByteSize, &item.Width, &item.Height, &item.ChecksumSHA256,
			&item.StorageKeyOriginal, &item.ThumbPath, &item.DerivativesStatus, &item.ModerationStatus,
			&item.ModeratedBy, &item.ModeratedAt, &item.ModerationNotes, &item.CreditCost,
		); err != nil {
			return nil, err
		}
		res = append(res, item)
	}
	return res, nil
}

func (h sqlHelper) listMediaItemsByAlbumType(albumType, status string) ([]MediaItem, error) {
	albumType = strings.TrimSpace(albumType)
	if albumType == "" {
		return []MediaItem{}, nil
	}
	query := `
        SELECT mi.id, mi.public_id, mi.album_id, COALESCE(mi.title, ''), COALESCE(mi.original_filename, ''), COALESCE(mi.mime_type, ''),
               COALESCE(mi.byte_size, 0), COALESCE(mi.width, 0), COALESCE(mi.height, 0), COALESCE(mi.checksum_sha256, ''),
               mi.storage_key_original, COALESCE(mi.thumb_path, ''), mi.derivatives_status, mi.moderation_status,
               mi.moderated_by, mi.moderated_at, COALESCE(mi.moderation_notes, ''), mi.credit_cost
        FROM media_items mi
        INNER JOIN media_albums a ON a.id = mi.album_id
        WHERE a.album_type = ?`
	args := []interface{}{albumType}
	status = strings.TrimSpace(status)
	if status != "" {
		query += " AND mi.moderation_status = ?"
		args = append(args, status)
	}
	query += " ORDER BY mi.id ASC"
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []MediaItem
	for rows.Next() {
		var item MediaItem
		if err := rows.Scan(
			&item.ID, &item.PublicID, &item.AlbumID, &item.Title, &item.OriginalFilename, &item.MimeType,
			&item.ByteSize, &item.Width, &item.Height, &item.ChecksumSHA256,
			&item.StorageKeyOriginal, &item.ThumbPath, &item.DerivativesStatus, &item.ModerationStatus,
			&item.ModeratedBy, &item.ModeratedAt, &item.ModerationNotes, &item.CreditCost,
		); err != nil {
			return nil, err
		}
		res = append(res, item)
	}
	return res, nil
}

func (h sqlHelper) getMediaItemByID(id int) (*MediaItem, error) {
	query := `
        SELECT id, public_id, album_id, COALESCE(title, ''), COALESCE(original_filename, ''), COALESCE(mime_type, ''),
               COALESCE(byte_size, 0), COALESCE(width, 0), COALESCE(height, 0), COALESCE(checksum_sha256, ''),
               storage_key_original, COALESCE(thumb_path, ''), derivatives_status, moderation_status,
               moderated_by, moderated_at, COALESCE(moderation_notes, ''), credit_cost
        FROM media_items WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, id)
	var item MediaItem
	if err := row.Scan(
		&item.ID, &item.PublicID, &item.AlbumID, &item.Title, &item.OriginalFilename, &item.MimeType,
		&item.ByteSize, &item.Width, &item.Height, &item.ChecksumSHA256,
		&item.StorageKeyOriginal, &item.ThumbPath, &item.DerivativesStatus, &item.ModerationStatus,
		&item.ModeratedBy, &item.ModeratedAt, &item.ModerationNotes, &item.CreditCost,
	); err != nil {
		return nil, err
	}
	return &item, nil
}

func (h sqlHelper) getMediaItemByPublicID(publicID string) (*MediaItem, error) {
	query := `
        SELECT id, public_id, album_id, COALESCE(title, ''), COALESCE(original_filename, ''), COALESCE(mime_type, ''),
               COALESCE(byte_size, 0), COALESCE(width, 0), COALESCE(height, 0), COALESCE(checksum_sha256, ''),
               storage_key_original, COALESCE(thumb_path, ''), derivatives_status, moderation_status,
               moderated_by, moderated_at, COALESCE(moderation_notes, ''), credit_cost
        FROM media_items WHERE public_id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, publicID)
	var item MediaItem
	if err := row.Scan(
		&item.ID, &item.PublicID, &item.AlbumID, &item.Title, &item.OriginalFilename, &item.MimeType,
		&item.ByteSize, &item.Width, &item.Height, &item.ChecksumSHA256,
		&item.StorageKeyOriginal, &item.ThumbPath, &item.DerivativesStatus, &item.ModerationStatus,
		&item.ModeratedBy, &item.ModeratedAt, &item.ModerationNotes, &item.CreditCost,
	); err != nil {
		return nil, err
	}
	return &item, nil
}

func (h sqlHelper) createMediaItem(item *MediaItem) (int, error) {
	if item.ModerationStatus == "" {
		item.ModerationStatus = "pending"
	}
	query := `
        INSERT INTO media_items
            (public_id, album_id, title, original_filename, mime_type, byte_size, width, height, checksum_sha256,
             storage_key_original, thumb_path, derivatives_status, moderation_status, moderated_by, moderated_at,
             moderation_notes, credit_cost, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		query += " RETURNING id"
	}
	query = formatPlaceholders(h.style, query)
	args := []interface{}{
		item.PublicID, item.AlbumID, item.Title, item.OriginalFilename, item.MimeType, item.ByteSize, item.Width, item.Height, item.ChecksumSHA256,
		item.StorageKeyOriginal, item.ThumbPath, item.DerivativesStatus, item.ModerationStatus, item.ModeratedBy, item.ModeratedAt,
		item.ModerationNotes, item.CreditCost,
	}
	if h.style == "postgres" {
		if err := h.db.QueryRow(query, args...).Scan(&item.ID); err != nil {
			return 0, err
		}
		return item.ID, nil
	}
	res, err := h.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		item.ID = int(id)
	}
	return item.ID, nil
}

func (h sqlHelper) updateMediaItemDerivativesStatus(itemID int, status string) error {
	stmt := `UPDATE media_items SET derivatives_status = ?, updated_at = ` + h.nowFun + ` WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, status, itemID)
	return err
}

func (h sqlHelper) listMediaAlbumsByStatus(status string) ([]MediaAlbum, error) {
	query := `
        SELECT a.id, a.public_id, a.title, COALESCE(a.description, ''), a.album_type, a.owner_user_id,
               a.moderation_status, a.visibility, a.restricted_group_id, a.access_policy_id,
               a.credit_cost, a.difficulty_score, COALESCE(a.source_type, ''), a.moderated_by, a.moderated_at,
               COALESCE(a.moderation_notes, ''), COALESCE(cnt.total, 0) as items_count
        FROM media_albums a
        LEFT JOIN (
            SELECT album_id, COUNT(*) as total FROM media_items GROUP BY album_id
        ) cnt ON cnt.album_id = a.id
        WHERE a.moderation_status = ?
        ORDER BY a.created_at DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []MediaAlbum
	for rows.Next() {
		var a MediaAlbum
		if err := rows.Scan(
			&a.ID, &a.PublicID, &a.Title, &a.Description, &a.AlbumType, &a.OwnerUserID,
			&a.ModerationStatus, &a.Visibility, &a.RestrictedGroupID, &a.AccessPolicyID,
			&a.CreditCost, &a.DifficultyScore, &a.SourceType, &a.ModeratedBy, &a.ModeratedAt,
			&a.ModerationNotes, &a.ItemsCount,
		); err != nil {
			return nil, err
		}
		res = append(res, a)
	}
	return res, nil
}

func (h sqlHelper) listMediaItemsByStatus(status string) ([]MediaItem, error) {
	query := `
        SELECT id, public_id, album_id, COALESCE(title, ''), COALESCE(original_filename, ''), COALESCE(mime_type, ''),
               COALESCE(byte_size, 0), COALESCE(width, 0), COALESCE(height, 0), COALESCE(checksum_sha256, ''),
               storage_key_original, COALESCE(thumb_path, ''), derivatives_status, moderation_status,
               moderated_by, moderated_at, COALESCE(moderation_notes, ''), credit_cost
        FROM media_items
        WHERE moderation_status = ?
        ORDER BY created_at DESC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []MediaItem
	for rows.Next() {
		var item MediaItem
		if err := rows.Scan(
			&item.ID, &item.PublicID, &item.AlbumID, &item.Title, &item.OriginalFilename, &item.MimeType,
			&item.ByteSize, &item.Width, &item.Height, &item.ChecksumSHA256,
			&item.StorageKeyOriginal, &item.ThumbPath, &item.DerivativesStatus, &item.ModerationStatus,
			&item.ModeratedBy, &item.ModeratedAt, &item.ModerationNotes, &item.CreditCost,
		); err != nil {
			return nil, err
		}
		res = append(res, item)
	}
	return res, nil
}

func (h sqlHelper) updateMediaAlbumModeration(id int, status, visibility string, restrictedGroupID, accessPolicyID, creditCost, difficultyScore int, sourceType, notes string, moderatorID int) error {
	restricted := sql.NullInt64{Int64: int64(restrictedGroupID), Valid: restrictedGroupID > 0}
	accessPolicy := sql.NullInt64{Int64: int64(accessPolicyID), Valid: accessPolicyID > 0}
	moderatedBy := sql.NullInt64{Int64: int64(moderatorID), Valid: moderatorID > 0}
	stmt := `UPDATE media_albums
        SET moderation_status = ?, visibility = ?, restricted_group_id = ?, access_policy_id = ?, credit_cost = ?,
            difficulty_score = ?, source_type = ?, moderation_notes = ?, moderated_by = ?, moderated_at = ` + h.nowFun + `,
            updated_at = ` + h.nowFun + `
        WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, status, visibility, restricted, accessPolicy, creditCost, difficultyScore, sourceType, notes, moderatedBy, id)
	return err
}

func (h sqlHelper) updateMediaItemModeration(id int, status string, creditCost int, notes string, moderatorID int) error {
	moderatedBy := sql.NullInt64{Int64: int64(moderatorID), Valid: moderatorID > 0}
	stmt := `UPDATE media_items
        SET moderation_status = ?, credit_cost = ?, moderation_notes = ?, moderated_by = ?, moderated_at = ` + h.nowFun + `,
            updated_at = ` + h.nowFun + `
        WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, status, creditCost, notes, moderatedBy, id)
	return err
}

func (h sqlHelper) listMediaItemLinksByPagina(paginaID int) ([]MediaItemPageLink, error) {
	query := `
        SELECT mp.id, mp.media_item_id, mp.page_order, COALESCE(mp.notes, ''),
               i.public_id, COALESCE(i.title, ''), COALESCE(i.thumb_path, ''), i.moderation_status,
               a.id, a.public_id, COALESCE(a.title, ''), a.owner_user_id, a.moderation_status,
               a.visibility, a.restricted_group_id, a.access_policy_id
        FROM media_item_pages mp
        JOIN media_items i ON i.id = mp.media_item_id
        JOIN media_albums a ON a.id = i.album_id
        WHERE mp.pagina_id = ?
        ORDER BY mp.page_order ASC, mp.id ASC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, paginaID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []MediaItemPageLink
	for rows.Next() {
		var row MediaItemPageLink
		if err := rows.Scan(
			&row.ID, &row.MediaItemID, &row.PageOrder, &row.Notes,
			&row.MediaItemPublicID, &row.MediaItemTitle, &row.MediaItemThumbPath, &row.MediaItemStatus,
			&row.AlbumID, &row.AlbumPublicID, &row.AlbumTitle, &row.AlbumOwnerUserID, &row.AlbumModerationStatus,
			&row.AlbumVisibility, &row.AlbumRestrictedGroupID, &row.AlbumAccessPolicyID,
		); err != nil {
			return nil, err
		}
		res = append(res, row)
	}
	return res, nil
}

func (h sqlHelper) upsertMediaItemPageLink(mediaItemID, llibreID, paginaID, pageOrder int, notes string) error {
	stmt := `
        INSERT INTO media_item_pages (media_item_id, llibre_id, pagina_id, page_order, notes)
        VALUES (?, ?, ?, ?, ?)`
	if h.style == "mysql" {
		stmt += " ON DUPLICATE KEY UPDATE page_order = VALUES(page_order), notes = VALUES(notes)"
	} else {
		stmt += " ON CONFLICT (media_item_id, pagina_id) DO UPDATE SET page_order = excluded.page_order, notes = excluded.notes"
	}
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, mediaItemID, llibreID, paginaID, pageOrder, notes)
	return err
}

func (h sqlHelper) deleteMediaItemPageLink(mediaItemID, paginaID int) error {
	stmt := `DELETE FROM media_item_pages WHERE media_item_id = ? AND pagina_id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, mediaItemID, paginaID)
	return err
}

func (h sqlHelper) countMediaItemLinksByAlbum(albumID int) (map[int]int, error) {
	query := `
        SELECT mp.media_item_id, COUNT(*)
        FROM media_item_pages mp
        JOIN media_items i ON i.id = mp.media_item_id
        WHERE i.album_id = ?
        GROUP BY mp.media_item_id`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, albumID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := map[int]int{}
	for rows.Next() {
		var itemID int
		var count int
		if err := rows.Scan(&itemID, &count); err != nil {
			return nil, err
		}
		res[itemID] = count
	}
	return res, nil
}

func (h sqlHelper) searchMediaItems(query string, limit int) ([]MediaItemSearchRow, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return []MediaItemSearchRow{}, nil
	}
	search := "%" + strings.ToLower(query) + "%"
	stmt := `
        SELECT i.id, i.public_id, COALESCE(i.title, ''), COALESCE(i.thumb_path, ''), i.moderation_status,
               a.id, a.public_id, COALESCE(a.title, ''), a.owner_user_id, a.moderation_status,
               a.visibility, a.restricted_group_id, a.access_policy_id
        FROM media_items i
        JOIN media_albums a ON a.id = i.album_id
        WHERE LOWER(COALESCE(i.title, '')) LIKE ?
           OR LOWER(COALESCE(i.original_filename, '')) LIKE ?
           OR LOWER(COALESCE(a.title, '')) LIKE ?
           OR i.public_id = ?
        ORDER BY a.title ASC, i.title ASC`
	args := []interface{}{search, search, search, query}
	if limit > 0 {
		stmt += " LIMIT ?"
		args = append(args, limit)
	}
	stmt = formatPlaceholders(h.style, stmt)
	rows, err := h.db.Query(stmt, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []MediaItemSearchRow
	for rows.Next() {
		var row MediaItemSearchRow
		if err := rows.Scan(
			&row.MediaItemID, &row.MediaItemPublicID, &row.MediaItemTitle, &row.MediaItemThumb, &row.MediaItemStatus,
			&row.AlbumID, &row.AlbumPublicID, &row.AlbumTitle, &row.AlbumOwnerUserID, &row.AlbumStatus,
			&row.AlbumVisibility, &row.AlbumRestrictedGroupID, &row.AlbumAccessPolicyID,
		); err != nil {
			return nil, err
		}
		res = append(res, row)
	}
	return res, nil
}

func (h sqlHelper) getUserCreditsBalance(userID int) (int, error) {
	query := `SELECT COALESCE(SUM(delta), 0) FROM user_credits_ledger WHERE user_id = ?`
	row := h.db.QueryRow(formatPlaceholders(h.style, query), userID)
	var total int64
	if err := row.Scan(&total); err != nil {
		return 0, err
	}
	return int(total), nil
}

func (h sqlHelper) insertUserCreditsLedger(entry *UserCreditsLedgerEntry) (int, error) {
	if entry == nil {
		return 0, errors.New("entry nil")
	}
	stmt := `INSERT INTO user_credits_ledger (user_id, delta, reason, ref_type, ref_id, created_at)
	         VALUES (?, ?, ?, ?, ?, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	if h.style == "postgres" {
		var id int
		if err := h.db.QueryRow(stmt, entry.UserID, entry.Delta, entry.Reason, entry.RefType, entry.RefID).Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}
	res, err := h.db.Exec(stmt, entry.UserID, entry.Delta, entry.Reason, entry.RefType, entry.RefID)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		return int(id), nil
	}
	return 0, nil
}

func (h sqlHelper) getActiveMediaAccessGrant(userID, mediaItemID int) (*MediaAccessGrant, error) {
	query := `SELECT id, user_id, media_item_id, grant_token, expires_at, credits_spent, created_at
	          FROM media_access_grants
	          WHERE user_id = ? AND media_item_id = ? AND expires_at > ` + h.nowFun + `
	          ORDER BY expires_at DESC
	          LIMIT 1`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, userID, mediaItemID)
	var g MediaAccessGrant
	if err := row.Scan(&g.ID, &g.UserID, &g.MediaItemID, &g.GrantToken, &g.ExpiresAt, &g.CreditsSpent, &g.CreatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &g, nil
}

func (h sqlHelper) getMediaAccessGrantByToken(token string) (*MediaAccessGrant, error) {
	query := `SELECT id, user_id, media_item_id, grant_token, expires_at, credits_spent, created_at
	          FROM media_access_grants
	          WHERE grant_token = ? AND expires_at > ` + h.nowFun + `
	          LIMIT 1`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, token)
	var g MediaAccessGrant
	if err := row.Scan(&g.ID, &g.UserID, &g.MediaItemID, &g.GrantToken, &g.ExpiresAt, &g.CreditsSpent, &g.CreatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &g, nil
}

func (h sqlHelper) createMediaAccessGrant(grant *MediaAccessGrant) (int, error) {
	if grant == nil {
		return 0, errors.New("grant nil")
	}
	stmt := `INSERT INTO media_access_grants (user_id, media_item_id, grant_token, expires_at, credits_spent, created_at)
	         VALUES (?, ?, ?, ?, ?, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	if h.style == "postgres" {
		var id int
		if err := h.db.QueryRow(stmt, grant.UserID, grant.MediaItemID, grant.GrantToken, grant.ExpiresAt, grant.CreditsSpent).Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}
	res, err := h.db.Exec(stmt, grant.UserID, grant.MediaItemID, grant.GrantToken, grant.ExpiresAt, grant.CreditsSpent)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		return int(id), nil
	}
	return 0, nil
}

func (h sqlHelper) insertMediaAccessLog(entry *MediaAccessLog) (int, error) {
	if entry == nil {
		return 0, errors.New("entry nil")
	}
	stmt := `INSERT INTO media_access_logs (user_id, media_item_id, access_type, credits_spent, created_at)
	         VALUES (?, ?, ?, ?, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	if h.style == "postgres" {
		var id int
		if err := h.db.QueryRow(stmt, entry.UserID, entry.MediaItemID, entry.AccessType, entry.CreditsSpent).Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}
	res, err := h.db.Exec(stmt, entry.UserID, entry.MediaItemID, entry.AccessType, entry.CreditsSpent)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		return int(id), nil
	}
	return 0, nil
}

// Mapes municipi
func (h sqlHelper) listMunicipiMapes(filter MunicipiMapaFilter) ([]MunicipiMapa, error) {
	args := []interface{}{}
	clauses := []string{"1=1"}
	if filter.MunicipiID > 0 {
		clauses = append(clauses, "municipi_id = ?")
		args = append(args, filter.MunicipiID)
	}
	if strings.TrimSpace(filter.GroupType) != "" {
		clauses = append(clauses, "group_type = ?")
		args = append(args, strings.TrimSpace(filter.GroupType))
	}
	if filter.CreatedBy > 0 {
		clauses = append(clauses, "created_by = ?")
		args = append(args, filter.CreatedBy)
	}
	query := `
        SELECT id, municipi_id, group_type, title, period_label, period_start, period_end, topic,
               current_version_id, created_by, created_at, updated_at
        FROM municipi_mapes
        WHERE ` + strings.Join(clauses, " AND ") + `
        ORDER BY updated_at DESC, id DESC`
	if filter.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, filter.Limit)
	}
	if filter.Offset > 0 {
		query += " OFFSET ?"
		args = append(args, filter.Offset)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []MunicipiMapa
	for rows.Next() {
		var item MunicipiMapa
		var periodLabel sql.NullString
		var periodStart sql.NullInt64
		var periodEnd sql.NullInt64
		var topic sql.NullString
		if err := rows.Scan(&item.ID, &item.MunicipiID, &item.GroupType, &item.Title, &periodLabel, &periodStart, &periodEnd, &topic,
			&item.CurrentVersionID, &item.CreatedBy, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		item.PeriodLabel = periodLabel.String
		item.PeriodStart = periodStart
		item.PeriodEnd = periodEnd
		item.Topic = topic.String
		res = append(res, item)
	}
	return res, nil
}

func (h sqlHelper) getMunicipiMapa(id int) (*MunicipiMapa, error) {
	query := `
        SELECT id, municipi_id, group_type, title, period_label, period_start, period_end, topic,
               current_version_id, created_by, created_at, updated_at
        FROM municipi_mapes
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, id)
	var item MunicipiMapa
	var periodLabel sql.NullString
	var periodStart sql.NullInt64
	var periodEnd sql.NullInt64
	var topic sql.NullString
	if err := row.Scan(&item.ID, &item.MunicipiID, &item.GroupType, &item.Title, &periodLabel, &periodStart, &periodEnd, &topic,
		&item.CurrentVersionID, &item.CreatedBy, &item.CreatedAt, &item.UpdatedAt); err != nil {
		return nil, err
	}
	item.PeriodLabel = periodLabel.String
	item.PeriodStart = periodStart
	item.PeriodEnd = periodEnd
	item.Topic = topic.String
	return &item, nil
}

func (h sqlHelper) createMunicipiMapa(m *MunicipiMapa) (int, error) {
	if m == nil {
		return 0, errors.New("mapa nil")
	}
	args := []interface{}{m.MunicipiID, m.GroupType, m.Title, m.PeriodLabel, m.PeriodStart, m.PeriodEnd, m.Topic, m.CurrentVersionID, m.CreatedBy}
	query := `
        INSERT INTO municipi_mapes (municipi_id, group_type, title, period_label, period_start, period_end, topic,
                                    current_version_id, created_by, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		query += ` RETURNING id`
	}
	query = formatPlaceholders(h.style, query)
	if h.style == "postgres" {
		if err := h.db.QueryRow(query, args...).Scan(&m.ID); err != nil {
			return 0, err
		}
		return m.ID, nil
	}
	res, err := h.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		m.ID = int(id)
	}
	return m.ID, nil
}

func (h sqlHelper) updateMunicipiMapa(m *MunicipiMapa) error {
	if m == nil {
		return errors.New("mapa nil")
	}
	query := `
        UPDATE municipi_mapes
        SET group_type = ?, title = ?, period_label = ?, period_start = ?, period_end = ?, topic = ?, updated_at = ` + h.nowFun + `
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query, m.GroupType, m.Title, m.PeriodLabel, m.PeriodStart, m.PeriodEnd, m.Topic, m.ID)
	return err
}

func (h sqlHelper) updateMunicipiMapaCurrentVersion(mapaID, versionID int) error {
	query := `
        UPDATE municipi_mapes
        SET current_version_id = ?, updated_at = ` + h.nowFun + `
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query, versionID, mapaID)
	return err
}

func (h sqlHelper) nextMunicipiMapaVersionNumber(mapaID int) (int, error) {
	query := `SELECT COALESCE(MAX(version), 0) + 1 FROM municipi_mapa_versions WHERE mapa_id = ?`
	query = formatPlaceholders(h.style, query)
	var next int
	if err := h.db.QueryRow(query, mapaID).Scan(&next); err != nil {
		return 0, err
	}
	return next, nil
}

func (h sqlHelper) listMunicipiMapaVersions(filter MunicipiMapaVersionFilter) ([]MunicipiMapaVersion, error) {
	args := []interface{}{}
	clauses := []string{"1=1"}
	if filter.MapaID > 0 {
		clauses = append(clauses, "mapa_id = ?")
		args = append(args, filter.MapaID)
	}
	if strings.TrimSpace(filter.Status) != "" {
		clauses = append(clauses, "status = ?")
		args = append(args, strings.TrimSpace(filter.Status))
	}
	if filter.CreatedBy > 0 {
		clauses = append(clauses, "created_by = ?")
		args = append(args, filter.CreatedBy)
	}
	query := `
        SELECT id, mapa_id, version, status, data_json, changelog, lock_version,
               created_by, created_at, moderated_by, moderated_at, moderation_notes
        FROM municipi_mapa_versions
        WHERE ` + strings.Join(clauses, " AND ") + `
        ORDER BY version DESC`
	if filter.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, filter.Limit)
	}
	if filter.Offset > 0 {
		query += " OFFSET ?"
		args = append(args, filter.Offset)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []MunicipiMapaVersion
	for rows.Next() {
		var item MunicipiMapaVersion
		if err := rows.Scan(&item.ID, &item.MapaID, &item.Version, &item.Status, &item.JSONData, &item.Changelog, &item.LockVersion,
			&item.CreatedBy, &item.CreatedAt, &item.ModeratedBy, &item.ModeratedAt, &item.ModerationNotes); err != nil {
			return nil, err
		}
		res = append(res, item)
	}
	return res, nil
}

func (h sqlHelper) getMunicipiMapaVersion(id int) (*MunicipiMapaVersion, error) {
	query := `
        SELECT id, mapa_id, version, status, data_json, changelog, lock_version,
               created_by, created_at, moderated_by, moderated_at, moderation_notes
        FROM municipi_mapa_versions
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, id)
	var item MunicipiMapaVersion
	if err := row.Scan(&item.ID, &item.MapaID, &item.Version, &item.Status, &item.JSONData, &item.Changelog, &item.LockVersion,
		&item.CreatedBy, &item.CreatedAt, &item.ModeratedBy, &item.ModeratedAt, &item.ModerationNotes); err != nil {
		return nil, err
	}
	return &item, nil
}

func (h sqlHelper) createMunicipiMapaVersion(v *MunicipiMapaVersion) (int, error) {
	if v == nil {
		return 0, errors.New("version nil")
	}
	query := `
        INSERT INTO municipi_mapa_versions
            (mapa_id, version, status, data_json, changelog, lock_version, created_by, created_at, moderated_by, moderated_at, moderation_notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ?, ?, ?)`
	if h.style == "postgres" {
		query += ` RETURNING id`
	}
	query = formatPlaceholders(h.style, query)
	args := []interface{}{v.MapaID, v.Version, v.Status, v.JSONData, v.Changelog, v.LockVersion, v.CreatedBy, v.ModeratedBy, v.ModeratedAt, v.ModerationNotes}
	if h.style == "postgres" {
		if err := h.db.QueryRow(query, args...).Scan(&v.ID); err != nil {
			return 0, err
		}
		return v.ID, nil
	}
	res, err := h.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		v.ID = int(id)
	}
	return v.ID, nil
}

func (h sqlHelper) saveMunicipiMapaDraft(versionID int, jsonData, changelog string, expectedLock int) (int, error) {
	query := `
        UPDATE municipi_mapa_versions
        SET data_json = ?, changelog = ?, lock_version = lock_version + 1
        WHERE id = ? AND lock_version = ? AND status = 'draft'`
	query = formatPlaceholders(h.style, query)
	res, err := h.db.Exec(query, jsonData, changelog, versionID, expectedLock)
	if err != nil {
		return 0, err
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return 0, ErrConflict
	}
	return expectedLock + 1, nil
}

func (h sqlHelper) updateMunicipiMapaVersionStatus(id int, status, notes string, moderatorID int) error {
	var (
		query string
		args  []interface{}
	)
	if moderatorID > 0 {
		query = `
            UPDATE municipi_mapa_versions
            SET status = ?, moderation_notes = ?, moderated_by = ?, moderated_at = ?
            WHERE id = ?`
		args = []interface{}{status, notes, moderatorID, time.Now(), id}
	} else {
		query = `
            UPDATE municipi_mapa_versions
            SET status = ?, moderation_notes = ?, moderated_by = NULL, moderated_at = NULL
            WHERE id = ?`
		args = []interface{}{status, notes, id}
	}
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query, args...)
	return err
}

func (h sqlHelper) resolveMunicipiIDByMapaID(mapaID int) (int, error) {
	query := formatPlaceholders(h.style, `SELECT municipi_id FROM municipi_mapes WHERE id = ?`)
	var id int
	if err := h.db.QueryRow(query, mapaID).Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

func (h sqlHelper) resolveMunicipiIDByMapaVersionID(versionID int) (int, error) {
	query := `
        SELECT m.municipi_id
        FROM municipi_mapa_versions v
        JOIN municipi_mapes m ON m.id = v.mapa_id
        WHERE v.id = ?`
	query = formatPlaceholders(h.style, query)
	var id int
	if err := h.db.QueryRow(query, versionID).Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

// Historia municipi
func (h sqlHelper) ensureMunicipiHistoria(municipiID int) (*MunicipiHistoria, error) {
	if municipiID <= 0 {
		return nil, errors.New("municipi_id invalid")
	}
	if row, err := h.getMunicipiHistoriaByMunicipiID(municipiID); err == nil && row != nil {
		return row, nil
	} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	query := `
        INSERT INTO municipi_historia (municipi_id, created_at, updated_at)
        VALUES (?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		query += " ON CONFLICT (municipi_id) DO NOTHING"
	} else if h.style == "mysql" {
		query += " ON DUPLICATE KEY UPDATE municipi_id = VALUES(municipi_id)"
	} else {
		query += " ON CONFLICT(municipi_id) DO NOTHING"
	}
	query = formatPlaceholders(h.style, query)
	if _, err := h.db.Exec(query, municipiID); err != nil {
		return nil, err
	}
	return h.getMunicipiHistoriaByMunicipiID(municipiID)
}

func (h sqlHelper) getMunicipiHistoriaByMunicipiID(municipiID int) (*MunicipiHistoria, error) {
	query := `
        SELECT id, municipi_id, current_general_version_id, created_at, updated_at
        FROM municipi_historia
        WHERE municipi_id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, municipiID)
	var item MunicipiHistoria
	if err := row.Scan(&item.ID, &item.MunicipiID, &item.CurrentGeneralVersionID, &item.CreatedAt, &item.UpdatedAt); err != nil {
		return nil, err
	}
	return &item, nil
}

func (h sqlHelper) resolveMunicipiIDByHistoriaGeneralVersionID(versionID int) (int, error) {
	query := `
        SELECT h.municipi_id
        FROM municipi_historia_general_versions v
        JOIN municipi_historia h ON h.id = v.historia_id
        WHERE v.id = ?`
	query = formatPlaceholders(h.style, query)
	var id int
	if err := h.db.QueryRow(query, versionID).Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

func (h sqlHelper) resolveMunicipiIDByHistoriaFetVersionID(versionID int) (int, error) {
	query := `
        SELECT f.municipi_id
        FROM municipi_historia_fet_versions v
        JOIN municipi_historia_fets f ON f.id = v.fet_id
        WHERE v.id = ?`
	query = formatPlaceholders(h.style, query)
	var id int
	if err := h.db.QueryRow(query, versionID).Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

func (h sqlHelper) getMunicipiHistoriaFet(id int) (*MunicipiHistoriaFet, error) {
	if id <= 0 {
		return nil, errors.New("fet_id invalid")
	}
	query := `
        SELECT id, municipi_id, current_version_id, created_at, updated_at
        FROM municipi_historia_fets
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, id)
	var item MunicipiHistoriaFet
	if err := row.Scan(&item.ID, &item.MunicipiID, &item.CurrentVersionID, &item.CreatedAt, &item.UpdatedAt); err != nil {
		return nil, err
	}
	return &item, nil
}

func (h sqlHelper) nextMunicipiHistoriaGeneralVersion(historiaID int) (int, error) {
	query := formatPlaceholders(h.style, `
        SELECT COALESCE(MAX(version), 0) + 1
        FROM municipi_historia_general_versions
        WHERE historia_id = ?`)
	var next int
	if err := h.db.QueryRow(query, historiaID).Scan(&next); err != nil {
		return 0, err
	}
	return next, nil
}

func (h sqlHelper) createMunicipiHistoriaGeneralDraft(historiaID int, createdBy int, baseFromCurrent bool) (int, error) {
	if historiaID <= 0 {
		return 0, errors.New("historia_id invalid")
	}
	version, err := h.nextMunicipiHistoriaGeneralVersion(historiaID)
	if err != nil {
		return 0, err
	}
	titol := ""
	resum := ""
	cosText := ""
	tags := ""
	if baseFromCurrent {
		query := `
            SELECT v.titol, v.resum, v.cos_text, v.tags_json
            FROM municipi_historia h
            JOIN municipi_historia_general_versions v ON v.id = h.current_general_version_id
            WHERE h.id = ?`
		query = formatPlaceholders(h.style, query)
		var titolVal sql.NullString
		var resumVal sql.NullString
		var cosVal sql.NullString
		var tagsVal sql.NullString
		if err := h.db.QueryRow(query, historiaID).Scan(&titolVal, &resumVal, &cosVal, &tagsVal); err == nil {
			titol = titolVal.String
			resum = resumVal.String
			cosText = cosVal.String
			tags = tagsVal.String
		} else if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
	}
	createdByVal := sql.NullInt64{}
	if createdBy > 0 {
		createdByVal = sql.NullInt64{Int64: int64(createdBy), Valid: true}
	}
	titolVal := sql.NullString{String: strings.TrimSpace(titol), Valid: strings.TrimSpace(titol) != ""}
	resumVal := sql.NullString{String: strings.TrimSpace(resum), Valid: strings.TrimSpace(resum) != ""}
	tagsVal := sql.NullString{String: strings.TrimSpace(tags), Valid: strings.TrimSpace(tags) != ""}
	if cosText == "" {
		cosText = ""
	}
	query := `
        INSERT INTO municipi_historia_general_versions
            (historia_id, version, titol, resum, cos_text, tags_json, status, moderation_notes, lock_version, created_by, created_at, updated_at, moderated_by, moderated_at)
        VALUES (?, ?, ?, ?, ?, ?, 'draft', '', 0, ?, ` + h.nowFun + `, ` + h.nowFun + `, NULL, NULL)`
	if h.style == "postgres" {
		query += " RETURNING id"
	}
	query = formatPlaceholders(h.style, query)
	args := []interface{}{historiaID, version, titolVal, resumVal, cosText, tagsVal, createdByVal}
	if h.style == "postgres" {
		var id int
		if err := h.db.QueryRow(query, args...).Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}
	res, err := h.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		return int(id), nil
	}
	return 0, nil
}

func (h sqlHelper) getMunicipiHistoriaGeneralVersion(id int) (*MunicipiHistoriaGeneralVersion, error) {
	query := `
        SELECT id, historia_id, version, titol, resum, cos_text, tags_json, status, moderation_notes,
               lock_version, created_by, created_at, updated_at, moderated_by, moderated_at
        FROM municipi_historia_general_versions
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, id)
	var item MunicipiHistoriaGeneralVersion
	var titol sql.NullString
	var resum sql.NullString
	var tags sql.NullString
	var notes sql.NullString
	if err := row.Scan(&item.ID, &item.HistoriaID, &item.Version, &titol, &resum, &item.CosText, &tags, &item.Status, &notes,
		&item.LockVersion, &item.CreatedBy, &item.CreatedAt, &item.UpdatedAt, &item.ModeratedBy, &item.ModeratedAt); err != nil {
		return nil, err
	}
	item.Titol = titol.String
	item.Resum = resum.String
	item.TagsJSON = tags.String
	item.ModerationNotes = notes.String
	return &item, nil
}

func (h sqlHelper) updateMunicipiHistoriaGeneralDraft(v *MunicipiHistoriaGeneralVersion) error {
	if v == nil {
		return errors.New("version nil")
	}
	titolVal := sql.NullString{String: strings.TrimSpace(v.Titol), Valid: strings.TrimSpace(v.Titol) != ""}
	resumVal := sql.NullString{String: strings.TrimSpace(v.Resum), Valid: strings.TrimSpace(v.Resum) != ""}
	tagsVal := sql.NullString{String: strings.TrimSpace(v.TagsJSON), Valid: strings.TrimSpace(v.TagsJSON) != ""}
	query := `
        UPDATE municipi_historia_general_versions
        SET titol = ?, resum = ?, cos_text = ?, tags_json = ?, lock_version = lock_version + 1, updated_at = ` + h.nowFun + `
        WHERE id = ? AND lock_version = ? AND status = 'draft'`
	query = formatPlaceholders(h.style, query)
	res, err := h.db.Exec(query, titolVal, resumVal, v.CosText, tagsVal, v.ID, v.LockVersion)
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return ErrConflict
	}
	return nil
}

func (h sqlHelper) setMunicipiHistoriaGeneralStatus(versionID int, status, notes string, moderatorID *int) error {
	var (
		query string
		args  []interface{}
	)
	notesVal := strings.TrimSpace(notes)
	if moderatorID != nil && *moderatorID > 0 {
		query = `
            UPDATE municipi_historia_general_versions
            SET status = ?, moderation_notes = ?, moderated_by = ?, moderated_at = ?, updated_at = ` + h.nowFun + `
            WHERE id = ?`
		args = []interface{}{status, notesVal, *moderatorID, time.Now(), versionID}
	} else {
		query = `
            UPDATE municipi_historia_general_versions
            SET status = ?, moderation_notes = ?, moderated_by = NULL, moderated_at = NULL, updated_at = ` + h.nowFun + `
            WHERE id = ?`
		args = []interface{}{status, notesVal, versionID}
	}
	query = formatPlaceholders(h.style, query)
	if _, err := h.db.Exec(query, args...); err != nil {
		return err
	}
	if status != "publicat" {
		return nil
	}
	query = formatPlaceholders(h.style, `SELECT historia_id FROM municipi_historia_general_versions WHERE id = ?`)
	var historiaID int
	if err := h.db.QueryRow(query, versionID).Scan(&historiaID); err != nil {
		return err
	}
	updateQuery := `
        UPDATE municipi_historia
        SET current_general_version_id = ?, updated_at = ` + h.nowFun + `
        WHERE id = ?`
	updateQuery = formatPlaceholders(h.style, updateQuery)
	_, err := h.db.Exec(updateQuery, versionID, historiaID)
	return err
}

func (h sqlHelper) createMunicipiHistoriaFet(municipiID int, createdBy int) (int, error) {
	if municipiID <= 0 {
		return 0, errors.New("municipi_id invalid")
	}
	query := `
        INSERT INTO municipi_historia_fets (municipi_id, created_at, updated_at)
        VALUES (?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		query += " RETURNING id"
	}
	query = formatPlaceholders(h.style, query)
	if h.style == "postgres" {
		var id int
		if err := h.db.QueryRow(query, municipiID).Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}
	res, err := h.db.Exec(query, municipiID)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		return int(id), nil
	}
	_ = createdBy
	return 0, nil
}

func (h sqlHelper) nextMunicipiHistoriaFetVersion(fetID int) (int, error) {
	query := formatPlaceholders(h.style, `
        SELECT COALESCE(MAX(version), 0) + 1
        FROM municipi_historia_fet_versions
        WHERE fet_id = ?`)
	var next int
	if err := h.db.QueryRow(query, fetID).Scan(&next); err != nil {
		return 0, err
	}
	return next, nil
}

func (h sqlHelper) createMunicipiHistoriaFetDraft(fetID int, createdBy int, baseFromCurrent bool) (int, error) {
	if fetID <= 0 {
		return 0, errors.New("fet_id invalid")
	}
	version, err := h.nextMunicipiHistoriaFetVersion(fetID)
	if err != nil {
		return 0, err
	}
	var (
		anyInici    sql.NullInt64
		anyFi       sql.NullInt64
		dataInici   sql.NullString
		dataFi      sql.NullString
		dataDisplay sql.NullString
		titol       sql.NullString
		resum       sql.NullString
		cosText     sql.NullString
		tags        sql.NullString
		fonts       sql.NullString
	)
	if baseFromCurrent {
		query := `
            SELECT v.any_inici, v.any_fi, v.data_inici, v.data_fi, v.data_display,
                   v.titol, v.resum, v.cos_text, v.tags_json, v.fonts_json
            FROM municipi_historia_fets f
            JOIN municipi_historia_fet_versions v ON v.id = f.current_version_id
            WHERE f.id = ?`
		query = formatPlaceholders(h.style, query)
		if err := h.db.QueryRow(query, fetID).Scan(&anyInici, &anyFi, &dataInici, &dataFi, &dataDisplay,
			&titol, &resum, &cosText, &tags, &fonts); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
	}
	createdByVal := sql.NullInt64{}
	if createdBy > 0 {
		createdByVal = sql.NullInt64{Int64: int64(createdBy), Valid: true}
	}
	if !cosText.Valid {
		cosText = sql.NullString{String: "", Valid: true}
	}
	query := `
        INSERT INTO municipi_historia_fet_versions
            (fet_id, version, any_inici, any_fi, data_inici, data_fi, data_display,
             titol, resum, cos_text, tags_json, fonts_json, status, moderation_notes, lock_version,
             created_by, created_at, updated_at, moderated_by, moderated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'draft', '', 0, ?, ` + h.nowFun + `, ` + h.nowFun + `, NULL, NULL)`
	if h.style == "postgres" {
		query += " RETURNING id"
	}
	query = formatPlaceholders(h.style, query)
	args := []interface{}{fetID, version, anyInici, anyFi, dataInici, dataFi, dataDisplay, titol, resum, cosText, tags, fonts, createdByVal}
	if h.style == "postgres" {
		var id int
		if err := h.db.QueryRow(query, args...).Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}
	res, err := h.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		return int(id), nil
	}
	return 0, nil
}

func (h sqlHelper) getMunicipiHistoriaFetVersion(id int) (*MunicipiHistoriaFetVersion, error) {
	query := `
        SELECT id, fet_id, version, any_inici, any_fi, data_inici, data_fi, data_display,
               titol, resum, cos_text, tags_json, fonts_json, status, moderation_notes,
               lock_version, created_by, created_at, updated_at, moderated_by, moderated_at
        FROM municipi_historia_fet_versions
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, id)
	var item MunicipiHistoriaFetVersion
	var dataInici sql.NullString
	var dataFi sql.NullString
	var dataDisplay sql.NullString
	var resum sql.NullString
	var tags sql.NullString
	var fonts sql.NullString
	var notes sql.NullString
	if err := row.Scan(&item.ID, &item.FetID, &item.Version, &item.AnyInici, &item.AnyFi, &dataInici, &dataFi, &dataDisplay,
		&item.Titol, &resum, &item.CosText, &tags, &fonts, &item.Status, &notes,
		&item.LockVersion, &item.CreatedBy, &item.CreatedAt, &item.UpdatedAt, &item.ModeratedBy, &item.ModeratedAt); err != nil {
		return nil, err
	}
	item.DataInici = dataInici.String
	item.DataFi = dataFi.String
	item.DataDisplay = dataDisplay.String
	item.Resum = resum.String
	item.TagsJSON = tags.String
	item.FontsJSON = fonts.String
	item.ModerationNotes = notes.String
	return &item, nil
}

func (h sqlHelper) updateMunicipiHistoriaFetDraft(v *MunicipiHistoriaFetVersion) error {
	if v == nil {
		return errors.New("version nil")
	}
	dataInici := sql.NullString{String: strings.TrimSpace(v.DataInici), Valid: strings.TrimSpace(v.DataInici) != ""}
	dataFi := sql.NullString{String: strings.TrimSpace(v.DataFi), Valid: strings.TrimSpace(v.DataFi) != ""}
	dataDisplay := sql.NullString{String: strings.TrimSpace(v.DataDisplay), Valid: strings.TrimSpace(v.DataDisplay) != ""}
	resumVal := sql.NullString{String: strings.TrimSpace(v.Resum), Valid: strings.TrimSpace(v.Resum) != ""}
	tagsVal := sql.NullString{String: strings.TrimSpace(v.TagsJSON), Valid: strings.TrimSpace(v.TagsJSON) != ""}
	fontsVal := sql.NullString{String: strings.TrimSpace(v.FontsJSON), Valid: strings.TrimSpace(v.FontsJSON) != ""}
	query := `
        UPDATE municipi_historia_fet_versions
        SET any_inici = ?, any_fi = ?, data_inici = ?, data_fi = ?, data_display = ?,
            titol = ?, resum = ?, cos_text = ?, tags_json = ?, fonts_json = ?,
            lock_version = lock_version + 1, updated_at = ` + h.nowFun + `
        WHERE id = ? AND lock_version = ? AND status = 'draft'`
	query = formatPlaceholders(h.style, query)
	res, err := h.db.Exec(query, v.AnyInici, v.AnyFi, dataInici, dataFi, dataDisplay,
		v.Titol, resumVal, v.CosText, tagsVal, fontsVal, v.ID, v.LockVersion)
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return ErrConflict
	}
	return nil
}

func (h sqlHelper) setMunicipiHistoriaFetStatus(versionID int, status, notes string, moderatorID *int) error {
	var (
		query string
		args  []interface{}
	)
	notesVal := strings.TrimSpace(notes)
	if moderatorID != nil && *moderatorID > 0 {
		query = `
            UPDATE municipi_historia_fet_versions
            SET status = ?, moderation_notes = ?, moderated_by = ?, moderated_at = ?, updated_at = ` + h.nowFun + `
            WHERE id = ?`
		args = []interface{}{status, notesVal, *moderatorID, time.Now(), versionID}
	} else {
		query = `
            UPDATE municipi_historia_fet_versions
            SET status = ?, moderation_notes = ?, moderated_by = NULL, moderated_at = NULL, updated_at = ` + h.nowFun + `
            WHERE id = ?`
		args = []interface{}{status, notesVal, versionID}
	}
	query = formatPlaceholders(h.style, query)
	if _, err := h.db.Exec(query, args...); err != nil {
		return err
	}
	if status != "publicat" {
		return nil
	}
	query = formatPlaceholders(h.style, `SELECT fet_id FROM municipi_historia_fet_versions WHERE id = ?`)
	var fetID int
	if err := h.db.QueryRow(query, versionID).Scan(&fetID); err != nil {
		return err
	}
	updateQuery := `
        UPDATE municipi_historia_fets
        SET current_version_id = ?, updated_at = ` + h.nowFun + `
        WHERE id = ?`
	updateQuery = formatPlaceholders(h.style, updateQuery)
	_, err := h.db.Exec(updateQuery, versionID, fetID)
	return err
}

func (h sqlHelper) getMunicipiHistoriaSummary(municipiID int) (*MunicipiHistoriaGeneralVersion, []MunicipiHistoriaFetVersion, error) {
	var general *MunicipiHistoriaGeneralVersion
	query := `
        SELECT v.id, v.historia_id, v.version, v.titol, v.resum, v.cos_text, v.tags_json, v.status, v.moderation_notes,
               v.lock_version, v.created_by, v.created_at, v.updated_at, v.moderated_by, v.moderated_at
        FROM municipi_historia h
        JOIN municipi_historia_general_versions v ON v.id = h.current_general_version_id
        WHERE h.municipi_id = ?`
	query = formatPlaceholders(h.style, query)
	var row MunicipiHistoriaGeneralVersion
	var titol sql.NullString
	var resum sql.NullString
	var tags sql.NullString
	var notes sql.NullString
	if err := h.db.QueryRow(query, municipiID).Scan(&row.ID, &row.HistoriaID, &row.Version, &titol, &resum, &row.CosText, &tags,
		&row.Status, &notes, &row.LockVersion, &row.CreatedBy, &row.CreatedAt, &row.UpdatedAt, &row.ModeratedBy, &row.ModeratedAt); err == nil {
		row.Titol = titol.String
		row.Resum = resum.String
		row.TagsJSON = tags.String
		row.ModerationNotes = notes.String
		general = &row
	} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, nil, err
	}

	timeline := []MunicipiHistoriaFetVersion{}
	query = `
        SELECT v.id, v.fet_id, v.version, v.any_inici, v.any_fi, v.data_inici, v.data_fi, v.data_display,
               v.titol, v.resum, v.cos_text, v.tags_json, v.fonts_json, v.status, v.moderation_notes,
               v.lock_version, v.created_by, v.created_at, v.updated_at, v.moderated_by, v.moderated_at
        FROM municipi_historia_fets f
        JOIN municipi_historia_fet_versions v ON v.id = f.current_version_id
        WHERE f.municipi_id = ? AND v.status = 'publicat'
        ORDER BY
            CASE WHEN v.any_inici IS NULL AND v.any_fi IS NULL THEN 1 ELSE 0 END,
            COALESCE(v.any_inici, v.any_fi) ASC,
            v.any_fi ASC,
            v.created_at ASC,
            v.id ASC
        LIMIT 6`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, municipiID)
	if err != nil {
		return general, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var item MunicipiHistoriaFetVersion
		var dataInici sql.NullString
		var dataFi sql.NullString
		var dataDisplay sql.NullString
		var resumVal sql.NullString
		var tags sql.NullString
		var fonts sql.NullString
		var notes sql.NullString
		if err := rows.Scan(&item.ID, &item.FetID, &item.Version, &item.AnyInici, &item.AnyFi, &dataInici, &dataFi, &dataDisplay,
			&item.Titol, &resumVal, &item.CosText, &tags, &fonts, &item.Status, &notes,
			&item.LockVersion, &item.CreatedBy, &item.CreatedAt, &item.UpdatedAt, &item.ModeratedBy, &item.ModeratedAt); err != nil {
			return general, nil, err
		}
		item.DataInici = dataInici.String
		item.DataFi = dataFi.String
		item.DataDisplay = dataDisplay.String
		item.Resum = resumVal.String
		item.TagsJSON = tags.String
		item.FontsJSON = fonts.String
		item.ModerationNotes = notes.String
		timeline = append(timeline, item)
	}
	return general, timeline, nil
}

func (h sqlHelper) listMunicipiHistoriaTimeline(municipiID int, status string, limit, offset int, q string, anyFrom, anyTo *int) ([]MunicipiHistoriaFetVersion, int, error) {
	args := []interface{}{municipiID}
	clauses := []string{"f.municipi_id = ?"}
	if strings.TrimSpace(status) != "" {
		clauses = append(clauses, "v.status = ?")
		args = append(args, strings.TrimSpace(status))
	}
	if strings.TrimSpace(q) != "" {
		qLike := "%" + strings.ToLower(strings.TrimSpace(q)) + "%"
		clauses = append(clauses, "(LOWER(v.titol) LIKE ? OR LOWER(v.resum) LIKE ? OR LOWER(v.cos_text) LIKE ? OR LOWER(v.data_display) LIKE ?)")
		args = append(args, qLike, qLike, qLike, qLike)
	}
	if anyFrom != nil {
		clauses = append(clauses, "COALESCE(v.any_fi, v.any_inici) >= ?")
		args = append(args, *anyFrom)
	}
	if anyTo != nil {
		clauses = append(clauses, "COALESCE(v.any_inici, v.any_fi) <= ?")
		args = append(args, *anyTo)
	}
	baseWhere := strings.Join(clauses, " AND ")
	countQuery := `
        SELECT COUNT(1)
        FROM municipi_historia_fets f
        JOIN municipi_historia_fet_versions v ON v.fet_id = f.id
        WHERE ` + baseWhere
	countQuery = formatPlaceholders(h.style, countQuery)
	total := 0
	if err := h.db.QueryRow(countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	query := `
        SELECT v.id, v.fet_id, v.version, v.any_inici, v.any_fi, v.data_inici, v.data_fi, v.data_display,
               v.titol, v.resum, v.cos_text, v.tags_json, v.fonts_json, v.status, v.moderation_notes,
               v.lock_version, v.created_by, v.created_at, v.updated_at, v.moderated_by, v.moderated_at
        FROM municipi_historia_fets f
        JOIN municipi_historia_fet_versions v ON v.fet_id = f.id
        WHERE ` + baseWhere + `
        ORDER BY
            CASE WHEN v.any_inici IS NULL AND v.any_fi IS NULL THEN 1 ELSE 0 END,
            COALESCE(v.any_inici, v.any_fi) ASC,
            v.any_fi ASC,
            v.created_at ASC,
            v.id ASC`
	listArgs := make([]interface{}, len(args))
	copy(listArgs, args)
	if limit > 0 {
		query += " LIMIT ?"
		listArgs = append(listArgs, limit)
	}
	if offset > 0 {
		query += " OFFSET ?"
		listArgs = append(listArgs, offset)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, listArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var res []MunicipiHistoriaFetVersion
	for rows.Next() {
		var item MunicipiHistoriaFetVersion
		var dataInici sql.NullString
		var dataFi sql.NullString
		var dataDisplay sql.NullString
		var resumVal sql.NullString
		var tags sql.NullString
		var fonts sql.NullString
		var notes sql.NullString
		if err := rows.Scan(&item.ID, &item.FetID, &item.Version, &item.AnyInici, &item.AnyFi, &dataInici, &dataFi, &dataDisplay,
			&item.Titol, &resumVal, &item.CosText, &tags, &fonts, &item.Status, &notes,
			&item.LockVersion, &item.CreatedBy, &item.CreatedAt, &item.UpdatedAt, &item.ModeratedBy, &item.ModeratedAt); err != nil {
			return nil, 0, err
		}
		item.DataInici = dataInici.String
		item.DataFi = dataFi.String
		item.DataDisplay = dataDisplay.String
		item.Resum = resumVal.String
		item.TagsJSON = tags.String
		item.FontsJSON = fonts.String
		item.ModerationNotes = notes.String
		res = append(res, item)
	}
	return res, total, nil
}

func (h sqlHelper) listPendingMunicipiHistoriaGeneralVersions(limit, offset int) ([]MunicipiHistoriaGeneralVersion, int, error) {
	countQuery := formatPlaceholders(h.style, `SELECT COUNT(1) FROM municipi_historia_general_versions WHERE status = 'pendent'`)
	total := 0
	if err := h.db.QueryRow(countQuery).Scan(&total); err != nil {
		return nil, 0, err
	}
	query := `
        SELECT v.id, v.historia_id, h.municipi_id, m.nom, v.version, v.titol, v.resum, v.cos_text, v.tags_json, v.status, v.moderation_notes,
               v.lock_version, v.created_by, v.created_at, v.updated_at, v.moderated_by, v.moderated_at
        FROM municipi_historia_general_versions v
        JOIN municipi_historia h ON h.id = v.historia_id
        JOIN municipis m ON m.id = h.municipi_id
        WHERE v.status = 'pendent'
        ORDER BY v.created_at ASC, v.id ASC`
	args := []interface{}{}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	if offset > 0 {
		query += " OFFSET ?"
		args = append(args, offset)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var res []MunicipiHistoriaGeneralVersion
	for rows.Next() {
		var item MunicipiHistoriaGeneralVersion
		var titol sql.NullString
		var resum sql.NullString
		var tags sql.NullString
		var notes sql.NullString
		if err := rows.Scan(&item.ID, &item.HistoriaID, &item.MunicipiID, &item.MunicipiNom, &item.Version,
			&titol, &resum, &item.CosText, &tags, &item.Status, &notes, &item.LockVersion, &item.CreatedBy,
			&item.CreatedAt, &item.UpdatedAt, &item.ModeratedBy, &item.ModeratedAt); err != nil {
			return nil, 0, err
		}
		item.Titol = titol.String
		item.Resum = resum.String
		item.TagsJSON = tags.String
		item.ModerationNotes = notes.String
		res = append(res, item)
	}
	return res, total, nil
}

func (h sqlHelper) listPendingMunicipiHistoriaFetVersions(limit, offset int) ([]MunicipiHistoriaFetVersion, int, error) {
	countQuery := formatPlaceholders(h.style, `SELECT COUNT(1) FROM municipi_historia_fet_versions WHERE status = 'pendent'`)
	total := 0
	if err := h.db.QueryRow(countQuery).Scan(&total); err != nil {
		return nil, 0, err
	}
	query := `
        SELECT v.id, v.fet_id, f.municipi_id, m.nom, v.version, v.any_inici, v.any_fi, v.data_inici, v.data_fi, v.data_display,
               v.titol, v.resum, v.cos_text, v.tags_json, v.fonts_json, v.status, v.moderation_notes,
               v.lock_version, v.created_by, v.created_at, v.updated_at, v.moderated_by, v.moderated_at
        FROM municipi_historia_fet_versions v
        JOIN municipi_historia_fets f ON f.id = v.fet_id
        JOIN municipis m ON m.id = f.municipi_id
        WHERE v.status = 'pendent'
        ORDER BY v.created_at ASC, v.id ASC`
	args := []interface{}{}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	if offset > 0 {
		query += " OFFSET ?"
		args = append(args, offset)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var res []MunicipiHistoriaFetVersion
	for rows.Next() {
		var item MunicipiHistoriaFetVersion
		var dataInici sql.NullString
		var dataFi sql.NullString
		var dataDisplay sql.NullString
		var resumVal sql.NullString
		var tags sql.NullString
		var fonts sql.NullString
		var notes sql.NullString
		if err := rows.Scan(&item.ID, &item.FetID, &item.MunicipiID, &item.MunicipiNom, &item.Version,
			&item.AnyInici, &item.AnyFi, &dataInici, &dataFi, &dataDisplay,
			&item.Titol, &resumVal, &item.CosText, &tags, &fonts, &item.Status, &notes,
			&item.LockVersion, &item.CreatedBy, &item.CreatedAt, &item.UpdatedAt, &item.ModeratedBy, &item.ModeratedAt); err != nil {
			return nil, 0, err
		}
		item.DataInici = dataInici.String
		item.DataFi = dataFi.String
		item.DataDisplay = dataDisplay.String
		item.Resum = resumVal.String
		item.TagsJSON = tags.String
		item.FontsJSON = fonts.String
		item.ModerationNotes = notes.String
		res = append(res, item)
	}
	return res, total, nil
}

// Demografia municipi
func (h sqlHelper) getMunicipiDemografiaMeta(municipiID int) (*MunicipiDemografiaMeta, error) {
	if municipiID <= 0 {
		return nil, errors.New("municipi_id invalid")
	}
	query := `SELECT municipi_id, any_min, any_max, total_natalitat, total_matrimonis, total_defuncions, updated_at
	          FROM municipi_demografia_meta WHERE municipi_id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, municipiID)
	var item MunicipiDemografiaMeta
	if err := row.Scan(&item.MunicipiID, &item.AnyMin, &item.AnyMax, &item.TotalNatalitat, &item.TotalMatrimonis, &item.TotalDefuncions, &item.UpdatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &item, nil
}

func (h sqlHelper) listMunicipiDemografiaAny(municipiID int, from, to int) ([]MunicipiDemografiaAny, error) {
	if municipiID <= 0 {
		return nil, errors.New("municipi_id invalid")
	}
	yearCol := demografiaYearColumn(h.style)
	query := fmt.Sprintf(`
        SELECT municipi_id, %s, natalitat, matrimonis, defuncions, updated_at
        FROM municipi_demografia_any
        WHERE municipi_id = ?`, yearCol)
	args := []interface{}{municipiID}
	if from > 0 {
		query += fmt.Sprintf(" AND %s >= ?", yearCol)
		args = append(args, from)
	}
	if to > 0 {
		query += fmt.Sprintf(" AND %s <= ?", yearCol)
		args = append(args, to)
	}
	query += " ORDER BY " + yearCol + " ASC"
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []MunicipiDemografiaAny
	for rows.Next() {
		var row MunicipiDemografiaAny
		if err := rows.Scan(&row.MunicipiID, &row.Any, &row.Natalitat, &row.Matrimonis, &row.Defuncions, &row.UpdatedAt); err != nil {
			return nil, err
		}
		res = append(res, row)
	}
	return res, nil
}

func (h sqlHelper) listMunicipiDemografiaDecades(municipiID int, from, to int) ([]MunicipiDemografiaAny, error) {
	rows, err := h.listMunicipiDemografiaAny(municipiID, from, to)
	if err != nil {
		return nil, err
	}
	decades := map[int]*MunicipiDemografiaAny{}
	for _, row := range rows {
		if row.Any <= 0 {
			continue
		}
		decade := (row.Any / 10) * 10
		item := decades[decade]
		if item == nil {
			item = &MunicipiDemografiaAny{MunicipiID: row.MunicipiID, Any: decade}
			decades[decade] = item
		}
		item.Natalitat += row.Natalitat
		item.Matrimonis += row.Matrimonis
		item.Defuncions += row.Defuncions
		if !row.UpdatedAt.Valid {
			continue
		}
		if !item.UpdatedAt.Valid || row.UpdatedAt.Time.After(item.UpdatedAt.Time) {
			item.UpdatedAt = row.UpdatedAt
		}
	}
	keys := make([]int, 0, len(decades))
	for decade := range decades {
		keys = append(keys, decade)
	}
	sort.Ints(keys)
	res := make([]MunicipiDemografiaAny, 0, len(keys))
	for _, decade := range keys {
		res = append(res, *decades[decade])
	}
	return res, nil
}

func (h sqlHelper) applyMunicipiDemografiaDelta(municipiID, year int, tipus string, delta int) error {
	if municipiID <= 0 || year <= 0 {
		return errors.New("invalid ids")
	}
	if delta == 0 {
		return nil
	}
	tx, err := h.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := h.applyMunicipiDemografiaDeltaTx(tx, municipiID, year, tipus, delta); err != nil {
		return err
	}
	return tx.Commit()
}

func (h sqlHelper) applyMunicipiDemografiaDeltaTx(tx *sql.Tx, municipiID, year int, tipus string, delta int) error {
	if tx == nil {
		return errors.New("tx required")
	}
	if municipiID <= 0 || year <= 0 {
		return errors.New("invalid ids")
	}
	if delta == 0 {
		return nil
	}
	natalitat := 0
	matrimonis := 0
	defuncions := 0
	switch strings.ToLower(strings.TrimSpace(tipus)) {
	case "natalitat":
		natalitat = delta
	case "matrimonis":
		matrimonis = delta
	case "defuncions":
		defuncions = delta
	default:
		return errors.New("tipus invalid")
	}
	yearCol := demografiaYearColumn(h.style)
	var upsert string
	switch h.style {
	case "mysql":
		upsert = fmt.Sprintf(`INSERT INTO municipi_demografia_any (municipi_id, %s, natalitat, matrimonis, defuncions, updated_at)
            VALUES (?, ?, ?, ?, ?, `+h.nowFun+`)
            ON DUPLICATE KEY UPDATE natalitat = natalitat + VALUES(natalitat),
            matrimonis = matrimonis + VALUES(matrimonis),
            defuncions = defuncions + VALUES(defuncions),
            updated_at = `+h.nowFun, yearCol)
	case "postgres":
		upsert = fmt.Sprintf(`INSERT INTO municipi_demografia_any (municipi_id, %s, natalitat, matrimonis, defuncions, updated_at)
            VALUES (?, ?, ?, ?, ?, `+h.nowFun+`)
            ON CONFLICT (municipi_id, %s) DO UPDATE SET
            natalitat = municipi_demografia_any.natalitat + EXCLUDED.natalitat,
            matrimonis = municipi_demografia_any.matrimonis + EXCLUDED.matrimonis,
            defuncions = municipi_demografia_any.defuncions + EXCLUDED.defuncions,
            updated_at = `+h.nowFun, yearCol, yearCol)
	default: // sqlite
		upsert = fmt.Sprintf(`INSERT INTO municipi_demografia_any (municipi_id, %s, natalitat, matrimonis, defuncions, updated_at)
            VALUES (?, ?, ?, ?, ?, `+h.nowFun+`)
            ON CONFLICT(municipi_id, %s) DO UPDATE SET
            natalitat = natalitat + excluded.natalitat,
            matrimonis = matrimonis + excluded.matrimonis,
            defuncions = defuncions + excluded.defuncions,
            updated_at = `+h.nowFun, yearCol, yearCol)
	}
	upsert = formatPlaceholders(h.style, upsert)
	if _, err := tx.Exec(upsert, municipiID, year, natalitat, matrimonis, defuncions); err != nil {
		return err
	}
	selectStmt := fmt.Sprintf(`SELECT natalitat, matrimonis, defuncions
        FROM municipi_demografia_any
        WHERE municipi_id = ? AND %s = ?`, yearCol)
	selectStmt = formatPlaceholders(h.style, selectStmt)
	var curNat, curMat, curDef int
	if err := tx.QueryRow(selectStmt, municipiID, year).Scan(&curNat, &curMat, &curDef); err != nil {
		return err
	}
	changed := false
	if curNat < 0 {
		curNat = 0
		changed = true
	}
	if curMat < 0 {
		curMat = 0
		changed = true
	}
	if curDef < 0 {
		curDef = 0
		changed = true
	}
	if curNat == 0 && curMat == 0 && curDef == 0 {
		delStmt := fmt.Sprintf(`DELETE FROM municipi_demografia_any WHERE municipi_id = ? AND %s = ?`, yearCol)
		delStmt = formatPlaceholders(h.style, delStmt)
		if _, err := tx.Exec(delStmt, municipiID, year); err != nil {
			return err
		}
	} else if changed {
		updateStmt := fmt.Sprintf(`UPDATE municipi_demografia_any
            SET natalitat = ?, matrimonis = ?, defuncions = ?, updated_at = `+h.nowFun+`
            WHERE municipi_id = ? AND %s = ?`, yearCol)
		updateStmt = formatPlaceholders(h.style, updateStmt)
		if _, err := tx.Exec(updateStmt, curNat, curMat, curDef, municipiID, year); err != nil {
			return err
		}
	}
	insertMeta := `INSERT INTO municipi_demografia_meta (municipi_id, any_min, any_max, total_natalitat, total_matrimonis, total_defuncions, updated_at)
        VALUES (?, NULL, NULL, 0, 0, 0, ` + h.nowFun + `)`
	if h.style == "postgres" {
		insertMeta += " ON CONFLICT (municipi_id) DO NOTHING"
	} else if h.style == "mysql" {
		insertMeta += " ON DUPLICATE KEY UPDATE municipi_id = VALUES(municipi_id)"
	} else {
		insertMeta += " ON CONFLICT(municipi_id) DO NOTHING"
	}
	insertMeta = formatPlaceholders(h.style, insertMeta)
	if _, err := tx.Exec(insertMeta, municipiID); err != nil {
		return err
	}
	updateTotals := `UPDATE municipi_demografia_meta
        SET total_natalitat = CASE WHEN total_natalitat + ? < 0 THEN 0 ELSE total_natalitat + ? END,
            total_matrimonis = CASE WHEN total_matrimonis + ? < 0 THEN 0 ELSE total_matrimonis + ? END,
            total_defuncions = CASE WHEN total_defuncions + ? < 0 THEN 0 ELSE total_defuncions + ? END,
            updated_at = ` + h.nowFun + `
        WHERE municipi_id = ?`
	updateTotals = formatPlaceholders(h.style, updateTotals)
	if _, err := tx.Exec(updateTotals, natalitat, natalitat, matrimonis, matrimonis, defuncions, defuncions, municipiID); err != nil {
		return err
	}
	minMaxStmt := fmt.Sprintf(`SELECT MIN(%s), MAX(%s) FROM municipi_demografia_any WHERE municipi_id = ?`, yearCol, yearCol)
	minMaxStmt = formatPlaceholders(h.style, minMaxStmt)
	var minAny sql.NullInt64
	var maxAny sql.NullInt64
	if err := tx.QueryRow(minMaxStmt, municipiID).Scan(&minAny, &maxAny); err != nil {
		return err
	}
	updateRange := `UPDATE municipi_demografia_meta
        SET any_min = ?, any_max = ?, updated_at = ` + h.nowFun + `
        WHERE municipi_id = ?`
	updateRange = formatPlaceholders(h.style, updateRange)
	if _, err := tx.Exec(updateRange, minAny, maxAny, municipiID); err != nil {
		return err
	}
	return nil
}

func (h sqlHelper) rebuildMunicipiDemografia(municipiID int) error {
	if municipiID <= 0 {
		return errors.New("municipi_id invalid")
	}
	tx, err := h.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	delAny := formatPlaceholders(h.style, `DELETE FROM municipi_demografia_any WHERE municipi_id = ?`)
	if _, err := tx.Exec(delAny, municipiID); err != nil {
		return err
	}
	delMeta := formatPlaceholders(h.style, `DELETE FROM municipi_demografia_meta WHERE municipi_id = ?`)
	if _, err := tx.Exec(delMeta, municipiID); err != nil {
		return err
	}
	yearExpr := demografiaYearExpr(h.style)
	query := fmt.Sprintf(`
        SELECT %s AS any,
               SUM(CASE WHEN t.tipus_acte='baptisme' THEN 1 ELSE 0 END) AS natalitat,
               SUM(CASE WHEN t.tipus_acte='matrimoni' THEN 1 ELSE 0 END) AS matrimonis,
               SUM(CASE WHEN t.tipus_acte='obit' THEN 1 ELSE 0 END) AS defuncions
        FROM transcripcions_raw t
        JOIN llibres l ON l.id = t.llibre_id
        WHERE t.moderation_status='publicat'
          AND t.tipus_acte IN ('baptisme','matrimoni','obit')
          AND l.municipi_id = ?
          AND %s BETWEEN 1200 AND 2100
        GROUP BY %s
        ORDER BY %s ASC`, yearExpr, yearExpr, yearExpr, yearExpr)
	query = formatPlaceholders(h.style, query)
	rows, err := tx.Query(query, municipiID)
	if err != nil {
		return err
	}
	defer rows.Close()
	yearCol := demografiaYearColumn(h.style)
	insertAny := fmt.Sprintf(`INSERT INTO municipi_demografia_any (municipi_id, %s, natalitat, matrimonis, defuncions, updated_at)
        VALUES (?, ?, ?, ?, ?, `+h.nowFun+`)`, yearCol)
	insertAny = formatPlaceholders(h.style, insertAny)
	first := true
	minAny := 0
	maxAny := 0
	totalNat := 0
	totalMat := 0
	totalDef := 0
	for rows.Next() {
		var year int
		var nat int
		var mat int
		var def int
		if err := rows.Scan(&year, &nat, &mat, &def); err != nil {
			return err
		}
		if year <= 0 {
			continue
		}
		if _, err := tx.Exec(insertAny, municipiID, year, nat, mat, def); err != nil {
			return err
		}
		totalNat += nat
		totalMat += mat
		totalDef += def
		if first {
			minAny = year
			maxAny = year
			first = false
		} else {
			if year < minAny {
				minAny = year
			}
			if year > maxAny {
				maxAny = year
			}
		}
	}
	if !first {
		minVal := sql.NullInt64{Int64: int64(minAny), Valid: true}
		maxVal := sql.NullInt64{Int64: int64(maxAny), Valid: true}
		insertMeta := `INSERT INTO municipi_demografia_meta (municipi_id, any_min, any_max, total_natalitat, total_matrimonis, total_defuncions, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ` + h.nowFun + `)`
		if h.style == "postgres" {
			insertMeta += " ON CONFLICT (municipi_id) DO UPDATE SET any_min = EXCLUDED.any_min, any_max = EXCLUDED.any_max, total_natalitat = EXCLUDED.total_natalitat, total_matrimonis = EXCLUDED.total_matrimonis, total_defuncions = EXCLUDED.total_defuncions, updated_at = " + h.nowFun
		} else if h.style == "mysql" {
			insertMeta += " ON DUPLICATE KEY UPDATE any_min = VALUES(any_min), any_max = VALUES(any_max), total_natalitat = VALUES(total_natalitat), total_matrimonis = VALUES(total_matrimonis), total_defuncions = VALUES(total_defuncions), updated_at = " + h.nowFun
		} else {
			insertMeta += " ON CONFLICT(municipi_id) DO UPDATE SET any_min = excluded.any_min, any_max = excluded.any_max, total_natalitat = excluded.total_natalitat, total_matrimonis = excluded.total_matrimonis, total_defuncions = excluded.total_defuncions, updated_at = " + h.nowFun
		}
		insertMeta = formatPlaceholders(h.style, insertMeta)
		if _, err := tx.Exec(insertMeta, municipiID, minVal, maxVal, totalNat, totalMat, totalDef); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Anecdotari municipi
func (h sqlHelper) listMunicipiAnecdotariPublished(municipiID int, f MunicipiAnecdotariFilter) ([]MunicipiAnecdotariVersion, int, error) {
	if municipiID <= 0 {
		return nil, 0, errors.New("municipi_id invalid")
	}
	status := strings.TrimSpace(f.Status)
	if status == "" {
		status = "publicat"
	}
	args := []interface{}{municipiID, status}
	clauses := []string{"i.municipi_id = ?", "v.status = ?"}
	if tag := strings.TrimSpace(f.Tag); tag != "" {
		clauses = append(clauses, "v.tag = ?")
		args = append(args, tag)
	}
	if q := strings.TrimSpace(f.Query); q != "" {
		qLike := "%" + strings.ToLower(q) + "%"
		clauses = append(clauses, "(LOWER(v.titol) LIKE ? OR LOWER(v.text) LIKE ? OR LOWER(v.tag) LIKE ? OR LOWER(v.data_ref) LIKE ?)")
		args = append(args, qLike, qLike, qLike, qLike)
	}
	baseWhere := strings.Join(clauses, " AND ")
	countQuery := `
        SELECT COUNT(1)
        FROM municipi_anecdotari_items i
        JOIN municipi_anecdotari_versions v ON v.id = i.current_version_id
        WHERE ` + baseWhere
	countQuery = formatPlaceholders(h.style, countQuery)
	total := 0
	if err := h.db.QueryRow(countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	query := `
        SELECT v.id, v.item_id, i.municipi_id, v.version, v.status, v.titol, v.tag, v.data_ref, v.text, v.font_url,
               v.moderation_notes, v.lock_version, v.created_by, v.created_at, v.updated_at, v.moderated_by, v.moderated_at
        FROM municipi_anecdotari_items i
        JOIN municipi_anecdotari_versions v ON v.id = i.current_version_id
        WHERE ` + baseWhere + `
        ORDER BY v.created_at DESC, v.id DESC`
	listArgs := make([]interface{}, len(args))
	copy(listArgs, args)
	if f.Limit > 0 {
		query += " LIMIT ?"
		listArgs = append(listArgs, f.Limit)
	}
	if f.Offset > 0 {
		query += " OFFSET ?"
		listArgs = append(listArgs, f.Offset)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, listArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var res []MunicipiAnecdotariVersion
	for rows.Next() {
		var item MunicipiAnecdotariVersion
		var dataRef sql.NullString
		var fontURL sql.NullString
		var notes sql.NullString
		if err := rows.Scan(&item.ID, &item.ItemID, &item.MunicipiID, &item.Version, &item.Status, &item.Titol, &item.Tag,
			&dataRef, &item.Text, &fontURL, &notes, &item.LockVersion, &item.CreatedBy, &item.CreatedAt,
			&item.UpdatedAt, &item.ModeratedBy, &item.ModeratedAt); err != nil {
			return nil, 0, err
		}
		item.DataRef = dataRef.String
		item.FontURL = fontURL.String
		item.ModerationNotes = notes.String
		res = append(res, item)
	}
	return res, total, nil
}

func (h sqlHelper) getMunicipiAnecdotariPublished(itemID int) (*MunicipiAnecdotariVersion, error) {
	if itemID <= 0 {
		return nil, errors.New("item_id invalid")
	}
	query := `
        SELECT v.id, v.item_id, i.municipi_id, v.version, v.status, v.titol, v.tag, v.data_ref, v.text, v.font_url,
               v.moderation_notes, v.lock_version, v.created_by, v.created_at, v.updated_at, v.moderated_by, v.moderated_at
        FROM municipi_anecdotari_items i
        JOIN municipi_anecdotari_versions v ON v.id = i.current_version_id
        WHERE i.id = ? AND v.status = 'publicat'`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, itemID)
	var item MunicipiAnecdotariVersion
	var dataRef sql.NullString
	var fontURL sql.NullString
	var notes sql.NullString
	if err := row.Scan(&item.ID, &item.ItemID, &item.MunicipiID, &item.Version, &item.Status, &item.Titol, &item.Tag,
		&dataRef, &item.Text, &fontURL, &notes, &item.LockVersion, &item.CreatedBy, &item.CreatedAt,
		&item.UpdatedAt, &item.ModeratedBy, &item.ModeratedAt); err != nil {
		return nil, err
	}
	item.DataRef = dataRef.String
	item.FontURL = fontURL.String
	item.ModerationNotes = notes.String
	return &item, nil
}

func (h sqlHelper) listMunicipiAnecdotariComments(itemID int, limit, offset int) ([]MunicipiAnecdotariComment, int, error) {
	if itemID <= 0 {
		return nil, 0, errors.New("item_id invalid")
	}
	countQuery := formatPlaceholders(h.style, `SELECT COUNT(1) FROM municipi_anecdotari_comments WHERE item_id = ?`)
	total := 0
	if err := h.db.QueryRow(countQuery, itemID).Scan(&total); err != nil {
		return nil, 0, err
	}
	query := `
        SELECT id, item_id, user_id, body, created_at
        FROM municipi_anecdotari_comments
        WHERE item_id = ?
        ORDER BY created_at ASC, id ASC`
	args := []interface{}{itemID}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	if offset > 0 {
		query += " OFFSET ?"
		args = append(args, offset)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var res []MunicipiAnecdotariComment
	for rows.Next() {
		var item MunicipiAnecdotariComment
		if err := rows.Scan(&item.ID, &item.ItemID, &item.UserID, &item.Body, &item.CreatedAt); err != nil {
			return nil, 0, err
		}
		res = append(res, item)
	}
	return res, total, nil
}

func (h sqlHelper) createMunicipiAnecdotariItem(municipiID int, createdBy int) (int, error) {
	if municipiID <= 0 {
		return 0, errors.New("municipi_id invalid")
	}
	createdByVal := sql.NullInt64{}
	if createdBy > 0 {
		createdByVal = sql.NullInt64{Int64: int64(createdBy), Valid: true}
	}
	query := `
        INSERT INTO municipi_anecdotari_items (municipi_id, current_version_id, created_by, created_at, updated_at)
        VALUES (?, NULL, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		query += " RETURNING id"
	}
	query = formatPlaceholders(h.style, query)
	if h.style == "postgres" {
		var id int
		if err := h.db.QueryRow(query, municipiID, createdByVal).Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}
	res, err := h.db.Exec(query, municipiID, createdByVal)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		return int(id), nil
	}
	return 0, nil
}

func (h sqlHelper) nextMunicipiAnecdotariVersion(itemID int) (int, error) {
	query := formatPlaceholders(h.style, `
        SELECT COALESCE(MAX(version), 0) + 1
        FROM municipi_anecdotari_versions
        WHERE item_id = ?`)
	var next int
	if err := h.db.QueryRow(query, itemID).Scan(&next); err != nil {
		return 0, err
	}
	return next, nil
}

func (h sqlHelper) createMunicipiAnecdotariDraft(itemID int, createdBy int, baseFromCurrent bool) (int, error) {
	if itemID <= 0 {
		return 0, errors.New("item_id invalid")
	}
	version, err := h.nextMunicipiAnecdotariVersion(itemID)
	if err != nil {
		return 0, err
	}
	titol := ""
	tag := ""
	text := ""
	dataRef := sql.NullString{}
	fontURL := sql.NullString{}
	if baseFromCurrent {
		query := `
            SELECT v.titol, v.tag, v.data_ref, v.text, v.font_url
            FROM municipi_anecdotari_items i
            JOIN municipi_anecdotari_versions v ON v.id = i.current_version_id
            WHERE i.id = ?`
		query = formatPlaceholders(h.style, query)
		var dataRefVal sql.NullString
		var fontURLVal sql.NullString
		if err := h.db.QueryRow(query, itemID).Scan(&titol, &tag, &dataRefVal, &text, &fontURLVal); err == nil {
			dataRef = dataRefVal
			fontURL = fontURLVal
		} else if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
	}
	createdByVal := sql.NullInt64{}
	if createdBy > 0 {
		createdByVal = sql.NullInt64{Int64: int64(createdBy), Valid: true}
	}
	query := `
        INSERT INTO municipi_anecdotari_versions
            (item_id, version, status, titol, tag, data_ref, text, font_url, moderation_notes,
             lock_version, created_by, created_at, updated_at, moderated_by, moderated_at)
        VALUES (?, ?, 'draft', ?, ?, ?, ?, ?, '', 0, ?, ` + h.nowFun + `, ` + h.nowFun + `, NULL, NULL)`
	if h.style == "postgres" {
		query += " RETURNING id"
	}
	query = formatPlaceholders(h.style, query)
	args := []interface{}{itemID, version, titol, tag, dataRef, text, fontURL, createdByVal}
	if h.style == "postgres" {
		var id int
		if err := h.db.QueryRow(query, args...).Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}
	res, err := h.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		return int(id), nil
	}
	return 0, nil
}

func (h sqlHelper) getMunicipiAnecdotariVersion(id int) (*MunicipiAnecdotariVersion, error) {
	query := `
        SELECT id, item_id, version, status, titol, tag, data_ref, text, font_url, moderation_notes,
               lock_version, created_by, created_at, updated_at, moderated_by, moderated_at
        FROM municipi_anecdotari_versions
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, id)
	var item MunicipiAnecdotariVersion
	var dataRef sql.NullString
	var fontURL sql.NullString
	var notes sql.NullString
	if err := row.Scan(&item.ID, &item.ItemID, &item.Version, &item.Status, &item.Titol, &item.Tag,
		&dataRef, &item.Text, &fontURL, &notes, &item.LockVersion, &item.CreatedBy, &item.CreatedAt,
		&item.UpdatedAt, &item.ModeratedBy, &item.ModeratedAt); err != nil {
		return nil, err
	}
	item.DataRef = dataRef.String
	item.FontURL = fontURL.String
	item.ModerationNotes = notes.String
	return &item, nil
}

func (h sqlHelper) getPendingMunicipiAnecdotariVersionByItemID(itemID int) (*MunicipiAnecdotariVersion, error) {
	if itemID <= 0 {
		return nil, errors.New("item_id invalid")
	}
	query := `
        SELECT id, item_id, version, status, titol, tag, data_ref, text, font_url, moderation_notes,
               lock_version, created_by, created_at, updated_at, moderated_by, moderated_at
        FROM municipi_anecdotari_versions
        WHERE item_id = ? AND status = 'pendent'
        ORDER BY created_at DESC, id DESC
        LIMIT 1`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, itemID)
	var item MunicipiAnecdotariVersion
	var dataRef sql.NullString
	var fontURL sql.NullString
	var notes sql.NullString
	if err := row.Scan(&item.ID, &item.ItemID, &item.Version, &item.Status, &item.Titol, &item.Tag,
		&dataRef, &item.Text, &fontURL, &notes, &item.LockVersion, &item.CreatedBy, &item.CreatedAt,
		&item.UpdatedAt, &item.ModeratedBy, &item.ModeratedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	item.DataRef = dataRef.String
	item.FontURL = fontURL.String
	item.ModerationNotes = notes.String
	return &item, nil
}

func (h sqlHelper) updateMunicipiAnecdotariDraft(v *MunicipiAnecdotariVersion) error {
	if v == nil {
		return errors.New("version nil")
	}
	dataRef := sql.NullString{String: strings.TrimSpace(v.DataRef), Valid: strings.TrimSpace(v.DataRef) != ""}
	fontURL := sql.NullString{String: strings.TrimSpace(v.FontURL), Valid: strings.TrimSpace(v.FontURL) != ""}
	query := `
        UPDATE municipi_anecdotari_versions
        SET titol = ?, tag = ?, data_ref = ?, text = ?, font_url = ?, lock_version = lock_version + 1, updated_at = ` + h.nowFun + `
        WHERE id = ? AND lock_version = ? AND status = 'draft'`
	query = formatPlaceholders(h.style, query)
	res, err := h.db.Exec(query, strings.TrimSpace(v.Titol), strings.TrimSpace(v.Tag), dataRef, v.Text, fontURL, v.ID, v.LockVersion)
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return ErrConflict
	}
	return nil
}

func (h sqlHelper) submitMunicipiAnecdotariVersion(versionID int) error {
	if versionID <= 0 {
		return errors.New("version_id invalid")
	}
	query := `
        UPDATE municipi_anecdotari_versions
        SET status = 'pendent', updated_at = ` + h.nowFun + `
        WHERE id = ? AND status = 'draft'`
	query = formatPlaceholders(h.style, query)
	res, err := h.db.Exec(query, versionID)
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return ErrConflict
	}
	return nil
}

func (h sqlHelper) listPendingMunicipiAnecdotariVersions(limit, offset int) ([]MunicipiAnecdotariVersion, int, error) {
	countQuery := formatPlaceholders(h.style, `SELECT COUNT(1) FROM municipi_anecdotari_versions WHERE status = 'pendent'`)
	total := 0
	if err := h.db.QueryRow(countQuery).Scan(&total); err != nil {
		return nil, 0, err
	}
	query := `
        SELECT v.id, v.item_id, i.municipi_id, m.nom, v.version, v.status, v.titol, v.tag, v.data_ref, v.text, v.font_url,
               v.moderation_notes, v.lock_version, v.created_by, v.created_at, v.updated_at, v.moderated_by, v.moderated_at
        FROM municipi_anecdotari_versions v
        JOIN municipi_anecdotari_items i ON i.id = v.item_id
        JOIN municipis m ON m.id = i.municipi_id
        WHERE v.status = 'pendent'
        ORDER BY v.created_at ASC, v.id ASC`
	args := []interface{}{}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	if offset > 0 {
		query += " OFFSET ?"
		args = append(args, offset)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var res []MunicipiAnecdotariVersion
	for rows.Next() {
		var item MunicipiAnecdotariVersion
		var dataRef sql.NullString
		var fontURL sql.NullString
		var notes sql.NullString
		if err := rows.Scan(&item.ID, &item.ItemID, &item.MunicipiID, &item.MunicipiNom, &item.Version, &item.Status,
			&item.Titol, &item.Tag, &dataRef, &item.Text, &fontURL, &notes, &item.LockVersion,
			&item.CreatedBy, &item.CreatedAt, &item.UpdatedAt, &item.ModeratedBy, &item.ModeratedAt); err != nil {
			return nil, 0, err
		}
		item.DataRef = dataRef.String
		item.FontURL = fontURL.String
		item.ModerationNotes = notes.String
		res = append(res, item)
	}
	return res, total, nil
}

func (h sqlHelper) approveMunicipiAnecdotariVersion(versionID int, moderatorID int) error {
	if versionID <= 0 {
		return errors.New("version_id invalid")
	}
	if moderatorID <= 0 {
		return errors.New("moderator_id invalid")
	}
	query := `
        UPDATE municipi_anecdotari_versions
        SET status = 'publicat', moderation_notes = '', moderated_by = ?, moderated_at = ?, updated_at = ` + h.nowFun + `
        WHERE id = ? AND status = 'pendent'`
	query = formatPlaceholders(h.style, query)
	res, err := h.db.Exec(query, moderatorID, time.Now(), versionID)
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return ErrConflict
	}
	query = formatPlaceholders(h.style, `SELECT item_id FROM municipi_anecdotari_versions WHERE id = ?`)
	var itemID int
	if err := h.db.QueryRow(query, versionID).Scan(&itemID); err != nil {
		return err
	}
	updateQuery := `
        UPDATE municipi_anecdotari_items
        SET current_version_id = ?, updated_at = ` + h.nowFun + `
        WHERE id = ?`
	updateQuery = formatPlaceholders(h.style, updateQuery)
	_, err = h.db.Exec(updateQuery, versionID, itemID)
	return err
}

func (h sqlHelper) rejectMunicipiAnecdotariVersion(versionID int, moderatorID int, notes string) error {
	if versionID <= 0 {
		return errors.New("version_id invalid")
	}
	if moderatorID <= 0 {
		return errors.New("moderator_id invalid")
	}
	notesVal := strings.TrimSpace(notes)
	query := `
        UPDATE municipi_anecdotari_versions
        SET status = 'rebutjat', moderation_notes = ?, moderated_by = ?, moderated_at = ?, updated_at = ` + h.nowFun + `
        WHERE id = ? AND status = 'pendent'`
	query = formatPlaceholders(h.style, query)
	res, err := h.db.Exec(query, notesVal, moderatorID, time.Now(), versionID)
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return ErrConflict
	}
	return nil
}

func (h sqlHelper) createMunicipiAnecdotariComment(itemID int, userID int, body string) (int, error) {
	if itemID <= 0 {
		return 0, errors.New("item_id invalid")
	}
	if userID <= 0 {
		return 0, errors.New("user_id invalid")
	}
	bodyVal := strings.TrimSpace(body)
	if bodyVal == "" {
		return 0, errors.New("body empty")
	}
	query := `
        INSERT INTO municipi_anecdotari_comments (item_id, user_id, body, created_at)
        VALUES (?, ?, ?, ` + h.nowFun + `)`
	if h.style == "postgres" {
		query += " RETURNING id"
	}
	query = formatPlaceholders(h.style, query)
	if h.style == "postgres" {
		var id int
		if err := h.db.QueryRow(query, itemID, userID, bodyVal).Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}
	res, err := h.db.Exec(query, itemID, userID, bodyVal)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		return int(id), nil
	}
	return 0, nil
}

func (h sqlHelper) getMunicipiAnecdotariLastCommentAt(userID int) (time.Time, error) {
	if userID <= 0 {
		return time.Time{}, errors.New("user_id invalid")
	}
	query := `
        SELECT created_at
        FROM municipi_anecdotari_comments
        WHERE user_id = ?
        ORDER BY created_at DESC
        LIMIT 1`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, userID)
	var last sql.NullTime
	if err := row.Scan(&last); err != nil {
		if err == sql.ErrNoRows {
			return time.Time{}, nil
		}
		return time.Time{}, err
	}
	if !last.Valid {
		return time.Time{}, nil
	}
	return last.Time, nil
}

func (h sqlHelper) resolveMunicipiIDByAnecdotariItemID(itemID int) (int, error) {
	if itemID <= 0 {
		return 0, errors.New("item_id invalid")
	}
	query := formatPlaceholders(h.style, `SELECT municipi_id FROM municipi_anecdotari_items WHERE id = ?`)
	var munID int
	if err := h.db.QueryRow(query, itemID).Scan(&munID); err != nil {
		return 0, err
	}
	return munID, nil
}

func (h sqlHelper) resolveMunicipiIDByAnecdotariVersionID(versionID int) (int, error) {
	if versionID <= 0 {
		return 0, errors.New("version_id invalid")
	}
	query := `
        SELECT i.municipi_id
        FROM municipi_anecdotari_versions v
        JOIN municipi_anecdotari_items i ON i.id = v.item_id
        WHERE v.id = ?`
	query = formatPlaceholders(h.style, query)
	var munID int
	if err := h.db.QueryRow(query, versionID).Scan(&munID); err != nil {
		return 0, err
	}
	return munID, nil
}
