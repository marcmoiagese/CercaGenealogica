package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
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

// Policies
func (h sqlHelper) ensureDefaultPolicies() error {
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
	// Bootstrap: assigna admin al primer usuari si no hi ha cap assignació
	var assignCount int
	if err := h.db.QueryRow("SELECT COUNT(*) FROM usuaris_politiques").Scan(&assignCount); err == nil && assignCount == 0 {
		var userID int
		if err := h.db.QueryRow("SELECT id FROM usuaris ORDER BY id ASC LIMIT 1").Scan(&userID); err == nil {
			var policyID int
			if err := h.db.QueryRow(formatPlaceholders(h.style, "SELECT id FROM politiques WHERE nom = ?"), "admin").Scan(&policyID); err == nil {
				stmt := formatPlaceholders(h.style, "INSERT INTO usuaris_politiques (usuari_id, politica_id) VALUES (?, ?)")
				_, _ = h.db.Exec(stmt, userID, policyID)
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
	stmt := `UPDATE municipis SET moderation_status = ?, moderation_notes = ?, moderated_by = ?, moderated_at = ?, updated_at = ? WHERE id = ?`
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
	if f.PaisID > 0 {
		where += " AND n.pais_id = ?"
		args = append(args, f.PaisID)
	}
	if f.Nivel > 0 {
		where += " AND n.nivel = ?"
		args = append(args, f.Nivel)
	}
	if strings.TrimSpace(f.Estat) != "" {
		where += " AND n.estat = ?"
		args = append(args, strings.TrimSpace(f.Estat))
	}
	if strings.TrimSpace(f.Status) != "" {
		where += " AND n.moderation_status = ?"
		args = append(args, strings.TrimSpace(f.Status))
	}
	query := `
        SELECT n.id, n.pais_id, n.nivel, n.nom_nivell, n.tipus_nivell, n.codi_oficial, n.altres,
               n.parent_id, p.nom_nivell as parent_nom, n.any_inici, n.any_fi, n.estat,
               n.created_by, n.moderation_status, n.moderated_by, n.moderated_at, n.moderation_notes
        FROM nivells_administratius n
        LEFT JOIN nivells_administratius p ON p.id = n.parent_id
        WHERE ` + where + `
        ORDER BY n.nivel, n.nom_nivell`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []NivellAdministratiu
	for rows.Next() {
		var n NivellAdministratiu
		if err := rows.Scan(&n.ID, &n.PaisID, &n.Nivel, &n.NomNivell, &n.TipusNivell, &n.CodiOficial, &n.Altres, &n.ParentID, &n.ParentNom, &n.AnyInici, &n.AnyFi, &n.Estat,
			&n.CreatedBy, &n.ModeracioEstat, &n.ModeratedBy, &n.ModeratedAt, &n.ModeracioMotiu); err != nil {
			return nil, err
		}
		res = append(res, n)
	}
	return res, nil
}

func (h sqlHelper) getNivell(id int) (*NivellAdministratiu, error) {
	query := `
        SELECT n.id, n.pais_id, n.nivel, n.nom_nivell, n.tipus_nivell, n.codi_oficial, n.altres,
               n.parent_id, p.nom_nivell as parent_nom, n.any_inici, n.any_fi, n.estat,
               n.created_by, n.moderation_status, n.moderated_by, n.moderated_at, n.moderation_notes
        FROM nivells_administratius n
        LEFT JOIN nivells_administratius p ON p.id = n.parent_id
        WHERE n.id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, id)
	var n NivellAdministratiu
	if err := row.Scan(&n.ID, &n.PaisID, &n.Nivel, &n.NomNivell, &n.TipusNivell, &n.CodiOficial, &n.Altres, &n.ParentID, &n.ParentNom, &n.AnyInici, &n.AnyFi, &n.Estat,
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
	query := `
        SELECT m.id, m.nom, m.tipus, m.estat, m.codi_postal,
               na1.nom_nivell AS pais_nom,
               na3.nom_nivell AS provincia_nom,
               na4.nom_nivell AS comarca_nom
        FROM municipis m
        LEFT JOIN nivells_administratius na1 ON na1.id = m.nivell_administratiu_id_1
        LEFT JOIN nivells_administratius na3 ON na3.id = m.nivell_administratiu_id_3
        LEFT JOIN nivells_administratius na4 ON na4.id = m.nivell_administratiu_id_4
        WHERE ` + where + `
        ORDER BY m.nom`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []MunicipiRow
	for rows.Next() {
		var r MunicipiRow
		if err := rows.Scan(&r.ID, &r.Nom, &r.Tipus, &r.Estat, &r.CodiPostal, &r.PaisNom, &r.ProvNom, &r.Comarca); err != nil {
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
        INSERT INTO municipis
            (nom, municipi_id, tipus, nivell_administratiu_id_1, nivell_administratiu_id_2, nivell_administratiu_id_3,
             nivell_administratiu_id_4, nivell_administratiu_id_5, nivell_administratiu_id_6, nivell_administratiu_id_7,
            codi_postal, latitud, longitud, what3words, web, wikipedia, altres, estat, created_by, moderation_status, moderation_notes, data_creacio, ultima_modificacio)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		query += ` RETURNING id`
	}
	query = formatPlaceholders(h.style, query)
	args := []interface{}{
		m.Nom, m.MunicipiID, m.Tipus,
		m.NivellAdministratiuID[0], m.NivellAdministratiuID[1], m.NivellAdministratiuID[2],
		m.NivellAdministratiuID[3], m.NivellAdministratiuID[4], m.NivellAdministratiuID[5], m.NivellAdministratiuID[6],
		m.CodiPostal, m.Latitud, m.Longitud, m.What3Words, m.Web, m.Wikipedia, m.Altres, m.Estat, m.CreatedBy, m.ModeracioEstat, m.ModeracioMotiu,
	}
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
	query := `
        SELECT a.id, a.nom, a.tipus_entitat, p.codi_iso3, a.nivell, parent.nom as parent_nom, a.any_inici, a.any_fi
        FROM arquebisbats a
        LEFT JOIN paisos p ON p.id = a.pais_id
        LEFT JOIN arquebisbats parent ON parent.id = a.parent_id
        WHERE ` + where + `
        ORDER BY a.nom`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []ArquebisbatRow
	for rows.Next() {
		var r ArquebisbatRow
		if err := rows.Scan(&r.ID, &r.Nom, &r.TipusEntitat, &r.PaisNom, &r.Nivell, &r.ParentNom, &r.AnyInici, &r.AnyFi); err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
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

// Arxius
func (h sqlHelper) listArxius(filter ArxiuFilter) ([]ArxiuWithCount, error) {
	h.ensureUserExtraColumns() // no-op but keeps migrations consistent
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
	if strings.TrimSpace(filter.Status) != "" {
		clauses = append(clauses, "a.moderation_status = ?")
		args = append(args, strings.TrimSpace(filter.Status))
	}
	limit := 50
	offset := 0
	if filter.Limit > 0 {
		limit = filter.Limit
	}
	if filter.Offset > 0 {
		offset = filter.Offset
	}
	args = append(args, limit, offset)
	query := `
        SELECT a.id, a.nom, a.tipus, a.municipi_id, a.entitat_eclesiastica_id, a.adreca, a.ubicacio, a.web, a.acces, a.notes,
               a.created_by, a.moderation_status, a.moderated_by, a.moderated_at, a.moderation_notes,
               m.nom as municipi_nom, ae.nom as entitat_nom,
               COALESCE(cnt.total, 0) AS llibres
        FROM arxius a
        LEFT JOIN municipis m ON m.id = a.municipi_id
        LEFT JOIN arquebisbats ae ON ae.id = a.entitat_eclesiastica_id
        LEFT JOIN (
            SELECT arxiu_id, COUNT(*) as total FROM arxius_llibres GROUP BY arxiu_id
        ) cnt ON cnt.arxiu_id = a.id
        WHERE ` + strings.Join(clauses, " AND ") + `
        ORDER BY a.nom ASC
        LIMIT ? OFFSET ?`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []ArxiuWithCount
	for rows.Next() {
		var a ArxiuWithCount
		if err := rows.Scan(&a.ID, &a.Nom, &a.Tipus, &a.MunicipiID, &a.EntitatEclesiasticaID, &a.Adreca, &a.Ubicacio, &a.Web, &a.Acces, &a.Notes,
			&a.CreatedBy, &a.ModeracioEstat, &a.ModeratedBy, &a.ModeratedAt, &a.ModeracioMotiu,
			&a.MunicipiNom, &a.EntitatNom, &a.Llibres); err != nil {
			return nil, err
		}
		res = append(res, a)
	}
	return res, nil
}

func (h sqlHelper) getArxiu(id int) (*Arxiu, error) {
	query := formatPlaceholders(h.style, `
        SELECT id, nom, tipus, municipi_id, entitat_eclesiastica_id, adreca, ubicacio, web, acces, notes,
               created_by, moderation_status, moderated_by, moderated_at, moderation_notes
        FROM arxius WHERE id = ?`)
	row := h.db.QueryRow(query, id)
	var a Arxiu
	if err := row.Scan(&a.ID, &a.Nom, &a.Tipus, &a.MunicipiID, &a.EntitatEclesiasticaID, &a.Adreca, &a.Ubicacio, &a.Web, &a.Acces, &a.Notes,
		&a.CreatedBy, &a.ModeracioEstat, &a.ModeratedBy, &a.ModeratedAt, &a.ModeracioMotiu); err != nil {
		return nil, err
	}
	return &a, nil
}

func (h sqlHelper) createArxiu(a *Arxiu) (int, error) {
	args := []interface{}{a.Nom, a.Tipus, a.MunicipiID, a.EntitatEclesiasticaID, a.Adreca, a.Ubicacio, a.Web, a.Acces, a.Notes, a.CreatedBy, a.ModeracioEstat, a.ModeratedBy, a.ModeratedAt, a.ModeracioMotiu}
	if h.style == "postgres" {
		query := `
            INSERT INTO arxius (nom, tipus, municipi_id, entitat_eclesiastica_id, adreca, ubicacio, web, acces, notes, created_by, moderation_status, moderated_by, moderated_at, moderation_notes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)
            RETURNING id`
		query = formatPlaceholders(h.style, query)
		if err := h.db.QueryRow(query, args...).Scan(&a.ID); err != nil {
			return 0, err
		}
		return a.ID, nil
	}

	query := `
        INSERT INTO arxius (nom, tipus, municipi_id, entitat_eclesiastica_id, adreca, ubicacio, web, acces, notes, created_by, moderation_status, moderated_by, moderated_at, moderation_notes, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
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
	query := `
        UPDATE arxius
        SET nom = ?, tipus = ?, municipi_id = ?, entitat_eclesiastica_id = ?, adreca = ?, ubicacio = ?, web = ?, acces = ?, notes = ?,
            moderation_status = ?, moderated_by = ?, moderated_at = ?, moderation_notes = ?, updated_at = ` + h.nowFun + `
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query, a.Nom, a.Tipus, a.MunicipiID, a.EntitatEclesiasticaID, a.Adreca, a.Ubicacio, a.Web, a.Acces, a.Notes, a.ModeracioEstat, a.ModeratedBy, a.ModeratedAt, a.ModeracioMotiu, a.ID)
	return err
}

func (h sqlHelper) deleteArxiu(id int) error {
	stmt := formatPlaceholders(h.style, `DELETE FROM arxius WHERE id = ?`)
	_, err := h.db.Exec(stmt, id)
	return err
}

func (h sqlHelper) listArxiuLlibres(arxiuID int) ([]ArxiuLlibreDetail, error) {
	query := `
        SELECT al.arxiu_id, al.llibre_id, al.signatura, al.url_override,
               l.titol, l.nom_esglesia, l.cronologia, m.nom as municipi, a.nom as arxiu_nom
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
		if err := rows.Scan(&d.ArxiuID, &d.LlibreID, &d.Signatura, &d.URLOverride, &d.Titol, &d.NomEsglesia, &d.Cronologia, &d.Municipi, &d.ArxiuNom); err != nil {
			return nil, err
		}
		res = append(res, d)
	}
	return res, nil
}

func (h sqlHelper) listLlibreArxius(llibreID int) ([]ArxiuLlibreDetail, error) {
	query := `
        SELECT al.arxiu_id, al.llibre_id, al.signatura, al.url_override,
               a.nom as arxiu_nom, m.nom as municipi
        FROM arxius_llibres al
        INNER JOIN arxius a ON a.id = al.arxiu_id
        LEFT JOIN municipis m ON m.id = a.municipi_id
        WHERE al.llibre_id = ?
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
	if strings.TrimSpace(filter.Status) != "" {
		clauses = append(clauses, "l.moderation_status = ?")
		args = append(args, strings.TrimSpace(filter.Status))
	}
	query := `
        SELECT l.id, l.arquevisbat_id, l.municipi_id, l.nom_esglesia, l.codi_digital, l.codi_fisic,
               l.titol, l.cronologia, l.volum, l.abat, l.contingut, l.llengua,
               l.requeriments_tecnics, l.unitat_catalogacio, l.unitat_instalacio, l.pagines,
               l.url_base, l.url_imatge_prefix, l.pagina,
               l.created_by, l.moderation_status, l.moderated_by, l.moderated_at, l.moderation_notes,
               ae.nom as arquebisbat_nom, m.nom as municipi_nom
        FROM llibres l
        LEFT JOIN arquebisbats ae ON ae.id = l.arquevisbat_id
        LEFT JOIN municipis m ON m.id = l.municipi_id
        WHERE ` + strings.Join(clauses, " AND ") + `
        ORDER BY l.titol`
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
			&lr.Titol, &lr.Cronologia, &lr.Volum, &lr.Abat, &lr.Contingut, &lr.Llengua,
			&lr.Requeriments, &lr.UnitatCatalogacio, &lr.UnitatInstalacio, &lr.Pagines,
			&lr.URLBase, &lr.URLImatgePrefix, &lr.Pagina,
			&lr.CreatedBy, &lr.ModeracioEstat, &lr.ModeratedBy, &lr.ModeratedAt, &lr.ModeracioMotiu,
			&lr.ArquebisbatNom, &lr.MunicipiNom,
		); err != nil {
			return nil, err
		}
		res = append(res, lr)
	}
	return res, nil
}

func (h sqlHelper) getLlibre(id int) (*Llibre, error) {
	query := `
        SELECT id, arquevisbat_id, municipi_id, nom_esglesia, codi_digital, codi_fisic,
               titol, cronologia, volum, abat, contingut, llengua,
               requeriments_tecnics, unitat_catalogacio, unitat_instalacio, pagines,
               url_base, url_imatge_prefix, pagina,
               created_by, moderation_status, moderated_by, moderated_at, moderation_notes
        FROM llibres WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	row := h.db.QueryRow(query, id)
	var l Llibre
	if err := row.Scan(
		&l.ID, &l.ArquebisbatID, &l.MunicipiID, &l.NomEsglesia, &l.CodiDigital, &l.CodiFisic,
		&l.Titol, &l.Cronologia, &l.Volum, &l.Abat, &l.Contingut, &l.Llengua,
		&l.Requeriments, &l.UnitatCatalogacio, &l.UnitatInstalacio, &l.Pagines,
		&l.URLBase, &l.URLImatgePrefix, &l.Pagina,
		&l.CreatedBy, &l.ModeracioEstat, &l.ModeratedBy, &l.ModeratedAt, &l.ModeracioMotiu,
	); err != nil {
		return nil, err
	}
	return &l, nil
}

func (h sqlHelper) createLlibre(l *Llibre) (int, error) {
	query := `
        INSERT INTO llibres
            (arquevisbat_id, municipi_id, nom_esglesia, codi_digital, codi_fisic, titol, cronologia, volum, abat, contingut, llengua,
             requeriments_tecnics, unitat_catalogacio, unitat_instalacio, pagines, url_base, url_imatge_prefix, pagina,
             created_by, moderation_status, moderated_by, moderated_at, moderation_notes, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ` + h.nowFun + `, ` + h.nowFun + `)`
	if h.style == "postgres" {
		query += ` RETURNING id`
	}
	query = formatPlaceholders(h.style, query)
	args := []interface{}{
		l.ArquebisbatID, l.MunicipiID, l.NomEsglesia, l.CodiDigital, l.CodiFisic, l.Titol, l.Cronologia, l.Volum, l.Abat, l.Contingut, l.Llengua,
		l.Requeriments, l.UnitatCatalogacio, l.UnitatInstalacio, l.Pagines, l.URLBase, l.URLImatgePrefix, l.Pagina,
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
        SET arquevisbat_id=?, municipi_id=?, nom_esglesia=?, codi_digital=?, codi_fisic=?, titol=?, cronologia=?, volum=?, abat=?, contingut=?, llengua=?,
            requeriments_tecnics=?, unitat_catalogacio=?, unitat_instalacio=?, pagines=?, url_base=?, url_imatge_prefix=?, pagina=?,
            moderation_status=?, moderated_by=?, moderated_at=?, moderation_notes=?, updated_at=` + h.nowFun + `
        WHERE id = ?`
	query = formatPlaceholders(h.style, query)
	_, err := h.db.Exec(query,
		l.ArquebisbatID, l.MunicipiID, l.NomEsglesia, l.CodiDigital, l.CodiFisic, l.Titol, l.Cronologia, l.Volum, l.Abat, l.Contingut, l.Llengua,
		l.Requeriments, l.UnitatCatalogacio, l.UnitatInstalacio, l.Pagines, l.URLBase, l.URLImatgePrefix, l.Pagina,
		l.ModeracioEstat, l.ModeratedBy, l.ModeratedAt, l.ModeracioMotiu, l.ID)
	return err
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
