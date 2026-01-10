package core

import (
	"database/sql"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func (a *App) AdminListEclesiastic(w http.ResponseWriter, r *http.Request) {
	filter := db.ArquebisbatFilter{
		Text: strings.TrimSpace(r.URL.Query().Get("q")),
	}
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	if status == "" {
		status = "publicat"
	}
	filter.Status = status
	if pid := strings.TrimSpace(r.URL.Query().Get("pais_id")); pid != "" {
		if v, err := strconv.Atoi(pid); err == nil {
			filter.PaisID = v
		}
	}
	perPage := parseListPerPage(r.URL.Query().Get("per_page"))
	page := parseListPage(r.URL.Query().Get("page"))
	user, ok := a.requirePermissionKeyAnyScope(w, r, permKeyTerritoriEclesView)
	if !ok {
		return
	}
	perms := a.getPermissionsForUser(user.ID)
	scopeFilter := a.buildListScopeFilter(user.ID, permKeyTerritoriEclesView, ScopeEcles)
	createPaisID := filter.PaisID
	canCreateEcles := false
	if createPaisID > 0 {
		canCreateEcles = a.HasPermission(user.ID, permKeyTerritoriEclesCreate, PermissionTarget{PaisID: intPtr(createPaisID)})
	} else {
		canCreateEcles = a.HasPermission(user.ID, permKeyTerritoriEclesCreate, PermissionTarget{})
	}
	canImportEcles := a.HasPermission(user.ID, permKeyAdminEclesImport, PermissionTarget{})
	if !scopeFilter.hasGlobal {
		if scopeFilter.isEmpty() {
			pagination := buildPagination(r, page, perPage, 0, "#page-stats-controls")
			RenderPrivateTemplate(w, r, "admin-eclesiastic-list.html", map[string]interface{}{
				"Entitats":         []db.ArquebisbatRow{},
				"Filter":           filter,
				"Paisos":           []db.Pais{},
				"CanManageArxius":  a.hasPerm(perms, permArxius),
				"CanCreateEcles":   canCreateEcles,
				"CreatePaisID":     createPaisID,
				"CanImportEcles":   canImportEcles,
				"CanEditEcles":     map[int]bool{},
				"ShowEclesActions": false,
				"Page":             pagination.Page,
				"PerPage":          pagination.PerPage,
				"Total":            pagination.Total,
				"TotalPages":       pagination.TotalPages,
				"PageLinks":        pagination.Links,
				"PageSelectBase":   pagination.SelectBase,
				"PageAnchor":       pagination.Anchor,
				"User":             user,
			})
			return
		}
		filter.AllowedEclesIDs = scopeFilter.eclesIDs
		filter.AllowedPaisIDs = scopeFilter.paisIDs
	}
	total, _ := a.DB.CountArquebisbats(filter)
	pagination := buildPagination(r, page, perPage, total, "#page-stats-controls")
	filter.Limit = pagination.PerPage
	filter.Offset = pagination.Offset
	entitats, _ := a.DB.ListArquebisbats(filter)
	canEditEcles := make(map[int]bool, len(entitats))
	showEclesActions := false
	for _, ent := range entitats {
		entTarget := PermissionTarget{EclesID: intPtr(ent.ID)}
		canEdit := a.HasPermission(user.ID, permKeyTerritoriEclesEdit, entTarget)
		canEditEcles[ent.ID] = canEdit
		if canEdit {
			showEclesActions = true
		}
	}
	paisos, _ := a.DB.ListPaisos()
	if !scopeFilter.hasGlobal && len(scopeFilter.paisIDs) > 0 {
		allowed := map[int]struct{}{}
		for _, id := range scopeFilter.paisIDs {
			allowed[id] = struct{}{}
		}
		filtered := make([]db.Pais, 0, len(paisos))
		for _, pais := range paisos {
			if _, ok := allowed[pais.ID]; ok {
				filtered = append(filtered, pais)
			}
		}
		paisos = filtered
	}
	RenderPrivateTemplate(w, r, "admin-eclesiastic-list.html", map[string]interface{}{
		"Entitats":         entitats,
		"Filter":           filter,
		"Paisos":           paisos,
		"CanManageArxius":  a.hasPerm(perms, permArxius),
		"CanCreateEcles":   canCreateEcles,
		"CreatePaisID":     createPaisID,
		"CanImportEcles":   canImportEcles,
		"CanEditEcles":     canEditEcles,
		"ShowEclesActions": showEclesActions,
		"Page":             pagination.Page,
		"PerPage":          pagination.PerPage,
		"Total":            pagination.Total,
		"TotalPages":       pagination.TotalPages,
		"PageLinks":        pagination.Links,
		"PageSelectBase":   pagination.SelectBase,
		"PageAnchor":       pagination.Anchor,
		"User":             user,
	})
}

func (a *App) AdminNewEclesiastic(w http.ResponseWriter, r *http.Request) {
	returnURL := strings.TrimSpace(r.URL.Query().Get("return_to"))
	target := PermissionTarget{}
	if pid := strings.TrimSpace(r.URL.Query().Get("pais_id")); pid != "" {
		if v, err := strconv.Atoi(pid); err == nil && v > 0 {
			target.PaisID = intPtr(v)
		}
	}
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriEclesCreate, target)
	if !ok {
		return
	}
	paisos, _ := a.DB.ListPaisos()
	RenderPrivateTemplate(w, r, "admin-eclesiastic-form.html", map[string]interface{}{
		"Entitat":         &db.Arquebisbat{TipusEntitat: "bisbat", ModeracioEstat: "pendent"},
		"Paisos":          paisos,
		"Parents":         nil,
		"ReturnURL":       returnURL,
		"IsNew":           true,
		"CanManageArxius": true,
		"User":            user,
	})
}

func (a *App) AdminEditEclesiastic(w http.ResponseWriter, r *http.Request) {
	returnURL := strings.TrimSpace(r.URL.Query().Get("return_to"))
	id := extractID(r.URL.Path)
	ent, err := a.DB.GetArquebisbat(id)
	if err != nil || ent == nil {
		http.NotFound(w, r)
		return
	}
	target := PermissionTarget{EclesID: intPtr(id)}
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriEclesEdit, target)
	if !ok {
		return
	}
	paisos, _ := a.DB.ListPaisos()
	parents, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	RenderPrivateTemplate(w, r, "admin-eclesiastic-form.html", map[string]interface{}{
		"Entitat":         ent,
		"Paisos":          paisos,
		"Parents":         parents,
		"ReturnURL":       returnURL,
		"IsNew":           false,
		"CanManageArxius": true,
		"User":            user,
	})
}

func (a *App) AdminSaveEclesiastic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/territori/eclesiastic", http.StatusSeeOther)
		return
	}
	id, _ := strconv.Atoi(r.FormValue("id"))
	permKey := permKeyTerritoriEclesCreate
	if id != 0 {
		permKey = permKeyTerritoriEclesEdit
	}
	returnURL := strings.TrimSpace(r.FormValue("return_to"))
	paisID := sqlNullInt(r.FormValue("pais_id"))
	target := PermissionTarget{}
	if id != 0 {
		target.EclesID = intPtr(id)
	} else if paisID.Valid {
		target.PaisID = intPtr(int(paisID.Int64))
	}
	user, ok := a.requirePermissionKey(w, r, permKey, target)
	if !ok {
		return
	}
	parentID := sqlNullInt(r.FormValue("parent_id"))
	nivell := sqlNullInt(r.FormValue("nivell"))
	anyInici := sqlNullInt(r.FormValue("any_inici"))
	anyFi := sqlNullInt(r.FormValue("any_fi"))
	ent := &db.Arquebisbat{
		ID:             id,
		Nom:            strings.TrimSpace(r.FormValue("nom")),
		TipusEntitat:   strings.TrimSpace(r.FormValue("tipus_entitat")),
		PaisID:         paisID,
		Nivell:         nivell,
		ParentID:       parentID,
		AnyInici:       anyInici,
		AnyFi:          anyFi,
		Web:            strings.TrimSpace(r.FormValue("web")),
		WebArxiu:       strings.TrimSpace(r.FormValue("web_arxiu")),
		WebWikipedia:   strings.TrimSpace(r.FormValue("web_wikipedia")),
		Territori:      strings.TrimSpace(r.FormValue("territori")),
		Observacions:   strings.TrimSpace(r.FormValue("observacions")),
		ModeracioEstat: "pendent",
	}
	if errMsg := validateEclesiastic(ent); errMsg != "" {
		a.renderEclesiasticError(w, r, ent, errMsg, id == 0)
		return
	}
	var saveErr error
	isNew := ent.ID == 0
	if isNew {
		ent.CreatedBy = sqlNullIntFromInt(user.ID)
		ent.ModeratedBy = sql.NullInt64{}
		ent.ModeratedAt = sql.NullTime{}
		ent.ModeracioMotiu = ""
		if newID, err := a.DB.CreateArquebisbat(ent); err == nil {
			ent.ID = newID
		} else {
			saveErr = err
		}
	} else {
		ent.ModeratedBy = sql.NullInt64{}
		ent.ModeratedAt = sql.NullTime{}
		ent.ModeracioMotiu = ""
		saveErr = a.DB.UpdateArquebisbat(ent)
	}
	if saveErr != nil {
		a.renderEclesiasticError(w, r, ent, "No s'ha pogut desar l'entitat.", id == 0)
		return
	}
	rule := ruleEclesiasticCreate
	action := "crear"
	if !isNew {
		rule = ruleEclesiasticUpdate
		action = "editar"
	}
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, rule, action, "eclesiastic", &ent.ID, "pendent", nil, "")
	if returnURL != "" {
		http.Redirect(w, r, returnURL, http.StatusSeeOther)
	} else {
		http.Redirect(w, r, "/territori/eclesiastic", http.StatusSeeOther)
	}
}

func validateEclesiastic(e *db.Arquebisbat) string {
	if strings.TrimSpace(e.Nom) == "" {
		return "El nom és obligatori."
	}
	if e.TipusEntitat == "" {
		return "El tipus d'entitat és obligatori."
	}
	if e.ParentID.Valid && e.ID != 0 && e.ParentID.Int64 == int64(e.ID) {
		return "L'entitat no pot ser pare de si mateixa."
	}
	return ""
}

func (a *App) renderEclesiasticError(w http.ResponseWriter, r *http.Request, e *db.Arquebisbat, msg string, isNew bool) {
	paisos, _ := a.DB.ListPaisos()
	parents, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	RenderPrivateTemplate(w, r, "admin-eclesiastic-form.html", map[string]interface{}{
		"Entitat":         e,
		"Paisos":          paisos,
		"Parents":         parents,
		"Error":           msg,
		"IsNew":           isNew,
		"ReturnURL":       strings.TrimSpace(r.FormValue("return_to")),
		"CanManageArxius": true,
	})
}
