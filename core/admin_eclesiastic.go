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
	lang := ResolveLang(r)
	perPage := parseListPerPage(r.URL.Query().Get("per_page"))
	page := parseListPage(r.URL.Query().Get("page"))
	filterKeys := []string{"nom", "tipus", "pais", "nivell", "parent", "anys", "status"}
	filterValues := map[string]string{}
	filterMatch := map[string]string{}
	for _, key := range filterKeys {
		paramKey := "f_" + key
		if val := strings.TrimSpace(r.URL.Query().Get(paramKey)); val != "" {
			filterValues[key] = val
			filterMatch[key] = strings.ToLower(val)
		}
	}
	filterOrder := []string{}
	if orderParam := strings.TrimSpace(r.URL.Query().Get("order")); orderParam != "" {
		for _, key := range strings.Split(orderParam, ",") {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			if _, ok := filterMatch[key]; ok {
				filterOrder = append(filterOrder, key)
			}
		}
	}
	if len(filterOrder) == 0 {
		for _, key := range filterKeys {
			if _, ok := filterMatch[key]; ok {
				filterOrder = append(filterOrder, key)
			}
		}
	} else {
		seen := map[string]bool{}
		for _, key := range filterOrder {
			seen[key] = true
		}
		for _, key := range filterKeys {
			if _, ok := filterMatch[key]; ok && !seen[key] {
				filterOrder = append(filterOrder, key)
			}
		}
	}
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
			pagination := buildPagination(r, page, perPage, 0, "#eclesTable")
			RenderPrivateTemplate(w, r, "admin-eclesiastic-list.html", map[string]interface{}{
				"Entitats":         []db.ArquebisbatRow{},
				"Filter":           filter,
				"FilterValues":     filterValues,
				"FilterOrder":      strings.Join(filterOrder, ","),
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
	entitats := []db.ArquebisbatRow{}
	total := 0
	pagination := Pagination{}
	filtered := len(filterMatch) > 0
	if filtered {
		listFilter := filter
		listFilter.Limit = 0
		listFilter.Offset = 0
		allEntitats, _ := a.DB.ListArquebisbats(listFilter)
		matches := make([]db.ArquebisbatRow, 0, len(allEntitats))
		for _, ent := range allEntitats {
			match := true
			for _, key := range filterOrder {
				filterVal := filterMatch[key]
				if filterVal == "" {
					continue
				}
				value := strings.ToLower(eclesFilterValue(ent, key, lang))
				if !strings.Contains(value, filterVal) {
					match = false
					break
				}
			}
			if match {
				matches = append(matches, ent)
			}
		}
		total = len(matches)
		pagination = buildPagination(r, page, perPage, total, "#eclesTable")
		start := pagination.Offset
		end := start + pagination.PerPage
		if start < 0 {
			start = 0
		}
		if start > total {
			start = total
		}
		if end > total {
			end = total
		}
		entitats = matches[start:end]
	} else {
		total, _ = a.DB.CountArquebisbats(filter)
		pagination = buildPagination(r, page, perPage, total, "#eclesTable")
		listFilter := filter
		listFilter.Limit = pagination.PerPage
		listFilter.Offset = pagination.Offset
		entitats, _ = a.DB.ListArquebisbats(listFilter)
	}
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
		"FilterValues":     filterValues,
		"FilterOrder":      strings.Join(filterOrder, ","),
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

func eclesYearsLabel(e db.ArquebisbatRow) string {
	start := ""
	end := ""
	if e.AnyInici.Valid {
		start = strconv.FormatInt(e.AnyInici.Int64, 10)
	}
	if e.AnyFi.Valid {
		end = strconv.FormatInt(e.AnyFi.Int64, 10)
	}
	if start != "" && end != "" {
		return start + " - " + end
	}
	return start + end
}

func eclesFilterValue(e db.ArquebisbatRow, key, lang string) string {
	switch key {
	case "nom":
		return e.Nom
	case "tipus":
		return e.TipusEntitat
	case "pais":
		if e.PaisNom.Valid {
			return e.PaisNom.String
		}
	case "nivell":
		if e.Nivell.Valid {
			return strconv.FormatInt(e.Nivell.Int64, 10)
		}
	case "parent":
		if e.ParentNom.Valid {
			return e.ParentNom.String
		}
	case "anys":
		return eclesYearsLabel(e)
	case "status":
		if e.ModeracioEstat != "" {
			return T(lang, "activity.status."+e.ModeracioEstat)
		}
	}
	return ""
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
