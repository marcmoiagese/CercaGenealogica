package core

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type arxiuProBook struct {
	ID          int    `json:"id"`
	Titol       string `json:"titol"`
	NomEsglesia string `json:"nom_esglesia,omitempty"`
	Tipus       string `json:"tipus,omitempty"`
	Cronologia  string `json:"cronologia,omitempty"`
	Municipi    string `json:"municipi,omitempty"`
	Pagines     *int   `json:"pagines,omitempty"`
	Signatura   string `json:"signatura,omitempty"`
	URL         string `json:"url,omitempty"`
	Href        string `json:"href,omitempty"`
	CanEdit     bool   `json:"can_edit,omitempty"`
	CanView     bool   `json:"can_view,omitempty"`
}

type arxiuProMeta struct {
	ID                  int    `json:"id"`
	Nom                 string `json:"nom"`
	Tipus               string `json:"tipus,omitempty"`
	Acces               string `json:"acces,omitempty"`
	MunicipiID          int    `json:"municipi_id,omitempty"`
	MunicipiNom         string `json:"municipi_nom,omitempty"`
	EntitatEclesiastica string `json:"entitat_eclesiastica,omitempty"`
	Adreca              string `json:"adreca,omitempty"`
	Ubicacio            string `json:"ubicacio,omitempty"`
	What3Words          string `json:"what3words,omitempty"`
	Web                 string `json:"web,omitempty"`
	Notes               string `json:"notes,omitempty"`
	Estat               string `json:"estat,omitempty"`
	EstatLabel          string `json:"estat_label,omitempty"`
}

type arxiuProDonacio struct {
	Title string `json:"title,omitempty"`
	Sub   string `json:"sub,omitempty"`
	URL   string `json:"url,omitempty"`
}

type arxiuProData struct {
	Arxiu            arxiuProMeta     `json:"arxiu"`
	Llibres          []arxiuProBook   `json:"llibres"`
	ShowActions      bool             `json:"show_actions,omitempty"`
	AcceptaDonacions bool             `json:"accepta_donacions,omitempty"`
	Donacions        *arxiuProDonacio `json:"donacions,omitempty"`
}

// CanManageArxius és un helper públic per saber si l'usuari pot gestionar arxius.
func (a *App) CanManageArxius(user *db.User) bool {
	if user == nil {
		return false
	}
	perms := a.getPermissionsForUser(user.ID)
	return a.hasPerm(perms, permArxius)
}

// Llistat públic (lectura) d'arxius per a usuaris logats
func (a *App) ListArxius(w http.ResponseWriter, r *http.Request) {
	user, authenticated := a.VerificarSessio(r)
	if !authenticated || user == nil {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	perms := a.getPermissionsForUser(user.ID)
	*r = *a.withPermissions(r, perms)
	canManage := a.hasPerm(perms, permArxius)
	isAdmin := a.hasPerm(perms, permAdmin)
	canManageTerritory := a.hasPerm(perms, permTerritory)
	canManageEclesia := a.hasPerm(perms, permEclesia)
	canModerate := a.hasPerm(perms, permModerate)
	canManageUsers := a.hasPerm(perms, permUsers)
	canManagePolicies := a.hasPerm(perms, permPolicies)
	filter := db.ArxiuFilter{
		Text:  strings.TrimSpace(r.URL.Query().Get("q")),
		Tipus: strings.TrimSpace(r.URL.Query().Get("tipus")),
		Acces: strings.TrimSpace(r.URL.Query().Get("acces")),
	}
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	filter.Status = status
	if v := strings.TrimSpace(r.URL.Query().Get("entitat_id")); v != "" {
		if id, err := strconv.Atoi(v); err == nil {
			filter.EntitatID = id
		}
	}
	lang := ResolveLang(r)
	perPage := parseListPerPage(r.URL.Query().Get("per_page"))
	page := parseListPage(r.URL.Query().Get("page"))
	filterKeys := []string{"nom", "tipus", "acces", "entitat", "municipi", "web", "llibres", "status"}
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
	arxius := []db.ArxiuWithCount{}
	total := 0
	pagination := Pagination{}
	filtered := len(filterMatch) > 0
	if filtered {
		listFilter := filter
		listFilter.Limit = -1
		allArxius, _ := a.DB.ListArxius(listFilter)
		matches := make([]db.ArxiuWithCount, 0, len(allArxius))
		for _, arxiu := range allArxius {
			match := true
			for _, key := range filterOrder {
				filterVal := filterMatch[key]
				if filterVal == "" {
					continue
				}
				value := strings.ToLower(arxiuFilterValue(arxiu, key, lang))
				if !strings.Contains(value, filterVal) {
					match = false
					break
				}
			}
			if match {
				matches = append(matches, arxiu)
			}
		}
		total = len(matches)
		pagination = buildPagination(r, page, perPage, total, "#arxiusTable")
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
		arxius = matches[start:end]
	} else {
		total, _ = a.DB.CountArxius(filter)
		pagination = buildPagination(r, page, perPage, total, "#arxiusTable")
		listFilter := filter
		listFilter.Limit = pagination.PerPage
		listFilter.Offset = pagination.Offset
		arxius, _ = a.DB.ListArxius(listFilter)
	}
	arquebisbats, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	RenderPrivateTemplate(w, r, "admin-arxius-list.html", map[string]interface{}{
		"Arxius":             arxius,
		"Filter":             filter,
		"FilterValues":       filterValues,
		"FilterOrder":        strings.Join(filterOrder, ","),
		"CanManageArxius":    canManage,
		"ArxiusBasePath":     "/arxius",
		"Arquebisbats":       arquebisbats,
		"User":               user,
		"IsAdmin":            isAdmin,
		"CanManageTerritory": canManageTerritory,
		"CanManageEclesia":   canManageEclesia,
		"CanModerate":        canModerate,
		"CanManageUsers":     canManageUsers,
		"CanManagePolicies":  canManagePolicies,
		"Page":               pagination.Page,
		"PerPage":            pagination.PerPage,
		"Total":              pagination.Total,
		"TotalPages":         pagination.TotalPages,
		"PageLinks":          pagination.Links,
		"PageSelectBase":     pagination.SelectBase,
		"PageAnchor":         pagination.Anchor,
	})
}

// Detall en lectura d'un arxiu
func (a *App) ShowArxiu(w http.ResponseWriter, r *http.Request) {
	user, authenticated := a.VerificarSessio(r)
	if !authenticated || user == nil {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	perms := a.getPermissionsForUser(user.ID)
	*r = *a.withPermissions(r, perms)
	canManage := a.hasPerm(perms, permArxius)
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	arxiu, err := a.DB.GetArxiu(id)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if !canManage && arxiu.ModeracioEstat != "publicat" {
		http.NotFound(w, r)
		return
	}
	llibres, _ := a.DB.ListArxiuLlibres(id)
	entNom := a.loadEntitatNom(arxiu)
	munNom := a.loadMunicipiNom(arxiu)
	lang := ResolveLang(r)
	arxiuPro := a.buildArxiuProData(lang, arxiu, entNom, munNom, llibres, nil, nil, false)
	RenderPrivateTemplate(w, r, "admin-arxius-show.html", map[string]interface{}{
		"Arxiu":           arxiu,
		"Llibres":         llibres,
		"EntitatNom":      entNom,
		"MunicipiNom":     munNom,
		"ArxiuProData":    arxiuPro,
		"DonacionsClicks": 0,
		"CanManageArxius": canManage,
		"ArxiusBasePath":  "/arxius",
		"User":            user,
	})
}

func (a *App) ArxiuDonacionsRedirect(w http.ResponseWriter, r *http.Request) {
	user, authenticated := a.VerificarSessio(r)
	if !authenticated || user == nil {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	arxiu, err := a.DB.GetArxiu(id)
	if err != nil || arxiu == nil {
		http.NotFound(w, r)
		return
	}
	if !arxiu.AcceptaDonacions || strings.TrimSpace(arxiu.DonacionsURL) == "" {
		http.NotFound(w, r)
		return
	}
	uid := user.ID
	_ = a.DB.InsertArxiuDonacioClick(id, &uid)
	http.Redirect(w, r, arxiu.DonacionsURL, http.StatusSeeOther)
}

// Admin: llistat d'arxius
func (a *App) AdminListArxius(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKeyAnyScope(w, r, permKeyDocumentalsArxiusView)
	if !ok {
		return
	}
	perms := a.getPermissionsForUser(user.ID)
	canManage := a.hasPerm(perms, permArxius)
	isAdmin := a.hasPerm(perms, permAdmin)
	canManageTerritory := a.hasPerm(perms, permTerritory)
	canManageEclesia := a.hasPerm(perms, permEclesia)
	canModerate := a.hasPerm(perms, permModerate)
	canManageUsers := a.hasPerm(perms, permUsers)
	canManagePolicies := a.hasPerm(perms, permPolicies)
	canCreateArxiu := a.hasAnyPermissionKey(user.ID, permKeyDocumentalsArxiusCreate)
	canImportArxiu := a.HasPermission(user.ID, permKeyAdminArxiusImport, PermissionTarget{})
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	if status == "" {
		status = "publicat"
	}
	lang := ResolveLang(r)
	perPage := parseListPerPage(r.URL.Query().Get("per_page"))
	page := parseListPage(r.URL.Query().Get("page"))
	scopeFilter := a.buildListScopeFilter(user.ID, permKeyDocumentalsArxiusView, ScopeArxiu)
	filter := db.ArxiuFilter{
		Text:   strings.TrimSpace(r.URL.Query().Get("q")),
		Tipus:  strings.TrimSpace(r.URL.Query().Get("tipus")),
		Acces:  strings.TrimSpace(r.URL.Query().Get("acces")),
		Status: status,
	}
	filterKeys := []string{"nom", "tipus", "acces", "entitat", "municipi", "web", "llibres", "status"}
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
	if v := strings.TrimSpace(r.URL.Query().Get("entitat_id")); v != "" {
		if id, err := strconv.Atoi(v); err == nil {
			filter.EntitatID = id
		}
	}
	if !scopeFilter.hasGlobal {
		if scopeFilter.isEmpty() {
			pagination := buildPagination(r, page, perPage, 0, "#arxiusTable")
			RenderPrivateTemplate(w, r, "admin-arxius-list.html", map[string]interface{}{
				"Arxius":             []db.ArxiuWithCount{},
				"Filter":             filter,
				"FilterValues":       filterValues,
				"FilterOrder":        strings.Join(filterOrder, ","),
				"ArxiusBasePath":     "/documentals/arxius",
				"Arquebisbats":       []db.ArquebisbatRow{},
				"CanManageArxius":    canManage,
				"CanCreateArxiu":     canCreateArxiu,
				"CanImportArxiu":     canImportArxiu,
				"CanEditArxiu":       map[int]bool{},
				"CanDeleteArxiu":     map[int]bool{},
				"ShowArxiuActions":   false,
				"Page":               pagination.Page,
				"PerPage":            pagination.PerPage,
				"Total":              pagination.Total,
				"TotalPages":         pagination.TotalPages,
				"PageLinks":          pagination.Links,
				"PageSelectBase":     pagination.SelectBase,
				"PageAnchor":         pagination.Anchor,
				"User":               user,
				"IsAdmin":            isAdmin,
				"CanManageTerritory": canManageTerritory,
				"CanManageEclesia":   canManageEclesia,
				"CanModerate":        canModerate,
				"CanManageUsers":     canManageUsers,
				"CanManagePolicies":  canManagePolicies,
			})
			return
		}
		filter.AllowedArxiuIDs = scopeFilter.arxiuIDs
		filter.AllowedMunicipiIDs = scopeFilter.municipiIDs
		filter.AllowedProvinciaIDs = scopeFilter.provinciaIDs
		filter.AllowedComarcaIDs = scopeFilter.comarcaIDs
		filter.AllowedPaisIDs = scopeFilter.paisIDs
		filter.AllowedEclesIDs = scopeFilter.eclesIDs
	}
	arxius := []db.ArxiuWithCount{}
	total := 0
	pagination := Pagination{}
	filtered := len(filterMatch) > 0
	if filtered {
		listFilter := filter
		listFilter.Limit = -1
		allArxius, _ := a.DB.ListArxius(listFilter)
		matches := make([]db.ArxiuWithCount, 0, len(allArxius))
		for _, arxiu := range allArxius {
			match := true
			for _, key := range filterOrder {
				filterVal := filterMatch[key]
				if filterVal == "" {
					continue
				}
				value := strings.ToLower(arxiuFilterValue(arxiu, key, lang))
				if !strings.Contains(value, filterVal) {
					match = false
					break
				}
			}
			if match {
				matches = append(matches, arxiu)
			}
		}
		total = len(matches)
		pagination = buildPagination(r, page, perPage, total, "#arxiusTable")
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
		arxius = matches[start:end]
	} else {
		total, _ = a.DB.CountArxius(filter)
		pagination = buildPagination(r, page, perPage, total, "#arxiusTable")
		listFilter := filter
		listFilter.Limit = pagination.PerPage
		listFilter.Offset = pagination.Offset
		arxius, _ = a.DB.ListArxius(listFilter)
	}
	canEditArxiu := make(map[int]bool, len(arxius))
	canDeleteArxiu := make(map[int]bool, len(arxius))
	showArxiuActions := false
	for _, arxiu := range arxius {
		target := a.resolveArxiuTarget(arxiu.ID)
		canEdit := a.HasPermission(user.ID, permKeyDocumentalsArxiusEdit, target)
		canDelete := a.HasPermission(user.ID, permKeyDocumentalsArxiusDelete, target)
		canEditArxiu[arxiu.ID] = canEdit
		canDeleteArxiu[arxiu.ID] = canDelete
		if canEdit || canDelete {
			showArxiuActions = true
		}
	}
	arquebisbats, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	RenderPrivateTemplate(w, r, "admin-arxius-list.html", map[string]interface{}{
		"Arxius":             arxius,
		"Filter":             filter,
		"FilterValues":       filterValues,
		"FilterOrder":        strings.Join(filterOrder, ","),
		"ArxiusBasePath":     "/documentals/arxius",
		"Arquebisbats":       arquebisbats,
		"CanManageArxius":    canManage,
		"CanCreateArxiu":     canCreateArxiu,
		"CanImportArxiu":     canImportArxiu,
		"CanEditArxiu":       canEditArxiu,
		"CanDeleteArxiu":     canDeleteArxiu,
		"ShowArxiuActions":   showArxiuActions,
		"Page":               pagination.Page,
		"PerPage":            pagination.PerPage,
		"Total":              pagination.Total,
		"TotalPages":         pagination.TotalPages,
		"PageLinks":          pagination.Links,
		"PageSelectBase":     pagination.SelectBase,
		"PageAnchor":         pagination.Anchor,
		"User":               user,
		"IsAdmin":            isAdmin,
		"CanManageTerritory": canManageTerritory,
		"CanManageEclesia":   canManageEclesia,
		"CanModerate":        canModerate,
		"CanManageUsers":     canManageUsers,
		"CanManagePolicies":  canManagePolicies,
	})
}

func arxiuFilterValue(a db.ArxiuWithCount, key, lang string) string {
	switch key {
	case "nom":
		return a.Nom
	case "tipus":
		if a.Tipus == "" {
			return ""
		}
		return strings.TrimSpace(T(lang, "archives.type."+a.Tipus) + " " + a.Tipus)
	case "acces":
		if a.Acces == "" {
			return ""
		}
		return strings.TrimSpace(T(lang, "archives.access."+a.Acces) + " " + a.Acces)
	case "entitat":
		if a.EntitatNom.Valid {
			return a.EntitatNom.String
		}
	case "municipi":
		if a.MunicipiNom.Valid {
			return a.MunicipiNom.String
		}
	case "web":
		return a.Web
	case "llibres":
		return strconv.Itoa(a.Llibres)
	case "status":
		if a.ModeracioEstat != "" {
			return strings.TrimSpace(T(lang, "activity.status."+a.ModeracioEstat) + " " + a.ModeracioEstat)
		}
	}
	return ""
}

func parseArxiuForm(r *http.Request) *db.Arxiu {
	_ = r.ParseForm()
	municipiID := sqlNullInt(r.FormValue("municipi_id"))
	entitatID := sqlNullInt(r.FormValue("entitat_eclesiastica_id"))
	acceptaDonacions := strings.TrimSpace(r.FormValue("accepta_donacions")) != ""
	donacionsURL := strings.TrimSpace(r.FormValue("donacions_url"))
	if !acceptaDonacions || donacionsURL == "" {
		donacionsURL = ""
		acceptaDonacions = false
	}
	return &db.Arxiu{
		Nom:                   strings.TrimSpace(r.FormValue("nom")),
		Tipus:                 strings.TrimSpace(r.FormValue("tipus")),
		Acces:                 strings.TrimSpace(r.FormValue("acces")),
		MunicipiID:            municipiID,
		EntitatEclesiasticaID: entitatID,
		Adreca:                strings.TrimSpace(r.FormValue("adreca")),
		Ubicacio:              strings.TrimSpace(r.FormValue("ubicacio")),
		What3Words:            strings.TrimSpace(r.FormValue("what3words")),
		Web:                   strings.TrimSpace(r.FormValue("web")),
		Notes:                 strings.TrimSpace(r.FormValue("notes")),
		AcceptaDonacions:      acceptaDonacions,
		DonacionsURL:          donacionsURL,
	}
}

func (a *App) renderArxiuForm(w http.ResponseWriter, r *http.Request, arxiu *db.Arxiu, isNew bool, errMsg string, user *db.User, returnURL string) {
	municipiLabel := a.loadMunicipiNom(arxiu)
	entitatLabel := a.loadEntitatNom(arxiu)
	RenderPrivateTemplate(w, r, "admin-arxius-form.html", map[string]interface{}{
		"Arxiu":           arxiu,
		"IsNew":           isNew,
		"Error":           errMsg,
		"ReturnURL":       returnURL,
		"CanManageArxius": true,
		"MunicipiLabel":   municipiLabel,
		"EntitatLabel":    entitatLabel,
		"User":            user,
	})
}

// Alta
func (a *App) AdminNewArxiu(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKeyAnyScope(w, r, permKeyDocumentalsArxiusCreate)
	if !ok {
		return
	}
	returnURL := strings.TrimSpace(r.URL.Query().Get("return_to"))
	a.renderArxiuForm(w, r, &db.Arxiu{}, true, "", user, returnURL)
}

func (a *App) AdminCreateArxiu(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/documentals/arxius", http.StatusSeeOther)
		return
	}
	arxiu := parseArxiuForm(r)
	target := PermissionTarget{}
	if arxiu.EntitatEclesiasticaID.Valid {
		entitatID := int(arxiu.EntitatEclesiasticaID.Int64)
		if entitatID > 0 {
			target.EclesID = intPtr(entitatID)
		}
	} else if arxiu.MunicipiID.Valid {
		municipiID := int(arxiu.MunicipiID.Int64)
		if municipiID > 0 {
			target = a.resolveMunicipiTarget(municipiID)
		}
	}
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsArxiusCreate, target)
	if !ok {
		return
	}
	returnURL := strings.TrimSpace(r.FormValue("return_to"))
	if arxiu.Nom == "" || len(arxiu.Nom) < 3 {
		a.renderArxiuForm(w, r, arxiu, true, "El nom és obligatori (mínim 3 caràcters).", user, returnURL)
		return
	}
	arxiu.CreatedBy = sqlNullIntFromInt(user.ID)
	arxiu.ModeracioEstat = "pendent"
	arxiu.ModeratedBy = sql.NullInt64{}
	arxiu.ModeratedAt = sql.NullTime{}
	id, err := a.DB.CreateArxiu(arxiu)
	if err != nil {
		a.renderArxiuForm(w, r, arxiu, true, "No s'ha pogut crear l'arxiu.", user, returnURL)
		return
	}
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleArxiuCreate, "crear", "arxiu", &id, "pendent", nil, "")
	if returnURL != "" {
		http.Redirect(w, r, returnURL, http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/documentals/arxius/"+strconv.Itoa(id), http.StatusSeeOther)
}

// Edició
func (a *App) AdminEditArxiu(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	target := a.resolveArxiuTarget(id)
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsArxiusEdit, target)
	if !ok {
		return
	}
	arxiu, err := a.DB.GetArxiu(id)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	returnURL := strings.TrimSpace(r.URL.Query().Get("return_to"))
	a.renderArxiuForm(w, r, arxiu, false, "", user, returnURL)
}

func (a *App) AdminUpdateArxiu(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	target := a.resolveArxiuTarget(id)
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsArxiusEdit, target)
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/documentals/arxius/"+strconv.Itoa(id)+"/edit", http.StatusSeeOther)
		return
	}
	returnURL := strings.TrimSpace(r.FormValue("return_to"))
	arxiu := parseArxiuForm(r)
	arxiu.ID = id
	arxiu.ModeracioEstat = "pendent"
	arxiu.ModeratedBy = sql.NullInt64{}
	arxiu.ModeratedAt = sql.NullTime{}
	existing, err := a.DB.GetArxiu(id)
	if err != nil || existing == nil {
		a.renderArxiuForm(w, r, arxiu, false, "No s'ha pogut carregar l'arxiu existent.", user, returnURL)
		return
	}
	if existing.ModeracioEstat == "publicat" {
		lang := resolveUserLang(r, user)
		if !a.ensureWikiChangeAllowed(w, r, lang) {
			return
		}
		after := *arxiu
		after.ModeracioEstat = "pendent"
		after.ModeracioMotiu = ""
		after.ModeratedBy = sql.NullInt64{}
		after.ModeratedAt = sql.NullTime{}
		if existing.CreatedBy.Valid {
			after.CreatedBy = existing.CreatedBy
		}
		beforeJSON, _ := json.Marshal(existing)
		afterJSON, _ := json.Marshal(after)
		meta, err := buildWikiChangeMetadata(beforeJSON, afterJSON, 0)
		if err != nil {
			a.renderArxiuForm(w, r, arxiu, false, "No s'ha pogut preparar el canvi de l'arxiu.", user, returnURL)
			return
		}
		changeID, err := a.createWikiChange(&db.WikiChange{
			ObjectType:     "arxiu",
			ObjectID:       id,
			ChangeType:     "form",
			FieldKey:       "bulk",
			Metadata:       meta,
			ModeracioEstat: "pendent",
			ChangedBy:      sqlNullIntFromInt(user.ID),
		})
		if err != nil {
			if _, msg, ok := a.wikiGuardrailInfo(lang, err); ok {
				a.renderArxiuForm(w, r, arxiu, false, msg, user, returnURL)
				return
			}
			a.renderArxiuForm(w, r, arxiu, false, "No s'ha pogut crear la proposta de canvi.", user, returnURL)
			return
		}
		detail := "arxiu:" + strconv.Itoa(id)
		_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleArxiuUpdate, "editar", "arxiu_canvi", &changeID, "pendent", nil, detail)
	} else {
		if err := a.DB.UpdateArxiu(arxiu); err != nil {
			a.renderArxiuForm(w, r, arxiu, false, "No s'ha pogut actualitzar.", user, returnURL)
			return
		}
		_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleArxiuUpdate, "editar", "arxiu", &id, "pendent", nil, "")
	}
	if returnURL != "" {
		http.Redirect(w, r, returnURL, http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/documentals/arxius/"+strconv.Itoa(id), http.StatusSeeOther)
}

func (a *App) AdminDeleteArxiu(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	target := a.resolveArxiuTarget(id)
	if _, ok := a.requirePermissionKey(w, r, permKeyDocumentalsArxiusDelete, target); !ok {
		return
	}
	_ = a.DB.DeleteArxiu(id)
	http.Redirect(w, r, "/documentals/arxius", http.StatusSeeOther)
}

func (a *App) AdminShowArxiu(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	target := a.resolveArxiuTarget(id)
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsArxiusView, target)
	if !ok {
		return
	}
	arxiu, err := a.DB.GetArxiu(id)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	donacionsClicks := 0
	if arxiu.AcceptaDonacions && strings.TrimSpace(arxiu.DonacionsURL) != "" {
		if total, err := a.DB.CountArxiuDonacioClicks(id); err == nil {
			donacionsClicks = total
		}
	}
	markType := ""
	markPublic := true
	markOwn := false
	if marks, err := a.DB.ListWikiMarks("arxiu", []int{id}); err == nil {
		for _, mark := range marks {
			if mark.UserID == user.ID {
				markType = mark.Tipus
				markPublic = mark.IsPublic
				markOwn = true
				break
			}
		}
	}
	llibres, _ := a.DB.ListArxiuLlibres(id)
	canEditArxiu := a.HasPermission(user.ID, permKeyDocumentalsArxiusEdit, target)
	canDeleteArxiu := a.HasPermission(user.ID, permKeyDocumentalsArxiusDelete, target)
	canCreateLlibre := a.HasPermission(user.ID, permKeyDocumentalsLlibresCreate, target)
	canEditAll := a.HasPermission(user.ID, permKeyDocumentalsLlibresEdit, target)
	canViewAll := a.HasPermission(user.ID, permKeyDocumentalsLlibresView, target) || canEditAll
	viewScope := a.buildListScopeFilter(user.ID, permKeyDocumentalsLlibresView, ScopeLlibre)
	editScope := a.buildListScopeFilter(user.ID, permKeyDocumentalsLlibresEdit, ScopeLlibre)
	allowedView := map[int]bool{}
	allowedEdit := map[int]bool{}
	if !viewScope.hasGlobal {
		for _, lid := range viewScope.llibreIDs {
			if lid > 0 {
				allowedView[lid] = true
			}
		}
	}
	if !editScope.hasGlobal {
		for _, lid := range editScope.llibreIDs {
			if lid > 0 {
				allowedEdit[lid] = true
			}
		}
	}
	canEditLlibre := make(map[int]bool, len(llibres))
	canViewLlibre := make(map[int]bool, len(llibres))
	showLlibreActions := false
	for _, llibre := range llibres {
		canEdit := canEditAll || allowedEdit[llibre.LlibreID]
		canView := canViewAll || canEdit || allowedView[llibre.LlibreID]
		canEditLlibre[llibre.LlibreID] = canEdit
		canViewLlibre[llibre.LlibreID] = canView
		if canEdit || canView {
			showLlibreActions = true
		}
	}
	entNom := a.loadEntitatNom(arxiu)
	munNom := a.loadMunicipiNom(arxiu)
	lang := ResolveLang(r)
	arxiuPro := a.buildArxiuProData(lang, arxiu, entNom, munNom, llibres, canEditLlibre, canViewLlibre, showLlibreActions)
	RenderPrivateTemplate(w, r, "admin-arxius-show.html", map[string]interface{}{
		"Arxiu":             arxiu,
		"Llibres":           llibres,
		"EntitatNom":        entNom,
		"MunicipiNom":       munNom,
		"ArxiuProData":      arxiuPro,
		"DonacionsClicks":   donacionsClicks,
		"MarkType":          markType,
		"MarkPublic":        markPublic,
		"MarkOwn":           markOwn,
		"CanManageArxius":   true,
		"CanEditArxiu":      canEditArxiu,
		"CanDeleteArxiu":    canDeleteArxiu,
		"CanCreateLlibre":   canCreateLlibre,
		"CanEditLlibre":     canEditLlibre,
		"CanViewLlibre":     canViewLlibre,
		"ShowLlibreActions": showLlibreActions,
		"ArxiusBasePath":    "/documentals/arxius",
		"User":              user,
	})
}

func (a *App) AdminAddArxiuLlibre(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	target := a.resolveArxiuTarget(id)
	if _, ok := a.requirePermissionKey(w, r, permKeyDocumentalsArxiusEdit, target); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/documentals/arxius/"+strconv.Itoa(id), http.StatusSeeOther)
		return
	}
	llibreID, _ := strconv.Atoi(r.FormValue("llibre_id"))
	signatura := strings.TrimSpace(r.FormValue("signatura"))
	urlOverride := strings.TrimSpace(r.FormValue("url_override"))
	if llibreID == 0 {
		http.Redirect(w, r, "/documentals/arxius/"+strconv.Itoa(id)+"?error=llibre", http.StatusSeeOther)
		return
	}
	if err := a.DB.AddArxiuLlibre(id, llibreID, signatura, urlOverride); err != nil {
		http.Redirect(w, r, "/documentals/arxius/"+strconv.Itoa(id)+"?error=dup", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/documentals/arxius/"+strconv.Itoa(id), http.StatusSeeOther)
}

func (a *App) AdminUpdateArxiuLlibre(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 5 {
		http.NotFound(w, r)
		return
	}
	arxiuID, _ := strconv.Atoi(parts[2])
	target := a.resolveArxiuTarget(arxiuID)
	if _, ok := a.requirePermissionKey(w, r, permKeyDocumentalsArxiusEdit, target); !ok {
		return
	}
	llibreID, _ := strconv.Atoi(parts[4])
	signatura := strings.TrimSpace(r.FormValue("signatura"))
	urlOverride := strings.TrimSpace(r.FormValue("url_override"))
	_ = a.DB.UpdateArxiuLlibre(arxiuID, llibreID, signatura, urlOverride)
	http.Redirect(w, r, "/documentals/arxius/"+strconv.Itoa(arxiuID), http.StatusSeeOther)
}

func (a *App) AdminDeleteArxiuLlibre(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 5 {
		http.NotFound(w, r)
		return
	}
	arxiuID, _ := strconv.Atoi(parts[2])
	target := a.resolveArxiuTarget(arxiuID)
	if _, ok := a.requirePermissionKey(w, r, permKeyDocumentalsArxiusEdit, target); !ok {
		return
	}
	llibreID, _ := strconv.Atoi(parts[4])
	_ = a.DB.DeleteArxiuLlibre(arxiuID, llibreID)
	http.Redirect(w, r, "/documentals/arxius/"+strconv.Itoa(arxiuID), http.StatusSeeOther)
}

// util
func extractID(path string) int {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i := len(parts) - 1; i >= 0; i-- {
		if id, err := strconv.Atoi(parts[i]); err == nil {
			return id
		}
	}
	return 0
}

func sqlNullInt(val string) (n sql.NullInt64) {
	if strings.TrimSpace(val) == "" {
		return
	}
	if i, err := strconv.Atoi(val); err == nil {
		n.Int64 = int64(i)
		n.Valid = true
	}
	return
}

func sqlNullIntFromInt(v int) sql.NullInt64 {
	return sql.NullInt64{Int64: int64(v), Valid: true}
}

func (a *App) loadEntitatNom(arxiu *db.Arxiu) string {
	if arxiu == nil || !arxiu.EntitatEclesiasticaID.Valid {
		return ""
	}
	if ent, err := a.DB.GetArquebisbat(int(arxiu.EntitatEclesiasticaID.Int64)); err == nil && ent != nil {
		return ent.Nom
	}
	return ""
}

func (a *App) loadMunicipiNom(arxiu *db.Arxiu) string {
	if arxiu == nil || !arxiu.MunicipiID.Valid {
		return ""
	}
	if mun, err := a.DB.GetMunicipi(int(arxiu.MunicipiID.Int64)); err == nil && mun != nil {
		return mun.Nom
	}
	return ""
}

func translateOrFallback(lang, key, fallback string) string {
	if strings.TrimSpace(key) == "" {
		return fallback
	}
	val := T(lang, key)
	if val == key {
		return fallback
	}
	return val
}

func (a *App) buildArxiuProData(lang string, arxiu *db.Arxiu, entNom, munNom string, llibres []db.ArxiuLlibreDetail, canEdit, canView map[int]bool, showActions bool) arxiuProData {
	if arxiu == nil {
		return arxiuProData{}
	}
	tipusLabel := strings.TrimSpace(arxiu.Tipus)
	if tipusLabel != "" {
		tipusLabel = translateOrFallback(lang, "archives.type."+arxiu.Tipus, arxiu.Tipus)
	}
	accesLabel := strings.TrimSpace(arxiu.Acces)
	if accesLabel != "" {
		accesLabel = translateOrFallback(lang, "archives.access."+arxiu.Acces, arxiu.Acces)
	}
	estatLabel := ""
	if strings.TrimSpace(arxiu.ModeracioEstat) != "" {
		estatLabel = translateOrFallback(lang, "activity.status."+arxiu.ModeracioEstat, arxiu.ModeracioEstat)
	}
	munID := 0
	if arxiu.MunicipiID.Valid {
		munID = int(arxiu.MunicipiID.Int64)
	}
	meta := arxiuProMeta{
		ID:                  arxiu.ID,
		Nom:                 strings.TrimSpace(arxiu.Nom),
		Tipus:               tipusLabel,
		Acces:               accesLabel,
		MunicipiID:          munID,
		MunicipiNom:         strings.TrimSpace(munNom),
		EntitatEclesiastica: strings.TrimSpace(entNom),
		Adreca:              strings.TrimSpace(arxiu.Adreca),
		Ubicacio:            strings.TrimSpace(arxiu.Ubicacio),
		What3Words:          strings.TrimSpace(arxiu.What3Words),
		Web:                 strings.TrimSpace(arxiu.Web),
		Notes:               strings.TrimSpace(arxiu.Notes),
		Estat:               strings.TrimSpace(arxiu.ModeracioEstat),
		EstatLabel:          estatLabel,
	}
	books := make([]arxiuProBook, 0, len(llibres))
	for _, rel := range llibres {
		title := strings.TrimSpace(rel.Titol)
		if title == "" {
			title = strings.TrimSpace(rel.NomEsglesia)
		}
		muni := ""
		if rel.Municipi.Valid {
			muni = strings.TrimSpace(rel.Municipi.String)
		}
		signatura := ""
		if rel.Signatura.Valid {
			signatura = strings.TrimSpace(rel.Signatura.String)
		}
		url := ""
		if rel.URLOverride.Valid {
			url = strings.TrimSpace(rel.URLOverride.String)
		}
		var pages *int
		if rel.Pagines.Valid {
			p := int(rel.Pagines.Int64)
			pages = &p
		}
		tipusLlibre := strings.TrimSpace(rel.TipusLlibre)
		if tipusLlibre != "" {
			tipusLlibre = translateOrFallback(lang, "books.type."+rel.TipusLlibre, rel.TipusLlibre)
		}
		book := arxiuProBook{
			ID:          rel.LlibreID,
			Titol:       title,
			NomEsglesia: strings.TrimSpace(rel.NomEsglesia),
			Tipus:       tipusLlibre,
			Cronologia:  strings.TrimSpace(rel.Cronologia),
			Municipi:    muni,
			Pagines:     pages,
			Signatura:   signatura,
			URL:         url,
			Href:        "/documentals/llibres/" + strconv.Itoa(rel.LlibreID),
		}
		if canEdit != nil {
			book.CanEdit = canEdit[rel.LlibreID]
		}
		if canView != nil {
			book.CanView = canView[rel.LlibreID]
		}
		books = append(books, book)
	}
	donacionsURL := strings.TrimSpace(arxiu.DonacionsURL)
	acceptaDonacions := arxiu.AcceptaDonacions && donacionsURL != ""
	var donacions *arxiuProDonacio
	if acceptaDonacions {
		donacions = &arxiuProDonacio{
			URL: "/documentals/arxius/" + strconv.Itoa(arxiu.ID) + "/donacions",
		}
	}
	return arxiuProData{
		Arxiu:            meta,
		Llibres:          books,
		ShowActions:      showActions,
		AcceptaDonacions: acceptaDonacions,
		Donacions:        donacions,
	}
}
