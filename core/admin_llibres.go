package core

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func (a *App) AdminListLlibres(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKeyAnyScope(w, r, permKeyDocumentalsLlibresView)
	if !ok {
		return
	}
	perms := a.getPermissionsForUser(user.ID)
	canManage := a.hasPerm(perms, permArxius)
	isAdmin := a.hasPerm(perms, permAdmin)
	canCreateLlibre := a.hasAnyPermissionKey(user.ID, permKeyDocumentalsLlibresCreate)
	canImportLlibres := a.HasPermission(user.ID, permKeyDocumentalsLlibresImport, PermissionTarget{})
	canExportLlibres := a.HasPermission(user.ID, permKeyDocumentalsLlibresExport, PermissionTarget{})
	canImportRegistresGlobal := a.HasPermission(user.ID, permKeyDocumentalsLlibresImportCSV, PermissionTarget{})
	scopeFilter := a.buildListScopeFilter(user.ID, permKeyDocumentalsLlibresView, ScopeLlibre)
	filter := db.LlibreFilter{
		Text: strings.TrimSpace(r.URL.Query().Get("q")),
	}
	perPage := parseListPerPage(r.URL.Query().Get("per_page"))
	page := parseListPage(r.URL.Query().Get("page"))
	if v := strings.TrimSpace(r.URL.Query().Get("arquevisbat_id")); v != "" {
		if id, err := strconv.Atoi(v); err == nil {
			filter.ArquebisbatID = id
		}
	}
	if v := strings.TrimSpace(r.URL.Query().Get("municipi_id")); v != "" {
		if id, err := strconv.Atoi(v); err == nil {
			filter.MunicipiID = id
		}
	}
	if v := strings.TrimSpace(r.URL.Query().Get("arxiu_id")); v != "" {
		if id, err := strconv.Atoi(v); err == nil {
			filter.ArxiuID = id
		}
	}
	filter.ArxiuTipus = strings.TrimSpace(r.URL.Query().Get("arxiu_tipus"))
	filter.TipusLlibre = strings.TrimSpace(r.URL.Query().Get("tipus_llibre"))
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	if status == "" {
		status = "publicat"
	}
	filter.Status = status
	lang := ResolveLang(r)
	filterKeys := []string{"titol", "entitat", "municipi", "crono", "pagines", "status"}
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
	if !scopeFilter.hasGlobal {
		if scopeFilter.isEmpty() {
			pagination := buildPagination(r, page, perPage, 0, "#llibresTable")
			RenderPrivateTemplate(w, r, "admin-llibres-list.html", map[string]interface{}{
				"Llibres":                  []db.LlibreRow{},
				"IndexacioStats":           map[string]LlibreIndexacioView{},
				"Filter":                   filter,
				"FilterValues":             filterValues,
				"FilterOrder":              strings.Join(filterOrder, ","),
				"Arquebisbats":             []db.ArquebisbatRow{},
				"MunicipiLabel":            "",
				"Arxius":                   []db.ArxiuWithCount{},
				"TipusOptions":             llibreTipusOptions,
				"CanManageArxius":          canManage,
				"CanCreateLlibre":          canCreateLlibre,
				"CanImportLlibres":         canImportLlibres,
				"CanExportLlibres":         canExportLlibres,
				"CanImportRegistresGlobal": canImportRegistresGlobal,
				"CanEditLlibre":            map[int]bool{},
				"CanDeleteLlibre":          map[int]bool{},
				"CanIndexLlibre":           map[int]bool{},
				"CanViewRegistres":         map[int]bool{},
				"CanImportCSV":             map[int]bool{},
				"CanExportCSV":             map[int]bool{},
				"CanMarkIndexed":           map[int]bool{},
				"CanRecalcIndex":           map[int]bool{},
				"ShowLlibreActions":        false,
				"ShowPurgeModal":           false,
				"IsAdmin":                  isAdmin,
				"Page":                     pagination.Page,
				"PerPage":                  pagination.PerPage,
				"Total":                    pagination.Total,
				"TotalPages":               pagination.TotalPages,
				"PageLinks":                pagination.Links,
				"PageSelectBase":           pagination.SelectBase,
				"PageAnchor":               pagination.Anchor,
				"User":                     user,
				"CurrentURL":               r.URL.RequestURI(),
			})
			return
		}
		filter.AllowedLlibreIDs = scopeFilter.llibreIDs
		filter.AllowedArxiuIDs = scopeFilter.arxiuIDs
		filter.AllowedMunicipiIDs = scopeFilter.municipiIDs
		filter.AllowedProvinciaIDs = scopeFilter.provinciaIDs
		filter.AllowedComarcaIDs = scopeFilter.comarcaIDs
		filter.AllowedPaisIDs = scopeFilter.paisIDs
		filter.AllowedEclesIDs = scopeFilter.eclesIDs
	}
	llibres := []db.LlibreRow{}
	total := 0
	pagination := Pagination{}
	filtered := len(filterMatch) > 0
	indexacioStats := map[string]LlibreIndexacioView{}
	if filtered {
		listFilter := filter
		listFilter.Limit = 0
		listFilter.Offset = 0
		allLlibres, _ := a.DB.ListLlibres(listFilter)
		matches := make([]db.LlibreRow, 0, len(allLlibres))
		for _, llibre := range allLlibres {
			match := true
			for _, key := range filterOrder {
				filterVal := filterMatch[key]
				if filterVal == "" {
					continue
				}
				value := strings.ToLower(llibreFilterValue(llibre, nil, key, lang))
				if !strings.Contains(value, filterVal) {
					match = false
					break
				}
			}
			if match {
				matches = append(matches, llibre)
			}
		}
		total = len(matches)
		pagination = buildPagination(r, page, perPage, total, "#llibresTable")
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
		llibres = matches[start:end]
		indexacioStats = a.buildLlibresIndexacioViews(llibres)
	} else {
		total, _ = a.DB.CountLlibres(filter)
		pagination = buildPagination(r, page, perPage, total, "#llibresTable")
		listFilter := filter
		listFilter.Limit = pagination.PerPage
		listFilter.Offset = pagination.Offset
		llibres, _ = a.DB.ListLlibres(listFilter)
		indexacioStats = a.buildLlibresIndexacioViews(llibres)
	}
	canEditLlibre := make(map[int]bool, len(llibres))
	canDeleteLlibre := make(map[int]bool, len(llibres))
	canIndexLlibre := make(map[int]bool, len(llibres))
	canViewRegistres := make(map[int]bool, len(llibres))
	canImportCSV := make(map[int]bool, len(llibres))
	canExportCSV := make(map[int]bool, len(llibres))
	canMarkIndexed := make(map[int]bool, len(llibres))
	canRecalcIndex := make(map[int]bool, len(llibres))
	showLlibreActions := false
	showPurgeModal := false
	for _, llibre := range llibres {
		target := a.resolveLlibreTarget(llibre.ID)
		edit := a.HasPermission(user.ID, permKeyDocumentalsLlibresEdit, target)
		del := a.HasPermission(user.ID, permKeyDocumentalsLlibresDelete, target)
		index := a.HasPermission(user.ID, permKeyDocumentalsLlibresBulkIndex, target)
		viewRegs := a.HasPermission(user.ID, permKeyDocumentalsLlibresViewRegistres, target)
		importCSV := a.HasPermission(user.ID, permKeyDocumentalsLlibresImportCSV, target)
		exportCSV := a.HasPermission(user.ID, permKeyDocumentalsLlibresExportCSV, target)
		markIndexed := a.HasPermission(user.ID, permKeyDocumentalsLlibresMarkIndexed, target)
		recalcIndex := a.HasPermission(user.ID, permKeyDocumentalsLlibresRecalcIndex, target)
		canEditLlibre[llibre.ID] = edit
		canDeleteLlibre[llibre.ID] = del
		canIndexLlibre[llibre.ID] = index
		canViewRegistres[llibre.ID] = viewRegs
		canImportCSV[llibre.ID] = importCSV
		canExportCSV[llibre.ID] = exportCSV
		canMarkIndexed[llibre.ID] = markIndexed
		canRecalcIndex[llibre.ID] = recalcIndex
		if edit || del || index || viewRegs || importCSV || exportCSV || markIndexed || recalcIndex || isAdmin {
			showLlibreActions = true
		}
		if del || isAdmin {
			showPurgeModal = true
		}
	}
	arquebisbats, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	municipiLabel := ""
	if filter.MunicipiID > 0 {
		if mun, err := a.DB.GetMunicipi(filter.MunicipiID); err == nil && mun != nil {
			municipiLabel = mun.Nom
		}
	}
	arxius, _ := a.DB.ListArxius(db.ArxiuFilter{Limit: 200})
	RenderPrivateTemplate(w, r, "admin-llibres-list.html", map[string]interface{}{
		"Llibres":                  llibres,
		"IndexacioStats":           indexacioStats,
		"Filter":                   filter,
		"FilterValues":             filterValues,
		"FilterOrder":              strings.Join(filterOrder, ","),
		"Arquebisbats":             arquebisbats,
		"MunicipiLabel":            municipiLabel,
		"Arxius":                   arxius,
		"TipusOptions":             llibreTipusOptions,
		"CanManageArxius":          canManage,
		"CanCreateLlibre":          canCreateLlibre,
		"CanImportLlibres":         canImportLlibres,
		"CanExportLlibres":         canExportLlibres,
		"CanImportRegistresGlobal": canImportRegistresGlobal,
		"CanEditLlibre":            canEditLlibre,
		"CanDeleteLlibre":          canDeleteLlibre,
		"CanIndexLlibre":           canIndexLlibre,
		"CanViewRegistres":         canViewRegistres,
		"CanImportCSV":             canImportCSV,
		"CanExportCSV":             canExportCSV,
		"CanMarkIndexed":           canMarkIndexed,
		"CanRecalcIndex":           canRecalcIndex,
		"ShowLlibreActions":        showLlibreActions,
		"ShowPurgeModal":           showPurgeModal,
		"IsAdmin":                  isAdmin,
		"Page":                     pagination.Page,
		"PerPage":                  pagination.PerPage,
		"Total":                    pagination.Total,
		"TotalPages":               pagination.TotalPages,
		"PageLinks":                pagination.Links,
		"PageSelectBase":           pagination.SelectBase,
		"PageAnchor":               pagination.Anchor,
		"User":                     user,
		"CurrentURL":               r.URL.RequestURI(),
	})
}

func llibreFilterValue(llibre db.LlibreRow, stats map[string]LlibreIndexacioView, key, lang string) string {
	switch key {
	case "titol":
		title := strings.TrimSpace(llibre.Titol)
		nom := strings.TrimSpace(llibre.NomEsglesia)
		if title == "" {
			return nom
		}
		if nom == "" {
			return title
		}
		return title + " " + nom
	case "entitat":
		if llibre.ArquebisbatNom.Valid {
			return llibre.ArquebisbatNom.String
		}
	case "municipi":
		if llibre.MunicipiNom.Valid {
			return llibre.MunicipiNom.String
		}
	case "crono":
		return llibre.Cronologia
	case "pagines":
		if llibre.Pagines.Valid {
			return strconv.FormatInt(llibre.Pagines.Int64, 10)
		}
	case "registres":
		if stats != nil {
			if stat, ok := stats[strconv.Itoa(llibre.ID)]; ok {
				return strconv.Itoa(stat.TotalRegistres)
			}
		}
	case "indexat":
		if stats != nil {
			if stat, ok := stats[strconv.Itoa(llibre.ID)]; ok {
				return strings.TrimSpace(strconv.Itoa(stat.Percentatge) + "% " + strconv.Itoa(stat.Percentatge))
			}
		}
	case "status":
		if llibre.ModeracioEstat != "" {
			return strings.TrimSpace(T(lang, "activity.status."+llibre.ModeracioEstat) + " " + llibre.ModeracioEstat)
		}
	}
	return ""
}

var llibreTipusOptions = []string{
	"baptismes",
	"matrimonis",
	"obits",
	"confirmacions",
	"padrons",
	"reclutaments",
	"altres",
}

func (a *App) AdminNewLlibre(w http.ResponseWriter, r *http.Request) {
	returnURL := strings.TrimSpace(r.URL.Query().Get("return_to"))
	selectedArxiu := intFromForm(r.URL.Query().Get("arxiu_id"))
	var ok bool
	if selectedArxiu > 0 {
		target := a.resolveArxiuTarget(selectedArxiu)
		_, ok = a.requirePermissionKey(w, r, permKeyDocumentalsLlibresCreate, target)
	} else {
		_, ok = a.requirePermissionKeyAnyScope(w, r, permKeyDocumentalsLlibresCreate)
	}
	if !ok {
		return
	}
	newLlibre := &db.Llibre{ModeracioEstat: "pendent"}
	if selectedArxiu > 0 {
		if arxiu, err := a.DB.GetArxiu(selectedArxiu); err == nil && arxiu != nil {
			if arxiu.EntitatEclesiasticaID.Valid {
				newLlibre.ArquebisbatID = int(arxiu.EntitatEclesiasticaID.Int64)
			}
		}
	}
	a.renderLlibreForm(w, r, newLlibre, true, "", returnURL, selectedArxiu)
}

func (a *App) AdminEditLlibre(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	target := a.resolveLlibreTarget(id)
	_, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresEdit, target)
	if !ok {
		return
	}
	llibre, err := a.DB.GetLlibre(id)
	if err != nil || llibre == nil {
		http.NotFound(w, r)
		return
	}
	returnURL := strings.TrimSpace(r.URL.Query().Get("return_to"))
	selectedArxiu := 0
	if rels, err := a.DB.ListLlibreArxius(id); err == nil && len(rels) > 0 {
		selectedArxiu = rels[0].ArxiuID
	}
	a.renderLlibreForm(w, r, llibre, false, "", returnURL, selectedArxiu)
}

func (a *App) AdminToggleIndexacioLlibre(w http.ResponseWriter, r *http.Request) {
	llibreID := extractID(r.URL.Path)
	target := a.resolveLlibreTarget(llibreID)
	_, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresMarkIndexed, target)
	if !ok {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	llibre, err := a.DB.GetLlibre(llibreID)
	if err != nil || llibre == nil {
		http.NotFound(w, r)
		return
	}
	setIndexat := strings.TrimSpace(r.FormValue("indexacio_completa")) == "1"
	llibre.IndexacioCompleta = setIndexat
	if err := a.DB.UpdateLlibre(llibre); err != nil {
		Errorf("Error actualitzant indexacio completa del llibre %d: %v", llibreID, err)
		http.Redirect(w, r, "/documentals/llibres", http.StatusSeeOther)
		return
	}
	if setIndexat {
		if err := a.DB.RecalcTranscripcionsRawPageStats(llibreID); err != nil {
			Errorf("Error recalculant registres per pagina del llibre %d: %v", llibreID, err)
		}
		if err := a.DB.SetTranscripcionsRawPageStatsIndexacio(llibreID, 1); err != nil {
			Errorf("Error marcant pagines indexades del llibre %d: %v", llibreID, err)
		}
	} else {
		if err := a.DB.SetTranscripcionsRawPageStatsIndexacio(llibreID, 0); err != nil {
			Errorf("Error desmarcant pagines indexades del llibre %d: %v", llibreID, err)
		}
	}
	returnTo := strings.TrimSpace(r.FormValue("return_to"))
	if returnTo == "" {
		returnTo = "/documentals/llibres"
	}
	http.Redirect(w, r, returnTo, http.StatusSeeOther)
}

func (a *App) AdminRecalcIndexacioLlibre(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	llibreID := extractID(r.URL.Path)
	target := a.resolveLlibreTarget(llibreID)
	if _, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresRecalcIndex, target); !ok {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	if llibreID == 0 {
		http.NotFound(w, r)
		return
	}
	if _, err := a.recalcLlibreIndexacioStats(llibreID); err != nil {
		Errorf("Error recalculant indexacio del llibre %d: %v", llibreID, err)
	}
	if err := a.DB.RecalcTranscripcionsRawPageStats(llibreID); err != nil {
		Errorf("Error recalculant registres per pagina del llibre %d: %v", llibreID, err)
	}
	returnTo := strings.TrimSpace(r.FormValue("return_to"))
	if returnTo == "" {
		returnTo = "/documentals/llibres"
	}
	http.Redirect(w, r, returnTo, http.StatusSeeOther)
}

func parseNullInt64(val string) sql.NullInt64 {
	if strings.TrimSpace(val) == "" {
		return sql.NullInt64{}
	}
	n, err := strconv.Atoi(strings.TrimSpace(val))
	if err != nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(n), Valid: true}
}

func parseLlibreForm(r *http.Request) *db.Llibre {
	_ = r.ParseForm()
	arquevisbatID, _ := strconv.Atoi(r.FormValue("arquevisbat_id"))
	municipiID, _ := strconv.Atoi(r.FormValue("municipi_id"))
	paginesVal := strings.TrimSpace(r.FormValue("pagines"))
	pagines := sql.NullInt64{}
	if paginesVal != "" {
		if p, err := strconv.Atoi(paginesVal); err == nil {
			pagines = sql.NullInt64{Int64: int64(p), Valid: true}
		}
	}
	return &db.Llibre{
		ID:                intFromForm(r.FormValue("id")),
		ArquebisbatID:     arquevisbatID,
		MunicipiID:        municipiID,
		NomEsglesia:       strings.TrimSpace(r.FormValue("nom_esglesia")),
		CodiDigital:       strings.TrimSpace(r.FormValue("codi_digital")),
		CodiFisic:         strings.TrimSpace(r.FormValue("codi_fisic")),
		Titol:             strings.TrimSpace(r.FormValue("titol")),
		TipusLlibre:       strings.TrimSpace(r.FormValue("tipus_llibre")),
		Cronologia:        strings.TrimSpace(r.FormValue("cronologia")),
		Volum:             strings.TrimSpace(r.FormValue("volum")),
		Abat:              strings.TrimSpace(r.FormValue("abat")),
		Contingut:         strings.TrimSpace(r.FormValue("contingut")),
		Llengua:           strings.TrimSpace(r.FormValue("llengua")),
		Requeriments:      strings.TrimSpace(r.FormValue("requeriments_tecnics")),
		UnitatCatalogacio: strings.TrimSpace(r.FormValue("unitat_catalogacio")),
		UnitatInstalacio:  strings.TrimSpace(r.FormValue("unitat_instalacio")),
		Pagines:           pagines,
		URLBase:           strings.TrimSpace(r.FormValue("url_base")),
		URLImatgePrefix:   strings.TrimSpace(r.FormValue("url_imatge_prefix")),
		Pagina:            strings.TrimSpace(r.FormValue("pagina")),
		IndexacioCompleta: strings.TrimSpace(r.FormValue("indexacio_completa")) != "",
	}
}

func (a *App) validateLlibre(l *db.Llibre, arxiuID int) string {
	if l.MunicipiID == 0 {
		return "Cal indicar el municipi."
	}
	if strings.TrimSpace(l.Titol) == "" && strings.TrimSpace(l.NomEsglesia) == "" {
		return "Cal un títol o nom d'església."
	}
	if arxiuID == 0 {
		return "Cal indicar l'arxiu."
	}
	if (strings.TrimSpace(l.CodiDigital) != "" || strings.TrimSpace(l.CodiFisic) != "") &&
		strings.TrimSpace(l.TipusLlibre) != "" && strings.TrimSpace(l.Cronologia) != "" {
		dup, err := a.DB.HasLlibreDuplicate(l.MunicipiID, l.TipusLlibre, l.Cronologia, l.CodiDigital, l.CodiFisic, l.ID)
		if err != nil {
			Errorf("Error comprovant duplicats de llibre: %v", err)
			return "No s'ha pogut validar el llibre."
		}
		if dup {
			return "Ja existeix un llibre amb el mateix tipus, cronologia i codi dins del municipi."
		}
	}
	return ""
}

func (a *App) renderLlibreForm(w http.ResponseWriter, r *http.Request, l *db.Llibre, isNew bool, errMsg string, returnURL string, selectedArxiu int) {
	arquebisbats, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	municipis, _ := a.DB.ListMunicipis(db.MunicipiFilter{})
	arxius, _ := a.DB.ListArxius(db.ArxiuFilter{Status: "publicat", Limit: 500})
	RenderPrivateTemplate(w, r, "admin-llibres-form.html", map[string]interface{}{
		"Llibre":          l,
		"TipusOptions":    llibreTipusOptions,
		"Arquebisbats":    arquebisbats,
		"Municipis":       municipis,
		"Arxius":          arxius,
		"SelectedArxiuID": selectedArxiu,
		"IsNew":           isNew,
		"Error":           errMsg,
		"ReturnURL":       returnURL,
		"CanManageArxius": true,
	})
}

func intFromForm(val string) int {
	if v, err := strconv.Atoi(strings.TrimSpace(val)); err == nil {
		return v
	}
	return 0
}

func (a *App) AdminSaveLlibre(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/documentals/llibres", http.StatusSeeOther)
		return
	}
	llibre := parseLlibreForm(r)
	arxiuID := intFromForm(r.FormValue("arxiu_id"))
	permKey := permKeyDocumentalsLlibresCreate
	target := PermissionTarget{}
	if llibre.ID != 0 {
		permKey = permKeyDocumentalsLlibresEdit
		target = a.resolveLlibreTarget(llibre.ID)
	} else if arxiuID > 0 {
		target = a.resolveArxiuTarget(arxiuID)
	}
	user, ok := a.requirePermissionKey(w, r, permKey, target)
	if !ok {
		return
	}
	returnURL := strings.TrimSpace(r.FormValue("return_to"))
	isNew := llibre.ID == 0
	if msg := a.validateLlibre(llibre, arxiuID); msg != "" {
		a.renderLlibreForm(w, r, llibre, isNew, msg, returnURL, arxiuID)
		return
	}
	llibre.CreatedBy = sqlNullIntFromInt(user.ID)
	llibre.ModeracioEstat = "pendent"
	llibre.ModeratedBy = sql.NullInt64{}
	llibre.ModeratedAt = sql.NullTime{}
	if isNew {
		id, err := a.DB.CreateLlibre(llibre)
		if err != nil {
			Errorf("Error creant llibre: %v", err)
			a.renderLlibreForm(w, r, llibre, isNew, "No s'ha pogut crear el llibre.", returnURL, arxiuID)
			return
		}
		if arxiuID > 0 {
			_ = a.DB.AddArxiuLlibre(arxiuID, id, "", "")
		}
		_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleLlibreCreate, "crear", "llibre", &id, "pendent", nil, "")
		if llibre.IndexacioCompleta {
			if err := a.DB.RecalcTranscripcionsRawPageStats(id); err != nil {
				Errorf("Error recalculant registres per pagina del llibre %d: %v", id, err)
			}
		}
	} else {
		existing, err := a.DB.GetLlibre(llibre.ID)
		if err != nil || existing == nil {
			a.renderLlibreForm(w, r, llibre, isNew, "No s'ha pogut carregar el llibre existent.", returnURL, arxiuID)
			return
		}
		if existing.ModeracioEstat == "publicat" {
			lang := resolveUserLang(r, user)
			if !a.ensureWikiChangeAllowed(w, r, lang) {
				return
			}
			after := *llibre
			after.ModeracioEstat = "pendent"
			after.ModeracioMotiu = ""
			after.ModeratedBy = sql.NullInt64{}
			after.ModeratedAt = sql.NullTime{}
			if existing.CreatedBy.Valid {
				after.CreatedBy = existing.CreatedBy
			}
			beforeJSON, _ := json.Marshal(existing)
			afterJSON, _ := json.Marshal(after)
			meta := map[string]interface{}{
				"before": json.RawMessage(beforeJSON),
				"after":  json.RawMessage(afterJSON),
			}
			if arxiuID > 0 {
				meta["arxiu_id"] = arxiuID
			}
			metaJSON, _ := json.Marshal(meta)
			changeID, err := a.createWikiChange(&db.WikiChange{
				ObjectType:     "llibre",
				ObjectID:       llibre.ID,
				ChangeType:     "form",
				FieldKey:       "bulk",
				Metadata:       string(metaJSON),
				ModeracioEstat: "pendent",
				ChangedBy:      sqlNullIntFromInt(user.ID),
			})
			if err != nil {
				if _, msg, ok := a.wikiGuardrailInfo(lang, err); ok {
					a.renderLlibreForm(w, r, llibre, isNew, msg, returnURL, arxiuID)
					return
				}
				Errorf("Error creant proposta llibre: %v", err)
				a.renderLlibreForm(w, r, llibre, isNew, "No s'ha pogut crear la proposta de canvi.", returnURL, arxiuID)
				return
			}
			detail := "llibre:" + strconv.Itoa(llibre.ID)
			_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleLlibreUpdate, "editar", "llibre_canvi", &changeID, "pendent", nil, detail)
		} else {
			if err := a.DB.UpdateLlibre(llibre); err != nil {
				Errorf("Error actualitzant llibre: %v", err)
				a.renderLlibreForm(w, r, llibre, isNew, "No s'ha pogut actualitzar el llibre.", returnURL, arxiuID)
				return
			}
			if arxiuID > 0 {
				_ = a.DB.AddArxiuLlibre(arxiuID, llibre.ID, "", "")
			}
			_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleLlibreUpdate, "editar", "llibre", &llibre.ID, "pendent", nil, "")
			if llibre.IndexacioCompleta {
				if err := a.DB.RecalcTranscripcionsRawPageStats(llibre.ID); err != nil {
					Errorf("Error recalculant registres per pagina del llibre %d: %v", llibre.ID, err)
				}
			}
		}
	}
	if strings.TrimSpace(r.FormValue("recalc_pagines")) != "" && llibre.Pagines.Valid {
		_ = a.DB.RecalcLlibrePagines(llibre.ID, int(llibre.Pagines.Int64))
	}
	if returnURL != "" {
		http.Redirect(w, r, returnURL, http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/documentals/llibres", http.StatusSeeOther)
}

func (a *App) AdminLlibrePagines(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	target := a.resolveLlibreTarget(id)
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresView, target)
	if !ok {
		return
	}
	llibre, err := a.DB.GetLlibre(id)
	if err != nil || llibre == nil {
		http.NotFound(w, r)
		return
	}
	pagines, _ := a.DB.ListLlibrePagines(id)
	RenderPrivateTemplate(w, r, "admin-llibres-pagines.html", map[string]interface{}{
		"Llibre":          llibre,
		"Pagines":         pagines,
		"User":            user,
		"CanManageArxius": true,
	})
}

func (a *App) AdminSaveLlibrePagina(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/documentals/llibres", http.StatusSeeOther)
		return
	}
	llibreID := extractID(r.URL.Path)
	if llibreID == 0 {
		http.NotFound(w, r)
		return
	}
	target := a.resolveLlibreTarget(llibreID)
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresEdit, target)
	if !ok {
		return
	}
	if strings.TrimSpace(r.FormValue("recalc")) != "" {
		total, _ := strconv.Atoi(strings.TrimSpace(r.FormValue("total_pagines")))
		if total == 0 {
			if ll, err := a.DB.GetLlibre(llibreID); err == nil && ll != nil && ll.Pagines.Valid {
				total = int(ll.Pagines.Int64)
			}
		}
		if total > 0 {
			_ = a.DB.RecalcLlibrePagines(llibreID, total)
		}
		http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID)+"/pagines", http.StatusSeeOther)
		return
	}
	numPagina, _ := strconv.Atoi(r.FormValue("num_pagina"))
	if numPagina == 0 {
		http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID)+"/pagines?error=page", http.StatusSeeOther)
		return
	}
	p := &db.LlibrePagina{
		ID:        intFromForm(r.FormValue("page_id")),
		LlibreID:  llibreID,
		NumPagina: numPagina,
		Estat:     strings.TrimSpace(r.FormValue("estat")),
		IndexedAt: sql.NullString{String: strings.TrimSpace(r.FormValue("indexed_at")), Valid: strings.TrimSpace(r.FormValue("indexed_at")) != ""},
		IndexedBy: parseNullInt64(r.FormValue("indexed_by")),
		Notes:     strings.TrimSpace(r.FormValue("notes")),
	}
	if p.Estat == "" {
		p.Estat = "pendent"
	}
	pageID, _ := a.DB.SaveLlibrePagina(p)
	if pageID == 0 {
		pageID = p.ID
	}
	if strings.ToLower(p.Estat) == "indexada" {
		_, _ = a.RegisterUserActivity(r.Context(), user.ID, rulePaginaIndex, "indexar", "llibre_pagina", &pageID, "validat", nil, "")
	}
	http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID)+"/pagines", http.StatusSeeOther)
}

func (a *App) AdminShowLlibre(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	target := a.resolveLlibreTarget(id)
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresView, target)
	if !ok {
		return
	}
	llibre, err := a.DB.GetLlibre(id)
	if err != nil || llibre == nil {
		http.NotFound(w, r)
		return
	}
	if user == nil {
		user, _ = a.VerificarSessio(r)
	}
	isAdmin := false
	canModerate := false
	if user != nil {
		perms := a.getPermissionsForUser(user.ID)
		isAdmin = a.hasPerm(perms, permAdmin)
		canModerate = a.hasPerm(perms, permModerate)
	}
	canEditLlibre := user != nil && a.HasPermission(user.ID, permKeyDocumentalsLlibresEdit, target)
	canDeleteLlibre := user != nil && a.HasPermission(user.ID, permKeyDocumentalsLlibresDelete, target)
	canIndexLlibre := user != nil && a.HasPermission(user.ID, permKeyDocumentalsLlibresBulkIndex, target)
	canViewRegistres := user != nil && a.HasPermission(user.ID, permKeyDocumentalsLlibresViewRegistres, target)
	canImportCSV := user != nil && a.HasPermission(user.ID, permKeyDocumentalsLlibresImportCSV, target)
	canExportCSV := user != nil && a.HasPermission(user.ID, permKeyDocumentalsLlibresExportCSV, target)
	canMarkIndexed := user != nil && a.HasPermission(user.ID, permKeyDocumentalsLlibresMarkIndexed, target)
	canRecalcIndex := user != nil && a.HasPermission(user.ID, permKeyDocumentalsLlibresRecalcIndex, target)
	markType := ""
	markPublic := true
	markOwn := false
	if user != nil {
		if marks, err := a.DB.ListWikiMarks("llibre", []int{id}); err == nil {
			for _, mark := range marks {
				if mark.UserID == user.ID {
					markType = mark.Tipus
					markPublic = mark.IsPublic
					markOwn = true
					break
				}
			}
		}
	}
	if (user == nil || !a.CanManageArxius(user)) && llibre.ModeracioEstat != "publicat" {
		http.NotFound(w, r)
		return
	}
	statusFilter := ""
	if user == nil || !a.CanManageArxius(user) {
		statusFilter = "publicat"
	}
	registres, _ := a.DB.ListTranscripcionsRaw(id, db.TranscripcioFilter{
		Status: statusFilter,
		Limit:  10000,
	})
	pageStats, _ := a.DB.ListTranscripcionsRawPageStats(id)
	if len(pageStats) == 0 {
		totalAll, _ := a.DB.CountTranscripcionsRaw(id, db.TranscripcioFilter{})
		if totalAll > 0 {
			if err := a.DB.RecalcTranscripcionsRawPageStats(id); err != nil {
				Errorf("Error recalculant registres per pagina del llibre %d: %v", id, err)
			} else {
				pageStats, _ = a.DB.ListTranscripcionsRawPageStats(id)
			}
		}
	}
	// no auto-recalc when totals are zero; admins can trigger recalculation explicitly
	mergedStats := pageStats
	if llibre.Pagines.Valid && llibre.Pagines.Int64 > 0 {
		totalPages := int(llibre.Pagines.Int64)
		seen := make(map[string]db.TranscripcioRawPageStat, len(pageStats))
		for _, stat := range pageStats {
			seen[stat.NumPaginaText] = stat
		}
		mergedStats = make([]db.TranscripcioRawPageStat, 0, totalPages+len(pageStats))
		for i := 1; i <= totalPages; i++ {
			key := strconv.Itoa(i)
			if stat, ok := seen[key]; ok {
				mergedStats = append(mergedStats, stat)
				delete(seen, key)
				continue
			}
			mergedStats = append(mergedStats, db.TranscripcioRawPageStat{
				LlibreID:          llibre.ID,
				NumPaginaText:     key,
				TipusPagina:       "normal",
				Exclosa:           0,
				IndexacioCompleta: 0,
				TotalRegistres:    0,
			})
		}
		for _, stat := range seen {
			mergedStats = append(mergedStats, stat)
		}
	}
	pagines := []db.LlibrePagina{}
	paginesByNum := map[int]int{}
	if pageList, err := a.DB.ListLlibrePagines(id); err == nil {
		pagines = pageList
		for _, p := range pageList {
			paginesByNum[p.NumPagina] = p.ID
		}
	}
	if len(paginesByNum) > 0 {
		for i := range mergedStats {
			if mergedStats[i].PaginaID.Valid {
				continue
			}
			if n, err := strconv.Atoi(strings.TrimSpace(mergedStats[i].NumPaginaText)); err == nil {
				if pid, ok := paginesByNum[n]; ok {
					mergedStats[i].PaginaID = sql.NullInt64{Int64: int64(pid), Valid: true}
				}
			}
		}
	}
	psFilter := strings.TrimSpace(r.URL.Query().Get("ps_filter"))
	if psFilter != "" {
		needle := strings.ToLower(psFilter)
		filtered := make([]db.TranscripcioRawPageStat, 0, len(mergedStats))
		for _, stat := range mergedStats {
			if strings.Contains(strings.ToLower(stat.NumPaginaText), needle) {
				filtered = append(filtered, stat)
			}
		}
		mergedStats = filtered
	}
	psPerPage := 5
	if val := strings.TrimSpace(r.URL.Query().Get("ps_per_page")); val != "" {
		if n, err := strconv.Atoi(val); err == nil {
			switch n {
			case 5, 10, 25, 50, 100:
				psPerPage = n
			}
		}
	}
	psPage := 1
	if val := strings.TrimSpace(r.URL.Query().Get("ps_page")); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			psPage = n
		}
	}
	psTotal := len(mergedStats)
	psTotalPages := 0
	if psPerPage > 0 {
		psTotalPages = (psTotal + psPerPage - 1) / psPerPage
	}
	if psTotalPages == 0 {
		psTotalPages = 1
	}
	if psPage > psTotalPages {
		psPage = psTotalPages
	}
	psStart := (psPage - 1) * psPerPage
	if psStart < 0 {
		psStart = 0
	}
	psEnd := psStart + psPerPage
	if psEnd > psTotal {
		psEnd = psTotal
	}
	pageStatsPage := mergedStats
	if psTotal > 0 && psStart < psEnd {
		pageStatsPage = mergedStats[psStart:psEnd]
	}
	selectQuery := cloneValues(r.URL.Query())
	selectQuery.Del("ps_page")
	selectQuery.Del("ps_per_page")
	selectBase := r.URL.Path
	if len(selectQuery) > 0 {
		selectBase = selectBase + "?" + selectQuery.Encode() + "&ps_per_page="
	} else {
		selectBase = selectBase + "?ps_per_page="
	}
	pageAnchor := "#page-stats-controls"
	pageQuery := cloneValues(r.URL.Query())
	pageQuery.Del("ps_page")
	pageQuery.Set("ps_per_page", strconv.Itoa(psPerPage))
	pageBase := r.URL.Path + "?" + pageQuery.Encode()
	pageLinks := make([]map[string]interface{}, 0, psTotalPages+4)
	addPageLink := func(label string, target int, current bool, isNav bool) {
		q := cloneValues(pageQuery)
		q.Set("ps_page", strconv.Itoa(target))
		pageLinks = append(pageLinks, map[string]interface{}{
			"Label":   label,
			"URL":     r.URL.Path + "?" + q.Encode() + pageAnchor,
			"Current": current,
			"IsNav":   isNav,
		})
	}
	if psPage > 1 {
		addPageLink("<<", 1, false, true)
		addPageLink("<", psPage-1, false, true)
	}
	windowSize := 10
	start := 1
	end := psTotalPages
	if psTotalPages > windowSize {
		half := windowSize / 2
		start = psPage - half
		if start < 1 {
			start = 1
		}
		end = start + windowSize - 1
		if end > psTotalPages {
			end = psTotalPages
			start = end - windowSize + 1
		}
	}
	for i := start; i <= end; i++ {
		addPageLink(strconv.Itoa(i), i, i == psPage, false)
	}
	if psPage < psTotalPages {
		addPageLink(">", psPage+1, false, true)
		addPageLink(">>", psTotalPages, false, true)
	}
	filterQuery := cloneValues(r.URL.Query())
	filterQuery.Del("ps_page")
	filterQuery.Del("ps_filter")
	filterQuery.Set("ps_per_page", strconv.Itoa(psPerPage))
	filterBase := r.URL.Path
	if len(filterQuery) > 0 {
		filterBase = filterBase + "?" + filterQuery.Encode()
	}
	filterBase += pageAnchor
	type registreStat struct {
		Tipus string
		Count int
	}
	counts := make(map[string]int)
	for _, r := range registres {
		counts[r.TipusActe]++
	}
	stats := make([]registreStat, 0, len(transcripcioTipusActe))
	for _, tipus := range transcripcioTipusActe {
		stats = append(stats, registreStat{Tipus: tipus, Count: counts[tipus]})
	}
	arxius, _ := a.DB.ListLlibreArxius(id)
	if len(arxius) > 1 {
		uniq := make(map[int]db.ArxiuLlibreDetail, len(arxius))
		for _, rel := range arxius {
			existing, ok := uniq[rel.ArxiuID]
			if !ok {
				uniq[rel.ArxiuID] = rel
				continue
			}
			if !existing.URLOverride.Valid && rel.URLOverride.Valid {
				uniq[rel.ArxiuID] = rel
				continue
			}
			if !existing.Signatura.Valid && rel.Signatura.Valid {
				uniq[rel.ArxiuID] = rel
			}
		}
		arxius = arxius[:0]
		for _, rel := range uniq {
			arxius = append(arxius, rel)
		}
		sort.Slice(arxius, func(i, j int) bool {
			return arxius[i].ArxiuNom.String < arxius[j].ArxiuNom.String
		})
	}
	arxiusOpts, _ := a.DB.ListArxius(db.ArxiuFilter{Limit: 200})
	links, _ := a.DB.ListLlibreURLs(id)
	purgeStatus := strings.TrimSpace(r.URL.Query().Get("purge"))
	entityName := ""
	if llibre.ArquebisbatID > 0 {
		if ae, err := a.DB.GetArquebisbat(llibre.ArquebisbatID); err == nil && ae != nil {
			entityName = ae.Nom
		}
	}
	municipiName := ""
	if llibre.MunicipiID > 0 {
		if m, err := a.DB.GetMunicipi(llibre.MunicipiID); err == nil && m != nil {
			municipiName = m.Nom
		}
	}
	mediaPageID := parseIntDefault(r.URL.Query().Get("media_page_id"), 0)
	if len(pagines) == 0 {
		mediaPageID = 0
	} else if mediaPageID == 0 {
		mediaPageID = pagines[0].ID
	} else {
		found := false
		for _, p := range pagines {
			if p.ID == mediaPageID {
				found = true
				break
			}
		}
		if !found {
			mediaPageID = pagines[0].ID
		}
	}
	mediaSearch := strings.TrimSpace(r.URL.Query().Get("media_search"))
	mediaModalOpen := r.URL.Query().Get("media_modal") == "1" || mediaSearch != ""
	mediaLinks := []db.MediaItemPageLink{}
	if mediaPageID > 0 {
		if links, err := a.DB.ListMediaItemLinksByPagina(mediaPageID); err == nil {
			mediaLinks = a.filterMediaItemLinks(r, user, links)
		}
	}
	mediaSearchResults := []db.MediaItemSearchRow{}
	if mediaSearch != "" {
		if rows, err := a.DB.SearchMediaItems(mediaSearch, 100); err == nil {
			mediaSearchResults = a.filterMediaSearchResults(r, user, rows)
		}
	}
	mediaSelectQuery := cloneValues(r.URL.Query())
	mediaSelectQuery.Del("media_page_id")
	mediaSelectQuery.Del("media_modal")
	mediaSelectQuery.Del("media_search")
	mediaSelectBase := r.URL.Path
	if len(mediaSelectQuery) > 0 {
		mediaSelectBase = mediaSelectBase + "?" + mediaSelectQuery.Encode() + "&media_page_id="
	} else {
		mediaSelectBase = mediaSelectBase + "?media_page_id="
	}
	mediaReturnTo := r.URL.RequestURI() + "#media-links"
	RenderPrivateTemplate(w, r, "admin-llibres-show.html", map[string]interface{}{
		"Llibre":              llibre,
		"LlibreEntityName":    entityName,
		"LlibreMunicipiName":  municipiName,
		"LlibreCronologia":    formatCronologiaDisplay(llibre.Cronologia),
		"Arxius":              arxius,
		"ArxiusOptions":       arxiusOpts,
		"LlibreLinks":         links,
		"RegistresStats":      stats,
		"RegistresTotal":      len(registres),
		"PageStats":           pageStatsPage,
		"TotalPages":          llibre.Pagines,
		"PageStatsTotal":      psTotal,
		"PageStatsPage":       psPage,
		"PageStatsPerPage":    psPerPage,
		"PageStatsPages":      psTotalPages,
		"PageStatsBase":       pageBase,
		"PageStatsSelectBase": selectBase,
		"PageStatsFilter":     psFilter,
		"PageStatsFilterBase": filterBase,
		"PageStatsAnchor":     pageAnchor,
		"PageStatsLinks":      pageLinks,
		"MediaPageOptions":    pagines,
		"MediaPageID":         mediaPageID,
		"MediaPageSelectBase": mediaSelectBase,
		"MediaPageLinks":      mediaLinks,
		"MediaSearch":         mediaSearch,
		"MediaSearchResults":  mediaSearchResults,
		"MediaModalOpen":      mediaModalOpen,
		"MediaReturnTo":       mediaReturnTo,
		"User":                user,
		"CanManageArxius":     true,
		"CanEditLlibre":       canEditLlibre,
		"CanDeleteLlibre":     canDeleteLlibre,
		"CanIndexLlibre":      canIndexLlibre,
		"CanViewRegistres":    canViewRegistres,
		"CanImportCSV":        canImportCSV,
		"CanExportCSV":        canExportCSV,
		"CanMarkIndexed":      canMarkIndexed,
		"CanRecalcIndex":      canRecalcIndex,
		"IsAdmin":             isAdmin,
		"CanModerate":         canModerate,
		"PurgeStatus":         purgeStatus,
		"MarkType":            markType,
		"MarkPublic":          markPublic,
		"MarkOwn":             markOwn,
		"CurrentURL":          r.URL.RequestURI(),
	})
}

func (a *App) AdminUpdateLlibrePageStat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/documentals/llibres", http.StatusSeeOther)
		return
	}
	llibreID := extractID(r.URL.Path)
	if llibreID == 0 {
		llibreID = intFromForm(r.FormValue("llibre_id"))
	}
	target := a.resolveLlibreTarget(llibreID)
	if _, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresEdit, target); !ok {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	statID := intFromForm(r.FormValue("stat_id"))
	stat := &db.TranscripcioRawPageStat{
		ID:                statID,
		LlibreID:          llibreID,
		NumPaginaText:     strings.TrimSpace(r.FormValue("num_pagina_text")),
		TipusPagina:       strings.TrimSpace(r.FormValue("tipus_pagina")),
		Exclosa:           intFromForm(r.FormValue("exclosa")),
		IndexacioCompleta: intFromForm(r.FormValue("indexacio_completa")),
		DuplicadaDe:       parseNullString(r.FormValue("duplicada_de")),
		TotalRegistres:    intFromForm(r.FormValue("total_registres")),
		PaginaID:          parseNullInt64(r.FormValue("pagina_id")),
	}
	if stat.LlibreID == 0 {
		http.Redirect(w, r, "/documentals/llibres", http.StatusSeeOther)
		return
	}
	if stat.ID == 0 && stat.NumPaginaText == "" && !stat.PaginaID.Valid {
		http.Redirect(w, r, "/documentals/llibres", http.StatusSeeOther)
		return
	}
	if stat.TipusPagina == "" {
		stat.TipusPagina = "normal"
	}
	if err := a.DB.UpdateTranscripcionsRawPageStat(stat); err != nil {
		Errorf("Error actualitzant stats de pagina %d: %v", statID, err)
	} else {
		if user, _ := a.VerificarSessio(r); user != nil {
			objID := stat.LlibreID
			_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleLlibrePageStatsUpdate, "actualitzar", "llibre_pagina_stats", &objID, "validat", nil, "")
		}
	}
	returnTo := strings.TrimSpace(r.FormValue("return_to"))
	if returnTo == "" {
		if stat.LlibreID > 0 {
			returnTo = "/documentals/llibres/" + strconv.Itoa(stat.LlibreID)
		} else {
			returnTo = "/documentals/llibres"
		}
	}
	http.Redirect(w, r, returnTo, http.StatusSeeOther)
}

func (a *App) AdminPurgeLlibreRegistres(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	llibreID := extractID(r.URL.Path)
	target := a.resolveLlibreTarget(llibreID)
	if _, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresDelete, target); !ok {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	user, _ := a.VerificarSessio(r)
	if user == nil {
		http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID)+"?purge=error", http.StatusSeeOther)
		return
	}
	password := strings.TrimSpace(r.FormValue("confirm_password"))
	if password == "" {
		http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID)+"?purge=required", http.StatusSeeOther)
		return
	}
	if _, err := a.DB.AuthenticateUser(user.Usuari, password); err != nil {
		http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID)+"?purge=auth", http.StatusSeeOther)
		return
	}
	if err := a.DB.DeleteTranscripcionsByLlibre(llibreID); err != nil {
		Errorf("Error eliminant registres del llibre %d: %v", llibreID, err)
		http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID)+"?purge=error", http.StatusSeeOther)
		return
	}
	_, _ = a.recalcLlibreIndexacioStats(llibreID)
	http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID)+"?purge=success", http.StatusSeeOther)
}

func (a *App) AdminAddLlibreArxiu(w http.ResponseWriter, r *http.Request) {
	llibreID := extractID(r.URL.Path)
	target := a.resolveLlibreTarget(llibreID)
	if _, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresEdit, target); !ok {
		return
	}
	arxiuID, _ := strconv.Atoi(r.FormValue("arxiu_id"))
	signatura := strings.TrimSpace(r.FormValue("signatura"))
	urlOverride := strings.TrimSpace(r.FormValue("url_override"))
	if llibreID == 0 || arxiuID == 0 {
		http.Redirect(w, r, "/documentals/llibres", http.StatusSeeOther)
		return
	}
	_ = a.DB.AddArxiuLlibre(arxiuID, llibreID, signatura, urlOverride)
	http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID), http.StatusSeeOther)
}

func (a *App) AdminUpdateLlibreArxiu(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 5 {
		http.NotFound(w, r)
		return
	}
	llibreID, _ := strconv.Atoi(parts[2])
	target := a.resolveLlibreTarget(llibreID)
	if _, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresEdit, target); !ok {
		return
	}
	arxiuID, _ := strconv.Atoi(parts[4])
	signatura := strings.TrimSpace(r.FormValue("signatura"))
	urlOverride := strings.TrimSpace(r.FormValue("url_override"))
	_ = a.DB.UpdateArxiuLlibre(arxiuID, llibreID, signatura, urlOverride)
	http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID), http.StatusSeeOther)
}

func (a *App) AdminDeleteLlibreArxiu(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 5 {
		http.NotFound(w, r)
		return
	}
	llibreID, _ := strconv.Atoi(parts[2])
	target := a.resolveLlibreTarget(llibreID)
	if _, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresEdit, target); !ok {
		return
	}
	arxiuID, _ := strconv.Atoi(parts[4])
	_ = a.DB.DeleteArxiuLlibre(arxiuID, llibreID)
	http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID), http.StatusSeeOther)
}

func (a *App) AdminAddLlibreURL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	llibreID := extractID(r.URL.Path)
	target := a.resolveLlibreTarget(llibreID)
	if _, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresEdit, target); !ok {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	if llibreID == 0 {
		http.Redirect(w, r, "/documentals/llibres", http.StatusSeeOther)
		return
	}
	url := strings.TrimSpace(r.FormValue("url"))
	if url == "" {
		http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID)+"?links=error", http.StatusSeeOther)
		return
	}
	arxiuID := parseNullInt64(r.FormValue("arxiu_id"))
	tipus := parseNullString(r.FormValue("tipus"))
	descripcio := parseNullString(r.FormValue("descripcio"))
	createdBy := sql.NullInt64{}
	if user, _ := a.VerificarSessio(r); user != nil {
		createdBy = sql.NullInt64{Int64: int64(user.ID), Valid: true}
	}
	link := &db.LlibreURL{
		LlibreID:   llibreID,
		ArxiuID:    arxiuID,
		URL:        url,
		Tipus:      tipus,
		Descripcio: descripcio,
		CreatedBy:  createdBy,
	}
	_ = a.DB.AddLlibreURL(link)
	returnTo := strings.TrimSpace(r.FormValue("return_to"))
	if returnTo == "" {
		returnTo = "/documentals/llibres/" + strconv.Itoa(llibreID)
	}
	http.Redirect(w, r, returnTo, http.StatusSeeOther)
}

func (a *App) AdminDeleteLlibreURL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 6 {
		http.NotFound(w, r)
		return
	}
	llibreID, _ := strconv.Atoi(parts[2])
	target := a.resolveLlibreTarget(llibreID)
	if _, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresEdit, target); !ok {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	linkID, _ := strconv.Atoi(parts[4])
	if linkID == 0 {
		http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID), http.StatusSeeOther)
		return
	}
	_ = a.DB.DeleteLlibreURL(linkID)
	returnTo := strings.TrimSpace(r.FormValue("return_to"))
	if returnTo == "" {
		returnTo = "/documentals/llibres/" + strconv.Itoa(llibreID)
	}
	http.Redirect(w, r, returnTo, http.StatusSeeOther)
}

func (a *App) AdminEditLlibreArxiuLinks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 6 {
		http.NotFound(w, r)
		return
	}
	llibreID, _ := strconv.Atoi(parts[2])
	target := a.resolveLlibreTarget(llibreID)
	if _, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresEdit, target); !ok {
		return
	}
	arxiuID, _ := strconv.Atoi(parts[4])
	if llibreID == 0 || arxiuID == 0 {
		http.NotFound(w, r)
		return
	}
	llibre, err := a.DB.GetLlibre(llibreID)
	if err != nil || llibre == nil {
		http.NotFound(w, r)
		return
	}
	arxius, err := a.DB.ListLlibreArxius(llibreID)
	if err != nil {
		Errorf("Error carregant enllaços d'arxiu: %v", err)
		http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID), http.StatusSeeOther)
		return
	}
	var rel *db.ArxiuLlibreDetail
	for i := range arxius {
		if arxius[i].ArxiuID == arxiuID {
			rel = &arxius[i]
			break
		}
	}
	if rel == nil {
		http.NotFound(w, r)
		return
	}
	originalURL := strings.TrimSpace(llibre.URLBase)
	returnURL := r.URL.Query().Get("return_to")
	if returnURL == "" {
		returnURL = "/documentals/llibres/" + strconv.Itoa(llibreID)
	}
	RenderPrivateTemplate(w, r, "admin-llibres-arxiu-links.html", map[string]interface{}{
		"Llibre":      llibre,
		"ArxiuLink":   rel,
		"OriginalURL": originalURL,
		"ReturnURL":   returnURL,
	})
}
