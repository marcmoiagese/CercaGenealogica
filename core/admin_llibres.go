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

const (
	csrfInvalidMessage                   = "CSRF invàlid"
	bookValidationReligiousScopeEmptyKey = "books.form.validation.religious_scope_empty"
)

func (a *App) AdminListLlibres(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKeyAnyScope(w, r, permKeyDocumentalsLlibresView)
	if !ok {
		return
	}
	canManage := a.canManageAnyDocumentalsModular(user)
	isAdmin := a.effectiveAdminForUser(user.ID)
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
	nivellID := 0
	if v := strings.TrimSpace(r.URL.Query().Get("nivell_id")); v != "" {
		if id, err := strconv.Atoi(v); err == nil {
			nivellID = id
		}
	}
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	if status == "" {
		status = "publicat"
	}
	filter.Status = status
	lang := ResolveLang(r)
	filterKeys := []string{"titol", "municipi", "crono", "pagines", "status"}
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
		filter.AllowedNivellIDs = scopeFilter.nivellIDs
		filter.AllowedPaisIDs = scopeFilter.paisIDs
		filter.AllowedEclesIDs = scopeFilter.eclesIDs
	}
	if nivellID > 0 && filter.MunicipiID == 0 {
		munRows, err := a.DB.ListMunicipisBrowse(db.MunicipiBrowseFilter{
			NivellID: nivellID,
			Sort:     "nom",
			SortDir:  "asc",
		})
		if err != nil {
			Errorf("Error carregant municipis per nivell %d: %v", nivellID, err)
			munRows = []db.MunicipiBrowseRow{}
		}
		munIDs := make([]int, 0, len(munRows))
		for _, mun := range munRows {
			munIDs = append(munIDs, mun.ID)
		}
		if len(filter.AllowedMunicipiIDs) > 0 {
			allowed := make(map[int]bool, len(filter.AllowedMunicipiIDs))
			for _, id := range filter.AllowedMunicipiIDs {
				allowed[id] = true
			}
			filtered := make([]int, 0, len(munIDs))
			for _, id := range munIDs {
				if allowed[id] {
					filtered = append(filtered, id)
				}
			}
			munIDs = filtered
		}
		if len(munIDs) == 0 {
			munIDs = []int{-1}
		}
		filter.AllowedMunicipiIDs = munIDs
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
	selectedEntitat := intFromForm(r.URL.Query().Get("entitat_religiosa_id"))
	lang := ResolveLang(r)
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
	a.renderLlibreForm(w, r, newLlibre, true, "", returnURL, a.llibreFormStateFromRequest(lang, newLlibre, selectedArxiu, selectedEntitat))
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
	lang := ResolveLang(r)
	if rels, err := a.DB.ListLlibreArxius(id); err == nil && len(rels) > 0 {
		selectedArxiu = rels[0].ArxiuID
	}
	selectedEntitat := 0
	if ctx := a.deriveLlibreReligiousContext(id); ctx.ID > 0 || strings.TrimSpace(ctx.Code) != "" || strings.TrimSpace(ctx.Name) != "" {
		if items, _, err := a.publishedReligiousEntities(); err == nil {
			selectedEntitat = matchReligiousEntityFromContext(items, ctx)
		}
	}
	a.renderLlibreForm(w, r, llibre, false, "", returnURL, a.llibreFormStateFromRequest(lang, llibre, selectedArxiu, selectedEntitat))
}

func (a *App) AdminToggleIndexacioLlibre(w http.ResponseWriter, r *http.Request) {
	llibreID := extractID(r.URL.Path)
	target := a.resolveLlibreTarget(llibreID)
	_, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresMarkIndexed, target)
	if !ok {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, csrfInvalidMessage, http.StatusBadRequest)
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
		http.Error(w, csrfInvalidMessage, http.StatusBadRequest)
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
		MunicipiID:        municipiID,
		NomEsglesia:       strings.TrimSpace(r.FormValue("nom_esglesia")),
		Codi:              strings.TrimSpace(r.FormValue("codi")),
		CodiDigital:       strings.TrimSpace(r.FormValue("codi_digital")),
		CodiFisic:         strings.TrimSpace(r.FormValue("codi_fisic")),
		SourceSystem:      strings.TrimSpace(r.FormValue("source_system")),
		ExternalID:        strings.TrimSpace(r.FormValue("external_id")),
		ExternalCode:      strings.TrimSpace(r.FormValue("external_code")),
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

func (a *App) validateLlibre(lang string, l *db.Llibre, arxiuID int, entitatReligiosaID int) string {
	if l.MunicipiID == 0 {
		return T(lang, "books.form.validation.municipi_required")
	}
	if mun, err := a.DB.GetMunicipi(l.MunicipiID); err != nil {
		Errorf("Error validant municipi del llibre: %v", err)
		return T(lang, "books.form.validation.municipi_validate_error")
	} else if mun == nil {
		return T(lang, "books.form.validation.municipi_missing")
	}
	if l.ArquebisbatID > 0 {
		if ent, err := a.DB.GetArquebisbat(l.ArquebisbatID); err != nil {
			Errorf("Error validant entitat eclesiastica del llibre: %v", err)
			return T(lang, "books.form.validation.legacy_entity_validate_error")
		} else if ent == nil {
			return T(lang, "books.form.validation.legacy_entity_missing")
		}
	}
	if msg := a.validateLlibreReligiousScope(lang, l, entitatReligiosaID); msg != "" {
		return msg
	}
	if strings.TrimSpace(l.Titol) == "" && strings.TrimSpace(l.NomEsglesia) == "" {
		return T(lang, "books.form.validation.title_or_church_required")
	}
	if arxiuID == 0 {
		return T(lang, "books.form.validation.archive_required")
	}
	if arxiu, err := a.DB.GetArxiu(arxiuID); err != nil {
		Errorf("Error validant arxiu del llibre: %v", err)
		return T(lang, "books.form.validation.archive_validate_error")
	} else if arxiu == nil {
		return T(lang, "books.form.validation.archive_missing")
	} else if strings.TrimSpace(arxiu.ModeracioEstat) != "publicat" {
		return T(lang, "books.form.validation.archive_not_published")
	}
	if (strings.TrimSpace(l.CodiDigital) != "" || strings.TrimSpace(l.CodiFisic) != "") &&
		strings.TrimSpace(l.TipusLlibre) != "" && strings.TrimSpace(l.Cronologia) != "" {
		dup, err := a.DB.HasLlibreDuplicate(l.MunicipiID, l.TipusLlibre, l.Cronologia, l.CodiDigital, l.CodiFisic, l.ID)
		if err != nil {
			Errorf("Error comprovant duplicats de llibre: %v", err)
			return T(lang, "books.form.validation.book_validate_error")
		}
		if dup {
			return T(lang, "books.form.validation.duplicate")
		}
	}
	return ""
}

func isCivilBookType(tipus string) bool {
	switch strings.ToLower(strings.TrimSpace(tipus)) {
	case "padrons", "reclutaments":
		return true
	default:
		return false
	}
}

type llibreReligiousContextView struct {
	ID   int
	Code string
	Name string
}

func (a *App) validateLlibreReligiousScope(lang string, l *db.Llibre, entitatReligiosaID int) string {
	if entitatReligiosaID <= 0 {
		return ""
	}
	entitat, err := a.DB.GetEntitatReligiosa(entitatReligiosaID)
	if err != nil {
		Errorf("Error validant entitat religiosa del llibre: %v", err)
		return T(lang, "books.form.validation.religious_entity_validate_error")
	}
	if entitat == nil || strings.TrimSpace(entitat.ModeracioEstat) != "publicat" {
		return T(lang, "books.form.validation.religious_entity_missing_or_unpublished")
	}
	allowedMunicipis, err := a.municipalityScopeForReligiousEntity(entitatReligiosaID)
	if err != nil {
		Errorf("Error validant abast municipal de l'entitat religiosa %d: %v", entitatReligiosaID, err)
		return T(lang, "books.form.validation.religious_scope_validate_error")
	}
	if len(allowedMunicipis) == 0 {
		return T(lang, bookValidationReligiousScopeEmptyKey)
	}
	if !allowedMunicipis[l.MunicipiID] {
		return T(lang, "books.form.validation.religious_scope_mismatch")
	}
	return ""
}

type llibreFormState struct {
	ArxiuID                int
	ArxiuLabel             string
	EntitatReligiosaID     int
	EntitatReligiosaLabel  string
	MunicipiID             int
	MunicipiLabel          string
	MunicipiScopeEmpty     bool
	MunicipiScopeMessage   string
	RelatedEntitiesMessage string
}

func (a *App) deriveLlibreReligiousContext(llibreID int) llibreReligiousContextView {
	contexts, err := a.DB.ListLlibreDocumentaryContexts(llibreID)
	if err != nil {
		return llibreReligiousContextView{}
	}
	for _, ctx := range contexts {
		if strings.TrimSpace(ctx.RelationModerationStatus.String) != "" && strings.TrimSpace(ctx.RelationModerationStatus.String) != "publicat" {
			continue
		}
		id := 0
		if ctx.ReligiousEntityID.Valid {
			id = int(ctx.ReligiousEntityID.Int64)
		}
		code := strings.TrimSpace(ctx.ReligiousEntityCode.String)
		name := strings.TrimSpace(ctx.ReligiousEntityName.String)
		if id == 0 && code == "" && name == "" {
			continue
		}
		return llibreReligiousContextView{
			ID:   id,
			Code: code,
			Name: name,
		}
	}
	return llibreReligiousContextView{}
}

func (a *App) loadLlibreMunicipiNom(llibre *db.Llibre) string {
	if llibre == nil || llibre.MunicipiID <= 0 {
		return ""
	}
	if municipi, err := a.DB.GetMunicipi(llibre.MunicipiID); err == nil && municipi != nil {
		return strings.TrimSpace(municipi.Nom)
	}
	return ""
}

func (a *App) loadArxiuNom(arxiuID int) string {
	if arxiuID <= 0 {
		return ""
	}
	if arxiu, err := a.DB.GetArxiu(arxiuID); err == nil && arxiu != nil {
		return strings.TrimSpace(arxiu.Nom)
	}
	return ""
}

func (a *App) publishedReligiousEntities() ([]db.EntitatReligiosa, map[int]db.EntitatReligiosa, error) {
	all, err := a.DB.ListEntitatsReligioses()
	if err != nil {
		return nil, nil, err
	}
	out := make([]db.EntitatReligiosa, 0, len(all))
	byID := make(map[int]db.EntitatReligiosa, len(all))
	for _, item := range all {
		if strings.TrimSpace(item.ModeracioEstat) != "publicat" {
			continue
		}
		out = append(out, item)
		byID[item.ID] = item
	}
	sort.SliceStable(out, func(i, j int) bool {
		return strings.ToLower(strings.TrimSpace(out[i].Nom)) < strings.ToLower(strings.TrimSpace(out[j].Nom))
	})
	return out, byID, nil
}

func matchReligiousEntityFromContext(items []db.EntitatReligiosa, ctx llibreReligiousContextView) int {
	if ctx.ID > 0 {
		if id := findReligiousEntityID(items, func(item db.EntitatReligiosa) bool {
			return item.ID == ctx.ID
		}); id > 0 {
			return id
		}
	}
	if code := strings.TrimSpace(ctx.Code); code != "" {
		if id := findReligiousEntityID(items, func(item db.EntitatReligiosa) bool {
			return strings.EqualFold(strings.TrimSpace(item.Codi), code)
		}); id > 0 {
			return id
		}
	}
	if name := strings.TrimSpace(ctx.Name); name != "" {
		return findReligiousEntityID(items, func(item db.EntitatReligiosa) bool {
			return strings.EqualFold(strings.TrimSpace(item.Nom), name)
		})
	}
	return 0
}

func findReligiousEntityID(items []db.EntitatReligiosa, match func(db.EntitatReligiosa) bool) int {
	for _, item := range items {
		if match(item) {
			return item.ID
		}
	}
	return 0
}

func (a *App) confessionalChildrenByParent() (map[int][]int, error) {
	rels, err := a.DB.ListEntitatReligiosaRelacions()
	if err != nil {
		return nil, err
	}
	children := map[int][]int{}
	for _, rel := range rels {
		if strings.TrimSpace(rel.ModeracioEstat) != "publicat" {
			continue
		}
		if rel.EntitatOrigenID <= 0 || rel.EntitatDestiID <= 0 {
			continue
		}
		children[rel.EntitatOrigenID] = append(children[rel.EntitatOrigenID], rel.EntitatDestiID)
	}
	return children, nil
}

func (a *App) archivePublishedReligiousEntities(arxiuID int) ([]db.EntitatReligiosa, error) {
	if arxiuID <= 0 {
		return nil, nil
	}
	rels, err := a.DB.ListArxiuEntitatsReligioses(arxiuID, 0, "")
	if err != nil {
		return nil, err
	}
	_, byID, err := a.publishedReligiousEntities()
	if err != nil {
		return nil, err
	}
	out := make([]db.EntitatReligiosa, 0, len(rels))
	seen := map[int]bool{}
	for _, rel := range rels {
		if strings.TrimSpace(rel.ModeracioEstat) != "publicat" {
			continue
		}
		if strings.TrimSpace(rel.Estat) != "" && strings.TrimSpace(rel.Estat) != "actiu" {
			continue
		}
		item, ok := byID[rel.EntitatReligiosaID]
		if !ok || seen[item.ID] {
			continue
		}
		seen[item.ID] = true
		out = append(out, item)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return strings.ToLower(strings.TrimSpace(out[i].Nom)) < strings.ToLower(strings.TrimSpace(out[j].Nom))
	})
	return out, nil
}

func (a *App) municipalityScopeForReligiousEntity(entitatReligiosaID int) (map[int]bool, error) {
	if entitatReligiosaID <= 0 {
		return nil, nil
	}
	_, publishedEntitiesByID, err := a.publishedReligiousEntities()
	if err != nil {
		return nil, err
	}
	if _, ok := publishedEntitiesByID[entitatReligiosaID]; !ok {
		return map[int]bool{}, nil
	}
	childrenByParent, err := a.confessionalChildrenByParent()
	if err != nil {
		return nil, err
	}
	allowedEntities := map[int]bool{entitatReligiosaID: true}
	for childID := range confessionalDescendantSet(entitatReligiosaID, childrenByParent) {
		if _, ok := publishedEntitiesByID[childID]; ok {
			allowedEntities[childID] = true
		}
	}
	rels, err := a.DB.ListMunicipiEntitatsReligioses(0)
	if err != nil {
		return nil, err
	}
	publishedMunicipiIDs, err := a.publishedMunicipiIDSet()
	if err != nil {
		return nil, err
	}
	allowedMunicipis := map[int]bool{}
	for _, rel := range rels {
		if strings.TrimSpace(rel.ModeracioEstat) != "publicat" {
			continue
		}
		if rel.MunicipiID <= 0 || !allowedEntities[rel.EntitatReligiosaID] {
			continue
		}
		if !publishedMunicipiIDs[rel.MunicipiID] {
			continue
		}
		allowedMunicipis[rel.MunicipiID] = true
	}
	return allowedMunicipis, nil
}

func (a *App) publishedMunicipiIDSet() (map[int]bool, error) {
	rows, err := a.DB.ListMunicipisBrowse(db.MunicipiBrowseFilter{Status: "publicat"})
	if err != nil {
		return nil, err
	}
	out := make(map[int]bool, len(rows))
	for _, row := range rows {
		out[row.ID] = true
	}
	return out, nil
}

func (a *App) llibreFormStateFromRequest(lang string, llibre *db.Llibre, arxiuID, entitatReligiosaID int) llibreFormState {
	state := llibreFormState{
		ArxiuID:            arxiuID,
		EntitatReligiosaID: entitatReligiosaID,
	}
	if llibre != nil {
		state.MunicipiID = llibre.MunicipiID
	}
	state.ArxiuLabel = a.loadArxiuNom(state.ArxiuID)
	state.MunicipiLabel = a.loadLlibreMunicipiNom(llibre)
	if state.EntitatReligiosaID > 0 {
		if entitat, err := a.DB.GetEntitatReligiosa(state.EntitatReligiosaID); err == nil && entitat != nil {
			state.EntitatReligiosaLabel = strings.TrimSpace(entitat.Nom)
		}
	}
	if state.EntitatReligiosaID == 0 && state.ArxiuID > 0 {
		if rels, err := a.archivePublishedReligiousEntities(state.ArxiuID); err == nil && len(rels) == 1 {
			state.EntitatReligiosaID = rels[0].ID
			state.EntitatReligiosaLabel = strings.TrimSpace(rels[0].Nom)
			state.RelatedEntitiesMessage = T(lang, "books.form.religious_entity.auto_selected")
		}
	}
	if state.EntitatReligiosaID > 0 {
		if allowed, err := a.municipalityScopeForReligiousEntity(state.EntitatReligiosaID); err == nil && len(allowed) == 0 {
			state.MunicipiScopeEmpty = true
			state.MunicipiScopeMessage = T(lang, bookValidationReligiousScopeEmptyKey)
		}
	}
	return state
}

func (a *App) renderLlibreForm(w http.ResponseWriter, r *http.Request, l *db.Llibre, isNew bool, errMsg string, returnURL string, formState llibreFormState) {
	RenderPrivateTemplate(w, r, "admin-llibres-form.html", map[string]interface{}{
		"Llibre":          l,
		"TipusOptions":    llibreTipusOptions,
		"FormState":       formState,
		"IsNew":           isNew,
		"Error":           errMsg,
		"ReturnURL":       returnURL,
		"CanManageArxius": true,
	})
}

func (a *App) SearchBookReligiousEntitiesSuggestJSON(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	arxiuID := parseIntDefault(r.URL.Query().Get("arxiu_id"), 0)
	limit := parseSuggestLimit(r.URL.Query().Get("limit"))
	related, err := a.archivePublishedReligiousEntities(arxiuID)
	if err != nil {
		Errorf("Error carregant entitats religioses relacionades amb arxiu %d: %v", arxiuID, err)
		writeJSON(w, map[string]interface{}{"items": []interface{}{}})
		return
	}
	relatedIDs := map[int]bool{}
	for _, item := range related {
		relatedIDs[item.ID] = true
	}
	all, _, err := a.publishedReligiousEntities()
	if err != nil {
		Errorf("Error carregant entitats religioses publicades: %v", err)
		writeJSON(w, map[string]interface{}{"items": []interface{}{}})
		return
	}
	matches := make([]db.EntitatReligiosa, 0, len(all))
	if query == "" && arxiuID > 0 {
		matches = append(matches, related...)
	} else {
		needle := normalizeConfessionalSearchComparable(query)
		for _, item := range all {
			if needle != "" {
				hay := normalizeConfessionalSearchComparable(strings.Join([]string{item.Codi, item.Nom, item.ReligioConfessioCodi, item.NivellConfessionalCodi}, " "))
				ok := true
				for _, token := range strings.Fields(needle) {
					if !strings.Contains(hay, token) {
						ok = false
						break
					}
				}
				if !ok {
					continue
				}
			}
			matches = append(matches, item)
		}
		sort.SliceStable(matches, func(i, j int) bool {
			if relatedIDs[matches[i].ID] != relatedIDs[matches[j].ID] {
				return relatedIDs[matches[i].ID]
			}
			return strings.ToLower(strings.TrimSpace(matches[i].Nom)) < strings.ToLower(strings.TrimSpace(matches[j].Nom))
		})
	}
	if len(matches) > limit {
		matches = matches[:limit]
	}
	items := buildReligiousSuggestItems(matches, relatedIDs)
	writeJSON(w, map[string]interface{}{
		"items":                  items,
		"archive_related_count":  len(related),
		"archive_related_single": len(related) == 1,
		"archive_related_empty":  len(related) == 0,
		"archive_related_entity_id": func() interface{} {
			if len(related) == 1 {
				return related[0].ID
			}
			return nil
		}(),
	})
}

func (a *App) SearchBookMunicipisSuggestJSON(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	entitatReligiosaID := parseIntDefault(r.URL.Query().Get("entitat_religiosa_id"), 0)
	limit := parseSuggestLimit(r.URL.Query().Get("limit"))
	lang := ResolveLang(r)
	filter := db.MunicipiBrowseFilter{
		Text:   query,
		Status: "publicat",
		Limit:  limit,
	}
	scopeEmpty, scopeMessage, err := a.applyReligiousMunicipalityScope(lang, entitatReligiosaID, &filter)
	if err != nil {
		Errorf("Error carregant abast municipal d'entitat religiosa %d: %v", entitatReligiosaID, err)
		writeJSON(w, map[string]interface{}{"items": []interface{}{}})
		return
	}
	if scopeEmpty {
		writeJSON(w, map[string]interface{}{
			"items":         []interface{}{},
			"scope_empty":   true,
			"scope_message": scopeMessage,
		})
		return
	}
	if query == "" {
		filter.Text = ""
	}
	rows, err := a.DB.SuggestMunicipis(filter)
	if err != nil {
		Errorf("Error suggerint municipis per llibres: %v", err)
		writeJSON(w, map[string]interface{}{"items": []interface{}{}})
		return
	}
	items := buildMunicipiSuggestItems(rows)
	writeJSON(w, map[string]interface{}{
		"items":         items,
		"scope_empty":   scopeEmpty,
		"scope_message": scopeMessage,
	})
}

func parseSuggestLimit(raw string) int {
	limit := 10
	if val := strings.TrimSpace(raw); val != "" {
		if v, err := strconv.Atoi(val); err == nil && v > 0 && v <= 25 {
			limit = v
		}
	}
	return limit
}

func buildReligiousSuggestItems(items []db.EntitatReligiosa, relatedIDs map[int]bool) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(items))
	for _, item := range items {
		context := joinNonEmpty(strings.TrimSpace(item.Codi), joinNonEmpty(strings.TrimSpace(item.ReligioConfessioCodi), strings.TrimSpace(item.NivellConfessionalCodi), " · "), " · ")
		out = append(out, map[string]interface{}{
			"id":      item.ID,
			"nom":     strings.TrimSpace(item.Nom),
			"context": strings.TrimSpace(context),
			"related": relatedIDs[item.ID],
		})
	}
	return out
}

func buildMunicipiSuggestItems(rows []db.MunicipiSuggestRow) []map[string]interface{} {
	items := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		levelIDs := make([]interface{}, 7)
		levelNames := make([]interface{}, 7)
		levelTypes := make([]interface{}, 7)
		for i := 0; i < 7; i++ {
			if row.LevelIDs[i].Valid {
				levelIDs[i] = int(row.LevelIDs[i].Int64)
			}
			if row.LevelNames[i].Valid {
				levelNames[i] = strings.TrimSpace(row.LevelNames[i].String)
			}
			if row.LevelTypes[i].Valid {
				levelTypes[i] = strings.TrimSpace(row.LevelTypes[i].String)
			}
		}
		items = append(items, map[string]interface{}{
			"id":            row.ID,
			"nom":           strings.TrimSpace(row.Nom),
			"tipus":         strings.TrimSpace(row.Tipus),
			"pais_id":       row.PaisID,
			"nivells":       levelIDs,
			"nivells_nom":   levelNames,
			"nivells_tipus": levelTypes,
		})
	}
	return items
}

func (a *App) applyReligiousMunicipalityScope(lang string, entitatReligiosaID int, filter *db.MunicipiBrowseFilter) (bool, string, error) {
	if entitatReligiosaID <= 0 {
		return false, "", nil
	}
	allowed, err := a.municipalityScopeForReligiousEntity(entitatReligiosaID)
	if err != nil {
		return false, "", err
	}
	if len(allowed) == 0 {
		return true, T(lang, bookValidationReligiousScopeEmptyKey), nil
	}
	filter.AllowedMunicipiIDs = make([]int, 0, len(allowed))
	for municipiID := range allowed {
		filter.AllowedMunicipiIDs = append(filter.AllowedMunicipiIDs, municipiID)
	}
	sort.Ints(filter.AllowedMunicipiIDs)
	return false, "", nil
}

func intFromForm(val string) int {
	if v, err := strconv.Atoi(strings.TrimSpace(val)); err == nil {
		return v
	}
	return 0
}

type llibreArxiuLinkSaver interface {
	SaveArxiuLlibreLink(link *db.ArxiuLlibreLink) error
}

func (a *App) saveLlibreArxiuLink(link *db.ArxiuLlibreLink) error {
	if a == nil || a.DB == nil || link == nil {
		return nil
	}
	if saver, ok := a.DB.(llibreArxiuLinkSaver); ok {
		return saver.SaveArxiuLlibreLink(link)
	}
	return a.DB.UpdateArxiuLlibre(link.ArxiuID, link.LlibreID, link.Signatura, link.URLOverride)
}

func (a *App) currentLlibreArxiuLinks(llibreID int) ([]db.ArxiuLlibreLink, error) {
	rels, err := a.DB.ListLlibreArxius(llibreID)
	if err != nil {
		return nil, err
	}
	links := make([]db.ArxiuLlibreLink, 0, len(rels))
	for _, rel := range rels {
		link := db.ArxiuLlibreLink{
			ArxiuID:               rel.ArxiuID,
			LlibreID:              rel.LlibreID,
			TipusRelacio:          strings.TrimSpace(rel.TipusRelacio),
			Principal:             rel.Principal,
			PreferitVisualitzacio: rel.PreferitVisualitzacio,
			Signatura:             strings.TrimSpace(rel.Signatura.String),
			URLOverride:           strings.TrimSpace(rel.URLOverride.String),
			Estat:                 strings.TrimSpace(rel.Estat.String),
			ModeracioEstat:        strings.TrimSpace(rel.ModeracioEstat.String),
			CreatedBy:             rel.CreatedBy,
			UpdatedBy:             rel.UpdatedBy,
			ModeratedBy:           rel.ModeratedBy,
			ModeratedAt:           rel.ModeratedAt,
		}
		if rel.SourceSystem.Valid {
			link.SourceSystem = strings.TrimSpace(rel.SourceSystem.String)
		}
		if rel.ExternalID.Valid {
			link.ExternalID = strings.TrimSpace(rel.ExternalID.String)
		}
		if rel.ExternalCode.Valid {
			link.ExternalCode = strings.TrimSpace(rel.ExternalCode.String)
		}
		if rel.Notes.Valid {
			link.Notes = strings.TrimSpace(rel.Notes.String)
		}
		links = append(links, link)
	}
	return normalizeLlibreArxiuLinks(links), nil
}

func normalizeLlibreArxiuLinks(links []db.ArxiuLlibreLink) []db.ArxiuLlibreLink {
	if len(links) == 0 {
		return nil
	}
	byArchive := make(map[int]db.ArxiuLlibreLink, len(links))
	order := make([]int, 0, len(links))
	for _, link := range links {
		if link.ArxiuID <= 0 {
			continue
		}
		link.TipusRelacio = strings.TrimSpace(link.TipusRelacio)
		if link.TipusRelacio == "" {
			link.TipusRelacio = "custodia_original"
		}
		link.Signatura = strings.TrimSpace(link.Signatura)
		link.URLOverride = strings.TrimSpace(link.URLOverride)
		link.SourceSystem = strings.TrimSpace(link.SourceSystem)
		link.ExternalID = strings.TrimSpace(link.ExternalID)
		link.ExternalCode = strings.TrimSpace(link.ExternalCode)
		link.Notes = strings.TrimSpace(link.Notes)
		link.Estat = strings.TrimSpace(link.Estat)
		if link.Estat == "" {
			link.Estat = "actiu"
		}
		link.ModeracioEstat = strings.TrimSpace(link.ModeracioEstat)
		if link.ModeracioEstat == "" {
			link.ModeracioEstat = "publicat"
		}
		if _, ok := byArchive[link.ArxiuID]; !ok {
			order = append(order, link.ArxiuID)
		}
		byArchive[link.ArxiuID] = link
	}
	if len(order) == 0 {
		return nil
	}
	sort.Ints(order)
	out := make([]db.ArxiuLlibreLink, 0, len(order))
	primaryIdx := -1
	preferredIdx := -1
	for _, arxiuID := range order {
		link := byArchive[arxiuID]
		if link.Principal && primaryIdx < 0 {
			primaryIdx = len(out)
		}
		if link.PreferitVisualitzacio && preferredIdx < 0 {
			preferredIdx = len(out)
		}
		link.Principal = false
		link.PreferitVisualitzacio = false
		out = append(out, link)
	}
	if preferredIdx < 0 {
		preferredIdx = 0
	}
	if primaryIdx < 0 {
		primaryIdx = 0
	}
	out[primaryIdx].Principal = true
	out[preferredIdx].PreferitVisualitzacio = true
	return out
}

func (a *App) buildLlibreWikiMeta(existing, after *db.Llibre, links []db.ArxiuLlibreLink) (string, error) {
	beforeJSON, err := json.Marshal(existing)
	if err != nil {
		return "", err
	}
	afterJSON, err := json.Marshal(after)
	if err != nil {
		return "", err
	}
	meta := wikiChangeMeta{
		Before:       beforeJSON,
		After:        afterJSON,
		ArchiveLinks: normalizeLlibreArxiuLinks(links),
	}
	payload, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func (a *App) queueLlibreArchiveLinksChange(r *http.Request, user *db.User, existing *db.Llibre, links []db.ArxiuLlibreLink) error {
	if existing == nil {
		return nil
	}
	after := *existing
	after.ModeracioEstat = "pendent"
	after.ModeracioMotiu = ""
	after.ModeratedBy = sql.NullInt64{}
	after.ModeratedAt = sql.NullTime{}
	meta, err := a.buildLlibreWikiMeta(existing, &after, links)
	if err != nil {
		return err
	}
	changeID, err := a.createWikiChange(&db.WikiChange{
		ObjectType:     "llibre",
		ObjectID:       existing.ID,
		ChangeType:     "relations",
		FieldKey:       "arxius",
		Metadata:       meta,
		ModeracioEstat: "pendent",
		ChangedBy:      sqlNullIntFromInt(user.ID),
	})
	if err != nil {
		return err
	}
	detail := "llibre:" + strconv.Itoa(existing.ID)
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleLlibreUpdate, "editar", "llibre_canvi", &changeID, "pendent", nil, detail)
	return nil
}

func (a *App) AdminSaveLlibre(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/documentals/llibres", http.StatusSeeOther)
		return
	}
	llibre := parseLlibreForm(r)
	if isCivilBookType(llibre.TipusLlibre) {
		llibre.ArquebisbatID = 0
	}
	arxiuID := intFromForm(r.FormValue("arxiu_id"))
	entitatReligiosaID := intFromForm(r.FormValue("entitat_religiosa_id"))
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
	lang := ResolveLang(r)
	formState := a.llibreFormStateFromRequest(lang, llibre, arxiuID, entitatReligiosaID)
	if msg := a.validateLlibre(lang, llibre, arxiuID, entitatReligiosaID); msg != "" {
		a.renderLlibreForm(w, r, llibre, isNew, msg, returnURL, formState)
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
			a.renderLlibreForm(w, r, llibre, isNew, "No s'ha pogut crear el llibre.", returnURL, formState)
			return
		}
		if arxiuID > 0 {
			_ = a.saveLlibreArxiuLink(&db.ArxiuLlibreLink{
				ArxiuID:               arxiuID,
				LlibreID:              id,
				Principal:             true,
				PreferitVisualitzacio: true,
				CreatedBy:             sqlNullIntFromInt(user.ID),
				UpdatedBy:             sqlNullIntFromInt(user.ID),
			})
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
			a.renderLlibreForm(w, r, llibre, isNew, "No s'ha pogut carregar el llibre existent.", returnURL, formState)
			return
		}
		if isCivilBookType(llibre.TipusLlibre) {
			llibre.ArquebisbatID = 0
		} else {
			llibre.ArquebisbatID = existing.ArquebisbatID
		}
		archiveLinks, err := a.currentLlibreArxiuLinks(llibre.ID)
		if err != nil {
			a.renderLlibreForm(w, r, llibre, isNew, "No s'han pogut carregar els arxius del llibre.", returnURL, formState)
			return
		}
		if arxiuID > 0 {
			updated := false
			for i := range archiveLinks {
				if archiveLinks[i].ArxiuID == arxiuID {
					archiveLinks[i].UpdatedBy = sqlNullIntFromInt(user.ID)
					updated = true
					break
				}
			}
			if !updated {
				archiveLinks = append(archiveLinks, db.ArxiuLlibreLink{
					ArxiuID:               arxiuID,
					LlibreID:              llibre.ID,
					Principal:             len(archiveLinks) == 0,
					PreferitVisualitzacio: len(archiveLinks) == 0,
					CreatedBy:             sqlNullIntFromInt(user.ID),
					UpdatedBy:             sqlNullIntFromInt(user.ID),
				})
			}
		}
		archiveLinks = normalizeLlibreArxiuLinks(archiveLinks)
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
			metaJSON, err := a.buildLlibreWikiMeta(existing, &after, archiveLinks)
			if err != nil {
				a.renderLlibreForm(w, r, llibre, isNew, "No s'ha pogut preparar el canvi del llibre.", returnURL, formState)
				return
			}
			changeID, err := a.createWikiChange(&db.WikiChange{
				ObjectType:     "llibre",
				ObjectID:       llibre.ID,
				ChangeType:     "form",
				FieldKey:       "bulk",
				Metadata:       metaJSON,
				ModeracioEstat: "pendent",
				ChangedBy:      sqlNullIntFromInt(user.ID),
			})
			if err != nil {
				if _, msg, ok := a.wikiGuardrailInfo(lang, err); ok {
					a.renderLlibreForm(w, r, llibre, isNew, msg, returnURL, formState)
					return
				}
				Errorf("Error creant proposta llibre: %v", err)
				a.renderLlibreForm(w, r, llibre, isNew, "No s'ha pogut crear la proposta de canvi.", returnURL, formState)
				return
			}
			detail := "llibre:" + strconv.Itoa(llibre.ID)
			_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleLlibreUpdate, "editar", "llibre_canvi", &changeID, "pendent", nil, detail)
		} else {
			if err := a.DB.UpdateLlibre(llibre); err != nil {
				Errorf("Error actualitzant llibre: %v", err)
				a.renderLlibreForm(w, r, llibre, isNew, "No s'ha pogut actualitzar el llibre.", returnURL, formState)
				return
			}
			for i := range archiveLinks {
				archiveLinks[i].LlibreID = llibre.ID
				archiveLinks[i].UpdatedBy = sqlNullIntFromInt(user.ID)
				if !archiveLinks[i].CreatedBy.Valid {
					archiveLinks[i].CreatedBy = sqlNullIntFromInt(user.ID)
				}
				_ = a.saveLlibreArxiuLink(&archiveLinks[i])
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
		isAdmin = a.effectiveAdminForUser(user.ID)
		canModerate = a.canModerateModular(user)
	}
	canEditLlibre := user != nil && a.HasPermission(user.ID, permKeyDocumentalsLlibresEdit, target)
	canDeleteLlibre := user != nil && a.HasPermission(user.ID, permKeyDocumentalsLlibresDelete, target)
	canIndexLlibre := user != nil && a.HasPermission(user.ID, permKeyDocumentalsLlibresBulkIndex, target)
	canViewRegistres := user != nil && a.HasPermission(user.ID, permKeyDocumentalsLlibresViewRegistres, target)
	canImportCSV := user != nil && a.HasPermission(user.ID, permKeyDocumentalsLlibresImportCSV, target)
	canExportCSV := user != nil && a.HasPermission(user.ID, permKeyDocumentalsLlibresExportCSV, target)
	canMarkIndexed := user != nil && a.HasPermission(user.ID, permKeyDocumentalsLlibresMarkIndexed, target)
	canRecalcIndex := user != nil && a.HasPermission(user.ID, permKeyDocumentalsLlibresRecalcIndex, target)
	canManageLlibre := canEditLlibre || canDeleteLlibre || canIndexLlibre || canImportCSV || canExportCSV || canMarkIndexed || canRecalcIndex
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
	if (user == nil || !canManageLlibre) && llibre.ModeracioEstat != "publicat" {
		http.NotFound(w, r)
		return
	}
	statusFilter := ""
	if user == nil || !canManageLlibre {
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
	pageMediaLinks := map[int64][]db.MediaItemPageLink{}
	if len(pageStatsPage) > 0 {
		for _, stat := range pageStatsPage {
			if !stat.PaginaID.Valid {
				continue
			}
			pageID := stat.PaginaID.Int64
			if _, ok := pageMediaLinks[pageID]; ok {
				continue
			}
			if links, err := a.DB.ListMediaItemLinksByPagina(int(pageID)); err == nil {
				links = a.filterMediaItemLinks(r, user, links)
				if len(links) > 0 {
					pageMediaLinks[pageID] = links
				}
			}
		}
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
	linksErrorKey := ""
	switch strings.TrimSpace(r.URL.Query().Get("links")) {
	case "error":
		linksErrorKey = "books.links.error.url"
	case "missing_book":
		linksErrorKey = "books.links.error.missing_book"
	case "no_books":
		linksErrorKey = "books.links.error.no_books"
	case "invalid_book":
		linksErrorKey = "books.links.error.invalid_book"
	}
	entityName := ""
	if llibre.ArquebisbatID > 0 {
		if ae, err := a.DB.GetArquebisbat(llibre.ArquebisbatID); err == nil && ae != nil {
			entityName = ae.Nom
		}
	}
	derivedReligiousContext := a.deriveLlibreReligiousContext(id)
	if entityName == "" {
		entityName = derivedReligiousContext.Name
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
	internalAlbum := (*db.MediaAlbum)(nil)
	if albums, err := a.DB.ListMediaAlbumsByLlibre(id); err == nil {
		for i := range albums {
			album := &albums[i]
			if a.mediaUserCanAccess(r, user, album) {
				internalAlbum = album
				break
			}
		}
	}
	RenderPrivateTemplate(w, r, "admin-llibres-show.html", map[string]interface{}{
		"Llibre":              llibre,
		"LlibreEntityName":    entityName,
		"LlibreReligiousCode": derivedReligiousContext.Code,
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
		"PageMediaLinks":      pageMediaLinks,
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
		"InternalAlbum":       internalAlbum,
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
		"LinksErrorKey":       linksErrorKey,
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
		http.Error(w, csrfInvalidMessage, http.StatusBadRequest)
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
		http.Error(w, csrfInvalidMessage, http.StatusBadRequest)
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
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresEdit, target)
	if !ok {
		return
	}
	arxiuID, _ := strconv.Atoi(r.FormValue("arxiu_id"))
	signatura := strings.TrimSpace(r.FormValue("signatura"))
	urlOverride := strings.TrimSpace(r.FormValue("url_override"))
	if llibreID == 0 || arxiuID == 0 {
		http.Redirect(w, r, "/documentals/llibres", http.StatusSeeOther)
		return
	}
	llibre, err := a.DB.GetLlibre(llibreID)
	if err == nil && llibre != nil && llibre.ModeracioEstat == "publicat" {
		lang := resolveUserLang(r, user)
		if !a.ensureWikiChangeAllowed(w, r, lang) {
			return
		}
		links, err := a.currentLlibreArxiuLinks(llibreID)
		if err == nil {
			links = append(links, db.ArxiuLlibreLink{
				ArxiuID:               arxiuID,
				LlibreID:              llibreID,
				Signatura:             signatura,
				URLOverride:           urlOverride,
				Principal:             len(links) == 0,
				PreferitVisualitzacio: len(links) == 0,
				CreatedBy:             sqlNullIntFromInt(user.ID),
				UpdatedBy:             sqlNullIntFromInt(user.ID),
			})
			if err := a.queueLlibreArchiveLinksChange(r, user, llibre, links); err == nil {
				http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID), http.StatusSeeOther)
				return
			}
		}
	}
	existingLinks, err := a.currentLlibreArxiuLinks(llibreID)
	hasExistingLinks := err == nil && len(existingLinks) > 0
	_ = a.saveLlibreArxiuLink(&db.ArxiuLlibreLink{
		ArxiuID:               arxiuID,
		LlibreID:              llibreID,
		Signatura:             signatura,
		URLOverride:           urlOverride,
		Principal:             !hasExistingLinks,
		PreferitVisualitzacio: !hasExistingLinks,
		CreatedBy:             sqlNullIntFromInt(user.ID),
		UpdatedBy:             sqlNullIntFromInt(user.ID),
	})
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
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresEdit, target)
	if !ok {
		return
	}
	arxiuID, _ := strconv.Atoi(parts[4])
	signatura := strings.TrimSpace(r.FormValue("signatura"))
	urlOverride := strings.TrimSpace(r.FormValue("url_override"))
	llibre, err := a.DB.GetLlibre(llibreID)
	if err == nil && llibre != nil && llibre.ModeracioEstat == "publicat" {
		lang := resolveUserLang(r, user)
		if !a.ensureWikiChangeAllowed(w, r, lang) {
			return
		}
		links, err := a.currentLlibreArxiuLinks(llibreID)
		if err == nil {
			for i := range links {
				if links[i].ArxiuID != arxiuID {
					continue
				}
				links[i].Signatura = signatura
				links[i].URLOverride = urlOverride
				links[i].UpdatedBy = sqlNullIntFromInt(user.ID)
				if err := a.queueLlibreArchiveLinksChange(r, user, llibre, links); err == nil {
					http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID), http.StatusSeeOther)
					return
				}
				break
			}
		}
	}
	_ = a.saveLlibreArxiuLink(&db.ArxiuLlibreLink{
		ArxiuID:     arxiuID,
		LlibreID:    llibreID,
		Signatura:   signatura,
		URLOverride: urlOverride,
		UpdatedBy:   sqlNullIntFromInt(user.ID),
	})
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
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresEdit, target)
	if !ok {
		return
	}
	arxiuID, _ := strconv.Atoi(parts[4])
	llibre, err := a.DB.GetLlibre(llibreID)
	if err == nil && llibre != nil && llibre.ModeracioEstat == "publicat" {
		lang := resolveUserLang(r, user)
		if !a.ensureWikiChangeAllowed(w, r, lang) {
			return
		}
		links, err := a.currentLlibreArxiuLinks(llibreID)
		if err == nil {
			filtered := make([]db.ArxiuLlibreLink, 0, len(links))
			for _, link := range links {
				if link.ArxiuID != arxiuID {
					filtered = append(filtered, link)
				}
			}
			if err := a.queueLlibreArchiveLinksChange(r, user, llibre, filtered); err == nil {
				http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID), http.StatusSeeOther)
				return
			}
		}
	}
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
		http.Error(w, csrfInvalidMessage, http.StatusBadRequest)
		return
	}
	if llibreID == 0 {
		http.Redirect(w, r, "/documentals/llibres", http.StatusSeeOther)
		return
	}
	returnTo := safeReturnTo(r.FormValue("return_to"), "/documentals/llibres/"+strconv.Itoa(llibreID))
	redirectWithError := func(code string) {
		http.Redirect(w, r, withQueryParams(returnTo, map[string]string{"links": code}), http.StatusSeeOther)
	}
	arxiuID := parseNullInt64(r.FormValue("arxiu_id"))
	if arxiuID.Valid && arxiuID.Int64 <= 0 {
		arxiuID = sql.NullInt64{}
	}
	llibreRefID := parseNullInt64(r.FormValue("llibre_ref_id"))
	if llibreRefID.Valid && llibreRefID.Int64 <= 0 {
		llibreRefID = sql.NullInt64{}
	}
	url := strings.TrimSpace(r.FormValue("url"))
	if !arxiuID.Valid && url == "" {
		redirectWithError("error")
		return
	}
	if arxiuID.Valid {
		if !llibreRefID.Valid {
			redirectWithError("missing_book")
			return
		}
		arxiuBooks, err := a.DB.ListArxiuLlibres(int(arxiuID.Int64))
		if err != nil {
			Errorf("Error carregant llibres d'arxiu %d: %v", arxiuID.Int64, err)
			redirectWithError("error")
			return
		}
		if len(arxiuBooks) == 0 {
			redirectWithError("no_books")
			return
		}
		found := false
		var selectedBook *db.ArxiuLlibreDetail
		for _, row := range arxiuBooks {
			if row.LlibreID == int(llibreRefID.Int64) {
				found = true
				selectedBook = &row
				break
			}
		}
		if !found {
			redirectWithError("invalid_book")
			return
		}
		if url == "" && selectedBook != nil && selectedBook.URLOverride.Valid {
			url = strings.TrimSpace(selectedBook.URLOverride.String)
		}
		if url == "" {
			refBook, err := a.DB.GetLlibre(int(llibreRefID.Int64))
			if err == nil && refBook != nil {
				url = strings.TrimSpace(refBook.URLBase)
			}
		}
		if url == "" {
			url = "/documentals/llibres/" + strconv.Itoa(int(llibreRefID.Int64))
		}
	} else {
		llibreRefID = sql.NullInt64{}
	}
	tipus := parseNullString(r.FormValue("tipus"))
	descripcio := parseNullString(r.FormValue("descripcio"))
	createdBy := sql.NullInt64{}
	if user, _ := a.VerificarSessio(r); user != nil {
		createdBy = sql.NullInt64{Int64: int64(user.ID), Valid: true}
	}
	link := &db.LlibreURL{
		LlibreID:    llibreID,
		ArxiuID:     arxiuID,
		LlibreRefID: llibreRefID,
		URL:         url,
		Tipus:       tipus,
		Descripcio:  descripcio,
		CreatedBy:   createdBy,
	}
	_ = a.DB.AddLlibreURL(link)
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
		http.Error(w, csrfInvalidMessage, http.StatusBadRequest)
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
