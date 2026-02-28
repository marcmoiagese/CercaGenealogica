package core

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type municipiLevelSelect struct {
	Level      int
	Label      string
	Options    []db.NivellAdministratiu
	SelectedID int
}

type municipiVisibleCol struct {
	Level   int
	Label   string
	SortKey string
}

func (a *App) AdminListMunicipis(w http.ResponseWriter, r *http.Request) {
	filter := db.MunicipiBrowseFilter{
		Text:    strings.TrimSpace(r.URL.Query().Get("q")),
		Estat:   strings.TrimSpace(r.URL.Query().Get("estat")),
		Status:  strings.TrimSpace(r.URL.Query().Get("status")),
		Tipus:   strings.TrimSpace(r.URL.Query().Get("tipus")),
		Sort:    strings.TrimSpace(r.URL.Query().Get("sort")),
		SortDir: strings.TrimSpace(r.URL.Query().Get("dir")),
	}
	if filter.Status == "" {
		filter.Status = "publicat"
	}
	if filter.Sort == "" {
		filter.Sort = "nom"
	}
	if !isMunicipiSortKey(filter.Sort) {
		filter.Sort = "nom"
	}
	if filter.SortDir == "" || (!strings.EqualFold(filter.SortDir, "asc") && !strings.EqualFold(filter.SortDir, "desc")) {
		filter.SortDir = "asc"
	}
	if pid := strings.TrimSpace(r.URL.Query().Get("pais_id")); pid != "" {
		if v, err := strconv.Atoi(pid); err == nil {
			filter.PaisID = v
		}
	}
	for i := 0; i < 7; i++ {
		key := fmt.Sprintf("nivell_id_%d", i+1)
		if val := strings.TrimSpace(r.URL.Query().Get(key)); val != "" {
			if v, err := strconv.Atoi(val); err == nil {
				filter.LevelIDs[i] = v
			}
		}
	}
	if val := strings.TrimSpace(r.URL.Query().Get("municipi_id")); val != "" {
		if v, err := strconv.Atoi(val); err == nil {
			filter.MunicipiID = v
		}
	}
	if val := strings.TrimSpace(r.URL.Query().Get("focus_id")); val != "" {
		if v, err := strconv.Atoi(val); err == nil {
			filter.FocusID = v
		}
	}
	perPage := parseListPerPage(r.URL.Query().Get("per_page"))
	page := parseListPage(r.URL.Query().Get("page"))
	user, ok := a.requirePermissionKeyAnyScope(w, r, permKeyTerritoriMunicipisView)
	if !ok {
		return
	}
	scopeFilter := a.buildListScopeFilter(user.ID, permKeyTerritoriMunicipisView, ScopeMunicipi)
	perms := a.getPermissionsForUser(user.ID)
	if !scopeFilter.hasGlobal {
		if scopeFilter.isEmpty() {
			pagination := buildPagination(r, page, perPage, 0, "#page-stats-controls")
			RenderPrivateTemplate(w, r, "admin-municipis-list.html", map[string]interface{}{
				"Municipis":           []db.MunicipiBrowseRow{},
				"Filter":              filter,
				"Paisos":              []db.Pais{},
				"LevelSelects":        []municipiLevelSelect{},
				"VisibleLevelCols":    []municipiVisibleCol{},
				"ShowTipus":           false,
				"TipusOptions":        []municipiTypeOption{},
				"TipusLabels":         map[string]string{},
				"LevelTypeLabels":     []string{},
				"LevelNamesByID":      map[int][]string{},
				"BooksClassByID":      map[int]string{},
				"CanManageArxius":     a.hasPerm(perms, permArxius),
				"CanCreateMunicipi":   false,
				"CreatePaisID":        0,
				"CanEditMunicipi":     map[int]bool{},
				"ShowMunicipiActions": false,
				"HasFilters":          false,
				"SortKey":             filter.Sort,
				"SortDir":             filter.SortDir,
				"SortLinks":           map[string]string{},
				"Page":                pagination.Page,
				"PerPage":             pagination.PerPage,
				"Total":               pagination.Total,
				"TotalPages":          pagination.TotalPages,
				"PageLinks":           pagination.Links,
				"PageSelectBase":      pagination.SelectBase,
				"PageAnchor":          pagination.Anchor,
				"ReturnURL":           currentRequestURL(r),
				"User":                user,
			})
			return
		}
		filter.AllowedMunicipiIDs = scopeFilter.municipiIDs
		filter.AllowedProvinciaIDs = scopeFilter.provinciaIDs
		filter.AllowedComarcaIDs = scopeFilter.comarcaIDs
		filter.AllowedNivellIDs = scopeFilter.nivellIDs
		filter.AllowedPaisIDs = scopeFilter.paisIDs
	}
	if filter.PaisID == 0 {
		for _, id := range filter.LevelIDs {
			if id <= 0 {
				continue
			}
			if nivell, err := a.DB.GetNivell(id); err == nil && nivell != nil && nivell.PaisID > 0 {
				filter.PaisID = nivell.PaisID
				break
			}
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
	var nivells []db.NivellAdministratiu
	if filter.PaisID > 0 {
		nivells = a.municipiLevelsForPais(filter.PaisID)
	}
	levelSelects := []municipiLevelSelect{}
	visibleCols := []municipiVisibleCol{}
	showTipus := false
	levelTypeLabels := []string{}
	if filter.PaisID > 0 {
		levelSelects, visibleCols, showTipus, levelTypeLabels = buildMunicipiLevelUI(nivells, filter.LevelIDs)
	}
	createPaisID := filter.PaisID
	canCreateMunicipi := false
	if createPaisID > 0 {
		canCreateMunicipi = a.HasPermission(user.ID, permKeyTerritoriMunicipisCreate, PermissionTarget{PaisID: intPtr(createPaisID)})
	} else {
		canCreateMunicipi = a.HasPermission(user.ID, permKeyTerritoriMunicipisCreate, PermissionTarget{})
	}
	hasFilters := filter.PaisID > 0 || filter.Text != ""
	if filter.MunicipiID > 0 || strings.TrimSpace(filter.Tipus) != "" {
		hasFilters = true
	}
	if !hasFilters {
		for _, id := range filter.LevelIDs {
			if id > 0 {
				hasFilters = true
				break
			}
		}
	}
	var (
		muns            []db.MunicipiBrowseRow
		total           int
		levelNamesByID  = map[int][]string{}
		booksClassByID  = map[int]string{}
		canEditMunicipi = map[int]bool{}
		showActions     = false
		pagination      Pagination
	)
	if hasFilters {
		total, _ = a.DB.CountMunicipisBrowse(filter)
		pagination = buildPagination(r, page, perPage, total, "#page-stats-controls")
		filter.Limit = pagination.PerPage
		filter.Offset = pagination.Offset
		muns, _ = a.DB.ListMunicipisBrowse(filter)
		for _, mun := range muns {
			names := make([]string, 7)
			for i := 0; i < 7; i++ {
				if mun.LevelNames[i].Valid {
					names[i] = strings.TrimSpace(mun.LevelNames[i].String)
				}
			}
			levelNamesByID[mun.ID] = names
			booksClassByID[mun.ID] = progressClassForPercent(mun.RegistresIndexats)
			munTarget := PermissionTarget{MunicipiID: intPtr(mun.ID)}
			if mun.LevelIDs[2].Valid {
				munTarget.ProvinciaID = intPtr(int(mun.LevelIDs[2].Int64))
			}
			if mun.LevelIDs[3].Valid {
				munTarget.ComarcaID = intPtr(int(mun.LevelIDs[3].Int64))
			}
			if mun.LevelIDs[0].Valid {
				munTarget.PaisID = intPtr(int(mun.LevelIDs[0].Int64))
			}
			canEdit := a.HasPermission(user.ID, permKeyTerritoriMunicipisEdit, munTarget)
			canEditMunicipi[mun.ID] = canEdit
			if canEdit {
				showActions = true
			}
		}
	} else {
		pagination = buildPagination(r, page, perPage, 0, "#page-stats-controls")
	}
	knownTypes := []string{"nucli_urba", "urbanitzacio", "masia", "poble", "ciutat", "barri", "llogaret"}
	typeOptions := make([]municipiTypeOption, 0, len(knownTypes))
	typeLabels := map[string]string{}
	lang := ResolveLang(r)
	for _, typ := range knownTypes {
		label := T(lang, fmt.Sprintf("municipis.type.%s", typ))
		typeLabels[typ] = label
		typeOptions = append(typeOptions, municipiTypeOption{Value: typ, Label: label})
	}
	sortKeys := []string{"pais", "nom"}
	for _, col := range visibleCols {
		sortKeys = append(sortKeys, col.SortKey)
	}
	sortLinks := buildMunicipiSortLinks(r, sortKeys, filter.Sort, filter.SortDir)
	RenderPrivateTemplate(w, r, "admin-municipis-list.html", map[string]interface{}{
		"Municipis":           muns,
		"Filter":              filter,
		"Paisos":              paisos,
		"LevelSelects":        levelSelects,
		"VisibleLevelCols":    visibleCols,
		"ShowTipus":           showTipus,
		"TipusOptions":        typeOptions,
		"TipusLabels":         typeLabels,
		"LevelTypeLabels":     levelTypeLabels,
		"LevelNamesByID":      levelNamesByID,
		"BooksClassByID":      booksClassByID,
		"CanManageArxius":     a.hasPerm(perms, permArxius),
		"CanCreateMunicipi":   canCreateMunicipi,
		"CreatePaisID":        createPaisID,
		"CanEditMunicipi":     canEditMunicipi,
		"ShowMunicipiActions": showActions,
		"HasFilters":          hasFilters,
		"SortKey":             filter.Sort,
		"SortDir":             filter.SortDir,
		"SortLinks":           sortLinks,
		"Page":                pagination.Page,
		"PerPage":             pagination.PerPage,
		"Total":               pagination.Total,
		"TotalPages":          pagination.TotalPages,
		"PageLinks":           pagination.Links,
		"PageSelectBase":      pagination.SelectBase,
		"PageAnchor":          pagination.Anchor,
		"ReturnURL":           currentRequestURL(r),
		"User":                user,
	})
}

type municipiTypeOption struct {
	Value string
	Label string
}

func buildMunicipiLevelUI(levels []db.NivellAdministratiu, selected [7]int) ([]municipiLevelSelect, []municipiVisibleCol, bool, []string) {
	levelTypeLabels := make([]string, 7)
	if len(levels) == 0 {
		return nil, nil, true, levelTypeLabels
	}
	levelsByNumber := map[int][]db.NivellAdministratiu{}
	minLevel := 0
	maxLevel := 0
	for _, lvl := range levels {
		if lvl.Nivel <= 0 {
			continue
		}
		levelsByNumber[lvl.Nivel] = append(levelsByNumber[lvl.Nivel], lvl)
		if minLevel == 0 || lvl.Nivel < minLevel {
			minLevel = lvl.Nivel
		}
		if lvl.Nivel > maxLevel {
			maxLevel = lvl.Nivel
		}
	}
	if minLevel == 0 {
		return nil, nil, true, levelTypeLabels
	}
	startLevel := minLevel
	for level, opts := range levelsByNumber {
		if level < 1 || level > 7 || len(opts) == 0 {
			continue
		}
		label := strings.TrimSpace(opts[0].TipusNivell)
		if label == "" {
			label = fmt.Sprintf("Nivell %d", level)
		}
		levelTypeLabels[level-1] = label
	}
	for level := startLevel; level <= maxLevel; level++ {
		if len(levelsByNumber[level]) == 0 {
			continue
		}
		startLevel = level
		break
	}
	if startLevel > maxLevel || len(levelsByNumber[startLevel]) == 0 {
		return nil, nil, true, levelTypeLabels
	}
	var (
		selects   []municipiLevelSelect
		cols      []municipiVisibleCol
		showTipus bool
		parentID  int
	)
	for level := startLevel; level <= maxLevel; level++ {
		opts := levelsByNumber[level]
		if level > startLevel {
			if parentID <= 0 {
				break
			}
			filtered := make([]db.NivellAdministratiu, 0, len(opts))
			for _, opt := range opts {
				if opt.ParentID.Valid && int(opt.ParentID.Int64) == parentID {
					filtered = append(filtered, opt)
				}
			}
			opts = filtered
		}
		if len(opts) == 0 {
			break
		}
		label := levelTypeLabels[level-1]
		if label == "" {
			label = fmt.Sprintf("Nivell %d", level)
		}
		selectedID := 0
		if level >= 1 && level <= 7 {
			selectedID = selected[level-1]
		}
		selects = append(selects, municipiLevelSelect{
			Level:      level,
			Label:      label,
			Options:    opts,
			SelectedID: selectedID,
		})
		if level > 1 {
			cols = append(cols, municipiVisibleCol{
				Level:   level,
				Label:   label,
				SortKey: fmt.Sprintf("level%d", level),
			})
		}
		if selectedID <= 0 {
			break
		}
		nextLevel := level + 1
		if nextLevel > maxLevel {
			showTipus = true
			break
		}
		nextOpts := levelsByNumber[nextLevel]
		filteredNext := make([]db.NivellAdministratiu, 0, len(nextOpts))
		for _, opt := range nextOpts {
			if opt.ParentID.Valid && int(opt.ParentID.Int64) == selectedID {
				filteredNext = append(filteredNext, opt)
			}
		}
		if len(filteredNext) == 0 {
			showTipus = true
			break
		}
		parentID = selectedID
	}
	return selects, cols, showTipus, levelTypeLabels
}

func buildMunicipiSortLinks(r *http.Request, keys []string, currentKey, currentDir string) map[string]string {
	links := map[string]string{}
	for _, key := range keys {
		dir := "asc"
		if key == currentKey && strings.EqualFold(currentDir, "asc") {
			dir = "desc"
		}
		q := cloneValues(r.URL.Query())
		q.Set("sort", key)
		q.Set("dir", dir)
		q.Set("page", "1")
		url := r.URL.Path + "?" + q.Encode()
		if strings.TrimSpace(url) == "" {
			continue
		}
		url += "#page-stats-controls"
		links[key] = url
	}
	return links
}

func isMunicipiSortKey(key string) bool {
	switch strings.TrimSpace(key) {
	case "pais", "nom", "level1", "level2", "level3", "level4", "level5", "level6", "level7":
		return true
	default:
		return false
	}
}

func progressClassForPercent(percent int64) string {
	switch {
	case percent >= 75:
		return "verd"
	case percent >= 50:
		return "groc"
	case percent >= 25:
		return "taronja"
	default:
		return "rosa"
	}
}

func currentRequestURL(r *http.Request) string {
	if r == nil {
		return ""
	}
	if r.URL.RawQuery == "" {
		return r.URL.Path
	}
	return r.URL.Path + "?" + r.URL.RawQuery
}

func (a *App) municipiLevelsForPais(paisID int) []db.NivellAdministratiu {
	if a == nil || a.DB == nil || paisID <= 0 {
		return nil
	}
	levels, _ := a.DB.ListNivells(db.NivellAdminFilter{})
	if len(levels) == 0 {
		return nil
	}
	byID := make(map[int]db.NivellAdministratiu, len(levels))
	for _, lvl := range levels {
		byID[lvl.ID] = lvl
	}
	out := make([]db.NivellAdministratiu, 0, len(levels))
	for _, lvl := range levels {
		if resolveNivellPaisID(lvl.ID, byID) == paisID {
			out = append(out, lvl)
		}
	}
	return out
}

func resolveNivellPaisID(levelID int, byID map[int]db.NivellAdministratiu) int {
	seen := map[int]struct{}{}
	for levelID > 0 {
		if _, ok := seen[levelID]; ok {
			break
		}
		seen[levelID] = struct{}{}
		lvl, ok := byID[levelID]
		if !ok {
			break
		}
		if lvl.PaisID > 0 {
			return lvl.PaisID
		}
		if lvl.ParentID.Valid {
			levelID = int(lvl.ParentID.Int64)
			continue
		}
		break
	}
	return 0
}

func (a *App) AdminMunicipisSuggest(w http.ResponseWriter, r *http.Request) {
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	perms := a.getPermissionsForUser(user.ID)
	allowAll := false
	if !a.hasAnyPermissionKey(user.ID, permKeyTerritoriMunicipisView) {
		if !permPolicies(perms) {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		allowAll = true
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if len(query) < 1 {
		writeJSON(w, map[string]interface{}{"items": []interface{}{}})
		return
	}
	limit := 10
	if val := strings.TrimSpace(r.URL.Query().Get("limit")); val != "" {
		if v, err := strconv.Atoi(val); err == nil && v > 0 && v <= 25 {
			limit = v
		}
	}
	filter := db.MunicipiBrowseFilter{
		Text:   query,
		Status: "publicat",
		Limit:  limit,
	}
	if pid := strings.TrimSpace(r.URL.Query().Get("pais_id")); pid != "" {
		if v, err := strconv.Atoi(pid); err == nil {
			filter.PaisID = v
		}
	}
	scopeFilter := listScopeFilter{}
	if !allowAll {
		scopeFilter = a.buildListScopeFilter(user.ID, permKeyTerritoriMunicipisView, ScopeMunicipi)
		if !scopeFilter.hasGlobal && scopeFilter.isEmpty() {
			writeJSON(w, map[string]interface{}{"items": []interface{}{}})
			return
		}
		if !scopeFilter.hasGlobal {
			filter.AllowedMunicipiIDs = scopeFilter.municipiIDs
			filter.AllowedProvinciaIDs = scopeFilter.provinciaIDs
			filter.AllowedComarcaIDs = scopeFilter.comarcaIDs
			filter.AllowedNivellIDs = scopeFilter.nivellIDs
			filter.AllowedPaisIDs = scopeFilter.paisIDs
		}
	}
	rows, _ := a.DB.SuggestMunicipis(filter)
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
		item := map[string]interface{}{
			"id":            row.ID,
			"nom":           row.Nom,
			"tipus":         row.Tipus,
			"pais_id":       row.PaisID,
			"nivells":       levelIDs,
			"nivells_nom":   levelNames,
			"nivells_tipus": levelTypes,
		}
		if row.Latitud.Valid {
			item["lat"] = row.Latitud.Float64
		} else {
			item["lat"] = nil
		}
		if row.Longitud.Valid {
			item["lon"] = row.Longitud.Float64
		} else {
			item["lon"] = nil
		}
		items = append(items, item)
	}
	writeJSON(w, map[string]interface{}{"items": items})
}

func (a *App) AdminDeleteMunicipi(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	id := extractID(r.URL.Path)
	if id <= 0 {
		http.NotFound(w, r)
		return
	}
	target := a.resolveMunicipiTarget(id)
	if _, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisEdit, target); !ok {
		return
	}
	mun, err := a.DB.GetMunicipi(id)
	if err != nil || mun == nil {
		http.NotFound(w, r)
		return
	}
	mun.Estat = "inactiu"
	mun.ModeracioEstat = "pendent"
	if err := a.DB.UpdateMunicipi(mun); err != nil {
		http.Error(w, "No s'ha pogut eliminar", http.StatusInternalServerError)
		return
	}
	returnTo := strings.TrimSpace(r.FormValue("return_to"))
	if returnTo == "" {
		returnTo = "/territori/municipis"
	}
	http.Redirect(w, r, returnTo, http.StatusSeeOther)
}

func (a *App) AdminNewMunicipi(w http.ResponseWriter, r *http.Request) {
	target := PermissionTarget{}
	if pid := strings.TrimSpace(r.URL.Query().Get("pais_id")); pid != "" {
		if v, err := strconv.Atoi(pid); err == nil && v > 0 {
			target.PaisID = intPtr(v)
		}
	}
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisCreate, target)
	if !ok {
		return
	}
	returnURL := strings.TrimSpace(r.URL.Query().Get("return_to"))
	paisos, _ := a.DB.ListPaisos()
	var (
		levels        []db.NivellAdministratiu
		allLevels     []db.NivellAdministratiu
		allLevelsJSON []map[string]interface{}
		mun           = &db.Municipi{Estat: "actiu"}
	)
	allLevels, _ = a.DB.ListNivells(db.NivellAdminFilter{})
	for _, l := range allLevels {
		entry := map[string]interface{}{
			"ID": l.ID, "PaisID": l.PaisID, "Nivel": l.Nivel, "NomNivell": l.NomNivell, "TipusNivell": l.TipusNivell,
		}
		if l.ParentID.Valid {
			entry["ParentID"] = int(l.ParentID.Int64)
		}
		allLevelsJSON = append(allLevelsJSON, entry)
	}
	if pid := strings.TrimSpace(r.URL.Query().Get("pais_id")); pid != "" {
		if v, err := strconv.Atoi(pid); err == nil {
			levels, _ = a.DB.ListNivells(db.NivellAdminFilter{PaisID: v})
			mun.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(v), Valid: true}
		}
	}
	// Si no hi ha filtre, carrega nivells del primer país per donar referència visual
	if len(levels) == 0 && len(paisos) > 0 {
		levels, _ = a.DB.ListNivells(db.NivellAdminFilter{PaisID: paisos[0].ID})
	}
	arquebisbats, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	RenderPrivateTemplate(w, r, "admin-municipis-form.html", map[string]interface{}{
		"Municipi":        mun,
		"Paisos":          paisos,
		"Levels":          levels,
		"AllLevels":       allLevelsJSON,
		"ReturnURL":       returnURL,
		"Arquebisbats":    arquebisbats,
		"CodisPostals":    nil,
		"IsNew":           true,
		"CanManageArxius": true,
		"User":            user,
	})
}

func (a *App) AdminEditMunicipi(w http.ResponseWriter, r *http.Request) {
	returnURL := strings.TrimSpace(r.URL.Query().Get("return_to"))
	id := extractID(r.URL.Path)
	mun, err := a.DB.GetMunicipi(id)
	if err != nil || mun == nil {
		http.NotFound(w, r)
		return
	}
	target := a.resolveMunicipiTarget(mun.ID)
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisEdit, target)
	if !ok {
		return
	}
	paisos, _ := a.DB.ListPaisos()
	var levels []db.NivellAdministratiu
	allLevels, _ := a.DB.ListNivells(db.NivellAdminFilter{})
	var allLevelsJSON []map[string]interface{}
	for _, l := range allLevels {
		entry := map[string]interface{}{
			"ID": l.ID, "PaisID": l.PaisID, "Nivel": l.Nivel, "NomNivell": l.NomNivell, "TipusNivell": l.TipusNivell,
		}
		if l.ParentID.Valid {
			entry["ParentID"] = int(l.ParentID.Int64)
		}
		allLevelsJSON = append(allLevelsJSON, entry)
	}
	if pid := strings.TrimSpace(r.URL.Query().Get("pais_id")); pid != "" {
		if v, err := strconv.Atoi(pid); err == nil {
			mun.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(v), Valid: true}
		}
	}
	if mun.NivellAdministratiuID[0].Valid {
		levels, _ = a.DB.ListNivells(db.NivellAdminFilter{PaisID: int(mun.NivellAdministratiuID[0].Int64)})
	}
	codis, _ := a.DB.ListCodisPostals(mun.ID)
	ecles, _ := a.DB.ListArquebisbatMunicipis(mun.ID)
	arquebisbats, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	nomsH, _ := a.DB.ListNomsHistorics("municipi", mun.ID)
	var editEcles *db.ArquebisbatMunicipi
	if editParam := strings.TrimSpace(r.URL.Query().Get("edit_am")); editParam != "" {
		if editID, err := strconv.Atoi(editParam); err == nil {
			for _, el := range ecles {
				if el.ID == editID {
					editEcles = &el
					break
				}
			}
		}
	}
	RenderPrivateTemplate(w, r, "admin-municipis-form.html", map[string]interface{}{
		"Municipi":        mun,
		"Paisos":          paisos,
		"Levels":          levels,
		"AllLevels":       allLevelsJSON,
		"CodisPostals":    codis,
		"Ecles":           ecles,
		"Arquebisbats":    arquebisbats,
		"NomsHistorics":   nomsH,
		"EditEcles":       editEcles,
		"ReturnURL":       returnURL,
		"IsNew":           false,
		"CanManageArxius": true,
		"User":            user,
	})
}

func parseNullFloat(val string) sql.NullFloat64 {
	var n sql.NullFloat64
	if strings.TrimSpace(val) == "" {
		return n
	}
	if f, err := strconv.ParseFloat(val, 64); err == nil {
		n.Valid = true
		n.Float64 = f
	}
	return n
}

func (a *App) AdminSaveMunicipi(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/territori/municipis", http.StatusSeeOther)
		return
	}
	id, _ := strconv.Atoi(r.FormValue("id"))
	permKey := permKeyTerritoriMunicipisCreate
	if id != 0 {
		permKey = permKeyTerritoriMunicipisEdit
	}
	target := PermissionTarget{}
	if id != 0 {
		target = a.resolveMunicipiTarget(id)
	} else {
		if lvlStr := strings.TrimSpace(r.FormValue("nivell_administratiu_id_1")); lvlStr != "" {
			if lvlID, err := strconv.Atoi(lvlStr); err == nil && lvlID > 0 {
				if nivell, err := a.DB.GetNivell(lvlID); err == nil && nivell != nil && nivell.PaisID > 0 {
					target.PaisID = intPtr(nivell.PaisID)
				}
			}
		}
	}
	user, ok := a.requirePermissionKey(w, r, permKey, target)
	if !ok {
		return
	}
	returnURL := strings.TrimSpace(r.FormValue("return_to"))
	parent := parseNullInt(r.FormValue("municipi_id"))
	m := &db.Municipi{
		ID:             id,
		Nom:            strings.TrimSpace(r.FormValue("nom")),
		MunicipiID:     parent,
		Tipus:          strings.TrimSpace(r.FormValue("tipus")),
		CodiPostal:     strings.TrimSpace(r.FormValue("codi_postal")),
		Latitud:        parseNullFloat(r.FormValue("latitud")),
		Longitud:       parseNullFloat(r.FormValue("longitud")),
		What3Words:     strings.TrimSpace(r.FormValue("what3words")),
		Web:            strings.TrimSpace(r.FormValue("web")),
		Wikipedia:      strings.TrimSpace(r.FormValue("wikipedia")),
		Altres:         strings.TrimSpace(r.FormValue("altres")),
		Estat:          strings.TrimSpace(r.FormValue("estat")),
		CreatedBy:      sqlNullIntFromInt(user.ID),
		ModeracioEstat: "pendent",
		ModeratedBy:    sql.NullInt64{},
		ModeratedAt:    sql.NullTime{},
	}
	for i := 0; i < 7; i++ {
		field := strings.TrimSpace(r.FormValue("nivell_administratiu_id_" + strconv.Itoa(i+1)))
		if field != "" {
			m.NivellAdministratiuID[i] = parseNullInt(field)
		}
	}
	if !m.NivellAdministratiuID[0].Valid {
		a.renderMunicipiFormError(w, r, m, "Cal seleccionar un país i el primer nivell administratiu.", id == 0)
		return
	}
	if m.Estat == "" {
		m.Estat = "actiu"
	}
	if errMsg := a.validateMunicipi(m); errMsg != "" {
		a.renderMunicipiFormError(w, r, m, errMsg, id == 0)
		return
	}
	if m.ID == 0 {
		createdID, err := a.DB.CreateMunicipi(m)
		if err != nil {
			a.renderMunicipiFormError(w, r, m, "No s'ha pogut crear el municipi: "+err.Error(), true)
			return
		}
		m.ID = createdID
		a.rebuildAdminClosureForMunicipi(m)
		_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleMunicipiCreate, "crear", "municipi", &createdID, "pendent", nil, "")
	} else {
		existing, err := a.DB.GetMunicipi(m.ID)
		if err != nil || existing == nil {
			a.renderMunicipiFormError(w, r, m, "No s'ha pogut carregar el municipi existent.", false)
			return
		}
		if existing.ModeracioEstat == "publicat" {
			lang := resolveUserLang(r, user)
			if !a.ensureWikiChangeAllowed(w, r, lang) {
				return
			}
			after := *m
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
				a.renderMunicipiFormError(w, r, m, "No s'ha pogut preparar el canvi del municipi.", false)
				return
			}
			changeID, err := a.createWikiChange(&db.WikiChange{
				ObjectType:     "municipi",
				ObjectID:       m.ID,
				ChangeType:     "form",
				FieldKey:       "bulk",
				Metadata:       meta,
				ModeracioEstat: "pendent",
				ChangedBy:      sqlNullIntFromInt(user.ID),
			})
			if err != nil {
				if _, msg, ok := a.wikiGuardrailInfo(lang, err); ok {
					a.renderMunicipiFormError(w, r, m, msg, false)
					return
				}
				a.renderMunicipiFormError(w, r, m, "No s'ha pogut crear la proposta de canvi.", false)
				return
			}
			detail := fmt.Sprintf("municipi:%d", m.ID)
			_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleMunicipiUpdate, "editar", "municipi_canvi", &changeID, "pendent", nil, detail)
		} else {
			if err := a.DB.UpdateMunicipi(m); err != nil {
				a.renderMunicipiFormError(w, r, m, "No s'ha pogut actualitzar el municipi: "+err.Error(), false)
				return
			}
			a.rebuildAdminClosureForMunicipi(m)
			_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleMunicipiUpdate, "editar", "municipi", &id, "pendent", nil, "")
		}
	}
	if returnURL != "" {
		http.Redirect(w, r, returnURL, http.StatusSeeOther)
	} else {
		http.Redirect(w, r, "/territori/municipis", http.StatusSeeOther)
	}
}

func (a *App) validateMunicipi(m *db.Municipi) string {
	if strings.TrimSpace(m.Nom) == "" {
		return "El nom és obligatori."
	}
	if m.Tipus == "" {
		return "El tipus és obligatori."
	}
	if m.MunicipiID.Valid && m.ID != 0 && m.MunicipiID.Int64 == int64(m.ID) {
		return "Un municipi no pot ser pare de si mateix."
	}
	return ""
}

func (a *App) renderMunicipiFormError(w http.ResponseWriter, r *http.Request, m *db.Municipi, msg string, isNew bool) {
	paisos, _ := a.DB.ListPaisos()
	var levels []db.NivellAdministratiu
	if m.NivellAdministratiuID[0].Valid {
		levels, _ = a.DB.ListNivells(db.NivellAdminFilter{PaisID: int(m.NivellAdministratiuID[0].Int64)})
	}
	allLevels, _ := a.DB.ListNivells(db.NivellAdminFilter{})
	var allLevelsJSON []map[string]interface{}
	for _, l := range allLevels {
		entry := map[string]interface{}{
			"ID": l.ID, "PaisID": l.PaisID, "Nivel": l.Nivel, "NomNivell": l.NomNivell, "TipusNivell": l.TipusNivell,
		}
		if l.ParentID.Valid {
			entry["ParentID"] = int(l.ParentID.Int64)
		}
		allLevelsJSON = append(allLevelsJSON, entry)
	}
	var ecles []db.ArquebisbatMunicipi
	if !isNew && m.ID != 0 {
		ecles, _ = a.DB.ListArquebisbatMunicipis(m.ID)
	}
	arquebisbats, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	nomsH, _ := a.DB.ListNomsHistorics("municipi", m.ID)
	RenderPrivateTemplate(w, r, "admin-municipis-form.html", map[string]interface{}{
		"Municipi":        m,
		"Paisos":          paisos,
		"Levels":          levels,
		"AllLevels":       allLevelsJSON,
		"CodisPostals":    nil,
		"Ecles":           ecles,
		"Arquebisbats":    arquebisbats,
		"NomsHistorics":   nomsH,
		"Error":           msg,
		"IsNew":           isNew,
		"ReturnURL":       strings.TrimSpace(r.FormValue("return_to")),
		"CanManageArxius": true,
	})
}

func (a *App) AdminSaveCodiPostal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/territori/municipis", http.StatusSeeOther)
		return
	}
	munID := extractID(r.URL.Path)
	if munID == 0 {
		http.NotFound(w, r)
		return
	}
	target := a.resolveMunicipiTarget(munID)
	if _, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisEdit, target); !ok {
		return
	}
	_, err := a.DB.GetMunicipi(munID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	cpID, _ := strconv.Atoi(r.FormValue("cp_id"))
	cp := &db.CodiPostal{
		ID:         cpID,
		MunicipiID: munID,
		CodiPostal: strings.TrimSpace(r.FormValue("codi_postal")),
		Zona:       strings.TrimSpace(r.FormValue("zona")),
		Desde:      sql.NullString{String: strings.TrimSpace(r.FormValue("desde")), Valid: strings.TrimSpace(r.FormValue("desde")) != ""},
		Fins:       sql.NullString{String: strings.TrimSpace(r.FormValue("fins")), Valid: strings.TrimSpace(r.FormValue("fins")) != ""},
	}
	if cp.CodiPostal == "" {
		http.Redirect(w, r, "/territori/municipis/"+strconv.Itoa(munID)+"/edit?error=cp", http.StatusSeeOther)
		return
	}
	_, _ = a.DB.SaveCodiPostal(cp)
	http.Redirect(w, r, "/territori/municipis/"+strconv.Itoa(munID)+"/edit", http.StatusSeeOther)
}

func (a *App) AdminSaveMunicipiEcles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/territori/municipis", http.StatusSeeOther)
		return
	}
	munID := extractID(r.URL.Path)
	if munID == 0 {
		http.NotFound(w, r)
		return
	}
	target := a.resolveMunicipiTarget(munID)
	if _, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisEdit, target); !ok {
		return
	}
	_, err := a.DB.GetMunicipi(munID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	amID, _ := strconv.Atoi(r.FormValue("am_id"))
	arqID, _ := strconv.Atoi(r.FormValue("arquebisbat_id"))
	am := &db.ArquebisbatMunicipi{
		ID:            amID,
		MunicipiID:    munID,
		ArquebisbatID: arqID,
		AnyInici:      parseNullInt(r.FormValue("any_inici")),
		AnyFi:         parseNullInt(r.FormValue("any_fi")),
		Motiu:         strings.TrimSpace(r.FormValue("motiu")),
		Font:          strings.TrimSpace(r.FormValue("font")),
	}
	if am.ArquebisbatID == 0 {
		http.Redirect(w, r, "/territori/municipis/"+strconv.Itoa(munID)+"/edit?error=ecles", http.StatusSeeOther)
		return
	}
	_, _ = a.DB.SaveArquebisbatMunicipi(am)
	http.Redirect(w, r, "/territori/municipis/"+strconv.Itoa(munID)+"/edit", http.StatusSeeOther)
}

func (a *App) AdminSaveMunicipiNomHistoric(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/territori/municipis", http.StatusSeeOther)
		return
	}
	munID := extractID(r.URL.Path)
	if munID == 0 {
		http.NotFound(w, r)
		return
	}
	target := a.resolveMunicipiTarget(munID)
	if _, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisEdit, target); !ok {
		return
	}
	_, err := a.DB.GetMunicipi(munID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	nhID, _ := strconv.Atoi(r.FormValue("nh_id"))
	nh := &db.NomHistoric{
		ID:                    nhID,
		EntitatTipus:          "municipi",
		EntitatID:             munID,
		Nom:                   strings.TrimSpace(r.FormValue("nom")),
		AnyInici:              parseNullInt(r.FormValue("any_inici")),
		AnyFi:                 parseNullInt(r.FormValue("any_fi")),
		PaisRegne:             strings.TrimSpace(r.FormValue("pais_regne")),
		DistribucioGeografica: strings.TrimSpace(r.FormValue("distribucio_geografica")),
		Font:                  strings.TrimSpace(r.FormValue("font")),
	}
	if nh.Nom == "" {
		http.Redirect(w, r, "/territori/municipis/"+strconv.Itoa(munID)+"/edit?error=nomh", http.StatusSeeOther)
		return
	}
	_, _ = a.DB.SaveNomHistoric(nh)
	http.Redirect(w, r, "/territori/municipis/"+strconv.Itoa(munID)+"/edit", http.StatusSeeOther)
}
