package core

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type espaiPersonaTableRow struct {
	ID         int
	Name       string
	TreeName   string
	Visibility string
}

type espaiPeoplePager struct {
	Page       int
	PerPage    int
	Total      int
	TotalPages int
	HasPrev    bool
	HasNext    bool
	PrevPage   int
	NextPage   int
	PageBase   string
	PageSep    string
}

type espaiTreeRowView struct {
	ID             int
	Name           string
	Description    string
	Visibility     string
	PeopleCount    int
	RelationsCount int
	FamiliesCount  int
	GrampsIntegrationID int
	SourceType     string
	ImportStatus   string
	ImportType     string
}

type espaiTreeEditView struct {
	ID                  int
	Name                string
	SourceType          string
	GrampsIntegrationID int
	GrampsBaseURL       string
	GrampsUsername      string
	CloseURL            string
}

const espaiPeoplePerPage = 25

func buildEspaiPeoplePageBase(values url.Values) (string, string) {
	query := cloneValues(values)
	query.Del("page")
	base := "/espai"
	if len(query) == 0 {
		return base, "?"
	}
	return base + "?" + query.Encode(), "&"
}

func (a *App) EspaiPersonalOverviewPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	lang := ResolveLang(r)

	trees, _ := a.DB.ListEspaiArbresByOwner(user.ID)
	filterKeys := []string{"name", "tree", "visibility"}
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

	nameFilter := strings.TrimSpace(filterValues["name"])
	treeFilter := strings.TrimSpace(filterValues["tree"])
	visibilityFilter := strings.ToLower(strings.TrimSpace(filterValues["visibility"]))
	if !isValidPersonaVisibility(visibilityFilter) {
		visibilityFilter = ""
	}

	page := parseListPage(r.URL.Query().Get("page"))
	perPage := espaiPeoplePerPage

	total := 0
	if count, err := a.DB.CountEspaiPersonesByOwnerFilters(user.ID, nameFilter, treeFilter, visibilityFilter); err == nil {
		total = count
	}
	totalPages := 1
	if total > 0 {
		totalPages = (total + perPage - 1) / perPage
	}
	if page > totalPages {
		page = totalPages
	}
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * perPage

	rows, _ := a.DB.ListEspaiPersonesByOwnerFilters(user.ID, nameFilter, treeFilter, visibilityFilter, perPage, offset)
	people := make([]espaiPersonaTableRow, 0, len(rows))
	for _, row := range rows {
		visibility := strings.TrimSpace(row.Visibility)
		if visibility == "" {
			visibility = "visible"
		}
		people = append(people, espaiPersonaTableRow{
			ID:         row.ID,
			Name:       espaiPersonaDisplayNameWithFallback(row.EspaiPersona, T(lang, "tree.unknown.name")),
			TreeName:   strings.TrimSpace(row.TreeName),
			Visibility: visibility,
		})
	}

	pageBase, pageSep := buildEspaiPeoplePageBase(r.URL.Query())
	peoplePager := espaiPeoplePager{
		Page:       page,
		PerPage:    perPage,
		Total:      total,
		TotalPages: totalPages,
		HasPrev:    page > 1,
		HasNext:    page < totalPages,
		PrevPage:   page - 1,
		NextPage:   page + 1,
		PageBase:   pageBase,
		PageSep:    pageSep,
	}

	spaceState := "ready"
	if len(trees) == 0 {
		spaceState = "empty"
	}

	pendingMatches := 0
	if matches, err := a.DB.ListEspaiCoincidenciesByOwner(user.ID); err == nil {
		for _, m := range matches {
			if strings.TrimSpace(m.Status) == "pending" {
				pendingMatches++
			}
		}
	}

	syncFailures := 0
	if integracions, err := a.DB.ListEspaiIntegracionsGrampsByOwner(user.ID); err == nil {
		for _, integ := range integracions {
			if strings.TrimSpace(integ.Status) == "error" {
				syncFailures++
			}
		}
	}

	groupConflicts := 0
	if groups, err := a.DB.ListEspaiGrupsByUser(user.ID); err == nil {
		for _, g := range groups {
			conflicts, _ := a.DB.ListEspaiGrupConflictes(g.ID)
			for _, c := range conflicts {
				if strings.TrimSpace(c.Status) == "pending" {
					groupConflicts++
				}
			}
		}
	}

	notifications, unread := a.listEspaiNotificationViews(user.ID, lang, 20)
	prefs := a.loadEspaiNotificationPrefs(user.ID)

	RenderPrivateTemplate(w, r, "espai.html", map[string]interface{}{
		"SpaceSection":      "overview",
		"SpaceState":        spaceState,
		"EspaiPeople":       people,
		"EspaiPeoplePager":  peoplePager,
		"EspaiPeopleFilterValues": filterValues,
		"EspaiPeopleFilterOrder":  strings.Join(filterOrder, ","),
		"EspaiReturnTo":     r.URL.RequestURI(),
		"EspaiNotifications": notifications,
		"EspaiNotificationUnread": unread,
		"EspaiNotificationPrefs": prefs,
		"EspaiOverviewCounts": espaiOverviewCounts{
			PendingMatches: pendingMatches,
			SyncFailures:   syncFailures,
			GroupConflicts: groupConflicts,
			UnreadAlerts:   unread,
		},
		"UploadError":       strings.TrimSpace(r.URL.Query().Get("error")),
		"UploadNotice":      strings.TrimSpace(r.URL.Query().Get("notice")),
	})
}

func (a *App) EspaiPersonalTreesPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	lang := ResolveLang(r)

	trees, _ := a.DB.ListEspaiArbresByOwner(user.ID)
	grampsByTree := map[int]db.EspaiIntegracioGramps{}
	if integrations, err := a.DB.ListEspaiIntegracionsGrampsByOwner(user.ID); err == nil {
		for _, integ := range integrations {
			if integ.ArbreID > 0 {
				grampsByTree[integ.ArbreID] = integ
			}
		}
	}
	latestGedcomByTree := map[int]db.EspaiImport{}
	latestAnyImportByTree := map[int]db.EspaiImport{}
	if imports, err := a.DB.ListEspaiImportsByOwner(user.ID); err == nil {
		for _, imp := range imports {
			if imp.ArbreID == 0 {
				continue
			}
			if _, ok := latestAnyImportByTree[imp.ArbreID]; !ok {
				latestAnyImportByTree[imp.ArbreID] = imp
			}
			if strings.TrimSpace(imp.ImportType) != "gedcom" {
				continue
			}
			prev, ok := latestGedcomByTree[imp.ArbreID]
			if !ok {
				latestGedcomByTree[imp.ArbreID] = imp
				continue
			}
			if imp.CreatedAt.Valid && (!prev.CreatedAt.Valid || imp.CreatedAt.Time.After(prev.CreatedAt.Time)) {
				latestGedcomByTree[imp.ArbreID] = imp
			}
		}
	}
	rows := make([]espaiTreeRowView, 0, len(trees))
	editTreeID := parseIntDefault(r.URL.Query().Get("edit_tree"), 0)
	closeURL := "/espai/arbres"
	if editTreeID > 0 {
		closeQuery := cloneValues(r.URL.Query())
		closeQuery.Del("edit_tree")
		if len(closeQuery) > 0 {
			closeURL = closeURL + "?" + closeQuery.Encode()
		}
	}
	var editView *espaiTreeEditView
	for _, tree := range trees {
		peopleCount := 0
		if total, _, err := a.DB.CountEspaiPersonesByArbre(tree.ID); err == nil {
			peopleCount = total
		}
		relationsCount := 0
		if total, err := a.DB.CountEspaiRelacionsByArbre(tree.ID); err == nil {
			relationsCount = total
		}
		visibility := strings.TrimSpace(tree.Visibility)
		if visibility == "" {
			visibility = "private"
		}
		desc := ""
		if tree.Descripcio.Valid {
			desc = tree.Descripcio.String
		}
		familiesCount := 0
		if imp, ok := latestGedcomByTree[tree.ID]; ok && imp.SummaryJSON.Valid {
			var summary gedcomImportSummary
			if err := json.Unmarshal([]byte(imp.SummaryJSON.String), &summary); err == nil {
				familiesCount = summary.Families
			}
		}
		if familiesCount == 0 {
			if spouseCount, err := a.DB.CountEspaiRelacionsByArbreType(tree.ID, "spouse"); err == nil && spouseCount > 0 {
				familiesCount = spouseCount / 2
			}
		}
		grampsIntegrationID := 0
		sourceType := ""
		if integ, ok := grampsByTree[tree.ID]; ok {
			grampsIntegrationID = integ.ID
			sourceType = "gramps"
			if editTreeID == tree.ID {
				editView = &espaiTreeEditView{
					ID:                  tree.ID,
					Name:                tree.Nom,
					SourceType:          sourceType,
					GrampsIntegrationID: integ.ID,
					GrampsBaseURL:       integ.BaseURL,
					GrampsUsername:      integ.Username.String,
					CloseURL:            closeURL,
				}
			}
		} else if _, ok := latestGedcomByTree[tree.ID]; ok {
			sourceType = "gedcom"
			if editTreeID == tree.ID {
				editView = &espaiTreeEditView{
					ID:         tree.ID,
					Name:       tree.Nom,
					SourceType: sourceType,
					CloseURL:   closeURL,
				}
			}
		} else if editTreeID == tree.ID {
			editView = &espaiTreeEditView{
				ID:   tree.ID,
				Name: tree.Nom,
				CloseURL: closeURL,
			}
		}
		importStatus := ""
		importType := ""
		if imp, ok := latestAnyImportByTree[tree.ID]; ok {
			importStatus = strings.ToLower(strings.TrimSpace(imp.Status))
			importType = strings.ToLower(strings.TrimSpace(imp.ImportType))
		}
		rows = append(rows, espaiTreeRowView{
			ID:             tree.ID,
			Name:           tree.Nom,
			Description:    desc,
			Visibility:     visibility,
			PeopleCount:    peopleCount,
			RelationsCount: relationsCount,
			FamiliesCount:  familiesCount,
			GrampsIntegrationID: grampsIntegrationID,
			SourceType:     sourceType,
			ImportStatus:   importStatus,
			ImportType:     importType,
		})
	}

	treeLimit := parseIntDefault(a.Config["ESP_TREE_LIMIT"], 0)
	limitLabel := T(lang, "space.trees.limit.unlimited")
	if treeLimit > 0 {
		limitLabel = strconv.Itoa(treeLimit)
	}

	spaceState := "ready"
	if len(trees) == 0 {
		spaceState = "empty"
	}

	RenderPrivateTemplate(w, r, "espai.html", map[string]interface{}{
		"SpaceSection":       "trees",
		"SpaceState":         spaceState,
		"EspaiTreeRows":      rows,
		"EspaiTreeEdit":      editView,
		"EspaiTreeCount":     len(trees),
		"EspaiTreeLimit":     treeLimit,
		"EspaiTreeLimitLabel": limitLabel,
		"EspaiReturnTo":      r.URL.RequestURI(),
		"UploadError":        strings.TrimSpace(r.URL.Query().Get("error")),
		"UploadNotice":       strings.TrimSpace(r.URL.Query().Get("notice")),
	})
}

func (a *App) EspaiPrivacyUpdateTree(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, T(ResolveLang(r), "error.csrf"), http.StatusBadRequest)
		return
	}
	redirectBase := espaiRedirectTarget(r, "/espai")
	treeID := parseFormInt(r.FormValue("tree_id"))
	visibility := strings.TrimSpace(r.FormValue("visibility"))
	if treeID == 0 {
		http.Redirect(w, r, withQueryParams(redirectBase, map[string]string{"error": T(ResolveLang(r), "space.privacy.error.tree_not_found")}), http.StatusSeeOther)
		return
	}
	if !isValidTreeVisibility(visibility) {
		http.Redirect(w, r, withQueryParams(redirectBase, map[string]string{"error": T(ResolveLang(r), "space.privacy.error.invalid_visibility")}), http.StatusSeeOther)
		return
	}
	tree, err := a.DB.GetEspaiArbre(treeID)
	if err != nil || tree == nil || tree.OwnerUserID != user.ID {
		http.Redirect(w, r, withQueryParams(redirectBase, map[string]string{"error": T(ResolveLang(r), "space.privacy.error.tree_not_found")}), http.StatusSeeOther)
		return
	}
	prev := strings.TrimSpace(tree.Visibility)
	tree.Visibility = visibility
	if err := a.DB.UpdateEspaiArbre(tree); err != nil {
		http.Redirect(w, r, withQueryParams(redirectBase, map[string]string{"error": err.Error()}), http.StatusSeeOther)
		return
	}
	if err := a.upsertSearchDocForEspaiArbreID(tree.ID); err != nil {
		Errorf("SearchIndex espai arbre %d: %v", tree.ID, err)
	}
	_, _ = a.DB.CreateEspaiPrivacyAudit(&db.EspaiPrivacyAudit{
		OwnerUserID:    user.ID,
		ArbreID:        tree.ID,
		PersonaID:      sql.NullInt64{},
		Action:         "tree_visibility",
		FromVisibility: sqlNullString(prev),
		ToVisibility:   sqlNullString(visibility),
		IP:             sqlNullString(getIP(r)),
	})
	http.Redirect(w, r, withQueryParams(redirectBase, map[string]string{"notice": T(ResolveLang(r), "space.privacy.notice.tree_updated")}), http.StatusSeeOther)
}

func (a *App) EspaiPrivacyDeleteTree(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, T(ResolveLang(r), "error.csrf"), http.StatusBadRequest)
		return
	}
	redirectBase := espaiRedirectTarget(r, "/espai")
	treeID := parseFormInt(r.FormValue("tree_id"))
	if treeID == 0 {
		http.Redirect(w, r, withQueryParams(redirectBase, map[string]string{"error": T(ResolveLang(r), "space.privacy.error.tree_not_found")}), http.StatusSeeOther)
		return
	}
	tree, err := a.DB.GetEspaiArbre(treeID)
	if err != nil || tree == nil || tree.OwnerUserID != user.ID {
		http.Redirect(w, r, withQueryParams(redirectBase, map[string]string{"error": T(ResolveLang(r), "space.privacy.error.tree_not_found")}), http.StatusSeeOther)
		return
	}

	fontsToDelete := map[int]*db.EspaiFontImportacio{}
	if imports, err := a.DB.ListEspaiImportsByArbre(treeID); err == nil {
		for _, imp := range imports {
			if !imp.FontID.Valid {
				continue
			}
			fontID := int(imp.FontID.Int64)
			if _, ok := fontsToDelete[fontID]; ok {
				continue
			}
			font, err := a.DB.GetEspaiFontImportacio(fontID)
			if err != nil || font == nil || font.OwnerUserID != user.ID {
				continue
			}
			fontsToDelete[fontID] = font
		}
	}

	if err := a.DB.DeleteEspaiArbre(user.ID, treeID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Redirect(w, r, withQueryParams(redirectBase, map[string]string{"error": T(ResolveLang(r), "space.privacy.error.tree_not_found")}), http.StatusSeeOther)
			return
		}
		http.Redirect(w, r, withQueryParams(redirectBase, map[string]string{"error": err.Error()}), http.StatusSeeOther)
		return
	}
	if err := a.DB.DeleteSearchDoc("espai_arbre", treeID); err != nil {
		Errorf("SearchIndex espai arbre delete %d: %v", treeID, err)
	}
	cfg := a.gedcomConfig()
	for _, font := range fontsToDelete {
		if font.StoragePath.Valid {
			if err := os.Remove(font.StoragePath.String); err != nil && !errors.Is(err, os.ErrNotExist) {
				Errorf("GEDCOM delete file %s: %v", font.StoragePath.String, err)
			}
		}
		if err := a.DB.DeleteEspaiFontImportacio(font.ID); err != nil && !errors.Is(err, sql.ErrNoRows) {
			Errorf("GEDCOM delete font %d: %v", font.ID, err)
		}
	}
	userDir := filepath.Join(cfg.Root, strconv.Itoa(user.ID))
	if err := os.Remove(userDir); err != nil && !errors.Is(err, os.ErrNotExist) {
		if !strings.Contains(strings.ToLower(err.Error()), "directory not empty") {
			Errorf("GEDCOM delete dir %s: %v", userDir, err)
		}
	}
	_, _ = a.DB.CreateEspaiPrivacyAudit(&db.EspaiPrivacyAudit{
		OwnerUserID: user.ID,
		ArbreID:     treeID,
		PersonaID:   sql.NullInt64{},
		Action:      "tree_deleted",
		IP:          sqlNullString(getIP(r)),
	})
	http.Redirect(w, r, withQueryParams(redirectBase, map[string]string{"notice": T(ResolveLang(r), "space.privacy.notice.tree_deleted")}), http.StatusSeeOther)
}

func (a *App) EspaiPrivacyUpdatePersona(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, T(ResolveLang(r), "error.csrf"), http.StatusBadRequest)
		return
	}
	redirectBase := espaiRedirectTarget(r, "/espai")
	personaID := parseFormInt(r.FormValue("persona_id"))
	visibility := strings.TrimSpace(r.FormValue("visibility"))
	if personaID == 0 {
		http.Redirect(w, r, withQueryParams(redirectBase, map[string]string{"error": T(ResolveLang(r), "space.privacy.error.person_not_found")}), http.StatusSeeOther)
		return
	}
	if !isValidPersonaVisibility(visibility) {
		http.Redirect(w, r, withQueryParams(redirectBase, map[string]string{"error": T(ResolveLang(r), "space.privacy.error.invalid_visibility")}), http.StatusSeeOther)
		return
	}
	persona, err := a.DB.GetEspaiPersona(personaID)
	if err != nil || persona == nil || persona.OwnerUserID != user.ID {
		http.Redirect(w, r, withQueryParams(redirectBase, map[string]string{"error": T(ResolveLang(r), "space.privacy.error.person_not_found")}), http.StatusSeeOther)
		return
	}
	prev := strings.TrimSpace(persona.Visibility)
	if prev == "" {
		prev = "visible"
	}
	if err := a.DB.UpdateEspaiPersonaVisibility(persona.ID, visibility); err != nil {
		http.Redirect(w, r, withQueryParams(redirectBase, map[string]string{"error": err.Error()}), http.StatusSeeOther)
		return
	}
	_ = a.upsertSearchDocForEspaiPersonaID(persona.ID)
	_, _ = a.DB.CreateEspaiPrivacyAudit(&db.EspaiPrivacyAudit{
		OwnerUserID:    user.ID,
		ArbreID:        persona.ArbreID,
		PersonaID:      sql.NullInt64{Int64: int64(persona.ID), Valid: true},
		Action:         "person_visibility",
		FromVisibility: sqlNullString(prev),
		ToVisibility:   sqlNullString(visibility),
		IP:             sqlNullString(getIP(r)),
	})
	http.Redirect(w, r, withQueryParams(redirectBase, map[string]string{"notice": T(ResolveLang(r), "space.privacy.notice.person_updated")}), http.StatusSeeOther)
}

func (a *App) EspaiPublicArbrePage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	lang := ResolveLang(r)
	treeID := extractID(r.URL.Path)
	if treeID == 0 {
		http.Error(w, T(lang, "space.privacy.error.tree_not_found"), http.StatusNotFound)
		return
	}
	tree, err := a.DB.GetEspaiArbre(treeID)
	if err != nil || tree == nil || strings.TrimSpace(tree.Visibility) != "public" || strings.TrimSpace(tree.Status) != "active" {
		http.Error(w, T(lang, "space.privacy.error.tree_not_found"), http.StatusNotFound)
		return
	}
	view := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("view")))
	if view != "familiar" && view != "ventall" {
		view = "pedigree"
	}
	gens := parseTreeGens(r.URL.Query().Get("gens"), treeDefaultGens)
	rootID := parseFormInt(r.URL.Query().Get("persona_id"))

	dataset, err := a.buildEspaiArbreDataset(tree.ID, rootID, lang, true)
	if err != nil || dataset.RootPersonID == 0 {
		http.Error(w, T(lang, "space.privacy.error.tree_empty"), http.StatusNotFound)
		return
	}

	treeI18n := treeI18nMap(lang)
	RenderTemplate(w, r, "espai-public-arbre.html", map[string]interface{}{
		"Tree":         tree,
		"TreeName":     tree.Nom,
		"TreeID":       tree.ID,
		"View":         view,
		"Gens":         gens,
		"FamilyData":   dataset.FamilyData,
		"FamilyLinks":  dataset.FamilyLinks,
		"RootPersonId": dataset.RootPersonID,
		"DatasetStats": dataset.DatasetStats,
		"TreeI18n":     treeI18n,
	})
}

func (a *App) EspaiPublicArbreAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	lang := ResolveLang(r)
	treeID := extractID(r.URL.Path)
	if treeID == 0 {
		http.Error(w, T(lang, "space.privacy.error.tree_not_found"), http.StatusNotFound)
		return
	}
	tree, err := a.DB.GetEspaiArbre(treeID)
	if err != nil || tree == nil || strings.TrimSpace(tree.Visibility) != "public" || strings.TrimSpace(tree.Status) != "active" {
		http.Error(w, T(lang, "space.privacy.error.tree_not_found"), http.StatusNotFound)
		return
	}
	rootID := parseFormInt(r.URL.Query().Get("persona_id"))
	dataset, err := a.buildEspaiArbreDataset(tree.ID, rootID, lang, true)
	if err != nil || dataset.RootPersonID == 0 {
		http.Error(w, T(lang, "space.privacy.error.tree_empty"), http.StatusNotFound)
		return
	}

	writeJSON(w, map[string]interface{}{
		"tree_id":       tree.ID,
		"tree_name":     tree.Nom,
		"root_person":   dataset.RootPersonID,
		"familyData":    dataset.FamilyData,
		"familyLinks":   dataset.FamilyLinks,
		"__DATASET_STATS": dataset.DatasetStats,
	})
}

func (a *App) buildEspaiArbreDataset(arbreID int, rootID int, lang string, publicOnly bool) (treeDataset, error) {
	dataset := treeDataset{}
	persones, err := a.DB.ListEspaiPersonesByArbre(arbreID)
	if err != nil {
		return dataset, err
	}
	visible := map[int]db.EspaiPersona{}
	for _, p := range persones {
		if strings.TrimSpace(p.Status) != "" && strings.TrimSpace(p.Status) != "active" {
			continue
		}
		visibility := strings.TrimSpace(p.Visibility)
		if visibility == "" {
			visibility = "visible"
		}
		if publicOnly && visibility == "hidden" {
			continue
		}
		visible[p.ID] = p
	}
	if len(visible) == 0 {
		return dataset, errors.New("no visible persons")
	}
	if rootID == 0 || visible[rootID].ID == 0 {
		rootID = pickFirstVisibleID(visible)
	}
	people := make([]treePerson, 0, len(visible))
	for _, p := range visible {
		birth := ""
		if p.DataNaixement.Valid {
			birth = formatDateDisplay(p.DataNaixement.String)
		}
		death := ""
		if p.DataDefuncio.Valid {
			death = formatDateDisplay(p.DataDefuncio.String)
		}
		birthPlace := strings.TrimSpace(p.LlocNaixement.String)
		deathPlace := strings.TrimSpace(p.LlocDefuncio.String)
		visibility := strings.TrimSpace(p.Visibility)
		if visibility == "" {
			visibility = "visible"
		}
		hidden := visibility == "hidden"
		people = append(people, treePerson{
			ID:         p.ID,
			Name:       espaiPersonaDisplayNameWithFallback(p, T(lang, "tree.unknown.name")),
			Sex:        espaiSexToTree(p.Sexe),
			Birth:      birth,
			BirthPlace: birthPlace,
			Death:      death,
			DeathPlace: deathPlace,
			Hidden:     hidden,
		})
	}
	sort.Slice(people, func(i, j int) bool { return people[i].ID < people[j].ID })

	relations, _ := a.DB.ListEspaiRelacionsByArbre(arbreID)
	parentMap := map[int]parentPair{}
	for _, rel := range relations {
		if visible[rel.PersonaID].ID == 0 || visible[rel.RelatedPersonaID].ID == 0 {
			continue
		}
		relType := strings.ToLower(strings.TrimSpace(rel.RelationType))
		switch relType {
		case "father", "mother", "parent":
			childID := rel.PersonaID
			parentID := rel.RelatedPersonaID
			pair := parentMap[childID]
			if relType == "father" {
				pair.Father = parentID
			} else if relType == "mother" {
				pair.Mother = parentID
			} else {
				if pair.Father == 0 {
					pair.Father = parentID
				} else if pair.Mother == 0 {
					pair.Mother = parentID
				}
			}
			parentMap[childID] = pair
		case "child":
			parentID := rel.PersonaID
			childID := rel.RelatedPersonaID
			sex := espaiSexToTree(visible[parentID].Sexe)
			pair := parentMap[childID]
			if sex == 1 {
				if pair.Mother == 0 {
					pair.Mother = parentID
				} else if pair.Father == 0 {
					pair.Father = parentID
				}
			} else {
				if pair.Father == 0 {
					pair.Father = parentID
				} else if pair.Mother == 0 {
					pair.Mother = parentID
				}
			}
			parentMap[childID] = pair
		}
	}
	links := make([]treeLink, 0, len(parentMap))
	for childID, pair := range parentMap {
		if pair.Father == 0 && pair.Mother == 0 {
			continue
		}
		links = append(links, treeLink{Child: childID, Father: pair.Father, Mother: pair.Mother})
	}
	sort.Slice(links, func(i, j int) bool { return links[i].Child < links[j].Child })

	dataset = treeDataset{
		FamilyData:   people,
		FamilyLinks:  links,
		RootPersonID: rootID,
		DatasetStats: treeDatasetStats{People: len(people), Links: len(links)},
	}
	return dataset, nil
}

func pickFirstVisibleID(items map[int]db.EspaiPersona) int {
	if len(items) == 0 {
		return 0
	}
	ids := make([]int, 0, len(items))
	for id := range items {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids[0]
}

func espaiPersonaDisplayNameWithFallback(p db.EspaiPersona, fallback string) string {
	name := espaiPersonaDisplayName(p)
	if name == "-" {
		return fallback
	}
	return name
}

func espaiSexToTree(val sql.NullString) int {
	raw := strings.ToLower(strings.TrimSpace(val.String))
	switch raw {
	case "male", "m":
		return 0
	case "female", "f":
		return 1
	default:
		return 2
	}
}

func isValidTreeVisibility(val string) bool {
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "private", "public", "restricted":
		return true
	default:
		return false
	}
}

func espaiRedirectTarget(r *http.Request, fallback string) string {
	next := strings.TrimSpace(r.FormValue("next"))
	if next == "" {
		return fallback
	}
	if !strings.HasPrefix(next, "/") || strings.HasPrefix(next, "//") {
		return fallback
	}
	return next
}

func isValidPersonaVisibility(val string) bool {
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "visible", "hidden":
		return true
	default:
		return false
	}
}

func treeI18nMap(lang string) map[string]string {
	treeKeys := []string{
		"tree.dataset",
		"tree.visible",
		"tree.error.d3",
		"tree.error.root",
		"tree.error.expand",
		"tree.unknown.name",
		"tree.unknown.person",
		"tree.placeholder.father",
		"tree.placeholder.mother",
		"tree.drawer.section",
		"tree.drawer.empty",
		"tree.drawer.birth",
		"tree.drawer.birth_place",
		"tree.drawer.death",
		"tree.drawer.death_place",
		"tree.drawer.occupation",
		"tree.drawer.sex",
		"tree.drawer.select_person",
		"tree.drawer.segment_hint",
		"tree.drawer.no_extra",
		"tree.drawer.open_profile",
		"tree.sex.male",
		"tree.sex.female",
		"tree.sex.unknown",
		"tree.fan.birth_prefix",
		"tree.fan.death_prefix",
		"tree.controls.view_switch_aria",
		"tree.controls.generations",
		"tree.controls.generation_singular",
		"tree.controls.generation_plural",
		"tree.controls.zoom_in",
		"tree.controls.zoom_out",
		"tree.controls.fit",
		"tree.controls.dataset_title",
		"tree.controls.visible_title",
		"tree.controls.drawer_title",
		"tree.controls.drawer",
		"tree.aria.fan",
		"tree.aria.tree",
		"tree.drawer.close",
		"tree.drawer.click_hint_node",
		"tree.view.pedigree",
		"tree.view.familiar",
		"tree.view.ventall",
		"tree.title",
	}
	treeI18n := map[string]string{}
	for _, key := range treeKeys {
		treeI18n[key] = T(lang, key)
	}
	return treeI18n
}
