package core

import (
	"database/sql"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type espaiTreeStats struct {
	Total  int
	Hidden int
}

type espaiPersonaView struct {
	ID         int
	Name       string
	Visibility string
}

type espaiTreePeoplePager struct {
	Query      string
	Page       int
	PerPage    int
	Total      int
	TotalPages int
	HasPrev    bool
	HasNext    bool
	PrevPage   int
	NextPage   int
	PageBase   string
}

const espaiTreePeoplePerPage = 25

func buildEspaiTreePeoplePageBase(treeID int, query string) string {
	base := "/espai?tree_id=" + strconv.Itoa(treeID)
	if strings.TrimSpace(query) != "" {
		base += "&q=" + url.QueryEscape(query)
	}
	return base
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
	treeStats := map[int]espaiTreeStats{}
	treePeople := map[int][]espaiPersonaView{}
	treePeoplePager := map[int]espaiTreePeoplePager{}

	selectedTreeID := parseFormInt(r.URL.Query().Get("tree_id"))
	selectedPage := parseFormInt(r.URL.Query().Get("page"))
	if selectedPage < 1 {
		selectedPage = 1
	}
	selectedQuery := strings.TrimSpace(r.URL.Query().Get("q"))

	for _, tree := range trees {
		stats := espaiTreeStats{}
		if total, hidden, err := a.DB.CountEspaiPersonesByArbre(tree.ID); err == nil {
			stats.Total = total
			stats.Hidden = hidden
		}

		query := ""
		page := 1
		if selectedTreeID == tree.ID {
			query = selectedQuery
			page = selectedPage
		}
		totalFiltered := stats.Total
		if query != "" {
			if count, err := a.DB.CountEspaiPersonesByArbreQuery(tree.ID, query); err == nil {
				totalFiltered = count
			} else {
				totalFiltered = 0
			}
		}

		totalPages := 1
		if totalFiltered > 0 {
			totalPages = (totalFiltered + espaiTreePeoplePerPage - 1) / espaiTreePeoplePerPage
		}
		if page > totalPages {
			page = totalPages
		}
		if page < 1 {
			page = 1
		}
		offset := (page - 1) * espaiTreePeoplePerPage
		persones, _ := a.DB.ListEspaiPersonesByArbreQuery(tree.ID, query, espaiTreePeoplePerPage, offset)

		views := make([]espaiPersonaView, 0, len(persones))
		for _, p := range persones {
			visibility := strings.TrimSpace(p.Visibility)
			if visibility == "" {
				visibility = "visible"
			}
			views = append(views, espaiPersonaView{
				ID:         p.ID,
				Name:       espaiPersonaDisplayNameWithFallback(p, T(lang, "tree.unknown.name")),
				Visibility: visibility,
			})
		}

		treeStats[tree.ID] = stats
		treePeople[tree.ID] = views
		treePeoplePager[tree.ID] = espaiTreePeoplePager{
			Query:      query,
			Page:       page,
			PerPage:    espaiTreePeoplePerPage,
			Total:      totalFiltered,
			TotalPages: totalPages,
			HasPrev:    page > 1,
			HasNext:    page < totalPages,
			PrevPage:   page - 1,
			NextPage:   page + 1,
			PageBase:   buildEspaiTreePeoplePageBase(tree.ID, query),
		}
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
		"EspaiTrees":        trees,
		"EspaiTreeStats":    treeStats,
		"EspaiTreePersons":  treePeople,
		"EspaiTreePeoplePager": treePeoplePager,
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
	treeID := parseFormInt(r.FormValue("tree_id"))
	visibility := strings.TrimSpace(r.FormValue("visibility"))
	if treeID == 0 {
		http.Redirect(w, r, "/espai?error="+urlQueryEscape(T(ResolveLang(r), "space.privacy.error.tree_not_found")), http.StatusSeeOther)
		return
	}
	if !isValidTreeVisibility(visibility) {
		http.Redirect(w, r, "/espai?error="+urlQueryEscape(T(ResolveLang(r), "space.privacy.error.invalid_visibility")), http.StatusSeeOther)
		return
	}
	tree, err := a.DB.GetEspaiArbre(treeID)
	if err != nil || tree == nil || tree.OwnerUserID != user.ID {
		http.Redirect(w, r, "/espai?error="+urlQueryEscape(T(ResolveLang(r), "space.privacy.error.tree_not_found")), http.StatusSeeOther)
		return
	}
	prev := strings.TrimSpace(tree.Visibility)
	tree.Visibility = visibility
	if err := a.DB.UpdateEspaiArbre(tree); err != nil {
		http.Redirect(w, r, "/espai?error="+urlQueryEscape(err.Error()), http.StatusSeeOther)
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
	http.Redirect(w, r, "/espai?notice="+urlQueryEscape(T(ResolveLang(r), "space.privacy.notice.tree_updated")), http.StatusSeeOther)
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
	personaID := parseFormInt(r.FormValue("persona_id"))
	visibility := strings.TrimSpace(r.FormValue("visibility"))
	if personaID == 0 {
		http.Redirect(w, r, "/espai?error="+urlQueryEscape(T(ResolveLang(r), "space.privacy.error.person_not_found")), http.StatusSeeOther)
		return
	}
	if !isValidPersonaVisibility(visibility) {
		http.Redirect(w, r, "/espai?error="+urlQueryEscape(T(ResolveLang(r), "space.privacy.error.invalid_visibility")), http.StatusSeeOther)
		return
	}
	persona, err := a.DB.GetEspaiPersona(personaID)
	if err != nil || persona == nil || persona.OwnerUserID != user.ID {
		http.Redirect(w, r, "/espai?error="+urlQueryEscape(T(ResolveLang(r), "space.privacy.error.person_not_found")), http.StatusSeeOther)
		return
	}
	prev := strings.TrimSpace(persona.Visibility)
	if prev == "" {
		prev = "visible"
	}
	if err := a.DB.UpdateEspaiPersonaVisibility(persona.ID, visibility); err != nil {
		http.Redirect(w, r, "/espai?error="+urlQueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	_, _ = a.DB.CreateEspaiPrivacyAudit(&db.EspaiPrivacyAudit{
		OwnerUserID:    user.ID,
		ArbreID:        persona.ArbreID,
		PersonaID:      sql.NullInt64{Int64: int64(persona.ID), Valid: true},
		Action:         "person_visibility",
		FromVisibility: sqlNullString(prev),
		ToVisibility:   sqlNullString(visibility),
		IP:             sqlNullString(getIP(r)),
	})
	http.Redirect(w, r, "/espai?notice="+urlQueryEscape(T(ResolveLang(r), "space.privacy.notice.person_updated")), http.StatusSeeOther)
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
