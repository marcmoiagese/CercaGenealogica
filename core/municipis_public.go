package core

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type municipiCategoryView struct {
	Key        string
	Label      string
	Count      int
	Href       string
	Icon       string
	CountLabel string
}

func (a *App) MunicipiPublic(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	mun, err := a.DB.GetMunicipi(id)
	if err != nil || mun == nil {
		http.NotFound(w, r)
		return
	}

	user, _ := a.VerificarSessio(r)
	perms := db.PolicyPermissions{}
	if user != nil {
		perms = a.getPermissionsForUser(user.ID)
	}
	canManageTerritory := user != nil && a.hasPerm(perms, permTerritory)
	canModerate := user != nil && a.hasPerm(perms, permModerate)
	canManageArxius := user != nil && a.hasPerm(perms, permArxius)
	canManagePolicies := user != nil && (perms.CanManagePolicies || perms.Admin)
	munTarget := a.resolveMunicipiTarget(mun.ID)
	canViewMap := user != nil && a.HasPermission(user.ID, permKeyTerritoriMunicipisMapesView, munTarget)
	canCreateMap := user != nil && a.HasPermission(user.ID, permKeyTerritoriMunicipisMapesCreate, munTarget)
	canViewLlibres := user != nil && a.hasAnyPermissionKey(user.ID, permKeyDocumentalsLlibresView)
	canCreateLlibre := user != nil && a.hasAnyPermissionKey(user.ID, permKeyDocumentalsLlibresCreate)
	canCreateAnecdote := user != nil && a.HasPermission(user.ID, permKeyTerritoriMunicipisAnecdotesCreate, munTarget)
	markType := ""
	markPublic := true
	markOwn := false
	if user != nil {
		if marks, err := a.DB.ListWikiMarks("municipi", []int{mun.ID}); err == nil {
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

	if mun.ModeracioEstat != "" && mun.ModeracioEstat != "publicat" && !(canManageTerritory || canModerate) {
		http.NotFound(w, r)
		return
	}

	lang := ResolveLang(r)
	seen := make(map[int]bool)
	var levels []db.NivellAdministratiu
	for _, lvlID := range mun.NivellAdministratiuID {
		if !lvlID.Valid {
			continue
		}
		id := int(lvlID.Int64)
		if seen[id] {
			continue
		}
		seen[id] = true
		if lvl, err := a.DB.GetNivell(id); err == nil && lvl != nil {
			levels = append(levels, *lvl)
		}
	}
	sort.Slice(levels, func(i, j int) bool {
		return levels[i].Nivel < levels[j].Nivel
	})

	type levelView struct {
		Nivel int
		Nom   string
		Tipus string
	}
	var levelViews []levelView
	for _, lvl := range levels {
		labelKey := fmt.Sprintf("levels.types.%s", lvl.TipusNivell)
		label := T(lang, labelKey)
		if label == labelKey {
			label = lvl.TipusNivell
		}
		levelViews = append(levelViews, levelView{
			Nivel: lvl.Nivel,
			Nom:   lvl.NomNivell,
			Tipus: label,
		})
	}

	countryLabel := ""
	for _, lvl := range levels {
		if lvl.PaisISO2.Valid {
			countryLabel = a.countryLabelFromISO(lvl.PaisISO2.String, lang)
			break
		}
	}
	typeKey := fmt.Sprintf("municipis.type.%s", mun.Tipus)
	typeLabel := T(lang, typeKey)
	if typeLabel == typeKey {
		typeLabel = mun.Tipus
	}
	regionLabel := ""
	if len(levelViews) > 0 {
		regionLabel = levelViews[len(levelViews)-1].Nom
	}
	mapesData := map[string]interface{}{
		"municipi_id": mun.ID,
		"mapes_api":   fmt.Sprintf("/api/municipis/%d/mapes", mun.ID),
		"mapes_page":  fmt.Sprintf("/territori/municipis/%d/mapes", mun.ID),
	}
	if mun.Latitud.Valid {
		mapesData["lat"] = mun.Latitud.Float64
	}
	if mun.Longitud.Valid {
		mapesData["lon"] = mun.Longitud.Float64
	}

	historiaGeneral, historiaTimeline, historiaErr := a.DB.GetMunicipiHistoriaSummary(mun.ID)
	if historiaErr != nil {
		Errorf("Error carregant historia municipi %d: %v", mun.ID, historiaErr)
	}
	historiaSummary := ""
	if historiaGeneral != nil {
		if strings.TrimSpace(historiaGeneral.Resum) != "" {
			historiaSummary = strings.TrimSpace(historiaGeneral.Resum)
		} else {
			historiaSummary = summarizeHistoriaText(historiaGeneral.CosText, 260)
		}
	}
	historiaTimelineView := []map[string]string{}
	for _, item := range historiaTimeline {
		historiaTimelineView = append(historiaTimelineView, map[string]string{
			"date":  historiaDateLabel(item),
			"title": strings.TrimSpace(item.Titol),
			"resum": strings.TrimSpace(item.Resum),
		})
	}
	canAportarHistoria := user != nil && a.HasPermission(user.ID, permKeyTerritoriMunicipisHistoriaCreate, munTarget)

	eventViews := []map[string]interface{}{}
	eventFilter := db.EventHistoricFilter{
		Status: "publicat",
		Limit:  6,
	}
	events, err := a.DB.ListEventsByScope("municipi", mun.ID, eventFilter)
	if err != nil {
		Errorf("Error carregant events historics municipi %d: %v", mun.ID, err)
		events = []db.EventHistoric{}
	}
	for _, ev := range events {
		intensitat := 0
		if impacts, err := a.DB.ListEventImpacts(ev.ID); err == nil {
			for _, impact := range impacts {
				if impact.ScopeType == "municipi" && impact.ScopeID == mun.ID && impact.Intensitat > intensitat {
					intensitat = impact.Intensitat
				}
			}
		}
		eventViews = append(eventViews, map[string]interface{}{
			"ID":        ev.ID,
			"Title":     strings.TrimSpace(ev.Titol),
			"TypeLabel": eventTypeLabel(lang, ev.Tipus),
			"DateLabel": eventDateLabel(ev),
			"URL":       fmt.Sprintf("/historia/events/%d", ev.ID),
			"Intensity": intensitat,
		})
	}
	canCreateEvent := user != nil

	demografiaMeta, demografiaErr := a.DB.GetMunicipiDemografiaMeta(mun.ID)
	if demografiaErr != nil {
		Errorf("Error carregant demografia municipi %d: %v", mun.ID, demografiaErr)
	}
	demografiaSummary := buildDemografiaSummary(demografiaMeta)

	llibreStatus := "publicat"
	if canManageArxius {
		llibreStatus = ""
	}
	llibres, _ := a.DB.ListLlibres(db.LlibreFilter{
		MunicipiID: mun.ID,
		Status:     llibreStatus,
	})
	categoryCounts := map[string]int{}
	for _, llibre := range llibres {
		key := strings.TrimSpace(llibre.TipusLlibre)
		if key == "" {
			key = "altres"
		}
		categoryCounts[key]++
	}
	categoryOrder := []string{"baptismes", "confirmacions", "matrimonis", "obits", "padrons", "reclutaments", "altres"}
	iconByType := map[string]string{
		"baptismes":     "fa-droplet",
		"confirmacions": "fa-user-check",
		"matrimonis":    "fa-ring",
		"obits":         "fa-cross",
		"padrons":       "fa-users",
		"reclutaments":  "fa-shield-halved",
		"altres":        "fa-book",
	}
	countLabel := T(lang, "books.title")
	allDocsURL := ""
	if canViewLlibres {
		allDocsURL = fmt.Sprintf("/documentals/llibres?municipi_id=%d", mun.ID)
	}
	categories := []municipiCategoryView{}
	seenCategories := map[string]bool{}
	appendCategory := func(key string, count int) {
		if count == 0 {
			return
		}
		labelKey := fmt.Sprintf("books.type.%s", key)
		label := T(lang, labelKey)
		if label == labelKey {
			label = key
		}
		icon := iconByType[key]
		if icon == "" {
			icon = "fa-book"
		}
		href := "#"
		if canViewLlibres {
			href = fmt.Sprintf("/documentals/llibres?municipi_id=%d&tipus_llibre=%s", mun.ID, url.QueryEscape(key))
		}
		categories = append(categories, municipiCategoryView{
			Key:        key,
			Label:      label,
			Count:      count,
			Href:       href,
			Icon:       icon,
			CountLabel: countLabel,
		})
		seenCategories[key] = true
	}
	for _, key := range categoryOrder {
		appendCategory(key, categoryCounts[key])
	}
	extraKeys := []string{}
	for key := range categoryCounts {
		if seenCategories[key] {
			continue
		}
		extraKeys = append(extraKeys, key)
	}
	sort.Strings(extraKeys)
	for _, key := range extraKeys {
		appendCategory(key, categoryCounts[key])
	}

	data := map[string]interface{}{
		"Municipi":                  mun,
		"Hierarchy":                 levelViews,
		"CountryLabel":              countryLabel,
		"TypeLabel":                 typeLabel,
		"RegionLabel":               regionLabel,
		"LlibreCategories":          categories,
		"LlibresURL":                allDocsURL,
		"User":                      user,
		"CanManageArxius":           canManageArxius,
		"CanViewLlibres":            canViewLlibres,
		"CanCreateLlibre":           canCreateLlibre,
		"CanManagePolicies":         canManagePolicies,
		"CanModerate":               canModerate,
		"CanManageTerritory":        canManageTerritory,
		"CanViewMap":                canViewMap,
		"CanCreateMap":              canCreateMap,
		"CanCreateAnecdote":         canCreateAnecdote,
		"ShowStatusBadge":           canManageTerritory || canModerate,
		"MapesData":                 mapesData,
		"HistoriaGeneral":           historiaGeneral,
		"HistoriaGeneralSummary":    historiaSummary,
		"HistoriaTimelineDestacat":  historiaTimelineView,
		"CanAportarHistoria":        canAportarHistoria,
		"HistoricEvents":            eventViews,
		"HistoricEventsURL":         fmt.Sprintf("/territori/municipis/%d/events", mun.ID),
		"HistoricEventsTimelineURL": fmt.Sprintf("/historia/events?scope_type=municipi&scope_id=%d&view=timeline", mun.ID),
		"HistoricEventsNewURL":      fmt.Sprintf("/historia/events/nou?scope_type=municipi&scope_id=%d", mun.ID),
		"CanCreateEvent":            canCreateEvent,
		"DemografiaSummary":         demografiaSummary,
		"MarkType":                  markType,
		"MarkPublic":                markPublic,
		"MarkOwn":                   markOwn,
	}
	if user != nil {
		RenderPrivateTemplate(w, r, "municipi-perfil-pro.html", data)
		return
	}
	RenderTemplate(w, r, "municipi-perfil-pro.html", data)
}
