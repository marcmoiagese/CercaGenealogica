package core

import (
	"database/sql"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type nivellBookAgg struct {
	Key       string
	Label     string
	Volums    int
	Estimated int
	Indexed   int
	MinYear   int
	MaxYear   int
}

type nivellDemoTotals struct {
	Births    int
	Marriages int
	Deaths    int
}

var nivellYearRegex = regexp.MustCompile(`\d{4}`)

func (a *App) NivellPublic(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	nivell, err := a.DB.GetNivell(id)
	if err != nil || nivell == nil {
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
	if nivell.ModeracioEstat != "" && nivell.ModeracioEstat != "publicat" && !(canManageTerritory || canModerate) {
		http.NotFound(w, r)
		return
	}
	lang := ResolveLang(r)

	canViewMunicipis := user != nil && a.hasAnyPermissionKey(user.ID, permKeyTerritoriMunicipisView)
	canViewLlibres := user != nil && a.hasAnyPermissionKey(user.ID, permKeyDocumentalsLlibresView)
	canManageArxius := user != nil && a.hasPerm(perms, permArxius)

	status := nivell.ModeracioEstat
	if status == "" {
		status = "publicat"
	}
	statusLabel := translateStatusLabel(lang, status)
	statusClass := statusBadgeClass(status)

	levelLabel := translateOrFallbackLabel(lang, "levels.types."+strings.TrimSpace(nivell.TipusNivell), nivell.TipusNivell)
	countryLabel := nivellCountryLabel(a, nivell, lang)

	subtitle := strings.TrimSpace(nivell.Altres)
	if subtitle == "" {
		subtitle = fmt.Sprintf("Unitat territorial de nivell %d.", nivell.Nivel)
	}

	hierarchy := buildNivellHierarchy(a, nivell, lang, countryLabel)

	munStatus := "publicat"
	if canManageTerritory || canModerate {
		munStatus = ""
	}
	municipis, err := a.DB.ListMunicipisBrowse(db.MunicipiBrowseFilter{
		NivellID: id,
		Status:   munStatus,
		Sort:     "nom",
		SortDir:  "asc",
	})
	if err != nil {
		Errorf("Error carregant municipis nivell %d: %v", id, err)
		municipis = []db.MunicipiBrowseRow{}
	}

	munIDs := make([]int, 0, len(municipis))
	for _, mun := range municipis {
		munIDs = append(munIDs, mun.ID)
	}

	llibreStatus := "publicat"
	if canManageArxius {
		llibreStatus = ""
	}
	llibres := []db.LlibreRow{}
	if len(munIDs) > 0 {
		llibres, _ = a.DB.ListLlibres(db.LlibreFilter{
			Status:             llibreStatus,
			AllowedMunicipiIDs: munIDs,
		})
	}

	allowFallbackCounts := len(llibres) <= 300
	booksAgg, indexing, kpis := buildNivellBookStats(a, llibres, allowFallbackCounts, lang)
	booksList := buildNivellBooksList(booksAgg)
	bookCategories := buildNivellBookCategories(booksAgg, nivell.ID, canViewLlibres, lang)

	unitRows := buildNivellUnits(municipis, llibres, allowFallbackCounts, a, lang)

	demoSeries, demoTotals := buildNivellDemografia(a, nivell.ID)
	topNames := buildNivellTopNames(a, nivell.ID, true)
	topSurnames := buildNivellTopNames(a, nivell.ID, false)

	events := buildNivellEvents(a, id, lang)

	links := map[string]string{}
	links["llibres"] = "#llibresTerritori"
	if canViewMunicipis {
		links["municipis"] = fmt.Sprintf("/territori/municipis?nivell_id_%d=%d", nivell.Nivel, nivell.ID)
	}

	adminData := map[string]interface{}{
		"id":        nivell.ID,
		"name":      nivell.NomNivell,
		"subtitle":  subtitle,
		"level":     levelLabel,
		"status":    statusLabel,
		"country":   countryLabel,
		"code":      strings.TrimSpace(nivell.CodiOficial),
		"municipis": len(municipis),
		"updated_at": formatDateISO(nivell.ModeratedAt),
		"links":      links,
		"kpis":       kpis,
		"hierarchy":  hierarchy,
		"indexing":   indexing,
		"units":      unitRows,
		"books":      booksList,
		"demography_series": map[string]interface{}{
			"births":    demoSeries["births"],
			"marriages": demoSeries["marriages"],
			"deaths":    demoSeries["deaths"],
		},
		"top_names":    topNames,
		"top_surnames": topSurnames,
		"events":       events,
		"history":      []string{},
		"memo":         []map[string]string{},
	}

	data := map[string]interface{}{
		"AdminLevelName":  nivell.NomNivell,
		"AdminLevelData":   adminData,
		"StatusBadgeClass": statusClass,
		"CanViewLlibres":   canViewLlibres,
		"CanViewMunicipis": canViewMunicipis,
		"HasActions":       true,
		"DemografiaTotals": demoTotals,
		"StatsLimited":     false,
		"ShowBooksAnchor":  true,
		"NivellID":         nivell.ID,
		"BookCategories":   bookCategories,
		"User":             user,
		"CanManageArxius":  canManageArxius,
		"CanManageTerritory": canManageTerritory,
		"CanManageEclesia": user != nil && a.hasPerm(perms, permEclesia),
		"CanManagePolicies": user != nil && (perms.CanManagePolicies || perms.Admin),
		"CanModerate":       canModerate,
		"IsAdmin":           user != nil && perms.Admin,
	}
	if user != nil {
		RenderPrivateTemplate(w, r, "nivell-administratiu-perfil-pro.html", data)
		return
	}
	RenderTemplate(w, r, "nivell-administratiu-perfil-pro.html", data)
}

func translateOrFallbackLabel(lang, key, fallback string) string {
	label := T(lang, key)
	if label == key {
		return fallback
	}
	if strings.TrimSpace(label) == "" {
		return fallback
	}
	return label
}

func translateStatusLabel(lang, status string) string {
	if strings.TrimSpace(status) == "" {
		return "—"
	}
	key := "activity.status." + status
	label := T(lang, key)
	if label == key {
		return status
	}
	return label
}

func statusBadgeClass(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "publicat":
		return "badge-success"
	case "pendent":
		return "badge-warning"
	default:
		return "badge-muted"
	}
}

func nivellCountryLabel(a *App, nivell *db.NivellAdministratiu, lang string) string {
	if nivell == nil {
		return ""
	}
	if nivell.PaisISO2.Valid {
		if label := a.countryLabelFromISO(nivell.PaisISO2.String, lang); label != "" {
			return label
		}
	}
	if nivell.PaisID > 0 {
		if pais, err := a.DB.GetPais(nivell.PaisID); err == nil && pais != nil {
			if pais.CodiISO2 != "" {
				return a.countryLabelFromISO(pais.CodiISO2, lang)
			}
		}
	}
	return ""
}

func buildNivellHierarchy(a *App, nivell *db.NivellAdministratiu, lang, countryLabel string) []map[string]interface{} {
	var items []map[string]interface{}
	if countryLabel != "" {
		items = append(items, map[string]interface{}{
			"label": "País",
			"value": countryLabel,
			"href":  "",
		})
	}
	if nivell == nil {
		return items
	}
	chain := []db.NivellAdministratiu{*nivell}
	seen := map[int]bool{nivell.ID: true}
	curr := nivell
	for curr.ParentID.Valid {
		parentID := int(curr.ParentID.Int64)
		if parentID <= 0 || seen[parentID] {
			break
		}
		parent, err := a.DB.GetNivell(parentID)
		if err != nil || parent == nil {
			break
		}
		seen[parentID] = true
		chain = append(chain, *parent)
		curr = parent
	}
	for i := len(chain) - 1; i >= 0; i-- {
		item := chain[i]
		label := translateOrFallbackLabel(lang, "levels.types."+strings.TrimSpace(item.TipusNivell), item.TipusNivell)
		items = append(items, map[string]interface{}{
			"label": label,
			"value": item.NomNivell,
			"href":  fmt.Sprintf("/territori/nivells/%d", item.ID),
		})
	}
	return items
}

func buildNivellBookStats(a *App, llibres []db.LlibreRow, allowFallback bool, lang string) (map[string]*nivellBookAgg, map[string]interface{}, []map[string]interface{}) {
	agg := map[string]*nivellBookAgg{}
	if len(llibres) == 0 {
		kpis := []map[string]interface{}{
			{"label": "Llibres totals", "value": 0, "icon": "fa-book"},
			{"label": "Registres estimats", "value": 0, "icon": "fa-file-lines"},
			{"label": "Registres indexats", "value": 0, "icon": "fa-keyboard"},
			{"label": "Cobertura global", "value": "0,0%", "icon": "fa-chart-pie"},
		}
		return agg, map[string]interface{}{
			"total_pct": 0,
			"indexed":   0,
			"estimated": 0,
			"by_type":   []map[string]interface{}{},
		}, kpis
	}
	kpis := []map[string]interface{}{}
	bookIDs := make([]int, 0, len(llibres))
	for _, llibre := range llibres {
		bookIDs = append(bookIDs, llibre.ID)
	}
	stats, err := a.DB.GetLlibresIndexacioStats(bookIDs)
	if err != nil {
		stats = map[int]db.LlibreIndexacioStats{}
	}
	totalIndexed := 0
	totalEstimated := 0
	for _, llibre := range llibres {
		key := normalizeIndexerBookType(llibre.TipusLlibre)
		item := agg[key]
		if item == nil {
			item = &nivellBookAgg{
				Key:   key,
				Label: translateOrFallbackLabel(lang, "books.type."+key, key),
			}
			agg[key] = item
		}
		item.Volums++
		yearMin, yearMax := parseYearRange(llibre.Cronologia)
		if yearMin > 0 {
			if item.MinYear == 0 || yearMin < item.MinYear {
				item.MinYear = yearMin
			}
			if item.MaxYear == 0 || yearMax > item.MaxYear {
				item.MaxYear = yearMax
			}
		}

		stat := stats[llibre.ID]
		estimated := stat.TotalRegistres
		percent := stat.Percentatge
		if allowFallback && estimated == 0 {
			if count, err := a.DB.CountTranscripcionsRaw(llibre.ID, db.TranscripcioFilter{}); err == nil {
				estimated = count
			}
		}
		if percent == 0 && llibre.IndexacioCompleta {
			percent = 100
		}
		indexed := int(math.Round(float64(estimated) * float64(percent) / 100))
		item.Estimated += estimated
		item.Indexed += indexed
		totalEstimated += estimated
		totalIndexed += indexed
	}

	indexPct := 0.0
	if totalEstimated > 0 {
		indexPct = (float64(totalIndexed) / float64(totalEstimated)) * 100
	}

	byType := buildNivellIndexByType(agg)

	indexing := map[string]interface{}{
		"total_pct": indexPct,
		"indexed":   totalIndexed,
		"estimated": totalEstimated,
		"by_type":   byType,
	}

	kpis = []map[string]interface{}{
		{"label": "Llibres totals", "value": len(llibres), "icon": "fa-book"},
		{"label": "Registres estimats", "value": totalEstimated, "icon": "fa-file-lines"},
		{"label": "Registres indexats", "value": totalIndexed, "icon": "fa-keyboard"},
		{"label": "Cobertura global", "value": fmt.Sprintf("%.1f%%", indexPct), "icon": "fa-chart-pie"},
	}

	return agg, indexing, kpis
}

func buildNivellUnits(municipis []db.MunicipiBrowseRow, llibres []db.LlibreRow, allowFallback bool, a *App, lang string) []map[string]interface{} {
	type munStat struct {
		Books     int
		Estimated int
		Indexed   int
	}
	statsByMun := map[int]*munStat{}
	if len(llibres) > 0 {
		bookIDs := make([]int, 0, len(llibres))
		for _, llibre := range llibres {
			bookIDs = append(bookIDs, llibre.ID)
		}
		stats, err := a.DB.GetLlibresIndexacioStats(bookIDs)
		if err != nil {
			stats = map[int]db.LlibreIndexacioStats{}
		}
		for _, llibre := range llibres {
			stat := stats[llibre.ID]
			estimated := stat.TotalRegistres
			if allowFallback && estimated == 0 {
				if count, err := a.DB.CountTranscripcionsRaw(llibre.ID, db.TranscripcioFilter{}); err == nil {
					estimated = count
				}
			}
			percent := stat.Percentatge
			if percent == 0 && llibre.IndexacioCompleta {
				percent = 100
			}
			indexed := int(math.Round(float64(estimated) * float64(percent) / 100))

			entry := statsByMun[llibre.MunicipiID]
			if entry == nil {
				entry = &munStat{}
				statsByMun[llibre.MunicipiID] = entry
			}
			entry.Books++
			entry.Estimated += estimated
			entry.Indexed += indexed
		}
	}

	unitRows := make([]map[string]interface{}, 0, len(municipis))
	for _, mun := range municipis {
		entry := statsByMun[mun.ID]
		books := 0
		indexPct := 0.0
		if entry != nil {
			books = entry.Books
			if entry.Estimated > 0 {
				indexPct = (float64(entry.Indexed) / float64(entry.Estimated)) * 100
			}
		}
		unitRows = append(unitRows, map[string]interface{}{
			"name":      mun.Nom,
			"type":      translateOrFallbackLabel(lang, "municipis.type."+strings.TrimSpace(mun.Tipus), defaultMunicipiType(mun.Tipus)),
			"level":     municipiLevelNumber(mun.LevelIDs),
			"municipis": 1,
			"books":     books,
			"index_pct": indexPct,
			"href":      fmt.Sprintf("/territori/municipis/%d", mun.ID),
		})
	}
	return unitRows
}

func buildNivellBookCategories(agg map[string]*nivellBookAgg, nivellID int, canViewLlibres bool, lang string) []municipiCategoryView {
	if len(agg) == 0 {
		return []municipiCategoryView{}
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
	seen := map[string]bool{}
	categories := []municipiCategoryView{}
	appendCategory := func(key string, count int, label string) {
		if count == 0 {
			return
		}
		icon := iconByType[key]
		if icon == "" {
			icon = "fa-book"
		}
		href := "#"
		if canViewLlibres {
			href = fmt.Sprintf("/documentals/llibres?nivell_id=%d&tipus_llibre=%s", nivellID, url.QueryEscape(key))
		}
		categories = append(categories, municipiCategoryView{
			Key:        key,
			Label:      label,
			Count:      count,
			Href:       href,
			Icon:       icon,
			CountLabel: countLabel,
		})
		seen[key] = true
	}
	for _, key := range categoryOrder {
		item, ok := agg[key]
		if !ok {
			continue
		}
		appendCategory(key, item.Volums, item.Label)
	}
	extra := []string{}
	for key := range agg {
		if !seen[key] {
			extra = append(extra, key)
		}
	}
	sort.Strings(extra)
	for _, key := range extra {
		item := agg[key]
		if item == nil {
			continue
		}
		appendCategory(key, item.Volums, item.Label)
	}
	return categories
}

func buildNivellIndexByType(agg map[string]*nivellBookAgg) []map[string]interface{} {
	order := []string{"baptismes", "confirmacions", "matrimonis", "obits", "padrons", "reclutaments", "altres"}
	seen := map[string]bool{}
	res := []map[string]interface{}{}
	appendRow := func(item *nivellBookAgg) {
		if item == nil {
			return
		}
		pct := 0.0
		if item.Estimated > 0 {
			pct = (float64(item.Indexed) / float64(item.Estimated)) * 100
		}
		res = append(res, map[string]interface{}{
			"name":      item.Label,
			"pct":       pct,
			"indexed":   item.Indexed,
			"estimated": item.Estimated,
		})
	}
	for _, key := range order {
		if item, ok := agg[key]; ok {
			appendRow(item)
			seen[key] = true
		}
	}
	extra := []string{}
	for key := range agg {
		if !seen[key] {
			extra = append(extra, key)
		}
	}
	sort.Strings(extra)
	for _, key := range extra {
		appendRow(agg[key])
	}
	return res
}

func buildNivellBooksList(agg map[string]*nivellBookAgg) []map[string]interface{} {
	if len(agg) == 0 {
		return []map[string]interface{}{}
	}
	order := []string{"baptismes", "confirmacions", "matrimonis", "obits", "padrons", "reclutaments", "altres"}
	seen := map[string]bool{}
	books := []map[string]interface{}{}
	appendRow := func(item *nivellBookAgg) {
		if item == nil {
			return
		}
		period := "—"
		if item.MinYear > 0 && item.MaxYear > 0 {
			if item.MinYear == item.MaxYear {
				period = fmt.Sprintf("%d", item.MinYear)
			} else {
				period = fmt.Sprintf("%d–%d", item.MinYear, item.MaxYear)
			}
		}
		books = append(books, map[string]interface{}{
			"cat":       item.Label,
			"volums":    item.Volums,
			"period":    period,
			"estimated": item.Estimated,
			"indexed":   item.Indexed,
		})
	}
	for _, key := range order {
		if item, ok := agg[key]; ok {
			appendRow(item)
			seen[key] = true
		}
	}
	extra := []string{}
	for key := range agg {
		if !seen[key] {
			extra = append(extra, key)
		}
	}
	sort.Strings(extra)
	for _, key := range extra {
		appendRow(agg[key])
	}
	return books
}

func buildNivellDemografia(a *App, nivellID int) (map[string][]map[string]interface{}, map[string]int) {
	series := map[string][]map[string]interface{}{
		"births":    {},
		"marriages": {},
		"deaths":    {},
	}
	totals := map[string]int{"births": 0, "marriages": 0, "deaths": 0}
	if nivellID <= 0 {
		return series, totals
	}
	rows, err := a.DB.ListNivellDemografiaDecades(nivellID, 0, 0)
	if err != nil {
		return series, totals
	}
	for _, row := range rows {
		if row.Any <= 0 {
			continue
		}
		series["births"] = append(series["births"], map[string]interface{}{"x": row.Any, "y": row.Natalitat})
		series["marriages"] = append(series["marriages"], map[string]interface{}{"x": row.Any, "y": row.Matrimonis})
		series["deaths"] = append(series["deaths"], map[string]interface{}{"x": row.Any, "y": row.Defuncions})
		totals["births"] += row.Natalitat
		totals["marriages"] += row.Matrimonis
		totals["deaths"] += row.Defuncions
	}
	if meta, err := a.DB.GetNivellDemografiaMeta(nivellID); err == nil && meta != nil {
		totals["births"] = meta.TotalNatalitat
		totals["marriages"] = meta.TotalMatrimonis
		totals["deaths"] = meta.TotalDefuncions
	}
	return series, totals
}

func buildNivellTopNames(a *App, nivellID int, isNom bool) []map[string]interface{} {
	type aggRow struct {
		ID     int
		Label  string
		Total  int
		Series map[int]int
	}
	limit := 50
	if nivellID <= 0 {
		return []map[string]interface{}{}
	}
	agg := map[int]*aggRow{}
	if isNom {
		rows, err := a.DB.ListTopNomsByNivell(nivellID, limit)
		if err != nil {
			return []map[string]interface{}{}
		}
		for _, row := range rows {
			if row.NomID <= 0 {
				continue
			}
			label := strings.TrimSpace(row.Forma)
			if label == "" {
				label = "—"
			}
			item := agg[row.NomID]
			if item == nil {
				item = &aggRow{ID: row.NomID, Label: label}
				agg[row.NomID] = item
			}
			item.Total += row.TotalFreq
		}
	} else {
		rows, err := a.DB.ListTopCognomsByNivell(nivellID, limit)
		if err != nil {
			return []map[string]interface{}{}
		}
		for _, row := range rows {
			if row.CognomID <= 0 {
				continue
			}
			label := strings.TrimSpace(row.Forma)
			if label == "" {
				label = "—"
			}
			item := agg[row.CognomID]
			if item == nil {
				item = &aggRow{ID: row.CognomID, Label: label}
				agg[row.CognomID] = item
			}
			item.Total += row.TotalFreq
		}
	}
	type pair struct {
		row *aggRow
	}
	pairs := make([]pair, 0, len(agg))
	for _, row := range agg {
		pairs = append(pairs, pair{row: row})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].row.Total == pairs[j].row.Total {
			return pairs[i].row.Label < pairs[j].row.Label
		}
		return pairs[i].row.Total > pairs[j].row.Total
	})
	maxItems := 40
	if len(pairs) < maxItems {
		maxItems = len(pairs)
	}
	seriesLimit := 12
	if maxItems < seriesLimit {
		seriesLimit = maxItems
	}
	for i := 0; i < seriesLimit; i++ {
		item := pairs[i].row
		if item == nil || item.ID <= 0 {
			continue
		}
		item.Series = map[int]int{}
		if isNom {
			rows, err := a.DB.ListNomSeriesByNivell(nivellID, item.ID, "decade")
			if err != nil {
				continue
			}
			for _, row := range rows {
				if row.AnyDoc <= 0 {
					continue
				}
				item.Series[row.AnyDoc] += row.Freq
			}
		} else {
			rows, err := a.DB.ListCognomSeriesByNivell(nivellID, item.ID, "decade")
			if err != nil {
				continue
			}
			for _, row := range rows {
				if row.AnyDoc <= 0 {
					continue
				}
				item.Series[row.AnyDoc] += row.Freq
			}
		}
	}
	res := make([]map[string]interface{}, 0, maxItems)
	for i := 0; i < maxItems; i++ {
		item := pairs[i].row
		if item == nil {
			continue
		}
		payload := map[string]interface{}{
			"name":  item.Label,
			"count": item.Total,
		}
		if len(item.Series) > 0 {
			years := make([]int, 0, len(item.Series))
			for year := range item.Series {
				years = append(years, year)
			}
			sort.Ints(years)
			series := make([]map[string]interface{}, 0, len(years))
			for _, year := range years {
				series = append(series, map[string]interface{}{
					"x": year,
					"y": item.Series[year],
				})
			}
			payload["series"] = series
		}
		res = append(res, payload)
	}
	return res
}

func buildNivellEvents(a *App, nivellID int, lang string) []map[string]interface{} {
	filter := db.EventHistoricFilter{
		Status: "publicat",
		Limit:  6,
	}
	events, err := a.DB.ListEventsByScope("nivell_admin", nivellID, filter)
	if err != nil {
		return []map[string]interface{}{}
	}
	rows := make([]map[string]interface{}, 0, len(events))
	for _, ev := range events {
		text := strings.TrimSpace(ev.Resum)
		if text == "" {
			text = strings.TrimSpace(ev.Descripcio)
		}
		rows = append(rows, map[string]interface{}{
			"year":  eventDateLabel(ev),
			"title": strings.TrimSpace(ev.Titol),
			"text":  text,
			"tag":   eventTypeLabel(lang, ev.Tipus),
		})
	}
	return rows
}

func parseYearRange(raw string) (int, int) {
	years := nivellYearRegex.FindAllString(raw, -1)
	if len(years) == 0 {
		return 0, 0
	}
	minYear := 0
	maxYear := 0
	for _, y := range years {
		yr, err := strconv.Atoi(y)
		if err != nil {
			continue
		}
		if minYear == 0 || yr < minYear {
			minYear = yr
		}
		if maxYear == 0 || yr > maxYear {
			maxYear = yr
		}
	}
	return minYear, maxYear
}

func defaultMunicipiType(tipus string) string {
	if strings.TrimSpace(tipus) == "" {
		return "Municipi"
	}
	return tipus
}

func municipiLevelNumber(levels [7]sql.NullInt64) int {
	for i := len(levels) - 1; i >= 0; i-- {
		if levels[i].Valid {
			return i + 1
		}
	}
	return 0
}

func formatDateISO(val sql.NullTime) string {
	if !val.Valid {
		return ""
	}
	return val.Time.Format("2006-01-02")
}
