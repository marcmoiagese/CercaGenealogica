package core

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type advancedSearchFilterView struct {
	Q                        string
	Nom                      string
	CognomPatern             string
	CognomMatern             string
	Pare                     string
	Mare                     string
	Parella                  string
	Entity                   string
	AncestorType             string
	AncestorID               int
	AncestorLabel            string
	MunicipiID               int
	PaisID                   int
	LevelIDs                 [7]int
	Exact                    bool
	OnlySurnameDirect        bool
	EntitatEclesiasticaID    int
	EntitatEclesiasticaLabel string
	ArxiuID                  int
	ArxiuLabel               string
	LlibreID                 int
	LlibreLabel              string
	DateFrom                 string
	DateTo                   string
	AnyFrom                  int
	AnyTo                    int
	TipusActe                string
	Sort                     string
	Page                     int
	PageSize                 int
}

type searchReason struct {
	Code  string `json:"code"`
	Label string `json:"label"`
}

func (a *App) AdvancedSearchPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	if _, ok := a.requirePermissionKeyAnyScope(w, r, permKeySearchAdvancedView); !ok {
		return
	}
	filter, view := a.parseAdvancedSearchFilter(r)
	if view.AncestorType == "municipi" && view.MunicipiID == 0 && view.AncestorID > 0 {
		view.MunicipiID = view.AncestorID
	}
	if view.AncestorType == "pais" && view.PaisID == 0 && view.AncestorID > 0 {
		view.PaisID = view.AncestorID
	}
	if (view.AncestorType == "nivell_admin" || view.AncestorType == "nivell") && view.AncestorID > 0 {
		needsLevels := true
		for _, id := range view.LevelIDs {
			if id > 0 {
				needsLevels = false
				break
			}
		}
		if needsLevels {
			levels, _ := a.DB.ListNivells(db.NivellAdminFilter{})
			byID := map[int]db.NivellAdministratiu{}
			for _, lvl := range levels {
				byID[lvl.ID] = lvl
			}
			levelID := view.AncestorID
			for levelID > 0 {
				lvl, ok := byID[levelID]
				if !ok {
					break
				}
				if lvl.Nivel >= 1 && lvl.Nivel <= 7 {
					view.LevelIDs[lvl.Nivel-1] = lvl.ID
				}
				if lvl.ParentID.Valid {
					levelID = int(lvl.ParentID.Int64)
					continue
				}
				break
			}
			if view.PaisID == 0 {
				view.PaisID = resolveNivellPaisID(view.AncestorID, byID)
			}
		}
	}
	if view.MunicipiID > 0 {
		needsLevels := true
		for _, id := range view.LevelIDs {
			if id > 0 {
				needsLevels = false
				break
			}
		}
		if needsLevels || view.PaisID == 0 {
			if mun, err := a.DB.GetMunicipi(view.MunicipiID); err == nil && mun != nil {
				if needsLevels {
					for i := 0; i < 7; i++ {
						if mun.NivellAdministratiuID[i].Valid {
							view.LevelIDs[i] = int(mun.NivellAdministratiuID[i].Int64)
						}
					}
				}
				if view.PaisID == 0 {
					levels, _ := a.DB.ListNivells(db.NivellAdminFilter{})
					byID := map[int]db.NivellAdministratiu{}
					for _, lvl := range levels {
						byID[lvl.ID] = lvl
					}
					for _, id := range view.LevelIDs {
						if id > 0 {
							view.PaisID = resolveNivellPaisID(id, byID)
							if view.PaisID > 0 {
								break
							}
						}
					}
				}
			}
		}
	}
	paisos, _ := a.DB.ListPaisos()
	levelSelects := []municipiLevelSelect{}
	levelTypeLabels := []string{}
	municipiOptions := []db.MunicipiBrowseRow{}
	if view.PaisID > 0 {
		nivells := a.municipiLevelsForPais(view.PaisID)
		var cols []municipiVisibleCol
		var showTipus bool
		levelSelects, cols, showTipus, levelTypeLabels = buildMunicipiLevelUI(nivells, view.LevelIDs)
		_ = cols
		_ = showTipus
	} else {
		levelTypeLabels = make([]string, 7)
	}
	hasTerritoryFilter := view.PaisID > 0 || view.MunicipiID > 0
	if !hasTerritoryFilter {
		for _, id := range view.LevelIDs {
			if id > 0 {
				hasTerritoryFilter = true
				break
			}
		}
	}
	if hasTerritoryFilter {
		filter := db.MunicipiBrowseFilter{
			PaisID: view.PaisID,
			Status: "publicat",
			Sort:   "nom",
			SortDir:"asc",
			Limit:  200,
		}
		filter.LevelIDs = view.LevelIDs
		rows, _ := a.DB.ListMunicipisBrowse(filter)
		municipiOptions = rows
	}
	if view.MunicipiID > 0 {
		found := false
		for _, row := range municipiOptions {
			if row.ID == view.MunicipiID {
				found = true
				break
			}
		}
		if !found {
			if mun, err := a.DB.GetMunicipi(view.MunicipiID); err == nil && mun != nil {
				municipiOptions = append([]db.MunicipiBrowseRow{{ID: mun.ID, Nom: mun.Nom}}, municipiOptions...)
			}
		}
	}
	RenderPrivateTemplate(w, r, "cerca-avancada.html", map[string]interface{}{
		"Filter":           view,
		"Paisos":           paisos,
		"LevelSelects":     levelSelects,
		"LevelTypeLabels":  levelTypeLabels,
		"MunicipiOptions": municipiOptions,
		"TipusActeOptions": transcripcioTipusActe,
		"Sort":             filter.Sort,
	})
}

func (a *App) SearchAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	if _, ok := a.requirePermissionKeyAnyScope(w, r, permKeySearchAdvancedView); !ok {
		return
	}
	a.ensureSearchIndexReady()
	filter, _ := a.parseAdvancedSearchFilter(r)
	results, total, facets, err := a.DB.SearchDocs(filter)
	if err != nil {
		Errorf("SearchAPI error: %v", err)
		http.Error(w, "No s'ha pogut fer la cerca", http.StatusInternalServerError)
		return
	}
	lang := ResolveLang(r)
	page := filter.Page
	if page <= 0 {
		page = 1
	}
	pageSize := filter.PageSize
	if pageSize <= 0 {
		pageSize = 25
	}
	totalPages := 1
	if total > 0 {
		totalPages = (total + pageSize - 1) / pageSize
	}

	personaCache := map[int]*db.Persona{}
	registreCache := map[int]*db.TranscripcioRaw{}
	llibreCache := map[int]*db.Llibre{}
	municipiCache := map[int]*db.Municipi{}

	getPersona := func(id int) *db.Persona {
		if v, ok := personaCache[id]; ok {
			return v
		}
		p, err := a.DB.GetPersona(id)
		if err != nil || p == nil {
			personaCache[id] = nil
			return nil
		}
		personaCache[id] = p
		return p
	}
	getRegistre := func(id int) *db.TranscripcioRaw {
		if v, ok := registreCache[id]; ok {
			return v
		}
		reg, err := a.DB.GetTranscripcioRaw(id)
		if err != nil || reg == nil {
			registreCache[id] = nil
			return nil
		}
		registreCache[id] = reg
		return reg
	}
	getLlibre := func(id int) *db.Llibre {
		if id <= 0 {
			return nil
		}
		if v, ok := llibreCache[id]; ok {
			return v
		}
		llib, err := a.DB.GetLlibre(id)
		if err != nil || llib == nil {
			llibreCache[id] = nil
			return nil
		}
		llibreCache[id] = llib
		return llib
	}
	getMunicipi := func(id int) *db.Municipi {
		if id <= 0 {
			return nil
		}
		if v, ok := municipiCache[id]; ok {
			return v
		}
		mun, err := a.DB.GetMunicipi(id)
		if err != nil || mun == nil {
			municipiCache[id] = nil
			return nil
		}
		municipiCache[id] = mun
		return mun
	}

	items := make([]map[string]interface{}, 0, len(results))
	for _, row := range results {
		reasons := buildSearchReasons(row, filter, lang)
		switch row.EntityType {
		case "persona":
			persona := getPersona(row.EntityID)
			if persona == nil {
				continue
			}
			title := strings.TrimSpace(strings.Join([]string{persona.Nom, persona.Cognom1, persona.Cognom2}, " "))
			if title == "" {
				title = "#" + strconv.Itoa(persona.ID)
			}
			metaParts := []string{}
			if persona.DataNaixement.Valid {
				metaParts = append(metaParts, "n. "+strings.TrimSpace(persona.DataNaixement.String))
			}
			if persona.DataDefuncio.Valid {
				metaParts = append(metaParts, "† "+strings.TrimSpace(persona.DataDefuncio.String))
			}
			if strings.TrimSpace(persona.Municipi) != "" {
				metaParts = append(metaParts, strings.TrimSpace(persona.Municipi))
			}
			items = append(items, map[string]interface{}{
				"entity_type":       row.EntityType,
				"entity_type_label": T(lang, "search.entity.persona"),
				"entity_id":         row.EntityID,
				"title":             title,
				"subtitle":          strings.Join(metaParts, " · "),
				"url":               "/persones/" + strconv.Itoa(row.EntityID),
				"reasons":           reasons,
				"score":             row.Score,
			})
		case "registre_raw":
			registre := getRegistre(row.EntityID)
			if registre == nil {
				continue
			}
			persones, _ := a.DB.ListTranscripcioPersones(registre.ID)
			subjecte := subjectFromPersons(registre.TipusActe, persones)
			if strings.TrimSpace(subjecte) == "" {
				subjecte = T(lang, "records.detail.none")
			}
			matchInfo := buildRegistreMatchInfo(filter, persones, lang)
			tipusLabel := strings.TrimSpace(registre.TipusActe)
			if tipusLabel != "" {
				tipusLabel = T(lang, "records.type."+tipusLabel)
			}
			llib := getLlibre(registre.LlibreID)
			llibLabel := ""
			munLabel := ""
			if llib != nil {
				llibLabel = strings.TrimSpace(llib.Titol)
				if llibLabel == "" {
					llibLabel = strings.TrimSpace(llib.NomEsglesia)
				}
				if mun := getMunicipi(llib.MunicipiID); mun != nil {
					munLabel = strings.TrimSpace(mun.Nom)
				}
			}
			metaParts := []string{}
			if tipusLabel != "" {
				metaParts = append(metaParts, tipusLabel)
			}
			if registre.AnyDoc.Valid {
				metaParts = append(metaParts, strconv.FormatInt(registre.AnyDoc.Int64, 10))
			} else if registre.DataActeISO.Valid {
				metaParts = append(metaParts, strings.TrimSpace(registre.DataActeISO.String))
			}
			if llibLabel != "" {
				metaParts = append(metaParts, llibLabel)
			}
			if munLabel != "" {
				metaParts = append(metaParts, munLabel)
			}
			items = append(items, map[string]interface{}{
				"entity_type":       row.EntityType,
				"entity_type_label": T(lang, "search.entity.registre_raw"),
				"entity_id":         row.EntityID,
				"title":             subjecte,
				"subtitle":          strings.Join(metaParts, " · "),
				"url":               "/documentals/registres/" + strconv.Itoa(row.EntityID),
				"tipus_acte":        registre.TipusActe,
				"tipus_acte_label":  tipusLabel,
				"match_info":        matchInfo,
				"reasons":           reasons,
				"score":             row.Score,
			})
		}
	}

	writeJSON(w, map[string]interface{}{
		"items":       items,
		"total":       total,
		"page":        page,
		"page_size":   pageSize,
		"total_pages": totalPages,
		"facets": map[string]interface{}{
			"entity_type": facets.EntityType,
			"tipus_acte":  facets.TipusActe,
		},
		"query": map[string]interface{}{
			"q":      filter.Query,
			"entity": filter.Entity,
		},
	})
}

func (a *App) parseAdvancedSearchFilter(r *http.Request) (db.SearchQueryFilter, advancedSearchFilterView) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	nom := strings.TrimSpace(r.URL.Query().Get("nom"))
	cognom1 := strings.TrimSpace(r.URL.Query().Get("cognom1"))
	cognom2 := strings.TrimSpace(r.URL.Query().Get("cognom2"))
	if cognom1 == "" && cognom2 == "" {
		if legacy := strings.TrimSpace(r.URL.Query().Get("cognom")); legacy != "" {
			cognom1 = legacy
		}
	}
	pare := strings.TrimSpace(r.URL.Query().Get("pare"))
	mare := strings.TrimSpace(r.URL.Query().Get("mare"))
	parella := strings.TrimSpace(r.URL.Query().Get("parella"))
	entity := strings.TrimSpace(r.URL.Query().Get("entity"))
	if entity == "" {
		entity = "all"
	}
	ancestorType := strings.TrimSpace(r.URL.Query().Get("ancestor_type"))
	if ancestorType == "nivell_admin" {
		ancestorType = "nivell"
	}
	ancestorID := parseFormInt(r.URL.Query().Get("ancestor_id"))
	ancestorLabel := strings.TrimSpace(r.URL.Query().Get("ancestor_label"))
	municipiID := parseFormInt(r.URL.Query().Get("municipi_id"))
	paisID := parseFormInt(r.URL.Query().Get("pais_id"))
	levelIDs := [7]int{}
	for i := 0; i < 7; i++ {
		key := fmt.Sprintf("nivell_id_%d", i+1)
		levelIDs[i] = parseFormInt(r.URL.Query().Get(key))
	}
	if municipiID == 0 && ancestorType == "municipi" && ancestorID > 0 {
		municipiID = ancestorID
	}
	if paisID == 0 && ancestorType == "pais" && ancestorID > 0 {
		paisID = ancestorID
	}
	entitatID := parseFormInt(r.URL.Query().Get("entitat_eclesiastica_id"))
	entitatLabel := strings.TrimSpace(r.URL.Query().Get("entitat_label"))
	arxiuID := parseFormInt(r.URL.Query().Get("arxiu_id"))
	arxiuLabel := strings.TrimSpace(r.URL.Query().Get("arxiu_label"))
	llibreID := parseFormInt(r.URL.Query().Get("llibre_id"))
	llibreLabel := strings.TrimSpace(r.URL.Query().Get("llibre_label"))
	dateFrom := strings.TrimSpace(r.URL.Query().Get("from"))
	if dateFrom != "" && ParseDate(dateFrom).IsZero() {
		dateFrom = ""
	}
	dateTo := strings.TrimSpace(r.URL.Query().Get("to"))
	if dateTo != "" && ParseDate(dateTo).IsZero() {
		dateTo = ""
	}
	anyFrom := parseFormInt(r.URL.Query().Get("any_from"))
	anyTo := parseFormInt(r.URL.Query().Get("any_to"))
	tipusActe := strings.TrimSpace(r.URL.Query().Get("tipus_acte"))
	if tipusActe != "" && !validTipusActe(tipusActe) {
		tipusActe = ""
	}
	sort := strings.TrimSpace(r.URL.Query().Get("sort"))
	switch sort {
	case "date_desc", "date_asc", "relevance":
	default:
		sort = "relevance"
	}
	page := parseFormInt(r.URL.Query().Get("page"))
	if page <= 0 {
		page = 1
	}
	pageSizeParam := strings.TrimSpace(r.URL.Query().Get("page_size"))
	if pageSizeParam == "" {
		pageSizeParam = strings.TrimSpace(r.URL.Query().Get("per_page"))
	}
	pageSize := parseListPerPage(pageSizeParam)

	queryNorm := ""
	queryPhonetic := ""
	queryTokens := []string{}
	canonTokens := []string{}
	variantTokens := []string{}
	nameNorm := ""
	surnameNorm := ""
	nameTokens := []string{}
	surnameTokens := []string{}
	surnameTokens1 := []string{}
	surnameTokens2 := []string{}
	parentTokens := []string{}
	motherTokens := []string{}
	partnerTokens := []string{}
	if nom != "" {
		nameNorm = normalizeSearchText(nom)
		nameTokens = normalizeQueryTokens(nom)
	}
	if cognom1 != "" || cognom2 != "" {
		combined := strings.TrimSpace(strings.Join([]string{cognom1, cognom2}, " "))
		surnameNorm = normalizeSearchText(combined)
		surnameTokens = normalizeQueryTokens(combined)
		if cognom1 != "" {
			surnameTokens1 = normalizeQueryTokens(cognom1)
		}
		if cognom2 != "" {
			surnameTokens2 = normalizeQueryTokens(cognom2)
		}
		if len(surnameTokens) > 0 {
			canonTokens, variantTokens = a.expandCognomTokens(surnameTokens)
		}
	}
	if pare != "" {
		parentTokens = normalizeQueryTokens(pare)
	}
	if mare != "" {
		motherTokens = normalizeQueryTokens(mare)
	}
	if parella != "" {
		partnerTokens = normalizeQueryTokens(parella)
	}
	exact := parseFormBool(r.URL.Query().Get("exact"))
	onlySurnameDirect := parseFormBool(r.URL.Query().Get("only_surname"))
	if q == "" && (nom != "" || cognom1 != "" || cognom2 != "") {
		q = strings.TrimSpace(strings.Join([]string{nom, cognom1, cognom2}, " "))
	}
	if q != "" {
		queryNorm = normalizeSearchText(q)
		queryTokens = normalizeQueryTokens(q)
		if len(queryTokens) > 0 {
			if len(surnameTokens) == 0 {
				canonTokens, variantTokens = a.expandCognomTokens(queryTokens)
			}
			queryPhonetic = strings.Join(phoneticTokens(queryTokens), " ")
		}
	}
	if ancestorType == "" && ancestorID == 0 {
		if municipiID > 0 {
			ancestorType = "municipi"
			ancestorID = municipiID
		} else {
			deepest := 0
			for i := 0; i < 7; i++ {
				if levelIDs[i] > 0 {
					deepest = levelIDs[i]
				}
			}
			if deepest > 0 {
				ancestorType = "nivell"
				ancestorID = deepest
			} else if paisID > 0 {
				ancestorType = "pais"
				ancestorID = paisID
			}
		}
	}

	filter := db.SearchQueryFilter{
		Query:                 q,
		QueryNorm:             queryNorm,
		QueryPhonetic:         queryPhonetic,
		QueryTokens:           queryTokens,
		CanonTokens:           canonTokens,
		VariantTokens:         variantTokens,
		Name:                  nom,
		Surname1:              cognom1,
		Surname2:              cognom2,
		NameNorm:              nameNorm,
		SurnameNorm:           surnameNorm,
		NameTokens:            nameTokens,
		SurnameTokens:         surnameTokens,
		SurnameTokens1:        surnameTokens1,
		SurnameTokens2:        surnameTokens2,
		Father:                pare,
		Mother:                mare,
		Partner:               parella,
		FatherTokens:          parentTokens,
		MotherTokens:          motherTokens,
		PartnerTokens:         partnerTokens,
		Exact:                 exact,
		OnlySurnameDirect:     onlySurnameDirect,
		Entity:                entity,
		AncestorType:          ancestorType,
		AncestorID:            ancestorID,
		EntitatEclesiasticaID: entitatID,
		ArxiuID:               arxiuID,
		LlibreID:              llibreID,
		DateFrom:              dateFrom,
		DateTo:                dateTo,
		AnyFrom:               anyFrom,
		AnyTo:                 anyTo,
		TipusActe:             tipusActe,
		Page:                  page,
		PageSize:              pageSize,
		Sort:                  sort,
	}
	view := advancedSearchFilterView{
		Q:                        q,
		Nom:                      nom,
		CognomPatern:             cognom1,
		CognomMatern:             cognom2,
		Pare:                     pare,
		Mare:                     mare,
		Parella:                  parella,
		Entity:                   entity,
		AncestorType:             ancestorType,
		AncestorID:               ancestorID,
		AncestorLabel:            ancestorLabel,
		MunicipiID:               municipiID,
		PaisID:                   paisID,
		LevelIDs:                 levelIDs,
		Exact:                    exact,
		OnlySurnameDirect:        onlySurnameDirect,
		EntitatEclesiasticaID:    entitatID,
		EntitatEclesiasticaLabel: entitatLabel,
		ArxiuID:                  arxiuID,
		ArxiuLabel:               arxiuLabel,
		LlibreID:                 llibreID,
		LlibreLabel:              llibreLabel,
		DateFrom:                 dateFrom,
		DateTo:                   dateTo,
		AnyFrom:                  anyFrom,
		AnyTo:                    anyTo,
		TipusActe:                tipusActe,
		Sort:                     sort,
		Page:                     page,
		PageSize:                 pageSize,
	}
	return filter, view
}

func buildSearchReasons(row db.SearchDocRow, filter db.SearchQueryFilter, lang string) []searchReason {
	if strings.TrimSpace(filter.QueryNorm) == "" {
		return []searchReason{}
	}
	reasons := []searchReason{}
	addReason := func(code, label string) {
		for _, r := range reasons {
			if r.Code == code {
				return
			}
		}
		reasons = append(reasons, searchReason{Code: code, Label: label})
	}
	if row.PersonFullNorm == filter.QueryNorm {
		addReason("exact_full", T(lang, "search.reason.exact_full"))
	}
	if len(filter.VariantTokens) > 0 && containsAnyToken(row.CognomsCanon, filter.CanonTokens) {
		addReason("surname_variant", T(lang, "search.reason.surname_variant"))
	}
	if containsAnyToken(row.PersonTokensNorm, filter.QueryTokens) || containsAnyToken(row.CognomsTokensNorm, filter.QueryTokens) {
		addReason("partial_tokens", T(lang, "search.reason.partial_tokens"))
	}
	if filter.QueryPhonetic != "" && (strings.Contains(row.PersonPhonetic, filter.QueryPhonetic) || strings.Contains(row.CognomsPhonetic, filter.QueryPhonetic)) {
		addReason("phonetic", T(lang, "search.reason.phonetic"))
	}
	return reasons
}

func containsAnyToken(haystack string, tokens []string) bool {
	if strings.TrimSpace(haystack) == "" || len(tokens) == 0 {
		return false
	}
	for _, token := range tokens {
		if token == "" {
			continue
		}
		if strings.Contains(haystack, token) {
			return true
		}
	}
	return false
}

func buildRegistreMatchInfo(filter db.SearchQueryFilter, persones []db.TranscripcioPersonaRaw, lang string) string {
	if strings.TrimSpace(filter.QueryNorm) == "" || len(persones) == 0 {
		return ""
	}
	preferCognom := len(filter.CanonTokens) > 0 || len(filter.VariantTokens) > 0 || len(filter.SurnameTokens) > 0 || filter.OnlySurnameDirect
	queryTokens := filter.QueryTokens
	if preferCognom && len(filter.CanonTokens) > 0 {
		queryTokens = filter.CanonTokens
	} else if preferCognom && len(filter.SurnameTokens) > 0 {
		queryTokens = filter.SurnameTokens
	}
	if preferCognom && len(queryTokens) == 0 {
		queryTokens = filter.QueryTokens
	}
	seen := map[string]struct{}{}
	matches := []string{}
	for _, p := range persones {
		fullName := strings.TrimSpace(strings.Join([]string{p.Nom, p.Cognom1, p.Cognom2}, " "))
		if fullName == "" {
			continue
		}
		cognomTokens := normalizeTokens(p.Cognom1, p.Cognom2)
		personTokens := normalizeTokens(p.Nom, p.Cognom1, p.Cognom2)
		matched := false
		if preferCognom {
			if containsAnyToken(strings.Join(cognomTokens, " "), queryTokens) {
				matched = true
			}
			if !matched && filter.QueryPhonetic != "" {
				phoneticStr := strings.Join(phoneticTokens(cognomTokens), " ")
				if hasAnyPhoneticMatch(phoneticStr, filter.QueryPhonetic) {
					matched = true
				}
			}
		} else {
			if containsAnyToken(strings.Join(personTokens, " "), queryTokens) {
				matched = true
			}
			if !matched && filter.QueryPhonetic != "" {
				phoneticStr := strings.Join(phoneticTokens(personTokens), " ")
				if hasAnyPhoneticMatch(phoneticStr, filter.QueryPhonetic) {
					matched = true
				}
			}
		}
		if !matched {
			continue
		}
		label := roleLabelForSearch(lang, p.Rol)
		entry := fullName
		if label != "" {
			entry = label + ": " + fullName
		}
		if _, ok := seen[entry]; ok {
			continue
		}
		seen[entry] = struct{}{}
		matches = append(matches, entry)
	}
	if len(matches) == 0 {
		return ""
	}
	limit := 3
	extra := 0
	if len(matches) > limit {
		extra = len(matches) - limit
		matches = matches[:limit]
	}
	prefix := T(lang, "search.match.label")
	if prefix == "search.match.label" {
		prefix = "Coincidència"
	}
	info := fmt.Sprintf("%s: %s", prefix, strings.Join(matches, " · "))
	if extra > 0 {
		info = info + fmt.Sprintf(" · +%d", extra)
	}
	return info
}

func roleLabelForSearch(lang, role string) string {
	role = strings.TrimSpace(role)
	if role == "" {
		return ""
	}
	key := "records.role." + role
	label := T(lang, key)
	if label == key {
		return role
	}
	return label
}

func hasAnyPhoneticMatch(phoneticStr, queryPhonetic string) bool {
	if strings.TrimSpace(phoneticStr) == "" || strings.TrimSpace(queryPhonetic) == "" {
		return false
	}
	for _, code := range strings.Fields(queryPhonetic) {
		if code == "" {
			continue
		}
		if strings.Contains(phoneticStr, code) {
			return true
		}
	}
	return false
}

func normalizeQueryTokens(query string) []string {
	rawTokens := normalizeTokens(query)
	seen := map[string]struct{}{}
	out := []string{}
	for _, token := range rawTokens {
		if token == "" {
			continue
		}
		switch token {
		case "y":
			token = "i"
		case "de", "del", "d", "da", "di", "la", "el", "l":
			continue
		}
		if len(token) < 2 {
			continue
		}
		if _, ok := seen[token]; ok {
			continue
		}
		seen[token] = struct{}{}
		out = append(out, token)
	}
	return out
}

func (a *App) expandCognomTokens(tokens []string) ([]string, []string) {
	canonSet := map[string]struct{}{}
	variantSet := map[string]struct{}{}
	for _, token := range tokens {
		if token == "" {
			continue
		}
		canonToken := token
		usedVariant := false
		id, canon, ok, err := a.DB.ResolveCognomPublicatByForma(token)
		if err == nil && ok && id > 0 {
			canonNorm := normalizeSearchText(canon)
			if canonNorm != "" {
				canonToken = canonNorm
			}
			if canonToken != token {
				usedVariant = true
			}
			if forms, err := a.DB.ListCognomFormesPublicades(id); err == nil {
				for _, form := range forms {
					formNorm := normalizeSearchText(form)
					if formNorm == "" {
						continue
					}
					variantSet[formNorm] = struct{}{}
				}
			}
		}
		canonSet[canonToken] = struct{}{}
		if usedVariant {
			variantSet[token] = struct{}{}
		}
	}
	canonTokens := make([]string, 0, len(canonSet))
	for key := range canonSet {
		canonTokens = append(canonTokens, key)
	}
	variantTokens := make([]string, 0, len(variantSet))
	for key := range variantSet {
		variantTokens = append(variantTokens, key)
	}
	return canonTokens, variantTokens
}

func (a *App) SearchArxiusSuggestJSON(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
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
	filter := db.ArxiuFilter{
		Text:   query,
		Status: "publicat",
		Limit:  limit,
	}
	rows, _ := a.DB.ListArxius(filter)
	items := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		context := joinNonEmpty(row.MunicipiNom.String, row.EntitatNom.String, " · ")
		items = append(items, map[string]interface{}{
			"id":      row.ID,
			"nom":     row.Nom,
			"context": strings.TrimSpace(context),
		})
	}
	writeJSON(w, map[string]interface{}{"items": items})
}

func (a *App) SearchLlibresSuggestJSON(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
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
	rows, _ := a.DB.SearchLlibresSimple(query, limit)
	items := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		label := strings.TrimSpace(row.Titol)
		if label == "" {
			label = strings.TrimSpace(row.NomEsglesia)
		}
		context := joinNonEmpty(strings.TrimSpace(row.NomEsglesia), strings.TrimSpace(row.Cronologia), " · ")
		if row.Municipi.Valid {
			context = joinNonEmpty(context, strings.TrimSpace(row.Municipi.String), " · ")
		}
		items = append(items, map[string]interface{}{
			"id":      row.ID,
			"nom":     label,
			"context": strings.TrimSpace(context),
		})
	}
	writeJSON(w, map[string]interface{}{"items": items})
}
