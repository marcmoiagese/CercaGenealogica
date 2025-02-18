package core

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const (
	espaiMatchDefaultMaxCandidates  = 25
	espaiMatchDefaultMinScore       = 60
	espaiMatchDefaultWeightName     = 40
	espaiMatchDefaultWeightSurname  = 30
	espaiMatchDefaultWeightDate     = 15
	espaiMatchDefaultWeightPlace    = 10
	espaiMatchDefaultWeightRelation = 5
)

type espaiMatchConfig struct {
	MaxCandidates int
	MinScore      int
	WeightName    int
	WeightSurname int
	WeightDate    int
	WeightPlace   int
	WeightRel     int
}

type espaiMatchReasonItem struct {
	Key    string `json:"key"`
	Score  int    `json:"score"`
	Weight int    `json:"weight"`
}

type espaiMatchReason struct {
	Total int                   `json:"total"`
	Items []espaiMatchReasonItem `json:"items"`
}

type espaiMatchView struct {
	ID            int
	PersonaName   string
	TargetName    string
	TargetMeta    string
	TargetURL     string
	Score         int
	Status        string
	Reasons       []espaiMatchReasonItem
	UpdatedAtText string
}

type espaiRelationInfo struct {
	Fathers []string
	Mothers []string
}

func (a *App) espaiMatchConfig() espaiMatchConfig {
	cfg := espaiMatchConfig{
		MaxCandidates: parseIntDefault(a.Config["ESP_MATCH_MAX_CANDIDATES"], espaiMatchDefaultMaxCandidates),
		MinScore:      parseIntDefault(a.Config["ESP_MATCH_MIN_SCORE"], espaiMatchDefaultMinScore),
		WeightName:    parseIntDefault(a.Config["ESP_MATCH_WEIGHT_NAME"], espaiMatchDefaultWeightName),
		WeightSurname: parseIntDefault(a.Config["ESP_MATCH_WEIGHT_SURNAME"], espaiMatchDefaultWeightSurname),
		WeightDate:    parseIntDefault(a.Config["ESP_MATCH_WEIGHT_DATE"], espaiMatchDefaultWeightDate),
		WeightPlace:   parseIntDefault(a.Config["ESP_MATCH_WEIGHT_PLACE"], espaiMatchDefaultWeightPlace),
		WeightRel:     parseIntDefault(a.Config["ESP_MATCH_WEIGHT_RELATIONS"], espaiMatchDefaultWeightRelation),
	}
	if cfg.MaxCandidates <= 0 {
		cfg.MaxCandidates = espaiMatchDefaultMaxCandidates
	}
	if cfg.MinScore <= 0 {
		cfg.MinScore = espaiMatchDefaultMinScore
	}
	return cfg
}

func (a *App) EspaiPersonalCoincidenciesPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}

	statusFilter := strings.TrimSpace(r.URL.Query().Get("status"))
	if statusFilter == "" {
		statusFilter = "pending"
	}
	lang := ResolveLang(r)

	allMatches, _ := a.DB.ListEspaiCoincidenciesByOwner(user.ID)
	counts := map[string]int{
		"pending":  0,
		"accepted": 0,
		"ignored":  0,
		"rejected": 0,
	}

	for _, m := range allMatches {
		if _, ok := counts[m.Status]; ok {
			counts[m.Status]++
		}
	}

	personCache := map[int]*db.EspaiPersona{}
	regCache := map[int]*db.TranscripcioRaw{}
	regPersonsCache := map[int][]db.TranscripcioPersonaRaw{}
	llibreCache := map[int]*db.Llibre{}
	municipiCache := map[int]*db.Municipi{}

	views := []espaiMatchView{}
	for _, m := range allMatches {
		if statusFilter != "all" && statusFilter != "" && m.Status != statusFilter {
			continue
		}
		persona := getEspaiPersonaCached(a, personCache, m.PersonaID)
		personaName := "-"
		if persona != nil {
			personaName = espaiPersonaDisplayName(*persona)
		}
		targetName, targetMeta, targetURL := "-", "", ""
		if m.TargetType == "registre_raw" {
			reg := getRegistreCached(a, regCache, m.TargetID)
			if reg != nil {
				persones := getRegistrePersonesCached(a, regPersonsCache, reg.ID)
				subjecte := subjectFromPersons(reg.TipusActe, persones)
				if strings.TrimSpace(subjecte) == "" {
					subjecte = T(lang, "records.detail.none")
				}
				targetName = subjecte
				targetMeta = buildRegistreMeta(a, reg, llibreCache, municipiCache, lang)
				targetURL = "/documentals/registres/" + strconv.Itoa(reg.ID)
			}
		}
		score := 0
		reasons := []espaiMatchReasonItem{}
		if m.Score.Valid {
			score = int(math.Round(m.Score.Float64 * 100))
		}
		if m.ReasonJSON.Valid {
			var reason espaiMatchReason
			if err := json.Unmarshal([]byte(m.ReasonJSON.String), &reason); err == nil {
				reasons = reason.Items
			}
		}
		updatedText := ""
		if m.UpdatedAt.Valid {
			updatedText = m.UpdatedAt.Time.Format("2006-01-02 15:04")
		}
		views = append(views, espaiMatchView{
			ID:            m.ID,
			PersonaName:   personaName,
			TargetName:    targetName,
			TargetMeta:    targetMeta,
			TargetURL:     targetURL,
			Score:         score,
			Status:        m.Status,
			Reasons:       reasons,
			UpdatedAtText: updatedText,
		})
	}

	trees, _ := a.DB.ListEspaiArbresByOwner(user.ID)

	spaceState := "ready"
	if len(allMatches) == 0 {
		spaceState = "empty"
	}

	RenderPrivateTemplate(w, r, "espai.html", map[string]interface{}{
		"SpaceSection": "coincidencies",
		"SpaceState":   spaceState,
		"Matches":      views,
		"MatchCounts":  counts,
		"MatchStatus":  statusFilter,
		"MatchTrees":   trees,
		"UploadError":  strings.TrimSpace(r.URL.Query().Get("error")),
		"UploadNotice": strings.TrimSpace(r.URL.Query().Get("notice")),
	})
}

func (a *App) EspaiCoincidenciesDecide(w http.ResponseWriter, r *http.Request) {
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

	matchID := parseFormInt(r.FormValue("match_id"))
	decision := strings.TrimSpace(r.FormValue("decision"))
	if matchID == 0 || decision == "" {
		http.Redirect(w, r, "/espai/coincidencies?error="+urlQueryEscape(T(ResolveLang(r), "space.matches.error.invalid")), http.StatusSeeOther)
		return
	}

	match, err := a.DB.GetEspaiCoincidencia(matchID)
	if err != nil || match == nil || match.OwnerUserID != user.ID {
		http.Redirect(w, r, "/espai/coincidencies?error="+urlQueryEscape(T(ResolveLang(r), "space.matches.error.not_found")), http.StatusSeeOther)
		return
	}

	status := mapDecisionToStatus(decision)
	if status == "" {
		http.Redirect(w, r, "/espai/coincidencies?error="+urlQueryEscape(T(ResolveLang(r), "space.matches.error.invalid")), http.StatusSeeOther)
		return
	}
	if err := a.DB.UpdateEspaiCoincidenciaStatus(match.ID, status); err != nil {
		http.Redirect(w, r, "/espai/coincidencies?error="+urlQueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	_, _ = a.DB.CreateEspaiCoincidenciaDecision(&db.EspaiCoincidenciaDecision{
		CoincidenciaID: match.ID,
		Decision:       decision,
		DecidedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
	})

	http.Redirect(w, r, "/espai/coincidencies?status="+urlQueryEscape(status), http.StatusSeeOther)
}

func (a *App) EspaiCoincidenciesBulk(w http.ResponseWriter, r *http.Request) {
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

	decision := strings.TrimSpace(r.FormValue("decision"))
	status := mapDecisionToStatus(decision)
	if status == "" {
		http.Redirect(w, r, "/espai/coincidencies?error="+urlQueryEscape(T(ResolveLang(r), "space.matches.error.invalid")), http.StatusSeeOther)
		return
	}

	ids := parseMatchIDs(r)
	if len(ids) == 0 {
		http.Redirect(w, r, "/espai/coincidencies?error="+urlQueryEscape(T(ResolveLang(r), "space.matches.error.empty")), http.StatusSeeOther)
		return
	}

	for _, id := range ids {
		match, err := a.DB.GetEspaiCoincidencia(id)
		if err != nil || match == nil || match.OwnerUserID != user.ID {
			continue
		}
		_ = a.DB.UpdateEspaiCoincidenciaStatus(match.ID, status)
		_, _ = a.DB.CreateEspaiCoincidenciaDecision(&db.EspaiCoincidenciaDecision{
			CoincidenciaID: match.ID,
			Decision:       decision,
			DecidedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
		})
	}

	http.Redirect(w, r, "/espai/coincidencies?status="+urlQueryEscape(status), http.StatusSeeOther)
}

func (a *App) EspaiCoincidenciesRebuild(w http.ResponseWriter, r *http.Request) {
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

	arbreID := parseFormInt(r.FormValue("arbre_id"))
	trees, _ := a.DB.ListEspaiArbresByOwner(user.ID)
	rebuilt := 0
	for _, t := range trees {
		if arbreID > 0 && t.ID != arbreID {
			continue
		}
		if count, err := a.rebuildEspaiCoincidenciesForArbre(user.ID, t.ID); err == nil {
			rebuilt += count
		}
	}

	notice := fmt.Sprintf(T(ResolveLang(r), "space.matches.notice.rebuilt"), rebuilt)
	http.Redirect(w, r, "/espai/coincidencies?notice="+urlQueryEscape(notice), http.StatusSeeOther)
}

func (a *App) EspaiCoincidenciesAPI(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	statusFilter := strings.TrimSpace(r.URL.Query().Get("status"))
	allMatches, _ := a.DB.ListEspaiCoincidenciesByOwner(user.ID)
	out := []db.EspaiCoincidencia{}
	for _, m := range allMatches {
		if statusFilter == "" || statusFilter == "all" || m.Status == statusFilter {
			out = append(out, m)
		}
	}
	writeJSON(w, map[string]interface{}{
		"ok":    true,
		"items": out,
	})
}

func (a *App) rebuildEspaiCoincidenciesForArbre(ownerID, arbreID int) (int, error) {
	persones, err := a.DB.ListEspaiPersonesByArbre(arbreID)
	if err != nil {
		return 0, err
	}
	relacions, _ := a.DB.ListEspaiRelacionsByArbre(arbreID)
	relationIndex := buildEspaiRelationIndex(persones, relacions)

	cfg := a.espaiMatchConfig()
	newMatches := 0
	a.ensureSearchIndexReady()

	munCache := map[int]string{}
	for _, p := range persones {
		if p.OwnerUserID != ownerID {
			continue
		}
		info := relationIndex[p.ID]
		count, err := a.matchEspaiPersona(ownerID, p, info, cfg, munCache)
		if err != nil {
			continue
		}
		newMatches += count
	}
	if newMatches > 0 {
		a.notifyEspaiMatches(ownerID, arbreID, newMatches)
	}
	return newMatches, nil
}

func (a *App) matchEspaiPersona(ownerID int, p db.EspaiPersona, rel espaiRelationInfo, cfg espaiMatchConfig, munCache map[int]string) (int, error) {
	name := espaiPersonaDisplayName(p)
	if name == "" || name == "-" {
		return 0, nil
	}
	fullNorm := normalizeSearchText(name)
	nameTokens := normalizeTokens(p.Nom.String)
	surnameTokens := normalizeTokens(strings.TrimSpace(strings.Join([]string{p.Cognom1.String, p.Cognom2.String}, " ")))

	filter := db.SearchQueryFilter{
		Entity:        "registre_raw",
		QueryNorm:     fullNorm,
		QueryTokens:   normalizeTokens(name),
		NameNorm:      normalizeSearchText(p.Nom.String),
		SurnameNorm:   normalizeSearchText(strings.TrimSpace(strings.Join([]string{p.Cognom1.String, p.Cognom2.String}, " "))),
		NameTokens:    nameTokens,
		SurnameTokens: surnameTokens,
		Page:          1,
		PageSize:      cfg.MaxCandidates,
	}

	year := espaiPersonaYear(p)
	if year > 0 {
		from := year - 5
		if from < 0 {
			from = 0
		}
		filter.AnyFrom = from
		filter.AnyTo = year + 5
	}

	rows, _, _, err := a.DB.SearchDocs(filter)
	if err != nil {
		return 0, err
	}

	newMatches := 0
	for _, row := range rows {
		if row.EntityType != "registre_raw" {
			continue
		}
		if existing, err := a.DB.GetEspaiCoincidenciaByTarget(ownerID, p.ID, "registre_raw", row.EntityID); err == nil && existing != nil {
			continue
		}

		persones, _ := a.DB.ListTranscripcioPersones(row.EntityID)
		nameScore := tokenMatchRatio(nameTokens, tokensFromNorm(row.PersonTokensNorm))
		surnameScore := tokenMatchRatio(surnameTokens, tokensFromNorm(row.CognomsTokensNorm+" "+row.CognomsCanon))
		dateScore := dateMatchScore(year, row)
		placeScore := placeMatchScore(a, p, row, munCache)
		relScore := relationsMatchScore(rel, persones)

		totalScore, reason := buildMatchReason(cfg, nameScore, surnameScore, dateScore, placeScore, relScore)
		if totalScore < cfg.MinScore {
			continue
		}
		reasonJSON := ""
		if b, err := json.Marshal(reason); err == nil {
			reasonJSON = string(b)
		}
		match := &db.EspaiCoincidencia{
			OwnerUserID: ownerID,
			ArbreID:     p.ArbreID,
			PersonaID:   p.ID,
			TargetType:  "registre_raw",
			TargetID:    row.EntityID,
			Score:       sql.NullFloat64{Float64: float64(totalScore) / 100.0, Valid: true},
			ReasonJSON:  sql.NullString{String: reasonJSON, Valid: reasonJSON != ""},
			Status:      "pending",
		}
		if _, err := a.DB.CreateEspaiCoincidencia(match); err == nil {
			newMatches++
		}
	}
	return newMatches, nil
}

func buildMatchReason(cfg espaiMatchConfig, nameScore, surnameScore, dateScore, placeScore, relScore float64) (int, espaiMatchReason) {
	totalWeight := cfg.WeightName + cfg.WeightSurname + cfg.WeightDate + cfg.WeightPlace + cfg.WeightRel
	if totalWeight <= 0 {
		totalWeight = 1
	}
	score := (nameScore*float64(cfg.WeightName) +
		surnameScore*float64(cfg.WeightSurname) +
		dateScore*float64(cfg.WeightDate) +
		placeScore*float64(cfg.WeightPlace) +
		relScore*float64(cfg.WeightRel)) / float64(totalWeight)
	totalScore := int(math.Round(score * 100))

	reason := espaiMatchReason{
		Total: totalScore,
		Items: []espaiMatchReasonItem{
			{Key: "name", Score: int(math.Round(nameScore * 100)), Weight: cfg.WeightName},
			{Key: "surname", Score: int(math.Round(surnameScore * 100)), Weight: cfg.WeightSurname},
			{Key: "date", Score: int(math.Round(dateScore * 100)), Weight: cfg.WeightDate},
			{Key: "place", Score: int(math.Round(placeScore * 100)), Weight: cfg.WeightPlace},
			{Key: "relations", Score: int(math.Round(relScore * 100)), Weight: cfg.WeightRel},
		},
	}
	return totalScore, reason
}

func tokenMatchRatio(tokens []string, target map[string]struct{}) float64 {
	if len(tokens) == 0 || len(target) == 0 {
		return 0
	}
	match := 0
	for _, token := range tokens {
		if _, ok := target[token]; ok {
			match++
		}
	}
	return float64(match) / float64(len(tokens))
}

func tokensFromNorm(val string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, token := range strings.Fields(normalizeSearchText(val)) {
		out[token] = struct{}{}
	}
	return out
}

func dateMatchScore(year int, row db.SearchDocRow) float64 {
	if year <= 0 {
		return 0
	}
	targetYear := 0
	if row.AnyActe.Valid {
		targetYear = int(row.AnyActe.Int64)
	} else if row.DataActe.Valid {
		targetYear = extractYear(row.DataActe.String)
	}
	if targetYear <= 0 {
		return 0
	}
	diff := int(math.Abs(float64(year - targetYear)))
	switch {
	case diff == 0:
		return 1
	case diff <= 1:
		return 0.8
	case diff <= 2:
		return 0.6
	case diff <= 5:
		return 0.4
	case diff <= 10:
		return 0.2
	default:
		return 0
	}
}

func placeMatchScore(a *App, p db.EspaiPersona, row db.SearchDocRow, munCache map[int]string) float64 {
	loc := strings.TrimSpace(p.LlocNaixement.String)
	if loc == "" {
		loc = strings.TrimSpace(p.LlocDefuncio.String)
	}
	if loc == "" || !row.MunicipiID.Valid {
		return 0
	}
	munID := int(row.MunicipiID.Int64)
	munName := munCache[munID]
	if munName == "" {
		if mun, err := a.DB.GetMunicipi(munID); err == nil && mun != nil {
			munName = mun.Nom
			munCache[munID] = munName
		}
	}
	if munName == "" {
		return 0
	}
	locNorm := normalizeSearchText(loc)
	munNorm := normalizeSearchText(munName)
	if strings.Contains(locNorm, munNorm) || strings.Contains(munNorm, locNorm) {
		return 1
	}
	return 0
}

func relationsMatchScore(rel espaiRelationInfo, persones []db.TranscripcioPersonaRaw) float64 {
	total := len(rel.Fathers) + len(rel.Mothers)
	if total == 0 {
		return 0
	}
	fatherTokens := extractRoleTokens(persones, []string{"pare", "parenovi", "parenovia"})
	motherTokens := extractRoleTokens(persones, []string{"mare", "marenovi", "marenovia"})

	matches := 0
	for _, name := range rel.Fathers {
		if matchTokens(normalizeTokens(name), fatherTokens) {
			matches++
		}
	}
	for _, name := range rel.Mothers {
		if matchTokens(normalizeTokens(name), motherTokens) {
			matches++
		}
	}
	return float64(matches) / float64(total)
}

func extractRoleTokens(persones []db.TranscripcioPersonaRaw, roles []string) [][]string {
	roleSet := map[string]struct{}{}
	for _, r := range roles {
		roleSet[r] = struct{}{}
	}
	out := [][]string{}
	for _, p := range persones {
		role := normalizeRole(p.Rol)
		if _, ok := roleSet[role]; !ok {
			continue
		}
		name := strings.TrimSpace(strings.Join([]string{p.Nom, p.Cognom1, p.Cognom2}, " "))
		if name == "" {
			continue
		}
		out = append(out, normalizeTokens(name))
	}
	return out
}

func matchTokens(tokens []string, candidates [][]string) bool {
	if len(tokens) == 0 {
		return false
	}
	for _, cand := range candidates {
		if len(cand) == 0 {
			continue
		}
		match := 0
		candSet := map[string]struct{}{}
		for _, t := range cand {
			candSet[t] = struct{}{}
		}
		for _, t := range tokens {
			if _, ok := candSet[t]; ok {
				match++
			}
		}
		if float64(match)/float64(len(tokens)) >= 0.6 {
			return true
		}
	}
	return false
}

func espaiPersonaDisplayName(p db.EspaiPersona) string {
	parts := []string{}
	if p.Nom.Valid {
		parts = append(parts, strings.TrimSpace(p.Nom.String))
	}
	if p.Cognom1.Valid {
		parts = append(parts, strings.TrimSpace(p.Cognom1.String))
	}
	if p.Cognom2.Valid {
		parts = append(parts, strings.TrimSpace(p.Cognom2.String))
	}
	name := strings.TrimSpace(strings.Join(parts, " "))
	if name == "" && p.NomComplet.Valid {
		name = strings.TrimSpace(p.NomComplet.String)
	}
	if name == "" {
		return "-"
	}
	return name
}

func espaiPersonaYear(p db.EspaiPersona) int {
	if p.DataNaixement.Valid {
		if year := extractYear(p.DataNaixement.String); year > 0 {
			return year
		}
	}
	if p.DataDefuncio.Valid {
		if year := extractYear(p.DataDefuncio.String); year > 0 {
			return year
		}
	}
	return 0
}

var yearRegex = regexp.MustCompile(`\b(\d{4})\b`)

func extractYear(val string) int {
	val = strings.TrimSpace(val)
	if val == "" {
		return 0
	}
	match := yearRegex.FindStringSubmatch(val)
	if len(match) < 2 {
		return 0
	}
	year, _ := strconv.Atoi(match[1])
	return year
}

func buildEspaiRelationIndex(persones []db.EspaiPersona, relacions []db.EspaiRelacio) map[int]espaiRelationInfo {
	nameByID := map[int]string{}
	for _, p := range persones {
		nameByID[p.ID] = espaiPersonaDisplayName(p)
	}
	out := map[int]espaiRelationInfo{}
	for _, r := range relacions {
		info := out[r.PersonaID]
		name := strings.TrimSpace(nameByID[r.RelatedPersonaID])
		if name == "" || name == "-" {
			out[r.PersonaID] = info
			continue
		}
		switch r.RelationType {
		case "father":
			info.Fathers = append(info.Fathers, name)
		case "mother":
			info.Mothers = append(info.Mothers, name)
		}
		out[r.PersonaID] = info
	}
	return out
}

func buildRegistreMeta(a *App, reg *db.TranscripcioRaw, llibreCache map[int]*db.Llibre, municipiCache map[int]*db.Municipi, lang string) string {
	if reg == nil {
		return ""
	}
	parts := []string{}
	if reg.TipusActe != "" {
		parts = append(parts, T(lang, "records.type."+reg.TipusActe))
	}
	if reg.AnyDoc.Valid {
		parts = append(parts, strconv.FormatInt(reg.AnyDoc.Int64, 10))
	} else if reg.DataActeISO.Valid {
		parts = append(parts, strings.TrimSpace(reg.DataActeISO.String))
	}
	llibre := llibreCache[reg.LlibreID]
	if llibre == nil {
		if got, err := a.loadLlibreForRegistre(reg); err == nil && got != nil {
			llibre = got
			llibreCache[reg.LlibreID] = got
		}
	}
	if llibre != nil {
		title := strings.TrimSpace(llibre.Titol)
		if title == "" {
			title = strings.TrimSpace(llibre.NomEsglesia)
		}
		if title != "" {
			parts = append(parts, title)
		}
		munID := llibre.MunicipiID
		if munID > 0 {
			mun := municipiCache[munID]
			if mun == nil {
				if got, err := a.DB.GetMunicipi(munID); err == nil && got != nil {
					mun = got
					municipiCache[munID] = got
				}
			}
			if mun != nil && strings.TrimSpace(mun.Nom) != "" {
				parts = append(parts, strings.TrimSpace(mun.Nom))
			}
		}
	}
	return strings.Join(parts, " · ")
}

func mapDecisionToStatus(decision string) string {
	switch decision {
	case "accept":
		return "accepted"
	case "ignore":
		return "ignored"
	case "reject":
		return "rejected"
	case "undo":
		return "pending"
	default:
		return ""
	}
}

func parseMatchIDs(r *http.Request) []int {
	ids := []int{}
	for _, raw := range r.Form["match_id"] {
		if id := parseFormInt(raw); id > 0 {
			ids = append(ids, id)
		}
	}
	if raw := strings.TrimSpace(r.FormValue("match_ids")); raw != "" {
		for _, part := range strings.Split(raw, ",") {
			if id := parseFormInt(strings.TrimSpace(part)); id > 0 {
				ids = append(ids, id)
			}
		}
	}
	if len(ids) == 0 {
		return ids
	}
	sort.Ints(ids)
	out := []int{}
	last := 0
	for _, id := range ids {
		if id == last {
			continue
		}
		out = append(out, id)
		last = id
	}
	return out
}

func getEspaiPersonaCached(a *App, cache map[int]*db.EspaiPersona, id int) *db.EspaiPersona {
	if cached, ok := cache[id]; ok {
		return cached
	}
	p, err := a.DB.GetEspaiPersona(id)
	if err != nil {
		cache[id] = nil
		return nil
	}
	cache[id] = p
	return p
}

func getRegistreCached(a *App, cache map[int]*db.TranscripcioRaw, id int) *db.TranscripcioRaw {
	if cached, ok := cache[id]; ok {
		return cached
	}
	reg, err := a.DB.GetTranscripcioRaw(id)
	if err != nil {
		cache[id] = nil
		return nil
	}
	cache[id] = reg
	return reg
}

func getRegistrePersonesCached(a *App, cache map[int][]db.TranscripcioPersonaRaw, id int) []db.TranscripcioPersonaRaw {
	if cached, ok := cache[id]; ok {
		return cached
	}
	persones, _ := a.DB.ListTranscripcioPersones(id)
	cache[id] = persones
	return persones
}
