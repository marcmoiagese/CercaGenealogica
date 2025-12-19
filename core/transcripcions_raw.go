package core

import (
	"database/sql"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

var transcripcioTipusActe = []string{
	"baptisme",
	"obit",
	"matrimoni",
	"confirmacio",
	"padro",
	"reclutament",
	"altres",
}

type roleOption struct {
	Value string
	Label string
}

func transcripcioRoleOptions(lang, tipus string, llibre *db.Llibre) []roleOption {
	bookType := normalizeIndexerBookType(tipus)
	if bookType == "altres" && llibre != nil && llibre.TipusLlibre != "" {
		bookType = normalizeIndexerBookType(llibre.TipusLlibre)
	}
	rolesSet := map[string]bool{}
	for _, field := range indexerSchema(bookType) {
		if field.Target == "person" && field.Role != "" {
			rolesSet[field.Role] = true
		}
	}
	roles := make([]string, 0, len(rolesSet))
	for role := range rolesSet {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	options := make([]roleOption, 0, len(roles))
	for _, role := range roles {
		options = append(options, roleOption{
			Value: role,
			Label: T(lang, "records.role."+role),
		})
	}
	return options
}

func ensureRoleOptions(options []roleOption, persones []db.TranscripcioPersonaRaw) []roleOption {
	seen := map[string]bool{}
	for _, opt := range options {
		seen[opt.Value] = true
	}
	for _, p := range persones {
		if p.Rol == "" || seen[p.Rol] {
			continue
		}
		options = append(options, roleOption{
			Value: p.Rol,
			Label: p.Rol,
		})
		seen[p.Rol] = true
	}
	return options
}

var transcripcioQualitat = []string{
	"clar",
	"dubtos",
	"incomplet",
	"illegible",
	"no_consta",
}

func transcripcioQualitatLabels(lang string) map[string]string {
	labels := make(map[string]string, len(transcripcioQualitat))
	for _, opt := range transcripcioQualitat {
		labels[opt] = T(lang, "records.quality."+opt)
	}
	return labels
}

var transcripcioValorTipus = []string{
	"text",
	"int",
	"date",
	"bool",
}

type personaLinkSuggestion struct {
	ID       int
	Nom      string
	Municipi string
	Any      string
}

func personaSearchName(p db.PersonaSearchResult) string {
	return strings.TrimSpace(strings.Join([]string{p.Nom, p.Cognom1, p.Cognom2}, " "))
}

func yearFromDateString(val string) string {
	val = strings.TrimSpace(val)
	if len(val) >= 4 {
		return val[:4]
	}
	return ""
}

func personaSearchYear(p db.PersonaSearchResult) string {
	if p.DataNaixement.Valid {
		if y := yearFromDateString(p.DataNaixement.String); y != "" {
			return y
		}
	}
	if p.DataBateig.Valid {
		if y := yearFromDateString(p.DataBateig.String); y != "" {
			return y
		}
	}
	if p.DataDefuncio.Valid {
		if y := yearFromDateString(p.DataDefuncio.String); y != "" {
			return y
		}
	}
	return ""
}

func parseNullString(val string) sql.NullString {
	val = strings.TrimSpace(val)
	if val == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: val, Valid: true}
}

func normalizeRole(role string) string {
	role = strings.ToLower(strings.TrimSpace(role))
	role = strings.ReplaceAll(role, "_", "")
	role = strings.ReplaceAll(role, " ", "")
	role = strings.ReplaceAll(role, "-", "")
	return role
}

func subjectFromPersons(tipus string, persones []db.TranscripcioPersonaRaw) string {
	tipus = normalizeRole(tipus)
	roles := []string{}
	switch tipus {
	case "baptisme":
		roles = []string{"batejat", "baptizat", "infant"}
	case "obit":
		roles = []string{"difunt", "defunt", "mort"}
	case "matrimoni":
		roles = []string{"marit", "espos", "esposo", "esposa", "nuvi", "novia"}
	case "confirmacio":
		roles = []string{"confirmat", "confirmand", "confirmanda"}
	case "padro":
		roles = []string{"capfamilia", "capdefamilia", "cap"}
	case "reclutament":
		roles = []string{"recluta", "soldat"}
	}
	for _, role := range roles {
		for _, p := range persones {
			if normalizeRole(p.Rol) == role {
				return strings.TrimSpace(strings.Join([]string{p.Nom, p.Cognom1, p.Cognom2}, " "))
			}
		}
	}
	for _, p := range persones {
		name := strings.TrimSpace(strings.Join([]string{p.Nom, p.Cognom1, p.Cognom2}, " "))
		if name != "" {
			return name
		}
	}
	return ""
}

func personDisplayName(p db.TranscripcioPersonaRaw) string {
	return strings.TrimSpace(strings.Join([]string{p.Nom, p.Cognom1, p.Cognom2}, " "))
}

func personRoleMap(persones []db.TranscripcioPersonaRaw) map[string][]string {
	res := make(map[string][]string)
	for _, p := range persones {
		name := personDisplayName(p)
		if name == "" {
			continue
		}
		role := normalizeRole(p.Rol)
		if role == "" {
			continue
		}
		res[role] = append(res[role], name)
	}
	return res
}

func firstNameByRoles(roleMap map[string][]string, roles []string) string {
	for _, role := range roles {
		key := normalizeRole(role)
		if names, ok := roleMap[key]; ok && len(names) > 0 {
			return names[0]
		}
	}
	return ""
}

func namesByRoles(roleMap map[string][]string, roles []string) []string {
	seen := map[string]bool{}
	var res []string
	for _, role := range roles {
		key := normalizeRole(role)
		if names, ok := roleMap[key]; ok {
			for _, name := range names {
				if name == "" || seen[name] {
					continue
				}
				seen[name] = true
				res = append(res, name)
			}
		}
	}
	return res
}

func attrValueString(a db.TranscripcioAtributRaw, lang string) string {
	if a.ValorText != "" {
		return a.ValorText
	}
	if a.ValorInt.Valid {
		return strconv.FormatInt(a.ValorInt.Int64, 10)
	}
	if a.ValorDate.Valid {
		return a.ValorDate.String
	}
	if a.ValorBool.Valid {
		if a.ValorBool.Bool {
			return T(lang, "common.yes")
		}
		return T(lang, "common.no")
	}
	return ""
}

func attrValueMap(attrs []db.TranscripcioAtributRaw, lang string) map[string]string {
	res := make(map[string]string)
	for _, a := range attrs {
		key := normalizeRole(a.Clau)
		if key == "" {
			continue
		}
		if _, ok := res[key]; ok {
			continue
		}
		val := attrValueString(a, lang)
		if val != "" {
			res[key] = val
		}
	}
	return res
}

func attrValueByKeys(attrs map[string]string, keys ...string) string {
	for _, key := range keys {
		k := normalizeRole(key)
		if v, ok := attrs[k]; ok {
			return v
		}
	}
	return ""
}

func registreDetailSummary(lang, tipus string, persones []db.TranscripcioPersonaRaw, atributs []db.TranscripcioAtributRaw) string {
	roleMap := personRoleMap(persones)
	attrMap := attrValueMap(atributs, lang)
	parts := []string{}
	switch normalizeRole(tipus) {
	case "baptisme":
		if v := firstNameByRoles(roleMap, []string{"pare", "paire", "parent"}); v != "" {
			parts = append(parts, T(lang, "records.detail.father")+": "+v)
		}
		if v := firstNameByRoles(roleMap, []string{"mare", "maire"}); v != "" {
			parts = append(parts, T(lang, "records.detail.mother")+": "+v)
		}
		if v := attrValueByKeys(attrMap, "data_naixement", "datanaixement", "naixement"); v != "" {
			parts = append(parts, T(lang, "records.detail.birth")+": "+v)
		}
		if v := attrValueByKeys(attrMap, "data_bateig", "databateig", "data_baptisme", "databaptisme"); v != "" {
			parts = append(parts, T(lang, "records.detail.baptism")+": "+v)
		}
	case "obit":
		if v := attrValueByKeys(attrMap, "edat"); v != "" {
			parts = append(parts, T(lang, "records.detail.age")+": "+v)
		} else if v := firstNameByRoles(roleMap, []string{"difunt", "defunt", "mort"}); v != "" {
			if age := attrValueByKeys(attrMap, "edatdifunt"); age != "" {
				parts = append(parts, T(lang, "records.detail.age")+": "+age)
			}
		}
		if v := firstNameByRoles(roleMap, []string{"parella", "espos", "esposa", "marit", "muller"}); v != "" {
			parts = append(parts, T(lang, "records.detail.spouse")+": "+v)
		}
		if v := attrValueByKeys(attrMap, "causa", "causamort", "causadeath"); v != "" {
			parts = append(parts, T(lang, "records.detail.cause")+": "+v)
		}
	case "matrimoni":
		if v := firstNameByRoles(roleMap, []string{"marit", "nuvi", "espos", "esposo"}); v != "" {
			parts = append(parts, T(lang, "records.detail.groom")+": "+v)
		}
		if v := firstNameByRoles(roleMap, []string{"esposa", "novia", "nuvi", "muller"}); v != "" {
			parts = append(parts, T(lang, "records.detail.bride")+": "+v)
		}
		if witnesses := namesByRoles(roleMap, []string{"testimoni", "testimonis", "testigo", "testigos"}); len(witnesses) > 0 {
			parts = append(parts, T(lang, "records.detail.witnesses")+": "+strings.Join(witnesses, ", "))
		}
	}
	return strings.Join(parts, " · ")
}

func buildPersonDetails(persones []db.TranscripcioPersonaRaw) []personDisplay {
	res := []personDisplay{}
	for _, p := range persones {
		name := personDisplayName(p)
		if name == "" {
			continue
		}
		res = append(res, personDisplay{Rol: p.Rol, Nom: name})
	}
	return res
}

func buildAttrDetails(attrs []db.TranscripcioAtributRaw, lang string) []attrDisplay {
	res := []attrDisplay{}
	for _, a := range attrs {
		val := attrValueString(a, lang)
		if val == "" {
			continue
		}
		res = append(res, attrDisplay{Key: a.Clau, Value: val})
	}
	return res
}

func transcripcioFilterQuery(f db.TranscripcioFilter) string {
	values := url.Values{}
	if strings.TrimSpace(f.TipusActe) != "" {
		values.Set("tipus_acte", strings.TrimSpace(f.TipusActe))
	}
	if f.AnyDoc > 0 {
		values.Set("any_doc", strconv.Itoa(f.AnyDoc))
	}
	if f.PaginaID > 0 {
		values.Set("pagina_id", strconv.Itoa(f.PaginaID))
	}
	if strings.TrimSpace(f.Status) != "" {
		values.Set("status", strings.TrimSpace(f.Status))
	}
	if strings.TrimSpace(f.Qualitat) != "" {
		values.Set("qualitat", strings.TrimSpace(f.Qualitat))
	}
	if strings.TrimSpace(f.Search) != "" {
		values.Set("q", strings.TrimSpace(f.Search))
	}
	if f.UseFullText {
		values.Set("fts", "1")
	}
	if f.Limit > 0 {
		values.Set("limit", strconv.Itoa(f.Limit))
	}
	return values.Encode()
}

func parseTranscripcioFilterFromRequest(r *http.Request, defaultLimit int) (db.TranscripcioFilter, int, int) {
	filter := db.TranscripcioFilter{
		TipusActe: strings.TrimSpace(r.URL.Query().Get("tipus_acte")),
		Status:    strings.TrimSpace(r.URL.Query().Get("status")),
		Qualitat:  strings.TrimSpace(r.URL.Query().Get("qualitat")),
		Search:    strings.TrimSpace(r.URL.Query().Get("q")),
	}
	if v := strings.TrimSpace(r.URL.Query().Get("fts")); v != "" {
		if v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes") {
			filter.UseFullText = true
		}
	}
	if v := strings.TrimSpace(r.URL.Query().Get("any_doc")); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			filter.AnyDoc = n
		}
	}
	if v := strings.TrimSpace(r.URL.Query().Get("pagina_id")); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			filter.PaginaID = n
		}
	}
	if v := strings.TrimSpace(r.URL.Query().Get("llibre_id")); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			filter.LlibreID = n
		}
	}
	limit := defaultLimit
	page := 1
	if v := strings.TrimSpace(r.URL.Query().Get("limit")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if v := strings.TrimSpace(r.URL.Query().Get("page")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			page = n
		}
	}
	filter.Limit = limit
	filter.Offset = (page - 1) * limit
	return filter, page, limit
}

func maxLen(slices ...[]string) int {
	max := 0
	for _, s := range slices {
		if len(s) > max {
			max = len(s)
		}
	}
	return max
}

func sliceValue(s []string, idx int) string {
	if idx >= 0 && idx < len(s) {
		return strings.TrimSpace(s[idx])
	}
	return ""
}

func parseTranscripcioForm(r *http.Request, llibreID int) (*db.TranscripcioRaw, []db.TranscripcioPersonaRaw, []db.TranscripcioAtributRaw) {
	_ = r.ParseForm()
	t := &db.TranscripcioRaw{
		LlibreID:                   llibreID,
		PaginaID:                   sqlNullInt(r.FormValue("pagina_id")),
		NumPaginaText:              strings.TrimSpace(r.FormValue("num_pagina_text")),
		PosicioPagina:              sqlNullInt(r.FormValue("posicio_pagina")),
		TipusActe:                  strings.TrimSpace(r.FormValue("tipus_acte")),
		AnyDoc:                     sqlNullInt(r.FormValue("any_doc")),
		DataActeText:               strings.TrimSpace(r.FormValue("data_acte_text")),
		DataActeISO:                parseNullString(r.FormValue("data_acte_iso")),
		DataActeEstat:              strings.TrimSpace(r.FormValue("data_acte_estat")),
		TranscripcioLiteral:        strings.TrimSpace(r.FormValue("transcripcio_literal")),
		NotesMarginals:             strings.TrimSpace(r.FormValue("notes_marginals")),
		ObservacionsPaleografiques: strings.TrimSpace(r.FormValue("observacions_paleografiques")),
	}

	persones := parseTranscripcioPersones(r)
	atributs := parseTranscripcioAtributs(r)
	return t, persones, atributs
}

func parseTranscripcioPersones(r *http.Request) []db.TranscripcioPersonaRaw {
	roles := r.Form["person_rol"]
	noms := r.Form["person_nom"]
	nomEstats := r.Form["person_nom_estat"]
	c1 := r.Form["person_cognom1"]
	c1Estats := r.Form["person_cognom1_estat"]
	c2 := r.Form["person_cognom2"]
	c2Estats := r.Form["person_cognom2_estat"]
	sexes := r.Form["person_sexe"]
	sexesEstats := r.Form["person_sexe_estat"]
	edats := r.Form["person_edat"]
	edatEstats := r.Form["person_edat_estat"]
	civils := r.Form["person_estat_civil"]
	civilEstats := r.Form["person_estat_civil_estat"]
	muns := r.Form["person_municipi"]
	munEstats := r.Form["person_municipi_estat"]
	oficis := r.Form["person_ofici"]
	oficiEstats := r.Form["person_ofici_estat"]
	cases := r.Form["person_casa"]
	casaEstats := r.Form["person_casa_estat"]
	notes := r.Form["person_notes"]

	max := maxLen(roles, noms, c1, c2, sexes, edats, civils, muns, oficis, cases, notes)
	var res []db.TranscripcioPersonaRaw
	for i := 0; i < max; i++ {
		p := db.TranscripcioPersonaRaw{
			Rol:             sliceValue(roles, i),
			Nom:             sliceValue(noms, i),
			NomEstat:        sliceValue(nomEstats, i),
			Cognom1:         sliceValue(c1, i),
			Cognom1Estat:    sliceValue(c1Estats, i),
			Cognom2:         sliceValue(c2, i),
			Cognom2Estat:    sliceValue(c2Estats, i),
			Sexe:            sliceValue(sexes, i),
			SexeEstat:       sliceValue(sexesEstats, i),
			EdatText:        sliceValue(edats, i),
			EdatEstat:       sliceValue(edatEstats, i),
			EstatCivilText:  sliceValue(civils, i),
			EstatCivilEstat: sliceValue(civilEstats, i),
			MunicipiText:    sliceValue(muns, i),
			MunicipiEstat:   sliceValue(munEstats, i),
			OficiText:       sliceValue(oficis, i),
			OficiEstat:      sliceValue(oficiEstats, i),
			CasaNom:         sliceValue(cases, i),
			CasaEstat:       sliceValue(casaEstats, i),
			Notes:           sliceValue(notes, i),
		}
		if p.Rol == "" && p.Nom == "" && p.Cognom1 == "" && p.Cognom2 == "" && p.Sexe == "" && p.EdatText == "" && p.EstatCivilText == "" && p.MunicipiText == "" && p.OficiText == "" && p.CasaNom == "" && p.Notes == "" {
			continue
		}
		res = append(res, p)
	}
	return res
}

func parseTranscripcioAtributs(r *http.Request) []db.TranscripcioAtributRaw {
	keys := r.Form["attr_key"]
	types := r.Form["attr_type"]
	values := r.Form["attr_value"]
	states := r.Form["attr_state"]
	notes := r.Form["attr_notes"]

	max := maxLen(keys, types, values, states, notes)
	var res []db.TranscripcioAtributRaw
	for i := 0; i < max; i++ {
		key := sliceValue(keys, i)
		val := sliceValue(values, i)
		tip := sliceValue(types, i)
		if tip == "" {
			tip = "text"
		}
		if key == "" && val == "" {
			continue
		}
		a := db.TranscripcioAtributRaw{
			Clau:       key,
			TipusValor: tip,
			Estat:      sliceValue(states, i),
			Notes:      sliceValue(notes, i),
		}
		switch tip {
		case "int":
			if val != "" {
				if n, err := strconv.Atoi(val); err == nil {
					a.ValorInt = sql.NullInt64{Int64: int64(n), Valid: true}
				} else {
					a.ValorText = val
				}
			}
		case "date":
			a.ValorDate = parseNullString(val)
		case "bool":
			l := strings.ToLower(val)
			if l == "1" || l == "true" || l == "si" || l == "yes" || l == "on" {
				a.ValorBool = sql.NullBool{Bool: true, Valid: true}
			} else if l != "" {
				a.ValorBool = sql.NullBool{Bool: false, Valid: true}
			}
		default:
			a.ValorText = val
		}
		res = append(res, a)
	}
	return res
}

func (a *App) AdminListRegistresLlibre(w http.ResponseWriter, r *http.Request) {
	user, perms, ok := a.requirePermission(w, r, permArxius)
	if !ok {
		return
	}
	canManageArxius := a.hasPerm(perms, permArxius)
	canManagePolicies := perms.CanManagePolicies || perms.Admin
	canModerate := perms.CanModerate || perms.Admin
	llibreID := extractID(r.URL.Path)
	if llibreID == 0 {
		http.NotFound(w, r)
		return
	}
	llibre, err := a.DB.GetLlibre(llibreID)
	if err != nil || llibre == nil {
		http.NotFound(w, r)
		return
	}
	lang := ResolveLang(r)
	filter, page, limit := parseTranscripcioFilterFromRequest(r, 25)
	filter.LlibreID = 0
	total, _ := a.DB.CountTranscripcionsRaw(llibreID, filter)
	registres, _ := a.DB.ListTranscripcionsRaw(llibreID, filter)
	pagines, _ := a.DB.ListLlibrePagines(llibreID)
	rows := make([]registreRow, 0, len(registres))
	for _, r := range registres {
		persones, _ := a.DB.ListTranscripcioPersones(r.ID)
		atributs, _ := a.DB.ListTranscripcioAtributs(r.ID)
		subjecte := subjectFromPersons(r.TipusActe, persones)
		if subjecte == "" {
			subjecte = "-"
		}
		detall := registreDetailSummary(lang, r.TipusActe, persones, atributs)
		rows = append(rows, registreRow{
			TranscripcioRaw: r,
			Subjecte:        subjecte,
			Detall:          detall,
			Persones:        buildPersonDetails(persones),
			Atributs:        buildAttrDetails(atributs, lang),
		})
	}
	totalPages := 1
	if limit > 0 {
		totalPages = (total + limit - 1) / limit
		if totalPages < 1 {
			totalPages = 1
		}
	}
	pages := make([]int, 0, totalPages)
	for i := 1; i <= totalPages; i++ {
		pages = append(pages, i)
	}
	filterQuery := transcripcioFilterQuery(filter)
	var importSummary map[string]int
	importErrorToken := strings.TrimSpace(r.URL.Query().Get("errors_token"))
	if v := strings.TrimSpace(r.URL.Query().Get("imported")); v != "" {
		if created, err := strconv.Atoi(v); err == nil {
			failed := 0
			if e := strings.TrimSpace(r.URL.Query().Get("failed")); e != "" {
				if n, err := strconv.Atoi(e); err == nil {
					failed = n
				}
			}
			importSummary = map[string]int{"Created": created, "Failed": failed}
		}
	}
	RenderPrivateTemplate(w, r, "admin-llibres-registres-list.html", map[string]interface{}{
		"Llibre":            llibre,
		"Registres":         rows,
		"Filter":            filter,
		"Pagines":           pagines,
		"TipusActeOptions":  transcripcioTipusActe,
		"QualitatOptions":   transcripcioQualitat,
		"QualitatLabels":    transcripcioQualitatLabels(lang),
		"FilterQuery":       filterQuery,
		"ImportSummary":     importSummary,
		"ImportErrorToken":  importErrorToken,
		"Total":             total,
		"Page":              page,
		"Limit":             limit,
		"Pages":             pages,
		"User":              user,
		"CanManageArxius":   canManageArxius,
		"CanManagePolicies": canManagePolicies,
		"CanModerate":       canModerate,
	})
}

func (a *App) AdminNewRegistre(w http.ResponseWriter, r *http.Request) {
	user, perms, ok := a.requirePermission(w, r, permArxius)
	if !ok {
		return
	}
	canManageArxius := a.hasPerm(perms, permArxius)
	canManagePolicies := perms.CanManagePolicies || perms.Admin
	canModerate := perms.CanModerate || perms.Admin
	lang := ResolveLang(r)
	llibreID := extractID(r.URL.Path)
	if llibreID == 0 {
		http.NotFound(w, r)
		return
	}
	llibre, err := a.DB.GetLlibre(llibreID)
	if err != nil || llibre == nil {
		http.NotFound(w, r)
		return
	}
	pagines, _ := a.DB.ListLlibrePagines(llibreID)
	returnURL := strings.TrimSpace(r.URL.Query().Get("return_to"))
	if returnURL == "" {
		returnURL = "/documentals/llibres/" + strconv.Itoa(llibreID) + "/registres"
	}
	roleOptions := transcripcioRoleOptions(lang, "", llibre)
	RenderPrivateTemplate(w, r, "admin-llibres-registres-form.html", map[string]interface{}{
		"Llibre":            llibre,
		"Registre":          &db.TranscripcioRaw{LlibreID: llibreID},
		"Persones":          []db.TranscripcioPersonaRaw{},
		"Atributs":          []db.TranscripcioAtributRaw{},
		"Pagines":           pagines,
		"IsNew":             true,
		"ReturnURL":         returnURL,
		"TipusActeOptions":  transcripcioTipusActe,
		"QualitatOptions":   transcripcioQualitat,
		"QualitatLabels":    transcripcioQualitatLabels(lang),
		"ValorTipus":        transcripcioValorTipus,
		"RoleOptions":       roleOptions,
		"User":              user,
		"CanManageArxius":   canManageArxius,
		"CanManagePolicies": canManagePolicies,
		"CanModerate":       canModerate,
	})
}

func (a *App) AdminCreateRegistre(w http.ResponseWriter, r *http.Request) {
	user, perms, ok := a.requirePermission(w, r, permArxius)
	if !ok {
		return
	}
	canManageArxius := a.hasPerm(perms, permArxius)
	canManagePolicies := perms.CanManagePolicies || perms.Admin
	canModerate := perms.CanModerate || perms.Admin
	lang := ResolveLang(r)
	llibreID := extractID(r.URL.Path)
	if llibreID == 0 {
		http.NotFound(w, r)
		return
	}
	llibre, err := a.DB.GetLlibre(llibreID)
	if err != nil || llibre == nil {
		http.NotFound(w, r)
		return
	}
	returnURL := strings.TrimSpace(r.FormValue("return_to"))
	if returnURL == "" {
		returnURL = "/documentals/llibres/" + strconv.Itoa(llibreID) + "/registres"
	}
	registre, persones, atributs := parseTranscripcioForm(r, llibreID)
	registre.CreatedBy = sqlNullIntFromInt(user.ID)
	registre.ModeracioEstat = "pendent"
	id, err := a.DB.CreateTranscripcioRaw(registre)
	if err != nil {
		pagines, _ := a.DB.ListLlibrePagines(llibreID)
		roleOptions := ensureRoleOptions(transcripcioRoleOptions(lang, registre.TipusActe, llibre), persones)
		RenderPrivateTemplate(w, r, "admin-llibres-registres-form.html", map[string]interface{}{
			"Llibre":            llibre,
			"Registre":          registre,
			"Persones":          persones,
			"Atributs":          atributs,
			"Pagines":           pagines,
			"IsNew":             true,
			"Error":             "No s'ha pogut crear el registre.",
			"ReturnURL":         returnURL,
			"TipusActeOptions":  transcripcioTipusActe,
			"QualitatOptions":   transcripcioQualitat,
			"QualitatLabels":    transcripcioQualitatLabels(lang),
			"ValorTipus":        transcripcioValorTipus,
			"RoleOptions":       roleOptions,
			"User":              user,
			"CanManageArxius":   canManageArxius,
			"CanManagePolicies": canManagePolicies,
			"CanModerate":       canModerate,
		})
		return
	}
	for i := range persones {
		persones[i].TranscripcioID = id
		_, _ = a.DB.CreateTranscripcioPersona(&persones[i])
	}
	for i := range atributs {
		atributs[i].TranscripcioID = id
		_, _ = a.DB.CreateTranscripcioAtribut(&atributs[i])
	}
	http.Redirect(w, r, returnURL, http.StatusSeeOther)
}

func (a *App) AdminShowRegistre(w http.ResponseWriter, r *http.Request) {
	user, perms, ok := a.requirePermission(w, r, permArxius)
	if !ok {
		return
	}
	canManageArxius := a.hasPerm(perms, permArxius)
	canManagePolicies := perms.CanManagePolicies || perms.Admin
	canModerate := perms.CanModerate || perms.Admin
	lang := ResolveLang(r)
	id := extractID(r.URL.Path)
	registre, err := a.DB.GetTranscripcioRaw(id)
	if err != nil || registre == nil {
		http.NotFound(w, r)
		return
	}
	llibre, _ := a.DB.GetLlibre(registre.LlibreID)
	persones, _ := a.DB.ListTranscripcioPersones(id)
	atributs, _ := a.DB.ListTranscripcioAtributs(id)
	linkedPersones := map[string]*db.Persona{}
	for _, p := range persones {
		if p.PersonaID.Valid {
			key := strconv.FormatInt(p.PersonaID.Int64, 10)
			if _, ok := linkedPersones[key]; ok {
				continue
			}
			if persona, err := a.DB.GetPersona(int(p.PersonaID.Int64)); err == nil && persona != nil {
				linkedPersones[key] = persona
			}
		}
	}
	linkSuggestions := map[string][]personaLinkSuggestion{}
	for _, p := range persones {
		if p.PersonaID.Valid {
			continue
		}
		if p.Nom == "" && p.Cognom1 == "" && p.Cognom2 == "" {
			continue
		}
		filter := db.PersonaSearchFilter{
			Nom:      p.Nom,
			Cognom1:  p.Cognom1,
			Cognom2:  p.Cognom2,
			Municipi: p.MunicipiText,
			Limit:    5,
		}
		if registre.AnyDoc.Valid {
			year := int(registre.AnyDoc.Int64)
			filter.AnyMin = year - 5
			filter.AnyMax = year + 5
		}
		results, _ := a.DB.SearchPersones(filter)
		if len(results) == 0 {
			continue
		}
		key := strconv.Itoa(p.ID)
		for _, res := range results {
			linkSuggestions[key] = append(linkSuggestions[key], personaLinkSuggestion{
				ID:       res.ID,
				Nom:      personaSearchName(res),
				Municipi: res.Municipi,
				Any:      personaSearchYear(res),
			})
		}
	}
	linkTargetID, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("link_raw_id")))
	searchQuery := strings.TrimSpace(r.URL.Query().Get("q"))
	searchMunicipi := strings.TrimSpace(r.URL.Query().Get("municipi"))
	searchAnyMin, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("any_min")))
	searchAnyMax, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("any_max")))
	var searchResults []personaLinkSuggestion
	if linkTargetID > 0 && (searchQuery != "" || searchMunicipi != "" || searchAnyMin > 0 || searchAnyMax > 0) {
		filter := db.PersonaSearchFilter{
			Query:    searchQuery,
			Municipi: searchMunicipi,
			AnyMin:   searchAnyMin,
			AnyMax:   searchAnyMax,
			Limit:    20,
		}
		results, _ := a.DB.SearchPersones(filter)
		for _, res := range results {
			searchResults = append(searchResults, personaLinkSuggestion{
				ID:       res.ID,
				Nom:      personaSearchName(res),
				Municipi: res.Municipi,
				Any:      personaSearchYear(res),
			})
		}
	}
	RenderPrivateTemplate(w, r, "admin-llibres-registres-show.html", map[string]interface{}{
		"Llibre":             llibre,
		"Registre":           registre,
		"Persones":           persones,
		"Atributs":           atributs,
		"User":               user,
		"LinkedPersones":     linkedPersones,
		"LinkSuggestions":    linkSuggestions,
		"LinkSearchTarget":   linkTargetID,
		"LinkSearchResults":  searchResults,
		"LinkSearchQuery":    searchQuery,
		"LinkSearchMunicipi": searchMunicipi,
		"LinkSearchAnyMin":   searchAnyMin,
		"LinkSearchAnyMax":   searchAnyMax,
		"CanLink":            registre.ModeracioEstat != "rebutjat",
		"Lang":               lang,
		"CanManageArxius":    canManageArxius,
		"CanManagePolicies":  canManagePolicies,
		"CanModerate":        canModerate,
	})
}

func (a *App) AdminEditRegistre(w http.ResponseWriter, r *http.Request) {
	user, perms, ok := a.requirePermission(w, r, permArxius)
	if !ok {
		return
	}
	canManageArxius := a.hasPerm(perms, permArxius)
	canManagePolicies := perms.CanManagePolicies || perms.Admin
	canModerate := perms.CanModerate || perms.Admin
	lang := ResolveLang(r)
	id := extractID(r.URL.Path)
	registre, err := a.DB.GetTranscripcioRaw(id)
	if err != nil || registre == nil {
		http.NotFound(w, r)
		return
	}
	llibre, _ := a.DB.GetLlibre(registre.LlibreID)
	persones, _ := a.DB.ListTranscripcioPersones(id)
	atributs, _ := a.DB.ListTranscripcioAtributs(id)
	pagines, _ := a.DB.ListLlibrePagines(registre.LlibreID)
	roleOptions := ensureRoleOptions(transcripcioRoleOptions(lang, registre.TipusActe, llibre), persones)
	returnURL := strings.TrimSpace(r.URL.Query().Get("return_to"))
	if returnURL == "" {
		returnURL = "/documentals/llibres/" + strconv.Itoa(registre.LlibreID) + "/registres"
	}
	RenderPrivateTemplate(w, r, "admin-llibres-registres-form.html", map[string]interface{}{
		"Llibre":            llibre,
		"Registre":          registre,
		"Persones":          persones,
		"Atributs":          atributs,
		"Pagines":           pagines,
		"IsNew":             false,
		"ReturnURL":         returnURL,
		"TipusActeOptions":  transcripcioTipusActe,
		"QualitatOptions":   transcripcioQualitat,
		"QualitatLabels":    transcripcioQualitatLabels(lang),
		"ValorTipus":        transcripcioValorTipus,
		"RoleOptions":       roleOptions,
		"User":              user,
		"CanManageArxius":   canManageArxius,
		"CanManagePolicies": canManagePolicies,
		"CanModerate":       canModerate,
	})
}

func (a *App) AdminUpdateRegistre(w http.ResponseWriter, r *http.Request) {
	user, perms, ok := a.requirePermission(w, r, permArxius)
	if !ok {
		return
	}
	canManageArxius := a.hasPerm(perms, permArxius)
	canManagePolicies := perms.CanManagePolicies || perms.Admin
	canModerate := perms.CanModerate || perms.Admin
	lang := ResolveLang(r)
	id := extractID(r.URL.Path)
	existing, err := a.DB.GetTranscripcioRaw(id)
	if err != nil || existing == nil {
		http.NotFound(w, r)
		return
	}
	returnURL := strings.TrimSpace(r.FormValue("return_to"))
	if returnURL == "" {
		returnURL = "/documentals/llibres/" + strconv.Itoa(existing.LlibreID) + "/registres"
	}
	registre, persones, atributs := parseTranscripcioForm(r, existing.LlibreID)
	registre.ID = id
	registre.ModeracioEstat = "pendent"
	registre.CreatedBy = existing.CreatedBy
	if err := a.DB.UpdateTranscripcioRaw(registre); err != nil {
		llibre, _ := a.DB.GetLlibre(existing.LlibreID)
		pagines, _ := a.DB.ListLlibrePagines(existing.LlibreID)
		roleOptions := ensureRoleOptions(transcripcioRoleOptions(lang, registre.TipusActe, llibre), persones)
		RenderPrivateTemplate(w, r, "admin-llibres-registres-form.html", map[string]interface{}{
			"Llibre":            llibre,
			"Registre":          registre,
			"Persones":          persones,
			"Atributs":          atributs,
			"Pagines":           pagines,
			"IsNew":             false,
			"Error":             "No s'ha pogut actualitzar el registre.",
			"ReturnURL":         returnURL,
			"TipusActeOptions":  transcripcioTipusActe,
			"QualitatOptions":   transcripcioQualitat,
			"QualitatLabels":    transcripcioQualitatLabels(lang),
			"ValorTipus":        transcripcioValorTipus,
			"RoleOptions":       roleOptions,
			"User":              user,
			"CanManageArxius":   canManageArxius,
			"CanManagePolicies": canManagePolicies,
			"CanModerate":       canModerate,
		})
		return
	}
	_ = a.DB.DeleteTranscripcioPersones(id)
	_ = a.DB.DeleteTranscripcioAtributs(id)
	for i := range persones {
		persones[i].TranscripcioID = id
		_, _ = a.DB.CreateTranscripcioPersona(&persones[i])
	}
	for i := range atributs {
		atributs[i].TranscripcioID = id
		_, _ = a.DB.CreateTranscripcioAtribut(&atributs[i])
	}
	http.Redirect(w, r, returnURL, http.StatusSeeOther)
}

func (a *App) AdminDeleteRegistre(w http.ResponseWriter, r *http.Request) {
	_, _, ok := a.requirePermission(w, r, permArxius)
	if !ok {
		return
	}
	id := extractID(r.URL.Path)
	registre, _ := a.DB.GetTranscripcioRaw(id)
	if registre == nil {
		http.NotFound(w, r)
		return
	}
	_ = a.DB.DeleteTranscripcioRaw(id)
	http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(registre.LlibreID)+"/registres", http.StatusSeeOther)
}

func parseRegistrePersonaIDs(path string) (int, int) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	registreID := 0
	personaRawID := 0
	for i := 0; i < len(parts); i++ {
		if parts[i] == "registres" && i+1 < len(parts) {
			registreID, _ = strconv.Atoi(parts[i+1])
		}
		if parts[i] == "persones" && i+1 < len(parts) {
			personaRawID, _ = strconv.Atoi(parts[i+1])
		}
	}
	return registreID, personaRawID
}

func (a *App) AdminLinkPersonaToRaw(w http.ResponseWriter, r *http.Request) {
	user, _, ok := a.requirePermission(w, r, permModerate)
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	registreID, personaRawID := parseRegistrePersonaIDs(r.URL.Path)
	if registreID == 0 || personaRawID == 0 {
		http.NotFound(w, r)
		return
	}
	personaID, _ := strconv.Atoi(strings.TrimSpace(r.FormValue("persona_id")))
	if personaID == 0 {
		http.Error(w, "Persona invàlida", http.StatusBadRequest)
		return
	}
	registre, err := a.DB.GetTranscripcioRaw(registreID)
	if err != nil || registre == nil {
		http.NotFound(w, r)
		return
	}
	if registre.ModeracioEstat == "rebutjat" {
		http.Error(w, "Registre rebutjat", http.StatusBadRequest)
		return
	}
	persones, _ := a.DB.ListTranscripcioPersones(registreID)
	found := false
	for _, p := range persones {
		if p.ID == personaRawID {
			found = true
			break
		}
	}
	if !found {
		http.NotFound(w, r)
		return
	}
	if err := a.DB.LinkTranscripcioPersona(personaRawID, personaID, user.ID); err != nil {
		http.Error(w, "No s'ha pogut enllaçar la persona", http.StatusInternalServerError)
		return
	}
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, "", "enllacar_persona", "transcripcio_persona_raw", &personaRawID, "validat", nil, "")
	returnURL := strings.TrimSpace(r.FormValue("return_to"))
	if returnURL == "" {
		returnURL = "/documentals/registres/" + strconv.Itoa(registreID)
	}
	http.Redirect(w, r, returnURL, http.StatusSeeOther)
}

func (a *App) AdminUnlinkPersonaFromRaw(w http.ResponseWriter, r *http.Request) {
	user, _, ok := a.requirePermission(w, r, permModerate)
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	registreID, personaRawID := parseRegistrePersonaIDs(r.URL.Path)
	if registreID == 0 || personaRawID == 0 {
		http.NotFound(w, r)
		return
	}
	registre, err := a.DB.GetTranscripcioRaw(registreID)
	if err != nil || registre == nil {
		http.NotFound(w, r)
		return
	}
	if err := a.DB.UnlinkTranscripcioPersona(personaRawID, user.ID); err != nil {
		http.Error(w, "No s'ha pogut desenllaçar la persona", http.StatusInternalServerError)
		return
	}
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, "", "desenllacar_persona", "transcripcio_persona_raw", &personaRawID, "validat", nil, "")
	returnURL := strings.TrimSpace(r.FormValue("return_to"))
	if returnURL == "" {
		returnURL = "/documentals/registres/" + strconv.Itoa(registreID)
	}
	http.Redirect(w, r, returnURL, http.StatusSeeOther)
}
