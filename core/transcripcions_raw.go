package core

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"unicode"

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

func isQualityField(f indexerField) bool {
	if f.Key == "qualitat_general" || f.RawField == "data_acte_estat" {
		return true
	}
	if f.Target == "person" && strings.HasSuffix(f.PersonField, "_estat") {
		return true
	}
	return false
}

func registreFieldQuality(f indexerField, raw db.TranscripcioRaw, persones []db.TranscripcioPersonaRaw, atributs map[string]db.TranscripcioAtributRaw, cache map[string]*db.TranscripcioPersonaRaw) string {
	if f.Target == "person" && f.PersonField != "" && !strings.HasSuffix(f.PersonField, "_estat") {
		qualField := f
		qualField.PersonField = f.PersonField + "_estat"
		return registreCellValue(qualField, raw, persones, atributs, cache)
	}
	if f.Target == "attr" && f.AttrKey != "" {
		if attr, ok := atributs[f.AttrKey]; ok {
			return attr.Estat
		}
	}
	return ""
}

type registreTableColumn struct {
	Key        string
	Label      string
	Filterable bool
	Options    map[string]string
	Field      indexerField
	IsStatus   bool
	IsActions  bool
}

type registreTableRow struct {
	ID                 int
	ModeracioEstat     string
	Cells              map[string]interface{}
	RawValues          map[string]interface{}
	Qualitats          map[string]interface{}
	MarkType           string
	MarkPublic         bool
	MarkOwn            bool
	LinkPersonID       int
	LinkPersonName     string
	LinkPersonMunicipi string
	LinkPersonAny      string
	ConvertPerson      int
	HasPeople          bool
	PrimaryRole        string
}

func buildRegistreTableColumns(lang string, cfg indexerConfig) []registreTableColumn {
	cols := make([]registreTableColumn, 0, len(cfg.Fields)+2)
	for _, f := range cfg.Fields {
		if isQualityField(f) {
			continue
		}
		optLabels := map[string]string{}
		for _, opt := range f.Options {
			optLabels[opt.Value] = opt.Label
		}
		cols = append(cols, registreTableColumn{
			Key:        f.Key,
			Label:      f.Label,
			Filterable: true,
			Options:    optLabels,
			Field:      f,
		})
	}
	cols = append(cols, registreTableColumn{
		Key:        "_status",
		Label:      T(lang, "records.table.status"),
		Filterable: true,
		IsStatus:   true,
	})
	cols = append(cols, registreTableColumn{
		Key:       "_actions",
		Label:     T(lang, "records.table.actions"),
		IsActions: true,
	})
	return cols
}

type registreColumnMeta struct {
	Key         string          `json:"key"`
	Input       string          `json:"input"`
	Target      string          `json:"target"`
	RawField    string          `json:"raw_field,omitempty"`
	AttrKey     string          `json:"attr_key,omitempty"`
	AttrType    string          `json:"attr_type,omitempty"`
	PersonKey   string          `json:"person_key,omitempty"`
	Role        string          `json:"role,omitempty"`
	PersonField string          `json:"person_field,omitempty"`
	Options     []indexerOption `json:"options,omitempty"`
}

func findIndexerField(cfg indexerConfig, key string) *indexerField {
	for i := range cfg.Fields {
		if cfg.Fields[i].Key == key {
			return &cfg.Fields[i]
		}
	}
	return nil
}

func findPersonForFieldMutable(persones []db.TranscripcioPersonaRaw, role, key string) *db.TranscripcioPersonaRaw {
	if role == "" {
		return nil
	}
	idx := personKeyIndex(key) - 1
	list := make([]*db.TranscripcioPersonaRaw, 0)
	for i := range persones {
		if persones[i].Rol == role {
			list = append(list, &persones[i])
		}
	}
	if len(list) == 0 {
		return nil
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].ID < list[j].ID
	})
	if idx < 0 || idx >= len(list) {
		return nil
	}
	return list[idx]
}

func clearRawField(raw *db.TranscripcioRaw, field string) {
	switch field {
	case "num_pagina_text":
		raw.NumPaginaText = ""
	case "posicio_pagina":
		raw.PosicioPagina = sql.NullInt64{}
	case "any_doc":
		raw.AnyDoc = sql.NullInt64{}
	case "data_acte_iso":
		raw.DataActeISO = sql.NullString{}
		raw.DataActeText = ""
	case "data_acte_estat":
		raw.DataActeEstat = ""
	case "notes_marginals":
		raw.NotesMarginals = ""
	case "observacions_paleografiques":
		raw.ObservacionsPaleografiques = ""
	}
}

func personKeyIndex(key string) int {
	i := len(key)
	for i > 0 && unicode.IsDigit(rune(key[i-1])) {
		i--
	}
	if i == len(key) {
		return 1
	}
	if idx, err := strconv.Atoi(key[i:]); err == nil && idx > 0 {
		return idx
	}
	return 1
}

func personForField(persones []db.TranscripcioPersonaRaw, role, key string, cache map[string]*db.TranscripcioPersonaRaw) *db.TranscripcioPersonaRaw {
	if cached, ok := cache[key]; ok {
		return cached
	}
	if role == "" {
		return nil
	}
	roleBuckets := map[string][]db.TranscripcioPersonaRaw{}
	for _, p := range persones {
		roleBuckets[p.Rol] = append(roleBuckets[p.Rol], p)
	}
	for r := range roleBuckets {
		sort.Slice(roleBuckets[r], func(i, j int) bool {
			return roleBuckets[r][i].ID < roleBuckets[r][j].ID
		})
	}
	list := roleBuckets[role]
	if len(list) == 0 {
		return nil
	}
	idx := personKeyIndex(key)
	if idx > len(list) {
		idx = 1
	}
	cache[key] = &list[idx-1]
	return cache[key]
}

func rawFieldValue(r db.TranscripcioRaw, field string) string {
	switch field {
	case "num_pagina_text":
		return r.NumPaginaText
	case "posicio_pagina":
		if r.PosicioPagina.Valid {
			return strconv.FormatInt(r.PosicioPagina.Int64, 10)
		}
	case "any_doc":
		if r.AnyDoc.Valid {
			return strconv.FormatInt(r.AnyDoc.Int64, 10)
		}
	case "data_acte_iso":
		if r.DataActeISO.Valid {
			return formatDateDisplay(r.DataActeISO.String)
		}
		return r.DataActeText
	case "data_acte_estat":
		return r.DataActeEstat
	case "notes_marginals":
		return r.NotesMarginals
	case "observacions_paleografiques":
		return r.ObservacionsPaleografiques
	}
	return ""
}

func attrValueStringRaw(a db.TranscripcioAtributRaw) string {
	if a.ValorDate.Valid {
		return formatDateDisplay(a.ValorDate.String)
	}
	if a.ValorInt.Valid {
		return strconv.FormatInt(a.ValorInt.Int64, 10)
	}
	if a.ValorBool.Valid {
		if a.ValorBool.Bool {
			return "1"
		}
		return "0"
	}
	return a.ValorText
}

func personFieldValue(p *db.TranscripcioPersonaRaw, field string) string {
	if p == nil {
		return ""
	}
	switch field {
	case "nom":
		return p.Nom
	case "nom_estat":
		return p.NomEstat
	case "cognom1":
		return p.Cognom1
	case "cognom1_estat":
		return p.Cognom1Estat
	case "cognom2":
		return p.Cognom2
	case "cognom2_estat":
		return p.Cognom2Estat
	case "sexe":
		return p.Sexe
	case "sexe_estat":
		return p.SexeEstat
	case "edat":
		return p.EdatText
	case "edat_estat":
		return p.EdatEstat
	case "estat_civil":
		return p.EstatCivilText
	case "estat_civil_estat":
		return p.EstatCivilEstat
	case "municipi":
		return p.MunicipiText
	case "municipi_estat":
		return p.MunicipiEstat
	case "ofici":
		return p.OficiText
	case "ofici_estat":
		return p.OficiEstat
	case "casa":
		return p.CasaNom
	case "casa_estat":
		return p.CasaEstat
	case "notes":
		return p.Notes
	}
	return ""
}

func registreCellValue(f indexerField, raw db.TranscripcioRaw, persones []db.TranscripcioPersonaRaw, atributs map[string]db.TranscripcioAtributRaw, cache map[string]*db.TranscripcioPersonaRaw) string {
	switch f.Target {
	case "raw":
		return rawFieldValue(raw, f.RawField)
	case "attr":
		if attr, ok := atributs[f.AttrKey]; ok {
			return attrValueStringRaw(attr)
		}
		return ""
	case "person":
		person := personForField(persones, f.Role, f.PersonKey, cache)
		return personFieldValue(person, f.PersonField)
	default:
		return ""
	}
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

func cloneValues(v url.Values) url.Values {
	out := url.Values{}
	for key, vals := range v {
		copied := make([]string, len(vals))
		copy(copied, vals)
		out[key] = copied
	}
	return out
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

func primaryPersonForTipus(tipus string, persones []db.TranscripcioPersonaRaw) *db.TranscripcioPersonaRaw {
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
		for i := range persones {
			if normalizeRole(persones[i].Rol) == role {
				return &persones[i]
			}
		}
	}
	for i := range persones {
		if persones[i].Nom != "" || persones[i].Cognom1 != "" || persones[i].Cognom2 != "" {
			return &persones[i]
		}
	}
	return nil
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

func attrValueByKeysRaw(attrs []db.TranscripcioAtributRaw, keys ...string) string {
	for _, key := range keys {
		k := normalizeRole(key)
		for _, a := range attrs {
			if normalizeRole(a.Clau) == k {
				return attrValueStringRaw(a)
			}
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
	cfg := buildIndexerConfig(lang, llibre)
	columns := buildRegistreTableColumns(lang, cfg)
	page := 1
	perPage := 25
	if val := strings.TrimSpace(r.URL.Query().Get("page")); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			page = n
		}
	}
	if val := strings.TrimSpace(r.URL.Query().Get("per_page")); val != "" {
		if n, err := strconv.Atoi(val); err == nil {
			switch n {
			case 10, 25, 50, 100:
				perPage = n
			}
		}
	}
	filterValues := map[string]string{}
	filterMatch := map[string]string{}
	for _, col := range columns {
		if !col.Filterable {
			continue
		}
		paramKey := "f_" + col.Key
		if val := strings.TrimSpace(r.URL.Query().Get(paramKey)); val != "" {
			filterValues[col.Key] = val
			filterMatch[col.Key] = strings.ToLower(val)
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
		for _, col := range columns {
			if _, ok := filterMatch[col.Key]; ok {
				filterOrder = append(filterOrder, col.Key)
			}
		}
	} else {
		seen := map[string]bool{}
		for _, key := range filterOrder {
			seen[key] = true
		}
		for _, col := range columns {
			if _, ok := filterMatch[col.Key]; ok && !seen[col.Key] {
				filterOrder = append(filterOrder, col.Key)
			}
		}
	}
	filtered := len(filterMatch) > 0
	total := 0
	pageRegistres := []db.TranscripcioRaw{}
	if !filtered {
		total, _ = a.DB.CountTranscripcionsRaw(llibreID, db.TranscripcioFilter{})
		totalPages := (total + perPage - 1) / perPage
		if totalPages < 1 {
			totalPages = 1
		}
		if page > totalPages {
			page = totalPages
		}
		offset := (page - 1) * perPage
		registres, _ := a.DB.ListTranscripcionsRaw(llibreID, db.TranscripcioFilter{
			Limit:  perPage,
			Offset: offset,
		})
		pageRegistres = registres
	} else {
		colByKey := map[string]registreTableColumn{}
		needsPeople := false
		needsAttrs := false
		for _, col := range columns {
			colByKey[col.Key] = col
		}
		for key := range filterMatch {
			if col, ok := colByKey[key]; ok {
				if col.IsStatus {
					continue
				}
				switch col.Field.Target {
				case "person":
					needsPeople = true
				case "attr":
					needsAttrs = true
				}
			}
		}
		registres, _ := a.DB.ListTranscripcionsRaw(llibreID, db.TranscripcioFilter{Limit: -1})
		start := (page - 1) * perPage
		end := start + perPage
		for _, reg := range registres {
			var persones []db.TranscripcioPersonaRaw
			atributs := map[string]db.TranscripcioAtributRaw{}
			if needsPeople {
				persones, _ = a.DB.ListTranscripcioPersones(reg.ID)
			}
			if needsAttrs {
				atributsList, _ := a.DB.ListTranscripcioAtributs(reg.ID)
				for _, attr := range atributsList {
					if _, ok := atributs[attr.Clau]; ok {
						continue
					}
					atributs[attr.Clau] = attr
				}
			}
			cache := map[string]*db.TranscripcioPersonaRaw{}
			match := true
			for _, key := range filterOrder {
				filterVal := filterMatch[key]
				if filterVal == "" {
					continue
				}
				col, ok := colByKey[key]
				if !ok {
					continue
				}
				val := ""
				switch {
				case col.IsStatus:
					val = T(lang, "activity.status."+reg.ModeracioEstat)
				case col.Field.Target == "raw":
					val = rawFieldValue(reg, col.Field.RawField)
				default:
					val = registreCellValue(col.Field, reg, persones, atributs, cache)
				}
				if val != "" && len(col.Options) > 0 {
					if label, ok := col.Options[val]; ok {
						val = label
					}
				}
				if !strings.Contains(strings.ToLower(val), filterVal) {
					match = false
					break
				}
			}
			if !match {
				continue
			}
			if total >= start && total < end {
				pageRegistres = append(pageRegistres, reg)
			}
			total++
		}
		if total == 0 {
			page = 1
		} else {
			totalPages := (total + perPage - 1) / perPage
			if page > totalPages {
				page = totalPages
			}
		}
	}
	marksByReg := map[int]db.TranscripcioRawMark{}
	if len(pageRegistres) > 0 {
		ids := make([]int, 0, len(pageRegistres))
		for _, reg := range pageRegistres {
			ids = append(ids, reg.ID)
		}
		if marks, err := a.DB.ListTranscripcioMarks(ids); err == nil {
			for _, mark := range marks {
				if mark.UserID == user.ID {
					marksByReg[mark.TranscripcioID] = mark
				}
			}
		} else {
			Errorf("Error carregant marques de registre: %v", err)
		}
	}
	rows := make([]registreTableRow, 0, len(pageRegistres))
	for _, reg := range pageRegistres {
		persones, _ := a.DB.ListTranscripcioPersones(reg.ID)
		atributsList, _ := a.DB.ListTranscripcioAtributs(reg.ID)
		atributs := map[string]db.TranscripcioAtributRaw{}
		for _, attr := range atributsList {
			if _, ok := atributs[attr.Clau]; ok {
				continue
			}
			atributs[attr.Clau] = attr
		}
		cache := map[string]*db.TranscripcioPersonaRaw{}
		cells := map[string]interface{}{}
		rawValues := map[string]interface{}{}
		qualitats := map[string]interface{}{}
		for _, col := range columns {
			if col.IsStatus || col.IsActions {
				continue
			}
			rawVal := registreCellValue(col.Field, reg, persones, atributs, cache)
			val := rawVal
			if val != "" && len(col.Options) > 0 {
				if label, ok := col.Options[val]; ok {
					val = label
				}
			}
			cells[col.Key] = val
			rawValues[col.Key] = rawVal
			qualitat := registreFieldQuality(col.Field, reg, persones, atributs, cache)
			if qualitat != "" {
				qualitats[col.Key] = qualitat
			}
		}
		hasPeople := len(persones) > 0
		linkPersonID := 0
		linkPersonName := ""
		linkPersonMunicipi := ""
		linkPersonAny := ""
		primaryRole := ""
		var primaryPerson *db.TranscripcioPersonaRaw
		if primary := primaryPersonForTipus(reg.TipusActe, persones); primary != nil {
			primaryPerson = primary
			primaryRole = primary.Rol
		}
		var displayPerson *db.TranscripcioPersonaRaw
		if primaryPerson != nil {
			displayPerson = primaryPerson
		}
		if displayPerson == nil {
			for i := range persones {
				p := &persones[i]
				if p.Nom == "" && p.Cognom1 == "" && p.Cognom2 == "" {
					continue
				}
				displayPerson = p
				break
			}
		}
		if displayPerson != nil {
			linkPersonID = displayPerson.ID
			linkPersonName = personDisplayName(*displayPerson)
			linkPersonMunicipi = displayPerson.MunicipiText
		}
		if reg.AnyDoc.Valid {
			linkPersonAny = strconv.Itoa(int(reg.AnyDoc.Int64))
		}
		convertPersonID := 0
		if primaryPerson != nil && !primaryPerson.PersonaID.Valid {
			convertPersonID = primaryPerson.ID
		}
		markType := ""
		markPublic := false
		markOwn := false
		if mark, ok := marksByReg[reg.ID]; ok {
			markType = mark.Tipus
			markPublic = mark.IsPublic
			markOwn = true
		}
		rows = append(rows, registreTableRow{
			ID:                 reg.ID,
			ModeracioEstat:     reg.ModeracioEstat,
			Cells:              cells,
			RawValues:          rawValues,
			Qualitats:          qualitats,
			MarkType:           markType,
			MarkPublic:         markPublic,
			MarkOwn:            markOwn,
			LinkPersonID:       linkPersonID,
			LinkPersonName:     linkPersonName,
			LinkPersonMunicipi: linkPersonMunicipi,
			LinkPersonAny:      linkPersonAny,
			ConvertPerson:      convertPersonID,
			HasPeople:          hasPeople,
			PrimaryRole:        primaryRole,
		})
	}
	municipiNom := ""
	if llibre.MunicipiID != 0 {
		if mun, err := a.DB.GetMunicipi(llibre.MunicipiID); err == nil && mun != nil {
			municipiNom = mun.Nom
		}
	}
	totalPages := (total + perPage - 1) / perPage
	if totalPages < 1 {
		totalPages = 1
	}
	pageLinks := make([]map[string]interface{}, 0, totalPages+4)
	queryBase := url.Values{}
	queryBase.Set("per_page", strconv.Itoa(perPage))
	for key, val := range filterValues {
		queryBase.Set("f_"+key, val)
	}
	if len(filterOrder) > 0 {
		queryBase.Set("order", strings.Join(filterOrder, ","))
	}
	addPageLink := func(label string, target int, current bool, isNav bool) {
		q := cloneValues(queryBase)
		q.Set("page", strconv.Itoa(target))
		pageLinks = append(pageLinks, map[string]interface{}{
			"Label":   label,
			"URL":     "/documentals/llibres/" + strconv.Itoa(llibreID) + "/registres?" + q.Encode(),
			"Current": current,
			"IsNav":   isNav,
		})
	}
	if page > 1 {
		addPageLink("<<", 1, false, true)
		addPageLink("<", page-1, false, true)
	}
	windowSize := 10
	start := 1
	end := totalPages
	if totalPages > windowSize {
		half := windowSize / 2
		start = page - half
		if start < 1 {
			start = 1
		}
		end = start + windowSize - 1
		if end > totalPages {
			end = totalPages
			start = end - windowSize + 1
		}
	}
	for i := start; i <= end; i++ {
		addPageLink(strconv.Itoa(i), i, i == page, false)
	}
	if page < totalPages {
		addPageLink(">", page+1, false, true)
		addPageLink(">>", totalPages, false, true)
	}
	filterValuesTemplate := map[string]interface{}{}
	for key, val := range filterValues {
		filterValuesTemplate[key] = val
	}
	columnsMeta := map[string]registreColumnMeta{}
	for _, col := range columns {
		if col.IsStatus || col.IsActions {
			continue
		}
		field := col.Field
		columnsMeta[col.Key] = registreColumnMeta{
			Key:         col.Key,
			Input:       field.Input,
			Target:      field.Target,
			RawField:    field.RawField,
			AttrKey:     field.AttrKey,
			AttrType:    field.AttrType,
			PersonKey:   field.PersonKey,
			Role:        field.Role,
			PersonField: field.PersonField,
			Options:     field.Options,
		}
	}
	statusLabels := map[string]string{
		"pendent":  T(lang, "activity.status.pendent"),
		"publicat": T(lang, "activity.status.publicat"),
		"rebutjat": T(lang, "activity.status.rebutjat"),
	}
	RenderPrivateTemplate(w, r, "admin-llibres-registres-list.html", map[string]interface{}{
		"Llibre":            llibre,
		"Columns":           columns,
		"Rows":              rows,
		"MunicipiNom":       municipiNom,
		"Page":              page,
		"PerPage":           perPage,
		"Total":             total,
		"PageLinks":         pageLinks,
		"FilterValues":      filterValuesTemplate,
		"FilterOrder":       strings.Join(filterOrder, ","),
		"ColumnsMeta":       columnsMeta,
		"StatusLabels":      statusLabels,
		"User":              user,
		"CanManageArxius":   canManageArxius,
		"CanManagePolicies": canManagePolicies,
		"CanModerate":       canModerate,
	})
}

func (a *App) AdminSearchPersonesJSON(w http.ResponseWriter, r *http.Request) {
	_, _, ok := a.requirePermission(w, r, permArxius)
	if !ok {
		return
	}
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	municipi := strings.TrimSpace(r.URL.Query().Get("municipi"))
	anyMin, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("any_min")))
	anyMax, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("any_max")))
	filter := db.PersonaSearchFilter{
		Query:    query,
		Municipi: municipi,
		AnyMin:   anyMin,
		AnyMax:   anyMax,
		Limit:    25,
	}
	if query != "" {
		cand := extractSurnameCandidate(query)
		if len(cand) >= 3 {
			if cognomID, _, ok, err := a.DB.ResolveCognomPublicatByForma(cand); err == nil && ok {
				if forms, err := a.DB.ListCognomFormesPublicades(cognomID); err == nil && len(forms) > 0 {
					filter.UseCognomDictionary = true
					filter.ExpandedCognoms = forms
				}
			}
		}
	}
	results, err := a.DB.SearchPersones(filter)
	if err != nil {
		http.Error(w, "No s'han pogut cercar persones", http.StatusInternalServerError)
		return
	}
	payload := make([]personaLinkSuggestion, 0, len(results))
	for _, res := range results {
		payload = append(payload, personaLinkSuggestion{
			ID:       res.ID,
			Nom:      personaSearchName(res),
			Municipi: res.Municipi,
			Any:      personaSearchYear(res),
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

type rawPersonLinkItem struct {
	ID          int    `json:"ID"`
	Rol         string `json:"Rol"`
	Nom         string `json:"Nom"`
	Cognom1     string `json:"Cognom1"`
	Cognom2     string `json:"Cognom2"`
	PersonaID   int    `json:"PersonaID"`
	DisplayName string `json:"DisplayName"`
}

func (a *App) AdminListRegistrePersonesJSON(w http.ResponseWriter, r *http.Request) {
	_, _, ok := a.requirePermission(w, r, permArxius)
	if !ok {
		return
	}
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	registreID := extractID(r.URL.Path)
	if registreID == 0 {
		http.NotFound(w, r)
		return
	}
	registre, err := a.DB.GetTranscripcioRaw(registreID)
	if err != nil || registre == nil {
		http.NotFound(w, r)
		return
	}
	persones, err := a.DB.ListTranscripcioPersones(registreID)
	if err != nil {
		http.Error(w, "No s'han pogut carregar persones", http.StatusInternalServerError)
		return
	}
	primaryRole := ""
	if primary := primaryPersonForTipus(registre.TipusActe, persones); primary != nil {
		primaryRole = primary.Rol
	}
	items := make([]rawPersonLinkItem, 0, len(persones))
	for _, p := range persones {
		personaID := 0
		if p.PersonaID.Valid {
			personaID = int(p.PersonaID.Int64)
		}
		items = append(items, rawPersonLinkItem{
			ID:          p.ID,
			Rol:         p.Rol,
			Nom:         p.Nom,
			Cognom1:     p.Cognom1,
			Cognom2:     p.Cognom2,
			PersonaID:   personaID,
			DisplayName: personDisplayName(p),
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"primary_role": primaryRole,
		"people":       items,
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
	_, _ = a.recalcLlibreIndexacioStats(llibreID)
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
	beforePersones, _ := a.DB.ListTranscripcioPersones(id)
	beforeAtributs, _ := a.DB.ListTranscripcioAtributs(id)
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
	meta := map[string]interface{}{
		"before": map[string]interface{}{
			"raw":      existing,
			"persones": beforePersones,
			"atributs": beforeAtributs,
		},
		"after": map[string]interface{}{
			"raw":      registre,
			"persones": persones,
			"atributs": atributs,
		},
	}
	metaJSON, _ := json.Marshal(meta)
	_, _ = a.DB.CreateTranscripcioRawChange(&db.TranscripcioRawChange{
		TranscripcioID: id,
		ChangeType:     "form",
		FieldKey:       "bulk",
		OldValue:       "",
		NewValue:       "",
		Metadata:       string(metaJSON),
		ChangedBy:      sqlNullIntFromInt(user.ID),
	})
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, "", "editar_registre", "registre", &id, "pendent", nil, "")
	_, _ = a.recalcLlibreIndexacioStats(existing.LlibreID)
	http.Redirect(w, r, returnURL, http.StatusSeeOther)
}

type inlineRegistreUpdatePayload struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (a *App) AdminInlineUpdateRegistreField(w http.ResponseWriter, r *http.Request) {
	user, _, ok := a.requirePermission(w, r, permArxius)
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	csrfToken := r.Header.Get("X-CSRF-Token")
	if csrfToken == "" {
		csrfToken = r.FormValue("csrf_token")
	}
	if !validateCSRF(r, csrfToken) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	var payload inlineRegistreUpdatePayload
	if strings.Contains(r.Header.Get("Content-Type"), "application/json") {
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "Payload invàlid", http.StatusBadRequest)
			return
		}
	} else {
		payload.Key = r.FormValue("key")
		payload.Value = r.FormValue("value")
	}
	payload.Key = strings.TrimSpace(payload.Key)
	if payload.Key == "" {
		http.Error(w, "Camp invàlid", http.StatusBadRequest)
		return
	}
	registreID := extractID(r.URL.Path)
	if registreID == 0 {
		http.NotFound(w, r)
		return
	}
	registre, err := a.DB.GetTranscripcioRaw(registreID)
	if err != nil || registre == nil {
		http.NotFound(w, r)
		return
	}
	llibre, _ := a.DB.GetLlibre(registre.LlibreID)
	cfg := buildIndexerConfig(ResolveLang(r), llibre)
	field := findIndexerField(cfg, payload.Key)
	if field == nil {
		http.Error(w, "Camp desconegut", http.StatusBadRequest)
		return
	}
	persones, _ := a.DB.ListTranscripcioPersones(registreID)
	atributsList, _ := a.DB.ListTranscripcioAtributs(registreID)
	attrMap := map[string]*db.TranscripcioAtributRaw{}
	for i := range atributsList {
		attrMap[atributsList[i].Clau] = &atributsList[i]
	}
	oldAttrs := map[string]db.TranscripcioAtributRaw{}
	for key, attr := range attrMap {
		if attr == nil {
			continue
		}
		oldAttrs[key] = *attr
	}
	oldCache := map[string]*db.TranscripcioPersonaRaw{}
	oldValue := registreCellValue(*field, *registre, persones, oldAttrs, oldCache)
	val := strings.TrimSpace(payload.Value)
	switch field.Target {
	case "raw":
		if val == "" {
			clearRawField(registre, field.RawField)
		} else {
			applyRawField(registre, field.RawField, val)
		}
	case "attr":
		attr := attrMap[field.AttrKey]
		if attr == nil {
			attr = &db.TranscripcioAtributRaw{Clau: field.AttrKey, TipusValor: field.AttrType}
			attrMap[field.AttrKey] = attr
		}
		if val == "" {
			attr.ValorText = ""
			attr.ValorInt = sql.NullInt64{}
			attr.ValorDate = sql.NullString{}
			attr.ValorBool = sql.NullBool{}
		} else {
			if attr.TipusValor == "" {
				attr.TipusValor = field.AttrType
			}
			applyAttrValue(attr, val)
		}
	case "person":
		person := findPersonForFieldMutable(persones, field.Role, field.PersonKey)
		if person == nil {
			persones = append(persones, db.TranscripcioPersonaRaw{Rol: field.Role})
			person = &persones[len(persones)-1]
		}
		applyPersonField(person, field.PersonField, val)
	default:
		http.Error(w, "Camp invàlid", http.StatusBadRequest)
		return
	}
	registre.ModeracioEstat = "pendent"
	registre.ModeratedBy = sql.NullInt64{}
	registre.ModeratedAt = sql.NullTime{}
	registre.ModeracioMotiu = ""
	if err := a.DB.UpdateTranscripcioRaw(registre); err != nil {
		http.Error(w, "No s'ha pogut actualitzar el registre", http.StatusInternalServerError)
		return
	}
	if field.Target == "person" {
		_ = a.DB.DeleteTranscripcioPersones(registreID)
		for i := range persones {
			persones[i].TranscripcioID = registreID
			if isEmptyPerson(&persones[i]) {
				continue
			}
			_, _ = a.DB.CreateTranscripcioPersona(&persones[i])
		}
	}
	if field.Target == "attr" {
		_ = a.DB.DeleteTranscripcioAtributs(registreID)
		for _, attr := range attrMap {
			attr.TranscripcioID = registreID
			if isEmptyAttr(attr) {
				continue
			}
			_, _ = a.DB.CreateTranscripcioAtribut(attr)
		}
	}
	newAttrs := map[string]db.TranscripcioAtributRaw{}
	for key, attr := range attrMap {
		if attr == nil {
			continue
		}
		newAttrs[key] = *attr
	}
	newCache := map[string]*db.TranscripcioPersonaRaw{}
	newValue := registreCellValue(*field, *registre, persones, newAttrs, newCache)
	if oldValue != newValue {
		changeMeta := map[string]interface{}{
			"target":       field.Target,
			"raw_field":    field.RawField,
			"attr_key":     field.AttrKey,
			"attr_type":    field.AttrType,
			"role":         field.Role,
			"person_field": field.PersonField,
			"person_key":   field.PersonKey,
		}
		metaJSON, _ := json.Marshal(changeMeta)
		_, _ = a.DB.CreateTranscripcioRawChange(&db.TranscripcioRawChange{
			TranscripcioID: registreID,
			ChangeType:     "inline",
			FieldKey:       payload.Key,
			OldValue:       oldValue,
			NewValue:       newValue,
			Metadata:       string(metaJSON),
			ChangedBy:      sqlNullIntFromInt(user.ID),
		})
		_, _ = a.RegisterUserActivity(r.Context(), user.ID, "", "editar_registre", "registre", &registreID, "pendent", nil, payload.Key)
		_, _ = a.recalcLlibreIndexacioStats(registre.LlibreID)
	}
	displayAttrs := map[string]db.TranscripcioAtributRaw{}
	for _, attr := range attrMap {
		displayAttrs[attr.Clau] = *attr
	}
	cache := map[string]*db.TranscripcioPersonaRaw{}
	display := registreCellValue(*field, *registre, persones, displayAttrs, cache)
	for _, opt := range field.Options {
		if opt.Value == display {
			display = opt.Label
			break
		}
	}
	writeJSON(w, map[string]interface{}{
		"ok":     true,
		"value":  display,
		"raw":    val,
		"status": registre.ModeracioEstat,
		"field":  payload.Key,
		"user":   user.ID,
	})
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
	_, _ = a.recalcLlibreIndexacioStats(registre.LlibreID)
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

func isValidRegistreMarkType(t string) bool {
	switch t {
	case "consanguini", "politic", "interes":
		return true
	default:
		return false
	}
}

func (a *App) AdminSetRegistreMark(w http.ResponseWriter, r *http.Request) {
	user, _, ok := a.requirePermission(w, r, permArxius)
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.Header.Get("X-CSRF-Token")) && !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	registreID := extractID(r.URL.Path)
	if registreID == 0 {
		http.NotFound(w, r)
		return
	}
	tipus := strings.TrimSpace(r.FormValue("type"))
	if !isValidRegistreMarkType(tipus) {
		http.Error(w, "Tipus invàlid", http.StatusBadRequest)
		return
	}
	publicVal := strings.TrimSpace(r.FormValue("public"))
	isPublic := true
	if publicVal != "" {
		lower := strings.ToLower(publicVal)
		isPublic = lower == "1" || lower == "true" || lower == "yes" || lower == "si" || lower == "on"
	}
	mark := db.TranscripcioRawMark{
		TranscripcioID: registreID,
		UserID:         user.ID,
		Tipus:          tipus,
		IsPublic:       isPublic,
	}
	if err := a.DB.UpsertTranscripcioMark(&mark); err != nil {
		Errorf("Error desant marca de registre: %v", err)
		http.Error(w, "No s'ha pogut desar la marca", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{
		"ok":        true,
		"type":      tipus,
		"is_public": isPublic,
		"own":       true,
	})
}

func (a *App) AdminClearRegistreMark(w http.ResponseWriter, r *http.Request) {
	user, _, ok := a.requirePermission(w, r, permArxius)
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.Header.Get("X-CSRF-Token")) && !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	registreID := extractID(r.URL.Path)
	if registreID == 0 {
		http.NotFound(w, r)
		return
	}
	if err := a.DB.DeleteTranscripcioMark(registreID, user.ID); err != nil {
		Errorf("Error eliminant marca de registre: %v", err)
		http.Error(w, "No s'ha pogut eliminar la marca", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{
		"ok":        true,
		"type":      "",
		"is_public": false,
		"own":       false,
	})
}

func (a *App) AdminConvertRegistreToPersona(w http.ResponseWriter, r *http.Request) {
	user, _, ok := a.requirePermission(w, r, permCreatePerson)
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
	registreID := extractID(r.URL.Path)
	if registreID == 0 {
		http.NotFound(w, r)
		return
	}
	registre, err := a.DB.GetTranscripcioRaw(registreID)
	if err != nil || registre == nil {
		http.NotFound(w, r)
		return
	}
	persones, _ := a.DB.ListTranscripcioPersones(registreID)
	atributs, _ := a.DB.ListTranscripcioAtributs(registreID)
	rawPersonID, _ := strconv.Atoi(strings.TrimSpace(r.FormValue("raw_person_id")))
	var target *db.TranscripcioPersonaRaw
	if rawPersonID != 0 {
		for i := range persones {
			if persones[i].ID == rawPersonID {
				target = &persones[i]
				break
			}
		}
	}
	if target == nil {
		target = primaryPersonForTipus(registre.TipusActe, persones)
	}
	if target == nil {
		http.Error(w, "No s'ha trobat cap persona per convertir", http.StatusBadRequest)
		return
	}
	persona := db.Persona{
		Nom:            target.Nom,
		Cognom1:        target.Cognom1,
		Cognom2:        target.Cognom2,
		Municipi:       target.MunicipiText,
		Ofici:          target.OficiText,
		EstatCivil:     target.EstatCivilText,
		ModeracioEstat: "pendent",
		CreatedBy:      sqlNullIntFromInt(user.ID),
		UpdatedBy:      sqlNullIntFromInt(user.ID),
	}
	if val := attrValueByKeysRaw(atributs, "data_naixement", "datanaixement", "naixement"); val != "" {
		persona.DataNaixement = parseNullString(val)
	}
	if val := attrValueByKeysRaw(atributs, "data_bateig", "databateig", "data_baptisme", "databaptisme"); val != "" {
		persona.DataBateig = parseNullString(val)
	}
	if val := attrValueByKeysRaw(atributs, "data_defuncio", "datadefuncio", "defuncio"); val != "" {
		persona.DataDefuncio = parseNullString(val)
	}
	personaID, err := a.DB.CreatePersona(&persona)
	if err != nil {
		http.Error(w, "No s'ha pogut crear la persona", http.StatusInternalServerError)
		return
	}
	_ = a.DB.LinkTranscripcioPersona(target.ID, personaID, user.ID)
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, "persona_create", "crear", "persona", &personaID, "pendent", nil, "convertida_des_de_registre")
	returnURL := strings.TrimSpace(r.FormValue("return_to"))
	if returnURL == "" {
		returnURL = "/documentals/llibres/" + strconv.Itoa(registre.LlibreID) + "/registres"
	}
	sep := "?"
	if strings.Contains(returnURL, "?") {
		sep = "&"
	}
	http.Redirect(w, r, returnURL+sep+"converted=1", http.StatusSeeOther)
}
