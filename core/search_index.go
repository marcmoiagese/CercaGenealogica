package core

import (
	"database/sql"
	"strconv"
	"strings"
	"unicode"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type SearchIndexScope struct {
	MunicipiID int
	LlibreID   int
}

func (a *App) ensureSearchIndexReady() {
	a.searchIndexOnce.Do(func() {
		if err := a.bootstrapSearchIndex(); err != nil {
			Errorf("SearchIndex bootstrap error: %v", err)
		}
	})
}

func (a *App) bootstrapSearchIndex() error {
	empty, err := a.isSearchDocsEmpty()
	if err != nil {
		return err
	}
	if !empty {
		return nil
	}
	hasData, err := a.hasSearchableData()
	if err != nil {
		return err
	}
	if !hasData {
		return nil
	}
	return a.RebuildSearchIndex(SearchIndexScope{})
}

func (a *App) isSearchDocsEmpty() (bool, error) {
	rows, err := a.DB.Query("SELECT 1 AS one FROM search_docs LIMIT 1")
	if err != nil {
		return false, err
	}
	return len(rows) == 0, nil
}

func (a *App) hasSearchableData() (bool, error) {
	rows, err := a.DB.Query("SELECT 1 AS one FROM persona WHERE estat_civil = 'publicat' LIMIT 1")
	if err == nil && len(rows) > 0 {
		return true, nil
	}
	rows, err = a.DB.Query("SELECT 1 AS one FROM transcripcions_raw WHERE moderation_status = 'publicat' LIMIT 1")
	if err == nil && len(rows) > 0 {
		return true, nil
	}
	rows, err = a.DB.Query("SELECT 1 AS one FROM espai_arbres WHERE visibility = 'public' AND status = 'active' LIMIT 1")
	if err == nil && len(rows) > 0 {
		return true, nil
	}
	rows, err = a.DB.Query("SELECT 1 AS one FROM espai_persones WHERE status = 'active' AND visibility = 'visible' LIMIT 1")
	if err == nil && len(rows) > 0 {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return false, nil
}

func (a *App) RebuildSearchIndex(scope SearchIndexScope) error {
	_ = scope
	if _, err := a.DB.Exec("DELETE FROM search_docs"); err != nil {
		return err
	}
	if err := a.rebuildSearchIndexForAllRegistres(); err != nil {
		return err
	}
	if err := a.rebuildSearchIndexForPersones(); err != nil {
		return err
	}
	if err := a.rebuildSearchIndexForEspaiPersones(); err != nil {
		return err
	}
	return a.rebuildSearchIndexForEspaiArbres()
}

func (a *App) rebuildSearchIndexForPersones() error {
	persones, err := a.DB.ListPersones(db.PersonaFilter{Estat: "publicat", Limit: -1})
	if err != nil {
		return err
	}
	for i := range persones {
		if err := a.upsertSearchDocForPersonaID(persones[i].ID); err != nil {
			Errorf("SearchIndex persona %d: %v", persones[i].ID, err)
		}
	}
	return nil
}

func (a *App) rebuildSearchIndexForAllRegistres() error {
	registres, err := a.DB.ListTranscripcionsRawGlobal(db.TranscripcioFilter{Status: "publicat", Limit: -1})
	if err != nil {
		return err
	}
	for i := range registres {
		if err := a.upsertSearchDocForRegistreID(registres[i].ID); err != nil {
			Errorf("SearchIndex registre %d: %v", registres[i].ID, err)
		}
	}
	return nil
}

func (a *App) rebuildSearchIndexForEspaiArbres() error {
	arbres, err := a.DB.ListEspaiArbresPublic()
	if err != nil {
		return err
	}
	for i := range arbres {
		if err := a.upsertSearchDocForEspaiArbreID(arbres[i].ID); err != nil {
			Errorf("SearchIndex espai arbre %d: %v", arbres[i].ID, err)
		}
	}
	return nil
}

func (a *App) rebuildSearchIndexForEspaiPersones() error {
	rows, err := a.DB.Query("SELECT id FROM espai_persones")
	if err != nil {
		return err
	}
	for _, row := range rows {
		id := rowInt(row, "id")
		if id <= 0 {
			continue
		}
		if err := a.upsertSearchDocForEspaiPersonaID(id); err != nil {
			Errorf("SearchIndex espai persona %d: %v", id, err)
		}
	}
	return nil
}

func (a *App) upsertSearchDocForPersonaID(personaID int) error {
	if personaID <= 0 {
		return nil
	}
	persona, err := a.DB.GetPersona(personaID)
	if err != nil || persona == nil {
		return err
	}
	if persona.ModeracioEstat != "publicat" {
		return a.DB.DeleteSearchDoc("persona", personaID)
	}
	doc := a.buildSearchDocFromPersona(persona)
	return a.DB.UpsertSearchDoc(doc)
}

func (a *App) upsertSearchDocForEspaiPersonaID(personaID int) error {
	if personaID <= 0 {
		return nil
	}
	persona, err := a.DB.GetEspaiPersona(personaID)
	if err != nil || persona == nil {
		return err
	}
	status := strings.TrimSpace(persona.Status)
	if status != "" && status != "active" {
		return a.DB.DeleteSearchDoc("espai_persona", personaID)
	}
	visibility := strings.TrimSpace(persona.Visibility)
	if visibility == "" {
		visibility = "visible"
	}
	if visibility != "visible" {
		return a.DB.DeleteSearchDoc("espai_persona", personaID)
	}
	tree, err := a.DB.GetEspaiArbre(persona.ArbreID)
	if err != nil || tree == nil {
		return a.DB.DeleteSearchDoc("espai_persona", personaID)
	}
	treeStatus := strings.TrimSpace(tree.Status)
	if treeStatus != "" && treeStatus != "active" {
		return a.DB.DeleteSearchDoc("espai_persona", personaID)
	}
	doc := a.buildSearchDocFromEspaiPersona(persona)
	return a.DB.UpsertSearchDoc(doc)
}

func (a *App) upsertSearchDocForRegistreID(registreID int) error {
	if registreID <= 0 {
		return nil
	}
	registre, err := a.DB.GetTranscripcioRaw(registreID)
	if err != nil || registre == nil {
		return err
	}
	if registre.ModeracioEstat != "publicat" {
		return a.DB.DeleteSearchDoc("registre_raw", registreID)
	}
	persones, _ := a.DB.ListTranscripcioPersones(registreID)
	llibre, _ := a.loadLlibreForRegistre(registre)
	arxiuID := 0
	if llibre != nil {
		if arxius, err := a.DB.ListLlibreArxius(llibre.ID); err == nil && len(arxius) > 0 {
			arxiuID = arxius[0].ArxiuID
		}
	}
	doc := a.buildSearchDocFromRegistre(registre, persones, llibre, arxiuID)
	return a.DB.UpsertSearchDoc(doc)
}

func (a *App) upsertSearchDocForEspaiArbreID(arbreID int) error {
	if arbreID <= 0 {
		return nil
	}
	arbre, err := a.DB.GetEspaiArbre(arbreID)
	if err != nil || arbre == nil {
		return err
	}
	if strings.TrimSpace(arbre.Visibility) != "public" || strings.TrimSpace(arbre.Status) != "active" {
		return a.DB.DeleteSearchDoc("espai_arbre", arbreID)
	}
	doc := a.buildSearchDocFromEspaiArbre(arbre)
	return a.DB.UpsertSearchDoc(doc)
}

func (a *App) buildSearchDocFromPersona(p *db.Persona) *db.SearchDoc {
	nom := strings.TrimSpace(p.Nom)
	cognoms := compactStrings([]string{p.Cognom1, p.Cognom2})
	nomNorm := normalizeSearchText(nom)
	cognomsNorm := normalizeSearchText(strings.Join(cognoms, " "))
	fullNorm := strings.TrimSpace(strings.Join([]string{nomNorm, cognomsNorm}, " "))
	personTokens := normalizeTokens(append([]string{nom}, cognoms...)...)
	cognomTokens := normalizeTokens(cognoms...)
	return &db.SearchDoc{
		EntityType:        "persona",
		EntityID:          p.ID,
		Published:         true,
		PersonNomNorm:     nomNorm,
		PersonCognomsNorm: cognomsNorm,
		PersonFullNorm:    fullNorm,
		PersonTokensNorm:  strings.Join(personTokens, " "),
		CognomsTokensNorm: strings.Join(cognomTokens, " "),
		PersonPhonetic:    strings.Join(phoneticTokens(personTokens), " "),
		CognomsPhonetic:   strings.Join(phoneticTokens(cognomTokens), " "),
		CognomsCanon:      strings.Join(a.canonicalizeCognoms(cognoms), " "),
	}
}

func (a *App) buildSearchDocFromEspaiPersona(p *db.EspaiPersona) *db.SearchDoc {
	nom := strings.TrimSpace(p.Nom.String)
	cognoms := compactStrings([]string{p.Cognom1.String, p.Cognom2.String})
	if nom == "" && len(cognoms) == 0 {
		nom = strings.TrimSpace(p.NomComplet.String)
	}
	nomNorm := normalizeSearchText(nom)
	cognomsNorm := normalizeSearchText(strings.Join(cognoms, " "))
	fullNorm := strings.TrimSpace(strings.Join([]string{nomNorm, cognomsNorm}, " "))
	if fullNorm == "" {
		fullNorm = nomNorm
	}
	personTokens := normalizeTokens(append([]string{nom}, cognoms...)...)
	cognomTokens := normalizeTokens(cognoms...)
	return &db.SearchDoc{
		EntityType:        "espai_persona",
		EntityID:          p.ID,
		Published:         true,
		PersonNomNorm:     nomNorm,
		PersonCognomsNorm: cognomsNorm,
		PersonFullNorm:    fullNorm,
		PersonTokensNorm:  strings.Join(personTokens, " "),
		CognomsTokensNorm: strings.Join(cognomTokens, " "),
		PersonPhonetic:    strings.Join(phoneticTokens(personTokens), " "),
		CognomsPhonetic:   strings.Join(phoneticTokens(cognomTokens), " "),
		CognomsCanon:      strings.Join(a.canonicalizeCognoms(cognoms), " "),
	}
}

func (a *App) buildSearchDocFromEspaiArbre(arbre *db.EspaiArbre) *db.SearchDoc {
	name := strings.TrimSpace(arbre.Nom)
	if name == "" {
		name = "Arbre " + strconv.Itoa(arbre.ID)
	}
	nameNorm := normalizeSearchText(name)
	tokens := normalizeTokens(name)
	return &db.SearchDoc{
		EntityType:       "espai_arbre",
		EntityID:         arbre.ID,
		Published:        true,
		PersonNomNorm:    nameNorm,
		PersonFullNorm:   nameNorm,
		PersonTokensNorm: strings.Join(tokens, " "),
		PersonPhonetic:   strings.Join(phoneticTokens(tokens), " "),
	}
}

func (a *App) buildSearchDocFromRegistre(reg *db.TranscripcioRaw, persones []db.TranscripcioPersonaRaw, llibre *db.Llibre, arxiuID int) *db.SearchDoc {
	var noms []string
	var cognoms []string
	for _, p := range persones {
		if strings.TrimSpace(p.Nom) != "" {
			noms = append(noms, p.Nom)
		}
		if strings.TrimSpace(p.Cognom1) != "" {
			cognoms = append(cognoms, p.Cognom1)
		}
		if strings.TrimSpace(p.Cognom2) != "" {
			cognoms = append(cognoms, p.Cognom2)
		}
	}
	nomNorm := normalizeSearchText(strings.Join(noms, " "))
	cognomsNorm := normalizeSearchText(strings.Join(cognoms, " "))
	fullNorm := strings.TrimSpace(strings.Join([]string{nomNorm, cognomsNorm}, " "))
	personTokens := normalizeTokens(append(noms, cognoms...)...)
	cognomTokens := normalizeTokens(cognoms...)
	doc := &db.SearchDoc{
		EntityType:        "registre_raw",
		EntityID:          reg.ID,
		Published:         true,
		PersonNomNorm:     nomNorm,
		PersonCognomsNorm: cognomsNorm,
		PersonFullNorm:    fullNorm,
		PersonTokensNorm:  strings.Join(personTokens, " "),
		CognomsTokensNorm: strings.Join(cognomTokens, " "),
		PersonPhonetic:    strings.Join(phoneticTokens(personTokens), " "),
		CognomsPhonetic:   strings.Join(phoneticTokens(cognomTokens), " "),
		CognomsCanon:      strings.Join(a.canonicalizeCognoms(cognoms), " "),
	}
	if llibre != nil {
		doc.MunicipiID = sqlNullIntFromInt(llibre.MunicipiID)
		doc.LlibreID = sqlNullIntFromInt(llibre.ID)
		doc.EntitatEclesiasticaID = sqlNullIntFromInt(llibre.ArquebisbatID)
	}
	if arxiuID > 0 {
		doc.ArxiuID = sqlNullIntFromInt(arxiuID)
	}
	doc.DataActe = reg.DataActeISO
	if reg.AnyDoc.Valid && reg.AnyDoc.Int64 > 0 {
		doc.AnyActe = reg.AnyDoc
	} else if year := yearFromDateInt(reg.DataActeISO.String); year > 0 {
		doc.AnyActe = sql.NullInt64{Int64: int64(year), Valid: true}
	}
	return doc
}

func (a *App) canonicalizeCognoms(raw []string) []string {
	out := make([]string, 0, len(raw))
	seen := map[string]struct{}{}
	for _, val := range raw {
		val = strings.TrimSpace(val)
		if val == "" {
			continue
		}
		key := normalizeCognomKey(val)
		if key == "" {
			continue
		}
		canon := ""
		if id, err := a.DB.FindCognomIDByKey(key); err == nil && id > 0 {
			if canonID, _, err := a.resolveCognomCanonicalID(id); err == nil && canonID > 0 {
				id = canonID
			}
			if cognom, err := a.DB.GetCognom(id); err == nil && cognom != nil {
				canon = normalizeSearchText(cognom.Forma)
			}
		}
		if canon == "" {
			canon = normalizeSearchText(val)
		}
		if canon == "" {
			continue
		}
		if _, ok := seen[canon]; ok {
			continue
		}
		seen[canon] = struct{}{}
		out = append(out, canon)
	}
	return out
}

func normalizeSearchText(val string) string {
	val = strings.TrimSpace(strings.ToLower(val))
	if val == "" {
		return ""
	}
	val = stripDiacritics(val)
	replacer := strings.NewReplacer(
		"’", " ", "'", " ", "-", " ", ".", " ", ",", " ", ";", " ", ":", " ", "·", " ",
		"(", " ", ")", " ", "[", " ", "]", " ", "{", " ", "}", " ", "/", " ", "\\", " ",
	)
	val = replacer.Replace(val)
	val = strings.Join(strings.Fields(val), " ")
	return val
}

func normalizeCognomKey(val string) string {
	val = strings.TrimSpace(val)
	if val == "" {
		return ""
	}
	val = strings.ToLower(val)
	val = stripDiacritics(val)
	val = strings.ReplaceAll(val, "’", "")
	val = strings.ReplaceAll(val, "'", "")
	val = strings.ReplaceAll(val, "-", " ")
	val = strings.ReplaceAll(val, ".", " ")
	val = strings.ReplaceAll(val, ",", " ")
	val = strings.Join(strings.Fields(val), " ")
	val = strings.ReplaceAll(val, " ", "")
	return strings.ToUpper(val)
}

func normalizeTokens(parts ...string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, part := range parts {
		for _, token := range strings.Fields(normalizeSearchText(part)) {
			if _, ok := seen[token]; ok {
				continue
			}
			seen[token] = struct{}{}
			out = append(out, token)
		}
	}
	return out
}

func phoneticTokens(tokens []string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, token := range tokens {
		code := soundex(token)
		if code == "" {
			continue
		}
		if _, ok := seen[code]; ok {
			continue
		}
		seen[code] = struct{}{}
		out = append(out, code)
	}
	return out
}

func soundex(token string) string {
	token = strings.ToUpper(stripNonLetters(token))
	if token == "" {
		return ""
	}
	first := token[:1]
	var lastDigit string
	var b strings.Builder
	b.WriteString(first)
	for i := 1; i < len(token); i++ {
		d := soundexDigit(token[i])
		if d == "" {
			lastDigit = ""
			continue
		}
		if d == lastDigit {
			continue
		}
		b.WriteString(d)
		lastDigit = d
		if b.Len() >= 4 {
			break
		}
	}
	for b.Len() < 4 {
		b.WriteByte('0')
	}
	return b.String()
}

func soundexDigit(ch byte) string {
	switch ch {
	case 'B', 'F', 'P', 'V':
		return "1"
	case 'C', 'G', 'J', 'K', 'Q', 'S', 'X', 'Z':
		return "2"
	case 'D', 'T':
		return "3"
	case 'L':
		return "4"
	case 'M', 'N':
		return "5"
	case 'R':
		return "6"
	default:
		return ""
	}
}

func stripNonLetters(val string) string {
	var b strings.Builder
	for _, r := range val {
		if unicode.IsLetter(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func yearFromDateInt(val string) int {
	val = strings.TrimSpace(val)
	if len(val) < 4 {
		return 0
	}
	year := 0
	for i := 0; i < 4 && i < len(val); i++ {
		ch := val[i]
		if ch < '0' || ch > '9' {
			return 0
		}
		year = year*10 + int(ch-'0')
	}
	return year
}

func compactStrings(vals []string) []string {
	out := make([]string, 0, len(vals))
	for _, v := range vals {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		out = append(out, v)
	}
	return out
}
