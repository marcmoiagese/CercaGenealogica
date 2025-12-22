package core

import (
	"crypto/rand"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type personDisplay struct {
	Rol string
	Nom string
}

type attrDisplay struct {
	Key   string
	Value string
}

type registreRow struct {
	db.TranscripcioRaw
	Subjecte    string
	Detall      string
	LlibreTitol string
	Persones    []personDisplay
	Atributs    []attrDisplay
}

func parseCSVSeparator(val string) rune {
	if val == ";" {
		return ';'
	}
	return ','
}

func parseIntNull(val string) sql.NullInt64 {
	val = strings.TrimSpace(val)
	if val == "" {
		return sql.NullInt64{}
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(n), Valid: true}
}

func validTipusActe(val string) bool {
	for _, opt := range transcripcioTipusActe {
		if opt == val {
			return true
		}
	}
	return false
}

func buildRegistreRows(a *App, registres []db.TranscripcioRaw) []registreRow {
	rows := make([]registreRow, 0, len(registres))
	for _, r := range registres {
		persones, _ := a.DB.ListTranscripcioPersones(r.ID)
		subjecte := subjectFromPersons(r.TipusActe, persones)
		if subjecte == "" {
			subjecte = "-"
		}
		rows = append(rows, registreRow{TranscripcioRaw: r, Subjecte: subjecte})
	}
	return rows
}

func (a *App) AdminExportRegistresLlibre(w http.ResponseWriter, r *http.Request) {
	user, _, ok := a.requirePermission(w, r, permArxius)
	if !ok {
		return
	}
	llibreID := extractID(r.URL.Path)
	if llibreID == 0 {
		http.NotFound(w, r)
		return
	}
	_ = user
	filter, _, _ := parseTranscripcioFilterFromRequest(r, 25)
	filter.Limit = -1
	filter.Offset = 0
	registres, _ := a.DB.ListTranscripcionsRaw(llibreID, filter)
	separator := parseCSVSeparator(strings.TrimSpace(r.URL.Query().Get("separator")))
	includeLiteral := strings.TrimSpace(r.URL.Query().Get("literal")) != "0"
	writeRegistresCSV(a, w, registres, includeLiteral, separator)
}

func (a *App) AdminExportRegistresGlobal(w http.ResponseWriter, r *http.Request) {
	_, _, ok := a.requirePermission(w, r, permArxius)
	if !ok {
		return
	}
	filter, _, _ := parseTranscripcioFilterFromRequest(r, 25)
	filter.Limit = -1
	filter.Offset = 0
	registres, _ := a.DB.ListTranscripcionsRawGlobal(filter)
	separator := parseCSVSeparator(strings.TrimSpace(r.URL.Query().Get("separator")))
	includeLiteral := strings.TrimSpace(r.URL.Query().Get("literal")) != "0"
	writeRegistresCSV(a, w, registres, includeLiteral, separator)
}

func writeRegistresCSV(a *App, w http.ResponseWriter, registres []db.TranscripcioRaw, includeLiteral bool, sep rune) {
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename=registres.csv")
	writer := csv.NewWriter(w)
	writer.Comma = sep
	header := []string{
		"llibre_id",
		"pagina_id",
		"num_pagina_text",
		"posicio_pagina",
		"tipus_acte",
		"any_doc",
		"data_acte_text",
		"data_acte_iso",
		"data_acte_estat",
		"subjecte",
		"pare",
		"mare",
		"parella",
		"marit",
		"esposa",
		"testimonis",
	}
	if includeLiteral {
		header = append(header, "transcripcio_literal", "notes_marginals", "observacions_paleografiques")
	}
	_ = writer.Write(header)
	for _, r := range registres {
		persones, _ := a.DB.ListTranscripcioPersones(r.ID)
		roleMap := personRoleMap(persones)
		subjecte := subjectFromPersons(r.TipusActe, persones)
		pare := firstNameByRoles(roleMap, []string{"pare", "paire", "parent"})
		mare := firstNameByRoles(roleMap, []string{"mare", "maire"})
		parella := firstNameByRoles(roleMap, []string{"parella", "espos", "esposa", "marit", "muller"})
		marit := firstNameByRoles(roleMap, []string{"marit", "espos", "esposo", "nuvi"})
		esposa := firstNameByRoles(roleMap, []string{"esposa", "novia", "muller"})
		testimonis := strings.Join(namesByRoles(roleMap, []string{"testimoni", "testimonis", "testigo", "testigos"}), "; ")
		row := []string{
			strconv.Itoa(r.LlibreID),
			nullIntToString(r.PaginaID),
			r.NumPaginaText,
			nullIntToString(r.PosicioPagina),
			r.TipusActe,
			nullIntToString(r.AnyDoc),
			r.DataActeText,
			nullStringToString(r.DataActeISO),
			r.DataActeEstat,
			subjecte,
			pare,
			mare,
			parella,
			marit,
			esposa,
			testimonis,
		}
		if includeLiteral {
			row = append(row, r.TranscripcioLiteral, r.NotesMarginals, r.ObservacionsPaleografiques)
		}
		_ = writer.Write(row)
	}
	writer.Flush()
}

func nullIntToString(v sql.NullInt64) string {
	if v.Valid {
		return strconv.FormatInt(v.Int64, 10)
	}
	return ""
}

func nullStringToString(v sql.NullString) string {
	if v.Valid {
		return v.String
	}
	return ""
}

type csvColumn struct {
	Kind     string
	Role     string
	Field    string
	AttrKey  string
	AttrType string
}

var personFields = []string{
	"nom",
	"cognom1",
	"cognom2",
	"sexe",
	"edat",
	"estat_civil",
	"municipi",
	"ofici",
	"casa",
	"notes",
}

type importErrorEntry struct {
	Row    int
	Reason string
	Fields map[string]string
}

type importErrorStore struct {
	sync.Mutex
	data map[string][]importErrorEntry
}

var registreImportErrors = importErrorStore{data: map[string][]importErrorEntry{}}

func randomToken(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	for i := range b {
		b[i] = chars[int(b[i])%len(chars)]
	}
	return string(b)
}

func storeImportErrors(entries []importErrorEntry) string {
	if len(entries) == 0 {
		return ""
	}
	token := randomToken(24)
	registreImportErrors.Lock()
	registreImportErrors.data[token] = entries
	registreImportErrors.Unlock()
	return token
}

func popImportErrors(token string) ([]importErrorEntry, bool) {
	registreImportErrors.Lock()
	entries, ok := registreImportErrors.data[token]
	if ok {
		delete(registreImportErrors.data, token)
	}
	registreImportErrors.Unlock()
	return entries, ok
}

func parseCSVHeader(header string) csvColumn {
	h := strings.ToLower(strings.TrimSpace(header))
	if h == "" {
		return csvColumn{Kind: "skip"}
	}
	baseFields := map[string]string{
		"llibre_id":                   "llibre_id",
		"pagina_id":                   "pagina_id",
		"num_pagina_text":             "num_pagina_text",
		"posicio_pagina":              "posicio_pagina",
		"tipus_acte":                  "tipus_acte",
		"any_doc":                     "any_doc",
		"data_acte_text":              "data_acte_text",
		"data_acte_iso":               "data_acte_iso",
		"data_acte_estat":             "data_acte_estat",
		"transcripcio_literal":        "transcripcio_literal",
		"notes_marginals":             "notes_marginals",
		"observacions_paleografiques": "observacions_paleografiques",
	}
	if field, ok := baseFields[h]; ok {
		return csvColumn{Kind: "base", Field: field}
	}
	if strings.HasPrefix(h, "attr:") {
		rest := strings.TrimPrefix(h, "attr:")
		parts := strings.Split(rest, ":")
		key := parts[0]
		attrType := "text"
		if len(parts) > 1 && parts[1] != "" {
			attrType = parts[1]
		}
		return csvColumn{Kind: "attr", AttrKey: key, AttrType: attrType}
	}
	if strings.HasPrefix(h, "person:") {
		rest := strings.TrimPrefix(h, "person:")
		parts := strings.Split(rest, ":")
		if len(parts) == 2 {
			return csvColumn{Kind: "person", Role: parts[0], Field: parts[1]}
		}
	}
	for _, field := range personFields {
		stateSuffix := "_" + field + "_estat"
		if strings.HasSuffix(h, stateSuffix) {
			role := strings.TrimSuffix(h, stateSuffix)
			if role != "" {
				return csvColumn{Kind: "person", Role: role, Field: field + "_estat"}
			}
		}
		suffix := "_" + field
		if strings.HasSuffix(h, suffix) {
			role := strings.TrimSuffix(h, suffix)
			if role != "" {
				return csvColumn{Kind: "person", Role: role, Field: field}
			}
		}
	}
	return csvColumn{Kind: "skip"}
}

func applyPersonField(p *db.TranscripcioPersonaRaw, field, val string) {
	switch field {
	case "nom":
		p.Nom = val
	case "nom_estat":
		p.NomEstat = val
	case "cognom1":
		p.Cognom1 = val
	case "cognom1_estat":
		p.Cognom1Estat = val
	case "cognom2":
		p.Cognom2 = val
	case "cognom2_estat":
		p.Cognom2Estat = val
	case "sexe":
		p.Sexe = val
	case "sexe_estat":
		p.SexeEstat = val
	case "edat":
		p.EdatText = val
	case "edat_estat":
		p.EdatEstat = val
	case "estat_civil":
		p.EstatCivilText = val
	case "estat_civil_estat":
		p.EstatCivilEstat = val
	case "municipi":
		p.MunicipiText = val
	case "municipi_estat":
		p.MunicipiEstat = val
	case "ofici":
		p.OficiText = val
	case "ofici_estat":
		p.OficiEstat = val
	case "casa":
		p.CasaNom = val
	case "casa_estat":
		p.CasaEstat = val
	case "notes":
		p.Notes = val
	}
}

func applyAttrValue(a *db.TranscripcioAtributRaw, val string) {
	switch a.TipusValor {
	case "int":
		if n, err := strconv.Atoi(val); err == nil {
			a.ValorInt = sql.NullInt64{Int64: int64(n), Valid: true}
		} else {
			a.ValorText = val
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
}

func (a *App) AdminImportRegistresView(w http.ResponseWriter, r *http.Request) {
	user, perms, ok := a.requirePermission(w, r, permArxius)
	if !ok {
		return
	}
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
	RenderPrivateTemplate(w, r, "admin-llibres-registres-import.html", map[string]interface{}{
		"Llibre":            llibre,
		"User":              user,
		"CanManageArxius":   a.hasPerm(perms, permArxius),
		"CanManagePolicies": a.hasPerm(perms, permPolicies),
		"CanModerate":       a.hasPerm(perms, permModerate),
	})
}

func (a *App) AdminImportRegistresLlibre(w http.ResponseWriter, r *http.Request) {
	user, _, ok := a.requirePermission(w, r, permArxius)
	if !ok {
		return
	}
	llibreID := extractID(r.URL.Path)
	if llibreID == 0 {
		http.NotFound(w, r)
		return
	}
	file, _, err := r.FormFile("csv_file")
	if err != nil {
		token := storeImportErrors([]importErrorEntry{{Row: 0, Reason: "fitxer CSV no vàlid"}})
		http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID)+"/indexar?imported=0&failed=1&errors_token="+token, http.StatusSeeOther)
		return
	}
	defer file.Close()
	separator := parseCSVSeparator(strings.TrimSpace(r.FormValue("separator")))
	reader := csv.NewReader(file)
	reader.Comma = separator
	reader.TrimLeadingSpace = true

	headers, err := reader.Read()
	if err != nil {
		token := storeImportErrors([]importErrorEntry{{Row: 0, Reason: "capçalera CSV invàlida"}})
		http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID)+"/indexar?imported=0&failed=1&errors_token="+token, http.StatusSeeOther)
		return
	}
	columns := make([]csvColumn, len(headers))
	for i, h := range headers {
		columns[i] = parseCSVHeader(h)
	}
	pagines, _ := a.DB.ListLlibrePagines(llibreID)
	paginaMap := map[int]bool{}
	for _, p := range pagines {
		paginaMap[p.ID] = true
	}

	created := 0
	failed := 0
	rowNum := 1
	var errors []importErrorEntry
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			rowNum++
			failed++
			errors = append(errors, importErrorEntry{Row: rowNum, Reason: "error llegint fila"})
			continue
		}
		rowNum++
		fail := func(reason string) {
			failed++
			errors = append(errors, importErrorEntry{Row: rowNum, Reason: reason})
		}
		t := db.TranscripcioRaw{
			LlibreID:       llibreID,
			ModeracioEstat: "pendent",
			CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
		}
		persones := map[string]*db.TranscripcioPersonaRaw{}
		atributs := map[string]*db.TranscripcioAtributRaw{}
		rowErr := ""
		for i, val := range record {
			if i >= len(columns) {
				continue
			}
			col := columns[i]
			val = strings.TrimSpace(val)
			if val == "" {
				continue
			}
			switch col.Kind {
			case "base":
				switch col.Field {
				case "llibre_id":
					if id, err := strconv.Atoi(val); err == nil && id != llibreID {
						rowErr = "llibre_id no coincideix"
					}
				case "pagina_id":
					if id, err := strconv.Atoi(val); err == nil {
						if !paginaMap[id] {
							rowErr = "pagina_id no existent"
						} else {
							t.PaginaID = sql.NullInt64{Int64: int64(id), Valid: true}
						}
					}
				case "num_pagina_text":
					t.NumPaginaText = val
				case "posicio_pagina":
					t.PosicioPagina = parseIntNull(val)
				case "tipus_acte":
					t.TipusActe = val
				case "any_doc":
					t.AnyDoc = parseIntNull(val)
				case "data_acte_text":
					t.DataActeText = val
				case "data_acte_iso":
					t.DataActeISO = parseNullString(val)
				case "data_acte_estat":
					t.DataActeEstat = val
				case "transcripcio_literal":
					t.TranscripcioLiteral = val
				case "notes_marginals":
					t.NotesMarginals = val
				case "observacions_paleografiques":
					t.ObservacionsPaleografiques = val
				}
			case "person":
				role := strings.TrimSpace(col.Role)
				if role == "" {
					continue
				}
				p, ok := persones[role]
				if !ok {
					p = &db.TranscripcioPersonaRaw{Rol: role}
					persones[role] = p
				}
				applyPersonField(p, col.Field, val)
			case "attr":
				if col.AttrKey == "" {
					continue
				}
				a := atributs[col.AttrKey]
				if a == nil {
					a = &db.TranscripcioAtributRaw{Clau: col.AttrKey, TipusValor: col.AttrType}
					atributs[col.AttrKey] = a
				}
				if col.AttrType == "estat" {
					a.Estat = val
				} else {
					a.TipusValor = col.AttrType
					applyAttrValue(a, val)
				}
			}
		}
		if rowErr != "" {
			fail(rowErr)
			continue
		}
		if !validTipusActe(t.TipusActe) {
			fail("tipus_acte invàlid")
			continue
		}
		id, err := a.DB.CreateTranscripcioRaw(&t)
		if err != nil || id == 0 {
			fail("no s'ha pogut crear el registre")
			continue
		}
		for _, p := range persones {
			if p.Nom == "" && p.Cognom1 == "" && p.Cognom2 == "" && p.Notes == "" {
				continue
			}
			p.TranscripcioID = id
			_, _ = a.DB.CreateTranscripcioPersona(p)
		}
		for _, attr := range atributs {
			if attr.ValorText == "" && !attr.ValorInt.Valid && !attr.ValorDate.Valid && !attr.ValorBool.Valid && attr.Estat == "" {
				continue
			}
			attr.TranscripcioID = id
			_, _ = a.DB.CreateTranscripcioAtribut(attr)
		}
		created++
	}
	token := storeImportErrors(errors)
	if created > 0 {
		_, _ = a.recalcLlibreIndexacioStats(llibreID)
	}
	target := fmt.Sprintf("/documentals/llibres/%d/indexar?imported=%d&failed=%d", llibreID, created, failed)
	if token != "" {
		target += "&errors_token=" + token
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}

func (a *App) AdminDownloadImportErrors(w http.ResponseWriter, r *http.Request) {
	_, _, ok := a.requirePermission(w, r, permArxius)
	if !ok {
		return
	}
	token := strings.TrimSpace(r.URL.Query().Get("token"))
	if token == "" {
		http.NotFound(w, r)
		return
	}
	entries, ok := popImportErrors(token)
	if !ok || len(entries) == 0 {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename=import-errors.csv")
	writer := csv.NewWriter(w)
	fieldSet := map[string]struct{}{}
	for _, e := range entries {
		for k := range e.Fields {
			fieldSet[k] = struct{}{}
		}
	}
	fieldKeys := make([]string, 0, len(fieldSet))
	for k := range fieldSet {
		fieldKeys = append(fieldKeys, k)
	}
	sort.Strings(fieldKeys)
	header := append([]string{"row", "error"}, fieldKeys...)
	_ = writer.Write(header)
	for _, e := range entries {
		row := []string{strconv.Itoa(e.Row), e.Reason}
		for _, key := range fieldKeys {
			row = append(row, e.Fields[key])
		}
		_ = writer.Write(row)
	}
	writer.Flush()
}

func (a *App) AdminSearchRegistres(w http.ResponseWriter, r *http.Request) {
	user, _, ok := a.requirePermission(w, r, permArxius)
	if !ok {
		return
	}
	lang := ResolveLang(r)
	filter, page, limit := parseTranscripcioFilterFromRequest(r, 25)
	total, _ := a.DB.CountTranscripcionsRawGlobal(filter)
	registres, _ := a.DB.ListTranscripcionsRawGlobal(filter)
	rows := make([]registreRow, 0, len(registres))
	llibreTitles := map[int]string{}
	for _, r := range registres {
		persones, _ := a.DB.ListTranscripcioPersones(r.ID)
		atributs, _ := a.DB.ListTranscripcioAtributs(r.ID)
		subjecte := subjectFromPersons(r.TipusActe, persones)
		if subjecte == "" {
			subjecte = "-"
		}
		detall := registreDetailSummary(lang, r.TipusActe, persones, atributs)
		title, ok := llibreTitles[r.LlibreID]
		if !ok {
			llibre, err := a.DB.GetLlibre(r.LlibreID)
			if err == nil && llibre != nil {
				title = llibre.Titol
				if title == "" {
					title = llibre.NomEsglesia
				}
			}
			if title == "" {
				title = "-"
			}
			llibreTitles[r.LlibreID] = title
		}
		rows = append(rows, registreRow{
			TranscripcioRaw: r,
			Subjecte:        subjecte,
			Detall:          detall,
			LlibreTitol:     title,
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
	llibres, _ := a.DB.ListLlibres(db.LlibreFilter{})
	RenderPrivateTemplate(w, r, "admin-llibres-registres-search.html", map[string]interface{}{
		"Registres":        rows,
		"Filter":           filter,
		"Llibres":          llibres,
		"TipusActeOptions": transcripcioTipusActe,
		"QualitatOptions":  transcripcioQualitat,
		"QualitatLabels":   transcripcioQualitatLabels(lang),
		"FilterQuery":      filterQuery,
		"Total":            total,
		"Page":             page,
		"Limit":            limit,
		"Pages":            pages,
		"User":             user,
	})
}
