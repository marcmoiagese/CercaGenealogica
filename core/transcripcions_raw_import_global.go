package core

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"database/sql"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func (a *App) AdminImportRegistresGlobalView(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresImportCSV, PermissionTarget{})
	if !ok {
		return
	}
	perms := a.getPermissionsForUser(user.ID)
	imported, _ := strconv.Atoi(r.URL.Query().Get("imported"))
	updated, _ := strconv.Atoi(r.URL.Query().Get("updated"))
	failed, _ := strconv.Atoi(r.URL.Query().Get("failed"))
	errorsToken := strings.TrimSpace(r.URL.Query().Get("errors_token"))
	municipis, _ := a.DB.ListMunicipis(db.MunicipiFilter{})
	arxius, _ := a.DB.ListArxius(db.ArxiuFilter{})
	templates, _ := a.DB.ListCSVImportTemplates(db.CSVImportTemplateFilter{
		OwnerUserID:   user.ID,
		IncludePublic: true,
		Limit:         200,
	})
	RenderPrivateTemplate(w, r, "admin-llibres-registres-import-global.html", map[string]interface{}{
		"Imported":          imported,
		"Updated":           updated,
		"Failed":            failed,
		"ErrorsToken":       errorsToken,
		"Municipis":         municipis,
		"Arxius":            arxius,
		"ImportTemplates":   templates,
		"User":              user,
		"CanManageArxius":   a.hasPerm(perms, permArxius),
		"CanManagePolicies": a.hasPerm(perms, permPolicies),
		"CanModerate":       a.hasPerm(perms, permModerate),
	})
}

func (a *App) AdminImportRegistresGlobal(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresImportCSV, PermissionTarget{})
	if !ok {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "invalid csrf", http.StatusBadRequest)
		return
	}
	file, _, err := r.FormFile("csv_file")
	if err != nil {
		token := storeImportErrors([]importErrorEntry{{Row: 0, Reason: "fitxer CSV no vàlid"}})
		http.Redirect(w, r, "/documentals/llibres/importar?imported=0&failed=1&errors_token="+token, http.StatusSeeOther)
		return
	}
	defer file.Close()
	model := strings.TrimSpace(r.FormValue("model"))
	if model == "" {
		model = "generic"
	}
	separator := parseCSVSeparator(strings.TrimSpace(r.FormValue("separator")))
	ctx := importContext{
		MunicipiID: parseIntValue(r.FormValue("municipi_id")),
		ArxiuID:    parseIntValue(r.FormValue("arxiu_id")),
	}
	var result csvImportResult
	switch model {
	case "template":
		templateID := parseIntValue(r.FormValue("template_id"))
		template, err := a.DB.GetCSVImportTemplate(templateID)
		perms := a.getPermissionsForUser(user.ID)
		if err != nil || template == nil || !canViewImportTemplate(user, perms, template) {
			result.Failed = 1
			result.Errors = append(result.Errors, importErrorEntry{Row: 0, Reason: "plantilla no trobada"})
			break
		}
		if separator == 0 && strings.TrimSpace(template.DefaultSeparator) != "" {
			separator = parseCSVSeparator(template.DefaultSeparator)
		}
		if separator == 0 {
			separator = ','
		}
		result = a.RunCSVTemplateImport(template, file, separator, user.ID, ctx, 0)
	case "baptismes_marcmoia":
		if separator == 0 {
			separator = ','
		}
		result = a.importBaptismesMarcmoiaCSV(file, separator, user.ID, ctx)
	case "generic":
		if separator == 0 {
			separator = ','
		}
		result = a.importGenericTranscripcionsCSV(file, separator, user.ID, ctx)
	default:
		result.Failed = 1
		result.Errors = append(result.Errors, importErrorEntry{Row: 0, Reason: "model d'importació no suportat"})
	}
	token := storeImportErrors(result.Errors)
	for llibreID := range result.BookIDs {
		_, _ = a.recalcLlibreIndexacioStats(llibreID)
	}
	target := fmt.Sprintf("/documentals/llibres/importar?imported=%d&updated=%d&failed=%d", result.Created, result.Updated, result.Failed)
	if token != "" {
		target += "&errors_token=" + token
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}

func (a *App) importGenericTranscripcionsCSV(reader io.Reader, sep rune, userID int, ctx importContext) csvImportResult {
	result := csvImportResult{}
	csvReader := csv.NewReader(reader)
	csvReader.Comma = sep
	csvReader.TrimLeadingSpace = true
	headers, err := csvReader.Read()
	if err != nil {
		result.Failed = 1
		result.Errors = append(result.Errors, importErrorEntry{Row: 0, Reason: "capçalera CSV invàlida"})
		return result
	}
	columns := make([]csvColumn, len(headers))
	hasLlibreID := false
	for i, h := range headers {
		col := parseCSVHeader(h)
		if col.Kind == "base" && col.Field == "llibre_id" {
			hasLlibreID = true
		}
		columns[i] = col
	}
	if !hasLlibreID {
		result.Failed = 1
		result.Errors = append(result.Errors, importErrorEntry{Row: 0, Reason: "falta la columna llibre_id"})
		return result
	}
	filter := db.LlibreFilter{}
	if ctx.MunicipiID != 0 {
		filter.MunicipiID = ctx.MunicipiID
	}
	if ctx.ArxiuID != 0 {
		filter.ArxiuID = ctx.ArxiuID
	}
	llibres, _ := a.DB.ListLlibres(filter)
	llibreMap := map[int]bool{}
	for _, l := range llibres {
		llibreMap[l.ID] = true
	}
	rowNum := 1
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		rowNum++
		if err != nil {
			result.Failed++
			result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: "error llegint fila"})
			continue
		}
		t := db.TranscripcioRaw{
			ModeracioEstat: "pendent",
			CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
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
					if id, err := strconv.Atoi(val); err == nil {
						t.LlibreID = id
					} else {
						rowErr = "llibre_id invàlid"
					}
				case "pagina_id":
					if id, err := strconv.Atoi(val); err == nil {
						t.PaginaID = sql.NullInt64{Int64: int64(id), Valid: true}
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
				attr := atributs[col.AttrKey]
				if attr == nil {
					attr = &db.TranscripcioAtributRaw{Clau: col.AttrKey, TipusValor: col.AttrType}
					atributs[col.AttrKey] = attr
				}
				if col.AttrType == "estat" {
					attr.Estat = val
				} else {
					attr.TipusValor = col.AttrType
					applyAttrValue(attr, val)
				}
			}
		}
		if rowErr != "" {
			result.Failed++
			result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: rowErr})
			continue
		}
		if t.LlibreID == 0 {
			result.Failed++
			result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: "llibre_id obligatori"})
			continue
		}
		if !llibreMap[t.LlibreID] {
			result.Failed++
			result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: "llibre no trobat: " + strconv.Itoa(t.LlibreID)})
			continue
		}
		if !validTipusActe(t.TipusActe) {
			result.Failed++
			result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: "tipus_acte invàlid"})
			continue
		}
		id, err := a.DB.CreateTranscripcioRaw(&t)
		if err != nil || id == 0 {
			result.Failed++
			result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: "no s'ha pogut crear el registre"})
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
		result.Created++
		result.markBook(t.LlibreID)
	}
	return result
}

func parseIntValue(val string) int {
	val = strings.TrimSpace(val)
	if val == "" {
		return 0
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return 0
	}
	return n
}
