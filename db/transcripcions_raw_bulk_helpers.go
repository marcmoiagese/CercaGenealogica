package db

import "strings"

const bulkTranscripcioRawBundleBatchSize = 200
const postgresBulkInsertParamLimit = 65535
const postgresBulkInsertTargetRows = 1000
const postgresBulkInsertTargetRowsRaw = 200

func normalizeTranscripcioRawForInsert(t *TranscripcioRaw) {
	if t == nil {
		return
	}
	if strings.TrimSpace(t.ModeracioEstat) == "" {
		t.ModeracioEstat = "pendent"
	}
	if strings.TrimSpace(t.DataActeEstat) == "" {
		t.DataActeEstat = "clar"
	}
}

func bulkInsertStatementBatchSize(style string, argsPerRow int) int {
	return bulkInsertStatementBatchSizeFor(style, "", argsPerRow)
}

func bulkInsertStatementBatchSizeFor(style, entity string, argsPerRow int) int {
	if strings.ToLower(style) != "postgres" || argsPerRow <= 0 {
		return bulkTranscripcioRawBundleBatchSize
	}
	maxRows := postgresBulkInsertParamLimit / argsPerRow
	if maxRows <= 0 {
		return 1
	}
	targetRows := postgresBulkInsertTargetRows
	if entity == "transcripcions_raw" {
		targetRows = postgresBulkInsertTargetRowsRaw
	}
	if maxRows > targetRows {
		maxRows = targetRows
	}
	if maxRows < bulkTranscripcioRawBundleBatchSize {
		return maxRows
	}
	return maxRows
}

func buildInsertTranscripcioRawQuery(style, nowFun string, includeID, returning bool) string {
	cols := []string{
		"llibre_id",
		"pagina_id",
		"num_pagina_text",
		"posicio_pagina",
		"tipus_acte",
		"any_doc",
		"data_acte_text",
		"data_acte_iso",
		"data_acte_estat",
		"transcripcio_literal",
		"notes_marginals",
		"observacions_paleografiques",
		"moderation_status",
		"moderated_by",
		"moderated_at",
		"moderation_notes",
		"created_by",
		"created_at",
		"updated_at",
	}
	values := []string{
		"?",
		"?",
		"?",
		"?",
		"?",
		"?",
		"?",
		"?",
		"?",
		"?",
		"?",
		"?",
		"?",
		"?",
		"?",
		"?",
		"?",
		nowFun,
		nowFun,
	}
	if includeID {
		cols = append([]string{"id"}, cols...)
		values = append([]string{"?"}, values...)
	}
	query := `
        INSERT INTO transcripcions_raw (
            ` + strings.Join(cols, ", ") + `
        )
        VALUES (` + strings.Join(values, ", ") + `)`
	if returning {
		query += " RETURNING id"
	}
	return formatPlaceholders(style, query)
}

func buildInsertTranscripcioRawArgs(t TranscripcioRaw, explicitID int, includeID bool) []interface{} {
	args := make([]interface{}, 0, 18)
	if includeID {
		args = append(args, explicitID)
	}
	args = append(args,
		t.LlibreID,
		t.PaginaID,
		t.NumPaginaText,
		t.PosicioPagina,
		t.TipusActe,
		t.AnyDoc,
		t.DataActeText,
		t.DataActeISO,
		t.DataActeEstat,
		t.TranscripcioLiteral,
		t.NotesMarginals,
		t.ObservacionsPaleografiques,
		t.ModeracioEstat,
		t.ModeratedBy,
		t.ModeratedAt,
		t.ModeracioMotiu,
		t.CreatedBy,
	)
	return args
}

func buildInsertTranscripcioPersonaQuery(style string, returning bool) string {
	query := `
        INSERT INTO transcripcions_persones_raw (
            transcripcio_id, rol, nom, nom_estat, cognom1, cognom1_estat, cognom2, cognom2_estat, cognom_soltera, cognom_soltera_estat, sexe, sexe_estat,
            edat_text, edat_estat, estat_civil_text, estat_civil_estat, municipi_text, municipi_estat, ofici_text, ofici_estat,
            casa_nom, casa_estat, persona_id, linked_by, linked_at, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if returning {
		query += " RETURNING id"
	}
	return formatPlaceholders(style, query)
}

func buildInsertTranscripcioPersonaArgs(p TranscripcioPersonaRaw) []interface{} {
	return []interface{}{
		p.TranscripcioID, p.Rol, p.Nom, p.NomEstat, p.Cognom1, p.Cognom1Estat, p.Cognom2, p.Cognom2Estat, p.CognomSoltera, p.CognomSolteraEstat, p.Sexe, p.SexeEstat,
		p.EdatText, p.EdatEstat, p.EstatCivilText, p.EstatCivilEstat, p.MunicipiText, p.MunicipiEstat, p.OficiText, p.OficiEstat,
		p.CasaNom, p.CasaEstat, p.PersonaID, p.LinkedBy, p.LinkedAt, p.Notes,
	}
}

func buildInsertTranscripcioAtributQuery(style string, returning bool) string {
	query := `
        INSERT INTO transcripcions_atributs_raw (transcripcio_id, clau, tipus_valor, valor_text, valor_int, valor_date, valor_bool, estat, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if returning {
		query += " RETURNING id"
	}
	return formatPlaceholders(style, query)
}

func buildInsertTranscripcioAtributArgs(a TranscripcioAtributRaw) []interface{} {
	return []interface{}{
		a.TranscripcioID, a.Clau, a.TipusValor, a.ValorText, a.ValorInt, a.ValorDate, a.ValorBool, a.Estat, a.Notes,
	}
}

func buildBulkInsertTranscripcionsRaw(style, nowFun string, rows []TranscripcioRaw, ids []int) (string, []interface{}) {
	if len(rows) == 0 {
		return "", nil
	}
	includeID := len(ids) == len(rows) && len(ids) > 0
	cols := []string{
		"llibre_id",
		"pagina_id",
		"num_pagina_text",
		"posicio_pagina",
		"tipus_acte",
		"any_doc",
		"data_acte_text",
		"data_acte_iso",
		"data_acte_estat",
		"transcripcio_literal",
		"notes_marginals",
		"observacions_paleografiques",
		"moderation_status",
		"moderated_by",
		"moderated_at",
		"moderation_notes",
		"created_by",
		"created_at",
		"updated_at",
	}
	if includeID {
		cols = append([]string{"id"}, cols...)
	}
	values := make([]string, 0, len(rows))
	argsPerRow := 17
	if includeID {
		argsPerRow++
	}
	args := make([]interface{}, 0, len(rows)*argsPerRow)
	for i, row := range rows {
		value := "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + nowFun + ", " + nowFun + ")"
		if includeID {
			value = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + nowFun + ", " + nowFun + ")"
		}
		values = append(values, value)
		args = append(args, buildInsertTranscripcioRawArgs(row, idAt(ids, i), includeID)...)
	}
	query := "INSERT INTO transcripcions_raw (" + strings.Join(cols, ", ") + ") VALUES " + strings.Join(values, ", ")
	return formatPlaceholders(style, query), args
}

func buildBulkInsertTranscripcioPersones(style string, rows []TranscripcioPersonaRaw) (string, []interface{}) {
	if len(rows) == 0 {
		return "", nil
	}
	cols := []string{
		"transcripcio_id",
		"rol",
		"nom",
		"nom_estat",
		"cognom1",
		"cognom1_estat",
		"cognom2",
		"cognom2_estat",
		"cognom_soltera",
		"cognom_soltera_estat",
		"sexe",
		"sexe_estat",
		"edat_text",
		"edat_estat",
		"estat_civil_text",
		"estat_civil_estat",
		"municipi_text",
		"municipi_estat",
		"ofici_text",
		"ofici_estat",
		"casa_nom",
		"casa_estat",
		"persona_id",
		"linked_by",
		"linked_at",
		"notes",
	}
	values := make([]string, 0, len(rows))
	args := make([]interface{}, 0, len(rows)*26)
	for _, row := range rows {
		values = append(values, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		args = append(args, buildInsertTranscripcioPersonaArgs(row)...)
	}
	query := "INSERT INTO transcripcions_persones_raw (" + strings.Join(cols, ", ") + ") VALUES " + strings.Join(values, ", ")
	return formatPlaceholders(style, query), args
}

func buildBulkInsertTranscripcioAtributs(style string, rows []TranscripcioAtributRaw) (string, []interface{}) {
	if len(rows) == 0 {
		return "", nil
	}
	cols := []string{
		"transcripcio_id",
		"clau",
		"tipus_valor",
		"valor_text",
		"valor_int",
		"valor_date",
		"valor_bool",
		"estat",
		"notes",
	}
	values := make([]string, 0, len(rows))
	args := make([]interface{}, 0, len(rows)*9)
	for _, row := range rows {
		values = append(values, "(?, ?, ?, ?, ?, ?, ?, ?, ?)")
		args = append(args, buildInsertTranscripcioAtributArgs(row)...)
	}
	query := "INSERT INTO transcripcions_atributs_raw (" + strings.Join(cols, ", ") + ") VALUES " + strings.Join(values, ", ")
	return formatPlaceholders(style, query), args
}

func idAt(ids []int, idx int) int {
	if idx < 0 || idx >= len(ids) {
		return 0
	}
	return ids[idx]
}
