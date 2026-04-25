package db

import (
	"database/sql"
	"sort"
	"strconv"
	"strings"
	"time"

	pq "github.com/lib/pq"
)

func relatedPersonesByTranscripcioIDsPostgres(database DB, transcripcioIDs []int) (map[int][]TranscripcioPersonaRaw, error) {
	if database == nil {
		return map[int][]TranscripcioPersonaRaw{}, nil
	}
	rows, err := database.ListTranscripcioPersonesByTranscripcioIDs(transcripcioIDs)
	if err != nil {
		return nil, err
	}
	return ensurePersonaMaps(rows), nil
}

func relatedAtributsByTranscripcioIDsPostgres(database DB, transcripcioIDs []int) (map[int][]TranscripcioAtributRaw, error) {
	if database == nil {
		return map[int][]TranscripcioAtributRaw{}, nil
	}
	rows, err := database.ListTranscripcioAtributsByTranscripcioIDs(transcripcioIDs)
	if err != nil {
		return nil, err
	}
	return ensureAtributMaps(rows), nil
}

func (h sqlHelper) listTranscripcioStrongMatchCandidatesPostgres(bookID int, tipusActe, pageKey string, maxExistingID int) ([]TranscripcioRaw, map[int][]TranscripcioPersonaRaw, map[int][]TranscripcioAtributRaw, error) {
	personesByTranscripcioID := map[int][]TranscripcioPersonaRaw{}
	atributsByTranscripcioID := map[int][]TranscripcioAtributRaw{}
	pageKey = strings.TrimSpace(pageKey)
	tipusActe = strings.TrimSpace(tipusActe)
	if bookID <= 0 || pageKey == "" || tipusActe == "" {
		return nil, personesByTranscripcioID, atributsByTranscripcioID, nil
	}
	query := `
        SELECT DISTINCT t.id
        FROM transcripcions_raw t
        LEFT JOIN transcripcions_atributs_raw a
               ON a.transcripcio_id = t.id
              AND a.clau = 'pagina_digital'
        LEFT JOIN llibre_pagines p
               ON p.id = t.pagina_id
        WHERE t.llibre_id = $1
          AND t.tipus_acte = $2
          AND (
                LOWER(TRIM(COALESCE(a.valor_text, ''))) = LOWER(TRIM($3))
             OR LOWER(TRIM(COALESCE(t.num_pagina_text, ''))) = LOWER(TRIM($3))`
	args := []interface{}{bookID, tipusActe, pageKey}
	if pageNum, err := strconv.Atoi(pageKey); err == nil && pageNum > 0 {
		query += `
             OR p.num_pagina = $4`
		args = append(args, pageNum)
	}
	query += `
          )`
	if maxExistingID > 0 {
		query += `
          AND t.id <= $` + strconv.Itoa(len(args)+1)
		args = append(args, maxExistingID)
	}
	query += `
        ORDER BY t.id`
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, personesByTranscripcioID, atributsByTranscripcioID, err
	}
	defer rows.Close()
	ids := make([]int, 0)
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, personesByTranscripcioID, atributsByTranscripcioID, err
		}
		if id > 0 {
			ids = append(ids, id)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, personesByTranscripcioID, atributsByTranscripcioID, err
	}
	trans, err := h.listTranscripcionsRawByIDs(ids)
	if err != nil {
		return nil, personesByTranscripcioID, atributsByTranscripcioID, err
	}
	if len(ids) > 0 {
		if personesByTranscripcioID, err = h.listTranscripcioPersonesByTranscripcioIDsPostgres(ids); err != nil {
			return nil, map[int][]TranscripcioPersonaRaw{}, map[int][]TranscripcioAtributRaw{}, err
		}
		if atributsByTranscripcioID, err = h.listTranscripcioAtributsByTranscripcioIDsPostgres(ids); err != nil {
			return nil, map[int][]TranscripcioPersonaRaw{}, map[int][]TranscripcioAtributRaw{}, err
		}
	}
	return trans, personesByTranscripcioID, atributsByTranscripcioID, nil
}

func (h sqlHelper) bulkCreateTranscripcioRawBundlesPostgres(bundles []TranscripcioRawImportBundle) (TranscripcioRawImportBulkResult, error) {
	res := TranscripcioRawImportBulkResult{
		IDs: make([]int, 0, len(bundles)),
		Metrics: TranscripcioRawImportBulkMetrics{
			Rows: len(bundles),
		},
	}
	if len(bundles) == 0 {
		return res, nil
	}
	tx, err := h.db.Begin()
	if err != nil {
		return res, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	totalPersones := 0
	totalAtributs := 0
	rawBatchSize := bulkInsertStatementBatchSizeFor(h.style, "transcripcions_raw", len(buildInsertTranscripcioRawArgs(TranscripcioRaw{}, 1, true)))
	for i := range bundles {
		totalPersones += len(bundles[i].Persones)
		totalAtributs += len(bundles[i].Atributs)
	}
	personRows := make([]TranscripcioPersonaRaw, 0, totalPersones)
	attrRows := make([]TranscripcioAtributRaw, 0, totalAtributs)
	start := time.Now()
	for i := 0; i < len(bundles); i += rawBatchSize {
		end := i + rawBatchSize
		if end > len(bundles) {
			end = len(bundles)
		}
		batchIDs, err := h.allocatePostgresSerialIDsTx(tx, "transcripcions_raw", "id", end-i)
		if err != nil {
			res.Metrics.TranscripcioInsertDur += time.Since(start)
			return res, err
		}
		rawRows := make([]TranscripcioRaw, 0, end-i)
		for offset := i; offset < end; offset++ {
			raw := bundles[offset].Transcripcio
			normalizeTranscripcioRawForInsert(&raw)
			rawID := batchIDs[offset-i]
			rawRows = append(rawRows, raw)
			res.IDs = append(res.IDs, rawID)
			for j := range bundles[offset].Persones {
				p := bundles[offset].Persones[j]
				p.TranscripcioID = rawID
				personRows = append(personRows, p)
			}
			for j := range bundles[offset].Atributs {
				attr := bundles[offset].Atributs[j]
				attr.TranscripcioID = rawID
				attrRows = append(attrRows, attr)
			}
		}
		query, args := buildBulkInsertTranscripcionsRaw(h.style, h.nowFun, rawRows, batchIDs)
		if query == "" {
			continue
		}
		if _, err := tx.Exec(query, args...); err != nil {
			res.Metrics.TranscripcioInsertDur += time.Since(start)
			return res, err
		}
		res.Metrics.TranscripcioBatches++
	}
	res.Metrics.TranscripcioInsertDur += time.Since(start)
	start = time.Now()
	if err := h.copyInPostgresTranscripcioPersonesTx(tx, personRows); err != nil {
		res.Metrics.PersonaPersistDur += time.Since(start)
		return res, err
	}
	if len(personRows) > 0 {
		res.Metrics.PersonaBatches = 1
	}
	res.Metrics.PersonaPersistDur += time.Since(start)
	res.Metrics.Persones = len(personRows)
	start = time.Now()
	if err := h.copyInPostgresTranscripcioAtributsTx(tx, attrRows); err != nil {
		res.Metrics.LinksPersistDur += time.Since(start)
		return res, err
	}
	if len(attrRows) > 0 {
		res.Metrics.AtributBatches = 1
	}
	res.Metrics.LinksPersistDur += time.Since(start)
	res.Metrics.Atributs = len(attrRows)
	start = time.Now()
	if err := tx.Commit(); err != nil {
		res.Metrics.CommitDur += time.Since(start)
		return res, err
	}
	res.Metrics.CommitDur += time.Since(start)
	committed = true
	return res, nil
}

func (h sqlHelper) copyInPostgresTranscripcioPersonesTx(tx *sql.Tx, rows []TranscripcioPersonaRaw) error {
	if len(rows) == 0 {
		return nil
	}
	stmt, err := tx.Prepare(pq.CopyIn(
		"transcripcions_persones_raw",
		"transcripcio_id", "rol", "nom", "nom_estat", "cognom1", "cognom1_estat", "cognom2", "cognom2_estat",
		"cognom_soltera", "cognom_soltera_estat", "sexe", "sexe_estat", "edat_text", "edat_estat",
		"estat_civil_text", "estat_civil_estat", "municipi_text", "municipi_estat", "ofici_text", "ofici_estat",
		"casa_nom", "casa_estat", "persona_id", "linked_by", "linked_at", "notes",
	))
	if err != nil {
		return err
	}
	closeStmt := true
	defer func() {
		if closeStmt {
			_ = stmt.Close()
		}
	}()
	for _, row := range rows {
		if _, err := stmt.Exec(buildInsertTranscripcioPersonaArgs(row)...); err != nil {
			return err
		}
	}
	if _, err := stmt.Exec(); err != nil {
		return err
	}
	if err := stmt.Close(); err != nil {
		return err
	}
	closeStmt = false
	return nil
}

func (h sqlHelper) copyInPostgresTranscripcioAtributsTx(tx *sql.Tx, rows []TranscripcioAtributRaw) error {
	if len(rows) == 0 {
		return nil
	}
	stmt, err := tx.Prepare(pq.CopyIn(
		"transcripcions_atributs_raw",
		"transcripcio_id", "clau", "tipus_valor", "valor_text", "valor_int", "valor_date", "valor_bool", "estat", "notes",
	))
	if err != nil {
		return err
	}
	closeStmt := true
	defer func() {
		if closeStmt {
			_ = stmt.Close()
		}
	}()
	for _, row := range rows {
		if _, err := stmt.Exec(buildInsertTranscripcioAtributArgs(row)...); err != nil {
			return err
		}
	}
	if _, err := stmt.Exec(); err != nil {
		return err
	}
	if err := stmt.Close(); err != nil {
		return err
	}
	closeStmt = false
	return nil
}

func (h sqlHelper) allocatePostgresSerialIDsTx(tx *sql.Tx, table, column string, count int) ([]int, error) {
	if count <= 0 {
		return nil, nil
	}
	query := `
        SELECT nextval(pg_get_serial_sequence($1, $2))
        FROM generate_series(1, $3)`
	rows, err := tx.Query(query, table, column, count)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]int, 0, count)
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) != count {
		return nil, sql.ErrNoRows
	}
	return ids, nil
}

func postgresStrongMatchPolicyKey(principalRoles []string) string {
	if len(principalRoles) == 0 {
		return "batejat\x1fpersona_principal"
	}
	return strings.Join(principalRoles, "\x1f")
}

func postgresStrongMatchKeyForRow(row *TranscripcioRaw, persones map[string]*TranscripcioPersonaRaw, attrs map[string]*TranscripcioAtributRaw, principalRoles []string) string {
	if row == nil {
		return ""
	}
	principalKey := postgresStrongPrincipalKey(persones, principalRoles)
	if principalKey == "" {
		return ""
	}
	signals := []string{"principal:" + principalKey}
	extraCount := 0
	seenSignals := map[string]struct{}{}
	addExtra := func(kind, value string) {
		value = postgresNormalizeStrongMatchPart(value)
		if value == "" {
			return
		}
		signal := kind + ":" + value
		if _, exists := seenSignals[signal]; exists {
			return
		}
		seenSignals[signal] = struct{}{}
		signals = append(signals, signal)
		extraCount++
	}
	if row.DataActeISO.Valid {
		addExtra("data_acte", row.DataActeISO.String)
	} else {
		addExtra("data_acte", row.DataActeText)
	}
	switch strings.ToLower(strings.TrimSpace(row.TipusActe)) {
	case "baptisme":
		for _, key := range []string{"data_bateig", "data_naixement", "data_defuncio", "casat"} {
			if attr := attrs[key]; attr != nil {
				addExtra("attr:"+key, postgresStrongAttrComparableValue(attr))
			}
		}
		for _, role := range []string{"pare", "mare", "avi_patern", "avia_paterna", "avi_matern", "avia_materna", "padri", "padrina"} {
			if p := persones[role]; p != nil {
				addExtra("person:"+role, postgresStrongPersonKey(p))
			}
		}
	default:
		attrKeys := make([]string, 0, len(attrs))
		for key := range attrs {
			if key == "pagina_digital" {
				continue
			}
			attrKeys = append(attrKeys, key)
		}
		sort.Strings(attrKeys)
		for _, key := range attrKeys {
			addExtra("attr:"+key, postgresStrongAttrComparableValue(attrs[key]))
		}
		principalRoleSet := map[string]struct{}{}
		for _, role := range principalRoles {
			role = strings.TrimSpace(role)
			if role != "" {
				principalRoleSet[role] = struct{}{}
			}
		}
		if len(principalRoleSet) == 0 {
			principalRoleSet["batejat"] = struct{}{}
			principalRoleSet["persona_principal"] = struct{}{}
		}
		roleKeys := make([]string, 0, len(persones))
		for role := range persones {
			if _, skip := principalRoleSet[role]; skip {
				continue
			}
			roleKeys = append(roleKeys, role)
		}
		sort.Strings(roleKeys)
		for _, role := range roleKeys {
			addExtra("person:"+role, postgresStrongPersonKey(persones[role]))
		}
	}
	if extraCount < 2 {
		return ""
	}
	return strings.Join(signals, "|")
}

func postgresStrongPrincipalKey(persones map[string]*TranscripcioPersonaRaw, roles []string) string {
	if len(roles) == 0 {
		roles = []string{"batejat", "persona_principal"}
	}
	for _, role := range roles {
		if key := postgresStrongPersonKey(persones[role]); key != "" {
			return key
		}
	}
	return ""
}

func postgresStrongPersonKey(p *TranscripcioPersonaRaw) string {
	if p == nil {
		return ""
	}
	nom := postgresNormalizeStrongMatchPart(p.Nom)
	cognom1 := postgresNormalizeStrongMatchPart(p.Cognom1)
	cognom2 := postgresNormalizeStrongMatchPart(p.Cognom2)
	if nom == "" || (cognom1 == "" && cognom2 == "") {
		return ""
	}
	return nom + "|" + cognom1 + "|" + cognom2
}

func postgresStrongAttrComparableValue(attr *TranscripcioAtributRaw) string {
	if attr == nil {
		return ""
	}
	if attr.ValorDate.Valid {
		return attr.ValorDate.String
	}
	if attr.ValorInt.Valid {
		return strconv.FormatInt(attr.ValorInt.Int64, 10)
	}
	if attr.ValorBool.Valid {
		if attr.ValorBool.Bool {
			return "true"
		}
		return "false"
	}
	return attr.ValorText
}

func postgresNormalizeStrongMatchPart(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) >= 10 && isISODate(value[:10]) {
		return value[:10]
	}
	value = stripDiacritics(value)
	return strings.Join(strings.Fields(value), " ")
}

func isISODate(value string) bool {
	if len(value) != 10 {
		return false
	}
	for i, r := range value {
		switch i {
		case 4, 7:
			if r != '-' {
				return false
			}
		default:
			if r < '0' || r > '9' {
				return false
			}
		}
	}
	return true
}
