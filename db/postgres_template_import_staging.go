package db

import (
	"database/sql"
	"fmt"
	"time"

	pq "github.com/lib/pq"
)

type postgresTemplateImportStagedRawRow struct {
	ImportSeq int
	ID        int
	Row       TranscripcioRaw
}

type postgresTemplateImportStagedPersonaRow struct {
	ImportSeq int
	SubSeq    int
	Row       TranscripcioPersonaRaw
}

type postgresTemplateImportStagedAtributRow struct {
	ImportSeq int
	SubSeq    int
	Row       TranscripcioAtributRaw
}

func (h sqlHelper) bulkCreateTranscripcioRawBundlesPostgresStaging(bundles []TranscripcioRawImportBundle) (TranscripcioRawImportBulkResult, error) {
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
	rawBatchSize := bulkInsertStatementBatchSizeFor(h.style, "transcripcions_raw", len(buildInsertTranscripcioRawArgs(TranscripcioRaw{}, 1, true)))
	totalPersones := 0
	totalAtributs := 0
	for i := range bundles {
		totalPersones += len(bundles[i].Persones)
		totalAtributs += len(bundles[i].Atributs)
	}
	rawRows := make([]postgresTemplateImportStagedRawRow, 0, len(bundles))
	personRows := make([]postgresTemplateImportStagedPersonaRow, 0, totalPersones)
	attrRows := make([]postgresTemplateImportStagedAtributRow, 0, totalAtributs)
	if err := h.createPostgresTemplateImportStagingTablesTx(tx); err != nil {
		return res, err
	}

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
		for offset := i; offset < end; offset++ {
			raw := bundles[offset].Transcripcio
			normalizeTranscripcioRawForInsert(&raw)
			rawID := batchIDs[offset-i]
			rawRows = append(rawRows, postgresTemplateImportStagedRawRow{
				ImportSeq: offset,
				ID:        rawID,
				Row:       raw,
			})
			res.IDs = append(res.IDs, rawID)
			for j := range bundles[offset].Persones {
				persona := bundles[offset].Persones[j]
				persona.TranscripcioID = rawID
				personRows = append(personRows, postgresTemplateImportStagedPersonaRow{
					ImportSeq: offset,
					SubSeq:    j,
					Row:       persona,
				})
			}
			for j := range bundles[offset].Atributs {
				attribute := bundles[offset].Atributs[j]
				attribute.TranscripcioID = rawID
				attrRows = append(attrRows, postgresTemplateImportStagedAtributRow{
					ImportSeq: offset,
					SubSeq:    j,
					Row:       attribute,
				})
			}
		}
	}
	if err := h.copyInPostgresTemplateImportStagedRawTx(tx, rawRows); err != nil {
		res.Metrics.TranscripcioInsertDur += time.Since(start)
		return res, err
	}
	for i := 0; i < len(rawRows); i += rawBatchSize {
		end := i + rawBatchSize
		if end > len(rawRows) {
			end = len(rawRows)
		}
		if err := h.insertPostgresTemplateImportStagedRawBatchTx(tx, i, end); err != nil {
			res.Metrics.TranscripcioInsertDur += time.Since(start)
			return res, err
		}
		res.Metrics.TranscripcioBatches++
	}
	res.Metrics.TranscripcioInsertDur += time.Since(start)

	start = time.Now()
	if err := h.copyInPostgresTemplateImportStagedPersonesTx(tx, personRows); err != nil {
		res.Metrics.PersonaPersistDur += time.Since(start)
		return res, err
	}
	if err := h.insertPostgresTemplateImportStagedPersonesTx(tx); err != nil {
		res.Metrics.PersonaPersistDur += time.Since(start)
		return res, err
	}
	if len(personRows) > 0 {
		res.Metrics.PersonaBatches = 1
	}
	res.Metrics.PersonaPersistDur += time.Since(start)
	res.Metrics.Persones = len(personRows)

	start = time.Now()
	if err := h.copyInPostgresTemplateImportStagedAtributsTx(tx, attrRows); err != nil {
		res.Metrics.LinksPersistDur += time.Since(start)
		return res, err
	}
	if err := h.insertPostgresTemplateImportStagedAtributsTx(tx); err != nil {
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

func (h sqlHelper) createPostgresTemplateImportStagingTablesTx(tx *sql.Tx) error {
	if tx == nil {
		return fmt.Errorf("nil tx")
	}
	stmts := []string{
		`
        CREATE TEMP TABLE tmp_template_import_transcripcions_raw AS
        SELECT
            0::INTEGER AS import_seq,
            id,
            llibre_id,
            pagina_id,
            num_pagina_text,
            posicio_pagina,
            tipus_acte,
            any_doc,
            data_acte_text,
            data_acte_iso,
            data_acte_estat,
            transcripcio_literal,
            notes_marginals,
            observacions_paleografiques,
            moderation_status,
            moderated_by,
            moderated_at,
            moderation_notes,
            created_by
        FROM transcripcions_raw
        WITH NO DATA`,
		`
        CREATE TEMP TABLE tmp_template_import_transcripcions_persones_raw AS
        SELECT
            0::INTEGER AS import_seq,
            0::INTEGER AS import_subseq,
            transcripcio_id,
            rol,
            nom,
            nom_estat,
            cognom1,
            cognom1_estat,
            cognom2,
            cognom2_estat,
            cognom_soltera,
            cognom_soltera_estat,
            sexe,
            sexe_estat,
            edat_text,
            edat_estat,
            estat_civil_text,
            estat_civil_estat,
            municipi_text,
            municipi_estat,
            ofici_text,
            ofici_estat,
            casa_nom,
            casa_estat,
            persona_id,
            linked_by,
            linked_at,
            notes
        FROM transcripcions_persones_raw
        WITH NO DATA`,
		`
        CREATE TEMP TABLE tmp_template_import_transcripcions_atributs_raw AS
        SELECT
            0::INTEGER AS import_seq,
            0::INTEGER AS import_subseq,
            transcripcio_id,
            clau,
            tipus_valor,
            valor_text,
            valor_int,
            valor_date,
            valor_bool,
            estat,
            notes
        FROM transcripcions_atributs_raw
        WITH NO DATA`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (h sqlHelper) copyInPostgresTemplateImportStagedRawTx(tx *sql.Tx, rows []postgresTemplateImportStagedRawRow) error {
	if len(rows) == 0 {
		return nil
	}
	stmt, err := tx.Prepare(pq.CopyIn(
		"tmp_template_import_transcripcions_raw",
		"import_seq", "id", "llibre_id", "pagina_id", "num_pagina_text", "posicio_pagina", "tipus_acte", "any_doc",
		"data_acte_text", "data_acte_iso", "data_acte_estat", "transcripcio_literal", "notes_marginals",
		"observacions_paleografiques", "moderation_status", "moderated_by", "moderated_at", "moderation_notes", "created_by",
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
		args := append([]interface{}{row.ImportSeq}, buildInsertTranscripcioRawArgs(row.Row, row.ID, true)...)
		if _, err := stmt.Exec(args...); err != nil {
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

func (h sqlHelper) insertPostgresTemplateImportStagedRawBatchTx(tx *sql.Tx, startSeq, endSeq int) error {
	if tx == nil || startSeq >= endSeq {
		return nil
	}
	_, err := tx.Exec(`
        INSERT INTO transcripcions_raw (
            id, llibre_id, pagina_id, num_pagina_text, posicio_pagina, tipus_acte, any_doc,
            data_acte_text, data_acte_iso, data_acte_estat, transcripcio_literal, notes_marginals,
            observacions_paleografiques, moderation_status, moderated_by, moderated_at, moderation_notes,
            created_by, created_at, updated_at
        )
        SELECT
            id, llibre_id, pagina_id, num_pagina_text, posicio_pagina, tipus_acte, any_doc,
            data_acte_text, data_acte_iso, data_acte_estat, transcripcio_literal, notes_marginals,
            observacions_paleografiques, moderation_status, moderated_by, moderated_at, moderation_notes,
            created_by, NOW(), NOW()
        FROM tmp_template_import_transcripcions_raw
        WHERE import_seq >= $1 AND import_seq < $2
        ORDER BY import_seq`, startSeq, endSeq)
	return err
}

func (h sqlHelper) copyInPostgresTemplateImportStagedPersonesTx(tx *sql.Tx, rows []postgresTemplateImportStagedPersonaRow) error {
	if len(rows) == 0 {
		return nil
	}
	stmt, err := tx.Prepare(pq.CopyIn(
		"tmp_template_import_transcripcions_persones_raw",
		"import_seq", "import_subseq",
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
		args := append([]interface{}{row.ImportSeq, row.SubSeq}, buildInsertTranscripcioPersonaArgs(row.Row)...)
		if _, err := stmt.Exec(args...); err != nil {
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

func (h sqlHelper) insertPostgresTemplateImportStagedPersonesTx(tx *sql.Tx) error {
	if tx == nil {
		return nil
	}
	_, err := tx.Exec(`
        INSERT INTO transcripcions_persones_raw (
            transcripcio_id, rol, nom, nom_estat, cognom1, cognom1_estat, cognom2, cognom2_estat,
            cognom_soltera, cognom_soltera_estat, sexe, sexe_estat, edat_text, edat_estat,
            estat_civil_text, estat_civil_estat, municipi_text, municipi_estat, ofici_text, ofici_estat,
            casa_nom, casa_estat, persona_id, linked_by, linked_at, notes
        )
        SELECT
            transcripcio_id, rol, nom, nom_estat, cognom1, cognom1_estat, cognom2, cognom2_estat,
            cognom_soltera, cognom_soltera_estat, sexe, sexe_estat, edat_text, edat_estat,
            estat_civil_text, estat_civil_estat, municipi_text, municipi_estat, ofici_text, ofici_estat,
            casa_nom, casa_estat, persona_id, linked_by, linked_at, notes
        FROM tmp_template_import_transcripcions_persones_raw
        ORDER BY import_seq, import_subseq`)
	return err
}

func (h sqlHelper) copyInPostgresTemplateImportStagedAtributsTx(tx *sql.Tx, rows []postgresTemplateImportStagedAtributRow) error {
	if len(rows) == 0 {
		return nil
	}
	stmt, err := tx.Prepare(pq.CopyIn(
		"tmp_template_import_transcripcions_atributs_raw",
		"import_seq", "import_subseq",
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
		args := append([]interface{}{row.ImportSeq, row.SubSeq}, buildInsertTranscripcioAtributArgs(row.Row)...)
		if _, err := stmt.Exec(args...); err != nil {
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

func (h sqlHelper) insertPostgresTemplateImportStagedAtributsTx(tx *sql.Tx) error {
	if tx == nil {
		return nil
	}
	_, err := tx.Exec(`
        INSERT INTO transcripcions_atributs_raw (
            transcripcio_id, clau, tipus_valor, valor_text, valor_int, valor_date, valor_bool, estat, notes
        )
        SELECT
            transcripcio_id, clau, tipus_valor, valor_text, valor_int, valor_date, valor_bool, estat, notes
        FROM tmp_template_import_transcripcions_atributs_raw
        ORDER BY import_seq, import_subseq`)
	return err
}
