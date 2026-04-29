package db

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	pq "github.com/lib/pq"
)

type PostgresTemplateImportStagingBatchMetrics struct {
	Index                   int
	RangeStart              int
	RangeEnd                int
	Rows                    int
	Persones                int
	Atributs                int
	CreateDropTempTablesDur time.Duration
	BuildRowsDur            time.Duration
	CopyRawStagingDur       time.Duration
	InsertRawFinalDur       time.Duration
	CopyPersonesStagingDur  time.Duration
	InsertPersonesFinalDur  time.Duration
	CopyAtributsStagingDur  time.Duration
	InsertAtributsFinalDur  time.Duration
	DirectPersonesCopyDur   time.Duration
	DirectAtributsCopyDur   time.Duration
	CommitDur               time.Duration
	UnaccountedDur          time.Duration
	TotalDur                time.Duration
}

type PostgresTemplateImportStagingMetrics struct {
	Batches                 []PostgresTemplateImportStagingBatchMetrics
	CreateDropTempTablesDur time.Duration
	BuildRowsDur            time.Duration
	CopyRawStagingDur       time.Duration
	InsertRawFinalDur       time.Duration
	CopyPersonesStagingDur  time.Duration
	InsertPersonesFinalDur  time.Duration
	CopyAtributsStagingDur  time.Duration
	InsertAtributsFinalDur  time.Duration
	DirectPersonesCopyDur   time.Duration
	DirectAtributsCopyDur   time.Duration
	CommitDur               time.Duration
	UnaccountedDur          time.Duration
	TotalDur                time.Duration
	Rows                    int
	Persones                int
	Atributs                int
}

var postgresTemplateImportStagingProfileMu sync.Mutex
var postgresTemplateImportStagingProfile PostgresTemplateImportStagingMetrics

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

type postgresTemplateImportDirectChildCopyOptions struct {
	Persones bool
	Atributs bool
}

func (o postgresTemplateImportDirectChildCopyOptions) enabled() bool {
	return o.Persones || o.Atributs
}

func postgresTemplateImportStagingProfileEnabled() bool {
	return strings.TrimSpace(os.Getenv("CG_POSTGRES_STAGING_PROFILE")) == "1"
}

func postgresTemplateImportDirectChildCopyOptionsFromEnv() postgresTemplateImportDirectChildCopyOptions {
	if strings.TrimSpace(os.Getenv("CG_POSTGRES_STAGING_WHOLE_IMPORT")) == "1" {
		return postgresTemplateImportDirectChildCopyOptions{}
	}
	directChildren := strings.TrimSpace(os.Getenv("CG_POSTGRES_DIRECT_CHILD_COPY")) == "1"
	return postgresTemplateImportDirectChildCopyOptions{
		Persones: directChildren || strings.TrimSpace(os.Getenv("CG_POSTGRES_DIRECT_PERSON_COPY")) == "1",
		Atributs: directChildren || strings.TrimSpace(os.Getenv("CG_POSTGRES_DIRECT_ATTR_COPY")) == "1",
	}
}

func ResetPostgresTemplateImportStagingProfile() {
	postgresTemplateImportStagingProfileMu.Lock()
	defer postgresTemplateImportStagingProfileMu.Unlock()
	postgresTemplateImportStagingProfile = PostgresTemplateImportStagingMetrics{}
}

func DrainPostgresTemplateImportStagingProfile() PostgresTemplateImportStagingMetrics {
	postgresTemplateImportStagingProfileMu.Lock()
	defer postgresTemplateImportStagingProfileMu.Unlock()
	metrics := postgresTemplateImportStagingProfile
	postgresTemplateImportStagingProfile = PostgresTemplateImportStagingMetrics{}
	return metrics
}

func recordPostgresTemplateImportStagingBatch(batch PostgresTemplateImportStagingBatchMetrics) {
	postgresTemplateImportStagingProfileMu.Lock()
	defer postgresTemplateImportStagingProfileMu.Unlock()
	batch.Index = len(postgresTemplateImportStagingProfile.Batches) + 1
	batch.RangeStart = postgresTemplateImportStagingProfile.Rows + 1
	batch.RangeEnd = postgresTemplateImportStagingProfile.Rows + batch.Rows
	postgresTemplateImportStagingProfile.Batches = append(postgresTemplateImportStagingProfile.Batches, batch)
	postgresTemplateImportStagingProfile.CreateDropTempTablesDur += batch.CreateDropTempTablesDur
	postgresTemplateImportStagingProfile.BuildRowsDur += batch.BuildRowsDur
	postgresTemplateImportStagingProfile.CopyRawStagingDur += batch.CopyRawStagingDur
	postgresTemplateImportStagingProfile.InsertRawFinalDur += batch.InsertRawFinalDur
	postgresTemplateImportStagingProfile.CopyPersonesStagingDur += batch.CopyPersonesStagingDur
	postgresTemplateImportStagingProfile.InsertPersonesFinalDur += batch.InsertPersonesFinalDur
	postgresTemplateImportStagingProfile.CopyAtributsStagingDur += batch.CopyAtributsStagingDur
	postgresTemplateImportStagingProfile.InsertAtributsFinalDur += batch.InsertAtributsFinalDur
	postgresTemplateImportStagingProfile.DirectPersonesCopyDur += batch.DirectPersonesCopyDur
	postgresTemplateImportStagingProfile.DirectAtributsCopyDur += batch.DirectAtributsCopyDur
	postgresTemplateImportStagingProfile.CommitDur += batch.CommitDur
	postgresTemplateImportStagingProfile.UnaccountedDur += batch.UnaccountedDur
	postgresTemplateImportStagingProfile.TotalDur += batch.TotalDur
	postgresTemplateImportStagingProfile.Rows += batch.Rows
	postgresTemplateImportStagingProfile.Persones += batch.Persones
	postgresTemplateImportStagingProfile.Atributs += batch.Atributs
}

func (m PostgresTemplateImportStagingBatchMetrics) measuredDur() time.Duration {
	return m.CreateDropTempTablesDur +
		m.BuildRowsDur +
		m.CopyRawStagingDur +
		m.InsertRawFinalDur +
		m.CopyPersonesStagingDur +
		m.InsertPersonesFinalDur +
		m.CopyAtributsStagingDur +
		m.InsertAtributsFinalDur +
		m.DirectPersonesCopyDur +
		m.DirectAtributsCopyDur +
		m.CommitDur
}

func (h sqlHelper) bulkCreateTranscripcioRawBundlesPostgresStaging(bundles []TranscripcioRawImportBundle) (TranscripcioRawImportBulkResult, error) {
	directChildCopy := postgresTemplateImportDirectChildCopyOptionsFromEnv()
	if directChildCopy.enabled() {
		res, err := h.bulkCreateTranscripcioRawBundlesPostgresStagingTx(bundles, directChildCopy)
		if err == nil {
			return res, nil
		}
	}
	return h.bulkCreateTranscripcioRawBundlesPostgresStagingTx(bundles, postgresTemplateImportDirectChildCopyOptions{})
}

func (h sqlHelper) bulkCreateTranscripcioRawBundlesPostgresStagingTx(bundles []TranscripcioRawImportBundle, directChildCopy postgresTemplateImportDirectChildCopyOptions) (TranscripcioRawImportBulkResult, error) {
	res := TranscripcioRawImportBulkResult{
		IDs: make([]int, 0, len(bundles)),
		Metrics: TranscripcioRawImportBulkMetrics{
			Rows: len(bundles),
		},
	}
	if len(bundles) == 0 {
		return res, nil
	}
	var stagingBatch *PostgresTemplateImportStagingBatchMetrics
	if postgresTemplateImportStagingProfileEnabled() {
		stagingBatch = &PostgresTemplateImportStagingBatchMetrics{Rows: len(bundles)}
	}
	tx, err := h.db.Begin()
	if err != nil {
		return res, err
	}
	totalStart := time.Now()
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
	phaseStart := time.Now()
	if err := h.createPostgresTemplateImportStagingTablesTx(tx, directChildCopy); err != nil {
		return res, err
	}
	if stagingBatch != nil {
		stagingBatch.CreateDropTempTablesDur += time.Since(phaseStart)
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
	if stagingBatch != nil {
		stagingBatch.BuildRowsDur += time.Since(start)
	}
	phaseStart = time.Now()
	if err := h.copyInPostgresTemplateImportStagedRawTx(tx, rawRows); err != nil {
		res.Metrics.TranscripcioInsertDur += time.Since(start)
		return res, err
	}
	if stagingBatch != nil {
		stagingBatch.CopyRawStagingDur += time.Since(phaseStart)
	}
	phaseStart = time.Now()
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
	if stagingBatch != nil {
		stagingBatch.InsertRawFinalDur += time.Since(phaseStart)
	}
	res.Metrics.TranscripcioInsertDur += time.Since(start)

	start = time.Now()
	phaseStart = time.Now()
	if directChildCopy.Persones {
		if err := h.copyInPostgresTemplateImportDirectPersonesTx(tx, personRows); err != nil {
			res.Metrics.PersonaPersistDur += time.Since(start)
			return res, err
		}
		if stagingBatch != nil {
			stagingBatch.DirectPersonesCopyDur += time.Since(phaseStart)
		}
	} else {
		if err := h.copyInPostgresTemplateImportStagedPersonesTx(tx, personRows); err != nil {
			res.Metrics.PersonaPersistDur += time.Since(start)
			return res, err
		}
		if stagingBatch != nil {
			stagingBatch.CopyPersonesStagingDur += time.Since(phaseStart)
		}
		phaseStart = time.Now()
		if err := h.insertPostgresTemplateImportStagedPersonesTx(tx); err != nil {
			res.Metrics.PersonaPersistDur += time.Since(start)
			return res, err
		}
		if stagingBatch != nil {
			stagingBatch.InsertPersonesFinalDur += time.Since(phaseStart)
		}
	}
	if len(personRows) > 0 {
		res.Metrics.PersonaBatches = 1
	}
	res.Metrics.PersonaPersistDur += time.Since(start)
	res.Metrics.Persones = len(personRows)

	start = time.Now()
	phaseStart = time.Now()
	if directChildCopy.Atributs {
		if err := h.copyInPostgresTemplateImportDirectAtributsTx(tx, attrRows); err != nil {
			res.Metrics.LinksPersistDur += time.Since(start)
			return res, err
		}
		if stagingBatch != nil {
			stagingBatch.DirectAtributsCopyDur += time.Since(phaseStart)
		}
	} else {
		if err := h.copyInPostgresTemplateImportStagedAtributsTx(tx, attrRows); err != nil {
			res.Metrics.LinksPersistDur += time.Since(start)
			return res, err
		}
		if stagingBatch != nil {
			stagingBatch.CopyAtributsStagingDur += time.Since(phaseStart)
		}
		phaseStart = time.Now()
		if err := h.insertPostgresTemplateImportStagedAtributsTx(tx); err != nil {
			res.Metrics.LinksPersistDur += time.Since(start)
			return res, err
		}
		if stagingBatch != nil {
			stagingBatch.InsertAtributsFinalDur += time.Since(phaseStart)
		}
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
	if stagingBatch != nil {
		stagingBatch.CommitDur += res.Metrics.CommitDur
		stagingBatch.Persones = len(personRows)
		stagingBatch.Atributs = len(attrRows)
		measuredDur := stagingBatch.measuredDur()
		if elapsed := time.Since(totalStart); elapsed > measuredDur {
			stagingBatch.UnaccountedDur += elapsed - measuredDur
		}
		stagingBatch.TotalDur += measuredDur + stagingBatch.UnaccountedDur
		recordPostgresTemplateImportStagingBatch(*stagingBatch)
	}
	committed = true
	return res, nil
}

func (h sqlHelper) createPostgresTemplateImportStagingTablesTx(tx *sql.Tx, directChildCopy postgresTemplateImportDirectChildCopyOptions) error {
	if tx == nil {
		return fmt.Errorf("nil tx")
	}
	stmts := []string{
		`DROP TABLE IF EXISTS pg_temp.tmp_template_import_transcripcions_raw`,
		`
        CREATE TEMP TABLE tmp_template_import_transcripcions_raw
        ON COMMIT DROP AS
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
	}
	if !directChildCopy.Persones {
		stmts = append(stmts,
			`DROP TABLE IF EXISTS pg_temp.tmp_template_import_transcripcions_persones_raw`,
			`
        CREATE TEMP TABLE tmp_template_import_transcripcions_persones_raw
        ON COMMIT DROP AS
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
		)
	}
	if !directChildCopy.Atributs {
		stmts = append(stmts,
			`DROP TABLE IF EXISTS pg_temp.tmp_template_import_transcripcions_atributs_raw`,
			`
        CREATE TEMP TABLE tmp_template_import_transcripcions_atributs_raw
        ON COMMIT DROP AS
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
		)
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
		if _, err := stmt.Exec(
			row.ImportSeq,
			row.ID,
			row.Row.LlibreID,
			row.Row.PaginaID,
			row.Row.NumPaginaText,
			row.Row.PosicioPagina,
			row.Row.TipusActe,
			row.Row.AnyDoc,
			row.Row.DataActeText,
			row.Row.DataActeISO,
			row.Row.DataActeEstat,
			row.Row.TranscripcioLiteral,
			row.Row.NotesMarginals,
			row.Row.ObservacionsPaleografiques,
			row.Row.ModeracioEstat,
			row.Row.ModeratedBy,
			row.Row.ModeratedAt,
			row.Row.ModeracioMotiu,
			row.Row.CreatedBy,
		); err != nil {
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
		if _, err := stmt.Exec(
			row.ImportSeq,
			row.SubSeq,
			row.Row.TranscripcioID,
			row.Row.Rol,
			row.Row.Nom,
			row.Row.NomEstat,
			row.Row.Cognom1,
			row.Row.Cognom1Estat,
			row.Row.Cognom2,
			row.Row.Cognom2Estat,
			row.Row.CognomSoltera,
			row.Row.CognomSolteraEstat,
			row.Row.Sexe,
			row.Row.SexeEstat,
			row.Row.EdatText,
			row.Row.EdatEstat,
			row.Row.EstatCivilText,
			row.Row.EstatCivilEstat,
			row.Row.MunicipiText,
			row.Row.MunicipiEstat,
			row.Row.OficiText,
			row.Row.OficiEstat,
			row.Row.CasaNom,
			row.Row.CasaEstat,
			row.Row.PersonaID,
			row.Row.LinkedBy,
			row.Row.LinkedAt,
			row.Row.Notes,
		); err != nil {
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

func (h sqlHelper) copyInPostgresTemplateImportDirectPersonesTx(tx *sql.Tx, rows []postgresTemplateImportStagedPersonaRow) error {
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
		if _, err := stmt.Exec(
			row.Row.TranscripcioID,
			row.Row.Rol,
			row.Row.Nom,
			row.Row.NomEstat,
			row.Row.Cognom1,
			row.Row.Cognom1Estat,
			row.Row.Cognom2,
			row.Row.Cognom2Estat,
			row.Row.CognomSoltera,
			row.Row.CognomSolteraEstat,
			row.Row.Sexe,
			row.Row.SexeEstat,
			row.Row.EdatText,
			row.Row.EdatEstat,
			row.Row.EstatCivilText,
			row.Row.EstatCivilEstat,
			row.Row.MunicipiText,
			row.Row.MunicipiEstat,
			row.Row.OficiText,
			row.Row.OficiEstat,
			row.Row.CasaNom,
			row.Row.CasaEstat,
			row.Row.PersonaID,
			row.Row.LinkedBy,
			row.Row.LinkedAt,
			row.Row.Notes,
		); err != nil {
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
		if _, err := stmt.Exec(
			row.ImportSeq,
			row.SubSeq,
			row.Row.TranscripcioID,
			row.Row.Clau,
			row.Row.TipusValor,
			row.Row.ValorText,
			row.Row.ValorInt,
			row.Row.ValorDate,
			row.Row.ValorBool,
			row.Row.Estat,
			row.Row.Notes,
		); err != nil {
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

func (h sqlHelper) copyInPostgresTemplateImportDirectAtributsTx(tx *sql.Tx, rows []postgresTemplateImportStagedAtributRow) error {
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
		if _, err := stmt.Exec(
			row.Row.TranscripcioID,
			row.Row.Clau,
			row.Row.TipusValor,
			row.Row.ValorText,
			row.Row.ValorInt,
			row.Row.ValorDate,
			row.Row.ValorBool,
			row.Row.Estat,
			row.Row.Notes,
		); err != nil {
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
