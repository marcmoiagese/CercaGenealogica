package core

import (
	"fmt"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func persistTemplateImportPlanPostgres(plan *TemplateImportPlan, options TemplateImportPersistOptions) TemplateImportPersistResult {
	result := TemplateImportPersistResult{}
	if plan == nil || options.Result == nil || len(plan.Rows) == 0 {
		return result
	}
	runtime := options.Runtime
	if runtime == nil && options.App != nil {
		runtime = db.TemplateImportRuntimeFor(options.App.DB)
	}
	for start := 0; start < len(plan.Rows); start += templateImportCreateBatchSize {
		end := start + templateImportCreateBatchSize
		if end > len(plan.Rows) {
			end = len(plan.Rows)
		}
		persistTemplateImportPlanPostgresBatch(plan.Rows[start:end], options.Result, runtime)
	}
	result.Created = options.Result.Created
	result.Failed = options.Result.Failed
	return result
}

func persistTemplateImportPlanPostgresBatch(rows []TemplateImportPlanRow, result *csvImportResult, runtime db.TemplateImportRuntime) {
	if len(rows) == 0 || result == nil {
		return
	}
	if result.WritePrepareBreakdown != nil {
		result.WritePrepareBreakdown.Batches++
	}
	if runtime != nil {
		preallocStart := time.Now()
		bundles := make([]db.TranscripcioRawImportBundle, len(rows))
		if result.WritePrepareBreakdown != nil {
			result.WritePrepareBreakdown.PreallocDur += time.Since(preallocStart)
		}
		for i := range rows {
			transStart := time.Now()
			bundles[i].Transcripcio = rows[i].Transcripcio
			if result.WritePrepareBreakdown != nil {
				result.WritePrepareBreakdown.BuildTranscripcionsBatchDur += time.Since(transStart)
			}
			personesStart := time.Now()
			bundles[i].Persones = make([]db.TranscripcioPersonaRaw, 0, len(rows[i].Persones))
			for _, persona := range rows[i].Persones {
				bundles[i].Persones = append(bundles[i].Persones, persona.Persona)
			}
			if result.WritePrepareBreakdown != nil {
				result.WritePrepareBreakdown.BuildPersonesBatchDur += time.Since(personesStart)
			}
			linksStart := time.Now()
			bundles[i].Atributs = make([]db.TranscripcioAtributRaw, 0, len(rows[i].Atributs))
			for _, attribute := range rows[i].Atributs {
				bundles[i].Atributs = append(bundles[i].Atributs, attribute.Attribute)
			}
			if result.WritePrepareBreakdown != nil {
				result.WritePrepareBreakdown.BuildLinksBatchDur += time.Since(linksStart)
			}
		}
		bulkResult, err := runtime.BulkCreateBundles(bundles)
		if err == nil && len(bulkResult.IDs) == len(rows) {
			result.Debug.addWriteBulkBatch(len(rows))
			result.Debug.addWriteBulkStatementBatches(
				bulkResult.Metrics.TranscripcioBatches,
				bulkResult.Metrics.PersonaBatches,
				bulkResult.Metrics.AtributBatches,
			)
			result.Debug.addWriteTranscripcioInsert(bulkResult.Metrics.TranscripcioInsertDur)
			result.Debug.addWritePersonaPersist(bulkResult.Metrics.PersonaPersistDur)
			result.Debug.addWriteLinksPersist(bulkResult.Metrics.LinksPersistDur)
			result.Debug.addWriteCommit(bulkResult.Metrics.CommitDur)
			for i := range rows {
				result.Created++
				result.markBook(rows[i].BookID)
			}
			return
		}
		result.Debug.addWriteBulkFallback()
	}
	for i := range rows {
		persistTemplateImportPlanPostgresRow(rows[i], result, runtime)
	}
}

func persistTemplateImportPlanPostgresRow(row TemplateImportPlanRow, result *csvImportResult, runtime db.TemplateImportRuntime) {
	if result == nil || runtime == nil {
		return
	}
	createResult, err := runtime.CreateBundle(templateImportPlanRowBundle(row))
	result.Debug.addWriteTranscripcioInsert(createResult.Metrics.TranscripcioInsertDur)
	result.Debug.addWritePersonaPersist(createResult.Metrics.PersonaPersistDur)
	result.Debug.addWriteLinksPersist(createResult.Metrics.LinksPersistDur)
	if err != nil || len(createResult.IDs) == 0 || createResult.IDs[0] == 0 {
		result.Failed++
		reason := "no s'ha pogut crear el registre"
		if err != nil {
			reason = fmt.Sprintf("no s'ha pogut crear el registre: %v", err)
		}
		result.Errors = append(result.Errors, importErrorEntry{Row: row.RowNum, Reason: reason})
		return
	}
	result.Created++
	result.markBook(row.BookID)
}

func templateImportPlanRowBundle(row TemplateImportPlanRow) db.TranscripcioRawImportBundle {
	bundle := db.TranscripcioRawImportBundle{
		Transcripcio: row.Transcripcio,
		Persones:     make([]db.TranscripcioPersonaRaw, 0, len(row.Persones)),
		Atributs:     make([]db.TranscripcioAtributRaw, 0, len(row.Atributs)),
	}
	for _, persona := range row.Persones {
		bundle.Persones = append(bundle.Persones, persona.Persona)
	}
	for _, attribute := range row.Atributs {
		bundle.Atributs = append(bundle.Atributs, attribute.Attribute)
	}
	return bundle
}
