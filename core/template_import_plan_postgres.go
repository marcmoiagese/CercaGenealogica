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
	if IsPostgresStagingWholeImportEnabled() {
		if persistTemplateImportPlanPostgresWhole(plan, options.App, options.Result) {
			result.Created = options.Result.Created
			result.Failed = options.Result.Failed
			return result
		}
	}
	for start := 0; start < len(plan.Rows); start += templateImportCreateBatchSize {
		end := start + templateImportCreateBatchSize
		if end > len(plan.Rows) {
			end = len(plan.Rows)
		}
		persistTemplateImportPlanPostgresBatch(plan.Rows[start:end], options.App, options.Result, runtime)
	}
	result.Created = options.Result.Created
	result.Failed = options.Result.Failed
	return result
}

func persistTemplateImportPlanPostgresWhole(plan *TemplateImportPlan, app *App, result *csvImportResult) bool {
	if plan == nil || len(plan.Rows) == 0 || app == nil || app.DB == nil || result == nil {
		return false
	}
	preallocStart := time.Now()
	bundles := make([]db.TranscripcioRawImportBundle, 0, len(plan.Rows))
	preallocDur := time.Since(preallocStart)
	buildTranscripcionsDur := time.Duration(0)
	buildPersonesDur := time.Duration(0)
	buildLinksDur := time.Duration(0)
	for i := range plan.Rows {
		transStart := time.Now()
		bundle := db.TranscripcioRawImportBundle{Transcripcio: plan.Rows[i].Transcripcio}
		buildTranscripcionsDur += time.Since(transStart)
		personesStart := time.Now()
		bundle.Persones = make([]db.TranscripcioPersonaRaw, 0, len(plan.Rows[i].Persones))
		for _, persona := range plan.Rows[i].Persones {
			bundle.Persones = append(bundle.Persones, persona.Persona)
		}
		buildPersonesDur += time.Since(personesStart)
		linksStart := time.Now()
		bundle.Atributs = make([]db.TranscripcioAtributRaw, 0, len(plan.Rows[i].Atributs))
		for _, attribute := range plan.Rows[i].Atributs {
			bundle.Atributs = append(bundle.Atributs, attribute.Attribute)
		}
		buildLinksDur += time.Since(linksStart)
		bundles = append(bundles, bundle)
	}
	bulkResult, err := app.DB.BulkCreateTranscripcioRawBundles(bundles)
	if err != nil || len(bulkResult.IDs) != len(plan.Rows) {
		return false
	}
	if result.WritePrepareBreakdown != nil {
		result.WritePrepareBreakdown.Batches++
		result.WritePrepareBreakdown.PreallocDur += preallocDur
		result.WritePrepareBreakdown.BuildTranscripcionsBatchDur += buildTranscripcionsDur
		result.WritePrepareBreakdown.BuildPersonesBatchDur += buildPersonesDur
		result.WritePrepareBreakdown.BuildLinksBatchDur += buildLinksDur
	}
	result.Debug.addWriteBulkBatch(len(plan.Rows))
	result.Debug.addWriteBulkStatementBatches(
		bulkResult.Metrics.TranscripcioBatches,
		bulkResult.Metrics.PersonaBatches,
		bulkResult.Metrics.AtributBatches,
	)
	result.Debug.addWriteTranscripcioInsert(bulkResult.Metrics.TranscripcioInsertDur)
	result.Debug.addWritePersonaPersist(bulkResult.Metrics.PersonaPersistDur)
	result.Debug.addWriteLinksPersist(bulkResult.Metrics.LinksPersistDur)
	result.Debug.addWriteCommit(bulkResult.Metrics.CommitDur)
	for i := range plan.Rows {
		result.Created++
		result.markBook(plan.Rows[i].BookID)
	}
	return true
}

func persistTemplateImportPlanPostgresBatch(rows []TemplateImportPlanRow, app *App, result *csvImportResult, runtime db.TemplateImportRuntime) {
	if len(rows) == 0 || result == nil {
		return
	}
	if result.WritePrepareBreakdown != nil {
		result.WritePrepareBreakdown.Batches++
	}
	if app != nil && app.DB != nil {
		preallocStart := time.Now()
		bundles := make([]db.TranscripcioRawImportBundle, 0, len(rows))
		if result.WritePrepareBreakdown != nil {
			result.WritePrepareBreakdown.PreallocDur += time.Since(preallocStart)
		}
		for i := range rows {
			transStart := time.Now()
			bundle := db.TranscripcioRawImportBundle{Transcripcio: rows[i].Transcripcio}
			if result.WritePrepareBreakdown != nil {
				result.WritePrepareBreakdown.BuildTranscripcionsBatchDur += time.Since(transStart)
			}
			personesStart := time.Now()
			bundle.Persones = make([]db.TranscripcioPersonaRaw, 0, len(rows[i].Persones))
			for _, persona := range rows[i].Persones {
				bundle.Persones = append(bundle.Persones, persona.Persona)
			}
			if result.WritePrepareBreakdown != nil {
				result.WritePrepareBreakdown.BuildPersonesBatchDur += time.Since(personesStart)
			}
			linksStart := time.Now()
			bundle.Atributs = make([]db.TranscripcioAtributRaw, 0, len(rows[i].Atributs))
			for _, attribute := range rows[i].Atributs {
				bundle.Atributs = append(bundle.Atributs, attribute.Attribute)
			}
			if result.WritePrepareBreakdown != nil {
				result.WritePrepareBreakdown.BuildLinksBatchDur += time.Since(linksStart)
			}
			bundles = append(bundles, bundle)
		}
		bulkResult, err := app.DB.BulkCreateTranscripcioRawBundles(bundles)
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
