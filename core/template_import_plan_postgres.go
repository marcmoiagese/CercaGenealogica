package core

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const (
	postgresTemplateImportBatchSizeMin = templateImportCreateBatchSize
	postgresTemplateImportBatchSizeMax = 1500
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
	batchSize := postgresTemplateImportBatchSize()
	for start := 0; start < len(plan.Rows); start += batchSize {
		end := start + batchSize
		if end > len(plan.Rows) {
			end = len(plan.Rows)
		}
		persistTemplateImportPlanPostgresBatch(plan.Rows[start:end], options.App, options.Result, runtime)
	}
	result.Created = options.Result.Created
	result.Failed = options.Result.Failed
	return result
}

func postgresTemplateImportBatchSize() int {
	value := strings.TrimSpace(os.Getenv("CG_POSTGRES_TEMPLATE_IMPORT_BATCH_SIZE"))
	if value == "" {
		return templateImportCreateBatchSize
	}
	size, err := strconv.Atoi(value)
	if err != nil {
		return templateImportCreateBatchSize
	}
	if size < postgresTemplateImportBatchSizeMin {
		return postgresTemplateImportBatchSizeMin
	}
	if size > postgresTemplateImportBatchSizeMax {
		return postgresTemplateImportBatchSizeMax
	}
	return size
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
				result.addCreatedRegistre(bulkResult.IDs[i])
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
	result.addCreatedRegistre(createResult.IDs[0])
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
