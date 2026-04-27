package core

import "github.com/marcmoiagese/CercaGenealogica/db"

const (
	sqliteTemplateImportLargeBatchThreshold = templateImportCreateBatchSize * 2
	sqliteTemplateImportLargeBatchSize      = 2000
)

func sqliteTemplateImportPersistBatchSize(totalRows int) int {
	if totalRows > sqliteTemplateImportLargeBatchThreshold {
		return sqliteTemplateImportLargeBatchSize
	}
	return templateImportCreateBatchSize
}

func persistTemplateImportPlanSQLite(plan *TemplateImportPlan, options TemplateImportPersistOptions) TemplateImportPersistResult {
	result := TemplateImportPersistResult{}
	if plan == nil || options.App == nil || options.Result == nil || len(plan.Rows) == 0 {
		return result
	}
	runtime := options.Runtime
	if runtime == nil {
		runtime = db.TemplateImportRuntimeFor(options.App.DB)
	}
	pending := templateImportPlanPendingCreates(plan)
	batchSize := sqliteTemplateImportPersistBatchSize(len(pending))
	for len(pending) > 0 {
		batchEnd := batchSize
		if batchEnd > len(pending) {
			batchEnd = len(pending)
		}
		options.App.flushTemplatePendingCreates(pending[:batchEnd], options.Result, runtime)
		pending = pending[batchEnd:]
	}
	result.Created = options.Result.Created
	result.Failed = options.Result.Failed
	return result
}
