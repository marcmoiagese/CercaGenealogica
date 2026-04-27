package core

import "github.com/marcmoiagese/CercaGenealogica/db"

type TemplateImportPlan struct {
	Rows    []TemplateImportPlanRow
	Summary TemplateImportPlanSummary
}

type TemplateImportPlanRow struct {
	RowNum       int
	BookID       int
	Transcripcio db.TranscripcioRaw
	Persones     []TemplateImportPlanPerson
	Atributs     []TemplateImportPlanAttribute
	Links        []TemplateImportPlanLink
}

type TemplateImportPlanPerson struct {
	Persona db.TranscripcioPersonaRaw
}

type TemplateImportPlanAttribute struct {
	Attribute db.TranscripcioAtributRaw
}

type TemplateImportPlanLink struct {
	Kind string
	Key  string
}

type TemplateImportPlanSummary struct {
	Rows     int
	Books    int
	Persones int
	Atributs int
	Links    int
}

type TemplateImportPersistOptions struct {
	App     *App
	Result  *csvImportResult
	Runtime db.TemplateImportRuntime
}

type TemplateImportPersistResult struct {
	Created    int
	Failed     int
	UsedLegacy bool
}

type TemplateImportPersister interface {
	Persist(plan *TemplateImportPlan, options TemplateImportPersistOptions) TemplateImportPersistResult
}

type PostgresTemplateImportPersister struct{}

type SQLiteTemplateImportPersister struct{}

type legacyTemplateImportPersister struct{}

func buildTemplateImportPlan(pending []templatePendingCreate) *TemplateImportPlan {
	plan := &TemplateImportPlan{
		Rows: make([]TemplateImportPlanRow, 0, len(pending)),
	}
	books := map[int]struct{}{}
	for _, row := range pending {
		planRow := TemplateImportPlanRow{
			RowNum:       row.RowNum,
			BookID:       row.BookID,
			Transcripcio: row.Bundle.Transcripcio,
			Persones:     make([]TemplateImportPlanPerson, 0, len(row.Bundle.Persones)),
			Atributs:     make([]TemplateImportPlanAttribute, 0, len(row.Bundle.Atributs)),
			Links:        make([]TemplateImportPlanLink, 0, len(row.Bundle.Atributs)),
		}
		for _, persona := range row.Bundle.Persones {
			planRow.Persones = append(planRow.Persones, TemplateImportPlanPerson{Persona: persona})
		}
		for _, attribute := range row.Bundle.Atributs {
			planRow.Atributs = append(planRow.Atributs, TemplateImportPlanAttribute{Attribute: attribute})
			planRow.Links = append(planRow.Links, TemplateImportPlanLink{
				Kind: "attribute",
				Key:  attribute.Clau,
			})
		}
		plan.Rows = append(plan.Rows, planRow)
		plan.Summary.Rows++
		plan.Summary.Persones += len(planRow.Persones)
		plan.Summary.Atributs += len(planRow.Atributs)
		plan.Summary.Links += len(planRow.Links)
		if planRow.BookID > 0 {
			books[planRow.BookID] = struct{}{}
		}
	}
	plan.Summary.Books = len(books)
	return plan
}

func newTemplateImportPersister(runtime db.TemplateImportRuntime) TemplateImportPersister {
	if runtime == nil {
		return legacyTemplateImportPersister{}
	}
	switch runtime.Engine() {
	case "postgres":
		return PostgresTemplateImportPersister{}
	case "sqlite":
		return SQLiteTemplateImportPersister{}
	default:
		return legacyTemplateImportPersister{}
	}
}

func (p PostgresTemplateImportPersister) Persist(plan *TemplateImportPlan, options TemplateImportPersistOptions) TemplateImportPersistResult {
	return persistTemplateImportPlanPostgresLegacy(plan, options)
}

func (p SQLiteTemplateImportPersister) Persist(plan *TemplateImportPlan, options TemplateImportPersistOptions) TemplateImportPersistResult {
	return persistTemplateImportPlanSQLiteLegacy(plan, options)
}

func (p legacyTemplateImportPersister) Persist(plan *TemplateImportPlan, options TemplateImportPersistOptions) TemplateImportPersistResult {
	return persistTemplateImportPlanLegacy(plan, options)
}

func persistTemplateImportPlanPostgresLegacy(plan *TemplateImportPlan, options TemplateImportPersistOptions) TemplateImportPersistResult {
	return persistTemplateImportPlanLegacy(plan, options)
}

func persistTemplateImportPlanSQLiteLegacy(plan *TemplateImportPlan, options TemplateImportPersistOptions) TemplateImportPersistResult {
	return persistTemplateImportPlanLegacy(plan, options)
}

func persistTemplateImportPlanLegacy(plan *TemplateImportPlan, options TemplateImportPersistOptions) TemplateImportPersistResult {
	result := TemplateImportPersistResult{UsedLegacy: true}
	if plan == nil || options.App == nil || options.Result == nil || len(plan.Rows) == 0 {
		return result
	}
	runtime := options.Runtime
	if runtime == nil {
		runtime = db.TemplateImportRuntimeFor(options.App.DB)
	}
	pending := templateImportPlanPendingCreates(plan)
	for len(pending) > 0 {
		batchEnd := templateImportCreateBatchSize
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

func templateImportPlanPendingCreates(plan *TemplateImportPlan) []templatePendingCreate {
	if plan == nil || len(plan.Rows) == 0 {
		return nil
	}
	pending := make([]templatePendingCreate, 0, len(plan.Rows))
	for _, row := range plan.Rows {
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
		pending = append(pending, templatePendingCreate{
			RowNum: row.RowNum,
			BookID: row.BookID,
			Bundle: bundle,
		})
	}
	return pending
}
