package core

import (
	"sort"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type sidefxComputeBookDebugMetrics struct {
	LlibreID                int
	ComputeDur              time.Duration
	GroupRegistresByBookDur time.Duration
	GroupPersonesByTransDur time.Duration
	GroupAtributsByTransDur time.Duration
	NormalizeStringsDur     time.Duration
	BuildIndexPayloadDur    time.Duration
	JSONSerializeDur        time.Duration
	DerivedStatsDur         time.Duration
	Registres               int
	Persones                int
	Atributs                int
	FieldEvaluations        int
}

type sidefxComputeDebugMetrics struct {
	GroupRegistresByBookDur time.Duration
	GroupPersonesByTransDur time.Duration
	GroupAtributsByTransDur time.Duration
	NormalizeStringsDur     time.Duration
	BuildIndexPayloadDur    time.Duration
	JSONSerializeDur        time.Duration
	DerivedStatsDur         time.Duration
	Registres               int
	Persones                int
	Atributs                int
	FieldEvaluations        int
	Books                   []sidefxComputeBookDebugMetrics
}

type csvImportDebugMetrics struct {
	Enabled                      bool
	Model                        string
	Scope                        string
	Rows                         int
	Books                        int
	ParseDur                     time.Duration
	ParseModelDur                time.Duration
	ParseValidationDur           time.Duration
	ParseHeaderReadDur           time.Duration
	ParseHeaderPrepareDur        time.Duration
	ParseRowContextDur           time.Duration
	ParseColumnsDur              time.Duration
	ParseConditionDur            time.Duration
	ParseTransformsDur           time.Duration
	ParseDateDur                 time.Duration
	ParseQualityDur              time.Duration
	ParsePersonBuildDur          time.Duration
	ParseConditionCalls          int
	ParseTransformCalls          int
	ParseDateCalls               int
	ParseQualityCalls            int
	ParsePersonBuildCalls        int
	ResolveDur                   time.Duration
	WriteDur                     time.Duration
	WritePrepareDur              time.Duration
	WritePageLookupDur           time.Duration
	WriteDuplicateCheckDur       time.Duration
	WriteTranscripcioInsertDur   time.Duration
	WritePersonaResolveDur       time.Duration
	WritePersonaPersistDur       time.Duration
	WriteLinksPersistDur         time.Duration
	WriteCommitDur               time.Duration
	WriteBulkBatches             int
	WriteBulkRows                int
	WriteBulkFallbacks           int
	WriteBulkTranscripcioBatches int
	WriteBulkPersonaBatches      int
	WriteBulkLinksBatches        int
	SidefxIndexacioStatsDur      time.Duration
	SidefxLoadRegistresDur       time.Duration
	SidefxLoadPersonesDur        time.Duration
	SidefxLoadAtributsDur        time.Duration
	SidefxComputeDur             time.Duration
	SidefxUpsertDur              time.Duration
	SidefxPageStatsDur           time.Duration
	SidefxIndexacioRegistres     int
	SidefxIndexacioPersones      int
	SidefxIndexacioAtributs      int
	SidefxComputeProfile         sidefxComputeDebugMetrics
	SidefxDur                    time.Duration
	TotalDur                     time.Duration
}

func newCSVImportDebugMetrics(model, scope string) csvImportDebugMetrics {
	return csvImportDebugMetrics{
		Enabled: IsDebugEnabled(),
		Model:   model,
		Scope:   scope,
	}
}

func (m *csvImportDebugMetrics) addParse(d time.Duration) {
	if m != nil && m.Enabled {
		m.ParseDur += d
	}
}

func (m *csvImportDebugMetrics) addParseModel(d time.Duration) {
	if m != nil && m.Enabled {
		m.ParseModelDur += d
	}
}

func (m *csvImportDebugMetrics) addParseValidation(d time.Duration) {
	if m != nil && m.Enabled {
		m.ParseValidationDur += d
	}
}

func (m *csvImportDebugMetrics) addParseHeaderRead(d time.Duration) {
	if m != nil && m.Enabled {
		m.ParseHeaderReadDur += d
	}
}

func (m *csvImportDebugMetrics) addParseHeaderPrepare(d time.Duration) {
	if m != nil && m.Enabled {
		m.ParseHeaderPrepareDur += d
	}
}

func (m *csvImportDebugMetrics) addParseRowContext(d time.Duration) {
	if m != nil && m.Enabled {
		m.ParseRowContextDur += d
	}
}

func (m *csvImportDebugMetrics) addParseColumns(d time.Duration) {
	if m != nil && m.Enabled {
		m.ParseColumnsDur += d
	}
}

func (m *csvImportDebugMetrics) addParseCondition(d time.Duration) {
	if m != nil && m.Enabled {
		m.ParseConditionDur += d
		m.ParseConditionCalls++
	}
}

func (m *csvImportDebugMetrics) addParseTransform(d time.Duration) {
	if m != nil && m.Enabled {
		m.ParseTransformsDur += d
		m.ParseTransformCalls++
	}
}

func (m *csvImportDebugMetrics) addParseDate(d time.Duration) {
	if m != nil && m.Enabled {
		m.ParseDateDur += d
		m.ParseDateCalls++
	}
}

func (m *csvImportDebugMetrics) addParseQuality(d time.Duration) {
	if m != nil && m.Enabled {
		m.ParseQualityDur += d
		m.ParseQualityCalls++
	}
}

func (m *csvImportDebugMetrics) addParsePersonBuild(d time.Duration) {
	if m != nil && m.Enabled {
		m.ParsePersonBuildDur += d
		m.ParsePersonBuildCalls++
	}
}

func (m *csvImportDebugMetrics) addResolve(d time.Duration) {
	if m != nil && m.Enabled {
		m.ResolveDur += d
	}
}

func (m *csvImportDebugMetrics) addWrite(d time.Duration) {
	if m != nil && m.Enabled {
		m.WriteDur += d
	}
}

func (m *csvImportDebugMetrics) addWritePrepare(d time.Duration) {
	if m != nil && m.Enabled {
		m.WritePrepareDur += d
		m.WriteDur += d
	}
}

func (m *csvImportDebugMetrics) addWritePageLookup(d time.Duration) {
	if m != nil && m.Enabled {
		m.WritePageLookupDur += d
		m.WriteDur += d
	}
}

func (m *csvImportDebugMetrics) addWriteDuplicateCheck(d time.Duration) {
	if m != nil && m.Enabled {
		m.WriteDuplicateCheckDur += d
		m.WriteDur += d
	}
}

func (m *csvImportDebugMetrics) addWriteTranscripcioInsert(d time.Duration) {
	if m != nil && m.Enabled {
		m.WriteTranscripcioInsertDur += d
		m.WriteDur += d
	}
}

func (m *csvImportDebugMetrics) addWritePersonaResolve(d time.Duration) {
	if m != nil && m.Enabled {
		m.WritePersonaResolveDur += d
		m.WriteDur += d
	}
}

func (m *csvImportDebugMetrics) addWritePersonaPersist(d time.Duration) {
	if m != nil && m.Enabled {
		m.WritePersonaPersistDur += d
		m.WriteDur += d
	}
}

func (m *csvImportDebugMetrics) addWriteLinksPersist(d time.Duration) {
	if m != nil && m.Enabled {
		m.WriteLinksPersistDur += d
		m.WriteDur += d
	}
}

func (m *csvImportDebugMetrics) addWriteCommit(d time.Duration) {
	if m != nil && m.Enabled {
		m.WriteCommitDur += d
		m.WriteDur += d
	}
}

func (m *csvImportDebugMetrics) addWriteBulkBatch(rows int) {
	if m != nil && m.Enabled {
		m.WriteBulkBatches++
		m.WriteBulkRows += rows
	}
}

func (m *csvImportDebugMetrics) addWriteBulkFallback() {
	if m != nil && m.Enabled {
		m.WriteBulkFallbacks++
	}
}

func (m *csvImportDebugMetrics) addWriteBulkStatementBatches(transcripcions, persones, links int) {
	if m != nil && m.Enabled {
		m.WriteBulkTranscripcioBatches += transcripcions
		m.WriteBulkPersonaBatches += persones
		m.WriteBulkLinksBatches += links
	}
}

func (m *csvImportDebugMetrics) addSidefx(d time.Duration) {
	if m != nil && m.Enabled {
		m.SidefxDur += d
	}
}

func (m *csvImportDebugMetrics) addSidefxIndexacio(metrics llibreIndexacioRecalcMetrics) {
	if m == nil || !m.Enabled {
		return
	}
	m.SidefxIndexacioStatsDur += metrics.IndexacioStatsDur()
	m.SidefxLoadRegistresDur += metrics.LoadRegistresDur
	m.SidefxLoadPersonesDur += metrics.LoadPersonesDur
	m.SidefxLoadAtributsDur += metrics.LoadAtributsDur
	m.SidefxComputeDur += metrics.ComputeDur
	m.SidefxUpsertDur += metrics.UpsertDur
	m.SidefxPageStatsDur += metrics.PageStatsDur
	m.SidefxIndexacioRegistres += metrics.TotalRegistres
	m.SidefxIndexacioPersones += metrics.TotalPersones
	m.SidefxIndexacioAtributs += metrics.TotalAtributs
	m.SidefxComputeProfile.GroupRegistresByBookDur += metrics.ComputeGroupRegistresDur
	m.SidefxComputeProfile.GroupPersonesByTransDur += metrics.ComputeGroupPersonesMapDur
	m.SidefxComputeProfile.GroupAtributsByTransDur += metrics.ComputeGroupAtributsMapDur
	m.SidefxComputeProfile.NormalizeStringsDur += metrics.ComputeNormalizeStringsDur
	m.SidefxComputeProfile.BuildIndexPayloadDur += metrics.ComputeBuildPayloadDur
	m.SidefxComputeProfile.JSONSerializeDur += metrics.ComputeJSONSerializeDur
	m.SidefxComputeProfile.DerivedStatsDur += metrics.ComputeStatsDemografiaDur
	m.SidefxComputeProfile.Registres += metrics.TotalRegistres
	m.SidefxComputeProfile.Persones += metrics.TotalPersones
	m.SidefxComputeProfile.Atributs += metrics.TotalAtributs
	m.SidefxComputeProfile.FieldEvaluations += metrics.ComputeFieldEvaluations
	m.SidefxComputeProfile.Books = append(m.SidefxComputeProfile.Books, sidefxComputeBookDebugMetrics{
		LlibreID:                metrics.LlibreID,
		ComputeDur:              metrics.ComputeDur,
		GroupRegistresByBookDur: metrics.ComputeGroupRegistresDur,
		GroupPersonesByTransDur: metrics.ComputeGroupPersonesMapDur,
		GroupAtributsByTransDur: metrics.ComputeGroupAtributsMapDur,
		NormalizeStringsDur:     metrics.ComputeNormalizeStringsDur,
		BuildIndexPayloadDur:    metrics.ComputeBuildPayloadDur,
		JSONSerializeDur:        metrics.ComputeJSONSerializeDur,
		DerivedStatsDur:         metrics.ComputeStatsDemografiaDur,
		Registres:               metrics.TotalRegistres,
		Persones:                metrics.TotalPersones,
		Atributs:                metrics.TotalAtributs,
		FieldEvaluations:        metrics.ComputeFieldEvaluations,
	})
	m.SidefxDur += metrics.TotalDur()
}

func (m *csvImportDebugMetrics) incRows() {
	if m != nil && m.Enabled {
		m.Rows++
	}
}

func (m *csvImportDebugMetrics) finalize(bookCount int, total time.Duration) {
	if m != nil && m.Enabled {
		m.Books = bookCount
		m.TotalDur = total
	}
}

func (a *App) logCSVImportDebug(actorID int, result csvImportResult) {
	if a == nil {
		return
	}
	if result.Debug.Enabled {
		Debugf(
			"registre import model=%s scope=%s actor=%d rows=%d books=%d created=%d updated=%d failed=%d parse_dur=%s parse_model_dur=%s parse_validation_dur=%s parse_header_read_dur=%s parse_header_prepare_dur=%s parse_row_context_dur=%s parse_columns_dur=%s parse_condition_dur=%s parse_condition_calls=%d parse_transforms_dur=%s parse_transform_calls=%d parse_date_dur=%s parse_date_calls=%d parse_quality_dur=%s parse_quality_calls=%d parse_person_build_dur=%s parse_person_build_calls=%d resolve_dur=%s write_dur=%s write_prepare_dur=%s write_page_lookup_dur=%s write_duplicate_check_dur=%s write_transcripcio_insert_dur=%s write_persona_resolve_dur=%s write_persona_persist_dur=%s write_links_persist_dur=%s write_commit_dur=%s write_bulk_batches=%d write_bulk_rows=%d write_bulk_fallbacks=%d write_bulk_transcripcio_batches=%d write_bulk_persona_batches=%d write_bulk_links_batches=%d sidefx_dur=%s sidefx_indexacio_stats_dur=%s sidefx_load_registres_dur=%s sidefx_load_persones_dur=%s sidefx_load_atributs_dur=%s sidefx_compute_dur=%s sidefx_upsert_dur=%s sidefx_page_stats_dur=%s sidefx_indexacio_registres=%d sidefx_indexacio_persones=%d sidefx_indexacio_atributs=%d total_dur=%s",
			result.Debug.Model,
			result.Debug.Scope,
			actorID,
			result.Debug.Rows,
			result.Debug.Books,
			result.Created,
			result.Updated,
			result.Failed,
			result.Debug.ParseDur,
			result.Debug.ParseModelDur,
			result.Debug.ParseValidationDur,
			result.Debug.ParseHeaderReadDur,
			result.Debug.ParseHeaderPrepareDur,
			result.Debug.ParseRowContextDur,
			result.Debug.ParseColumnsDur,
			result.Debug.ParseConditionDur,
			result.Debug.ParseConditionCalls,
			result.Debug.ParseTransformsDur,
			result.Debug.ParseTransformCalls,
			result.Debug.ParseDateDur,
			result.Debug.ParseDateCalls,
			result.Debug.ParseQualityDur,
			result.Debug.ParseQualityCalls,
			result.Debug.ParsePersonBuildDur,
			result.Debug.ParsePersonBuildCalls,
			result.Debug.ResolveDur,
			result.Debug.WriteDur,
			result.Debug.WritePrepareDur,
			result.Debug.WritePageLookupDur,
			result.Debug.WriteDuplicateCheckDur,
			result.Debug.WriteTranscripcioInsertDur,
			result.Debug.WritePersonaResolveDur,
			result.Debug.WritePersonaPersistDur,
			result.Debug.WriteLinksPersistDur,
			result.Debug.WriteCommitDur,
			result.Debug.WriteBulkBatches,
			result.Debug.WriteBulkRows,
			result.Debug.WriteBulkFallbacks,
			result.Debug.WriteBulkTranscripcioBatches,
			result.Debug.WriteBulkPersonaBatches,
			result.Debug.WriteBulkLinksBatches,
			result.Debug.SidefxDur,
			result.Debug.SidefxIndexacioStatsDur,
			result.Debug.SidefxLoadRegistresDur,
			result.Debug.SidefxLoadPersonesDur,
			result.Debug.SidefxLoadAtributsDur,
			result.Debug.SidefxComputeDur,
			result.Debug.SidefxUpsertDur,
			result.Debug.SidefxPageStatsDur,
			result.Debug.SidefxIndexacioRegistres,
			result.Debug.SidefxIndexacioPersones,
			result.Debug.SidefxIndexacioAtributs,
			result.Debug.TotalDur,
		)
	}
	if result.WritePrepareBreakdown != nil && IsImportProfileEnabled() {
		Debugf(
			"write_prepare_breakdown build_transcripcions_batch_dur=%s build_persones_batch_dur=%s build_links_batch_dur=%s prepare_maps_slices_dur=%s prealloc_dur=%s transcripcions=%d persones=%d links=%d atributs=%d batches=%d",
			result.WritePrepareBreakdown.BuildTranscripcionsBatchDur,
			result.WritePrepareBreakdown.BuildPersonesBatchDur,
			result.WritePrepareBreakdown.BuildLinksBatchDur,
			result.WritePrepareBreakdown.PrepareMapsSlicesDur,
			result.WritePrepareBreakdown.PreallocDur,
			result.WritePrepareBreakdown.TranscripcionsCount,
			result.WritePrepareBreakdown.PersonesCount,
			result.WritePrepareBreakdown.LinksCount,
			result.WritePrepareBreakdown.AtributsCount,
			result.WritePrepareBreakdown.Batches,
		)
	}
	if result.ImportPhaseGaps != nil && IsImportProfileEnabled() {
		Debugf(
			"import_phase_gap_summary parse_to_write_prepare_gap=%s write_prepare_to_duplicate_check_gap=%s duplicate_check_to_inserts_gap=%s write_to_sidefx_gap=%s duplicate_before_write_prepare_count=%d",
			result.ImportPhaseGaps.ParseToWritePrepareGap,
			result.ImportPhaseGaps.WritePrepareToDuplicateCheckGap,
			result.ImportPhaseGaps.DuplicateCheckToInsertsGap,
			result.ImportPhaseGaps.WriteToSidefxGap,
			result.ImportPhaseGaps.DuplicateBeforeWritePrepareCount,
		)
	}
	if len(result.Debug.SidefxComputeProfile.Books) > 0 && IsImportProfileEnabled() {
		Debugf(
			"sidefx_compute_summary books=%d total_compute_dur=%s group_registres_by_book_dur=%s group_persones_by_transcripcio_dur=%s group_atributs_by_transcripcio_dur=%s normalize_strings_dur=%s build_index_payload_dur=%s json_serialize_dur=%s derived_stats_dur=%s registres=%d persones=%d atributs=%d field_evaluations=%d",
			len(result.Debug.SidefxComputeProfile.Books),
			result.Debug.SidefxComputeDur,
			result.Debug.SidefxComputeProfile.GroupRegistresByBookDur,
			result.Debug.SidefxComputeProfile.GroupPersonesByTransDur,
			result.Debug.SidefxComputeProfile.GroupAtributsByTransDur,
			result.Debug.SidefxComputeProfile.NormalizeStringsDur,
			result.Debug.SidefxComputeProfile.BuildIndexPayloadDur,
			result.Debug.SidefxComputeProfile.JSONSerializeDur,
			result.Debug.SidefxComputeProfile.DerivedStatsDur,
			result.Debug.SidefxComputeProfile.Registres,
			result.Debug.SidefxComputeProfile.Persones,
			result.Debug.SidefxComputeProfile.Atributs,
			result.Debug.SidefxComputeProfile.FieldEvaluations,
		)
		books := append([]sidefxComputeBookDebugMetrics(nil), result.Debug.SidefxComputeProfile.Books...)
		sort.Slice(books, func(i, j int) bool {
			if books[i].ComputeDur == books[j].ComputeDur {
				return books[i].LlibreID < books[j].LlibreID
			}
			return books[i].ComputeDur > books[j].ComputeDur
		})
		limit := 5
		if len(books) < limit {
			limit = len(books)
		}
		for i := 0; i < limit; i++ {
			book := books[i]
			Debugf(
				"sidefx_compute_top_book rank=%d llibre_id=%d compute_dur=%s group_registres_by_book_dur=%s group_persones_by_transcripcio_dur=%s group_atributs_by_transcripcio_dur=%s normalize_strings_dur=%s build_index_payload_dur=%s json_serialize_dur=%s derived_stats_dur=%s registres=%d persones=%d atributs=%d field_evaluations=%d",
				i+1,
				book.LlibreID,
				book.ComputeDur,
				book.GroupRegistresByBookDur,
				book.GroupPersonesByTransDur,
				book.GroupAtributsByTransDur,
				book.NormalizeStringsDur,
				book.BuildIndexPayloadDur,
				book.JSONSerializeDur,
				book.DerivedStatsDur,
				book.Registres,
				book.Persones,
				book.Atributs,
				book.FieldEvaluations,
			)
		}
	}
	a.logPostgresStagingProfile(result)
}

func (a *App) logPostgresStagingProfile(result csvImportResult) {
	if !IsPostgresStagingProfileEnabled() {
		return
	}
	profile := db.DrainPostgresTemplateImportStagingProfile()
	if len(profile.Batches) == 0 {
		return
	}
	PostgresStagingProfilef(
		"postgres_staging_profile_summary batches=%d rows=%d persones=%d atributs=%d create_drop_temp_tables_dur=%s build_rows_dur=%s copy_raw_staging_dur=%s insert_raw_final_dur=%s copy_persones_staging_dur=%s insert_persones_final_dur=%s copy_atributs_staging_dur=%s insert_atributs_final_dur=%s commit_dur=%s unaccounted_dur=%s total_dur=%s",
		len(profile.Batches),
		profile.Rows,
		profile.Persones,
		profile.Atributs,
		profile.CreateDropTempTablesDur,
		profile.BuildRowsDur,
		profile.CopyRawStagingDur,
		profile.InsertRawFinalDur,
		profile.CopyPersonesStagingDur,
		profile.InsertPersonesFinalDur,
		profile.CopyAtributsStagingDur,
		profile.InsertAtributsFinalDur,
		profile.CommitDur,
		profile.UnaccountedDur,
		profile.TotalDur,
	)
	if len(profile.Batches) == 1 && IsPostgresStagingWholeImportEnabled() {
		logPostgresStagingTopPhases(profile.Batches[0])
		return
	}
	batches := append([]db.PostgresTemplateImportStagingBatchMetrics(nil), profile.Batches...)
	sort.Slice(batches, func(i, j int) bool {
		if batches[i].TotalDur == batches[j].TotalDur {
			return batches[i].Index < batches[j].Index
		}
		return batches[i].TotalDur > batches[j].TotalDur
	})
	limit := 5
	if len(batches) < limit {
		limit = len(batches)
	}
	for i := 0; i < limit; i++ {
		batch := batches[i]
		PostgresStagingProfilef(
			"postgres_staging_profile_top_batch rank=%d batch=%d range=%d-%d rows=%d persones=%d atributs=%d create_drop_temp_tables_dur=%s build_rows_dur=%s copy_raw_staging_dur=%s insert_raw_final_dur=%s copy_persones_staging_dur=%s insert_persones_final_dur=%s copy_atributs_staging_dur=%s insert_atributs_final_dur=%s commit_dur=%s unaccounted_dur=%s total_dur=%s",
			i+1,
			batch.Index,
			batch.RangeStart,
			batch.RangeEnd,
			batch.Rows,
			batch.Persones,
			batch.Atributs,
			batch.CreateDropTempTablesDur,
			batch.BuildRowsDur,
			batch.CopyRawStagingDur,
			batch.InsertRawFinalDur,
			batch.CopyPersonesStagingDur,
			batch.InsertPersonesFinalDur,
			batch.CopyAtributsStagingDur,
			batch.InsertAtributsFinalDur,
			batch.CommitDur,
			batch.UnaccountedDur,
			batch.TotalDur,
		)
	}
}

func logPostgresStagingTopPhases(batch db.PostgresTemplateImportStagingBatchMetrics) {
	type postgresStagingPhase struct {
		Name string
		Dur  time.Duration
	}
	phases := []postgresStagingPhase{
		{Name: "create_drop_temp_tables", Dur: batch.CreateDropTempTablesDur},
		{Name: "build_rows", Dur: batch.BuildRowsDur},
		{Name: "copy_raw_staging", Dur: batch.CopyRawStagingDur},
		{Name: "insert_raw_final", Dur: batch.InsertRawFinalDur},
		{Name: "copy_persones_staging", Dur: batch.CopyPersonesStagingDur},
		{Name: "insert_persones_final", Dur: batch.InsertPersonesFinalDur},
		{Name: "copy_atributs_staging", Dur: batch.CopyAtributsStagingDur},
		{Name: "insert_atributs_final", Dur: batch.InsertAtributsFinalDur},
		{Name: "commit", Dur: batch.CommitDur},
		{Name: "unaccounted", Dur: batch.UnaccountedDur},
	}
	sort.Slice(phases, func(i, j int) bool {
		if phases[i].Dur == phases[j].Dur {
			return phases[i].Name < phases[j].Name
		}
		return phases[i].Dur > phases[j].Dur
	})
	limit := 5
	if len(phases) < limit {
		limit = len(phases)
	}
	for i := 0; i < limit; i++ {
		phase := phases[i]
		PostgresStagingProfilef(
			"postgres_staging_profile_top_phase rank=%d phase=%s rows=%d persones=%d atributs=%d dur=%s total_dur=%s",
			i+1,
			phase.Name,
			batch.Rows,
			batch.Persones,
			batch.Atributs,
			phase.Dur,
			batch.TotalDur,
		)
	}
}
