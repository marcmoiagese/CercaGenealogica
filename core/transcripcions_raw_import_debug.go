package core

import "time"

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
	if a == nil || !result.Debug.Enabled {
		return
	}
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
