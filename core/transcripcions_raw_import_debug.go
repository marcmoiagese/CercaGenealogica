package core

import "time"

type csvImportDebugMetrics struct {
	Enabled                      bool
	Model                        string
	Scope                        string
	Rows                         int
	Books                        int
	ParseDur                     time.Duration
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
		"registre import model=%s scope=%s actor=%d rows=%d books=%d created=%d updated=%d failed=%d parse_dur=%s resolve_dur=%s write_dur=%s write_prepare_dur=%s write_page_lookup_dur=%s write_duplicate_check_dur=%s write_transcripcio_insert_dur=%s write_persona_resolve_dur=%s write_persona_persist_dur=%s write_links_persist_dur=%s write_commit_dur=%s write_bulk_batches=%d write_bulk_rows=%d write_bulk_fallbacks=%d write_bulk_transcripcio_batches=%d write_bulk_persona_batches=%d write_bulk_links_batches=%d sidefx_dur=%s sidefx_indexacio_stats_dur=%s sidefx_load_registres_dur=%s sidefx_load_persones_dur=%s sidefx_load_atributs_dur=%s sidefx_compute_dur=%s sidefx_upsert_dur=%s sidefx_page_stats_dur=%s sidefx_indexacio_registres=%d sidefx_indexacio_persones=%d sidefx_indexacio_atributs=%d total_dur=%s",
		result.Debug.Model,
		result.Debug.Scope,
		actorID,
		result.Debug.Rows,
		result.Debug.Books,
		result.Created,
		result.Updated,
		result.Failed,
		result.Debug.ParseDur,
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
