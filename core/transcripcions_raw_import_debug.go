package core

import "time"

type csvImportDebugMetrics struct {
	Enabled    bool
	Model      string
	Scope      string
	Rows       int
	Books      int
	ParseDur   time.Duration
	ResolveDur time.Duration
	WriteDur   time.Duration
	SidefxDur  time.Duration
	TotalDur   time.Duration
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

func (m *csvImportDebugMetrics) addSidefx(d time.Duration) {
	if m != nil && m.Enabled {
		m.SidefxDur += d
	}
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
		"registre import model=%s scope=%s actor=%d rows=%d books=%d created=%d updated=%d failed=%d parse_dur=%s resolve_dur=%s write_dur=%s sidefx_dur=%s total_dur=%s",
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
		result.Debug.SidefxDur,
		result.Debug.TotalDur,
	)
}
