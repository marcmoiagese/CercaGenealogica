package core

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type llibreIndexacioRecalcMetrics struct {
	LoadRegistresDur time.Duration
	LoadPersonesDur  time.Duration
	LoadAtributsDur  time.Duration
	ComputeDur       time.Duration
	UpsertDur        time.Duration
	PageStatsDur     time.Duration
	TotalRegistres   int
	TotalPersones    int
	TotalAtributs    int
}

func (m llibreIndexacioRecalcMetrics) IndexacioStatsDur() time.Duration {
	return m.LoadRegistresDur + m.LoadPersonesDur + m.LoadAtributsDur + m.ComputeDur + m.UpsertDur
}

func (m llibreIndexacioRecalcMetrics) TotalDur() time.Duration {
	return m.IndexacioStatsDur() + m.PageStatsDur
}

type LlibreIndexacioView struct {
	TotalRegistres int
	Percentatge    int
	ColorClass     string
}

func (a *App) buildLlibresIndexacioViews(llibres []db.LlibreRow) map[string]LlibreIndexacioView {
	res := map[string]LlibreIndexacioView{}
	if len(llibres) == 0 {
		return res
	}
	ids := make([]int, 0, len(llibres))
	for _, llibre := range llibres {
		ids = append(ids, llibre.ID)
	}
	stats, err := a.DB.GetLlibresIndexacioStats(ids)
	if err != nil {
		stats = map[int]db.LlibreIndexacioStats{}
	}
	for _, llibre := range llibres {
		stat, ok := stats[llibre.ID]
		if !ok {
			if recalced, err := a.recalcLlibreIndexacioStats(llibre.ID); err == nil && recalced != nil {
				stat = *recalced
				ok = true
			}
		}
		totalRegistres := stat.TotalRegistres
		if count, err := a.DB.CountTranscripcionsRaw(llibre.ID, db.TranscripcioFilter{}); err == nil {
			totalRegistres = count
			if ok && stat.TotalRegistres != count {
				stat.TotalRegistres = count
			}
		}
		view := LlibreIndexacioView{
			TotalRegistres: totalRegistres,
			Percentatge:    stat.Percentatge,
			ColorClass:     indexacioColorClass(stat.Percentatge),
		}
		res[strconv.Itoa(llibre.ID)] = view
	}
	return res
}

func indexacioColorClass(percent int) string {
	switch {
	case percent >= 80:
		return "verd"
	case percent >= 60:
		return "groc"
	case percent >= 30:
		return "taronja"
	default:
		return "rosa"
	}
}

func (a *App) recalcLlibreIndexacioStats(llibreID int) (*db.LlibreIndexacioStats, error) {
	stats, _, err := a.recalcLlibreIndexacioStatsWithMetrics(llibreID)
	return stats, err
}

func (a *App) recalcLlibreIndexacioStatsWithMetrics(llibreID int) (*db.LlibreIndexacioStats, llibreIndexacioRecalcMetrics, error) {
	metrics := llibreIndexacioRecalcMetrics{}
	llibre, err := a.DB.GetLlibre(llibreID)
	if err != nil {
		return nil, metrics, err
	}
	if llibre == nil {
		return nil, metrics, nil
	}
	bookType := normalizeIndexerBookType(llibre.TipusLlibre)
	fields := indexerContentFields(indexerSchema(bookType))
	stats := &db.LlibreIndexacioStats{LlibreID: llibreID}
	loadStart := time.Now()
	registres, err := a.DB.ListTranscripcionsRaw(llibreID, db.TranscripcioFilter{Limit: -1})
	metrics.LoadRegistresDur = time.Since(loadStart)
	if err != nil {
		return nil, metrics, err
	}
	stats.TotalRegistres = len(registres)
	metrics.TotalRegistres = len(registres)
	if stats.TotalRegistres == 0 || len(fields) == 0 {
		stats.TotalCamps = 0
		stats.CampsEmplenats = 0
		stats.Percentatge = 0
		upsertStart := time.Now()
		err = a.DB.UpsertLlibreIndexacioStats(stats)
		metrics.UpsertDur = time.Since(upsertStart)
		return stats, metrics, err
	}
	registreIDs := make([]int, 0, len(registres))
	for _, registre := range registres {
		registreIDs = append(registreIDs, registre.ID)
	}
	importRuntime := db.TemplateImportRuntimeFor(a.DB)
	personesByRegistre := map[int][]db.TranscripcioPersonaRaw{}
	loadStart = time.Now()
	personesByRegistre, err = importRuntime.LoadPersonesByLlibreID(llibreID, registreIDs)
	metrics.LoadPersonesDur = time.Since(loadStart)
	if err != nil {
		personesByRegistre = make(map[int][]db.TranscripcioPersonaRaw, len(registreIDs))
		fallbackStart := time.Now()
		for _, registreID := range registreIDs {
			persones, listErr := a.DB.ListTranscripcioPersones(registreID)
			if listErr != nil {
				return nil, metrics, listErr
			}
			personesByRegistre[registreID] = persones
		}
		metrics.LoadPersonesDur = time.Since(fallbackStart)
	}
	for _, persones := range personesByRegistre {
		metrics.TotalPersones += len(persones)
	}
	atributsByRegistre := map[int][]db.TranscripcioAtributRaw{}
	loadStart = time.Now()
	atributsByRegistre, err = importRuntime.LoadAtributsByLlibreID(llibreID, registreIDs)
	metrics.LoadAtributsDur = time.Since(loadStart)
	if err != nil {
		atributsByRegistre = make(map[int][]db.TranscripcioAtributRaw, len(registreIDs))
		fallbackStart := time.Now()
		for _, registreID := range registreIDs {
			atributs, listErr := a.DB.ListTranscripcioAtributs(registreID)
			if listErr != nil {
				return nil, metrics, listErr
			}
			atributsByRegistre[registreID] = atributs
		}
		metrics.LoadAtributsDur = time.Since(fallbackStart)
	}
	for _, atributs := range atributsByRegistre {
		metrics.TotalAtributs += len(atributs)
	}
	stats.TotalCamps = len(fields) * stats.TotalRegistres
	computeStart := time.Now()
	for _, registre := range registres {
		persones := personesByRegistre[registre.ID]
		atributs := atributsByRegistre[registre.ID]
		cache := map[string]*db.TranscripcioPersonaRaw{}
		for _, field := range fields {
			if indexerFieldValue(field, registre, persones, atributs, cache) != "" {
				stats.CampsEmplenats++
			}
		}
	}
	metrics.ComputeDur = time.Since(computeStart)
	stats.Percentatge = int(math.Round(float64(stats.CampsEmplenats) * 100 / float64(stats.TotalCamps)))
	if stats.Percentatge < 0 {
		stats.Percentatge = 0
	}
	if stats.Percentatge > 100 {
		stats.Percentatge = 100
	}
	upsertStart := time.Now()
	if err := a.DB.UpsertLlibreIndexacioStats(stats); err != nil {
		metrics.UpsertDur = time.Since(upsertStart)
		return stats, metrics, err
	}
	metrics.UpsertDur = time.Since(upsertStart)
	if llibre.IndexacioCompleta {
		pageStatsStart := time.Now()
		if err := a.DB.RecalcTranscripcionsRawPageStats(llibreID); err != nil {
			metrics.PageStatsDur = time.Since(pageStatsStart)
			Errorf("Error recalculant registres per pagina del llibre %d: %v", llibreID, err)
		} else {
			metrics.PageStatsDur = time.Since(pageStatsStart)
		}
	}
	return stats, metrics, nil
}

func indexerContentFields(fields []indexerField) []indexerField {
	res := make([]indexerField, 0, len(fields))
	for _, field := range fields {
		if field.Key == "" || field.Target == "" {
			continue
		}
		if isIndexerQualityField(field) {
			continue
		}
		res = append(res, field)
	}
	return res
}

func isIndexerQualityField(field indexerField) bool {
	if field.Key == "qualitat_general" || field.RawField == "data_acte_estat" {
		return true
	}
	if field.Target == "person" && strings.HasSuffix(field.PersonField, "_estat") {
		return true
	}
	if field.Target == "raw" && strings.HasSuffix(field.RawField, "_estat") {
		return true
	}
	if field.Target == "attr" && (field.AttrType == "estat" || strings.HasSuffix(field.AttrKey, "_estat")) {
		return true
	}
	return false
}

func indexerFieldValue(field indexerField, raw db.TranscripcioRaw, persones []db.TranscripcioPersonaRaw, attrs []db.TranscripcioAtributRaw, cache map[string]*db.TranscripcioPersonaRaw) string {
	switch field.Target {
	case "raw":
		return strings.TrimSpace(rawFieldValue(raw, field.RawField))
	case "attr":
		return strings.TrimSpace(attrValueByKeysRaw(attrs, field.AttrKey))
	case "person":
		person := personForField(persones, field.Role, field.PersonKey, cache)
		return strings.TrimSpace(personFieldValue(person, field.PersonField))
	default:
		return ""
	}
}
