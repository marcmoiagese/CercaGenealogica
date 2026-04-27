package core

import (
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type llibreIndexacioRecalcMetrics struct {
	LlibreID                   int
	LoadRegistresDur           time.Duration
	LoadPersonesDur            time.Duration
	LoadAtributsDur            time.Duration
	ComputeDur                 time.Duration
	ComputeGroupRegistresDur   time.Duration
	ComputeGroupPersonesMapDur time.Duration
	ComputeGroupAtributsMapDur time.Duration
	ComputeGroupPersonesDur    time.Duration
	ComputeGroupAtributsDur    time.Duration
	ComputeNormalizeStringsDur time.Duration
	ComputeBuildPayloadDur     time.Duration
	ComputeJSONSerializeDur    time.Duration
	ComputeStatsDemografiaDur  time.Duration
	UpsertDur                  time.Duration
	PageStatsDur               time.Duration
	TotalRegistres             int
	TotalPersones              int
	TotalAtributs              int
	ComputeFieldEvaluations    int
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

type compiledIndexField struct {
	Field        indexerField
	AttrKeyNorm  string
	PersonLookup string
}

type indexAttrLookupValue struct {
	Value string
	Found bool
}

type indexPayloadFastContext struct {
	raw                db.TranscripcioRaw
	personesByRole     map[string][]*db.TranscripcioPersonaRaw
	atributsByKey      map[string]indexAttrLookupValue
	personSelection    map[string]*db.TranscripcioPersonaRaw
	rawValueCache      map[string]string
	normalizedValCache map[string]string
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
	metrics := llibreIndexacioRecalcMetrics{LlibreID: llibreID}
	llibre, err := a.DB.GetLlibre(llibreID)
	if err != nil {
		return nil, metrics, err
	}
	if llibre == nil {
		return nil, metrics, nil
	}
	bookType := normalizeIndexerBookType(llibre.TipusLlibre)
	fields := indexerContentFields(indexerSchema(bookType))
	compiledFields := compileIndexFields(fields)
	stats := &db.LlibreIndexacioStats{LlibreID: llibreID}
	loadStart := time.Now()
	registres, err := a.DB.ListTranscripcionsRaw(llibreID, db.TranscripcioFilter{Limit: -1})
	metrics.LoadRegistresDur = time.Since(loadStart)
	if err != nil {
		return nil, metrics, err
	}
	stats.TotalRegistres = len(registres)
	metrics.TotalRegistres = len(registres)
	if stats.TotalRegistres == 0 || len(compiledFields) == 0 {
		stats.TotalCamps = 0
		stats.CampsEmplenats = 0
		stats.Percentatge = 0
		upsertStart := time.Now()
		err = a.DB.UpsertLlibreIndexacioStats(stats)
		metrics.UpsertDur = time.Since(upsertStart)
		return stats, metrics, err
	}
	registreIDs := make([]int, 0, len(registres))
	groupStart := time.Now()
	for _, registre := range registres {
		registreIDs = append(registreIDs, registre.ID)
	}
	metrics.ComputeGroupRegistresDur += time.Since(groupStart)
	importRuntime := db.TemplateImportRuntimeFor(a.DB)
	postgresDebug := IsDebugEnabled() && strings.EqualFold(importRuntime.Engine(), "postgres")
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
		groupStart = time.Now()
		metrics.TotalPersones += len(persones)
		metrics.ComputeGroupPersonesMapDur += time.Since(groupStart)
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
		groupStart = time.Now()
		metrics.TotalAtributs += len(atributs)
		metrics.ComputeGroupAtributsMapDur += time.Since(groupStart)
	}
	stats.TotalCamps = len(compiledFields) * stats.TotalRegistres
	computeStart := time.Now()
	for _, registre := range registres {
		persones := personesByRegistre[registre.ID]
		atributs := atributsByRegistre[registre.ID]
		stats.CampsEmplenats += buildIndexPayloadFast(registre, compiledFields, persones, atributs, &metrics)
	}
	metrics.ComputeDur = time.Since(computeStart)
	if postgresDebug && IsImportProfileEnabled() {
		Debugf(
			"sidefx_compute_breakdown llibre_id=%d group_registres_dur=%s group_persones_dur=%s group_atributs_dur=%s normalize_strings_dur=%s build_payload_dur=%s stats_demografia_dur=%s registres=%d persones=%d atributs=%d field_evaluations=%d",
			llibreID,
			metrics.ComputeGroupRegistresDur,
			metrics.ComputeGroupPersonesDur,
			metrics.ComputeGroupAtributsDur,
			metrics.ComputeNormalizeStringsDur,
			metrics.ComputeBuildPayloadDur,
			metrics.ComputeStatsDemografiaDur,
			metrics.TotalRegistres,
			metrics.TotalPersones,
			metrics.TotalAtributs,
			metrics.ComputeFieldEvaluations,
		)
	}
	derivedStatsStart := time.Now()
	stats.Percentatge = int(math.Round(float64(stats.CampsEmplenats) * 100 / float64(stats.TotalCamps)))
	if stats.Percentatge < 0 {
		stats.Percentatge = 0
	}
	if stats.Percentatge > 100 {
		stats.Percentatge = 100
	}
	metrics.ComputeStatsDemografiaDur += time.Since(derivedStatsStart)
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

func compileIndexFields(fields []indexerField) []compiledIndexField {
	compiled := make([]compiledIndexField, 0, len(fields))
	for _, field := range fields {
		entry := compiledIndexField{Field: field}
		if field.Target == "attr" {
			entry.AttrKeyNorm = normalizeRole(field.AttrKey)
		}
		if field.Target == "person" {
			entry.PersonLookup = strings.TrimSpace(field.Role) + "\x1f" + strconv.Itoa(personKeyIndex(field.PersonKey))
		}
		compiled = append(compiled, entry)
	}
	return compiled
}

func buildIndexPersonesByTranscripcio(persones []db.TranscripcioPersonaRaw) map[string][]*db.TranscripcioPersonaRaw {
	if len(persones) == 0 {
		return map[string][]*db.TranscripcioPersonaRaw{}
	}
	byRole := make(map[string][]*db.TranscripcioPersonaRaw, len(persones))
	for i := range persones {
		byRole[persones[i].Rol] = append(byRole[persones[i].Rol], &persones[i])
	}
	for role := range byRole {
		sort.Slice(byRole[role], func(i, j int) bool {
			return byRole[role][i].ID < byRole[role][j].ID
		})
	}
	return byRole
}

func buildIndexAtributsByTranscripcio(attrs []db.TranscripcioAtributRaw) map[string]indexAttrLookupValue {
	if len(attrs) == 0 {
		return map[string]indexAttrLookupValue{}
	}
	byKey := make(map[string]indexAttrLookupValue, len(attrs))
	for _, attr := range attrs {
		key := normalizeRole(attr.Clau)
		if key == "" {
			continue
		}
		if _, ok := byKey[key]; ok {
			continue
		}
		byKey[key] = indexAttrLookupValue{
			Value: attrValueStringRaw(attr),
			Found: true,
		}
	}
	return byKey
}

func buildIndexPayloadFast(raw db.TranscripcioRaw, fields []compiledIndexField, persones []db.TranscripcioPersonaRaw, attrs []db.TranscripcioAtributRaw, metrics *llibreIndexacioRecalcMetrics) int {
	personesGroupStart := time.Now()
	personesByRole := buildIndexPersonesByTranscripcio(persones)
	if metrics != nil {
		metrics.ComputeGroupPersonesDur += time.Since(personesGroupStart)
	}
	atributsGroupStart := time.Now()
	atributsByKey := buildIndexAtributsByTranscripcio(attrs)
	if metrics != nil {
		metrics.ComputeGroupAtributsDur += time.Since(atributsGroupStart)
	}
	ctx := indexPayloadFastContext{
		raw:                raw,
		personesByRole:     personesByRole,
		atributsByKey:      atributsByKey,
		personSelection:    make(map[string]*db.TranscripcioPersonaRaw, len(personesByRole)),
		rawValueCache:      make(map[string]string, 4),
		normalizedValCache: make(map[string]string, len(fields)),
	}
	filled := 0
	for _, field := range fields {
		if metrics != nil {
			metrics.ComputeFieldEvaluations++
		}
		buildPayloadStart := time.Now()
		val := buildIndexPayloadFastValue(ctx, field, metrics)
		if metrics != nil {
			metrics.ComputeBuildPayloadDur += time.Since(buildPayloadStart)
		}
		normalizeStart := time.Now()
		val = normalizeIndexedPayloadValue(ctx.normalizedValCache, val)
		if metrics != nil {
			metrics.ComputeNormalizeStringsDur += time.Since(normalizeStart)
		}
		if val != "" {
			filled++
		}
	}
	return filled
}

func buildIndexPayloadFastValue(ctx indexPayloadFastContext, field compiledIndexField, metrics *llibreIndexacioRecalcMetrics) string {
	switch field.Field.Target {
	case "raw":
		partStart := time.Now()
		val, ok := ctx.rawValueCache[field.Field.RawField]
		if !ok {
			val = rawFieldValue(ctx.raw, field.Field.RawField)
			ctx.rawValueCache[field.Field.RawField] = val
		}
		if metrics != nil {
			metrics.ComputeGroupRegistresDur += time.Since(partStart)
		}
		return val
	case "attr":
		partStart := time.Now()
		if attr, ok := ctx.atributsByKey[field.AttrKeyNorm]; ok && attr.Found {
			if metrics != nil {
				metrics.ComputeGroupAtributsDur += time.Since(partStart)
			}
			return attr.Value
		}
		if metrics != nil {
			metrics.ComputeGroupAtributsDur += time.Since(partStart)
		}
		return ""
	case "person":
		partStart := time.Now()
		person := ctx.personSelection[field.PersonLookup]
		if person == nil {
			role := field.Field.Role
			list := ctx.personesByRole[role]
			if len(list) > 0 {
				parts := strings.Split(field.PersonLookup, "\x1f")
				idx := 1
				if len(parts) == 2 {
					if parsed, err := strconv.Atoi(parts[1]); err == nil && parsed > 0 {
						idx = parsed
					}
				}
				if idx > len(list) {
					idx = 1
				}
				person = list[idx-1]
				ctx.personSelection[field.PersonLookup] = person
			}
		}
		val := personFieldValue(person, field.Field.PersonField)
		if metrics != nil {
			metrics.ComputeGroupPersonesDur += time.Since(partStart)
		}
		return val
	default:
		return ""
	}
}

func normalizeIndexedPayloadValue(cache map[string]string, val string) string {
	if cache != nil {
		if cached, ok := cache[val]; ok {
			return cached
		}
	}
	normalized := strings.TrimSpace(val)
	if cache != nil {
		cache[val] = normalized
	}
	return normalized
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
