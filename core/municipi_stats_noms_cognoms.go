package core

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type nomCognomContrib struct {
	AnyDoc       int
	NomCounts    map[string]int
	NomForms     map[string]string
	CognomCounts map[string]int
	CognomForms  map[string]string
}

type nomCognomContribMetrics struct {
	Cache             *nomCognomContribCache
	SetupDur          time.Duration
	RoleMatchDur      time.Duration
	NomDur            time.Duration
	CognomDur         time.Duration
	Persones          int
	Matched           int
	NomValues         int
	CognomValues      int
	NomCacheHits      int
	NomCacheMisses    int
	CognomCacheHits   int
	CognomCacheMisses int
}

type nomCognomContribCache struct {
	nom    map[string]nomCognomCleanKey
	cognom map[string]nomCognomCleanKey
}

type nomCognomCleanKey struct {
	form string
	key  string
}

func newNomCognomContribCache() *nomCognomContribCache {
	return &nomCognomContribCache{
		nom:    map[string]nomCognomCleanKey{},
		cognom: map[string]nomCognomCleanKey{},
	}
}

func contribCacheKey(value, status string) string {
	return status + "\x00" + value
}

func (c *nomCognomContribCache) cleanNom(value, status string) (string, string, bool) {
	if c == nil {
		form := cleanNomValue(value, status)
		if form == "" {
			return "", "", false
		}
		return form, NormalizeNomKey(form), false
	}
	cacheKey := contribCacheKey(value, status)
	if cached, ok := c.nom[cacheKey]; ok {
		return cached.form, cached.key, true
	}
	form := cleanNomValue(value, status)
	key := ""
	if form != "" {
		key = NormalizeNomKey(form)
	}
	c.nom[cacheKey] = nomCognomCleanKey{form: form, key: key}
	return form, key, false
}

func (c *nomCognomContribCache) cleanCognom(value, status string) (string, string, bool) {
	if c == nil {
		form := cleanCognomValue(value, status)
		if form == "" {
			return "", "", false
		}
		return form, NormalizeCognomKey(form), false
	}
	cacheKey := contribCacheKey(value, status)
	if cached, ok := c.cognom[cacheKey]; ok {
		return cached.form, cached.key, true
	}
	form := cleanCognomValue(value, status)
	key := ""
	if form != "" {
		key = NormalizeCognomKey(form)
	}
	c.cognom[cacheKey] = nomCognomCleanKey{form: form, key: key}
	return form, key, false
}

type nomCognomBulkDelta struct {
	MunicipiID int
	NivellIDs  []int
	Contrib    nomCognomContrib
	Sign       int
}

type bulkNomCognomStatsStore interface {
	BulkApplyNomCognomStatsDeltas(deltas db.NomCognomStatsDeltas) error
}

type bulkNomCognomEntityStore interface {
	BulkEnsureNoms(formsByKey map[string]string, notes string, createdBy *int) (map[string]int, error)
	BulkEnsureCognoms(formsByKey map[string]string, origen, notes string, createdBy *int) (map[string]int, error)
}

type nomCognomBulkApplyMetrics struct {
	Items                   int
	NomKeys                 int
	CognomKeys              int
	DeltaRows               int
	Municipis               int
	Nivells                 int
	NegativeDeltaRows       int
	NomMunicipiAnyRows      int
	NomMunicipiTotalRows    int
	NomNivellAnyRows        int
	NomNivellTotalRows      int
	CognomMunicipiAnyRows   int
	CognomMunicipiTotalRows int
	CognomNivellAnyRows     int
	CognomNivellTotalRows   int
	AggregateDur            time.Duration
	EnsureDur               time.Duration
	BuildDeltasDur          time.Duration
	ApplyDur                time.Duration
	EnsureMode              string
	ApplyMode               string
}

type bulkNomMunicipiAnyKey struct {
	Key        string
	MunicipiID int
	AnyDoc     int
}

type bulkNomMunicipiTotalKey struct {
	Key        string
	MunicipiID int
}

type bulkNomNivellAnyKey struct {
	Key      string
	NivellID int
	AnyDoc   int
}

type bulkNomNivellTotalKey struct {
	Key      string
	NivellID int
}

type bulkCognomMunicipiAnyKey struct {
	Key        string
	MunicipiID int
	AnyDoc     int
}

type bulkCognomMunicipiTotalKey struct {
	Key        string
	MunicipiID int
}

type bulkCognomNivellAnyKey struct {
	Key      string
	NivellID int
	AnyDoc   int
}

type bulkCognomNivellTotalKey struct {
	Key      string
	NivellID int
}

func primaryRolesForTipus(tipusActe string) []string {
	switch strings.ToLower(strings.TrimSpace(tipusActe)) {
	case "baptisme":
		return []string{"batejat"}
	case "matrimoni":
		return []string{"nuvi", "novia"}
	case "obit":
		return []string{"difunt"}
	case "confirmacio":
		return []string{"confirmat"}
	default:
		return []string{}
	}
}

func isValueStatusValid(status string) bool {
	status = strings.ToLower(strings.TrimSpace(status))
	return status == "" || (status != "illegible" && status != "no_consta")
}

func cleanNomValue(value, status string) string {
	if strings.Contains(value, "?") {
		return ""
	}
	if !isValueStatusValid(status) {
		return ""
	}
	return sanitizeNomLiteral(value)
}

func cleanCognomValue(value, status string) string {
	if strings.Contains(value, "?") {
		return ""
	}
	if !isValueStatusValid(status) {
		return ""
	}
	return sanitizeCognomLiteral(value)
}

func yearFromActe(registre db.TranscripcioRaw) int {
	if registre.AnyDoc.Valid {
		if registre.AnyDoc.Int64 > 0 {
			return int(registre.AnyDoc.Int64)
		}
	}
	if registre.DataActeISO.Valid && len(registre.DataActeISO.String) >= 4 {
		if year, err := strconv.Atoi(registre.DataActeISO.String[:4]); err == nil && year > 0 {
			return year
		}
	}
	return 0
}

func calcNomCognomContribs(registre db.TranscripcioRaw, persones []db.TranscripcioPersonaRaw) nomCognomContrib {
	return calcNomCognomContribsWithMetrics(registre, persones, nil)
}

func calcNomCognomContribsWithMetrics(registre db.TranscripcioRaw, persones []db.TranscripcioPersonaRaw, metrics *nomCognomContribMetrics) nomCognomContrib {
	setupStart := time.Now()
	anyDoc := yearFromActe(registre)
	contrib := nomCognomContrib{AnyDoc: anyDoc}
	if metrics != nil {
		metrics.Persones += len(persones)
	}
	if anyDoc <= 0 {
		if metrics != nil {
			metrics.SetupDur += time.Since(setupStart)
		}
		return contrib
	}
	roles := primaryRolesForTipus(registre.TipusActe)
	if len(roles) == 0 {
		if metrics != nil {
			metrics.SetupDur += time.Since(setupStart)
		}
		return contrib
	}
	contrib.NomCounts = make(map[string]int)
	contrib.NomForms = make(map[string]string)
	contrib.CognomCounts = make(map[string]int)
	contrib.CognomForms = make(map[string]string)
	roleSet := map[string]bool{}
	for _, role := range roles {
		roleSet[strings.ToLower(role)] = true
	}
	if metrics != nil {
		metrics.SetupDur += time.Since(setupStart)
	}
	addNom := func(value, status string) {
		if metrics != nil {
			metrics.NomValues++
		}
		nomStart := time.Now()
		var form, key string
		var hit bool
		if metrics != nil {
			form, key, hit = metrics.Cache.cleanNom(value, status)
			if hit {
				metrics.NomCacheHits++
			} else {
				metrics.NomCacheMisses++
			}
		} else {
			form, key, _ = (*nomCognomContribCache)(nil).cleanNom(value, status)
		}
		if form == "" {
			if metrics != nil {
				metrics.NomDur += time.Since(nomStart)
			}
			return
		}
		if key == "" {
			if metrics != nil {
				metrics.NomDur += time.Since(nomStart)
			}
			return
		}
		contrib.NomCounts[key]++
		if _, ok := contrib.NomForms[key]; !ok {
			contrib.NomForms[key] = form
		}
		if metrics != nil {
			metrics.NomDur += time.Since(nomStart)
		}
	}
	addCognom := func(value, status string) {
		if metrics != nil {
			metrics.CognomValues++
		}
		cognomStart := time.Now()
		var form, key string
		var hit bool
		if metrics != nil {
			form, key, hit = metrics.Cache.cleanCognom(value, status)
			if hit {
				metrics.CognomCacheHits++
			} else {
				metrics.CognomCacheMisses++
			}
		} else {
			form, key, _ = (*nomCognomContribCache)(nil).cleanCognom(value, status)
		}
		if form == "" {
			if metrics != nil {
				metrics.CognomDur += time.Since(cognomStart)
			}
			return
		}
		if key == "" {
			if metrics != nil {
				metrics.CognomDur += time.Since(cognomStart)
			}
			return
		}
		contrib.CognomCounts[key]++
		if _, ok := contrib.CognomForms[key]; !ok {
			contrib.CognomForms[key] = form
		}
		if metrics != nil {
			metrics.CognomDur += time.Since(cognomStart)
		}
	}
	for _, persona := range persones {
		roleStart := time.Now()
		role := strings.ToLower(strings.TrimSpace(persona.Rol))
		matched := roleSet[role]
		if metrics != nil {
			metrics.RoleMatchDur += time.Since(roleStart)
		}
		if !matched {
			continue
		}
		if metrics != nil {
			metrics.Matched++
		}
		addNom(persona.Nom, persona.NomEstat)
		addCognom(persona.Cognom1, persona.Cognom1Estat)
		addCognom(persona.Cognom2, persona.Cognom2Estat)
	}
	return contrib
}

func (a *App) applyNomCognomDelta(municipiID int, contrib nomCognomContrib, sign int) error {
	if municipiID <= 0 || contrib.AnyDoc <= 0 || sign == 0 {
		return nil
	}
	nivellIDs := a.listNivellAncestorsForMunicipi(municipiID)
	return a.applyNomCognomDeltaWithNivells(municipiID, contrib, sign, nivellIDs)
}

func (a *App) applyNomCognomDeltaWithNivells(municipiID int, contrib nomCognomContrib, sign int, nivellIDs []int) error {
	if municipiID <= 0 || contrib.AnyDoc <= 0 || sign == 0 {
		return nil
	}
	for key, count := range contrib.NomCounts {
		form := strings.TrimSpace(contrib.NomForms[key])
		if form == "" {
			form = key
		}
		nomID, err := a.DB.UpsertNom(form, key, "stats_auto", nil)
		if err != nil {
			return err
		}
		delta := count * sign
		if err := a.DB.UpsertNomFreqMunicipiAny(nomID, municipiID, contrib.AnyDoc, delta); err != nil {
			return err
		}
		if err := a.DB.UpsertNomFreqMunicipiTotal(nomID, municipiID, delta); err != nil {
			return err
		}
		for _, nivellID := range nivellIDs {
			if err := a.DB.UpsertNomFreqNivellAny(nomID, nivellID, contrib.AnyDoc, delta); err != nil {
				return err
			}
			if err := a.DB.UpsertNomFreqNivellTotal(nomID, nivellID, delta); err != nil {
				return err
			}
		}
	}
	for key, count := range contrib.CognomCounts {
		form := strings.TrimSpace(contrib.CognomForms[key])
		if form == "" {
			form = key
		}
		cognomID, err := a.DB.UpsertCognom(form, key, "stats_auto", "stats_auto", nil)
		if err != nil {
			return err
		}
		delta := count * sign
		if err := a.DB.ApplyCognomFreqMunicipiAnyDelta(cognomID, municipiID, contrib.AnyDoc, delta); err != nil {
			return err
		}
		if err := a.DB.UpsertCognomFreqMunicipiTotal(cognomID, municipiID, delta); err != nil {
			return err
		}
		for _, nivellID := range nivellIDs {
			if err := a.DB.ApplyCognomFreqNivellAnyDelta(cognomID, nivellID, contrib.AnyDoc, delta); err != nil {
				return err
			}
			if err := a.DB.UpsertCognomFreqNivellTotal(cognomID, nivellID, delta); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *App) applyNomCognomBulkDeltas(items []nomCognomBulkDelta) (nomCognomBulkApplyMetrics, error) {
	metrics := nomCognomBulkApplyMetrics{Items: len(items)}
	if len(items) == 0 {
		return metrics, nil
	}

	aggregateStart := time.Now()
	nomForms := map[string]string{}
	cognomForms := map[string]string{}
	municipis := map[int]struct{}{}
	nivells := map[int]struct{}{}
	nomMunicipiAny := map[bulkNomMunicipiAnyKey]int{}
	nomMunicipiTotal := map[bulkNomMunicipiTotalKey]int{}
	nomNivellAny := map[bulkNomNivellAnyKey]int{}
	nomNivellTotal := map[bulkNomNivellTotalKey]int{}
	cognomMunicipiAny := map[bulkCognomMunicipiAnyKey]int{}
	cognomMunicipiTotal := map[bulkCognomMunicipiTotalKey]int{}
	cognomNivellAny := map[bulkCognomNivellAnyKey]int{}
	cognomNivellTotal := map[bulkCognomNivellTotalKey]int{}

	for _, item := range items {
		if item.MunicipiID <= 0 || item.Contrib.AnyDoc <= 0 || item.Sign == 0 {
			continue
		}
		municipis[item.MunicipiID] = struct{}{}
		nivellIDs := dedupeInts(item.NivellIDs)
		for _, nivellID := range nivellIDs {
			if nivellID > 0 {
				nivells[nivellID] = struct{}{}
			}
		}
		for key, count := range item.Contrib.NomCounts {
			delta := count * item.Sign
			if delta == 0 {
				continue
			}
			form := strings.TrimSpace(item.Contrib.NomForms[key])
			if form == "" {
				form = key
			}
			if _, ok := nomForms[key]; !ok {
				nomForms[key] = form
			}
			nomMunicipiAny[bulkNomMunicipiAnyKey{Key: key, MunicipiID: item.MunicipiID, AnyDoc: item.Contrib.AnyDoc}] += delta
			nomMunicipiTotal[bulkNomMunicipiTotalKey{Key: key, MunicipiID: item.MunicipiID}] += delta
			for _, nivellID := range nivellIDs {
				nomNivellAny[bulkNomNivellAnyKey{Key: key, NivellID: nivellID, AnyDoc: item.Contrib.AnyDoc}] += delta
				nomNivellTotal[bulkNomNivellTotalKey{Key: key, NivellID: nivellID}] += delta
			}
		}
		for key, count := range item.Contrib.CognomCounts {
			delta := count * item.Sign
			if delta == 0 {
				continue
			}
			form := strings.TrimSpace(item.Contrib.CognomForms[key])
			if form == "" {
				form = key
			}
			if _, ok := cognomForms[key]; !ok {
				cognomForms[key] = form
			}
			cognomMunicipiAny[bulkCognomMunicipiAnyKey{Key: key, MunicipiID: item.MunicipiID, AnyDoc: item.Contrib.AnyDoc}] += delta
			cognomMunicipiTotal[bulkCognomMunicipiTotalKey{Key: key, MunicipiID: item.MunicipiID}] += delta
			for _, nivellID := range nivellIDs {
				cognomNivellAny[bulkCognomNivellAnyKey{Key: key, NivellID: nivellID, AnyDoc: item.Contrib.AnyDoc}] += delta
				cognomNivellTotal[bulkCognomNivellTotalKey{Key: key, NivellID: nivellID}] += delta
			}
		}
	}
	metrics.AggregateDur = time.Since(aggregateStart)
	metrics.Municipis = len(municipis)
	metrics.Nivells = len(nivells)

	ensureStart := time.Now()
	nomIDs, cognomIDs, ensureMode, err := a.ensureNomCognomBulkIDs(nomForms, cognomForms)
	if err != nil {
		return metrics, err
	}
	metrics.EnsureDur = time.Since(ensureStart)
	metrics.EnsureMode = ensureMode
	metrics.NomKeys = len(nomIDs)
	metrics.CognomKeys = len(cognomIDs)

	buildDeltasStart := time.Now()
	deltas := db.NomCognomStatsDeltas{
		NomMunicipiAny:      make([]db.NomFreqMunicipiAnyDelta, 0, len(nomMunicipiAny)),
		NomMunicipiTotal:    make([]db.NomFreqMunicipiTotalDelta, 0, len(nomMunicipiTotal)),
		NomNivellAny:        make([]db.NomFreqNivellAnyDelta, 0, len(nomNivellAny)),
		NomNivellTotal:      make([]db.NomFreqNivellTotalDelta, 0, len(nomNivellTotal)),
		CognomMunicipiAny:   make([]db.CognomFreqMunicipiAnyDelta, 0, len(cognomMunicipiAny)),
		CognomMunicipiTotal: make([]db.CognomFreqMunicipiTotalDelta, 0, len(cognomMunicipiTotal)),
		CognomNivellAny:     make([]db.CognomFreqNivellAnyDelta, 0, len(cognomNivellAny)),
		CognomNivellTotal:   make([]db.CognomFreqNivellTotalDelta, 0, len(cognomNivellTotal)),
	}

	for agg, delta := range nomMunicipiAny {
		if delta == 0 {
			continue
		}
		if delta < 0 {
			metrics.NegativeDeltaRows++
		}
		deltas.NomMunicipiAny = append(deltas.NomMunicipiAny, db.NomFreqMunicipiAnyDelta{NomID: nomIDs[agg.Key], MunicipiID: agg.MunicipiID, AnyDoc: agg.AnyDoc, Delta: delta})
	}
	for agg, delta := range nomMunicipiTotal {
		if delta == 0 {
			continue
		}
		if delta < 0 {
			metrics.NegativeDeltaRows++
		}
		deltas.NomMunicipiTotal = append(deltas.NomMunicipiTotal, db.NomFreqMunicipiTotalDelta{NomID: nomIDs[agg.Key], MunicipiID: agg.MunicipiID, Delta: delta})
	}
	for agg, delta := range nomNivellAny {
		if delta == 0 {
			continue
		}
		if delta < 0 {
			metrics.NegativeDeltaRows++
		}
		deltas.NomNivellAny = append(deltas.NomNivellAny, db.NomFreqNivellAnyDelta{NomID: nomIDs[agg.Key], NivellID: agg.NivellID, AnyDoc: agg.AnyDoc, Delta: delta})
	}
	for agg, delta := range nomNivellTotal {
		if delta == 0 {
			continue
		}
		if delta < 0 {
			metrics.NegativeDeltaRows++
		}
		deltas.NomNivellTotal = append(deltas.NomNivellTotal, db.NomFreqNivellTotalDelta{NomID: nomIDs[agg.Key], NivellID: agg.NivellID, Delta: delta})
	}

	for agg, delta := range cognomMunicipiAny {
		if delta == 0 {
			continue
		}
		if delta < 0 {
			metrics.NegativeDeltaRows++
		}
		deltas.CognomMunicipiAny = append(deltas.CognomMunicipiAny, db.CognomFreqMunicipiAnyDelta{CognomID: cognomIDs[agg.Key], MunicipiID: agg.MunicipiID, AnyDoc: agg.AnyDoc, Delta: delta})
	}
	for agg, delta := range cognomMunicipiTotal {
		if delta == 0 {
			continue
		}
		if delta < 0 {
			metrics.NegativeDeltaRows++
		}
		deltas.CognomMunicipiTotal = append(deltas.CognomMunicipiTotal, db.CognomFreqMunicipiTotalDelta{CognomID: cognomIDs[agg.Key], MunicipiID: agg.MunicipiID, Delta: delta})
	}
	for agg, delta := range cognomNivellAny {
		if delta == 0 {
			continue
		}
		if delta < 0 {
			metrics.NegativeDeltaRows++
		}
		deltas.CognomNivellAny = append(deltas.CognomNivellAny, db.CognomFreqNivellAnyDelta{CognomID: cognomIDs[agg.Key], NivellID: agg.NivellID, AnyDoc: agg.AnyDoc, Delta: delta})
	}
	for agg, delta := range cognomNivellTotal {
		if delta == 0 {
			continue
		}
		if delta < 0 {
			metrics.NegativeDeltaRows++
		}
		deltas.CognomNivellTotal = append(deltas.CognomNivellTotal, db.CognomFreqNivellTotalDelta{CognomID: cognomIDs[agg.Key], NivellID: agg.NivellID, Delta: delta})
	}
	metrics.BuildDeltasDur = time.Since(buildDeltasStart)
	metrics.NomMunicipiAnyRows = len(deltas.NomMunicipiAny)
	metrics.NomMunicipiTotalRows = len(deltas.NomMunicipiTotal)
	metrics.NomNivellAnyRows = len(deltas.NomNivellAny)
	metrics.NomNivellTotalRows = len(deltas.NomNivellTotal)
	metrics.CognomMunicipiAnyRows = len(deltas.CognomMunicipiAny)
	metrics.CognomMunicipiTotalRows = len(deltas.CognomMunicipiTotal)
	metrics.CognomNivellAnyRows = len(deltas.CognomNivellAny)
	metrics.CognomNivellTotalRows = len(deltas.CognomNivellTotal)
	metrics.DeltaRows = metrics.NomMunicipiAnyRows + metrics.NomMunicipiTotalRows + metrics.NomNivellAnyRows + metrics.NomNivellTotalRows + metrics.CognomMunicipiAnyRows + metrics.CognomMunicipiTotalRows + metrics.CognomNivellAnyRows + metrics.CognomNivellTotalRows

	applyStart := time.Now()
	if store, ok := a.DB.(bulkNomCognomStatsStore); ok {
		err := store.BulkApplyNomCognomStatsDeltas(deltas)
		metrics.ApplyDur = time.Since(applyStart)
		metrics.ApplyMode = "bulk"
		if IsDebugEnabled() {
			Debugf("moderacio bulk registre stats aggregate items=%d nom_keys=%d cognom_keys=%d delta_rows=%d municipis=%d nivells=%d negative_delta_rows=%d nom_mun_any=%d nom_mun_total=%d nom_nivell_any=%d nom_nivell_total=%d cognom_mun_any=%d cognom_mun_total=%d cognom_nivell_any=%d cognom_nivell_total=%d aggregate_dur=%s ensure_dur=%s build_deltas_dur=%s apply_dur=%s ensure=%s apply=bulk", metrics.Items, metrics.NomKeys, metrics.CognomKeys, metrics.DeltaRows, metrics.Municipis, metrics.Nivells, metrics.NegativeDeltaRows, metrics.NomMunicipiAnyRows, metrics.NomMunicipiTotalRows, metrics.NomNivellAnyRows, metrics.NomNivellTotalRows, metrics.CognomMunicipiAnyRows, metrics.CognomMunicipiTotalRows, metrics.CognomNivellAnyRows, metrics.CognomNivellTotalRows, metrics.AggregateDur, metrics.EnsureDur, metrics.BuildDeltasDur, metrics.ApplyDur, metrics.EnsureMode)
		}
		return metrics, err
	}

	metrics.ApplyMode = "sequential_fallback"
	if IsDebugEnabled() {
		defer func() {
			Debugf("moderacio bulk registre stats aggregate items=%d nom_keys=%d cognom_keys=%d delta_rows=%d municipis=%d nivells=%d negative_delta_rows=%d nom_mun_any=%d nom_mun_total=%d nom_nivell_any=%d nom_nivell_total=%d cognom_mun_any=%d cognom_mun_total=%d cognom_nivell_any=%d cognom_nivell_total=%d aggregate_dur=%s ensure_dur=%s build_deltas_dur=%s apply_dur=%s ensure=%s apply=sequential_fallback", metrics.Items, metrics.NomKeys, metrics.CognomKeys, metrics.DeltaRows, metrics.Municipis, metrics.Nivells, metrics.NegativeDeltaRows, metrics.NomMunicipiAnyRows, metrics.NomMunicipiTotalRows, metrics.NomNivellAnyRows, metrics.NomNivellTotalRows, metrics.CognomMunicipiAnyRows, metrics.CognomMunicipiTotalRows, metrics.CognomNivellAnyRows, metrics.CognomNivellTotalRows, metrics.AggregateDur, metrics.EnsureDur, metrics.BuildDeltasDur, metrics.ApplyDur, metrics.EnsureMode)
		}()
	}
	for _, row := range deltas.NomMunicipiAny {
		if err := a.DB.UpsertNomFreqMunicipiAny(row.NomID, row.MunicipiID, row.AnyDoc, row.Delta); err != nil {
			metrics.ApplyDur = time.Since(applyStart)
			return metrics, err
		}
	}
	for _, row := range deltas.NomMunicipiTotal {
		if err := a.DB.UpsertNomFreqMunicipiTotal(row.NomID, row.MunicipiID, row.Delta); err != nil {
			metrics.ApplyDur = time.Since(applyStart)
			return metrics, err
		}
	}
	for _, row := range deltas.NomNivellAny {
		if err := a.DB.UpsertNomFreqNivellAny(row.NomID, row.NivellID, row.AnyDoc, row.Delta); err != nil {
			metrics.ApplyDur = time.Since(applyStart)
			return metrics, err
		}
	}
	for _, row := range deltas.NomNivellTotal {
		if err := a.DB.UpsertNomFreqNivellTotal(row.NomID, row.NivellID, row.Delta); err != nil {
			metrics.ApplyDur = time.Since(applyStart)
			return metrics, err
		}
	}
	for _, row := range deltas.CognomMunicipiAny {
		if err := a.DB.ApplyCognomFreqMunicipiAnyDelta(row.CognomID, row.MunicipiID, row.AnyDoc, row.Delta); err != nil {
			metrics.ApplyDur = time.Since(applyStart)
			return metrics, err
		}
	}
	for _, row := range deltas.CognomMunicipiTotal {
		if err := a.DB.UpsertCognomFreqMunicipiTotal(row.CognomID, row.MunicipiID, row.Delta); err != nil {
			metrics.ApplyDur = time.Since(applyStart)
			return metrics, err
		}
	}
	for _, row := range deltas.CognomNivellAny {
		if err := a.DB.ApplyCognomFreqNivellAnyDelta(row.CognomID, row.NivellID, row.AnyDoc, row.Delta); err != nil {
			metrics.ApplyDur = time.Since(applyStart)
			return metrics, err
		}
	}
	for _, row := range deltas.CognomNivellTotal {
		if err := a.DB.UpsertCognomFreqNivellTotal(row.CognomID, row.NivellID, row.Delta); err != nil {
			metrics.ApplyDur = time.Since(applyStart)
			return metrics, err
		}
	}

	metrics.ApplyDur = time.Since(applyStart)
	return metrics, nil
}

func (a *App) ensureNomCognomBulkIDs(nomForms, cognomForms map[string]string) (map[string]int, map[string]int, string, error) {
	if store, ok := a.DB.(bulkNomCognomEntityStore); ok {
		nomIDs, err := store.BulkEnsureNoms(nomForms, "stats_auto", nil)
		if err != nil {
			return nil, nil, "bulk", err
		}
		cognomIDs, err := store.BulkEnsureCognoms(cognomForms, "stats_auto", "stats_auto", nil)
		if err != nil {
			return nil, nil, "bulk", err
		}
		return nomIDs, cognomIDs, "bulk", nil
	}

	nomIDs := make(map[string]int, len(nomForms))
	nomKeys := make([]string, 0, len(nomForms))
	for key := range nomForms {
		nomKeys = append(nomKeys, key)
	}
	sort.Strings(nomKeys)
	for _, key := range nomKeys {
		form := strings.TrimSpace(nomForms[key])
		if form == "" {
			form = key
		}
		nomID, err := a.DB.UpsertNom(form, key, "stats_auto", nil)
		if err != nil {
			return nil, nil, "sequential_fallback", err
		}
		nomIDs[key] = nomID
	}

	cognomIDs := make(map[string]int, len(cognomForms))
	cognomKeys := make([]string, 0, len(cognomForms))
	for key := range cognomForms {
		cognomKeys = append(cognomKeys, key)
	}
	sort.Strings(cognomKeys)
	for _, key := range cognomKeys {
		form := strings.TrimSpace(cognomForms[key])
		if form == "" {
			form = key
		}
		cognomID, err := a.DB.UpsertCognom(form, key, "stats_auto", "stats_auto", nil)
		if err != nil {
			return nil, nil, "sequential_fallback", err
		}
		cognomIDs[key] = cognomID
	}
	return nomIDs, cognomIDs, "sequential_fallback", nil
}

func (a *App) applyNomCognomDeltaForRegistre(reg *db.TranscripcioRaw, persones []db.TranscripcioPersonaRaw, delta int) {
	if reg == nil || delta == 0 {
		return
	}
	llibre, err := a.loadLlibreForRegistre(reg)
	if err != nil || llibre == nil {
		return
	}
	munID := demografiaMunicipiIDFromRegistre(reg, llibre)
	if munID <= 0 {
		return
	}
	contrib := calcNomCognomContribs(*reg, persones)
	if err := a.applyNomCognomDelta(munID, contrib, delta); err != nil {
		Errorf("Error actualitzant stats noms/cognoms municipi %d: %v", munID, err)
	}
}

func (a *App) rebuildMunicipiNomCognomStats(municipiID int) (int, error) {
	if municipiID <= 0 {
		return 0, fmt.Errorf("municipi invalid")
	}
	if err := a.DB.ClearNomCognomStatsByMunicipi(municipiID); err != nil {
		return 0, err
	}
	llibres, err := a.DB.ListLlibres(db.LlibreFilter{MunicipiID: municipiID})
	if err != nil {
		return 0, err
	}
	processed := 0
	for _, llibre := range llibres {
		registres, err := a.DB.ListTranscripcionsRaw(llibre.ID, db.TranscripcioFilter{Status: "publicat", Limit: -1})
		if err != nil {
			return processed, err
		}
		for _, registre := range registres {
			persones, _ := a.DB.ListTranscripcioPersones(registre.ID)
			contrib := calcNomCognomContribs(registre, persones)
			if contrib.AnyDoc <= 0 {
				continue
			}
			if err := a.applyNomCognomDelta(municipiID, contrib, 1); err != nil {
				return processed, err
			}
			processed++
		}
	}
	return processed, nil
}

func (a *App) loadMunicipiStatsAccess(r *http.Request, municipiID int) (*db.Municipi, bool) {
	if municipiID <= 0 {
		return nil, false
	}
	mun, err := a.DB.GetMunicipi(municipiID)
	if err != nil || mun == nil {
		return nil, false
	}
	if mun.ModeracioEstat == "" || mun.ModeracioEstat == "publicat" {
		return mun, true
	}
	user, _ := a.VerificarSessio(r)
	if user == nil {
		return nil, false
	}
	perms := a.getPermissionsForUser(user.ID)
	if a.hasPerm(perms, permTerritory) || a.hasPerm(perms, permModerate) {
		return mun, true
	}
	return nil, false
}

func (a *App) municipiStatsTopNoms(w http.ResponseWriter, r *http.Request, municipiID int) {
	if _, ok := a.loadMunicipiStatsAccess(r, municipiID); !ok {
		http.NotFound(w, r)
		return
	}
	limit := parseFormInt(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}
	items, err := a.DB.ListTopNomsByMunicipi(municipiID, limit)
	if err != nil {
		http.Error(w, "failed to load", http.StatusInternalServerError)
		return
	}
	totalDistinct, _ := a.DB.CountNomTotalsByMunicipi(municipiID)
	payload := make([]map[string]interface{}, 0, len(items))
	for _, item := range items {
		payload = append(payload, map[string]interface{}{
			"id":    item.NomID,
			"label": strings.TrimSpace(item.Forma),
			"total": item.TotalFreq,
		})
	}
	writeJSON(w, map[string]interface{}{
		"items":         payload,
		"totalDistinct": totalDistinct,
	})
}

func (a *App) municipiStatsTopCognoms(w http.ResponseWriter, r *http.Request, municipiID int) {
	if _, ok := a.loadMunicipiStatsAccess(r, municipiID); !ok {
		http.NotFound(w, r)
		return
	}
	limit := parseFormInt(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}
	items, err := a.DB.ListTopCognomsByMunicipi(municipiID, limit)
	if err != nil {
		http.Error(w, "failed to load", http.StatusInternalServerError)
		return
	}
	totalDistinct, _ := a.DB.CountCognomTotalsByMunicipi(municipiID)
	payload := make([]map[string]interface{}, 0, len(items))
	for _, item := range items {
		payload = append(payload, map[string]interface{}{
			"id":    item.CognomID,
			"label": strings.TrimSpace(item.Forma),
			"total": item.TotalFreq,
		})
	}
	writeJSON(w, map[string]interface{}{
		"items":         payload,
		"totalDistinct": totalDistinct,
	})
}

func (a *App) municipiStatsNomSeries(w http.ResponseWriter, r *http.Request, municipiID int) {
	if _, ok := a.loadMunicipiStatsAccess(r, municipiID); !ok {
		http.NotFound(w, r)
		return
	}
	nomID := parseFormInt(r.URL.Query().Get("nom_id"))
	if nomID <= 0 {
		http.Error(w, "missing nom_id", http.StatusBadRequest)
		return
	}
	bucket := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("bucket")))
	if bucket != "decade" {
		bucket = "year"
	}
	rows, err := a.DB.ListNomSeries(municipiID, nomID, bucket)
	if err != nil {
		http.Error(w, "failed to load", http.StatusInternalServerError)
		return
	}
	payload := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		payload = append(payload, map[string]interface{}{
			"x": row.AnyDoc,
			"y": row.Freq,
		})
	}
	writeJSON(w, map[string]interface{}{
		"bucket": bucket,
		"items":  payload,
	})
}

func (a *App) municipiStatsCognomSeries(w http.ResponseWriter, r *http.Request, municipiID int) {
	if _, ok := a.loadMunicipiStatsAccess(r, municipiID); !ok {
		http.NotFound(w, r)
		return
	}
	cognomID := parseFormInt(r.URL.Query().Get("cognom_id"))
	if cognomID <= 0 {
		http.Error(w, "missing cognom_id", http.StatusBadRequest)
		return
	}
	bucket := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("bucket")))
	if bucket != "decade" {
		bucket = "year"
	}
	rows, err := a.DB.ListCognomSeries(municipiID, cognomID, bucket)
	if err != nil {
		http.Error(w, "failed to load", http.StatusInternalServerError)
		return
	}
	payload := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		payload = append(payload, map[string]interface{}{
			"x": row.AnyDoc,
			"y": row.Freq,
		})
	}
	writeJSON(w, map[string]interface{}{
		"bucket": bucket,
		"items":  payload,
	})
}
