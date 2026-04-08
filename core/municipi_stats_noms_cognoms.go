package core

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type nomCognomContrib struct {
	AnyDoc       int
	NomCounts    map[string]int
	NomForms     map[string]string
	CognomCounts map[string]int
	CognomForms  map[string]string
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
	anyDoc := yearFromActe(registre)
	contrib := nomCognomContrib{
		AnyDoc:       anyDoc,
		NomCounts:    map[string]int{},
		NomForms:     map[string]string{},
		CognomCounts: map[string]int{},
		CognomForms:  map[string]string{},
	}
	if anyDoc <= 0 {
		return contrib
	}
	roles := primaryRolesForTipus(registre.TipusActe)
	if len(roles) == 0 {
		return contrib
	}
	roleSet := map[string]bool{}
	for _, role := range roles {
		roleSet[strings.ToLower(role)] = true
	}
	addNom := func(value, status string) {
		form := cleanNomValue(value, status)
		if form == "" {
			return
		}
		key := NormalizeNomKey(form)
		if key == "" {
			return
		}
		contrib.NomCounts[key]++
		if _, ok := contrib.NomForms[key]; !ok {
			contrib.NomForms[key] = form
		}
	}
	addCognom := func(value, status string) {
		form := cleanCognomValue(value, status)
		if form == "" {
			return
		}
		key := NormalizeCognomKey(form)
		if key == "" {
			return
		}
		contrib.CognomCounts[key]++
		if _, ok := contrib.CognomForms[key]; !ok {
			contrib.CognomForms[key] = form
		}
	}
	for _, persona := range persones {
		role := strings.ToLower(strings.TrimSpace(persona.Rol))
		if !roleSet[role] {
			continue
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

func (a *App) applyNomCognomBulkDeltas(items []nomCognomBulkDelta) error {
	if len(items) == 0 {
		return nil
	}

	nomForms := map[string]string{}
	cognomForms := map[string]string{}
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
		nivellIDs := dedupeInts(item.NivellIDs)
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
			return err
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
			return err
		}
		cognomIDs[key] = cognomID
	}

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
		deltas.NomMunicipiAny = append(deltas.NomMunicipiAny, db.NomFreqMunicipiAnyDelta{NomID: nomIDs[agg.Key], MunicipiID: agg.MunicipiID, AnyDoc: agg.AnyDoc, Delta: delta})
	}
	for agg, delta := range nomMunicipiTotal {
		if delta == 0 {
			continue
		}
		deltas.NomMunicipiTotal = append(deltas.NomMunicipiTotal, db.NomFreqMunicipiTotalDelta{NomID: nomIDs[agg.Key], MunicipiID: agg.MunicipiID, Delta: delta})
	}
	for agg, delta := range nomNivellAny {
		if delta == 0 {
			continue
		}
		deltas.NomNivellAny = append(deltas.NomNivellAny, db.NomFreqNivellAnyDelta{NomID: nomIDs[agg.Key], NivellID: agg.NivellID, AnyDoc: agg.AnyDoc, Delta: delta})
	}
	for agg, delta := range nomNivellTotal {
		if delta == 0 {
			continue
		}
		deltas.NomNivellTotal = append(deltas.NomNivellTotal, db.NomFreqNivellTotalDelta{NomID: nomIDs[agg.Key], NivellID: agg.NivellID, Delta: delta})
	}

	for agg, delta := range cognomMunicipiAny {
		if delta == 0 {
			continue
		}
		deltas.CognomMunicipiAny = append(deltas.CognomMunicipiAny, db.CognomFreqMunicipiAnyDelta{CognomID: cognomIDs[agg.Key], MunicipiID: agg.MunicipiID, AnyDoc: agg.AnyDoc, Delta: delta})
	}
	for agg, delta := range cognomMunicipiTotal {
		if delta == 0 {
			continue
		}
		deltas.CognomMunicipiTotal = append(deltas.CognomMunicipiTotal, db.CognomFreqMunicipiTotalDelta{CognomID: cognomIDs[agg.Key], MunicipiID: agg.MunicipiID, Delta: delta})
	}
	for agg, delta := range cognomNivellAny {
		if delta == 0 {
			continue
		}
		deltas.CognomNivellAny = append(deltas.CognomNivellAny, db.CognomFreqNivellAnyDelta{CognomID: cognomIDs[agg.Key], NivellID: agg.NivellID, AnyDoc: agg.AnyDoc, Delta: delta})
	}
	for agg, delta := range cognomNivellTotal {
		if delta == 0 {
			continue
		}
		deltas.CognomNivellTotal = append(deltas.CognomNivellTotal, db.CognomFreqNivellTotalDelta{CognomID: cognomIDs[agg.Key], NivellID: agg.NivellID, Delta: delta})
	}

	if store, ok := a.DB.(bulkNomCognomStatsStore); ok {
		if IsDebugEnabled() {
			Debugf("moderacio bulk registre stats aggregate nom_keys=%d cognom_keys=%d nom_mun_any=%d nom_mun_total=%d nom_nivell_any=%d nom_nivell_total=%d cognom_mun_any=%d cognom_mun_total=%d cognom_nivell_any=%d cognom_nivell_total=%d apply=bulk", len(nomIDs), len(cognomIDs), len(deltas.NomMunicipiAny), len(deltas.NomMunicipiTotal), len(deltas.NomNivellAny), len(deltas.NomNivellTotal), len(deltas.CognomMunicipiAny), len(deltas.CognomMunicipiTotal), len(deltas.CognomNivellAny), len(deltas.CognomNivellTotal))
		}
		return store.BulkApplyNomCognomStatsDeltas(deltas)
	}

	if IsDebugEnabled() {
		Debugf("moderacio bulk registre stats aggregate nom_keys=%d cognom_keys=%d nom_mun_any=%d nom_mun_total=%d nom_nivell_any=%d nom_nivell_total=%d cognom_mun_any=%d cognom_mun_total=%d cognom_nivell_any=%d cognom_nivell_total=%d apply=sequential_fallback", len(nomIDs), len(cognomIDs), len(deltas.NomMunicipiAny), len(deltas.NomMunicipiTotal), len(deltas.NomNivellAny), len(deltas.NomNivellTotal), len(deltas.CognomMunicipiAny), len(deltas.CognomMunicipiTotal), len(deltas.CognomNivellAny), len(deltas.CognomNivellTotal))
	}
	for _, row := range deltas.NomMunicipiAny {
		if err := a.DB.UpsertNomFreqMunicipiAny(row.NomID, row.MunicipiID, row.AnyDoc, row.Delta); err != nil {
			return err
		}
	}
	for _, row := range deltas.NomMunicipiTotal {
		if err := a.DB.UpsertNomFreqMunicipiTotal(row.NomID, row.MunicipiID, row.Delta); err != nil {
			return err
		}
	}
	for _, row := range deltas.NomNivellAny {
		if err := a.DB.UpsertNomFreqNivellAny(row.NomID, row.NivellID, row.AnyDoc, row.Delta); err != nil {
			return err
		}
	}
	for _, row := range deltas.NomNivellTotal {
		if err := a.DB.UpsertNomFreqNivellTotal(row.NomID, row.NivellID, row.Delta); err != nil {
			return err
		}
	}
	for _, row := range deltas.CognomMunicipiAny {
		if err := a.DB.ApplyCognomFreqMunicipiAnyDelta(row.CognomID, row.MunicipiID, row.AnyDoc, row.Delta); err != nil {
			return err
		}
	}
	for _, row := range deltas.CognomMunicipiTotal {
		if err := a.DB.UpsertCognomFreqMunicipiTotal(row.CognomID, row.MunicipiID, row.Delta); err != nil {
			return err
		}
	}
	for _, row := range deltas.CognomNivellAny {
		if err := a.DB.ApplyCognomFreqNivellAnyDelta(row.CognomID, row.NivellID, row.AnyDoc, row.Delta); err != nil {
			return err
		}
	}
	for _, row := range deltas.CognomNivellTotal {
		if err := a.DB.UpsertCognomFreqNivellTotal(row.CognomID, row.NivellID, row.Delta); err != nil {
			return err
		}
	}

	return nil
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
