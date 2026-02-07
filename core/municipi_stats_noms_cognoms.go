package core

import (
	"fmt"
	"net/http"
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
