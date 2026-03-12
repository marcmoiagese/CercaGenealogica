package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type arxiusExportPayload struct {
	Version    int                 `json:"version"`
	ExportedAt string              `json:"exported_at"`
	Arxius     []arxiuExportRecord `json:"arxius"`
}

type arxiuExportRecord struct {
	ID               int    `json:"id"`
	Nom              string `json:"nom"`
	Tipus            string `json:"tipus"`
	Acces            string `json:"acces"`
	Adreca           string `json:"adreca"`
	Ubicacio         string `json:"ubicacio"`
	What3Words       string `json:"what3words,omitempty"`
	Web              string `json:"web"`
	Notes            string `json:"notes"`
	AcceptaDonacions bool   `json:"accepta_donacions,omitempty"`
	DonacionsURL     string `json:"donacions_url,omitempty"`
	MunicipiNom      string `json:"municipi_nom,omitempty"`
	MunicipiPaisISO2 string `json:"municipi_pais_iso2,omitempty"`
	EntitatNom       string `json:"entitat_nom,omitempty"`
}

func (a *App) AdminArxiusImport(w http.ResponseWriter, r *http.Request) {
	http.NotFound(w, r)
}

func (a *App) AdminArxiusExport(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminArxiusExport, PermissionTarget{}); !ok {
		return
	}
	levelISO := a.levelISOMap()
	arxius, err := a.DB.ListArxius(db.ArxiuFilter{})
	if err != nil {
		http.NotFound(w, r)
		return
	}
	payload := arxiusExportPayload{
		Version:    3,
		ExportedAt: time.Now().Format(time.RFC3339),
	}
	for _, row := range arxius {
		var iso2 string
		if row.MunicipiNom.Valid && row.MunicipiID.Valid {
			if mun, err := a.DB.GetMunicipi(int(row.MunicipiID.Int64)); err == nil && mun != nil {
				iso2 = municipiISO2(mun, levelISO)
			}
		}
		payload.Arxius = append(payload.Arxius, arxiuExportRecord{
			ID:               row.ID,
			Nom:              row.Nom,
			Tipus:            row.Tipus,
			Acces:            row.Acces,
			Adreca:           row.Adreca,
			Ubicacio:         row.Ubicacio,
			What3Words:       row.What3Words,
			Web:              row.Web,
			Notes:            row.Notes,
			AcceptaDonacions: row.AcceptaDonacions,
			DonacionsURL:     row.DonacionsURL,
			MunicipiNom:      row.MunicipiNom.String,
			MunicipiPaisISO2: iso2,
			EntitatNom:       row.EntitatNom.String,
		})
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename=arxius-export.json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(payload)
}

func (a *App) AdminArxiusImportRun(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKey(w, r, permKeyAdminArxiusImport, PermissionTarget{})
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	start := time.Now()
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		a.logAdminImportRun(r, "arxius", adminImportStatusError, user.ID)
		http.Redirect(w, r, withQueryParams("/admin/arxius/import", map[string]string{"err": "1"}), http.StatusSeeOther)
		return
	}
	returnTo := safeReturnTo(r.FormValue("return_to"), "/admin/arxius/import")
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		a.logAdminImportRun(r, "arxius", adminImportStatusError, user.ID)
		http.Redirect(w, r, withQueryParams(returnTo, map[string]string{"err": "1"}), http.StatusSeeOther)
		return
	}
	file, _, err := r.FormFile("import_file")
	if err != nil {
		a.logAdminImportRun(r, "arxius", adminImportStatusError, user.ID)
		http.Redirect(w, r, withQueryParams(returnTo, map[string]string{"err": "1"}), http.StatusSeeOther)
		return
	}
	defer file.Close()

	var payload arxiusExportPayload
	if err := json.NewDecoder(file).Decode(&payload); err != nil {
		a.logAdminImportRun(r, "arxius", adminImportStatusError, user.ID)
		http.Redirect(w, r, "/admin/arxius/import?err=1", http.StatusSeeOther)
		return
	}
	engine := territoriImportEngineName(a.DB)
	bulkInserter, hasBulkInserter := a.DB.(arxiusBulkInserter)

	type activityRule struct {
		ruleID sql.NullInt64
		points int
	}
	resolveActivityRule := func(code string) activityRule {
		if code == "" {
			return activityRule{}
		}
		rule, err := a.DB.GetPointsRuleByCode(code)
		if err != nil || rule == nil || !rule.Active {
			return activityRule{}
		}
		return activityRule{
			ruleID: sql.NullInt64{Int64: int64(rule.ID), Valid: true},
			points: rule.Points,
		}
	}
	activityRuleArxiu := resolveActivityRule(ruleArxiuCreate)
	pendingActivities := make([]db.UserActivity, 0, len(payload.Arxius))
	addActivity := func(rule activityRule, objectID int) {
		if objectID <= 0 {
			return
		}
		pendingActivities = append(pendingActivities, db.UserActivity{
			UserID:     user.ID,
			RuleID:     rule.ruleID,
			Action:     "crear",
			ObjectType: "arxiu",
			ObjectID:   sql.NullInt64{Int64: int64(objectID), Valid: true},
			Points:     rule.points,
			Status:     "pendent",
			Details:    "import",
		})
	}
	activityCount := 0

	total := len(payload.Arxius)
	created, skipped, errors, duplicates := 0, 0, 0, 0

	prepStart := time.Now()
	entMap, entNameMap, entMode, entKeys := a.arxiuEntitatNameMapsForPayload(payload.Arxius)
	munMap, munNameMap, munMode, munKeys := a.arxiuMunicipiNameMapsForPayload(payload.Arxius)
	existingByName, _, existingMode, existingKeys := a.arxiuExistingMapsForPayload(payload.Arxius)
	prepDuration := time.Since(prepStart)

	resolveStart := time.Now()
	seenPayload := map[string]struct{}{}
	toInsert := make([]db.Arxiu, 0, len(payload.Arxius))
	for _, row := range payload.Arxius {
		nameKey := normalizeKey(row.Nom)
		if nameKey == "" {
			skipped++
			duplicates++
			continue
		}
		hasMunicipi := strings.TrimSpace(row.MunicipiNom) != ""
		munID := 0
		if hasMunicipi {
			iso2 := strings.ToUpper(strings.TrimSpace(row.MunicipiPaisISO2))
			key := normalizeKey(row.MunicipiNom, iso2)
			if key != "" {
				munID = munMap[key]
			}
			if munID == 0 {
				munID = munNameMap[normalizeKey(row.MunicipiNom)]
			}
			if munID == 0 {
				errors++
				Errorf("Arxius import: municipi no trobat (%s, %s) per arxiu %s", row.MunicipiNom, row.MunicipiPaisISO2, row.Nom)
				continue
			}
		}
		entID := 0
		if strings.TrimSpace(row.EntitatNom) != "" {
			entID = entNameMap[normalizeKey(row.EntitatNom)]
			if entID == 0 {
				entID = entMap[normalizeKey(row.EntitatNom, row.Tipus)]
			}
		}
		key := arxiuImportKey(nameKey, munID)
		if _, ok := seenPayload[key]; ok {
			skipped++
			duplicates++
			continue
		}
		seenPayload[key] = struct{}{}
		if _, ok := existingByName[nameKey]; ok {
			skipped++
			continue
		}
		arxiu := db.Arxiu{
			Nom:              row.Nom,
			Tipus:            row.Tipus,
			Acces:            row.Acces,
			Adreca:           row.Adreca,
			Ubicacio:         row.Ubicacio,
			What3Words:       row.What3Words,
			Web:              row.Web,
			Notes:            row.Notes,
			AcceptaDonacions: row.AcceptaDonacions && strings.TrimSpace(row.DonacionsURL) != "",
			DonacionsURL:     strings.TrimSpace(row.DonacionsURL),
			CreatedBy:        sqlNullIntFromInt(user.ID),
			ModeracioEstat:   "pendent",
			ModeratedBy:      sql.NullInt64{},
			ModeratedAt:      sql.NullTime{},
			ModeracioMotiu:   "",
		}
		if munID > 0 {
			arxiu.MunicipiID = sql.NullInt64{Int64: int64(munID), Valid: true}
		}
		if entID > 0 {
			arxiu.EntitatEclesiasticaID = sql.NullInt64{Int64: int64(entID), Valid: true}
		}
		toInsert = append(toInsert, arxiu)
	}
	resolveDuration := time.Since(resolveStart)

	insertStart := time.Now()
	bulkMode := "generic"
	if len(toInsert) > 0 {
		applyInserted := func(ids []int) {
			for _, id := range ids {
				created++
				activityCount++
				addActivity(activityRuleArxiu, id)
			}
		}
		var ids []int
		var err error
		bulkAttempted := false
		if hasBulkInserter {
			bulkAttempted = true
			ids, bulkMode, err = bulkInserter.BulkInsertArxius(r.Context(), toInsert)
			if bulkMode == "" {
				bulkMode = "bulk"
			}
		}
		if err == nil && len(ids) == len(toInsert) {
			applyInserted(ids)
		} else {
			if err != nil && bulkAttempted {
				Errorf("Arxius import: bulk insert fallit (%s): %v", bulkMode, err)
			}
			bulkMode = "generic"
			for i := range toInsert {
				arxiu := toInsert[i]
				newID, err := a.DB.CreateArxiu(&arxiu)
				if err != nil {
					errors++
					Errorf("Arxius import: error creant arxiu %s: %v", arxiu.Nom, err)
					continue
				}
				created++
				activityCount++
				addActivity(activityRuleArxiu, newID)
			}
		}
	}
	insertDuration := time.Since(insertStart)

	activityStart := time.Now()
	activityMode := "bulk"
	if len(pendingActivities) > 0 {
		mode, err := a.DB.BulkInsertUserActivities(r.Context(), pendingActivities)
		if err != nil {
			Errorf("Arxius import: bulk insert activitats fallit (%s): %v", mode, err)
			activityMode = "generic"
			for i := range pendingActivities {
				act := pendingActivities[i]
				if _, err := a.DB.InsertUserActivity(&act); err != nil {
					Errorf("Arxius import: insert activitat fallit: %v", err)
				}
			}
		} else if mode != "" {
			activityMode = mode
		}
	}
	activityDuration := time.Since(activityStart)

	if activityCount > 0 {
		now := time.Now()
		a.EvaluateAchievementsForUser(context.Background(), user.ID, AchievementTrigger{CreatedAt: now})
		a.logAntiAbuseSignals(user.ID, now)
	}
	totalDuration := time.Since(start)
	resolveSummary := fmt.Sprintf("mun=%s:%d ent=%s:%d arxius=%s:%d", munMode, munKeys, entMode, entKeys, existingMode, existingKeys)
	Infof("Arxius import: engine=%s mode=%s arxius=%d resolve=%s activity=%s prep=%s resolve_dur=%s insert_dur=%s activity_dur=%s totals=%d created=%d skipped=%d duplicates=%d errors=%d duration=%s",
		engine,
		bulkMode,
		total,
		resolveSummary,
		activityMode,
		prepDuration.String(),
		resolveDuration.String(),
		insertDuration.String(),
		activityDuration.String(),
		total,
		created,
		skipped,
		duplicates,
		errors,
		totalDuration.String(),
	)

	redirect := withQueryParams(returnTo, map[string]string{
		"import":         "1",
		"arxius_total":   strconv.Itoa(total),
		"arxius_created": strconv.Itoa(created),
		"arxius_skipped": strconv.Itoa(skipped),
		"arxius_errors":  strconv.Itoa(errors),
	})
	status := adminImportStatusOK
	if errors > 0 {
		status = adminImportStatusError
	}
	a.logAdminImportRun(r, "arxius", status, user.ID)
	http.Redirect(w, r, redirect, http.StatusSeeOther)
}

type arxiusBulkInserter interface {
	BulkInsertArxius(ctx context.Context, rows []db.Arxiu) ([]int, string, error)
}

func arxiuImportKey(nameKey string, municipiID int) string {
	_ = municipiID
	return nameKey
}

func (a *App) arxiuEntitatNameMapsForPayload(records []arxiuExportRecord) (map[string]int, map[string]int, string, int) {
	namesSet := map[string]struct{}{}
	for _, row := range records {
		name := strings.TrimSpace(row.EntitatNom)
		if name == "" {
			continue
		}
		namesSet[strings.ToLower(name)] = struct{}{}
	}
	keysCount := len(namesSet)
	if keysCount == 0 {
		return map[string]int{}, map[string]int{}, "empty", 0
	}
	names := make([]string, 0, keysCount)
	for name := range namesSet {
		names = append(names, name)
	}
	const batchSize = 500
	entMap := map[string]int{}
	nameMap := map[string]int{}
	for i := 0; i < len(names); i += batchSize {
		end := i + batchSize
		if end > len(names) {
			end = len(names)
		}
		batch := names[i:end]
		rows, err := a.DB.ResolveArquebisbatsByNames(batch)
		if err != nil {
			Errorf("Arxius import: resolucio entitats fallida: %v", err)
			entMap, nameMap := a.arxiuEntitatNameMapsFallback()
			return entMap, nameMap, "fallback", keysCount
		}
		for _, row := range rows {
			key := normalizeKey(row.Nom, row.TipusEntitat)
			if key != "" {
				entMap[key] = row.ID
			}
			nameKey := normalizeKey(row.Nom)
			if nameKey != "" {
				nameMap[nameKey] = row.ID
			}
		}
	}
	return entMap, nameMap, "payload", keysCount
}

func (a *App) arxiuEntitatNameMapsFallback() (map[string]int, map[string]int) {
	entitats, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	entMap := map[string]int{}
	nameMap := map[string]int{}
	for _, ent := range entitats {
		key := normalizeKey(ent.Nom, ent.TipusEntitat)
		if key != "" {
			entMap[key] = ent.ID
		}
		nameKey := normalizeKey(ent.Nom)
		if nameKey != "" {
			nameMap[nameKey] = ent.ID
		}
	}
	return entMap, nameMap
}

func (a *App) arxiuMunicipiNameMapsForPayload(records []arxiuExportRecord) (map[string]int, map[string]int, string, int) {
	namesSet := map[string]struct{}{}
	for _, row := range records {
		name := strings.TrimSpace(row.MunicipiNom)
		if name == "" {
			continue
		}
		namesSet[strings.ToLower(name)] = struct{}{}
	}
	keysCount := len(namesSet)
	if keysCount == 0 {
		return map[string]int{}, map[string]int{}, "empty", 0
	}
	names := make([]string, 0, keysCount)
	for name := range namesSet {
		names = append(names, name)
	}
	const batchSize = 500
	res := map[string]int{}
	nameMap := map[string]int{}
	for i := 0; i < len(names); i += batchSize {
		end := i + batchSize
		if end > len(names) {
			end = len(names)
		}
		batch := names[i:end]
		rows, err := a.DB.ResolveMunicipisByNames(batch)
		if err != nil {
			Errorf("Arxius import: resolucio municipis fallida: %v", err)
			fallbackMap := a.municipiNameMap()
			fallbackNameMap := a.municipiNameOnlyMap()
			return fallbackMap, fallbackNameMap, "fallback", keysCount
		}
		for _, row := range rows {
			iso := ""
			if row.ISO2.Valid {
				iso = strings.ToUpper(strings.TrimSpace(row.ISO2.String))
			}
			key := normalizeKey(row.Nom, iso)
			if key != "" {
				res[key] = row.ID
			}
			nameKey := normalizeKey(row.Nom)
			if nameKey != "" {
				nameMap[nameKey] = row.ID
			}
		}
	}
	return res, nameMap, "payload", keysCount
}

func (a *App) municipiNameOnlyMap() map[string]int {
	rows, _ := a.DB.ListMunicipis(db.MunicipiFilter{})
	res := map[string]int{}
	for _, row := range rows {
		key := normalizeKey(row.Nom)
		if key != "" {
			res[key] = row.ID
		}
	}
	return res
}

func (a *App) arxiuExistingMapsForPayload(records []arxiuExportRecord) (map[string]struct{}, map[string]struct{}, string, int) {
	namesSet := map[string]struct{}{}
	for _, row := range records {
		name := strings.TrimSpace(row.Nom)
		if name == "" {
			continue
		}
		namesSet[strings.ToLower(name)] = struct{}{}
	}
	keysCount := len(namesSet)
	if keysCount == 0 {
		return map[string]struct{}{}, map[string]struct{}{}, "empty", 0
	}
	names := make([]string, 0, keysCount)
	for name := range namesSet {
		names = append(names, name)
	}
	const batchSize = 500
	byName := map[string]struct{}{}
	byKey := map[string]struct{}{}
	for i := 0; i < len(names); i += batchSize {
		end := i + batchSize
		if end > len(names) {
			end = len(names)
		}
		batch := names[i:end]
		rows, err := a.DB.ResolveArxiusByNames(batch)
		if err != nil {
			Errorf("Arxius import: resolucio arxius fallida: %v", err)
			byName, byKey := a.arxiuExistingMapsFallback()
			return byName, byKey, "fallback", keysCount
		}
		for _, row := range rows {
			nameKey := normalizeKey(row.Nom)
			if nameKey == "" {
				continue
			}
			byName[nameKey] = struct{}{}
			if row.MunicipiID.Valid {
				key := arxiuImportKey(nameKey, int(row.MunicipiID.Int64))
				byKey[key] = struct{}{}
			}
		}
	}
	return byName, byKey, "payload", keysCount
}

func (a *App) arxiuExistingMapsFallback() (map[string]struct{}, map[string]struct{}) {
	byName := map[string]struct{}{}
	byKey := map[string]struct{}{}
	rows, _ := a.DB.ListArxius(db.ArxiuFilter{})
	for _, row := range rows {
		nameKey := normalizeKey(row.Nom)
		if nameKey == "" {
			continue
		}
		byName[nameKey] = struct{}{}
		if row.MunicipiID.Valid {
			key := arxiuImportKey(nameKey, int(row.MunicipiID.Int64))
			byKey[key] = struct{}{}
		}
	}
	return byName, byKey
}
