package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
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

type arxiusExportPayloadV2 struct {
	Schema     string              `json:"schema"`
	ExportedAt string              `json:"exported_at"`
	Source     arxiusExportSource  `json:"source"`
	Items      arxiusExportItemsV2 `json:"items"`
}

type arxiusExportSource struct {
	App    string `json:"app"`
	Module string `json:"module"`
}

type arxiusExportItemsV2 struct {
	Arxius []arxiuExportRecordV2 `json:"arxius"`
}

type arxiuExportRecordV2 struct {
	Code                string                      `json:"code,omitempty"`
	Name                string                      `json:"name"`
	Type                string                      `json:"type,omitempty"`
	Access              string                      `json:"access,omitempty"`
	Address             string                      `json:"address,omitempty"`
	Location            string                      `json:"location,omitempty"`
	What3Words          string                      `json:"what3words,omitempty"`
	Web                 string                      `json:"web,omitempty"`
	Notes               string                      `json:"notes,omitempty"`
	AcceptsDonations    bool                        `json:"accepts_donations,omitempty"`
	DonationsURL        string                      `json:"donations_url,omitempty"`
	Municipality        *arxiuExportMunicipalityV2  `json:"municipality,omitempty"`
	ReligiousEntityRefs []arxiuReligiousEntityRefV2 `json:"religious_entity_refs,omitempty"`
	Legacy              *arxiuExportLegacyV2        `json:"legacy,omitempty"`
}

type arxiuExportMunicipalityV2 struct {
	Name        string `json:"name"`
	CountryISO2 string `json:"country_iso2,omitempty"`
}

type arxiuExportLegacyV2 struct {
	OldID      int    `json:"old_id,omitempty"`
	EntitatNom string `json:"entitat_nom,omitempty"`
}

type arxiuReligiousEntityRefV2 struct {
	EntityCode       string `json:"entity_code"`
	ReligionCode     string `json:"religion_code,omitempty"`
	LevelCode        string `json:"level_code,omitempty"`
	RelationType     string `json:"relation_type,omitempty"`
	State            string `json:"state,omitempty"`
	ModerationStatus string `json:"moderation_status,omitempty"`
	AnyInici         *int   `json:"any_inici,omitempty"`
	AnyFi            *int   `json:"any_fi,omitempty"`
	Observations     string `json:"observations,omitempty"`
	EntityID         int    `json:"-"`
}

type arxiusImportEnvelope struct {
	Schema string `json:"schema"`
}

type preparedArxiuImportItem struct {
	Archive      db.Arxiu
	CodeKey      string
	NameKey      string
	RelationRefs []arxiuReligiousEntityRefV2
}

func (a *App) AdminArxiusImport(w http.ResponseWriter, r *http.Request) {
	http.NotFound(w, r)
}

func (a *App) AdminArxiusExport(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyDocumentalsArxiusExport, PermissionTarget{}); !ok {
		return
	}
	levelISO := a.levelISOMap()
	arxius, err := a.DB.ListArxius(db.ArxiuFilter{})
	if err != nil {
		http.NotFound(w, r)
		return
	}
	entitatsReligioses, _ := a.DB.ListEntitatsReligioses()
	entitatsByID := make(map[int]db.EntitatReligiosa, len(entitatsReligioses))
	for _, entitat := range entitatsReligioses {
		entitatsByID[entitat.ID] = entitat
	}
	payload := arxiusExportPayloadV2{
		Schema:     "cercagenealogica.arxius.v2",
		ExportedAt: time.Now().Format(time.RFC3339),
		Source: arxiusExportSource{
			App:    "CercaGenealogica",
			Module: "arxius",
		},
	}
	for _, row := range arxius {
		var iso2 string
		var municipality *arxiuExportMunicipalityV2
		if row.MunicipiNom.Valid && row.MunicipiID.Valid {
			if mun, err := a.DB.GetMunicipi(int(row.MunicipiID.Int64)); err == nil && mun != nil {
				iso2 = municipiISO2(mun, levelISO)
				municipality = &arxiuExportMunicipalityV2{
					Name:        row.MunicipiNom.String,
					CountryISO2: iso2,
				}
			}
		}
		record := arxiuExportRecordV2{
			Code:             strings.TrimSpace(row.Codi),
			Name:             row.Nom,
			Type:             row.Tipus,
			Access:           row.Acces,
			Address:          row.Adreca,
			Location:         row.Ubicacio,
			What3Words:       row.What3Words,
			Web:              row.Web,
			Notes:            row.Notes,
			AcceptsDonations: row.AcceptaDonacions,
			DonationsURL:     row.DonacionsURL,
			Municipality:     municipality,
			Legacy: &arxiuExportLegacyV2{
				OldID:      row.ID,
				EntitatNom: row.EntitatNom.String,
			},
		}
		relacions, err := a.DB.ListArxiuEntitatsReligioses(row.ID, 0, "")
		if err == nil {
			for _, rel := range relacions {
				entitat, ok := entitatsByID[rel.EntitatReligiosaID]
				if !ok || strings.TrimSpace(entitat.Codi) == "" {
					continue
				}
				ref := arxiuReligiousEntityRefV2{
					EntityCode:       strings.TrimSpace(entitat.Codi),
					ReligionCode:     strings.TrimSpace(entitat.ReligioConfessioCodi),
					LevelCode:        strings.TrimSpace(entitat.NivellConfessionalCodi),
					RelationType:     strings.TrimSpace(rel.TipusRelacio),
					State:            strings.TrimSpace(rel.Estat),
					ModerationStatus: strings.TrimSpace(rel.ModeracioEstat),
					Observations:     strings.TrimSpace(rel.Observacions),
				}
				if rel.AnyInici.Valid {
					value := int(rel.AnyInici.Int64)
					ref.AnyInici = &value
				}
				if rel.AnyFi.Valid {
					value := int(rel.AnyFi.Int64)
					ref.AnyFi = &value
				}
				record.ReligiousEntityRefs = append(record.ReligiousEntityRefs, ref)
			}
		}
		payload.Items.Arxius = append(payload.Items.Arxius, record)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename=arxius-export.json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(payload)
}

func (a *App) AdminArxiusImportRun(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsArxiusImport, PermissionTarget{})
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

	rawPayload, err := io.ReadAll(file)
	if err != nil {
		a.logAdminImportRun(r, "arxius", adminImportStatusError, user.ID)
		http.Redirect(w, r, withQueryParams(returnTo, map[string]string{"err": "1"}), http.StatusSeeOther)
		return
	}
	importFormat, legacyPayload, v2Payload, err := decodeArxiusImportPayload(rawPayload)
	if err != nil {
		a.logAdminImportRun(r, "arxius", adminImportStatusError, user.ID)
		http.Redirect(w, r, withQueryParams(returnTo, map[string]string{"err": "1"}), http.StatusSeeOther)
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
	pendingActivities := make([]db.UserActivity, 0, 16)
	addActivity := func(rule activityRule, objectType string, objectID int) {
		if objectID <= 0 {
			return
		}
		pendingActivities = append(pendingActivities, db.UserActivity{
			UserID:     user.ID,
			RuleID:     rule.ruleID,
			Action:     "crear",
			ObjectType: objectType,
			ObjectID:   sql.NullInt64{Int64: int64(objectID), Valid: true},
			Points:     rule.points,
			Status:     "pendent",
			Details:    "import",
		})
	}
	activityCount := 0

	total, relationRequested := 0, 0
	created, skipped, errors, duplicates, relationsCreated := 0, 0, 0, 0, 0
	prepStart := time.Now()
	resolveSummary := ""
	prepared := make([]preparedArxiuImportItem, 0)
	switch importFormat {
	case "v2":
		total = len(v2Payload.Items.Arxius)
		for _, row := range v2Payload.Items.Arxius {
			relationRequested += len(row.ReligiousEntityRefs)
		}
		prepared, resolveSummary, skipped, errors, duplicates = a.prepareArxiusV2Import(v2Payload.Items.Arxius, user.ID)
	default:
		total = len(legacyPayload.Arxius)
		prepared, resolveSummary, skipped, errors, duplicates = a.prepareArxiusLegacyImport(legacyPayload.Arxius, user.ID)
	}
	prepDuration := time.Since(prepStart)
	resolveDuration := prepDuration

	insertStart := time.Now()
	bulkMode := "generic"
	createdArchiveIDs := make([]int, len(prepared))
	toInsert := make([]db.Arxiu, 0, len(prepared))
	for _, item := range prepared {
		toInsert = append(toInsert, item.Archive)
	}
	if len(toInsert) > 0 {
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
			copy(createdArchiveIDs, ids)
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
				createdArchiveIDs[i] = newID
			}
		}
	}
	adminTargets := make([]db.AdminJobTarget, 0, len(prepared)*2)
	for idx, id := range createdArchiveIDs {
		if id <= 0 {
			continue
		}
		prepared[idx].Archive.ID = id
		created++
		activityCount++
		addActivity(activityRuleArxiu, "arxiu", id)
		adminTargets = append(adminTargets, db.AdminJobTarget{ObjectType: "arxiu", ObjectID: id})
	}
	insertDuration := time.Since(insertStart)

	relationsStart := time.Now()
	if importFormat == "v2" {
		for idx, item := range prepared {
			arxiuID := createdArchiveIDs[idx]
			if arxiuID <= 0 || len(item.RelationRefs) == 0 {
				continue
			}
			seenRelations := map[string]struct{}{}
			for _, ref := range item.RelationRefs {
				key := arxiuReligiousRefKey(ref)
				if key == "" {
					continue
				}
				if _, exists := seenRelations[key]; exists {
					continue
				}
				seenRelations[key] = struct{}{}
				rel, ok := a.buildPendingArxiuEntitatReligiosaFromRef(arxiuID, user.ID, ref)
				if !ok {
					errors++
					Errorf("Arxius import v2: entitat religiosa no trobada per codi %q (arxiu %s)", ref.EntityCode, item.Archive.Nom)
					continue
				}
				if a.arxiuReligiousRelationExists(rel) {
					skipped++
					continue
				}
				relID, err := a.DB.SaveArxiuEntitatReligiosa(rel)
				if err != nil {
					errors++
					Errorf("Arxius import v2: error creant relacio arxiu-entitat per arxiu %s: %v", item.Archive.Nom, err)
					continue
				}
				relationsCreated++
				activityCount++
				addActivity(activityRuleArxiu, "arxiu_entitat_religiosa", relID)
				adminTargets = append(adminTargets, db.AdminJobTarget{ObjectType: "arxiu_entitat_religiosa", ObjectID: relID})
			}
		}
	}
	relationsDuration := time.Since(relationsStart)

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
	Infof("Arxius import: engine=%s format=%s mode=%s arxius=%d rels=%d resolve=%s activity=%s prep=%s resolve_dur=%s insert_dur=%s rel_dur=%s activity_dur=%s totals=%d created=%d rel_created=%d skipped=%d duplicates=%d errors=%d duration=%s",
		engine,
		importFormat,
		bulkMode,
		total,
		relationRequested,
		resolveSummary,
		activityMode,
		prepDuration.String(),
		resolveDuration.String(),
		insertDuration.String(),
		relationsDuration.String(),
		activityDuration.String(),
		total,
		created,
		relationsCreated,
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
		redirect = withQueryParams(returnTo, map[string]string{
			"import":         "1",
			"err":            "1",
			"arxius_total":   strconv.Itoa(total),
			"arxius_created": strconv.Itoa(created),
			"arxius_skipped": strconv.Itoa(skipped),
			"arxius_errors":  strconv.Itoa(errors),
		})
	}
	a.logAdminImportRunDetailed(r, "arxius", status, user.ID, &adminImportJobDetail{
		Payload: map[string]interface{}{
			"import_type":         "arxius",
			"import_format":       importFormat,
			"archives_requested":  total,
			"relations_requested": relationRequested,
		},
		Result: map[string]interface{}{
			"status":             status,
			"archives_created":   created,
			"relations_created":  relationsCreated,
			"skipped":            skipped,
			"duplicates":         duplicates,
			"errors":             errors,
			"activity_count":     activityCount,
			"admin_target_count": len(adminTargets),
		},
		Targets:       adminTargets,
		ProgressTotal: maxInt(total+relationRequested, 1),
		ProgressDone:  len(adminTargets),
		StartedAt:     start,
		FinishedAt:    time.Now(),
	})
	http.Redirect(w, r, redirect, http.StatusSeeOther)
}

type arxiusBulkInserter interface {
	BulkInsertArxius(ctx context.Context, rows []db.Arxiu) ([]int, string, error)
}

func decodeArxiusImportPayload(raw []byte) (string, arxiusExportPayload, arxiusExportPayloadV2, error) {
	var envelope arxiusImportEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return "", arxiusExportPayload{}, arxiusExportPayloadV2{}, err
	}
	if strings.TrimSpace(envelope.Schema) == "cercagenealogica.arxius.v2" {
		var payload arxiusExportPayloadV2
		if err := json.Unmarshal(raw, &payload); err != nil {
			return "", arxiusExportPayload{}, arxiusExportPayloadV2{}, err
		}
		return "v2", arxiusExportPayload{}, payload, nil
	}
	var payload arxiusExportPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return "", arxiusExportPayload{}, arxiusExportPayloadV2{}, err
	}
	return "legacy", payload, arxiusExportPayloadV2{}, nil
}

func (a *App) prepareArxiusLegacyImport(records []arxiuExportRecord, userID int) ([]preparedArxiuImportItem, string, int, int, int) {
	entMap, entNameMap, entMode, entKeys := a.arxiuEntitatNameMapsForPayload(records)
	munMap, munNameMap, munMode, munKeys := a.arxiuMunicipiNameMapsForPayload(records)
	existingByName, _, existingMode, existingKeys := a.arxiuExistingMapsForPayload(records)
	resolveSummary := fmt.Sprintf("mun=%s:%d ent_legacy=%s:%d arxius_nom=%s:%d", munMode, munKeys, entMode, entKeys, existingMode, existingKeys)
	prepared := make([]preparedArxiuImportItem, 0, len(records))
	seenPayload := map[string]struct{}{}
	skipped, errors, duplicates := 0, 0, 0
	for _, row := range records {
		nameKey := normalizeKey(row.Nom)
		if nameKey == "" {
			skipped++
			duplicates++
			continue
		}
		munID, ok := resolveLegacyArxiuMunicipi(row, munMap, munNameMap)
		if !ok {
			errors++
			Errorf("Arxius import: municipi no trobat (%s, %s) per arxiu %s", row.MunicipiNom, row.MunicipiPaisISO2, row.Nom)
			continue
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
		arxiu := buildPendingImportedArxiu(userID)
		arxiu.Nom = row.Nom
		arxiu.Tipus = row.Tipus
		arxiu.Acces = row.Acces
		arxiu.Adreca = row.Adreca
		arxiu.Ubicacio = row.Ubicacio
		arxiu.What3Words = row.What3Words
		arxiu.Web = row.Web
		arxiu.Notes = row.Notes
		arxiu.AcceptaDonacions = row.AcceptaDonacions && strings.TrimSpace(row.DonacionsURL) != ""
		arxiu.DonacionsURL = strings.TrimSpace(row.DonacionsURL)
		if munID > 0 {
			arxiu.MunicipiID = sql.NullInt64{Int64: int64(munID), Valid: true}
		}
		if entID > 0 {
			arxiu.EntitatEclesiasticaID = sql.NullInt64{Int64: int64(entID), Valid: true}
		}
		prepared = append(prepared, preparedArxiuImportItem{
			Archive: arxiu,
			NameKey: nameKey,
		})
	}
	return prepared, resolveSummary, skipped, errors, duplicates
}

func (a *App) prepareArxiusV2Import(records []arxiuExportRecordV2, userID int) ([]preparedArxiuImportItem, string, int, int, int) {
	legacyMirror := make([]arxiuExportRecord, 0, len(records))
	codesSet := map[string]struct{}{}
	entityCodesSet := map[string]struct{}{}
	for _, row := range records {
		legacyRow := arxiuExportRecord{
			Nom:   row.Name,
			Tipus: row.Type,
		}
		if row.Municipality != nil {
			legacyRow.MunicipiNom = row.Municipality.Name
			legacyRow.MunicipiPaisISO2 = row.Municipality.CountryISO2
		}
		if row.Legacy != nil {
			legacyRow.EntitatNom = row.Legacy.EntitatNom
		}
		legacyMirror = append(legacyMirror, legacyRow)
		if code := normalizeConfessionalCode(row.Code); code != "" {
			codesSet[code] = struct{}{}
		}
		for _, ref := range row.ReligiousEntityRefs {
			if code := normalizeConfessionalCode(ref.EntityCode); code != "" {
				entityCodesSet[code] = struct{}{}
			}
		}
	}
	entMap, entNameMap, entMode, entKeys := a.arxiuEntitatNameMapsForPayload(legacyMirror)
	munMap, munNameMap, munMode, munKeys := a.arxiuMunicipiNameMapsForPayload(legacyMirror)
	existingByName, _, existingMode, existingKeys := a.arxiuExistingMapsForPayload(legacyMirror)
	existingByCode := map[string]db.ArxiuResolveRow{}
	if len(codesSet) > 0 {
		codes := make([]string, 0, len(codesSet))
		for code := range codesSet {
			codes = append(codes, code)
		}
		if rows, err := a.DB.ResolveArxiusByCodes(codes); err == nil {
			for _, row := range rows {
				if key := normalizeConfessionalCode(row.Codi); key != "" {
					existingByCode[key] = row
				}
			}
		} else {
			Errorf("Arxius import v2: resolucio arxius per codi fallida: %v", err)
		}
	}
	entityByCode := map[string]db.EntitatReligiosaResolveRow{}
	if len(entityCodesSet) > 0 {
		codes := make([]string, 0, len(entityCodesSet))
		for code := range entityCodesSet {
			codes = append(codes, code)
		}
		if rows, err := a.DB.ResolveEntitatsReligiosesByCodes(codes); err == nil {
			for _, row := range rows {
				if key := normalizeConfessionalCode(row.Codi); key != "" {
					entityByCode[key] = row
				}
			}
		} else {
			Errorf("Arxius import v2: resolucio entitats religioses per codi fallida: %v", err)
		}
	}
	resolveSummary := fmt.Sprintf("mun=%s:%d ent_legacy=%s:%d arxius_nom=%s:%d arxius_codi=%d entitats_codi=%d", munMode, munKeys, entMode, entKeys, existingMode, existingKeys, len(existingByCode), len(entityByCode))
	prepared := make([]preparedArxiuImportItem, 0, len(records))
	seenPayload := map[string]struct{}{}
	skipped, errors, duplicates := 0, 0, 0
	for _, row := range records {
		nameKey := normalizeKey(row.Name)
		if nameKey == "" {
			skipped++
			duplicates++
			continue
		}
		codeKey := normalizeConfessionalCode(row.Code)
		if codeKey == "" {
			codeKey = normalizeConfessionalCode(row.Name)
		}
		munID := 0
		if row.Municipality != nil {
			var ok bool
			legacyRow := arxiuExportRecord{
				MunicipiNom:      row.Municipality.Name,
				MunicipiPaisISO2: row.Municipality.CountryISO2,
			}
			munID, ok = resolveLegacyArxiuMunicipi(legacyRow, munMap, munNameMap)
			if !ok {
				errors++
				Errorf("Arxius import v2: municipi no trobat (%s, %s) per arxiu %s", row.Municipality.Name, row.Municipality.CountryISO2, row.Name)
				continue
			}
		}
		payloadKey := codeKey
		if payloadKey == "" {
			payloadKey = arxiuImportKey(nameKey, munID)
		}
		if _, ok := seenPayload[payloadKey]; ok {
			skipped++
			duplicates++
			continue
		}
		seenPayload[payloadKey] = struct{}{}
		if codeKey != "" {
			if _, ok := existingByCode[codeKey]; ok {
				skipped++
				continue
			}
		} else if _, ok := existingByName[nameKey]; ok {
			skipped++
			continue
		}
		if _, ok := existingByName[nameKey]; ok && strings.TrimSpace(row.Code) == "" {
			skipped++
			continue
		}
		entitatLegacyID := 0
		if row.Legacy != nil && strings.TrimSpace(row.Legacy.EntitatNom) != "" {
			entitatLegacyID = entNameMap[normalizeKey(row.Legacy.EntitatNom)]
			if entitatLegacyID == 0 {
				entitatLegacyID = entMap[normalizeKey(row.Legacy.EntitatNom, row.Type)]
			}
		}
		refs := make([]arxiuReligiousEntityRefV2, 0, len(row.ReligiousEntityRefs))
		missingEntity := false
		for _, ref := range row.ReligiousEntityRefs {
			code := normalizeConfessionalCode(ref.EntityCode)
			entity, ok := entityByCode[code]
			if code == "" || !ok {
				errors++
				missingEntity = true
				Errorf("Arxius import v2: entitat religiosa no trobada per codi %q (arxiu %s)", ref.EntityCode, row.Name)
				break
			}
			ref.EntityCode = code
			ref.EntityID = entity.ID
			refs = append(refs, ref)
		}
		if missingEntity {
			continue
		}
		arxiu := buildPendingImportedArxiu(userID)
		arxiu.Codi = codeKey
		arxiu.Nom = row.Name
		arxiu.Tipus = row.Type
		arxiu.Acces = row.Access
		arxiu.Adreca = row.Address
		arxiu.Ubicacio = row.Location
		arxiu.What3Words = row.What3Words
		arxiu.Web = row.Web
		arxiu.Notes = row.Notes
		arxiu.AcceptaDonacions = row.AcceptsDonations && strings.TrimSpace(row.DonationsURL) != ""
		arxiu.DonacionsURL = strings.TrimSpace(row.DonationsURL)
		if munID > 0 {
			arxiu.MunicipiID = sql.NullInt64{Int64: int64(munID), Valid: true}
		}
		if entitatLegacyID > 0 {
			arxiu.EntitatEclesiasticaID = sql.NullInt64{Int64: int64(entitatLegacyID), Valid: true}
		}
		prepared = append(prepared, preparedArxiuImportItem{
			Archive:      arxiu,
			CodeKey:      codeKey,
			NameKey:      nameKey,
			RelationRefs: refs,
		})
	}
	return prepared, resolveSummary, skipped, errors, duplicates
}

func buildPendingImportedArxiu(userID int) db.Arxiu {
	return db.Arxiu{
		CreatedBy:      sqlNullIntFromInt(userID),
		ModeracioEstat: "pendent",
		ModeratedBy:    sql.NullInt64{},
		ModeratedAt:    sql.NullTime{},
		ModeracioMotiu: "",
	}
}

func resolveLegacyArxiuMunicipi(row arxiuExportRecord, munMap, munNameMap map[string]int) (int, bool) {
	if strings.TrimSpace(row.MunicipiNom) == "" {
		return 0, true
	}
	iso2 := strings.ToUpper(strings.TrimSpace(row.MunicipiPaisISO2))
	key := normalizeKey(row.MunicipiNom, iso2)
	if key != "" {
		if munID := munMap[key]; munID > 0 {
			return munID, true
		}
	}
	munID := munNameMap[normalizeKey(row.MunicipiNom)]
	return munID, munID > 0
}

func arxiuReligiousRefKey(ref arxiuReligiousEntityRefV2) string {
	code := normalizeConfessionalCode(ref.EntityCode)
	if code == "" {
		return ""
	}
	parts := []string{
		code,
		strings.TrimSpace(ref.RelationType),
		fmt.Sprintf("%d", derefInt(ref.AnyInici)),
		fmt.Sprintf("%d", derefInt(ref.AnyFi)),
	}
	return strings.Join(parts, "|")
}

func derefInt(v *int) int {
	if v == nil {
		return 0
	}
	return *v
}

func (a *App) buildPendingArxiuEntitatReligiosaFromRef(arxiuID, userID int, ref arxiuReligiousEntityRefV2) (*db.ArxiuEntitatReligiosa, bool) {
	if ref.EntityID <= 0 {
		return nil, false
	}
	author := sqlNullIntFromInt(userID)
	rel := &db.ArxiuEntitatReligiosa{
		ArxiuID:            arxiuID,
		EntitatReligiosaID: ref.EntityID,
		TipusRelacio:       strings.TrimSpace(ref.RelationType),
		Estat:              normalizeArxiuEntitatReligiosaEstat(ref.State),
		ModeracioEstat:     "pendent",
		ModeracioMotiu:     "",
		CreatedBy:          author,
		UpdatedBy:          author,
		ModeratedBy:        sql.NullInt64{},
		ModeratedAt:        sql.NullTime{},
		Observacions:       strings.TrimSpace(ref.Observations),
	}
	if rel.TipusRelacio == "" {
		rel.TipusRelacio = "arxiu_institucional"
	}
	if ref.AnyInici != nil {
		rel.AnyInici = sql.NullInt64{Int64: int64(*ref.AnyInici), Valid: true}
	}
	if ref.AnyFi != nil {
		rel.AnyFi = sql.NullInt64{Int64: int64(*ref.AnyFi), Valid: true}
	}
	return rel, true
}

func (a *App) arxiuReligiousRelationExists(rel *db.ArxiuEntitatReligiosa) bool {
	if rel == nil {
		return false
	}
	rows, err := a.DB.ListArxiuEntitatsReligioses(rel.ArxiuID, rel.EntitatReligiosaID, "")
	if err != nil {
		return false
	}
	for _, row := range rows {
		if row.ModeracioEstat == "rebutjat" {
			continue
		}
		if row.TipusRelacio == rel.TipusRelacio && nullIntEqual(row.AnyInici, rel.AnyInici) && nullIntEqual(row.AnyFi, rel.AnyFi) {
			return true
		}
	}
	return false
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
