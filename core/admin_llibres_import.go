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

type llibresExportPayload struct {
	Version    int                  `json:"version"`
	ExportedAt string               `json:"exported_at"`
	Llibres    []llibreExportRecord `json:"llibres"`
}

type llibreExportRecord struct {
	ID                int                 `json:"id"`
	Titol             string              `json:"titol,omitempty"`
	NomEsglesia       string              `json:"nom_esglesia,omitempty"`
	TipusLlibre       string              `json:"tipus_llibre,omitempty"`
	Cronologia        string              `json:"cronologia,omitempty"`
	Volum             string              `json:"volum,omitempty"`
	Abat              string              `json:"abat,omitempty"`
	Contingut         string              `json:"contingut,omitempty"`
	Llengua           string              `json:"llengua,omitempty"`
	Requeriments      string              `json:"requeriments_tecnics,omitempty"`
	UnitatCatalogacio string              `json:"unitat_catalogacio,omitempty"`
	UnitatInstalacio  string              `json:"unitat_instalacio,omitempty"`
	Pagines           *int                `json:"pagines,omitempty"`
	URLBase           string              `json:"url_base,omitempty"`
	URLImatgePrefix   string              `json:"url_imatge_prefix,omitempty"`
	Pagina            string              `json:"pagina,omitempty"`
	IndexacioCompleta bool                `json:"indexacio_completa,omitempty"`
	CodiDigital       string              `json:"codi_digital,omitempty"`
	CodiFisic         string              `json:"codi_fisic,omitempty"`
	MunicipiNom       string              `json:"municipi_nom"`
	MunicipiPaisISO2  string              `json:"municipi_pais_iso2,omitempty"`
	ArquebisbatNom    string              `json:"arquebisbat_nom,omitempty"`
	ArquebisbatTipus  string              `json:"arquebisbat_tipus,omitempty"`
	Arxius            []llibreExportArxiu `json:"arxius,omitempty"`
	URLs              []llibreExportURL   `json:"urls,omitempty"`
}

type llibreExportArxiu struct {
	Nom         string `json:"nom"`
	Signatura   string `json:"signatura,omitempty"`
	URLOverride string `json:"url_override,omitempty"`
}

type llibreExportURL struct {
	URL        string `json:"url"`
	Tipus      string `json:"tipus,omitempty"`
	Descripcio string `json:"descripcio,omitempty"`
	ArxiuNom   string `json:"arxiu_nom,omitempty"`
}

func (a *App) AdminLlibresImport(w http.ResponseWriter, r *http.Request) {
	http.NotFound(w, r)
}

func (a *App) AdminLlibresExport(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresExport, PermissionTarget{}); !ok {
		return
	}
	llibres, err := a.DB.ListLlibres(db.LlibreFilter{})
	if err != nil {
		http.NotFound(w, r)
		return
	}
	entitats, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	entMap := map[int]db.Arquebisbat{}
	for _, ent := range entitats {
		entMap[ent.ID] = db.Arquebisbat{Nom: ent.Nom, TipusEntitat: ent.TipusEntitat}
	}
	municipiISO := a.municipiISOMapByID()
	payload := llibresExportPayload{
		Version:    1,
		ExportedAt: time.Now().Format(time.RFC3339),
	}
	for _, row := range llibres {
		rec := llibreExportRecord{
			ID:                row.ID,
			Titol:             strings.TrimSpace(row.Titol),
			NomEsglesia:       strings.TrimSpace(row.NomEsglesia),
			TipusLlibre:       strings.TrimSpace(row.TipusLlibre),
			Cronologia:        strings.TrimSpace(row.Cronologia),
			Volum:             strings.TrimSpace(row.Volum),
			Abat:              strings.TrimSpace(row.Abat),
			Contingut:         strings.TrimSpace(row.Contingut),
			Llengua:           strings.TrimSpace(row.Llengua),
			Requeriments:      strings.TrimSpace(row.Requeriments),
			UnitatCatalogacio: strings.TrimSpace(row.UnitatCatalogacio),
			UnitatInstalacio:  strings.TrimSpace(row.UnitatInstalacio),
			URLBase:           strings.TrimSpace(row.URLBase),
			URLImatgePrefix:   strings.TrimSpace(row.URLImatgePrefix),
			Pagina:            strings.TrimSpace(row.Pagina),
			IndexacioCompleta: row.IndexacioCompleta,
			CodiDigital:       strings.TrimSpace(row.CodiDigital),
			CodiFisic:         strings.TrimSpace(row.CodiFisic),
		}
		if row.Pagines.Valid {
			v := int(row.Pagines.Int64)
			rec.Pagines = &v
		}
		if row.MunicipiNom.Valid {
			rec.MunicipiNom = strings.TrimSpace(row.MunicipiNom.String)
		}
		if iso2, ok := municipiISO[row.MunicipiID]; ok {
			rec.MunicipiPaisISO2 = iso2
		}
		if ent, ok := entMap[row.ArquebisbatID]; ok {
			rec.ArquebisbatNom = strings.TrimSpace(ent.Nom)
			rec.ArquebisbatTipus = strings.TrimSpace(ent.TipusEntitat)
		}
		arxius, _ := a.DB.ListLlibreArxius(row.ID)
		for _, link := range arxius {
			rec.Arxius = append(rec.Arxius, llibreExportArxiu{
				Nom:         strings.TrimSpace(link.ArxiuNom.String),
				Signatura:   strings.TrimSpace(link.Signatura.String),
				URLOverride: strings.TrimSpace(link.URLOverride.String),
			})
		}
		urls, _ := a.DB.ListLlibreURLs(row.ID)
		for _, link := range urls {
			typ := ""
			if link.Tipus.Valid {
				typ = strings.TrimSpace(link.Tipus.String)
			}
			desc := ""
			if link.Descripcio.Valid {
				desc = strings.TrimSpace(link.Descripcio.String)
			}
			arxiuNom := ""
			if link.ArxiuNom.Valid {
				arxiuNom = strings.TrimSpace(link.ArxiuNom.String)
			}
			rec.URLs = append(rec.URLs, llibreExportURL{
				URL:        strings.TrimSpace(link.URL),
				Tipus:      typ,
				Descripcio: desc,
				ArxiuNom:   arxiuNom,
			})
		}
		payload.Llibres = append(payload.Llibres, rec)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename=llibres-export.json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(payload)
}

func (a *App) AdminLlibresImportRun(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresImport, PermissionTarget{})
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	start := time.Now()
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		a.logAdminImportRun(r, "llibres", adminImportStatusError, user.ID)
		http.Redirect(w, r, withQueryParams("/admin/llibres/import", map[string]string{"err": "1"}), http.StatusSeeOther)
		return
	}
	returnTo := safeReturnTo(r.FormValue("return_to"), "/admin/llibres/import")
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		a.logAdminImportRun(r, "llibres", adminImportStatusError, user.ID)
		http.Redirect(w, r, withQueryParams(returnTo, map[string]string{"err": "1"}), http.StatusSeeOther)
		return
	}
	file, _, err := r.FormFile("import_file")
	if err != nil {
		a.logAdminImportRun(r, "llibres", adminImportStatusError, user.ID)
		http.Redirect(w, r, withQueryParams(returnTo, map[string]string{"err": "1"}), http.StatusSeeOther)
		return
	}
	defer file.Close()

	var payload llibresExportPayload
	if err := json.NewDecoder(file).Decode(&payload); err != nil {
		a.logAdminImportRun(r, "llibres", adminImportStatusError, user.ID)
		http.Redirect(w, r, withQueryParams(returnTo, map[string]string{"err": "1"}), http.StatusSeeOther)
		return
	}
	engine := territoriImportEngineName(a.DB)
	bulkInserter, hasBulkInserter := a.DB.(llibreBulkInserter)

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
	activityRuleLlibre := resolveActivityRule(ruleLlibreCreate)
	pendingActivities := make([]db.UserActivity, 0, len(payload.Llibres))
	addActivity := func(rule activityRule, objectID int) {
		if objectID <= 0 {
			return
		}
		pendingActivities = append(pendingActivities, db.UserActivity{
			UserID:     user.ID,
			RuleID:     rule.ruleID,
			Action:     "crear",
			ObjectType: "llibre",
			ObjectID:   sql.NullInt64{Int64: int64(objectID), Valid: true},
			Points:     rule.points,
			Status:     "pendent",
			Details:    "import",
		})
	}
	activityCount := 0

	total := len(payload.Llibres)
	created, skipped, errors, duplicates := 0, 0, 0, 0

	prepStart := time.Now()
	entMap, entNameMap, entMode, entKeys := a.llibreEntitatNameMapsForPayload(payload.Llibres)
	munMap, munNameMap, munMode, munKeys := a.llibreMunicipiNameMapsForPayload(payload.Llibres)
	arxiuMap, arxiuMode, arxiuKeys := a.llibreArxiuNameMapForPayload(payload.Llibres)
	prepDuration := time.Since(prepStart)

	type comboInfo struct {
		munID      int
		tipus      string
		cronologia string
		digital    map[string]struct{}
		fisic      map[string]struct{}
	}
	comboByKey := map[string]*comboInfo{}
	seenDigital := map[string]map[string]struct{}{}
	seenFisic := map[string]map[string]struct{}{}

	type llibreImportEntry struct {
		llibre     db.Llibre
		codiDigital string
		codiFisic   string
		comboKey    string
		arxius      []db.ArxiuLlibreLink
		urls        []db.LlibreURL
	}
	entries := make([]llibreImportEntry, 0, len(payload.Llibres))

	resolveStart := time.Now()
	for _, row := range payload.Llibres {
		munID := 0
		if strings.TrimSpace(row.MunicipiNom) != "" {
			munID = munMap[normalizeKey(row.MunicipiNom, strings.ToUpper(row.MunicipiPaisISO2))]
			if munID == 0 {
				munID = munNameMap[normalizeKey(row.MunicipiNom)]
			}
		}
		if munID == 0 {
			errors++
			continue
		}
		entID := 0
		if strings.TrimSpace(row.ArquebisbatNom) != "" {
			entID = entMap[normalizeKey(row.ArquebisbatNom, row.ArquebisbatTipus)]
			if entID == 0 {
				entID = entNameMap[normalizeKey(row.ArquebisbatNom)]
			}
		}
		if entID == 0 {
			errors++
			continue
		}
		tipus := strings.TrimSpace(row.TipusLlibre)
		cronologia := strings.TrimSpace(row.Cronologia)
		codiDigital := strings.TrimSpace(row.CodiDigital)
		codiFisic := strings.TrimSpace(row.CodiFisic)
		comboKey := llibreComboKey(munID, tipus, cronologia)
		hasCodes := (codiDigital != "" || codiFisic != "")
		if hasCodes && tipus != "" && cronologia != "" {
			if codiDigital != "" {
				if seenDigital[comboKey] == nil {
					seenDigital[comboKey] = map[string]struct{}{}
				}
				if _, ok := seenDigital[comboKey][codiDigital]; ok {
					duplicates++
					skipped++
					continue
				}
				seenDigital[comboKey][codiDigital] = struct{}{}
			}
			if codiFisic != "" {
				if seenFisic[comboKey] == nil {
					seenFisic[comboKey] = map[string]struct{}{}
				}
				if _, ok := seenFisic[comboKey][codiFisic]; ok {
					duplicates++
					skipped++
					continue
				}
				seenFisic[comboKey][codiFisic] = struct{}{}
			}
			info, ok := comboByKey[comboKey]
			if !ok {
				info = &comboInfo{
					munID:      munID,
					tipus:      tipus,
					cronologia: cronologia,
					digital:    map[string]struct{}{},
					fisic:      map[string]struct{}{},
				}
				comboByKey[comboKey] = info
			}
			if codiDigital != "" {
				info.digital[codiDigital] = struct{}{}
			}
			if codiFisic != "" {
				info.fisic[codiFisic] = struct{}{}
			}
		}
		var pagines sql.NullInt64
		if row.Pagines != nil && *row.Pagines > 0 {
			pagines = sql.NullInt64{Int64: int64(*row.Pagines), Valid: true}
		}
		llibre := db.Llibre{
			ArquebisbatID:     entID,
			MunicipiID:        munID,
			NomEsglesia:       strings.TrimSpace(row.NomEsglesia),
			CodiDigital:       codiDigital,
			CodiFisic:         codiFisic,
			Titol:             strings.TrimSpace(row.Titol),
			TipusLlibre:       tipus,
			Cronologia:        cronologia,
			Volum:             strings.TrimSpace(row.Volum),
			Abat:              strings.TrimSpace(row.Abat),
			Contingut:         strings.TrimSpace(row.Contingut),
			Llengua:           strings.TrimSpace(row.Llengua),
			Requeriments:      strings.TrimSpace(row.Requeriments),
			UnitatCatalogacio: strings.TrimSpace(row.UnitatCatalogacio),
			UnitatInstalacio:  strings.TrimSpace(row.UnitatInstalacio),
			Pagines:           pagines,
			URLBase:           strings.TrimSpace(row.URLBase),
			URLImatgePrefix:   strings.TrimSpace(row.URLImatgePrefix),
			Pagina:            strings.TrimSpace(row.Pagina),
			IndexacioCompleta: row.IndexacioCompleta,
			CreatedBy:         sqlNullIntFromInt(user.ID),
			ModeracioEstat:    "pendent",
			ModeratedBy:       sql.NullInt64{},
			ModeratedAt:       sql.NullTime{},
			ModeracioMotiu:    "",
		}
		entry := llibreImportEntry{
			llibre:     llibre,
			codiDigital: codiDigital,
			codiFisic:   codiFisic,
			comboKey:    comboKey,
		}
		seenArxiu := map[int]struct{}{}
		for _, link := range row.Arxius {
			if strings.TrimSpace(link.Nom) == "" {
				continue
			}
			arxiuID := arxiuMap[normalizeKey(link.Nom)]
			if arxiuID <= 0 {
				continue
			}
			if _, ok := seenArxiu[arxiuID]; ok {
				continue
			}
			seenArxiu[arxiuID] = struct{}{}
			entry.arxius = append(entry.arxius, db.ArxiuLlibreLink{
				ArxiuID:     arxiuID,
				Signatura:   strings.TrimSpace(link.Signatura),
				URLOverride: strings.TrimSpace(link.URLOverride),
			})
		}
		for _, link := range row.URLs {
			if strings.TrimSpace(link.URL) == "" {
				continue
			}
			arxiuID := sql.NullInt64{}
			if strings.TrimSpace(link.ArxiuNom) != "" {
				if id := arxiuMap[normalizeKey(link.ArxiuNom)]; id > 0 {
					arxiuID = sql.NullInt64{Int64: int64(id), Valid: true}
				}
			}
			entry.urls = append(entry.urls, db.LlibreURL{
				ArxiuID:     arxiuID,
				LlibreRefID: sql.NullInt64{},
				URL:         strings.TrimSpace(link.URL),
				Tipus:       sqlNullString(link.Tipus),
				Descripcio:  sqlNullString(link.Descripcio),
				CreatedBy:   sqlNullIntFromInt(user.ID),
			})
		}
		entries = append(entries, entry)
	}

	existingDigital := map[string]map[string]struct{}{}
	existingFisic := map[string]map[string]struct{}{}
	resolveMode := "payload"
	if engine == "postgres" {
		resolveMode = "payload-pg-array"
	}
	resolveKeys := 0
	for key, info := range comboByKey {
		if info == nil {
			continue
		}
		digital := make([]string, 0, len(info.digital))
		for code := range info.digital {
			digital = append(digital, code)
		}
		fisic := make([]string, 0, len(info.fisic))
		for code := range info.fisic {
			fisic = append(fisic, code)
		}
		if len(digital) == 0 && len(fisic) == 0 {
			continue
		}
		resolveKeys += len(digital) + len(fisic)
		rows, err := a.DB.ResolveLlibresByCodes(info.munID, info.tipus, info.cronologia, digital, fisic)
		if err != nil {
			Errorf("Llibres import: resolucio duplicats fallida: %v", err)
			resolveMode = "fallback"
			existingDigital = map[string]map[string]struct{}{}
			existingFisic = map[string]map[string]struct{}{}
			break
		}
		for _, row := range rows {
			if row.CodiDigital.Valid {
				if existingDigital[key] == nil {
					existingDigital[key] = map[string]struct{}{}
				}
				existingDigital[key][strings.TrimSpace(row.CodiDigital.String)] = struct{}{}
			}
			if row.CodiFisic.Valid {
				if existingFisic[key] == nil {
					existingFisic[key] = map[string]struct{}{}
				}
				existingFisic[key][strings.TrimSpace(row.CodiFisic.String)] = struct{}{}
			}
		}
	}
	if resolveMode == "fallback" {
		for key, info := range comboByKey {
			if info == nil {
				continue
			}
			for code := range info.digital {
				dup, err := a.DB.HasLlibreDuplicate(info.munID, info.tipus, info.cronologia, code, "", 0)
				if err != nil {
					continue
				}
				if dup {
					if existingDigital[key] == nil {
						existingDigital[key] = map[string]struct{}{}
					}
					existingDigital[key][code] = struct{}{}
				}
			}
			for code := range info.fisic {
				dup, err := a.DB.HasLlibreDuplicate(info.munID, info.tipus, info.cronologia, "", code, 0)
				if err != nil {
					continue
				}
				if dup {
					if existingFisic[key] == nil {
						existingFisic[key] = map[string]struct{}{}
					}
					existingFisic[key][code] = struct{}{}
				}
			}
		}
	}

	toInsert := make([]db.Llibre, 0, len(entries))
	insertMeta := make([]llibreImportEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.comboKey != "" {
			if entry.codiDigital != "" {
				if existingDigital[entry.comboKey] != nil {
					if _, ok := existingDigital[entry.comboKey][entry.codiDigital]; ok {
						duplicates++
						skipped++
						continue
					}
				}
			}
			if entry.codiFisic != "" {
				if existingFisic[entry.comboKey] != nil {
					if _, ok := existingFisic[entry.comboKey][entry.codiFisic]; ok {
						duplicates++
						skipped++
						continue
					}
				}
			}
		}
		toInsert = append(toInsert, entry.llibre)
		insertMeta = append(insertMeta, entry)
	}
	resolveDuration := time.Since(resolveStart)

	insertStart := time.Now()
	bulkMode := "generic"
	var insertedIDs []int
	insertedMeta := make([]llibreImportEntry, 0)
	if len(toInsert) > 0 {
		bulkAttempted := false
		var err error
		if hasBulkInserter {
			bulkAttempted = true
			insertedIDs, bulkMode, err = bulkInserter.BulkInsertLlibres(r.Context(), toInsert)
			if bulkMode == "" {
				bulkMode = "bulk"
			}
		}
		if err != nil || len(insertedIDs) != len(toInsert) {
			if err != nil && bulkAttempted {
				Errorf("Llibres import: bulk insert fallit (%s): %v", bulkMode, err)
			}
			bulkMode = "generic"
			insertedIDs = make([]int, 0, len(toInsert))
			insertedMeta = make([]llibreImportEntry, 0, len(toInsert))
			for i := range toInsert {
				llibre := toInsert[i]
				newID, err := a.DB.CreateLlibre(&llibre)
				if err != nil {
					errors++
					continue
				}
				insertedIDs = append(insertedIDs, newID)
				insertedMeta = append(insertedMeta, insertMeta[i])
			}
		} else {
			insertedMeta = insertMeta
		}
	}
	insertDuration := time.Since(insertStart)

	relationsStart := time.Now()
	arxiuLinks := make([]db.ArxiuLlibreLink, 0)
	urlLinks := make([]db.LlibreURL, 0)
	for i, id := range insertedIDs {
		meta := insertedMeta[i]
		created++
		activityCount++
		addActivity(activityRuleLlibre, id)
		for _, link := range meta.arxius {
			link.LlibreID = id
			arxiuLinks = append(arxiuLinks, link)
		}
		for _, link := range meta.urls {
			link.LlibreID = id
			urlLinks = append(urlLinks, link)
		}
	}
	relationsMode := "bulk"
	if len(arxiuLinks) > 0 {
		var mode string
		var err error
		if hasBulkInserter {
			mode, err = bulkInserter.BulkInsertArxiuLlibres(r.Context(), arxiuLinks)
		} else {
			err = fmt.Errorf("bulk inserter unavailable")
		}
		if !hasBulkInserter || err != nil {
			if err != nil && hasBulkInserter {
				Errorf("Llibres import: bulk insert arxiu-llibre fallit (%s): %v", mode, err)
			}
			relationsMode = "generic"
			for _, link := range arxiuLinks {
				if err := a.DB.AddArxiuLlibre(link.ArxiuID, link.LlibreID, link.Signatura, link.URLOverride); err != nil {
					Errorf("Llibres import: error afegint arxiu-llibre: %v", err)
				}
			}
		} else if mode != "" {
			relationsMode = mode
		}
	}
	if len(urlLinks) > 0 {
		var mode string
		var err error
		if hasBulkInserter {
			mode, err = bulkInserter.BulkInsertLlibreURLs(r.Context(), urlLinks)
		} else {
			err = fmt.Errorf("bulk inserter unavailable")
		}
		if !hasBulkInserter || err != nil {
			if err != nil && hasBulkInserter {
				Errorf("Llibres import: bulk insert URLs fallit (%s): %v", mode, err)
			}
			relationsMode = "generic"
			for i := range urlLinks {
				link := urlLinks[i]
				_ = a.DB.AddLlibreURL(&link)
			}
		} else if mode != "" {
			relationsMode = mode
		}
	}
	relationsDuration := time.Since(relationsStart)

	activityStart := time.Now()
	activityMode := "bulk"
	if len(pendingActivities) > 0 {
		mode, err := a.DB.BulkInsertUserActivities(r.Context(), pendingActivities)
		if err != nil {
			Errorf("Llibres import: bulk insert activitats fallit (%s): %v", mode, err)
			activityMode = "generic"
			for i := range pendingActivities {
				act := pendingActivities[i]
				if _, err := a.DB.InsertUserActivity(&act); err != nil {
					Errorf("Llibres import: insert activitat fallit: %v", err)
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
	resolveSummary := fmt.Sprintf("mun=%s:%d ent=%s:%d arxius=%s:%d llibres=%s:%d", munMode, munKeys, entMode, entKeys, arxiuMode, arxiuKeys, resolveMode, resolveKeys)
	Infof("Llibres import: engine=%s mode=%s resolve=%s relations=%s activity=%s prep=%s resolve_dur=%s insert_dur=%s relations_dur=%s activity_dur=%s totals=%d created=%d skipped=%d duplicates=%d errors=%d duration=%s",
		engine,
		bulkMode,
		resolveSummary,
		relationsMode,
		activityMode,
		prepDuration.String(),
		resolveDuration.String(),
		insertDuration.String(),
		relationsDuration.String(),
		activityDuration.String(),
		total,
		created,
		skipped,
		duplicates,
		errors,
		totalDuration.String(),
	)
	redirect := withQueryParams(returnTo, map[string]string{
		"import":          "1",
		"llibres_total":   strconv.Itoa(total),
		"llibres_created": strconv.Itoa(created),
		"llibres_skipped": strconv.Itoa(skipped),
		"llibres_errors":  strconv.Itoa(errors),
	})
	status := adminImportStatusOK
	if errors > 0 {
		status = adminImportStatusError
	}
	a.logAdminImportRun(r, "llibres", status, user.ID)
	http.Redirect(w, r, redirect, http.StatusSeeOther)
}

type llibreBulkInserter interface {
	BulkInsertLlibres(ctx context.Context, rows []db.Llibre) ([]int, string, error)
	BulkInsertArxiuLlibres(ctx context.Context, rows []db.ArxiuLlibreLink) (string, error)
	BulkInsertLlibreURLs(ctx context.Context, rows []db.LlibreURL) (string, error)
}

func llibreComboKey(munID int, tipus, cronologia string) string {
	if munID <= 0 {
		return ""
	}
	tipus = strings.TrimSpace(tipus)
	cronologia = strings.TrimSpace(cronologia)
	if tipus == "" || cronologia == "" {
		return ""
	}
	return strconv.Itoa(munID) + "|" + tipus + "|" + cronologia
}

func (a *App) llibreEntitatNameMapsForPayload(records []llibreExportRecord) (map[string]int, map[string]int, string, int) {
	namesSet := map[string]struct{}{}
	for _, row := range records {
		name := strings.TrimSpace(row.ArquebisbatNom)
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
			Errorf("Llibres import: resolucio entitats fallida: %v", err)
			entMap, nameMap = a.arxiuEntitatNameMapsFallback()
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

func (a *App) llibreMunicipiNameMapsForPayload(records []llibreExportRecord) (map[string]int, map[string]int, string, int) {
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
			Errorf("Llibres import: resolucio municipis fallida: %v", err)
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

func (a *App) llibreArxiuNameMapForPayload(records []llibreExportRecord) (map[string]int, string, int) {
	namesSet := map[string]struct{}{}
	for _, row := range records {
		for _, link := range row.Arxius {
			name := strings.TrimSpace(link.Nom)
			if name == "" {
				continue
			}
			namesSet[strings.ToLower(name)] = struct{}{}
		}
		for _, link := range row.URLs {
			name := strings.TrimSpace(link.ArxiuNom)
			if name == "" {
				continue
			}
			namesSet[strings.ToLower(name)] = struct{}{}
		}
	}
	keysCount := len(namesSet)
	if keysCount == 0 {
		return map[string]int{}, "empty", 0
	}
	names := make([]string, 0, keysCount)
	for name := range namesSet {
		names = append(names, name)
	}
	const batchSize = 500
	arxiuMap := map[string]int{}
	for i := 0; i < len(names); i += batchSize {
		end := i + batchSize
		if end > len(names) {
			end = len(names)
		}
		batch := names[i:end]
		rows, err := a.DB.ResolveArxiusByNames(batch)
		if err != nil {
			Errorf("Llibres import: resolucio arxius fallida: %v", err)
			arxius, _ := a.DB.ListArxius(db.ArxiuFilter{})
			for _, arxiu := range arxius {
				key := normalizeKey(arxiu.Nom)
				if key != "" {
					arxiuMap[key] = arxiu.ID
				}
			}
			return arxiuMap, "fallback", keysCount
		}
		for _, row := range rows {
			key := normalizeKey(row.Nom)
			if key != "" {
				arxiuMap[key] = row.ID
			}
		}
	}
	return arxiuMap, "payload", keysCount
}

func (a *App) municipiISOMapByID() map[int]string {
	levelISO := a.levelISOMap()
	rows, _ := a.DB.ListMunicipis(db.MunicipiFilter{})
	res := map[int]string{}
	for _, row := range rows {
		m, err := a.DB.GetMunicipi(row.ID)
		if err != nil || m == nil {
			continue
		}
		iso2 := municipiISO2(m, levelISO)
		if iso2 != "" {
			res[m.ID] = iso2
		}
	}
	return res
}

func sqlNullString(val string) sql.NullString {
	if strings.TrimSpace(val) == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: strings.TrimSpace(val), Valid: true}
}
