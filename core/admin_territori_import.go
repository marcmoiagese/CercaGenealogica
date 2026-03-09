package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type territoriExportPayload struct {
	Version    int                       `json:"version"`
	ExportedAt string                    `json:"exported_at"`
	Countries  []territoriExportCountry  `json:"countries"`
	Levels     []territoriExportLevel    `json:"levels"`
	Municipis  []territoriExportMunicipi `json:"municipis"`
}

type territoriExportCountry struct {
	ISO2 string `json:"iso2"`
	ISO3 string `json:"iso3"`
	Num  string `json:"num"`
}

type territoriExportLevel struct {
	ID       int    `json:"id"`
	PaisISO2 string `json:"pais_iso2"`
	Nivel    int    `json:"nivel"`
	Nom      string `json:"nom"`
	Tipus    string `json:"tipus"`
	Codi     string `json:"codi"`
	Altres   string `json:"altres"`
	ParentID *int   `json:"parent_id,omitempty"`
	AnyInici *int   `json:"any_inici,omitempty"`
	AnyFi    *int   `json:"any_fi,omitempty"`
	Estat    string `json:"estat"`
}

type territoriExportMunicipi struct {
	ID         int      `json:"id"`
	PaisISO2   string   `json:"pais_iso2,omitempty"`
	Nom        string   `json:"nom"`
	Tipus      string   `json:"tipus"`
	ParentID   *int     `json:"parent_id,omitempty"`
	Nivells    []int    `json:"nivells"`
	CodiPostal string   `json:"codi_postal"`
	Latitud    *float64 `json:"latitud,omitempty"`
	Longitud   *float64 `json:"longitud,omitempty"`
	What3Words string   `json:"what3words"`
	Web        string   `json:"web"`
	Wikipedia  string   `json:"wikipedia"`
	Altres     string   `json:"altres"`
	Estat      string   `json:"estat"`
}

type territoriImportMunicipi struct {
	row       territoriexportMunicipiRow
	municipi  db.Municipi
	oldParent int
}

type territoriexportMunicipiRow struct {
	ID         int
	ParentID   int
	PaisISO2   string
	Nom        string
	Tipus      string
	Nivells    []int
	CodiPostal string
	Latitud    *float64
	Longitud   *float64
	What3Words string
	Web        string
	Wikipedia  string
	Altres     string
	Estat      string
}

func (a *App) AdminTerritoriImport(w http.ResponseWriter, r *http.Request) {
	http.NotFound(w, r)
}

func (a *App) AdminTerritoriExport(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminTerritoriExport, PermissionTarget{}); !ok {
		return
	}
	nivellID := parseIntDefault(strings.TrimSpace(r.URL.Query().Get("nivell_id")), 0)
	paisos, err := a.DB.ListPaisos()
	if err != nil {
		http.NotFound(w, r)
		return
	}
	nivells, err := a.DB.ListNivells(db.NivellAdminFilter{})
	if err != nil {
		http.NotFound(w, r)
		return
	}
	levelISO := map[int]string{}
	levelsByID := map[int]db.NivellAdministratiu{}
	childrenByID := map[int][]int{}
	for _, n := range nivells {
		levelsByID[n.ID] = n
		if n.ParentID.Valid {
			pid := int(n.ParentID.Int64)
			childrenByID[pid] = append(childrenByID[pid], n.ID)
		}
		if n.PaisISO2.Valid {
			levelISO[n.ID] = strings.ToUpper(n.PaisISO2.String)
		}
	}
	branchLevelIDs := map[int]struct{}{}
	allowedLevelIDs := map[int]struct{}{}
	if nivellID > 0 {
		if _, ok := levelsByID[nivellID]; !ok {
			http.NotFound(w, r)
			return
		}
		queue := []int{nivellID}
		for len(queue) > 0 {
			id := queue[0]
			queue = queue[1:]
			if _, ok := branchLevelIDs[id]; ok {
				continue
			}
			branchLevelIDs[id] = struct{}{}
			if children := childrenByID[id]; len(children) > 0 {
				queue = append(queue, children...)
			}
		}
		for id := range branchLevelIDs {
			allowedLevelIDs[id] = struct{}{}
			addNivellAncestors(id, levelsByID, allowedLevelIDs)
		}
	}
	municipiRows, err := a.DB.ListMunicipis(db.MunicipiFilter{})
	if err != nil {
		http.NotFound(w, r)
		return
	}
	payload := territoriExportPayload{
		Version:    1,
		ExportedAt: time.Now().Format(time.RFC3339),
	}
	for _, row := range municipiRows {
		m, err := a.DB.GetMunicipi(row.ID)
		if err != nil {
			continue
		}
		nivells := make([]int, 7)
		for i := 0; i < 7; i++ {
			if m.NivellAdministratiuID[i].Valid {
				nivells[i] = int(m.NivellAdministratiuID[i].Int64)
			}
		}
		if len(branchLevelIDs) > 0 && !municipiMatchesBranch(nivells, branchLevelIDs) {
			continue
		}
		if len(branchLevelIDs) > 0 {
			for _, id := range nivells {
				if id <= 0 {
					continue
				}
				allowedLevelIDs[id] = struct{}{}
				addNivellAncestors(id, levelsByID, allowedLevelIDs)
			}
		}
		var parent *int
		if m.MunicipiID.Valid {
			v := int(m.MunicipiID.Int64)
			parent = &v
		}
		var lat *float64
		if m.Latitud.Valid {
			v := m.Latitud.Float64
			lat = &v
		}
		var lon *float64
		if m.Longitud.Valid {
			v := m.Longitud.Float64
			lon = &v
		}
		paisISO2 := ""
		if nivells[0] > 0 {
			paisISO2 = levelISO[nivells[0]]
		}
		payload.Municipis = append(payload.Municipis, territoriExportMunicipi{
			ID:         m.ID,
			PaisISO2:   paisISO2,
			Nom:        m.Nom,
			Tipus:      m.Tipus,
			ParentID:   parent,
			Nivells:    nivells,
			CodiPostal: m.CodiPostal,
			Latitud:    lat,
			Longitud:   lon,
			What3Words: m.What3Words,
			Web:        m.Web,
			Wikipedia:  m.Wikipedia,
			Altres:     m.Altres,
			Estat:      m.Estat,
		})
	}
	allowedCountries := map[string]struct{}{}
	if len(branchLevelIDs) > 0 {
		for _, n := range nivells {
			if _, ok := allowedLevelIDs[n.ID]; !ok {
				continue
			}
			if n.PaisISO2.Valid {
				iso := strings.ToUpper(strings.TrimSpace(n.PaisISO2.String))
				if iso != "" {
					allowedCountries[iso] = struct{}{}
				}
			}
		}
	}
	for _, p := range paisos {
		iso := strings.ToUpper(strings.TrimSpace(p.CodiISO2))
		if len(branchLevelIDs) > 0 {
			if iso == "" {
				continue
			}
			if _, ok := allowedCountries[iso]; !ok {
				continue
			}
		}
		payload.Countries = append(payload.Countries, territoriExportCountry{
			ISO2: iso,
			ISO3: strings.ToUpper(strings.TrimSpace(p.CodiISO3)),
			Num:  strings.TrimSpace(p.CodiPaisNum),
		})
	}
	for _, n := range nivells {
		if len(branchLevelIDs) > 0 {
			if _, ok := allowedLevelIDs[n.ID]; !ok {
				continue
			}
		}
		var parent *int
		if n.ParentID.Valid {
			v := int(n.ParentID.Int64)
			parent = &v
		}
		var anyInici *int
		if n.AnyInici.Valid {
			v := int(n.AnyInici.Int64)
			anyInici = &v
		}
		var anyFi *int
		if n.AnyFi.Valid {
			v := int(n.AnyFi.Int64)
			anyFi = &v
		}
		iso2 := ""
		if n.PaisISO2.Valid {
			iso2 = strings.ToUpper(strings.TrimSpace(n.PaisISO2.String))
		}
		payload.Levels = append(payload.Levels, territoriExportLevel{
			ID:       n.ID,
			PaisISO2: iso2,
			Nivel:    n.Nivel,
			Nom:      n.NomNivell,
			Tipus:    n.TipusNivell,
			Codi:     n.CodiOficial,
			Altres:   n.Altres,
			ParentID: parent,
			AnyInici: anyInici,
			AnyFi:    anyFi,
			Estat:    n.Estat,
		})
	}
	sort.Slice(payload.Levels, func(i, j int) bool {
		if payload.Levels[i].Nivel == payload.Levels[j].Nivel {
			return payload.Levels[i].ID < payload.Levels[j].ID
		}
		return payload.Levels[i].Nivel < payload.Levels[j].Nivel
	})
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename=territori-export.json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(payload)
}

func (a *App) AdminTerritoriImportRun(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKey(w, r, permKeyAdminTerritoriImport, PermissionTarget{})
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	start := time.Now()
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		a.logAdminImportRun(r, "territori", adminImportStatusError, user.ID)
		http.Redirect(w, r, withQueryParams("/admin/territori/import", map[string]string{"err": "1"}), http.StatusSeeOther)
		return
	}
	returnTo := safeReturnTo(r.FormValue("return_to"), "/admin/territori/import")
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		a.logAdminImportRun(r, "territori", adminImportStatusError, user.ID)
		http.Redirect(w, r, withQueryParams(returnTo, map[string]string{"err": "1"}), http.StatusSeeOther)
		return
	}
	file, _, err := r.FormFile("import_file")
	if err != nil {
		a.logAdminImportRun(r, "territori", adminImportStatusError, user.ID)
		http.Redirect(w, r, withQueryParams(returnTo, map[string]string{"err": "1"}), http.StatusSeeOther)
		return
	}
	defer file.Close()
	var payload territoriExportPayload
	dec := json.NewDecoder(file)
	if err := dec.Decode(&payload); err != nil {
		a.logAdminImportRun(r, "territori", adminImportStatusError, user.ID)
		http.Redirect(w, r, withQueryParams(returnTo, map[string]string{"err": "1"}), http.StatusSeeOther)
		return
	}
	engine := territoriImportEngineName(a.DB)
	bulkInserter, hasBulkInserter := a.DB.(territoriBulkInserter)
	activityCount := 0
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
	activityRuleNivell := resolveActivityRule(ruleNivellCreate)
	activityRuleMunicipi := resolveActivityRule(ruleMunicipiCreate)
	pendingActivities := make([]db.UserActivity, 0, len(payload.Levels)+len(payload.Municipis))
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
	bulkModeLevels := "generic"
	bulkModeMunicipis := "generic"
	bulkModeParents := "generic"
	prepStart := time.Now()
	paisos, err := a.DB.ListPaisos()
	if err != nil {
		a.logAdminImportRun(r, "territori", adminImportStatusError, user.ID)
		http.Redirect(w, r, "/admin/territori/import?err=1", http.StatusSeeOther)
		return
	}
	paisByISO2 := map[string]db.Pais{}
	for _, p := range paisos {
		iso2 := strings.ToUpper(strings.TrimSpace(p.CodiISO2))
		if iso2 != "" {
			paisByISO2[iso2] = p
		}
	}
	existingLevels, _ := a.DB.ListNivells(db.NivellAdminFilter{})
	levelKeyMap := map[string]int{}
	for _, n := range existingLevels {
		key := nivellUniqueKey(n.PaisID, n.Nivel, n.ParentID, n.NomNivell)
		if key != "" {
			levelKeyMap[key] = n.ID
		}
	}
	prepDuration := time.Since(prepStart)
	countriesCreated := 0
	for _, c := range payload.Countries {
		iso2 := strings.ToUpper(strings.TrimSpace(c.ISO2))
		if iso2 == "" {
			continue
		}
		if _, ok := paisByISO2[iso2]; ok {
			continue
		}
		p := db.Pais{
			CodiISO2:    iso2,
			CodiISO3:    strings.ToUpper(strings.TrimSpace(c.ISO3)),
			CodiPaisNum: strings.TrimSpace(c.Num),
		}
		if _, err := a.DB.CreatePais(&p); err != nil {
			continue
		}
		paisByISO2[iso2] = p
		countriesCreated++
	}

	levelIDMap := map[int]int{}
	levelsTotal := len(payload.Levels)
	levelsCreated := 0
	levelsSkipped := 0
	levelsErrors := 0
	levelsStart := time.Now()
	pending := make([]territoriExportLevel, 0, len(payload.Levels))
	for _, l := range payload.Levels {
		pending = append(pending, l)
	}
	sort.Slice(pending, func(i, j int) bool {
		if pending[i].Nivel == pending[j].Nivel {
			return pending[i].ID < pending[j].ID
		}
		return pending[i].Nivel < pending[j].Nivel
	})
	for len(pending) > 0 {
		progressed := false
		next := make([]territoriExportLevel, 0, len(pending))
		type levelInsertMeta struct {
			exportID int
			key      string
		}
		toInsert := make([]db.NivellAdministratiu, 0, len(pending))
		insertMeta := make([]levelInsertMeta, 0, len(pending))
		for _, l := range pending {
			iso2 := strings.ToUpper(strings.TrimSpace(l.PaisISO2))
			pais, ok := paisByISO2[iso2]
			if !ok || iso2 == "" {
				levelsSkipped++
				continue
			}
			var parent sql.NullInt64
			if l.ParentID != nil && *l.ParentID > 0 {
				if pid, ok := levelIDMap[*l.ParentID]; ok {
					parent = sql.NullInt64{Int64: int64(pid), Valid: true}
				} else {
					next = append(next, l)
					continue
				}
			}
			n := db.NivellAdministratiu{
				PaisID:         pais.ID,
				Nivel:          l.Nivel,
				NomNivell:      strings.TrimSpace(l.Nom),
				TipusNivell:    strings.TrimSpace(l.Tipus),
				CodiOficial:    strings.TrimSpace(l.Codi),
				Altres:         l.Altres,
				ParentID:       parent,
				AnyInici:       intPtrToNull(l.AnyInici),
				AnyFi:          intPtrToNull(l.AnyFi),
				Estat:          strings.TrimSpace(l.Estat),
				CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
				ModeracioEstat: "pendent",
			}
			if n.Estat == "" {
				n.Estat = "actiu"
			}
			key := nivellUniqueKey(n.PaisID, n.Nivel, n.ParentID, n.NomNivell)
			if key != "" {
				if existingID, ok := levelKeyMap[key]; ok {
					levelIDMap[l.ID] = existingID
					levelsSkipped++
					progressed = true
					continue
				}
			}
			toInsert = append(toInsert, n)
			insertMeta = append(insertMeta, levelInsertMeta{exportID: l.ID, key: key})
		}
		if len(toInsert) > 0 {
			applyInserted := func(ids []int) {
				for i, id := range ids {
					levelIDMap[insertMeta[i].exportID] = id
					if insertMeta[i].key != "" {
						levelKeyMap[insertMeta[i].key] = id
					}
					levelsCreated++
					progressed = true
					activityCount++
					addActivity(activityRuleNivell, "nivell", id)
				}
			}
			var ids []int
			var mode string
			var err error
			bulkAttempted := false
			if hasBulkInserter {
				bulkAttempted = true
				ids, mode, err = bulkInserter.BulkInsertNivells(r.Context(), toInsert)
				if mode != "" {
					bulkModeLevels = mode
				}
			}
			if err == nil && len(ids) == len(toInsert) {
				applyInserted(ids)
			} else {
				if err != nil && bulkAttempted {
					Errorf("Territori import: bulk insert nivells fallit (%s): %v", bulkModeLevels, err)
				}
				bulkModeLevels = "generic"
				for i := range toInsert {
					n := toInsert[i]
					id, err := a.DB.CreateNivell(&n)
					if err != nil {
						levelsErrors++
						continue
					}
					levelIDMap[insertMeta[i].exportID] = id
					if insertMeta[i].key != "" {
						levelKeyMap[insertMeta[i].key] = id
					}
					levelsCreated++
					progressed = true
					activityCount++
					addActivity(activityRuleNivell, "nivell", id)
				}
			}
		}
		if !progressed {
			levelsSkipped += len(next)
			break
		}
		pending = next
	}
	levelsDuration := time.Since(levelsStart)

	existingMunicipiKey := map[string]int{}
	existingMunicipiParent := map[int]int{}
	duplicateMunicipiKeys := map[string]struct{}{}
	if len(payload.Municipis) > 0 {
		levelIDSet := map[int]struct{}{}
		for _, mu := range payload.Municipis {
			for _, lvl := range normalizeNivellSlice(mu.Nivells) {
				if lvl <= 0 {
					continue
				}
				if id, ok := levelIDMap[lvl]; ok && id > 0 {
					levelIDSet[id] = struct{}{}
				}
			}
		}
		allowedLevels := make([]int, 0, len(levelIDSet))
		for id := range levelIDSet {
			allowedLevels = append(allowedLevels, id)
		}
		existingRows, err := a.DB.ListMunicipis(db.MunicipiFilter{
			AllowedNivellIDs: allowedLevels,
		})
		if err == nil {
			for _, row := range existingRows {
				m, err := a.DB.GetMunicipi(row.ID)
				if err != nil || m == nil {
					continue
				}
				key := municipiUniqueKey(m)
				if key == "" {
					continue
				}
				if existingID, ok := existingMunicipiKey[key]; ok {
					if existingID != m.ID {
						existingMunicipiKey[key] = 0
						duplicateMunicipiKeys[key] = struct{}{}
					}
					continue
				}
				existingMunicipiKey[key] = m.ID
				if m.MunicipiID.Valid {
					existingMunicipiParent[m.ID] = int(m.MunicipiID.Int64)
				}
			}
		}
	}

	municipisTotal := len(payload.Municipis)
	municipisCreated := 0
	municipisSkipped := 0
	municipisErrors := 0
	munIDMap := map[int]int{}
	municipisStart := time.Now()
	type municipiInsertMeta struct {
		exportID  int
		oldParent int
	}
	type municipiParentCandidate struct {
		childID   int
		oldParent int
	}
	seenMunicipiKeys := map[string]struct{}{}
	toInsertMunicipis := make([]db.Municipi, 0, len(payload.Municipis))
	insertMeta := make([]municipiInsertMeta, 0, len(payload.Municipis))
	parentCandidates := make([]municipiParentCandidate, 0, len(payload.Municipis))
	for _, mu := range payload.Municipis {
		if strings.TrimSpace(mu.Nom) == "" {
			municipisSkipped++
			continue
		}
		nivells := normalizeNivellSlice(mu.Nivells)
		var m db.Municipi
		m.Nom = strings.TrimSpace(mu.Nom)
		m.Tipus = strings.TrimSpace(mu.Tipus)
		m.CodiPostal = strings.TrimSpace(mu.CodiPostal)
		m.Latitud = floatPtrToNull(mu.Latitud)
		m.Longitud = floatPtrToNull(mu.Longitud)
		m.What3Words = strings.TrimSpace(mu.What3Words)
		m.Web = strings.TrimSpace(mu.Web)
		m.Wikipedia = strings.TrimSpace(mu.Wikipedia)
		m.Altres = mu.Altres
		m.Estat = strings.TrimSpace(mu.Estat)
		if m.Estat == "" {
			m.Estat = "actiu"
		}
		m.CreatedBy = sql.NullInt64{Int64: int64(user.ID), Valid: true}
		m.ModeracioEstat = "pendent"
		for i := 0; i < 7; i++ {
			if nivells[i] > 0 {
				if id, ok := levelIDMap[nivells[i]]; ok {
					m.NivellAdministratiuID[i] = sql.NullInt64{Int64: int64(id), Valid: true}
				}
			}
		}
		oldParent := 0
		if mu.ParentID != nil && *mu.ParentID > 0 {
			oldParent = *mu.ParentID
		}
		key := municipiUniqueKey(&m)
		if key != "" {
			if existingID, ok := existingMunicipiKey[key]; ok && existingID > 0 {
				munIDMap[mu.ID] = existingID
				municipisSkipped++
				if oldParent > 0 {
					parentCandidates = append(parentCandidates, municipiParentCandidate{childID: existingID, oldParent: oldParent})
				}
				continue
			}
			if _, dup := duplicateMunicipiKeys[key]; dup {
				municipisSkipped++
				continue
			}
			if _, seen := seenMunicipiKeys[key]; seen {
				municipisSkipped++
				continue
			}
			seenMunicipiKeys[key] = struct{}{}
		}
		toInsertMunicipis = append(toInsertMunicipis, m)
		insertMeta = append(insertMeta, municipiInsertMeta{exportID: mu.ID, oldParent: oldParent})
	}
	if len(toInsertMunicipis) > 0 {
		applyInserted := func(ids []int) {
			for i, id := range ids {
				meta := insertMeta[i]
				munIDMap[meta.exportID] = id
				municipisCreated++
				activityCount++
				addActivity(activityRuleMunicipi, "municipi", id)
				if meta.oldParent > 0 {
					parentCandidates = append(parentCandidates, municipiParentCandidate{childID: id, oldParent: meta.oldParent})
				}
			}
		}
		var ids []int
		var mode string
		var err error
		bulkAttempted := false
		if hasBulkInserter {
			bulkAttempted = true
			ids, mode, err = bulkInserter.BulkInsertMunicipis(r.Context(), toInsertMunicipis)
			if mode != "" {
				bulkModeMunicipis = mode
			}
		}
		if err == nil && len(ids) == len(toInsertMunicipis) {
			applyInserted(ids)
		} else {
			if err != nil && bulkAttempted {
				Errorf("Territori import: bulk insert municipis fallit (%s): %v", bulkModeMunicipis, err)
			}
			bulkModeMunicipis = "generic"
			for i := range toInsertMunicipis {
				m := toInsertMunicipis[i]
				newID, err := a.DB.CreateMunicipi(&m)
				if err != nil {
					municipisErrors++
					continue
				}
				munIDMap[insertMeta[i].exportID] = newID
				municipisCreated++
				activityCount++
				addActivity(activityRuleMunicipi, "municipi", newID)
				if insertMeta[i].oldParent > 0 {
					parentCandidates = append(parentCandidates, municipiParentCandidate{childID: newID, oldParent: insertMeta[i].oldParent})
				}
			}
		}
	}
	parentStart := time.Now()
	parentErrors := 0
	parentUpdates := make([]db.MunicipiParentUpdate, 0, len(parentCandidates))
	for _, cand := range parentCandidates {
		if cand.oldParent <= 0 {
			continue
		}
		if newParent, ok := munIDMap[cand.oldParent]; ok {
			if currentParent, ok := existingMunicipiParent[cand.childID]; ok && currentParent == newParent {
				continue
			}
			parentUpdates = append(parentUpdates, db.MunicipiParentUpdate{ID: cand.childID, ParentID: newParent})
		}
	}
	if len(parentUpdates) > 0 {
		var mode string
		var err error
		bulkAttempted := false
		if hasBulkInserter {
			bulkAttempted = true
			mode, err = bulkInserter.BulkUpdateMunicipiParents(r.Context(), parentUpdates)
			if mode != "" {
				bulkModeParents = mode
			}
		}
		if !hasBulkInserter || err != nil {
			if err != nil && bulkAttempted {
				Errorf("Territori import: bulk update parents fallit (%s): %v", bulkModeParents, err)
			}
			bulkModeParents = "generic"
			for _, upd := range parentUpdates {
				mun, err := a.DB.GetMunicipi(upd.ID)
				if err != nil {
					parentErrors++
					continue
				}
				mun.MunicipiID = sql.NullInt64{Int64: int64(upd.ParentID), Valid: true}
				if err := a.DB.UpdateMunicipi(mun); err != nil {
					parentErrors++
				}
			}
		}
	}
	parentDuration := time.Since(parentStart)
	municipisDuration := time.Since(municipisStart)

	activityMode := "bulk"
	if len(pendingActivities) > 0 {
		mode, err := a.DB.BulkInsertUserActivities(r.Context(), pendingActivities)
		if err != nil {
			Errorf("Territori import: bulk insert activitats fallit (%s): %v", mode, err)
			activityMode = "generic"
			for i := range pendingActivities {
				act := pendingActivities[i]
				if _, err := a.DB.InsertUserActivity(&act); err != nil {
					Errorf("Territori import: insert activitat fallit: %v", err)
				}
			}
		} else if mode != "" {
			activityMode = mode
		}
	}

	if activityCount > 0 {
		now := time.Now()
		a.EvaluateAchievementsForUser(context.Background(), user.ID, AchievementTrigger{CreatedAt: now})
		a.logAntiAbuseSignals(user.ID, now)
	}
	totalDuration := time.Since(start)
	Infof("Territori import: engine=%s modes=%s/%s/%s activity=%s prep=%s levels=%s municipis=%s parents=%s totals=%d created=%d skipped=%d errors=%d parentErrors=%d duration=%s",
		engine,
		bulkModeLevels,
		bulkModeMunicipis,
		bulkModeParents,
		activityMode,
		prepDuration.String(),
		levelsDuration.String(),
		municipisDuration.String(),
		parentDuration.String(),
		levelsTotal+municipisTotal,
		levelsCreated+municipisCreated,
		levelsSkipped+municipisSkipped,
		levelsErrors+municipisErrors,
		parentErrors,
		totalDuration.String(),
	)

	redirect := withQueryParams(returnTo, map[string]string{
		"import":            "1",
		"countries_created": strconv.Itoa(countriesCreated),
		"levels_total":      strconv.Itoa(levelsTotal),
		"levels_created":    strconv.Itoa(levelsCreated),
		"levels_skipped":    strconv.Itoa(levelsSkipped),
		"levels_errors":     strconv.Itoa(levelsErrors),
		"municipis_total":   strconv.Itoa(municipisTotal),
		"municipis_created": strconv.Itoa(municipisCreated),
		"municipis_skipped": strconv.Itoa(municipisSkipped),
		"municipis_errors":  strconv.Itoa(municipisErrors),
	})
	status := adminImportStatusOK
	if levelsErrors > 0 || municipisErrors > 0 {
		status = adminImportStatusError
	}
	a.logAdminImportRun(r, "territori", status, user.ID)
	http.Redirect(w, r, redirect, http.StatusSeeOther)
}

func intPtrToNull(v *int) sql.NullInt64 {
	if v == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(*v), Valid: true}
}

func floatPtrToNull(v *float64) sql.NullFloat64 {
	if v == nil {
		return sql.NullFloat64{}
	}
	return sql.NullFloat64{Float64: *v, Valid: true}
}

func nivellUniqueKey(paisID, nivel int, parentID sql.NullInt64, name string) string {
	parentKey := "root"
	if parentID.Valid && parentID.Int64 > 0 {
		parentKey = "parent:" + strconv.FormatInt(parentID.Int64, 10)
	}
	return normalizeKey("pais:"+strconv.Itoa(paisID), "nivel:"+strconv.Itoa(nivel), parentKey, name)
}

func municipiUniqueKey(m *db.Municipi) string {
	if m == nil {
		return ""
	}
	parts := []string{
		"nom:" + strings.TrimSpace(m.Nom),
		"tipus:" + strings.TrimSpace(m.Tipus),
	}
	if cp := strings.TrimSpace(m.CodiPostal); cp != "" {
		parts = append(parts, "cp:"+cp)
	}
	for i := 0; i < 7; i++ {
		if m.NivellAdministratiuID[i].Valid {
			parts = append(parts, fmt.Sprintf("nivell%d:%d", i+1, m.NivellAdministratiuID[i].Int64))
		}
	}
	return normalizeKey(parts...)
}

func municipiMatchesBranch(nivells []int, branch map[int]struct{}) bool {
	if len(branch) == 0 {
		return true
	}
	for _, id := range nivells {
		if id <= 0 {
			continue
		}
		if _, ok := branch[id]; ok {
			return true
		}
	}
	return false
}

func addNivellAncestors(id int, levels map[int]db.NivellAdministratiu, dst map[int]struct{}) {
	cur, ok := levels[id]
	for ok && cur.ParentID.Valid && cur.ParentID.Int64 > 0 {
		pid := int(cur.ParentID.Int64)
		if _, exists := dst[pid]; exists {
			break
		}
		dst[pid] = struct{}{}
		cur, ok = levels[pid]
	}
}

func normalizeNivellSlice(v []int) []int {
	res := make([]int, 7)
	copy(res, v)
	return res
}

type territoriBulkInserter interface {
	BulkInsertNivells(ctx context.Context, rows []db.NivellAdministratiu) ([]int, string, error)
	BulkInsertMunicipis(ctx context.Context, rows []db.Municipi) ([]int, string, error)
	BulkUpdateMunicipiParents(ctx context.Context, updates []db.MunicipiParentUpdate) (string, error)
}

func territoriImportEngineName(database db.DB) string {
	switch database.(type) {
	case *db.PostgreSQL:
		return "postgres"
	case *db.MySQL:
		return "mysql"
	case *db.SQLite:
		return "sqlite"
	default:
		return "unknown"
	}
}
