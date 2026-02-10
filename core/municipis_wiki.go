package core

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type wikiHistoryItem struct {
	ID             int
	Seq            int
	ChangeType     string
	FieldKey       string
	OldValue       string
	NewValue       string
	ChangedAt      string
	ChangedBy      string
	ChangedByID    int
	ModeratedBy    string
	ModeratedAt    string
	ModeracioEstat string
	HasSnapshot    bool
	CanRevert      bool
}

type wikiStatsUser struct {
	ID     int
	Usuari string
}

type wikiStatsGroup struct {
	Key   string
	Label string
	Count int
	Users []wikiStatsUser
}

func (a *App) MunicipiWikiHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	if !isValidWikiObjectType("municipi") {
		http.NotFound(w, r)
		return
	}
	lang := ResolveLang(r)
	municipiID := extractID(r.URL.Path)
	if municipiID == 0 {
		http.NotFound(w, r)
		return
	}
	mun, err := a.DB.GetMunicipi(municipiID)
	if err != nil || mun == nil {
		http.NotFound(w, r)
		return
	}
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	*r = *a.withUser(r, user)
	perms := a.getPermissionsForUser(user.ID)
	*r = *a.withPermissions(r, perms)
	if !a.hasAnyPermissionKey(user.ID, permKeyWikiRevert) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	canManageTerritory := a.hasPerm(perms, permTerritory)
	canModerate := a.hasPerm(perms, permModerate)
	canRevertPerm := a.hasAnyPermissionKey(user.ID, permKeyWikiRevert)
	target := a.resolveMunicipiTarget(mun.ID)
	if !a.HasPermission(user.ID, permKeyTerritoriMunicipisView, target) && !(canManageTerritory || canModerate) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if mun.ModeracioEstat != "" && mun.ModeracioEstat != "publicat" && !(canManageTerritory || canModerate) {
		http.NotFound(w, r)
		return
	}

	changes, _ := a.DB.ListWikiChanges("municipi", municipiID)
	changes = filterVisibleWikiChanges(changes, user.ID, canModerate)
	totalChanges := len(changes)
	userCache := map[int]*db.User{}
	resolveUserName := func(userID int) string {
		if userID <= 0 {
			return "—"
		}
		if cached, ok := userCache[userID]; ok {
			username := strings.TrimSpace(cached.Usuari)
			if username == "" {
				full := strings.TrimSpace(strings.TrimSpace(cached.Name) + " " + strings.TrimSpace(cached.Surname))
				if full != "" {
					username = full
				}
			}
			if username == "" {
				username = "—"
			}
			return username
		}
		u, err := a.DB.GetUserByID(userID)
		if err != nil || u == nil {
			return "—"
		}
		userCache[userID] = u
		username := strings.TrimSpace(u.Usuari)
		if username == "" {
			full := strings.TrimSpace(strings.TrimSpace(u.Name) + " " + strings.TrimSpace(u.Surname))
			if full != "" {
				username = full
			}
		}
		if username == "" {
			username = "—"
		}
		return username
	}

	type wikiChangeSnapshots struct {
		Before json.RawMessage
		After  json.RawMessage
	}
	seqByID := map[int]int{}
	snapByID := map[int]wikiChangeSnapshots{}
	history := make([]wikiHistoryItem, 0, len(changes))
	for idx, ch := range changes {
		seq := totalChanges - idx
		seqByID[ch.ID] = seq
		before, after := parseWikiChangeMeta(ch.Metadata)
		if len(before) > 0 || len(after) > 0 {
			snapByID[ch.ID] = wikiChangeSnapshots{Before: before, After: after}
		}
		changedByID := 0
		if ch.ChangedBy.Valid {
			changedByID = int(ch.ChangedBy.Int64)
		}
		changedBy := resolveUserName(changedByID)
		changedAt := ""
		if !ch.ChangedAt.IsZero() {
			changedAt = ch.ChangedAt.Format("02/01/2006 15:04")
		}
		moderatedBy := ""
		if ch.ModeratedBy.Valid {
			moderatedBy = resolveUserName(int(ch.ModeratedBy.Int64))
		}
		moderatedAt := ""
		if ch.ModeratedAt.Valid {
			moderatedAt = ch.ModeratedAt.Time.Format("02/01/2006 15:04")
		}
		hasSnapshot := len(before) > 0 || len(after) > 0
		canRevert := false
		if hasSnapshot && canRevertPerm {
			if canModerate {
				canRevert = true
			} else if changedByID == user.ID {
				canRevert = true
			}
		}
		history = append(history, wikiHistoryItem{
			ID:             ch.ID,
			Seq:            seq,
			ChangeType:     ch.ChangeType,
			FieldKey:       ch.FieldKey,
			OldValue:       ch.OldValue,
			NewValue:       ch.NewValue,
			ChangedAt:      changedAt,
			ChangedBy:      changedBy,
			ChangedByID:    changedByID,
			ModeratedBy:    moderatedBy,
			ModeratedAt:    moderatedAt,
			ModeracioEstat: ch.ModeracioEstat,
			HasSnapshot:    hasSnapshot,
			CanRevert:      canRevert,
		})
	}

	var viewFields []wikiHistoryFieldView
	viewLabel := ""
	viewToken := strings.TrimSpace(r.URL.Query().Get("view"))
	if viewToken != "" {
		switch viewToken {
		case "current", "published":
			if payload, err := json.Marshal(mun); err == nil {
				viewFields = buildWikiViewFields(payload)
				viewLabel = T(lang, "wiki.history.current")
			}
		default:
			if viewID, _ := strconv.Atoi(viewToken); viewID > 0 {
				if snap, ok := snapByID[viewID]; ok {
					payload := snap.After
					if len(payload) == 0 {
						payload = snap.Before
					}
					if len(payload) > 0 {
						viewFields = buildWikiViewFields(payload)
						seqLabel := seqByID[viewID]
						if seqLabel == 0 {
							seqLabel = viewID
						}
						viewLabel = fmt.Sprintf("%s #%d", T(lang, "wiki.history.version"), seqLabel)
					}
				}
			}
		}
	}

	var compareFields []WikiDiffField
	compareLeftLabel := ""
	compareRightLabel := ""
	compareParam := strings.TrimSpace(r.URL.Query().Get("compare"))
	if compareParam != "" {
		parts := strings.Split(compareParam, ",")
		if len(parts) == 2 {
			resolveSnapshot := func(token string) (json.RawMessage, string) {
				token = strings.TrimSpace(token)
				if token == "" || token == "current" || token == "published" {
					if payload, err := json.Marshal(mun); err == nil {
						return payload, T(lang, "wiki.history.current")
					}
					return nil, ""
				}
				if id, _ := strconv.Atoi(token); id > 0 {
					if snap, ok := snapByID[id]; ok {
						payload := snap.After
						if len(payload) == 0 {
							payload = snap.Before
						}
						if len(payload) == 0 {
							return nil, ""
						}
						seqLabel := seqByID[id]
						if seqLabel == 0 {
							seqLabel = id
						}
						return payload, fmt.Sprintf("%s #%d", T(lang, "wiki.history.version"), seqLabel)
					}
				}
				return nil, ""
			}
			leftSnap, leftLabel := resolveSnapshot(parts[0])
			rightSnap, rightLabel := resolveSnapshot(parts[1])
			if len(leftSnap) > 0 && len(rightSnap) > 0 {
				compareFields = buildWikiDiff(leftSnap, rightSnap)
				compareLeftLabel = leftLabel
				compareRightLabel = rightLabel
			}
		}
	}

	title := fmt.Sprintf("%s: %s", T(lang, "wiki.entity.municipi"), mun.Nom)
	historyURL := fmt.Sprintf("/territori/municipis/%d/historial", mun.ID)
	RenderPrivateTemplate(w, r, "wiki-history.html", map[string]interface{}{
		"Title":             title,
		"BackURL":           fmt.Sprintf("/territori/municipis/%d", mun.ID),
		"HistoryURL":        historyURL,
		"RevertURL":         fmt.Sprintf("/territori/municipis/%d/historial/revert", mun.ID),
		"History":           history,
		"ViewFields":        viewFields,
		"ViewLabel":         viewLabel,
		"CompareFields":     compareFields,
		"CompareLeftLabel":  compareLeftLabel,
		"CompareRightLabel": compareRightLabel,
		"CanModerate":       canModerate,
	})
}

func (a *App) MunicipiWikiStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	if !isValidWikiObjectType("municipi") {
		http.NotFound(w, r)
		return
	}
	lang := ResolveLang(r)
	municipiID := extractID(r.URL.Path)
	if municipiID == 0 {
		http.NotFound(w, r)
		return
	}
	mun, err := a.DB.GetMunicipi(municipiID)
	if err != nil || mun == nil {
		http.NotFound(w, r)
		return
	}
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	*r = *a.withUser(r, user)
	perms := a.getPermissionsForUser(user.ID)
	*r = *a.withPermissions(r, perms)
	canManageTerritory := a.hasPerm(perms, permTerritory)
	canModerate := a.hasPerm(perms, permModerate)
	target := a.resolveMunicipiTarget(mun.ID)
	if !a.HasPermission(user.ID, permKeyTerritoriMunicipisView, target) && !(canManageTerritory || canModerate) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if mun.ModeracioEstat != "" && mun.ModeracioEstat != "publicat" && !(canManageTerritory || canModerate) {
		http.NotFound(w, r)
		return
	}

	counts, _ := a.DB.GetWikiPublicCounts("municipi", municipiID)
	groupOrder := []string{"consanguini", "politic", "interes"}
	var groups []wikiStatsGroup
	for _, key := range groupOrder {
		label := T(lang, "wiki.stats.group."+key)
		groups = append(groups, wikiStatsGroup{
			Key:   key,
			Label: label,
			Count: counts[key],
		})
	}

	title := fmt.Sprintf("%s: %s", T(lang, "wiki.entity.municipi"), mun.Nom)
	RenderPrivateTemplate(w, r, "wiki-stats.html", map[string]interface{}{
		"Title":   title,
		"BackURL": fmt.Sprintf("/territori/municipis/%d", mun.ID),
		"Groups":  groups,
	})
}

func (a *App) MunicipiWikiRevert(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !isValidWikiObjectType("municipi") {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	municipiID := extractID(r.URL.Path)
	if municipiID == 0 {
		http.NotFound(w, r)
		return
	}
	mun, err := a.DB.GetMunicipi(municipiID)
	if err != nil || mun == nil {
		http.NotFound(w, r)
		return
	}
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	*r = *a.withUser(r, user)
	perms := a.getPermissionsForUser(user.ID)
	*r = *a.withPermissions(r, perms)
	canModerate := a.hasPerm(perms, permModerate)
	target := a.resolveMunicipiTarget(mun.ID)
	if !a.HasPermission(user.ID, permKeyTerritoriMunicipisView, target) && !canModerate {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	changeID, _ := strconv.Atoi(strings.TrimSpace(r.FormValue("change_id")))
	if changeID == 0 {
		http.Error(w, "Canvi invàlid", http.StatusBadRequest)
		return
	}
	reason := strings.TrimSpace(r.FormValue("reason"))
	change, err := a.DB.GetWikiChange(changeID)
	if err != nil || change == nil {
		http.NotFound(w, r)
		return
	}
	if change.ObjectType != "municipi" || change.ObjectID != municipiID {
		http.Error(w, "Canvi invàlid", http.StatusBadRequest)
		return
	}
	isAuthor := change.ChangedBy.Valid && int(change.ChangedBy.Int64) == user.ID
	if !canModerate && !isAuthor {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	lang := resolveUserLang(r, user)
	if !a.ensureWikiChangeAllowed(w, r, lang) {
		return
	}
	beforeJSON, _ := json.Marshal(mun)
	_, after := parseWikiChangeMeta(change.Metadata)
	if len(after) == 0 {
		http.Error(w, "No es pot revertir aquesta versió", http.StatusBadRequest)
		return
	}
	meta := map[string]interface{}{
		"before":           json.RawMessage(beforeJSON),
		"after":            after,
		"source_change_id": changeID,
	}
	if reason != "" {
		meta["reason"] = reason
	}
	metaJSON, _ := json.Marshal(meta)
	newID, err := a.createWikiChange(&db.WikiChange{
		ObjectType:     "municipi",
		ObjectID:       municipiID,
		ChangeType:     "revert",
		FieldKey:       "revert",
		Metadata:       string(metaJSON),
		ModeracioEstat: "pendent",
		ChangedBy:      sqlNullIntFromInt(user.ID),
	})
	if err != nil {
		if status, msg, ok := a.wikiGuardrailInfo(lang, err); ok {
			http.Error(w, msg, status)
			return
		}
		http.Error(w, "No s'ha pogut crear la proposta", http.StatusInternalServerError)
		return
	}
	if newID > 0 {
		detail := fmt.Sprintf("municipi:%d", municipiID)
		_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleMunicipiUpdate, "editar", "municipi_canvi", &newID, "pendent", nil, detail)
	}
	returnURL := strings.TrimSpace(r.FormValue("return_to"))
	if returnURL == "" {
		returnURL = fmt.Sprintf("/territori/municipis/%d/historial", municipiID)
	}
	http.Redirect(w, r, returnURL, http.StatusSeeOther)
}

func (a *App) MunicipiWikiMark(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !isValidWikiObjectType("municipi") {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.Header.Get("X-CSRF-Token")) && !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	municipiID := extractID(r.URL.Path)
	if municipiID == 0 {
		http.NotFound(w, r)
		return
	}
	mun, err := a.DB.GetMunicipi(municipiID)
	if err != nil || mun == nil {
		http.NotFound(w, r)
		return
	}
	target := a.resolveMunicipiTarget(mun.ID)
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisView, target)
	if !ok {
		return
	}
	lang := resolveUserLang(r, user)
	if !a.ensureWikiMarkAllowed(w, r, lang) {
		return
	}
	if mun.ModeracioEstat != "" && mun.ModeracioEstat != "publicat" && !a.hasPerm(a.getPermissionsForUser(user.ID), permModerate) {
		http.NotFound(w, r)
		return
	}
	tipus := strings.TrimSpace(r.FormValue("type"))
	if !isValidWikiMarkType(tipus) {
		http.Error(w, "Tipus invàlid", http.StatusBadRequest)
		return
	}
	publicVal := strings.TrimSpace(r.FormValue("public"))
	isPublic := true
	if publicVal != "" {
		lower := strings.ToLower(publicVal)
		isPublic = lower == "1" || lower == "true" || lower == "yes" || lower == "si" || lower == "on"
	}
	mark := db.WikiMark{
		ObjectType: "municipi",
		ObjectID:   municipiID,
		UserID:     user.ID,
		Tipus:      tipus,
		IsPublic:   isPublic,
	}
	prevMark, _ := a.DB.GetWikiMark("municipi", municipiID, user.ID)
	if err := a.DB.UpsertWikiMark(&mark); err != nil {
		Errorf("Error desant marca wiki: %v", err)
		http.Error(w, "No s'ha pogut desar la marca", http.StatusInternalServerError)
		return
	}
	if err := a.updateWikiPublicCounts("municipi", municipiID, prevMark, tipus, isPublic); err != nil {
		Errorf("Error actualitzant stats wiki: %v", err)
		http.Error(w, "No s'ha pogut desar la marca", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{
		"ok":        true,
		"type":      tipus,
		"is_public": isPublic,
		"own":       true,
	})
}

func (a *App) MunicipiWikiUnmark(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !isValidWikiObjectType("municipi") {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.Header.Get("X-CSRF-Token")) && !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	municipiID := extractID(r.URL.Path)
	if municipiID == 0 {
		http.NotFound(w, r)
		return
	}
	mun, err := a.DB.GetMunicipi(municipiID)
	if err != nil || mun == nil {
		http.NotFound(w, r)
		return
	}
	target := a.resolveMunicipiTarget(mun.ID)
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisView, target)
	if !ok {
		return
	}
	lang := resolveUserLang(r, user)
	if !a.ensureWikiMarkAllowed(w, r, lang) {
		return
	}
	if mun.ModeracioEstat != "" && mun.ModeracioEstat != "publicat" && !a.hasPerm(a.getPermissionsForUser(user.ID), permModerate) {
		http.NotFound(w, r)
		return
	}
	prevMark, _ := a.DB.GetWikiMark("municipi", municipiID, user.ID)
	if err := a.DB.DeleteWikiMark("municipi", municipiID, user.ID); err != nil {
		Errorf("Error eliminant marca wiki: %v", err)
		http.Error(w, "No s'ha pogut eliminar la marca", http.StatusInternalServerError)
		return
	}
	if prevMark != nil {
		if err := a.updateWikiPublicCounts("municipi", municipiID, prevMark, prevMark.Tipus, false); err != nil {
			Errorf("Error actualitzant stats wiki: %v", err)
			http.Error(w, "No s'ha pogut eliminar la marca", http.StatusInternalServerError)
			return
		}
	}
	writeJSON(w, map[string]interface{}{
		"ok":        true,
		"type":      "",
		"is_public": false,
		"own":       false,
	})
}

func isValidWikiMarkType(t string) bool {
	switch t {
	case "consanguini", "politic", "interes":
		return true
	default:
		return false
	}
}
