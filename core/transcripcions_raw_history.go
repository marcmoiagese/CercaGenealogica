package core

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type registreHistoryItem struct {
	ID             int
	Key            string
	Seq            int
	ChangeType     string
	FieldKey       string
	OldValue       string
	NewValue       string
	ChangedAt      string
	ChangedBy      string
	ChangedByID    int
	ModeratedBy    string
	ModeratedByID  int
	ModeratedAt    string
	ModeracioEstat string
	HasSnapshot    bool
	HasComparator  bool
	IsPublished    bool
}

type registreHistoryFieldView struct {
	Label string
	Value string
}

type registreHistoryFieldDiff struct {
	Label   string
	Before  string
	After   string
	Changed bool
}

type transcripcioSnapshot struct {
	Raw      db.TranscripcioRaw          `json:"raw"`
	Persones []db.TranscripcioPersonaRaw `json:"persones"`
	Atributs []db.TranscripcioAtributRaw `json:"atributs"`
}

type transcripcioChangeMeta struct {
	Before *transcripcioSnapshot `json:"before"`
	After  *transcripcioSnapshot `json:"after"`
}

type transcripcioChangeMetaInfo struct {
	Target      string `json:"target"`
	RawField    string `json:"raw_field"`
	AttrKey     string `json:"attr_key"`
	AttrType    string `json:"attr_type"`
	Role        string `json:"role"`
	PersonField string `json:"person_field"`
	PersonKey   string `json:"person_key"`
}

type registreStatsUser struct {
	ID     int
	Usuari string
}

type registreStatsGroup struct {
	Key   string
	Label string
	Users []registreStatsUser
}

func parseTranscripcioChangeMeta(change db.TranscripcioRawChange) (*transcripcioSnapshot, *transcripcioSnapshot) {
	payload := strings.TrimSpace(change.Metadata)
	if payload == "" {
		return nil, nil
	}
	var meta transcripcioChangeMeta
	if err := json.Unmarshal([]byte(payload), &meta); err == nil {
		if meta.Before != nil || meta.After != nil {
			return meta.Before, meta.After
		}
	}
	raw := map[string]json.RawMessage{}
	if err := json.Unmarshal([]byte(payload), &raw); err != nil {
		raw = nil
	}
	var beforeSnap *transcripcioSnapshot
	var afterSnap *transcripcioSnapshot
	parseSnapshot := func(data json.RawMessage) *transcripcioSnapshot {
		if len(data) == 0 {
			return nil
		}
		var snap transcripcioSnapshot
		if err := json.Unmarshal(data, &snap); err == nil {
			return &snap
		}
		var nested string
		if err := json.Unmarshal(data, &nested); err == nil && strings.TrimSpace(nested) != "" {
			if err := json.Unmarshal([]byte(nested), &snap); err == nil {
				return &snap
			}
		}
		return nil
	}
	if raw != nil {
		if rawPayload, ok := raw["before"]; ok {
			beforeSnap = parseSnapshot(rawPayload)
		}
		if rawPayload, ok := raw["after"]; ok {
			afterSnap = parseSnapshot(rawPayload)
		}
	}
	if beforeSnap != nil || afterSnap != nil {
		return beforeSnap, afterSnap
	}
	var snap transcripcioSnapshot
	if err := json.Unmarshal([]byte(payload), &snap); err == nil {
		if snap.Raw.ID != 0 || snap.Raw.LlibreID != 0 || len(snap.Persones) > 0 || len(snap.Atributs) > 0 {
			return nil, &snap
		}
	}
	return beforeSnap, afterSnap
}

func parseChangeMetaInfo(payload string) *transcripcioChangeMetaInfo {
	text := strings.TrimSpace(payload)
	if text == "" {
		return nil
	}
	var info transcripcioChangeMetaInfo
	if err := json.Unmarshal([]byte(text), &info); err == nil {
		if info.Target != "" || info.RawField != "" || info.AttrKey != "" || info.Role != "" || info.PersonField != "" {
			return &info
		}
	}
	raw := map[string]json.RawMessage{}
	if err := json.Unmarshal([]byte(text), &raw); err != nil {
		return nil
	}
	parseInfo := func(data json.RawMessage) *transcripcioChangeMetaInfo {
		if len(data) == 0 {
			return nil
		}
		var out transcripcioChangeMetaInfo
		if err := json.Unmarshal(data, &out); err == nil {
			if out.Target != "" || out.RawField != "" || out.AttrKey != "" || out.Role != "" || out.PersonField != "" {
				return &out
			}
		}
		var nested string
		if err := json.Unmarshal(data, &nested); err == nil && strings.TrimSpace(nested) != "" {
			if err := json.Unmarshal([]byte(nested), &out); err == nil {
				if out.Target != "" || out.RawField != "" || out.AttrKey != "" || out.Role != "" || out.PersonField != "" {
					return &out
				}
			}
		}
		return nil
	}
	if changeRaw, ok := raw["change"]; ok {
		if parsed := parseInfo(changeRaw); parsed != nil {
			return parsed
		}
	}
	if parsed := parseInfo(json.RawMessage(text)); parsed != nil {
		return parsed
	}
	return nil
}

type changeSnapshots struct {
	Before *transcripcioSnapshot
	After  *transcripcioSnapshot
}

func collectChangeSnapshotsWithSource(changes []db.TranscripcioRawChange) (map[int]changeSnapshots, map[int]bool) {
	snaps := make(map[int]changeSnapshots, len(changes))
	explicit := make(map[int]bool, len(changes))
	for _, ch := range changes {
		before, after := parseTranscripcioChangeMeta(ch)
		if before == nil && after == nil {
			continue
		}
		snaps[ch.ID] = changeSnapshots{
			Before: before,
			After:  after,
		}
		explicit[ch.ID] = true
	}
	return snaps, explicit
}

func collectChangeSnapshots(changes []db.TranscripcioRawChange) map[int]changeSnapshots {
	snaps, _ := collectChangeSnapshotsWithSource(changes)
	return snaps
}

func cloneSnapshot(src *transcripcioSnapshot) *transcripcioSnapshot {
	if src == nil {
		return nil
	}
	out := &transcripcioSnapshot{
		Raw:      src.Raw,
		Persones: append([]db.TranscripcioPersonaRaw(nil), src.Persones...),
		Atributs: append([]db.TranscripcioAtributRaw(nil), src.Atributs...),
	}
	return out
}

func applyChangeValue(snap *transcripcioSnapshot, meta *transcripcioChangeMetaInfo, val string) {
	if snap == nil || meta == nil {
		return
	}
	switch meta.Target {
	case "raw":
		if val == "" {
			clearRawField(&snap.Raw, meta.RawField)
			return
		}
		applyRawField(&snap.Raw, meta.RawField, val)
	case "attr":
		var target *db.TranscripcioAtributRaw
		for i := range snap.Atributs {
			if snap.Atributs[i].Clau == meta.AttrKey {
				target = &snap.Atributs[i]
				break
			}
		}
		if target == nil {
			if val == "" {
				return
			}
			snap.Atributs = append(snap.Atributs, db.TranscripcioAtributRaw{
				Clau:       meta.AttrKey,
				TipusValor: meta.AttrType,
			})
			target = &snap.Atributs[len(snap.Atributs)-1]
		}
		if val == "" {
			target.ValorText = ""
			target.ValorInt = sql.NullInt64{}
			target.ValorDate = sql.NullString{}
			target.ValorBool = sql.NullBool{}
			return
		}
		if target.TipusValor == "" {
			target.TipusValor = meta.AttrType
		}
		applyAttrValue(target, val)
	case "person":
		person := findPersonForFieldMutable(snap.Persones, meta.Role, meta.PersonKey)
		if person == nil {
			if meta.Role == "" {
				return
			}
			snap.Persones = append(snap.Persones, db.TranscripcioPersonaRaw{Rol: meta.Role})
			person = &snap.Persones[len(snap.Persones)-1]
		}
		applyPersonField(person, meta.PersonField, val)
	}
}

func fillMissingSnapshots(changes []db.TranscripcioRawChange, snaps map[int]changeSnapshots, current *transcripcioSnapshot) map[int]changeSnapshots {
	if current == nil {
		return snaps
	}
	state := cloneSnapshot(current)
	for _, ch := range changes {
		snap := snaps[ch.ID]
		if snap.Before == nil && snap.After == nil {
			meta := parseChangeMetaInfo(ch.Metadata)
			if meta == nil {
				continue
			}
			after := cloneSnapshot(state)
			before := cloneSnapshot(state)
			applyChangeValue(after, meta, ch.NewValue)
			applyChangeValue(before, meta, ch.OldValue)
			snaps[ch.ID] = changeSnapshots{Before: before, After: after}
			state = before
			continue
		}
		if snap.Before != nil {
			state = cloneSnapshot(snap.Before)
			continue
		}
		if snap.After != nil {
			meta := parseChangeMetaInfo(ch.Metadata)
			if meta != nil {
				before := cloneSnapshot(snap.After)
				applyChangeValue(before, meta, ch.OldValue)
				snaps[ch.ID] = changeSnapshots{Before: before, After: snap.After}
				state = before
			} else {
				state = cloneSnapshot(snap.After)
			}
		}
	}
	return snaps
}

func pickSnapshotForView(snap changeSnapshots) *transcripcioSnapshot {
	if snap.After != nil {
		return snap.After
	}
	return snap.Before
}

func normalizeModeracioEstat(value string) string {
	return strings.TrimSpace(strings.ToLower(value))
}

func latestPublishedChangeID(changes []db.TranscripcioRawChange) int {
	for _, ch := range changes {
		if normalizeModeracioEstat(ch.ModeracioEstat) == "publicat" {
			return ch.ID
		}
	}
	return 0
}

func versionOffset(totalChanges int) int {
	if totalChanges > 0 {
		return 1
	}
	return 0
}

func versionNumberForChangeID(changeID int, seqByID map[int]int, offset int) int {
	if seq, ok := seqByID[changeID]; ok {
		return seq + offset
	}
	return 0
}

func publishedVersionNumber(publishedKey string, publishedChangeID int, seqByID map[int]int, offset int) int {
	if publishedKey == "published" {
		return 1
	}
	if publishedChangeID > 0 {
		return versionNumberForChangeID(publishedChangeID, seqByID, offset)
	}
	return 0
}

func resolvePublishedSnapshot(registre *db.TranscripcioRaw, current *transcripcioSnapshot, changes []db.TranscripcioRawChange, snaps map[int]changeSnapshots) (*transcripcioSnapshot, string, int, string) {
	var published *transcripcioSnapshot
	publishedKey := ""
	publishedID := 0
	publishedSource := ""
	for _, ch := range changes {
		snap := snaps[ch.ID]
		if normalizeModeracioEstat(ch.ModeracioEstat) != "publicat" || ch.ID <= publishedID {
			continue
		}
		if snap.After != nil {
			published = snap.After
			publishedSource = "after"
		} else if snap.Before != nil {
			published = snap.Before
			publishedSource = "before"
		}
		if published != nil {
			publishedID = ch.ID
			publishedKey = strconv.Itoa(ch.ID)
		}
	}
	if published == nil && registre != nil && normalizeModeracioEstat(registre.ModeracioEstat) == "publicat" && current != nil {
		return current, "published", 0, "current"
	}
	return published, publishedKey, publishedID, publishedSource
}

func snapshotFieldValue(col registreTableColumn, snap *transcripcioSnapshot) string {
	if snap == nil {
		return ""
	}
	attrs := map[string]db.TranscripcioAtributRaw{}
	for _, attr := range snap.Atributs {
		if _, ok := attrs[attr.Clau]; ok {
			continue
		}
		attrs[attr.Clau] = attr
	}
	cache := map[string]*db.TranscripcioPersonaRaw{}
	val := registreCellValue(col.Field, snap.Raw, snap.Persones, attrs, cache)
	return strings.TrimSpace(val)
}

func buildSnapshotView(lang string, llibre *db.Llibre, snap *transcripcioSnapshot) []registreHistoryFieldView {
	if snap == nil {
		return nil
	}
	cfg := buildIndexerConfig(lang, llibre)
	cols := buildRegistreTableColumns(lang, cfg)
	fields := make([]registreHistoryFieldView, 0, len(cols))
	for _, col := range cols {
		if col.IsStatus || col.IsActions {
			continue
		}
		val := snapshotFieldValue(col, snap)
		if val == "" {
			continue
		}
		fields = append(fields, registreHistoryFieldView{
			Label: col.Label,
			Value: val,
		})
	}
	return fields
}

func buildSnapshotDiff(lang string, llibre *db.Llibre, before, after *transcripcioSnapshot) []registreHistoryFieldDiff {
	cfg := buildIndexerConfig(lang, llibre)
	cols := buildRegistreTableColumns(lang, cfg)
	fields := make([]registreHistoryFieldDiff, 0, len(cols))
	for _, col := range cols {
		if col.IsStatus || col.IsActions {
			continue
		}
		beforeVal := snapshotFieldValue(col, before)
		afterVal := snapshotFieldValue(col, after)
		if beforeVal == "" && afterVal == "" {
			continue
		}
		fields = append(fields, registreHistoryFieldDiff{
			Label:   col.Label,
			Before:  beforeVal,
			After:   afterVal,
			Changed: beforeVal != afterVal,
		})
	}
	return fields
}

func (a *App) AdminRegistreHistory(w http.ResponseWriter, r *http.Request) {
	lang := ResolveLang(r)
	registreID := extractID(r.URL.Path)
	if registreID == 0 {
		http.NotFound(w, r)
		return
	}
	registre, err := a.DB.GetTranscripcioRaw(registreID)
	if err != nil || registre == nil {
		http.NotFound(w, r)
		return
	}
	target := a.resolveLlibreTarget(registre.LlibreID)
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsRegistresEdit, target)
	if !ok {
		return
	}
	perms := a.getPermissionsForUser(user.ID)
	canManageArxius := a.hasPerm(perms, permArxius)
	canManagePolicies := perms.CanManagePolicies || perms.Admin
	canModerate := perms.CanModerate || perms.Admin
	canRevert := a.hasAnyPermissionKey(user.ID, permKeyWikiRevert)
	llibre, _ := a.DB.GetLlibre(registre.LlibreID)
	if llibre == nil {
		llibre = &db.Llibre{TipusLlibre: registre.TipusActe}
	}
	changes, _ := a.DB.ListTranscripcioRawChanges(registreID)
	userCache := map[int]*db.User{}
	latestChangeID := 0
	for _, ch := range changes {
		if ch.ID > latestChangeID {
			latestChangeID = ch.ID
		}
	}
	totalChanges := len(changes)
	offset := versionOffset(totalChanges)
	seqByID := map[int]int{}
	for i, ch := range changes {
		seqByID[ch.ID] = totalChanges - i
	}

	persones, _ := a.DB.ListTranscripcioPersones(registreID)
	atributs, _ := a.DB.ListTranscripcioAtributs(registreID)
	currentSnapshot := &transcripcioSnapshot{
		Raw:      *registre,
		Persones: persones,
		Atributs: atributs,
	}

	snaps, explicitSnapshots := collectChangeSnapshotsWithSource(changes)
	snaps = fillMissingSnapshots(changes, snaps, currentSnapshot)
	publishedSnap, publishedKey, publishedID, publishedSource := resolvePublishedSnapshot(registre, currentSnapshot, changes, snaps)
	publishedFromCurrent := publishedKey == "published"
	publishedChangeID := 0
	if !publishedFromCurrent && publishedID > 0 {
		publishedChangeID = publishedID
	}
	publishedVersion := publishedVersionNumber(publishedKey, publishedChangeID, seqByID, offset)

	resolveUserName := func(userID int) string {
		if userID == 0 {
			return "-"
		}
		if cached, ok := userCache[userID]; ok {
			if cached != nil && cached.Usuari != "" {
				return cached.Usuari
			}
			return "-"
		}
		if u, err := a.DB.GetUserByID(userID); err == nil {
			userCache[userID] = u
			if u != nil && u.Usuari != "" {
				return u.Usuari
			}
		}
		return "-"
	}

	history := make([]registreHistoryItem, 0, len(changes)+1)
	for _, ch := range changes {
		key := strconv.Itoa(ch.ID)
		changedByID := 0
		if ch.ChangedBy.Valid {
			changedByID = int(ch.ChangedBy.Int64)
		}
		changedBy := resolveUserName(changedByID)
		moderatedByID := 0
		if ch.ModeratedBy.Valid {
			moderatedByID = int(ch.ModeratedBy.Int64)
		}
		moderatedBy := ""
		if moderatedByID > 0 {
			moderatedBy = resolveUserName(moderatedByID)
		}
		moderatedAt := ""
		if ch.ModeratedAt.Valid {
			moderatedAt = ch.ModeratedAt.Time.Format("02/01/2006 15:04")
		}
		snap := snaps[ch.ID]
		hasSnapshot := snap.Before != nil || snap.After != nil
		estado := normalizeModeracioEstat(ch.ModeracioEstat)
		isPublished := publishedChangeID > 0 && ch.ID == publishedChangeID
		if explicitSnapshots[ch.ID] {
			if estado == "" {
				if snap.After != nil && snap.After.Raw.ModeracioEstat != "" {
					estado = normalizeModeracioEstat(snap.After.Raw.ModeracioEstat)
				} else if snap.Before != nil && snap.Before.Raw.ModeracioEstat != "" {
					estado = normalizeModeracioEstat(snap.Before.Raw.ModeracioEstat)
				}
			}
		}
		if estado == "" {
			recordStatus := normalizeModeracioEstat(registre.ModeracioEstat)
			if ch.ID == latestChangeID && recordStatus != "" && recordStatus != "publicat" {
				estado = recordStatus
			}
		}
		if estado == "" && isPublished {
			estado = "publicat"
		}
		if estado == "publicat" && !isPublished {
			estado = "publicat_anterior"
		}
		history = append(history, registreHistoryItem{
			ID:             ch.ID,
			Key:            key,
			Seq:            versionNumberForChangeID(ch.ID, seqByID, offset),
			ChangeType:     ch.ChangeType,
			FieldKey:       ch.FieldKey,
			OldValue:       ch.OldValue,
			NewValue:       ch.NewValue,
			ChangedAt:      ch.ChangedAt.Format("02/01/2006 15:04"),
			ChangedBy:      changedBy,
			ChangedByID:    changedByID,
			ModeratedBy:    moderatedBy,
			ModeratedByID:  moderatedByID,
			ModeratedAt:    moderatedAt,
			ModeracioEstat: estado,
			HasSnapshot:    hasSnapshot,
			HasComparator:  hasSnapshot,
			IsPublished:    isPublished,
		})
	}

	baseSnap := (*transcripcioSnapshot)(nil)
	if totalChanges > 0 {
		for i := len(changes) - 1; i >= 0; i-- {
			snap := snaps[changes[i].ID]
			if snap.Before != nil {
				baseSnap = snap.Before
				break
			}
			if baseSnap == nil && snap.After != nil {
				baseSnap = snap.After
			}
		}
		if baseSnap == nil {
			baseSnap = currentSnapshot
		}
		baseChangedByID := 0
		if baseSnap != nil && baseSnap.Raw.CreatedBy.Valid {
			baseChangedByID = int(baseSnap.Raw.CreatedBy.Int64)
		}
		baseChangedBy := resolveUserName(baseChangedByID)
		baseChangedAt := ""
		if baseSnap != nil && !baseSnap.Raw.CreatedAt.IsZero() {
			baseChangedAt = baseSnap.Raw.CreatedAt.Format("02/01/2006 15:04")
		}
		baseModeratedBy := ""
		baseModeratedByID := 0
		baseModeratedAt := ""
		if baseSnap != nil && baseSnap.Raw.ModeratedBy.Valid {
			baseModeratedByID = int(baseSnap.Raw.ModeratedBy.Int64)
			baseModeratedBy = resolveUserName(baseModeratedByID)
		}
		if baseSnap != nil && baseSnap.Raw.ModeratedAt.Valid {
			baseModeratedAt = baseSnap.Raw.ModeratedAt.Time.Format("02/01/2006 15:04")
		}
		history = append(history, registreHistoryItem{
			ID:             0,
			Key:            "base",
			Seq:            1,
			ChangeType:     "",
			FieldKey:       "",
			OldValue:       "",
			NewValue:       "",
			ChangedAt:      baseChangedAt,
			ChangedBy:      baseChangedBy,
			ChangedByID:    baseChangedByID,
			ModeratedBy:    baseModeratedBy,
			ModeratedByID:  baseModeratedByID,
			ModeratedAt:    baseModeratedAt,
			ModeracioEstat: func() string {
				if publishedVersion == 1 {
					return "publicat"
				}
				return "publicat_anterior"
			}(),
			HasSnapshot:   true,
			HasComparator: true,
			IsPublished:    publishedVersion == 1,
		})
	}

	if publishedKey == "published" && publishedSnap != nil && publishedSource != "after" && totalChanges == 0 {
		changedByID := 0
		if publishedSnap.Raw.ModeratedBy.Valid {
			changedByID = int(publishedSnap.Raw.ModeratedBy.Int64)
		} else if publishedSnap.Raw.CreatedBy.Valid {
			changedByID = int(publishedSnap.Raw.CreatedBy.Int64)
		}
		changedBy := resolveUserName(changedByID)
		changedAt := ""
		if publishedSnap.Raw.ModeratedAt.Valid {
			changedAt = publishedSnap.Raw.ModeratedAt.Time.Format("02/01/2006 15:04")
		} else if !publishedSnap.Raw.UpdatedAt.IsZero() {
			changedAt = publishedSnap.Raw.UpdatedAt.Format("02/01/2006 15:04")
		} else if !publishedSnap.Raw.CreatedAt.IsZero() {
			changedAt = publishedSnap.Raw.CreatedAt.Format("02/01/2006 15:04")
		}
		moderatedBy := ""
		moderatedByID := 0
		if publishedSnap.Raw.ModeratedBy.Valid {
			moderatedByID = int(publishedSnap.Raw.ModeratedBy.Int64)
			moderatedBy = resolveUserName(moderatedByID)
		}
		moderatedAt := ""
		if publishedSnap.Raw.ModeratedAt.Valid {
			moderatedAt = publishedSnap.Raw.ModeratedAt.Time.Format("02/01/2006 15:04")
		}
		publishedItem := registreHistoryItem{
			ID:             0,
			Key:            "published",
			Seq:            publishedVersion,
			ChangeType:     "",
			FieldKey:       "",
			OldValue:       "",
			NewValue:       "",
			ChangedAt:      changedAt,
			ChangedBy:      changedBy,
			ChangedByID:    changedByID,
			ModeratedBy:    moderatedBy,
			ModeratedByID:  moderatedByID,
			ModeratedAt:    moderatedAt,
			ModeracioEstat: "publicat",
			HasSnapshot:    true,
			HasComparator:  true,
			IsPublished:    true,
		}
		history = append([]registreHistoryItem{publishedItem}, history...)
	}

	var viewFields []registreHistoryFieldView
	viewLabel := ""
	viewToken := strings.TrimSpace(r.URL.Query().Get("view"))
	if viewToken != "" {
		switch viewToken {
		case "current":
			viewFields = buildSnapshotView(lang, llibre, currentSnapshot)
			viewLabel = T(lang, "records.history.current")
		case "published":
			if publishedSnap != nil {
				viewFields = buildSnapshotView(lang, llibre, publishedSnap)
				viewLabel = T(lang, "records.history.published")
			}
		default:
			if viewID, _ := strconv.Atoi(viewToken); viewID > 0 {
				if snap, ok := snaps[viewID]; ok {
					viewSnap := pickSnapshotForView(snap)
					if viewSnap != nil {
						viewFields = buildSnapshotView(lang, llibre, viewSnap)
						seqLabel := versionNumberForChangeID(viewID, seqByID, offset)
						if seqLabel == 0 {
							seqLabel = viewID
						}
						viewLabel = fmt.Sprintf("%s #%d", T(lang, "records.history.version"), seqLabel)
					}
				}
			}
		}
	}

	var compareFields []registreHistoryFieldDiff
	compareLeftLabel := ""
	compareRightLabel := ""
	compareParam := strings.TrimSpace(r.URL.Query().Get("compare"))
	if compareParam != "" {
		parts := strings.Split(compareParam, ",")
		if len(parts) == 2 {
			leftToken := strings.TrimSpace(parts[0])
			rightToken := strings.TrimSpace(parts[1])
			rankForToken := func(token string) int {
				token = strings.TrimSpace(token)
				switch token {
				case "", "current":
					if publishedVersion > 0 {
						return publishedVersion
					}
					return totalChanges + offset
				case "published":
					if publishedVersion > 0 {
						return publishedVersion
					}
					return totalChanges + offset
				case "base":
					return 1
				}
				if id, _ := strconv.Atoi(token); id > 0 {
					if version := versionNumberForChangeID(id, seqByID, offset); version > 0 {
						return version
					}
				}
				return 0
			}
			resolveSnapshot := func(token string) (*transcripcioSnapshot, string) {
				token = strings.TrimSpace(token)
				if token == "" || token == "current" {
					if publishedVersion > 0 {
						return currentSnapshot, fmt.Sprintf("%s #%d", T(lang, "records.history.version"), publishedVersion)
					}
					return currentSnapshot, T(lang, "records.history.current")
				}
				if token == "published" {
					if publishedSnap != nil {
						if publishedVersion > 0 {
							return publishedSnap, fmt.Sprintf("%s #%d", T(lang, "records.history.version"), publishedVersion)
						}
						return publishedSnap, T(lang, "records.history.published")
					}
					return currentSnapshot, T(lang, "records.history.current")
				}
				if token == "base" && baseSnap != nil {
					return baseSnap, fmt.Sprintf("%s #1", T(lang, "records.history.version"))
				}
				if id, _ := strconv.Atoi(token); id > 0 {
					if snap, ok := snaps[id]; ok {
						viewSnap := pickSnapshotForView(snap)
						if viewSnap != nil {
							seqLabel := versionNumberForChangeID(id, seqByID, offset)
							if seqLabel == 0 {
								seqLabel = id
							}
							return viewSnap, fmt.Sprintf("%s #%d", T(lang, "records.history.version"), seqLabel)
						}
					}
				}
				return nil, ""
			}
			leftSnap, leftLabel := resolveSnapshot(leftToken)
			rightSnap, rightLabel := resolveSnapshot(rightToken)
			if leftSnap != nil && rightSnap != nil {
				handled := false
				comparePublishedID := publishedChangeID
				if comparePublishedID == 0 && publishedKey != "" && publishedKey != "published" {
					if parsed, err := strconv.Atoi(publishedKey); err == nil {
						comparePublishedID = parsed
					}
				}
				if comparePublishedID > 0 {
					publishedToken := strconv.Itoa(comparePublishedID)
					if (leftToken == "published" && rightToken == publishedToken) || (rightToken == "published" && leftToken == publishedToken) {
						if snap, ok := snaps[comparePublishedID]; ok && snap.Before != nil && snap.After != nil {
							leftSnap = snap.Before
							rightSnap = snap.After
							if version := versionNumberForChangeID(comparePublishedID, seqByID, offset); version > 0 {
								if version > 1 {
									leftLabel = fmt.Sprintf("%s #%d", T(lang, "records.history.version"), version-1)
								}
								rightLabel = fmt.Sprintf("%s #%d", T(lang, "records.history.version"), version)
							}
							handled = true
						}
					}
				}
				if !handled {
					leftRank := rankForToken(leftToken)
					rightRank := rankForToken(rightToken)
					if leftRank > 0 && rightRank > 0 && leftRank > rightRank {
						leftSnap, rightSnap = rightSnap, leftSnap
						leftLabel, rightLabel = rightLabel, leftLabel
					}
				}
				compareLeftLabel = leftLabel
				compareRightLabel = rightLabel
				compareFields = buildSnapshotDiff(lang, llibre, leftSnap, rightSnap)
			}
		}
	}

	RenderPrivateTemplate(w, r, "admin-llibres-registres-history.html", map[string]interface{}{
		"Llibre":            llibre,
		"Registre":          registre,
		"History":           history,
		"PublishedKey":       publishedKey,
		"ViewFields":        viewFields,
		"ViewLabel":         viewLabel,
		"CompareFields":     compareFields,
		"CompareLeftLabel":  compareLeftLabel,
		"CompareRightLabel": compareRightLabel,
		"User":              user,
		"Lang":              lang,
		"CanManageArxius":   canManageArxius,
		"CanManagePolicies": canManagePolicies,
		"CanModerate":       canModerate,
		"CanRevert":         canRevert,
	})
}

func (a *App) AdminRevertRegistreChange(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	registreID := extractID(r.URL.Path)
	if registreID == 0 {
		http.NotFound(w, r)
		return
	}
	changeKey := strings.TrimSpace(r.FormValue("change_id"))
	reason := strings.TrimSpace(r.FormValue("reason"))
	if changeKey == "" {
		http.Error(w, "Canvi invàlid", http.StatusBadRequest)
		return
	}
	registre, err := a.DB.GetTranscripcioRaw(registreID)
	if err != nil || registre == nil {
		http.NotFound(w, r)
		return
	}
	target := a.resolveLlibreTarget(registre.LlibreID)
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsRegistresEdit, target)
	if !ok {
		return
	}
	if !a.hasAnyPermissionKey(user.ID, permKeyWikiRevert) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	beforePersones, _ := a.DB.ListTranscripcioPersones(registreID)
	beforeAtributs, _ := a.DB.ListTranscripcioAtributs(registreID)
	changes, _ := a.DB.ListTranscripcioRawChanges(registreID)
	snaps, _ := collectChangeSnapshotsWithSource(changes)
	currentSnapshot := &transcripcioSnapshot{
		Raw:      *registre,
		Persones: beforePersones,
		Atributs: beforeAtributs,
	}
	snaps = fillMissingSnapshots(changes, snaps, currentSnapshot)
	publishedSnap, _, _, _ := resolvePublishedSnapshot(registre, currentSnapshot, changes, snaps)

	var targetSnap *transcripcioSnapshot
	var sourceChange *db.TranscripcioRawChange
	sourceKey := changeKey
	if changeKey == "published" {
		if publishedSnap == nil {
			http.Error(w, "No es pot revertir aquesta versió", http.StatusBadRequest)
			return
		}
		targetSnap = publishedSnap
		latestPendingID := 0
		for i := range changes {
			snap := snaps[changes[i].ID]
			if snap.After == nil {
				continue
			}
			if snap.After.Raw.ModeracioEstat == "publicat" {
				continue
			}
			if changes[i].ID > latestPendingID {
				latestPendingID = changes[i].ID
				sourceChange = &changes[i]
			}
		}
		if sourceChange == nil {
			latestID := 0
			for i := range changes {
				if changes[i].ID > latestID {
					latestID = changes[i].ID
					sourceChange = &changes[i]
				}
			}
		}
	} else {
		changeID, err := strconv.Atoi(changeKey)
		if err != nil || changeID <= 0 {
			http.Error(w, "Canvi invàlid", http.StatusBadRequest)
			return
		}
		for i := range changes {
			if changes[i].ID == changeID {
				sourceChange = &changes[i]
				break
			}
		}
		if sourceChange == nil {
			http.Error(w, "Canvi no trobat", http.StatusNotFound)
			return
		}
		targetSnap = pickSnapshotForView(snaps[changeID])
	}
	if targetSnap == nil {
		http.Error(w, "No es pot revertir aquesta versió", http.StatusBadRequest)
		return
	}

	afterSnap := *targetSnap
	afterSnap.Raw.ModeracioEstat = "pendent"
	afterSnap.Persones = append([]db.TranscripcioPersonaRaw(nil), targetSnap.Persones...)
	afterSnap.Atributs = append([]db.TranscripcioAtributRaw(nil), targetSnap.Atributs...)
	afterSnap.Raw.ID = registreID
	revertMeta := map[string]interface{}{
		"source_change_key": sourceKey,
		"reason":           reason,
	}
	if sourceChange != nil {
		revertMeta["source_change_id"] = sourceChange.ID
	}
	meta := map[string]interface{}{
		"before": map[string]interface{}{
			"raw":      registre,
			"persones": beforePersones,
			"atributs": beforeAtributs,
		},
		"after": map[string]interface{}{
			"raw":      afterSnap.Raw,
			"persones": afterSnap.Persones,
			"atributs": afterSnap.Atributs,
		},
		"revert": revertMeta,
	}
	metaJSON, _ := json.Marshal(meta)
	changeID, _ := a.DB.CreateTranscripcioRawChange(&db.TranscripcioRawChange{
		TranscripcioID: registreID,
		ChangeType:     "revert",
		FieldKey:       "bulk",
		OldValue:       "",
		NewValue:       "",
		Metadata:       string(metaJSON),
		ModeracioEstat: "pendent",
		ChangedBy:      sqlNullIntFromInt(user.ID),
	})
	detail := ""
	if changeID > 0 {
		detail = fmt.Sprintf("change:%d", changeID)
	}
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, "", "editar_registre", "registre", &registreID, "pendent", nil, detail)

	returnURL := strings.TrimSpace(r.FormValue("return_to"))
	if returnURL == "" {
		returnURL = fmt.Sprintf("/documentals/registres/%d/historial", registreID)
	}
	http.Redirect(w, r, returnURL, http.StatusSeeOther)
}

func (a *App) AdminRegistreStats(w http.ResponseWriter, r *http.Request) {
	lang := ResolveLang(r)
	registreID := extractID(r.URL.Path)
	if registreID == 0 {
		http.NotFound(w, r)
		return
	}
	registre, err := a.DB.GetTranscripcioRaw(registreID)
	if err != nil || registre == nil {
		http.NotFound(w, r)
		return
	}
	target := a.resolveLlibreTarget(registre.LlibreID)
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresViewRegistres, target)
	if !ok {
		return
	}
	perms := a.getPermissionsForUser(user.ID)
	canManageArxius := a.hasPerm(perms, permArxius)
	canManagePolicies := perms.CanManagePolicies || perms.Admin
	canModerate := perms.CanModerate || perms.Admin
	llibre, _ := a.DB.GetLlibre(registre.LlibreID)
	if llibre == nil {
		llibre = &db.Llibre{TipusLlibre: registre.TipusActe}
	}
	marks, _ := a.DB.ListTranscripcioMarks([]int{registreID})
	userCache := map[int]*db.User{}
	groupMap := map[string][]registreStatsUser{}
	for _, mark := range marks {
		if !mark.IsPublic {
			continue
		}
		if mark.UserID == 0 {
			continue
		}
		u, ok := userCache[mark.UserID]
		if !ok {
			u, _ = a.DB.GetUserByID(mark.UserID)
			userCache[mark.UserID] = u
		}
		if u == nil || u.Usuari == "" {
			continue
		}
		groupMap[mark.Tipus] = append(groupMap[mark.Tipus], registreStatsUser{
			ID:     u.ID,
			Usuari: u.Usuari,
		})
	}
	groups := []registreStatsGroup{
		{Key: "consanguini", Label: T(lang, "records.stats.group.consanguini"), Users: groupMap["consanguini"]},
		{Key: "politic", Label: T(lang, "records.stats.group.politic"), Users: groupMap["politic"]},
		{Key: "interes", Label: T(lang, "records.stats.group.interes"), Users: groupMap["interes"]},
	}

	RenderPrivateTemplate(w, r, "admin-llibres-registres-stats.html", map[string]interface{}{
		"Llibre":            llibre,
		"Registre":          registre,
		"Groups":            groups,
		"User":              user,
		"Lang":              lang,
		"CanManageArxius":   canManageArxius,
		"CanManagePolicies": canManagePolicies,
		"CanModerate":       canModerate,
		"Now":               time.Now(),
	})
}
