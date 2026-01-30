package core

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type wikiChangeMeta struct {
	Before         json.RawMessage `json:"before"`
	After          json.RawMessage `json:"after"`
	SourceChangeID int             `json:"source_change_id,omitempty"`
}

type WikiDiffField struct {
	Key     string
	Label   string
	Before  string
	After   string
	Changed bool
}

type wikiHistoryFieldView struct {
	Label string
	Value string
}

func parseWikiChangeMeta(metadata string) (json.RawMessage, json.RawMessage) {
	payload := strings.TrimSpace(metadata)
	if payload == "" {
		return nil, nil
	}
	parse := func(raw []byte) (json.RawMessage, json.RawMessage, bool) {
		var meta wikiChangeMeta
		if err := json.Unmarshal(raw, &meta); err == nil {
			if len(meta.Before) > 0 || len(meta.After) > 0 {
				return meta.Before, meta.After, true
			}
		}
		rawMap := map[string]json.RawMessage{}
		if err := json.Unmarshal(raw, &rawMap); err == nil {
			before := rawMap["before"]
			after := rawMap["after"]
			if len(before) > 0 || len(after) > 0 {
				return before, after, true
			}
		}
		return nil, nil, false
	}
	if before, after, ok := parse([]byte(payload)); ok {
		return before, after
	}
	var nested string
	if err := json.Unmarshal([]byte(payload), &nested); err == nil {
		if strings.TrimSpace(nested) != "" {
			if before, after, ok := parse([]byte(nested)); ok {
				return before, after
			}
		}
	}
	return nil, nil
}

func buildWikiChangeMetadata(beforeJSON, afterJSON json.RawMessage, sourceChangeID int) (string, error) {
	meta := wikiChangeMeta{
		Before:         beforeJSON,
		After:          afterJSON,
		SourceChangeID: sourceChangeID,
	}
	payload, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func unmarshalWikiSnapshot(raw json.RawMessage, dest interface{}) error {
	if len(raw) == 0 {
		return fmt.Errorf("snapshot buit")
	}
	if err := json.Unmarshal(raw, dest); err == nil {
		return nil
	}
	var nested string
	if err := json.Unmarshal(raw, &nested); err == nil && strings.TrimSpace(nested) != "" {
		return json.Unmarshal([]byte(nested), dest)
	}
	return fmt.Errorf("snapshot invàlid")
}

func wikiLabelForKey(key string) string {
	return key
}

func buildWikiDiff(beforeJSON, afterJSON json.RawMessage) []WikiDiffField {
	before := flattenWikiJSON(beforeJSON)
	after := flattenWikiJSON(afterJSON)
	keys := map[string]struct{}{}
	for k := range before {
		keys[k] = struct{}{}
	}
	for k := range after {
		keys[k] = struct{}{}
	}
	var list []string
	for k := range keys {
		if shouldSkipWikiKey(k) {
			continue
		}
		list = append(list, k)
	}
	sort.Strings(list)
	var out []WikiDiffField
	for _, key := range list {
		beforeVal := strings.TrimSpace(before[key])
		afterVal := strings.TrimSpace(after[key])
		if beforeVal == afterVal {
			continue
		}
		out = append(out, WikiDiffField{
			Key:     key,
			Label:   wikiLabelForKey(key),
			Before:  beforeVal,
			After:   afterVal,
			Changed: true,
		})
	}
	return out
}

func buildWikiViewFields(snapshot json.RawMessage) []wikiHistoryFieldView {
	flat := flattenWikiJSON(snapshot)
	keys := make([]string, 0, len(flat))
	for k := range flat {
		if shouldSkipWikiKey(k) {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out []wikiHistoryFieldView
	for _, key := range keys {
		out = append(out, wikiHistoryFieldView{
			Label: wikiLabelForKey(key),
			Value: strings.TrimSpace(flat[key]),
		})
	}
	return out
}

func flattenWikiJSON(raw json.RawMessage) map[string]string {
	if len(raw) == 0 {
		return map[string]string{}
	}
	var data interface{}
	if err := json.Unmarshal(raw, &data); err != nil {
		var nested string
		if err2 := json.Unmarshal(raw, &nested); err2 == nil && strings.TrimSpace(nested) != "" {
			_ = json.Unmarshal([]byte(nested), &data)
		}
	}
	out := map[string]string{}
	switch v := data.(type) {
	case map[string]interface{}:
		for key, val := range v {
			flattenWikiValue(key, val, out)
		}
	default:
		if data != nil {
			flattenWikiValue("value", data, out)
		}
	}
	return out
}

func flattenWikiValue(prefix string, val interface{}, out map[string]string) {
	switch v := val.(type) {
	case map[string]interface{}:
		if normalized, ok := normalizeNullWrapper(v); ok {
			if prefix == "" {
				prefix = "value"
			}
			out[prefix] = normalized
			return
		}
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			next := key
			if prefix != "" {
				next = prefix + "." + key
			}
			flattenWikiValue(next, v[key], out)
		}
	case []interface{}:
		if len(v) == 0 {
			if prefix == "" {
				prefix = "value"
			}
			out[prefix] = "[]"
			return
		}
		for i, item := range v {
			key := fmt.Sprintf("%s[%d]", prefix, i)
			flattenWikiValue(key, item, out)
		}
	default:
		if prefix == "" {
			prefix = "value"
		}
		out[prefix] = stringifyWikiScalar(v)
	}
}

func normalizeNullWrapper(val map[string]interface{}) (string, bool) {
	rawValid, ok := val["Valid"]
	if !ok {
		return "", false
	}
	valid, ok := rawValid.(bool)
	if !ok {
		return "", false
	}
	if !valid {
		return "", true
	}
	if raw, ok := val["String"]; ok {
		return stringifyWikiScalar(raw), true
	}
	if raw, ok := val["Int64"]; ok {
		return stringifyWikiScalar(raw), true
	}
	if raw, ok := val["Float64"]; ok {
		return stringifyWikiScalar(raw), true
	}
	if raw, ok := val["Bool"]; ok {
		return stringifyWikiScalar(raw), true
	}
	if raw, ok := val["Time"]; ok {
		return stringifyWikiScalar(raw), true
	}
	return "", true
}

func stringifyWikiScalar(val interface{}) string {
	switch v := val.(type) {
	case nil:
		return ""
	case string:
		return v
	case bool:
		if v {
			return "true"
		}
		return "false"
	case float64:
		if v == float64(int64(v)) {
			return fmt.Sprintf("%d", int64(v))
		}
		return fmt.Sprintf("%.4f", v)
	default:
		return fmt.Sprint(v)
	}
}

func shouldSkipWikiKey(key string) bool {
	base := key
	if idx := strings.LastIndex(base, "."); idx >= 0 {
		base = base[idx+1:]
	}
	if idx := strings.Index(base, "["); idx >= 0 {
		base = base[:idx]
	}
	switch base {
	case "ID", "CreatedBy", "ModeracioEstat", "ModeracioMotiu", "ModeratedBy", "ModeratedAt":
		return true
	default:
		return false
	}
}

func filterVisibleWikiChanges(changes []db.WikiChange, userID int, canModerate bool) []db.WikiChange {
	if canModerate {
		return changes
	}
	visible := make([]db.WikiChange, 0, len(changes))
	for _, ch := range changes {
		if ch.ModeracioEstat == "publicat" {
			visible = append(visible, ch)
			continue
		}
		if userID > 0 && ch.ChangedBy.Valid && int(ch.ChangedBy.Int64) == userID {
			visible = append(visible, ch)
		}
	}
	return visible
}

func (a *App) applyWikiMunicipiChange(change *db.WikiChange, motiu string, moderatorID int) error {
	_, after := parseWikiChangeMeta(change.Metadata)
	if len(after) == 0 {
		return fmt.Errorf("canvi sense dades")
	}
	var mun db.Municipi
	if err := unmarshalWikiSnapshot(after, &mun); err != nil {
		return err
	}
	mun.ID = change.ObjectID
	mun.ModeracioEstat = "publicat"
	mun.ModeracioMotiu = motiu
	mun.ModeratedBy = sqlNullIntFromInt(moderatorID)
	mun.ModeratedAt = sql.NullTime{Time: time.Now(), Valid: true}
	return a.DB.UpdateMunicipi(&mun)
}

func (a *App) applyWikiArxiuChange(change *db.WikiChange, motiu string, moderatorID int) error {
	_, after := parseWikiChangeMeta(change.Metadata)
	if len(after) == 0 {
		return fmt.Errorf("canvi sense dades")
	}
	var arxiu db.Arxiu
	if err := unmarshalWikiSnapshot(after, &arxiu); err != nil {
		return err
	}
	arxiu.ID = change.ObjectID
	arxiu.ModeracioEstat = "publicat"
	arxiu.ModeracioMotiu = motiu
	arxiu.ModeratedBy = sqlNullIntFromInt(moderatorID)
	arxiu.ModeratedAt = sql.NullTime{Time: time.Now(), Valid: true}
	return a.DB.UpdateArxiu(&arxiu)
}

func (a *App) applyWikiLlibreChange(change *db.WikiChange, motiu string, moderatorID int) error {
	_, after := parseWikiChangeMeta(change.Metadata)
	if len(after) == 0 {
		return fmt.Errorf("canvi sense dades")
	}
	var llibre db.Llibre
	if err := unmarshalWikiSnapshot(after, &llibre); err != nil {
		return err
	}
	llibre.ID = change.ObjectID
	llibre.ModeracioEstat = "publicat"
	llibre.ModeracioMotiu = motiu
	llibre.ModeratedBy = sqlNullIntFromInt(moderatorID)
	llibre.ModeratedAt = sql.NullTime{Time: time.Now(), Valid: true}
	if err := a.DB.UpdateLlibre(&llibre); err != nil {
		return err
	}
	arxiuID := extractWikiArxiuID(change.Metadata)
	if arxiuID > 0 {
		if rels, err := a.DB.ListLlibreArxius(llibre.ID); err == nil {
			for _, rel := range rels {
				if rel.ArxiuID != arxiuID {
					_ = a.DB.DeleteArxiuLlibre(rel.ArxiuID, llibre.ID)
				}
			}
		}
		_ = a.DB.AddArxiuLlibre(arxiuID, llibre.ID, "", "")
	}
	return nil
}

func (a *App) applyWikiPersonaChange(change *db.WikiChange, motiu string, moderatorID int) error {
	_ = motiu
	_, after := parseWikiChangeMeta(change.Metadata)
	if len(after) == 0 {
		return fmt.Errorf("canvi sense dades")
	}
	var persona db.Persona
	if err := unmarshalWikiSnapshot(after, &persona); err != nil {
		return err
	}
	persona.ID = change.ObjectID
	persona.ModeracioEstat = "publicat"
	persona.ModeratedBy = sqlNullIntFromInt(moderatorID)
	persona.ModeratedAt = sql.NullTime{Time: time.Now(), Valid: true}
	return a.DB.UpdatePersona(&persona)
}

func (a *App) applyWikiCognomChange(change *db.WikiChange, motiu string, moderatorID int) error {
	_ = motiu
	_ = moderatorID
	_, after := parseWikiChangeMeta(change.Metadata)
	if len(after) == 0 {
		return fmt.Errorf("canvi sense dades")
	}
	var cognom db.Cognom
	if err := unmarshalWikiSnapshot(after, &cognom); err != nil {
		return err
	}
	cognom.ID = change.ObjectID
	return a.DB.UpdateCognom(&cognom)
}

func extractWikiArxiuID(metadata string) int {
	payload := strings.TrimSpace(metadata)
	if payload == "" {
		return 0
	}
	parse := func(raw []byte) int {
		var data map[string]interface{}
		if err := json.Unmarshal(raw, &data); err != nil {
			return 0
		}
		val, ok := data["arxiu_id"]
		if !ok {
			return 0
		}
		switch v := val.(type) {
		case float64:
			return int(v)
		case string:
			if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
				return n
			}
		}
		return 0
	}
	if id := parse([]byte(payload)); id > 0 {
		return id
	}
	var nested string
	if err := json.Unmarshal([]byte(payload), &nested); err == nil && strings.TrimSpace(nested) != "" {
		return parse([]byte(nested))
	}
	return 0
}
