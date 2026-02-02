package core

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type templateSimilarRequest struct {
	ModelJSON string `json:"model_json"`
	Limit     int    `json:"limit"`
}

type templateSimilarity struct {
	ID         int
	Name       string
	Score      float64
	CanEdit    bool
	CanClone   bool
	Visibility string
}

type templateFeatures struct {
	BookMode   string
	Targets    map[string]struct{}
	Roles      map[string]struct{}
	AttrKeys   map[string]struct{}
	Transforms map[string]struct{}
}

func (a *App) importTemplatesSimilarJSON(w http.ResponseWriter, r *http.Request, user *db.User) {
	var payload templateSimilarRequest
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&payload); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	modelJSON := strings.TrimSpace(payload.ModelJSON)
	if modelJSON == "" {
		http.Error(w, "model required", http.StatusBadRequest)
		return
	}
	limit := payload.Limit
	if limit <= 0 || limit > 20 {
		limit = 10
	}
	sourceFeatures := extractTemplateFeatures(modelJSON)
	sourceSignature := db.ComputeTemplateSignature(modelJSON)
	filter := db.CSVImportTemplateFilter{
		OwnerUserID:   user.ID,
		IncludePublic: true,
		Limit:         500,
	}
	templates, err := a.DB.ListCSVImportTemplates(filter)
	if err != nil {
		http.Error(w, "failed to load templates", http.StatusInternalServerError)
		return
	}
	perms, _ := a.permissionsFromContext(r)
	similar := make([]templateSimilarity, 0, len(templates))
	for _, tpl := range templates {
		if tpl.ID == 0 {
			continue
		}
		score := computeTemplateSimilarity(sourceFeatures, extractTemplateFeatures(tpl.ModelJSON))
		if sourceSignature != "" && tpl.Signature == sourceSignature {
			score = 1
		}
		if score <= 0 {
			continue
		}
		ownerID := 0
		if tpl.OwnerUserID.Valid {
			ownerID = int(tpl.OwnerUserID.Int64)
		}
		isOwner := ownerID == user.ID
		canEdit := perms.Admin || isOwner
		canClone := strings.TrimSpace(strings.ToLower(tpl.Visibility)) == "public" && !isOwner
		similar = append(similar, templateSimilarity{
			ID:         tpl.ID,
			Name:       tpl.Name,
			Score:      score,
			CanEdit:    canEdit,
			CanClone:   canClone,
			Visibility: tpl.Visibility,
		})
	}
	sort.Slice(similar, func(i, j int) bool {
		if similar[i].Score == similar[j].Score {
			return similar[i].Name < similar[j].Name
		}
		return similar[i].Score > similar[j].Score
	})
	if len(similar) > limit {
		similar = similar[:limit]
	}
	items := make([]map[string]interface{}, 0, len(similar))
	for _, s := range similar {
		items = append(items, map[string]interface{}{
			"id":         s.ID,
			"name":       s.Name,
			"score":      s.Score,
			"can_edit":   s.CanEdit,
			"can_clone":  s.CanClone,
			"visibility": s.Visibility,
		})
	}
	writeJSON(w, map[string]interface{}{"items": items})
}

func extractTemplateFeatures(modelJSON string) templateFeatures {
	features := templateFeatures{
		Targets:    map[string]struct{}{},
		Roles:      map[string]struct{}{},
		AttrKeys:   map[string]struct{}{},
		Transforms: map[string]struct{}{},
	}
	var root map[string]interface{}
	if err := json.Unmarshal([]byte(modelJSON), &root); err != nil {
		return features
	}
	if book, ok := root["book_resolution"].(map[string]interface{}); ok {
		if mode, ok := book["mode"].(string); ok {
			features.BookMode = strings.TrimSpace(mode)
		}
	}
	mapping, ok := root["mapping"].(map[string]interface{})
	if !ok {
		return features
	}
	cols, ok := mapping["columns"].([]interface{})
	if !ok {
		return features
	}
	for _, c := range cols {
		colMap, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		if cond, ok := colMap["condition"].(map[string]interface{}); ok {
			extractMapToFeatures(cond["then"], &features)
			extractMapToFeatures(cond["else"], &features)
		}
		extractMapToFeatures(colMap["map_to"], &features)
	}
	return features
}

func extractMapToFeatures(raw interface{}, features *templateFeatures) {
	if features == nil || raw == nil {
		return
	}
	switch v := raw.(type) {
	case []interface{}:
		for _, item := range v {
			extractMapToFeatures(item, features)
		}
	case map[string]interface{}:
		if target, ok := v["target"].(string); ok {
			target = strings.TrimSpace(target)
			if target != "" {
				features.Targets[target] = struct{}{}
				if strings.HasPrefix(target, "person.") {
					parts := strings.Split(target, ".")
					if len(parts) > 1 {
						features.Roles[parts[1]] = struct{}{}
					}
				}
				if strings.HasPrefix(target, "attr.") {
					parts := strings.Split(target, ".")
					if len(parts) > 1 {
						features.AttrKeys[parts[1]] = struct{}{}
					}
				}
			}
		}
		if transforms, ok := v["transforms"]; ok {
			extractTransformFeatures(transforms, features)
		}
		if transforms, ok := v["transform"]; ok {
			extractTransformFeatures(transforms, features)
		}
		if cond, ok := v["condition"].(map[string]interface{}); ok {
			extractMapToFeatures(cond["then"], features)
			extractMapToFeatures(cond["else"], features)
		}
	default:
		return
	}
}

func extractTransformFeatures(raw interface{}, features *templateFeatures) {
	list, ok := raw.([]interface{})
	if !ok {
		return
	}
	for _, t := range list {
		switch tr := t.(type) {
		case string:
			name := strings.TrimSpace(tr)
			if name != "" {
				features.Transforms[name] = struct{}{}
			}
		case map[string]interface{}:
			name := ""
			if v, ok := tr["name"].(string); ok {
				name = v
			} else if v, ok := tr["op"].(string); ok {
				name = v
			}
			name = strings.TrimSpace(name)
			if name != "" {
				features.Transforms[name] = struct{}{}
			}
		}
	}
}

func computeTemplateSimilarity(a, b templateFeatures) float64 {
	score := 0.0
	parts := 5.0
	if strings.TrimSpace(a.BookMode) != "" || strings.TrimSpace(b.BookMode) != "" {
		if strings.TrimSpace(a.BookMode) == strings.TrimSpace(b.BookMode) {
			score += 1
		}
	} else {
		parts--
	}
	score += jaccard(a.Targets, b.Targets)
	score += jaccard(a.Roles, b.Roles)
	score += jaccard(a.AttrKeys, b.AttrKeys)
	score += jaccard(a.Transforms, b.Transforms)
	if parts <= 0 {
		return 0
	}
	return score / parts
}

func jaccard(a, b map[string]struct{}) float64 {
	if len(a) == 0 && len(b) == 0 {
		return 1
	}
	intersection := 0
	union := 0
	seen := map[string]bool{}
	for k := range a {
		union++
		seen[k] = true
		if _, ok := b[k]; ok {
			intersection++
		}
	}
	for k := range b {
		if seen[k] {
			continue
		}
		union++
	}
	if union == 0 {
		return 0
	}
	return float64(intersection) / float64(union)
}

func buildTemplateSimilarityReason(a, b templateFeatures) string {
	return ""
}
