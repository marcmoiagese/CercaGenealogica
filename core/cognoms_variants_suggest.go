package core

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

type cognomVariantSuggestion struct {
	ID          int    `json:"id"`
	Forma       string `json:"forma"`
	Reason      string `json:"reason"`
	ReasonLabel string `json:"reason_label"`
	Distance    int    `json:"distance"`
}

func (a *App) CognomVariantsSuggestJSON(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	if _, _, ok := a.requirePermission(w, r, permAdmin); !ok {
		return
	}
	lang := ResolveLang(r)
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	baseID := parseFormInt(r.URL.Query().Get("cognom_id"))
	if baseID > 0 {
		if canonID, _, err := a.resolveCognomCanonicalID(baseID); err == nil && canonID > 0 {
			baseID = canonID
		}
		if cognom, err := a.DB.GetCognom(baseID); err == nil && cognom != nil {
			q = cognom.Forma
		}
	}
	if q == "" {
		writeJSON(w, map[string]interface{}{"items": []interface{}{}})
		return
	}
	seed := q
	runes := []rune(seed)
	if len(runes) > 2 {
		seed = string(runes[:2])
	}
	limit := 12
	if val := strings.TrimSpace(r.URL.Query().Get("limit")); val != "" {
		if v, err := strconv.Atoi(val); err == nil && v > 0 && v <= 50 {
			limit = v
		}
	}
	list, err := a.DB.ListCognoms(seed, 500, 0)
	if err != nil {
		Errorf("CognomVariantsSuggestJSON error: %v", err)
		http.Error(w, "Error", http.StatusInternalServerError)
		return
	}
	baseKey := NormalizeCognomKey(q)
	baseSound := soundex(baseKey)
	existingVariants := map[string]struct{}{}
	if baseID > 0 {
		if forms, err := a.DB.ListCognomFormesPublicades(baseID); err == nil {
			for _, form := range forms {
				existingVariants[NormalizeCognomKey(form)] = struct{}{}
			}
		}
	}
	suggestions := make([]cognomVariantSuggestion, 0, len(list))
	for _, c := range list {
		if baseID > 0 && c.ID == baseID {
			continue
		}
		key := strings.TrimSpace(c.Key)
		if key == "" {
			key = NormalizeCognomKey(c.Forma)
		}
		if key == "" || key == baseKey {
			continue
		}
		if _, exists := existingVariants[key]; exists {
			continue
		}
		reason := ""
		dist := levenshteinDistance(baseKey, key)
		if baseSound != "" && soundex(key) == baseSound {
			reason = "phonetic"
		}
		if dist > 0 && dist <= 2 {
			reason = "edit_distance"
		}
		if reason == "" {
			continue
		}
		suggestions = append(suggestions, cognomVariantSuggestion{
			ID:          c.ID,
			Forma:       c.Forma,
			Reason:      reason,
			ReasonLabel: T(lang, "admin.surnames.merge.suggestions.reason."+reason),
			Distance:    dist,
		})
	}
	sort.Slice(suggestions, func(i, j int) bool {
		if suggestions[i].Distance != suggestions[j].Distance {
			return suggestions[i].Distance < suggestions[j].Distance
		}
		return strings.ToLower(suggestions[i].Forma) < strings.ToLower(suggestions[j].Forma)
	})
	if len(suggestions) > limit {
		suggestions = suggestions[:limit]
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"items": suggestions})
}

func levenshteinDistance(a, b string) int {
	if a == b {
		return 0
	}
	if a == "" {
		return len(b)
	}
	if b == "" {
		return len(a)
	}
	ar := []rune(a)
	br := []rune(b)
	if len(ar) == 0 {
		return len(br)
	}
	if len(br) == 0 {
		return len(ar)
	}
	prev := make([]int, len(br)+1)
	curr := make([]int, len(br)+1)
	for j := 0; j <= len(br); j++ {
		prev[j] = j
	}
	for i := 1; i <= len(ar); i++ {
		curr[0] = i
		for j := 1; j <= len(br); j++ {
			cost := 0
			if ar[i-1] != br[j-1] {
				cost = 1
			}
			del := prev[j] + 1
			ins := curr[j-1] + 1
			sub := prev[j-1] + cost
			curr[j] = del
			if ins < curr[j] {
				curr[j] = ins
			}
			if sub < curr[j] {
				curr[j] = sub
			}
		}
		copy(prev, curr)
	}
	return prev[len(br)]
}
