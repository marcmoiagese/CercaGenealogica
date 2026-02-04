package core

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func (a *App) cognomStatsAPI(w http.ResponseWriter, r *http.Request, cognomID int, parts []string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	switch parts[0] {
	case "total":
		a.cognomStatsTotalJSON(w, r, cognomID)
	case "series":
		a.cognomStatsSeriesJSON(w, r, cognomID)
	case "zones":
		a.cognomStatsZonesJSON(w, r, cognomID)
	case "top":
		a.cognomStatsTopJSON(w, r, cognomID)
	default:
		http.NotFound(w, r)
	}
}

func (a *App) cognomStatsTotalJSON(w http.ResponseWriter, r *http.Request, cognomID int) {
	stat, err := a.DB.GetCognomStatsTotal(cognomID)
	if err != nil {
		http.Error(w, "failed to load", http.StatusInternalServerError)
		return
	}
	totalPersones := 0
	totalAparicions := 0
	updatedAt := ""
	if stat != nil {
		totalPersones = stat.TotalPersones
		totalAparicions = stat.TotalAparicions
		if stat.UpdatedAt.Valid {
			updatedAt = stat.UpdatedAt.Time.Format(time.RFC3339)
		}
	}
	writeJSON(w, map[string]interface{}{
		"cognom_id":        cognomID,
		"total_persones":   totalPersones,
		"total_aparicions": totalAparicions,
		"updated_at":       updatedAt,
	})
}

func (a *App) cognomStatsSeriesJSON(w http.ResponseWriter, r *http.Request, cognomID int) {
	bucket := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("bucket")))
	if bucket != "decade" {
		bucket = "year"
	}
	from := parseFormInt(r.URL.Query().Get("from"))
	to := parseFormInt(r.URL.Query().Get("to"))
	if from > 0 && to > 0 && from > to {
		from, to = to, from
	}
	var rows []db.CognomStatsAnyRow
	var err error
	if bucket == "decade" {
		rows, err = a.DB.ListCognomStatsAnyDecade(cognomID, from, to)
	} else {
		rows, err = a.DB.ListCognomStatsAny(cognomID, from, to)
	}
	if err != nil {
		http.Error(w, "failed to load", http.StatusInternalServerError)
		return
	}
	payload := make([]map[string]interface{}, 0, len(rows))
	minAny := 0
	maxAny := 0
	for _, row := range rows {
		payload = append(payload, map[string]interface{}{
			"x": row.Any,
			"y": row.Total,
		})
		if row.Any <= 0 {
			continue
		}
		if minAny == 0 || row.Any < minAny {
			minAny = row.Any
		}
		if maxAny == 0 || row.Any > maxAny {
			maxAny = row.Any
		}
	}
	if from == 0 {
		from = minAny
	}
	if to == 0 {
		to = maxAny
	}
	writeJSON(w, map[string]interface{}{
		"bucket": bucket,
		"from":   from,
		"to":     to,
		"items":  payload,
	})
}

func (a *App) cognomStatsZonesJSON(w http.ResponseWriter, r *http.Request, cognomID int) {
	ancestorType, level := resolveCognomStatsScope(r.URL.Query().Get("ancestor_type"), r.URL.Query().Get("level"))
	if ancestorType == "" {
		http.Error(w, "invalid scope", http.StatusBadRequest)
		return
	}
	any := parseFormInt(r.URL.Query().Get("any"))
	limit := parseFormInt(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	items, err := a.DB.ListCognomStatsAncestor(cognomID, ancestorType, level, any, limit)
	if err != nil {
		http.Error(w, "failed to load", http.StatusInternalServerError)
		return
	}
	totalDistinct, _ := a.DB.CountCognomStatsAncestorDistinct(cognomID, ancestorType, level, any)
	payload := make([]map[string]interface{}, 0, len(items))
	for _, item := range items {
		payload = append(payload, map[string]interface{}{
			"id":    item.AncestorID,
			"label": strings.TrimSpace(item.Label),
			"total": item.Total,
			"level": item.Level,
		})
	}
	writeJSON(w, map[string]interface{}{
		"ancestor_type": ancestorType,
		"level":         level,
		"any":           any,
		"totalDistinct": totalDistinct,
		"items":         payload,
	})
}

func (a *App) cognomStatsTopJSON(w http.ResponseWriter, r *http.Request, cognomID int) {
	ancestorType, level := resolveCognomStatsScope(r.URL.Query().Get("scope"), r.URL.Query().Get("level"))
	if ancestorType == "" {
		http.Error(w, "invalid scope", http.StatusBadRequest)
		return
	}
	any := parseFormInt(r.URL.Query().Get("any"))
	limit := parseFormInt(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}
	items, err := a.DB.ListCognomStatsAncestor(cognomID, ancestorType, level, any, limit)
	if err != nil {
		http.Error(w, "failed to load", http.StatusInternalServerError)
		return
	}
	totalDistinct, _ := a.DB.CountCognomStatsAncestorDistinct(cognomID, ancestorType, level, any)
	payload := make([]map[string]interface{}, 0, len(items))
	for _, item := range items {
		payload = append(payload, map[string]interface{}{
			"id":    item.AncestorID,
			"label": strings.TrimSpace(item.Label),
			"total": item.Total,
			"level": item.Level,
		})
	}
	writeJSON(w, map[string]interface{}{
		"scope":         ancestorType,
		"level":         level,
		"any":           any,
		"totalDistinct": totalDistinct,
		"items":         payload,
	})
}

func resolveCognomStatsScope(raw string, levelRaw string) (string, int) {
	raw = strings.ToLower(strings.TrimSpace(raw))
	level := parseFormInt(levelRaw)
	switch raw {
	case "", "municipi":
		return "municipi", 0
	case "pais":
		return "nivell_admin", 1
	case "nivell_admin":
		return "nivell_admin", level
	default:
		if strings.HasPrefix(raw, "nivell_") {
			if n, err := strconv.Atoi(strings.TrimPrefix(raw, "nivell_")); err == nil && n > 0 {
				return "nivell_admin", n
			}
		}
	}
	return "", 0
}
