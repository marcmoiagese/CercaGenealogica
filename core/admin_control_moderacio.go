package core

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

// AdminControlModeracioSummaryAPI retorna el resum de moderació amb SLA.
func (a *App) AdminControlModeracioSummaryAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user, _, canModerateAll, ok := a.requireModeracioUser(w, r)
	if !ok {
		return
	}
	filters, _ := parseModeracioFilters(r)
	_, _, summary := a.buildModeracioItems(ResolveLang(r), 1, 1, user, canModerateAll, filters)
	payload := map[string]interface{}{
		"ok":          true,
		"summary":     summary,
		"generated_at": time.Now().Format(time.RFC3339),
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(payload)
}

// AdminControlModeracioJobStatus retorna l'estat d'un job de bulk moderació.
func (a *App) AdminControlModeracioJobStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	if _, _, _, ok := a.requireModeracioUser(w, r); !ok {
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 6 {
		http.NotFound(w, r)
		return
	}
	jobID := strings.TrimSpace(parts[5])
	if jobID == "" {
		http.NotFound(w, r)
		return
	}
	job, ok := a.moderacioBulkStore().snapshot(jobID)
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, map[string]interface{}{"ok": true, "job": job})
}
