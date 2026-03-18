package core

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"
)

const adminControlModeracioSummaryCacheTTL = 10 * time.Second

type adminControlModeracioSummaryCacheState struct {
	mu       sync.RWMutex
	loaded   bool
	total    int
	byType   []adminControlPendingType
	cachedAt time.Time
}

var adminControlModeracioSummaryCache adminControlModeracioSummaryCacheState

// AdminControlModeracioSummaryAPI retorna el resum de moderació amb SLA.
func (a *App) AdminControlModeracioSummaryAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user, _, _, ok := a.requireModeracioUser(w, r)
	if !ok {
		return
	}
	filters, _ := parseModeracioFilters(r)
	start := time.Now()
	summary, mode, err := a.adminControlModeracioSummaryCached(filters)
	if err != nil {
		http.Error(w, "No s'ha pogut carregar el resum", http.StatusInternalServerError)
		return
	}
	Infof("Moderacio summary mode=%s user=%d status=%s type=%s age=%s dur=%s", mode, user.ID, strings.TrimSpace(filters.Status), strings.TrimSpace(filters.Type), strings.TrimSpace(filters.AgeBucket), time.Since(start))
	payload := map[string]interface{}{
		"ok":           true,
		"summary":      summary,
		"summary_mode": mode,
		"generated_at": time.Now().Format(time.RFC3339),
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(payload)
}

func (a *App) adminControlModeracioSummaryCached(filters moderacioFilters) (moderacioSummary, string, error) {
	total, byType, err := a.adminControlModeracioPendingCountsCached()
	if err != nil {
		return moderacioSummary{}, "", err
	}
	return buildModeracioSummaryFromCounts(filters, total, byType), moderacioSummaryMode(filters), nil
}

func (a *App) adminControlModeracioPendingCountsCached() (int, []adminControlPendingType, error) {
	now := time.Now()
	adminControlModeracioSummaryCache.mu.RLock()
	if adminControlModeracioSummaryCache.loaded && now.Sub(adminControlModeracioSummaryCache.cachedAt) < adminControlModeracioSummaryCacheTTL {
		total := adminControlModeracioSummaryCache.total
		byType := adminControlModeracioSummaryCache.byType
		adminControlModeracioSummaryCache.mu.RUnlock()
		return total, byType, nil
	}
	adminControlModeracioSummaryCache.mu.RUnlock()

	adminControlModeracioSummaryCache.mu.Lock()
	defer adminControlModeracioSummaryCache.mu.Unlock()
	if adminControlModeracioSummaryCache.loaded && now.Sub(adminControlModeracioSummaryCache.cachedAt) < adminControlModeracioSummaryCacheTTL {
		return adminControlModeracioSummaryCache.total, adminControlModeracioSummaryCache.byType, nil
	}
	total, byType, err := a.adminPendingModerationCounts()
	if err != nil {
		return 0, nil, err
	}
	adminControlModeracioSummaryCache.loaded = true
	adminControlModeracioSummaryCache.cachedAt = now
	adminControlModeracioSummaryCache.total = total
	adminControlModeracioSummaryCache.byType = byType
	return total, byType, nil
}

func buildModeracioSummaryFromCounts(filters moderacioFilters, total int, byType []adminControlPendingType) moderacioSummary {
	filtered := byType
	filterType := strings.TrimSpace(filters.Type)
	if filterType != "" && filterType != "all" {
		filtered = []adminControlPendingType{}
		total = 0
		for _, item := range byType {
			if item.Type == filterType {
				filtered = append(filtered, item)
				total = item.Total
				break
			}
		}
	}
	summary := moderacioSummary{
		Total:    total,
		SLA0_24h: 0,
		SLA1_3d:  0,
		SLA3Plus: 0,
	}
	if len(filtered) == 0 {
		summary.ByType = []moderacioTypeCount{}
		return summary
	}
	summary.ByType = make([]moderacioTypeCount, 0, len(filtered))
	for _, item := range filtered {
		summary.ByType = append(summary.ByType, moderacioTypeCount{
			Type:  item.Type,
			Total: item.Total,
		})
		if item.Total > summary.TopTypeTotal {
			summary.TopTypeTotal = item.Total
			summary.TopType = item.Type
		}
	}
	return summary
}

func moderacioSummaryMode(filters moderacioFilters) string {
	status := strings.TrimSpace(filters.Status)
	if status != "" && status != "pendent" {
		return "light_filters_ignored"
	}
	if strings.TrimSpace(filters.UserQuery) != "" || filters.UserID > 0 || strings.TrimSpace(filters.AgeBucket) != "" {
		return "light_filters_ignored"
	}
	return "light"
}

// AdminControlModeracioJobStatus retorna l'estat d'un job de bulk moderació.
func (a *App) AdminControlModeracioJobStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user, perms, isAdmin, ok := a.requireModeracioMassivaUser(w, r)
	if !ok {
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
	if !isAdmin && !a.canModeracioMassiva(user, perms) {
		http.NotFound(w, r)
		return
	}
	if !isAdmin && job.OwnerID != user.ID {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, map[string]interface{}{"ok": true, "job": job})
}
