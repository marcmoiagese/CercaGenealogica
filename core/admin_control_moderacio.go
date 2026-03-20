package core

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
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
	user, perms, canModerateAll, ok := a.requireModeracioUser(w, r)
	if !ok {
		return
	}
	filters, _ := parseModeracioFilters(r)
	start := time.Now()
	summary, mode, scopeMode, err := a.adminControlModeracioSummaryCached(filters, user, perms, canModerateAll)
	if err != nil {
		http.Error(w, "No s'ha pogut carregar el resum", http.StatusInternalServerError)
		return
	}
	summaryTypes := "all"
	if scopeMode == "scoped" {
		summaryTypes = "municipi_historia_general,municipi_historia_fet,municipi_anecdota_version"
	}
	Infof("Moderacio summary mode=%s scope=%s types=%s user=%d status=%s type=%s age=%s dur=%s", mode, scopeMode, summaryTypes, user.ID, strings.TrimSpace(filters.Status), strings.TrimSpace(filters.Type), strings.TrimSpace(filters.AgeBucket), time.Since(start))
	payload := map[string]interface{}{
		"ok":           true,
		"summary":      summary,
		"summary_mode": mode,
		"summary_scope": scopeMode,
		"generated_at": time.Now().Format(time.RFC3339),
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(payload)
}

func (a *App) adminControlModeracioSummaryCached(filters moderacioFilters, user *db.User, perms db.PolicyPermissions, canModerateAll bool) (moderacioSummary, string, string, error) {
	total, byType, scopeMode, err := a.adminControlModeracioPendingCountsCached(user, perms, canModerateAll)
	if err != nil {
		return moderacioSummary{}, "", "", err
	}
	return buildModeracioSummaryFromCounts(filters, total, byType), moderacioSummaryMode(filters), scopeMode, nil
}

func (a *App) adminControlModeracioPendingCountsCached(user *db.User, perms db.PolicyPermissions, canModerateAll bool) (int, []adminControlPendingType, string, error) {
	if !canModerateAll {
		total, byType, err := a.adminPendingModerationCountsForUser(user, perms, canModerateAll)
		return total, byType, "scoped", err
	}
	now := time.Now()
	adminControlModeracioSummaryCache.mu.RLock()
	if adminControlModeracioSummaryCache.loaded && now.Sub(adminControlModeracioSummaryCache.cachedAt) < adminControlModeracioSummaryCacheTTL {
		total := adminControlModeracioSummaryCache.total
		byType := adminControlModeracioSummaryCache.byType
		adminControlModeracioSummaryCache.mu.RUnlock()
		return total, byType, "global", nil
	}
	adminControlModeracioSummaryCache.mu.RUnlock()

	adminControlModeracioSummaryCache.mu.Lock()
	defer adminControlModeracioSummaryCache.mu.Unlock()
	if adminControlModeracioSummaryCache.loaded && now.Sub(adminControlModeracioSummaryCache.cachedAt) < adminControlModeracioSummaryCacheTTL {
		return adminControlModeracioSummaryCache.total, adminControlModeracioSummaryCache.byType, "global", nil
	}
	total, byType, err := a.adminPendingModerationCounts()
	if err != nil {
		return 0, nil, "", err
	}
	adminControlModeracioSummaryCache.loaded = true
	adminControlModeracioSummaryCache.cachedAt = now
	adminControlModeracioSummaryCache.total = total
	adminControlModeracioSummaryCache.byType = byType
	return total, byType, "global", nil
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

func (a *App) adminPendingModerationCountsForUser(user *db.User, perms db.PolicyPermissions, canModerateAll bool) (int, []adminControlPendingType, error) {
	if canModerateAll {
		return a.adminPendingModerationCounts()
	}
	scope := a.moderacioScopeForUser(user, canModerateAll)
	counts := map[string]int{}
	// El resum scoped només és segur per historial/anecdotari de municipis.
	if scope.CanModerateHistoria {
		scopeFilter := a.buildListScopeFilter(user.ID, permKeyTerritoriMunicipisHistoriaModerate, ScopeMunicipi)
		if scopeFilter.hasGlobal || !scopeFilter.isEmpty() {
			filter := db.MunicipiScopeFilter{
				AllowedMunicipiIDs:  scopeFilter.municipiIDs,
				AllowedProvinciaIDs: scopeFilter.provinciaIDs,
				AllowedComarcaIDs:   scopeFilter.comarcaIDs,
				AllowedNivellIDs:    scopeFilter.nivellIDs,
				AllowedPaisIDs:      scopeFilter.paisIDs,
			}
			total, err := a.DB.CountPendingMunicipiHistoriaGeneralVersionsScoped(filter)
			if err != nil {
				return 0, nil, err
			}
			if total > 0 {
				counts["municipi_historia_general"] = total
			}
			total, err = a.DB.CountPendingMunicipiHistoriaFetVersionsScoped(filter)
			if err != nil {
				return 0, nil, err
			}
			if total > 0 {
				counts["municipi_historia_fet"] = total
			}
		}
	}
	if scope.CanModerateAnecdotes {
		scopeFilter := a.buildListScopeFilter(user.ID, permKeyTerritoriMunicipisAnecdotesModerate, ScopeMunicipi)
		if scopeFilter.hasGlobal || !scopeFilter.isEmpty() {
			filter := db.MunicipiScopeFilter{
				AllowedMunicipiIDs:  scopeFilter.municipiIDs,
				AllowedProvinciaIDs: scopeFilter.provinciaIDs,
				AllowedComarcaIDs:   scopeFilter.comarcaIDs,
				AllowedNivellIDs:    scopeFilter.nivellIDs,
				AllowedPaisIDs:      scopeFilter.paisIDs,
			}
			total, err := a.DB.CountPendingMunicipiAnecdotariVersionsScoped(filter)
			if err != nil {
				return 0, nil, err
			}
			if total > 0 {
				counts["municipi_anecdota_version"] = total
			}
		}
	}
	order := []string{
		"municipi_historia_general",
		"municipi_historia_fet",
		"municipi_anecdota_version",
	}
	byType := make([]adminControlPendingType, 0, len(order))
	total := 0
	for _, key := range order {
		count := counts[key]
		if count <= 0 {
			continue
		}
		byType = append(byType, adminControlPendingType{Type: key, Total: count})
		total += count
	}
	return total, byType, nil
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
