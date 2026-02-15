package core

import (
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"sync"
	"time"
)

const adminKPIsCacheTTL = 2 * time.Minute

type adminKPIsGeneralResponse struct {
	TotalUsers             int     `json:"total_users"`
	ActiveUsers            int     `json:"active_users"`
	ContributorsUsers      int     `json:"contributors_users"`
	ContributorsPct        float64 `json:"contributors_pct"`
	ValidatedContributions int     `json:"validated_contributions"`
}

type adminKPIsCacheState struct {
	mu       sync.RWMutex
	loaded   bool
	value    adminKPIsGeneralResponse
	cachedAt time.Time
}

var adminKPIsCache adminKPIsCacheState

// AdminKPIsPage mostra els KPI globals de governanca.
func (a *App) AdminKPIsPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user, ok := a.requirePermissionKey(w, r, permKeyAdminAnalyticsView, PermissionTarget{})
	if !ok {
		return
	}
	RenderPrivateTemplate(w, r, "admin-kpis.html", map[string]interface{}{
		"User": user,
	})
}

// AdminKPIsGeneralAPI retorna KPI globals en JSON.
func (a *App) AdminKPIsGeneralAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminAnalyticsView, PermissionTarget{}); !ok {
		return
	}
	payload, err := a.adminKPIsGeneralCached()
	if err != nil {
		http.Error(w, "No s'han pogut carregar els KPI", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(payload)
}

func (a *App) adminKPIsGeneralCached() (adminKPIsGeneralResponse, error) {
	now := time.Now()
	adminKPIsCache.mu.RLock()
	if adminKPIsCache.loaded && now.Sub(adminKPIsCache.cachedAt) < adminKPIsCacheTTL {
		value := adminKPIsCache.value
		adminKPIsCache.mu.RUnlock()
		return value, nil
	}
	adminKPIsCache.mu.RUnlock()

	adminKPIsCache.mu.Lock()
	defer adminKPIsCache.mu.Unlock()
	if adminKPIsCache.loaded && now.Sub(adminKPIsCache.cachedAt) < adminKPIsCacheTTL {
		return adminKPIsCache.value, nil
	}
	if a == nil || a.DB == nil {
		return adminKPIsGeneralResponse{}, errors.New("db no disponible")
	}
	stats, err := a.DB.GetAdminKPIsGeneral()
	if err != nil {
		return adminKPIsGeneralResponse{}, err
	}
	pct := 0.0
	if stats.ActiveUsers > 0 {
		pct = float64(stats.ContributorUsers) / float64(stats.ActiveUsers) * 100
	}
	pct = math.Round(pct*10) / 10
	value := adminKPIsGeneralResponse{
		TotalUsers:             stats.TotalUsers,
		ActiveUsers:            stats.ActiveUsers,
		ContributorsUsers:      stats.ContributorUsers,
		ContributorsPct:        pct,
		ValidatedContributions: stats.ValidatedContributions,
	}
	adminKPIsCache.loaded = true
	adminKPIsCache.cachedAt = now
	adminKPIsCache.value = value
	return value, nil
}
