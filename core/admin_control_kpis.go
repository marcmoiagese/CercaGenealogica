package core

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const adminControlKPIsCacheTTL = 30 * time.Second

type adminControlPendingType struct {
	Type  string `json:"type"`
	Total int    `json:"total"`
}

type adminControlImportsSummary struct {
	Ok    int `json:"ok"`
	Error int `json:"error"`
}

type adminControlKPIsResponse struct {
	PendingModerationTotal  int                        `json:"pending_moderation_total"`
	PendingModerationByType []adminControlPendingType  `json:"pending_moderation_by_type"`
	NewUsers7d              int                        `json:"new_users_7d"`
	NewUsers30d             int                        `json:"new_users_30d"`
	ImportsLast24h          adminControlImportsSummary `json:"imports_last_24h"`
	RebuildJobsRunning      int                        `json:"rebuild_jobs_running"`
	RebuildJobsFailed       int                        `json:"rebuild_jobs_failed"`
	GeneratedAt             string                     `json:"generated_at"`
}

type adminControlKPIsCacheState struct {
	mu       sync.RWMutex
	loaded   bool
	value    adminControlKPIsResponse
	cachedAt time.Time
}

var adminControlKPIsCache adminControlKPIsCacheState

// AdminControlKPIsAPI retorna KPI operatius per al control center.
func (a *App) AdminControlKPIsAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminAnalyticsView, PermissionTarget{}); !ok {
		return
	}
	payload, err := a.adminControlKPIsCached()
	if err != nil {
		http.Error(w, "No s'han pogut carregar els KPI", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(payload)
}

func (a *App) adminControlKPIsCached() (adminControlKPIsResponse, error) {
	now := time.Now()
	adminControlKPIsCache.mu.RLock()
	if adminControlKPIsCache.loaded && now.Sub(adminControlKPIsCache.cachedAt) < adminControlKPIsCacheTTL {
		value := adminControlKPIsCache.value
		adminControlKPIsCache.mu.RUnlock()
		return value, nil
	}
	adminControlKPIsCache.mu.RUnlock()

	adminControlKPIsCache.mu.Lock()
	defer adminControlKPIsCache.mu.Unlock()
	if adminControlKPIsCache.loaded && now.Sub(adminControlKPIsCache.cachedAt) < adminControlKPIsCacheTTL {
		return adminControlKPIsCache.value, nil
	}
	payload, err := a.adminControlKPIs()
	if err != nil {
		return adminControlKPIsResponse{}, err
	}
	adminControlKPIsCache.loaded = true
	adminControlKPIsCache.cachedAt = now
	adminControlKPIsCache.value = payload
	return payload, nil
}

func (a *App) adminControlKPIs() (adminControlKPIsResponse, error) {
	if a == nil || a.DB == nil {
		return adminControlKPIsResponse{}, errors.New("db no disponible")
	}
	totalPending, pendingByType, err := a.adminPendingModerationCounts()
	if err != nil {
		return adminControlKPIsResponse{}, err
	}
	now := time.Now()
	users7d, err := a.DB.CountUsersSince(now.AddDate(0, 0, -7))
	if err != nil {
		return adminControlKPIsResponse{}, err
	}
	users30d, err := a.DB.CountUsersSince(now.AddDate(0, 0, -30))
	if err != nil {
		return adminControlKPIsResponse{}, err
	}
	imports, err := a.DB.CountAdminImportRunsSince(now.Add(-24 * time.Hour))
	if err != nil {
		return adminControlKPIsResponse{}, err
	}
	runningJobs, failedJobs := a.nivellRebuildStore().summary()
	return adminControlKPIsResponse{
		PendingModerationTotal:  totalPending,
		PendingModerationByType: pendingByType,
		NewUsers7d:              users7d,
		NewUsers30d:             users30d,
		ImportsLast24h: adminControlImportsSummary{
			Ok:    imports.Ok,
			Error: imports.Error,
		},
		RebuildJobsRunning: runningJobs,
		RebuildJobsFailed:  failedJobs,
		GeneratedAt:        now.Format(time.RFC3339),
	}, nil
}

func (a *App) adminPendingModerationCounts() (int, []adminControlPendingType, error) {
	counts := map[string]int{}
	if total, err := a.DB.CountPersones(db.PersonaFilter{Estat: "pendent"}); err != nil {
		return 0, nil, err
	} else {
		counts["persona"] = total
	}
	if total, err := a.DB.CountArxius(db.ArxiuFilter{Status: "pendent"}); err != nil {
		return 0, nil, err
	} else {
		counts["arxiu"] = total
	}
	if total, err := a.DB.CountLlibres(db.LlibreFilter{Status: "pendent"}); err != nil {
		return 0, nil, err
	} else {
		counts["llibre"] = total
	}
	if total, err := a.DB.CountNivells(db.NivellAdminFilter{Status: "pendent"}); err != nil {
		return 0, nil, err
	} else {
		counts["nivell"] = total
	}
	if total, err := a.DB.CountMunicipis(db.MunicipiFilter{Status: "pendent"}); err != nil {
		return 0, nil, err
	} else {
		counts["municipi"] = total
	}
	if total, err := a.DB.CountArquebisbats(db.ArquebisbatFilter{Status: "pendent"}); err != nil {
		return 0, nil, err
	} else {
		counts["eclesiastic"] = total
	}
	if total, err := a.DB.CountMunicipiMapaVersionsScoped(db.MunicipiMapaVersionFilter{Status: "pendent"}, db.MunicipiScopeFilter{}); err != nil {
		return 0, nil, err
	} else {
		counts["municipi_mapa_version"] = total
	}
	if total, err := a.DB.CountCognomVariants(db.CognomVariantFilter{Status: "pendent"}); err != nil {
		return 0, nil, err
	} else {
		counts["cognom_variant"] = total
	}
	if total, err := a.DB.CountCognomReferencies(db.CognomReferenciaFilter{Status: "pendent"}); err != nil {
		return 0, nil, err
	} else {
		counts["cognom_referencia"] = total
	}
	if total, err := a.DB.CountCognomRedirectSuggestions(db.CognomRedirectSuggestionFilter{Status: "pendent"}); err != nil {
		return 0, nil, err
	} else {
		counts["cognom_merge"] = total
	}
	if total, err := a.DB.CountEventsHistoric(db.EventHistoricFilter{Status: "pendent"}); err != nil {
		return 0, nil, err
	} else {
		counts["event_historic"] = total
	}
	if _, total, err := a.DB.ListPendingMunicipiHistoriaGeneralVersions(1, 0); err != nil {
		return 0, nil, err
	} else {
		counts["municipi_historia_general"] = total
	}
	if _, total, err := a.DB.ListPendingMunicipiHistoriaFetVersions(1, 0); err != nil {
		return 0, nil, err
	} else {
		counts["municipi_historia_fet"] = total
	}
	if _, total, err := a.DB.ListPendingMunicipiAnecdotariVersions(1, 0); err != nil {
		return 0, nil, err
	} else {
		counts["municipi_anecdota_version"] = total
	}
	if total, err := a.DB.CountTranscripcionsRawGlobal(db.TranscripcioFilter{Status: "pendent"}); err != nil {
		return 0, nil, err
	} else {
		counts["registre"] = total
	}
	if total, err := a.DB.CountTranscripcioRawChangesPending(); err != nil {
		return 0, nil, err
	} else {
		counts["registre_canvi"] = total
	}
	if total, err := a.DB.CountMediaAlbumsByStatus("pending"); err != nil {
		return 0, nil, err
	} else {
		counts["media_album"] = total
	}
	if total, err := a.DB.CountMediaItemsByStatus("pending"); err != nil {
		return 0, nil, err
	} else {
		counts["media_item"] = total
	}
	if total, err := a.DB.CountExternalLinksByStatus("pending"); err != nil {
		return 0, nil, err
	} else {
		counts["external_link"] = total
	}

	if wikiCounts, err := a.DB.CountWikiPendingChangesByType(); err != nil {
		return 0, nil, err
	} else if len(wikiCounts) > 0 {
		for objType, total := range wikiCounts {
			moderacioType := moderacioWikiTypeMap[strings.ToLower(strings.TrimSpace(objType))]
			if moderacioType == "" {
				continue
			}
			counts[moderacioType] += total
		}
	}

	order := []string{
		"persona",
		"arxiu",
		"llibre",
		"nivell",
		"municipi",
		"eclesiastic",
		"municipi_mapa_version",
		"cognom_variant",
		"cognom_referencia",
		"cognom_merge",
		"event_historic",
		"municipi_historia_general",
		"municipi_historia_fet",
		"municipi_anecdota_version",
		"registre",
		"registre_canvi",
		"media_album",
		"media_item",
		"external_link",
		"municipi_canvi",
		"arxiu_canvi",
		"llibre_canvi",
		"persona_canvi",
		"cognom_canvi",
		"event_historic_canvi",
	}
	byType := make([]adminControlPendingType, 0, len(order))
	total := 0
	seen := map[string]bool{}
	for _, key := range order {
		count := counts[key]
		seen[key] = true
		if count > 0 {
			byType = append(byType, adminControlPendingType{Type: key, Total: count})
		}
		total += count
	}
	for key, count := range counts {
		if seen[key] || count <= 0 {
			continue
		}
		byType = append(byType, adminControlPendingType{Type: key, Total: count})
		total += count
	}
	return total, byType, nil
}
