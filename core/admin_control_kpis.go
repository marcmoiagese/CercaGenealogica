package core

import (
	"encoding/json"
	"errors"
	"net/http"
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
	PendingModerationTotal  int                       `json:"pending_moderation_total"`
	PendingModerationByType []adminControlPendingType `json:"pending_moderation_by_type"`
	NewUsers7d              int                       `json:"new_users_7d"`
	NewUsers30d             int                       `json:"new_users_30d"`
	ImportsLast24h          adminControlImportsSummary `json:"imports_last_24h"`
	RebuildJobsRunning      int                       `json:"rebuild_jobs_running"`
	RebuildJobsFailed       int                       `json:"rebuild_jobs_failed"`
	GeneratedAt             string                    `json:"generated_at"`
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
	if rows, err := a.DB.ListPersones(db.PersonaFilter{Estat: "pendent"}); err != nil {
		return 0, nil, err
	} else {
		counts["persona"] = len(rows)
	}
	if rows, err := a.DB.ListArxius(db.ArxiuFilter{Status: "pendent"}); err != nil {
		return 0, nil, err
	} else {
		counts["arxiu"] = len(rows)
	}
	if rows, err := a.DB.ListLlibres(db.LlibreFilter{Status: "pendent"}); err != nil {
		return 0, nil, err
	} else {
		counts["llibre"] = len(rows)
	}
	if rows, err := a.DB.ListNivells(db.NivellAdminFilter{Status: "pendent"}); err != nil {
		return 0, nil, err
	} else {
		counts["nivell"] = len(rows)
	}
	if rows, err := a.DB.ListMunicipis(db.MunicipiFilter{Status: "pendent"}); err != nil {
		return 0, nil, err
	} else {
		counts["municipi"] = len(rows)
	}
	if rows, err := a.DB.ListArquebisbats(db.ArquebisbatFilter{Status: "pendent"}); err != nil {
		return 0, nil, err
	} else {
		counts["eclesiastic"] = len(rows)
	}
	if rows, err := a.DB.ListCognomVariants(db.CognomVariantFilter{Status: "pendent"}); err != nil {
		return 0, nil, err
	} else {
		counts["cognom_variant"] = len(rows)
	}
	if rows, err := a.DB.ListCognomReferencies(db.CognomReferenciaFilter{Status: "pendent"}); err != nil {
		return 0, nil, err
	} else {
		counts["cognom_referencia"] = len(rows)
	}
	if rows, err := a.DB.ListCognomRedirectSuggestions(db.CognomRedirectSuggestionFilter{Status: "pendent"}); err != nil {
		return 0, nil, err
	} else {
		counts["cognom_merge"] = len(rows)
	}
	if rows, err := a.DB.ListEventsHistoric(db.EventHistoricFilter{Status: "pendent"}); err != nil {
		return 0, nil, err
	} else {
		counts["event_historic"] = len(rows)
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
	if rows, err := a.DB.ListTranscripcioRawChangesPending(); err != nil {
		return 0, nil, err
	} else {
		counts["registre_canvi"] = len(rows)
	}

	if items, err := a.DB.ListWikiPending(0); err != nil {
		return 0, nil, err
	} else if len(items) > 0 {
		typeMap := map[string]string{
			"municipi":       "municipi_canvi",
			"arxiu":          "arxiu_canvi",
			"llibre":         "llibre_canvi",
			"persona":        "persona_canvi",
			"cognom":         "cognom_canvi",
			"event_historic": "event_historic_canvi",
		}
		for _, item := range items {
			change, err := a.DB.GetWikiChange(item.ChangeID)
			if err != nil || change == nil {
				continue
			}
			if change.ModeracioEstat != "pendent" {
				_ = a.DB.DequeueWikiPending(change.ID)
				continue
			}
			objType := typeMap[change.ObjectType]
			if objType == "" {
				objType = "wiki_canvi"
			}
			counts[objType]++
		}
	}

	order := []string{
		"persona",
		"arxiu",
		"llibre",
		"nivell",
		"municipi",
		"eclesiastic",
		"cognom_variant",
		"cognom_referencia",
		"cognom_merge",
		"event_historic",
		"municipi_historia_general",
		"municipi_historia_fet",
		"municipi_anecdota_version",
		"registre",
		"registre_canvi",
		"municipi_canvi",
		"arxiu_canvi",
		"llibre_canvi",
		"persona_canvi",
		"cognom_canvi",
		"event_historic_canvi",
		"wiki_canvi",
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
