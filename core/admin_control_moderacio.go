package core

import (
	"encoding/json"
	"fmt"
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
	summary, mode, scopeMode, cacheHit, err := a.adminControlModeracioSummaryCached(filters, user, perms, canModerateAll)
	if err != nil {
		http.Error(w, "No s'ha pogut carregar el resum", http.StatusInternalServerError)
		return
	}
	summaryTypes := "all"
	if scopeMode == "scoped" {
		summaryTypes = moderacioSummaryTypesLabel(summary.ByType)
	}
	Infof("Moderacio summary mode=%s scope=%s types=%s user=%d status=%s type=%s age=%s dur=%s", mode, scopeMode, summaryTypes, user.ID, strings.TrimSpace(filters.Status), strings.TrimSpace(filters.Type), strings.TrimSpace(filters.AgeBucket), time.Since(start))
	payload := map[string]interface{}{
		"ok":            true,
		"summary":       summary,
		"summary_mode":  mode,
		"summary_scope": scopeMode,
		"generated_at":  time.Now().Format(time.RFC3339),
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(payload)
	if IsDebugEnabled() {
		Debugf("moderacio summary user=%d scope=%s cache=%t total=%d by_type=%s dur=%s", user.ID, scopeMode, cacheHit, summary.Total, moderacioSummaryCountsLabel(summary.ByType), time.Since(start))
	}
}

func (a *App) adminControlModeracioSummaryCached(filters moderacioFilters, user *db.User, perms db.PolicyPermissions, canModerateAll bool) (moderacioSummary, string, string, bool, error) {
	total, byType, scopeMode, cacheHit, err := a.adminControlModeracioPendingCountsCached(user, perms, canModerateAll)
	if err != nil {
		return moderacioSummary{}, "", "", false, err
	}
	return buildModeracioSummaryFromCounts(filters, total, byType), moderacioSummaryMode(filters), scopeMode, cacheHit, nil
}

func (a *App) adminControlModeracioPendingCountsCached(user *db.User, perms db.PolicyPermissions, canModerateAll bool) (int, []adminControlPendingType, string, bool, error) {
	if !canModerateAll {
		total, byType, err := a.adminPendingModerationCountsForUser(user, perms, canModerateAll)
		return total, byType, "scoped", false, err
	}
	now := time.Now()
	adminControlModeracioSummaryCache.mu.RLock()
	if adminControlModeracioSummaryCache.loaded && now.Sub(adminControlModeracioSummaryCache.cachedAt) < adminControlModeracioSummaryCacheTTL {
		total := adminControlModeracioSummaryCache.total
		byType := adminControlModeracioSummaryCache.byType
		adminControlModeracioSummaryCache.mu.RUnlock()
		return total, byType, "global", true, nil
	}
	adminControlModeracioSummaryCache.mu.RUnlock()

	adminControlModeracioSummaryCache.mu.Lock()
	defer adminControlModeracioSummaryCache.mu.Unlock()
	if adminControlModeracioSummaryCache.loaded && now.Sub(adminControlModeracioSummaryCache.cachedAt) < adminControlModeracioSummaryCacheTTL {
		return adminControlModeracioSummaryCache.total, adminControlModeracioSummaryCache.byType, "global", true, nil
	}
	total, byType, err := a.adminPendingModerationCounts()
	if err != nil {
		return 0, nil, "", false, err
	}
	adminControlModeracioSummaryCache.loaded = true
	adminControlModeracioSummaryCache.cachedAt = now
	adminControlModeracioSummaryCache.total = total
	adminControlModeracioSummaryCache.byType = byType
	return total, byType, "global", false, nil
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

func moderacioSummaryTypesLabel(byType []moderacioTypeCount) string {
	if len(byType) == 0 {
		return "none"
	}
	types := make([]string, 0, len(byType))
	for _, item := range byType {
		if strings.TrimSpace(item.Type) == "" {
			continue
		}
		types = append(types, item.Type)
	}
	if len(types) == 0 {
		return "none"
	}
	return strings.Join(types, ",")
}

func moderacioSummaryCountsLabel(byType []moderacioTypeCount) string {
	if len(byType) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(byType))
	for _, item := range byType {
		if strings.TrimSpace(item.Type) == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s:%d", item.Type, item.Total))
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ",")
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
	scopeModel := a.newModeracioScopeModel(user, perms, canModerateAll)
	counts := map[string]int{}
	if scopeModel.canModerateType("persona") {
		if total, err := a.DB.CountPersones(db.PersonaFilter{Estat: "pendent"}); err != nil {
			return 0, nil, err
		} else if total > 0 {
			counts["persona"] = total
		}
	}
	if scopeModel.canModerateType("arxiu") {
		filter := db.ArxiuFilter{Status: "pendent"}
		if scope, ok := scopeModel.scopeFilterForType("arxiu"); ok && !scope.hasGlobal {
			applyScopeFilterToArxiu(&filter, scope)
		}
		if total, err := a.DB.CountArxius(filter); err != nil {
			return 0, nil, err
		} else if total > 0 {
			counts["arxiu"] = total
		}
	}
	if scopeModel.canModerateType("llibre") {
		filter := db.LlibreFilter{Status: "pendent"}
		if scope, ok := scopeModel.scopeFilterForType("llibre"); ok && !scope.hasGlobal {
			applyScopeFilterToLlibre(&filter, scope)
		}
		if total, err := a.DB.CountLlibres(filter); err != nil {
			return 0, nil, err
		} else if total > 0 {
			counts["llibre"] = total
		}
	}
	if scopeModel.canModerateType("nivell") {
		filter := db.NivellAdminFilter{Status: "pendent"}
		if scope, ok := scopeModel.scopeFilterForType("nivell"); ok && !scope.hasGlobal {
			applyScopeFilterToNivell(&filter, scope)
		}
		if total, err := a.DB.CountNivells(filter); err != nil {
			return 0, nil, err
		} else if total > 0 {
			counts["nivell"] = total
		}
	}
	if scopeModel.canModerateType("municipi") {
		filter := db.MunicipiFilter{Status: "pendent"}
		if scope, ok := scopeModel.scopeFilterForType("municipi"); ok && !scope.hasGlobal {
			applyScopeFilterToMunicipi(&filter, scope)
		}
		if total, err := a.DB.CountMunicipis(filter); err != nil {
			return 0, nil, err
		} else if total > 0 {
			counts["municipi"] = total
		}
	}
	if scopeModel.canModerateType("eclesiastic") {
		filter := db.ArquebisbatFilter{Status: "pendent"}
		if scope, ok := scopeModel.scopeFilterForType("eclesiastic"); ok && !scope.hasGlobal {
			applyScopeFilterToEcles(&filter, scope)
		}
		if total, err := a.DB.CountArquebisbats(filter); err != nil {
			return 0, nil, err
		} else if total > 0 {
			counts["eclesiastic"] = total
		}
	}
	if scopeModel.canModerateType("municipi_mapa_version") {
		scope, ok := scopeModel.scopeFilterForType("municipi_mapa_version")
		if ok && (scope.hasGlobal || !scope.isEmpty()) {
			filter := db.MunicipiMapaVersionFilter{Status: "pendent"}
			scopeFilter := db.MunicipiScopeFilter{}
			if !scope.hasGlobal {
				scopeFilter = db.MunicipiScopeFilter{
					AllowedMunicipiIDs:  scope.municipiIDs,
					AllowedProvinciaIDs: scope.provinciaIDs,
					AllowedComarcaIDs:   scope.comarcaIDs,
					AllowedNivellIDs:    scope.nivellIDs,
					AllowedPaisIDs:      scope.paisIDs,
				}
			}
			total, err := a.DB.CountMunicipiMapaVersionsScoped(filter, scopeFilter)
			if err != nil {
				return 0, nil, err
			}
			if total > 0 {
				counts["municipi_mapa_version"] = total
			}
		}
	}
	if scopeModel.canModerateType("cognom_variant") {
		if total, err := a.DB.CountCognomVariants(db.CognomVariantFilter{Status: "pendent"}); err != nil {
			return 0, nil, err
		} else if total > 0 {
			counts["cognom_variant"] = total
		}
	}
	if scopeModel.canModerateType("cognom_referencia") {
		if total, err := a.DB.CountCognomReferencies(db.CognomReferenciaFilter{Status: "pendent"}); err != nil {
			return 0, nil, err
		} else if total > 0 {
			counts["cognom_referencia"] = total
		}
	}
	if scopeModel.canModerateType("cognom_merge") {
		if total, err := a.DB.CountCognomRedirectSuggestions(db.CognomRedirectSuggestionFilter{Status: "pendent"}); err != nil {
			return 0, nil, err
		} else if total > 0 {
			counts["cognom_merge"] = total
		}
	}
	if scopeModel.canModerateType("event_historic") {
		if total, err := a.DB.CountEventsHistoric(db.EventHistoricFilter{Status: "pendent"}); err != nil {
			return 0, nil, err
		} else if total > 0 {
			counts["event_historic"] = total
		}
	}
	if scopeModel.canModerateType("municipi_historia_general") {
		scope, ok := scopeModel.scopeFilterForType("municipi_historia_general")
		if ok && (scope.hasGlobal || !scope.isEmpty()) {
			filter := db.MunicipiScopeFilter{
				AllowedMunicipiIDs:  scope.municipiIDs,
				AllowedProvinciaIDs: scope.provinciaIDs,
				AllowedComarcaIDs:   scope.comarcaIDs,
				AllowedNivellIDs:    scope.nivellIDs,
				AllowedPaisIDs:      scope.paisIDs,
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
	if scopeModel.canModerateType("municipi_anecdota_version") {
		scope, ok := scopeModel.scopeFilterForType("municipi_anecdota_version")
		if ok && (scope.hasGlobal || !scope.isEmpty()) {
			filter := db.MunicipiScopeFilter{
				AllowedMunicipiIDs:  scope.municipiIDs,
				AllowedProvinciaIDs: scope.provinciaIDs,
				AllowedComarcaIDs:   scope.comarcaIDs,
				AllowedNivellIDs:    scope.nivellIDs,
				AllowedPaisIDs:      scope.paisIDs,
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
	if scopeModel.canModerateType("registre") {
		filter := db.TranscripcioFilter{Status: "pendent"}
		if scope, ok := scopeModel.scopeFilterForType("registre"); ok && !scope.hasGlobal {
			applyScopeFilterToRegistre(&filter, scope)
		}
		if total, err := a.DB.CountTranscripcionsRawGlobal(filter); err != nil {
			return 0, nil, err
		} else if total > 0 {
			counts["registre"] = total
		}
	}
	if scopeModel.canModerateType("registre_canvi") {
		if scopeModel.canModerateAll {
			if total, err := a.DB.CountTranscripcioRawChangesPending(); err != nil {
				return 0, nil, err
			} else if total > 0 {
				counts["registre_canvi"] = total
			}
		} else if scope, ok := scopeModel.scopeFilterForType("registre_canvi"); ok && !scope.isEmpty() {
			filter := db.TranscripcioFilter{}
			applyScopeFilterToRegistre(&filter, scope)
			if total, err := a.DB.CountTranscripcioRawChangesPendingScoped(filter); err != nil {
				return 0, nil, err
			} else if total > 0 {
				counts["registre_canvi"] = total
			}
		}
	}
	if scopeModel.canModerateType("external_link") {
		if total, err := a.DB.CountExternalLinksByStatus("pending"); err != nil {
			return 0, nil, err
		} else if total > 0 {
			counts["external_link"] = total
		}
	}
	if scopeModel.canModerateType("media_album") {
		if total, err := a.DB.CountMediaAlbumsByStatus("pending"); err != nil {
			return 0, nil, err
		} else if total > 0 {
			counts["media_album"] = total
		}
	}
	if scopeModel.canModerateType("media_item") {
		if total, err := a.DB.CountMediaItemsByStatus("pending"); err != nil {
			return 0, nil, err
		} else if total > 0 {
			counts["media_item"] = total
		}
	}
	needsWiki := scopeModel.canModerateType("municipi_canvi") ||
		scopeModel.canModerateType("arxiu_canvi") ||
		scopeModel.canModerateType("llibre_canvi") ||
		scopeModel.canModerateType("persona_canvi") ||
		scopeModel.canModerateType("cognom_canvi") ||
		scopeModel.canModerateType("event_historic_canvi")
	if needsWiki {
		wikiCounts, err := a.DB.CountWikiPendingChangesByType()
		if err != nil {
			return 0, nil, err
		}
		getWikiCount := func(objType string) int {
			key := strings.ToLower(strings.TrimSpace(objType))
			if key == "" {
				return 0
			}
			return wikiCounts[key]
		}
		if scopeModel.canModerateType("persona_canvi") {
			if total := getWikiCount("persona"); total > 0 {
				counts["persona_canvi"] = total
			}
		}
		if scopeModel.canModerateType("cognom_canvi") {
			if total := getWikiCount("cognom"); total > 0 {
				counts["cognom_canvi"] = total
			}
		}
		if scopeModel.canModerateType("event_historic_canvi") {
			if total := getWikiCount("event_historic"); total > 0 {
				counts["event_historic_canvi"] = total
			}
		}
		if scopeModel.canModerateType("municipi_canvi") {
			if scope, ok := scopeModel.scopeFilterForType("municipi_canvi"); ok {
				if scope.hasGlobal {
					if total := getWikiCount("municipi"); total > 0 {
						counts["municipi_canvi"] = total
					}
				} else if !scope.isEmpty() {
					scopeFilter := db.MunicipiScopeFilter{
						AllowedMunicipiIDs:  scope.municipiIDs,
						AllowedProvinciaIDs: scope.provinciaIDs,
						AllowedComarcaIDs:   scope.comarcaIDs,
						AllowedNivellIDs:    scope.nivellIDs,
						AllowedPaisIDs:      scope.paisIDs,
					}
					total, err := a.DB.CountWikiPendingMunicipiChangesScoped(scopeFilter)
					if err != nil {
						return 0, nil, err
					}
					if total > 0 {
						counts["municipi_canvi"] = total
					}
				}
			}
		}
		if scopeModel.canModerateType("arxiu_canvi") {
			if scope, ok := scopeModel.scopeFilterForType("arxiu_canvi"); ok {
				if scope.hasGlobal {
					if total := getWikiCount("arxiu"); total > 0 {
						counts["arxiu_canvi"] = total
					}
				} else if !scope.isEmpty() {
					filter := db.ArxiuFilter{}
					applyScopeFilterToArxiu(&filter, scope)
					total, err := a.DB.CountWikiPendingArxiuChangesScoped(filter)
					if err != nil {
						return 0, nil, err
					}
					if total > 0 {
						counts["arxiu_canvi"] = total
					}
				}
			}
		}
		if scopeModel.canModerateType("llibre_canvi") {
			if scope, ok := scopeModel.scopeFilterForType("llibre_canvi"); ok {
				if scope.hasGlobal {
					if total := getWikiCount("llibre"); total > 0 {
						counts["llibre_canvi"] = total
					}
				} else if !scope.isEmpty() {
					filter := db.LlibreFilter{}
					applyScopeFilterToLlibre(&filter, scope)
					total, err := a.DB.CountWikiPendingLlibreChangesScoped(filter)
					if err != nil {
						return 0, nil, err
					}
					if total > 0 {
						counts["llibre_canvi"] = total
					}
				}
			}
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
	if IsDebugEnabled() {
		Debugf("moderacio job status user=%d job=%s done=%t processed=%d total=%d err=%t age=%s", user.ID, job.ID, job.Done, job.Processed, job.Total, job.Error != "", time.Since(job.StartedAt))
	}
}
