package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const (
	adminJobPhaseQueued            = "queued"
	adminJobPhaseResolvingTargets  = "resolving_targets"
	adminJobPhaseApplyingChanges   = "applying_changes"
	adminJobPhaseRecordingHistory  = "recording_history"
	adminJobPhaseDone              = "done"
	adminJobPhaseError             = "error"
	moderacioBulkProgressFinalStep = 1
)

type moderacioBulkJobPayload struct {
	Action     string `json:"action"`
	Scope      string `json:"scope"`
	BulkType   string `json:"bulk_type"`
	BulkUserID int    `json:"bulk_user_id,omitempty"`
	Reason     string `json:"reason,omitempty"`
	Source     string `json:"source"`
}

type moderacioBulkJobResult struct {
	Action       string                            `json:"action"`
	Scope        string                            `json:"scope"`
	BulkType     string                            `json:"bulk_type"`
	BulkUserID   int                               `json:"bulk_user_id,omitempty"`
	Phase        string                            `json:"phase"`
	ScopeMode    string                            `json:"scope_mode"`
	Candidates   int                               `json:"candidates"`
	Targets      int                               `json:"targets"`
	Processed    int                               `json:"processed"`
	Updated      int                               `json:"updated"`
	Skipped      int                               `json:"skipped"`
	Errors       int                               `json:"errors"`
	ByType       []moderacioTypeCount              `json:"by_type"`
	ResolveMs    int64                             `json:"resolve_ms"`
	UpdateMs     int64                             `json:"update_ms"`
	ActivityMs   int64                             `json:"activity_ms"`
	TotalMs      int64                             `json:"total_ms"`
	ActivityMode string                            `json:"activity_mode"`
	ErrorPhases  []moderacioBulkJobErrorPhaseCount `json:"error_phases,omitempty"`
	ErrorSamples []moderacioBulkJobErrorSample     `json:"error_samples,omitempty"`
}

type moderacioBulkJobErrorPhaseCount struct {
	Phase string `json:"phase"`
	Count int    `json:"count"`
}

type moderacioBulkJobErrorSample struct {
	Phase      string `json:"phase"`
	Step       string `json:"step"`
	ObjectType string `json:"object_type,omitempty"`
	ObjectID   int    `json:"object_id,omitempty"`
	Message    string `json:"message"`
}

type moderacioBulkSnapshot struct {
	Targets    []db.AdminJobTarget
	Candidates int
	ScopeMode  string
	ByType     []moderacioTypeCount
	ResolveDur time.Duration
}

var moderacioBulkSimpleTypes = map[string]bool{
	"arxiu":             true,
	"llibre":            true,
	"nivell":            true,
	"municipi":          true,
	"eclesiastic":       true,
	"cognom_variant":    true,
	"cognom_referencia": true,
	"event_historic":    true,
}

const moderacioBulkErrorSampleLimit = 8

type moderacioBulkErrorCollector struct {
	phaseCounts map[string]int
	samples     []moderacioBulkJobErrorSample
}

func newModeracioBulkErrorCollector() *moderacioBulkErrorCollector {
	return &moderacioBulkErrorCollector{
		phaseCounts: map[string]int{},
		samples:     make([]moderacioBulkJobErrorSample, 0, moderacioBulkErrorSampleLimit),
	}
}

func (c *moderacioBulkErrorCollector) add(phase, step, objectType string, objectID int, err error) bool {
	if c == nil || err == nil {
		return false
	}
	phase = strings.TrimSpace(phase)
	if phase == "" {
		phase = adminJobPhaseError
	}
	c.phaseCounts[phase]++
	if len(c.samples) >= moderacioBulkErrorSampleLimit {
		return false
	}
	c.samples = append(c.samples, moderacioBulkJobErrorSample{
		Phase:      phase,
		Step:       strings.TrimSpace(step),
		ObjectType: strings.TrimSpace(objectType),
		ObjectID:   objectID,
		Message:    strings.TrimSpace(err.Error()),
	})
	return true
}

func (c *moderacioBulkErrorCollector) phaseCountsSlice() []moderacioBulkJobErrorPhaseCount {
	if c == nil || len(c.phaseCounts) == 0 {
		return []moderacioBulkJobErrorPhaseCount{}
	}
	order := []string{adminJobPhaseResolvingTargets, adminJobPhaseApplyingChanges, adminJobPhaseRecordingHistory, adminJobPhaseError}
	seen := map[string]bool{}
	out := make([]moderacioBulkJobErrorPhaseCount, 0, len(c.phaseCounts))
	for _, phase := range order {
		if count := c.phaseCounts[phase]; count > 0 {
			out = append(out, moderacioBulkJobErrorPhaseCount{Phase: phase, Count: count})
			seen[phase] = true
		}
	}
	for phase, count := range c.phaseCounts {
		if count <= 0 || seen[phase] {
			continue
		}
		out = append(out, moderacioBulkJobErrorPhaseCount{Phase: phase, Count: count})
	}
	return out
}

func (c *moderacioBulkErrorCollector) samplesSlice() []moderacioBulkJobErrorSample {
	if c == nil || len(c.samples) == 0 {
		return []moderacioBulkJobErrorSample{}
	}
	out := make([]moderacioBulkJobErrorSample, len(c.samples))
	copy(out, c.samples)
	return out
}

func recordModeracioBulkWorkerError(jobID, actorID int, collector *moderacioBulkErrorCollector, result *moderacioBulkJobResult, phase, step, objectType string, objectID int, err error) int {
	if err == nil {
		return 0
	}
	if collector.add(phase, step, objectType, objectID, err) {
		Errorf("moderacio bulk worker issue job=%d actor=%d phase=%s step=%s type=%s object_id=%d err=%v", jobID, actorID, phase, step, objectType, objectID, err)
	}
	if result != nil {
		result.ErrorPhases = collector.phaseCountsSlice()
		result.ErrorSamples = collector.samplesSlice()
	}
	return 1
}

func moderacioBulkErrorText(result moderacioBulkJobResult) string {
	if result.Errors <= 0 {
		return ""
	}
	base := fmt.Sprintf("moderacio bulk completada amb incidencies: updated=%d skipped=%d errors=%d", result.Updated, result.Skipped, result.Errors)
	if len(result.ErrorSamples) == 0 {
		return base
	}
	sample := result.ErrorSamples[0]
	parts := []string{strings.TrimSpace(sample.Phase), strings.TrimSpace(sample.Step)}
	if strings.TrimSpace(sample.ObjectType) != "" {
		if sample.ObjectID > 0 {
			parts = append(parts, fmt.Sprintf("%s:%d", sample.ObjectType, sample.ObjectID))
		} else {
			parts = append(parts, sample.ObjectType)
		}
	}
	context := strings.Join(filterEmptyStrings(parts), "/")
	if context == "" {
		return base + "; primer error: " + sample.Message
	}
	return base + "; primer error " + context + ": " + sample.Message
}

func (a *App) startModeracioBulkAdminJob(action, bulkType, motiu string, user *db.User, perms db.PolicyPermissions, bulkUserID int) (int, error) {
	if a == nil || a.DB == nil || user == nil {
		return 0, fmt.Errorf("context bulk invàlid")
	}
	if IsDebugEnabled() {
		Debugf("moderacio bulk job create start actor=%d action=%s scope=all type=%s bulk_user_id=%d", user.ID, action, bulkType, bulkUserID)
	}
	payload := moderacioBulkJobPayload{
		Action:     action,
		Scope:      "all",
		BulkType:   bulkType,
		BulkUserID: bulkUserID,
		Reason:     motiu,
		Source:     "moderacio",
	}
	payloadJSON, _ := json.Marshal(payload)
	now := adminJobNow()
	adminJob := db.AdminJob{
		Kind:          adminJobKindModeracioBulk,
		Status:        adminJobStatusRunning,
		Phase:         adminJobPhaseResolvingTargets,
		ProgressTotal: moderacioBulkProgressFinalStep,
		ProgressDone:  0,
		PayloadJSON:   string(payloadJSON),
		StartedAt:     sql.NullTime{Time: now, Valid: true},
		CreatedBy:     sqlNullIntFromInt(user.ID),
	}
	jobID, err := a.DB.CreateAdminJob(&adminJob)
	if err != nil {
		Errorf("moderacio bulk admin job row create failed actor=%d action=%s scope=all type=%s bulk_user_id=%d err=%v", user.ID, action, bulkType, bulkUserID, err)
		return 0, err
	}
	if IsDebugEnabled() {
		Debugf("moderacio bulk admin job row created job=%d actor=%d action=%s type=%s bulk_user_id=%d", jobID, user.ID, action, bulkType, bulkUserID)
	}
	canModerateAll := a.hasPerm(perms, permModerate)
	snapshot, err := a.resolveModeracioBulkAllSnapshot(bulkType, user, perms, canModerateAll, bulkUserID)
	if err != nil {
		Errorf("moderacio bulk snapshot resolve failed job=%d actor=%d action=%s type=%s bulk_user_id=%d err=%v", jobID, user.ID, action, bulkType, bulkUserID, err)
		resultJSON := mustMarshalModeracioBulkResult(moderacioBulkJobResult{
			Action:     action,
			Scope:      "all",
			BulkType:   bulkType,
			BulkUserID: bulkUserID,
			Phase:      adminJobPhaseError,
			ScopeMode:  snapshot.ScopeMode,
			Candidates: snapshot.Candidates,
			Targets:    len(snapshot.Targets),
			ByType:     snapshot.ByType,
			ResolveMs:  durationMillis(snapshot.ResolveDur),
			TotalMs:    durationMillis(snapshot.ResolveDur),
		})
		a.setAdminJobState(jobID, adminJobStatusError, adminJobPhaseError, err, resultJSON, &now)
		return 0, err
	}
	if IsDebugEnabled() {
		Debugf("moderacio bulk snapshot resolved job=%d actor=%d action=%s type=%s bulk_user_id=%d candidates=%d targets=%d scope_mode=%s resolve_ms=%d", jobID, user.ID, action, bulkType, bulkUserID, snapshot.Candidates, len(snapshot.Targets), snapshot.ScopeMode, durationMillis(snapshot.ResolveDur))
	}
	persistStart := time.Now()
	if err := a.DB.CreateAdminJobTargets(jobID, snapshot.Targets); err != nil {
		Errorf("moderacio bulk snapshot persist failed job=%d actor=%d action=%s type=%s bulk_user_id=%d targets=%d err=%v", jobID, user.ID, action, bulkType, bulkUserID, len(snapshot.Targets), err)
		resultJSON := mustMarshalModeracioBulkResult(moderacioBulkJobResult{
			Action:     action,
			Scope:      "all",
			BulkType:   bulkType,
			BulkUserID: bulkUserID,
			Phase:      adminJobPhaseError,
			ScopeMode:  snapshot.ScopeMode,
			Candidates: snapshot.Candidates,
			Targets:    len(snapshot.Targets),
			ByType:     snapshot.ByType,
			ResolveMs:  durationMillis(snapshot.ResolveDur),
			TotalMs:    durationMillis(snapshot.ResolveDur),
		})
		a.setAdminJobState(jobID, adminJobStatusError, adminJobPhaseError, err, resultJSON, &now)
		return 0, err
	}
	if IsDebugEnabled() {
		Debugf("moderacio bulk snapshot persisted job=%d targets=%d persist_ms=%d", jobID, len(snapshot.Targets), durationMillis(time.Since(persistStart)))
	}
	progressTotal := len(snapshot.Targets) + moderacioBulkProgressFinalStep
	if progressTotal <= 0 {
		progressTotal = moderacioBulkProgressFinalStep
	}
	a.updateAdminJobProgress(jobID, 0, progressTotal)
	initialResultJSON := mustMarshalModeracioBulkResult(moderacioBulkJobResult{
		Action:     action,
		Scope:      "all",
		BulkType:   bulkType,
		BulkUserID: bulkUserID,
		Phase:      adminJobPhaseQueued,
		ScopeMode:  snapshot.ScopeMode,
		Candidates: snapshot.Candidates,
		Targets:    len(snapshot.Targets),
		ByType:     snapshot.ByType,
		ResolveMs:  durationMillis(snapshot.ResolveDur),
		TotalMs:    durationMillis(snapshot.ResolveDur),
	})
	a.setAdminJobState(jobID, adminJobStatusQueued, adminJobPhaseQueued, nil, initialResultJSON, nil)
	if len(snapshot.Targets) == 0 {
		doneJSON := mustMarshalModeracioBulkResult(moderacioBulkJobResult{
			Action:       action,
			Scope:        "all",
			BulkType:     bulkType,
			BulkUserID:   bulkUserID,
			Phase:        adminJobPhaseDone,
			ScopeMode:    snapshot.ScopeMode,
			Candidates:   snapshot.Candidates,
			Targets:      0,
			Processed:    0,
			Updated:      0,
			Skipped:      0,
			Errors:       0,
			ByType:       snapshot.ByType,
			ResolveMs:    durationMillis(snapshot.ResolveDur),
			TotalMs:      durationMillis(snapshot.ResolveDur),
			ActivityMode: "bulk",
		})
		a.updateAdminJobProgress(jobID, progressTotal, progressTotal)
		a.setAdminJobState(jobID, adminJobStatusDone, adminJobPhaseDone, nil, doneJSON, &now)
		if IsDebugEnabled() {
			Debugf("moderacio bulk job completed without targets job=%d", jobID)
		}
		return jobID, nil
	}
	if IsDebugEnabled() {
		Debugf("moderacio bulk worker scheduled job=%d targets=%d action=%s type=%s queue_setup_ms=%d", jobID, len(snapshot.Targets), action, bulkType, durationMillis(time.Since(persistStart)))
	}
	go a.runModeracioBulkAdminJob(jobID, action, motiu, user.ID, snapshot)
	return jobID, nil
}

func (a *App) resolveModeracioBulkAllSnapshot(bulkType string, user *db.User, perms db.PolicyPermissions, canModerateAll bool, bulkUserID int) (moderacioBulkSnapshot, error) {
	start := time.Now()
	scopeModel := a.newModeracioScopeModel(user, perms, canModerateAll)
	scopeMode := "scoped"
	if scopeModel.canModerateAll {
		scopeMode = "global"
	}
	allowedTypes := scopeModel.allowedTypes()
	allowedMap := map[string]bool{}
	for _, t := range allowedTypes {
		allowedMap[t] = true
	}
	types := allowedTypes
	if bulkType != "" && bulkType != "all" {
		if !allowedMap[bulkType] {
			return moderacioBulkSnapshot{}, fmt.Errorf("tipus no autoritzat")
		}
		types = []string{bulkType}
	}
	if len(types) == 0 {
		return moderacioBulkSnapshot{}, fmt.Errorf("cap tipus autoritzat")
	}
	targets := make([]db.AdminJobTarget, 0)
	countsByType := map[string]int{}
	candidates := 0
	addTarget := func(objType string, objID int) {
		if strings.TrimSpace(objType) == "" || objID <= 0 {
			return
		}
		targets = append(targets, db.AdminJobTarget{
			SeqNum:     len(targets) + 1,
			ObjectType: objType,
			ObjectID:   objID,
		})
		countsByType[objType]++
	}
	wikiPendingByType := map[string][]int{}
	needsWikiChanges := false
	for _, t := range types {
		switch t {
		case "municipi_canvi", "arxiu_canvi", "llibre_canvi", "persona_canvi", "cognom_canvi", "event_historic_canvi":
			needsWikiChanges = true
		}
		if needsWikiChanges {
			break
		}
	}
	if needsWikiChanges {
		changes, stale, err := a.DB.ListWikiPendingChanges(0, 0)
		if err != nil {
			return moderacioBulkSnapshot{}, err
		}
		for _, changeID := range stale {
			_ = a.DB.DequeueWikiPending(changeID)
		}
		for _, change := range changes {
			objType := resolveWikiChangeModeracioType(change)
			if objType == "" {
				return moderacioBulkSnapshot{}, fmt.Errorf("wiki change sense tipus moderable: %d", change.ID)
			}
			if !allowedMap[objType] {
				continue
			}
			if !scopeModel.canModerateAll && !scopeModel.canModerateWikiChange(change, objType) {
				continue
			}
			wikiPendingByType[objType] = append(wikiPendingByType[objType], change.ID)
		}
	}
	for _, objType := range types {
		switch objType {
		case "persona":
			if !scopeModel.canModerateType("persona") {
				continue
			}
			rows, err := a.DB.ListPersones(db.PersonaFilter{Estat: "pendent"})
			if err != nil {
				return moderacioBulkSnapshot{}, err
			}
			candidates += len(rows)
			for _, row := range rows {
				addTarget(objType, row.ID)
			}
		case "arxiu":
			filter := db.ArxiuFilter{Status: "pendent", Limit: -1}
			if scope, ok := scopeModel.scopeFilterForType("arxiu"); ok && !scope.hasGlobal {
				applyScopeFilterToArxiu(&filter, scope)
			}
			rows, err := a.DB.ListArxius(filter)
			if err != nil {
				return moderacioBulkSnapshot{}, err
			}
			candidates += len(rows)
			for _, row := range rows {
				addTarget(objType, row.ID)
			}
		case "llibre":
			filter := db.LlibreFilter{Status: "pendent"}
			if scope, ok := scopeModel.scopeFilterForType("llibre"); ok && !scope.hasGlobal {
				applyScopeFilterToLlibre(&filter, scope)
			}
			rows, err := a.DB.ListLlibres(filter)
			if err != nil {
				return moderacioBulkSnapshot{}, err
			}
			candidates += len(rows)
			for _, row := range rows {
				addTarget(objType, row.ID)
			}
		case "nivell":
			filter := db.NivellAdminFilter{Status: "pendent"}
			if scope, ok := scopeModel.scopeFilterForType("nivell"); ok && !scope.hasGlobal {
				applyScopeFilterToNivell(&filter, scope)
			}
			rows, err := a.DB.ListNivells(filter)
			if err != nil {
				return moderacioBulkSnapshot{}, err
			}
			candidates += len(rows)
			for _, row := range rows {
				addTarget(objType, row.ID)
			}
		case "municipi":
			filter := db.MunicipiFilter{Status: "pendent"}
			if scope, ok := scopeModel.scopeFilterForType("municipi"); ok && !scope.hasGlobal {
				applyScopeFilterToMunicipi(&filter, scope)
			}
			rows, err := a.DB.ListMunicipis(filter)
			if err != nil {
				return moderacioBulkSnapshot{}, err
			}
			candidates += len(rows)
			for _, row := range rows {
				addTarget(objType, row.ID)
			}
		case "eclesiastic":
			filter := db.ArquebisbatFilter{Status: "pendent"}
			if scope, ok := scopeModel.scopeFilterForType("eclesiastic"); ok && !scope.hasGlobal {
				applyScopeFilterToEcles(&filter, scope)
			}
			rows, err := a.DB.ListArquebisbats(filter)
			if err != nil {
				return moderacioBulkSnapshot{}, err
			}
			candidates += len(rows)
			for _, row := range rows {
				addTarget(objType, row.ID)
			}
		case "municipi_mapa_version":
			rows, err := a.DB.ListMunicipiMapaVersions(db.MunicipiMapaVersionFilter{Status: "pendent"})
			if err != nil {
				return moderacioBulkSnapshot{}, err
			}
			candidates += len(rows)
			for _, row := range rows {
				if scopeModel.canModerateAll || scopeModel.canModerateItem("municipi_mapa_version", row.ID) {
					addTarget(objType, row.ID)
				}
			}
		case "cognom_variant":
			if !scopeModel.canModerateType("cognom_variant") {
				continue
			}
			rows, err := a.DB.ListCognomVariants(db.CognomVariantFilter{Status: "pendent"})
			if err != nil {
				return moderacioBulkSnapshot{}, err
			}
			candidates += len(rows)
			for _, row := range rows {
				addTarget(objType, row.ID)
			}
		case "cognom_referencia":
			if !scopeModel.canModerateType("cognom_referencia") {
				continue
			}
			rows, err := a.DB.ListCognomReferencies(db.CognomReferenciaFilter{Status: "pendent"})
			if err != nil {
				return moderacioBulkSnapshot{}, err
			}
			candidates += len(rows)
			for _, row := range rows {
				addTarget(objType, row.ID)
			}
		case "cognom_merge":
			if !scopeModel.canModerateType("cognom_merge") {
				continue
			}
			rows, err := a.DB.ListCognomRedirectSuggestions(db.CognomRedirectSuggestionFilter{Status: "pendent"})
			if err != nil {
				return moderacioBulkSnapshot{}, err
			}
			candidates += len(rows)
			for _, row := range rows {
				addTarget(objType, row.ID)
			}
		case "municipi_historia_general":
			rows, _, err := a.DB.ListPendingMunicipiHistoriaGeneralVersions(0, 0)
			if err != nil {
				return moderacioBulkSnapshot{}, err
			}
			candidates += len(rows)
			for _, row := range rows {
				if scopeModel.canModerateAll || scopeModel.canModerateItem("municipi_historia_general", row.ID) {
					addTarget(objType, row.ID)
				}
			}
		case "municipi_historia_fet":
			rows, _, err := a.DB.ListPendingMunicipiHistoriaFetVersions(0, 0)
			if err != nil {
				return moderacioBulkSnapshot{}, err
			}
			candidates += len(rows)
			for _, row := range rows {
				if scopeModel.canModerateAll || scopeModel.canModerateItem("municipi_historia_fet", row.ID) {
					addTarget(objType, row.ID)
				}
			}
		case "municipi_anecdota_version":
			rows, _, err := a.DB.ListPendingMunicipiAnecdotariVersions(0, 0)
			if err != nil {
				return moderacioBulkSnapshot{}, err
			}
			candidates += len(rows)
			for _, row := range rows {
				if scopeModel.canModerateAll || scopeModel.canModerateItem("municipi_anecdota_version", row.ID) {
					addTarget(objType, row.ID)
				}
			}
		case "event_historic":
			if !scopeModel.canModerateType("event_historic") {
				continue
			}
			rows, err := a.DB.ListEventsHistoric(db.EventHistoricFilter{Status: "pendent"})
			if err != nil {
				return moderacioBulkSnapshot{}, err
			}
			candidates += len(rows)
			for _, row := range rows {
				addTarget(objType, row.ID)
			}
		case "media_album":
			if !scopeModel.canModerateType("media_album") {
				continue
			}
			rows, err := a.DB.ListMediaAlbumsByStatus("pending")
			if err != nil {
				return moderacioBulkSnapshot{}, err
			}
			candidates += len(rows)
			for _, row := range rows {
				addTarget(objType, row.ID)
			}
		case "media_item":
			if !scopeModel.canModerateType("media_item") {
				continue
			}
			rows, err := a.DB.ListMediaItemsByStatus("pending")
			if err != nil {
				return moderacioBulkSnapshot{}, err
			}
			candidates += len(rows)
			for _, row := range rows {
				addTarget(objType, row.ID)
			}
		case "external_link":
			if !scopeModel.canModerateType("external_link") {
				continue
			}
			rows, err := a.DB.ExternalLinksListByStatus("pending")
			if err != nil {
				return moderacioBulkSnapshot{}, err
			}
			candidates += len(rows)
			for _, row := range rows {
				addTarget(objType, row.ID)
			}
		case "municipi_canvi", "arxiu_canvi", "llibre_canvi", "persona_canvi", "cognom_canvi", "event_historic_canvi":
			ids := wikiPendingByType[objType]
			candidates += len(ids)
			for _, id := range ids {
				addTarget(objType, id)
			}
		case "registre_canvi":
			rows, err := a.DB.ListTranscripcioRawChangesPending()
			if err != nil {
				return moderacioBulkSnapshot{}, err
			}
			candidates += len(rows)
			for _, row := range rows {
				if scopeModel.canModerateAll || scopeModel.canModerateItem("registre_canvi", row.ID) {
					addTarget(objType, row.ID)
				}
			}
		case "registre":
			filter := db.TranscripcioFilter{Status: "pendent", Limit: -1}
			if scope, ok := scopeModel.scopeFilterForType("registre"); ok && !scope.hasGlobal {
				applyScopeFilterToRegistre(&filter, scope)
			}
			rows, err := a.DB.ListTranscripcionsRawGlobal(filter)
			if err != nil {
				return moderacioBulkSnapshot{}, err
			}
			candidates += len(rows)
			for _, row := range rows {
				if bulkUserID > 0 {
					if !row.CreatedBy.Valid || int(row.CreatedBy.Int64) != bulkUserID {
						continue
					}
				}
				addTarget(objType, row.ID)
			}
		}
	}
	return moderacioBulkSnapshot{
		Targets:    targets,
		Candidates: candidates,
		ScopeMode:  scopeMode,
		ByType:     moderacioCountsFromMap(countsByType),
		ResolveDur: time.Since(start),
	}, nil
}

func (a *App) runModeracioBulkAdminJob(jobID int, action, motiu string, actorID int, snapshot moderacioBulkSnapshot) {
	start := time.Now()
	if IsDebugEnabled() {
		Debugf("moderacio bulk worker started job=%d actor=%d action=%s targets=%d", jobID, actorID, action, len(snapshot.Targets))
	}
	result := moderacioBulkJobResult{
		Action:     action,
		Scope:      "all",
		BulkType:   "all",
		Phase:      adminJobPhaseApplyingChanges,
		ScopeMode:  snapshot.ScopeMode,
		Candidates: snapshot.Candidates,
		Targets:    len(snapshot.Targets),
		ByType:     snapshot.ByType,
		ResolveMs:  durationMillis(snapshot.ResolveDur),
	}
	errorCollector := newModeracioBulkErrorCollector()
	job, err := a.DB.GetAdminJob(jobID)
	if err == nil && job != nil {
		var payload moderacioBulkJobPayload
		if strings.TrimSpace(job.PayloadJSON) != "" && json.Unmarshal([]byte(job.PayloadJSON), &payload) == nil {
			if payload.BulkType != "" {
				result.BulkType = payload.BulkType
			}
			result.BulkUserID = payload.BulkUserID
		}
	}
	a.setAdminJobState(jobID, adminJobStatusRunning, adminJobPhaseApplyingChanges, nil, mustMarshalModeracioBulkResult(result), nil)
	targets, err := a.DB.ListAdminJobTargets(jobID)
	if err != nil {
		result.Errors += recordModeracioBulkWorkerError(jobID, actorID, errorCollector, &result, adminJobPhaseApplyingChanges, "load_snapshot_targets", "", 0, err)
		result.Phase = adminJobPhaseError
		result.TotalMs = durationMillis(time.Since(start))
		a.setAdminJobState(jobID, adminJobStatusError, adminJobPhaseError, err, mustMarshalModeracioBulkResult(result), timePtr(adminJobNow()))
		return
	}
	grouped := map[string][]int{}
	order := make([]string, 0)
	for _, target := range targets {
		key := strings.TrimSpace(target.ObjectType)
		if key == "" || target.ObjectID <= 0 {
			continue
		}
		if _, ok := grouped[key]; !ok {
			order = append(order, key)
		}
		grouped[key] = append(grouped[key], target.ObjectID)
	}
	bulkStatus := "publicat"
	bulkNotes := ""
	if action == "reject" {
		bulkStatus = "rebutjat"
		bulkNotes = motiu
	}
	processed := 0
	updated := 0
	skipped := 0
	errCount := 0
	updateDur := time.Duration(0)
	activityDur := time.Duration(0)
	activityMode := "bulk"
	successByType := map[string][]int{}
	flushProgress := func() {
		a.updateAdminJobProgress(jobID, processed, len(targets)+moderacioBulkProgressFinalStep)
		result.Processed = processed
		result.Updated = updated
		result.Skipped = skipped
		result.Errors = errCount
		result.UpdateMs = durationMillis(updateDur)
		result.ActivityMs = durationMillis(activityDur)
		result.TotalMs = durationMillis(time.Since(start))
	}
	for _, objType := range order {
		ids := grouped[objType]
		if len(ids) == 0 {
			continue
		}
		if objType == "registre" {
			registreMetrics := &moderacioApplyMetrics{}
			registreResult := a.applyModeracioBulkRegistreUpdates(action, ids, motiu, actorID, registreMetrics, func(chunkMetrics moderacioBulkRegistreChunkMetrics) {
				processed += chunkMetrics.ChunkSize
				flushProgress()
				if IsDebugEnabled() {
					Debugf("moderacio bulk worker registre job=%d chunk=%d size=%d loaded=%d updated=%d errors=%d load_dur=%s update_dur=%s activity_dur=%s audit_dur=%s postproc_dur=%s total_dur=%s throughput=%.1f/s deferred_activity=%t", jobID, chunkMetrics.ChunkIndex, chunkMetrics.ChunkSize, chunkMetrics.LoadedRows, chunkMetrics.Updated, chunkMetrics.Errors, chunkMetrics.LoadDur, chunkMetrics.UpdateDur, chunkMetrics.ActivityDur, chunkMetrics.AuditDur, chunkMetrics.PostprocDur, chunkMetrics.TotalDur, chunkMetrics.Throughput, chunkMetrics.DeferredActivity)
				}
			})
			updateDur += registreMetrics.UpdateDur
			updated += registreResult.Updated
			skipped += registreResult.Skipped
			for _, itemErr := range registreResult.Errors {
				errCount += recordModeracioBulkWorkerError(jobID, actorID, errorCollector, &result, adminJobPhaseApplyingChanges, "bulk_update_registre", objType, itemErr.ID, itemErr.Err)
			}
			if len(registreResult.SuccessIDs) > 0 {
				successByType[objType] = append(successByType[objType], registreResult.SuccessIDs...)
			}
			flushProgress()
			continue
		}
		if moderacioBulkSimpleTypes[objType] {
			stepStart := time.Now()
			updatedNow, err := a.DB.BulkUpdateModeracioSimple(objType, bulkStatus, bulkNotes, actorID, ids)
			updateDur += time.Since(stepStart)
			processed += len(ids)
			if err != nil {
				errCount += recordModeracioBulkWorkerError(jobID, actorID, errorCollector, &result, adminJobPhaseApplyingChanges, "bulk_update", objType, 0, err)
				flushProgress()
				continue
			}
			updated += updatedNow
			if updatedNow < len(ids) {
				skipped += len(ids) - updatedNow
			}
			if updatedNow > 0 {
				successByType[objType] = append(successByType[objType], ids...)
			}
			flushProgress()
			continue
		}
		successIDs := make([]int, 0, len(ids))
		for idx, id := range ids {
			stepStart := time.Now()
			if err := a.applyModeracioUpdate(action, objType, id, motiu, actorID, nil); err != nil {
				errCount += recordModeracioBulkWorkerError(jobID, actorID, errorCollector, &result, adminJobPhaseApplyingChanges, "apply_update", objType, id, err)
			} else {
				successIDs = append(successIDs, id)
				updated++
			}
			updateDur += time.Since(stepStart)
			processed++
			if processed == len(targets) || idx == len(ids)-1 || processed%100 == 0 {
				flushProgress()
			}
		}
		if len(successIDs) > 0 {
			successByType[objType] = append(successByType[objType], successIDs...)
		}
	}
	result.Phase = adminJobPhaseRecordingHistory
	flushProgress()
	a.setAdminJobState(jobID, adminJobStatusRunning, adminJobPhaseRecordingHistory, nil, mustMarshalModeracioBulkResult(result), nil)
	ctx := withActivityBulkMode(context.Background(), ActivityBulkMode{})
	for _, objType := range order {
		ids := successByType[objType]
		if len(ids) == 0 {
			continue
		}
		stepStart := time.Now()
		if err := a.applyModeracioActivitiesBulk(ctx, action, objType, ids, motiu, actorID, nil); err != nil {
			errCount += recordModeracioBulkWorkerError(jobID, actorID, errorCollector, &result, adminJobPhaseRecordingHistory, "apply_activity_bulk", objType, 0, err)
			activityMode = "mixed"
		}
		stepDur := time.Since(stepStart)
		activityDur += stepDur
		if IsDebugEnabled() {
			Debugf("moderacio bulk worker history job=%d type=%s ids=%d activity_dur=%s", jobID, objType, len(ids), stepDur)
		}
	}
	a.updateAdminJobProgress(jobID, len(targets)+moderacioBulkProgressFinalStep, len(targets)+moderacioBulkProgressFinalStep)
	result.Phase = adminJobPhaseDone
	result.Processed = processed
	result.Updated = updated
	result.Skipped = skipped
	result.Errors = errCount
	result.UpdateMs = durationMillis(updateDur)
	result.ActivityMs = durationMillis(activityDur)
	result.TotalMs = durationMillis(time.Since(start))
	result.ActivityMode = activityMode
	result.ErrorPhases = errorCollector.phaseCountsSlice()
	result.ErrorSamples = errorCollector.samplesSlice()
	auditStart := time.Now()
	if err := a.insertAdminAudit(nil, actorID, auditActionModeracioBulk, "moderacio", 0, map[string]interface{}{
		"action":       action,
		"scope":        "all",
		"bulk_type":    result.BulkType,
		"bulk_user_id": result.BulkUserID,
		"candidates":   result.Candidates,
		"total":        result.Targets,
		"processed":    result.Processed,
		"updated":      result.Updated,
		"errors":       result.Errors,
		"skipped":      result.Skipped,
		"job_id":       jobID,
		"async":        true,
		"persistent":   true,
	}); err != nil {
		errCount += recordModeracioBulkWorkerError(jobID, actorID, errorCollector, &result, adminJobPhaseRecordingHistory, "admin_audit", "moderacio", 0, err)
		result.Errors = errCount
		result.ErrorPhases = errorCollector.phaseCountsSlice()
		result.ErrorSamples = errorCollector.samplesSlice()
	}
	metrics := moderacioBulkMetrics{
		ResolveDur:  snapshot.ResolveDur,
		UpdateDur:   updateDur,
		ActivityDur: activityDur,
		TotalDur:    time.Since(start),
		Mode:        "persistent",
		ScopeMode:   snapshot.ScopeMode,
	}
	a.logModeracioBulkExecution(action, "all", result.BulkType, actorID, result.BulkUserID, true, moderacioBulkResult{
		Candidates: result.Candidates,
		Total:      result.Targets,
		Processed:  result.Processed,
		Errors:     result.Errors,
		Skipped:    result.Skipped,
	}, metrics, time.Since(auditStart))
	finishedAt := adminJobNow()
	if errCount > 0 {
		Errorf("moderacio bulk worker completed with errors job=%d actor=%d processed=%d updated=%d errors=%d", jobID, actorID, result.Processed, result.Updated, errCount)
		result.Phase = adminJobPhaseError
		a.setAdminJobState(jobID, adminJobStatusError, adminJobPhaseError, fmt.Errorf("%s", moderacioBulkErrorText(result)), mustMarshalModeracioBulkResult(result), &finishedAt)
		return
	}
	if IsDebugEnabled() {
		Debugf("moderacio bulk worker completed job=%d actor=%d processed=%d updated=%d errors=%d update_dur=%s activity_dur=%s total_dur=%s", jobID, actorID, result.Processed, result.Updated, errCount, updateDur, activityDur, time.Since(start))
	}
	a.setAdminJobState(jobID, adminJobStatusDone, adminJobPhaseDone, nil, mustMarshalModeracioBulkResult(result), &finishedAt)
}

func moderacioCountsFromMap(counts map[string]int) []moderacioTypeCount {
	if len(counts) == 0 {
		return []moderacioTypeCount{}
	}
	res := make([]moderacioTypeCount, 0, len(counts))
	for _, objType := range moderacioBulkAllowedTypes {
		total := counts[objType]
		if total <= 0 {
			continue
		}
		res = append(res, moderacioTypeCount{Type: objType, Total: total})
	}
	return res
}

func mustMarshalModeracioBulkResult(result moderacioBulkJobResult) string {
	raw, err := json.Marshal(result)
	if err != nil {
		return "{}"
	}
	return string(raw)
}

func durationMillis(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return d.Milliseconds()
}

func timePtr(t time.Time) *time.Time {
	return &t
}

func (a *App) failInterruptedModeracioBulkJobs() {
	if a == nil || a.DB == nil {
		return
	}
	for _, status := range []string{adminJobStatusQueued, adminJobStatusRunning} {
		jobs, err := a.DB.ListAdminJobs(db.AdminJobFilter{
			Kind:   adminJobKindModeracioBulk,
			Status: status,
			Limit:  5000,
		})
		if err != nil {
			return
		}
		for _, job := range jobs {
			resultJSON := strings.TrimSpace(job.ResultJSON)
			if resultJSON == "" {
				resultJSON = mustMarshalModeracioBulkResult(moderacioBulkJobResult{
					Phase: adminJobPhaseError,
				})
			}
			finishedAt := adminJobNow()
			a.setAdminJobState(job.ID, adminJobStatusError, adminJobPhaseError, fmt.Errorf("job interromput per reinici del worker"), resultJSON, &finishedAt)
		}
	}
}

func filterEmptyStrings(parts []string) []string {
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}
