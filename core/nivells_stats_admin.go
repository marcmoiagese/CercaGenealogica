package core

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func (a *App) NivellStatsAdminAPI(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 6 || parts[0] != "api" || parts[1] != "admin" || parts[2] != "nivells" || parts[5] != "rebuild" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !validateCSRF(r, r.Header.Get("X-CSRF-Token")) && !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriNivellsRebuild, PermissionTarget{})
	if !ok {
		return
	}
	nivellID, err := strconv.Atoi(parts[3])
	if err != nil || nivellID < 0 {
		http.NotFound(w, r)
		return
	}
	all := strings.TrimSpace(r.URL.Query().Get("all")) == "1"
	async := strings.TrimSpace(r.URL.Query().Get("async")) == "1"
	switch parts[4] {
	case "demografia":
		if async {
			job, err := a.startNivellRebuildJob("demografia", nivellID, all, user.ID)
			if err != nil {
				http.Error(w, "failed to start", http.StatusInternalServerError)
				return
			}
			a.logAdminAudit(r, user.ID, auditActionNivellsRebuild, "nivell", nivellID, map[string]interface{}{
				"kind":   "demografia",
				"all":    all,
				"source": "nivells_admin",
			})
			writeJSON(w, map[string]interface{}{"ok": true, "job_id": job.ID, "admin_job_id": job.AdminJobID})
			return
		}
		if nivellID <= 0 && !all {
			http.NotFound(w, r)
			return
		}
		processed := 0
		if nivellID <= 0 && all {
			nivells, err := a.DB.ListNivells(db.NivellAdminFilter{})
			if err != nil {
				http.Error(w, "failed to list", http.StatusInternalServerError)
				return
			}
			for _, nivell := range nivells {
				if nivell.ID <= 0 {
					continue
				}
				if err := a.DB.RebuildNivellDemografia(nivell.ID); err != nil {
					http.Error(w, "failed to rebuild", http.StatusInternalServerError)
					return
				}
				processed++
			}
		} else {
			if err := a.DB.RebuildNivellDemografia(nivellID); err != nil {
				http.Error(w, "failed to rebuild", http.StatusInternalServerError)
				return
			}
			processed = 1
		}
		writeJSON(w, map[string]interface{}{"ok": true, "processed": processed})
	case "stats":
		if async {
			job, err := a.startNivellRebuildJob("stats", nivellID, all, user.ID)
			if err != nil {
				http.Error(w, "failed to start", http.StatusInternalServerError)
				return
			}
			a.logAdminAudit(r, user.ID, auditActionNivellsRebuild, "nivell", nivellID, map[string]interface{}{
				"kind":   "stats",
				"all":    all,
				"source": "nivells_admin",
			})
			writeJSON(w, map[string]interface{}{"ok": true, "job_id": job.ID, "admin_job_id": job.AdminJobID})
			return
		}
		if nivellID <= 0 && !all {
			http.NotFound(w, r)
			return
		}
		processed := 0
		if nivellID <= 0 && all {
			nivells, err := a.DB.ListNivells(db.NivellAdminFilter{})
			if err != nil {
				http.Error(w, "failed to list", http.StatusInternalServerError)
				return
			}
			for _, nivell := range nivells {
				if nivell.ID <= 0 {
					continue
				}
				if err := a.DB.RebuildNivellNomCognomStats(nivell.ID); err != nil {
					http.Error(w, "failed to rebuild", http.StatusInternalServerError)
					return
				}
				processed++
			}
		} else {
			if err := a.DB.RebuildNivellNomCognomStats(nivellID); err != nil {
				http.Error(w, "failed to rebuild", http.StatusInternalServerError)
				return
			}
			processed = 1
		}
		writeJSON(w, map[string]interface{}{"ok": true, "processed": processed})
	default:
		http.NotFound(w, r)
	}
}

func (a *App) NivellStatsAdminJobStatusAPI(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 5 || parts[0] != "api" || parts[1] != "admin" || parts[2] != "nivells" || parts[3] != "rebuild" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if _, ok := a.requirePermissionKey(w, r, permKeyTerritoriNivellsRebuild, PermissionTarget{}); !ok {
		return
	}
	jobID := strings.TrimSpace(parts[4])
	if jobID == "" {
		http.NotFound(w, r)
		return
	}
	job, ok := a.nivellRebuildStore().snapshot(jobID)
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, map[string]interface{}{"ok": true, "job": job})
}

func (a *App) startNivellRebuildJob(kind string, nivellID int, all bool, createdBy int) (*nivellRebuildJob, error) {
	payload := map[string]interface{}{
		"kind":       kind,
		"nivell_id":  nivellID,
		"all":        all,
		"job_source": "admin",
	}
	payloadJSON, _ := json.Marshal(payload)
	now := time.Now()
	adminJob := db.AdminJob{
		Kind:          adminJobKindNivellsRebuild,
		Status:        adminJobStatusRunning,
		ProgressTotal: 0,
		ProgressDone:  0,
		PayloadJSON:   string(payloadJSON),
		StartedAt:     sql.NullTime{Time: now, Valid: true},
	}
	if createdBy > 0 {
		adminJob.CreatedBy = sqlNullIntFromInt(createdBy)
	}
	adminJobID, err := a.DB.CreateAdminJob(&adminJob)
	if err != nil {
		return nil, err
	}
	store := a.nivellRebuildStore()
	job := store.newJob(kind, 0, adminJobID)
	go func() {
		store.appendLog(job.ID, "Preparant llista de nivells")
		ids, err := a.collectNivellIDs(nivellID, all)
		if err != nil {
			store.appendLog(job.ID, err.Error())
			store.finish(job.ID, err)
			a.finishAdminJob(adminJobID, adminJobStatusError, err, "")
			return
		}
		total := len(ids)
		if kind == "all" {
			total = total * 2
		}
		store.setTotal(job.ID, total)
		a.updateAdminJobProgress(adminJobID, 0, total)
		if len(ids) == 0 {
			store.appendLog(job.ID, "Sense nivells per recalcular")
			store.finish(job.ID, nil)
			resultJSON, _ := json.Marshal(map[string]interface{}{
				"processed": 0,
				"kind":      kind,
			})
			a.finishAdminJob(adminJobID, adminJobStatusDone, nil, string(resultJSON))
			return
		}
		a.runNivellRebuildJob(job.ID, adminJobID, kind, ids)
	}()
	return job, nil
}

func (a *App) collectNivellIDs(nivellID int, all bool) ([]int, error) {
	if nivellID > 0 {
		return []int{nivellID}, nil
	}
	if !all {
		return nil, fmt.Errorf("missing nivell id")
	}
	nivells, err := a.DB.ListNivells(db.NivellAdminFilter{})
	if err != nil {
		return nil, err
	}
	ids := make([]int, 0, len(nivells))
	for _, nivell := range nivells {
		if nivell.ID > 0 {
			ids = append(ids, nivell.ID)
		}
	}
	return ids, nil
}

func (a *App) runNivellRebuildJob(jobID string, adminJobID int, kind string, ids []int) {
	store := a.nivellRebuildStore()
	processed := 0
	total := len(ids)
	if kind == "all" {
		total = total * 2
	}
	store.appendLog(jobID, "Actualitzant jerarquia administrativa")
	if err := a.rebuildAdminClosureAll(); err != nil {
		store.appendLog(jobID, fmt.Sprintf("Error jerarquia: %v", err))
		store.finish(jobID, err)
		a.finishAdminJob(adminJobID, adminJobStatusError, err, "")
		return
	}
	store.appendLog(jobID, "Jerarquia actualitzada")
	run := func(step string, fn func(int) error) bool {
		for _, id := range ids {
			if err := fn(id); err != nil {
				store.appendLog(jobID, fmt.Sprintf("%s %d: %v", step, id, err))
				store.finish(jobID, err)
				a.finishAdminJob(adminJobID, adminJobStatusError, err, "")
				return false
			}
			processed++
			store.setProcessed(jobID, processed)
			a.updateAdminJobProgress(adminJobID, processed, total)
			store.appendLog(jobID, fmt.Sprintf("%s %d", step, id))
		}
		return true
	}
	store.appendLog(jobID, "Inici recalcul")
	switch kind {
	case "demografia":
		if !run("demografia", a.DB.RebuildNivellDemografia) {
			return
		}
	case "stats":
		if !run("stats", a.DB.RebuildNivellNomCognomStats) {
			return
		}
	case "all":
		if !run("demografia", a.DB.RebuildNivellDemografia) {
			return
		}
		if !run("stats", a.DB.RebuildNivellNomCognomStats) {
			return
		}
	default:
		err := fmt.Errorf("unknown kind")
		store.finish(jobID, err)
		a.finishAdminJob(adminJobID, adminJobStatusError, err, "")
		return
	}
	store.appendLog(jobID, "Recalcul complet")
	store.finish(jobID, nil)
	resultJSON, _ := json.Marshal(map[string]interface{}{
		"processed": processed,
		"kind":      kind,
	})
	a.finishAdminJob(adminJobID, adminJobStatusDone, nil, string(resultJSON))
}
