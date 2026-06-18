package core

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const (
	adminImportStatusOK    = "ok"
	adminImportStatusError = "error"
)

type adminImportJobDetail struct {
	Payload       map[string]interface{}
	Result        map[string]interface{}
	Targets       []db.AdminJobTarget
	ProgressTotal int
	ProgressDone  int
	StartedAt     time.Time
	FinishedAt    time.Time
}

type adminImportLogResult struct {
	ImportRunID int
	AdminJobID  int
	AuditID     int
}

func (a *App) logAdminImportRun(r *http.Request, importType, status string, userID int) {
	a.logAdminImportRunDetailed(r, importType, status, userID, nil)
}

func (a *App) logAdminImportRunDetailed(r *http.Request, importType, status string, userID int, detail *adminImportJobDetail) {
	_ = a.logAdminImportRunDetailedResult(r, importType, status, userID, detail)
}

func (a *App) logAdminImportRunDetailedResult(r *http.Request, importType, status string, userID int, detail *adminImportJobDetail) adminImportLogResult {
	var logResult adminImportLogResult
	if a == nil || a.DB == nil {
		return logResult
	}
	cleanType := strings.TrimSpace(importType)
	if cleanType == "" {
		cleanType = "unknown"
	}
	cleanStatus := strings.ToLower(strings.TrimSpace(status))
	if cleanStatus != adminImportStatusOK {
		cleanStatus = adminImportStatusError
	}
	importRunID, err := a.DB.InsertAdminImportRun(cleanType, cleanStatus, userID)
	if err != nil {
		Errorf("Admin import run log failed: %v", err)
	} else {
		logResult.ImportRunID = importRunID
	}
	auditMeta := map[string]interface{}{
		"type":   cleanType,
		"status": cleanStatus,
	}
	payload := map[string]interface{}{
		"import_type": cleanType,
	}
	if detail != nil && len(detail.Payload) > 0 {
		payload = detail.Payload
		if _, ok := payload["import_type"]; !ok {
			payload["import_type"] = cleanType
		}
	}
	result := map[string]interface{}{
		"status": cleanStatus,
	}
	if detail != nil && len(detail.Result) > 0 {
		result = detail.Result
		if _, ok := result["status"]; !ok {
			result["status"] = cleanStatus
		}
	}
	if len(payload) > 0 {
		auditMeta["payload"] = payload
	}
	if len(result) > 0 {
		auditMeta["result"] = result
	}
	if auditID, err := a.insertAdminAudit(r, userID, auditActionAdminImport, "import", 0, auditMeta); err == nil {
		logResult.AuditID = auditID
	}
	payloadJSON, _ := json.Marshal(payload)
	resultJSON, _ := json.Marshal(result)
	now := time.Now()
	startedAt := now
	finishedAt := now
	progressTotal := 1
	progressDone := 1
	if detail != nil {
		if !detail.StartedAt.IsZero() {
			startedAt = detail.StartedAt
		}
		if !detail.FinishedAt.IsZero() {
			finishedAt = detail.FinishedAt
		}
		if detail.ProgressTotal > 0 {
			progressTotal = detail.ProgressTotal
		}
		if detail.ProgressDone > 0 {
			progressDone = detail.ProgressDone
		}
		if progressDone > progressTotal {
			progressDone = progressTotal
		}
	}
	jobStatus := adminJobStatusDone
	if cleanStatus != adminImportStatusOK {
		jobStatus = adminJobStatusError
	}
	adminJob := db.AdminJob{
		Kind:          adminJobKindImport,
		Status:        jobStatus,
		Phase:         jobStatus,
		ProgressTotal: progressTotal,
		ProgressDone:  progressDone,
		PayloadJSON:   string(payloadJSON),
		ResultJSON:    string(resultJSON),
		StartedAt:     sql.NullTime{Time: startedAt, Valid: true},
		FinishedAt:    sql.NullTime{Time: finishedAt, Valid: true},
	}
	if userID > 0 {
		adminJob.CreatedBy = sqlNullIntFromInt(userID)
	}
	jobID, err := a.DB.CreateAdminJob(&adminJob)
	if err != nil {
		Errorf("Admin job import log failed: %v", err)
		return logResult
	}
	logResult.AdminJobID = jobID
	if detail == nil || len(detail.Targets) == 0 {
		return logResult
	}
	targets := make([]db.AdminJobTarget, 0, len(detail.Targets))
	for idx, target := range detail.Targets {
		if strings.TrimSpace(target.ObjectType) == "" || target.ObjectID <= 0 {
			continue
		}
		target.JobID = jobID
		if target.SeqNum <= 0 {
			target.SeqNum = idx + 1
		}
		targets = append(targets, target)
	}
	if len(targets) == 0 {
		return logResult
	}
	if err := a.DB.CreateAdminJobTargets(jobID, targets); err != nil {
		Errorf("Admin job import targets log failed: %v", err)
	}
	return logResult
}
