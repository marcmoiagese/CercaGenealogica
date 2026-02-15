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

func (a *App) logAdminImportRun(r *http.Request, importType, status string, userID int) {
	if a == nil || a.DB == nil {
		return
	}
	cleanType := strings.TrimSpace(importType)
	if cleanType == "" {
		cleanType = "unknown"
	}
	cleanStatus := strings.ToLower(strings.TrimSpace(status))
	if cleanStatus != adminImportStatusOK {
		cleanStatus = adminImportStatusError
	}
	if err := a.DB.InsertAdminImportRun(cleanType, cleanStatus, userID); err != nil {
		Errorf("Admin import run log failed: %v", err)
	}
	a.logAdminAudit(r, userID, auditActionAdminImport, "import", 0, map[string]interface{}{
		"type":   cleanType,
		"status": cleanStatus,
	})
	payloadJSON, _ := json.Marshal(map[string]interface{}{
		"import_type": cleanType,
	})
	resultJSON, _ := json.Marshal(map[string]interface{}{
		"status": cleanStatus,
	})
	now := time.Now()
	jobStatus := adminJobStatusDone
	if cleanStatus != adminImportStatusOK {
		jobStatus = adminJobStatusError
	}
	adminJob := db.AdminJob{
		Kind:          adminJobKindImport,
		Status:        jobStatus,
		ProgressTotal: 1,
		ProgressDone:  1,
		PayloadJSON:   string(payloadJSON),
		ResultJSON:    string(resultJSON),
		StartedAt:     sql.NullTime{Time: now, Valid: true},
		FinishedAt:    sql.NullTime{Time: now, Valid: true},
	}
	if userID > 0 {
		adminJob.CreatedBy = sqlNullIntFromInt(userID)
	}
	if _, err := a.DB.CreateAdminJob(&adminJob); err != nil {
		Errorf("Admin job import log failed: %v", err)
	}
}
