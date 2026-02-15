package core

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const (
	adminJobStatusQueued  = "queued"
	adminJobStatusRunning = "running"
	adminJobStatusDone    = "done"
	adminJobStatusError   = "error"
)

const (
	adminJobKindNivellsRebuild = "nivells_rebuild"
	adminJobKindImport         = "admin_import"
)

type adminJobOption struct {
	Value string
	Label string
}

type adminJobView struct {
	ID              int
	Kind            string
	Status          string
	StatusClass     string
	ProgressDone    int
	ProgressTotal   int
	ProgressPercent int
	ProgressLabel   string
	CreatedAt       string
	StartedAt       string
	FinishedAt      string
	CreatedBy       string
	ErrorText       string
	DetailURL       string
	RetryURL        string
	CanRetry        bool
}

type adminJobCreateRequest struct {
	Kind    string          `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

type nivellsRebuildPayload struct {
	Kind     string `json:"kind"`
	NivellID int    `json:"nivell_id"`
	All      bool   `json:"all"`
}

func (a *App) AdminJobsListPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user, _, ok := a.requirePermission(w, r, permAdmin)
	if !ok {
		return
	}
	lang := ResolveLang(r)
	filterKind := strings.TrimSpace(r.URL.Query().Get("kind"))
	filterStatus := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("status")))
	perPage := parseListPerPage(r.URL.Query().Get("per_page"))
	page := parseListPage(r.URL.Query().Get("page"))
	filter := db.AdminJobFilter{
		Kind:   filterKind,
		Status: filterStatus,
	}
	total, err := a.DB.CountAdminJobs(filter)
	if err != nil {
		http.Error(w, "failed to count", http.StatusInternalServerError)
		return
	}
	if perPage <= 0 {
		perPage = 25
	}
	totalPages := 1
	if total > 0 {
		totalPages = (total + perPage - 1) / perPage
	}
	if page > totalPages {
		page = totalPages
	}
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * perPage
	filter.Limit = perPage
	filter.Offset = offset
	jobs, err := a.DB.ListAdminJobs(filter)
	if err != nil {
		http.Error(w, "failed to list", http.StatusInternalServerError)
		return
	}
	userCache := map[int]string{}
	views := make([]adminJobView, 0, len(jobs))
	for _, row := range jobs {
		views = append(views, buildAdminJobView(a, row, userCache))
	}
	pageValues := cloneValues(r.URL.Query())
	pageValues.Del("page")
	pageValues.Set("per_page", strconv.Itoa(perPage))
	if filterKind != "" {
		pageValues.Set("kind", filterKind)
	}
	if filterStatus != "" {
		pageValues.Set("status", filterStatus)
	}
	pageBase := "/admin/jobs"
	if encoded := pageValues.Encode(); encoded != "" {
		pageBase += "?" + encoded
	}
	pageStart := 0
	pageEnd := 0
	if total > 0 && len(jobs) > 0 {
		pageStart = offset + 1
		pageEnd = offset + len(jobs)
	}
	token, _ := ensureCSRF(w, r)
	RenderPrivateTemplate(w, r, "admin-jobs-list.html", map[string]interface{}{
		"User":          user,
		"Jobs":          views,
		"Total":         total,
		"PerPage":       perPage,
		"Page":          page,
		"TotalPages":    totalPages,
		"HasPrev":       page > 1,
		"HasNext":       page < totalPages,
		"PrevPage":      page - 1,
		"NextPage":      page + 1,
		"PageBase":      pageBase,
		"PageStart":     pageStart,
		"PageEnd":       pageEnd,
		"FilterKind":    filterKind,
		"FilterStatus":  filterStatus,
		"KindOptions":   adminJobKindOptions(lang),
		"StatusOptions": adminJobStatusOptions(lang),
		"CSRFToken":     token,
	})
}

func (a *App) AdminJobsShowPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user, _, ok := a.requirePermission(w, r, permAdmin)
	if !ok {
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) == 2 {
		http.Redirect(w, r, "/admin/jobs", http.StatusSeeOther)
		return
	}
	if len(parts) < 3 {
		http.NotFound(w, r)
		return
	}
	jobID, err := strconv.Atoi(parts[2])
	if err != nil || jobID <= 0 {
		http.NotFound(w, r)
		return
	}
	job, err := a.DB.GetAdminJob(jobID)
	if err != nil {
		http.Error(w, "failed to load", http.StatusInternalServerError)
		return
	}
	if job == nil {
		http.NotFound(w, r)
		return
	}
	userCache := map[int]string{}
	view := buildAdminJobView(a, *job, userCache)
	payload := formatJSONForDisplay(job.PayloadJSON)
	result := formatJSONForDisplay(job.ResultJSON)
	token, _ := ensureCSRF(w, r)
	RenderPrivateTemplate(w, r, "admin-jobs-show.html", map[string]interface{}{
		"User":      user,
		"Job":       view,
		"Payload":   payload,
		"Result":    result,
		"CSRFToken": token,
	})
}

func (a *App) AdminJobsAPI(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		a.adminJobsListAPI(w, r)
	case http.MethodPost:
		a.adminJobsCreateAPI(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (a *App) AdminJobsDetailAPI(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 4 || parts[0] != "api" || parts[1] != "admin" || parts[2] != "jobs" {
		http.NotFound(w, r)
		return
	}
	jobID, err := strconv.Atoi(parts[3])
	if err != nil || jobID <= 0 {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 4 && r.Method == http.MethodGet {
		a.adminJobsShowAPI(w, r, jobID)
		return
	}
	if len(parts) == 5 && parts[4] == "retry" && r.Method == http.MethodPost {
		a.adminJobsRetryAPI(w, r, jobID)
		return
	}
	http.NotFound(w, r)
}

func (a *App) adminJobsListAPI(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permAdmin); !ok {
		return
	}
	filter := db.AdminJobFilter{
		Kind:   strings.TrimSpace(r.URL.Query().Get("kind")),
		Status: strings.ToLower(strings.TrimSpace(r.URL.Query().Get("status"))),
	}
	perPage := parseListPerPage(r.URL.Query().Get("per_page"))
	page := parseListPage(r.URL.Query().Get("page"))
	total, err := a.DB.CountAdminJobs(filter)
	if err != nil {
		http.Error(w, "failed to count", http.StatusInternalServerError)
		return
	}
	if perPage <= 0 {
		perPage = 25
	}
	totalPages := 1
	if total > 0 {
		totalPages = (total + perPage - 1) / perPage
	}
	if page > totalPages {
		page = totalPages
	}
	if page < 1 {
		page = 1
	}
	filter.Limit = perPage
	filter.Offset = (page - 1) * perPage
	jobs, err := a.DB.ListAdminJobs(filter)
	if err != nil {
		http.Error(w, "failed to list", http.StatusInternalServerError)
		return
	}
	payload := make([]map[string]interface{}, 0, len(jobs))
	for _, job := range jobs {
		payload = append(payload, adminJobAPIItem(job))
	}
	writeJSON(w, map[string]interface{}{
		"ok":         true,
		"jobs":       payload,
		"page":       page,
		"per_page":   perPage,
		"total":      total,
		"totalPages": totalPages,
	})
}

func (a *App) adminJobsShowAPI(w http.ResponseWriter, r *http.Request, jobID int) {
	if _, _, ok := a.requirePermission(w, r, permAdmin); !ok {
		return
	}
	job, err := a.DB.GetAdminJob(jobID)
	if err != nil {
		http.Error(w, "failed to load", http.StatusInternalServerError)
		return
	}
	if job == nil {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, map[string]interface{}{
		"ok":  true,
		"job": adminJobAPIItem(*job),
	})
}

func (a *App) adminJobsCreateAPI(w http.ResponseWriter, r *http.Request) {
	user, _, ok := a.requirePermission(w, r, permAdmin)
	if !ok {
		return
	}
	if !validateCSRF(r, r.Header.Get("X-CSRF-Token")) && !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	req, err := parseAdminJobCreateRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	switch req.Kind {
	case adminJobKindNivellsRebuild:
		payload, err := parseNivellsRebuildPayload(req, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		job, err := a.startNivellRebuildJob(payload.Kind, payload.NivellID, payload.All, user.ID)
		if err != nil {
			http.Error(w, "failed to start", http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]interface{}{
			"ok":             true,
			"job_id":         job.AdminJobID,
			"rebuild_job_id": job.ID,
		})
	default:
		http.Error(w, "unsupported job kind", http.StatusBadRequest)
	}
}

func (a *App) adminJobsRetryAPI(w http.ResponseWriter, r *http.Request, jobID int) {
	user, _, ok := a.requirePermission(w, r, permAdmin)
	if !ok {
		return
	}
	if !validateCSRF(r, r.Header.Get("X-CSRF-Token")) && !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	job, err := a.DB.GetAdminJob(jobID)
	if err != nil {
		http.Error(w, "failed to load", http.StatusInternalServerError)
		return
	}
	if job == nil {
		http.NotFound(w, r)
		return
	}
	if strings.ToLower(strings.TrimSpace(job.Status)) != adminJobStatusError {
		http.Error(w, "job not retryable", http.StatusBadRequest)
		return
	}
	switch job.Kind {
	case adminJobKindNivellsRebuild:
		payload := nivellsRebuildPayload{}
		if strings.TrimSpace(job.PayloadJSON) != "" {
			if err := json.Unmarshal([]byte(job.PayloadJSON), &payload); err != nil {
				http.Error(w, "invalid payload", http.StatusBadRequest)
				return
			}
		}
		if payload.Kind == "" {
			http.Error(w, "missing rebuild kind", http.StatusBadRequest)
			return
		}
		newJob, err := a.startNivellRebuildJob(payload.Kind, payload.NivellID, payload.All, user.ID)
		if err != nil {
			http.Error(w, "failed to retry", http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]interface{}{
			"ok":             true,
			"job_id":         newJob.AdminJobID,
			"rebuild_job_id": newJob.ID,
		})
	default:
		http.Error(w, "retry not supported", http.StatusBadRequest)
	}
}

func parseAdminJobCreateRequest(r *http.Request) (adminJobCreateRequest, error) {
	req := adminJobCreateRequest{}
	contentType := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
	if strings.Contains(contentType, "application/json") {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return req, err
		}
		if len(body) == 0 {
			return req, errors.New("empty body")
		}
		if err := json.Unmarshal(body, &req); err != nil {
			return req, err
		}
	} else {
		if err := r.ParseForm(); err != nil {
			return req, err
		}
		req.Kind = strings.TrimSpace(r.FormValue("kind"))
	}
	req.Kind = strings.TrimSpace(req.Kind)
	if req.Kind == "" {
		return req, errors.New("missing kind")
	}
	return req, nil
}

func parseNivellsRebuildPayload(req adminJobCreateRequest, r *http.Request) (nivellsRebuildPayload, error) {
	payload := nivellsRebuildPayload{}
	if len(req.Payload) > 0 {
		if err := json.Unmarshal(req.Payload, &payload); err != nil {
			return payload, err
		}
	}
	if payload.Kind == "" {
		payload.Kind = strings.TrimSpace(r.FormValue("rebuild_kind"))
	}
	if payload.NivellID == 0 {
		if val := strings.TrimSpace(r.FormValue("nivell_id")); val != "" {
			if id, err := strconv.Atoi(val); err == nil {
				payload.NivellID = id
			}
		}
	}
	if !payload.All {
		payload.All = parseBool(r.FormValue("all"))
	}
	payload.Kind = strings.TrimSpace(payload.Kind)
	if payload.Kind == "" {
		return payload, errors.New("missing rebuild kind")
	}
	switch payload.Kind {
	case "demografia", "stats", "all":
	default:
		return payload, errors.New("invalid rebuild kind")
	}
	return payload, nil
}

func parseBool(val string) bool {
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func adminJobKindOptions(lang string) []adminJobOption {
	return []adminJobOption{
		{Value: adminJobKindNivellsRebuild, Label: T(lang, "admin.jobs.kind.nivells_rebuild")},
		{Value: adminJobKindImport, Label: T(lang, "admin.jobs.kind.admin_import")},
	}
}

func adminJobStatusOptions(lang string) []adminJobOption {
	return []adminJobOption{
		{Value: adminJobStatusQueued, Label: T(lang, "admin.jobs.status.queued")},
		{Value: adminJobStatusRunning, Label: T(lang, "admin.jobs.status.running")},
		{Value: adminJobStatusDone, Label: T(lang, "admin.jobs.status.done")},
		{Value: adminJobStatusError, Label: T(lang, "admin.jobs.status.error")},
	}
}

func buildAdminJobView(a *App, job db.AdminJob, cache map[int]string) adminJobView {
	progressLabel := "-"
	progressPercent := 0
	if job.ProgressTotal > 0 {
		progressPercent = int((float64(job.ProgressDone) / float64(job.ProgressTotal)) * 100)
		if progressPercent < 0 {
			progressPercent = 0
		}
		if progressPercent > 100 {
			progressPercent = 100
		}
		progressLabel = strconv.Itoa(job.ProgressDone) + " / " + strconv.Itoa(job.ProgressTotal)
	} else if job.ProgressDone > 0 {
		progressLabel = strconv.Itoa(job.ProgressDone)
	}
	status := strings.ToLower(strings.TrimSpace(job.Status))
	if status == "" {
		status = adminJobStatusQueued
	}
	statusClass := adminJobStatusClass(status)
	errorText := strings.TrimSpace(job.ErrorText)
	return adminJobView{
		ID:              job.ID,
		Kind:            strings.TrimSpace(job.Kind),
		Status:          status,
		StatusClass:     statusClass,
		ProgressDone:    job.ProgressDone,
		ProgressTotal:   job.ProgressTotal,
		ProgressPercent: progressPercent,
		ProgressLabel:   progressLabel,
		CreatedAt:       formatAdminJobTime(job.CreatedAt),
		StartedAt:       formatAdminJobTime(job.StartedAt),
		FinishedAt:      formatAdminJobTime(job.FinishedAt),
		CreatedBy:       resolveJobUser(a, job.CreatedBy, cache),
		ErrorText:       errorText,
		DetailURL:       "/admin/jobs/" + strconv.Itoa(job.ID),
		RetryURL:        "/api/admin/jobs/" + strconv.Itoa(job.ID) + "/retry",
		CanRetry:        status == adminJobStatusError,
	}
}

func adminJobStatusClass(status string) string {
	switch status {
	case adminJobStatusRunning:
		return "job-status--running"
	case adminJobStatusDone:
		return "job-status--done"
	case adminJobStatusError:
		return "job-status--error"
	case adminJobStatusQueued:
		return "job-status--queued"
	default:
		return "job-status--neutral"
	}
}

func resolveJobUser(a *App, id sql.NullInt64, cache map[int]string) string {
	if !id.Valid {
		return ""
	}
	uid := int(id.Int64)
	if name, ok := cache[uid]; ok {
		return name
	}
	user, err := a.DB.GetUserByID(uid)
	if err != nil || user == nil {
		cache[uid] = ""
		return ""
	}
	name := strings.TrimSpace(user.Usuari)
	if name == "" {
		name = strings.TrimSpace(strings.TrimSpace(user.Name) + " " + strings.TrimSpace(user.Surname))
	}
	cache[uid] = name
	return name
}

func formatAdminJobTime(val sql.NullTime) string {
	if !val.Valid {
		return ""
	}
	return val.Time.Format("2006-01-02 15:04")
}

func formatAdminJobTimeISO(val sql.NullTime) string {
	if !val.Valid {
		return ""
	}
	return val.Time.Format(time.RFC3339)
}

func formatJSONForDisplay(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	var out bytes.Buffer
	if err := json.Indent(&out, []byte(raw), "", "  "); err != nil {
		return raw
	}
	return out.String()
}

func adminJobAPIItem(job db.AdminJob) map[string]interface{} {
	item := map[string]interface{}{
		"id":             job.ID,
		"kind":           strings.TrimSpace(job.Kind),
		"status":         strings.ToLower(strings.TrimSpace(job.Status)),
		"progress_total": job.ProgressTotal,
		"progress_done":  job.ProgressDone,
		"payload_json":   strings.TrimSpace(job.PayloadJSON),
		"result_json":    strings.TrimSpace(job.ResultJSON),
		"error_text":     strings.TrimSpace(job.ErrorText),
		"started_at":     formatAdminJobTimeISO(job.StartedAt),
		"finished_at":    formatAdminJobTimeISO(job.FinishedAt),
		"created_at":     formatAdminJobTimeISO(job.CreatedAt),
		"updated_at":     formatAdminJobTimeISO(job.UpdatedAt),
	}
	if job.CreatedBy.Valid {
		item["created_by"] = int(job.CreatedBy.Int64)
	}
	return item
}

func (a *App) updateAdminJobProgress(jobID, progressDone, progressTotal int) {
	if a == nil || a.DB == nil || jobID <= 0 {
		return
	}
	if err := a.DB.UpdateAdminJobProgress(jobID, progressDone, progressTotal); err != nil {
		Errorf("Admin job progress update failed: %v", err)
	}
}

func (a *App) finishAdminJob(jobID int, status string, err error, resultJSON string) {
	if a == nil || a.DB == nil || jobID <= 0 {
		return
	}
	errorText := ""
	if err != nil {
		errorText = err.Error()
		status = adminJobStatusError
	}
	now := time.Now()
	if err := a.DB.UpdateAdminJobStatus(jobID, status, errorText, resultJSON, &now); err != nil {
		Errorf("Admin job status update failed: %v", err)
	}
}
