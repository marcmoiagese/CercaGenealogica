package integration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func waitForAdminJobTerminal(t *testing.T, database db.DB, jobID int) *db.AdminJob {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		job, err := database.GetAdminJob(jobID)
		if err != nil {
			t.Fatalf("GetAdminJob ha fallat: %v", err)
		}
		if job != nil {
			status := strings.TrimSpace(job.Status)
			if status == "done" || status == "error" {
				return job
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	job, err := database.GetAdminJob(jobID)
	if err != nil {
		t.Fatalf("GetAdminJob final ha fallat: %v", err)
	}
	if job == nil {
		t.Fatalf("job %d no trobat", jobID)
	}
	t.Fatalf("job %d no ha acabat: status=%s phase=%s", jobID, job.Status, job.Phase)
	return nil
}

func TestModeracioBulkAsyncPersistsSnapshotAndCompletes(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_12_bulk_job_snapshot.sqlite3")

	admin := createTestUser(t, database, "admin_bulk_f30_12")
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_bulk_f30_12")

	arxiu := &db.Arxiu{
		Nom:            "Arxiu Async Persistent",
		Tipus:          "Parroquial",
		ModeracioEstat: "pendent",
	}
	if _, err := database.CreateArxiu(arxiu); err != nil {
		t.Fatalf("CreateArxiu ha fallat: %v", err)
	}

	csrf := "csrf_bulk_f30_12"
	form := newFormValues(map[string]string{
		"bulk_action": "approve",
		"bulk_scope":  "all",
		"bulk_type":   "arxiu",
		"csrf_token":  csrf,
		"return_to":   "/moderacio",
		"async":       "1",
	})
	req := httptest.NewRequest(http.MethodPost, "/moderacio/bulk", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()
	app.AdminModeracioBulk(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("bulk async esperava 200, got %d", rr.Code)
	}

	var payload struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("response JSON invàlid: %v", err)
	}
	jobID, err := strconv.Atoi(payload.JobID)
	if err != nil || jobID <= 0 {
		t.Fatalf("job_id invàlid: %q err=%v", payload.JobID, err)
	}

	job, err := database.GetAdminJob(jobID)
	if err != nil || job == nil {
		t.Fatalf("GetAdminJob inicial ha fallat: %v", err)
	}
	if job.Kind != "moderacio_bulk" {
		t.Fatalf("kind esperat moderacio_bulk, got %s", job.Kind)
	}
	var payloadMap map[string]interface{}
	if err := json.Unmarshal([]byte(job.PayloadJSON), &payloadMap); err != nil {
		t.Fatalf("payload_json invàlid: %v", err)
	}
	if _, ok := payloadMap["ids"]; ok {
		t.Fatalf("payload_json no ha de contenir ids")
	}
	if _, ok := payloadMap["targets"]; ok {
		t.Fatalf("payload_json no ha de contenir targets")
	}

	targetRows, err := database.Query("SELECT object_type, object_id FROM admin_job_targets WHERE job_id = ? ORDER BY seq_num", jobID)
	if err != nil {
		t.Fatalf("Query admin_job_targets ha fallat: %v", err)
	}
	if len(targetRows) != 1 {
		t.Fatalf("snapshot targets esperats 1, got %d", len(targetRows))
	}
	if targetRows[0]["object_type"] != "arxiu" {
		t.Fatalf("object_type esperat arxiu, got %#v", targetRows[0]["object_type"])
	}
	if parseCountValue(t, targetRows[0]["object_id"]) != arxiu.ID {
		t.Fatalf("object_id esperat %d", arxiu.ID)
	}

	job = waitForAdminJobTerminal(t, database, jobID)
	if job.Status != "done" {
		t.Fatalf("job esperat done, got status=%s phase=%s error=%s", job.Status, job.Phase, job.ErrorText)
	}
	if job.Phase != "done" {
		t.Fatalf("phase esperada done, got %s", job.Phase)
	}

	var result struct {
		Targets   int    `json:"targets"`
		Updated   int    `json:"updated"`
		Errors    int    `json:"errors"`
		Phase     string `json:"phase"`
		ResolveMs int64  `json:"resolve_ms"`
	}
	if err := json.Unmarshal([]byte(job.ResultJSON), &result); err != nil {
		t.Fatalf("result_json invàlid: %v", err)
	}
	if result.Targets != 1 || result.Updated != 1 || result.Errors != 0 {
		t.Fatalf("result inesperat: %+v", result)
	}
	if result.Phase != "done" {
		t.Fatalf("result phase esperada done, got %s", result.Phase)
	}
	if result.ResolveMs < 0 {
		t.Fatalf("resolve_ms invàlid: %d", result.ResolveMs)
	}

	updated, err := database.GetArxiu(arxiu.ID)
	if err != nil || updated == nil {
		t.Fatalf("GetArxiu ha fallat: %v", err)
	}
	if updated.ModeracioEstat != "publicat" {
		t.Fatalf("arxiu esperat publicat, got %s", updated.ModeracioEstat)
	}
}

func TestModeracioBulkAsyncJobVisibleToBulkModerators(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_12_bulk_job_visibility.sqlite3")

	admin := createTestUser(t, database, "admin_bulk_f30_12_visibility")
	assignPolicyByName(t, database, admin.ID, "admin")
	adminSession := createSessionCookie(t, database, admin.ID, "sess_bulk_f30_12_admin")

	viewer := createNonAdminTestUser(t, database, "moderator_bulk_f30_12_viewer")
	assignPolicyByName(t, database, viewer.ID, "moderador")
	grantPolicyPermKey(t, database, "moderador", "moderacio.bulk")
	viewerSession := createSessionCookie(t, database, viewer.ID, "sess_bulk_f30_12_viewer")

	arxiu := &db.Arxiu{
		Nom:            "Arxiu Async Visible",
		Tipus:          "Municipal",
		ModeracioEstat: "pendent",
	}
	if _, err := database.CreateArxiu(arxiu); err != nil {
		t.Fatalf("CreateArxiu ha fallat: %v", err)
	}

	csrf := "csrf_bulk_f30_12_visibility"
	form := newFormValues(map[string]string{
		"bulk_action": "approve",
		"bulk_scope":  "all",
		"bulk_type":   "arxiu",
		"csrf_token":  csrf,
		"return_to":   "/moderacio",
		"async":       "1",
	})
	req := httptest.NewRequest(http.MethodPost, "/moderacio/bulk", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.AddCookie(adminSession)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()
	app.AdminModeracioBulk(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("bulk async esperava 200, got %d", rr.Code)
	}

	var payload struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("response JSON invàlid: %v", err)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/admin/control/moderacio/jobs/"+payload.JobID, nil)
	req.AddCookie(viewerSession)
	rr = httptest.NewRecorder()
	app.AdminControlModeracioJobStatus(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status per moderador bulk esperava 200, got %d", rr.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/admin/jobs/"+payload.JobID, nil)
	req.AddCookie(viewerSession)
	rr = httptest.NewRecorder()
	app.AdminJobsShowPage(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("show page per moderador bulk esperava 200, got %d", rr.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/admin/jobs", nil)
	req.AddCookie(viewerSession)
	rr = httptest.NewRecorder()
	app.AdminJobsListPage(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("jobs list per moderador bulk esperava 200, got %d", rr.Code)
	}
}
