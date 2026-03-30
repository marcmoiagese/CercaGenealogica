package integration

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestModeracioBulkJobStatusAgeNonNegative(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_12_fix_2_age.sqlite3")

	admin := createTestUser(t, database, "admin_bulk_f30_12_fix_2_age")
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_bulk_f30_12_fix_2_age")

	arxiu := &db.Arxiu{
		Nom:            "Arxiu Async Age",
		Tipus:          "Parroquial",
		ModeracioEstat: "pendent",
	}
	if _, err := database.CreateArxiu(arxiu); err != nil {
		t.Fatalf("CreateArxiu ha fallat: %v", err)
	}

	csrf := "csrf_bulk_f30_12_fix_2_age"
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

	var created struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &created); err != nil {
		t.Fatalf("response JSON invàlid: %v", err)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/admin/control/moderacio/jobs/"+created.JobID, nil)
	req.AddCookie(session)
	rr = httptest.NewRecorder()
	app.AdminControlModeracioJobStatus(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status esperava 200, got %d", rr.Code)
	}

	var payload struct {
		Job struct {
			AgeMS     int64  `json:"age_ms"`
			DetailURL string `json:"detail_url"`
		} `json:"job"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("status JSON invàlid: %v", err)
	}
	if payload.Job.AgeMS < 0 {
		t.Fatalf("age_ms no pot ser negatiu: %d", payload.Job.AgeMS)
	}
	if payload.Job.DetailURL != "/admin/jobs/"+created.JobID {
		t.Fatalf("detail_url inesperada: %q", payload.Job.DetailURL)
	}
}

func TestAdminJobsShowPageDisplaysModeracioBulkErrorSamples(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_12_fix_2_job_detail.sqlite3")

	admin := createTestUser(t, database, "admin_bulk_f30_12_fix_2_detail")
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_bulk_f30_12_fix_2_detail")

	payloadJSON := `{"action":"approve","scope":"all","bulk_type":"arxiu","source":"moderacio"}`
	resultJSON := `{"action":"approve","scope":"all","bulk_type":"arxiu","phase":"error","scope_mode":"global","targets":1,"updated":1,"errors":1,"activity_mode":"mixed","error_phases":[{"phase":"recording_history","count":1}],"error_samples":[{"phase":"recording_history","step":"apply_activity_bulk","object_type":"arxiu","object_id":42,"message":"bulk_update_pending_status failed: FOREIGN KEY constraint failed"}]}`
	jobID, err := database.CreateAdminJob(&db.AdminJob{
		Kind:        "moderacio_bulk",
		Status:      "error",
		Phase:       "error",
		PayloadJSON: payloadJSON,
		ResultJSON:  resultJSON,
		ErrorText:   "moderacio bulk completada amb incidencies: updated=1 skipped=0 errors=1; primer error recording_history/apply_activity_bulk/arxiu:42: bulk_update_pending_status failed: FOREIGN KEY constraint failed",
		CreatedBy:   sql.NullInt64{Int64: int64(admin.ID), Valid: true},
	})
	if err != nil {
		t.Fatalf("CreateAdminJob ha fallat: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/admin/jobs/"+strconv.Itoa(jobID), nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminJobsShowPage(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("show page esperava 200, got %d", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "Fases amb errors") {
		t.Fatalf("la pàgina hauria de mostrar el resum de fases amb errors")
	}
	if !strings.Contains(body, "recording_history / apply_activity_bulk / arxiu:42") {
		t.Fatalf("la pàgina hauria de mostrar la mostra d'errors")
	}
	if !strings.Contains(body, "FOREIGN KEY constraint failed") {
		t.Fatalf("la pàgina hauria de mostrar el missatge concret de l'error")
	}
}
