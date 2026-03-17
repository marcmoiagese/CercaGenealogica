package integration

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func grantPolicyPermKey(t *testing.T, database db.DB, policyName, permKey string) {
	t.Helper()
	rows, err := database.Query("SELECT id FROM politiques WHERE nom = ?", policyName)
	if err != nil || len(rows) == 0 {
		t.Fatalf("no puc obtenir politica %q: %v", policyName, err)
	}
	policyID := parseCountValue(t, rows[0]["id"])
	grant := &db.PoliticaGrant{
		PoliticaID:      policyID,
		PermKey:         permKey,
		ScopeType:       "global",
		ScopeID:         sql.NullInt64{},
		IncludeChildren: false,
	}
	if _, err := database.SavePoliticaGrant(grant); err != nil {
		t.Fatalf("SavePoliticaGrant ha fallat: %v", err)
	}
}

func TestModeracioBulkRequiresPermission(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_1_bulk_perm.sqlite3")

	admin := createTestUser(t, database, "admin_bulk_perm")
	assignPolicyByName(t, database, admin.ID, "admin")

	moderator := createTestUser(t, database, "moderator_bulk_perm")
	assignPolicyByName(t, database, moderator.ID, "moderador")

	session := createSessionCookie(t, database, moderator.ID, "sess_bulk_perm_mod")
	csrf := "csrf_bulk_perm_mod"

	form := newFormValues(map[string]string{
		"bulk_action": "approve",
		"bulk_scope":  "page",
		"selected":    "persona:1",
		"csrf_token":  csrf,
		"return_to":   "/moderacio",
	})
	req := httptest.NewRequest(http.MethodPost, "/moderacio/bulk", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()
	app.AdminModeracioBulk(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("bulk sense perm esperava 403, got %d", rr.Code)
	}
}

func TestModeracioBulkWithPermissionAllowed(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_1_bulk_ok.sqlite3")

	admin := createTestUser(t, database, "admin_bulk_ok")
	assignPolicyByName(t, database, admin.ID, "admin")

	moderator := createTestUser(t, database, "moderator_bulk_ok")
	assignPolicyByName(t, database, moderator.ID, "moderador")
	grantPolicyPermKey(t, database, "moderador", "moderacio.bulk")

	session := createSessionCookie(t, database, moderator.ID, "sess_bulk_ok")
	csrf := "csrf_bulk_ok"

	form := newFormValues(map[string]string{
		"bulk_action": "approve",
		"bulk_scope":  "page",
		"selected":    "persona:1",
		"csrf_token":  csrf,
		"return_to":   "/moderacio",
	})
	req := httptest.NewRequest(http.MethodPost, "/moderacio/bulk", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()
	app.AdminModeracioBulk(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("bulk amb perm esperava 303, got %d", rr.Code)
	}
}

func TestModeracioBulkJobOwnership(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_1_bulk_jobs.sqlite3")

	admin := createTestUser(t, database, "admin_bulk_job")
	assignPolicyByName(t, database, admin.ID, "admin")
	adminSession := createSessionCookie(t, database, admin.ID, "sess_bulk_job_admin")

	other := createTestUser(t, database, "moderator_bulk_job")
	assignPolicyByName(t, database, other.ID, "moderador")
	grantPolicyPermKey(t, database, "moderador", "moderacio.bulk")
	otherSession := createSessionCookie(t, database, other.ID, "sess_bulk_job_other")

	csrf := "csrf_bulk_job_admin"
	form := newFormValues(map[string]string{
		"bulk_action": "approve",
		"bulk_scope":  "all",
		"bulk_type":   "all",
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
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil || payload.JobID == "" {
		t.Fatalf("bulk async job_id invalid: %v", err)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/admin/control/moderacio/jobs/"+payload.JobID, nil)
	req.AddCookie(otherSession)
	rr = httptest.NewRecorder()
	app.AdminControlModeracioJobStatus(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("job status per altre usuari esperava 404, got %d", rr.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/admin/control/moderacio/jobs/"+payload.JobID, nil)
	req.AddCookie(adminSession)
	rr = httptest.NewRecorder()
	app.AdminControlModeracioJobStatus(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("job status admin esperava 200, got %d", rr.Code)
	}
}
