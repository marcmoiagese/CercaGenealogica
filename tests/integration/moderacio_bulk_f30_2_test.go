package integration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestModeracioSummaryLight(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_2_summary.sqlite3")

	admin := createTestUser(t, database, "admin_bulk_summary")
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_bulk_summary")

	arxiu := &db.Arxiu{
		Nom:            "Arxiu Summary",
		Tipus:          "Municipal",
		ModeracioEstat: "pendent",
	}
	if _, err := database.CreateArxiu(arxiu); err != nil {
		t.Fatalf("CreateArxiu ha fallat: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/admin/control/moderacio/summary", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminControlModeracioSummaryAPI(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("summary esperava 200, got %d", rr.Code)
	}
	var payload struct {
		Ok           bool   `json:"ok"`
		SummaryMode  string `json:"summary_mode"`
		SummaryScope string `json:"summary_scope"`
		Summary      struct {
			Total int `json:"total"`
		} `json:"summary"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("summary response invalid: %v", err)
	}
	if !payload.Ok {
		t.Fatalf("summary ok esperat true")
	}
	if payload.SummaryMode != "light" {
		t.Fatalf("summary_mode esperat light, got %s", payload.SummaryMode)
	}
	if payload.SummaryScope != "global" {
		t.Fatalf("summary_scope esperat global, got %s", payload.SummaryScope)
	}
	if payload.Summary.Total == 0 {
		t.Fatalf("summary total esperat > 0")
	}
}

func TestModeracioBulkAllUsesBulkUpdate(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_2_bulk_simple.sqlite3")

	admin := createTestUser(t, database, "admin_bulk_simple")
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_bulk_simple")

	arxiu := &db.Arxiu{
		Nom:            "Arxiu Bulk",
		Tipus:          "Parroquial",
		ModeracioEstat: "pendent",
	}
	if _, err := database.CreateArxiu(arxiu); err != nil {
		t.Fatalf("CreateArxiu ha fallat: %v", err)
	}

	csrf := "csrf_bulk_simple"
	form := newFormValues(map[string]string{
		"bulk_action": "approve",
		"bulk_scope":  "all",
		"bulk_type":   "arxiu",
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
		t.Fatalf("bulk all arxiu esperava 303, got %d", rr.Code)
	}

	updated, err := database.GetArxiu(arxiu.ID)
	if err != nil || updated == nil {
		t.Fatalf("GetArxiu ha fallat: %v", err)
	}
	if updated.ModeracioEstat != "publicat" {
		t.Fatalf("arxiu esperat publicat, got %s", updated.ModeracioEstat)
	}
	if !updated.ModeratedBy.Valid || int(updated.ModeratedBy.Int64) != admin.ID {
		t.Fatalf("moderated_by esperat %d", admin.ID)
	}
}
