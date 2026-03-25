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

func createPendingCognomReferencia(t *testing.T, database db.DB, cognomID, userID int, url string) int {
	t.Helper()
	ref := &db.CognomReferencia{
		CognomID:       cognomID,
		Kind:           "url",
		URL:            url,
		Titol:          "Referencia test",
		Descripcio:     "",
		Pagina:         "",
		ModeracioEstat: "pendent",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	id, err := database.CreateCognomReferencia(ref)
	if err != nil {
		t.Fatalf("CreateCognomReferencia ha fallat: %v", err)
	}
	return id
}

func TestModeracioSummaryScopedIncludesCognomReferencia(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_9_summary_cognom_referencia.sqlite3")

	user := createNonAdminTestUser(t, database, "moderator_cognom_ref")
	policyID := createPolicyWithGrant(t, database, "moderator_cognom_ref_policy", "cognoms.moderate")
	assignPolicyToUser(t, database, user.ID, policyID)

	session := createSessionCookie(t, database, user.ID, "sess_summary_cognom_ref")

	cognomID, err := database.UpsertCognom("Roca", "roca", "", "", &user.ID)
	if err != nil {
		t.Fatalf("UpsertCognom ha fallat: %v", err)
	}
	createPendingCognomReferencia(t, database, cognomID, user.ID, "https://example.com/ref")

	req := httptest.NewRequest(http.MethodGet, "/api/admin/control/moderacio/summary", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminControlModeracioSummaryAPI(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("summary esperava 200, got %d", rr.Code)
	}

	var payload struct {
		Ok           bool   `json:"ok"`
		SummaryScope string `json:"summary_scope"`
		Summary      struct {
			ByType []struct {
				Type  string `json:"type"`
				Total int    `json:"total"`
			} `json:"by_type"`
		} `json:"summary"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("summary response invalid: %v", err)
	}
	if !payload.Ok {
		t.Fatalf("summary ok esperat true")
	}
	if payload.SummaryScope != "scoped" {
		t.Fatalf("summary_scope esperat scoped, got %s", payload.SummaryScope)
	}
	got := map[string]int{}
	for _, item := range payload.Summary.ByType {
		got[item.Type] = item.Total
	}
	if got["cognom_referencia"] != 1 {
		t.Fatalf("summary cognom_referencia esperat 1, got %d", got["cognom_referencia"])
	}
}

func TestModeracioBulkApprovesCognomReferencia(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_9_bulk_cognom_referencia.sqlite3")

	user := createNonAdminTestUser(t, database, "moderator_cognom_ref_bulk")
	policyID := createPolicyWithGrant(t, database, "moderator_cognom_ref_bulk_policy", "cognoms.moderate")
	addGrantToPolicy(t, database, policyID, "moderacio.bulk")
	assignPolicyToUser(t, database, user.ID, policyID)

	session := createSessionCookie(t, database, user.ID, "sess_bulk_cognom_ref")

	cognomID, err := database.UpsertCognom("Roca", "roca", "", "", &user.ID)
	if err != nil {
		t.Fatalf("UpsertCognom ha fallat: %v", err)
	}
	refID := createPendingCognomReferencia(t, database, cognomID, user.ID, "https://example.com/ref2")

	csrf := "csrf_bulk_cognom_ref"
	form := newFormValues(map[string]string{
		"bulk_action": "approve",
		"bulk_scope":  "all",
		"bulk_type":   "cognom_referencia",
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
		t.Fatalf("bulk cognom_referencia esperava 303, got %d", rr.Code)
	}

	refs, err := database.ListCognomReferencies(db.CognomReferenciaFilter{CognomID: cognomID, Status: "publicat"})
	if err != nil {
		t.Fatalf("ListCognomReferencies ha fallat: %v", err)
	}
	found := false
	for _, ref := range refs {
		if ref.ID == refID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("cognom_referencia esperat publicat per id %d", refID)
	}
}
