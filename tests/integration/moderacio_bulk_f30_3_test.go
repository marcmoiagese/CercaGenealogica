package integration

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func createPolicyWithScopedGrant(t *testing.T, database db.DB, name, permKey string, scopeType string, scopeID int) int {
	t.Helper()
	policy := &db.Politica{
		Nom:        name,
		Descripcio: "test scoped policy",
	}
	policyID, err := database.SavePolitica(policy)
	if err != nil {
		t.Fatalf("SavePolitica ha fallat: %v", err)
	}
	grant := &db.PoliticaGrant{
		PoliticaID:      policyID,
		PermKey:         permKey,
		ScopeType:       scopeType,
		ScopeID:         sql.NullInt64{Int64: int64(scopeID), Valid: scopeID > 0},
		IncludeChildren: false,
	}
	if _, err := database.SavePoliticaGrant(grant); err != nil {
		t.Fatalf("SavePoliticaGrant ha fallat: %v", err)
	}
	return policyID
}

func addGrantToPolicy(t *testing.T, database db.DB, policyID int, permKey string) {
	t.Helper()
	grant := &db.PoliticaGrant{
		PoliticaID:      policyID,
		PermKey:         permKey,
		ScopeType:       string(core.ScopeGlobal),
		ScopeID:         sql.NullInt64{},
		IncludeChildren: false,
	}
	if _, err := database.SavePoliticaGrant(grant); err != nil {
		t.Fatalf("SavePoliticaGrant ha fallat: %v", err)
	}
}

func createPendingHistoriaGeneralVersion(t *testing.T, database db.DB, userID, municipiID int) int {
	t.Helper()
	hist, err := database.EnsureMunicipiHistoria(municipiID)
	if err != nil || hist == nil {
		t.Fatalf("EnsureMunicipiHistoria ha fallat: %v", err)
	}
	versionID, err := database.CreateMunicipiHistoriaGeneralDraft(hist.ID, userID, false)
	if err != nil || versionID <= 0 {
		t.Fatalf("CreateMunicipiHistoriaGeneralDraft ha fallat: %v", err)
	}
	if err := database.SetMunicipiHistoriaGeneralStatus(versionID, "pendent", "", nil); err != nil {
		t.Fatalf("SetMunicipiHistoriaGeneralStatus ha fallat: %v", err)
	}
	return versionID
}

func TestModeracioSummaryRespectsScope(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_3_summary_scoped.sqlite3")

	user := createTestUser(t, database, "moderator_scope_summary")
	munAllowed := createHistoriaMunicipi(t, database, user.ID)
	munOther := createHistoriaMunicipi(t, database, user.ID)

	createPendingHistoriaGeneralVersion(t, database, user.ID, munAllowed)
	createPendingHistoriaGeneralVersion(t, database, user.ID, munOther)

	policyID := createPolicyWithScopedGrant(t, database, "historia_scope_summary", "municipis.historia.moderate", string(core.ScopeMunicipi), munAllowed)
	assignPolicyToUser(t, database, user.ID, policyID)

	session := createSessionCookie(t, database, user.ID, "sess_scope_summary")
	req := httptest.NewRequest(http.MethodGet, "/api/admin/control/moderacio/summary", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminControlModeracioSummaryAPI(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("summary esperava 200, got %d", rr.Code)
	}

	var payload struct {
		Ok           bool `json:"ok"`
		SummaryScope string `json:"summary_scope"`
		Summary      struct {
			Total  int `json:"total"`
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
	if payload.Summary.Total != 1 {
		t.Fatalf("summary total esperat 1, got %d", payload.Summary.Total)
	}
	if len(payload.Summary.ByType) != 1 || payload.Summary.ByType[0].Type != "municipi_historia_general" || payload.Summary.ByType[0].Total != 1 {
		t.Fatalf("summary by_type inesperat: %+v", payload.Summary.ByType)
	}
}

func TestModeracioBulkScopedHistoria(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_3_bulk_scoped.sqlite3")

	user := createTestUser(t, database, "moderator_scope_bulk")
	munAllowed := createHistoriaMunicipi(t, database, user.ID)
	munOther := createHistoriaMunicipi(t, database, user.ID)

	versionAllowed := createPendingHistoriaGeneralVersion(t, database, user.ID, munAllowed)
	versionOther := createPendingHistoriaGeneralVersion(t, database, user.ID, munOther)

	policyID := createPolicyWithScopedGrant(t, database, "historia_scope_bulk", "municipis.historia.moderate", string(core.ScopeMunicipi), munAllowed)
	addGrantToPolicy(t, database, policyID, "moderacio.bulk")
	assignPolicyToUser(t, database, user.ID, policyID)

	session := createSessionCookie(t, database, user.ID, "sess_scope_bulk")
	csrf := "csrf_scope_bulk"

	form := newFormValues(map[string]string{
		"bulk_action": "approve",
		"bulk_scope":  "all",
		"bulk_type":   "municipi_historia_general",
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
		t.Fatalf("bulk scoped esperava 303, got %d", rr.Code)
	}

	allowed, err := database.GetMunicipiHistoriaGeneralVersion(versionAllowed)
	if err != nil || allowed == nil {
		t.Fatalf("GetMunicipiHistoriaGeneralVersion allowed ha fallat: %v", err)
	}
	other, err := database.GetMunicipiHistoriaGeneralVersion(versionOther)
	if err != nil || other == nil {
		t.Fatalf("GetMunicipiHistoriaGeneralVersion other ha fallat: %v", err)
	}
	if allowed.Status != "publicat" {
		t.Fatalf("version allowed esperat publicat, got %s", allowed.Status)
	}
	if other.Status != "pendent" {
		t.Fatalf("version other esperat pendent, got %s", other.Status)
	}
}

func TestModeracioBulkRejectsOutOfScopeType(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_3_bulk_reject.sqlite3")

	user := createTestUser(t, database, "moderator_scope_reject")
	munAllowed := createHistoriaMunicipi(t, database, user.ID)
	createPendingHistoriaGeneralVersion(t, database, user.ID, munAllowed)

	policyID := createPolicyWithScopedGrant(t, database, "historia_scope_reject", "municipis.historia.moderate", string(core.ScopeMunicipi), munAllowed)
	addGrantToPolicy(t, database, policyID, "moderacio.bulk")
	assignPolicyToUser(t, database, user.ID, policyID)

	arxiu := &db.Arxiu{
		Nom:            "Arxiu bulk out of scope",
		Tipus:          "Municipal",
		ModeracioEstat: "pendent",
	}
	if _, err := database.CreateArxiu(arxiu); err != nil {
		t.Fatalf("CreateArxiu ha fallat: %v", err)
	}

	session := createSessionCookie(t, database, user.ID, "sess_scope_reject")
	csrf := "csrf_scope_reject"

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
		t.Fatalf("bulk out of scope esperava 303, got %d", rr.Code)
	}
	location := rr.Result().Header.Get("Location")
	if !strings.Contains(location, "err=1") {
		t.Fatalf("bulk out of scope esperava err=1, got %s", location)
	}

	updated, err := database.GetArxiu(arxiu.ID)
	if err != nil || updated == nil {
		t.Fatalf("GetArxiu ha fallat: %v", err)
	}
	if updated.ModeracioEstat != "pendent" {
		t.Fatalf("arxiu esperat pendent, got %s", updated.ModeracioEstat)
	}
}
