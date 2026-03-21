package integration

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func addScopedGrantToPolicy(t *testing.T, database db.DB, policyID int, permKey string, scopeType core.ScopeType, scopeID int) {
	t.Helper()
	grant := &db.PoliticaGrant{
		PoliticaID:      policyID,
		PermKey:         permKey,
		ScopeType:       string(scopeType),
		ScopeID:         sql.NullInt64{Int64: int64(scopeID), Valid: scopeID > 0},
		IncludeChildren: false,
	}
	if _, err := database.SavePoliticaGrant(grant); err != nil {
		t.Fatalf("SavePoliticaGrant ha fallat: %v", err)
	}
}

func createPendingLlibre(t *testing.T, database db.DB, userID, municipiID, archID int, title string) int {
	t.Helper()
	llibre := &db.Llibre{
		ArquebisbatID:  archID,
		MunicipiID:     municipiID,
		Titol:          title,
		ModeracioEstat: "pendent",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	llibreID, err := database.CreateLlibre(llibre)
	if err != nil {
		t.Fatalf("CreateLlibre ha fallat: %v", err)
	}
	return llibreID
}

func createPendingArxiu(t *testing.T, database db.DB, userID, municipiID int, name string) int {
	t.Helper()
	arxiu := &db.Arxiu{
		Nom:            name,
		Tipus:          "parroquia",
		MunicipiID:     sql.NullInt64{Int64: int64(municipiID), Valid: true},
		ModeracioEstat: "pendent",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	arxiuID, err := database.CreateArxiu(arxiu)
	if err != nil {
		t.Fatalf("CreateArxiu ha fallat: %v", err)
	}
	return arxiuID
}

func createPendingRegistre(t *testing.T, database db.DB, userID, llibreID int) int {
	t.Helper()
	registre := &db.TranscripcioRaw{
		LlibreID:       llibreID,
		TipusActe:      "baptisme",
		ModeracioEstat: "pendent",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	registreID, err := database.CreateTranscripcioRaw(registre)
	if err != nil {
		t.Fatalf("CreateTranscripcioRaw ha fallat: %v", err)
	}
	return registreID
}

func TestModeracioSummaryScopedDocumentals(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_5_summary_doc_scoped.sqlite3")

	user := createNonAdminTestUser(t, database, "moderator_doc_scoped")
	munID := createHistoriaMunicipi(t, database, user.ID)
	archID := createTestArquebisbat(t, database, user.ID)

	arxiuAllowed := createPendingArxiu(t, database, user.ID, munID, "Arxiu Allowed")
	createPendingArxiu(t, database, user.ID, munID, "Arxiu Other")

	llibreAllowed := createPendingLlibre(t, database, user.ID, munID, archID, "Llibre Allowed")
	llibreOther := createPendingLlibre(t, database, user.ID, munID, archID, "Llibre Other")
	createPendingRegistre(t, database, user.ID, llibreAllowed)
	createPendingRegistre(t, database, user.ID, llibreOther)

	policyID := createPolicyWithScopedGrant(t, database, "doc_scope_summary", "documentals.llibres.edit", string(core.ScopeLlibre), llibreAllowed)
	addScopedGrantToPolicy(t, database, policyID, "documentals.registres.edit", core.ScopeLlibre, llibreAllowed)
	addScopedGrantToPolicy(t, database, policyID, "documentals.arxius.edit", core.ScopeArxiu, arxiuAllowed)
	assignPolicyToUser(t, database, user.ID, policyID)

	session := createSessionCookie(t, database, user.ID, "sess_doc_summary")
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
	if payload.Summary.Total != 3 {
		t.Fatalf("summary total esperat 3, got %d", payload.Summary.Total)
	}
	got := map[string]int{}
	for _, item := range payload.Summary.ByType {
		got[item.Type] = item.Total
	}
	if got["arxiu"] != 1 || got["llibre"] != 1 || got["registre"] != 1 || len(got) != 3 {
		t.Fatalf("summary by_type inesperat: %+v", got)
	}
}

func TestModeracioBulkScopedRegistres(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_5_bulk_registres_scoped.sqlite3")

	user := createNonAdminTestUser(t, database, "moderator_registre_scoped")
	munID := createHistoriaMunicipi(t, database, user.ID)
	archID := createTestArquebisbat(t, database, user.ID)

	llibreAllowed := createPendingLlibre(t, database, user.ID, munID, archID, "Llibre Allowed")
	llibreOther := createPendingLlibre(t, database, user.ID, munID, archID, "Llibre Other")
	regAllowed := createPendingRegistre(t, database, user.ID, llibreAllowed)
	regOther := createPendingRegistre(t, database, user.ID, llibreOther)

	policyID := createPolicyWithScopedGrant(t, database, "registre_scope_bulk", "documentals.registres.edit", string(core.ScopeLlibre), llibreAllowed)
	addGrantToPolicy(t, database, policyID, "moderacio.bulk")
	assignPolicyToUser(t, database, user.ID, policyID)

	session := createSessionCookie(t, database, user.ID, "sess_registre_bulk")
	csrf := "csrf_registre_bulk"

	form := newFormValues(map[string]string{
		"bulk_action": "approve",
		"bulk_scope":  "all",
		"bulk_type":   "registre",
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
		t.Fatalf("bulk registres esperava 303, got %d", rr.Code)
	}

	allowed, err := database.GetTranscripcioRaw(regAllowed)
	if err != nil || allowed == nil {
		t.Fatalf("GetTranscripcioRaw allowed ha fallat: %v", err)
	}
	other, err := database.GetTranscripcioRaw(regOther)
	if err != nil || other == nil {
		t.Fatalf("GetTranscripcioRaw other ha fallat: %v", err)
	}
	if allowed.ModeracioEstat != "publicat" {
		t.Fatalf("registre allowed esperat publicat, got %s", allowed.ModeracioEstat)
	}
	if other.ModeracioEstat != "pendent" {
		t.Fatalf("registre other esperat pendent, got %s", other.ModeracioEstat)
	}
}

func TestModeracioBulkPageScopedLlibre(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_5_bulk_page_llibres_scoped.sqlite3")

	user := createNonAdminTestUser(t, database, "moderator_llibre_scope_page")
	munID := createHistoriaMunicipi(t, database, user.ID)
	archID := createTestArquebisbat(t, database, user.ID)

	llibreAllowed := createPendingLlibre(t, database, user.ID, munID, archID, "Llibre Allowed")
	llibreOther := createPendingLlibre(t, database, user.ID, munID, archID, "Llibre Other")

	policyID := createPolicyWithScopedGrant(t, database, "llibre_scope_page", "documentals.llibres.edit", string(core.ScopeLlibre), llibreAllowed)
	addGrantToPolicy(t, database, policyID, "moderacio.bulk")
	assignPolicyToUser(t, database, user.ID, policyID)

	session := createSessionCookie(t, database, user.ID, "sess_llibre_page")
	csrf := "csrf_llibre_page"

	form := url.Values{}
	form.Set("bulk_action", "approve")
	form.Set("bulk_scope", "page")
	form.Set("bulk_type", "llibre")
	form.Set("csrf_token", csrf)
	form.Set("return_to", "/moderacio?type=llibre")
	form.Add("selected", fmt.Sprintf("llibre:%d", llibreAllowed))
	form.Add("selected", fmt.Sprintf("llibre:%d", llibreOther))

	req := httptest.NewRequest(http.MethodPost, "/moderacio/bulk", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()
	app.AdminModeracioBulk(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("bulk scope page esperava 303, got %d", rr.Code)
	}
	location := rr.Result().Header.Get("Location")
	if !strings.Contains(location, "err=1") {
		t.Fatalf("bulk scope page esperava err=1, got %s", location)
	}

	allowed, err := database.GetLlibre(llibreAllowed)
	if err != nil || allowed == nil {
		t.Fatalf("GetLlibre allowed ha fallat: %v", err)
	}
	other, err := database.GetLlibre(llibreOther)
	if err != nil || other == nil {
		t.Fatalf("GetLlibre other ha fallat: %v", err)
	}
	if allowed.ModeracioEstat != "publicat" {
		t.Fatalf("llibre allowed esperat publicat, got %s", allowed.ModeracioEstat)
	}
	if other.ModeracioEstat != "pendent" {
		t.Fatalf("llibre other esperat pendent, got %s", other.ModeracioEstat)
	}
}
