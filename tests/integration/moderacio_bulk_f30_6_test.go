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
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func createPendingMunicipi(t *testing.T, database db.DB, userID int, name string) int {
	t.Helper()
	mun := &db.Municipi{
		Nom:            name,
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	munID, err := database.CreateMunicipi(mun)
	if err != nil {
		t.Fatalf("CreateMunicipi ha fallat: %v", err)
	}
	return munID
}

func createPais(t *testing.T, database db.DB, iso2, iso3, num string) int {
	t.Helper()
	pais := &db.Pais{
		CodiISO2:    iso2,
		CodiISO3:    iso3,
		CodiPaisNum: num,
	}
	paisID, err := database.CreatePais(pais)
	if err != nil {
		t.Fatalf("CreatePais ha fallat: %v", err)
	}
	return paisID
}

func createPendingNivell(t *testing.T, database db.DB, userID, paisID int, name string) int {
	t.Helper()
	nivell := &db.NivellAdministratiu{
		PaisID:         paisID,
		Nivel:          1,
		NomNivell:      name,
		TipusNivell:    "pais",
		CodiOficial:    fmt.Sprintf("TS-%d", time.Now().UnixNano()),
		Altres:         "",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	nivellID, err := database.CreateNivell(nivell)
	if err != nil {
		t.Fatalf("CreateNivell ha fallat: %v", err)
	}
	return nivellID
}

func createPendingArquebisbat(t *testing.T, database db.DB, userID, paisID int, name string) int {
	t.Helper()
	arch := &db.Arquebisbat{
		Nom:            name,
		TipusEntitat:   "bisbat",
		PaisID:         sql.NullInt64{Int64: int64(paisID), Valid: true},
		ModeracioEstat: "pendent",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	id, err := database.CreateArquebisbat(arch)
	if err != nil {
		t.Fatalf("CreateArquebisbat ha fallat: %v", err)
	}
	return id
}

func createPendingEventHistoric(t *testing.T, database db.DB, userID int, title string) int {
	t.Helper()
	event := &db.EventHistoric{
		Titol:            title,
		Slug:             fmt.Sprintf("event-%d", time.Now().UnixNano()),
		Tipus:            "altres",
		DataInici:        "1900-01-01",
		ModerationStatus: "pendent",
		CreatedBy:        sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	eventID, err := database.CreateEventHistoric(event)
	if err != nil {
		t.Fatalf("CreateEventHistoric ha fallat: %v", err)
	}
	return eventID
}

func TestModeracioSummaryScopedTerritoriEclesiastic(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_6_summary_territori.sqlite3")

	user := createNonAdminTestUser(t, database, "moderator_territori_scoped")

	munAllowed := createPendingMunicipi(t, database, user.ID, "Municipi Allowed")
	createPendingMunicipi(t, database, user.ID, "Municipi Other")

	paisAllowed := createPais(t, database, "TA", "TAA", "901")
	paisOther := createPais(t, database, "TB", "TBB", "902")
	createPendingNivell(t, database, user.ID, paisOther, "Nivell Other")
	createPendingNivell(t, database, user.ID, paisAllowed, "Nivell Allowed")

	eclesAllowed := createPendingArquebisbat(t, database, user.ID, paisAllowed, "Bisbat Allowed")
	createPendingArquebisbat(t, database, user.ID, paisOther, "Bisbat Other")

	policyID := createPolicyWithScopedGrant(t, database, "territori_scope_summary", "territori.municipis.edit", string(core.ScopeMunicipi), munAllowed)
	addScopedGrantToPolicy(t, database, policyID, "territori.nivells.edit", core.ScopePais, paisAllowed)
	addScopedGrantToPolicy(t, database, policyID, "territori.eclesiastic.edit", core.ScopeEcles, eclesAllowed)
	assignPolicyToUser(t, database, user.ID, policyID)

	session := createSessionCookie(t, database, user.ID, "sess_territori_summary")
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
	if got["municipi"] != 1 || got["nivell"] != 1 || got["eclesiastic"] != 1 || len(got) != 3 {
		t.Fatalf("summary by_type inesperat: %+v", got)
	}
}

func TestModeracioBulkScopedMunicipiPageMixed(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_6_bulk_municipi_page.sqlite3")

	user := createNonAdminTestUser(t, database, "moderator_mun_scope_page")
	munAllowed := createPendingMunicipi(t, database, user.ID, "Municipi Allowed")
	munOther := createPendingMunicipi(t, database, user.ID, "Municipi Other")

	policyID := createPolicyWithScopedGrant(t, database, "mun_scope_page", "territori.municipis.edit", string(core.ScopeMunicipi), munAllowed)
	addGrantToPolicy(t, database, policyID, "moderacio.bulk")
	assignPolicyToUser(t, database, user.ID, policyID)

	session := createSessionCookie(t, database, user.ID, "sess_mun_page")
	csrf := "csrf_mun_page"

	form := url.Values{}
	form.Set("bulk_action", "approve")
	form.Set("bulk_scope", "page")
	form.Set("bulk_type", "municipi")
	form.Set("csrf_token", csrf)
	form.Set("return_to", "/moderacio?type=municipi")
	form.Add("selected", fmt.Sprintf("municipi:%d", munAllowed))
	form.Add("selected", fmt.Sprintf("municipi:%d", munOther))

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

	allowed, err := database.GetMunicipi(munAllowed)
	if err != nil || allowed == nil {
		t.Fatalf("GetMunicipi allowed ha fallat: %v", err)
	}
	other, err := database.GetMunicipi(munOther)
	if err != nil || other == nil {
		t.Fatalf("GetMunicipi other ha fallat: %v", err)
	}
	if allowed.ModeracioEstat != "publicat" {
		t.Fatalf("municipi allowed esperat publicat, got %s", allowed.ModeracioEstat)
	}
	if other.ModeracioEstat != "pendent" {
		t.Fatalf("municipi other esperat pendent, got %s", other.ModeracioEstat)
	}
}

func TestModeracioBulkScopedEclesiasticAll(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_6_bulk_ecles_all.sqlite3")

	user := createNonAdminTestUser(t, database, "moderator_ecles_scope")
	paisAllowed := createPais(t, database, "EC", "ECL", "911")
	paisOther := createPais(t, database, "ED", "ECD", "912")
	eclesAllowed := createPendingArquebisbat(t, database, user.ID, paisAllowed, "Bisbat Allowed")
	eclesOther := createPendingArquebisbat(t, database, user.ID, paisOther, "Bisbat Other")

	policyID := createPolicyWithScopedGrant(t, database, "ecles_scope_all", "territori.eclesiastic.edit", string(core.ScopeEcles), eclesAllowed)
	addGrantToPolicy(t, database, policyID, "moderacio.bulk")
	assignPolicyToUser(t, database, user.ID, policyID)

	session := createSessionCookie(t, database, user.ID, "sess_ecles_all")
	csrf := "csrf_ecles_all"

	form := newFormValues(map[string]string{
		"bulk_action": "approve",
		"bulk_scope":  "all",
		"bulk_type":   "eclesiastic",
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
		t.Fatalf("bulk eclesiastic esperava 303, got %d", rr.Code)
	}

	allowed, err := database.GetArquebisbat(eclesAllowed)
	if err != nil || allowed == nil {
		t.Fatalf("GetArquebisbat allowed ha fallat: %v", err)
	}
	other, err := database.GetArquebisbat(eclesOther)
	if err != nil || other == nil {
		t.Fatalf("GetArquebisbat other ha fallat: %v", err)
	}
	if allowed.ModeracioEstat != "publicat" {
		t.Fatalf("eclesiastic allowed esperat publicat, got %s", allowed.ModeracioEstat)
	}
	if other.ModeracioEstat != "pendent" {
		t.Fatalf("eclesiastic other esperat pendent, got %s", other.ModeracioEstat)
	}
}

func TestModeracioBulkAdminEventHistoricAll(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_6_bulk_event_admin.sqlite3")

	admin := createTestUser(t, database, "admin_event_bulk")
	assignPolicyByName(t, database, admin.ID, "admin")

	eventID := createPendingEventHistoric(t, database, admin.ID, "Event Historic Pending")

	session := createSessionCookie(t, database, admin.ID, "sess_event_bulk")
	csrf := "csrf_event_bulk"

	form := newFormValues(map[string]string{
		"bulk_action": "approve",
		"bulk_scope":  "all",
		"bulk_type":   "event_historic",
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
		t.Fatalf("bulk event historic esperava 303, got %d", rr.Code)
	}

	event, err := database.GetEventHistoric(eventID)
	if err != nil || event == nil {
		t.Fatalf("GetEventHistoric ha fallat: %v", err)
	}
	if event.ModerationStatus != "publicat" {
		t.Fatalf("event historic esperat publicat, got %s", event.ModerationStatus)
	}
}
