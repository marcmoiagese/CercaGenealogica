package integration

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestWikiMunicipiFlowApproveReject(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_wiki_municipi_flow.sqlite3")

	admin := createTestUser(t, database, "wiki_mun_admin_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_mun_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	paisID, nivellID := createTestPaisNivell(t, database, admin.ID)
	_ = paisID

	mun := &db.Municipi{
		Nom:            "Municipi Original",
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(admin.ID), Valid: true},
	}
	mun.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(nivellID), Valid: true}
	munID, err := database.CreateMunicipi(mun)
	if err != nil {
		t.Fatalf("CreateMunicipi ha fallat: %v", err)
	}

	form := url.Values{}
	form.Set("id", strconv.Itoa(munID))
	form.Set("nom", "Municipi Nou")
	form.Set("tipus", "municipi")
	form.Set("estat", "actiu")
	form.Set("nivell_administratiu_id_1", strconv.Itoa(nivellID))

	req := httptest.NewRequest(http.MethodPost, "/territori/municipis/save", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	rr := httptest.NewRecorder()

	app.AdminSaveMunicipi(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("save municipi expected 303, got %d", rr.Code)
	}

	munAfter, err := database.GetMunicipi(munID)
	if err != nil || munAfter == nil {
		t.Fatalf("GetMunicipi ha fallat: %v", err)
	}
	if munAfter.Nom != "Municipi Original" {
		t.Fatalf("el municipi publicat no s'hauria d'actualitzar, got %q", munAfter.Nom)
	}

	changes, err := database.ListWikiChanges("municipi", munID)
	if err != nil {
		t.Fatalf("ListWikiChanges ha fallat: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("esperava 1 canvi wiki, got %d", len(changes))
	}
	changeID := changes[0].ID
	if changes[0].ModeracioEstat != "pendent" {
		t.Fatalf("estat esperat pendent, got %s", changes[0].ModeracioEstat)
	}

	csrf := "csrf_mun_approve"
	form = url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("object_type", "municipi_canvi")
	req = httptest.NewRequest(http.MethodPost, "/moderacio/"+strconv.Itoa(changeID)+"/aprovar", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr = httptest.NewRecorder()

	app.AdminModeracioAprovar(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("approve expected 303, got %d", rr.Code)
	}

	munAfter, err = database.GetMunicipi(munID)
	if err != nil || munAfter == nil {
		t.Fatalf("GetMunicipi després d'aprovar ha fallat: %v", err)
	}
	if munAfter.Nom != "Municipi Nou" {
		t.Fatalf("esperava nom actualitzat, got %q", munAfter.Nom)
	}

	change, err := database.GetWikiChange(changeID)
	if err != nil || change == nil {
		t.Fatalf("GetWikiChange ha fallat: %v", err)
	}
	if change.ModeracioEstat != "publicat" {
		t.Fatalf("estat esperat publicat, got %s", change.ModeracioEstat)
	}

	form = url.Values{}
	form.Set("id", strconv.Itoa(munID))
	form.Set("nom", "Municipi Rebutjat")
	form.Set("tipus", "municipi")
	form.Set("estat", "actiu")
	form.Set("nivell_administratiu_id_1", strconv.Itoa(nivellID))
	req = httptest.NewRequest(http.MethodPost, "/territori/municipis/save", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	rr = httptest.NewRecorder()

	app.AdminSaveMunicipi(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("save municipi reject expected 303, got %d", rr.Code)
	}

	changes, err = database.ListWikiChanges("municipi", munID)
	if err != nil {
		t.Fatalf("ListWikiChanges ha fallat: %v", err)
	}
	if len(changes) != 2 {
		t.Fatalf("esperava 2 canvis wiki, got %d", len(changes))
	}
	changeID2 := changes[0].ID
	if changeID2 == changeID {
		changeID2 = changes[1].ID
	}

	csrf = "csrf_mun_reject"
	form = url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("object_type", "municipi_canvi")
	form.Set("reason", "no")
	req = httptest.NewRequest(http.MethodPost, "/moderacio/"+strconv.Itoa(changeID2)+"/rebutjar", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr = httptest.NewRecorder()

	app.AdminModeracioRebutjar(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("reject expected 303, got %d", rr.Code)
	}

	munAfter, err = database.GetMunicipi(munID)
	if err != nil || munAfter == nil {
		t.Fatalf("GetMunicipi després de rebutjar ha fallat: %v", err)
	}
	if munAfter.Nom != "Municipi Nou" {
		t.Fatalf("el municipi no hauria de canviar en rebutjar, got %q", munAfter.Nom)
	}

	change, err = database.GetWikiChange(changeID2)
	if err != nil || change == nil {
		t.Fatalf("GetWikiChange rebutjat ha fallat: %v", err)
	}
	if change.ModeracioEstat != "rebutjat" {
		t.Fatalf("estat esperat rebutjat, got %s", change.ModeracioEstat)
	}
}

func createTestPaisNivell(t *testing.T, database db.DB, userID int) (int, int) {
	t.Helper()
	pais := &db.Pais{
		CodiISO2:    "TS",
		CodiISO3:    "TST",
		CodiPaisNum: "999",
	}
	paisID, err := database.CreatePais(pais)
	if err != nil {
		t.Fatalf("CreatePais ha fallat: %v", err)
	}

	nivell := &db.NivellAdministratiu{
		PaisID:         paisID,
		Nivel:          1,
		NomNivell:      "Test",
		TipusNivell:    "pais",
		CodiOficial:    "TS-1",
		Altres:         "",
		Estat:          "actiu",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	nivellID, err := database.CreateNivell(nivell)
	if err != nil {
		t.Fatalf("CreateNivell ha fallat: %v", err)
	}
	return paisID, nivellID
}
