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

	"github.com/marcmoiagese/CercaGenealogica/core"
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

func TestF336WikiMunicipiScopedRevertStaysInsideScope(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f33_6_wiki_municipi_scoped.sqlite3")

	admin := createTestUser(t, database, "wiki_mun_f336_admin_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	assignPolicyByName(t, database, admin.ID, "admin")
	adminSession := createSessionCookie(t, database, admin.ID, "sess_mun_f336_admin_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	_, nivellID := createTestPaisNivell(t, database, admin.ID)
	allowedMunID := createF336WikiMunicipi(t, database, admin.ID, nivellID, "Municipi F33-6 allowed")
	blockedMunID := createF336WikiMunicipi(t, database, admin.ID, nivellID, "Municipi F33-6 blocked")
	allowedChangeID := submitF336MunicipiUpdate(t, app, adminSession, allowedMunID, nivellID, "Municipi F33-6 allowed v2")
	blockedChangeID := submitF336MunicipiUpdate(t, app, adminSession, blockedMunID, nivellID, "Municipi F33-6 blocked v2")

	moderator := createTestUser(t, database, "wiki_mun_f336_mod_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	policyID := createPolicyWithScopedGrant(t, database, "f33_6_wiki_municipi_scoped", "territori.municipis.edit", "municipi", allowedMunID)
	addGrantToPolicy(t, database, policyID, "wiki.revert")
	if err := database.AddUserPolitica(moderator.ID, policyID); err != nil {
		t.Fatalf("AddUserPolitica scoped wiki ha fallat: %v", err)
	}
	moderatorSession := createSessionCookie(t, database, moderator.ID, "sess_mun_f336_mod_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	csrf := "csrf_f33_6_allowed"
	form := url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("change_id", strconv.Itoa(allowedChangeID))
	form.Set("reason", "revert scoped")
	req := httptest.NewRequest(http.MethodPost, "/territori/municipis/"+strconv.Itoa(allowedMunID)+"/historial/revert", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(moderatorSession)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()
	app.MunicipiWikiRevert(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("revert scoped dins ambit esperava 303, got %d body=%s", rr.Code, rr.Body.String())
	}

	csrf = "csrf_f33_6_blocked"
	form = url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("change_id", strconv.Itoa(blockedChangeID))
	form.Set("reason", "revert scoped out")
	req = httptest.NewRequest(http.MethodPost, "/territori/municipis/"+strconv.Itoa(blockedMunID)+"/historial/revert", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(moderatorSession)
	req.AddCookie(csrfCookie(csrf))
	rr = httptest.NewRecorder()
	app.MunicipiWikiRevert(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("revert scoped fora ambit esperava 403, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func createF336WikiMunicipi(t *testing.T, database db.DB, userID, nivellID int, nom string) int {
	t.Helper()
	mun := &db.Municipi{
		Nom:            nom,
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	mun.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(nivellID), Valid: true}
	munID, err := database.CreateMunicipi(mun)
	if err != nil {
		t.Fatalf("CreateMunicipi F33-6 ha fallat: %v", err)
	}
	return munID
}

func submitF336MunicipiUpdate(t *testing.T, app *core.App, session *http.Cookie, munID, nivellID int, nom string) int {
	t.Helper()
	form := url.Values{}
	form.Set("id", strconv.Itoa(munID))
	form.Set("nom", nom)
	form.Set("tipus", "municipi")
	form.Set("estat", "actiu")
	form.Set("nivell_administratiu_id_1", strconv.Itoa(nivellID))
	req := httptest.NewRequest(http.MethodPost, "/territori/municipis/save", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminSaveMunicipi(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("save municipi F33-6 expected 303, got %d body=%s", rr.Code, rr.Body.String())
	}
	changes, err := app.DB.ListWikiChanges("municipi", munID)
	if err != nil {
		t.Fatalf("ListWikiChanges F33-6 ha fallat: %v", err)
	}
	for _, ch := range changes {
		if ch.ModeracioEstat == "pendent" {
			return ch.ID
		}
	}
	t.Fatalf("no s'ha creat canvi wiki pendent F33-6")
	return 0
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
