package integration

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestWikiCognomFlowAndStats(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_wiki_cognom_flow.sqlite3")

	user := createTestUser(t, database, "wiki_cognom_user_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	userSession := createSessionCookie(t, database, user.ID, "sess_cognom_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	moderator := createTestUser(t, database, "wiki_cognom_mod_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	assignPolicyByName(t, database, moderator.ID, "admin")
	moderatorSession := createSessionCookie(t, database, moderator.ID, "sess_cognom_mod_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	cognomID, err := database.UpsertCognom("Garcia", "garcia", "", "", &user.ID)
	if err != nil {
		t.Fatalf("UpsertCognom ha fallat: %v", err)
	}

	csrf := "csrf_cognom_propose"
	form := url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("origen", "Prova")
	form.Set("notes", "Notes")
	req := httptest.NewRequest(http.MethodPost, "/cognoms/"+strconv.Itoa(cognomID)+"/propose", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(userSession)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()

	app.CognomProposeUpdate(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("propose expected 303, got %d", rr.Code)
	}

	cognom, err := database.GetCognom(cognomID)
	if err != nil || cognom == nil {
		t.Fatalf("GetCognom ha fallat: %v", err)
	}
	if cognom.Origen != "" {
		t.Fatalf("el cognom publicat no s'hauria d'actualitzar, got origen=%q", cognom.Origen)
	}

	changes, err := database.ListWikiChanges("cognom", cognomID)
	if err != nil {
		t.Fatalf("ListWikiChanges ha fallat: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("esperava 1 canvi, got %d", len(changes))
	}
	changeID := changes[0].ID

	approveWikiChange(t, app, moderatorSession, changeID, "cognom_canvi")

	cognom, _ = database.GetCognom(cognomID)
	if cognom == nil || cognom.Origen != "Prova" {
		t.Fatalf("esperava cognom actualitzat, got origen=%q", cognom.Origen)
	}

	csrf = "csrf_cognom_reject"
	form = url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("origen", "Rebutjat")
	form.Set("notes", "No")
	req = httptest.NewRequest(http.MethodPost, "/cognoms/"+strconv.Itoa(cognomID)+"/propose", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(userSession)
	req.AddCookie(csrfCookie(csrf))
	rr = httptest.NewRecorder()

	app.CognomProposeUpdate(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("propose reject expected 303, got %d", rr.Code)
	}

	changes, _ = database.ListWikiChanges("cognom", cognomID)
	changeID2 := changes[0].ID
	if changeID2 == changeID && len(changes) > 1 {
		changeID2 = changes[1].ID
	}
	rejectWikiChange(t, app, moderatorSession, changeID2, "cognom_canvi")

	cognom, _ = database.GetCognom(cognomID)
	if cognom == nil || cognom.Origen != "Prova" {
		t.Fatalf("el cognom no hauria de canviar en rebutjar, got origen=%q", cognom.Origen)
	}

	missingCSRFReq := httptest.NewRequest(http.MethodPost, "/cognoms/"+strconv.Itoa(cognomID)+"/propose", strings.NewReader(form.Encode()))
	missingCSRFReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	missingCSRFReq.AddCookie(userSession)
	missingCSRFRec := httptest.NewRecorder()
	beforeCount := countRows(t, database, "SELECT COUNT(*) AS n FROM wiki_canvis WHERE object_type = ? AND object_id = ?", "cognom", cognomID)
	app.CognomProposeUpdate(missingCSRFRec, missingCSRFReq)
	if missingCSRFRec.Code != http.StatusSeeOther {
		t.Fatalf("missing CSRF expected 303, got %d", missingCSRFRec.Code)
	}
	afterCount := countRows(t, database, "SELECT COUNT(*) AS n FROM wiki_canvis WHERE object_type = ? AND object_id = ?", "cognom", cognomID)
	if afterCount != beforeCount {
		t.Fatalf("no s'hauria de crear canvi sense CSRF")
	}

	markWithoutCSRF := httptest.NewRequest(http.MethodPost, "/cognoms/"+strconv.Itoa(cognomID)+"/seguir", strings.NewReader(""))
	markWithoutCSRF.AddCookie(userSession)
	markRR := httptest.NewRecorder()
	app.CognomWikiMark(markRR, markWithoutCSRF)
	if markRR.Code != http.StatusBadRequest {
		t.Fatalf("mark sense CSRF expected 400, got %d", markRR.Code)
	}

	csrf = "csrf_cognom_private_first"
	form = url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("type", "interes")
	form.Set("public", "0")
	req = httptest.NewRequest(http.MethodPost, "/cognoms/"+strconv.Itoa(cognomID)+"/seguir", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(userSession)
	req.AddCookie(csrfCookie(csrf))
	rr = httptest.NewRecorder()

	app.CognomWikiMark(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("mark private first expected 200, got %d", rr.Code)
	}
	counts, err := database.GetWikiPublicCounts("cognom", cognomID)
	if err != nil {
		t.Fatalf("GetWikiPublicCounts ha fallat: %v", err)
	}
	if counts["interes"] != 0 {
		t.Fatalf("esperava 0 seguidors publics després del mark privat, got %d", counts["interes"])
	}

	csrf = "csrf_cognom_mark"
	form = url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("type", "interes")
	form.Set("public", "1")
	req = httptest.NewRequest(http.MethodPost, "/cognoms/"+strconv.Itoa(cognomID)+"/seguir", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(userSession)
	req.AddCookie(csrfCookie(csrf))
	rr = httptest.NewRecorder()

	app.CognomWikiMark(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("mark expected 200, got %d", rr.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/cognoms/"+strconv.Itoa(cognomID)+"/seguir", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(userSession)
	req.AddCookie(csrfCookie(csrf))
	rr = httptest.NewRecorder()

	app.CognomWikiMark(rr, req)
	count := countRows(t, database, "SELECT COUNT(*) AS n FROM wiki_marques WHERE object_type = ? AND object_id = ? AND user_id = ?", "cognom", cognomID, user.ID)
	if count != 1 {
		t.Fatalf("esperava 1 marca, got %d", count)
	}

	counts, err = database.GetWikiPublicCounts("cognom", cognomID)
	if err != nil {
		t.Fatalf("GetWikiPublicCounts ha fallat: %v", err)
	}
	if counts["interes"] != 1 {
		t.Fatalf("esperava 1 seguidor public, got %d", counts["interes"])
	}

	csrf = "csrf_cognom_private"
	form = url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("type", "interes")
	form.Set("public", "0")
	req = httptest.NewRequest(http.MethodPost, "/cognoms/"+strconv.Itoa(cognomID)+"/seguir", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(userSession)
	req.AddCookie(csrfCookie(csrf))
	rr = httptest.NewRecorder()

	app.CognomWikiMark(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("mark private expected 200, got %d", rr.Code)
	}

	counts, err = database.GetWikiPublicCounts("cognom", cognomID)
	if err != nil {
		t.Fatalf("GetWikiPublicCounts ha fallat: %v", err)
	}
	if counts["interes"] != 0 {
		t.Fatalf("esperava 0 seguidors publics, got %d", counts["interes"])
	}

	csrf = "csrf_cognom_unmark"
	form = url.Values{}
	form.Set("csrf_token", csrf)
	req = httptest.NewRequest(http.MethodPost, "/cognoms/"+strconv.Itoa(cognomID)+"/desseguir", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(userSession)
	req.AddCookie(csrfCookie(csrf))
	rr = httptest.NewRecorder()

	app.CognomWikiUnmark(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("unmark expected 200, got %d", rr.Code)
	}
	req = httptest.NewRequest(http.MethodPost, "/cognoms/"+strconv.Itoa(cognomID)+"/desseguir", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(userSession)
	req.AddCookie(csrfCookie(csrf))
	rr = httptest.NewRecorder()

	app.CognomWikiUnmark(rr, req)
	count = countRows(t, database, "SELECT COUNT(*) AS n FROM wiki_marques WHERE object_type = ? AND object_id = ? AND user_id = ?", "cognom", cognomID, user.ID)
	if count != 0 {
		t.Fatalf("esperava 0 marques després d'esborrar, got %d", count)
	}
	counts, err = database.GetWikiPublicCounts("cognom", cognomID)
	if err != nil {
		t.Fatalf("GetWikiPublicCounts ha fallat: %v", err)
	}
	if counts["interes"] != 0 {
		t.Fatalf("esperava 0 seguidors publics després d'esborrar, got %d", counts["interes"])
	}
}
