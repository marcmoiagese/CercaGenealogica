package integration

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func newWikiGuardrailsApp(t *testing.T, overrides map[string]string) (*core.App, db.DB) {
	t.Helper()
	cfg := map[string]string{
		"DB_ENGINE": "sqlite",
		"DB_PATH":   filepath.Join(t.TempDir(), "test_wiki_guardrails.sqlite3"),
		"RECREADB":  "true",
		"LOG_LEVEL": "silent",
	}
	for k, v := range overrides {
		cfg[k] = v
	}
	return newTestAppForConfig(t, cfg)
}

func createTestCognom(t *testing.T, database db.DB, userID int) int {
	t.Helper()
	key := fmt.Sprintf("cognom_test_%d", time.Now().UnixNano())
	id, err := database.UpsertCognom("Cognom "+key, key, "", "", &userID)
	if err != nil || id == 0 {
		t.Fatalf("UpsertCognom ha fallat: %v", err)
	}
	return id
}

func postCognomPropose(t *testing.T, app *core.App, session *http.Cookie, csrfToken string, cognomID int, origen, notes string) *httptest.ResponseRecorder {
	t.Helper()
	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("origen", origen)
	form.Set("notes", notes)
	path := fmt.Sprintf("/cognoms/%d/proposar", cognomID)
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfToken))
	req.RemoteAddr = "127.0.0.1:9201"
	rr := httptest.NewRecorder()
	app.CognomProposeUpdate(rr, req)
	return rr
}

func postCognomMark(t *testing.T, app *core.App, session *http.Cookie, csrfToken string, cognomID int) *httptest.ResponseRecorder {
	t.Helper()
	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("type", "interes")
	form.Set("public", "1")
	path := fmt.Sprintf("/cognoms/%d/marcar", cognomID)
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfToken))
	req.RemoteAddr = "127.0.0.1:9202"
	rr := httptest.NewRecorder()
	app.CognomWikiMark(rr, req)
	return rr
}

func TestWikiGuardrailsMetadataTooLarge(t *testing.T) {
	app, database := newWikiGuardrailsApp(t, map[string]string{
		"WIKI_META_MAX_BYTES": "300",
	})
	user := createTestUser(t, database, "wiki_meta_user")
	cognomID := createTestCognom(t, database, user.ID)

	session := createSessionCookie(t, database, user.ID, "sess-wiki-meta")
	csrfToken := "csrf-wiki-meta"

	hugeNotes := strings.Repeat("a", 1000)
	rr := postCognomPropose(t, app, session, csrfToken, cognomID, "origen", hugeNotes)
	if rr.Result().StatusCode != http.StatusBadRequest {
		t.Fatalf("esperava 400 per metadata massa gran, rebut %d", rr.Result().StatusCode)
	}
}

func TestWikiGuardrailsPendingPerUser(t *testing.T) {
	app, database := newWikiGuardrailsApp(t, map[string]string{
		"WIKI_PENDING_PER_USER":   "1",
		"WIKI_PENDING_PER_OBJECT": "10",
		"WIKI_META_MAX_BYTES":     "50000",
	})
	user := createTestUser(t, database, "wiki_pending_user")
	cognomID := createTestCognom(t, database, user.ID)

	session := createSessionCookie(t, database, user.ID, "sess-wiki-pending-user")
	csrfToken := "csrf-wiki-pending-user"

	rr := postCognomPropose(t, app, session, csrfToken, cognomID, "origen", "notes")
	if rr.Result().StatusCode != http.StatusSeeOther {
		t.Fatalf("esperava 303 a la primera proposta, rebut %d", rr.Result().StatusCode)
	}
	rr = postCognomPropose(t, app, session, csrfToken, cognomID, "origen2", "notes2")
	if rr.Result().StatusCode != http.StatusBadRequest {
		t.Fatalf("esperava 400 per límit per usuari, rebut %d", rr.Result().StatusCode)
	}
}

func TestWikiGuardrailsPendingPerObject(t *testing.T) {
	app, database := newWikiGuardrailsApp(t, map[string]string{
		"WIKI_PENDING_PER_USER":   "10",
		"WIKI_PENDING_PER_OBJECT": "1",
		"WIKI_META_MAX_BYTES":     "50000",
	})
	userA := createTestUser(t, database, "wiki_pending_obj_a")
	userB := createTestUser(t, database, "wiki_pending_obj_b")
	cognomID := createTestCognom(t, database, userA.ID)

	sessionA := createSessionCookie(t, database, userA.ID, "sess-wiki-obj-a")
	sessionB := createSessionCookie(t, database, userB.ID, "sess-wiki-obj-b")
	csrfToken := "csrf-wiki-pending-obj"

	rr := postCognomPropose(t, app, sessionA, csrfToken, cognomID, "origen", "notes")
	if rr.Result().StatusCode != http.StatusSeeOther {
		t.Fatalf("esperava 303 a la primera proposta, rebut %d", rr.Result().StatusCode)
	}
	rr = postCognomPropose(t, app, sessionB, csrfToken, cognomID, "origen2", "notes2")
	if rr.Result().StatusCode != http.StatusBadRequest {
		t.Fatalf("esperava 400 per límit per objecte, rebut %d", rr.Result().StatusCode)
	}
}

func TestWikiGuardrailsRateLimit(t *testing.T) {
	app, database := newWikiGuardrailsApp(t, map[string]string{
		"WIKI_CHANGE_RATE":        "0.01",
		"WIKI_CHANGE_BURST":       "3",
		"WIKI_PENDING_PER_USER":   "100",
		"WIKI_PENDING_PER_OBJECT": "100",
		"WIKI_META_MAX_BYTES":     "50000",
	})
	user := createTestUser(t, database, "wiki_rate_user")
	cognomID := createTestCognom(t, database, user.ID)

	session := createSessionCookie(t, database, user.ID, "sess-wiki-rate")
	csrfToken := "csrf-wiki-rate"

	lastStatus := 0
	for i := 0; i < 4; i++ {
		rr := postCognomPropose(t, app, session, csrfToken, cognomID, "origen", "notes")
		lastStatus = rr.Result().StatusCode
		if i < 3 && lastStatus == http.StatusTooManyRequests {
			t.Fatalf("no s'esperava 429 abans del burst: iter=%d", i)
		}
	}
	if lastStatus != http.StatusTooManyRequests {
		t.Fatalf("esperava 429 després del burst, rebut %d", lastStatus)
	}
}

func TestWikiGuardrailsMarkRateLimit(t *testing.T) {
	app, database := newWikiGuardrailsApp(t, map[string]string{
		"WIKI_MARK_RATE":  "0.01",
		"WIKI_MARK_BURST": "3",
	})
	user := createTestUser(t, database, "wiki_mark_rate_user")
	cognomID := createTestCognom(t, database, user.ID)

	session := createSessionCookie(t, database, user.ID, "sess-wiki-mark-rate")
	csrfToken := "csrf-wiki-mark-rate"

	lastStatus := 0
	for i := 0; i < 4; i++ {
		rr := postCognomMark(t, app, session, csrfToken, cognomID)
		lastStatus = rr.Result().StatusCode
		if i < 3 && lastStatus == http.StatusTooManyRequests {
			t.Fatalf("no s'esperava 429 abans del burst: iter=%d", i)
		}
	}
	if lastStatus != http.StatusTooManyRequests {
		t.Fatalf("esperava 429 després del burst, rebut %d", lastStatus)
	}
}
