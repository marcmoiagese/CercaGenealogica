package integration

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func newTestAppWithMail(t *testing.T, dbFileName string) (*core.App, db.DB) {
	t.Helper()
	cfg := map[string]string{
		"DB_ENGINE":    "sqlite",
		"DB_PATH":      filepath.Join(t.TempDir(), dbFileName),
		"RECREADB":     "true",
		"LOG_LEVEL":    "silent",
		"MAIL_ENABLED": "true",
	}
	return newTestAppForConfig(t, cfg)
}

func setPrivacy(t *testing.T, database db.DB, userID int, allowContact, notifyEmail bool) {
	t.Helper()
	if err := database.CreatePrivacyDefaults(userID); err != nil {
		t.Fatalf("CreatePrivacyDefaults ha fallat: %v", err)
	}
	p, err := database.GetPrivacySettings(userID)
	if err != nil || p == nil {
		t.Fatalf("GetPrivacySettings ha fallat: %v", err)
	}
	p.AllowContact = allowContact
	p.NotifyEmail = notifyEmail
	if err := database.SavePrivacySettings(userID, p); err != nil {
		t.Fatalf("SavePrivacySettings ha fallat: %v", err)
	}
}

func postMessagesNew(t *testing.T, app *core.App, session *http.Cookie, csrfToken string, recipientID int, body string) *httptest.ResponseRecorder {
	t.Helper()
	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("recipient_id", strconv.Itoa(recipientID))
	form.Set("body", body)
	req := httptest.NewRequest(http.MethodPost, "/missatges/nou", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfToken))
	req.RemoteAddr = "127.0.0.1:9101"
	rr := httptest.NewRecorder()
	app.MessagesNew(rr, req)
	return rr
}

func postMessagesSend(t *testing.T, app *core.App, session *http.Cookie, csrfToken string, threadID int, body string) *httptest.ResponseRecorder {
	t.Helper()
	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("body", body)
	path := fmt.Sprintf("/missatges/fil/%d/enviar", threadID)
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfToken))
	req.RemoteAddr = "127.0.0.1:9102"
	rr := httptest.NewRecorder()
	app.MessagesSend(rr, req)
	return rr
}

func postMessagesArchive(t *testing.T, app *core.App, session *http.Cookie, csrfToken string, threadID int, archived bool) *httptest.ResponseRecorder {
	t.Helper()
	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	if archived {
		form.Set("archived", "1")
	} else {
		form.Set("archived", "0")
	}
	path := fmt.Sprintf("/missatges/fil/%d/arxivar", threadID)
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfToken))
	req.RemoteAddr = "127.0.0.1:9103"
	rr := httptest.NewRecorder()
	app.MessagesArchive(rr, req)
	return rr
}

func postMessagesDelete(t *testing.T, app *core.App, session *http.Cookie, csrfToken string, threadID int) *httptest.ResponseRecorder {
	t.Helper()
	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	path := fmt.Sprintf("/missatges/fil/%d/esborrar", threadID)
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfToken))
	req.RemoteAddr = "127.0.0.1:9104"
	rr := httptest.NewRecorder()
	app.MessagesDelete(rr, req)
	return rr
}

func postMessagesBlock(t *testing.T, app *core.App, session *http.Cookie, csrfToken string, threadID int, blocked bool) *httptest.ResponseRecorder {
	t.Helper()
	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	if blocked {
		form.Set("blocked", "1")
	} else {
		form.Set("blocked", "0")
	}
	path := fmt.Sprintf("/missatges/fil/%d/bloc", threadID)
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfToken))
	req.RemoteAddr = "127.0.0.1:9105"
	rr := httptest.NewRecorder()
	app.MessagesBlock(rr, req)
	return rr
}

func getMessagesThread(t *testing.T, app *core.App, session *http.Cookie, threadID int) *httptest.ResponseRecorder {
	t.Helper()
	path := fmt.Sprintf("/missatges/fil/%d", threadID)
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.AddCookie(session)
	req.RemoteAddr = "127.0.0.1:9106"
	rr := httptest.NewRecorder()
	app.MessagesThread(rr, req)
	return rr
}

func parseThreadIDFromLocation(t *testing.T, loc string) int {
	t.Helper()
	parts := strings.Split(strings.Trim(loc, "/"), "/")
	if len(parts) < 3 {
		t.Fatalf("location inesperada: %q", loc)
	}
	id, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		t.Fatalf("no puc parsejar thread id de %q: %v", loc, err)
	}
	return id
}

func TestMessagesAllowContactAndBlock(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f21_contact.sqlite3")

	userA := createTestUser(t, database, "dm_user_a")
	userB := createTestUser(t, database, "dm_user_b")

	setPrivacy(t, database, userA.ID, true, true)
	setPrivacy(t, database, userB.ID, false, true)

	session := createSessionCookie(t, database, userA.ID, "sess-dm-contact")
	csrfToken := "csrf-dm-contact"

	rr := postMessagesNew(t, app, session, csrfToken, userB.ID, "hola")
	if rr.Result().StatusCode != http.StatusForbidden {
		t.Fatalf("esperava 403 amb allow_contact=false, rebut %d", rr.Result().StatusCode)
	}

	setPrivacy(t, database, userB.ID, true, true)
	setPrivacy(t, database, userA.ID, false, true)
	rr = postMessagesNew(t, app, session, csrfToken, userB.ID, "hola")
	if rr.Result().StatusCode != http.StatusForbidden {
		t.Fatalf("esperava 403 amb allow_contact=false (sender), rebut %d", rr.Result().StatusCode)
	}

	setPrivacy(t, database, userA.ID, true, true)
	rr = postMessagesNew(t, app, session, csrfToken, userB.ID, "hola")
	if rr.Result().StatusCode != http.StatusSeeOther {
		t.Fatalf("esperava 303 quan allow_contact=true, rebut %d", rr.Result().StatusCode)
	}
	threadID := parseThreadIDFromLocation(t, rr.Result().Header.Get("Location"))

	rr = postMessagesBlock(t, app, session, csrfToken, threadID, true)
	if rr.Result().StatusCode != http.StatusSeeOther {
		t.Fatalf("esperava 303 en bloc, rebut %d", rr.Result().StatusCode)
	}
	if blocked, _ := database.IsUserBlocked(userA.ID, userB.ID); !blocked {
		t.Fatalf("esperava user_blocks per %d -> %d", userA.ID, userB.ID)
	}

	rr = postMessagesSend(t, app, session, csrfToken, threadID, "prova")
	if rr.Result().StatusCode != http.StatusForbidden {
		t.Fatalf("esperava 403 en enviar amb bloc, rebut %d", rr.Result().StatusCode)
	}
}

func TestMessagesUnreadReadArchiveDelete(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f21_unread.sqlite3")

	userA := createTestUser(t, database, "dm_unread_a")
	userB := createTestUser(t, database, "dm_unread_b")

	setPrivacy(t, database, userA.ID, true, true)
	setPrivacy(t, database, userB.ID, true, true)

	sessionA := createSessionCookie(t, database, userA.ID, "sess-dm-unread-a")
	sessionB := createSessionCookie(t, database, userB.ID, "sess-dm-unread-b")
	csrfToken := "csrf-dm-unread"

	rr := postMessagesNew(t, app, sessionA, csrfToken, userB.ID, "hola")
	if rr.Result().StatusCode != http.StatusSeeOther {
		t.Fatalf("esperava 303 en crear missatge, rebut %d", rr.Result().StatusCode)
	}
	threadID := parseThreadIDFromLocation(t, rr.Result().Header.Get("Location"))

	rows, err := database.Query("SELECT last_read_message_id, archived, deleted FROM dm_thread_state WHERE thread_id = ? AND user_id = ?", threadID, userB.ID)
	if err != nil || len(rows) == 0 {
		t.Fatalf("no puc llegir dm_thread_state: %v", err)
	}
	if rows[0]["last_read_message_id"] != nil {
		t.Fatalf("esperava last_read_message_id NULL abans de llegir")
	}

	rr = getMessagesThread(t, app, sessionB, threadID)
	if rr.Result().StatusCode != http.StatusOK {
		t.Fatalf("esperava 200 en veure thread, rebut %d", rr.Result().StatusCode)
	}

	rows, err = database.Query("SELECT last_read_message_id FROM dm_thread_state WHERE thread_id = ? AND user_id = ?", threadID, userB.ID)
	if err != nil || len(rows) == 0 {
		t.Fatalf("no puc llegir dm_thread_state després de read: %v", err)
	}
	if rows[0]["last_read_message_id"] == nil || parseCountValue(t, rows[0]["last_read_message_id"]) <= 0 {
		t.Fatalf("esperava last_read_message_id > 0 després de read")
	}

	rr = postMessagesArchive(t, app, sessionA, csrfToken, threadID, true)
	if rr.Result().StatusCode != http.StatusSeeOther {
		t.Fatalf("esperava 303 arxivant, rebut %d", rr.Result().StatusCode)
	}
	rows, err = database.Query("SELECT archived FROM dm_thread_state WHERE thread_id = ? AND user_id = ?", threadID, userA.ID)
	if err != nil || len(rows) == 0 {
		t.Fatalf("no puc llegir archived: %v", err)
	}
	if parseCountValue(t, rows[0]["archived"]) != 1 {
		t.Fatalf("esperava archived=1")
	}

	rr = postMessagesDelete(t, app, sessionA, csrfToken, threadID)
	if rr.Result().StatusCode != http.StatusSeeOther {
		t.Fatalf("esperava 303 esborrant, rebut %d", rr.Result().StatusCode)
	}
	rows, err = database.Query("SELECT deleted FROM dm_thread_state WHERE thread_id = ? AND user_id = ?", threadID, userA.ID)
	if err != nil || len(rows) == 0 {
		t.Fatalf("no puc llegir deleted: %v", err)
	}
	if parseCountValue(t, rows[0]["deleted"]) != 1 {
		t.Fatalf("esperava deleted=1")
	}
}

func TestMessagesEmailNotifications(t *testing.T) {
	app, database := newTestAppWithMail(t, "test_f21_mail.sqlite3")

	userA := createTestUser(t, database, "dm_mail_a")
	userB := createTestUser(t, database, "dm_mail_b")

	setPrivacy(t, database, userA.ID, true, true)
	setPrivacy(t, database, userB.ID, true, true)

	type sentMail struct {
		To      string
		Subject string
		Body    string
	}
	var sent []sentMail
	core.SetMailSendOverride(func(to, subject, body string) error {
		sent = append(sent, sentMail{To: to, Subject: subject, Body: body})
		return nil
	})
	t.Cleanup(func() {
		core.SetMailSendOverride(nil)
	})

	session := createSessionCookie(t, database, userA.ID, "sess-dm-mail")
	csrfToken := "csrf-dm-mail"

	longBody := strings.Repeat("a", 200)
	rr := postMessagesNew(t, app, session, csrfToken, userB.ID, longBody)
	if rr.Result().StatusCode != http.StatusSeeOther {
		t.Fatalf("esperava 303 en enviar, rebut %d", rr.Result().StatusCode)
	}
	threadID := parseThreadIDFromLocation(t, rr.Result().Header.Get("Location"))

	if len(sent) != 1 {
		t.Fatalf("esperava 1 correu enviat, rebut %d", len(sent))
	}
	if !strings.Contains(sent[0].Subject, "@"+userA.Usuari) {
		t.Fatalf("subject no conté emissor: %q", sent[0].Subject)
	}
	expectedURL := fmt.Sprintf("http://localhost:8080/missatges/fil/%d", threadID)
	if !strings.Contains(sent[0].Body, expectedURL) {
		t.Fatalf("body no conté l'URL del thread")
	}

	sent = nil
	setPrivacy(t, database, userB.ID, true, false)
	rr = postMessagesSend(t, app, session, csrfToken, threadID, "segon")
	if rr.Result().StatusCode != http.StatusSeeOther {
		t.Fatalf("esperava 303 en enviar segon missatge, rebut %d", rr.Result().StatusCode)
	}
	if len(sent) != 0 {
		t.Fatalf("no s'esperaven correus quan notify_email=false")
	}
}

func TestMessagesRateLimit(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f21_rate.sqlite3")

	userA := createTestUser(t, database, "dm_rate_a")
	userB := createTestUser(t, database, "dm_rate_b")

	setPrivacy(t, database, userA.ID, true, true)
	setPrivacy(t, database, userB.ID, true, true)

	thread, err := database.GetOrCreateDMThread(userA.ID, userB.ID)
	if err != nil || thread == nil {
		t.Fatalf("GetOrCreateDMThread ha fallat: %v", err)
	}

	session := createSessionCookie(t, database, userA.ID, "sess-dm-rate-"+strconv.FormatInt(time.Now().UnixNano(), 10))
	csrfToken := "csrf-dm-rate"

	var lastStatus int
	for i := 0; i < 31; i++ {
		rr := postMessagesSend(t, app, session, csrfToken, thread.ID, "ok")
		lastStatus = rr.Result().StatusCode
		if i < 30 && lastStatus == http.StatusTooManyRequests {
			t.Fatalf("no s'esperava 429 abans del burst: iter=%d", i)
		}
	}
	if lastStatus != http.StatusTooManyRequests {
		t.Fatalf("esperava 429 després del burst, rebut %d", lastStatus)
	}
}
