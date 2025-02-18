package integration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestEspaiNotificationsReadAndPrefs(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f25_notifications.sqlite3")

	user := createTestUser(t, database, "notif_user")
	session := createSessionCookie(t, database, user.ID, "sess_notif_user")

	id1, err := database.CreateEspaiNotification(&db.EspaiNotification{
		UserID: user.ID,
		Kind:   "matches_pending",
		Status: "unread",
	})
	if err != nil {
		t.Fatalf("CreateEspaiNotification 1 ha fallat: %v", err)
	}
	id2, err := database.CreateEspaiNotification(&db.EspaiNotification{
		UserID: user.ID,
		Kind:   "group_conflicts",
		Status: "unread",
	})
	if err != nil {
		t.Fatalf("CreateEspaiNotification 2 ha fallat: %v", err)
	}

	csrfToken := "csrf_notif_read"
	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("id", strconv.Itoa(id1))
	req := httptest.NewRequest(http.MethodPost, "/espai/notificacions/read", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfToken))
	rr := httptest.NewRecorder()
	app.RequireLogin(app.EspaiNotificationsRead)(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("read: esperava 303, rebut %d", rr.Code)
	}
	if got := getNotificationStatus(t, database, id1); got != "read" {
		t.Fatalf("read: esperava status read, rebut %q", got)
	}

	csrfAll := "csrf_notif_read_all"
	form = url.Values{}
	form.Set("csrf_token", csrfAll)
	req = httptest.NewRequest(http.MethodPost, "/espai/notificacions/read-all", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfAll))
	rr = httptest.NewRecorder()
	app.RequireLogin(app.EspaiNotificationsReadAll)(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("read-all: esperava 303, rebut %d", rr.Code)
	}
	if got := getNotificationStatus(t, database, id2); got != "read" {
		t.Fatalf("read-all: esperava status read, rebut %q", got)
	}

	csrfPrefs := "csrf_notif_prefs"
	form = url.Values{}
	form.Set("csrf_token", csrfPrefs)
	form.Set("freq", "weekly")
	form.Add("types", "matches")
	req = httptest.NewRequest(http.MethodPost, "/espai/notificacions/prefs", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfPrefs))
	rr = httptest.NewRecorder()
	app.RequireLogin(app.EspaiNotificationsPrefs)(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("prefs: esperava 303, rebut %d", rr.Code)
	}
	pref, err := database.GetEspaiNotificationPref(user.ID)
	if err != nil {
		t.Fatalf("GetEspaiNotificationPref ha fallat: %v", err)
	}
	if pref.Freq != "weekly" {
		t.Fatalf("prefs: esperava freq weekly, rebut %q", pref.Freq)
	}
	if !pref.TypesJSON.Valid || pref.TypesJSON.String == "" {
		t.Fatalf("prefs: esperava types_json no buit")
	}
	var types []string
	if err := json.Unmarshal([]byte(pref.TypesJSON.String), &types); err != nil {
		t.Fatalf("prefs: no puc parsejar types_json: %v", err)
	}
	if len(types) != 1 || types[0] != "matches" {
		t.Fatalf("prefs: esperava tipus [matches], rebut %#v", types)
	}

	csrfPrefs = "csrf_notif_prefs_invalid"
	form = url.Values{}
	form.Set("csrf_token", csrfPrefs)
	form.Set("freq", "invalid")
	req = httptest.NewRequest(http.MethodPost, "/espai/notificacions/prefs", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfPrefs))
	rr = httptest.NewRecorder()
	app.RequireLogin(app.EspaiNotificationsPrefs)(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("prefs invalid: esperava 303, rebut %d", rr.Code)
	}
	pref, err = database.GetEspaiNotificationPref(user.ID)
	if err != nil {
		t.Fatalf("GetEspaiNotificationPref invalid ha fallat: %v", err)
	}
	if pref.Freq != "instant" {
		t.Fatalf("prefs invalid: esperava freq instant, rebut %q", pref.Freq)
	}
}

func TestEspaiNotificationsACLAndCSRF(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f25_notifications_acl.sqlite3")

	userA := createTestUser(t, database, "notif_user_a")
	userB := createTestUser(t, database, "notif_user_b")
	sessionB := createSessionCookie(t, database, userB.ID, "sess_notif_b")

	idA, err := database.CreateEspaiNotification(&db.EspaiNotification{
		UserID: userA.ID,
		Kind:   "matches_pending",
		Status: "unread",
	})
	if err != nil {
		t.Fatalf("CreateEspaiNotification ha fallat: %v", err)
	}

	form := url.Values{}
	form.Set("csrf_token", "csrf_missing_cookie")
	form.Set("id", strconv.Itoa(idA))
	req := httptest.NewRequest(http.MethodPost, "/espai/notificacions/read", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(sessionB)
	rr := httptest.NewRecorder()
	app.RequireLogin(app.EspaiNotificationsRead)(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("csrf: esperava 400, rebut %d", rr.Code)
	}

	csrfToken := "csrf_acl_read"
	form = url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("id", strconv.Itoa(idA))
	req = httptest.NewRequest(http.MethodPost, "/espai/notificacions/read", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(sessionB)
	req.AddCookie(csrfCookie(csrfToken))
	rr = httptest.NewRecorder()
	app.RequireLogin(app.EspaiNotificationsRead)(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("acl: esperava 303, rebut %d", rr.Code)
	}
	if got := getNotificationStatus(t, database, idA); got != "unread" {
		t.Fatalf("acl: status ha de continuar unread, rebut %q", got)
	}
}

func getNotificationStatus(t *testing.T, database db.DB, id int) string {
	t.Helper()
	rows, err := database.Query("SELECT status FROM espai_notifications WHERE id = ?", id)
	if err != nil || len(rows) == 0 {
		t.Fatalf("getNotificationStatus: err=%v rows=%d", err, len(rows))
	}
	val := rows[0]["status"]
	switch v := val.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		t.Fatalf("status tipus inesperat: %T", val)
	}
	return ""
}
