package integration

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func createTestUser(t *testing.T, database db.DB, username string) *db.User {
	t.Helper()
	user := &db.User{
		Usuari: username,
		Email:  username + "@example.com",
		Active: true,
		Password: []byte("hash"),
	}
	if err := database.InsertUser(user); err != nil {
		t.Fatalf("InsertUser ha fallat: %v", err)
	}
	return user
}

func assignPolicyByName(t *testing.T, database db.DB, userID int, policyName string) {
	t.Helper()
	_ = database.EnsureDefaultPolicies()
	rows, err := database.Query("SELECT id FROM politiques WHERE nom = ?", policyName)
	if err != nil || len(rows) == 0 {
		t.Fatalf("no puc obtenir politica %q: %v", policyName, err)
	}
	policyID := parseCountValue(t, rows[0]["id"])
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("AddUserPolitica ha fallat: %v", err)
	}
}

func createSessionCookie(t *testing.T, database db.DB, userID int, sessionID string) *http.Cookie {
	t.Helper()
	expiry := time.Now().Add(24 * time.Hour).Format(time.RFC3339)
	if err := database.SaveSession(sessionID, userID, expiry); err != nil {
		t.Fatalf("SaveSession ha fallat: %v", err)
	}
	return &http.Cookie{
		Name:  "cg_session",
		Value: sessionID,
		Path:  "/",
	}
}

func csrfCookie(token string) *http.Cookie {
	return &http.Cookie{
		Name:  "cg_csrf",
		Value: token,
		Path:  "/",
	}
}

func countRows(t *testing.T, database db.DB, query string, args ...interface{}) int {
	t.Helper()
	rows, err := database.Query(query, args...)
	if err != nil || len(rows) == 0 {
		t.Fatalf("countRows: query err=%v rows=%d", err, len(rows))
	}
	return parseCountValue(t, rows[0]["n"])
}

func parseCountValue(t *testing.T, raw interface{}) int {
	t.Helper()
	switch v := raw.(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case float64:
		return int(v)
	case []byte:
		n, err := strconv.Atoi(string(v))
		if err != nil {
			t.Fatalf("no es pot convertir %q a int: %v", string(v), err)
		}
		return n
	case string:
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("no es pot convertir %q a int: %v", v, err)
		}
		return n
	default:
		t.Fatalf("tipus inesperat per count: %T", raw)
	}
	return 0
}
