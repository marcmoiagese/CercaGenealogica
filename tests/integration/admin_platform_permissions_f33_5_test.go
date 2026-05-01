package integration

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func createF335PlatformUser(t *testing.T, database db.DB, username string, permKey string) (*db.User, *http.Cookie) {
	t.Helper()
	user := createTestUser(t, database, username+"_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	policyID, err := database.SavePolitica(&db.Politica{
		Nom:        "policy_" + username,
		Permisos:   "{}",
		Descripcio: "",
	})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica F33-5: %v", err)
	}
	if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
		PoliticaID: policyID,
		PermKey:    permKey,
		ScopeType:  "global",
	}); err != nil {
		t.Fatalf("no s'ha pogut crear grant F33-5 %s: %v", permKey, err)
	}
	if err := database.AddUserPolitica(user.ID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica F33-5: %v", err)
	}
	return user, createSessionCookie(t, database, user.ID, "sess_"+username+"_"+strconv.FormatInt(time.Now().UnixNano(), 10))
}

func performPlatformGET(handler http.HandlerFunc, path string, cookie *http.Cookie) int {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if cookie != nil {
		req.AddCookie(cookie)
	}
	rr := httptest.NewRecorder()
	handler(rr, req)
	return rr.Result().StatusCode
}

func TestF335AdminUsersKeyAccessesUsersOnly(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f33_5_admin_users.sqlite3")
	_ = createTestUser(t, database, "f33_5_seed_admin")
	_, cookie := createF335PlatformUser(t, database, "f33_5_users", "admin.users.manage")

	if got := performPlatformGET(app.AdminListUsuaris, "/admin/usuaris", cookie); got != http.StatusOK {
		t.Fatalf("admin.users.manage hauria d'accedir a usuaris, got=%d", got)
	}
	if got := performPlatformGET(app.AdminListPolitiques, "/admin/politiques", cookie); got != http.StatusForbidden {
		t.Fatalf("admin.users.manage no hauria d'accedir a politiques, got=%d", got)
	}
}

func TestF335AdminPoliciesKeyAccessesPoliciesAndAssignmentsOnly(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f33_5_admin_policies.sqlite3")
	_ = createTestUser(t, database, "f33_5_seed_admin")
	_, cookie := createF335PlatformUser(t, database, "f33_5_policies", "admin.policies.manage")

	if got := performPlatformGET(app.AdminListPolitiques, "/admin/politiques", cookie); got != http.StatusOK {
		t.Fatalf("admin.policies.manage hauria d'accedir a politiques, got=%d", got)
	}
	if got := performPlatformGET(app.AdminAssignacionsPolitiques, "/admin/politiques/assignacions", cookie); got != http.StatusOK {
		t.Fatalf("admin.policies.manage hauria d'accedir a assignacions, got=%d", got)
	}
	if got := performPlatformGET(app.AdminListUsuaris, "/admin/usuaris", cookie); got != http.StatusForbidden {
		t.Fatalf("admin.policies.manage no hauria d'accedir a usuaris, got=%d", got)
	}
}

func TestF335AdminAuditAndJobsKeysAreScoped(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f33_5_admin_audit_jobs.sqlite3")
	_ = createTestUser(t, database, "f33_5_seed_admin")
	_, auditCookie := createF335PlatformUser(t, database, "f33_5_audit", "admin.audit.view")
	_, jobsCookie := createF335PlatformUser(t, database, "f33_5_jobs", "admin.jobs.manage")

	if got := performPlatformGET(app.AdminAuditPage, "/admin/auditoria", auditCookie); got != http.StatusOK {
		t.Fatalf("admin.audit.view hauria d'accedir a auditoria, got=%d", got)
	}
	if got := performPlatformGET(app.AdminJobsListPage, "/admin/jobs", auditCookie); got != http.StatusForbidden {
		t.Fatalf("admin.audit.view no hauria d'accedir a jobs, got=%d", got)
	}
	if got := performPlatformGET(app.AdminJobsListPage, "/admin/jobs", jobsCookie); got != http.StatusOK {
		t.Fatalf("admin.jobs.manage hauria d'accedir a jobs, got=%d", got)
	}
	if got := performPlatformGET(app.AdminAuditPage, "/admin/auditoria", jobsCookie); got != http.StatusForbidden {
		t.Fatalf("admin.jobs.manage no hauria d'accedir a auditoria, got=%d", got)
	}
}
