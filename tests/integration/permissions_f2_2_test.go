package integration

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func hasPolicy(pols []db.Politica, name string) bool {
	for _, p := range pols {
		if strings.EqualFold(p.Nom, name) {
			return true
		}
	}
	return false
}

func TestEnsureDefaultPoliciesBackfillUsuari(t *testing.T) {
	_, database := newTestAppForLogin(t, "test_f2_2_backfill.sqlite3")

	user1, _ := createF7UserWithSession(t, database)
	user2, _ := createF7UserWithSession(t, database)

	if err := database.EnsureDefaultPolicies(); err != nil {
		t.Fatalf("EnsureDefaultPolicies ha fallat: %v", err)
	}

	pols1, err := database.ListUserPolitiques(user1.ID)
	if err != nil {
		t.Fatalf("ListUserPolitiques(user1) ha fallat: %v", err)
	}
	pols2, err := database.ListUserPolitiques(user2.ID)
	if err != nil {
		t.Fatalf("ListUserPolitiques(user2) ha fallat: %v", err)
	}

	if !hasPolicy(pols2, "usuari") {
		t.Fatalf("user2 hauria de tenir política usuari")
	}
	if hasPolicy(pols2, "admin") {
		t.Fatalf("user2 no hauria de ser admin per defecte")
	}
	if !hasPolicy(pols1, "admin") {
		t.Fatalf("user1 hauria de tenir admin assignat per bootstrap")
	}
}

func TestNoAdminFallbackForRegularUser(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f2_2_no_admin_fallback.sqlite3")

	_, _ = createF7UserWithSession(t, database) // primer usuari (admin bootstrap)
	user2, sessionID := createF7UserWithSession(t, database)

	if err := database.EnsureDefaultPolicies(); err != nil {
		t.Fatalf("EnsureDefaultPolicies ha fallat: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/admin/paisos", nil)
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	rr := httptest.NewRecorder()

	app.AdminListPaisos(rr, req)

	if rr.Result().StatusCode != http.StatusForbidden {
		t.Fatalf("esperava 403 per usuari no admin (id=%d), rebut %d", user2.ID, rr.Result().StatusCode)
	}
}
