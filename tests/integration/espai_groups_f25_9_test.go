package integration

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestEspaiGroupsTreeACL(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f25_groups_acl.sqlite3")

	owner := createTestUser(t, database, "group_owner")
	viewer := createTestUser(t, database, "group_viewer")
	viewerSession := createSessionCookie(t, database, viewer.ID, "sess_group_viewer")

	group := &db.EspaiGrup{
		OwnerUserID: owner.ID,
		Nom:         "Test Grup",
		Status:      "active",
	}
	if _, err := database.CreateEspaiGrup(group); err != nil {
		t.Fatalf("CreateEspaiGrup ha fallat: %v", err)
	}
	if _, err := database.AddEspaiGrupMembre(&db.EspaiGrupMembre{
		GrupID: group.ID,
		UserID: owner.ID,
		Role:   "owner",
		Status: "active",
	}); err != nil {
		t.Fatalf("AddEspaiGrupMembre ha fallat: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/espai/grups/arbre?group_id="+strconv.Itoa(group.ID), nil)
	req.AddCookie(viewerSession)
	rr := httptest.NewRecorder()
	app.RequireLogin(app.EspaiGrupsTreeView)(rr, req)

	if rr.Code != http.StatusSeeOther {
		t.Fatalf("esperava 303 per no membre, rebut %d", rr.Code)
	}
	loc := rr.Result().Header.Get("Location")
	if loc == "" || loc == "/espai/grups" {
		t.Fatalf("esperava redirect amb error, rebut %q", loc)
	}
}
