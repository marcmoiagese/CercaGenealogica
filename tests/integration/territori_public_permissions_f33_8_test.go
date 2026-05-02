package integration

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func createF338Municipi(t *testing.T, database db.DB, suffix string, status string) int {
	t.Helper()
	id, err := database.CreateMunicipi(&db.Municipi{
		Nom:            fmt.Sprintf("Municipi F33-8 %s %d", suffix, time.Now().UnixNano()),
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: status,
	})
	if err != nil {
		t.Fatalf("CreateMunicipi ha fallat: %v", err)
	}
	return id
}

func TestF338DemografiaRebuildUsesScopedMunicipiEdit(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f33_8_demografia_rebuild.sqlite3")

	allowedMunID := createF338Municipi(t, database, "allowed", "publicat")
	blockedMunID := createF338Municipi(t, database, "blocked", "publicat")
	bootstrapAdmin := createTestUser(t, database, "f33_8_bootstrap_admin")
	assignPolicyByName(t, database, bootstrapAdmin.ID, "admin")

	noPerm := createTestUser(t, database, "f33_8_demografia_no_perm")
	noPermSession := createSessionCookie(t, database, noPerm.ID, "sess_f33_8_demografia_no_perm")
	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/admin/municipis/%d/demografia/rebuild", allowedMunID), nil)
	req.AddCookie(noPermSession)
	rr := httptest.NewRecorder()
	app.MunicipiDemografiaAdminAPI(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("rebuild demografia sense permis hauria de bloquejar amb 403, got %d", rr.Code)
	}

	editor := createTestUser(t, database, "f33_8_demografia_editor")
	editorSession := createSessionCookie(t, database, editor.ID, "sess_f33_8_demografia_editor")
	policyID := createPolicyWithScopedGrant(t, database, "f33_8_municipi_edit", "territori.municipis.edit", string(core.ScopeMunicipi), allowedMunID)
	assignF333Policy(t, database, editor.ID, policyID)

	req = httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/admin/municipis/%d/demografia/rebuild", allowedMunID), nil)
	req.AddCookie(editorSession)
	rr = httptest.NewRecorder()
	app.MunicipiDemografiaAdminAPI(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("rebuild demografia dins municipi permes esperava 200, got %d", rr.Code)
	}

	req = httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/admin/municipis/%d/demografia/rebuild", blockedMunID), nil)
	req.AddCookie(editorSession)
	rr = httptest.NewRecorder()
	app.MunicipiDemografiaAdminAPI(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("rebuild demografia fora municipi permes hauria de bloquejar amb 403, got %d", rr.Code)
	}

	admin := createTestUser(t, database, "f33_8_demografia_admin")
	assignPolicyByName(t, database, admin.ID, "admin")
	adminSession := createSessionCookie(t, database, admin.ID, "sess_f33_8_demografia_admin")
	req = httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/admin/municipis/%d/demografia/rebuild", blockedMunID), nil)
	req.AddCookie(adminSession)
	rr = httptest.NewRecorder()
	app.MunicipiDemografiaAdminAPI(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("admin global hauria de poder fer rebuild demografia, got %d", rr.Code)
	}
}
