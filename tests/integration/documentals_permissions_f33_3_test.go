package integration

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func createF333DocumentalFixture(t *testing.T, database db.DB, userID int, suffix string) (int, int) {
	t.Helper()
	munID, err := database.CreateMunicipi(&db.Municipi{
		Nom:            "Municipi F33-3 " + suffix,
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: userID > 0},
	})
	if err != nil {
		t.Fatalf("CreateMunicipi ha fallat: %v", err)
	}
	eclesID, err := database.CreateArquebisbat(&db.Arquebisbat{
		Nom:            fmt.Sprintf("Bisbat F33-3 %s %d", suffix, time.Now().UnixNano()),
		TipusEntitat:   "bisbat",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: userID > 0},
	})
	if err != nil {
		t.Fatalf("CreateArquebisbat ha fallat: %v", err)
	}
	arxiuID, err := database.CreateArxiu(&db.Arxiu{
		Nom:                   "Arxiu F33-3 " + suffix,
		Tipus:                 "parroquia",
		Acces:                 "online",
		MunicipiID:            sql.NullInt64{Int64: int64(munID), Valid: true},
		EntitatEclesiasticaID: sql.NullInt64{Int64: int64(eclesID), Valid: true},
		ModeracioEstat:        "publicat",
		CreatedBy:             sql.NullInt64{Int64: int64(userID), Valid: userID > 0},
	})
	if err != nil {
		t.Fatalf("CreateArxiu ha fallat: %v", err)
	}
	llibreID, err := database.CreateLlibre(&db.Llibre{
		ArquebisbatID:  eclesID,
		MunicipiID:     munID,
		Titol:          "Llibre F33-3 " + suffix,
		TipusLlibre:    "baptismes",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: userID > 0},
	})
	if err != nil {
		t.Fatalf("CreateLlibre ha fallat: %v", err)
	}
	if err := database.AddArxiuLlibre(arxiuID, llibreID, "", ""); err != nil {
		t.Fatalf("AddArxiuLlibre ha fallat: %v", err)
	}
	return arxiuID, llibreID
}

func assignF333Policy(t *testing.T, database db.DB, userID int, policyID int) {
	t.Helper()
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("AddUserPolitica ha fallat: %v", err)
	}
}

func TestF333DocumentalsGlobalAdminCanEditArxiuAndLlibre(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f33_3_documentals_admin.sqlite3")

	admin := createTestUser(t, database, "f33_3_documentals_admin")
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_f33_3_documentals_admin")
	arxiuID, llibreID := createF333DocumentalFixture(t, database, admin.ID, "admin")

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/documentals/arxius/%d/edit", arxiuID), nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminEditArxiu(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminEditArxiu admin global esperava 200, got %d", rr.Code)
	}

	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/documentals/llibres/%d/edit", llibreID), nil)
	req.AddCookie(session)
	rr = httptest.NewRecorder()
	app.AdminEditLlibre(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminEditLlibre admin global esperava 200, got %d", rr.Code)
	}
}

func TestF333DocumentalsArxiuScopedEditAndUI(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f33_3_arxiu_scoped.sqlite3")

	owner := createTestUser(t, database, "f33_3_arxiu_owner")
	allowedArxiuID, _ := createF333DocumentalFixture(t, database, owner.ID, "allowed")
	blockedArxiuID, _ := createF333DocumentalFixture(t, database, owner.ID, "blocked")

	noPerm := createTestUser(t, database, "f33_3_arxiu_no_perm")
	noPermSession := createSessionCookie(t, database, noPerm.ID, "sess_f33_3_arxiu_no_perm")
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/documentals/arxius/%d/edit", allowedArxiuID), nil)
	req.AddCookie(noPermSession)
	rr := httptest.NewRecorder()
	app.AdminEditArxiu(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("AdminEditArxiu sense permis hauria de bloquejar amb 403, got %d", rr.Code)
	}

	editor := createTestUser(t, database, "f33_3_arxiu_editor")
	session := createSessionCookie(t, database, editor.ID, "sess_f33_3_arxiu_editor")
	policyID := createPolicyWithScopedGrant(t, database, "f33_3_arxiu_scoped_edit", "documentals.arxius.edit", string(core.ScopeArxiu), allowedArxiuID)
	addGrantToPolicy(t, database, policyID, "documentals.arxius.view")
	assignF333Policy(t, database, editor.ID, policyID)

	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/documentals/arxius/%d/edit", allowedArxiuID), nil)
	req.AddCookie(session)
	rr = httptest.NewRecorder()
	app.AdminEditArxiu(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminEditArxiu dins arxiu permes esperava 200, got %d", rr.Code)
	}

	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/documentals/arxius/%d/edit", blockedArxiuID), nil)
	req.AddCookie(session)
	rr = httptest.NewRecorder()
	app.AdminEditArxiu(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("AdminEditArxiu fora arxiu permes hauria de bloquejar amb 403, got %d", rr.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/documentals/arxius?status=publicat", nil)
	req.AddCookie(session)
	rr = httptest.NewRecorder()
	app.AdminListArxius(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminListArxius esperava 200, got %d", rr.Code)
	}
	body := rr.Body.String()
	allowedEdit := fmt.Sprintf(`/documentals/arxius/%d/edit`, allowedArxiuID)
	blockedEdit := fmt.Sprintf(`/documentals/arxius/%d/edit`, blockedArxiuID)
	if !strings.Contains(body, allowedEdit) {
		t.Fatalf("la UI hauria de mostrar editar per arxiu dins ambit; falta %q", allowedEdit)
	}
	if strings.Contains(body, blockedEdit) {
		t.Fatalf("la UI no hauria de mostrar editar per arxiu fora ambit")
	}
}

func TestF333DocumentalsLlibreScopedEditAndUI(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f33_3_llibre_scoped.sqlite3")

	owner := createTestUser(t, database, "f33_3_llibre_owner")
	_, allowedLlibreID := createF333DocumentalFixture(t, database, owner.ID, "book-allowed")
	_, blockedLlibreID := createF333DocumentalFixture(t, database, owner.ID, "book-blocked")

	noPerm := createTestUser(t, database, "f33_3_llibre_no_perm")
	noPermSession := createSessionCookie(t, database, noPerm.ID, "sess_f33_3_llibre_no_perm")
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/documentals/llibres/%d/edit", allowedLlibreID), nil)
	req.AddCookie(noPermSession)
	rr := httptest.NewRecorder()
	app.AdminEditLlibre(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("AdminEditLlibre sense permis hauria de bloquejar amb 403, got %d", rr.Code)
	}

	editor := createTestUser(t, database, "f33_3_llibre_editor")
	session := createSessionCookie(t, database, editor.ID, "sess_f33_3_llibre_editor")
	policyID := createPolicyWithScopedGrant(t, database, "f33_3_llibre_scoped_edit", "documentals.llibres.edit", string(core.ScopeLlibre), allowedLlibreID)
	addGrantToPolicy(t, database, policyID, "documentals.llibres.view")
	assignF333Policy(t, database, editor.ID, policyID)

	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/documentals/llibres/%d/edit", allowedLlibreID), nil)
	req.AddCookie(session)
	rr = httptest.NewRecorder()
	app.AdminEditLlibre(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminEditLlibre dins llibre permes esperava 200, got %d", rr.Code)
	}

	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/documentals/llibres/%d/edit", blockedLlibreID), nil)
	req.AddCookie(session)
	rr = httptest.NewRecorder()
	app.AdminEditLlibre(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("AdminEditLlibre fora llibre permes hauria de bloquejar amb 403, got %d", rr.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/documentals/llibres?status=publicat", nil)
	req.AddCookie(session)
	rr = httptest.NewRecorder()
	app.AdminListLlibres(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminListLlibres esperava 200, got %d", rr.Code)
	}
	body := rr.Body.String()
	allowedEdit := fmt.Sprintf(`/documentals/llibres/%d/edit`, allowedLlibreID)
	blockedEdit := fmt.Sprintf(`/documentals/llibres/%d/edit`, blockedLlibreID)
	if !strings.Contains(body, allowedEdit) {
		t.Fatalf("la UI hauria de mostrar editar per llibre dins ambit; falta %q", allowedEdit)
	}
	if strings.Contains(body, blockedEdit) {
		t.Fatalf("la UI no hauria de mostrar editar per llibre fora ambit")
	}
}

func TestF333DocumentalsImportExportUsesDocumentalArxiuKeys(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f33_3_arxiu_import_export_keys.sqlite3")

	user := createTestUser(t, database, "f33_3_arxiu_import_export")
	session := createSessionCookie(t, database, user.ID, "sess_f33_3_arxiu_import_export")
	policyID := createPolicyWithScopedGrant(t, database, "f33_3_arxiu_import_export", "documentals.arxius.import", string(core.ScopeGlobal), 0)
	addGrantToPolicy(t, database, policyID, "documentals.arxius.export")
	assignF333Policy(t, database, user.ID, policyID)

	req := httptest.NewRequest(http.MethodGet, "/admin/import-export?tab=arxius", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminImportExport(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminImportExport amb keys documentals d'arxius esperava 200, got %d", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, `data-tab="arxius-import"`) || !strings.Contains(body, `/admin/arxius/export`) {
		t.Fatalf("la UI import/export hauria de mostrar import i export d'arxius amb keys documentals")
	}

	req = httptest.NewRequest(http.MethodGet, "/admin/arxius/export", nil)
	req.AddCookie(session)
	rr = httptest.NewRecorder()
	app.AdminArxiusExport(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminArxiusExport amb documentals.arxius.export esperava 200, got %d", rr.Code)
	}
}
