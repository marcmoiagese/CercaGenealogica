package integration

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF354U1ApplyTransactionalAndReimportIdempotent(t *testing.T) {
	targetApp, targetDB, targetSession, exportedJSON, _, _ := f354U1PrepareTargetAndExport(t, "test_f35_4u1_ok.sqlite3", true)
	csrfToken, csrfCookie := extractCSRFContextFromImportExport(t, targetApp, targetSession)

	dryRunBody := f354U1DryRun(t, targetApp, targetDB, targetSession, csrfCookie, csrfToken, exportedJSON)
	if len(f354U1ListEntitats(t, targetDB)) != 0 {
		t.Fatalf("el dry-run F35-4U1 no ha de persistir cap entitat")
	}

	applyRR := f354U1Apply(t, targetApp, targetSession, csrfCookie, extractCSRFTokenFromHTML(t, dryRunBody), extractHiddenTextareaValue(t, dryRunBody, "payload_b64"))
	if applyRR.Code != http.StatusSeeOther {
		t.Fatalf("apply F35-4U1 status=%d body=%s", applyRR.Code, applyRR.Body.String())
	}
	if len(f354U1ListEntitats(t, targetDB)) != 2 {
		t.Fatalf("la primera aplicacio F35-4U1 ha de crear 2 entitats")
	}
	if len(f354U1ListHierarchy(t, targetDB)) != 1 {
		t.Fatalf("la primera aplicacio F35-4U1 ha de crear 1 relacio jerarquica")
	}
	if len(f354U1ListTerritory(t, targetDB)) != 1 {
		t.Fatalf("la primera aplicacio F35-4U1 ha de crear 1 relacio territorial")
	}
	if len(f354U1ListArchive(t, targetDB)) != 1 {
		t.Fatalf("la primera aplicacio F35-4U1 ha de crear 1 relacio arxiu-entitat")
	}

	applyRR = f354U1Apply(t, targetApp, targetSession, csrfCookie, csrfToken, extractHiddenTextareaValue(t, dryRunBody, "payload_b64"))
	if applyRR.Code != http.StatusSeeOther {
		t.Fatalf("reapply F35-4U1 status=%d body=%s", applyRR.Code, applyRR.Body.String())
	}
	if len(f354U1ListEntitats(t, targetDB)) != 2 {
		t.Fatalf("el reimport F35-4U1 no ha de duplicar entitats")
	}
	if len(f354U1ListHierarchy(t, targetDB)) != 1 {
		t.Fatalf("el reimport F35-4U1 no ha de duplicar relacions jerarquiques")
	}
	if len(f354U1ListTerritory(t, targetDB)) != 1 {
		t.Fatalf("el reimport F35-4U1 no ha de duplicar relacions territorials")
	}
	if len(f354U1ListArchive(t, targetDB)) != 1 {
		t.Fatalf("el reimport F35-4U1 no ha de duplicar relacions arxiu-entitat")
	}
}

func TestF354U1RollbackAfterEntitiesHookLeavesNoPartialData(t *testing.T) {
	targetApp, targetDB, targetSession, exportedJSON, _, _ := f354U1PrepareTargetAndExport(t, "test_f35_4u1_rb_entities.sqlite3", true)
	csrfToken, csrfCookie := extractCSRFContextFromImportExport(t, targetApp, targetSession)
	dryRunBody := f354U1DryRun(t, targetApp, targetDB, targetSession, csrfCookie, csrfToken, exportedJSON)

	db.ConfessionalImportTxTestHook = func(stage string) error {
		if stage == db.ConfessionalImportTxStageAfterEntities {
			return fmt.Errorf("fallada de prova despres d'entitats")
		}
		return nil
	}
	defer func() { db.ConfessionalImportTxTestHook = nil }()

	applyRR := f354U1Apply(t, targetApp, targetSession, csrfCookie, extractCSRFTokenFromHTML(t, dryRunBody), extractHiddenTextareaValue(t, dryRunBody, "payload_b64"))
	if applyRR.Code != http.StatusOK {
		t.Fatalf("apply rollback entitats status=%d body=%s", applyRR.Code, applyRR.Body.String())
	}
	if !strings.Contains(applyRR.Body.String(), "Import transaccional revertit") {
		t.Fatalf("l'error de rollback ha d'explicar que no hi ha canvis parcials; body=%s", applyRR.Body.String())
	}
	f354U1AssertNoImportedConfessionalData(t, targetDB)
}

func TestF354U1RollbackWhenTerritoryBecomesInvalidLeavesNoPartialData(t *testing.T) {
	targetApp, targetDB, targetSession, exportedJSON, _, _ := f354U1PrepareTargetAndExport(t, "test_f35_4u1_rb_territory.sqlite3", true)
	csrfToken, csrfCookie := extractCSRFContextFromImportExport(t, targetApp, targetSession)
	dryRunBody := f354U1DryRun(t, targetApp, targetDB, targetSession, csrfCookie, csrfToken, exportedJSON)

	db.ConfessionalImportTxTestHook = func(stage string) error {
		if stage == db.ConfessionalImportTxStageAfterHierarchy {
			return fmt.Errorf("fallada de prova abans de relacions territorials")
		}
		return nil
	}
	defer func() { db.ConfessionalImportTxTestHook = nil }()

	applyRR := f354U1Apply(t, targetApp, targetSession, csrfCookie, extractCSRFTokenFromHTML(t, dryRunBody), extractHiddenTextareaValue(t, dryRunBody, "payload_b64"))
	if applyRR.Code != http.StatusOK {
		t.Fatalf("apply rollback territori status=%d body=%s", applyRR.Code, applyRR.Body.String())
	}
	if !strings.Contains(applyRR.Body.String(), "Import transaccional revertit") {
		t.Fatalf("el rollback territorial ha de deixar missatge clar; body=%s", applyRR.Body.String())
	}
	f354U1AssertNoImportedConfessionalData(t, targetDB)
}

func TestF354U1RollbackWhenArchiveDisappearsLeavesNoPartialData(t *testing.T) {
	targetApp, targetDB, targetSession, exportedJSON, _, _ := f354U1PrepareTargetAndExport(t, "test_f35_4u1_rb_archive.sqlite3", true)
	csrfToken, csrfCookie := extractCSRFContextFromImportExport(t, targetApp, targetSession)
	dryRunBody := f354U1DryRun(t, targetApp, targetDB, targetSession, csrfCookie, csrfToken, exportedJSON)

	db.ConfessionalImportTxTestHook = func(stage string) error {
		if stage == db.ConfessionalImportTxStageAfterTerritory {
			return fmt.Errorf("fallada de prova abans de relacions arxiu-entitat")
		}
		return nil
	}
	defer func() { db.ConfessionalImportTxTestHook = nil }()

	applyRR := f354U1Apply(t, targetApp, targetSession, csrfCookie, extractCSRFTokenFromHTML(t, dryRunBody), extractHiddenTextareaValue(t, dryRunBody, "payload_b64"))
	if applyRR.Code != http.StatusOK {
		t.Fatalf("apply rollback arxiu status=%d body=%s", applyRR.Code, applyRR.Body.String())
	}
	if !strings.Contains(applyRR.Body.String(), "Import transaccional revertit") {
		t.Fatalf("el rollback arxivistic ha de deixar missatge clar; body=%s", applyRR.Body.String())
	}
	f354U1AssertNoImportedConfessionalData(t, targetDB)
}

func f354U1PrepareTargetAndExport(t *testing.T, targetDBFile string, withNucli bool) (*core.App, db.DB, *http.Cookie, []byte, int, int) {
	t.Helper()
	sourceApp, sourceDB := newTestAppForLogin(t, "test_f35_4u1_source_"+time.Now().Format("150405000000000")+".sqlite3")
	sourceUser := createTestUser(t, sourceDB, "f35_4u1_source_"+time.Now().Format("150405000000000"))
	sourceSession := createSessionCookie(t, sourceDB, sourceUser.ID, "sess_f35_4u1_source_"+time.Now().Format("150405000000000"))
	sourcePolicy := createPolicyWithGrant(t, sourceDB, "f35_4u1_source_export", "territori.confessional.import_export.export")
	assignPolicyToUser(t, sourceDB, sourceUser.ID, sourcePolicy)

	suffix := time.Now().Format("150405000000000")
	sourceMunicipiID := f353YCreateMunicipi(t, sourceDB, "Municipi F35-4U1 "+suffix)
	sourceNucliID := 0
	if withNucli {
		id, err := sourceDB.CreateMunicipi(&db.Municipi{
			Nom:            "Nucli F35-4U1 " + suffix,
			Tipus:          "nucli_urba",
			MunicipiID:     sql.NullInt64{Int64: int64(sourceMunicipiID), Valid: true},
			Estat:          "actiu",
			ModeracioEstat: "publicat",
		})
		if err != nil {
			t.Fatalf("CreateMunicipi nucli source: %v", err)
		}
		sourceNucliID = id
	}
	sourceParentID := f354SSaveEntitat(t, sourceDB, "f35_4u1_root_"+suffix, "Bisbat F35-4U1 "+suffix, "catolicisme_ritu_llati", "bisbat_diocesi", "publicat")
	sourceChildID := f354SSaveEntitat(t, sourceDB, "f35_4u1_local_"+suffix, "Parroquia F35-4U1 "+suffix, "catolicisme_ritu_llati", "parroquia", "publicat")
	f354SSaveEntitatRelacio(t, sourceDB, sourceParentID, sourceChildID, "parroquia", "publicat")
	if _, err := sourceDB.SaveMunicipiEntitatReligiosa(&db.MunicipiEntitatReligiosa{
		MunicipiID:         sourceMunicipiID,
		NucliID:            sql.NullInt64{Int64: int64(sourceNucliID), Valid: sourceNucliID > 0},
		EntitatReligiosaID: sourceChildID,
		TipusRelacio:       "parroquia_local",
		ModeracioEstat:     "publicat",
	}); err != nil {
		t.Fatalf("SaveMunicipiEntitatReligiosa source: %v", err)
	}
	sourceArxiuID := f354SCreateArxiu(t, sourceDB, "Arxiu F35-4U1 "+suffix, sourceMunicipiID)
	f354SSaveArxiuEntitatRelacio(t, sourceDB, sourceArxiuID, sourceChildID, "custodia", "publicat")

	req := httptest.NewRequest(http.MethodGet, "/admin/confessional/export", nil)
	req.AddCookie(sourceSession)
	rr := httptest.NewRecorder()
	sourceApp.AdminConfessionalExport(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("export F35-4U1 status=%d body=%s", rr.Code, rr.Body.String())
	}

	targetApp, targetDB := newTestAppForLogin(t, targetDBFile)
	targetUser := createTestUser(t, targetDB, "f35_4u1_target_"+time.Now().Format("150405000000000"))
	targetSession := createSessionCookie(t, targetDB, targetUser.ID, "sess_f35_4u1_target_"+time.Now().Format("150405000000000"))
	targetPolicy := createPolicyWithGrant(t, targetDB, "f35_4u1_target_import", "territori.confessional.import_export.import")
	addGrantToPolicy(t, targetDB, targetPolicy, "territori.confessional.import_export.view")
	assignPolicyToUser(t, targetDB, targetUser.ID, targetPolicy)

	targetMunicipiID := f353YCreateMunicipi(t, targetDB, "Municipi F35-4U1 "+suffix)
	targetNucliID := 0
	if withNucli {
		id, err := targetDB.CreateMunicipi(&db.Municipi{
			Nom:            "Nucli F35-4U1 " + suffix,
			Tipus:          "nucli_urba",
			MunicipiID:     sql.NullInt64{Int64: int64(targetMunicipiID), Valid: true},
			Estat:          "actiu",
			ModeracioEstat: "publicat",
		})
		if err != nil {
			t.Fatalf("CreateMunicipi nucli target: %v", err)
		}
		targetNucliID = id
	}
	targetArxiuID := f354SCreateArxiu(t, targetDB, "Arxiu F35-4U1 "+suffix, targetMunicipiID)

	return targetApp, targetDB, targetSession, rr.Body.Bytes(), targetArxiuID, targetNucliID
}

func f354U1DryRun(t *testing.T, app *core.App, _ db.DB, session, csrfCookie *http.Cookie, csrfToken string, exportedJSON []byte) string {
	t.Helper()
	req := newMultipartRequest(t, "/admin/confessional/import/dry-run", "import_file", "confessional-export.json", exportedJSON, map[string]string{
		"csrf_token": csrfToken,
	})
	req.AddCookie(session)
	req.AddCookie(csrfCookie)
	rr := httptest.NewRecorder()
	app.AdminConfessionalImportDryRun(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("dry-run F35-4U1 status=%d body=%s", rr.Code, rr.Body.String())
	}
	return rr.Body.String()
}

func f354U1Apply(t *testing.T, app *core.App, session, csrfCookie *http.Cookie, csrfToken, payloadB64 string) *httptest.ResponseRecorder {
	t.Helper()
	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("payload_b64", payloadB64)
	req := httptest.NewRequest(http.MethodPost, "/admin/confessional/import/apply", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie)
	rr := httptest.NewRecorder()
	app.AdminConfessionalImportApply(rr, req)
	return rr
}

func f354U1AssertNoImportedConfessionalData(t *testing.T, database db.DB) {
	t.Helper()
	if got := len(f354U1ListEntitats(t, database)); got != 0 {
		t.Fatalf("despres del rollback no hi ha d'haver entitats importades, got %d", got)
	}
	if got := len(f354U1ListHierarchy(t, database)); got != 0 {
		t.Fatalf("despres del rollback no hi ha d'haver relacions jerarquiques importades, got %d", got)
	}
	if got := len(f354U1ListTerritory(t, database)); got != 0 {
		t.Fatalf("despres del rollback no hi ha d'haver relacions territorials importades, got %d", got)
	}
	if got := len(f354U1ListArchive(t, database)); got != 0 {
		t.Fatalf("despres del rollback no hi ha d'haver relacions arxiu-entitat importades, got %d", got)
	}
}

func f354U1ListEntitats(t *testing.T, database db.DB) []db.EntitatReligiosa {
	t.Helper()
	rows, err := database.ListEntitatsReligioses()
	if err != nil {
		t.Fatalf("ListEntitatsReligioses: %v", err)
	}
	out := make([]db.EntitatReligiosa, 0)
	for _, row := range rows {
		if strings.HasPrefix(row.Codi, "f35_4u1_") {
			out = append(out, row)
		}
	}
	return out
}

func f354U1ListHierarchy(t *testing.T, database db.DB) []db.EntitatReligiosaRelacio {
	t.Helper()
	rows, err := database.ListEntitatReligiosaRelacions()
	if err != nil {
		t.Fatalf("ListEntitatReligiosaRelacions: %v", err)
	}
	return rows
}

func f354U1ListTerritory(t *testing.T, database db.DB) []db.MunicipiEntitatReligiosa {
	t.Helper()
	rows, err := database.ListMunicipiEntitatsReligioses(0)
	if err != nil {
		t.Fatalf("ListMunicipiEntitatsReligioses: %v", err)
	}
	return rows
}

func f354U1ListArchive(t *testing.T, database db.DB) []db.ArxiuEntitatReligiosa {
	t.Helper()
	rows, err := database.ListArxiuEntitatsReligioses(0, 0, "")
	if err != nil {
		t.Fatalf("ListArxiuEntitatsReligioses: %v", err)
	}
	return rows
}
