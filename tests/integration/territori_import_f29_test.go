package integration

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

type moderacioSummaryPayload struct {
	Ok      bool `json:"ok"`
	Summary struct {
		Total  int `json:"total"`
		ByType []struct {
			Type  string `json:"type"`
			Total int    `json:"total"`
		} `json:"by_type"`
	} `json:"summary"`
}

func TestTerritoriImportBulkMode(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f29_territori_import.sqlite3")

	admin := createTestUser(t, database, "f29_admin_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_f29_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	ruleJSON := `{"type":"count","filters":{"rule_codes":["nivell_create"]},"threshold":1}`
	achievement := &db.Achievement{
		Code:         "bulk_import_test",
		Name:         "Bulk Import Test",
		Description:  "Test bulk import",
		Rarity:       "common",
		Visibility:   "visible",
		Domain:       "territori",
		IsEnabled:    true,
		IsRepeatable: false,
		RuleJSON:     ruleJSON,
	}
	if _, err := database.SaveAchievement(achievement); err != nil {
		t.Fatalf("SaveAchievement ha fallat: %v", err)
	}

	projectRoot := findProjectRoot(t)
	fixturePath := filepath.Join(projectRoot, "tests", "fixtures", "territori_export_sample.json")
	payload, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("ReadFile fixture ha fallat: %v", err)
	}

	csrfToken := "csrf_f29_import"
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	_ = writer.WriteField("csrf_token", csrfToken)
	part, err := writer.CreateFormFile("import_file", "territori.json")
	if err != nil {
		t.Fatalf("CreateFormFile ha fallat: %v", err)
	}
	if _, err := io.Copy(part, bytes.NewReader(payload)); err != nil {
		t.Fatalf("escriure payload ha fallat: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close multipart ha fallat: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/admin/territori/import/run", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfToken))
	rr := httptest.NewRecorder()
	app.AdminTerritoriImportRun(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("import: esperava 303, rebut %d", rr.Code)
	}

	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM paisos"); got != 1 {
		t.Fatalf("paisos totals esperats 1, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM nivells_administratius"); got != 2 {
		t.Fatalf("nivells totals esperats 2, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM municipis"); got != 2 {
		t.Fatalf("municipis totals esperats 2, got %d", got)
	}

	parentRows, err := database.Query("SELECT id FROM municipis WHERE nom = ?", "Municipi Pare")
	if err != nil || len(parentRows) != 1 {
		t.Fatalf("no he trobat Municipi Pare: %v len=%d", err, len(parentRows))
	}
	parentID := parseCountValue(t, parentRows[0]["id"])
	childRows, err := database.Query("SELECT municipi_id FROM municipis WHERE nom = ?", "Municipi Fill")
	if err != nil || len(childRows) != 1 {
		t.Fatalf("no he trobat Municipi Fill: %v len=%d", err, len(childRows))
	}
	childParentRaw := childRows[0]["municipi_id"]
	if childParentRaw == nil {
		t.Fatalf("Municipi Fill no te parent_id")
	}
	childParentID := parseCountValue(t, childParentRaw)
	if childParentID != parentID {
		t.Fatalf("parent_id esperat %d, got %d", parentID, childParentID)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM usuaris_activitat WHERE usuari_id = ? AND estat = 'pendent' AND detalls = 'import' AND objecte_tipus IN ('nivell','municipi')", admin.ID); got != 4 {
		t.Fatalf("activitats import esperades 4, got %d", got)
	}

	achievements, err := database.ListUserAchievements(admin.ID)
	if err != nil {
		t.Fatalf("ListUserAchievements ha fallat: %v", err)
	}
	if len(achievements) != 1 {
		t.Fatalf("esperava 1 achievement, got %d", len(achievements))
	}
	if !achievements[0].MetaJSON.Valid {
		t.Fatalf("achievement sense meta_json")
	}
	var meta map[string]interface{}
	if err := json.Unmarshal([]byte(achievements[0].MetaJSON.String), &meta); err != nil {
		t.Fatalf("MetaJSON invalid: %v", err)
	}
	if _, ok := meta["activity_id"]; ok {
		t.Fatalf("no esperava activity_id en bulk mode")
	}

	csrfToken = "csrf_f29_import_2"
	body = &bytes.Buffer{}
	writer = multipart.NewWriter(body)
	_ = writer.WriteField("csrf_token", csrfToken)
	part, err = writer.CreateFormFile("import_file", "territori.json")
	if err != nil {
		t.Fatalf("CreateFormFile (2) ha fallat: %v", err)
	}
	if _, err := io.Copy(part, bytes.NewReader(payload)); err != nil {
		t.Fatalf("escriure payload (2) ha fallat: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close multipart (2) ha fallat: %v", err)
	}

	req = httptest.NewRequest(http.MethodPost, "/admin/territori/import/run", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfToken))
	rr = httptest.NewRecorder()
	app.AdminTerritoriImportRun(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("import (2): esperava 303, rebut %d", rr.Code)
	}

	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM paisos"); got != 1 {
		t.Fatalf("paisos totals (2) esperats 1, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM nivells_administratius"); got != 2 {
		t.Fatalf("nivells totals (2) esperats 2, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM municipis"); got != 2 {
		t.Fatalf("municipis totals (2) esperats 2, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM usuaris_activitat WHERE usuari_id = ? AND estat = 'pendent' AND detalls = 'import' AND objecte_tipus IN ('nivell','municipi')", admin.ID); got != 4 {
		t.Fatalf("activitats import (2) esperades 4, got %d", got)
	}

	achievements, err = database.ListUserAchievements(admin.ID)
	if err != nil {
		t.Fatalf("ListUserAchievements (2) ha fallat: %v", err)
	}
	if len(achievements) != 1 {
		t.Fatalf("esperava 1 achievement després del segon import, got %d", len(achievements))
	}
}

func runTerritoriImportFixture(t *testing.T, app *core.App, session *http.Cookie, fixturePath string) {
	t.Helper()

	payload, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("ReadFile fixture ha fallat: %v", err)
	}

	csrfToken := "csrf_territori_import_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	_ = writer.WriteField("csrf_token", csrfToken)
	part, err := writer.CreateFormFile("import_file", "territori.json")
	if err != nil {
		t.Fatalf("CreateFormFile ha fallat: %v", err)
	}
	if _, err := io.Copy(part, bytes.NewReader(payload)); err != nil {
		t.Fatalf("escriure payload ha fallat: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close multipart ha fallat: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/admin/territori/import/run", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfToken))
	rr := httptest.NewRecorder()
	app.AdminTerritoriImportRun(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("import: esperava 303, rebut %d", rr.Code)
	}
}

func findNivellByNom(t *testing.T, database db.DB, name string) *db.NivellAdministratiu {
	t.Helper()
	nivells, err := database.ListNivells(db.NivellAdminFilter{})
	if err != nil {
		t.Fatalf("ListNivells ha fallat: %v", err)
	}
	for i := range nivells {
		if nivells[i].NomNivell == name {
			return &nivells[i]
		}
	}
	t.Fatalf("nivell %q no trobat", name)
	return nil
}

func findMunicipiByNom(t *testing.T, database db.DB, name string) *db.Municipi {
	t.Helper()
	rows, err := database.ListMunicipis(db.MunicipiFilter{Text: name})
	if err != nil {
		t.Fatalf("ListMunicipis ha fallat: %v", err)
	}
	for _, row := range rows {
		if row.Nom == name {
			mun, err := database.GetMunicipi(row.ID)
			if err != nil || mun == nil {
				t.Fatalf("GetMunicipi %q ha fallat: %v", name, err)
			}
			return mun
		}
	}
	t.Fatalf("municipi %q no trobat", name)
	return nil
}

func TestTerritoriImportLegacyPayloadLeavesHierarchyPendingForModeration(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f29_territori_import_legacy_visibility.sqlite3")

	admin := createTestUser(t, database, "f29_import_fix_admin_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_f29_fix_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	projectRoot := findProjectRoot(t)
	fixturePath := filepath.Join(projectRoot, "tests", "fixtures", "territori_export_sample.json")
	runTerritoriImportFixture(t, app, session, fixturePath)

	comarca := findNivellByNom(t, database, "Comarca Test")
	if comarca.ModeracioEstat != "pendent" {
		t.Fatalf("nivell importat esperat pendent, got %s", comarca.ModeracioEstat)
	}

	total, err := database.CountMunicipisBrowse(db.MunicipiBrowseFilter{
		NivellID: comarca.ID,
		Status:   "pendent",
	})
	if err != nil {
		t.Fatalf("CountMunicipisBrowse ha fallat: %v", err)
	}
	if total != 2 {
		t.Fatalf("municipis pendents esperats 2 dins la comarca importada, got %d", total)
	}

	rows, err := database.ListMunicipisBrowse(db.MunicipiBrowseFilter{
		NivellID: comarca.ID,
		Status:   "pendent",
		Sort:     "nom",
		SortDir:  "asc",
	})
	if err != nil {
		t.Fatalf("ListMunicipisBrowse ha fallat: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows municipis pendents esperades 2, got %d", len(rows))
	}
	if rows[0].Nom != "Municipi Fill" || rows[1].Nom != "Municipi Pare" {
		t.Fatalf("ordre/contingut municipis inesperat: %+v", rows)
	}

	munPare := findMunicipiByNom(t, database, "Municipi Pare")
	munFill := findMunicipiByNom(t, database, "Municipi Fill")
	if munPare.ModeracioEstat != "pendent" || munFill.ModeracioEstat != "pendent" {
		t.Fatalf("municipis importats esperats pendent/pendent, got %s/%s", munPare.ModeracioEstat, munFill.ModeracioEstat)
	}

	if totalNivells, err := database.CountNivells(db.NivellAdminFilter{Status: "pendent"}); err != nil || totalNivells != 2 {
		t.Fatalf("nivells pendents esperats 2, got %d err=%v", totalNivells, err)
	}
	if totalMunicipis, err := database.CountMunicipis(db.MunicipiFilter{Status: "pendent"}); err != nil || totalMunicipis != 2 {
		t.Fatalf("municipis pendents esperats 2, got %d err=%v", totalMunicipis, err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/admin/control/moderacio/summary", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminControlModeracioSummaryAPI(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("summary moderacio esperava 200, got %d", rr.Code)
	}
	var payload moderacioSummaryPayload
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("summary moderacio invalid: %v", err)
	}
	if !payload.Ok {
		t.Fatalf("summary moderacio ok esperat true")
	}
	got := map[string]int{}
	for _, item := range payload.Summary.ByType {
		got[item.Type] = item.Total
	}
	if got["nivell"] != 2 || got["municipi"] != 2 {
		t.Fatalf("summary moderacio esperava nivell=2 i municipi=2, got %+v", got)
	}
}

func TestTerritoriImportReimportPreservesPendingDuplicatesForModeration(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f29_territori_import_reimport_promotes.sqlite3")

	admin := createTestUser(t, database, "f29_import_fix_reimport_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_f29_fix_reimport_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	pais := db.Pais{CodiISO2: "TS", CodiISO3: "TST", CodiPaisNum: "999"}
	if _, err := database.CreatePais(&pais); err != nil {
		t.Fatalf("CreatePais ha fallat: %v", err)
	}
	nivellPais := &db.NivellAdministratiu{
		PaisID:         pais.ID,
		Nivel:          1,
		NomNivell:      "Testland",
		TipusNivell:    "pais",
		CodiOficial:    "TS",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
		CreatedBy:      sql.NullInt64{Int64: int64(admin.ID), Valid: true},
	}
	nivellPaisID, err := database.CreateNivell(nivellPais)
	if err != nil {
		t.Fatalf("CreateNivell pais ha fallat: %v", err)
	}
	nivellComarca := &db.NivellAdministratiu{
		PaisID:         pais.ID,
		Nivel:          2,
		NomNivell:      "Comarca Test",
		TipusNivell:    "comarca",
		CodiOficial:    "TS-C",
		ParentID:       sql.NullInt64{Int64: int64(nivellPaisID), Valid: true},
		Estat:          "actiu",
		ModeracioEstat: "pendent",
		CreatedBy:      sql.NullInt64{Int64: int64(admin.ID), Valid: true},
	}
	nivellComarcaID, err := database.CreateNivell(nivellComarca)
	if err != nil {
		t.Fatalf("CreateNivell comarca ha fallat: %v", err)
	}
	munPare := &db.Municipi{
		Nom:            "Municipi Pare",
		Tipus:          "municipi",
		CodiPostal:     "00000",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
		CreatedBy:      sql.NullInt64{Int64: int64(admin.ID), Valid: true},
	}
	munPare.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(nivellPaisID), Valid: true}
	munPare.NivellAdministratiuID[1] = sql.NullInt64{Int64: int64(nivellComarcaID), Valid: true}
	munPareID, err := database.CreateMunicipi(munPare)
	if err != nil {
		t.Fatalf("CreateMunicipi pare ha fallat: %v", err)
	}
	munFill := &db.Municipi{
		Nom:            "Municipi Fill",
		Tipus:          "municipi",
		CodiPostal:     "00001",
		MunicipiID:     sql.NullInt64{Int64: int64(munPareID), Valid: true},
		Estat:          "actiu",
		ModeracioEstat: "pendent",
		CreatedBy:      sql.NullInt64{Int64: int64(admin.ID), Valid: true},
	}
	munFill.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(nivellPaisID), Valid: true}
	munFill.NivellAdministratiuID[1] = sql.NullInt64{Int64: int64(nivellComarcaID), Valid: true}
	if _, err := database.CreateMunicipi(munFill); err != nil {
		t.Fatalf("CreateMunicipi fill ha fallat: %v", err)
	}

	projectRoot := findProjectRoot(t)
	fixturePath := filepath.Join(projectRoot, "tests", "fixtures", "territori_export_sample.json")
	runTerritoriImportFixture(t, app, session, fixturePath)

	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM nivells_administratius"); got != 2 {
		t.Fatalf("nivells totals esperats 2 després de reimport, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM municipis"); got != 2 {
		t.Fatalf("municipis totals esperats 2 després de reimport, got %d", got)
	}

	nivellPaisAfter := findNivellByNom(t, database, "Testland")
	nivellComarcaAfter := findNivellByNom(t, database, "Comarca Test")
	if nivellPaisAfter.ModeracioEstat != "pendent" || nivellComarcaAfter.ModeracioEstat != "pendent" {
		t.Fatalf("nivells reimportats esperats pendent/pendent, got %s/%s", nivellPaisAfter.ModeracioEstat, nivellComarcaAfter.ModeracioEstat)
	}

	munPareAfter := findMunicipiByNom(t, database, "Municipi Pare")
	munFillAfter := findMunicipiByNom(t, database, "Municipi Fill")
	if munPareAfter.ModeracioEstat != "pendent" || munFillAfter.ModeracioEstat != "pendent" {
		t.Fatalf("municipis reimportats esperats pendent/pendent, got %s/%s", munPareAfter.ModeracioEstat, munFillAfter.ModeracioEstat)
	}

	total, err := database.CountMunicipisBrowse(db.MunicipiBrowseFilter{
		NivellID: nivellComarcaAfter.ID,
		Status:   "pendent",
	})
	if err != nil {
		t.Fatalf("CountMunicipisBrowse després de reimport ha fallat: %v", err)
	}
	if total != 2 {
		t.Fatalf("municipis pendents esperats 2 després de reimport, got %d", total)
	}
}

func TestTerritoriImportRebuildsAdminClosureForImportedMunicipis(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f29_territori_import_closure.sqlite3")

	admin := createTestUser(t, database, "f29_import_closure_admin_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_f29_closure_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	projectRoot := findProjectRoot(t)
	fixturePath := filepath.Join(projectRoot, "tests", "fixtures", "territori_export_sample.json")
	runTerritoriImportFixture(t, app, session, fixturePath)

	comarca := findNivellByNom(t, database, "Comarca Test")
	munPare := findMunicipiByNom(t, database, "Municipi Pare")

	entries, err := database.ListAdminClosure(munPare.ID)
	if err != nil {
		t.Fatalf("ListAdminClosure ha fallat: %v", err)
	}
	if len(entries) < 3 {
		t.Fatalf("admin_closure esperava com a mínim municipi+nivell+pais, got %d", len(entries))
	}

	foundMunicipi := false
	foundComarca := false
	foundPais := false
	for _, entry := range entries {
		if entry.AncestorType == "municipi" && entry.AncestorID == munPare.ID {
			foundMunicipi = true
		}
		if entry.AncestorType == "nivell" && entry.AncestorID == comarca.ID {
			foundComarca = true
		}
		if entry.AncestorType == "pais" {
			foundPais = true
		}
	}
	if !foundMunicipi || !foundComarca || !foundPais {
		t.Fatalf("admin_closure incompleta: municipi=%t comarca=%t pais=%t entries=%+v", foundMunicipi, foundComarca, foundPais, entries)
	}
}
