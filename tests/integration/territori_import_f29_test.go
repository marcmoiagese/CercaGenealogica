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

func TestTerritoriImportLegacyPayloadPublishesVisibleHierarchy(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f29_territori_import_legacy_visibility.sqlite3")

	admin := createTestUser(t, database, "f29_import_fix_admin_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_f29_fix_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	projectRoot := findProjectRoot(t)
	fixturePath := filepath.Join(projectRoot, "tests", "fixtures", "territori_export_sample.json")
	runTerritoriImportFixture(t, app, session, fixturePath)

	comarca := findNivellByNom(t, database, "Comarca Test")
	if comarca.ModeracioEstat != "publicat" {
		t.Fatalf("nivell importat esperat publicat, got %s", comarca.ModeracioEstat)
	}

	total, err := database.CountMunicipisBrowse(db.MunicipiBrowseFilter{
		NivellID: comarca.ID,
		Status:   "publicat",
	})
	if err != nil {
		t.Fatalf("CountMunicipisBrowse ha fallat: %v", err)
	}
	if total != 2 {
		t.Fatalf("municipis públics esperats 2 dins la comarca importada, got %d", total)
	}

	rows, err := database.ListMunicipisBrowse(db.MunicipiBrowseFilter{
		NivellID: comarca.ID,
		Status:   "publicat",
		Sort:     "nom",
		SortDir:  "asc",
	})
	if err != nil {
		t.Fatalf("ListMunicipisBrowse ha fallat: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows municipis públics esperades 2, got %d", len(rows))
	}
	if rows[0].Nom != "Municipi Fill" || rows[1].Nom != "Municipi Pare" {
		t.Fatalf("ordre/contingut municipis inesperat: %+v", rows)
	}

	munPare := findMunicipiByNom(t, database, "Municipi Pare")
	munFill := findMunicipiByNom(t, database, "Municipi Fill")
	if munPare.ModeracioEstat != "publicat" || munFill.ModeracioEstat != "publicat" {
		t.Fatalf("municipis importats esperats publicat/publicat, got %s/%s", munPare.ModeracioEstat, munFill.ModeracioEstat)
	}
}

func TestTerritoriImportReimportPromotesPendingDuplicatesToPublicat(t *testing.T) {
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
	if nivellPaisAfter.ModeracioEstat != "publicat" || nivellComarcaAfter.ModeracioEstat != "publicat" {
		t.Fatalf("nivells reimportats esperats publicat/publicat, got %s/%s", nivellPaisAfter.ModeracioEstat, nivellComarcaAfter.ModeracioEstat)
	}

	munPareAfter := findMunicipiByNom(t, database, "Municipi Pare")
	munFillAfter := findMunicipiByNom(t, database, "Municipi Fill")
	if munPareAfter.ModeracioEstat != "publicat" || munFillAfter.ModeracioEstat != "publicat" {
		t.Fatalf("municipis reimportats esperats publicat/publicat, got %s/%s", munPareAfter.ModeracioEstat, munFillAfter.ModeracioEstat)
	}

	total, err := database.CountMunicipisBrowse(db.MunicipiBrowseFilter{
		NivellID: nivellComarcaAfter.ID,
		Status:   "publicat",
	})
	if err != nil {
		t.Fatalf("CountMunicipisBrowse després de reimport ha fallat: %v", err)
	}
	if total != 2 {
		t.Fatalf("municipis públics esperats 2 després de reimport, got %d", total)
	}
}
