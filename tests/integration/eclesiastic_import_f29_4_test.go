package integration

import (
	"bytes"
	"database/sql"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestEclesiasticImportBulkMode(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f29_4_ecles_import.sqlite3")

	admin := createTestUser(t, database, "f29_4_admin_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_f29_4_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	pais := &db.Pais{
		CodiISO2:    "ES",
		CodiISO3:    "ESP",
		CodiPaisNum: "724",
	}
	paisID, err := database.CreatePais(pais)
	if err != nil {
		t.Fatalf("CreatePais ha fallat: %v", err)
	}
	paisFR := &db.Pais{
		CodiISO2:    "FR",
		CodiISO3:    "FRA",
		CodiPaisNum: "250",
	}
	paisFRID, err := database.CreatePais(paisFR)
	if err != nil {
		t.Fatalf("CreatePais FR ha fallat: %v", err)
	}
	nivell := &db.NivellAdministratiu{
		PaisID:         paisID,
		Nivel:          1,
		NomNivell:      "Test nivell",
		TipusNivell:    "pais",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	}
	nivellID, err := database.CreateNivell(nivell)
	if err != nil {
		t.Fatalf("CreateNivell ha fallat: %v", err)
	}
	nivellFR := &db.NivellAdministratiu{
		PaisID:         paisFRID,
		Nivel:          1,
		NomNivell:      "Test nivell FR",
		TipusNivell:    "pais",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	}
	nivellFRID, err := database.CreateNivell(nivellFR)
	if err != nil {
		t.Fatalf("CreateNivell FR ha fallat: %v", err)
	}
	mun := &db.Municipi{
		Nom:            "Municipi Test",
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	}
	mun.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(nivellID), Valid: true}
	munID, err := database.CreateMunicipi(mun)
	if err != nil {
		t.Fatalf("CreateMunicipi ha fallat: %v", err)
	}
	munFR := &db.Municipi{
		Nom:            "Municipi Test",
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	}
	munFR.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(nivellFRID), Valid: true}
	if _, err := database.CreateMunicipi(munFR); err != nil {
		t.Fatalf("CreateMunicipi FR ha fallat: %v", err)
	}

	projectRoot := findProjectRoot(t)
	fixturePath := filepath.Join(projectRoot, "tests", "fixtures", "eclesiastic_export_sample.json")
	payload, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("ReadFile fixture ha fallat: %v", err)
	}

	csrfToken := "csrf_f29_4_import"
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	_ = writer.WriteField("csrf_token", csrfToken)
	part, err := writer.CreateFormFile("import_file", "eclesiastic.json")
	if err != nil {
		t.Fatalf("CreateFormFile ha fallat: %v", err)
	}
	if _, err := io.Copy(part, bytes.NewReader(payload)); err != nil {
		t.Fatalf("escriure payload ha fallat: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close multipart ha fallat: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/admin/eclesiastic/import/run", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfToken))
	rr := httptest.NewRecorder()
	app.AdminEclesiasticImportRun(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("import: esperava 303, rebut %d", rr.Code)
	}

	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM arquebisbats"); got != 2 {
		t.Fatalf("arquebisbats totals esperats 2, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM arquebisbats_municipi"); got != 1 {
		t.Fatalf("relacions totals esperades 1, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM usuaris_activitat WHERE usuari_id = ? AND estat = 'pendent' AND detalls = 'import' AND objecte_tipus = 'eclesiastic'", admin.ID); got != 2 {
		t.Fatalf("activitats import esperades 2, got %d", got)
	}

	rows, err := database.Query("SELECT parent_id FROM arquebisbats WHERE nom = ?", "Bisbat Fill")
	if err != nil || len(rows) != 1 {
		t.Fatalf("no he trobat Bisbat Fill: %v len=%d", err, len(rows))
	}
	parentID := parseCountValue(t, rows[0]["parent_id"])
	parentRows, err := database.Query("SELECT id FROM arquebisbats WHERE nom = ?", "Arquebisbat Pare")
	if err != nil || len(parentRows) != 1 {
		t.Fatalf("no he trobat Arquebisbat Pare: %v len=%d", err, len(parentRows))
	}
	parentExpected := parseCountValue(t, parentRows[0]["id"])
	if parentID != parentExpected {
		t.Fatalf("parent_id esperat %d, got %d", parentExpected, parentID)
	}
	relRows, err := database.Query("SELECT id_municipi FROM arquebisbats_municipi")
	if err != nil || len(relRows) != 1 {
		t.Fatalf("no he trobat relacio municipi: %v len=%d", err, len(relRows))
	}
	relMunID := parseCountValue(t, relRows[0]["id_municipi"])
	if relMunID != munID {
		t.Fatalf("municipi relacio esperat %d, got %d", munID, relMunID)
	}

	csrfToken = "csrf_f29_4_import_2"
	body = &bytes.Buffer{}
	writer = multipart.NewWriter(body)
	_ = writer.WriteField("csrf_token", csrfToken)
	part, err = writer.CreateFormFile("import_file", "eclesiastic.json")
	if err != nil {
		t.Fatalf("CreateFormFile (2) ha fallat: %v", err)
	}
	if _, err := io.Copy(part, bytes.NewReader(payload)); err != nil {
		t.Fatalf("escriure payload (2) ha fallat: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close multipart (2) ha fallat: %v", err)
	}

	req = httptest.NewRequest(http.MethodPost, "/admin/eclesiastic/import/run", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfToken))
	rr = httptest.NewRecorder()
	app.AdminEclesiasticImportRun(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("import (2): esperava 303, rebut %d", rr.Code)
	}

	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM arquebisbats"); got != 2 {
		t.Fatalf("arquebisbats totals (2) esperats 2, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM arquebisbats_municipi"); got != 1 {
		t.Fatalf("relacions totals (2) esperades 1, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM usuaris_activitat WHERE usuari_id = ? AND estat = 'pendent' AND detalls = 'import' AND objecte_tipus = 'eclesiastic'", admin.ID); got != 2 {
		t.Fatalf("activitats import (2) esperades 2, got %d", got)
	}
}
