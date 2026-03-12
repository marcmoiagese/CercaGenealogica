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

func TestArxiusImportBulkMode(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f29_6_arxius_import.sqlite3")

	admin := createTestUser(t, database, "f29_6_admin_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_f29_6_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	pais := &db.Pais{
		CodiISO2:    "ES",
		CodiISO3:    "ESP",
		CodiPaisNum: "724",
	}
	paisID, err := database.CreatePais(pais)
	if err != nil {
		t.Fatalf("CreatePais ha fallat: %v", err)
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
	mun := &db.Municipi{
		Nom:            "Municipi Test",
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	}
	mun.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(nivellID), Valid: true}
	if _, err := database.CreateMunicipi(mun); err != nil {
		t.Fatalf("CreateMunicipi ha fallat: %v", err)
	}
	entitat := &db.Arquebisbat{
		Nom:            "Bisbat Test",
		TipusEntitat:   "bisbat",
		PaisID:         sql.NullInt64{Int64: int64(paisID), Valid: true},
		ModeracioEstat: "pendent",
	}
	if _, err := database.CreateArquebisbat(entitat); err != nil {
		t.Fatalf("CreateArquebisbat ha fallat: %v", err)
	}

	projectRoot := findProjectRoot(t)
	fixturePath := filepath.Join(projectRoot, "tests", "fixtures", "arxius_export_sample.json")
	payload, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("ReadFile fixture ha fallat: %v", err)
	}

	csrfToken := "csrf_f29_6_import"
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	_ = writer.WriteField("csrf_token", csrfToken)
	part, err := writer.CreateFormFile("import_file", "arxius.json")
	if err != nil {
		t.Fatalf("CreateFormFile ha fallat: %v", err)
	}
	if _, err := io.Copy(part, bytes.NewReader(payload)); err != nil {
		t.Fatalf("escriure payload ha fallat: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close multipart ha fallat: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/admin/arxius/import/run", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfToken))
	rr := httptest.NewRecorder()
	app.AdminArxiusImportRun(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("import: esperava 303, rebut %d", rr.Code)
	}

	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM arxius"); got != 1 {
		t.Fatalf("arxius totals esperats 1, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM usuaris_activitat WHERE usuari_id = ? AND estat = 'pendent' AND detalls = 'import' AND objecte_tipus = 'arxiu'", admin.ID); got != 1 {
		t.Fatalf("activitats import esperades 1, got %d", got)
	}
	rows, err := database.Query("SELECT municipi_id, entitat_eclesiastica_id FROM arxius WHERE nom = ?", "Arxiu Test A")
	if err != nil || len(rows) != 1 {
		t.Fatalf("no he trobat Arxiu Test A: %v len=%d", err, len(rows))
	}
	munID := parseCountValue(t, rows[0]["municipi_id"])
	entID := parseCountValue(t, rows[0]["entitat_eclesiastica_id"])
	if munID == 0 || entID == 0 {
		t.Fatalf("esperava municipi i entitat amb valors > 0 (mun=%d ent=%d)", munID, entID)
	}

	csrfToken = "csrf_f29_6_import_2"
	body = &bytes.Buffer{}
	writer = multipart.NewWriter(body)
	_ = writer.WriteField("csrf_token", csrfToken)
	part, err = writer.CreateFormFile("import_file", "arxius.json")
	if err != nil {
		t.Fatalf("CreateFormFile (2) ha fallat: %v", err)
	}
	if _, err := io.Copy(part, bytes.NewReader(payload)); err != nil {
		t.Fatalf("escriure payload (2) ha fallat: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close multipart (2) ha fallat: %v", err)
	}

	req = httptest.NewRequest(http.MethodPost, "/admin/arxius/import/run", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfToken))
	rr = httptest.NewRecorder()
	app.AdminArxiusImportRun(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("import (2): esperava 303, rebut %d", rr.Code)
	}

	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM arxius"); got != 1 {
		t.Fatalf("arxius totals (2) esperats 1, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM usuaris_activitat WHERE usuari_id = ? AND estat = 'pendent' AND detalls = 'import' AND objecte_tipus = 'arxiu'", admin.ID); got != 1 {
		t.Fatalf("activitats import (2) esperades 1, got %d", got)
	}
}
