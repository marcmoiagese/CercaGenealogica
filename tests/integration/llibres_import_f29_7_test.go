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

func TestLlibresImportBulkMode(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f29_7_llibres_import.sqlite3")

	admin := createTestUser(t, database, "f29_7_admin_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_f29_7_"+strconv.FormatInt(time.Now().UnixNano(), 10))

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
	arxiu := &db.Arxiu{
		Nom:            "Arxiu Test",
		Tipus:          "municipal",
		Acces:          "public",
		CreatedBy:      sql.NullInt64{Int64: int64(admin.ID), Valid: true},
		ModeracioEstat: "pendent",
	}
	if _, err := database.CreateArxiu(arxiu); err != nil {
		t.Fatalf("CreateArxiu ha fallat: %v", err)
	}

	projectRoot := findProjectRoot(t)
	fixturePath := filepath.Join(projectRoot, "tests", "fixtures", "llibres_export_sample.json")
	payload, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("ReadFile fixture ha fallat: %v", err)
	}

	csrfToken := "csrf_f29_7_import"
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	_ = writer.WriteField("csrf_token", csrfToken)
	part, err := writer.CreateFormFile("import_file", "llibres.json")
	if err != nil {
		t.Fatalf("CreateFormFile ha fallat: %v", err)
	}
	if _, err := io.Copy(part, bytes.NewReader(payload)); err != nil {
		t.Fatalf("escriure payload ha fallat: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close multipart ha fallat: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/admin/llibres/import/run", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfToken))
	rr := httptest.NewRecorder()
	app.AdminLlibresImportRun(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("import: esperava 303, rebut %d", rr.Code)
	}

	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM llibres"); got != 1 {
		t.Fatalf("llibres totals esperats 1, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM arxius_llibres"); got != 1 {
		t.Fatalf("arxius_llibres totals esperats 1, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM llibres_urls"); got != 1 {
		t.Fatalf("llibres_urls totals esperats 1, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM usuaris_activitat WHERE usuari_id = ? AND estat = 'pendent' AND detalls = 'import' AND objecte_tipus = 'llibre'", admin.ID); got != 1 {
		t.Fatalf("activitats import esperades 1, got %d", got)
	}

	csrfToken = "csrf_f29_7_import_2"
	body = &bytes.Buffer{}
	writer = multipart.NewWriter(body)
	_ = writer.WriteField("csrf_token", csrfToken)
	part, err = writer.CreateFormFile("import_file", "llibres.json")
	if err != nil {
		t.Fatalf("CreateFormFile (2) ha fallat: %v", err)
	}
	if _, err := io.Copy(part, bytes.NewReader(payload)); err != nil {
		t.Fatalf("escriure payload (2) ha fallat: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close multipart (2) ha fallat: %v", err)
	}

	req = httptest.NewRequest(http.MethodPost, "/admin/llibres/import/run", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfToken))
	rr = httptest.NewRecorder()
	app.AdminLlibresImportRun(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("import (2): esperava 303, rebut %d", rr.Code)
	}

	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM llibres"); got != 1 {
		t.Fatalf("llibres totals (2) esperats 1, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM arxius_llibres"); got != 1 {
		t.Fatalf("arxius_llibres totals (2) esperats 1, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM llibres_urls"); got != 1 {
		t.Fatalf("llibres_urls totals (2) esperats 1, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM usuaris_activitat WHERE usuari_id = ? AND estat = 'pendent' AND detalls = 'import' AND objecte_tipus = 'llibre'", admin.ID); got != 1 {
		t.Fatalf("activitats import (2) esperades 1, got %d", got)
	}
}
