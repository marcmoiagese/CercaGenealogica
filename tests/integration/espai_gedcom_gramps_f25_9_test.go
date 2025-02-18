package integration

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEspaiGedcomUploadCreatesImport(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := map[string]string{
		"DB_ENGINE":     "sqlite",
		"DB_PATH":       filepath.Join(tmpDir, "test_f25_gedcom.sqlite3"),
		"RECREADB":      "true",
		"LOG_LEVEL":     "silent",
		"GEDCOM_ROOT":   filepath.Join(tmpDir, "gedcom"),
		"GEDCOM_MAX_UPLOAD_MB": "5",
	}
	app, database := newTestAppForConfig(t, cfg)

	user := createTestUser(t, database, "gedcom_user")
	session := createSessionCookie(t, database, user.ID, "sess_gedcom_user")

	csrfToken := "csrf_gedcom_upload"
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	_ = writer.WriteField("csrf_token", csrfToken)
	_ = writer.WriteField("tree_name", "Arbre Test")
	part, err := writer.CreateFormFile("file", "test.ged")
	if err != nil {
		t.Fatalf("CreateFormFile ha fallat: %v", err)
	}
	gedcom := strings.Join([]string{
		"0 HEAD",
		"1 SOUR TEST",
		"1 GEDC",
		"0 @I1@ INDI",
		"1 NAME Joan /Prova/",
		"1 SEX M",
		"1 BIRT",
		"2 DATE 1 JAN 1900",
		"0 TRLR",
		"",
	}, "\n")
	if _, err := io.WriteString(part, gedcom); err != nil {
		t.Fatalf("escriure GEDCOM ha fallat: %v", err)
	}
	_ = writer.Close()

	req := httptest.NewRequest(http.MethodPost, "/espai/gedcom/upload", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfToken))
	rr := httptest.NewRecorder()
	app.RequireLogin(app.EspaiGedcomUpload)(rr, req)

	if rr.Code != http.StatusSeeOther {
		t.Fatalf("upload: esperava 303, rebut %d", rr.Code)
	}

	imports, err := database.ListEspaiImportsByOwner(user.ID)
	if err != nil || len(imports) == 0 {
		t.Fatalf("ListEspaiImportsByOwner ha fallat: %v len=%d", err, len(imports))
	}
	if imports[0].Status != "done" {
		t.Fatalf("esperava status done, rebut %q", imports[0].Status)
	}
}

func TestEspaiGrampsConnectAndSync(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := map[string]string{
		"DB_ENGINE":     "sqlite",
		"DB_PATH":       filepath.Join(tmpDir, "test_f25_gramps.sqlite3"),
		"RECREADB":      "true",
		"LOG_LEVEL":     "silent",
		"ESP_GRAMPS_SECRET": "test-secret",
		"ESP_GRAMPS_HTTP_TIMEOUT_SECONDS": "2",
	}
	app, database := newTestAppForConfig(t, cfg)

	user := createTestUser(t, database, "gramps_user")
	session := createSessionCookie(t, database, user.ID, "sess_gramps_user")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/health", "/api/v1/health":
			w.WriteHeader(http.StatusOK)
			return
		case "/api/people", "/api/people/":
			payload := []map[string]interface{}{
				{"gramps_id": "P1", "primary_name": map[string]interface{}{"first": "Anna", "surname": "Serra"}, "gender": "F"},
				{"gramps_id": "P2", "primary_name": map[string]interface{}{"first": "Pere", "surname": "Serra"}, "gender": "M"},
				{"gramps_id": "P3", "primary_name": map[string]interface{}{"first": "Clara", "surname": "Serra"}, "gender": "F"},
			}
			_ = json.NewEncoder(w).Encode(payload)
			return
		case "/api/families", "/api/families/":
			payload := []map[string]interface{}{
				{"father_id": "P2", "mother_id": "P1", "children": []string{"P3"}},
			}
			_ = json.NewEncoder(w).Encode(payload)
			return
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	csrfToken := "csrf_gramps_connect"
	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("base_url", server.URL)
	form.Set("token", "token")
	form.Set("tree_name", "Arbre Gramps")

	req := httptest.NewRequest(http.MethodPost, "/espai/integracions/gramps/connect", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfToken))
	rr := httptest.NewRecorder()
	app.RequireLogin(app.EspaiGrampsConnect)(rr, req)

	if rr.Code != http.StatusSeeOther {
		t.Fatalf("connect: esperava 303, rebut %d", rr.Code)
	}

	integracions, err := database.ListEspaiIntegracionsGrampsByOwner(user.ID)
	if err != nil || len(integracions) == 0 {
		t.Fatalf("ListEspaiIntegracionsGrampsByOwner ha fallat: %v len=%d", err, len(integracions))
	}
	if integracions[0].Status != "connected" {
		t.Fatalf("esperava status connected, rebut %q", integracions[0].Status)
	}

	arbres, err := database.ListEspaiArbresByOwner(user.ID)
	if err != nil || len(arbres) == 0 {
		t.Fatalf("ListEspaiArbresByOwner ha fallat: %v len=%d", err, len(arbres))
	}
	persones, err := database.ListEspaiPersonesByArbre(arbres[0].ID)
	if err != nil {
		t.Fatalf("ListEspaiPersonesByArbre ha fallat: %v", err)
	}
	if len(persones) < 2 {
		t.Fatalf("esperava persones >= 2, rebut %d", len(persones))
	}

	logs, err := database.ListEspaiIntegracioGrampsLogs(integracions[0].ID, 5)
	if err != nil {
		t.Fatalf("ListEspaiIntegracioGrampsLogs ha fallat: %v", err)
	}
	if len(logs) == 0 {
		t.Fatalf("esperava logs de sync")
	}

	if integracions[0].LastSyncAt.Valid {
		if time.Since(integracions[0].LastSyncAt.Time) > time.Minute {
			t.Fatalf("LastSyncAt massa antiga: %v", integracions[0].LastSyncAt.Time)
		}
	}
}
