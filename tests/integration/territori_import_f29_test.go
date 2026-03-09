package integration

import (
	"bytes"
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
