package integration

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF354U12ArxiuAbastSchemaSQLite(t *testing.T) {
	_, database := newTestAppForLogin(t, "test_f35_4u12_schema.sqlite3")
	if !f351SQLiteTableExists(t, database, "arxiu_abast") {
		t.Fatalf("taula arxiu_abast no creada")
	}
	got := f351SQLiteColumns(t, database, "arxiu_abast")
	for _, column := range []string{
		"id", "arxiu_id", "target_kind", "target_id", "target_code", "target_label",
		"relation_kind", "notes", "estat", "moderation_status", "created_by", "updated_by",
		"moderated_by", "moderated_at", "created_at", "updated_at",
	} {
		if !got[column] {
			t.Fatalf("arxiu_abast no te columna %s; columns=%v", column, got)
		}
	}
	for _, idx := range []string{
		"idx_arxiu_abast_arxiu",
		"idx_arxiu_abast_target",
		"idx_arxiu_abast_moderacio",
		"idx_arxiu_abast_relacio",
	} {
		if !f351SQLiteIndexExists(t, database, idx) {
			t.Fatalf("index esperat no creat: %s", idx)
		}
	}
	fks := f351SQLiteFKs(t, database, "arxiu_abast")
	if !fks["arxiu_id->arxius"] {
		t.Fatalf("FK arxiu_id->arxius absent: %v", fks)
	}
}

func TestF354U12ArxiuAbastSQLFilesAligned(t *testing.T) {
	root := findProjectRoot(t)
	for _, rel := range []string{"db/SQLite.sql", "db/PostgreSQL.sql", "db/MySQL.sql"} {
		body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("no s'ha pogut llegir %s: %v", rel, err)
		}
		src := string(body)
		for _, token := range []string{
			"CREATE TABLE IF NOT EXISTS arxiu_abast",
			"target_kind",
			"target_id",
			"target_code",
			"target_label",
			"relation_kind",
			"moderation_status",
			"idx_arxiu_abast_arxiu",
			"idx_arxiu_abast_target",
			"idx_arxiu_abast_moderacio",
			"idx_arxiu_abast_relacio",
		} {
			if !strings.Contains(src, token) {
				t.Fatalf("%s no conte token F35-4U12 %q", rel, token)
			}
		}
	}
	body := readProjectFileF354(t, root, "db/arxiu_abast_per_motor.go")
	for _, required := range []string{
		"func sqliteListArxiuAbasts(",
		"func sqliteGetArxiuAbast(",
		"func sqliteSaveArxiuAbast(",
		"func sqliteDeleteArxiuAbast(",
		"func sqliteUpdateArxiuAbastModeracio(",
		"func postgresListArxiuAbasts(",
		"func mysqlListArxiuAbasts(",
		"target_kind = ?",
		"target_kind = $2",
		"moderation_status = $3",
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("falta contracte DB per motor F35-4U12: %s", required)
		}
	}
}

func TestF354U12ArxiuEditShowsScopeSection(t *testing.T) {
	app, database, _, session := setupF354U12ArxiuAbastAdmin(t, "test_f35_4u12_edit_ui.sqlite3")
	arxiuID := f354CreateArxiu(t, database, "Arxiu F35-4U12 UI "+strconv.FormatInt(time.Now().UnixNano(), 10))

	body := f354Get(t, app.AdminEditArxiu, "/documentals/arxius/"+strconv.Itoa(arxiuID)+"/edit", session)
	for _, want := range []string{
		"Abast de l&#39;arxiu",
		"Abast territorial/administratiu",
		"Abast religi",
		"Altres institucions relacionades",
		"/documentals/arxius/" + strconv.Itoa(arxiuID) + "/abasts/new?return_to=/documentals/arxius/" + strconv.Itoa(arxiuID) + "/edit",
		"Entitats religioses relacionades",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("la pantalla d'edicio ha de contenir %q, body=%s", want, body)
		}
	}
}

func TestF354U12CreateTerritorialAndReligiousArchiveScopes(t *testing.T) {
	app, database, admin, session := setupF354U12ArxiuAbastAdmin(t, "test_f35_4u12_create.sqlite3")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	arxiuID := f354CreateArxiu(t, database, "Arxiu F35-4U12 "+suffix)
	arxiu, err := database.GetArxiu(arxiuID)
	if err != nil || arxiu == nil || !arxiu.MunicipiID.Valid {
		t.Fatalf("GetArxiu: err=%v arxiu=%v", err, arxiu)
	}
	entitatID := f353YCreateEntitat(t, database, "Parroquia F35-4U12 "+suffix, "publicat")

	rr := postF354U12ArxiuAbast(t, app, session, url.Values{
		"arxiu_id":      {strconv.Itoa(arxiuID)},
		"target_kind":   {"municipi"},
		"target_id":     {strconv.Itoa(int(arxiu.MunicipiID.Int64))},
		"relation_kind": {"coverage"},
		"notes":         {"abast municipal F35-4U12"},
		"return_to":     {"/documentals/arxius/" + strconv.Itoa(arxiuID) + "/edit?from=test"},
	})
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("save municipi status=%d body=%s", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("Location") != "/documentals/arxius/"+strconv.Itoa(arxiuID)+"/edit?from=test" {
		t.Fatalf("return_to municipi no respectat: %s", rr.Header().Get("Location"))
	}

	rr = postF354U12ArxiuAbast(t, app, session, url.Values{
		"arxiu_id":      {strconv.Itoa(arxiuID)},
		"target_kind":   {"religious_entity"},
		"target_id":     {strconv.Itoa(entitatID)},
		"relation_kind": {"jurisdiction"},
		"notes":         {"abast religios F35-4U12"},
		"return_to":     {"/documentals/arxius/" + strconv.Itoa(arxiuID)},
	})
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("save religios status=%d body=%s", rr.Code, rr.Body.String())
	}

	rows, err := database.ListArxiuAbasts(arxiuID, "", "")
	if err != nil || len(rows) != 2 {
		t.Fatalf("ListArxiuAbasts: err=%v rows=%d", err, len(rows))
	}
	foundMunicipi := false
	foundReligious := false
	for _, row := range rows {
		if row.TargetKind == "municipi" {
			foundMunicipi = row.RelationKind == "coverage" && row.ModeracioEstat == "pendent" && row.CreatedBy.Valid && int(row.CreatedBy.Int64) == admin.ID
		}
		if row.TargetKind == "religious_entity" {
			foundReligious = row.RelationKind == "jurisdiction" && row.ModeracioEstat == "pendent" && strings.Contains(row.TargetLabel, "Parroquia F35-4U12")
		}
	}
	if !foundMunicipi || !foundReligious {
		t.Fatalf("abasts no desats amb el contracte esperat: %+v", rows)
	}

	body := f354Get(t, app.AdminEditArxiu, "/documentals/arxius/"+strconv.Itoa(arxiuID)+"/edit", session)
	for _, want := range []string{"abast municipal F35-4U12", "Parroquia F35-4U12", "abast religios F35-4U12"} {
		if !strings.Contains(body, want) {
			t.Fatalf("la UI d'edicio ha de mostrar %q, body=%s", want, body)
		}
	}
}

func TestF354U12ArchiveScopeDeleteRespectsReturnTo(t *testing.T) {
	app, database, _, session := setupF354U12ArxiuAbastAdmin(t, "test_f35_4u12_delete.sqlite3")
	arxiuID := f354CreateArxiu(t, database, "Arxiu F35-4U12 Delete "+strconv.FormatInt(time.Now().UnixNano(), 10))
	arxiu, _ := database.GetArxiu(arxiuID)
	relID, err := database.SaveArxiuAbast(&db.ArxiuAbast{
		ArxiuID:        arxiuID,
		TargetKind:     "municipi",
		TargetID:       sql.NullInt64{Int64: arxiu.MunicipiID.Int64, Valid: true},
		TargetLabel:    "Municipi delete F35-4U12",
		RelationKind:   "coverage",
		Notes:          "pendent delete",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	})
	if err != nil {
		t.Fatalf("SaveArxiuAbast: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/documentals/arxius/abasts/"+strconv.Itoa(relID)+"/delete", strings.NewReader("return_to=%2Fdocumentals%2Farxius%2F"+strconv.Itoa(arxiuID)))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminDeleteArxiuAbast(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("delete abast status=%d body=%s", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("Location") != "/documentals/arxius/"+strconv.Itoa(arxiuID) {
		t.Fatalf("return_to delete no respectat: %s", rr.Header().Get("Location"))
	}
	if row, err := database.GetArxiuAbast(relID); err == nil && row != nil {
		t.Fatalf("l'abast pendent s'hauria d'eliminar fisicament, got=%+v", row)
	}
}

func TestF354U12ArchiveScopeTemplatePreservesCSPAndSuggestUI(t *testing.T) {
	root := findProjectRoot(t)
	src := readProjectFileF354(t, root, "templates/admin-arxiu-abast-form.html")
	for _, forbidden := range []string{"<script>", "<style>", "style=", "onclick=", "javascript:"} {
		if strings.Contains(src, forbidden) {
			t.Fatalf("template admin-arxiu-abast-form.html viola CSP amb %q", forbidden)
		}
	}
	if !strings.Contains(src, `data-suggest="1"`) {
		t.Fatalf(`template ha de contenir data-suggest="1"`)
	}
	js := readProjectFileF354(t, root, "static/js/arxiu-abast-form.js")
	for _, required := range []string{
		"/api/confessional/entitats/suggest",
		"/api/territori/municipis/suggest",
		"/api/territori/nivells/suggest",
	} {
		if !strings.Contains(js, required) {
			t.Fatalf("JS arxiu-abast-form ha de contenir %q", required)
		}
	}
}

func setupF354U12ArxiuAbastAdmin(t *testing.T, dbName string) (*core.App, db.DB, *db.User, *http.Cookie) {
	t.Helper()
	app, database := newTestAppForLogin(t, dbName)
	admin := createTestUser(t, database, "f35_4u12_admin_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	policy := createPolicyWithGrant(t, database, "f35_4u12_policy_"+strconv.FormatInt(time.Now().UnixNano(), 10), "documentals.arxius.view")
	for _, grant := range []string{
		"documentals.arxius.edit",
		"documentals.arxius.delete",
		"territori.confessional.arxius_entitats.create",
		"territori.confessional.arxius_entitats.edit",
		"territori.confessional.arxius_entitats.delete",
		"territori.confessional.entitats.view",
		"territori.municipis.view",
		"territori.nivells.view",
	} {
		addGrantToPolicy(t, database, policy, grant)
	}
	assignPolicyToUser(t, database, admin.ID, policy)
	session := createSessionCookie(t, database, admin.ID, "sess_f35_4u12_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	return app, database, admin, session
}

func postF354U12ArxiuAbast(t *testing.T, app *core.App, session *http.Cookie, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/documentals/arxius/abasts/save", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminSaveArxiuAbast(rr, req)
	return rr
}
