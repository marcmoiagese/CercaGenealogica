package integration

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
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
		"ux_arxiu_abast_identity_id",
		"ux_arxiu_abast_identity_text",
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
		requiredTokens := []string{
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
			"ux_arxiu_abast_identity",
		}
		if rel != "db/MySQL.sql" {
			requiredTokens = append(requiredTokens, "ux_arxiu_abast_identity_id", "ux_arxiu_abast_identity_text")
		}
		for _, token := range requiredTokens {
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

	rr := postF354U12ArxiuAbast(t, app, session, "/documentals/arxius/"+strconv.Itoa(arxiuID)+"/abasts/new", url.Values{
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

	rr = postF354U12ArxiuAbast(t, app, session, "/documentals/arxius/"+strconv.Itoa(arxiuID)+"/abasts/new", url.Values{
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
	editBody, csrfCookie := getArxiuPageWithCSRF(t, app, session, "/documentals/arxius/"+strconv.Itoa(arxiuID)+"/edit", false)
	csrf := extractDeleteFormCSRFToken(t, editBody, relID)
	req := httptest.NewRequest(http.MethodPost, "/documentals/arxius/abasts/"+strconv.Itoa(relID)+"/delete", strings.NewReader("csrf_token="+url.QueryEscape(csrf)+"&return_to=%2Fdocumentals%2Farxius%2F"+strconv.Itoa(arxiuID)))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie)
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
	if !strings.Contains(src, `name="csrf_token"`) {
		t.Fatalf(`template ha de contenir name="csrf_token"`)
	}
	sections := readProjectFileF354(t, root, "templates/admin-arxiu-abast-sections.html")
	if !strings.Contains(sections, `name="csrf_token"`) {
		t.Fatalf(`partial d'abast ha de contenir csrf_token`)
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

func TestF354U12ArchiveScopeFormsRenderNonEmptyCSRFToken(t *testing.T) {
	app, database, _, session := setupF354U12ArxiuAbastAdmin(t, "test_f35_4u12_csrf_render.sqlite3")
	arxiuID := f354CreateArxiu(t, database, "Arxiu F35-4U12 CSRF "+strconv.FormatInt(time.Now().UnixNano(), 10))
	body, _ := getArxiuAbastFormWithCSRF(t, app, session, "/documentals/arxius/"+strconv.Itoa(arxiuID)+"/abasts/new")
	if strings.TrimSpace(extractCSRFTokenFromHTML(t, body)) == "" {
		t.Fatalf("el formulari new d'abast ha de renderitzar un csrf_token no buit")
	}
	relID, err := database.SaveArxiuAbast(&db.ArxiuAbast{
		ArxiuID:        arxiuID,
		TargetKind:     "free_text",
		TargetLabel:    "Abast CSRF delete",
		RelationKind:   "other",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	})
	if err != nil {
		t.Fatalf("SaveArxiuAbast delete csrf: %v", err)
	}
	editBody, _ := getArxiuPageWithCSRF(t, app, session, "/documentals/arxius/"+strconv.Itoa(arxiuID)+"/edit", false)
	if strings.TrimSpace(extractDeleteFormCSRFToken(t, editBody, relID)) == "" {
		t.Fatalf("el formulari delete d'abast ha de renderitzar un csrf_token no buit")
	}
}

func TestF354U12ArchiveScopeReturnToRejectsExternalTargets(t *testing.T) {
	app, database, _, session := setupF354U12ArxiuAbastAdmin(t, "test_f35_4u12_return_to.sqlite3")
	for idx, raw := range []string{"https://evil.example", "//evil.example"} {
		arxiuID := f354CreateArxiu(t, database, "Arxiu F35-4U12 Return "+strconv.FormatInt(time.Now().UnixNano(), 10)+strconv.Itoa(idx))
		arxiu, _ := database.GetArxiu(arxiuID)
		rr := postF354U12ArxiuAbast(t, app, session, "/documentals/arxius/"+strconv.Itoa(arxiuID)+"/abasts/new", url.Values{
			"arxiu_id":      {strconv.Itoa(arxiuID)},
			"target_kind":   {"municipi"},
			"target_id":     {strconv.Itoa(int(arxiu.MunicipiID.Int64))},
			"relation_kind": {"coverage"},
			"notes":         {"redirect check"},
			"return_to":     {raw},
		})
		if rr.Code != http.StatusSeeOther {
			t.Fatalf("save amb return_to=%q status=%d body=%s", raw, rr.Code, rr.Body.String())
		}
		expected := "/documentals/arxius/" + strconv.Itoa(arxiuID) + "/edit?notice=scope-pending"
		if rr.Header().Get("Location") != expected {
			t.Fatalf("return_to insegur %q ha de caure al fallback %q, got %q", raw, expected, rr.Header().Get("Location"))
		}
	}
}

func TestF354U12ArchiveScopeEditCannotMoveToAnotherArchive(t *testing.T) {
	app, database, _, session := setupF354U12ArxiuAbastAdmin(t, "test_f35_4u12_move.sqlite3")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	arxiuA := f354CreateArxiu(t, database, "Arxiu A F35-4U12 "+suffix)
	arxiuB := f354CreateArxiu(t, database, "Arxiu B F35-4U12 "+suffix)
	rowID, err := database.SaveArxiuAbast(&db.ArxiuAbast{
		ArxiuID:        arxiuA,
		TargetKind:     "free_text",
		TargetLabel:    "Abast inicial F35-4U12",
		RelationKind:   "other",
		Notes:          "before move",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	})
	if err != nil {
		t.Fatalf("SaveArxiuAbast initial: %v", err)
	}

	rr := postF354U12ArxiuAbast(t, app, session, "/documentals/arxius/abasts/"+strconv.Itoa(rowID)+"/edit", url.Values{
		"id":            {strconv.Itoa(rowID)},
		"arxiu_id":      {strconv.Itoa(arxiuB)},
		"target_kind":   {"free_text"},
		"target_label":  {"Abast inicial F35-4U12"},
		"relation_kind": {"other"},
		"notes":         {"tampered move"},
		"return_to":     {"/documentals/arxius/" + strconv.Itoa(arxiuB) + "/edit"},
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("edit tampered move status=%d body=%s", rr.Code, rr.Body.String())
	}
	updated, err := database.GetArxiuAbast(rowID)
	if err != nil || updated == nil {
		t.Fatalf("GetArxiuAbast after move attempt: err=%v row=%v", err, updated)
	}
	if updated.ArxiuID != arxiuA {
		t.Fatalf("l'abast no s'ha de moure d'arxiu: got=%d want=%d", updated.ArxiuID, arxiuA)
	}
	if strings.TrimSpace(updated.Notes) != "before move" {
		t.Fatalf("el bloqueig de move no ha d'actualitzar altres camps: got=%q", updated.Notes)
	}
	if !strings.Contains(rr.Body.String(), `name="arxiu_id" value="`+strconv.Itoa(arxiuA)+`"`) {
		t.Fatalf("el formulari re-renderitzat ha de conservar l'arxiu original, body=%s", rr.Body.String())
	}
}

func TestF354U12ArchiveScopeSQLiteUniqueConstraintBlocksDuplicates(t *testing.T) {
	_, database := newTestAppForLogin(t, "test_f35_4u12_unique.sqlite3")
	arxiuID := f354CreateArxiu(t, database, "Arxiu F35-4U12 Unique "+strconv.FormatInt(time.Now().UnixNano(), 10))
	if _, err := database.SaveArxiuAbast(&db.ArxiuAbast{
		ArxiuID:        arxiuID,
		TargetKind:     "free_text",
		TargetLabel:    "Mateix abast",
		RelationKind:   "coverage",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	}); err != nil {
		t.Fatalf("primer SaveArxiuAbast: %v", err)
	}
	if _, err := database.SaveArxiuAbast(&db.ArxiuAbast{
		ArxiuID:        arxiuID,
		TargetKind:     "free_text",
		TargetLabel:    "Mateix abast",
		RelationKind:   "coverage",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	}); err == nil {
		t.Fatalf("el duplicat SQLite ha de fallar per unique constraint")
	}
	if _, err := database.SaveArxiuAbast(&db.ArxiuAbast{
		ArxiuID:        arxiuID,
		TargetKind:     "free_text",
		TargetLabel:    "Abast diferent",
		RelationKind:   "coverage",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	}); err != nil {
		t.Fatalf("un segon abast diferent ha de funcionar: %v", err)
	}
}

func TestF354U12ArchiveScopeRejectsDuplicateRejectedScopeBeforeDB(t *testing.T) {
	app, database, _, session := setupF354U12ArxiuAbastAdmin(t, "test_f35_4u12_rejected_duplicate.sqlite3")
	arxiuID := f354CreateArxiu(t, database, "Arxiu F35-4U12 Rejected "+strconv.FormatInt(time.Now().UnixNano(), 10))
	if _, err := database.SaveArxiuAbast(&db.ArxiuAbast{
		ArxiuID:        arxiuID,
		TargetKind:     "free_text",
		TargetLabel:    "Abast rebutjat",
		RelationKind:   "coverage",
		Estat:          "actiu",
		ModeracioEstat: "rebutjat",
	}); err != nil {
		t.Fatalf("SaveArxiuAbast rebutjat: %v", err)
	}
	rr := postF354U12ArxiuAbast(t, app, session, "/documentals/arxius/"+strconv.Itoa(arxiuID)+"/abasts/new", url.Values{
		"arxiu_id":      {strconv.Itoa(arxiuID)},
		"target_kind":   {"free_text"},
		"target_label":  {"Abast rebutjat"},
		"relation_kind": {"coverage"},
		"notes":         {"duplicat rebutjat"},
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("el duplicat sobre un rebutjat ha de fallar en validacio, got=%d body=%s", rr.Code, rr.Body.String())
	}
	rows, err := database.ListArxiuAbasts(arxiuID, "", "")
	if err != nil {
		t.Fatalf("ListArxiuAbasts: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("no s'ha de crear un segon registre duplicat; rows=%d", len(rows))
	}
}

func TestF354U12ArchiveScopeStableIdentityUsesTargetID(t *testing.T) {
	_, database := newTestAppForLogin(t, "test_f35_4u12_identity_id.sqlite3")
	arxiuID := f354CreateArxiu(t, database, "Arxiu F35-4U12 Stable ID "+strconv.FormatInt(time.Now().UnixNano(), 10))
	arxiu, err := database.GetArxiu(arxiuID)
	if err != nil || arxiu == nil || !arxiu.MunicipiID.Valid {
		t.Fatalf("GetArxiu stable identity: err=%v arxiu=%v", err, arxiu)
	}
	targetID := sql.NullInt64{Int64: arxiu.MunicipiID.Int64, Valid: true}
	if _, err := database.SaveArxiuAbast(&db.ArxiuAbast{
		ArxiuID:        arxiuID,
		TargetKind:     "municipi",
		TargetID:       targetID,
		TargetCode:     "CODI-1",
		TargetLabel:    "Nom original",
		RelationKind:   "coverage",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	}); err != nil {
		t.Fatalf("primer SaveArxiuAbast ID-backed: %v", err)
	}
	if _, err := database.SaveArxiuAbast(&db.ArxiuAbast{
		ArxiuID:        arxiuID,
		TargetKind:     "municipi",
		TargetID:       targetID,
		TargetCode:     "CODI-2",
		TargetLabel:    "Nom canviat",
		RelationKind:   "coverage",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	}); err == nil {
		t.Fatalf("un target ID-backed amb el mateix target_id ha de fallar encara que canvii el label")
	}
}

func TestF354U12ArchiveScopeLevelKindMismatchDoesNotInsert(t *testing.T) {
	app, database, _, session := setupF354U12ArxiuAbastAdmin(t, "test_f35_4u12_level_mismatch.sqlite3")
	arxiuID := f354CreateArxiu(t, database, "Arxiu F35-4U12 Level "+strconv.FormatInt(time.Now().UnixNano(), 10))
	paisID, err := database.CreatePais(&db.Pais{
		CodiISO2:    "FQ",
		CodiISO3:    "FQT",
		CodiPaisNum: "998",
	})
	if err != nil {
		t.Fatalf("CreatePais mismatch: %v", err)
	}
	paisLevelID, err := database.CreateNivell(&db.NivellAdministratiu{
		PaisID:         paisID,
		Nivel:          1,
		NomNivell:      "Pais mismatch F35-4U12",
		TipusNivell:    "pais",
		CodiOficial:    "FQ",
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateNivell pais mismatch: %v", err)
	}
	nivellID, err := database.CreateNivell(&db.NivellAdministratiu{
		PaisID:         paisID,
		Nivel:          2,
		NomNivell:      "Provincia mismatch F35-4U12",
		TipusNivell:    "provincia",
		CodiOficial:    "FQ-P",
		ParentID:       sql.NullInt64{Int64: int64(paisLevelID), Valid: true},
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateNivell provincia mismatch: %v", err)
	}
	rr := postF354U12ArxiuAbast(t, app, session, "/documentals/arxius/"+strconv.Itoa(arxiuID)+"/abasts/new", url.Values{
		"arxiu_id":      {strconv.Itoa(arxiuID)},
		"target_kind":   {"comarca"},
		"target_id":     {strconv.Itoa(nivellID)},
		"relation_kind": {"coverage"},
		"notes":         {"mismatch"},
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("el mismatch de nivell ha de re-renderitzar el formulari, got=%d body=%s", rr.Code, rr.Body.String())
	}
	rows, err := database.ListArxiuAbasts(arxiuID, "", "")
	if err != nil {
		t.Fatalf("ListArxiuAbasts mismatch: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("el mismatch de nivell no ha d'inserir registres; rows=%d", len(rows))
	}
	if !strings.Contains(rr.Body.String(), `name="target_kind"`) {
		t.Fatalf("el formulari s'ha de tornar a renderitzar en el mismatch de nivell")
	}
}

func TestF354U12ArchiveScopeLegacyMunicipiLabelFallback(t *testing.T) {
	app, database, _, session := setupF354U12ArxiuAbastAdmin(t, "test_f35_4u12_legacy_label.sqlite3")
	arxiuID := f354CreateArxiu(t, database, "Arxiu F35-4U12 Legacy Label "+strconv.FormatInt(time.Now().UnixNano(), 10))
	arxiu, err := database.GetArxiu(arxiuID)
	if err != nil || arxiu == nil || !arxiu.MunicipiID.Valid {
		t.Fatalf("GetArxiu legacy label: err=%v arxiu=%v", err, arxiu)
	}
	if _, err := database.SaveArxiuAbast(&db.ArxiuAbast{
		ArxiuID:        arxiuID,
		TargetKind:     "municipi",
		TargetID:       sql.NullInt64{Int64: arxiu.MunicipiID.Int64, Valid: true},
		TargetLabel:    "",
		TargetCode:     "",
		RelationKind:   "coverage",
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	}); err != nil {
		t.Fatalf("SaveArxiuAbast legacy municipi: %v", err)
	}
	body := f354Get(t, app.AdminEditArxiu, "/documentals/arxius/"+strconv.Itoa(arxiuID)+"/edit", session)
	municipiNom := ""
	if mun, munErr := database.GetMunicipi(int(arxiu.MunicipiID.Int64)); munErr == nil && mun != nil {
		municipiNom = strings.TrimSpace(mun.Nom)
	}
	if municipiNom == "" || !strings.Contains(body, municipiNom) {
		t.Fatalf("la UI ha de fer fallback al nom del municipi legacy; municipi=%q body=%s", municipiNom, body)
	}
}

func TestF354U12AdminNewArxiuFormDoesNotPanicWithoutArchive(t *testing.T) {
	app, _, _, session := setupF354U12ArxiuAbastAdmin(t, "test_f35_4u12_new_arxiu.sqlite3")
	body := f354Get(t, app.AdminNewArxiu, "/documentals/arxius/new", session)
	if strings.Contains(body, "/abasts/new?return_to=") {
		t.Fatalf("el formulari de nou arxiu no ha de renderitzar links d'abast trencats, body=%s", body)
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

func postF354U12ArxiuAbast(t *testing.T, app *core.App, session *http.Cookie, formPath string, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	csrfToken, csrfCookie := extractCSRFContextFromArxiuAbastForm(t, app, session, formPath)
	form.Set("csrf_token", csrfToken)
	req := httptest.NewRequest(http.MethodPost, "/documentals/arxius/abasts/save", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie)
	rr := httptest.NewRecorder()
	app.AdminSaveArxiuAbast(rr, req)
	return rr
}

func extractCSRFContextFromArxiuAbastForm(t *testing.T, app *core.App, session *http.Cookie, path string) (string, *http.Cookie) {
	t.Helper()
	body, csrfCookie := getArxiuAbastFormWithCSRF(t, app, session, path)
	return extractCSRFTokenFromHTML(t, body), csrfCookie
}

func getArxiuAbastFormWithCSRF(t *testing.T, app *core.App, session *http.Cookie, path string) (string, *http.Cookie) {
	t.Helper()
	return getArxiuPageWithCSRF(t, app, session, path, strings.HasSuffix(path, "/edit"))
}

func getArxiuPageWithCSRF(t *testing.T, app *core.App, session *http.Cookie, path string, isAbastEdit bool) (string, *http.Cookie) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	switch {
	case isAbastEdit:
		app.AdminEditArxiuAbast(rr, req)
	case strings.Contains(path, "/abasts/new"):
		app.AdminNewArxiuAbastFromArxiu(rr, req)
	case strings.HasSuffix(path, "/edit"):
		app.AdminEditArxiu(rr, req)
	default:
		app.AdminShowArxiu(rr, req)
	}
	if rr.Code != http.StatusOK {
		t.Fatalf("GET %s per extreure CSRF ha fallat: %d body=%s", path, rr.Code, rr.Body.String())
	}
	resp := rr.Result()
	defer resp.Body.Close()
	for _, cookie := range resp.Cookies() {
		if cookie.Name == "cg_csrf" {
			cloned := *cookie
			return rr.Body.String(), &cloned
		}
	}
	t.Fatalf("no s'ha trobat la cookie cg_csrf a %s", path)
	return "", nil
}

func extractDeleteFormCSRFToken(t *testing.T, body string, relID int) string {
	t.Helper()
	re := regexp.MustCompile(`(?s)action="/documentals/arxius/abasts/` + strconv.Itoa(relID) + `/delete"[^>]*>.*?name="csrf_token" value="([^"]+)"`)
	match := re.FindStringSubmatch(body)
	if len(match) != 2 {
		t.Fatalf("no s'ha trobat el csrf_token del formulari delete per relID=%d al body=%s", relID, body)
	}
	return match[1]
}
