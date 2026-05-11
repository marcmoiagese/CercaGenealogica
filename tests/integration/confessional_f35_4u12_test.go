package integration

import (
	"database/sql"
	"encoding/json"
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
			"nivell_administratiu",
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
	arxiuID, arxiu := f354U12CreateArchiveWithLoadedMunicipi(t, database, "Arxiu F35-4U12 "+suffix)
	entitatID := f353YCreateEntitat(t, database, "Parroquia F35-4U12 "+suffix, "publicat")

	rr := postF354U12ArxiuAbast(t, app, session, f354U12ArxiuAbastNewPath(arxiuID), f354U12ScopeFormValues(arxiuID, "municipi", strconv.Itoa(int(arxiu.MunicipiID.Int64)), "", "coverage", "abast municipal F35-4U12", "/documentals/arxius/"+strconv.Itoa(arxiuID)+"/edit?from=test"))
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("save municipi status=%d body=%s", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("Location") != "/documentals/arxius/"+strconv.Itoa(arxiuID)+"/edit?from=test" {
		t.Fatalf("return_to municipi no respectat: %s", rr.Header().Get("Location"))
	}

	rr = postF354U12ArxiuAbast(t, app, session, f354U12ArxiuAbastNewPath(arxiuID), f354U12ScopeFormValues(arxiuID, "religious_entity", strconv.Itoa(entitatID), "", "jurisdiction", "abast religios F35-4U12", "/documentals/arxius/"+strconv.Itoa(arxiuID)))
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
	arxiuID, arxiu := f354U12CreateArchiveWithLoadedMunicipi(t, database, "Arxiu F35-4U12 Delete "+strconv.FormatInt(time.Now().UnixNano(), 10))
	relID := f354U12SaveScope(t, database, f354U12MunicipiScope(arxiuID, arxiu.MunicipiID.Int64, "Municipi delete F35-4U12", "coverage", "pendent"))
	editBody, csrfCookie := getArxiuEditPageWithCSRF(t, app, session, arxiuID)
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
		"target_kind=comarca",
		"target_kind=provincia",
		"target_kind=comunitat_autonoma",
		"target_kind=estat",
		"target_kind=nivell_administratiu",
	} {
		if !strings.Contains(js, required) {
			t.Fatalf("JS arxiu-abast-form ha de contenir %q", required)
		}
	}
	for _, forbidden := range []string{"nivel=1", "nivel=2", "nivel=3", "nivel=4"} {
		if strings.Contains(js, forbidden) {
			t.Fatalf("JS arxiu-abast-form no ha d'usar mapping numeric antic %q", forbidden)
		}
	}
}

func TestF354U12ArchiveScopeFormsRenderNonEmptyCSRFToken(t *testing.T) {
	app, database, _, session := setupF354U12ArxiuAbastAdmin(t, "test_f35_4u12_csrf_render.sqlite3")
	arxiuID, _ := f354U12CreateArchiveWithLoadedMunicipi(t, database, "Arxiu F35-4U12 CSRF "+strconv.FormatInt(time.Now().UnixNano(), 10))
	body, _ := getArxiuAbastNewFormWithCSRF(t, app, session, arxiuID)
	if strings.TrimSpace(extractCSRFTokenFromHTML(t, body)) == "" {
		t.Fatalf("el formulari new d'abast ha de renderitzar un csrf_token no buit")
	}
	relID := f354U12SaveScope(t, database, f354U12FreeTextScope(arxiuID, "Abast CSRF delete", "other", "pendent"))
	editBody, _ := getArxiuEditPageWithCSRF(t, app, session, arxiuID)
	if strings.TrimSpace(extractDeleteFormCSRFToken(t, editBody, relID)) == "" {
		t.Fatalf("el formulari delete d'abast ha de renderitzar un csrf_token no buit")
	}
}

func TestF354U12ArchiveScopeReturnToRejectsExternalTargets(t *testing.T) {
	app, database, _, session := setupF354U12ArxiuAbastAdmin(t, "test_f35_4u12_return_to.sqlite3")
	for idx, raw := range []string{"https://evil.example", "//evil.example"} {
		arxiuID, arxiu := f354U12CreateArchiveWithLoadedMunicipi(t, database, "Arxiu F35-4U12 Return "+strconv.FormatInt(time.Now().UnixNano(), 10)+strconv.Itoa(idx))
		rr := postF354U12ArxiuAbast(t, app, session, f354U12ArxiuAbastNewPath(arxiuID), f354U12ScopeFormValues(arxiuID, "municipi", strconv.Itoa(int(arxiu.MunicipiID.Int64)), "", "coverage", "redirect check", raw))
		if rr.Code != http.StatusSeeOther {
			t.Fatalf("save amb return_to=%q status=%d body=%s", raw, rr.Code, rr.Body.String())
		}
		expected := "/documentals/arxius/" + strconv.Itoa(arxiuID) + "/edit?notice=scope-pending"
		if rr.Header().Get("Location") != expected {
			t.Fatalf("return_to insegur %q ha de caure al fallback %q, got %q", raw, expected, rr.Header().Get("Location"))
		}
	}
}

func TestF354U12GetArxiuAbastFormWithCSRFTreatsEditQueryAsEdit(t *testing.T) {
	app, database, _, session := setupF354U12ArxiuAbastAdmin(t, "test_f35_4u12_form_helper.sqlite3")
	arxiuID := f354CreateArxiu(t, database, "Arxiu F35-4U12 Form Helper "+strconv.FormatInt(time.Now().UnixNano(), 10))
	relID := f354U12SaveScope(t, database, f354U12FreeTextScope(arxiuID, "Abast helper edit", "other", "pendent"))
	path := "/documentals/arxius/abasts/" + strconv.Itoa(relID) + "/edit?return_to=%2Fdocumentals%2Farxius%2F" + strconv.Itoa(arxiuID) + "%2Fedit"

	body, _ := getArxiuAbastFormWithCSRF(t, app, session, path)
	if !strings.Contains(body, `name="id" value="`+strconv.Itoa(relID)+`"`) {
		t.Fatalf("el helper ha de tractar %q com a formulari edit, body=%s", path, body)
	}
}

func TestF354U12ArchiveScopeEditCannotMoveToAnotherArchive(t *testing.T) {
	app, database, _, session := setupF354U12ArxiuAbastAdmin(t, "test_f35_4u12_move.sqlite3")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	arxiuA := f354CreateArxiu(t, database, "Arxiu A F35-4U12 "+suffix)
	arxiuB := f354CreateArxiu(t, database, "Arxiu B F35-4U12 "+suffix)
	rowID := f354U12SaveScope(t, database, f354U12FreeTextScope(arxiuA, "Abast inicial F35-4U12", "other", "pendent"))
	f354U12UpdateScopeNotes(t, database, rowID, "before move")

	rr := postF354U12ArxiuAbast(t, app, session, f354U12ArxiuAbastEditPath(rowID), f354U12EditScopeFormValues(f354U12EditScopeFormInput{
		ScopeID:      rowID,
		ArxiuID:      arxiuB,
		TargetKind:   "free_text",
		TargetLabel:  "Abast inicial F35-4U12",
		RelationKind: "other",
		Notes:        "tampered move",
		ReturnTo:     "/documentals/arxius/" + strconv.Itoa(arxiuB) + "/edit",
	}))
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
	f354U12SaveScope(t, database, f354U12FreeTextScope(arxiuID, "Mateix abast", "coverage", "pendent"))
	if _, err := database.SaveArxiuAbast(f354U12FreeTextScope(arxiuID, "Mateix abast", "coverage", "pendent")); err == nil {
		t.Fatalf("el duplicat SQLite ha de fallar per unique constraint")
	}
	if _, err := database.SaveArxiuAbast(f354U12FreeTextScope(arxiuID, "Abast diferent", "coverage", "pendent")); err != nil {
		t.Fatalf("un segon abast diferent ha de funcionar: %v", err)
	}
}

func TestF354U12ArchiveScopeRejectsDuplicateRejectedScopeBeforeDB(t *testing.T) {
	app, database, _, session := setupF354U12ArxiuAbastAdmin(t, "test_f35_4u12_rejected_duplicate.sqlite3")
	arxiuID := f354CreateArxiu(t, database, "Arxiu F35-4U12 Rejected "+strconv.FormatInt(time.Now().UnixNano(), 10))
	if _, err := database.SaveArxiuAbast(f354U12FreeTextScope(arxiuID, "Abast rebutjat", "coverage", "rebutjat")); err != nil {
		t.Fatalf("SaveArxiuAbast rebutjat: %v", err)
	}
	rr := postF354U12ArxiuAbast(t, app, session, f354U12ArxiuAbastNewPath(arxiuID), f354U12ScopeFormValues(arxiuID, "free_text", "", "Abast rebutjat", "coverage", "duplicat rebutjat", ""))
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
	arxiuID, arxiu := f354U12CreateArchiveWithLoadedMunicipi(t, database, "Arxiu F35-4U12 Stable ID "+strconv.FormatInt(time.Now().UnixNano(), 10))
	targetID := sql.NullInt64{Int64: arxiu.MunicipiID.Int64, Valid: true}
	if _, err := database.SaveArxiuAbast(f354U12MunicipiScope(arxiuID, targetID.Int64, "Nom original", "coverage", "pendent")); err != nil {
		t.Fatalf("primer SaveArxiuAbast ID-backed: %v", err)
	}
	if _, err := database.SaveArxiuAbast(f354U12MunicipiScope(arxiuID, targetID.Int64, "Nom canviat", "coverage", "pendent")); err == nil {
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
	rr := postF354U12ArxiuAbast(t, app, session, f354U12ArxiuAbastNewPath(arxiuID), f354U12ScopeFormValues(arxiuID, "comarca", strconv.Itoa(nivellID), "", "coverage", "mismatch", ""))
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

func TestF354U12ArchiveScopeAdministrativeKindValidationAndGenericLevel(t *testing.T) {
	app, database, _, session := setupF354U12ArxiuAbastAdmin(t, "test_f35_4u12_admin_kind_validation.sqlite3")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	arxiuID := f354CreateArxiu(t, database, "Arxiu F35-4U12 Admin Kind "+suffix)
	levels := f354U12CreateAdministrativeSuggestFixture(t, database, f354U12SuggestFixtureInput{
		CountryISO2: "QA",
		CountryISO3: "QAT",
		CountryNum:  "997",
		Suffix:      suffix,
	})

	rr := postF354U12ArxiuAbast(t, app, session, f354U12ArxiuAbastNewPath(arxiuID), f354U12ScopeFormValues(arxiuID, "provincia", strconv.Itoa(levels.ComarcaID), "", "coverage", "mismatch provincia", ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("provincia amb comarca ha de fallar, got=%d body=%s", rr.Code, rr.Body.String())
	}
	rr = postF354U12ArxiuAbast(t, app, session, f354U12ArxiuAbastNewPath(arxiuID), f354U12ScopeFormValues(arxiuID, "comarca", strconv.Itoa(levels.ProvinciaID), "", "coverage", "mismatch comarca", ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("comarca amb provincia ha de fallar, got=%d body=%s", rr.Code, rr.Body.String())
	}

	rr = postF354U12ArxiuAbast(t, app, session, f354U12ArxiuAbastNewPath(arxiuID), f354U12ScopeFormValues(arxiuID, "provincia", strconv.Itoa(levels.ProvinciaID), "", "coverage", "ok provincia", ""))
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("provincia amb provincia ha de funcionar, got=%d body=%s", rr.Code, rr.Body.String())
	}
	rr = postF354U12ArxiuAbast(t, app, session, f354U12ArxiuAbastNewPath(arxiuID), f354U12ScopeFormValues(arxiuID, "comarca", strconv.Itoa(levels.ComarcaID), "", "coverage", "ok comarca", ""))
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("comarca amb comarca ha de funcionar, got=%d body=%s", rr.Code, rr.Body.String())
	}
	rr = postF354U12ArxiuAbast(t, app, session, f354U12ArxiuAbastNewPath(arxiuID), f354U12ScopeFormValues(arxiuID, "estat", strconv.Itoa(levels.PaisLevelID), "", "coverage", "ok estat", ""))
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("estat amb pais ha de funcionar, got=%d body=%s", rr.Code, rr.Body.String())
	}
	rr = postF354U12ArxiuAbast(t, app, session, f354U12ArxiuAbastNewPath(arxiuID), f354U12ScopeFormValues(arxiuID, "nivell_administratiu", strconv.Itoa(levels.DepartamentID), "", "coverage", "ok generic", ""))
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("nivell_administratiu amb departament ha de funcionar, got=%d body=%s", rr.Code, rr.Body.String())
	}
	rr = postF354U12ArxiuAbast(t, app, session, f354U12ArxiuAbastNewPath(arxiuID), f354U12ScopeFormValues(arxiuID, "nivell_administratiu", strconv.Itoa(levels.DepartamentID), "", "coverage", "duplicat generic", ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("duplicat generic ha de fallar en validacio, got=%d body=%s", rr.Code, rr.Body.String())
	}

	rows, err := database.ListArxiuAbasts(arxiuID, "", "")
	if err != nil {
		t.Fatalf("ListArxiuAbasts admin kind validation: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("s'esperaven 4 abasts valids desats, got=%d rows=%+v", len(rows), rows)
	}
	foundGeneric := false
	for _, row := range rows {
		if row.TargetKind == "nivell_administratiu" {
			foundGeneric = row.TargetID.Valid && int(row.TargetID.Int64) == levels.DepartamentID && strings.TrimSpace(row.TargetLabel) == levels.DepartamentNom
		}
	}
	if !foundGeneric {
		t.Fatalf("cal desar l'abast generic amb target_id i label reals, rows=%+v", rows)
	}

	body := f354Get(t, app.AdminEditArxiu, "/documentals/arxius/"+strconv.Itoa(arxiuID)+"/edit", session)
	if !strings.Contains(body, levels.DepartamentNom) {
		t.Fatalf("la UI ha de mostrar el nivell generic desat, body=%s", body)
	}
}

func TestF354U12AdminNivellsSuggestFiltersAdministrativeKinds(t *testing.T) {
	app, database, _, session := setupF354U12ArxiuAbastAdmin(t, "test_f35_4u12_nivells_suggest.sqlite3")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	levels := f354U12CreateAdministrativeSuggestFixture(t, database, f354U12SuggestFixtureInput{
		CountryISO2: "QB",
		CountryISO3: "QBT",
		CountryNum:  "996",
		Suffix:      suffix,
	})

	provItems := f354U12SuggestNivells(t, app, session, "/api/territori/nivells/suggest?q=Suggest+F35R5&target_kind=provincia")
	if !f354U12SuggestContainsNom(provItems, levels.ProvinciaNom) || f354U12SuggestContainsNom(provItems, levels.ComarcaNom) {
		t.Fatalf("target_kind=provincia ha de retornar nomes provincies: %+v", provItems)
	}

	comarcaItems := f354U12SuggestNivells(t, app, session, "/api/territori/nivells/suggest?q=Suggest+F35R5&target_kind=comarca")
	if !f354U12SuggestContainsNom(comarcaItems, levels.ComarcaNom) || f354U12SuggestContainsNom(comarcaItems, levels.ProvinciaNom) {
		t.Fatalf("target_kind=comarca ha de retornar nomes comarques: %+v", comarcaItems)
	}

	comunitatItems := f354U12SuggestNivells(t, app, session, "/api/territori/nivells/suggest?q=Suggest+F35R5&target_kind=comunitat_autonoma")
	if !f354U12SuggestContainsNom(comunitatItems, levels.ComunitatNom) || f354U12SuggestContainsNom(comunitatItems, levels.ComarcaNom) || f354U12SuggestContainsNom(comunitatItems, levels.ProvinciaNom) || f354U12SuggestContainsNom(comunitatItems, levels.DepartamentNom) {
		t.Fatalf("target_kind=comunitat_autonoma ha de retornar nomes comunitats autonomes equivalents: %+v", comunitatItems)
	}

	estatItems := f354U12SuggestNivells(t, app, session, "/api/territori/nivells/suggest?q=Pais+Suggest+F35R5&target_kind=estat")
	if !f354U12SuggestContainsNom(estatItems, levels.PaisLevelNom) {
		t.Fatalf("target_kind=estat ha de permetre pais/estat: %+v", estatItems)
	}

	genericItems := f354U12SuggestNivells(t, app, session, "/api/territori/nivells/suggest?q=Suggest+F35R5&target_kind=nivell_administratiu")
	if !f354U12SuggestContainsNom(genericItems, levels.DepartamentNom) || !f354U12SuggestContainsNom(genericItems, levels.ProvinciaNom) || !f354U12SuggestContainsNom(genericItems, levels.ComarcaNom) || !f354U12SuggestContainsNom(genericItems, levels.ComunitatNom) {
		t.Fatalf("target_kind=nivell_administratiu ha de permetre qualsevol nivell publicat: %+v", genericItems)
	}
}

func TestF354U12ArchiveScopeLegacyMunicipiLabelFallback(t *testing.T) {
	app, database, _, session := setupF354U12ArxiuAbastAdmin(t, "test_f35_4u12_legacy_label.sqlite3")
	arxiuID, arxiu := f354U12CreateArchiveWithLoadedMunicipi(t, database, "Arxiu F35-4U12 Legacy Label "+strconv.FormatInt(time.Now().UnixNano(), 10))
	if _, err := database.SaveArxiuAbast(f354U12MunicipiScope(arxiuID, arxiu.MunicipiID.Int64, "", "coverage", "publicat")); err != nil {
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
	u, err := url.Parse(path)
	if err != nil {
		t.Fatalf("url.Parse(%q): %v", path, err)
	}
	cleanPath := u.Path
	if strings.HasSuffix(cleanPath, "/edit") {
		return getArxiuAbastEditFormWithCSRF(t, app, session, extractTrailingID(cleanPath))
	}
	return getArxiuAbastNewFormWithCSRF(t, app, session, extractIDBeforeSegmentLocal(cleanPath, "abasts"))
}

func getArxiuAbastNewFormWithCSRF(t *testing.T, app *core.App, session *http.Cookie, arxiuID int) (string, *http.Cookie) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, f354U12ArxiuAbastNewPath(arxiuID), nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminNewArxiuAbastFromArxiu(rr, req)
	return f354U12CSRFResponse(t, rr, req.URL.Path)
}

func getArxiuAbastEditFormWithCSRF(t *testing.T, app *core.App, session *http.Cookie, relID int) (string, *http.Cookie) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, f354U12ArxiuAbastEditPath(relID), nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminEditArxiuAbast(rr, req)
	return f354U12CSRFResponse(t, rr, req.URL.Path)
}

func getArxiuEditPageWithCSRF(t *testing.T, app *core.App, session *http.Cookie, arxiuID int) (string, *http.Cookie) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/documentals/arxius/"+strconv.Itoa(arxiuID)+"/edit", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminEditArxiu(rr, req)
	return f354U12CSRFResponse(t, rr, req.URL.Path)
}

func getArxiuShowPageWithCSRF(t *testing.T, app *core.App, session *http.Cookie, arxiuID int) (string, *http.Cookie) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/documentals/arxius/"+strconv.Itoa(arxiuID), nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminShowArxiu(rr, req)
	return f354U12CSRFResponse(t, rr, req.URL.Path)
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

func f354U12ArxiuAbastNewPath(arxiuID int) string {
	return "/documentals/arxius/" + strconv.Itoa(arxiuID) + "/abasts/new"
}

func f354U12ArxiuAbastEditPath(relID int) string {
	return "/documentals/arxius/abasts/" + strconv.Itoa(relID) + "/edit"
}

type f354U12EditScopeFormInput struct {
	ScopeID      int
	ArxiuID      int
	TargetKind   string
	TargetID     string
	TargetLabel  string
	RelationKind string
	Notes        string
	ReturnTo     string
}

func f354U12ScopeFormValues(arxiuID int, targetKind, targetID, targetLabel, relationKind, notes, returnTo string) url.Values {
	values := url.Values{
		"arxiu_id":      {strconv.Itoa(arxiuID)},
		"target_kind":   {targetKind},
		"relation_kind": {relationKind},
		"notes":         {notes},
	}
	if strings.TrimSpace(targetID) != "" {
		values.Set("target_id", targetID)
	}
	if strings.TrimSpace(targetLabel) != "" {
		values.Set("target_label", targetLabel)
	}
	if strings.TrimSpace(returnTo) != "" {
		values.Set("return_to", returnTo)
	}
	return values
}

func f354U12MunicipiScopeValues(arxiuID int, arxiu *db.Arxiu, relationKind, notes string) url.Values {
	if arxiu == nil || !arxiu.MunicipiID.Valid {
		return f354U12ScopeFormValues(arxiuID, "municipi", "", "", relationKind, notes, "")
	}
	return f354U12ScopeFormValues(arxiuID, "municipi", strconv.Itoa(int(arxiu.MunicipiID.Int64)), "", relationKind, notes, "")
}

func f354U12EditScopeFormValues(input f354U12EditScopeFormInput) url.Values {
	values := f354U12ScopeFormValues(input.ArxiuID, input.TargetKind, input.TargetID, input.TargetLabel, input.RelationKind, input.Notes, input.ReturnTo)
	values.Set("id", strconv.Itoa(input.ScopeID))
	return values
}

func f354U12FreeTextScope(arxiuID int, label, relationKind, status string) *db.ArxiuAbast {
	return &db.ArxiuAbast{
		ArxiuID:        arxiuID,
		TargetKind:     "free_text",
		TargetLabel:    label,
		RelationKind:   relationKind,
		Estat:          "actiu",
		ModeracioEstat: status,
	}
}

func f354U12MunicipiScope(arxiuID int, municipiID int64, label, relationKind, status string) *db.ArxiuAbast {
	scope := &db.ArxiuAbast{
		ArxiuID:        arxiuID,
		TargetKind:     "municipi",
		TargetLabel:    label,
		RelationKind:   relationKind,
		Estat:          "actiu",
		ModeracioEstat: status,
	}
	if municipiID > 0 {
		scope.TargetID = sql.NullInt64{Int64: municipiID, Valid: true}
	}
	return scope
}

func f354U12SaveScope(t *testing.T, database db.DB, scope *db.ArxiuAbast) int {
	t.Helper()
	id, err := database.SaveArxiuAbast(scope)
	if err != nil {
		t.Fatalf("SaveArxiuAbast: %v", err)
	}
	return id
}

func f354U12CreateArchiveWithLoadedMunicipi(t *testing.T, database db.DB, name string) (int, *db.Arxiu) {
	t.Helper()
	arxiuID := f354CreateArxiu(t, database, name)
	arxiu, err := database.GetArxiu(arxiuID)
	if err != nil || arxiu == nil || !arxiu.MunicipiID.Valid {
		t.Fatalf("GetArxiu: err=%v arxiu=%v", err, arxiu)
	}
	return arxiuID, arxiu
}

func f354U12UpdateScopeNotes(t *testing.T, database db.DB, scopeID int, notes string) {
	t.Helper()
	scope, err := database.GetArxiuAbast(scopeID)
	if err != nil || scope == nil {
		t.Fatalf("GetArxiuAbast: err=%v scope=%v", err, scope)
	}
	scope.Notes = notes
	if _, err := database.SaveArxiuAbast(scope); err != nil {
		t.Fatalf("SaveArxiuAbast update notes: %v", err)
	}
}

func f354U12CSRFResponse(t *testing.T, rr *httptest.ResponseRecorder, path string) (string, *http.Cookie) {
	t.Helper()
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

func f354U12CreatePais(t *testing.T, database db.DB, iso2, iso3, num string) int {
	t.Helper()
	paisID, err := database.CreatePais(&db.Pais{
		CodiISO2:    iso2,
		CodiISO3:    iso3,
		CodiPaisNum: num,
	})
	if err != nil {
		t.Fatalf("CreatePais(%s): %v", iso2, err)
	}
	return paisID
}

type f354U12LevelInput struct {
	PaisID   int
	Nivel    int
	Nom      string
	Tipus    string
	Codi     string
	ParentID int
}

func f354U12CreatePublishedLevel(t *testing.T, database db.DB, input f354U12LevelInput) int {
	t.Helper()
	level := &db.NivellAdministratiu{
		PaisID:         input.PaisID,
		Nivel:          input.Nivel,
		NomNivell:      input.Nom,
		TipusNivell:    input.Tipus,
		CodiOficial:    input.Codi,
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	}
	if input.ParentID > 0 {
		level.ParentID = sql.NullInt64{Int64: int64(input.ParentID), Valid: true}
	}
	levelID, err := database.CreateNivell(level)
	if err != nil {
		t.Fatalf("CreateNivell(%s,%s): %v", input.Nom, input.Tipus, err)
	}
	return levelID
}

type f354U12SuggestFixtureInput struct {
	CountryISO2 string
	CountryISO3 string
	CountryNum  string
	Suffix      string
}

type f354U12SuggestFixture struct {
	PaisID         int
	PaisLevelID    int
	PaisLevelNom   string
	ComunitatID    int
	ComunitatNom   string
	ProvinciaID    int
	ProvinciaNom   string
	ComarcaID      int
	ComarcaNom     string
	DepartamentID  int
	DepartamentNom string
}

func f354U12CreateAdministrativeSuggestFixture(t *testing.T, database db.DB, input f354U12SuggestFixtureInput) f354U12SuggestFixture {
	t.Helper()
	fixture := f354U12SuggestFixture{
		PaisLevelNom:   "Pais Suggest F35R5 " + input.Suffix,
		ComunitatNom:   "Comunitat Suggest F35R5 " + input.Suffix,
		ProvinciaNom:   "Provincia Suggest F35R5 " + input.Suffix,
		ComarcaNom:     "Comarca Suggest F35R5 " + input.Suffix,
		DepartamentNom: "Departament Suggest F35R5 " + input.Suffix,
	}
	fixture.PaisID = f354U12CreatePais(t, database, input.CountryISO2, input.CountryISO3, input.CountryNum)
	fixture.PaisLevelID = f354U12CreatePublishedLevel(t, database, f354U12LevelInput{
		PaisID: fixture.PaisID,
		Nivel:  1,
		Nom:    fixture.PaisLevelNom,
		Tipus:  "pais",
	})
	fixture.ComunitatID = f354U12CreatePublishedLevel(t, database, f354U12LevelInput{
		PaisID:   fixture.PaisID,
		Nivel:    2,
		Nom:      fixture.ComunitatNom,
		Tipus:    "regio_autonoma",
		Codi:     input.CountryISO2 + "-RA",
		ParentID: fixture.PaisLevelID,
	})
	fixture.ProvinciaID = f354U12CreatePublishedLevel(t, database, f354U12LevelInput{
		PaisID:   fixture.PaisID,
		Nivel:    3,
		Nom:      fixture.ProvinciaNom,
		Tipus:    "provincia",
		Codi:     input.CountryISO2 + "-P",
		ParentID: fixture.ComunitatID,
	})
	fixture.ComarcaID = f354U12CreatePublishedLevel(t, database, f354U12LevelInput{
		PaisID:   fixture.PaisID,
		Nivel:    4,
		Nom:      fixture.ComarcaNom,
		Tipus:    "comarca",
		Codi:     input.CountryISO2 + "-C",
		ParentID: fixture.ProvinciaID,
	})
	fixture.DepartamentID = f354U12CreatePublishedLevel(t, database, f354U12LevelInput{
		PaisID:   fixture.PaisID,
		Nivel:    5,
		Nom:      fixture.DepartamentNom,
		Tipus:    "departament",
		Codi:     input.CountryISO2 + "-D",
		ParentID: fixture.ComarcaID,
	})
	return fixture
}

type f354U12SuggestItem struct {
	Nom     string `json:"nom"`
	Context string `json:"context"`
}

func f354U12SuggestNivells(t *testing.T, app *core.App, session *http.Cookie, path string) []f354U12SuggestItem {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminNivellsSuggest(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminNivellsSuggest(%s) status=%d body=%s", path, rr.Code, rr.Body.String())
	}
	var payload struct {
		Items []f354U12SuggestItem `json:"items"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal suggest payload: %v body=%s", err, rr.Body.String())
	}
	return payload.Items
}

func f354U12SuggestContainsNom(items []f354U12SuggestItem, want string) bool {
	for _, item := range items {
		if strings.TrimSpace(item.Nom) == strings.TrimSpace(want) {
			return true
		}
	}
	return false
}

func extractTrailingID(path string) int {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i := len(parts) - 1; i >= 0; i-- {
		if id, err := strconv.Atoi(parts[i]); err == nil {
			return id
		}
	}
	return 0
}

func extractIDBeforeSegmentLocal(path, segment string) int {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, part := range parts {
		if part != segment || i == 0 {
			continue
		}
		if id, err := strconv.Atoi(parts[i-1]); err == nil {
			return id
		}
	}
	return 0
}

func TestF354U12ExtractIDBeforeSegmentLocal(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		segment string
		want    int
	}{
		{name: "archive id before abasts", path: "/documentals/arxius/10/abasts/new", segment: "abasts", want: 10},
		{name: "non numeric before abasts", path: "/documentals/arxius/x/abasts/new", segment: "abasts", want: 0},
		{name: "skips non numeric match and uses later valid segment", path: "/documentals/abasts/10/abasts/new", segment: "abasts", want: 10},
		{name: "missing segment", path: "/documentals/arxius/10/edit", segment: "abasts", want: 0},
	}
	for _, tc := range tests {
		if got := extractIDBeforeSegmentLocal(tc.path, tc.segment); got != tc.want {
			t.Fatalf("%s: extractIDBeforeSegmentLocal(%q, %q)=%d want=%d", tc.name, tc.path, tc.segment, got, tc.want)
		}
	}
}
