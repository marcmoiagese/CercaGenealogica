package integration

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
	testcommon "github.com/marcmoiagese/CercaGenealogica/tests/common"
)

func TestImportTemplatesCRUD(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f20_templates_crud.sqlite3")
	_, sessionID := createF7UserWithSession(t, database)

	csrf := "csrf-f20-crud"
	modelJSON := `{"version":1,"kind":"transcripcions_raw","mapping":{"columns":[]}}`

	payload := map[string]interface{}{
		"name":              "Plantilla CRUD",
		"description":       "Test CRUD",
		"visibility":        "private",
		"default_separator": ";",
		"model_json":        modelJSON,
		"csrf_token":        csrf,
	}
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/api/import-templates", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrf})
	rr := httptest.NewRecorder()
	app.RequireLogin(app.ImportTemplatesAPI)(rr, req)

	if rr.Result().StatusCode != http.StatusOK {
		t.Fatalf("create status inesperat: %d", rr.Result().StatusCode)
	}
	var createResp struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&createResp); err != nil || createResp.ID == 0 {
		t.Fatalf("no he rebut id de plantilla: %v", err)
	}

	payload["name"] = "Plantilla CRUD Editada"
	payload["visibility"] = "public"
	body, _ = json.Marshal(payload)
	req = httptest.NewRequest(http.MethodPut, fmt.Sprintf("/api/import-templates/%d", createResp.ID), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrf})
	rr = httptest.NewRecorder()
	app.RequireLogin(app.ImportTemplatesAPI)(rr, req)
	if rr.Result().StatusCode != http.StatusOK {
		t.Fatalf("update status inesperat: %d", rr.Result().StatusCode)
	}

	req = httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/import-templates/%d/clone", createResp.ID), nil)
	req.Header.Set("X-CSRF-Token", csrf)
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrf})
	rr = httptest.NewRecorder()
	app.RequireLogin(app.ImportTemplatesAPI)(rr, req)
	if rr.Result().StatusCode != http.StatusOK {
		t.Fatalf("clone status inesperat: %d", rr.Result().StatusCode)
	}
	var cloneResp struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&cloneResp); err != nil || cloneResp.ID == 0 {
		t.Fatalf("no he rebut id clone: %v", err)
	}

	req = httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/import-templates/%d", createResp.ID), nil)
	req.Header.Set("X-CSRF-Token", csrf)
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrf})
	rr = httptest.NewRecorder()
	app.RequireLogin(app.ImportTemplatesAPI)(rr, req)
	if rr.Result().StatusCode != http.StatusOK {
		t.Fatalf("delete status inesperat: %d", rr.Result().StatusCode)
	}
}

func TestTemplateImportCreatesRows(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f20_template_import.sqlite3")
	user, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)
	llibreID, _ := createF7LlibreWithPagina(t, database, user.ID)

	modelJSON := `{
  "version": 1,
  "kind": "transcripcions_raw",
  "book_resolution": { "mode": "llibre_id", "column": "llibre_id" },
  "mapping": {
    "columns": [
      { "header": "llibre_id", "key": "llibre_id", "required": true, "map_to": [{ "target": "base.llibre_id" }] },
      { "header": "tipus_acte", "key": "tipus_acte", "required": true, "map_to": [{ "target": "base.tipus_acte" }] },
      { "header": "batejat", "key": "batejat", "map_to": [{ "target": "person.batejat", "transform": [{ "op": "parse_person_from_nom" }] }] },
      { "header": "ofici", "key": "ofici", "map_to": [{ "target": "attr.ofici.text" }] }
    ]
  }
}`
	templateID, err := database.CreateCSVImportTemplate(&db.CSVImportTemplate{
		Name:             "Template Simple",
		OwnerUserID:      sqlNullFromInt(user.ID),
		Visibility:       "private",
		DefaultSeparator: ",",
		ModelJSON:        modelJSON,
	})
	if err != nil || templateID == 0 {
		t.Fatalf("CreateCSVImportTemplate ha fallat: %v", err)
	}

	csvContent := strings.Join([]string{
		"llibre_id,tipus_acte,batejat,ofici",
		fmt.Sprintf("%d,baptisme,Joan Garcia,forner", llibreID),
		fmt.Sprintf("%d,baptisme,Maria Puig,teixidora", llibreID),
		fmt.Sprintf("%d,baptisme,Pere Soler,pages", llibreID),
	}, "\n")

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	filePart, err := writer.CreateFormFile("csv_file", "import.csv")
	if err != nil {
		t.Fatalf("CreateFormFile ha fallat: %v", err)
	}
	if _, err := filePart.Write([]byte(csvContent)); err != nil {
		t.Fatalf("escriptura CSV ha fallat: %v", err)
	}
	_ = writer.WriteField("model", "template")
	_ = writer.WriteField("template_id", strconv.Itoa(templateID))
	_ = writer.WriteField("separator", ",")
	csrfToken := "csrf-f20-template-import"
	_ = writer.WriteField("csrf_token", csrfToken)
	if err := writer.Close(); err != nil {
		t.Fatalf("Close multipart ha fallat: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/documentals/llibres/importar", &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrfToken})
	rr := httptest.NewRecorder()

	app.AdminImportRegistresGlobal(rr, req)
	if rr.Result().StatusCode != http.StatusSeeOther {
		t.Fatalf("status inesperat: %d", rr.Result().StatusCode)
	}

	registres, err := database.ListTranscripcionsRaw(llibreID, db.TranscripcioFilter{Limit: -1})
	if err != nil {
		t.Fatalf("ListTranscripcionsRaw ha fallat: %v", err)
	}
	if len(registres) != 3 {
		t.Fatalf("esperava 3 registres, però n'he trobat %d", len(registres))
	}
}

func TestTemplateImportHistoryShowsInitialVersionWithoutManualChangesF324(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f32_4_template_history.sqlite3")
	user, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)
	llibreID, _ := createF7LlibreWithPagina(t, database, user.ID)

	modelJSON := `{
  "version": 1,
  "kind": "transcripcions_raw",
  "book_resolution": { "mode": "llibre_id", "column": "llibre_id" },
  "mapping": {
    "columns": [
      { "header": "llibre_id", "key": "llibre_id", "required": true, "map_to": [{ "target": "base.llibre_id" }] },
      { "header": "tipus_acte", "key": "tipus_acte", "required": true, "map_to": [{ "target": "base.tipus_acte" }] },
      { "header": "batejat", "key": "batejat", "map_to": [{ "target": "person.batejat", "transform": [{ "op": "parse_person_from_nom" }] }] }
    ]
  }
}`
	templateID, err := database.CreateCSVImportTemplate(&db.CSVImportTemplate{
		Name:             "Template Historial Inicial",
		OwnerUserID:      sqlNullFromInt(user.ID),
		Visibility:       "private",
		DefaultSeparator: ",",
		ModelJSON:        modelJSON,
	})
	if err != nil || templateID == 0 {
		t.Fatalf("CreateCSVImportTemplate ha fallat: %v", err)
	}

	req := buildImportGlobalRequest(t, sessionID, "csrf-f32-4-history", map[string]string{
		"model":       "template",
		"template_id": strconv.Itoa(templateID),
		"separator":   ",",
	}, strings.Join([]string{
		"llibre_id,tipus_acte,batejat",
		fmt.Sprintf("%d,baptisme,Joan Garcia", llibreID),
	}, "\n"))
	rr := httptest.NewRecorder()
	app.AdminImportRegistresGlobal(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("import via plantilla esperava 303, got %d", rr.Code)
	}

	registres, err := database.ListTranscripcionsRaw(llibreID, db.TranscripcioFilter{Limit: -1})
	if err != nil {
		t.Fatalf("ListTranscripcionsRaw ha fallat: %v", err)
	}
	if len(registres) != 1 {
		t.Fatalf("esperava 1 registre importat, got %d", len(registres))
	}

	historyReq := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/documentals/registres/%d/historial", registres[0].ID), nil)
	historyReq.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	historyRR := httptest.NewRecorder()
	app.AdminRegistreHistory(historyRR, historyReq)
	if historyRR.Code != http.StatusOK {
		t.Fatalf("historial esperava 200, got %d body=%s", historyRR.Code, historyRR.Body.String())
	}
	body := historyRR.Body.String()
	if !strings.Contains(body, fmt.Sprintf("/documentals/registres/%d?view=base", registres[0].ID)) {
		t.Fatalf("esperava enllaç a la versió base, body=%s", body)
	}
	if !strings.Contains(body, "#1") {
		t.Fatalf("esperava versió inicial #1 visible a l'historial, body=%s", body)
	}
}

func TestTemplateImportHistorySingleInitialRevisionAcrossStatesF324Fix1(t *testing.T) {
	for _, cfg := range loadSQLiteAndPostgresConfigsForImportHistory(t) {
		cfg := cfg
		t.Run(cfg.Label, func(t *testing.T) {
			app, database := newTestAppForConfig(t, cfg.Config)
			user, sessionID := createF7UserWithSession(t, database)
			ensureAdminPolicyForUser(t, database, user.ID)
			llibreID, paginaID := createF7LlibreWithPagina(t, database, user.ID)

			publicTemplateID := createF324HistoryTemplate(t, database, user.ID, "publicat")
			publicRegistreID := importSingleHistoryTemplateRow(t, app, database, sessionID, publicTemplateID, llibreID, paginaID, "Joan Garcia")
			publicBody := fetchRegistreHistoryBody(t, app, sessionID, publicRegistreID)
			if got := countHistoryTimelineItems(publicBody); got != 1 {
				t.Fatalf("[%s] import publicat hauria de mostrar 1 revisió inicial, got=%d body=%s", cfg.Label, got, publicBody)
			}

			pendingTemplateID := createF324HistoryTemplate(t, database, user.ID, "pendent")
			pendingRegistreID := importSingleHistoryTemplateRow(t, app, database, sessionID, pendingTemplateID, llibreID, paginaID, "Maria Puig")
			pendingBody := fetchRegistreHistoryBody(t, app, sessionID, pendingRegistreID)
			if got := countHistoryTimelineItems(pendingBody); got != 1 {
				t.Fatalf("[%s] import pendent hauria de mostrar 1 revisió inicial, got=%d body=%s", cfg.Label, got, pendingBody)
			}

			approveForm := url.Values{
				"csrf_token":  {"csrf-f32-4-fix-1-approve"},
				"object_type": {"registre"},
				"return_to":   {fmt.Sprintf("/documentals/registres/%d/historial", pendingRegistreID)},
			}
			approveReq := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/moderacio/%d/aprovar", pendingRegistreID), strings.NewReader(approveForm.Encode()))
			approveReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			approveReq.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
			approveReq.AddCookie(&http.Cookie{Name: "cg_csrf", Value: "csrf-f32-4-fix-1-approve"})
			approveRR := httptest.NewRecorder()
			app.AdminModeracioAprovar(approveRR, approveReq)
			if approveRR.Code != http.StatusSeeOther {
				t.Fatalf("[%s] aprovar registre esperava 303, got %d body=%s", cfg.Label, approveRR.Code, approveRR.Body.String())
			}

			registre, err := database.GetTranscripcioRaw(pendingRegistreID)
			if err != nil || registre == nil {
				t.Fatalf("[%s] GetTranscripcioRaw ha fallat: %v", cfg.Label, err)
			}
			if registre.ModeracioEstat != "publicat" {
				t.Fatalf("[%s] esperava registre publicat després de moderar, got=%q", cfg.Label, registre.ModeracioEstat)
			}

			approvedBody := fetchRegistreHistoryBody(t, app, sessionID, pendingRegistreID)
			if got := countHistoryTimelineItems(approvedBody); got != 1 {
				t.Fatalf("[%s] publicar sense canvi real no ha de crear una revisió extra, got=%d body=%s", cfg.Label, got, approvedBody)
			}
		})
	}
}

func TestTemplateImportHistoryAddsRevisionOnlyAfterManualChangeF324Fix1(t *testing.T) {
	for _, cfg := range loadSQLiteAndPostgresConfigsForImportHistory(t) {
		cfg := cfg
		t.Run(cfg.Label, func(t *testing.T) {
			app, database := newTestAppForConfig(t, cfg.Config)
			user, sessionID := createF7UserWithSession(t, database)
			ensureAdminPolicyForUser(t, database, user.ID)
			llibreID, paginaID := createF7LlibreWithPagina(t, database, user.ID)

			templateID := createF324HistoryTemplate(t, database, user.ID, "pendent")
			registreID := importSingleHistoryTemplateRow(t, app, database, sessionID, templateID, llibreID, paginaID, "Pere Soler")
			initialBody := fetchRegistreHistoryBody(t, app, sessionID, registreID)
			if got := countHistoryTimelineItems(initialBody); got != 1 {
				t.Fatalf("[%s] import inicial hauria de mostrar 1 revisió, got=%d body=%s", cfg.Label, got, initialBody)
			}

			updateForm := url.Values{
				"csrf_token":           {"csrf-f32-4-fix-1-update"},
				"return_to":            {fmt.Sprintf("/documentals/registres/%d", registreID)},
				"pagina_id":            {strconv.Itoa(paginaID)},
				"tipus_acte":           {"baptisme"},
				"data_acte_estat":      {"clar"},
				"transcripcio_literal": {"canvi manual de prova"},
			}
			updateReq := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/documentals/registres/%d/editar", registreID), strings.NewReader(updateForm.Encode()))
			updateReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			updateReq.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
			updateReq.AddCookie(&http.Cookie{Name: "cg_csrf", Value: "csrf-f32-4-fix-1-update"})
			updateRR := httptest.NewRecorder()
			app.AdminUpdateRegistre(updateRR, updateReq)
			if updateRR.Code != http.StatusSeeOther {
				t.Fatalf("[%s] canvi manual esperava 303, got %d body=%s", cfg.Label, updateRR.Code, updateRR.Body.String())
			}

			updatedBody := fetchRegistreHistoryBody(t, app, sessionID, registreID)
			if got := countHistoryTimelineItems(updatedBody); got != 2 {
				t.Fatalf("[%s] un canvi manual real ha de crear una segona revisió, got=%d body=%s", cfg.Label, got, updatedBody)
			}
			if !strings.Contains(updatedBody, "#2") {
				t.Fatalf("[%s] esperava veure la segona revisió després del canvi manual, body=%s", cfg.Label, updatedBody)
			}
		})
	}
}

func TestTemplateImportConditions(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f20_template_conditions.sqlite3")
	user, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)
	llibreID, _ := createF7LlibreWithPagina(t, database, user.ID)

	modelJSON := `{
  "version": 1,
  "kind": "transcripcions_raw",
  "book_resolution": { "mode": "llibre_id", "column": "llibre_id" },
  "mapping": {
    "columns": [
      { "header": "llibre_id", "key": "llibre_id", "required": true, "map_to": [{ "target": "base.llibre_id" }] },
      { "header": "tipus_acte", "key": "tipus_acte", "required": true, "map_to": [{ "target": "base.tipus_acte" }] },
      { "header": "estat", "key": "estat", "required": true,
        "condition": {
          "expr": "value == 'A'",
          "then": { "map_to": [{ "target": "attr.estat.text" }] },
          "else": { "map_to": [{ "target": "attr.estat_alt.text" }] }
        }
      }
    ]
  }
}`
	templateID, err := database.CreateCSVImportTemplate(&db.CSVImportTemplate{
		Name:        "Template Condicions",
		OwnerUserID: sqlNullFromInt(user.ID),
		Visibility:  "private",
		ModelJSON:   modelJSON,
	})
	if err != nil || templateID == 0 {
		t.Fatalf("CreateCSVImportTemplate ha fallat: %v", err)
	}
	csvContent := strings.Join([]string{
		"llibre_id,tipus_acte,estat",
		fmt.Sprintf("%d,baptisme,A", llibreID),
		fmt.Sprintf("%d,baptisme,B", llibreID),
	}, "\n")
	csrfToken := "csrf-f20-conditions"
	req := buildImportGlobalRequest(t, sessionID, csrfToken, map[string]string{
		"model":       "template",
		"template_id": strconv.Itoa(templateID),
		"separator":   ",",
	}, csvContent)
	rr := httptest.NewRecorder()
	app.AdminImportRegistresGlobal(rr, req)
	if rr.Result().StatusCode != http.StatusSeeOther {
		t.Fatalf("status inesperat: %d", rr.Result().StatusCode)
	}
	registres, _ := database.ListTranscripcionsRaw(llibreID, db.TranscripcioFilter{Limit: -1})
	if len(registres) != 2 {
		t.Fatalf("esperava 2 registres, però n'he trobat %d", len(registres))
	}
	foundEstat := false
	foundAlt := false
	for _, reg := range registres {
		attrs, _ := database.ListTranscripcioAtributs(reg.ID)
		if hasAttrKey(attrs, "estat") {
			foundEstat = true
		}
		if hasAttrKey(attrs, "estat_alt") {
			foundAlt = true
		}
	}
	if !foundEstat || !foundAlt {
		t.Fatalf("esperava attrs 'estat' i 'estat_alt', però foundEstat=%v foundAlt=%v", foundEstat, foundAlt)
	}
}

func TestTemplateImportDedupWithinFile(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f20_template_dedup.sqlite3")
	user, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)
	llibreID, _ := createF7LlibreWithPagina(t, database, user.ID)

	modelJSON := `{
  "version": 1,
  "kind": "transcripcions_raw",
  "book_resolution": { "mode": "llibre_id", "column": "llibre_id" },
  "mapping": {
    "columns": [
      { "header": "llibre_id", "key": "llibre_id", "required": true, "map_to": [{ "target": "base.llibre_id" }] },
      { "header": "tipus_acte", "key": "tipus_acte", "required": true, "map_to": [{ "target": "base.tipus_acte" }] }
    ]
  },
  "policies": {
    "dedup": { "within_file": true, "key_columns": ["llibre_id","tipus_acte"] }
  }
}`
	templateID, err := database.CreateCSVImportTemplate(&db.CSVImportTemplate{
		Name:        "Template Dedup",
		OwnerUserID: sqlNullFromInt(user.ID),
		Visibility:  "private",
		ModelJSON:   modelJSON,
	})
	if err != nil || templateID == 0 {
		t.Fatalf("CreateCSVImportTemplate ha fallat: %v", err)
	}
	csvContent := strings.Join([]string{
		"llibre_id,tipus_acte",
		fmt.Sprintf("%d,baptisme", llibreID),
		fmt.Sprintf("%d,baptisme", llibreID),
	}, "\n")
	csrfToken := "csrf-f20-dedup"
	req := buildImportGlobalRequest(t, sessionID, csrfToken, map[string]string{
		"model":       "template",
		"template_id": strconv.Itoa(templateID),
		"separator":   ",",
	}, csvContent)
	rr := httptest.NewRecorder()
	app.AdminImportRegistresGlobal(rr, req)
	if rr.Result().StatusCode != http.StatusSeeOther {
		t.Fatalf("status inesperat: %d", rr.Result().StatusCode)
	}
	imported, failed := parseRedirectCounts(t, rr)
	if imported != 1 || failed != 1 {
		t.Fatalf("esperava imported=1 failed=1, però he rebut imported=%d failed=%d", imported, failed)
	}
}

func TestTemplateImportMergeExisting(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f20_template_merge.sqlite3")
	user, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)
	llibreID, _ := createF7LlibreWithPagina(t, database, user.ID)

	llibre, _ := database.GetLlibre(llibreID)
	llibre.IndexacioCompleta = true
	_ = database.UpdateLlibre(llibre)

	existingID, err := database.CreateTranscripcioRaw(&db.TranscripcioRaw{
		LlibreID:       llibreID,
		TipusActe:      "baptisme",
		ModeracioEstat: "pendent",
	})
	if err != nil {
		t.Fatalf("CreateTranscripcioRaw ha fallat: %v", err)
	}
	_, _ = database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
		TranscripcioID: existingID,
		Rol:            "batejat",
		Nom:            "Joan",
		Cognom1:        "Garcia",
	})

	modelJSON := `{
  "version": 1,
  "kind": "transcripcions_raw",
  "book_resolution": { "mode": "llibre_id", "column": "llibre_id" },
  "mapping": {
    "columns": [
      { "header": "llibre_id", "key": "llibre_id", "required": true, "map_to": [{ "target": "base.llibre_id" }] },
      { "header": "tipus_acte", "key": "tipus_acte", "required": true, "map_to": [{ "target": "base.tipus_acte" }] },
      { "header": "notes", "key": "notes", "map_to": [{ "target": "base.notes_marginals" }] },
      { "header": "batejat", "key": "batejat", "map_to": [{ "target": "person.batejat", "transform": [{ "op": "parse_person_from_nom" }] }] }
    ]
  },
  "policies": {
    "merge_existing": {
      "mode": "by_principal_person_if_book_indexed",
      "principal_roles": ["batejat"],
      "update_missing_only": true,
      "add_missing_people": true,
      "add_missing_attrs": true
    }
  }
}`
	templateID, err := database.CreateCSVImportTemplate(&db.CSVImportTemplate{
		Name:        "Template Merge",
		OwnerUserID: sqlNullFromInt(user.ID),
		Visibility:  "private",
		ModelJSON:   modelJSON,
	})
	if err != nil || templateID == 0 {
		t.Fatalf("CreateCSVImportTemplate ha fallat: %v", err)
	}
	csvContent := strings.Join([]string{
		"llibre_id,tipus_acte,notes,batejat",
		fmt.Sprintf("%d,baptisme,Nota nova,Joan Garcia", llibreID),
	}, "\n")
	csrfToken := "csrf-f20-merge"
	req := buildImportGlobalRequest(t, sessionID, csrfToken, map[string]string{
		"model":       "template",
		"template_id": strconv.Itoa(templateID),
		"separator":   ",",
	}, csvContent)
	rr := httptest.NewRecorder()
	app.AdminImportRegistresGlobal(rr, req)
	if rr.Result().StatusCode != http.StatusSeeOther {
		t.Fatalf("status inesperat: %d", rr.Result().StatusCode)
	}
	updated, _ := database.GetTranscripcioRaw(existingID)
	if updated.NotesMarginals != "Nota nova" {
		t.Fatalf("esperava NotesMarginals actualitzades, però tinc %q", updated.NotesMarginals)
	}
}

func TestImportersRegression(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f20_regression.sqlite3")
	user, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)
	llibreID, _ := createF7LlibreWithPagina(t, database, user.ID)

	csvGeneric := strings.Join([]string{
		"llibre_id,tipus_acte",
		fmt.Sprintf("%d,baptisme", llibreID),
	}, "\n")
	csrfToken := "csrf-f20-generic"
	req := buildImportGlobalRequest(t, sessionID, csrfToken, map[string]string{
		"model":     "generic",
		"separator": ",",
	}, csvGeneric)
	rr := httptest.NewRecorder()
	app.AdminImportRegistresGlobal(rr, req)
	if rr.Result().StatusCode != http.StatusSeeOther {
		t.Fatalf("status generic inesperat: %d", rr.Result().StatusCode)
	}
	imported, _ := parseRedirectCounts(t, rr)
	if imported != 1 {
		t.Fatalf("import generic fallat: imported=%d", imported)
	}

	llibre, _ := database.GetLlibre(llibreID)
	llibre.Cronologia = "1890-1891"
	_ = database.UpdateLlibre(llibre)

	csvMarcmoia := strings.Join([]string{
		"llibre,cognoms",
		"1890-1891,Garcia Joan",
	}, "\n")
	csrfToken = "csrf-f20-marcmoia"
	req = buildImportGlobalRequest(t, sessionID, csrfToken, map[string]string{
		"model":     "baptismes_marcmoia",
		"separator": ",",
	}, csvMarcmoia)
	rr = httptest.NewRecorder()
	app.AdminImportRegistresGlobal(rr, req)
	if rr.Result().StatusCode != http.StatusSeeOther {
		t.Fatalf("status marcmoia inesperat: %d", rr.Result().StatusCode)
	}
	imported, _ = parseRedirectCounts(t, rr)
	if imported < 1 {
		t.Fatalf("import marcmoia fallat: imported=%d", imported)
	}
}

func hasAttrKey(list []db.TranscripcioAtributRaw, key string) bool {
	for _, a := range list {
		if a.Clau == key {
			return true
		}
	}
	return false
}

func sqlNullFromInt(val int) sql.NullInt64 {
	if val == 0 {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(val), Valid: true}
}

func buildImportGlobalRequest(t *testing.T, sessionID string, csrf string, fields map[string]string, csvContent string) *http.Request {
	t.Helper()

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	filePart, err := writer.CreateFormFile("csv_file", "import.csv")
	if err != nil {
		t.Fatalf("CreateFormFile ha fallat: %v", err)
	}
	if _, err := filePart.Write([]byte(csvContent)); err != nil {
		t.Fatalf("escriptura CSV ha fallat: %v", err)
	}
	for key, val := range fields {
		if err := writer.WriteField(key, val); err != nil {
			t.Fatalf("WriteField %s ha fallat: %v", key, err)
		}
	}
	if csrf != "" {
		if err := writer.WriteField("csrf_token", csrf); err != nil {
			t.Fatalf("WriteField csrf_token ha fallat: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close multipart ha fallat: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/documentals/llibres/importar", &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if sessionID != "" {
		req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	}
	if csrf != "" {
		req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrf})
	}
	return req
}

func parseRedirectCounts(t *testing.T, rr *httptest.ResponseRecorder) (int, int) {
	t.Helper()
	loc := rr.Result().Header.Get("Location")
	if loc == "" {
		return 0, 0
	}
	parsed, err := url.Parse(loc)
	if err != nil {
		t.Fatalf("no puc parsejar Location: %v", err)
	}
	q := parsed.Query()
	imported, _ := strconv.Atoi(q.Get("imported"))
	failed, _ := strconv.Atoi(q.Get("failed"))
	return imported, failed
}

func loadSQLiteAndPostgresConfigsForImportHistory(t *testing.T) []testcommon.TestDBConfig {
	t.Helper()

	raw := testcommon.LoadTestDBConfigs(t)
	var out []testcommon.TestDBConfig
	for _, cfg := range raw {
		if cfg.Engine == "mysql" {
			continue
		}
		cfgCopy := map[string]string{}
		for k, v := range cfg.Config {
			cfgCopy[k] = v
		}
		cfgCopy["RECREADB"] = "true"
		if _, ok := cfgCopy["LOG_LEVEL"]; !ok {
			cfgCopy["LOG_LEVEL"] = "silent"
		}
		if cfg.Engine == "sqlite" {
			cfgCopy["DB_PATH"] = filepath.Join(t.TempDir(), "f32_4_fix_1_"+cfg.Label+".sqlite3")
		}
		out = append(out, testcommon.TestDBConfig{
			Engine: cfg.Engine,
			Label:  cfg.Label,
			Config: cfgCopy,
		})
	}
	if len(out) == 0 {
		t.Fatalf("no hi ha configuracions SQLite/PostgreSQL disponibles per F32-4-fix-1")
	}
	return out
}

func createF324HistoryTemplate(t *testing.T, database db.DB, userID int, moderationStatus string) int {
	t.Helper()

	modelJSON := fmt.Sprintf(`{
  "version": 1,
  "kind": "transcripcions_raw",
  "base_defaults": {
    "moderation_status": %q
  },
  "book_resolution": { "mode": "llibre_id", "column": "llibre_id" },
  "mapping": {
    "columns": [
      { "header": "llibre_id", "key": "llibre_id", "required": true, "map_to": [{ "target": "base.llibre_id" }] },
      { "header": "pagina_id", "key": "pagina_id", "required": true, "map_to": [{ "target": "base.pagina_id" }] },
      { "header": "tipus_acte", "key": "tipus_acte", "required": true, "map_to": [{ "target": "base.tipus_acte" }] },
      { "header": "batejat", "key": "batejat", "map_to": [{ "target": "person.batejat", "transform": [{ "op": "parse_person_from_nom" }] }] }
    ]
  }
}`, moderationStatus)

	templateID, err := database.CreateCSVImportTemplate(&db.CSVImportTemplate{
		Name:             "Template Historial F32-4-fix-1 " + moderationStatus,
		OwnerUserID:      sqlNullFromInt(userID),
		Visibility:       "private",
		DefaultSeparator: ",",
		ModelJSON:        modelJSON,
	})
	if err != nil || templateID == 0 {
		t.Fatalf("CreateCSVImportTemplate ha fallat: %v", err)
	}
	return templateID
}

func importSingleHistoryTemplateRow(t *testing.T, app interface {
	AdminImportRegistresGlobal(http.ResponseWriter, *http.Request)
}, database db.DB, sessionID string, templateID, llibreID, paginaID int, principal string) int {
	t.Helper()

	req := buildImportGlobalRequest(t, sessionID, "csrf-f32-4-fix-1-import", map[string]string{
		"model":       "template",
		"template_id": strconv.Itoa(templateID),
		"separator":   ",",
	}, strings.Join([]string{
		"llibre_id,pagina_id,tipus_acte,batejat",
		fmt.Sprintf("%d,%d,baptisme,%s", llibreID, paginaID, principal),
	}, "\n"))
	rr := httptest.NewRecorder()
	app.AdminImportRegistresGlobal(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("import via plantilla esperava 303, got %d body=%s", rr.Code, rr.Body.String())
	}

	registres, err := database.ListTranscripcionsRaw(llibreID, db.TranscripcioFilter{Limit: -1})
	if err != nil {
		t.Fatalf("ListTranscripcionsRaw ha fallat: %v", err)
	}
	if len(registres) == 0 {
		t.Fatalf("esperava almenys 1 registre importat")
	}
	return registres[len(registres)-1].ID
}

func fetchRegistreHistoryBody(t *testing.T, app interface {
	AdminRegistreHistory(http.ResponseWriter, *http.Request)
}, sessionID string, registreID int) string {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/documentals/registres/%d/historial", registreID), nil)
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	rr := httptest.NewRecorder()
	app.AdminRegistreHistory(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("historial esperava 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	return rr.Body.String()
}

func countHistoryTimelineItems(body string) int {
	return strings.Count(body, `class="timeline-item"`)
}
