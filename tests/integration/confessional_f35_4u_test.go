package integration

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"html"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF354UImportExportUITabPermissionsAndI18N(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u_ui.sqlite3")

	user := createTestUser(t, database, "f35_4u_ui_"+time.Now().Format("150405000000000"))
	session := createSessionCookie(t, database, user.ID, "sess_f35_4u_ui_"+time.Now().Format("150405000000000"))
	policyID := createPolicyWithGrant(t, database, "f35_4u_ui", "territori.confessional.import_export.view")
	addGrantToPolicy(t, database, policyID, "territori.confessional.import_export.import")
	addGrantToPolicy(t, database, policyID, "territori.confessional.import_export.export")
	assignPolicyToUser(t, database, user.ID, policyID)

	req := httptest.NewRequest(http.MethodGet, "/admin/import-export?tab=confessional", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminImportExport(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminImportExport confessional esperava 200, got %d", rr.Code)
	}
	body := rr.Body.String()
	for _, token := range []string{
		`data-tab="confessional"`,
		`data-tab="confessional-import"`,
		`data-tab="confessional-export"`,
		`/admin/confessional/import/dry-run`,
		`/admin/confessional/export`,
		`confessional-file`,
	} {
		if !strings.Contains(body, token) {
			t.Fatalf("la UI import/export confessional ha de contenir %q; body=%s", token, body)
		}
	}
	if strings.Contains(body, `name="payload_b64"`) {
		t.Fatalf("la UI GET no ha de renderitzar el formulari d'apply sense dry-run previ; body=%s", body)
	}

	req = httptest.NewRequest(http.MethodGet, "/admin/confessional/export", nil)
	noExportUser := createTestUser(t, database, "f35_4u_no_export_"+time.Now().Format("150405000000000"))
	noExportSession := createSessionCookie(t, database, noExportUser.ID, "sess_f35_4u_no_export_"+time.Now().Format("150405000000000"))
	noExportPolicy := createPolicyWithGrant(t, database, "f35_4u_no_export", "territori.confessional.import_export.view")
	assignPolicyToUser(t, database, noExportUser.ID, noExportPolicy)
	req.AddCookie(noExportSession)
	rr = httptest.NewRecorder()
	app.AdminConfessionalExport(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("AdminConfessionalExport sense permis hauria de bloquejar amb 403, got %d", rr.Code)
	}

	root := findProjectRoot(t)
	templateBody := readProjectFileF354S(t, root, "templates/admin-import-export.html")
	if strings.Contains(templateBody, "confessional-tabs") || strings.Contains(templateBody, "onclick=") {
		t.Fatalf("la plantilla import/export no ha de reintroduir confessional-tabs ni JS inline")
	}

	for _, localeFile := range []string{"locales/cat.json", "locales/en.json", "locales/oc.json"} {
		raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(localeFile)))
		if err != nil {
			t.Fatalf("no s'ha pogut llegir %s: %v", localeFile, err)
		}
		var payload map[string]string
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatalf("json invalid a %s: %v", localeFile, err)
		}
		for _, key := range []string{
			"admin.io.tab.confessional",
			"confessional.io.title",
			"confessional.io.import.description",
			"confessional.io.import.dry_run",
			"confessional.io.import.apply",
			"confessional.io.export.all_religions",
			"confessional.io.export.all_levels",
			"confessional.io.export.options_title",
			"confessional.io.export.clear_selection",
			"confessional.io.error.invalid_json",
			"confessional.io.error.unresolved_reference",
			"confessional.io.error.invalid_filter",
		} {
			if strings.TrimSpace(payload[key]) == "" {
				t.Fatalf("%s ha de definir %s", localeFile, key)
			}
		}
		if strings.HasSuffix(localeFile, "oc.json") {
			for key, value := range payload {
				if strings.HasPrefix(key, "confessional.io.") && regexp.MustCompile(`\pL\?\pL`).MatchString(value) {
					t.Fatalf("occita F354U no pot tenir ? dins paraules: %s=%q", key, value)
				}
			}
		}
	}
}

func TestF354U2ConfessionalExportUIUsesControlledSuggestsAndStyledOptions(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u2_ui.sqlite3")

	user := createTestUser(t, database, "f35_4u2_ui_"+time.Now().Format("150405000000000"))
	session := createSessionCookie(t, database, user.ID, "sess_f35_4u2_ui_"+time.Now().Format("150405000000000"))
	policyID := createPolicyWithGrant(t, database, "f35_4u2_ui", "territori.confessional.import_export.export")
	addGrantToPolicy(t, database, policyID, "territori.confessional.import_export.view")
	assignPolicyToUser(t, database, user.ID, policyID)
	f354SSaveEntitat(t, database, "f35_4u2_bisbat_"+time.Now().Format("150405000000000"), "Bisbat F35-4U2", "catolicisme_ritu_llati", "bisbat_diocesi", "publicat")
	f354SSaveEntitat(t, database, "f35_4u2_parroquia_"+time.Now().Format("150405000000000"), "Parroquia F35-4U2", "catolicisme_ritu_llati", "parroquia", "publicat")

	req := httptest.NewRequest(http.MethodGet, "/admin/import-export?tab=confessional&subtab=confessional-export", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminImportExport(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminImportExport F35-4U2 status=%d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	for _, token := range []string{
		`data-confessional-export-form`,
		`data-confessional-export-tab="confessional-export"`,
		`data-confessional-export-panel="confessional-export"`,
		`class="form-vertical confessional-export-form"`,
		`<link rel="stylesheet" href="/static/css/admin-import-export.css?v=1">`,
		`<script src="/static/js/admin-import-export.js?v=3"></script>`,
		`class="confessional-export-options"`,
		`class="confessional-export-option" for="conf-export-include-hierarchy"`,
		`id="conf-export-religion-search"`,
		`role="combobox"`,
		`aria-controls="conf-export-religion-suggestions"`,
		`data-local-suggest="1"`,
		`data-hidden="conf-export-religion"`,
		`type="hidden" name="religio_confessio_codi"`,
		`data-code="catolicisme_ritu_llati"`,
		`id="conf-export-level-search"`,
		`data-hidden="conf-export-level"`,
		`type="hidden" name="nivell_confessional_codi"`,
		`data-religion-filter-hidden="conf-export-religion"`,
		`data-code="parroquia"`,
		`data-religion-code="catolicisme_ritu_llati"`,
		`role="listbox"`,
		`data-confessional-import-form`,
		`class="form-vertical confessional-import-form"`,
		`class="confessional-import-layout"`,
		`class="confessional-import-panel"`,
		`class="confessional-export-option confessional-import-option" for="conf-import-include-non-published"`,
		`Totes les religions/confessions`,
		`Tots els nivells/divisions`,
	} {
		if !strings.Contains(body, token) {
			t.Fatalf("la UI F35-4U2 ha de contenir %q; body=%s", token, body)
		}
	}
	for _, forbidden := range []string{
		`id="conf-export-religion" type="text" name="religio_confessio_codi"`,
		`id="conf-export-level" type="text" name="nivell_confessional_codi"`,
		`confessional-tabs`,
		`onclick=`,
		`onchange=`,
		`oninput=`,
		`onkeyup=`,
		`onfocus=`,
		`onblur=`,
		`onsubmit=`,
		`javascript:`,
		`style=`,
		`<style>`,
		`class="grup-camp checkbox-linia"`,
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("la UI F35-4U2 no ha de contenir %q; body=%s", forbidden, body)
		}
	}

	religionOptions := regexp.MustCompile(`(?s)<div id="conf-export-religion-options"[^>]*>(.*?)</div>`).FindStringSubmatch(body)
	if len(religionOptions) != 2 || !strings.Contains(religionOptions[1], `data-suggest-option`) {
		t.Fatalf("el contenidor d'opcions de religio no pot quedar buit; body=%s", body)
	}
	levelOptions := regexp.MustCompile(`(?s)<div id="conf-export-level-options"[^>]*>(.*?)</div>`).FindStringSubmatch(body)
	if len(levelOptions) != 2 || !strings.Contains(levelOptions[1], `data-suggest-option`) {
		t.Fatalf("el contenidor d'opcions de nivell no pot quedar buit; body=%s", body)
	}

	root := findProjectRoot(t)
	cssBody := readProjectFileF354S(t, root, "static/css/estils.css")
	pageCSSBody := readProjectFileF354S(t, root, "static/css/admin-import-export.css")
	jsBody := readProjectFileF354S(t, root, "static/js/admin-import-export.js")
	templateBody := readProjectFileF354S(t, root, "templates/admin-import-export.html")
	for _, token := range []string{
		`.io-tabs .tab-boto`,
		`.confessional-import-form`,
		`.confessional-import-layout`,
		`.confessional-export-options`,
		`.confessional-export-option`,
		`.confessional-selected-value.has-selection`,
		`.confessional-suggest-clear:focus-visible`,
	} {
		if !strings.Contains(pageCSSBody, token) {
			t.Fatalf("falta CSS F35-4U2 %q", token)
		}
	}
	if strings.Contains(cssBody, `.io-tabs .tab-boto`) || strings.Contains(cssBody, `.confessional-import-form`) {
		t.Fatalf("el CSS especific d'admin/import-export no ha de quedar duplicat a estils.css")
	}
	for _, token := range []string{
		`[data-local-suggest='1']`,
		`aria-expanded`,
		`mousedown`,
		`religionFilterHidden`,
		`form.querySelectorAll("[data-local-suggest='1']")`,
		`pane.hidden = !activePane;`,
	} {
		if !strings.Contains(jsBody, token) {
			t.Fatalf("falta JS F35-4U2 %q", token)
		}
	}
	if strings.Contains(templateBody, "confessional-tabs") || strings.Contains(templateBody, "onclick=") || strings.Contains(templateBody, "<style>") {
		t.Fatalf("la plantilla F35-4U2 no ha de reintroduir tabs legacy ni inline")
	}
	assertConfessionalExportTabContract(t, body)
}

func TestF354U3ConfessionalAutocompleteAssetsAndAliasSubtabWork(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u3_ui.sqlite3")

	user := createTestUser(t, database, "f35_4u3_ui_"+time.Now().Format("150405000000000"))
	session := createSessionCookie(t, database, user.ID, "sess_f35_4u3_ui_"+time.Now().Format("150405000000000"))
	policyID := createPolicyWithGrant(t, database, "f35_4u3_ui", "territori.confessional.import_export.export")
	addGrantToPolicy(t, database, policyID, "territori.confessional.import_export.import")
	addGrantToPolicy(t, database, policyID, "territori.confessional.import_export.view")
	assignPolicyToUser(t, database, user.ID, policyID)

	req := httptest.NewRequest(http.MethodGet, "/admin/import-export?tab=confessional&confessional_subtab=confessional-export", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminImportExport(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminImportExport F35-4U3 status=%d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !regexp.MustCompile(`id="tab-confessional-export" class="tab-pane actiu"[^>]*data-tab-panel="confessional-export"[^>]*role="tabpanel"`).MatchString(body) ||
		regexp.MustCompile(`id="tab-confessional-export" class="tab-pane actiu"[^>]*hidden`).MatchString(body) {
		t.Fatalf("confessional_subtab ha d'activar el panell export; body=%s", body)
	}
	if !regexp.MustCompile(`id="tab-confessional-import" class="tab-pane[^"]*"[^>]*data-tab-panel="confessional-import"[^>]*role="tabpanel"[^>]*hidden`).MatchString(body) {
		t.Fatalf("el subpanell import confessional inactiu ha de renderitzar hidden; body=%s", body)
	}
	if !strings.Contains(body, `<script src="/static/js/admin-import-export.js?v=3"></script>`) {
		t.Fatalf("la resposta ha de carregar el JS extern d'admin import/export; body=%s", body)
	}
	if !strings.Contains(body, `<link rel="stylesheet" href="/static/css/admin-import-export.css?v=1">`) {
		t.Fatalf("la resposta ha de carregar el CSS extern versionat d'admin import/export; body=%s", body)
	}

	staticReq := httptest.NewRequest(http.MethodGet, "http://localhost:8080/static/js/admin-import-export.js", nil)
	staticRR := httptest.NewRecorder()
	core.ServeStatic(staticRR, staticReq)
	if staticRR.Code != http.StatusOK {
		t.Fatalf("ServeStatic del JS F35-4U3 ha de retornar 200, got %d", staticRR.Code)
	}
	jsServed := staticRR.Body.String()
	for _, token := range []string{`setupLocalSuggest`, `aria-expanded`, `mousedown`} {
		if !strings.Contains(jsServed, token) {
			t.Fatalf("el fitxer JS servit ha de contenir %q", token)
		}
	}

	staticReq = httptest.NewRequest(http.MethodGet, "http://localhost:8080/static/css/admin-import-export.css", nil)
	staticRR = httptest.NewRecorder()
	core.ServeStatic(staticRR, staticReq)
	if staticRR.Code != http.StatusOK {
		t.Fatalf("ServeStatic del CSS F35-4U3 ha de retornar 200, got %d", staticRR.Code)
	}
	cssServed := staticRR.Body.String()
	for _, token := range []string{`.io-tabs .tab-pane[hidden]`, `.confessional-export-option`, `.confessional-suggestions`} {
		if !strings.Contains(cssServed, token) {
			t.Fatalf("el fitxer CSS servit ha de contenir %q", token)
		}
	}

	root := findProjectRoot(t)
	assertNoInvalidCSSUnits(t, readProjectFileF354S(t, root, "static/css/estils.css"), "static/css/estils.css")
	assertNoInvalidCSSUnits(t, readProjectFileF354S(t, root, "static/css/admin-import-export.css"), "static/css/admin-import-export.css")
	assertAdminImportExportInitialVisibility(t, body)
	assertConfessionalExportTabContract(t, body)
}

func TestF354U4ConfessionalExportSelectorsStayEmptyWithoutRealEntities(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u4_empty.sqlite3")

	user := createTestUser(t, database, "f35_4u4_empty_"+time.Now().Format("150405000000000"))
	session := createSessionCookie(t, database, user.ID, "sess_f35_4u4_empty_"+time.Now().Format("150405000000000"))
	policyID := createPolicyWithGrant(t, database, "f35_4u4_empty", "territori.confessional.import_export.export")
	addGrantToPolicy(t, database, policyID, "territori.confessional.import_export.import")
	addGrantToPolicy(t, database, policyID, "territori.confessional.import_export.view")
	assignPolicyToUser(t, database, user.ID, policyID)

	req := httptest.NewRequest(http.MethodGet, "/admin/import-export?tab=confessional&subtab=confessional-export", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminImportExport(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminImportExport F35-4U4 empty status=%d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, `data-confessional-export-form`) {
		t.Fatalf("el formulari export confessional ha d'existir sense entitats reals; body=%s", body)
	}
	if got := extractSuggestOptionsFromContainer(t, body, "conf-export-religion-options"); len(got) != 0 {
		t.Fatalf("sense entitats reals el selector d'export de religions ha de quedar buit, got=%v", got)
	}
	if got := extractSuggestOptionsFromContainer(t, body, "conf-export-level-options"); len(got) != 0 {
		t.Fatalf("sense entitats reals el selector d'export de nivells ha de quedar buit, got=%v", got)
	}
	for _, token := range []string{
		`data-confessional-import-form`,
		`/admin/confessional/import/dry-run`,
		`id="conf-import-include-non-published"`,
	} {
		if !strings.Contains(body, token) {
			t.Fatalf("la pestanya d'import ha de continuar intacta; falta %q body=%s", token, body)
		}
	}
}

func TestF354U4ConfessionalExportSelectorsUseOnlySingleEntityData(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u4_single.sqlite3")

	user := createTestUser(t, database, "f35_4u4_single_"+time.Now().Format("150405000000000"))
	session := createSessionCookie(t, database, user.ID, "sess_f35_4u4_single_"+time.Now().Format("150405000000000"))
	policyID := createPolicyWithGrant(t, database, "f35_4u4_single", "territori.confessional.import_export.export")
	addGrantToPolicy(t, database, policyID, "territori.confessional.import_export.view")
	assignPolicyToUser(t, database, user.ID, policyID)
	f354SSaveEntitat(t, database, "f35_4u4_single_entity_"+time.Now().Format("150405000000000"), "Parroquia F35-4U4", "catolicisme_ritu_llati", "parroquia", "publicat")

	req := httptest.NewRequest(http.MethodGet, "/admin/import-export?tab=confessional&subtab=confessional-export", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminImportExport(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminImportExport F35-4U4 single status=%d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()

	religions := extractSuggestOptionsFromContainer(t, body, "conf-export-religion-options")
	if len(religions) != 1 || religions[0].Code != "catolicisme_ritu_llati" {
		t.Fatalf("amb una sola entitat religiosa l'export ha de mostrar nomes la seva religio real, got=%v", religions)
	}
	levels := extractSuggestOptionsFromContainer(t, body, "conf-export-level-options")
	if len(levels) != 1 || levels[0].Code != "parroquia" || levels[0].ReligionCode != "catolicisme_ritu_llati" {
		t.Fatalf("amb una sola entitat religiosa l'export ha de mostrar nomes el seu nivell real, got=%v", levels)
	}
	if strings.Contains(body, `data-code="anglicanisme"`) || strings.Contains(body, `data-code="bisbat_diocesi"`) {
		t.Fatalf("l'export no ha de mostrar opcions sense dades reals; body=%s", body)
	}
}

func TestF354U4ConfessionalExportSelectorsUseOnlyRealReligions(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u4_religions.sqlite3")

	user := createTestUser(t, database, "f35_4u4_religions_"+time.Now().Format("150405000000000"))
	session := createSessionCookie(t, database, user.ID, "sess_f35_4u4_religions_"+time.Now().Format("150405000000000"))
	policyID := createPolicyWithGrant(t, database, "f35_4u4_religions", "territori.confessional.import_export.export")
	addGrantToPolicy(t, database, policyID, "territori.confessional.import_export.view")
	assignPolicyToUser(t, database, user.ID, policyID)
	f354SSaveEntitat(t, database, "f35_4u4_religions_cat_"+time.Now().Format("150405000000000"), "Bisbat F35-4U4", "catolicisme_ritu_llati", "bisbat_diocesi", "publicat")
	f354SSaveEntitat(t, database, "f35_4u4_religions_ort_"+time.Now().Format("150405000000000"), "Parroquia Ortodoxa F35-4U4", "ortodoxia", "ortodoxia_parroquia", "publicat")

	req := httptest.NewRequest(http.MethodGet, "/admin/import-export?tab=confessional&subtab=confessional-export", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminImportExport(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminImportExport F35-4U4 religions status=%d body=%s", rr.Code, rr.Body.String())
	}

	religions := extractSuggestOptionsFromContainer(t, rr.Body.String(), "conf-export-religion-options")
	if !hasSuggestCode(religions, "catolicisme_ritu_llati") || !hasSuggestCode(religions, "ortodoxia") {
		t.Fatalf("l'export ha de llistar nomes religions amb entitats reals, got=%v", religions)
	}
	if hasSuggestCode(religions, "anglicanisme") || hasSuggestCode(religions, "budisme") {
		t.Fatalf("l'export no ha de llistar religions sense entitats reals, got=%v", religions)
	}
}

func TestF354U4ConfessionalExportLevelsKeepOnlyRealCompatibleOptions(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u4_levels.sqlite3")

	user := createTestUser(t, database, "f35_4u4_levels_"+time.Now().Format("150405000000000"))
	session := createSessionCookie(t, database, user.ID, "sess_f35_4u4_levels_"+time.Now().Format("150405000000000"))
	policyID := createPolicyWithGrant(t, database, "f35_4u4_levels", "territori.confessional.import_export.export")
	addGrantToPolicy(t, database, policyID, "territori.confessional.import_export.view")
	assignPolicyToUser(t, database, user.ID, policyID)
	f354SSaveEntitat(t, database, "f35_4u4_levels_bisbat_"+time.Now().Format("150405000000000"), "Bisbat F35-4U4", "catolicisme_ritu_llati", "bisbat_diocesi", "publicat")
	f354SSaveEntitat(t, database, "f35_4u4_levels_parroquia_"+time.Now().Format("150405000000000"), "Parroquia F35-4U4", "catolicisme_ritu_llati", "parroquia", "publicat")
	f354SSaveEntitat(t, database, "f35_4u4_levels_ort_"+time.Now().Format("150405000000000"), "Parroquia Ortodoxa F35-4U4", "ortodoxia", "ortodoxia_parroquia", "publicat")

	req := httptest.NewRequest(http.MethodGet, "/admin/import-export?tab=confessional&subtab=confessional-export", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminImportExport(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminImportExport F35-4U4 levels status=%d body=%s", rr.Code, rr.Body.String())
	}

	levels := extractSuggestOptionsFromContainer(t, rr.Body.String(), "conf-export-level-options")
	if !hasSuggestOption(levels, "bisbat_diocesi", "catolicisme_ritu_llati") ||
		!hasSuggestOption(levels, "parroquia", "catolicisme_ritu_llati") ||
		!hasSuggestOption(levels, "ortodoxia_parroquia", "ortodoxia") {
		t.Fatalf("els nivells exportables han de sortir de dades reals amb religio compatible, got=%v", levels)
	}
	if hasSuggestCode(levels, "arxiprestat_vicariat_forani") || hasSuggestCode(levels, "ortodoxia_metropolia") {
		t.Fatalf("els nivells sense entitats reals no han d'apareixer, got=%v", levels)
	}
}

func TestF354U3ConfessionalExportOnlyDefaultsVisible(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/admin/import-export?tab=confessional", nil)
	rr := httptest.NewRecorder()
	core.RenderPrivateTemplate(rr, req, "admin-import-export.html", map[string]interface{}{
		"User":                        &db.User{Usuari: "f35_4u3_export_only"},
		"ActiveTab":                   "confessional",
		"ConfessionalSubtab":          "confessional-export",
		"CanConfessionalView":         true,
		"CanConfessionalImport":       false,
		"CanConfessionalExport":       true,
		"ConfessionalExportLevels":    []map[string]string{},
		"ConfessionalExportReligions": []map[string]string{},
		"CanTerritoriImport":          false,
		"CanTerritoriExport":          false,
		"CanEclesImport":              false,
		"CanEclesExport":              false,
		"CanArxiusImport":             false,
		"CanArxiusExport":             false,
		"CanLlibresImport":            false,
		"CanLlibresExport":            false,
	})
	body := rr.Body.String()
	if !strings.Contains(body, `data-confessional-export-tab="confessional-export"`) {
		t.Fatalf("ha d'existir el boto export confessional; body=%s", body)
	}
	if !regexp.MustCompile(`data-confessional-export-tab="confessional-export"[^>]*role="tab"[^>]*aria-selected="true"`).MatchString(body) {
		t.Fatalf("exportar ha de quedar actiu per defecte quan nomes hi ha export; body=%s", body)
	}
	if !regexp.MustCompile(`data-confessional-export-panel="confessional-export"[^>]*role="tabpanel"`).MatchString(body) ||
		regexp.MustCompile(`data-confessional-export-panel="confessional-export"[^>]*hidden`).MatchString(body) {
		t.Fatalf("el panell export confessional no pot quedar hidden en mode nomes export; body=%s", body)
	}
	assertConfessionalExportFormContents(t, body)
	assertConfessionalExportTabContract(t, body)
}

func TestF354UExportUsesPortableStableReferences(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u_export.sqlite3")
	user := createTestUser(t, database, "f35_4u_export_"+time.Now().Format("150405000000000"))
	session := createSessionCookie(t, database, user.ID, "sess_f35_4u_export_"+time.Now().Format("150405000000000"))
	policyID := createPolicyWithGrant(t, database, "f35_4u_export", "territori.confessional.import_export.export")
	assignPolicyToUser(t, database, user.ID, policyID)

	municipiID := f353YCreateMunicipi(t, database, "Municipi F35-4U export "+time.Now().Format("150405000000000"))
	nucliID, err := database.CreateMunicipi(&db.Municipi{
		Nom:            "Nucli F35-4U export " + time.Now().Format("150405000000000"),
		Tipus:          "nucli_urba",
		MunicipiID:     sql.NullInt64{Int64: int64(municipiID), Valid: true},
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateMunicipi nucli: %v", err)
	}
	parentID := f354SSaveEntitat(t, database, "f35_4u_parent_"+time.Now().Format("150405000000000"), "Bisbat F35-4U export", "catolicisme_ritu_llati", "bisbat_diocesi", "publicat")
	childID := f354SSaveEntitat(t, database, "f35_4u_child_"+time.Now().Format("150405000000000"), "Parroquia F35-4U export", "catolicisme_ritu_llati", "parroquia", "publicat")
	f354SSaveEntitatRelacio(t, database, parentID, childID, "parroquia", "publicat")
	if _, err := database.SaveMunicipiEntitatReligiosa(&db.MunicipiEntitatReligiosa{
		MunicipiID:         municipiID,
		NucliID:            sql.NullInt64{Int64: int64(nucliID), Valid: true},
		EntitatReligiosaID: childID,
		TipusRelacio:       "parroquia_local",
		ModeracioEstat:     "publicat",
	}); err != nil {
		t.Fatalf("SaveMunicipiEntitatReligiosa: %v", err)
	}
	arxiuID := f354SCreateArxiu(t, database, "Arxiu F35-4U export", municipiID)
	f354SSaveArxiuEntitatRelacio(t, database, arxiuID, childID, "custodia", "publicat")

	req := httptest.NewRequest(http.MethodGet, "/admin/confessional/export", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminConfessionalExport(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminConfessionalExport status=%d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if strings.Contains(body, "created_by") || strings.Contains(body, "updated_by") || strings.Contains(body, "entitat_origen_id") {
		t.Fatalf("l'export no ha d'exposar camps interns ni IDs-only; body=%s", body)
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("export json invalid: %v", err)
	}
	if payload["schema"] != "cercagenealogica.confessional.v1" {
		t.Fatalf("schema exportat inesperat: %#v", payload["schema"])
	}
	if !strings.Contains(body, `"code"`) || !strings.Contains(body, `"religion_code"`) || !strings.Contains(body, `"level_code"`) {
		t.Fatalf("l'export ha d'incloure claus estables d'entitat; body=%s", body)
	}
	if !strings.Contains(body, `"municipality"`) || !strings.Contains(body, `"archive"`) {
		t.Fatalf("l'export ha d'incloure referencies portables de municipi i arxiu; body=%s", body)
	}
}

func TestF354U2ConfessionalExportRejectsInvalidCatalogFilters(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u2_invalid_filter.sqlite3")
	user := createTestUser(t, database, "f35_4u2_invalid_"+time.Now().Format("150405000000000"))
	session := createSessionCookie(t, database, user.ID, "sess_f35_4u2_invalid_"+time.Now().Format("150405000000000"))
	policyID := createPolicyWithGrant(t, database, "f35_4u2_invalid", "territori.confessional.import_export.export")
	assignPolicyToUser(t, database, user.ID, policyID)

	req := httptest.NewRequest(http.MethodGet, "/admin/confessional/export?religio_confessio_codi=no_existeix", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminConfessionalExport(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("filtre religio invalid ha de retornar 400, got=%d body=%s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/admin/confessional/export?religio_confessio_codi=catolicisme_ritu_llati&nivell_confessional_codi=ortodoxia_parroquia", nil)
	req.AddCookie(session)
	rr = httptest.NewRecorder()
	app.AdminConfessionalExport(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("nivell incompatible ha de retornar 400, got=%d body=%s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/admin/confessional/export?religio_confessio_codi=cat", nil)
	req.AddCookie(session)
	rr = httptest.NewRecorder()
	app.AdminConfessionalExport(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("filtre religio parcial ha de retornar 400, got=%d body=%s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/admin/confessional/export?religio_confessio_codi=catolicisme_ritu_llati&nivell_confessional_codi=arxiprestat_vicariat_forani", nil)
	req.AddCookie(session)
	rr = httptest.NewRecorder()
	app.AdminConfessionalExport(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("codis reals del cataleg han de ser acceptats, got=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestF354UDryRunApplyAndReimportArePortableAndIdempotent(t *testing.T) {
	sourceApp, sourceDB := newTestAppForLogin(t, "test_f35_4u_source.sqlite3")
	sourceUser := createTestUser(t, sourceDB, "f35_4u_source_"+time.Now().Format("150405000000000"))
	sourceSession := createSessionCookie(t, sourceDB, sourceUser.ID, "sess_f35_4u_source_"+time.Now().Format("150405000000000"))
	sourcePolicy := createPolicyWithGrant(t, sourceDB, "f35_4u_source_export", "territori.confessional.import_export.export")
	assignPolicyToUser(t, sourceDB, sourceUser.ID, sourcePolicy)

	suffix := time.Now().Format("150405000000000")
	sourceMunicipiID := f353YCreateMunicipi(t, sourceDB, "Municipi F35-4U "+suffix)
	sourceParentID := f354SSaveEntitat(t, sourceDB, "f35_4u_root_"+suffix, "Bisbat F35-4U "+suffix, "catolicisme_ritu_llati", "bisbat_diocesi", "publicat")
	sourceChildID := f354SSaveEntitat(t, sourceDB, "f35_4u_local_"+suffix, "Parroquia F35-4U "+suffix, "catolicisme_ritu_llati", "parroquia", "publicat")
	sourceRelID := f354SSaveEntitatRelacio(t, sourceDB, sourceParentID, sourceChildID, "parroquia", "publicat")
	if sourceRelID <= 0 {
		t.Fatalf("relacio jerarquica font no creada")
	}
	f354SSaveMunicipiEntitatRelacio(t, sourceDB, sourceMunicipiID, sourceChildID, "parroquia_local", "publicat")
	sourceArxiuID := f354SCreateArxiu(t, sourceDB, "Arxiu F35-4U "+suffix, sourceMunicipiID)
	f354SSaveArxiuEntitatRelacio(t, sourceDB, sourceArxiuID, sourceChildID, "custodia", "publicat")

	req := httptest.NewRequest(http.MethodGet, "/admin/confessional/export", nil)
	req.AddCookie(sourceSession)
	rr := httptest.NewRecorder()
	sourceApp.AdminConfessionalExport(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("export status=%d body=%s", rr.Code, rr.Body.String())
	}
	exportedJSON := rr.Body.Bytes()

	targetApp, targetDB := newTestAppForLogin(t, "test_f35_4u_target.sqlite3")
	targetUser := createTestUser(t, targetDB, "f35_4u_target_"+time.Now().Format("150405000000000"))
	targetSession := createSessionCookie(t, targetDB, targetUser.ID, "sess_f35_4u_target_"+time.Now().Format("150405000000000"))
	targetPolicy := createPolicyWithGrant(t, targetDB, "f35_4u_target_import", "territori.confessional.import_export.import")
	addGrantToPolicy(t, targetDB, targetPolicy, "territori.confessional.import_export.view")
	assignPolicyToUser(t, targetDB, targetUser.ID, targetPolicy)

	if _, err := targetDB.CreateMunicipi(&db.Municipi{
		Nom:            "Placeholder F35-4U " + suffix,
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	}); err != nil {
		t.Fatalf("placeholder municipi: %v", err)
	}
	targetMunicipiID := f353YCreateMunicipi(t, targetDB, "Municipi F35-4U "+suffix)
	if _, err := targetDB.SaveEntitatReligiosa(&db.EntitatReligiosa{
		Codi:                   "dummy_target_pre_" + suffix,
		Nom:                    "Dummy target pre " + suffix,
		ReligioConfessioCodi:   "catolicisme_ritu_llati",
		NivellConfessionalCodi: "bisbat_diocesi",
		Estat:                  "actiu",
		ModeracioEstat:         "publicat",
	}); err != nil {
		t.Fatalf("dummy target pre entity: %v", err)
	}
	if _, err := targetDB.SaveEntitatReligiosa(&db.EntitatReligiosa{
		Codi:                   "dummy_target_" + suffix,
		Nom:                    "Dummy target " + suffix,
		ReligioConfessioCodi:   "catolicisme_ritu_llati",
		NivellConfessionalCodi: "bisbat_diocesi",
		Estat:                  "actiu",
		ModeracioEstat:         "publicat",
	}); err != nil {
		t.Fatalf("dummy target entity: %v", err)
	}
	targetArxiuID := f354SCreateArxiu(t, targetDB, "Arxiu F35-4U "+suffix, targetMunicipiID)
	if targetArxiuID <= 0 {
		t.Fatalf("arxiu target no creat")
	}

	csrfToken, csrfCookie := extractCSRFContextFromImportExport(t, targetApp, targetSession)
	dryRunReq := newMultipartRequest(t, "/admin/confessional/import/dry-run", "import_file", "confessional-export.json", exportedJSON, map[string]string{
		"csrf_token": csrfToken,
	})
	dryRunReq.AddCookie(targetSession)
	dryRunReq.AddCookie(csrfCookie)
	dryRunRR := httptest.NewRecorder()
	targetApp.AdminConfessionalImportDryRun(dryRunRR, dryRunReq)
	if dryRunRR.Code != http.StatusOK {
		t.Fatalf("dry-run status=%d body=%s", dryRunRR.Code, dryRunRR.Body.String())
	}
	dryRunBody := dryRunRR.Body.String()
	for _, token := range []string{
		`name="payload_b64"`,
		`/admin/confessional/import/apply`,
		`Entitats a crear`,
	} {
		if !strings.Contains(dryRunBody, token) {
			t.Fatalf("dry-run ha de mostrar %q; body=%s", token, dryRunBody)
		}
	}
	if rows, _ := targetDB.ListEntitatsReligioses(); len(filterPublishedConfEntitats(rows)) != 2 {
		t.Fatalf("el dry-run no ha de crear entitats noves")
	}

	payloadB64 := extractHiddenTextareaValue(t, dryRunBody, "payload_b64")
	applyReq := httptest.NewRequest(http.MethodPost, "/admin/confessional/import/apply", strings.NewReader("csrf_token="+extractCSRFTokenFromHTML(t, dryRunBody)+"&payload_b64="+payloadB64))
	applyReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	applyReq.AddCookie(targetSession)
	applyReq.AddCookie(csrfCookie)
	applyRR := httptest.NewRecorder()
	targetApp.AdminConfessionalImportApply(applyRR, applyReq)
	if applyRR.Code != http.StatusSeeOther {
		t.Fatalf("apply status=%d body=%s", applyRR.Code, applyRR.Body.String())
	}

	targetEntities, _ := targetDB.ListEntitatsReligioses()
	targetPublished := filterPublishedConfEntitats(targetEntities)
	if len(targetPublished) != 4 {
		t.Fatalf("s'esperaven 4 entitats publicades al target (2 dummy + 2 importades), got %d", len(targetPublished))
	}
	targetHierarchy, _ := targetDB.ListEntitatReligiosaRelacions()
	if len(filterPublishedConfHierarchy(targetHierarchy)) != 1 {
		t.Fatalf("s'esperava 1 relacio jerarquica publicada al target")
	}
	targetTerritory, _ := targetDB.ListMunicipiEntitatsReligioses(0)
	if len(filterPublishedConfTerritory(targetTerritory)) != 1 {
		t.Fatalf("s'esperava 1 relacio territorial publicada al target")
	}
	targetArchiveRels, _ := targetDB.ListArxiuEntitatsReligioses(0, 0, "")
	if len(filterPublishedConfArchive(targetArchiveRels)) != 1 {
		t.Fatalf("s'esperava 1 relacio arxiu-entitat publicada al target")
	}
	importedParent := findConfEntityByCode(targetPublished, "f35_4u_root_"+suffix)
	importedChild := findConfEntityByCode(targetPublished, "f35_4u_local_"+suffix)
	if importedParent == nil || importedChild == nil {
		t.Fatalf("no s'han trobat les entitats importades al target")
	}
	if importedParent.ID == sourceParentID || importedChild.ID == sourceChildID {
		t.Fatalf("la prova necessita IDs interns diferents entre entorns per validar portabilitat")
	}
	if !hasHierarchyRelation(targetHierarchy, importedParent.ID, importedChild.ID) {
		t.Fatalf("la relacio jerarquica importada no s'ha reconstruït amb els IDs del target")
	}

	applyReq = httptest.NewRequest(http.MethodPost, "/admin/confessional/import/apply", strings.NewReader("csrf_token="+csrfToken+"&payload_b64="+payloadB64))
	applyReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	applyReq.AddCookie(targetSession)
	applyReq.AddCookie(csrfCookie)
	applyRR = httptest.NewRecorder()
	targetApp.AdminConfessionalImportApply(applyRR, applyReq)
	if applyRR.Code != http.StatusSeeOther {
		t.Fatalf("reapply status=%d body=%s", applyRR.Code, applyRR.Body.String())
	}
	targetEntities, _ = targetDB.ListEntitatsReligioses()
	if len(filterPublishedConfEntitats(targetEntities)) != 4 {
		t.Fatalf("el reimport no ha de duplicar entitats")
	}
	targetHierarchy, _ = targetDB.ListEntitatReligiosaRelacions()
	if len(filterPublishedConfHierarchy(targetHierarchy)) != 1 {
		t.Fatalf("el reimport no ha de duplicar relacions jerarquiques")
	}
}

func TestF354UDryRunDetectsUnresolvedReferencesAndCycles(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u_errors.sqlite3")
	user := createTestUser(t, database, "f35_4u_errors_"+time.Now().Format("150405000000000"))
	session := createSessionCookie(t, database, user.ID, "sess_f35_4u_errors_"+time.Now().Format("150405000000000"))
	policyID := createPolicyWithGrant(t, database, "f35_4u_errors", "territori.confessional.import_export.import")
	assignPolicyToUser(t, database, user.ID, policyID)

	payload := map[string]interface{}{
		"schema":      "cercagenealogica.confessional.v1",
		"exported_at": time.Now().Format(time.RFC3339),
		"source": map[string]string{
			"app":    "CercaGenealogica",
			"module": "confessional",
		},
		"items": map[string]interface{}{
			"entitats_religioses": []map[string]interface{}{
				{
					"ref": map[string]string{
						"code":          "f35_4u_err_a",
						"religion_code": "catolicisme_ritu_llati",
						"level_code":    "santa_seu",
					},
					"name":              "Seu error A",
					"religion_code":     "catolicisme_ritu_llati",
					"level_code":        "santa_seu",
					"moderation_status": "publicat",
				},
				{
					"ref": map[string]string{
						"code":          "f35_4u_err_b",
						"religion_code": "catolicisme_ritu_llati",
						"level_code":    "bisbat_diocesi",
					},
					"name":              "Seu error B",
					"religion_code":     "catolicisme_ritu_llati",
					"level_code":        "bisbat_diocesi",
					"moderation_status": "publicat",
				},
			},
			"relacions_entitats": []map[string]interface{}{
				{
					"parent":        map[string]string{"code": "f35_4u_err_a", "religion_code": "catolicisme_ritu_llati", "level_code": "santa_seu"},
					"child":         map[string]string{"code": "f35_4u_err_b", "religion_code": "catolicisme_ritu_llati", "level_code": "bisbat_diocesi"},
					"relation_type": "jerarquica",
				},
				{
					"parent":        map[string]string{"code": "f35_4u_err_b", "religion_code": "catolicisme_ritu_llati", "level_code": "bisbat_diocesi"},
					"child":         map[string]string{"code": "f35_4u_err_a", "religion_code": "catolicisme_ritu_llati", "level_code": "santa_seu"},
					"relation_type": "jerarquica",
				},
			},
			"relacions_territorials": []map[string]interface{}{
				{
					"entity":        map[string]string{"code": "f35_4u_err_b", "religion_code": "catolicisme_ritu_llati", "level_code": "bisbat_diocesi"},
					"municipality":  map[string]string{"name": "Municipi inexistent", "type": "municipi", "country_iso2": "ES"},
					"relation_type": "parroquia_local",
				},
			},
			"relacions_arxius": []map[string]interface{}{
				{
					"entity":        map[string]string{"code": "f35_4u_err_b", "religion_code": "catolicisme_ritu_llati", "level_code": "bisbat_diocesi"},
					"archive":       map[string]string{"name": "Arxiu inexistent", "type": "parroquia"},
					"relation_type": "custodia",
				},
			},
		},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	csrfToken, csrfCookie := extractCSRFContextFromImportExport(t, app, session)
	req := newMultipartRequest(t, "/admin/confessional/import/dry-run", "import_file", "bad-confessional.json", raw, map[string]string{
		"csrf_token": csrfToken,
	})
	req.AddCookie(session)
	req.AddCookie(csrfCookie)
	rr := httptest.NewRecorder()
	app.AdminConfessionalImportDryRun(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("dry-run errors status=%d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	for _, token := range []string{
		`Municipi inexistent`,
		`Arxiu inexistent`,
		`f35_4u_err_a`,
	} {
		if !strings.Contains(body, token) {
			t.Fatalf("dry-run ha de mostrar %q; body=%s", token, body)
		}
	}
	if strings.Contains(body, `/admin/confessional/import/apply`) {
		t.Fatalf("dry-run amb errors bloquejants no ha de permetre apply; body=%s", body)
	}
	if entities, _ := database.ListEntitatsReligioses(); len(entities) != 0 {
		t.Fatalf("el dry-run amb errors no ha de crear cap entitat")
	}
}

func newMultipartRequest(t *testing.T, path, fieldName, fileName string, fileBody []byte, fields map[string]string) *http.Request {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for key, value := range fields {
		if err := writer.WriteField(key, value); err != nil {
			t.Fatalf("WriteField %s: %v", key, err)
		}
	}
	part, err := writer.CreateFormFile(fieldName, fileName)
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := part.Write(fileBody); err != nil {
		t.Fatalf("part.Write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, path, &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Accept-Language", "ca")
	return req
}

func extractCSRFFromImportExport(t *testing.T, app *core.App, session *http.Cookie) string {
	t.Helper()
	token, _ := extractCSRFContextFromImportExport(t, app, session)
	return token
}

func extractCSRFContextFromImportExport(t *testing.T, app *core.App, session *http.Cookie) (string, *http.Cookie) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/admin/import-export?tab=confessional&subtab=confessional-import", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminImportExport(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminImportExport per extreure CSRF ha fallat: %d", rr.Code)
	}
	resp := rr.Result()
	defer resp.Body.Close()
	var csrfCookie *http.Cookie
	for _, cookie := range resp.Cookies() {
		if cookie.Name == "cg_csrf" {
			cloned := *cookie
			csrfCookie = &cloned
			break
		}
	}
	if csrfCookie == nil {
		t.Fatalf("no s'ha trobat la cookie cg_csrf a la resposta")
	}
	return extractCSRFTokenFromHTML(t, rr.Body.String()), csrfCookie
}

func extractCSRFTokenFromHTML(t *testing.T, body string) string {
	t.Helper()
	re := regexp.MustCompile(`name="csrf_token" value="([^"]+)"`)
	match := re.FindStringSubmatch(body)
	if len(match) != 2 {
		t.Fatalf("no s'ha trobat csrf_token al body")
	}
	return html.UnescapeString(match[1])
}

func extractHiddenTextareaValue(t *testing.T, body, name string) string {
	t.Helper()
	re := regexp.MustCompile(`(?s)<textarea name="` + regexp.QuoteMeta(name) + `" hidden>(.*?)</textarea>`)
	match := re.FindStringSubmatch(body)
	if len(match) != 2 {
		t.Fatalf("no s'ha trobat textarea %s al body", name)
	}
	return html.UnescapeString(strings.TrimSpace(match[1]))
}

func assertNoInvalidCSSUnits(t *testing.T, body, path string) {
	t.Helper()
	re := regexp.MustCompile(`[0-9]+ +(px|fr|s)\b`)
	if match := re.FindString(body); match != "" {
		t.Fatalf("%s conte CSS invalid amb unitats separades: %q", path, match)
	}
}

func assertConfessionalExportTabContract(t *testing.T, body string) {
	t.Helper()
	buttonMatch := regexp.MustCompile(`data-confessional-export-tab="([^"]+)"`).FindStringSubmatch(body)
	panelMatch := regexp.MustCompile(`data-confessional-export-panel="([^"]+)"`).FindStringSubmatch(body)
	buttonDataMatch := regexp.MustCompile(`data-confessional-export-tab="[^"]+"[^>]*data-tab="([^"]+)"|data-tab="([^"]+)"[^>]*data-confessional-export-tab="[^"]+"`).FindStringSubmatch(body)
	panelDataMatch := regexp.MustCompile(`data-confessional-export-panel="[^"]+"[^>]*data-tab-panel="([^"]+)"|data-tab-panel="([^"]+)"[^>]*data-confessional-export-panel="[^"]+"`).FindStringSubmatch(body)
	if len(buttonMatch) != 2 || len(panelMatch) != 2 {
		t.Fatalf("calen data-confessional-export-tab i data-confessional-export-panel; body=%s", body)
	}
	if len(buttonDataMatch) == 0 || len(panelDataMatch) == 0 {
		t.Fatalf("calen data-tab i data-tab-panel coherents per l'export confessional; body=%s", body)
	}
	buttonValue := buttonMatch[1]
	panelValue := panelMatch[1]
	if buttonValue != "confessional-export" || panelValue != "confessional-export" {
		t.Fatalf("els identifiers export confessionals han de ser confessional-export; boto=%q panell=%q body=%s", buttonValue, panelValue, body)
	}
	buttonDataValue := firstNonEmpty(buttonDataMatch[1:]...)
	panelDataValue := firstNonEmpty(panelDataMatch[1:]...)
	if buttonDataValue != buttonValue || panelDataValue != panelValue {
		t.Fatalf("data-tab/data-tab-panel han de coincidir amb els data-confessional-export-*; data-tab=%q data-tab-panel=%q export=%q body=%s", buttonDataValue, panelDataValue, buttonValue, body)
	}
	assertConfessionalExportFormContents(t, body)
}

func assertConfessionalExportFormContents(t *testing.T, body string) {
	t.Helper()
	for _, token := range []string{
		`data-confessional-export-form`,
		`name="include_hierarchy"`,
		`name="include_territorial"`,
		`name="include_archives"`,
		`name="include_non_published"`,
		`type="hidden" name="religio_confessio_codi"`,
		`type="hidden" name="nivell_confessional_codi"`,
		`conf-export-religion-search`,
		`conf-export-level-search`,
		`type="submit" class="boto-primari"`,
		`fa-file-export`,
	} {
		if !strings.Contains(body, token) {
			t.Fatalf("el formulari export confessional ha de contenir %q; body=%s", token, body)
		}
	}
}

func assertAdminImportExportInitialVisibility(t *testing.T, body string) {
	t.Helper()

	mainPanels := []string{"territori", "eclesiastic", "confessional", "arxius", "llibres"}
	mainVisible := 0
	for _, panel := range mainPanels {
		activePattern := regexp.MustCompile(`id="tab-` + regexp.QuoteMeta(panel) + `" class="tab-pane actiu"[^>]*data-tab-panel="` + regexp.QuoteMeta(panel) + `"[^>]*role="tabpanel"`)
		hiddenPattern := regexp.MustCompile(`id="tab-` + regexp.QuoteMeta(panel) + `" class="tab-pane[^"]*"[^>]*data-tab-panel="` + regexp.QuoteMeta(panel) + `"[^>]*role="tabpanel"[^>]*hidden`)
		if activePattern.MatchString(body) && !regexp.MustCompile(`id="tab-`+regexp.QuoteMeta(panel)+`" class="tab-pane actiu"[^>]*hidden`).MatchString(body) {
			mainVisible++
			continue
		}
		if strings.Contains(body, `id="tab-`+panel+`"`) &&
			!activePattern.MatchString(body) &&
			!hiddenPattern.MatchString(body) {
			t.Fatalf("el panell principal %s ha de renderitzar hidden quan es inactiu; body=%s", panel, body)
		}
	}
	if mainVisible != 1 {
		t.Fatalf("hi ha d'haver exactament un panell principal visible al render inicial, trobat=%d body=%s", mainVisible, body)
	}

	subtabs := map[string][]string{
		"territori":    {"territori-import", "territori-export"},
		"eclesiastic":  {"eclesiastic-import", "eclesiastic-export"},
		"confessional": {"confessional-import", "confessional-export"},
		"arxius":       {"arxius-import", "arxius-export"},
		"llibres":      {"llibres-import", "llibres-export"},
	}
	for group, panels := range subtabs {
		present := 0
		visible := 0
		for _, panel := range panels {
			activePattern := regexp.MustCompile(`id="tab-` + regexp.QuoteMeta(panel) + `" class="tab-pane actiu"[^>]*data-tab-panel="` + regexp.QuoteMeta(panel) + `"[^>]*role="tabpanel"`)
			hiddenPattern := regexp.MustCompile(`id="tab-` + regexp.QuoteMeta(panel) + `" class="tab-pane[^"]*"[^>]*data-tab-panel="` + regexp.QuoteMeta(panel) + `"[^>]*role="tabpanel"[^>]*hidden`)
			if activePattern.MatchString(body) {
				present++
				if !regexp.MustCompile(`id="tab-` + regexp.QuoteMeta(panel) + `" class="tab-pane actiu"[^>]*hidden`).MatchString(body) {
					visible++
				}
				continue
			}
			if hiddenPattern.MatchString(body) {
				present++
			}
		}
		if present > 0 && visible != 1 {
			t.Fatalf("el grup de subpestanyes %s ha de tenir exactament un panell visible, trobat=%d body=%s", group, visible, body)
		}
	}

	for _, forbidden := range []string{`style=`, `onclick=`, `onchange=`, `oninput=`, `onkeyup=`, `onfocus=`, `onblur=`, `onsubmit=`, `javascript:`} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("admin-import-export no ha de contenir %q; body=%s", forbidden, body)
		}
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func filterPublishedConfEntitats(all []db.EntitatReligiosa) []db.EntitatReligiosa {
	out := make([]db.EntitatReligiosa, 0)
	for _, item := range all {
		if item.ModeracioEstat == "publicat" {
			out = append(out, item)
		}
	}
	return out
}

func filterPublishedConfHierarchy(all []db.EntitatReligiosaRelacio) []db.EntitatReligiosaRelacio {
	out := make([]db.EntitatReligiosaRelacio, 0)
	for _, item := range all {
		if item.ModeracioEstat == "publicat" {
			out = append(out, item)
		}
	}
	return out
}

func filterPublishedConfTerritory(all []db.MunicipiEntitatReligiosa) []db.MunicipiEntitatReligiosa {
	out := make([]db.MunicipiEntitatReligiosa, 0)
	for _, item := range all {
		if item.ModeracioEstat == "publicat" {
			out = append(out, item)
		}
	}
	return out
}

func filterPublishedConfArchive(all []db.ArxiuEntitatReligiosa) []db.ArxiuEntitatReligiosa {
	out := make([]db.ArxiuEntitatReligiosa, 0)
	for _, item := range all {
		if item.ModeracioEstat == "publicat" {
			out = append(out, item)
		}
	}
	return out
}

func findConfEntityByCode(all []db.EntitatReligiosa, code string) *db.EntitatReligiosa {
	for i := range all {
		if all[i].Codi == code {
			return &all[i]
		}
	}
	return nil
}

func hasHierarchyRelation(all []db.EntitatReligiosaRelacio, parentID, childID int) bool {
	for _, item := range all {
		if item.ModeracioEstat == "publicat" && item.EntitatOrigenID == parentID && item.EntitatDestiID == childID {
			return true
		}
	}
	return false
}

type suggestOption struct {
	Code         string
	Label        string
	Context      string
	ReligionCode string
}

func extractSuggestOptionsFromContainer(t *testing.T, body, containerID string) []suggestOption {
	t.Helper()
	re := regexp.MustCompile(`(?s)<div id="` + regexp.QuoteMeta(containerID) + `"[^>]*>(.*?)</div>`)
	match := re.FindStringSubmatch(body)
	if len(match) != 2 {
		t.Fatalf("no s'ha trobat el contenidor %s", containerID)
	}
	tagRe := regexp.MustCompile(`(?s)<span[^>]*data-suggest-option[^>]*></span>`)
	tags := tagRe.FindAllString(match[1], -1)
	out := make([]suggestOption, 0, len(tags))
	for _, tag := range tags {
		out = append(out, suggestOption{
			Code:         extractHTMLAttr(tag, "data-code"),
			Label:        extractHTMLAttr(tag, "data-label"),
			Context:      extractHTMLAttr(tag, "data-context"),
			ReligionCode: extractHTMLAttr(tag, "data-religion-code"),
		})
	}
	return out
}

func extractHTMLAttr(tag, attr string) string {
	re := regexp.MustCompile(attr + `="([^"]*)"`)
	match := re.FindStringSubmatch(tag)
	if len(match) != 2 {
		return ""
	}
	return html.UnescapeString(match[1])
}

func hasSuggestCode(options []suggestOption, code string) bool {
	for _, option := range options {
		if option.Code == code {
			return true
		}
	}
	return false
}

func hasSuggestOption(options []suggestOption, code, religionCode string) bool {
	for _, option := range options {
		if option.Code == code && option.ReligionCode == religionCode {
			return true
		}
	}
	return false
}
