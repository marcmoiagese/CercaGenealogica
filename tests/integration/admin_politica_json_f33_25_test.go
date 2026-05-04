package integration

import (
	"encoding/json"
	"html"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF3325RAdminNewPoliticaShowsInitialJSON(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f33_25r_new_politica_json_tab.sqlite3")
	_ = createTestUser(t, database, "f33_25r_seed_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	_, adminCookie := createF335PlatformUser(t, database, "f33_25r_admin_new_json", "admin.policies.manage")

	req := httptest.NewRequest(http.MethodGet, "/admin/politiques/new?tab=json", nil)
	req.AddCookie(adminCookie)
	rr := httptest.NewRecorder()
	app.AdminNewPolitica(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET new JSON status=%d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, `id="tab-json"`) || !strings.Contains(body, `/admin/politiques/json/apply`) {
		t.Fatalf("alta nova no renderitza pestanya JSON: %s", body)
	}
	raw := f3325TextareaValue(t, body, `id="policy-json-editor"`)
	var payload struct {
		Version int `json:"version"`
		Grants  []struct {
			PermKey string `json:"perm_key"`
		} `json:"grants"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("JSON inicial invalid: %v raw=%s", err, raw)
	}
	if payload.Version != 1 || payload.Grants == nil || len(payload.Grants) != 0 {
		t.Fatalf("JSON inicial inesperat: %#v", payload)
	}
}

func TestF3325RAdminCreatePoliticaFromJSON(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f33_25r_create_politica_json.sqlite3")
	_ = createTestUser(t, database, "f33_25r_create_seed_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	_, adminCookie := createF335PlatformUser(t, database, "f33_25r_create_admin_json", "admin.policies.manage")

	form := url.Values{}
	form.Set("policy_json", `{"version":1,"policy":{"name":"f33_25r_created_from_json","description":"created from JSON"},"grants":[{"perm_key":"admin.audit.view","scope_type":"global","scope_id":null,"include_children":false}]}`)
	rr := f3325Post(app.AdminApplyPoliticaJSON, "/admin/politiques/json/apply", adminCookie, form, "csrf_f33_25r_create")
	if rr.Code != http.StatusSeeOther || !strings.Contains(rr.Header().Get("Location"), "tab=json") {
		t.Fatalf("crear politica des de JSON redirect inesperat status=%d location=%s body=%s", rr.Code, rr.Header().Get("Location"), rr.Body.String())
	}
	policyID := f3325PolicyIDByName(t, database, "f33_25r_created_from_json")
	rows, err := database.ListPoliticaGrants(policyID)
	if err != nil {
		t.Fatalf("no s'han pogut llistar grants de politica nova: %v", err)
	}
	if len(rows) != 1 || rows[0].PermKey != "admin.audit.view" || rows[0].ScopeType != "global" {
		t.Fatalf("crear des de JSON no ha inserit grants esperats: %#v", rows)
	}
}

func TestF3325AdminPoliticaJSONTabAndApply(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f33_25_admin_politica_json.sqlite3")
	_ = createTestUser(t, database, "f33_25_seed_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	target := createTestUser(t, database, "f33_25_target_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	_, adminCookie := createF335PlatformUser(t, database, "f33_25_admin_json", "admin.policies.manage")
	policyID := createF3325Policy(t, database, "f33_25_json_policy")
	if err := database.AddUserPolitica(target.ID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica F33-25: %v", err)
	}
	if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
		PoliticaID: policyID,
		PermKey:    "admin.audit.view",
		ScopeType:  "global",
	}); err != nil {
		t.Fatalf("no s'ha pogut crear grant inicial F33-25: %v", err)
	}
	if !app.HasPermission(target.ID, "admin.audit.view", core.PermissionTarget{}) {
		t.Fatalf("grant inicial hauria d'autoritzar admin.audit.view")
	}

	req := httptest.NewRequest(http.MethodGet, "/admin/politiques/"+strconv.Itoa(policyID)+"/edit?tab=json", nil)
	req.AddCookie(adminCookie)
	rr := httptest.NewRecorder()
	app.AdminEditPolitica(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET edit JSON status=%d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, `id="tab-json"`) || !strings.Contains(body, `/admin/politiques/json/apply`) {
		t.Fatalf("la pestanya JSON no s'ha renderitzat: %s", body)
	}
	if strings.Contains(body, "Policy"+"Permissions") || strings.Contains(body, "politiques."+"permisos") {
		t.Fatalf("la UI JSON no ha de renderitzar residus legacy")
	}

	form := url.Values{}
	form.Set("politica_id", strconv.Itoa(policyID))
	form.Set("policy_json", `{"version":1,"policy":{"name":"f33_25_json_policy","description":""},"grants":[{"perm_key":"admin.jobs.manage","scope_type":"global","scope_id":null,"include_children":false}]}`)
	rr = f3325Post(app.AdminApplyPoliticaJSON, "/admin/politiques/json/apply", adminCookie, form, "")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("POST JSON sense CSRF hauria de ser 400, got=%d", rr.Code)
	}
	rr = f3325Post(app.AdminApplyPoliticaJSON, "/admin/politiques/json/apply", adminCookie, form, "csrf_f33_25")
	if rr.Code != http.StatusSeeOther || !strings.Contains(rr.Header().Get("Location"), "tab=json") {
		t.Fatalf("POST JSON valid redirect inesperat status=%d location=%s body=%s", rr.Code, rr.Header().Get("Location"), rr.Body.String())
	}
	if app.HasPermission(target.ID, "admin.audit.view", core.PermissionTarget{}) {
		t.Fatalf("aplicar JSON hauria de retirar admin.audit.view")
	}
	if !app.HasPermission(target.ID, "admin.jobs.manage", core.PermissionTarget{}) {
		t.Fatalf("aplicar JSON hauria d'afegir admin.jobs.manage")
	}
	rows, err := database.ListPoliticaGrants(policyID)
	if err != nil {
		t.Fatalf("no s'han pogut llistar grants F33-25: %v", err)
	}
	if len(rows) != 1 || rows[0].PermKey != "admin.jobs.manage" {
		t.Fatalf("JSON no ha substituit politica_grants: %#v", rows)
	}

	plain := createTestUser(t, database, "f33_25_plain_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	plainCookie := createSessionCookie(t, database, plain.ID, "sess_f33_25_plain_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	rr = f3325Post(app.AdminApplyPoliticaJSON, "/admin/politiques/json/apply", plainCookie, form, "csrf_f33_25_plain")
	if rr.Code != http.StatusForbidden {
		t.Fatalf("usuari sense admin.policies.manage hauria de rebre 403, got=%d", rr.Code)
	}
}

func f3325TextareaValue(t *testing.T, body, marker string) string {
	t.Helper()
	idx := strings.Index(body, marker)
	if idx < 0 {
		t.Fatalf("no s'ha trobat textarea %s", marker)
	}
	afterStart := body[idx:]
	openEnd := strings.Index(afterStart, ">")
	closeIdx := strings.Index(afterStart, "</textarea>")
	if openEnd < 0 || closeIdx < 0 || closeIdx <= openEnd {
		t.Fatalf("textarea mal format")
	}
	return html.UnescapeString(strings.TrimSpace(afterStart[openEnd+1 : closeIdx]))
}

func f3325PolicyIDByName(t *testing.T, database db.DB, name string) int {
	t.Helper()
	policies, err := database.ListPolitiques()
	if err != nil {
		t.Fatalf("no s'han pogut llistar politiques: %v", err)
	}
	for _, p := range policies {
		if p.Nom == name {
			return p.ID
		}
	}
	t.Fatalf("politica %q no trobada", name)
	return 0
}

func createF3325Policy(t *testing.T, database db.DB, name string) int {
	t.Helper()
	id, err := database.SavePolitica(&db.Politica{Nom: name, Descripcio: ""})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica F33-25: %v", err)
	}
	return id
}

func f3325Post(handler http.HandlerFunc, path string, session *http.Cookie, form url.Values, csrf string) *httptest.ResponseRecorder {
	if csrf != "" {
		form.Set("csrf_token", csrf)
	}
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if session != nil {
		req.AddCookie(session)
	}
	if csrf != "" {
		req.AddCookie(csrfCookie(csrf))
	}
	rr := httptest.NewRecorder()
	handler(rr, req)
	return rr
}
