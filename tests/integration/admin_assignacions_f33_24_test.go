package integration

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

const (
	f3324AdminPoliciesManage = "admin.policies.manage"
	f3324AdminAuditView      = "admin.audit.view"
)

func TestF3324AdminAssignacionsGroupFlowAndSnapshots(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f33_24_admin_assignacions_flow.sqlite3")
	_ = createTestUser(t, database, "f33_24_seed_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	targetUser := createTestUser(t, database, "f33_24_target_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	_, adminCookie := createF335PlatformUser(t, database, "f33_24_admin", f3324AdminPoliciesManage)
	policyID := createF3324Policy(t, database, "f33_24_group_policy", f3324AdminAuditView)
	csrf := "csrf_f33_24_flow"

	if app.HasPermission(targetUser.ID, f3324AdminAuditView, core.PermissionTarget{}) {
		t.Fatalf("el test necessita un usuari inicial sense admin.audit.view")
	}

	createForm := url.Values{}
	createForm.Set("name", "f33-24-permission-group")
	rr := f3324Post(app.AdminCreateGroup, "/admin/politiques/grups/create", adminCookie, createForm, csrf)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("create group status=%d body=%s", rr.Code, rr.Body.String())
	}
	groupID := f3324LocationID(t, rr, "group_id")
	if groupID <= 0 {
		t.Fatalf("create group no ha retornat group_id valid")
	}

	assignUser := url.Values{}
	assignUser.Set("user_id", strconv.Itoa(targetUser.ID))
	assignUser.Set("group_id", strconv.Itoa(groupID))
	assignUser.Set("return_to", "/admin/politiques/assignacions")
	rr = f3324Post(app.AdminAssignarUsuariGrup, "/admin/politiques/grups/assignar-usuari", adminCookie, assignUser, csrf)
	if rr.Code != http.StatusSeeOther || !strings.Contains(rr.Header().Get("Location"), "ok=user_group_added") {
		t.Fatalf("assign user group redirect inesperat status=%d location=%s", rr.Code, rr.Header().Get("Location"))
	}

	assignPolicy := url.Values{}
	assignPolicy.Set("group_id", strconv.Itoa(groupID))
	assignPolicy.Set("politica_id", strconv.Itoa(policyID))
	assignPolicy.Set("return_to", "/admin/politiques/assignacions")
	rr = f3324Post(app.AdminAssignarPoliticaGrup, "/admin/politiques/assignar-grup", adminCookie, assignPolicy, csrf)
	if rr.Code != http.StatusSeeOther || !strings.Contains(rr.Header().Get("Location"), "ok=group_policy_added") {
		t.Fatalf("assign group policy redirect inesperat status=%d location=%s", rr.Code, rr.Header().Get("Location"))
	}
	if !app.HasPermission(targetUser.ID, f3324AdminAuditView, core.PermissionTarget{}) {
		t.Fatalf("la politica del grup hauria d'autoritzar l'usuari")
	}

	rr = f3324Post(app.AdminTreurePoliticaGrup, "/admin/politiques/treure-grup", adminCookie, assignPolicy, csrf)
	if rr.Code != http.StatusSeeOther || !strings.Contains(rr.Header().Get("Location"), "ok=group_policy_removed") {
		t.Fatalf("remove group policy redirect inesperat status=%d location=%s", rr.Code, rr.Header().Get("Location"))
	}
	if app.HasPermission(targetUser.ID, f3324AdminAuditView, core.PermissionTarget{}) {
		t.Fatalf("retirar politica del grup hauria d'invalidar el snapshot")
	}

	rr = f3324Post(app.AdminAssignarPoliticaGrup, "/admin/politiques/assignar-grup", adminCookie, assignPolicy, csrf)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("reassign group policy status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !app.HasPermission(targetUser.ID, f3324AdminAuditView, core.PermissionTarget{}) {
		t.Fatalf("reassignar politica del grup hauria de reconstruir permisos")
	}

	rr = f3324Post(app.AdminTreureUsuariGrup, "/admin/politiques/grups/treure-usuari", adminCookie, assignUser, csrf)
	if rr.Code != http.StatusSeeOther || !strings.Contains(rr.Header().Get("Location"), "ok=user_group_removed") {
		t.Fatalf("remove user group redirect inesperat status=%d location=%s", rr.Code, rr.Header().Get("Location"))
	}
	if app.HasPermission(targetUser.ID, f3324AdminAuditView, core.PermissionTarget{}) {
		t.Fatalf("retirar l'usuari del grup hauria d'invalidar el snapshot")
	}
}

func TestF3324AdminAssignacionsSecurityCSRFAndVisibleErrors(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f33_24_admin_assignacions_security.sqlite3")
	_ = createTestUser(t, database, "f33_24_seed_security_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	plainUser := createTestUser(t, database, "f33_24_plain_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	plainCookie := createSessionCookie(t, database, plainUser.ID, "sess_f33_24_plain_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	_, adminCookie := createF335PlatformUser(t, database, "f33_24_admin_security", f3324AdminPoliciesManage)

	req := httptest.NewRequest(http.MethodGet, "/admin/politiques/assignacions", nil)
	req.AddCookie(plainCookie)
	rr := httptest.NewRecorder()
	app.AdminAssignacionsPolitiques(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("usuari sense admin.policies.manage hauria de rebre 403, got=%d", rr.Code)
	}

	form := url.Values{}
	form.Set("name", "f33-24-missing-csrf")
	rr = f3324Post(app.AdminCreateGroup, "/admin/politiques/grups/create", adminCookie, form, "")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("POST sense CSRF hauria de rebre 400, got=%d", rr.Code)
	}

	if _, err := database.CreateGroup("f33-24-duplicate", ""); err != nil {
		t.Fatalf("no s'ha pogut preparar grup duplicat F33-24: %v", err)
	}
	duplicate := url.Values{}
	duplicate.Set("name", "f33-24-duplicate")
	rr = f3324Post(app.AdminCreateGroup, "/admin/politiques/grups/create", adminCookie, duplicate, "csrf_f33_24_dup")
	if rr.Code != http.StatusSeeOther || !strings.Contains(rr.Header().Get("Location"), "err=create_group_failed") {
		t.Fatalf("duplicat hauria de redirigir amb err visible, status=%d location=%s", rr.Code, rr.Header().Get("Location"))
	}

	req = httptest.NewRequest(http.MethodGet, "/admin/politiques/assignacions?err=create_group_failed", nil)
	req.AddCookie(adminCookie)
	rr = httptest.NewRecorder()
	app.AdminAssignacionsPolitiques(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET assignacions admin status=%d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, "alert-error") || !strings.Contains(body, "No s&#39;ha pogut crear el grup de permisos.") {
		t.Fatalf("l'error visible no s'ha renderitzat al template: %s", body)
	}
	f3324AssertPostFormsHaveCSRF(t, body)
}

func f3324Post(handler http.HandlerFunc, path string, session *http.Cookie, form url.Values, csrf string) *httptest.ResponseRecorder {
	if form == nil {
		form = url.Values{}
	}
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

func createF3324Policy(t *testing.T, database db.DB, name, permKey string) int {
	t.Helper()
	policyID, err := database.SavePolitica(&db.Politica{Nom: name, Descripcio: ""})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica F33-24: %v", err)
	}
	if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
		PoliticaID:      policyID,
		PermKey:         permKey,
		ScopeType:       "global",
		ScopeID:         sql.NullInt64{},
		IncludeChildren: false,
	}); err != nil {
		t.Fatalf("no s'ha pogut crear grant F33-24: %v", err)
	}
	return policyID
}

func f3324LocationID(t *testing.T, rr *httptest.ResponseRecorder, key string) int {
	t.Helper()
	loc := rr.Header().Get("Location")
	u, err := url.Parse(loc)
	if err != nil {
		t.Fatalf("Location invalid %q: %v", loc, err)
	}
	id, err := strconv.Atoi(u.Query().Get(key))
	if err != nil {
		t.Fatalf("Location no conte %s numeric: %q", key, loc)
	}
	return id
}

func f3324AssertPostFormsHaveCSRF(t *testing.T, body string) {
	t.Helper()
	re := regexp.MustCompile(`(?is)<form[^>]*method="post"[^>]*>.*?</form>`)
	forms := re.FindAllString(body, -1)
	if len(forms) == 0 {
		t.Fatalf("no s'ha trobat cap form POST a assignacions")
	}
	for _, form := range forms {
		if !strings.Contains(form, `name="csrf_token"`) {
			t.Fatalf("form POST sense csrf_token: %s", form)
		}
	}
}
