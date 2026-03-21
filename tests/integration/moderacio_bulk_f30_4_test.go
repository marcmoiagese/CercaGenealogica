package integration

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
)

func TestModeracioBulkPageMixedSelectionScoped(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_4_bulk_page_mixed.sqlite3")

	user := createNonAdminTestUser(t, database, "moderator_scope_page")
	munAllowed := createHistoriaMunicipi(t, database, user.ID)
	munOther := createHistoriaMunicipi(t, database, user.ID)

	versionAllowed := createPendingHistoriaGeneralVersion(t, database, user.ID, munAllowed)
	versionOther := createPendingHistoriaGeneralVersion(t, database, user.ID, munOther)

	policyID := createPolicyWithScopedGrant(t, database, "historia_scope_page", "municipis.historia.moderate", string(core.ScopeMunicipi), munAllowed)
	addGrantToPolicy(t, database, policyID, "moderacio.bulk")
	assignPolicyToUser(t, database, user.ID, policyID)

	session := createSessionCookie(t, database, user.ID, "sess_scope_page")
	csrf := "csrf_scope_page"

	form := url.Values{}
	form.Set("bulk_action", "approve")
	form.Set("bulk_scope", "page")
	form.Set("bulk_type", "municipi_historia_general")
	form.Set("csrf_token", csrf)
	form.Set("return_to", "/moderacio")
	form.Add("selected", fmt.Sprintf("municipi_historia_general:%d", versionAllowed))
	form.Add("selected", fmt.Sprintf("municipi_historia_general:%d", versionOther))

	req := httptest.NewRequest(http.MethodPost, "/moderacio/bulk", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()
	app.AdminModeracioBulk(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("bulk scope page esperava 303, got %d", rr.Code)
	}
	location := rr.Result().Header.Get("Location")
	if !strings.Contains(location, "err=1") {
		t.Fatalf("bulk scope page esperava err=1, got %s", location)
	}

	allowed, err := database.GetMunicipiHistoriaGeneralVersion(versionAllowed)
	if err != nil || allowed == nil {
		t.Fatalf("GetMunicipiHistoriaGeneralVersion allowed ha fallat: %v", err)
	}
	other, err := database.GetMunicipiHistoriaGeneralVersion(versionOther)
	if err != nil || other == nil {
		t.Fatalf("GetMunicipiHistoriaGeneralVersion other ha fallat: %v", err)
	}
	if allowed.Status != "publicat" {
		t.Fatalf("version allowed esperat publicat, got %s", allowed.Status)
	}
	if other.Status != "pendent" {
		t.Fatalf("version other esperat pendent, got %s", other.Status)
	}
}
