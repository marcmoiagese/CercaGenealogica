package integration

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMunicipiHistoriaPermissions(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f13_historia_perm.sqlite3")

	creator, creatorSession := createF7UserWithSession(t, database)
	other, otherSession := createF7UserWithSession(t, database)
	_, noPermSession := createF7UserWithSession(t, database)

	createPolicyID := createPolicyWithGrant(t, database, "historia_create", "municipis.historia.create")
	assignPolicyToUser(t, database, creator.ID, createPolicyID)
	assignPolicyToUser(t, database, other.ID, createPolicyID)

	munID := createHistoriaMunicipi(t, database, creator.ID)

	csrf := "csrf_historia_perm_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	form := url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("titol", "Historia general")
	form.Set("resum", "Resum curt")
	form.Set("cos_text", "Text prova")
	form.Set("tags_json", `["perm"]`)

	req := httptest.NewRequest(http.MethodPost, "/territori/municipis/"+strconv.Itoa(munID)+"/historia/general/save", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrf})
	rr := httptest.NewRecorder()

	app.MunicipiHistoriaGeneralSave(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("not logged in expected 303, got %d", rr.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/territori/municipis/"+strconv.Itoa(munID)+"/historia/general/save", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: creatorSession})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrf})
	rr = httptest.NewRecorder()

	app.MunicipiHistoriaGeneralSave(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("creator save expected 303, got %d", rr.Code)
	}
	versionID := parseLocationID(t, rr.Result().Header.Get("Location"), "general_version_id")

	version, err := database.GetMunicipiHistoriaGeneralVersion(versionID)
	if err != nil || version == nil {
		t.Fatalf("GetMunicipiHistoriaGeneralVersion failed: %v", err)
	}

	form = url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("version_id", strconv.Itoa(versionID))
	form.Set("lock_version", strconv.Itoa(version.LockVersion))
	form.Set("titol", "Historia general edit")
	form.Set("resum", "Resum curt")
	form.Set("cos_text", "Text prova")
	form.Set("tags_json", `["perm"]`)

	req = httptest.NewRequest(http.MethodPost, "/territori/municipis/"+strconv.Itoa(munID)+"/historia/general/save", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: otherSession})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrf})
	rr = httptest.NewRecorder()

	app.MunicipiHistoriaGeneralSave(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("edit other draft expected 403, got %d", rr.Code)
	}

	form = url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("version_id", strconv.Itoa(versionID))
	req = httptest.NewRequest(http.MethodPost, "/territori/municipis/"+strconv.Itoa(munID)+"/historia/general/submit", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: creatorSession})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrf})
	rr = httptest.NewRecorder()

	app.MunicipiHistoriaGeneralSubmit(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("submit without perm expected 403, got %d", rr.Code)
	}

	form = url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("version_id", strconv.Itoa(versionID))
	req = httptest.NewRequest(http.MethodPost, "/territori/municipis/"+strconv.Itoa(munID)+"/historia/general/submit", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: noPermSession})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrf})
	rr = httptest.NewRecorder()

	app.MunicipiHistoriaGeneralSubmit(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("submit without perm expected 403, got %d", rr.Code)
	}

}
