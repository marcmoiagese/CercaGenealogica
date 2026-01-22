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

func TestMunicipiHistoriaSubmitApproveFlow(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f13_historia_submit.sqlite3")

	user, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)

	munID := createHistoriaMunicipi(t, database, user.ID)

	csrf := "csrf_historia_submit_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	form := url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("titol", "Historia general")
	form.Set("resum", "Resum curt")
	form.Set("cos_text", "Text llarg de prova")
	form.Set("tags_json", `["origen","segles"]`)

	req := httptest.NewRequest(http.MethodPost, "/territori/municipis/"+strconv.Itoa(munID)+"/historia/general/save", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrf})
	rr := httptest.NewRecorder()

	app.MunicipiHistoriaGeneralSave(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("save expected 303, got %d", rr.Code)
	}
	versionID := parseLocationID(t, rr.Result().Header.Get("Location"), "general_version_id")

	form = url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("version_id", strconv.Itoa(versionID))
	req = httptest.NewRequest(http.MethodPost, "/territori/municipis/"+strconv.Itoa(munID)+"/historia/general/submit", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrf})
	rr = httptest.NewRecorder()

	app.MunicipiHistoriaGeneralSubmit(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("submit expected 303, got %d", rr.Code)
	}

	version, err := database.GetMunicipiHistoriaGeneralVersion(versionID)
	if err != nil || version == nil {
		t.Fatalf("GetMunicipiHistoriaGeneralVersion failed: %v", err)
	}
	if version.Status != "pendent" {
		t.Fatalf("expected status pendent, got %s", version.Status)
	}

	form = url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("object_type", "municipi_historia_general")
	req = httptest.NewRequest(http.MethodPost, "/moderacio/"+strconv.Itoa(versionID)+"/aprovar", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrf})
	rr = httptest.NewRecorder()

	app.AdminModeracioAprovar(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("approve expected 303, got %d", rr.Code)
	}

	version, err = database.GetMunicipiHistoriaGeneralVersion(versionID)
	if err != nil || version == nil {
		t.Fatalf("GetMunicipiHistoriaGeneralVersion after approve failed: %v", err)
	}
	if version.Status != "publicat" {
		t.Fatalf("expected status publicat, got %s", version.Status)
	}

	historia, err := database.GetMunicipiHistoriaByMunicipiID(munID)
	if err != nil || historia == nil {
		t.Fatalf("GetMunicipiHistoriaByMunicipiID failed: %v", err)
	}
	if !historia.CurrentGeneralVersionID.Valid || int(historia.CurrentGeneralVersionID.Int64) != versionID {
		t.Fatalf("current_general_version_id not updated")
	}

	acts, err := database.ListActivityByObject("municipi_historia_general", versionID, "validat")
	if err != nil {
		t.Fatalf("ListActivityByObject failed: %v", err)
	}
	foundSubmit := false
	for _, act := range acts {
		if act.Action == "municipi_historia_general_submit" {
			foundSubmit = true
			break
		}
	}
	if !foundSubmit {
		t.Fatalf("submit activity not validated")
	}

	points, err := database.GetUserPoints(user.ID)
	if err != nil || points == nil || points.Total <= 0 {
		t.Fatalf("expected user points after approval")
	}
}
