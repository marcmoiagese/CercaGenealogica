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

func TestMunicipiHistoriaFetFlow(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f13_historia_fet.sqlite3")

	user, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)

	munID := createHistoriaMunicipi(t, database, user.ID)

	csrf := "csrf_historia_fet_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	form := url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("data_display", "Segle XII")
	form.Set("any_inici", "1150")
	form.Set("any_fi", "1160")
	form.Set("titol", "Fundacio")
	form.Set("resum", "Resum fet")
	form.Set("cos_text", "Cos fet de prova")
	form.Set("tags_json", `["fundacio","poble"]`)
	form.Set("fonts_json", "Font test|https://example.com")

	req := httptest.NewRequest(http.MethodPost, "/territori/municipis/"+strconv.Itoa(munID)+"/historia/fets/new", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrf})
	rr := httptest.NewRecorder()

	app.MunicipiHistoriaFetNew(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("new expected 303, got %d", rr.Code)
	}
	versionID := parseLocationID(t, rr.Result().Header.Get("Location"), "fet_version_id")

	version, err := database.GetMunicipiHistoriaFetVersion(versionID)
	if err != nil || version == nil {
		t.Fatalf("GetMunicipiHistoriaFetVersion failed: %v", err)
	}
	fetID := version.FetID

	form = url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("version_id", strconv.Itoa(versionID))
	req = httptest.NewRequest(http.MethodPost, "/territori/municipis/"+strconv.Itoa(munID)+"/historia/fets/"+strconv.Itoa(fetID)+"/submit", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrf})
	rr = httptest.NewRecorder()

	app.MunicipiHistoriaFetSubmit(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("submit expected 303, got %d", rr.Code)
	}

	version, err = database.GetMunicipiHistoriaFetVersion(versionID)
	if err != nil || version == nil {
		t.Fatalf("GetMunicipiHistoriaFetVersion after submit failed: %v", err)
	}
	if version.Status != "pendent" {
		t.Fatalf("expected status pendent, got %s", version.Status)
	}

	form = url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("object_type", "municipi_historia_fet")
	req = httptest.NewRequest(http.MethodPost, "/moderacio/"+strconv.Itoa(versionID)+"/aprovar", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrf})
	rr = httptest.NewRecorder()

	app.AdminModeracioAprovar(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("approve expected 303, got %d", rr.Code)
	}

	version, err = database.GetMunicipiHistoriaFetVersion(versionID)
	if err != nil || version == nil {
		t.Fatalf("GetMunicipiHistoriaFetVersion after approve failed: %v", err)
	}
	if version.Status != "publicat" {
		t.Fatalf("expected status publicat, got %s", version.Status)
	}

	fetRow, err := database.GetMunicipiHistoriaFet(fetID)
	if err != nil || fetRow == nil {
		t.Fatalf("GetMunicipiHistoriaFet failed: %v", err)
	}
	if !fetRow.CurrentVersionID.Valid || int(fetRow.CurrentVersionID.Int64) != versionID {
		t.Fatalf("current_version_id not updated")
	}

	_, timeline, err := database.GetMunicipiHistoriaSummary(munID)
	if err != nil {
		t.Fatalf("GetMunicipiHistoriaSummary failed: %v", err)
	}
	found := false
	for _, item := range timeline {
		if item.ID == versionID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("approved fact not present in summary timeline")
	}
}
