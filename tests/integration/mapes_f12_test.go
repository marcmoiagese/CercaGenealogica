package integration

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const mapesTestJSON = `{"viewBox":[0,0,1000,700],"layers":{"houses":[],"streets":[],"toponyms":[],"bounds":[]}}`

func createMunicipiAndMap(t *testing.T, database db.DB, userID int) (int, int) {
	t.Helper()

	mun := &db.Municipi{
		Nom:            "Municipi Maps",
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	munID, err := database.CreateMunicipi(mun)
	if err != nil {
		t.Fatalf("CreateMunicipi ha fallat: %v", err)
	}
	mapa := &db.MunicipiMapa{
		MunicipiID: munID,
		GroupType:  "actual",
		Title:      "Mapa test",
		CreatedBy:  sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	mapID, err := database.CreateMunicipiMapa(mapa)
	if err != nil {
		t.Fatalf("CreateMunicipiMapa ha fallat: %v", err)
	}
	return munID, mapID
}

func newMapRequest(method, path, body, sessionID, csrf string) *http.Request {
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	if sessionID != "" {
		req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	}
	if csrf != "" {
		req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrf})
	}
	return req
}

func TestMapesDraftSaveConflict(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f12_mapes_conflict.sqlite3")

	user, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)
	_, mapID := createMunicipiAndMap(t, database, user.ID)

	csrf := "csrf_mapes_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	req := newMapRequest(http.MethodPost, "/api/mapes/"+strconv.Itoa(mapID)+"/draft", `{"csrf_token":"`+csrf+`"}`, sessionID, csrf)
	rr := httptest.NewRecorder()
	app.MapesAPI(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("draft expected 200, got %d", rr.Code)
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode draft response: %v", err)
	}
	versionID := int(resp["version_id"].(float64))

	saveBody := `{"lock_version":0,"changelog":"","data":` + mapesTestJSON + `,"csrf_token":"` + csrf + `" }`
	req = newMapRequest(http.MethodPut, "/api/mapes/versions/"+strconv.Itoa(versionID), saveBody, sessionID, csrf)
	rr = httptest.NewRecorder()
	app.MapesAPI(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("save expected 200, got %d", rr.Code)
	}

	req = newMapRequest(http.MethodPut, "/api/mapes/versions/"+strconv.Itoa(versionID), saveBody, sessionID, csrf)
	rr = httptest.NewRecorder()
	app.MapesAPI(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("conflict expected 409, got %d", rr.Code)
	}
}

func TestMapesSubmitApproveFlow(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f12_mapes_submit.sqlite3")

	user, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)
	_, mapID := createMunicipiAndMap(t, database, user.ID)

	csrf := "csrf_mapes_submit_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	req := newMapRequest(http.MethodPost, "/api/mapes/"+strconv.Itoa(mapID)+"/draft", `{"csrf_token":"`+csrf+`"}`, sessionID, csrf)
	rr := httptest.NewRecorder()
	app.MapesAPI(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("draft expected 200, got %d", rr.Code)
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode draft response: %v", err)
	}
	versionID := int(resp["version_id"].(float64))

	saveBody := `{"lock_version":0,"changelog":"","data":` + mapesTestJSON + `,"csrf_token":"` + csrf + `" }`
	req = newMapRequest(http.MethodPut, "/api/mapes/versions/"+strconv.Itoa(versionID), saveBody, sessionID, csrf)
	rr = httptest.NewRecorder()
	app.MapesAPI(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("save expected 200, got %d", rr.Code)
	}

	req = newMapRequest(http.MethodPost, "/api/mapes/versions/"+strconv.Itoa(versionID)+"/submit", `{"csrf_token":"`+csrf+`"}`, sessionID, csrf)
	rr = httptest.NewRecorder()
	app.MapesAPI(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("submit expected 200, got %d", rr.Code)
	}

	version, err := database.GetMunicipiMapaVersion(versionID)
	if err != nil || version == nil {
		t.Fatalf("GetMunicipiMapaVersion failed: %v", err)
	}
	if version.Status != "pendent" {
		t.Fatalf("expected status pendent, got %s", version.Status)
	}

	form := url.Values{}
	form.Set("csrf_token", csrf)
	formReq := httptest.NewRequest(http.MethodPost, "/admin/moderacio/mapes/"+strconv.Itoa(versionID)+"/approve", strings.NewReader(form.Encode()))
	formReq.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	formReq.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrf})
	formReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr = httptest.NewRecorder()
	app.AdminModeracioMapesApprove(rr, formReq)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("approve expected 303, got %d", rr.Code)
	}

	version, _ = database.GetMunicipiMapaVersion(versionID)
	if version == nil || version.Status != "publicat" {
		t.Fatalf("expected status publicat after approve")
	}
	mapa, _ := database.GetMunicipiMapa(version.MapaID)
	if mapa == nil || !mapa.CurrentVersionID.Valid || int(mapa.CurrentVersionID.Int64) != versionID {
		t.Fatalf("expected current_version_id updated")
	}
}

func TestMapesPermissionDenied(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f12_mapes_perm.sqlite3")

	adminUser, _ := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, adminUser.ID)
	_, mapID := createMunicipiAndMap(t, database, adminUser.ID)

	_, sessionID := createF7UserWithSession(t, database)
	csrf := "csrf_mapes_perm_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	req := newMapRequest(http.MethodPost, "/api/mapes/"+strconv.Itoa(mapID)+"/draft", `{"csrf_token":"`+csrf+`"}`, sessionID, csrf)
	rr := httptest.NewRecorder()
	app.MapesAPI(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("permission expected 403, got %d", rr.Code)
	}
}
