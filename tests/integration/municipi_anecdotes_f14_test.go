package integration

import (
	"encoding/json"
	"fmt"
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

func createAndSubmitAnecdote(t *testing.T, app *core.App, database db.DB, sessionID string, munID int) (int, int, string) {
	t.Helper()

	csrf := "csrf_f14_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	title := "Anecdota de prova"
	form := url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("titol", title)
	form.Set("tag", "cases")
	form.Set("data_ref", "1890")
	form.Set("text", "Text de prova suficientment llarg per validar l'anecdota.")
	form.Set("font_url", "")
	form.Set("action", "save")
	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/territori/municipis/%d/anecdotes/new", munID), strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrf})
	rr := httptest.NewRecorder()

	app.MunicipiAnecdoteNewPage(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("new expected 303, got %d", rr.Code)
	}
	versionID := parseLocationID(t, rr.Result().Header.Get("Location"), "version_id")
	version, err := database.GetMunicipiAnecdotariVersion(versionID)
	if err != nil || version == nil {
		t.Fatalf("GetMunicipiAnecdotariVersion failed: %v", err)
	}

	form = url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("version_id", strconv.Itoa(versionID))
	form.Set("lock_version", strconv.Itoa(version.LockVersion))
	form.Set("titol", title)
	form.Set("tag", "cases")
	form.Set("data_ref", "1890")
	form.Set("text", "Text de prova suficientment llarg per validar l'anecdota.")
	form.Set("font_url", "")
	form.Set("action", "submit")
	req = httptest.NewRequest(http.MethodPost, fmt.Sprintf("/territori/municipis/%d/anecdotes/%d/edit", munID, version.ItemID), strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrf})
	rr = httptest.NewRecorder()

	app.MunicipiAnecdoteEditPage(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("submit expected 303, got %d", rr.Code)
	}
	return versionID, version.ItemID, title
}

func pointsTotal(database db.DB, userID int) int {
	points, err := database.GetUserPoints(userID)
	if err != nil || points == nil {
		return 0
	}
	return points.Total
}

func TestMunicipiAnecdoteFlowAndPoints(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f14_anecdotes_flow.sqlite3")

	user, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)
	if err := database.EnsureDefaultPointsRules(); err != nil {
		t.Fatalf("EnsureDefaultPointsRules failed: %v", err)
	}

	munID := createHistoriaMunicipi(t, database, user.ID)
	basePoints := pointsTotal(database, user.ID)

	versionID, itemID, title := createAndSubmitAnecdote(t, app, database, sessionID, munID)

	version, err := database.GetMunicipiAnecdotariVersion(versionID)
	if err != nil || version == nil {
		t.Fatalf("GetMunicipiAnecdotariVersion failed: %v", err)
	}
	if version.Status != "pendent" {
		t.Fatalf("expected pendent, got %s", version.Status)
	}
	acts, err := database.ListActivityByObject("municipi_anecdota_version", versionID, "pendent")
	if err != nil || len(acts) == 0 {
		t.Fatalf("expected pending activity")
	}
	if pointsTotal(database, user.ID) != basePoints {
		t.Fatalf("points should not change before approval")
	}

	moderateObject(t, app, sessionID, "municipi_anecdota_version", versionID, "aprovar")

	version, err = database.GetMunicipiAnecdotariVersion(versionID)
	if err != nil || version == nil {
		t.Fatalf("GetMunicipiAnecdotariVersion after approve failed: %v", err)
	}
	if version.Status != "publicat" {
		t.Fatalf("expected publicat, got %s", version.Status)
	}
	if pointsTotal(database, user.ID) <= basePoints {
		t.Fatalf("points should increase after approval")
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/municipis/%d/anecdotes?limit=6", munID), nil)
	rr := httptest.NewRecorder()
	app.MunicipiMapesAPI(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("api expected 200, got %d", rr.Code)
	}
	var payload struct {
		Items []struct {
			Title string `json:"title"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	found := false
	for _, item := range payload.Items {
		if item.Title == title {
			found = true
		}
	}
	if !found {
		t.Fatalf("title not found in api list")
	}

	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/territori/municipis/%d/anecdotes", munID), nil)
	rr = httptest.NewRecorder()
	app.MunicipiAnecdotesListPage(rr, req)
	if !strings.Contains(rr.Body.String(), title) {
		t.Fatalf("title not found in list page")
	}

	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/territori/municipis/%d/anecdotes/%d", munID, itemID), nil)
	rr = httptest.NewRecorder()
	app.MunicipiAnecdoteDetailPage(rr, req)
	if !strings.Contains(rr.Body.String(), title) {
		t.Fatalf("title not found in detail page")
	}
}

func TestMunicipiAnecdoteCommentsRequireLogin(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f14_anecdotes_comments.sqlite3")

	user, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)
	if err := database.EnsureDefaultPointsRules(); err != nil {
		t.Fatalf("EnsureDefaultPointsRules failed: %v", err)
	}

	munID := createHistoriaMunicipi(t, database, user.ID)
	versionID, itemID, _ := createAndSubmitAnecdote(t, app, database, sessionID, munID)
	moderateObject(t, app, sessionID, "municipi_anecdota_version", versionID, "aprovar")

	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/anecdotes/%d/comments", itemID), strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	app.AnecdotesAPI(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("expected redirect for anonymous comment, got %d", rr.Code)
	}

	commentUser, commentSession := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, commentUser.ID)
	if err := database.EnsureDefaultPointsRules(); err != nil {
		t.Fatalf("EnsureDefaultPointsRules failed: %v", err)
	}
	beforeActs, _ := database.ListUserActivityByUser(commentUser.ID, db.ActivityFilter{Limit: -1})
	beforePoints := pointsTotal(database, commentUser.ID)

	csrf := "csrf_f14_comment_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	body := fmt.Sprintf(`{"body":"Comentari de prova","csrf_token":"%s"}`, csrf)
	req = httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/anecdotes/%d/comments", itemID), strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: commentSession})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrf})
	rr = httptest.NewRecorder()
	app.AnecdotesAPI(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	comments, total, err := database.ListMunicipiAnecdotariComments(itemID, 10, 0)
	if err != nil || total == 0 || len(comments) == 0 {
		t.Fatalf("expected comment in db")
	}
	afterActs, _ := database.ListUserActivityByUser(commentUser.ID, db.ActivityFilter{Limit: -1})
	if len(afterActs) != len(beforeActs) {
		t.Fatalf("comment should not create activity")
	}
	if pointsTotal(database, commentUser.ID) != beforePoints {
		t.Fatalf("comment should not grant points")
	}
}
