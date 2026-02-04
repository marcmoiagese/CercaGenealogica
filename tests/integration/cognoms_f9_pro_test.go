package integration

import (
	"database/sql"
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

func TestCognomRedirect(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f9_cognom_redirect.sqlite3")

	user, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)

	canonicalID, err := database.UpsertCognom("Garcia", "garcia", "", "", &user.ID)
	if err != nil {
		t.Fatalf("UpsertCognom canonic ha fallat: %v", err)
	}
	aliasID, err := database.UpsertCognom("Garsia", "garsia", "", "", &user.ID)
	if err != nil {
		t.Fatalf("UpsertCognom alias ha fallat: %v", err)
	}
	if err := database.SetCognomRedirect(aliasID, canonicalID, &user.ID, "merge"); err != nil {
		t.Fatalf("SetCognomRedirect ha fallat: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/cognoms/"+strconv.Itoa(aliasID), nil)
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	rr := httptest.NewRecorder()

	app.CognomDetall(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("redirect expected 303, got %d", rr.Code)
	}
	loc := rr.Result().Header.Get("Location")
	if !strings.Contains(loc, "/cognoms/"+strconv.Itoa(canonicalID)) {
		t.Fatalf("redirect location inesperada: %s", loc)
	}
}

func TestCognomReferencesModeration(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f9_cognom_references.sqlite3")

	user, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)

	cognomID, err := database.UpsertCognom("Serra", "serra", "", "", &user.ID)
	if err != nil {
		t.Fatalf("UpsertCognom ha fallat: %v", err)
	}

	csrf := "csrf_ref_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	form := url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("kind", "url")
	form.Set("url", "https://example.com/ref")
	form.Set("titol", "Referència prova")
	req := httptest.NewRequest(http.MethodPost, "/cognoms/"+strconv.Itoa(cognomID)+"/referencies/submit", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()

	app.CognomSubmitReferencia(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("submit referencia expected 303, got %d", rr.Code)
	}

	pendents, err := database.ListCognomReferencies(db.CognomReferenciaFilter{CognomID: cognomID, Status: "pendent"})
	if err != nil || len(pendents) == 0 {
		t.Fatalf("esperava referència pendent, err=%v", err)
	}
	refID := pendents[0].ID

	approveWikiChange(t, app, &http.Cookie{Name: "cg_session", Value: sessionID}, refID, "cognom_referencia")

	publicats, err := database.ListCognomReferencies(db.CognomReferenciaFilter{CognomID: cognomID, Status: "publicat"})
	if err != nil || len(publicats) == 0 {
		t.Fatalf("esperava referència publicada, err=%v", err)
	}

	req = httptest.NewRequest(http.MethodGet, "/cognoms/"+strconv.Itoa(cognomID), nil)
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	rr = httptest.NewRecorder()

	app.CognomDetall(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("detall cognom expected 200, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "Referència prova") {
		t.Fatalf("la referència no apareix a la fitxa del cognom")
	}
}

func TestCognomWikiCompareRevert(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f9_cognom_wiki_compare.sqlite3")

	author := createTestUser(t, database, "wiki_cognom_author_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	assignPolicyByName(t, database, author.ID, "confiança")
	authorSession := createSessionCookie(t, database, author.ID, "sess_cognom_author_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	moderator := createTestUser(t, database, "wiki_cognom_mod_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	assignPolicyByName(t, database, moderator.ID, "admin")
	moderatorSession := createSessionCookie(t, database, moderator.ID, "sess_cognom_mod_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	cognomID, err := database.UpsertCognom("Roca", "roca", "", "", &author.ID)
	if err != nil {
		t.Fatalf("UpsertCognom ha fallat: %v", err)
	}

	changeID1 := submitCognomHistoria(t, app, database, authorSession, cognomID, "Historia 1")
	if changeID1 == 0 {
		t.Fatalf("no s'ha creat el primer canvi")
	}
	approveWikiChange(t, app, moderatorSession, changeID1, "cognom_canvi")

	changeID2 := submitCognomHistoria(t, app, database, authorSession, cognomID, "Historia 2")
	if changeID2 == 0 {
		t.Fatalf("no s'ha creat el segon canvi")
	}
	approveWikiChange(t, app, moderatorSession, changeID2, "cognom_canvi")

	req := httptest.NewRequest(http.MethodGet, "/cognoms/"+strconv.Itoa(cognomID)+"/compare?compare="+strconv.Itoa(changeID1)+","+strconv.Itoa(changeID2), nil)
	req.AddCookie(authorSession)
	rr := httptest.NewRecorder()

	app.CognomWikiHistory(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("compare expected 200, got %d", rr.Code)
	}

	revertID := submitCognomRevert(t, app, authorSession, cognomID, changeID1)
	if revertID == 0 {
		t.Fatalf("no s'ha creat la proposta de revert")
	}
	approveWikiChange(t, app, moderatorSession, revertID, "cognom_canvi")

	cognom, _ := database.GetCognom(cognomID)
	if cognom == nil || cognom.Origen != "Historia 1" {
		t.Fatalf("esperava cognom revertit a Historia 1, got %q", cognom.Origen)
	}
}

func TestCognomStatsAPI(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f9_cognom_stats.sqlite3")

	user, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)

	llibreID := createF9LlibreWithMunicipi(t, database, user.ID)
	registre := &db.TranscripcioRaw{
		LlibreID:       llibreID,
		TipusActe:      "baptisme",
		AnyDoc:         sql.NullInt64{Int64: 1890, Valid: true},
		DataActeEstat:  "clar",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
	}
	registreID, err := database.CreateTranscripcioRaw(registre)
	if err != nil {
		t.Fatalf("CreateTranscripcioRaw ha fallat: %v", err)
	}
	_, _ = database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
		TranscripcioID: registreID,
		Rol:            "batejat",
		Nom:            "Josep",
		Cognom1:        "Garcia",
		Cognom1Estat:   "clar",
	})

	csrfToken := "csrf_f9_stats_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	req := httptest.NewRequest(http.MethodPost, "/admin/cognoms/stats/run", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrfToken})
	rr := httptest.NewRecorder()

	app.AdminCognomsStatsRun(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("stats run expected 303, got %d", rr.Code)
	}

	cognoms, err := database.ListCognoms("", 0, 0)
	if err != nil {
		t.Fatalf("ListCognoms ha fallat: %v", err)
	}
	cognomID := 0
	for _, c := range cognoms {
		if strings.EqualFold(c.Forma, "Garcia") || c.Key == "garcia" {
			cognomID = c.ID
			break
		}
	}
	if cognomID == 0 {
		t.Fatalf("no he trobat cognom importat")
	}

	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/cognoms/%d/stats/total", cognomID), nil)
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	rr = httptest.NewRecorder()

	app.CognomHeatmapJSON(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("stats total expected 200, got %d", rr.Code)
	}
	var totalPayload struct {
		TotalPersones   int `json:"total_persones"`
		TotalAparicions int `json:"total_aparicions"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&totalPayload); err != nil {
		t.Fatalf("no puc parsejar JSON total: %v", err)
	}
	if totalPayload.TotalAparicions != 1 || totalPayload.TotalPersones != 1 {
		t.Fatalf("totals inesperats: %+v", totalPayload)
	}

	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/cognoms/%d/stats/series?bucket=year", cognomID), nil)
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	rr = httptest.NewRecorder()

	app.CognomHeatmapJSON(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("stats series expected 200, got %d", rr.Code)
	}
	var seriesPayload struct {
		Items []struct {
			X int `json:"x"`
			Y int `json:"y"`
		} `json:"items"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&seriesPayload); err != nil {
		t.Fatalf("no puc parsejar JSON series: %v", err)
	}
	if len(seriesPayload.Items) == 0 || seriesPayload.Items[0].Y == 0 {
		t.Fatalf("series sense dades: %+v", seriesPayload.Items)
	}
}

func submitCognomHistoria(t *testing.T, app *core.App, database db.DB, session *http.Cookie, cognomID int, historia string) int {
	t.Helper()
	csrf := "csrf_historia_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	form := url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("historia", historia)
	req := httptest.NewRequest(http.MethodPost, "/cognoms/"+strconv.Itoa(cognomID)+"/historia/submit", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()

	app.CognomSubmitHistoria(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("submit historia expected 303, got %d", rr.Code)
	}
	changes, err := database.ListWikiChanges("cognom", cognomID)
	if err != nil || len(changes) == 0 {
		t.Fatalf("ListWikiChanges ha fallat: %v", err)
	}
	return changes[0].ID
}

func submitCognomRevert(t *testing.T, app *core.App, session *http.Cookie, cognomID, changeID int) int {
	t.Helper()
	csrf := "csrf_revert_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	form := url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("change_id", strconv.Itoa(changeID))
	form.Set("reason", "revert")
	req := httptest.NewRequest(http.MethodPost, "/cognoms/"+strconv.Itoa(cognomID)+"/historial/revert", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()

	app.CognomWikiRevert(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("revert expected 303, got %d", rr.Code)
	}

	changes, err := app.DB.ListWikiChanges("cognom", cognomID)
	if err != nil {
		t.Fatalf("ListWikiChanges ha fallat: %v", err)
	}
	for _, ch := range changes {
		if ch.ChangeType == "revert" && ch.ModeracioEstat == "pendent" {
			return ch.ID
		}
	}
	return 0
}
