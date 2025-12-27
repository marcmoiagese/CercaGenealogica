package integration

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func createF9LlibreWithMunicipi(t *testing.T, database db.DB, userID int) int {
	t.Helper()

	mun := &db.Municipi{
		Nom:            "Municipi F9",
		Tipus:          "municipi",
		Estat:          "actiu",
		Latitud:        sql.NullFloat64{Float64: 41.3879, Valid: true},
		Longitud:       sql.NullFloat64{Float64: 2.16992, Valid: true},
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	munID, err := database.CreateMunicipi(mun)
	if err != nil {
		t.Fatalf("CreateMunicipi ha fallat: %v", err)
	}
	arch := &db.Arquebisbat{
		Nom:            fmt.Sprintf("Bisbat F9 %d", time.Now().UnixNano()),
		TipusEntitat:   "bisbat",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	archID, err := database.CreateArquebisbat(arch)
	if err != nil {
		t.Fatalf("CreateArquebisbat ha fallat: %v", err)
	}
	llibre := &db.Llibre{
		ArquebisbatID:  archID,
		MunicipiID:     munID,
		Titol:          "Llibre F9",
		TipusLlibre:    "baptismes",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	llibreID, err := database.CreateLlibre(llibre)
	if err != nil {
		t.Fatalf("CreateLlibre ha fallat: %v", err)
	}
	return llibreID
}

func TestCognomsImportStatsHeatmap(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f9_cognoms.sqlite3")

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

	csrfToken := "csrf-f9-import"
	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	req := httptest.NewRequest(http.MethodPost, "/admin/cognoms/import/run", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrfToken})
	rr := httptest.NewRecorder()

	app.AdminCognomsImportRun(rr, req)
	if rr.Result().StatusCode != http.StatusSeeOther {
		t.Fatalf("status inesperat import: %d", rr.Result().StatusCode)
	}

	csrfToken = "csrf-f9-stats"
	form = url.Values{}
	form.Set("csrf_token", csrfToken)
	req = httptest.NewRequest(http.MethodPost, "/admin/cognoms/stats/run", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrfToken})
	rr = httptest.NewRecorder()

	app.AdminCognomsStatsRun(rr, req)
	if rr.Result().StatusCode != http.StatusSeeOther {
		t.Fatalf("status inesperat stats: %d", rr.Result().StatusCode)
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

	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/cognoms/%d/heatmap?y0=1880&y1=1900", cognomID), nil)
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	rr = httptest.NewRecorder()

	app.CognomHeatmapJSON(rr, req)
	if rr.Result().StatusCode != http.StatusOK {
		t.Fatalf("status inesperat heatmap: %d", rr.Result().StatusCode)
	}
	var payload struct {
		Points []struct {
			W int `json:"w"`
		} `json:"points"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&payload); err != nil {
		t.Fatalf("no puc parsejar JSON heatmap: %v", err)
	}
	found := false
	for _, p := range payload.Points {
		if p.W > 0 {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("heatmap sense punts esperats: %+v", payload.Points)
	}
}
