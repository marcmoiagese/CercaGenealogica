package integration

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func createF16Registre(t *testing.T, database db.DB, llibreID, paginaID, userID int, tipus string, any int, status string) int {
	t.Helper()

	registre := &db.TranscripcioRaw{
		LlibreID:       llibreID,
		PaginaID:       sql.NullInt64{Int64: int64(paginaID), Valid: true},
		TipusActe:      tipus,
		DataActeEstat:  "clar",
		ModeracioEstat: status,
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	if any > 0 {
		registre.AnyDoc = sql.NullInt64{Int64: int64(any), Valid: true}
	}
	registreID, err := database.CreateTranscripcioRaw(registre)
	if err != nil {
		t.Fatalf("CreateTranscripcioRaw ha fallat: %v", err)
	}
	return registreID
}

func fetchMunicipiStatsJSON(t *testing.T, app interface {
	MunicipiMapesAPI(http.ResponseWriter, *http.Request)
}, path string) []byte {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, path, nil)
	rr := httptest.NewRecorder()
	app.MunicipiMapesAPI(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status inesperat %s: %d", path, rr.Code)
	}
	return rr.Body.Bytes()
}

func TestMunicipiStatsBaptisme(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f16_stats_baptisme.sqlite3")

	user, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)

	llibreID, paginaID := createF7LlibreWithPagina(t, database, user.ID)
	llibre, err := database.GetLlibre(llibreID)
	if err != nil || llibre == nil {
		t.Fatalf("GetLlibre ha fallat: %v", err)
	}
	munID := llibre.MunicipiID

	registreID := createF16Registre(t, database, llibreID, paginaID, user.ID, "baptisme", 1890, "pendent")
	_, _ = database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
		TranscripcioID: registreID,
		Rol:            "batejat",
		Nom:            "Josep",
		NomEstat:       "clar",
		Cognom1:        "Garcia",
		Cognom1Estat:   "clar",
		Cognom2:        "Puig",
		Cognom2Estat:   "clar",
	})
	_, _ = database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
		TranscripcioID: registreID,
		Rol:            "batejat",
		Nom:            "Joan2",
		NomEstat:       "clar",
	})

	moderateObject(t, app, sessionID, "registre", registreID, "aprovar")

	var topResp struct {
		Items []struct {
			ID    int    `json:"id"`
			Label string `json:"label"`
			Total int    `json:"total"`
		} `json:"items"`
	}
	body := fetchMunicipiStatsJSON(t, app, fmt.Sprintf("/api/municipis/%d/stats/top-noms?limit=10", munID))
	if err := json.Unmarshal(body, &topResp); err != nil {
		t.Fatalf("json top-noms invalid: %v", err)
	}
	nomID := 0
	for _, item := range topResp.Items {
		if item.Label == "Josep" {
			if item.Total != 1 {
				t.Fatalf("esperava Josep=1, got %d", item.Total)
			}
			nomID = item.ID
		}
		if item.Label == "Joan2" {
			t.Fatalf("no s'hauria de comptar Joan2")
		}
	}
	if nomID == 0 {
		t.Fatalf("no he trobat Josep a top-noms")
	}

	var seriesResp struct {
		Items []struct {
			X int `json:"x"`
			Y int `json:"y"`
		} `json:"items"`
	}
	body = fetchMunicipiStatsJSON(t, app, fmt.Sprintf("/api/municipis/%d/stats/nom-series?nom_id=%d&bucket=year", munID, nomID))
	if err := json.Unmarshal(body, &seriesResp); err != nil {
		t.Fatalf("json nom-series invalid: %v", err)
	}
	found := false
	for _, item := range seriesResp.Items {
		if item.X == 1890 {
			found = true
			if item.Y != 1 {
				t.Fatalf("esperava serie 1890=1, got %d", item.Y)
			}
		}
	}
	if !found {
		t.Fatalf("no s'ha trobat l'any 1890 a la serie")
	}

	var cognomResp struct {
		Items []struct {
			ID    int    `json:"id"`
			Label string `json:"label"`
			Total int    `json:"total"`
		} `json:"items"`
	}
	body = fetchMunicipiStatsJSON(t, app, fmt.Sprintf("/api/municipis/%d/stats/top-cognoms?limit=10", munID))
	if err := json.Unmarshal(body, &cognomResp); err != nil {
		t.Fatalf("json top-cognoms invalid: %v", err)
	}
	cognomID := 0
	for _, item := range cognomResp.Items {
		if item.Label == "Garcia" {
			if item.Total != 1 {
				t.Fatalf("esperava Garcia=1, got %d", item.Total)
			}
			cognomID = item.ID
		}
	}
	if cognomID == 0 {
		t.Fatalf("no he trobat Garcia a top-cognoms")
	}
	body = fetchMunicipiStatsJSON(t, app, fmt.Sprintf("/api/municipis/%d/stats/cognom-series?cognom_id=%d&bucket=year", munID, cognomID))
	if err := json.Unmarshal(body, &seriesResp); err != nil {
		t.Fatalf("json cognom-series invalid: %v", err)
	}
	found = false
	for _, item := range seriesResp.Items {
		if item.X == 1890 {
			found = true
			if item.Y != 1 {
				t.Fatalf("esperava serie cognom 1890=1, got %d", item.Y)
			}
		}
	}
	if !found {
		t.Fatalf("no s'ha trobat l'any 1890 a la serie cognom")
	}
}

func TestMunicipiStatsMatrimoni(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f16_stats_matrimoni.sqlite3")

	user, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)

	llibreID, paginaID := createF7LlibreWithPagina(t, database, user.ID)
	llibre, err := database.GetLlibre(llibreID)
	if err != nil || llibre == nil {
		t.Fatalf("GetLlibre ha fallat: %v", err)
	}
	munID := llibre.MunicipiID

	registreID := createF16Registre(t, database, llibreID, paginaID, user.ID, "matrimoni", 1901, "pendent")
	_, _ = database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
		TranscripcioID: registreID,
		Rol:            "nuvi",
		Nom:            "Joan",
		NomEstat:       "clar",
	})
	_, _ = database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
		TranscripcioID: registreID,
		Rol:            "novia",
		Nom:            "Maria",
		NomEstat:       "clar",
	})

	moderateObject(t, app, sessionID, "registre", registreID, "aprovar")

	var topResp struct {
		Items []struct {
			Label string `json:"label"`
			Total int    `json:"total"`
		} `json:"items"`
	}
	body := fetchMunicipiStatsJSON(t, app, fmt.Sprintf("/api/municipis/%d/stats/top-noms?limit=10", munID))
	if err := json.Unmarshal(body, &topResp); err != nil {
		t.Fatalf("json top-noms invalid: %v", err)
	}
	foundJoan := false
	foundMaria := false
	for _, item := range topResp.Items {
		switch item.Label {
		case "Joan":
			foundJoan = true
			if item.Total != 1 {
				t.Fatalf("esperava Joan=1, got %d", item.Total)
			}
		case "Maria":
			foundMaria = true
			if item.Total != 1 {
				t.Fatalf("esperava Maria=1, got %d", item.Total)
			}
		}
	}
	if !foundJoan || !foundMaria {
		t.Fatalf("esperava Joan i Maria a top-noms")
	}
}

func TestMunicipiStatsObitIgnoresMissingYear(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f16_stats_obit.sqlite3")

	user, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)

	llibreID, paginaID := createF7LlibreWithPagina(t, database, user.ID)
	llibre, err := database.GetLlibre(llibreID)
	if err != nil || llibre == nil {
		t.Fatalf("GetLlibre ha fallat: %v", err)
	}
	munID := llibre.MunicipiID

	registreID := createF16Registre(t, database, llibreID, paginaID, user.ID, "obit", 1880, "pendent")
	_, _ = database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
		TranscripcioID: registreID,
		Rol:            "difunt",
		Nom:            "Ramon",
		NomEstat:       "clar",
	})
	invalidID := createF16Registre(t, database, llibreID, paginaID, user.ID, "obit", 0, "pendent")
	_, _ = database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
		TranscripcioID: invalidID,
		Rol:            "difunt",
		Nom:            "Ignasi",
		NomEstat:       "clar",
	})

	moderateObject(t, app, sessionID, "registre", registreID, "aprovar")
	moderateObject(t, app, sessionID, "registre", invalidID, "aprovar")

	var topResp struct {
		Items []struct {
			Label string `json:"label"`
		} `json:"items"`
	}
	body := fetchMunicipiStatsJSON(t, app, fmt.Sprintf("/api/municipis/%d/stats/top-noms?limit=10", munID))
	if err := json.Unmarshal(body, &topResp); err != nil {
		t.Fatalf("json top-noms invalid: %v", err)
	}
	for _, item := range topResp.Items {
		if item.Label == "Ignasi" {
			t.Fatalf("no s'hauria de comptar Ignasi sense any")
		}
	}
}
