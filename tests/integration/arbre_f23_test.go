package integration

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
	testcommon "github.com/marcmoiagese/CercaGenealogica/tests/common"
)

type arbrePerson struct {
	ID int `json:"id"`
}

type arbreLink struct {
	Child  int `json:"child"`
	Father int `json:"father"`
	Mother int `json:"mother"`
}

type arbreDatasetResp struct {
	FamilyData   []arbrePerson `json:"familyData"`
	FamilyLinks  []arbreLink   `json:"familyLinks"`
	RootPersonID int           `json:"rootPersonId"`
	DatasetStats struct {
		People int `json:"people"`
		Links  int `json:"links"`
	} `json:"__DATASET_STATS"`
}

type arbreExpandResp struct {
	People []arbrePerson `json:"people"`
	Links  []arbreLink   `json:"links"`
	Stats  struct {
		People int `json:"people"`
		Links  int `json:"links"`
	} `json:"stats"`
}

func forEachArbreDB(t *testing.T, fn func(t *testing.T, label string, app *core.App, database db.DB, engine string)) {
	t.Helper()

	dbConfs := testcommon.LoadTestDBConfigs(t)
	for _, dbCfg := range dbConfs {
		dbCfg := dbCfg
		t.Run(dbCfg.Label, func(t *testing.T) {
			cfg := map[string]string{}
			for k, v := range dbCfg.Config {
				cfg[k] = v
			}
			if _, ok := cfg["LOG_LEVEL"]; !ok {
				cfg["LOG_LEVEL"] = "silent"
			}
			cfg["RECREADB"] = "true"
			if dbCfg.Engine == "sqlite" {
				tmpDir := t.TempDir()
				cfg["DB_PATH"] = filepath.Join(tmpDir, fmt.Sprintf("test_arbre_%d.sqlite3", time.Now().UnixNano()))
			}

			app, database := newTestAppForConfig(t, cfg)
			testcommon.EnsurePostgresBoolCompat(t, database, dbCfg.Engine)

			fn(t, dbCfg.Label, app, database, dbCfg.Engine)
		})
	}
}

func createTestPersona(t *testing.T, database db.DB, userID int, nom, cognom string) int {
	t.Helper()

	personaID, err := database.CreatePersona(&db.Persona{
		Nom:            nom,
		Cognom1:        cognom,
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	})
	if err != nil || personaID == 0 {
		t.Fatalf("CreatePersona ha fallat: %v", err)
	}
	return personaID
}

func createBaptismeTranscripcio(t *testing.T, database db.DB, llibreID, paginaID, userID int, dateISO string) int {
	t.Helper()

	registre := &db.TranscripcioRaw{
		LlibreID:       llibreID,
		PaginaID:       sql.NullInt64{Int64: int64(paginaID), Valid: true},
		TipusActe:      "baptisme",
		DataActeEstat:  "clar",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	if strings.TrimSpace(dateISO) != "" {
		registre.DataActeISO = sql.NullString{String: dateISO, Valid: true}
	}
	registreID, err := database.CreateTranscripcioRaw(registre)
	if err != nil || registreID == 0 {
		t.Fatalf("CreateTranscripcioRaw ha fallat: %v", err)
	}
	return registreID
}

func linkPersonaToTranscripcio(t *testing.T, database db.DB, transcripcioID, personaID int, role string) {
	t.Helper()

	_, err := database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
		TranscripcioID: transcripcioID,
		Rol:            role,
		Nom:            fmt.Sprintf("P%d", personaID),
		PersonaID:      sql.NullInt64{Int64: int64(personaID), Valid: true},
	})
	if err != nil {
		t.Fatalf("CreateTranscripcioPersona ha fallat: %v", err)
	}
}

func createBaptismeWithParents(t *testing.T, database db.DB, llibreID, paginaID, userID, childID, fatherID, motherID int) {
	t.Helper()

	transID := createBaptismeTranscripcio(t, database, llibreID, paginaID, userID, "")
	linkPersonaToTranscripcio(t, database, transID, childID, "batejat")
	if fatherID > 0 {
		linkPersonaToTranscripcio(t, database, transID, fatherID, "pare")
	}
	if motherID > 0 {
		linkPersonaToTranscripcio(t, database, transID, motherID, "mare")
	}
}

func hasPersonID(people []arbrePerson, target int) bool {
	for _, p := range people {
		if p.ID == target {
			return true
		}
	}
	return false
}

func TestArbreAPIRootOnly(t *testing.T) {
	forEachArbreDB(t, func(t *testing.T, label string, app *core.App, database db.DB, _ string) {
		user, sessionID := createF7UserWithSession(t, database)
		rootID := createTestPersona(t, database, user.ID, "Root", "Solo")

		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/persones/%d/arbre?gens=3", rootID), nil)
		req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
		rr := httptest.NewRecorder()

		app.RequireLogin(app.PersonaArbreAPI)(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status inesperat (%s): %d", label, rr.Code)
		}

		var resp arbreDatasetResp
		if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
			t.Fatalf("decode resposta API ha fallat: %v", err)
		}
		if resp.RootPersonID != rootID {
			t.Fatalf("rootPersonId inesperat: %d", resp.RootPersonID)
		}
		if len(resp.FamilyData) != 1 {
			t.Fatalf("esperava 1 persona, got %d", len(resp.FamilyData))
		}
		if len(resp.FamilyLinks) != 0 {
			t.Fatalf("esperava 0 links, got %d", len(resp.FamilyLinks))
		}
		if resp.DatasetStats.People != len(resp.FamilyData) || resp.DatasetStats.Links != len(resp.FamilyLinks) {
			t.Fatalf("stats incoherents: %+v", resp.DatasetStats)
		}
	})
}

func TestArbreAPIWithParents(t *testing.T) {
	forEachArbreDB(t, func(t *testing.T, label string, app *core.App, database db.DB, _ string) {
		user, sessionID := createF7UserWithSession(t, database)
		llibreID, paginaID := createF7LlibreWithPagina(t, database, user.ID)

		rootID := createTestPersona(t, database, user.ID, "Root", "Child")
		fatherID := createTestPersona(t, database, user.ID, "Father", "Parent")
		motherID := createTestPersona(t, database, user.ID, "Mother", "Parent")

		createBaptismeWithParents(t, database, llibreID, paginaID, user.ID, rootID, fatherID, motherID)

		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/persones/%d/arbre?gens=3", rootID), nil)
		req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
		rr := httptest.NewRecorder()

		app.RequireLogin(app.PersonaArbreAPI)(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status inesperat (%s): %d", label, rr.Code)
		}

		var resp arbreDatasetResp
		if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
			t.Fatalf("decode resposta API ha fallat: %v", err)
		}
		if len(resp.FamilyData) != 3 {
			t.Fatalf("esperava 3 persones, got %d", len(resp.FamilyData))
		}
		if len(resp.FamilyLinks) != 1 {
			t.Fatalf("esperava 1 link, got %d", len(resp.FamilyLinks))
		}
		if !hasPersonID(resp.FamilyData, fatherID) || !hasPersonID(resp.FamilyData, motherID) {
			t.Fatalf("pares no presents al dataset")
		}
		link := resp.FamilyLinks[0]
		if link.Child != rootID || link.Father != fatherID || link.Mother != motherID {
			t.Fatalf("link inesperat: %+v", link)
		}
	})
}

func TestArbreAPIGensLimit(t *testing.T) {
	forEachArbreDB(t, func(t *testing.T, label string, app *core.App, database db.DB, _ string) {
		user, sessionID := createF7UserWithSession(t, database)
		llibreID, paginaID := createF7LlibreWithPagina(t, database, user.ID)

		rootID := createTestPersona(t, database, user.ID, "Root", "Tree")
		f1 := createTestPersona(t, database, user.ID, "F1", "Tree")
		m1 := createTestPersona(t, database, user.ID, "M1", "Tree")
		f2 := createTestPersona(t, database, user.ID, "F2", "Tree")
		m2 := createTestPersona(t, database, user.ID, "M2", "Tree")
		f3 := createTestPersona(t, database, user.ID, "F3", "Tree")
		m3 := createTestPersona(t, database, user.ID, "M3", "Tree")
		g1 := createTestPersona(t, database, user.ID, "G1", "Tree")
		g2 := createTestPersona(t, database, user.ID, "G2", "Tree")

		createBaptismeWithParents(t, database, llibreID, paginaID, user.ID, rootID, f1, m1)
		createBaptismeWithParents(t, database, llibreID, paginaID, user.ID, f1, f2, m2)
		createBaptismeWithParents(t, database, llibreID, paginaID, user.ID, m1, f3, m3)
		createBaptismeWithParents(t, database, llibreID, paginaID, user.ID, f2, g1, g2)

		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/persones/%d/arbre?gens=3", rootID), nil)
		req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
		rr := httptest.NewRecorder()

		app.RequireLogin(app.PersonaArbreAPI)(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status inesperat (%s): %d", label, rr.Code)
		}

		var resp arbreDatasetResp
		if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
			t.Fatalf("decode resposta API ha fallat: %v", err)
		}
		if len(resp.FamilyData) != 7 {
			t.Fatalf("esperava 7 persones (3 generacions), got %d", len(resp.FamilyData))
		}
		if hasPersonID(resp.FamilyData, g1) || hasPersonID(resp.FamilyData, g2) {
			t.Fatalf("great-grandparents no haurien d'aparèixer amb gens=3")
		}
	})
}

func TestArbreAPILoopResilience(t *testing.T) {
	forEachArbreDB(t, func(t *testing.T, label string, app *core.App, database db.DB, _ string) {
		user, sessionID := createF7UserWithSession(t, database)
		llibreID, paginaID := createF7LlibreWithPagina(t, database, user.ID)

		aID := createTestPersona(t, database, user.ID, "A", "Loop")
		bID := createTestPersona(t, database, user.ID, "B", "Loop")

		createBaptismeWithParents(t, database, llibreID, paginaID, user.ID, aID, bID, 0)
		createBaptismeWithParents(t, database, llibreID, paginaID, user.ID, bID, aID, 0)

		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/persones/%d/arbre?gens=4", aID), nil)
		req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
		rr := httptest.NewRecorder()

		app.RequireLogin(app.PersonaArbreAPI)(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status inesperat (%s): %d", label, rr.Code)
		}

		var resp arbreDatasetResp
		if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
			t.Fatalf("decode resposta API ha fallat: %v", err)
		}
		if len(resp.FamilyData) != 2 {
			t.Fatalf("esperava 2 persones amb loop, got %d", len(resp.FamilyData))
		}
	})
}

func TestArbreExpandEndpoint(t *testing.T) {
	forEachArbreDB(t, func(t *testing.T, label string, app *core.App, database db.DB, _ string) {
		user, sessionID := createF7UserWithSession(t, database)
		llibreID, paginaID := createF7LlibreWithPagina(t, database, user.ID)

		rootID := createTestPersona(t, database, user.ID, "Root", "Expand")
		fatherID := createTestPersona(t, database, user.ID, "Father", "Expand")
		motherID := createTestPersona(t, database, user.ID, "Mother", "Expand")

		createBaptismeWithParents(t, database, llibreID, paginaID, user.ID, rootID, fatherID, motherID)

		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/arbre/expand?person_id=%d&gens=2&mode=ancestors", rootID), nil)
		req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
		rr := httptest.NewRecorder()

		app.RequireLogin(app.ArbreExpandAPI)(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status inesperat (%s): %d", label, rr.Code)
		}

		var resp arbreExpandResp
		if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
			t.Fatalf("decode resposta expand ha fallat: %v", err)
		}
		if len(resp.People) != 3 {
			t.Fatalf("esperava 3 persones, got %d", len(resp.People))
		}
		if len(resp.Links) != 1 {
			t.Fatalf("esperava 1 link, got %d", len(resp.Links))
		}
		if resp.Stats.People != len(resp.People) || resp.Stats.Links != len(resp.Links) {
			t.Fatalf("stats incoherents: %+v", resp.Stats)
		}
		seenPeople := map[int]struct{}{}
		for _, p := range resp.People {
			if _, ok := seenPeople[p.ID]; ok {
				t.Fatalf("persona duplicada en expand: %d", p.ID)
			}
			seenPeople[p.ID] = struct{}{}
		}
		seenLinks := map[string]struct{}{}
		for _, l := range resp.Links {
			key := fmt.Sprintf("%d:%d:%d", l.Child, l.Father, l.Mother)
			if _, ok := seenLinks[key]; ok {
				t.Fatalf("link duplicat en expand: %s", key)
			}
			seenLinks[key] = struct{}{}
		}
	})
}

func TestPersonaArbreViewsRender(t *testing.T) {
	forEachArbreDB(t, func(t *testing.T, label string, app *core.App, database db.DB, _ string) {
		user, sessionID := createF7UserWithSession(t, database)
		rootID := createTestPersona(t, database, user.ID, "Root", "UI")

		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/persones/%d/arbre?view=pedigree&gens=3", rootID), nil)
		req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
		rr := httptest.NewRecorder()
		app.RequireLogin(app.PersonaArbre)(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status inesperat (%s): %d", label, rr.Code)
		}
		body := rr.Body.String()
		if !strings.Contains(body, "id=\"treeSvg\"") {
			t.Fatalf("no trobo treeSvg al HTML")
		}
		if !strings.Contains(body, "tree-i18n") {
			t.Fatalf("no trobo tree-i18n al HTML")
		}

		req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/persones/%d/arbre?view=ventall&gens=3", rootID), nil)
		req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
		rr = httptest.NewRecorder()
		app.RequireLogin(app.PersonaArbre)(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status inesperat (%s) ventall: %d", label, rr.Code)
		}
		body = rr.Body.String()
		if !strings.Contains(body, "id=\"fanSvg\"") {
			t.Fatalf("no trobo fanSvg al HTML")
		}
		if !strings.Contains(body, "tree-i18n") {
			t.Fatalf("no trobo tree-i18n al HTML (ventall)")
		}
	})
}
