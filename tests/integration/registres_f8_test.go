package integration

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func ensureAdminPolicyForUser(t *testing.T, database db.DB, userID int) {
	t.Helper()

	if err := database.EnsureDefaultPolicies(); err != nil {
		t.Fatalf("EnsureDefaultPolicies ha fallat: %v", err)
	}

	policies, err := database.ListPolitiques()
	if err != nil {
		t.Fatalf("ListPolitiques ha fallat: %v", err)
	}
	adminID := 0
	for _, p := range policies {
		if p.Nom == "admin" {
			adminID = p.ID
			break
		}
	}
	if adminID == 0 {
		t.Fatalf("no he trobat la política admin")
	}
	if err := database.AddUserPolitica(userID, adminID); err != nil {
		t.Fatalf("AddUserPolitica ha fallat: %v", err)
	}
}

func TestLinkAndUnlinkTranscripcioPersona(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f8_link.sqlite3")

	user, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)

	llibreID, paginaID := createF7LlibreWithPagina(t, database, user.ID)
	registre := &db.TranscripcioRaw{
		LlibreID:       llibreID,
		PaginaID:       sql.NullInt64{Int64: int64(paginaID), Valid: true},
		TipusActe:      "baptisme",
		DataActeEstat:  "clar",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
	}
	registreID, err := database.CreateTranscripcioRaw(registre)
	if err != nil {
		t.Fatalf("CreateTranscripcioRaw ha fallat: %v", err)
	}
	personaRawID, err := database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
		TranscripcioID: registreID,
		Rol:            "batejat",
		Nom:            "Joan",
		Cognom1:        "Vila",
	})
	if err != nil {
		t.Fatalf("CreateTranscripcioPersona ha fallat: %v", err)
	}
	personaID, err := database.CreatePersona(&db.Persona{
		Nom:            "Joan",
		Cognom1:        "Vila",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
		UpdatedBy:      sql.NullInt64{},
	})
	if err != nil {
		t.Fatalf("CreatePersona ha fallat: %v", err)
	}

	csrfToken := "csrf-link"
	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("persona_id", strconv.Itoa(personaID))
	form.Set("return_to", fmt.Sprintf("/documentals/registres/%d", registreID))

	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/documentals/registres/%d/persones/%d/enllacar", registreID, personaRawID), strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrfToken})
	rr := httptest.NewRecorder()

	app.AdminLinkPersonaToRaw(rr, req)

	if rr.Result().StatusCode != http.StatusSeeOther {
		t.Fatalf("status inesperat en enllaçar: %d", rr.Result().StatusCode)
	}

	persones, err := database.ListTranscripcioPersones(registreID)
	if err != nil {
		t.Fatalf("ListTranscripcioPersones ha fallat: %v", err)
	}
	var linked *db.TranscripcioPersonaRaw
	for i := range persones {
		if persones[i].ID == personaRawID {
			linked = &persones[i]
			break
		}
	}
	if linked == nil || !linked.PersonaID.Valid || int(linked.PersonaID.Int64) != personaID {
		t.Fatalf("persona no enllaçada correctament: %+v", linked)
	}
	if !linked.LinkedBy.Valid || int(linked.LinkedBy.Int64) != user.ID {
		t.Fatalf("linked_by inesperat: %+v", linked)
	}

	csrfToken = "csrf-unlink"
	form = url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("return_to", fmt.Sprintf("/documentals/registres/%d", registreID))

	req = httptest.NewRequest(http.MethodPost, fmt.Sprintf("/documentals/registres/%d/persones/%d/desenllacar", registreID, personaRawID), strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrfToken})
	rr = httptest.NewRecorder()

	app.AdminUnlinkPersonaFromRaw(rr, req)

	if rr.Result().StatusCode != http.StatusSeeOther {
		t.Fatalf("status inesperat en desenllaçar: %d", rr.Result().StatusCode)
	}

	persones, err = database.ListTranscripcioPersones(registreID)
	if err != nil {
		t.Fatalf("ListTranscripcioPersones ha fallat: %v", err)
	}
	for _, p := range persones {
		if p.ID == personaRawID && p.PersonaID.Valid {
			t.Fatalf("persona encara enllaçada després de desenllaçar: %+v", p)
		}
	}
}

func TestPersonaRegistresListsLinkedRecords(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f8_persona_registres.sqlite3")

	user, sessionID := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, user.ID)

	llibreID, paginaID := createF7LlibreWithPagina(t, database, user.ID)
	registre := &db.TranscripcioRaw{
		LlibreID:       llibreID,
		PaginaID:       sql.NullInt64{Int64: int64(paginaID), Valid: true},
		TipusActe:      "baptisme",
		DataActeEstat:  "clar",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
	}
	registreID, err := database.CreateTranscripcioRaw(registre)
	if err != nil {
		t.Fatalf("CreateTranscripcioRaw ha fallat: %v", err)
	}
	personaRawID, err := database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
		TranscripcioID: registreID,
		Rol:            "batejat",
		Nom:            "Maria",
		Cognom1:        "Roca",
	})
	if err != nil {
		t.Fatalf("CreateTranscripcioPersona ha fallat: %v", err)
	}
	personaID, err := database.CreatePersona(&db.Persona{
		Nom:            "Maria",
		Cognom1:        "Roca",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
		UpdatedBy:      sql.NullInt64{},
	})
	if err != nil {
		t.Fatalf("CreatePersona ha fallat: %v", err)
	}
	if err := database.LinkTranscripcioPersona(personaRawID, personaID, user.ID); err != nil {
		t.Fatalf("LinkTranscripcioPersona ha fallat: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/persones/%d/registres", personaID), nil)
	req.AddCookie(&http.Cookie{Name: "cg_session", Value: sessionID})
	rr := httptest.NewRecorder()

	app.PersonaRegistres(rr, req)

	if rr.Result().StatusCode != http.StatusOK {
		t.Fatalf("status inesperat: %d", rr.Result().StatusCode)
	}
	if !strings.Contains(rr.Body.String(), fmt.Sprintf("/documentals/registres/%d", registreID)) {
		t.Fatalf("no trobo l'enllaç al registre a la resposta")
	}
}
