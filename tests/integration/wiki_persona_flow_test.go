package integration

import (
	"bytes"
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

type personaUpdateResponse struct {
	ChangeID int  `json:"change_id"`
	Pending  bool `json:"pending"`
}

type wikiMetaResponse struct {
	Before         json.RawMessage `json:"before"`
	After          json.RawMessage `json:"after"`
	SourceChangeID int             `json:"source_change_id"`
}

func TestWikiPersonaFlowHistoryAndRevert(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_wiki_persona_flow.sqlite3")

	author := createTestUser(t, database, "wiki_persona_author_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	assignPolicyByName(t, database, author.ID, "confiança")
	authorSession := createSessionCookie(t, database, author.ID, "sess_author_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	randomUser := createTestUser(t, database, "wiki_persona_random_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	randomSession := createSessionCookie(t, database, randomUser.ID, "sess_random_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	moderator := createTestUser(t, database, "wiki_persona_mod_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	assignPolicyByName(t, database, moderator.ID, "admin")
	moderatorSession := createSessionCookie(t, database, moderator.ID, "sess_mod_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	persona := &db.Persona{
		Nom:            "Joan",
		Cognom1:        "Riera",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(author.ID), Valid: true},
	}
	personaID, err := database.CreatePersona(persona)
	if err != nil {
		t.Fatalf("CreatePersona ha fallat: %v", err)
	}

	changeID1 := submitPersonaUpdate(t, app, authorSession, personaID, "Joan", "Riera", "Fuster 1")
	if changeID1 == 0 {
		t.Fatalf("no s'ha rebut change_id per la primera proposta")
	}

	stored, err := database.GetPersona(personaID)
	if err != nil || stored == nil {
		t.Fatalf("GetPersona ha fallat: %v", err)
	}
	if stored.Ofici != "" {
		t.Fatalf("la persona publicat no s'hauria d'actualitzar, got ofici=%q", stored.Ofici)
	}

	checkHistoryVisibility(t, app, personaID, randomSession, changeID1, false)
	checkHistoryVisibility(t, app, personaID, authorSession, changeID1, true)
	checkHistoryVisibility(t, app, personaID, moderatorSession, changeID1, true)

	approveWikiChange(t, app, moderatorSession, changeID1, "persona_canvi")

	stored, _ = database.GetPersona(personaID)
	if stored == nil || stored.Ofici != "Fuster 1" {
		t.Fatalf("esperava ofici aplicat 'Fuster 1', got %q", stored.Ofici)
	}

	changeID2 := submitPersonaUpdate(t, app, authorSession, personaID, "Joan", "Riera", "Fuster 2")
	if changeID2 == 0 {
		t.Fatalf("no s'ha rebut change_id per la segona proposta")
	}
	approveWikiChange(t, app, moderatorSession, changeID2, "persona_canvi")

	stored, _ = database.GetPersona(personaID)
	if stored == nil || stored.Ofici != "Fuster 2" {
		t.Fatalf("esperava ofici aplicat 'Fuster 2', got %q", stored.Ofici)
	}

	revertChangeID := submitPersonaRevert(t, app, authorSession, personaID, changeID1)
	if revertChangeID == 0 {
		t.Fatalf("no s'ha creat la proposta de revert")
	}

	change, err := database.GetWikiChange(revertChangeID)
	if err != nil || change == nil {
		t.Fatalf("GetWikiChange revert ha fallat: %v", err)
	}
	var meta wikiMetaResponse
	if err := json.Unmarshal([]byte(change.Metadata), &meta); err != nil {
		t.Fatalf("metadata revert invàlida: %v", err)
	}
	if meta.SourceChangeID != changeID1 {
		t.Fatalf("source_change_id esperat %d, got %d", changeID1, meta.SourceChangeID)
	}

	approveWikiChange(t, app, moderatorSession, revertChangeID, "persona_canvi")

	stored, _ = database.GetPersona(personaID)
	if stored == nil || stored.Ofici != "Fuster 1" {
		t.Fatalf("esperava ofici revertit 'Fuster 1', got %q", stored.Ofici)
	}

	changeID3 := submitPersonaUpdate(t, app, authorSession, personaID, "Joan", "Riera", "Fuster 3")
	rejectWikiChange(t, app, moderatorSession, changeID3, "persona_canvi")

	stored, _ = database.GetPersona(personaID)
	if stored == nil || stored.Ofici != "Fuster 1" {
		t.Fatalf("la persona no hauria de canviar en rebutjar, got %q", stored.Ofici)
	}

	rejected, err := database.GetWikiChange(changeID3)
	if err != nil || rejected == nil {
		t.Fatalf("GetWikiChange rebutjat ha fallat: %v", err)
	}
	if rejected.ModeracioEstat != "rebutjat" {
		t.Fatalf("estat esperat rebutjat, got %s", rejected.ModeracioEstat)
	}
}

func submitPersonaUpdate(t *testing.T, app *core.App, session *http.Cookie, personaID int, nom, cognom, ofici string) int {
	t.Helper()
	payload := map[string]string{
		"nom":      nom,
		"cognom1":  cognom,
		"cognom2":  "",
		"municipi": "",
		"llibre":   "",
		"pagina":   "",
		"ofici":    ofici,
		"motiu":    "test",
	}
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/persones/"+strconv.Itoa(personaID), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(session)
	rr := httptest.NewRecorder()

	app.UpdatePersona(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("UpdatePersona expected 200, got %d", rr.Code)
	}
	var resp personaUpdateResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("no puc parsejar resposta UpdatePersona: %v", err)
	}
	if !resp.Pending || resp.ChangeID == 0 {
		t.Fatalf("resposta UpdatePersona inesperada: %+v", resp)
	}
	return resp.ChangeID
}

func submitPersonaRevert(t *testing.T, app *core.App, session *http.Cookie, personaID, changeID int) int {
	t.Helper()
	csrf := "csrf_revert_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	form := url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("change_id", strconv.Itoa(changeID))
	form.Set("reason", "revert")
	req := httptest.NewRequest(http.MethodPost, "/persones/"+strconv.Itoa(personaID)+"/historial/revert", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()

	app.PersonaWikiRevert(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("revert expected 303, got %d", rr.Code)
	}

	changes, err := app.DB.ListWikiChanges("persona", personaID)
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

func checkHistoryVisibility(t *testing.T, app *core.App, personaID int, session *http.Cookie, changeID int, shouldSee bool) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/persones/"+strconv.Itoa(personaID)+"/historial", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()

	app.PersonaWikiHistory(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("history expected 200, got %d", rr.Code)
	}
	needle := fmt.Sprintf("data-change-id=\"%d\"", changeID)
	found := strings.Contains(rr.Body.String(), needle)
	if shouldSee && !found {
		t.Fatalf("esperava veure el canvi %d a l'historial", changeID)
	}
	if !shouldSee && found {
		t.Fatalf("no hauria de veure el canvi %d a l'historial", changeID)
	}
}

func approveWikiChange(t *testing.T, app *core.App, session *http.Cookie, changeID int, objType string) {
	t.Helper()
	csrf := "csrf_approve_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	form := url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("object_type", objType)
	req := httptest.NewRequest(http.MethodPost, "/moderacio/"+strconv.Itoa(changeID)+"/aprovar", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()

	app.AdminModeracioAprovar(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("approve expected 303, got %d", rr.Code)
	}
}

func rejectWikiChange(t *testing.T, app *core.App, session *http.Cookie, changeID int, objType string) {
	t.Helper()
	csrf := "csrf_reject_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	form := url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("object_type", objType)
	form.Set("reason", "no")
	req := httptest.NewRequest(http.MethodPost, "/moderacio/"+strconv.Itoa(changeID)+"/rebutjar", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()

	app.AdminModeracioRebutjar(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("reject expected 303, got %d", rr.Code)
	}
}
