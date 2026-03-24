package integration

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestModeracioSummaryScopedGlobalModerationTypes(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_8_summary_scoped_global.sqlite3")

	user := createNonAdminTestUser(t, database, "moderator_scoped_global")
	policyID := createPolicyWithGrant(t, database, "moderator_global_summary", "persones.moderate")
	addGrantToPolicy(t, database, policyID, "media.moderate")
	assignPolicyToUser(t, database, user.ID, policyID)

	session := createSessionCookie(t, database, user.ID, "sess_summary_scoped_global")

	createPendingPersona(t, database, user.ID, "Persona Scoped")
	createPendingMediaAlbum(t, database, user.ID, "Album Scoped")

	req := httptest.NewRequest(http.MethodGet, "/api/admin/control/moderacio/summary", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminControlModeracioSummaryAPI(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("summary esperava 200, got %d", rr.Code)
	}

	var payload struct {
		Ok           bool   `json:"ok"`
		SummaryScope string `json:"summary_scope"`
		Summary      struct {
			ByType []struct {
				Type  string `json:"type"`
				Total int    `json:"total"`
			} `json:"by_type"`
		} `json:"summary"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("summary response invalid: %v", err)
	}
	if !payload.Ok {
		t.Fatalf("summary ok esperat true")
	}
	if payload.SummaryScope != "scoped" {
		t.Fatalf("summary_scope esperat scoped, got %s", payload.SummaryScope)
	}
	got := map[string]int{}
	for _, item := range payload.Summary.ByType {
		got[item.Type] = item.Total
	}
	if got["persona"] != 1 {
		t.Fatalf("summary persona esperat 1, got %d", got["persona"])
	}
	if got["media_album"] != 1 {
		t.Fatalf("summary media_album esperat 1, got %d", got["media_album"])
	}
}

func TestModeracioBulkGlobalPermsBaseTypes(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_8_bulk_base_types.sqlite3")

	user := createNonAdminTestUser(t, database, "moderator_bulk_global")
	policyID := createPolicyWithGrant(t, database, "moderator_global_bulk", "persones.moderate")
	addGrantToPolicy(t, database, policyID, "cognoms.moderate")
	addGrantToPolicy(t, database, policyID, "events.moderate")
	addGrantToPolicy(t, database, policyID, "media.moderate")
	addGrantToPolicy(t, database, policyID, "moderacio.bulk")
	assignPolicyToUser(t, database, user.ID, policyID)

	session := createSessionCookie(t, database, user.ID, "sess_bulk_global")

	personaID := createPendingPersona(t, database, user.ID, "Persona Bulk")
	cognomID, err := database.UpsertCognom("Roca", "roca", "", "", &user.ID)
	if err != nil {
		t.Fatalf("UpsertCognom ha fallat: %v", err)
	}
	variantID := createPendingCognomVariant(t, database, cognomID, user.ID, "RocaVariant")
	eventID := createPendingEventHistoric(t, database, user.ID, "Event Bulk")
	albumID := createPendingMediaAlbum(t, database, user.ID, "Album Bulk")
	itemID := createPendingMediaItem(t, database, albumID, "Item Bulk")

	csrf := "csrf_bulk_persona_global"
	form := newFormValues(map[string]string{
		"bulk_action": "approve",
		"bulk_scope":  "all",
		"bulk_type":   "persona",
		"csrf_token":  csrf,
		"return_to":   "/moderacio",
	})
	req := httptest.NewRequest(http.MethodPost, "/moderacio/bulk", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()
	app.AdminModeracioBulk(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("bulk persona esperava 303, got %d", rr.Code)
	}

	persona, err := database.GetPersona(personaID)
	if err != nil || persona == nil {
		t.Fatalf("GetPersona ha fallat: %v", err)
	}
	if persona.ModeracioEstat != "publicat" {
		t.Fatalf("persona status esperat publicat, got %s", persona.ModeracioEstat)
	}

	csrf = "csrf_bulk_variant_global"
	form = newFormValues(map[string]string{
		"bulk_action": "approve",
		"bulk_scope":  "all",
		"bulk_type":   "cognom_variant",
		"csrf_token":  csrf,
		"return_to":   "/moderacio",
	})
	req = httptest.NewRequest(http.MethodPost, "/moderacio/bulk", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr = httptest.NewRecorder()
	app.AdminModeracioBulk(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("bulk cognom_variant esperava 303, got %d", rr.Code)
	}
	variants, err := database.ListCognomVariants(db.CognomVariantFilter{Status: "publicat"})
	if err != nil {
		t.Fatalf("ListCognomVariants ha fallat: %v", err)
	}
	foundVariant := false
	for _, row := range variants {
		if row.ID == variantID {
			foundVariant = true
			break
		}
	}
	if !foundVariant {
		t.Fatalf("cognom_variant aprovat no trobat")
	}

	csrf = "csrf_bulk_event_global"
	form = newFormValues(map[string]string{
		"bulk_action": "approve",
		"bulk_scope":  "all",
		"bulk_type":   "event_historic",
		"csrf_token":  csrf,
		"return_to":   "/moderacio",
	})
	req = httptest.NewRequest(http.MethodPost, "/moderacio/bulk", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr = httptest.NewRecorder()
	app.AdminModeracioBulk(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("bulk event_historic esperava 303, got %d", rr.Code)
	}
	event, err := database.GetEventHistoric(eventID)
	if err != nil || event == nil {
		t.Fatalf("GetEventHistoric ha fallat: %v", err)
	}
	if event.ModerationStatus != "publicat" {
		t.Fatalf("event_historic status esperat publicat, got %s", event.ModerationStatus)
	}

	csrf = "csrf_bulk_media_album_global"
	form = newFormValues(map[string]string{
		"bulk_action": "approve",
		"bulk_scope":  "all",
		"bulk_type":   "media_album",
		"csrf_token":  csrf,
		"return_to":   "/moderacio",
	})
	req = httptest.NewRequest(http.MethodPost, "/moderacio/bulk", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr = httptest.NewRecorder()
	app.AdminModeracioBulk(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("bulk media_album esperava 303, got %d", rr.Code)
	}
	album, err := database.GetMediaAlbumByID(albumID)
	if err != nil || album == nil {
		t.Fatalf("GetMediaAlbumByID ha fallat: %v", err)
	}
	if album.ModerationStatus != "approved" {
		t.Fatalf("media_album status esperat approved, got %s", album.ModerationStatus)
	}

	csrf = "csrf_bulk_media_item_global"
	form = newFormValues(map[string]string{
		"bulk_action": "approve",
		"bulk_scope":  "all",
		"bulk_type":   "media_item",
		"csrf_token":  csrf,
		"return_to":   "/moderacio",
	})
	req = httptest.NewRequest(http.MethodPost, "/moderacio/bulk", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr = httptest.NewRecorder()
	app.AdminModeracioBulk(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("bulk media_item esperava 303, got %d", rr.Code)
	}
	item, err := database.GetMediaItemByID(itemID)
	if err != nil || item == nil {
		t.Fatalf("GetMediaItemByID ha fallat: %v", err)
	}
	if item.ModerationStatus != "approved" {
		t.Fatalf("media_item status esperat approved, got %s", item.ModerationStatus)
	}
}

func TestModeracioBulkGlobalPermsWikiChanges(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_8_bulk_wiki_changes_global.sqlite3")

	user := createNonAdminTestUser(t, database, "moderator_wiki_global")
	policyID := createPolicyWithGrant(t, database, "moderator_global_wiki", "persones.moderate")
	addGrantToPolicy(t, database, policyID, "cognoms.moderate")
	addGrantToPolicy(t, database, policyID, "events.moderate")
	addGrantToPolicy(t, database, policyID, "moderacio.bulk")
	assignPolicyToUser(t, database, user.ID, policyID)

	session := createSessionCookie(t, database, user.ID, "sess_wiki_global")

	persona := &db.Persona{
		Nom:            "Persona Wiki",
		Cognom1:        "Test",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
	}
	personaID, err := database.CreatePersona(persona)
	if err != nil {
		t.Fatalf("CreatePersona ha fallat: %v", err)
	}
	cognomID, err := database.UpsertCognom("Roca", "roca", "", "", &user.ID)
	if err != nil {
		t.Fatalf("UpsertCognom ha fallat: %v", err)
	}
	eventID, err := database.CreateEventHistoric(&db.EventHistoric{
		Titol:            "Event Wiki",
		Slug:             "event-wiki",
		Tipus:            "altres",
		DataInici:        "1900-01-01",
		ModerationStatus: "publicat",
		CreatedBy:        sql.NullInt64{Int64: int64(user.ID), Valid: true},
	})
	if err != nil {
		t.Fatalf("CreateEventHistoric ha fallat: %v", err)
	}

	personaChangeID := createPendingWikiChange(t, database, user.ID, "persona", personaID, "nom")
	cognomChangeID := createPendingWikiChange(t, database, user.ID, "cognom", cognomID, "notes")
	eventChangeID := createPendingWikiChange(t, database, user.ID, "event_historic", eventID, "titol")

	csrf := "csrf_bulk_persona_canvi"
	form := newFormValues(map[string]string{
		"bulk_action": "reject",
		"bulk_scope":  "all",
		"bulk_type":   "persona_canvi",
		"csrf_token":  csrf,
		"return_to":   "/moderacio",
	})
	req := httptest.NewRequest(http.MethodPost, "/moderacio/bulk", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()
	app.AdminModeracioBulk(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("bulk persona_canvi esperava 303, got %d", rr.Code)
	}
	change, err := database.GetWikiChange(personaChangeID)
	if err != nil || change == nil {
		t.Fatalf("GetWikiChange persona ha fallat: %v", err)
	}
	if change.ModeracioEstat != "rebutjat" {
		t.Fatalf("persona_canvi status esperat rebutjat, got %s", change.ModeracioEstat)
	}

	csrf = "csrf_bulk_cognom_canvi"
	form = newFormValues(map[string]string{
		"bulk_action": "reject",
		"bulk_scope":  "all",
		"bulk_type":   "cognom_canvi",
		"csrf_token":  csrf,
		"return_to":   "/moderacio",
	})
	req = httptest.NewRequest(http.MethodPost, "/moderacio/bulk", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr = httptest.NewRecorder()
	app.AdminModeracioBulk(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("bulk cognom_canvi esperava 303, got %d", rr.Code)
	}
	change, err = database.GetWikiChange(cognomChangeID)
	if err != nil || change == nil {
		t.Fatalf("GetWikiChange cognom ha fallat: %v", err)
	}
	if change.ModeracioEstat != "rebutjat" {
		t.Fatalf("cognom_canvi status esperat rebutjat, got %s", change.ModeracioEstat)
	}

	csrf = "csrf_bulk_event_canvi"
	form = newFormValues(map[string]string{
		"bulk_action": "reject",
		"bulk_scope":  "all",
		"bulk_type":   "event_historic_canvi",
		"csrf_token":  csrf,
		"return_to":   "/moderacio",
	})
	req = httptest.NewRequest(http.MethodPost, "/moderacio/bulk", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr = httptest.NewRecorder()
	app.AdminModeracioBulk(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("bulk event_historic_canvi esperava 303, got %d", rr.Code)
	}
	change, err = database.GetWikiChange(eventChangeID)
	if err != nil || change == nil {
		t.Fatalf("GetWikiChange event ha fallat: %v", err)
	}
	if change.ModeracioEstat != "rebutjat" {
		t.Fatalf("event_historic_canvi status esperat rebutjat, got %s", change.ModeracioEstat)
	}
}
