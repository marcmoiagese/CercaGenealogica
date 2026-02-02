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

type eventsListResponse struct {
	Items []struct {
		ID int `json:"id"`
	} `json:"items"`
}

func TestEventsModerationAndFilters(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f19_events_filters.sqlite3")

	author := createTestUser(t, database, "event_author_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	authorSession := createSessionCookie(t, database, author.ID, "sess_event_author_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	moderator := createTestUser(t, database, "event_mod_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	assignPolicyByName(t, database, moderator.ID, "admin")
	moderatorSession := createSessionCookie(t, database, moderator.ID, "sess_event_mod_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	paisID, nivellID := createTestPaisNivell(t, database, moderator.ID)
	munID := createHistoriaMunicipi(t, database, moderator.ID)
	eclesID := createTestArquebisbat(t, database, moderator.ID)

	pendingID := submitEventForm(t, app, authorSession, "Event pendent", "guerra", "1910-05-10", "municipi", munID, "directe", 4)

	pending, err := database.GetEventHistoric(pendingID)
	if err != nil || pending == nil {
		t.Fatalf("GetEventHistoric pendent ha fallat: %v", err)
	}
	if pending.ModerationStatus != "pendent" {
		t.Fatalf("esperava estat pendent, got %s", pending.ModerationStatus)
	}

	ids := fetchEventIDsFromAPI(t, app, "/api/events")
	if containsEventID(ids, pendingID) {
		t.Fatalf("event pendent no hauria d'aparèixer a l'API pública")
	}

	req := httptest.NewRequest(http.MethodGet, "/api/events/"+strconv.Itoa(pendingID), nil)
	rr := httptest.NewRecorder()
	app.EventsAPI(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("event pendent API expected 404, got %d", rr.Code)
	}

	approveWikiChange(t, app, moderatorSession, pendingID, "event_historic")

	ids = fetchEventIDsFromAPI(t, app, "/api/events")
	if !containsEventID(ids, pendingID) {
		t.Fatalf("event aprovat no apareix a l'API pública")
	}

	ids = fetchEventIDsFromAPI(t, app, fmt.Sprintf("/api/events?scope_type=municipi&scope_id=%d", munID))
	if !containsEventID(ids, pendingID) {
		t.Fatalf("event aprovat no apareix al filtre per municipi")
	}

	eventPais := createPublicEventWithImpact(t, database, moderator.ID, "Event pais", "pesta", "1900-01-01", "", "pais", paisID, "indirecte", 2)
	eventNivell := createPublicEventWithImpact(t, database, moderator.ID, "Event nivell", "revolta", "1850-01-01", "", "nivell_admin", nivellID, "directe", 5)
	eventEcles := createPublicEventWithImpact(t, database, moderator.ID, "Event ecles", "incendi", "", "", "entitat_eclesiastica", eclesID, "directe", 3)

	ids = fetchEventIDsFromAPI(t, app, "/api/events?tipus=pesta")
	if !containsEventID(ids, eventPais) || containsEventID(ids, eventNivell) || containsEventID(ids, eventEcles) || containsEventID(ids, pendingID) {
		t.Fatalf("filtre per tipus no retorna els esdeveniments esperats")
	}

	ids = fetchEventIDsFromAPI(t, app, "/api/events?from=1890-01-01&to=1920-01-01")
	if !containsEventID(ids, pendingID) || !containsEventID(ids, eventPais) || containsEventID(ids, eventNivell) || containsEventID(ids, eventEcles) {
		t.Fatalf("filtre per dates no retorna els esdeveniments esperats")
	}

	ids = fetchEventIDsFromAPI(t, app, fmt.Sprintf("/api/events?scope_type=pais&scope_id=%d", paisID))
	if !containsEventID(ids, eventPais) || containsEventID(ids, pendingID) || containsEventID(ids, eventNivell) || containsEventID(ids, eventEcles) {
		t.Fatalf("filtre per país incorrecte")
	}

	ids = fetchEventIDsFromAPI(t, app, fmt.Sprintf("/api/events?scope_type=nivell_admin&scope_id=%d", nivellID))
	if !containsEventID(ids, eventNivell) || containsEventID(ids, pendingID) || containsEventID(ids, eventPais) || containsEventID(ids, eventEcles) {
		t.Fatalf("filtre per nivell incorrecte")
	}

	ids = fetchEventIDsFromAPI(t, app, fmt.Sprintf("/api/events?scope_type=entitat_eclesiastica&scope_id=%d", eclesID))
	if !containsEventID(ids, eventEcles) || containsEventID(ids, pendingID) || containsEventID(ids, eventPais) || containsEventID(ids, eventNivell) {
		t.Fatalf("filtre per entitat eclesiàstica incorrecte")
	}

	ids = fetchEventIDsFromAPI(t, app, "/api/events?impacte_tipus=indirecte")
	if !containsEventID(ids, eventPais) || containsEventID(ids, eventNivell) || containsEventID(ids, eventEcles) || containsEventID(ids, pendingID) {
		t.Fatalf("filtre per impacte_tipus incorrecte")
	}

	ids = fetchEventIDsFromAPI(t, app, "/api/events?intensitat_min=4")
	if !containsEventID(ids, pendingID) || !containsEventID(ids, eventNivell) || containsEventID(ids, eventPais) || containsEventID(ids, eventEcles) {
		t.Fatalf("filtre per intensitat mínima incorrecte")
	}

	ids = fetchEventIDsFromAPI(t, app, "/api/events?with_dates=1")
	if !containsEventID(ids, pendingID) || !containsEventID(ids, eventPais) || !containsEventID(ids, eventNivell) || containsEventID(ids, eventEcles) {
		t.Fatalf("filtre only_with_dates incorrecte")
	}

	ids = fetchEventIDsFromAPI(t, app, "/api/events?order=intensitat_desc")
	if len(ids) == 0 || ids[0] != eventNivell {
		t.Fatalf("ordenació per intensitat incorrecta, primer=%v", ids)
	}
}

func TestEventWikiFlow(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f19_events_wiki.sqlite3")

	editor := createTestUser(t, database, "event_editor_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	editorSession := createSessionCookie(t, database, editor.ID, "sess_event_editor_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	randomUser := createTestUser(t, database, "event_random_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	randomSession := createSessionCookie(t, database, randomUser.ID, "sess_event_random_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	moderator := createTestUser(t, database, "event_mod_wiki_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	assignPolicyByName(t, database, moderator.ID, "admin")
	moderatorSession := createSessionCookie(t, database, moderator.ID, "sess_event_mod_wiki_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	munID := createHistoriaMunicipi(t, database, moderator.ID)
	eventID := createPublicEventWithImpact(t, database, moderator.ID, "Event original", "guerra", "1900-01-01", "", "municipi", munID, "directe", 2)

	submitEventUpdate(t, app, editorSession, eventID, "Event v1", "guerra", "1901-01-01", "Resum v1", "municipi", munID, "directe", 4)

	stored, err := database.GetEventHistoric(eventID)
	if err != nil || stored == nil {
		t.Fatalf("GetEventHistoric ha fallat: %v", err)
	}
	if stored.Titol != "Event original" {
		t.Fatalf("event publicat no s'hauria d'actualitzar, got %q", stored.Titol)
	}

	changeID1 := findPendingWikiChangeID(t, database, "event_historic", eventID)
	if changeID1 == 0 {
		t.Fatalf("no s'ha creat el canvi wiki pendent")
	}
	checkEventHistoryVisibility(t, app, eventID, randomSession, changeID1, false)
	checkEventHistoryVisibility(t, app, eventID, editorSession, changeID1, true)
	checkEventHistoryVisibility(t, app, eventID, moderatorSession, changeID1, true)

	approveWikiChange(t, app, moderatorSession, changeID1, "event_historic_canvi")

	stored, _ = database.GetEventHistoric(eventID)
	if stored == nil || stored.Titol != "Event v1" {
		t.Fatalf("esperava títol aplicat, got %q", stored.Titol)
	}
	impacts, _ := database.ListEventImpacts(eventID)
	if len(impacts) == 0 || impacts[0].Intensitat != 4 {
		t.Fatalf("impactes no actualitzats després d'aprovar")
	}

	submitEventUpdate(t, app, editorSession, eventID, "Event v2", "guerra", "1902-01-01", "Resum v2", "municipi", munID, "directe", 1)
	changeID2 := findPendingWikiChangeID(t, database, "event_historic", eventID)
	if changeID2 == 0 {
		t.Fatalf("no s'ha creat el segon canvi wiki pendent")
	}
	if changeID2 == changeID1 {
		t.Fatalf("esperava un segon canvi wiki")
	}
	approveWikiChange(t, app, moderatorSession, changeID2, "event_historic_canvi")

	stored, _ = database.GetEventHistoric(eventID)
	if stored == nil || stored.Titol != "Event v2" {
		t.Fatalf("esperava títol v2, got %q", stored.Titol)
	}

	revertID := submitEventRevert(t, app, editorSession, eventID, changeID1)
	if revertID == 0 {
		t.Fatalf("no s'ha creat la proposta de revert")
	}
	approveWikiChange(t, app, moderatorSession, revertID, "event_historic_canvi")

	stored, _ = database.GetEventHistoric(eventID)
	if stored == nil || stored.Titol != "Event v1" {
		t.Fatalf("esperava revert a v1, got %q", stored.Titol)
	}
	impacts, _ = database.ListEventImpacts(eventID)
	if len(impacts) == 0 || impacts[0].Intensitat != 4 {
		t.Fatalf("impactes no restaurats després de revert")
	}
}

func TestEventWikiMarks(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f19_events_marks.sqlite3")

	user := createTestUser(t, database, "event_mark_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	session := createSessionCookie(t, database, user.ID, "sess_event_mark_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	munID := createHistoriaMunicipi(t, database, user.ID)
	eventID := createPublicEventWithImpact(t, database, user.ID, "Event marks", "guerra", "1899-01-01", "", "municipi", munID, "directe", 3)

	csrf := "csrf_event_mark_private"
	form := url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("type", "interes")
	form.Set("public", "0")
	req := httptest.NewRequest(http.MethodPost, "/historia/events/"+strconv.Itoa(eventID)+"/marcar", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()

	app.EventHistoricWikiMark(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("mark private expected 200, got %d", rr.Code)
	}
	counts, _ := database.GetWikiPublicCounts("event_historic", eventID)
	if counts["interes"] != 0 {
		t.Fatalf("marca privada no hauria d'incrementar stats")
	}

	csrf = "csrf_event_mark_public"
	form = url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("type", "interes")
	form.Set("public", "1")
	req = httptest.NewRequest(http.MethodPost, "/historia/events/"+strconv.Itoa(eventID)+"/marcar", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr = httptest.NewRecorder()

	app.EventHistoricWikiMark(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("mark public expected 200, got %d", rr.Code)
	}
	counts, _ = database.GetWikiPublicCounts("event_historic", eventID)
	if counts["interes"] != 1 {
		t.Fatalf("marca pública hauria de sumar 1")
	}

	csrf = "csrf_event_mark_public_again"
	form = url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("type", "interes")
	form.Set("public", "1")
	req = httptest.NewRequest(http.MethodPost, "/historia/events/"+strconv.Itoa(eventID)+"/marcar", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr = httptest.NewRecorder()

	app.EventHistoricWikiMark(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("mark public again expected 200, got %d", rr.Code)
	}
	counts, _ = database.GetWikiPublicCounts("event_historic", eventID)
	if counts["interes"] != 1 {
		t.Fatalf("marca pública duplicada no hauria de sumar")
	}

	csrf = "csrf_event_unmark"
	form = url.Values{}
	form.Set("csrf_token", csrf)
	req = httptest.NewRequest(http.MethodPost, "/historia/events/"+strconv.Itoa(eventID)+"/desmarcar", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr = httptest.NewRecorder()

	app.EventHistoricWikiUnmark(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("unmark expected 200, got %d", rr.Code)
	}
	counts, _ = database.GetWikiPublicCounts("event_historic", eventID)
	if counts["interes"] != 0 {
		t.Fatalf("unmark hauria de deixar stats a 0")
	}

	csrf = "csrf_event_unmark_again"
	form = url.Values{}
	form.Set("csrf_token", csrf)
	req = httptest.NewRequest(http.MethodPost, "/historia/events/"+strconv.Itoa(eventID)+"/desmarcar", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr = httptest.NewRecorder()

	app.EventHistoricWikiUnmark(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("unmark again expected 200, got %d", rr.Code)
	}
	counts, _ = database.GetWikiPublicCounts("event_historic", eventID)
	if counts["interes"] != 0 {
		t.Fatalf("unmark duplicat no hauria de canviar stats")
	}
}

func submitEventForm(t *testing.T, app *core.App, session *http.Cookie, title, typ, start, scopeType string, scopeID int, impactType string, intensity int) int {
	t.Helper()
	csrf := "csrf_event_create_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	form := url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("titol", title)
	form.Set("tipus", typ)
	form.Set("resum", "Resum test")
	form.Set("descripcio", "Descripcio test")
	if start != "" {
		form.Set("data_inici", start)
	}
	form.Set("precisio", "dia")
	form.Set("impact_scope_type", scopeType)
	form.Set("impact_scope_id", strconv.Itoa(scopeID))
	form.Set("impact_type", impactType)
	form.Set("impact_intensitat", strconv.Itoa(intensity))
	form.Set("impact_notes", "notes")

	req := httptest.NewRequest(http.MethodPost, "/historia/events/nou", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()

	app.EventHistoricCreate(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("EventHistoricCreate expected 303, got %d", rr.Code)
	}
	location := rr.Result().Header.Get("Location")
	id := parseEventIDFromLocation(t, location)
	if id == 0 {
		t.Fatalf("id no vàlid en Location %q", location)
	}
	return id
}

func submitEventUpdate(t *testing.T, app *core.App, session *http.Cookie, eventID int, title, typ, start, summary, scopeType string, scopeID int, impactType string, intensity int) {
	t.Helper()
	csrf := "csrf_event_update_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	form := url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("titol", title)
	form.Set("tipus", typ)
	form.Set("resum", summary)
	form.Set("descripcio", summary+" desc")
	if start != "" {
		form.Set("data_inici", start)
	}
	form.Set("precisio", "dia")
	form.Set("impact_scope_type", scopeType)
	form.Set("impact_scope_id", strconv.Itoa(scopeID))
	form.Set("impact_type", impactType)
	form.Set("impact_intensitat", strconv.Itoa(intensity))
	form.Set("impact_notes", "notes")

	req := httptest.NewRequest(http.MethodPost, "/historia/events/"+strconv.Itoa(eventID)+"/editar", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()

	app.EventHistoricUpdate(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("EventHistoricUpdate expected 303, got %d", rr.Code)
	}
}

func submitEventRevert(t *testing.T, app *core.App, session *http.Cookie, eventID, changeID int) int {
	t.Helper()
	csrf := "csrf_event_revert_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	form := url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("change_id", strconv.Itoa(changeID))
	form.Set("reason", "revert")

	req := httptest.NewRequest(http.MethodPost, "/historia/events/"+strconv.Itoa(eventID)+"/historial/revert", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()

	app.EventHistoricWikiRevert(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("revert expected 303, got %d", rr.Code)
	}

	changes, err := app.DB.ListWikiChanges("event_historic", eventID)
	if err != nil {
		t.Fatalf("ListWikiChanges revert ha fallat: %v", err)
	}
	for _, ch := range changes {
		if ch.ChangeType == "revert" && ch.ModeracioEstat == "pendent" {
			return ch.ID
		}
	}
	return 0
}

func fetchEventIDsFromAPI(t *testing.T, app *core.App, path string) []int {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rr := httptest.NewRecorder()
	app.EventsAPI(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("EventsAPI expected 200, got %d", rr.Code)
	}
	var resp eventsListResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("no puc parsejar resposta EventsAPI: %v", err)
	}
	ids := make([]int, 0, len(resp.Items))
	for _, item := range resp.Items {
		ids = append(ids, item.ID)
	}
	return ids
}

func containsEventID(ids []int, target int) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}

func parseEventIDFromLocation(t *testing.T, location string) int {
	t.Helper()
	u, err := url.Parse(location)
	if err != nil {
		t.Fatalf("invalid redirect location: %v", err)
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) == 0 {
		t.Fatalf("invalid redirect path: %q", u.Path)
	}
	idStr := parts[len(parts)-1]
	id, err := strconv.Atoi(idStr)
	if err != nil || id <= 0 {
		t.Fatalf("invalid event id in redirect: %s", idStr)
	}
	return id
}

func createPublicEventWithImpact(t *testing.T, database db.DB, userID int, title, typ, start, end, scopeType string, scopeID int, impactType string, intensity int) int {
	t.Helper()
	precision := ""
	if start != "" || end != "" {
		precision = "dia"
	}
	event := &db.EventHistoric{
		Titol:            title,
		Slug:             fmt.Sprintf("event-%d-%d", time.Now().UnixNano(), scopeID),
		Tipus:            typ,
		Resum:            "resum",
		Descripcio:       "desc",
		DataInici:        start,
		DataFi:           end,
		Precisio:         precision,
		CreatedBy:        sql.NullInt64{Int64: int64(userID), Valid: true},
		ModerationStatus: "publicat",
	}
	eventID, err := database.CreateEventHistoric(event)
	if err != nil {
		t.Fatalf("CreateEventHistoric ha fallat: %v", err)
	}
	impact := db.EventHistoricImpact{
		EventID:      eventID,
		ScopeType:    scopeType,
		ScopeID:      scopeID,
		ImpacteTipus: impactType,
		Intensitat:   intensity,
		CreatedBy:    sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	if err := database.ReplaceEventImpacts(eventID, []db.EventHistoricImpact{impact}); err != nil {
		t.Fatalf("ReplaceEventImpacts ha fallat: %v", err)
	}
	return eventID
}

func createTestArquebisbat(t *testing.T, database db.DB, userID int) int {
	t.Helper()
	arch := &db.Arquebisbat{
		Nom:            fmt.Sprintf("Bisbat Test %d", time.Now().UnixNano()),
		TipusEntitat:   "bisbat",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	id, err := database.CreateArquebisbat(arch)
	if err != nil {
		t.Fatalf("CreateArquebisbat ha fallat: %v", err)
	}
	return id
}

func findPendingWikiChangeID(t *testing.T, database db.DB, objectType string, objectID int) int {
	t.Helper()
	changes, err := database.ListWikiChanges(objectType, objectID)
	if err != nil {
		t.Fatalf("ListWikiChanges ha fallat: %v", err)
	}
	for _, ch := range changes {
		if ch.ModeracioEstat == "pendent" {
			return ch.ID
		}
	}
	return 0
}

func checkEventHistoryVisibility(t *testing.T, app *core.App, eventID int, session *http.Cookie, changeID int, shouldSee bool) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/historia/events/"+strconv.Itoa(eventID)+"/historial", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()

	app.EventHistoricWikiHistory(rr, req)
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
