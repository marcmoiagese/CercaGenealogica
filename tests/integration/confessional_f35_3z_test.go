package integration

import (
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

func TestF353ZConfessionalAuthorshipAndModerationMetadata(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z_authorship.sqlite3")
	author, authorSession := f353ZAdminUserSession(t, database, "author")
	moderator, moderatorSession := f353ZAdminUserSession(t, database, "moderator")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	name := "Entitat F35-3Z autoria " + suffix
	form := f353ZEntityForm(name, "f35_3z_auth_"+suffix, 0)
	rr := f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, authorSession, form)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("create status=%d", rr.Code)
	}
	entitat := f353ZFindEntitatByName(t, database, name)
	if entitat.ModeracioEstat != "pendent" {
		t.Fatalf("new status=%q, want pendent", entitat.ModeracioEstat)
	}
	if !entitat.CreatedBy.Valid || int(entitat.CreatedBy.Int64) != author.ID {
		t.Fatalf("created_by=%v, want author %d", entitat.CreatedBy, author.ID)
	}

	body := f353YGet(t, app.AdminModeracioList, "/moderacio?type=entitat_religiosa", moderatorSession)
	if !strings.Contains(body, name) || !strings.Contains(body, author.Usuari) {
		t.Fatalf("/moderacio ha de mostrar nom i autor; body=%s", body)
	}

	f353YPostModeracio(t, app.AdminModeracioAprovar, moderatorSession, entitat.ID, "entitat_religiosa", "aprovar", "")
	approved, err := database.GetEntitatReligiosa(entitat.ID)
	if err != nil {
		t.Fatalf("GetEntitatReligiosa approved: %v", err)
	}
	if approved.ModeracioEstat != "publicat" || !approved.CreatedBy.Valid || int(approved.CreatedBy.Int64) != author.ID {
		t.Fatalf("approved metadata unexpected: %+v", approved)
	}
	if !approved.ModeratedBy.Valid || int(approved.ModeratedBy.Int64) != moderator.ID || !approved.ModeratedAt.Valid {
		t.Fatalf("moderation metadata missing: moderated_by=%v moderated_at=%v", approved.ModeratedBy, approved.ModeratedAt)
	}

	rejectedName := "Entitat F35-3Z rebutjada " + suffix
	_ = f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, authorSession, f353ZEntityForm(rejectedName, "f35_3z_rej_"+suffix, 0))
	rejected := f353ZFindEntitatByName(t, database, rejectedName)
	f353YPostModeracio(t, app.AdminModeracioRebutjar, moderatorSession, rejected.ID, "entitat_religiosa", "rebutjar", "duplicada")
	rejectedAfter, err := database.GetEntitatReligiosa(rejected.ID)
	if err != nil {
		t.Fatalf("GetEntitatReligiosa rejected: %v", err)
	}
	if rejectedAfter.ModeracioEstat != "rebutjat" || rejectedAfter.ModeracioMotiu != "duplicada" {
		t.Fatalf("reject metadata unexpected: %+v", rejectedAfter)
	}
	if !rejectedAfter.CreatedBy.Valid || int(rejectedAfter.CreatedBy.Int64) != author.ID {
		t.Fatalf("reject lost author: %+v", rejectedAfter.CreatedBy)
	}
}

func TestF353Z4EntitatInitialWikiVersionLifecycle(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z4_initial_wiki.sqlite3")
	author, authorSession := f353ZAdminUserSession(t, database, "z4_author")
	moderator, moderatorSession := f353ZAdminUserSession(t, database, "z4_moderator")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	name := "Entitat F35-3Z4 inicial " + suffix
	_ = f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, authorSession, f353ZEntityForm(name, "f35_3z4_init_"+suffix, 0))
	entitat := f353ZFindEntitatByName(t, database, name)
	if got := f353Z4PublishedWikiCount(t, database, entitat.ID); got != 0 {
		t.Fatalf("l'entitat pendent no ha de tenir historial publicat, got=%d", got)
	}

	f353YPostModeracio(t, app.AdminModeracioAprovar, moderatorSession, entitat.ID, "entitat_religiosa", "aprovar", "")
	published := f353Z4PublishedWikiChanges(t, database, entitat.ID)
	if len(published) != 1 {
		t.Fatalf("la primera publicacio ha de crear v1, got=%d changes=%+v", len(published), published)
	}
	initial := published[0]
	if initial.ChangeType != "create" || initial.FieldKey != "*" || initial.ModeracioEstat != "publicat" {
		t.Fatalf("v1 inesperada: %+v", initial)
	}
	if !initial.ChangedBy.Valid || int(initial.ChangedBy.Int64) != author.ID {
		t.Fatalf("v1 changed_by=%v, want author %d", initial.ChangedBy, author.ID)
	}
	if !initial.ModeratedBy.Valid || int(initial.ModeratedBy.Int64) != moderator.ID || !initial.ModeratedAt.Valid {
		t.Fatalf("v1 moderation metadata missing: %+v", initial)
	}
	var meta struct {
		After json.RawMessage `json:"after"`
	}
	if err := json.Unmarshal([]byte(initial.Metadata), &meta); err != nil || !strings.Contains(string(meta.After), name) {
		t.Fatalf("v1 metadata after missing entity: err=%v metadata=%s", err, initial.Metadata)
	}

	f353YPostModeracio(t, app.AdminModeracioAprovar, moderatorSession, entitat.ID, "entitat_religiosa", "aprovar", "")
	if got := f353Z4PublishedWikiCount(t, database, entitat.ID); got != 1 {
		t.Fatalf("aprovar dues vegades no ha de duplicar v1, got=%d", got)
	}
	historyBody := f353YGet(t, app.EntitatReligiosaWikiHistory, "/confessional/entitats/"+strconv.Itoa(entitat.ID)+"/history", authorSession)
	if !strings.Contains(historyBody, "#1") || !strings.Contains(historyBody, "create") {
		t.Fatalf("historial ha de mostrar la versio inicial #1, body=%s", historyBody)
	}

	newName := "Entitat F35-3Z4 versio 2 " + suffix
	_ = f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, authorSession, f353ZEntityForm(newName, "f35_3z4_init_"+suffix, entitat.ID))
	current, err := database.GetEntitatReligiosa(entitat.ID)
	if err != nil {
		t.Fatalf("GetEntitatReligiosa current: %v", err)
	}
	if current.Nom == newName {
		t.Fatalf("la proposta pendent no ha de sobreescriure la fitxa publicada")
	}
	pending := f353Z4LatestPendingWikiChange(t, database, entitat.ID)
	f353YPostModeracio(t, app.AdminModeracioAprovar, moderatorSession, pending.ID, "entitat_religiosa_canvi", "aprovar", "")
	approved, err := database.GetEntitatReligiosa(entitat.ID)
	if err != nil {
		t.Fatalf("GetEntitatReligiosa approved v2: %v", err)
	}
	if approved.Nom != newName {
		t.Fatalf("approved name=%q, want %q", approved.Nom, newName)
	}
	if got := f353Z4PublishedWikiCount(t, database, entitat.ID); got != 2 {
		t.Fatalf("aprovar proposta ha de crear v2, got=%d", got)
	}

	rejectName := "Entitat F35-3Z4 rebutjada " + suffix
	_ = f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, authorSession, f353ZEntityForm(rejectName, "f35_3z4_init_"+suffix, entitat.ID))
	rejected := f353Z4LatestPendingWikiChange(t, database, entitat.ID)
	f353YPostModeracio(t, app.AdminModeracioRebutjar, moderatorSession, rejected.ID, "entitat_religiosa_canvi", "rebutjar", "descartada")
	unchanged, err := database.GetEntitatReligiosa(entitat.ID)
	if err != nil {
		t.Fatalf("GetEntitatReligiosa rejected proposal: %v", err)
	}
	if unchanged.Nom != newName {
		t.Fatalf("rebutjar proposta no ha de canviar el publicat, got=%q want=%q", unchanged.Nom, newName)
	}
	if got := f353Z4PublishedWikiCount(t, database, entitat.ID); got != 2 {
		t.Fatalf("rebutjar proposta no ha de crear versio publicada, got=%d", got)
	}
}

func TestF353ZPublishedEntityEditCreatesWikiProposal(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z_wiki_edit.sqlite3")
	editor, editorSession := f353ZAdminUserSession(t, database, "editor")
	_, moderatorSession := f353ZAdminUserSession(t, database, "moderator")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	id := f353YCreateEntitat(t, database, "Entitat F35-3Z publicada "+suffix, "publicat")

	newName := "Entitat F35-3Z proposada " + suffix
	form := f353ZEntityForm(newName, "f35_3z_pub_"+suffix, id)
	rr := f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, editorSession, form)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("edit status=%d", rr.Code)
	}
	current, err := database.GetEntitatReligiosa(id)
	if err != nil {
		t.Fatalf("GetEntitatReligiosa current: %v", err)
	}
	if current.Nom == newName {
		t.Fatalf("l'edicio publicada no ha de sobreescriure directament")
	}
	changes, err := database.ListWikiChanges("entitat_religiosa", id)
	if err != nil {
		t.Fatalf("ListWikiChanges: %v", err)
	}
	if len(changes) != 1 || changes[0].ModeracioEstat != "pendent" {
		t.Fatalf("wiki changes unexpected: %+v", changes)
	}
	if !changes[0].ChangedBy.Valid || int(changes[0].ChangedBy.Int64) != editor.ID {
		t.Fatalf("changed_by=%v, want editor", changes[0].ChangedBy)
	}
	var meta struct {
		After json.RawMessage `json:"after"`
	}
	if err := json.Unmarshal([]byte(changes[0].Metadata), &meta); err != nil || !strings.Contains(string(meta.After), newName) {
		t.Fatalf("metadata after missing proposed name: err=%v metadata=%s", err, changes[0].Metadata)
	}
	body := f353YGet(t, app.AdminModeracioList, "/moderacio?type=entitat_religiosa_canvi", moderatorSession)
	if !strings.Contains(body, newName) && !strings.Contains(body, "entitat_religiosa") {
		t.Fatalf("/moderacio ha de mostrar la proposta wiki, body=%s", body)
	}
	f353YPostModeracio(t, app.AdminModeracioAprovar, moderatorSession, changes[0].ID, "entitat_religiosa_canvi", "aprovar", "")
	approved, err := database.GetEntitatReligiosa(id)
	if err != nil {
		t.Fatalf("GetEntitatReligiosa approved change: %v", err)
	}
	if approved.Nom != newName {
		t.Fatalf("approved name=%q, want %q", approved.Nom, newName)
	}

	rejectName := "Entitat F35-3Z rebutjar canvi " + suffix
	_ = f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, editorSession, f353ZEntityForm(rejectName, "f35_3z_pub_"+suffix, id))
	changes, _ = database.ListWikiChanges("entitat_religiosa", id)
	last := changes[0]
	for _, ch := range changes {
		if ch.ID > last.ID {
			last = ch
		}
	}
	f353YPostModeracio(t, app.AdminModeracioRebutjar, moderatorSession, last.ID, "entitat_religiosa_canvi", "rebutjar", "no valid")
	unchanged, _ := database.GetEntitatReligiosa(id)
	if unchanged.Nom != newName {
		t.Fatalf("rejected proposal must not change published row: got=%q want=%q", unchanged.Nom, newName)
	}
}

func TestF353ZEntityProfileShowsRelationsAndHistory(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z_profile.sqlite3")
	user, session := f353ZAdminUserSession(t, database, "profile")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	parentID := f353YCreateEntitat(t, database, "Diocesi F35-3Z "+suffix, "publicat")
	childID := f353YCreateEntitat(t, database, "Parroquia F35-3Z "+suffix, "publicat")
	municipiID := f353YCreateMunicipi(t, database, "Municipi F35-3Z "+suffix)
	_, err := database.SaveEntitatReligiosaRelacio(&db.EntitatReligiosaRelacio{
		EntitatOrigenID: parentID,
		EntitatDestiID:  childID,
		TipusRelacio:    "jerarquia_f35_3z",
		ModeracioEstat:  "publicat",
		CreatedBy:       sql.NullInt64{Int64: int64(user.ID), Valid: true},
	})
	if err != nil {
		t.Fatalf("SaveEntitatReligiosaRelacio: %v", err)
	}
	_, err = database.SaveMunicipiEntitatReligiosa(&db.MunicipiEntitatReligiosa{
		MunicipiID:         municipiID,
		EntitatReligiosaID: childID,
		TipusRelacio:       "territorial_f35_3z",
		ModeracioEstat:     "publicat",
		CreatedBy:          sql.NullInt64{Int64: int64(user.ID), Valid: true},
	})
	if err != nil {
		t.Fatalf("SaveMunicipiEntitatReligiosa: %v", err)
	}
	_, err = database.CreateWikiChange(&db.WikiChange{
		ObjectType:     "entitat_religiosa",
		ObjectID:       childID,
		ChangeType:     "update",
		FieldKey:       "Nom",
		OldValue:       "old",
		NewValue:       "new",
		ModeracioEstat: "pendent",
		ChangedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
	})
	if err != nil {
		t.Fatalf("CreateWikiChange: %v", err)
	}

	listBody := f353YGet(t, app.AdminConfessionalNavigation, "/confessional/navegacio?q=Parroquia%20F35-3Z", session)
	if !strings.Contains(listBody, "/confessional/entitats/"+strconv.Itoa(childID)) {
		t.Fatalf("la cerca global de la navegacio ha d'enllacar a la fitxa")
	}
	profileBody := f353YGet(t, app.AdminConfessionalEntityShow, "/confessional/entitats/"+strconv.Itoa(childID), session)
	for _, want := range []string{"Parroquia F35-3Z", "jerarquia_f35_3z", "territorial_f35_3z", user.Usuari, "pending"} {
		if !strings.Contains(profileBody, want) {
			t.Fatalf("profile missing %q; body=%s", want, profileBody)
		}
	}
	historyBody := f353YGet(t, app.EntitatReligiosaWikiHistory, "/confessional/entitats/"+strconv.Itoa(childID)+"/history", session)
	if !strings.Contains(historyBody, "Parroquia F35-3Z") || !strings.Contains(historyBody, user.Usuari) {
		t.Fatalf("history missing entity/user; body=%s", historyBody)
	}
}

func f353ZAdminUserSession(t *testing.T, database db.DB, label string) (*db.User, *http.Cookie) {
	t.Helper()
	user := createTestUser(t, database, "f35_3z_"+label+"_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	assignPolicyByName(t, database, user.ID, "admin")
	return user, createSessionCookie(t, database, user.ID, "sess_f35_3z_"+label+"_"+strconv.FormatInt(time.Now().UnixNano(), 10))
}

func f353ZEntityForm(name, code string, id int) url.Values {
	form := url.Values{}
	form.Set("kind", "entitat")
	if id > 0 {
		form.Set("id", strconv.Itoa(id))
	}
	form.Set("nom", name)
	form.Set("codi", code)
	form.Set("religio_confessio_codi", "catolicisme_ritu_llati")
	form.Set("nivell_confessional_codi", "parroquia")
	form.Set("estat", "actiu")
	return form
}

func f353ZPostConfessionalRedirect(t *testing.T, handler http.HandlerFunc, session *http.Cookie, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/confessional/save", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	handler(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("POST confessional status=%d body=%s", rr.Code, rr.Body.String())
	}
	return rr
}

func f353ZFindEntitatByName(t *testing.T, database db.DB, name string) db.EntitatReligiosa {
	t.Helper()
	items, err := database.ListEntitatsReligioses()
	if err != nil {
		t.Fatalf("ListEntitatsReligioses: %v", err)
	}
	for _, item := range items {
		if item.Nom == name {
			return item
		}
	}
	t.Fatalf("entitat %q not found", name)
	return db.EntitatReligiosa{}
}

func f353Z4PublishedWikiChanges(t *testing.T, database db.DB, entitatID int) []db.WikiChange {
	t.Helper()
	changes, err := database.ListWikiChanges("entitat_religiosa", entitatID)
	if err != nil {
		t.Fatalf("ListWikiChanges entitat=%d: %v", entitatID, err)
	}
	published := make([]db.WikiChange, 0, len(changes))
	for _, ch := range changes {
		if ch.ModeracioEstat == "publicat" {
			published = append(published, ch)
		}
	}
	return published
}

func f353Z4PublishedWikiCount(t *testing.T, database db.DB, entitatID int) int {
	t.Helper()
	return len(f353Z4PublishedWikiChanges(t, database, entitatID))
}

func f353Z4LatestPendingWikiChange(t *testing.T, database db.DB, entitatID int) db.WikiChange {
	t.Helper()
	changes, err := database.ListWikiChanges("entitat_religiosa", entitatID)
	if err != nil {
		t.Fatalf("ListWikiChanges entitat=%d: %v", entitatID, err)
	}
	for _, ch := range changes {
		if ch.ModeracioEstat == "pendent" {
			return ch
		}
	}
	t.Fatalf("no pending wiki change for entitat=%d; changes=%+v", entitatID, changes)
	return db.WikiChange{}
}
