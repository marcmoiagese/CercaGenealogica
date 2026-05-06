package integration

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF353Z2ConfessionalFormsUseProfileLabelsAndHideModeration(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z2_forms.sqlite3")
	session := f353YAdminSession(t, database, "forms")
	parentName := "Diocesi F35-3Z2 formulari " + strconv.FormatInt(time.Now().UnixNano(), 10)
	f353Z2CreateEntitat(t, database, parentName, "f35_3z2_parent_form", "bisbat_diocesi", "publicat")

	entityBody := f353YGet(t, app.AdminNewConfessional, "/confessional/entitats/new", session)
	for _, want := range []string{"Entitat pare opcional", "Parroquia", parentName} {
		if !strings.Contains(entityBody, want) {
			t.Fatalf("entity form missing %q; body=%s", want, entityBody)
		}
	}
	for _, banned := range []string{"estat-moderacio", "Parent entity", "CanonicalName"} {
		if strings.Contains(entityBody, banned) {
			t.Fatalf("entity form should not contain %q; body=%s", banned, entityBody)
		}
	}

	relationBody := f353YGet(t, app.AdminNewConfessional, "/confessional/relacions-entitats/new", session)
	for _, banned := range []string{`name="tipus_relacio"`, "jerarquia"} {
		if strings.Contains(relationBody, banned) {
			t.Fatalf("relation form should not contain %q; body=%s", banned, relationBody)
		}
	}
	if !strings.Contains(relationBody, "Es dedueix del nivell de l&#39;entitat filla.") && !strings.Contains(relationBody, "Es dedueix del nivell de l'entitat filla.") {
		t.Fatalf("relation form must explain the derived type; body=%s", relationBody)
	}
}

func TestF353Z2CreateEntityWithParentCreatesPendingTypedRelation(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z2_create_parent.sqlite3")
	user, session := f353ZAdminUserSession(t, database, "create_parent")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	parentID := f353Z2CreateEntitat(t, database, "Arquebisbat F35-3Z2 "+suffix, "f35_3z2_parent_"+suffix, "arquebisbat_arxidiocesi", "publicat")

	childName := "Arxiprestat F35-3Z2 " + suffix
	form := f353ZEntityForm(childName, "f35_3z2_child_"+suffix, 0)
	form.Set("nivell_confessional_codi", "arxiprestat_vicariat_forani")
	form.Set("parent_id", strconv.Itoa(parentID))
	rr := f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, session, form)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("create status=%d", rr.Code)
	}

	child := f353ZFindEntitatByName(t, database, childName)
	if child.ModeracioEstat != "pendent" || !child.ParentID.Valid || int(child.ParentID.Int64) != parentID {
		t.Fatalf("child metadata unexpected: %+v", child)
	}

	rels, err := database.ListEntitatReligiosaRelacions()
	if err != nil {
		t.Fatalf("ListEntitatReligiosaRelacions: %v", err)
	}
	var found *db.EntitatReligiosaRelacio
	for i := range rels {
		if rels[i].EntitatOrigenID == parentID && rels[i].EntitatDestiID == child.ID {
			found = &rels[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("created parent relation not found: %+v", rels)
	}
	if found.TipusRelacio != "arxiprestat_vicariat_forani" || found.ModeracioEstat != "pendent" {
		t.Fatalf("relation unexpected: %+v", found)
	}
	if !found.CreatedBy.Valid || int(found.CreatedBy.Int64) != user.ID {
		t.Fatalf("relation author=%v, want %d", found.CreatedBy, user.ID)
	}
}

func TestF353Z2ProfileOptionsAndSplitRelations(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z2_profile.sqlite3")
	user, session := f353ZAdminUserSession(t, database, "profile")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	parentID := f353Z2CreateEntitat(t, database, "Diocesi F35-3Z2 "+suffix, "f35_3z2_sup_"+suffix, "bisbat_diocesi", "publicat")
	childID := f353Z2CreateEntitat(t, database, "Parroquia F35-3Z2 "+suffix, "f35_3z2_child_"+suffix, "parroquia", "publicat")
	grandChildID := f353Z2CreateEntitat(t, database, "Lloc F35-3Z2 "+suffix, "f35_3z2_inf_"+suffix, "lloc_de_culte", "publicat")
	_, err := database.SaveEntitatReligiosaRelacio(&db.EntitatReligiosaRelacio{EntitatOrigenID: parentID, EntitatDestiID: childID, TipusRelacio: "parroquia", ModeracioEstat: "publicat", CreatedBy: sql.NullInt64{Int64: int64(user.ID), Valid: true}})
	if err != nil {
		t.Fatalf("Save parent relation: %v", err)
	}
	_, err = database.SaveEntitatReligiosaRelacio(&db.EntitatReligiosaRelacio{EntitatOrigenID: childID, EntitatDestiID: grandChildID, TipusRelacio: "lloc_de_culte", ModeracioEstat: "publicat", CreatedBy: sql.NullInt64{Int64: int64(user.ID), Valid: true}})
	if err != nil {
		t.Fatalf("Save child relation: %v", err)
	}

	body := f353YGet(t, app.AdminConfessionalEntityShow, "/confessional/entitats/"+strconv.Itoa(childID), session)
	for _, want := range []string{"opcions-dropdown", "dropdownOpcions", "/history", "Entitats superiors", "Entitats inferiors", "Parroquia", "Lloc de culte"} {
		if !strings.Contains(body, want) {
			t.Fatalf("profile missing %q; body=%s", want, body)
		}
	}
	for _, banned := range []string{"wiki.history.no_changes", "history?view=", "<th>ID</th>"} {
		if strings.Contains(body, banned) {
			t.Fatalf("profile should not contain embedded history marker %q; body=%s", banned, body)
		}
	}
}

func TestF353Z2AdvancedRelationDeducesTypeAndBlocksSelf(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z2_relation_type.sqlite3")
	session := f353YAdminSession(t, database, "relation_type")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	parentID := f353Z2CreateEntitat(t, database, "Diocesi F35-3Z2 rel "+suffix, "f35_3z2_rel_parent_"+suffix, "bisbat_diocesi", "publicat")
	childID := f353Z2CreateEntitat(t, database, "Parroquia F35-3Z2 rel "+suffix, "f35_3z2_rel_child_"+suffix, "parroquia", "publicat")

	form := url.Values{}
	form.Set("kind", "rel_ent")
	form.Set("entitat_origen_id", strconv.Itoa(parentID))
	form.Set("entitat_desti_id", strconv.Itoa(childID))
	rr := f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, session, form)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("relation create status=%d", rr.Code)
	}
	rels, err := database.ListEntitatReligiosaRelacions()
	if err != nil {
		t.Fatalf("ListEntitatReligiosaRelacions: %v", err)
	}
	if len(rels) != 1 || rels[0].TipusRelacio != "parroquia" {
		t.Fatalf("deduced relation type unexpected: %+v", rels)
	}

	self := url.Values{}
	self.Set("kind", "rel_ent")
	self.Set("entitat_origen_id", strconv.Itoa(parentID))
	self.Set("entitat_desti_id", strconv.Itoa(parentID))
	body := f353YPostConfessional(t, app.AdminSaveConfessional, session, self)
	if !strings.Contains(body, "no poden ser la mateixa") {
		t.Fatalf("self relation must be blocked; body=%s", body)
	}
}

func f353Z2CreateEntitat(t *testing.T, database db.DB, name, code, level, status string) int {
	t.Helper()
	id, err := database.SaveEntitatReligiosa(&db.EntitatReligiosa{
		Codi:                   code,
		Nom:                    name,
		ReligioConfessioCodi:   "catolicisme_ritu_llati",
		NivellConfessionalCodi: level,
		Estat:                  "actiu",
		ModeracioEstat:         status,
	})
	if err != nil {
		t.Fatalf("SaveEntitatReligiosa: %v", err)
	}
	return id
}

func f353Z2PostConfessionalNoRedirect(t *testing.T, handler http.HandlerFunc, session *http.Cookie, form url.Values) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/confessional/save", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	handler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("POST confessional status=%d body=%s", rr.Code, rr.Body.String())
	}
	return rr.Body.String()
}
