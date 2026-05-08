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
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF353Z14GroupedApprovalWithInvalidParentLeavesEntityAndRelationPending(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z14_invalid_parent.sqlite3")
	user, session := f353ZAdminUserSession(t, database, "z14_invalid_parent")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	parentID := f353Z8SaveEntity(t, database, "z14_parent_"+suffix, "Arquebisbat F35-3Z14 "+suffix, "arquebisbat_arxidiocesi", "publicat")
	childName := "Arxiprestat pendent consistencia F35-3Z14 " + suffix
	form := f353ZEntityForm(childName, "z14_child_"+suffix, 0)
	form.Set("nivell_confessional_codi", "arxiprestat_vicariat_forani")
	form.Set("parent_id", strconv.Itoa(parentID))
	_ = f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, session, form)

	child := f353ZFindEntitatByName(t, database, childName)
	rel := f353Z14FindRelation(t, database, parentID, child.ID)
	if rel == nil || rel.ModeracioEstat != "pendent" {
		t.Fatalf("precondicio relacio pendent: %+v", rel)
	}
	if err := database.UpdateEntitatReligiosaModeracio(parentID, "rebutjat", "pare retirat", user.ID); err != nil {
		t.Fatalf("UpdateEntitatReligiosaModeracio parent: %v", err)
	}

	location := f353Z14PostModeracioLocation(t, app.AdminModeracioAprovar, session, child.ID, "entitat_religiosa", "aprovar", "")
	if !strings.Contains(location, "err=1") {
		t.Fatalf("aprovar amb pare no publicat ha de tornar error, location=%s", location)
	}

	childAfter, err := database.GetEntitatReligiosa(child.ID)
	if err != nil {
		t.Fatalf("GetEntitatReligiosa child after: %v", err)
	}
	if childAfter.ModeracioEstat != "pendent" {
		t.Fatalf("l'entitat ha de continuar pendent si falla la validacio agrupada: %+v", childAfter)
	}
	relAfter := f353Z14FindRelation(t, database, parentID, child.ID)
	if relAfter == nil || relAfter.ModeracioEstat != "pendent" {
		t.Fatalf("la relacio dependent ha de continuar pendent si falla la validacio agrupada: %+v", relAfter)
	}
	changes, err := database.ListWikiChanges("entitat_religiosa", child.ID)
	if err != nil {
		t.Fatalf("ListWikiChanges: %v", err)
	}
	if len(changes) != 0 {
		t.Fatalf("no s'ha de crear wiki v1 si no s'aprova l'entitat: %+v", changes)
	}
}

func TestF353Z14DuplicateInitialParentRelationsBlockGroupedModerationWithoutPartialState(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z14_duplicate_parent.sqlite3")
	user, session := f353ZAdminUserSession(t, database, "z14_duplicate_parent")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	parentID := f353Z8SaveEntity(t, database, "z14_dup_parent_a_"+suffix, "Arquebisbat A F35-3Z14 "+suffix, "arquebisbat_arxidiocesi", "publicat")
	otherParentID := f353Z8SaveEntity(t, database, "z14_dup_parent_b_"+suffix, "Arquebisbat B F35-3Z14 "+suffix, "arquebisbat_arxidiocesi", "publicat")
	childName := "Arxiprestat duplicat F35-3Z14 " + suffix
	form := f353ZEntityForm(childName, "z14_dup_child_"+suffix, 0)
	form.Set("nivell_confessional_codi", "arxiprestat_vicariat_forani")
	form.Set("parent_id", strconv.Itoa(parentID))
	_ = f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, session, form)

	child := f353ZFindEntitatByName(t, database, childName)
	_, err := database.SaveEntitatReligiosaRelacio(&db.EntitatReligiosaRelacio{
		EntitatOrigenID: otherParentID,
		EntitatDestiID:  child.ID,
		TipusRelacio:    "arxiprestat_vicariat_forani",
		ModeracioEstat:  "pendent",
		CreatedBy:       sql.NullInt64{Int64: int64(user.ID), Valid: true},
	})
	if err != nil {
		t.Fatalf("SaveEntitatReligiosaRelacio duplicate: %v", err)
	}

	entityBody := f353YGet(t, app.AdminModeracioList, "/moderacio?type=entitat_religiosa", session)
	if !strings.Contains(entityBody, "AVIS: multiples relacions pare/filla inicials pendents") {
		t.Fatalf("l'item d'entitat ha d'avisar de multiples relacions dependents; body=%s", entityBody)
	}
	location := f353Z14PostModeracioLocation(t, app.AdminModeracioAprovar, session, child.ID, "entitat_religiosa", "aprovar", "")
	if !strings.Contains(location, "err=1") {
		t.Fatalf("aprovar amb multiples pares inicials ha de tornar error, location=%s", location)
	}

	childAfter, err := database.GetEntitatReligiosa(child.ID)
	if err != nil {
		t.Fatalf("GetEntitatReligiosa child after duplicate: %v", err)
	}
	if childAfter.ModeracioEstat != "pendent" {
		t.Fatalf("l'entitat ha de continuar pendent si hi ha multiples pares: %+v", childAfter)
	}
	for _, parent := range []int{parentID, otherParentID} {
		rel := f353Z14FindRelation(t, database, parent, child.ID)
		if rel == nil || rel.ModeracioEstat != "pendent" {
			t.Fatalf("cap relacio duplicada no s'ha d'aprovar parcialment parent=%d rel=%+v", parent, rel)
		}
	}
}

func f353Z14PostModeracioLocation(t *testing.T, handler http.HandlerFunc, session *http.Cookie, id int, objectType, action, motiu string) string {
	t.Helper()
	csrf := "csrf_f35_3z14_" + action + "_" + strconv.Itoa(id)
	form := url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("object_type", objectType)
	form.Set("return_to", "/moderacio?type="+url.QueryEscape(objectType))
	if motiu != "" {
		form.Set("motiu", motiu)
	}
	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/moderacio/%d/%s", id, action), strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()
	handler(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("POST moderacio %s %s:%d status=%d body=%s", action, objectType, id, rr.Code, rr.Body.String())
	}
	return rr.Header().Get("Location")
}

func f353Z14FindRelation(t *testing.T, database interface {
	ListEntitatReligiosaRelacions() ([]db.EntitatReligiosaRelacio, error)
}, parentID, childID int) *db.EntitatReligiosaRelacio {
	t.Helper()
	rels, err := database.ListEntitatReligiosaRelacions()
	if err != nil {
		t.Fatalf("ListEntitatReligiosaRelacions: %v", err)
	}
	for i := range rels {
		if rels[i].EntitatOrigenID == parentID && rels[i].EntitatDestiID == childID {
			return &rels[i]
		}
	}
	return nil
}
