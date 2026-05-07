package integration

import (
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF353Z13ModeratesNewEntityWithInitialParentRelationAsSingleFunctionalItem(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z13_group_approve.sqlite3")
	session := f353YAdminSession(t, database, "z13_group_approve")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	parentID := f353Z8SaveEntity(t, database, "z13_parent_"+suffix, "Arquebisbat F35-3Z13 "+suffix, "arquebisbat_arxidiocesi", "publicat")
	childName := "Arxiprestat pendent F35-3Z13 " + suffix
	form := f353ZEntityForm(childName, "z13_child_"+suffix, 0)
	form.Set("nivell_confessional_codi", "arxiprestat_vicariat_forani")
	form.Set("parent_id", strconv.Itoa(parentID))
	_ = f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, session, form)

	child := f353ZFindEntitatByName(t, database, childName)
	if child.ModeracioEstat != "pendent" {
		t.Fatalf("l'entitat nova ha de quedar pendent: %+v", child)
	}
	rel := f353Z13FindRelation(t, database, parentID, child.ID)
	if rel == nil || rel.ModeracioEstat != "pendent" {
		t.Fatalf("la relacio inicial ha de quedar pendent: %+v", rel)
	}

	entityBody := f353YGet(t, app.AdminModeracioList, "/moderacio?type=entitat_religiosa", session)
	for _, want := range []string{childName, "Pare proposat: Arquebisbat F35-3Z13 " + suffix, "Tipus de relacio: arxiprestat_vicariat_forani"} {
		if !strings.Contains(entityBody, want) {
			t.Fatalf("l'item d'entitat ha de mostrar %q; body=%s", want, entityBody)
		}
	}
	relationBody := f353YGet(t, app.AdminModeracioList, "/moderacio?type=entitat_religiosa_relacio", session)
	if strings.Contains(relationBody, childName) {
		t.Fatalf("la relacio inicial dependent no ha de sortir com item independent; body=%s", relationBody)
	}

	f353YPostModeracio(t, app.AdminModeracioAprovar, session, child.ID, "entitat_religiosa", "aprovar", "")
	approvedChild, err := database.GetEntitatReligiosa(child.ID)
	if err != nil {
		t.Fatalf("GetEntitatReligiosa approved child: %v", err)
	}
	if approvedChild.ModeracioEstat != "publicat" {
		t.Fatalf("aprovar l'entitat ha de publicar-la: %+v", approvedChild)
	}
	rel = f353Z13FindRelation(t, database, parentID, child.ID)
	if rel == nil || rel.ModeracioEstat != "publicat" || !rel.ModeratedBy.Valid {
		t.Fatalf("aprovar l'entitat ha de publicar tambe la relacio dependent amb moderador: %+v", rel)
	}
	changes, err := database.ListWikiChanges("entitat_religiosa", child.ID)
	if err != nil {
		t.Fatalf("ListWikiChanges: %v", err)
	}
	if len(changes) != 1 || changes[0].ModeracioEstat != "publicat" {
		t.Fatalf("l'aprovacio de l'entitat ha de crear nomes la wiki v1 inicial: %+v", changes)
	}
	treeBody := f353YGet(t, app.AdminConfessionalSectionList, "/confessional/entitats?parent_id="+strconv.Itoa(parentID), session)
	if !strings.Contains(treeBody, childName) {
		t.Fatalf("l'entitat aprovada ha d'apareixer sota el pare; body=%s", treeBody)
	}
}

func TestF353Z13RejectingNewEntityRejectsInitialParentRelation(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z13_group_reject.sqlite3")
	session := f353YAdminSession(t, database, "z13_group_reject")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	parentID := f353Z8SaveEntity(t, database, "z13_rej_parent_"+suffix, "Arquebisbat rebuig F35-3Z13 "+suffix, "arquebisbat_arxidiocesi", "publicat")
	childName := "Arxiprestat rebuig F35-3Z13 " + suffix
	form := f353ZEntityForm(childName, "z13_rej_child_"+suffix, 0)
	form.Set("nivell_confessional_codi", "arxiprestat_vicariat_forani")
	form.Set("parent_id", strconv.Itoa(parentID))
	_ = f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, session, form)

	child := f353ZFindEntitatByName(t, database, childName)
	f353YPostModeracio(t, app.AdminModeracioRebutjar, session, child.ID, "entitat_religiosa", "rebutjar", "fora d'abast")

	rejectedChild, err := database.GetEntitatReligiosa(child.ID)
	if err != nil {
		t.Fatalf("GetEntitatReligiosa rejected child: %v", err)
	}
	if rejectedChild.ModeracioEstat != "rebutjat" {
		t.Fatalf("rebutjar l'entitat ha de rebutjar-la: %+v", rejectedChild)
	}
	rel := f353Z13FindRelation(t, database, parentID, child.ID)
	if rel == nil || rel.ModeracioEstat != "rebutjat" || !rel.ModeratedBy.Valid || rel.ModeracioMotiu != "fora d'abast" {
		t.Fatalf("rebutjar l'entitat ha de rebutjar la relacio dependent amb notes: %+v", rel)
	}
	relationBody := f353YGet(t, app.AdminModeracioList, "/moderacio?type=entitat_religiosa_relacio", session)
	if strings.Contains(relationBody, childName) {
		t.Fatalf("no ha de quedar relacio pendent orfena a moderacio; body=%s", relationBody)
	}
}

func TestF353Z13DoesNotGroupPublishedReparentOrManualRelations(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z13_no_group.sqlite3")
	session := f353YAdminSession(t, database, "z13_no_group")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	parentID := f353Z8SaveEntity(t, database, "z13_ng_parent_"+suffix, "Arquebisbat no group F35-3Z13 "+suffix, "arquebisbat_arxidiocesi", "publicat")
	childID := f353Z8SaveEntity(t, database, "z13_ng_child_"+suffix, "Arxiprestat publicat no group F35-3Z13 "+suffix, "arxiprestat_vicariat_forani", "publicat")
	child, err := database.GetEntitatReligiosa(childID)
	if err != nil {
		t.Fatalf("GetEntitatReligiosa child: %v", err)
	}
	editForm := f353Z12EntityEditForm(childID, child.Codi, child.Nom+" editat", "arxiprestat_vicariat_forani", parentID)
	_ = f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, session, editForm)

	reparentBody := f353YGet(t, app.AdminModeracioList, "/moderacio?type=entitat_religiosa_relacio", session)
	if !strings.Contains(reparentBody, child.Nom) || !strings.Contains(reparentBody, "Arquebisbat no group F35-3Z13 "+suffix) {
		t.Fatalf("el reparent d'una entitat publicada ha de sortir com relacio independent; body=%s", reparentBody)
	}

	manualChildID := f353Z8SaveEntity(t, database, "z13_manual_child_"+suffix, "Parroquia manual F35-3Z13 "+suffix, "parroquia", "publicat")
	manualForm := url.Values{}
	manualForm.Set("kind", "rel_ent")
	manualForm.Set("entitat_origen_id", strconv.Itoa(childID))
	manualForm.Set("entitat_desti_id", strconv.Itoa(manualChildID))
	_ = f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, session, manualForm)

	manualBody := f353YGet(t, app.AdminModeracioList, "/moderacio?type=entitat_religiosa_relacio", session)
	if !strings.Contains(manualBody, "Parroquia manual F35-3Z13 "+suffix) {
		t.Fatalf("la relacio manual entre entitats publicades ha de sortir com item independent; body=%s", manualBody)
	}
}

func f353Z13FindRelation(t *testing.T, database interface {
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
