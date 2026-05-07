package integration

import (
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF353Z12EditingExistingEntityWithParentCreatesPendingHierarchyRelation(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z12_reparent.sqlite3")
	session := f353YAdminSession(t, database, "z12_reparent")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	parentID := f353Z8SaveEntity(t, database, "z12_parent_"+suffix, "Arquebisbat reparent F35-3Z12 "+suffix, "arquebisbat_arxidiocesi", "publicat")
	childID := f353Z8SaveEntity(t, database, "z12_child_"+suffix, "Arxiprestat reparent F35-3Z12 "+suffix, "arxiprestat_vicariat_forani", "publicat")
	child, err := database.GetEntitatReligiosa(childID)
	if err != nil {
		t.Fatalf("GetEntitatReligiosa child: %v", err)
	}

	form := f353Z12EntityEditForm(childID, child.Codi, child.Nom+" editat", "arxiprestat_vicariat_forani", parentID)
	_ = f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, session, form)

	relID := f353Z12FindHierarchyRelation(t, database, parentID, childID, "pendent")
	if relID == 0 {
		t.Fatalf("editar una entitat existent amb parent_id ha de crear relacio jerarquica pendent")
	}
	if got := f353Z12CountHierarchyRelations(t, database, parentID, childID); got != 1 {
		t.Fatalf("relacions jerarquiques parent-child=%d, want 1", got)
	}
	changes, err := database.ListWikiChanges("entitat_religiosa", childID)
	if err != nil {
		t.Fatalf("ListWikiChanges: %v", err)
	}
	if len(changes) != 1 || changes[0].ModeracioEstat != "pendent" {
		t.Fatalf("editar entitat publicada ha de conservar proposta wiki pendent: %+v", changes)
	}

	_ = f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, session, form)
	if got := f353Z12CountHierarchyRelations(t, database, parentID, childID); got != 1 {
		t.Fatalf("repetir edicio amb el mateix pare no ha de duplicar relacions, got=%d", got)
	}

	f353YPostModeracio(t, app.AdminModeracioAprovar, session, relID, "entitat_religiosa_relacio", "aprovar", "")
	body := f353YGet(t, app.AdminConfessionalSectionList, "/confessional/entitats?parent_id="+strconv.Itoa(parentID), session)
	if !strings.Contains(body, "Arxiprestat reparent F35-3Z12 "+suffix) {
		t.Fatalf("la relacio aprovada ha de situar l'entitat sota el pare a la gestio jerarquica, body=%s", body)
	}
}

func f353Z12EntityEditForm(id int, code, name, level string, parentID int) url.Values {
	form := f353ZEntityForm(name, code, id)
	form.Set("nivell_confessional_codi", level)
	form.Set("parent_id", strconv.Itoa(parentID))
	return form
}

func f353Z12FindHierarchyRelation(t *testing.T, database interface {
	ListEntitatReligiosaRelacions() ([]db.EntitatReligiosaRelacio, error)
}, parentID, childID int, status string) int {
	t.Helper()
	rels, err := database.ListEntitatReligiosaRelacions()
	if err != nil {
		t.Fatalf("ListEntitatReligiosaRelacions: %v", err)
	}
	for _, rel := range rels {
		if rel.EntitatOrigenID == parentID && rel.EntitatDestiID == childID && rel.ModeracioEstat == status {
			return rel.ID
		}
	}
	return 0
}

func f353Z12CountHierarchyRelations(t *testing.T, database interface {
	ListEntitatReligiosaRelacions() ([]db.EntitatReligiosaRelacio, error)
}, parentID, childID int) int {
	t.Helper()
	rels, err := database.ListEntitatReligiosaRelacions()
	if err != nil {
		t.Fatalf("ListEntitatReligiosaRelacions: %v", err)
	}
	count := 0
	for _, rel := range rels {
		if rel.EntitatOrigenID == parentID && rel.EntitatDestiID == childID && rel.ModeracioEstat != "rebutjat" {
			count++
		}
	}
	return count
}
