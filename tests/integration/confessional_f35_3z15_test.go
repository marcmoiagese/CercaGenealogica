package integration

import (
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF353Z15GroupedApprovalHandlerPublishesEntityRelationAndWiki(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z15_group_approve.sqlite3")
	session := f353YAdminSession(t, database, "z15_group_approve")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	parentID := f353Z8SaveEntity(t, database, "z15_parent_"+suffix, "Arquebisbat F35-3Z15 "+suffix, "arquebisbat_arxidiocesi", "publicat")
	childName := "Arxiprestat pendent F35-3Z15 " + suffix
	form := f353ZEntityForm(childName, "z15_child_"+suffix, 0)
	form.Set("nivell_confessional_codi", "arxiprestat_vicariat_forani")
	form.Set("parent_id", strconv.Itoa(parentID))
	_ = f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, session, form)

	child := f353ZFindEntitatByName(t, database, childName)
	rel := f353Z14FindRelation(t, database, parentID, child.ID)
	if rel == nil || rel.ModeracioEstat != "pendent" {
		t.Fatalf("precondicio relacio pendent: %+v", rel)
	}

	f353YPostModeracio(t, app.AdminModeracioAprovar, session, child.ID, "entitat_religiosa", "aprovar", "")

	approvedChild, err := database.GetEntitatReligiosa(child.ID)
	if err != nil {
		t.Fatalf("GetEntitatReligiosa approved child: %v", err)
	}
	if approvedChild.ModeracioEstat != "publicat" {
		t.Fatalf("l'entitat ha de quedar publicada: %+v", approvedChild)
	}
	rel = f353Z14FindRelation(t, database, parentID, child.ID)
	if rel == nil || rel.ModeracioEstat != "publicat" || !rel.ModeratedBy.Valid {
		t.Fatalf("la relacio dependent ha de quedar publicada dins el mateix flux: %+v", rel)
	}
	changes, err := database.ListWikiChanges("entitat_religiosa", child.ID)
	if err != nil {
		t.Fatalf("ListWikiChanges: %v", err)
	}
	if len(changes) != 1 || changes[0].ModeracioEstat != "publicat" {
		t.Fatalf("l'aprovacio agrupada ha de crear exactament una wiki v1 publicada: %+v", changes)
	}
	treeBody := f353YGet(t, app.AdminConfessionalSectionList, "/confessional/entitats?parent_id="+strconv.Itoa(parentID), session)
	if !strings.Contains(treeBody, childName) {
		t.Fatalf("l'entitat aprovada ha d'apareixer sota el pare; body=%s", treeBody)
	}
}

func TestF353Z15ApproveTxRollsBackEntityAndRelationWhenWikiInsertCannotProceed(t *testing.T) {
	_, database := newTestAppForLogin(t, "test_f35_3z15_rollback_wiki.sqlite3")
	user, _ := f353ZAdminUserSession(t, database, "z15_rollback_wiki")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	parentID := f353Z8SaveEntity(t, database, "z15_rb_parent_"+suffix, "Arquebisbat rollback F35-3Z15 "+suffix, "arquebisbat_arxidiocesi", "publicat")
	childName := "Arxiprestat rollback F35-3Z15 " + suffix
	childID, err := database.SaveEntitatReligiosa(&db.EntitatReligiosa{
		Codi:                   "z15_rb_child_" + suffix,
		Nom:                    childName,
		ReligioConfessioCodi:   "catolicisme_ritu_llati",
		NivellConfessionalCodi: "arxiprestat_vicariat_forani",
		Estat:                  "actiu",
		ModeracioEstat:         "pendent",
		CreatedBy:              sql.NullInt64{Int64: int64(user.ID), Valid: true},
	})
	if err != nil {
		t.Fatalf("SaveEntitatReligiosa child: %v", err)
	}
	relID, err := database.SaveEntitatReligiosaRelacio(&db.EntitatReligiosaRelacio{
		EntitatOrigenID: parentID,
		EntitatDestiID:  childID,
		TipusRelacio:    "arxiprestat_vicariat_forani",
		ModeracioEstat:  "pendent",
		CreatedBy:       sql.NullInt64{Int64: int64(user.ID), Valid: true},
	})
	if err != nil {
		t.Fatalf("SaveEntitatReligiosaRelacio: %v", err)
	}
	_, err = database.CreateWikiChange(&db.WikiChange{
		ObjectType:     "entitat_religiosa",
		ObjectID:       childID,
		ChangeType:     "create",
		FieldKey:       "*",
		Metadata:       `{"before":null,"after":{"id":` + strconv.Itoa(childID) + `}}`,
		ModeracioEstat: "publicat",
		ChangedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
		ModeratedBy:    sql.NullInt64{Int64: int64(user.ID), Valid: true},
	})
	if err != nil {
		t.Fatalf("CreateWikiChange seed publicat: %v", err)
	}

	err = database.ApproveEntitatReligiosaWithInitialParentTx(childID, relID, "", user.ID)
	if err == nil {
		t.Fatalf("l'aprovacio agrupada ha de fallar si la wiki inicial publicada ja existeix")
	}

	childAfter, err := database.GetEntitatReligiosa(childID)
	if err != nil {
		t.Fatalf("GetEntitatReligiosa after rollback: %v", err)
	}
	if childAfter.ModeracioEstat != "pendent" {
		t.Fatalf("la transaccio ha de desfer l'aprovacio parcial de l'entitat: %+v", childAfter)
	}
	relAfter := f353Z14FindRelation(t, database, parentID, childID)
	if relAfter == nil || relAfter.ModeracioEstat != "pendent" {
		t.Fatalf("la transaccio ha de desfer l'aprovacio parcial de la relacio: %+v", relAfter)
	}
	changes, err := database.ListWikiChanges("entitat_religiosa", childID)
	if err != nil {
		t.Fatalf("ListWikiChanges after rollback: %v", err)
	}
	if len(changes) != 1 || changes[0].ModeracioEstat != "publicat" {
		t.Fatalf("el rollback no ha de crear cap wiki extra ni alterar la preexistent: %+v", changes)
	}
}

func TestF353Z15RejectTxRejectsEntityAndRelationTogether(t *testing.T) {
	_, database := newTestAppForLogin(t, "test_f35_3z15_group_reject.sqlite3")
	user, _ := f353ZAdminUserSession(t, database, "z15_group_reject")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	parentID := f353Z8SaveEntity(t, database, "z15_rej_parent_"+suffix, "Arquebisbat rebuig F35-3Z15 "+suffix, "arquebisbat_arxidiocesi", "publicat")
	childID, err := database.SaveEntitatReligiosa(&db.EntitatReligiosa{
		Codi:                   "z15_rej_child_" + suffix,
		Nom:                    "Arxiprestat rebuig F35-3Z15 " + suffix,
		ReligioConfessioCodi:   "catolicisme_ritu_llati",
		NivellConfessionalCodi: "arxiprestat_vicariat_forani",
		Estat:                  "actiu",
		ModeracioEstat:         "pendent",
		CreatedBy:              sql.NullInt64{Int64: int64(user.ID), Valid: true},
	})
	if err != nil {
		t.Fatalf("SaveEntitatReligiosa child: %v", err)
	}
	relID, err := database.SaveEntitatReligiosaRelacio(&db.EntitatReligiosaRelacio{
		EntitatOrigenID: parentID,
		EntitatDestiID:  childID,
		TipusRelacio:    "arxiprestat_vicariat_forani",
		ModeracioEstat:  "pendent",
		CreatedBy:       sql.NullInt64{Int64: int64(user.ID), Valid: true},
	})
	if err != nil {
		t.Fatalf("SaveEntitatReligiosaRelacio: %v", err)
	}

	if err := database.RejectEntitatReligiosaWithInitialParentTx(childID, relID, "fora d'abast", user.ID); err != nil {
		t.Fatalf("RejectEntitatReligiosaWithInitialParentTx: %v", err)
	}

	childAfter, err := database.GetEntitatReligiosa(childID)
	if err != nil {
		t.Fatalf("GetEntitatReligiosa rejected child: %v", err)
	}
	if childAfter.ModeracioEstat != "rebutjat" || childAfter.ModeracioMotiu != "fora d'abast" {
		t.Fatalf("l'entitat ha de quedar rebutjada dins la mateixa transaccio: %+v", childAfter)
	}
	relAfter := f353Z14FindRelation(t, database, parentID, childID)
	if relAfter == nil || relAfter.ModeracioEstat != "rebutjat" || relAfter.ModeracioMotiu != "fora d'abast" {
		t.Fatalf("la relacio dependent ha de quedar rebutjada dins la mateixa transaccio: %+v", relAfter)
	}
	changes, err := database.ListWikiChanges("entitat_religiosa", childID)
	if err != nil {
		t.Fatalf("ListWikiChanges reject: %v", err)
	}
	if len(changes) != 0 {
		t.Fatalf("el rebuig agrupat no ha de crear wiki: %+v", changes)
	}
}
