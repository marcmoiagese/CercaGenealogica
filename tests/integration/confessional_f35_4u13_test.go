package integration

import (
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF354U13CreateEntityWithPrimaryMunicipalityCreatesPendingTerritorialRelation(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u13_primary_municipi.sqlite3")
	session := f353YAdminSession(t, database, "f354u13_primary_municipi")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	municipiName := "Municipi principal F35-4U13 " + suffix
	municipiID := f353YCreateMunicipi(t, database, municipiName)
	entityName := "Parroquia municipi F35-4U13 " + suffix
	form := f353ZEntityForm(entityName, "f354u13_primary_"+suffix, 0)
	form.Set("municipi_principal_id", strconv.Itoa(municipiID))
	form.Set("municipi_principal_label", municipiName)
	form.Set("initial_relation_notes", "Relacio inicial F35-4U13")

	rr := f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, session, form)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("save status=%d body=%s", rr.Code, rr.Body.String())
	}

	entity := f353ZFindEntitatByName(t, database, entityName)
	rel := f354U13FindPrimaryMunicipiRelation(t, database, entity.ID, municipiID)
	if rel == nil {
		t.Fatalf("no s'ha creat la relacio territorial principal")
	}
	if rel.ModeracioEstat != "pendent" || rel.TipusRelacio != "parroquia" || rel.Observacions != "Relacio inicial F35-4U13" {
		t.Fatalf("relacio territorial inesperada: %+v", rel)
	}

	editBody := f353YGet(t, app.AdminEditConfessional, "/confessional/entitats/"+strconv.Itoa(entity.ID)+"/edit", session)
	for _, token := range []string{
		`id="municipi_principal_label"`,
		`value="` + municipiName + `"`,
		`id="municipi_principal_id" name="municipi_principal_id" type="hidden" value="` + strconv.Itoa(municipiID) + `"`,
		`/api/territori/municipis/suggest`,
	} {
		if !strings.Contains(editBody, token) {
			t.Fatalf("l'edicio ha de conservar el municipi principal; falta %q body=%s", token, editBody)
		}
	}

	profileBody := f353YGet(t, app.AdminConfessionalEntityShow, "/confessional/entitats/"+strconv.Itoa(entity.ID), session)
	if !strings.Contains(profileBody, municipiName) {
		t.Fatalf("la fitxa ha de mostrar el municipi principal; body=%s", profileBody)
	}
}

func TestF354U13CreateEntityWithParentAndPrimaryMunicipalityUsesSingleForm(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u13_parent_and_municipi.sqlite3")
	session := f353YAdminSession(t, database, "f354u13_parent_and_municipi")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	parentID := f353Z8SaveEntity(t, database, "f354u13_parent_"+suffix, "Arxiprestat F35-4U13 "+suffix, "arxiprestat_vicariat_forani", "publicat")
	municipiID := f353YCreateMunicipi(t, database, "Municipi combinat F35-4U13 "+suffix)
	childName := "Parroquia combinada F35-4U13 " + suffix
	form := f353ZEntityForm(childName, "f354u13_child_"+suffix, 0)
	form.Set("parent_id", strconv.Itoa(parentID))
	form.Set("municipi_principal_id", strconv.Itoa(municipiID))
	_ = f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, session, form)

	child := f353ZFindEntitatByName(t, database, childName)
	if rel := f353Z13FindRelation(t, database, parentID, child.ID); rel == nil {
		t.Fatalf("falta la relacio jerarquica inicial")
	}
	if rel := f354U13FindPrimaryMunicipiRelation(t, database, child.ID, municipiID); rel == nil {
		t.Fatalf("falta la relacio territorial principal")
	}
}

func TestF354U13EditEntityWithSamePrimaryMunicipalityDoesNotDuplicateRelation(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u13_no_duplicate.sqlite3")
	session := f353YAdminSession(t, database, "f354u13_no_duplicate")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	municipiID := f353YCreateMunicipi(t, database, "Municipi duplicat F35-4U13 "+suffix)
	entityID := f353Z8SaveEntity(t, database, "f354u13_dup_"+suffix, "Parroquia duplicat F35-4U13 "+suffix, "parroquia", "publicat")
	_, err := database.SaveMunicipiEntitatReligiosa(&db.MunicipiEntitatReligiosa{
		MunicipiID:         municipiID,
		EntitatReligiosaID: entityID,
		TipusRelacio:       "parroquia",
		ModeracioEstat:     "publicat",
	})
	if err != nil {
		t.Fatalf("SaveMunicipiEntitatReligiosa: %v", err)
	}

	entity, err := database.GetEntitatReligiosa(entityID)
	if err != nil || entity == nil {
		t.Fatalf("GetEntitatReligiosa: %v", err)
	}
	form := f353Z12EntityEditForm(entityID, entity.Codi, entity.Nom, entity.NivellConfessionalCodi, 0)
	form.Set("parent_id", "")
	form.Set("municipi_principal_id", strconv.Itoa(municipiID))
	form.Set("municipi_principal_label", "Municipi duplicat F35-4U13 "+suffix)
	_ = f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, session, form)

	if got := f354U13CountPrimaryMunicipiRelations(t, database, entityID, municipiID); got != 1 {
		t.Fatalf("editar amb el mateix municipi principal no ha de duplicar relacions; got=%d", got)
	}
}

func TestF354U13ValidatesInitialRelationsAndKeepsAutocompleteContract(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u13_validation.sqlite3")
	session := f353YAdminSession(t, database, "f354u13_validation")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	formBody := f353YGet(t, app.AdminNewConfessional, "/confessional/entitats/new", session)
	for _, token := range []string{
		`id="municipi_principal_label"`,
		`id="municipi_principal_id" name="municipi_principal_id" type="hidden"`,
		`data-hidden="municipi_principal_id"`,
		`/api/territori/municipis/suggest`,
		`/static/js/arxiu-form-suggest.js`,
		`/static/js/confessional-form.js`,
		`Relacions inicials`,
	} {
		if !strings.Contains(formBody, token) {
			t.Fatalf("falta contracte del formulari F35-4U13: %s", token)
		}
	}
	for _, banned := range []string{`name="municipi_principal_label"`, "<script>", "onclick=", "onchange=", "style="} {
		if strings.Contains(formBody, banned) {
			t.Fatalf("el formulari no ha de contenir %q; body=%s", banned, formBody)
		}
	}

	badMunicipiName := "Parroquia invalid municipi F35-4U13 " + suffix
	badMunicipi := f353ZEntityForm(badMunicipiName, "f354u13_bad_mun_"+suffix, 0)
	badMunicipi.Set("municipi_principal_id", "999999")
	body := f353YPostConfessional(t, app.AdminSaveConfessional, session, badMunicipi)
	if !strings.Contains(body, "municipi principal") {
		t.Fatalf("cal error de municipi principal invalid; body=%s", body)
	}
	if f354U13EntityExistsByName(t, database, badMunicipiName) {
		t.Fatalf("no s'ha de crear l'entitat amb municipi invalid")
	}

	badParent := f353ZEntityForm("Parroquia invalid parent F35-4U13 "+suffix, "f354u13_bad_parent_"+suffix, 0)
	badParent.Set("parent_id", "999999")
	body = f353YPostConfessional(t, app.AdminSaveConfessional, session, badParent)
	if !strings.Contains(body, "entitat pare indicada no existeix") {
		t.Fatalf("cal error de pare invalid; body=%s", body)
	}
}

func TestF354U13TemplateAndLocalesContract(t *testing.T) {
	root := findProjectRoot(t)
	formBody := readProjectFileF353U(t, root, "templates/admin-confessional-form.html")
	staticBody := readProjectFileF353U(t, root, "static/js/confessional-form.js")
	for _, token := range []string{
		`municipi_principal_label`,
		`municipi_principal_id`,
		`initial_relation_notes`,
		`initial_parent_relation_kind`,
		`/api/territori/municipis/suggest`,
	} {
		if !strings.Contains(formBody, token) {
			t.Fatalf("falta contracte template F35-4U13: %s", token)
		}
	}
	for _, token := range []string{
		`syncInitialRelationKind`,
		`initial_parent_relation_kind`,
	} {
		if !strings.Contains(staticBody, token) {
			t.Fatalf("falta contracte JS F35-4U13: %s", token)
		}
	}
	for _, lang := range []string{"cat", "en", "oc"} {
		values := readLocaleF353Z7(t, root, lang)
		for _, key := range []string{
			"confessional.initial_relations.title",
			"confessional.initial_relations.primary_municipality.label",
			"confessional.initial_relations.primary_municipality.help",
			"confessional.initial_relations.parent_relation_kind.label",
			"confessional.initial_relations.notes.label",
			"confessional.initial_relations.primary_municipality.invalid",
		} {
			if strings.TrimSpace(values[key]) == "" {
				t.Fatalf("%s no defineix %s", lang, key)
			}
		}
	}
}

func f354U13FindPrimaryMunicipiRelation(t *testing.T, database db.DB, entityID, municipiID int) *db.MunicipiEntitatReligiosa {
	t.Helper()
	rels, err := database.ListMunicipiEntitatsReligioses(0)
	if err != nil {
		t.Fatalf("ListMunicipiEntitatsReligioses: %v", err)
	}
	for i := range rels {
		if rels[i].EntitatReligiosaID == entityID && rels[i].MunicipiID == municipiID && !rels[i].NucliID.Valid {
			return &rels[i]
		}
	}
	return nil
}

func f354U13CountPrimaryMunicipiRelations(t *testing.T, database db.DB, entityID, municipiID int) int {
	t.Helper()
	rels, err := database.ListMunicipiEntitatsReligioses(0)
	if err != nil {
		t.Fatalf("ListMunicipiEntitatsReligioses: %v", err)
	}
	total := 0
	for _, rel := range rels {
		if rel.EntitatReligiosaID == entityID && rel.MunicipiID == municipiID && !rel.NucliID.Valid {
			total++
		}
	}
	return total
}

func f354U13EntityExistsByName(t *testing.T, database db.DB, name string) bool {
	t.Helper()
	items, err := database.ListEntitatsReligioses()
	if err != nil {
		t.Fatalf("ListEntitatsReligioses: %v", err)
	}
	for _, item := range items {
		if item.Nom == name {
			return true
		}
	}
	return false
}
