package integration

import (
	"database/sql"
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
	f354U13AssertSafeRedirectLocation(t, rr.Header().Get("Location"))

	entity := f353ZFindEntitatByName(t, database, entityName)
	if rr.Header().Get("Location") != "/confessional/entitats/"+strconv.Itoa(entity.ID)+"?notice=pending" {
		t.Fatalf("nova entitat amb relacions inicials ha d'anar a la fitxa pendent; got=%s", rr.Header().Get("Location"))
	}
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
	if strings.Count(formBody, `id="municipi_principal_id"`) != 1 {
		t.Fatalf("el formulari ha de tenir un sol hidden de municipi principal; body=%s", formBody)
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

	unsafeReturnTo := f353ZEntityForm("Parroquia return_to F35-4U13 "+suffix, "f354u13_bad_return_"+suffix, 0)
	unsafeReturnTo.Set("return_to", "https://evil.example/out")
	unsafeReturnTo.Set("municipi_principal_id", strconv.Itoa(f353YCreateMunicipi(t, database, "Municipi return_to F35-4U13 "+suffix)))
	rr := f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, session, unsafeReturnTo)
	f354U13AssertSafeRedirectLocation(t, rr.Header().Get("Location"))

	unsafeSchemeRelative := f353ZEntityForm("Parroquia return_to protocol-relative F35-4U13 "+suffix, "f354u13_bad_return2_"+suffix, 0)
	unsafeSchemeRelative.Set("return_to", "//evil.example/out")
	unsafeSchemeRelative.Set("municipi_principal_id", strconv.Itoa(f353YCreateMunicipi(t, database, "Municipi return_to protocol-relative F35-4U13 "+suffix)))
	rr = f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, session, unsafeSchemeRelative)
	f354U13AssertSafeRedirectLocation(t, rr.Header().Get("Location"))
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

func TestF354U13PrimaryMunicipiIgnoresNucliRelations(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u13_ignore_nucli.sqlite3")
	session := f353YAdminSession(t, database, "f354u13_ignore_nucli")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	municipiName := "Municipi nucli F35-4U13 " + suffix
	municipiID := f353YCreateMunicipi(t, database, municipiName)
	nucliID, err := database.CreateMunicipi(&db.Municipi{
		Nom:            "Nucli F35-4U13 " + suffix,
		Tipus:          "nucli_urba",
		MunicipiID:     sql.NullInt64{Int64: int64(municipiID), Valid: true},
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateMunicipi nucli: %v", err)
	}
	entityID := f353Z8SaveEntity(t, database, "f354u13_nucli_"+suffix, "Parroquia nucli F35-4U13 "+suffix, "parroquia", "publicat")
	_, err = database.SaveMunicipiEntitatReligiosa(&db.MunicipiEntitatReligiosa{
		MunicipiID:         municipiID,
		NucliID:            sql.NullInt64{Int64: int64(nucliID), Valid: true},
		EntitatReligiosaID: entityID,
		TipusRelacio:       "parroquia",
		ModeracioEstat:     "publicat",
	})
	if err != nil {
		t.Fatalf("SaveMunicipiEntitatReligiosa nucli: %v", err)
	}

	entity, err := database.GetEntitatReligiosa(entityID)
	if err != nil || entity == nil {
		t.Fatalf("GetEntitatReligiosa: %v", err)
	}
	form := f353Z12EntityEditForm(entityID, entity.Codi, entity.Nom, entity.NivellConfessionalCodi, 0)
	form.Set("parent_id", "")
	form.Set("municipi_principal_id", strconv.Itoa(municipiID))
	form.Set("municipi_principal_label", municipiName)
	_ = f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, session, form)

	if got := f354U13CountPrimaryMunicipiRelations(t, database, entityID, municipiID); got != 1 {
		t.Fatalf("la relacio amb nucli no ha de bloquejar la creacio del municipi principal; got=%d", got)
	}
}

func TestF354U13EditEntityWithSameParentDoesNotDuplicateRelation(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4u13_no_duplicate_parent.sqlite3")
	session := f353YAdminSession(t, database, "f354u13_no_duplicate_parent")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	parentID := f353Z8SaveEntity(t, database, "f354u13_parent_dup_"+suffix, "Arxiprestat duplicat F35-4U13 "+suffix, "arxiprestat_vicariat_forani", "publicat")
	childID := f353Z8SaveEntity(t, database, "f354u13_child_dup_"+suffix, "Parroquia duplicat pare F35-4U13 "+suffix, "parroquia", "publicat")
	if _, err := database.SaveEntitatReligiosaRelacio(&db.EntitatReligiosaRelacio{
		EntitatOrigenID: parentID,
		EntitatDestiID:  childID,
		TipusRelacio:    "parroquia",
		ModeracioEstat:  "publicat",
	}); err != nil {
		t.Fatalf("SaveEntitatReligiosaRelacio: %v", err)
	}

	child, err := database.GetEntitatReligiosa(childID)
	if err != nil || child == nil {
		t.Fatalf("GetEntitatReligiosa: %v", err)
	}
	form := f353Z12EntityEditForm(childID, child.Codi, child.Nom, child.NivellConfessionalCodi, parentID)
	_ = f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, session, form)

	if got := f354U13CountParentRelations(t, database, parentID, childID); got != 1 {
		t.Fatalf("editar amb el mateix pare no ha de duplicar relacions; got=%d", got)
	}
}

func f354U13FindPrimaryMunicipiRelation(t *testing.T, database db.DB, entityID, municipiID int) *db.MunicipiEntitatReligiosa {
	t.Helper()
	rels, err := database.ListMunicipiEntitatsReligiosesByEntitat(entityID)
	if err != nil {
		t.Fatalf("ListMunicipiEntitatsReligiosesByEntitat: %v", err)
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
	rels, err := database.ListMunicipiEntitatsReligiosesByEntitat(entityID)
	if err != nil {
		t.Fatalf("ListMunicipiEntitatsReligiosesByEntitat: %v", err)
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

func f354U13CountParentRelations(t *testing.T, database db.DB, parentID, childID int) int {
	t.Helper()
	rels, err := database.ListEntitatReligiosaRelacions()
	if err != nil {
		t.Fatalf("ListEntitatReligiosaRelacions: %v", err)
	}
	total := 0
	for _, rel := range rels {
		if rel.EntitatOrigenID == parentID && rel.EntitatDestiID == childID && rel.ModeracioEstat != "rebutjat" {
			total++
		}
	}
	return total
}

func f354U13AssertSafeRedirectLocation(t *testing.T, loc string) {
	t.Helper()
	for _, badPrefix := range []string{"http://", "https://", "//"} {
		if strings.HasPrefix(loc, badPrefix) {
			t.Fatalf("return_to insegur no s'ha de respectar: %s", loc)
		}
	}
}
