package integration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF353Z8ParentLevelCompatibilityCatalog(t *testing.T) {
	compatible := map[[2]string]bool{
		{"arquebisbat_arxidiocesi", "arxiprestat_vicariat_forani"}: true,
		{"unitat_pastoral", "parroquia"}:                           true,
		{"parroquia", "lloc_de_culte"}:                             true,
		{"parroquia", "arxiprestat_vicariat_forani"}:               false,
		{"bisbat_diocesi", "santa_seu"}:                            false,
	}
	for pair, want := range compatible {
		if got := core.ConfessionalParentLevelCompatible(pair[0], pair[1]); got != want {
			t.Fatalf("ConfessionalParentLevelCompatible(%q,%q)=%v, want %v", pair[0], pair[1], got, want)
		}
	}
	parish, ok := core.GetConfessionalLevelCatalogByCode("parroquia")
	if !ok || !strings.Contains(core.ConfessionalAllowedParentLevelCodesCSV(parish), "unitat_pastoral") {
		t.Fatalf("parroquia ha d'exposar nivells pare compatibles; parish=%+v", parish)
	}
}

func TestF353Z8HierarchicalCreateValidatesCompatiblePublishedParent(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z8_create.sqlite3")
	session := f353YAdminSession(t, database, "z8_create")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	archID := f353Z8SaveEntity(t, database, "z8_arch_"+suffix, "Arquebisbat F35-3Z8 "+suffix, "arquebisbat_arxidiocesi", "publicat")
	parishID := f353Z8SaveEntity(t, database, "z8_parish_"+suffix, "Parroquia F35-3Z8 "+suffix, "parroquia", "publicat")
	pendingArchID := f353Z8SaveEntity(t, database, "z8_pending_arch_"+suffix, "Arquebisbat pendent F35-3Z8 "+suffix, "arquebisbat_arxidiocesi", "pendent")

	validForm := f353ZEntityForm("Arxiprestat valid F35-3Z8 "+suffix, "z8_child_ok_"+suffix, 0)
	validForm.Set("nivell_confessional_codi", "arxiprestat_vicariat_forani")
	validForm.Set("parent_id", strconv.Itoa(archID))
	_ = f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, session, validForm)
	child := f353ZFindEntitatByName(t, database, validForm.Get("nom"))
	rels, err := database.ListEntitatReligiosaRelacions()
	if err != nil {
		t.Fatalf("ListEntitatReligiosaRelacions: %v", err)
	}
	foundRelation := false
	for _, rel := range rels {
		if rel.EntitatOrigenID == archID && rel.EntitatDestiID == child.ID && rel.TipusRelacio == "arxiprestat_vicariat_forani" {
			foundRelation = true
			if rel.ModeracioEstat != "pendent" {
				t.Fatalf("la relacio automatica nova ha de quedar pendent; rel=%+v", rel)
			}
		}
	}
	if !foundRelation {
		t.Fatalf("no s'ha creat la relacio jerarquica deduida per parent_id=%d child=%d", archID, child.ID)
	}

	badLevelForm := f353ZEntityForm("Arxiprestat invalid F35-3Z8 "+suffix, "z8_child_bad_"+suffix, 0)
	badLevelForm.Set("nivell_confessional_codi", "arxiprestat_vicariat_forani")
	badLevelForm.Set("parent_id", strconv.Itoa(parishID))
	if body := f353YPostConfessional(t, app.AdminSaveConfessional, session, badLevelForm); !strings.Contains(body, "no es compatible") {
		t.Fatalf("un pare parroquia no pot contenir arxiprestat; body=%s", body)
	}

	pendingParentForm := f353ZEntityForm("Arxiprestat parent pendent F35-3Z8 "+suffix, "z8_child_pending_"+suffix, 0)
	pendingParentForm.Set("nivell_confessional_codi", "arxiprestat_vicariat_forani")
	pendingParentForm.Set("parent_id", strconv.Itoa(pendingArchID))
	if body := f353YPostConfessional(t, app.AdminSaveConfessional, session, pendingParentForm); !strings.Contains(body, "no esta publicada") {
		t.Fatalf("un pare pendent no pot ser seleccionat per POST manipulat; body=%s", body)
	}

	selfForm := f353ZEntityForm("Arquebisbat self F35-3Z8 "+suffix, "z8_self_"+suffix, archID)
	selfForm.Set("nivell_confessional_codi", "arquebisbat_arxidiocesi")
	selfForm.Set("parent_id", strconv.Itoa(archID))
	if body := f353YPostConfessional(t, app.AdminSaveConfessional, session, selfForm); !strings.Contains(body, "mateixa") {
		t.Fatalf("una entitat no pot ser pare d'ella mateixa; body=%s", body)
	}

	rootID := f353Z8SaveEntity(t, database, "z8_cycle_root_"+suffix, "Santa Seu F35-3Z8 "+suffix, "santa_seu", "publicat")
	childCycleID := f353Z8SaveEntity(t, database, "z8_cycle_child_"+suffix, "Arquebisbat cicle F35-3Z8 "+suffix, "arquebisbat_arxidiocesi", "publicat")
	f353Z9SavePublishedRelation(t, database, rootID, childCycleID, "arquebisbat_arxidiocesi")
	cycleForm := f353ZEntityForm("Santa Seu F35-3Z8 "+suffix, "z8_cycle_root_"+suffix, rootID)
	cycleForm.Set("nivell_confessional_codi", "santa_seu")
	cycleForm.Set("parent_id", strconv.Itoa(childCycleID))
	if body := f353YPostConfessional(t, app.AdminSaveConfessional, session, cycleForm); !strings.Contains(body, "descendent") {
		t.Fatalf("una entitat no pot passar a dependre d'una descendent; body=%s", body)
	}
}

func TestF353Z8ParentSelectorFiltersCompatiblePublishedParents(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z8_selector.sqlite3")
	session := f353YAdminSession(t, database, "z8_selector")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	archID := f353Z8SaveEntity(t, database, "z8_sel_arch_"+suffix, "Arquebisbat selector F35-3Z8 "+suffix, "arquebisbat_arxidiocesi", "publicat")
	dioceseID := f353Z8SaveEntity(t, database, "z8_sel_diocese_"+suffix, "Diocesi selector F35-3Z8 "+suffix, "bisbat_diocesi", "publicat")
	parishID := f353Z8SaveEntity(t, database, "z8_sel_parish_"+suffix, "Parroquia selector F35-3Z8 "+suffix, "parroquia", "publicat")
	pendingID := f353Z8SaveEntity(t, database, "z8_sel_pending_"+suffix, "Pendent selector F35-3Z8 "+suffix, "arquebisbat_arxidiocesi", "pendent")
	childID := f353Z8SaveEntity(t, database, "z8_sel_child_"+suffix, "Arxiprestat selector F35-3Z8 "+suffix, "arxiprestat_vicariat_forani", "publicat")

	body := f353YGet(t, app.AdminEditConfessional, "/confessional/entitats/"+strconv.Itoa(childID)+"/edit", session)
	levelOption := f353Z5OptionSnippet(body, "arxiprestat_vicariat_forani")
	if !strings.Contains(levelOption, `data-parent-level-codes="arquebisbat_arxidiocesi,bisbat_diocesi"`) {
		t.Fatalf("el nivell fill ha d'exposar els codis pare compatibles; option=%s", levelOption)
	}
	for _, token := range []string{
		`id="parent_id_label"`,
		`id="parent_id" name="parent_id" type="hidden"`,
		`id="parent_id_suggestions"`,
		`data-api="/api/confessional/entitats/suggest"`,
		`id="parent_id_help"`,
		`/static/js/confessional-form.js`,
	} {
		if !strings.Contains(body, token) {
			t.Fatalf("falta contracte selector pare F35-3Z8: %s", token)
		}
	}
	if strings.Contains(body, `select id="parent_id"`) {
		t.Fatalf("el formulari no ha de tornar al select massiu de pares; body=%s", body)
	}
	if strings.Contains(body, "Pendent selector F35-3Z8 "+suffix) {
		t.Fatalf("pares pendents no han d'apareixer precarregats al formulari; body=%s", body)
	}
	_ = archID
	_ = dioceseID
	_ = parishID
	_ = pendingID
}

func TestF353Z8EntityListFiltersPublishedHierarchySafely(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z8_list.sqlite3")
	session := f353YAdminSession(t, database, "z8_list")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	parentID := f353Z8SaveEntity(t, database, "z8_list_parent_"+suffix, "Arquebisbat llista F35-3Z8 "+suffix, "arquebisbat_arxidiocesi", "publicat")
	childID := f353Z8SaveEntity(t, database, "z8_list_child_"+suffix, "Parroquia visible F35-3Z8 "+suffix, "parroquia", "publicat")
	otherID := f353Z8SaveEntity(t, database, "z8_list_other_"+suffix, "Parroquia amagada F35-3Z8 "+suffix, "parroquia", "publicat")
	pendingID := f353Z8SaveEntity(t, database, "z8_list_pending_"+suffix, "Parroquia pendent F35-3Z8 "+suffix, "parroquia", "pendent")
	_, err := database.SaveEntitatReligiosaRelacio(&db.EntitatReligiosaRelacio{
		EntitatOrigenID: parentID,
		EntitatDestiID:  childID,
		TipusRelacio:    "parroquia",
		ModeracioEstat:  "publicat",
	})
	if err != nil {
		t.Fatalf("SaveEntitatReligiosaRelacio: %v", err)
	}

	path := "/confessional/entitats?religio_confessio_codi=catolicisme_ritu_llati&nivell_confessional_codi=parroquia&parent_id=" + strconv.Itoa(parentID) + "&q=visible&status=pendent&sort=nom%3BDROP&dir=desc&per_page=1000"
	body := f353YGet(t, app.AdminConfessionalSectionList, path, session)
	for _, token := range []string{
		`id="nivellsTable"`,
		"Context pare",
		"Parroquia visible F35-3Z8 " + suffix,
	} {
		if !strings.Contains(body, token) {
			t.Fatalf("el llistat filtrat no conte %q; body=%s", token, body)
		}
	}
	for _, id := range []int{otherID, pendingID} {
		item, err := database.GetEntitatReligiosa(id)
		if err != nil {
			t.Fatalf("GetEntitatReligiosa(%d): %v", id, err)
		}
		if strings.Contains(body, "<td>"+item.Nom+"</td>") {
			t.Fatalf("el filtre per pare/publicat no ha de mostrar %q a la taula; body=%s", item.Nom, body)
		}
	}
	if got, err := database.GetEntitatReligiosa(childID); err != nil || got == nil {
		t.Fatalf("la consulta filtrada no ha d'alterar dades ni permetre injeccio; got=%+v err=%v", got, err)
	}
}

func TestF353Z8HierarchyI18NAndCSPRegression(t *testing.T) {
	root := findProjectRoot(t)
	formBody := readProjectFileF353U(t, root, "templates/admin-confessional-form.html")
	listBody := readProjectFileF353U(t, root, "templates/admin-confessional-list.html")
	navBody := readProjectFileF353U(t, root, "templates/admin-confessional-navegacio.html")
	staticBody := readProjectFileF353U(t, root, "static/js/confessional-form.js")
	handlerBody := readProjectFileF353U(t, root, "core/admin_confessional.go")

	for _, token := range []string{
		"ConfessionalParentLevelCompatible",
		"parseConfessionalEntityListFilter",
		"filterConfessionalEntitats",
		"confessionalRelationErrorMessage",
	} {
		if !strings.Contains(handlerBody, token) {
			t.Fatalf("falta validacio/filtratge servidor F35-3Z8: %s", token)
		}
	}
	for _, token := range []string{
		`data-parent-level-codes`,
		`id="parent_id_label"`,
		`id="parent_id_suggestions"`,
		`/api/confessional/entitats/suggest`,
		`confessional.help.parents.choose_level`,
		`confessional.help.parents.none`,
	} {
		if !strings.Contains(formBody, token) {
			t.Fatalf("falta contracte template F35-3Z8: %s", token)
		}
	}
	for _, token := range []string{
		`fetchParentSuggestions`,
		`child_id`,
		`exclude_id`,
	} {
		if !strings.Contains(staticBody, token) {
			t.Fatalf("falta sincronitzacio JS F35-3Z8: %s", token)
		}
	}
	if strings.Contains(formBody, "<script>\n") || strings.Contains(formBody, "onclick=") || strings.Contains(formBody, "onchange=") || strings.Contains(listBody, "<style>") || strings.Contains(navBody, "<style>") {
		t.Fatalf("no s'ha de reintroduir JS inline al formulari confessional")
	}
	for _, token := range []string{
		`confessional.management.parent_context`,
		`confessional.management.new_child`,
	} {
		if !strings.Contains(listBody, token) {
			t.Fatalf("falta contracte de gestio al llistat F35-3Z8/Z10: %s", token)
		}
	}
	for _, token := range []string{
		`confessional.navigation.search.label`,
		`confessional.filter.religion`,
		`confessional.filter.level`,
		`confessional.navigation.parent`,
	} {
		if !strings.Contains(navBody, token) {
			t.Fatalf("falta filtre i18n a la navegacio F35-3Z8/Z10: %s", token)
		}
	}
	for _, lang := range []string{"cat", "en", "oc"} {
		values := readLocaleF353Z7(t, root, lang)
		for _, key := range []string{
			"confessional.filter.search",
			"confessional.filter.parent",
			"confessional.filter.status_publicat",
			"confessional.error.parent_level_incompatible",
			"confessional.error.parent_cycle",
			"confessional.help.parents.choose_level",
			"confessional.help.parents.none",
			"confessional.suggest.empty",
		} {
			if strings.TrimSpace(values[key]) == "" {
				t.Fatalf("%s no defineix %s", lang, key)
			}
		}
	}
}

func TestF353Z8ParentSuggestFiltersPublishedCompatibleRootsAndDescendants(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z8_parent_suggest.sqlite3")
	session := f353YAdminSession(t, database, "z8_parent_suggest")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	rootID := f353Z8SaveEntity(t, database, "z8_sugg_root_"+suffix, "Arquebisbat suggest F35-3Z8 "+suffix, "arquebisbat_arxidiocesi", "publicat")
	childID := f353Z8SaveEntity(t, database, "z8_sugg_child_"+suffix, "Arxiprestat suggest F35-3Z8 "+suffix, "arxiprestat_vicariat_forani", "publicat")
	grandchildID := f353Z8SaveEntity(t, database, "z8_sugg_grand_"+suffix, "Parroquia suggest F35-3Z8 "+suffix, "parroquia", "publicat")
	pendingID := f353Z8SaveEntity(t, database, "z8_sugg_pending_"+suffix, "Arquebisbat pendent suggest F35-3Z8 "+suffix, "arquebisbat_arxidiocesi", "pendent")
	f353Z9SavePublishedRelation(t, database, rootID, childID, "arxiprestat_vicariat_forani")
	f353Z9SavePublishedRelation(t, database, childID, grandchildID, "parroquia")

	req := httptest.NewRequest(http.MethodGet, "/api/confessional/entitats/suggest?q=suggest&scope=roots&nivell_confessional_codi=parroquia&religio_confessio_codi=catolicisme_ritu_llati&child_id="+strconv.Itoa(childID), nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminConfessionalEntitiesSuggestJSON(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("suggest status=%d body=%s", rr.Code, rr.Body.String())
	}
	var payload struct {
		Items []struct {
			ID  int    `json:"id"`
			Nom string `json:"nom"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json invalid suggest confessional: %v", err)
	}
	if len(payload.Items) == 0 || payload.Items[0].ID != rootID {
		t.Fatalf("el suggest ha d'incloure l'arrel compatible publicada; payload=%+v", payload)
	}
	for _, item := range payload.Items {
		if item.ID == childID || item.ID == grandchildID || item.ID == pendingID {
			t.Fatalf("el suggest no ha d'incloure descendents ni pendents; payload=%+v", payload)
		}
	}
}

func f353Z8SaveEntity(t *testing.T, database db.DB, code, name, level, moderation string) int {
	t.Helper()
	id, err := database.SaveEntitatReligiosa(&db.EntitatReligiosa{
		Codi:                   code,
		Nom:                    name,
		ReligioConfessioCodi:   "catolicisme_ritu_llati",
		NivellConfessionalCodi: level,
		Estat:                  "actiu",
		ModeracioEstat:         moderation,
	})
	if err != nil {
		t.Fatalf("SaveEntitatReligiosa(%s): %v", name, err)
	}
	return id
}
