package integration

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF353Z9ConfessionalHierarchyNavigationAndSearch(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z9_hierarchy.sqlite3")
	session := f353YAdminSession(t, database, "z9_hierarchy")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	archID := f353Z8SaveEntity(t, database, "z9_arch_"+suffix, "Arquebisbat de Tarragona F35-3Z9 "+suffix, "arquebisbat_arxidiocesi", "publicat")
	arxID := f353Z8SaveEntity(t, database, "z9_arx_"+suffix, "Arxiprestat de la Conca F35-3Z9 "+suffix, "arxiprestat_vicariat_forani", "publicat")
	unitID := f353Z8SaveEntity(t, database, "z9_unit_"+suffix, "Unitat Pastoral Arbeca F35-3Z9 "+suffix, "unitat_pastoral", "publicat")
	parishID := f353Z8SaveEntity(t, database, "z9_parish_"+suffix, "Parroquia de Sant Jaume Apostol d'Arbeca F35-3Z9 "+suffix, "parroquia", "publicat")
	pendingID := f353Z8SaveEntity(t, database, "z9_pending_"+suffix, "Parroquia pendent Arbeca F35-3Z9 "+suffix, "parroquia", "pendent")

	f353Z9SavePublishedRelation(t, database, archID, arxID, "arxiprestat_vicariat_forani")
	f353Z9SavePublishedRelation(t, database, arxID, unitID, "unitat_pastoral")
	f353Z9SavePublishedRelation(t, database, unitID, parishID, "parroquia")
	f353Z9SavePublishedRelation(t, database, unitID, pendingID, "parroquia")

	rootBody := f353YGet(t, app.AdminConfessionalSectionList, "/confessional/entitats", session)
	for _, token := range []string{
		`id="nivellsFilterForm"`,
		`id="nivellsTable"`,
		`/static/js/nivells-taula.js`,
		"Arquebisbat de Tarragona F35-3Z9 " + suffix,
	} {
		if !strings.Contains(rootBody, token) {
			t.Fatalf("vista arrel jerarquica no conte %q; body=%s", token, rootBody)
		}
	}
	if strings.Contains(rootBody, `class="municipis-browse"`) || strings.Contains(rootBody, `id="confessional-q"`) || strings.Contains(rootBody, `/static/js/confessional-hierarchy.js`) {
		t.Fatalf("la gestio d'entitats no ha de ser una pantalla tipus municipis; body=%s", rootBody)
	}

	branchBody := f353YGet(t, app.AdminConfessionalSectionList, "/confessional/entitats?parent_id="+strconv.Itoa(archID), session)
	for _, token := range []string{
		"Cami actual",
		"Arquebisbat de Tarragona F35-3Z9 " + suffix,
		"Arxiprestat de la Conca F35-3Z9 " + suffix,
		"Relacions territorials",
	} {
		if !strings.Contains(branchBody, token) {
			t.Fatalf("la branca jerarquica no mostra %q; body=%s", token, branchBody)
		}
	}
	if strings.Contains(branchBody, `href="/confessional/entitats/`+strconv.Itoa(unitID)+`">Unitat Pastoral Arbeca F35-3Z9 `+suffix) {
		t.Fatalf("la gestio jerarquica ha de mostrar filles directes del pare seleccionat, no descendents globals; body=%s", branchBody)
	}
	if strings.Contains(branchBody, "Parroquia pendent Arbeca F35-3Z9 "+suffix) {
		t.Fatalf("les entitats pendents no han d'apareixer a la vista normal; body=%s", branchBody)
	}

	searchBody := f353YGet(t, app.AdminConfessionalNavigation, "/confessional/navegacio?q=Arbeca%3A", session)
	if !strings.Contains(searchBody, "Parroquia de Sant Jaume Apostol d&#39;Arbeca F35-3Z9 "+suffix) || !strings.Contains(searchBody, "Unitat Pastoral Arbeca F35-3Z9 "+suffix) {
		t.Fatalf("la navegacio separada ha de cercar entitats sense preseleccionar context; body=%s", searchBody)
	}

	levelBody := f353YGet(t, app.AdminConfessionalNavigation, "/confessional/navegacio?nivell_confessional_codi=parroquia&q=Arbeca", session)
	if !strings.Contains(levelBody, "Parroquia de Sant Jaume Apostol d&#39;Arbeca F35-3Z9 "+suffix) || strings.Contains(levelBody, `<td><a href="/confessional/entitats/`+strconv.Itoa(unitID)+`">Unitat Pastoral Arbeca F35-3Z9 `+suffix) {
		t.Fatalf("el filtre per nivell parroquia ha de limitar resultats; body=%s", levelBody)
	}
}

func TestF353Z9ConfessionalHierarchyGetParamsAreValidated(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z9_security.sqlite3")
	session := f353YAdminSession(t, database, "z9_security")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	rootID := f353Z8SaveEntity(t, database, "z9_sec_root_"+suffix, "Santa Seu segura F35-3Z9 "+suffix, "santa_seu", "publicat")
	_ = rootID

	body := f353YGet(t, app.AdminConfessionalNavigation, "/confessional/navegacio?religio_confessio_codi=nope&nivell_confessional_codi=nope&parent_id=abc&q=%3Cscript%3Ealert(1)%3C%2Fscript%3E&sort=nom%3BDROP&dir=DROP&per_page=1000", session)
	for _, token := range []string{
		`name="sort" value="path"`,
		`name="dir" value="asc"`,
		`name="per_page" value="100"`,
	} {
		if !strings.Contains(body, token) {
			t.Fatalf("parametres GET manipulats no s'han normalitzat com s'esperava (%q); body=%s", token, body)
		}
	}
	if strings.Contains(body, "<script>alert(1)</script>") || strings.Contains(body, "nom;DROP") {
		t.Fatalf("payload malicios no ha de sortir com HTML executable ni com sort interpolat; body=%s", body)
	}
}

func TestF353Z9ConfessionalHierarchyI18NAndCSPContract(t *testing.T) {
	root := findProjectRoot(t)
	listBody := readProjectFileF353U(t, root, "templates/admin-confessional-list.html")
	navBody := readProjectFileF353U(t, root, "templates/admin-confessional-navegacio.html")
	jsBody := readProjectFileF353U(t, root, "static/js/confessional-hierarchy.js")
	handlerBody := readProjectFileF353U(t, root, "core/admin_confessional.go")

	for _, token := range []string{
		"filterConfessionalHierarchyRows",
		"normalizeConfessionalHierarchySort",
		"normalizeConfessionalSortDir",
		"confessionalHierarchyParentOptions",
	} {
		if !strings.Contains(handlerBody, token) {
			t.Fatalf("falta contracte servidor Z9: %s", token)
		}
	}
	for _, token := range []string{
		`confessional.management.parent_context`,
		`/static/js/nivells-taula.js`,
		`id="nivellsTable"`,
	} {
		if !strings.Contains(listBody, token) {
			t.Fatalf("falta contracte template gestio Z9/Z10: %s", token)
		}
	}
	for _, token := range []string{
		`confessional.navigation.search.label`,
		`confessional.hierarchy.path`,
		`confessional.hierarchy.scope.descendants`,
		`/static/js/confessional-hierarchy.js`,
	} {
		if !strings.Contains(navBody, token) {
			t.Fatalf("falta contracte template navegacio Z9/Z10: %s", token)
		}
	}
	if strings.Contains(listBody, "<script>\n") || strings.Contains(navBody, "<script>\n") || strings.Contains(listBody, "onchange=") || strings.Contains(navBody, "onchange=") || strings.Contains(listBody, "onclick=") || strings.Contains(navBody, "onclick=") {
		t.Fatalf("les vistes confessionals no han de tenir JS inline")
	}
	for _, token := range []string{
		`data-submit-form`,
		`syncLevelsAndParents`,
		`data-parent-level-codes`,
	} {
		if !strings.Contains(jsBody, token) {
			t.Fatalf("falta sincronitzacio JS externa Z9: %s", token)
		}
	}
	for _, lang := range []string{"cat", "en", "oc"} {
		values := readLocaleF353Z7(t, root, lang)
		for _, key := range []string{
			"confessional.hierarchy.search.label",
			"confessional.hierarchy.search.placeholder",
			"confessional.navigation.title",
			"confessional.menu.navigation",
			"confessional.hierarchy.root",
			"confessional.hierarchy.path",
			"confessional.hierarchy.children",
			"confessional.hierarchy.territorial_relations",
		} {
			if strings.TrimSpace(values[key]) == "" {
				t.Fatalf("%s no defineix %s", lang, key)
			}
		}
	}
}

func f353Z9SavePublishedRelation(t *testing.T, database db.DB, parentID, childID int, relationType string) {
	t.Helper()
	if _, err := database.SaveEntitatReligiosaRelacio(&db.EntitatReligiosaRelacio{
		EntitatOrigenID: parentID,
		EntitatDestiID:  childID,
		TipusRelacio:    relationType,
		ModeracioEstat:  "publicat",
	}); err != nil {
		t.Fatalf("SaveEntitatReligiosaRelacio(%d,%d): %v", parentID, childID, err)
	}
}
