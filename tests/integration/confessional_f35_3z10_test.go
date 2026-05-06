package integration

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF353Z10SeparatesManagementAndReligiousNavigation(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z10_split.sqlite3")
	session := f353YAdminSession(t, database, "z10_split")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	archID := f353Z8SaveEntity(t, database, "z10_arch_"+suffix, "Arquebisbat de Tarragona F35-3Z10 "+suffix, "arquebisbat_arxidiocesi", "publicat")
	arxID := f353Z8SaveEntity(t, database, "z10_arx_"+suffix, "Arxiprestat de la Conca-Urgell-Garrigues F35-3Z10 "+suffix, "arxiprestat_vicariat_forani", "publicat")
	parishID := f353Z8SaveEntity(t, database, "z10_parish_"+suffix, "Parroquia de Sant Pere F35-3Z10 "+suffix, "parroquia", "publicat")
	pendingID := f353Z8SaveEntity(t, database, "z10_pending_"+suffix, "Parroquia pendent F35-3Z10 "+suffix, "parroquia", "pendent")
	f353Z9SavePublishedRelation(t, database, archID, arxID, "arxiprestat_vicariat_forani")
	f353Z9SavePublishedRelation(t, database, arxID, parishID, "parroquia")
	f353Z9SavePublishedRelation(t, database, arxID, pendingID, "parroquia")
	municipiID := f353YCreateMunicipi(t, database, "Municipi F35-3Z10 "+suffix)
	if _, err := database.SaveMunicipiEntitatReligiosa(&db.MunicipiEntitatReligiosa{
		MunicipiID:         municipiID,
		EntitatReligiosaID: parishID,
		TipusRelacio:       "parroquia",
		ModeracioEstat:     "publicat",
	}); err != nil {
		t.Fatalf("SaveMunicipiEntitatReligiosa: %v", err)
	}

	managementBody := f353YGet(t, app.AdminConfessionalSectionList, "/confessional/entitats", session)
	for _, token := range []string{
		`id="nivellsFilterForm"`,
		`id="nivellsTable"`,
		`/confessional/navegacio`,
		`/static/js/nivells-taula.js`,
		"Arquebisbat de Tarragona F35-3Z10 " + suffix,
	} {
		if !strings.Contains(managementBody, token) {
			t.Fatalf("la gestio d'entitats no segueix el contracte tipus nivells (%q); body=%s", token, managementBody)
		}
	}
	for _, forbidden := range []string{
		`class="card municipis-browse"`,
		`id="confessional-q"`,
		`confessional.navigation.search.label`,
		`/static/js/confessional-hierarchy.js`,
		`name="parent_mode"`,
	} {
		if strings.Contains(managementBody, forbidden) {
			t.Fatalf("/confessional/entitats barreja navegacio tipus municipis: %q body=%s", forbidden, managementBody)
		}
	}

	childrenBody := f353YGet(t, app.AdminConfessionalSectionList, "/confessional/entitats?parent_id="+strconv.Itoa(archID), session)
	if !strings.Contains(childrenBody, `href="/confessional/entitats/`+strconv.Itoa(arxID)+`">Arxiprestat de la Conca-Urgell-Garrigues F35-3Z10 `+suffix) || strings.Contains(childrenBody, `href="/confessional/entitats/`+strconv.Itoa(parishID)+`">Parroquia de Sant Pere F35-3Z10 `+suffix) {
		t.Fatalf("/confessional/entitats ha de mostrar filles directes del pare seleccionat; body=%s", childrenBody)
	}
	newChildBody := f353YGet(t, app.AdminNewConfessional, "/confessional/entitats/new?parent_id="+strconv.Itoa(archID), session)
	if !strings.Contains(newChildBody, `value="`+strconv.Itoa(archID)+`" data-religion-code="catolicisme_ritu_llati"`) || !strings.Contains(newChildBody, `selected>Arquebisbat de Tarragona F35-3Z10 `+suffix) {
		t.Fatalf("nova entitat sota pare ha de preseleccionar el pare; body=%s", newChildBody)
	}

	navBody := f353YGet(t, app.AdminConfessionalNavigation, "/confessional/navegacio", session)
	for _, token := range []string{
		`class="card municipis-browse"`,
		`id="confessional-hierarchy-filters"`,
		`id="confessional-q"`,
		`/static/js/confessional-hierarchy.js`,
		"Selecciona una relig",
	} {
		if !strings.Contains(navBody, token) {
			t.Fatalf("la navegacio religiosa no segueix el contracte tipus municipis (%q); body=%s", token, navBody)
		}
	}

	levelBody := f353YGet(t, app.AdminConfessionalNavigation, "/confessional/navegacio?religio_confessio_codi=catolicisme_ritu_llati&nivell_confessional_codi=parroquia", session)
	if !strings.Contains(levelBody, "Parroquia de Sant Pere F35-3Z10 "+suffix) || strings.Contains(levelBody, "Parroquia pendent F35-3Z10 "+suffix) {
		t.Fatalf("la navegacio per nivell ha de mostrar publicades del nivell seleccionat i amagar pendents; body=%s", levelBody)
	}
	if strings.Contains(levelBody, `<option value="islam_wilaya"`) && !strings.Contains(levelBody, `<option value="islam_wilaya" data-religion-code="islam"`) {
		t.Fatalf("les opcions de nivell han de conservar data-religion-code per al filtratge concatenat; body=%s", levelBody)
	}

	descBody := f353YGet(t, app.AdminConfessionalNavigation, "/confessional/navegacio?parent_id="+strconv.Itoa(archID)+"&parent_mode=descendants", session)
	if !strings.Contains(descBody, "Arxiprestat de la Conca-Urgell-Garrigues F35-3Z10 "+suffix) || !strings.Contains(descBody, "Parroquia de Sant Pere F35-3Z10 "+suffix) || !strings.Contains(descBody, ">1</td>") {
		t.Fatalf("la navegacio per descendents ha de permetre baixar jerarquia i veure relacions territorials; body=%s", descBody)
	}

	searchBody := f353YGet(t, app.AdminConfessionalNavigation, "/confessional/navegacio?q=Sant%20Pere", session)
	if !strings.Contains(searchBody, "Parroquia de Sant Pere F35-3Z10 "+suffix) {
		t.Fatalf("el cercador lliure de navegacio ha de trobar entitats sense context previ; body=%s", searchBody)
	}
}

func TestF353Z10NavigationSecurityI18NAndMenuContract(t *testing.T) {
	root := findProjectRoot(t)
	mainBody := readProjectFileF353U(t, root, "main.go")
	menuBody := readProjectFileF353U(t, root, "templates/layouts/menu-private.html")
	listBody := readProjectFileF353U(t, root, "templates/admin-confessional-list.html")
	navBody := readProjectFileF353U(t, root, "templates/admin-confessional-navegacio.html")
	handlerBody := readProjectFileF353U(t, root, "core/admin_confessional.go")

	for _, token := range []string{
		"AdminConfessionalNavigation",
		"/confessional/navegacio",
		"parseConfessionalEntityListFilter",
		"normalizeConfessionalHierarchySort",
		"parsePositiveIntDefault",
	} {
		if !strings.Contains(handlerBody+mainBody, token) {
			t.Fatalf("falta contracte servidor Z10: %s", token)
		}
	}
	if !strings.Contains(menuBody, `/confessional/navegacio`) || !strings.Contains(menuBody, `confessional.menu.navigation`) {
		t.Fatalf("el menu no separa entitats i navegacio religiosa")
	}
	if strings.Contains(listBody, `id="confessional-q"`) || strings.Contains(listBody, `class="card municipis-browse"`) {
		t.Fatalf("el template d'entitats no ha de contenir cercador lliure ni layout municipis")
	}
	for _, token := range []string{
		`action="/confessional/navegacio"`,
		`data-submit-form`,
		`data-parent-level-codes`,
		`data-religion-code`,
		`/static/js/confessional-hierarchy.js`,
	} {
		if !strings.Contains(navBody, token) {
			t.Fatalf("falta contracte navegador religios Z10: %s", token)
		}
	}
	if strings.Contains(listBody, "<script>\n") || strings.Contains(navBody, "<script>\n") || strings.Contains(listBody, "onclick=") || strings.Contains(navBody, "onclick=") || strings.Contains(listBody, "onchange=") || strings.Contains(navBody, "onchange=") {
		t.Fatalf("Z10 no ha de reintroduir JS inline")
	}
	for _, lang := range []string{"cat", "en", "oc"} {
		values := readLocaleF353Z7(t, root, lang)
		for _, key := range []string{
			"confessional.menu.navigation",
			"confessional.management.parent_context",
			"confessional.management.new_child",
			"confessional.navigation.title",
			"confessional.navigation.search.label",
			"confessional.navigation.search.placeholder",
			"confessional.navigation.parent",
			"confessional.navigation.empty.prompt",
		} {
			if strings.TrimSpace(values[key]) == "" {
				t.Fatalf("%s no defineix %s", lang, key)
			}
		}
	}
}
