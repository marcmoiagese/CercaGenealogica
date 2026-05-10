package integration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
		`/api/confessional/entitats/suggest?scope=roots`,
		`id="confessional-parent-filter-label"`,
		`id="confessional-parent-filter"`,
		`id="confessional-parent-filter-suggestions"`,
		`label for="confessional-parent-filter-label"`,
		`/confessional/entitats/new?parent_id=` + strconv.Itoa(archID),
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

	childrenBody := f353YGet(t, app.AdminConfessionalSectionList, "/confessional/entitats?parent_id="+strconv.Itoa(archID)+"&parent_mode=direct", session)
	if !strings.Contains(childrenBody, `href="/confessional/entitats/`+strconv.Itoa(arxID)+`">Arxiprestat de la Conca-Urgell-Garrigues F35-3Z10 `+suffix) || strings.Contains(childrenBody, `href="/confessional/entitats/`+strconv.Itoa(parishID)+`">Parroquia de Sant Pere F35-3Z10 `+suffix) {
		t.Fatalf("/confessional/entitats ha de mostrar filles directes del pare seleccionat; body=%s", childrenBody)
	}
	newChildBody := f353YGet(t, app.AdminNewConfessional, "/confessional/entitats/new?parent_id="+strconv.Itoa(archID), session)
	if !strings.Contains(newChildBody, `id="confessional_entity_parent_id" name="parent_id" type="hidden" value="`+strconv.Itoa(archID)+`"`) || !strings.Contains(newChildBody, `id="parent_id_label" type="text" value="Arquebisbat de Tarragona F35-3Z10 `+suffix) || !strings.Contains(newChildBody, `label for="parent_id_label"`) || !strings.Contains(newChildBody, `data-selected-parent-level-code="arquebisbat_arxidiocesi"`) || !strings.Contains(newChildBody, `data-selected-parent-religion-code="catolicisme_ritu_llati"`) {
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

	req := httptest.NewRequest(http.MethodGet, "/api/confessional/entitats/suggest?q=Arquebisbat&scope=roots&nivell_confessional_codi=parroquia&religio_confessio_codi=catolicisme_ritu_llati", nil)
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
	if !payloadContainsConfessionalID(payload.Items, archID) {
		t.Fatalf("el suggest de pares ha de retornar l'entitat arrel compatible; payload=%+v", payload)
	}

	for i := 0; i < 28; i++ {
		name := "Paginacio F35-3Z10 " + suffix + " " + strconv.Itoa(i)
		code := "z10_page_" + suffix + "_" + strconv.Itoa(i)
		f353Z8SaveEntity(t, database, code, name, "parroquia", "publicat")
	}
	pageTwoBody := f353YGet(t, app.AdminConfessionalSectionList, "/confessional/entitats?page=2&per_page=25&religio_confessio_codi=catolicisme_ritu_llati", session)
	if !strings.Contains(pageTwoBody, `href="/confessional/entitats?page=1&per_page=25&religio_confessio_codi=catolicisme_ritu_llati#nivellsTable"`) {
		t.Fatalf("el paginador ha de renderitzar enllacos reals conservant filtres; body=%s", pageTwoBody)
	}
	if strings.Contains(pageTwoBody, `data-nav-href=`) {
		t.Fatalf("el paginador no ha d'usar data-nav-href; body=%s", pageTwoBody)
	}
	if strings.Contains(pageTwoBody, "Paginacio F35-3Z10 "+suffix+" 0") {
		t.Fatalf("page=2 no ha de clampjar sempre a la primera pagina; body=%s", pageTwoBody)
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
		`id="confessional-parent-filter-label"`,
		`id="confessional-parent-filter-suggestions"`,
		`href="{{ .URL }}"`,
		`fa-eye`,
		`fa-edit`,
		`fa-plus`,
		`fa-trash`,
		`class="icon-action danger"`,
	} {
		if !strings.Contains(listBody, token) {
			t.Fatalf("falta contracte de gestio/paginacio Z10: %s", token)
		}
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
	if strings.Contains(listBody, "<script>\n") || strings.Contains(navBody, "<script>\n") || strings.Contains(listBody, "onclick=") || strings.Contains(navBody, "onclick=") || strings.Contains(listBody, "onchange=") || strings.Contains(navBody, "onchange=") || strings.Contains(listBody, "<style>") || strings.Contains(navBody, "<style>") {
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
