package integration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
)

func TestF353Z7ConfessionalCatalogSelectableReligionsAreUseful(t *testing.T) {
	if _, ok := core.GetConfessionalReligionCatalogByCode("baptisme"); ok {
		t.Fatalf("el codi confessional antic baptisme no ha de continuar al cataleg")
	}
	baptist, ok := core.GetConfessionalReligionCatalogByCode("esglesies_baptistes")
	if !ok {
		t.Fatalf("falta esglesies_baptistes al cataleg confessional")
	}
	if baptist.ParentCode != "protestantisme" || baptist.CategoryCode != "confessio" {
		t.Fatalf("esglesies_baptistes inesperat: %+v", baptist)
	}

	selectable := core.ListSelectableConfessionalReligionCatalog()
	got := map[string]bool{}
	for _, item := range selectable {
		got[item.Code] = true
		if item.Code == "baptisme" {
			t.Fatalf("baptisme no pot ser seleccionable")
		}
		if !item.CanCreateEntities && len(core.ListConfessionalLevelsByReligionCode(item.Code)) == 0 {
			t.Fatalf("%s es seleccionable sense nivells actius ni flag explicit", item.Code)
		}
	}
	for _, code := range []string{
		"catolicisme_ritu_llati",
		"ortodoxia",
		"anglicanisme",
		"islam",
		"judaisme",
		"budisme",
		"sintoisme",
		"fe_bahai",
	} {
		if !got[code] {
			t.Fatalf("%s ha de sortir al selector normal", code)
		}
	}
	for _, code := range []string{
		"cristianisme",
		"catolicisme",
		"protestantisme",
		"luteranisme",
		"calvinisme_reformats",
		"metodisme",
		"esglesies_baptistes",
		"religions_tradicionals_africanes",
	} {
		if got[code] {
			t.Fatalf("%s ha de quedar com a cataleg intern mentre no tingui nivells actius", code)
		}
	}
}

func TestF353Z7SelectableCatalogLabelsHaveI18N(t *testing.T) {
	root := findProjectRoot(t)
	localeValues := map[string]map[string]string{}
	for _, lang := range []string{"cat", "en", "oc"} {
		localeValues[lang] = readLocaleF353Z7(t, root, lang)
	}

	selectableReligions := core.ListSelectableConfessionalReligionCatalog()
	selectableReligionCodes := map[string]bool{}
	for _, item := range selectableReligions {
		selectableReligionCodes[item.Code] = true
		key := "confessional.religion." + item.Code
		for _, lang := range []string{"cat", "en", "oc"} {
			if strings.TrimSpace(localeValues[lang][key]) == "" {
				t.Fatalf("%s no defineix %s", lang, key)
			}
		}
	}

	for _, level := range core.ListConfessionalLevelCatalog() {
		if !level.Active || !selectableReligionCodes[level.ReligionCode] {
			continue
		}
		key := level.I18nKey
		if key == "" {
			key = "confessional.level." + level.Code
		}
		for _, lang := range []string{"cat", "en", "oc"} {
			if strings.TrimSpace(localeValues[lang][key]) == "" {
				t.Fatalf("%s no defineix %s", lang, key)
			}
		}
	}

	catholic, _ := core.GetConfessionalReligionCatalogByCode("catolicisme_ritu_llati")
	if got := core.ConfessionalReligionLabel(catholic, "en"); got != "Catholicism - Latin Rite" {
		t.Fatalf("label en de catolicisme_ritu_llati=%q", got)
	}
	if got := core.ConfessionalReligionLabel(catholic, "cat"); got != "Catolicisme - Ritu llatí" {
		t.Fatalf("label cat de catolicisme_ritu_llati=%q", got)
	}
	parish, _ := core.GetConfessionalLevelCatalogByCode("parroquia")
	if got := core.ConfessionalLevelLabel(parish, "en"); got != "Parish" {
		t.Fatalf("label en de parroquia=%q", got)
	}
	if got := core.ConfessionalLevelLabel(parish, "cat"); got != "Parròquia" {
		t.Fatalf("label cat de parroquia=%q", got)
	}
	if got := core.ConfessionalCategoryLabel("confessio", "en"); got != "Confession" {
		t.Fatalf("label categoria en=%q", got)
	}
}

func TestF353Z7EntitySelectorUsesSelectableI18NLabelsAndStableCodes(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z7_selector.sqlite3")
	session := f353YAdminSession(t, database, "z7_selector")

	bodyCat := f353Z7Get(t, app.AdminNewConfessional, "/confessional/entitats/new", session, "cat")
	for _, code := range []string{
		"catolicisme_ritu_llati",
		"ortodoxia",
		"anglicanisme",
		"islam",
		"judaisme",
		"budisme",
		"sintoisme",
		"fe_bahai",
	} {
		if !strings.Contains(bodyCat, `value="`+code+`"`) {
			t.Fatalf("el selector cat no conte el codi estable %s; body=%s", code, bodyCat)
		}
	}
	for _, code := range []string{"cristianisme", "protestantisme", "esglesies_baptistes"} {
		if strings.Contains(bodyCat, `value="`+code+`"`) {
			t.Fatalf("%s no ha d'apareixer al selector normal sense nivells directes; body=%s", code, bodyCat)
		}
	}
	if !strings.Contains(bodyCat, "Catolicisme - Ritu llatí") || !strings.Contains(bodyCat, "Fe bahá&#39;í") {
		t.Fatalf("el selector cat ha de mostrar labels i18n coherents; body=%s", bodyCat)
	}
	santaOption := f353Z5OptionSnippet(bodyCat, "santa_seu")
	if !strings.Contains(santaOption, `data-religion-code="catolicisme_ritu_llati"`) {
		t.Fatalf("santa_seu ha de conservar data-religion-code; option=%s", santaOption)
	}
	if !strings.Contains(bodyCat, `<script src="/static/js/confessional-form.js"></script>`) {
		t.Fatalf("el JS confessional ha de continuar carregant-se com a fitxer extern")
	}
	if strings.Contains(bodyCat, "<script>\n") || strings.Contains(bodyCat, `onchange=`) || strings.Contains(bodyCat, `onsubmit=`) {
		t.Fatalf("el formulari no ha de reintroduir JS inline")
	}

	bodyEn := f353Z7Get(t, app.AdminNewConfessional, "/confessional/entitats/new", session, "en")
	if !strings.Contains(bodyEn, "Catholicism - Latin Rite") || !strings.Contains(bodyEn, "Holy See") || !strings.Contains(bodyEn, "Parish") {
		t.Fatalf("el selector en ha d'usar labels anglesos; body=%s", bodyEn)
	}
	if strings.Contains(bodyEn, "Catolicisme - Ritu llatí") || strings.Contains(bodyEn, "Parròquia") {
		t.Fatalf("el selector en no ha de quedar tot en catala; body=%s", bodyEn)
	}
}

func TestF353Z7ConfessionalLevelCompatibilityRegression(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z7_compat.sqlite3")
	session := f353YAdminSession(t, database, "z7_compat")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	for _, tc := range []struct {
		name  string
		level string
	}{
		{name: "santa_seu", level: "santa_seu"},
		{name: "parroquia", level: "parroquia"},
	} {
		form := f353ZEntityForm("Entitat F35-3Z7 valida "+tc.name+" "+suffix, "f35_3z7_ok_"+tc.name+"_"+suffix, 0)
		form.Set("religio_confessio_codi", "catolicisme_ritu_llati")
		form.Set("nivell_confessional_codi", tc.level)
		_ = f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, session, form)
	}

	for _, tc := range []struct {
		name     string
		religion string
		level    string
	}{
		{name: "cristianisme_santa_seu", religion: "cristianisme", level: "santa_seu"},
		{name: "islam_parroquia", religion: "islam", level: "parroquia"},
	} {
		form := f353ZEntityForm("Entitat F35-3Z7 invalida "+tc.name+" "+suffix, "f35_3z7_bad_"+tc.name+"_"+suffix, 0)
		form.Set("religio_confessio_codi", tc.religion)
		form.Set("nivell_confessional_codi", tc.level)
		body := f353YPostConfessional(t, app.AdminSaveConfessional, session, form)
		if !strings.Contains(body, "no es compatible") {
			t.Fatalf("%s ha de continuar rebutjant-se server-side; body=%s", tc.name, body)
		}
	}
}

func readLocaleF353Z7(t *testing.T, root, lang string) map[string]string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(root, "locales", lang+".json"))
	if err != nil {
		t.Fatalf("no s'ha pogut llegir locale %s: %v", lang, err)
	}
	var values map[string]string
	if err := json.Unmarshal(body, &values); err != nil {
		t.Fatalf("locale %s no es JSON valid: %v", lang, err)
	}
	return values
}

func f353Z7Get(t *testing.T, handler http.HandlerFunc, path string, session *http.Cookie, lang string) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.AddCookie(session)
	req.AddCookie(&http.Cookie{Name: "lang", Value: lang})
	rr := httptest.NewRecorder()
	handler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET %s status=%d body=%s", path, rr.Code, rr.Body.String())
	}
	return rr.Body.String()
}
