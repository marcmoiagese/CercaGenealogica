package integration

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF353Z5ConfessionalEntityFormFiltersLevelsByReligion(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z5_form_filter.sqlite3")
	session := f353YAdminSession(t, database, "z5_filter")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	catholicID, err := database.SaveEntitatReligiosa(&db.EntitatReligiosa{
		Codi:                   "f35_3z5_catholic_" + suffix,
		Nom:                    "Santa Seu F35-3Z5 " + suffix,
		ReligioConfessioCodi:   "catolicisme_ritu_llati",
		NivellConfessionalCodi: "santa_seu",
		Estat:                  "actiu",
		ModeracioEstat:         "publicat",
	})
	if err != nil {
		t.Fatalf("SaveEntitatReligiosa catholic: %v", err)
	}
	catholicBody := f353YGet(t, app.AdminEditConfessional, "/confessional/entitats/"+strconv.Itoa(catholicID)+"/edit", session)
	catholicSantaOption := f353Z5OptionSnippet(catholicBody, "santa_seu")
	if !strings.Contains(catholicSantaOption, `data-religion-code="catolicisme_ritu_llati"`) || !strings.Contains(catholicSantaOption, "selected") {
		t.Fatalf("el formulari catolic ha de permetre Santa Seu; option=%s body=%s", catholicSantaOption, catholicBody)
	}
	if !strings.Contains(catholicBody, "Provincia eclesiastica") || !strings.Contains(catholicBody, "Arxiprestat / Vicariat forani") || !strings.Contains(catholicBody, "Parroquia") {
		t.Fatalf("el formulari catolic ha de mostrar nivells compatibles principals; body=%s", catholicBody)
	}

	invalidID, err := database.SaveEntitatReligiosa(&db.EntitatReligiosa{
		Codi:                   "f35_3z5_invalid_" + suffix,
		Nom:                    "Cristianisme Santa Seu F35-3Z5 " + suffix,
		ReligioConfessioCodi:   "cristianisme",
		NivellConfessionalCodi: "santa_seu",
		Estat:                  "actiu",
		ModeracioEstat:         "publicat",
	})
	if err != nil {
		t.Fatalf("SaveEntitatReligiosa invalid fixture: %v", err)
	}
	invalidBody := f353YGet(t, app.AdminEditConfessional, "/confessional/entitats/"+strconv.Itoa(invalidID)+"/edit", session)
	santaOption := f353Z5OptionSnippet(invalidBody, "santa_seu")
	if strings.Contains(santaOption, "selected") {
		t.Fatalf("Santa Seu no pot quedar seleccionat amb Cristianisme; option=%s body=%s", santaOption, invalidBody)
	}
	if !strings.Contains(santaOption, "hidden") || !strings.Contains(santaOption, "disabled") {
		t.Fatalf("Santa Seu ha de quedar ocult/deshabilitat amb Cristianisme; option=%s body=%s", santaOption, invalidBody)
	}
	if !strings.Contains(invalidBody, "Selecciona primer una religio/confessio concreta") {
		t.Fatalf("falta ajuda i18n curta per al selector de nivells; body=%s", invalidBody)
	}
}

func TestF353Z5ManipulatedPostsValidateConfessionalLevelCompatibility(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z5_post_validation.sqlite3")
	session := f353YAdminSession(t, database, "z5_post")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	for _, tc := range []struct {
		name     string
		religion string
		level    string
	}{
		{name: "cristianisme_santa_seu", religion: "cristianisme", level: "santa_seu"},
		{name: "cristianisme_provincia", religion: "cristianisme", level: "provincia_eclesiastica"},
		{name: "islam_parroquia", religion: "islam", level: "parroquia"},
		{name: "budisme_arxiprestat", religion: "budisme", level: "arxiprestat_vicariat_forani"},
	} {
		form := f353ZEntityForm("Entitat F35-3Z5 invalida "+tc.name+" "+suffix, "f35_3z5_bad_"+tc.name+"_"+suffix, 0)
		form.Set("religio_confessio_codi", tc.religion)
		form.Set("nivell_confessional_codi", tc.level)
		body := f353YPostConfessional(t, app.AdminSaveConfessional, session, form)
		if !strings.Contains(body, "no es compatible") {
			t.Fatalf("%s ha de donar error de compatibilitat; body=%s", tc.name, body)
		}
		if strings.Contains(body, "no esta publicat") {
			t.Fatalf("%s no ha de confondre compatibilitat amb publicacio; body=%s", tc.name, body)
		}
	}

	for _, tc := range []struct {
		name  string
		level string
	}{
		{name: "santa_seu", level: "santa_seu"},
		{name: "provincia", level: "provincia_eclesiastica"},
	} {
		form := f353ZEntityForm("Entitat F35-3Z5 valida "+tc.name+" "+suffix, "f35_3z5_ok_"+tc.name+"_"+suffix, 0)
		form.Set("religio_confessio_codi", "catolicisme_ritu_llati")
		form.Set("nivell_confessional_codi", tc.level)
		_ = f353ZPostConfessionalRedirect(t, app.AdminSaveConfessional, session, form)
		got := f353ZFindEntitatByName(t, database, form.Get("nom"))
		if got.ModeracioEstat != "pendent" || got.NivellConfessionalCodi != tc.level {
			t.Fatalf("entitat valida inesperada: %+v", got)
		}
	}
}

func TestF353Z5ConfessionalLevelMessagesAndClientSyncAreI18N(t *testing.T) {
	root := findProjectRoot(t)
	handlerBody := readProjectFileF353U(t, root, "core/admin_confessional.go")
	catalogBody := readProjectFileF353U(t, root, "core/confessional_catalog.go")
	formBody := readProjectFileF353U(t, root, "templates/admin-confessional-form.html")

	for _, token := range []string{
		"ConfessionalLevelCompatibleWithReligion",
		"confessional.error.religion_unknown",
		"confessional.error.level_unknown",
		"confessional.error.level_incompatible",
	} {
		if !strings.Contains(handlerBody+catalogBody, token) {
			t.Fatalf("falta validacio diferenciada F35-3Z5: %s", token)
		}
	}
	for _, token := range []string{
		`data-religion-code`,
		`level.disabled = visibleLevels === 0`,
		`form.addEventListener("submit"`,
		`confessional.help.levels.choose_religion`,
		`confessional.help.levels.none_for_religion`,
	} {
		if !strings.Contains(formBody, token) {
			t.Fatalf("falta sincronitzacio client-side F35-3Z5: %s", token)
		}
	}

	for _, rel := range []string{"locales/cat.json", "locales/en.json", "locales/oc.json"} {
		body := readProjectFileF353U(t, root, rel)
		var values map[string]string
		if err := json.Unmarshal([]byte(body), &values); err != nil {
			t.Fatalf("%s no es JSON valid: %v", rel, err)
		}
		for _, key := range []string{
			"confessional.error.religion_unknown",
			"confessional.error.level_unknown",
			"confessional.error.level_incompatible",
			"confessional.help.levels.choose_religion",
			"confessional.help.levels.none_for_religion",
		} {
			if strings.TrimSpace(values[key]) == "" {
				t.Fatalf("%s no defineix %s", rel, key)
			}
		}
	}
}

func f353Z5OptionSnippet(body, value string) string {
	marker := `value="` + value + `"`
	start := strings.Index(body, marker)
	if start < 0 {
		return ""
	}
	open := strings.LastIndex(body[:start], "<option")
	if open < 0 {
		open = start
	}
	close := strings.Index(body[start:], "</option>")
	if close < 0 {
		return body[open:]
	}
	return body[open : start+close+len("</option>")]
}
