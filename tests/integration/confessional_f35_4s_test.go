package integration

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF354SDiagnosticRouteMenuAndLocaleFiles(t *testing.T) {
	root := findProjectRoot(t)

	mainBody := readProjectFileF354S(t, root, "main.go")
	menuBody := readProjectFileF354S(t, root, "templates/layouts/menu-private.html")
	templateBody := readProjectFileF354S(t, root, "templates/admin-confessional-diagnostic.html")
	permissionsBody := readProjectFileF354S(t, root, "core/permissions_modular.go")
	templatesBody := readProjectFileF354S(t, root, "core/templates.go")

	for _, token := range []string{
		`/confessional/diagnostic`,
		`app.AdminConfessionalDiagnostic`,
	} {
		if !strings.Contains(mainBody, token) {
			t.Fatalf("main.go no conté el contracte F35-4S %q", token)
		}
	}
	for _, token := range []string{
		`CanViewConfessionalDiagnostic`,
		`/confessional/diagnostic`,
		`confessional.menu.diagnostic`,
	} {
		if !strings.Contains(menuBody, token) {
			t.Fatalf("menu-private.html no conté el contracte F35-4S %q", token)
		}
	}
	for _, token := range []string{
		`territori.confessional.diagnostic.view`,
		`CanViewConfessionalDiagnostic`,
	} {
		if !strings.Contains(permissionsBody+templatesBody, token) {
			t.Fatalf("permis o flag de template absent: %q", token)
		}
	}
	for _, forbidden := range []string{
		`confessional-tabs`,
		`onclick=`,
		`onchange=`,
		`oninput=`,
		`onsubmit=`,
	} {
		if strings.Contains(templateBody, forbidden) {
			t.Fatalf("la nova pantalla no ha de contenir JS inline ni tabs legacy: %q", forbidden)
		}
	}
	for _, rel := range []string{"locales/cat.json", "locales/en.json", "locales/oc.json"} {
		body := readProjectFileF354S(t, root, rel)
		for _, token := range []string{
			`"confessional.menu.diagnostic"`,
			`"confessional.diagnostic.title"`,
			`"confessional.diagnostic.type.missing_parent"`,
			`"confessional.diagnostic.summary.published_entities"`,
		} {
			if !strings.Contains(body, token) {
				t.Fatalf("%s no conté la clau i18n F35-4S %q", rel, token)
			}
		}
	}
}

func TestF354SDiagnosticRequiresServerPermissionAndRendersForDiagnosticViewer(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4s_permission.sqlite3")

	rr := f354SDiagnosticGET(app.AdminConfessionalDiagnostic, "/confessional/diagnostic", nil)
	if rr.Code == http.StatusOK {
		t.Fatalf("sense sessio la ruta no ha de ser publica; body=%s", rr.Body.String())
	}

	_, diagCookie := createF335PlatformUser(t, database, "f35_4s_diag", "territori.confessional.diagnostic.view")
	rr = f354SDiagnosticGET(app.AdminConfessionalDiagnostic, "/confessional/diagnostic", diagCookie)
	if rr.Code != http.StatusOK {
		t.Fatalf("amb permis diagnostic s'esperava 200, got=%d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	for _, token := range []string{
		`confessionalDiagnosticFilters`,
		`/confessional/diagnostic`,
		`name="severity"`,
		`name="alert_type"`,
	} {
		if !strings.Contains(body, token) {
			t.Fatalf("la resposta diagnostic no conté %q; body=%s", token, body)
		}
	}
	if strings.Contains(body, "confessional-tabs") {
		t.Fatalf("la resposta diagnostic no ha de reintroduir confessional-tabs")
	}
}

func TestF354SDiagnosticDetectsAlertsCoverageAndSafeFilters(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_4s_alerts.sqlite3")
	session := f353YAdminSession(t, database, "f35_4s_alerts")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	rootID := f354SSaveEntitat(t, database, "f35_4s_root_"+suffix, "Santa Seu F35-4S "+suffix, "catolicisme_ritu_llati", "santa_seu", "publicat")
	diocesiA := f354SSaveEntitat(t, database, "f35_4s_dioc_a_"+suffix, "Bisbat A F35-4S "+suffix, "catolicisme_ritu_llati", "bisbat_diocesi", "publicat")
	diocesiB := f354SSaveEntitat(t, database, "f35_4s_dioc_b_"+suffix, "Bisbat B F35-4S "+suffix, "catolicisme_ritu_llati", "bisbat_diocesi", "publicat")
	f354SSaveEntitatRelacio(t, database, rootID, diocesiA, "bisbat_diocesi", "publicat")
	f354SSaveEntitatRelacio(t, database, rootID, diocesiB, "bisbat_diocesi", "publicat")

	orphanParishName := "Parroquia orfena F35-4S " + suffix
	orphanParishID := f354SSaveEntitat(t, database, "f35_4s_orphan_"+suffix, orphanParishName, "catolicisme_ritu_llati", "parroquia", "publicat")
	f354SSaveEntitatRelacio(t, database, diocesiA, orphanParishID, "parroquia", "pendent")

	multiChildName := "Parroquia multiparent F35-4S " + suffix
	multiChildID := f354SSaveEntitat(t, database, "f35_4s_multi_"+suffix, multiChildName, "catolicisme_ritu_llati", "parroquia", "publicat")
	f354SSaveEntitatRelacio(t, database, diocesiA, multiChildID, "parroquia", "publicat")
	f354SSaveEntitatRelacio(t, database, diocesiB, multiChildID, "parroquia", "publicat")

	rejectedChildName := "Parroquia rebutjada F35-4S " + suffix
	rejectedChildID := f354SSaveEntitat(t, database, "f35_4s_rejected_"+suffix, rejectedChildName, "catolicisme_ritu_llati", "parroquia", "rebutjat")
	f354SSaveEntitatRelacio(t, database, diocesiA, rejectedChildID, "parroquia", "publicat")

	pendingDuplicateName := "Parroquia pendent duplicada F35-4S " + suffix
	pendingDuplicateID := f354SSaveEntitat(t, database, "f35_4s_pending_dup_"+suffix, pendingDuplicateName, "catolicisme_ritu_llati", "parroquia", "pendent")
	f354SSaveEntitatRelacio(t, database, diocesiA, pendingDuplicateID, "parroquia", "pendent")
	f354SSaveEntitatRelacio(t, database, diocesiB, pendingDuplicateID, "parroquia", "pendent")

	pendingParentID := f354SSaveEntitat(t, database, "f35_4s_pending_parent_"+suffix, "Bisbat pendent F35-4S "+suffix, "catolicisme_ritu_llati", "bisbat_diocesi", "pendent")
	pendingHiddenName := "Parroquia pendent amagada F35-4S " + suffix
	pendingHiddenID := f354SSaveEntitat(t, database, "f35_4s_pending_hidden_"+suffix, pendingHiddenName, "catolicisme_ritu_llati", "parroquia", "pendent")
	f354SSaveEntitatRelacio(t, database, pendingParentID, pendingHiddenID, "parroquia", "pendent")

	duplicateName := "Seu duplicada F35-4S " + suffix
	f354SSaveEntitat(t, database, "dup_root_a_"+suffix, duplicateName, "catolicisme_ritu_llati", "santa_seu", "publicat")
	f354SSaveEntitat(t, database, "dup_root_b_"+suffix, duplicateName, "catolicisme_ritu_llati", "santa_seu", "publicat")

	wilayaName := "Wilaya incompatible F35-4S " + suffix
	wilayaID := f354SSaveEntitat(t, database, "f35_4s_wilaya_"+suffix, wilayaName, "islam", "islam_wilaya", "publicat")
	incompatibleChildName := "Parroquia incompatible F35-4S " + suffix
	incompatibleChildID := f354SSaveEntitat(t, database, "f35_4s_incompat_"+suffix, incompatibleChildName, "catolicisme_ritu_llati", "parroquia", "publicat")
	f354SSaveEntitatRelacio(t, database, wilayaID, incompatibleChildID, "parroquia", "publicat")

	coveredMunicipiID := f353YCreateMunicipi(t, database, "Municipi cobert F35-4S "+suffix)
	uncoveredMunicipiName := "Municipi descobert F35-4S " + suffix
	uncoveredMunicipiID := f353YCreateMunicipi(t, database, uncoveredMunicipiName)
	f354SSaveMunicipiEntitatRelacio(t, database, coveredMunicipiID, multiChildID, "principal", "publicat")

	archiveNoContextName := "Arxiu Parroquial sense context F35-4S " + suffix
	archiveNoContextID := f354SCreateArxiu(t, database, archiveNoContextName, coveredMunicipiID)
	archiveAmbiguousName := "Arxiu Parroquial generic F35-4S " + suffix
	archiveAmbiguousID := f354SCreateArxiu(t, database, archiveAmbiguousName, uncoveredMunicipiID)
	f354SSaveArxiuEntitatRelacio(t, database, archiveAmbiguousID, diocesiA, "context_religios", "publicat")
	f354SSaveArxiuEntitatRelacio(t, database, archiveAmbiguousID, diocesiB, "context_religios", "publicat")
	_ = archiveNoContextID

	rr := f354SDiagnosticGET(app.AdminConfessionalDiagnostic, "/confessional/diagnostic", session)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /confessional/diagnostic status=%d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	for _, token := range []string{
		orphanParishName,
		"Santa Seu F35-4S " + suffix,
		multiChildName,
		pendingDuplicateName,
		pendingHiddenName,
		wilayaName,
		incompatibleChildName,
		rejectedChildName,
		uncoveredMunicipiName,
		archiveNoContextName,
		archiveAmbiguousName,
		duplicateName,
		"Catolicisme",
		"Islam",
		"Relació incompatible",
		"Múltiples pares",
		"Arxiu sense context religiós",
	} {
		if !strings.Contains(body, token) {
			t.Fatalf("la diagnostica no mostra %q; body=%s", token, body)
		}
	}

	filterPath := "/confessional/diagnostic?severity=critical&alert_type=missing_parent&religio_confessio_codi=catolicisme_ritu_llati&q=" + url.QueryEscape("orfena "+suffix)
	rr = f354SDiagnosticGET(app.AdminConfessionalDiagnostic, filterPath, session)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET filtrat diagnostic status=%d body=%s", rr.Code, rr.Body.String())
	}
	filteredBody := rr.Body.String()
	if !strings.Contains(filteredBody, orphanParishName) {
		t.Fatalf("el filtre critic+missing_parent ha de mantenir l'orfe: %s", filteredBody)
	}
	for _, unexpected := range []string{archiveNoContextName, archiveAmbiguousName, uncoveredMunicipiName} {
		if strings.Contains(filteredBody, unexpected) {
			t.Fatalf("el filtre critic+missing_parent no ha de mostrar %q; body=%s", unexpected, filteredBody)
		}
	}

	malicious := "/confessional/diagnostic?religio_confessio_codi=%3Cscript%3E&nivell_confessional_codi=drop&severity=boom&alert_type=hack&q=" + url.QueryEscape("<script>alert(1)</script>")
	rr = f354SDiagnosticGET(app.AdminConfessionalDiagnostic, malicious, session)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET diagnostic malicios no ha de fallar: status=%d body=%s", rr.Code, rr.Body.String())
	}
	maliciousBody := rr.Body.String()
	if strings.Contains(maliciousBody, "<script>alert(1)</script>") {
		t.Fatalf("la query maliciosa no ha d'apareixer sense escapar al body: %s", maliciousBody)
	}
	if !strings.Contains(maliciousBody, "confessionalDiagnosticFilters") {
		t.Fatalf("la pagina ha de continuar renderitzant-se amb filtres maliciosos: %s", maliciousBody)
	}
}

func TestF354S1DiagnosticI18NKeysAreCompleteCleanAndConsistent(t *testing.T) {
	root := findProjectRoot(t)
	locales := map[string]map[string]string{}
	for _, lang := range []string{"cat", "en", "oc"} {
		var values map[string]string
		body := readProjectFileF354S(t, root, "locales/"+lang+".json")
		if err := json.Unmarshal([]byte(body), &values); err != nil {
			t.Fatalf("locales/%s.json no es JSON valid: %v", lang, err)
		}
		locales[lang] = values
	}

	keys := f354S1DiagnosticLocaleKeys(locales["cat"])
	if len(keys) != 62 {
		t.Fatalf("s'esperaven 62 claus i18n F35-4S, got=%d", len(keys))
	}

	placeholderRE := regexp.MustCompile(`%[sd]`)
	ocBrokenWordQuestionRE := regexp.MustCompile(`\pL\?\pL|\pL\?(?:\P{L}|$)|(?:^|\P{L})\?\pL`)
	mojibake := []string{"Ãƒ", "Ã‚", "Ã¢", "ï¿½"}
	for _, key := range keys {
		catValue := locales["cat"][key]
		catPlaceholders := placeholderRE.FindAllString(catValue, -1)
		for _, lang := range []string{"cat", "en", "oc"} {
			value, ok := locales[lang][key]
			if !ok {
				t.Fatalf("falta %s a locales/%s.json", key, lang)
			}
			if strings.TrimSpace(value) == "" {
				t.Fatalf("%s a locales/%s.json es buit", key, lang)
			}
			for _, bad := range mojibake {
				if strings.Contains(value, bad) {
					t.Fatalf("%s a locales/%s.json conte mojibake %q: %q", key, lang, bad, value)
				}
			}
			if lang == "oc" && ocBrokenWordQuestionRE.MatchString(value) {
				t.Fatalf("%s a locales/oc.json conte '?' dins una paraula: %q", key, value)
			}
			placeholders := placeholderRE.FindAllString(value, -1)
			if strings.Join(placeholders, "|") != strings.Join(catPlaceholders, "|") {
				t.Fatalf("%s a locales/%s.json te placeholders %v, esperats %v", key, lang, placeholders, catPlaceholders)
			}
		}
	}

	for key, badValue := range map[string]string{
		"confessional.menu.diagnostic":                               "Diagnostic de qualitat",
		"confessional.diagnostic.title":                              "Diagnostic de qualitat confessional",
		"confessional.diagnostic.summary.critical_alerts":            "Alertes critiques",
		"confessional.diagnostic.severity.critical":                  "Critica",
		"confessional.diagnostic.severity.warning":                   "Avis",
		"confessional.diagnostic.type.incompatible_relation":         "Relacio incompatible",
		"confessional.diagnostic.type.multiple_parents":              "Multiples pares",
		"confessional.diagnostic.type.pending_relation_inconsistent": "Relacio pendent incoherent",
		"confessional.diagnostic.col.description":                    "Descripcio",
		"confessional.diagnostic.coverage.title":                     "Cobertura per religio/confessio",
		"confessional.diagnostic.context.inferred_municipality":      "Municipi deduible",
		"confessional.diagnostic.message.issue.self_relation":        "La relacio apunta a la mateixa entitat.",
		"confessional.diagnostic.message.pending_relation.duplicate": "La filla pendent te multiples relacions inicials pendents.",
		"confessional.diagnostic.message.archive_without_context":    "L'arxiu sembla religios pero no te cap relacio religiosa publicada.",
		"confessional.diagnostic.message.archive_context_ambiguous":  "L'arxiu te multiples relacions religioses massa generiques o sense tipus clar.",
	} {
		if locales["cat"][key] == badValue {
			t.Fatalf("%s conserva el valor catala degradat %q", key, badValue)
		}
	}
}

func f354S1DiagnosticLocaleKeys(values map[string]string) []string {
	keys := make([]string, 0, 62)
	for key := range values {
		if key == "confessional.menu.diagnostic" || strings.HasPrefix(key, "confessional.diagnostic.") {
			keys = append(keys, key)
		}
	}
	return keys
}

func f354SDiagnosticGET(handler http.HandlerFunc, path string, session *http.Cookie) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Accept-Language", "ca")
	if session != nil {
		req.AddCookie(session)
	}
	rr := httptest.NewRecorder()
	handler(rr, req)
	return rr
}

func f354SSaveEntitat(t *testing.T, database db.DB, code, name, religionCode, levelCode, status string) int {
	t.Helper()
	id, err := database.SaveEntitatReligiosa(&db.EntitatReligiosa{
		Codi:                   code,
		Nom:                    name,
		ReligioConfessioCodi:   religionCode,
		NivellConfessionalCodi: levelCode,
		Estat:                  "actiu",
		ModeracioEstat:         status,
	})
	if err != nil {
		t.Fatalf("SaveEntitatReligiosa %s: %v", name, err)
	}
	return id
}

func f354SSaveEntitatRelacio(t *testing.T, database db.DB, parentID, childID int, relationType, status string) int {
	t.Helper()
	id, err := database.SaveEntitatReligiosaRelacio(&db.EntitatReligiosaRelacio{
		EntitatOrigenID: parentID,
		EntitatDestiID:  childID,
		TipusRelacio:    relationType,
		ModeracioEstat:  status,
	})
	if err != nil {
		t.Fatalf("SaveEntitatReligiosaRelacio %d->%d: %v", parentID, childID, err)
	}
	return id
}

func f354SSaveMunicipiEntitatRelacio(t *testing.T, database db.DB, municipiID, entitatID int, relationType, status string) int {
	t.Helper()
	id, err := database.SaveMunicipiEntitatReligiosa(&db.MunicipiEntitatReligiosa{
		MunicipiID:         municipiID,
		EntitatReligiosaID: entitatID,
		TipusRelacio:       relationType,
		ModeracioEstat:     status,
	})
	if err != nil {
		t.Fatalf("SaveMunicipiEntitatReligiosa municipi=%d entitat=%d: %v", municipiID, entitatID, err)
	}
	return id
}

func f354SSaveArxiuEntitatRelacio(t *testing.T, database db.DB, arxiuID, entitatID int, relationType, status string) int {
	t.Helper()
	id, err := database.SaveArxiuEntitatReligiosa(&db.ArxiuEntitatReligiosa{
		ArxiuID:            arxiuID,
		EntitatReligiosaID: entitatID,
		TipusRelacio:       relationType,
		Estat:              "actiu",
		ModeracioEstat:     status,
	})
	if err != nil {
		t.Fatalf("SaveArxiuEntitatReligiosa arxiu=%d entitat=%d: %v", arxiuID, entitatID, err)
	}
	return id
}

func f354SCreateArxiu(t *testing.T, database db.DB, name string, municipiID int) int {
	t.Helper()
	id, err := database.CreateArxiu(&db.Arxiu{
		Nom:            name,
		Tipus:          "parroquia",
		Acces:          "online",
		MunicipiID:     sql.NullInt64{Int64: int64(municipiID), Valid: true},
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateArxiu %s: %v", name, err)
	}
	return id
}

func readProjectFileF354S(t *testing.T, root, rel string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("no s'ha pogut llegir %s: %v", rel, err)
	}
	return string(body)
}
