package integration

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestF353SConfessionalMenuIsTopLevelAndTerritoryStaysClean(t *testing.T) {
	root := findProjectRoot(t)
	menuBody := readProjectFileF353S(t, root, "templates/layouts/menu-private.html")
	templatesBody := readProjectFileF353S(t, root, "core/templates.go")

	territoryStart := strings.Index(menuBody, "menu.section.territory")
	if territoryStart == -1 {
		t.Fatalf("falta la seccio Territori al menu")
	}
	confessionalStart := strings.Index(menuBody, "confessional.menu.section")
	if confessionalStart == -1 {
		t.Fatalf("falta Religios/confessional com a seccio principal")
	}
	if confessionalStart <= territoryStart {
		t.Fatalf("Religios/confessional no queda separat despres de Territori")
	}
	territoryBlock := menuBody[territoryStart:confessionalStart]
	if strings.Contains(territoryBlock, "confessional.menu.section") || strings.Contains(territoryBlock, "/territori/confessional/") {
		t.Fatalf("Territori encara conte entrades confessionals")
	}
	for _, route := range []string{
		"/confessional/religions",
		"/confessional/nivells",
		"/confessional/entitats",
		"/confessional/relacions-entitats",
		"/confessional/municipis-entitats",
	} {
		if !strings.Contains(menuBody, route) {
			t.Fatalf("falta ruta al menu confessional: %s", route)
		}
	}
	territoryManageStart := strings.Index(templatesBody, "func hasModularTerritoryManageKey")
	nivellsStart := strings.Index(templatesBody, "func hasModularNivellsViewKey")
	if territoryManageStart == -1 || nivellsStart == -1 || nivellsStart <= territoryManageStart {
		t.Fatalf("no es pot delimitar hasModularTerritoryManageKey")
	}
	if strings.Contains(templatesBody[territoryManageStart:nivellsStart], "Confessional") {
		t.Fatalf("els permisos confessionals no han d'obrir la seccio Territori")
	}
	if strings.Contains(templatesBody, `m["CanViewEcles"].(bool) || m["CanViewConfessional"].(bool)`) {
		t.Fatalf("CanViewTerritory encara depen de CanViewConfessional")
	}
}

func TestF353SConfessionalCRUDCannotPublishDirectly(t *testing.T) {
	root := findProjectRoot(t)
	handlerBody := readProjectFileF353S(t, root, "core/admin_confessional.go")
	formBody := readProjectFileF353S(t, root, "templates/admin-confessional-form.html")

	if strings.Contains(formBody, `name="moderation_status"`) || strings.Contains(formBody, `id="moderation_status"`) {
		t.Fatalf("el formulari confessional no ha d'exposar un selector editable de moderacio")
	}
	if strings.Contains(handlerBody, `r.FormValue("moderation_status")`) {
		t.Fatalf("el POST confessional no ha de llegir moderation_status manipulat")
	}
	if got := strings.Count(handlerBody, `ModeracioEstat: "pendent"`); got < 4 {
		t.Fatalf("les altes confessionals actives han de quedar pendents; defaults detectats=%d", got)
	}
	if !strings.Contains(handlerBody, "confessionalModerationStatusForSave(kind, id)") {
		t.Fatalf("el guardat ha de calcular l'estat de moderacio al servidor")
	}
}

func TestF353SConfessionalRelationNucliIsFilteredAndValidated(t *testing.T) {
	root := findProjectRoot(t)
	handlerBody := readProjectFileF353S(t, root, "core/admin_confessional.go")
	formBody := readProjectFileF353S(t, root, "templates/admin-confessional-form.html")

	for _, token := range []string{
		`"Nuclis":`,
		"compatibleNucliRows",
		"full.MunicipiID.Int64 != int64(municipiID)",
		"nucli.MunicipiID.Int64 != int64(item.MunicipiID)",
		"El nucli indicat no pertany al municipi seleccionat.",
	} {
		if !strings.Contains(handlerBody, token) {
			t.Fatalf("falta control de nucli compatible: %s", token)
		}
	}
	if !strings.Contains(formBody, "range .Data.Nuclis") {
		t.Fatalf("el selector de nucli ha d'usar nomes nuclis filtrats")
	}
	if strings.Contains(formBody, "range .Data.Municipis }}\n                        <option value=\"{{ .ID }}\" {{ if and $.Data.Form.Relacio.NucliID.Valid") {
		t.Fatalf("el selector de nucli encara mostra tots els municipis")
	}
}

func TestF353SConfessionalListUsesAdminTableLayout(t *testing.T) {
	root := findProjectRoot(t)
	body := readProjectFileF353S(t, root, "templates/admin-confessional-list.html")

	for _, token := range []string{
		`/static/css/registres-taula.css`,
		`class="contingut-principal taula-ample"`,
		`class="confessional-tabs"`,
		`class="taula-resultats"`,
		`class="taula-container"`,
		`class="taula taula-registres"`,
		`class="confessional-empty"`,
		`class="confessional-actions"`,
	} {
		if !strings.Contains(body, token) {
			t.Fatalf("falta layout administratiu confessional: %s", token)
		}
	}
}

func readProjectFileF353S(t *testing.T, root, rel string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("no s'ha pogut llegir %s: %v", rel, err)
	}
	return string(body)
}
