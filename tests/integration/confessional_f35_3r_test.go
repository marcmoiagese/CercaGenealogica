package integration

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestF353RConfessionalRoutesMenuAndPermissionsAreSeparated(t *testing.T) {
	root := findProjectRoot(t)
	mainBody := readProjectFileF353R(t, root, "main.go")
	handlerBody := readProjectFileF353R(t, root, "core/admin_confessional.go")
	menuBody := readProjectFileF353R(t, root, "templates/layouts/menu-private.html")
	permsBody := readProjectFileF353R(t, root, "core/permissions_modular.go")

	for _, route := range []string{
		"/confessional/religions",
		"/confessional/nivells",
		"/confessional/entitats",
		"/confessional/relacions-entitats",
		"/confessional/municipis-entitats",
	} {
		if !strings.Contains(menuBody, route) && !strings.Contains(mainBody, route) {
			t.Fatalf("falta ruta confessional separada %s", route)
		}
	}
	if !strings.Contains(handlerBody, "http.Redirect(w, r, \"/confessional/entitats\"") {
		t.Fatalf("/confessional ha de redirigir a una seccio, no renderitzar la pantalla agregada")
	}
	if !strings.Contains(mainBody, "/territori/confessional") || !strings.Contains(mainBody, "compatibilitat historica") {
		t.Fatalf("les rutes antigues /territori/confessional han de quedar documentades com compatibilitat")
	}
	if strings.Contains(menuBody, "Model religios/confessional</a>") {
		t.Fatalf("el menu no ha de conservar l'enllac unic agregat antic")
	}
	if !strings.Contains(menuBody, "confessional.menu.section") {
		t.Fatalf("falta submenu Religios/confessional")
	}
	for _, key := range []string{
		"territori.confessional.religions.view",
		"territori.confessional.religions.create",
		"territori.confessional.religions.edit",
		"territori.confessional.religions.delete",
		"territori.confessional.models.view",
		"territori.confessional.models.create",
		"territori.confessional.models.edit",
		"territori.confessional.models.delete",
		"territori.confessional.nivells.view",
		"territori.confessional.nivells.create",
		"territori.confessional.nivells.edit",
		"territori.confessional.nivells.delete",
		"territori.confessional.entitats.view",
		"territori.confessional.entitats.create",
		"territori.confessional.entitats.edit",
		"territori.confessional.entitats.delete",
		"territori.confessional.relacions_entitats.view",
		"territori.confessional.relacions_entitats.create",
		"territori.confessional.relacions_entitats.edit",
		"territori.confessional.relacions_entitats.delete",
		"territori.confessional.municipis_entitats.view",
		"territori.confessional.municipis_entitats.create",
		"territori.confessional.municipis_entitats.edit",
		"territori.confessional.municipis_entitats.delete",
	} {
		if !strings.Contains(permsBody, key) {
			t.Fatalf("falta permis granular %s", key)
		}
	}
	for _, token := range []string{"section.ViewPerm", "section.CreatePerm", "section.EditPerm", "section.DeletePerm"} {
		if !strings.Contains(handlerBody, token) {
			t.Fatalf("handlers confessionals no validen %s", token)
		}
	}
}

func TestF353RConfessionalDBNoLongerDelegatesCRUDToSQLCommon(t *testing.T) {
	root := findProjectRoot(t)
	for _, rel := range []string{"db/sqlite.go", "db/postgres.go", "db/mysql.go"} {
		body := readProjectFileF353R(t, root, rel)
		for _, forbidden := range []string{
			"d.help.listReligioConfessions",
			"d.help.saveReligioConfessio",
			"d.help.listModelsConfessionals",
			"d.help.saveModelConfessional",
			"d.help.listNivellsConfessionals",
			"d.help.saveNivellConfessional",
			"d.help.listEntitatsReligioses",
			"d.help.saveEntitatReligiosa",
			"d.help.listMunicipiEntitatsReligioses",
			"d.help.saveMunicipiEntitatReligiosa",
		} {
			if strings.Contains(body, forbidden) {
				t.Fatalf("%s encara delega CRUD confessional a sqlcommon: %s", rel, forbidden)
			}
		}
	}
	perMotorBody := readProjectFileF353R(t, root, "db/confessional_per_motor.go")
	for _, token := range []string{"datetime('now')", "NOW()", "RETURNING id", "LastInsertId", "$1", "municipi_entitat_religiosa"} {
		if !strings.Contains(perMotorBody, token) {
			t.Fatalf("db/confessional_per_motor.go no mostra estrategia multi-motor: falta %s", token)
		}
	}
}

func readProjectFileF353R(t *testing.T, root, rel string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("no s'ha pogut llegir %s: %v", rel, err)
	}
	return string(body)
}
