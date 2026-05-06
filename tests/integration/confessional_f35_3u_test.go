package integration

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
	testcommon "github.com/marcmoiagese/CercaGenealogica/tests/common"
)

func TestF353USystemCatalogAndSimplifiedEntityFlowMultiDB(t *testing.T) {
	for _, env := range newConfessionalAppsForAllDBs(t) {
		t.Run(env.Label, func(t *testing.T) {
			if err := env.App.EnsureSystemConfessionalCatalogs(); err != nil {
				t.Fatalf("EnsureSystemConfessionalCatalogs: %v", err)
			}

			religions, err := env.DB.ListReligioConfessions()
			if err != nil {
				t.Fatalf("ListReligioConfessions: %v", err)
			}
			if len(religions) != 0 {
				t.Fatalf("EnsureSystemConfessionalCatalogs no ha de sembrar religions a DB: %+v", religions)
			}

			nivells, err := env.DB.ListNivellsConfessionals()
			if err != nil {
				t.Fatalf("ListNivellsConfessionals: %v", err)
			}
			if len(nivells) != 0 {
				t.Fatalf("EnsureSystemConfessionalCatalogs no ha de sembrar nivells a DB: %+v", nivells)
			}

			cristianisme, ok := core.GetConfessionalReligionCatalogByCode("cristianisme")
			if !ok || !cristianisme.SystemManaged || !cristianisme.Active || cristianisme.CategoryCode != "religio" {
				t.Fatalf("cristianisme no existeix com a cataleg de codi: %+v", cristianisme)
			}
			catolicisme, ok := core.GetConfessionalReligionCatalogByCode("catolicisme")
			if !ok || catolicisme.ParentCode != "cristianisme" {
				t.Fatalf("catolicisme no penja de cristianisme al cataleg de codi: %+v", catolicisme)
			}
			if _, ok := core.GetConfessionalReligionCatalogByCode("catolicisme_ritu_llati"); !ok {
				t.Fatalf("catolicisme_ritu_llati no existeix al cataleg de codi")
			}
			parroquia, ok := core.GetConfessionalLevelCatalogByCode("parroquia")
			if !ok || parroquia.ReligionCode != "catolicisme_ritu_llati" || !parroquia.CanLinkMunicipi {
				t.Fatalf("parroquia no existeix correctament al cataleg de codi: %+v", parroquia)
			}
			diocesi, ok := core.GetConfessionalLevelCatalogByCode("bisbat_diocesi")
			if !ok || diocesi.CategoryCode == "" {
				t.Fatalf("diocesi no existeix correctament al cataleg de codi: %+v", diocesi)
			}

			suffix := time.Now().Format("150405.000000000")
			entitatID, err := env.DB.SaveEntitatReligiosa(&db.EntitatReligiosa{
				Codi:                   "entitat_f35_3u_" + suffix,
				Nom:                    "Parroquia F35-3U " + suffix,
				ReligioConfessioCodi:   "catolicisme_ritu_llati",
				NivellConfessionalCodi: "parroquia",
				Estat:                  "actiu",
				Descripcio:             "flux simplificat sense model confessional",
				ModeracioEstat:         "publicat",
			})
			if err != nil {
				t.Fatalf("SaveEntitatReligiosa sense model: %v", err)
			}
			gotEntitat, err := env.DB.GetEntitatReligiosa(entitatID)
			if err != nil {
				t.Fatalf("GetEntitatReligiosa: %v", err)
			}
			if gotEntitat.ModelConfessionalID.Valid {
				t.Fatalf("l'entitat del flux normal no ha de requerir model_confessional: %+v", gotEntitat)
			}
			if gotEntitat.ReligioConfessioCodi != "catolicisme_ritu_llati" || gotEntitat.NivellConfessionalCodi != "parroquia" {
				t.Fatalf("l'entitat ha de persistir codis del cataleg: %+v", gotEntitat)
			}

			parentID, err := env.DB.SaveEntitatReligiosa(&db.EntitatReligiosa{
				Codi:                   "diocesi_f35_3u_" + suffix,
				Nom:                    "Diocesi F35-3U " + suffix,
				ReligioConfessioCodi:   "catolicisme_ritu_llati",
				NivellConfessionalCodi: "bisbat_diocesi",
				Estat:                  "actiu",
				ModeracioEstat:         "publicat",
			})
			if err != nil {
				t.Fatalf("SaveEntitatReligiosa parent: %v", err)
			}
			relID, err := env.DB.SaveEntitatReligiosaRelacio(&db.EntitatReligiosaRelacio{
				EntitatOrigenID: parentID,
				EntitatDestiID:  entitatID,
				TipusRelacio:    "conté",
				ModeracioEstat:  "publicat",
			})
			if err != nil {
				t.Fatalf("SaveEntitatReligiosaRelacio: %v", err)
			}
			if _, err := env.DB.SaveEntitatReligiosaRelacio(&db.EntitatReligiosaRelacio{
				EntitatOrigenID: entitatID,
				EntitatDestiID:  parentID,
				TipusRelacio:    "cicle",
				ModeracioEstat:  "publicat",
			}); !errors.Is(err, db.ErrInvalidReference) {
				t.Fatalf("relacio ciclica = %v, want ErrInvalidReference", err)
			}
			if err := env.DB.DeleteEntitatReligiosaRelacio(relID); err != nil {
				t.Fatalf("DeleteEntitatReligiosaRelacio: %v", err)
			}
		})
	}
}

func TestF353UConfessionalUIUsesPublishedCatalogsAndI18N(t *testing.T) {
	root := findProjectRoot(t)
	handlerBody := readProjectFileF353U(t, root, "core/admin_confessional.go")
	formBody := readProjectFileF353U(t, root, "templates/admin-confessional-form.html")
	listBody := readProjectFileF353U(t, root, "templates/admin-confessional-list.html")
	menuBody := readProjectFileF353U(t, root, "templates/layouts/menu-private.html")
	permsBody := readProjectFileF353U(t, root, "core/permissions_modular.go")
	catalogBody := readProjectFileF353U(t, root, "core/confessional_catalog.go")

	for _, token := range []string{
		"SelectableReligions",
		"SelectableNivells",
		"SelectableEntitats",
		"publishedEntitatsReligioses",
		"no esta publicat",
		"confessionalModerationStatusForSave(kind, id)",
		"suggestConfessionalRelationType",
		"RelEnt",
		"ListConfessionalReligionCatalog",
		"ListConfessionalLevelCatalog",
	} {
		if !strings.Contains(handlerBody, token) {
			t.Fatalf("falta control F35-3U al handler: %s", token)
		}
	}
	if strings.Contains(handlerBody, `r.FormValue("moderation_status")`) {
		t.Fatalf("el POST confessional no ha de llegir moderation_status manipulat")
	}
	for _, token := range []string{
		`/territori/confessional/models`,
		`name="model_confessional_id"`,
		`range .Data.Models`,
	} {
		if strings.Contains(menuBody+formBody, token) {
			t.Fatalf("el model_confessional no ha d'apareixer al flux normal: %s", token)
		}
	}
	for _, token := range []string{
		`name="religio_confessio_codi"`,
		`name="nivell_confessional_codi"`,
		`ListConfessionalReligionCatalog()`,
		`ListConfessionalLevelCatalog()`,
	} {
		if !strings.Contains(formBody+handlerBody, token) {
			t.Fatalf("el flux d'entitat ha d'usar codis/catalog de codi: %s", token)
		}
	}
	for _, token := range []string{
		"confessional.menu.catalog_religions",
		"confessional.menu.catalog_levels",
		"confessional.menu.entities",
		"confessional.menu.entity_relations",
		"confessional.menu.territorial_relations",
		"confessional.col.code",
		"confessional.col.category",
		"confessional.col.flags",
	} {
		if !strings.Contains(menuBody+listBody+formBody, token) {
			t.Fatalf("falta clau i18n confessional a templates: %s", token)
		}
	}
	for _, key := range []string{
		"territori.confessional.relacions_entitats.view",
		"territori.confessional.relacions_entitats.create",
		"territori.confessional.relacions_entitats.edit",
		"territori.confessional.relacions_entitats.delete",
	} {
		if !strings.Contains(permsBody, key) {
			t.Fatalf("falta permis granular relacions_entitats: %s", key)
		}
	}
	for _, token := range []string{
		`"cristianisme"`,
		`"catolicisme"`,
		`"catolicisme_ritu_llati"`,
		`"parroquia"`,
		`"arquebisbat_arxidiocesi"`,
	} {
		if !strings.Contains(catalogBody, token) {
			t.Fatalf("falta llavor canonica F35-3U: %s", token)
		}
	}

	for _, rel := range []string{"locales/cat.json", "locales/en.json", "locales/oc.json"} {
		body := readProjectFileF353U(t, root, rel)
		var values map[string]string
		if err := json.Unmarshal([]byte(body), &values); err != nil {
			t.Fatalf("%s no es JSON valid: %v", rel, err)
		}
		for _, key := range []string{
			"confessional.menu.section",
			"confessional.menu.catalog_religions",
			"confessional.menu.catalog_levels",
			"confessional.menu.entity_relations",
			"confessional.col.code",
			"confessional.religion.category.ritus",
			"confessional.level.category.territorial_local",
		} {
			if values[key] == "" {
				t.Fatalf("%s no defineix %s", rel, key)
			}
		}
	}
}

func TestF353VConfessionalCatalogIsStaticAndNotSeeded(t *testing.T) {
	root := findProjectRoot(t)
	mainBody := readProjectFileF353U(t, root, "main.go")
	catalogBody := readProjectFileF353U(t, root, "core/confessional_catalog.go")
	handlerBody := readProjectFileF353U(t, root, "core/admin_confessional.go")
	listBody := readProjectFileF353U(t, root, "templates/admin-confessional-list.html")

	for _, forbidden := range []string{
		"app.EnsureSystemConfessionalCatalogs()",
		"ensureSystemReligioConfessions",
		"ensureSystemNivellsConfessionals",
		"SaveReligioConfessio",
		"SaveNivellConfessional",
	} {
		if strings.Contains(mainBody+catalogBody, forbidden) {
			t.Fatalf("F35-3V no ha de sembrar cataleg base a DB: %s", forbidden)
		}
	}
	for _, token := range []string{
		"type ConfessionalReligionCatalogItem struct",
		"type ConfessionalLevelCatalogItem struct",
		"ListConfessionalReligionCatalog",
		"GetConfessionalReligionCatalogByCode",
		"ListConfessionalLevelCatalog",
		"ListConfessionalLevelsByReligionCode",
		"GetConfessionalLevelCatalogByCode",
		"ListConfessionalLevelCategories",
	} {
		if !strings.Contains(catalogBody, token) {
			t.Fatalf("falta API de cataleg estatic: %s", token)
		}
	}
	for _, token := range []string{
		"ReligionCatalog",
		"LevelCatalog",
		"ReligionCatalogLabels",
		"LevelCatalogLabels",
	} {
		if !strings.Contains(handlerBody+listBody, token) {
			t.Fatalf("la UI ha de llegir cataleg de codi: %s", token)
		}
	}
	if strings.Contains(listBody, `/territori/confessional/religions/{{ .ID }}/edit`) ||
		strings.Contains(listBody, `/territori/confessional/nivells/{{ .ID }}/edit`) ||
		strings.Contains(listBody, `name="kind" value="religio"`) ||
		strings.Contains(listBody, `name="kind" value="nivell"`) {
		t.Fatalf("les pantalles de cataleg base no han de mostrar accions CRUD destructives")
	}
}

func f353UFindReligion(t *testing.T, religions []db.ReligioConfessio, code string) db.ReligioConfessio {
	t.Helper()
	for _, item := range religions {
		if item.Codi == code {
			return item
		}
	}
	t.Fatalf("no s'ha trobat religio/confessio amb codi %s", code)
	return db.ReligioConfessio{}
}

func f353UFindLevel(t *testing.T, levels []db.NivellConfessional, code string) db.NivellConfessional {
	t.Helper()
	for _, item := range levels {
		if item.Codi == code {
			return item
		}
	}
	t.Fatalf("no s'ha trobat nivell confessional amb codi %s", code)
	return db.NivellConfessional{}
}

func newConfessionalAppsForAllDBs(t *testing.T) []appDB {
	t.Helper()

	configs := testcommon.LoadTestDBConfigs(t)
	projectRoot := findProjectRoot(t)
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatalf("no puc fer chdir a l'arrel del projecte (%s): %v", projectRoot, err)
	}
	loadTemplatesForTests(t, projectRoot)

	var result []appDB
	for _, c := range configs {
		cfg := map[string]string{}
		for k, v := range c.Config {
			cfg[k] = v
		}
		cfg["RECREADB"] = "true"
		cfg["RECREADB_RESET"] = "true"
		if _, ok := cfg["LOG_LEVEL"]; !ok {
			cfg["LOG_LEVEL"] = "silent"
		}

		dbInstance, err := db.NewDB(cfg)
		if err != nil {
			t.Fatalf("no s'ha pogut inicialitzar DB %s per tests confessionals: %v", c.Label, err)
		}
		app := core.NewApp(cfg, dbInstance)
		t.Cleanup(func() {
			app.Close()
		})

		result = append(result, appDB{Label: c.Label, App: app, DB: dbInstance})
	}
	return result
}

func readProjectFileF353U(t *testing.T, root, rel string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("no s'ha pogut llegir %s: %v", rel, err)
	}
	return string(body)
}
