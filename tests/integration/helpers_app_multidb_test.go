package integration

import (
	"os"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
	testcommon "github.com/marcmoiagese/CercaGenealogica/tests/common"
)

type appDB struct {
	Label string
	App   *core.App
	DB    db.DB
}

// newAppsForAllDBs construeix una App + DB per a cada motor definit a tests/cnf/cnf.cfg.
// Sempre inclourà sqlite, i afegirà postgres/mysql si hi ha config.
func newAppsForAllDBs(t *testing.T) []appDB {
	t.Helper()

	configs := testcommon.LoadTestDBConfigs(t)

	projectRoot := findProjectRoot(t)
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatalf("no puc fer chdir a l'arrel del projecte (%s): %v", projectRoot, err)
	}

	// Carreguem plantilles una vegada per test
	loadTemplatesForTests(t, projectRoot)

	var result []appDB

	for _, c := range configs {
		// Fem una còpia del map perquè cada App tingui el seu
		cfg := map[string]string{}
		for k, v := range c.Config {
			cfg[k] = v
		}

		// Assegurem LOG_LEVEL en silent si no ve definit
		if _, ok := cfg["LOG_LEVEL"]; !ok {
			cfg["LOG_LEVEL"] = "silent"
		}

		dbInstance, err := db.NewDB(cfg)
		if err != nil {
			t.Fatalf("no s'ha pogut inicialitzar DB %s per tests: %v", c.Label, err)
		}

		app := core.NewApp(cfg, dbInstance)

		// Ens assegurem de tancar al final
		t.Cleanup(func() {
			app.Close()
		})

		result = append(result, appDB{
			Label: c.Label,
			App:   app,
			DB:    dbInstance,
		})
	}

	return result
}

func newTestAppForConfig(t *testing.T, cfg map[string]string) (*core.App, db.DB) {
	t.Helper()

	// Ens assegurem que estem a l'arrel del projecte
	projectRoot := findProjectRoot(t)
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatalf("no puc fer chdir a l'arrel del projecte (%s): %v", projectRoot, err)
	}

	// Carreguem plantilles perquè el handler pugui renderitzar pàgines
	loadTemplatesForTests(t, projectRoot)

	dbInstance, err := db.NewDB(cfg)
	if err != nil {
		t.Fatalf("NewDB ha fallat: %v", err)
	}

	app := core.NewApp(cfg, dbInstance)

	t.Cleanup(func() {
		app.Close()
	})

	return app, dbInstance
}
