package integration

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
	testcommon "github.com/marcmoiagese/CercaGenealogica/tests/common"
)

func loadSQLitePostgresAndMySQLConfigsForImportHistory(t *testing.T) []testcommon.TestDBConfig {
	t.Helper()

	raw := testcommon.LoadTestDBConfigs(t)
	var out []testcommon.TestDBConfig
	for _, cfg := range raw {
		cfgCopy := map[string]string{}
		for k, v := range cfg.Config {
			cfgCopy[k] = v
		}
		cfgCopy["RECREADB"] = "true"
		if _, ok := cfgCopy["LOG_LEVEL"]; !ok {
			cfgCopy["LOG_LEVEL"] = "silent"
		}
		if cfg.Engine == "sqlite" {
			cfgCopy["DB_PATH"] = filepath.Join(t.TempDir(), "f32_13_3_"+cfg.Label+".sqlite3")
		}
		out = append(out, testcommon.TestDBConfig{
			Engine: cfg.Engine,
			Label:  cfg.Label,
			Config: cfgCopy,
		})
	}
	if len(out) == 0 {
		t.Fatalf("no hi ha configuracions disponibles per F32-13-3")
	}
	return out
}

func newTestAppForConfigOrSkipMySQL(t *testing.T, cfg map[string]string) (*core.App, db.DB) {
	t.Helper()

	projectRoot := findProjectRoot(t)
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatalf("no puc fer chdir a l'arrel del projecte (%s): %v", projectRoot, err)
	}
	loadTemplatesForTests(t, projectRoot)

	dbInstance, err := db.NewDB(cfg)
	if err != nil {
		if strings.EqualFold(strings.TrimSpace(cfg["DB_ENGINE"]), "mysql") {
			t.Skipf("MySQL no s'ha pogut inicialitzar en aquest entorn: %v", err)
		}
		t.Fatalf("NewDB ha fallat: %v", err)
	}
	app := core.NewApp(cfg, dbInstance)
	t.Cleanup(func() {
		app.Close()
	})
	return app, dbInstance
}
