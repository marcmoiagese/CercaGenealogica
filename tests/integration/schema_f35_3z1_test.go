package integration

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
	testcommon "github.com/marcmoiagese/CercaGenealogica/tests/common"
)

var f353Z1ConfessionalTables = []string{
	"entitat_religiosa",
	"entitat_religiosa_relacio",
	"municipi_entitat_religiosa",
}

var f353Z1AuthorshipColumns = []string{
	"created_by",
	"updated_by",
	"moderation_notes",
	"moderated_by",
	"moderated_at",
}

func TestF353Z1SQLiteFreshSchemaAndIdempotentApply(t *testing.T) {
	_, database := newTestAppForLogin(t, "test_f35_3z1_sqlite_fresh.sqlite3")

	f353Z1AssertAuthorshipColumns(t, "sqlite", database)

	if err := db.ApplyDatabaseFromSQL("db/SQLite.sql", "sqlite", database); err != nil {
		t.Fatalf("reaplicar db/SQLite.sql no hauria de fallar: %v", err)
	}

	f353Z1AssertAuthorshipColumns(t, "sqlite", database)
}

func TestF353Z1ConfessionalAuthorshipFreshSchemaMultiDB(t *testing.T) {
	projectRoot := findProjectRoot(t)
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatalf("no puc fer chdir a l'arrel del projecte (%s): %v", projectRoot, err)
	}

	for _, cfg := range testcommon.LoadTestDBConfigs(t) {
		cfg := cfg
		t.Run(cfg.Label, func(t *testing.T) {
			config := map[string]string{}
			for k, v := range cfg.Config {
				config[k] = v
			}
			config["RECREADB"] = "true"
			config["LOG_LEVEL"] = "silent"
			if cfg.Engine == "sqlite" {
				config["DB_PATH"] = filepath.Join(t.TempDir(), "test_f35_3z1_multidb.sqlite3")
			}

			database, err := db.NewDB(config)
			if err != nil {
				t.Fatalf("NewDB(%s): %v", cfg.Label, err)
			}
			defer database.Close()

			f353Z1AssertAuthorshipColumns(t, cfg.Engine, database)
		})
	}
}

func TestF353Z1AuthorshipColumnsAreNotDuplicatedAsSchemaAlters(t *testing.T) {
	root := findProjectRoot(t)
	files := []string{"db/SQLite.sql", "db/PostgreSQL.sql", "db/MySQL.sql"}

	for _, rel := range files {
		body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("no s'ha pogut llegir %s: %v", rel, err)
		}
		src := strings.ToLower(string(body))

		for _, table := range f353Z1ConfessionalTables {
			for _, column := range f353Z1AuthorshipColumns {
				createIdx := strings.Index(src, "create table if not exists "+table)
				if createIdx < 0 {
					t.Fatalf("%s no crea %s", rel, table)
				}
				if !strings.Contains(src[createIdx:], column) {
					t.Fatalf("%s no declara %s.%s al CREATE TABLE", rel, table, column)
				}
				alterNeedle := "alter table " + table + " add column"
				if strings.Contains(src, alterNeedle+" "+column) || strings.Contains(src, alterNeedle+" if not exists "+column) {
					t.Fatalf("%s torna a afegir %s.%s amb ALTER duplicat", rel, table, column)
				}
			}
		}
	}
}

func f353Z1AssertAuthorshipColumns(t *testing.T, engine string, database db.DB) {
	t.Helper()
	for _, table := range f353Z1ConfessionalTables {
		columns := f353Z1Columns(t, engine, database, table)
		for _, column := range f353Z1AuthorshipColumns {
			if !columns[column] {
				t.Fatalf("%s.%s no te columna %s; columns=%v", engine, table, column, columns)
			}
		}
	}
}

func f353Z1Columns(t *testing.T, engine string, database db.DB, table string) map[string]bool {
	t.Helper()
	var query string
	switch engine {
	case "sqlite":
		query = "PRAGMA table_info(" + table + ")"
	case "postgres":
		query = "SELECT column_name AS name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '" + table + "'"
	case "mysql":
		query = "SELECT column_name AS name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = '" + table + "'"
	default:
		t.Fatalf("motor no suportat: %s", engine)
	}

	rows, err := database.Query(query)
	if err != nil {
		t.Fatalf("columns(%s.%s): %v", engine, table, err)
	}
	columns := map[string]bool{}
	for _, row := range rows {
		name := strings.ToLower(f351String(row["name"]))
		if name != "" {
			columns[name] = true
		}
	}
	return columns
}
