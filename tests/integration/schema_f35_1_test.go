package integration

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestF351SchemaReligiosConfessionalSQLite(t *testing.T) {
	_, database := newTestAppForLogin(t, "test_f35_1_schema_religios_confessional.sqlite3")

	for _, table := range []string{
		"religio_confessio",
		"model_confessional",
		"nivell_confessional",
		"entitat_religiosa",
		"entitat_religiosa_relacio",
		"arquebisbats",
		"arquebisbats_municipi",
		"llibres",
	} {
		if !f351SQLiteTableExists(t, database, table) {
			t.Fatalf("taula esperada no creada: %s", table)
		}
	}

	expectedColumns := map[string][]string{
		"religio_confessio": {
			"id", "nom", "pare_id", "descripcio", "estat", "observacions", "moderation_status", "created_at", "updated_at",
		},
		"model_confessional": {
			"id", "nom", "religio_confessio_id", "pais_id", "descripcio", "any_inici", "any_fi", "estat", "observacions", "moderation_status", "created_at", "updated_at",
		},
		"nivell_confessional": {
			"id", "model_confessional_id", "ordre", "nom_nivell", "nom_plural", "tipus_nivell", "codi_oficial", "parent_id", "any_inici", "any_fi", "estat", "observacions", "moderation_status", "created_at", "updated_at",
		},
		"entitat_religiosa": {
			"id", "nom", "religio_confessio_id", "model_confessional_id", "nivell_confessional_id", "pais_id", "parent_id", "tipus_entitat", "tipus_especific", "any_inici", "any_fi", "estat", "web", "web_wikipedia", "territori", "observacions", "moderation_status", "created_at", "updated_at",
		},
		"entitat_religiosa_relacio": {
			"id", "entitat_origen_id", "entitat_desti_id", "tipus_relacio", "any_inici", "any_fi", "font_id", "observacions", "moderation_status", "created_at", "updated_at",
		},
	}
	for table, columns := range expectedColumns {
		got := f351SQLiteColumns(t, database, table)
		for _, column := range columns {
			if !got[column] {
				t.Fatalf("%s no te columna esperada %s; columns=%v", table, column, got)
			}
		}
	}

	for _, idx := range []string{
		"idx_religio_confessio_pare",
		"idx_model_confessional_religio",
		"idx_model_confessional_pais",
		"idx_nivell_confessional_model",
		"idx_nivell_confessional_parent",
		"idx_entitat_religiosa_religio",
		"idx_entitat_religiosa_model",
		"idx_entitat_religiosa_nivell",
		"idx_entitat_religiosa_parent",
		"idx_entitat_religiosa_pais",
		"idx_entitat_religiosa_relacio_origen",
		"idx_entitat_religiosa_relacio_desti",
	} {
		if !f351SQLiteIndexExists(t, database, idx) {
			t.Fatalf("index esperat no creat: %s", idx)
		}
	}

	expectedFKs := map[string][]string{
		"religio_confessio":         {"pare_id->religio_confessio"},
		"model_confessional":        {"religio_confessio_id->religio_confessio", "pais_id->paisos"},
		"nivell_confessional":       {"model_confessional_id->model_confessional", "parent_id->nivell_confessional"},
		"entitat_religiosa":         {"religio_confessio_id->religio_confessio", "model_confessional_id->model_confessional", "nivell_confessional_id->nivell_confessional", "pais_id->paisos", "parent_id->entitat_religiosa"},
		"entitat_religiosa_relacio": {"entitat_origen_id->entitat_religiosa", "entitat_desti_id->entitat_religiosa"},
	}
	for table, fks := range expectedFKs {
		got := f351SQLiteFKs(t, database, table)
		for _, fk := range fks {
			if !got[fk] {
				t.Fatalf("%s no te FK esperada %s; fks=%v", table, fk, got)
			}
		}
	}

	for table, columns := range map[string][]string{
		"arquebisbats": {"id", "nom", "tipus_entitat", "pais_id", "parent_id"},
		"llibres":      {"id", "arquevisbat_id", "municipi_id", "nom_esglesia", "tipus_" + "llibre"},
	} {
		got := f351SQLiteColumns(t, database, table)
		for _, column := range columns {
			if !got[column] {
				t.Fatalf("%s ha perdut columna fora d'abast %s", table, column)
			}
		}
	}
}

func TestF351SchemaReligiosConfessionalSQLFilesAligned(t *testing.T) {
	root := findProjectRoot(t)
	files := []string{"db/SQLite.sql", "db/PostgreSQL.sql", "db/MySQL.sql"}
	tables := []string{
		"religio_confessio",
		"model_confessional",
		"nivell_confessional",
		"entitat_religiosa",
		"entitat_religiosa_relacio",
	}
	indexes := []string{
		"idx_religio_confessio_pare",
		"idx_model_confessional_religio",
		"idx_model_confessional_pais",
		"idx_nivell_confessional_model",
		"idx_nivell_confessional_parent",
		"idx_entitat_religiosa_religio",
		"idx_entitat_religiosa_model",
		"idx_entitat_religiosa_nivell",
		"idx_entitat_religiosa_parent",
		"idx_entitat_religiosa_pais",
		"idx_entitat_religiosa_relacio_origen",
		"idx_entitat_religiosa_relacio_desti",
	}

	for _, rel := range files {
		body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("no s'ha pogut llegir %s: %v", rel, err)
		}
		src := string(body)
		for _, table := range tables {
			if !strings.Contains(src, "CREATE TABLE IF NOT EXISTS "+table) {
				t.Fatalf("%s no crea %s", rel, table)
			}
		}
		for _, idx := range indexes {
			if !strings.Contains(src, "CREATE INDEX") || !strings.Contains(src, idx) {
				t.Fatalf("%s no crea index %s", rel, idx)
			}
		}
		for _, legacy := range []string{
			"CREATE TABLE IF NOT EXISTS arquebisbats",
			"CREATE TABLE IF NOT EXISTS arquebisbats_municipi",
			"CREATE TABLE IF NOT EXISTS llibres",
			"tipus_" + "llibre",
			"nom_esglesia",
			"arquevisbat_id",
		} {
			if !strings.Contains(src, legacy) {
				t.Fatalf("%s ha perdut element fora d'abast %q", rel, legacy)
			}
		}
		if strings.Contains(src, "llibre_entitat_"+"religiosa") ||
			strings.Contains(src, "arxiu_"+"entitat") {
			t.Fatalf("%s introdueix relacio fora d'abast", rel)
		}
	}
}

func f351SQLiteTableExists(t *testing.T, database interface {
	Query(string, ...interface{}) ([]map[string]interface{}, error)
}, table string) bool {
	t.Helper()
	rows, err := database.Query("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", table)
	if err != nil {
		t.Fatalf("sqlite_master table %s: %v", table, err)
	}
	return len(rows) == 1 && f351String(rows[0]["name"]) == table
}

func f351SQLiteIndexExists(t *testing.T, database interface {
	Query(string, ...interface{}) ([]map[string]interface{}, error)
}, index string) bool {
	t.Helper()
	rows, err := database.Query("SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?", index)
	if err != nil {
		t.Fatalf("sqlite_master index %s: %v", index, err)
	}
	return len(rows) == 1 && f351String(rows[0]["name"]) == index
}

func f351SQLiteColumns(t *testing.T, database interface {
	Query(string, ...interface{}) ([]map[string]interface{}, error)
}, table string) map[string]bool {
	t.Helper()
	rows, err := database.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		t.Fatalf("PRAGMA table_info(%s): %v", table, err)
	}

	columns := map[string]bool{}
	for _, row := range rows {
		name := f351String(row["name"])
		columns[name] = true
	}
	return columns
}

func f351SQLiteFKs(t *testing.T, database interface {
	Query(string, ...interface{}) ([]map[string]interface{}, error)
}, table string) map[string]bool {
	t.Helper()
	rows, err := database.Query("PRAGMA foreign_key_list(" + table + ")")
	if err != nil {
		t.Fatalf("PRAGMA foreign_key_list(%s): %v", table, err)
	}

	fks := map[string]bool{}
	for _, row := range rows {
		refTable := f351String(row["table"])
		from := f351String(row["from"])
		fks[from+"->"+refTable] = true
	}
	return fks
}

func f351String(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return ""
	}
}
