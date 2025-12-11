package common

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDBConfig descriu una configuració de base de dades per a tests.
// Engine: "sqlite", "postgres" o "mysql".
// Config: mapa de claus/valors que passen directament a db.NewDB(...).
// Label: nom curt per fer servir a t.Run (normalment igual que Engine).
type TestDBConfig struct {
	Engine string
	Config map[string]string
	Label  string
}

// LoadTestDBConfigs llegeix tests/cnf/cnf.cfg des de l'arrel del projecte
// i construeix una llista de TestDBConfig. Sempre retorna com a mínim
// una entrada per SQLite. PostgreSQL i MySQL només s'afegeixen si al fitxer
// hi ha definides POSTGRES_DB_HOST / MYSQL_DB_HOST respectivament.
func LoadTestDBConfigs(t *testing.T) []TestDBConfig {
	t.Helper()

	projectRoot := findProjectRoot(t)
	cfgPath := filepath.Join(projectRoot, "tests", "cnf", "cnf.cfg")

	raw := readKeyValueFile(t, cfgPath)

	var result []TestDBConfig

	// --- SQLite (sempre present) ---
	sqlitePath := raw["SQLITE_DB_PATH"]
	if sqlitePath == "" {
		// Per defecte, fem servir un fitxer a tests/tmp/test.db
		sqlitePath = filepath.Join(projectRoot, "tests", "tmp", "test.db")
	} else if !filepath.IsAbs(sqlitePath) {
		// Convertim a ruta absoluta relativa a l'arrel del projecte
		sqlitePath = filepath.Join(projectRoot, sqlitePath)
	}

	sqliteCfg := map[string]string{
		"DB_ENGINE": "sqlite",
		"DB_PATH":   sqlitePath,
		"RECREADB":  firstNonEmpty(raw["SQLITE_RECREADB"], "true"),
	}
	result = append(result, TestDBConfig{
		Engine: "sqlite",
		Label:  "sqlite",
		Config: sqliteCfg,
	})

	// --- PostgreSQL (opcional) ---
	if host := strings.TrimSpace(raw["POSTGRES_DB_HOST"]); host != "" {
		pgCfg := map[string]string{
			"DB_ENGINE": "postgres",
			"DB_HOST":   host,
			"DB_PORT":   firstNonEmpty(raw["POSTGRES_DB_PORT"], "5432"),
			"DB_USR":    firstNonEmpty(raw["POSTGRES_DB_USER"], "postgres"),
			"DB_PASS":   raw["POSTGRES_DB_PASS"],
			"DB_NAME":   firstNonEmpty(raw["POSTGRES_DB_NAME"], "postgres"),
			"RECREADB":  firstNonEmpty(raw["POSTGRES_RECREADB"], "true"),
		}
		result = append(result, TestDBConfig{
			Engine: "postgres",
			Label:  "postgres",
			Config: pgCfg, // <- aquí estava el typo (_Config)
		})
	}

	// --- MySQL (opcional) ---
	if host := strings.TrimSpace(raw["MYSQL_DB_HOST"]); host != "" {
		myCfg := map[string]string{
			"DB_ENGINE": "mysql",
			"DB_HOST":   host,
			"DB_PORT":   firstNonEmpty(raw["MYSQL_DB_PORT"], "3306"),
			"DB_USR":    firstNonEmpty(raw["MYSQL_DB_USER"], "root"),
			"DB_PASS":   raw["MYSQL_DB_PASS"],
			"DB_NAME":   firstNonEmpty(raw["MYSQL_DB_NAME"], "mysql"),
			"RECREADB":  firstNonEmpty(raw["MYSQL_RECREADB"], "true"),
		}
		result = append(result, TestDBConfig{
			Engine: "mysql",
			Label:  "mysql",
			Config: myCfg,
		})
	}

	return result
}

// firstNonEmpty retorna el primer valor no buit d'una llista de cadenes.
// Si tots són buits, retorna "".
func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// findProjectRoot cerca l'arrel del projecte (directori que conté go.mod)
// pujant des del directori actual. Si no la troba, el test falla.
func findProjectRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("no puc obtenir directori actual: %v", err)
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("no s'ha trobat go.mod a cap directori pare de %s", dir)
		}
		dir = parent
	}
}

// readKeyValueFile llegeix un fitxer simple de K=V, però també
// entén seccions [sqlite], [postgres], [mysql] i prefixa les claus
// amb el nom de la secció en majúscules.
//
// Exemple:
//
//	[postgres]
//	DB_HOST=devstack.marc.cat
//	DB_PORT=5432
//
// es converteix en:
//
//	POSTGRES_DB_HOST -> "devstack.marc.cat"
//	POSTGRES_DB_PORT -> "5432"
func readKeyValueFile(t *testing.T, path string) map[string]string {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("no puc obrir fitxer de config de tests %q: %v", path, err)
	}
	defer f.Close()

	out := make(map[string]string)
	scanner := bufio.NewScanner(f)

	currentSection := "" // ex: "SQLITE", "POSTGRES", "MYSQL"

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}

		// Detecta seccions tipus [sqlite], [postgres], [mysql]
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			sec := strings.TrimSpace(line[1 : len(line)-1])
			currentSection = strings.ToUpper(sec)
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			// línia escombraria o format inesperat
			continue
		}

		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])

		// Normalitzem la clau a MAJÚSCULES
		keyUpper := strings.ToUpper(key)

		// Si estem dins d'una secció, prefixem (si cal)
		//
		// Ex:
		//   [postgres] + DB_HOST -> POSTGRES_DB_HOST
		//   [sqlite]   + DB_PATH -> SQLITE_DB_PATH
		//
		// Però si algú ja posa POSTGRES_DB_HOST dins la secció,
		// no volem POSTGRES_POSTGRES_DB_HOST, així que controlem
		// el prefix.
		if currentSection != "" {
			prefix := currentSection + "_"
			if !strings.HasPrefix(keyUpper, prefix) {
				keyUpper = prefix + keyUpper
			}
		}

		out[keyUpper] = val
	}

	if err := scanner.Err(); err != nil {
		t.Fatalf("error llegint fitxer de config de tests %q: %v", path, err)
	}

	return out
}
