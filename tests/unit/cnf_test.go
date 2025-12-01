package unit

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/cnf"
)

// TestLoadConfigBasic comprova que:
//   - s'ignoren línies buides i comentaris (# i ;)
//   - es llegeixen clau=valor
//   - l'última definició d'una clau guanya
func TestLoadConfigBasic(t *testing.T) {
	content := `
# Comentari
; Comentari estil INI

DB_ENGINE = sqlite
LOG_LEVEL = debug

# Aquesta línia queda sobreescrita:
LOG_LEVEL = info

SENSEVALOR=

`

	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.cfg")

	if err := os.WriteFile(cfgPath, []byte(content), 0o600); err != nil {
		t.Fatalf("no puc escriure config temporal: %v", err)
	}

	cfg, err := cnf.LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("LoadConfig ha fallat: %v", err)
	}

	if got := cfg["DB_ENGINE"]; got != "sqlite" {
		t.Errorf("DB_ENGINE = %q, vull sqlite", got)
	}

	if got := cfg["LOG_LEVEL"]; got != "info" {
		t.Errorf("LOG_LEVEL = %q, vull info (última definició guanya)", got)
	}

	// La clau sense valor hauria d'existir amb valor buit o no existir.
	// En cap cas hauria de provocar panics.
	_ = cfg["SENSEVALOR"]
}

// TestParseConfigDefaults comprova els valors per defecte quan el mapa
// de config està pràcticament buit.
func TestParseConfigDefaults(t *testing.T) {
	// Ens assegurem que ENVIRONMENT estigui buit per provar el fallback.
	oldEnv := os.Getenv("ENVIRONMENT")
	t.Cleanup(func() {
		_ = os.Setenv("ENVIRONMENT", oldEnv)
	})
	_ = os.Unsetenv("ENVIRONMENT")

	cfg := map[string]string{} // buit

	appCfg, err := cnf.ParseConfig(cfg)
	if err != nil {
		t.Fatalf("ParseConfig ha retornat error amb config buida: %v", err)
	}

	if appCfg.DBEngine != "sqlite" {
		t.Errorf("DBEngine = %q, vull 'sqlite' per defecte", appCfg.DBEngine)
	}
	if appCfg.DBPath != "./database.db" {
		t.Errorf("DBPath = %q, vull './database.db' per defecte", appCfg.DBPath)
	}
	if appCfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, vull 'info' per defecte", appCfg.LogLevel)
	}
	if appCfg.Env != "development" {
		t.Errorf("Env = %q, vull 'development' quan ENVIRONMENT no està definit", appCfg.Env)
	}
	if appCfg.RecreaDB {
		t.Errorf("RecreaDB hauria de ser false per defecte")
	}
	if appCfg.RegisterD {
		t.Errorf("RegisterD hauria de ser false per defecte")
	}
}

// TestParseConfigEnvFromEnvVar comprova que si ENVIRONMENT està definit
// i el mapa de config no el sobreescriu, s'agafa el valor de l'entorn.
func TestParseConfigEnvFromEnvVar(t *testing.T) {
	oldEnv := os.Getenv("ENVIRONMENT")
	t.Cleanup(func() {
		_ = os.Setenv("ENVIRONMENT", oldEnv)
	})

	if err := os.Setenv("ENVIRONMENT", "production"); err != nil {
		t.Fatalf("no puc establir ENVIRONMENT: %v", err)
	}

	cfg := map[string]string{
		"DB_ENGINE": "sqlite",
		"DB_PATH":   "/tmp/test.db",
		// No posem ENVIRONMENT aquí per veure el fallback a la variable d'entorn
	}

	appCfg, err := cnf.ParseConfig(cfg)
	if err != nil {
		t.Fatalf("ParseConfig ha retornat error: %v", err)
	}

	if appCfg.Env != "production" {
		t.Errorf("Env = %q, vull 'production' agafat de ENVIRONMENT", appCfg.Env)
	}
	if appCfg.DBPath != "/tmp/test.db" {
		t.Errorf("DBPath = %q, vull '/tmp/test.db'", appCfg.DBPath)
	}
}
