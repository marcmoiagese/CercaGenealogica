package common

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type Engine string

const (
	EngineSQLite   Engine = "sqlite"
	EnginePostgres Engine = "postgres"
	EngineMySQL    Engine = "mysql"
)

type DBTestConfig struct {
	Engine Engine

	// Per SQLite
	Path string

	// Per Postgres/MySQL
	Host string
	Port string
	User string
	Pass string
	Name string
}

// retorna tots els motors que cal provar (sempre sqlite + els definits a cnf.cfg)
func LoadDBTestConfigs(t *testing.T) []DBTestConfig {
	t.Helper()

	// 1) SQLite sempre
	cfgs := []DBTestConfig{
		{
			Engine: EngineSQLite,
			Path:   "./database.test.db", // o el que vulguis; la BD es recrea igualment
		},
	}

	// 2) Intentem llegir tests/cnf/cnf.cfg
	cfgPath := filepath.Join("tests", "cnf", "cnf.cfg")
	f, err := os.Open(cfgPath)
	if err != nil {
		// Si no existeix, només fem SQLite
		return cfgs
	}
	defer f.Close()

	// Parser INI molt simple
	type kv = map[string]string
	sections := map[string]kv{}
	var current string

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			current = strings.ToLower(strings.TrimSpace(line[1 : len(line)-1]))
			if _, ok := sections[current]; !ok {
				sections[current] = kv{}
			}
			continue
		}
		if current == "" {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		k := strings.TrimSpace(parts[0])
		v := strings.TrimSpace(parts[1])
		sections[current][k] = v
	}

	// 3) Afegim Postgres si hi és
	if sec, ok := sections["postgres"]; ok {
		cfgs = append(cfgs, DBTestConfig{
			Engine: EnginePostgres,
			Host:   sec["DB_HOST"],
			Port:   sec["DB_PORT"],
			User:   sec["DB_USR"],
			Pass:   sec["DB_PASS"],
			Name:   sec["DB_NAME"],
		})
	}

	// 4) Afegim MySQL si hi és
	if sec, ok := sections["mysql"]; ok {
		cfgs = append(cfgs, DBTestConfig{
			Engine: EngineMySQL,
			Host:   sec["DB_HOST"],
			Port:   sec["DB_PORT"],
			User:   sec["DB_USR"],
			Pass:   sec["DB_PASS"],
			Name:   sec["DB_NAME"],
		})
	}

	return cfgs
}

// Crea un db.DB real per a un motor concret, amb RECREADB=true
func NewDBForTest(t *testing.T, c DBTestConfig) (db.DB, func()) {
	t.Helper()

	config := map[string]string{
		"DB_ENGINE": string(c.Engine),
		"RECREADB":  "true",
	}

	if c.Engine == EngineSQLite {
		config["DB_PATH"] = c.Path
	} else {
		config["DB_HOST"] = c.Host
		config["DB_PORT"] = c.Port
		config["DB_USR"] = c.User
		config["DB_PASS"] = c.Pass
		config["DB_NAME"] = c.Name
	}

	dbInstance, err := db.NewDB(config)
	if err != nil {
		t.Fatalf("no s'ha pogut inicialitzar DB %s de prova: %v", c.Engine, err)
	}

	cleanup := func() {
		dbInstance.Close()
	}

	return dbInstance, cleanup
}

// Helper per iterar sobre tots els motors
func ForEachTestDB(t *testing.T, fn func(t *testing.T, cfg DBTestConfig, d db.DB)) {
	t.Helper()

	cfgs := LoadDBTestConfigs(t)
	for _, c := range cfgs {
		c := c
		t.Run(string(c.Engine), func(t *testing.T) {
			t.Parallel()

			d, cleanup := NewDBForTest(t, c)
			defer cleanup()

			fn(t, c, d)
		})
	}
}
