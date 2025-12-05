package unit

import (
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

// Pending marca un test com pendent d'implementar.
func Pending(t *testing.T, msg string) {
	t.Helper()
	t.Skip("PENDENT: " + msg)
}

// newTestConfig retorna una configuració mínima per a tests amb SQLite en memòria.
func newTestConfig() map[string]string {
	return map[string]string{
		"DB_ENGINE": "sqlite",
		"DB_PATH":   ":memory:",
		"ENV":       "test",
		"LOG_LEVEL": "silent",
		"RECREADB":  "true", // perquè es creï l’esquema de la BD als tests
	}
}

// newTestApp crea una *core.App per a tests amb una BD SQLite in-memory.
func newTestApp(t *testing.T) *core.App {
	t.Helper()

	cfg := newTestConfig()

	database, err := db.NewDB(cfg)
	if err != nil {
		t.Fatalf("db.NewDB: %v", err)
	}

	app := core.NewApp(cfg, database)
	return app
}

// closeTestApp tanca l'App de proves.
func closeTestApp(t *testing.T, app *core.App) {
	t.Helper()
	if app != nil {
		app.Close()
	}
}
