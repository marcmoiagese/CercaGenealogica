package integration

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

// newTestAppForActivation crea una BD SQLite temporal, aplica l'esquema
// (RECREADB=true) i construeix una *core.App perquè els handlers la facin servir.
func newTestAppForActivation(t *testing.T, dbFileName string) (*core.App, db.DB) {
	t.Helper()

	projectRoot := findProjectRoot(t)
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatalf("no puc fer chdir a l'arrel del projecte (%s): %v", projectRoot, err)
	}

	// Ens assegurem que les plantilles estan carregades
	loadTemplatesForTests(t, projectRoot)

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, dbFileName)

	cfg := map[string]string{
		"DB_ENGINE": "sqlite",
		"DB_PATH":   dbPath,
		"RECREADB":  "true",
		"LOG_LEVEL": "silent",
	}

	dbInstance, err := db.NewDB(cfg)
	if err != nil {
		t.Fatalf("NewDB ha fallat: %v", err)
	}

	app := core.NewApp(cfg, dbInstance)

	// Tancarem la BD en acabar el test
	t.Cleanup(func() {
		app.Close()
	})

	return app, dbInstance
}

// TestActivacioCorrecta comprova que un usuari amb token vàlid
// passa de Active=false a Active=true després de cridar /activar.
func TestActivacioCorrecta(t *testing.T) {
	app, dbInstance := newTestAppForActivation(t, "test_activacio_ok.sqlite3")

	email := "activacio.correcte@example.com"
	user := &db.User{
		Usuari:        "activacio_ok",
		Name:          "Test",
		Surname:       "Activacio",
		Email:         email,
		Password:      []byte("dummy"),
		DataNaixament: "1990-01-01",
		Active:        false,
	}

	if err := dbInstance.InsertUser(user); err != nil {
		t.Fatalf("InsertUser ha fallat: %v", err)
	}

	token := "TOKEN_ACTIVACIO_OK"
	if err := dbInstance.SaveActivationToken(email, token); err != nil {
		t.Fatalf("SaveActivationToken ha fallat: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/activar?token="+token, nil)
	req.RemoteAddr = "127.0.0.1:50001"

	rr := httptest.NewRecorder()
	app.ActivarUsuariHTTP(rr, req)

	res := rr.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("s'esperava status 200 en activació correcta, però tinc %d. Cos:\n%s",
			res.StatusCode, rr.Body.String())
	}

	updated, err := dbInstance.GetUserByEmail(email)
	if err != nil {
		t.Fatalf("GetUserByEmail ha fallat després d'activació: %v", err)
	}
	if updated == nil {
		t.Fatalf("no s'ha trobat l'usuari %s després d'activació", email)
	}
	if !updated.Active {
		t.Fatalf("s'esperava Active=true després d'activació, però és false")
	}
}

// TestActivacioTokenInvalid comprova que un token que no existeix
// NO activa l’usuari.
func TestActivacioTokenInvalid(t *testing.T) {
	app, dbInstance := newTestAppForActivation(t, "test_activacio_invalid.sqlite3")

	email := "activacio.invalid@example.com"
	user := &db.User{
		Usuari:        "activacio_invalid",
		Name:          "Test",
		Surname:       "Invalid",
		Email:         email,
		Password:      []byte("dummy"),
		DataNaixament: "1990-01-01",
		Active:        false,
	}

	if err := dbInstance.InsertUser(user); err != nil {
		t.Fatalf("InsertUser ha fallat: %v", err)
	}

	// Guardem un token "real" que NO farem servir al request
	if err := dbInstance.SaveActivationToken(email, "TOKEN_REAL_PERO_NO_L_FAREM_SERVIR"); err != nil {
		t.Fatalf("SaveActivationToken ha fallat: %v", err)
	}

	invalidToken := "TOKEN_QUE_NO_EXISTEIX"

	req := httptest.NewRequest(http.MethodGet, "/activar?token="+invalidToken, nil)
	req.RemoteAddr = "127.0.0.1:50002"

	rr := httptest.NewRecorder()
	app.ActivarUsuariHTTP(rr, req)

	res := rr.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("s'esperava status 200 fins i tot amb token invàlid, però tinc %d. Cos:\n%s",
			res.StatusCode, rr.Body.String())
	}

	updated, err := dbInstance.GetUserByEmail(email)
	if err != nil {
		t.Fatalf("GetUserByEmail ha fallat després d'intent amb token invàlid: %v", err)
	}
	if updated == nil {
		t.Fatalf("no s'ha trobat l'usuari %s després de cridar /activar amb token invàlid", email)
	}
	if updated.Active {
		t.Fatalf("Active hauria de seguir sent false per token invàlid, però és true")
	}
}

// TestActivacioTokenExpirat comprova el cas en què el token existeix
// però expira_token és anterior a datetime('now').
func TestActivacioTokenExpirat(t *testing.T) {
	app, dbInstance := newTestAppForActivation(t, "test_activacio_expirat.sqlite3")

	email := "activacio.expirat@example.com"
	user := &db.User{
		Usuari:        "activacio_expirat",
		Name:          "Test",
		Surname:       "Expirat",
		Email:         email,
		Password:      []byte("dummy"),
		DataNaixament: "1990-01-01",
		Active:        false,
	}

	if err := dbInstance.InsertUser(user); err != nil {
		t.Fatalf("InsertUser ha fallat: %v", err)
	}

	token := "TOKEN_EXPIRAT"
	if err := dbInstance.SaveActivationToken(email, token); err != nil {
		t.Fatalf("SaveActivationToken ha fallat: %v", err)
	}

	// Forcem expira_token al passat (SQLite)
	if _, err := dbInstance.Exec(
		"UPDATE usuaris SET expira_token = datetime('now','-1 hour') WHERE correu = ?",
		email,
	); err != nil {
		t.Fatalf("no puc forçar expira_token al passat: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/activar?token="+token, nil)
	req.RemoteAddr = "127.0.0.1:50003"

	rr := httptest.NewRecorder()
	app.ActivarUsuariHTTP(rr, req)

	res := rr.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("s'esperava status 200 fins i tot amb token expirat, però tinc %d. Cos:\n%s",
			res.StatusCode, rr.Body.String())
	}

	updated, err := dbInstance.GetUserByEmail(email)
	if err != nil {
		t.Fatalf("GetUserByEmail ha fallat després d'intent amb token expirat: %v", err)
	}
	if updated == nil {
		t.Fatalf("no s'ha trobat l'usuari %s després de cridar /activar amb token expirat", email)
	}
	if updated.Active {
		t.Fatalf("Active hauria de seguir sent false per token expirat, però és true")
	}
}
