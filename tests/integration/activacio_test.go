package integration

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

// TestActivacioUsuariFluxCorrecte comprova que un usuari amb token
// d'activació vàlid passa de Active=false a Active=true.
func TestActivacioUsuariFluxCorrecte(t *testing.T) {
	// 1. Assegurem-nos que som a l'arrel del projecte
	projectRoot := findProjectRoot(t)
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatalf("no puc fer chdir a l'arrel del projecte (%s): %v", projectRoot, err)
	}

	// 2. Carreguem plantilles (activat-user.html)
	loadTemplatesForTests(t, projectRoot)

	// 3. BD SQLite de test
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_activacio_ok.sqlite3")

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
	defer dbInstance.Close()

	app := core.NewApp(cfg, dbInstance)
	defer app.Close()

	// 4. Creem un usuari inactiu directament a BD
	email := "activacio.correcte@example.com"
	user := &db.User{
		Usuari:        "activacio_ok",
		Name:          "Test",
		Surname:       "Activacio",
		Email:         email,
		Password:      []byte("dummy"), // no testegem login aquí
		DataNaixament: "1990-01-01",
		Active:        false,
		CreatedAt:     time.Now().Format(time.RFC3339),
		Pais:          "",
		Estat:         "",
		Provincia:     "",
		Poblacio:      "",
		CodiPostal:    "",
	}

	if err := dbInstance.InsertUser(user); err != nil {
		t.Fatalf("InsertUser ha fallat: %v", err)
	}

	// 5. Assignem un token d'activació a aquest usuari
	token := "TOKEN_ACTIVACIO_OK"
	if err := dbInstance.SaveActivationToken(email, token); err != nil {
		t.Fatalf("SaveActivationToken ha fallat: %v", err)
	}

	// 6. Fem una petició GET /activar?token=TOKEN_ACTIVACIO_OK
	req := httptest.NewRequest(http.MethodGet, "/activar?token="+token, nil)
	req.RemoteAddr = "127.0.0.1:40001"

	rr := httptest.NewRecorder()
	app.ActivarUsuariHTTP(rr, req)

	res := rr.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("esperava status 200 a l'activació correcta, però tinc %d. Cos:\n%s",
			res.StatusCode, rr.Body.String())
	}

	// 7. Verifiquem a BD que el camp Active ara és true
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

// TestActivacioUsuariTokenInvalid comprova que un token invàlid
// NO activa l’usuari (Active continua sent false).
func TestActivacioUsuariTokenInvalid(t *testing.T) {
	// 1. Arrel del projecte i plantilles
	projectRoot := findProjectRoot(t)
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatalf("no puc fer chdir a l'arrel del projecte (%s): %v", projectRoot, err)
	}

	loadTemplatesForTests(t, projectRoot)

	// 2. BD de test
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_activacio_invalid.sqlite3")

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
	defer dbInstance.Close()

	app := core.NewApp(cfg, dbInstance)
	defer app.Close()

	// 3. Usuari inactiu a BD
	email := "activacio.invalid@example.com"
	user := &db.User{
		Usuari:        "activacio_invalid",
		Name:          "Test",
		Surname:       "Invalid",
		Email:         email,
		Password:      []byte("dummy"),
		DataNaixament: "1990-01-01",
		Active:        false,
		CreatedAt:     time.Now().Format(time.RFC3339),
	}

	if err := dbInstance.InsertUser(user); err != nil {
		t.Fatalf("InsertUser ha fallat: %v", err)
	}

	// 4. (Opcional) assignem un token real diferent
	if err := dbInstance.SaveActivationToken(email, "TOKEN_REAL_PERO_NO_L_FAREM_SERVIR"); err != nil {
		t.Fatalf("SaveActivationToken ha fallat: %v", err)
	}

	// 5. Cridem l'endpoint amb un token inventat que no existeix a BD
	invalidToken := "TOKEN_QUE_NO_EXISTEIX"
	req := httptest.NewRequest(http.MethodGet, "/activar?token="+invalidToken, nil)
	req.RemoteAddr = "127.0.0.1:40002"

	rr := httptest.NewRecorder()
	app.ActivarUsuariHTTP(rr, req)

	res := rr.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("esperava status 200 fins i tot amb token invàlid, però tinc %d. Cos:\n%s",
			res.StatusCode, rr.Body.String())
	}

	// 6. Comprovem que l'usuari CONTINUA inactiu
	updated, err := dbInstance.GetUserByEmail(email)
	if err != nil {
		t.Fatalf("GetUserByEmail ha fallat després d'activació amb token invàlid: %v", err)
	}
	if updated == nil {
		t.Fatalf("no s'ha trobat l'usuari %s després de cridar /activar amb token invàlid", email)
	}
	if updated.Active {
		t.Fatalf("Active hauria de seguir sent false per token invàlid, però és true")
	}
}
