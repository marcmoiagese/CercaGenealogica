package integration

import (
	"html/template"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

// findProjectRoot busca l'arrel del projecte pujant directoris fins trobar `cnf/config.cfg`.
func findProjectRoot(t *testing.T) string {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("no puc obtenir working dir: %v", err)
	}

	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "cnf", "config.cfg")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("no he pogut trobar l'arrel del projecte a partir de %s", wd)
		}
		dir = parent
	}
}

var templatesOnce sync.Once

// loadTemplatesForTests carrega les plantilles a core.Templates perquè
// RenderTemplate pugui trobar registre-correcte.html / registre-incorrecte.html.
func loadTemplatesForTests(t *testing.T, projectRoot string) {
	t.Helper()

	templatesOnce.Do(func() {
		patterns := []string{
			filepath.Join(projectRoot, "templates", "*.html"),
			filepath.Join(projectRoot, "templates", "layouts", "*.html"),
		}

		for _, p := range patterns {
			matches, err := filepath.Glob(p)
			if err != nil {
				t.Fatalf("error fent glob sobre %s: %v", p, err)
			}
			if len(matches) == 0 {
				t.Fatalf("no s’han trobat plantilles per al patró %s", p)
			}
			core.Templates = template.Must(core.Templates.ParseFiles(matches...))
		}
	})
}

// TestFluxRegistre fa un test d’integració bàsic del flux de registre:
//
//   - crea una BD SQLite temporal (fitxer) amb l'esquema de db/SQLite.sql
//   - crea una App amb aquesta DB
//   - carrega les plantilles
//   - envia un POST al mètode app.RegistrarUsuari
//   - comprova que retorna 200 (cas feliç)
func TestFluxRegistre(t *testing.T) {
	// 1. Arrel del projecte i cwd
	projectRoot := findProjectRoot(t)
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatalf("no puc fer chdir a l'arrel del projecte (%s): %v", projectRoot, err)
	}

	// 2. Carreguem plantilles
	loadTemplatesForTests(t, projectRoot)

	// 3. Configuració de BD de test
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_registre.sqlite3")

	cfg := map[string]string{
		"DB_ENGINE": "sqlite",
		"DB_PATH":   dbPath,
		"RECREADB":  "true", // crea l'esquema des de db/SQLite.sql
		"LOG_LEVEL": "silent",
	}

	// 4. Inicialitza la BD de test
	dbInstance, err := db.NewDB(cfg)
	if err != nil {
		t.Fatalf("NewDB ha fallat: %v", err)
	}
	defer dbInstance.Close()

	// 5. Crea una App amb aquesta config i BD
	app := core.NewApp(cfg, dbInstance)
	defer app.Close()

	// 6. Prepara el formulari de registre amb dades vàlides
	email := "test.integration@example.com"
	csrfToken := "test-csrf-token"

	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("usuari", "testintegration")
	form.Set("nom", "Test")
	form.Set("cognoms", "Integration")
	form.Set("data_naixament", "1990-01-01")
	form.Set("email", email)
	form.Set("contrassenya", "P4ssword!")
	form.Set("confirmar_contrasenya", "P4ssword!")
	form.Set("captcha", "8")             // 5 + 3
	form.Set("accepta_condicions", "on") // cal per no donar error

	body := strings.NewReader(form.Encode())
	req := httptest.NewRequest(http.MethodPost, "/registre", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	// CSRF double-submit: cookie + camp de formulari han de coincidir
	req.AddCookie(&http.Cookie{
		Name:  "cg_csrf",
		Value: csrfToken,
	})
	req.RemoteAddr = "127.0.0.1:12345"

	// 7. Executa el mètode real de registre
	rr := httptest.NewRecorder()
	app.RegistrarUsuari(rr, req)

	res := rr.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("esperava status 200, però tinc %d. Cos:\n%s",
			res.StatusCode, rr.Body.String())
	}
}

// TestFluxRegistreCaptchaInvalid comprova el cas d’error:
//   - CAPTCHA incorrecte
//   - ha de retornar 200 (pantalla d'error) i NO crear l’usuari a BD
func TestFluxRegistreCaptchaInvalid(t *testing.T) {
	// 1. Arrel del projecte i cwd
	projectRoot := findProjectRoot(t)
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatalf("no puc fer chdir a l'arrel del projecte (%s): %v", projectRoot, err)
	}

	// 2. Carreguem plantilles (registre-incorrecte.html)
	loadTemplatesForTests(t, projectRoot)

	// 3. Configuració de BD de test
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_registre_captcha.sqlite3")

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

	// 4. Formulari amb CAPTCHA incorrecte
	email := "test.captcha@example.com"
	csrfToken := "test-csrf-token-2"

	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("usuari", "testcaptcha")
	form.Set("nom", "Test")
	form.Set("cognoms", "Captcha")
	form.Set("data_naixament", "1990-01-01")
	form.Set("email", email)
	form.Set("contrassenya", "P4ssword!")
	form.Set("confirmar_contrasenya", "P4ssword!")
	form.Set("captcha", "7") // INCORRECTE: ha de ser 8
	form.Set("accepta_condicions", "on")

	body := strings.NewReader(form.Encode())
	req := httptest.NewRequest(http.MethodPost, "/registre", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept-Language", "ca")
	req.AddCookie(&http.Cookie{
		Name:  "cg_csrf",
		Value: csrfToken,
	})
	req.RemoteAddr = "127.0.0.1:23456"

	rr := httptest.NewRecorder()
	app.RegistrarUsuari(rr, req)

	res := rr.Result()
	defer res.Body.Close()
	respBody := rr.Body.String()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("esperava status 200, però tinc %d. Cos:\n%s",
			res.StatusCode, respBody)
	}

	// 5. Comprovem que NO s’ha creat l’usuari a la BD
	user, err := dbInstance.GetUserByEmail(email)
	if err == nil && user != nil {
		t.Fatalf("no hauria d’existir un usuari amb email %s quan el CAPTCHA és invàlid", email)
	}

	// Si vols, només per debug:
	// t.Logf("Cos resposta CAPTCHA invàlid:\n%s", respBody)
}
