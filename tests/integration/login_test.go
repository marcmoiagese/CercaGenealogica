package integration

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
	"golang.org/x/crypto/bcrypt"
)

// newTestAppForLogin crea una App i una BD SQLite temporal per provar el login.
func newTestAppForLogin(t *testing.T, dbFileName string) (*core.App, db.DB) {
	t.Helper()

	// Ens assegurem que estem a l'arrel del projecte
	projectRoot := findProjectRoot(t)
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatalf("no puc fer chdir a l'arrel del projecte (%s): %v", projectRoot, err)
	}

	// Carreguem plantilles perquè el handler pugui renderitzar pàgines
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

	t.Cleanup(func() {
		app.Close()
	})

	return app, dbInstance
}

// TestFluxLoginCorrecte comprova que un usuari actiu amb credencials correctes
// pot fer login, rep cookie de sessió i és redirigit a /inici.
func TestFluxLoginCorrecte(t *testing.T) {
	app, dbInstance := newTestAppForLogin(t, "test_login_ok.sqlite3")

	// 1. Creem un usuari actiu amb contrasenya hashejada
	rawPassword := "P4ssword!"
	hash, err := bcrypt.GenerateFromPassword([]byte(rawPassword), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("no puc generar hash de contrasenya: %v", err)
	}

	email := "login.correcte@example.com"
	user := &db.User{
		Usuari:        "login_ok",
		Name:          "Test",
		Surname:       "Login",
		Email:         email,
		Password:      hash,
		DataNaixament: "1990-01-01",
		Active:        true,
		CreatedAt:     time.Now().Format(time.RFC3339),
	}

	if err := dbInstance.InsertUser(user); err != nil {
		t.Fatalf("InsertUser ha fallat: %v", err)
	}

	// 2. Preparem el formulari de login
	csrfToken := "csrf-login-ok"

	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("usuari", "login_ok") // pot ser usuari o email
	form.Set("contrassenya", rawPassword)
	form.Set("captcha", "8")          // 5 + 3
	form.Set("mantenir_sessio", "on") // opcional

	body := strings.NewReader(form.Encode())

	req := httptest.NewRequest(http.MethodPost, "/login", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept-Language", "ca")
	// CSRF double-submit: cookie + hidden field
	req.AddCookie(&http.Cookie{
		Name:  "cg_csrf",
		Value: csrfToken,
	})
	req.RemoteAddr = "127.0.0.1:60001"

	// 3. Executem el handler real de login
	rr := httptest.NewRecorder()
	app.IniciarSessio(rr, req)

	res := rr.Result()
	defer res.Body.Close()

	// Esperem una redirecció a /inici (303 See Other)
	if res.StatusCode != http.StatusSeeOther {
		t.Fatalf("esperava 303 SeeOther, però tinc %d. Cos:\n%s",
			res.StatusCode, rr.Body.String())
	}

	loc := res.Header.Get("Location")
	if loc != "/inici" {
		t.Fatalf("esperava redirecció a /inici, però tinc %q", loc)
	}

	// 4. Comprovem que s'ha creat la cookie de sessió
	var sessionCookie *http.Cookie
	for _, c := range res.Cookies() {
		if c.Name == "cg_session" {
			sessionCookie = c
			break
		}
	}
	if sessionCookie == nil || sessionCookie.Value == "" {
		t.Fatalf("després d'un login correcte, s'esperava cookie cg_session no buida")
	}
}

// TestFluxLoginContrasenyaIncorrecta comprova que una contrasenya dolenta
// no crea sessió ni cookie, i es queda a la pàgina de login.
func TestFluxLoginContrasenyaIncorrecta(t *testing.T) {
	app, dbInstance := newTestAppForLogin(t, "test_login_badpass.sqlite3")

	// 1. Usuari actiu amb una contrasenya determinada
	rawPassword := "P4ssword!"
	hash, err := bcrypt.GenerateFromPassword([]byte(rawPassword), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("no puc generar hash de contrasenya: %v", err)
	}

	email := "login.badpass@example.com"
	user := &db.User{
		Usuari:        "login_badpass",
		Name:          "Test",
		Surname:       "BadPass",
		Email:         email,
		Password:      hash,
		DataNaixament: "1990-01-01",
		Active:        true,
		CreatedAt:     time.Now().Format(time.RFC3339),
	}

	if err := dbInstance.InsertUser(user); err != nil {
		t.Fatalf("InsertUser ha fallat: %v", err)
	}

	// 2. Formulari amb contrasenya incorrecta
	csrfToken := "csrf-login-badpass"

	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("usuari", "login_badpass")
	form.Set("contrassenya", "ContrasenyaDolenta")
	form.Set("captcha", "8")
	form.Set("mantenir_sessio", "on")

	body := strings.NewReader(form.Encode())

	req := httptest.NewRequest(http.MethodPost, "/login", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept-Language", "ca")
	req.AddCookie(&http.Cookie{
		Name:  "cg_csrf",
		Value: csrfToken,
	})
	req.RemoteAddr = "127.0.0.1:60002"

	rr := httptest.NewRecorder()
	app.IniciarSessio(rr, req)

	res := rr.Result()
	defer res.Body.Close()

	// En cas d'error d'autenticació el codi que tens ara re-renderitza l'index,
	// així que el codi esperat és 200.
	if res.StatusCode != http.StatusOK {
		t.Fatalf("esperava status 200 en login amb contrasenya incorrecta, però tinc %d. Cos:\n%s",
			res.StatusCode, rr.Body.String())
	}

	// No s'hauria de crear cookie de sessió
	for _, c := range res.Cookies() {
		if c.Name == "cg_session" {
			t.Fatalf("no s'hauria de crear cookie cg_session quan la contrasenya és incorrecta")
		}
	}
}
