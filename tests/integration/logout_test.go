package integration

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
	"golang.org/x/crypto/bcrypt"
)

// TestLogoutSenseCookie comprova que, si no hi ha cookie de sessió,
// el handler de logout redirigeix simplement a "/" sense petar.
func TestLogoutSenseCookie(t *testing.T) {
	app, _ := newTestAppForLogin(t, "test_logout_sense_cookie.sqlite3")

	req := httptest.NewRequest(http.MethodGet, "/logout", nil)
	req.RemoteAddr = "127.0.0.1:80001"

	rr := httptest.NewRecorder()
	app.TancarSessio(rr, req)

	res := rr.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusSeeOther {
		t.Fatalf("esperava status %d, rebut %d", http.StatusSeeOther, res.StatusCode)
	}

	loc := res.Header.Get("Location")
	if loc != "/" {
		t.Fatalf("esperava Location '/', rebut %q", loc)
	}
}

// TestLogoutAmbCookieSessio comprova que, si hi ha cookie cg_session,
// el handler:
//   - redirigeix a "/"
//   - envia una cookie cg_session buida amb MaxAge=-1 (esborrada)
func TestLogoutAmbCookieSessio(t *testing.T) {
	app, _ := newTestAppForLogin(t, "test_logout_amb_cookie.sqlite3")

	req := httptest.NewRequest(http.MethodGet, "/logout", nil)
	req.RemoteAddr = "127.0.0.1:80002"

	// Simulem que el navegador porta una sessió activa
	req.AddCookie(&http.Cookie{
		Name:  "cg_session",
		Value: "sessio-de-prova",
		Path:  "/",
	})

	rr := httptest.NewRecorder()
	app.TancarSessio(rr, req)

	res := rr.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusSeeOther {
		t.Fatalf("esperava status %d, rebut %d", http.StatusSeeOther, res.StatusCode)
	}

	loc := res.Header.Get("Location")
	if loc != "/" {
		t.Fatalf("esperava Location '/', rebut %q", loc)
	}

	var sessCookie *http.Cookie
	for _, c := range res.Cookies() {
		if c.Name == "cg_session" {
			sessCookie = c
			break
		}
	}

	if sessCookie == nil {
		t.Fatalf("esperava una cookie 'cg_session' a la resposta")
	}

	if sessCookie.Value != "" {
		t.Fatalf("esperava cookie 'cg_session' buida, rebut %q", sessCookie.Value)
	}

	if sessCookie.MaxAge != -1 {
		t.Fatalf("esperava MaxAge=-1 per esborrar la cookie, rebut %d", sessCookie.MaxAge)
	}
}

// TestLogoutRevocaSessioEnBD comprova que,
//   - el login crea una sessió activa a BD
//   - el logout la marca com a no activa (revocat=1)
func TestLogoutRevocaSessioEnBD(t *testing.T) {
	app, dbInstance := newTestAppForLogin(t, "test_logout_revoca.sqlite3")

	// 1) Creem un usuari actiu amb contrasenya hashejada
	rawPassword := "P4ssword!"
	hash, err := bcrypt.GenerateFromPassword([]byte(rawPassword), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("no puc generar hash de contrasenya: %v", err)
	}

	email := "logout.revoca@example.com"
	user := &db.User{
		Usuari:        "logout_revoca",
		Name:          "Test",
		Surname:       "Logout",
		Email:         email,
		Password:      hash,
		DataNaixament: "1990-01-01",
		Active:        true,
		CreatedAt:     time.Now().Format(time.RFC3339),
	}

	if err := dbInstance.InsertUser(user); err != nil {
		t.Fatalf("InsertUser ha fallat: %v", err)
	}

	// Comptem sessions actives abans de login
	before := countActiveSessions(t, dbInstance)

	// 2) Fem login per obtenir una cookie cg_session i crear la sessió a BD
	csrfToken := "csrf-logout-revoca"

	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("usuari", "logout_revoca")
	form.Set("contrassenya", rawPassword)
	form.Set("captcha", "8")
	form.Set("mantenir_sessio", "on")

	body := strings.NewReader(form.Encode())

	reqLogin := httptest.NewRequest(http.MethodPost, "/login", body)
	reqLogin.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqLogin.Header.Set("Accept-Language", "ca")
	reqLogin.AddCookie(&http.Cookie{
		Name:  "cg_csrf",
		Value: csrfToken,
	})
	reqLogin.RemoteAddr = "127.0.0.1:80003"

	rrLogin := httptest.NewRecorder()
	app.IniciarSessio(rrLogin, reqLogin)

	resLogin := rrLogin.Result()
	defer resLogin.Body.Close()

	if resLogin.StatusCode != http.StatusSeeOther {
		t.Fatalf("login: esperava 303 SeeOther, rebut %d. Cos:\n%s",
			resLogin.StatusCode, rrLogin.Body.String())
	}

	var sessionCookie *http.Cookie
	for _, c := range resLogin.Cookies() {
		if c.Name == "cg_session" {
			sessionCookie = c
			break
		}
	}
	if sessionCookie == nil || sessionCookie.Value == "" {
		t.Fatalf("login: s'esperava cookie cg_session no buida")
	}

	// Comptem sessions actives després del login
	afterLogin := countActiveSessions(t, dbInstance)
	if afterLogin != before+1 {
		t.Fatalf("després del login s'esperava una sessió activa més: abans=%d, després=%d",
			before, afterLogin)
	}

	// 3) Fem logout amb aquesta cookie
	reqLogout := httptest.NewRequest(http.MethodGet, "/logout", nil)
	reqLogout.RemoteAddr = "127.0.0.1:80004"
	reqLogout.AddCookie(sessionCookie)

	rrLogout := httptest.NewRecorder()
	app.TancarSessio(rrLogout, reqLogout)

	resLogout := rrLogout.Result()
	defer resLogout.Body.Close()

	if resLogout.StatusCode != http.StatusSeeOther {
		t.Fatalf("logout: esperava 303 SeeOther, rebut %d. Cos:\n%s",
			resLogout.StatusCode, rrLogout.Body.String())
	}

	// 4) Comptem sessions actives després del logout
	afterLogout := countActiveSessions(t, dbInstance)
	if afterLogout != afterLogin-1 {
		t.Fatalf("després del logout s'esperava una sessió activa menys: abans=%d, després=%d",
			afterLogin, afterLogout)
	}
}
