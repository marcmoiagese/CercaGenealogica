package integration

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// TestE2EAuthFlow comprova el flux complet:
//
//  1. Registre d'usuari via HTTP POST /registre
//  2. Assignació de token conegut i activació via HTTP GET /activar
//  3. Login via HTTP POST /login → cookie cg_session
//  4. Accés a /inici amb sessió → 200 OK
//  5. Logout via HTTP GET /logout
//  6. Accés a /inici sense sessió → redirecció
func TestE2EAuthFlow(t *testing.T) {
	app, dbInstance := newTestAppForLogin(t, "test_e2e_auth.sqlite3")

	// ─────────────────────────────────────────────
	// 1) REGISTRE
	// ─────────────────────────────────────────────
	email := "e2e.user@example.com"
	username := "e2euser"
	password := "P4ssword!"
	csrfReg := "csrf-e2e-reg"

	formReg := url.Values{}
	formReg.Set("csrf_token", csrfReg)
	formReg.Set("usuari", username)
	formReg.Set("nom", "EndToEnd")
	formReg.Set("cognoms", "Test")
	formReg.Set("data_naixament", "1990-01-01")
	formReg.Set("email", email)
	formReg.Set("contrassenya", password)
	formReg.Set("confirmar_contrasenya", password)
	formReg.Set("captcha", "8")             // 5+3
	formReg.Set("accepta_condicions", "on") // imprescindible

	bodyReg := strings.NewReader(formReg.Encode())
	reqReg := httptest.NewRequest(http.MethodPost, "/registre", bodyReg)
	reqReg.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqReg.Header.Set("Accept-Language", "ca")
	reqReg.AddCookie(&http.Cookie{
		Name:  "cg_csrf",
		Value: csrfReg,
	})
	reqReg.RemoteAddr = "127.0.0.1:90001"

	rrReg := httptest.NewRecorder()
	app.RegistrarUsuari(rrReg, reqReg)

	resReg := rrReg.Result()
	defer resReg.Body.Close()

	if resReg.StatusCode != http.StatusOK {
		t.Fatalf("registre: esperava 200 OK, rebut %d. Cos:\n%s",
			resReg.StatusCode, rrReg.Body.String())
	}

	// Verifiquem que l'usuari existeix a BD
	u, err := dbInstance.GetUserByEmail(email)
	if err != nil {
		t.Fatalf("registre: GetUserByEmail ha fallat: %v", err)
	}
	if u == nil {
		t.Fatalf("registre: no s'ha creat l'usuari %s", email)
	}
	if u.Active {
		t.Fatalf("registre: l'usuari hauria de començar inactiu (Active=false)")
	}

	// ─────────────────────────────────────────────
	// 2) ACTIVACIÓ (assignem token conegut)
	// ─────────────────────────────────────────────
	activationToken := "E2E_TOKEN_ACTIVACIO"
	if err := dbInstance.SaveActivationToken(email, activationToken); err != nil {
		t.Fatalf("activacio: SaveActivationToken ha fallat: %v", err)
	}

	reqAct := httptest.NewRequest(http.MethodGet, "/activar?token="+activationToken, nil)
	reqAct.RemoteAddr = "127.0.0.1:90002"

	rrAct := httptest.NewRecorder()
	app.ActivarUsuariHTTP(rrAct, reqAct)

	resAct := rrAct.Result()
	defer resAct.Body.Close()

	if resAct.StatusCode != http.StatusOK {
		t.Fatalf("activacio: esperava 200 OK, rebut %d. Cos:\n%s",
			resAct.StatusCode, rrAct.Body.String())
	}

	u2, err := dbInstance.GetUserByEmail(email)
	if err != nil {
		t.Fatalf("activacio: GetUserByEmail ha fallat: %v", err)
	}
	if u2 == nil || !u2.Active {
		t.Fatalf("activacio: s'esperava usuari actiu després d'activació")
	}

	// ─────────────────────────────────────────────
	// 3) LOGIN
	// ─────────────────────────────────────────────
	csrfLogin := "csrf-e2e-login"

	formLogin := url.Values{}
	formLogin.Set("csrf_token", csrfLogin)
	formLogin.Set("usuari", username) // podríem usar email també
	formLogin.Set("contrassenya", password)
	formLogin.Set("captcha", "8")
	formLogin.Set("mantenir_sessio", "on")

	bodyLogin := strings.NewReader(formLogin.Encode())
	reqLogin := httptest.NewRequest(http.MethodPost, "/login", bodyLogin)
	reqLogin.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqLogin.Header.Set("Accept-Language", "ca")
	reqLogin.AddCookie(&http.Cookie{
		Name:  "cg_csrf",
		Value: csrfLogin,
	})
	reqLogin.RemoteAddr = "127.0.0.1:90003"

	rrLogin := httptest.NewRecorder()
	app.IniciarSessio(rrLogin, reqLogin)

	resLogin := rrLogin.Result()
	defer resLogin.Body.Close()

	if resLogin.StatusCode != http.StatusSeeOther {
		t.Fatalf("login: esperava 303 SeeOther, rebut %d. Cos:\n%s",
			resLogin.StatusCode, rrLogin.Body.String())
	}
	if loc := resLogin.Header.Get("Location"); loc != "/inici" {
		t.Fatalf("login: esperava redirecció a /inici, rebut %q", loc)
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

	// ─────────────────────────────────────────────
	// 4) /INICI AMB SESSIÓ
	// ─────────────────────────────────────────────
	reqInici := httptest.NewRequest(http.MethodGet, "/inici", nil)
	reqInici.RemoteAddr = "127.0.0.1:90004"
	reqInici.AddCookie(sessionCookie)

	rrInici := httptest.NewRecorder()
	handlerSimulatInici(rrInici, reqInici)

	resInici := rrInici.Result()
	defer resInici.Body.Close()

	if resInici.StatusCode != http.StatusOK {
		t.Fatalf("inici: esperava 200 OK amb sessió, rebut %d. Cos:\n%s",
			resInici.StatusCode, rrInici.Body.String())
	}

	// ─────────────────────────────────────────────
	// 5) LOGOUT
	// ─────────────────────────────────────────────
	reqLogout := httptest.NewRequest(http.MethodGet, "/logout", nil)
	reqLogout.RemoteAddr = "127.0.0.1:90005"
	reqLogout.AddCookie(sessionCookie)

	rrLogout := httptest.NewRecorder()
	app.TancarSessio(rrLogout, reqLogout)

	resLogout := rrLogout.Result()
	defer resLogout.Body.Close()

	if resLogout.StatusCode != http.StatusSeeOther {
		t.Fatalf("logout: esperava 303 SeeOther, rebut %d. Cos:\n%s",
			resLogout.StatusCode, rrLogout.Body.String())
	}
	if loc := resLogout.Header.Get("Location"); loc != "/" {
		t.Fatalf("logout: esperava redirecció a '/', rebut %q", loc)
	}

	// Opcional: verifiquem cookie esborrada
	var deletedCookie *http.Cookie
	for _, c := range resLogout.Cookies() {
		if c.Name == "cg_session" {
			deletedCookie = c
			break
		}
	}
	if deletedCookie == nil {
		t.Fatalf("logout: s'esperava Set-Cookie per cg_session")
	}
	if deletedCookie.Value != "" || deletedCookie.MaxAge != -1 {
		t.Fatalf("logout: s'esperava cookie cg_session buida i MaxAge=-1, rebut value=%q MaxAge=%d",
			deletedCookie.Value, deletedCookie.MaxAge)
	}

	// ─────────────────────────────────────────────
	// 6) /INICI DESPRÉS DE LOGOUT (sense cookie)
	// ─────────────────────────────────────────────
	reqInici2 := httptest.NewRequest(http.MethodGet, "/inici", nil)
	reqInici2.RemoteAddr = "127.0.0.1:90006"
	// Simulem el navegador després d'aplicar Set-Cookie d'esborrat:
	// NO tornem a enviar cg_session.

	rrInici2 := httptest.NewRecorder()
	handlerSimulatInici(rrInici2, reqInici2)

	resInici2 := rrInici2.Result()
	defer resInici2.Body.Close()

	if resInici2.StatusCode != http.StatusSeeOther && resInici2.StatusCode != http.StatusFound {
		t.Fatalf("inici després de logout: esperava redirecció (302/303), rebut %d. Cos:\n%s",
			resInici2.StatusCode, rrInici2.Body.String())
	}
}
