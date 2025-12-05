package unit

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// Tests centrats en els handlers d'usuari que treballen amb HTTP.

func TestRegistrarUsuari_InvalidCSRF(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	form := url.Values{}
	form.Set("nom", "Prova")
	form.Set("cognoms", "Usuari Test")
	form.Set("email", "prova@example.org")
	form.Set("contrassenya", "S3cret!!")
	form.Set("confirmar_contrasenya", "S3cret!!")
	form.Set("captcha", "1234")
	form.Set("usuari", "provauser")
	form.Set("accepta_condicions", "on")
	// NO establim csrf_token ni cookie => validateCSRF ha de fallar

	req := httptest.NewRequest(http.MethodPost, "/registre", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()

	app.RegistrarUsuari(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("esperava 403 per CSRF invàlid, tinc %d", rr.Code)
	}
}

func TestIniciarSessio_InvalidMethod(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	req := httptest.NewRequest(http.MethodGet, "/login", nil)
	rr := httptest.NewRecorder()

	app.IniciarSessio(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("esperava 405 per mètode no permès, tinc %d", rr.Code)
	}
}

func TestTancarSessio_SenseCookie(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	req := httptest.NewRequest(http.MethodGet, "/logout", nil)
	rr := httptest.NewRecorder()

	app.TancarSessio(rr, req)

	// Sense cookie, actualment redirigeix a "/" amb 303
	if rr.Code != http.StatusSeeOther && rr.Code != http.StatusFound {
		t.Fatalf("esperava redirecció si no hi ha cookie, tinc %d", rr.Code)
	}
}

func TestVerificarSessio_WithoutSession(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	req := httptest.NewRequest(http.MethodGet, "/privat", nil)

	user, ok := app.VerificarSessio(req)
	if ok || user != nil {
		t.Fatalf("sense cookie de sessió no hauria de validar, ok=%v user=%v", ok, user)
	}
}
