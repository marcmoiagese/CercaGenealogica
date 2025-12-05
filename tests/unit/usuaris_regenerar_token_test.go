package unit

import (
	"html/template"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
)

// MostrarFormulariRegenerarToken hauria de renderitzar la pàgina sense petar.
func TestMostrarFormulariRegenerarToken_OK(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	// Sobreescrivim les plantilles globals perquè "regenerar-token.html" existeixi
	oldTpl := core.Templates
	defer func() { core.Templates = oldTpl }()

	const tpl = `{{ define "regenerar-token.html" }}OK regenerar token{{ end }}`
	core.Templates = template.Must(template.New("regenerar-token.html").Parse(tpl))

	req := httptest.NewRequest(http.MethodGet, "/regenerar-token", nil)
	rr := httptest.NewRecorder()

	http.HandlerFunc(app.MostrarFormulariRegenerarToken).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("esperava 200 a MostrarFormulariRegenerarToken, tinc %d", rr.Code)
	}
}

// ProcessarRegenerarToken amb GET ha de redirigir al formulari.
func TestProcessarRegenerarToken_GetRedirects(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	req := httptest.NewRequest(http.MethodGet, "/regenerar-token", nil)
	rr := httptest.NewRecorder()

	http.HandlerFunc(app.ProcessarRegenerarToken).ServeHTTP(rr, req)

	resp := rr.Result()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("esperava redirect (303/303), tinc %d", resp.StatusCode)
	}
	loc := resp.Header.Get("Location")
	if loc != "/regenerar-token" {
		t.Errorf("esperava Location=/regenerar-token, tinc %q", loc)
	}
}

// ProcessarRegenerarToken amb POST però sense CSRF ha de donar Forbidden (entra a RegenerarTokenActivacio).
func TestProcessarRegenerarToken_PostInvalidCSRF(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	req := httptest.NewRequest(http.MethodPost, "/regenerar-token?email=test@example.com", nil)
	rr := httptest.NewRecorder()

	http.HandlerFunc(app.ProcessarRegenerarToken).ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("esperava 403 per CSRF invàlid, tinc %d", rr.Code)
	}
}

// CheckAvailability amb mètode incorrecte (GET) ha de retornar 405.
func TestCheckAvailability_InvalidMethod(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	req := httptest.NewRequest(http.MethodGet, "/check-availability", nil)
	rr := httptest.NewRecorder()

	http.HandlerFunc(app.CheckAvailability).ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("esperava 405 per mètode no permès, tinc %d", rr.Code)
	}
}
