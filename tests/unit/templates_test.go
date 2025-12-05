package unit

import (
	"html/template"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
)

// Comprova que RenderTemplate pot renderitzar una plantilla senzilla definida en temps de test.
func TestRenderTemplate_SimpleTemplate(t *testing.T) {
	// Guardem el motor actual i el restaurem al final
	oldTemplates := core.Templates
	defer func() {
		core.Templates = oldTemplates
	}()

	// Crear directori temporal i una plantilla mínima
	tplDir := t.TempDir()
	tplPath := filepath.Join(tplDir, "test.html")

	const tpl = `{{ define "test.html" }}Hola {{ .Lang }}{{ end }}`

	if err := os.WriteFile(tplPath, []byte(tpl), 0o644); err != nil {
		t.Fatalf("no puc escriure plantilla temporal: %v", err)
	}

	// Carregar només aquesta plantilla al motor global
	core.Templates = template.Must(template.ParseFiles(tplPath))

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	// RenderTemplate no retorna error: només escriu al ResponseWriter
	core.RenderTemplate(rr, req, "test.html", nil)

	body := rr.Body.String()
	if !strings.Contains(body, "Hola") {
		t.Fatalf("esperava que la sortida contingués 'Hola', tinc: %q", body)
	}
}

// Comprova que RenderTemplate funciona per una pàgina pública real (index.html)
func TestRenderTemplate_Index(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()

	core.RenderTemplate(rr, req, "index.html", nil)

	// Si hi hagués un problema greu (plantilla inexistent, panic, etc.)
	// el més probable és que veiéssim un 500.
	if rr.Code >= http.StatusInternalServerError {
		t.Fatalf("RenderTemplate(index.html) ha retornat codi d'error: %d", rr.Code)
	}
}

// Comprova que RenderPrivateTemplate no peta amb una pàgina privada
// (index-logedin.html). No ens casem amb el contingut exactament.
func TestRenderPrivateTemplate_IndexLogedin_UsesPrivateHeader(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/inici", nil)
	rr := httptest.NewRecorder()

	core.RenderPrivateTemplate(rr, req, "index-logedin.html", nil)

	if rr.Code >= http.StatusInternalServerError {
		t.Fatalf("RenderPrivateTemplate(index-logedin.html) ha retornat codi d'error: %d", rr.Code)
	}
}

// Comprova que LogLoadedTemplates s'executa sense panic
func TestLogLoadedTemplates_DoesNotPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("LogLoadedTemplates ha fet panic: %v", r)
		}
	}()

	core.LogLoadedTemplates()
}
