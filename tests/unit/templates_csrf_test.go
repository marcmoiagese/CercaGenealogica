package unit

import (
	"html/template"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
)

var csrfTplOnce sync.Once

// enganxa una plantilla mínima a core.Templates que mostra .Data.CSRFToken
func ensureCSRFTestTemplate(t *testing.T) {
	t.Helper()

	csrfTplOnce.Do(func() {
		const src = `{{define "test_csrf_token.html"}}{{.Data.CSRFToken}}{{end}}`

		// Parse sobre el Template global de core
		core.Templates = template.Must(core.Templates.Parse(src))
	})
}

func renderWithCSRFToken(t *testing.T, token string, data interface{}) string {
	t.Helper()

	ensureCSRFTestTemplate(t)

	req := httptest.NewRequest(http.MethodGet, "/dummy", nil)
	// forcem que ensureCSRF utilitzi aquest token
	req.AddCookie(&http.Cookie{
		Name:  "cg_csrf",
		Value: token,
	})

	rr := httptest.NewRecorder()

	core.RenderTemplate(rr, req, "test_csrf_token.html", data)

	if rr.Code != http.StatusOK {
		t.Fatalf("esperava 200, tinc %d, body=%q", rr.Code, rr.Body.String())
	}

	return rr.Body.String()
}

type csrfStruct struct {
	CSRFToken string
	Other     string
}

func TestInjectCSRFToken_NilData(t *testing.T) {
	body := renderWithCSRFToken(t, "token-nil", nil)

	if !strings.Contains(body, "token-nil") {
		t.Fatalf("esperava veure el token CSRF %q a la sortida, body=%q", "token-nil", body)
	}
}

func TestInjectCSRFToken_MapData(t *testing.T) {
	data := map[string]interface{}{
		"foo":       "bar",
		"CSRFToken": "antic",
	}

	body := renderWithCSRFToken(t, "token-map", data)

	if !strings.Contains(body, "token-map") {
		t.Fatalf("esperava veure el token CSRF %q a la sortida, body=%q", "token-map", body)
	}
}

func TestInjectCSRFToken_PtrStruct(t *testing.T) {
	data := &csrfStruct{
		CSRFToken: "vell",
		Other:     "altre",
	}

	body := renderWithCSRFToken(t, "token-ptr", data)

	if !strings.Contains(body, "token-ptr") {
		t.Fatalf("esperava veure el token CSRF %q a la sortida, body=%q", "token-ptr", body)
	}
}

func TestInjectCSRFToken_StructValue(t *testing.T) {
	data := csrfStruct{
		CSRFToken: "vell",
		Other:     "altre",
	}

	body := renderWithCSRFToken(t, "token-val", data)

	if !strings.Contains(body, "token-val") {
		t.Fatalf("esperava veure el token CSRF %q a la sortida, body=%q", "token-val", body)
	}
}
