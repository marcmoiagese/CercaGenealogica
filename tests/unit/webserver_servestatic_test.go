package unit

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
)

// Helper: assegura que el fitxer estàtic existeix, sinó fa Skip del test.
func mustHaveStaticFile(t *testing.T, rel string) {
	t.Helper()
	if _, err := os.Stat("static/" + rel); err != nil {
		t.Skipf("static/%s no existeix, salto el test: %v", rel, err)
	}
}

// --- 1) Fitxer existent i autoritzat → 200 ---

func TestServeStatic_ExistingFile(t *testing.T) {
	// HA DE SER un dels fitxers definits a allowedFiles
	// i existent a static/.
	const rel = "css/estils.css"

	mustHaveStaticFile(t, rel)

	req := httptest.NewRequest(http.MethodGet, "/static/"+rel, nil)
	rr := httptest.NewRecorder()

	core.ServeStatic(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("esperava status 200 per %q, tinc %d", rel, rr.Code)
	}
	if rr.Body.Len() == 0 {
		t.Fatalf("esperava cos no buit per %q", rel)
	}
}

// --- 2) Fitxer existent però NO al mapa allowedFiles → 403 ---

func TestServeStatic_NonAllowedFileGets403(t *testing.T) {
	// Triem un nom que segur que NO és al mapa allowedFiles.
	const rel = "css/prova-no-llista.css"
	fullPath := "static/" + rel

	// Creem el fitxer perquè os.Stat passi i arribi al check d'allowedFiles.
	if err := os.MkdirAll("static/css", 0o755); err != nil {
		t.Fatalf("no puc crear directori static/css: %v", err)
	}
	if err := os.WriteFile(fullPath, []byte("body { background: #000; }"), 0o644); err != nil {
		t.Fatalf("no puc crear fitxer de prova: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(fullPath) })

	req := httptest.NewRequest(http.MethodGet, "/static/"+rel, nil)
	rr := httptest.NewRecorder()

	core.ServeStatic(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("esperava 403 per fitxer no autoritzat, tinc %d", rr.Code)
	}
}

// --- 3) Path traversal → 403 ---

func TestServeStatic_PathTraversalGets403(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/static/../core/webserver.go", nil)
	rr := httptest.NewRecorder()

	core.ServeStatic(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("esperava 403 per path traversal, tinc %d", rr.Code)
	}
}

// --- 4) JS/CSS amb Referer sospitós → 403 ---

func TestServeStatic_SuspiciousRefererGets403(t *testing.T) {
	// Fitxer JS autoritzat segons allowedFiles
	const rel = "js/idioma.js"

	mustHaveStaticFile(t, rel)

	req := httptest.NewRequest(http.MethodGet, "/static/"+rel, nil)
	// Referer que NO comença per cap de les rutes “bones”
	req.Header.Set("Referer", "http://evil.example.com/malicios")

	rr := httptest.NewRecorder()

	core.ServeStatic(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("esperava 403 per referer sospitós, tinc %d", rr.Code)
	}
}
