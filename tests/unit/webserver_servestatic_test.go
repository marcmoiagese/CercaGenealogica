package unit

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
)

func TestServeStatic_BlocksPathTraversal(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/static/../secret.txt", nil)
	rr := httptest.NewRecorder()

	core.ServeStatic(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("esperava 403 per path traversal, tinc %d", rr.Code)
	}
}

func TestServeStatic_BlocksDirectoryListing(t *testing.T) {
	// Creem una carpeta dins de static/ perquè ServeStatic la vegi com a directori
	if err := os.MkdirAll("static/testdir", 0o755); err != nil {
		t.Fatalf("no puc crear static/testdir: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/static/testdir/", nil)
	rr := httptest.NewRecorder()

	core.ServeStatic(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("esperava 403 per llistar carpeta, tinc %d", rr.Code)
	}
}

func TestServeStatic_UnauthorizedFile(t *testing.T) {
	// Fitxer que existeix però NO és a allowedFiles → ha de donar 403
	if err := os.MkdirAll("static/testdir", 0o755); err != nil {
		t.Fatalf("no puc crear static/testdir: %v", err)
	}

	if err := os.WriteFile("static/testdir/hello.txt", []byte("hola món"), 0o644); err != nil {
		t.Fatalf("no puc crear fitxer: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/static/testdir/hello.txt", nil)
	rr := httptest.NewRecorder()

	core.ServeStatic(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("esperava 403 per fitxer no autoritzat, tinc %d", rr.Code)
	}
}

// Helper: assegura que el fitxer estàtic existeix, sinó fa Skip del test.
func mustHaveStaticFile(t *testing.T, rel string) {
	t.Helper()
	if _, err := os.Stat("static/" + rel); err != nil {
		t.Skipf("static/%s no existeix, salto el test: %v", rel, err)
	}
}

func TestServeStatic_UnauthorizedExistingFile(t *testing.T) {
	// static/prova.css existeix al repo, però NO és a allowedFiles
	const rel = "prova.css"

	mustHaveStaticFile(t, rel)

	req := httptest.NewRequest(http.MethodGet, "/static/"+rel, nil)
	rr := httptest.NewRecorder()

	core.ServeStatic(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("esperava 403 per fitxer existent però no autoritzat, tinc %d", rr.Code)
	}
}
