package unit

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/cnf"
	"github.com/marcmoiagese/CercaGenealogica/core"
)

func TestSecureHeadersAddsSecurityHeaders(t *testing.T) {
	base := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := core.SecureHeaders(base)

	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	rr := httptest.NewRecorder()

	handler(rr, req)
	resp := rr.Result()

	wantHeaders := []string{
		"Content-Security-Policy",
		"X-XSS-Protection",
		"X-Frame-Options",
		"Referrer-Policy",
		"X-Robots-Tag",
		"Access-Control-Allow-Origin",
		"Access-Control-Allow-Methods",
		"Access-Control-Allow-Headers",
		"Strict-Transport-Security",
		"X-Content-Type-Options",
	}

	for _, h := range wantHeaders {
		if resp.Header.Get(h) == "" {
			t.Errorf("header %s hauria d'estar informat", h)
		}
	}
}

func TestInitWebServerOverridesConfig(t *testing.T) {
	// Guardem una còpia profunda de la config actual
	oldCfg := make(map[string]string)
	for k, v := range cnf.Config {
		oldCfg[k] = v
	}
	defer func() {
		cnf.Config = oldCfg
	}()

	newCfg := map[string]string{
		"BLOCKED_IPS": "1.2.3.4",
		"ENV":         "test",
	}

	core.InitWebServer(newCfg)

	if got := cnf.Config["BLOCKED_IPS"]; got != "1.2.3.4" {
		t.Fatalf("esperava BLOCKED_IPS=1.2.3.4, tinc %q", got)
	}
}

func TestIsBlockedUsesConfig(t *testing.T) {
	// Guardem i restaurem config
	oldCfg := make(map[string]string)
	for k, v := range cnf.Config {
		oldCfg[k] = v
	}
	defer func() {
		cnf.Config = oldCfg
	}()

	cnf.Config["BLOCKED_IPS"] = "10.0.0.1,192.168.1.50"

	if !core.IsBlocked("10.0.0.1") {
		t.Errorf("10.0.0.1 hauria d'estar bloquejada")
	}
	if core.IsBlocked("8.8.8.8") {
		t.Errorf("8.8.8.8 no hauria d'estar bloquejada")
	}
}

func TestApplyRateLimitEventuallyBlocks(t *testing.T) {
	ip := "198.51.100.77"

	blocked := false
	for i := 0; i < 200; i++ {
		if !core.ApplyRateLimit(ip) {
			blocked = true
			break
		}
	}

	if !blocked {
		t.Fatalf("ApplyRateLimit hauria d'acabar bloquejant aquesta IP després de moltes peticions")
	}
}

func TestServeStatic_PathTraversalForbidden(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/static/../etc/passwd", nil)
	rr := httptest.NewRecorder()

	core.ServeStatic(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("esperava StatusForbidden per path traversal, tinc %d", rr.Code)
	}
}

func TestServeStatic_AllowedFileOK(t *testing.T) {
	// Ens assegurem que el fitxer existeix al cwd dels tests (tests/unit)
	if err := os.MkdirAll("static/img", 0o755); err != nil {
		t.Fatalf("no puc crear static/img: %v", err)
	}

	if _, err := os.Stat("static/img/logo.png"); os.IsNotExist(err) {
		// Contingut dummy, ServeFile no comprova que sigui PNG real
		if err := os.WriteFile("static/img/logo.png", []byte("fake-png"), 0o644); err != nil {
			t.Fatalf("no puc crear static/img/logo.png: %v", err)
		}
	} else if err != nil {
		t.Fatalf("error comprovant static/img/logo.png: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/static/img/logo.png", nil)
	rr := httptest.NewRecorder()

	core.ServeStatic(rr, req)

	resp := rr.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("esperava 200 servint logo.png, tinc %d", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "image/png") {
		t.Errorf("esperava Content-Type image/png, tinc %q", ct)
	}
}

func TestRateLimit_UsesIPWhenNoSession(t *testing.T) {
	handler := core.RateLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/alguna-ruta", nil)
	// Sense cap cookie de sessió → ha d'acabar usant IP
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("esperava 200 amb RateLimit sense sessió, tinc %d", rr.Code)
	}
}

func TestRateLimit_UsesSessionWhenAvailable(t *testing.T) {
	handler := core.RateLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/alguna-ruta", nil)
	// Afegim cookie de sessió perquè getSessionID no torni buit
	req.AddCookie(&http.Cookie{
		Name:  "cg_session",
		Value: "sessio-de-prova",
	})

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("esperava 200 amb RateLimit i sessió, tinc %d", rr.Code)
	}
}
