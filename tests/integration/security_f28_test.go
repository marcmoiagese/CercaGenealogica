package integration

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/cnf"
	"github.com/marcmoiagese/CercaGenealogica/core"
)

func mustHaveStaticFile(t *testing.T, rel string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join("static", rel)); err != nil {
		t.Skipf("static/%s no existeix, salto el test: %v", rel, err)
	}
}

func TestF28_GetRootLocalhostOK(t *testing.T) {
	old := cnf.Config
	cnf.Config = map[string]string{
		"TRUSTED_ORIGINS": "http://localhost:8080",
	}
	t.Cleanup(func() {
		cnf.Config = old
	})
	t.Setenv("ENVIRONMENT", "development")

	handler := core.SecureHeaders(core.OriginGuard(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})))

	req := httptest.NewRequest(http.MethodGet, "http://localhost:8080/", nil)
	req.Host = "localhost:8080"
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("esperava 200 per GET / amb localhost, tinc %d", rr.Code)
	}
}

func TestF28_StaticAllowsLocalHosts(t *testing.T) {
	projectRoot := findProjectRoot(t)
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatalf("no puc fer chdir a l'arrel del projecte (%s): %v", projectRoot, err)
	}

	mustHaveStaticFile(t, "css/estils.css")

	cases := []struct {
		host    string
		referer string
	}{
		{host: "localhost:8080", referer: "http://localhost:8080/"},
		{host: "127.0.0.1:8080", referer: "http://127.0.0.1:8080/"},
	}

	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodGet, "http://"+tc.host+"/static/css/estils.css", nil)
		req.Host = tc.host
		req.Header.Set("Referer", tc.referer)
		rr := httptest.NewRecorder()

		core.ServeStatic(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("esperava 200 per static amb host %s, tinc %d", tc.host, rr.Code)
		}
	}
}

func TestF28_StaticDoesNotRequireSession(t *testing.T) {
	projectRoot := findProjectRoot(t)
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatalf("no puc fer chdir a l'arrel del projecte (%s): %v", projectRoot, err)
	}

	mustHaveStaticFile(t, "css/estils.css")

	req := httptest.NewRequest(http.MethodGet, "http://localhost:8080/static/css/estils.css", nil)
	rr := httptest.NewRecorder()

	core.ServeStatic(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("esperava 200 per static sense sessió, tinc %d", rr.Code)
	}
}

func TestF28_1_StaticAllowsLoopbackRefererOnNAT(t *testing.T) {
	projectRoot := findProjectRoot(t)
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatalf("no puc fer chdir a l'arrel del projecte (%s): %v", projectRoot, err)
	}

	mustHaveStaticFile(t, "js/idioma.js")

	cases := []struct {
		name    string
		referer string
	}{
		{name: "referer-127", referer: "http://127.0.0.1:8080/"},
		{name: "referer-localhost", referer: "http://localhost:8080/"},
		{name: "referer-empty", referer: ""},
	}

	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodGet, "http://localhost:8080/static/js/idioma.js", nil)
		req.RemoteAddr = "10.0.2.2:12345"
		if tc.referer != "" {
			req.Header.Set("Referer", tc.referer)
		}
		rr := httptest.NewRecorder()

		core.ServeStatic(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("esperava 200 per static (%s) amb NAT, tinc %d", tc.name, rr.Code)
		}
	}
}

func TestF28_OriginGuardBlocksUntrustedOrigin(t *testing.T) {
	old := cnf.Config
	cnf.Config = map[string]string{
		"TRUSTED_ORIGINS": "http://localhost:8080",
	}
	t.Cleanup(func() {
		cnf.Config = old
	})

	handler := core.OriginGuard(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "http://localhost:8080/prova", nil)
	req.Header.Set("Origin", "http://evil.example")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("esperava 403 per Origin no confiat, tinc %d", rr.Code)
	}
}

func TestF28_1_OriginGuardAllowsLoopbackVariantsInDev(t *testing.T) {
	old := cnf.Config
	cnf.Config = map[string]string{
		"TRUSTED_ORIGINS": "http://localhost:8080",
	}
	t.Cleanup(func() {
		cnf.Config = old
	})
	t.Setenv("ENVIRONMENT", "development")

	handler := core.OriginGuard(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8080/prova", nil)
	req.Host = "127.0.0.1:8080"
	req.Header.Set("Origin", "http://127.0.0.1:8080")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("esperava 200 per Origin loopback en development, tinc %d", rr.Code)
	}
}

func TestF28_OriginGuardAllowsTrustedOrigin(t *testing.T) {
	old := cnf.Config
	cnf.Config = map[string]string{
		"TRUSTED_ORIGINS": "http://localhost:8080",
	}
	t.Cleanup(func() {
		cnf.Config = old
	})

	handler := core.OriginGuard(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "http://localhost:8080/prova", nil)
	req.Header.Set("Origin", "http://localhost:8080")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("esperava 200 per Origin confiat, tinc %d", rr.Code)
	}
}

func TestF28_OriginGuardBlocksMissingOrigin(t *testing.T) {
	old := cnf.Config
	cnf.Config = map[string]string{
		"TRUSTED_ORIGINS": "http://localhost:8080",
	}
	t.Cleanup(func() {
		cnf.Config = old
	})

	handler := core.OriginGuard(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "http://localhost:8080/prova", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("esperava 403 sense Origin/Referer, tinc %d", rr.Code)
	}
}

func TestF28_ForwardedHeadersRequireTrustedProxy(t *testing.T) {
	old := cnf.Config
	cnf.Config = map[string]string{
		"BLOCKED_IPS":         "203.0.113.10",
		"TRUSTED_PROXY_CIDRS": "127.0.0.1/32",
	}
	t.Cleanup(func() {
		cnf.Config = old
	})

	handler := core.BlockIPs(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "http://localhost:8080/", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("X-Forwarded-For", "203.0.113.10, 198.51.100.2")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("esperava 403 amb proxy confiat i X-Forwarded-For, tinc %d", rr.Code)
	}
}

func TestF28_1_OriginGuardUsesForwardedHostProtoWhenTrustedProxy(t *testing.T) {
	old := cnf.Config
	cnf.Config = map[string]string{
		"TRUSTED_PROXY_CIDRS": "127.0.0.1/32",
	}
	t.Cleanup(func() {
		cnf.Config = old
	})

	handler := core.OriginGuard(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "http://internal.local/prova", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("X-Forwarded-Host", "example.test:8443")
	req.Header.Set("X-Forwarded-Proto", "https")
	req.Header.Set("Origin", "https://example.test:8443")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("esperava 200 per Origin amb proxy confiat, tinc %d", rr.Code)
	}
}

func TestF28_1_OriginGuardIgnoresForwardedHostWhenUntrustedProxy(t *testing.T) {
	old := cnf.Config
	cnf.Config = map[string]string{}
	t.Cleanup(func() {
		cnf.Config = old
	})

	handler := core.OriginGuard(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "http://internal.local/prova", nil)
	req.RemoteAddr = "10.0.0.5:5555"
	req.Host = "internal.local"
	req.Header.Set("X-Forwarded-Host", "example.test:8443")
	req.Header.Set("X-Forwarded-Proto", "https")
	req.Header.Set("Origin", "https://example.test:8443")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("esperava 403 per forwarded host no confiat, tinc %d", rr.Code)
	}
}

func TestF28_ForwardedHeadersIgnoredWithoutTrustedProxy(t *testing.T) {
	old := cnf.Config
	cnf.Config = map[string]string{
		"BLOCKED_IPS":         "203.0.113.10",
		"TRUSTED_PROXY_CIDRS": "127.0.0.1/32",
	}
	t.Cleanup(func() {
		cnf.Config = old
	})

	handler := core.BlockIPs(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "http://localhost:8080/", nil)
	req.RemoteAddr = "10.0.0.5:5555"
	req.Header.Set("X-Forwarded-For", "203.0.113.10")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("esperava 200 quan el proxy no és confiat, tinc %d", rr.Code)
	}
}

func TestF28_RateLimitStaticBurst(t *testing.T) {
	handler := core.RateLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for i := 0; i < 25; i++ {
		req := httptest.NewRequest(http.MethodGet, "http://localhost:8080/static/css/estils.css", nil)
		req.RemoteAddr = "203.0.113.55:12345"
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("esperava 200 per /static/ dins burst, iter %d, tinc %d", i, rr.Code)
		}
	}
}

func TestF28_NoHardcodedLocalhostAssets(t *testing.T) {
	projectRoot := findProjectRoot(t)
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatalf("no puc fer chdir a l'arrel del projecte (%s): %v", projectRoot, err)
	}
	loadTemplatesForTests(t, projectRoot)

	req := httptest.NewRequest(http.MethodGet, "http://localhost:8080/", nil)
	rr := httptest.NewRecorder()

	core.RenderTemplate(rr, req, "index.html", map[string]interface{}{})
	body := rr.Body.String()

	if strings.Contains(body, "http://localhost:8080/static/") || strings.Contains(body, "http://127.0.0.1:8080/static/") {
		t.Fatalf("no s'haurien d'injectar URLs absolutes de localhost/127.0.0.1 als assets")
	}
}
