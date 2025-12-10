package unit

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/cnf"
	"github.com/marcmoiagese/CercaGenealogica/core"
)

func TestBlockIPs_UsesForwardedIP(t *testing.T) {
	old := cnf.Config
	cnf.Config = map[string]string{
		"BLOCKED_IPS": "1.2.3.4",
	}
	defer func() { cnf.Config = old }()

	handler := core.BlockIPs(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Forwarded-For", "1.2.3.4, 5.6.7.8")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("esperava 403 per IP bloquejada, tinc %d", rr.Code)
	}
}

func TestBlockIPs_UsesRemoteAddr(t *testing.T) {
	old := cnf.Config
	cnf.Config = map[string]string{
		"BLOCKED_IPS": "10.0.0.1",
	}
	defer func() { cnf.Config = old }()

	handler := core.BlockIPs(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.0.0.1:12345"

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("esperava 403 per RemoteAddr bloquejada, tinc %d", rr.Code)
	}
}

func TestBlockIPs_UsesXRealIP(t *testing.T) {
	old := cnf.Config
	cnf.Config = map[string]string{
		"BLOCKED_IPS": "2.3.4.5",
	}
	defer func() { cnf.Config = old }()

	handler := core.BlockIPs(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Real-IP", "2.3.4.5")

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("esperava 403 per X-Real-IP bloquejada, tinc %d", rr.Code)
	}
}
