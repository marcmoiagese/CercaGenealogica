package integration

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/cnf"
	"github.com/marcmoiagese/CercaGenealogica/core"
)

//
// 1. BlockIPs – IPs bloquejades i permeses
//

func TestBlockIPsDenied(t *testing.T) {
	// Ens assegurem que el mapa de config existeix
	if cnf.Config == nil {
		cnf.Config = map[string]string{}
	}

	old := cnf.Config["BLOCKED_IPS"]
	cnf.Config["BLOCKED_IPS"] = "1.2.3.4"
	t.Cleanup(func() {
		cnf.Config["BLOCKED_IPS"] = old
	})

	// Handler “innocent” que només fa 200
	baseHandler := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}

	handler := core.BlockIPs(baseHandler)

	req := httptest.NewRequest(http.MethodGet, "/login", nil)
	req.RemoteAddr = "1.2.3.4:12345" // IP bloquejada

	rr := httptest.NewRecorder()
	handler(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("s'esperava 403 per IP bloquejada, rebut: %d", rr.Code)
	}
}

func TestBlockIPsAllowed(t *testing.T) {
	if cnf.Config == nil {
		cnf.Config = map[string]string{}
	}

	old := cnf.Config["BLOCKED_IPS"]
	cnf.Config["BLOCKED_IPS"] = "1.2.3.4"
	t.Cleanup(func() {
		cnf.Config["BLOCKED_IPS"] = old
	})

	baseHandler := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}

	handler := core.BlockIPs(baseHandler)

	req := httptest.NewRequest(http.MethodGet, "/login", nil)
	req.RemoteAddr = "5.6.7.8:9999" // IP no bloquejada

	rr := httptest.NewRecorder()
	handler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("s'esperava 200 per IP permesa, rebut: %d", rr.Code)
	}
}

//
// 2. RateLimit – límit de peticions per IP a /login
//

func TestRateLimitLogin(t *testing.T) {
	// Handler base que sempre torna 200
	baseHandler := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}

	handler := core.RateLimit(baseHandler)

	const ip = "9.9.9.9:12345"

	// Per /login tenim rate=5, burst=10 → les 10 primeres haurien de passar,
	// a partir de la 11a (índex 10) hauria de començar a retornar 429.
	for i := 0; i < 15; i++ {
		req := httptest.NewRequest(http.MethodPost, "/login", nil)
		req.RemoteAddr = ip

		rr := httptest.NewRecorder()
		handler(rr, req)

		if i < 10 {
			if rr.Code == http.StatusTooManyRequests {
				t.Fatalf("no s'esperava 429 abans de la 11a petició (iter %d), rebut: %d", i, rr.Code)
			}
		} else {
			if rr.Code != http.StatusTooManyRequests {
				t.Fatalf("s'esperava 429 a partir de la 11a petició (iter %d), rebut: %d", i, rr.Code)
			}
		}
	}
}

//
// 3. CSRF – registre sense cookie / cookie que no quadra
//

// helper per construir un formulari de registre coherent
func buildValidRegisterForm() url.Values {
	form := url.Values{}
	form.Set("nom", "Prova")
	form.Set("cognoms", "Usuari")
	form.Set("email", "csrf@example.com")
	form.Set("contrassenya", "contrasenyaSegura123")
	form.Set("confirmar_contrasenya", "contrasenyaSegura123")
	form.Set("captcha", "8")             // correcte
	form.Set("usuari", "csrfuser")       // username
	form.Set("accepta_condicions", "on") // accepta condicions
	return form
}

func TestRegistreCSRFWithoutCookie(t *testing.T) {
	// Truquem directament al mètode de registre.
	// IMPORTANT: en el codi actual, la primera cosa que fa és validar CSRF
	// i, si falla, retorna 403 ABANS de tocar a.DB. Per això podem fer servir
	// un *core.App nil (no es desreferencia en el camí de CSRF KO).
	var app *core.App

	form := buildValidRegisterForm()
	form.Set("csrf_token", "token-formulari") // però sense cookie → mismatch

	body := strings.NewReader(form.Encode())
	req := httptest.NewRequest(http.MethodPost, "/registre", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.RemoteAddr = "203.0.113.10:54321"

	rr := httptest.NewRecorder()
	app.RegistrarUsuari(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("s'esperava 403 per registre sense cookie CSRF, rebut: %d", rr.Code)
	}
}

func TestRegistreCSRFMismatch(t *testing.T) {
	var app *core.App

	form := buildValidRegisterForm()
	form.Set("csrf_token", "token-formulari")

	body := strings.NewReader(form.Encode())
	req := httptest.NewRequest(http.MethodPost, "/registre", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.RemoteAddr = "203.0.113.11:54321"

	// Cookie CG_CSRF amb un valor diferent del csrf_token del formulari
	req.AddCookie(&http.Cookie{
		Name:  "cg_csrf",
		Value: "token-cookie-diferent",
		Path:  "/",
	})

	rr := httptest.NewRecorder()
	app.RegistrarUsuari(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("s'esperava 403 per registre amb CSRF mismatch, rebut: %d", rr.Code)
	}
}
