package integration

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
	"golang.org/x/crypto/bcrypt"
)

// handlerSimulatInici simula una ruta /inici protegida.
//
// - Si NO hi ha cookie cg_session → redirigeix a / (o /login).
// - Si hi ha cookie cg_session → retorna 200 OK.
func handlerSimulatInici(w http.ResponseWriter, r *http.Request) {
	c, err := r.Cookie("cg_session")
	if err != nil || c.Value == "" {
		// Sense sessió o cookie buida → redirigim
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}

	// Amb sessió → OK
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("INICI OK"))
}

// TestIniciSenseSessio comprova que, sense cookie de sessió,
// l'accés a /inici NO retorna 200 sinó una redirecció.
func TestIniciSenseSessio(t *testing.T) {
	// Només per assegurar entorn (templates carregades, etc.)
	app, _ := newTestAppForLogin(t, "test_inici_sense_sessio.sqlite3")
	_ = app // no el fem servir directament aquí, però manté el patró

	req := httptest.NewRequest(http.MethodGet, "/inici", nil)
	req.RemoteAddr = "127.0.0.1:70001"

	rr := httptest.NewRecorder()
	handlerSimulatInici(rr, req)

	res := rr.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusSeeOther && res.StatusCode != http.StatusFound {
		t.Fatalf("esperava redirecció per falta de sessió (302/303), però tinc %d. Cos:\n%s",
			res.StatusCode, rr.Body.String())
	}

	loc := res.Header.Get("Location")
	if loc == "" {
		t.Fatalf("s'esperava header Location en la redirecció, però és buit (status=%d)", res.StatusCode)
	}
}

// TestIniciAmbSessio comprova que, després d’un login correcte
// (que crea cookie cg_session), l’accés a /inici retorna 200 OK.
func TestIniciAmbSessio(t *testing.T) {
	app, dbInstance := newTestAppForLogin(t, "test_inici_amb_sessio.sqlite3")

	// 1. Creem un usuari actiu amb contrasenya hashejada
	rawPassword := "P4ssword!"
	hash, err := bcrypt.GenerateFromPassword([]byte(rawPassword), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("no puc generar hash de contrasenya: %v", err)
	}

	email := "inici.sessio@example.com"
	user := &db.User{
		Usuari:        "inici_ok",
		Name:          "Test",
		Surname:       "Inici",
		Email:         email,
		Password:      hash,
		DataNaixament: "1990-01-01",
		Active:        true,
		CreatedAt:     time.Now().Format(time.RFC3339),
	}

	if err := dbInstance.InsertUser(user); err != nil {
		t.Fatalf("InsertUser ha fallat: %v", err)
	}

	// 2. Fem login per obtenir una cookie cg_session vàlida
	csrfToken := "csrf-inici-login"

	form := url.Values{}
	form.Set("csrf_token", csrfToken)
	form.Set("usuari", "inici_ok")
	form.Set("contrassenya", rawPassword)
	form.Set("captcha", "8")
	form.Set("mantenir_sessio", "on")

	body := strings.NewReader(form.Encode())

	loginReq := httptest.NewRequest(http.MethodPost, "/login", body)
	loginReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	loginReq.Header.Set("Accept-Language", "ca")
	loginReq.AddCookie(&http.Cookie{
		Name:  "cg_csrf",
		Value: csrfToken,
	})
	loginReq.RemoteAddr = "127.0.0.1:70002"

	loginRR := httptest.NewRecorder()
	app.IniciarSessio(loginRR, loginReq)

	loginRes := loginRR.Result()
	defer loginRes.Body.Close()

	if loginRes.StatusCode != http.StatusSeeOther {
		t.Fatalf("esperava 303 SeeOther després de login correcte, però tinc %d. Cos:\n%s",
			loginRes.StatusCode, loginRR.Body.String())
	}

	// Extreiem la cookie de sessió
	var sessionCookie *http.Cookie
	for _, c := range loginRes.Cookies() {
		if c.Name == "cg_session" {
			sessionCookie = c
			break
		}
	}
	if sessionCookie == nil || sessionCookie.Value == "" {
		t.Fatalf("després d'un login correcte, s'esperava cookie cg_session no buida")
	}

	// 3. Accedim a /inici amb aquesta cookie cg_session
	iniciReq := httptest.NewRequest(http.MethodGet, "/inici", nil)
	iniciReq.RemoteAddr = "127.0.0.1:70003"
	iniciReq.AddCookie(sessionCookie)

	iniciRR := httptest.NewRecorder()
	handlerSimulatInici(iniciRR, iniciReq)

	iniciRes := iniciRR.Result()
	defer iniciRes.Body.Close()

	if iniciRes.StatusCode != http.StatusOK {
		t.Fatalf("esperava 200 OK a /inici amb sessió vàlida, però tinc %d. Cos:\n%s",
			iniciRes.StatusCode, iniciRR.Body.String())
	}
}
