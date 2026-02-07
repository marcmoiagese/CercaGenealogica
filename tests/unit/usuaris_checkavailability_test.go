package unit

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type availabilityResponse struct {
	UsernameTaken bool `json:"usernameTaken"`
	EmailTaken    bool `json:"emailTaken"`
}

func TestCheckAvailability_HappyPath(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	// 1) Usuari existent a la BB.DD.
	u := &db.User{
		Usuari:   "pep",
		Name:     "Pep",
		Surname:  "Garcia",
		Email:    "pep@example.com",
		Password: []byte("dummy-hash"),
		Active:   true,
	}
	if err := app.DB.InsertUser(u); err != nil {
		t.Fatalf("no puc inserir usuari de prova: %v", err)
	}

	// 2) POST amb form i CSRF vàlid (cookie + header)
	form := url.Values{}
	form.Set("username", "pep")
	form.Set("email", "pep@example.com")

	req := httptest.NewRequest(http.MethodPost, "/check-availability", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	csrfToken := "test-csrf-token"

	// token via header (ramal AJAX)
	req.Header.Set("X-CSRF-Token", csrfToken)

	// cookie amb el NOM CORRECTE: "cg_csrf"
	req.AddCookie(&http.Cookie{
		Name:  "cg_csrf",
		Value: csrfToken,
	})

	rr := httptest.NewRecorder()
	http.HandlerFunc(app.CheckAvailability).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("esperava 200 a CheckAvailability, tinc %d", rr.Code)
	}

	var resp availabilityResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("no puc parsejar JSON de resposta: %v", err)
	}

	if !resp.UsernameTaken {
		t.Errorf("esperava UsernameTaken=true per usuari existent")
	}
	if !resp.EmailTaken {
		t.Errorf("esperava EmailTaken=true per email existent")
	}
}

func TestCheckAvailability_UsernameTaken(t *testing.T) {
	email := "u@example.com"
	users := map[string]*db.User{
		email: {
			Email:  email,
			Usuari: "marc",
		},
	}
	app := newFakeAppWithUsers(t, users) // fa servir la fakeDB dels tests de RegenerarToken

	form := url.Values{}
	form.Set("username", "marc")
	form.Set("email", "nou@example.com")

	csrfToken := "test-csrf-token"

	req := httptest.NewRequest(http.MethodPost, "/check-availability", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-CSRF-Token", csrfToken)
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrfToken})

	rr := httptest.NewRecorder()
	http.HandlerFunc(app.CheckAvailability).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("esperava 200, tinc %d", rr.Code)
	}

	var resp availabilityResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("no puc parsejar JSON: %v", err)
	}

	if !resp.UsernameTaken {
		t.Errorf("esperava UsernameTaken=true per usuari existent")
	}
	// ens és igual EmailTaken aquí, pot ser true o false segons la lògica
}

func TestCheckAvailability_EmailFree(t *testing.T) {
	email := "u@example.com"
	users := map[string]*db.User{
		email: {
			Email:  email,
			Usuari: "marc",
		},
	}
	app := newFakeAppWithUsers(t, users)

	form := url.Values{}
	form.Set("username", "nou")          // nou usuari
	form.Set("email", "nou@example.com") // nou email

	csrfToken := "test-csrf-token"

	req := httptest.NewRequest(http.MethodPost, "/check-availability", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-CSRF-Token", csrfToken)
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrfToken})

	rr := httptest.NewRecorder()
	http.HandlerFunc(app.CheckAvailability).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("esperava 200, tinc %d", rr.Code)
	}

	var resp availabilityResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("no puc parsejar JSON: %v", err)
	}

	if resp.UsernameTaken {
		t.Errorf("no esperava UsernameTaken=true per usuari nou")
	}
	if resp.EmailTaken {
		t.Errorf("no esperava EmailTaken=true per email nou")
	}
}
