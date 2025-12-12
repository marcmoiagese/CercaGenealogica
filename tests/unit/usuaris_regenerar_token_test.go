package unit

import (
	"fmt"
	"html/template"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

// MostrarFormulariRegenerarToken hauria de renderitzar la pàgina sense petar.
func TestMostrarFormulariRegenerarToken_OK(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	// Sobreescrivim les plantilles globals perquè "regenerar-token.html" existeixi
	oldTpl := core.Templates
	defer func() { core.Templates = oldTpl }()

	const tpl = `{{ define "regenerar-token.html" }}OK regenerar token{{ end }}`
	core.Templates = template.Must(template.New("regenerar-token.html").Parse(tpl))

	req := httptest.NewRequest(http.MethodGet, "/regenerar-token", nil)
	rr := httptest.NewRecorder()

	http.HandlerFunc(app.MostrarFormulariRegenerarToken).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("esperava 200 a MostrarFormulariRegenerarToken, tinc %d", rr.Code)
	}
}

// ProcessarRegenerarToken amb GET ha de redirigir al formulari.
func TestProcessarRegenerarToken_GetRedirects(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	req := httptest.NewRequest(http.MethodGet, "/regenerar-token", nil)
	rr := httptest.NewRecorder()

	http.HandlerFunc(app.ProcessarRegenerarToken).ServeHTTP(rr, req)

	resp := rr.Result()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("esperava redirect (303/303), tinc %d", resp.StatusCode)
	}
	loc := resp.Header.Get("Location")
	if loc != "/regenerar-token" {
		t.Errorf("esperava Location=/regenerar-token, tinc %q", loc)
	}
}

// ProcessarRegenerarToken amb POST però sense CSRF ha de donar Forbidden (entra a RegenerarTokenActivacio).
func TestProcessarRegenerarToken_PostInvalidCSRF(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	req := httptest.NewRequest(http.MethodPost, "/regenerar-token?email=test@example.com", nil)
	rr := httptest.NewRecorder()

	http.HandlerFunc(app.ProcessarRegenerarToken).ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("esperava 403 per CSRF invàlid, tinc %d", rr.Code)
	}
}

// CheckAvailability amb mètode incorrecte (GET) ha de retornar 405.
func TestCheckAvailability_InvalidMethod(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	req := httptest.NewRequest(http.MethodGet, "/check-availability", nil)
	rr := httptest.NewRecorder()

	http.HandlerFunc(app.CheckAvailability).ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("esperava 405 per mètode no permès, tinc %d", rr.Code)
	}
}

func TestRegenerarTokenActivacio_InvalidCSRF(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	req := httptest.NewRequest(http.MethodPost, "/regenerar-token?email=test@example.com", nil)
	// Sense cookie ni camp csrf_token
	rr := httptest.NewRecorder()

	http.HandlerFunc(app.RegenerarTokenActivacio).ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("esperava 403 per CSRF invàlid, tinc %d", rr.Code)
	}
}

func TestRegenerarTokenActivacio_MissingEmail(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	form := url.Values{}
	form.Set("csrf_token", "tok")
	req := httptest.NewRequest(http.MethodPost, "/regenerar-token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: "tok"})

	rr := httptest.NewRecorder()
	http.HandlerFunc(app.RegenerarTokenActivacio).ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("esperava 400 per email buit, tinc %d", rr.Code)
	}
}

func TestRegenerarTokenActivacio_UserNotFound(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	email := "noexisteix@example.com"
	form := url.Values{}
	form.Set("csrf_token", "tok")
	url := "/regenerar-token?email=" + url.QueryEscape(email)

	req := httptest.NewRequest(http.MethodPost, url, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: "tok"})

	rr := httptest.NewRecorder()
	http.HandlerFunc(app.RegenerarTokenActivacio).ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("esperava 404 per usuari no trobat, tinc %d", rr.Code)
	}
}

func TestRegenerarTokenActivacio_UserAlreadyActive(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	email := "actiu@example.com"

	sqlite, ok := app.DB.(*db.SQLite)
	if !ok {
		t.Fatalf("DB no és SQLite en test")
	}

	user := &db.User{
		Usuari:        "actiu",
		Name:          "Usuari",
		Surname:       "Actiu",
		Email:         email,
		Password:      []byte("dummy"),
		DataNaixament: "1990-01-01",
		Active:        true,
	}
	if err := sqlite.InsertUser(user); err != nil {
		t.Fatalf("InsertUser ha fallat: %v", err)
	}

	form := url.Values{}
	form.Set("csrf_token", "tok")
	url := "/regenerar-token?email=" + url.QueryEscape(email)

	req := httptest.NewRequest(http.MethodPost, url, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: "tok"})

	rr := httptest.NewRecorder()
	http.HandlerFunc(app.RegenerarTokenActivacio).ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("esperava 400 per usuari ja actiu, tinc %d", rr.Code)
	}
}

// fakeDB implementa db.DB però només el que necessitem per a RegenerarTokenActivacio.
type fakeDB struct {
	users map[string]*db.User
}

func (f *fakeDB) Connect() error                                    { return nil }
func (f *fakeDB) Close()                                            {}
func (f *fakeDB) Exec(q string, args ...interface{}) (int64, error) { return 0, nil }
func (f *fakeDB) Query(q string, args ...interface{}) ([]map[string]interface{}, error) {
	return nil, nil
}

func (f *fakeDB) InsertUser(u *db.User) error {
	if f.users == nil {
		f.users = make(map[string]*db.User)
	}
	f.users[u.Email] = u
	return nil
}

func (f *fakeDB) SaveActivationToken(email, token string) error {
	// Per aquests tests no cal guardar res realment
	return nil
}

func (f *fakeDB) GetUserByEmail(email string) (*db.User, error) {
	if f.users == nil {
		return nil, fmt.Errorf("not found")
	}
	u, ok := f.users[email]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return u, nil
}

func (f *fakeDB) ExistsUserByUsername(username string) (bool, error) {
	if f.users == nil {
		return false, nil
	}
	for _, u := range f.users {
		if u.Usuari == username {
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeDB) ExistsUserByEmail(email string) (bool, error) {
	if f.users == nil {
		return false, nil
	}
	_, ok := f.users[email]
	return ok, nil
}

func (f *fakeDB) ActivateUser(token string) error { return nil }

func (f *fakeDB) AuthenticateUser(usernameOrEmail, password string) (*db.User, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *fakeDB) SaveSession(sessionID string, userID int, expiry string) error { return nil }
func (f *fakeDB) GetSessionUser(sessionID string) (*db.User, error) {
	return nil, fmt.Errorf("not found")
}
func (f *fakeDB) DeleteSession(sessionID string) error { return nil }
func (f *fakeDB) CreatePasswordReset(email, token, expiry, lang string) (bool, error) {
	return false, nil
}
func (f *fakeDB) GetPasswordReset(token string) (*db.PasswordReset, error) { return nil, fmt.Errorf("not implemented") }
func (f *fakeDB) MarkPasswordResetUsed(id int) error                      { return nil }
func (f *fakeDB) UpdateUserPassword(userID int, passwordHash []byte) error {
	return nil
}
func (f *fakeDB) CreatePrivacyDefaults(userID int) error { return nil }
func (f *fakeDB) GetPrivacySettings(userID int) (*db.PrivacySettings, error) {
	return nil, fmt.Errorf("not implemented")
}
func (f *fakeDB) SavePrivacySettings(userID int, p *db.PrivacySettings) error { return nil }
func (f *fakeDB) UpdateUserProfile(u *db.User) error                         { return nil }
func (f *fakeDB) UpdateUserEmail(userID int, newEmail string) error          { return nil }
func (f *fakeDB) CreateEmailChange(userID int, newEmail, tokenConfirm, expConfirm, tokenRevert, expRevert, lang string) error {
	return nil
}
func (f *fakeDB) ConfirmEmailChange(token string) (*db.EmailChange, error) { return nil, fmt.Errorf("not implemented") }
func (f *fakeDB) RevertEmailChange(token string) (*db.EmailChange, error)  { return nil, fmt.Errorf("not implemented") }
func (f *fakeDB) markEmailChangeConfirmed(id int) error                    { return nil }
func (f *fakeDB) markEmailChangeReverted(id int) error                     { return nil }

// Helper per crear una App amb fakeDB
func newFakeAppWithUsers(users map[string]*db.User) *core.App {
	fdb := &fakeDB{users: users}
	// Per aquests tests, la config ens és igual
	return core.NewApp(map[string]string{}, fdb)
}

func TestRegenerarTokenActivacio_Success(t *testing.T) {
	email := "nou@example.com"
	users := map[string]*db.User{
		email: {
			Email:  email,
			Active: false, // molt important: inactiu
		},
	}
	app := newFakeAppWithUsers(users)

	form := url.Values{}
	form.Set("csrf_token", "tok")
	urlStr := "/regenerar-token?email=" + url.QueryEscape(email)

	req := httptest.NewRequest(http.MethodPost, urlStr, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: "tok"})

	rr := httptest.NewRecorder()

	http.HandlerFunc(app.RegenerarTokenActivacio).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK && rr.Code != http.StatusFound {
		t.Fatalf("esperava 200 o 302 en el cas bo, tinc %d", rr.Code)
	}

	// Opcional: comprovar que a la resposta hi ha algun missatge de confirmació
	body := rr.Body.String()
	if !strings.Contains(body, "token") && !strings.Contains(body, "activació") {
		t.Logf("cos de resposta: %s", body)
	}
}
