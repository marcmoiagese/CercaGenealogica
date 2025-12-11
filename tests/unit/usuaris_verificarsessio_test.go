package unit

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

// fakeDBVerificar implementa db.DB però només fa servir GetSessionUser
// per aquests tests. La resta de mètodes tornen un error "not implemented".
type fakeDBVerificar struct {
	sessions map[string]*db.User
}

// --- Implementació de la interfície db.DB ---

func (f *fakeDBVerificar) Connect() error { return nil }
func (f *fakeDBVerificar) Close()         {}

func (f *fakeDBVerificar) Exec(query string, args ...interface{}) (int64, error) {
	return 0, errors.New("Exec not implemented in fakeDBVerificar")
}

func (f *fakeDBVerificar) Query(query string, args ...interface{}) ([]map[string]interface{}, error) {
	return nil, errors.New("Query not implemented in fakeDBVerificar")
}

func (f *fakeDBVerificar) InsertUser(user *db.User) error {
	return errors.New("InsertUser not implemented in fakeDBVerificar")
}

func (f *fakeDBVerificar) SaveActivationToken(email, token string) error {
	return errors.New("SaveActivationToken not implemented in fakeDBVerificar")
}

func (f *fakeDBVerificar) GetUserByEmail(email string) (*db.User, error) {
	return nil, errors.New("GetUserByEmail not implemented in fakeDBVerificar")
}

func (f *fakeDBVerificar) ExistsUserByUsername(username string) (bool, error) {
	return false, errors.New("ExistsUserByUsername not implemented in fakeDBVerificar")
}

func (f *fakeDBVerificar) ExistsUserByEmail(email string) (bool, error) {
	return false, errors.New("ExistsUserByEmail not implemented in fakeDBVerificar")
}

func (f *fakeDBVerificar) ActivateUser(token string) error {
	return errors.New("ActivateUser not implemented in fakeDBVerificar")
}

func (f *fakeDBVerificar) AuthenticateUser(usernameOrEmail, password string) (*db.User, error) {
	return nil, errors.New("AuthenticateUser not implemented in fakeDBVerificar")
}

func (f *fakeDBVerificar) SaveSession(sessionID string, userID int, expiry string) error {
	return errors.New("SaveSession not implemented in fakeDBVerificar")
}

// Aquesta és l'única funció que ens importa per aquests tests
func (f *fakeDBVerificar) GetSessionUser(sessionID string) (*db.User, error) {
	u, ok := f.sessions[sessionID]
	if !ok {
		return nil, errors.New("session not found")
	}
	return u, nil
}

func (f *fakeDBVerificar) DeleteSession(sessionID string) error {
	return errors.New("DeleteSession not implemented in fakeDBVerificar")
}
func (f *fakeDBVerificar) CreatePasswordReset(email, token, expiry, lang string) (bool, error) {
	return false, errors.New("CreatePasswordReset not implemented in fakeDBVerificar")
}
func (f *fakeDBVerificar) GetPasswordReset(token string) (*db.PasswordReset, error) {
	return nil, errors.New("GetPasswordReset not implemented in fakeDBVerificar")
}
func (f *fakeDBVerificar) MarkPasswordResetUsed(id int) error {
	return errors.New("MarkPasswordResetUsed not implemented in fakeDBVerificar")
}
func (f *fakeDBVerificar) UpdateUserPassword(userID int, passwordHash []byte) error {
	return errors.New("UpdateUserPassword not implemented in fakeDBVerificar")
}

// Crea una App només amb la fake DB per testejar VerificarSessio.
func newAppVerificar(dbFake db.DB) *core.App {
	// FIX: aquí només hi ha Config i DB, res de Templates
	return &core.App{
		Config: map[string]string{},
		DB:     dbFake,
	}
}

// Sense cap cookie de sessió → ha de tornar (nil, false)
func TestVerificarSessio_NoCookie_ReturnsFalse(t *testing.T) {
	fake := &fakeDBVerificar{sessions: map[string]*db.User{}}
	app := newAppVerificar(fake)

	req := httptest.NewRequest(http.MethodGet, "/", nil)

	user, ok := app.VerificarSessio(req)
	if ok {
		t.Fatalf("esperava ok=false sense cookie de sessió, tinc true")
	}
	if user != nil {
		t.Fatalf("esperava user=nil sense cookie de sessió, tinc %#v", user)
	}
}

// Cookie present però buida → també (nil, false)
func TestVerificarSessio_EmptyCookie_ReturnsFalse(t *testing.T) {
	fake := &fakeDBVerificar{sessions: map[string]*db.User{}}
	app := newAppVerificar(fake)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{
		Name:  "cg_session",
		Value: "",
	})

	user, ok := app.VerificarSessio(req)
	if ok {
		t.Fatalf("esperava ok=false amb cookie buida, tinc true")
	}
	if user != nil {
		t.Fatalf("esperava user=nil amb cookie buida, tinc %#v", user)
	}
}

// Cookie amb ID de sessió inexistent → (nil, false)
func TestVerificarSessio_InvalidSession_ReturnsFalse(t *testing.T) {
	fake := &fakeDBVerificar{sessions: map[string]*db.User{}}
	app := newAppVerificar(fake)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{
		Name:  "cg_session",
		Value: "sessio-inexistent",
	})

	user, ok := app.VerificarSessio(req)
	if ok {
		t.Fatalf("esperava ok=false amb sessió inexistent, tinc true")
	}
	if user != nil {
		t.Fatalf("esperava user=nil amb sessió inexistent, tinc %#v", user)
	}
}

// Sessió vàlida → ha de retornar l'usuari i ok=true
func TestVerificarSessio_ValidSession_ReturnsUser(t *testing.T) {
	const sessionID = "sessio-de-prova"

	expectedUser := &db.User{
		ID:      123,
		Usuari:  "testuser",
		Name:    "Test",
		Surname: "User",
		Email:   "test@example.com",
	}

	fake := &fakeDBVerificar{
		sessions: map[string]*db.User{
			sessionID: expectedUser,
		},
	}
	app := newAppVerificar(fake)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{
		Name:  "cg_session",
		Value: sessionID,
	})

	user, ok := app.VerificarSessio(req)
	if !ok {
		t.Fatalf("esperava ok=true amb sessió vàlida, tinc false")
	}
	if user == nil {
		t.Fatalf("esperava user != nil amb sessió vàlida")
	}
	if user.ID != expectedUser.ID {
		t.Fatalf("esperava ID usuari %d, tinc %d", expectedUser.ID, user.ID)
	}
	if user.Usuari != expectedUser.Usuari {
		t.Fatalf("esperava Usuari=%q, tinc %q", expectedUser.Usuari, user.Usuari)
	}
}
