package unit

import (
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
	"golang.org/x/crypto/bcrypt"
)

// crea un usuari de prova i el desa a la BD utilitzant d.InsertUser.
// Retorna l'usuari (amb ID emplenat) i la contrasenya en clar.
func createTestUserForSessions(t *testing.T, d db.DB) (*db.User, string) {
	t.Helper()

	rawPassword := "SessioProva123!"
	hashed, err := bcrypt.GenerateFromPassword([]byte(rawPassword), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("no s'ha pogut generar hash de contrasenya: %v", err)
	}

	u := &db.User{
		Usuari:        "sessio_test",
		Name:          "Sessio",
		Surname:       "Prova",
		Email:         "sessio.test@example.com",
		Password:      hashed,
		DataNaixament: "1990-01-01",
		Pais:          "ES",
		Estat:         "CAT",
		Provincia:     "Lleida",
		Poblacio:      "Linyola",
		CodiPostal:    "25240",
		Active:        true, // per a sessions, ja el considerem actiu
	}

	if err := d.InsertUser(u); err != nil {
		t.Fatalf("InsertUser (sessions) ha fallat: %v", err)
	}
	if u.ID == 0 {
		t.Fatalf("després d'InsertUser, l'ID hauria de ser > 0, però és %d", u.ID)
	}

	return u, rawPassword
}

// TestSessionSaveAndGet comprova que desar una sessió i recuperar-la
// retorna l'usuari correcte mentre la sessió no està revocada.
func TestSessionSaveAndGet(t *testing.T) {
	d := newTestSQLiteDB(t) // helper definit a db_users_lifecycle_test.go
	defer d.Close()

	user, _ := createTestUserForSessions(t, d)

	const sessionID = "sessio-token-123"

	if err := d.SaveSession(sessionID, user.ID, ""); err != nil {
		t.Fatalf("SaveSession ha fallat: %v", err)
	}

	uFromSession, err := d.GetSessionUser(sessionID)
	if err != nil {
		t.Fatalf("GetSessionUser ha fallat: %v", err)
	}

	if uFromSession.ID != user.ID {
		t.Errorf("ID d'usuari des de sessió = %d, esperava %d", uFromSession.ID, user.ID)
	}
	if uFromSession.Usuari != user.Usuari {
		t.Errorf("Usuari des de sessió = %q, esperava %q", uFromSession.Usuari, user.Usuari)
	}
}

// TestSessionDeleteRevokesAccess comprova que DeleteSession marca la sessió
// com a revocada i que GetSessionUser deixa de trobar-la.
func TestSessionDeleteRevokesAccess(t *testing.T) {
	d := newTestSQLiteDB(t)
	defer d.Close()

	user, _ := createTestUserForSessions(t, d)

	const sessionID = "sessio-token-456"

	if err := d.SaveSession(sessionID, user.ID, ""); err != nil {
		t.Fatalf("SaveSession ha fallat: %v", err)
	}

	// Primer comprovem que la sessió funciona
	if _, err := d.GetSessionUser(sessionID); err != nil {
		t.Fatalf("GetSessionUser abans de DeleteSession ha fallat: %v", err)
	}

	// Ara revoquem la sessió
	if err := d.DeleteSession(sessionID); err != nil {
		t.Fatalf("DeleteSession ha fallat: %v", err)
	}

	// I ara ja no s'hauria de poder recuperar
	if _, err := d.GetSessionUser(sessionID); err == nil {
		t.Fatalf("GetSessionUser després de DeleteSession hauria de fallar, però no retorna error")
	}
}
