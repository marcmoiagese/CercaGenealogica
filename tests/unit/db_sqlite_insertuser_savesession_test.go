package unit

import (
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestSQLiteInsertUser_DuplicateUser(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	u1 := &db.User{
		Usuari:   "dupuser",
		Name:     "Prim",
		Surname:  "Usuari",
		Email:    "dup@example.com",
		Password: []byte("hash"),
		Active:   true,
	}

	if err := app.DB.InsertUser(u1); err != nil {
		t.Fatalf("primer InsertUser hauria de funcionar, error: %v", err)
	}

	u2 := &db.User{
		// mateix usuari → viola UNIQUE(usuari)
		Usuari:   "dupuser",
		Name:     "Segon",
		Surname:  "Usuari",
		Email:    "alt@example.com",
		Password: []byte("hash2"),
		Active:   true,
	}

	if err := app.DB.InsertUser(u2); err == nil {
		t.Fatalf("esperava error per usuari duplicat, però és nil")
	}
}

func TestSQLiteSaveSession_DuplicateToken(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	u := &db.User{
		Usuari:   "sessuser",
		Name:     "Sessio",
		Surname:  "Usuari",
		Email:    "sess@example.com",
		Password: []byte("hash"),
		Active:   true,
	}

	if err := app.DB.InsertUser(u); err != nil {
		t.Fatalf("no puc inserir usuari de prova: %v", err)
	}

	token := "sess-token-1"
	expiry := "2030-01-01T00:00:00Z"

	if err := app.DB.SaveSession(token, u.ID, expiry); err != nil {
		t.Fatalf("primer SaveSession hauria de funcionar, error: %v", err)
	}

	// segon cop amb el mateix token_hash → viola UNIQUE(token_hash)
	if err := app.DB.SaveSession(token, u.ID, expiry); err == nil {
		t.Fatalf("esperava error per token de sessió duplicat, però és nil")
	}
}
