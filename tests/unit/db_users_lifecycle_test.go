package unit

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
	"golang.org/x/crypto/bcrypt"
)

// newTestSQLiteDB crea una BD SQLite temporal amb l'esquema de SQLite.sql
// utilitzant el mateix codi que fa servir l'aplicació (db.NewDB).
func newTestSQLiteDB(t *testing.T) db.DB {
	t.Helper()

	// Ens assegurem que el directori de treball és l'arrel del projecte,
	// perquè db.NewDB buscarà "db/SQLite.sql" de forma relativa.
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("no puc obtenir el path del fitxer de test (runtime.Caller)")
	}

	// thisFile = .../tests/unit/db_users_lifecycle_test.go
	rootDir := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	if err := os.Chdir(rootDir); err != nil {
		t.Fatalf("no puc fer chdir al root del projecte (%s): %v", rootDir, err)
	}

	// La BD en si va en un directori temporal independent
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	cfg := map[string]string{
		"DB_ENGINE": "sqlite",
		"DB_PATH":   dbPath,
		"RECREADB":  "true", // força a aplicar db/SQLite.sql
	}

	d, err := db.NewDB(cfg)
	if err != nil {
		t.Fatalf("NewDB ha fallat: %v", err)
	}

	return d
}

// TestUserLifecycleSQLite comprova el cicle bàsic:
// 1) Inserir usuari
// 2) Guardar token d'activació
// 3) Activar usuari amb el token
// 4) Autenticar usuari amb nom d'usuari i amb correu
func TestUserLifecycleSQLite(t *testing.T) {
	d := newTestSQLiteDB(t)
	defer d.Close()

	// 1) Creem un usuari amb contrasenya hashejada (bcrypt)
	rawPassword := "ContrasenyaDeProva123!"
	hashed, err := bcrypt.GenerateFromPassword([]byte(rawPassword), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("no s'ha pogut generar hash de contrasenya: %v", err)
	}

	u := &db.User{
		Usuari:        "usuari_test",
		Name:          "NomProva",
		Surname:       "CognomsProva",
		Email:         "usuari.test@example.com",
		Password:      hashed,
		DataNaixament: "2000-01-02",
		Pais:          "ES",
		Estat:         "CAT",
		Provincia:     "Lleida",
		Poblacio:      "Linyola",
		CodiPostal:    "25240",
		Active:        false, // l'activarem més endavant
	}

	if err := d.InsertUser(u); err != nil {
		t.Fatalf("InsertUser ha fallat: %v", err)
	}
	if u.ID == 0 {
		t.Fatalf("després d'InsertUser, l'ID hauria de ser > 0, però és %d", u.ID)
	}

	// 2) Comprovem que el podem recuperar per correu, encara inactiu
	uDB, err := d.GetUserByEmail(u.Email)
	if err != nil {
		t.Fatalf("GetUserByEmail ha fallat: %v", err)
	}
	if uDB.Email != u.Email {
		t.Errorf("Email recuperat = %q, esperava %q", uDB.Email, u.Email)
	}
	if uDB.Active {
		t.Errorf("esperava usuari inactiu just després d'InsertUser, però Active = true")
	}

	// 3) Guardem un token d'activació i el fem servir per activar
	const token = "TOKEN-DE-PROVA-123"

	if err := d.SaveActivationToken(u.Email, token); err != nil {
		t.Fatalf("SaveActivationToken ha fallat: %v", err)
	}

	if err := d.ActivateUser(token); err != nil {
		t.Fatalf("ActivateUser ha fallat: %v", err)
	}

	// Després d'activar, l'usuari ha de sortir com a actiu
	uDB2, err := d.GetUserByEmail(u.Email)
	if err != nil {
		t.Fatalf("GetUserByEmail després d'activar ha fallat: %v", err)
	}
	if !uDB2.Active {
		t.Errorf("esperava usuari actiu després d'activar, però Active = false")
	}

	// 4) Ara AuthenticateUser ha de funcionar tant pel nom d'usuari com pel correu
	uAuthByUser, err := d.AuthenticateUser(u.Usuari, rawPassword)
	if err != nil {
		t.Fatalf("AuthenticateUser amb usuari ha fallat: %v", err)
	}
	if uAuthByUser.ID != u.ID {
		t.Errorf("AuthenticateUser(usuari) ha retornat ID %d, esperava %d", uAuthByUser.ID, u.ID)
	}

	uAuthByEmail, err := d.AuthenticateUser(u.Email, rawPassword)
	if err != nil {
		t.Fatalf("AuthenticateUser amb correu ha fallat: %v", err)
	}
	if uAuthByEmail.ID != u.ID {
		t.Errorf("AuthenticateUser(email) ha retornat ID %d, esperava %d", uAuthByEmail.ID, u.ID)
	}

	// I si posem una contrasenya incorrecta, ha de fallar
	if _, err := d.AuthenticateUser(u.Email, "ContrasenyaIncorrecta!"); err == nil {
		t.Fatalf("AuthenticateUser amb contrasenya incorrecta hauria de fallar, però no retorna error")
	}
}
