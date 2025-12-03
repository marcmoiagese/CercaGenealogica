package unit

import (
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

// IMPORTANT: aquí REUTILITZEM la funció newTestSQLiteDB(t *testing.T) db.DB
// que ja tens definida en un altre fitxer de tests del mateix package unit.

// Helpers per convertir valors retornats per DB.Query
func toString(v interface{}) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return ""
	}
}

func toInt(v interface{}) int {
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	case int32:
		return int(x)
	case bool:
		if x {
			return 1
		}
		return 0
	case []byte:
		s := string(x)
		if s == "1" {
			return 1
		}
		if s == "0" {
			return 0
		}
		return 0
	case string:
		if x == "1" {
			return 1
		}
		if x == "0" {
			return 0
		}
		return 0
	default:
		return 0
	}
}

func TestExistsUserByUsernameAndEmail(t *testing.T) {
	d := newTestSQLiteDB(t) // reutilitzem helper existent

	user := &db.User{
		Usuari:   "jdoe",
		Name:     "John",
		Surname:  "Doe",
		Email:    "john@example.com",
		Password: []byte("dummy-hash"),
		Active:   true,
	}

	if err := d.InsertUser(user); err != nil {
		t.Fatalf("InsertUser ha fallat: %v", err)
	}

	// Ha d'existir per usuari
	exists, err := d.ExistsUserByUsername("jdoe")
	if err != nil {
		t.Fatalf("ExistsUserByUsername ha retornat error: %v", err)
	}
	if !exists {
		t.Fatalf("s'esperava que l'usuari 'jdoe' existís")
	}

	// Ha d'existir per correu
	exists, err = d.ExistsUserByEmail("john@example.com")
	if err != nil {
		t.Fatalf("ExistsUserByEmail ha retornat error: %v", err)
	}
	if !exists {
		t.Fatalf("s'esperava que el correu 'john@example.com' existís")
	}

	// No ha d'existir per usuari inexistent
	if exists, err := d.ExistsUserByUsername("no-such-user"); err != nil {
		t.Fatalf("ExistsUserByUsername (no-such-user) ha retornat error inesperat: %v", err)
	} else if exists {
		t.Fatalf("no s'esperava que 'no-such-user' existís")
	}

	// No ha d'existir per correu inexistent
	if exists, err := d.ExistsUserByEmail("no@example.com"); err != nil {
		t.Fatalf("ExistsUserByEmail (no@example.com) ha retornat error inesperat: %v", err)
	} else if exists {
		t.Fatalf("no s'esperava que 'no@example.com' existís")
	}
}

func TestSaveActivationTokenAndActivateUserOK(t *testing.T) {
	d := newTestSQLiteDB(t)

	user := &db.User{
		Usuari:   "pending",
		Name:     "Pending",
		Surname:  "User",
		Email:    "pending@example.com",
		Password: []byte("dummy-hash"),
		Active:   false, // usuari no actiu d'entrada
	}

	if err := d.InsertUser(user); err != nil {
		t.Fatalf("InsertUser ha fallat: %v", err)
	}

	const token = "token-activacio-test"

	// 1) Guardem token d'activació
	if err := d.SaveActivationToken(user.Email, token); err != nil {
		t.Fatalf("SaveActivationToken ha fallat: %v", err)
	}

	rows, err := d.Query("SELECT token_activacio, expira_token, actiu FROM usuaris WHERE correu = ?", user.Email)
	if err != nil {
		t.Fatalf("Query després de SaveActivationToken ha fallat: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("s'esperava 1 fila per a l'usuari, s'han trobat %d", len(rows))
	}

	row := rows[0]
	gotToken := toString(row["token_activacio"])
	if gotToken != token {
		t.Fatalf("token_activacio incorrecte, esperat %q, rebut %q", token, gotToken)
	}

	// Nota: no assumim res sobre expira_token aquí, perquè pel comportament
	// actual pot ser NULL/buit. Si en el futur decideixes omplir-lo, ja
	// podrem afegir una comprovació més estricta.

	// 2) Activem l'usuari amb el token
	if err := d.ActivateUser(token); err != nil {
		t.Fatalf("ActivateUser ha fallat: %v", err)
	}

	rows, err = d.Query("SELECT token_activacio, expira_token, actiu FROM usuaris WHERE correu = ?", user.Email)
	if err != nil {
		t.Fatalf("Query després de ActivateUser ha fallat: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("s'esperava 1 fila per a l'usuari després d'activar, s'han trobat %d", len(rows))
	}

	row = rows[0]
	if toInt(row["actiu"]) != 1 {
		t.Fatalf("després de ActivateUser s'esperava actiu=1, rebut: %#v", row["actiu"])
	}

	// Si la teva implementació actual neteja token_activacio / expira_token,
	// aquestes asserts passaran; si en el futur canvies la lògica, podem
	// adaptar-les fàcilment.
	if toString(row["token_activacio"]) != "" {
		t.Fatalf("després de ActivateUser s'esperava token_activacio NULL/buit")
	}
	if toString(row["expira_token"]) != "" {
		t.Fatalf("després de ActivateUser s'esperava expira_token NULL/buit")
	}
}

func TestActivateUserInvalidOrExpiredToken(t *testing.T) {
	d := newTestSQLiteDB(t)

	user := &db.User{
		Usuari:   "pending2",
		Name:     "Pending2",
		Surname:  "User2",
		Email:    "pending2@example.com",
		Password: []byte("dummy-hash"),
		Active:   false,
	}

	if err := d.InsertUser(user); err != nil {
		t.Fatalf("InsertUser ha fallat: %v", err)
	}

	const token = "token-expirat"

	if err := d.SaveActivationToken(user.Email, token); err != nil {
		t.Fatalf("SaveActivationToken ha fallat: %v", err)
	}

	// Forcem expira_token al passat perquè el token sigui "expirat".
	if _, err := d.Exec("UPDATE usuaris SET expira_token = datetime('now', '-1 hour') WHERE correu = ?", user.Email); err != nil {
		t.Fatalf("no s'ha pogut forçar expira_token al passat: %v", err)
	}

	err := d.ActivateUser(token)
	if err == nil {
		t.Fatalf("s'esperava error activant amb token expirat")
	}
	if !strings.Contains(err.Error(), "token") {
		t.Fatalf("esperàvem un error de token invàlid o expirat, rebut: %v", err)
	}

	// També comprovem que un token completament inventat falla
	err = d.ActivateUser("token-inexistent")
	if err == nil {
		t.Fatalf("s'esperava error activant amb token inexistent")
	}
}

// Tests de gestió de sessions a la capa DB (SQLite)

func insertDummyUserForSessions(t *testing.T, d db.DB) *db.User {
	t.Helper()

	u := &db.User{
		Usuari:        "sess_user",
		Name:          "Sessio",
		Surname:       "Prova",
		Email:         "sessio.prova@example.com",
		Password:      []byte("hash-fictici"),
		DataNaixament: "2000-01-01",
		Pais:          "",
		Estat:         "",
		Provincia:     "",
		Poblacio:      "",
		CodiPostal:    "",
		CreatedAt:     "",
		Active:        true,
	}

	if err := d.InsertUser(u); err != nil {
		t.Fatalf("no s'ha pogut inserir usuari de prova per sessions: %v", err)
	}
	if u.ID == 0 {
		t.Fatalf("després d'InsertUser s'esperava ID > 0, rebut %d", u.ID)
	}

	return u
}

func TestSessionLifecycleCreateAndGet(t *testing.T) {
	d := newTestSQLiteDB(t)
	defer d.Close()

	user := insertDummyUserForSessions(t, d)

	const sessionID = "sessio-test-123"

	// Ara mateix l'argument expiry no s'utilitza a SQLite,
	// però el passem igualment per mantenir la signatura.
	if err := d.SaveSession(sessionID, user.ID, "ignored-expiry"); err != nil {
		t.Fatalf("SaveSession ha fallat: %v", err)
	}

	dbUser, err := d.GetSessionUser(sessionID)
	if err != nil {
		t.Fatalf("GetSessionUser ha retornat error: %v", err)
	}
	if dbUser == nil {
		t.Fatalf("s'esperava usuari no nul per la sessió %q", sessionID)
	}
	if dbUser.ID != user.ID {
		t.Fatalf("s'esperava user.ID=%d, rebut %d", user.ID, dbUser.ID)
	}
	if dbUser.Email != user.Email {
		t.Fatalf("s'esperava email=%q, rebut %q", user.Email, dbUser.Email)
	}
}

func TestSessionLifecycleDelete(t *testing.T) {
	d := newTestSQLiteDB(t)
	defer d.Close()

	user := insertDummyUserForSessions(t, d)

	const sessionID = "sessio-test-delete"

	if err := d.SaveSession(sessionID, user.ID, "ignored-expiry"); err != nil {
		t.Fatalf("SaveSession ha fallat: %v", err)
	}

	// Ens assegurem que existeix abans d'eliminar
	if _, err := d.GetSessionUser(sessionID); err != nil {
		t.Fatalf("abans de DeleteSession, GetSessionUser ha fallat: %v", err)
	}

	if err := d.DeleteSession(sessionID); err != nil {
		t.Fatalf("DeleteSession ha fallat: %v", err)
	}

	// Després d'eliminar, GetSessionUser ha de fallar
	dbUser, err := d.GetSessionUser(sessionID)
	if err == nil {
		t.Fatalf("després de DeleteSession, s'esperava error però n'hem rebut cap; usuari=%+v", dbUser)
	}
}
