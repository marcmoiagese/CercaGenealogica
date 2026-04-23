package integration

import (
	"bytes"
	"database/sql"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func createModeracioPersona(t *testing.T, database db.DB, nom, cognom string, userID int) *db.Persona {
	t.Helper()
	persona := &db.Persona{
		Nom:            nom,
		Cognom1:        cognom,
		ModeracioEstat: "pendent",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
		UpdatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	if _, err := database.CreatePersona(persona); err != nil {
		t.Fatalf("CreatePersona ha fallat: %v", err)
	}
	return persona
}

func TestModeracioListPaginacioPersones(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_11_list.sqlite3")

	admin := createTestUser(t, database, "admin_list")
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_list")

	surnames := []string{
		"Alpha",
		"Beta",
		"Gamma",
		"Delta",
		"Epsilon",
		"Zeta",
		"Eta",
		"Theta",
		"Iota",
		"Kappa",
		"Lambda",
	}
	for _, surname := range surnames {
		createModeracioPersona(t, database, "Marc", surname, admin.ID)
	}

	req := httptest.NewRequest(http.MethodGet, "/moderacio?type=persona&per_page=10&page=1", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminModeracioList(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("moderacio page 1 esperava 200, got %d", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "Marc Lambda") || !strings.Contains(body, "Marc Kappa") {
		t.Fatalf("page 1 hauria de contenir les persones mes recents")
	}
	if strings.Contains(body, "Marc Alpha") {
		t.Fatalf("page 1 no hauria de contenir la persona de pagina 2")
	}
	if !strings.Contains(body, "1 / 2") {
		t.Fatalf("page 1 hauria de mostrar paginacio 1 / 2")
	}

	req2 := httptest.NewRequest(http.MethodGet, "/moderacio?type=persona&per_page=10&page=2", nil)
	req2.AddCookie(session)
	rr2 := httptest.NewRecorder()
	app.AdminModeracioList(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("moderacio page 2 esperava 200, got %d", rr2.Code)
	}
	body2 := rr2.Body.String()
	if !strings.Contains(body2, "Marc Alpha") {
		t.Fatalf("page 2 hauria de contenir la persona restant")
	}
	if strings.Contains(body2, "Marc Lambda") {
		t.Fatalf("page 2 no hauria de contenir la persona de pagina 1")
	}
	if !strings.Contains(body2, "2 / 2") {
		t.Fatalf("page 2 hauria de mostrar paginacio 2 / 2")
	}
}

func TestModeracioListUserFilter(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_11_list_user.sqlite3")

	admin := createTestUser(t, database, "admin_list_user")
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_list_user")

	userA := createTestUser(t, database, "user_a")
	userB := createTestUser(t, database, "user_b")

	createModeracioPersona(t, database, "Laura", "A", userA.ID)
	createModeracioPersona(t, database, "Pau", "B", userB.ID)

	req := httptest.NewRequest(http.MethodGet, "/moderacio?type=persona&user=user_b", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminModeracioList(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("moderacio filter esperava 200, got %d", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "Pau B") {
		t.Fatalf("filtre usuari hauria de mostrar persona de user_b")
	}
	if strings.Contains(body, "Laura A") {
		t.Fatalf("filtre usuari no hauria de mostrar persona de user_a")
	}
	if !strings.Contains(body, "Pendent") {
		t.Fatalf("la columna d'estat hauria de traduir 'pendent'")
	}
	if strings.Contains(body, "moderation.status.pendent") {
		t.Fatalf("la columna d'estat no hauria de renderitzar la clau d'i18n crua")
	}
}

func TestModeracioListDebugLogs(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_11_list_debug.sqlite3")

	admin := createTestUser(t, database, "admin_list_debug")
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_list_debug")

	createModeracioPersona(t, database, "Nil", "Debug", admin.ID)

	origOutput := log.Writer()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() {
		log.SetOutput(origOutput)
		core.SetLogLevel("silent")
	})

	core.SetLogLevel("error")
	req := httptest.NewRequest(http.MethodGet, "/moderacio?type=persona", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminModeracioList(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("moderacio debug off esperava 200, got %d", rr.Code)
	}
	if strings.Contains(buf.String(), "moderacio entry") {
		t.Fatalf("no s'haurien de logar timings en mode no debug")
	}

	buf.Reset()
	core.SetLogLevel("debug")
	req2 := httptest.NewRequest(http.MethodGet, "/moderacio?type=persona", nil)
	req2.AddCookie(session)
	rr2 := httptest.NewRecorder()
	app.AdminModeracioList(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("moderacio debug on esperava 200, got %d", rr2.Code)
	}
	if !strings.Contains(buf.String(), "moderacio entry") {
		t.Fatalf("hauria de logar timings en mode debug")
	}
}
