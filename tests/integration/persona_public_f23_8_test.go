package integration

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestPersonaPublicPage(t *testing.T) {
	forEachArbreDB(t, func(t *testing.T, label string, app *core.App, database db.DB, _ string) {
		user, _ := createF7UserWithSession(t, database)
		personaID := createTestPersona(t, database, user.ID, "Nom", "Public")

		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/public/persones/%d", personaID), nil)
		rr := httptest.NewRecorder()
		app.PersonaPublic(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("status inesperat (%s): %d", label, rr.Code)
		}
		body := rr.Body.String()
		if !strings.Contains(body, "Nom Public") {
			t.Fatalf("no trobo el nom de la persona al HTML")
		}
		if !strings.Contains(body, fmt.Sprintf("/public/persones/%d/arbre", personaID)) {
			t.Fatalf("no trobo l'enllaç a l'arbre public")
		}
	})
}

func TestPersonaPublicTreePlaceholder(t *testing.T) {
	forEachArbreDB(t, func(t *testing.T, label string, app *core.App, database db.DB, _ string) {
		user, _ := createF7UserWithSession(t, database)
		personaID := createTestPersona(t, database, user.ID, "Nom", "PublicArbre")

		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/public/persones/%d/arbre", personaID), nil)
		rr := httptest.NewRecorder()
		app.PersonaPublicArbre(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("status inesperat (%s) arbre public: %d", label, rr.Code)
		}
	})
}
