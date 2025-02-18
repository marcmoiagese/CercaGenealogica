package integration

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestEspaiPublicArbreHidesHiddenPersons(t *testing.T) {
	forEachArbreDB(t, func(t *testing.T, label string, app *core.App, database db.DB, _ string) {
		user, _ := createF7UserWithSession(t, database)

		tree := &db.EspaiArbre{
			OwnerUserID: user.ID,
			Nom:         "Arbre public",
			Visibility:  "public",
			Status:      "active",
		}
		if _, err := database.CreateEspaiArbre(tree); err != nil {
			t.Fatalf("CreateEspaiArbre ha fallat: %v", err)
		}

		visible := &db.EspaiPersona{
			OwnerUserID: user.ID,
			ArbreID:     tree.ID,
			Nom:         sql.NullString{String: "Visible", Valid: true},
			Visibility:  "visible",
			Status:      "active",
		}
		if _, err := database.CreateEspaiPersona(visible); err != nil {
			t.Fatalf("CreateEspaiPersona visible ha fallat: %v", err)
		}

		hidden := &db.EspaiPersona{
			OwnerUserID: user.ID,
			ArbreID:     tree.ID,
			Nom:         sql.NullString{String: "Ocult", Valid: true},
			Visibility:  "hidden",
			Status:      "active",
		}
		if _, err := database.CreateEspaiPersona(hidden); err != nil {
			t.Fatalf("CreateEspaiPersona hidden ha fallat: %v", err)
		}

		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/public/espai/arbres/%d", tree.ID), nil)
		rr := httptest.NewRecorder()

		app.EspaiPublicArbreAPI(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status inesperat (%s): %d", label, rr.Code)
		}

		var resp arbreDatasetResp
		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatalf("no es pot parsejar resposta: %v", err)
		}

		if !hasPersonID(resp.FamilyData, visible.ID) {
			t.Fatalf("persona visible no retornada (%s)", label)
		}
		if hasPersonID(resp.FamilyData, hidden.ID) {
			t.Fatalf("persona oculta exposada (%s)", label)
		}
	})
}
