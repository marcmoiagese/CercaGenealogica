package core

import (
	"database/sql"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func newF330PermissionsTestApp(t *testing.T) (*App, db.DB) {
	t.Helper()

	start, err := os.Getwd()
	if err != nil {
		t.Fatalf("no puc obtenir directori actual: %v", err)
	}
	root := start
	for {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(root)
		if parent == root {
			t.Fatalf("no s'ha trobat go.mod a cap directori pare de %s", start)
		}
		root = parent
	}
	if err := os.Chdir(root); err != nil {
		t.Fatalf("no puc entrar a l'arrel del projecte: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(start) })

	cfg := map[string]string{
		"DB_DRIVER": "sqlite",
		"DB_PATH":   filepath.Join(t.TempDir(), "f33-0-permissions.db"),
		"RECREADB":  "true",
	}
	database, err := db.NewDB(cfg)
	if err != nil {
		t.Fatalf("no s'ha pogut crear DB test: %v", err)
	}
	if err := database.EnsureDefaultPolicies(); err != nil {
		t.Fatalf("no s'han pogut assegurar politiques: %v", err)
	}
	app := NewApp(cfg, database)
	t.Cleanup(func() { app.Close() })
	return app, database
}

func createF330User(t *testing.T, database db.DB, username string) int {
	t.Helper()
	user := &db.User{
		Usuari:   username,
		Name:     "Test",
		Surname:  "F33",
		Email:    fmt.Sprintf("%s@example.com", username),
		Password: []byte("pw"),
		Active:   true,
	}
	if err := database.InsertUser(user); err != nil {
		t.Fatalf("no s'ha pogut crear usuari: %v", err)
	}
	return user.ID
}

func findF330PolicyID(t *testing.T, database db.DB, name string) int {
	t.Helper()
	policies, err := database.ListPolitiques()
	if err != nil {
		t.Fatalf("no s'han pogut llistar politiques: %v", err)
	}
	for _, p := range policies {
		if p.Nom == name {
			return p.ID
		}
	}
	t.Fatalf("no s'ha trobat la politica %q", name)
	return 0
}

func TestF330AdminPolicyNameIsEffectiveModularAdminForUI(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-admin-name")
	adminID := findF330PolicyID(t, database, "admin")

	if _, err := database.SavePolitica(&db.Politica{
		ID:        adminID,
		Nom:       "admin",
		Permisos:  "{}",
		Descripcio: "",
	}); err != nil {
		t.Fatalf("no s'ha pogut simular admin legacy sense flag JSON: %v", err)
	}
	if err := database.AddUserPolitica(userID, adminID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica admin: %v", err)
	}

	if !app.HasPermission(userID, permKeyTerritoriNivellsEdit, PermissionTarget{PaisID: intPtr(1)}) {
		t.Fatalf("admin per politica admin hauria de tenir HasPermission per editar nivells")
	}
	if !app.hasAnyPermissionKey(userID, permKeyTerritoriNivellsView) {
		t.Fatalf("admin per politica admin hauria de tenir hasAnyPermissionKey")
	}
	if keys := app.permissionKeysForUser(userID); !keys[permKeyTerritoriNivellsEdit] || !keys[permKeyTerritoriNivellsView] {
		t.Fatalf("permissionKeysForUser(admin) hauria d'exposar claus de nivells: %#v", keys)
	}
	if filter := app.buildListScopeFilter(userID, permKeyTerritoriNivellsView, ScopePais); !filter.hasGlobal {
		t.Fatalf("admin per politica admin hauria de tenir filtre global")
	}

	perms := app.getPermissionsForUser(userID)
	if perms.Admin {
		t.Fatalf("el test necessita JSON legacy sense Admin=true per reproduir la incoherencia UI")
	}
	req := httptest.NewRequest("GET", "/territori/nivells", nil)
	req = app.withPermissions(req, perms)
	req = app.withEffectiveAdmin(req, app.effectiveAdminForUser(userID, perms))
	req = app.withPermissionKeys(req, app.permissionKeysForUser(userID))
	data := injectPermsIfMissing(req, map[string]interface{}{}).(map[string]interface{})
	if got := data["IsAdmin"]; got != true {
		t.Fatalf("la UI hauria de veure IsAdmin efectiu modular, rebut %#v", got)
	}
	if got := data["CanViewNivells"]; got != true {
		t.Fatalf("la UI hauria de veure nivells per admin efectiu, rebut %#v", got)
	}
	if got := data["CanManageTerritory"]; got != true {
		t.Fatalf("la UI hauria de veure territori per admin efectiu, rebut %#v", got)
	}
}

func TestF330PermissionPolicyAdminFlagIsEffectiveModularAdmin(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-admin-flag")
	policy := &db.Politica{
		Nom:       "admin-json",
		Permisos:  `{"admin":true}`,
		Descripcio: "",
	}
	policyID, err := database.SavePolitica(policy)
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica admin JSON: %v", err)
	}
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica admin JSON: %v", err)
	}

	if !app.HasPermission(userID, permKeyTerritoriNivellsEdit, PermissionTarget{PaisID: intPtr(1)}) {
		t.Fatalf("PolicyPermissions.Admin=true hauria de donar permisos modulars efectius")
	}
	if filter := app.buildListScopeFilter(userID, permKeyTerritoriNivellsView, ScopePais); !filter.hasGlobal {
		t.Fatalf("PolicyPermissions.Admin=true hauria de donar filtre global")
	}
}

func TestF330ScopedTerritoryUserDoesNotBecomeGlobalAdmin(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-scoped")
	policy := &db.Politica{
		Nom:       "territori-scoped",
		Permisos:  "{}",
		Descripcio: "",
	}
	policyID, err := database.SavePolitica(policy)
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica scoped: %v", err)
	}
	if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
		PoliticaID:      policyID,
		PermKey:         permKeyTerritoriNivellsEdit,
		ScopeType:       string(ScopePais),
		ScopeID:         sql.NullInt64{Int64: 7, Valid: true},
		IncludeChildren: true,
	}); err != nil {
		t.Fatalf("no s'ha pogut crear grant scoped: %v", err)
	}
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica scoped: %v", err)
	}

	if app.effectiveAdminForUser(userID, app.getPermissionsForUser(userID)) {
		t.Fatalf("un usuari territorial scoped no s'ha de convertir en admin global")
	}
	if app.HasPermission(userID, permKeyTerritoriNivellsEdit, PermissionTarget{PaisID: intPtr(8)}) {
		t.Fatalf("grant scoped a pais 7 no hauria d'autoritzar pais 8")
	}
	if !app.HasPermission(userID, permKeyTerritoriNivellsEdit, PermissionTarget{PaisID: intPtr(7)}) {
		t.Fatalf("grant scoped a pais 7 hauria d'autoritzar pais 7")
	}
}
