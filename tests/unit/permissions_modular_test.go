package unit

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

const (
	permKeyDocumentalsArxiusDelete = "documentals.arxius.delete"
	permKeyDocumentalsArxiusView   = "documentals.arxius.view"
	permKeyTerritoriPaisosView     = "territori.paisos.view"
	permKeyDocumentalsLlibresEdit  = "documentals.llibres.edit"
	permKeyTerritoriMunicipisEdit  = "territori.municipis.edit"
	permKeyTerritoriMunicipisView  = "territori.municipis.view"
)

func intPtr(val int) *int {
	if val == 0 {
		return nil
	}
	v := val
	return &v
}

func chdirProjectRoot(t *testing.T) {
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
		t.Fatalf("no puc fer chdir a l'arrel del projecte (%s): %v", root, err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(start)
	})
}

func newPermissionsTestApp(t *testing.T) (*core.App, db.DB) {
	t.Helper()

	chdirProjectRoot(t)

	dbPath := filepath.Join(t.TempDir(), "permissions-test.db")
	cfg := newTestConfig()
	cfg["DB_PATH"] = dbPath
	database, err := db.NewDB(cfg)
	if err != nil {
		t.Fatalf("no s'ha pogut crear DB test: %v", err)
	}
	if err := database.EnsureDefaultPolicies(); err != nil {
		t.Fatalf("no s'ha pogut assegurar polítiques per defecte: %v", err)
	}
	app := core.NewApp(cfg, database)
	t.Cleanup(func() {
		app.Close()
	})
	return app, database
}

func createTestUser(t *testing.T, database db.DB, username string) int {
	t.Helper()
	user := &db.User{
		Usuari:   username,
		Name:     "Test",
		Surname:  "User",
		Email:    fmt.Sprintf("%s@example.com", username),
		Password: []byte("pw"),
		Active:   true,
	}
	if err := database.InsertUser(user); err != nil {
		t.Fatalf("no s'ha pogut crear usuari: %v", err)
	}
	return user.ID
}

func createTestPolicy(t *testing.T, database db.DB, name string) int {
	t.Helper()
	pol := &db.Politica{
		Nom:        name,
		Descripcio: "",
		Permisos:   "{}",
	}
	id, err := database.SavePolitica(pol)
	if err != nil {
		t.Fatalf("no s'ha pogut crear política: %v", err)
	}
	return id
}

func addTestGrant(t *testing.T, database db.DB, policyID int, permKey string, scopeType core.ScopeType, scopeID int, includeChildren bool) {
	t.Helper()
	grant := &db.PoliticaGrant{
		PoliticaID:      policyID,
		PermKey:         permKey,
		ScopeType:       string(scopeType),
		IncludeChildren: includeChildren,
	}
	if scopeType != core.ScopeGlobal {
		grant.ScopeID = sql.NullInt64{Int64: int64(scopeID), Valid: true}
	}
	if _, err := database.SavePoliticaGrant(grant); err != nil {
		t.Fatalf("no s'ha pogut crear grant: %v", err)
	}
}

func findPolicyID(t *testing.T, database db.DB, name string) int {
	t.Helper()
	policies, err := database.ListPolitiques()
	if err != nil {
		t.Fatalf("no s'ha pogut llistar polítiques: %v", err)
	}
	for _, p := range policies {
		if p.Nom == name {
			return p.ID
		}
	}
	t.Fatalf("no s'ha trobat la política %q", name)
	return 0
}

func TestHasPermissionAdminAlwaysTrue(t *testing.T) {
	app, database := newPermissionsTestApp(t)
	userID := createTestUser(t, database, "admin-test")
	adminID := findPolicyID(t, database, "admin")
	if err := database.AddUserPolitica(userID, adminID); err != nil {
		t.Fatalf("no s'ha pogut assignar política admin: %v", err)
	}
	if !app.HasPermission(userID, permKeyDocumentalsArxiusDelete, core.PermissionTarget{}) {
		t.Fatalf("admin hauria de tenir permís per defecte")
	}
}

func TestHasPermissionGlobalGrant(t *testing.T) {
	app, database := newPermissionsTestApp(t)
	userID := createTestUser(t, database, "global-test")
	policyID := createTestPolicy(t, database, "global-policy")
	addTestGrant(t, database, policyID, permKeyDocumentalsArxiusView, core.ScopeGlobal, 0, false)
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar política: %v", err)
	}
	if !app.HasPermission(userID, permKeyDocumentalsArxiusView, core.PermissionTarget{}) {
		t.Fatalf("grant global hauria de donar permís")
	}
	if app.HasPermission(userID, permKeyTerritoriPaisosView, core.PermissionTarget{}) {
		t.Fatalf("permís no assignat no hauria de ser vàlid")
	}
}

func TestHasPermissionArxiuScope(t *testing.T) {
	app, database := newPermissionsTestApp(t)

	userID := createTestUser(t, database, "arxiu-sense")
	policyID := createTestPolicy(t, database, "arxiu-policy")
	addTestGrant(t, database, policyID, permKeyDocumentalsLlibresEdit, core.ScopeArxiu, 7, false)
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar política: %v", err)
	}
	arxiuTarget := core.PermissionTarget{ArxiuID: intPtr(7), ArxiuIDs: []int{7}}
	if !app.HasPermission(userID, permKeyDocumentalsLlibresEdit, arxiuTarget) {
		t.Fatalf("grant d'arxiu hauria d'aplicar al mateix arxiu")
	}
	llibreTarget := core.PermissionTarget{LlibreID: intPtr(11), ArxiuIDs: []int{7}}
	if app.HasPermission(userID, permKeyDocumentalsLlibresEdit, llibreTarget) {
		t.Fatalf("grant d'arxiu sense children no hauria d'aplicar a llibres")
	}

	userID2 := createTestUser(t, database, "arxiu-amb")
	policyID2 := createTestPolicy(t, database, "arxiu-children-policy")
	addTestGrant(t, database, policyID2, permKeyDocumentalsLlibresEdit, core.ScopeArxiu, 7, true)
	if err := database.AddUserPolitica(userID2, policyID2); err != nil {
		t.Fatalf("no s'ha pogut assignar política: %v", err)
	}
	if !app.HasPermission(userID2, permKeyDocumentalsLlibresEdit, llibreTarget) {
		t.Fatalf("grant d'arxiu amb children hauria d'aplicar a llibres")
	}
}

func TestHasPermissionTerritoryIncludeChildren(t *testing.T) {
	app, database := newPermissionsTestApp(t)

	userID := createTestUser(t, database, "comarca-amb")
	policyID := createTestPolicy(t, database, "comarca-policy")
	addTestGrant(t, database, policyID, permKeyTerritoriMunicipisEdit, core.ScopeComarca, 3, true)
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar política: %v", err)
	}
	target := core.PermissionTarget{
		MunicipiID: intPtr(5),
		ComarcaID:  intPtr(3),
	}
	if !app.HasPermission(userID, permKeyTerritoriMunicipisEdit, target) {
		t.Fatalf("grant de comarca amb children hauria d'aplicar a municipis")
	}

	userID2 := createTestUser(t, database, "comarca-sense")
	policyID2 := createTestPolicy(t, database, "comarca-sense-policy")
	addTestGrant(t, database, policyID2, permKeyTerritoriMunicipisEdit, core.ScopeComarca, 3, false)
	if err := database.AddUserPolitica(userID2, policyID2); err != nil {
		t.Fatalf("no s'ha pogut assignar política: %v", err)
	}
	if app.HasPermission(userID2, permKeyTerritoriMunicipisEdit, target) {
		t.Fatalf("grant de comarca sense children no hauria d'aplicar a municipis")
	}
}

func TestHasPermissionMultiplePolicies(t *testing.T) {
	app, database := newPermissionsTestApp(t)
	userID := createTestUser(t, database, "multi-policy")

	policyA := createTestPolicy(t, database, "policy-a")
	addTestGrant(t, database, policyA, permKeyDocumentalsArxiusView, core.ScopeGlobal, 0, false)
	if err := database.AddUserPolitica(userID, policyA); err != nil {
		t.Fatalf("no s'ha pogut assignar política A: %v", err)
	}

	policyB := createTestPolicy(t, database, "policy-b")
	addTestGrant(t, database, policyB, permKeyTerritoriMunicipisView, core.ScopeGlobal, 0, false)
	if err := database.AddUserPolitica(userID, policyB); err != nil {
		t.Fatalf("no s'ha pogut assignar política B: %v", err)
	}

	if !app.HasPermission(userID, permKeyTerritoriMunicipisView, core.PermissionTarget{}) {
		t.Fatalf("permís en una altra política hauria de ser vàlid")
	}
	if !app.HasAnyPermission(userID, []string{permKeyDocumentalsArxiusDelete, permKeyTerritoriMunicipisView}, core.PermissionTarget{}) {
		t.Fatalf("HasAnyPermission hauria de retornar true quan hi ha una coincidència")
	}
}

func TestPermissionsVersionBumps(t *testing.T) {
	_, database := newPermissionsTestApp(t)
	userID := createTestUser(t, database, "version-test")

	policyID := createTestPolicy(t, database, "version-policy")
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar política: %v", err)
	}
	before, err := database.GetUserPermissionsVersion(userID)
	if err != nil {
		t.Fatalf("no s'ha pogut obtenir permissions_version: %v", err)
	}
	if err := database.BumpPolicyPermissionsVersion(policyID); err != nil {
		t.Fatalf("no s'ha pogut fer bump de política: %v", err)
	}
	after, err := database.GetUserPermissionsVersion(userID)
	if err != nil {
		t.Fatalf("no s'ha pogut obtenir permissions_version final: %v", err)
	}
	if after != before+1 {
		t.Fatalf("permissions_version no ha pujat per política: %d -> %d", before, after)
	}

	groupID, err := database.Exec("INSERT INTO grups (nom, descripcio) VALUES (?, ?)", "group-test", "")
	if err != nil {
		t.Fatalf("no s'ha pogut crear grup: %v", err)
	}
	if _, err := database.Exec("INSERT INTO usuaris_grups (usuari_id, grup_id) VALUES (?, ?)", userID, groupID); err != nil {
		t.Fatalf("no s'ha pogut assignar usuari al grup: %v", err)
	}
	beforeGroup, err := database.GetUserPermissionsVersion(userID)
	if err != nil {
		t.Fatalf("no s'ha pogut obtenir permissions_version grup: %v", err)
	}
	if err := database.BumpGroupPermissionsVersion(int(groupID)); err != nil {
		t.Fatalf("no s'ha pogut fer bump de grup: %v", err)
	}
	afterGroup, err := database.GetUserPermissionsVersion(userID)
	if err != nil {
		t.Fatalf("no s'ha pogut obtenir permissions_version grup final: %v", err)
	}
	if afterGroup != beforeGroup+1 {
		t.Fatalf("permissions_version no ha pujat per grup: %d -> %d", beforeGroup, afterGroup)
	}
}
