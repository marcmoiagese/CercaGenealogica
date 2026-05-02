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
		"DB_ENGINE": "sqlite",
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
		ID:         adminID,
		Nom:        "admin",
		Permisos:   "{}",
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
		Nom:        "admin-json",
		Permisos:   `{"admin":true}`,
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
		Nom:        "territori-scoped",
		Permisos:   "{}",
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

func TestF334MediaModerationKeyEnablesModerationUI(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-4-media-moderator")
	policy := &db.Politica{
		Nom:        "media-moderator",
		Permisos:   "{}",
		Descripcio: "",
	}
	policyID, err := database.SavePolitica(policy)
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica media moderator: %v", err)
	}
	if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
		PoliticaID: policyID,
		PermKey:    permKeyMediaModerate,
		ScopeType:  string(ScopeGlobal),
	}); err != nil {
		t.Fatalf("no s'ha pogut crear grant media.moderate: %v", err)
	}
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica media moderator: %v", err)
	}

	perms := app.getPermissionsForUser(userID)
	if app.hasPerm(perms, permModerate) {
		t.Fatalf("el test necessita usuari sense permModerate legacy")
	}
	req := httptest.NewRequest("GET", "/admin/moderacio/media", nil)
	req = app.withPermissions(req, perms)
	req = app.withEffectiveAdmin(req, app.effectiveAdminForUser(userID, perms))
	req = app.withPermissionKeys(req, app.permissionKeysForUser(userID))
	data := injectPermsIfMissing(req, map[string]interface{}{}).(map[string]interface{})
	if got := data["CanModerate"]; got != true {
		t.Fatalf("media.moderate modular hauria de mostrar moderacio al menu, rebut %#v", got)
	}

	model := app.newModeracioScopeModel(&db.User{ID: userID}, perms, false)
	if !model.canModerateType("media_album") || !model.canModerateType("media_item") {
		t.Fatalf("media.moderate hauria d'autoritzar tipus media_album i media_item")
	}
}

func TestF334ScopedMunicipiModeratorDoesNotModerateOutsideScope(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-4-municipi-scoped")
	policy := &db.Politica{
		Nom:        "municipi-moderator-scoped",
		Permisos:   "{}",
		Descripcio: "",
	}
	policyID, err := database.SavePolitica(policy)
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica municipi scoped: %v", err)
	}
	if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
		PoliticaID:      policyID,
		PermKey:         permKeyTerritoriMunicipisMapesModerate,
		ScopeType:       string(ScopeMunicipi),
		ScopeID:         sql.NullInt64{Int64: 7, Valid: true},
		IncludeChildren: false,
	}); err != nil {
		t.Fatalf("no s'ha pogut crear grant municipi scoped: %v", err)
	}
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica municipi scoped: %v", err)
	}

	perms := app.getPermissionsForUser(userID)
	if app.hasPerm(perms, permModerate) {
		t.Fatalf("el test necessita usuari sense permModerate legacy")
	}
	model := app.newModeracioScopeModel(&db.User{ID: userID}, perms, false)
	if !model.canModerateType("municipi_mapa_version") {
		t.Fatalf("grant scoped hauria d'habilitar el tipus municipi_mapa_version")
	}
	if !app.HasPermission(userID, permKeyTerritoriMunicipisMapesModerate, PermissionTarget{MunicipiID: intPtr(7)}) {
		t.Fatalf("grant scoped hauria d'autoritzar municipi 7")
	}
	if app.HasPermission(userID, permKeyTerritoriMunicipisMapesModerate, PermissionTarget{MunicipiID: intPtr(8)}) {
		t.Fatalf("grant scoped no hauria d'autoritzar municipi 8")
	}
}

func TestF336WikiModerationUsesScopedObjectTargets(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-6-wiki-scoped")
	policy := &db.Politica{
		Nom:        "wiki-scoped-moderator",
		Permisos:   "{}",
		Descripcio: "",
	}
	policyID, err := database.SavePolitica(policy)
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica wiki scoped: %v", err)
	}
	grants := []db.PoliticaGrant{
		{
			PoliticaID:      policyID,
			PermKey:         permKeyTerritoriMunicipisEdit,
			ScopeType:       string(ScopeMunicipi),
			ScopeID:         sql.NullInt64{Int64: 7, Valid: true},
			IncludeChildren: false,
		},
		{
			PoliticaID:      policyID,
			PermKey:         permKeyDocumentalsArxiusEdit,
			ScopeType:       string(ScopeArxiu),
			ScopeID:         sql.NullInt64{Int64: 11, Valid: true},
			IncludeChildren: false,
		},
		{
			PoliticaID:      policyID,
			PermKey:         permKeyDocumentalsLlibresEdit,
			ScopeType:       string(ScopeLlibre),
			ScopeID:         sql.NullInt64{Int64: 13, Valid: true},
			IncludeChildren: false,
		},
	}
	for _, grant := range grants {
		grant := grant
		if _, err := database.SavePoliticaGrant(&grant); err != nil {
			t.Fatalf("no s'ha pogut crear grant wiki scoped: %v", err)
		}
	}
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica wiki scoped: %v", err)
	}

	user := &db.User{ID: userID}
	perms := app.getPermissionsForUser(userID)
	if app.hasPerm(perms, permModerate) {
		t.Fatalf("el test necessita usuari sense permModerate legacy")
	}
	if !app.canModerateWikiObject(user, perms, "municipi", 7) {
		t.Fatalf("territori.municipis.edit scoped hauria d'autoritzar wiki municipi dins ambit")
	}
	if app.canModerateWikiObject(user, perms, "municipi", 8) {
		t.Fatalf("territori.municipis.edit scoped no hauria d'autoritzar wiki municipi fora ambit")
	}
	if !app.canModerateWikiObject(user, perms, "arxiu", 11) {
		t.Fatalf("documentals.arxius.edit scoped hauria d'autoritzar wiki arxiu dins ambit")
	}
	if app.canModerateWikiObject(user, perms, "arxiu", 12) {
		t.Fatalf("documentals.arxius.edit scoped no hauria d'autoritzar wiki arxiu fora ambit")
	}
	if !app.canModerateWikiObject(user, perms, "llibre", 13) {
		t.Fatalf("documentals.llibres.edit scoped hauria d'autoritzar wiki llibre dins ambit")
	}
	if app.canModerateWikiObject(user, perms, "llibre", 14) {
		t.Fatalf("documentals.llibres.edit scoped no hauria d'autoritzar wiki llibre fora ambit")
	}
}

func TestF336WikiModerationUsesDomainGlobalKeys(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-6-wiki-domain")
	policy := &db.Politica{
		Nom:        "wiki-domain-moderator",
		Permisos:   "{}",
		Descripcio: "",
	}
	policyID, err := database.SavePolitica(policy)
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica wiki domain: %v", err)
	}
	for _, key := range []string{permKeyPersonesModerate, permKeyCognomsModerate, permKeyEventsModerate} {
		grant := &db.PoliticaGrant{
			PoliticaID: policyID,
			PermKey:    key,
			ScopeType:  string(ScopeGlobal),
		}
		if _, err := database.SavePoliticaGrant(grant); err != nil {
			t.Fatalf("no s'ha pogut crear grant %s: %v", key, err)
		}
	}
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica wiki domain: %v", err)
	}

	user := &db.User{ID: userID}
	perms := app.getPermissionsForUser(userID)
	if app.hasPerm(perms, permModerate) {
		t.Fatalf("el test necessita usuari sense permModerate legacy")
	}
	for _, objectType := range []string{"persona", "cognom", "event_historic"} {
		if !app.canModerateWikiObject(user, perms, objectType, 1) {
			t.Fatalf("key modular de domini hauria d'autoritzar wiki %s", objectType)
		}
	}
	if app.canModerateWikiObject(user, perms, "municipi", 1) {
		t.Fatalf("keys globals de persona/cognom/event no han d'autoritzar municipi")
	}
}

func TestF335AdminPlatformKeysDriveMenuFlags(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-5-platform-user")
	policy := &db.Politica{
		Nom:        "platform-users",
		Permisos:   "{}",
		Descripcio: "",
	}
	policyID, err := database.SavePolitica(policy)
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica platform: %v", err)
	}
	if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
		PoliticaID: policyID,
		PermKey:    permKeyAdminUsersManage,
		ScopeType:  string(ScopeGlobal),
	}); err != nil {
		t.Fatalf("no s'ha pogut crear grant admin.users.manage: %v", err)
	}
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica platform: %v", err)
	}

	perms := app.getPermissionsForUser(userID)
	if app.hasPerm(perms, permAdmin) || app.hasPerm(perms, permPolicies) {
		t.Fatalf("el test necessita usuari sense permisos legacy admin/policies")
	}
	req := httptest.NewRequest("GET", "/admin/usuaris", nil)
	req = app.withPermissions(req, perms)
	req = app.withEffectiveAdmin(req, app.effectiveAdminForUser(userID, perms))
	req = app.withPermissionKeys(req, app.permissionKeysForUser(userID))
	data := injectPermsIfMissing(req, map[string]interface{}{}).(map[string]interface{})
	if got := data["CanManageUsers"]; got != true {
		t.Fatalf("admin.users.manage hauria de mostrar gestio d'usuaris, rebut %#v", got)
	}
	if got := data["CanManagePolicies"]; got == true {
		t.Fatalf("admin.users.manage no hauria de mostrar politiques sense key, rebut %#v", got)
	}
	if !app.HasPermission(userID, permKeyAdminUsersManage, PermissionTarget{}) {
		t.Fatalf("admin.users.manage hauria d'autoritzar backend d'usuaris")
	}
	if app.HasPermission(userID, permKeyAdminPoliciesManage, PermissionTarget{}) {
		t.Fatalf("admin.users.manage no hauria d'autoritzar politiques")
	}
}
