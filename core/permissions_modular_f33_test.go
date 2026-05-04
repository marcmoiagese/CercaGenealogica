package core

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

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

	req := httptest.NewRequest("GET", "/territori/nivells", nil)
	req = app.withEffectiveAdmin(req, app.effectiveAdminForUser(userID))
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

func TestF3319LegacyJSONAdminFlagDoesNotGrantSnapshotAdmin(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-admin-flag")
	policy := &db.Politica{
		Nom:        "admin-json",
		Descripcio: "",
	}
	policyID, err := database.SavePolitica(policy)
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica admin JSON: %v", err)
	}
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica admin JSON: %v", err)
	}

	snap, err := app.buildPermissionSnapshot(userID)
	if err != nil {
		t.Fatalf("no s'ha pogut construir snapshot: %v", err)
	}
	if snap.isAdmin {
		t.Fatalf("admin=true legacy no hauria d'activar admin modular")
	}
	if app.HasPermission(userID, permKeyTerritoriNivellsEdit, PermissionTarget{PaisID: intPtr(1)}) {
		t.Fatalf("admin=true legacy no hauria de donar permisos modulars efectius")
	}
	if filter := app.buildListScopeFilter(userID, permKeyTerritoriNivellsView, ScopePais); filter.hasGlobal {
		t.Fatalf("admin=true legacy no hauria de donar filtre global")
	}
}

func TestF330ScopedTerritoryUserDoesNotBecomeGlobalAdmin(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-scoped")
	policy := &db.Politica{
		Nom:        "territori-scoped",
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

	if app.effectiveAdminForUser(userID) {
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
	req := httptest.NewRequest("GET", "/admin/moderacio/media", nil)
	req = app.withEffectiveAdmin(req, app.effectiveAdminForUser(userID))
	req = app.withPermissionKeys(req, app.permissionKeysForUser(userID))
	data := injectPermsIfMissing(req, map[string]interface{}{}).(map[string]interface{})
	if got := data["CanModerate"]; got != true {
		t.Fatalf("media.moderate modular hauria de mostrar moderacio al menu, rebut %#v", got)
	}

	model := app.newModeracioScopeModel(&db.User{ID: userID}, false)
	if !model.canModerateType("media_album") || !model.canModerateType("media_item") {
		t.Fatalf("media.moderate hauria d'autoritzar tipus media_album i media_item")
	}
}

func TestF334ScopedMunicipiModeratorDoesNotModerateOutsideScope(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-4-municipi-scoped")
	policy := &db.Politica{
		Nom:        "municipi-moderator-scoped",
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
	model := app.newModeracioScopeModel(&db.User{ID: userID}, false)
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
	if !app.canModerateWikiObject(user, "municipi", 7) {
		t.Fatalf("territori.municipis.edit scoped hauria d'autoritzar wiki municipi dins ambit")
	}
	if app.canModerateWikiObject(user, "municipi", 8) {
		t.Fatalf("territori.municipis.edit scoped no hauria d'autoritzar wiki municipi fora ambit")
	}
	if !app.canModerateWikiObject(user, "arxiu", 11) {
		t.Fatalf("documentals.arxius.edit scoped hauria d'autoritzar wiki arxiu dins ambit")
	}
	if app.canModerateWikiObject(user, "arxiu", 12) {
		t.Fatalf("documentals.arxius.edit scoped no hauria d'autoritzar wiki arxiu fora ambit")
	}
	if !app.canModerateWikiObject(user, "llibre", 13) {
		t.Fatalf("documentals.llibres.edit scoped hauria d'autoritzar wiki llibre dins ambit")
	}
	if app.canModerateWikiObject(user, "llibre", 14) {
		t.Fatalf("documentals.llibres.edit scoped no hauria d'autoritzar wiki llibre fora ambit")
	}
}

func TestF336WikiModerationUsesDomainGlobalKeys(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-6-wiki-domain")
	policy := &db.Politica{
		Nom:        "wiki-domain-moderator",
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
	for _, objectType := range []string{"persona", "cognom", "event_historic"} {
		if !app.canModerateWikiObject(user, objectType, 1) {
			t.Fatalf("key modular de domini hauria d'autoritzar wiki %s", objectType)
		}
	}
	if app.canModerateWikiObject(user, "municipi", 1) {
		t.Fatalf("keys globals de persona/cognom/event no han d'autoritzar municipi")
	}
}

func TestF336RWikiModerationDoesNotUseLegacyPermModerateFallback(t *testing.T) {
	app, _ := newF330PermissionsTestApp(t)
	user := &db.User{ID: 9901}

	for _, objectType := range []string{"municipi", "arxiu", "llibre", "persona", "cognom", "event_historic"} {
		if app.canModerateWikiObject(user, objectType, 1) {
			t.Fatalf("permModerate legacy pur no hauria d'autoritzar wiki %s", objectType)
		}
	}
}

func TestF336RWikiModerationKeepsAdminViaModularBridge(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-6r-admin")
	adminID := findF330PolicyID(t, database, "admin")
	if err := database.AddUserPolitica(userID, adminID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica admin: %v", err)
	}

	user := &db.User{ID: userID}
	if !app.canModerateWikiObject(user, "municipi", 123) {
		t.Fatalf("admin global hauria d'autoritzar wiki municipi via pont modular")
	}
	if !app.canModerateWikiObject(user, "persona", 123) {
		t.Fatalf("admin global hauria d'autoritzar wiki persona via pont modular")
	}
}

func TestF337LegacyPermModerateDoesNotGrantDocumentalOrMediaModeration(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-7-legacy-moderate")
	user := &db.User{ID: userID}

	if app.canModerateModular(user) {
		t.Fatalf("permModerate legacy pur no hauria d'obrir moderacio modular")
	}
	if isAdmin, isModerator := app.mediaUserRoles(httptest.NewRequest("GET", "/media/albums", nil), user); isAdmin || isModerator {
		t.Fatalf("permModerate legacy pur no hauria de donar privilegi media, admin=%v moderator=%v", isAdmin, isModerator)
	}
}

func TestF337MediaModerationUsesMediaModerateKeyAndAdminBridge(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-7-media-moderator")
	policy := &db.Politica{Nom: "f33-7-media", Descripcio: ""}
	policyID, err := database.SavePolitica(policy)
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica media: %v", err)
	}
	if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
		PoliticaID: policyID,
		PermKey:    permKeyMediaModerate,
		ScopeType:  string(ScopeGlobal),
	}); err != nil {
		t.Fatalf("no s'ha pogut crear grant media.moderate: %v", err)
	}
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica media: %v", err)
	}

	user := &db.User{ID: userID}
	isAdmin, isModerator := app.mediaUserRoles(httptest.NewRequest("GET", "/media/albums", nil), user)
	if isAdmin {
		t.Fatalf("media.moderate no ha de convertir l'usuari en admin")
	}
	if !isModerator {
		t.Fatalf("media.moderate modular hauria de donar privilegi media")
	}

	adminUserID := createF330User(t, database, "f33-7-media-admin")
	adminID := findF330PolicyID(t, database, "admin")
	if err := database.AddUserPolitica(adminUserID, adminID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica admin: %v", err)
	}
	isAdmin, isModerator = app.mediaUserRoles(httptest.NewRequest("GET", "/media/albums", nil), &db.User{ID: adminUserID})
	if !isAdmin || !isModerator {
		t.Fatalf("admin global hauria de mantenir privilegis media via bridge modular, admin=%v moderator=%v", isAdmin, isModerator)
	}
}

func TestF337DocumentalScopedModerationUsesRegistreKey(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-7-registre-scoped")
	policy := &db.Politica{Nom: "f33-7-registre", Descripcio: ""}
	policyID, err := database.SavePolitica(policy)
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica registre: %v", err)
	}
	if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
		PoliticaID: policyID,
		PermKey:    permKeyDocumentalsRegistresEdit,
		ScopeType:  string(ScopeLlibre),
		ScopeID:    sql.NullInt64{Int64: 11, Valid: true},
	}); err != nil {
		t.Fatalf("no s'ha pogut crear grant registre scoped: %v", err)
	}
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica registre: %v", err)
	}

	user := &db.User{ID: userID}
	if !app.canModerateModular(user) {
		t.Fatalf("documentals.registres.edit scoped hauria d'obrir moderacio modular")
	}
	if !app.HasPermission(userID, permKeyDocumentalsRegistresEdit, PermissionTarget{LlibreID: intPtr(11)}) {
		t.Fatalf("grant scoped de registre hauria d'autoritzar el llibre permÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¨s")
	}
	if app.HasPermission(userID, permKeyDocumentalsRegistresEdit, PermissionTarget{LlibreID: intPtr(12)}) {
		t.Fatalf("grant scoped de registre no hauria d'autoritzar un altre llibre")
	}
}

func TestF338TerritoriPublicDoesNotUseLegacyPermModerate(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-8-legacy-moderate")
	user := &db.User{ID: userID}
	target := PermissionTarget{MunicipiID: intPtr(7)}

	if app.canModerateMunicipiPublic(user, target) {
		t.Fatalf("permModerate legacy pur no hauria d'autoritzar territori public")
	}
	if app.canEditMunicipiPublic(user, target) {
		t.Fatalf("permTerritory legacy pur no hauria d'autoritzar edit/rebuild de municipi")
	}
	if app.canModerateModular(user) {
		t.Fatalf("permisos legacy purs no haurien d'obrir moderacio modular")
	}
}

func TestF338TerritoriPublicScopedModerationStaysInsideMunicipi(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-8-historia-scoped")
	policy := &db.Politica{Nom: "f33-8-historia", Descripcio: ""}
	policyID, err := database.SavePolitica(policy)
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica historia: %v", err)
	}
	if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
		PoliticaID: policyID,
		PermKey:    permKeyTerritoriMunicipisHistoriaModerate,
		ScopeType:  string(ScopeMunicipi),
		ScopeID:    sql.NullInt64{Int64: 7, Valid: true},
	}); err != nil {
		t.Fatalf("no s'ha pogut crear grant historia scoped: %v", err)
	}
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica historia: %v", err)
	}

	user := &db.User{ID: userID}
	if !app.canModerateMunicipiPublic(user, PermissionTarget{MunicipiID: intPtr(7)}) {
		t.Fatalf("historia moderate scoped hauria d'autoritzar el municipi permes")
	}
	if app.canModerateMunicipiPublic(user, PermissionTarget{MunicipiID: intPtr(8)}) {
		t.Fatalf("historia moderate scoped no hauria d'autoritzar un altre municipi")
	}
	if app.canEditMunicipiPublic(user, PermissionTarget{MunicipiID: intPtr(7)}) {
		t.Fatalf("historia moderate no ha de convertir-se en edit/rebuild de municipi")
	}
}

func TestF338TerritoriPublicAdminBridge(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-8-admin")
	adminID := findF330PolicyID(t, database, "admin")
	if err := database.AddUserPolitica(userID, adminID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica admin: %v", err)
	}

	user := &db.User{ID: userID}
	target := PermissionTarget{MunicipiID: intPtr(99)}
	if !app.canModerateMunicipiPublic(user, target) {
		t.Fatalf("admin global hauria de moderar territori public via bridge modular")
	}
	if !app.canEditMunicipiPublic(user, target) {
		t.Fatalf("admin global hauria d'editar/rebuild municipi via bridge modular")
	}
	if !app.canEditNivellPublic(user, PermissionTarget{PaisID: intPtr(1)}) {
		t.Fatalf("admin global hauria d'editar nivell via bridge modular")
	}
}

func TestF339PersonesCognomsEventsDoNotUseLegacyPermModerate(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-9-legacy-moderate")
	user := &db.User{ID: userID}

	if app.canModeratePersonesPublic(user) {
		t.Fatalf("permModerate legacy pur no hauria d'autoritzar persones publiques")
	}
	if app.canModerateCognomsPublic(user) {
		t.Fatalf("permModerate legacy pur no hauria d'autoritzar cognoms publics")
	}
	if app.canModerateEventHistoricPublic(user, 0) {
		t.Fatalf("permModerate legacy pur no hauria d'autoritzar events historics publics")
	}
	if app.canModerateModular(user) {
		t.Fatalf("permisos legacy purs no haurien d'obrir moderacio modular")
	}
}

func TestF339DomainGlobalKeysEnablePublicModeration(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-9-domain-global")
	policy := &db.Politica{Nom: "f33-9-domain-global", Descripcio: ""}
	policyID, err := database.SavePolitica(policy)
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica: %v", err)
	}
	for _, key := range []string{permKeyPersonesModerate, permKeyCognomsModerate, permKeyEventsModerate} {
		if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
			PoliticaID: policyID,
			PermKey:    key,
			ScopeType:  string(ScopeGlobal),
		}); err != nil {
			t.Fatalf("no s'ha pogut crear grant %s: %v", key, err)
		}
	}
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica: %v", err)
	}

	user := &db.User{ID: userID}
	if !app.canModeratePersonesPublic(user) {
		t.Fatalf("persones.moderate global hauria d'autoritzar persones publiques")
	}
	if !app.canModerateCognomsPublic(user) {
		t.Fatalf("cognoms.moderate global hauria d'autoritzar cognoms publics")
	}
	if !app.canModerateEventHistoricPublic(user, 0) {
		t.Fatalf("events.moderate global hauria d'autoritzar events publics")
	}
}

func TestF339EventHistoricScopedModerationUsesImpactTarget(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-9-event-scoped")
	allowedMunID := createF339Municipi(t, database, "allowed")
	blockedMunID := createF339Municipi(t, database, "blocked")
	allowedEventID := createF339EventWithImpact(t, database, userID, allowedMunID)
	blockedEventID := createF339EventWithImpact(t, database, userID, blockedMunID)
	policy := &db.Politica{Nom: "f33-9-event-scoped", Descripcio: ""}
	policyID, err := database.SavePolitica(policy)
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica events scoped: %v", err)
	}
	if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
		PoliticaID: policyID,
		PermKey:    permKeyEventsModerate,
		ScopeType:  string(ScopeMunicipi),
		ScopeID:    sql.NullInt64{Int64: int64(allowedMunID), Valid: true},
	}); err != nil {
		t.Fatalf("no s'ha pogut crear grant events scoped: %v", err)
	}
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica events scoped: %v", err)
	}

	user := &db.User{ID: userID}
	if !app.canModerateEventHistoricPublic(user, allowedEventID) {
		t.Fatalf("events.moderate scoped hauria d'autoritzar l'event amb impacte dins municipi permes")
	}
	if app.canModerateEventHistoricPublic(user, blockedEventID) {
		t.Fatalf("events.moderate scoped no hauria d'autoritzar l'event fora de municipi")
	}
}

func TestF339PublicModerationKeepsAdminBridge(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-9-admin")
	adminID := findF330PolicyID(t, database, "admin")
	if err := database.AddUserPolitica(userID, adminID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica admin: %v", err)
	}

	user := &db.User{ID: userID}
	if !app.canModeratePersonesPublic(user) {
		t.Fatalf("admin global hauria de moderar persones via bridge modular")
	}
	if !app.canModerateCognomsPublic(user) {
		t.Fatalf("admin global hauria de moderar cognoms via bridge modular")
	}
	if !app.canModerateEventHistoricPublic(user, 0) {
		t.Fatalf("admin global hauria de moderar events via bridge modular")
	}
}

func createF339Municipi(t *testing.T, database db.DB, suffix string) int {
	t.Helper()
	id, err := database.CreateMunicipi(&db.Municipi{
		Nom:            fmt.Sprintf("Municipi F33-9 %s", suffix),
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateMunicipi ha fallat: %v", err)
	}
	return id
}

func createF339EventWithImpact(t *testing.T, database db.DB, userID, municipiID int) int {
	t.Helper()
	event := &db.EventHistoric{
		Titol:            fmt.Sprintf("Event F33-9 %d", municipiID),
		Slug:             fmt.Sprintf("event-f33-9-%d", municipiID),
		Tipus:            "revolta",
		Resum:            "resum",
		Descripcio:       "desc",
		CreatedBy:        sql.NullInt64{Int64: int64(userID), Valid: true},
		ModerationStatus: "pendent",
	}
	eventID, err := database.CreateEventHistoric(event)
	if err != nil {
		t.Fatalf("CreateEventHistoric ha fallat: %v", err)
	}
	impact := db.EventHistoricImpact{
		EventID:      eventID,
		ScopeType:    "municipi",
		ScopeID:      municipiID,
		ImpacteTipus: "directe",
		Intensitat:   3,
		CreatedBy:    sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	if err := database.ReplaceEventImpacts(eventID, []db.EventHistoricImpact{impact}); err != nil {
		t.Fatalf("ReplaceEventImpacts ha fallat: %v", err)
	}
	return eventID
}

func TestF3310LegacyPermModerateDoesNotGrantResidualModeration(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-10-legacy-moderate")
	user := &db.User{ID: userID}

	if app.canModerateAllModular(user) {
		t.Fatalf("permModerate legacy pur no hauria d'autoritzar canModerateAll residual")
	}
	if app.newModeracioScopeModel(user, app.canModerateAllModular(user)).canModerateAnything() {
		t.Fatalf("permModerate legacy pur no hauria d'obrir moderacio residual")
	}
	if app.canModerateMap(user, 7) {
		t.Fatalf("permModerate legacy pur no hauria d'autoritzar moderacio de mapes")
	}
}

func TestF3310ResidualModerationKeepsAdminBridge(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-10-admin")
	adminID := findF330PolicyID(t, database, "admin")
	if err := database.AddUserPolitica(userID, adminID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica admin: %v", err)
	}

	user := &db.User{ID: userID}
	if !app.canModerateAllModular(user) {
		t.Fatalf("admin global hauria de tenir canModerateAll via bridge modular")
	}
	if !app.canModerateMap(user, 7) {
		t.Fatalf("admin global hauria de moderar mapes via bridge modular")
	}
}

func TestF3310GlobalModularGrantsCanModerateAllResidual(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-10-global-modular")
	policy := &db.Politica{Nom: "f33-10-all-moderation", Descripcio: ""}
	policyID, err := database.SavePolitica(policy)
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica global: %v", err)
	}
	seen := map[string]bool{}
	for _, objType := range moderacioBulkAllowedTypes {
		spec := moderacioTypeSpecs[objType]
		if seen[spec.PermKey] {
			continue
		}
		seen[spec.PermKey] = true
		if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
			PoliticaID: policyID,
			PermKey:    spec.PermKey,
			ScopeType:  string(ScopeGlobal),
		}); err != nil {
			t.Fatalf("no s'ha pogut crear grant %s: %v", spec.PermKey, err)
		}
	}
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica global: %v", err)
	}

	user := &db.User{ID: userID}
	if !app.canModerateAllModular(user) {
		t.Fatalf("grants globals de tots els tipus haurien d'autoritzar canModerateAll residual")
	}
}

func TestF3310MapModerationRespectsMunicipiScope(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-10-mapes-scoped")
	allowedMunID := createF339Municipi(t, database, "mapes-allowed")
	blockedMunID := createF339Municipi(t, database, "mapes-blocked")
	policy := &db.Politica{Nom: "f33-10-mapes-scoped", Descripcio: ""}
	policyID, err := database.SavePolitica(policy)
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica mapes scoped: %v", err)
	}
	if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
		PoliticaID: policyID,
		PermKey:    permKeyTerritoriMunicipisMapesModerate,
		ScopeType:  string(ScopeMunicipi),
		ScopeID:    sql.NullInt64{Int64: int64(allowedMunID), Valid: true},
	}); err != nil {
		t.Fatalf("no s'ha pogut crear grant mapes scoped: %v", err)
	}
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica mapes scoped: %v", err)
	}

	user := &db.User{ID: userID}
	if !app.canModerateMap(user, allowedMunID) {
		t.Fatalf("mapes scoped hauria d'autoritzar el municipi permes")
	}
	if app.canModerateMap(user, blockedMunID) {
		t.Fatalf("mapes scoped no hauria d'autoritzar un altre municipi")
	}
	if app.canModerateAllModular(user) {
		t.Fatalf("mapes scoped no hauria de convertir-se en canModerateAll")
	}
}

func TestF335AdminPlatformKeysDriveMenuFlags(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-5-platform-user")
	policy := &db.Politica{
		Nom:        "platform-users",
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
	req := httptest.NewRequest("GET", "/admin/usuaris", nil)
	req = app.withEffectiveAdmin(req, app.effectiveAdminForUser(userID))
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

func TestF3311LegacyCanModerateDoesNotSetGlobalTemplateFlag(t *testing.T) {
	app := &App{}
	req := httptest.NewRequest("GET", "/admin/moderacio", nil)
	req = app.withEffectiveAdmin(req, false)
	req = app.withPermissionKeys(req, map[string]bool{})

	data := injectPermsIfMissing(req, map[string]interface{}{}).(map[string]interface{})
	if got := data["CanModerate"]; got == true {
		t.Fatalf("can_moderate legacy pur no hauria d'activar CanModerate global, rebut %#v", got)
	}
}

func TestF3311GlobalTemplateModerationKeepsAdminAndModularKeys(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)

	adminUserID := createF330User(t, database, "f33-11-template-admin")
	adminID := findF330PolicyID(t, database, "admin")
	if err := database.AddUserPolitica(adminUserID, adminID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica admin: %v", err)
	}
	adminReq := httptest.NewRequest("GET", "/admin/moderacio", nil)
	adminReq = app.withEffectiveAdmin(adminReq, app.effectiveAdminForUser(adminUserID))
	adminReq = app.withPermissionKeys(adminReq, app.permissionKeysForUser(adminUserID))
	adminData := injectPermsIfMissing(adminReq, map[string]interface{}{}).(map[string]interface{})
	if got := adminData["CanModerate"]; got != true {
		t.Fatalf("admin global hauria d'activar CanModerate via bridge modular, rebut %#v", got)
	}

	moderatorUserID := createF330User(t, database, "f33-11-template-modular")
	policyID, err := database.SavePolitica(&db.Politica{Nom: "f33-11-media-moderator", Descripcio: ""})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica modular: %v", err)
	}
	if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
		PoliticaID: policyID,
		PermKey:    permKeyMediaModerate,
		ScopeType:  string(ScopeGlobal),
	}); err != nil {
		t.Fatalf("no s'ha pogut crear grant media.moderate: %v", err)
	}
	if err := database.AddUserPolitica(moderatorUserID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica modular: %v", err)
	}
	modReq := httptest.NewRequest("GET", "/admin/moderacio/media", nil)
	modReq = app.withEffectiveAdmin(modReq, app.effectiveAdminForUser(moderatorUserID))
	modReq = app.withPermissionKeys(modReq, app.permissionKeysForUser(moderatorUserID))
	modData := injectPermsIfMissing(modReq, map[string]interface{}{}).(map[string]interface{})
	if got := modData["CanModerate"]; got != true {
		t.Fatalf("media.moderate modular hauria d'activar CanModerate global, rebut %#v", got)
	}
}

func TestF3311DashboardRoleSetDoesNotUseLegacyCanModerate(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-11-dashboard-legacy")
	policyID, err := database.SavePolitica(&db.Politica{
		Nom:        "",
		Descripcio: "legacy moderate only",
	})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica legacy: %v", err)
	}
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica legacy: %v", err)
	}
	if roles := app.dashboardUserRoleSet(userID); roles["moderador"] {
		t.Fatalf("dashboard no hauria d'afegir rol moderador per CanModerate legacy pur: %#v", roles)
	}
}

func TestF3311PermTerritoryLegacyDoesNotGrantTerritoryOrArchivesUI(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-11-legacy-territory")
	user := &db.User{ID: userID}
	target := PermissionTarget{MunicipiID: intPtr(7)}

	if app.canManageTerritoryTarget(user, target) {
		t.Fatalf("permTerritory legacy pur no hauria d'autoritzar gestio territorial scoped")
	}
	if app.canManageAnyTerritoryModular(user) {
		t.Fatalf("permTerritory legacy pur no hauria d'activar UI territorial en arxius")
	}
}

func TestF3311ModularTerritoryScopedKeepsAccessWithinScope(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-11-territory-scoped")
	policyID, err := database.SavePolitica(&db.Politica{Nom: "f33-11-territory", Descripcio: ""})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica territorial: %v", err)
	}
	if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
		PoliticaID: policyID,
		PermKey:    permKeyTerritoriMunicipisEdit,
		ScopeType:  string(ScopeMunicipi),
		ScopeID:    sql.NullInt64{Int64: 7, Valid: true},
	}); err != nil {
		t.Fatalf("no s'ha pogut crear grant territorial: %v", err)
	}
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica territorial: %v", err)
	}

	user := &db.User{ID: userID}
	if !app.canManageTerritoryTarget(user, PermissionTarget{MunicipiID: intPtr(7)}) {
		t.Fatalf("territori.municipis.edit scoped hauria d'autoritzar gestio dins ambit")
	}
	if app.canManageTerritoryTarget(user, PermissionTarget{MunicipiID: intPtr(8)}) {
		t.Fatalf("territori.municipis.edit scoped no hauria d'autoritzar gestio fora ambit")
	}
	if !app.canManageAnyTerritoryModular(user) {
		t.Fatalf("un grant territorial modular hauria de mantenir visible la UI territorial relacionada")
	}
}

func TestF3311RLegacyCanManageTerritoryDoesNotSetTemplateTerritoryFlags(t *testing.T) {
	app := &App{}
	req := httptest.NewRequest("GET", "/territori", nil)
	req = app.withEffectiveAdmin(req, false)
	req = app.withPermissionKeys(req, map[string]bool{})

	data := injectPermsIfMissing(req, map[string]interface{}{}).(map[string]interface{})
	if got := data["CanManageTerritory"]; got == true {
		t.Fatalf("CanManageTerritory legacy pur no hauria d'activar CanManageTerritory, rebut %#v", got)
	}
	if got := data["CanViewNivells"]; got == true {
		t.Fatalf("CanManageTerritory legacy pur no hauria d'activar CanViewNivells, rebut %#v", got)
	}
	if got := data["CanViewMunicipis"]; got == true {
		t.Fatalf("CanManageTerritory legacy pur no hauria d'activar CanViewMunicipis, rebut %#v", got)
	}
}

func TestF3311RTemplateTerritoryFlagsKeepAdminBridge(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-11r-template-admin")
	adminID := findF330PolicyID(t, database, "admin")
	if err := database.AddUserPolitica(userID, adminID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica admin: %v", err)
	}

	req := httptest.NewRequest("GET", "/territori", nil)
	req = app.withEffectiveAdmin(req, app.effectiveAdminForUser(userID))
	req = app.withPermissionKeys(req, app.permissionKeysForUser(userID))

	data := injectPermsIfMissing(req, map[string]interface{}{}).(map[string]interface{})
	for _, key := range []string{"CanManageTerritory", "CanViewNivells", "CanViewMunicipis"} {
		if got := data[key]; got != true {
			t.Fatalf("admin global hauria d'activar %s via bridge modular, rebut %#v", key, got)
		}
	}
}

func TestF3311RTemplateTerritoryFlagsUseModularKeys(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-11r-template-territory")
	policyID, err := database.SavePolitica(&db.Politica{Nom: "f33-11r-territory", Descripcio: ""})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica territorial: %v", err)
	}
	for _, permKey := range []string{permKeyTerritoriNivellsView, permKeyTerritoriMunicipisView, permKeyTerritoriMunicipisEdit} {
		if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
			PoliticaID: policyID,
			PermKey:    permKey,
			ScopeType:  string(ScopeGlobal),
		}); err != nil {
			t.Fatalf("no s'ha pogut crear grant %s: %v", permKey, err)
		}
	}
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica territorial: %v", err)
	}

	req := httptest.NewRequest("GET", "/territori", nil)
	req = app.withEffectiveAdmin(req, app.effectiveAdminForUser(userID))
	req = app.withPermissionKeys(req, app.permissionKeysForUser(userID))

	data := injectPermsIfMissing(req, map[string]interface{}{}).(map[string]interface{})
	if got := data["CanViewNivells"]; got != true {
		t.Fatalf("territori.nivells.view hauria d'activar CanViewNivells, rebut %#v", got)
	}
	if got := data["CanViewMunicipis"]; got != true {
		t.Fatalf("territori.municipis.view hauria d'activar CanViewMunicipis, rebut %#v", got)
	}
	if got := data["CanManageTerritory"]; got != true {
		t.Fatalf("territori.municipis.edit hauria d'activar CanManageTerritory, rebut %#v", got)
	}
}

func TestF3312LegacyCanManageArchivesDoesNotSetTemplateDocumentalFlags(t *testing.T) {
	app := &App{}
	req := httptest.NewRequest("GET", "/documentals", nil)
	req = app.withEffectiveAdmin(req, false)
	req = app.withPermissionKeys(req, map[string]bool{})

	data := injectPermsIfMissing(req, map[string]interface{}{}).(map[string]interface{})
	for _, key := range []string{"CanManageArxius", "CanViewArxius", "CanViewLlibres", "CanImportTemplates", "CanIndexRegistres", "CanBulkIndex"} {
		if got := data[key]; got == true {
			t.Fatalf("CanManageArchives legacy pur no hauria d'activar %s, rebut %#v", key, got)
		}
	}
}

func TestF3312TemplateDocumentalFlagsKeepAdminBridge(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-12-template-admin")
	adminID := findF330PolicyID(t, database, "admin")
	if err := database.AddUserPolitica(userID, adminID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica admin: %v", err)
	}

	req := httptest.NewRequest("GET", "/documentals", nil)
	req = app.withEffectiveAdmin(req, app.effectiveAdminForUser(userID))
	req = app.withPermissionKeys(req, app.permissionKeysForUser(userID))

	data := injectPermsIfMissing(req, map[string]interface{}{}).(map[string]interface{})
	for _, key := range []string{"CanManageArxius", "CanViewArxius", "CanViewLlibres", "CanImportTemplates", "CanIndexRegistres", "CanBulkIndex"} {
		if got := data[key]; got != true {
			t.Fatalf("admin global hauria d'activar %s via bridge modular, rebut %#v", key, got)
		}
	}
}

func TestF3312DocumentalScopedKeysStayInScope(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-12-documental-scoped")
	policyID, err := database.SavePolitica(&db.Politica{Nom: "f33-12-documental", Descripcio: ""})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica documental: %v", err)
	}
	grants := []db.PoliticaGrant{
		{PoliticaID: policyID, PermKey: permKeyDocumentalsArxiusEdit, ScopeType: string(ScopeArxiu), ScopeID: sql.NullInt64{Int64: 12, Valid: true}},
		{PoliticaID: policyID, PermKey: permKeyDocumentalsLlibresEdit, ScopeType: string(ScopeLlibre), ScopeID: sql.NullInt64{Int64: 34, Valid: true}},
	}
	for _, grant := range grants {
		grant := grant
		if _, err := database.SavePoliticaGrant(&grant); err != nil {
			t.Fatalf("no s'ha pogut crear grant documental scoped: %v", err)
		}
	}
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica documental: %v", err)
	}

	user := &db.User{ID: userID}
	if !app.CanManageArxius(user) {
		t.Fatalf("grant documental modular hauria d'activar CanManageArxius helper")
	}
	if !app.HasPermission(userID, permKeyDocumentalsArxiusEdit, PermissionTarget{ArxiuID: intPtr(12)}) {
		t.Fatalf("documentals.arxius.edit scoped hauria de permetre l'arxiu 12")
	}
	if app.HasPermission(userID, permKeyDocumentalsArxiusEdit, PermissionTarget{ArxiuID: intPtr(13)}) {
		t.Fatalf("documentals.arxius.edit scoped no hauria de permetre l'arxiu 13")
	}
	if !app.HasPermission(userID, permKeyDocumentalsLlibresEdit, PermissionTarget{LlibreID: intPtr(34)}) {
		t.Fatalf("documentals.llibres.edit scoped hauria de permetre el llibre 34")
	}
	if app.HasPermission(userID, permKeyDocumentalsLlibresEdit, PermissionTarget{LlibreID: intPtr(35)}) {
		t.Fatalf("documentals.llibres.edit scoped no hauria de permetre el llibre 35")
	}
}

func TestF3313LegacyCanManagePoliciesDoesNotSetTemplateFlag(t *testing.T) {
	app := &App{}
	req := httptest.NewRequest("GET", "/persones", nil)
	req = app.withEffectiveAdmin(req, false)
	req = app.withPermissionKeys(req, map[string]bool{})

	data := injectPermsIfMissing(req, map[string]interface{}{}).(map[string]interface{})
	if got := data["CanManagePolicies"]; got == true {
		t.Fatalf("CanManagePolicies legacy pur no hauria d'activar el flag de template, rebut %#v", got)
	}
}

func TestF3313TemplatePoliciesFlagUsesAdminBridgeAndModularKey(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)

	adminUserID := createF330User(t, database, "f33-13-template-admin")
	adminID := findF330PolicyID(t, database, "admin")
	if err := database.AddUserPolitica(adminUserID, adminID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica admin: %v", err)
	}
	adminReq := httptest.NewRequest("GET", "/admin/politiques", nil)
	adminReq = app.withEffectiveAdmin(adminReq, app.effectiveAdminForUser(adminUserID))
	adminReq = app.withPermissionKeys(adminReq, app.permissionKeysForUser(adminUserID))
	adminData := injectPermsIfMissing(adminReq, map[string]interface{}{}).(map[string]interface{})
	if got := adminData["CanManagePolicies"]; got != true {
		t.Fatalf("admin global hauria d'activar CanManagePolicies via bridge, rebut %#v", got)
	}

	policyUserID := createF330User(t, database, "f33-13-template-policy")
	policyID, err := database.SavePolitica(&db.Politica{Nom: "f33-13-policy-manage", Descripcio: ""})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica modular policies: %v", err)
	}
	if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
		PoliticaID: policyID,
		PermKey:    permKeyAdminPoliciesManage,
		ScopeType:  string(ScopeGlobal),
	}); err != nil {
		t.Fatalf("no s'ha pogut crear grant admin.policies.manage: %v", err)
	}
	if err := database.AddUserPolitica(policyUserID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica modular policies: %v", err)
	}
	policyReq := httptest.NewRequest("GET", "/admin/politiques", nil)
	policyReq = app.withEffectiveAdmin(policyReq, app.effectiveAdminForUser(policyUserID))
	policyReq = app.withPermissionKeys(policyReq, map[string]bool{permKeyAdminPoliciesManage: true})
	policyData := injectPermsIfMissing(policyReq, map[string]interface{}{}).(map[string]interface{})
	if got := policyData["CanManagePolicies"]; got != true {
		t.Fatalf("admin.policies.manage hauria d'activar CanManagePolicies, rebut %#v", got)
	}
}

func TestF3313CanManagePoliciesModularIgnoresLegacyPure(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)

	legacyUserID := createF330User(t, database, "f33-13-legacy-policies")
	legacyPolicyID, err := database.SavePolitica(&db.Politica{
		Nom:        "f33-13-legacy-policies",
		Descripcio: "",
	})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica legacy policies: %v", err)
	}
	if err := database.AddUserPolitica(legacyUserID, legacyPolicyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica legacy policies: %v", err)
	}
	if app.canManagePoliciesModular(&db.User{ID: legacyUserID}) {
		t.Fatalf("CanManagePolicies legacy pur no hauria de passar canManagePoliciesModular")
	}

	modularUserID := createF330User(t, database, "f33-13-modular-policies")
	modularPolicyID, err := database.SavePolitica(&db.Politica{Nom: "f33-13-modular-policies", Descripcio: ""})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica modular policies: %v", err)
	}
	if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
		PoliticaID: modularPolicyID,
		PermKey:    permKeyAdminPoliciesManage,
		ScopeType:  string(ScopeGlobal),
	}); err != nil {
		t.Fatalf("no s'ha pogut crear grant modular policies: %v", err)
	}
	if err := database.AddUserPolitica(modularUserID, modularPolicyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica modular policies: %v", err)
	}
	if !app.canManagePoliciesModular(&db.User{ID: modularUserID}) {
		t.Fatalf("admin.policies.manage hauria de passar canManagePoliciesModular")
	}
}

func TestF3313PoliciesSuggestGuardsUseModularKey(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)

	noPermUserID := createF330User(t, database, "f33-13-suggest-none")
	noPermSession := createF3313SessionCookie(t, database, noPermUserID, "sess_f33_13_none")
	for name, handler := range f3313SuggestHandlers(app) {
		rr := runF3313SuggestHandler(handler, noPermSession)
		if rr.Code != http.StatusForbidden {
			t.Fatalf("%s sense permis esperava 403, rebut %d", name, rr.Code)
		}
	}

	legacyUserID := createF330User(t, database, "f33-13-suggest-legacy")
	legacyPolicyID, err := database.SavePolitica(&db.Politica{
		Nom:        "f33-13-suggest-legacy",
		Descripcio: "",
	})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica legacy suggest: %v", err)
	}
	if err := database.AddUserPolitica(legacyUserID, legacyPolicyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica legacy suggest: %v", err)
	}
	legacySession := createF3313SessionCookie(t, database, legacyUserID, "sess_f33_13_legacy")
	for name, handler := range f3313SuggestHandlers(app) {
		rr := runF3313SuggestHandler(handler, legacySession)
		if rr.Code != http.StatusForbidden {
			t.Fatalf("%s amb legacy pur esperava 403, rebut %d", name, rr.Code)
		}
	}

	modularUserID := createF330User(t, database, "f33-13-suggest-modular")
	modularPolicyID, err := database.SavePolitica(&db.Politica{Nom: "f33-13-suggest-modular", Descripcio: ""})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica modular suggest: %v", err)
	}
	if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
		PoliticaID: modularPolicyID,
		PermKey:    permKeyAdminPoliciesManage,
		ScopeType:  string(ScopeGlobal),
	}); err != nil {
		t.Fatalf("no s'ha pogut crear grant modular suggest: %v", err)
	}
	if err := database.AddUserPolitica(modularUserID, modularPolicyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica modular suggest: %v", err)
	}
	modularSession := createF3313SessionCookie(t, database, modularUserID, "sess_f33_13_modular")
	for name, handler := range f3313SuggestHandlers(app) {
		rr := runF3313SuggestHandler(handler, modularSession)
		if rr.Code == http.StatusForbidden {
			t.Fatalf("%s amb admin.policies.manage no hauria de retornar 403", name)
		}
	}

	adminUserID := createF330User(t, database, "f33-13-suggest-admin")
	adminID := findF330PolicyID(t, database, "admin")
	if err := database.AddUserPolitica(adminUserID, adminID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica admin suggest: %v", err)
	}
	adminSession := createF3313SessionCookie(t, database, adminUserID, "sess_f33_13_admin")
	for name, handler := range f3313SuggestHandlers(app) {
		rr := runF3313SuggestHandler(handler, adminSession)
		if rr.Code == http.StatusForbidden {
			t.Fatalf("%s amb admin global no hauria de retornar 403", name)
		}
	}
}

func TestF3314LegacyUsersAndEclesiaDoNotSetTemplateFlags(t *testing.T) {
	app := &App{}
	req := httptest.NewRequest("GET", "/admin", nil)
	req = app.withEffectiveAdmin(req, false)
	req = app.withPermissionKeys(req, map[string]bool{})

	data := injectPermsIfMissing(req, map[string]interface{}{}).(map[string]interface{})
	if got := data["CanManageUsers"]; got == true {
		t.Fatalf("CanManageUsers legacy pur no hauria d'activar el flag de template, rebut %#v", got)
	}
	if got := data["CanManageEclesia"]; got == true {
		t.Fatalf("CanManageEclesia legacy pur no hauria d'activar el flag de template, rebut %#v", got)
	}
	if got := data["CanViewEcles"]; got == true {
		t.Fatalf("CanManageEclesia legacy pur no hauria d'activar CanViewEcles, rebut %#v", got)
	}
}

func TestF3314TemplateFlagsUseAdminAndModularKeys(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)

	adminUserID := createF330User(t, database, "f33-14-template-admin")
	adminID := findF330PolicyID(t, database, "admin")
	if err := database.AddUserPolitica(adminUserID, adminID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica admin F33-14: %v", err)
	}
	adminReq := httptest.NewRequest("GET", "/admin", nil)
	adminReq = app.withEffectiveAdmin(adminReq, app.effectiveAdminForUser(adminUserID))
	adminReq = app.withPermissionKeys(adminReq, app.permissionKeysForUser(adminUserID))
	adminData := injectPermsIfMissing(adminReq, map[string]interface{}{}).(map[string]interface{})
	for _, key := range []string{"CanManageUsers", "CanManageEclesia", "CanViewEcles"} {
		if got := adminData[key]; got != true {
			t.Fatalf("admin global hauria d'activar %s via bridge, rebut %#v", key, got)
		}
	}

	req := httptest.NewRequest("GET", "/admin", nil)
	req = app.withEffectiveAdmin(req, false)
	req = app.withPermissionKeys(req, map[string]bool{
		permKeyAdminUsersManage:         true,
		permKeyTerritoriEclesEdit:       true,
		permKeyTerritoriEclesView:       true,
		permKeyTerritoriEclesImportJSON: true,
	})
	data := injectPermsIfMissing(req, map[string]interface{}{}).(map[string]interface{})
	if got := data["CanManageUsers"]; got != true {
		t.Fatalf("admin.users.manage hauria d'activar CanManageUsers, rebut %#v", got)
	}
	if got := data["CanManageEclesia"]; got != true {
		t.Fatalf("territori.eclesiastic.edit hauria d'activar CanManageEclesia, rebut %#v", got)
	}
	if got := data["CanViewEcles"]; got != true {
		t.Fatalf("claus eclesiastiques modulars haurien d'activar CanViewEcles, rebut %#v", got)
	}
}

func TestF3314UsersAndEclesiaHelpersIgnoreLegacyPure(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)

	legacyUserID := createF330User(t, database, "f33-14-legacy-users-ecles")
	legacyPolicyID, err := database.SavePolitica(&db.Politica{
		Nom:        "f33-14-legacy-users-ecles",
		Descripcio: "",
	})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica legacy F33-14: %v", err)
	}
	if err := database.AddUserPolitica(legacyUserID, legacyPolicyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica legacy F33-14: %v", err)
	}
	legacyUser := &db.User{ID: legacyUserID}
	if app.canManageUsersModular(legacyUser) {
		t.Fatalf("CanManageUsers legacy pur no hauria de passar canManageUsersModular")
	}
	if app.canManageEclesiaModular(legacyUser) {
		t.Fatalf("CanManageEclesia legacy pur no hauria de passar canManageEclesiaModular")
	}
	if app.canViewEclesiaModular(legacyUser) {
		t.Fatalf("CanManageEclesia legacy pur no hauria de passar canViewEclesiaModular")
	}

	modularUserID := createF330User(t, database, "f33-14-modular-users-ecles")
	modularPolicyID, err := database.SavePolitica(&db.Politica{Nom: "f33-14-modular-users-ecles", Descripcio: ""})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica modular F33-14: %v", err)
	}
	for _, grant := range []db.PoliticaGrant{
		{PoliticaID: modularPolicyID, PermKey: permKeyAdminUsersManage, ScopeType: string(ScopeGlobal)},
		{PoliticaID: modularPolicyID, PermKey: permKeyTerritoriEclesEdit, ScopeType: string(ScopeEcles), ScopeID: sql.NullInt64{Int64: 1, Valid: true}},
	} {
		if _, err := database.SavePoliticaGrant(&grant); err != nil {
			t.Fatalf("no s'ha pogut crear grant modular F33-14 %s: %v", grant.PermKey, err)
		}
	}
	if err := database.AddUserPolitica(modularUserID, modularPolicyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica modular F33-14: %v", err)
	}
	modularUser := &db.User{ID: modularUserID}
	if !app.canManageUsersModular(modularUser) {
		t.Fatalf("admin.users.manage hauria de passar canManageUsersModular")
	}
	if !app.canManageEclesiaModular(modularUser) {
		t.Fatalf("territori.eclesiastic.edit hauria de passar canManageEclesiaModular")
	}
	if !app.canViewEclesiaModular(modularUser) {
		t.Fatalf("territori.eclesiastic.edit hauria de passar canViewEclesiaModular")
	}

	adminUserID := createF330User(t, database, "f33-14-helper-admin")
	adminID := findF330PolicyID(t, database, "admin")
	if err := database.AddUserPolitica(adminUserID, adminID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica admin helper F33-14: %v", err)
	}
	adminUser := &db.User{ID: adminUserID}
	if !app.canManageUsersModular(adminUser) || !app.canManageEclesiaModular(adminUser) || !app.canViewEclesiaModular(adminUser) {
		t.Fatalf("admin global hauria d'activar els helpers F33-14")
	}
}

func TestF3315LegacyCanCreatePersonDoesNotPassCreateGuard(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)

	legacyUserID := createF330User(t, database, "f33-15-legacy-create")
	legacyPolicyID, err := database.SavePolitica(&db.Politica{
		Nom:        "f33-15-legacy-create",
		Descripcio: "",
	})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica legacy create: %v", err)
	}
	if err := database.AddUserPolitica(legacyUserID, legacyPolicyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica legacy create: %v", err)
	}
	legacySession := createF3313SessionCookie(t, database, legacyUserID, "sess_f33_15_legacy_create")
	rr := runF3315CreatePersona(app, legacySession, "Legacy")
	if rr.Code != http.StatusForbidden {
		t.Fatalf("CanCreatePerson legacy pur esperava 403 a CreatePersona, rebut %d", rr.Code)
	}
	if app.canCreatePersonModular(&db.User{ID: legacyUserID}) {
		t.Fatalf("CanCreatePerson legacy pur no hauria de passar canCreatePersonModular")
	}

	modularUserID := createF330User(t, database, "f33-15-modular-create")
	assignF3315Policy(t, database, modularUserID, "f33-15-modular-create", permKeyPersonesCreate)
	modularSession := createF3313SessionCookie(t, database, modularUserID, "sess_f33_15_modular_create")
	rr = runF3315CreatePersona(app, modularSession, "Modular")
	if rr.Code != http.StatusCreated {
		t.Fatalf("persones.create esperava 201 a CreatePersona, rebut %d body=%s", rr.Code, rr.Body.String())
	}

	trustedUserID := createF330User(t, database, "f33-15-confianca-create")
	trustedPolicyID, err := database.SavePolitica(&db.Politica{Nom: "confianca", Descripcio: ""})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica confianca: %v", err)
	}
	if err := database.AddUserPolitica(trustedUserID, trustedPolicyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica confianca: %v", err)
	}
	if !app.canCreatePersonModular(&db.User{ID: trustedUserID}) {
		t.Fatalf("la politica confianca ha de conservar la capacitat de crear persones via default modular")
	}
	adminUserID := createF330User(t, database, "f33-15-admin-create")
	adminID := findF330PolicyID(t, database, "admin")
	if err := database.AddUserPolitica(adminUserID, adminID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica admin create: %v", err)
	}
	adminSession := createF3313SessionCookie(t, database, adminUserID, "sess_f33_15_admin_create")
	rr = runF3315CreatePersona(app, adminSession, "Admin")
	if rr.Code != http.StatusCreated {
		t.Fatalf("admin global esperava 201 a CreatePersona, rebut %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestF3315EditAnyPersonUsesModularKey(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)

	ownerID := createF330User(t, database, "f33-15-owner")
	assignF3315Policy(t, database, ownerID, "f33-15-owner-create", permKeyPersonesCreate)
	ownerSession := createF3313SessionCookie(t, database, ownerID, "sess_f33_15_owner")
	personaID, err := database.CreatePersona(&db.Persona{
		Nom:            "Persona",
		Cognom1:        "Propia",
		ModeracioEstat: "pendent",
		CreatedBy:      sql.NullInt64{Int64: int64(ownerID), Valid: true},
	})
	if err != nil {
		t.Fatalf("no s'ha pogut crear persona owner: %v", err)
	}
	rr := runF3315UpdatePersona(app, ownerSession, personaID, "Propia editada")
	if rr.Code != http.StatusOK {
		t.Fatalf("usuari creador amb persones.create esperava 200 editant propia, rebut %d body=%s", rr.Code, rr.Body.String())
	}

	legacyEditAnyID := createF330User(t, database, "f33-15-legacy-edit-any")
	assignF3315Policy(t, database, legacyEditAnyID, "f33-15-legacy-edit-any-create", permKeyPersonesCreate)
	legacyPolicyID, err := database.SavePolitica(&db.Politica{
		Nom:        "f33-15-legacy-edit-any",
		Descripcio: "",
	})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica legacy edit any: %v", err)
	}
	if err := database.AddUserPolitica(legacyEditAnyID, legacyPolicyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica legacy edit any: %v", err)
	}
	legacySession := createF3313SessionCookie(t, database, legacyEditAnyID, "sess_f33_15_legacy_edit_any")
	rr = runF3315UpdatePersona(app, legacySession, personaID, "Alien legacy")
	if rr.Code != http.StatusForbidden {
		t.Fatalf("CanEditAnyPerson legacy pur amb create modular esperava 403 editant aliena, rebut %d", rr.Code)
	}
	if app.canEditAnyPersonModular(&db.User{ID: legacyEditAnyID}) {
		t.Fatalf("CanEditAnyPerson legacy pur no hauria de passar canEditAnyPersonModular")
	}

	modularEditAnyID := createF330User(t, database, "f33-15-modular-edit-any")
	assignF3315Policy(t, database, modularEditAnyID, "f33-15-modular-edit-any", permKeyPersonesCreate, permKeyPersonesEditAny)
	modularSession := createF3313SessionCookie(t, database, modularEditAnyID, "sess_f33_15_modular_edit_any")
	rr = runF3315UpdatePersona(app, modularSession, personaID, "Alien modular")
	if rr.Code != http.StatusOK {
		t.Fatalf("persones.edit.any esperava 200 editant aliena, rebut %d body=%s", rr.Code, rr.Body.String())
	}

	adminUserID := createF330User(t, database, "f33-15-admin-edit-any")
	adminID := findF330PolicyID(t, database, "admin")
	if err := database.AddUserPolitica(adminUserID, adminID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica admin edit any: %v", err)
	}
	adminSession := createF3313SessionCookie(t, database, adminUserID, "sess_f33_15_admin_edit_any")
	rr = runF3315UpdatePersona(app, adminSession, personaID, "Alien admin")
	if rr.Code != http.StatusOK {
		t.Fatalf("admin global esperava 200 editant aliena, rebut %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestF3315TemplatePersonFlagsUseModularKeys(t *testing.T) {
	app := &App{}
	req := httptest.NewRequest("GET", "/persones", nil)
	req = app.withEffectiveAdmin(req, false)
	req = app.withPermissionKeys(req, map[string]bool{})
	data := injectPermsIfMissing(req, map[string]interface{}{}).(map[string]interface{})
	if got := data["CanCreatePerson"]; got == true {
		t.Fatalf("CanCreatePerson legacy pur no hauria d'activar flag template, rebut %#v", got)
	}
	if got := data["CanViewPersones"]; got == true {
		t.Fatalf("CanCreatePerson legacy pur no hauria d'activar CanViewPersones, rebut %#v", got)
	}

	req = httptest.NewRequest("GET", "/persones", nil)
	req = app.withEffectiveAdmin(req, false)
	req = app.withPermissionKeys(req, map[string]bool{permKeyPersonesCreate: true})
	data = injectPermsIfMissing(req, map[string]interface{}{}).(map[string]interface{})
	if got := data["CanCreatePerson"]; got != true {
		t.Fatalf("persones.create hauria d'activar CanCreatePerson, rebut %#v", got)
	}
	if got := data["CanViewPersones"]; got != true {
		t.Fatalf("persones.create hauria d'activar CanViewPersones, rebut %#v", got)
	}
}

func TestF3315ConvertRegistreUsesDocumentalOrCreatePersonModular(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	registreID, rawPersonID := createF3315RawRegistre(t, database)

	legacyUserID := createF330User(t, database, "f33-15-convert-legacy")
	legacyPolicyID, err := database.SavePolitica(&db.Politica{
		Nom:        "f33-15-convert-legacy",
		Descripcio: "",
	})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica legacy convert: %v", err)
	}
	if err := database.AddUserPolitica(legacyUserID, legacyPolicyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica legacy convert: %v", err)
	}
	legacySession := createF3313SessionCookie(t, database, legacyUserID, "sess_f33_15_convert_legacy")
	rr := runF3315ConvertRegistre(app, legacySession, registreID, rawPersonID)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("CanCreatePerson legacy pur esperava 403 convertint registre, rebut %d", rr.Code)
	}

	modularCreateID := createF330User(t, database, "f33-15-convert-create")
	assignF3315Policy(t, database, modularCreateID, "f33-15-convert-create", permKeyPersonesCreate)
	modularCreateSession := createF3313SessionCookie(t, database, modularCreateID, "sess_f33_15_convert_create")
	rr = runF3315ConvertRegistre(app, modularCreateSession, registreID, rawPersonID)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("persones.create esperava 303 convertint registre, rebut %d body=%s", rr.Code, rr.Body.String())
	}

	registreID, rawPersonID = createF3315RawRegistre(t, database)
	documentalID := createF330User(t, database, "f33-15-convert-documental")
	assignF3315Policy(t, database, documentalID, "f33-15-convert-documental", permKeyDocumentalsRegistresConvertToPerson)
	documentalSession := createF3313SessionCookie(t, database, documentalID, "sess_f33_15_convert_documental")
	rr = runF3315ConvertRegistre(app, documentalSession, registreID, rawPersonID)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("documentals.registres.convert_to_person esperava 303 convertint registre, rebut %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestF3316AdminUIFilesDoNotUseLocalLegacyAdminChecks(t *testing.T) {
	patterns := []string{
		"perms." + "Admin",
		"has" + "Perm(perms, permAdmin",
		"require" + "Permission(w, r, permAdmin",
	}
	for _, path := range []string{
		"templates.go",
		"dashboard_widgets.go",
		"admin_arxius.go",
		"admin_llibres.go",
		"admin_moderacio.go",
	} {
		body, err := readF3316CoreSource(path)
		if err != nil {
			t.Fatalf("no s'ha pogut llegir %s: %v", path, err)
		}
		src := string(body)
		for _, pattern := range patterns {
			if strings.Contains(src, pattern) {
				t.Fatalf("%s encara conte el patro legacy local %q", path, pattern)
			}
		}
	}
}

func readF3316CoreSource(path string) ([]byte, error) {
	for _, candidate := range []string{
		filepath.Join("core", path),
		filepath.Join("..", "core", path),
		path,
	} {
		body, err := os.ReadFile(candidate)
		if err == nil {
			return body, nil
		}
	}
	return nil, os.ErrNotExist
}

func TestF3316TemplateAdminFlagsUseEffectiveAdminContext(t *testing.T) {
	app := &App{}
	req := httptest.NewRequest("GET", "/admin", nil)
	req = app.withPermissionKeys(req, map[string]bool{})
	data := injectPermsIfMissing(req, map[string]interface{}{}).(map[string]interface{})
	if got := data["IsAdmin"]; got == true {
		t.Fatalf("admin=true local sense context efectiu no hauria d'activar IsAdmin, rebut %#v", got)
	}

	req = httptest.NewRequest("GET", "/admin", nil)
	req = app.withEffectiveAdmin(req, true)
	req = app.withPermissionKeys(req, map[string]bool{})
	data = injectPermsIfMissing(req, map[string]interface{}{}).(map[string]interface{})
	for _, key := range []string{"IsAdmin", "CanManageArxius", "CanManageUsers", "CanManagePolicies", "CanCreatePerson"} {
		if got := data[key]; got != true {
			t.Fatalf("effectiveAdmin hauria d'activar %s, rebut %#v", key, got)
		}
	}
}

func TestF3316DashboardAdminRoleUsesEffectiveAdminBridge(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)

	legacyAdminUserID := createF330User(t, database, "f33-16-dashboard-admin-flag")
	legacyAdminPolicyID, err := database.SavePolitica(&db.Politica{
		Nom:        "f33-16-dashboard-admin-flag",
		Descripcio: "",
	})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica admin F33-16: %v", err)
	}
	if err := database.AddUserPolitica(legacyAdminUserID, legacyAdminPolicyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica admin F33-16: %v", err)
	}
	if roles := app.dashboardUserRoleSet(legacyAdminUserID); roles["admin"] {
		t.Fatalf("dashboard no hauria de marcar rol admin per admin=true legacy")
	}

	adminUserID := createF330User(t, database, "f33-16-dashboard-admin-name")
	adminPolicyID := findF330PolicyID(t, database, "admin")
	if _, err := database.SavePolitica(&db.Politica{
		ID:         adminPolicyID,
		Nom:        "admin",
		Descripcio: "",
	}); err != nil {
		t.Fatalf("no s'ha pogut preparar politica admin F33-16: %v", err)
	}
	if err := database.AddUserPolitica(adminUserID, adminPolicyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica admin per nom F33-16: %v", err)
	}
	if roles := app.dashboardUserRoleSet(adminUserID); !roles["admin"] {
		t.Fatalf("dashboard hauria de marcar rol admin via effectiveAdminForUser")
	}

	plainUserID := createF330User(t, database, "f33-16-dashboard-plain")
	if roles := app.dashboardUserRoleSet(plainUserID); roles["admin"] {
		t.Fatalf("usuari no admin no hauria de rebre rol admin")
	}
}

func TestF3319LegacyJSONFlagsDoNotGrantSnapshotPermissions(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-19-legacy-full")
	policyID, err := database.SavePolitica(&db.Politica{
		Nom:        "f33-19-legacy-full",
		Descripcio: "",
	})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica legacy F33-19: %v", err)
	}
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica legacy F33-19: %v", err)
	}

	snap, err := app.buildPermissionSnapshot(userID)
	if err != nil {
		t.Fatalf("no s'ha pogut construir snapshot F33-19: %v", err)
	}
	if snap.isAdmin {
		t.Fatalf("admin=true legacy no hauria d'activar admin al snapshot")
	}
	for _, key := range []string{
		permKeyTerritoriNivellsEdit,
		permKeyTerritoriMunicipisEdit,
		permKeyTerritoriEclesEdit,
		permKeyDocumentalsArxiusEdit,
		permKeyDocumentalsLlibresEdit,
		permKeyAdminPoliciesManage,
		permKeyAdminUsersManage,
	} {
		if f3319SnapshotHasGrant(snap, key) {
			t.Fatalf("legacy JSON no hauria de crear grant modular per %s", key)
		}
		if app.HasPermission(userID, key, PermissionTarget{}) {
			t.Fatalf("legacy JSON no hauria d'autoritzar %s", key)
		}
	}
}

func TestF3319RealModularGrantAndAdminPolicyStillWork(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)

	adminUserID := createF330User(t, database, "f33-19-admin-name")
	adminPolicyID := findF330PolicyID(t, database, "admin")
	if _, err := database.SavePolitica(&db.Politica{
		ID:         adminPolicyID,
		Nom:        "admin",
		Descripcio: "",
	}); err != nil {
		t.Fatalf("no s'ha pogut preparar politica admin F33-19: %v", err)
	}
	if err := database.AddUserPolitica(adminUserID, adminPolicyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica admin F33-19: %v", err)
	}
	adminSnap, err := app.buildPermissionSnapshot(adminUserID)
	if err != nil {
		t.Fatalf("no s'ha pogut construir snapshot admin F33-19: %v", err)
	}
	if !adminSnap.isAdmin {
		t.Fatalf("la politica amb nom admin hauria de continuar activant admin modular")
	}
	if !app.HasPermission(adminUserID, permKeyAdminPoliciesManage, PermissionTarget{}) {
		t.Fatalf("admin per nom hauria de conservar permisos modulars globals")
	}

	grantUserID := createF330User(t, database, "f33-19-real-grant")
	grantPolicyID, err := database.SavePolitica(&db.Politica{
		Nom:        "f33-19-real-grant",
		Descripcio: "",
	})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica grant F33-19: %v", err)
	}
	if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
		PoliticaID: grantPolicyID,
		PermKey:    permKeyAdminPoliciesManage,
		ScopeType:  string(ScopeGlobal),
	}); err != nil {
		t.Fatalf("no s'ha pogut crear grant real F33-19: %v", err)
	}
	if err := database.AddUserPolitica(grantUserID, grantPolicyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica grant F33-19: %v", err)
	}
	grantSnap, err := app.buildPermissionSnapshot(grantUserID)
	if err != nil {
		t.Fatalf("no s'ha pogut construir snapshot grant F33-19: %v", err)
	}
	if !f3319SnapshotHasGrant(grantSnap, permKeyAdminPoliciesManage) {
		t.Fatalf("un grant real politica_grants hauria d'apareixer al snapshot")
	}
	if !app.HasPermission(grantUserID, permKeyAdminPoliciesManage, PermissionTarget{}) {
		t.Fatalf("un grant real politica_grants hauria d'autoritzar el permis modular")
	}
}

func TestF3319KnownPolicyDefaultsRemainExplicit(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)

	defaultUserID := createF330User(t, database, "f33-19-default-policy")
	defaultPolicyID, err := database.SavePolitica(&db.Politica{Nom: "confianca", Descripcio: ""})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica default F33-19: %v", err)
	}
	if err := database.AddUserPolitica(defaultUserID, defaultPolicyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica default F33-19: %v", err)
	}
	defaultSnap, err := app.buildPermissionSnapshot(defaultUserID)
	if err != nil {
		t.Fatalf("no s'ha pogut construir snapshot default F33-19: %v", err)
	}
	if !f3319SnapshotHasGrant(defaultSnap, permKeyPersonesCreate) {
		t.Fatalf("la politica confianca hauria de conservar els defaults modulars explicits")
	}
	if f3319SnapshotHasGrant(defaultSnap, permKeyAdminUsersManage) {
		t.Fatalf("una politica coneguda no hauria d'afegir admin.users.manage als defaults explicits")
	}
}
func f3319SnapshotHasGrant(snap permissionSnapshot, key string) bool {
	return len(snap.grants[key]) > 0
}

func TestF3320BuildPermissionSnapshotDoesNotUseLegacyMigrationMapper(t *testing.T) {
	body, err := readF3316CoreSource("permissions_modular.go")
	if err != nil {
		t.Fatalf("no s'ha pogut llegir permissions_modular.go: %v", err)
	}
	src := string(body)
	fn := f3320FunctionSource(t, src, "func (a *App) buildPermissionSnapshot")
	for _, pattern := range []string{
		"legacyPermKeys",
		"Policy" + "Permissions",
		"perms." + "Admin",
		"perms." + "CanManageTerritory",
		"perms." + "CanManageEclesia",
		"perms." + "CanManageArchives",
		"perms." + "CanManagePolicies",
		"perms." + "CanManageUsers",
	} {
		if strings.Contains(fn, pattern) {
			t.Fatalf("buildPermissionSnapshot no hauria de contenir el patro legacy %q", pattern)
		}
	}
}

func TestF3320CoreHandlersDoNotUseLegacyPermissionHelpers(t *testing.T) {
	root := f3320RepoRoot(t)
	allowed := map[string]bool{
		"core/permissions.go":                  true,
		"core/permissions_modular.go":          true,
		"core/permissions_modular_f33_test.go": true,
	}
	patterns := []string{
		"has" + "Perm(",
		"require" + "Permission(",
		"permAdmin",
		"permPolicies",
		"permUsers",
		"permEclesia",
		"permArxius",
		"permTerritory",
		"permModerate",
		"permCreatePerson",
		"perms." + "Admin",
		"perms." + "CanManagePolicies",
		"perms." + "CanManageUsers",
		"perms." + "CanManageEclesia",
		"perms." + "CanManageArchives",
		"perms." + "CanManageTerritory",
		"perms." + "CanModerate",
		"perms." + "CanCreatePerson",
		"perms." + "CanEditAnyPerson",
	}
	var violations []string
	if err := filepath.Walk(filepath.Join(root, "core"), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info == nil || info.IsDir() || !strings.HasSuffix(info.Name(), ".go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if allowed[rel] {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		src := string(body)
		for _, pattern := range patterns {
			if strings.Contains(src, pattern) {
				violations = append(violations, rel+" conte "+pattern)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("no s'ha pogut escanejar core: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("handlers/core no autoritzats amb patrons legacy: %s", strings.Join(violations, "; "))
	}
}

func TestF3320LegacyPermissionHelpersAreRemovedOrMigrationOnly(t *testing.T) {
	permissionsBody, err := readF3316CoreSource("permissions.go")
	if err != nil {
		t.Fatalf("no s'ha pogut llegir permissions.go: %v", err)
	}
	for _, pattern := range []string{
		"func (a *App) hasPerm",
		"func (a *App) requirePermission",
		"func permAdmin",
		"func permPolicies",
		"func permUsers",
		"func permEclesia",
		"func permArxius",
		"func permTerritory",
		"func permModerate",
		"func permCreatePerson",
		"if perms." + "Admin",
	} {
		if strings.Contains(string(permissionsBody), pattern) {
			t.Fatalf("permissions.go conserva helper/cami legacy %q", pattern)
		}
	}

	root := f3320RepoRoot(t)
	legacyMigration := "permissions_" + "migration.go"
	if _, err := os.Stat(filepath.Join(root, "core", legacyMigration)); !os.IsNotExist(err) {
		t.Fatalf("%s hauria d'estar eliminat, err=%v", legacyMigration, err)
	}
}

func TestF3321LegacyPermissionRuntimePlumbingIsRemoved(t *testing.T) {
	permissionsBody, err := readF3316CoreSource("permissions.go")
	if err != nil {
		t.Fatalf("no s'ha pogut llegir permissions.go: %v", err)
	}
	permissionsSrc := string(permissionsBody)
	for _, pattern := range []string{
		"permissionsFromContext",
		"withPermissions",
		"getPermissionsForUser",
	} {
		if strings.Contains(permissionsSrc, pattern) {
			t.Fatalf("permissions.go conserva plumbing runtime legacy %q", pattern)
		}
	}

	root := f3320RepoRoot(t)
	allowed := map[string]bool{
		"core/permissions_modular_f33_test.go": true,
	}
	var violations []string
	if err := filepath.Walk(filepath.Join(root, "core"), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info == nil || info.IsDir() || !strings.HasSuffix(info.Name(), ".go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if allowed[rel] {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		src := string(body)
		for _, pattern := range []string{"permissionsFromContext", "withPermissions", "getPermissionsForUser"} {
			if strings.Contains(src, pattern) {
				violations = append(violations, rel+" conte "+pattern)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("no s'ha pogut escanejar core F33-21: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("plumbing runtime legacy detectat: %s", strings.Join(violations, "; "))
	}
}

func TestF3322DBLegacyEffectivePermissionsAPIIsRemoved(t *testing.T) {
	root := f3320RepoRoot(t)
	legacyEffective := "Get" + "EffectivePoliticaPerms"
	legacyBump := "Bump" + "Policy" + "Permissions" + "Version"
	legacyHelper := "get" + "EffectivePoliticaPerms"
	files := []string{
		"db/motor.go",
		"db/sqlcommon.go",
		"db/sqlite.go",
		"db/mysql.go",
		"db/postgres.go",
	}
	for _, rel := range files {
		body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("no s'ha pogut llegir %s: %v", rel, err)
		}
		src := string(body)
		for _, pattern := range []string{legacyEffective, legacyBump, legacyHelper} {
			if strings.Contains(src, pattern) {
				t.Fatalf("%s conserva API legacy de permisos %q", rel, pattern)
			}
		}
	}
}

func TestF3322PolicyGrantChangesInvalidateModularSnapshotCache(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-22-cache")
	policyID, err := database.SavePolitica(&db.Politica{
		Nom:        "f33-22-cache-policy",
		Descripcio: "",
	})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica F33-22: %v", err)
	}
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica F33-22: %v", err)
	}
	target := PermissionTarget{}
	if app.HasPermission(userID, permKeyAdminAuditView, target) {
		t.Fatalf("el test necessita cache inicial sense admin.audit.view")
	}
	if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
		PoliticaID:      policyID,
		PermKey:         permKeyAdminAuditView,
		ScopeType:       string(ScopeGlobal),
		IncludeChildren: false,
	}); err != nil {
		t.Fatalf("no s'ha pogut desar grant F33-22: %v", err)
	}
	if !app.HasPermission(userID, permKeyAdminAuditView, target) {
		t.Fatalf("el canvi de grant modular hauria d'invalidar snapshot/cache via permissions_version")
	}
}

func TestF3323LegacyPermissionModelAndLegacyMapperAreRemoved(t *testing.T) {
	root := f3320RepoRoot(t)
	for _, rel := range []string{
		"db/motor.go",
		"db/sqlcommon.go",
	} {
		body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("no s'ha pogut llegir %s: %v", rel, err)
		}
		src := string(body)
		for _, pattern := range []string{
			"Policy" + "Permissions",
			"legacyPermKeys" + "ForMigrationOnly",
			"CanManageTerritory bool",
			"CanManageArchives bool",
			"CanModerate bool",
		} {
			if strings.Contains(src, pattern) {
				t.Fatalf("%s conserva model/mapper legacy %q", rel, pattern)
			}
		}
	}
	for _, rel := range []string{"core/policies_" + "document.go", "core/permissions_" + "migration.go"} {
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel))); !os.IsNotExist(err) {
			t.Fatalf("%s hauria d'estar eliminat, err=%v", rel, err)
		}
	}
}

func TestF3323RPolicyPermisosSchemaSQLAndUIAreRemoved(t *testing.T) {
	root := f3320RepoRoot(t)
	legacyColumn := "per" + "misos"
	for _, rel := range []string{"db/SQLite.sql", "db/PostgreSQL.sql", "db/MySQL.sql"} {
		body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("no s'ha pogut llegir %s: %v", rel, err)
		}
		src := string(body)
		if !strings.Contains(src, "politica_grants") {
			t.Fatalf("%s hauria de conservar schema modular politica_grants", rel)
		}
		if strings.Contains(src, legacyColumn+" TEXT") || strings.Contains(src, "politiques."+legacyColumn) {
			t.Fatalf("%s conserva columna legacy permisos", rel)
		}
	}
	scans := map[string][]string{
		"db/sqlcommon.go": {
			"SELECT id, nom, descripcio, " + legacyColumn,
			"INSERT INTO politiques (nom, descripcio, " + legacyColumn,
			"UPDATE politiques SET nom=?, descripcio=?, " + legacyColumn,
			"p." + legacyColumn,
			legacyColumn + " =",
		},
		"core/admin_politiques.go": {
			"FormValue(\"" + legacyColumn + "\")",
			"JSON de " + legacyColumn,
			"refreshPolicyPermsJSON",
			"parse" + "Policy" + "Document",
		},
		"templates/admin-politiques-form.html": {
			"name=\"" + legacyColumn + "\"",
			"id=\"" + legacyColumn + "\"",
		},
		"templates/admin-politiques-list.html": {
			"." + "Perm" + "isos",
			"data-perms",
		},
	}
	for rel, patterns := range scans {
		body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("no s'ha pogut llegir %s: %v", rel, err)
		}
		src := string(body)
		for _, pattern := range patterns {
			if strings.Contains(src, pattern) {
				t.Fatalf("%s conserva residu legacy de politica JSON: %q", rel, pattern)
			}
		}
	}
}

func TestF3324GroupPolicyAndMembershipChangesInvalidateSnapshot(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-24-group-cache")
	groupID, err := database.CreateGroup("f33-24-permission-group", "")
	if err != nil {
		t.Fatalf("no s'ha pogut crear grup de permisos F33-24: %v", err)
	}
	policyID, err := database.SavePolitica(&db.Politica{
		Nom:        "f33-24-group-policy",
		Descripcio: "",
	})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica F33-24: %v", err)
	}
	if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
		PoliticaID: policyID,
		PermKey:    permKeyAdminAuditView,
		ScopeType:  string(ScopeGlobal),
	}); err != nil {
		t.Fatalf("no s'ha pogut crear grant F33-24: %v", err)
	}
	target := PermissionTarget{}
	if app.HasPermission(userID, permKeyAdminAuditView, target) {
		t.Fatalf("el test necessita cache inicial sense admin.audit.view")
	}
	if err := database.AddUserGroup(userID, groupID); err != nil {
		t.Fatalf("no s'ha pogut afegir l'usuari al grup F33-24: %v", err)
	}
	if err := database.AddGroupPolitica(groupID, policyID); err != nil {
		t.Fatalf("no s'ha pogut afegir politica al grup F33-24: %v", err)
	}
	if err := database.BumpGroupPermissionsVersion(groupID); err != nil {
		t.Fatalf("no s'ha pogut invalidar grup F33-24: %v", err)
	}
	if !app.HasPermission(userID, permKeyAdminAuditView, target) {
		t.Fatalf("la politica del grup hauria de donar admin.audit.view")
	}
	if err := database.RemoveGroupPolitica(groupID, policyID); err != nil {
		t.Fatalf("no s'ha pogut retirar politica del grup F33-24: %v", err)
	}
	if err := database.BumpGroupPermissionsVersion(groupID); err != nil {
		t.Fatalf("no s'ha pogut invalidar retirada de politica F33-24: %v", err)
	}
	if app.HasPermission(userID, permKeyAdminAuditView, target) {
		t.Fatalf("retirar la politica del grup hauria d'invalidar el snapshot")
	}
	if err := database.AddGroupPolitica(groupID, policyID); err != nil {
		t.Fatalf("no s'ha pogut reassignar politica al grup F33-24: %v", err)
	}
	if err := database.BumpGroupPermissionsVersion(groupID); err != nil {
		t.Fatalf("no s'ha pogut invalidar reassignacio de politica F33-24: %v", err)
	}
	if !app.HasPermission(userID, permKeyAdminAuditView, target) {
		t.Fatalf("reassignar la politica del grup hauria de reconstruir el snapshot")
	}
	if err := database.RemoveUserGroup(userID, groupID); err != nil {
		t.Fatalf("no s'ha pogut retirar l'usuari del grup F33-24: %v", err)
	}
	if app.HasPermission(userID, permKeyAdminAuditView, target) {
		t.Fatalf("retirar l'usuari del grup hauria d'invalidar el snapshot")
	}
}

func TestF3325PolicyModularJSONExportIsDeterministic(t *testing.T) {
	scopeID := 12
	pol := &db.Politica{ID: 7, Nom: "f33-25-json", Descripcio: "export"}
	out := exportPolicyModularJSON(pol, []db.PoliticaGrant{
		{PoliticaID: 7, PermKey: permKeyTerritoriMunicipisEdit, ScopeType: string(ScopeMunicipi), ScopeID: sql.NullInt64{Int64: int64(scopeID), Valid: true}, IncludeChildren: true},
		{PoliticaID: 7, PermKey: permKeyAdminAuditView, ScopeType: string(ScopeGlobal), ScopeID: sql.NullInt64{}, IncludeChildren: false},
	})
	if !strings.Contains(out, `"version": 1`) || !strings.Contains(out, `"name": "f33-25-json"`) {
		t.Fatalf("export JSON no conte capcalera esperada: %s", out)
	}
	first := strings.Index(out, permKeyAdminAuditView)
	second := strings.Index(out, permKeyTerritoriMunicipisEdit)
	if first < 0 || second < 0 || first > second {
		t.Fatalf("export JSON no ordena grants deterministicament: %s", out)
	}
	if strings.Contains(out, "Policy"+"Permissions") || strings.Contains(out, "per"+"misos") {
		t.Fatalf("export JSON no ha d'incloure camps legacy: %s", out)
	}
}

func TestF3325PolicyModularJSONValidationRejectsUnsafeInput(t *testing.T) {
	valid := `{"version":1,"policy":{"name":"x","description":""},"grants":[{"perm_key":"admin.audit.view","scope_type":"global","scope_id":null,"include_children":false}]}`
	if _, err := parsePolicyModularJSON(valid, 1); err != nil {
		t.Fatalf("JSON valid rebutjat: %v", err)
	}
	cases := []string{
		`{"version":2,"policy":{"name":"x","description":""},"grants":[]}`,
		`{"version":1,"policy":{"name":"x","description":""},"grants":[{"perm_key":"desconegut","scope_type":"global","scope_id":null,"include_children":false}]}`,
		`{"version":1,"policy":{"name":"x","description":""},"grants":[{"perm_key":"admin.audit.view","scope_type":"misteri","scope_id":1,"include_children":false}]}`,
		`{"version":1,"policy":{"name":"x","description":""},"grants":[{"perm_key":"admin.audit.view","scope_type":"municipi","scope_id":null,"include_children":false}]}`,
		`{"version":1,"policy":{"name":"x","description":""},"grants":[{"perm_key":"admin.audit.view","scope_type":"global","scope_id":null,"include_children":false},{"perm_key":"admin.audit.view","scope_type":"global","scope_id":null,"include_children":false}]}`,
		`{"version":1,"policy":{"name":"x","description":""},"permisos":{},"grants":[]}`,
		`{"version":1,"policy":{"name":"x","description":"","Admin":true},"grants":[]}`,
	}
	for _, input := range cases {
		if _, err := parsePolicyModularJSON(input, 1); err == nil {
			t.Fatalf("JSON invalid acceptat: %s", input)
		}
	}
}

func TestF3325ApplyPolicyJSONReplacesGrantsAndInvalidatesSnapshot(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-25-json-user")
	policyID, err := database.SavePolitica(&db.Politica{Nom: "f33-25-json-policy", Descripcio: ""})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica F33-25: %v", err)
	}
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica F33-25: %v", err)
	}
	if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
		PoliticaID: policyID,
		PermKey:    permKeyAdminAuditView,
		ScopeType:  string(ScopeGlobal),
	}); err != nil {
		t.Fatalf("no s'ha pogut crear grant inicial F33-25: %v", err)
	}
	if !app.HasPermission(userID, permKeyAdminAuditView, PermissionTarget{}) {
		t.Fatalf("grant inicial hauria d'autoritzar admin.audit.view")
	}
	input := `{"version":1,"policy":{"name":"f33-25-json-policy","description":""},"grants":[{"perm_key":"admin.jobs.manage","scope_type":"global","scope_id":null,"include_children":false}]}`
	grants, err := parsePolicyModularJSON(input, policyID)
	if err != nil {
		t.Fatalf("JSON F33-25 invalid: %v", err)
	}
	if err := database.ReplacePoliticaGrants(policyID, grants); err != nil {
		t.Fatalf("ReplacePoliticaGrants ha fallat: %v", err)
	}
	if app.HasPermission(userID, permKeyAdminAuditView, PermissionTarget{}) {
		t.Fatalf("aplicar JSON hauria d'haver retirat admin.audit.view")
	}
	if !app.HasPermission(userID, permKeyAdminJobsManage, PermissionTarget{}) {
		t.Fatalf("aplicar JSON hauria d'haver afegit admin.jobs.manage")
	}
	rows, err := database.ListPoliticaGrants(policyID)
	if err != nil {
		t.Fatalf("no s'han pogut llistar grants F33-25: %v", err)
	}
	if len(rows) != 1 || rows[0].PermKey != permKeyAdminJobsManage {
		t.Fatalf("ReplacePoliticaGrants no ha substituit grants: %#v", rows)
	}
}

func f3320FunctionSource(t *testing.T, src, signature string) string {
	t.Helper()
	start := strings.Index(src, signature)
	if start < 0 {
		t.Fatalf("no s'ha trobat la funcio %s", signature)
	}
	rest := src[start+len(signature):]
	next := strings.Index(rest, "\nfunc ")
	if next < 0 {
		return src[start:]
	}
	return src[start : start+len(signature)+next]
}

func f3320RepoRoot(t *testing.T) string {
	t.Helper()
	start, err := os.Getwd()
	if err != nil {
		t.Fatalf("no puc obtenir directori actual: %v", err)
	}
	root := start
	for {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			return root
		}
		parent := filepath.Dir(root)
		if parent == root {
			t.Fatalf("no s'ha trobat go.mod a cap directori pare de %s", start)
		}
		root = parent
	}
}

func TestF3317ImportTemplateEditUsesOwnerOrEffectiveAdmin(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)

	ownerID := createF330User(t, database, "f33-17-template-owner")
	otherID := createF330User(t, database, "f33-17-template-other")
	adminID := createF330User(t, database, "f33-17-template-admin")
	adminPolicyID := findF330PolicyID(t, database, "admin")
	if _, err := database.SavePolitica(&db.Politica{
		ID:         adminPolicyID,
		Nom:        "admin",
		Descripcio: "",
	}); err != nil {
		t.Fatalf("no s'ha pogut preparar admin efectiu F33-17: %v", err)
	}
	if err := database.AddUserPolitica(adminID, adminPolicyID); err != nil {
		t.Fatalf("no s'ha pogut assignar admin efectiu F33-17: %v", err)
	}

	template := &db.CSVImportTemplate{
		Name:        "F33-17 owner template",
		OwnerUserID: sqlNullIntFromInt(ownerID),
		Visibility:  "private",
		ModelJSON:   `{"metadata":{"kind":"transcripcions_raw"},"mapping":{"columns":[]}}`,
	}

	owner := &db.User{ID: ownerID}
	if !app.canEditImportTemplate(owner, template) {
		t.Fatalf("owner hauria de poder editar la seva plantilla")
	}

	other := &db.User{ID: otherID}
	if app.canEditImportTemplate(other, template) {
		t.Fatalf("usuari no owner i no admin no hauria de poder editar")
	}

	admin := &db.User{ID: adminID}
	if !app.canEditImportTemplate(admin, template) {
		t.Fatalf("admin efectiu hauria de poder editar plantilla aliena")
	}

	rows := []db.CSVImportTemplate{*template}
	items := app.buildImportTemplateEntries(rows, other)
	if len(items) != 1 || items[0].CanEdit || items[0].CanDelete || items[0].CanToggle {
		t.Fatalf("no-owner no-admin no hauria de veure accions d'edicio: %#v", items)
	}
	items = app.buildImportTemplateEntries(rows, admin)
	if len(items) != 1 || !items[0].CanEdit || !items[0].CanDelete || !items[0].CanToggle {
		t.Fatalf("admin efectiu hauria de veure accions d'edicio: %#v", items)
	}
}

func TestF3317ImportTemplatesSimilarUsesOwnerOrEffectiveAdmin(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)

	ownerID := createF330User(t, database, "f33-17-similar-owner")
	otherID := createF330User(t, database, "f33-17-similar-other")
	adminID := createF330User(t, database, "f33-17-similar-admin")
	adminPolicyID := findF330PolicyID(t, database, "admin")
	if _, err := database.SavePolitica(&db.Politica{
		ID:         adminPolicyID,
		Nom:        "admin",
		Descripcio: "",
	}); err != nil {
		t.Fatalf("no s'ha pogut preparar admin similar F33-17: %v", err)
	}
	if err := database.AddUserPolitica(adminID, adminPolicyID); err != nil {
		t.Fatalf("no s'ha pogut assignar admin similar F33-17: %v", err)
	}

	modelJSON := `{"metadata":{"kind":"transcripcions_raw"},"book_resolution":{"mode":"llibre_id"},"mapping":{"columns":[{"key":"nom","map_to":[{"target":"person.nom"}]}]}}`
	templateID, err := database.CreateCSVImportTemplate(&db.CSVImportTemplate{
		Name:        "F33-17 similar public",
		OwnerUserID: sqlNullIntFromInt(ownerID),
		Visibility:  "public",
		ModelJSON:   modelJSON,
		Signature:   db.ComputeTemplateSignature(modelJSON),
	})
	if err != nil || templateID <= 0 {
		t.Fatalf("no s'ha pogut crear template similar F33-17: %v", err)
	}

	ownerItems := runF3317SimilarItems(t, app, ownerID, modelJSON)
	if !f3317SimilarItemCanEdit(ownerItems, templateID) {
		t.Fatalf("owner hauria de poder editar a similar JSON: %#v", ownerItems)
	}
	otherItems := runF3317SimilarItems(t, app, otherID, modelJSON)
	if f3317SimilarItemCanEdit(otherItems, templateID) {
		t.Fatalf("no-owner no-admin no hauria de poder editar a similar JSON: %#v", otherItems)
	}
	adminItems := runF3317SimilarItems(t, app, adminID, modelJSON)
	if !f3317SimilarItemCanEdit(adminItems, templateID) {
		t.Fatalf("admin efectiu hauria de poder editar a similar JSON: %#v", adminItems)
	}
}

func TestF3317ImportTemplatesDoNotUseLocalLegacyAdminChecks(t *testing.T) {
	patterns := []string{
		"perms." + "Admin",
		"has" + "Perm(perms, permAdmin",
		"require" + "Permission(w, r, permAdmin",
	}
	for _, path := range []string{
		"import_templates.go",
		"import_templates_similar.go",
	} {
		body, err := readF3316CoreSource(path)
		if err != nil {
			t.Fatalf("no s'ha pogut llegir %s: %v", path, err)
		}
		src := string(body)
		for _, pattern := range patterns {
			if strings.Contains(src, pattern) {
				t.Fatalf("%s encara conte el patro legacy local %q", path, pattern)
			}
		}
	}
}

func runF3317SimilarItems(t *testing.T, app *App, userID int, modelJSON string) []map[string]interface{} {
	t.Helper()
	payload, err := json.Marshal(map[string]interface{}{
		"model_json": modelJSON,
		"limit":      10,
	})
	if err != nil {
		t.Fatalf("no s'ha pogut preparar payload similar F33-17: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/import-templates/similar", bytes.NewReader(payload))
	rr := httptest.NewRecorder()
	app.importTemplatesSimilarJSON(rr, req, &db.User{ID: userID})
	if rr.Code != http.StatusOK {
		t.Fatalf("similar JSON F33-17 status inesperat %d body=%s", rr.Code, rr.Body.String())
	}
	var resp struct {
		Items []map[string]interface{} `json:"items"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("no s'ha pogut llegir resposta similar F33-17: %v", err)
	}
	return resp.Items
}

func f3317SimilarItemCanEdit(items []map[string]interface{}, id int) bool {
	for _, item := range items {
		if intFromJSONNumber(item["id"]) != id {
			continue
		}
		canEdit, _ := item["can_edit"].(bool)
		return canEdit
	}
	return false
}

func intFromJSONNumber(val interface{}) int {
	switch v := val.(type) {
	case float64:
		return int(v)
	case int:
		return v
	default:
		return 0
	}
}

func TestF3318CognomsAdminGuardsUseEffectiveAdminBridge(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)

	adminUserID := createF330User(t, database, "f33-18-cognoms-admin")
	adminPolicyID := findF330PolicyID(t, database, "admin")
	if _, err := database.SavePolitica(&db.Politica{
		ID:         adminPolicyID,
		Nom:        "admin",
		Descripcio: "",
	}); err != nil {
		t.Fatalf("no s'ha pogut preparar admin efectiu F33-18: %v", err)
	}
	if err := database.AddUserPolitica(adminUserID, adminPolicyID); err != nil {
		t.Fatalf("no s'ha pogut assignar admin efectiu F33-18: %v", err)
	}
	session := createF3313SessionCookie(t, database, adminUserID, "sess_f33_18_admin")

	for _, tc := range []struct {
		name string
		run  func(*App, *http.Cookie) *httptest.ResponseRecorder
	}{
		{name: "import", run: runF3318AdminCognomsImport},
		{name: "merge", run: runF3318AdminCognomsMerge},
	} {
		rr := tc.run(app, session)
		if rr.Code != http.StatusOK {
			t.Fatalf("admin efectiu esperava 200 a %s, rebut %d body=%s", tc.name, rr.Code, rr.Body.String())
		}
	}
}

func TestF3318CognomsAdminGuardsBlockNonAdmin(t *testing.T) {
	app, database := newF330PermissionsTestApp(t)
	userID := createF330User(t, database, "f33-18-cognoms-plain")
	session := createF3313SessionCookie(t, database, userID, "sess_f33_18_plain")

	for _, tc := range []struct {
		name string
		run  func(*App, *http.Cookie) *httptest.ResponseRecorder
	}{
		{name: "import", run: runF3318AdminCognomsImport},
		{name: "merge", run: runF3318AdminCognomsMerge},
	} {
		rr := tc.run(app, session)
		if rr.Code != http.StatusForbidden {
			t.Fatalf("usuari no admin esperava 403 a %s, rebut %d", tc.name, rr.Code)
		}
	}
}

func TestF3318CognomsAdminFilesDoNotUseLegacyAdminGuard(t *testing.T) {
	patterns := []string{
		"perms." + "Admin",
		"has" + "Perm(perms, permAdmin",
		"require" + "Permission(w, r, permAdmin",
	}
	for _, path := range []string{
		"admin_cognoms_import.go",
		"admin_cognoms_merge.go",
	} {
		body, err := readF3316CoreSource(path)
		if err != nil {
			t.Fatalf("no s'ha pogut llegir %s: %v", path, err)
		}
		src := string(body)
		for _, pattern := range patterns {
			if strings.Contains(src, pattern) {
				t.Fatalf("%s encara conte el patro legacy local %q", path, pattern)
			}
		}
	}
}

func runF3318AdminCognomsImport(app *App, session *http.Cookie) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/admin/cognoms/import", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminCognomsImport(rr, req)
	return rr
}

func runF3318AdminCognomsMerge(app *App, session *http.Cookie) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/admin/cognoms/merge", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminCognomsMerge(rr, req)
	return rr
}

func assignF3315Policy(t *testing.T, database db.DB, userID int, name string, permKeys ...string) int {
	t.Helper()
	policyID, err := database.SavePolitica(&db.Politica{Nom: name, Descripcio: ""})
	if err != nil {
		t.Fatalf("no s'ha pogut crear politica %s: %v", name, err)
	}
	for _, key := range permKeys {
		if _, err := database.SavePoliticaGrant(&db.PoliticaGrant{
			PoliticaID: policyID,
			PermKey:    key,
			ScopeType:  string(ScopeGlobal),
		}); err != nil {
			t.Fatalf("no s'ha pogut crear grant %s: %v", key, err)
		}
	}
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("no s'ha pogut assignar politica %s: %v", name, err)
	}
	return policyID
}

func runF3315CreatePersona(app *App, session *http.Cookie, nom string) *httptest.ResponseRecorder {
	payload, _ := json.Marshal(map[string]string{"nom": nom, "cognom1": "Test", "municipi": "Vila"})
	req := httptest.NewRequest(http.MethodPost, "/persones", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.CreatePersona(rr, req)
	return rr
}

func runF3315UpdatePersona(app *App, session *http.Cookie, personaID int, ofici string) *httptest.ResponseRecorder {
	payload, _ := json.Marshal(map[string]string{"nom": "Persona", "cognom1": "Test", "municipi": "Vila", "ofici": ofici})
	req := httptest.NewRequest(http.MethodPost, "/persones/"+strconv.Itoa(personaID), bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.UpdatePersona(rr, req)
	return rr
}

func createF3315RawRegistre(t *testing.T, database db.DB) (int, int) {
	t.Helper()
	llibreID := createF3315Llibre(t, database)
	registreID, err := database.CreateTranscripcioRaw(&db.TranscripcioRaw{
		LlibreID:       llibreID,
		TipusActe:      "naixement",
		DataActeText:   "1900-01-02",
		ModeracioEstat: "pendent",
	})
	if err != nil {
		t.Fatalf("no s'ha pogut crear registre raw F33-15: %v", err)
	}
	rawPersonID, err := database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
		TranscripcioID: registreID,
		Rol:            "nascut",
		Nom:            "Infant",
		Cognom1:        "Prova",
		MunicipiText:   "Vila",
	})
	if err != nil {
		t.Fatalf("no s'ha pogut crear persona raw F33-15: %v", err)
	}
	return registreID, rawPersonID
}

func createF3315Llibre(t *testing.T, database db.DB) int {
	t.Helper()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	munID, err := database.CreateMunicipi(&db.Municipi{
		Nom:            "Municipi F33-15 " + suffix,
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("no s'ha pogut crear municipi F33-15: %v", err)
	}
	eclesID, err := database.CreateArquebisbat(&db.Arquebisbat{
		Nom:            "Bisbat F33-15 " + suffix,
		TipusEntitat:   "bisbat",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("no s'ha pogut crear arquebisbat F33-15: %v", err)
	}
	arxiuID, err := database.CreateArxiu(&db.Arxiu{
		Nom:                   "Arxiu F33-15 " + suffix,
		Tipus:                 "parroquia",
		Acces:                 "online",
		MunicipiID:            sql.NullInt64{Int64: int64(munID), Valid: true},
		EntitatEclesiasticaID: sql.NullInt64{Int64: int64(eclesID), Valid: true},
		ModeracioEstat:        "publicat",
	})
	if err != nil {
		t.Fatalf("no s'ha pogut crear arxiu F33-15: %v", err)
	}
	llibreID, err := database.CreateLlibre(&db.Llibre{
		ArquebisbatID:  eclesID,
		MunicipiID:     munID,
		Titol:          "Llibre F33-15 " + suffix,
		TipusLlibre:    "naixements",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("no s'ha pogut crear llibre F33-15: %v", err)
	}
	if err := database.AddArxiuLlibre(arxiuID, llibreID, "", ""); err != nil {
		t.Fatalf("no s'ha pogut vincular arxiu/llibre F33-15: %v", err)
	}
	return llibreID
}

func runF3315ConvertRegistre(app *App, session *http.Cookie, registreID, rawPersonID int) *httptest.ResponseRecorder {
	csrf := "csrf_f33_15_" + strconv.Itoa(registreID)
	form := url.Values{}
	form.Set("csrf_token", csrf)
	form.Set("raw_person_id", strconv.Itoa(rawPersonID))
	form.Set("return_to", "/done")
	req := httptest.NewRequest(http.MethodPost, "/documentals/registres/"+strconv.Itoa(registreID)+"/convert-persona", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: csrf})
	rr := httptest.NewRecorder()
	app.AdminConvertRegistreToPersona(rr, req)
	return rr
}

func createF3313SessionCookie(t *testing.T, database db.DB, userID int, sessionID string) *http.Cookie {
	t.Helper()
	expiry := time.Now().Add(24 * time.Hour).Format(time.RFC3339)
	if err := database.SaveSession(sessionID, userID, expiry); err != nil {
		t.Fatalf("no s'ha pogut crear sessio F33-13: %v", err)
	}
	return &http.Cookie{Name: "cg_session", Value: sessionID, Path: "/"}
}

func f3313SuggestHandlers(app *App) map[string]func(http.ResponseWriter, *http.Request) {
	return map[string]func(http.ResponseWriter, *http.Request){
		"AdminPaisosSuggest":              app.AdminPaisosSuggest,
		"AdminMunicipisSuggest":           app.AdminMunicipisSuggest,
		"AdminNivellsSuggest":             app.AdminNivellsSuggest,
		"AdminNivellAdministratiuSuggest": app.AdminNivellAdministratiuSuggest,
		"AdminEclesSuggest":               app.AdminEclesSuggest,
	}
}

func runF3313SuggestHandler(handler func(http.ResponseWriter, *http.Request), session *http.Cookie) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/suggest?q=a", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	handler(rr, req)
	return rr
}
