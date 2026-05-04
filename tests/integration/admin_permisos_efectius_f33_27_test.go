package integration

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF3327AdminPermisosEfectiusShowsDirectAndGroupOrigins(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f33_27_permisos_efectius.sqlite3")
	target := createTestUser(t, database, "f33_27_target_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	plain := createTestUser(t, database, "f33_27_plain_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	_, adminCookie := createF335PlatformUser(t, database, "f33_27_admin", "admin.policies.manage")
	plainCookie := createSessionCookie(t, database, plain.ID, "sess_f33_27_plain_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	directPolicyID := createF3327PolicyWithGrant(t, database, "f33_27_direct_policy", "admin.audit.view", "global", 0, false)
	groupPolicyID := createF3327PolicyWithGrant(t, database, "f33_27_group_policy", "admin.audit.view", "global", 0, false)
	scopedPolicyID := createF3327PolicyWithGrant(t, database, "f33_27_group_scoped_policy", "territori.municipis.edit", "nivell", 7, true)
	if err := database.AddUserPolitica(target.ID, directPolicyID); err != nil {
		t.Fatalf("AddUserPolitica F33-27 ha fallat: %v", err)
	}
	groupID, err := database.CreateGroup("f33_27_group", "grup F33-27")
	if err != nil {
		t.Fatalf("CreateGroup F33-27 ha fallat: %v", err)
	}
	if err := database.AddUserGroup(target.ID, groupID); err != nil {
		t.Fatalf("AddUserGroup F33-27 ha fallat: %v", err)
	}
	if err := database.AddGroupPolitica(groupID, groupPolicyID); err != nil {
		t.Fatalf("AddGroupPolitica F33-27 ha fallat: %v", err)
	}
	if err := database.AddGroupPolitica(groupID, scopedPolicyID); err != nil {
		t.Fatalf("AddGroupPolitica scoped F33-27 ha fallat: %v", err)
	}

	beforeVersion, err := database.GetUserPermissionsVersion(target.ID)
	if err != nil {
		t.Fatalf("GetUserPermissionsVersion abans F33-27 ha fallat: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/admin/politiques/permisos-efectius?user_id="+strconv.Itoa(target.ID)+"&group_id="+strconv.Itoa(groupID), nil)
	req.AddCookie(adminCookie)
	rr := httptest.NewRecorder()
	app.AdminPermisosEfectius(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("visor F33-27 status=%d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	for _, want := range []string{
		"Visor de permisos efectius",
		"Grants efectius",
		"Detall del grup",
		"source_type",
		"policy_id",
		"group_id",
		"direct",
		"group",
		"f33_27_direct_policy",
		"f33_27_group_policy",
		"f33_27_group_scoped_policy",
		"f33_27_group",
		"territori.municipis.edit",
		"nivell",
		"true",
		"@f33_27_target_",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("visor F33-27 no conte %q", want)
		}
	}
	if strings.Count(body, "admin.audit.view") < 2 {
		t.Fatalf("el visor ha de mostrar el mateix grant per origen directe i de grup: %s", body)
	}

	afterVersion, err := database.GetUserPermissionsVersion(target.ID)
	if err != nil {
		t.Fatalf("GetUserPermissionsVersion despres F33-27 ha fallat: %v", err)
	}
	if afterVersion != beforeVersion {
		t.Fatalf("el visor read-only no hauria de canviar permissions_version: before=%d after=%d", beforeVersion, afterVersion)
	}

	req = httptest.NewRequest(http.MethodGet, "/admin/politiques/permisos-efectius?user_id="+strconv.Itoa(target.ID), nil)
	req.AddCookie(plainCookie)
	rr = httptest.NewRecorder()
	app.AdminPermisosEfectius(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("usuari no admin hauria de rebre 403 al visor F33-27, got=%d", rr.Code)
	}
}

func createF3327PolicyWithGrant(t *testing.T, database db.DB, name, permKey, scopeType string, scopeID int, includeChildren bool) int {
	t.Helper()
	policyID, err := database.SavePolitica(&db.Politica{Nom: name, Descripcio: "F33-27"})
	if err != nil {
		t.Fatalf("SavePolitica F33-27 ha fallat: %v", err)
	}
	grant := &db.PoliticaGrant{
		PoliticaID:      policyID,
		PermKey:         permKey,
		ScopeType:       scopeType,
		IncludeChildren: includeChildren,
	}
	if scopeID > 0 {
		grant.ScopeID = sql.NullInt64{Int64: int64(scopeID), Valid: true}
	}
	if _, err := database.SavePoliticaGrant(grant); err != nil {
		t.Fatalf("SavePoliticaGrant F33-27 ha fallat: %v", err)
	}
	return policyID
}
