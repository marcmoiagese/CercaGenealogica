package integration

import (
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func createHistoriaMunicipi(t *testing.T, database db.DB, userID int) int {
	t.Helper()

	mun := &db.Municipi{
		Nom:            fmt.Sprintf("Municipi Historia %d", time.Now().UnixNano()),
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	munID, err := database.CreateMunicipi(mun)
	if err != nil {
		t.Fatalf("CreateMunicipi ha fallat: %v", err)
	}
	return munID
}

func createPolicyWithGrant(t *testing.T, database db.DB, name, permKey string) int {
	t.Helper()

	policy := &db.Politica{
		Nom:         name,
		Descripcio:  "test policy",
	}
	policyID, err := database.SavePolitica(policy)
	if err != nil {
		t.Fatalf("SavePolitica ha fallat: %v", err)
	}
	grant := &db.PoliticaGrant{
		PoliticaID:      policyID,
		PermKey:         permKey,
		ScopeType:       string(core.ScopeGlobal),
		ScopeID:         sql.NullInt64{},
		IncludeChildren: false,
	}
	if _, err := database.SavePoliticaGrant(grant); err != nil {
		t.Fatalf("SavePoliticaGrant ha fallat: %v", err)
	}
	return policyID
}

func assignPolicyToUser(t *testing.T, database db.DB, userID, policyID int) {
	t.Helper()
	if err := database.AddUserPolitica(userID, policyID); err != nil {
		t.Fatalf("AddUserPolitica ha fallat: %v", err)
	}
}

func parseLocationID(t *testing.T, location, key string) int {
	t.Helper()
	u, err := url.Parse(location)
	if err != nil {
		t.Fatalf("invalid redirect location: %v", err)
	}
	val := u.Query().Get(key)
	if val == "" {
		t.Fatalf("missing %s in redirect", key)
	}
	id, err := strconv.Atoi(val)
	if err != nil || id <= 0 {
		t.Fatalf("invalid %s in redirect: %s", key, val)
	}
	return id
}
