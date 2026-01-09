package core

import (
	"database/sql"
	"encoding/json"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

// EnsurePolicyGrants migrates legacy JSON policies into global grants when no grants exist.
func (a *App) EnsurePolicyGrants() error {
	if a == nil || a.DB == nil {
		return nil
	}
	policies, err := a.DB.ListPolitiques()
	if err != nil {
		return err
	}
	for _, policy := range policies {
		if policy.ID <= 0 {
			continue
		}
		permsRaw := strings.TrimSpace(policy.Permisos)
		if permsRaw == "" {
			continue
		}
		var perms db.PolicyPermissions
		if err := json.Unmarshal([]byte(permsRaw), &perms); err != nil {
			continue
		}
		if err := a.ensurePolicyGrantsFromPerms(policy.ID, perms); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) ensurePolicyGrantsFromPerms(policyID int, perms db.PolicyPermissions) error {
	if a == nil || a.DB == nil || policyID <= 0 {
		return nil
	}
	grants, err := a.DB.ListPoliticaGrants(policyID)
	if err != nil {
		return err
	}
	if len(grants) > 0 {
		return nil
	}
	keys := legacyPermKeys(perms)
	if len(keys) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		grant := &db.PoliticaGrant{
			PoliticaID:      policyID,
			PermKey:         key,
			ScopeType:       string(ScopeGlobal),
			ScopeID:         sql.NullInt64{},
			IncludeChildren: false,
		}
		if _, err := a.DB.SavePoliticaGrant(grant); err != nil {
			return err
		}
	}
	return nil
}
