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
		grants, err := a.DB.ListPoliticaGrants(policy.ID)
		if err != nil {
			return err
		}
		if len(grants) > 0 {
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
		keys := legacyPermKeys(perms)
		if len(keys) == 0 {
			continue
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
				PoliticaID:      policy.ID,
				PermKey:         key,
				ScopeType:       string(ScopeGlobal),
				ScopeID:         sql.NullInt64{},
				IncludeChildren: false,
			}
			if _, err := a.DB.SavePoliticaGrant(grant); err != nil {
				return err
			}
		}
	}
	return nil
}
