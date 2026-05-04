package core

import "strings"

// EnsurePolicyGrants materializes modular policy documents into grants when no grants exist.
// Legacy permission flags are intentionally ignored and never grant permissions.
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
		doc, _, err := parsePolicyDocument(permsRaw)
		if err != nil {
			continue
		}
		if err := a.ensurePolicyGrantsFromDocument(policy.ID, doc); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) ensurePolicyGrantsFromDocument(policyID int, doc policyDocument) error {
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
	if len(doc.Statement) > 0 {
		return a.replacePolicyGrantsFromDocument(policyID, doc)
	}
	return nil
}
