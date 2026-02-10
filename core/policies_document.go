package core

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const policyDocumentVersion = "2024-02-07"

type policyDocument struct {
	db.PolicyPermissions
	Version   string            `json:"Version,omitempty"`
	Statement []policyStatement `json:"Statement,omitempty"`
}

type policyStatement struct {
	Effect   string   `json:"Effect,omitempty"`
	Action   []string `json:"Action,omitempty"`
	Resource []string `json:"Resource,omitempty"`
}

type policyStatementRaw struct {
	Effect   string          `json:"Effect,omitempty"`
	Action   json.RawMessage `json:"Action,omitempty"`
	Resource json.RawMessage `json:"Resource,omitempty"`
}

type policyDocumentRaw struct {
	db.PolicyPermissions
	Version   string               `json:"Version,omitempty"`
	Statement []policyStatementRaw `json:"Statement,omitempty"`
}

func parsePolicyDocument(raw string) (policyDocument, bool, error) {
	doc := policyDocument{Version: policyDocumentVersion}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return doc, false, nil
	}
	var parsed policyDocumentRaw
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return doc, false, err
	}
	doc.PolicyPermissions = parsed.PolicyPermissions
	if strings.TrimSpace(parsed.Version) != "" {
		doc.Version = strings.TrimSpace(parsed.Version)
	}
	hasStatements := len(parsed.Statement) > 0
	for _, stmt := range parsed.Statement {
		normalized, ok, err := normalizePolicyStatement(stmt)
		if err != nil {
			return doc, hasStatements, err
		}
		if ok {
			doc.Statement = append(doc.Statement, normalized)
		}
	}
	return doc, hasStatements, nil
}

func normalizePolicyStatement(stmt policyStatementRaw) (policyStatement, bool, error) {
	effect := strings.TrimSpace(stmt.Effect)
	if effect == "" {
		effect = "Allow"
	}
	if !strings.EqualFold(effect, "allow") {
		return policyStatement{}, false, fmt.Errorf("unsupported effect")
	}
	actions, err := parsePolicyStringList(stmt.Action)
	if err != nil {
		return policyStatement{}, false, err
	}
	if len(actions) == 0 {
		return policyStatement{}, false, nil
	}
	resources, err := parsePolicyStringList(stmt.Resource)
	if err != nil {
		return policyStatement{}, false, err
	}
	if len(resources) == 0 {
		resources = []string{"global"}
	}
	return policyStatement{
		Effect:   "Allow",
		Action:   actions,
		Resource: resources,
	}, true, nil
}

func parsePolicyStringList(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var single string
	if err := json.Unmarshal(raw, &single); err == nil {
		single = strings.TrimSpace(single)
		if single == "" {
			return nil, nil
		}
		return []string{single}, nil
	}
	var list []string
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(list))
	for _, item := range list {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		out = append(out, item)
	}
	return out, nil
}

func policyStatementsFromGrants(grants []db.PoliticaGrant) []policyStatement {
	if len(grants) == 0 {
		return nil
	}
	byKey := map[string]map[string]struct{}{}
	for _, g := range grants {
		permKey := strings.TrimSpace(g.PermKey)
		if permKey == "" {
			continue
		}
		resource := policyResourceString(g)
		if resource == "" {
			continue
		}
		if _, ok := byKey[permKey]; !ok {
			byKey[permKey] = map[string]struct{}{}
		}
		byKey[permKey][resource] = struct{}{}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]policyStatement, 0, len(keys))
	for _, key := range keys {
		resSet := byKey[key]
		resources := make([]string, 0, len(resSet))
		for res := range resSet {
			resources = append(resources, res)
		}
		sort.Strings(resources)
		out = append(out, policyStatement{
			Effect:   "Allow",
			Action:   []string{key},
			Resource: resources,
		})
	}
	return out
}

func policyDocumentFromGrants(perms db.PolicyPermissions, grants []db.PoliticaGrant, version string) policyDocument {
	doc := policyDocument{
		PolicyPermissions: perms,
		Version:           strings.TrimSpace(version),
		Statement:         policyStatementsFromGrants(grants),
	}
	if doc.Version == "" {
		doc.Version = policyDocumentVersion
	}
	return doc
}

func policyDocumentJSON(doc policyDocument) (string, error) {
	payload, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func policyJSONForForm(raw string, grants []db.PoliticaGrant) string {
	doc, _, err := parsePolicyDocument(raw)
	if err != nil {
		doc = policyDocument{Version: policyDocumentVersion}
	}
	doc.Statement = policyStatementsFromGrants(grants)
	payload, err := policyDocumentJSON(doc)
	if err != nil {
		return raw
	}
	return payload
}

func policyResourceString(g db.PoliticaGrant) string {
	scopeType, ok := parseScopeType(g.ScopeType)
	if !ok {
		return ""
	}
	if scopeType == ScopeGlobal {
		return "global"
	}
	if !g.ScopeID.Valid || g.ScopeID.Int64 <= 0 {
		return ""
	}
	resource := fmt.Sprintf("%s:%d", string(scopeType), g.ScopeID.Int64)
	if g.IncludeChildren {
		resource += "/*"
	}
	return resource
}

func policyGrantsFromDocument(doc policyDocument) ([]db.PoliticaGrant, error) {
	if len(doc.Statement) == 0 {
		return nil, nil
	}
	actionKeys := permissionCatalog()
	actionSet := map[string]struct{}{}
	for _, key := range actionKeys {
		actionSet[key] = struct{}{}
	}
	type grantKey struct {
		permKey         string
		scopeType       string
		scopeID         int
		includeChildren bool
	}
	seen := map[grantKey]struct{}{}
	out := []db.PoliticaGrant{}
	for _, stmt := range doc.Statement {
		if stmt.Effect != "" && !strings.EqualFold(stmt.Effect, "allow") {
			return nil, fmt.Errorf("unsupported effect")
		}
		actions, err := expandPolicyActions(stmt.Action, actionSet)
		if err != nil {
			return nil, err
		}
		if len(actions) == 0 {
			continue
		}
		resources := stmt.Resource
		if len(resources) == 0 {
			resources = []string{"global"}
		}
		for _, action := range actions {
			for _, res := range resources {
				scopeType, scopeID, includeChildren, err := parsePolicyResource(res)
				if err != nil {
					return nil, err
				}
				key := grantKey{
					permKey:         action,
					scopeType:       string(scopeType),
					scopeID:         scopeID,
					includeChildren: includeChildren,
				}
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				grant := db.PoliticaGrant{
					PermKey:         action,
					ScopeType:       string(scopeType),
					IncludeChildren: includeChildren,
				}
				if scopeType != ScopeGlobal {
					grant.ScopeID = sqlNullIntFromInt(scopeID)
				}
				out = append(out, grant)
			}
		}
	}
	return out, nil
}

func expandPolicyActions(actions []string, actionSet map[string]struct{}) ([]string, error) {
	if len(actions) == 0 {
		return nil, nil
	}
	out := []string{}
	for _, action := range actions {
		action = strings.TrimSpace(action)
		if action == "" {
			continue
		}
		if action == "*" {
			for key := range actionSet {
				out = append(out, key)
			}
			continue
		}
		if _, ok := actionSet[action]; !ok {
			return nil, fmt.Errorf("unknown action")
		}
		out = append(out, action)
	}
	if len(out) == 0 {
		return nil, nil
	}
	sort.Strings(out)
	uniq := out[:0]
	last := ""
	for _, item := range out {
		if item == last {
			continue
		}
		last = item
		uniq = append(uniq, item)
	}
	return uniq, nil
}

func parsePolicyResource(raw string) (ScopeType, int, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "*" || raw == "global" {
		return ScopeGlobal, 0, false, nil
	}
	includeChildren := false
	if strings.HasSuffix(raw, "/*") {
		includeChildren = true
		raw = strings.TrimSuffix(raw, "/*")
	}
	parts := strings.SplitN(raw, ":", 2)
	if len(parts) != 2 {
		return "", 0, false, fmt.Errorf("invalid resource")
	}
	scopeKey := strings.ToLower(strings.TrimSpace(parts[0]))
	switch scopeKey {
	case "entitat-eclesiastica", "ecles":
		scopeKey = "entitat_eclesiastica"
	}
	scopeType, ok := parseScopeType(scopeKey)
	if !ok || scopeType == ScopeGlobal {
		return "", 0, false, fmt.Errorf("invalid resource")
	}
	id, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil || id <= 0 {
		return "", 0, false, fmt.Errorf("invalid resource")
	}
	return scopeType, id, includeChildren, nil
}

func (a *App) replacePolicyGrantsFromDocument(policyID int, doc policyDocument) error {
	if a == nil || a.DB == nil || policyID <= 0 {
		return nil
	}
	grants, err := policyGrantsFromDocument(doc)
	if err != nil {
		return err
	}
	existing, err := a.DB.ListPoliticaGrants(policyID)
	if err != nil {
		return err
	}
	for _, g := range existing {
		if g.ID <= 0 {
			continue
		}
		if err := a.DB.DeletePoliticaGrant(g.ID); err != nil {
			return err
		}
	}
	for _, grant := range grants {
		grant.PoliticaID = policyID
		if _, err := a.DB.SavePoliticaGrant(&grant); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) refreshPolicyPermsJSON(policyID int) error {
	if a == nil || a.DB == nil || policyID <= 0 {
		return nil
	}
	pol, err := a.DB.GetPolitica(policyID)
	if err != nil || pol == nil {
		return err
	}
	doc, _, err := parsePolicyDocument(pol.Permisos)
	if err != nil {
		doc = policyDocument{Version: policyDocumentVersion}
	}
	grants, err := a.DB.ListPoliticaGrants(policyID)
	if err != nil {
		return err
	}
	doc.Statement = policyStatementsFromGrants(grants)
	payload, err := policyDocumentJSON(doc)
	if err != nil {
		return err
	}
	pol.Permisos = payload
	_, err = a.DB.SavePolitica(pol)
	return err
}
