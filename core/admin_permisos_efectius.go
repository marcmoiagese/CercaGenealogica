package core

import (
	"database/sql"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type EffectivePermissionRow struct {
	SourceType      string
	PolicyID        int
	PolicyName      string
	GroupID         int
	GroupName       string
	PermKey         string
	ScopeType       string
	ScopeID         int
	ScopeIDValid    bool
	IncludeChildren bool
}

type EffectiveGroupPolicyView struct {
	Group    db.Group
	Policies []db.Politica
}

type GroupPermissionsViewData struct {
	Group    db.Group
	Members  []db.UserAdminRow
	Policies []db.Politica
	Grants   []EffectivePermissionRow
}

func (a *App) AdminPermisosEfectius(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKey(w, r, permKeyAdminPoliciesManage, PermissionTarget{})
	if !ok {
		return
	}
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}

	q := r.URL.Query()
	userID := parsePositiveQueryID(q.Get("user_id"))
	groupID := parsePositiveQueryID(q.Get("group_id"))
	userQuery := strings.TrimSpace(q.Get("user_q"))
	errMsg := ""

	users, _, err := a.loadUsersAdmin(db.UserAdminFilter{Query: userQuery, Limit: 250})
	if err != nil {
		Errorf("[admin_permisos_efectius] list users: %v", err)
		errMsg = "No s'han pogut carregar els usuaris."
	}
	groups, err := a.DB.ListGroups()
	if err != nil {
		Errorf("[admin_permisos_efectius] list groups: %v", err)
		if errMsg == "" {
			errMsg = "No s'han pogut carregar els grups de permisos."
		}
	}

	var selectedUser *db.UserAdminRow
	var directPolicies []db.Politica
	var userGroups []db.Group
	var inherited []EffectiveGroupPolicyView
	var effectiveRows []EffectivePermissionRow
	if userID > 0 {
		selectedUser, err = a.loadEffectiveUserRow(userID)
		if err != nil {
			Errorf("[admin_permisos_efectius] load selected user user_id=%d: %v", userID, err)
			if errMsg == "" {
				errMsg = "No s'ha pogut carregar l'usuari seleccionat."
			}
		} else if selectedUser == nil {
			if errMsg == "" {
				errMsg = "L'usuari seleccionat no existeix."
			}
			userID = 0
		}
	}
	if userID > 0 {
		directPolicies, userGroups, inherited, effectiveRows, err = a.buildUserEffectivePermissions(userID)
		if err != nil {
			Errorf("[admin_permisos_efectius] build user permissions user_id=%d: %v", userID, err)
			if errMsg == "" {
				errMsg = "No s'han pogut calcular els permisos efectius de l'usuari."
			}
		}
	}

	var groupDetail *GroupPermissionsViewData
	if groupID > 0 {
		groupDetail, err = a.buildGroupPermissionsDetail(groupID, groups)
		if err != nil {
			Errorf("[admin_permisos_efectius] build group detail group_id=%d: %v", groupID, err)
			if errMsg == "" {
				errMsg = "No s'ha pogut carregar el detall del grup."
			}
		}
	}

	RenderPrivateTemplate(w, r, "admin-politiques-permisos-efectius.html", map[string]interface{}{
		"Users":               users,
		"Groups":              groups,
		"UserID":              userID,
		"GroupID":             groupID,
		"UserQuery":           userQuery,
		"SelectedUser":        selectedUser,
		"DirectPolicies":      directPolicies,
		"UserGroups":          userGroups,
		"InheritedPolicies":   inherited,
		"EffectiveGrants":     effectiveRows,
		"GroupDetail":         groupDetail,
		"Error":               errMsg,
		"CanManageArxius":     true,
		"CanManagePolicies":   true,
		"User":                user,
		"AssignacionsPath":    adminAssignacionsPath,
		"PermisosEfectiusURL": "/admin/politiques/permisos-efectius",
	})
}

func parsePositiveQueryID(raw string) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	id, err := strconv.Atoi(raw)
	if err != nil || id <= 0 {
		return 0
	}
	return id
}

func (a *App) loadEffectiveUserRow(userID int) (*db.UserAdminRow, error) {
	rows, _, err := a.loadUsersAdmin(db.UserAdminFilter{UserID: userID, Limit: 1})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &rows[0], nil
}

func (a *App) buildUserEffectivePermissions(userID int) ([]db.Politica, []db.Group, []EffectiveGroupPolicyView, []EffectivePermissionRow, error) {
	directPolicies, err := a.DB.ListUserPolitiques(userID)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	groups, err := a.DB.ListUserGroups(userID)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	rows := []EffectivePermissionRow{}
	for _, pol := range directPolicies {
		grants, err := a.DB.ListPoliticaGrants(pol.ID)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		rows = append(rows, effectiveRowsFromGrants("direct", pol, db.Group{}, grants)...)
	}
	inherited := make([]EffectiveGroupPolicyView, 0, len(groups))
	for _, group := range groups {
		policies, err := a.DB.ListGroupPolitiques(group.ID)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		inherited = append(inherited, EffectiveGroupPolicyView{Group: group, Policies: policies})
		for _, pol := range policies {
			grants, err := a.DB.ListPoliticaGrants(pol.ID)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			rows = append(rows, effectiveRowsFromGrants("group", pol, group, grants)...)
		}
	}
	return directPolicies, groups, inherited, rows, nil
}

func (a *App) buildGroupPermissionsDetail(groupID int, groups []db.Group) (*GroupPermissionsViewData, error) {
	var selected db.Group
	for _, group := range groups {
		if group.ID == groupID {
			selected = group
			break
		}
	}
	if selected.ID == 0 {
		allGroups, err := a.DB.ListGroups()
		if err != nil {
			return nil, err
		}
		for _, group := range allGroups {
			if group.ID == groupID {
				selected = group
				break
			}
		}
	}
	if selected.ID == 0 {
		return nil, sql.ErrNoRows
	}
	members, err := a.DB.ListGroupMembers(groupID)
	if err != nil {
		return nil, err
	}
	policies, err := a.DB.ListGroupPolitiques(groupID)
	if err != nil {
		return nil, err
	}
	rows := []EffectivePermissionRow{}
	for _, pol := range policies {
		grants, err := a.DB.ListPoliticaGrants(pol.ID)
		if err != nil {
			return nil, err
		}
		rows = append(rows, effectiveRowsFromGrants("group", pol, selected, grants)...)
	}
	return &GroupPermissionsViewData{
		Group:    selected,
		Members:  members,
		Policies: policies,
		Grants:   rows,
	}, nil
}

func effectiveRowsFromGrants(sourceType string, pol db.Politica, group db.Group, grants []db.PoliticaGrant) []EffectivePermissionRow {
	rows := make([]EffectivePermissionRow, 0, len(grants))
	for _, grant := range grants {
		row := EffectivePermissionRow{
			SourceType:      sourceType,
			PolicyID:        pol.ID,
			PolicyName:      pol.Nom,
			GroupID:         group.ID,
			GroupName:       group.Nom,
			PermKey:         strings.TrimSpace(grant.PermKey),
			ScopeType:       strings.TrimSpace(grant.ScopeType),
			IncludeChildren: grant.IncludeChildren,
		}
		if grant.ScopeID.Valid {
			row.ScopeID = int(grant.ScopeID.Int64)
			row.ScopeIDValid = true
		}
		rows = append(rows, row)
	}
	return rows
}
