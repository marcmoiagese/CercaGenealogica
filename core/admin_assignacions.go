package core

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const adminAssignacionsPath = "/admin/politiques/assignacions"

func (a *App) AdminAssignacionsPolitiques(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminPoliciesManage, PermissionTarget{}); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	_ = r.ParseForm()
	userID, _ := strconv.Atoi(r.FormValue("user_id"))
	groupID, _ := strconv.Atoi(r.FormValue("group_id"))
	userQuery := strings.TrimSpace(r.FormValue("user_q"))
	userPage := 1
	if pStr := strings.TrimSpace(r.FormValue("user_page")); pStr != "" {
		if v, err := strconv.Atoi(pStr); err == nil && v > 0 {
			userPage = v
		}
	}
	userPerPage := parseAdminUsersPerPage(r.FormValue("user_per_page"))
	if userPerPage <= 0 {
		userPerPage = 10
	}
	userOffset := (userPage - 1) * userPerPage
	notice := adminAssignacionsNotice(r.FormValue("ok"))
	errMsg := adminAssignacionsError(r.FormValue("err"))

	politiques, err := a.DB.ListPolitiques()
	if err != nil {
		Errorf("[admin_assignacions] list politiques: %v", err)
		if errMsg == "" {
			errMsg = "No s'han pogut carregar les politiques."
		}
	}
	grups, err := a.DB.ListGroups()
	if err != nil {
		Errorf("[admin_assignacions] list groups: %v", err)
		if errMsg == "" {
			errMsg = "No s'han pogut carregar els grups de permisos."
		}
	}
	userFilter := db.UserAdminFilter{
		Query:  userQuery,
		Limit:  userPerPage,
		Offset: userOffset,
	}
	userRows, userTotal, err := a.loadUsersAdmin(userFilter)
	if err != nil {
		Errorf("[admin_assignacions] list users: %v", err)
		if errMsg == "" {
			errMsg = "No s'han pogut carregar els usuaris."
		}
	}
	users := make([]adminUserRowView, 0, len(userRows))
	for _, row := range userRows {
		users = append(users, adminUserRowView{
			ID:        row.ID,
			Usuari:    row.Usuari,
			Nom:       row.Nom,
			Cognoms:   row.Cognoms,
			Email:     row.Email,
			CreatedAt: formatDateTimeDisplay(row.CreatedAt),
			LastLogin: formatDateTimeDisplay(row.LastLogin),
			Active:    row.Active,
			Banned:    row.Banned,
		})
	}

	var userPols []db.Politica
	if userID > 0 {
		userPols, err = a.DB.ListUserPolitiques(userID)
		if err != nil {
			Errorf("[admin_assignacions] list user politiques user_id=%d: %v", userID, err)
			if errMsg == "" {
				errMsg = "No s'han pogut carregar les politiques de l'usuari."
			}
		}
	}
	var groupPols []db.Politica
	if groupID > 0 {
		groupPols, err = a.DB.ListGroupPolitiques(groupID)
		if err != nil {
			Errorf("[admin_assignacions] list group politiques group_id=%d: %v", groupID, err)
			if errMsg == "" {
				errMsg = "No s'han pogut carregar les politiques del grup."
			}
		}
	}
	var userGroups []db.Group
	if userID > 0 {
		userGroups, err = a.DB.ListUserGroups(userID)
		if err != nil {
			Errorf("[admin_assignacions] list user groups user_id=%d: %v", userID, err)
			if errMsg == "" {
				errMsg = "No s'han pogut carregar els grups de permisos de l'usuari."
			}
		}
	}
	var groupMembers []db.UserAdminRow
	if groupID > 0 {
		groupMembers, err = a.DB.ListGroupMembers(groupID)
		if err != nil {
			Errorf("[admin_assignacions] list group members group_id=%d: %v", groupID, err)
			if errMsg == "" {
				errMsg = "No s'han pogut carregar els membres del grup de permisos."
			}
		}
	}
	var selectedUser *adminUserRowView
	if userID > 0 {
		for _, row := range users {
			if row.ID == userID {
				copyRow := row
				selectedUser = &copyRow
				break
			}
		}
		if selectedUser == nil {
			userFilter.UserID = userID
			if rows, _, err := a.loadUsersAdmin(userFilter); err == nil && len(rows) > 0 {
				selectedUser = &adminUserRowView{
					ID:        rows[0].ID,
					Usuari:    rows[0].Usuari,
					Nom:       rows[0].Nom,
					Cognoms:   rows[0].Cognoms,
					Email:     rows[0].Email,
					CreatedAt: formatDateTimeDisplay(rows[0].CreatedAt),
					LastLogin: formatDateTimeDisplay(rows[0].LastLogin),
					Active:    rows[0].Active,
					Banned:    rows[0].Banned,
				}
			} else if err != nil {
				Errorf("[admin_assignacions] load selected user user_id=%d: %v", userID, err)
				if errMsg == "" {
					errMsg = "No s'ha pogut carregar l'usuari seleccionat."
				}
			}
		}
	}
	userTotalPages := 1
	if userPerPage > 0 && userTotal > 0 {
		userTotalPages = (userTotal + userPerPage - 1) / userPerPage
	}
	if userTotalPages <= 0 {
		userTotalPages = 1
	}
	userRangeStart := 0
	userRangeEnd := 0
	if userTotal > 0 {
		userRangeStart = userOffset + 1
		if userRangeStart > userTotal {
			userRangeStart = userTotal
		}
		userRangeEnd = userOffset + len(userRows)
		if userRangeEnd > userTotal {
			userRangeEnd = userTotal
		}
	}
	userPageBaseValues := cloneValues(r.URL.Query())
	userPageBaseValues.Del("user_page")
	userPageBaseValues.Del("ok")
	userPageBaseValues.Del("err")
	userPageBase := userPageBaseValues.Encode()
	userSelectValues := cloneValues(r.URL.Query())
	userSelectValues.Del("user_id")
	userSelectValues.Del("user_page")
	userSelectValues.Del("ok")
	userSelectValues.Del("err")
	userSelectBase := userSelectValues.Encode()
	currentValues := cloneValues(r.URL.Query())
	currentValues.Del("ok")
	currentValues.Del("err")
	currentURL := r.URL.Path
	if encoded := currentValues.Encode(); encoded != "" {
		currentURL += "?" + encoded
	}

	RenderPrivateTemplate(w, r, "admin-politiques-assignacions.html", map[string]interface{}{
		"Users":             users,
		"Groups":            grups,
		"Politiques":        politiques,
		"UserID":            userID,
		"GroupID":           groupID,
		"UserPols":          userPols,
		"GroupPols":         groupPols,
		"UserGroups":        userGroups,
		"GroupMembers":      groupMembers,
		"SelectedUser":      selectedUser,
		"UserQuery":         userQuery,
		"UserPage":          userPage,
		"UserPerPage":       userPerPage,
		"UserTotal":         userTotal,
		"UserTotalPages":    userTotalPages,
		"UserHasPrev":       userPage > 1,
		"UserHasNext":       userPage < userTotalPages,
		"UserPrevPage":      userPage - 1,
		"UserNextPage":      userPage + 1,
		"UserPageBase":      userPageBase,
		"UserSelectBase":    userSelectBase,
		"UserRangeStart":    userRangeStart,
		"UserRangeEnd":      userRangeEnd,
		"ReturnTo":          currentURL,
		"Notice":            notice,
		"Error":             errMsg,
		"CanManageArxius":   true,
		"CanManagePolicies": true,
		"User":              user,
	})
}

func (a *App) AdminCreateGroup(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminPoliciesManage, PermissionTarget{}); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	_ = r.ParseForm()
	name := strings.TrimSpace(r.FormValue("name"))
	desc := strings.TrimSpace(r.FormValue("description"))
	if name == "" {
		a.redirectAdminAssignacions(w, r, 0, 0, "", "group_name_required")
		return
	}
	groupID, err := a.DB.CreateGroup(name, desc)
	if err != nil {
		Errorf("[admin_assignacions] create group name=%q: %v", name, err)
		a.redirectAdminAssignacions(w, r, 0, 0, "", "create_group_failed")
		return
	}
	http.Redirect(w, r, adminAssignacionsRedirectURL(adminAssignacionsPath, 0, groupID, "group_created", ""), http.StatusSeeOther)
}

func (a *App) AdminAssignarUsuariGrup(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminPoliciesManage, PermissionTarget{}); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	_ = r.ParseForm()
	userID, _ := strconv.Atoi(r.FormValue("user_id"))
	groupID, _ := strconv.Atoi(r.FormValue("group_id"))
	if userID <= 0 || groupID <= 0 {
		a.redirectAdminAssignacions(w, r, userID, groupID, "", "invalid")
		return
	}
	if err := a.DB.AddUserGroup(userID, groupID); err != nil {
		Errorf("[admin_assignacions] add user group user_id=%d group_id=%d: %v", userID, groupID, err)
		a.redirectAdminAssignacions(w, r, userID, groupID, "", "add_user_group_failed")
		return
	}
	a.redirectAdminAssignacions(w, r, userID, groupID, "user_group_added", "")
}

func (a *App) AdminTreureUsuariGrup(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminPoliciesManage, PermissionTarget{}); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	_ = r.ParseForm()
	userID, _ := strconv.Atoi(r.FormValue("user_id"))
	groupID, _ := strconv.Atoi(r.FormValue("group_id"))
	if userID <= 0 || groupID <= 0 {
		a.redirectAdminAssignacions(w, r, userID, groupID, "", "invalid")
		return
	}
	if err := a.DB.RemoveUserGroup(userID, groupID); err != nil {
		Errorf("[admin_assignacions] remove user group user_id=%d group_id=%d: %v", userID, groupID, err)
		a.redirectAdminAssignacions(w, r, userID, groupID, "", "remove_user_group_failed")
		return
	}
	a.redirectAdminAssignacions(w, r, userID, groupID, "user_group_removed", "")
}

func (a *App) AdminAssignarPoliticaUsuari(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminPoliciesManage, PermissionTarget{}); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	_ = r.ParseForm()
	userID, _ := strconv.Atoi(r.FormValue("user_id"))
	polID, _ := strconv.Atoi(r.FormValue("politica_id"))
	if userID <= 0 || polID <= 0 {
		a.redirectAdminAssignacions(w, r, userID, 0, "", "invalid")
		return
	}
	if err := a.DB.AddUserPolitica(userID, polID); err != nil {
		Errorf("[admin_assignacions] add user policy user_id=%d politica_id=%d: %v", userID, polID, err)
		a.redirectAdminAssignacions(w, r, userID, 0, "", "add_user_policy_failed")
		return
	}
	a.redirectAdminAssignacions(w, r, userID, 0, "user_policy_added", "")
}

func (a *App) AdminTreurePoliticaUsuari(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminPoliciesManage, PermissionTarget{}); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	_ = r.ParseForm()
	userID, _ := strconv.Atoi(r.FormValue("user_id"))
	polID, _ := strconv.Atoi(r.FormValue("politica_id"))
	if userID <= 0 || polID <= 0 {
		a.redirectAdminAssignacions(w, r, userID, 0, "", "invalid")
		return
	}
	if err := a.DB.RemoveUserPolitica(userID, polID); err != nil {
		Errorf("[admin_assignacions] remove user policy user_id=%d politica_id=%d: %v", userID, polID, err)
		a.redirectAdminAssignacions(w, r, userID, 0, "", "remove_user_policy_failed")
		return
	}
	a.redirectAdminAssignacions(w, r, userID, 0, "user_policy_removed", "")
}

func (a *App) AdminAssignarPoliticaGrup(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminPoliciesManage, PermissionTarget{}); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	_ = r.ParseForm()
	groupID, _ := strconv.Atoi(r.FormValue("group_id"))
	polID, _ := strconv.Atoi(r.FormValue("politica_id"))
	if groupID <= 0 || polID <= 0 {
		a.redirectAdminAssignacions(w, r, 0, groupID, "", "invalid")
		return
	}
	if err := a.DB.AddGroupPolitica(groupID, polID); err != nil {
		Errorf("[admin_assignacions] add group policy group_id=%d politica_id=%d: %v", groupID, polID, err)
		a.redirectAdminAssignacions(w, r, 0, groupID, "", "add_group_policy_failed")
		return
	}
	if err := a.DB.BumpGroupPermissionsVersion(groupID); err != nil {
		Errorf("[admin_assignacions] bump group permissions group_id=%d after add policy: %v", groupID, err)
		a.redirectAdminAssignacions(w, r, 0, groupID, "", "bump_failed")
		return
	}
	a.redirectAdminAssignacions(w, r, 0, groupID, "group_policy_added", "")
}

func (a *App) AdminTreurePoliticaGrup(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminPoliciesManage, PermissionTarget{}); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	_ = r.ParseForm()
	groupID, _ := strconv.Atoi(r.FormValue("group_id"))
	polID, _ := strconv.Atoi(r.FormValue("politica_id"))
	if groupID <= 0 || polID <= 0 {
		a.redirectAdminAssignacions(w, r, 0, groupID, "", "invalid")
		return
	}
	if err := a.DB.RemoveGroupPolitica(groupID, polID); err != nil {
		Errorf("[admin_assignacions] remove group policy group_id=%d politica_id=%d: %v", groupID, polID, err)
		a.redirectAdminAssignacions(w, r, 0, groupID, "", "remove_group_policy_failed")
		return
	}
	if err := a.DB.BumpGroupPermissionsVersion(groupID); err != nil {
		Errorf("[admin_assignacions] bump group permissions group_id=%d after remove policy: %v", groupID, err)
		a.redirectAdminAssignacions(w, r, 0, groupID, "", "bump_failed")
		return
	}
	a.redirectAdminAssignacions(w, r, 0, groupID, "group_policy_removed", "")
}

func (a *App) redirectAdminAssignacions(w http.ResponseWriter, r *http.Request, userID, groupID int, okCode, errCode string) {
	http.Redirect(w, r, adminAssignacionsRedirectURL(r.FormValue("return_to"), userID, groupID, okCode, errCode), http.StatusSeeOther)
}

func adminAssignacionsRedirectURL(raw string, userID, groupID int, okCode, errCode string) string {
	target := safeReturnTo(raw, adminAssignacionsPath)
	u, err := url.Parse(target)
	if err != nil || u.Path == "" {
		u = &url.URL{Path: adminAssignacionsPath}
	}
	q := u.Query()
	if userID > 0 {
		q.Set("user_id", strconv.Itoa(userID))
	}
	if groupID > 0 {
		q.Set("group_id", strconv.Itoa(groupID))
	}
	q.Del("ok")
	q.Del("err")
	if okCode != "" {
		q.Set("ok", okCode)
	}
	if errCode != "" {
		q.Set("err", errCode)
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func adminAssignacionsNotice(code string) string {
	switch code {
	case "group_created":
		return "Grup de permisos creat."
	case "user_group_added":
		return "Usuari afegit al grup de permisos."
	case "user_group_removed":
		return "Usuari retirat del grup de permisos."
	case "user_policy_added":
		return "Politica assignada a l'usuari."
	case "user_policy_removed":
		return "Politica retirada de l'usuari."
	case "group_policy_added":
		return "Politica assignada al grup de permisos."
	case "group_policy_removed":
		return "Politica retirada del grup de permisos."
	default:
		return ""
	}
}

func adminAssignacionsError(code string) string {
	switch code {
	case "invalid":
		return "La peticio no es valida."
	case "group_name_required":
		return "Cal indicar el nom del grup de permisos."
	case "create_group_failed":
		return "No s'ha pogut crear el grup de permisos."
	case "add_user_group_failed":
		return "No s'ha pogut afegir l'usuari al grup de permisos."
	case "remove_user_group_failed":
		return "No s'ha pogut retirar l'usuari del grup de permisos."
	case "add_user_policy_failed":
		return "No s'ha pogut assignar la politica a l'usuari."
	case "remove_user_policy_failed":
		return "No s'ha pogut retirar la politica de l'usuari."
	case "add_group_policy_failed":
		return "No s'ha pogut assignar la politica al grup de permisos."
	case "remove_group_policy_failed":
		return "No s'ha pogut retirar la politica del grup de permisos."
	case "bump_failed":
		return "S'ha fet el canvi, pero no s'ha pogut invalidar la cache de permisos."
	default:
		return ""
	}
}
