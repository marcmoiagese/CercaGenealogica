package core

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func (a *App) AdminAssignacionsPolitiques(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permPolicies); !ok {
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

	politiques, _ := a.DB.ListPolitiques()
	grups, _ := a.DB.ListGroups()
	userFilter := db.UserAdminFilter{
		Query:  userQuery,
		Limit:  userPerPage,
		Offset: userOffset,
	}
	userRows, userTotal, _ := a.loadUsersAdmin(userFilter)
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
		userPols, _ = a.DB.ListUserPolitiques(userID)
	}
	var groupPols []db.Politica
	if groupID > 0 {
		groupPols, _ = a.DB.ListGroupPolitiques(groupID)
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
	currentURL := r.URL.Path
	if r.URL.RawQuery != "" {
		currentURL += "?" + r.URL.RawQuery
	}

	RenderPrivateTemplate(w, r, "admin-politiques-assignacions.html", map[string]interface{}{
		"Users":             users,
		"Groups":            grups,
		"Politiques":        politiques,
		"UserID":            userID,
		"GroupID":           groupID,
		"UserPols":          userPols,
		"GroupPols":         groupPols,
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
		"CanManageArxius":   true,
		"CanManagePolicies": true,
		"User":              user,
	})
}

func (a *App) AdminAssignarPoliticaUsuari(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permPolicies); !ok {
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
	_ = a.DB.AddUserPolitica(userID, polID)
	_ = a.DB.BumpUserPermissionsVersion(userID)
	http.Redirect(w, r, safeReturnTo(r.FormValue("return_to"), fmt.Sprintf("/admin/politiques/assignacions?user_id=%d", userID)), http.StatusSeeOther)
}

func (a *App) AdminTreurePoliticaUsuari(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permPolicies); !ok {
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
	_ = a.DB.RemoveUserPolitica(userID, polID)
	_ = a.DB.BumpUserPermissionsVersion(userID)
	http.Redirect(w, r, safeReturnTo(r.FormValue("return_to"), fmt.Sprintf("/admin/politiques/assignacions?user_id=%d", userID)), http.StatusSeeOther)
}

func (a *App) AdminAssignarPoliticaGrup(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permPolicies); !ok {
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
	_ = a.DB.AddGroupPolitica(groupID, polID)
	_ = a.DB.BumpGroupPermissionsVersion(groupID)
	http.Redirect(w, r, safeReturnTo(r.FormValue("return_to"), fmt.Sprintf("/admin/politiques/assignacions?group_id=%d", groupID)), http.StatusSeeOther)
}

func (a *App) AdminTreurePoliticaGrup(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permPolicies); !ok {
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
	_ = a.DB.RemoveGroupPolitica(groupID, polID)
	_ = a.DB.BumpGroupPermissionsVersion(groupID)
	http.Redirect(w, r, safeReturnTo(r.FormValue("return_to"), fmt.Sprintf("/admin/politiques/assignacions?group_id=%d", groupID)), http.StatusSeeOther)
}
