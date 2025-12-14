package core

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type simpleUser struct {
	ID     int
	Usuari string
	Email  string
}

func (a *App) AdminAssignacionsPolitiques(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permPolicies); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	_ = r.ParseForm()
	userID, _ := strconv.Atoi(r.FormValue("user_id"))
	groupID, _ := strconv.Atoi(r.FormValue("group_id"))

	politiques, _ := a.DB.ListPolitiques()
	grups, _ := a.DB.ListGroups()
	users := a.listBasicUsers()

	var userPols []db.Politica
	if userID > 0 {
		userPols, _ = a.DB.ListUserPolitiques(userID)
	}
	var groupPols []db.Politica
	if groupID > 0 {
		groupPols, _ = a.DB.ListGroupPolitiques(groupID)
	}

	RenderPrivateTemplate(w, r, "admin-politiques-assignacions.html", map[string]interface{}{
		"Users":             users,
		"Groups":            grups,
		"Politiques":        politiques,
		"UserID":            userID,
		"GroupID":           groupID,
		"UserPols":          userPols,
		"GroupPols":         groupPols,
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
	http.Redirect(w, r, fmt.Sprintf("/admin/politiques/assignacions?user_id=%d", userID), http.StatusSeeOther)
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
	http.Redirect(w, r, fmt.Sprintf("/admin/politiques/assignacions?user_id=%d", userID), http.StatusSeeOther)
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
	http.Redirect(w, r, fmt.Sprintf("/admin/politiques/assignacions?group_id=%d", groupID), http.StatusSeeOther)
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
	http.Redirect(w, r, fmt.Sprintf("/admin/politiques/assignacions?group_id=%d", groupID), http.StatusSeeOther)
}

func (a *App) listBasicUsers() []simpleUser {
	rows, err := a.DB.Query(`SELECT id, usuari, correu FROM usuaris ORDER BY id LIMIT 200`)
	if err != nil {
		return nil
	}
	var res []simpleUser
	for _, r := range rows {
		idVal, _ := r["id"]
		usuariVal, _ := r["usuari"]
		correuVal, _ := r["correu"]
		id := toInt(idVal)
		res = append(res, simpleUser{
			ID:     id,
			Usuari: fmt.Sprintf("%v", usuariVal),
			Email:  fmt.Sprintf("%v", correuVal),
		})
	}
	return res
}

// converteix interface{} a int
func toInt(v interface{}) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case int32:
		return int(t)
	case float64:
		return int(t)
	default:
		return 0
	}
}
