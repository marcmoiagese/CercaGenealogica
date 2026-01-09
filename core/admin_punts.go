package core

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

// Llista de regles de punts
func (a *App) AdminListPuntsRegles(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requireAnyPermissionKey(w, r, []string{permKeyAdminPuntsAdd, permKeyAdminPuntsEdit}, PermissionTarget{})
	if !ok {
		return
	}
	regles, err := a.DB.ListPointsRules()
	if err != nil {
		http.Error(w, "Error llistant regles", http.StatusInternalServerError)
		return
	}
	token, _ := ensureCSRF(w, r)
	msg := ""
	if r.URL.Query().Get("ok") != "" {
		msg = T(ResolveLang(r), "common.saved")
	} else if r.URL.Query().Get("recalc_ok") != "" {
		msg = T(ResolveLang(r), "points.rules.recalc.ok")
	} else if r.URL.Query().Get("err") != "" {
		msg = T(ResolveLang(r), "common.error")
	}
	RenderPrivateTemplate(w, r, "admin-punts-list.html", map[string]interface{}{
		"Regles":            regles,
		"User":              user,
		"CanManageArxius":   true,
		"CanManagePolicies": true,
		"Msg":               msg,
		"CSRFToken":         token,
	})
}

// Nova regla
func (a *App) AdminNewPuntsRegla(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminPuntsAdd, PermissionTarget{}); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	token, _ := ensureCSRF(w, r)
	RenderPrivateTemplate(w, r, "admin-punts-form.html", map[string]interface{}{
		"Regla":             &db.PointsRule{},
		"IsNew":             true,
		"CSRFToken":         token,
		"User":              user,
		"CanManageArxius":   true,
		"CanManagePolicies": true,
	})
}

// Edita regla
func (a *App) AdminEditPuntsRegla(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminPuntsEdit, PermissionTarget{}); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	id := extractID(r.URL.Path)
	regla, err := a.DB.GetPointsRule(id)
	if err != nil || regla == nil {
		http.NotFound(w, r)
		return
	}
	token, _ := ensureCSRF(w, r)
	RenderPrivateTemplate(w, r, "admin-punts-form.html", map[string]interface{}{
		"Regla":             regla,
		"IsNew":             false,
		"CSRFToken":         token,
		"User":              user,
		"CanManageArxius":   true,
		"CanManagePolicies": true,
	})
}

// Desa regla
func (a *App) AdminSavePuntsRegla(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Formulari invàlid", http.StatusBadRequest)
		return
	}
	id, _ := strconv.Atoi(r.FormValue("id"))
	permKey := permKeyAdminPuntsAdd
	if id > 0 {
		permKey = permKeyAdminPuntsEdit
	}
	user, ok := a.requirePermissionKey(w, r, permKey, PermissionTarget{})
	if !ok {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	code := strings.TrimSpace(r.FormValue("code"))
	if id > 0 {
		// No permetre canviar el codi via form (immutable)
		if existing, err := a.DB.GetPointsRule(id); err == nil && existing != nil {
			code = existing.Code
		}
	}
	name := strings.TrimSpace(r.FormValue("name"))
	desc := strings.TrimSpace(r.FormValue("description"))
	points, _ := strconv.Atoi(strings.TrimSpace(r.FormValue("points")))
	active := r.FormValue("active") == "1"
	if code == "" || name == "" {
		token, _ := ensureCSRF(w, r)
		RenderPrivateTemplate(w, r, "admin-punts-form.html", map[string]interface{}{
			"Regla":     &db.PointsRule{ID: id, Code: code, Name: name, Description: desc, Points: points, Active: active},
			"IsNew":     id == 0,
			"Error":     "Codi i nom són obligatoris",
			"CSRFToken": token,
			"User":      user,
		})
		return
	}
	regla := &db.PointsRule{
		ID:          id,
		Code:        code,
		Name:        name,
		Description: desc,
		Points:      points,
		Active:      active,
	}
	if _, err := a.DB.SavePointsRule(regla); err != nil {
		token, _ := ensureCSRF(w, r)
		RenderPrivateTemplate(w, r, "admin-punts-form.html", map[string]interface{}{
			"Regla":     regla,
			"IsNew":     id == 0,
			"Error":     "No s'ha pogut desar la regla",
			"CSRFToken": token,
			"User":      user,
		})
		return
	}
	http.Redirect(w, r, "/admin/punts/regles", http.StatusSeeOther)
}

// Recalcula usuaris_punts des de usuaris_activitat
func (a *App) AdminRecalcPunts(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminPuntsEdit, PermissionTarget{}); !ok {
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
	if err := a.DB.RecalcUserPoints(); err != nil {
		http.Redirect(w, r, "/admin/punts/regles?err=1", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/admin/punts/regles?recalc_ok=1", http.StatusSeeOther)
}
