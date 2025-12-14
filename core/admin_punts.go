package core

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

// Llista de regles de punts
func (a *App) AdminListPuntsRegles(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permAdmin); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	regles, err := a.DB.ListPointsRules()
	if err != nil {
		http.Error(w, "Error llistant regles", http.StatusInternalServerError)
		return
	}
	msg := ""
	if r.URL.Query().Get("ok") != "" {
		msg = T(ResolveLang(r), "common.saved")
	} else if r.URL.Query().Get("recalc_ok") != "" {
		msg = T(ResolveLang(r), "points.rules.recalc.ok")
	} else if r.URL.Query().Get("err") != "" {
		msg = T(ResolveLang(r), "common.error")
	}
	RenderPrivateTemplate(w, r, "admin-punts-regles-list.html", map[string]interface{}{
		"Regles":            regles,
		"User":              user,
		"CanManageArxius":   true,
		"CanManagePolicies": true,
		"Msg":               msg,
	})
}

// Nova regla
func (a *App) AdminNewPuntsRegla(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permAdmin); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	token, _ := ensureCSRF(w, r)
	RenderPrivateTemplate(w, r, "admin-punts-regles-form.html", map[string]interface{}{
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
	if _, _, ok := a.requirePermission(w, r, permAdmin); !ok {
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
	RenderPrivateTemplate(w, r, "admin-punts-regles-form.html", map[string]interface{}{
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
	if _, _, ok := a.requirePermission(w, r, permAdmin); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Formulari invàlid", http.StatusBadRequest)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	id, _ := strconv.Atoi(r.FormValue("id"))
	code := strings.TrimSpace(r.FormValue("codi"))
	if id > 0 {
		// No permetre canviar el codi via form (immutable)
		if existing, err := a.DB.GetPointsRule(id); err == nil && existing != nil {
			code = existing.Code
		}
	}
	name := strings.TrimSpace(r.FormValue("nom"))
	desc := strings.TrimSpace(r.FormValue("descripcio"))
	points, _ := strconv.Atoi(r.FormValue("punts"))
	active := r.FormValue("actiu") == "1"
	if code == "" || name == "" {
		RenderPrivateTemplate(w, r, "admin-punts-regles-form.html", map[string]interface{}{
			"Regla": &db.PointsRule{ID: id, Code: code, Name: name, Description: desc, Points: points, Active: active},
			"IsNew": id == 0,
			"Error": "Codi i nom són obligatoris",
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
		RenderPrivateTemplate(w, r, "admin-punts-regles-form.html", map[string]interface{}{
			"Regla": regla,
			"IsNew": id == 0,
			"Error": "No s'ha pogut desar la regla",
		})
		return
	}
	http.Redirect(w, r, "/admin/punts/regles", http.StatusSeeOther)
}

// Recalcula usuaris_punts des de usuaris_activitat
func (a *App) AdminRecalcPunts(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permAdmin); !ok {
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
