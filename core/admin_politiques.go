package core

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func (a *App) AdminListPolitiques(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permPolicies); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	pols, err := a.DB.ListPolitiques()
	if err != nil {
		http.Error(w, "Error obtenint polítiques", http.StatusInternalServerError)
		return
	}
	RenderPrivateTemplate(w, r, "admin-politiques-list.html", map[string]interface{}{
		"Politiques":        pols,
		"CanManageArxius":   true,
		"CanManagePolicies": true,
		"User":              user,
	})
}

func (a *App) AdminNewPolitica(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permPolicies); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	RenderPrivateTemplate(w, r, "admin-politiques-form.html", map[string]interface{}{
		"Politica":          &db.Politica{},
		"IsNew":             true,
		"CanManageArxius":   true,
		"CanManagePolicies": true,
		"User":              user,
	})
}

func (a *App) AdminEditPolitica(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permPolicies); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	id := extractID(r.URL.Path)
	pol, err := a.DB.GetPolitica(id)
	if err != nil || pol == nil {
		http.NotFound(w, r)
		return
	}
	RenderPrivateTemplate(w, r, "admin-politiques-form.html", map[string]interface{}{
		"Politica":          pol,
		"IsNew":             false,
		"CanManageArxius":   true,
		"CanManagePolicies": true,
		"User":              user,
	})
}

func (a *App) AdminSavePolitica(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permPolicies); !ok {
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
	id, _ := strconv.Atoi(r.FormValue("id"))
	name := strings.TrimSpace(r.FormValue("nom"))
	desc := strings.TrimSpace(r.FormValue("descripcio"))
	permsRaw := strings.TrimSpace(r.FormValue("permisos"))

	if name == "" {
		RenderPrivateTemplate(w, r, "admin-politiques-form.html", map[string]interface{}{
			"Politica": &db.Politica{ID: id, Nom: name, Descripcio: desc, Permisos: permsRaw},
			"IsNew":    id == 0,
			"Error":    "El nom és obligatori",
		})
		return
	}

	// Validar JSON de permisos
	var parsed db.PolicyPermissions
	if permsRaw == "" {
		permsRaw = "{}"
	}
	if err := json.Unmarshal([]byte(permsRaw), &parsed); err != nil {
		RenderPrivateTemplate(w, r, "admin-politiques-form.html", map[string]interface{}{
			"Politica": &db.Politica{ID: id, Nom: name, Descripcio: desc, Permisos: permsRaw},
			"IsNew":    id == 0,
			"Error":    "JSON de permisos invàlid",
		})
		return
	}
	// Re-marshal per guardar net
	permsClean, _ := json.Marshal(parsed)
	p := &db.Politica{
		ID:         id,
		Nom:        name,
		Descripcio: desc,
		Permisos:   string(permsClean),
	}
	if _, err := a.DB.SavePolitica(p); err != nil {
		RenderPrivateTemplate(w, r, "admin-politiques-form.html", map[string]interface{}{
			"Politica": p,
			"IsNew":    id == 0,
			"Error":    "No s'ha pogut desar la política",
		})
		return
	}
	http.Redirect(w, r, "/admin/politiques", http.StatusSeeOther)
}
