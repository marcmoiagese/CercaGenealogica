package core

import (
	"net/http"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type moderacioItem struct {
	Persona db.Persona
	Autor   string
	Created string
}

// Llista de persones pendents de moderació
func (a *App) AdminModeracioList(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permModerate); !ok {
		return
	}
	pendents, err := a.DB.ListPersones(db.PersonaFilter{Estat: "pendent", Limit: 200})
	if err != nil {
		http.Error(w, "Error carregant pendents", http.StatusInternalServerError)
		return
	}
	items := make([]moderacioItem, 0, len(pendents))
	for _, p := range pendents {
		autor := "—"
		if p.CreatedBy.Valid {
			if u, err := a.DB.GetUserByID(int(p.CreatedBy.Int64)); err == nil && u != nil {
				if strings.TrimSpace(u.Name) != "" {
					autor = strings.TrimSpace(u.Name + " " + u.Surname)
				} else if strings.TrimSpace(u.Usuari) != "" {
					autor = u.Usuari
				}
			}
		}
		created := ""
		if p.CreatedAt.Valid {
			created = p.CreatedAt.Time.Format("2006-01-02 15:04")
		}
		items = append(items, moderacioItem{Persona: p, Autor: autor, Created: created})
	}
	user, _ := a.VerificarSessio(r)
	perms := a.getPermissionsForUser(user.ID)
	canManageArxius := a.hasPerm(perms, permArxius)
	msg := ""
	okFlag := false
	if r.URL.Query().Get("ok") != "" {
		okFlag = true
		msg = T(ResolveLang(r), "moderation.success")
	} else if r.URL.Query().Get("err") != "" {
		msg = T(ResolveLang(r), "moderation.error")
	}
	RenderPrivateTemplate(w, r, "admin-moderacio-list.html", map[string]interface{}{
		"Persones":        items,
		"CanModerate":     true,
		"CanManageArxius": canManageArxius,
		"Msg":             msg,
		"Ok":              okFlag,
		"User":            user,
	})
}

// Aprovar persona
func (a *App) AdminModeracioAprovar(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permModerate); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	id := extractID(r.URL.Path)
	_ = r.ParseForm()
	_ = a.DB.UpdatePersonaModeracio(id, "publicat", "", user.ID)
	http.Redirect(w, r, "/admin/moderacio?ok=1", http.StatusSeeOther)
}

// Rebutjar persona amb motiu
func (a *App) AdminModeracioRebutjar(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permModerate); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	id := extractID(r.URL.Path)
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/admin/moderacio?err=1", http.StatusSeeOther)
		return
	}
	motiu := r.FormValue("motiu")
	_ = a.DB.UpdatePersonaModeracio(id, "rebutjat", motiu, user.ID)
	http.Redirect(w, r, "/admin/moderacio?ok=1", http.StatusSeeOther)
}
