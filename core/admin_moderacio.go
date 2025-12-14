package core

import (
	"net/http"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

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
	RenderPrivateTemplate(w, r, "admin-moderacio-list.html", map[string]interface{}{
		"Persones":        pendents,
		"CanModerate":     true,
		"CanManageArxius": true,
	})
}

// Aprovar persona
func (a *App) AdminModeracioAprovar(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permModerate); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	id := extractID(r.URL.Path)
	_ = r.ParseForm()
	_ = a.DB.UpdatePersonaModeracio(id, "publicat", "")
	http.Redirect(w, r, "/admin/moderacio", http.StatusSeeOther)
}

// Rebutjar persona amb motiu
func (a *App) AdminModeracioRebutjar(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permModerate); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	id := extractID(r.URL.Path)
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/admin/moderacio", http.StatusSeeOther)
		return
	}
	motiu := r.FormValue("motiu")
	_ = a.DB.UpdatePersonaModeracio(id, "rebutjat", motiu)
	http.Redirect(w, r, "/admin/moderacio", http.StatusSeeOther)
}
