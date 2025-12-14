package core

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func (a *App) AdminListEclesiastic(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permEclesia); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	filter := db.ArquebisbatFilter{
		Text: strings.TrimSpace(r.URL.Query().Get("q")),
	}
	if pid := strings.TrimSpace(r.URL.Query().Get("pais_id")); pid != "" {
		if v, err := strconv.Atoi(pid); err == nil {
			filter.PaisID = v
		}
	}
	entitats, _ := a.DB.ListArquebisbats(filter)
	paisos, _ := a.DB.ListPaisos()
	RenderPrivateTemplate(w, r, "admin-eclesiastic-list.html", map[string]interface{}{
		"Entitats":        entitats,
		"Filter":          filter,
		"Paisos":          paisos,
		"CanManageArxius": true,
		"User":            user,
	})
}

func (a *App) AdminNewEclesiastic(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permEclesia); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	paisos, _ := a.DB.ListPaisos()
	RenderPrivateTemplate(w, r, "admin-eclesiastic-form.html", map[string]interface{}{
		"Entitat":         &db.Arquebisbat{TipusEntitat: "bisbat"},
		"Paisos":          paisos,
		"Parents":         nil,
		"IsNew":           true,
		"CanManageArxius": true,
		"User":            user,
	})
}

func (a *App) AdminEditEclesiastic(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permEclesia); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	id := extractID(r.URL.Path)
	ent, err := a.DB.GetArquebisbat(id)
	if err != nil || ent == nil {
		http.NotFound(w, r)
		return
	}
	paisos, _ := a.DB.ListPaisos()
	parents, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	RenderPrivateTemplate(w, r, "admin-eclesiastic-form.html", map[string]interface{}{
		"Entitat":         ent,
		"Paisos":          paisos,
		"Parents":         parents,
		"IsNew":           false,
		"CanManageArxius": true,
		"User":            user,
	})
}

func (a *App) AdminSaveEclesiastic(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permEclesia); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin/eclesiastic", http.StatusSeeOther)
		return
	}
	id, _ := strconv.Atoi(r.FormValue("id"))
	paisID := sqlNullInt(r.FormValue("pais_id"))
	parentID := sqlNullInt(r.FormValue("parent_id"))
	nivell := sqlNullInt(r.FormValue("nivell"))
	anyInici := sqlNullInt(r.FormValue("any_inici"))
	anyFi := sqlNullInt(r.FormValue("any_fi"))
	ent := &db.Arquebisbat{
		ID:           id,
		Nom:          strings.TrimSpace(r.FormValue("nom")),
		TipusEntitat: strings.TrimSpace(r.FormValue("tipus_entitat")),
		PaisID:       paisID,
		Nivell:       nivell,
		ParentID:     parentID,
		AnyInici:     anyInici,
		AnyFi:        anyFi,
		Web:          strings.TrimSpace(r.FormValue("web")),
		WebArxiu:     strings.TrimSpace(r.FormValue("web_arxiu")),
		WebWikipedia: strings.TrimSpace(r.FormValue("web_wikipedia")),
		Territori:    strings.TrimSpace(r.FormValue("territori")),
		Observacions: strings.TrimSpace(r.FormValue("observacions")),
	}
	if errMsg := validateEclesiastic(ent); errMsg != "" {
		a.renderEclesiasticError(w, r, ent, errMsg, id == 0)
		return
	}
	var saveErr error
	if ent.ID == 0 {
		_, saveErr = a.DB.CreateArquebisbat(ent)
	} else {
		saveErr = a.DB.UpdateArquebisbat(ent)
	}
	if saveErr != nil {
		a.renderEclesiasticError(w, r, ent, "No s'ha pogut desar l'entitat.", id == 0)
		return
	}
	http.Redirect(w, r, "/admin/eclesiastic", http.StatusSeeOther)
}

func validateEclesiastic(e *db.Arquebisbat) string {
	if strings.TrimSpace(e.Nom) == "" {
		return "El nom és obligatori."
	}
	if e.TipusEntitat == "" {
		return "El tipus d'entitat és obligatori."
	}
	if e.ParentID.Valid && e.ID != 0 && e.ParentID.Int64 == int64(e.ID) {
		return "L'entitat no pot ser pare de si mateixa."
	}
	return ""
}

func (a *App) renderEclesiasticError(w http.ResponseWriter, r *http.Request, e *db.Arquebisbat, msg string, isNew bool) {
	paisos, _ := a.DB.ListPaisos()
	parents, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	RenderPrivateTemplate(w, r, "admin-eclesiastic-form.html", map[string]interface{}{
		"Entitat":         e,
		"Paisos":          paisos,
		"Parents":         parents,
		"Error":           msg,
		"IsNew":           isNew,
		"CanManageArxius": true,
	})
}
