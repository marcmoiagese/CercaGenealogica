package core

import (
	"database/sql"
	"fmt"
	"net/http"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type moderacioItem struct {
	ID      int
	Type    string
	Nom     string
	Context string
	Autor   string
	Created string
	Motiu   string
}

func (a *App) autorFromID(id sql.NullInt64) string {
	if id.Valid {
		if u, err := a.DB.GetUserByID(int(id.Int64)); err == nil && u != nil {
			if strings.TrimSpace(u.Name) != "" {
				return strings.TrimSpace(strings.TrimSpace(u.Name) + " " + strings.TrimSpace(u.Surname))
			}
			if strings.TrimSpace(u.Usuari) != "" {
				return u.Usuari
			}
		}
	}
	return "—"
}

func (a *App) buildModeracioItems() []moderacioItem {
	var items []moderacioItem

	if pendents, err := a.DB.ListPersones(db.PersonaFilter{Estat: "pendent", Limit: 200}); err == nil {
		for _, p := range pendents {
			created := ""
			if p.CreatedAt.Valid {
				created = p.CreatedAt.Time.Format("2006-01-02 15:04")
			}
			context := strings.TrimSpace(fmt.Sprintf("%s %s", p.Llibre, p.Pagina))
			if context == "" {
				context = p.Municipi
			}
			items = append(items, moderacioItem{
				ID:      p.ID,
				Type:    "persona",
				Nom:     strings.TrimSpace(strings.Join([]string{p.Nom, p.Cognom1, p.Cognom2}, " ")),
				Context: context,
				Autor:   a.autorFromID(p.CreatedBy),
				Created: created,
				Motiu:   p.ModeracioMotiu,
			})
		}
	}

	if arxius, err := a.DB.ListArxius(db.ArxiuFilter{Status: "pendent", Limit: 200}); err == nil {
		for _, arow := range arxius {
			items = append(items, moderacioItem{
				ID:      arow.ID,
				Type:    "arxiu",
				Nom:     arow.Nom,
				Context: arow.Tipus,
				Autor:   a.autorFromID(arow.CreatedBy),
				Motiu:   arow.ModeracioMotiu,
			})
		}
	}

	if llibres, err := a.DB.ListLlibres(db.LlibreFilter{Status: "pendent"}); err == nil {
		for _, l := range llibres {
			items = append(items, moderacioItem{
				ID:      l.ID,
				Type:    "llibre",
				Nom:     l.Titol,
				Context: l.NomEsglesia,
				Autor:   a.autorFromID(l.CreatedBy),
				Motiu:   l.ModeracioMotiu,
			})
		}
	}

	if nivells, err := a.DB.ListNivells(db.NivellAdminFilter{Status: "pendent"}); err == nil {
		for _, n := range nivells {
			items = append(items, moderacioItem{
				ID:      n.ID,
				Type:    "nivell",
				Nom:     n.NomNivell,
				Context: fmt.Sprintf("Nivell %d", n.Nivel),
				Autor:   a.autorFromID(n.CreatedBy),
				Motiu:   n.ModeracioMotiu,
			})
		}
	}

	if municipis, err := a.DB.ListMunicipis(db.MunicipiFilter{Status: "pendent"}); err == nil {
		for _, mrow := range municipis {
			m, _ := a.DB.GetMunicipi(mrow.ID)
			autor := "—"
			motiu := ""
			if m != nil {
				autor = a.autorFromID(m.CreatedBy)
				motiu = m.ModeracioMotiu
			}
			ctx := strings.TrimSpace(strings.Join([]string{mrow.PaisNom.String, mrow.ProvNom.String, mrow.Comarca.String}, " / "))
			items = append(items, moderacioItem{
				ID:      mrow.ID,
				Type:    "municipi",
				Nom:     mrow.Nom,
				Context: ctx,
				Autor:   autor,
				Motiu:   motiu,
			})
		}
	}

	if ents, err := a.DB.ListArquebisbats(db.ArquebisbatFilter{Status: "pendent"}); err == nil {
		for _, row := range ents {
			ent, _ := a.DB.GetArquebisbat(row.ID)
			autor := "—"
			motiu := ""
			if ent != nil {
				autor = a.autorFromID(ent.CreatedBy)
				motiu = ent.ModeracioMotiu
			}
			items = append(items, moderacioItem{
				ID:      row.ID,
				Type:    "eclesiastic",
				Nom:     row.Nom,
				Context: row.TipusEntitat,
				Autor:   autor,
				Motiu:   motiu,
			})
		}
	}

	return items
}

// Llista de persones pendents de moderació
func (a *App) AdminModeracioList(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permModerate); !ok {
		return
	}
	items := a.buildModeracioItems()
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
	objType := strings.TrimSpace(r.FormValue("object_type"))
	if objType == "" {
		objType = "persona"
	}
	_ = r.ParseForm()
	if err := a.updateModeracioObject(objType, id, "publicat", "", user.ID); err != nil {
		http.Redirect(w, r, "/admin/moderacio?err=1", http.StatusSeeOther)
		return
	}
	if acts, err := a.DB.ListActivityByObject(objType, id, "pendent"); err == nil {
		for _, act := range acts {
			_ = a.ValidateActivity(act.ID, user.ID)
		}
	}
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleModeracioApprove, "moderar_aprovar", objType, &id, "validat", nil, "")
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
	objType := strings.TrimSpace(r.FormValue("object_type"))
	if objType == "" {
		objType = "persona"
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/admin/moderacio?err=1", http.StatusSeeOther)
		return
	}
	motiu := r.FormValue("motiu")
	if err := a.updateModeracioObject(objType, id, "rebutjat", motiu, user.ID); err != nil {
		http.Redirect(w, r, "/admin/moderacio?err=1", http.StatusSeeOther)
		return
	}
	if acts, err := a.DB.ListActivityByObject(objType, id, "pendent"); err == nil {
		for _, act := range acts {
			_ = a.DB.UpdateUserActivityStatus(act.ID, "anulat", &user.ID)
		}
	}
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleModeracioReject, "moderar_rebutjar", objType, &id, "validat", nil, motiu)
	http.Redirect(w, r, "/admin/moderacio?ok=1", http.StatusSeeOther)
}

func (a *App) updateModeracioObject(objectType string, id int, estat, motiu string, moderatorID int) error {
	switch objectType {
	case "persona":
		return a.DB.UpdatePersonaModeracio(id, estat, motiu, moderatorID)
	case "arxiu":
		return a.DB.UpdateArxiuModeracio(id, estat, motiu, moderatorID)
	case "llibre":
		return a.DB.UpdateLlibreModeracio(id, estat, motiu, moderatorID)
	case "municipi":
		return a.DB.UpdateMunicipiModeracio(id, estat, motiu, moderatorID)
	case "nivell":
		return a.DB.UpdateNivellModeracio(id, estat, motiu, moderatorID)
	case "eclesiastic":
		return a.DB.UpdateArquebisbatModeracio(id, estat, motiu, moderatorID)
	default:
		return fmt.Errorf("tipus desconegut")
	}
}
