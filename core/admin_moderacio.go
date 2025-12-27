package core

import (
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type moderacioItem struct {
	ID        int
	Type      string
	Nom       string
	Context   string
	Autor     string
	AutorURL  string
	Created   string
	CreatedAt time.Time
	Motiu     string
	EditURL   string
}

func (a *App) buildModeracioItems(lang string, page, perPage int) ([]moderacioItem, int) {
	var items []moderacioItem
	userCache := map[int]*db.User{}
	autorFromID := func(id sql.NullInt64) (string, string) {
		if !id.Valid {
			return "—", ""
		}
		uid := int(id.Int64)
		if cached, ok := userCache[uid]; ok {
			username := strings.TrimSpace(cached.Usuari)
			if username == "" {
				full := strings.TrimSpace(strings.TrimSpace(cached.Name) + " " + strings.TrimSpace(cached.Surname))
				if full != "" {
					username = full
				}
			}
			if username == "" {
				username = "—"
			}
			return username, "/u/" + strconv.Itoa(cached.ID)
		}
		u, err := a.DB.GetUserByID(uid)
		if err != nil || u == nil {
			return "—", ""
		}
		userCache[uid] = u
		username := strings.TrimSpace(u.Usuari)
		if username == "" {
			full := strings.TrimSpace(strings.TrimSpace(u.Name) + " " + strings.TrimSpace(u.Surname))
			if full != "" {
				username = full
			}
		}
		if username == "" {
			username = "—"
		}
		return username, "/u/" + strconv.Itoa(u.ID)
	}

	persones := []db.Persona{}
	if pendents, err := a.DB.ListPersones(db.PersonaFilter{Estat: "pendent"}); err == nil {
		persones = pendents
	}
	arxius := []db.ArxiuWithCount{}
	if rows, err := a.DB.ListArxius(db.ArxiuFilter{Status: "pendent"}); err == nil {
		arxius = rows
	}
	llibres := []db.LlibreRow{}
	if rows, err := a.DB.ListLlibres(db.LlibreFilter{Status: "pendent"}); err == nil {
		llibres = rows
	}
	nivells := []db.NivellAdministratiu{}
	if rows, err := a.DB.ListNivells(db.NivellAdminFilter{Status: "pendent"}); err == nil {
		nivells = rows
	}
	municipis := []db.MunicipiRow{}
	if rows, err := a.DB.ListMunicipis(db.MunicipiFilter{Status: "pendent"}); err == nil {
		municipis = rows
	}
	ents := []db.ArquebisbatRow{}
	if rows, err := a.DB.ListArquebisbats(db.ArquebisbatFilter{Status: "pendent"}); err == nil {
		ents = rows
	}
	variants := []db.CognomVariant{}
	if rows, err := a.DB.ListCognomVariants(db.CognomVariantFilter{Status: "pendent"}); err == nil {
		variants = rows
	}

	totalNonReg := len(persones) + len(arxius) + len(llibres) + len(nivells) + len(municipis) + len(ents) + len(variants)
	regTotal := 0
	if total, err := a.DB.CountTranscripcionsRawGlobal(db.TranscripcioFilter{Status: "pendent"}); err == nil {
		regTotal = total
	}
	total := totalNonReg + regTotal
	start := (page - 1) * perPage
	if start < 0 {
		start = 0
	}
	end := start + perPage
	if end > total {
		end = total
	}
	index := 0
	appendIfVisible := func(item moderacioItem) {
		if index >= start && index < end {
			items = append(items, item)
		}
		index++
	}

	for _, p := range persones {
		created := ""
		var createdAt time.Time
		if p.CreatedAt.Valid {
			created = p.CreatedAt.Time.Format("2006-01-02 15:04")
			createdAt = p.CreatedAt.Time
		}
		context := strings.TrimSpace(fmt.Sprintf("%s %s", p.Llibre, p.Pagina))
		if context == "" {
			context = p.Municipi
		}
		autorNom, autorURL := autorFromID(p.CreatedBy)
		appendIfVisible(moderacioItem{
			ID:        p.ID,
			Type:      "persona",
			Nom:       strings.TrimSpace(strings.Join([]string{p.Nom, p.Cognom1, p.Cognom2}, " ")),
			Context:   context,
			Autor:     autorNom,
			AutorURL:  autorURL,
			Created:   created,
			CreatedAt: createdAt,
			Motiu:     p.ModeracioMotiu,
			EditURL:   fmt.Sprintf("/persones/%d?return_to=/moderacio", p.ID),
		})
	}

	for _, arow := range arxius {
		autorNom, autorURL := autorFromID(arow.CreatedBy)
		appendIfVisible(moderacioItem{
			ID:        arow.ID,
			Type:      "arxiu",
			Nom:       arow.Nom,
			Context:   arow.Tipus,
			Autor:     autorNom,
			AutorURL:  autorURL,
			Created:   "",
			CreatedAt: time.Time{},
			Motiu:     arow.ModeracioMotiu,
			EditURL:   fmt.Sprintf("/documentals/arxius/%d/edit?return_to=/moderacio", arow.ID),
		})
	}

	for _, l := range llibres {
		autorNom, autorURL := autorFromID(l.CreatedBy)
		appendIfVisible(moderacioItem{
			ID:        l.ID,
			Type:      "llibre",
			Nom:       l.Titol,
			Context:   l.NomEsglesia,
			Autor:     autorNom,
			AutorURL:  autorURL,
			Created:   "",
			CreatedAt: time.Time{},
			Motiu:     l.ModeracioMotiu,
			EditURL:   fmt.Sprintf("/documentals/llibres/%d/edit?return_to=/moderacio", l.ID),
		})
	}

	for _, n := range nivells {
		autorNom, autorURL := autorFromID(n.CreatedBy)
		appendIfVisible(moderacioItem{
			ID:        n.ID,
			Type:      "nivell",
			Nom:       n.NomNivell,
			Context:   fmt.Sprintf("Nivell %d", n.Nivel),
			Autor:     autorNom,
			AutorURL:  autorURL,
			Created:   "",
			CreatedAt: time.Time{},
			Motiu:     n.ModeracioMotiu,
			EditURL:   fmt.Sprintf("/territori/nivells/%d/edit?return_to=/moderacio", n.ID),
		})
	}

	for _, mrow := range municipis {
		autorNom := "—"
		autorURL := ""
		motiu := ""
		ctx := strings.TrimSpace(strings.Join([]string{mrow.PaisNom.String, mrow.ProvNom.String, mrow.Comarca.String}, " / "))
		appendIfVisible(moderacioItem{
			ID:        mrow.ID,
			Type:      "municipi",
			Nom:       mrow.Nom,
			Context:   ctx,
			Autor:     autorNom,
			AutorURL:  autorURL,
			Created:   "",
			CreatedAt: time.Time{},
			Motiu:     motiu,
			EditURL:   fmt.Sprintf("/territori/municipis/%d/edit?return_to=/moderacio", mrow.ID),
		})
	}

	for _, row := range ents {
		autorNom := "—"
		autorURL := ""
		motiu := ""
		appendIfVisible(moderacioItem{
			ID:        row.ID,
			Type:      "eclesiastic",
			Nom:       row.Nom,
			Context:   row.TipusEntitat,
			Autor:     autorNom,
			AutorURL:  autorURL,
			Created:   "",
			CreatedAt: time.Time{},
			Motiu:     motiu,
			EditURL:   fmt.Sprintf("/territori/eclesiastic/%d/edit?return_to=/moderacio", row.ID),
		})
	}

	cognomCache := map[int]string{}
	for _, v := range variants {
		created := ""
		var createdAt time.Time
		if v.CreatedAt.Valid {
			created = v.CreatedAt.Time.Format("2006-01-02 15:04")
			createdAt = v.CreatedAt.Time
		}
		autorNom, autorURL := autorFromID(v.CreatedBy)
		forma := cognomCache[v.CognomID]
		if forma == "" {
			if c, err := a.DB.GetCognom(v.CognomID); err == nil && c != nil {
				forma = c.Forma
				cognomCache[v.CognomID] = forma
			}
		}
		context := strings.TrimSpace(fmt.Sprintf("%s → %s", forma, v.Variant))
		if context == "" {
			context = v.Variant
		}
		appendIfVisible(moderacioItem{
			ID:        v.ID,
			Type:      "cognom_variant",
			Nom:       v.Variant,
			Context:   context,
			Autor:     autorNom,
			AutorURL:  autorURL,
			Created:   created,
			CreatedAt: createdAt,
			Motiu:     v.ModeracioMotiu,
			EditURL:   fmt.Sprintf("/cognoms/%d", v.CognomID),
		})
	}

	if regTotal > 0 && index < end {
		regOffset := 0
		if start > index {
			regOffset = start - index
		}
		regLimit := end - maxInt(index, start)
		if regLimit < 0 {
			regLimit = 0
		}
		registres, _ := a.DB.ListTranscripcionsRawGlobal(db.TranscripcioFilter{
			Status: "pendent",
			Limit:  regLimit,
			Offset: regOffset,
		})
		if start > index {
			index = start
		}
		for _, reg := range registres {
			autorNom, autorURL := autorFromID(reg.CreatedBy)
			created := ""
			var createdAt time.Time
			if !reg.CreatedAt.IsZero() {
				created = reg.CreatedAt.Format("2006-01-02 15:04")
				createdAt = reg.CreatedAt
			}
			contextParts := []string{}
			if reg.TipusActe != "" {
				contextParts = append(contextParts, reg.TipusActe)
			}
			if reg.DataActeText != "" {
				contextParts = append(contextParts, reg.DataActeText)
			} else if reg.AnyDoc.Valid {
				contextParts = append(contextParts, fmt.Sprintf("%d", reg.AnyDoc.Int64))
			}
			if reg.NumPaginaText != "" {
				contextParts = append(contextParts, reg.NumPaginaText)
			}
			appendIfVisible(moderacioItem{
				ID:        reg.ID,
				Type:      "registre",
				Nom:       fmt.Sprintf("Registre %d", reg.ID),
				Context:   strings.Join(contextParts, " · "),
				Autor:     autorNom,
				AutorURL:  autorURL,
				Created:   created,
				CreatedAt: createdAt,
				Motiu:     reg.ModeracioMotiu,
				EditURL:   fmt.Sprintf("/documentals/registres/%d/editar?return_to=/moderacio", reg.ID),
			})
		}
	}

	return items, total
}

func (a *App) firstPendingActivityTime(objectType string, objectID int) string {
	if acts, err := a.DB.ListActivityByObject(objectType, objectID, "pendent"); err == nil {
		for _, act := range acts {
			return act.CreatedAt.Format("2006-01-02 15:04")
		}
	}
	return "—"
}

func parseModeracioTime(val string) time.Time {
	if val == "" || val == "—" {
		return time.Time{}
	}
	t, err := time.Parse("2006-01-02 15:04", val)
	if err != nil {
		return time.Time{}
	}
	return t
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Llista de persones pendents de moderació
func (a *App) AdminModeracioList(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permModerate); !ok {
		return
	}
	page := 1
	perPage := 25
	if val := strings.TrimSpace(r.URL.Query().Get("page")); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			page = n
		}
	}
	if val := strings.TrimSpace(r.URL.Query().Get("per_page")); val != "" {
		if n, err := strconv.Atoi(val); err == nil {
			switch n {
			case 10, 25, 50, 100:
				perPage = n
			}
		}
	}
	pageItems, total := a.buildModeracioItems(ResolveLang(r), page, perPage)
	totalPages := 1
	if total > 0 {
		totalPages = (total + perPage - 1) / perPage
	}
	if page > totalPages {
		page = totalPages
	}
	start := (page - 1) * perPage
	if start < 0 {
		start = 0
	}
	end := start + perPage
	if end > total {
		end = total
	}
	pageStart := 0
	pageEnd := 0
	if total > 0 {
		pageStart = start + 1
		pageEnd = end
	}
	user, _ := a.VerificarSessio(r)
	perms := a.getPermissionsForUser(user.ID)
	canManageArxius := a.hasPerm(perms, permArxius)
	isAdmin := a.hasPerm(perms, permAdmin)
	msg := ""
	okFlag := false
	if r.URL.Query().Get("ok") != "" {
		okFlag = true
		msg = T(ResolveLang(r), "moderation.success")
	} else if r.URL.Query().Get("err") != "" {
		msg = T(ResolveLang(r), "moderation.error")
	}
	RenderPrivateTemplate(w, r, "admin-moderacio-list.html", map[string]interface{}{
		"Persones":        pageItems,
		"CanModerate":     true,
		"CanManageArxius": canManageArxius,
		"IsAdmin":         isAdmin,
		"Msg":             msg,
		"Ok":              okFlag,
		"User":            user,
		"Total":           total,
		"Page":            page,
		"PerPage":         perPage,
		"TotalPages":      totalPages,
		"HasPrev":         page > 1,
		"HasNext":         page < totalPages,
		"PrevPage":        page - 1,
		"NextPage":        page + 1,
		"PageStart":       pageStart,
		"PageEnd":         pageEnd,
		"PageBase":        "/moderacio?per_page=" + strconv.Itoa(perPage),
	})
}

// Accions massives de moderació
func (a *App) AdminModeracioBulk(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permModerate); !ok {
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
	user, _ := a.VerificarSessio(r)
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/moderacio?err=1", http.StatusSeeOther)
		return
	}
	action := strings.TrimSpace(r.FormValue("bulk_action"))
	if action == "" {
		action = strings.TrimSpace(r.FormValue("action"))
	}
	scope := strings.TrimSpace(r.FormValue("bulk_scope"))
	if scope == "" {
		scope = "page"
	}
	bulkType := strings.TrimSpace(r.FormValue("bulk_type"))
	if bulkType == "" {
		bulkType = "all"
	}
	selected := r.Form["selected"]
	motiu := strings.TrimSpace(r.FormValue("bulk_reason"))
	perms := a.getPermissionsForUser(user.ID)
	isAdmin := a.hasPerm(perms, permAdmin)
	if scope == "all" && !isAdmin {
		http.Redirect(w, r, "/moderacio?err=1", http.StatusSeeOther)
		return
	}
	errCount := 0
	applyAction := func(objType string, id int) {
		switch action {
		case "approve":
			if err := a.updateModeracioObject(objType, id, "publicat", "", user.ID); err != nil {
				Errorf("Moderacio massiva aprovar %s:%d ha fallat: %v", objType, id, err)
				errCount++
				return
			}
			if acts, err := a.DB.ListActivityByObject(objType, id, "pendent"); err == nil {
				for _, act := range acts {
					_ = a.ValidateActivity(act.ID, user.ID)
				}
			}
			_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleModeracioApprove, "moderar_aprovar", objType, &id, "validat", nil, "")
		case "reject":
			if err := a.updateModeracioObject(objType, id, "rebutjat", motiu, user.ID); err != nil {
				Errorf("Moderacio massiva rebutjar %s:%d ha fallat: %v", objType, id, err)
				errCount++
				return
			}
			if acts, err := a.DB.ListActivityByObject(objType, id, "pendent"); err == nil {
				for _, act := range acts {
					_ = a.DB.UpdateUserActivityStatus(act.ID, "anulat", &user.ID)
				}
			}
			_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleModeracioReject, "moderar_rebutjar", objType, &id, "validat", nil, motiu)
		default:
			errCount++
		}
	}
	if scope == "all" {
		types := []string{"persona", "arxiu", "llibre", "nivell", "municipi", "eclesiastic", "registre", "cognom_variant"}
		if bulkType != "" && bulkType != "all" {
			types = []string{bulkType}
		}
		for _, objType := range types {
			switch objType {
			case "persona":
				if rows, err := a.DB.ListPersones(db.PersonaFilter{Estat: "pendent"}); err == nil {
					for _, row := range rows {
						applyAction(objType, row.ID)
					}
				} else {
					errCount++
				}
			case "arxiu":
				if rows, err := a.DB.ListArxius(db.ArxiuFilter{Status: "pendent"}); err == nil {
					for _, row := range rows {
						applyAction(objType, row.ID)
					}
				} else {
					errCount++
				}
			case "llibre":
				if rows, err := a.DB.ListLlibres(db.LlibreFilter{Status: "pendent"}); err == nil {
					for _, row := range rows {
						applyAction(objType, row.ID)
					}
				} else {
					errCount++
				}
			case "nivell":
				if rows, err := a.DB.ListNivells(db.NivellAdminFilter{Status: "pendent"}); err == nil {
					for _, row := range rows {
						applyAction(objType, row.ID)
					}
				} else {
					errCount++
				}
			case "municipi":
				if rows, err := a.DB.ListMunicipis(db.MunicipiFilter{Status: "pendent"}); err == nil {
					for _, row := range rows {
						applyAction(objType, row.ID)
					}
				} else {
					errCount++
				}
			case "eclesiastic":
				if rows, err := a.DB.ListArquebisbats(db.ArquebisbatFilter{Status: "pendent"}); err == nil {
					for _, row := range rows {
						applyAction(objType, row.ID)
					}
				} else {
					errCount++
				}
			case "cognom_variant":
				if rows, err := a.DB.ListCognomVariants(db.CognomVariantFilter{Status: "pendent"}); err == nil {
					for _, row := range rows {
						applyAction(objType, row.ID)
					}
				} else {
					errCount++
				}
			case "registre":
				const chunk = 200
				for {
					rows, err := a.DB.ListTranscripcionsRawGlobal(db.TranscripcioFilter{
						Status: "pendent",
						Limit:  chunk,
					})
					if err != nil {
						errCount++
						break
					}
					if len(rows) == 0 {
						break
					}
					for _, row := range rows {
						applyAction(objType, row.ID)
					}
				}
			}
		}
	} else {
		if len(selected) == 0 {
			http.Redirect(w, r, "/moderacio?err=1", http.StatusSeeOther)
			return
		}
		for _, entry := range selected {
			parts := strings.SplitN(entry, ":", 2)
			if len(parts) != 2 {
				errCount++
				continue
			}
			objType := strings.TrimSpace(parts[0])
			id, err := strconv.Atoi(parts[1])
			if err != nil {
				errCount++
				continue
			}
			applyAction(objType, id)
		}
	}
	if errCount > 0 {
		http.Redirect(w, r, "/moderacio?err=1", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/moderacio?ok=1", http.StatusSeeOther)
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
		Errorf("Moderacio aprovar %s:%d ha fallat: %v", objType, id, err)
		http.Redirect(w, r, "/moderacio?err=1", http.StatusSeeOther)
		return
	}
	if acts, err := a.DB.ListActivityByObject(objType, id, "pendent"); err == nil {
		for _, act := range acts {
			_ = a.ValidateActivity(act.ID, user.ID)
		}
	}
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleModeracioApprove, "moderar_aprovar", objType, &id, "validat", nil, "")
	http.Redirect(w, r, "/moderacio?ok=1", http.StatusSeeOther)
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
		http.Redirect(w, r, "/moderacio?err=1", http.StatusSeeOther)
		return
	}
	motiu := r.FormValue("motiu")
	if err := a.updateModeracioObject(objType, id, "rebutjat", motiu, user.ID); err != nil {
		Errorf("Moderacio rebutjar %s:%d ha fallat: %v", objType, id, err)
		http.Redirect(w, r, "/moderacio?err=1", http.StatusSeeOther)
		return
	}
	if acts, err := a.DB.ListActivityByObject(objType, id, "pendent"); err == nil {
		for _, act := range acts {
			_ = a.DB.UpdateUserActivityStatus(act.ID, "anulat", &user.ID)
		}
	}
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleModeracioReject, "moderar_rebutjar", objType, &id, "validat", nil, motiu)
	http.Redirect(w, r, "/moderacio?ok=1", http.StatusSeeOther)
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
	case "registre":
		return a.DB.UpdateTranscripcioModeracio(id, estat, motiu, moderatorID)
	case "cognom_variant":
		return a.DB.UpdateCognomVariantModeracio(id, estat, motiu, moderatorID)
	default:
		return fmt.Errorf("tipus desconegut")
	}
}
