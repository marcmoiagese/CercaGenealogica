package core

import (
	"database/sql"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func (a *App) AdminListLlibres(w http.ResponseWriter, r *http.Request) {
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	perms := a.getPermissionsForUser(user.ID)
	canManage := a.hasPerm(perms, permArxius)
	filter := db.LlibreFilter{
		Text: strings.TrimSpace(r.URL.Query().Get("q")),
	}
	if v := strings.TrimSpace(r.URL.Query().Get("arquevisbat_id")); v != "" {
		if id, err := strconv.Atoi(v); err == nil {
			filter.ArquebisbatID = id
		}
	}
	if v := strings.TrimSpace(r.URL.Query().Get("municipi_id")); v != "" {
		if id, err := strconv.Atoi(v); err == nil {
			filter.MunicipiID = id
		}
	}
	if v := strings.TrimSpace(r.URL.Query().Get("arxiu_id")); v != "" {
		if id, err := strconv.Atoi(v); err == nil {
			filter.ArxiuID = id
		}
	}
	filter.ArxiuTipus = strings.TrimSpace(r.URL.Query().Get("arxiu_tipus"))
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	if status == "" {
		status = "publicat"
	}
	filter.Status = status
	llibres, _ := a.DB.ListLlibres(filter)
	arquebisbats, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	municipis, _ := a.DB.ListMunicipis(db.MunicipiFilter{})
	arxius, _ := a.DB.ListArxius(db.ArxiuFilter{Limit: 200})
	RenderPrivateTemplate(w, r, "admin-llibres-list.html", map[string]interface{}{
		"Llibres":         llibres,
		"Filter":          filter,
		"Arquebisbats":    arquebisbats,
		"Municipis":       municipis,
		"Arxius":          arxius,
		"CanManageArxius": canManage,
		"User":            user,
	})
}

func (a *App) AdminNewLlibre(w http.ResponseWriter, r *http.Request) {
	_, _, ok := a.requirePermission(w, r, permArxius)
	if !ok {
		return
	}
	a.renderLlibreForm(w, r, &db.Llibre{ModeracioEstat: "pendent"}, true, "")
}

func (a *App) AdminEditLlibre(w http.ResponseWriter, r *http.Request) {
	_, _, ok := a.requirePermission(w, r, permArxius)
	if !ok {
		return
	}
	id := extractID(r.URL.Path)
	llibre, err := a.DB.GetLlibre(id)
	if err != nil || llibre == nil {
		http.NotFound(w, r)
		return
	}
	a.renderLlibreForm(w, r, llibre, false, "")
}

func parseNullInt64(val string) sql.NullInt64 {
	if strings.TrimSpace(val) == "" {
		return sql.NullInt64{}
	}
	n, err := strconv.Atoi(strings.TrimSpace(val))
	if err != nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(n), Valid: true}
}

func parseLlibreForm(r *http.Request) *db.Llibre {
	_ = r.ParseForm()
	arquevisbatID, _ := strconv.Atoi(r.FormValue("arquevisbat_id"))
	municipiID, _ := strconv.Atoi(r.FormValue("municipi_id"))
	paginesVal := strings.TrimSpace(r.FormValue("pagines"))
	pagines := sql.NullInt64{}
	if paginesVal != "" {
		if p, err := strconv.Atoi(paginesVal); err == nil {
			pagines = sql.NullInt64{Int64: int64(p), Valid: true}
		}
	}
	return &db.Llibre{
		ID:                intFromForm(r.FormValue("id")),
		ArquebisbatID:     arquevisbatID,
		MunicipiID:        municipiID,
		NomEsglesia:       strings.TrimSpace(r.FormValue("nom_esglesia")),
		CodiDigital:       strings.TrimSpace(r.FormValue("codi_digital")),
		CodiFisic:         strings.TrimSpace(r.FormValue("codi_fisic")),
		Titol:             strings.TrimSpace(r.FormValue("titol")),
		Cronologia:        strings.TrimSpace(r.FormValue("cronologia")),
		Volum:             strings.TrimSpace(r.FormValue("volum")),
		Abat:              strings.TrimSpace(r.FormValue("abat")),
		Contingut:         strings.TrimSpace(r.FormValue("contingut")),
		Llengua:           strings.TrimSpace(r.FormValue("llengua")),
		Requeriments:      strings.TrimSpace(r.FormValue("requeriments_tecnics")),
		UnitatCatalogacio: strings.TrimSpace(r.FormValue("unitat_catalogacio")),
		UnitatInstalacio:  strings.TrimSpace(r.FormValue("unitat_instalacio")),
		Pagines:           pagines,
		URLBase:           strings.TrimSpace(r.FormValue("url_base")),
		URLImatgePrefix:   strings.TrimSpace(r.FormValue("url_imatge_prefix")),
		Pagina:            strings.TrimSpace(r.FormValue("pagina")),
	}
}

func (a *App) validateLlibre(l *db.Llibre) string {
	if l.ArquebisbatID == 0 {
		return "Cal indicar l'entitat eclesiàstica."
	}
	if l.MunicipiID == 0 {
		return "Cal indicar el municipi."
	}
	if strings.TrimSpace(l.Titol) == "" && strings.TrimSpace(l.NomEsglesia) == "" {
		return "Cal un títol o nom d'església."
	}
	return ""
}

func (a *App) renderLlibreForm(w http.ResponseWriter, r *http.Request, l *db.Llibre, isNew bool, errMsg string) {
	arquebisbats, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	municipis, _ := a.DB.ListMunicipis(db.MunicipiFilter{})
	RenderPrivateTemplate(w, r, "admin-llibres-form.html", map[string]interface{}{
		"Llibre":          l,
		"Arquebisbats":    arquebisbats,
		"Municipis":       municipis,
		"IsNew":           isNew,
		"Error":           errMsg,
		"CanManageArxius": true,
	})
}

func intFromForm(val string) int {
	if v, err := strconv.Atoi(strings.TrimSpace(val)); err == nil {
		return v
	}
	return 0
}

func (a *App) AdminSaveLlibre(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permArxius); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/documentals/llibres", http.StatusSeeOther)
		return
	}
	user, _ := a.VerificarSessio(r)
	llibre := parseLlibreForm(r)
	isNew := llibre.ID == 0
	if msg := a.validateLlibre(llibre); msg != "" {
		a.renderLlibreForm(w, r, llibre, isNew, msg)
		return
	}
	llibre.CreatedBy = sqlNullIntFromInt(user.ID)
	llibre.ModeracioEstat = "pendent"
	llibre.ModeratedBy = sql.NullInt64{}
	llibre.ModeratedAt = sql.NullTime{}
	if isNew {
		id, err := a.DB.CreateLlibre(llibre)
		if err != nil {
			a.renderLlibreForm(w, r, llibre, isNew, "No s'ha pogut crear el llibre.")
			return
		}
		_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleLlibreCreate, "crear", "llibre", &id, "pendent", nil, "")
	} else {
		if err := a.DB.UpdateLlibre(llibre); err != nil {
			a.renderLlibreForm(w, r, llibre, isNew, "No s'ha pogut actualitzar el llibre.")
			return
		}
		_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleLlibreUpdate, "editar", "llibre", &llibre.ID, "pendent", nil, "")
	}
	if strings.TrimSpace(r.FormValue("recalc_pagines")) != "" && llibre.Pagines.Valid {
		_ = a.DB.RecalcLlibrePagines(llibre.ID, int(llibre.Pagines.Int64))
	}
	http.Redirect(w, r, "/documentals/llibres", http.StatusSeeOther)
}

func (a *App) AdminLlibrePagines(w http.ResponseWriter, r *http.Request) {
	user, _, ok := a.requirePermission(w, r, permArxius)
	if !ok {
		return
	}
	id := extractID(r.URL.Path)
	llibre, err := a.DB.GetLlibre(id)
	if err != nil || llibre == nil {
		http.NotFound(w, r)
		return
	}
	pagines, _ := a.DB.ListLlibrePagines(id)
	RenderPrivateTemplate(w, r, "admin-llibres-pagines.html", map[string]interface{}{
		"Llibre":          llibre,
		"Pagines":         pagines,
		"User":            user,
		"CanManageArxius": true,
	})
}

func (a *App) AdminSaveLlibrePagina(w http.ResponseWriter, r *http.Request) {
	user, _, ok := a.requirePermission(w, r, permArxius)
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/documentals/llibres", http.StatusSeeOther)
		return
	}
	llibreID := extractID(r.URL.Path)
	if llibreID == 0 {
		http.NotFound(w, r)
		return
	}
	if strings.TrimSpace(r.FormValue("recalc")) != "" {
		total, _ := strconv.Atoi(strings.TrimSpace(r.FormValue("total_pagines")))
		if total == 0 {
			if ll, err := a.DB.GetLlibre(llibreID); err == nil && ll != nil && ll.Pagines.Valid {
				total = int(ll.Pagines.Int64)
			}
		}
		if total > 0 {
			_ = a.DB.RecalcLlibrePagines(llibreID, total)
		}
		http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID)+"/pagines", http.StatusSeeOther)
		return
	}
	numPagina, _ := strconv.Atoi(r.FormValue("num_pagina"))
	if numPagina == 0 {
		http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID)+"/pagines?error=page", http.StatusSeeOther)
		return
	}
	p := &db.LlibrePagina{
		ID:        intFromForm(r.FormValue("page_id")),
		LlibreID:  llibreID,
		NumPagina: numPagina,
		Estat:     strings.TrimSpace(r.FormValue("estat")),
		IndexedAt: sql.NullString{String: strings.TrimSpace(r.FormValue("indexed_at")), Valid: strings.TrimSpace(r.FormValue("indexed_at")) != ""},
		IndexedBy: parseNullInt64(r.FormValue("indexed_by")),
		Notes:     strings.TrimSpace(r.FormValue("notes")),
	}
	if p.Estat == "" {
		p.Estat = "pendent"
	}
	pageID, _ := a.DB.SaveLlibrePagina(p)
	if pageID == 0 {
		pageID = p.ID
	}
	if strings.ToLower(p.Estat) == "indexada" {
		_, _ = a.RegisterUserActivity(r.Context(), user.ID, rulePaginaIndex, "indexar", "llibre_pagina", &pageID, "validat", nil, "")
	}
	http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID)+"/pagines", http.StatusSeeOther)
}

func (a *App) AdminShowLlibre(w http.ResponseWriter, r *http.Request) {
	user, _, _ := a.requirePermission(w, r, permArxius)
	id := extractID(r.URL.Path)
	llibre, err := a.DB.GetLlibre(id)
	if err != nil || llibre == nil {
		http.NotFound(w, r)
		return
	}
	if user == nil {
		user, _ = a.VerificarSessio(r)
	}
	if (user == nil || !a.CanManageArxius(user)) && llibre.ModeracioEstat != "publicat" {
		http.NotFound(w, r)
		return
	}
	arxius, _ := a.DB.ListLlibreArxius(id)
	arxiusOpts, _ := a.DB.ListArxius(db.ArxiuFilter{Limit: 200})
	RenderPrivateTemplate(w, r, "admin-llibres-show.html", map[string]interface{}{
		"Llibre":          llibre,
		"Arxius":          arxius,
		"ArxiusOptions":   arxiusOpts,
		"User":            user,
		"CanManageArxius": true,
	})
}

func (a *App) AdminAddLlibreArxiu(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permArxius); !ok {
		return
	}
	llibreID := extractID(r.URL.Path)
	arxiuID, _ := strconv.Atoi(r.FormValue("arxiu_id"))
	signatura := strings.TrimSpace(r.FormValue("signatura"))
	urlOverride := strings.TrimSpace(r.FormValue("url_override"))
	if llibreID == 0 || arxiuID == 0 {
		http.Redirect(w, r, "/documentals/llibres", http.StatusSeeOther)
		return
	}
	_ = a.DB.AddArxiuLlibre(arxiuID, llibreID, signatura, urlOverride)
	http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID), http.StatusSeeOther)
}

func (a *App) AdminUpdateLlibreArxiu(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permArxius); !ok {
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 5 {
		http.NotFound(w, r)
		return
	}
	llibreID, _ := strconv.Atoi(parts[2])
	arxiuID, _ := strconv.Atoi(parts[4])
	signatura := strings.TrimSpace(r.FormValue("signatura"))
	urlOverride := strings.TrimSpace(r.FormValue("url_override"))
	_ = a.DB.UpdateArxiuLlibre(arxiuID, llibreID, signatura, urlOverride)
	http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID), http.StatusSeeOther)
}

func (a *App) AdminDeleteLlibreArxiu(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permArxius); !ok {
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 5 {
		http.NotFound(w, r)
		return
	}
	llibreID, _ := strconv.Atoi(parts[2])
	arxiuID, _ := strconv.Atoi(parts[4])
	_ = a.DB.DeleteArxiuLlibre(arxiuID, llibreID)
	http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(llibreID), http.StatusSeeOther)
}
