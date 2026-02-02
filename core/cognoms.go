package core

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func (a *App) CognomsList(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
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
	offset := (page - 1) * perPage
	list, err := a.DB.ListCognoms(q, perPage, offset)
	if err != nil {
		http.Error(w, "Error carregant cognoms", http.StatusInternalServerError)
		return
	}
	all, err := a.DB.ListCognoms(q, 0, 0)
	if err != nil {
		http.Error(w, "Error carregant cognoms", http.StatusInternalServerError)
		return
	}
	total := len(all)
	totalPages := 1
	if total > 0 {
		totalPages = (total + perPage - 1) / perPage
	}
	if page > totalPages {
		page = totalPages
	}
	pageBase := "/cognoms?per_page=" + strconv.Itoa(perPage)
	if q != "" {
		pageBase += "&q=" + url.QueryEscape(q)
	}
	RenderPrivateTemplate(w, r, "cognoms-list.html", map[string]interface{}{
		"Cognoms":    list,
		"Q":          q,
		"Total":      total,
		"Page":       page,
		"PerPage":    perPage,
		"TotalPages": totalPages,
		"HasPrev":    page > 1,
		"HasNext":    page < totalPages,
		"PrevPage":   page - 1,
		"NextPage":   page + 1,
		"PageBase":   pageBase,
	})
}

func (a *App) CognomDetall(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	cognom, err := a.DB.GetCognom(id)
	if err != nil || cognom == nil {
		http.NotFound(w, r)
		return
	}
	variants, err := a.DB.ListCognomVariants(db.CognomVariantFilter{
		CognomID: id,
		Status:   "publicat",
	})
	if err != nil {
		http.Error(w, "Error carregant variants", http.StatusInternalServerError)
		return
	}
	user, _ := a.VerificarSessio(r)
	perms := a.getPermissionsForUser(user.ID)
	canModerate := a.hasPerm(perms, permModerate)
	markType := ""
	markPublic := true
	markOwn := false
	if user != nil {
		if marks, err := a.DB.ListWikiMarks("cognom", []int{id}); err == nil {
			for _, mark := range marks {
				if mark.UserID == user.ID {
					markType = mark.Tipus
					markPublic = mark.IsPublic
					markOwn = true
					break
				}
			}
		}
	}
	var pendents []db.CognomVariant
	if canModerate {
		if rows, err := a.DB.ListCognomVariants(db.CognomVariantFilter{CognomID: id, Status: "pendent"}); err == nil {
			pendents = rows
		}
	}
	maxYear := time.Now().Year()
	RenderPrivateTemplate(w, r, "cognom-detall.html", map[string]interface{}{
		"Cognom":           cognom,
		"Variants":         variants,
		"VariantsPendents": pendents,
		"CanModerate":      canModerate,
		"MarkType":         markType,
		"MarkPublic":       markPublic,
		"MarkOwn":          markOwn,
		"WikiPending":      strings.TrimSpace(r.URL.Query().Get("pending")) != "",
		"MaxYear":          maxYear,
		"Y0":               1800,
		"Y1":               maxYear,
		"SuggestOk":        r.URL.Query().Get("suggest_ok") != "",
		"DuplicateVariant": r.URL.Query().Get("duplicate") != "",
		"SuggestError":     r.URL.Query().Get("err") != "",
	})
}

func (a *App) CognomProposeUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/cognoms", http.StatusSeeOther)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Redirect(w, r, "/cognoms?err=csrf", http.StatusSeeOther)
		return
	}
	user, _ := a.VerificarSessio(r)
	if user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	lang := resolveUserLang(r, user)
	id := extractID(r.URL.Path)
	cognom, err := a.DB.GetCognom(id)
	if err != nil || cognom == nil {
		http.NotFound(w, r)
		return
	}
	if !a.ensureWikiChangeAllowed(w, r, lang) {
		return
	}
	origen := strings.TrimSpace(r.FormValue("origen"))
	notes := strings.TrimSpace(r.FormValue("notes"))
	after := *cognom
	after.Origen = origen
	after.Notes = notes
	beforeJSON, _ := json.Marshal(cognom)
	afterJSON, _ := json.Marshal(after)
	meta, err := buildWikiChangeMetadata(beforeJSON, afterJSON, 0)
	if err != nil {
		http.Error(w, "No s'ha pogut preparar la proposta", http.StatusInternalServerError)
		return
	}
	changeID, err := a.createWikiChange(&db.WikiChange{
		ObjectType:     "cognom",
		ObjectID:       id,
		ChangeType:     "form",
		FieldKey:       "origen_notes",
		Metadata:       meta,
		ModeracioEstat: "pendent",
		ChangedBy:      sqlNullIntFromInt(user.ID),
	})
	if err != nil {
		if status, msg, ok := a.wikiGuardrailInfo(lang, err); ok {
			http.Error(w, msg, status)
			return
		}
		http.Error(w, "No s'ha pogut crear la proposta", http.StatusInternalServerError)
		return
	}
	detail := "cognom:" + strconv.Itoa(id)
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, "", "editar", "cognom_canvi", &changeID, "pendent", nil, detail)
	http.Redirect(w, r, "/cognoms/"+strconv.Itoa(id)+"?pending=1", http.StatusSeeOther)
}

func (a *App) CognomSuggestVariant(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/cognoms", http.StatusSeeOther)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Redirect(w, r, "/cognoms?err=csrf", http.StatusSeeOther)
		return
	}
	user, _ := a.VerificarSessio(r)
	id := extractID(r.URL.Path)
	cognom, err := a.DB.GetCognom(id)
	if err != nil || cognom == nil {
		http.NotFound(w, r)
		return
	}
	variant := strings.TrimSpace(r.FormValue("variant"))
	if len(variant) < 2 {
		http.Redirect(w, r, "/cognoms/"+strconv.Itoa(id)+"?err=1", http.StatusSeeOther)
		return
	}
	if len([]rune(variant)) > 80 {
		http.Redirect(w, r, "/cognoms/"+strconv.Itoa(id)+"?err=1", http.StatusSeeOther)
		return
	}
	key := NormalizeCognomKey(variant)
	if key == "" || key == cognom.Key {
		http.Redirect(w, r, "/cognoms/"+strconv.Itoa(id)+"?duplicate=1", http.StatusSeeOther)
		return
	}
	existing, _ := a.DB.ListCognomVariants(db.CognomVariantFilter{CognomID: id, Q: key})
	for _, v := range existing {
		if v.Key == key {
			http.Redirect(w, r, "/cognoms/"+strconv.Itoa(id)+"?duplicate=1", http.StatusSeeOther)
			return
		}
	}
	cv := &db.CognomVariant{
		CognomID:       id,
		Variant:        variant,
		Key:            key,
		Llengua:        strings.TrimSpace(r.FormValue("llengua")),
		AnyInici:       sqlNullInt(r.FormValue("any_inici")),
		AnyFi:          sqlNullInt(r.FormValue("any_fi")),
		PaisID:         sqlNullInt(r.FormValue("pais_id")),
		MunicipiID:     sqlNullInt(r.FormValue("municipi_id")),
		ModeracioEstat: "pendent",
		CreatedBy:      sqlNullIntFromInt(user.ID),
	}
	variantID, err := a.DB.CreateCognomVariant(cv)
	if err != nil {
		http.Redirect(w, r, "/cognoms/"+strconv.Itoa(id)+"?err=1", http.StatusSeeOther)
		return
	}
	details, _ := json.Marshal(map[string]interface{}{
		"cognom_id": id,
		"variant":   variant,
		"key":       key,
	})
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, "cognom_variant_create", "crear", "cognom_variant", &variantID, "pendent", nil, string(details))
	http.Redirect(w, r, "/cognoms/"+strconv.Itoa(id)+"?suggest_ok=1", http.StatusSeeOther)
}

func (a *App) SearchCognomsJSON(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	list, err := a.DB.ListCognoms(q, 20, 0)
	if err != nil {
		http.Error(w, "Error", http.StatusInternalServerError)
		return
	}
	type result struct {
		ID    int    `json:"id"`
		Forma string `json:"forma"`
	}
	resp := make([]result, 0, len(list))
	for _, c := range list {
		resp = append(resp, result{ID: c.ID, Forma: c.Forma})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (a *App) CognomHeatmapJSON(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/cognoms/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 || parts[0] == "" || parts[1] != "heatmap" {
		http.NotFound(w, r)
		return
	}
	id, _ := strconv.Atoi(parts[0])
	if id <= 0 {
		http.NotFound(w, r)
		return
	}
	y0, _ := strconv.Atoi(r.URL.Query().Get("y0"))
	y1, _ := strconv.Atoi(r.URL.Query().Get("y1"))
	if y0 > 0 && y1 > 0 && y0 > y1 {
		y0, y1 = y1, y0
	}
	points, err := a.DB.QueryCognomHeatmap(id, y0, y1)
	if err != nil {
		http.Error(w, "Error", http.StatusInternalServerError)
		return
	}
	type point struct {
		Lat  float64 `json:"lat"`
		Lon  float64 `json:"lon"`
		W    int     `json:"w"`
		Name string  `json:"name"`
	}
	out := make([]point, 0, len(points))
	for _, p := range points {
		if !p.Latitud.Valid || !p.Longitud.Valid {
			continue
		}
		out = append(out, point{
			Lat:  p.Latitud.Float64,
			Lon:  p.Longitud.Float64,
			W:    p.Freq,
			Name: p.MunicipiNom.String,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"y0":     y0,
		"y1":     y1,
		"points": out,
	})
}
