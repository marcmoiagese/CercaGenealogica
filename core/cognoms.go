package core

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type cognomReferenciaView struct {
	ID          int
	Kind        string
	KindLabel   string
	Title       string
	Description string
	Page        string
	LinkURL     string
	LinkLabel   string
	Status      string
}

func normalizeCognomReferenciaKind(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	switch raw {
	case "llibre", "arxiu", "media", "url":
		return raw
	default:
		return ""
	}
}

func (a *App) buildCognomReferenciaViews(lang string, refs []db.CognomReferencia) []cognomReferenciaView {
	if len(refs) == 0 {
		return nil
	}
	arxius := map[int]*db.Arxiu{}
	llibres := map[int]*db.Llibre{}
	media := map[int]*db.MediaItem{}
	var out []cognomReferenciaView
	for _, ref := range refs {
		view := cognomReferenciaView{
			ID:          ref.ID,
			Kind:        strings.TrimSpace(ref.Kind),
			Title:       strings.TrimSpace(ref.Titol),
			Description: strings.TrimSpace(ref.Descripcio),
			Page:        strings.TrimSpace(ref.Pagina),
			Status:      strings.TrimSpace(ref.ModeracioEstat),
		}
		if view.Kind != "" {
			view.KindLabel = T(lang, "surnames.references.kind."+view.Kind)
		}
		if view.KindLabel == "" {
			view.KindLabel = view.Kind
		}
		if ref.RefID.Valid {
			id := int(ref.RefID.Int64)
			switch view.Kind {
			case "llibre":
				llibre, ok := llibres[id]
				if !ok {
					row, err := a.DB.GetLlibre(id)
					if err == nil {
						llibre = row
					}
					llibres[id] = llibre
				}
				view.LinkURL = fmt.Sprintf("/documentals/llibres/%d", id)
				if llibre != nil {
					view.LinkLabel = strings.TrimSpace(llibre.Titol)
					if view.LinkLabel == "" {
						view.LinkLabel = strings.TrimSpace(llibre.NomEsglesia)
					}
				}
				if view.LinkLabel == "" {
					view.LinkLabel = fmt.Sprintf("%s #%d", T(lang, "surnames.references.kind.llibre"), id)
				}
			case "arxiu":
				arxiu, ok := arxius[id]
				if !ok {
					row, err := a.DB.GetArxiu(id)
					if err == nil {
						arxiu = row
					}
					arxius[id] = arxiu
				}
				view.LinkURL = fmt.Sprintf("/documentals/arxius/%d", id)
				if arxiu != nil {
					view.LinkLabel = strings.TrimSpace(arxiu.Nom)
				}
				if view.LinkLabel == "" {
					view.LinkLabel = fmt.Sprintf("%s #%d", T(lang, "surnames.references.kind.arxiu"), id)
				}
			case "media":
				item, ok := media[id]
				if !ok {
					row, err := a.DB.GetMediaItemByID(id)
					if err == nil {
						item = row
					}
					media[id] = item
				}
				if item != nil && strings.TrimSpace(item.PublicID) != "" {
					view.LinkURL = "/media/items/" + strings.TrimSpace(item.PublicID)
				}
				if item != nil {
					view.LinkLabel = strings.TrimSpace(item.Title)
					if view.LinkLabel == "" {
						view.LinkLabel = strings.TrimSpace(item.PublicID)
					}
				}
				if view.LinkLabel == "" {
					view.LinkLabel = fmt.Sprintf("%s #%d", T(lang, "surnames.references.kind.media"), id)
				}
			}
		}
		if view.Kind == "url" {
			view.LinkURL = strings.TrimSpace(ref.URL)
			view.LinkLabel = view.LinkURL
		}
		if view.Title == "" {
			view.Title = view.LinkLabel
		}
		if view.Title == "" {
			view.Title = view.KindLabel
		}
		out = append(out, view)
	}
	return out
}

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
		Errorf("Error llistant cognoms (q=%s): %v", q, err)
		http.Error(w, "Error carregant cognoms", http.StatusInternalServerError)
		return
	}
	all, err := a.DB.ListCognoms(q, 0, 0)
	if err != nil {
		Errorf("Error comptant cognoms (q=%s): %v", q, err)
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
	if id > 0 {
		canonID, redirected, err := a.resolveCognomCanonicalID(id)
		if err != nil {
			http.Error(w, "Error carregant cognom", http.StatusInternalServerError)
			return
		}
		if redirected {
			http.Redirect(w, r, fmt.Sprintf("/cognoms/%d", canonID), http.StatusSeeOther)
			return
		}
		id = canonID
	}
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
	lang := ResolveLang(r)
	if user != nil {
		lang = resolveUserLang(r, user)
	}
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
	refPub, _ := a.DB.ListCognomReferencies(db.CognomReferenciaFilter{CognomID: id, Status: "publicat"})
	refViews := a.buildCognomReferenciaViews(lang, refPub)
	redirects, _ := a.DB.ListCognomRedirectsByTo(id)
	redirectViews := a.buildCognomRedirectViews(redirects)
	var refPendingViews []cognomReferenciaView
	if canModerate {
		if refsPending, err := a.DB.ListCognomReferencies(db.CognomReferenciaFilter{CognomID: id, Status: "pendent"}); err == nil {
			refPendingViews = a.buildCognomReferenciaViews(lang, refsPending)
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
		"RefOk":            strings.TrimSpace(r.URL.Query().Get("ref_ok")) != "",
		"RefError":         strings.TrimSpace(r.URL.Query().Get("ref_err")) != "",
		"References":       refViews,
		"MergeRedirects":   redirectViews,
		"ReferencesPending": func() []cognomReferenciaView {
			if len(refPendingViews) == 0 {
				return nil
			}
			return refPendingViews
		}(),
		"MaxYear":          maxYear,
		"Y0":               1800,
		"Y1":               maxYear,
		"SuggestOk":        r.URL.Query().Get("suggest_ok") != "",
		"DuplicateVariant": r.URL.Query().Get("duplicate") != "",
		"SuggestError":     r.URL.Query().Get("err") != "",
	})
}

func buildCognomChangeMeta(cognom *db.Cognom, apply func(*db.Cognom)) (string, error) {
	if cognom == nil {
		return "", fmt.Errorf("cognom buit")
	}
	after := *cognom
	if apply != nil {
		apply(&after)
	}
	beforeJSON, err := json.Marshal(cognom)
	if err != nil {
		return "", err
	}
	afterJSON, err := json.Marshal(after)
	if err != nil {
		return "", err
	}
	return buildWikiChangeMetadata(beforeJSON, afterJSON, 0)
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
	if id > 0 {
		if canonID, _, err := a.resolveCognomCanonicalID(id); err == nil && canonID > 0 {
			id = canonID
		}
	}
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
	meta, err := buildCognomChangeMeta(cognom, func(after *db.Cognom) {
		after.Origen = origen
		after.Notes = notes
	})
	if err != nil {
		http.Error(w, T(lang, "surnames.detail.contribution.prepare_error"), http.StatusInternalServerError)
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
		http.Error(w, T(lang, "surnames.detail.contribution.create_error"), http.StatusInternalServerError)
		return
	}
	detail := "cognom:" + strconv.Itoa(id)
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, "", "editar", "cognom_canvi", &changeID, "pendent", nil, detail)
	http.Redirect(w, r, "/cognoms/"+strconv.Itoa(id)+"?pending=1", http.StatusSeeOther)
}

func (a *App) CognomSubmitHistoria(w http.ResponseWriter, r *http.Request) {
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
	if id > 0 {
		if canonID, _, err := a.resolveCognomCanonicalID(id); err == nil && canonID > 0 {
			id = canonID
		}
	}
	cognom, err := a.DB.GetCognom(id)
	if err != nil || cognom == nil {
		http.NotFound(w, r)
		return
	}
	if !a.ensureWikiChangeAllowed(w, r, lang) {
		return
	}
	historia := strings.TrimSpace(r.FormValue("historia"))
	meta, err := buildCognomChangeMeta(cognom, func(after *db.Cognom) {
		after.Origen = historia
	})
	if err != nil {
		http.Error(w, T(lang, "surnames.detail.contribution.prepare_error"), http.StatusInternalServerError)
		return
	}
	changeID, err := a.createWikiChange(&db.WikiChange{
		ObjectType:     "cognom",
		ObjectID:       id,
		ChangeType:     "form",
		FieldKey:       "historia",
		Metadata:       meta,
		ModeracioEstat: "pendent",
		ChangedBy:      sqlNullIntFromInt(user.ID),
	})
	if err != nil {
		if status, msg, ok := a.wikiGuardrailInfo(lang, err); ok {
			http.Error(w, msg, status)
			return
		}
		http.Error(w, T(lang, "surnames.detail.contribution.create_error"), http.StatusInternalServerError)
		return
	}
	detail := "cognom:" + strconv.Itoa(id)
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, "", "editar", "cognom_canvi", &changeID, "pendent", nil, detail)
	http.Redirect(w, r, "/cognoms/"+strconv.Itoa(id)+"?pending=1", http.StatusSeeOther)
}

func (a *App) CognomSubmitNotes(w http.ResponseWriter, r *http.Request) {
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
	if id > 0 {
		if canonID, _, err := a.resolveCognomCanonicalID(id); err == nil && canonID > 0 {
			id = canonID
		}
	}
	cognom, err := a.DB.GetCognom(id)
	if err != nil || cognom == nil {
		http.NotFound(w, r)
		return
	}
	if !a.ensureWikiChangeAllowed(w, r, lang) {
		return
	}
	notes := strings.TrimSpace(r.FormValue("notes"))
	meta, err := buildCognomChangeMeta(cognom, func(after *db.Cognom) {
		after.Notes = notes
	})
	if err != nil {
		http.Error(w, T(lang, "surnames.detail.contribution.prepare_error"), http.StatusInternalServerError)
		return
	}
	changeID, err := a.createWikiChange(&db.WikiChange{
		ObjectType:     "cognom",
		ObjectID:       id,
		ChangeType:     "form",
		FieldKey:       "notes",
		Metadata:       meta,
		ModeracioEstat: "pendent",
		ChangedBy:      sqlNullIntFromInt(user.ID),
	})
	if err != nil {
		if status, msg, ok := a.wikiGuardrailInfo(lang, err); ok {
			http.Error(w, msg, status)
			return
		}
		http.Error(w, T(lang, "surnames.detail.contribution.create_error"), http.StatusInternalServerError)
		return
	}
	detail := "cognom:" + strconv.Itoa(id)
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, "", "editar", "cognom_canvi", &changeID, "pendent", nil, detail)
	http.Redirect(w, r, "/cognoms/"+strconv.Itoa(id)+"?pending=1", http.StatusSeeOther)
}

func (a *App) CognomSubmitReferencia(w http.ResponseWriter, r *http.Request) {
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
	if id > 0 {
		if canonID, _, err := a.resolveCognomCanonicalID(id); err == nil && canonID > 0 {
			id = canonID
		}
	}
	cognom, err := a.DB.GetCognom(id)
	if err != nil || cognom == nil {
		http.NotFound(w, r)
		return
	}
	kind := normalizeCognomReferenciaKind(r.FormValue("kind"))
	if kind == "" {
		http.Redirect(w, r, "/cognoms/"+strconv.Itoa(id)+"?ref_err=1", http.StatusSeeOther)
		return
	}
	refID := sqlNullInt(r.FormValue("ref_id"))
	urlVal := strings.TrimSpace(r.FormValue("url"))
	if kind == "url" {
		if urlVal == "" {
			http.Redirect(w, r, "/cognoms/"+strconv.Itoa(id)+"?ref_err=1", http.StatusSeeOther)
			return
		}
		refID = sql.NullInt64{}
	} else if !refID.Valid {
		http.Redirect(w, r, "/cognoms/"+strconv.Itoa(id)+"?ref_err=1", http.StatusSeeOther)
		return
	} else {
		ref := int(refID.Int64)
		switch kind {
		case "llibre":
			if row, err := a.DB.GetLlibre(ref); err != nil || row == nil {
				http.Redirect(w, r, "/cognoms/"+strconv.Itoa(id)+"?ref_err=1", http.StatusSeeOther)
				return
			}
		case "arxiu":
			if row, err := a.DB.GetArxiu(ref); err != nil || row == nil {
				http.Redirect(w, r, "/cognoms/"+strconv.Itoa(id)+"?ref_err=1", http.StatusSeeOther)
				return
			}
		case "media":
			if row, err := a.DB.GetMediaItemByID(ref); err != nil || row == nil {
				http.Redirect(w, r, "/cognoms/"+strconv.Itoa(id)+"?ref_err=1", http.StatusSeeOther)
				return
			}
		}
	}
	ref := &db.CognomReferencia{
		CognomID:       id,
		Kind:           kind,
		RefID:          refID,
		URL:            urlVal,
		Titol:          strings.TrimSpace(r.FormValue("titol")),
		Descripcio:     strings.TrimSpace(r.FormValue("descripcio")),
		Pagina:         strings.TrimSpace(r.FormValue("pagina")),
		ModeracioEstat: "pendent",
		CreatedBy:      sqlNullIntFromInt(user.ID),
	}
	if _, err := a.DB.CreateCognomReferencia(ref); err != nil {
		http.Error(w, T(lang, "surnames.references.error"), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/cognoms/"+strconv.Itoa(id)+"?ref_ok=1", http.StatusSeeOther)
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
	if id > 0 {
		if canonID, _, err := a.resolveCognomCanonicalID(id); err == nil && canonID > 0 {
			id = canonID
		}
	}
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
		Errorf("Error cerca cognoms JSON (q=%s): %v", q, err)
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
	if len(parts) < 2 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	id, _ := strconv.Atoi(parts[0])
	if id <= 0 {
		http.NotFound(w, r)
		return
	}
	if canonID, _, err := a.resolveCognomCanonicalID(id); err == nil && canonID > 0 {
		id = canonID
	}
	switch parts[1] {
	case "heatmap":
		a.cognomHeatmapJSON(w, r, id)
		return
	case "stats":
		a.cognomStatsAPI(w, r, id, parts[2:])
		return
	default:
		http.NotFound(w, r)
		return
	}
}

func (a *App) cognomHeatmapJSON(w http.ResponseWriter, r *http.Request, id int) {
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
