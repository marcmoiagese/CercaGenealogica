package core

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const (
	municipiAnecdoteMaxBytes int64 = 64 << 10
	municipiAnecdoteRateLimit      = 0.5
	municipiAnecdoteRateBurst      = 3
	anecdoteTitleMin               = 3
	anecdoteTitleMax               = 120
	anecdoteTextMin                = 20
	anecdoteTextMax                = 4000
	anecdoteTagMaxLen              = 40
	anecdoteDataRefMax             = 32
	anecdoteFontURLMax             = 200
	anecdoteCommentMin             = 3
	anecdoteCommentMax             = 1000
	municipiAnecdoteSubmitDailyLimit = 5
	municipiAnecdoteSubmitWindow     = 24 * time.Hour
	municipiAnecdoteCommentCooldown  = 30 * time.Second
)

var (
	anecdoteTags = []string{"cases", "carrers", "toponims", "costums", "festes", "cognoms", "altres"}
	anecdoteDataRefPattern = regexp.MustCompile(`^\d{4}(-\d{2}-\d{2})?$`)
)

type anecdoteListItem struct {
	ItemID    int
	Title     string
	Tag       string
	DataRef   string
	Snippet   string
	CreatedAt string
}

type anecdoteCommentView struct {
	Body      string
	CreatedAt string
}

type anecdoteFormTagOption struct {
	Value string
	Label string
}

type anecdoteCreatePayload struct {
	Title     string `json:"titol"`
	Tag       string `json:"tag"`
	DataRef   string `json:"data_ref"`
	Text      string `json:"text"`
	FontURL   string `json:"font_url"`
	CSRFToken string `json:"csrf_token"`
}

type anecdoteSavePayload struct {
	LockVersion int    `json:"lock_version"`
	Title       string `json:"titol"`
	Tag         string `json:"tag"`
	DataRef     string `json:"data_ref"`
	Text        string `json:"text"`
	FontURL     string `json:"font_url"`
	CSRFToken   string `json:"csrf_token"`
}

type anecdoteCommentPayload struct {
	Body      string `json:"body"`
	CSRFToken string `json:"csrf_token"`
}

func (a *App) MunicipiAnecdotesListPage(w http.ResponseWriter, r *http.Request) {
	munID := municipiIDFromPath(r.URL.Path)
	if munID <= 0 {
		http.NotFound(w, r)
		return
	}
	mun, err := a.DB.GetMunicipi(munID)
	if err != nil || mun == nil {
		http.NotFound(w, r)
		return
	}
	user, _ := a.VerificarSessio(r)
	perms := db.PolicyPermissions{}
	if user != nil {
		perms = a.getPermissionsForUser(user.ID)
	}
	canManageTerritory := user != nil && a.hasPerm(perms, permTerritory)
	canModerate := user != nil && a.hasPerm(perms, permModerate)
	if mun.ModeracioEstat != "" && mun.ModeracioEstat != "publicat" && !(canManageTerritory || canModerate) {
		http.NotFound(w, r)
		return
	}

	filterQ := strings.TrimSpace(r.URL.Query().Get("q"))
	filterTag := normalizeAnecdoteTag(r.URL.Query().Get("tag"))
	page := 1
	perPage := 12
	if val := strings.TrimSpace(r.URL.Query().Get("page")); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			page = n
		}
	}
	if val := strings.TrimSpace(r.URL.Query().Get("per_page")); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			perPage = n
		}
	}
	offset := (page - 1) * perPage

	filter := db.MunicipiAnecdotariFilter{
		Tag:    filterTag,
		Query:  filterQ,
		Status: "publicat",
		Limit:  perPage,
		Offset: offset,
	}
	items, total, err := a.DB.ListMunicipiAnecdotariPublished(munID, filter)
	if err != nil {
		Errorf("Error carregant anecdotari municipi %d: %v", munID, err)
		items = []db.MunicipiAnecdotariVersion{}
		total = 0
	}
	viewItems := []anecdoteListItem{}
	for _, item := range items {
		viewItems = append(viewItems, anecdoteListItem{
			ItemID:    item.ItemID,
			Title:     strings.TrimSpace(item.Titol),
			Tag:       strings.TrimSpace(item.Tag),
			DataRef:   formatCronologiaDisplay(item.DataRef),
			Snippet:   summarizeAnecdoteText(item.Text, 220),
			CreatedAt: formatAnecdoteDate(item.CreatedAt),
		})
	}

	totalPages := 1
	if total > 0 {
		totalPages = (total + perPage - 1) / perPage
	}
	if page > totalPages {
		page = totalPages
	}
	values := url.Values{}
	if filterQ != "" {
		values.Set("q", filterQ)
	}
	if filterTag != "" {
		values.Set("tag", filterTag)
	}
	if perPage != 12 {
		values.Set("per_page", strconv.Itoa(perPage))
	}
	pageBase := "/territori/municipis/" + strconv.Itoa(munID) + "/anecdotes?" + values.Encode()

	munTarget := a.resolveMunicipiTarget(mun.ID)
	canCreateAnecdote := user != nil && a.HasPermission(user.ID, permKeyTerritoriMunicipisAnecdotesCreate, munTarget)

	data := map[string]interface{}{
		"Municipi":         mun,
		"Anecdotes":        viewItems,
		"FilterQ":          filterQ,
		"FilterTag":        filterTag,
		"TagOptions":       anecdoteTagOptions(ResolveLang(r)),
		"Page":             page,
		"PerPage":          perPage,
		"TotalPages":       totalPages,
		"HasPrev":          page > 1,
		"HasNext":          page < totalPages,
		"PrevPage":         page - 1,
		"NextPage":         page + 1,
		"PageBase":         pageBase,
		"CanCreateAnecdote": canCreateAnecdote,
	}
	if user != nil {
		RenderPrivateTemplate(w, r, "municipi-anecdotes-list.html", data)
		return
	}
	RenderTemplate(w, r, "municipi-anecdotes-list.html", data)
}

func (a *App) MunicipiAnecdoteDetailPage(w http.ResponseWriter, r *http.Request) {
	munID := municipiIDFromPath(r.URL.Path)
	itemID := anecdoteItemIDFromPath(r.URL.Path)
	if munID <= 0 || itemID <= 0 {
		http.NotFound(w, r)
		return
	}
	mun, err := a.DB.GetMunicipi(munID)
	if err != nil || mun == nil {
		http.NotFound(w, r)
		return
	}
	user, _ := a.VerificarSessio(r)
	perms := db.PolicyPermissions{}
	if user != nil {
		perms = a.getPermissionsForUser(user.ID)
	}
	canManageTerritory := user != nil && a.hasPerm(perms, permTerritory)
	munTarget := a.resolveMunicipiTarget(mun.ID)
	canModerate := user != nil && (a.hasPerm(perms, permModerate) || a.HasPermission(user.ID, permKeyTerritoriMunicipisAnecdotesModerate, munTarget))
	if mun.ModeracioEstat != "" && mun.ModeracioEstat != "publicat" && !(canManageTerritory || canModerate) {
		http.NotFound(w, r)
		return
	}
	versionID := parseFormInt(r.URL.Query().Get("version_id"))
	var anecdote *db.MunicipiAnecdotariVersion
	if versionID > 0 && canModerate {
		if version, err := a.DB.GetMunicipiAnecdotariVersion(versionID); err == nil && version != nil && version.ItemID == itemID {
			if munIDFromVersion, err := a.DB.ResolveMunicipiIDByAnecdotariVersionID(versionID); err == nil && munIDFromVersion == munID {
				anecdote = version
			}
		}
	}
	if anecdote == nil {
		if published, err := a.DB.GetMunicipiAnecdotariPublished(itemID); err == nil && published != nil && published.MunicipiID == munID {
			anecdote = published
		} else if canModerate {
			if itemMunID, err := a.DB.ResolveMunicipiIDByAnecdotariItemID(itemID); err == nil && itemMunID == munID {
				if pending, err := a.DB.GetPendingMunicipiAnecdotariVersionByItemID(itemID); err == nil && pending != nil {
					anecdote = pending
				}
			}
		}
	}
	if anecdote == nil {
		http.NotFound(w, r)
		return
	}
	isPublished := anecdote.Status == "publicat"
	if r.Method == http.MethodPost {
		if !isPublished {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		a.municipiAnecdoteCommentPost(w, r, mun, anecdote, user)
		return
	}
	page := 1
	perPage := 20
	if val := strings.TrimSpace(r.URL.Query().Get("page")); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			page = n
		}
	}
	offset := (page - 1) * perPage
	comments := []db.MunicipiAnecdotariComment{}
	total := 0
	commentViews := []anecdoteCommentView{}
	if isPublished {
		var err error
		comments, total, err = a.DB.ListMunicipiAnecdotariComments(itemID, perPage, offset)
		if err != nil {
			Errorf("Error carregant comentaris anecdotari %d: %v", itemID, err)
			comments = []db.MunicipiAnecdotariComment{}
			total = 0
		}
		for _, c := range comments {
			commentViews = append(commentViews, anecdoteCommentView{
				Body:      strings.TrimSpace(c.Body),
				CreatedAt: formatAnecdoteDate(c.CreatedAt),
			})
		}
	}
	totalPages := 1
	if total > 0 {
		totalPages = (total + perPage - 1) / perPage
	}
	if page > totalPages {
		page = totalPages
	}
	pageBase := "/territori/municipis/" + strconv.Itoa(munID) + "/anecdotes/" + strconv.Itoa(itemID)

	canComment := isPublished && user != nil && a.HasPermission(user.ID, permKeyTerritoriMunicipisAnecdotesComment, munTarget)
	canCreateAnecdote := user != nil && a.HasPermission(user.ID, permKeyTerritoriMunicipisAnecdotesCreate, munTarget)
	token, _ := ensureCSRF(w, r)
	ok := strings.TrimSpace(r.URL.Query().Get("ok")) == "1"
	submitted := strings.TrimSpace(r.URL.Query().Get("submitted")) == "1"
	errMsg := ""
	if strings.TrimSpace(r.URL.Query().Get("err")) == "cooldown" {
		errMsg = T(ResolveLang(r), "municipi.anecdotes.comment.cooldown")
	}

	data := map[string]interface{}{
		"Municipi":         mun,
		"Anecdote":         anecdote,
		"AnecdoteDate":     formatCronologiaDisplay(anecdote.DataRef),
		"AnecdoteSnippet":  summarizeAnecdoteText(anecdote.Text, 320),
		"Comments":         commentViews,
		"Page":             page,
		"PerPage":          perPage,
		"TotalPages":       totalPages,
		"HasPrev":          page > 1,
		"HasNext":          page < totalPages,
		"PrevPage":         page - 1,
		"NextPage":         page + 1,
		"PageBase":         pageBase,
		"CanComment":       canComment,
		"CanCreateAnecdote": canCreateAnecdote,
		"CSRFToken":        token,
		"Ok":               ok,
		"Submitted":        submitted,
		"ErrorMessage":     errMsg,
	}
	if user != nil {
		RenderPrivateTemplate(w, r, "municipi-anecdotes-detail.html", data)
		return
	}
	RenderTemplate(w, r, "municipi-anecdotes-detail.html", data)
}

func (a *App) MunicipiAnecdoteNewPage(w http.ResponseWriter, r *http.Request) {
	munID := municipiIDFromPath(r.URL.Path)
	if munID <= 0 {
		http.NotFound(w, r)
		return
	}
	mun, err := a.DB.GetMunicipi(munID)
	if err != nil || mun == nil {
		http.NotFound(w, r)
		return
	}
	target := a.resolveMunicipiTarget(munID)
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisAnecdotesCreate, target)
	if !ok {
		return
	}
	token, _ := ensureCSRF(w, r)
	if r.Method == http.MethodPost {
		if !parseAnecdoteForm(w, r, "/territori/municipis/anecdotes/new") {
			return
		}
		if !validateCSRF(r, r.FormValue("csrf_token")) {
			http.Error(w, "CSRF invalid", http.StatusBadRequest)
			return
		}
		form := readAnecdoteForm(r)
		if err := validateAnecdoteValues(form.Title, form.Tag, form.DataRef, form.Text, form.FontURL); err != nil {
			a.renderAnecdoteForm(w, r, mun, form, token, err.Error(), true)
			return
		}
		if form.Submit {
			if !a.HasPermission(user.ID, permKeyTerritoriMunicipisAnecdotesSubmit, target) {
				http.Error(w, "Forbidden", http.StatusForbidden)
				return
			}
			allowed, err := a.canSubmitAnecdote(user.ID, munID)
			if err != nil {
				Errorf("Error comprovant limit anecdotari municipi %d: %v", munID, err)
				a.renderAnecdoteForm(w, r, mun, form, token, "Failed to submit proposal", true)
				return
			}
			if !allowed {
				a.renderAnecdoteForm(w, r, mun, form, token, T(ResolveLang(r), "municipi.anecdotes.form.limit"), true)
				return
			}
		}
		form.Tag = normalizeAnecdoteTag(form.Tag)
		itemID, err := a.DB.CreateMunicipiAnecdotariItem(munID, user.ID)
		if err != nil || itemID <= 0 {
			a.renderAnecdoteForm(w, r, mun, form, token, "Failed to create anecdote", true)
			return
		}
		versionID, err := a.DB.CreateMunicipiAnecdotariDraft(itemID, user.ID, false)
		if err != nil || versionID <= 0 {
			a.renderAnecdoteForm(w, r, mun, form, token, "Failed to create draft", true)
			return
		}
		draft := &db.MunicipiAnecdotariVersion{
			ID:          versionID,
			ItemID:      itemID,
			Titol:       form.Title,
			Tag:         form.Tag,
			DataRef:     form.DataRef,
			Text:        form.Text,
			FontURL:     form.FontURL,
			LockVersion: 0,
		}
		if err := a.DB.UpdateMunicipiAnecdotariDraft(draft); err != nil {
			a.renderAnecdoteForm(w, r, mun, form, token, "Failed to save draft", true)
			return
		}
		if form.Submit {
			if err := a.DB.SubmitMunicipiAnecdotariVersion(versionID); err != nil {
				a.renderAnecdoteForm(w, r, mun, form, token, "Failed to submit proposal", true)
				return
			}
			_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleMunicipiAnecdotaPublicada, "municipi_anecdota_submit", "municipi_anecdota_version", &versionID, "pendent", nil, anecdoteActivityDetails(munID))
			http.Redirect(w, r, "/territori/municipis/"+strconv.Itoa(munID)+"/anecdotes/"+strconv.Itoa(itemID)+"?submitted=1", http.StatusSeeOther)
			return
		}
		http.Redirect(w, r, "/territori/municipis/"+strconv.Itoa(munID)+"/anecdotes/"+strconv.Itoa(itemID)+"/edit?version_id="+strconv.Itoa(versionID)+"&ok=1", http.StatusSeeOther)
		return
	}
	a.renderAnecdoteForm(w, r, mun, anecdoteFormData{}, token, "", true)
}

func (a *App) MunicipiAnecdoteEditPage(w http.ResponseWriter, r *http.Request) {
	munID := municipiIDFromPath(r.URL.Path)
	itemID := anecdoteItemIDFromPath(r.URL.Path)
	if munID <= 0 || itemID <= 0 {
		http.NotFound(w, r)
		return
	}
	mun, err := a.DB.GetMunicipi(munID)
	if err != nil || mun == nil {
		http.NotFound(w, r)
		return
	}
	if itemMunID, err := a.DB.ResolveMunicipiIDByAnecdotariItemID(itemID); err != nil || itemMunID != munID {
		http.NotFound(w, r)
		return
	}
	target := a.resolveMunicipiTarget(munID)
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisAnecdotesEdit, target)
	if !ok {
		return
	}
	token, _ := ensureCSRF(w, r)
	if r.Method == http.MethodPost {
		if !parseAnecdoteForm(w, r, "/territori/municipis/anecdotes/edit") {
			return
		}
		if !validateCSRF(r, r.FormValue("csrf_token")) {
			http.Error(w, "CSRF invalid", http.StatusBadRequest)
			return
		}
		form := readAnecdoteForm(r)
		if form.VersionID <= 0 {
			http.NotFound(w, r)
			return
		}
		version, err := a.DB.GetMunicipiAnecdotariVersion(form.VersionID)
		if err != nil || version == nil || version.ItemID != itemID || version.Status != "draft" {
			http.NotFound(w, r)
			return
		}
		if !ownsDraft(user, version.CreatedBy) && !a.HasPermission(user.ID, permKeyTerritoriMunicipisAnecdotesEdit, target) {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		if err := validateAnecdoteValues(form.Title, form.Tag, form.DataRef, form.Text, form.FontURL); err != nil {
			form.LockVersion = version.LockVersion
			a.renderAnecdoteForm(w, r, mun, form, token, err.Error(), false)
			return
		}
		if form.Submit {
			if !a.HasPermission(user.ID, permKeyTerritoriMunicipisAnecdotesSubmit, target) {
				http.Error(w, "Forbidden", http.StatusForbidden)
				return
			}
			allowed, err := a.canSubmitAnecdote(user.ID, munID)
			if err != nil {
				Errorf("Error comprovant limit anecdotari municipi %d: %v", munID, err)
				a.renderAnecdoteForm(w, r, mun, form, token, "Failed to submit proposal", false)
				return
			}
			if !allowed {
				a.renderAnecdoteForm(w, r, mun, form, token, T(ResolveLang(r), "municipi.anecdotes.form.limit"), false)
				return
			}
		}
		version.Titol = form.Title
		version.Tag = normalizeAnecdoteTag(form.Tag)
		version.DataRef = form.DataRef
		version.Text = form.Text
		version.FontURL = form.FontURL
		version.LockVersion = form.LockVersion
		if err := a.DB.UpdateMunicipiAnecdotariDraft(version); err != nil {
			if err == db.ErrConflict {
				http.Redirect(w, r, "/territori/municipis/"+strconv.Itoa(munID)+"/anecdotes/"+strconv.Itoa(itemID)+"/edit?version_id="+strconv.Itoa(form.VersionID)+"&err=conflict", http.StatusSeeOther)
				return
			}
			a.renderAnecdoteForm(w, r, mun, form, token, "Failed to save draft", false)
			return
		}
		if form.Submit {
			if err := a.DB.SubmitMunicipiAnecdotariVersion(form.VersionID); err != nil {
				a.renderAnecdoteForm(w, r, mun, form, token, "Failed to submit proposal", false)
				return
			}
			versionID := form.VersionID
			_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleMunicipiAnecdotaPublicada, "municipi_anecdota_submit", "municipi_anecdota_version", &versionID, "pendent", nil, anecdoteActivityDetails(munID))
			http.Redirect(w, r, "/territori/municipis/"+strconv.Itoa(munID)+"/anecdotes/"+strconv.Itoa(itemID)+"?submitted=1", http.StatusSeeOther)
			return
		}
		http.Redirect(w, r, "/territori/municipis/"+strconv.Itoa(munID)+"/anecdotes/"+strconv.Itoa(itemID)+"/edit?version_id="+strconv.Itoa(form.VersionID)+"&ok=1", http.StatusSeeOther)
		return
	}
	versionID := parseFormInt(r.URL.Query().Get("version_id"))
	if versionID <= 0 {
		newID, err := a.DB.CreateMunicipiAnecdotariDraft(itemID, user.ID, true)
		if err != nil || newID <= 0 {
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, "/territori/municipis/"+strconv.Itoa(munID)+"/anecdotes/"+strconv.Itoa(itemID)+"/edit?version_id="+strconv.Itoa(newID), http.StatusSeeOther)
		return
	}
	version, err := a.DB.GetMunicipiAnecdotariVersion(versionID)
	if err != nil || version == nil || version.ItemID != itemID {
		http.NotFound(w, r)
		return
	}
	if version.Status != "draft" {
		http.NotFound(w, r)
		return
	}
	form := anecdoteFormData{
		VersionID:  version.ID,
		LockVersion: version.LockVersion,
		Title:      strings.TrimSpace(version.Titol),
		Tag:        strings.TrimSpace(version.Tag),
		DataRef:    strings.TrimSpace(version.DataRef),
		Text:       strings.TrimSpace(version.Text),
		FontURL:    strings.TrimSpace(version.FontURL),
	}
	a.renderAnecdoteForm(w, r, mun, form, token, "", false)
}


func (a *App) AnecdotesAPI(w http.ResponseWriter, r *http.Request) {
	base := strings.TrimPrefix(r.URL.Path, "/api/anecdotes/")
	base = strings.Trim(base, "/")
	parts := strings.Split(base, "/")
	if len(parts) < 1 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	itemID, err := strconv.Atoi(parts[0])
	if err != nil || itemID <= 0 {
		http.NotFound(w, r)
		return
	}
	if len(parts) != 2 {
		http.NotFound(w, r)
		return
	}
	switch parts[1] {
	case "draft":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		a.municipiAnecdoteDraft(w, r, itemID)
	case "comments":
		switch r.Method {
		case http.MethodGet:
			a.municipiAnecdoteCommentsList(w, r, itemID)
		case http.MethodPost:
			a.municipiAnecdoteCommentCreate(w, r, itemID)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	default:
		http.NotFound(w, r)
	}
}

func (a *App) AnecdoteVersionsAPI(w http.ResponseWriter, r *http.Request) {
	base := strings.TrimPrefix(r.URL.Path, "/api/anecdote_versions/")
	base = strings.Trim(base, "/")
	parts := strings.Split(base, "/")
	if len(parts) < 1 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	versionID, err := strconv.Atoi(parts[0])
	if err != nil || versionID <= 0 {
		http.NotFound(w, r)
		return
	}
	switch len(parts) {
	case 1:
		switch r.Method {
		case http.MethodGet:
			a.municipiAnecdoteVersionGet(w, r, versionID)
		case http.MethodPut:
			a.municipiAnecdoteVersionSave(w, r, versionID)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case 2:
		if parts[1] != "submit" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		a.municipiAnecdoteVersionSubmit(w, r, versionID)
	default:
		http.NotFound(w, r)
	}
}

func (a *App) municipiAnecdotesListJSON(w http.ResponseWriter, r *http.Request, municipiID int) {
	lang := ResolveLang(r)
	limit := 6
	if val := strings.TrimSpace(r.URL.Query().Get("limit")); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			limit = n
		}
	}
	filter := db.MunicipiAnecdotariFilter{
		Status: "publicat",
		Limit:  limit,
	}
	items, _, err := a.DB.ListMunicipiAnecdotariPublished(municipiID, filter)
	if err != nil {
		http.Error(w, "failed to load anecdotes", http.StatusInternalServerError)
		return
	}
	payload := make([]map[string]interface{}, 0, len(items))
	for _, item := range items {
		tag := strings.TrimSpace(item.Tag)
		tagLabelKey := "municipi.anecdotes.tags." + tag
		tagLabel := T(lang, tagLabelKey)
		if tagLabel == tagLabelKey {
			tagLabel = tag
		}
		payload = append(payload, map[string]interface{}{
			"item_id":  item.ItemID,
			"title":    strings.TrimSpace(item.Titol),
			"tag":      tag,
			"tag_label": tagLabel,
			"data_ref": formatCronologiaDisplay(item.DataRef),
			"snippet":  summarizeAnecdoteText(item.Text, 180),
		})
	}
	writeJSON(w, map[string]interface{}{"items": payload})
}

func (a *App) municipiAnecdoteCreateJSON(w http.ResponseWriter, r *http.Request, municipiID int) {
	target := a.resolveMunicipiTarget(municipiID)
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisAnecdotesCreate, target)
	if !ok {
		return
	}
	var payload anecdoteCreatePayload
	if err := decodeAnecdoteJSON(w, r, &payload); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	token := readCSRFToken(r, payload.CSRFToken)
	if !validateCSRF(r, token) {
		http.Error(w, "invalid csrf", http.StatusBadRequest)
		return
	}
	if err := validateAnecdoteValues(payload.Title, payload.Tag, payload.DataRef, payload.Text, payload.FontURL); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	itemID, err := a.DB.CreateMunicipiAnecdotariItem(municipiID, user.ID)
	if err != nil || itemID <= 0 {
		http.Error(w, "failed to create", http.StatusInternalServerError)
		return
	}
	versionID, err := a.DB.CreateMunicipiAnecdotariDraft(itemID, user.ID, false)
	if err != nil || versionID <= 0 {
		http.Error(w, "failed to create draft", http.StatusInternalServerError)
		return
	}
	draft := &db.MunicipiAnecdotariVersion{
		ID:          versionID,
		ItemID:      itemID,
		Titol:       strings.TrimSpace(payload.Title),
		Tag:         normalizeAnecdoteTag(payload.Tag),
		DataRef:     strings.TrimSpace(payload.DataRef),
		Text:        strings.TrimSpace(payload.Text),
		FontURL:     strings.TrimSpace(payload.FontURL),
		LockVersion: 0,
	}
	if err := a.DB.UpdateMunicipiAnecdotariDraft(draft); err != nil {
		http.Error(w, "failed to save draft", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{"item_id": itemID, "version_id": versionID})
}

func (a *App) municipiAnecdoteDraft(w http.ResponseWriter, r *http.Request, itemID int) {
	munID, err := a.DB.ResolveMunicipiIDByAnecdotariItemID(itemID)
	if err != nil || munID <= 0 {
		http.NotFound(w, r)
		return
	}
	target := a.resolveMunicipiTarget(munID)
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisAnecdotesEdit, target)
	if !ok {
		return
	}
	var payload anecdoteCommentPayload
	if err := decodeAnecdoteJSON(w, r, &payload); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	token := readCSRFToken(r, payload.CSRFToken)
	if !validateCSRF(r, token) {
		http.Error(w, "invalid csrf", http.StatusBadRequest)
		return
	}
	versionID, err := a.DB.CreateMunicipiAnecdotariDraft(itemID, user.ID, true)
	if err != nil || versionID <= 0 {
		http.Error(w, "failed to create draft", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{"version_id": versionID})
}

func (a *App) municipiAnecdoteVersionGet(w http.ResponseWriter, r *http.Request, versionID int) {
	munID, err := a.DB.ResolveMunicipiIDByAnecdotariVersionID(versionID)
	if err != nil || munID <= 0 {
		http.NotFound(w, r)
		return
	}
	target := a.resolveMunicipiTarget(munID)
	_, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisAnecdotesEdit, target)
	if !ok {
		return
	}
	version, err := a.DB.GetMunicipiAnecdotariVersion(versionID)
	if err != nil || version == nil {
		http.NotFound(w, r)
		return
	}
	payload := map[string]interface{}{
		"id":           version.ID,
		"item_id":      version.ItemID,
		"status":       version.Status,
		"lock_version": version.LockVersion,
		"titol":        strings.TrimSpace(version.Titol),
		"tag":          strings.TrimSpace(version.Tag),
		"data_ref":     strings.TrimSpace(version.DataRef),
		"text":         strings.TrimSpace(version.Text),
		"font_url":     strings.TrimSpace(version.FontURL),
	}
	writeJSON(w, payload)
}

func (a *App) municipiAnecdoteVersionSave(w http.ResponseWriter, r *http.Request, versionID int) {
	munID, err := a.DB.ResolveMunicipiIDByAnecdotariVersionID(versionID)
	if err != nil || munID <= 0 {
		http.NotFound(w, r)
		return
	}
	target := a.resolveMunicipiTarget(munID)
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisAnecdotesEdit, target)
	if !ok {
		return
	}
	var payload anecdoteSavePayload
	if err := decodeAnecdoteJSON(w, r, &payload); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	token := readCSRFToken(r, payload.CSRFToken)
	if !validateCSRF(r, token) {
		http.Error(w, "invalid csrf", http.StatusBadRequest)
		return
	}
	version, err := a.DB.GetMunicipiAnecdotariVersion(versionID)
	if err != nil || version == nil {
		http.NotFound(w, r)
		return
	}
	if version.Status != "draft" {
		http.Error(w, "invalid status", http.StatusBadRequest)
		return
	}
	if !ownsDraft(user, version.CreatedBy) && !a.HasPermission(user.ID, permKeyTerritoriMunicipisAnecdotesEdit, target) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if err := validateAnecdoteValues(payload.Title, payload.Tag, payload.DataRef, payload.Text, payload.FontURL); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	version.Titol = strings.TrimSpace(payload.Title)
	version.Tag = normalizeAnecdoteTag(payload.Tag)
	version.DataRef = strings.TrimSpace(payload.DataRef)
	version.Text = strings.TrimSpace(payload.Text)
	version.FontURL = strings.TrimSpace(payload.FontURL)
	version.LockVersion = payload.LockVersion
	if err := a.DB.UpdateMunicipiAnecdotariDraft(version); err != nil {
		if err == db.ErrConflict {
			http.Error(w, "conflict", http.StatusConflict)
			return
		}
		http.Error(w, "failed to save", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{"ok": true})
}

func (a *App) municipiAnecdoteVersionSubmit(w http.ResponseWriter, r *http.Request, versionID int) {
	munID, err := a.DB.ResolveMunicipiIDByAnecdotariVersionID(versionID)
	if err != nil || munID <= 0 {
		http.NotFound(w, r)
		return
	}
	target := a.resolveMunicipiTarget(munID)
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisAnecdotesSubmit, target)
	if !ok {
		return
	}
	var payload anecdoteCommentPayload
	if err := decodeAnecdoteJSON(w, r, &payload); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	token := readCSRFToken(r, payload.CSRFToken)
	if !validateCSRF(r, token) {
		http.Error(w, "invalid csrf", http.StatusBadRequest)
		return
	}
	if !allowRouteLimit(r, "/api/anecdote_versions/submit", municipiAnecdoteRateLimit, municipiAnecdoteRateBurst) {
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}
	version, err := a.DB.GetMunicipiAnecdotariVersion(versionID)
	if err != nil || version == nil {
		http.NotFound(w, r)
		return
	}
	if version.Status != "draft" {
		http.Error(w, "invalid status", http.StatusBadRequest)
		return
	}
	if !ownsDraft(user, version.CreatedBy) && !a.HasPermission(user.ID, permKeyTerritoriMunicipisAnecdotesEdit, target) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if err := validateAnecdoteValues(version.Titol, version.Tag, version.DataRef, version.Text, version.FontURL); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	allowed, err := a.canSubmitAnecdote(user.ID, munID)
	if err != nil {
		Errorf("Error comprovant limit anecdotari municipi %d: %v", munID, err)
		http.Error(w, "failed to submit", http.StatusInternalServerError)
		return
	}
	if !allowed {
		http.Error(w, "submit limit reached", http.StatusTooManyRequests)
		return
	}
	if err := a.DB.SubmitMunicipiAnecdotariVersion(versionID); err != nil {
		http.Error(w, "failed to submit", http.StatusInternalServerError)
		return
	}
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleMunicipiAnecdotaPublicada, "municipi_anecdota_submit", "municipi_anecdota_version", &versionID, "pendent", nil, anecdoteActivityDetails(munID))
	writeJSON(w, map[string]interface{}{"ok": true})
}

func (a *App) municipiAnecdoteCommentsList(w http.ResponseWriter, r *http.Request, itemID int) {
	if _, err := a.DB.GetMunicipiAnecdotariPublished(itemID); err != nil {
		http.NotFound(w, r)
		return
	}
	page := 1
	perPage := 20
	if val := strings.TrimSpace(r.URL.Query().Get("page")); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			page = n
		}
	}
	if val := strings.TrimSpace(r.URL.Query().Get("per_page")); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			perPage = n
		}
	}
	offset := (page - 1) * perPage
	comments, total, err := a.DB.ListMunicipiAnecdotariComments(itemID, perPage, offset)
	if err != nil {
		http.Error(w, "failed to load comments", http.StatusInternalServerError)
		return
	}
	payload := make([]map[string]interface{}, 0, len(comments))
	for _, c := range comments {
		payload = append(payload, map[string]interface{}{
			"body":       strings.TrimSpace(c.Body),
			"created_at": formatAnecdoteDate(c.CreatedAt),
		})
	}
	writeJSON(w, map[string]interface{}{"items": payload, "total": total})
}

func (a *App) municipiAnecdoteCommentCreate(w http.ResponseWriter, r *http.Request, itemID int) {
	published, err := a.DB.GetMunicipiAnecdotariPublished(itemID)
	if err != nil || published == nil {
		http.NotFound(w, r)
		return
	}
	munID := published.MunicipiID
	target := a.resolveMunicipiTarget(munID)
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisAnecdotesComment, target)
	if !ok {
		return
	}
	var payload anecdoteCommentPayload
	if err := decodeAnecdoteJSON(w, r, &payload); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	token := readCSRFToken(r, payload.CSRFToken)
	if !validateCSRF(r, token) {
		http.Error(w, "invalid csrf", http.StatusBadRequest)
		return
	}
	if !allowRouteLimit(r, "/api/anecdotes/comments", municipiAnecdoteRateLimit, municipiAnecdoteRateBurst) {
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}
	allowed, err := a.canPostAnecdoteComment(user.ID)
	if err != nil {
		Errorf("Error comprovant cooldown comentari anecdotari %d: %v", itemID, err)
		http.Error(w, "failed to create comment", http.StatusInternalServerError)
		return
	}
	if !allowed {
		http.Error(w, "comment cooldown", http.StatusTooManyRequests)
		return
	}
	if err := validateAnecdoteComment(payload.Body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := a.DB.CreateMunicipiAnecdotariComment(itemID, user.ID, payload.Body); err != nil {
		http.Error(w, "failed to create comment", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{"ok": true})
}

func (a *App) municipiAnecdoteCommentPost(w http.ResponseWriter, r *http.Request, mun *db.Municipi, anecdote *db.MunicipiAnecdotariVersion, user *db.User) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !parseAnecdoteForm(w, r, "/territori/municipis/anecdotes/comment") {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	target := a.resolveMunicipiTarget(mun.ID)
	if user == nil || !a.HasPermission(user.ID, permKeyTerritoriMunicipisAnecdotesComment, target) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if anecdote.Status != "publicat" {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	allowed, err := a.canPostAnecdoteComment(user.ID)
	if err != nil {
		http.Error(w, "Failed to save comment", http.StatusInternalServerError)
		return
	}
	if !allowed {
		http.Redirect(w, r, "/territori/municipis/"+strconv.Itoa(mun.ID)+"/anecdotes/"+strconv.Itoa(anecdote.ItemID)+"?err=cooldown", http.StatusSeeOther)
		return
	}
	body := strings.TrimSpace(r.FormValue("comment_body"))
	if err := validateAnecdoteComment(body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := a.DB.CreateMunicipiAnecdotariComment(anecdote.ItemID, user.ID, body); err != nil {
		http.Error(w, "Failed to save comment", http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/territori/municipis/"+strconv.Itoa(mun.ID)+"/anecdotes/"+strconv.Itoa(anecdote.ItemID)+"?ok=1", http.StatusSeeOther)
}

type anecdoteFormData struct {
	VersionID   int
	LockVersion int
	Title       string
	Tag         string
	DataRef     string
	Text        string
	FontURL     string
	Submit      bool
}

func readAnecdoteForm(r *http.Request) anecdoteFormData {
	return anecdoteFormData{
		VersionID:   parseFormInt(r.FormValue("version_id")),
		LockVersion: parseFormInt(r.FormValue("lock_version")),
		Title:       strings.TrimSpace(r.FormValue("titol")),
		Tag:         strings.TrimSpace(r.FormValue("tag")),
		DataRef:     strings.TrimSpace(r.FormValue("data_ref")),
		Text:        strings.TrimSpace(r.FormValue("text")),
		FontURL:     strings.TrimSpace(r.FormValue("font_url")),
		Submit:      r.FormValue("action") == "submit",
	}
}

func (a *App) renderAnecdoteForm(w http.ResponseWriter, r *http.Request, mun *db.Municipi, form anecdoteFormData, token string, errMsg string, isNew bool) {
	data := map[string]interface{}{
		"Municipi":       mun,
		"Form":           form,
		"TagOptions":     anecdoteTagOptions(ResolveLang(r)),
		"CSRFToken":      token,
		"ErrorMessage":   errMsg,
		"IsNew":          isNew,
		"IsEdit":         !isNew,
		"Submitted":      strings.TrimSpace(r.URL.Query().Get("submitted")) == "1",
		"Ok":             strings.TrimSpace(r.URL.Query().Get("ok")) == "1",
		"Conflict":       strings.TrimSpace(r.URL.Query().Get("err")) == "conflict",
	}
	RenderPrivateTemplate(w, r, "municipi-anecdotes-form.html", data)
}

func decodeAnecdoteJSON(w http.ResponseWriter, r *http.Request, payload interface{}) error {
	r.Body = http.MaxBytesReader(w, r.Body, municipiAnecdoteMaxBytes)
	dec := json.NewDecoder(r.Body)
	return dec.Decode(payload)
}

func parseAnecdoteForm(w http.ResponseWriter, r *http.Request, route string) bool {
	if !allowRouteLimit(r, route, municipiAnecdoteRateLimit, municipiAnecdoteRateBurst) {
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return false
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return false
	}
	return true
}

func anecdoteActivityDetails(munID int) string {
	return fmt.Sprintf("municipi:%d", munID)
}

func (a *App) canSubmitAnecdote(userID, municipiID int) (bool, error) {
	if userID <= 0 || municipiID <= 0 {
		return false, errors.New("invalid ids")
	}
	from := time.Now().Add(-municipiAnecdoteSubmitWindow)
	acts, err := a.DB.ListUserActivityByUser(userID, db.ActivityFilter{
		ObjectType: "municipi_anecdota_version",
		From:       from,
	})
	if err != nil {
		return false, err
	}
	targetDetail := anecdoteActivityDetails(municipiID)
	count := 0
	for _, act := range acts {
		if act.Details != targetDetail {
			continue
		}
		count++
		if count >= municipiAnecdoteSubmitDailyLimit {
			return false, nil
		}
	}
	return true, nil
}

func (a *App) canPostAnecdoteComment(userID int) (bool, error) {
	if userID <= 0 {
		return false, errors.New("user_id invalid")
	}
	last, err := a.DB.GetMunicipiAnecdotariLastCommentAt(userID)
	if err != nil {
		return false, err
	}
	if last.IsZero() {
		return true, nil
	}
	if time.Since(last) < municipiAnecdoteCommentCooldown {
		return false, nil
	}
	return true, nil
}

func anecdoteTagOptions(lang string) []anecdoteFormTagOption {
	options := make([]anecdoteFormTagOption, 0, len(anecdoteTags))
	for _, tag := range anecdoteTags {
		labelKey := "municipi.anecdotes.tags." + tag
		label := T(lang, labelKey)
		if label == labelKey {
			label = tag
		}
		options = append(options, anecdoteFormTagOption{Value: tag, Label: label})
	}
	return options
}

func normalizeAnecdoteTag(value string) string {
	val := strings.ToLower(strings.TrimSpace(value))
	if val == "" {
		return ""
	}
	for _, tag := range anecdoteTags {
		if val == tag {
			return tag
		}
	}
	return ""
}

func validateAnecdoteValues(title, tag, dataRef, text, fontURL string) error {
	title = strings.TrimSpace(title)
	text = strings.TrimSpace(text)
	tag = normalizeAnecdoteTag(tag)
	dataRef = strings.TrimSpace(dataRef)
	fontURL = strings.TrimSpace(fontURL)
	if utf8.RuneCountInString(title) < anecdoteTitleMin {
		return errors.New("title too short")
	}
	if utf8.RuneCountInString(title) > anecdoteTitleMax {
		return errors.New("title too long")
	}
	if utf8.RuneCountInString(text) < anecdoteTextMin {
		return errors.New("text too short")
	}
	if utf8.RuneCountInString(text) > anecdoteTextMax {
		return errors.New("text too long")
	}
	if tag == "" || utf8.RuneCountInString(tag) > anecdoteTagMaxLen {
		return errors.New("invalid tag")
	}
	if dataRef != "" {
		if utf8.RuneCountInString(dataRef) > anecdoteDataRefMax || !anecdoteDataRefPattern.MatchString(dataRef) {
			return errors.New("invalid data_ref")
		}
	}
	if fontURL != "" {
		if utf8.RuneCountInString(fontURL) > anecdoteFontURLMax {
			return errors.New("font_url too long")
		}
		if !isValidHistoriaURL(fontURL) {
			return errors.New("invalid font_url")
		}
	}
	return nil
}

func validateAnecdoteComment(body string) error {
	body = strings.TrimSpace(body)
	if utf8.RuneCountInString(body) < anecdoteCommentMin {
		return errors.New("comment too short")
	}
	if utf8.RuneCountInString(body) > anecdoteCommentMax {
		return errors.New("comment too long")
	}
	return nil
}

func summarizeAnecdoteText(text string, max int) string {
	clean := strings.TrimSpace(text)
	if clean == "" || max <= 0 {
		return ""
	}
	clean = strings.Join(strings.Fields(clean), " ")
	runes := []rune(clean)
	if len(runes) <= max {
		return clean
	}
	if max <= 3 {
		return string(runes[:max])
	}
	trimmed := strings.TrimSpace(string(runes[:max-3]))
	if trimmed == "" {
		return ""
	}
	return trimmed + "..."
}

func formatAnecdoteDate(val sql.NullTime) string {
	if !val.Valid {
		return ""
	}
	return val.Time.Format("2006-01-02 15:04")
}

func anecdoteItemIDFromPath(path string) int {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i := 0; i < len(parts)-1; i++ {
		if parts[i] == "anecdotes" {
			if id, err := strconv.Atoi(parts[i+1]); err == nil {
				return id
			}
		}
	}
	return extractID(path)
}
