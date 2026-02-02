package core

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const importTemplateMaxBytes = 2 << 20

type importTemplateListEntry struct {
	ID          int
	Name        string
	Description string
	Visibility  string
	IsPublic    bool
	CanEdit     bool
	CanDelete   bool
	CanClone    bool
	CanToggle   bool
	UpdatedAt   string
	OwnerID     int
}

type importTemplateEditorView struct {
	Template *db.CSVImportTemplate
	Model    interface{}
	IsNew    bool
	Error    string
}

type importTemplatePayload struct {
	Name             string `json:"name"`
	Description      string `json:"description"`
	Visibility       string `json:"visibility"`
	DefaultSeparator string `json:"default_separator"`
	ModelJSON        string `json:"model_json"`
	CSRFToken        string `json:"csrf_token"`
}

func (a *App) ImportTemplatesRoute(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	base := strings.TrimPrefix(r.URL.Path, "/importador/plantilles")
	base = strings.Trim(base, "/")
	if base == "" {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		a.importTemplatesListPage(w, r, user)
		return
	}
	if base == "nova" {
		switch r.Method {
		case http.MethodGet:
			a.importTemplateNewForm(w, r, user)
		case http.MethodPost:
			a.importTemplateCreate(w, r, user)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}
	parts := strings.Split(base, "/")
	if len(parts) < 1 {
		http.NotFound(w, r)
		return
	}
	id, err := strconv.Atoi(parts[0])
	if err != nil || id <= 0 {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 1 {
		http.NotFound(w, r)
		return
	}
	switch parts[1] {
	case "export.csv":
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		a.importTemplateExportCSV(w, r, user, id)
	case "editar":
		switch r.Method {
		case http.MethodGet:
			a.importTemplateEditForm(w, r, user, id)
		case http.MethodPost:
			a.importTemplateUpdate(w, r, user, id)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "delete":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		a.importTemplateDelete(w, r, user, id)
	case "clone":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		a.importTemplateClone(w, r, user, id)
	case "export.xlsx":
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		a.importTemplateExportXLSX(w, r, user, id)
	case "toggle-visibility":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		a.importTemplateToggleVisibility(w, r, user, id)
	default:
		http.NotFound(w, r)
	}
}

func (a *App) ImportTemplatesAPI(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	base := strings.TrimPrefix(r.URL.Path, "/api/import-templates")
	base = strings.Trim(base, "/")
	if base == "" {
		switch r.Method {
		case http.MethodGet:
			a.importTemplatesListJSON(w, r, user)
		case http.MethodPost:
			a.importTemplateCreateJSON(w, r, user)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}
	if base == "similar" {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		a.importTemplatesSimilarJSON(w, r, user)
		return
	}
	parts := strings.Split(base, "/")
	if len(parts) < 1 {
		http.NotFound(w, r)
		return
	}
	id, err := strconv.Atoi(parts[0])
	if err != nil || id <= 0 {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			a.importTemplateGetJSON(w, r, user, id)
		case http.MethodPut:
			a.importTemplateUpdateJSON(w, r, user, id)
		case http.MethodDelete:
			a.importTemplateDeleteJSON(w, r, user, id)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}
	if len(parts) == 2 {
		switch parts[1] {
		case "clone":
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			a.importTemplateCloneJSON(w, r, user, id)
		case "toggle-visibility":
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			a.importTemplateToggleVisibilityJSON(w, r, user, id)
		default:
			http.NotFound(w, r)
		}
		return
	}
	http.NotFound(w, r)
}

func (a *App) importTemplatesListPage(w http.ResponseWriter, r *http.Request, user *db.User) {
	scope := strings.TrimSpace(r.URL.Query().Get("scope"))
	if scope != "public" {
		scope = "mine"
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	filter := db.CSVImportTemplateFilter{
		Query: query,
		Limit: 100,
	}
	if scope == "public" {
		filter.IncludePublic = true
	} else {
		filter.OwnerUserID = user.ID
	}
	rows, err := a.DB.ListCSVImportTemplates(filter)
	if err != nil {
		http.Error(w, "failed to load templates", http.StatusInternalServerError)
		return
	}
	perms, _ := a.permissionsFromContext(r)
	items := a.buildImportTemplateEntries(rows, user, perms)
	if scope == "public" {
		items = filterPublicTemplateEntries(items, user.ID)
	}
	RenderPrivateTemplate(w, r, "import-templates-list.html", map[string]interface{}{
		"Templates":  items,
		"ActiveScope": scope,
		"Query":      query,
	})
}

func (a *App) importTemplateNewForm(w http.ResponseWriter, r *http.Request, user *db.User) {
	lang := ResolveLang(r)
	model := defaultImportTemplateModel(lang)
	modelJSON := encodeTemplateModel(model)
	template := &db.CSVImportTemplate{
		Name:             "",
		Description:      "",
		Visibility:       "private",
		DefaultSeparator: ",",
		ModelJSON:        modelJSON,
	}
	view := importTemplateEditorView{
		Template: template,
		Model:    model,
		IsNew:    true,
	}
	RenderPrivateTemplate(w, r, "import-template-editor.html", map[string]interface{}{
		"View": view,
	})
}

func (a *App) importTemplateCreate(w http.ResponseWriter, r *http.Request, user *db.User) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	formTemplate, model, err := buildImportTemplateFromForm(r, ResolveLang(r))
	if err != nil {
		view := importTemplateEditorView{
			Template: formTemplate,
			Model:    model,
			IsNew:    true,
			Error:    err.Error(),
		}
		RenderPrivateTemplate(w, r, "import-template-editor.html", map[string]interface{}{
			"View": view,
		})
		return
	}
	formTemplate.OwnerUserID = sql.NullInt64{Int64: int64(user.ID), Valid: true}
	newID, err := a.DB.CreateCSVImportTemplate(formTemplate)
	if err != nil || newID <= 0 {
		view := importTemplateEditorView{
			Template: formTemplate,
			Model:    model,
			IsNew:    true,
			Error:    "No s'ha pogut crear la plantilla",
		}
		RenderPrivateTemplate(w, r, "import-template-editor.html", map[string]interface{}{
			"View": view,
		})
		return
	}
	http.Redirect(w, r, "/importador/plantilles/"+strconv.Itoa(newID)+"/editar", http.StatusSeeOther)
}

func (a *App) importTemplateEditForm(w http.ResponseWriter, r *http.Request, user *db.User, id int) {
	template, err := a.DB.GetCSVImportTemplate(id)
	if err != nil || template == nil {
		http.NotFound(w, r)
		return
	}
	perms, _ := a.permissionsFromContext(r)
	if !canEditImportTemplate(user, perms, template) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	model := parseTemplateModel(template.ModelJSON)
	view := importTemplateEditorView{
		Template: template,
		Model:    model,
		IsNew:    false,
	}
	RenderPrivateTemplate(w, r, "import-template-editor.html", map[string]interface{}{
		"View": view,
	})
}

func (a *App) importTemplateUpdate(w http.ResponseWriter, r *http.Request, user *db.User, id int) {
	template, err := a.DB.GetCSVImportTemplate(id)
	if err != nil || template == nil {
		http.NotFound(w, r)
		return
	}
	perms, _ := a.permissionsFromContext(r)
	if !canEditImportTemplate(user, perms, template) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	formTemplate, model, err := buildImportTemplateFromForm(r, ResolveLang(r))
	if err != nil {
		formTemplate.ID = template.ID
		formTemplate.OwnerUserID = template.OwnerUserID
		view := importTemplateEditorView{
			Template: formTemplate,
			Model:    model,
			IsNew:    false,
			Error:    err.Error(),
		}
		RenderPrivateTemplate(w, r, "import-template-editor.html", map[string]interface{}{
			"View": view,
		})
		return
	}
	formTemplate.ID = template.ID
	formTemplate.OwnerUserID = template.OwnerUserID
	if err := a.DB.UpdateCSVImportTemplate(formTemplate); err != nil {
		view := importTemplateEditorView{
			Template: formTemplate,
			Model:    model,
			IsNew:    false,
			Error:    "No s'ha pogut desar la plantilla",
		}
		RenderPrivateTemplate(w, r, "import-template-editor.html", map[string]interface{}{
			"View": view,
		})
		return
	}
	http.Redirect(w, r, "/importador/plantilles/"+strconv.Itoa(id)+"/editar?saved=1", http.StatusSeeOther)
}

func (a *App) importTemplateDelete(w http.ResponseWriter, r *http.Request, user *db.User, id int) {
	template, err := a.DB.GetCSVImportTemplate(id)
	if err != nil || template == nil {
		http.NotFound(w, r)
		return
	}
	perms, _ := a.permissionsFromContext(r)
	if !canEditImportTemplate(user, perms, template) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	if err := a.DB.DeleteCSVImportTemplate(id); err != nil {
		http.Error(w, "failed to delete", http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/importador/plantilles?scope=mine", http.StatusSeeOther)
}

func (a *App) importTemplateClone(w http.ResponseWriter, r *http.Request, user *db.User, id int) {
	template, err := a.DB.GetCSVImportTemplate(id)
	if err != nil || template == nil {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	perms, _ := a.permissionsFromContext(r)
	if !canViewImportTemplate(user, perms, template) || template.Visibility != "public" {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	cloneName := a.uniqueCloneName(user.ID, template.Name)
	cloned := &db.CSVImportTemplate{
		Name:             cloneName,
		Description:      template.Description,
		OwnerUserID:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
		Visibility:       "private",
		DefaultSeparator: template.DefaultSeparator,
		ModelJSON:        template.ModelJSON,
	}
	newID, err := a.DB.CreateCSVImportTemplate(cloned)
	if err != nil || newID <= 0 {
		http.Error(w, "failed to clone", http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/importador/plantilles/"+strconv.Itoa(newID)+"/editar", http.StatusSeeOther)
}

func (a *App) importTemplateToggleVisibility(w http.ResponseWriter, r *http.Request, user *db.User, id int) {
	template, err := a.DB.GetCSVImportTemplate(id)
	if err != nil || template == nil {
		http.NotFound(w, r)
		return
	}
	perms, _ := a.permissionsFromContext(r)
	if !canEditImportTemplate(user, perms, template) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(strings.ToLower(template.Visibility)) == "public" {
		template.Visibility = "private"
	} else {
		template.Visibility = "public"
	}
	if err := a.DB.UpdateCSVImportTemplate(template); err != nil {
		http.Error(w, "failed to update", http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/importador/plantilles?scope=mine", http.StatusSeeOther)
}

func (a *App) importTemplatesListJSON(w http.ResponseWriter, r *http.Request, user *db.User) {
	scope := strings.TrimSpace(r.URL.Query().Get("scope"))
	if scope != "public" {
		scope = "mine"
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	limit := parseFormInt(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	filter := db.CSVImportTemplateFilter{
		Query: query,
		Limit: limit,
	}
	if scope == "public" {
		filter.IncludePublic = true
	} else {
		filter.OwnerUserID = user.ID
	}
	rows, err := a.DB.ListCSVImportTemplates(filter)
	if err != nil {
		http.Error(w, "failed to load templates", http.StatusInternalServerError)
		return
	}
	perms, _ := a.permissionsFromContext(r)
	items := a.buildImportTemplateEntries(rows, user, perms)
	if scope == "public" {
		items = filterPublicTemplateEntries(items, user.ID)
	}
	payload := make([]map[string]interface{}, 0, len(items))
	for _, item := range items {
		payload = append(payload, map[string]interface{}{
			"id":           item.ID,
			"name":         item.Name,
			"description":  item.Description,
			"visibility":   item.Visibility,
			"is_public":    item.IsPublic,
			"updated_at":   item.UpdatedAt,
			"can_edit":     item.CanEdit,
			"can_delete":   item.CanDelete,
			"can_clone":    item.CanClone,
			"can_toggle":   item.CanToggle,
		})
	}
	writeJSON(w, map[string]interface{}{"items": payload})
}

func (a *App) importTemplateGetJSON(w http.ResponseWriter, r *http.Request, user *db.User, id int) {
	template, err := a.DB.GetCSVImportTemplate(id)
	if err != nil || template == nil {
		http.NotFound(w, r)
		return
	}
	perms, _ := a.permissionsFromContext(r)
	if !canViewImportTemplate(user, perms, template) {
		http.NotFound(w, r)
		return
	}
	ownerID := nullIntToPtr(template.OwnerUserID)
	writeJSON(w, map[string]interface{}{
		"id":                template.ID,
		"name":              template.Name,
		"description":       template.Description,
		"owner_user_id":     ownerID,
		"visibility":        template.Visibility,
		"default_separator": template.DefaultSeparator,
		"model_json":        template.ModelJSON,
		"signature":         template.Signature,
	})
}

func (a *App) importTemplateCreateJSON(w http.ResponseWriter, r *http.Request, user *db.User) {
	payload, _, csrfToken, err := decodeImportTemplatePayload(w, r, ResolveLang(r))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	token := readCSRFToken(r, csrfToken)
	if !validateCSRF(r, token) {
		http.Error(w, "invalid csrf", http.StatusBadRequest)
		return
	}
	payload.OwnerUserID = sql.NullInt64{Int64: int64(user.ID), Valid: true}
	newID, err := a.DB.CreateCSVImportTemplate(payload)
	if err != nil || newID <= 0 {
		http.Error(w, "failed to create", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{"id": newID})
}

func (a *App) importTemplateUpdateJSON(w http.ResponseWriter, r *http.Request, user *db.User, id int) {
	template, err := a.DB.GetCSVImportTemplate(id)
	if err != nil || template == nil {
		http.NotFound(w, r)
		return
	}
	perms, _ := a.permissionsFromContext(r)
	if !canEditImportTemplate(user, perms, template) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	payload, _, csrfToken, err := decodeImportTemplatePayload(w, r, ResolveLang(r))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	token := readCSRFToken(r, csrfToken)
	if !validateCSRF(r, token) {
		http.Error(w, "invalid csrf", http.StatusBadRequest)
		return
	}
	payload.ID = id
	payload.OwnerUserID = template.OwnerUserID
	if err := a.DB.UpdateCSVImportTemplate(payload); err != nil {
		http.Error(w, "failed to update", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{"ok": true})
}

func (a *App) importTemplateDeleteJSON(w http.ResponseWriter, r *http.Request, user *db.User, id int) {
	template, err := a.DB.GetCSVImportTemplate(id)
	if err != nil || template == nil {
		http.NotFound(w, r)
		return
	}
	perms, _ := a.permissionsFromContext(r)
	if !canEditImportTemplate(user, perms, template) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	token := readCSRFToken(r, "")
	if !validateCSRF(r, token) {
		http.Error(w, "invalid csrf", http.StatusBadRequest)
		return
	}
	if err := a.DB.DeleteCSVImportTemplate(id); err != nil {
		http.Error(w, "failed to delete", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{"ok": true})
}

func (a *App) importTemplateCloneJSON(w http.ResponseWriter, r *http.Request, user *db.User, id int) {
	template, err := a.DB.GetCSVImportTemplate(id)
	if err != nil || template == nil {
		http.NotFound(w, r)
		return
	}
	perms, _ := a.permissionsFromContext(r)
	if !canViewImportTemplate(user, perms, template) || template.Visibility != "public" {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	token := readCSRFToken(r, "")
	if !validateCSRF(r, token) {
		http.Error(w, "invalid csrf", http.StatusBadRequest)
		return
	}
	cloneName := a.uniqueCloneName(user.ID, template.Name)
	cloned := &db.CSVImportTemplate{
		Name:             cloneName,
		Description:      template.Description,
		OwnerUserID:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
		Visibility:       "private",
		DefaultSeparator: template.DefaultSeparator,
		ModelJSON:        template.ModelJSON,
	}
	newID, err := a.DB.CreateCSVImportTemplate(cloned)
	if err != nil || newID <= 0 {
		http.Error(w, "failed to clone", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{"id": newID})
}

func (a *App) importTemplateToggleVisibilityJSON(w http.ResponseWriter, r *http.Request, user *db.User, id int) {
	template, err := a.DB.GetCSVImportTemplate(id)
	if err != nil || template == nil {
		http.NotFound(w, r)
		return
	}
	perms, _ := a.permissionsFromContext(r)
	if !canEditImportTemplate(user, perms, template) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	token := readCSRFToken(r, "")
	if !validateCSRF(r, token) {
		http.Error(w, "invalid csrf", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(strings.ToLower(template.Visibility)) == "public" {
		template.Visibility = "private"
	} else {
		template.Visibility = "public"
	}
	if err := a.DB.UpdateCSVImportTemplate(template); err != nil {
		http.Error(w, "failed to update", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{"ok": true, "visibility": template.Visibility})
}

func buildImportTemplateFromForm(r *http.Request, lang string) (*db.CSVImportTemplate, interface{}, error) {
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		return &db.CSVImportTemplate{
			Name:             name,
			Description:      strings.TrimSpace(r.FormValue("description")),
			Visibility:       normalizeTemplateVisibility(r.FormValue("visibility")),
			DefaultSeparator: normalizeTemplateSeparator(r.FormValue("default_separator")),
			ModelJSON:        strings.TrimSpace(r.FormValue("model_json")),
		}, nil, fmt.Errorf("El nom és obligatori")
	}
	modelJSON := strings.TrimSpace(r.FormValue("model_json"))
	if modelJSON == "" {
		return &db.CSVImportTemplate{
			Name:             name,
			Description:      strings.TrimSpace(r.FormValue("description")),
			Visibility:       normalizeTemplateVisibility(r.FormValue("visibility")),
			DefaultSeparator: normalizeTemplateSeparator(r.FormValue("default_separator")),
			ModelJSON:        modelJSON,
		}, nil, fmt.Errorf("El model és obligatori")
	}
	model := parseTemplateModel(modelJSON)
	if model == nil {
		return &db.CSVImportTemplate{
			Name:             name,
			Description:      strings.TrimSpace(r.FormValue("description")),
			Visibility:       normalizeTemplateVisibility(r.FormValue("visibility")),
			DefaultSeparator: normalizeTemplateSeparator(r.FormValue("default_separator")),
			ModelJSON:        modelJSON,
		}, nil, fmt.Errorf("Model JSON invàlid")
	}
	parsedModel, err := parseTemplateImportModel(modelJSON)
	if err != nil {
		return &db.CSVImportTemplate{
			Name:             name,
			Description:      strings.TrimSpace(r.FormValue("description")),
			Visibility:       normalizeTemplateVisibility(r.FormValue("visibility")),
			DefaultSeparator: normalizeTemplateSeparator(r.FormValue("default_separator")),
			ModelJSON:        modelJSON,
		}, model, fmt.Errorf("Model JSON invàlid")
	}
	if err := validateTemplateImportModel(parsedModel); err != nil {
		return &db.CSVImportTemplate{
			Name:             name,
			Description:      strings.TrimSpace(r.FormValue("description")),
			Visibility:       normalizeTemplateVisibility(r.FormValue("visibility")),
			DefaultSeparator: normalizeTemplateSeparator(r.FormValue("default_separator")),
			ModelJSON:        modelJSON,
		}, model, err
	}
	return &db.CSVImportTemplate{
		Name:             name,
		Description:      strings.TrimSpace(r.FormValue("description")),
		Visibility:       normalizeTemplateVisibility(r.FormValue("visibility")),
		DefaultSeparator: normalizeTemplateSeparator(r.FormValue("default_separator")),
		ModelJSON:        modelJSON,
	}, model, nil
}

func decodeImportTemplatePayload(w http.ResponseWriter, r *http.Request, lang string) (*db.CSVImportTemplate, interface{}, string, error) {
	r.Body = http.MaxBytesReader(w, r.Body, importTemplateMaxBytes)
	dec := json.NewDecoder(r.Body)
	var payload importTemplatePayload
	if err := dec.Decode(&payload); err != nil {
		return nil, nil, "", fmt.Errorf("invalid payload")
	}
	name := strings.TrimSpace(payload.Name)
	if name == "" {
		return nil, nil, "", fmt.Errorf("name required")
	}
	modelJSON := strings.TrimSpace(payload.ModelJSON)
	if modelJSON == "" {
		return nil, nil, "", fmt.Errorf("model required")
	}
	model := parseTemplateModel(modelJSON)
	if model == nil {
		return nil, nil, "", fmt.Errorf("invalid model")
	}
	parsedModel, err := parseTemplateImportModel(modelJSON)
	if err != nil {
		return nil, nil, "", fmt.Errorf("invalid model")
	}
	if err := validateTemplateImportModel(parsedModel); err != nil {
		return nil, nil, "", err
	}
	return &db.CSVImportTemplate{
		Name:             name,
		Description:      strings.TrimSpace(payload.Description),
		Visibility:       normalizeTemplateVisibility(payload.Visibility),
		DefaultSeparator: normalizeTemplateSeparator(payload.DefaultSeparator),
		ModelJSON:        modelJSON,
		Signature:        "",
		OwnerUserID:      sql.NullInt64{},
	}, model, payload.CSRFToken, nil
}

func parseTemplateModel(modelJSON string) interface{} {
	if strings.TrimSpace(modelJSON) == "" {
		return nil
	}
	var model interface{}
	if err := json.Unmarshal([]byte(modelJSON), &model); err != nil {
		return nil
	}
	return normalizeTemplateModel(model)
}

func normalizeTemplateModel(model interface{}) interface{} {
	root, ok := model.(map[string]interface{})
	if !ok {
		return model
	}
	defaults := defaultImportTemplateModel("")
	meta := ensureMap(root, "metadata")
	if _, ok := meta["version"]; !ok {
		meta["version"] = 1
	}
	if _, ok := meta["kind"]; !ok {
		meta["kind"] = "transcripcions_raw"
	}
	if _, ok := root["mapping"]; !ok {
		root["mapping"] = map[string]interface{}{"columns": []interface{}{}}
	}
	if _, ok := root["policies"]; !ok {
		root["policies"] = defaults["policies"]
	}
	if _, ok := root["book_resolution"]; !ok {
		root["book_resolution"] = defaults["book_resolution"]
	}
	return root
}

func ensureMap(root map[string]interface{}, key string) map[string]interface{} {
	val, ok := root[key]
	if !ok {
		empty := map[string]interface{}{}
		root[key] = empty
		return empty
	}
	m, ok := val.(map[string]interface{})
	if !ok {
		empty := map[string]interface{}{}
		root[key] = empty
		return empty
	}
	return m
}

func defaultImportTemplateModel(lang string) map[string]interface{} {
	locale := strings.TrimSpace(lang)
	if locale == "" {
		locale = "ca"
	}
	return map[string]interface{}{
		"metadata": map[string]interface{}{
			"version": 1,
			"kind":    "transcripcions_raw",
			"locale":  locale,
			"record_type": "baptisme",
		},
		"book_resolution": map[string]interface{}{
			"mode":                 "llibre_id",
			"column":               "llibre_id",
			"cronologia_normalize": false,
			"ambiguity_policy":     "fail",
			"scope_filters":        true,
		},
		"mapping": map[string]interface{}{
			"columns": []interface{}{},
		},
		"policies": map[string]interface{}{
			"moderation_status": "pendent",
			"dedup": map[string]interface{}{
				"within_file": true,
				"key_fields":  []interface{}{},
			},
			"merge_existing": map[string]interface{}{
				"mode":              "none",
				"principal_roles":   []interface{}{"batejat"},
				"update_missing_only": true,
				"add_missing_people":  true,
				"add_missing_attrs":   true,
			},
		},
	}
}

func encodeTemplateModel(model interface{}) string {
	if model == nil {
		return ""
	}
	raw, err := json.Marshal(model)
	if err != nil {
		return ""
	}
	return string(raw)
}

func normalizeTemplateVisibility(val string) string {
	val = strings.ToLower(strings.TrimSpace(val))
	if val == "public" {
		return "public"
	}
	return "private"
}

func normalizeTemplateSeparator(val string) string {
	val = strings.TrimSpace(val)
	switch val {
	case ",", ";", "|":
		return val
	case "\t", "\\t":
		return "\t"
	default:
		return ""
	}
}

func (a *App) buildImportTemplateEntries(rows []db.CSVImportTemplate, user *db.User, perms db.PolicyPermissions) []importTemplateListEntry {
	items := make([]importTemplateListEntry, 0, len(rows))
	for _, row := range rows {
		ownerID := 0
		if row.OwnerUserID.Valid {
			ownerID = int(row.OwnerUserID.Int64)
		}
		isOwner := user != nil && ownerID == user.ID
		canEdit := user != nil && (perms.Admin || isOwner)
		isPublic := strings.TrimSpace(strings.ToLower(row.Visibility)) == "public"
		updatedAt := formatImportTemplateTime(row.UpdatedAt)
		items = append(items, importTemplateListEntry{
			ID:          row.ID,
			Name:        strings.TrimSpace(row.Name),
			Description: strings.TrimSpace(row.Description),
			Visibility:  row.Visibility,
			IsPublic:    isPublic,
			CanEdit:     canEdit,
			CanDelete:   canEdit,
			CanClone:    isPublic && !isOwner,
			CanToggle:   canEdit,
			UpdatedAt:   updatedAt,
			OwnerID:     ownerID,
		})
	}
	return items
}

func filterPublicTemplateEntries(items []importTemplateListEntry, userID int) []importTemplateListEntry {
	if userID == 0 {
		return items
	}
	filtered := make([]importTemplateListEntry, 0, len(items))
	for _, item := range items {
		if !item.IsPublic {
			continue
		}
		if item.OwnerID == userID {
			continue
		}
		filtered = append(filtered, item)
	}
	return filtered
}

func canEditImportTemplate(user *db.User, perms db.PolicyPermissions, template *db.CSVImportTemplate) bool {
	if user == nil || template == nil {
		return false
	}
	if perms.Admin {
		return true
	}
	if template.OwnerUserID.Valid && int(template.OwnerUserID.Int64) == user.ID {
		return true
	}
	return false
}

func canViewImportTemplate(user *db.User, perms db.PolicyPermissions, template *db.CSVImportTemplate) bool {
	if template == nil {
		return false
	}
	if template.Visibility == "public" {
		return true
	}
	return canEditImportTemplate(user, perms, template)
}

func (a *App) uniqueCloneName(userID int, name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		name = "Plantilla"
	}
	existing, _ := a.DB.ListCSVImportTemplates(db.CSVImportTemplateFilter{OwnerUserID: userID, Limit: 500})
	names := map[string]bool{}
	for _, t := range existing {
		names[strings.TrimSpace(t.Name)] = true
	}
	base := name + " (copia)"
	if !names[base] {
		return base
	}
	for i := 2; i < 1000; i++ {
		candidate := fmt.Sprintf("%s (copia %d)", name, i)
		if !names[candidate] {
			return candidate
		}
	}
	return fmt.Sprintf("%s (copia %d)", name, int(time.Now().Unix()))
}

func formatImportTemplateTime(val sql.NullTime) string {
	if !val.Valid {
		return ""
	}
	return val.Time.Format("02/01/2006 15:04")
}
