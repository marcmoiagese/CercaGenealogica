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
const importTemplateWizardMaxColumns = 200

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

type importTemplateWizardColumn struct {
	Index int
	Name  string
	Target string
}

type importTemplateWizardView struct {
	Step           int
	ColumnCount    int
	Separator      string
	SeparatorLabel string
	Columns        []importTemplateWizardColumn
	Name           string
	Description    string
	Visibility     string
	RecordType     string
	MainRole       string
	NameOrder      string
	DateFormat     string
	QualityLabels  bool
	QualityDubtos  string
	QualityNoConsta string
	QualityIncomplet string
	QualityIllegible string
	Error          string
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
		mode := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("mode")))
		isAdvanced := mode == "advanced" || mode == "avancat"
		switch r.Method {
		case http.MethodGet:
			if isAdvanced {
				a.importTemplateNewForm(w, r, user)
			} else {
				a.importTemplateWizardForm(w, r, user)
			}
		case http.MethodPost:
			if isAdvanced {
				a.importTemplateCreate(w, r, user)
			} else {
				a.importTemplateWizardSubmit(w, r, user)
			}
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
	created := strings.TrimSpace(r.URL.Query().Get("created")) == "1"
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
		"Created":    created,
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

func (a *App) importTemplateWizardForm(w http.ResponseWriter, r *http.Request, user *db.User) {
	view := importTemplateWizardView{
		Step:           1,
		ColumnCount:    0,
		Separator:      ",",
		SeparatorLabel: wizardSeparatorLabel(","),
		Visibility:     "private",
		RecordType:     "baptisme",
		MainRole:       "batejat",
		NameOrder:      "cognoms_first",
		DateFormat:     "dd/mm",
		QualityDubtos:  "?",
		QualityNoConsta: "¿",
	}
	a.renderImportTemplateWizard(w, r, view)
}

func (a *App) importTemplateWizardSubmit(w http.ResponseWriter, r *http.Request, user *db.User) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	step := parseFormInt(r.FormValue("wizard_step"))
	if step <= 0 {
		step = 1
	}
	action := strings.TrimSpace(r.FormValue("wizard_action"))

	columnCount, sepRaw, sepNorm, baseErr := parseWizardBasics(r)
	if step == 1 {
		if baseErr != "" {
			view := importTemplateWizardView{
				Step:        1,
				ColumnCount: columnCount,
				Separator:   sepRaw,
				Error:       baseErr,
			}
			view.SeparatorLabel = wizardSeparatorLabel(view.Separator)
			a.renderImportTemplateWizard(w, r, view)
			return
		}
		view := importTemplateWizardView{
			Step:           2,
			ColumnCount:    columnCount,
			Separator:      sepRaw,
			SeparatorLabel: wizardSeparatorLabel(sepRaw),
			Columns:        buildWizardColumns(columnCount, nil, nil),
			RecordType:     "baptisme",
			NameOrder:      "cognoms_first",
		}
		a.renderImportTemplateWizard(w, r, view)
		return
	}

	if baseErr != "" {
		view := importTemplateWizardView{
			Step:        step,
			ColumnCount: columnCount,
			Separator:   sepRaw,
			Error:       baseErr,
		}
		view.SeparatorLabel = wizardSeparatorLabel(view.Separator)
		a.renderImportTemplateWizard(w, r, view)
		return
	}

	if step == 2 {
		if action == "back" {
			view := importTemplateWizardView{
				Step:        1,
				ColumnCount: columnCount,
				Separator:   sepRaw,
			}
			view.SeparatorLabel = wizardSeparatorLabel(view.Separator)
			a.renderImportTemplateWizard(w, r, view)
			return
		}
		recordType := strings.TrimSpace(r.FormValue("record_type"))
		if recordType == "" {
			recordType = "baptisme"
		}
		names, nameErr := parseWizardColumnNames(r, columnCount)
		targets, targetErr := parseWizardColumnTargets(r, columnCount, recordType)
		view := importTemplateWizardView{
			Step:           2,
			ColumnCount:    columnCount,
			Separator:      sepRaw,
			SeparatorLabel: wizardSeparatorLabel(sepRaw),
			Columns:        buildWizardColumns(columnCount, names, targets),
			Name:           strings.TrimSpace(r.FormValue("template_name")),
			Description:    strings.TrimSpace(r.FormValue("template_description")),
			Visibility:     normalizeTemplateVisibility(r.FormValue("visibility")),
			RecordType:     recordType,
			MainRole:       strings.TrimSpace(r.FormValue("main_role")),
			NameOrder:      strings.TrimSpace(r.FormValue("name_order")),
			DateFormat:     strings.TrimSpace(r.FormValue("date_format")),
			QualityLabels:  strings.TrimSpace(r.FormValue("quality_labels")) == "1",
			QualityDubtos:  strings.TrimSpace(r.FormValue("quality_dubtos")),
			QualityNoConsta: strings.TrimSpace(r.FormValue("quality_no_consta")),
			QualityIncomplet: strings.TrimSpace(r.FormValue("quality_incomplet")),
			QualityIllegible: strings.TrimSpace(r.FormValue("quality_illegible")),
		}
		if view.Visibility == "" {
			view.Visibility = "private"
		}
		if view.RecordType == "" {
			view.RecordType = "baptisme"
		}
		if view.MainRole == "" {
			view.MainRole = defaultWizardRole(view.RecordType)
		}
		if view.NameOrder == "" {
			view.NameOrder = "cognoms_first"
		}
		if view.DateFormat == "" {
			view.DateFormat = "dd/mm"
		}
		if view.QualityDubtos == "" {
			view.QualityDubtos = "?"
		}
		if view.QualityNoConsta == "" {
			view.QualityNoConsta = "¿"
		}
		if nameErr != "" {
			view.Error = nameErr
			a.renderImportTemplateWizard(w, r, view)
			return
		}
		if targetErr != "" {
			view.Error = targetErr
			a.renderImportTemplateWizard(w, r, view)
			return
		}
		view.Step = 3
		a.renderImportTemplateWizard(w, r, view)
		return
	}

	if step == 3 {
		recordType := strings.TrimSpace(r.FormValue("record_type"))
		if recordType == "" {
			recordType = "baptisme"
		}
		names, nameErr := parseWizardColumnNames(r, columnCount)
		targets, targetErr := parseWizardColumnTargets(r, columnCount, recordType)
		view := importTemplateWizardView{
			Step:           3,
			ColumnCount:    columnCount,
			Separator:      sepRaw,
			SeparatorLabel: wizardSeparatorLabel(sepRaw),
			Columns:        buildWizardColumns(columnCount, names, targets),
			Name:           strings.TrimSpace(r.FormValue("template_name")),
			Description:    strings.TrimSpace(r.FormValue("template_description")),
			Visibility:     normalizeTemplateVisibility(r.FormValue("visibility")),
			RecordType:     recordType,
			MainRole:       strings.TrimSpace(r.FormValue("main_role")),
			NameOrder:      strings.TrimSpace(r.FormValue("name_order")),
			DateFormat:     strings.TrimSpace(r.FormValue("date_format")),
			QualityLabels:  strings.TrimSpace(r.FormValue("quality_labels")) == "1",
			QualityDubtos:  strings.TrimSpace(r.FormValue("quality_dubtos")),
			QualityNoConsta: strings.TrimSpace(r.FormValue("quality_no_consta")),
			QualityIncomplet: strings.TrimSpace(r.FormValue("quality_incomplet")),
			QualityIllegible: strings.TrimSpace(r.FormValue("quality_illegible")),
		}
		if view.Visibility == "" {
			view.Visibility = "private"
		}
		if view.RecordType == "" {
			view.RecordType = "baptisme"
		}
		if view.MainRole == "" {
			view.MainRole = defaultWizardRole(view.RecordType)
		}
		if view.NameOrder == "" {
			view.NameOrder = "cognoms_first"
		}
		if view.DateFormat == "" {
			view.DateFormat = "dd/mm"
		}
		if view.QualityDubtos == "" {
			view.QualityDubtos = "?"
		}
		if view.QualityNoConsta == "" {
			view.QualityNoConsta = "¿"
		}
		if action == "back" {
			view.Step = 2
			a.renderImportTemplateWizard(w, r, view)
			return
		}
		if nameErr != "" {
			view.Error = nameErr
			a.renderImportTemplateWizard(w, r, view)
			return
		}
		if targetErr != "" {
			view.Error = targetErr
			a.renderImportTemplateWizard(w, r, view)
			return
		}
		if view.Name == "" {
			view.Error = "El nom de la plantilla és obligatori."
			a.renderImportTemplateWizard(w, r, view)
			return
		}
		model := defaultImportTemplateModel(ResolveLang(r))
		metadata := ensureMap(model, "metadata")
		metadata["record_type"] = view.RecordType
		model["date_format"] = view.DateFormat
		model["name_order"] = view.NameOrder
		model["quality"] = map[string]interface{}{
			"labels": view.QualityLabels,
			"markers": map[string]interface{}{
				"dubtos": view.QualityDubtos,
				"no_consta": view.QualityNoConsta,
				"incomplet": view.QualityIncomplet,
				"illegible": view.QualityIllegible,
			},
		}
		if policies, ok := model["policies"].(map[string]interface{}); ok {
			if merge, ok := policies["merge_existing"].(map[string]interface{}); ok {
				merge["principal_roles"] = []interface{}{view.MainRole}
			}
		}
		columns := make([]interface{}, 0, len(names))
		for i, header := range names {
			target := ""
			if i < len(targets) {
				target = targets[i]
			}
			columns = append(columns, buildWizardColumnMapping(header, target, view))
		}
		model["mapping"] = map[string]interface{}{"columns": columns}
		modelJSON := encodeTemplateModel(model)
		newTemplate := &db.CSVImportTemplate{
			Name:             view.Name,
			Description:      view.Description,
			Visibility:       view.Visibility,
			DefaultSeparator: sepNorm,
			ModelJSON:        modelJSON,
			OwnerUserID:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
		}
		newID, err := a.DB.CreateCSVImportTemplate(newTemplate)
		if err != nil || newID <= 0 {
			view.Error = "No s'ha pogut crear la plantilla."
			a.renderImportTemplateWizard(w, r, view)
			return
		}
		http.Redirect(w, r, "/importador/plantilles?scope=mine&created=1", http.StatusSeeOther)
		return
	}

	http.NotFound(w, r)
}

func (a *App) renderImportTemplateWizard(w http.ResponseWriter, r *http.Request, view importTemplateWizardView) {
	RenderPrivateTemplate(w, r, "import-template-wizard.html", map[string]interface{}{
		"View": view,
	})
}

func buildWizardColumns(count int, names []string, kinds []string) []importTemplateWizardColumn {
	if count <= 0 {
		return []importTemplateWizardColumn{}
	}
	cols := make([]importTemplateWizardColumn, 0, count)
	for i := 1; i <= count; i++ {
		name := ""
		if i-1 < len(names) {
			name = names[i-1]
		}
		target := ""
		if i-1 < len(kinds) {
			target = kinds[i-1]
		}
		cols = append(cols, importTemplateWizardColumn{
			Index: i,
			Name:  name,
			Target: target,
		})
	}
	return cols
}

func parseWizardBasics(r *http.Request) (int, string, string, string) {
	columnCount := parseFormInt(r.FormValue("column_count"))
	if columnCount <= 0 {
		return columnCount, strings.TrimSpace(r.FormValue("separator")), "", "Indica quantes columnes té el CSV."
	}
	if columnCount > importTemplateWizardMaxColumns {
		return columnCount, strings.TrimSpace(r.FormValue("separator")), "", fmt.Sprintf("Massa columnes (màxim %d).", importTemplateWizardMaxColumns)
	}
	sepRaw := strings.TrimSpace(r.FormValue("separator"))
	sepNorm := normalizeTemplateSeparator(sepRaw)
	if sepNorm == "" {
		return columnCount, sepRaw, "", "Selecciona un separador vàlid."
	}
	return columnCount, sepRaw, sepNorm, ""
}

func parseWizardColumnNames(r *http.Request, count int) ([]string, string) {
	if count <= 0 {
		return nil, "Indica quantes columnes té el CSV."
	}
	names := make([]string, count)
	for i := 1; i <= count; i++ {
		name := strings.TrimSpace(r.FormValue(fmt.Sprintf("col_%d", i)))
		names[i-1] = name
		if name == "" {
			return names, fmt.Sprintf("El nom de la columna %d és obligatori.", i)
		}
	}
	return names, ""
}

func parseWizardColumnTargets(r *http.Request, count int, recordType string) ([]string, string) {
	if count <= 0 {
		return nil, "Indica quantes columnes té el CSV."
	}
	targets := make([]string, count)
	allowedTargets := allowedTemplateTargetsForRecordType(recordType)
	for i := 1; i <= count; i++ {
		val := strings.TrimSpace(r.FormValue(fmt.Sprintf("col_target_%d", i)))
		val = strings.TrimSpace(strings.TrimPrefix(val, "target:"))
		if val == "" {
			val = legacyWizardTypeToTarget(strings.TrimSpace(r.FormValue(fmt.Sprintf("col_type_%d", i))), recordType)
		}
		if val == "" || val == "ignore" {
			targets[i-1] = ""
			continue
		}
		if allowedTargets != nil && !allowedTargets[val] {
			return targets, fmt.Sprintf("Target invàlid per la columna %d.", i)
		}
		targets[i-1] = val
	}
	return targets, ""
}

func legacyWizardTypeToTarget(val string, recordType string) string {
	switch strings.TrimSpace(val) {
	case "base_llibre_id":
		return "base.llibre_id"
	case "base_pagina_id":
		return "base.pagina_id"
	case "base_num_pagina_text":
		return "base.num_pagina_text"
	case "base_any_doc":
		return "base.any_doc"
	case "base_data_acte":
		return "base.data_acte_iso_text_estat"
	case "base_transcripcio_literal":
		return "base.transcripcio_literal"
	case "person_full":
		return "person." + defaultWizardRole(recordType)
	case "person_nom":
		return "person." + defaultWizardRole(recordType) + ".nom"
	case "person_cognom1":
		return "person." + defaultWizardRole(recordType) + ".cognom1"
	case "person_cognom2":
		return "person." + defaultWizardRole(recordType) + ".cognom2"
	default:
		return ""
	}
}

func defaultWizardRole(recordType string) string {
	switch strings.ToLower(strings.TrimSpace(recordType)) {
	case "obit":
		return "difunt"
	case "matrimoni":
		return "nuvi"
	default:
		return "batejat"
	}
}

func buildWizardColumnMapping(header string, target string, view importTemplateWizardView) map[string]interface{} {
	header = strings.TrimSpace(header)
	column := map[string]interface{}{
		"header":   header,
		"aliases":  []interface{}{},
		"required": false,
		"map_to":   []interface{}{},
	}
	transforms := []interface{}{}
	target = strings.TrimSpace(target)
	if isWizardPersonRoleTarget(target) {
		if view.NameOrder == "nom_first" {
			transforms = append(transforms, map[string]interface{}{"name": "parse_person_from_nom_marcmoia_v2"})
		} else {
			transforms = append(transforms, map[string]interface{}{"name": "parse_person_from_cognoms_marcmoia_v2"})
		}
	}
	if target == "base.data_acte_iso_text_estat" {
		transforms = append(transforms, map[string]interface{}{"name": "parse_date_flexible_to_base_data_acte"})
	}
	if strings.HasPrefix(target, "attr.") && strings.HasSuffix(target, ".date_or_text_with_quality") {
		transforms = append(transforms, map[string]interface{}{"name": "parse_date_flexible_to_date_or_text_with_quality"})
	}
	if target != "" {
		column["map_to"] = []interface{}{
			map[string]interface{}{
				"target":     target,
				"transforms": transforms,
			},
		}
	}
	return column
}

func isWizardPersonRoleTarget(target string) bool {
	if !strings.HasPrefix(target, "person.") {
		return false
	}
	rest := strings.TrimPrefix(target, "person.")
	return rest != "" && !strings.Contains(rest, ".")
}

func wizardSeparatorLabel(raw string) string {
	raw = strings.TrimSpace(raw)
	switch raw {
	case ",":
		return "Coma (,)"
	case ";":
		return "Punt i coma (;)"
	case "|":
		return "Barra vertical (|)"
	case "\\t", "\t":
		return "Tabulador (\\t)"
	default:
		return raw
	}
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
