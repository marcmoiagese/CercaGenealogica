package core

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type confessionalFormData struct {
	Kind      string
	IsNew     bool
	ReturnURL string
	Error     string
	Religio   *db.ReligioConfessio
	Model     *db.ModelConfessional
	Nivell    *db.NivellConfessional
	Entitat   *db.EntitatReligiosa
	Relacio   *db.MunicipiEntitatReligiosa
	RelEnt    *db.EntitatReligiosaRelacio
}

type confessionalSection struct {
	Kind       string
	Slug       string
	Title      string
	NewLabel   string
	ViewPerm   string
	CreatePerm string
	EditPerm   string
	DeletePerm string
}

type confessionalEntityListFilter struct {
	ReligionCode string
	LevelCode    string
	ParentID     int
	ParentMode   string
	Text         string
	Status       string
	Sort         string
	SortDir      string
	Page         int
	PerPage      int
	Total        int
	TotalPages   int
}

type confessionalNavigationContext struct {
	HasFilters bool
}

type confessionalHierarchyPathItem struct {
	ID    int
	Name  string
	Level string
}

type confessionalHierarchyRow struct {
	Entitat        db.EntitatReligiosa
	ParentID       int
	ParentName     string
	Path           []confessionalHierarchyPathItem
	ChildCount     int
	TerritoryCount int
	Depth          int
}

type confessionalEntitySuggestFilter struct {
	Query        string
	Limit        int
	Scope        string
	ReligionCode string
	LevelCode    string
	ExcludeID    int
	ChildID      int
}

type confessionalEntitySuggestion struct {
	label string
	score int
	item  map[string]interface{}
}

var confessionalSections = map[string]confessionalSection{
	"religio": {Kind: "religio", Slug: "religions", Title: "Religions/confessions", NewLabel: "Nova religio/confessio", ViewPerm: permKeyTerritoriConfessionalReligionsView, CreatePerm: permKeyTerritoriConfessionalReligionsCreate, EditPerm: permKeyTerritoriConfessionalReligionsEdit, DeletePerm: permKeyTerritoriConfessionalReligionsDelete},
	"model":   {Kind: "model", Slug: "models", Title: "Models confessionals", NewLabel: "Nou model", ViewPerm: permKeyTerritoriConfessionalModelsView, CreatePerm: permKeyTerritoriConfessionalModelsCreate, EditPerm: permKeyTerritoriConfessionalModelsEdit, DeletePerm: permKeyTerritoriConfessionalModelsDelete},
	"nivell":  {Kind: "nivell", Slug: "nivells", Title: "Nivells confessionals", NewLabel: "Nou nivell", ViewPerm: permKeyTerritoriConfessionalNivellsView, CreatePerm: permKeyTerritoriConfessionalNivellsCreate, EditPerm: permKeyTerritoriConfessionalNivellsEdit, DeletePerm: permKeyTerritoriConfessionalNivellsDelete},
	"entitat": {Kind: "entitat", Slug: "entitats", Title: "Entitats religioses", NewLabel: "Nova entitat", ViewPerm: permKeyTerritoriConfessionalEntitatsView, CreatePerm: permKeyTerritoriConfessionalEntitatsCreate, EditPerm: permKeyTerritoriConfessionalEntitatsEdit, DeletePerm: permKeyTerritoriConfessionalEntitatsDelete},
	"rel_ent": {Kind: "rel_ent", Slug: "relacions-entitats", Title: "Relacions entre entitats religioses", NewLabel: "Nova relacio entre entitats", ViewPerm: permKeyTerritoriConfessionalRelacionsEntitatsView, CreatePerm: permKeyTerritoriConfessionalRelacionsEntitatsCreate, EditPerm: permKeyTerritoriConfessionalRelacionsEntitatsEdit, DeletePerm: permKeyTerritoriConfessionalRelacionsEntitatsDelete},
	"relacio": {Kind: "relacio", Slug: "municipis-entitats", Title: "Relacions municipi/nucli - entitat religiosa", NewLabel: "Nova relacio territorial", ViewPerm: permKeyTerritoriConfessionalMunicipisEntitatsView, CreatePerm: permKeyTerritoriConfessionalMunicipisEntitatsCreate, EditPerm: permKeyTerritoriConfessionalMunicipisEntitatsEdit, DeletePerm: permKeyTerritoriConfessionalMunicipisEntitatsDelete},
}

var errConfessionalParentLevelIncompatible = errors.New("confessional parent level incompatible")
var errConfessionalParentCycle = errors.New("confessional parent cycle")

func (a *App) AdminConfessionalList(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/confessional/entitats", http.StatusSeeOther)
}

func (a *App) AdminConfessionalNavigation(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriConfessionalEntitatsView, PermissionTarget{})
	if !ok {
		return
	}
	lang := ResolveLangForUser(r, user.PreferredLang)
	filter := parseConfessionalEntityListFilter(r)
	filter.ParentMode = normalizeConfessionalNavigationParentMode(r.URL.Query().Get("parent_mode"))
	allEntitats, _ := a.DB.ListEntitatsReligioses()
	allRelEntitats, _ := a.DB.ListEntitatReligiosaRelacions()
	allRelacions, _ := a.DB.ListMunicipiEntitatsReligioses(0)
	publishedEntitats := publishedEntitatsReligioses(allEntitats)
	publishedRelEntitats := publishedEntitatReligiosaRelacions(allRelEntitats)
	hasFilters := filter.Text != "" || filter.ReligionCode != "" || filter.LevelCode != "" || filter.ParentID > 0
	rows := []confessionalHierarchyRow{}
	parentPath := []confessionalHierarchyPathItem{}
	if hasFilters {
		rows, parentPath = filterConfessionalHierarchyRows(publishedEntitats, publishedRelEntitats, publishedMunicipiEntitatsReligioses(allRelacions), &filter, lang)
	}
	RenderPrivateTemplate(w, r, "admin-confessional-navegacio.html", map[string]interface{}{
		"Section":               confessionalSectionMust("entitat"),
		"Navigation":            confessionalNavigationContext{HasFilters: hasFilters},
		"HierarchyRows":         rows,
		"Filter":                filter,
		"ParentOptions":         confessionalHierarchyParentOptions(publishedEntitats, &filter),
		"ParentPath":            parentPath,
		"SelectableReligions":   ListSelectableConfessionalReligionCatalog(),
		"SelectableNivells":     ListConfessionalLevelCatalog(),
		"ParentLevelCodesCSV":   confessionalParentLevelCodesCSVMap(),
		"LevelCanHaveChildren":  confessionalLevelCanHaveChildrenMap(),
		"ReligionCatalogLabels": confessionalReligionCatalogLabels(lang),
		"LevelCatalogLabels":    confessionalLevelCatalogLabels(lang),
		"CanCreate":             a.HasPermission(user.ID, permKeyTerritoriConfessionalEntitatsCreate, PermissionTarget{}),
		"CanEdit":               a.HasPermission(user.ID, permKeyTerritoriConfessionalEntitatsEdit, PermissionTarget{}),
		"CanDelete":             a.HasPermission(user.ID, permKeyTerritoriConfessionalEntitatsDelete, PermissionTarget{}),
		"CanManageArxius":       a.canManageAnyDocumentalsModular(user),
		"User":                  user,
	})
}

func (a *App) AdminConfessionalSectionList(w http.ResponseWriter, r *http.Request) {
	section, okSection := confessionalSectionFromPath(r.URL.Path)
	if !okSection {
		http.NotFound(w, r)
		return
	}
	if confessionalCatalogSection(section.Kind) {
		http.Redirect(w, r, "/confessional/entitats", http.StatusSeeOther)
		return
	}
	user, ok := a.requirePermissionKey(w, r, section.ViewPerm, PermissionTarget{})
	if !ok {
		return
	}
	var religions []db.ReligioConfessio
	var models []db.ModelConfessional
	var nivells []db.NivellConfessional
	var entitats []db.EntitatReligiosa
	var hierarchyRows []confessionalHierarchyRow
	var relacions []db.MunicipiEntitatReligiosa
	var relEntitats []db.EntitatReligiosaRelacio
	var municipis []db.MunicipiRow
	var paisos []db.Pais
	listFilter := parseConfessionalEntityListFilter(r)
	parentOptions := []db.EntitatReligiosa{}
	var selectedParent *db.EntitatReligiosa
	var parentPath []confessionalHierarchyPathItem
	pagination := Pagination{}
	lang := ResolveLangForUser(r, user.PreferredLang)
	switch section.Kind {
	case "model":
		religions, _ = a.DB.ListReligioConfessions()
		models, _ = a.DB.ListModelsConfessionals()
		paisos, _ = a.DB.ListPaisos()
	case "entitat":
		allEntitats, _ := a.DB.ListEntitatsReligioses()
		allRelEntitats, _ := a.DB.ListEntitatReligiosaRelacions()
		allRelacions, _ := a.DB.ListMunicipiEntitatsReligioses(0)
		publishedEntitats := publishedEntitatsReligioses(allEntitats)
		publishedRelEntitats := publishedEntitatReligiosaRelacions(allRelEntitats)
		if strings.TrimSpace(r.URL.Query().Get("parent_mode")) == "" {
			listFilter.ParentMode = "all"
		}
		if listFilter.Sort == "" {
			listFilter.Sort = "path"
		}
		parentOptions = confessionalManagementParentOptionsWithRelations(publishedEntitats, publishedRelEntitats)
		selectedParent = confessionalEntityByID(publishedEntitats, listFilter.ParentID)
		if listFilter.PerPage <= 0 {
			listFilter.PerPage = parseListPerPage("")
		}
		if listFilter.Page <= 0 {
			listFilter.Page = 1
		}
		hierarchyRows, parentPath = filterConfessionalHierarchyRows(publishedEntitats, publishedRelEntitats, publishedMunicipiEntitatsReligioses(allRelacions), &listFilter, lang)
		pagination = buildPagination(r, listFilter.Page, listFilter.PerPage, listFilter.Total, "#nivellsTable")
		listFilter.Page = pagination.Page
		listFilter.PerPage = pagination.PerPage
		listFilter.Total = pagination.Total
		listFilter.TotalPages = pagination.TotalPages
		relEntitats = publishedEntitatReligiosaRelacions(allRelEntitats)
	case "rel_ent":
		allEntitats, _ := a.DB.ListEntitatsReligioses()
		allRelEntitats, _ := a.DB.ListEntitatReligiosaRelacions()
		entitats = publishedEntitatsReligioses(allEntitats)
		relEntitats = publishedEntitatReligiosaRelacions(allRelEntitats)
	case "relacio":
		allEntitats, _ := a.DB.ListEntitatsReligioses()
		allRelacions, _ := a.DB.ListMunicipiEntitatsReligioses(0)
		entitats = publishedEntitatsReligioses(allEntitats)
		relacions = publishedMunicipiEntitatsReligioses(allRelacions)
		municipis, _ = a.DB.ListMunicipis(db.MunicipiFilter{})
	}
	canCreate := a.HasPermission(user.ID, section.CreatePerm, PermissionTarget{})
	canEdit := a.HasPermission(user.ID, section.EditPerm, PermissionTarget{})
	canDelete := a.HasPermission(user.ID, section.DeletePerm, PermissionTarget{})
	RenderPrivateTemplate(w, r, "admin-confessional-list.html", map[string]interface{}{
		"Section":               section,
		"Religions":             religions,
		"Models":                models,
		"Nivells":               nivells,
		"Entitats":              entitats,
		"HierarchyRows":         hierarchyRows,
		"Relacions":             relacions,
		"RelEntitats":           relEntitats,
		"Filter":                listFilter,
		"FilterValues":          confessionalFilterValues(r),
		"FilterOrder":           strings.TrimSpace(r.URL.Query().Get("order")),
		"ParentOptions":         parentOptions,
		"SelectedParent":        selectedParent,
		"ParentPath":            parentPath,
		"Page":                  pagination.Page,
		"PerPage":               pagination.PerPage,
		"Total":                 pagination.Total,
		"TotalPages":            pagination.TotalPages,
		"PageLinks":             pagination.Links,
		"PageSelectBase":        pagination.SelectBase,
		"PageAnchor":            pagination.Anchor,
		"Municipis":             municipis,
		"Paisos":                paisos,
		"SelectableReligions":   ListSelectableConfessionalReligionCatalog(),
		"SelectableNivells":     ListConfessionalLevelCatalog(),
		"ParentLevelCodesCSV":   confessionalParentLevelCodesCSVMap(),
		"LevelCanHaveChildren":  confessionalLevelCanHaveChildrenMap(),
		"CanCreate":             canCreate,
		"CanEdit":               canEdit,
		"CanDelete":             canDelete,
		"Notice":                strings.TrimSpace(r.URL.Query().Get("notice")),
		"Error":                 strings.TrimSpace(r.URL.Query().Get("error")),
		"ReligionLabels":        religioLabels(religions),
		"ReligionCatalogLabels": confessionalReligionCatalogLabels(lang),
		"ModelLabels":           modelLabels(models),
		"NivellLabels":          nivellConfessionalLabels(nivells),
		"LevelCatalogLabels":    confessionalLevelCatalogLabels(lang),
		"EntitatLabels":         entitatReligiosaLabels(entitats),
		"MunicipiLabels":        municipiLabels(municipis),
		"PaisLabels":            paisLabels(paisos),
		"CanManageArxius":       a.canManageAnyDocumentalsModular(user),
		"User":                  user,
	})
}

func (a *App) AdminNewConfessional(w http.ResponseWriter, r *http.Request) {
	section, okSection := confessionalSectionFromPath(r.URL.Path)
	if !okSection {
		kind := confessionalKind(r.URL.Query().Get("kind"))
		section, okSection = confessionalSectionByKind(kind)
	}
	if !okSection {
		http.NotFound(w, r)
		return
	}
	user, ok := a.requirePermissionKey(w, r, section.CreatePerm, PermissionTarget{})
	if !ok {
		return
	}
	kind := section.Kind
	if kind == "" {
		http.NotFound(w, r)
		return
	}
	if confessionalCatalogSection(kind) {
		http.NotFound(w, r)
		return
	}
	data := confessionalFormData{Kind: kind, IsNew: true, ReturnURL: firstNonEmpty(strings.TrimSpace(r.URL.Query().Get("return_to")), confessionalSectionURL(section, ""))}
	switch kind {
	case "model":
		data.Model = &db.ModelConfessional{Estat: "actiu", ModeracioEstat: "pendent"}
	case "entitat":
		data.Entitat = &db.EntitatReligiosa{Estat: "actiu", ModeracioEstat: "pendent"}
		if parentID := parsePositiveIntDefault(r.URL.Query().Get("parent_id"), 0, 0, 1000000000); parentID > 0 {
			if parent, err := a.DB.GetEntitatReligiosa(parentID); err == nil && parent != nil && parent.ModeracioEstat == "publicat" {
				data.Entitat.ParentID = sql.NullInt64{Int64: int64(parentID), Valid: true}
				data.Entitat.ReligioConfessioCodi = parent.ReligioConfessioCodi
			}
		}
	case "relacio":
		data.Relacio = &db.MunicipiEntitatReligiosa{TipusRelacio: "principal", ModeracioEstat: "pendent"}
		if municipiID := parsePositiveIntDefault(r.URL.Query().Get("municipi_id"), 0, 0, 1000000000); municipiID > 0 {
			if municipi, err := a.DB.GetMunicipi(municipiID); err == nil && municipi != nil {
				data.Relacio.MunicipiID = municipiID
			}
		}
		if nucliID := parsePositiveIntDefault(r.URL.Query().Get("nucli_id"), 0, 0, 1000000000); nucliID > 0 {
			if nucli, err := a.DB.GetMunicipi(nucliID); err == nil && nucli != nil {
				if data.Relacio.MunicipiID == 0 && nucli.MunicipiID.Valid {
					data.Relacio.MunicipiID = int(nucli.MunicipiID.Int64)
				}
				if nucli.MunicipiID.Valid && int(nucli.MunicipiID.Int64) == data.Relacio.MunicipiID {
					data.Relacio.NucliID = sql.NullInt64{Int64: int64(nucliID), Valid: true}
				}
			}
		}
		if entitatID := parsePositiveIntDefault(r.URL.Query().Get("entitat_religiosa_id"), 0, 0, 1000000000); entitatID > 0 {
			if entitat, err := a.DB.GetEntitatReligiosa(entitatID); err == nil && entitat != nil && entitat.ModeracioEstat == "publicat" {
				data.Relacio.EntitatReligiosaID = entitatID
			}
		}
	case "rel_ent":
		data.RelEnt = &db.EntitatReligiosaRelacio{ModeracioEstat: "pendent"}
		if parentID := parsePositiveIntDefault(r.URL.Query().Get("parent_id"), 0, 0, 1000000000); parentID > 0 {
			if parent, err := a.DB.GetEntitatReligiosa(parentID); err == nil && parent != nil && parent.ModeracioEstat == "publicat" {
				data.RelEnt.EntitatOrigenID = parentID
			}
		}
		if childID := parsePositiveIntDefault(r.URL.Query().Get("child_id"), 0, 0, 1000000000); childID > 0 {
			if child, err := a.DB.GetEntitatReligiosa(childID); err == nil && child != nil && child.ModeracioEstat == "publicat" {
				data.RelEnt.EntitatDestiID = childID
			}
		}
	}
	a.renderConfessionalForm(w, r, user, data)
}

func (a *App) AdminEditConfessional(w http.ResponseWriter, r *http.Request) {
	section, okSection := confessionalSectionFromPath(r.URL.Path)
	if !okSection {
		kind, _ := extractConfessionalPath(r.URL.Path)
		section, okSection = confessionalSectionByKind(kind)
		if !okSection {
			http.NotFound(w, r)
			return
		}
	}
	user, ok := a.requirePermissionKey(w, r, section.EditPerm, PermissionTarget{})
	if !ok {
		return
	}
	kind, id := extractConfessionalPath(r.URL.Path)
	if kind == "" || id == 0 {
		http.NotFound(w, r)
		return
	}
	if confessionalCatalogSection(kind) {
		http.NotFound(w, r)
		return
	}
	data := confessionalFormData{Kind: kind, ReturnURL: strings.TrimSpace(r.URL.Query().Get("return_to"))}
	var err error
	switch kind {
	case "model":
		data.Model, err = a.DB.GetModelConfessional(id)
	case "entitat":
		data.Entitat, err = a.DB.GetEntitatReligiosa(id)
	case "relacio":
		data.Relacio, err = a.DB.GetMunicipiEntitatReligiosa(id)
	case "rel_ent":
		data.RelEnt, err = a.DB.GetEntitatReligiosaRelacio(id)
	}
	if err != nil {
		http.NotFound(w, r)
		return
	}
	a.renderConfessionalForm(w, r, user, data)
}

func (a *App) AdminSaveConfessional(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/confessional", http.StatusSeeOther)
		return
	}
	id, _ := strconv.Atoi(r.FormValue("id"))
	kind := confessionalKind(r.FormValue("kind"))
	if kind == "" {
		http.NotFound(w, r)
		return
	}
	section, okSection := confessionalSectionByKind(kind)
	if !okSection {
		http.NotFound(w, r)
		return
	}
	permKey := section.CreatePerm
	if id != 0 {
		permKey = section.EditPerm
	}
	user, ok := a.requirePermissionKey(w, r, permKey, PermissionTarget{})
	if !ok {
		return
	}
	if confessionalCatalogSection(kind) {
		http.NotFound(w, r)
		return
	}
	lang := ResolveLangForUser(r, user.PreferredLang)
	data, errMsg := a.parseConfessionalForm(kind, id, r, lang)
	if errMsg != "" {
		data.Error = errMsg
		a.renderConfessionalForm(w, r, user, data)
		return
	}
	if err := a.applyConfessionalAuthorship(kind, id, user.ID, &data); err != nil {
		data.Error = "No s'ha pogut preparar l'autoria del registre confessional."
		a.renderConfessionalForm(w, r, user, data)
		return
	}
	needsParentRelation := false
	if kind == "entitat" && data.Entitat != nil && data.Entitat.ParentID.Valid {
		if id == 0 {
			needsParentRelation = true
		} else {
			var err error
			needsParentRelation, err = a.needsConfessionalParentRelation(int(data.Entitat.ParentID.Int64), id)
			if err != nil {
				data.Error = "No s'ha pogut comprovar la relacio jerarquica."
				a.renderConfessionalForm(w, r, user, data)
				return
			}
		}
		if needsParentRelation && !a.HasPermission(user.ID, permKeyTerritoriConfessionalRelacionsEntitatsCreate, PermissionTarget{}) {
			data.Error = "No tens permis per crear la relacio jerarquica."
			a.renderConfessionalForm(w, r, user, data)
			return
		}
	}
	if kind == "entitat" && id > 0 {
		if proposed, err := a.createEntitatReligiosaWikiProposal(data.Entitat, user.ID); err != nil {
			data.Error = "No s'ha pogut crear la proposta de canvi."
			a.renderConfessionalForm(w, r, user, data)
			return
		} else if proposed {
			if needsParentRelation {
				if err := a.createConfessionalParentRelation(data.Entitat, id, user.ID); err != nil {
					data.Error = "No s'ha pogut crear la relacio jerarquica."
					a.renderConfessionalForm(w, r, user, data)
					return
				}
			}
			returnURL := data.ReturnURL
			if returnURL == "" {
				returnURL = fmt.Sprintf("/confessional/entitats/%d?notice=pending", id)
			}
			http.Redirect(w, r, returnURL, http.StatusSeeOther)
			return
		}
	}
	savedID, err := a.saveConfessionalData(kind, data)
	if err != nil {
		data.Error = "No s'ha pogut desar el registre confessional."
		a.renderConfessionalForm(w, r, user, data)
		return
	}
	if kind == "entitat" && data.Entitat != nil && data.Entitat.ParentID.Valid && (id == 0 || needsParentRelation) {
		if err := a.createConfessionalParentRelation(data.Entitat, savedID, user.ID); err != nil {
			data.Error = "No s'ha pogut crear la relacio jerarquica."
			a.renderConfessionalForm(w, r, user, data)
			return
		}
	}
	returnURL := data.ReturnURL
	if returnURL == "" {
		returnURL = confessionalSectionURL(section, "notice=saved")
	}
	http.Redirect(w, r, returnURL, http.StatusSeeOther)
}

func (a *App) AdminConfessionalEntityShow(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriConfessionalEntitatsView, PermissionTarget{})
	if !ok {
		return
	}
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	entitat, err := a.DB.GetEntitatReligiosa(id)
	if err != nil || entitat == nil {
		http.NotFound(w, r)
		return
	}
	canModerate := a.canModerateWikiObject(user, "entitat_religiosa", id)
	canEdit := a.HasPermission(user.ID, permKeyTerritoriConfessionalEntitatsEdit, PermissionTarget{})
	canDelete := a.HasPermission(user.ID, permKeyTerritoriConfessionalEntitatsDelete, PermissionTarget{})
	if entitat.ModeracioEstat != "publicat" && !(canEdit || canModerate) {
		http.NotFound(w, r)
		return
	}

	allEntitats, _ := a.DB.ListEntitatsReligioses()
	allRelEnt, _ := a.DB.ListEntitatReligiosaRelacions()
	allRelTerr, _ := a.DB.ListMunicipiEntitatsReligioses(0)
	relacionsArxius, _ := a.DB.ListArxiuEntitatsReligioses(0, id, "publicat")
	arxius, _ := a.DB.ListArxius(db.ArxiuFilter{Status: "publicat", Limit: -1})
	municipis, _ := a.DB.ListMunicipis(db.MunicipiFilter{})
	paisos, _ := a.DB.ListPaisos()
	changes, _ := a.DB.ListWikiChanges("entitat_religiosa", id)

	relsSuperiors := make([]db.EntitatReligiosaRelacio, 0)
	relsInferiors := make([]db.EntitatReligiosaRelacio, 0)
	for _, rel := range allRelEnt {
		if rel.ModeracioEstat != "publicat" && !canModerate {
			continue
		}
		if rel.EntitatDestiID == id {
			relsSuperiors = append(relsSuperiors, rel)
		}
		if rel.EntitatOrigenID == id {
			relsInferiors = append(relsInferiors, rel)
		}
	}
	relsTerritori := make([]db.MunicipiEntitatReligiosa, 0)
	for _, rel := range allRelTerr {
		if rel.EntitatReligiosaID == id && (rel.ModeracioEstat == "publicat" || canModerate) {
			relsTerritori = append(relsTerritori, rel)
		}
	}
	hasPending := entitat.ModeracioEstat == "pendent"
	for _, ch := range changes {
		if ch.ModeracioEstat == "pendent" {
			hasPending = true
			break
		}
	}

	lang := ResolveLangForUser(r, user.PreferredLang)
	relationTypeLabels := confessionalLevelCatalogLabels(lang)
	for _, rel := range allRelEnt {
		if _, ok := relationTypeLabels[rel.TipusRelacio]; !ok {
			relationTypeLabels[rel.TipusRelacio] = rel.TipusRelacio
		}
	}
	for _, rel := range allRelTerr {
		if _, ok := relationTypeLabels[rel.TipusRelacio]; !ok {
			relationTypeLabels[rel.TipusRelacio] = rel.TipusRelacio
		}
	}
	RenderPrivateTemplate(w, r, "admin-confessional-entity-show.html", map[string]interface{}{
		"Entitat":                         entitat,
		"EntitatLabels":                   entitatReligiosaLabels(allEntitats),
		"ReligionCatalogLabels":           confessionalReligionCatalogLabels(lang),
		"LevelCatalogLabels":              confessionalLevelCatalogLabels(lang),
		"RelationTypeLabels":              relationTypeLabels,
		"PaisLabels":                      paisLabels(paisos),
		"MunicipiLabels":                  municipiLabels(municipis),
		"RelacionsSuperiors":              relsSuperiors,
		"RelacionsInferiors":              relsInferiors,
		"RelacionsTerritori":              relsTerritori,
		"RelacionsArxius":                 relacionsArxius,
		"ArxiuLabels":                     arxiuLabels(arxius),
		"ArxiuEntitatReligiosaTypeLabels": arxiuEntitatReligiosaTypeLabels(lang),
		"HasPendingChanges":               hasPending,
		"CanEdit":                         canEdit,
		"CanDelete":                       canDelete,
		"CanCreateArxiuEntitatReligiosa":  a.HasPermission(user.ID, permKeyTerritoriConfessionalArxiusEntitatsCreate, PermissionTarget{}),
		"CanEditArxiuEntitatReligiosa":    a.HasPermission(user.ID, permKeyTerritoriConfessionalArxiusEntitatsEdit, PermissionTarget{}),
		"CanDeleteArxiuEntitatReligiosa":  a.HasPermission(user.ID, permKeyTerritoriConfessionalArxiusEntitatsDelete, PermissionTarget{}),
		"CanModerate":                     canModerate,
		"Creator":                         a.confessionalUserLabel(entitat.CreatedBy),
		"Updater":                         a.confessionalUserLabel(entitat.UpdatedBy),
		"Moderator":                       a.confessionalUserLabel(entitat.ModeratedBy),
		"Notice":                          strings.TrimSpace(r.URL.Query().Get("notice")),
		"User":                            user,
	})
}

func (a *App) confessionalUserLabel(id sql.NullInt64) string {
	if !id.Valid || id.Int64 <= 0 {
		return "-"
	}
	u, err := a.DB.GetUserByID(int(id.Int64))
	if err != nil || u == nil {
		return fmt.Sprintf("#%d", id.Int64)
	}
	label := strings.TrimSpace(u.Usuari)
	if label == "" {
		label = strings.TrimSpace(strings.TrimSpace(u.Name) + " " + strings.TrimSpace(u.Surname))
	}
	if label == "" {
		label = fmt.Sprintf("#%d", u.ID)
	}
	return label
}

func (a *App) EntitatReligiosaWikiHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriConfessionalEntitatsView, PermissionTarget{})
	if !ok {
		return
	}
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	entitat, err := a.DB.GetEntitatReligiosa(id)
	if err != nil || entitat == nil {
		http.NotFound(w, r)
		return
	}
	canModerate := a.canModerateWikiObject(user, "entitat_religiosa", id)
	if entitat.ModeracioEstat != "publicat" && !canModerate {
		http.NotFound(w, r)
		return
	}
	changes, _ := a.DB.ListWikiChanges("entitat_religiosa", id)
	changes = filterVisibleWikiChanges(changes, user.ID, canModerate)
	totalChanges := len(changes)
	history := make([]wikiHistoryItem, 0, len(changes))
	for idx, ch := range changes {
		changedByID := 0
		if ch.ChangedBy.Valid {
			changedByID = int(ch.ChangedBy.Int64)
		}
		moderatedBy := ""
		if ch.ModeratedBy.Valid {
			moderatedBy = a.confessionalUserLabel(ch.ModeratedBy)
		}
		changedAt := ""
		if !ch.ChangedAt.IsZero() {
			changedAt = ch.ChangedAt.Format("02/01/2006 15:04")
		}
		moderatedAt := ""
		if ch.ModeratedAt.Valid {
			moderatedAt = ch.ModeratedAt.Time.Format("02/01/2006 15:04")
		}
		before, after := parseWikiChangeMeta(ch.Metadata)
		history = append(history, wikiHistoryItem{
			ID:             ch.ID,
			Seq:            totalChanges - idx,
			ChangeType:     ch.ChangeType,
			FieldKey:       ch.FieldKey,
			OldValue:       ch.OldValue,
			NewValue:       ch.NewValue,
			ChangedAt:      changedAt,
			ChangedBy:      a.confessionalUserLabel(sql.NullInt64{Int64: int64(changedByID), Valid: changedByID > 0}),
			ChangedByID:    changedByID,
			ModeratedBy:    moderatedBy,
			ModeratedAt:    moderatedAt,
			ModeracioEstat: ch.ModeracioEstat,
			HasSnapshot:    len(before) > 0 || len(after) > 0,
			CanRevert:      false,
		})
	}
	RenderPrivateTemplate(w, r, "wiki-history.html", map[string]interface{}{
		"Title":      entitat.Nom,
		"BackURL":    fmt.Sprintf("/confessional/entitats/%d", id),
		"HistoryURL": fmt.Sprintf("/confessional/entitats/%d/history", id),
		"RevertURL":  "",
		"History":    history,
		"User":       user,
	})
}

func (a *App) AdminDeleteConfessional(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/confessional", http.StatusSeeOther)
		return
	}
	kind := confessionalKind(r.FormValue("kind"))
	id, _ := strconv.Atoi(r.FormValue("id"))
	if kind == "" || id == 0 {
		http.NotFound(w, r)
		return
	}
	section, okSection := confessionalSectionByKind(kind)
	if !okSection {
		http.NotFound(w, r)
		return
	}
	if _, ok := a.requirePermissionKey(w, r, section.DeletePerm, PermissionTarget{}); !ok {
		return
	}
	if confessionalCatalogSection(kind) {
		http.NotFound(w, r)
		return
	}
	var err error
	switch kind {
	case "model":
		err = a.DB.DeleteModelConfessional(id)
	case "entitat":
		err = a.DB.DeleteEntitatReligiosa(id)
	case "relacio":
		err = a.DB.DeleteMunicipiEntitatReligiosa(id)
	case "rel_ent":
		err = a.DB.DeleteEntitatReligiosaRelacio(id)
	}
	if err != nil {
		msg := "delete"
		if errors.Is(err, db.ErrUnsafeDelete) {
			msg = "unsafe_delete"
		}
		http.Redirect(w, r, confessionalSectionURL(section, "error="+msg), http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, confessionalSectionURL(section, "notice=deleted"), http.StatusSeeOther)
}

func (a *App) renderConfessionalForm(w http.ResponseWriter, r *http.Request, user *db.User, data confessionalFormData) {
	var allReligions []db.ReligioConfessio
	var models []db.ModelConfessional
	var allEntitats []db.EntitatReligiosa
	var municipis []db.MunicipiRow
	var paisos []db.Pais
	switch data.Kind {
	case "model":
		allReligions, _ = a.DB.ListReligioConfessions()
		paisos, _ = a.DB.ListPaisos()
	case "relacio":
		allEntitats, _ = a.DB.ListEntitatsReligioses()
		municipis, _ = a.DB.ListMunicipis(db.MunicipiFilter{})
	case "rel_ent":
		allEntitats, _ = a.DB.ListEntitatsReligioses()
	case "entitat":
		allEntitats, _ = a.DB.ListEntitatsReligioses()
		paisos, _ = a.DB.ListPaisos()
		if data.Entitat != nil && data.Entitat.NivellConfessionalCodi != "" {
			_, _, _, _, compatible := ConfessionalLevelCompatibleWithReligion(data.Entitat.ReligioConfessioCodi, data.Entitat.NivellConfessionalCodi)
			if !compatible {
				data.Entitat.NivellConfessionalCodi = ""
			}
		}
	}
	nuclis := a.compatibleNucliRows(municipis, selectedRelacioMunicipiID(data.Relacio))
	lang := ResolveLangForUser(r, user.PreferredLang)
	selectableEntitats := publishedEntitatsReligioses(allEntitats)
	selectedParent := confessionalEntityByID(selectableEntitats, selectedConfessionalParentID(data.Entitat))
	RenderPrivateTemplate(w, r, "admin-confessional-form.html", map[string]interface{}{
		"Section":               confessionalSectionMust(data.Kind),
		"Form":                  data,
		"Religions":             allReligions,
		"SelectableReligions":   ListSelectableConfessionalReligionCatalog(),
		"Models":                models,
		"Nivells":               ListConfessionalLevelCatalog(),
		"SelectableNivells":     ListConfessionalLevelCatalog(),
		"ReligionCatalogLabels": confessionalReligionCatalogLabels(lang),
		"LevelCatalogLabels":    confessionalLevelCatalogLabels(lang),
		"Entitats":              allEntitats,
		"SelectableEntitats":    selectableEntitats,
		"SelectedParent":        selectedParent,
		"ParentOptionAllowed":   confessionalParentOptionAllowedMap(selectableEntitats, data.Entitat),
		"ParentLevelCodesCSV":   confessionalParentLevelCodesCSVMap(),
		"LevelCanHaveChildren":  confessionalLevelCanHaveChildrenMap(),
		"Municipis":             municipis,
		"Nuclis":                nuclis,
		"Paisos":                paisos,
		"CanManageArxius":       a.canManageAnyDocumentalsModular(user),
		"User":                  user,
	})
}

func (a *App) parseConfessionalForm(kind string, id int, r *http.Request, lang string) (confessionalFormData, string) {
	data := confessionalFormData{Kind: kind, IsNew: id == 0, ReturnURL: strings.TrimSpace(r.FormValue("return_to"))}
	estat := normalizeConfessionalEstat(r.FormValue("estat"))
	status := a.confessionalModerationStatusForSave(kind, id)
	switch kind {
	case "model":
		item := &db.ModelConfessional{
			ID:                 id,
			Nom:                strings.TrimSpace(r.FormValue("nom")),
			ReligioConfessioID: sqlNullInt(r.FormValue("religio_confessio_id")),
			PaisID:             sqlNullInt(r.FormValue("pais_id")),
			Descripcio:         strings.TrimSpace(r.FormValue("descripcio")),
			AnyInici:           sqlNullInt(r.FormValue("any_inici")),
			AnyFi:              sqlNullInt(r.FormValue("any_fi")),
			Estat:              estat,
			Observacions:       strings.TrimSpace(r.FormValue("observacions")),
			ModeracioEstat:     status,
		}
		data.Model = item
		if item.Nom == "" {
			return data, "El nom es obligatori."
		}
	case "entitat":
		item := &db.EntitatReligiosa{
			ID:                     id,
			Codi:                   normalizeConfessionalCode(r.FormValue("codi")),
			Nom:                    strings.TrimSpace(r.FormValue("nom")),
			ReligioConfessioCodi:   normalizeConfessionalCode(r.FormValue("religio_confessio_codi")),
			NivellConfessionalCodi: normalizeConfessionalCode(r.FormValue("nivell_confessional_codi")),
			ParentID:               sqlNullInt(r.FormValue("parent_id")),
			PaisID:                 sqlNullInt(r.FormValue("pais_id")),
			AnyInici:               sqlNullInt(r.FormValue("any_inici")),
			AnyFi:                  sqlNullInt(r.FormValue("any_fi")),
			Estat:                  estat,
			Web:                    strings.TrimSpace(r.FormValue("web")),
			WebWikipedia:           strings.TrimSpace(r.FormValue("web_wikipedia")),
			Descripcio:             strings.TrimSpace(r.FormValue("descripcio")),
			Observacions:           strings.TrimSpace(r.FormValue("observacions")),
			ModeracioEstat:         status,
		}
		data.Entitat = item
		if item.Nom == "" {
			return data, "El nom es obligatori."
		}
		if item.Codi == "" {
			return data, "El codi es obligatori."
		}
		if item.ReligioConfessioCodi == "" {
			return data, "Cal indicar la religio/confessio."
		}
		if item.NivellConfessionalCodi == "" {
			return data, "Cal indicar el nivell confessional."
		}
		_, _, religionOK, levelOK, compatible := ConfessionalLevelCompatibleWithReligion(item.ReligioConfessioCodi, item.NivellConfessionalCodi)
		if !religionOK {
			return data, T(lang, "confessional.error.religion_unknown")
		}
		if !levelOK {
			return data, T(lang, "confessional.error.level_unknown")
		}
		if !compatible {
			return data, T(lang, "confessional.error.level_incompatible")
		}
		if item.ParentID.Valid {
			if id > 0 && item.ParentID.Int64 == int64(id) {
				return data, "L'entitat pare i la filla no poden ser la mateixa."
			}
			parent, err := a.DB.GetEntitatReligiosa(int(item.ParentID.Int64))
			if err != nil || parent == nil {
				return data, "L'entitat pare indicada no existeix."
			}
			if parent.ModeracioEstat != "publicat" {
				return data, "L'entitat pare indicada no esta publicada."
			}
			if parent.ReligioConfessioCodi != "" && parent.ReligioConfessioCodi != item.ReligioConfessioCodi {
				return data, "L'entitat pare ha de compartir religio/confessio."
			}
			if err := validateConfessionalEntityRelation(parent, item); err != nil {
				return data, confessionalRelationErrorMessage(lang, err)
			}
			if id > 0 {
				rels, err := a.DB.ListEntitatReligiosaRelacions()
				if err != nil {
					return data, "No s'ha pogut validar la jerarquia religiosa."
				}
				if confessionalWouldCreateCycle(id, int(item.ParentID.Int64), rels) {
					return data, confessionalRelationErrorMessage(lang, errConfessionalParentCycle)
				}
			}
		}
	case "relacio":
		municipiID, _ := strconv.Atoi(r.FormValue("municipi_id"))
		entitatID, _ := strconv.Atoi(r.FormValue("entitat_religiosa_id"))
		item := &db.MunicipiEntitatReligiosa{
			ID:                 id,
			MunicipiID:         municipiID,
			NucliID:            sqlNullInt(r.FormValue("nucli_id")),
			EntitatReligiosaID: entitatID,
			TipusRelacio:       "",
			AnyInici:           sqlNullInt(r.FormValue("any_inici")),
			AnyFi:              sqlNullInt(r.FormValue("any_fi")),
			Observacions:       strings.TrimSpace(r.FormValue("observacions")),
			ModeracioEstat:     status,
		}
		data.Relacio = item
		if item.MunicipiID == 0 {
			return data, "Cal indicar el municipi."
		}
		if _, err := a.DB.GetMunicipi(item.MunicipiID); err != nil {
			return data, "El municipi indicat no existeix."
		}
		if item.NucliID.Valid {
			if item.NucliID.Int64 == int64(item.MunicipiID) {
				return data, "El nucli no pot ser el mateix registre que el municipi."
			}
			nucli, err := a.DB.GetMunicipi(int(item.NucliID.Int64))
			if err != nil || nucli == nil {
				return data, "El nucli indicat no existeix."
			}
			if !nucli.MunicipiID.Valid || nucli.MunicipiID.Int64 != int64(item.MunicipiID) {
				return data, "El nucli indicat no pertany al municipi seleccionat."
			}
		}
		if item.EntitatReligiosaID == 0 {
			return data, "Cal indicar l'entitat religiosa."
		}
		entitat, err := a.DB.GetEntitatReligiosa(item.EntitatReligiosaID)
		if err != nil || entitat == nil {
			return data, "L'entitat religiosa indicada no existeix."
		}
		if entitat.ModeracioEstat != "publicat" {
			return data, "L'entitat religiosa indicada no esta publicada."
		}
		item.TipusRelacio = suggestConfessionalRelationType(entitat.NivellConfessionalCodi)
	case "rel_ent":
		origenID, _ := strconv.Atoi(r.FormValue("entitat_origen_id"))
		destiID, _ := strconv.Atoi(r.FormValue("entitat_desti_id"))
		item := &db.EntitatReligiosaRelacio{
			ID:              id,
			EntitatOrigenID: origenID,
			EntitatDestiID:  destiID,
			TipusRelacio:    "",
			AnyInici:        sqlNullInt(r.FormValue("any_inici")),
			AnyFi:           sqlNullInt(r.FormValue("any_fi")),
			Observacions:    strings.TrimSpace(r.FormValue("observacions")),
			ModeracioEstat:  status,
		}
		data.RelEnt = item
		if item.EntitatOrigenID == 0 {
			return data, "Cal indicar l'entitat pare."
		}
		if item.EntitatDestiID == 0 {
			return data, "Cal indicar l'entitat filla."
		}
		if item.EntitatOrigenID == item.EntitatDestiID {
			return data, "L'entitat pare i la filla no poden ser la mateixa."
		}
		parent, err := a.DB.GetEntitatReligiosa(item.EntitatOrigenID)
		if err != nil || parent == nil {
			return data, "L'entitat pare indicada no existeix."
		}
		child, err := a.DB.GetEntitatReligiosa(item.EntitatDestiID)
		if err != nil || child == nil {
			return data, "L'entitat filla indicada no existeix."
		}
		if parent.ModeracioEstat != "publicat" || child.ModeracioEstat != "publicat" {
			return data, "Les entitats de la relacio han d'estar publicades."
		}
		if parent.ReligioConfessioCodi != "" && child.ReligioConfessioCodi != "" && parent.ReligioConfessioCodi != child.ReligioConfessioCodi {
			return data, "Les entitats de la relacio han de compartir religio/confessio."
		}
		if err := validateConfessionalEntityRelation(parent, child); err != nil {
			return data, confessionalRelationErrorMessage(lang, err)
		}
		item.TipusRelacio = suggestConfessionalRelationType(child.NivellConfessionalCodi)
	}
	return data, ""
}

func selectedRelacioMunicipiID(rel *db.MunicipiEntitatReligiosa) int {
	if rel == nil {
		return 0
	}
	return rel.MunicipiID
}

func (a *App) compatibleNucliRows(municipis []db.MunicipiRow, municipiID int) []db.MunicipiRow {
	if municipiID == 0 {
		return nil
	}
	nuclis := make([]db.MunicipiRow, 0)
	for _, row := range municipis {
		if row.ID == municipiID {
			continue
		}
		full, err := a.DB.GetMunicipi(row.ID)
		if err != nil || full == nil || !full.MunicipiID.Valid || full.MunicipiID.Int64 != int64(municipiID) {
			continue
		}
		nuclis = append(nuclis, row)
	}
	return nuclis
}

func parseConfessionalEntityListFilter(r *http.Request) confessionalEntityListFilter {
	q := r.URL.Query()
	text := normalizeConfessionalSearchText(q.Get("q"))
	if text == "" {
		text = normalizeConfessionalSearchText(strings.Join([]string{q.Get("f_nom"), q.Get("f_codi"), q.Get("f_parent")}, " "))
	}
	filter := confessionalEntityListFilter{
		ReligionCode: normalizeCatalogCode(q.Get("religio_confessio_codi")),
		LevelCode:    normalizeCatalogCode(q.Get("nivell_confessional_codi")),
		ParentMode:   normalizeConfessionalParentMode(q.Get("parent_mode")),
		Text:         text,
		Status:       "publicat",
		Sort:         normalizeConfessionalHierarchySort(q.Get("sort")),
		SortDir:      normalizeConfessionalSortDir(q.Get("dir")),
		Page:         parseListPage(q.Get("page")),
		PerPage:      parseListPerPage(q.Get("per_page")),
	}
	if _, ok := GetConfessionalReligionCatalogByCode(filter.ReligionCode); filter.ReligionCode != "" && !ok {
		filter.ReligionCode = ""
	}
	if _, ok := GetConfessionalLevelCatalogByCode(filter.LevelCode); filter.LevelCode != "" && !ok {
		filter.LevelCode = ""
	}
	if filter.ReligionCode != "" && filter.LevelCode != "" {
		_, _, _, _, compatible := ConfessionalLevelCompatibleWithReligion(filter.ReligionCode, filter.LevelCode)
		if !compatible {
			filter.LevelCode = ""
		}
	}
	filter.ParentID = parsePositiveIntDefault(q.Get("parent_id"), 0, 0, 1000000000)
	return filter
}

func confessionalFilterValues(r *http.Request) map[string]string {
	q := r.URL.Query()
	return map[string]string{
		"nom":     normalizeConfessionalSearchText(q.Get("f_nom")),
		"codi":    normalizeConfessionalSearchText(q.Get("f_codi")),
		"religio": normalizeConfessionalSearchText(q.Get("f_religio")),
		"nivell":  normalizeConfessionalSearchText(q.Get("f_nivell")),
		"parent":  normalizeConfessionalSearchText(q.Get("f_parent")),
		"status":  normalizeConfessionalSearchText(q.Get("f_status")),
	}
}

func normalizeConfessionalParentMode(raw string) string {
	switch strings.TrimSpace(raw) {
	case "all":
		return "all"
	case "descendants":
		return "descendants"
	default:
		return "direct"
	}
}

func normalizeConfessionalNavigationParentMode(raw string) string {
	mode := normalizeConfessionalParentMode(raw)
	if mode == "all" {
		return "direct"
	}
	return mode
}

func normalizeConfessionalHierarchySort(raw string) string {
	switch strings.TrimSpace(raw) {
	case "code", "level", "children":
		return strings.TrimSpace(raw)
	default:
		return "path"
	}
}

func normalizeConfessionalSortDir(raw string) string {
	if strings.EqualFold(strings.TrimSpace(raw), "desc") {
		return "desc"
	}
	return "asc"
}

func normalizeConfessionalSearchText(raw string) string {
	text := strings.TrimSpace(raw)
	runes := []rune(text)
	if len(runes) > 80 {
		text = string(runes[:80])
	}
	return text
}

func parsePositiveIntDefault(raw string, fallback, min, max int) int {
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return fallback
	}
	if n < min {
		return fallback
	}
	if n > max {
		return max
	}
	return n
}

func filterConfessionalEntitats(all []db.EntitatReligiosa, rels []db.EntitatReligiosaRelacio, filter *confessionalEntityListFilter) []db.EntitatReligiosa {
	publishedParentIDs := map[int]bool{}
	for _, rel := range rels {
		if rel.ModeracioEstat == "publicat" && rel.EntitatOrigenID == filter.ParentID {
			publishedParentIDs[rel.EntitatDestiID] = true
		}
	}
	needle := strings.ToLower(filter.Text)
	out := make([]db.EntitatReligiosa, 0, len(all))
	for _, item := range all {
		if item.ModeracioEstat != filter.Status {
			continue
		}
		if filter.ReligionCode != "" && item.ReligioConfessioCodi != filter.ReligionCode {
			continue
		}
		if filter.LevelCode != "" && item.NivellConfessionalCodi != filter.LevelCode {
			continue
		}
		if filter.ParentID > 0 && !publishedParentIDs[item.ID] {
			continue
		}
		if needle != "" {
			name := strings.ToLower(item.Nom)
			code := strings.ToLower(item.Codi)
			if !strings.Contains(name, needle) && !strings.Contains(code, needle) {
				continue
			}
		}
		out = append(out, item)
	}
	filter.Total = len(out)
	filter.TotalPages = 1
	if filter.PerPage <= 0 {
		filter.PerPage = parseListPerPage("")
	}
	if filter.PerPage > 0 {
		filter.TotalPages = (filter.Total + filter.PerPage - 1) / filter.PerPage
		if filter.TotalPages == 0 {
			filter.TotalPages = 1
		}
	}
	if filter.Page > filter.TotalPages {
		filter.Page = filter.TotalPages
	}
	if filter.Page <= 0 {
		filter.Page = 1
	}
	start := (filter.Page - 1) * filter.PerPage
	if start >= len(out) {
		return []db.EntitatReligiosa{}
	}
	end := start + filter.PerPage
	if end > len(out) {
		end = len(out)
	}
	return out[start:end]
}

func filterConfessionalHierarchyRows(all []db.EntitatReligiosa, rels []db.EntitatReligiosaRelacio, territorials []db.MunicipiEntitatReligiosa, filter *confessionalEntityListFilter, lang string) ([]confessionalHierarchyRow, []confessionalHierarchyPathItem) {
	byID, parentByChild, childrenByParent := confessionalHierarchyMaps(all, rels)
	childCounts := map[int]int{}
	for parentID, children := range childrenByParent {
		childCounts[parentID] = len(children)
	}
	territoryCounts := map[int]int{}
	for _, rel := range territorials {
		territoryCounts[rel.EntitatReligiosaID]++
	}
	parentPath := []confessionalHierarchyPathItem{}
	if filter.ParentID > 0 {
		if parent, ok := byID[filter.ParentID]; ok {
			parentPath = confessionalEntityPath(parent.ID, byID, parentByChild, lang)
		}
	}
	descendants := map[int]bool{}
	if filter.ParentID > 0 && filter.ParentMode == "descendants" {
		descendants = confessionalDescendantSet(filter.ParentID, childrenByParent)
	}

	rows := make([]confessionalHierarchyRow, 0, len(all))
	needle := normalizeConfessionalSearchComparable(filter.Text)
	for _, item := range all {
		if item.ModeracioEstat != "publicat" {
			continue
		}
		parentID := parentByChild[item.ID]
		if filter.ReligionCode != "" && item.ReligioConfessioCodi != filter.ReligionCode {
			continue
		}
		if filter.LevelCode != "" && item.NivellConfessionalCodi != filter.LevelCode {
			continue
		}
		if filter.ParentID > 0 {
			if filter.ParentMode == "descendants" {
				if !descendants[item.ID] {
					continue
				}
			} else if filter.ParentMode == "direct" && parentID != filter.ParentID {
				continue
			} else if filter.ParentMode == "all" && item.ID != filter.ParentID && parentID != filter.ParentID {
				continue
			}
		}
		path := confessionalEntityPath(item.ID, byID, parentByChild, lang)
		if needle != "" && !confessionalHierarchyMatchesText(item, path, needle) {
			continue
		}
		parentName := ""
		if parentID > 0 {
			if parent, ok := byID[parentID]; ok {
				parentName = parent.Nom
			}
		}
		rows = append(rows, confessionalHierarchyRow{
			Entitat:        item,
			ParentID:       parentID,
			ParentName:     parentName,
			Path:           path,
			ChildCount:     childCounts[item.ID],
			TerritoryCount: territoryCounts[item.ID],
			Depth:          len(path) - 1,
		})
	}
	sortConfessionalHierarchyRows(rows, filter)
	filter.Total = len(rows)
	filter.TotalPages = 1
	if filter.PerPage <= 0 {
		filter.PerPage = parseListPerPage("")
	}
	if filter.PerPage > 0 {
		filter.TotalPages = (filter.Total + filter.PerPage - 1) / filter.PerPage
		if filter.TotalPages == 0 {
			filter.TotalPages = 1
		}
	}
	if filter.Page <= 0 {
		filter.Page = 1
	}
	if filter.Page > filter.TotalPages {
		filter.Page = filter.TotalPages
	}
	start := (filter.Page - 1) * filter.PerPage
	if start >= len(rows) {
		return []confessionalHierarchyRow{}, parentPath
	}
	end := start + filter.PerPage
	if end > len(rows) {
		end = len(rows)
	}
	return rows[start:end], parentPath
}

func confessionalHierarchyMaps(all []db.EntitatReligiosa, rels []db.EntitatReligiosaRelacio) (map[int]db.EntitatReligiosa, map[int]int, map[int][]int) {
	byID := make(map[int]db.EntitatReligiosa, len(all))
	for _, item := range all {
		byID[item.ID] = item
	}
	parentByChild := map[int]int{}
	childrenByParent := map[int][]int{}
	for _, rel := range rels {
		if rel.ModeracioEstat != "publicat" {
			continue
		}
		if _, ok := byID[rel.EntitatOrigenID]; !ok {
			continue
		}
		if _, ok := byID[rel.EntitatDestiID]; !ok {
			continue
		}
		if _, exists := parentByChild[rel.EntitatDestiID]; exists {
			continue
		}
		parentByChild[rel.EntitatDestiID] = rel.EntitatOrigenID
		childrenByParent[rel.EntitatOrigenID] = append(childrenByParent[rel.EntitatOrigenID], rel.EntitatDestiID)
	}
	return byID, parentByChild, childrenByParent
}

func confessionalEntityPath(id int, byID map[int]db.EntitatReligiosa, parentByChild map[int]int, lang string) []confessionalHierarchyPathItem {
	path := []confessionalHierarchyPathItem{}
	seen := map[int]bool{}
	current := id
	for current > 0 {
		if seen[current] {
			break
		}
		seen[current] = true
		item, ok := byID[current]
		if !ok {
			break
		}
		levelLabel := item.NivellConfessionalCodi
		if level, ok := GetConfessionalLevelCatalogByCode(item.NivellConfessionalCodi); ok {
			levelLabel = ConfessionalLevelLabel(level, lang)
		}
		path = append([]confessionalHierarchyPathItem{{ID: item.ID, Name: item.Nom, Level: levelLabel}}, path...)
		current = parentByChild[current]
	}
	return path
}

func confessionalDescendantSet(parentID int, childrenByParent map[int][]int) map[int]bool {
	out := map[int]bool{}
	stack := append([]int{}, childrenByParent[parentID]...)
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if out[id] {
			continue
		}
		out[id] = true
		stack = append(stack, childrenByParent[id]...)
	}
	return out
}

func normalizeConfessionalSearchComparable(raw string) string {
	replacer := strings.NewReplacer(":", " ", ";", " ", ",", " ", ".", " ", "'", " ", "\"", " ")
	return strings.Join(strings.Fields(strings.ToLower(replacer.Replace(raw))), " ")
}

func confessionalHierarchyMatchesText(item db.EntitatReligiosa, path []confessionalHierarchyPathItem, needle string) bool {
	if needle == "" {
		return true
	}
	hayParts := []string{item.Nom, item.Codi}
	for _, part := range path {
		hayParts = append(hayParts, part.Name, part.Level)
	}
	hay := normalizeConfessionalSearchComparable(strings.Join(hayParts, " "))
	for _, token := range strings.Fields(needle) {
		if !strings.Contains(hay, token) {
			return false
		}
	}
	return true
}

func sortConfessionalHierarchyRows(rows []confessionalHierarchyRow, filter *confessionalEntityListFilter) {
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		cmp := 0
		switch filter.Sort {
		case "code":
			cmp = strings.Compare(strings.ToLower(a.Entitat.Codi), strings.ToLower(b.Entitat.Codi))
		case "level":
			cmp = strings.Compare(strings.ToLower(a.Entitat.NivellConfessionalCodi), strings.ToLower(b.Entitat.NivellConfessionalCodi))
		case "children":
			cmp = a.ChildCount - b.ChildCount
		default:
			cmp = strings.Compare(strings.ToLower(confessionalPathSortKey(a.Path)), strings.ToLower(confessionalPathSortKey(b.Path)))
		}
		if cmp == 0 {
			cmp = strings.Compare(strings.ToLower(a.Entitat.Nom), strings.ToLower(b.Entitat.Nom))
		}
		if filter.SortDir == "desc" {
			return cmp > 0
		}
		return cmp < 0
	})
}

func confessionalPathSortKey(path []confessionalHierarchyPathItem) string {
	parts := make([]string, 0, len(path))
	for _, item := range path {
		parts = append(parts, item.Name)
	}
	return strings.Join(parts, " / ")
}

func confessionalHierarchyParentOptions(all []db.EntitatReligiosa, filter *confessionalEntityListFilter) []db.EntitatReligiosa {
	out := make([]db.EntitatReligiosa, 0, len(all))
	for _, item := range all {
		if item.ModeracioEstat != "publicat" {
			continue
		}
		if filter.ReligionCode != "" && item.ReligioConfessioCodi != filter.ReligionCode {
			continue
		}
		if filter.LevelCode != "" && !ConfessionalParentLevelCompatible(item.NivellConfessionalCodi, filter.LevelCode) {
			continue
		}
		if filter.LevelCode == "" {
			level, ok := GetConfessionalLevelCatalogByCode(item.NivellConfessionalCodi)
			if !ok || !level.CanHaveChildren {
				continue
			}
		}
		out = append(out, item)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return strings.ToLower(out[i].Nom) < strings.ToLower(out[j].Nom)
	})
	return out
}

func confessionalManagementParentOptions(all []db.EntitatReligiosa) []db.EntitatReligiosa {
	return confessionalManagementParentOptionsWithRelations(all, nil)
}

func confessionalManagementParentOptionsWithRelations(all []db.EntitatReligiosa, rels []db.EntitatReligiosaRelacio) []db.EntitatReligiosa {
	out := make([]db.EntitatReligiosa, 0, len(all))
	_, parentByChild, _ := confessionalHierarchyMaps(all, rels)
	for _, item := range all {
		if item.ModeracioEstat != "publicat" {
			continue
		}
		level, ok := GetConfessionalLevelCatalogByCode(item.NivellConfessionalCodi)
		if !ok || !level.CanHaveChildren {
			continue
		}
		if parentByChild[item.ID] != 0 {
			continue
		}
		out = append(out, item)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return strings.ToLower(out[i].Nom) < strings.ToLower(out[j].Nom)
	})
	return out
}

func confessionalParentOptionAllowedMap(candidates []db.EntitatReligiosa, child *db.EntitatReligiosa) map[int]bool {
	out := map[int]bool{}
	if child == nil || child.ReligioConfessioCodi == "" || child.NivellConfessionalCodi == "" {
		return out
	}
	for _, parent := range candidates {
		if child.ID != 0 && parent.ID == child.ID {
			continue
		}
		parentCopy := parent
		if validateConfessionalEntityRelation(&parentCopy, child) == nil {
			out[parent.ID] = true
		}
	}
	return out
}

func confessionalEntityByID(items []db.EntitatReligiosa, id int) *db.EntitatReligiosa {
	if id <= 0 {
		return nil
	}
	for _, item := range items {
		if item.ID == id {
			copy := item
			return &copy
		}
	}
	return nil
}

func confessionalParentLevelCodesCSVMap() map[string]string {
	out := map[string]string{}
	for _, level := range ListConfessionalLevelCatalog() {
		out[level.Code] = ConfessionalAllowedParentLevelCodesCSV(level)
	}
	return out
}

func confessionalLevelCanHaveChildrenMap() map[string]bool {
	out := map[string]bool{}
	for _, level := range ListConfessionalLevelCatalog() {
		out[level.Code] = level.CanHaveChildren
	}
	return out
}

func (a *App) confessionalModerationStatusForSave(kind string, id int) string {
	if id == 0 {
		return "pendent"
	}
	var status string
	switch kind {
	case "model":
		if item, err := a.DB.GetModelConfessional(id); err == nil && item != nil {
			status = item.ModeracioEstat
		}
	case "entitat":
		if item, err := a.DB.GetEntitatReligiosa(id); err == nil && item != nil {
			status = item.ModeracioEstat
		}
	case "relacio":
		if item, err := a.DB.GetMunicipiEntitatReligiosa(id); err == nil && item != nil {
			status = item.ModeracioEstat
		}
	case "rel_ent":
		if item, err := a.DB.GetEntitatReligiosaRelacio(id); err == nil && item != nil {
			status = item.ModeracioEstat
		}
	}
	return normalizeModerationStatus(status)
}

func (a *App) saveConfessionalData(kind string, data confessionalFormData) (int, error) {
	switch kind {
	case "model":
		return a.DB.SaveModelConfessional(data.Model)
	case "entitat":
		return a.DB.SaveEntitatReligiosa(data.Entitat)
	case "relacio":
		return a.DB.SaveMunicipiEntitatReligiosa(data.Relacio)
	case "rel_ent":
		return a.DB.SaveEntitatReligiosaRelacio(data.RelEnt)
	default:
		return 0, errors.New("tipus confessional no valid")
	}
}

func (a *App) createConfessionalParentRelation(entitat *db.EntitatReligiosa, childID, userID int) error {
	if entitat == nil || childID == 0 || !entitat.ParentID.Valid {
		return nil
	}
	needsRelation, err := a.needsConfessionalParentRelation(int(entitat.ParentID.Int64), childID)
	if err != nil {
		return err
	}
	if !needsRelation {
		return nil
	}
	parent, err := a.DB.GetEntitatReligiosa(int(entitat.ParentID.Int64))
	if err != nil || parent == nil {
		if err != nil {
			return err
		}
		return errors.New("entitat pare inexistent")
	}
	if err := validateConfessionalEntityRelation(parent, entitat); err != nil {
		return err
	}
	_, err = a.DB.SaveEntitatReligiosaRelacio(&db.EntitatReligiosaRelacio{
		EntitatOrigenID: int(entitat.ParentID.Int64),
		EntitatDestiID:  childID,
		TipusRelacio:    suggestConfessionalRelationType(entitat.NivellConfessionalCodi),
		AnyInici:        entitat.AnyInici,
		AnyFi:           entitat.AnyFi,
		ModeracioEstat:  "pendent",
		CreatedBy:       sqlNullIntFromInt(userID),
		UpdatedBy:       sqlNullIntFromInt(userID),
	})
	return err
}

func (a *App) needsConfessionalParentRelation(parentID, childID int) (bool, error) {
	if parentID <= 0 || childID <= 0 {
		return false, nil
	}
	rels, err := a.DB.ListEntitatReligiosaRelacions()
	if err != nil {
		return false, err
	}
	for _, rel := range rels {
		if rel.EntitatOrigenID == parentID && rel.EntitatDestiID == childID && rel.ModeracioEstat != "rebutjat" {
			return false, nil
		}
	}
	return true, nil
}

func confessionalWouldCreateCycle(childID, proposedParentID int, rels []db.EntitatReligiosaRelacio) bool {
	if childID <= 0 || proposedParentID <= 0 || childID == proposedParentID {
		return childID == proposedParentID && childID > 0
	}
	childrenByParent := map[int][]int{}
	for _, rel := range rels {
		if rel.ModeracioEstat == "rebutjat" {
			continue
		}
		childrenByParent[rel.EntitatOrigenID] = append(childrenByParent[rel.EntitatOrigenID], rel.EntitatDestiID)
	}
	descendants := confessionalDescendantSet(childID, childrenByParent)
	return descendants[proposedParentID]
}

func validateConfessionalEntityRelation(parent, child *db.EntitatReligiosa) error {
	if parent == nil || child == nil {
		return errors.New("Cal indicar entitat pare i entitat filla.")
	}
	if parent.ID != 0 && child.ID != 0 && parent.ID == child.ID {
		return errors.New("L'entitat pare i la filla no poden ser la mateixa.")
	}
	if parent.ReligioConfessioCodi != "" && child.ReligioConfessioCodi != "" && parent.ReligioConfessioCodi != child.ReligioConfessioCodi {
		return errors.New("Les entitats de la relacio han de compartir religio/confessio.")
	}
	parentLevel, parentOK := GetConfessionalLevelCatalogByCode(parent.NivellConfessionalCodi)
	childLevel, childOK := GetConfessionalLevelCatalogByCode(child.NivellConfessionalCodi)
	if parentOK && childOK {
		if !parentLevel.CanHaveChildren {
			return errors.New("El nivell de l'entitat pare no admet entitats filles.")
		}
		if parentLevel.ReligionCode != "" && childLevel.ReligionCode != "" && parentLevel.ReligionCode != childLevel.ReligionCode {
			return errors.New("Els nivells de la relacio han de pertanyer a la mateixa religio/confessio.")
		}
		if !ConfessionalParentLevelCompatible(parentLevel.Code, childLevel.Code) {
			return errConfessionalParentLevelIncompatible
		}
	}
	return nil
}

func confessionalRelationErrorMessage(lang string, err error) string {
	if errors.Is(err, errConfessionalParentLevelIncompatible) {
		return T(lang, "confessional.error.parent_level_incompatible")
	}
	if errors.Is(err, errConfessionalParentCycle) {
		return T(lang, "confessional.error.parent_cycle")
	}
	return err.Error()
}

func selectedConfessionalParentID(item *db.EntitatReligiosa) int {
	if item == nil || !item.ParentID.Valid {
		return 0
	}
	return int(item.ParentID.Int64)
}

func (a *App) AdminConfessionalEntitiesSuggestJSON(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriConfessionalEntitatsView, PermissionTarget{})
	if !ok {
		return
	}
	values, err := suggestRequestValues(r)
	if err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	lang := ResolveLangForUser(r, user.PreferredLang)
	filter := parseConfessionalEntitySuggestFilter(values)
	allEntitats, err := a.DB.ListEntitatsReligioses()
	if err != nil {
		Errorf("AdminConfessionalEntitiesSuggestJSON ListEntitatsReligioses: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	allRels, err := a.DB.ListEntitatReligiosaRelacions()
	if err != nil {
		Errorf("AdminConfessionalEntitiesSuggestJSON ListEntitatReligiosaRelacions: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	publishedEntitats := publishedEntitatsReligioses(allEntitats)
	publishedRels := publishedEntitatReligiosaRelacions(allRels)
	entitiesByID, parentByChild, childrenByParent := confessionalHierarchyMaps(publishedEntitats, publishedRels)
	blockedParentIDs := map[int]bool{}
	if filter.ChildID > 0 {
		blockedParentIDs = confessionalDescendantSet(filter.ChildID, childrenByParent)
		blockedParentIDs[filter.ChildID] = true
	}
	suggestions := make([]confessionalEntitySuggestion, 0, len(publishedEntitats))
	for _, item := range publishedEntitats {
		if !confessionalSuggestEntityEligible(item, filter, blockedParentIDs, parentByChild) {
			continue
		}
		suggestion, ok := buildConfessionalEntitySuggestion(item, filter, entitiesByID, parentByChild, lang)
		if !ok {
			continue
		}
		suggestions = append(suggestions, suggestion)
	}
	sort.SliceStable(suggestions, func(i, j int) bool {
		if suggestions[i].score != suggestions[j].score {
			return suggestions[i].score > suggestions[j].score
		}
		return suggestions[i].label < suggestions[j].label
	})
	items := make([]map[string]interface{}, 0, filter.Limit)
	for _, suggestion := range suggestions {
		items = append(items, suggestion.item)
		if len(items) >= filter.Limit {
			break
		}
	}
	writeJSON(w, map[string]interface{}{"items": items})
}

func parseConfessionalEntitySuggestFilter(values map[string]string) confessionalEntitySuggestFilter {
	return confessionalEntitySuggestFilter{
		Query:        normalizeConfessionalSearchText(suggestValue(values, "q")),
		Limit:        parseSuggestLimit(suggestValue(values, "limit")),
		Scope:        strings.TrimSpace(suggestValue(values, "scope")),
		ReligionCode: normalizeCatalogCode(suggestValue(values, "religio_confessio_codi")),
		LevelCode:    normalizeCatalogCode(suggestValue(values, "nivell_confessional_codi")),
		ExcludeID:    parsePositiveIntDefault(suggestValue(values, "exclude_id"), 0, 0, 1000000000),
		ChildID:      parsePositiveIntDefault(suggestValue(values, "child_id"), 0, 0, 1000000000),
	}
}

func confessionalSuggestMatchScore(queryLower, label string) int {
	clean := strings.ToLower(strings.TrimSpace(label))
	if clean == "" {
		return 0
	}
	if queryLower == "" {
		return 1
	}
	if clean == queryLower {
		return 4
	}
	if strings.HasPrefix(clean, queryLower) {
		return 3
	}
	if strings.Contains(clean, queryLower) {
		return 2
	}
	return 0
}

func confessionalSuggestEntityEligible(
	item db.EntitatReligiosa,
	filter confessionalEntitySuggestFilter,
	blockedParentIDs map[int]bool,
	parentByChild map[int]int,
) bool {
	if filter.ExcludeID > 0 && item.ID == filter.ExcludeID {
		return false
	}
	if blockedParentIDs[item.ID] {
		return false
	}
	if filter.ReligionCode != "" && item.ReligioConfessioCodi != filter.ReligionCode {
		return false
	}
	level, ok := GetConfessionalLevelCatalogByCode(item.NivellConfessionalCodi)
	if !ok || !level.CanHaveChildren {
		return false
	}
	if filter.LevelCode != "" && !ConfessionalParentLevelCompatible(item.NivellConfessionalCodi, filter.LevelCode) {
		return false
	}
	if filter.Scope == "roots" && parentByChild[item.ID] != 0 {
		return false
	}
	return true
}

func buildConfessionalEntitySuggestion(
	item db.EntitatReligiosa,
	filter confessionalEntitySuggestFilter,
	entitiesByID map[int]db.EntitatReligiosa,
	parentByChild map[int]int,
	lang string,
) (confessionalEntitySuggestion, bool) {
	path := confessionalEntityPath(item.ID, entitiesByID, parentByChild, lang)
	label := strings.TrimSpace(item.Nom)
	score := confessionalSuggestMatchScore(strings.ToLower(filter.Query), label+" "+item.Codi+" "+confessionalPathSortKey(path))
	if score == 0 {
		return confessionalEntitySuggestion{}, false
	}
	contextParts := []string{}
	if item.Codi != "" {
		contextParts = append(contextParts, item.Codi)
	}
	if len(path) > 1 {
		contextParts = append(contextParts, confessionalPathSortKey(path[:len(path)-1]))
	}
	contextParts = append(contextParts, firstNonEmpty(confessionalLevelLabel(item.NivellConfessionalCodi, lang), item.NivellConfessionalCodi))
	return confessionalEntitySuggestion{
		label: strings.ToLower(label),
		score: score,
		item: map[string]interface{}{
			"id":                  item.ID,
			"nom":                 label,
			"context":             strings.Join(contextParts, " / "),
			"religio_confessio":   item.ReligioConfessioCodi,
			"nivell_confessional": item.NivellConfessionalCodi,
		},
	}, true
}

func confessionalEntitiesMap(items []db.EntitatReligiosa) map[int]db.EntitatReligiosa {
	out := make(map[int]db.EntitatReligiosa, len(items))
	for _, item := range items {
		out[item.ID] = item
	}
	return out
}

func confessionalLevelLabel(code, lang string) string {
	level, ok := GetConfessionalLevelCatalogByCode(code)
	if !ok {
		return ""
	}
	return ConfessionalLevelLabel(level, lang)
}

func (a *App) applyConfessionalAuthorship(kind string, id, userID int, data *confessionalFormData) error {
	author := sqlNullIntFromInt(userID)
	switch kind {
	case "entitat":
		if data.Entitat == nil {
			return nil
		}
		data.Entitat.UpdatedBy = author
		if id == 0 {
			data.Entitat.CreatedBy = author
			return nil
		}
		current, err := a.DB.GetEntitatReligiosa(id)
		if err != nil || current == nil {
			return err
		}
		data.Entitat.CreatedBy = current.CreatedBy
		data.Entitat.ModeratedBy = current.ModeratedBy
		data.Entitat.ModeratedAt = current.ModeratedAt
		data.Entitat.ModeracioMotiu = current.ModeracioMotiu
	case "relacio":
		if data.Relacio == nil {
			return nil
		}
		data.Relacio.UpdatedBy = author
		if id == 0 {
			data.Relacio.CreatedBy = author
			return nil
		}
		current, err := a.DB.GetMunicipiEntitatReligiosa(id)
		if err != nil || current == nil {
			return err
		}
		data.Relacio.CreatedBy = current.CreatedBy
		data.Relacio.ModeratedBy = current.ModeratedBy
		data.Relacio.ModeratedAt = current.ModeratedAt
		data.Relacio.ModeracioMotiu = current.ModeracioMotiu
	case "rel_ent":
		if data.RelEnt == nil {
			return nil
		}
		data.RelEnt.UpdatedBy = author
		if id == 0 {
			data.RelEnt.CreatedBy = author
			return nil
		}
		current, err := a.DB.GetEntitatReligiosaRelacio(id)
		if err != nil || current == nil {
			return err
		}
		data.RelEnt.CreatedBy = current.CreatedBy
		data.RelEnt.ModeratedBy = current.ModeratedBy
		data.RelEnt.ModeratedAt = current.ModeratedAt
		data.RelEnt.ModeracioMotiu = current.ModeracioMotiu
	}
	return nil
}

func (a *App) createEntitatReligiosaWikiProposal(after *db.EntitatReligiosa, userID int) (bool, error) {
	if after == nil || after.ID == 0 {
		return false, nil
	}
	before, err := a.DB.GetEntitatReligiosa(after.ID)
	if err != nil || before == nil {
		return false, err
	}
	if before.ModeracioEstat != "publicat" {
		return false, nil
	}
	next := *after
	next.ID = before.ID
	next.CreatedBy = before.CreatedBy
	next.ModeracioEstat = "publicat"
	next.ModeracioMotiu = before.ModeracioMotiu
	next.ModeratedBy = before.ModeratedBy
	next.ModeratedAt = before.ModeratedAt
	if !next.UpdatedBy.Valid {
		next.UpdatedBy = sqlNullIntFromInt(userID)
	}
	beforeJSON, err := json.Marshal(before)
	if err != nil {
		return false, err
	}
	afterJSON, err := json.Marshal(next)
	if err != nil {
		return false, err
	}
	metadata, err := buildWikiChangeMetadata(beforeJSON, afterJSON, 0)
	if err != nil {
		return false, err
	}
	_, err = a.createWikiChange(&db.WikiChange{
		ObjectType:     "entitat_religiosa",
		ObjectID:       before.ID,
		ChangeType:     "update",
		FieldKey:       "*",
		OldValue:       before.Nom,
		NewValue:       next.Nom,
		Metadata:       metadata,
		ModeracioEstat: "pendent",
		ModeracioMotiu: "",
		ChangedBy:      sqlNullIntFromInt(userID),
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

func confessionalKind(raw string) string {
	switch strings.TrimSpace(raw) {
	case "religio", "model", "nivell", "entitat", "relacio", "rel_ent":
		return strings.TrimSpace(raw)
	default:
		return ""
	}
}

func confessionalSectionByKind(kind string) (confessionalSection, bool) {
	section, ok := confessionalSections[confessionalKind(kind)]
	return section, ok
}

func confessionalCatalogSection(kind string) bool {
	return kind == "religio" || kind == "nivell"
}

func confessionalSectionMust(kind string) confessionalSection {
	section, _ := confessionalSectionByKind(kind)
	return section
}

func confessionalSectionFromSlug(slug string) (confessionalSection, bool) {
	slug = strings.Trim(strings.TrimSpace(slug), "/")
	for _, section := range confessionalSections {
		if section.Slug == slug {
			return section, true
		}
	}
	return confessionalSection{}, false
}

func confessionalSectionFromPath(path string) (confessionalSection, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] != "confessional" {
			continue
		}
		return confessionalSectionFromSlug(parts[i+1])
	}
	return confessionalSection{}, false
}

func confessionalSectionURL(section confessionalSection, query string) string {
	url := "/confessional/" + section.Slug
	query = strings.TrimSpace(query)
	if query != "" {
		url += "?" + query
	}
	return url
}

func extractConfessionalPath(path string) (string, int) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i := 0; i+2 < len(parts); i++ {
		if parts[i] != "confessional" {
			continue
		}
		if section, ok := confessionalSectionFromSlug(parts[i+1]); ok {
			id, _ := strconv.Atoi(parts[i+2])
			return section.Kind, id
		}
		kind := confessionalKind(parts[i+1])
		id, _ := strconv.Atoi(parts[i+2])
		return kind, id
	}
	return "", 0
}

func normalizeReligioEstat(raw string) string {
	if strings.TrimSpace(raw) == "inactiu" {
		return "inactiu"
	}
	return "actiu"
}

func normalizeConfessionalCode(raw string) string {
	code := strings.ToLower(strings.TrimSpace(raw))
	replacer := strings.NewReplacer(" ", "_", "-", "_", ".", "_", "/", "_")
	code = replacer.Replace(code)
	code = strings.Trim(code, "_")
	return code
}

func normalizeReligioCategoria(raw string) string {
	switch strings.TrimSpace(raw) {
	case "religio", "branca", "confessio", "ritus", "tradicio":
		return strings.TrimSpace(raw)
	default:
		return "religio"
	}
}

func normalizeNivellConfessionalCategoria(raw string) string {
	switch strings.TrimSpace(raw) {
	case "govern_universal", "coordinacio", "territorial_major", "territorial_intermedi", "territorial_local", "unitat_pastoral", "lloc_de_culte", "comunitat_religiosa", "pelegrinatge", "llinatge_comunitat", "no_territorial":
		return strings.TrimSpace(raw)
	default:
		return "no_territorial"
	}
}

func normalizeConfessionalEstat(raw string) string {
	switch strings.TrimSpace(raw) {
	case "inactiu", "historic":
		return strings.TrimSpace(raw)
	default:
		return "actiu"
	}
}

func normalizeModerationStatus(raw string) string {
	switch strings.TrimSpace(raw) {
	case "publicat":
		return "publicat"
	case "pendent", "rebutjat":
		return strings.TrimSpace(raw)
	default:
		return "pendent"
	}
}

func normalizeRelacioTipus(raw string) string {
	switch strings.TrimSpace(raw) {
	case "principal", "parroquia", "bisbat", "arxiprestat", "historica", "altres":
		return strings.TrimSpace(raw)
	default:
		return "principal"
	}
}

func publishedReligioConfessions(items []db.ReligioConfessio) []db.ReligioConfessio {
	out := make([]db.ReligioConfessio, 0, len(items))
	for _, item := range items {
		if item.ModeracioEstat == "publicat" && item.Estat == "actiu" {
			out = append(out, item)
		}
	}
	return out
}

func publishedNivellsConfessionals(items []db.NivellConfessional) []db.NivellConfessional {
	out := make([]db.NivellConfessional, 0, len(items))
	for _, item := range items {
		if item.ModeracioEstat == "publicat" && item.Estat == "actiu" {
			out = append(out, item)
		}
	}
	return out
}

func publishedEntitatsReligioses(items []db.EntitatReligiosa) []db.EntitatReligiosa {
	out := make([]db.EntitatReligiosa, 0, len(items))
	for _, item := range items {
		if item.ModeracioEstat == "publicat" && item.Estat == "actiu" {
			out = append(out, item)
		}
	}
	return out
}

func publishedEntitatReligiosaRelacions(items []db.EntitatReligiosaRelacio) []db.EntitatReligiosaRelacio {
	out := make([]db.EntitatReligiosaRelacio, 0, len(items))
	for _, item := range items {
		if item.ModeracioEstat == "publicat" {
			out = append(out, item)
		}
	}
	return out
}

func publishedMunicipiEntitatsReligioses(items []db.MunicipiEntitatReligiosa) []db.MunicipiEntitatReligiosa {
	out := make([]db.MunicipiEntitatReligiosa, 0, len(items))
	for _, item := range items {
		if item.ModeracioEstat == "publicat" {
			out = append(out, item)
		}
	}
	return out
}

func suggestConfessionalRelationType(levelCode string) string {
	level, ok := GetConfessionalLevelCatalogByCode(levelCode)
	if !ok || strings.TrimSpace(level.Code) == "" {
		return "relacio"
	}
	return level.Code
}

func religioLabels(items []db.ReligioConfessio) map[int]string {
	labels := map[int]string{}
	for _, item := range items {
		labels[item.ID] = item.Nom
	}
	return labels
}

func confessionalReligionCatalogLabels(lang string) map[string]string {
	labels := map[string]string{}
	for _, item := range ListConfessionalReligionCatalog() {
		labels[item.Code] = ConfessionalReligionLabel(item, lang)
	}
	return labels
}

func confessionalLevelCatalogLabels(lang string) map[string]string {
	labels := map[string]string{}
	for _, item := range ListConfessionalLevelCatalog() {
		labels[item.Code] = ConfessionalLevelLabel(item, lang)
	}
	return labels
}

func modelLabels(items []db.ModelConfessional) map[int]string {
	labels := map[int]string{}
	for _, item := range items {
		labels[item.ID] = item.Nom
	}
	return labels
}

func nivellConfessionalLabels(items []db.NivellConfessional) map[int]string {
	labels := map[int]string{}
	for _, item := range items {
		labels[item.ID] = item.NomNivell
	}
	return labels
}

func entitatReligiosaLabels(items []db.EntitatReligiosa) map[int]string {
	labels := map[int]string{}
	for _, item := range items {
		labels[item.ID] = item.Nom
	}
	return labels
}

func paisLabels(items []db.Pais) map[int]string {
	labels := map[int]string{}
	for _, item := range items {
		labels[item.ID] = item.CodiISO3
	}
	return labels
}

func municipiLabels(items []db.MunicipiRow) map[int]string {
	labels := map[int]string{}
	for _, item := range items {
		label := strings.TrimSpace(item.Nom)
		if item.Tipus != "" {
			label += " (" + item.Tipus + ")"
		}
		labels[item.ID] = label
	}
	return labels
}
