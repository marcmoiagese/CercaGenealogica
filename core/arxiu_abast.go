package core

import (
	"database/sql"
	"fmt"
	"net/http"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const arxiuAbastDeleteRequestMarker = "__delete_requested__"

type arxiuAbastTargetKind struct {
	Code string
	Key  string
}

type arxiuAbastRelationKind struct {
	Code string
	Key  string
}

type arxiuAbastFormData struct {
	Abast              *db.ArxiuAbast
	Arxiu              *db.Arxiu
	CSRFToken          string
	Error              string
	ReturnURL          string
	IsNew              bool
	TargetKinds        []arxiuAbastTargetKind
	RelationKinds      []arxiuAbastRelationKind
	TargetKindLabels   map[string]string
	RelationKindLabels map[string]string
	TargetDisplayLabel string
	TargetInputMode    string
	User               *db.User
	CanManageArxius    bool
}

type arxiuAbastViewRow struct {
	ID                int
	TargetKind        string
	TargetKindLabel   string
	TargetLabel       string
	RelationKind      string
	RelationKindLabel string
	Notes             string
	ModeracioEstat    string
}

type arxiuAbastViewSection struct {
	Key   string
	Title string
	Rows  []arxiuAbastViewRow
}

var arxiuAbastTargetKinds = []arxiuAbastTargetKind{
	{Code: "municipi", Key: "archives.scope.target_kind.municipi"},
	{Code: "comarca", Key: "archives.scope.target_kind.comarca"},
	{Code: "provincia", Key: "archives.scope.target_kind.provincia"},
	{Code: "comunitat_autonoma", Key: "archives.scope.target_kind.comunitat_autonoma"},
	{Code: "estat", Key: "archives.scope.target_kind.estat"},
	{Code: "religious_entity", Key: "archives.scope.target_kind.religious_entity"},
	{Code: "institucio", Key: "archives.scope.target_kind.institucio"},
	{Code: "free_text", Key: "archives.scope.target_kind.free_text"},
}

var arxiuAbastRelationKinds = []arxiuAbastRelationKind{
	{Code: "coverage", Key: "archives.scope.relation_kind.coverage"},
	{Code: "jurisdiction", Key: "archives.scope.relation_kind.jurisdiction"},
	{Code: "repository_scope", Key: "archives.scope.relation_kind.repository_scope"},
	{Code: "custody", Key: "archives.scope.relation_kind.custody"},
	{Code: "origin", Key: "archives.scope.relation_kind.origin"},
	{Code: "other", Key: "archives.scope.relation_kind.other"},
}

func arxiuAbastTargetKindLabels(lang string) map[string]string {
	labels := map[string]string{}
	for _, item := range arxiuAbastTargetKinds {
		labels[item.Code] = T(lang, item.Key)
	}
	return labels
}

func arxiuAbastRelationKindLabels(lang string) map[string]string {
	labels := map[string]string{}
	for _, item := range arxiuAbastRelationKinds {
		labels[item.Code] = T(lang, item.Key)
	}
	return labels
}

func arxiuAbastTargetKindValid(code string) bool {
	for _, item := range arxiuAbastTargetKinds {
		if item.Code == strings.TrimSpace(code) {
			return true
		}
	}
	return false
}

func arxiuAbastRelationKindValid(code string) bool {
	for _, item := range arxiuAbastRelationKinds {
		if item.Code == strings.TrimSpace(code) {
			return true
		}
	}
	return false
}

func (a *App) AdminNewArxiuAbastFromArxiu(w http.ResponseWriter, r *http.Request) {
	arxiuID := extractIDBeforeSegment(r.URL.Path, "abasts")
	if arxiuID <= 0 {
		http.NotFound(w, r)
		return
	}
	target := a.resolveArxiuTarget(arxiuID)
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsArxiusEdit, target)
	if !ok {
		return
	}
	arxiu, err := a.DB.GetArxiu(arxiuID)
	if err != nil || arxiu == nil {
		http.NotFound(w, r)
		return
	}
	abast := &db.ArxiuAbast{
		ArxiuID:        arxiuID,
		TargetKind:     "municipi",
		RelationKind:   "coverage",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	}
	a.renderArxiuAbastForm(w, r, user, arxiu, abast, true, "", safeReturnTo(r.URL.Query().Get("return_to"), fmt.Sprintf("/documentals/arxius/%d/edit", arxiuID)))
}

func (a *App) AdminEditArxiuAbast(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	if id <= 0 {
		http.NotFound(w, r)
		return
	}
	abast, err := a.DB.GetArxiuAbast(id)
	if err != nil || abast == nil {
		http.NotFound(w, r)
		return
	}
	target := a.resolveArxiuTarget(abast.ArxiuID)
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsArxiusEdit, target)
	if !ok {
		return
	}
	arxiu, err := a.DB.GetArxiu(abast.ArxiuID)
	if err != nil || arxiu == nil {
		http.NotFound(w, r)
		return
	}
	a.renderArxiuAbastForm(w, r, user, arxiu, abast, false, "", safeReturnTo(r.URL.Query().Get("return_to"), fmt.Sprintf("/documentals/arxius/%d/edit", abast.ArxiuID)))
}

func (a *App) AdminSaveArxiuAbast(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/documentals/arxius", http.StatusSeeOther)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	id := parsePositiveIntDefault(r.FormValue("id"), 0, 0, 1000000000)
	abast := parseArxiuAbastForm(r)
	var current *db.ArxiuAbast
	if id > 0 {
		current, _ = a.DB.GetArxiuAbast(id)
		if current == nil {
			http.NotFound(w, r)
			return
		}
		abast.ID = id
		if abast.ArxiuID != current.ArxiuID {
			abast.ArxiuID = current.ArxiuID
		}
	}
	targetArxiuID := abast.ArxiuID
	if current != nil {
		targetArxiuID = current.ArxiuID
	}
	target := a.resolveArxiuTarget(targetArxiuID)
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsArxiusEdit, target)
	if !ok {
		return
	}
	returnURL := safeReturnTo(r.FormValue("return_to"), fmt.Sprintf("/documentals/arxius/%d/edit?notice=scope-pending", targetArxiuID))
	arxiu, err := a.DB.GetArxiu(targetArxiuID)
	if err != nil || arxiu == nil {
		http.NotFound(w, r)
		return
	}
	lang := ResolveLangForUser(r, user.PreferredLang)
	if current != nil && parsePositiveIntDefault(r.FormValue("arxiu_id"), 0, 0, 1000000000) != current.ArxiuID {
		a.renderArxiuAbastForm(w, r, user, arxiu, abast, false, T(lang, "archives.scope.error.move_restricted"), returnURL)
		return
	}
	if current != nil && current.ModeracioEstat == "publicat" && !a.canModerateModular(user) {
		a.renderArxiuAbastForm(w, r, user, arxiu, abast, false, T(lang, "archives.scope.error.published_edit_restricted"), returnURL)
		return
	}
	if errMsg := a.validateArxiuAbast(lang, abast); errMsg != "" {
		a.renderArxiuAbastForm(w, r, user, arxiu, abast, id == 0, errMsg, returnURL)
		return
	}
	author := sqlNullIntFromInt(user.ID)
	abast.UpdatedBy = author
	abast.ModeracioEstat = "pendent"
	abast.ModeracioMotiu = ""
	abast.ModeratedBy = sql.NullInt64{}
	abast.ModeratedAt = sql.NullTime{}
	if current != nil {
		abast.CreatedBy = current.CreatedBy
	} else {
		abast.CreatedBy = author
	}
	savedID, err := a.DB.SaveArxiuAbast(abast)
	if err != nil {
		msgKey := "archives.scope.error.save"
		if arxiuAbastIsDuplicateErr(err) {
			msgKey = "archives.scope.error.duplicate"
		}
		a.renderArxiuAbastForm(w, r, user, arxiu, abast, id == 0, T(lang, msgKey), returnURL)
		return
	}
	objectID := savedID
	action := "crear"
	if id > 0 {
		objectID = id
		action = "editar"
	}
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleArxiuUpdate, action, "arxiu_abast", &objectID, "pendent", nil, "")
	http.Redirect(w, r, returnURL, http.StatusSeeOther)
}

func (a *App) AdminDeleteArxiuAbast(w http.ResponseWriter, r *http.Request) {
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	id := extractID(r.URL.Path)
	abast, err := a.DB.GetArxiuAbast(id)
	if err != nil || abast == nil {
		http.NotFound(w, r)
		return
	}
	target := a.resolveArxiuTarget(abast.ArxiuID)
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsArxiusDelete, target)
	if !ok {
		return
	}
	returnURL := safeReturnTo(r.FormValue("return_to"), fmt.Sprintf("/documentals/arxius/%d/edit", abast.ArxiuID))
	if abast.ModeracioEstat == "publicat" || arxiuAbastDeleteRequested(abast) {
		if !arxiuAbastDeleteRequested(abast) {
			abast.UpdatedBy = sqlNullIntFromInt(user.ID)
			abast.ModeracioEstat = "pendent"
			abast.ModeracioMotiu = arxiuAbastDeleteRequestMarker
			abast.ModeratedBy = sql.NullInt64{}
			abast.ModeratedAt = sql.NullTime{}
			if _, err := a.DB.SaveArxiuAbast(abast); err == nil {
				_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleArxiuUpdate, "editar", "arxiu_abast", &abast.ID, "pendent", nil, arxiuAbastDeleteRequestMarker)
			}
		}
		http.Redirect(w, r, returnURL, http.StatusSeeOther)
		return
	}
	if acts, err := a.DB.ListActivityByObject("arxiu_abast", id, "pendent"); err == nil {
		for _, act := range acts {
			_ = a.DB.UpdateUserActivityStatus(act.ID, "anulat", &user.ID)
		}
	}
	_ = a.DB.DeleteArxiuAbast(id)
	http.Redirect(w, r, returnURL, http.StatusSeeOther)
}

func parseArxiuAbastForm(r *http.Request) *db.ArxiuAbast {
	_ = r.ParseForm()
	targetID := parsePositiveIntDefault(r.FormValue("target_id"), 0, 0, 1000000000)
	targetCode := strings.TrimSpace(r.FormValue("target_code"))
	targetLabel := strings.TrimSpace(r.FormValue("target_label"))
	kind := strings.TrimSpace(r.FormValue("target_kind"))
	if kind == "institucio" || kind == "free_text" {
		targetID = 0
		targetCode = ""
	}
	targetNull := sql.NullInt64{}
	if targetID > 0 {
		targetNull = sqlNullIntFromInt(targetID)
	}
	return &db.ArxiuAbast{
		ArxiuID:      parsePositiveIntDefault(r.FormValue("arxiu_id"), 0, 0, 1000000000),
		TargetKind:   kind,
		TargetID:     targetNull,
		TargetCode:   targetCode,
		TargetLabel:  targetLabel,
		RelationKind: strings.TrimSpace(r.FormValue("relation_kind")),
		Notes:        strings.TrimSpace(r.FormValue("notes")),
		Estat:        normalizeArxiuAbastEstat(r.FormValue("estat")),
	}
}

func (a *App) validateArxiuAbast(lang string, abast *db.ArxiuAbast) string {
	if msg := a.validateArxiuAbastCommon(lang, abast); msg != "" {
		return msg
	}
	if msg := a.resolveAndValidateArxiuAbastTarget(lang, abast); msg != "" {
		return msg
	}
	return a.validateArxiuAbastDuplicate(lang, abast)
}

func (a *App) validateArxiuAbastCommon(lang string, abast *db.ArxiuAbast) string {
	if abast == nil || abast.ArxiuID <= 0 {
		return T(lang, "archives.scope.error.archive_required")
	}
	if arxiu, err := a.DB.GetArxiu(abast.ArxiuID); err != nil || arxiu == nil {
		return T(lang, "archives.scope.error.archive_not_found")
	}
	if !arxiuAbastTargetKindValid(abast.TargetKind) {
		return T(lang, "archives.scope.error.invalid_target_kind")
	}
	if !arxiuAbastRelationKindValid(abast.RelationKind) {
		return T(lang, "archives.scope.error.invalid_relation_kind")
	}
	return ""
}

func (a *App) resolveAndValidateArxiuAbastTarget(lang string, abast *db.ArxiuAbast) string {
	switch abast.TargetKind {
	case "religious_entity":
		if !abast.TargetID.Valid || abast.TargetID.Int64 <= 0 {
			return T(lang, "archives.scope.error.religious_entity_required")
		}
		entitat, err := a.DB.GetEntitatReligiosa(int(abast.TargetID.Int64))
		if err != nil || entitat == nil {
			return T(lang, "archives.scope.error.religious_entity_not_found")
		}
		if entitat.ModeracioEstat != "publicat" || entitat.Estat != "actiu" {
			return T(lang, "archives.scope.error.religious_entity_not_published")
		}
		abast.TargetLabel = strings.TrimSpace(entitat.Nom)
		abast.TargetCode = strings.TrimSpace(entitat.Codi)
	case "municipi":
		if !abast.TargetID.Valid || abast.TargetID.Int64 <= 0 {
			return T(lang, "archives.scope.error.municipi_required")
		}
		mun, err := a.DB.GetMunicipi(int(abast.TargetID.Int64))
		if err != nil || mun == nil {
			return T(lang, "archives.scope.error.municipi_not_found")
		}
		if mun.ModeracioEstat != "publicat" {
			return T(lang, "archives.scope.error.municipi_not_published")
		}
		abast.TargetLabel = strings.TrimSpace(mun.Nom)
	case "comarca", "provincia", "comunitat_autonoma", "estat":
		if !abast.TargetID.Valid || abast.TargetID.Int64 <= 0 {
			return T(lang, "archives.scope.error.level_required")
		}
		nivell, err := a.DB.GetNivell(int(abast.TargetID.Int64))
		if err != nil || nivell == nil {
			return T(lang, "archives.scope.error.level_not_found")
		}
		if nivell.ModeracioEstat != "publicat" {
			return T(lang, "archives.scope.error.level_not_published")
		}
		if !arxiuAbastLevelKindMatches(abast.TargetKind, nivell.TipusNivell) {
			return T(lang, "archives.scope.error.level_kind_mismatch")
		}
		abast.TargetLabel = strings.TrimSpace(nivell.NomNivell)
		if abast.TargetLabel == "" {
			abast.TargetLabel = strings.TrimSpace(nivell.TipusNivell)
		}
		abast.TargetCode = strings.TrimSpace(nivell.CodiOficial)
	case "institucio", "free_text":
		if strings.TrimSpace(abast.TargetLabel) == "" {
			return T(lang, "archives.scope.error.label_required")
		}
	default:
		return T(lang, "archives.scope.error.invalid_target_kind")
	}
	return ""
}

func (a *App) validateArxiuAbastDuplicate(lang string, abast *db.ArxiuAbast) string {
	existing, err := a.DB.ListArxiuAbasts(abast.ArxiuID, "", "")
	if err != nil {
		return T(lang, "archives.scope.error.validate")
	}
	for _, row := range existing {
		if row.ID == abast.ID || row.ModeracioEstat == "rebutjat" {
			continue
		}
		if row.TargetKind == abast.TargetKind &&
			nullIntEqual(row.TargetID, abast.TargetID) &&
			strings.EqualFold(strings.TrimSpace(row.TargetCode), strings.TrimSpace(abast.TargetCode)) &&
			strings.EqualFold(strings.TrimSpace(row.TargetLabel), strings.TrimSpace(abast.TargetLabel)) &&
			row.RelationKind == abast.RelationKind {
			return T(lang, "archives.scope.error.duplicate")
		}
	}
	return ""
}

func (a *App) renderArxiuAbastForm(w http.ResponseWriter, r *http.Request, user *db.User, arxiu *db.Arxiu, abast *db.ArxiuAbast, isNew bool, errMsg, returnURL string) {
	lang := ResolveLangForUser(r, user.PreferredLang)
	targetDisplay := a.resolveArxiuAbastDisplayLabel(lang, abast)
	mode := arxiuAbastInputMode(abast.TargetKind)
	RenderPrivateTemplate(w, r, "admin-arxiu-abast-form.html", arxiuAbastFormData{
		Abast:              abast,
		Arxiu:              arxiu,
		Error:              errMsg,
		ReturnURL:          returnURL,
		IsNew:              isNew,
		TargetKinds:        arxiuAbastTargetKinds,
		RelationKinds:      arxiuAbastRelationKinds,
		TargetKindLabels:   arxiuAbastTargetKindLabels(lang),
		RelationKindLabels: arxiuAbastRelationKindLabels(lang),
		TargetDisplayLabel: targetDisplay,
		TargetInputMode:    mode,
		User:               user,
		CanManageArxius:    a.canManageAnyDocumentalsModular(user),
	})
}

func (a *App) buildArxiuAbastSections(lang string, arxiuID int, status string) ([]arxiuAbastViewSection, error) {
	rows, err := a.DB.ListArxiuAbasts(arxiuID, "", status)
	if err != nil {
		return []arxiuAbastViewSection{}, err
	}
	sections := []arxiuAbastViewSection{
		{Key: "territorial", Title: T(lang, "archives.scope.territorial"), Rows: []arxiuAbastViewRow{}},
		{Key: "religious", Title: T(lang, "archives.scope.religious"), Rows: []arxiuAbastViewRow{}},
		{Key: "other", Title: T(lang, "archives.scope.other_institutions"), Rows: []arxiuAbastViewRow{}},
	}
	targetLabels := arxiuAbastTargetKindLabels(lang)
	relationLabels := arxiuAbastRelationKindLabels(lang)
	for _, row := range rows {
		item := arxiuAbastViewRow{
			ID:                row.ID,
			TargetKind:        row.TargetKind,
			TargetKindLabel:   targetLabels[row.TargetKind],
			TargetLabel:       arxiuAbastListLabel(&row),
			RelationKind:      row.RelationKind,
			RelationKindLabel: relationLabels[row.RelationKind],
			Notes:             strings.TrimSpace(row.Notes),
			ModeracioEstat:    row.ModeracioEstat,
		}
		switch row.TargetKind {
		case "religious_entity":
			sections[1].Rows = append(sections[1].Rows, item)
		case "institucio", "free_text":
			sections[2].Rows = append(sections[2].Rows, item)
		default:
			sections[0].Rows = append(sections[0].Rows, item)
		}
	}
	return sections, nil
}

func (a *App) resolveArxiuAbastDisplayLabel(lang string, abast *db.ArxiuAbast) string {
	if abast == nil {
		return ""
	}
	if strings.TrimSpace(abast.TargetLabel) != "" {
		return strings.TrimSpace(abast.TargetLabel)
	}
	switch abast.TargetKind {
	case "religious_entity":
		if abast.TargetID.Valid {
			if entitat, err := a.DB.GetEntitatReligiosa(int(abast.TargetID.Int64)); err == nil && entitat != nil {
				return strings.TrimSpace(entitat.Nom)
			}
		}
	case "municipi":
		if abast.TargetID.Valid {
			if mun, err := a.DB.GetMunicipi(int(abast.TargetID.Int64)); err == nil && mun != nil {
				return strings.TrimSpace(mun.Nom)
			}
		}
	case "comarca", "provincia", "comunitat_autonoma", "estat":
		if abast.TargetID.Valid {
			if nivell, err := a.DB.GetNivell(int(abast.TargetID.Int64)); err == nil && nivell != nil {
				if strings.TrimSpace(nivell.NomNivell) != "" {
					return strings.TrimSpace(nivell.NomNivell)
				}
				return translateOrFallbackLabel(lang, "levels.types."+strings.TrimSpace(nivell.TipusNivell), nivell.TipusNivell)
			}
		}
	}
	return strings.TrimSpace(abast.TargetCode)
}

func arxiuAbastInputMode(targetKind string) string {
	switch strings.TrimSpace(targetKind) {
	case "institucio", "free_text":
		return "text"
	case "municipi", "comarca", "provincia", "comunitat_autonoma", "estat", "religious_entity":
		return "suggest"
	default:
		return "suggest"
	}
}

func arxiuAbastLevelKindMatches(targetKind, levelType string) bool {
	left := normalizeArxiuAbastKey(targetKind)
	right := normalizeArxiuAbastKey(levelType)
	if left == right {
		return true
	}
	return left == "estat" && right == "pais"
}

func normalizeArxiuAbastKey(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	raw = strings.ReplaceAll(raw, "_", "")
	raw = strings.ReplaceAll(raw, " ", "")
	return raw
}

func normalizeArxiuAbastEstat(raw string) string {
	if strings.TrimSpace(raw) == "historic" {
		return "historic"
	}
	return "actiu"
}

func arxiuAbastDeleteRequested(rel *db.ArxiuAbast) bool {
	if rel == nil {
		return false
	}
	return strings.TrimSpace(rel.ModeracioMotiu) == arxiuAbastDeleteRequestMarker
}

func arxiuAbastListLabel(abast *db.ArxiuAbast) string {
	if abast == nil {
		return ""
	}
	if strings.TrimSpace(abast.TargetLabel) != "" {
		return strings.TrimSpace(abast.TargetLabel)
	}
	return strings.TrimSpace(abast.TargetCode)
}

func arxiuAbastIsDuplicateErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, "ux_arxiu_abast_identity") ||
		strings.Contains(msg, "unique constraint failed") ||
		strings.Contains(msg, "duplicate entry") ||
		strings.Contains(msg, "duplicate key value")
}
