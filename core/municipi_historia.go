package core

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type historiaTimelineViewItem struct {
	ID      int
	FetID   int
	Date    string
	Title   string
	Summary string
	Body    string
}

const (
	municipiHistoriaMaxBytes    int64 = 256 << 10
	municipiHistoriaRateLimit         = 0.5
	municipiHistoriaRateBurst         = 3
	historiaTitleMin                  = 3
	historiaTitleMax                  = 120
	historiaResumMax                  = 600
	historiaBodyMax                   = 50000
	historiaTagsMax                   = 10
	historiaTagMaxLen                 = 40
	historiaYearMin                   = 0
	historiaYearMax                   = 2100
	historiaSourceLabelMax            = 120
	historiaSourceURLMax              = 200
)

func (a *App) MunicipiHistoriaPublic(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKeyIfLogged(w, r, permKeyTerritoriMunicipisView); !ok {
		return
	}
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

	general, _, err := a.DB.GetMunicipiHistoriaSummary(munID)
	if err != nil {
		Errorf("Error carregant historia general municipi %d: %v", munID, err)
	}
	filterQ := strings.TrimSpace(r.URL.Query().Get("q"))
	filterFrom := strings.TrimSpace(r.URL.Query().Get("from"))
	filterTo := strings.TrimSpace(r.URL.Query().Get("to"))
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
	var anyFrom *int
	var anyTo *int
	if filterFrom != "" {
		if n, err := strconv.Atoi(filterFrom); err == nil {
			anyFrom = &n
		}
	}
	if filterTo != "" {
		if n, err := strconv.Atoi(filterTo); err == nil {
			anyTo = &n
		}
	}
	timeline, total, err := a.DB.ListMunicipiHistoriaTimeline(munID, "publicat", perPage, offset, filterQ, anyFrom, anyTo)
	if err != nil {
		Errorf("Error carregant timeline municipi %d: %v", munID, err)
		timeline = []db.MunicipiHistoriaFetVersion{}
		total = 0
	}
	viewItems := []historiaTimelineViewItem{}
	for _, item := range timeline {
		summary := strings.TrimSpace(item.Resum)
		if summary == "" {
			summary = summarizeHistoriaText(item.CosText, 220)
		}
		viewItems = append(viewItems, historiaTimelineViewItem{
			ID:      item.ID,
			FetID:   item.FetID,
			Date:    historiaDateLabel(item),
			Title:   strings.TrimSpace(item.Titol),
			Summary: summary,
			Body:    strings.TrimSpace(item.CosText),
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
	if filterFrom != "" {
		values.Set("from", filterFrom)
	}
	if filterTo != "" {
		values.Set("to", filterTo)
	}
	if perPage != 20 {
		values.Set("per_page", strconv.Itoa(perPage))
	}
	pageBase := "/territori/municipis/" + strconv.Itoa(munID) + "/historia?" + values.Encode()

	munTarget := a.resolveMunicipiTarget(mun.ID)
	canAportarHistoria := user != nil && a.HasPermission(user.ID, permKeyTerritoriMunicipisHistoriaCreate, munTarget)

	data := map[string]interface{}{
		"Municipi":             mun,
		"HistoriaGeneral":      general,
		"Timeline":             viewItems,
		"FilterQ":              filterQ,
		"FilterFrom":           filterFrom,
		"FilterTo":             filterTo,
		"Page":                 page,
		"PerPage":              perPage,
		"TotalPages":           totalPages,
		"HasPrev":              page > 1,
		"HasNext":              page < totalPages,
		"PrevPage":             page - 1,
		"NextPage":             page + 1,
		"PageBase":             pageBase,
		"CanAportarHistoria":   canAportarHistoria,
	}
	if user != nil {
		RenderPrivateTemplate(w, r, "municipi-historia.html", data)
		return
	}
	RenderTemplate(w, r, "municipi-historia.html", data)
}

func (a *App) MunicipiHistoriaAportar(w http.ResponseWriter, r *http.Request) {
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
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisHistoriaCreate, target)
	if !ok {
		return
	}
	perms, _ := a.permissionsFromContext(r)
	canModerateHistory := user != nil && (a.hasPerm(perms, permModerate) || a.HasPermission(user.ID, permKeyTerritoriMunicipisHistoriaModerate, target))
	token, _ := ensureCSRF(w, r)

	var generalDraft *db.MunicipiHistoriaGeneralVersion
	if val := strings.TrimSpace(r.URL.Query().Get("general_version_id")); val != "" {
		if id, err := strconv.Atoi(val); err == nil && id > 0 {
			if row, err := a.DB.GetMunicipiHistoriaGeneralVersion(id); err == nil && row != nil {
				if munIDFromVersion, err := a.DB.ResolveMunicipiIDByHistoriaGeneralVersionID(row.ID); err == nil && munIDFromVersion == munID {
					if row.Status == "draft" || canModerateHistory {
						if ownsDraft(user, row.CreatedBy) || a.HasPermission(user.ID, permKeyTerritoriMunicipisHistoriaEdit, target) || canModerateHistory {
							generalDraft = row
						}
					}
				}
			}
		}
	}

	var fetDraft *db.MunicipiHistoriaFetVersion
	if val := strings.TrimSpace(r.URL.Query().Get("fet_version_id")); val != "" {
		if id, err := strconv.Atoi(val); err == nil && id > 0 {
			if row, err := a.DB.GetMunicipiHistoriaFetVersion(id); err == nil && row != nil {
				if munIDFromVersion, err := a.DB.ResolveMunicipiIDByHistoriaFetVersionID(row.ID); err == nil && munIDFromVersion == munID {
					if row.Status == "draft" || canModerateHistory {
						if ownsDraft(user, row.CreatedBy) || a.HasPermission(user.ID, permKeyTerritoriMunicipisHistoriaEdit, target) || canModerateHistory {
							fetDraft = row
						}
					}
				}
			}
		}
	}

	generalStatus := ""
	generalTitle := ""
	generalResum := ""
	generalBody := ""
	generalTags := ""
	generalDraftID := 0
	generalLock := 0
	if generalDraft != nil {
		generalStatus = generalDraft.Status
		generalTitle = generalDraft.Titol
		generalResum = generalDraft.Resum
		generalBody = generalDraft.CosText
		generalTags = generalDraft.TagsJSON
		generalDraftID = generalDraft.ID
		generalLock = generalDraft.LockVersion
	}

	fetStatus := ""
	fetTitle := ""
	fetResum := ""
	fetBody := ""
	fetTags := ""
	fetFonts := ""
	fetDataDisplay := ""
	fetDraftID := 0
	fetDraftFetID := 0
	fetLock := 0
	if fetDraft != nil {
		fetStatus = fetDraft.Status
		fetTitle = fetDraft.Titol
		fetResum = fetDraft.Resum
		fetBody = fetDraft.CosText
		fetTags = fetDraft.TagsJSON
		fetFonts = fetDraft.FontsJSON
		fetDataDisplay = fetDraft.DataDisplay
		fetDraftID = fetDraft.ID
		fetDraftFetID = fetDraft.FetID
		fetLock = fetDraft.LockVersion
	}

	okMsg := ""
	if r.URL.Query().Get("ok") != "" {
		okMsg = T(ResolveLang(r), "common.saved")
	}
	errMsg := ""
	if r.URL.Query().Get("err") != "" {
		errMsg = T(ResolveLang(r), "common.error")
	}

	fetFormAction := "/territori/municipis/" + strconv.Itoa(munID) + "/historia/fets/new"
	if fetDraft != nil && fetDraft.FetID > 0 {
		fetFormAction = "/territori/municipis/" + strconv.Itoa(munID) + "/historia/fets/" + strconv.Itoa(fetDraft.FetID) + "/save"
	}

	data := map[string]interface{}{
		"Municipi":           mun,
		"CSRFToken":          token,
		"GeneralDraftID":     generalDraftID,
		"GeneralDraftLock":   generalLock,
		"GeneralDraftStatus": generalStatus,
		"GeneralDraftTitle":  strings.TrimSpace(generalTitle),
		"GeneralDraftResum":  strings.TrimSpace(generalResum),
		"GeneralDraftBody":   strings.TrimSpace(generalBody),
		"GeneralDraftTags":   strings.TrimSpace(generalTags),
		"FetDraftID":         fetDraftID,
		"FetDraftFetID":      fetDraftFetID,
		"FetDraftLock":       fetLock,
		"FetDraftStatus":     fetStatus,
		"FetDraftDataDisplay": strings.TrimSpace(fetDataDisplay),
		"FetDraftAnyInici":   draftInt(fetDraft, true),
		"FetDraftAnyFi":      draftInt(fetDraft, false),
		"FetDraftTitle":      strings.TrimSpace(fetTitle),
		"FetDraftResum":      strings.TrimSpace(fetResum),
		"FetDraftBody":       strings.TrimSpace(fetBody),
		"FetDraftTags":       strings.TrimSpace(fetTags),
		"FetDraftFonts":      strings.TrimSpace(fetFonts),
		"FetFormAction":      fetFormAction,
		"Ok":                 okMsg,
		"Error":              errMsg,
		"User":               user,
	}
	RenderPrivateTemplate(w, r, "municipi-historia-aportar.html", data)
}

func (a *App) MunicipiHistoriaGeneralSave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !parseHistoriaForm(w, r, "/territori/municipis/historia/general/save") {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
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
	_, ok := a.requireAnyPermissionKey(w, r, []string{permKeyTerritoriMunicipisHistoriaCreate, permKeyTerritoriMunicipisHistoriaEdit}, target)
	if !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	versionID := parseFormInt(r.FormValue("version_id"))
	lockVersion := parseFormInt(r.FormValue("lock_version"))
	historia, err := a.DB.EnsureMunicipiHistoria(munID)
	if err != nil || historia == nil {
		http.Redirect(w, r, historiaAportarURL(munID, "", "err=1"), http.StatusSeeOther)
		return
	}
	var version *db.MunicipiHistoriaGeneralVersion
	if versionID > 0 {
		row, err := a.DB.GetMunicipiHistoriaGeneralVersion(versionID)
		if err != nil || row == nil {
			http.Redirect(w, r, historiaAportarURL(munID, "", "err=1"), http.StatusSeeOther)
			return
		}
		if row.HistoriaID != historia.ID {
			http.NotFound(w, r)
			return
		}
		if row.Status != "draft" {
			http.Redirect(w, r, historiaAportarURL(munID, "", "err=1"), http.StatusSeeOther)
			return
		}
		if !ownsDraft(user, row.CreatedBy) && !a.HasPermission(user.ID, permKeyTerritoriMunicipisHistoriaEdit, target) {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		version = row
	} else {
		newID, err := a.DB.CreateMunicipiHistoriaGeneralDraft(historia.ID, user.ID, false)
		if err != nil || newID <= 0 {
			http.Redirect(w, r, historiaAportarURL(munID, "", "err=1"), http.StatusSeeOther)
			return
		}
		versionID = newID
		version = &db.MunicipiHistoriaGeneralVersion{ID: newID, HistoriaID: historia.ID, LockVersion: 0}
	}
	version.Titol = strings.TrimSpace(r.FormValue("titol"))
	version.Resum = strings.TrimSpace(r.FormValue("resum"))
	version.CosText = strings.TrimSpace(r.FormValue("cos_text"))
	version.TagsJSON = strings.TrimSpace(r.FormValue("tags_json"))
	if err := validateHistoriaGeneralValues(version.Titol, version.Resum, version.CosText, version.TagsJSON, false); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if tags, err := normalizeHistoriaTags(version.TagsJSON); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	} else {
		version.TagsJSON = tags
	}
	version.LockVersion = lockVersion
	if err := a.DB.UpdateMunicipiHistoriaGeneralDraft(version); err != nil {
		if err == db.ErrConflict {
			http.Redirect(w, r, historiaAportarURL(munID, "general_version_id="+strconv.Itoa(versionID), "err=conflict"), http.StatusSeeOther)
			return
		}
		http.Redirect(w, r, historiaAportarURL(munID, "general_version_id="+strconv.Itoa(versionID), "err=1"), http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, historiaAportarURL(munID, "general_version_id="+strconv.Itoa(versionID), "ok=1"), http.StatusSeeOther)
}

func (a *App) MunicipiHistoriaGeneralSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !parseHistoriaForm(w, r, "/territori/municipis/historia/general/submit") {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	munID := municipiIDFromPath(r.URL.Path)
	if munID <= 0 {
		http.NotFound(w, r)
		return
	}
	target := a.resolveMunicipiTarget(munID)
	_, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisHistoriaSubmit, target)
	if !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	versionID := parseFormInt(r.FormValue("version_id"))
	if versionID <= 0 {
		http.Redirect(w, r, historiaAportarURL(munID, "", "err=1"), http.StatusSeeOther)
		return
	}
	version, err := a.DB.GetMunicipiHistoriaGeneralVersion(versionID)
	if err != nil || version == nil {
		http.Redirect(w, r, historiaAportarURL(munID, "", "err=1"), http.StatusSeeOther)
		return
	}
	if munIDFromVersion, err := a.DB.ResolveMunicipiIDByHistoriaGeneralVersionID(versionID); err != nil || munIDFromVersion != munID {
		http.NotFound(w, r)
		return
	}
	if version.Status != "draft" {
		http.Redirect(w, r, historiaAportarURL(munID, "", "err=1"), http.StatusSeeOther)
		return
	}
	if !ownsDraft(user, version.CreatedBy) && !a.HasPermission(user.ID, permKeyTerritoriMunicipisHistoriaEdit, target) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if err := validateHistoriaGeneralValues(version.Titol, version.Resum, version.CosText, version.TagsJSON, true); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := normalizeHistoriaTags(version.TagsJSON); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := a.DB.SetMunicipiHistoriaGeneralStatus(versionID, "pendent", "", nil); err != nil {
		http.Redirect(w, r, historiaAportarURL(munID, "general_version_id="+strconv.Itoa(versionID), "err=1"), http.StatusSeeOther)
		return
	}
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleMunicipiHistoriaGeneralSubmit, "municipi_historia_general_submit", "municipi_historia_general", &versionID, "pendent", nil, fmt.Sprintf("municipi:%d", munID))
	http.Redirect(w, r, historiaAportarURL(munID, "general_version_id="+strconv.Itoa(versionID), "ok=1"), http.StatusSeeOther)
}

func (a *App) MunicipiHistoriaFetNew(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !parseHistoriaForm(w, r, "/territori/municipis/historia/fets/new") {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	munID := municipiIDFromPath(r.URL.Path)
	if munID <= 0 {
		http.NotFound(w, r)
		return
	}
	target := a.resolveMunicipiTarget(munID)
	_, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisHistoriaCreate, target)
	if !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	fetID, err := a.DB.CreateMunicipiHistoriaFet(munID, user.ID)
	if err != nil || fetID <= 0 {
		http.Redirect(w, r, historiaAportarURL(munID, "", "err=1"), http.StatusSeeOther)
		return
	}
	versionID, err := a.DB.CreateMunicipiHistoriaFetDraft(fetID, user.ID, false)
	if err != nil || versionID <= 0 {
		http.Redirect(w, r, historiaAportarURL(munID, "", "err=1"), http.StatusSeeOther)
		return
	}
	version := &db.MunicipiHistoriaFetVersion{
		ID:          versionID,
		FetID:       fetID,
		AnyInici:    sqlNullInt(r.FormValue("any_inici")),
		AnyFi:       sqlNullInt(r.FormValue("any_fi")),
		DataInici:   strings.TrimSpace(r.FormValue("data_inici")),
		DataFi:      strings.TrimSpace(r.FormValue("data_fi")),
		DataDisplay: strings.TrimSpace(r.FormValue("data_display")),
		Titol:       strings.TrimSpace(r.FormValue("titol")),
		Resum:       strings.TrimSpace(r.FormValue("resum")),
		CosText:     strings.TrimSpace(r.FormValue("cos_text")),
		TagsJSON:    strings.TrimSpace(r.FormValue("tags_json")),
		FontsJSON:   strings.TrimSpace(r.FormValue("fonts_json")),
		LockVersion: 0,
	}
	if err := validateHistoriaFetValues(version, false); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if tags, err := normalizeHistoriaTags(version.TagsJSON); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	} else {
		version.TagsJSON = tags
	}
	if fonts, err := normalizeHistoriaFonts(version.FontsJSON); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	} else {
		version.FontsJSON = fonts
	}
	if err := a.DB.UpdateMunicipiHistoriaFetDraft(version); err != nil {
		http.Redirect(w, r, historiaAportarURL(munID, "fet_version_id="+strconv.Itoa(versionID), "err=1"), http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, historiaAportarURL(munID, "fet_version_id="+strconv.Itoa(versionID), "ok=1"), http.StatusSeeOther)
}

func (a *App) MunicipiHistoriaFetSave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !parseHistoriaForm(w, r, "/territori/municipis/historia/fets/save") {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	munID := municipiIDFromPath(r.URL.Path)
	if munID <= 0 {
		http.NotFound(w, r)
		return
	}
	fetID := historiaFetIDFromPath(r.URL.Path)
	if fetID <= 0 {
		http.NotFound(w, r)
		return
	}
	target := a.resolveMunicipiTarget(munID)
	_, ok := a.requireAnyPermissionKey(w, r, []string{permKeyTerritoriMunicipisHistoriaCreate, permKeyTerritoriMunicipisHistoriaEdit}, target)
	if !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	versionID := parseFormInt(r.FormValue("version_id"))
	lockVersion := parseFormInt(r.FormValue("lock_version"))
	if versionID <= 0 {
		http.Redirect(w, r, historiaAportarURL(munID, "", "err=1"), http.StatusSeeOther)
		return
	}
	version, err := a.DB.GetMunicipiHistoriaFetVersion(versionID)
	if err != nil || version == nil {
		http.Redirect(w, r, historiaAportarURL(munID, "", "err=1"), http.StatusSeeOther)
		return
	}
	if munIDFromVersion, err := a.DB.ResolveMunicipiIDByHistoriaFetVersionID(versionID); err != nil || munIDFromVersion != munID {
		http.NotFound(w, r)
		return
	}
	if version.FetID != fetID {
		http.NotFound(w, r)
		return
	}
	if version.Status != "draft" {
		http.Redirect(w, r, historiaAportarURL(munID, "", "err=1"), http.StatusSeeOther)
		return
	}
	if !ownsDraft(user, version.CreatedBy) && !a.HasPermission(user.ID, permKeyTerritoriMunicipisHistoriaEdit, target) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	version.AnyInici = sqlNullInt(r.FormValue("any_inici"))
	version.AnyFi = sqlNullInt(r.FormValue("any_fi"))
	version.DataInici = strings.TrimSpace(r.FormValue("data_inici"))
	version.DataFi = strings.TrimSpace(r.FormValue("data_fi"))
	version.DataDisplay = strings.TrimSpace(r.FormValue("data_display"))
	version.Titol = strings.TrimSpace(r.FormValue("titol"))
	version.Resum = strings.TrimSpace(r.FormValue("resum"))
	version.CosText = strings.TrimSpace(r.FormValue("cos_text"))
	version.TagsJSON = strings.TrimSpace(r.FormValue("tags_json"))
	version.FontsJSON = strings.TrimSpace(r.FormValue("fonts_json"))
	if err := validateHistoriaFetValues(version, false); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if tags, err := normalizeHistoriaTags(version.TagsJSON); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	} else {
		version.TagsJSON = tags
	}
	if fonts, err := normalizeHistoriaFonts(version.FontsJSON); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	} else {
		version.FontsJSON = fonts
	}
	version.LockVersion = lockVersion
	if err := a.DB.UpdateMunicipiHistoriaFetDraft(version); err != nil {
		if err == db.ErrConflict {
			http.Redirect(w, r, historiaAportarURL(munID, "fet_version_id="+strconv.Itoa(versionID), "err=conflict"), http.StatusSeeOther)
			return
		}
		http.Redirect(w, r, historiaAportarURL(munID, "fet_version_id="+strconv.Itoa(versionID), "err=1"), http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, historiaAportarURL(munID, "fet_version_id="+strconv.Itoa(versionID), "ok=1"), http.StatusSeeOther)
}

func (a *App) MunicipiHistoriaFetSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !parseHistoriaForm(w, r, "/territori/municipis/historia/fets/submit") {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	munID := municipiIDFromPath(r.URL.Path)
	if munID <= 0 {
		http.NotFound(w, r)
		return
	}
	target := a.resolveMunicipiTarget(munID)
	_, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisHistoriaSubmit, target)
	if !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	versionID := parseFormInt(r.FormValue("version_id"))
	if versionID <= 0 {
		http.Redirect(w, r, historiaAportarURL(munID, "", "err=1"), http.StatusSeeOther)
		return
	}
	version, err := a.DB.GetMunicipiHistoriaFetVersion(versionID)
	if err != nil || version == nil {
		http.Redirect(w, r, historiaAportarURL(munID, "", "err=1"), http.StatusSeeOther)
		return
	}
	if munIDFromVersion, err := a.DB.ResolveMunicipiIDByHistoriaFetVersionID(versionID); err != nil || munIDFromVersion != munID {
		http.NotFound(w, r)
		return
	}
	if version.Status != "draft" {
		http.Redirect(w, r, historiaAportarURL(munID, "", "err=1"), http.StatusSeeOther)
		return
	}
	if !ownsDraft(user, version.CreatedBy) && !a.HasPermission(user.ID, permKeyTerritoriMunicipisHistoriaEdit, target) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if err := validateHistoriaFetValues(version, true); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := normalizeHistoriaTags(version.TagsJSON); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := normalizeHistoriaFonts(version.FontsJSON); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := a.DB.SetMunicipiHistoriaFetStatus(versionID, "pendent", "", nil); err != nil {
		http.Redirect(w, r, historiaAportarURL(munID, "fet_version_id="+strconv.Itoa(versionID), "err=1"), http.StatusSeeOther)
		return
	}
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleMunicipiHistoriaFetSubmit, "municipi_historia_fet_submit", "municipi_historia_fet", &versionID, "pendent", nil, fmt.Sprintf("municipi:%d", munID))
	http.Redirect(w, r, historiaAportarURL(munID, "fet_version_id="+strconv.Itoa(versionID), "ok=1"), http.StatusSeeOther)
}

func municipiIDFromPath(path string) int {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i := 0; i < len(parts)-1; i++ {
		if parts[i] == "municipis" {
			if id, err := strconv.Atoi(parts[i+1]); err == nil {
				return id
			}
		}
	}
	return extractID(path)
}

func historiaFetIDFromPath(path string) int {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i := 0; i < len(parts)-1; i++ {
		if parts[i] == "fets" {
			if id, err := strconv.Atoi(parts[i+1]); err == nil {
				return id
			}
		}
	}
	return extractID(path)
}

func parseFormInt(val string) int {
	val = strings.TrimSpace(val)
	if val == "" {
		return 0
	}
	if n, err := strconv.Atoi(val); err == nil {
		return n
	}
	return 0
}

func historiaAportarURL(munID int, extraQuery, status string) string {
	base := "/territori/municipis/" + strconv.Itoa(munID) + "/historia/aportar"
	q := url.Values{}
	if extraQuery != "" {
		parts := strings.SplitN(extraQuery, "=", 2)
		if len(parts) == 2 {
			q.Set(parts[0], parts[1])
		}
	}
	if status != "" {
		parts := strings.SplitN(status, "=", 2)
		if len(parts) == 2 {
			q.Set(parts[0], parts[1])
		}
	}
	if len(q) == 0 {
		return base
	}
	return base + "?" + q.Encode()
}

func parseHistoriaForm(w http.ResponseWriter, r *http.Request, route string) bool {
	if !allowRouteLimit(r, route, municipiHistoriaRateLimit, municipiHistoriaRateBurst) {
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, municipiHistoriaMaxBytes)
	if err := r.ParseForm(); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "invalid form", http.StatusBadRequest)
		}
		return false
	}
	return true
}

func validateHistoriaGeneralValues(titol, resum, cosText, tagsJSON string, strict bool) error {
	title := strings.TrimSpace(titol)
	titleLen := utf8.RuneCountInString(title)
	if strict && titleLen == 0 {
		return errors.New("titol required")
	}
	if titleLen > 0 && titleLen < historiaTitleMin {
		return errors.New("titol too short")
	}
	if titleLen > historiaTitleMax {
		return errors.New("titol too long")
	}
	summary := strings.TrimSpace(resum)
	if utf8.RuneCountInString(summary) > historiaResumMax {
		return errors.New("resum too long")
	}
	body := strings.TrimSpace(cosText)
	if utf8.RuneCountInString(body) > historiaBodyMax {
		return errors.New("cos_text too long")
	}
	if strings.TrimSpace(tagsJSON) != "" {
		if _, err := normalizeHistoriaTags(tagsJSON); err != nil {
			return err
		}
	}
	return nil
}

func validateHistoriaFetValues(version *db.MunicipiHistoriaFetVersion, strict bool) error {
	if version == nil {
		return errors.New("invalid payload")
	}
	if err := validateHistoriaGeneralValues(version.Titol, version.Resum, version.CosText, version.TagsJSON, strict); err != nil {
		return err
	}
	anyInici := version.AnyInici
	anyFi := version.AnyFi
	if anyInici.Valid {
		if anyInici.Int64 < historiaYearMin || anyInici.Int64 > historiaYearMax {
			return errors.New("any_inici out of range")
		}
	}
	if anyFi.Valid {
		if anyFi.Int64 < historiaYearMin || anyFi.Int64 > historiaYearMax {
			return errors.New("any_fi out of range")
		}
	}
	if anyInici.Valid && anyFi.Valid && anyInici.Int64 > anyFi.Int64 {
		return errors.New("any_inici after any_fi")
	}
	if strings.TrimSpace(version.FontsJSON) != "" {
		if _, err := normalizeHistoriaFonts(version.FontsJSON); err != nil {
			return err
		}
	}
	return nil
}

func normalizeHistoriaTags(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	var tags []string
	if err := json.Unmarshal([]byte(raw), &tags); err != nil {
		return "", errors.New("tags_json invalid")
	}
	seen := map[string]bool{}
	out := []string{}
	for _, t := range tags {
		tag := strings.TrimSpace(t)
		if tag == "" {
			continue
		}
		if utf8.RuneCountInString(tag) > historiaTagMaxLen {
			return "", errors.New("tag too long")
		}
		key := strings.ToLower(tag)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, tag)
	}
	if len(out) > historiaTagsMax {
		return "", errors.New("too many tags")
	}
	if len(out) == 0 {
		return "", nil
	}
	normalized, _ := json.Marshal(out)
	return string(normalized), nil
}

func normalizeHistoriaFonts(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	lines := strings.Split(raw, "\n")
	out := []string{}
	for _, line := range lines {
		entry := strings.TrimSpace(line)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, "|", 2)
		if len(parts) != 2 {
			return "", errors.New("fonts_json invalid line")
		}
		label := strings.TrimSpace(parts[0])
		link := strings.TrimSpace(parts[1])
		if label == "" || link == "" {
			return "", errors.New("fonts_json invalid line")
		}
		if utf8.RuneCountInString(label) > historiaSourceLabelMax {
			return "", errors.New("source label too long")
		}
		if utf8.RuneCountInString(link) > historiaSourceURLMax {
			return "", errors.New("source url too long")
		}
		if !isValidHistoriaURL(link) {
			return "", errors.New("source url invalid")
		}
		out = append(out, label+"|"+link)
	}
	if len(out) == 0 {
		return "", nil
	}
	return strings.Join(out, "\n"), nil
}

func isValidHistoriaURL(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u == nil {
		return false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}
	return u.Host != ""
}

func ownsDraft(user *db.User, createdBy sql.NullInt64) bool {
	if user == nil || !createdBy.Valid {
		return false
	}
	return int(createdBy.Int64) == user.ID
}

func draftInt(draft *db.MunicipiHistoriaFetVersion, start bool) string {
	if draft == nil {
		return ""
	}
	if start {
		if draft.AnyInici.Valid {
			return fmt.Sprintf("%d", draft.AnyInici.Int64)
		}
		return ""
	}
	if draft.AnyFi.Valid {
		return fmt.Sprintf("%d", draft.AnyFi.Int64)
	}
	return ""
}
