package core

import (
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type maintenanceSeverityOption struct {
	Value    string
	LabelKey string
}

type maintenanceFormState struct {
	ID          int
	Title       string
	Message     string
	Severity    string
	ShowFrom    string
	StartsAt    string
	EndsAt      string
	CTALabel    string
	CTAURL      string
	IsEnabled   bool
	Dismissible bool
}

type maintenanceListRow struct {
	db.MaintenanceWindow
	ShowFromLabel      string
	StartsAtLabel      string
	EndsAtLabel        string
	StatusKey          string
	StatusClass        string
	SeverityLabelKey   string
	SeverityBadgeClass string
}

func maintenanceSeverityOptions() []maintenanceSeverityOption {
	return []maintenanceSeverityOption{
		{Value: "info", LabelKey: "maintenance.severity.info"},
		{Value: "warning", LabelKey: "maintenance.severity.warning"},
		{Value: "critical", LabelKey: "maintenance.severity.critical"},
	}
}

func maintenanceSeverityLabelKey(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "critical":
		return "maintenance.severity.critical"
	case "warning":
		return "maintenance.severity.warning"
	default:
		return "maintenance.severity.info"
	}
}

func maintenanceSeverityBadgeClass(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "critical":
		return "badge-danger"
	case "warning":
		return "badge-warning"
	default:
		return "badge-muted"
	}
}

func maintenanceStatus(now time.Time, startsAtRaw, endsAtRaw string) (string, string) {
	startsAt, errStart := parseMaintenanceTime(startsAtRaw)
	endsAt, errEnd := parseMaintenanceTime(endsAtRaw)
	if errStart == nil && errEnd == nil {
		if now.Before(startsAt) {
			return "maintenance.status.scheduled", "badge-warning"
		}
		if now.After(endsAt) {
			return "maintenance.status.ended", "badge-muted"
		}
		return "maintenance.status.active", "badge-success"
	}
	if errEnd == nil && now.After(endsAt) {
		return "maintenance.status.ended", "badge-muted"
	}
	return "maintenance.status.active", "badge-success"
}

// AdminListMaintenance mostra el llistat de manteniments programats.
func (a *App) AdminListMaintenance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user, ok := a.requirePermissionKey(w, r, permKeyAdminMaintenanceManage, PermissionTarget{})
	if !ok {
		return
	}
	rows, err := a.DB.ListMaintenanceWindows()
	if err != nil {
		http.Error(w, "Error llistant manteniments", http.StatusInternalServerError)
		return
	}
	now := time.Now()
	list := make([]maintenanceListRow, 0, len(rows))
	for _, row := range rows {
		statusKey, statusClass := maintenanceStatus(now, row.StartsAt, row.EndsAt)
		list = append(list, maintenanceListRow{
			MaintenanceWindow:  row,
			ShowFromLabel:      maintenanceDisplayTime(row.ShowFrom),
			StartsAtLabel:      maintenanceDisplayTime(row.StartsAt),
			EndsAtLabel:        maintenanceDisplayTime(row.EndsAt),
			StatusKey:          statusKey,
			StatusClass:        statusClass,
			SeverityLabelKey:   maintenanceSeverityLabelKey(row.Severity),
			SeverityBadgeClass: maintenanceSeverityBadgeClass(row.Severity),
		})
	}
	token, _ := ensureCSRF(w, r)
	msg := ""
	okMsg := false
	lang := ResolveLang(r)
	switch {
	case r.URL.Query().Get("ok") != "":
		msg = T(lang, "admin.maintenance.msg.saved")
		okMsg = true
	case r.URL.Query().Get("deleted") != "":
		msg = T(lang, "admin.maintenance.msg.deleted")
		okMsg = true
	case r.URL.Query().Get("err") != "":
		msg = T(lang, "common.error")
	}
	RenderPrivateTemplate(w, r, "admin-maintenance-list.html", map[string]interface{}{
		"User":      user,
		"Windows":   list,
		"CSRFToken": token,
		"Msg":       msg,
		"Ok":        okMsg,
	})
}

// AdminNewMaintenance mostra el formulari nou.
func (a *App) AdminNewMaintenance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user, ok := a.requirePermissionKey(w, r, permKeyAdminMaintenanceManage, PermissionTarget{})
	if !ok {
		return
	}
	now := time.Now()
	form := maintenanceFormState{
		Severity:    "warning",
		ShowFrom:    maintenanceInputValueFromTime(now),
		StartsAt:    maintenanceInputValueFromTime(now.Add(1 * time.Hour)),
		EndsAt:      maintenanceInputValueFromTime(now.Add(2 * time.Hour)),
		IsEnabled:   true,
		Dismissible: true,
	}
	a.renderMaintenanceForm(w, r, user, form, true, "")
}

// AdminEditMaintenance carrega una finestra existent.
func (a *App) AdminEditMaintenance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user, ok := a.requirePermissionKey(w, r, permKeyAdminMaintenanceManage, PermissionTarget{})
	if !ok {
		return
	}
	id := extractID(r.URL.Path)
	if id <= 0 {
		http.NotFound(w, r)
		return
	}
	window, err := a.DB.GetMaintenanceWindow(id)
	if err != nil || window == nil {
		http.NotFound(w, r)
		return
	}
	form := maintenanceFormState{
		ID:          window.ID,
		Title:       window.Title,
		Message:     window.Message,
		Severity:    strings.TrimSpace(window.Severity),
		ShowFrom:    maintenanceInputValue(window.ShowFrom),
		StartsAt:    maintenanceInputValue(window.StartsAt),
		EndsAt:      maintenanceInputValue(window.EndsAt),
		CTALabel:    window.CTALabel,
		CTAURL:      window.CTAURL,
		IsEnabled:   window.IsEnabled,
		Dismissible: window.Dismissible,
	}
	if form.Severity == "" {
		form.Severity = "info"
	}
	a.renderMaintenanceForm(w, r, user, form, false, "")
}

// AdminSaveMaintenance desa la finestra.
func (a *App) AdminSaveMaintenance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Formulari invalid", http.StatusBadRequest)
		return
	}
	user, ok := a.requirePermissionKey(w, r, permKeyAdminMaintenanceManage, PermissionTarget{})
	if !ok {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	id, _ := strconv.Atoi(strings.TrimSpace(r.FormValue("id")))
	form := maintenanceFormState{
		ID:          id,
		Title:       sanitizeMaintenanceText(r.FormValue("title")),
		Message:     sanitizeMaintenanceText(r.FormValue("message")),
		Severity:    strings.TrimSpace(r.FormValue("severity")),
		ShowFrom:    strings.TrimSpace(r.FormValue("show_from")),
		StartsAt:    strings.TrimSpace(r.FormValue("starts_at")),
		EndsAt:      strings.TrimSpace(r.FormValue("ends_at")),
		CTALabel:    sanitizeMaintenanceText(r.FormValue("cta_label")),
		CTAURL:      strings.TrimSpace(r.FormValue("cta_url")),
		IsEnabled:   r.FormValue("is_enabled") == "1",
		Dismissible: r.FormValue("dismissible") == "1",
	}
	lang := ResolveLang(r)
	if form.Title == "" || form.Message == "" {
		a.renderMaintenanceForm(w, r, user, form, id == 0, T(lang, "common.required"))
		return
	}
	showFrom, showFromTime, err := normalizeMaintenanceTime(form.ShowFrom)
	if err != nil {
		a.renderMaintenanceForm(w, r, user, form, id == 0, T(lang, "common.invalid"))
		return
	}
	startsAt, startsAtTime, err := normalizeMaintenanceTime(form.StartsAt)
	if err != nil {
		a.renderMaintenanceForm(w, r, user, form, id == 0, T(lang, "common.invalid"))
		return
	}
	endsAt, endsAtTime, err := normalizeMaintenanceTime(form.EndsAt)
	if err != nil {
		a.renderMaintenanceForm(w, r, user, form, id == 0, T(lang, "common.invalid"))
		return
	}
	if showFromTime.After(startsAtTime) || startsAtTime.After(endsAtTime) {
		a.renderMaintenanceForm(w, r, user, form, id == 0, T(lang, "admin.maintenance.error.range"))
		return
	}
	ctaURL, err := sanitizeMaintenanceURL(form.CTAURL)
	if err != nil {
		a.renderMaintenanceForm(w, r, user, form, id == 0, T(lang, "admin.maintenance.error.url"))
		return
	}
	if form.Severity == "" {
		form.Severity = "info"
	}
	if !isMaintenanceSeverity(form.Severity) {
		form.Severity = "info"
	}
	window := &db.MaintenanceWindow{
		ID:          id,
		Title:       form.Title,
		Message:     form.Message,
		Severity:    form.Severity,
		ShowFrom:    showFrom,
		StartsAt:    startsAt,
		EndsAt:      endsAt,
		CTALabel:    form.CTALabel,
		CTAURL:      ctaURL,
		IsEnabled:   form.IsEnabled,
		Dismissible: form.Dismissible,
	}
	if id == 0 {
		window.CreatedBy = sqlNullIntFromInt(user.ID)
	}
	window.UpdatedBy = sqlNullIntFromInt(user.ID)
	if _, err := a.DB.SaveMaintenanceWindow(window); err != nil {
		a.renderMaintenanceForm(w, r, user, form, id == 0, T(lang, "common.error"))
		return
	}
	a.logAdminAudit(r, user.ID, auditActionMaintenanceSave, "maintenance", window.ID, map[string]interface{}{
		"enabled": form.IsEnabled,
	})
	InvalidateMaintenanceCache()
	http.Redirect(w, r, "/admin/manteniments?ok=1", http.StatusSeeOther)
}

// AdminDeleteMaintenance elimina una finestra.
func (a *App) AdminDeleteMaintenance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Formulari invalid", http.StatusBadRequest)
		return
	}
	user, ok := a.requirePermissionKey(w, r, permKeyAdminMaintenanceManage, PermissionTarget{})
	if !ok {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	id, _ := strconv.Atoi(strings.TrimSpace(r.FormValue("id")))
	if id <= 0 {
		http.Redirect(w, r, "/admin/manteniments?err=1", http.StatusSeeOther)
		return
	}
	if err := a.DB.DeleteMaintenanceWindow(id); err != nil {
		http.Redirect(w, r, "/admin/manteniments?err=1", http.StatusSeeOther)
		return
	}
	a.logAdminAudit(r, user.ID, auditActionMaintenanceDelete, "maintenance", id, nil)
	InvalidateMaintenanceCache()
	http.Redirect(w, r, "/admin/manteniments?deleted=1", http.StatusSeeOther)
}

func (a *App) renderMaintenanceForm(w http.ResponseWriter, r *http.Request, user *db.User, form maintenanceFormState, isNew bool, errMsg string) {
	token, _ := ensureCSRF(w, r)
	RenderPrivateTemplate(w, r, "admin-maintenance-form.html", map[string]interface{}{
		"User":       user,
		"Form":       form,
		"IsNew":      isNew,
		"CSRFToken":  token,
		"Error":      errMsg,
		"Severities": maintenanceSeverityOptions(),
	})
}

func sanitizeMaintenanceText(value string) string {
	value = strings.TrimSpace(value)
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' || r == '\r' {
			return r
		}
		if r < 32 {
			return -1
		}
		return r
	}, value)
}

func sanitizeMaintenanceURL(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	if strings.HasPrefix(value, "/") {
		return value, nil
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		if err == nil {
			err = errors.New("url invalida")
		}
		return "", err
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http", "https":
		return parsed.String(), nil
	default:
		return "", errors.New("url invalida")
	}
}

func isMaintenanceSeverity(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "info", "warning", "critical":
		return true
	default:
		return false
	}
}
