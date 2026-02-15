package core

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type transparencySettings struct {
	CurrentTotalCost string
	MonthlyRunCost   string
	Currency         string
	NotePublic       string
}

type transparencyContributorForm struct {
	ID          int
	Name        string
	Type        string
	Description string
	Amount      string
	Currency    string
	URL         string
	IsPublic    bool
	SortOrder   int
}

type transparencyContributorTypeOption struct {
	Value    string
	LabelKey string
}

func transparencyContributorTypeOptions() []transparencyContributorTypeOption {
	return []transparencyContributorTypeOption{
		{Value: "development", LabelKey: "transparency.contributor.type.development"},
		{Value: "financial", LabelKey: "transparency.contributor.type.financial"},
		{Value: "infrastructure", LabelKey: "transparency.contributor.type.infrastructure"},
		{Value: "other", LabelKey: "transparency.contributor.type.other"},
	}
}

func (a *App) AdminTransparencyPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user, ok := a.requirePermissionKey(w, r, permKeyAdminTransparencyManage, PermissionTarget{})
	if !ok {
		return
	}
	settings, err := a.loadTransparencySettings()
	if err != nil {
		http.Error(w, "Error carregant la transparència", http.StatusInternalServerError)
		return
	}
	contributors, err := a.DB.ListTransparencyContributors(true)
	if err != nil {
		http.Error(w, "Error carregant els contributors", http.StatusInternalServerError)
		return
	}
	editID := 0
	if val := strings.TrimSpace(r.URL.Query().Get("edit")); val != "" {
		editID, _ = strconv.Atoi(val)
	}
	form := transparencyContributorForm{
		Type:     "other",
		IsPublic: true,
	}
	if editID > 0 {
		if c, err := a.DB.GetTransparencyContributor(editID); err == nil && c != nil {
			form = transparencyContributorForm{
				ID:          c.ID,
				Name:        c.Name,
				Type:        c.Type,
				Description: c.Description,
				Amount:      transparencyAmountInput(c.Amount),
				Currency:    c.Currency,
				URL:         c.URL,
				IsPublic:    c.IsPublic,
				SortOrder:   c.SortOrder,
			}
		}
	}
	msg := ""
	okMsg := false
	lang := ResolveLang(r)
	switch {
	case r.URL.Query().Get("ok") != "":
		msg = T(lang, "admin.transparency.msg.saved")
		okMsg = true
	case r.URL.Query().Get("deleted") != "":
		msg = T(lang, "admin.transparency.msg.deleted")
		okMsg = true
	case r.URL.Query().Get("err") != "":
		msg = T(lang, "common.error")
	}
	token, _ := ensureCSRF(w, r)
	RenderPrivateTemplate(w, r, "admin-transparency.html", map[string]interface{}{
		"User":         user,
		"Settings":     settings,
		"Contributors": contributors,
		"Form":         form,
		"Types":        transparencyContributorTypeOptions(),
		"IsEdit":       editID > 0,
		"Msg":          msg,
		"Ok":           okMsg,
		"CSRFToken":    token,
	})
}

func (a *App) AdminTransparencySaveSettings(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Formulari invalid", http.StatusBadRequest)
		return
	}
	user, ok := a.requirePermissionKey(w, r, permKeyAdminTransparencyManage, PermissionTarget{})
	if !ok {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	settings := transparencySettings{
		CurrentTotalCost: sanitizeTransparencyText(r.FormValue("current_total_cost")),
		MonthlyRunCost:   sanitizeTransparencyText(r.FormValue("monthly_run_cost")),
		Currency:         sanitizeTransparencyText(r.FormValue("currency")),
		NotePublic:       sanitizeTransparencyText(r.FormValue("note_public")),
	}
	if strings.TrimSpace(settings.Currency) == "" {
		settings.Currency = "EUR"
	}
	if err := a.saveTransparencySettings(settings, user.ID); err != nil {
		http.Redirect(w, r, "/admin/transparencia?err=1", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/admin/transparencia?ok=1", http.StatusSeeOther)
}

func (a *App) AdminTransparencySaveContributor(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Formulari invalid", http.StatusBadRequest)
		return
	}
	user, ok := a.requirePermissionKey(w, r, permKeyAdminTransparencyManage, PermissionTarget{})
	if !ok {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	id, _ := strconv.Atoi(strings.TrimSpace(r.FormValue("id")))
	form := transparencyContributorForm{
		ID:          id,
		Name:        sanitizeTransparencyText(r.FormValue("name")),
		Type:        strings.TrimSpace(r.FormValue("type")),
		Description: sanitizeTransparencyText(r.FormValue("description")),
		Amount:      strings.TrimSpace(r.FormValue("amount")),
		Currency:    sanitizeTransparencyText(r.FormValue("currency")),
		URL:         strings.TrimSpace(r.FormValue("url")),
		IsPublic:    r.FormValue("is_public") == "1",
	}
	if orderVal := strings.TrimSpace(r.FormValue("sort_order")); orderVal != "" {
		if order, err := strconv.Atoi(orderVal); err == nil {
			form.SortOrder = order
		}
	}
	lang := ResolveLang(r)
	if strings.TrimSpace(form.Name) == "" {
		a.renderTransparencyWithError(w, r, user, form, T(lang, "common.required"))
		return
	}
	if !isTransparencyContributorType(form.Type) {
		form.Type = "other"
	}
	amount := sqlNullFloatFromString(form.Amount)
	if strings.TrimSpace(form.Amount) != "" && !amount.Valid {
		a.renderTransparencyWithError(w, r, user, form, T(lang, "common.invalid"))
		return
	}
	if strings.TrimSpace(form.Currency) == "" {
		settings, _ := a.loadTransparencySettings()
		form.Currency = settings.Currency
	}
	urlVal, err := sanitizeTransparencyURL(form.URL)
	if err != nil {
		a.renderTransparencyWithError(w, r, user, form, T(lang, "common.invalid"))
		return
	}
	contributor := &db.TransparencyContributor{
		ID:          form.ID,
		Name:        form.Name,
		Type:        form.Type,
		Description: form.Description,
		Amount:      amount,
		Currency:    form.Currency,
		URL:         urlVal,
		IsPublic:    form.IsPublic,
		SortOrder:   form.SortOrder,
	}
	if form.ID == 0 {
		contributor.CreatedBy = sqlNullIntFromInt(user.ID)
	}
	contributor.UpdatedBy = sqlNullIntFromInt(user.ID)
	if _, err := a.DB.SaveTransparencyContributor(contributor); err != nil {
		a.renderTransparencyWithError(w, r, user, form, T(lang, "common.error"))
		return
	}
	http.Redirect(w, r, "/admin/transparencia?ok=1", http.StatusSeeOther)
}

func (a *App) AdminTransparencyDeleteContributor(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Formulari invalid", http.StatusBadRequest)
		return
	}
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminTransparencyManage, PermissionTarget{}); !ok {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	id, _ := strconv.Atoi(strings.TrimSpace(r.FormValue("id")))
	if id <= 0 {
		http.Redirect(w, r, "/admin/transparencia?err=1", http.StatusSeeOther)
		return
	}
	if err := a.DB.DeleteTransparencyContributor(id); err != nil {
		http.Redirect(w, r, "/admin/transparencia?err=1", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/admin/transparencia?deleted=1", http.StatusSeeOther)
}

func (a *App) TransparencyPublicPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	settings, err := a.loadTransparencySettings()
	if err != nil {
		http.Error(w, "Error carregant la transparència", http.StatusInternalServerError)
		return
	}
	contributors, err := a.DB.ListTransparencyContributors(false)
	if err != nil {
		http.Error(w, "Error carregant la transparència", http.StatusInternalServerError)
		return
	}
	RenderTemplate(w, r, "transparency.html", map[string]interface{}{
		"Settings":     settings,
		"Contributors": contributors,
	})
}

func (a *App) loadTransparencySettings() (transparencySettings, error) {
	settings := transparencySettings{Currency: "EUR"}
	if a == nil || a.DB == nil {
		return settings, errors.New("db unavailable")
	}
	rows, err := a.DB.ListTransparencySettings()
	if err != nil {
		return settings, err
	}
	values := map[string]string{}
	for _, row := range rows {
		values[row.Key] = row.Value
	}
	if val := strings.TrimSpace(values["transparency.current_total_cost"]); val != "" {
		settings.CurrentTotalCost = val
	}
	if val := strings.TrimSpace(values["transparency.monthly_run_cost"]); val != "" {
		settings.MonthlyRunCost = val
	}
	if val := strings.TrimSpace(values["transparency.currency"]); val != "" {
		settings.Currency = val
	}
	if val := strings.TrimSpace(values["transparency.note_public"]); val != "" {
		settings.NotePublic = val
	}
	return settings, nil
}

func (a *App) saveTransparencySettings(settings transparencySettings, userID int) error {
	if a == nil || a.DB == nil {
		return errors.New("db unavailable")
	}
	if err := a.DB.UpsertTransparencySetting("transparency.current_total_cost", settings.CurrentTotalCost, userID); err != nil {
		return err
	}
	if err := a.DB.UpsertTransparencySetting("transparency.monthly_run_cost", settings.MonthlyRunCost, userID); err != nil {
		return err
	}
	if err := a.DB.UpsertTransparencySetting("transparency.currency", settings.Currency, userID); err != nil {
		return err
	}
	if err := a.DB.UpsertTransparencySetting("transparency.note_public", settings.NotePublic, userID); err != nil {
		return err
	}
	return nil
}

func (a *App) renderTransparencyWithError(w http.ResponseWriter, r *http.Request, user *db.User, form transparencyContributorForm, errMsg string) {
	settings, _ := a.loadTransparencySettings()
	contributors, _ := a.DB.ListTransparencyContributors(true)
	token, _ := ensureCSRF(w, r)
	RenderPrivateTemplate(w, r, "admin-transparency.html", map[string]interface{}{
		"User":         user,
		"Settings":     settings,
		"Contributors": contributors,
		"Form":         form,
		"Types":        transparencyContributorTypeOptions(),
		"IsEdit":       form.ID > 0,
		"Error":        errMsg,
		"CSRFToken":    token,
	})
}

func sanitizeTransparencyText(value string) string {
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

func sanitizeTransparencyURL(value string) (string, error) {
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

func isTransparencyContributorType(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "development", "financial", "infrastructure", "other":
		return true
	default:
		return false
	}
}

func transparencyAmountInput(amount sql.NullFloat64) string {
	if !amount.Valid {
		return ""
	}
	return strconv.FormatFloat(amount.Float64, 'f', -1, 64)
}

func sqlNullFloatFromString(val string) sql.NullFloat64 {
	val = strings.TrimSpace(val)
	if val == "" {
		return sql.NullFloat64{}
	}
	if f, err := strconv.ParseFloat(val, 64); err == nil {
		return sql.NullFloat64{Float64: f, Valid: true}
	}
	return sql.NullFloat64{}
}

func transparencyAmountLabel(amount sql.NullFloat64, currency string) string {
	if !amount.Valid {
		return ""
	}
	if strings.TrimSpace(currency) == "" {
		return fmt.Sprintf("%.2f", amount.Float64)
	}
	return fmt.Sprintf("%.2f %s", amount.Float64, strings.TrimSpace(currency))
}
