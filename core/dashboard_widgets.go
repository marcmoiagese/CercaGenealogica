package core

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type DashboardWidgetDefinition struct {
	ID           string
	Title        string
	DefaultOrder int
	DefaultHidden bool
}

type DashboardWidgetState struct {
	Order        int
	Hidden       bool
	DefaultOrder int
	DefaultHidden bool
	Settings     DashboardWidgetSettings
	SettingsJSON string
	RangeLabel   string
	RoleHidden   bool
}

type DashboardWidgetSettings struct {
	Range         string
	AlertWarning  *int
	AlertCritical *int
	Roles         []string
}

type dashboardWidgetSettingsInput struct {
	Range         *string `json:"range,omitempty"`
	AlertWarning  *int    `json:"alert_warning,omitempty"`
	AlertCritical *int    `json:"alert_critical,omitempty"`
	Roles         []string `json:"roles,omitempty"`
}

type DashboardWidgetSchema struct {
	Ranges       []string `json:"ranges,omitempty"`
	DefaultRange string   `json:"default_range,omitempty"`
	AllowAlerts  bool     `json:"allow_alerts,omitempty"`
	AllowRoles   bool     `json:"allow_roles,omitempty"`
}

type DashboardRoleOption struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

type DashboardWidgetAlert struct {
	Level   string
	Message string
	Value   int
}

type DashboardActivityItem struct {
	Label  string
	Detail string
	Meta   string
}

type DashboardActivityView struct {
	Items []DashboardActivityItem
	Alert *DashboardWidgetAlert
}

type DashboardPointsView struct {
	Total int
	Alert *DashboardWidgetAlert
}

type DashboardWidgetConfigView struct {
	ID           string
	Title        string
	Schema       DashboardWidgetSchema
	Settings     DashboardWidgetSettings
	SettingsJSON string
	RoleSet      map[string]bool
	AlertWarning string
	AlertCritical string
	RangeOptions []DashboardRangeOption
}

type DashboardRangeOption struct {
	ID    string
	Label string
}

type dashboardWidgetSettingsPayload struct {
	Range         string   `json:"range,omitempty"`
	AlertWarning  *int     `json:"alert_warning,omitempty"`
	AlertCritical *int     `json:"alert_critical,omitempty"`
	Roles         []string `json:"roles,omitempty"`
}

type dashboardRangeDef struct {
	ID       string
	Days     int
	LabelKey string
}

type dashboardWidgetsPayload struct {
	Reset   bool                         `json:"reset"`
	Widgets []dashboardWidgetPayloadItem `json:"widgets"`
}

type dashboardWidgetPayloadItem struct {
	ID          string `json:"id"`
	Order       int    `json:"order"`
	Hidden      bool   `json:"hidden"`
	SettingsJSON string `json:"settings_json"`
}

func dashboardWidgetDefinitions() []DashboardWidgetDefinition {
	return []DashboardWidgetDefinition{
		{ID: "quick-search", Title: "Cerca rapida", DefaultOrder: 1, DefaultHidden: false},
		{ID: "shortcuts", Title: "Acces rapid", DefaultOrder: 2, DefaultHidden: false},
		{ID: "activity", Title: "La meva activitat", DefaultOrder: 3, DefaultHidden: false},
		{ID: "points", Title: "Punts i bescanvi", DefaultOrder: 4, DefaultHidden: false},
		{ID: "groups", Title: "Grups de treball", DefaultOrder: 5, DefaultHidden: false},
		{ID: "blog", Title: "Novetats del blog", DefaultOrder: 6, DefaultHidden: false},
		{ID: "getting-started", Title: "Comenca en 3 passos", DefaultOrder: 7, DefaultHidden: false},
	}
}

func dashboardWidgetCatalog() map[string]DashboardWidgetDefinition {
	catalog := make(map[string]DashboardWidgetDefinition)
	for _, def := range dashboardWidgetDefinitions() {
		catalog[def.ID] = def
	}
	return catalog
}

var dashboardRanges = []dashboardRangeDef{
	{ID: "week", Days: 7, LabelKey: "dashboard.range.week"},
	{ID: "month", Days: 30, LabelKey: "dashboard.range.month"},
	{ID: "quarter", Days: 90, LabelKey: "dashboard.range.quarter"},
	{ID: "year", Days: 365, LabelKey: "dashboard.range.year"},
	{ID: "all", Days: 0, LabelKey: "dashboard.range.all"},
}

func dashboardRangeOptions(lang string) []DashboardRangeOption {
	options := make([]DashboardRangeOption, 0, len(dashboardRanges))
	for _, entry := range dashboardRanges {
		label := T(lang, entry.LabelKey)
		if label == entry.LabelKey {
			label = entry.ID
		}
		options = append(options, DashboardRangeOption{ID: entry.ID, Label: label})
	}
	return options
}

func filterDashboardRangeOptions(options []DashboardRangeOption, allowed []string) []DashboardRangeOption {
	if len(allowed) == 0 {
		return nil
	}
	allowedSet := map[string]bool{}
	for _, id := range allowed {
		allowedSet[id] = true
	}
	filtered := make([]DashboardRangeOption, 0, len(options))
	for _, opt := range options {
		if allowedSet[opt.ID] {
			filtered = append(filtered, opt)
		}
	}
	return filtered
}

func dashboardRangeLabel(lang, rangeID string) string {
	entry, ok := dashboardRangeByID(rangeID)
	if !ok {
		return ""
	}
	label := T(lang, entry.LabelKey)
	if label == entry.LabelKey {
		return entry.ID
	}
	return label
}

func dashboardRangeByID(rangeID string) (dashboardRangeDef, bool) {
	rangeID = strings.TrimSpace(rangeID)
	for _, entry := range dashboardRanges {
		if entry.ID == rangeID {
			return entry, true
		}
	}
	return dashboardRangeDef{}, false
}

func dashboardRangeWindow(rangeID string) (time.Time, time.Time) {
	entry, ok := dashboardRangeByID(rangeID)
	if !ok || entry.Days <= 0 {
		return time.Time{}, time.Time{}
	}
	now := time.Now()
	from := now.AddDate(0, 0, -entry.Days)
	return from, now
}

func dashboardWidgetSchema(widgetID string) DashboardWidgetSchema {
	switch widgetID {
	case "activity":
		return DashboardWidgetSchema{
			Ranges:       []string{"week", "month", "quarter", "year", "all"},
			DefaultRange: "month",
			AllowAlerts:  true,
			AllowRoles:   true,
		}
	case "points":
		return DashboardWidgetSchema{
			Ranges:       []string{"month", "quarter", "year", "all"},
			DefaultRange: "all",
			AllowAlerts:  true,
			AllowRoles:   true,
		}
	default:
		return DashboardWidgetSchema{
			AllowRoles: true,
		}
	}
}

func (a *App) DashboardWidgetStates(userID int, lang string) (map[string]DashboardWidgetState, error) {
	states := make(map[string]DashboardWidgetState)
	defs := dashboardWidgetDefinitions()
	for _, def := range defs {
		states[def.ID] = DashboardWidgetState{
			Order:         def.DefaultOrder,
			Hidden:        def.DefaultHidden,
			DefaultOrder:  def.DefaultOrder,
			DefaultHidden: def.DefaultHidden,
		}
	}

	configs, err := a.DB.ListDashboardWidgets(userID)
	if err != nil {
		return states, err
	}
	configMap := make(map[string]db.DashboardWidgetConfig)
	for _, cfg := range configs {
		configMap[cfg.WidgetID] = cfg
	}
	roleOptions := a.dashboardRoleOptions()
	roleSet := dashboardRoleOptionSet(roleOptions)
	userRoleSet := a.dashboardUserRoleSet(userID)
	for _, def := range defs {
		state := states[def.ID]
		cfg, ok := configMap[def.ID]
		if ok {
			if cfg.Order > 0 {
				state.Order = cfg.Order
			}
			state.Hidden = cfg.Hidden
		}
		settings, settingsJSON, _ := normalizeDashboardWidgetSettings(def.ID, cfg.SettingsJSON, roleSet, false)
		state.Settings = settings
		state.SettingsJSON = settingsJSON
		schema := dashboardWidgetSchema(def.ID)
		if len(schema.Ranges) > 0 {
			state.RangeLabel = dashboardRangeLabel(lang, settings.Range)
		}
		if len(settings.Roles) > 0 && !dashboardRoleAllowed(userRoleSet, settings.Roles) {
			state.RoleHidden = true
			state.Hidden = true
		}
		states[def.ID] = state
	}
	return states, nil
}

func (a *App) DashboardWidgetsAPI(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	switch r.Method {
	case http.MethodGet:
		widgets, err := a.DB.ListDashboardWidgets(user.ID)
		if err != nil {
			http.Error(w, "Error carregant widgets", http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]interface{}{"widgets": widgets})
		return
	case http.MethodPost:
		if !validateCSRF(r, r.Header.Get("X-CSRF-Token")) && !validateCSRF(r, r.FormValue("csrf_token")) {
			http.Error(w, "CSRF invalid", http.StatusBadRequest)
			return
		}

		var payload dashboardWidgetsPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "Payload invalid", http.StatusBadRequest)
			return
		}

		if payload.Reset {
			if err := a.DB.ClearDashboardWidgets(user.ID); err != nil {
				http.Error(w, "No s'ha pogut restablir", http.StatusInternalServerError)
				return
			}
			writeJSON(w, map[string]interface{}{"ok": true, "reset": true})
			return
		}

		existingConfigs, err := a.DB.ListDashboardWidgets(user.ID)
		if err != nil {
			http.Error(w, "Error carregant widgets", http.StatusInternalServerError)
			return
		}
		existingSettings := make(map[string]string)
		for _, cfg := range existingConfigs {
			existingSettings[cfg.WidgetID] = strings.TrimSpace(cfg.SettingsJSON)
		}
		roleSet := dashboardRoleOptionSet(a.dashboardRoleOptions())

		catalog := dashboardWidgetCatalog()
		var cleaned []db.DashboardWidgetConfig
		for _, item := range payload.Widgets {
			id := strings.TrimSpace(item.ID)
			def, ok := catalog[id]
			if !ok || id == "" {
				continue
			}
			order := item.Order
			if order <= 0 {
				order = def.DefaultOrder
			}
			settingsRaw := strings.TrimSpace(item.SettingsJSON)
			if settingsRaw == "" {
				if existing, found := existingSettings[id]; found {
					settingsRaw = existing
				}
			}
			_, settingsJSON, err := normalizeDashboardWidgetSettings(id, settingsRaw, roleSet, true)
			if err != nil {
				http.Error(w, "Configuracio invalid", http.StatusBadRequest)
				return
			}
			cleaned = append(cleaned, db.DashboardWidgetConfig{
				WidgetID:     id,
				Order:        order,
				Hidden:       item.Hidden,
				SettingsJSON: settingsJSON,
			})
		}
		sort.SliceStable(cleaned, func(i, j int) bool {
			if cleaned[i].Order == cleaned[j].Order {
				return cleaned[i].WidgetID < cleaned[j].WidgetID
			}
			return cleaned[i].Order < cleaned[j].Order
		})

		if err := a.DB.SaveDashboardWidgets(user.ID, cleaned); err != nil {
			http.Error(w, "No s'ha pogut desar", http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]interface{}{"ok": true})
		return
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a *App) dashboardRoleOptions() []DashboardRoleOption {
	if a == nil || a.DB == nil {
		return nil
	}
	rows, err := a.DB.ListPolitiques()
	if err != nil {
		return fallbackDashboardRoleOptions()
	}
	seen := map[string]bool{}
	options := make([]DashboardRoleOption, 0, len(rows))
	for _, row := range rows {
		name := strings.TrimSpace(row.Nom)
		id := normalizeDashboardRole(name)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		options = append(options, DashboardRoleOption{ID: id, Label: name})
	}
	if len(options) == 0 {
		return fallbackDashboardRoleOptions()
	}
	sort.Slice(options, func(i, j int) bool {
		return options[i].ID < options[j].ID
	})
	return options
}

func fallbackDashboardRoleOptions() []DashboardRoleOption {
	roles := append([]string{}, adminPolicies...)
	roles = append(roles, "usuari")
	seen := map[string]bool{}
	options := make([]DashboardRoleOption, 0, len(roles))
	for _, role := range roles {
		id := normalizeDashboardRole(role)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		options = append(options, DashboardRoleOption{ID: id, Label: role})
	}
	sort.Slice(options, func(i, j int) bool {
		return options[i].ID < options[j].ID
	})
	return options
}

func dashboardRoleOptionSet(options []DashboardRoleOption) map[string]DashboardRoleOption {
	set := make(map[string]DashboardRoleOption)
	for _, opt := range options {
		if opt.ID == "" {
			continue
		}
		set[opt.ID] = opt
	}
	return set
}

func (a *App) dashboardUserRoleSet(userID int) map[string]bool {
	roleSet := map[string]bool{}
	if a == nil || a.DB == nil || userID <= 0 {
		return roleSet
	}
	if policies, err := a.DB.ListUserPolitiques(userID); err == nil {
		for _, pol := range policies {
			id := normalizeDashboardRole(pol.Nom)
			if id != "" {
				roleSet[id] = true
			}
		}
	}
	if groups, err := a.DB.ListUserGroups(userID); err == nil {
		for _, group := range groups {
			policies, err := a.DB.ListGroupPolitiques(group.ID)
			if err != nil {
				continue
			}
			for _, pol := range policies {
				id := normalizeDashboardRole(pol.Nom)
				if id != "" {
					roleSet[id] = true
				}
			}
		}
	}
	if len(roleSet) == 0 {
		perms := a.getPermissionsForUser(userID)
		if perms.Admin {
			roleSet["admin"] = true
		}
		if perms.CanModerate {
			roleSet["moderador"] = true
		}
	}
	return roleSet
}

func dashboardRoleAllowed(userRoles map[string]bool, required []string) bool {
	if len(required) == 0 {
		return true
	}
	for _, role := range required {
		if userRoles[normalizeDashboardRole(role)] {
			return true
		}
	}
	return false
}

func normalizeDashboardRole(role string) string {
	return strings.ToLower(strings.TrimSpace(role))
}

func normalizeDashboardWidgetSettings(widgetID string, raw string, roleSet map[string]DashboardRoleOption, strict bool) (DashboardWidgetSettings, string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return normalizeDashboardWidgetSettingsFromInput(widgetID, dashboardWidgetSettingsInput{}, roleSet, strict)
	}
	var input dashboardWidgetSettingsInput
	dec := json.NewDecoder(strings.NewReader(raw))
	if strict {
		dec.DisallowUnknownFields()
	}
	if err := dec.Decode(&input); err != nil {
		if strict {
			return DashboardWidgetSettings{}, "", err
		}
		return normalizeDashboardWidgetSettingsFromInput(widgetID, dashboardWidgetSettingsInput{}, roleSet, strict)
	}
	return normalizeDashboardWidgetSettingsFromInput(widgetID, input, roleSet, strict)
}

func normalizeDashboardWidgetSettingsFromInput(widgetID string, input dashboardWidgetSettingsInput, roleSet map[string]DashboardRoleOption, strict bool) (DashboardWidgetSettings, string, error) {
	schema := dashboardWidgetSchema(widgetID)
	settings := DashboardWidgetSettings{}

	rangeProvided := input.Range != nil
	if rangeProvided {
		settings.Range = strings.TrimSpace(*input.Range)
	}

	alertWarningProvided := input.AlertWarning != nil
	if alertWarningProvided {
		val := *input.AlertWarning
		if val < 0 {
			if strict {
				return DashboardWidgetSettings{}, "", fmt.Errorf("alert warning invalid")
			}
		} else {
			settings.AlertWarning = &val
		}
	}
	alertCriticalProvided := input.AlertCritical != nil
	if alertCriticalProvided {
		val := *input.AlertCritical
		if val < 0 {
			if strict {
				return DashboardWidgetSettings{}, "", fmt.Errorf("alert critical invalid")
			}
		} else {
			settings.AlertCritical = &val
		}
	}

	rolesProvided := len(input.Roles) > 0
	if rolesProvided {
		roles, err := normalizeDashboardWidgetRoles(input.Roles, roleSet, strict)
		if err != nil {
			return DashboardWidgetSettings{}, "", err
		}
		settings.Roles = roles
	}

	if len(schema.Ranges) == 0 {
		if rangeProvided && strict {
			return DashboardWidgetSettings{}, "", fmt.Errorf("range not allowed")
		}
		settings.Range = ""
	} else {
		if settings.Range == "" {
			settings.Range = schema.DefaultRange
		}
		if settings.Range == "" && len(schema.Ranges) > 0 {
			settings.Range = schema.Ranges[0]
		}
		if !stringInSlice(settings.Range, schema.Ranges) {
			if strict {
				return DashboardWidgetSettings{}, "", fmt.Errorf("range not allowed")
			}
			settings.Range = schema.DefaultRange
			if settings.Range == "" && len(schema.Ranges) > 0 {
				settings.Range = schema.Ranges[0]
			}
		}
	}

	if !schema.AllowAlerts {
		if (alertWarningProvided || alertCriticalProvided) && strict {
			return DashboardWidgetSettings{}, "", fmt.Errorf("alerts not allowed")
		}
		settings.AlertWarning = nil
		settings.AlertCritical = nil
	} else if settings.AlertWarning != nil && settings.AlertCritical != nil {
		if *settings.AlertCritical > *settings.AlertWarning {
			if strict {
				return DashboardWidgetSettings{}, "", fmt.Errorf("alert thresholds invalid")
			}
			settings.AlertCritical = settings.AlertWarning
		}
	}

	if !schema.AllowRoles {
		if rolesProvided && strict {
			return DashboardWidgetSettings{}, "", fmt.Errorf("roles not allowed")
		}
		settings.Roles = nil
	}

	settingsJSON := encodeDashboardWidgetSettings(settings, schema)
	return settings, settingsJSON, nil
}

func normalizeDashboardWidgetRoles(raw []string, roleSet map[string]DashboardRoleOption, strict bool) ([]string, error) {
	seen := map[string]bool{}
	out := []string{}
	for _, role := range raw {
		id := normalizeDashboardRole(role)
		if id == "" || seen[id] {
			continue
		}
		if len(roleSet) > 0 {
			if _, ok := roleSet[id]; !ok {
				if strict {
					return nil, fmt.Errorf("role invalid")
				}
				continue
			}
		}
		seen[id] = true
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

func encodeDashboardWidgetSettings(settings DashboardWidgetSettings, schema DashboardWidgetSchema) string {
	payload := dashboardWidgetSettingsPayload{}
	if len(schema.Ranges) > 0 && strings.TrimSpace(settings.Range) != "" {
		payload.Range = strings.TrimSpace(settings.Range)
	}
	if schema.AllowAlerts {
		payload.AlertWarning = settings.AlertWarning
		payload.AlertCritical = settings.AlertCritical
	}
	if schema.AllowRoles && len(settings.Roles) > 0 {
		payload.Roles = append([]string{}, settings.Roles...)
	}
	if payload.Range == "" && payload.AlertWarning == nil && payload.AlertCritical == nil && len(payload.Roles) == 0 {
		return ""
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	return string(raw)
}

func buildDashboardWidgetConfigViews(lang string, states map[string]DashboardWidgetState) []DashboardWidgetConfigView {
	defs := dashboardWidgetDefinitions()
	views := make([]DashboardWidgetConfigView, 0, len(defs))
	allRanges := dashboardRangeOptions(lang)
	for _, def := range defs {
		state := states[def.ID]
		roleSet := map[string]bool{}
		for _, role := range state.Settings.Roles {
			roleSet[normalizeDashboardRole(role)] = true
		}
		schema := dashboardWidgetSchema(def.ID)
		rangeOptions := filterDashboardRangeOptions(allRanges, schema.Ranges)
		alertWarning := ""
		if state.Settings.AlertWarning != nil {
			alertWarning = strconv.Itoa(*state.Settings.AlertWarning)
		}
		alertCritical := ""
		if state.Settings.AlertCritical != nil {
			alertCritical = strconv.Itoa(*state.Settings.AlertCritical)
		}
		views = append(views, DashboardWidgetConfigView{
			ID:           def.ID,
			Title:        def.Title,
			Schema:       schema,
			Settings:     state.Settings,
			SettingsJSON: state.SettingsJSON,
			RoleSet:      roleSet,
			AlertWarning: alertWarning,
			AlertCritical: alertCritical,
			RangeOptions: rangeOptions,
		})
	}
	return views
}

func (a *App) buildDashboardWidgetConfigsForUser(userID int, settings map[string]string) ([]db.DashboardWidgetConfig, error) {
	defs := dashboardWidgetDefinitions()
	existing, err := a.DB.ListDashboardWidgets(userID)
	if err != nil {
		return nil, err
	}
	existingMap := make(map[string]db.DashboardWidgetConfig)
	for _, cfg := range existing {
		existingMap[cfg.WidgetID] = cfg
	}
	out := make([]db.DashboardWidgetConfig, 0, len(defs))
	for _, def := range defs {
		cfg, ok := existingMap[def.ID]
		order := def.DefaultOrder
		hidden := def.DefaultHidden
		if ok {
			if cfg.Order > 0 {
				order = cfg.Order
			}
			hidden = cfg.Hidden
		}
		settingsJSON, found := settings[def.ID]
		if !found {
			settingsJSON = strings.TrimSpace(cfg.SettingsJSON)
		}
		out = append(out, db.DashboardWidgetConfig{
			WidgetID:     def.ID,
			Order:        order,
			Hidden:       hidden,
			SettingsJSON: settingsJSON,
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Order == out[j].Order {
			return out[i].WidgetID < out[j].WidgetID
		}
		return out[i].Order < out[j].Order
	})
	return out, nil
}

func parseOptionalInt(value string) *int {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return nil
	}
	return &parsed
}

func dashboardWidgetAlert(lang string, settings DashboardWidgetSettings, value int, baseKey string) *DashboardWidgetAlert {
	if settings.AlertCritical != nil && value <= *settings.AlertCritical {
		return &DashboardWidgetAlert{
			Level:   "critical",
			Message: T(lang, baseKey+".critical"),
			Value:   value,
		}
	}
	if settings.AlertWarning != nil && value <= *settings.AlertWarning {
		return &DashboardWidgetAlert{
			Level:   "warning",
			Message: T(lang, baseKey+".warning"),
			Value:   value,
		}
	}
	return nil
}

func (a *App) DashboardActivityWidget(userID int, lang string, settings DashboardWidgetSettings) DashboardActivityView {
	view := DashboardActivityView{}
	if a == nil || a.DB == nil || userID <= 0 {
		return view
	}
	from, to := dashboardRangeWindow(settings.Range)
	filter := db.ActivityFilter{Status: "validat", Limit: 5}
	if !from.IsZero() {
		filter.From = from
		filter.To = to
	}
	activities, err := a.DB.ListUserActivityByUser(userID, filter)
	if err == nil {
		for _, act := range activities {
			actionKey := "activity.action." + act.Action
			actionLabel := T(lang, actionKey)
			if actionLabel == actionKey {
				actionLabel = act.Action
			}
			objectLabel := ""
			if act.ObjectType != "" {
				objKey := "activity.object." + act.ObjectType
				objectLabel = T(lang, objKey)
				if objectLabel == objKey {
					objectLabel = act.ObjectType
				}
			}
			detail := objectLabel
			if act.ObjectID.Valid {
				if detail != "" {
					detail = fmt.Sprintf("%s #%d", detail, act.ObjectID.Int64)
				} else {
					detail = fmt.Sprintf("#%d", act.ObjectID.Int64)
				}
			}
			view.Items = append(view.Items, DashboardActivityItem{
				Label:  actionLabel,
				Detail: detail,
				Meta:   act.CreatedAt.Format("2006-01-02"),
			})
		}
	}
	if settings.AlertWarning != nil || settings.AlertCritical != nil {
		countFilter := db.ActivityFilter{Status: "validat"}
		if !from.IsZero() {
			countFilter.From = from
			countFilter.To = to
		}
		if rows, err := a.DB.ListUserActivityByUser(userID, countFilter); err == nil {
			view.Alert = dashboardWidgetAlert(lang, settings, len(rows), "dashboard.widget.activity.alert")
		}
	}
	return view
}

func (a *App) DashboardPointsWidget(userID int, lang string, settings DashboardWidgetSettings) DashboardPointsView {
	view := DashboardPointsView{}
	if a == nil || a.DB == nil || userID <= 0 {
		return view
	}
	total := 0
	rangeID := strings.TrimSpace(settings.Range)
	if rangeID == "" || rangeID == "all" {
		if points, err := a.DB.GetUserPoints(userID); err == nil && points != nil {
			total = points.Total
		}
	} else {
		from, to := dashboardRangeWindow(rangeID)
		filter := db.ActivityFilter{Status: "validat"}
		if !from.IsZero() {
			filter.From = from
			filter.To = to
		}
		if rows, err := a.DB.ListUserActivityByUser(userID, filter); err == nil {
			for _, act := range rows {
				total += act.Points
			}
		}
	}
	view.Total = total
	view.Alert = dashboardWidgetAlert(lang, settings, total, "dashboard.widget.points.alert")
	return view
}

func (a *App) AdminDashboardWidgetsPage(w http.ResponseWriter, r *http.Request) {
	user, _, ok := a.requirePermission(w, r, permAdmin)
	if !ok {
		return
	}
	lang := ResolveLang(r)
	if pref := strings.TrimSpace(user.PreferredLang); pref != "" {
		lang = pref
	}
	roleOptions := a.dashboardRoleOptions()
	roleSet := dashboardRoleOptionSet(roleOptions)
	var saveError string
	if r.Method == http.MethodPost {
		if !validateCSRF(r, r.FormValue("csrf_token")) {
			http.Error(w, "CSRF invalid", http.StatusBadRequest)
			return
		}
		if err := r.ParseForm(); err != nil {
			http.Error(w, "Form invalid", http.StatusBadRequest)
			return
		}
		settingsByWidget := make(map[string]string)
		for _, def := range dashboardWidgetDefinitions() {
			schema := dashboardWidgetSchema(def.ID)
			input := dashboardWidgetSettingsInput{}
			if len(schema.Ranges) > 0 {
				val := strings.TrimSpace(r.FormValue("range_" + def.ID))
				if val != "" {
					input.Range = &val
				}
			}
			if schema.AllowAlerts {
				input.AlertWarning = parseOptionalInt(r.FormValue("alert_warning_" + def.ID))
				input.AlertCritical = parseOptionalInt(r.FormValue("alert_critical_" + def.ID))
			}
			if schema.AllowRoles {
				input.Roles = r.Form["roles_"+def.ID]
			}
			_, settingsJSON, err := normalizeDashboardWidgetSettingsFromInput(def.ID, input, roleSet, true)
			if err != nil {
				saveError = T(lang, "admin.widgets.error.invalid")
				break
			}
			settingsByWidget[def.ID] = settingsJSON
		}
		if saveError == "" {
			configs, err := a.buildDashboardWidgetConfigsForUser(user.ID, settingsByWidget)
			if err != nil {
				saveError = T(lang, "admin.widgets.error.save")
			} else if err := a.DB.SaveDashboardWidgets(user.ID, configs); err != nil {
				saveError = T(lang, "admin.widgets.error.save")
			} else {
				http.Redirect(w, r, "/admin/control/widgets?success=1", http.StatusSeeOther)
				return
			}
		}
	}
	states, err := a.DashboardWidgetStates(user.ID, lang)
	if err != nil {
		Errorf("Error carregant widgets admin: %v", err)
	}
	views := buildDashboardWidgetConfigViews(lang, states)
	RenderPrivateTemplateLang(w, r, "admin-control-widgets.html", lang, map[string]interface{}{
		"WidgetConfigs": views,
		"RoleOptions":   roleOptions,
		"SaveError":     saveError,
		"Success":       r.URL.Query().Get("success") == "1",
	})
}
