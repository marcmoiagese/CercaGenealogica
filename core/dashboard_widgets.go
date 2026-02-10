package core

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"

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

func (a *App) DashboardWidgetStates(userID int) (map[string]DashboardWidgetState, error) {
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
	for _, cfg := range configs {
		state, ok := states[cfg.WidgetID]
		if !ok {
			continue
		}
		if cfg.Order > 0 {
			state.Order = cfg.Order
		}
		state.Hidden = cfg.Hidden
		states[cfg.WidgetID] = state
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
			cleaned = append(cleaned, db.DashboardWidgetConfig{
				WidgetID:     id,
				Order:        order,
				Hidden:       item.Hidden,
				SettingsJSON: strings.TrimSpace(item.SettingsJSON),
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
