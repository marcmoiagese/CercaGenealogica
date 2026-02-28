package core

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type externalLinksSiteAPI struct {
	Slug       string `json:"slug"`
	Name       string `json:"name"`
	AccessMode string `json:"access_mode"`
	IconURL    string `json:"icon_url"`
}

type externalLinksItemAPI struct {
	Title string `json:"title,omitempty"`
	Meta  string `json:"meta,omitempty"`
	URL   string `json:"url"`
	Count int    `json:"count"`
}

type externalLinksGroupAPI struct {
	Site  externalLinksSiteAPI   `json:"site"`
	Items []externalLinksItemAPI `json:"items"`
}

type externalLinksAPIResponse struct {
	PersonaID int                     `json:"persona_id"`
	Groups    []externalLinksGroupAPI `json:"groups"`
}

type externalLinksSubmitRequest struct {
	URL   string `json:"url"`
	Title string `json:"title"`
}

func (a *App) PersonesExternalLinksAPI(w http.ResponseWriter, r *http.Request) {
	if !strings.HasSuffix(r.URL.Path, "/external-links") {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		a.personaExternalLinksJSON(w, r)
		return
	case http.MethodPost:
		a.personaExternalLinksSubmitAPI(w, r)
		return
	default:
		http.NotFound(w, r)
		return
	}
}

func (a *App) personaExternalLinksJSON(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	lang := ResolveLang(r)
	persona, err := a.DB.GetPersona(id)
	if err != nil || persona == nil || strings.TrimSpace(persona.ModeracioEstat) != "publicat" {
		http.NotFound(w, r)
		return
	}
	rows, err := a.DB.ExternalLinksListByPersona(id, "approved")
	if err != nil {
		http.Error(w, "No s'han pogut carregar els enllacos externs", http.StatusInternalServerError)
		return
	}
	response := buildExternalLinksAPIResponse(lang, id, rows)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(response)
}

func (a *App) personaExternalLinksSubmitAPI(w http.ResponseWriter, r *http.Request) {
	if !strings.HasSuffix(r.URL.Path, "/external-links") {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	personaID := extractID(r.URL.Path)
	if personaID == 0 {
		http.NotFound(w, r)
		return
	}
	persona, err := a.DB.GetPersona(personaID)
	if err != nil || persona == nil || strings.TrimSpace(persona.ModeracioEstat) != "publicat" {
		http.NotFound(w, r)
		return
	}
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if !validateCSRF(r, r.Header.Get("X-CSRF-Token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	var payload externalLinksSubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid payload", http.StatusBadRequest)
		return
	}
	urlVal := strings.TrimSpace(payload.URL)
	title := strings.TrimSpace(payload.Title)
	if urlVal == "" {
		writeExternalLinksStatus(w, "invalid", http.StatusBadRequest)
		return
	}
	_, normURL, _, err := db.NormalizeExternalURL(urlVal)
	if err != nil {
		writeExternalLinksStatus(w, "invalid", http.StatusBadRequest)
		return
	}
	existing, err := a.DB.ExternalLinksListByPersona(personaID, "")
	if err != nil {
		http.Error(w, "No s'han pogut carregar els enllacos externs", http.StatusInternalServerError)
		return
	}
	for _, row := range existing {
		rowNorm := strings.TrimSpace(row.URLNorm)
		if rowNorm == "" {
			if _, altNorm, _, err := db.NormalizeExternalURL(row.URL); err == nil {
				rowNorm = altNorm
			}
		}
		if rowNorm == normURL {
			writeExternalLinksStatus(w, "dup", http.StatusOK)
			return
		}
	}
	if _, err := a.DB.ExternalLinkInsertPending(personaID, user.ID, urlVal, title); err != nil {
		if errorsIsInvalidExternalURL(err) {
			writeExternalLinksStatus(w, "invalid", http.StatusBadRequest)
			return
		}
		http.Error(w, "No s'ha pogut guardar l'enllaç", http.StatusInternalServerError)
		return
	}
	writeExternalLinksStatus(w, "ok", http.StatusOK)
}

func (a *App) PersonaExternalLinkSubmit(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePersonesView(w, r); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	personaID := extractID(r.URL.Path)
	if personaID == 0 {
		http.NotFound(w, r)
		return
	}
	persona, err := a.DB.GetPersona(personaID)
	if err != nil || persona == nil || strings.TrimSpace(persona.ModeracioEstat) != "publicat" {
		http.NotFound(w, r)
		return
	}
	user := userFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	urlVal := strings.TrimSpace(r.FormValue("external_url"))
	title := strings.TrimSpace(r.FormValue("external_title"))
	if urlVal == "" {
		redirectExternalLinksFeedback(w, r, personaID, "", "empty")
		return
	}
	if _, err := a.DB.ExternalLinkInsertPending(personaID, user.ID, urlVal, title); err != nil {
		if errorsIsInvalidExternalURL(err) {
			redirectExternalLinksFeedback(w, r, personaID, "", "invalid")
			return
		}
		redirectExternalLinksFeedback(w, r, personaID, "", "save")
		return
	}
	redirectExternalLinksFeedback(w, r, personaID, "pending", "")
}

func externalLinksFeedback(r *http.Request, lang string) (string, string) {
	noticeKey := strings.TrimSpace(r.URL.Query().Get("ext_notice"))
	errorKey := strings.TrimSpace(r.URL.Query().Get("ext_error"))
	notice := ""
	errMsg := ""
	if noticeKey != "" {
		switch noticeKey {
		case "pending":
			notice = T(lang, "persons.external.notice.pending")
		default:
			notice = T(lang, "persons.external.notice.pending")
		}
	}
	if errorKey != "" {
		switch errorKey {
		case "empty":
			errMsg = T(lang, "persons.external.error.empty")
		case "invalid":
			errMsg = T(lang, "persons.external.error.invalid")
		default:
			errMsg = T(lang, "persons.external.error.save")
		}
	}
	return notice, errMsg
}

func redirectExternalLinksFeedback(w http.ResponseWriter, r *http.Request, personaID int, noticeKey, errorKey string) {
	path := "/persones/" + strconv.Itoa(personaID)
	params := []string{}
	if noticeKey != "" {
		params = append(params, "ext_notice="+noticeKey)
	}
	if errorKey != "" {
		params = append(params, "ext_error="+errorKey)
	}
	if len(params) > 0 {
		path += "?" + strings.Join(params, "&")
	}
	http.Redirect(w, r, path+"#arbres-externs", http.StatusSeeOther)
}

func errorsIsInvalidExternalURL(err error) bool {
	return errors.Is(err, db.ErrInvalidExternalURL)
}

func buildExternalLinksAPIResponse(lang string, personaID int, rows []db.ExternalLinkRow) externalLinksAPIResponse {
	type externalLinksGroupBuild struct {
		externalLinksGroupAPI
		itemIndex map[string]int
	}

	groups := make([]externalLinksGroupBuild, 0)
	groupIndex := map[string]int{}

	for _, row := range rows {
		slug := strings.TrimSpace(row.SiteSlug.String)
		name := strings.TrimSpace(row.SiteName.String)
		icon := strings.TrimSpace(row.SiteIconPath.String)
		access := strings.TrimSpace(row.SiteAccessMode.String)

		if slug == "" {
			slug = "unknown"
		}
		if name == "" {
			if slug == "unknown" {
				name = T(lang, "persons.external.site.unknown")
			} else {
				name = slug
			}
		}
		if access == "" {
			access = "mixed"
		}
		if icon == "" {
			if slug == "unknown" {
				icon = "/static/img/ext-sites/unknown.svg"
			} else {
				icon = "/static/img/ext-sites/" + slug + ".svg"
			}
		} else if !strings.HasPrefix(icon, "/") && !strings.HasPrefix(icon, "http://") && !strings.HasPrefix(icon, "https://") {
			icon = "/" + icon
		}

		groupKey := slug
		idx, ok := groupIndex[groupKey]
		if !ok {
			idx = len(groups)
			groupIndex[groupKey] = idx
			groups = append(groups, externalLinksGroupBuild{
				externalLinksGroupAPI: externalLinksGroupAPI{
					Site: externalLinksSiteAPI{
						Slug:       slug,
						Name:       name,
						AccessMode: access,
						IconURL:    icon,
					},
					Items: []externalLinksItemAPI{},
				},
				itemIndex: map[string]int{},
			})
		}

		key := strings.TrimSpace(row.URLNorm)
		if key == "" {
			key = strings.TrimSpace(row.URL)
		}
		if key == "" {
			continue
		}

		title := strings.TrimSpace(row.Title.String)
		meta := strings.TrimSpace(row.Meta.String)
		if slug == "unknown" {
			host := externalLinksHost(row.URL)
			if host != "" && !strings.Contains(strings.ToLower(meta), strings.ToLower(host)) {
				if meta == "" {
					meta = host
				} else {
					meta = meta + " · " + host
				}
			}
		}

		group := &groups[idx]
		if itemIdx, found := group.itemIndex[key]; found {
			item := &group.Items[itemIdx]
			item.Count += 1
			if item.Title == "" && title != "" {
				item.Title = title
			}
			if item.Meta == "" && meta != "" {
				item.Meta = meta
			}
			continue
		}

		group.itemIndex[key] = len(group.Items)
		group.Items = append(group.Items, externalLinksItemAPI{
			URL:   strings.TrimSpace(row.URL),
			Title: title,
			Meta:  meta,
			Count: 1,
		})
	}

	responseGroups := make([]externalLinksGroupAPI, 0, len(groups))
	for _, group := range groups {
		responseGroups = append(responseGroups, group.externalLinksGroupAPI)
	}

	return externalLinksAPIResponse{
		PersonaID: personaID,
		Groups:    responseGroups,
	}
}

func externalLinksHost(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(parsed.Hostname()))
}

func writeExternalLinksStatus(w http.ResponseWriter, status string, code int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": status})
}
