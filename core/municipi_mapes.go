package core

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const municipiMapaMaxBytes = 2 << 20

type municipiMapaCreatePayload struct {
	Title       string `json:"title"`
	GroupType   string `json:"group_type"`
	PeriodLabel string `json:"period_label"`
	PeriodStart int    `json:"period_start"`
	PeriodEnd   int    `json:"period_end"`
	Topic       string `json:"topic"`
	CSRFToken   string `json:"csrf_token"`
}

type municipiMapaSavePayload struct {
	LockVersion int             `json:"lock_version"`
	Changelog   string          `json:"changelog"`
	Data        json.RawMessage `json:"data"`
	CSRFToken   string          `json:"csrf_token"`
}

type municipiMapaSubmitPayload struct {
	CSRFToken string `json:"csrf_token"`
}

type municipiMapaView struct {
	ID                   int    `json:"id"`
	MunicipiID           int    `json:"municipi_id"`
	GroupType            string `json:"group_type"`
	Title                string `json:"title"`
	PeriodLabel          string `json:"period_label,omitempty"`
	PeriodStart          *int   `json:"period_start,omitempty"`
	PeriodEnd            *int   `json:"period_end,omitempty"`
	Topic                string `json:"topic,omitempty"`
	CurrentVersionID     int    `json:"current_version_id,omitempty"`
	CurrentVersionStatus string `json:"current_version_status,omitempty"`
}

type municipiMapaVersionView struct {
	ID          int             `json:"id"`
	MapaID      int             `json:"mapa_id"`
	MunicipiID  int             `json:"municipi_id"`
	Status      string          `json:"status"`
	LockVersion int             `json:"lock_version"`
	Data        json.RawMessage `json:"data"`
}

func (a *App) MunicipiMapesAPI(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	var base string
	switch {
	case strings.HasPrefix(path, "/api/municipis/"):
		base = strings.TrimPrefix(path, "/api/municipis/")
	case strings.HasPrefix(path, "/api/territori/municipis/"):
		base = strings.TrimPrefix(path, "/api/territori/municipis/")
	default:
		http.NotFound(w, r)
		return
	}
	base = strings.Trim(base, "/")
	parts := strings.Split(base, "/")
	if len(parts) < 2 {
		http.NotFound(w, r)
		return
	}
	munID, err := strconv.Atoi(parts[0])
	if err != nil || munID <= 0 {
		http.NotFound(w, r)
		return
	}
	switch parts[1] {
	case "mapes":
		if len(parts) != 2 {
			http.NotFound(w, r)
			return
		}
		switch r.Method {
		case http.MethodGet:
			a.municipiMapesList(w, r, munID)
		case http.MethodPost:
			a.municipiMapaCreate(w, r, munID)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "persones":
		if len(parts) != 3 || parts[2] != "cerca" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		a.municipiPersonesSearch(w, r, munID)
	default:
		http.NotFound(w, r)
	}
}

func (a *App) MapesAPI(w http.ResponseWriter, r *http.Request) {
	base := strings.TrimPrefix(r.URL.Path, "/api/mapes/")
	base = strings.Trim(base, "/")
	parts := strings.Split(base, "/")
	if len(parts) < 1 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	if parts[0] == "versions" {
		if len(parts) < 2 {
			http.NotFound(w, r)
			return
		}
		versionID, err := strconv.Atoi(parts[1])
		if err != nil || versionID <= 0 {
			http.NotFound(w, r)
			return
		}
		switch len(parts) {
		case 2:
			switch r.Method {
			case http.MethodGet:
				a.municipiMapaVersionGet(w, r, versionID)
			case http.MethodPut:
				a.municipiMapaVersionSave(w, r, versionID)
			default:
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			}
		case 3:
			if parts[2] != "submit" {
				http.NotFound(w, r)
				return
			}
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			a.municipiMapaVersionSubmit(w, r, versionID)
		default:
			http.NotFound(w, r)
		}
		return
	}

	mapID, err := strconv.Atoi(parts[0])
	if err != nil || mapID <= 0 {
		http.NotFound(w, r)
		return
	}
	if len(parts) != 2 || parts[1] != "draft" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	a.municipiMapaDraft(w, r, mapID)
}

func (a *App) municipiMapesList(w http.ResponseWriter, r *http.Request, municipiID int) {
	groupType := strings.TrimSpace(r.URL.Query().Get("group_type"))
	filter := db.MunicipiMapaFilter{MunicipiID: municipiID}
	if groupType != "" {
		filter.GroupType = groupType
	}
	mapes, err := a.DB.ListMunicipiMapes(filter)
	if err != nil {
		http.Error(w, "failed to load maps", http.StatusInternalServerError)
		return
	}
	user, _ := a.VerificarSessio(r)
	target := a.resolveMunicipiTarget(municipiID)
	canViewAll := user != nil && a.HasPermission(user.ID, permKeyTerritoriMunicipisMapesView, target)
	items := make([]municipiMapaView, 0, len(mapes))
	for _, m := range mapes {
		view := municipiMapaView{
			ID:          m.ID,
			MunicipiID:  m.MunicipiID,
			GroupType:   m.GroupType,
			Title:       m.Title,
			PeriodLabel: m.PeriodLabel,
			PeriodStart: nullIntToPtr(m.PeriodStart),
			PeriodEnd:   nullIntToPtr(m.PeriodEnd),
			Topic:       m.Topic,
		}
		var picked *db.MunicipiMapaVersion
		if m.CurrentVersionID.Valid {
			version, err := a.DB.GetMunicipiMapaVersion(int(m.CurrentVersionID.Int64))
			if err == nil && version != nil {
				picked = version
			}
		} else if user != nil {
			versionFilter := db.MunicipiMapaVersionFilter{
				MapaID: m.ID,
				Limit:  1,
			}
			if !canViewAll {
				versionFilter.CreatedBy = user.ID
			}
			if versions, err := a.DB.ListMunicipiMapaVersions(versionFilter); err == nil && len(versions) > 0 {
				picked = &versions[0]
			}
		}
		if picked != nil {
			if !canViewAll && picked.Status != "publicat" {
				if user == nil || !picked.CreatedBy.Valid || int(picked.CreatedBy.Int64) != user.ID {
					continue
				}
			}
			view.CurrentVersionID = picked.ID
			view.CurrentVersionStatus = picked.Status
		} else if !canViewAll {
			continue
		}
		items = append(items, view)
	}
	writeJSON(w, map[string]interface{}{"items": items})
}

func (a *App) municipiPersonesSearch(w http.ResponseWriter, r *http.Request, municipiID int) {
	target := a.resolveMunicipiTarget(municipiID)
	_, ok := a.requireAnyPermissionKey(w, r, []string{
		permKeyTerritoriMunicipisMapesEdit,
		permKeyTerritoriMunicipisMapesCreate,
		permKeyTerritoriMunicipisMapesSubmit,
		permKeyTerritoriMunicipisMapesView,
	}, target)
	if !ok {
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query == "" {
		writeJSON(w, []personaLinkSuggestion{})
		return
	}
	municipi := strings.TrimSpace(r.URL.Query().Get("municipi"))
	anyMin, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("any_min")))
	anyMax, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("any_max")))
	filter := db.PersonaSearchFilter{
		Query:    query,
		Municipi: municipi,
		AnyMin:   anyMin,
		AnyMax:   anyMax,
		Limit:    25,
	}
	if query != "" {
		cand := extractSurnameCandidate(query)
		if len(cand) >= 3 {
			if cognomID, _, ok, err := a.DB.ResolveCognomPublicatByForma(cand); err == nil && ok {
				if forms, err := a.DB.ListCognomFormesPublicades(cognomID); err == nil && len(forms) > 0 {
					filter.UseCognomDictionary = true
					filter.ExpandedCognoms = forms
				}
			}
		}
	}
	results, err := a.DB.SearchPersones(filter)
	if err != nil {
		http.Error(w, "No s'han pogut cercar persones", http.StatusInternalServerError)
		return
	}
	payload := make([]personaLinkSuggestion, 0, len(results))
	for _, res := range results {
		payload = append(payload, personaLinkSuggestion{
			ID:       res.ID,
			Nom:      personaSearchName(res),
			Municipi: res.Municipi,
			Any:      personaSearchYear(res),
		})
	}
	writeJSON(w, payload)
}

func (a *App) municipiMapaCreate(w http.ResponseWriter, r *http.Request, municipiID int) {
	target := a.resolveMunicipiTarget(municipiID)
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisMapesCreate, target)
	if !ok {
		return
	}
	var payload municipiMapaCreatePayload
	if err := decodeMunicipiMapaJSON(w, r, &payload); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	token := readCSRFToken(r, payload.CSRFToken)
	if !validateCSRF(r, token) {
		http.Error(w, "invalid csrf", http.StatusBadRequest)
		return
	}
	groupType := normalizeMunicipiMapaGroupType(payload.GroupType)
	if groupType == "" {
		http.Error(w, "invalid group_type", http.StatusBadRequest)
		return
	}
	title := strings.TrimSpace(payload.Title)
	if title == "" {
		http.Error(w, "missing title", http.StatusBadRequest)
		return
	}
	createdBy := sql.NullInt64{Valid: false}
	if user != nil {
		createdBy = sql.NullInt64{Int64: int64(user.ID), Valid: true}
	}
	periodStart := intToNullInt64(payload.PeriodStart)
	periodEnd := intToNullInt64(payload.PeriodEnd)
	mapa := &db.MunicipiMapa{
		MunicipiID:  municipiID,
		GroupType:   groupType,
		Title:       title,
		PeriodLabel: strings.TrimSpace(payload.PeriodLabel),
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
		Topic:       strings.TrimSpace(payload.Topic),
		CreatedBy:   createdBy,
	}
	id, err := a.DB.CreateMunicipiMapa(mapa)
	if err != nil {
		http.Error(w, "failed to create map", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{"mapa_id": id})
}

func (a *App) municipiMapaDraft(w http.ResponseWriter, r *http.Request, mapID int) {
	munID, err := a.DB.ResolveMunicipiIDByMapaID(mapID)
	if err != nil || munID <= 0 {
		http.NotFound(w, r)
		return
	}
	target := a.resolveMunicipiTarget(munID)
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisMapesEdit, target)
	if !ok {
		return
	}
	var payload municipiMapaSubmitPayload
	if err := decodeMunicipiMapaJSON(w, r, &payload); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	token := readCSRFToken(r, payload.CSRFToken)
	if !validateCSRF(r, token) {
		http.Error(w, "invalid csrf", http.StatusBadRequest)
		return
	}
	mapa, err := a.DB.GetMunicipiMapa(mapID)
	if err != nil || mapa == nil {
		http.NotFound(w, r)
		return
	}
	baseJSON := emptyMapJSON()
	if mapa.CurrentVersionID.Valid {
		if version, err := a.DB.GetMunicipiMapaVersion(int(mapa.CurrentVersionID.Int64)); err == nil && version != nil {
			baseJSON = version.JSONData
		}
	} else {
		versionFilter := db.MunicipiMapaVersionFilter{
			MapaID: mapID,
			Limit:  1,
		}
		if versions, err := a.DB.ListMunicipiMapaVersions(versionFilter); err == nil && len(versions) > 0 {
			baseJSON = versions[0].JSONData
		}
	}
	next, err := a.DB.NextMunicipiMapaVersionNumber(mapID)
	if err != nil {
		http.Error(w, "failed to prepare version", http.StatusInternalServerError)
		return
	}
	createdBy := sql.NullInt64{Valid: false}
	if user != nil {
		createdBy = sql.NullInt64{Int64: int64(user.ID), Valid: true}
	}
	version := &db.MunicipiMapaVersion{
		MapaID:      mapID,
		Version:     next,
		Status:      "draft",
		JSONData:    baseJSON,
		Changelog:   "",
		LockVersion: 0,
		CreatedBy:   createdBy,
	}
	id, err := a.DB.CreateMunicipiMapaVersion(version)
	if err != nil {
		http.Error(w, "failed to create draft", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{"version_id": id})
}

func (a *App) municipiMapaVersionGet(w http.ResponseWriter, r *http.Request, versionID int) {
	version, err := a.DB.GetMunicipiMapaVersion(versionID)
	if err != nil || version == nil {
		http.NotFound(w, r)
		return
	}
	munID, err := a.DB.ResolveMunicipiIDByMapaVersionID(versionID)
	if err != nil || munID <= 0 {
		http.NotFound(w, r)
		return
	}
	user, _ := a.VerificarSessio(r)
	target := a.resolveMunicipiTarget(munID)
	canViewAll := user != nil && a.HasPermission(user.ID, permKeyTerritoriMunicipisMapesView, target)
	if version.Status != "publicat" && !canViewAll {
		if user == nil || !version.CreatedBy.Valid || int(version.CreatedBy.Int64) != user.ID {
			http.NotFound(w, r)
			return
		}
	}
	raw := json.RawMessage(version.JSONData)
	if !json.Valid(raw) {
		raw = json.RawMessage("{}")
	}
	view := municipiMapaVersionView{
		ID:          version.ID,
		MapaID:      version.MapaID,
		MunicipiID:  munID,
		Status:      version.Status,
		LockVersion: version.LockVersion,
		Data:        raw,
	}
	writeJSON(w, view)
}

func (a *App) municipiMapaVersionSave(w http.ResponseWriter, r *http.Request, versionID int) {
	version, err := a.DB.GetMunicipiMapaVersion(versionID)
	if err != nil || version == nil {
		http.NotFound(w, r)
		return
	}
	munID, err := a.DB.ResolveMunicipiIDByMapaVersionID(versionID)
	if err != nil || munID <= 0 {
		http.NotFound(w, r)
		return
	}
	target := a.resolveMunicipiTarget(munID)
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisMapesEdit, target)
	if !ok {
		return
	}
	if version.Status != "draft" {
		http.Error(w, "version is not draft", http.StatusBadRequest)
		return
	}
	if version.CreatedBy.Valid && user != nil && int(version.CreatedBy.Int64) != user.ID {
		http.Error(w, "not authorized", http.StatusForbidden)
		return
	}
	var payload municipiMapaSavePayload
	if err := decodeMunicipiMapaJSON(w, r, &payload); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	token := readCSRFToken(r, payload.CSRFToken)
	if !validateCSRF(r, token) {
		http.Error(w, "invalid csrf", http.StatusBadRequest)
		return
	}
	if !json.Valid(payload.Data) {
		http.Error(w, "invalid data", http.StatusBadRequest)
		return
	}
	if err := db.ValidateMapJSON(payload.Data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	newLock, err := a.DB.SaveMunicipiMapaDraft(versionID, string(payload.Data), strings.TrimSpace(payload.Changelog), payload.LockVersion)
	if err != nil {
		if err == db.ErrConflict {
			http.Error(w, "conflict", http.StatusConflict)
			return
		}
		http.Error(w, "failed to save", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{"ok": true, "lock_version": newLock})
}

func (a *App) municipiMapaVersionSubmit(w http.ResponseWriter, r *http.Request, versionID int) {
	version, err := a.DB.GetMunicipiMapaVersion(versionID)
	if err != nil || version == nil {
		http.NotFound(w, r)
		return
	}
	munID, err := a.DB.ResolveMunicipiIDByMapaVersionID(versionID)
	if err != nil || munID <= 0 {
		http.NotFound(w, r)
		return
	}
	target := a.resolveMunicipiTarget(munID)
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriMunicipisMapesSubmit, target)
	if !ok {
		return
	}
	if version.Status != "draft" {
		http.Error(w, "version is not draft", http.StatusBadRequest)
		return
	}
	if version.CreatedBy.Valid && user != nil && int(version.CreatedBy.Int64) != user.ID {
		http.Error(w, "not authorized", http.StatusForbidden)
		return
	}
	var payload municipiMapaSubmitPayload
	if err := decodeMunicipiMapaJSON(w, r, &payload); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	token := readCSRFToken(r, payload.CSRFToken)
	if !validateCSRF(r, token) {
		http.Error(w, "invalid csrf", http.StatusBadRequest)
		return
	}
	if !allowRouteLimit(r, "/api/mapes/versions/submit", 0.2, 2) {
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}
	if err := a.DB.UpdateMunicipiMapaVersionStatus(versionID, "pendent", "", 0); err != nil {
		http.Error(w, "failed to submit", http.StatusInternalServerError)
		return
	}
	if user != nil {
		_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleMunicipiMapaSubmit, "crear", mapModerationObjectType, &versionID, "pendent", nil, "")
	}
	writeJSON(w, map[string]interface{}{"ok": true})
}

func decodeMunicipiMapaJSON(w http.ResponseWriter, r *http.Request, payload interface{}) error {
	r.Body = http.MaxBytesReader(w, r.Body, municipiMapaMaxBytes)
	dec := json.NewDecoder(r.Body)
	return dec.Decode(payload)
}

func normalizeMunicipiMapaGroupType(value string) string {
	val := strings.ToLower(strings.TrimSpace(value))
	switch val {
	case "":
		return "actual"
	case "actual", "historic", "community":
		return val
	default:
		return ""
	}
}

func readCSRFToken(r *http.Request, payloadToken string) string {
	if strings.TrimSpace(payloadToken) != "" {
		return strings.TrimSpace(payloadToken)
	}
	if header := strings.TrimSpace(r.Header.Get("X-CSRF-Token")); header != "" {
		return header
	}
	return ""
}

func emptyMapJSON() string {
	model := map[string]interface{}{
		"viewBox": []int{0, 0, 1000, 700},
		"layers": map[string]interface{}{
			"houses":   []interface{}{},
			"streets":  []interface{}{},
			"rivers":   []interface{}{},
			"elements": []interface{}{},
			"toponyms": []interface{}{},
			"bounds":   []interface{}{},
		},
	}
	raw, _ := json.Marshal(model)
	return string(raw)
}

func nullIntToPtr(val sql.NullInt64) *int {
	if !val.Valid {
		return nil
	}
	v := int(val.Int64)
	return &v
}

func intToNullInt64(val int) sql.NullInt64 {
	if val == 0 {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(val), Valid: true}
}

func (a *App) MunicipiMapesListPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	munID, _, action, ok := parseMunicipiMapesPath(r.URL.Path)
	if !ok || action != "list" {
		http.NotFound(w, r)
		return
	}
	mun, err := a.DB.GetMunicipi(munID)
	if err != nil || mun == nil {
		http.NotFound(w, r)
		return
	}
	user, _ := a.VerificarSessio(r)
	if user != nil {
		*r = *a.withUser(r, user)
		if _, found := a.permissionsFromContext(r); !found {
			perms := a.getPermissionsForUser(user.ID)
			*r = *a.withPermissions(r, perms)
		}
	}
	target := a.resolveMunicipiTarget(munID)
	canCreate := user != nil && a.HasPermission(user.ID, permKeyTerritoriMunicipisMapesCreate, target)
	canEdit := user != nil && a.HasPermission(user.ID, permKeyTerritoriMunicipisMapesEdit, target)

	data := map[string]interface{}{
		"Municipi":  mun,
		"CanCreate": canCreate,
		"CanEdit":   canEdit,
		"User":      user,
	}
	if user != nil {
		RenderPrivateTemplate(w, r, "municipi-mapes-list.html", data)
		return
	}
	RenderTemplate(w, r, "municipi-mapes-list.html", data)
}

func (a *App) MunicipiMapaViewPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	munID, mapID, action, ok := parseMunicipiMapesPath(r.URL.Path)
	if !ok || action != "view" {
		http.NotFound(w, r)
		return
	}
	mun, err := a.DB.GetMunicipi(munID)
	if err != nil || mun == nil {
		http.NotFound(w, r)
		return
	}
	mapa, err := a.DB.GetMunicipiMapa(mapID)
	if err != nil || mapa == nil || mapa.MunicipiID != munID {
		http.NotFound(w, r)
		return
	}
	user, _ := a.VerificarSessio(r)
	if user != nil {
		*r = *a.withUser(r, user)
		if _, found := a.permissionsFromContext(r); !found {
			perms := a.getPermissionsForUser(user.ID)
			*r = *a.withPermissions(r, perms)
		}
	}
	target := a.resolveMunicipiTarget(munID)
	canViewAll := user != nil && a.HasPermission(user.ID, permKeyTerritoriMunicipisMapesView, target)
	if mapa.CurrentVersionID.Valid {
		version, err := a.DB.GetMunicipiMapaVersion(int(mapa.CurrentVersionID.Int64))
		if err != nil || version == nil {
			http.NotFound(w, r)
			return
		}
		if version.Status != "publicat" && !canViewAll {
			if user == nil || !version.CreatedBy.Valid || int(version.CreatedBy.Int64) != user.ID {
				http.NotFound(w, r)
				return
			}
		}
	} else if user != nil {
		versionFilter := db.MunicipiMapaVersionFilter{
			MapaID: mapa.ID,
			Limit:  1,
		}
		if !canViewAll {
			versionFilter.CreatedBy = user.ID
		}
		if versions, err := a.DB.ListMunicipiMapaVersions(versionFilter); err == nil && len(versions) > 0 {
			mapa.CurrentVersionID = sql.NullInt64{Int64: int64(versions[0].ID), Valid: true}
		}
	}
	data := map[string]interface{}{
		"Municipi": mun,
		"Mapa":     mapa,
		"User":     user,
	}
	if user != nil {
		RenderPrivateTemplate(w, r, "municipi-mapa-view.html", data)
		return
	}
	RenderTemplate(w, r, "municipi-mapa-view.html", data)
}

func (a *App) MunicipiMapaEditorPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	munID, mapID, action, ok := parseMunicipiMapesPath(r.URL.Path)
	if !ok || action != "editor" {
		http.NotFound(w, r)
		return
	}
	mun, err := a.DB.GetMunicipi(munID)
	if err != nil || mun == nil {
		http.NotFound(w, r)
		return
	}
	mapa, err := a.DB.GetMunicipiMapa(mapID)
	if err != nil || mapa == nil || mapa.MunicipiID != munID {
		http.NotFound(w, r)
		return
	}
	user, _ := a.VerificarSessio(r)
	if user == nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	*r = *a.withUser(r, user)
	if _, found := a.permissionsFromContext(r); !found {
		perms := a.getPermissionsForUser(user.ID)
		*r = *a.withPermissions(r, perms)
	}
	target := a.resolveMunicipiTarget(munID)
	if !a.HasPermission(user.ID, permKeyTerritoriMunicipisMapesEdit, target) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	data := map[string]interface{}{
		"Municipi": mun,
		"Mapa":     mapa,
		"User":     user,
	}
	RenderPrivateTemplate(w, r, "municipi-mapa-editor.html", data)
}

func parseMunicipiMapesPath(path string) (int, int, string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 4 {
		return 0, 0, "", false
	}
	if parts[0] != "territori" || parts[1] != "municipis" || parts[3] != "mapes" {
		return 0, 0, "", false
	}
	munID, err := strconv.Atoi(parts[2])
	if err != nil || munID <= 0 {
		return 0, 0, "", false
	}
	if len(parts) == 4 {
		return munID, 0, "list", true
	}
	mapID, err := strconv.Atoi(parts[4])
	if err != nil || mapID <= 0 {
		return 0, 0, "", false
	}
	if len(parts) == 5 {
		return munID, mapID, "view", true
	}
	if len(parts) == 6 && parts[5] == "editor" {
		return munID, mapID, "editor", true
	}
	return 0, 0, "", false
}
