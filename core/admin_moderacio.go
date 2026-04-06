package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type moderacioItem struct {
	ID         int
	Type       string
	Nom        string
	Context    string
	ContextURL string
	Autor      string
	AutorURL   string
	AutorID    int
	Created    string
	CreatedAt  time.Time
	Motiu      string
	EditURL    string
	Status     string
}

type moderacioTypeCount struct {
	Type  string `json:"type"`
	Total int    `json:"total"`
}

type moderacioSummary struct {
	Total        int                  `json:"total"`
	SLA0_24h     int                  `json:"sla_0_24h"`
	SLA1_3d      int                  `json:"sla_1_3d"`
	SLA3Plus     int                  `json:"sla_3d_plus"`
	TopType      string               `json:"top_type"`
	TopTypeTotal int                  `json:"top_type_total"`
	ByType       []moderacioTypeCount `json:"by_type"`
}

type moderacioFilters struct {
	Type      string
	Status    string
	AgeBucket string
	UserID    int
	UserQuery string
}

type moderacioBulkResult struct {
	Candidates int
	Total      int
	Processed  int
	Errors     int
	Skipped    int
}

type moderacioApplyMetrics struct {
	UpdateDur   time.Duration
	ActivityDur time.Duration
}

type moderacioBulkMetrics struct {
	ResolveDur  time.Duration
	UpdateDur   time.Duration
	ActivityDur time.Duration
	TotalDur    time.Duration
	Mode        string
	ScopeMode   string
	Revalidated bool
}

type moderacioBulkRegistreItemError struct {
	ID  int
	Err error
}

type moderacioBulkRegistreUpdateResult struct {
	SuccessIDs []int
	Updated    int
	Skipped    int
	Errors     []moderacioBulkRegistreItemError
}

type moderacioBulkRegistreChunkMetrics struct {
	ChunkIndex           int
	ChunkSize            int
	LoadedRows           int
	Updated              int
	Errors               int
	LoadDur              time.Duration
	UpdateDur            time.Duration
	DerivedDur           time.Duration
	DerivedDemografiaDur time.Duration
	DerivedStatsDur      time.Duration
	DerivedSearchDur     time.Duration
	ActivityDur          time.Duration
	AuditDur             time.Duration
	PostprocDur          time.Duration
	TotalDur             time.Duration
	Throughput           float64
	DeferredActivity     bool
}

type moderacioBulkRegistreState struct {
	Reg        db.TranscripcioRaw
	Persones   []db.TranscripcioPersonaRaw
	Llibre     *db.Llibre
	ArxiuID    int
	Delta      int
	MunicipiID int
	Year       int
	Tipus      string
}

type moderacioBulkRegistreDemoKey struct {
	MunicipiID int
	Year       int
	Tipus      string
	Delta      int
}

type moderacioBulkRegistreDerivedMetrics struct {
	DemografiaDur time.Duration
	StatsDur      time.Duration
	SearchDur     time.Duration
}

type moderacioTypeSpec struct {
	Key       string
	PermKey   string
	ListScope ScopeType
}

var moderacioTypeSpecs = map[string]moderacioTypeSpec{
	"persona":                   {Key: "persona", PermKey: permKeyPersonesModerate, ListScope: ScopeGlobal},
	"arxiu":                     {Key: "arxiu", PermKey: permKeyDocumentalsArxiusEdit, ListScope: ScopeArxiu},
	"llibre":                    {Key: "llibre", PermKey: permKeyDocumentalsLlibresEdit, ListScope: ScopeLlibre},
	"nivell":                    {Key: "nivell", PermKey: permKeyTerritoriNivellsEdit, ListScope: ScopePais},
	"municipi":                  {Key: "municipi", PermKey: permKeyTerritoriMunicipisEdit, ListScope: ScopeMunicipi},
	"eclesiastic":               {Key: "eclesiastic", PermKey: permKeyTerritoriEclesEdit, ListScope: ScopeEcles},
	"municipi_mapa_version":     {Key: "municipi_mapa_version", PermKey: permKeyTerritoriMunicipisMapesModerate, ListScope: ScopeMunicipi},
	"municipi_historia_general": {Key: "municipi_historia_general", PermKey: permKeyTerritoriMunicipisHistoriaModerate, ListScope: ScopeMunicipi},
	"municipi_historia_fet":     {Key: "municipi_historia_fet", PermKey: permKeyTerritoriMunicipisHistoriaModerate, ListScope: ScopeMunicipi},
	"municipi_anecdota_version": {Key: "municipi_anecdota_version", PermKey: permKeyTerritoriMunicipisAnecdotesModerate, ListScope: ScopeMunicipi},
	"event_historic":            {Key: "event_historic", PermKey: permKeyEventsModerate, ListScope: ScopeGlobal},
	"registre":                  {Key: "registre", PermKey: permKeyDocumentalsRegistresEdit, ListScope: ScopeLlibre},
	"registre_canvi":            {Key: "registre_canvi", PermKey: permKeyDocumentalsRegistresEdit, ListScope: ScopeLlibre},
	"cognom_variant":            {Key: "cognom_variant", PermKey: permKeyCognomsModerate, ListScope: ScopeGlobal},
	"cognom_referencia":         {Key: "cognom_referencia", PermKey: permKeyCognomsModerate, ListScope: ScopeGlobal},
	"cognom_merge":              {Key: "cognom_merge", PermKey: permKeyCognomsModerate, ListScope: ScopeGlobal},
	"media_album":               {Key: "media_album", PermKey: permKeyMediaModerate, ListScope: ScopeGlobal},
	"media_item":                {Key: "media_item", PermKey: permKeyMediaModerate, ListScope: ScopeGlobal},
	"external_link":             {Key: "external_link", PermKey: permKeyAdminExternalLinksModerate, ListScope: ScopeGlobal},
	"municipi_canvi":            {Key: "municipi_canvi", PermKey: permKeyTerritoriMunicipisEdit, ListScope: ScopeMunicipi},
	"arxiu_canvi":               {Key: "arxiu_canvi", PermKey: permKeyDocumentalsArxiusEdit, ListScope: ScopeArxiu},
	"llibre_canvi":              {Key: "llibre_canvi", PermKey: permKeyDocumentalsLlibresEdit, ListScope: ScopeLlibre},
	"persona_canvi":             {Key: "persona_canvi", PermKey: permKeyPersonesModerate, ListScope: ScopeGlobal},
	"cognom_canvi":              {Key: "cognom_canvi", PermKey: permKeyCognomsModerate, ListScope: ScopeGlobal},
	"event_historic_canvi":      {Key: "event_historic_canvi", PermKey: permKeyEventsModerate, ListScope: ScopeGlobal},
}

type moderacioScopeModel struct {
	app            *App
	user           *db.User
	perms          db.PolicyPermissions
	canModerateAll bool
	permScopes     map[string]listScopeFilter
}

func (a *App) newModeracioScopeModel(user *db.User, perms db.PolicyPermissions, canModerateAll bool) *moderacioScopeModel {
	model := &moderacioScopeModel{
		app:            a,
		user:           user,
		perms:          perms,
		canModerateAll: canModerateAll,
		permScopes:     map[string]listScopeFilter{},
	}
	if user == nil || canModerateAll {
		return model
	}
	seen := map[string]ScopeType{}
	for _, spec := range moderacioTypeSpecs {
		permKey := strings.TrimSpace(spec.PermKey)
		if permKey == "" {
			continue
		}
		if _, ok := seen[permKey]; ok {
			continue
		}
		seen[permKey] = spec.ListScope
		if !a.hasAnyPermissionKey(user.ID, permKey) {
			continue
		}
		model.permScopes[permKey] = a.buildListScopeFilter(user.ID, permKey, spec.ListScope)
	}
	return model
}

func (m *moderacioScopeModel) canModerateType(objType string) bool {
	if m == nil {
		return false
	}
	if m.canModerateAll {
		return true
	}
	spec, ok := moderacioTypeSpecs[objType]
	if !ok || strings.TrimSpace(spec.PermKey) == "" {
		return false
	}
	filter, ok := m.permScopes[spec.PermKey]
	if !ok {
		return false
	}
	return filter.hasGlobal || !filter.isEmpty()
}

func (m *moderacioScopeModel) scopeFilterForType(objType string) (listScopeFilter, bool) {
	if m == nil {
		return listScopeFilter{}, false
	}
	if m.canModerateAll {
		return listScopeFilter{hasGlobal: true}, true
	}
	spec, ok := moderacioTypeSpecs[objType]
	if !ok || strings.TrimSpace(spec.PermKey) == "" {
		return listScopeFilter{}, false
	}
	filter, ok := m.permScopes[spec.PermKey]
	if !ok {
		return listScopeFilter{}, false
	}
	if filter.hasGlobal || !filter.isEmpty() {
		return filter, true
	}
	return listScopeFilter{}, false
}

func (m *moderacioScopeModel) canModerateWikiChange(change db.WikiChange, objType string) bool {
	if m == nil || m.user == nil {
		return false
	}
	if m.canModerateAll {
		return true
	}
	switch objType {
	case "municipi_canvi":
		if change.ObjectType != "municipi" {
			return false
		}
		if !m.canModerateType("municipi_canvi") {
			return false
		}
		target := m.app.resolveMunicipiTarget(change.ObjectID)
		return m.app.HasPermission(m.user.ID, permKeyTerritoriMunicipisEdit, target)
	case "arxiu_canvi":
		if change.ObjectType != "arxiu" {
			return false
		}
		if !m.canModerateType("arxiu_canvi") {
			return false
		}
		target := m.app.resolveArxiuTarget(change.ObjectID)
		return m.app.HasPermission(m.user.ID, permKeyDocumentalsArxiusEdit, target)
	case "llibre_canvi":
		if change.ObjectType != "llibre" {
			return false
		}
		if !m.canModerateType("llibre_canvi") {
			return false
		}
		target := m.app.resolveLlibreTarget(change.ObjectID)
		return m.app.HasPermission(m.user.ID, permKeyDocumentalsLlibresEdit, target)
	case "persona_canvi":
		if change.ObjectType != "persona" {
			return false
		}
		return m.canModerateType("persona_canvi")
	case "cognom_canvi":
		if change.ObjectType != "cognom" {
			return false
		}
		return m.canModerateType("cognom_canvi")
	case "event_historic_canvi":
		if change.ObjectType != "event_historic" {
			return false
		}
		return m.canModerateType("event_historic_canvi")
	default:
		return false
	}
}

func (m *moderacioScopeModel) canModerateItem(objType string, id int) bool {
	if m == nil || m.user == nil {
		return false
	}
	if m.canModerateAll {
		return true
	}
	if !m.canModerateType(objType) {
		return false
	}
	switch objType {
	case "arxiu":
		target := m.app.resolveArxiuTarget(id)
		return m.app.HasPermission(m.user.ID, permKeyDocumentalsArxiusEdit, target)
	case "llibre":
		target := m.app.resolveLlibreTarget(id)
		return m.app.HasPermission(m.user.ID, permKeyDocumentalsLlibresEdit, target)
	case "nivell":
		lvl, err := m.app.DB.GetNivell(id)
		if err != nil || lvl == nil || lvl.PaisID <= 0 {
			return false
		}
		return m.app.HasPermission(m.user.ID, permKeyTerritoriNivellsEdit, PermissionTarget{PaisID: intPtr(lvl.PaisID)})
	case "municipi":
		target := m.app.resolveMunicipiTarget(id)
		return m.app.HasPermission(m.user.ID, permKeyTerritoriMunicipisEdit, target)
	case "eclesiastic":
		return m.app.HasPermission(m.user.ID, permKeyTerritoriEclesEdit, PermissionTarget{EclesID: intPtr(id)})
	case "municipi_historia_general":
		munID, err := m.app.DB.ResolveMunicipiIDByHistoriaGeneralVersionID(id)
		if err != nil || munID <= 0 {
			return false
		}
		target := m.app.resolveMunicipiTarget(munID)
		return m.app.HasPermission(m.user.ID, permKeyTerritoriMunicipisHistoriaModerate, target)
	case "municipi_historia_fet":
		munID, err := m.app.DB.ResolveMunicipiIDByHistoriaFetVersionID(id)
		if err != nil || munID <= 0 {
			return false
		}
		target := m.app.resolveMunicipiTarget(munID)
		return m.app.HasPermission(m.user.ID, permKeyTerritoriMunicipisHistoriaModerate, target)
	case "municipi_anecdota_version":
		munID, err := m.app.DB.ResolveMunicipiIDByAnecdotariVersionID(id)
		if err != nil || munID <= 0 {
			return false
		}
		target := m.app.resolveMunicipiTarget(munID)
		return m.app.HasPermission(m.user.ID, permKeyTerritoriMunicipisAnecdotesModerate, target)
	case "municipi_mapa_version":
		munID, err := m.app.DB.ResolveMunicipiIDByMapaVersionID(id)
		if err != nil || munID <= 0 {
			return false
		}
		target := m.app.resolveMunicipiTarget(munID)
		return m.app.HasPermission(m.user.ID, permKeyTerritoriMunicipisMapesModerate, target)
	case "registre":
		reg, err := m.app.DB.GetTranscripcioRaw(id)
		if err != nil || reg == nil {
			return false
		}
		target := m.app.resolveLlibreTarget(reg.LlibreID)
		return m.app.HasPermission(m.user.ID, permKeyDocumentalsRegistresEdit, target)
	case "registre_canvi":
		change, err := m.app.DB.GetTranscripcioRawChange(id)
		if err != nil || change == nil {
			return false
		}
		reg, err := m.app.DB.GetTranscripcioRaw(change.TranscripcioID)
		if err != nil || reg == nil {
			return false
		}
		target := m.app.resolveLlibreTarget(reg.LlibreID)
		return m.app.HasPermission(m.user.ID, permKeyDocumentalsRegistresEdit, target)
	case "municipi_canvi", "arxiu_canvi", "llibre_canvi":
		change, err := m.app.DB.GetWikiChange(id)
		if err != nil || change == nil {
			return false
		}
		return m.canModerateWikiChange(*change, objType)
	case "persona_canvi", "cognom_canvi", "event_historic_canvi":
		change, err := m.app.DB.GetWikiChange(id)
		if err != nil || change == nil {
			return false
		}
		return m.canModerateWikiChange(*change, objType)
	case "external_link":
		return m.canModerateType("external_link")
	case "persona", "event_historic", "cognom_variant", "cognom_referencia", "cognom_merge", "media_album", "media_item":
		return m.canModerateType(objType)
	default:
		return false
	}
}

func (m *moderacioScopeModel) allowedTypes() []string {
	if m == nil {
		return nil
	}
	if m.canModerateAll {
		return append([]string{}, moderacioBulkAllowedTypes...)
	}
	types := make([]string, 0, len(moderacioBulkAllowedTypes))
	for _, objType := range moderacioBulkAllowedTypes {
		if m.canModerateType(objType) {
			types = append(types, objType)
		}
	}
	return types
}

func (m *moderacioScopeModel) canModerateAnything() bool {
	if m == nil {
		return false
	}
	if m.canModerateAll {
		return true
	}
	for objType, spec := range moderacioTypeSpecs {
		if strings.TrimSpace(spec.PermKey) == "" {
			continue
		}
		if m.canModerateType(objType) {
			return true
		}
	}
	return false
}

func applyScopeFilterToArxiu(filter *db.ArxiuFilter, scope listScopeFilter) {
	if filter == nil {
		return
	}
	filter.AllowedArxiuIDs = scope.arxiuIDs
	filter.AllowedMunicipiIDs = scope.municipiIDs
	filter.AllowedProvinciaIDs = scope.provinciaIDs
	filter.AllowedComarcaIDs = scope.comarcaIDs
	filter.AllowedNivellIDs = scope.nivellIDs
	filter.AllowedPaisIDs = scope.paisIDs
	filter.AllowedEclesIDs = scope.eclesIDs
}

func applyScopeFilterToLlibre(filter *db.LlibreFilter, scope listScopeFilter) {
	if filter == nil {
		return
	}
	filter.AllowedLlibreIDs = scope.llibreIDs
	filter.AllowedArxiuIDs = scope.arxiuIDs
	filter.AllowedMunicipiIDs = scope.municipiIDs
	filter.AllowedProvinciaIDs = scope.provinciaIDs
	filter.AllowedComarcaIDs = scope.comarcaIDs
	filter.AllowedNivellIDs = scope.nivellIDs
	filter.AllowedPaisIDs = scope.paisIDs
	filter.AllowedEclesIDs = scope.eclesIDs
}

func applyScopeFilterToMunicipi(filter *db.MunicipiFilter, scope listScopeFilter) {
	if filter == nil {
		return
	}
	filter.AllowedMunicipiIDs = scope.municipiIDs
	filter.AllowedProvinciaIDs = scope.provinciaIDs
	filter.AllowedComarcaIDs = scope.comarcaIDs
	filter.AllowedNivellIDs = scope.nivellIDs
	filter.AllowedPaisIDs = scope.paisIDs
}

func applyScopeFilterToNivell(filter *db.NivellAdminFilter, scope listScopeFilter) {
	if filter == nil {
		return
	}
	filter.AllowedPaisIDs = scope.paisIDs
}

func applyScopeFilterToEcles(filter *db.ArquebisbatFilter, scope listScopeFilter) {
	if filter == nil {
		return
	}
	filter.AllowedEclesIDs = scope.eclesIDs
	filter.AllowedPaisIDs = scope.paisIDs
}

func applyScopeFilterToRegistre(filter *db.TranscripcioFilter, scope listScopeFilter) {
	if filter == nil {
		return
	}
	filter.AllowedLlibreIDs = scope.llibreIDs
	filter.AllowedArxiuIDs = scope.arxiuIDs
	filter.AllowedMunicipiIDs = scope.municipiIDs
	filter.AllowedProvinciaIDs = scope.provinciaIDs
	filter.AllowedComarcaIDs = scope.comarcaIDs
	filter.AllowedNivellIDs = scope.nivellIDs
	filter.AllowedPaisIDs = scope.paisIDs
	filter.AllowedEclesIDs = scope.eclesIDs
}

const (
	moderacioAge0_24h = "0_24h"
	moderacioAge1_3d  = "1_3d"
	moderacioAge3Plus = "3d_plus"
)

var moderacioBulkAllowedTypes = []string{
	"persona",
	"arxiu",
	"llibre",
	"nivell",
	"municipi",
	"eclesiastic",
	"municipi_mapa_version",
	"municipi_historia_general",
	"municipi_historia_fet",
	"municipi_anecdota_version",
	"event_historic",
	"registre",
	"registre_canvi",
	"cognom_variant",
	"cognom_referencia",
	"cognom_merge",
	"media_album",
	"media_item",
	"external_link",
	"municipi_canvi",
	"arxiu_canvi",
	"llibre_canvi",
	"persona_canvi",
	"cognom_canvi",
	"event_historic_canvi",
}

func isValidModeracioBulkAction(action string) bool {
	switch action {
	case "approve", "reject":
		return true
	default:
		return false
	}
}

func isValidModeracioBulkScope(scope string) bool {
	switch scope {
	case "page", "all":
		return true
	default:
		return false
	}
}

func isValidModeracioBulkType(bulkType string) bool {
	if bulkType == "" || bulkType == "all" {
		return true
	}
	for _, allowed := range moderacioBulkAllowedTypes {
		if bulkType == allowed {
			return true
		}
	}
	return false
}

func parseModeracioPagination(values url.Values) (int, int) {
	page := 1
	perPage := 25
	if val := strings.TrimSpace(values.Get("page")); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			page = n
		}
	}
	if val := strings.TrimSpace(values.Get("per_page")); val != "" {
		if n, err := strconv.Atoi(val); err == nil {
			switch n {
			case 10, 25, 50, 100:
				perPage = n
			}
		}
	}
	return page, perPage
}

func parseModeracioReturnTo(returnTo string) (moderacioFilters, int, int) {
	if strings.TrimSpace(returnTo) == "" {
		req := &http.Request{URL: &url.URL{}}
		filters, _ := parseModeracioFilters(req)
		return filters, 1, 25
	}
	parsed, err := url.Parse(returnTo)
	if err != nil {
		req := &http.Request{URL: &url.URL{}}
		filters, _ := parseModeracioFilters(req)
		return filters, 1, 25
	}
	req := &http.Request{URL: &url.URL{RawQuery: parsed.RawQuery}}
	filters, _ := parseModeracioFilters(req)
	page, perPage := parseModeracioPagination(parsed.Query())
	return filters, page, perPage
}

func moderacioAgeBucket(createdAt time.Time, now time.Time) string {
	if createdAt.IsZero() {
		return ""
	}
	age := now.Sub(createdAt)
	if age <= 24*time.Hour {
		return moderacioAge0_24h
	}
	if age <= 72*time.Hour {
		return moderacioAge1_3d
	}
	return moderacioAge3Plus
}

func moderacioStatusFromMedia(status string) string {
	switch strings.TrimSpace(status) {
	case "pending":
		return "pendent"
	case "approved":
		return "publicat"
	case "rejected":
		return "rebutjat"
	default:
		return status
	}
}

func mediaStatusFromModeracio(status string) string {
	switch strings.TrimSpace(status) {
	case "pendent":
		return "pending"
	case "publicat":
		return "approved"
	case "rebutjat":
		return "rejected"
	default:
		return status
	}
}

func moderacioStatusFromExternalLink(status string) string {
	switch strings.TrimSpace(status) {
	case "pending":
		return "pendent"
	case "approved":
		return "publicat"
	case "rejected":
		return "rebutjat"
	default:
		return status
	}
}

func externalLinkStatusFromModeracio(status string) string {
	switch strings.TrimSpace(status) {
	case "pendent":
		return "pending"
	case "publicat":
		return "approved"
	case "rebutjat":
		return "rejected"
	default:
		return status
	}
}

type moderacioBuildMetrics struct {
	countDur     time.Duration
	listDur      time.Duration
	listFetchDur time.Duration
	listBuildDur time.Duration
}

func (a *App) buildModeracioItems(lang string, page, perPage int, user *db.User, perms db.PolicyPermissions, canModerateAll bool, filters moderacioFilters, metrics *moderacioBuildMetrics) ([]moderacioItem, int, moderacioSummary, error) {
	var items []moderacioItem
	userCache := map[int]*db.User{}
	autorFromID := func(id sql.NullInt64) (string, string, int) {
		if !id.Valid {
			return "—", "", 0
		}
		uid := int(id.Int64)
		if cached, ok := userCache[uid]; ok {
			username := strings.TrimSpace(cached.Usuari)
			if username == "" {
				full := strings.TrimSpace(strings.TrimSpace(cached.Name) + " " + strings.TrimSpace(cached.Surname))
				if full != "" {
					username = full
				}
			}
			if username == "" {
				username = "—"
			}
			return username, "/u/" + strconv.Itoa(cached.ID), cached.ID
		}
		u, err := a.DB.GetUserByID(uid)
		if err != nil || u == nil {
			return "—", "", 0
		}
		userCache[uid] = u
		username := strings.TrimSpace(u.Usuari)
		if username == "" {
			full := strings.TrimSpace(strings.TrimSpace(u.Name) + " " + strings.TrimSpace(u.Surname))
			if full != "" {
				username = full
			}
		}
		if username == "" {
			username = "—"
		}
		return username, "/u/" + strconv.Itoa(u.ID), u.ID
	}

	statusFilter := strings.TrimSpace(filters.Status)
	statusAll := statusFilter == "" || statusFilter == "all"
	typeFilter := strings.TrimSpace(filters.Type)
	ageFilter := strings.TrimSpace(filters.AgeBucket)
	userQuery := strings.TrimSpace(filters.UserQuery)
	userID := filters.UserID
	now := time.Now()
	scopeModel := a.newModeracioScopeModel(user, perms, canModerateAll)
	typeAllowed := func(objType string) bool {
		if typeFilter != "" && typeFilter != "all" && typeFilter != objType {
			return false
		}
		return scopeModel.canModerateType(objType)
	}

	var userIDs []int
	if userID > 0 {
		userIDs = []int{userID}
	} else if userQuery != "" {
		rows, _, err := a.loadUsersAdmin(db.UserAdminFilter{Query: userQuery})
		if err != nil {
			return nil, 0, moderacioSummary{}, err
		}
		for _, row := range rows {
			userIDs = append(userIDs, row.ID)
		}
		if len(userIDs) == 0 {
			return []moderacioItem{}, 0, moderacioSummary{}, nil
		}
	}

	var createdAfter time.Time
	var createdBefore time.Time
	if ageFilter != "" {
		switch ageFilter {
		case moderacioAge0_24h:
			createdAfter = now.Add(-24 * time.Hour)
			createdBefore = now.Add(1 * time.Second)
		case moderacioAge1_3d:
			createdAfter = now.Add(-72 * time.Hour)
			createdBefore = now.Add(-24 * time.Hour)
		case moderacioAge3Plus:
			createdBefore = now.Add(-72 * time.Hour)
		}
	}

	skipMedia := ageFilter != ""
	pendingOnly := statusAll || statusFilter == "pendent"

	status := ""
	if !statusAll {
		status = statusFilter
	}

	personaFilter := db.PersonaFilter{
		Estat:         status,
		CreatedByIDs:  userIDs,
		CreatedAfter:  createdAfter,
		CreatedBefore: createdBefore,
	}
	arxiuFilter := db.ArxiuFilter{
		Status:        status,
		CreatedByIDs:  userIDs,
		CreatedAfter:  createdAfter,
		CreatedBefore: createdBefore,
	}
	if scope, ok := scopeModel.scopeFilterForType("arxiu"); ok && !scope.hasGlobal {
		applyScopeFilterToArxiu(&arxiuFilter, scope)
	}
	llibreFilter := db.LlibreFilter{
		Status:        status,
		CreatedByIDs:  userIDs,
		CreatedAfter:  createdAfter,
		CreatedBefore: createdBefore,
	}
	if scope, ok := scopeModel.scopeFilterForType("llibre"); ok && !scope.hasGlobal {
		applyScopeFilterToLlibre(&llibreFilter, scope)
	}
	nivellFilter := db.NivellAdminFilter{
		Status:        status,
		CreatedByIDs:  userIDs,
		CreatedAfter:  createdAfter,
		CreatedBefore: createdBefore,
	}
	if scope, ok := scopeModel.scopeFilterForType("nivell"); ok && !scope.hasGlobal {
		applyScopeFilterToNivell(&nivellFilter, scope)
	}
	municipiFilter := db.MunicipiFilter{
		Status:        status,
		CreatedByIDs:  userIDs,
		CreatedAfter:  createdAfter,
		CreatedBefore: createdBefore,
	}
	if scope, ok := scopeModel.scopeFilterForType("municipi"); ok && !scope.hasGlobal {
		applyScopeFilterToMunicipi(&municipiFilter, scope)
	}
	eclesFilter := db.ArquebisbatFilter{
		Status:        status,
		CreatedByIDs:  userIDs,
		CreatedAfter:  createdAfter,
		CreatedBefore: createdBefore,
	}
	if scope, ok := scopeModel.scopeFilterForType("eclesiastic"); ok && !scope.hasGlobal {
		applyScopeFilterToEcles(&eclesFilter, scope)
	}

	mediaStatus := ""
	if !statusAll {
		mediaStatus = mediaStatusFromModeracio(statusFilter)
	}
	mediaFilter := db.MediaModeracioFilter{
		Status:        mediaStatus,
		OwnerIDs:      userIDs,
		CreatedAfter:  createdAfter,
		CreatedBefore: createdBefore,
	}

	mapFilter := db.MunicipiMapaVersionFilter{
		Status:        status,
		CreatedByIDs:  userIDs,
		CreatedAfter:  createdAfter,
		CreatedBefore: createdBefore,
	}

	externalStatus := ""
	if !statusAll {
		externalStatus = externalLinkStatusFromModeracio(statusFilter)
	}
	externalFilter := db.ExternalLinkAdminFilter{
		Status:        externalStatus,
		CreatedByIDs:  userIDs,
		CreatedAfter:  createdAfter,
		CreatedBefore: createdBefore,
	}

	cognomVariantFilter := db.CognomVariantFilter{
		Status:        status,
		CreatedByIDs:  userIDs,
		CreatedAfter:  createdAfter,
		CreatedBefore: createdBefore,
	}
	cognomRefFilter := db.CognomReferenciaFilter{
		Status:        status,
		CreatedByIDs:  userIDs,
		CreatedAfter:  createdAfter,
		CreatedBefore: createdBefore,
	}
	cognomMergeFilter := db.CognomRedirectSuggestionFilter{
		Status:        status,
		CreatedByIDs:  userIDs,
		CreatedAfter:  createdAfter,
		CreatedBefore: createdBefore,
	}
	eventFilter := db.EventHistoricFilter{
		Status:        status,
		CreatedByIDs:  userIDs,
		CreatedAfter:  createdAfter,
		CreatedBefore: createdBefore,
	}

	registreFilter := db.TranscripcioFilter{
		Status:        status,
		CreatedByIDs:  userIDs,
		CreatedAfter:  createdAfter,
		CreatedBefore: createdBefore,
	}
	if scope, ok := scopeModel.scopeFilterForType("registre"); ok && !scope.hasGlobal {
		applyScopeFilterToRegistre(&registreFilter, scope)
	}

	changeFilter := db.TranscripcioFilter{
		Status:        status,
		CreatedByIDs:  userIDs,
		CreatedAfter:  createdAfter,
		CreatedBefore: createdBefore,
	}
	if scope, ok := scopeModel.scopeFilterForType("registre_canvi"); ok && !scope.hasGlobal {
		applyScopeFilterToRegistre(&changeFilter, scope)
	}

	countStart := time.Now()
	typeCounts := map[string]int{}
	summary := moderacioSummary{}

	if typeAllowed("persona") {
		if total, err := a.DB.CountPersones(personaFilter); err != nil {
			return nil, 0, moderacioSummary{}, err
		} else if total > 0 {
			typeCounts["persona"] = total
		}
	}
	if typeAllowed("arxiu") {
		if total, err := a.DB.CountArxius(arxiuFilter); err != nil {
			return nil, 0, moderacioSummary{}, err
		} else if total > 0 {
			typeCounts["arxiu"] = total
		}
	}
	if typeAllowed("llibre") {
		if total, err := a.DB.CountLlibres(llibreFilter); err != nil {
			return nil, 0, moderacioSummary{}, err
		} else if total > 0 {
			typeCounts["llibre"] = total
		}
	}
	if typeAllowed("nivell") {
		if total, err := a.DB.CountNivells(nivellFilter); err != nil {
			return nil, 0, moderacioSummary{}, err
		} else if total > 0 {
			typeCounts["nivell"] = total
		}
	}
	if typeAllowed("municipi") {
		if total, err := a.DB.CountMunicipis(municipiFilter); err != nil {
			return nil, 0, moderacioSummary{}, err
		} else if total > 0 {
			typeCounts["municipi"] = total
		}
	}
	if typeAllowed("eclesiastic") {
		if total, err := a.DB.CountArquebisbats(eclesFilter); err != nil {
			return nil, 0, moderacioSummary{}, err
		} else if total > 0 {
			typeCounts["eclesiastic"] = total
		}
	}
	if typeAllowed("media_album") && !skipMedia {
		if total, err := a.DB.CountMediaAlbumsModeracio(mediaFilter); err != nil {
			return nil, 0, moderacioSummary{}, err
		} else if total > 0 {
			typeCounts["media_album"] = total
		}
	}
	if typeAllowed("media_item") && !skipMedia {
		if total, err := a.DB.CountMediaItemsModeracio(mediaFilter); err != nil {
			return nil, 0, moderacioSummary{}, err
		} else if total > 0 {
			typeCounts["media_item"] = total
		}
	}
	if typeAllowed("municipi_mapa_version") {
		scope, _ := scopeModel.scopeFilterForType("municipi_mapa_version")
		mapScope := db.MunicipiScopeFilter{
			AllowedMunicipiIDs:  scope.municipiIDs,
			AllowedProvinciaIDs: scope.provinciaIDs,
			AllowedComarcaIDs:   scope.comarcaIDs,
			AllowedNivellIDs:    scope.nivellIDs,
			AllowedPaisIDs:      scope.paisIDs,
		}
		if total, err := a.DB.CountMunicipiMapaVersionsScoped(mapFilter, mapScope); err != nil {
			return nil, 0, moderacioSummary{}, err
		} else if total > 0 {
			typeCounts["municipi_mapa_version"] = total
		}
	}
	if typeAllowed("external_link") {
		if total, err := a.DB.CountExternalLinksAdmin(externalFilter); err != nil {
			return nil, 0, moderacioSummary{}, err
		} else if total > 0 {
			typeCounts["external_link"] = total
		}
	}
	if canModerateAll && typeAllowed("cognom_variant") {
		if total, err := a.DB.CountCognomVariants(cognomVariantFilter); err != nil {
			return nil, 0, moderacioSummary{}, err
		} else if total > 0 {
			typeCounts["cognom_variant"] = total
		}
	}
	if canModerateAll && typeAllowed("cognom_referencia") {
		if total, err := a.DB.CountCognomReferencies(cognomRefFilter); err != nil {
			return nil, 0, moderacioSummary{}, err
		} else if total > 0 {
			typeCounts["cognom_referencia"] = total
		}
	}
	if canModerateAll && typeAllowed("cognom_merge") {
		if total, err := a.DB.CountCognomRedirectSuggestions(cognomMergeFilter); err != nil {
			return nil, 0, moderacioSummary{}, err
		} else if total > 0 {
			typeCounts["cognom_merge"] = total
		}
	}
	if canModerateAll && typeAllowed("event_historic") {
		if total, err := a.DB.CountEventsHistoric(eventFilter); err != nil {
			return nil, 0, moderacioSummary{}, err
		} else if total > 0 {
			typeCounts["event_historic"] = total
		}
	}
	if typeAllowed("registre") {
		if total, err := a.DB.CountTranscripcionsRawGlobal(registreFilter); err != nil {
			return nil, 0, moderacioSummary{}, err
		} else if total > 0 {
			typeCounts["registre"] = total
		}
	}
	if typeAllowed("registre_canvi") && pendingOnly {
		if total, err := a.DB.CountTranscripcioRawChangesPendingScoped(changeFilter); err != nil {
			return nil, 0, moderacioSummary{}, err
		} else if total > 0 {
			typeCounts["registre_canvi"] = total
		}
	}
	if typeAllowed("municipi_historia_general") && pendingOnly {
		total, err := a.countModeracioHistoriaGeneral(userIDs, createdAfter, createdBefore, scopeModel, canModerateAll)
		if err != nil {
			return nil, 0, moderacioSummary{}, err
		}
		if total > 0 {
			typeCounts["municipi_historia_general"] = total
		}
	}
	if typeAllowed("municipi_historia_fet") && pendingOnly {
		total, err := a.countModeracioHistoriaFets(userIDs, createdAfter, createdBefore, scopeModel, canModerateAll)
		if err != nil {
			return nil, 0, moderacioSummary{}, err
		}
		if total > 0 {
			typeCounts["municipi_historia_fet"] = total
		}
	}
	if typeAllowed("municipi_anecdota_version") && pendingOnly {
		total, err := a.countModeracioAnecdotes(userIDs, createdAfter, createdBefore, scopeModel, canModerateAll)
		if err != nil {
			return nil, 0, moderacioSummary{}, err
		}
		if total > 0 {
			typeCounts["municipi_anecdota_version"] = total
		}
	}

	needsWikiChanges := typeAllowed("municipi_canvi") || typeAllowed("arxiu_canvi") || typeAllowed("llibre_canvi") || typeAllowed("persona_canvi") || typeAllowed("cognom_canvi") || typeAllowed("event_historic_canvi")
	if needsWikiChanges && pendingOnly {
		wikiCounts, err := a.countModeracioWikiChanges(userIDs, createdAfter, createdBefore, scopeModel, canModerateAll, typeAllowed)
		if err != nil {
			return nil, 0, moderacioSummary{}, err
		}
		for objType, count := range wikiCounts {
			if count > 0 {
				typeCounts[objType] = count
			}
		}
	}

	summary.Total = 0
	for _, count := range typeCounts {
		summary.Total += count
	}

	if ageFilter == "" && summary.Total > 0 {
		summary.SLA0_24h = a.countModeracioByAgeBucket(filters, scopeModel, canModerateAll, moderacioAge0_24h, userIDs, now)
		summary.SLA1_3d = a.countModeracioByAgeBucket(filters, scopeModel, canModerateAll, moderacioAge1_3d, userIDs, now)
		summary.SLA3Plus = a.countModeracioByAgeBucket(filters, scopeModel, canModerateAll, moderacioAge3Plus, userIDs, now)
	} else if ageFilter != "" {
		switch ageFilter {
		case moderacioAge0_24h:
			summary.SLA0_24h = summary.Total
		case moderacioAge1_3d:
			summary.SLA1_3d = summary.Total
		case moderacioAge3Plus:
			summary.SLA3Plus = summary.Total
		}
	}

	if summary.Total > 0 {
		byType := make([]moderacioTypeCount, 0, len(typeCounts))
		for key, count := range typeCounts {
			if count > 0 {
				byType = append(byType, moderacioTypeCount{Type: key, Total: count})
			}
		}
		sort.Slice(byType, func(i, j int) bool {
			if byType[i].Total == byType[j].Total {
				return byType[i].Type < byType[j].Type
			}
			return byType[i].Total > byType[j].Total
		})
		if len(byType) > 0 {
			summary.ByType = byType
			summary.TopType = byType[0].Type
			summary.TopTypeTotal = byType[0].Total
		}
	}

	if metrics != nil {
		metrics.countDur = time.Since(countStart)
	}

	listStart := time.Now()
	start := (page - 1) * perPage
	if start < 0 {
		start = 0
	}
	remaining := perPage
	skip := start

	typeOrder := []string{
		"persona",
		"arxiu",
		"llibre",
		"nivell",
		"municipi",
		"eclesiastic",
		"media_album",
		"media_item",
		"municipi_mapa_version",
		"external_link",
		"municipi_historia_general",
		"municipi_historia_fet",
		"municipi_anecdota_version",
		"cognom_variant",
		"cognom_referencia",
		"cognom_merge",
		"event_historic",
		"registre",
		"registre_canvi",
		"wiki_change",
	}

	wikiTotal := 0
	for _, key := range []string{"municipi_canvi", "arxiu_canvi", "llibre_canvi", "persona_canvi", "cognom_canvi", "event_historic_canvi"} {
		wikiTotal += typeCounts[key]
	}

	for _, objType := range typeOrder {
		typeCount := 0
		if objType == "wiki_change" {
			typeCount = wikiTotal
		} else {
			typeCount = typeCounts[objType]
		}
		if typeCount == 0 {
			continue
		}
		if skip >= typeCount {
			skip -= typeCount
			continue
		}
		limit := minInt(typeCount-skip, remaining)
		offset := skip
		var fetched []moderacioItem
		var err error

		switch objType {
		case "persona":
			fetched, err = a.listModeracioPersones(personaFilter, offset, limit, autorFromID, metrics)
		case "arxiu":
			fetched, err = a.listModeracioArxius(arxiuFilter, offset, limit, autorFromID, metrics)
		case "llibre":
			fetched, err = a.listModeracioLlibres(llibreFilter, offset, limit, autorFromID, metrics)
		case "nivell":
			fetched, err = a.listModeracioNivells(nivellFilter, offset, limit, autorFromID, metrics)
		case "municipi":
			fetched, err = a.listModeracioMunicipis(municipiFilter, offset, limit, autorFromID, metrics)
		case "eclesiastic":
			fetched, err = a.listModeracioEclesiastics(eclesFilter, offset, limit, autorFromID, metrics)
		case "media_album":
			if !skipMedia {
				fetched, err = a.listModeracioMediaAlbums(mediaFilter, offset, limit, autorFromID, metrics)
			}
		case "media_item":
			if !skipMedia {
				fetched, err = a.listModeracioMediaItems(mediaFilter, offset, limit, autorFromID, metrics)
			}
		case "municipi_mapa_version":
			fetched, err = a.listModeracioMunicipiMapaVersions(mapFilter, offset, limit, autorFromID, scopeModel, canModerateAll, metrics)
		case "external_link":
			fetched, err = a.listModeracioExternalLinks(lang, externalFilter, offset, limit, autorFromID, metrics)
		case "municipi_historia_general":
			fetched, err = a.listModeracioHistoriaGeneral(lang, offset, limit, userIDs, createdAfter, createdBefore, autorFromID, scopeModel, canModerateAll, metrics)
		case "municipi_historia_fet":
			fetched, err = a.listModeracioHistoriaFets(lang, offset, limit, userIDs, createdAfter, createdBefore, autorFromID, scopeModel, canModerateAll, metrics)
		case "municipi_anecdota_version":
			fetched, err = a.listModeracioAnecdotes(lang, offset, limit, userIDs, createdAfter, createdBefore, autorFromID, scopeModel, canModerateAll, metrics)
		case "cognom_variant":
			if canModerateAll {
				fetched, err = a.listModeracioCognomVariants(cognomVariantFilter, offset, limit, autorFromID, metrics)
			}
		case "cognom_referencia":
			if canModerateAll {
				fetched, err = a.listModeracioCognomReferencies(cognomRefFilter, offset, limit, autorFromID, metrics)
			}
		case "cognom_merge":
			if canModerateAll {
				fetched, err = a.listModeracioCognomMerges(cognomMergeFilter, offset, limit, autorFromID, metrics)
			}
		case "event_historic":
			if canModerateAll {
				fetched, err = a.listModeracioEvents(lang, eventFilter, offset, limit, autorFromID, metrics)
			}
		case "registre":
			fetched, err = a.listModeracioRegistres(registreFilter, offset, limit, autorFromID, metrics)
		case "registre_canvi":
			if pendingOnly {
				fetched, err = a.listModeracioRegistreCanvis(changeFilter, offset, limit, autorFromID, metrics)
			}
		case "wiki_change":
			if pendingOnly {
				fetched, err = a.listModeracioWikiChanges(lang, offset, limit, userIDs, createdAfter, createdBefore, autorFromID, scopeModel, canModerateAll, typeAllowed, metrics)
			}
		}
		if err != nil {
			return nil, 0, moderacioSummary{}, err
		}
		if len(fetched) > 0 {
			items = append(items, fetched...)
		}
		remaining -= len(fetched)
		skip = 0
		if remaining <= 0 {
			break
		}
	}

	if metrics != nil {
		metrics.listDur = time.Since(listStart)
	}

	return items, summary.Total, summary, nil
}

func (a *App) listModeracioPersones(filter db.PersonaFilter, offset, limit int, autorFromID func(sql.NullInt64) (string, string, int), metrics *moderacioBuildMetrics) ([]moderacioItem, error) {
	if limit <= 0 {
		return []moderacioItem{}, nil
	}
	filter.Limit = limit
	filter.Offset = offset
	fetchStart := time.Now()
	rows, err := a.DB.ListPersones(filter)
	if metrics != nil {
		metrics.listFetchDur += time.Since(fetchStart)
	}
	if err != nil {
		return nil, err
	}
	buildStart := time.Now()
	items := make([]moderacioItem, 0, len(rows))
	for _, p := range rows {
		created := ""
		var createdAt time.Time
		if p.CreatedAt.Valid {
			created = p.CreatedAt.Time.Format("2006-01-02 15:04")
			createdAt = p.CreatedAt.Time
		}
		context := strings.TrimSpace(fmt.Sprintf("%s %s", p.Llibre, p.Pagina))
		if context == "" {
			context = p.Municipi
		}
		autorNom, autorURL, autorID := autorFromID(p.CreatedBy)
		items = append(items, moderacioItem{
			ID:        p.ID,
			Type:      "persona",
			Nom:       strings.TrimSpace(strings.Join([]string{p.Nom, p.Cognom1, p.Cognom2}, " ")),
			Context:   context,
			Autor:     autorNom,
			AutorURL:  autorURL,
			AutorID:   autorID,
			Created:   created,
			CreatedAt: createdAt,
			Motiu:     p.ModeracioMotiu,
			EditURL:   fmt.Sprintf("/persones/%d?return_to=/moderacio", p.ID),
			Status:    p.ModeracioEstat,
		})
	}
	if metrics != nil {
		metrics.listBuildDur += time.Since(buildStart)
	}
	return items, nil
}

func (a *App) listModeracioArxius(filter db.ArxiuFilter, offset, limit int, autorFromID func(sql.NullInt64) (string, string, int), metrics *moderacioBuildMetrics) ([]moderacioItem, error) {
	if limit <= 0 {
		return []moderacioItem{}, nil
	}
	filter.Limit = limit
	filter.Offset = offset
	fetchStart := time.Now()
	rows, err := a.DB.ListArxius(filter)
	if metrics != nil {
		metrics.listFetchDur += time.Since(fetchStart)
	}
	if err != nil {
		return nil, err
	}
	buildStart := time.Now()
	items := make([]moderacioItem, 0, len(rows))
	for _, arow := range rows {
		created := ""
		var createdAt time.Time
		if arow.CreatedAt.Valid {
			created = arow.CreatedAt.Time.Format("2006-01-02 15:04")
			createdAt = arow.CreatedAt.Time
		}
		autorNom, autorURL, autorID := autorFromID(arow.CreatedBy)
		items = append(items, moderacioItem{
			ID:        arow.ID,
			Type:      "arxiu",
			Nom:       arow.Nom,
			Context:   arow.Tipus,
			Autor:     autorNom,
			AutorURL:  autorURL,
			AutorID:   autorID,
			Created:   created,
			CreatedAt: createdAt,
			Motiu:     arow.ModeracioMotiu,
			EditURL:   fmt.Sprintf("/documentals/arxius/%d/edit?return_to=/moderacio", arow.ID),
			Status:    arow.ModeracioEstat,
		})
	}
	if metrics != nil {
		metrics.listBuildDur += time.Since(buildStart)
	}
	return items, nil
}

func (a *App) listModeracioLlibres(filter db.LlibreFilter, offset, limit int, autorFromID func(sql.NullInt64) (string, string, int), metrics *moderacioBuildMetrics) ([]moderacioItem, error) {
	if limit <= 0 {
		return []moderacioItem{}, nil
	}
	filter.Limit = limit
	filter.Offset = offset
	fetchStart := time.Now()
	rows, err := a.DB.ListLlibres(filter)
	if metrics != nil {
		metrics.listFetchDur += time.Since(fetchStart)
	}
	if err != nil {
		return nil, err
	}
	buildStart := time.Now()
	items := make([]moderacioItem, 0, len(rows))
	for _, l := range rows {
		created := ""
		var createdAt time.Time
		if l.CreatedAt.Valid {
			created = l.CreatedAt.Time.Format("2006-01-02 15:04")
			createdAt = l.CreatedAt.Time
		}
		autorNom, autorURL, autorID := autorFromID(l.CreatedBy)
		items = append(items, moderacioItem{
			ID:        l.ID,
			Type:      "llibre",
			Nom:       l.Titol,
			Context:   l.NomEsglesia,
			Autor:     autorNom,
			AutorURL:  autorURL,
			AutorID:   autorID,
			Created:   created,
			CreatedAt: createdAt,
			Motiu:     l.ModeracioMotiu,
			EditURL:   fmt.Sprintf("/documentals/llibres/%d/edit?return_to=/moderacio", l.ID),
			Status:    l.ModeracioEstat,
		})
	}
	if metrics != nil {
		metrics.listBuildDur += time.Since(buildStart)
	}
	return items, nil
}

func (a *App) listModeracioNivells(filter db.NivellAdminFilter, offset, limit int, autorFromID func(sql.NullInt64) (string, string, int), metrics *moderacioBuildMetrics) ([]moderacioItem, error) {
	if limit <= 0 {
		return []moderacioItem{}, nil
	}
	filter.Limit = limit
	filter.Offset = offset
	fetchStart := time.Now()
	rows, err := a.DB.ListNivells(filter)
	if metrics != nil {
		metrics.listFetchDur += time.Since(fetchStart)
	}
	if err != nil {
		return nil, err
	}
	buildStart := time.Now()
	items := make([]moderacioItem, 0, len(rows))
	for _, n := range rows {
		created := ""
		var createdAt time.Time
		if n.CreatedAt.Valid {
			created = n.CreatedAt.Time.Format("2006-01-02 15:04")
			createdAt = n.CreatedAt.Time
		}
		autorNom, autorURL, autorID := autorFromID(n.CreatedBy)
		items = append(items, moderacioItem{
			ID:        n.ID,
			Type:      "nivell",
			Nom:       n.NomNivell,
			Context:   fmt.Sprintf("Nivell %d", n.Nivel),
			Autor:     autorNom,
			AutorURL:  autorURL,
			AutorID:   autorID,
			Created:   created,
			CreatedAt: createdAt,
			Motiu:     n.ModeracioMotiu,
			EditURL:   fmt.Sprintf("/territori/nivells/%d/edit?return_to=/moderacio", n.ID),
			Status:    n.ModeracioEstat,
		})
	}
	if metrics != nil {
		metrics.listBuildDur += time.Since(buildStart)
	}
	return items, nil
}

func (a *App) listModeracioMunicipis(filter db.MunicipiFilter, offset, limit int, autorFromID func(sql.NullInt64) (string, string, int), metrics *moderacioBuildMetrics) ([]moderacioItem, error) {
	if limit <= 0 {
		return []moderacioItem{}, nil
	}
	filter.Limit = limit
	filter.Offset = offset
	fetchStart := time.Now()
	rows, err := a.DB.ListMunicipis(filter)
	if metrics != nil {
		metrics.listFetchDur += time.Since(fetchStart)
	}
	if err != nil {
		return nil, err
	}
	buildStart := time.Now()
	items := make([]moderacioItem, 0, len(rows))
	for _, mrow := range rows {
		created := ""
		var createdAt time.Time
		if mrow.CreatedAt.Valid {
			created = mrow.CreatedAt.Time.Format("2006-01-02 15:04")
			createdAt = mrow.CreatedAt.Time
		}
		autorNom, autorURL, autorID := autorFromID(mrow.CreatedBy)
		ctx := strings.TrimSpace(strings.Join([]string{mrow.PaisNom.String, mrow.ProvNom.String, mrow.Comarca.String}, " / "))
		items = append(items, moderacioItem{
			ID:        mrow.ID,
			Type:      "municipi",
			Nom:       mrow.Nom,
			Context:   ctx,
			Autor:     autorNom,
			AutorURL:  autorURL,
			AutorID:   autorID,
			Created:   created,
			CreatedAt: createdAt,
			Motiu:     "",
			EditURL:   fmt.Sprintf("/territori/municipis/%d/edit?return_to=/moderacio", mrow.ID),
			Status:    mrow.ModeracioEstat,
		})
	}
	if metrics != nil {
		metrics.listBuildDur += time.Since(buildStart)
	}
	return items, nil
}

func (a *App) listModeracioEclesiastics(filter db.ArquebisbatFilter, offset, limit int, autorFromID func(sql.NullInt64) (string, string, int), metrics *moderacioBuildMetrics) ([]moderacioItem, error) {
	if limit <= 0 {
		return []moderacioItem{}, nil
	}
	filter.Limit = limit
	filter.Offset = offset
	fetchStart := time.Now()
	rows, err := a.DB.ListArquebisbats(filter)
	if metrics != nil {
		metrics.listFetchDur += time.Since(fetchStart)
	}
	if err != nil {
		return nil, err
	}
	buildStart := time.Now()
	items := make([]moderacioItem, 0, len(rows))
	for _, row := range rows {
		created := ""
		var createdAt time.Time
		if row.CreatedAt.Valid {
			created = row.CreatedAt.Time.Format("2006-01-02 15:04")
			createdAt = row.CreatedAt.Time
		}
		autorNom, autorURL, autorID := autorFromID(row.CreatedBy)
		items = append(items, moderacioItem{
			ID:        row.ID,
			Type:      "eclesiastic",
			Nom:       row.Nom,
			Context:   row.TipusEntitat,
			Autor:     autorNom,
			AutorURL:  autorURL,
			AutorID:   autorID,
			Created:   created,
			CreatedAt: createdAt,
			Motiu:     "",
			EditURL:   fmt.Sprintf("/territori/eclesiastic/%d/edit?return_to=/moderacio", row.ID),
			Status:    row.ModeracioEstat,
		})
	}
	if metrics != nil {
		metrics.listBuildDur += time.Since(buildStart)
	}
	return items, nil
}

func (a *App) listModeracioMediaAlbums(filter db.MediaModeracioFilter, offset, limit int, autorFromID func(sql.NullInt64) (string, string, int), metrics *moderacioBuildMetrics) ([]moderacioItem, error) {
	if limit <= 0 {
		return []moderacioItem{}, nil
	}
	filter.Limit = limit
	filter.Offset = offset
	fetchStart := time.Now()
	rows, err := a.DB.ListMediaAlbumsModeracio(filter)
	if metrics != nil {
		metrics.listFetchDur += time.Since(fetchStart)
	}
	if err != nil {
		return nil, err
	}
	buildStart := time.Now()
	items := make([]moderacioItem, 0, len(rows))
	for _, album := range rows {
		autorNom, autorURL, autorID := autorFromID(sql.NullInt64{Int64: int64(album.OwnerUserID), Valid: album.OwnerUserID > 0})
		contextParts := []string{}
		if album.AlbumType != "" {
			contextParts = append(contextParts, album.AlbumType)
		}
		if album.Visibility != "" {
			contextParts = append(contextParts, album.Visibility)
		}
		items = append(items, moderacioItem{
			ID:        album.ID,
			Type:      "media_album",
			Nom:       strings.TrimSpace(album.Title),
			Context:   strings.Join(contextParts, " · "),
			Autor:     autorNom,
			AutorURL:  autorURL,
			AutorID:   autorID,
			Created:   "",
			CreatedAt: time.Time{},
			Motiu:     strings.TrimSpace(album.ModerationNotes),
			EditURL:   fmt.Sprintf("/media/albums/%s", album.PublicID),
			Status:    moderacioStatusFromMedia(album.ModerationStatus),
		})
	}
	if metrics != nil {
		metrics.listBuildDur += time.Since(buildStart)
	}
	return items, nil
}

func (a *App) listModeracioMediaItems(filter db.MediaModeracioFilter, offset, limit int, autorFromID func(sql.NullInt64) (string, string, int), metrics *moderacioBuildMetrics) ([]moderacioItem, error) {
	if limit <= 0 {
		return []moderacioItem{}, nil
	}
	filter.Limit = limit
	filter.Offset = offset
	fetchStart := time.Now()
	rows, err := a.DB.ListMediaItemsModeracio(filter)
	if metrics != nil {
		metrics.listFetchDur += time.Since(fetchStart)
	}
	if err != nil {
		return nil, err
	}
	buildStart := time.Now()
	items := make([]moderacioItem, 0, len(rows))
	albumCache := map[int]*db.MediaAlbum{}
	for _, item := range rows {
		contextParts := []string{}
		autorNom := "-"
		autorURL := ""
		autorID := 0
		if album, ok := albumCache[item.AlbumID]; ok {
			if album != nil && album.Title != "" {
				contextParts = append(contextParts, album.Title)
				autorNom, autorURL, autorID = autorFromID(sql.NullInt64{Int64: int64(album.OwnerUserID), Valid: album.OwnerUserID > 0})
			}
		} else {
			album, err := a.DB.GetMediaAlbumByID(item.AlbumID)
			if err == nil && album != nil {
				albumCache[item.AlbumID] = album
				if album.Title != "" {
					contextParts = append(contextParts, album.Title)
					autorNom, autorURL, autorID = autorFromID(sql.NullInt64{Int64: int64(album.OwnerUserID), Valid: album.OwnerUserID > 0})
				}
			} else {
				albumCache[item.AlbumID] = nil
			}
		}
		name := strings.TrimSpace(item.Title)
		if name == "" {
			name = strings.TrimSpace(item.OriginalFilename)
		}
		items = append(items, moderacioItem{
			ID:        item.ID,
			Type:      "media_item",
			Nom:       name,
			Context:   strings.Join(contextParts, " · "),
			Autor:     autorNom,
			AutorURL:  autorURL,
			AutorID:   autorID,
			Created:   "",
			CreatedAt: time.Time{},
			Motiu:     strings.TrimSpace(item.ModerationNotes),
			EditURL:   fmt.Sprintf("/media/items/%s", item.PublicID),
			Status:    moderacioStatusFromMedia(item.ModerationStatus),
		})
	}
	if metrics != nil {
		metrics.listBuildDur += time.Since(buildStart)
	}
	return items, nil
}

func (a *App) listModeracioMunicipiMapaVersions(filter db.MunicipiMapaVersionFilter, offset, limit int, autorFromID func(sql.NullInt64) (string, string, int), scopeModel *moderacioScopeModel, canModerateAll bool, metrics *moderacioBuildMetrics) ([]moderacioItem, error) {
	if limit <= 0 {
		return []moderacioItem{}, nil
	}
	batchSize := maxInt(limit, 50)
	collected := 0
	skipped := 0
	cursor := 0
	items := []moderacioItem{}
	mapCache := map[int]*db.MunicipiMapa{}
	munCache := map[int]*db.Municipi{}
	for collected < limit {
		filterBatch := filter
		filterBatch.Limit = batchSize
		filterBatch.Offset = cursor
		fetchStart := time.Now()
		rows, err := a.DB.ListMunicipiMapaVersions(filterBatch)
		if metrics != nil {
			metrics.listFetchDur += time.Since(fetchStart)
		}
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			break
		}
		buildStart := time.Now()
		for _, version := range rows {
			if !canModerateAll && (scopeModel == nil || !scopeModel.canModerateItem("municipi_mapa_version", version.ID)) {
				continue
			}
			if skipped < offset {
				skipped++
				continue
			}
			mapa, ok := mapCache[version.MapaID]
			if !ok {
				row, err := a.DB.GetMunicipiMapa(version.MapaID)
				if err != nil || row == nil {
					mapCache[version.MapaID] = nil
					mapa = nil
				} else {
					mapCache[version.MapaID] = row
					mapa = row
				}
			}
			if mapa == nil {
				continue
			}
			mun, ok := munCache[mapa.MunicipiID]
			if !ok {
				row, err := a.DB.GetMunicipi(mapa.MunicipiID)
				if err != nil || row == nil {
					munCache[mapa.MunicipiID] = nil
					mun = nil
				} else {
					munCache[mapa.MunicipiID] = row
					mun = row
				}
			}
			if mun == nil {
				continue
			}
			created := ""
			var createdAt time.Time
			if version.CreatedAt.Valid {
				created = version.CreatedAt.Time.Format("2006-01-02 15:04")
				createdAt = version.CreatedAt.Time
			}
			autorNom, autorURL, autorID := autorFromID(version.CreatedBy)
			name := fmt.Sprintf("%s · v%d", mun.Nom, version.Version)
			items = append(items, moderacioItem{
				ID:         version.ID,
				Type:       "municipi_mapa_version",
				Nom:        name,
				Context:    strings.TrimSpace(mun.Nom),
				ContextURL: fmt.Sprintf("/territori/municipis/%d", mun.ID),
				Autor:      autorNom,
				AutorURL:   autorURL,
				AutorID:    autorID,
				Created:    created,
				CreatedAt:  createdAt,
				Motiu:      strings.TrimSpace(version.ModerationNotes),
				EditURL:    fmt.Sprintf("/territori/municipis/%d/mapes/%d?version=%d", mun.ID, mapa.ID, version.ID),
				Status:     version.Status,
			})
			collected++
			if collected >= limit {
				break
			}
		}
		if metrics != nil {
			metrics.listBuildDur += time.Since(buildStart)
		}
		if len(rows) < batchSize {
			break
		}
		cursor += batchSize
	}
	return items, nil
}

func (a *App) listModeracioExternalLinks(lang string, filter db.ExternalLinkAdminFilter, offset, limit int, autorFromID func(sql.NullInt64) (string, string, int), metrics *moderacioBuildMetrics) ([]moderacioItem, error) {
	if limit <= 0 {
		return []moderacioItem{}, nil
	}
	filter.Limit = limit
	filter.Offset = offset
	fetchStart := time.Now()
	rows, err := a.DB.ListExternalLinksAdmin(filter)
	if metrics != nil {
		metrics.listFetchDur += time.Since(fetchStart)
	}
	if err != nil {
		return nil, err
	}
	buildStart := time.Now()
	items := make([]moderacioItem, 0, len(rows))
	for _, link := range rows {
		created := ""
		var createdAt time.Time
		if link.CreatedAt.Valid {
			created = link.CreatedAt.Time.Format("2006-01-02 15:04")
			createdAt = link.CreatedAt.Time
		}
		autorNom, autorURL, autorID := autorFromID(link.CreatedByUserID)
		personaName := externalLinkPersonaName(link)
		context := externalLinkSiteLabel(lang, link)
		items = append(items, moderacioItem{
			ID:        link.ID,
			Type:      "external_link",
			Nom:       strings.TrimSpace(personaName),
			Context:   strings.TrimSpace(context),
			Autor:     autorNom,
			AutorURL:  autorURL,
			AutorID:   autorID,
			Created:   created,
			CreatedAt: createdAt,
			Motiu:     strings.TrimSpace(link.Meta.String),
			EditURL:   fmt.Sprintf("/persones/%d", link.PersonaID),
			Status:    moderacioStatusFromExternalLink(link.Status),
		})
	}
	if metrics != nil {
		metrics.listBuildDur += time.Since(buildStart)
	}
	return items, nil
}

func (a *App) countModeracioHistoriaGeneral(userIDs []int, createdAfter, createdBefore time.Time, scopeModel *moderacioScopeModel, canModerateAll bool) (int, error) {
	if shouldUseScopedCount(userIDs, createdAfter, createdBefore) {
		scope := listScopeFilter{}
		if scopeModel != nil {
			scope, _ = scopeModel.scopeFilterForType("municipi_historia_general")
		}
		filter := db.MunicipiScopeFilter{
			AllowedMunicipiIDs:  scope.municipiIDs,
			AllowedProvinciaIDs: scope.provinciaIDs,
			AllowedComarcaIDs:   scope.comarcaIDs,
			AllowedNivellIDs:    scope.nivellIDs,
			AllowedPaisIDs:      scope.paisIDs,
		}
		return a.DB.CountPendingMunicipiHistoriaGeneralVersionsScoped(filter)
	}
	total := 0
	batchSize := 200
	cursor := 0
	for {
		rows, _, err := a.DB.ListPendingMunicipiHistoriaGeneralVersions(batchSize, cursor)
		if err != nil {
			return 0, err
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			if !canModerateAll && (scopeModel == nil || !scopeModel.canModerateItem("municipi_historia_general", row.ID)) {
				continue
			}
			if len(userIDs) > 0 && (!row.CreatedBy.Valid || !intSliceContains(userIDs, int(row.CreatedBy.Int64))) {
				continue
			}
			if !createdAfter.IsZero() && (!row.CreatedAt.Valid || row.CreatedAt.Time.Before(createdAfter)) {
				continue
			}
			if !createdBefore.IsZero() && (!row.CreatedAt.Valid || !row.CreatedAt.Time.Before(createdBefore)) {
				continue
			}
			total++
		}
		if len(rows) < batchSize {
			break
		}
		cursor += batchSize
	}
	return total, nil
}

func (a *App) countModeracioHistoriaFets(userIDs []int, createdAfter, createdBefore time.Time, scopeModel *moderacioScopeModel, canModerateAll bool) (int, error) {
	if shouldUseScopedCount(userIDs, createdAfter, createdBefore) {
		scope := listScopeFilter{}
		if scopeModel != nil {
			scope, _ = scopeModel.scopeFilterForType("municipi_historia_fet")
		}
		filter := db.MunicipiScopeFilter{
			AllowedMunicipiIDs:  scope.municipiIDs,
			AllowedProvinciaIDs: scope.provinciaIDs,
			AllowedComarcaIDs:   scope.comarcaIDs,
			AllowedNivellIDs:    scope.nivellIDs,
			AllowedPaisIDs:      scope.paisIDs,
		}
		return a.DB.CountPendingMunicipiHistoriaFetVersionsScoped(filter)
	}
	total := 0
	batchSize := 200
	cursor := 0
	for {
		rows, _, err := a.DB.ListPendingMunicipiHistoriaFetVersions(batchSize, cursor)
		if err != nil {
			return 0, err
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			if !canModerateAll && (scopeModel == nil || !scopeModel.canModerateItem("municipi_historia_fet", row.ID)) {
				continue
			}
			if len(userIDs) > 0 && (!row.CreatedBy.Valid || !intSliceContains(userIDs, int(row.CreatedBy.Int64))) {
				continue
			}
			if !createdAfter.IsZero() && (!row.CreatedAt.Valid || row.CreatedAt.Time.Before(createdAfter)) {
				continue
			}
			if !createdBefore.IsZero() && (!row.CreatedAt.Valid || !row.CreatedAt.Time.Before(createdBefore)) {
				continue
			}
			total++
		}
		if len(rows) < batchSize {
			break
		}
		cursor += batchSize
	}
	return total, nil
}

func (a *App) countModeracioAnecdotes(userIDs []int, createdAfter, createdBefore time.Time, scopeModel *moderacioScopeModel, canModerateAll bool) (int, error) {
	if shouldUseScopedCount(userIDs, createdAfter, createdBefore) {
		scope := listScopeFilter{}
		if scopeModel != nil {
			scope, _ = scopeModel.scopeFilterForType("municipi_anecdota_version")
		}
		filter := db.MunicipiScopeFilter{
			AllowedMunicipiIDs:  scope.municipiIDs,
			AllowedProvinciaIDs: scope.provinciaIDs,
			AllowedComarcaIDs:   scope.comarcaIDs,
			AllowedNivellIDs:    scope.nivellIDs,
			AllowedPaisIDs:      scope.paisIDs,
		}
		return a.DB.CountPendingMunicipiAnecdotariVersionsScoped(filter)
	}
	total := 0
	batchSize := 200
	cursor := 0
	for {
		rows, _, err := a.DB.ListPendingMunicipiAnecdotariVersions(batchSize, cursor)
		if err != nil {
			return 0, err
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			if !canModerateAll && (scopeModel == nil || !scopeModel.canModerateItem("municipi_anecdota_version", row.ID)) {
				continue
			}
			if len(userIDs) > 0 && (!row.CreatedBy.Valid || !intSliceContains(userIDs, int(row.CreatedBy.Int64))) {
				continue
			}
			if !createdAfter.IsZero() && (!row.CreatedAt.Valid || row.CreatedAt.Time.Before(createdAfter)) {
				continue
			}
			if !createdBefore.IsZero() && (!row.CreatedAt.Valid || !row.CreatedAt.Time.Before(createdBefore)) {
				continue
			}
			total++
		}
		if len(rows) < batchSize {
			break
		}
		cursor += batchSize
	}
	return total, nil
}

func (a *App) listModeracioHistoriaGeneral(lang string, offset, limit int, userIDs []int, createdAfter, createdBefore time.Time, autorFromID func(sql.NullInt64) (string, string, int), scopeModel *moderacioScopeModel, canModerateAll bool, metrics *moderacioBuildMetrics) ([]moderacioItem, error) {
	if limit <= 0 {
		return []moderacioItem{}, nil
	}
	batchSize := maxInt(limit, 50)
	items := []moderacioItem{}
	skipped := 0
	cursor := 0
	for len(items) < limit {
		fetchStart := time.Now()
		rows, _, err := a.DB.ListPendingMunicipiHistoriaGeneralVersions(batchSize, cursor)
		if metrics != nil {
			metrics.listFetchDur += time.Since(fetchStart)
		}
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			break
		}
		buildStart := time.Now()
		for _, row := range rows {
			if !canModerateAll && (scopeModel == nil || !scopeModel.canModerateItem("municipi_historia_general", row.ID)) {
				continue
			}
			if len(userIDs) > 0 && (!row.CreatedBy.Valid || !intSliceContains(userIDs, int(row.CreatedBy.Int64))) {
				continue
			}
			if !createdAfter.IsZero() && (!row.CreatedAt.Valid || row.CreatedAt.Time.Before(createdAfter)) {
				continue
			}
			if !createdBefore.IsZero() && (!row.CreatedAt.Valid || !row.CreatedAt.Time.Before(createdBefore)) {
				continue
			}
			if skipped < offset {
				skipped++
				continue
			}
			created := ""
			var createdAt time.Time
			if row.CreatedAt.Valid {
				created = row.CreatedAt.Time.Format("2006-01-02 15:04")
				createdAt = row.CreatedAt.Time
			}
			autorNom, autorURL, autorID := autorFromID(row.CreatedBy)
			nomParts := []string{T(lang, "municipi.history.general")}
			if strings.TrimSpace(row.MunicipiNom) != "" {
				nomParts = append(nomParts, strings.TrimSpace(row.MunicipiNom))
			}
			items = append(items, moderacioItem{
				ID:         row.ID,
				Type:       "municipi_historia_general",
				Nom:        strings.Join(nomParts, " · "),
				Context:    strings.TrimSpace(row.MunicipiNom),
				ContextURL: fmt.Sprintf("/territori/municipis/%d", row.MunicipiID),
				Autor:      autorNom,
				AutorURL:   autorURL,
				AutorID:    autorID,
				Created:    created,
				CreatedAt:  createdAt,
				Motiu:      row.ModerationNotes,
				EditURL:    fmt.Sprintf("/moderacio/municipis/historia/general/%d", row.ID),
				Status:     row.Status,
			})
			if len(items) >= limit {
				break
			}
		}
		if metrics != nil {
			metrics.listBuildDur += time.Since(buildStart)
		}
		if len(rows) < batchSize {
			break
		}
		cursor += batchSize
	}
	return items, nil
}

func (a *App) listModeracioHistoriaFets(lang string, offset, limit int, userIDs []int, createdAfter, createdBefore time.Time, autorFromID func(sql.NullInt64) (string, string, int), scopeModel *moderacioScopeModel, canModerateAll bool, metrics *moderacioBuildMetrics) ([]moderacioItem, error) {
	if limit <= 0 {
		return []moderacioItem{}, nil
	}
	batchSize := maxInt(limit, 50)
	items := []moderacioItem{}
	skipped := 0
	cursor := 0
	for len(items) < limit {
		fetchStart := time.Now()
		rows, _, err := a.DB.ListPendingMunicipiHistoriaFetVersions(batchSize, cursor)
		if metrics != nil {
			metrics.listFetchDur += time.Since(fetchStart)
		}
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			break
		}
		buildStart := time.Now()
		for _, row := range rows {
			if !canModerateAll && (scopeModel == nil || !scopeModel.canModerateItem("municipi_historia_fet", row.ID)) {
				continue
			}
			if len(userIDs) > 0 && (!row.CreatedBy.Valid || !intSliceContains(userIDs, int(row.CreatedBy.Int64))) {
				continue
			}
			if !createdAfter.IsZero() && (!row.CreatedAt.Valid || row.CreatedAt.Time.Before(createdAfter)) {
				continue
			}
			if !createdBefore.IsZero() && (!row.CreatedAt.Valid || !row.CreatedAt.Time.Before(createdBefore)) {
				continue
			}
			if skipped < offset {
				skipped++
				continue
			}
			created := ""
			var createdAt time.Time
			if row.CreatedAt.Valid {
				created = row.CreatedAt.Time.Format("2006-01-02 15:04")
				createdAt = row.CreatedAt.Time
			}
			autorNom, autorURL, autorID := autorFromID(row.CreatedBy)
			dateLabel := strings.TrimSpace(historiaDateLabel(row))
			nameParts := []string{}
			if dateLabel != "" {
				nameParts = append(nameParts, dateLabel)
			}
			if strings.TrimSpace(row.Titol) != "" {
				nameParts = append(nameParts, strings.TrimSpace(row.Titol))
			}
			if strings.TrimSpace(row.MunicipiNom) != "" {
				nameParts = append(nameParts, strings.TrimSpace(row.MunicipiNom))
			}
			items = append(items, moderacioItem{
				ID:         row.ID,
				Type:       "municipi_historia_fet",
				Nom:        strings.Join(nameParts, " · "),
				Context:    strings.TrimSpace(row.MunicipiNom),
				ContextURL: fmt.Sprintf("/territori/municipis/%d", row.MunicipiID),
				Autor:      autorNom,
				AutorURL:   autorURL,
				AutorID:    autorID,
				Created:    created,
				CreatedAt:  createdAt,
				Motiu:      row.ModerationNotes,
				EditURL:    fmt.Sprintf("/moderacio/municipis/historia/fets/%d", row.ID),
				Status:     row.Status,
			})
			if len(items) >= limit {
				break
			}
		}
		if metrics != nil {
			metrics.listBuildDur += time.Since(buildStart)
		}
		if len(rows) < batchSize {
			break
		}
		cursor += batchSize
	}
	return items, nil
}

func (a *App) listModeracioAnecdotes(lang string, offset, limit int, userIDs []int, createdAfter, createdBefore time.Time, autorFromID func(sql.NullInt64) (string, string, int), scopeModel *moderacioScopeModel, canModerateAll bool, metrics *moderacioBuildMetrics) ([]moderacioItem, error) {
	if limit <= 0 {
		return []moderacioItem{}, nil
	}
	batchSize := maxInt(limit, 50)
	items := []moderacioItem{}
	skipped := 0
	cursor := 0
	for len(items) < limit {
		fetchStart := time.Now()
		rows, _, err := a.DB.ListPendingMunicipiAnecdotariVersions(batchSize, cursor)
		if metrics != nil {
			metrics.listFetchDur += time.Since(fetchStart)
		}
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			break
		}
		buildStart := time.Now()
		for _, row := range rows {
			if !canModerateAll && (scopeModel == nil || !scopeModel.canModerateItem("municipi_anecdota_version", row.ID)) {
				continue
			}
			if len(userIDs) > 0 && (!row.CreatedBy.Valid || !intSliceContains(userIDs, int(row.CreatedBy.Int64))) {
				continue
			}
			if !createdAfter.IsZero() && (!row.CreatedAt.Valid || row.CreatedAt.Time.Before(createdAfter)) {
				continue
			}
			if !createdBefore.IsZero() && (!row.CreatedAt.Valid || !row.CreatedAt.Time.Before(createdBefore)) {
				continue
			}
			if skipped < offset {
				skipped++
				continue
			}
			created := ""
			var createdAt time.Time
			if row.CreatedAt.Valid {
				created = row.CreatedAt.Time.Format("2006-01-02 15:04")
				createdAt = row.CreatedAt.Time
			}
			autorNom, autorURL, autorID := autorFromID(row.CreatedBy)
			tagLabel := strings.TrimSpace(row.Tag)
			if strings.TrimSpace(row.Tag) != "" {
				labelKey := "municipi.anecdotes.tags." + strings.TrimSpace(row.Tag)
				label := strings.TrimSpace(T(lang, labelKey))
				if label != "" && label != labelKey {
					tagLabel = label
				}
			}
			contextParts := []string{}
			if strings.TrimSpace(row.MunicipiNom) != "" {
				contextParts = append(contextParts, strings.TrimSpace(row.MunicipiNom))
			}
			if strings.TrimSpace(tagLabel) != "" {
				contextParts = append(contextParts, strings.TrimSpace(tagLabel))
			}
			name := strings.TrimSpace(row.Titol)
			if name == "" {
				name = T(lang, "municipi.anecdotes.title")
			}
			items = append(items, moderacioItem{
				ID:         row.ID,
				Type:       "municipi_anecdota_version",
				Nom:        name,
				Context:    strings.Join(contextParts, " · "),
				ContextURL: fmt.Sprintf("/territori/municipis/%d", row.MunicipiID),
				Autor:      autorNom,
				AutorURL:   autorURL,
				AutorID:    autorID,
				Created:    created,
				CreatedAt:  createdAt,
				Motiu:      row.ModerationNotes,
				EditURL:    fmt.Sprintf("/territori/municipis/%d/anecdotes/%d?version_id=%d", row.MunicipiID, row.ItemID, row.ID),
				Status:     row.Status,
			})
			if len(items) >= limit {
				break
			}
		}
		if metrics != nil {
			metrics.listBuildDur += time.Since(buildStart)
		}
		if len(rows) < batchSize {
			break
		}
		cursor += batchSize
	}
	return items, nil
}

func (a *App) listModeracioCognomVariants(filter db.CognomVariantFilter, offset, limit int, autorFromID func(sql.NullInt64) (string, string, int), metrics *moderacioBuildMetrics) ([]moderacioItem, error) {
	if limit <= 0 {
		return []moderacioItem{}, nil
	}
	filter.Limit = limit
	filter.Offset = offset
	fetchStart := time.Now()
	rows, err := a.DB.ListCognomVariants(filter)
	if metrics != nil {
		metrics.listFetchDur += time.Since(fetchStart)
	}
	if err != nil {
		return nil, err
	}
	buildStart := time.Now()
	cognomCache := map[int]string{}
	items := make([]moderacioItem, 0, len(rows))
	for _, v := range rows {
		created := ""
		var createdAt time.Time
		if v.CreatedAt.Valid {
			created = v.CreatedAt.Time.Format("2006-01-02 15:04")
			createdAt = v.CreatedAt.Time
		}
		autorNom, autorURL, autorID := autorFromID(v.CreatedBy)
		forma := cognomCache[v.CognomID]
		if forma == "" {
			if c, err := a.DB.GetCognom(v.CognomID); err == nil && c != nil {
				forma = c.Forma
				cognomCache[v.CognomID] = forma
			}
		}
		context := strings.TrimSpace(fmt.Sprintf("%s → %s", forma, v.Variant))
		if context == "" {
			context = v.Variant
		}
		items = append(items, moderacioItem{
			ID:        v.ID,
			Type:      "cognom_variant",
			Nom:       v.Variant,
			Context:   context,
			Autor:     autorNom,
			AutorURL:  autorURL,
			AutorID:   autorID,
			Created:   created,
			CreatedAt: createdAt,
			Motiu:     v.ModeracioMotiu,
			EditURL:   fmt.Sprintf("/cognoms/%d", v.CognomID),
			Status:    v.ModeracioEstat,
		})
	}
	if metrics != nil {
		metrics.listBuildDur += time.Since(buildStart)
	}
	return items, nil
}

func (a *App) listModeracioCognomReferencies(filter db.CognomReferenciaFilter, offset, limit int, autorFromID func(sql.NullInt64) (string, string, int), metrics *moderacioBuildMetrics) ([]moderacioItem, error) {
	if limit <= 0 {
		return []moderacioItem{}, nil
	}
	filter.Limit = limit
	filter.Offset = offset
	fetchStart := time.Now()
	rows, err := a.DB.ListCognomReferencies(filter)
	if metrics != nil {
		metrics.listFetchDur += time.Since(fetchStart)
	}
	if err != nil {
		return nil, err
	}
	buildStart := time.Now()
	cognomCache := map[int]string{}
	items := make([]moderacioItem, 0, len(rows))
	for _, ref := range rows {
		created := ""
		var createdAt time.Time
		if ref.CreatedAt.Valid {
			created = ref.CreatedAt.Time.Format("2006-01-02 15:04")
			createdAt = ref.CreatedAt.Time
		}
		autorNom, autorURL, autorID := autorFromID(ref.CreatedBy)
		forma := cognomCache[ref.CognomID]
		if forma == "" {
			if c, err := a.DB.GetCognom(ref.CognomID); err == nil && c != nil {
				forma = c.Forma
				cognomCache[ref.CognomID] = forma
			}
		}
		context := strings.TrimSpace(forma)
		if context == "" {
			context = fmt.Sprintf("Cognom %d", ref.CognomID)
		}
		name := strings.TrimSpace(ref.Titol)
		if name == "" {
			name = strings.TrimSpace(ref.URL)
		}
		if name == "" {
			name = strings.TrimSpace(ref.Kind)
		}
		items = append(items, moderacioItem{
			ID:         ref.ID,
			Type:       "cognom_referencia",
			Nom:        name,
			Context:    context,
			ContextURL: fmt.Sprintf("/cognoms/%d", ref.CognomID),
			Autor:      autorNom,
			AutorURL:   autorURL,
			AutorID:    autorID,
			Created:    created,
			CreatedAt:  createdAt,
			Motiu:      ref.ModeracioMotiu,
			EditURL:    fmt.Sprintf("/cognoms/%d", ref.CognomID),
			Status:     ref.ModeracioEstat,
		})
	}
	if metrics != nil {
		metrics.listBuildDur += time.Since(buildStart)
	}
	return items, nil
}

func (a *App) listModeracioCognomMerges(filter db.CognomRedirectSuggestionFilter, offset, limit int, autorFromID func(sql.NullInt64) (string, string, int), metrics *moderacioBuildMetrics) ([]moderacioItem, error) {
	if limit <= 0 {
		return []moderacioItem{}, nil
	}
	filter.Limit = limit
	filter.Offset = offset
	fetchStart := time.Now()
	rows, err := a.DB.ListCognomRedirectSuggestions(filter)
	if metrics != nil {
		metrics.listFetchDur += time.Since(fetchStart)
	}
	if err != nil {
		return nil, err
	}
	buildStart := time.Now()
	cognomCache := map[int]string{}
	items := make([]moderacioItem, 0, len(rows))
	for _, merge := range rows {
		created := ""
		var createdAt time.Time
		if merge.CreatedAt.Valid {
			created = merge.CreatedAt.Time.Format("2006-01-02 15:04")
			createdAt = merge.CreatedAt.Time
		}
		autorNom, autorURL, autorID := autorFromID(merge.CreatedBy)
		fromLabel := cognomCache[merge.FromCognomID]
		if fromLabel == "" {
			if c, err := a.DB.GetCognom(merge.FromCognomID); err == nil && c != nil {
				fromLabel = c.Forma
				cognomCache[merge.FromCognomID] = fromLabel
			}
		}
		toLabel := cognomCache[merge.ToCognomID]
		if toLabel == "" {
			if c, err := a.DB.GetCognom(merge.ToCognomID); err == nil && c != nil {
				toLabel = c.Forma
				cognomCache[merge.ToCognomID] = toLabel
			}
		}
		context := strings.TrimSpace(fmt.Sprintf("%s → %s", fromLabel, toLabel))
		if context == "" {
			context = fmt.Sprintf("Cognom %d → %d", merge.FromCognomID, merge.ToCognomID)
		}
		items = append(items, moderacioItem{
			ID:         merge.ID,
			Type:       "cognom_merge",
			Nom:        context,
			Context:    context,
			ContextURL: fmt.Sprintf("/cognoms/%d", merge.ToCognomID),
			Autor:      autorNom,
			AutorURL:   autorURL,
			AutorID:    autorID,
			Created:    created,
			CreatedAt:  createdAt,
			Motiu:      merge.Reason,
			EditURL:    fmt.Sprintf("/admin/cognoms/merge"),
			Status:     merge.ModeracioEstat,
		})
	}
	if metrics != nil {
		metrics.listBuildDur += time.Since(buildStart)
	}
	return items, nil
}

func (a *App) listModeracioEvents(lang string, filter db.EventHistoricFilter, offset, limit int, autorFromID func(sql.NullInt64) (string, string, int), metrics *moderacioBuildMetrics) ([]moderacioItem, error) {
	if limit <= 0 {
		return []moderacioItem{}, nil
	}
	filter.Limit = limit
	filter.Offset = offset
	fetchStart := time.Now()
	rows, err := a.DB.ListEventsHistoric(filter)
	if metrics != nil {
		metrics.listFetchDur += time.Since(fetchStart)
	}
	if err != nil {
		return nil, err
	}
	buildStart := time.Now()
	items := make([]moderacioItem, 0, len(rows))
	for _, ev := range rows {
		created := ""
		var createdAt time.Time
		if ev.CreatedAt.Valid {
			created = ev.CreatedAt.Time.Format("2006-01-02 15:04")
			createdAt = ev.CreatedAt.Time
		}
		autorNom, autorURL, autorID := autorFromID(ev.CreatedBy)
		contextParts := []string{}
		if label := eventTypeLabel(lang, ev.Tipus); label != "" {
			contextParts = append(contextParts, label)
		}
		if dateLabel := eventDateLabel(ev); dateLabel != "" {
			contextParts = append(contextParts, dateLabel)
		}
		context := strings.Join(contextParts, " · ")
		items = append(items, moderacioItem{
			ID:        ev.ID,
			Type:      "event_historic",
			Nom:       strings.TrimSpace(ev.Titol),
			Context:   context,
			Autor:     autorNom,
			AutorURL:  autorURL,
			AutorID:   autorID,
			Created:   created,
			CreatedAt: createdAt,
			Motiu:     ev.ModerationNotes,
			EditURL:   fmt.Sprintf("/historia/events/%d", ev.ID),
			Status:    ev.ModerationStatus,
		})
	}
	if metrics != nil {
		metrics.listBuildDur += time.Since(buildStart)
	}
	return items, nil
}

func (a *App) listModeracioRegistres(filter db.TranscripcioFilter, offset, limit int, autorFromID func(sql.NullInt64) (string, string, int), metrics *moderacioBuildMetrics) ([]moderacioItem, error) {
	if limit <= 0 {
		return []moderacioItem{}, nil
	}
	filter.Limit = limit
	filter.Offset = offset
	fetchStart := time.Now()
	registres, err := a.DB.ListTranscripcionsRawGlobal(filter)
	if metrics != nil {
		metrics.listFetchDur += time.Since(fetchStart)
	}
	if err != nil {
		return nil, err
	}
	buildStart := time.Now()
	items := make([]moderacioItem, 0, len(registres))
	for _, reg := range registres {
		autorNom, autorURL, autorID := autorFromID(reg.CreatedBy)
		created := ""
		var createdAt time.Time
		if !reg.CreatedAt.IsZero() {
			created = reg.CreatedAt.Format("2006-01-02 15:04")
			createdAt = reg.CreatedAt
		}
		contextParts := []string{}
		if reg.TipusActe != "" {
			contextParts = append(contextParts, reg.TipusActe)
		}
		if reg.DataActeText != "" {
			contextParts = append(contextParts, reg.DataActeText)
		} else if reg.AnyDoc.Valid {
			contextParts = append(contextParts, fmt.Sprintf("%d", reg.AnyDoc.Int64))
		}
		if reg.NumPaginaText != "" {
			contextParts = append(contextParts, reg.NumPaginaText)
		}
		items = append(items, moderacioItem{
			ID:        reg.ID,
			Type:      "registre",
			Nom:       fmt.Sprintf("Registre %d", reg.ID),
			Context:   strings.Join(contextParts, " · "),
			Autor:     autorNom,
			AutorURL:  autorURL,
			AutorID:   autorID,
			Created:   created,
			CreatedAt: createdAt,
			Motiu:     reg.ModeracioMotiu,
			EditURL:   fmt.Sprintf("/documentals/registres/%d/editar?return_to=/moderacio", reg.ID),
			Status:    reg.ModeracioEstat,
		})
	}
	if metrics != nil {
		metrics.listBuildDur += time.Since(buildStart)
	}
	return items, nil
}

func (a *App) listModeracioRegistreCanvis(filter db.TranscripcioFilter, offset, limit int, autorFromID func(sql.NullInt64) (string, string, int), metrics *moderacioBuildMetrics) ([]moderacioItem, error) {
	if limit <= 0 {
		return []moderacioItem{}, nil
	}
	filter.Limit = limit
	filter.Offset = offset
	fetchStart := time.Now()
	rows, err := a.DB.ListTranscripcioRawChangesPendingFiltered(filter)
	if metrics != nil {
		metrics.listFetchDur += time.Since(fetchStart)
	}
	if err != nil {
		return nil, err
	}
	buildStart := time.Now()
	items := make([]moderacioItem, 0, len(rows))
	for _, change := range rows {
		autorNom, autorURL, autorID := autorFromID(change.ChangedBy)
		created := ""
		var createdAt time.Time
		if !change.ChangedAt.IsZero() {
			created = change.ChangedAt.Format("2006-01-02 15:04")
			createdAt = change.ChangedAt
		}
		contextParts := []string{}
		if change.ChangeType != "" {
			contextParts = append(contextParts, change.ChangeType)
		}
		if change.FieldKey != "" {
			contextParts = append(contextParts, change.FieldKey)
		}
		context := strings.Join(contextParts, " · ")
		if context == "" {
			context = fmt.Sprintf("Canvi %d", change.ID)
		}
		items = append(items, moderacioItem{
			ID:        change.ID,
			Type:      "registre_canvi",
			Nom:       fmt.Sprintf("Registre %d", change.TranscripcioID),
			Context:   context,
			Autor:     autorNom,
			AutorURL:  autorURL,
			AutorID:   autorID,
			Created:   created,
			CreatedAt: createdAt,
			Motiu:     change.ModeracioMotiu,
			EditURL:   fmt.Sprintf("/documentals/registres/%d/editar?return_to=/moderacio", change.TranscripcioID),
			Status:    change.ModeracioEstat,
		})
	}
	if metrics != nil {
		metrics.listBuildDur += time.Since(buildStart)
	}
	return items, nil
}

func (a *App) countModeracioWikiChanges(userIDs []int, createdAfter, createdBefore time.Time, scopeModel *moderacioScopeModel, canModerateAll bool, typeAllowed func(string) bool) (map[string]int, error) {
	counts := map[string]int{}
	if typeAllowed == nil {
		return counts, nil
	}
	needs := typeAllowed("municipi_canvi") || typeAllowed("arxiu_canvi") || typeAllowed("llibre_canvi") || typeAllowed("persona_canvi") || typeAllowed("cognom_canvi") || typeAllowed("event_historic_canvi")
	if !needs {
		return counts, nil
	}
	if shouldUseScopedCount(userIDs, createdAfter, createdBefore) {
		rawCounts := map[string]int{}
		if canModerateAll || typeAllowed("persona_canvi") || typeAllowed("cognom_canvi") || typeAllowed("event_historic_canvi") {
			var err error
			rawCounts, err = a.DB.CountWikiPendingChangesByType()
			if err != nil {
				return nil, err
			}
		}
		if canModerateAll {
			for objType, total := range rawCounts {
				modType := resolveWikiChangeModeracioType(db.WikiChange{ObjectType: objType})
				if modType == "" || !typeAllowed(modType) {
					continue
				}
				if total > 0 {
					counts[modType] = total
				}
			}
			return counts, nil
		}
		for objType, total := range rawCounts {
			modType := resolveWikiChangeModeracioType(db.WikiChange{ObjectType: objType})
			if modType == "" || !typeAllowed(modType) {
				continue
			}
			switch modType {
			case "municipi_canvi", "arxiu_canvi", "llibre_canvi":
				continue
			}
			if total > 0 {
				counts[modType] = total
			}
		}
		if typeAllowed("municipi_canvi") {
			scope := listScopeFilter{}
			if scopeModel != nil {
				scope, _ = scopeModel.scopeFilterForType("municipi_canvi")
			}
			filter := db.MunicipiScopeFilter{
				AllowedMunicipiIDs:  scope.municipiIDs,
				AllowedProvinciaIDs: scope.provinciaIDs,
				AllowedComarcaIDs:   scope.comarcaIDs,
				AllowedNivellIDs:    scope.nivellIDs,
				AllowedPaisIDs:      scope.paisIDs,
			}
			total, err := a.DB.CountWikiPendingMunicipiChangesScoped(filter)
			if err != nil {
				return nil, err
			}
			if total > 0 {
				counts["municipi_canvi"] = total
			}
		}
		if typeAllowed("arxiu_canvi") {
			filter := db.ArxiuFilter{}
			if scopeModel != nil {
				if scope, ok := scopeModel.scopeFilterForType("arxiu_canvi"); ok && !scope.hasGlobal {
					applyScopeFilterToArxiu(&filter, scope)
				}
			}
			total, err := a.DB.CountWikiPendingArxiuChangesScoped(filter)
			if err != nil {
				return nil, err
			}
			if total > 0 {
				counts["arxiu_canvi"] = total
			}
		}
		if typeAllowed("llibre_canvi") {
			filter := db.LlibreFilter{}
			if scopeModel != nil {
				if scope, ok := scopeModel.scopeFilterForType("llibre_canvi"); ok && !scope.hasGlobal {
					applyScopeFilterToLlibre(&filter, scope)
				}
			}
			total, err := a.DB.CountWikiPendingLlibreChangesScoped(filter)
			if err != nil {
				return nil, err
			}
			if total > 0 {
				counts["llibre_canvi"] = total
			}
		}
		return counts, nil
	}

	batchSize := 200
	cursor := 0
	for {
		changes, stale, err := a.DB.ListWikiPendingChanges(batchSize, cursor)
		if err != nil {
			return nil, err
		}
		for _, changeID := range stale {
			_ = a.DB.DequeueWikiPending(changeID)
		}
		if len(changes) == 0 {
			break
		}
		for _, change := range changes {
			objType := resolveWikiChangeModeracioType(change)
			if objType == "" || !typeAllowed(objType) {
				continue
			}
			if len(userIDs) > 0 && (!change.ChangedBy.Valid || !intSliceContains(userIDs, int(change.ChangedBy.Int64))) {
				continue
			}
			if !createdAfter.IsZero() && (change.ChangedAt.IsZero() || change.ChangedAt.Before(createdAfter)) {
				continue
			}
			if !createdBefore.IsZero() && (change.ChangedAt.IsZero() || !change.ChangedAt.Before(createdBefore)) {
				continue
			}
			if !canModerateAll && (scopeModel == nil || !scopeModel.canModerateWikiChange(change, objType)) {
				continue
			}
			counts[objType]++
		}
		if len(changes) < batchSize {
			break
		}
		cursor += batchSize
	}
	return counts, nil
}

func (a *App) listModeracioWikiChanges(lang string, offset, limit int, userIDs []int, createdAfter, createdBefore time.Time, autorFromID func(sql.NullInt64) (string, string, int), scopeModel *moderacioScopeModel, canModerateAll bool, typeAllowed func(string) bool, metrics *moderacioBuildMetrics) ([]moderacioItem, error) {
	if limit <= 0 {
		return []moderacioItem{}, nil
	}
	items := []moderacioItem{}
	skipped := 0
	batchSize := maxInt(limit, 50)
	cursor := 0
	municipiCache := map[int]string{}
	arxiuCache := map[int]string{}
	llibreCache := map[int]string{}
	personaCache := map[int]string{}
	cognomCache := map[int]string{}
	eventCache := map[int]string{}
	for len(items) < limit {
		fetchStart := time.Now()
		changes, stale, err := a.DB.ListWikiPendingChanges(batchSize, cursor)
		if metrics != nil {
			metrics.listFetchDur += time.Since(fetchStart)
		}
		if err != nil {
			return nil, err
		}
		for _, changeID := range stale {
			_ = a.DB.DequeueWikiPending(changeID)
		}
		if len(changes) == 0 {
			break
		}
		buildStart := time.Now()
		for _, change := range changes {
			objType := resolveWikiChangeModeracioType(change)
			if objType == "" {
				continue
			}
			if typeAllowed != nil && !typeAllowed(objType) {
				continue
			}
			if len(userIDs) > 0 && (!change.ChangedBy.Valid || !intSliceContains(userIDs, int(change.ChangedBy.Int64))) {
				continue
			}
			if !createdAfter.IsZero() && (change.ChangedAt.IsZero() || change.ChangedAt.Before(createdAfter)) {
				continue
			}
			if !createdBefore.IsZero() && (change.ChangedAt.IsZero() || !change.ChangedAt.Before(createdBefore)) {
				continue
			}
			if !canModerateAll && scopeModel != nil && !scopeModel.canModerateWikiChange(change, objType) {
				continue
			}
			if skipped < offset {
				skipped++
				continue
			}
			autorNom, autorURL, autorID := autorFromID(change.ChangedBy)
			created := ""
			var createdAt time.Time
			if !change.ChangedAt.IsZero() {
				created = change.ChangedAt.Format("2006-01-02 15:04")
				createdAt = change.ChangedAt
			}
			contextParts := []string{}
			if change.ChangeType != "" {
				contextParts = append(contextParts, change.ChangeType)
			}
			if change.FieldKey != "" {
				contextParts = append(contextParts, change.FieldKey)
			}
			context := strings.Join(contextParts, " · ")
			if context == "" {
				context = fmt.Sprintf("Canvi %d", change.ID)
			}
			name := fmt.Sprintf("%s %d", change.ObjectType, change.ObjectID)
			editURL := ""
			contextURL := ""
			switch change.ObjectType {
			case "municipi":
				if cached, ok := municipiCache[change.ObjectID]; ok {
					name = cached
				} else if mun, err := a.DB.GetMunicipi(change.ObjectID); err == nil && mun != nil {
					name = mun.Nom
					municipiCache[change.ObjectID] = name
				}
				contextURL = fmt.Sprintf("/territori/municipis/%d", change.ObjectID)
				editURL = fmt.Sprintf("/territori/municipis/%d/historial?view=%d", change.ObjectID, change.ID)
			case "arxiu":
				if cached, ok := arxiuCache[change.ObjectID]; ok {
					name = cached
				} else if arxiu, err := a.DB.GetArxiu(change.ObjectID); err == nil && arxiu != nil {
					name = arxiu.Nom
					arxiuCache[change.ObjectID] = name
				}
				contextURL = fmt.Sprintf("/documentals/arxius/%d", change.ObjectID)
				editURL = fmt.Sprintf("/documentals/arxius/%d/historial?view=%d", change.ObjectID, change.ID)
			case "llibre":
				if cached, ok := llibreCache[change.ObjectID]; ok {
					name = cached
				} else if llibre, err := a.DB.GetLlibre(change.ObjectID); err == nil && llibre != nil {
					name = llibre.Titol
					llibreCache[change.ObjectID] = name
				}
				contextURL = fmt.Sprintf("/documentals/llibres/%d", change.ObjectID)
				editURL = fmt.Sprintf("/documentals/llibres/%d/historial?view=%d", change.ObjectID, change.ID)
			case "persona":
				if cached, ok := personaCache[change.ObjectID]; ok {
					name = cached
				} else if persona, err := a.DB.GetPersona(change.ObjectID); err == nil && persona != nil {
					name = strings.TrimSpace(strings.Join([]string{persona.Nom, persona.Cognom1, persona.Cognom2}, " "))
					personaCache[change.ObjectID] = name
				}
				contextURL = fmt.Sprintf("/persones/%d", change.ObjectID)
				editURL = fmt.Sprintf("/persones/%d/historial?view=%d", change.ObjectID, change.ID)
			case "cognom":
				if cached, ok := cognomCache[change.ObjectID]; ok {
					name = cached
				} else if cognom, err := a.DB.GetCognom(change.ObjectID); err == nil && cognom != nil {
					name = cognom.Forma
					cognomCache[change.ObjectID] = name
				}
				contextURL = fmt.Sprintf("/cognoms/%d", change.ObjectID)
				editURL = fmt.Sprintf("/cognoms/%d/historial?view=%d", change.ObjectID, change.ID)
			case "event_historic":
				if cached, ok := eventCache[change.ObjectID]; ok {
					name = cached
				} else if ev, err := a.DB.GetEventHistoric(change.ObjectID); err == nil && ev != nil {
					name = strings.TrimSpace(ev.Titol)
					eventCache[change.ObjectID] = name
				}
				contextURL = fmt.Sprintf("/historia/events/%d", change.ObjectID)
				editURL = fmt.Sprintf("/historia/events/%d/historial?view=%d", change.ObjectID, change.ID)
			}
			items = append(items, moderacioItem{
				ID:         change.ID,
				Type:       objType,
				Nom:        name,
				Context:    context,
				ContextURL: contextURL,
				Autor:      autorNom,
				AutorURL:   autorURL,
				AutorID:    autorID,
				Created:    created,
				CreatedAt:  createdAt,
				Motiu:      change.ModeracioMotiu,
				EditURL:    editURL,
				Status:     change.ModeracioEstat,
			})
			if len(items) >= limit {
				break
			}
		}
		if metrics != nil {
			metrics.listBuildDur += time.Since(buildStart)
		}
		if len(changes) < batchSize {
			break
		}
		cursor += batchSize
	}
	return items, nil
}

func (a *App) countModeracioByAgeBucket(filters moderacioFilters, scopeModel *moderacioScopeModel, canModerateAll bool, bucket string, userIDs []int, now time.Time) int {
	statusFilter := strings.TrimSpace(filters.Status)
	statusAll := statusFilter == "" || statusFilter == "all"
	typeFilter := strings.TrimSpace(filters.Type)
	typeAllowed := func(objType string) bool {
		if typeFilter != "" && typeFilter != "all" && typeFilter != objType {
			return false
		}
		return scopeModel.canModerateType(objType)
	}
	status := ""
	if !statusAll {
		status = statusFilter
	}
	createdAfter := time.Time{}
	createdBefore := time.Time{}
	switch bucket {
	case moderacioAge0_24h:
		createdAfter = now.Add(-24 * time.Hour)
		createdBefore = now.Add(1 * time.Second)
	case moderacioAge1_3d:
		createdAfter = now.Add(-72 * time.Hour)
		createdBefore = now.Add(-24 * time.Hour)
	case moderacioAge3Plus:
		createdBefore = now.Add(-72 * time.Hour)
	}
	pendingOnly := statusAll || statusFilter == "pendent"
	total := 0

	personaFilter := db.PersonaFilter{
		Estat:         status,
		CreatedByIDs:  userIDs,
		CreatedAfter:  createdAfter,
		CreatedBefore: createdBefore,
	}
	if typeAllowed("persona") {
		if count, err := a.DB.CountPersones(personaFilter); err == nil {
			total += count
		}
	}
	arxiuFilter := db.ArxiuFilter{
		Status:        status,
		CreatedByIDs:  userIDs,
		CreatedAfter:  createdAfter,
		CreatedBefore: createdBefore,
	}
	if scope, ok := scopeModel.scopeFilterForType("arxiu"); ok && !scope.hasGlobal {
		applyScopeFilterToArxiu(&arxiuFilter, scope)
	}
	if typeAllowed("arxiu") {
		if count, err := a.DB.CountArxius(arxiuFilter); err == nil {
			total += count
		}
	}
	llibreFilter := db.LlibreFilter{
		Status:        status,
		CreatedByIDs:  userIDs,
		CreatedAfter:  createdAfter,
		CreatedBefore: createdBefore,
	}
	if scope, ok := scopeModel.scopeFilterForType("llibre"); ok && !scope.hasGlobal {
		applyScopeFilterToLlibre(&llibreFilter, scope)
	}
	if typeAllowed("llibre") {
		if count, err := a.DB.CountLlibres(llibreFilter); err == nil {
			total += count
		}
	}
	nivellFilter := db.NivellAdminFilter{
		Status:        status,
		CreatedByIDs:  userIDs,
		CreatedAfter:  createdAfter,
		CreatedBefore: createdBefore,
	}
	if scope, ok := scopeModel.scopeFilterForType("nivell"); ok && !scope.hasGlobal {
		applyScopeFilterToNivell(&nivellFilter, scope)
	}
	if typeAllowed("nivell") {
		if count, err := a.DB.CountNivells(nivellFilter); err == nil {
			total += count
		}
	}
	municipiFilter := db.MunicipiFilter{
		Status:        status,
		CreatedByIDs:  userIDs,
		CreatedAfter:  createdAfter,
		CreatedBefore: createdBefore,
	}
	if scope, ok := scopeModel.scopeFilterForType("municipi"); ok && !scope.hasGlobal {
		applyScopeFilterToMunicipi(&municipiFilter, scope)
	}
	if typeAllowed("municipi") {
		if count, err := a.DB.CountMunicipis(municipiFilter); err == nil {
			total += count
		}
	}
	eclesFilter := db.ArquebisbatFilter{
		Status:        status,
		CreatedByIDs:  userIDs,
		CreatedAfter:  createdAfter,
		CreatedBefore: createdBefore,
	}
	if scope, ok := scopeModel.scopeFilterForType("eclesiastic"); ok && !scope.hasGlobal {
		applyScopeFilterToEcles(&eclesFilter, scope)
	}
	if typeAllowed("eclesiastic") {
		if count, err := a.DB.CountArquebisbats(eclesFilter); err == nil {
			total += count
		}
	}
	if typeAllowed("municipi_mapa_version") {
		mapFilter := db.MunicipiMapaVersionFilter{
			Status:        status,
			CreatedByIDs:  userIDs,
			CreatedAfter:  createdAfter,
			CreatedBefore: createdBefore,
		}
		scope, _ := scopeModel.scopeFilterForType("municipi_mapa_version")
		mapScope := db.MunicipiScopeFilter{
			AllowedMunicipiIDs:  scope.municipiIDs,
			AllowedProvinciaIDs: scope.provinciaIDs,
			AllowedComarcaIDs:   scope.comarcaIDs,
			AllowedNivellIDs:    scope.nivellIDs,
			AllowedPaisIDs:      scope.paisIDs,
		}
		if count, err := a.DB.CountMunicipiMapaVersionsScoped(mapFilter, mapScope); err == nil {
			total += count
		}
	}
	if typeAllowed("external_link") {
		externalStatus := ""
		if !statusAll {
			externalStatus = externalLinkStatusFromModeracio(statusFilter)
		}
		filter := db.ExternalLinkAdminFilter{
			Status:        externalStatus,
			CreatedByIDs:  userIDs,
			CreatedAfter:  createdAfter,
			CreatedBefore: createdBefore,
		}
		if count, err := a.DB.CountExternalLinksAdmin(filter); err == nil {
			total += count
		}
	}
	if canModerateAll && typeAllowed("cognom_variant") {
		filter := db.CognomVariantFilter{
			Status:        status,
			CreatedByIDs:  userIDs,
			CreatedAfter:  createdAfter,
			CreatedBefore: createdBefore,
		}
		if count, err := a.DB.CountCognomVariants(filter); err == nil {
			total += count
		}
	}
	if canModerateAll && typeAllowed("cognom_referencia") {
		filter := db.CognomReferenciaFilter{
			Status:        status,
			CreatedByIDs:  userIDs,
			CreatedAfter:  createdAfter,
			CreatedBefore: createdBefore,
		}
		if count, err := a.DB.CountCognomReferencies(filter); err == nil {
			total += count
		}
	}
	if canModerateAll && typeAllowed("cognom_merge") {
		filter := db.CognomRedirectSuggestionFilter{
			Status:        status,
			CreatedByIDs:  userIDs,
			CreatedAfter:  createdAfter,
			CreatedBefore: createdBefore,
		}
		if count, err := a.DB.CountCognomRedirectSuggestions(filter); err == nil {
			total += count
		}
	}
	if canModerateAll && typeAllowed("event_historic") {
		filter := db.EventHistoricFilter{
			Status:        status,
			CreatedByIDs:  userIDs,
			CreatedAfter:  createdAfter,
			CreatedBefore: createdBefore,
		}
		if count, err := a.DB.CountEventsHistoric(filter); err == nil {
			total += count
		}
	}
	if typeAllowed("registre") {
		filter := db.TranscripcioFilter{
			Status:        status,
			CreatedByIDs:  userIDs,
			CreatedAfter:  createdAfter,
			CreatedBefore: createdBefore,
		}
		if scope, ok := scopeModel.scopeFilterForType("registre"); ok && !scope.hasGlobal {
			applyScopeFilterToRegistre(&filter, scope)
		}
		if count, err := a.DB.CountTranscripcionsRawGlobal(filter); err == nil {
			total += count
		}
	}
	if typeAllowed("registre_canvi") && pendingOnly {
		filter := db.TranscripcioFilter{
			CreatedByIDs:  userIDs,
			CreatedAfter:  createdAfter,
			CreatedBefore: createdBefore,
		}
		if scope, ok := scopeModel.scopeFilterForType("registre_canvi"); ok && !scope.hasGlobal {
			applyScopeFilterToRegistre(&filter, scope)
		}
		if count, err := a.DB.CountTranscripcioRawChangesPendingScoped(filter); err == nil {
			total += count
		}
	}
	if typeAllowed("municipi_historia_general") && pendingOnly {
		if count, err := a.countModeracioHistoriaGeneral(userIDs, createdAfter, createdBefore, scopeModel, canModerateAll); err == nil {
			total += count
		}
	}
	if typeAllowed("municipi_historia_fet") && pendingOnly {
		if count, err := a.countModeracioHistoriaFets(userIDs, createdAfter, createdBefore, scopeModel, canModerateAll); err == nil {
			total += count
		}
	}
	if typeAllowed("municipi_anecdota_version") && pendingOnly {
		if count, err := a.countModeracioAnecdotes(userIDs, createdAfter, createdBefore, scopeModel, canModerateAll); err == nil {
			total += count
		}
	}
	needsWikiChanges := typeAllowed("municipi_canvi") || typeAllowed("arxiu_canvi") || typeAllowed("llibre_canvi") || typeAllowed("persona_canvi") || typeAllowed("cognom_canvi") || typeAllowed("event_historic_canvi")
	if needsWikiChanges && pendingOnly {
		if counts, err := a.countModeracioWikiChanges(userIDs, createdAfter, createdBefore, scopeModel, canModerateAll, typeAllowed); err == nil {
			for _, count := range counts {
				total += count
			}
		}
	}

	return total
}

func (a *App) firstPendingActivityTime(objectType string, objectID int) string {
	if acts, err := a.DB.ListActivityByObject(objectType, objectID, "pendent"); err == nil {
		for _, act := range acts {
			return act.CreatedAt.Format("2006-01-02 15:04")
		}
	}
	return "—"
}

func parseModeracioTime(val string) time.Time {
	if val == "" || val == "—" {
		return time.Time{}
	}
	t, err := time.Parse("2006-01-02 15:04", val)
	if err != nil {
		return time.Time{}
	}
	return t
}

func parseModeracioFilters(r *http.Request) (moderacioFilters, string) {
	filterType := strings.TrimSpace(r.URL.Query().Get("type"))
	filterStatus := strings.TrimSpace(r.URL.Query().Get("status"))
	if filterStatus == "" {
		filterStatus = "pendent"
	}
	filterAge := strings.TrimSpace(r.URL.Query().Get("age"))
	switch filterAge {
	case moderacioAge0_24h, moderacioAge1_3d, moderacioAge3Plus:
	default:
		filterAge = ""
	}
	userInput := strings.TrimSpace(r.URL.Query().Get("user"))
	filterUserID := 0
	filterUserQuery := strings.TrimSpace(userInput)
	if filterUserQuery != "" {
		filterUserQuery = strings.TrimPrefix(filterUserQuery, "@")
		if n, err := strconv.Atoi(filterUserQuery); err == nil && n > 0 {
			filterUserID = n
			filterUserQuery = ""
		}
	}
	return moderacioFilters{
		Type:      filterType,
		Status:    filterStatus,
		AgeBucket: filterAge,
		UserID:    filterUserID,
		UserQuery: filterUserQuery,
	}, userInput
}

func moderacioReturnWithFlag(path string, flag string) string {
	separator := "?"
	if strings.Contains(path, "?") {
		separator = "&"
	}
	return path + separator + flag + "=1"
}

func intSliceContains(list []int, value int) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func shouldUseScopedCount(userIDs []int, createdAfter, createdBefore time.Time) bool {
	return len(userIDs) == 0 && createdAfter.IsZero() && createdBefore.IsZero()
}

func (a *App) requireModeracioUser(w http.ResponseWriter, r *http.Request) (*db.User, db.PolicyPermissions, bool, bool) {
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return nil, db.PolicyPermissions{}, false, false
	}
	*r = *a.withUser(r, user)
	perms, found := a.permissionsFromContext(r)
	if !found {
		perms = a.getPermissionsForUser(user.ID)
		*r = *a.withPermissions(r, perms)
	}
	canModerateAll := a.hasPerm(perms, permModerate)
	scopeModel := a.newModeracioScopeModel(user, perms, canModerateAll)
	if canModerateAll || scopeModel.canModerateAnything() {
		return user, perms, canModerateAll, true
	}
	http.Error(w, "Forbidden", http.StatusForbidden)
	return user, perms, false, false
}

func (a *App) canModeracioMassiva(user *db.User, perms db.PolicyPermissions) bool {
	if user == nil {
		return false
	}
	if a.hasPerm(perms, permAdmin) {
		return true
	}
	return a.hasAnyPermissionKey(user.ID, permKeyModeracioMassiva)
}

func (a *App) requireModeracioMassivaUser(w http.ResponseWriter, r *http.Request) (*db.User, db.PolicyPermissions, bool, bool) {
	user, perms, _, ok := a.requireModeracioUser(w, r)
	if !ok {
		return nil, db.PolicyPermissions{}, false, false
	}
	isAdmin := a.hasPerm(perms, permAdmin)
	if isAdmin || a.hasAnyPermissionKey(user.ID, permKeyModeracioMassiva) {
		return user, perms, isAdmin, true
	}
	http.Error(w, "Forbidden", http.StatusForbidden)
	return user, perms, isAdmin, false
}

// Llista de persones pendents de moderació
func (a *App) AdminModeracioList(w http.ResponseWriter, r *http.Request) {
	handlerStart := time.Now()
	user, perms, canModerateAll, ok := a.requireModeracioUser(w, r)
	if !ok {
		return
	}
	page := 1
	perPage := 25
	if val := strings.TrimSpace(r.URL.Query().Get("page")); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			page = n
		}
	}
	if val := strings.TrimSpace(r.URL.Query().Get("per_page")); val != "" {
		if n, err := strconv.Atoi(val); err == nil {
			switch n {
			case 10, 25, 50, 100:
				perPage = n
			}
		}
	}
	filters, userInput := parseModeracioFilters(r)
	filterType := filters.Type
	filterStatus := filters.Status
	filterAge := filters.AgeBucket
	metrics := &moderacioBuildMetrics{}
	pageItems, total, summary, err := a.buildModeracioItems(ResolveLang(r), page, perPage, user, perms, canModerateAll, filters, metrics)
	if err != nil {
		http.Error(w, "No s'ha pogut carregar la moderació", http.StatusInternalServerError)
		return
	}
	totalPages := 1
	if total > 0 {
		totalPages = (total + perPage - 1) / perPage
	}
	if page > totalPages {
		page = totalPages
	}
	start := (page - 1) * perPage
	if start < 0 {
		start = 0
	}
	end := start + perPage
	if end > total {
		end = total
	}
	pageStart := 0
	pageEnd := 0
	if total > 0 {
		pageStart = start + 1
		pageEnd = end
	}
	pageValues := url.Values{}
	pageValues.Set("per_page", strconv.Itoa(perPage))
	if filterType != "" {
		pageValues.Set("type", filterType)
	}
	if filterStatus != "" {
		pageValues.Set("status", filterStatus)
	}
	if filterAge != "" {
		pageValues.Set("age", filterAge)
	}
	if userInput != "" {
		pageValues.Set("user", userInput)
	}
	pageBase := "/moderacio"
	if encoded := pageValues.Encode(); encoded != "" {
		pageBase += "?" + encoded
	}
	canManageArxius := a.hasPerm(perms, permArxius)
	isAdmin := a.hasPerm(perms, permAdmin)
	canBulk := a.canModeracioMassiva(user, perms)
	msg := ""
	okFlag := false
	if r.URL.Query().Get("ok") != "" {
		okFlag = true
		msg = T(ResolveLang(r), "moderation.success")
	} else if r.URL.Query().Get("err") != "" {
		msg = T(ResolveLang(r), "moderation.error")
	}
	renderStart := time.Now()
	RenderPrivateTemplate(w, r, "admin-moderacio-list.html", map[string]interface{}{
		"Persones":        pageItems,
		"CanModerate":     true,
		"CanManageArxius": canManageArxius,
		"IsAdmin":         isAdmin,
		"Msg":             msg,
		"Ok":              okFlag,
		"CanBulk":         canBulk,
		"User":            user,
		"Total":           total,
		"Page":            page,
		"PerPage":         perPage,
		"TotalPages":      totalPages,
		"HasPrev":         page > 1,
		"HasNext":         page < totalPages,
		"PrevPage":        page - 1,
		"NextPage":        page + 1,
		"PageStart":       pageStart,
		"PageEnd":         pageEnd,
		"PageBase":        pageBase,
		"FilterType":      filterType,
		"FilterStatus":    filterStatus,
		"FilterAge":       filterAge,
		"FilterUser":      userInput,
		"Summary":         summary,
		"ReturnTo":        r.URL.RequestURI(),
	})
	if IsDebugEnabled() {
		scopeMode := "scoped"
		if canModerateAll {
			scopeMode = "global"
		}
		renderDur := time.Since(renderStart)
		Debugf("moderacio entry user=%d page=%d page_size=%d count_dur=%s list_dur=%s list_fetch_dur=%s list_build_dur=%s render_dur=%s total_dur=%s scope=%s type=%s status=%s age=%s", user.ID, page, perPage, metrics.countDur, metrics.listDur, metrics.listFetchDur, metrics.listBuildDur, renderDur, time.Since(handlerStart), scopeMode, filterType, filterStatus, filterAge)
	}
}

func (a *App) applyModeracioAction(ctx context.Context, action string, objType string, id int, motiu string, userID int, metrics *moderacioApplyMetrics) error {
	if err := a.applyModeracioUpdate(action, objType, id, motiu, userID, metrics); err != nil {
		return err
	}
	return a.applyModeracioActivity(ctx, action, objType, id, motiu, userID, metrics)
}

func (a *App) applyModeracioUpdate(action string, objType string, id int, motiu string, userID int, metrics *moderacioApplyMetrics) error {
	start := time.Now()
	var err error
	switch action {
	case "approve":
		err = a.updateModeracioObject(objType, id, "publicat", "", userID)
	case "reject":
		err = a.updateModeracioObject(objType, id, "rebutjat", motiu, userID)
	default:
		err = fmt.Errorf("acció no vàlida")
	}
	if metrics != nil {
		metrics.UpdateDur += time.Since(start)
	}
	return err
}

func (a *App) applyModeracioActivity(ctx context.Context, action string, objType string, id int, motiu string, userID int, metrics *moderacioApplyMetrics) error {
	start := time.Now()
	switch action {
	case "approve":
		if acts, err := a.DB.ListActivityByObject(objType, id, "pendent"); err == nil {
			for _, act := range acts {
				_ = a.ValidateActivity(act.ID, userID)
			}
		}
		if objType == "municipi_mapa_version" {
			_, _ = a.RegisterUserActivity(ctx, userID, ruleMunicipiMapaApprove, "moderar_aprovar", objType, &id, "validat", nil, "")
		} else {
			_, _ = a.RegisterUserActivity(ctx, userID, ruleModeracioApprove, "moderar_aprovar", objType, &id, "validat", nil, "")
		}
		if objType == "event_historic" {
			a.registerEventHistoricModerationActivity(ctx, id, "publicat", userID, "")
		}
	case "reject":
		if acts, err := a.DB.ListActivityByObject(objType, id, "pendent"); err == nil {
			for _, act := range acts {
				_ = a.DB.UpdateUserActivityStatus(act.ID, "anulat", &userID)
			}
		}
		if objType == "municipi_mapa_version" {
			_, _ = a.RegisterUserActivity(ctx, userID, ruleMunicipiMapaReject, "moderar_rebutjar", objType, &id, "validat", nil, motiu)
		} else {
			_, _ = a.RegisterUserActivity(ctx, userID, ruleModeracioReject, "moderar_rebutjar", objType, &id, "validat", nil, motiu)
		}
		if objType == "event_historic" {
			a.registerEventHistoricModerationActivity(ctx, id, "rebutjat", userID, motiu)
		}
	default:
		if metrics != nil {
			metrics.ActivityDur += time.Since(start)
		}
		return fmt.Errorf("acció no vàlida")
	}
	if metrics != nil {
		metrics.ActivityDur += time.Since(start)
	}
	return nil
}

func (a *App) applyModeracioActivitiesBulk(ctx context.Context, action string, objType string, ids []int, motiu string, userID int, metrics *moderacioApplyMetrics) error {
	if len(ids) == 0 {
		return nil
	}
	start := time.Now()
	defer func() {
		if metrics != nil {
			metrics.ActivityDur += time.Since(start)
		}
	}()
	acts, err := a.DB.ListActivityByObjects(objType, ids, "pendent")
	if err != nil {
		return fmt.Errorf("list_pending_activities failed: %w", err)
	}
	pendingIDs := make([]int, 0, len(acts))
	pendingUsers := map[int]struct{}{}
	pointsByUser := map[int]int{}
	for _, act := range acts {
		pendingIDs = append(pendingIDs, act.ID)
		if act.UserID > 0 {
			pendingUsers[act.UserID] = struct{}{}
			if action == "approve" && act.Points != 0 {
				pointsByUser[act.UserID] += act.Points
			}
		}
	}
	switch action {
	case "approve":
		if err := a.DB.BulkUpdateUserActivityStatus(pendingIDs, "validat", &userID); err != nil {
			return fmt.Errorf("bulk_update_pending_status failed: %w", err)
		}
		for uid, delta := range pointsByUser {
			if delta == 0 {
				continue
			}
			if err := a.DB.AddPointsToUser(uid, delta); err != nil {
				return fmt.Errorf("author_points_update failed user=%d delta=%d: %w", uid, delta, err)
			}
		}
	case "reject":
		if err := a.DB.BulkUpdateUserActivityStatus(pendingIDs, "anulat", &userID); err != nil {
			return fmt.Errorf("bulk_update_pending_status failed: %w", err)
		}
	default:
		return fmt.Errorf("acció no vàlida")
	}
	activityRows, pointsPerActivity, ruleCode, activityAction, details := buildModeracioBulkActivityRows(a, action, objType, ids, motiu, userID)
	insertedCount := 0
	if len(activityRows) > 0 {
		if _, err := a.DB.BulkInsertUserActivities(ctx, activityRows); err != nil {
			bulkInsertErr := err
			for i := range activityRows {
				row := activityRows[i]
				if _, err := a.DB.InsertUserActivity(&row); err != nil {
					return fmt.Errorf("insert_activity_fallback failed after bulk_insert_error=%v object_type=%s object_id=%d: %w", bulkInsertErr, row.ObjectType, row.ObjectID.Int64, err)
				}
				insertedCount++
			}
		} else {
			insertedCount = len(activityRows)
		}
	}
	if insertedCount > 0 && pointsPerActivity != 0 {
		if err := a.DB.AddPointsToUser(userID, pointsPerActivity*insertedCount); err != nil {
			return fmt.Errorf("moderator_points_update failed user=%d delta=%d: %w", userID, pointsPerActivity*insertedCount, err)
		}
	}
	now := time.Now()
	for uid := range pendingUsers {
		a.EvaluateAchievementsForUser(context.Background(), uid, AchievementTrigger{CreatedAt: now})
		a.logAntiAbuseSignals(uid, now)
	}
	if insertedCount > 0 {
		a.EvaluateAchievementsForUser(context.Background(), userID, AchievementTrigger{
			RuleCode:   ruleCode,
			Action:     activityAction,
			ObjectType: objType,
			Status:     "validat",
			CreatedAt:  now,
		})
		a.logAntiAbuseSignals(userID, now)
	}
	if objType == "event_historic" {
		for _, id := range ids {
			if action == "approve" {
				a.registerEventHistoricModerationActivity(ctx, id, "publicat", userID, details)
			} else {
				a.registerEventHistoricModerationActivity(ctx, id, "rebutjat", userID, details)
			}
		}
	}
	return nil
}

func buildModeracioBulkActivityRows(a *App, action, objType string, ids []int, motiu string, userID int) ([]db.UserActivity, int, string, string, string) {
	ruleCode := ""
	activityAction := ""
	details := ""
	switch action {
	case "approve":
		activityAction = "moderar_aprovar"
		if objType == "municipi_mapa_version" {
			ruleCode = ruleMunicipiMapaApprove
		} else {
			ruleCode = ruleModeracioApprove
		}
	case "reject":
		activityAction = "moderar_rebutjar"
		details = motiu
		if objType == "municipi_mapa_version" {
			ruleCode = ruleMunicipiMapaReject
		} else {
			ruleCode = ruleModeracioReject
		}
	default:
		return nil, 0, "", "", ""
	}
	ruleID := sql.NullInt64{}
	points := 0
	if a != nil && a.DB != nil && ruleCode != "" {
		if rule, ok := getPointsRuleByCodeCached(a.DB, ruleCode); ok && rule != nil && rule.Active {
			ruleID = sql.NullInt64{Int64: int64(rule.ID), Valid: true}
			points = rule.Points
		}
	}
	rows := make([]db.UserActivity, 0, len(ids))
	for _, id := range ids {
		if id <= 0 {
			continue
		}
		rows = append(rows, db.UserActivity{
			UserID:     userID,
			RuleID:     ruleID,
			Action:     activityAction,
			ObjectType: objType,
			ObjectID:   sql.NullInt64{Int64: int64(id), Valid: true},
			Points:     points,
			Status:     "validat",
			Details:    details,
		})
	}
	return rows, points, ruleCode, activityAction, details
}

func listNivellAncestorsForMunicipiCached(a *App, munID int, cache map[int][]int) []int {
	if munID <= 0 {
		return nil
	}
	if cache != nil {
		if ids, ok := cache[munID]; ok {
			return ids
		}
	}
	ids := a.listNivellAncestorsForMunicipi(munID)
	if cache != nil {
		cache[munID] = ids
	}
	return ids
}

func firstArxiuIDForLlibre(arxiusByLlibre map[int][]db.ArxiuLlibreDetail, llibreID int) int {
	if len(arxiusByLlibre) == 0 || llibreID <= 0 {
		return 0
	}
	rows := arxiusByLlibre[llibreID]
	if len(rows) == 0 {
		return 0
	}
	return rows[0].ArxiuID
}

func bulkUpdateResultStatusFromAction(action, motiu string) (string, string, error) {
	switch action {
	case "approve":
		return "publicat", "", nil
	case "reject":
		return "rebutjat", motiu, nil
	default:
		return "", "", fmt.Errorf("acció no vàlida")
	}
}

func (a *App) applyModeracioBulkRegistreUpdates(action string, ids []int, motiu string, moderatorID int, metrics *moderacioApplyMetrics, onChunk func(moderacioBulkRegistreChunkMetrics)) moderacioBulkRegistreUpdateResult {
	result := moderacioBulkRegistreUpdateResult{SuccessIDs: make([]int, 0, len(ids))}
	estat, notes, err := bulkUpdateResultStatusFromAction(action, motiu)
	if err != nil {
		for _, id := range ids {
			result.Errors = append(result.Errors, moderacioBulkRegistreItemError{ID: id, Err: err})
		}
		return result
	}
	const chunkSize = 500
	nivellCache := map[int][]int{}
	for start := 0; start < len(ids); start += chunkSize {
		end := start + chunkSize
		if end > len(ids) {
			end = len(ids)
		}
		chunkIDs := ids[start:end]
		chunkStart := time.Now()
		chunkErrorStart := len(result.Errors)
		chunkMetrics := moderacioBulkRegistreChunkMetrics{
			ChunkIndex:       (start / chunkSize) + 1,
			ChunkSize:        len(chunkIDs),
			DeferredActivity: true,
		}
		loadStart := time.Now()
		rows, err := a.DB.ListTranscripcionsRawByIDs(chunkIDs)
		if err != nil {
			chunkMetrics.LoadDur = time.Since(loadStart)
			chunkMetrics.Errors = len(chunkIDs)
			chunkMetrics.TotalDur = time.Since(chunkStart)
			if chunkMetrics.TotalDur > 0 {
				chunkMetrics.Throughput = float64(len(chunkIDs)) / chunkMetrics.TotalDur.Seconds()
			}
			for _, id := range chunkIDs {
				result.Errors = append(result.Errors, moderacioBulkRegistreItemError{ID: id, Err: err})
			}
			if onChunk != nil {
				onChunk(chunkMetrics)
			}
			continue
		}
		rowByID := make(map[int]db.TranscripcioRaw, len(rows))
		llibreIDs := make([]int, 0, len(rows))
		seenLlibres := map[int]struct{}{}
		foundIDs := make([]int, 0, len(rows))
		for _, row := range rows {
			rowByID[row.ID] = row
			foundIDs = append(foundIDs, row.ID)
			if row.LlibreID > 0 {
				if _, ok := seenLlibres[row.LlibreID]; !ok {
					seenLlibres[row.LlibreID] = struct{}{}
					llibreIDs = append(llibreIDs, row.LlibreID)
				}
			}
		}
		personesByRegistre, err := a.DB.ListTranscripcioPersonesByTranscripcioIDs(foundIDs)
		if err != nil {
			Errorf("Moderacio bulk registre persones batch ha fallat: %v", err)
			personesByRegistre = map[int][]db.TranscripcioPersonaRaw{}
		}
		llibresByID, err := a.DB.GetLlibresByIDs(llibreIDs)
		if err != nil {
			Errorf("Moderacio bulk registre llibres batch ha fallat: %v", err)
			llibresByID = map[int]*db.Llibre{}
		}
		arxiusByLlibre, err := a.DB.ListLlibreArxiusByLlibreIDs(llibreIDs)
		if err != nil {
			Errorf("Moderacio bulk registre arxius batch ha fallat: %v", err)
			arxiusByLlibre = map[int][]db.ArxiuLlibreDetail{}
		}
		chunkMetrics.LoadDur = time.Since(loadStart)
		chunkMetrics.LoadedRows = len(rows)

		states := make([]moderacioBulkRegistreState, 0, len(rows))
		noDemoIDs := make([]int, 0, len(rows))
		demoGroups := map[moderacioBulkRegistreDemoKey][]int{}
		plannedSearchUpserts := 0
		plannedSearchDeletes := 0
		plannedNomCognom := 0
		for _, id := range chunkIDs {
			row, ok := rowByID[id]
			if !ok {
				result.Errors = append(result.Errors, moderacioBulkRegistreItemError{ID: id, Err: fmt.Errorf("registre no trobat")})
				continue
			}
			state := moderacioBulkRegistreState{
				Reg:      row,
				Persones: personesByRegistre[id],
				Llibre:   llibresByID[row.LlibreID],
				ArxiuID:  firstArxiuIDForLlibre(arxiusByLlibre, row.LlibreID),
				Delta:    demografiaDeltaFromStatus(row.ModeracioEstat, estat),
			}
			if state.Delta != 0 && state.Llibre != nil {
				if munID, year, tipus, ok := demografiaDeltaFromRegistre(&state.Reg, state.Llibre); ok {
					state.MunicipiID = munID
					state.Year = year
					state.Tipus = tipus
					key := moderacioBulkRegistreDemoKey{MunicipiID: munID, Year: year, Tipus: tipus, Delta: state.Delta}
					demoGroups[key] = append(demoGroups[key], id)
				} else {
					noDemoIDs = append(noDemoIDs, id)
				}
			} else {
				noDemoIDs = append(noDemoIDs, id)
			}
			if estat == "publicat" {
				plannedSearchUpserts++
			} else if row.ModeracioEstat == "publicat" {
				plannedSearchDeletes++
			}
			if state.Delta != 0 && state.Llibre != nil {
				plannedNomCognom++
			}
			states = append(states, state)
		}
		if IsDebugEnabled() {
			Debugf("moderacio bulk registre chunk plan=%d ids=%d found=%d unique_books=%d no_demo=%d demo_groups=%d planned_search_upserts=%d planned_search_deletes=%d planned_nom_stats=%d batch_reads=4", chunkMetrics.ChunkIndex, len(chunkIDs), len(foundIDs), len(llibreIDs), len(noDemoIDs), len(demoGroups), plannedSearchUpserts, plannedSearchDeletes, plannedNomCognom)
		}

		successSet := map[int]struct{}{}
		successIDs := make([]int, 0, len(foundIDs))
		markGroupError := func(groupIDs []int, groupErr error) {
			for _, id := range groupIDs {
				result.Errors = append(result.Errors, moderacioBulkRegistreItemError{ID: id, Err: groupErr})
			}
		}
		markUpdated := func(groupIDs []int, updated int, label string) {
			if updated < 0 {
				updated = 0
			}
			if updated < len(groupIDs) {
				result.Skipped += len(groupIDs) - updated
				Errorf("Moderacio bulk registre %s ha actualitzat %d/%d files", label, updated, len(groupIDs))
			}
			for _, id := range groupIDs {
				successSet[id] = struct{}{}
				successIDs = append(successIDs, id)
			}
		}

		chunkUpdateDur := time.Duration(0)
		if len(foundIDs) > 0 {
			updateStart := time.Now()
			updatedNow, err := a.DB.BulkUpdateTranscripcioModeracio(estat, notes, moderatorID, foundIDs)
			updateElapsed := time.Since(updateStart)
			chunkUpdateDur += updateElapsed
			if metrics != nil {
				metrics.UpdateDur += updateElapsed
			}
			if err != nil {
				markGroupError(foundIDs, err)
			} else {
				markUpdated(foundIDs, updatedNow, "bulk_update")
			}
		}

		chunkUpdated := 0
		for _, id := range successIDs {
			result.SuccessIDs = append(result.SuccessIDs, id)
			result.Updated++
			chunkUpdated++
		}
		derivedStart := time.Now()
		derivedMetrics := a.applyModeracioBulkRegistreDerivedSideEffects(states, successSet, estat, demoGroups, nivellCache)
		chunkMetrics.DerivedDemografiaDur = derivedMetrics.DemografiaDur
		chunkMetrics.DerivedStatsDur = derivedMetrics.StatsDur
		chunkMetrics.DerivedSearchDur = derivedMetrics.SearchDur
		chunkMetrics.DerivedDur = time.Since(derivedStart)
		chunkMetrics.PostprocDur = chunkMetrics.DerivedDur
		chunkMetrics.UpdateDur = chunkUpdateDur
		chunkMetrics.Updated = chunkUpdated
		chunkMetrics.Errors = len(result.Errors) - chunkErrorStart
		chunkMetrics.TotalDur = time.Since(chunkStart)
		if chunkMetrics.TotalDur > 0 {
			chunkMetrics.Throughput = float64(len(chunkIDs)) / chunkMetrics.TotalDur.Seconds()
		}
		if IsDebugEnabled() {
			Debugf("moderacio bulk registre chunk=%d size=%d loaded=%d updated=%d errors=%d load_dur=%s update_dur=%s derived_dur=%s derived_demografia_dur=%s derived_stats_dur=%s derived_search_dur=%s activity_dur=%s audit_dur=%s postproc_dur=%s total_dur=%s throughput=%.1f/s deferred_activity=%t", chunkMetrics.ChunkIndex, chunkMetrics.ChunkSize, chunkMetrics.LoadedRows, chunkMetrics.Updated, chunkMetrics.Errors, chunkMetrics.LoadDur, chunkMetrics.UpdateDur, chunkMetrics.DerivedDur, chunkMetrics.DerivedDemografiaDur, chunkMetrics.DerivedStatsDur, chunkMetrics.DerivedSearchDur, chunkMetrics.ActivityDur, chunkMetrics.AuditDur, chunkMetrics.PostprocDur, chunkMetrics.TotalDur, chunkMetrics.Throughput, chunkMetrics.DeferredActivity)
		}
		if onChunk != nil {
			onChunk(chunkMetrics)
		}
	}
	return result
}

func (a *App) applyModeracioBulkRegistreDerivedSideEffects(states []moderacioBulkRegistreState, successSet map[int]struct{}, estat string, demoGroups map[moderacioBulkRegistreDemoKey][]int, nivellCache map[int][]int) moderacioBulkRegistreDerivedMetrics {
	metrics := moderacioBulkRegistreDerivedMetrics{}
	if len(states) == 0 || len(successSet) == 0 {
		return metrics
	}

	successByID := make(map[int]moderacioBulkRegistreState, len(successSet))
	for _, state := range states {
		if _, ok := successSet[state.Reg.ID]; !ok {
			continue
		}
		if state.Delta != 0 && state.Llibre != nil && state.MunicipiID <= 0 {
			state.MunicipiID = demografiaMunicipiIDFromRegistre(&state.Reg, state.Llibre)
		}
		successByID[state.Reg.ID] = state
	}

	demoStart := time.Now()
	for key, groupIDs := range demoGroups {
		appliedCount := 0
		for _, id := range groupIDs {
			if _, ok := successByID[id]; ok {
				appliedCount++
			}
		}
		if appliedCount == 0 {
			continue
		}
		totalDelta := key.Delta * appliedCount
		if totalDelta == 0 {
			continue
		}
		if err := a.DB.ApplyMunicipiDemografiaDelta(key.MunicipiID, key.Year, key.Tipus, totalDelta); err != nil {
			Errorf("Error actualitzant demografia municipi %d: %v", key.MunicipiID, err)
			continue
		}
		nivellIDs := listNivellAncestorsForMunicipiCached(a, key.MunicipiID, nivellCache)
		a.applyNivellDemografiaDeltaForMunicipiWithNivells(key.MunicipiID, key.Year, key.Tipus, totalDelta, nivellIDs)
	}
	metrics.DemografiaDur = time.Since(demoStart)

	statsStart := time.Now()
	for _, state := range successByID {
		if state.Delta == 0 || state.Llibre == nil || state.MunicipiID <= 0 {
			continue
		}
		contrib := calcNomCognomContribs(state.Reg, state.Persones)
		nivellIDs := listNivellAncestorsForMunicipiCached(a, state.MunicipiID, nivellCache)
		if err := a.applyNomCognomDeltaWithNivells(state.MunicipiID, contrib, state.Delta, nivellIDs); err != nil {
			Errorf("Error actualitzant stats noms/cognoms municipi %d: %v", state.MunicipiID, err)
		}
	}
	metrics.StatsDur = time.Since(statsStart)

	searchStart := time.Now()
	for _, state := range successByID {
		oldStatus := state.Reg.ModeracioEstat
		state.Reg.ModeracioEstat = estat
		if estat == "publicat" {
			if err := a.upsertSearchDocForRegistre(&state.Reg, state.Persones, state.Llibre, state.ArxiuID); err != nil {
				Errorf("SearchIndex registre %d: %v", state.Reg.ID, err)
			}
		} else if oldStatus == "publicat" {
			if err := a.DB.DeleteSearchDoc("registre_raw", state.Reg.ID); err != nil {
				Errorf("SearchIndex delete registre %d: %v", state.Reg.ID, err)
			}
		}
	}
	metrics.SearchDur = time.Since(searchStart)
	return metrics
}

func (a *App) processModeracioBulkAll(ctx context.Context, action, bulkType, motiu string, user *db.User, perms db.PolicyPermissions, canModerateAll bool, bulkUserID int, update func(processed int, total int)) (moderacioBulkResult, moderacioBulkMetrics, error) {
	if update == nil {
		update = func(int, int) {}
	}
	start := time.Now()
	candidates := 0
	processed := 0
	total := 0
	errCount := 0
	skipped := 0
	resolveDur := time.Duration(0)
	updateDur := time.Duration(0)
	applyMetrics := &moderacioApplyMetrics{}
	perItemUsed := false
	bulkUsed := false
	scopeModel := a.newModeracioScopeModel(user, perms, canModerateAll)
	scopeMode := "scoped"
	if scopeModel.canModerateAll {
		scopeMode = "global"
	}
	allowedTypes := scopeModel.allowedTypes()
	allowedMap := map[string]bool{}
	for _, t := range allowedTypes {
		allowedMap[t] = true
	}

	updateTotal := func(add int) {
		if add <= 0 {
			return
		}
		total += add
		update(processed, total)
	}
	updateCandidates := func(add int) {
		if add <= 0 {
			return
		}
		candidates += add
	}
	apply := func(objType string, id int) {
		perItemUsed = true
		if err := a.applyModeracioAction(ctx, action, objType, id, motiu, user.ID, applyMetrics); err != nil {
			Errorf("Moderacio massiva %s %s:%d ha fallat: %v", action, objType, id, err)
			errCount++
		}
		processed++
		update(processed, total)
	}
	applyActivitiesBulk := func(objType string, ids []int) {
		if len(ids) == 0 {
			return
		}
		perItemUsed = true
		if err := a.applyModeracioActivitiesBulk(ctx, action, objType, ids, motiu, user.ID, applyMetrics); err != nil {
			Errorf("Moderacio massiva %s activitats %s bulk ha fallat: %v", action, objType, err)
			errCount++
		}
		for range ids {
			processed++
			update(processed, total)
		}
	}

	bulkStatus := ""
	bulkNotes := ""
	switch action {
	case "approve":
		bulkStatus = "publicat"
	case "reject":
		bulkStatus = "rebutjat"
		bulkNotes = motiu
	default:
		return moderacioBulkResult{}, moderacioBulkMetrics{}, fmt.Errorf("acció no vàlida")
	}

	wikiPendingByType := map[string][]int{}
	types := allowedTypes
	if bulkType != "" && bulkType != "all" {
		if !allowedMap[bulkType] {
			metrics := moderacioBulkMetrics{TotalDur: time.Since(start), Mode: "per-item", ScopeMode: scopeMode}
			return moderacioBulkResult{Candidates: candidates, Total: total, Processed: processed, Errors: 1, Skipped: skipped}, metrics, fmt.Errorf("tipus no autoritzat")
		}
		types = []string{bulkType}
	}
	if len(types) == 0 {
		metrics := moderacioBulkMetrics{TotalDur: time.Since(start), Mode: "per-item", ScopeMode: scopeMode}
		return moderacioBulkResult{Candidates: candidates, Total: total, Processed: processed, Errors: 1, Skipped: skipped}, metrics, fmt.Errorf("cap tipus autoritzat")
	}

	resolveStart := time.Now()
	needsWikiChanges := false
	for _, t := range types {
		switch t {
		case "municipi_canvi", "arxiu_canvi", "llibre_canvi", "persona_canvi", "cognom_canvi", "event_historic_canvi":
			needsWikiChanges = true
		}
		if needsWikiChanges {
			break
		}
	}
	if needsWikiChanges {
		if changes, stale, err := a.DB.ListWikiPendingChanges(0, 0); err == nil {
			for _, changeID := range stale {
				_ = a.DB.DequeueWikiPending(changeID)
			}
			for _, change := range changes {
				objType := resolveWikiChangeModeracioType(change)
				if objType == "" {
					metrics := moderacioBulkMetrics{TotalDur: time.Since(start), Mode: "per-item", ScopeMode: scopeMode}
					return moderacioBulkResult{Candidates: candidates, Total: total, Processed: processed, Errors: 1, Skipped: skipped}, metrics, fmt.Errorf("wiki change sense tipus moderable: %d", change.ID)
				}
				if !allowedMap[objType] {
					continue
				}
				if !scopeModel.canModerateAll {
					if !scopeModel.canModerateWikiChange(change, objType) {
						continue
					}
				}
				wikiPendingByType[objType] = append(wikiPendingByType[objType], change.ID)
			}
		} else {
			errCount++
		}
	}
	resolveDur += time.Since(resolveStart)
	for _, objType := range types {
		switch objType {
		case "persona":
			if !scopeModel.canModerateType("persona") {
				break
			}
			resolveStart = time.Now()
			rows, err := a.DB.ListPersones(db.PersonaFilter{Estat: "pendent"})
			resolveDur += time.Since(resolveStart)
			if err != nil {
				errCount++
				break
			}
			updateCandidates(len(rows))
			updateTotal(len(rows))
			for _, row := range rows {
				apply(objType, row.ID)
			}
		case "arxiu":
			resolveStart = time.Now()
			filter := db.ArxiuFilter{Status: "pendent", Limit: -1}
			if scope, ok := scopeModel.scopeFilterForType("arxiu"); ok && !scope.hasGlobal {
				applyScopeFilterToArxiu(&filter, scope)
			}
			rows, err := a.DB.ListArxius(filter)
			resolveDur += time.Since(resolveStart)
			if err != nil {
				errCount++
				break
			}
			updateCandidates(len(rows))
			ids := make([]int, 0, len(rows))
			for _, row := range rows {
				ids = append(ids, row.ID)
			}
			updateTotal(len(ids))
			if len(ids) == 0 {
				break
			}
			bulkUsed = true
			updateStart := time.Now()
			updated, err := a.DB.BulkUpdateModeracioSimple(objType, bulkStatus, bulkNotes, user.ID, ids)
			updateDur += time.Since(updateStart)
			if err != nil {
				errCount++
				break
			}
			if updated < len(ids) {
				skipped += len(ids) - updated
			}
			applyActivitiesBulk(objType, ids)
		case "llibre":
			resolveStart = time.Now()
			filter := db.LlibreFilter{Status: "pendent"}
			if scope, ok := scopeModel.scopeFilterForType("llibre"); ok && !scope.hasGlobal {
				applyScopeFilterToLlibre(&filter, scope)
			}
			rows, err := a.DB.ListLlibres(filter)
			resolveDur += time.Since(resolveStart)
			if err != nil {
				errCount++
				break
			}
			updateCandidates(len(rows))
			ids := make([]int, 0, len(rows))
			for _, row := range rows {
				ids = append(ids, row.ID)
			}
			updateTotal(len(ids))
			if len(ids) == 0 {
				break
			}
			bulkUsed = true
			updateStart := time.Now()
			updated, err := a.DB.BulkUpdateModeracioSimple(objType, bulkStatus, bulkNotes, user.ID, ids)
			updateDur += time.Since(updateStart)
			if err != nil {
				errCount++
				break
			}
			if updated < len(ids) {
				skipped += len(ids) - updated
			}
			applyActivitiesBulk(objType, ids)
		case "nivell":
			resolveStart = time.Now()
			filter := db.NivellAdminFilter{Status: "pendent"}
			if scope, ok := scopeModel.scopeFilterForType("nivell"); ok && !scope.hasGlobal {
				applyScopeFilterToNivell(&filter, scope)
			}
			rows, err := a.DB.ListNivells(filter)
			resolveDur += time.Since(resolveStart)
			if err != nil {
				errCount++
				break
			}
			updateCandidates(len(rows))
			ids := make([]int, 0, len(rows))
			for _, row := range rows {
				ids = append(ids, row.ID)
			}
			updateTotal(len(ids))
			if len(ids) == 0 {
				break
			}
			bulkUsed = true
			updateStart := time.Now()
			updated, err := a.DB.BulkUpdateModeracioSimple(objType, bulkStatus, bulkNotes, user.ID, ids)
			updateDur += time.Since(updateStart)
			if err != nil {
				errCount++
				break
			}
			if updated < len(ids) {
				skipped += len(ids) - updated
			}
			applyActivitiesBulk(objType, ids)
		case "municipi":
			resolveStart = time.Now()
			filter := db.MunicipiFilter{Status: "pendent"}
			if scope, ok := scopeModel.scopeFilterForType("municipi"); ok && !scope.hasGlobal {
				applyScopeFilterToMunicipi(&filter, scope)
			}
			rows, err := a.DB.ListMunicipis(filter)
			resolveDur += time.Since(resolveStart)
			if err != nil {
				errCount++
				break
			}
			updateCandidates(len(rows))
			ids := make([]int, 0, len(rows))
			for _, row := range rows {
				ids = append(ids, row.ID)
			}
			updateTotal(len(ids))
			if len(ids) == 0 {
				break
			}
			bulkUsed = true
			updateStart := time.Now()
			updated, err := a.DB.BulkUpdateModeracioSimple(objType, bulkStatus, bulkNotes, user.ID, ids)
			updateDur += time.Since(updateStart)
			if err != nil {
				errCount++
				break
			}
			if updated < len(ids) {
				skipped += len(ids) - updated
			}
			applyActivitiesBulk(objType, ids)
		case "eclesiastic":
			resolveStart = time.Now()
			filter := db.ArquebisbatFilter{Status: "pendent"}
			if scope, ok := scopeModel.scopeFilterForType("eclesiastic"); ok && !scope.hasGlobal {
				applyScopeFilterToEcles(&filter, scope)
			}
			rows, err := a.DB.ListArquebisbats(filter)
			resolveDur += time.Since(resolveStart)
			if err != nil {
				errCount++
				break
			}
			updateCandidates(len(rows))
			ids := make([]int, 0, len(rows))
			for _, row := range rows {
				ids = append(ids, row.ID)
			}
			updateTotal(len(ids))
			if len(ids) == 0 {
				break
			}
			bulkUsed = true
			updateStart := time.Now()
			updated, err := a.DB.BulkUpdateModeracioSimple(objType, bulkStatus, bulkNotes, user.ID, ids)
			updateDur += time.Since(updateStart)
			if err != nil {
				errCount++
				break
			}
			if updated < len(ids) {
				skipped += len(ids) - updated
			}
			applyActivitiesBulk(objType, ids)
		case "municipi_mapa_version":
			resolveStart = time.Now()
			rows, err := a.DB.ListMunicipiMapaVersions(db.MunicipiMapaVersionFilter{Status: "pendent"})
			resolveDur += time.Since(resolveStart)
			if err != nil {
				errCount++
				break
			}
			ids := make([]int, 0, len(rows))
			for _, row := range rows {
				if scopeModel.canModerateAll || scopeModel.canModerateItem("municipi_mapa_version", row.ID) {
					ids = append(ids, row.ID)
				}
			}
			updateCandidates(len(rows))
			updateTotal(len(ids))
			for _, id := range ids {
				apply(objType, id)
			}
		case "cognom_variant":
			if !scopeModel.canModerateType("cognom_variant") {
				break
			}
			resolveStart = time.Now()
			rows, err := a.DB.ListCognomVariants(db.CognomVariantFilter{Status: "pendent"})
			resolveDur += time.Since(resolveStart)
			if err != nil {
				errCount++
				break
			}
			updateCandidates(len(rows))
			ids := make([]int, 0, len(rows))
			for _, row := range rows {
				ids = append(ids, row.ID)
			}
			updateTotal(len(ids))
			if len(ids) == 0 {
				break
			}
			bulkUsed = true
			updateStart := time.Now()
			updated, err := a.DB.BulkUpdateModeracioSimple(objType, bulkStatus, bulkNotes, user.ID, ids)
			updateDur += time.Since(updateStart)
			if err != nil {
				errCount++
				break
			}
			if updated < len(ids) {
				skipped += len(ids) - updated
			}
			applyActivitiesBulk(objType, ids)
		case "cognom_referencia":
			if !scopeModel.canModerateType("cognom_referencia") {
				break
			}
			resolveStart = time.Now()
			rows, err := a.DB.ListCognomReferencies(db.CognomReferenciaFilter{Status: "pendent"})
			resolveDur += time.Since(resolveStart)
			if err != nil {
				errCount++
				break
			}
			updateCandidates(len(rows))
			ids := make([]int, 0, len(rows))
			for _, row := range rows {
				ids = append(ids, row.ID)
			}
			updateTotal(len(ids))
			if len(ids) == 0 {
				break
			}
			bulkUsed = true
			updateStart := time.Now()
			updated, err := a.DB.BulkUpdateModeracioSimple(objType, bulkStatus, bulkNotes, user.ID, ids)
			updateDur += time.Since(updateStart)
			if err != nil {
				errCount++
				break
			}
			if updated < len(ids) {
				skipped += len(ids) - updated
			}
			applyActivitiesBulk(objType, ids)
		case "cognom_merge":
			if !scopeModel.canModerateType("cognom_merge") {
				break
			}
			resolveStart = time.Now()
			rows, err := a.DB.ListCognomRedirectSuggestions(db.CognomRedirectSuggestionFilter{Status: "pendent"})
			resolveDur += time.Since(resolveStart)
			if err != nil {
				errCount++
				break
			}
			updateCandidates(len(rows))
			updateTotal(len(rows))
			for _, row := range rows {
				apply(objType, row.ID)
			}
		case "municipi_historia_general":
			resolveStart = time.Now()
			rows, _, err := a.DB.ListPendingMunicipiHistoriaGeneralVersions(0, 0)
			resolveDur += time.Since(resolveStart)
			if err != nil {
				errCount++
				break
			}
			updateCandidates(len(rows))
			ids := make([]int, 0, len(rows))
			for _, row := range rows {
				if scopeModel.canModerateAll || scopeModel.canModerateItem("municipi_historia_general", row.ID) {
					ids = append(ids, row.ID)
				}
			}
			updateTotal(len(ids))
			for _, id := range ids {
				apply(objType, id)
			}
		case "municipi_historia_fet":
			resolveStart = time.Now()
			rows, _, err := a.DB.ListPendingMunicipiHistoriaFetVersions(0, 0)
			resolveDur += time.Since(resolveStart)
			if err != nil {
				errCount++
				break
			}
			updateCandidates(len(rows))
			ids := make([]int, 0, len(rows))
			for _, row := range rows {
				if scopeModel.canModerateAll || scopeModel.canModerateItem("municipi_historia_fet", row.ID) {
					ids = append(ids, row.ID)
				}
			}
			updateTotal(len(ids))
			for _, id := range ids {
				apply(objType, id)
			}
		case "municipi_anecdota_version":
			resolveStart = time.Now()
			rows, _, err := a.DB.ListPendingMunicipiAnecdotariVersions(0, 0)
			resolveDur += time.Since(resolveStart)
			if err != nil {
				errCount++
				break
			}
			updateCandidates(len(rows))
			ids := make([]int, 0, len(rows))
			for _, row := range rows {
				if scopeModel.canModerateAll || scopeModel.canModerateItem("municipi_anecdota_version", row.ID) {
					ids = append(ids, row.ID)
				}
			}
			updateTotal(len(ids))
			for _, id := range ids {
				apply(objType, id)
			}
		case "event_historic":
			if !scopeModel.canModerateType("event_historic") {
				break
			}
			resolveStart = time.Now()
			rows, err := a.DB.ListEventsHistoric(db.EventHistoricFilter{Status: "pendent"})
			resolveDur += time.Since(resolveStart)
			if err != nil {
				errCount++
				break
			}
			updateCandidates(len(rows))
			ids := make([]int, 0, len(rows))
			for _, row := range rows {
				ids = append(ids, row.ID)
			}
			updateTotal(len(ids))
			if len(ids) == 0 {
				break
			}
			bulkUsed = true
			updateStart := time.Now()
			updated, err := a.DB.BulkUpdateModeracioSimple(objType, bulkStatus, bulkNotes, user.ID, ids)
			updateDur += time.Since(updateStart)
			if err != nil {
				errCount++
				break
			}
			if updated < len(ids) {
				skipped += len(ids) - updated
			}
			applyActivitiesBulk(objType, ids)
		case "media_album":
			if !scopeModel.canModerateType("media_album") {
				break
			}
			resolveStart = time.Now()
			rows, err := a.DB.ListMediaAlbumsByStatus("pending")
			resolveDur += time.Since(resolveStart)
			if err != nil {
				errCount++
				break
			}
			updateCandidates(len(rows))
			updateTotal(len(rows))
			for _, row := range rows {
				apply(objType, row.ID)
			}
		case "media_item":
			if !scopeModel.canModerateType("media_item") {
				break
			}
			resolveStart = time.Now()
			rows, err := a.DB.ListMediaItemsByStatus("pending")
			resolveDur += time.Since(resolveStart)
			if err != nil {
				errCount++
				break
			}
			updateCandidates(len(rows))
			updateTotal(len(rows))
			for _, row := range rows {
				apply(objType, row.ID)
			}
		case "external_link":
			if !scopeModel.canModerateType("external_link") {
				break
			}
			resolveStart = time.Now()
			rows, err := a.DB.ExternalLinksListByStatus("pending")
			resolveDur += time.Since(resolveStart)
			if err != nil {
				errCount++
				break
			}
			updateCandidates(len(rows))
			updateTotal(len(rows))
			for _, row := range rows {
				apply(objType, row.ID)
			}
		case "municipi_canvi":
			ids := wikiPendingByType["municipi_canvi"]
			updateCandidates(len(ids))
			updateTotal(len(ids))
			for _, id := range ids {
				apply(objType, id)
			}
		case "arxiu_canvi":
			ids := wikiPendingByType["arxiu_canvi"]
			updateCandidates(len(ids))
			updateTotal(len(ids))
			for _, id := range ids {
				apply(objType, id)
			}
		case "llibre_canvi":
			ids := wikiPendingByType["llibre_canvi"]
			updateCandidates(len(ids))
			updateTotal(len(ids))
			for _, id := range ids {
				apply(objType, id)
			}
		case "persona_canvi":
			ids := wikiPendingByType["persona_canvi"]
			updateCandidates(len(ids))
			updateTotal(len(ids))
			for _, id := range ids {
				apply(objType, id)
			}
		case "cognom_canvi":
			ids := wikiPendingByType["cognom_canvi"]
			updateCandidates(len(ids))
			updateTotal(len(ids))
			for _, id := range ids {
				apply(objType, id)
			}
		case "event_historic_canvi":
			ids := wikiPendingByType["event_historic_canvi"]
			updateCandidates(len(ids))
			updateTotal(len(ids))
			for _, id := range ids {
				apply(objType, id)
			}
		case "registre_canvi":
			resolveStart = time.Now()
			rows, err := a.DB.ListTranscripcioRawChangesPending()
			resolveDur += time.Since(resolveStart)
			if err != nil {
				errCount++
				break
			}
			updateCandidates(len(rows))
			ids := make([]int, 0, len(rows))
			for _, row := range rows {
				if scopeModel.canModerateAll || scopeModel.canModerateItem("registre_canvi", row.ID) {
					ids = append(ids, row.ID)
				}
			}
			updateTotal(len(ids))
			for _, id := range ids {
				apply(objType, id)
			}
		case "registre":
			resolveStart = time.Now()
			filter := db.TranscripcioFilter{
				Status: "pendent",
				Limit:  -1,
			}
			if scope, ok := scopeModel.scopeFilterForType("registre"); ok && !scope.hasGlobal {
				applyScopeFilterToRegistre(&filter, scope)
			}
			rows, err := a.DB.ListTranscripcionsRawGlobal(filter)
			resolveDur += time.Since(resolveStart)
			if err != nil {
				errCount++
				break
			}
			ids := make([]int, 0, len(rows))
			for _, row := range rows {
				if bulkUserID > 0 {
					if !row.CreatedBy.Valid || int(row.CreatedBy.Int64) != bulkUserID {
						continue
					}
				}
				ids = append(ids, row.ID)
			}
			updateCandidates(len(rows))
			updateTotal(len(ids))
			if len(ids) == 0 {
				break
			}
			bulkUsed = true
			registreResult := a.applyModeracioBulkRegistreUpdates(action, ids, motiu, user.ID, applyMetrics, nil)
			for _, itemErr := range registreResult.Errors {
				Errorf("Moderacio massiva %s %s:%d ha fallat: %v", action, objType, itemErr.ID, itemErr.Err)
				errCount++
			}
			skipped += registreResult.Skipped
			if len(registreResult.SuccessIDs) > 0 {
				perItemUsed = true
				activityStart := time.Now()
				if err := a.applyModeracioActivitiesBulk(ctx, action, objType, registreResult.SuccessIDs, motiu, user.ID, applyMetrics); err != nil {
					Errorf("Moderacio massiva %s activitats %s bulk ha fallat: %v", action, objType, err)
					errCount++
				}
				if IsDebugEnabled() {
					Debugf("moderacio bulk registre history scope=all ids=%d activity_dur=%s", len(registreResult.SuccessIDs), time.Since(activityStart))
				}
			}
			for range ids {
				processed++
				update(processed, total)
			}
		}
	}
	updateDur += applyMetrics.UpdateDur
	metrics := moderacioBulkMetrics{
		ResolveDur:  resolveDur,
		UpdateDur:   updateDur,
		ActivityDur: applyMetrics.ActivityDur,
		TotalDur:    time.Since(start),
		Mode:        "per-item",
		ScopeMode:   scopeMode,
	}
	if bulkUsed {
		metrics.Mode = "bulk-simple"
		if perItemUsed {
			metrics.Mode = "hybrid"
		}
	}
	result := moderacioBulkResult{Candidates: candidates, Total: total, Processed: processed, Errors: errCount, Skipped: skipped}
	if IsDebugEnabled() {
		Debugf("moderacio bulk bulk_type=%s scope=all candidates=%d total=%d processed=%d errors=%d skipped=%d resolve_dur=%s update_dur=%s activity_dur=%s total_dur=%s mode=%s scope_mode=%s", bulkType, candidates, total, processed, errCount, skipped, metrics.ResolveDur, metrics.UpdateDur, metrics.ActivityDur, metrics.TotalDur, metrics.Mode, metrics.ScopeMode)
	}
	if errCount > 0 {
		return result, metrics, fmt.Errorf("errors: %d", errCount)
	}
	return result, metrics, nil
}

func (a *App) logModeracioBulkExecution(action, scope, bulkType string, userID int, bulkUserID int, async bool, result moderacioBulkResult, metrics moderacioBulkMetrics, auditDur time.Duration) {
	engine := strings.TrimSpace(a.Config["DB_ENGINE"])
	updated := result.Processed
	if result.Skipped > 0 && updated >= result.Skipped {
		updated = result.Processed - result.Skipped
	}
	Infof(
		"Moderacio bulk exec engine=%s mode=%s scope_mode=%s revalidated=%t actor=%d action=%s scope=%s type=%s bulk_user_id=%d candidates=%d targets=%d updated=%d skipped=%d errors=%d resolve_dur=%s update_dur=%s activity_dur=%s audit_dur=%s total_dur=%s async=%t",
		engine,
		metrics.Mode,
		metrics.ScopeMode,
		metrics.Revalidated,
		userID,
		action,
		scope,
		bulkType,
		bulkUserID,
		result.Candidates,
		result.Total,
		updated,
		result.Skipped,
		result.Errors,
		metrics.ResolveDur,
		metrics.UpdateDur,
		metrics.ActivityDur,
		auditDur,
		metrics.TotalDur,
		async,
	)
}

// Accions massives de moderació
func (a *App) AdminModeracioBulk(w http.ResponseWriter, r *http.Request) {
	user, perms, _, ok := a.requireModeracioMassivaUser(w, r)
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/moderacio?err=1", http.StatusSeeOther)
		return
	}
	action := strings.TrimSpace(r.FormValue("bulk_action"))
	if action == "" {
		action = strings.TrimSpace(r.FormValue("action"))
	}
	if !isValidModeracioBulkAction(action) {
		http.Redirect(w, r, "/moderacio?err=1", http.StatusSeeOther)
		return
	}
	scope := strings.TrimSpace(r.FormValue("bulk_scope"))
	if scope == "" {
		scope = "page"
	}
	if !isValidModeracioBulkScope(scope) {
		http.Redirect(w, r, "/moderacio?err=1", http.StatusSeeOther)
		return
	}
	bulkType := strings.TrimSpace(r.FormValue("bulk_type"))
	if bulkType == "" {
		bulkType = "all"
	}
	if !isValidModeracioBulkType(bulkType) {
		http.Redirect(w, r, "/moderacio?err=1", http.StatusSeeOther)
		return
	}
	bulkUserID := 0
	if scope == "all" {
		bulkUserID = parseIntValue(r.FormValue("bulk_user_id"))
	}
	selected := r.Form["selected"]
	motiu := strings.TrimSpace(r.FormValue("bulk_reason"))
	canModerateAll := a.hasPerm(perms, permModerate)
	scopeModel := a.newModeracioScopeModel(user, perms, canModerateAll)
	async := strings.TrimSpace(r.FormValue("async")) == "1" || strings.Contains(r.Header.Get("Accept"), "application/json")
	returnTo := strings.TrimSpace(r.FormValue("return_to"))
	if returnTo == "" {
		returnTo = "/moderacio"
	}
	if scope == "all" {
		if async {
			jobID, err := a.startModeracioBulkAdminJob(action, bulkType, motiu, user, perms, bulkUserID)
			if err != nil {
				Errorf("moderacio bulk job create failed actor=%d action=%s scope=%s type=%s bulk_user_id=%d err=%v", user.ID, action, scope, bulkType, bulkUserID, err)
				http.Error(w, "No s'ha pogut crear el job de moderacio massiva. Torna-ho a provar.", http.StatusInternalServerError)
				return
			}
			a.logAdminAudit(r, user.ID, auditActionModeracioBulk, "moderacio", 0, map[string]interface{}{
				"action":       action,
				"scope":        scope,
				"bulk_type":    bulkType,
				"bulk_user_id": bulkUserID,
				"job_id":       jobID,
				"async":        true,
				"persistent":   true,
				"phase":        adminJobPhaseQueued,
			})
			writeJSON(w, map[string]interface{}{
				"ok":         true,
				"job_id":     fmt.Sprintf("%d", jobID),
				"detail_url": fmt.Sprintf("/admin/jobs/%d", jobID),
			})
			return
		}
		result, metrics, err := a.processModeracioBulkAll(r.Context(), action, bulkType, motiu, user, perms, canModerateAll, bulkUserID, nil)
		auditStart := time.Now()
		a.logAdminAudit(r, user.ID, auditActionModeracioBulk, "moderacio", 0, map[string]interface{}{
			"action":       action,
			"scope":        scope,
			"bulk_type":    bulkType,
			"bulk_user_id": bulkUserID,
			"candidates":   result.Candidates,
			"total":        result.Total,
			"processed":    result.Processed,
			"errors":       result.Errors,
			"skipped":      result.Skipped,
			"async":        false,
		})
		a.logModeracioBulkExecution(action, scope, bulkType, user.ID, bulkUserID, false, result, metrics, time.Since(auditStart))
		if err != nil {
			http.Redirect(w, r, moderacioReturnWithFlag(returnTo, "err"), http.StatusSeeOther)
			return
		}
		http.Redirect(w, r, moderacioReturnWithFlag(returnTo, "ok"), http.StatusSeeOther)
		return
	}
	if len(selected) == 0 {
		http.Redirect(w, r, moderacioReturnWithFlag(returnTo, "err"), http.StatusSeeOther)
		return
	}
	filters, page, perPage := parseModeracioReturnTo(returnTo)
	start := time.Now()
	resolveStart := time.Now()
	pageItems, _, _, err := a.buildModeracioItems(ResolveLang(r), page, perPage, user, perms, canModerateAll, filters, nil)
	if err != nil {
		http.Redirect(w, r, moderacioReturnWithFlag(returnTo, "err"), http.StatusSeeOther)
		return
	}
	resolveDur := time.Since(resolveStart)
	allowed := map[string]moderacioItem{}
	for _, item := range pageItems {
		key := fmt.Sprintf("%s:%d", item.Type, item.ID)
		allowed[key] = item
	}
	candidates := len(selected)
	errCount := 0
	processed := 0
	skipped := 0
	total := 0
	seen := map[string]struct{}{}
	updateStart := time.Now()
	for _, entry := range selected {
		parts := strings.SplitN(entry, ":", 2)
		if len(parts) != 2 {
			errCount++
			skipped++
			continue
		}
		objType := strings.TrimSpace(parts[0])
		id, err := strconv.Atoi(parts[1])
		if err != nil {
			errCount++
			skipped++
			continue
		}
		key := fmt.Sprintf("%s:%d", objType, id)
		if bulkType != "" && bulkType != "all" && objType != bulkType {
			errCount++
			skipped++
			continue
		}
		if _, ok := allowed[key]; !ok {
			errCount++
			skipped++
			continue
		}
		if !canModerateAll && !scopeModel.canModerateItem(objType, id) {
			errCount++
			skipped++
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		total++
		if err := a.applyModeracioAction(r.Context(), action, objType, id, motiu, user.ID, nil); err != nil {
			Errorf("Moderacio massiva %s %s:%d ha fallat: %v", action, objType, id, err)
			errCount++
		}
		processed++
	}
	updateDur := time.Since(updateStart)
	scopeMode := "scoped"
	if canModerateAll {
		scopeMode = "global"
	}
	result := moderacioBulkResult{Candidates: candidates, Total: total, Processed: processed, Errors: errCount, Skipped: skipped}
	metrics := moderacioBulkMetrics{
		ResolveDur:  resolveDur,
		UpdateDur:   updateDur,
		TotalDur:    time.Since(start),
		Mode:        "per-item",
		ScopeMode:   scopeMode,
		Revalidated: true,
	}
	if IsDebugEnabled() {
		Debugf("moderacio bulk bulk_type=%s scope=%s candidates=%d total=%d processed=%d errors=%d skipped=%d resolve_dur=%s update_dur=%s activity_dur=%s total_dur=%s mode=%s scope_mode=%s", bulkType, scope, candidates, total, processed, errCount, skipped, metrics.ResolveDur, metrics.UpdateDur, metrics.ActivityDur, metrics.TotalDur, metrics.Mode, metrics.ScopeMode)
	}
	auditStart := time.Now()
	a.logAdminAudit(r, user.ID, auditActionModeracioBulk, "moderacio", 0, map[string]interface{}{
		"action":     action,
		"scope":      scope,
		"bulk_type":  bulkType,
		"candidates": candidates,
		"total":      total,
		"processed":  processed,
		"errors":     errCount,
		"skipped":    skipped,
		"async":      false,
	})
	a.logModeracioBulkExecution(action, scope, bulkType, user.ID, bulkUserID, false, result, metrics, time.Since(auditStart))
	if errCount > 0 {
		http.Redirect(w, r, moderacioReturnWithFlag(returnTo, "err"), http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, moderacioReturnWithFlag(returnTo, "ok"), http.StatusSeeOther)
}

// Aprovar persona
func (a *App) AdminModeracioAprovar(w http.ResponseWriter, r *http.Request) {
	user, perms, canModerateAll, ok := a.requireModeracioUser(w, r)
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	id := extractID(r.URL.Path)
	objType := strings.TrimSpace(r.FormValue("object_type"))
	if objType == "" {
		objType = "persona"
	}
	_ = r.ParseForm()
	returnTo := strings.TrimSpace(r.FormValue("return_to"))
	if returnTo == "" {
		returnTo = "/moderacio"
	}
	if !canModerateAll && !a.newModeracioScopeModel(user, perms, canModerateAll).canModerateItem(objType, id) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if err := a.updateModeracioObject(objType, id, "publicat", "", user.ID); err != nil {
		Errorf("Moderacio aprovar %s:%d ha fallat: %v", objType, id, err)
		http.Redirect(w, r, moderacioReturnWithFlag(returnTo, "err"), http.StatusSeeOther)
		return
	}
	if acts, err := a.DB.ListActivityByObject(objType, id, "pendent"); err == nil {
		for _, act := range acts {
			_ = a.ValidateActivity(act.ID, user.ID)
		}
	}
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleModeracioApprove, "moderar_aprovar", objType, &id, "validat", nil, "")
	if objType == "event_historic" {
		a.registerEventHistoricModerationActivity(r.Context(), id, "publicat", user.ID, "")
	}
	http.Redirect(w, r, moderacioReturnWithFlag(returnTo, "ok"), http.StatusSeeOther)
}

// Rebutjar persona amb motiu
func (a *App) AdminModeracioRebutjar(w http.ResponseWriter, r *http.Request) {
	user, perms, canModerateAll, ok := a.requireModeracioUser(w, r)
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	id := extractID(r.URL.Path)
	objType := strings.TrimSpace(r.FormValue("object_type"))
	if objType == "" {
		objType = "persona"
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/moderacio?err=1", http.StatusSeeOther)
		return
	}
	returnTo := strings.TrimSpace(r.FormValue("return_to"))
	if returnTo == "" {
		returnTo = "/moderacio"
	}
	if !canModerateAll && !a.newModeracioScopeModel(user, perms, canModerateAll).canModerateItem(objType, id) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	motiu := r.FormValue("motiu")
	if err := a.updateModeracioObject(objType, id, "rebutjat", motiu, user.ID); err != nil {
		Errorf("Moderacio rebutjar %s:%d ha fallat: %v", objType, id, err)
		http.Redirect(w, r, moderacioReturnWithFlag(returnTo, "err"), http.StatusSeeOther)
		return
	}
	if acts, err := a.DB.ListActivityByObject(objType, id, "pendent"); err == nil {
		for _, act := range acts {
			_ = a.DB.UpdateUserActivityStatus(act.ID, "anulat", &user.ID)
		}
	}
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleModeracioReject, "moderar_rebutjar", objType, &id, "validat", nil, motiu)
	if objType == "event_historic" {
		a.registerEventHistoricModerationActivity(r.Context(), id, "rebutjat", user.ID, motiu)
	}
	http.Redirect(w, r, moderacioReturnWithFlag(returnTo, "ok"), http.StatusSeeOther)
}

func (a *App) updateModeracioObject(objectType string, id int, estat, motiu string, moderatorID int) error {
	switch objectType {
	case "persona":
		before, _ := a.DB.GetPersona(id)
		if err := a.DB.UpdatePersonaModeracio(id, estat, motiu, moderatorID); err != nil {
			return err
		}
		if estat == "publicat" {
			if err := a.upsertSearchDocForPersonaID(id); err != nil {
				Errorf("SearchIndex persona %d: %v", id, err)
			}
		} else if before != nil && before.ModeracioEstat == "publicat" {
			if err := a.DB.DeleteSearchDoc("persona", id); err != nil {
				Errorf("SearchIndex delete persona %d: %v", id, err)
			}
		}
		return nil
	case "arxiu":
		return a.DB.UpdateArxiuModeracio(id, estat, motiu, moderatorID)
	case "llibre":
		return a.DB.UpdateLlibreModeracio(id, estat, motiu, moderatorID)
	case "municipi":
		return a.DB.UpdateMunicipiModeracio(id, estat, motiu, moderatorID)
	case "nivell":
		return a.DB.UpdateNivellModeracio(id, estat, motiu, moderatorID)
	case "eclesiastic":
		return a.DB.UpdateArquebisbatModeracio(id, estat, motiu, moderatorID)
	case "registre":
		action := ""
		switch estat {
		case "publicat":
			action = "approve"
		case "rebutjat":
			action = "reject"
		default:
			return fmt.Errorf("estat de moderació invàlid")
		}
		res := a.applyModeracioBulkRegistreUpdates(action, []int{id}, motiu, moderatorID, nil, nil)
		if len(res.Errors) > 0 {
			return res.Errors[0].Err
		}
		return nil
	case "registre_canvi":
		return a.moderateRegistreChange(id, estat, motiu, moderatorID)
	case "cognom_variant":
		return a.DB.UpdateCognomVariantModeracio(id, estat, motiu, moderatorID)
	case "cognom_referencia":
		return a.DB.UpdateCognomReferenciaModeracio(id, estat, motiu, moderatorID)
	case "cognom_merge":
		return a.moderateCognomMergeSuggestion(id, estat, motiu, moderatorID)
	case "event_historic":
		return a.DB.UpdateEventHistoricModeracio(id, estat, motiu, moderatorID)
	case "municipi_canvi":
		return a.moderateWikiChange(id, "municipi", estat, motiu, moderatorID)
	case "arxiu_canvi":
		return a.moderateWikiChange(id, "arxiu", estat, motiu, moderatorID)
	case "llibre_canvi":
		return a.moderateWikiChange(id, "llibre", estat, motiu, moderatorID)
	case "persona_canvi":
		return a.moderateWikiChange(id, "persona", estat, motiu, moderatorID)
	case "cognom_canvi":
		return a.moderateWikiChange(id, "cognom", estat, motiu, moderatorID)
	case "event_historic_canvi":
		return a.moderateWikiChange(id, "event_historic", estat, motiu, moderatorID)
	case "municipi_historia_general":
		return a.DB.SetMunicipiHistoriaGeneralStatus(id, estat, motiu, &moderatorID)
	case "municipi_historia_fet":
		return a.DB.SetMunicipiHistoriaFetStatus(id, estat, motiu, &moderatorID)
	case "municipi_anecdota_version":
		if estat == "publicat" {
			if err := a.DB.ApproveMunicipiAnecdotariVersion(id, moderatorID); err != nil {
				return err
			}
			if version, err := a.DB.GetMunicipiAnecdotariVersion(id); err == nil && version != nil {
				Infof("Anecdota aprovada version=%d item=%d municipi=%d moderator=%d", id, version.ItemID, version.MunicipiID, moderatorID)
			} else {
				Infof("Anecdota aprovada version=%d moderator=%d", id, moderatorID)
			}
			return nil
		}
		if estat == "rebutjat" {
			if err := a.DB.RejectMunicipiAnecdotariVersion(id, moderatorID, motiu); err != nil {
				return err
			}
			if version, err := a.DB.GetMunicipiAnecdotariVersion(id); err == nil && version != nil {
				Infof("Anecdota rebutjada version=%d item=%d municipi=%d moderator=%d", id, version.ItemID, version.MunicipiID, moderatorID)
			} else {
				Infof("Anecdota rebutjada version=%d moderator=%d", id, moderatorID)
			}
			return nil
		}
		return fmt.Errorf("estat desconegut")
	case "municipi_mapa_version":
		return a.moderateMapaVersion(id, estat, motiu, moderatorID)
	case "media_album":
		return a.moderateMediaAlbum(id, estat, motiu, moderatorID)
	case "media_item":
		return a.moderateMediaItem(id, estat, motiu, moderatorID)
	case "external_link":
		status := externalLinkStatusFromModeracio(estat)
		if status == "" {
			return fmt.Errorf("estat desconegut")
		}
		return a.DB.ExternalLinkModerate(id, status)
	default:
		return fmt.Errorf("tipus desconegut")
	}
}

func (a *App) moderateMapaVersion(id int, estat, notes string, moderatorID int) error {
	version, err := a.DB.GetMunicipiMapaVersion(id)
	if err != nil {
		return err
	}
	if version == nil {
		return fmt.Errorf("mapa version no trobat")
	}
	if err := a.DB.UpdateMunicipiMapaVersionStatus(id, estat, notes, moderatorID); err != nil {
		return err
	}
	if estat == "publicat" {
		if err := a.DB.UpdateMunicipiMapaCurrentVersion(version.MapaID, id); err != nil {
			return err
		}
		if acts, err := a.DB.ListActivityByObject(mapModerationObjectType, id, "pendent"); err == nil {
			for _, act := range acts {
				_ = a.ValidateActivity(act.ID, moderatorID)
			}
		}
	}
	return nil
}

func (a *App) moderateMediaAlbum(id int, estat, notes string, moderatorID int) error {
	album, err := a.DB.GetMediaAlbumByID(id)
	if err != nil {
		return err
	}
	if album == nil {
		return fmt.Errorf("media album no trobat")
	}
	status := mediaStatusFromModeracio(estat)
	if status == "" {
		return fmt.Errorf("estat desconegut")
	}
	wasApproved := album.ModerationStatus == "approved"
	restrictedGroupID := 0
	if album.RestrictedGroupID.Valid {
		restrictedGroupID = int(album.RestrictedGroupID.Int64)
	}
	accessPolicyID := 0
	if album.AccessPolicyID.Valid {
		accessPolicyID = int(album.AccessPolicyID.Int64)
	}
	if err := a.DB.UpdateMediaAlbumModeration(id, status, album.Visibility, restrictedGroupID, accessPolicyID, album.CreditCost, album.DifficultyScore, album.SourceType, notes, moderatorID); err != nil {
		return err
	}
	if status == "approved" && !wasApproved && album.OwnerUserID > 0 {
		points := a.mediaPointsForDifficulty(album.DifficultyScore)
		if points > 0 {
			details := fmt.Sprintf("source=%s difficulty=%d", album.SourceType, album.DifficultyScore)
			if err := a.recordUserPoints(album.OwnerUserID, points, "media_approve", "media_album", album.ID, &moderatorID, details); err != nil {
				Errorf("Error afegint punts album media %d: %v", album.ID, err)
			}
		}
	}
	return nil
}

func (a *App) moderateMediaItem(id int, estat, notes string, moderatorID int) error {
	item, err := a.DB.GetMediaItemByID(id)
	if err != nil {
		return err
	}
	if item == nil {
		return fmt.Errorf("media item no trobat")
	}
	status := mediaStatusFromModeracio(estat)
	if status == "" {
		return fmt.Errorf("estat desconegut")
	}
	if err := a.DB.UpdateMediaItemModeration(id, status, item.CreditCost, notes, moderatorID); err != nil {
		return err
	}
	return nil
}

func (a *App) moderateCognomMergeSuggestion(id int, estat, motiu string, moderatorID int) error {
	sugg, err := a.DB.GetCognomRedirectSuggestion(id)
	if err != nil {
		return err
	}
	if sugg == nil {
		return fmt.Errorf("merge suggestion not found")
	}
	if estat == "publicat" {
		toID := sugg.ToCognomID
		if canonID, _, err := a.resolveCognomCanonicalID(toID); err == nil && canonID > 0 {
			toID = canonID
		}
		if sugg.FromCognomID != toID {
			var createdBy *int
			if sugg.CreatedBy.Valid {
				val := int(sugg.CreatedBy.Int64)
				createdBy = &val
			}
			if err := a.DB.SetCognomRedirect(sugg.FromCognomID, toID, createdBy, sugg.Reason); err != nil {
				return err
			}
		}
	}
	return a.DB.UpdateCognomRedirectSuggestionModeracio(id, estat, motiu, moderatorID)
}

func (a *App) registerEventHistoricModerationActivity(ctx context.Context, eventID int, status string, moderatorID int, reason string) {
	if eventID <= 0 {
		return
	}
	event, err := a.DB.GetEventHistoric(eventID)
	if err != nil || event == nil || !event.CreatedBy.Valid {
		return
	}
	authorID := int(event.CreatedBy.Int64)
	switch status {
	case "publicat":
		_, _ = a.RegisterUserActivity(ctx, authorID, ruleEventHistoricApprove, "event_historic_approve", "event_historic", &eventID, "validat", &moderatorID, "")
	case "rebutjat":
		_, _ = a.RegisterUserActivity(ctx, authorID, ruleEventHistoricReject, "event_historic_reject", "event_historic", &eventID, "validat", &moderatorID, reason)
	}
}

func (a *App) moderateRegistreChange(changeID int, estat, motiu string, moderatorID int) error {
	change, err := a.DB.GetTranscripcioRawChange(changeID)
	if err != nil {
		return err
	}
	if change == nil {
		return fmt.Errorf("canvi no trobat")
	}
	if err := a.DB.UpdateTranscripcioRawChangeModeracio(changeID, estat, motiu, moderatorID); err != nil {
		return err
	}
	if estat != "publicat" {
		a.updateRegistreChangeActivities(change.TranscripcioID, changeID, moderatorID, false)
		return nil
	}
	registre, err := a.DB.GetTranscripcioRaw(change.TranscripcioID)
	if err != nil || registre == nil {
		return fmt.Errorf("registre no trobat")
	}
	beforePersones, _ := a.DB.ListTranscripcioPersones(registre.ID)
	_, afterSnap := parseTranscripcioChangeMeta(*change)
	if afterSnap == nil {
		return fmt.Errorf("canvi sense dades")
	}
	after := *afterSnap
	after.Persones = append([]db.TranscripcioPersonaRaw(nil), afterSnap.Persones...)
	after.Atributs = append([]db.TranscripcioAtributRaw(nil), afterSnap.Atributs...)
	after.Raw.ID = registre.ID
	after.Raw.ModeracioEstat = "publicat"
	after.Raw.ModeratedBy = sqlNullIntFromInt(moderatorID)
	after.Raw.ModeratedAt = sql.NullTime{Time: time.Now(), Valid: true}
	after.Raw.ModeracioMotiu = motiu
	if !after.Raw.CreatedBy.Valid {
		after.Raw.CreatedBy = registre.CreatedBy
	}
	if err := a.DB.UpdateTranscripcioRaw(&after.Raw); err != nil {
		return err
	}
	_ = a.DB.DeleteTranscripcioPersones(registre.ID)
	for i := range after.Persones {
		after.Persones[i].TranscripcioID = registre.ID
		_, _ = a.DB.CreateTranscripcioPersona(&after.Persones[i])
	}
	_ = a.DB.DeleteTranscripcioAtributs(registre.ID)
	for i := range after.Atributs {
		after.Atributs[i].TranscripcioID = registre.ID
		_, _ = a.DB.CreateTranscripcioAtribut(&after.Atributs[i])
	}
	if registre.ModeracioEstat == "publicat" {
		a.applyDemografiaDeltaForRegistre(registre, -1)
	}
	if after.Raw.ModeracioEstat == "publicat" {
		a.applyDemografiaDeltaForRegistre(&after.Raw, 1)
	}
	if registre.ModeracioEstat == "publicat" {
		a.applyNomCognomDeltaForRegistre(registre, beforePersones, -1)
	}
	if after.Raw.ModeracioEstat == "publicat" {
		a.applyNomCognomDeltaForRegistre(&after.Raw, after.Persones, 1)
	}
	a.updateRegistreChangeActivities(change.TranscripcioID, changeID, moderatorID, true)
	if change.ChangeType == "revert" {
		if srcID := parseRevertSourceChangeID(change.Metadata); srcID > 0 {
			if srcChange, err := a.DB.GetTranscripcioRawChange(srcID); err == nil && srcChange != nil && srcChange.ChangedBy.Valid {
				changedByID := int(srcChange.ChangedBy.Int64)
				if acts, err := a.DB.ListActivityByObject("registre", change.TranscripcioID, "validat"); err == nil {
					for _, act := range acts {
						if act.UserID != changedByID || act.Action != "editar_registre" {
							continue
						}
						_ = a.DB.UpdateUserActivityStatus(act.ID, "anulat", &moderatorID)
						if act.Points != 0 {
							_ = a.DB.AddPointsToUser(act.UserID, -act.Points)
						}
						break
					}
				}
			}
		}
	}
	if err := a.upsertSearchDocForRegistreID(registre.ID); err != nil {
		Errorf("SearchIndex registre %d: %v", registre.ID, err)
	}
	_, _ = a.recalcLlibreIndexacioStats(registre.LlibreID)
	return nil
}

func (a *App) moderateWikiChange(changeID int, objectType string, estat, motiu string, moderatorID int) error {
	change, err := a.DB.GetWikiChange(changeID)
	if err != nil {
		return err
	}
	if change == nil {
		return fmt.Errorf("canvi no trobat")
	}
	if objectType != "" && change.ObjectType != objectType {
		return fmt.Errorf("tipus de canvi no coincideix")
	}
	if err := a.DB.UpdateWikiChangeModeracio(changeID, estat, motiu, moderatorID); err != nil {
		Errorf("WikiChangeModeracio failed change_id=%d object=%s object_id=%d err=%v", changeID, change.ObjectType, change.ObjectID, err)
		return err
	}
	if estat != "publicat" {
		return nil
	}
	if !isValidWikiObjectType(change.ObjectType) {
		return fmt.Errorf("tipus desconegut")
	}
	switch change.ObjectType {
	case "municipi":
		return a.applyWikiMunicipiChange(change, motiu, moderatorID)
	case "arxiu":
		return a.applyWikiArxiuChange(change, motiu, moderatorID)
	case "llibre":
		return a.applyWikiLlibreChange(change, motiu, moderatorID)
	case "persona":
		return a.applyWikiPersonaChange(change, motiu, moderatorID)
	case "cognom":
		return a.applyWikiCognomChange(change, motiu, moderatorID)
	case "event_historic":
		return a.applyWikiEventHistoricChange(change, motiu, moderatorID)
	default:
		return fmt.Errorf("tipus desconegut")
	}
}

func (a *App) updateRegistreChangeActivities(registreID, changeID, moderatorID int, validate bool) {
	acts, err := a.DB.ListActivityByObject("registre", registreID, "pendent")
	if err != nil {
		return
	}
	detailKey := fmt.Sprintf("change:%d", changeID)
	for _, act := range acts {
		if act.Details != "" && act.Details != detailKey {
			continue
		}
		if validate {
			_ = a.ValidateActivity(act.ID, moderatorID)
		} else {
			_ = a.CancelActivity(act.ID, moderatorID)
		}
	}
}

func parseRevertSourceChangeID(payload string) int {
	if strings.TrimSpace(payload) == "" {
		return 0
	}
	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(payload), &raw); err != nil {
		return 0
	}
	revertRaw, ok := raw["revert"]
	if !ok {
		return 0
	}
	revertMap, ok := revertRaw.(map[string]interface{})
	if !ok {
		return 0
	}
	val, ok := revertMap["source_change_id"]
	if !ok {
		return 0
	}
	switch v := val.(type) {
	case float64:
		return int(v)
	case int:
		return v
	case string:
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return 0
}
