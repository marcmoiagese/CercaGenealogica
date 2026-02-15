package core

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const (
	auditActionUserActivate            = "user_activate"
	auditActionUserDeactivate          = "user_deactivate"
	auditActionUserBan                 = "user_ban"
	auditActionUserUnban               = "user_unban"
	auditActionSessionsRevoke          = "sessions_revoke"
	auditActionNivellsRebuild          = "nivells_rebuild"
	auditActionAdminImport             = "admin_import"
	auditActionJobRetry                = "job_retry"
	auditActionMaintenanceSave         = "maintenance_save"
	auditActionMaintenanceDelete       = "maintenance_delete"
	auditActionPlatformUpdate          = "platform_update"
	auditActionTransparencyUpdate      = "transparency_update"
	auditActionTransparencyContributor = "transparency_contributor"
)

type adminAuditView struct {
	ID          int
	Actor       string
	ActionLabel string
	ObjectLabel string
	Metadata    string
	IP          string
	CreatedAt   string
}

type adminSessionView struct {
	ID         int
	UserID     int
	UserLabel  string
	CreatedAt  string
	ExpiresAt  string
	LastAccess string
}

type auditOption struct {
	Value string
	Label string
}

func (a *App) logAdminAudit(r *http.Request, actorID int, action string, objectType string, objectID int, metadata map[string]interface{}) {
	if a == nil || a.DB == nil {
		return
	}
	action = strings.TrimSpace(action)
	if action == "" {
		return
	}
	entry := &db.AdminAuditEntry{
		Action:     action,
		ObjectType: strings.TrimSpace(objectType),
		IP:         strings.TrimSpace(getIPSafe(r)),
	}
	if actorID > 0 {
		entry.ActorID = sqlNullIntFromInt(actorID)
	}
	if objectID > 0 {
		entry.ObjectID = sqlNullIntFromInt(objectID)
	}
	if len(metadata) > 0 {
		if b, err := json.Marshal(metadata); err == nil {
			entry.MetadataJSON = string(b)
		}
	}
	if _, err := a.DB.InsertAdminAudit(entry); err != nil {
		Errorf("Admin audit insert failed: %v", err)
	}
}

func getIPSafe(r *http.Request) string {
	if r == nil {
		return ""
	}
	return getIP(r)
}

func (a *App) AdminAuditPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user, _, ok := a.requirePermission(w, r, permAdmin)
	if !ok {
		return
	}
	lang := ResolveLang(r)
	filterAction := strings.TrimSpace(r.URL.Query().Get("action"))
	filterActor := strings.TrimSpace(r.URL.Query().Get("actor"))
	filterObj := strings.TrimSpace(r.URL.Query().Get("object"))
	actorID := 0
	if filterActor != "" {
		if id, err := strconv.Atoi(filterActor); err == nil {
			actorID = id
		}
	}
	perPage := parseListPerPage(r.URL.Query().Get("per_page"))
	page := parseListPage(r.URL.Query().Get("page"))
	filter := db.AdminAuditFilter{
		Action:     filterAction,
		ActorID:    actorID,
		ObjectType: filterObj,
	}
	total, err := a.DB.CountAdminAudit(filter)
	if err != nil {
		http.Error(w, "failed to count", http.StatusInternalServerError)
		return
	}
	if perPage <= 0 {
		perPage = 25
	}
	totalPages := 1
	if total > 0 {
		totalPages = (total + perPage - 1) / perPage
	}
	if page > totalPages {
		page = totalPages
	}
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * perPage
	filter.Limit = perPage
	filter.Offset = offset
	rows, err := a.DB.ListAdminAudit(filter)
	if err != nil {
		http.Error(w, "failed to list", http.StatusInternalServerError)
		return
	}
	userCache := map[int]string{}
	views := buildAdminAuditViews(a, lang, rows, userCache)
	pageValues := cloneValues(r.URL.Query())
	pageValues.Del("page")
	pageValues.Set("per_page", strconv.Itoa(perPage))
	if filterAction != "" {
		pageValues.Set("action", filterAction)
	}
	if filterObj != "" {
		pageValues.Set("object", filterObj)
	}
	if actorID > 0 {
		pageValues.Set("actor", strconv.Itoa(actorID))
	}
	pageBase := "/admin/auditoria"
	if encoded := pageValues.Encode(); encoded != "" {
		pageBase += "?" + encoded
	}
	token, _ := ensureCSRF(w, r)
	sessionFilter := buildSessionFilter(r)
	sessions, sessionsTotal, sessionsTotalPages, sessionsPage := a.loadAdminSessions(sessionFilter)
	sessionsPerPage := sessionFilter.Limit
	if sessionsPerPage <= 0 {
		sessionsPerPage = 10
	}
	RenderPrivateTemplate(w, r, "admin-auditoria.html", map[string]interface{}{
		"User":               user,
		"AuditRows":          views,
		"AuditTotal":         total,
		"AuditPage":          page,
		"AuditPerPage":       perPage,
		"AuditTotalPages":    totalPages,
		"AuditHasPrev":       page > 1,
		"AuditHasNext":       page < totalPages,
		"AuditPrevPage":      page - 1,
		"AuditNextPage":      page + 1,
		"AuditPageBase":      pageBase,
		"FilterAction":       filterAction,
		"FilterObject":       filterObj,
		"FilterActor":        filterActor,
		"ActionOptions":      adminAuditActionOptions(lang),
		"ObjectOptions":      adminAuditObjectOptions(lang),
		"Sessions":           sessions,
		"SessionsTotal":      sessionsTotal,
		"SessionsPage":       sessionsPage,
		"SessionsTotalPages": sessionsTotalPages,
		"SessionsPerPage":    sessionsPerPage,
		"SessionsFilterUser": r.URL.Query().Get("session_user"),
		"CSRFToken":          token,
	})
}

func (a *App) AdminAuditAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	if _, _, ok := a.requirePermission(w, r, permAdmin); !ok {
		return
	}
	filter := db.AdminAuditFilter{
		Action:     strings.TrimSpace(r.URL.Query().Get("action")),
		ObjectType: strings.TrimSpace(r.URL.Query().Get("object")),
	}
	if actorStr := strings.TrimSpace(r.URL.Query().Get("actor")); actorStr != "" {
		if actorID, err := strconv.Atoi(actorStr); err == nil {
			filter.ActorID = actorID
		}
	}
	perPage := parseListPerPage(r.URL.Query().Get("per_page"))
	page := parseListPage(r.URL.Query().Get("page"))
	total, err := a.DB.CountAdminAudit(filter)
	if err != nil {
		http.Error(w, "failed to count", http.StatusInternalServerError)
		return
	}
	if perPage <= 0 {
		perPage = 25
	}
	totalPages := 1
	if total > 0 {
		totalPages = (total + perPage - 1) / perPage
	}
	if page > totalPages {
		page = totalPages
	}
	if page < 1 {
		page = 1
	}
	filter.Limit = perPage
	filter.Offset = (page - 1) * perPage
	rows, err := a.DB.ListAdminAudit(filter)
	if err != nil {
		http.Error(w, "failed to list", http.StatusInternalServerError)
		return
	}
	items := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		item := map[string]interface{}{
			"id":          row.ID,
			"action":      strings.TrimSpace(row.Action),
			"object_type": strings.TrimSpace(row.ObjectType),
			"ip":          strings.TrimSpace(row.IP),
			"metadata":    strings.TrimSpace(row.MetadataJSON),
			"created_at":  formatAuditTimeISO(row.CreatedAt),
		}
		if row.ActorID.Valid {
			item["actor_id"] = int(row.ActorID.Int64)
		}
		if row.ObjectID.Valid {
			item["object_id"] = int(row.ObjectID.Int64)
		}
		items = append(items, item)
	}
	writeJSON(w, map[string]interface{}{
		"ok":         true,
		"rows":       items,
		"page":       page,
		"per_page":   perPage,
		"total":      total,
		"totalPages": totalPages,
	})
}

func (a *App) AdminRevokeUserSessions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	user, _, ok := a.requirePermission(w, r, permAdmin)
	if !ok {
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Formulari invalid", http.StatusBadRequest)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	userID, _ := strconv.Atoi(strings.TrimSpace(r.FormValue("user_id")))
	if userID <= 0 {
		http.Redirect(w, r, safeReturnTo(r.FormValue("return_to"), "/admin/auditoria?err=1"), http.StatusSeeOther)
		return
	}
	if err := a.DB.RevokeUserSessions(userID); err != nil {
		http.Redirect(w, r, safeReturnTo(r.FormValue("return_to"), "/admin/auditoria?err=1"), http.StatusSeeOther)
		return
	}
	a.logAdminAudit(r, user.ID, auditActionSessionsRevoke, "user", userID, nil)
	http.Redirect(w, r, safeReturnTo(r.FormValue("return_to"), "/admin/auditoria?ok=1"), http.StatusSeeOther)
}

func buildSessionFilter(r *http.Request) db.AdminSessionFilter {
	filter := db.AdminSessionFilter{
		ActiveOnly: true,
	}
	if r == nil {
		return filter
	}
	if userStr := strings.TrimSpace(r.URL.Query().Get("session_user")); userStr != "" {
		if id, err := strconv.Atoi(userStr); err == nil {
			filter.UserID = id
		}
	}
	perPageParam := strings.TrimSpace(r.URL.Query().Get("session_per_page"))
	perPage := parseListPerPage(perPageParam)
	if perPageParam == "" {
		perPage = 10
	}
	page := parseListPage(r.URL.Query().Get("session_page"))
	offset := (page - 1) * perPage
	if offset < 0 {
		offset = 0
	}
	filter.Limit = perPage
	filter.Offset = offset
	return filter
}

func (a *App) loadAdminSessions(filter db.AdminSessionFilter) ([]adminSessionView, int, int, int) {
	if a == nil || a.DB == nil {
		return nil, 0, 1, 1
	}
	total, err := a.DB.CountAdminSessions(filter)
	if err != nil {
		return nil, 0, 1, 1
	}
	perPage := filter.Limit
	if perPage <= 0 {
		perPage = 10
	}
	totalPages := 1
	if total > 0 {
		totalPages = (total + perPage - 1) / perPage
	}
	page := 1
	if filter.Offset > 0 && perPage > 0 {
		page = (filter.Offset / perPage) + 1
	}
	rows, err := a.DB.ListAdminSessions(filter)
	if err != nil {
		return nil, total, totalPages, page
	}
	views := make([]adminSessionView, 0, len(rows))
	for _, row := range rows {
		label := resolveUserLabel(row.Username, row.Nom, row.Cognoms)
		views = append(views, adminSessionView{
			ID:         row.ID,
			UserID:     row.UserID,
			UserLabel:  label,
			CreatedAt:  formatAuditTime(row.CreatedAt),
			ExpiresAt:  formatAuditTime(row.ExpiresAt),
			LastAccess: formatAuditTime(row.LastAccessAt),
		})
	}
	return views, total, totalPages, page
}

func resolveUserLabel(username, nom, cognoms string) string {
	name := strings.TrimSpace(username)
	if name != "" {
		return name
	}
	full := strings.TrimSpace(strings.TrimSpace(nom) + " " + strings.TrimSpace(cognoms))
	return strings.TrimSpace(full)
}

func buildAdminAuditViews(a *App, lang string, rows []db.AdminAuditEntry, userCache map[int]string) []adminAuditView {
	views := make([]adminAuditView, 0, len(rows))
	for _, row := range rows {
		actorLabel := resolveAuditUser(a, row.ActorID, userCache)
		actionLabel := translateWithFallback(lang, "admin.audit.action."+strings.TrimSpace(row.Action), strings.TrimSpace(row.Action))
		objectLabel := translateWithFallback(lang, "admin.audit.object."+strings.TrimSpace(row.ObjectType), strings.TrimSpace(row.ObjectType))
		if row.ObjectID.Valid {
			if objectLabel == "" {
				objectLabel = "#" + strconv.Itoa(int(row.ObjectID.Int64))
			} else {
				objectLabel = objectLabel + " #" + strconv.Itoa(int(row.ObjectID.Int64))
			}
		}
		meta := formatAuditMetadata(row.MetadataJSON)
		views = append(views, adminAuditView{
			ID:          row.ID,
			Actor:       actorLabel,
			ActionLabel: actionLabel,
			ObjectLabel: objectLabel,
			Metadata:    meta,
			IP:          strings.TrimSpace(row.IP),
			CreatedAt:   formatAuditTime(row.CreatedAt),
		})
	}
	return views
}

func resolveAuditUser(a *App, id sql.NullInt64, cache map[int]string) string {
	if !id.Valid {
		return ""
	}
	uid := int(id.Int64)
	if name, ok := cache[uid]; ok {
		return name
	}
	user, err := a.DB.GetUserByID(uid)
	if err != nil || user == nil {
		cache[uid] = ""
		return ""
	}
	name := strings.TrimSpace(user.Usuari)
	if name == "" {
		name = strings.TrimSpace(strings.TrimSpace(user.Name) + " " + strings.TrimSpace(user.Surname))
	}
	cache[uid] = name
	return name
}

func translateWithFallback(lang, key, fallback string) string {
	if key == "" {
		return fallback
	}
	val := T(lang, key)
	if val == key {
		return fallback
	}
	return val
}

func formatAuditMetadata(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	var out bytes.Buffer
	if err := json.Indent(&out, []byte(raw), "", "  "); err != nil {
		return raw
	}
	return out.String()
}

func formatAuditTime(val sql.NullTime) string {
	if !val.Valid {
		return ""
	}
	return val.Time.Format("2006-01-02 15:04")
}

func formatAuditTimeISO(val sql.NullTime) string {
	if !val.Valid {
		return ""
	}
	return val.Time.Format(time.RFC3339)
}

func adminAuditActionOptions(lang string) []auditOption {
	return []auditOption{
		{Value: auditActionUserActivate, Label: T(lang, "admin.audit.action.user_activate")},
		{Value: auditActionUserDeactivate, Label: T(lang, "admin.audit.action.user_deactivate")},
		{Value: auditActionUserBan, Label: T(lang, "admin.audit.action.user_ban")},
		{Value: auditActionUserUnban, Label: T(lang, "admin.audit.action.user_unban")},
		{Value: auditActionSessionsRevoke, Label: T(lang, "admin.audit.action.sessions_revoke")},
		{Value: auditActionNivellsRebuild, Label: T(lang, "admin.audit.action.nivells_rebuild")},
		{Value: auditActionAdminImport, Label: T(lang, "admin.audit.action.admin_import")},
		{Value: auditActionJobRetry, Label: T(lang, "admin.audit.action.job_retry")},
		{Value: auditActionMaintenanceSave, Label: T(lang, "admin.audit.action.maintenance_save")},
		{Value: auditActionMaintenanceDelete, Label: T(lang, "admin.audit.action.maintenance_delete")},
		{Value: auditActionPlatformUpdate, Label: T(lang, "admin.audit.action.platform_update")},
		{Value: auditActionTransparencyUpdate, Label: T(lang, "admin.audit.action.transparency_update")},
		{Value: auditActionTransparencyContributor, Label: T(lang, "admin.audit.action.transparency_contributor")},
	}
}

func adminAuditObjectOptions(lang string) []auditOption {
	return []auditOption{
		{Value: "user", Label: T(lang, "admin.audit.object.user")},
		{Value: "nivell", Label: T(lang, "admin.audit.object.nivell")},
		{Value: "maintenance", Label: T(lang, "admin.audit.object.maintenance")},
		{Value: "import", Label: T(lang, "admin.audit.object.import")},
		{Value: "job", Label: T(lang, "admin.audit.object.job")},
		{Value: "platform", Label: T(lang, "admin.audit.object.platform")},
		{Value: "transparency", Label: T(lang, "admin.audit.object.transparency")},
	}
}
