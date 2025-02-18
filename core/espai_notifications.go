package core

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const (
	espaiNotifKindMatches       = "matches_pending"
	espaiNotifKindGrampsError   = "gramps_error"
	espaiNotifKindGroupConflict = "group_conflicts"
)

var espaiNotifTypes = []string{"matches", "gramps", "groups"}

var espaiNotifKindToType = map[string]string{
	espaiNotifKindMatches:       "matches",
	espaiNotifKindGrampsError:   "gramps",
	espaiNotifKindGroupConflict: "groups",
}

type espaiNotificationView struct {
	ID        int
	Kind      string
	Title     string
	Body      string
	URL       string
	Status    string
	Icon      string
	CreatedAt sql.NullTime
}

type espaiNotificationPrefsView struct {
	Freq          string
	Types         map[string]bool
	HasCustomTypes bool
}

type espaiOverviewCounts struct {
	PendingMatches   int
	SyncFailures     int
	GroupConflicts   int
	UnreadAlerts     int
}

func (a *App) EspaiNotificationsRead(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, T(ResolveLang(r), "error.csrf"), http.StatusBadRequest)
		return
	}
	notifyID := parseFormInt(r.FormValue("id"))
	if notifyID > 0 {
		_ = a.DB.MarkEspaiNotificationRead(notifyID, user.ID)
	}
	http.Redirect(w, r, "/espai?notice="+urlQueryEscape(T(ResolveLang(r), "space.notifications.notice.read")), http.StatusSeeOther)
}

func (a *App) EspaiNotificationsReadAll(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, T(ResolveLang(r), "error.csrf"), http.StatusBadRequest)
		return
	}
	_ = a.DB.MarkEspaiNotificationsReadAll(user.ID)
	http.Redirect(w, r, "/espai?notice="+urlQueryEscape(T(ResolveLang(r), "space.notifications.notice.read_all")), http.StatusSeeOther)
}

func (a *App) EspaiNotificationsPrefs(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, T(ResolveLang(r), "error.csrf"), http.StatusBadRequest)
		return
	}
	freq := strings.TrimSpace(r.FormValue("freq"))
	if !isValidEspaiNotifFreq(freq) {
		freq = "instant"
	}
	types := r.Form["types"]
	typesJSON, _ := json.Marshal(filterEspaiNotifTypes(types))
	pref := &db.EspaiNotificationPref{
		UserID:    user.ID,
		Freq:      freq,
		TypesJSON: sqlNullString(string(typesJSON)),
	}
	if err := a.DB.UpsertEspaiNotificationPref(pref); err != nil {
		http.Redirect(w, r, "/espai?error="+urlQueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/espai?notice="+urlQueryEscape(T(ResolveLang(r), "space.notifications.notice.saved")), http.StatusSeeOther)
}

func (a *App) loadEspaiNotificationPrefs(userID int) espaiNotificationPrefsView {
	view := espaiNotificationPrefsView{
		Freq:  "instant",
		Types: map[string]bool{},
	}
	for _, t := range espaiNotifTypes {
		view.Types[t] = true
	}
	pref, err := a.DB.GetEspaiNotificationPref(userID)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return view
		}
		return view
	}
	view.Freq = strings.TrimSpace(pref.Freq)
	if view.Freq == "" {
		view.Freq = "instant"
	}
	customTypes := parseEspaiNotifTypes(pref.TypesJSON)
	if pref.TypesJSON.Valid {
		view.HasCustomTypes = true
		if customTypes != nil {
			view.Types = customTypes
		} else {
			view.Types = map[string]bool{}
		}
	}
	return view
}

func (a *App) listEspaiNotificationViews(userID int, lang string, limit int) ([]espaiNotificationView, int) {
	rows, _ := a.DB.ListEspaiNotificationsByUser(userID, "", limit)
	views := make([]espaiNotificationView, 0, len(rows))
	unread := 0
	for _, n := range rows {
		title := strings.TrimSpace(n.Title.String)
		body := strings.TrimSpace(n.Body.String)
		if title == "" {
			title = T(lang, "space.notifications.kind."+n.Kind)
		}
		if body == "" {
			body = T(lang, "space.notifications.kind."+n.Kind+".body")
		}
		url := strings.TrimSpace(n.URL.String)
		if n.Status == "unread" {
			unread++
		}
		views = append(views, espaiNotificationView{
			ID:        n.ID,
			Kind:      n.Kind,
			Title:     title,
			Body:      body,
			URL:       url,
			Status:    n.Status,
			Icon:      espaiNotifIcon(n.Kind),
			CreatedAt: n.CreatedAt,
		})
	}
	return views, unread
}

func (a *App) shouldEspaiNotify(userID int, kind string) bool {
	pref := a.loadEspaiNotificationPrefs(userID)
	if strings.TrimSpace(pref.Freq) == "off" {
		return false
	}
	typeKey := espaiNotifKindToType[kind]
	if typeKey != "" && pref.HasCustomTypes && !pref.Types[typeKey] {
		return false
	}
	return true
}

func (a *App) notifyEspaiMatches(ownerID, arbreID int, count int) {
	if count <= 0 || ownerID == 0 {
		return
	}
	if !a.shouldEspaiNotify(ownerID, espaiNotifKindMatches) {
		return
	}
	user, _ := a.DB.GetUserByID(ownerID)
	lang := resolveUserLang(nil, user)
	tree, _ := a.DB.GetEspaiArbre(arbreID)
	title := T(lang, "space.notifications.matches.title")
	body := fmt.Sprintf(T(lang, "space.notifications.matches.body"), count)
	if tree != nil {
		body = fmt.Sprintf(T(lang, "space.notifications.matches.body_tree"), count, tree.Nom)
	}
	dedupe := espaiNotifDedupeKey(espaiNotifKindMatches, arbreID, a.loadEspaiNotificationPrefs(ownerID).Freq)
	_, _ = a.DB.CreateEspaiNotification(&db.EspaiNotification{
		UserID:     ownerID,
		Kind:       espaiNotifKindMatches,
		Title:      sqlNullString(title),
		Body:       sqlNullString(body),
		URL:        sqlNullString("/espai/coincidencies"),
		Status:     "unread",
		TreeID:     sql.NullInt64{Int64: int64(arbreID), Valid: arbreID > 0},
		DedupeKey:  sqlNullString(dedupe),
	})
}

func (a *App) notifyEspaiGrampsError(integ *db.EspaiIntegracioGramps, message string) {
	if integ == nil || integ.OwnerUserID == 0 {
		return
	}
	if !a.shouldEspaiNotify(integ.OwnerUserID, espaiNotifKindGrampsError) {
		return
	}
	user, _ := a.DB.GetUserByID(integ.OwnerUserID)
	lang := resolveUserLang(nil, user)
	title := T(lang, "space.notifications.gramps.title")
	body := fmt.Sprintf(T(lang, "space.notifications.gramps.body"), strings.TrimSpace(integ.BaseURL))
	if strings.TrimSpace(message) != "" {
		body = fmt.Sprintf(T(lang, "space.notifications.gramps.body_error"), strings.TrimSpace(integ.BaseURL), message)
	}
	dedupe := espaiNotifDedupeKey(espaiNotifKindGrampsError, integ.ID, a.loadEspaiNotificationPrefs(integ.OwnerUserID).Freq)
	_, _ = a.DB.CreateEspaiNotification(&db.EspaiNotification{
		UserID:     integ.OwnerUserID,
		Kind:       espaiNotifKindGrampsError,
		Title:      sqlNullString(title),
		Body:       sqlNullString(body),
		URL:        sqlNullString("/espai/integracions"),
		Status:     "unread",
		ObjectType: sqlNullString("gramps_integration"),
		ObjectID:   sql.NullInt64{Int64: int64(integ.ID), Valid: true},
		TreeID:     sql.NullInt64{Int64: int64(integ.ArbreID), Valid: integ.ArbreID > 0},
		DedupeKey:  sqlNullString(dedupe),
	})
}

func (a *App) notifyEspaiGroupConflicts(groupID int, created int) {
	if created <= 0 || groupID == 0 {
		return
	}
	group, _ := a.DB.GetEspaiGrup(groupID)
	members, _ := a.DB.ListEspaiGrupMembres(groupID)
	for _, m := range members {
		if strings.TrimSpace(m.Status) != "active" {
			continue
		}
		if !a.shouldEspaiNotify(m.UserID, espaiNotifKindGroupConflict) {
			continue
		}
		user, _ := a.DB.GetUserByID(m.UserID)
		lang := resolveUserLang(nil, user)
		title := T(lang, "space.notifications.group.title")
		body := fmt.Sprintf(T(lang, "space.notifications.group.body"), created)
		if group != nil {
			body = fmt.Sprintf(T(lang, "space.notifications.group.body_named"), created, group.Nom)
		}
		dedupe := espaiNotifDedupeKey(espaiNotifKindGroupConflict, groupID, a.loadEspaiNotificationPrefs(m.UserID).Freq)
		_, _ = a.DB.CreateEspaiNotification(&db.EspaiNotification{
			UserID:    m.UserID,
			Kind:      espaiNotifKindGroupConflict,
			Title:     sqlNullString(title),
			Body:      sqlNullString(body),
			URL:       sqlNullString(fmt.Sprintf("/espai/grups?group_id=%d&conflict_status=pending", groupID)),
			Status:    "unread",
			GroupID:   sql.NullInt64{Int64: int64(groupID), Valid: true},
			DedupeKey: sqlNullString(dedupe),
		})
	}
}

func espaiNotifIcon(kind string) string {
	switch kind {
	case espaiNotifKindMatches:
		return "fa-magnifying-glass"
	case espaiNotifKindGrampsError:
		return "fa-plug-circle-xmark"
	case espaiNotifKindGroupConflict:
		return "fa-triangle-exclamation"
	default:
		return "fa-bell"
	}
}

func parseEspaiNotifTypes(raw sql.NullString) map[string]bool {
	if !raw.Valid || strings.TrimSpace(raw.String) == "" {
		return nil
	}
	var list []string
	if err := json.Unmarshal([]byte(raw.String), &list); err != nil {
		return nil
	}
	out := map[string]bool{}
	for _, t := range list {
		key := strings.TrimSpace(t)
		if key == "" {
			continue
		}
		out[key] = true
	}
	return out
}

func filterEspaiNotifTypes(raw []string) []string {
	allowed := map[string]struct{}{}
	for _, t := range espaiNotifTypes {
		allowed[t] = struct{}{}
	}
	out := []string{}
	seen := map[string]struct{}{}
	for _, t := range raw {
		key := strings.TrimSpace(t)
		if key == "" {
			continue
		}
		if _, ok := allowed[key]; !ok {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	return out
}

func espaiNotifDedupeKey(kind string, objectID int, freq string) string {
	now := time.Now()
	suffix := now.Format("2006-01-02")
	if freq == "weekly" {
		year, week := now.ISOWeek()
		suffix = fmt.Sprintf("%d-W%02d", year, week)
	}
	if objectID > 0 {
		return fmt.Sprintf("%s:%d:%s", kind, objectID, suffix)
	}
	return fmt.Sprintf("%s:%s", kind, suffix)
}

func isValidEspaiNotifFreq(freq string) bool {
	switch freq {
	case "instant", "daily", "weekly", "off":
		return true
	default:
		return false
	}
}
