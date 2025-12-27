package core

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type adminUserRowView struct {
	ID        int
	Usuari    string
	Nom       string
	Cognoms   string
	Email     string
	CreatedAt string
	LastLogin string
	Active    bool
	Banned    bool
}

type userAdminFilterer interface {
	ListUsersAdminFiltered(filter db.UserAdminFilter) ([]db.UserAdminRow, error)
}

type userAdminCounter interface {
	CountUsersAdmin(filter db.UserAdminFilter) (int, error)
}

func (a *App) AdminListUsuaris(w http.ResponseWriter, r *http.Request) {
	user, perms, ok := a.requirePermission(w, r, permUsers)
	if !ok {
		return
	}
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	banned := strings.TrimSpace(r.URL.Query().Get("banned"))
	perPage := parseAdminUsersPerPage(r.URL.Query().Get("per_page"))
	page := 1
	if pStr := r.URL.Query().Get("page"); pStr != "" {
		if v, err := strconv.Atoi(pStr); err == nil && v > 0 {
			page = v
		}
	}
	var activeFilter *bool
	switch status {
	case "active":
		val := true
		activeFilter = &val
	case "inactive":
		val := false
		activeFilter = &val
	}
	var bannedFilter *bool
	switch banned {
	case "yes":
		val := true
		bannedFilter = &val
	case "no":
		val := false
		bannedFilter = &val
	}
	offset := (page - 1) * perPage
	filter := db.UserAdminFilter{
		Query:  q,
		Active: activeFilter,
		Banned: bannedFilter,
		Limit:  perPage,
		Offset: offset,
	}
	rows, total, err := a.loadUsersAdmin(filter)
	if err != nil {
		http.Error(w, "Error carregant usuaris", http.StatusInternalServerError)
		return
	}
	view := make([]adminUserRowView, 0, len(rows))
	for _, row := range rows {
		created := formatDateTimeDisplay(row.CreatedAt)
		last := formatDateTimeDisplay(row.LastLogin)
		view = append(view, adminUserRowView{
			ID:        row.ID,
			Usuari:    row.Usuari,
			Nom:       row.Nom,
			Cognoms:   row.Cognoms,
			Email:     row.Email,
			CreatedAt: created,
			LastLogin: last,
			Active:    row.Active,
			Banned:    row.Banned,
		})
	}
	lang := ResolveLang(r)
	msg := ""
	okMsg := false
	if r.URL.Query().Get("ok") != "" {
		msg = T(lang, "common.saved")
		okMsg = true
	} else if r.URL.Query().Get("err") != "" {
		msg = T(lang, "common.error")
	}
	token, _ := ensureCSRF(w, r)
	totalPages := 1
	if perPage > 0 && total > 0 {
		totalPages = (total + perPage - 1) / perPage
	}
	if totalPages <= 0 {
		totalPages = 1
	}
	rangeStart := 0
	rangeEnd := 0
	if total > 0 {
		rangeStart = offset + 1
		if rangeStart > total {
			rangeStart = total
		}
		rangeEnd = offset + len(rows)
		if rangeEnd > total {
			rangeEnd = total
		}
	}
	pageBaseValues := cloneValues(r.URL.Query())
	pageBaseValues.Del("page")
	pageBaseValues.Del("ok")
	pageBaseValues.Del("err")
	pageBase := pageBaseValues.Encode()
	currentURL := r.URL.Path
	if r.URL.RawQuery != "" {
		currentURL += "?" + r.URL.RawQuery
	}
	RenderPrivateTemplate(w, r, "admin-usuaris-list.html", map[string]interface{}{
		"Users":             view,
		"User":              user,
		"Msg":               msg,
		"Ok":                okMsg,
		"CSRFToken":         token,
		"CurrentUserID":     user.ID,
		"CanManagePolicies": perms.Admin || perms.CanManagePolicies,
		"FilterQuery":       q,
		"FilterStatus":      status,
		"FilterBanned":      banned,
		"Page":              page,
		"PerPage":           perPage,
		"Total":             total,
		"TotalPages":        totalPages,
		"HasPrev":           page > 1,
		"HasNext":           page < totalPages,
		"PrevPage":          page - 1,
		"NextPage":          page + 1,
		"PageBase":          pageBase,
		"RangeStart":        rangeStart,
		"RangeEnd":          rangeEnd,
		"ReturnTo":          currentURL,
	})
}

func (a *App) AdminSetUserActive(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permUsers); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/admin/usuaris?err=1", http.StatusSeeOther)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	userID, _ := strconv.Atoi(strings.TrimSpace(r.FormValue("user_id")))
	active := r.FormValue("active") == "1"
	if userID <= 0 {
		http.Redirect(w, r, "/admin/usuaris?err=1", http.StatusSeeOther)
		return
	}
	if err := a.DB.SetUserActive(userID, active); err != nil {
		Errorf("Error actualitzant usuari %d actiu=%v: %v", userID, active, err)
		http.Redirect(w, r, safeReturnTo(r.FormValue("return_to"), "/admin/usuaris?err=1"), http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, safeReturnTo(r.FormValue("return_to"), "/admin/usuaris?ok=1"), http.StatusSeeOther)
}

func (a *App) AdminSetUserBanned(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permUsers); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/admin/usuaris?err=1", http.StatusSeeOther)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	userID, _ := strconv.Atoi(strings.TrimSpace(r.FormValue("user_id")))
	banned := r.FormValue("banned") == "1"
	if userID <= 0 {
		http.Redirect(w, r, "/admin/usuaris?err=1", http.StatusSeeOther)
		return
	}
	if err := a.DB.SetUserBanned(userID, banned); err != nil {
		Errorf("Error actualitzant usuari %d banned=%v: %v", userID, banned, err)
		http.Redirect(w, r, safeReturnTo(r.FormValue("return_to"), "/admin/usuaris?err=1"), http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, safeReturnTo(r.FormValue("return_to"), "/admin/usuaris?ok=1"), http.StatusSeeOther)
}

func (a *App) loadUsersAdmin(filter db.UserAdminFilter) ([]db.UserAdminRow, int, error) {
	if lister, ok := a.DB.(userAdminFilterer); ok {
		rows, err := lister.ListUsersAdminFiltered(filter)
		if err != nil {
			return nil, 0, err
		}
		total := len(rows)
		if counter, ok := a.DB.(userAdminCounter); ok {
			if cnt, err := counter.CountUsersAdmin(filter); err == nil {
				total = cnt
			}
		}
		return rows, total, nil
	}
	rows, err := a.DB.ListUsersAdmin()
	if err != nil {
		return nil, 0, err
	}
	q := strings.ToLower(strings.TrimSpace(filter.Query))
	filtered := make([]db.UserAdminRow, 0, len(rows))
	for _, row := range rows {
		if filter.UserID > 0 && row.ID != filter.UserID {
			continue
		}
		if filter.Active != nil && row.Active != *filter.Active {
			continue
		}
		if filter.Banned != nil && row.Banned != *filter.Banned {
			continue
		}
		if q != "" {
			if !strings.Contains(strings.ToLower(row.Usuari), q) &&
				!strings.Contains(strings.ToLower(row.Nom), q) &&
				!strings.Contains(strings.ToLower(row.Cognoms), q) &&
				!strings.Contains(strings.ToLower(row.Email), q) &&
				!strings.Contains(strconv.Itoa(row.ID), q) {
				continue
			}
		}
		filtered = append(filtered, row)
	}
	total := len(filtered)
	start := filter.Offset
	if start < 0 {
		start = 0
	}
	end := start + filter.Limit
	if filter.Limit <= 0 {
		end = total
	}
	if start > total {
		start = total
	}
	if end > total {
		end = total
	}
	return filtered[start:end], total, nil
}

func parseAdminUsersPerPage(val string) int {
	switch strings.TrimSpace(val) {
	case "10":
		return 10
	case "50":
		return 50
	case "100":
		return 100
	default:
		return 25
	}
}

func safeReturnTo(val string, fallback string) string {
	path := strings.TrimSpace(val)
	if path == "" {
		return fallback
	}
	if strings.HasPrefix(path, "/") && !strings.HasPrefix(path, "//") && !strings.Contains(path, "://") {
		return path
	}
	return fallback
}
