package core

import (
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type cognomRedirectView struct {
	FromID    int
	ToID      int
	FromLabel string
	ToLabel   string
	FromURL   string
	ToURL     string
	Reason    string
	CreatedAt string
	CreatedBy string
}

func (a *App) AdminCognomsMerge(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permAdmin); !ok {
		return
	}
	if r.Method == http.MethodPost {
		a.adminCognomsMergeSave(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	lang := ResolveLang(r)
	redirects, _ := a.DB.ListCognomRedirects()
	cognomCache := map[int]*db.Cognom{}
	userCache := map[int]*db.User{}
	resolveCognom := func(id int) string {
		if id <= 0 {
			return "—"
		}
		if cached, ok := cognomCache[id]; ok {
			if cached == nil {
				return fmt.Sprintf("Cognom %d", id)
			}
			if strings.TrimSpace(cached.Forma) != "" {
				return cached.Forma
			}
			return fmt.Sprintf("Cognom %d", id)
		}
		row, err := a.DB.GetCognom(id)
		if err != nil || row == nil {
			cognomCache[id] = nil
			return fmt.Sprintf("Cognom %d", id)
		}
		cognomCache[id] = row
		if strings.TrimSpace(row.Forma) != "" {
			return row.Forma
		}
		return fmt.Sprintf("Cognom %d", id)
	}
	resolveUser := func(id sql.NullInt64) string {
		if !id.Valid {
			return ""
		}
		uid := int(id.Int64)
		if cached, ok := userCache[uid]; ok {
			if cached == nil {
				return ""
			}
			name := strings.TrimSpace(cached.Usuari)
			if name == "" {
				full := strings.TrimSpace(strings.TrimSpace(cached.Name) + " " + strings.TrimSpace(cached.Surname))
				name = full
			}
			return name
		}
		row, err := a.DB.GetUserByID(uid)
		if err != nil || row == nil {
			userCache[uid] = nil
			return ""
		}
		userCache[uid] = row
		name := strings.TrimSpace(row.Usuari)
		if name == "" {
			name = strings.TrimSpace(strings.TrimSpace(row.Name) + " " + strings.TrimSpace(row.Surname))
		}
		return name
	}

	views := make([]cognomRedirectView, 0, len(redirects))
	for _, row := range redirects {
		created := ""
		if row.CreatedAt.Valid {
			created = row.CreatedAt.Time.Format("2006-01-02 15:04")
		}
		views = append(views, cognomRedirectView{
			FromID:    row.FromCognomID,
			ToID:      row.ToCognomID,
			FromLabel: resolveCognom(row.FromCognomID),
			ToLabel:   resolveCognom(row.ToCognomID),
			FromURL:   fmt.Sprintf("/cognoms/%d", row.FromCognomID),
			ToURL:     fmt.Sprintf("/cognoms/%d", row.ToCognomID),
			Reason:    strings.TrimSpace(row.Reason),
			CreatedAt: created,
			CreatedBy: resolveUser(row.CreatedBy),
		})
	}

	q := r.URL.Query()
	msg := ""
	ok := false
	if q.Get("ok") != "" {
		msg = T(lang, "admin.surnames.merge.success")
		ok = true
	} else if q.Get("deleted") != "" {
		msg = T(lang, "admin.surnames.merge.deleted")
		ok = true
	} else if q.Get("err") != "" {
		msg = T(lang, "admin.surnames.merge.error")
	}
	RenderPrivateTemplate(w, r, "admin-cognoms-merge.html", map[string]interface{}{
		"Redirects": views,
		"Msg":       msg,
		"Ok":        ok,
	})
}

func (a *App) adminCognomsMergeSave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/admin/cognoms/merge?err=1", http.StatusSeeOther)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Redirect(w, r, "/admin/cognoms/merge?err=1", http.StatusSeeOther)
		return
	}
	user, _ := a.VerificarSessio(r)
	if user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	toID, _ := strconv.Atoi(strings.TrimSpace(r.FormValue("canonical_id")))
	if toID <= 0 {
		http.Redirect(w, r, "/admin/cognoms/merge?err=1", http.StatusSeeOther)
		return
	}
	if canonID, _, err := a.resolveCognomCanonicalID(toID); err == nil && canonID > 0 {
		toID = canonID
	}
	if row, err := a.DB.GetCognom(toID); err != nil || row == nil {
		http.Redirect(w, r, "/admin/cognoms/merge?err=1", http.StatusSeeOther)
		return
	}
	aliasIDs := parseCognomIDList(r.FormValue("alias_ids"))
	if len(aliasIDs) == 0 {
		http.Redirect(w, r, "/admin/cognoms/merge?err=1", http.StatusSeeOther)
		return
	}
	reason := buildCognomMergeReason(r)
	created := 0
	for _, fromID := range aliasIDs {
		if fromID <= 0 || fromID == toID {
			continue
		}
		if row, err := a.DB.GetCognom(fromID); err != nil || row == nil {
			continue
		}
		if err := a.DB.SetCognomRedirect(fromID, toID, &user.ID, reason); err != nil {
			continue
		}
		created++
	}
	if created == 0 {
		http.Redirect(w, r, "/admin/cognoms/merge?err=1", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/admin/cognoms/merge?ok=1", http.StatusSeeOther)
}

func (a *App) AdminCognomsMergeDelete(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permAdmin); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/admin/cognoms/merge?err=1", http.StatusSeeOther)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Redirect(w, r, "/admin/cognoms/merge?err=1", http.StatusSeeOther)
		return
	}
	fromID, _ := strconv.Atoi(strings.TrimSpace(r.FormValue("from_id")))
	if fromID <= 0 {
		http.Redirect(w, r, "/admin/cognoms/merge?err=1", http.StatusSeeOther)
		return
	}
	_ = a.DB.DeleteCognomRedirect(fromID)
	http.Redirect(w, r, "/admin/cognoms/merge?deleted=1", http.StatusSeeOther)
}

func parseCognomIDList(raw string) []int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';' || r == '\n' || r == '\t' || r == ' '
	})
	seen := map[int]struct{}{}
	var out []int
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, err := strconv.Atoi(part)
		if err != nil || id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
