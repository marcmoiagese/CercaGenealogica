package core

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func buildCognomMergeReason(r *http.Request) string {
	preset := strings.TrimSpace(r.FormValue("reason_preset"))
	detail := strings.TrimSpace(r.FormValue("reason_detail"))
	if preset != "" && detail != "" {
		return preset + " - " + detail
	}
	if preset != "" {
		return preset
	}
	return detail
}

func (a *App) CognomMergeSuggest(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requireCognomsView(w, r)
	if !ok {
		return
	}
	if r.Method == http.MethodPost {
		a.cognomMergeSuggestSave(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	lang := resolveUserLang(r, user)
	q := r.URL.Query()
	msg := ""
	isOk := false
	if q.Get("ok") != "" {
		msg = T(lang, "surnames.merge.suggest.success")
		isOk = true
	} else if q.Get("err") != "" {
		msg = T(lang, "surnames.merge.suggest.error")
	}
	RenderPrivateTemplateLang(w, r, "cognoms-merge-suggest.html", lang, map[string]interface{}{
		"Msg": msg,
		"Ok":  isOk,
	})
}

func (a *App) CognomMergeSuggestTo(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requireCognomsView(w, r)
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/cognoms?err=merge", http.StatusSeeOther)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Redirect(w, r, "/cognoms?err=merge", http.StatusSeeOther)
		return
	}
	toID := extractID(r.URL.Path)
	if toID <= 0 {
		http.Redirect(w, r, "/cognoms?err=merge", http.StatusSeeOther)
		return
	}
	if formCanon := strings.TrimSpace(r.FormValue("canonical_id")); formCanon != "" {
		if canonVal, err := strconv.Atoi(formCanon); err != nil || canonVal != toID {
			http.Redirect(w, r, "/cognoms?err=merge", http.StatusSeeOther)
			return
		}
	}
	if canonID, _, err := a.resolveCognomCanonicalID(toID); err == nil && canonID > 0 {
		toID = canonID
	}
	if row, err := a.DB.GetCognom(toID); err != nil || row == nil {
		http.Redirect(w, r, "/cognoms?err=merge", http.StatusSeeOther)
		return
	}
	aliasIDs := parseCognomIDList(r.FormValue("alias_ids"))
	reason := buildCognomMergeReason(r)
	created := a.createCognomMergeSuggestions(r.Context(), user, toID, aliasIDs, reason)
	if created == 0 {
		http.Redirect(w, r, "/cognoms?err=merge", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/cognoms?ok=merge", http.StatusSeeOther)
}

func (a *App) CognomMergeSuggestFrom(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requireCognomsView(w, r)
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/cognoms?err=merge", http.StatusSeeOther)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Redirect(w, r, "/cognoms?err=merge", http.StatusSeeOther)
		return
	}
	fromID := extractID(r.URL.Path)
	if fromID <= 0 {
		http.Redirect(w, r, "/cognoms?err=merge", http.StatusSeeOther)
		return
	}
	toID, _ := strconv.Atoi(strings.TrimSpace(r.FormValue("canonical_id")))
	if toID <= 0 {
		http.Redirect(w, r, "/cognoms?err=merge", http.StatusSeeOther)
		return
	}
	if canonID, _, err := a.resolveCognomCanonicalID(toID); err == nil && canonID > 0 {
		toID = canonID
	}
	if row, err := a.DB.GetCognom(toID); err != nil || row == nil {
		http.Redirect(w, r, "/cognoms?err=merge", http.StatusSeeOther)
		return
	}
	aliasIDs := []int{fromID}
	reason := buildCognomMergeReason(r)
	created := a.createCognomMergeSuggestions(r.Context(), user, toID, aliasIDs, reason)
	if created == 0 {
		http.Redirect(w, r, "/cognoms?err=merge", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/cognoms?ok=merge", http.StatusSeeOther)
}

func (a *App) cognomMergeSuggestSave(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requireCognomsView(w, r)
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/cognoms/merge?err=1", http.StatusSeeOther)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Redirect(w, r, "/cognoms/merge?err=1", http.StatusSeeOther)
		return
	}
	toID, _ := strconv.Atoi(strings.TrimSpace(r.FormValue("canonical_id")))
	if toID <= 0 {
		http.Redirect(w, r, "/cognoms/merge?err=1", http.StatusSeeOther)
		return
	}
	if canonID, _, err := a.resolveCognomCanonicalID(toID); err == nil && canonID > 0 {
		toID = canonID
	}
	if row, err := a.DB.GetCognom(toID); err != nil || row == nil {
		http.Redirect(w, r, "/cognoms/merge?err=1", http.StatusSeeOther)
		return
	}
	aliasIDs := parseCognomIDList(r.FormValue("alias_ids"))
	if len(aliasIDs) == 0 {
		http.Redirect(w, r, "/cognoms/merge?err=1", http.StatusSeeOther)
		return
	}
	reason := buildCognomMergeReason(r)
	created := a.createCognomMergeSuggestions(r.Context(), user, toID, aliasIDs, reason)
	if created == 0 {
		http.Redirect(w, r, "/cognoms/merge?err=1", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/cognoms/merge?ok=1", http.StatusSeeOther)
}

func (a *App) createCognomMergeSuggestions(ctx context.Context, user *db.User, toID int, aliasIDs []int, reason string) int {
	created := 0
	for _, fromID := range aliasIDs {
		if fromID <= 0 || fromID == toID {
			continue
		}
		if _, err := a.DB.GetCognom(fromID); err != nil {
			continue
		}
		if existing, err := a.DB.GetCognomRedirect(fromID); err == nil && existing != nil {
			continue
		}
		if canonFrom, _, err := a.resolveCognomCanonicalID(fromID); err == nil && canonFrom > 0 && canonFrom != fromID {
			continue
		}
		if pending, err := a.DB.ListCognomRedirectSuggestions(db.CognomRedirectSuggestionFilter{
			Status:       "pendent",
			FromCognomID: fromID,
			ToCognomID:   toID,
		}); err == nil && len(pending) > 0 {
			continue
		}
		sugg := &db.CognomRedirectSuggestion{
			FromCognomID:   fromID,
			ToCognomID:     toID,
			Reason:         reason,
			ModeracioEstat: "pendent",
			CreatedBy:      sqlNullIntFromInt(user.ID),
		}
		suggID, err := a.DB.CreateCognomRedirectSuggestion(sugg)
		if err != nil {
			continue
		}
		details, _ := json.Marshal(map[string]interface{}{
			"from":   fromID,
			"to":     toID,
			"reason": reason,
		})
		_, _ = a.RegisterUserActivity(ctx, user.ID, "cognom_merge_suggest", "crear", "cognom_merge", &suggID, "pendent", nil, string(details))
		created++
	}
	return created
}
