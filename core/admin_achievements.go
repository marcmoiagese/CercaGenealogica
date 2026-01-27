package core

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

var achievementRarityOptions = []string{"common", "rare", "epic", "legendary"}
var achievementVisibilityOptions = []string{"visible", "hidden", "seasonal"}
var achievementDomainOptions = []string{
	"general",
	"moderacio",
	"municipis",
	"llibres",
	"persones",
	"connexions",
	"territori",
	"arxius",
	"eclesiastic",
}

// AdminListAchievements mostra tots els achievements (actius o no) per gestio admin.
func (a *App) AdminListAchievements(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requireAnyPermissionKey(w, r, []string{permKeyAdminAchievementsAdd, permKeyAdminAchievementsEdit}, PermissionTarget{})
	if !ok {
		return
	}
	items, err := a.DB.ListAchievements()
	if err != nil {
		http.Error(w, "Error llistant achievements", http.StatusInternalServerError)
		return
	}
	iconPublicIDs := map[int]string{}
	for _, ach := range items {
		if !ach.IconMediaItemID.Valid {
			continue
		}
		item, err := a.DB.GetMediaItemByID(int(ach.IconMediaItemID.Int64))
		if err != nil || item == nil || item.PublicID == "" {
			continue
		}
		iconPublicIDs[ach.ID] = item.PublicID
	}
	token, _ := ensureCSRF(w, r)
	lang := ResolveLang(r)
	msg := ""
	okMsg := false
	if r.URL.Query().Get("recompute") != "" {
		awarded, _ := strconv.Atoi(r.URL.Query().Get("awarded"))
		usersCount, _ := strconv.Atoi(r.URL.Query().Get("users"))
		dry := r.URL.Query().Get("dry") == "1"
		key := "achievements.recompute.success"
		if dry {
			key = "achievements.recompute.dry"
		}
		msg = fmt.Sprintf(T(lang, key), awarded, usersCount)
		okMsg = true
	} else if r.URL.Query().Get("recompute_err") != "" {
		msg = T(lang, "achievements.recompute.error")
	} else if r.URL.Query().Get("ok") != "" {
		msg = T(lang, "common.saved")
		okMsg = true
	} else if r.URL.Query().Get("err") != "" {
		msg = T(lang, "common.error")
	}
	RenderPrivateTemplate(w, r, "admin-achievements-list.html", map[string]interface{}{
		"Achievements":    items,
		"IconPublicIDs":   iconPublicIDs,
		"User":            user,
		"CanManageArxius": true,
		"Msg":             msg,
		"Ok":              okMsg,
		"CSRFToken":       token,
	})
}

// AdminNewAchievement mostra el formulari de nou achievement.
func (a *App) AdminNewAchievement(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminAchievementsAdd, PermissionTarget{}); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	token, _ := ensureCSRF(w, r)
	ach := &db.Achievement{
		Rarity:     "common",
		Visibility: "visible",
		Domain:     "general",
		IsEnabled:  true,
		RuleJSON:   `{"type":"count","filters":{"status":["validat"]},"threshold":1}`,
	}
	a.renderAchievementForm(w, r, user, ach, true, token, "")
}

// AdminEditAchievement mostra el formulari d'edició.
func (a *App) AdminEditAchievement(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminAchievementsEdit, PermissionTarget{}); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	id := extractID(r.URL.Path)
	ach, err := a.DB.GetAchievement(id)
	if err != nil || ach == nil {
		http.NotFound(w, r)
		return
	}
	token, _ := ensureCSRF(w, r)
	a.renderAchievementForm(w, r, user, ach, false, token, "")
}

// AdminSaveAchievement desa un achievement (create/update).
func (a *App) AdminSaveAchievement(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Formulari invàlid", http.StatusBadRequest)
		return
	}
	id, _ := strconv.Atoi(r.FormValue("id"))
	permKey := permKeyAdminAchievementsAdd
	if id > 0 {
		permKey = permKeyAdminAchievementsEdit
	}
	user, ok := a.requirePermissionKey(w, r, permKey, PermissionTarget{})
	if !ok {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	code := strings.TrimSpace(r.FormValue("code"))
	if id > 0 {
		if existing, err := a.DB.GetAchievement(id); err == nil && existing != nil {
			code = existing.Code
		}
	}
	name := strings.TrimSpace(r.FormValue("name"))
	desc := strings.TrimSpace(r.FormValue("description"))
	rarity := strings.TrimSpace(r.FormValue("rarity"))
	visibility := strings.TrimSpace(r.FormValue("visibility"))
	domain := strings.TrimSpace(r.FormValue("domain"))
	isEnabled := r.FormValue("is_enabled") == "1"
	isRepeatable := r.FormValue("is_repeatable") == "1"
	ruleJSON := strings.TrimSpace(r.FormValue("rule_json"))
	iconID, _ := strconv.Atoi(strings.TrimSpace(r.FormValue("icon_media_item_id")))

	if code == "" || name == "" || ruleJSON == "" {
		token, _ := ensureCSRF(w, r)
		ach := buildAchievementFromForm(id, code, name, desc, rarity, visibility, domain, isEnabled, isRepeatable, ruleJSON, iconID)
		a.renderAchievementForm(w, r, user, ach, id == 0, token, T(ResolveLang(r), "common.required"))
		return
	}
	if !isAchievementOptionAllowed(rarity, achievementRarityOptions) {
		token, _ := ensureCSRF(w, r)
		ach := buildAchievementFromForm(id, code, name, desc, rarity, visibility, domain, isEnabled, isRepeatable, ruleJSON, iconID)
		a.renderAchievementForm(w, r, user, ach, id == 0, token, T(ResolveLang(r), "common.invalid"))
		return
	}
	if !isAchievementOptionAllowed(visibility, achievementVisibilityOptions) {
		token, _ := ensureCSRF(w, r)
		ach := buildAchievementFromForm(id, code, name, desc, rarity, visibility, domain, isEnabled, isRepeatable, ruleJSON, iconID)
		a.renderAchievementForm(w, r, user, ach, id == 0, token, T(ResolveLang(r), "common.invalid"))
		return
	}
	if !isAchievementOptionAllowed(domain, achievementDomainOptions) {
		token, _ := ensureCSRF(w, r)
		ach := buildAchievementFromForm(id, code, name, desc, rarity, visibility, domain, isEnabled, isRepeatable, ruleJSON, iconID)
		a.renderAchievementForm(w, r, user, ach, id == 0, token, T(ResolveLang(r), "common.invalid"))
		return
	}
	if _, err := parseAchievementRule(ruleJSON); err != nil {
		token, _ := ensureCSRF(w, r)
		ach := buildAchievementFromForm(id, code, name, desc, rarity, visibility, domain, isEnabled, isRepeatable, ruleJSON, iconID)
		a.renderAchievementForm(w, r, user, ach, id == 0, token, T(ResolveLang(r), "achievements.rule.invalid"))
		return
	}
	icon := sql.NullInt64{}
	if iconID > 0 {
		item, err := a.DB.GetMediaItemByID(iconID)
		if err != nil || item == nil {
			token, _ := ensureCSRF(w, r)
			ach := buildAchievementFromForm(id, code, name, desc, rarity, visibility, domain, isEnabled, isRepeatable, ruleJSON, iconID)
			a.renderAchievementForm(w, r, user, ach, id == 0, token, T(ResolveLang(r), "achievements.icon.invalid"))
			return
		}
		album, err := a.DB.GetMediaAlbumByID(item.AlbumID)
		if err != nil || album == nil || album.AlbumType != "achievement_icon" {
			token, _ := ensureCSRF(w, r)
			ach := buildAchievementFromForm(id, code, name, desc, rarity, visibility, domain, isEnabled, isRepeatable, ruleJSON, iconID)
			a.renderAchievementForm(w, r, user, ach, id == 0, token, T(ResolveLang(r), "achievements.icon.invalid"))
			return
		}
		icon = sql.NullInt64{Int64: int64(iconID), Valid: true}
	}

	ach := &db.Achievement{
		ID:           id,
		Code:         code,
		Name:         name,
		Description:  desc,
		Rarity:       rarity,
		Visibility:   visibility,
		Domain:       domain,
		IsEnabled:    isEnabled,
		IsRepeatable: isRepeatable,
		IconMediaItemID: icon,
		RuleJSON:     ruleJSON,
	}
	if _, err := a.DB.SaveAchievement(ach); err != nil {
		token, _ := ensureCSRF(w, r)
		a.renderAchievementForm(w, r, user, ach, id == 0, token, T(ResolveLang(r), "common.error"))
		return
	}
	if a.achievementCache != nil {
		a.achievementCache.invalidate()
	}
	http.Redirect(w, r, "/admin/achievements?ok=1", http.StatusSeeOther)
}

func (a *App) AdminRecomputeAchievements(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Formulari invàlid", http.StatusBadRequest)
		return
	}
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminAchievementsEdit, PermissionTarget{}); !ok {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	achievementID, _ := strconv.Atoi(strings.TrimSpace(r.FormValue("achievement_id")))
	userID, _ := strconv.Atoi(strings.TrimSpace(r.FormValue("user_id")))
	dryRun := r.FormValue("dry_run") == "1" || r.FormValue("dry_run") == "on"

	achievements, err := a.loadAchievementsForRecompute(achievementID)
	if err != nil {
		http.Redirect(w, r, "/admin/achievements?recompute_err=1", http.StatusSeeOther)
		return
	}

	svc := NewAchievementsService(a.DB)
	svc.Candidates = achievements
	svc.DryRun = dryRun
	trigger := AchievementTrigger{CreatedAt: time.Now()}
	totalAwards := 0
	totalUsers := 0
	ctx := context.Background()

	if userID > 0 {
		if _, err := a.DB.GetUserByID(userID); err != nil {
			http.Redirect(w, r, "/admin/achievements?recompute_err=1", http.StatusSeeOther)
			return
		}
		awarded, err := svc.EvaluateForUser(ctx, userID, trigger)
		if err != nil {
			http.Redirect(w, r, "/admin/achievements?recompute_err=1", http.StatusSeeOther)
			return
		}
		totalAwards = len(awarded)
		totalUsers = 1
	} else {
		offset := 0
		for {
			ids, err := a.DB.ListUserIDs(100, offset)
			if err != nil {
				http.Redirect(w, r, "/admin/achievements?recompute_err=1", http.StatusSeeOther)
				return
			}
			if len(ids) == 0 {
				break
			}
			for _, id := range ids {
				awarded, err := svc.EvaluateForUser(ctx, id, trigger)
				if err != nil {
					http.Redirect(w, r, "/admin/achievements?recompute_err=1", http.StatusSeeOther)
					return
				}
				totalAwards += len(awarded)
			}
			totalUsers += len(ids)
			offset += len(ids)
		}
	}
	dryFlag := 0
	if dryRun {
		dryFlag = 1
	}
	redirectURL := fmt.Sprintf("/admin/achievements?recompute=1&awarded=%d&users=%d&dry=%d", totalAwards, totalUsers, dryFlag)
	http.Redirect(w, r, redirectURL, http.StatusSeeOther)
}

func (a *App) loadAchievementsForRecompute(achievementID int) ([]db.Achievement, error) {
	if achievementID <= 0 {
		return a.DB.ListEnabledAchievements()
	}
	ach, err := a.DB.GetAchievement(achievementID)
	if err != nil || ach == nil {
		return nil, errors.New("achievement not found")
	}
	if !ach.IsEnabled {
		return nil, errors.New("achievement disabled")
	}
	return []db.Achievement{*ach}, nil
}

func (a *App) renderAchievementForm(w http.ResponseWriter, r *http.Request, user *db.User, ach *db.Achievement, isNew bool, token string, errMsg string) {
	iconItems := a.achievementIconItems()
	selectedIconID := 0
	if ach != nil && ach.IconMediaItemID.Valid {
		selectedIconID = int(ach.IconMediaItemID.Int64)
	}
	RenderPrivateTemplate(w, r, "admin-achievements-form.html", map[string]interface{}{
		"Achievement":      ach,
		"IsNew":            isNew,
		"Error":            errMsg,
		"CSRFToken":        token,
		"User":             user,
		"IconItems":        iconItems,
		"SelectedIconID":   selectedIconID,
		"RarityOptions":    achievementRarityOptions,
		"VisibilityOptions": achievementVisibilityOptions,
		"DomainOptions":    achievementDomainOptions,
	})
}

func (a *App) achievementIconItems() []db.MediaItem {
	items, err := a.DB.ListMediaItemsByAlbumType("achievement_icon", "")
	if err != nil {
		return []db.MediaItem{}
	}
	return items
}

func buildAchievementFromForm(id int, code, name, desc, rarity, visibility, domain string, enabled, repeatable bool, ruleJSON string, iconID int) *db.Achievement {
	icon := sql.NullInt64{}
	if iconID > 0 {
		icon = sql.NullInt64{Int64: int64(iconID), Valid: true}
	}
	return &db.Achievement{
		ID:              id,
		Code:            code,
		Name:            name,
		Description:     desc,
		Rarity:          rarity,
		Visibility:      visibility,
		Domain:          domain,
		IsEnabled:       enabled,
		IsRepeatable:    repeatable,
		IconMediaItemID: icon,
		RuleJSON:        ruleJSON,
	}
}

func isAchievementOptionAllowed(val string, options []string) bool {
	val = strings.TrimSpace(val)
	for _, opt := range options {
		if opt == val {
			return true
		}
	}
	return false
}
