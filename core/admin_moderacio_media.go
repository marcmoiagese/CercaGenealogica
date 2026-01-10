package core

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type mediaModerationAlbumView struct {
	Album     db.MediaAlbum
	OwnerName string
	OwnerURL  string
}

type mediaModerationItemView struct {
	Item         db.MediaItem
	AlbumTitle   string
	AlbumPublic  string
	OwnerName    string
	OwnerURL     string
	AlbumPending bool
}

func (a *App) AdminModeracioMediaList(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permModerate); !ok {
		return
	}
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user := userFromContext(r)
	if user == nil {
		user, _ = a.VerificarSessio(r)
	}
	lang := resolveUserLang(r, user)

	msg := ""
	okFlag := false
	if r.URL.Query().Get("ok") != "" {
		okFlag = true
		msg = T(lang, "moderation.success")
	} else if r.URL.Query().Get("err") != "" {
		msg = T(lang, "moderation.error")
	}

	groups, _ := a.DB.ListGroups()
	policies, _ := a.DB.ListPolitiques()

	userCache := map[int]*db.User{}
	ownerFromID := func(id int) (string, string) {
		if id <= 0 {
			return "-", ""
		}
		if cached, ok := userCache[id]; ok {
			return userDisplayName(cached), "/u/" + strconv.Itoa(cached.ID)
		}
		u, err := a.DB.GetUserByID(id)
		if err != nil || u == nil {
			return "-", ""
		}
		userCache[id] = u
		return userDisplayName(u), "/u/" + strconv.Itoa(u.ID)
	}

	albums, err := a.DB.ListMediaAlbumsByStatus("pending")
	if err != nil {
		Errorf("Error carregant moderacio media albums: %v", err)
		albums = []db.MediaAlbum{}
	}
	albumViews := make([]mediaModerationAlbumView, 0, len(albums))
	for _, album := range albums {
		name, url := ownerFromID(album.OwnerUserID)
		albumViews = append(albumViews, mediaModerationAlbumView{
			Album:     album,
			OwnerName: name,
			OwnerURL:  url,
		})
	}

	items, err := a.DB.ListMediaItemsByStatus("pending")
	if err != nil {
		Errorf("Error carregant moderacio media items: %v", err)
		items = []db.MediaItem{}
	}
	albumCache := map[int]*db.MediaAlbum{}
	itemViews := make([]mediaModerationItemView, 0, len(items))
	for _, item := range items {
		album, ok := albumCache[item.AlbumID]
		if !ok {
			row, err := a.DB.GetMediaAlbumByID(item.AlbumID)
			if err != nil || row == nil {
				albumCache[item.AlbumID] = nil
				album = nil
			} else {
				albumCache[item.AlbumID] = row
				album = row
			}
		}
		albumTitle := ""
		albumPublic := ""
		ownerName := "-"
		ownerURL := ""
		albumPending := false
		if album != nil {
			albumTitle = album.Title
			albumPublic = album.PublicID
			albumPending = album.ModerationStatus == "pending"
			ownerName, ownerURL = ownerFromID(album.OwnerUserID)
		}
		itemViews = append(itemViews, mediaModerationItemView{
			Item:         item,
			AlbumTitle:   albumTitle,
			AlbumPublic:  albumPublic,
			OwnerName:    ownerName,
			OwnerURL:     ownerURL,
			AlbumPending: albumPending,
		})
	}

	RenderPrivateTemplateLang(w, r, "admin-moderacio-media.html", lang, map[string]interface{}{
		"User":              user,
		"PendingAlbums":     albumViews,
		"PendingItems":      itemViews,
		"Groups":            groups,
		"Policies":          policies,
		"VisibilityOptions": mediaVisibilityList(),
		"SourceTypes":       mediaSourceTypeList(),
		"Msg":               msg,
		"Ok":                okFlag,
	})
}

func (a *App) AdminModeracioMediaAlbumApprove(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permModerate); !ok {
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
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/admin/moderacio/media?err=1", http.StatusSeeOther)
		return
	}
	user := userFromContext(r)
	if user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	visibility := normalizeMediaVisibility(r.FormValue("visibility"))
	restrictedGroupID := parseIntDefault(r.FormValue("restricted_group_id"), 0)
	accessPolicyID := parseIntDefault(r.FormValue("access_policy_id"), 0)
	creditCost := parseIntDefault(r.FormValue("credit_cost"), 0)
	difficulty := parseIntDefault(r.FormValue("difficulty_score"), 0)
	sourceType := normalizeMediaSourceType(r.FormValue("source_type"))
	notes := strings.TrimSpace(r.FormValue("notes"))

	if creditCost < 0 || difficulty < 0 {
		http.Redirect(w, r, "/admin/moderacio/media?err=1", http.StatusSeeOther)
		return
	}
	if visibility == "restricted_group" && restrictedGroupID <= 0 {
		http.Redirect(w, r, "/admin/moderacio/media?err=1", http.StatusSeeOther)
		return
	}
	if visibility == "custom_policy" && accessPolicyID <= 0 {
		http.Redirect(w, r, "/admin/moderacio/media?err=1", http.StatusSeeOther)
		return
	}

	album, err := a.DB.GetMediaAlbumByID(id)
	if err != nil || album == nil {
		http.NotFound(w, r)
		return
	}
	wasApproved := album.ModerationStatus == "approved"

	if err := a.DB.UpdateMediaAlbumModeration(id, "approved", visibility, restrictedGroupID, accessPolicyID, creditCost, difficulty, sourceType, notes, user.ID); err != nil {
		Errorf("Moderacio media album aprovar %d ha fallat: %v", id, err)
		http.Redirect(w, r, "/admin/moderacio/media?err=1", http.StatusSeeOther)
		return
	}
	if !wasApproved && album.OwnerUserID > 0 {
		points := a.mediaPointsForDifficulty(difficulty)
		if points > 0 {
			details := fmt.Sprintf("source=%s difficulty=%d", sourceType, difficulty)
			if err := a.recordUserPoints(album.OwnerUserID, points, "media_approve", "media_album", album.ID, &user.ID, details); err != nil {
				Errorf("Error afegint punts album media %d: %v", album.ID, err)
			}
		}
	}
	http.Redirect(w, r, "/admin/moderacio/media?ok=1", http.StatusSeeOther)
}

func (a *App) AdminModeracioMediaAlbumReject(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permModerate); !ok {
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
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/admin/moderacio/media?err=1", http.StatusSeeOther)
		return
	}
	user := userFromContext(r)
	if user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	album, err := a.DB.GetMediaAlbumByID(id)
	if err != nil || album == nil {
		http.NotFound(w, r)
		return
	}
	restrictedGroupID := 0
	if album.RestrictedGroupID.Valid {
		restrictedGroupID = int(album.RestrictedGroupID.Int64)
	}
	accessPolicyID := 0
	if album.AccessPolicyID.Valid {
		accessPolicyID = int(album.AccessPolicyID.Int64)
	}
	notes := strings.TrimSpace(r.FormValue("notes"))
	if err := a.DB.UpdateMediaAlbumModeration(id, "rejected", album.Visibility, restrictedGroupID, accessPolicyID, album.CreditCost, album.DifficultyScore, album.SourceType, notes, user.ID); err != nil {
		Errorf("Moderacio media album rebutjar %d ha fallat: %v", id, err)
		http.Redirect(w, r, "/admin/moderacio/media?err=1", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/admin/moderacio/media?ok=1", http.StatusSeeOther)
}

func (a *App) AdminModeracioMediaItemApprove(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permModerate); !ok {
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
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/admin/moderacio/media?err=1", http.StatusSeeOther)
		return
	}
	user := userFromContext(r)
	if user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	creditCost := parseIntDefault(r.FormValue("credit_cost"), 0)
	if creditCost < 0 {
		http.Redirect(w, r, "/admin/moderacio/media?err=1", http.StatusSeeOther)
		return
	}
	notes := strings.TrimSpace(r.FormValue("notes"))

	itemPublicID := strings.TrimSpace(r.FormValue("item_public_id"))
	var item *db.MediaItem
	var album *db.MediaAlbum
	wasApproved := false
	if itemPublicID != "" {
		if row, err := a.DB.GetMediaItemByPublicID(itemPublicID); err == nil && row != nil && row.ID == id {
			item = row
			wasApproved = row.ModerationStatus == "approved"
			if aRow, err := a.DB.GetMediaAlbumByID(row.AlbumID); err == nil && aRow != nil {
				album = aRow
			}
		}
	}
	if err := a.DB.UpdateMediaItemModeration(id, "approved", creditCost, notes, user.ID); err != nil {
		Errorf("Moderacio media item aprovar %d ha fallat: %v", id, err)
		http.Redirect(w, r, "/admin/moderacio/media?err=1", http.StatusSeeOther)
		return
	}
	if !wasApproved && item != nil && album != nil && album.OwnerUserID > 0 {
		points := a.mediaPointsForDifficulty(album.DifficultyScore)
		if points > 0 {
			details := fmt.Sprintf("source=%s difficulty=%d", album.SourceType, album.DifficultyScore)
			if err := a.recordUserPoints(album.OwnerUserID, points, "media_approve", "media_item", item.ID, &user.ID, details); err != nil {
				Errorf("Error afegint punts item media %d: %v", item.ID, err)
			}
		}
	}
	http.Redirect(w, r, "/admin/moderacio/media?ok=1", http.StatusSeeOther)
}

func (a *App) AdminModeracioMediaItemReject(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permModerate); !ok {
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
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/admin/moderacio/media?err=1", http.StatusSeeOther)
		return
	}
	user := userFromContext(r)
	if user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	creditCost := parseIntDefault(r.FormValue("credit_cost"), 0)
	if creditCost < 0 {
		http.Redirect(w, r, "/admin/moderacio/media?err=1", http.StatusSeeOther)
		return
	}
	notes := strings.TrimSpace(r.FormValue("notes"))
	if err := a.DB.UpdateMediaItemModeration(id, "rejected", creditCost, notes, user.ID); err != nil {
		Errorf("Moderacio media item rebutjar %d ha fallat: %v", id, err)
		http.Redirect(w, r, "/admin/moderacio/media?err=1", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/admin/moderacio/media?ok=1", http.StatusSeeOther)
}

func userDisplayName(u *db.User) string {
	if u == nil {
		return "-"
	}
	name := strings.TrimSpace(u.Usuari)
	if name == "" {
		full := strings.TrimSpace(strings.TrimSpace(u.Name) + " " + strings.TrimSpace(u.Surname))
		if full != "" {
			name = full
		}
	}
	if name == "" {
		name = "-"
	}
	return name
}
