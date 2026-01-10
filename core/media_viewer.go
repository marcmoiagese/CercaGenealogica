package core

import (
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func (a *App) MediaDeepZoom(w http.ResponseWriter, r *http.Request) {
	cfg := a.mediaConfig()
	if !cfg.Enabled {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	itemPublicID, tail := splitMediaDZPath(r.URL.Path)
	if itemPublicID == "" || tail == "" {
		http.NotFound(w, r)
		return
	}

	item, err := a.DB.GetMediaItemByPublicID(itemPublicID)
	if err != nil || item == nil {
		http.NotFound(w, r)
		return
	}
	album, err := a.DB.GetMediaAlbumByID(item.AlbumID)
	if err != nil || album == nil {
		http.NotFound(w, r)
		return
	}
	if !a.mediaUserCanAccessItem(r, user, album, item) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if !a.mediaValidateGrantToken(user, item, r.URL.Query().Get("t")) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	baseDir := filepath.Join(cfg.Root, album.PublicID, item.PublicID, "dz")
	w.Header().Set("Cache-Control", "private, max-age=31536000, immutable")

	if tail == "dz.dzi" {
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		http.ServeFile(w, r, filepath.Join(baseDir, "dz.dzi"))
		return
	}
	if strings.HasPrefix(tail, "dz_files/") {
		level, tileName, ok := parseDZIFilePath(strings.TrimPrefix(tail, "dz_files/"))
		if !ok {
			http.NotFound(w, r)
			return
		}
		tilePath := filepath.Join(baseDir, "dz_files", strconv.Itoa(level), tileName)
		http.ServeFile(w, r, tilePath)
		return
	}

	http.NotFound(w, r)
}

func (a *App) mediaItemView(w http.ResponseWriter, r *http.Request, cfg mediaConfig, itemPublicID string) {
	user := a.mediaEnsureUser(r)
	if user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	item, err := a.DB.GetMediaItemByPublicID(itemPublicID)
	if err != nil || item == nil {
		http.NotFound(w, r)
		return
	}
	album, err := a.DB.GetMediaAlbumByID(item.AlbumID)
	if err != nil || album == nil {
		http.NotFound(w, r)
		return
	}
	if !a.mediaUserCanAccessItem(r, user, album, item) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	items, err := a.DB.ListMediaItemsByAlbum(album.ID)
	if err != nil {
		Errorf("Error carregant media items per visor: %v", err)
		items = []db.MediaItem{}
	}
	isPrivileged := a.mediaUserIsPrivileged(r, user, album)
	if !isPrivileged {
		filtered := make([]db.MediaItem, 0, len(items))
		for _, it := range items {
			if it.ModerationStatus == "approved" {
				filtered = append(filtered, it)
			}
		}
		items = filtered
	}
	currentIndex := 0
	prevItemID := ""
	nextItemID := ""
	for i := range items {
		if items[i].ID == item.ID {
			currentIndex = i + 1
			if i > 0 {
				prevItemID = items[i-1].PublicID
			}
			if i < len(items)-1 {
				nextItemID = items[i+1].PublicID
			}
			break
		}
	}
	if len(items) == 0 {
		items = []db.MediaItem{*item}
		currentIndex = 1
	} else if currentIndex == 0 {
		currentIndex = 1
	}

	lang := resolveUserLang(r, user)
	message := ""
	if item.DerivativesStatus != "ready" {
		if item.DerivativesStatus == "failed" {
			message = T(lang, "media.viewer.failed")
		} else {
			message = T(lang, "media.viewer.processing")
		}
	}
	grantToken := ""
	if message == "" {
		cost := mediaCreditCost(album, item)
		if isPrivileged {
			cost = 0
		}
		grant, err := a.mediaEnsureAccessGrant(user, item, cost)
		if err != nil {
			if err == errInsufficientCredits {
				http.Error(w, T(lang, "media.viewer.insufficient_credits"), http.StatusPaymentRequired)
				return
			}
			Errorf("Error generant grant media: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		grantToken = grant.GrantToken
	}

	payload := map[string]interface{}{
		"User":          user,
		"Album":         album,
		"Item":          item,
		"AlbumItems":    items,
		"CurrentIndex":  currentIndex,
		"TotalItems":    len(items),
		"PrevItemID":    prevItemID,
		"NextItemID":    nextItemID,
		"StatusMessage": message,
		"GrantToken":    grantToken,
		"IsOwner":       user != nil && album.OwnerUserID == user.ID,
	}
	if user != nil {
		RenderPrivateTemplateLang(w, r, "media-viewer.html", lang, payload)
		return
	}
	RenderTemplate(w, r, "media-viewer.html", payload)
}

func splitMediaDZPath(pathValue string) (string, string) {
	trimmed := strings.TrimPrefix(pathValue, "/media/dz/")
	trimmed = strings.Trim(trimmed, "/")
	if trimmed == "" {
		return "", ""
	}
	parts := strings.Split(trimmed, "/")
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], strings.Join(parts[1:], "/")
}

func parseDZIFilePath(pathValue string) (int, string, bool) {
	parts := strings.Split(pathValue, "/")
	if len(parts) != 2 {
		return 0, "", false
	}
	level, err := strconv.Atoi(parts[0])
	if err != nil || level < 0 {
		return 0, "", false
	}
	file := parts[1]
	if !strings.HasSuffix(file, ".jpg") {
		return 0, "", false
	}
	name := strings.TrimSuffix(file, ".jpg")
	coords := strings.Split(name, "_")
	if len(coords) != 2 {
		return 0, "", false
	}
	if _, err := strconv.Atoi(coords[0]); err != nil {
		return 0, "", false
	}
	if _, err := strconv.Atoi(coords[1]); err != nil {
		return 0, "", false
	}
	return level, file, true
}
