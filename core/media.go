package core

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	_ "image/png"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	_ "golang.org/x/image/tiff"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const (
	mediaDefaultRoot        = "./data/media"
	mediaDefaultMaxUploadMB = 200
	mediaThumbMaxSize       = 320
)

var defaultMediaMimes = []string{"image/jpeg", "image/png", "image/tiff"}

var mediaAlbumTypes = map[string]bool{
	"book":     true,
	"memorial": true,
	"photo":    true,
	"achievement_icon": true,
	"other":    true,
}

var mediaSourceTypes = map[string]bool{
	"online":          true,
	"offline_archive": true,
	"family_private":  true,
	"other":           true,
}

var mediaVisibilityTypes = map[string]bool{
	"private":          true,
	"registered":       true,
	"public":           true,
	"restricted_group": true,
	"admins_only":      true,
	"custom_policy":    true,
}

type mediaConfig struct {
	Enabled        bool
	Root           string
	MaxUploadMB    int
	MaxUploadBytes int64
	AllowedMimes   map[string]bool
	AllowedList    []string
	AllowedCSV     string
}

func (a *App) mediaConfig() mediaConfig {
	cfg := mediaConfig{}
	raw := map[string]string{}
	if a != nil && a.Config != nil {
		raw = a.Config
	}

	cfg.Enabled = parseBoolDefault(raw["MEDIA_ENABLED"], true)
	cfg.Root = strings.TrimSpace(raw["MEDIA_ROOT"])
	if cfg.Root == "" {
		cfg.Root = mediaDefaultRoot
	}

	cfg.MaxUploadMB = parseIntDefault(raw["MEDIA_MAX_UPLOAD_MB"], mediaDefaultMaxUploadMB)
	if cfg.MaxUploadMB <= 0 {
		cfg.MaxUploadMB = mediaDefaultMaxUploadMB
	}
	cfg.MaxUploadBytes = int64(cfg.MaxUploadMB) * 1024 * 1024

	allowed := strings.TrimSpace(raw["MEDIA_ALLOWED_MIME"])
	list := parseCSVDefault(allowed, defaultMediaMimes)
	cfg.AllowedMimes = make(map[string]bool, len(list))
	normalized := make([]string, 0, len(list))
	for _, v := range list {
		v = strings.ToLower(strings.TrimSpace(v))
		if v == "" {
			continue
		}
		cfg.AllowedMimes[v] = true
		normalized = append(normalized, v)
	}
	if len(normalized) == 0 {
		normalized = append([]string{}, defaultMediaMimes...)
	}
	cfg.AllowedList = normalized
	cfg.AllowedCSV = strings.Join(normalized, ",")

	return cfg
}

func parseBoolDefault(val string, fallback bool) bool {
	val = strings.TrimSpace(val)
	if val == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(strings.ToLower(val))
	if err != nil {
		return fallback
	}
	return parsed
}

func parseIntDefault(val string, fallback int) int {
	val = strings.TrimSpace(val)
	if val == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseCSVDefault(val string, fallback []string) []string {
	if strings.TrimSpace(val) == "" {
		return append([]string{}, fallback...)
	}
	parts := strings.Split(val, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return append([]string{}, fallback...)
	}
	return out
}

func (a *App) MediaAlbums(w http.ResponseWriter, r *http.Request) {
	cfg := a.mediaConfig()
	if !cfg.Enabled {
		http.NotFound(w, r)
		return
	}
	if _, ok := a.requireMediaView(w, r); !ok {
		return
	}
	switch r.Method {
	case http.MethodGet:
		a.mediaAlbumsList(w, r, cfg)
	case http.MethodPost:
		a.mediaAlbumCreate(w, r, cfg)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a *App) MediaAlbumNew(w http.ResponseWriter, r *http.Request) {
	cfg := a.mediaConfig()
	if !cfg.Enabled {
		http.NotFound(w, r)
		return
	}
	user, ok := a.requireMediaView(w, r)
	if !ok {
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	lang := resolveUserLang(r, user)
	perms, _ := a.permissionsFromContext(r)
	RenderPrivateTemplateLang(w, r, "media-albums-form.html", lang, map[string]interface{}{
		"User":           user,
		"AlbumTypes":     mediaAlbumTypeListForPerms(perms),
		"SourceTypes":    mediaSourceTypeList(),
		"FormTitle":      "",
		"FormType":       "other",
		"FormSource":     "online",
		"FormDesc":       "",
		"FormError":      "",
		"FormLlibreID":   0,
		"FormLlibreLabel": "",
		"MaxUploadMB":    cfg.MaxUploadMB,
		"AllowedMimeCSV": cfg.AllowedCSV,
	})
}

func (a *App) MediaLlibresSearchJSON(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user := userFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	perms, _ := a.permissionsFromContext(r)
	if !perms.Admin && !perms.CanManageArchives && !a.hasAnyPermissionKey(user.ID, permKeyDocumentalsLlibresView) {
		writeJSON(w, map[string]interface{}{"items": []interface{}{}})
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	cronologia := strings.TrimSpace(r.URL.Query().Get("cronologia"))
	arxiuID := parseIntDefault(r.URL.Query().Get("arxiu_id"), 0)
	eclesID := parseIntDefault(r.URL.Query().Get("ecles_id"), 0)
	municipiID := parseIntDefault(r.URL.Query().Get("municipi_id"), 0)
	limit := parseIntDefault(r.URL.Query().Get("limit"), 20)
	if limit <= 0 {
		limit = 20
	}
	if limit > 50 {
		limit = 50
	}
	filter := db.LlibreFilter{
		Text:          query,
		Cronologia:    cronologia,
		ArxiuID:       arxiuID,
		ArquebisbatID: eclesID,
		MunicipiID:    municipiID,
		Limit:         limit,
	}
	if !perms.Admin && !perms.CanManageArchives {
		filter.Status = "publicat"
	}
	scope := a.buildListScopeFilter(user.ID, permKeyDocumentalsLlibresView, ScopeLlibre)
	if perms.Admin || perms.CanManageArchives {
		scope.hasGlobal = true
	}
	if !scope.hasGlobal {
		filter.AllowedLlibreIDs = scope.llibreIDs
		filter.AllowedArxiuIDs = scope.arxiuIDs
		filter.AllowedMunicipiIDs = scope.municipiIDs
		filter.AllowedProvinciaIDs = scope.provinciaIDs
		filter.AllowedComarcaIDs = scope.comarcaIDs
		filter.AllowedNivellIDs = scope.nivellIDs
		filter.AllowedPaisIDs = scope.paisIDs
		filter.AllowedEclesIDs = scope.eclesIDs
	}
	if !scope.hasGlobal &&
		len(filter.AllowedLlibreIDs) == 0 &&
		len(filter.AllowedArxiuIDs) == 0 &&
		len(filter.AllowedMunicipiIDs) == 0 &&
		len(filter.AllowedProvinciaIDs) == 0 &&
		len(filter.AllowedComarcaIDs) == 0 &&
		len(filter.AllowedNivellIDs) == 0 &&
		len(filter.AllowedPaisIDs) == 0 &&
		len(filter.AllowedEclesIDs) == 0 {
		writeJSON(w, map[string]interface{}{"items": []interface{}{}})
		return
	}
	rows, err := a.DB.ListLlibres(filter)
	if err != nil {
		Errorf("MediaLlibresSearchJSON error: %v", err)
		writeJSON(w, map[string]interface{}{"items": []interface{}{}})
		return
	}
	items := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		label := strings.TrimSpace(row.Titol)
		if label == "" {
			label = strings.TrimSpace(row.NomEsglesia)
		}
		context := joinNonEmpty(strings.TrimSpace(row.Cronologia), strings.TrimSpace(row.NomEsglesia), " · ")
		if row.MunicipiNom.Valid {
			context = joinNonEmpty(context, strings.TrimSpace(row.MunicipiNom.String), " · ")
		}
		if row.ArquebisbatNom.Valid {
			context = joinNonEmpty(context, strings.TrimSpace(row.ArquebisbatNom.String), " · ")
		}
		items = append(items, map[string]interface{}{
			"id":         row.ID,
			"nom":        label,
			"context":    strings.TrimSpace(context),
			"cronologia": strings.TrimSpace(row.Cronologia),
		})
	}
	writeJSON(w, map[string]interface{}{"items": items})
}

func (a *App) MediaLlibrePaginesSuggestJSON(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user := userFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	llibreID := parseIntDefault(r.URL.Query().Get("llibre_id"), 0)
	if llibreID <= 0 {
		writeJSON(w, map[string]interface{}{"items": []interface{}{}})
		return
	}
	perms, _ := a.permissionsFromContext(r)
	target := a.resolveLlibreTarget(llibreID)
	if !perms.Admin && !perms.CanManageArchives && !a.HasPermission(user.ID, permKeyDocumentalsLlibresView, target) {
		writeJSON(w, map[string]interface{}{"items": []interface{}{}})
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	limit := parseIntDefault(r.URL.Query().Get("limit"), 10)
	if limit <= 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}
	pages, err := a.DB.SearchLlibrePagines(llibreID, query, limit)
	if err != nil {
		Errorf("MediaLlibrePaginesSuggestJSON error: %v", err)
		writeJSON(w, map[string]interface{}{"items": []interface{}{}})
		return
	}
	if len(pages) == 0 {
		if err := a.DB.RecalcTranscripcionsRawPageStats(llibreID); err == nil {
			pages, _ = a.DB.SearchLlibrePagines(llibreID, query, limit)
		}
	}
	items := make([]map[string]interface{}, 0, len(pages))
	for _, page := range pages {
		label := strings.TrimSpace(page.Notes)
		if label == "" && page.NumPagina > 0 {
			label = strconv.Itoa(page.NumPagina)
		}
		if label == "" {
			continue
		}
		items = append(items, map[string]interface{}{
			"id":    page.ID,
			"label": label,
			"num":   page.NumPagina,
		})
	}
	if len(items) == 0 && query != "" {
		if n, err := strconv.Atoi(query); err == nil {
			items = append(items, map[string]interface{}{
				"id":    0,
				"label": query,
				"num":   n,
			})
		}
	}
	writeJSON(w, map[string]interface{}{"items": items})
}

func (a *App) MediaAlbumDetail(w http.ResponseWriter, r *http.Request) {
	cfg := a.mediaConfig()
	if !cfg.Enabled {
		http.NotFound(w, r)
		return
	}
	if _, ok := a.requireMediaViewIfLogged(w, r); !ok {
		return
	}
	albumPublicID, tail := splitMediaAlbumPath(r.URL.Path)
	if albumPublicID == "" {
		http.NotFound(w, r)
		return
	}
	if tail == "upload" {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		a.mediaAlbumUpload(w, r, cfg, albumPublicID)
		return
	}
	if tail == "pages/link" {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		a.mediaAlbumPageLink(w, r, albumPublicID)
		return
	}
	if tail == "pages/unlink" {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		a.mediaAlbumPageUnlink(w, r, albumPublicID)
		return
	}
	if tail != "" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	a.mediaAlbumShow(w, r, cfg, albumPublicID)
}

func (a *App) MediaItemRoute(w http.ResponseWriter, r *http.Request) {
	cfg := a.mediaConfig()
	if !cfg.Enabled {
		http.NotFound(w, r)
		return
	}
	if _, ok := a.requireMediaViewIfLogged(w, r); !ok {
		return
	}
	itemPublicID, tail := splitMediaItemPath(r.URL.Path)
	if itemPublicID == "" {
		http.NotFound(w, r)
		return
	}
	if tail == "thumb" {
		a.mediaItemThumb(w, r, cfg, itemPublicID)
		return
	}
	if tail == "data" {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		a.mediaItemViewData(w, r, cfg, itemPublicID)
		return
	}
	if tail != "" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	a.mediaItemView(w, r, cfg, itemPublicID)
}

func (a *App) mediaAlbumsList(w http.ResponseWriter, r *http.Request, cfg mediaConfig) {
	user := userFromContext(r)
	if user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	lang := resolveUserLang(r, user)
	albums, err := a.DB.ListMediaAlbumsByOwner(user.ID)
	if err != nil {
		Errorf("Error carregant media albums: %v", err)
		albums = []db.MediaAlbum{}
	}
	RenderPrivateTemplateLang(w, r, "media-albums-list.html", lang, map[string]interface{}{
		"User":   user,
		"Albums": albums,
	})
}

func (a *App) mediaAlbumCreate(w http.ResponseWriter, r *http.Request, cfg mediaConfig) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/media/albums", http.StatusSeeOther)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	user := userFromContext(r)
	if user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	perms, _ := a.permissionsFromContext(r)

	title := strings.TrimSpace(r.FormValue("title"))
	desc := strings.TrimSpace(r.FormValue("description"))
	albumType := normalizeMediaAlbumType(r.FormValue("album_type"))
	sourceType := normalizeMediaSourceType(r.FormValue("source_type"))
	llibreID := parseIntDefault(r.FormValue("llibre_id"), 0)
	llibreLabel := strings.TrimSpace(r.FormValue("llibre_label"))
	if albumType == "achievement_icon" && !perms.Admin {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	if title == "" {
		a.renderMediaAlbumForm(w, r, user, cfg, title, albumType, sourceType, desc, llibreID, llibreLabel, T(resolveUserLang(r, user), "media.error.title_required"))
		return
	}
	if llibreID > 0 {
		target := a.resolveLlibreTarget(llibreID)
		if !perms.Admin && !perms.CanManageArchives && !a.HasPermission(user.ID, permKeyDocumentalsLlibresView, target) {
			a.renderMediaAlbumForm(w, r, user, cfg, title, albumType, sourceType, desc, llibreID, llibreLabel, T(resolveUserLang(r, user), "media.error.book_permission"))
			return
		}
	}

	album := &db.MediaAlbum{
		PublicID:         generateMediaPublicID(),
		Title:            title,
		Description:      desc,
		AlbumType:        albumType,
		OwnerUserID:      user.ID,
		LlibreID:         sql.NullInt64{Int64: int64(llibreID), Valid: llibreID > 0},
		ModerationStatus: "pending",
		Visibility:       "private",
		CreditCost:       0,
		DifficultyScore:  0,
		SourceType:       sourceType,
	}
	if albumType == "achievement_icon" {
		album.Visibility = "admins_only"
		album.ModerationStatus = "approved"
	}
	if _, err := a.DB.CreateMediaAlbum(album); err != nil {
		Errorf("Error creant media album: %v", err)
		a.renderMediaAlbumForm(w, r, user, cfg, title, albumType, sourceType, desc, llibreID, llibreLabel, T(resolveUserLang(r, user), "media.error.create_failed"))
		return
	}

	http.Redirect(w, r, "/media/albums/"+album.PublicID, http.StatusSeeOther)
}

func (a *App) mediaAlbumShow(w http.ResponseWriter, r *http.Request, cfg mediaConfig, albumPublicID string) {
	user := a.mediaEnsureUser(r)
	album, err := a.DB.GetMediaAlbumByPublicID(albumPublicID)
	if err != nil || album == nil {
		http.NotFound(w, r)
		return
	}
	if !a.mediaUserCanAccess(r, user, album) {
		if user == nil {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
		} else {
			http.Error(w, "Forbidden", http.StatusForbidden)
		}
		return
	}
	items, err := a.DB.ListMediaItemsByAlbum(album.ID)
	if err != nil {
		Errorf("Error carregant media items: %v", err)
		items = []db.MediaItem{}
	}
	isOwner := user != nil && album.OwnerUserID == user.ID
	isPrivileged := a.mediaUserIsPrivileged(r, user, album)
	if !isPrivileged {
		filtered := make([]db.MediaItem, 0, len(items))
		for _, item := range items {
			if item.ModerationStatus == "approved" {
				filtered = append(filtered, item)
			}
		}
		items = filtered
	}
	perms, _ := a.permissionsFromContext(r)
	var linkedBook *db.Llibre
	linkedBookLabel := ""
	canViewBook := false
	if album.LlibreID.Valid {
		if book, err := a.DB.GetLlibre(int(album.LlibreID.Int64)); err == nil && book != nil {
			linkedBook = book
			linkedBookLabel = strings.TrimSpace(book.Titol)
			if linkedBookLabel == "" {
				linkedBookLabel = strings.TrimSpace(book.NomEsglesia)
			}
		}
		if user != nil {
			target := a.resolveLlibreTarget(int(album.LlibreID.Int64))
			if perms.Admin || perms.CanManageArchives || a.HasPermission(user.ID, permKeyDocumentalsLlibresView, target) {
				canViewBook = true
			}
		}
	}
	if !canViewBook {
		linkedBook = nil
		linkedBookLabel = ""
	}
	canLinkPages := canViewBook && a.mediaUserIsPrivileged(r, user, album)
	linkCounts := map[int]int{}
	if len(items) > 0 {
		if counts, err := a.DB.CountMediaItemLinksByAlbum(album.ID); err == nil {
			linkCounts = counts
		}
	}
	itemLinks := map[int][]db.MediaItemPageLink{}
	if linkedBook != nil && canViewBook {
		if canLinkPages && linkedBook.Pagines.Valid && linkedBook.Pagines.Int64 > 0 {
			if pages, err := a.DB.ListLlibrePagines(linkedBook.ID); err == nil && len(pages) == 0 {
				if err := a.DB.RecalcLlibrePagines(linkedBook.ID, int(linkedBook.Pagines.Int64)); err != nil {
					Errorf("Error recalculant pagines llibre %d: %v", linkedBook.ID, err)
				}
			}
		}
		if links, err := a.DB.ListMediaItemLinksByAlbum(album.ID); err == nil {
			if !isPrivileged {
				links = a.filterMediaItemLinks(r, user, links)
			}
			itemIDs := map[int]struct{}{}
			for _, item := range items {
				itemIDs[item.ID] = struct{}{}
			}
			for _, link := range links {
				if _, ok := itemIDs[link.MediaItemID]; !ok {
					continue
				}
				itemLinks[link.MediaItemID] = append(itemLinks[link.MediaItemID], link)
			}
		}
	}
	uploaded := 0
	failed := 0
	if isOwner {
		uploaded = parseIntDefault(r.URL.Query().Get("uploaded"), 0)
		failed = parseIntDefault(r.URL.Query().Get("failed"), 0)
	}

	lang := resolveUserLang(r, user)
	payload := map[string]interface{}{
		"User":           user,
		"Album":          album,
		"Items":          items,
		"ItemLinkCounts": linkCounts,
		"ItemLinks":      itemLinks,
		"Uploaded":       uploaded,
		"Failed":         failed,
		"AllowedMimeCSV": cfg.AllowedCSV,
		"MaxUploadMB":    cfg.MaxUploadMB,
		"CanUpload":      isOwner,
		"IsOwner":        isOwner,
		"LinkedBook":     linkedBook,
		"LinkedBookLabel": linkedBookLabel,
		"CanViewBook":    canViewBook,
		"CanLinkPages":   canLinkPages,
		"BookURLBase":    "",
		"BookURLPrefix":  "",
	}
	if linkedBook != nil {
		payload["BookURLBase"] = strings.TrimSpace(linkedBook.URLBase)
		payload["BookURLPrefix"] = strings.TrimSpace(linkedBook.URLImatgePrefix)
	}
	if user != nil {
		RenderPrivateTemplateLang(w, r, "media-albums-show.html", lang, payload)
		return
	}
	RenderTemplate(w, r, "media-albums-show.html", payload)
}

func (a *App) mediaAlbumUpload(w http.ResponseWriter, r *http.Request, cfg mediaConfig, albumPublicID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	user := a.mediaEnsureUser(r)
	if user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	album, err := a.DB.GetMediaAlbumByPublicID(albumPublicID)
	if err != nil || album == nil || album.OwnerUserID != user.ID {
		http.NotFound(w, r)
		return
	}

	if cfg.MaxUploadBytes > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, cfg.MaxUploadBytes)
	}
	if err := r.ParseMultipartForm(cfg.MaxUploadBytes); err != nil {
		http.Error(w, "Upload too large or invalid", http.StatusRequestEntityTooLarge)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if r.MultipartForm == nil || r.MultipartForm.File == nil {
		http.Error(w, "No files provided", http.StatusBadRequest)
		return
	}

	files := r.MultipartForm.File["media_files"]
	if len(files) == 0 {
		http.Error(w, "No files provided", http.StatusBadRequest)
		return
	}

	created := 0
	failed := 0
	for _, header := range files {
		if err := a.saveMediaItemFromUpload(cfg, album, header); err != nil {
			Errorf("Error pujant media: %v", err)
			failed++
			continue
		}
		created++
	}

	target := fmt.Sprintf("/media/albums/%s?uploaded=%d&failed=%d", album.PublicID, created, failed)
	http.Redirect(w, r, target, http.StatusSeeOther)
}

func (a *App) mediaAlbumPageLink(w http.ResponseWriter, r *http.Request, albumPublicID string) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	user := a.mediaEnsureUser(r)
	if user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	album, err := a.DB.GetMediaAlbumByPublicID(albumPublicID)
	if err != nil || album == nil {
		http.NotFound(w, r)
		return
	}
	if !a.mediaUserIsPrivileged(r, user, album) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if !album.LlibreID.Valid {
		http.Redirect(w, r, "/media/albums/"+album.PublicID, http.StatusSeeOther)
		return
	}
	perms, _ := a.permissionsFromContext(r)
	target := a.resolveLlibreTarget(int(album.LlibreID.Int64))
	if !perms.Admin && !perms.CanManageArchives && !a.HasPermission(user.ID, permKeyDocumentalsLlibresView, target) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	itemPublicID := strings.TrimSpace(r.FormValue("media_item_public_id"))
	paginaIDText := strings.TrimSpace(r.FormValue("pagina_id"))
	paginaNumText := strings.TrimSpace(r.FormValue("pagina_num"))
	paginaID := parseIntDefault(paginaIDText, 0)
	paginaNum := parseIntDefault(paginaNumText, 0)
	if itemPublicID == "" || (paginaIDText == "" && paginaNumText == "") {
		Errorf("mediaAlbumPageLink: dades incompletes item=%q pagina_id=%q pagina_num=%q", itemPublicID, paginaIDText, paginaNumText)
		http.Redirect(w, r, "/media/albums/"+album.PublicID, http.StatusSeeOther)
		return
	}
	item, err := a.DB.GetMediaItemByPublicID(itemPublicID)
	if err != nil || item == nil || item.AlbumID != album.ID {
		if err != nil {
			Errorf("mediaAlbumPageLink: error item %q: %v", itemPublicID, err)
		} else {
			Errorf("mediaAlbumPageLink: item no valid %q album=%d", itemPublicID, album.ID)
		}
		http.Redirect(w, r, "/media/albums/"+album.PublicID, http.StatusSeeOther)
		return
	}
	var page *db.LlibrePagina
	if paginaID > 0 {
		page, err = a.DB.GetLlibrePaginaByID(paginaID)
		if err != nil || page == nil || page.LlibreID != int(album.LlibreID.Int64) {
			if err != nil {
				Errorf("mediaAlbumPageLink: error pagina id=%d: %v", paginaID, err)
			} else {
				Errorf("mediaAlbumPageLink: pagina id=%d no valida llibre=%d", paginaID, int(album.LlibreID.Int64))
			}
			http.Redirect(w, r, "/media/albums/"+album.PublicID, http.StatusSeeOther)
			return
		}
	} else {
		page, err = a.DB.GetLlibrePaginaByNum(int(album.LlibreID.Int64), paginaNum)
		if err != nil && err != sql.ErrNoRows {
			Errorf("Error cercant pagina llibre %d num %d: %v", int(album.LlibreID.Int64), paginaNum, err)
		}
		if page == nil {
			page = &db.LlibrePagina{
				LlibreID:  int(album.LlibreID.Int64),
				NumPagina: paginaNum,
				Estat:     "pendent",
			}
			pageID, saveErr := a.DB.SaveLlibrePagina(page)
			if saveErr != nil {
				Errorf("Error creant pagina llibre %d num %d: %v", int(album.LlibreID.Int64), paginaNum, saveErr)
			}
			if pageID == 0 {
				if existing, err := a.DB.GetLlibrePaginaByNum(int(album.LlibreID.Int64), paginaNum); err == nil && existing != nil {
					page = existing
				} else {
					if err != nil && err != sql.ErrNoRows {
						Errorf("Error reintentant pagina llibre %d num %d: %v", int(album.LlibreID.Int64), paginaNum, err)
					}
					http.Redirect(w, r, "/media/albums/"+album.PublicID, http.StatusSeeOther)
					return
				}
			} else {
				page.ID = pageID
			}
		}
	}
	pageOrder := parseIntDefault(r.FormValue("page_order"), 0)
	notes := strings.TrimSpace(r.FormValue("notes"))
	if err := a.DB.UpsertMediaItemPageLink(item.ID, page.LlibreID, page.ID, pageOrder, notes); err != nil {
		Errorf("Error vinculant media item %d a pagina %d: %v", item.ID, page.ID, err)
	}
	returnTo := safeReturnTo(r.FormValue("return_to"), "/media/albums/"+album.PublicID)
	http.Redirect(w, r, returnTo, http.StatusSeeOther)
}

func (a *App) mediaAlbumPageUnlink(w http.ResponseWriter, r *http.Request, albumPublicID string) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	user := a.mediaEnsureUser(r)
	if user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	album, err := a.DB.GetMediaAlbumByPublicID(albumPublicID)
	if err != nil || album == nil {
		http.NotFound(w, r)
		return
	}
	if !a.mediaUserIsPrivileged(r, user, album) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	itemPublicID := strings.TrimSpace(r.FormValue("media_item_public_id"))
	paginaID := parseIntDefault(r.FormValue("pagina_id"), 0)
	if itemPublicID == "" || paginaID == 0 {
		http.Redirect(w, r, "/media/albums/"+album.PublicID, http.StatusSeeOther)
		return
	}
	item, err := a.DB.GetMediaItemByPublicID(itemPublicID)
	if err != nil || item == nil || item.AlbumID != album.ID {
		http.Redirect(w, r, "/media/albums/"+album.PublicID, http.StatusSeeOther)
		return
	}
	if err := a.DB.DeleteMediaItemPageLink(item.ID, paginaID); err != nil {
		Errorf("Error desvinculant media item %d de pagina %d: %v", item.ID, paginaID, err)
	}
	returnTo := safeReturnTo(r.FormValue("return_to"), "/media/albums/"+album.PublicID)
	http.Redirect(w, r, returnTo, http.StatusSeeOther)
}

func (a *App) mediaItemThumb(w http.ResponseWriter, r *http.Request, cfg mediaConfig, itemPublicID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	user := a.mediaEnsureUser(r)
	item, err := a.DB.GetMediaItemByPublicID(itemPublicID)
	if err != nil || item == nil {
		http.NotFound(w, r)
		return
	}
	album, err := a.DB.GetMediaAlbumByID(item.AlbumID)
	if err != nil || album == nil || !a.mediaUserCanAccessItem(r, user, album, item) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if strings.TrimSpace(item.ThumbPath) == "" {
		http.NotFound(w, r)
		return
	}
	thumbPath := filepath.Join(cfg.Root, filepath.FromSlash(item.ThumbPath))
	if _, err := os.Stat(thumbPath); err != nil {
		http.NotFound(w, r)
		return
	}
	http.ServeFile(w, r, thumbPath)
}

func (a *App) saveMediaItemFromUpload(cfg mediaConfig, album *db.MediaAlbum, header *multipart.FileHeader) error {
	if header == nil {
		return errors.New("empty file")
	}
	if header.Size <= 0 {
		return errors.New("empty file")
	}
	if cfg.MaxUploadBytes > 0 && header.Size > cfg.MaxUploadBytes {
		return fmt.Errorf("file too large: %d", header.Size)
	}

	file, err := header.Open()
	if err != nil {
		return err
	}
	defer file.Close()

	sniff := make([]byte, 512)
	n, err := io.ReadFull(file, sniff)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return err
	}
	sniff = sniff[:n]
	detected := http.DetectContentType(sniff)
	if parsed, _, err := mime.ParseMediaType(detected); err == nil {
		detected = parsed
	}
	detected = strings.ToLower(strings.TrimSpace(detected))
	if !cfg.AllowedMimes[detected] {
		return fmt.Errorf("mime not allowed: %s", detected)
	}

	safeName := sanitizeFilename(header.Filename)
	ext := extensionForMime(detected)
	if safeName == "" {
		safeName = "upload" + ext
	} else if filepath.Ext(safeName) == "" && ext != "" {
		safeName += ext
	}

	itemPublicID := generateMediaPublicID()
	relOriginal := path.Join(album.PublicID, itemPublicID, "original", safeName)
	absOriginal := filepath.Join(cfg.Root, filepath.FromSlash(relOriginal))
	if err := os.MkdirAll(filepath.Dir(absOriginal), 0o755); err != nil {
		return err
	}

	dest, err := os.Create(absOriginal)
	if err != nil {
		return err
	}
	defer dest.Close()

	hash := sha256.New()
	reader := io.MultiReader(bytes.NewReader(sniff), file)
	size, err := io.Copy(io.MultiWriter(dest, hash), reader)
	if err != nil {
		_ = os.Remove(absOriginal)
		return err
	}
	checksum := hex.EncodeToString(hash.Sum(nil))

	width, height, thumbRel, err := createMediaThumbnail(cfg, absOriginal, album.PublicID, itemPublicID)
	if err != nil {
		_ = os.Remove(absOriginal)
		return err
	}

	title := strings.TrimSuffix(safeName, filepath.Ext(safeName))
	item := &db.MediaItem{
		PublicID:           itemPublicID,
		AlbumID:            album.ID,
		Title:              title,
		OriginalFilename:   filepath.Base(header.Filename),
		MimeType:           detected,
		ByteSize:           size,
		Width:              width,
		Height:             height,
		ChecksumSHA256:     checksum,
		StorageKeyOriginal: relOriginal,
		ThumbPath:          thumbRel,
		DerivativesStatus:  "pending",
		ModerationStatus:   "pending",
		CreditCost:         0,
	}

	if _, err := a.DB.CreateMediaItem(item); err != nil {
		_ = os.Remove(absOriginal)
		if thumbRel != "" {
			_ = os.Remove(filepath.Join(cfg.Root, filepath.FromSlash(thumbRel)))
		}
		return err
	}

	a.queueMediaDeepZoom(cfg, album.PublicID, *item)

	return nil
}

func createMediaThumbnail(cfg mediaConfig, originalPath, albumPublicID, itemPublicID string) (int, int, string, error) {
	f, err := os.Open(originalPath)
	if err != nil {
		return 0, 0, "", err
	}
	defer f.Close()

	img, _, err := image.Decode(f)
	if err != nil {
		return 0, 0, "", err
	}
	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()
	if width <= 0 || height <= 0 {
		return 0, 0, "", errors.New("invalid image")
	}

	thumbImg := resizeToMax(img, mediaThumbMaxSize)
	thumbRel := path.Join(albumPublicID, itemPublicID, "thumb.jpg")
	thumbAbs := filepath.Join(cfg.Root, filepath.FromSlash(thumbRel))

	if err := os.MkdirAll(filepath.Dir(thumbAbs), 0o755); err != nil {
		return 0, 0, "", err
	}

	out, err := os.Create(thumbAbs)
	if err != nil {
		return 0, 0, "", err
	}
	defer out.Close()

	if err := jpeg.Encode(out, thumbImg, &jpeg.Options{Quality: 82}); err != nil {
		return 0, 0, "", err
	}

	return width, height, thumbRel, nil
}

func resizeToMax(img image.Image, maxDim int) image.Image {
	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()
	if maxDim <= 0 || (width <= maxDim && height <= maxDim) {
		return img
	}

	scale := float64(maxDim) / float64(width)
	if height > width {
		scale = float64(maxDim) / float64(height)
	}
	newWidth := int(float64(width) * scale)
	newHeight := int(float64(height) * scale)
	if newWidth <= 0 {
		newWidth = 1
	}
	if newHeight <= 0 {
		newHeight = 1
	}

	dst := image.NewRGBA(image.Rect(0, 0, newWidth, newHeight))
	for y := 0; y < newHeight; y++ {
		srcY := bounds.Min.Y + int(float64(y)*float64(height)/float64(newHeight))
		for x := 0; x < newWidth; x++ {
			srcX := bounds.Min.X + int(float64(x)*float64(width)/float64(newWidth))
			dst.Set(x, y, img.At(srcX, srcY))
		}
	}
	return dst
}

func generateMediaPublicID() string {
	return generateToken(26)
}

func normalizeMediaAlbumType(val string) string {
	val = strings.ToLower(strings.TrimSpace(val))
	if val == "" {
		return "other"
	}
	if !mediaAlbumTypes[val] {
		return "other"
	}
	return val
}

func normalizeMediaSourceType(val string) string {
	val = strings.ToLower(strings.TrimSpace(val))
	if val == "" {
		return "online"
	}
	if !mediaSourceTypes[val] {
		return "online"
	}
	return val
}

func normalizeMediaVisibility(val string) string {
	val = strings.ToLower(strings.TrimSpace(val))
	if val == "" {
		return "private"
	}
	if !mediaVisibilityTypes[val] {
		return "private"
	}
	return val
}

func mediaAlbumTypeList() []string {
	return []string{"book", "memorial", "photo", "achievement_icon", "other"}
}

func mediaAlbumTypeListForPerms(perms db.PolicyPermissions) []string {
	list := mediaAlbumTypeList()
	if perms.Admin {
		return list
	}
	filtered := make([]string, 0, len(list))
	for _, entry := range list {
		if entry == "achievement_icon" {
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
}

func mediaSourceTypeList() []string {
	return []string{"online", "offline_archive", "family_private", "other"}
}

func mediaVisibilityList() []string {
	return []string{"private", "registered", "public", "restricted_group", "admins_only", "custom_policy"}
}

func splitMediaAlbumPath(pathValue string) (string, string) {
	trimmed := strings.TrimPrefix(pathValue, "/media/albums/")
	trimmed = strings.Trim(trimmed, "/")
	if trimmed == "" {
		return "", ""
	}
	parts := strings.Split(trimmed, "/")
	if len(parts) == 0 {
		return "", ""
	}
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], strings.Join(parts[1:], "/")
}

func splitMediaItemPath(pathValue string) (string, string) {
	trimmed := strings.TrimPrefix(pathValue, "/media/items/")
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

func mediaItemPublicIDFromPath(pathValue string) string {
	itemID, _ := splitMediaItemPath(pathValue)
	return itemID
}

func sanitizeFilename(name string) string {
	name = strings.TrimSpace(filepath.Base(name))
	if name == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '.', r == '-', r == '_':
			b.WriteRune(r)
		case r == ' ':
			b.WriteByte('_')
		}
	}
	cleaned := strings.Trim(b.String(), "._-")
	if cleaned == "" || cleaned == "." || cleaned == ".." {
		return ""
	}
	return cleaned
}

func extensionForMime(mime string) string {
	switch strings.ToLower(strings.TrimSpace(mime)) {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/tiff":
		return ".tiff"
	default:
		return ""
	}
}

func (a *App) renderMediaAlbumForm(w http.ResponseWriter, r *http.Request, user *db.User, cfg mediaConfig, title, albumType, sourceType, desc string, llibreID int, llibreLabel, errMsg string) {
	lang := resolveUserLang(r, user)
	perms, _ := a.permissionsFromContext(r)
	if llibreID > 0 && strings.TrimSpace(llibreLabel) == "" {
		if llibre, err := a.DB.GetLlibre(llibreID); err == nil && llibre != nil {
			llibreLabel = strings.TrimSpace(llibre.Titol)
			if llibreLabel == "" {
				llibreLabel = strings.TrimSpace(llibre.NomEsglesia)
			}
		}
	}
	RenderPrivateTemplateLang(w, r, "media-albums-form.html", lang, map[string]interface{}{
		"User":           user,
		"AlbumTypes":     mediaAlbumTypeListForPerms(perms),
		"SourceTypes":    mediaSourceTypeList(),
		"FormTitle":      title,
		"FormType":       albumType,
		"FormSource":     sourceType,
		"FormDesc":       desc,
		"FormError":      errMsg,
		"FormLlibreID":   llibreID,
		"FormLlibreLabel": llibreLabel,
		"MaxUploadMB":    cfg.MaxUploadMB,
		"AllowedMimeCSV": cfg.AllowedCSV,
	})
}
