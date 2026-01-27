package core

import (
	"bytes"
	"crypto/sha256"
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
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	user := userFromContext(r)
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
		"MaxUploadMB":    cfg.MaxUploadMB,
		"AllowedMimeCSV": cfg.AllowedCSV,
	})
}

func (a *App) MediaAlbumDetail(w http.ResponseWriter, r *http.Request) {
	cfg := a.mediaConfig()
	if !cfg.Enabled {
		http.NotFound(w, r)
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
	itemPublicID, tail := splitMediaItemPath(r.URL.Path)
	if itemPublicID == "" {
		http.NotFound(w, r)
		return
	}
	if tail == "thumb" {
		a.mediaItemThumb(w, r, cfg, itemPublicID)
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
	if albumType == "achievement_icon" && !perms.Admin {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	if title == "" {
		a.renderMediaAlbumForm(w, r, user, cfg, title, albumType, sourceType, desc, T(resolveUserLang(r, user), "media.error.title_required"))
		return
	}

	album := &db.MediaAlbum{
		PublicID:         generateMediaPublicID(),
		Title:            title,
		Description:      desc,
		AlbumType:        albumType,
		OwnerUserID:      user.ID,
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
		a.renderMediaAlbumForm(w, r, user, cfg, title, albumType, sourceType, desc, T(resolveUserLang(r, user), "media.error.create_failed"))
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
	linkCounts := map[int]int{}
	if len(items) > 0 {
		if counts, err := a.DB.CountMediaItemLinksByAlbum(album.ID); err == nil {
			linkCounts = counts
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
		"Uploaded":       uploaded,
		"Failed":         failed,
		"AllowedMimeCSV": cfg.AllowedCSV,
		"MaxUploadMB":    cfg.MaxUploadMB,
		"CanUpload":      isOwner,
		"IsOwner":        isOwner,
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

func (a *App) renderMediaAlbumForm(w http.ResponseWriter, r *http.Request, user *db.User, cfg mediaConfig, title, albumType, sourceType, desc, errMsg string) {
	lang := resolveUserLang(r, user)
	perms, _ := a.permissionsFromContext(r)
	RenderPrivateTemplateLang(w, r, "media-albums-form.html", lang, map[string]interface{}{
		"User":           user,
		"AlbumTypes":     mediaAlbumTypeListForPerms(perms),
		"SourceTypes":    mediaSourceTypeList(),
		"FormTitle":      title,
		"FormType":       albumType,
		"FormSource":     sourceType,
		"FormDesc":       desc,
		"FormError":      errMsg,
		"MaxUploadMB":    cfg.MaxUploadMB,
		"AllowedMimeCSV": cfg.AllowedCSV,
	})
}
