package core

import (
	"encoding/json"
	"net/http"
	"net/url"
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
	user, ok := a.requireMediaView(w, r)
	if !ok {
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
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
	pageContext := a.buildMediaViewerPageContext(r, user, album, item)
	var indexerCfg indexerConfig
	var indexerDraft []map[string]string
	indexerExisting := []map[string]string{}
	if pageContext != nil && pageContext.CanIndexPage {
		if book, err := a.DB.GetLlibre(pageContext.BookID); err == nil && book != nil {
			indexerCfg = buildIndexerConfig(lang, book)
			if draft, err := a.DB.GetTranscripcioDraft(user.ID, book.ID); err == nil && draft != nil && draft.Payload != "" {
				var payload indexerPayload
				if json.Unmarshal([]byte(draft.Payload), &payload) == nil {
					indexerDraft = payload.Rows
				}
			}
			if pageContext.PageTotalRegistres > 0 && pageContext.PageNumText != "" {
				if registres, err := a.DB.ListTranscripcionsRawByPageValue(book.ID, pageContext.PageNumText); err == nil {
					indexerExisting = buildViewerIndexerExistingRows(indexerCfg, registres, func(regID int) ([]db.TranscripcioPersonaRaw, []db.TranscripcioAtributRaw) {
						persones, _ := a.DB.ListTranscripcioPersones(regID)
						atributs, _ := a.DB.ListTranscripcioAtributs(regID)
						return persones, atributs
					})
				}
			}
		}
	}
	if pageContext != nil {
		pageContext.IndexerExisting = indexerExisting
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
		"PageContext":   pageContext,
		"CurrentURL":    r.URL.RequestURI(),
		"IndexerConfig": indexerCfg,
		"IndexerDraft":  indexerDraft,
		"IndexerExisting": indexerExisting,
	}
	if user != nil {
		RenderPrivateTemplateLang(w, r, "media-viewer.html", lang, payload)
		return
	}
	RenderTemplate(w, r, "media-viewer.html", payload)
}

func (a *App) mediaItemViewData(w http.ResponseWriter, r *http.Request, cfg mediaConfig, itemPublicID string) {
	user := a.mediaEnsureUser(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
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
	pageContext := a.buildMediaViewerPageContext(r, user, album, item)
	if pageContext != nil && pageContext.CanIndexPage && pageContext.PageTotalRegistres > 0 {
		if book, err := a.DB.GetLlibre(pageContext.BookID); err == nil && book != nil {
			cfg := buildIndexerConfig(lang, book)
			if pageContext.PageNumText != "" {
				if registres, err := a.DB.ListTranscripcionsRawByPageValue(book.ID, pageContext.PageNumText); err == nil {
					pageContext.IndexerExisting = buildViewerIndexerExistingRows(cfg, registres, func(regID int) ([]db.TranscripcioPersonaRaw, []db.TranscripcioAtributRaw) {
						persones, _ := a.DB.ListTranscripcioPersones(regID)
						atributs, _ := a.DB.ListTranscripcioAtributs(regID)
						return persones, atributs
					})
				}
			}
		}
	}

	writeJSON(w, map[string]interface{}{
		"item": map[string]interface{}{
			"public_id": item.PublicID,
			"title":     item.Title,
		},
		"album": map[string]interface{}{
			"public_id": album.PublicID,
		},
		"prev_id":       prevItemID,
		"next_id":       nextItemID,
		"current_index": currentIndex,
		"total_items":   len(items),
		"dzi":           "/media/dz/" + item.PublicID + "/dz.dzi",
		"grant_token":   grantToken,
		"status":        message,
		"page_context":  mediaViewerPageContextJSON(pageContext, lang),
	})
}

type mediaViewerPageContext struct {
	HasBook            bool
	HasLink            bool
	CanLinkPages       bool
	BookID             int
	BookTitle          string
	BookType           string
	BookCronologia     string
	BookPagines        int
	BookMunicipi       string
	BookURL            string
	PageID             int
	PageNum            int
	PageNumText        string
	StatID             int
	PageType           string
	PageExcluded       int
	PageIndexed        int
	PageDuplicate      string
	PageTotalRegistres int
	ExistingRegistres  int
	RemainingRegistres int
	CanEditPage        bool
	CanIndexPage       bool
	CanViewRegistres   bool
	CanEditRegistres   bool
	CanViewBook        bool
	RegistresURL       string
	IndexerURL         string
	IndexerExisting    []map[string]string
}

func (a *App) buildMediaViewerPageContext(r *http.Request, user *db.User, album *db.MediaAlbum, item *db.MediaItem) *mediaViewerPageContext {
	if user == nil || album == nil || item == nil || !album.LlibreID.Valid {
		return nil
	}
	perms, _ := a.permissionsFromContext(r)
	target := a.resolveLlibreTarget(int(album.LlibreID.Int64))
	canViewBook := perms.Admin || perms.CanManageArchives || a.HasPermission(user.ID, permKeyDocumentalsLlibresView, target)
	if !canViewBook {
		return nil
	}
	book, err := a.DB.GetLlibre(int(album.LlibreID.Int64))
	if err != nil || book == nil {
		return nil
	}
	ctx := &mediaViewerPageContext{
		HasBook:          true,
		BookID:           book.ID,
		BookTitle:        strings.TrimSpace(book.Titol),
		BookType:         strings.TrimSpace(book.TipusLlibre),
		BookCronologia:   strings.TrimSpace(formatCronologiaDisplay(book.Cronologia)),
		BookPagines:      int(book.Pagines.Int64),
		BookURL:          "/documentals/llibres/" + strconv.Itoa(book.ID),
		CanViewBook:      canViewBook,
		CanLinkPages:     canViewBook && a.mediaUserIsPrivileged(r, user, album),
		CanEditPage:      a.HasPermission(user.ID, permKeyDocumentalsLlibresEdit, target),
		CanIndexPage:     a.HasPermission(user.ID, permKeyDocumentalsLlibresBulkIndex, target),
		CanViewRegistres: a.HasPermission(user.ID, permKeyDocumentalsLlibresViewRegistres, target),
		CanEditRegistres: a.HasPermission(user.ID, permKeyDocumentalsRegistresEditInline, target) || a.HasPermission(user.ID, permKeyDocumentalsRegistresEdit, target),
	}
	if ctx.BookTitle == "" {
		ctx.BookTitle = strings.TrimSpace(book.NomEsglesia)
	}
	if ctx.BookType == "" {
		ctx.BookType = "altres"
	}
	if book.MunicipiID > 0 {
		if mun, err := a.DB.GetMunicipi(book.MunicipiID); err == nil && mun != nil {
			ctx.BookMunicipi = strings.TrimSpace(mun.Nom)
		}
	}
	if book.Pagines.Valid {
		ctx.BookPagines = int(book.Pagines.Int64)
	}

	links, err := a.DB.ListMediaItemLinksByAlbum(album.ID)
	if err != nil || len(links) == 0 {
		return ctx
	}
	var link *db.MediaItemPageLink
	for i := range links {
		if links[i].MediaItemID == item.ID {
			link = &links[i]
			break
		}
	}
	if link == nil {
		return ctx
	}
	ctx.HasLink = true
	if link.PaginaID.Valid {
		ctx.PageID = int(link.PaginaID.Int64)
	}
	if link.NumPagina.Valid {
		ctx.PageNum = int(link.NumPagina.Int64)
	}

	if ctx.PageID > 0 {
		if page, err := a.DB.GetLlibrePaginaByID(ctx.PageID); err == nil && page != nil {
			ctx.PageNum = page.NumPagina
		}
	} else if ctx.PageNum > 0 {
		if page, err := a.DB.GetLlibrePaginaByNum(book.ID, ctx.PageNum); err == nil && page != nil {
			ctx.PageID = page.ID
		}
	}
	if ctx.PageNum > 0 {
		ctx.PageNumText = strconv.Itoa(ctx.PageNum)
	}

	stats, err := a.DB.ListTranscripcionsRawPageStats(book.ID)
	if err == nil && len(stats) > 0 {
		stat := pickPageStat(stats, ctx.PageID, ctx.PageNumText)
		if stat != nil {
			ctx.StatID = stat.ID
			ctx.PageType = strings.TrimSpace(stat.TipusPagina)
			ctx.PageExcluded = stat.Exclosa
			ctx.PageIndexed = stat.IndexacioCompleta
			ctx.PageTotalRegistres = stat.TotalRegistres
			if stat.DuplicadaDe.Valid {
				ctx.PageDuplicate = stat.DuplicadaDe.String
			}
			if strings.TrimSpace(stat.NumPaginaText) != "" {
				ctx.PageNumText = strings.TrimSpace(stat.NumPaginaText)
			}
		}
	}
	if ctx.PageType == "" {
		ctx.PageType = "normal"
	}
	if ctx.PageNumText != "" {
		if count, err := a.DB.CountTranscripcionsRawByPageValue(book.ID, ctx.PageNumText); err == nil {
			ctx.ExistingRegistres = count
		}
	}
	if ctx.PageTotalRegistres > 0 {
		remaining := ctx.PageTotalRegistres - ctx.ExistingRegistres
		if remaining < 0 {
			remaining = 0
		}
		ctx.RemainingRegistres = remaining
	}
	if ctx.CanViewRegistres && ctx.PageID > 0 {
		ctx.RegistresURL = "/documentals/llibres/" + strconv.Itoa(book.ID) + "/registres?pagina_id=" + strconv.Itoa(ctx.PageID)
	}
	if ctx.CanIndexPage {
		indexURL := "/documentals/llibres/" + strconv.Itoa(book.ID) + "/indexar"
		params := []string{}
		if ctx.PageNumText != "" {
			params = append(params, "pagina="+url.QueryEscape(ctx.PageNumText))
		}
		if ctx.PageTotalRegistres > 0 {
			params = append(params, "page_remaining="+strconv.Itoa(ctx.RemainingRegistres))
		}
		if len(params) > 0 {
			indexURL += "?" + strings.Join(params, "&")
		}
		ctx.IndexerURL = indexURL
	}
	return ctx
}

func pickPageStat(stats []db.TranscripcioRawPageStat, paginaID int, numText string) *db.TranscripcioRawPageStat {
	if paginaID > 0 {
		for i := range stats {
			if stats[i].PaginaID.Valid && int(stats[i].PaginaID.Int64) == paginaID {
				return &stats[i]
			}
		}
	}
	if strings.TrimSpace(numText) == "" {
		return nil
	}
	numText = strings.TrimSpace(numText)
	for i := range stats {
		if strings.TrimSpace(stats[i].NumPaginaText) == numText {
			return &stats[i]
		}
	}
	return nil
}

func mediaViewerPageContextJSON(ctx *mediaViewerPageContext, lang string) map[string]interface{} {
	if ctx == nil {
		return nil
	}
	typeLabel := ""
	if strings.TrimSpace(ctx.BookType) != "" {
		typeLabel = T(lang, "books.type."+ctx.BookType)
	}
	res := map[string]interface{}{
		"has_book": ctx.HasBook,
		"has_link": ctx.HasLink,
		"book": map[string]interface{}{
			"id":         ctx.BookID,
			"title":      ctx.BookTitle,
			"type":       ctx.BookType,
			"type_label": typeLabel,
			"cronologia": ctx.BookCronologia,
			"pagines":    ctx.BookPagines,
			"municipi":   ctx.BookMunicipi,
			"url":        ctx.BookURL,
		},
		"page": map[string]interface{}{
			"id":              ctx.PageID,
			"num":             ctx.PageNum,
			"num_text":        ctx.PageNumText,
			"stat_id":         ctx.StatID,
			"type":            ctx.PageType,
			"excluded":        ctx.PageExcluded,
			"indexed":         ctx.PageIndexed,
			"duplicate":       ctx.PageDuplicate,
			"total_registres": ctx.PageTotalRegistres,
		},
		"counts": map[string]interface{}{
			"existing":  ctx.ExistingRegistres,
			"remaining": ctx.RemainingRegistres,
		},
		"permissions": map[string]interface{}{
			"can_edit":        ctx.CanEditPage,
			"can_index":       ctx.CanIndexPage,
			"can_view_records": ctx.CanViewRegistres,
			"can_inline_edit": ctx.CanEditRegistres,
			"can_link":        ctx.CanLinkPages,
			"can_view_book":   ctx.CanViewBook,
		},
		"links": map[string]interface{}{
			"registres": ctx.RegistresURL,
			"indexar":   ctx.IndexerURL,
		},
		"existing_rows": ctx.IndexerExisting,
	}
	return res
}

func buildViewerIndexerExistingRows(cfg indexerConfig, registres []db.TranscripcioRaw, fetch func(int) ([]db.TranscripcioPersonaRaw, []db.TranscripcioAtributRaw)) []map[string]string {
	if len(registres) == 0 || len(cfg.Fields) == 0 {
		return []map[string]string{}
	}
	rows := make([]map[string]string, 0, len(registres))
	for _, reg := range registres {
		persones, atributsList := fetch(reg.ID)
		atributs := map[string]db.TranscripcioAtributRaw{}
		for _, attr := range atributsList {
			if _, ok := atributs[attr.Clau]; ok {
				continue
			}
			atributs[attr.Clau] = attr
		}
		cache := map[string]*db.TranscripcioPersonaRaw{}
		row := map[string]string{}
		for _, field := range cfg.Fields {
			row[field.Key] = registreCellValue(field, reg, persones, atributs, cache)
		}
		row["__readonly"] = "1"
		row["__record_id"] = strconv.Itoa(reg.ID)
		if reg.PosicioPagina.Valid {
			row["__position"] = strconv.Itoa(int(reg.PosicioPagina.Int64))
		}
		rows = append(rows, row)
	}
	return rows
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
