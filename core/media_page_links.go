package core

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func (a *App) AdminLinkMediaToPagina(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/documentals/llibres", http.StatusSeeOther)
		return
	}
	paginaID := extractID(r.URL.Path)
	if paginaID == 0 {
		http.NotFound(w, r)
		return
	}
	pagina, err := a.DB.GetLlibrePaginaByID(paginaID)
	if err != nil || pagina == nil {
		http.NotFound(w, r)
		return
	}
	target := a.resolveLlibreTarget(pagina.LlibreID)
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresEdit, target)
	if !ok {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	returnTo := safeReturnTo(r.FormValue("return_to"), fmt.Sprintf("/documentals/llibres/%d?media_page_id=%d#media-links", pagina.LlibreID, pagina.ID))
	itemPublicID := strings.TrimSpace(r.FormValue("media_item_public_id"))
	if itemPublicID == "" {
		http.Redirect(w, r, returnTo, http.StatusSeeOther)
		return
	}
	item, err := a.DB.GetMediaItemByPublicID(itemPublicID)
	if err != nil || item == nil {
		http.Redirect(w, r, returnTo, http.StatusSeeOther)
		return
	}
	album, err := a.DB.GetMediaAlbumByID(item.AlbumID)
	if err != nil || album == nil {
		http.Redirect(w, r, returnTo, http.StatusSeeOther)
		return
	}
	if !a.mediaLinkAllowed(r, user, album, item) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	pageOrder := parseIntDefault(r.FormValue("page_order"), 0)
	if pageOrder == 0 && pagina.NumPagina > 0 {
		pageOrder = pagina.NumPagina
	}
	notes := strings.TrimSpace(r.FormValue("notes"))
	if err := a.DB.UpsertMediaItemPageLink(item.ID, pagina.LlibreID, pagina.ID, pageOrder, notes); err != nil {
		Errorf("Error vinculant media item %d a pagina %d: %v", item.ID, pagina.ID, err)
	}
	http.Redirect(w, r, returnTo, http.StatusSeeOther)
}

func (a *App) AdminUnlinkMediaFromPagina(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/documentals/llibres", http.StatusSeeOther)
		return
	}
	paginaID := extractID(r.URL.Path)
	if paginaID == 0 {
		http.NotFound(w, r)
		return
	}
	pagina, err := a.DB.GetLlibrePaginaByID(paginaID)
	if err != nil || pagina == nil {
		http.NotFound(w, r)
		return
	}
	target := a.resolveLlibreTarget(pagina.LlibreID)
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsLlibresEdit, target)
	if !ok {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	returnTo := safeReturnTo(r.FormValue("return_to"), fmt.Sprintf("/documentals/llibres/%d?media_page_id=%d#media-links", pagina.LlibreID, pagina.ID))
	itemPublicID := strings.TrimSpace(r.FormValue("media_item_public_id"))
	if itemPublicID == "" {
		http.Redirect(w, r, returnTo, http.StatusSeeOther)
		return
	}
	item, err := a.DB.GetMediaItemByPublicID(itemPublicID)
	if err != nil || item == nil {
		http.Redirect(w, r, returnTo, http.StatusSeeOther)
		return
	}
	album, err := a.DB.GetMediaAlbumByID(item.AlbumID)
	if err != nil || album == nil {
		http.Redirect(w, r, returnTo, http.StatusSeeOther)
		return
	}
	if !a.mediaLinkAllowed(r, user, album, item) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if err := a.DB.DeleteMediaItemPageLink(item.ID, pagina.ID); err != nil {
		Errorf("Error desvinculant media item %d de pagina %d: %v", item.ID, pagina.ID, err)
	}
	http.Redirect(w, r, returnTo, http.StatusSeeOther)
}

func (a *App) mediaLinkAllowed(r *http.Request, user *db.User, album *db.MediaAlbum, item *db.MediaItem) bool {
	if album == nil || item == nil {
		return false
	}
	if a.mediaUserIsPrivileged(r, user, album) {
		return true
	}
	if album.ModerationStatus != "approved" || item.ModerationStatus != "approved" {
		return false
	}
	return a.mediaUserCanAccess(r, user, album)
}

func (a *App) filterMediaItemLinks(r *http.Request, user *db.User, links []db.MediaItemPageLink) []db.MediaItemPageLink {
	if len(links) == 0 {
		return links
	}
	filtered := links[:0]
	for _, link := range links {
		album := mediaAlbumFromPageLink(link)
		item := mediaItemFromPageLink(link)
		if a.mediaLinkAllowed(r, user, album, item) {
			filtered = append(filtered, link)
		}
	}
	return filtered
}

func (a *App) filterMediaSearchResults(r *http.Request, user *db.User, rows []db.MediaItemSearchRow) []db.MediaItemSearchRow {
	if len(rows) == 0 {
		return rows
	}
	filtered := rows[:0]
	for _, row := range rows {
		album := mediaAlbumFromSearchRow(row)
		item := mediaItemFromSearchRow(row)
		if a.mediaLinkAllowed(r, user, album, item) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func mediaAlbumFromPageLink(link db.MediaItemPageLink) *db.MediaAlbum {
	return &db.MediaAlbum{
		ID:                link.AlbumID,
		PublicID:          link.AlbumPublicID,
		Title:             link.AlbumTitle,
		OwnerUserID:       link.AlbumOwnerUserID,
		ModerationStatus:  link.AlbumModerationStatus,
		Visibility:        link.AlbumVisibility,
		RestrictedGroupID: link.AlbumRestrictedGroupID,
		AccessPolicyID:    link.AlbumAccessPolicyID,
	}
}

func mediaItemFromPageLink(link db.MediaItemPageLink) *db.MediaItem {
	return &db.MediaItem{
		ID:               link.MediaItemID,
		PublicID:         link.MediaItemPublicID,
		Title:            link.MediaItemTitle,
		ThumbPath:        link.MediaItemThumbPath,
		ModerationStatus: link.MediaItemStatus,
	}
}

func mediaAlbumFromSearchRow(row db.MediaItemSearchRow) *db.MediaAlbum {
	return &db.MediaAlbum{
		ID:                row.AlbumID,
		PublicID:          row.AlbumPublicID,
		Title:             row.AlbumTitle,
		OwnerUserID:       row.AlbumOwnerUserID,
		ModerationStatus:  row.AlbumStatus,
		Visibility:        row.AlbumVisibility,
		RestrictedGroupID: row.AlbumRestrictedGroupID,
		AccessPolicyID:    row.AlbumAccessPolicyID,
	}
}

func mediaItemFromSearchRow(row db.MediaItemSearchRow) *db.MediaItem {
	return &db.MediaItem{
		ID:               row.MediaItemID,
		PublicID:         row.MediaItemPublicID,
		Title:            row.MediaItemTitle,
		ThumbPath:        row.MediaItemThumb,
		ModerationStatus: row.MediaItemStatus,
	}
}
