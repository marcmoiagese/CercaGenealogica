package integration

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func createPublicPersona(t *testing.T, database db.DB, userID int, name string) int {
	t.Helper()
	persona := &db.Persona{
		Nom:            name,
		Cognom1:        "Test",
		ModeracioEstat: "publicat",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	id, err := database.CreatePersona(persona)
	if err != nil {
		t.Fatalf("CreatePersona ha fallat: %v", err)
	}
	return id
}

func createPendingMediaAlbum(t *testing.T, database db.DB, userID int, title string) int {
	t.Helper()
	album := &db.MediaAlbum{
		PublicID:         fmt.Sprintf("album-%d", time.Now().UnixNano()),
		Title:            title,
		Description:      "",
		AlbumType:        "photo",
		OwnerUserID:      userID,
		LlibreID:         sql.NullInt64{},
		ModerationStatus: "pending",
		Visibility:       "private",
		RestrictedGroupID: sql.NullInt64{},
		AccessPolicyID:    sql.NullInt64{},
		CreditCost:        0,
		DifficultyScore:   0,
		SourceType:        "online",
		ModerationNotes:   "",
	}
	id, err := database.CreateMediaAlbum(album)
	if err != nil {
		t.Fatalf("CreateMediaAlbum ha fallat: %v", err)
	}
	return id
}

func createPendingMediaItem(t *testing.T, database db.DB, albumID int, title string) int {
	t.Helper()
	item := &db.MediaItem{
		PublicID:           fmt.Sprintf("item-%d", time.Now().UnixNano()),
		AlbumID:            albumID,
		Title:              title,
		OriginalFilename:   title + ".jpg",
		MimeType:           "image/jpeg",
		ByteSize:           128,
		ChecksumSHA256:     "",
		StorageKeyOriginal: fmt.Sprintf("media/%d", time.Now().UnixNano()),
		ThumbPath:          "",
		DerivativesStatus:  "pending",
		ModerationStatus:   "pending",
		ModerationNotes:    "",
		CreditCost:         0,
	}
	id, err := database.CreateMediaItem(item)
	if err != nil {
		t.Fatalf("CreateMediaItem ha fallat: %v", err)
	}
	return id
}

func createPendingMapaVersion(t *testing.T, database db.DB, userID, mapID int) int {
	t.Helper()
	version, err := database.NextMunicipiMapaVersionNumber(mapID)
	if err != nil {
		t.Fatalf("NextMunicipiMapaVersionNumber ha fallat: %v", err)
	}
	row := &db.MunicipiMapaVersion{
		MapaID:      mapID,
		Version:     version,
		Status:      "pendent",
		JSONData:    mapesTestJSON,
		Changelog:   "",
		LockVersion: 0,
		CreatedBy:   sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	id, err := database.CreateMunicipiMapaVersion(row)
	if err != nil {
		t.Fatalf("CreateMunicipiMapaVersion ha fallat: %v", err)
	}
	return id
}

func TestModeracioSummaryGlobalIncludesMediaExternalMap(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_7_summary_global.sqlite3")

	admin := createTestUser(t, database, "admin_summary_global")
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_summary_global")

	personaID := createPublicPersona(t, database, admin.ID, "Persona Summary")
	if _, err := database.ExternalLinkInsertPending(personaID, admin.ID, "https://example.com/profile", "Example"); err != nil {
		t.Fatalf("ExternalLinkInsertPending ha fallat: %v", err)
	}
	albumID := createPendingMediaAlbum(t, database, admin.ID, "Album Pending")
	createPendingMediaItem(t, database, albumID, "Item Pending")
	_, mapID := createMunicipiAndMap(t, database, admin.ID)
	createPendingMapaVersion(t, database, admin.ID, mapID)

	// Wait for admin summary cache TTL so fresh counts are computed.
	time.Sleep(11 * time.Second)

	req := httptest.NewRequest(http.MethodGet, "/api/admin/control/moderacio/summary", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminControlModeracioSummaryAPI(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("summary esperava 200, got %d", rr.Code)
	}

	var payload struct {
		Ok           bool   `json:"ok"`
		SummaryScope string `json:"summary_scope"`
		Summary      struct {
			ByType []struct {
				Type  string `json:"type"`
				Total int    `json:"total"`
			} `json:"by_type"`
		} `json:"summary"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("summary response invalid: %v", err)
	}
	if !payload.Ok {
		t.Fatalf("summary ok esperat true")
	}
	if payload.SummaryScope != "global" {
		t.Fatalf("summary_scope esperat global, got %s", payload.SummaryScope)
	}
	got := map[string]int{}
	for _, item := range payload.Summary.ByType {
		got[item.Type] = item.Total
	}
	if got["media_album"] != 1 {
		t.Fatalf("summary media_album esperat 1, got %d", got["media_album"])
	}
	if got["media_item"] != 1 {
		t.Fatalf("summary media_item esperat 1, got %d", got["media_item"])
	}
	if got["external_link"] != 1 {
		t.Fatalf("summary external_link esperat 1, got %d", got["external_link"])
	}
	if got["municipi_mapa_version"] != 1 {
		t.Fatalf("summary municipi_mapa_version esperat 1, got %d", got["municipi_mapa_version"])
	}
}

func TestModeracioSummaryScopedExternalLink(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_7_summary_external_scoped.sqlite3")

	user := createNonAdminTestUser(t, database, "moderator_external_scoped")
	personaID := createPublicPersona(t, database, user.ID, "Persona External")
	if _, err := database.ExternalLinkInsertPending(personaID, user.ID, "https://example.com/ext", "Example"); err != nil {
		t.Fatalf("ExternalLinkInsertPending ha fallat: %v", err)
	}

	policyID := createPolicyWithGrant(t, database, "external_links_scope", "admin.external_links.moderate")
	assignPolicyToUser(t, database, user.ID, policyID)

	session := createSessionCookie(t, database, user.ID, "sess_external_summary")
	req := httptest.NewRequest(http.MethodGet, "/api/admin/control/moderacio/summary", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminControlModeracioSummaryAPI(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("summary esperava 200, got %d", rr.Code)
	}

	var payload struct {
		Ok           bool   `json:"ok"`
		SummaryScope string `json:"summary_scope"`
		Summary      struct {
			Total  int `json:"total"`
			ByType []struct {
				Type  string `json:"type"`
				Total int    `json:"total"`
			} `json:"by_type"`
		} `json:"summary"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("summary response invalid: %v", err)
	}
	if !payload.Ok {
		t.Fatalf("summary ok esperat true")
	}
	if payload.SummaryScope != "scoped" {
		t.Fatalf("summary_scope esperat scoped, got %s", payload.SummaryScope)
	}
	if payload.Summary.Total != 1 {
		t.Fatalf("summary total esperat 1, got %d", payload.Summary.Total)
	}
	got := map[string]int{}
	for _, item := range payload.Summary.ByType {
		got[item.Type] = item.Total
	}
	if got["external_link"] != 1 || len(got) != 1 {
		t.Fatalf("summary by_type inesperat: %+v", got)
	}
}

func TestModeracioSummaryScopedMapaVersion(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_7_summary_map_scoped.sqlite3")

	user := createNonAdminTestUser(t, database, "moderator_map_scoped")
	munAllowed, mapAllowed := createMunicipiAndMap(t, database, user.ID)
	_, mapOther := createMunicipiAndMap(t, database, user.ID)

	createPendingMapaVersion(t, database, user.ID, mapAllowed)
	createPendingMapaVersion(t, database, user.ID, mapOther)

	policyID := createPolicyWithScopedGrant(t, database, "map_scope_summary", "municipis.mapes.moderate", string(core.ScopeMunicipi), munAllowed)
	assignPolicyToUser(t, database, user.ID, policyID)

	session := createSessionCookie(t, database, user.ID, "sess_map_summary")
	req := httptest.NewRequest(http.MethodGet, "/api/admin/control/moderacio/summary", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminControlModeracioSummaryAPI(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("summary esperava 200, got %d", rr.Code)
	}

	var payload struct {
		Ok           bool   `json:"ok"`
		SummaryScope string `json:"summary_scope"`
		Summary      struct {
			Total  int `json:"total"`
			ByType []struct {
				Type  string `json:"type"`
				Total int    `json:"total"`
			} `json:"by_type"`
		} `json:"summary"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("summary response invalid: %v", err)
	}
	if !payload.Ok {
		t.Fatalf("summary ok esperat true")
	}
	if payload.SummaryScope != "scoped" {
		t.Fatalf("summary_scope esperat scoped, got %s", payload.SummaryScope)
	}
	if payload.Summary.Total != 1 {
		t.Fatalf("summary total esperat 1, got %d", payload.Summary.Total)
	}
	got := map[string]int{}
	for _, item := range payload.Summary.ByType {
		got[item.Type] = item.Total
	}
	if got["municipi_mapa_version"] != 1 || len(got) != 1 {
		t.Fatalf("summary by_type inesperat: %+v", got)
	}
}

func TestModeracioBulkAllProcessesMediaExternalMap(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_7_bulk_all.sqlite3")

	admin := createTestUser(t, database, "admin_bulk_all_media")
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_bulk_all_media")

	personaID := createPublicPersona(t, database, admin.ID, "Persona Bulk")
	linkID, err := database.ExternalLinkInsertPending(personaID, admin.ID, "https://example.com/bulk", "Example")
	if err != nil {
		t.Fatalf("ExternalLinkInsertPending ha fallat: %v", err)
	}
	albumID := createPendingMediaAlbum(t, database, admin.ID, "Album Bulk")
	itemID := createPendingMediaItem(t, database, albumID, "Item Bulk")
	_, mapID := createMunicipiAndMap(t, database, admin.ID)
	mapVersionID := createPendingMapaVersion(t, database, admin.ID, mapID)

	csrf := "csrf_bulk_all_media"
	form := newFormValues(map[string]string{
		"bulk_action": "approve",
		"bulk_scope":  "all",
		"bulk_type":   "all",
		"csrf_token":  csrf,
		"return_to":   "/moderacio",
	})
	req := httptest.NewRequest(http.MethodPost, "/moderacio/bulk", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()
	app.AdminModeracioBulk(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("bulk all esperava 303, got %d", rr.Code)
	}

	album, err := database.GetMediaAlbumByID(albumID)
	if err != nil || album == nil {
		t.Fatalf("GetMediaAlbumByID ha fallat: %v", err)
	}
	if album.ModerationStatus != "approved" {
		t.Fatalf("album status esperat approved, got %s", album.ModerationStatus)
	}
	item, err := database.GetMediaItemByID(itemID)
	if err != nil || item == nil {
		t.Fatalf("GetMediaItemByID ha fallat: %v", err)
	}
	if item.ModerationStatus != "approved" {
		t.Fatalf("item status esperat approved, got %s", item.ModerationStatus)
	}
	version, err := database.GetMunicipiMapaVersion(mapVersionID)
	if err != nil || version == nil {
		t.Fatalf("GetMunicipiMapaVersion ha fallat: %v", err)
	}
	if version.Status != "publicat" {
		t.Fatalf("mapa version status esperat publicat, got %s", version.Status)
	}
	links, err := database.ExternalLinksListByStatus("approved")
	if err != nil {
		t.Fatalf("ExternalLinksListByStatus ha fallat: %v", err)
	}
	found := false
	for _, row := range links {
		if row.ID == linkID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("external_link aprovat no trobat")
	}
}

func TestModeracioBulkPageMediaItemSelection(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_7_bulk_page_media.sqlite3")

	admin := createTestUser(t, database, "admin_bulk_page_media")
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_bulk_page_media")

	albumID := createPendingMediaAlbum(t, database, admin.ID, "Album Page")
	itemKeep := createPendingMediaItem(t, database, albumID, "Item Keep")
	itemApprove := createPendingMediaItem(t, database, albumID, "Item Approve")

	csrf := "csrf_bulk_page_media"
	form := newFormValues(map[string]string{
		"bulk_action": "approve",
		"bulk_scope":  "page",
		"bulk_type":   "media_item",
		"csrf_token":  csrf,
		"return_to":   "/moderacio?type=media_item",
	})
	form.Add("selected", fmt.Sprintf("media_item:%d", itemApprove))
	req := httptest.NewRequest(http.MethodPost, "/moderacio/bulk", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()
	app.AdminModeracioBulk(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("bulk page esperava 303, got %d", rr.Code)
	}

	approved, err := database.GetMediaItemByID(itemApprove)
	if err != nil || approved == nil {
		t.Fatalf("GetMediaItemByID aprovat ha fallat: %v", err)
	}
	if approved.ModerationStatus != "approved" {
		t.Fatalf("item aprovat status esperat approved, got %s", approved.ModerationStatus)
	}
	kept, err := database.GetMediaItemByID(itemKeep)
	if err != nil || kept == nil {
		t.Fatalf("GetMediaItemByID pendent ha fallat: %v", err)
	}
	if kept.ModerationStatus != "pending" {
		t.Fatalf("item pendent status esperat pending, got %s", kept.ModerationStatus)
	}
}
