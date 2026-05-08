package integration

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF354U8ImportV2MinimalArchiveMunicipalityWithoutReligion(t *testing.T) {
	app, database, admin, session := setupF354U7BooksAdmin(t, "test_f35_4u8_minimal.sqlite3")
	_, municipiID := seedF354U7BookTerritory(t, database, "U8Minimal")
	arxiuID := createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u8_archive_min", "Arxiu F35-4U8 Minimal", 0)

	payload := map[string]interface{}{
		"schema": "cercagenealogica.llibres.v2",
		"items": map[string]interface{}{
			"llibres": []map[string]interface{}{{
				"code":          "f35_4u8_minimal_book",
				"title":         "Llibre F35-4U8 Minimal",
				"book_type":     "baptismes",
				"chronology":    "1880-1885",
				"source_system": "ahat",
				"municipality":  map[string]interface{}{"name": "Municipi F35-4U7 U8Minimal", "country_iso2": "ES"},
				"archives": []map[string]interface{}{{
					"archive_code":      "f35_4u8_archive_min",
					"relation_type":     "custodia_original",
					"principal":         true,
					"preferred_display": true,
				}},
			}},
		},
	}

	location := postF354U8Import(t, app.AdminLlibresImportRun, session, payload)
	if !strings.Contains(location, "llibres_created=1") || !strings.Contains(location, "llibres_errors=0") {
		t.Fatalf("redirect import minimal inesperat: %s", location)
	}
	rows, err := database.Query("SELECT id, arquevisbat_id, moderation_status FROM llibres WHERE codi = ?", "f35_4u8_minimal_book")
	if err != nil || len(rows) != 1 {
		t.Fatalf("Query llibre v2 minimal: err=%v rows=%d", err, len(rows))
	}
	llibreID := parseCountValue(t, rows[0]["id"])
	if got := strings.TrimSpace(asString(rows[0]["arquevisbat_id"])); got != "" && got != "0" {
		t.Fatalf("arquevisbat_id ha de quedar buit, got %q", got)
	}
	if got := strings.TrimSpace(asString(rows[0]["moderation_status"])); got != "pendent" {
		t.Fatalf("el llibre nou ha de quedar pendent, got %q", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM arxius_llibres WHERE llibre_id = ? AND arxiu_id = ?", llibreID, arxiuID); got != 1 {
		t.Fatalf("s'esperava 1 relacio documental, got %d", got)
	}
	result := latestF354U8ImportResult(t, database)
	if asJSONInt(result["errors_total"]) != 0 {
		t.Fatalf("l'import minim no ha de tenir errors, got %+v", result)
	}
}

func TestF354U8ImportV2MinimalMultiDB(t *testing.T) {
	apps := newAppsForAllDBs(t)
	for _, env := range apps {
		env := env
		t.Run(env.Label, func(t *testing.T) {
			suffix := env.Label + "_" + strconv.FormatInt(time.Now().UnixNano(), 36)
			admin := &db.User{
				Usuari:        "f35_4u8_admin_" + suffix,
				Email:         "f35_4u8_admin_" + suffix + "@example.com",
				Password:      []byte("hash"),
				Active:        true,
				DataNaixament: "1990-01-01",
			}
			if err := env.DB.InsertUser(admin); err != nil {
				t.Fatalf("[%s] InsertUser ha fallat: %v", env.Label, err)
			}
			if err := env.DB.EnsureDefaultPolicies(); err != nil {
				t.Fatalf("[%s] EnsureDefaultPolicies ha fallat: %v", env.Label, err)
			}
			if err := env.DB.EnsureDefaultPointsRules(); err != nil {
				t.Fatalf("[%s] EnsureDefaultPointsRules ha fallat: %v", env.Label, err)
			}
			policy := createPolicyWithGrant(t, env.DB, "f35_4u8_books_"+suffix, "documentals.llibres.create")
			addGrantToPolicy(t, env.DB, policy, "documentals.llibres.edit")
			addGrantToPolicy(t, env.DB, policy, "documentals.llibres.export")
			addGrantToPolicy(t, env.DB, policy, "documentals.llibres.import")
			assignPolicyToUser(t, env.DB, admin.ID, policy)
			session := createSessionCookie(t, env.DB, admin.ID, "sess_f35_4u8_"+suffix)

			paisID := getOrCreateF298Pais(t, env.DB)
			nivellID, err := env.DB.CreateNivell(&db.NivellAdministratiu{
				PaisID:         paisID,
				Nivel:          1,
				NomNivell:      "Pais F35-4U8 " + suffix,
				TipusNivell:    "pais",
				Estat:          "actiu",
				ModeracioEstat: "pendent",
			})
			if err != nil {
				t.Fatalf("[%s] CreateNivell ha fallat: %v", env.Label, err)
			}
			municipi := &db.Municipi{
				Nom:            "Municipi F35-4U7 U8MultiDB_" + suffix,
				Tipus:          "municipi",
				Estat:          "actiu",
				ModeracioEstat: "pendent",
			}
			municipi.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(nivellID), Valid: true}
			municipiID, err := env.DB.CreateMunicipi(municipi)
			if err != nil {
				t.Fatalf("[%s] CreateMunicipi ha fallat: %v", env.Label, err)
			}
			createF354U7Archive(t, env.DB, admin.ID, municipiID, "f35_4u8_multidb_"+suffix, "Arxiu F35-4U8 "+suffix, 0)

			payload := map[string]interface{}{
				"schema": "cercagenealogica.llibres.v2",
				"items": map[string]interface{}{
					"llibres": []map[string]interface{}{{
						"code":         "f35_4u8_multidb_book_" + suffix,
						"title":        "Llibre F35-4U8 MultiDB " + suffix,
						"book_type":    "baptismes",
						"chronology":   "1910-1915",
						"municipality": map[string]interface{}{"name": "Municipi F35-4U7 U8MultiDB_" + suffix, "country_iso2": "ES"},
						"archives": []map[string]interface{}{{
							"archive_code":      "f35_4u8_multidb_" + suffix,
							"principal":         true,
							"preferred_display": true,
						}},
					}},
				},
			}

			location := postF354U8Import(t, env.App.AdminLlibresImportRun, session, payload)
			if !strings.Contains(location, "llibres_created=1") || !strings.Contains(location, "llibres_errors=0") {
				t.Fatalf("[%s] redirect inesperat: %s", env.Label, location)
			}
			llibre, err := env.DB.ResolveLlibreByStableRef(db.LlibreStableRef{Codi: "f35_4u8_multidb_book_" + suffix})
			if err != nil {
				t.Fatalf("[%s] ResolveLlibreByStableRef ha fallat: %v", env.Label, err)
			}
			if llibre == nil {
				t.Fatalf("[%s] s'esperava 1 llibre v2 creat i no s'ha resolt per codi", env.Label)
			}
		})
	}
}

func TestF354U8ImportV2ReligiousAndCivilArchiveExportContext(t *testing.T) {
	app, database, admin, session := setupF354U7BooksAdmin(t, "test_f35_4u8_export_context.sqlite3")
	paisID, municipiID := seedF354U7BookTerritory(t, database, "U8Export")
	entitatLegacyID := createF354U7LegacyEntity(t, database, paisID, "Bisbat F35-4U8 Export")
	entitatReligiosaID := createF354U7ReligiousEntity(t, database, "f35_4u8_entitat_export", "Parroquia F35-4U8 Export")

	arxiuRelID := createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u8_arxiu_religios", "Arxiu F35-4U8 Religios", entitatLegacyID)
	if _, err := database.SaveArxiuEntitatReligiosa(&db.ArxiuEntitatReligiosa{
		ArxiuID:            arxiuRelID,
		EntitatReligiosaID: entitatReligiosaID,
		TipusRelacio:       "custodia_documentacio",
		Estat:              "actiu",
		ModeracioEstat:     "publicat",
	}); err != nil {
		t.Fatalf("SaveArxiuEntitatReligiosa: %v", err)
	}
	createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u8_arxiu_civil", "Arxiu F35-4U8 Civil", 0)

	payload := map[string]interface{}{
		"schema": "cercagenealogica.llibres.v2",
		"items": map[string]interface{}{
			"llibres": []map[string]interface{}{
				{
					"code":         "f35_4u8_book_religios",
					"title":        "Llibre F35-4U8 Religios",
					"book_type":    "baptismes",
					"chronology":   "1870-1875",
					"municipality": map[string]interface{}{"name": "Municipi F35-4U7 U8Export", "country_iso2": "ES"},
					"archives": []map[string]interface{}{
						{"archive_code": "f35_4u8_arxiu_religios", "principal": true, "preferred_display": true},
					},
				},
				{
					"code":         "f35_4u8_book_civil",
					"title":        "Llibre F35-4U8 Civil",
					"book_type":    "matrimonis",
					"chronology":   "1876-1880",
					"municipality": map[string]interface{}{"name": "Municipi F35-4U7 U8Export", "country_iso2": "ES"},
					"archives": []map[string]interface{}{
						{"archive_code": "f35_4u8_arxiu_civil", "principal": true, "preferred_display": true},
					},
				},
			},
		},
	}

	postF354U8Import(t, app.AdminLlibresImportRun, session, payload)

	req := httptest.NewRequest(http.MethodGet, "/admin/llibres/export", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminLlibresExport(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminLlibresExport status=%d body=%s", rr.Code, rr.Body.String())
	}

	var exported struct {
		Schema string `json:"schema"`
		Items  struct {
			Llibres []struct {
				Code             string `json:"code"`
				ReligiousContext []struct {
					EntityCode string `json:"entity_code"`
				} `json:"religious_context"`
			} `json:"llibres"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &exported); err != nil {
		t.Fatalf("json.Unmarshal export context: %v", err)
	}
	records := map[string][]string{}
	for _, row := range exported.Items.Llibres {
		codes := make([]string, 0, len(row.ReligiousContext))
		for _, rel := range row.ReligiousContext {
			codes = append(codes, rel.EntityCode)
		}
		records[row.Code] = codes
	}
	if !slicesContain(records["f35_4u8_book_religios"], "f35_4u8_entitat_export") {
		t.Fatalf("el llibre religios ha d'exportar religious_context, got=%v", records["f35_4u8_book_religios"])
	}
	if len(records["f35_4u8_book_civil"]) != 0 {
		t.Fatalf("el llibre civil no ha d'exportar religious_context, got=%v", records["f35_4u8_book_civil"])
	}
}

func TestF354U8ImportV2MultiArchiveIdempotentAndDiagnostic(t *testing.T) {
	app, database, admin, session := setupF354U7BooksAdmin(t, "test_f35_4u8_multi.sqlite3")
	_, municipiID := seedF354U7BookTerritory(t, database, "U8Multi")
	createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u8_ahat", "Arxiu F35-4U8 AHAT", 0)
	createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u8_fs", "Arxiu F35-4U8 FamilySearch", 0)

	payload := map[string]interface{}{
		"schema": "cercagenealogica.llibres.v2",
		"items": map[string]interface{}{
			"llibres": []map[string]interface{}{{
				"code":         "f35_4u8_multi_book",
				"title":        "Llibre F35-4U8 Multi",
				"book_type":    "baptismes",
				"chronology":   "1890-1895",
				"digital_code": "DIG-U8-1",
				"municipality": map[string]interface{}{"name": "Municipi F35-4U7 U8Multi", "country_iso2": "ES"},
				"archives": []map[string]interface{}{
					{"archive_code": "f35_4u8_ahat", "relation_type": "custodia_original", "principal": true, "preferred_display": true, "source_system": "ahat", "external_code": "A-1"},
					{"archive_code": "f35_4u8_fs", "relation_type": "copia_digital", "principal": false, "preferred_display": false, "source_system": "familysearch", "external_code": "FS-1"},
				},
				"urls": []map[string]interface{}{
					{"url": "https://example.test/u8/book", "type": "font", "archive_code": "f35_4u8_ahat"},
				},
				"pages": []map[string]interface{}{
					{"page_number": 1, "canonical_label": "f. 1r"},
					{"page_number": 2, "canonical_label": "f. 1v"},
				},
			}},
		},
	}

	postF354U8Import(t, app.AdminLlibresImportRun, session, payload)
	postF354U8Import(t, app.AdminLlibresImportRun, session, payload)

	rows, err := database.Query("SELECT id FROM llibres WHERE codi = ?", "f35_4u8_multi_book")
	if err != nil || len(rows) != 1 {
		t.Fatalf("Query llibre multiarxiu: err=%v rows=%d", err, len(rows))
	}
	llibreID := parseCountValue(t, rows[0]["id"])
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM arxius_llibres WHERE llibre_id = ?", llibreID); got != 2 {
		t.Fatalf("s'esperaven 2 relacions arxiu-llibre, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM llibres_urls WHERE llibre_id = ?", llibreID); got != 1 {
		t.Fatalf("s'esperava 1 URL, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM llibre_pagines WHERE llibre_id = ?", llibreID); got != 2 {
		t.Fatalf("s'esperaven 2 pagines, got %d", got)
	}
	rels, err := database.ListLlibreArxius(llibreID)
	if err != nil {
		t.Fatalf("ListLlibreArxius: %v", err)
	}
	primaryCount := 0
	preferredCount := 0
	for _, rel := range rels {
		if rel.Principal {
			primaryCount++
		}
		if rel.PreferitVisualitzacio {
			preferredCount++
		}
	}
	if primaryCount != 1 || preferredCount != 1 {
		t.Fatalf("principal/preferit han de ser unics, got principal=%d preferit=%d", primaryCount, preferredCount)
	}
	result := latestF354U8ImportResult(t, database)
	if asJSONInt(result["created_books"]) != 0 || asJSONInt(result["existing_books"]) == 0 {
		t.Fatalf("resultat import idempotent inesperat: %+v", result)
	}
	if asJSONInt(result["skipped_archive_links"]) == 0 || asJSONInt(result["skipped_urls"]) == 0 || asJSONInt(result["skipped_pages"]) == 0 {
		t.Fatalf("la segona execucio ha de marcar duplicats/skip, got %+v", result)
	}
}

func TestF354U8ImportV2ArchiveAndMunicipalityErrorsAreDiagnostic(t *testing.T) {
	app, database, _, session := setupF354U7BooksAdmin(t, "test_f35_4u8_errors.sqlite3")
	payload := map[string]interface{}{
		"schema": "cercagenealogica.llibres.v2",
		"items": map[string]interface{}{
			"llibres": []map[string]interface{}{
				{
					"code":         "f35_4u8_missing_mun",
					"title":        "Sense municipi",
					"book_type":    "baptismes",
					"chronology":   "1800-1801",
					"municipality": map[string]interface{}{"name": "Municipi inexistent", "country_iso2": "ES"},
					"archives":     []map[string]interface{}{{"archive_code": "archive_inexistent"}},
				},
				{
					"code":         "f35_4u8_missing_archive",
					"title":        "Arxiu inexistent",
					"book_type":    "baptismes",
					"chronology":   "1801-1802",
					"municipality": map[string]interface{}{"name": "Municipi F35-4U7 U8Errors", "country_iso2": "ES"},
					"archives":     []map[string]interface{}{{"archive_code": "archive_inexistent_visible"}},
				},
			},
		},
	}
	seedF354U7BookTerritory(t, database, "U8Errors")
	postF354U8Import(t, app.AdminLlibresImportRun, session, payload)
	result := latestF354U8ImportResult(t, database)
	errorsByReason, _ := result["errors_by_reason"].(map[string]interface{})
	if asJSONInt(errorsByReason["municipality_not_found"]) != 1 {
		t.Fatalf("esperava municipality_not_found=1, got %+v", errorsByReason)
	}
	if asJSONInt(errorsByReason["archive_not_found"]) != 1 {
		t.Fatalf("esperava archive_not_found=1, got %+v", errorsByReason)
	}
	firstErrors, _ := result["first_errors"].([]interface{})
	visibleArchive := false
	for _, item := range firstErrors {
		if strings.Contains(asString(item), "archive_inexistent_visible") {
			visibleArchive = true
			break
		}
	}
	if !visibleArchive {
		t.Fatalf("first_errors ha de mostrar l'archive_code que falla, got=%v", firstErrors)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM llibres"); got != 0 {
		t.Fatalf("no s'hauria de crear cap llibre amb errors de municipi/arxiu, got %d", got)
	}
}

func TestF354U8ImportV2PublishedExistingBookDoesNotDirectlyUpdate(t *testing.T) {
	app, database, admin, session := setupF354U7BooksAdmin(t, "test_f35_4u8_published.sqlite3")
	_, municipiID := seedF354U7BookTerritory(t, database, "U8Published")
	createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u8_pub_archive", "Arxiu Publicat U8", 0)
	llibreID := createF354U7Book(t, database, admin.ID, municipiID, 0, "f35_4u8_published_book", "Llibre Publicat U8")
	publishF354U7Book(t, database, llibreID)

	payload := map[string]interface{}{
		"schema": "cercagenealogica.llibres.v2",
		"items": map[string]interface{}{
			"llibres": []map[string]interface{}{{
				"code":         "f35_4u8_published_book",
				"title":        "Llibre Publicat U8 Actualitzat",
				"book_type":    "baptismes",
				"chronology":   "1900-1905",
				"municipality": map[string]interface{}{"name": "Municipi F35-4U7 U8Published", "country_iso2": "ES"},
				"archives":     []map[string]interface{}{{"archive_code": "f35_4u8_pub_archive"}},
			}},
		},
	}

	postF354U8Import(t, app.AdminLlibresImportRun, session, payload)
	llibre, err := database.GetLlibre(llibreID)
	if err != nil || llibre == nil {
		t.Fatalf("GetLlibre published: err=%v llibre=%v", err, llibre)
	}
	if llibre.Titol != "Llibre Publicat U8" {
		t.Fatalf("el llibre publicat no s'ha d'actualitzar en dur, got %q", llibre.Titol)
	}
	result := latestF354U8ImportResult(t, database)
	errorsByReason, _ := result["errors_by_reason"].(map[string]interface{})
	if asJSONInt(errorsByReason["published_book_requires_moderated_change"]) != 1 {
		t.Fatalf("esperava published_book_requires_moderated_change=1, got %+v", errorsByReason)
	}
}

func TestF354U8ImportV2PublishedExistingBookRejectsNewURL(t *testing.T) {
	app, database, admin, session := setupF354U7BooksAdmin(t, "test_f35_4u8_published_new_url.sqlite3")
	_, municipiID := seedF354U7BookTerritory(t, database, "U8PublishedURL")
	arxiuID := createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u8_pub_url_archive", "Arxiu Publicat URL U8", 0)
	llibreID := createF354U7Book(t, database, admin.ID, municipiID, 0, "f35_4u8_published_url_book", "Llibre Publicat URL U8")
	publishF354U7Book(t, database, llibreID)
	seedF354U8PublishedArchiveLink(t, database, llibreID, arxiuID, "custodia_original", true, true, "", "", "")

	payload := map[string]interface{}{
		"schema": "cercagenealogica.llibres.v2",
		"items": map[string]interface{}{
			"llibres": []map[string]interface{}{{
				"code":         "f35_4u8_published_url_book",
				"title":        "Llibre Publicat URL U8",
				"book_type":    "baptismes",
				"chronology":   "1900-1905",
				"municipality": map[string]interface{}{"name": "Municipi F35-4U7 U8PublishedURL", "country_iso2": "ES"},
				"archives":     []map[string]interface{}{{"archive_code": "f35_4u8_pub_url_archive", "principal": true, "preferred_display": true, "relation_type": "custodia_original"}},
				"urls":         []map[string]interface{}{{"url": "https://example.test/published/new-url", "type": "font", "archive_code": "f35_4u8_pub_url_archive"}},
			}},
		},
	}

	postF354U8Import(t, app.AdminLlibresImportRun, session, payload)
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM llibres_urls WHERE llibre_id = ?", llibreID); got != 0 {
		t.Fatalf("no s'ha de crear cap URL nova en llibre publicat, got %d", got)
	}
	assertF354U8PublishedBlocked(t, database, "f35_4u8_published_url_book")
}

func TestF354U8ImportV2PublishedExistingBookRejectsNewPage(t *testing.T) {
	app, database, admin, session := setupF354U7BooksAdmin(t, "test_f35_4u8_published_new_page.sqlite3")
	_, municipiID := seedF354U7BookTerritory(t, database, "U8PublishedPage")
	arxiuID := createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u8_pub_page_archive", "Arxiu Publicat Page U8", 0)
	llibreID := createF354U7Book(t, database, admin.ID, municipiID, 0, "f35_4u8_published_page_book", "Llibre Publicat Page U8")
	publishF354U7Book(t, database, llibreID)
	seedF354U8PublishedArchiveLink(t, database, llibreID, arxiuID, "custodia_original", true, true, "", "", "")

	payload := map[string]interface{}{
		"schema": "cercagenealogica.llibres.v2",
		"items": map[string]interface{}{
			"llibres": []map[string]interface{}{{
				"code":         "f35_4u8_published_page_book",
				"title":        "Llibre Publicat Page U8",
				"book_type":    "baptismes",
				"chronology":   "1900-1905",
				"municipality": map[string]interface{}{"name": "Municipi F35-4U7 U8PublishedPage", "country_iso2": "ES"},
				"archives":     []map[string]interface{}{{"archive_code": "f35_4u8_pub_page_archive", "principal": true, "preferred_display": true, "relation_type": "custodia_original"}},
				"pages":        []map[string]interface{}{{"page_number": 1, "canonical_label": "f. 1r"}},
			}},
		},
	}

	postF354U8Import(t, app.AdminLlibresImportRun, session, payload)
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM llibre_pagines WHERE llibre_id = ?", llibreID); got != 0 {
		t.Fatalf("no s'ha de crear cap pagina nova en llibre publicat, got %d", got)
	}
	assertF354U8PublishedBlocked(t, database, "f35_4u8_published_page_book")
}

func TestF354U8ImportV2PublishedExistingBookRejectsNewArchiveLink(t *testing.T) {
	app, database, admin, session := setupF354U7BooksAdmin(t, "test_f35_4u8_published_new_archive.sqlite3")
	_, municipiID := seedF354U7BookTerritory(t, database, "U8PublishedArchive")
	arxiuAID := createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u8_pub_archive_a", "Arxiu Publicat A U8", 0)
	arxiuBID := createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u8_pub_archive_b", "Arxiu Publicat B U8", 0)
	llibreID := createF354U7Book(t, database, admin.ID, municipiID, 0, "f35_4u8_published_archive_book", "Llibre Publicat Archive U8")
	publishF354U7Book(t, database, llibreID)
	seedF354U8PublishedArchiveLink(t, database, llibreID, arxiuAID, "custodia_original", true, true, "", "", "")

	payload := map[string]interface{}{
		"schema": "cercagenealogica.llibres.v2",
		"items": map[string]interface{}{
			"llibres": []map[string]interface{}{{
				"code":         "f35_4u8_published_archive_book",
				"title":        "Llibre Publicat Archive U8",
				"book_type":    "baptismes",
				"chronology":   "1900-1905",
				"municipality": map[string]interface{}{"name": "Municipi F35-4U7 U8PublishedArchive", "country_iso2": "ES"},
				"archives": []map[string]interface{}{
					{"archive_code": "f35_4u8_pub_archive_a", "principal": true, "preferred_display": true, "relation_type": "custodia_original"},
					{"archive_code": "f35_4u8_pub_archive_b", "principal": false, "preferred_display": false, "relation_type": "copia_digital"},
				},
			}},
		},
	}

	postF354U8Import(t, app.AdminLlibresImportRun, session, payload)
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM arxius_llibres WHERE llibre_id = ? AND arxiu_id = ?", llibreID, arxiuBID); got != 0 {
		t.Fatalf("no s'ha de crear cap relacio nova en llibre publicat, got %d", got)
	}
	assertF354U8PublishedBlocked(t, database, "f35_4u8_published_archive_book")
}

func TestF354U8ImportV2PublishedExistingBookRejectsArchiveLinkMetadataChange(t *testing.T) {
	app, database, admin, session := setupF354U7BooksAdmin(t, "test_f35_4u8_published_link_meta.sqlite3")
	_, municipiID := seedF354U7BookTerritory(t, database, "U8PublishedMeta")
	arxiuID := createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u8_pub_meta_archive", "Arxiu Publicat Meta U8", 0)
	llibreID := createF354U7Book(t, database, admin.ID, municipiID, 0, "f35_4u8_published_meta_book", "Llibre Publicat Meta U8")
	publishF354U7Book(t, database, llibreID)
	seedF354U8PublishedArchiveLink(t, database, llibreID, arxiuID, "custodia_original", true, true, "ahat", "", "A-1")

	payload := map[string]interface{}{
		"schema": "cercagenealogica.llibres.v2",
		"items": map[string]interface{}{
			"llibres": []map[string]interface{}{{
				"code":         "f35_4u8_published_meta_book",
				"title":        "Llibre Publicat Meta U8",
				"book_type":    "baptismes",
				"chronology":   "1900-1905",
				"municipality": map[string]interface{}{"name": "Municipi F35-4U7 U8PublishedMeta", "country_iso2": "ES"},
				"archives": []map[string]interface{}{
					{"archive_code": "f35_4u8_pub_meta_archive", "principal": false, "preferred_display": false, "relation_type": "copia_digital", "source_system": "familysearch", "external_code": "FS-1"},
				},
			}},
		},
	}

	postF354U8Import(t, app.AdminLlibresImportRun, session, payload)
	rels, err := database.ListLlibreArxius(llibreID)
	if err != nil {
		t.Fatalf("ListLlibreArxius: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("s'esperava 1 relacio existent, got %d", len(rels))
	}
	rel := rels[0]
	if !rel.Principal || !rel.PreferitVisualitzacio || strings.TrimSpace(rel.TipusRelacio) != "custodia_original" || strings.TrimSpace(rel.SourceSystem.String) != "ahat" || strings.TrimSpace(rel.ExternalCode.String) != "A-1" {
		t.Fatalf("la relacio publicada no s'ha de mutar, got %+v", rel)
	}
	assertF354U8PublishedBlocked(t, database, "f35_4u8_published_meta_book")
}

func TestF354U8ImportV2PublishedExistingBookNoOpReal(t *testing.T) {
	app, database, admin, session := setupF354U7BooksAdmin(t, "test_f35_4u8_published_noop.sqlite3")
	_, municipiID := seedF354U7BookTerritory(t, database, "U8PublishedNoop")
	arxiuID := createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u8_pub_noop_archive", "Arxiu Publicat Noop U8", 0)
	llibreID := createF354U7Book(t, database, admin.ID, municipiID, 0, "f35_4u8_published_noop_book", "Llibre Publicat Noop U8")
	if _, err := database.Query("UPDATE llibres SET tipus_llibre = ?, cronologia = ? WHERE id = ?", "baptismes", "1900-1905", llibreID); err != nil {
		t.Fatalf("UPDATE llibre noop: %v", err)
	}
	publishF354U7Book(t, database, llibreID)
	seedF354U8PublishedArchiveLink(t, database, llibreID, arxiuID, "custodia_original", true, true, "ahat", "", "A-1")
	if err := database.AddLlibreURL(&db.LlibreURL{
		LlibreID:  llibreID,
		ArxiuID:   sql.NullInt64{Int64: int64(arxiuID), Valid: true},
		URL:       "https://example.test/published/noop",
		Tipus:     sql.NullString{String: "font", Valid: true},
		CreatedBy: sql.NullInt64{Int64: int64(admin.ID), Valid: true},
	}); err != nil {
		t.Fatalf("AddLlibreURL noop: %v", err)
	}
	if _, err := database.SaveLlibrePagina(&db.LlibrePagina{LlibreID: llibreID, NumPagina: 1, Estat: "pendent", Notes: "f. 1r"}); err != nil {
		t.Fatalf("SaveLlibrePagina noop: %v", err)
	}

	payload := map[string]interface{}{
		"schema": "cercagenealogica.llibres.v2",
		"items": map[string]interface{}{
			"llibres": []map[string]interface{}{{
				"code":         "f35_4u8_published_noop_book",
				"title":        "Llibre Publicat Noop U8",
				"book_type":    "baptismes",
				"chronology":   "1900-1905",
				"municipality": map[string]interface{}{"name": "Municipi F35-4U7 U8PublishedNoop", "country_iso2": "ES"},
				"archives": []map[string]interface{}{
					{"archive_code": "f35_4u8_pub_noop_archive", "principal": true, "preferred_display": true, "relation_type": "custodia_original", "source_system": "ahat", "external_code": "A-1"},
				},
				"urls":  []map[string]interface{}{{"url": "https://example.test/published/noop", "type": "font", "archive_code": "f35_4u8_pub_noop_archive"}},
				"pages": []map[string]interface{}{{"page_number": 1, "canonical_label": "f. 1r"}},
			}},
		},
	}

	postF354U8Import(t, app.AdminLlibresImportRun, session, payload)
	result := latestF354U8ImportResult(t, database)
	if asJSONInt(result["errors_total"]) != 0 || asJSONInt(result["existing_books"]) != 1 || asJSONInt(result["created_archive_links"]) != 0 || asJSONInt(result["created_urls"]) != 0 || asJSONInt(result["created_pages"]) != 0 {
		t.Fatalf("el llibre publicat no-op ha de quedar com existing sense canvis, got %+v", result)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM arxius_llibres WHERE llibre_id = ?", llibreID); got != 1 {
		t.Fatalf("no-op no ha de duplicar relacions, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM llibres_urls WHERE llibre_id = ?", llibreID); got != 1 {
		t.Fatalf("no-op no ha de duplicar URLs, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM llibre_pagines WHERE llibre_id = ?", llibreID); got != 1 {
		t.Fatalf("no-op no ha de duplicar pagines, got %d", got)
	}
}

func TestF354U8ImportV2FallbackAmbiguityIsDiagnostic(t *testing.T) {
	app, database, admin, session := setupF354U7BooksAdmin(t, "test_f35_4u8_ambiguous.sqlite3")
	_, municipiID := seedF354U7BookTerritory(t, database, "U8Amb")
	createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u8_amb_archive", "Arxiu Amb U8", 0)
	createF354U7Book(t, database, admin.ID, municipiID, 0, "", "Llibre Ambiguous")
	createF354U7Book(t, database, admin.ID, municipiID, 0, "", "Llibre Ambiguous")
	if _, err := database.Query("UPDATE llibres SET tipus_llibre = ?, cronologia = ? WHERE titol = ?", "baptismes", "1900-1905", "Llibre Ambiguous"); err != nil {
		t.Fatalf("UPDATE llibres ambiguous: %v", err)
	}

	payload := map[string]interface{}{
		"schema": "cercagenealogica.llibres.v2",
		"items": map[string]interface{}{
			"llibres": []map[string]interface{}{{
				"title":        "Llibre Ambiguous",
				"book_type":    "baptismes",
				"chronology":   "1900-1905",
				"municipality": map[string]interface{}{"name": "Municipi F35-4U7 U8Amb", "country_iso2": "ES"},
				"archives":     []map[string]interface{}{{"archive_code": "f35_4u8_amb_archive"}},
			}},
		},
	}

	postF354U8Import(t, app.AdminLlibresImportRun, session, payload)
	result := latestF354U8ImportResult(t, database)
	errorsByReason, _ := result["errors_by_reason"].(map[string]interface{})
	if asJSONInt(errorsByReason["book_duplicate_ambiguous"]) != 1 {
		t.Fatalf("esperava book_duplicate_ambiguous=1, got %+v", errorsByReason)
	}
}

func TestF354U8ImportV2ExternalCodeWithoutSourceSystemDoesNotChooseRandomBook(t *testing.T) {
	app, database, admin, session := setupF354U7BooksAdmin(t, "test_f35_4u8_external_code_no_source.sqlite3")
	_, municipiID := seedF354U7BookTerritory(t, database, "U8ExternalCode")
	createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u8_ext_archive", "Arxiu Ext U8", 0)
	llibreAID := createF354U7Book(t, database, admin.ID, municipiID, 0, "", "Llibre Ext Ambiguous")
	llibreBID := createF354U7Book(t, database, admin.ID, municipiID, 0, "", "Llibre Ext Ambiguous")
	if _, err := database.Query("UPDATE llibres SET tipus_llibre = ?, cronologia = ?, source_system = ?, external_code = ? WHERE id = ?", "baptismes", "1900-1905", "ahat", "EXT-COLLIDE", llibreAID); err != nil {
		t.Fatalf("UPDATE llibre A ext code: %v", err)
	}
	if _, err := database.Query("UPDATE llibres SET tipus_llibre = ?, cronologia = ?, source_system = ?, external_code = ? WHERE id = ?", "baptismes", "1900-1905", "familysearch", "EXT-COLLIDE", llibreBID); err != nil {
		t.Fatalf("UPDATE llibre B ext code: %v", err)
	}

	payload := map[string]interface{}{
		"schema": "cercagenealogica.llibres.v2",
		"items": map[string]interface{}{
			"llibres": []map[string]interface{}{{
				"title":         "Llibre Ext Ambiguous",
				"book_type":     "baptismes",
				"chronology":    "1900-1905",
				"external_code": "EXT-COLLIDE",
				"municipality":  map[string]interface{}{"name": "Municipi F35-4U7 U8ExternalCode", "country_iso2": "ES"},
				"archives":      []map[string]interface{}{{"archive_code": "f35_4u8_ext_archive"}},
			}},
		},
	}

	postF354U8Import(t, app.AdminLlibresImportRun, session, payload)
	result := latestF354U8ImportResult(t, database)
	errorsByReason, _ := result["errors_by_reason"].(map[string]interface{})
	if asJSONInt(errorsByReason["book_duplicate_ambiguous"]) != 1 {
		t.Fatalf("esperava book_duplicate_ambiguous=1 quan external_code no te source_system segur, got %+v", errorsByReason)
	}
}

func seedF354U8PublishedArchiveLink(t *testing.T, database db.DB, llibreID, arxiuID int, relationType string, principal, preferred bool, sourceSystem, externalID, externalCode string) {
	t.Helper()
	saveLink := requireF354U7LinkSaver(t, database)
	if err := saveLink(&db.ArxiuLlibreLink{
		ArxiuID:               arxiuID,
		LlibreID:              llibreID,
		TipusRelacio:          relationType,
		Principal:             principal,
		PreferitVisualitzacio: preferred,
		SourceSystem:          sourceSystem,
		ExternalID:            externalID,
		ExternalCode:          externalCode,
		Estat:                 "actiu",
		ModeracioEstat:        "publicat",
	}); err != nil {
		t.Fatalf("seed published archive link: %v", err)
	}
}

func assertF354U8PublishedBlocked(t *testing.T, database db.DB, label string) {
	t.Helper()
	result := latestF354U8ImportResult(t, database)
	errorsByReason, _ := result["errors_by_reason"].(map[string]interface{})
	if asJSONInt(errorsByReason["published_book_requires_moderated_change"]) != 1 {
		t.Fatalf("esperava published_book_requires_moderated_change=1, got %+v", errorsByReason)
	}
	firstErrors, _ := result["first_errors"].([]interface{})
	found := false
	for _, item := range firstErrors {
		if strings.Contains(asString(item), label) {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("first_errors ha d'incloure %q, got %v", label, firstErrors)
	}
}

func postF354U8Import(t *testing.T, handler http.HandlerFunc, session *http.Cookie, payload interface{}) string {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal payload import v2: %v", err)
	}
	csrfToken := "csrf_f35_4u8_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	_ = writer.WriteField("csrf_token", csrfToken)
	part, err := writer.CreateFormFile("import_file", "llibres-v2.json")
	if err != nil {
		t.Fatalf("CreateFormFile import v2: %v", err)
	}
	if _, err := io.Copy(part, bytes.NewReader(raw)); err != nil {
		t.Fatalf("io.Copy import v2: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close import v2: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/admin/llibres/import/run", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfToken))
	rr := httptest.NewRecorder()
	handler(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("import v2 status=%d body=%s", rr.Code, rr.Body.String())
	}
	return rr.Header().Get("Location")
}

func latestF354U8ImportResult(t *testing.T, database db.DB) map[string]interface{} {
	t.Helper()
	rows, err := database.Query("SELECT result_json FROM admin_jobs WHERE kind = ? ORDER BY id DESC LIMIT 1", "admin_import")
	if err != nil || len(rows) == 0 {
		t.Fatalf("Query admin_jobs import result: err=%v rows=%d", err, len(rows))
	}
	raw := strings.TrimSpace(asString(rows[0]["result_json"]))
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		t.Fatalf("json.Unmarshal admin_jobs result: %v raw=%s", err, raw)
	}
	return result
}

func asJSONInt(v interface{}) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	default:
		return 0
	}
}

func parseMapIntValue(v interface{}, key string) int {
	if key == "" {
		return 0
	}
	m, ok := v.(map[string]interface{})
	if !ok {
		return 0
	}
	return asJSONInt(m[key])
}
