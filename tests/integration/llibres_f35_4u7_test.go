package integration

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF354U7CreateLlibreV2WithoutEntityUsesArchiveMunicipalityBase(t *testing.T) {
	app, database, admin, session := setupF354U7BooksAdmin(t, "test_f35_4u7_create.sqlite3")
	_, municipiID := seedF354U7BookTerritory(t, database, "Create")
	arxiuID := createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u7_create_arxiu", "Arxiu F35-4U7 Create", 0)

	form := url.Values{}
	form.Set("arxiu_id", strconv.Itoa(arxiuID))
	form.Set("municipi_id", strconv.Itoa(municipiID))
	form.Set("titol", "Llibre F35-4U7 Sense Entitat")
	form.Set("tipus_llibre", "baptismes")
	form.Set("cronologia", "1901-1909")
	form.Set("codi", "f35_4u7_book_create")
	form.Set("source_system", "ahat")
	form.Set("external_id", "EXT-BOOK-001")
	form.Set("external_code", "BOOK-001")

	req := httptest.NewRequest(http.MethodPost, "/documentals/llibres/save", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminSaveLlibre(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("AdminSaveLlibre status=%d body=%s", rr.Code, rr.Body.String())
	}

	rows, err := database.Query("SELECT id, arquevisbat_id, moderation_status, codi, source_system, external_id, external_code FROM llibres WHERE codi = ?", "f35_4u7_book_create")
	if err != nil || len(rows) != 1 {
		t.Fatalf("no s'ha pogut llegir el llibre creat: err=%v rows=%d", err, len(rows))
	}
	llibreID := parseCountValue(t, rows[0]["id"])
	if got := strings.TrimSpace(asString(rows[0]["arquevisbat_id"])); got != "" && got != "0" {
		t.Fatalf("el llibre v2 sense entitat no ha de forçar arquevisbat_id, got %q", got)
	}
	if got := strings.TrimSpace(asString(rows[0]["moderation_status"])); got != "pendent" {
		t.Fatalf("el llibre v2 nou ha d'entrar pendent, got %q", got)
	}
	if got := strings.TrimSpace(asString(rows[0]["source_system"])); got != "ahat" {
		t.Fatalf("source_system inesperat: %q", got)
	}
	if got := strings.TrimSpace(asString(rows[0]["external_id"])); got != "EXT-BOOK-001" {
		t.Fatalf("external_id inesperat: %q", got)
	}
	if got := strings.TrimSpace(asString(rows[0]["external_code"])); got != "BOOK-001" {
		t.Fatalf("external_code inesperat: %q", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM arxius_llibres WHERE llibre_id = ? AND arxiu_id = ?", llibreID, arxiuID); got != 1 {
		t.Fatalf("s'esperava 1 relacio arxiu-llibre, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM usuaris_activitat WHERE usuari_id = ? AND estat = 'pendent' AND objecte_tipus = 'llibre' AND objecte_id = ?", admin.ID, llibreID); got != 1 {
		t.Fatalf("s'esperava 1 activitat pendent de creacio de llibre, got %d", got)
	}

	byCode, err := database.ResolveLlibreByStableRef(db.LlibreStableRef{Codi: "f35_4u7_book_create"})
	if err != nil || byCode == nil || byCode.ID != llibreID {
		t.Fatalf("ResolveLlibreByStableRef per codi ha fallat: err=%v llibre=%+v", err, byCode)
	}
	byExternal, err := database.ResolveLlibreByStableRef(db.LlibreStableRef{
		SourceSystem: "ahat",
		ExternalID:   "EXT-BOOK-001",
	})
	if err != nil || byExternal == nil || byExternal.ID != llibreID {
		t.Fatalf("ResolveLlibreByStableRef per external_id ha fallat: err=%v llibre=%+v", err, byExternal)
	}
}

func TestF354U7LlibresExportV2DerivesReligiousContextFromArchive(t *testing.T) {
	app, database, admin, session := setupF354U7BooksAdmin(t, "test_f35_4u7_export.sqlite3")
	paisID, municipiID := seedF354U7BookTerritory(t, database, "Export")
	entitatLegacyID := createF354U7LegacyEntity(t, database, paisID, "Bisbat F35-4U7 Export")
	entitatReligiosaID := createF354U7ReligiousEntity(t, database, "f35_4u7_entitat_export", "Parroquia F35-4U7 Export")

	arxiuRelID := createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u7_arxiu_religios", "Arxiu F35-4U7 Religios", entitatLegacyID)
	if _, err := database.SaveArxiuEntitatReligiosa(&db.ArxiuEntitatReligiosa{
		ArxiuID:            arxiuRelID,
		EntitatReligiosaID: entitatReligiosaID,
		TipusRelacio:       "custodia_documentacio",
		Estat:              "actiu",
		ModeracioEstat:     "publicat",
	}); err != nil {
		t.Fatalf("SaveArxiuEntitatReligiosa: %v", err)
	}
	arxiuCivilID := createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u7_arxiu_civil", "Arxiu F35-4U7 Civil", 0)

	llibreRelID := createF354U7Book(t, database, admin.ID, municipiID, 0, "f35_4u7_book_religios", "Llibre F35-4U7 Religios")
	llibreCivilID := createF354U7Book(t, database, admin.ID, municipiID, 0, "f35_4u7_book_civil", "Llibre F35-4U7 Civil")
	if err := database.AddArxiuLlibre(arxiuRelID, llibreRelID, "SIG-REL", "https://example.test/rel"); err != nil {
		t.Fatalf("AddArxiuLlibre religios: %v", err)
	}
	if err := database.AddArxiuLlibre(arxiuCivilID, llibreCivilID, "SIG-CIV", "https://example.test/civ"); err != nil {
		t.Fatalf("AddArxiuLlibre civil: %v", err)
	}

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
				Code        string `json:"code"`
				ArchiveRefs []struct {
					ArchiveCode string `json:"archive_code"`
				} `json:"archive_refs"`
				ReligiousContext *struct {
					EntityCode string `json:"entity_code"`
				} `json:"religious_context"`
				Legacy struct {
					OldID int `json:"old_id"`
				} `json:"legacy"`
			} `json:"llibres"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &exported); err != nil {
		t.Fatalf("json export v2 invalid: %v", err)
	}
	if exported.Schema != "cercagenealogica.llibres.v2" {
		t.Fatalf("schema export inesperat: %q", exported.Schema)
	}
	records := map[string]struct {
		OldID         int
		ArchiveCode   string
		ReligiousCode string
	}{}
	for _, row := range exported.Items.Llibres {
		archiveCode := ""
		if len(row.ArchiveRefs) > 0 {
			archiveCode = row.ArchiveRefs[0].ArchiveCode
		}
		religiousCode := ""
		if row.ReligiousContext != nil {
			religiousCode = row.ReligiousContext.EntityCode
		}
		records[row.Code] = struct {
			OldID         int
			ArchiveCode   string
			ReligiousCode string
		}{
			OldID:         row.Legacy.OldID,
			ArchiveCode:   archiveCode,
			ReligiousCode: religiousCode,
		}
	}
	if got := records["f35_4u7_book_religios"]; got.OldID != llibreRelID || got.ArchiveCode != "f35_4u7_arxiu_religios" || got.ReligiousCode != "f35_4u7_entitat_export" {
		t.Fatalf("export v2 del llibre religios inesperat: %+v", got)
	}
	if got := records["f35_4u7_book_civil"]; got.OldID != llibreCivilID || got.ArchiveCode != "f35_4u7_arxiu_civil" || got.ReligiousCode != "" {
		t.Fatalf("export v2 del llibre civil inesperat: %+v", got)
	}
}

func setupF354U7BooksAdmin(t *testing.T, dbName string) (*core.App, db.DB, *db.User, *http.Cookie) {
	t.Helper()
	app, database := newTestAppForLogin(t, dbName)
	admin := createTestUser(t, database, "f35_4u7_admin_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	policy := createPolicyWithGrant(t, database, "f35_4u7_books_"+strconv.FormatInt(time.Now().UnixNano(), 10), "documentals.llibres.create")
	addGrantToPolicy(t, database, policy, "documentals.llibres.edit")
	addGrantToPolicy(t, database, policy, "documentals.llibres.export")
	assignPolicyToUser(t, database, admin.ID, policy)
	session := createSessionCookie(t, database, admin.ID, "sess_f35_4u7_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	return app, database, admin, session
}

func seedF354U7BookTerritory(t *testing.T, database db.DB, suffix string) (int, int) {
	t.Helper()
	paisID, err := database.CreatePais(&db.Pais{CodiISO2: "ES", CodiISO3: "ESP", CodiPaisNum: "724"})
	if err != nil {
		t.Fatalf("CreatePais: %v", err)
	}
	nivellID, err := database.CreateNivell(&db.NivellAdministratiu{
		PaisID:         paisID,
		Nivel:          1,
		NomNivell:      "Pais F35-4U7 " + suffix,
		TipusNivell:    "pais",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	})
	if err != nil {
		t.Fatalf("CreateNivell: %v", err)
	}
	mun := &db.Municipi{
		Nom:            "Municipi F35-4U7 " + suffix,
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	}
	mun.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(nivellID), Valid: true}
	municipiID, err := database.CreateMunicipi(mun)
	if err != nil {
		t.Fatalf("CreateMunicipi: %v", err)
	}
	return paisID, municipiID
}

func createF354U7Archive(t *testing.T, database db.DB, userID, municipiID int, codi, nom string, legacyEntitatID int) int {
	t.Helper()
	arxiu := &db.Arxiu{
		Codi:                  codi,
		Nom:                   nom,
		Tipus:                 "municipal",
		Acces:                 "public",
		MunicipiID:            sql.NullInt64{Int64: int64(municipiID), Valid: true},
		CreatedBy:             sql.NullInt64{Int64: int64(userID), Valid: true},
		ModeracioEstat:        "publicat",
		EntitatEclesiasticaID: sql.NullInt64{Int64: int64(legacyEntitatID), Valid: legacyEntitatID > 0},
	}
	arxiuID, err := database.CreateArxiu(arxiu)
	if err != nil {
		t.Fatalf("CreateArxiu %s: %v", codi, err)
	}
	return arxiuID
}

func createF354U7LegacyEntity(t *testing.T, database db.DB, paisID int, nom string) int {
	t.Helper()
	id, err := database.CreateArquebisbat(&db.Arquebisbat{
		Nom:            nom,
		TipusEntitat:   "bisbat",
		PaisID:         sql.NullInt64{Int64: int64(paisID), Valid: true},
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateArquebisbat: %v", err)
	}
	return id
}

func createF354U7ReligiousEntity(t *testing.T, database db.DB, codi, nom string) int {
	t.Helper()
	id, err := database.SaveEntitatReligiosa(&db.EntitatReligiosa{
		Codi:                   codi,
		Nom:                    nom,
		ReligioConfessioCodi:   "catolicisme_ritu_llati",
		NivellConfessionalCodi: "parroquia",
		Estat:                  "actiu",
		ModeracioEstat:         "publicat",
	})
	if err != nil {
		t.Fatalf("SaveEntitatReligiosa: %v", err)
	}
	return id
}

func createF354U7Book(t *testing.T, database db.DB, userID, municipiID, legacyEntitatID int, codi, titol string) int {
	t.Helper()
	llibre := &db.Llibre{
		Codi:           codi,
		Titol:          titol,
		TipusLlibre:    "baptismes",
		Cronologia:     "1900-1905",
		MunicipiID:     municipiID,
		ArquebisbatID:  legacyEntitatID,
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
		ModeracioEstat: "pendent",
	}
	llibreID, err := database.CreateLlibre(llibre)
	if err != nil {
		t.Fatalf("CreateLlibre %s: %v", codi, err)
	}
	return llibreID
}
