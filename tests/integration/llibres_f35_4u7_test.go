package integration

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"html"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func mustReadProjectFileF354U7(t *testing.T, root string, parts ...string) string {
	t.Helper()
	path := filepath.Clean(filepath.Join(append([]string{root}, parts...)...))
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile %s: %v", path, err)
	}
	return string(body)
}

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

	createdBook, err := database.ResolveLlibreByStableRef(db.LlibreStableRef{Codi: "f35_4u7_book_create"})
	if err != nil || createdBook == nil {
		t.Fatalf("ResolveLlibreByStableRef llibre creat: err=%v llibre=%v", err, createdBook)
	}
	llibreID := createdBook.ID
	llibre, err := database.GetLlibre(llibreID)
	if err != nil || llibre == nil {
		t.Fatalf("GetLlibre creat: err=%v llibre=%v", err, llibre)
	}
	if got := strings.TrimSpace(strconv.Itoa(llibre.ArquebisbatID)); got != "" && got != "0" {
		t.Fatalf("el llibre v2 sense entitat no ha de forçar arquevisbat_id, got %q", got)
	}
	if got := strings.TrimSpace(llibre.ModeracioEstat); got != "pendent" {
		t.Fatalf("el llibre v2 nou ha d'entrar pendent, got %q", got)
	}
	if got := strings.TrimSpace(llibre.SourceSystem); got != "ahat" {
		t.Fatalf("source_system inesperat: %q", got)
	}
	if got := strings.TrimSpace(llibre.ExternalID); got != "EXT-BOOK-001" {
		t.Fatalf("external_id inesperat: %q", got)
	}
	if got := strings.TrimSpace(llibre.ExternalCode); got != "BOOK-001" {
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
	saveLink := requireF354U7LinkSaver(t, database)
	if err := saveLink(&db.ArxiuLlibreLink{
		ArxiuID:               arxiuRelID,
		LlibreID:              llibreRelID,
		Signatura:             "SIG-REL",
		URLOverride:           "https://example.test/rel",
		Principal:             true,
		PreferitVisualitzacio: true,
		SourceSystem:          "ahat",
		ExternalID:            "COPY-001",
		ExternalCode:          "REL-001",
		Notes:                 "original",
		Estat:                 "actiu",
		ModeracioEstat:        "publicat",
	}); err != nil {
		t.Fatalf("SaveArxiuLlibreLink religios: %v", err)
	}
	if err := saveLink(&db.ArxiuLlibreLink{
		ArxiuID:               arxiuCivilID,
		LlibreID:              llibreRelID,
		Signatura:             "SIG-COPY",
		URLOverride:           "https://example.test/copy",
		TipusRelacio:          "copia_digital",
		Principal:             false,
		PreferitVisualitzacio: false,
		SourceSystem:          "familysearch",
		ExternalID:            "COPY-002",
		ExternalCode:          "CIV-002",
		Notes:                 "copy",
		Estat:                 "actiu",
		ModeracioEstat:        "publicat",
	}); err != nil {
		t.Fatalf("SaveArxiuLlibreLink copia: %v", err)
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
				Code     string `json:"code"`
				Archives []struct {
					ArchiveCode         string `json:"archive_code"`
					RelationType        string `json:"relation_type"`
					Primary             bool   `json:"primary"`
					PreferredForDisplay bool   `json:"preferred_for_display"`
					ExternalCode        string `json:"external_code"`
				} `json:"archives"`
				ReligiousContext []struct {
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
		OldID           int
		ArchiveCodes    []string
		ExternalCodes   []string
		PrimaryCount    int
		PreferredCount  int
		ReligiousCodes  []string
		HasCopyRelation bool
	}{}
	for _, row := range exported.Items.Llibres {
		archiveCodes := make([]string, 0, len(row.Archives))
		externalCodes := make([]string, 0, len(row.Archives))
		primaryCount := 0
		preferredCount := 0
		hasCopyRelation := false
		for _, archive := range row.Archives {
			archiveCodes = append(archiveCodes, archive.ArchiveCode)
			externalCodes = append(externalCodes, archive.ExternalCode)
			if archive.Primary {
				primaryCount++
			}
			if archive.PreferredForDisplay {
				preferredCount++
			}
			if archive.RelationType == "copia_digital" && !archive.PreferredForDisplay {
				hasCopyRelation = true
			}
		}
		religiousCodes := make([]string, 0, len(row.ReligiousContext))
		for _, rel := range row.ReligiousContext {
			religiousCodes = append(religiousCodes, rel.EntityCode)
		}
		records[row.Code] = struct {
			OldID           int
			ArchiveCodes    []string
			ExternalCodes   []string
			PrimaryCount    int
			PreferredCount  int
			ReligiousCodes  []string
			HasCopyRelation bool
		}{
			OldID:           row.Legacy.OldID,
			ArchiveCodes:    archiveCodes,
			ExternalCodes:   externalCodes,
			PrimaryCount:    primaryCount,
			PreferredCount:  preferredCount,
			ReligiousCodes:  religiousCodes,
			HasCopyRelation: hasCopyRelation,
		}
	}
	if got := records["f35_4u7_book_religios"]; got.OldID != llibreRelID || len(got.ArchiveCodes) != 2 || got.PrimaryCount != 1 || got.PreferredCount != 1 || !slicesContain(got.ArchiveCodes, "f35_4u7_arxiu_religios") || !slicesContain(got.ArchiveCodes, "f35_4u7_arxiu_civil") || !slicesContain(got.ExternalCodes, "REL-001") || !got.HasCopyRelation || !slicesContain(got.ReligiousCodes, "f35_4u7_entitat_export") {
		t.Fatalf("export v2 del llibre religios inesperat: %+v", got)
	}
	if got := records["f35_4u7_book_civil"]; got.OldID != llibreCivilID || len(got.ArchiveCodes) != 1 || got.ArchiveCodes[0] != "f35_4u7_arxiu_civil" || len(got.ReligiousCodes) != 0 {
		t.Fatalf("export v2 del llibre civil inesperat: %+v", got)
	}
}

func TestF354U7R1PublishedArchiveRelationChangesGoThroughModeration(t *testing.T) {
	app, database, admin, session := setupF354U7BooksAdmin(t, "test_f35_4u7r1_moderation.sqlite3")
	_, municipiID := seedF354U7BookTerritory(t, database, "Moderation")
	arxiuAID := createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u7_a", "Arxiu A F35-4U7", 0)
	arxiuBID := createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u7_b", "Arxiu B F35-4U7", 0)
	llibreID := createF354U7Book(t, database, admin.ID, municipiID, 0, "f35_4u7_book_mod", "Llibre F35-4U7 Moderacio")
	publishF354U7Book(t, database, llibreID)

	saveLink := requireF354U7LinkSaver(t, database)
	if err := saveLink(&db.ArxiuLlibreLink{
		ArxiuID:               arxiuAID,
		LlibreID:              llibreID,
		Signatura:             "SIG-A",
		URLOverride:           "https://example.test/a",
		Principal:             true,
		PreferitVisualitzacio: true,
		Estat:                 "actiu",
		ModeracioEstat:        "publicat",
	}); err != nil {
		t.Fatalf("save link A: %v", err)
	}
	if err := saveLink(&db.ArxiuLlibreLink{
		ArxiuID:               arxiuBID,
		LlibreID:              llibreID,
		Signatura:             "SIG-B",
		URLOverride:           "https://example.test/b",
		TipusRelacio:          "copia_digital",
		Principal:             false,
		PreferitVisualitzacio: false,
		Estat:                 "actiu",
		ModeracioEstat:        "publicat",
	}); err != nil {
		t.Fatalf("save link B: %v", err)
	}

	form := url.Values{}
	form.Set("signatura", "SIG-B-UPDATED")
	form.Set("url_override", "https://example.test/b-updated")
	req := httptest.NewRequest(http.MethodPost, "/documentals/llibres/"+strconv.Itoa(llibreID)+"/arxius/"+strconv.Itoa(arxiuBID)+"/update", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminUpdateLlibreArxiu(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("AdminUpdateLlibreArxiu status=%d body=%s", rr.Code, rr.Body.String())
	}

	rels, err := database.ListLlibreArxius(llibreID)
	if err != nil {
		t.Fatalf("ListLlibreArxius pre-approve: %v", err)
	}
	for _, rel := range rels {
		if rel.ArxiuID == arxiuBID && strings.TrimSpace(rel.Signatura.String) != "SIG-B" {
			t.Fatalf("la relacio publicada no s'ha d'actualitzar abans d'aprovar, got=%q", rel.Signatura.String)
		}
	}
	changes, err := database.ListWikiChanges("llibre", llibreID)
	if err != nil || len(changes) == 0 {
		t.Fatalf("ListWikiChanges llibre: err=%v len=%d", err, len(changes))
	}
	changeID := changes[len(changes)-1].ID
	f353YPostModeracio(t, app.AdminModeracioAprovar, session, changeID, "llibre_canvi", "aprovar", "")

	rels, err = database.ListLlibreArxius(llibreID)
	if err != nil {
		t.Fatalf("ListLlibreArxius post-approve: %v", err)
	}
	foundUpdated := false
	for _, rel := range rels {
		if rel.ArxiuID == arxiuBID && strings.TrimSpace(rel.Signatura.String) == "SIG-B-UPDATED" && strings.TrimSpace(rel.URLOverride.String) == "https://example.test/b-updated" {
			foundUpdated = true
		}
	}
	if !foundUpdated {
		t.Fatalf("la relacio aprovada no s'ha aplicat: %+v", rels)
	}
}

func TestF354U7R1BookFormArchiveSearchAvoidsMassiveSelect(t *testing.T) {
	app, database, admin, session := setupF354U7BooksAdmin(t, "test_f35_4u7r1_form.sqlite3")
	_, municipiID := seedF354U7BookTerritory(t, database, "Form")
	createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u7_hidden", "Arxiu Invisible F35-4U7", 0)
	createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u7_match", "Arxiu Cercable F35-4U7", 0)

	req := httptest.NewRequest(http.MethodGet, "/documentals/llibres/new", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminNewLlibre(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminNewLlibre status=%d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if strings.Contains(body, "Arxiu Invisible F35-4U7") || strings.Contains(body, "Arxiu Cercable F35-4U7") {
		t.Fatalf("el formulari nou no ha de carregar un selector massiu d'arxius sense cerca")
	}
	if strings.Contains(body, `name="archive_q"`) || strings.Contains(body, `name="search_archive"`) {
		t.Fatalf("el formulari nou no ha de mantenir la cerca roundtrip antiga, body=%s", body)
	}
	if strings.Contains(body, `<label for="arquevisbat_id">`) {
		t.Fatalf("el formulari nou no ha de mostrar el selector legacy d'entitat, body=%s", body)
	}
	if strings.Contains(body, `name="arquevisbat_id"`) {
		t.Fatalf("el formulari nou no ha d'enviar arquevisbat_id des del client, body=%s", body)
	}
	if !strings.Contains(body, "Base documental del llibre") ||
		!strings.Contains(body, `/api/documentals/arxius/suggest`) ||
		!strings.Contains(body, `/api/documentals/llibres/entitats-religioses/suggest`) ||
		!strings.Contains(body, `/api/documentals/llibres/municipis/suggest`) {
		t.Fatalf("el formulari nou ha de reforcar la base documental principal, body=%s", body)
	}
	if !strings.Contains(body, `/static/js/admin-llibres-form.js`) || !strings.Contains(body, `/static/js/arxiu-form-suggest.js`) {
		t.Fatalf("el formulari nou ha d'usar JS extern per suggest i coordinacio, body=%s", body)
	}
}

func TestF354U11BBookReligiousEntitySuggestUsesArchiveRelations(t *testing.T) {
	app, database, admin, session := setupF354U7BooksAdmin(t, "test_f35_4u11b_rel_suggest.sqlite3")
	_, municipiID := seedF354U7BookTerritory(t, database, "RelSuggest")
	arxiuID := createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u11b_archive_rel", "Arxiu F35-4U11B RelSuggest", 0)
	entitatID := createF354U7ReligiousEntity(t, database, "f35_4u11b_entitat_rel", "Parroquia F35-4U11B RelSuggest")
	if _, err := database.SaveArxiuEntitatReligiosa(&db.ArxiuEntitatReligiosa{
		ArxiuID:            arxiuID,
		EntitatReligiosaID: entitatID,
		TipusRelacio:       "custodia_documentacio",
		Estat:              "actiu",
		ModeracioEstat:     "publicat",
	}); err != nil {
		t.Fatalf("SaveArxiuEntitatReligiosa: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/documentals/llibres/entitats-religioses/suggest?arxiu_id="+strconv.Itoa(arxiuID), nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.SearchBookReligiousEntitiesSuggestJSON(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("SearchBookReligiousEntitiesSuggestJSON status=%d body=%s", rr.Code, rr.Body.String())
	}
	var payload struct {
		Items []struct {
			ID      int    `json:"id"`
			Nom     string `json:"nom"`
			Related bool   `json:"related"`
		} `json:"items"`
		ArchiveRelatedSingle bool `json:"archive_related_single"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json invalid suggest entitats: %v", err)
	}
	if !payload.ArchiveRelatedSingle || len(payload.Items) != 1 || payload.Items[0].ID != entitatID || !payload.Items[0].Related {
		t.Fatalf("payload suggest entitats inesperat: %+v", payload)
	}
}

func TestF354U11BBookReligiousEntitySuggestAcceptsPost(t *testing.T) {
	app, database, admin, session := setupF354U7BooksAdmin(t, "test_f35_4u11b_rel_suggest_post.sqlite3")
	_, municipiID := seedF354U7BookTerritory(t, database, "RelSuggestPost")
	arxiuID := createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u11b_archive_rel_post", "Arxiu F35-4U11B RelSuggest POST", 0)
	entitatID := createF354U7ReligiousEntity(t, database, "f35_4u11b_entitat_rel_post", "Parroquia F35-4U11B RelSuggest POST")
	if _, err := database.SaveArxiuEntitatReligiosa(&db.ArxiuEntitatReligiosa{
		ArxiuID:            arxiuID,
		EntitatReligiosaID: entitatID,
		TipusRelacio:       "custodia_documentacio",
		Estat:              "actiu",
		ModeracioEstat:     "publicat",
	}); err != nil {
		t.Fatalf("SaveArxiuEntitatReligiosa POST: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/documentals/llibres/entitats-religioses/suggest", bytes.NewBufferString("arxiu_id="+strconv.Itoa(arxiuID)+"&limit=25"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.SearchBookReligiousEntitiesSuggestJSON(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("SearchBookReligiousEntitiesSuggestJSON POST status=%d body=%s", rr.Code, rr.Body.String())
	}
	var payload struct {
		Items []struct {
			ID      int  `json:"id"`
			Related bool `json:"related"`
		} `json:"items"`
		ArchiveRelatedSingle bool `json:"archive_related_single"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json invalid suggest entitats POST: %v", err)
	}
	if !payload.ArchiveRelatedSingle || len(payload.Items) != 1 || payload.Items[0].ID != entitatID || !payload.Items[0].Related {
		t.Fatalf("payload suggest entitats POST inesperat: %+v", payload)
	}
}

func TestF354U11BBookMunicipiSuggestFiltersByReligiousEntityScope(t *testing.T) {
	app, database, _, session := setupF354U7BooksAdmin(t, "test_f35_4u11b_mun_suggest.sqlite3")
	paisID, municipiAID := seedF354U7BookTerritory(t, database, "MunScopeA")
	municipiBID := createF354U7MunicipiForPais(t, database, paisID, "MunScopeB")
	publishF354U7Municipi(t, database, municipiAID)
	publishF354U7Municipi(t, database, municipiBID)
	parentID := createF354U7ReligiousEntity(t, database, "f35_4u11b_parent", "Diocesi F35-4U11B Parent")
	childID := createF354U7ReligiousEntity(t, database, "f35_4u11b_child", "Parroquia F35-4U11B Child")
	if _, err := database.SaveEntitatReligiosaRelacio(&db.EntitatReligiosaRelacio{
		EntitatOrigenID: parentID,
		EntitatDestiID:  childID,
		TipusRelacio:    "jerarquia",
		ModeracioEstat:  "publicat",
	}); err != nil {
		t.Fatalf("SaveEntitatReligiosaRelacio: %v", err)
	}
	if _, err := database.SaveMunicipiEntitatReligiosa(&db.MunicipiEntitatReligiosa{
		MunicipiID:         municipiAID,
		EntitatReligiosaID: childID,
		TipusRelacio:       "parroquia",
		ModeracioEstat:     "publicat",
	}); err != nil {
		t.Fatalf("SaveMunicipiEntitatReligiosa A: %v", err)
	}
	otherID := createF354U7ReligiousEntity(t, database, "f35_4u11b_other", "Parroquia F35-4U11B Other")
	if _, err := database.SaveMunicipiEntitatReligiosa(&db.MunicipiEntitatReligiosa{
		MunicipiID:         municipiBID,
		EntitatReligiosaID: otherID,
		TipusRelacio:       "parroquia",
		ModeracioEstat:     "publicat",
	}); err != nil {
		t.Fatalf("SaveMunicipiEntitatReligiosa B: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/documentals/llibres/municipis/suggest?entitat_religiosa_id="+strconv.Itoa(parentID)+"&q=Municipi", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.SearchBookMunicipisSuggestJSON(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("SearchBookMunicipisSuggestJSON status=%d body=%s", rr.Code, rr.Body.String())
	}
	var payload struct {
		Items []struct {
			ID int `json:"id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json invalid suggest municipis: %v", err)
	}
	if len(payload.Items) != 1 || payload.Items[0].ID != municipiAID {
		t.Fatalf("scope municipi inesperat: %+v", payload)
	}
}

func TestF354U11BBookMunicipiSuggestAcceptsPost(t *testing.T) {
	app, database, _, session := setupF354U7BooksAdmin(t, "test_f35_4u11b_mun_suggest_post.sqlite3")
	paisID, municipiAID := seedF354U7BookTerritory(t, database, "MunScopePostA")
	municipiBID := createF354U7MunicipiForPais(t, database, paisID, "MunScopePostB")
	publishF354U7Municipi(t, database, municipiAID)
	publishF354U7Municipi(t, database, municipiBID)
	parentID := createF354U7ReligiousEntity(t, database, "f35_4u11b_parent_post", "Diocesi F35-4U11B Parent POST")
	childID := createF354U7ReligiousEntity(t, database, "f35_4u11b_child_post", "Parroquia F35-4U11B Child POST")
	if _, err := database.SaveEntitatReligiosaRelacio(&db.EntitatReligiosaRelacio{
		EntitatOrigenID: parentID,
		EntitatDestiID:  childID,
		TipusRelacio:    "jerarquia",
		ModeracioEstat:  "publicat",
	}); err != nil {
		t.Fatalf("SaveEntitatReligiosaRelacio POST: %v", err)
	}
	if _, err := database.SaveMunicipiEntitatReligiosa(&db.MunicipiEntitatReligiosa{
		MunicipiID:         municipiAID,
		EntitatReligiosaID: childID,
		TipusRelacio:       "parroquia",
		ModeracioEstat:     "publicat",
	}); err != nil {
		t.Fatalf("SaveMunicipiEntitatReligiosa POST A: %v", err)
	}
	otherID := createF354U7ReligiousEntity(t, database, "f35_4u11b_other_post", "Parroquia F35-4U11B Other POST")
	if _, err := database.SaveMunicipiEntitatReligiosa(&db.MunicipiEntitatReligiosa{
		MunicipiID:         municipiBID,
		EntitatReligiosaID: otherID,
		TipusRelacio:       "parroquia",
		ModeracioEstat:     "publicat",
	}); err != nil {
		t.Fatalf("SaveMunicipiEntitatReligiosa POST B: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/documentals/llibres/municipis/suggest", bytes.NewBufferString("entitat_religiosa_id="+strconv.Itoa(parentID)+"&q=Municipi"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.SearchBookMunicipisSuggestJSON(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("SearchBookMunicipisSuggestJSON POST status=%d body=%s", rr.Code, rr.Body.String())
	}
	var payload struct {
		Items []struct {
			ID int `json:"id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json invalid suggest municipis POST: %v", err)
	}
	if len(payload.Items) != 1 || payload.Items[0].ID != municipiAID {
		t.Fatalf("scope municipi POST inesperat: %+v", payload)
	}
}

func TestF354U11BBookSaveRejectsMunicipalityOutsideReligiousScope(t *testing.T) {
	app, database, admin, session := setupF354U7BooksAdmin(t, "test_f35_4u11b_save_scope.sqlite3")
	paisID, municipiAID := seedF354U7BookTerritory(t, database, "SaveScopeA")
	municipiBID := createF354U7MunicipiForPais(t, database, paisID, "SaveScopeB")
	publishF354U7Municipi(t, database, municipiAID)
	publishF354U7Municipi(t, database, municipiBID)
	arxiuID := createF354U7Archive(t, database, admin.ID, municipiAID, "f35_4u11b_scope_archive", "Arxiu Scope F35-4U11B", 0)
	entitatID := createF354U7ReligiousEntity(t, database, "f35_4u11b_scope_ent", "Parroquia Scope F35-4U11B")
	if _, err := database.SaveMunicipiEntitatReligiosa(&db.MunicipiEntitatReligiosa{
		MunicipiID:         municipiAID,
		EntitatReligiosaID: entitatID,
		TipusRelacio:       "parroquia",
		ModeracioEstat:     "publicat",
	}); err != nil {
		t.Fatalf("SaveMunicipiEntitatReligiosa: %v", err)
	}

	form := url.Values{}
	form.Set("arxiu_id", strconv.Itoa(arxiuID))
	form.Set("entitat_religiosa_id", strconv.Itoa(entitatID))
	form.Set("municipi_id", strconv.Itoa(municipiBID))
	form.Set("titol", "Llibre fora d'abast F35-4U11B")
	form.Set("tipus_llibre", "baptismes")
	form.Set("codi", "f35_4u11b_scope_book")
	req := httptest.NewRequest(http.MethodPost, "/documentals/llibres/save", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminSaveLlibre(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminSaveLlibre fora abast ha de rerenderitzar amb error, status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(html.UnescapeString(rr.Body.String()), "abast de l'entitat religiosa") {
		t.Fatalf("manca missatge d'abast religios, body=%s", rr.Body.String())
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM llibres WHERE codi = ?", "f35_4u11b_scope_book"); got != 0 {
		t.Fatalf("no s'hauria d'haver creat cap llibre fora d'abast, got=%d", got)
	}
}

func TestF354U11AR1BookUpdateIgnoresTamperedLegacyEntityID(t *testing.T) {
	app, database, admin, session := setupF354U7BooksAdmin(t, "test_f35_4u11ar1_book_tamper.sqlite3")
	paisID, municipiID := seedF354U7BookTerritory(t, database, "Tamper")
	originalLegacyID := createF354U7LegacyEntity(t, database, paisID, "Bisbat Legacy Original F35-4U11A-R1")
	tamperedLegacyID := createF354U7LegacyEntity(t, database, paisID, "Bisbat Legacy Tampered F35-4U11A-R1")
	arxiuID := createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u11ar1_archive", "Arxiu Tamper F35-4U11A-R1", 0)
	llibreID := createF354U7Book(t, database, admin.ID, municipiID, originalLegacyID, "f35_4u11ar1_book", "Llibre Tamper F35-4U11A-R1")

	form := url.Values{}
	form.Set("id", strconv.Itoa(llibreID))
	form.Set("arxiu_id", strconv.Itoa(arxiuID))
	form.Set("municipi_id", strconv.Itoa(municipiID))
	form.Set("arquevisbat_id", strconv.Itoa(tamperedLegacyID))
	form.Set("titol", "Llibre Tamper F35-4U11A-R1")
	form.Set("tipus_llibre", "baptismes")
	form.Set("codi", "f35_4u11ar1_book")
	req := httptest.NewRequest(http.MethodPost, "/documentals/llibres/save", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminSaveLlibre(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("AdminSaveLlibre tamper status=%d body=%s", rr.Code, rr.Body.String())
	}

	llibre, err := database.GetLlibre(llibreID)
	if err != nil || llibre == nil {
		t.Fatalf("GetLlibre tamper: err=%v llibre=%v", err, llibre)
	}
	if got := llibre.ArquebisbatID; got != originalLegacyID {
		t.Fatalf("el backend ha d'ignorar el tampering d'arquevisbat_id, got=%d want=%d", got, originalLegacyID)
	}
}

func TestF354U11AR2BookUpdateClearsLegacyArquebisbatForCivilType(t *testing.T) {
	app, database, admin, session := setupF354U7BooksAdmin(t, "test_f35_4u11ar2_book_civil_legacy.sqlite3")
	paisID, municipiID := seedF354U7BookTerritory(t, database, "CivilLegacy")
	legacyID := createF354U7LegacyEntity(t, database, paisID, "Bisbat Legacy Civil F35-4U11A-R2")
	tamperedLegacyID := createF354U7LegacyEntity(t, database, paisID, "Bisbat Legacy Civil Tampered F35-4U11A-R2")
	arxiuID := createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u11ar2_archive", "Arxiu Civil F35-4U11A-R2", 0)
	llibreID := createF354U7Book(t, database, admin.ID, municipiID, legacyID, "f35_4u11ar2_book", "Llibre Civil F35-4U11A-R2")

	form := url.Values{}
	form.Set("id", strconv.Itoa(llibreID))
	form.Set("arxiu_id", strconv.Itoa(arxiuID))
	form.Set("municipi_id", strconv.Itoa(municipiID))
	form.Set("arquevisbat_id", strconv.Itoa(tamperedLegacyID))
	form.Set("titol", "Llibre Civil F35-4U11A-R2")
	form.Set("tipus_llibre", "padrons")
	form.Set("codi", "f35_4u11ar2_book")
	req := httptest.NewRequest(http.MethodPost, "/documentals/llibres/save", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminSaveLlibre(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("AdminSaveLlibre civil status=%d body=%s", rr.Code, rr.Body.String())
	}

	llibre, err := database.GetLlibre(llibreID)
	if err != nil || llibre == nil {
		t.Fatalf("GetLlibre civil: err=%v llibre=%v", err, llibre)
	}
	if got := llibre.ArquebisbatID; got != 0 {
		t.Fatalf("un llibre civil ha de netejar arquevisbat_id legacy, got=%d", got)
	}
	if got := strings.TrimSpace(llibre.TipusLlibre); got != "padrons" {
		t.Fatalf("tipus_llibre inesperat després del canvi civil: %q", got)
	}
}

func TestF354U11AR1BooksListTemplateKeepsSequentialColumnsAndAccessibleFilters(t *testing.T) {
	root := findProjectRoot(t)
	src := mustReadProjectFileF354U7(t, root, "templates", "admin-llibres-list.html")
	for _, token := range []string{
		`id="llibres-filter-titol"`,
		`id="llibres-filter-municipi"`,
		`id="llibres-filter-crono"`,
		`id="llibres-filter-pagines"`,
		`id="llibres-filter-status"`,
		`class="sr-only" for="llibres-filter-titol"`,
		`scope="col" data-col="0" data-key="titol"`,
		`scope="col" data-col="5" data-key="status"`,
	} {
		if !strings.Contains(src, token) {
			t.Fatalf("falta contracte d'accessibilitat/reindexacio a llibres-list: %q", token)
		}
	}
	if strings.Contains(src, `data-key="entitat"`) {
		t.Fatalf("llibres-list no ha de conservar la columna legacy entitat")
	}
}

func TestF354U11BR1BookSaveRejectsUnpublishedArchive(t *testing.T) {
	app, database, admin, session := setupF354U7BooksAdmin(t, "test_f35_4u11br1_archive_pending.sqlite3")
	_, municipiID := seedF354U7BookTerritory(t, database, "ArchivePending")
	publishF354U7Municipi(t, database, municipiID)
	arxiuID := createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u11br1_pending_archive", "Arxiu Pendent F35-4U11B-R1", 0)
	arxiu, err := database.GetArxiu(arxiuID)
	if err != nil || arxiu == nil {
		t.Fatalf("GetArxiu pending: err=%v arxiu=%v", err, arxiu)
	}
	arxiu.ModeracioEstat = "pendent"
	if err := database.UpdateArxiu(arxiu); err != nil {
		t.Fatalf("UpdateArxiu pending: %v", err)
	}

	form := url.Values{}
	form.Set("arxiu_id", strconv.Itoa(arxiuID))
	form.Set("municipi_id", strconv.Itoa(municipiID))
	form.Set("titol", "Llibre arxiu pendent F35-4U11B-R1")
	form.Set("tipus_llibre", "baptismes")
	form.Set("codi", "f35_4u11br1_pending_archive_book")
	req := httptest.NewRequest(http.MethodPost, "/documentals/llibres/save", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminSaveLlibre(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminSaveLlibre amb arxiu pendent ha de rerenderitzar, status=%d body=%s", rr.Code, rr.Body.String())
	}
	body := strings.ToLower(html.UnescapeString(rr.Body.String()))
	if !strings.Contains(body, "arxiu seleccionat") || !strings.Contains(body, "publicat") {
		t.Fatalf("manca validacio d'arxiu publicat, body=%s", rr.Body.String())
	}
}

func TestF354U11BR1BookFormJSUsesLocalizedHelpersAndNoHardcodedCatalan(t *testing.T) {
	root := findProjectRoot(t)
	src := mustReadProjectFileF354U7(t, root, "static", "js", "admin-llibres-form.js")
	for _, token := range []string{
		"dataset.helperDefault",
		"dataset.helperAutoSelected",
		"dataset.emptyScopeMessage",
		"AbortController",
		"url.includes(\"?\")",
	} {
		if !strings.Contains(src, token) {
			t.Fatalf("falta contracte JS F35-4U11B-R1: %q", token)
		}
	}
	if strings.Contains(src, "Preseleccionada des de l'arxiu") {
		t.Fatalf("el JS no ha de contenir text dur en català, src=%s", src)
	}
	if strings.Contains(src, "var ") {
		t.Fatalf("el JS no ha d'usar var després del refactor, src=%s", src)
	}
}

func TestF354U7R1PendingBookAddArchiveKeepsSinglePrimaryAndPreferred(t *testing.T) {
	app, database, admin, session := setupF354U7BooksAdmin(t, "test_f35_4u7r1_pending_add.sqlite3")
	_, municipiID := seedF354U7BookTerritory(t, database, "PendingAdd")
	arxiuAID := createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u7_pa", "Arxiu A Pending", 0)
	arxiuBID := createF354U7Archive(t, database, admin.ID, municipiID, "f35_4u7_pb", "Arxiu B Pending", 0)
	llibreID := createF354U7Book(t, database, admin.ID, municipiID, 0, "f35_4u7_book_pending", "Llibre F35-4U7 Pending")

	saveLink := requireF354U7LinkSaver(t, database)
	if err := saveLink(&db.ArxiuLlibreLink{
		ArxiuID:               arxiuAID,
		LlibreID:              llibreID,
		Signatura:             "SIG-A",
		URLOverride:           "https://example.test/a",
		Principal:             true,
		PreferitVisualitzacio: true,
		Estat:                 "actiu",
		ModeracioEstat:        "publicat",
	}); err != nil {
		t.Fatalf("save link A: %v", err)
	}

	form := url.Values{}
	form.Set("arxiu_id", strconv.Itoa(arxiuBID))
	form.Set("signatura", "SIG-B")
	form.Set("url_override", "https://example.test/b")
	req := httptest.NewRequest(http.MethodPost, "/documentals/llibres/"+strconv.Itoa(llibreID)+"/arxius/add", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminAddLlibreArxiu(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("AdminAddLlibreArxiu status=%d body=%s", rr.Code, rr.Body.String())
	}

	rels, err := database.ListLlibreArxius(llibreID)
	if err != nil {
		t.Fatalf("ListLlibreArxius: %v", err)
	}
	if len(rels) != 2 {
		t.Fatalf("s'esperaven 2 relacions, got %d", len(rels))
	}
	primaryCount := 0
	preferredCount := 0
	seen := map[int]bool{}
	for _, rel := range rels {
		if seen[rel.ArxiuID] {
			t.Fatalf("duplicat exacte d'arxiu detectat a les relacions: %+v", rels)
		}
		seen[rel.ArxiuID] = true
		if rel.Principal {
			primaryCount++
		}
		if rel.PreferitVisualitzacio {
			preferredCount++
		}
		if rel.ArxiuID == arxiuAID && (!rel.Principal || !rel.PreferitVisualitzacio) {
			t.Fatalf("l'arxiu A ha de continuar principal/preferit, got %+v", rel)
		}
		if rel.ArxiuID == arxiuBID && (rel.Principal || rel.PreferitVisualitzacio) {
			t.Fatalf("l'arxiu B no ha d'entrar principal/preferit, got %+v", rel)
		}
	}
	if primaryCount != 1 || preferredCount != 1 {
		t.Fatalf("principal/preferit han de ser unics, got principal=%d preferit=%d rels=%+v", primaryCount, preferredCount, rels)
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

func createF354U7MunicipiForPais(t *testing.T, database db.DB, paisID int, suffix string) int {
	t.Helper()
	nivellID, err := database.CreateNivell(&db.NivellAdministratiu{
		PaisID:         paisID,
		Nivel:          1,
		NomNivell:      "Pais F35-4U7 Extra " + suffix,
		TipusNivell:    "pais",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	})
	if err != nil {
		t.Fatalf("CreateNivell extra: %v", err)
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
		t.Fatalf("CreateMunicipi extra: %v", err)
	}
	return municipiID
}

func publishF354U7Municipi(t *testing.T, database db.DB, municipiID int) {
	t.Helper()
	mun, err := database.GetMunicipi(municipiID)
	if err != nil || mun == nil {
		t.Fatalf("GetMunicipi publish: err=%v municipi=%v", err, mun)
	}
	mun.ModeracioEstat = "publicat"
	if err := database.UpdateMunicipi(mun); err != nil {
		t.Fatalf("UpdateMunicipi publish: %v", err)
	}
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

func publishF354U7Book(t *testing.T, database db.DB, llibreID int) {
	t.Helper()
	llibre, err := database.GetLlibre(llibreID)
	if err != nil || llibre == nil {
		t.Fatalf("GetLlibre publish: err=%v llibre=%v", err, llibre)
	}
	llibre.ModeracioEstat = "publicat"
	if err := database.UpdateLlibre(llibre); err != nil {
		t.Fatalf("UpdateLlibre publish: %v", err)
	}
}

func requireF354U7LinkSaver(t *testing.T, database db.DB) func(*db.ArxiuLlibreLink) error {
	t.Helper()
	type saver interface {
		SaveArxiuLlibreLink(link *db.ArxiuLlibreLink) error
	}
	impl, ok := database.(saver)
	if !ok {
		t.Fatalf("DB no implementa SaveArxiuLlibreLink")
	}
	return impl.SaveArxiuLlibreLink
}

func slicesContain(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
