package integration

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"mime/multipart"
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

func mustReadProjectFileF354U6(t *testing.T, root string, parts ...string) string {
	t.Helper()
	path := filepath.Clean(filepath.Join(append([]string{root}, parts...)...))
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile %s: %v", path, err)
	}
	return string(body)
}

func findSingleArxiuF354U6(t *testing.T, database db.DB, filter db.ArxiuFilter) db.ArxiuWithCount {
	t.Helper()
	rows, err := database.ListArxius(filter)
	if err != nil || len(rows) != 1 {
		t.Fatalf("ListArxius %+v: err=%v rows=%d", filter, err, len(rows))
	}
	return rows[0]
}

func resolveSingleArxiuByCodeF354U6(t *testing.T, database db.DB, code string) *db.Arxiu {
	t.Helper()
	rows, err := database.ResolveArxiusByCodes([]string{code})
	if err != nil || len(rows) != 1 {
		t.Fatalf("ResolveArxiusByCodes %q: err=%v rows=%d", code, err, len(rows))
	}
	arxiu, err := database.GetArxiu(rows[0].ID)
	if err != nil || arxiu == nil {
		t.Fatalf("GetArxiu %d by code %q: err=%v arxiu=%v", rows[0].ID, code, err, arxiu)
	}
	return arxiu
}

func latestAdminJobF354U6(t *testing.T, database db.DB, kind string) *db.AdminJob {
	t.Helper()
	rows, err := database.ListAdminJobs(db.AdminJobFilter{Kind: kind, Limit: 1})
	if err != nil || len(rows) != 1 {
		t.Fatalf("ListAdminJobs kind=%s: err=%v rows=%d", kind, err, len(rows))
	}
	job, err := database.GetAdminJob(rows[0].ID)
	if err != nil || job == nil {
		t.Fatalf("GetAdminJob %d: err=%v job=%v", rows[0].ID, err, job)
	}
	return job
}

func TestF354U6ArxiusV2ImportCreatesPendingArchiveWithoutReligiousRelation(t *testing.T) {
	app, database, admin, session := setupF354U6ArxiusAdmin(t, "test_f35_4u6_import_no_relation.sqlite3")
	_, municipiID, _ := seedF354U6ArxiuTerritory(t, database)

	payload := []byte(`{
  "schema": "cercagenealogica.arxius.v2",
  "exported_at": "2026-06-16T12:00:00Z",
  "source": { "app": "CercaGenealogica", "module": "arxius" },
  "items": {
    "arxius": [
      {
        "code": "f35_4u6_arxiu_sense_relacio",
        "name": "Arxiu F35-4U6 Sense Relacio",
        "type": "privat",
        "access": "online",
        "municipality": {
          "name": "Municipi F35-4U6",
          "country_iso2": "ES"
        },
        "web": "https://example.test/f35-4u6/no-rel",
        "notes": "Import v2 sense relacio"
      }
    ]
  }
}`)

	rr := postF354U6ArxiusImport(t, app, session, payload)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("import v2 sense relacio status=%d body=%s", rr.Code, rr.Body.String())
	}
	if location := rr.Header().Get("Location"); !strings.Contains(location, "arxius_created=1") {
		t.Fatalf("redirect import v2 sense relacio sense resum esperat: %s", location)
	}

	arxiuRow := findSingleArxiuF354U6(t, database, db.ArxiuFilter{Text: "Arxiu F35-4U6 Sense Relacio", Limit: 1})
	arxiu, err := database.GetArxiu(arxiuRow.ID)
	if err != nil || arxiu == nil {
		t.Fatalf("GetArxiu import v2 sense relacio: err=%v arxiu=%v", err, arxiu)
	}
	if got := strings.TrimSpace(arxiu.Codi); got != "f35_4u6_arxiu_sense_relacio" {
		t.Fatalf("codi estable inesperat: %q", got)
	}
	if !arxiu.MunicipiID.Valid || int(arxiu.MunicipiID.Int64) != municipiID {
		t.Fatalf("municipi_id inesperat: got=%d want=%d", arxiu.MunicipiID.Int64, municipiID)
	}
	if got := strings.TrimSpace(arxiu.ModeracioEstat); got != "pendent" {
		t.Fatalf("arxiu importat ha d'entrar pendent, got %q", got)
	}
	if arxiu.ModeratedBy.Valid || arxiu.ModeratedAt.Valid {
		t.Fatalf("arxiu importat no ha de tenir moderated_by/moderated_at")
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM arxiu_entitat_religiosa"); got != 0 {
		t.Fatalf("no s'hauria de crear cap relacio confessional, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM admin_import_runs WHERE import_type = ? AND status = 'ok'", "arxius"); got != 1 {
		t.Fatalf("s'esperava 1 admin_import_runs arxius ok, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM usuaris_activitat WHERE usuari_id = ? AND estat = 'pendent' AND detalls = 'import' AND objecte_tipus = 'arxiu'", admin.ID); got != 1 {
		t.Fatalf("s'esperava 1 activitat pendent d'import d'arxiu, got %d", got)
	}
}

func TestF354U6ArxiusV2ImportCreatesPendingArchiveAndConfessionalRelation(t *testing.T) {
	app, database, admin, session := setupF354U6ArxiusAdmin(t, "test_f35_4u6_import_with_relation.sqlite3")
	seedF354U6ArxiuTerritory(t, database)
	if _, err := database.SaveEntitatReligiosa(&db.EntitatReligiosa{
		Codi:                   "f35_4u6_entitat_publicada",
		Nom:                    "Parroquia F35-4U6 Publicada",
		ReligioConfessioCodi:   "catolicisme_ritu_llati",
		NivellConfessionalCodi: "parroquia",
		Estat:                  "actiu",
		ModeracioEstat:         "publicat",
	}); err != nil {
		t.Fatalf("SaveEntitatReligiosa: %v", err)
	}

	payload := []byte(`{
  "schema": "cercagenealogica.arxius.v2",
  "exported_at": "2026-06-16T12:30:00Z",
  "source": { "app": "CercaGenealogica", "module": "arxius" },
  "items": {
    "arxius": [
      {
        "code": "f35_4u6_arxiu_amb_relacio",
        "name": "Arxiu F35-4U6 Amb Relacio",
        "type": "arxiu_comarcal",
        "access": "presencial",
        "municipality": {
          "name": "Municipi F35-4U6",
          "country_iso2": "ES"
        },
        "religious_entity_refs": [
          {
            "entity_code": "f35_4u6_entitat_publicada",
            "religion_code": "catolicisme_ritu_llati",
            "level_code": "parroquia",
            "relation_type": "custodia_documentacio",
            "state": "actiu",
            "moderation_status": "publicat"
          }
        ]
      }
    ]
  }
}`)

	rr := postF354U6ArxiusImport(t, app, session, payload)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("import v2 amb relacio status=%d body=%s", rr.Code, rr.Body.String())
	}

	arxiu = resolveSingleArxiuByCodeF354U6(t, database, "f35_4u6_arxiu_amb_relacio")
	arxiuID := arxiu.ID
	if got := strings.TrimSpace(arxiu.ModeracioEstat); got != "pendent" {
		t.Fatalf("arxiu v2 amb relacio ha d'entrar pendent, got %q", got)
	}
	if arxiu.ModeratedBy.Valid || arxiu.ModeratedAt.Valid {
		t.Fatalf("arxiu v2 amb relacio no ha de quedar moderat")
	}

	rels, err := database.ListArxiuEntitatsReligioses(arxiuID, 0, "")
	if err != nil || len(rels) != 1 {
		t.Fatalf("ListArxiuEntitatsReligioses arxiu=%d: err=%v rows=%d", arxiuID, err, len(rels))
	}
	rel, err := database.GetArxiuEntitatReligiosa(rels[0].ID)
	if err != nil || rel == nil {
		t.Fatalf("GetArxiuEntitatReligiosa %d: err=%v rel=%v", rels[0].ID, err, rel)
	}
	if got := strings.TrimSpace(rel.ModeracioEstat); got != "pendent" {
		t.Fatalf("la relacio arxiu-entitat importada ha d'entrar pendent, got %q", got)
	}
	if rel.ModeratedBy.Valid || rel.ModeratedAt.Valid {
		t.Fatalf("la relacio arxiu-entitat importada no ha de quedar moderada")
	}
	if got := strings.TrimSpace(rel.TipusRelacio); got != "custodia_documentacio" {
		t.Fatalf("tipus_relacio inesperat: %q", got)
	}

	job := latestAdminJobF354U6(t, database, "admin_import")
	jobID := job.ID
	if got := strings.TrimSpace(job.Status); got != "done" {
		t.Fatalf("l'admin job d'import d'arxius ha d'acabar done, got %q", got)
	}
	for _, token := range []string{`"import_type":"arxius"`, `"import_format":"v2"`, `"archives_requested":1`, `"relations_requested":1`} {
		if !strings.Contains(job.PayloadJSON, token) {
			t.Fatalf("payload admin job sense token %q: %s", token, job.PayloadJSON)
		}
	}
	for _, token := range []string{`"archives_created":1`, `"relations_created":1`, `"activity_count":2`, `"admin_target_count":2`} {
		if !strings.Contains(job.ResultJSON, token) {
			t.Fatalf("result admin job sense token %q: %s", token, job.ResultJSON)
		}
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM admin_job_targets WHERE job_id = ?", jobID); got != 2 {
		t.Fatalf("s'esperaven 2 admin_job_targets, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM usuaris_activitat WHERE usuari_id = ? AND estat = 'pendent' AND detalls = 'import' AND objecte_tipus IN ('arxiu','arxiu_entitat_religiosa')", admin.ID); got != 2 {
		t.Fatalf("s'esperaven 2 activitats pendents d'import, got %d", got)
	}
}

func TestF354U6ArxiusV2ImportFailsWhenReligiousEntityCodeMissing(t *testing.T) {
	app, database, _, session := setupF354U6ArxiusAdmin(t, "test_f35_4u6_missing_entity.sqlite3")
	seedF354U6ArxiuTerritory(t, database)

	payload := []byte(`{
  "schema": "cercagenealogica.arxius.v2",
  "exported_at": "2026-06-16T13:00:00Z",
  "source": { "app": "CercaGenealogica", "module": "arxius" },
  "items": {
    "arxius": [
      {
        "code": "f35_4u6_arxiu_missing_entity",
        "name": "Arxiu F35-4U6 Missing Entity",
        "type": "estatal",
        "municipality": {
          "name": "Municipi F35-4U6",
          "country_iso2": "ES"
        },
        "religious_entity_refs": [
          {
            "entity_code": "f35_4u6_entitat_inexistent",
            "relation_type": "arxiu_institucional",
            "moderation_status": "publicat"
          }
        ]
      }
    ]
  }
}`)

	rr := postF354U6ArxiusImport(t, app, session, payload)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("import v2 amb entitat religiosa inexistent status=%d body=%s", rr.Code, rr.Body.String())
	}
	location := rr.Header().Get("Location")
	if !strings.Contains(location, "err=1") || !strings.Contains(location, "arxius_errors=1") {
		t.Fatalf("la resposta ha d'indicar error clar, location=%s", location)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM arxius WHERE codi = ?", "f35_4u6_arxiu_missing_entity"); got != 0 {
		t.Fatalf("no s'hauria de crear l'arxiu quan falta l'entitat religiosa, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM arxiu_entitat_religiosa"); got != 0 {
		t.Fatalf("no s'hauria de crear cap relacio orfe, got %d", got)
	}
}

func TestF354U6ArxiusV2ExportIncludesSchemaCodeAndOptionalReligiousRefs(t *testing.T) {
	app, database, _, session := setupF354U6ArxiusAdmin(t, "test_f35_4u6_export_v2.sqlite3")
	_, municipiID, _ := seedF354U6ArxiuTerritory(t, database)

	arxiuSenseRel := &db.Arxiu{
		Codi:           "f35_4u6_export_sense_relacio",
		Nom:            "Arxiu F35-4U6 Export Sense Relacio",
		Tipus:          "familysearch",
		MunicipiID:     sql.NullInt64{Int64: int64(municipiID), Valid: true},
		Acces:          "online",
		Web:            "https://example.test/f35-4u6/export/no-rel",
		ModeracioEstat: "pendent",
	}
	if _, err := database.CreateArxiu(arxiuSenseRel); err != nil {
		t.Fatalf("CreateArxiu sense relacio: %v", err)
	}
	arxiuAmbRel := &db.Arxiu{
		Codi:           "f35_4u6_export_amb_relacio",
		Nom:            "Arxiu F35-4U6 Export Amb Relacio",
		Tipus:          "arxiu_diocesa",
		MunicipiID:     sql.NullInt64{Int64: int64(municipiID), Valid: true},
		Acces:          "mixt",
		Web:            "https://example.test/f35-4u6/export/with-rel",
		ModeracioEstat: "pendent",
	}
	arxiuAmbRelID, err := database.CreateArxiu(arxiuAmbRel)
	if err != nil {
		t.Fatalf("CreateArxiu amb relacio: %v", err)
	}
	entitatID, err := database.SaveEntitatReligiosa(&db.EntitatReligiosa{
		Codi:                   "f35_4u6_export_entitat",
		Nom:                    "Entitat Religiosa Export F35-4U6",
		ReligioConfessioCodi:   "catolicisme_ritu_llati",
		NivellConfessionalCodi: "bisbat_diocesi",
		Estat:                  "actiu",
		ModeracioEstat:         "publicat",
	})
	if err != nil {
		t.Fatalf("SaveEntitatReligiosa export: %v", err)
	}
	if _, err := database.SaveArxiuEntitatReligiosa(&db.ArxiuEntitatReligiosa{
		ArxiuID:            arxiuAmbRelID,
		EntitatReligiosaID: entitatID,
		TipusRelacio:       "arxiu_institucional",
		Estat:              "actiu",
		ModeracioEstat:     "pendent",
	}); err != nil {
		t.Fatalf("SaveArxiuEntitatReligiosa export: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/admin/arxius/export", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminArxiusExport(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("AdminArxiusExport status=%d body=%s", rr.Code, rr.Body.String())
	}

	var exported struct {
		Schema string `json:"schema"`
		Items  struct {
			Arxius []struct {
				Code   string `json:"code"`
				Name   string `json:"name"`
				Legacy struct {
					OldID int `json:"old_id"`
				} `json:"legacy"`
				ReligiousEntityRefs []struct {
					EntityCode string `json:"entity_code"`
				} `json:"religious_entity_refs"`
			} `json:"arxius"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &exported); err != nil {
		t.Fatalf("json export v2 invalid: %v", err)
	}
	if exported.Schema != "cercagenealogica.arxius.v2" {
		t.Fatalf("schema export inesperat: %q", exported.Schema)
	}
	records := map[string]struct {
		OldID int
		Refs  int
	}{}
	for _, row := range exported.Items.Arxius {
		records[row.Code] = struct {
			OldID int
			Refs  int
		}{
			OldID: row.Legacy.OldID,
			Refs:  len(row.ReligiousEntityRefs),
		}
	}
	if got, ok := records["f35_4u6_export_sense_relacio"]; !ok || got.OldID <= 0 || got.Refs != 0 {
		t.Fatalf("export v2 ha d'incloure arxiu sense relacio amb code i sense refs: %+v", got)
	}
	if got, ok := records["f35_4u6_export_amb_relacio"]; !ok || got.OldID <= 0 || got.Refs != 1 {
		t.Fatalf("export v2 ha d'incloure arxiu amb relacio i 1 ref: %+v", got)
	}
}

func setupF354U6ArxiusAdmin(t *testing.T, dbName string) (*core.App, db.DB, *db.User, *http.Cookie) {
	t.Helper()
	app, database := newTestAppForLogin(t, dbName)
	admin := createTestUser(t, database, "f35_4u6_admin_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	policy := createPolicyWithGrant(t, database, "f35_4u6_arxius_import_"+strconv.FormatInt(time.Now().UnixNano(), 10), "documentals.arxius.import")
	addGrantToPolicy(t, database, policy, "documentals.arxius.export")
	assignPolicyToUser(t, database, admin.ID, policy)
	session := createSessionCookie(t, database, admin.ID, "sess_f35_4u6_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	return app, database, admin, session
}

func seedF354U6ArxiuTerritory(t *testing.T, database db.DB) (int, int, int) {
	t.Helper()
	pais := &db.Pais{CodiISO2: "ES", CodiISO3: "ESP", CodiPaisNum: "724"}
	paisID, err := database.CreatePais(pais)
	if err != nil {
		t.Fatalf("CreatePais: %v", err)
	}
	nivell := &db.NivellAdministratiu{
		PaisID:         paisID,
		Nivel:          1,
		NomNivell:      "Pais Test F35-4U6",
		TipusNivell:    "pais",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	}
	nivellID, err := database.CreateNivell(nivell)
	if err != nil {
		t.Fatalf("CreateNivell: %v", err)
	}
	mun := &db.Municipi{
		Nom:            "Municipi F35-4U6",
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	}
	mun.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(nivellID), Valid: true}
	municipiID, err := database.CreateMunicipi(mun)
	if err != nil {
		t.Fatalf("CreateMunicipi: %v", err)
	}
	entitat := &db.Arquebisbat{
		Nom:            "Bisbat F35-4U6",
		TipusEntitat:   "bisbat",
		PaisID:         sql.NullInt64{Int64: int64(paisID), Valid: true},
		ModeracioEstat: "pendent",
	}
	entitatID, err := database.CreateArquebisbat(entitat)
	if err != nil {
		t.Fatalf("CreateArquebisbat: %v", err)
	}
	return paisID, municipiID, entitatID
}

func postF354U6ArxiusImport(t *testing.T, app *core.App, session *http.Cookie, payload []byte) *httptest.ResponseRecorder {
	t.Helper()
	csrfToken := "csrf_f35_4u6_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	_ = writer.WriteField("csrf_token", csrfToken)
	part, err := writer.CreateFormFile("import_file", "arxius-v2.json")
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := io.Copy(part, bytes.NewReader(payload)); err != nil {
		t.Fatalf("io.Copy payload: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/admin/arxius/import/run", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrfToken))
	rr := httptest.NewRecorder()
	app.AdminArxiusImportRun(rr, req)
	return rr
}

func TestF354U6LegacyArxiusImportStillWorks(t *testing.T) {
	app, database, admin, session := setupF354U6ArxiusAdmin(t, "test_f35_4u6_legacy_still_works.sqlite3")
	seedF354U6LegacyFixtureTerritory(t, database)

	projectRoot := findProjectRoot(t)
	fixturePath := filepath.Clean(filepath.Join(projectRoot, "tests", "fixtures", "arxius_export_sample.json"))
	payload, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("ReadFile fixture legacy: %v", err)
	}
	rr := postF354U6ArxiusImport(t, app, session, payload)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("import legacy status=%d body=%s", rr.Code, rr.Body.String())
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM arxius"); got != 1 {
		t.Fatalf("legacy import ha de continuar creant 1 arxiu, got %d", got)
	}
	arxiuRow := findSingleArxiuF354U6(t, database, db.ArxiuFilter{Text: "Arxiu Test A", Limit: 1})
	arxiu, err := database.GetArxiu(arxiuRow.ID)
	if err != nil || arxiu == nil {
		t.Fatalf("GetArxiu legacy importat: err=%v arxiu=%v", err, arxiu)
	}
	if strings.TrimSpace(arxiu.ModeracioEstat) != "pendent" {
		t.Fatalf("legacy import ha de continuar entrant pendent")
	}
	if !arxiu.EntitatEclesiasticaID.Valid || arxiu.EntitatEclesiasticaID.Int64 == 0 {
		t.Fatalf("legacy import ha de continuar resolent l'entitat eclesiastica opcional")
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM usuaris_activitat WHERE usuari_id = ? AND estat = 'pendent' AND detalls = 'import' AND objecte_tipus = 'arxiu'", admin.ID); got != 1 {
		t.Fatalf("legacy import ha de continuar creant activitat pendent, got %d", got)
	}
}

func TestF354U6ArxiuEditFormShowsLegacyFieldAndOptionalConfessionalSection(t *testing.T) {
	app, database, admin, session := setupF354U6ArxiusUIAdmin(t, "test_f35_4u6_edit_form.sqlite3")
	arxiuID := f354CreateArxiu(t, database, "Arxiu UI F35-4U6")
	entitatID, err := database.SaveEntitatReligiosa(&db.EntitatReligiosa{
		Codi:                   "f35_4u6_ui_entity",
		Nom:                    "Entitat Religiosa UI F35-4U6",
		ReligioConfessioCodi:   "catolicisme_ritu_llati",
		NivellConfessionalCodi: "parroquia",
		Estat:                  "actiu",
		ModeracioEstat:         "publicat",
	})
	if err != nil {
		t.Fatalf("SaveEntitatReligiosa UI: %v", err)
	}
	if _, err := database.SaveArxiuEntitatReligiosa(&db.ArxiuEntitatReligiosa{
		ArxiuID:            arxiuID,
		EntitatReligiosaID: entitatID,
		TipusRelacio:       "arxiu_institucional",
		Estat:              "actiu",
		ModeracioEstat:     "pendent",
		CreatedBy:          sql.NullInt64{Int64: int64(admin.ID), Valid: true},
		UpdatedBy:          sql.NullInt64{Int64: int64(admin.ID), Valid: true},
	}); err != nil {
		t.Fatalf("SaveArxiuEntitatReligiosa UI: %v", err)
	}

	body := f354Get(t, app.AdminEditArxiu, "/documentals/arxius/"+strconv.Itoa(arxiuID)+"/edit", session)
	if strings.Contains(body, `id="entitat_search"`) {
		t.Fatalf("el camp visible legacy no ha de continuar al flux principal, body=%s", body)
	}
	if strings.Contains(body, `name="entitat_eclesiastica_id"`) {
		t.Fatalf("la pantalla d'edicio no ha d'enviar el camp legacy entitat_eclesiastica_id, body=%s", body)
	}
	if !strings.Contains(body, `name="codi"`) {
		t.Fatalf("la pantalla d'edicio ha d'exposar el codi estable, body=%s", body)
	}
	if !strings.Contains(body, "Entitats religioses relacionades") {
		t.Fatalf("falta la seccio separada de relacions confessionals opcionals, body=%s", body)
	}
	if !strings.Contains(body, "/documentals/arxius/"+strconv.Itoa(arxiuID)+"/entitats-religioses/new?return_to=/documentals/arxius/"+strconv.Itoa(arxiuID)+"/edit") {
		t.Fatalf("la seccio ha d'enllacar al formulari nou de relacio amb retorn a l'edicio, body=%s", body)
	}
	if !strings.Contains(body, "Entitat Religiosa UI F35-4U6") || !strings.Contains(body, "Pendent") {
		t.Fatalf("la seccio ha de mostrar la relacio existent i el seu estat de moderacio, body=%s", body)
	}
}

func TestF354U6ArchiveRelationTemplatesUseLocaleKeysAndNoInlineDeleteStyle(t *testing.T) {
	root := findProjectRoot(t)
	formSrc := mustReadProjectFileF354U6(t, root, "templates", "admin-arxius-form.html")
	for _, required := range []string{
		`archives.form.code`,
		`archives.type.estatal`,
		`archives.type.privat`,
		`archives.type.familiar`,
		`archives.type.familysearch`,
		`confessional.archive_relation.archive_save_first`,
		`confessional.archive_relation.archive_optional_hint`,
	} {
		if !strings.Contains(formSrc, required) {
			t.Fatalf("la plantilla d'arxiu ha d'usar clau i18n %q", required)
		}
	}
	for _, forbidden := range []string{
		`>Codi<`,
		`>Estatal<`,
		`>Privat<`,
		`>Familiar<`,
		`>FamilySearch<`,
		`Desa primer l'arxiu`,
		`Aquesta secció és opcional`,
	} {
		if strings.Contains(formSrc, forbidden) {
			t.Fatalf("la plantilla d'arxiu no ha de conservar literal hardcoded %q", forbidden)
		}
	}

	for _, rel := range []string{
		filepath.Clean(filepath.Join(root, "templates", "admin-arxius-show.html")),
		filepath.Clean(filepath.Join(root, "templates", "admin-confessional-entity-show.html")),
	} {
		body, err := os.ReadFile(rel)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", rel, err)
		}
		src := string(body)
		if strings.Contains(src, `/documentals/arxius/entitats-religioses/{{ .ID }}/delete" style="display:inline;"`) {
			t.Fatalf("%s no ha de tenir style inline al formulari de baixa de relacio", rel)
		}
		if !strings.Contains(src, `/documentals/arxius/entitats-religioses/{{ .ID }}/delete" class="inline-form"`) {
			t.Fatalf("%s ha d'usar class inline-form al formulari de baixa de relacio", rel)
		}
		for _, forbidden := range []string{`onclick=`, `oninput=`, `onsubmit=`, `javascript:`} {
			if strings.Contains(src, forbidden) {
				t.Fatalf("%s no ha d'afegir %q a la UI de relacions d'arxiu", rel, forbidden)
			}
		}
	}

	for _, localeRel := range []string{
		filepath.Clean(filepath.Join(root, "locales", "cat.json")),
		filepath.Clean(filepath.Join(root, "locales", "en.json")),
		filepath.Clean(filepath.Join(root, "locales", "oc.json")),
	} {
		body, err := os.ReadFile(localeRel)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", localeRel, err)
		}
		src := string(body)
		for _, key := range []string{
			`"archives.form.code"`,
			`"archives.type.estatal"`,
			`"archives.type.privat"`,
			`"archives.type.familiar"`,
			`"archives.type.familysearch"`,
			`"confessional.archive_relation.archive_save_first"`,
			`"confessional.archive_relation.archive_optional_hint"`,
		} {
			if !strings.Contains(src, key) {
				t.Fatalf("%s ha de contenir la clau locale %s", localeRel, key)
			}
		}
	}
}

func TestF354U6ArxiuUpdateWithoutReligiousRelationStaysValid(t *testing.T) {
	app, database, _, session := setupF354U6ArxiusUIAdmin(t, "test_f35_4u6_update_without_relation.sqlite3")
	municipiID := f353YCreateMunicipi(t, database, "Municipi Arxiu Sense Relacio UI F35-4U6")
	arxiuID, err := database.CreateArxiu(&db.Arxiu{
		Nom:            "Arxiu Sense Relacio UI F35-4U6",
		Tipus:          "parroquia",
		Acces:          "online",
		MunicipiID:     sql.NullInt64{Int64: int64(municipiID), Valid: true},
		ModeracioEstat: "pendent",
	})
	if err != nil {
		t.Fatalf("CreateArxiu pendent UI: %v", err)
	}
	arxiu, err := database.GetArxiu(arxiuID)
	if err != nil || arxiu == nil {
		t.Fatalf("GetArxiu: %v", err)
	}

	form := url.Values{}
	form.Set("codi", "f35_4u6_ui_sense_relacio")
	form.Set("nom", arxiu.Nom)
	form.Set("tipus", "privat")
	form.Set("acces", "online")
	form.Set("municipi_id", strconv.FormatInt(arxiu.MunicipiID.Int64, 10))
	form.Set("adreca", "")
	form.Set("ubicacio", "")
	form.Set("what3words", "")
	form.Set("web", "")
	form.Set("notes", "sense relacio opcional")
	req := httptest.NewRequest(http.MethodPost, "/documentals/arxius/"+strconv.Itoa(arxiuID), strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminUpdateArxiu(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("AdminUpdateArxiu sense relacio ha de redirigir, status=%d body=%s", rr.Code, rr.Body.String())
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM arxiu_entitat_religiosa WHERE arxiu_id = ?", arxiuID); got != 0 {
		t.Fatalf("actualitzar un arxiu sense relacio no ha de crear relacions buides, got %d", got)
	}
	arxiu, err := database.GetArxiu(arxiuID)
	if err != nil || arxiu == nil {
		t.Fatalf("GetArxiu actualitzat: err=%v arxiu=%v", err, arxiu)
	}
	if got := strings.TrimSpace(arxiu.Codi); got != "f35_4u6_ui_sense_relacio" {
		t.Fatalf("codi estable no desat a l'edicio: %q", got)
	}
}

func TestF354U6ArchiveUpdateIgnoresTamperedLegacyEntityID(t *testing.T) {
	app, database, _, session := setupF354U6ArxiusUIAdmin(t, "test_f35_4u6_tamper_legacy.sqlite3")
	paisID, _, _ := seedF354U6ArxiuTerritory(t, database)
	originalID, err := database.CreateArquebisbat(&db.Arquebisbat{
		Nom:            "Arquebisbat Legacy Original F35-4U11A-R1",
		TipusEntitat:   "bisbat",
		PaisID:         sql.NullInt64{Int64: int64(paisID), Valid: true},
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateArquebisbat original: %v", err)
	}
	tamperedID, err := database.CreateArquebisbat(&db.Arquebisbat{
		Nom:            "Arquebisbat Legacy Tampered F35-4U11A-R1",
		TipusEntitat:   "bisbat",
		PaisID:         sql.NullInt64{Int64: int64(paisID), Valid: true},
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateArquebisbat tampered: %v", err)
	}
	municipiID := f353YCreateMunicipi(t, database, "Municipi Tamper Arxiu F35-4U11A-R1")
	arxiuID, err := database.CreateArxiu(&db.Arxiu{
		Codi:                  "f35_4u11ar1_arxiu",
		Nom:                   "Arxiu Tamper Legacy F35-4U11A-R1",
		Tipus:                 "privat",
		Acces:                 "online",
		MunicipiID:            sql.NullInt64{Int64: int64(municipiID), Valid: true},
		EntitatEclesiasticaID: sql.NullInt64{Int64: int64(originalID), Valid: true},
		ModeracioEstat:        "pendent",
	})
	if err != nil {
		t.Fatalf("CreateArxiu: %v", err)
	}

	form := url.Values{}
	form.Set("codi", "f35_4u11ar1_arxiu_updated")
	form.Set("nom", "Arxiu Tamper Legacy F35-4U11A-R1")
	form.Set("tipus", "privat")
	form.Set("acces", "online")
	form.Set("municipi_id", strconv.Itoa(municipiID))
	form.Set("entitat_eclesiastica_id", strconv.Itoa(tamperedID))
	req := httptest.NewRequest(http.MethodPost, "/documentals/arxius/"+strconv.Itoa(arxiuID), strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminUpdateArxiu(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("AdminUpdateArxiu tamper status=%d body=%s", rr.Code, rr.Body.String())
	}

	arxiu, err = database.GetArxiu(arxiuID)
	if err != nil || arxiu == nil {
		t.Fatalf("GetArxiu tamper: err=%v arxiu=%v", err, arxiu)
	}
	if got := strings.TrimSpace(arxiu.Codi); got != "f35_4u11ar1_arxiu_updated" {
		t.Fatalf("codi estable no actualitzat: %q", got)
	}
	if !arxiu.EntitatEclesiasticaID.Valid || int(arxiu.EntitatEclesiasticaID.Int64) != originalID {
		t.Fatalf("el backend ha d'ignorar el tampering del legacy entity id, got=%d want=%d", arxiu.EntitatEclesiasticaID.Int64, originalID)
	}
}

func TestF354U11AR1ArchiveListTemplateKeepsSequentialColumnsAndAccessibleFilters(t *testing.T) {
	root := findProjectRoot(t)
	src := mustReadProjectFileF354U6(t, root, "templates", "admin-arxius-list.html")
	for _, token := range []string{
		`id="arxius-filter-nom"`,
		`id="arxius-filter-tipus"`,
		`id="arxius-filter-acces"`,
		`id="arxius-filter-municipi"`,
		`id="arxius-filter-web"`,
		`id="arxius-filter-llibres"`,
		`id="arxius-filter-status"`,
		`class="sr-only" for="arxius-filter-nom"`,
		`scope="col" data-col="0" data-key="nom"`,
		`scope="col" data-col="6" data-key="status"`,
		`action="{{ $manageBase }}/{{ .ID }}/delete" class="inline-form"`,
		`name="csrf_token" value="{{ $.Data.CSRFToken }}"`,
	} {
		if !strings.Contains(src, token) {
			t.Fatalf("falta contracte d'accessibilitat/reindexacio a arxius-list: %q", token)
		}
	}
	if strings.Contains(src, `data-key="entitat"`) {
		t.Fatalf("arxius-list no ha de conservar la columna legacy entitat")
	}
}

func TestF354U6ManualArchiveRelationCreateUsesEntitatReligiosaAndStaysPending(t *testing.T) {
	app, database, admin, session := setupF354U6ArxiusUIAdmin(t, "test_f35_4u6_manual_relation.sqlite3")
	paisID, _, _ := seedF354U6ArxiuTerritory(t, database)
	arxiuID := f354CreateArxiu(t, database, "Arxiu Manual Relacio F35-4U6")
	if _, err := database.CreateArquebisbat(&db.Arquebisbat{
		Nom:            "Arquebisbat Legacy No Selector F35-4U6",
		TipusEntitat:   "bisbat",
		PaisID:         sql.NullInt64{Int64: int64(paisID), Valid: true},
		ModeracioEstat: "publicat",
	}); err != nil {
		t.Fatalf("CreateArquebisbat legacy selector: %v", err)
	}
	entitatID, err := database.SaveEntitatReligiosa(&db.EntitatReligiosa{
		Codi:                   "f35_4u6_manual_entity",
		Nom:                    "Entitat Religiosa Selector F35-4U6",
		ReligioConfessioCodi:   "catolicisme_ritu_llati",
		NivellConfessionalCodi: "parroquia",
		Estat:                  "actiu",
		ModeracioEstat:         "publicat",
	})
	if err != nil {
		t.Fatalf("SaveEntitatReligiosa manual: %v", err)
	}

	newBody := f354Get(t, app.AdminNewArxiuEntitatReligiosaFromArxiu, "/documentals/arxius/"+strconv.Itoa(arxiuID)+"/entitats-religioses/new", session)
	if !strings.Contains(newBody, "Entitat Religiosa Selector F35-4U6") {
		t.Fatalf("el selector nou ha de mostrar entitats del model entitat_religiosa, body=%s", newBody)
	}
	if strings.Contains(newBody, "Arquebisbat Legacy No Selector F35-4U6") {
		t.Fatalf("el selector nou no ha de reutilitzar arquebisbats legacy, body=%s", newBody)
	}

	form := url.Values{}
	form.Set("arxiu_id", strconv.Itoa(arxiuID))
	form.Set("entitat_religiosa_id", strconv.Itoa(entitatID))
	form.Set("tipus_relacio", "custodia_documentacio")
	form.Set("observacions", "Alta manual F35-4U6")
	form.Set("estat", "actiu")
	req := httptest.NewRequest(http.MethodPost, "/documentals/arxius/entitats-religioses/save", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminSaveArxiuEntitatReligiosa(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("l'alta manual de relacio ha de redirigir en exit, status=%d body=%s", rr.Code, rr.Body.String())
	}

	rows, err := database.Query("SELECT id, moderation_status, moderated_by, moderated_at, created_by, updated_by FROM arxiu_entitat_religiosa WHERE arxiu_id = ? AND entitat_religiosa_id = ?", arxiuID, entitatID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("Query relacio manual creada: err=%v rows=%d", err, len(rows))
	}
	relID := parseCountValue(t, rows[0]["id"])
	if got := strings.TrimSpace(asString(rows[0]["moderation_status"])); got != "pendent" {
		t.Fatalf("relacio manual ha d'entrar pendent, got %q", got)
	}
	if strings.TrimSpace(asString(rows[0]["moderated_by"])) != "" || strings.TrimSpace(asString(rows[0]["moderated_at"])) != "" {
		t.Fatalf("relacio manual no ha de quedar moderada directament")
	}
	if got := parseCountValue(t, rows[0]["created_by"]); got != admin.ID {
		t.Fatalf("created_by inesperat: got=%d want=%d", got, admin.ID)
	}
	if got := parseCountValue(t, rows[0]["updated_by"]); got != admin.ID {
		t.Fatalf("updated_by inesperat: got=%d want=%d", got, admin.ID)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM usuaris_activitat WHERE usuari_id = ? AND estat = 'pendent' AND objecte_tipus = 'arxiu_entitat_religiosa' AND objecte_id = ?", admin.ID, relID); got != 1 {
		t.Fatalf("la relacio manual ha d'apareixer com a activitat pendent, got %d", got)
	}
	if moderacioBody := f353YGet(t, app.AdminModeracioList, "/moderacio?type=arxiu_entitat_religiosa", session); !strings.Contains(moderacioBody, "Entitat Religiosa Selector F35-4U6") {
		t.Fatalf("la relacio manual ha d'apareixer a moderacio, body=%s", moderacioBody)
	}
}

func TestF354U6PublishedArchiveRelationDeleteNeedsModerationInsteadOfHardDelete(t *testing.T) {
	app, database, admin, session := setupF354U6ArxiusUIAdmin(t, "test_f35_4u6_delete_published.sqlite3")
	arxiuID := f354CreateArxiu(t, database, "Arxiu Delete Published F35-4U6")
	entitatID := f353YCreateEntitat(t, database, "Entitat Delete Published F35-4U6", "publicat")
	relID, err := database.SaveArxiuEntitatReligiosa(&db.ArxiuEntitatReligiosa{
		ArxiuID:            arxiuID,
		EntitatReligiosaID: entitatID,
		TipusRelacio:       "arxiu_institucional",
		Estat:              "actiu",
		ModeracioEstat:     "publicat",
		CreatedBy:          sql.NullInt64{Int64: int64(admin.ID), Valid: true},
		UpdatedBy:          sql.NullInt64{Int64: int64(admin.ID), Valid: true},
	})
	if err != nil {
		t.Fatalf("SaveArxiuEntitatReligiosa publicada: %v", err)
	}

	rr := f354U6DeleteArchiveRelation(t, app, session, relID)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("el delete publicat ha de redirigir, got=%d body=%s", rr.Code, rr.Body.String())
	}
	rows, err := database.Query("SELECT moderation_status, moderation_notes FROM arxiu_entitat_religiosa WHERE id = ?", relID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("la relacio publicada no s'ha d'eliminar fisicament abans de moderacio: err=%v rows=%d", err, len(rows))
	}
	if got := strings.TrimSpace(asString(rows[0]["moderation_status"])); got != "pendent" {
		t.Fatalf("la baixa publicada ha de quedar pendent, got %q", got)
	}
	if got := strings.TrimSpace(asString(rows[0]["moderation_notes"])); got != "__delete_requested__" {
		t.Fatalf("la baixa publicada ha de marcar-se com delete request, got %q", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM usuaris_activitat WHERE usuari_id = ? AND estat = 'pendent' AND objecte_tipus = 'arxiu_entitat_religiosa' AND objecte_id = ?", admin.ID, relID); got != 1 {
		t.Fatalf("la baixa publicada ha de crear activitat pendent, got %d", got)
	}

	f353YPostModeracio(t, app.AdminModeracioAprovar, session, relID, "arxiu_entitat_religiosa", "aprovar", "")
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM arxiu_entitat_religiosa WHERE id = ?", relID); got != 0 {
		t.Fatalf("aprovar la baixa publicada ha d'eliminar la relacio, got %d", got)
	}
}

func TestF354U6PendingArchiveRelationDeleteCancelsProposalWithoutHardDeleteFlow(t *testing.T) {
	app, database, admin, session := setupF354U6ArxiusUIAdmin(t, "test_f35_4u6_delete_pending.sqlite3")
	arxiuID := f354CreateArxiu(t, database, "Arxiu Delete Pending F35-4U6")
	entitatID := f353YCreateEntitat(t, database, "Entitat Delete Pending F35-4U6", "publicat")
	relID, err := database.SaveArxiuEntitatReligiosa(&db.ArxiuEntitatReligiosa{
		ArxiuID:            arxiuID,
		EntitatReligiosaID: entitatID,
		TipusRelacio:       "context_religios",
		Estat:              "actiu",
		ModeracioEstat:     "pendent",
		CreatedBy:          sql.NullInt64{Int64: int64(admin.ID), Valid: true},
		UpdatedBy:          sql.NullInt64{Int64: int64(admin.ID), Valid: true},
	})
	if err != nil {
		t.Fatalf("SaveArxiuEntitatReligiosa pendent: %v", err)
	}
	if _, err := database.InsertUserActivity(&db.UserActivity{
		UserID:     admin.ID,
		Action:     "crear",
		ObjectType: "arxiu_entitat_religiosa",
		ObjectID:   sql.NullInt64{Int64: int64(relID), Valid: true},
		Status:     "pendent",
		Details:    "test",
		CreatedAt:  time.Now(),
	}); err != nil {
		t.Fatalf("InsertUserActivity pending relation: %v", err)
	}

	rr := f354U6DeleteArchiveRelation(t, app, session, relID)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("el delete pendent ha de redirigir, got=%d body=%s", rr.Code, rr.Body.String())
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM arxiu_entitat_religiosa WHERE id = ?", relID); got != 0 {
		t.Fatalf("la proposta pendent s'ha de poder cancel·lar fisicament, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM usuaris_activitat WHERE usuari_id = ? AND objecte_tipus = 'arxiu_entitat_religiosa' AND objecte_id = ? AND estat = 'anulat'", admin.ID, relID); got == 0 {
		t.Fatalf("cancel·lar la proposta pendent ha d'anul·lar l'activitat pendent")
	}
}

func TestF354U6ArchiveRelationEditLoadsCurrentTargetBeforeApplyingChanges(t *testing.T) {
	root := findProjectRoot(t)
	body, err := os.ReadFile(filepath.Join(root, "core", "arxiu_entitat_religiosa.go"))
	if err != nil {
		t.Fatalf("ReadFile arxiu_entitat_religiosa.go: %v", err)
	}
	src := string(body)
	currentLoad := `current, _ = a.DB.GetArxiuEntitatReligiosa(id)`
	currentPerm := `a.requirePermissionKey(w, r, permKeyTerritoriConfessionalArxiusEntitatsEdit, a.resolveArxiuTarget(current.ArxiuID))`
	parseForm := `rel := parseArxiuEntitatReligiosaForm(r)`
	moveGuard := `if current != nil && rel.ArxiuID != current.ArxiuID && !a.canModerateModular(user) {`
	for _, token := range []string{currentLoad, currentPerm, parseForm, moveGuard} {
		if !strings.Contains(src, token) {
			t.Fatalf("falta contracte de seguretat F35-4U6 al handler: %s", token)
		}
	}
	if strings.Index(src, currentLoad) > strings.Index(src, parseForm) {
		t.Fatalf("el handler ha de carregar la relacio existent abans de parsejar el formulari")
	}
	if strings.Index(src, currentPerm) > strings.Index(src, parseForm) {
		t.Fatalf("el handler ha de validar permisos sobre el target real abans d'aplicar canvis")
	}
}

func seedF354U6LegacyFixtureTerritory(t *testing.T, database db.DB) {
	t.Helper()
	pais := &db.Pais{CodiISO2: "ES", CodiISO3: "ESP", CodiPaisNum: "724"}
	paisID, err := database.CreatePais(pais)
	if err != nil {
		t.Fatalf("CreatePais legacy: %v", err)
	}
	nivell := &db.NivellAdministratiu{
		PaisID:         paisID,
		Nivel:          1,
		NomNivell:      "Pais Test Legacy F35-4U6",
		TipusNivell:    "pais",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	}
	nivellID, err := database.CreateNivell(nivell)
	if err != nil {
		t.Fatalf("CreateNivell legacy: %v", err)
	}
	mun := &db.Municipi{
		Nom:            "Municipi Test",
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	}
	mun.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(nivellID), Valid: true}
	if _, err := database.CreateMunicipi(mun); err != nil {
		t.Fatalf("CreateMunicipi legacy: %v", err)
	}
	entitat := &db.Arquebisbat{
		Nom:            "Bisbat Test",
		TipusEntitat:   "bisbat",
		PaisID:         sql.NullInt64{Int64: int64(paisID), Valid: true},
		ModeracioEstat: "pendent",
	}
	if _, err := database.CreateArquebisbat(entitat); err != nil {
		t.Fatalf("CreateArquebisbat legacy: %v", err)
	}
}

func setupF354U6ArxiusUIAdmin(t *testing.T, dbName string) (*core.App, db.DB, *db.User, *http.Cookie) {
	t.Helper()
	app, database := newTestAppForLogin(t, dbName)
	admin := createTestUser(t, database, "f35_4u6_ui_admin_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	policy := createPolicyWithGrant(t, database, "f35_4u6_ui_policy_"+strconv.FormatInt(time.Now().UnixNano(), 10), "documentals.arxius.view")
	for _, grant := range []string{
		"documentals.arxius.edit",
		"territori.confessional.arxius_entitats.create",
		"territori.confessional.arxius_entitats.edit",
		"territori.confessional.arxius_entitats.delete",
	} {
		addGrantToPolicy(t, database, policy, grant)
	}
	assignPolicyToUser(t, database, admin.ID, policy)
	session := createSessionCookie(t, database, admin.ID, "sess_f35_4u6_ui_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	return app, database, admin, session
}

func f354U6DeleteArchiveRelation(t *testing.T, app *core.App, session *http.Cookie, relID int) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/documentals/arxius/entitats-religioses/"+strconv.Itoa(relID)+"/delete", nil)
	req.AddCookie(session)
	rr := httptest.NewRecorder()
	app.AdminDeleteArxiuEntitatReligiosa(rr, req)
	return rr
}
