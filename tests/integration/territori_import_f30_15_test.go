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

	"github.com/marcmoiagese/CercaGenealogica/db"
	testcommon "github.com/marcmoiagese/CercaGenealogica/tests/common"
)

func cloneDBConfigForTest(t *testing.T, cfg testcommon.TestDBConfig, sqliteName string) map[string]string {
	t.Helper()

	out := map[string]string{}
	for k, v := range cfg.Config {
		out[k] = v
	}
	out["RECREADB"] = "true"
	if _, ok := out["LOG_LEVEL"]; !ok {
		out["LOG_LEVEL"] = "silent"
	}
	if strings.ToLower(cfg.Engine) == "sqlite" {
		tmpDir := t.TempDir()
		out["DB_PATH"] = filepath.Join(tmpDir, sqliteName)
	}
	return out
}

func buildMultipartRequestFromBytes(t *testing.T, method, targetURL, fileField, fileName string, payload []byte, fields map[string]string, session *http.Cookie) *http.Request {
	t.Helper()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	for k, v := range fields {
		if err := writer.WriteField(k, v); err != nil {
			t.Fatalf("WriteField %s ha fallat: %v", k, err)
		}
	}
	part, err := writer.CreateFormFile(fileField, fileName)
	if err != nil {
		t.Fatalf("CreateFormFile ha fallat: %v", err)
	}
	if _, err := io.Copy(part, bytes.NewReader(payload)); err != nil {
		t.Fatalf("escriure payload ha fallat: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close multipart ha fallat: %v", err)
	}

	req := httptest.NewRequest(method, targetURL, body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if session != nil {
		req.AddCookie(session)
	}
	if csrf, ok := fields["csrf_token"]; ok && strings.TrimSpace(csrf) != "" {
		req.AddCookie(csrfCookie(csrf))
	}
	return req
}

func buildMultipartRequestFromFile(t *testing.T, method, targetURL, fileField string, path string, fields map[string]string, session *http.Cookie) *http.Request {
	t.Helper()

	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile %s ha fallat: %v", path, err)
	}
	return buildMultipartRequestFromBytes(t, method, targetURL, fileField, filepath.Base(path), payload, fields, session)
}

func requireRedirectLocation(t *testing.T, rr *httptest.ResponseRecorder) *url.URL {
	t.Helper()

	if rr.Code != http.StatusSeeOther {
		t.Fatalf("esperava 303, rebut %d; body=%s", rr.Code, rr.Body.String())
	}
	loc := rr.Header().Get("Location")
	if strings.TrimSpace(loc) == "" {
		t.Fatalf("redirect sense Location")
	}
	parsed, err := url.Parse(loc)
	if err != nil {
		t.Fatalf("Location invàlida %q: %v", loc, err)
	}
	return parsed
}

func mustFindTemplateByName(t *testing.T, database db.DB, name string) int {
	t.Helper()

	templates, err := database.ListCSVImportTemplates(db.CSVImportTemplateFilter{
		IncludePublic: true,
		Limit:         500,
	})
	if err != nil {
		t.Fatalf("ListCSVImportTemplates ha fallat: %v", err)
	}
	for _, tpl := range templates {
		if tpl.Name == name {
			return tpl.ID
		}
	}
	t.Fatalf("plantilla %q no trobada", name)
	return 0
}

func runMultipartHandler(t *testing.T, req *http.Request, handler func(http.ResponseWriter, *http.Request)) *url.URL {
	t.Helper()

	rr := httptest.NewRecorder()
	handler(rr, req)
	return requireRedirectLocation(t, rr)
}

func TestReplaceAdminClosureDedupesAcrossBackendsF3015(t *testing.T) {
	for _, dbCfg := range testcommon.LoadTestDBConfigs(t) {
		dbCfg := dbCfg
		t.Run(dbCfg.Label, func(t *testing.T) {
			if strings.ToLower(dbCfg.Engine) == "mysql" {
				t.Skip("MySQL de tests continua amb incidència externa d'inicialització aliena a F30-15")
			}

			cfg := cloneDBConfigForTest(t, dbCfg, "test_f30_15_closure.sqlite3")
			_, database := newTestAppForConfig(t, cfg)

			suffix := strings.ToUpper(strconv.FormatInt(time.Now().UnixNano()%46655, 36))
			for len(suffix) < 3 {
				suffix = "0" + suffix
			}
			iso2 := suffix[:2]
			iso3 := suffix[:3]
			pais := &db.Pais{CodiISO2: iso2, CodiISO3: iso3, CodiPaisNum: strconv.FormatInt(100000+time.Now().UnixNano()%900000, 10)}
			paisID, err := database.CreatePais(pais)
			if err != nil {
				t.Fatalf("[%s] CreatePais ha fallat: %v", dbCfg.Label, err)
			}
			nivell := &db.NivellAdministratiu{
				PaisID:         paisID,
				Nivel:          1,
				NomNivell:      "Testland",
				TipusNivell:    "pais",
				CodiOficial:    "TS",
				Estat:          "actiu",
				ModeracioEstat: "pendent",
			}
			nivellID, err := database.CreateNivell(nivell)
			if err != nil {
				t.Fatalf("[%s] CreateNivell ha fallat: %v", dbCfg.Label, err)
			}
			mun := &db.Municipi{
				Nom:            "Municipi Closure",
				Tipus:          "municipi",
				Estat:          "actiu",
				ModeracioEstat: "pendent",
			}
			mun.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(nivellID), Valid: true}
			munID, err := database.CreateMunicipi(mun)
			if err != nil {
				t.Fatalf("[%s] CreateMunicipi ha fallat: %v", dbCfg.Label, err)
			}

			entries := []db.AdminClosureEntry{
				{DescendantMunicipiID: munID, AncestorType: "municipi", AncestorID: munID},
				{DescendantMunicipiID: munID + 999, AncestorType: "municipi", AncestorID: munID},
				{DescendantMunicipiID: munID, AncestorType: "nivell", AncestorID: nivellID},
				{DescendantMunicipiID: munID, AncestorType: "nivell", AncestorID: nivellID},
				{DescendantMunicipiID: munID, AncestorType: "pais", AncestorID: paisID},
			}
			if err := database.ReplaceAdminClosure(munID, entries); err != nil {
				t.Fatalf("[%s] ReplaceAdminClosure (1) ha fallat: %v", dbCfg.Label, err)
			}
			if err := database.ReplaceAdminClosure(munID, entries); err != nil {
				t.Fatalf("[%s] ReplaceAdminClosure (2) ha fallat: %v", dbCfg.Label, err)
			}

			got, err := database.ListAdminClosure(munID)
			if err != nil {
				t.Fatalf("[%s] ListAdminClosure ha fallat: %v", dbCfg.Label, err)
			}
			if len(got) != 3 {
				t.Fatalf("[%s] admin_closure esperat 3 entrades úniques, got %d: %+v", dbCfg.Label, len(got), got)
			}
			for _, row := range got {
				if row.DescendantMunicipiID != munID {
					t.Fatalf("[%s] descendant inesperat %d a %+v", dbCfg.Label, row.DescendantMunicipiID, row)
				}
			}
		})
	}
}

func TestTerritoriImportExplicitPublishedStatusDoesNotAutoPublishF3015(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_15_status_import.sqlite3")

	admin := createTestUser(t, database, "f30_15_status_admin_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_f30_15_status_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	pais := &db.Pais{CodiISO2: "TS", CodiISO3: "TST", CodiPaisNum: "999"}
	paisID, err := database.CreatePais(pais)
	if err != nil {
		t.Fatalf("CreatePais ha fallat: %v", err)
	}
	nivellExisting := &db.NivellAdministratiu{
		PaisID:         paisID,
		Nivel:          1,
		NomNivell:      "Testland",
		TipusNivell:    "pais",
		CodiOficial:    "TS",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	}
	nivellExistingID, err := database.CreateNivell(nivellExisting)
	if err != nil {
		t.Fatalf("CreateNivell existent ha fallat: %v", err)
	}
	munExisting := &db.Municipi{
		Nom:            "Municipi Existing",
		Tipus:          "municipi",
		CodiPostal:     "00000",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	}
	munExisting.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(nivellExistingID), Valid: true}
	if _, err := database.CreateMunicipi(munExisting); err != nil {
		t.Fatalf("CreateMunicipi existent ha fallat: %v", err)
	}

	payload := map[string]interface{}{
		"version": 1,
		"countries": []map[string]interface{}{
			{"iso2": "TS", "iso3": "TST", "num": "999"},
		},
		"levels": []map[string]interface{}{
			{"id": 1, "pais_iso2": "TS", "nivel": 1, "nom": "Testland", "tipus": "pais", "codi": "TS", "estat": "actiu", "moderation_status": "publicat"},
			{"id": 2, "pais_iso2": "TS", "nivel": 2, "nom": "Comarca Nova", "tipus": "comarca", "codi": "TS-C", "parent_id": 1, "estat": "actiu", "moderation_status": "publicat"},
		},
		"municipis": []map[string]interface{}{
			{"id": 1, "pais_iso2": "TS", "nom": "Municipi Existing", "tipus": "municipi", "nivells": []int{1, 0, 0, 0, 0, 0, 0}, "codi_postal": "00000", "estat": "actiu", "moderation_status": "publicat"},
			{"id": 2, "pais_iso2": "TS", "nom": "Municipi Nou", "tipus": "municipi", "nivells": []int{1, 2, 0, 0, 0, 0, 0}, "codi_postal": "00001", "estat": "actiu", "moderation_status": "publicat"},
		},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal payload ha fallat: %v", err)
	}

	csrfToken := "csrf_f30_15_status"
	req := buildMultipartRequestFromBytes(t, http.MethodPost, "/admin/territori/import/run", "import_file", "territori.json", raw, map[string]string{
		"csrf_token": csrfToken,
	}, session)
	location := runMultipartHandler(t, req, app.AdminTerritoriImportRun)

	if location.Query().Get("closure_errors") != "0" {
		t.Fatalf("closure_errors esperat 0, got %q", location.Query().Get("closure_errors"))
	}

	nivellAfter := findNivellByNom(t, database, "Testland")
	if nivellAfter.ModeracioEstat != "pendent" {
		t.Fatalf("nivell existent reparat esperat pendent, got %q", nivellAfter.ModeracioEstat)
	}
	nivellNou := findNivellByNom(t, database, "Comarca Nova")
	if nivellNou.ModeracioEstat != "pendent" {
		t.Fatalf("nivell nou esperat pendent, got %q", nivellNou.ModeracioEstat)
	}

	munExistingAfter := findMunicipiByNom(t, database, "Municipi Existing")
	if munExistingAfter.ModeracioEstat != "pendent" {
		t.Fatalf("municipi existent reparat esperat pendent, got %q", munExistingAfter.ModeracioEstat)
	}
	munNou := findMunicipiByNom(t, database, "Municipi Nou")
	if munNou.ModeracioEstat != "pendent" {
		t.Fatalf("municipi nou esperat pendent, got %q", munNou.ModeracioEstat)
	}

	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM nivells_administratius"); got != 2 {
		t.Fatalf("nivells totals esperats 2, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM municipis"); got != 2 {
		t.Fatalf("municipis totals esperats 2, got %d", got)
	}
}

func TestTerritoriRealFilesEndToEndSequenceF3015(t *testing.T) {
	projectRoot := findProjectRoot(t)
	territoriPath := filepath.Join(projectRoot, "plantilla-temporal", "importacions", "territori-export-idescat-nuclis-mapat.json")
	eclesPath := filepath.Join(projectRoot, "plantilla-temporal", "importacions", "eclesiastic-export.json")
	arxiusPath := filepath.Join(projectRoot, "plantilla-temporal", "importacions", "arxius-export.json")
	llibresPath := filepath.Join(projectRoot, "plantilla-temporal", "importacions", "llibres-export.json")
	csvPath := filepath.Join(projectRoot, "plantilla-temporal", "importacions", "Index_llibres_Digitals.csv")
	for _, path := range []string{territoriPath, eclesPath, arxiusPath, llibresPath, csvPath} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("fitxer local requerit no disponible %s: %v", path, err)
		}
	}

	app, database := newTestAppForLogin(t, "test_f30_15_real_sequence.sqlite3")
	if err := app.EnsureSystemImportTemplates(); err != nil {
		t.Fatalf("EnsureSystemImportTemplates ha fallat: %v", err)
	}

	admin := createTestUser(t, database, "f30_15_real_admin_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_f30_15_real_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	territoriLoc := runMultipartHandler(t, buildMultipartRequestFromFile(t, http.MethodPost, "/admin/territori/import/run", "import_file", territoriPath, map[string]string{
		"csrf_token": "csrf_f30_15_real_territori",
	}, session), app.AdminTerritoriImportRun)
	if territoriLoc.Query().Get("closure_errors") != "0" || territoriLoc.Query().Get("rebuild_errors") != "0" {
		t.Fatalf("import territori amb errors de closure/rebuild: %s", territoriLoc.String())
	}
	if territoriLoc.Query().Get("levels_errors") != "0" || territoriLoc.Query().Get("municipis_errors") != "0" {
		t.Fatalf("import territori amb errors de nivells/municipis: %s", territoriLoc.String())
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM paisos"); got != 1 {
		t.Fatalf("paisos totals esperats 1, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM nivells_administratius"); got != 787 {
		t.Fatalf("nivells totals esperats 787, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM municipis"); got != 5776 {
		t.Fatalf("municipis totals esperats 5776, got %d", got)
	}

	garrigues := findNivellByNom(t, database, "Garrigues")
	arbeca := findMunicipiByNom(t, database, "Arbeca")
	if garrigues.ModeracioEstat != "pendent" || arbeca.ModeracioEstat != "pendent" {
		t.Fatalf("Garrigues/Arbeca esperats pendents, got %s/%s", garrigues.ModeracioEstat, arbeca.ModeracioEstat)
	}
	garriguesCount, err := database.CountMunicipisBrowse(db.MunicipiBrowseFilter{
		NivellID: garrigues.ID,
		Status:   "pendent",
	})
	if err != nil {
		t.Fatalf("CountMunicipisBrowse Garrigues ha fallat: %v", err)
	}
	if garriguesCount != 24 {
		t.Fatalf("municipis pendents a Garrigues esperats 24, got %d", garriguesCount)
	}
	closureEntries, err := database.ListAdminClosure(arbeca.ID)
	if err != nil {
		t.Fatalf("ListAdminClosure Arbeca ha fallat: %v", err)
	}
	if len(closureEntries) < 3 {
		t.Fatalf("admin_closure Arbeca massa curt: %+v", closureEntries)
	}

	runMultipartHandler(t, buildMultipartRequestFromFile(t, http.MethodPost, "/admin/eclesiastic/import/run", "import_file", eclesPath, map[string]string{
		"csrf_token": "csrf_f30_15_real_ecles",
	}, session), app.AdminEclesiasticImportRun)
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM arquebisbats"); got < 10 {
		t.Fatalf("arquebisbats totals esperats >= 10, got %d", got)
	}

	runMultipartHandler(t, buildMultipartRequestFromFile(t, http.MethodPost, "/admin/arxius/import/run", "import_file", arxiusPath, map[string]string{
		"csrf_token": "csrf_f30_15_real_arxius",
	}, session), app.AdminArxiusImportRun)
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM arxius"); got != 2 {
		t.Fatalf("arxius totals esperats 2, got %d", got)
	}

	runMultipartHandler(t, buildMultipartRequestFromFile(t, http.MethodPost, "/admin/llibres/import/run", "import_file", llibresPath, map[string]string{
		"csrf_token": "csrf_f30_15_real_llibres",
	}, session), app.AdminLlibresImportRun)
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM llibres"); got != 12213 {
		t.Fatalf("llibres totals esperats 12213, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM llibres WHERE municipi_id = ?", arbeca.ID); got == 0 {
		t.Fatalf("esperava llibres vinculats a Arbeca després de l'import real")
	}

	templateID := mustFindTemplateByName(t, database, "System: Baptismes Marcmoia (v2)")
	csvLoc := runMultipartHandler(t, buildMultipartRequestFromFile(t, http.MethodPost, "/documentals/llibres/importar", "csv_file", csvPath, map[string]string{
		"csrf_token":  "csrf_f30_15_real_csv",
		"model":       "template",
		"template_id": strconv.Itoa(templateID),
		"separator":   ";",
	}, session), app.AdminImportRegistresGlobal)
	if csvLoc.Query().Get("failed") == "" {
		t.Fatalf("redirect CSV sense comptadors: %s", csvLoc.String())
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM transcripcions_raw"); got == 0 {
		t.Fatalf("esperava transcripcions importades del CSV real")
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM transcripcions_raw WHERE moderation_status = 'pendent'"); got == 0 {
		t.Fatalf("esperava transcripcions pendents després del CSV real")
	}
}
