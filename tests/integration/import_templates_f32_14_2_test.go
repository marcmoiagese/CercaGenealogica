package integration

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/cnf"
	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

type f32142PostgresStrongSnapshotCountingDB struct {
	db.DB
	listTranscripcionsCalls int
	listPersonesByBookCalls int
	listAtributsByBookCalls int
	listStrongScopedCalls   int
}

func (d *f32142PostgresStrongSnapshotCountingDB) ListTranscripcionsRaw(llibreID int, f db.TranscripcioFilter) ([]db.TranscripcioRaw, error) {
	d.listTranscripcionsCalls++
	return d.DB.ListTranscripcionsRaw(llibreID, f)
}

func (d *f32142PostgresStrongSnapshotCountingDB) ListTranscripcioPersonesByLlibreID(llibreID int) (map[int][]db.TranscripcioPersonaRaw, error) {
	d.listPersonesByBookCalls++
	return d.DB.ListTranscripcioPersonesByLlibreID(llibreID)
}

func (d *f32142PostgresStrongSnapshotCountingDB) ListTranscripcioAtributsByLlibreID(llibreID int) (map[int][]db.TranscripcioAtributRaw, error) {
	d.listAtributsByBookCalls++
	return d.DB.ListTranscripcioAtributsByLlibreID(llibreID)
}

func (d *f32142PostgresStrongSnapshotCountingDB) ListTranscripcioStrongMatchCandidatesUpToID(bookID int, tipusActe, pageKey string, maxExistingID int) ([]db.TranscripcioRaw, map[int][]db.TranscripcioPersonaRaw, map[int][]db.TranscripcioAtributRaw, error) {
	d.listStrongScopedCalls++
	loader, ok := d.DB.(interface {
		ListTranscripcioStrongMatchCandidatesUpToID(bookID int, tipusActe, pageKey string, maxExistingID int) ([]db.TranscripcioRaw, map[int][]db.TranscripcioPersonaRaw, map[int][]db.TranscripcioAtributRaw, error)
	})
	if !ok {
		return nil, nil, nil, nil
	}
	return loader.ListTranscripcioStrongMatchCandidatesUpToID(bookID, tipusActe, pageKey, maxExistingID)
}

func (d *f32142PostgresStrongSnapshotCountingDB) GetMaxTranscripcioRawID() (int, error) {
	loader, ok := d.DB.(interface {
		GetMaxTranscripcioRawID() (int, error)
	})
	if !ok {
		return 0, nil
	}
	return loader.GetMaxTranscripcioRawID()
}

func TestTemplateImportStrongDedupUsesBookScopedSnapshotPostgresF32142(t *testing.T) {
	for _, cfg := range loadSQLiteAndPostgresConfigsForImportHistory(t) {
		if cfg.Engine != "postgres" {
			continue
		}
		cfg := cfg
		t.Run(cfg.Label, func(t *testing.T) {
			app, database := newTestAppForConfig(t, cfg.Config)
			user, sessionID := createF7UserWithSession(t, database)
			ensureAdminPolicyForUser(t, database, user.ID)
			llibreID, pagina1ID := createF7LlibreWithPagina(t, database, user.ID)
			pagina2ID, err := database.SaveLlibrePagina(&db.LlibrePagina{LlibreID: llibreID, NumPagina: 2, Estat: "indexada"})
			if err != nil {
				t.Fatalf("[%s] SaveLlibrePagina(2) ha fallat: %v", cfg.Label, err)
			}

			countingDB := &f32142PostgresStrongSnapshotCountingDB{DB: database}
			app.DB = countingDB

			templateID := createF3210Template(t, database, user.ID, "f32142-"+cfg.Label)
			existingPage1ID := createF3210ExistingStrongBaptismeForPage(t, database, llibreID, 1, pagina1ID)
			existingPage2ID := createF3210ExistingStrongBaptismeForPage(t, database, llibreID, 2, pagina2ID)

			req := buildImportGlobalRequest(t, sessionID, "csrf-f32142-"+cfg.Label, map[string]string{
				"model":       "template",
				"template_id": strconv.Itoa(templateID),
				"separator":   ",",
			}, strings.Join([]string{
				"llibre_id,tipus_acte,pagina,cognoms,pare,mare,nascut,acte",
				strconv.Itoa(llibreID) + ",baptisme,1,Garcia Soler Joan,Pere Garcia,Maria Puig,01/02/1890,05/02/1890",
				strconv.Itoa(llibreID) + ",baptisme,2,Garcia Soler Joan,Pere Garcia,Maria Puig,01/02/1890,05/02/1890",
			}, "\n"))
			rr := httptest.NewRecorder()
			app.AdminImportRegistresGlobal(rr, req)
			if rr.Code != http.StatusSeeOther {
				t.Fatalf("[%s] status inesperat: %d body=%s", cfg.Label, rr.Code, rr.Body.String())
			}

			registres, err := database.ListTranscripcionsRaw(llibreID, db.TranscripcioFilter{Limit: -1})
			if err != nil {
				t.Fatalf("[%s] ListTranscripcionsRaw ha fallat: %v", cfg.Label, err)
			}
			if len(registres) != 2 {
				t.Fatalf("[%s] el merge fort per dues pàgines ha de conservar només els 2 existents, got=%d", cfg.Label, len(registres))
			}
			found := map[int]bool{}
			for _, row := range registres {
				found[row.ID] = true
			}
			if !found[existingPage1ID] || !found[existingPage2ID] {
				t.Fatalf("[%s] s'han de mantenir els registres existents a les dues pàgines, ids=%v", cfg.Label, found)
			}
			if countingDB.listStrongScopedCalls != 0 {
				t.Fatalf("[%s] PostgreSQL no ha de recórrer al carregador scoped per context un cop té snapshot per llibre: scoped_calls=%d", cfg.Label, countingDB.listStrongScopedCalls)
			}
			if countingDB.listTranscripcionsCalls > 2 || countingDB.listPersonesByBookCalls > 2 || countingDB.listAtributsByBookCalls > 2 {
				t.Fatalf("[%s] PostgreSQL no ha d'escalar per context al duplicate-check fort; s'accepta el preload del write i el read lateral de sidefx, got trans=%d persones=%d atributs=%d", cfg.Label, countingDB.listTranscripcionsCalls, countingDB.listPersonesByBookCalls, countingDB.listAtributsByBookCalls)
			}
		})
	}
}

func TestTemplateImportRealCSVWriteMetricsSQLitePostgresF32142(t *testing.T) {
	if os.Getenv("CG_F3212_FIX2_REAL_IMPORT") != "1" {
		t.Skip("validació real F32-14-2 només s'executa explícitament amb CG_F3212_FIX2_REAL_IMPORT=1")
	}

	projectRoot := findProjectRoot(t)
	territoriPath := filepath.Join(projectRoot, "plantilla-temporal", "importacions", "territori-export-idescat-nuclis-mapat.json")
	eclesPath := filepath.Join(projectRoot, "plantilla-temporal", "importacions", "eclesiastic-export.json")
	arxiusPath := filepath.Join(projectRoot, "plantilla-temporal", "importacions", "arxius-export.json")
	llibresPath := filepath.Join(projectRoot, "plantilla-temporal", "importacions", "llibres-export.json")
	csvPath := filepath.Join(projectRoot, "plantilla-temporal", "importacions", "Index_llibres_Digitals.csv")
	for _, path := range []string{territoriPath, eclesPath, arxiusPath, llibresPath, csvPath} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("fitxer real requerit no disponible %s: %v", path, err)
		}
	}

	rowsRe := regexp.MustCompile(`rows=([0-9]+)`)
	createdRe := regexp.MustCompile(`created=([0-9]+)`)
	updatedRe := regexp.MustCompile(`updated=([0-9]+)`)
	failedRe := regexp.MustCompile(`failed=([0-9]+)`)
	parseRe := regexp.MustCompile(`parse_dur=([^ ]+)`)
	writeRe := regexp.MustCompile(`write_dur=([^ ]+)`)
	dupRe := regexp.MustCompile(`write_duplicate_check_dur=([^ ]+)`)
	personaPersistRe := regexp.MustCompile(`write_persona_persist_dur=([^ ]+)`)
	linksPersistRe := regexp.MustCompile(`write_links_persist_dur=([^ ]+)`)
	sidefxDurRe := regexp.MustCompile(`sidefx_dur=([^ ]+)`)
	totalRe := regexp.MustCompile(`total_dur=([^ ]+)`)

	for _, cfg := range loadSQLiteAndPostgresConfigsForImportHistory(t) {
		cfg := cfg
		t.Run(cfg.Label, func(t *testing.T) {
			cfgMap := map[string]string{}
			for k, v := range cfg.Config {
				cfgMap[k] = v
			}
			cfgMap["RECREADB_RESET"] = "true"
			app, database := newTestAppForConfig(t, cfgMap)
			if err := app.EnsureSystemImportTemplates(); err != nil {
				t.Fatalf("[%s] EnsureSystemImportTemplates ha fallat: %v", cfg.Label, err)
			}

			admin, sessionID := createF7UserWithSession(t, database)
			ensureAdminPolicyForUser(t, database, admin.ID)
			session := &http.Cookie{Name: "cg_session", Value: sessionID, Path: "/"}

			prevDBConfig := cnf.Config
			prevDBLogLevel := ""
			if cnf.Config == nil {
				cnf.Config = map[string]string{}
			} else {
				prevDBLogLevel = cnf.Config["LOG_LEVEL"]
			}
			cnf.Config["LOG_LEVEL"] = "debug"
			core.SetLogLevel("debug")
			defer func() {
				core.SetLogLevel("error")
				if prevDBConfig == nil {
					cnf.Config = nil
				} else {
					cnf.Config["LOG_LEVEL"] = prevDBLogLevel
				}
			}()

			buf, restore := captureStandardLog(t)
			defer restore()

			runMultipartHandler(t, buildMultipartRequestFromFile(t, http.MethodPost, "/admin/territori/import/run", "import_file", territoriPath, map[string]string{
				"csrf_token": "csrf_f32142_real_territori_" + cfg.Label,
			}, session), app.AdminTerritoriImportRun)
			runMultipartHandler(t, buildMultipartRequestFromFile(t, http.MethodPost, "/admin/eclesiastic/import/run", "import_file", eclesPath, map[string]string{
				"csrf_token": "csrf_f32142_real_ecles_" + cfg.Label,
			}, session), app.AdminEclesiasticImportRun)
			runMultipartHandler(t, buildMultipartRequestFromFile(t, http.MethodPost, "/admin/arxius/import/run", "import_file", arxiusPath, map[string]string{
				"csrf_token": "csrf_f32142_real_arxius_" + cfg.Label,
			}, session), app.AdminArxiusImportRun)
			runMultipartHandler(t, buildMultipartRequestFromFile(t, http.MethodPost, "/admin/llibres/import/run", "import_file", llibresPath, map[string]string{
				"csrf_token": "csrf_f32142_real_llibres_" + cfg.Label,
			}, session), app.AdminLlibresImportRun)

			templateID := mustFindTemplateByName(t, database, "System: Baptismes Marcmoia (v2)")
			csvLoc := runMultipartHandler(t, buildMultipartRequestFromFile(t, http.MethodPost, "/documentals/llibres/importar", "csv_file", csvPath, map[string]string{
				"csrf_token":  "csrf_f32142_real_csv_" + cfg.Label,
				"model":       "template",
				"template_id": strconv.Itoa(templateID),
				"separator":   ";",
			}, session), app.AdminImportRegistresGlobal)
			if csvLoc.Query().Get("failed") == "" {
				t.Fatalf("[%s] redirect CSV sense comptadors: %s", cfg.Label, csvLoc.String())
			}

			logs := buf.String()
			var importLine string
			for _, line := range strings.Split(logs, "\n") {
				if strings.Contains(line, "registre import model=template:baptismes_marcmoia_v2") && strings.Contains(line, "rows=19578") {
					importLine = line
				}
			}
			if importLine == "" {
				t.Fatalf("[%s] no s'ha trobat la línia de log del CSV real", cfg.Label)
			}

			for name, re := range map[string]*regexp.Regexp{
				"rows":                  rowsRe,
				"created":               createdRe,
				"updated":               updatedRe,
				"failed":                failedRe,
				"parse_dur":             parseRe,
				"write_dur":             writeRe,
				"write_duplicate_check": dupRe,
				"write_persona_persist": personaPersistRe,
				"write_links_persist":   linksPersistRe,
				"sidefx_dur":            sidefxDurRe,
				"total_dur":             totalRe,
			} {
				if !re.MatchString(importLine) {
					t.Fatalf("[%s] mètrica %s absent al log real: %s", cfg.Label, name, importLine)
				}
			}

			rowsMatch := rowsRe.FindStringSubmatch(importLine)
			createdMatch := createdRe.FindStringSubmatch(importLine)
			updatedMatch := updatedRe.FindStringSubmatch(importLine)
			failedMatch := failedRe.FindStringSubmatch(importLine)
			if rowsMatch[1] != "19578" || createdMatch[1] != "19578" || updatedMatch[1] != "0" || failedMatch[1] != "0" {
				t.Fatalf("[%s] comptadors inesperats al run real: %s", cfg.Label, importLine)
			}

			t.Logf("[%s] real_csv=%s parse=%s write=%s dup=%s persona=%s links=%s sidefx=%s total=%s",
				cfg.Label,
				filepath.Base(csvPath),
				parseRe.FindStringSubmatch(importLine)[1],
				writeRe.FindStringSubmatch(importLine)[1],
				dupRe.FindStringSubmatch(importLine)[1],
				personaPersistRe.FindStringSubmatch(importLine)[1],
				linksPersistRe.FindStringSubmatch(importLine)[1],
				sidefxDurRe.FindStringSubmatch(importLine)[1],
				totalRe.FindStringSubmatch(importLine)[1],
			)
		})
	}
}
