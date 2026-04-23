package integration

import (
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/cnf"
	"github.com/marcmoiagese/CercaGenealogica/core"
)

func TestTemplateImportRealCSVMetricsSQLitePostgresF3212Fix2(t *testing.T) {
	if os.Getenv("CG_F3212_FIX2_REAL_IMPORT") != "1" {
		t.Skip("validació real F32-12-fix-2 només s'executa explícitament amb CG_F3212_FIX2_REAL_IMPORT=1")
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
	dupRe := regexp.MustCompile(`write_duplicate_check_dur=([^ ]+)`)
	personaPersistRe := regexp.MustCompile(`write_persona_persist_dur=([^ ]+)`)
	sidefxDurRe := regexp.MustCompile(`sidefx_dur=([^ ]+)`)
	sidefxAttrsRe := regexp.MustCompile(`sidefx_load_atributs_dur=([^ ]+)`)
	sidefxComputeRe := regexp.MustCompile(`sidefx_compute_dur=([^ ]+)`)
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
				"csrf_token": "csrf_f3212_fix2_real_territori_" + cfg.Label,
			}, session), app.AdminTerritoriImportRun)
			runMultipartHandler(t, buildMultipartRequestFromFile(t, http.MethodPost, "/admin/eclesiastic/import/run", "import_file", eclesPath, map[string]string{
				"csrf_token": "csrf_f3212_fix2_real_ecles_" + cfg.Label,
			}, session), app.AdminEclesiasticImportRun)
			runMultipartHandler(t, buildMultipartRequestFromFile(t, http.MethodPost, "/admin/arxius/import/run", "import_file", arxiusPath, map[string]string{
				"csrf_token": "csrf_f3212_fix2_real_arxius_" + cfg.Label,
			}, session), app.AdminArxiusImportRun)
			runMultipartHandler(t, buildMultipartRequestFromFile(t, http.MethodPost, "/admin/llibres/import/run", "import_file", llibresPath, map[string]string{
				"csrf_token": "csrf_f3212_fix2_real_llibres_" + cfg.Label,
			}, session), app.AdminLlibresImportRun)

			templateID := mustFindTemplateByName(t, database, "System: Baptismes Marcmoia (v2)")
			csvLoc := runMultipartHandler(t, buildMultipartRequestFromFile(t, http.MethodPost, "/documentals/llibres/importar", "csv_file", csvPath, map[string]string{
				"csrf_token":  "csrf_f3212_fix2_real_csv_" + cfg.Label,
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
				"rows":                     rowsRe,
				"created":                  createdRe,
				"updated":                  updatedRe,
				"failed":                   failedRe,
				"parse_dur":                parseRe,
				"write_duplicate_check":    dupRe,
				"write_persona_persist":    personaPersistRe,
				"sidefx_dur":               sidefxDurRe,
				"sidefx_load_atributs_dur": sidefxAttrsRe,
				"sidefx_compute_dur":       sidefxComputeRe,
				"total_dur":                totalRe,
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

			t.Logf("[%s] real_csv=%s parse=%s dup=%s persona=%s sidefx=%s sidefx_attrs=%s sidefx_compute=%s total=%s",
				cfg.Label,
				filepath.Base(csvPath),
				parseRe.FindStringSubmatch(importLine)[1],
				dupRe.FindStringSubmatch(importLine)[1],
				personaPersistRe.FindStringSubmatch(importLine)[1],
				sidefxDurRe.FindStringSubmatch(importLine)[1],
				sidefxAttrsRe.FindStringSubmatch(importLine)[1],
				sidefxComputeRe.FindStringSubmatch(importLine)[1],
				totalRe.FindStringSubmatch(importLine)[1],
			)
		})
	}
}
