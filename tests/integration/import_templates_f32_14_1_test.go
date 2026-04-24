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
	"time"

	"github.com/marcmoiagese/CercaGenealogica/cnf"
	"github.com/marcmoiagese/CercaGenealogica/core"
)

func TestTemplateImportParseBreakdownMetricsSQLitePostgresF32141(t *testing.T) {
	parseRe := regexp.MustCompile(`parse_dur=([^ ]+)`)
	parseModelRe := regexp.MustCompile(`parse_model_dur=([^ ]+)`)
	parseValidationRe := regexp.MustCompile(`parse_validation_dur=([^ ]+)`)
	parseHeaderReadRe := regexp.MustCompile(`parse_header_read_dur=([^ ]+)`)
	parseHeaderPrepareRe := regexp.MustCompile(`parse_header_prepare_dur=([^ ]+)`)
	parseRowContextRe := regexp.MustCompile(`parse_row_context_dur=([^ ]+)`)
	parseColumnsRe := regexp.MustCompile(`parse_columns_dur=([^ ]+)`)
	parseConditionRe := regexp.MustCompile(`parse_condition_dur=([^ ]+)`)
	parseConditionCallsRe := regexp.MustCompile(`parse_condition_calls=([0-9]+)`)
	parseTransformsRe := regexp.MustCompile(`parse_transforms_dur=([^ ]+)`)
	parseTransformCallsRe := regexp.MustCompile(`parse_transform_calls=([0-9]+)`)
	parseDateRe := regexp.MustCompile(`parse_date_dur=([^ ]+)`)
	parseDateCallsRe := regexp.MustCompile(`parse_date_calls=([0-9]+)`)
	parseQualityRe := regexp.MustCompile(`parse_quality_dur=([^ ]+)`)
	parseQualityCallsRe := regexp.MustCompile(`parse_quality_calls=([0-9]+)`)
	parsePersonBuildRe := regexp.MustCompile(`parse_person_build_dur=([^ ]+)`)
	parsePersonBuildCallsRe := regexp.MustCompile(`parse_person_build_calls=([0-9]+)`)

	for _, cfg := range loadSQLiteAndPostgresConfigsForImportHistory(t) {
		cfg := cfg
		t.Run(cfg.Label, func(t *testing.T) {
			app, database := newTestAppForConfig(t, cfg.Config)
			user, sessionID := createF7UserWithSession(t, database)
			ensureAdminPolicyForUser(t, database, user.ID)
			llibreID, _ := createF7LlibreWithPagina(t, database, user.ID)
			templateID := createF3210Template(t, database, user.ID, "f32141-"+cfg.Label)

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

			rows := []string{"llibre_id,tipus_acte,pagina,cognoms,pare,mare,nascut,acte"}
			for i := 0; i < 240; i++ {
				rows = append(rows, strings.Join([]string{
					strconv.Itoa(llibreID),
					"baptisme",
					"1",
					"Garcia Soler Joan" + strconv.Itoa(i),
					"Pere Garcia" + strconv.Itoa(i%17),
					"Maria Puig" + strconv.Itoa(i%19),
					strconv.Itoa(1+(i%28)) + "/02/1890",
					strconv.Itoa(1+(i%28)) + "/03/1890",
				}, ","))
			}

			req := buildImportGlobalRequest(t, sessionID, "csrf-f32141-"+cfg.Label, map[string]string{
				"model":       "template",
				"template_id": strconv.Itoa(templateID),
				"separator":   ",",
			}, strings.Join(rows, "\n"))
			rr := httptest.NewRecorder()
			app.AdminImportRegistresGlobal(rr, req)
			if rr.Code != http.StatusSeeOther {
				t.Fatalf("[%s] status inesperat: %d body=%s", cfg.Label, rr.Code, rr.Body.String())
			}

			logs := buf.String()
			var importLine string
			for _, line := range strings.Split(logs, "\n") {
				if strings.Contains(line, "registre import model=") && strings.Contains(line, "parse_columns_dur=") {
					importLine = line
				}
			}
			if importLine == "" {
				t.Fatalf("[%s] no s'ha trobat el log d'import amb desglós de parse: %s", cfg.Label, logs)
			}

			required := map[string]*regexp.Regexp{
				"parse_dur":                parseRe,
				"parse_model_dur":          parseModelRe,
				"parse_validation_dur":     parseValidationRe,
				"parse_header_read_dur":    parseHeaderReadRe,
				"parse_header_prepare_dur": parseHeaderPrepareRe,
				"parse_row_context_dur":    parseRowContextRe,
				"parse_columns_dur":        parseColumnsRe,
				"parse_condition_dur":      parseConditionRe,
				"parse_condition_calls":    parseConditionCallsRe,
				"parse_transforms_dur":     parseTransformsRe,
				"parse_transform_calls":    parseTransformCallsRe,
				"parse_date_dur":           parseDateRe,
				"parse_date_calls":         parseDateCallsRe,
				"parse_quality_dur":        parseQualityRe,
				"parse_quality_calls":      parseQualityCallsRe,
				"parse_person_build_dur":   parsePersonBuildRe,
				"parse_person_build_calls": parsePersonBuildCallsRe,
			}
			for name, re := range required {
				if !re.MatchString(importLine) {
					t.Fatalf("[%s] mètrica %s absent al log: %s", cfg.Label, name, importLine)
				}
			}

			parseDur := mustParseDurationMetricF32141(t, cfg.Label, "parse_dur", parseRe, importLine)
			parseColumns := mustParseDurationMetricF32141(t, cfg.Label, "parse_columns_dur", parseColumnsRe, importLine)
			parseRowContext := mustParseDurationMetricF32141(t, cfg.Label, "parse_row_context_dur", parseRowContextRe, importLine)
			parseTransforms := mustParseDurationMetricF32141(t, cfg.Label, "parse_transforms_dur", parseTransformsRe, importLine)
			parseDate := mustParseDurationMetricF32141(t, cfg.Label, "parse_date_dur", parseDateRe, importLine)
			parseQuality := mustParseDurationMetricF32141(t, cfg.Label, "parse_quality_dur", parseQualityRe, importLine)
			parsePerson := mustParseDurationMetricF32141(t, cfg.Label, "parse_person_build_dur", parsePersonBuildRe, importLine)
			parseModel := mustParseDurationMetricF32141(t, cfg.Label, "parse_model_dur", parseModelRe, importLine)
			parseValidation := mustParseDurationMetricF32141(t, cfg.Label, "parse_validation_dur", parseValidationRe, importLine)
			parseHeaderRead := mustParseDurationMetricF32141(t, cfg.Label, "parse_header_read_dur", parseHeaderReadRe, importLine)
			parseHeaderPrepare := mustParseDurationMetricF32141(t, cfg.Label, "parse_header_prepare_dur", parseHeaderPrepareRe, importLine)

			parseTopLevel := parseModel + parseValidation + parseHeaderRead + parseHeaderPrepare + parseRowContext + parseColumns
			if parseDur <= 0 || parseColumns <= 0 || parseRowContext <= 0 {
				t.Fatalf("[%s] durades de parse invàlides: parse=%s row_context=%s columns=%s", cfg.Label, parseDur, parseRowContext, parseColumns)
			}
			if parseTopLevel <= 0 || parseTopLevel > parseDur+2*time.Millisecond {
				t.Fatalf("[%s] el sumatori top-level de parse no és coherent: parse=%s top_level=%s line=%s", cfg.Label, parseDur, parseTopLevel, importLine)
			}
			if mustParseIntMetricF32141(t, cfg.Label, "parse_transform_calls", parseTransformCallsRe, importLine) <= 0 {
				t.Fatalf("[%s] s'esperaven transformacions de parse > 0", cfg.Label)
			}
			if mustParseIntMetricF32141(t, cfg.Label, "parse_date_calls", parseDateCallsRe, importLine) <= 0 {
				t.Fatalf("[%s] s'esperaven parse_date_calls > 0", cfg.Label)
			}
			if mustParseIntMetricF32141(t, cfg.Label, "parse_person_build_calls", parsePersonBuildCallsRe, importLine) <= 0 {
				t.Fatalf("[%s] s'esperaven parse_person_build_calls > 0", cfg.Label)
			}

			t.Logf("[%s] rows=240 parse=%s top_level=%s row_context=%s columns=%s transforms=%s date=%s quality=%s person_build=%s",
				cfg.Label,
				parseDur,
				parseTopLevel,
				parseRowContext,
				parseColumns,
				parseTransforms,
				parseDate,
				parseQuality,
				parsePerson,
			)
		})
	}
}

func TestTemplateImportRealCSVParseBreakdownSQLitePostgresF32141(t *testing.T) {
	if os.Getenv("CG_F3212_FIX2_REAL_IMPORT") != "1" {
		t.Skip("validació real F32-14-1 només s'executa explícitament amb CG_F3212_FIX2_REAL_IMPORT=1")
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

	parseRe := regexp.MustCompile(`parse_dur=([^ ]+)`)
	parseRowContextRe := regexp.MustCompile(`parse_row_context_dur=([^ ]+)`)
	parseColumnsRe := regexp.MustCompile(`parse_columns_dur=([^ ]+)`)
	parseTransformsRe := regexp.MustCompile(`parse_transforms_dur=([^ ]+)`)
	parseDateRe := regexp.MustCompile(`parse_date_dur=([^ ]+)`)
	parseQualityRe := regexp.MustCompile(`parse_quality_dur=([^ ]+)`)
	parsePersonBuildRe := regexp.MustCompile(`parse_person_build_dur=([^ ]+)`)
	resolveRe := regexp.MustCompile(`resolve_dur=([^ ]+)`)
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
				"csrf_token": "csrf_f32141_real_territori_" + cfg.Label,
			}, session), app.AdminTerritoriImportRun)
			runMultipartHandler(t, buildMultipartRequestFromFile(t, http.MethodPost, "/admin/eclesiastic/import/run", "import_file", eclesPath, map[string]string{
				"csrf_token": "csrf_f32141_real_ecles_" + cfg.Label,
			}, session), app.AdminEclesiasticImportRun)
			runMultipartHandler(t, buildMultipartRequestFromFile(t, http.MethodPost, "/admin/arxius/import/run", "import_file", arxiusPath, map[string]string{
				"csrf_token": "csrf_f32141_real_arxius_" + cfg.Label,
			}, session), app.AdminArxiusImportRun)
			runMultipartHandler(t, buildMultipartRequestFromFile(t, http.MethodPost, "/admin/llibres/import/run", "import_file", llibresPath, map[string]string{
				"csrf_token": "csrf_f32141_real_llibres_" + cfg.Label,
			}, session), app.AdminLlibresImportRun)

			templateID := mustFindTemplateByName(t, database, "System: Baptismes Marcmoia (v2)")
			csvLoc := runMultipartHandler(t, buildMultipartRequestFromFile(t, http.MethodPost, "/documentals/llibres/importar", "csv_file", csvPath, map[string]string{
				"csrf_token":  "csrf_f32141_real_csv_" + cfg.Label,
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
				t.Fatalf("[%s] no s'ha trobat la línia de log del CSV real", cfg.Label)
			}

			for name, re := range map[string]*regexp.Regexp{
				"parse_dur":              parseRe,
				"parse_row_context_dur":  parseRowContextRe,
				"parse_columns_dur":      parseColumnsRe,
				"parse_transforms_dur":   parseTransformsRe,
				"parse_date_dur":         parseDateRe,
				"parse_quality_dur":      parseQualityRe,
				"parse_person_build_dur": parsePersonBuildRe,
				"resolve_dur":            resolveRe,
				"total_dur":              totalRe,
			} {
				if !re.MatchString(importLine) {
					t.Fatalf("[%s] mètrica %s absent al log real: %s", cfg.Label, name, importLine)
				}
			}

			t.Logf("[%s] real_csv=%s parse=%s parse_row_context=%s parse_columns=%s parse_transforms=%s parse_date=%s parse_quality=%s parse_person_build=%s resolve=%s total=%s",
				cfg.Label,
				filepath.Base(csvPath),
				parseRe.FindStringSubmatch(importLine)[1],
				parseRowContextRe.FindStringSubmatch(importLine)[1],
				parseColumnsRe.FindStringSubmatch(importLine)[1],
				parseTransformsRe.FindStringSubmatch(importLine)[1],
				parseDateRe.FindStringSubmatch(importLine)[1],
				parseQualityRe.FindStringSubmatch(importLine)[1],
				parsePersonBuildRe.FindStringSubmatch(importLine)[1],
				resolveRe.FindStringSubmatch(importLine)[1],
				totalRe.FindStringSubmatch(importLine)[1],
			)
		})
	}
}

func mustParseDurationMetricF32141(t *testing.T, label, name string, re *regexp.Regexp, line string) time.Duration {
	t.Helper()
	match := re.FindStringSubmatch(line)
	if len(match) != 2 {
		t.Fatalf("[%s] no s'ha pogut parsejar %s a: %s", label, name, line)
	}
	dur, err := time.ParseDuration(match[1])
	if err != nil {
		t.Fatalf("[%s] %s invàlid (%q): %v", label, name, match[1], err)
	}
	return dur
}

func mustParseIntMetricF32141(t *testing.T, label, name string, re *regexp.Regexp, line string) int {
	t.Helper()
	match := re.FindStringSubmatch(line)
	if len(match) != 2 {
		t.Fatalf("[%s] no s'ha pogut parsejar %s a: %s", label, name, line)
	}
	n, err := strconv.Atoi(match[1])
	if err != nil {
		t.Fatalf("[%s] %s invàlid (%q): %v", label, name, match[1], err)
	}
	return n
}
