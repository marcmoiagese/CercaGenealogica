package integration

import (
	"bytes"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/cnf"
	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func newSQLiteAppWithLogLevel(t *testing.T, dbName, level string) (*core.App, db.DB) {
	t.Helper()

	cfg := map[string]string{
		"DB_ENGINE": "sqlite",
		"DB_PATH":   filepath.Join(t.TempDir(), dbName),
		"RECREADB":  "true",
		"LOG_LEVEL": level,
	}
	return newTestAppForConfig(t, cfg)
}

func captureStandardLog(t *testing.T) (*bytes.Buffer, func()) {
	t.Helper()

	var buf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&buf)
	return &buf, func() {
		log.SetOutput(prev)
	}
}

func TestRegistreImportDebugInstrumentationRespectsLogLevelF3017(t *testing.T) {
	for _, level := range []string{"info", "debug"} {
		level := level
		t.Run(level, func(t *testing.T) {
			app, database := newSQLiteAppWithLogLevel(t, "test_f30_17_import_logs.sqlite3", level)
			user, sessionID := createF7UserWithSession(t, database)
			ensureAdminPolicyForUser(t, database, user.ID)
			llibreID, _ := createF7LlibreWithPagina(t, database, user.ID)

			prevLevel := "error"
			if strings.EqualFold(level, "debug") {
				prevLevel = "debug"
			} else {
				prevLevel = "info"
			}
			core.SetLogLevel(prevLevel)
			defer core.SetLogLevel("error")

			buf, restore := captureStandardLog(t)
			defer restore()

			req := buildImportGlobalRequest(t, sessionID, "csrf_f30_17_import_"+level, map[string]string{
				"model":     "generic",
				"separator": ",",
			}, strings.Join([]string{
				"llibre_id,tipus_acte",
				strconv.Itoa(llibreID) + ",baptisme",
			}, "\n"))
			rr := httptest.NewRecorder()
			app.AdminImportRegistresGlobal(rr, req)
			if rr.Code != http.StatusSeeOther {
				t.Fatalf("import generic esperava 303, got %d", rr.Code)
			}

			logs := buf.String()
			if strings.EqualFold(level, "debug") {
				if !strings.Contains(logs, "registre import model=generic scope=global") {
					t.Fatalf("amb debug esperava log d'import, però no hi és: %s", logs)
				}
			} else if strings.Contains(logs, "registre import model=") {
				t.Fatalf("amb info no haurien d'aparèixer logs detallats d'import: %s", logs)
			}
		})
	}
}

func TestModeracioBulkRegistreDebugInstrumentationRespectsLogLevelF3017(t *testing.T) {
	for _, level := range []string{"info", "debug"} {
		level := level
		t.Run(level, func(t *testing.T) {
			app, database := newSQLiteAppWithLogLevel(t, "test_f30_17_bulk_logs.sqlite3", level)
			admin, _ := createF7UserWithSession(t, database)
			ensureAdminPolicyForUser(t, database, admin.ID)
			session := createSessionCookie(t, database, admin.ID, "sess_f30_17_bulk_"+level)

			llibreID, paginaID := createF7LlibreWithPagina(t, database, admin.ID)
			for i := 0; i < 2; i++ {
				createDemografiaRegistre(t, database, llibreID, paginaID, admin.ID, "baptisme", 1901+i, "pendent")
			}

			core.SetLogLevel(level)
			defer core.SetLogLevel("error")

			buf, restore := captureStandardLog(t)
			defer restore()

			jobID := submitAsyncRegistreBulkJob(t, app, session, "csrf_f30_17_bulk_"+level)
			job := waitForAdminJobTerminal(t, database, jobID)
			if job.Status != "done" {
				t.Fatalf("job bulk registre esperat done, got status=%s phase=%s", job.Status, job.Phase)
			}

			logs := buf.String()
			if strings.EqualFold(level, "debug") {
				if !strings.Contains(logs, "type=registre branch=registre_special apply=applyModeracioBulkRegistreUpdates") {
					t.Fatalf("amb debug esperava log de dispatch especial registre, però no hi és: %s", logs)
				}
				if !strings.Contains(logs, "[ModeracioBulkWorker] chunk=registre") || !strings.Contains(logs, "derived_dur=") || !strings.Contains(logs, "derived_stats_dur=") || !strings.Contains(logs, "derived_search_dur=") {
					t.Fatalf("amb debug esperava log resumit update/derived del chunk registre, però no hi és: %s", logs)
				}
				if !strings.Contains(logs, "moderacio bulk registre chunk plan=") {
					t.Fatalf("amb debug esperava log de plan de chunk registre, però no hi és: %s", logs)
				}
				if !strings.Contains(logs, "moderacio bulk registre chunk=") {
					t.Fatalf("amb debug esperava log de chunk bulk registre, però no hi és: %s", logs)
				}
				if !strings.Contains(logs, "moderacio bulk registre stats aggregate") || !strings.Contains(logs, "apply=bulk") {
					t.Fatalf("amb debug esperava log agregat de stats bulk registre, però no hi és: %s", logs)
				}
				if !strings.Contains(logs, "moderacio bulk registre search aggregate") {
					t.Fatalf("amb debug esperava log agregat de search bulk registre, però no hi és: %s", logs)
				}
				if !strings.Contains(logs, "search_job_cache_warmup_dur=") || !strings.Contains(logs, "search_job_cache_warmup_build_dur=") || !strings.Contains(logs, "search_job_cache_warmup_docs=") || !strings.Contains(logs, "derived_search_build_dur=") || !strings.Contains(logs, "derived_search_upsert_dur=") || !strings.Contains(logs, "search_cache_hits=") {
					t.Fatalf("amb debug esperava mètriques F31-3 de search bulk registre, però no hi són: %s", logs)
				}
				if !strings.Contains(logs, "derived_stats_prepare_dur=") || !strings.Contains(logs, "derived_stats_prepare_contrib_dur=") || !strings.Contains(logs, "derived_stats_prepare_nivells_dur=") || !strings.Contains(logs, "derived_stats_ensure_dur=") || !strings.Contains(logs, "derived_stats_apply_dur=") || !strings.Contains(logs, "derived_stats_delta_rows=") {
					t.Fatalf("amb debug esperava mètriques F31-5 de stats bulk registre, però no hi són: %s", logs)
				}
				if !strings.Contains(logs, "derived_stats_municipis=") || !strings.Contains(logs, "derived_stats_negative_rows=") || !strings.Contains(logs, "search_doc_cache_hits=") {
					t.Fatalf("amb debug esperava mètriques F31-6 de stats/search bulk registre, però no hi són: %s", logs)
				}
				if !strings.Contains(logs, "moderacio bulk worker history") {
					t.Fatalf("amb debug esperava log d'historial bulk, però no hi és: %s", logs)
				}
			} else {
				if strings.Contains(logs, "moderacio bulk worker dispatch") || strings.Contains(logs, "[ModeracioBulkWorker] chunk=registre") || strings.Contains(logs, "moderacio bulk registre chunk plan=") || strings.Contains(logs, "moderacio bulk registre chunk=") || strings.Contains(logs, "moderacio bulk registre stats aggregate") || strings.Contains(logs, "moderacio bulk registre search aggregate") || strings.Contains(logs, "moderacio bulk worker history") {
					t.Fatalf("amb info no haurien d'aparèixer logs detallats bulk registre: %s", logs)
				}
			}
		})
	}
}

func TestModeracioBulkWorkerDispatchAuditLogsF3017Fix1(t *testing.T) {
	app, database := newSQLiteAppWithLogLevel(t, "test_f30_17_fix_1_dispatch.sqlite3", "debug")
	admin, _ := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, admin.ID)
	session := createSessionCookie(t, database, admin.ID, "sess_f30_17_fix_1_dispatch")

	llibreID, paginaID := createF7LlibreWithPagina(t, database, admin.ID)
	llibre, err := database.GetLlibre(llibreID)
	if err != nil || llibre == nil {
		t.Fatalf("GetLlibre ha fallat: %v", err)
	}
	createPendingArxiu(t, database, admin.ID, llibre.MunicipiID, "Arxiu F30-17-fix-1")
	createDemografiaRegistre(t, database, llibreID, paginaID, admin.ID, "baptisme", 1901, "pendent")

	core.SetLogLevel("debug")
	defer core.SetLogLevel("error")

	buf, restore := captureStandardLog(t)
	defer restore()

	arxiuJobID := submitAsyncBulkJobByType(t, app, session, "csrf_f30_17_fix_1_arxiu", "arxiu")
	arxiuJob := waitForAdminJobTerminal(t, database, arxiuJobID)
	if arxiuJob.Status != "done" {
		t.Fatalf("job arxiu esperat done, got status=%s phase=%s", arxiuJob.Status, arxiuJob.Phase)
	}

	registreJobID := submitAsyncBulkJobByType(t, app, session, "csrf_f30_17_fix_1_registre", "registre")
	registreJob := waitForAdminJobTerminal(t, database, registreJobID)
	if registreJob.Status != "done" {
		t.Fatalf("job registre esperat done, got status=%s phase=%s", registreJob.Status, registreJob.Phase)
	}

	logs := buf.String()
	if !strings.Contains(logs, "type=arxiu branch=bulk_simple apply=BulkUpdateModeracioSimple chunking=false") {
		t.Fatalf("esperava log de dispatch bulk_simple per arxiu, però no hi és: %s", logs)
	}
	if !strings.Contains(logs, "type=registre branch=registre_special apply=applyModeracioBulkRegistreUpdates chunking=true chunk_size=500") {
		t.Fatalf("esperava log de dispatch registre_special, però no hi és: %s", logs)
	}
	if !strings.Contains(logs, "moderacio bulk registre chunk plan=") || !strings.Contains(logs, "demo_groups=") || !strings.Contains(logs, "batch_reads=4") {
		t.Fatalf("esperava log de plan del chunk de registre amb comptadors interns, però no hi és: %s", logs)
	}
}

func TestModeracioBulkRegistreLargeChunkSummaryF311(t *testing.T) {
	app, database := newSQLiteAppWithLogLevel(t, "test_f31_1_bulk_summary.sqlite3", "debug")
	admin, _ := createF7UserWithSession(t, database)
	ensureAdminPolicyForUser(t, database, admin.ID)
	session := createSessionCookie(t, database, admin.ID, "sess_f31_1_bulk_summary_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	llibreID, paginaID := createF7LlibreWithPagina(t, database, admin.ID)
	for i := 0; i < 520; i++ {
		registreID := createDemografiaRegistre(t, database, llibreID, paginaID, admin.ID, "baptisme", 1900+(i%3), "pendent")
		if _, err := database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
			TranscripcioID: registreID,
			Rol:            "batejat",
			Nom:            "Joan",
			Cognom1:        "Pujol",
		}); err != nil {
			t.Fatalf("CreateTranscripcioPersona ha fallat: %v", err)
		}
	}

	core.SetLogLevel("debug")
	defer core.SetLogLevel("error")

	buf, restore := captureStandardLog(t)
	defer restore()

	jobID := submitAsyncRegistreBulkJob(t, app, session, "csrf_f31_1_bulk_summary")
	job := waitForAdminJobTerminal(t, database, jobID)
	if job.Status != "done" {
		t.Fatalf("job bulk registre esperat done, got status=%s phase=%s error=%s", job.Status, job.Phase, job.ErrorText)
	}

	logs := buf.String()
	lines := []string{}
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, "[ModeracioBulkWorker] chunk=registre") {
			lines = append(lines, strings.TrimSpace(line))
		}
	}
	if len(lines) == 0 {
		t.Fatalf("no s'ha trobat cap resum de chunk registre: %s", logs)
	}
	summary := strings.Join(lines, " || ")
	if !strings.Contains(summary, "derived_stats_dur=") || !strings.Contains(summary, "derived_stats_prepare_dur=") || !strings.Contains(summary, "derived_stats_prepare_contrib_dur=") || !strings.Contains(summary, "derived_stats_prepare_nivells_dur=") || !strings.Contains(summary, "derived_stats_apply_dur=") || !strings.Contains(summary, "derived_stats_municipis=") || !strings.Contains(summary, "derived_search_dur=") || !strings.Contains(summary, "search_job_cache_warmup_build_dur=") || !strings.Contains(summary, "search_job_cache_warmup_docs=") || !strings.Contains(summary, "derived_search_build_dur=") || !strings.Contains(summary, "search_doc_cache_hits=") {
		t.Fatalf("resum de chunk sense mètriques derivades completes: %s", summary)
	}
	t.Log(summary)
}

func TestModeracioBulkRegistreMetricsAndStatsPersistMultiDBF316(t *testing.T) {
	forEachModeracioBulkHistoryDB(t, func(t *testing.T, label string, app *core.App, database db.DB, engine string) {
		admin, _ := createF7UserWithSession(t, database)
		ensureAdminPolicyForUser(t, database, admin.ID)
		session := createSessionCookie(t, database, admin.ID, "sess_f31_6_metrics_"+label+"_"+strconv.FormatInt(time.Now().UnixNano(), 10))

		llibreID, paginaID := createF7LlibreWithPagina(t, database, admin.ID)
		for i := 0; i < 80; i++ {
			registreID := createDemografiaRegistre(t, database, llibreID, paginaID, admin.ID, "baptisme", 1900+(i%4), "pendent")
			if _, err := database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
				TranscripcioID: registreID,
				Rol:            "batejat",
				Nom:            "Joan",
				Cognom1:        "Pujol" + strconv.Itoa(i%7),
			}); err != nil {
				t.Fatalf("CreateTranscripcioPersona ha fallat: %v", err)
			}
		}

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

		jobID := submitAsyncRegistreBulkJob(t, app, session, "csrf_f31_6_metrics_"+label)
		job := waitForAdminJobTerminal(t, database, jobID)
		if job.Status != "done" {
			t.Fatalf("job bulk registre esperat done, got status=%s phase=%s error=%s", job.Status, job.Phase, job.ErrorText)
		}

		logs := buf.String()
		lines := []string{}
		for _, line := range strings.Split(logs, "\n") {
			if strings.Contains(line, "[ModeracioBulkWorker] chunk=registre") {
				lines = append(lines, strings.TrimSpace(line))
			}
		}
		if len(lines) == 0 {
			t.Fatalf("%s: no s'ha trobat cap resum de chunk registre: %s", engine, logs)
		}
		summary := strings.Join(lines, " || ")
		for _, token := range []string{
			"derived_stats_dur=",
			"derived_stats_prepare_dur=",
			"derived_stats_prepare_contrib_dur=",
			"derived_stats_prepare_nivells_dur=",
			"derived_stats_apply_dur=",
			"derived_stats_municipis=",
			"derived_stats_nivells=",
			"derived_stats_negative_rows=",
			"derived_search_dur=",
			"search_job_cache_warmup_build_dur=",
			"search_job_cache_warmup_docs=80",
			"derived_search_build_dur=",
			"derived_search_upsert_dur=",
			"search_doc_cache_hits=80",
			"search_doc_cache_misses=0",
		} {
			if !strings.Contains(summary, token) {
				t.Fatalf("%s: resum de chunk sense token %q: %s", engine, token, summary)
			}
		}
		for _, token := range []string{
			"nom_cognom stats bulk persist",
			"total_batches=",
			"negative_rows=0",
			"noms_freq_municipi_any",
			"cognoms_freq_nivell_total",
		} {
			if !strings.Contains(logs, token) {
				t.Fatalf("%s: log de persistència stats bulk sense token %q: %s", engine, token, logs)
			}
		}
		t.Logf("%s: %s", engine, summary)
	})
}
