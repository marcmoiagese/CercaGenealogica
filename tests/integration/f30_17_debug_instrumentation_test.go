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
				if !strings.Contains(logs, "moderacio bulk registre chunk plan=") {
					t.Fatalf("amb debug esperava log de plan de chunk registre, però no hi és: %s", logs)
				}
				if !strings.Contains(logs, "moderacio bulk registre chunk=") {
					t.Fatalf("amb debug esperava log de chunk bulk registre, però no hi és: %s", logs)
				}
				if !strings.Contains(logs, "moderacio bulk worker history") {
					t.Fatalf("amb debug esperava log d'historial bulk, però no hi és: %s", logs)
				}
			} else {
				if strings.Contains(logs, "moderacio bulk worker dispatch") || strings.Contains(logs, "moderacio bulk registre chunk plan=") || strings.Contains(logs, "moderacio bulk registre chunk=") || strings.Contains(logs, "moderacio bulk worker history") {
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
