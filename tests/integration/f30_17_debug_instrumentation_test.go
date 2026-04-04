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
				if !strings.Contains(logs, "moderacio bulk registre chunk=") {
					t.Fatalf("amb debug esperava log de chunk bulk registre, però no hi és: %s", logs)
				}
				if !strings.Contains(logs, "moderacio bulk worker history") {
					t.Fatalf("amb debug esperava log d'historial bulk, però no hi és: %s", logs)
				}
			} else {
				if strings.Contains(logs, "moderacio bulk registre chunk=") || strings.Contains(logs, "moderacio bulk worker history") {
					t.Fatalf("amb info no haurien d'aparèixer logs detallats bulk registre: %s", logs)
				}
			}
		})
	}
}
