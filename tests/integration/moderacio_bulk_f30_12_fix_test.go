package integration

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
	testcommon "github.com/marcmoiagese/CercaGenealogica/tests/common"
)

func newBatchingFixDBs(t *testing.T) []appDB {
	t.Helper()

	configs := testcommon.LoadTestDBConfigs(t)
	projectRoot := findProjectRoot(t)
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatalf("no puc fer chdir a l'arrel del projecte (%s): %v", projectRoot, err)
	}

	result := make([]appDB, 0, len(configs))
	for _, c := range configs {
		cfg := map[string]string{}
		for k, v := range c.Config {
			cfg[k] = v
		}
		if _, ok := cfg["LOG_LEVEL"]; !ok {
			cfg["LOG_LEVEL"] = "silent"
		}
		dbInstance, err := db.NewDB(cfg)
		if err != nil {
			t.Logf("skip db %s per inicialitzacio externa: %v", c.Label, err)
			continue
		}
		app := core.NewApp(cfg, dbInstance)
		t.Cleanup(func() {
			app.Close()
		})
		result = append(result, appDB{
			Label: c.Label,
			App:   app,
			DB:    dbInstance,
		})
	}
	if len(result) == 0 {
		t.Fatalf("cap base de dades disponible per provar el fix")
	}
	return result
}

func createAdminJobForTargetsTest(t *testing.T, database db.DB) int {
	t.Helper()

	jobID, err := database.CreateAdminJob(&db.AdminJob{
		Kind:          "moderacio_bulk",
		Status:        "queued",
		Phase:         "queued",
		ProgressTotal: 1,
		ProgressDone:  0,
		PayloadJSON:   `{"scope":"all"}`,
		CreatedBy:     sql.NullInt64{},
	})
	if err != nil {
		t.Fatalf("CreateAdminJob ha fallat: %v", err)
	}
	if jobID <= 0 {
		t.Fatalf("jobID invàlid: %d", jobID)
	}
	return jobID
}

func countAdminJobTargetsForTest(t *testing.T, database db.DB, jobID int) int {
	t.Helper()

	targets, err := database.ListAdminJobTargets(jobID)
	if err != nil {
		t.Fatalf("ListAdminJobTargets ha fallat: %v", err)
	}
	return len(targets)
}

func TestAdminJobTargetsPersistLargeSnapshotInBatches(t *testing.T) {
	apps := newBatchingFixDBs(t)

	for _, env := range apps {
		env := env
		t.Run(env.Label, func(t *testing.T) {
			jobID := createAdminJobForTargetsTest(t, env.DB)
			const totalTargets = 20000
			targets := make([]db.AdminJobTarget, 0, totalTargets)
			for i := 1; i <= totalTargets; i++ {
				targets = append(targets, db.AdminJobTarget{
					SeqNum:     i,
					ObjectType: "arxiu",
					ObjectID:   i,
				})
			}
			if err := env.DB.CreateAdminJobTargets(jobID, targets); err != nil {
				t.Fatalf("CreateAdminJobTargets ha fallat: %v", err)
			}

			persisted, err := env.DB.ListAdminJobTargets(jobID)
			if err != nil {
				t.Fatalf("ListAdminJobTargets ha fallat: %v", err)
			}
			if len(persisted) != totalTargets {
				t.Fatalf("targets persistits inesperats: got %d want %d", len(persisted), totalTargets)
			}
			if persisted[0].SeqNum != 1 {
				t.Fatalf("min seq inesperat: got %d want 1", persisted[0].SeqNum)
			}
			if persisted[len(persisted)-1].SeqNum != totalTargets {
				t.Fatalf("max seq inesperat: got %d want %d", persisted[len(persisted)-1].SeqNum, totalTargets)
			}
		})
	}
}

func TestAdminJobTargetsBatchInsertRollsBackOnInvalidTarget(t *testing.T) {
	apps := newBatchingFixDBs(t)

	for _, env := range apps {
		env := env
		t.Run(env.Label, func(t *testing.T) {
			jobID := createAdminJobForTargetsTest(t, env.DB)
			targets := make([]db.AdminJobTarget, 0, 1001)
			for i := 1; i <= 1000; i++ {
				targets = append(targets, db.AdminJobTarget{
					SeqNum:     i,
					ObjectType: "persona",
					ObjectID:   i,
				})
			}
			targets = append(targets, db.AdminJobTarget{
				SeqNum:     1001,
				ObjectType: "",
				ObjectID:   1001,
			})

			err := env.DB.CreateAdminJobTargets(jobID, targets)
			if err == nil {
				t.Fatalf("s'esperava error per target invàlid")
			}
			if got := countAdminJobTargetsForTest(t, env.DB, jobID); got != 0 {
				t.Fatalf("no hi hauria d'haver inserts parcials; got %d", got)
			}
		})
	}
}

func TestModeracioBulkAsyncLargeSnapshotCreatesJob(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_12_fix_bulk_large_snapshot.sqlite3")

	admin := createTestUser(t, database, "admin_bulk_f30_12_fix")
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_bulk_f30_12_fix")

	const totalArxius = 1205
	for i := 1; i <= totalArxius; i++ {
		if _, err := database.CreateArxiu(&db.Arxiu{
			Nom:            fmt.Sprintf("Arxiu bulk fix %04d", i),
			Tipus:          "Municipal",
			ModeracioEstat: "pendent",
		}); err != nil {
			t.Fatalf("CreateArxiu %d ha fallat: %v", i, err)
		}
	}

	csrf := "csrf_bulk_f30_12_fix"
	form := newFormValues(map[string]string{
		"bulk_action": "approve",
		"bulk_scope":  "all",
		"bulk_type":   "arxiu",
		"csrf_token":  csrf,
		"return_to":   "/moderacio",
		"async":       "1",
	})
	req := httptest.NewRequest(http.MethodPost, "/moderacio/bulk", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.AddCookie(session)
	req.AddCookie(csrfCookie(csrf))
	rr := httptest.NewRecorder()
	app.AdminModeracioBulk(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("bulk async esperava 200, got %d body=%s", rr.Code, rr.Body.String())
	}

	var payload struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("response JSON invàlid: %v", err)
	}
	jobID, err := strconv.Atoi(payload.JobID)
	if err != nil || jobID <= 0 {
		t.Fatalf("job_id invàlid: %q err=%v", payload.JobID, err)
	}
	if got := countAdminJobTargetsForTest(t, database, jobID); got != totalArxius {
		t.Fatalf("snapshot persistit inesperat: got %d want %d", got, totalArxius)
	}

	job := waitForAdminJobTerminal(t, database, jobID)
	if job.Status != "done" {
		t.Fatalf("job esperat done, got status=%s phase=%s error=%s", job.Status, job.Phase, job.ErrorText)
	}

	var result struct {
		Targets int    `json:"targets"`
		Updated int    `json:"updated"`
		Errors  int    `json:"errors"`
		Phase   string `json:"phase"`
	}
	if err := json.Unmarshal([]byte(job.ResultJSON), &result); err != nil {
		t.Fatalf("result_json invàlid: %v", err)
	}
	if result.Targets != totalArxius || result.Updated != totalArxius || result.Errors != 0 || result.Phase != "done" {
		t.Fatalf("result inesperat: %+v", result)
	}
}
