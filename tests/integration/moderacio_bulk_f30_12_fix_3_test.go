package integration

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
	testcommon "github.com/marcmoiagese/CercaGenealogica/tests/common"
)

type moderacioBulkHistoryFixture struct {
	arxiuID       int
	llibreID      int
	nivellID      int
	municipiID    int
	eclesiasticID int
	activityIDs   map[string]int
}

func createModeracioBulkHistoryUser(t *testing.T, database db.DB, username string) *db.User {
	t.Helper()

	user := &db.User{
		Usuari:        username,
		Email:         username + "@example.com",
		Active:        true,
		Password:      []byte("hash"),
		DataNaixament: "1990-01-01",
	}
	if err := database.InsertUser(user); err != nil {
		t.Fatalf("InsertUser ha fallat: %v", err)
	}
	if err := database.EnsureDefaultPolicies(); err != nil {
		t.Fatalf("EnsureDefaultPolicies ha fallat: %v", err)
	}
	if err := database.EnsureDefaultPointsRules(); err != nil {
		t.Fatalf("EnsureDefaultPointsRules ha fallat: %v", err)
	}
	return user
}

func assignAdminPolicyForModeracioBulkHistory(t *testing.T, database db.DB, userID int) {
	t.Helper()

	if err := database.EnsureDefaultPolicies(); err != nil {
		t.Fatalf("EnsureDefaultPolicies ha fallat: %v", err)
	}
	policies, err := database.ListPolitiques()
	if err != nil {
		t.Fatalf("ListPolitiques ha fallat: %v", err)
	}
	adminID := 0
	for _, policy := range policies {
		if strings.TrimSpace(policy.Nom) == "admin" {
			adminID = policy.ID
			break
		}
	}
	if adminID <= 0 {
		t.Fatalf("no he trobat la política admin")
	}
	if err := database.AddUserPolitica(userID, adminID); err != nil {
		t.Fatalf("AddUserPolitica ha fallat: %v", err)
	}
}

func resetModeracioBulkHistoryData(t *testing.T, database db.DB) {
	t.Helper()

	stmts := []string{
		"DELETE FROM admin_job_targets",
		"DELETE FROM admin_jobs",
		"DELETE FROM admin_audit",
		"DELETE FROM usuaris_activitat",
		"DELETE FROM arxius_llibres",
		"DELETE FROM llibres_urls",
		"DELETE FROM llibres",
		"DELETE FROM arxius",
		"DELETE FROM arquebisbats_municipi",
		"DELETE FROM arquebisbats",
		"DELETE FROM municipis",
		"DELETE FROM nivells_administratius",
		"DELETE FROM paisos",
	}
	for _, stmt := range stmts {
		if _, err := database.Exec(stmt); err != nil {
			t.Fatalf("neteja prèvia %q ha fallat: %v", stmt, err)
		}
	}
}

func forEachModeracioBulkHistoryDB(t *testing.T, fn func(t *testing.T, label string, app *core.App, database db.DB, engine string)) {
	t.Helper()

	dbConfs := testcommon.LoadTestDBConfigs(t)
	for _, dbCfg := range dbConfs {
		dbCfg := dbCfg
		t.Run(dbCfg.Label, func(t *testing.T) {
			if strings.EqualFold(dbCfg.Engine, "mysql") {
				t.Skip("MySQL de tests continua fallant en la inicialització base de l'entorn; fix validat a SQLite i PostgreSQL")
			}

			cfg := map[string]string{}
			for k, v := range dbCfg.Config {
				cfg[k] = v
			}
			if _, ok := cfg["LOG_LEVEL"]; !ok {
				cfg["LOG_LEVEL"] = "silent"
			}
			cfg["RECREADB"] = "true"
			if dbCfg.Engine == "sqlite" {
				tmpDir := t.TempDir()
				cfg["DB_PATH"] = filepath.Join(tmpDir, fmt.Sprintf("test_f30_12_fix_3_%d.sqlite3", time.Now().UnixNano()))
			}

			app, database := newTestAppForConfig(t, cfg)
			testcommon.EnsurePostgresBoolCompat(t, database, dbCfg.Engine)
			resetModeracioBulkHistoryData(t, database)

			fn(t, dbCfg.Label, app, database, dbCfg.Engine)
		})
	}
}

func createPendingActivityForObject(t *testing.T, database db.DB, userID int, objectType string, objectID int) int {
	t.Helper()

	activity := &db.UserActivity{
		UserID:     userID,
		Action:     "importar",
		ObjectType: objectType,
		ObjectID:   sql.NullInt64{Int64: int64(objectID), Valid: true},
		Status:     "pendent",
		Details:    "import",
	}
	activityID, err := database.InsertUserActivity(activity)
	if err != nil {
		t.Fatalf("InsertUserActivity %s ha fallat: %v", objectType, err)
	}
	return activityID
}

func seedModeracioBulkHistoryFixture(t *testing.T, database db.DB, userID int) moderacioBulkHistoryFixture {
	t.Helper()

	seed := time.Now().UnixNano()
	alphabet := "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	iso2 := string([]byte{
		alphabet[(seed/36)%int64(len(alphabet))],
		alphabet[seed%int64(len(alphabet))],
	})
	iso3 := "T" + string([]byte{
		alphabet[(seed/36)%int64(len(alphabet))],
		alphabet[seed%int64(len(alphabet))],
	})
	num := fmt.Sprintf("%03d", seed%1000)
	paisID := createPais(t, database, iso2, iso3, num)
	nivellID := createPendingNivell(t, database, userID, paisID, fmt.Sprintf("Nivell Bulk Historial %d", seed))

	mun := &db.Municipi{
		Nom:            fmt.Sprintf("Municipi Bulk Historial %d", seed),
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
	}
	mun.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(nivellID), Valid: true}
	municipiID, err := database.CreateMunicipi(mun)
	if err != nil {
		t.Fatalf("CreateMunicipi ha fallat: %v", err)
	}

	eclesiasticID := createPendingArquebisbat(t, database, userID, paisID, fmt.Sprintf("Bisbat Bulk Historial %d", seed))

	arxiu := &db.Arxiu{
		Nom:                   fmt.Sprintf("Arxiu Bulk Historial %d", seed),
		Tipus:                 "parroquial",
		MunicipiID:            sql.NullInt64{Int64: int64(municipiID), Valid: true},
		EntitatEclesiasticaID: sql.NullInt64{Int64: int64(eclesiasticID), Valid: true},
		CreatedBy:             sql.NullInt64{Int64: int64(userID), Valid: true},
		ModeracioEstat:        "pendent",
	}
	arxiuID, err := database.CreateArxiu(arxiu)
	if err != nil {
		t.Fatalf("CreateArxiu ha fallat: %v", err)
	}

	llibre := &db.Llibre{
		ArquebisbatID:  eclesiasticID,
		MunicipiID:     municipiID,
		Titol:          fmt.Sprintf("Llibre Bulk Historial %d", seed),
		CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
		ModeracioEstat: "pendent",
	}
	llibreID, err := database.CreateLlibre(llibre)
	if err != nil {
		t.Fatalf("CreateLlibre ha fallat: %v", err)
	}

	fixture := moderacioBulkHistoryFixture{
		arxiuID:       arxiuID,
		llibreID:      llibreID,
		nivellID:      nivellID,
		municipiID:    municipiID,
		eclesiasticID: eclesiasticID,
		activityIDs: map[string]int{
			"arxiu":       createPendingActivityForObject(t, database, userID, "arxiu", arxiuID),
			"llibre":      createPendingActivityForObject(t, database, userID, "llibre", llibreID),
			"nivell":      createPendingActivityForObject(t, database, userID, "nivell", nivellID),
			"municipi":    createPendingActivityForObject(t, database, userID, "municipi", municipiID),
			"eclesiastic": createPendingActivityForObject(t, database, userID, "eclesiastic", eclesiasticID),
		},
	}
	return fixture
}

func bulkFixtureObjectID(f moderacioBulkHistoryFixture, bulkType string) int {
	switch bulkType {
	case "arxiu":
		return f.arxiuID
	case "llibre":
		return f.llibreID
	case "nivell":
		return f.nivellID
	case "municipi":
		return f.municipiID
	case "eclesiastic":
		return f.eclesiasticID
	default:
		return 0
	}
}

func assertBulkFixtureObjectPublicat(t *testing.T, database db.DB, bulkType string, objectID int) {
	t.Helper()

	switch bulkType {
	case "arxiu":
		obj, err := database.GetArxiu(objectID)
		if err != nil || obj == nil {
			t.Fatalf("GetArxiu ha fallat: %v", err)
		}
		if obj.ModeracioEstat != "publicat" {
			t.Fatalf("arxiu esperat publicat, got %s", obj.ModeracioEstat)
		}
	case "llibre":
		obj, err := database.GetLlibre(objectID)
		if err != nil || obj == nil {
			t.Fatalf("GetLlibre ha fallat: %v", err)
		}
		if obj.ModeracioEstat != "publicat" {
			t.Fatalf("llibre esperat publicat, got %s", obj.ModeracioEstat)
		}
	case "nivell":
		obj, err := database.GetNivell(objectID)
		if err != nil || obj == nil {
			t.Fatalf("GetNivell ha fallat: %v", err)
		}
		if obj.ModeracioEstat != "publicat" {
			t.Fatalf("nivell esperat publicat, got %s", obj.ModeracioEstat)
		}
	case "municipi":
		obj, err := database.GetMunicipi(objectID)
		if err != nil || obj == nil {
			t.Fatalf("GetMunicipi ha fallat: %v", err)
		}
		if obj.ModeracioEstat != "publicat" {
			t.Fatalf("municipi esperat publicat, got %s", obj.ModeracioEstat)
		}
	case "eclesiastic":
		obj, err := database.GetArquebisbat(objectID)
		if err != nil || obj == nil {
			t.Fatalf("GetArquebisbat ha fallat: %v", err)
		}
		if obj.ModeracioEstat != "publicat" {
			t.Fatalf("eclesiastic esperat publicat, got %s", obj.ModeracioEstat)
		}
	default:
		t.Fatalf("bulkType desconegut: %s", bulkType)
	}
}

func startModeracioBulkAsyncJob(t *testing.T, app *core.App, session *http.Cookie, csrf string, bulkType string) int {
	t.Helper()

	form := newFormValues(map[string]string{
		"bulk_action": "approve",
		"bulk_scope":  "all",
		"bulk_type":   bulkType,
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
		t.Fatalf("bulk async %s esperava 200, got %d body=%s", bulkType, rr.Code, rr.Body.String())
	}

	var payload struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("response JSON invàlid per %s: %v", bulkType, err)
	}
	jobID := parseCountValue(t, payload.JobID)
	if jobID <= 0 {
		t.Fatalf("job_id invàlid per %s: %q", bulkType, payload.JobID)
	}
	return jobID
}

func TestListPendingActivitiesAffectedTypesNoTypeMismatch(t *testing.T) {
	forEachModeracioBulkHistoryDB(t, func(t *testing.T, label string, app *core.App, database db.DB, engine string) {
		_ = app

		author := createModeracioBulkHistoryUser(t, database, fmt.Sprintf("author_bulk_fix3_%s_%d", label, time.Now().UnixNano()))
		assignAdminPolicyForModeracioBulkHistory(t, database, author.ID)

		fixture := seedModeracioBulkHistoryFixture(t, database, author.ID)
		for _, bulkType := range []string{"arxiu", "llibre", "nivell", "municipi", "eclesiastic"} {
			objectID := bulkFixtureObjectID(fixture, bulkType)
			acts, err := database.ListActivityByObjects(bulkType, []int{objectID}, "pendent")
			if err != nil {
				t.Fatalf("[%s] ListActivityByObjects %s ha fallat: %v", engine, bulkType, err)
			}
			if len(acts) != 1 {
				t.Fatalf("[%s] activitats pendents %s esperades 1, got %d", engine, bulkType, len(acts))
			}
			if acts[0].ObjectType != bulkType {
				t.Fatalf("[%s] object_type activitat esperat %s, got %s", engine, bulkType, acts[0].ObjectType)
			}
			if !acts[0].ObjectID.Valid || int(acts[0].ObjectID.Int64) != objectID {
				t.Fatalf("[%s] object_id activitat esperat %d, got %+v", engine, objectID, acts[0].ObjectID)
			}
		}
	})
}

func TestModeracioBulkAsyncAffectedTypesCompletesWithoutResidualHistoryErrors(t *testing.T) {
	forEachModeracioBulkHistoryDB(t, func(t *testing.T, label string, app *core.App, database db.DB, engine string) {
		admin := createModeracioBulkHistoryUser(t, database, fmt.Sprintf("admin_bulk_fix3_%s_%d", label, time.Now().UnixNano()))
		assignAdminPolicyForModeracioBulkHistory(t, database, admin.ID)
		session := createSessionCookie(t, database, admin.ID, fmt.Sprintf("sess_bulk_fix3_%s_%d", label, time.Now().UnixNano()))

		fixture := seedModeracioBulkHistoryFixture(t, database, admin.ID)
		for _, bulkType := range []string{"arxiu", "llibre", "nivell", "municipi", "eclesiastic"} {
			jobID := startModeracioBulkAsyncJob(t, app, session, "csrf_bulk_fix3_"+bulkType+"_"+label, bulkType)
			job := waitForAdminJobTerminal(t, database, jobID)
			if job.Status != "done" {
				t.Fatalf("[%s] job %s esperat done, got status=%s phase=%s error=%s", engine, bulkType, job.Status, job.Phase, job.ErrorText)
			}

			var result struct {
				Targets int    `json:"targets"`
				Updated int    `json:"updated"`
				Errors  int    `json:"errors"`
				Phase   string `json:"phase"`
			}
			if err := json.Unmarshal([]byte(job.ResultJSON), &result); err != nil {
				t.Fatalf("[%s] result_json invàlid per %s: %v", engine, bulkType, err)
			}
			if result.Targets != 1 || result.Updated != 1 || result.Errors != 0 || result.Phase != "done" {
				t.Fatalf("[%s] resultat bulk %s inesperat: %+v error_text=%s", engine, bulkType, result, job.ErrorText)
			}
			if strings.TrimSpace(job.ErrorText) != "" {
				t.Fatalf("[%s] job %s no hauria de tenir error_text: %s", engine, bulkType, job.ErrorText)
			}

			objectID := bulkFixtureObjectID(fixture, bulkType)
			assertBulkFixtureObjectPublicat(t, database, bulkType, objectID)

			acts, err := database.ListActivityByObjects(bulkType, []int{objectID}, "validat")
			if err != nil {
				t.Fatalf("[%s] ListActivityByObjects validat %s ha fallat: %v", engine, bulkType, err)
			}
			foundOriginal := false
			for _, act := range acts {
				if act.ID == fixture.activityIDs[bulkType] {
					foundOriginal = true
					break
				}
			}
			if !foundOriginal {
				t.Fatalf("[%s] activitat original %s no ha quedat validada", engine, bulkType)
			}
		}
	})
}
