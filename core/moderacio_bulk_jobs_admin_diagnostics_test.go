package core

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
	"golang.org/x/crypto/bcrypt"
)

func newModeracioBulkDiagnosticsApp(t *testing.T) (*App, db.DB) {
	t.Helper()

	projectRoot := findCoreProjectRoot(t)
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatalf("os.Chdir(%s): %v", projectRoot, err)
	}
	cfg := map[string]string{
		"DB_ENGINE": "sqlite",
		"DB_PATH":   filepath.Join(t.TempDir(), "moderacio-bulk-diagnostics.sqlite3"),
		"RECREADB":  "true",
		"LOG_LEVEL": "silent",
	}
	database, err := db.NewDB(cfg)
	if err != nil {
		t.Fatalf("db.NewDB ha fallat: %v", err)
	}
	app := NewApp(cfg, database)
	t.Cleanup(func() {
		app.Close()
	})
	return app, database
}

func findCoreProjectRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("no s'ha trobat go.mod a cap directori pare")
		}
		dir = parent
	}
}

func createModeracioBulkDiagnosticsUser(t *testing.T, database db.DB, username string) *db.User {
	t.Helper()

	hash, err := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt ha fallat: %v", err)
	}
	user := &db.User{
		Usuari:        username,
		Name:          "Test",
		Surname:       "User",
		Email:         username + "@example.org",
		Password:      hash,
		DataNaixament: "1990-01-01",
		Active:        true,
		CreatedAt:     time.Now().UTC().Format(time.RFC3339),
	}
	if err := database.InsertUser(user); err != nil {
		t.Fatalf("InsertUser ha fallat: %v", err)
	}
	stored, err := database.GetUserByEmail(user.Email)
	if err != nil || stored == nil {
		t.Fatalf("GetUserByEmail ha fallat: %v", err)
	}
	return stored
}

func TestRunModeracioBulkAdminJobPersistsErrorDiagnostics(t *testing.T) {
	app, database := newModeracioBulkDiagnosticsApp(t)

	creator := createModeracioBulkDiagnosticsUser(t, database, "bulk_diag_creator")
	arxiu := &db.Arxiu{
		Nom:            "Arxiu diag bulk",
		Tipus:          "Parroquial",
		ModeracioEstat: "pendent",
	}
	if _, err := database.CreateArxiu(arxiu); err != nil {
		t.Fatalf("CreateArxiu ha fallat: %v", err)
	}

	payloadJSON, _ := json.Marshal(moderacioBulkJobPayload{
		Action:   "approve",
		Scope:    "all",
		BulkType: "arxiu",
		Source:   "test",
	})
	jobID, err := database.CreateAdminJob(&db.AdminJob{
		Kind:        adminJobKindModeracioBulk,
		Status:      adminJobStatusQueued,
		Phase:       adminJobPhaseQueued,
		PayloadJSON: string(payloadJSON),
		CreatedBy:   sqlNullIntFromInt(creator.ID),
	})
	if err != nil {
		t.Fatalf("CreateAdminJob ha fallat: %v", err)
	}
	if err := database.CreateAdminJobTargets(jobID, []db.AdminJobTarget{{
		SeqNum:     1,
		ObjectType: "arxiu",
		ObjectID:   arxiu.ID,
	}}); err != nil {
		t.Fatalf("CreateAdminJobTargets ha fallat: %v", err)
	}

	snapshot := moderacioBulkSnapshot{
		Targets: []db.AdminJobTarget{{
			SeqNum:     1,
			ObjectType: "arxiu",
			ObjectID:   arxiu.ID,
		}},
		Candidates: 1,
		ScopeMode:  "global",
		ByType:     []moderacioTypeCount{{Type: "arxiu", Total: 1}},
	}

	app.runModeracioBulkAdminJob(jobID, "approve", "", 999999, snapshot)

	job, err := database.GetAdminJob(jobID)
	if err != nil || job == nil {
		t.Fatalf("GetAdminJob ha fallat: %v", err)
	}
	if strings.TrimSpace(job.Status) != adminJobStatusError {
		t.Fatalf("status esperat error, got %s", job.Status)
	}

	var result moderacioBulkJobResult
	if err := json.Unmarshal([]byte(job.ResultJSON), &result); err != nil {
		t.Fatalf("result_json invàlid: %v", err)
	}
	if result.Errors <= 0 {
		t.Fatalf("s'esperaven errors persistits al resultat: %+v", result)
	}
	if len(result.ErrorPhases) == 0 {
		t.Fatalf("s'esperava resum de fases amb errors")
	}
	if len(result.ErrorSamples) == 0 {
		t.Fatalf("s'esperava mostra d'errors")
	}
	if result.ErrorSamples[0].Phase != adminJobPhaseApplyingChanges {
		t.Fatalf("fase d'error inesperada: %+v", result.ErrorSamples[0])
	}
	if result.ErrorSamples[0].Step != "bulk_update" {
		t.Fatalf("step d'error inesperat: %+v", result.ErrorSamples[0])
	}
	if !strings.Contains(job.ErrorText, "primer error") {
		t.Fatalf("error_text hauria de resumir el primer error, got %q", job.ErrorText)
	}
}

func TestAdminJobAgeClampsFutureTimes(t *testing.T) {
	job := db.AdminJob{
		StartedAt: sql.NullTime{Time: adminJobNow().Add(2 * time.Hour), Valid: true},
	}
	if age := adminJobAge(job); age != 0 {
		t.Fatalf("age hauria de quedar clampat a zero, got %s", age)
	}
}
