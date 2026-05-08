package integration

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestF354U5ConfessionalImportApplyUsesSharedAdminAuditJobsActivitiesAndWiki(t *testing.T) {
	targetApp, targetDB, targetSession, exportedJSON, _, _ := f354U1PrepareTargetAndExport(t, "test_f35_4u5_shared_infra.sqlite3", true)
	csrfToken, csrfCookie := extractCSRFContextFromImportExport(t, targetApp, targetSession)
	dryRunBody := f354U1DryRun(t, targetApp, targetDB, targetSession, csrfCookie, csrfToken, exportedJSON)
	applyRR := f354U1Apply(t, targetApp, targetSession, csrfCookie, extractCSRFTokenFromHTML(t, dryRunBody), extractHiddenTextareaValue(t, dryRunBody, "payload_b64"))
	if applyRR.Code != http.StatusSeeOther {
		t.Fatalf("apply F35-4U5 status=%d body=%s", applyRR.Code, applyRR.Body.String())
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(targetSession)
	user, ok := targetApp.VerificarSessio(req)
	if !ok || user == nil {
		t.Fatalf("no s'ha pogut resoldre l'usuari del target")
	}

	if got := countRows(t, targetDB, "SELECT COUNT(*) AS n FROM admin_import_runs WHERE import_type = ? AND status = 'ok'", "confessional"); got != 1 {
		t.Fatalf("s'esperava 1 admin_import_runs confessional en estat ok, got %d", got)
	}
	if got := countRows(t, targetDB, "SELECT COUNT(*) AS n FROM admin_audit WHERE action = ? AND object_type = ? AND actor_id = ?", "admin_import", "import", user.ID); got != 1 {
		t.Fatalf("s'esperava 1 entrada d'auditoria admin_import, got %d", got)
	}

	jobRows, err := targetDB.Query("SELECT id, status, payload_json, result_json FROM admin_jobs WHERE kind = ? ORDER BY id DESC LIMIT 1", "admin_import")
	if err != nil || len(jobRows) != 1 {
		t.Fatalf("no s'ha pogut llegir l'admin job confessional: err=%v rows=%d", err, len(jobRows))
	}
	jobID := parseCountValue(t, jobRows[0]["id"])
	jobStatus := strings.TrimSpace(asString(jobRows[0]["status"]))
	jobPayload := asString(jobRows[0]["payload_json"])
	jobResult := asString(jobRows[0]["result_json"])
	if jobStatus != "done" {
		t.Fatalf("l'admin job confessional ha d'acabar en done, got %q", jobStatus)
	}
	for _, token := range []string{`"import_type":"confessional"`, `"entities_requested":2`, `"hierarchy_requested":1`, `"territory_requested":1`, `"archive_requested":1`} {
		if !strings.Contains(jobPayload, token) {
			t.Fatalf("payload de l'admin job ha de contenir %q; payload=%s", token, jobPayload)
		}
	}
	for _, token := range []string{`"entities_created":2`, `"hierarchy_created":1`, `"territory_created":1`, `"archive_created":1`, `"wiki_created":2`, `"activity_count":5`, `"admin_target_count":5`} {
		if !strings.Contains(jobResult, token) {
			t.Fatalf("result de l'admin job ha de contenir %q; result=%s", token, jobResult)
		}
	}
	if got := countRows(t, targetDB, "SELECT COUNT(*) AS n FROM admin_job_targets WHERE job_id = ?", jobID); got != 5 {
		t.Fatalf("s'esperaven 5 admin_job_targets pel job confessional, got %d", got)
	}
	if got := countRows(t, targetDB, "SELECT COUNT(*) AS n FROM usuaris_activitat WHERE usuari_id = ? AND estat = 'pendent' AND detalls = 'import' AND objecte_tipus IN ('entitat_religiosa','entitat_religiosa_relacio','municipi_entitat_religiosa','arxiu_entitat_religiosa')", user.ID); got != 5 {
		t.Fatalf("s'esperaven 5 activitats pendents d'import confessional, got %d", got)
	}

	importedEntities := filterPublishedConfEntitats(f354U1ListEntitats(t, targetDB))
	if len(importedEntities) != 2 {
		t.Fatalf("s'esperaven 2 entitats confessional importades, got %d", len(importedEntities))
	}
	for _, entity := range importedEntities {
		changes, err := targetDB.ListWikiChanges("entitat_religiosa", entity.ID)
		if err != nil {
			t.Fatalf("ListWikiChanges entitat %d: %v", entity.ID, err)
		}
		publicades := 0
		for _, change := range changes {
			if change.ModeracioEstat == "publicat" {
				publicades++
			}
		}
		if publicades != 1 {
			t.Fatalf("l'entitat %d ha de tenir exactament 1 versio wiki inicial publicada, got %d", entity.ID, publicades)
		}
	}
}

func asString(raw interface{}) string {
	switch v := raw.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return ""
	}
}
