package integration

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestF354U5ConfessionalImportApplyUsesSharedAdminAuditJobsActivitiesAndModeration(t *testing.T) {
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
	for _, token := range []string{`"entities_created":2`, `"hierarchy_created":1`, `"territory_created":1`, `"archive_created":1`, `"wiki_created":0`, `"activity_count":5`, `"admin_target_count":5`} {
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

	importedEntities := f354U1ListEntitats(t, targetDB)
	if len(importedEntities) != 2 {
		t.Fatalf("s'esperaven 2 entitats confessional importades, got %d", len(importedEntities))
	}
	for _, entity := range importedEntities {
		if entity.ModeracioEstat != "pendent" {
			t.Fatalf("l'entitat %d ha d'entrar pendent, got %q", entity.ID, entity.ModeracioEstat)
		}
		if entity.ModeratedBy.Valid || entity.ModeratedAt.Valid {
			t.Fatalf("l'entitat %d no s'ha de moderar directament durant l'import", entity.ID)
		}
		changes, err := targetDB.ListWikiChanges("entitat_religiosa", entity.ID)
		if err != nil {
			t.Fatalf("ListWikiChanges entitat %d: %v", entity.ID, err)
		}
		if len(changes) != 0 {
			t.Fatalf("l'entitat %d no ha de crear wiki publicada prematurament, got %d canvis", entity.ID, len(changes))
		}
	}

	hierarchyRows := f354U1ListHierarchy(t, targetDB)
	if len(hierarchyRows) != 1 {
		t.Fatalf("s'esperava 1 relacio jerarquica importada, got %d", len(hierarchyRows))
	}
	for _, rel := range hierarchyRows {
		if rel.ModeracioEstat != "pendent" || rel.ModeratedBy.Valid || rel.ModeratedAt.Valid {
			t.Fatalf("la relacio jerarquica %d ha d'entrar pendent i sense moderador", rel.ID)
		}
	}
	territoryRows := f354U1ListTerritory(t, targetDB)
	if len(territoryRows) != 1 {
		t.Fatalf("s'esperava 1 relacio territorial importada, got %d", len(territoryRows))
	}
	for _, rel := range territoryRows {
		if rel.ModeracioEstat != "pendent" || rel.ModeratedBy.Valid || rel.ModeratedAt.Valid {
			t.Fatalf("la relacio territorial %d ha d'entrar pendent i sense moderador", rel.ID)
		}
	}
	archiveRows := f354U1ListArchive(t, targetDB)
	if len(archiveRows) != 1 {
		t.Fatalf("s'esperava 1 relacio arxiu-entitat importada, got %d", len(archiveRows))
	}
	for _, rel := range archiveRows {
		if rel.ModeracioEstat != "pendent" || rel.ModeratedBy.Valid || rel.ModeratedAt.Valid {
			t.Fatalf("la relacio arxiu-entitat %d ha d'entrar pendent i sense moderador", rel.ID)
		}
	}
}

func TestF354U5ConfessionalImportForcesPendingEvenWhenJSONSaysPublished(t *testing.T) {
	targetApp, targetDB := newTestAppForLogin(t, "test_f35_4u5_force_pending.sqlite3")
	targetUser := createTestUser(t, targetDB, "f35_4u5_force_pending")
	targetSession := createSessionCookie(t, targetDB, targetUser.ID, "sess_f35_4u5_force_pending")
	targetPolicy := createPolicyWithGrant(t, targetDB, "f35_4u5_force_pending_import", "territori.confessional.import_export.import")
	addGrantToPolicy(t, targetDB, targetPolicy, "territori.confessional.import_export.view")
	assignPolicyToUser(t, targetDB, targetUser.ID, targetPolicy)

	payload := []byte(`{
  "schema": "cercagenealogica.confessional.v1",
  "exported_at": "2026-06-16T12:00:00Z",
  "source": { "app": "CercaGenealogica", "module": "confessional" },
  "items": {
    "entitats_religioses": [
      {
        "ref": {
          "code": "f35_4u5_entity_pending",
          "religion_code": "catolicisme_ritu_llati",
          "level_code": "parroquia"
        },
        "name": "Parroquia F35-4U5 Pendent",
        "religion_code": "catolicisme_ritu_llati",
        "level_code": "parroquia",
        "moderation_status": "publicat"
      }
    ],
    "relacions_entitats": [],
    "relacions_territorials": [],
    "relacions_arxius": []
  }
}`)

	csrfToken, csrfCookie := extractCSRFContextFromImportExport(t, targetApp, targetSession)
	dryRunBody := f354U1DryRun(t, targetApp, targetDB, targetSession, csrfCookie, csrfToken, payload)
	applyRR := f354U1Apply(t, targetApp, targetSession, csrfCookie, extractCSRFTokenFromHTML(t, dryRunBody), extractHiddenTextareaValue(t, dryRunBody, "payload_b64"))
	if applyRR.Code != http.StatusSeeOther {
		t.Fatalf("apply F35-4U5 pending status=%d body=%s", applyRR.Code, applyRR.Body.String())
	}

	rows, err := targetDB.ListEntitatsReligioses()
	if err != nil {
		t.Fatalf("ListEntitatsReligioses: %v", err)
	}
	entity := findConfEntityByCode(rows, "f35_4u5_entity_pending")
	if entity == nil {
		t.Fatalf("no s'ha trobat l'entitat importada amb moderation_status publicat al JSON")
	}
	if entity.ModeracioEstat != "pendent" {
		t.Fatalf("el JSON amb publicat s'ha de normalitzar a pendent, got %q", entity.ModeracioEstat)
	}
	if entity.ModeratedBy.Valid || entity.ModeratedAt.Valid {
		t.Fatalf("l'entitat importada no ha de tenir moderated_by/moderated_at")
	}
	changes, err := targetDB.ListWikiChanges("entitat_religiosa", entity.ID)
	if err != nil {
		t.Fatalf("ListWikiChanges entitat pending: %v", err)
	}
	if len(changes) != 0 {
		t.Fatalf("una entitat importada pendent no ha de crear wiki publicada prematura, got %d canvis", len(changes))
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
