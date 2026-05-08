package integration

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF354U6R1BulkApproveConfessionalImportHierarchyOutOfOrder(t *testing.T) {
	app, database, admin, session := setupF354U6R1BulkAdmin(t, "test_f35_4u6r1_bulk_all.sqlite3")
	suffix := time.Now().Format("150405000000000")
	prefix := "f35_4u6r1_all_" + suffix
	payload := []byte(fmt.Sprintf(`{
  "schema": "cercagenealogica.confessional.v1",
  "exported_at": "2026-06-18T12:00:00Z",
  "source": { "app": "CercaGenealogica", "module": "confessional" },
  "items": {
    "entitats_religioses": [
      {
        "ref": { "code": "%[1]s_child", "religion_code": "catolicisme_ritu_llati", "level_code": "arxiprestat_vicariat_forani" },
        "name": "Arxiprestat F35-4U6R1 %[2]s",
        "religion_code": "catolicisme_ritu_llati",
        "level_code": "arxiprestat_vicariat_forani",
        "moderation_status": "publicat"
      },
      {
        "ref": { "code": "%[1]s_root", "religion_code": "catolicisme_ritu_llati", "level_code": "santa_seu" },
        "name": "Santa Seu F35-4U6R1 %[2]s",
        "religion_code": "catolicisme_ritu_llati",
        "level_code": "santa_seu",
        "moderation_status": "publicat"
      },
      {
        "ref": { "code": "%[1]s_arch", "religion_code": "catolicisme_ritu_llati", "level_code": "arquebisbat_arxidiocesi" },
        "name": "Arquebisbat F35-4U6R1 %[2]s",
        "religion_code": "catolicisme_ritu_llati",
        "level_code": "arquebisbat_arxidiocesi",
        "moderation_status": "publicat"
      },
      {
        "ref": { "code": "%[1]s_prov", "religion_code": "catolicisme_ritu_llati", "level_code": "provincia_eclesiastica" },
        "name": "Provincia F35-4U6R1 %[2]s",
        "religion_code": "catolicisme_ritu_llati",
        "level_code": "provincia_eclesiastica",
        "moderation_status": "publicat"
      }
    ],
    "relacions_entitats": [
      {
        "parent": { "code": "%[1]s_prov", "religion_code": "catolicisme_ritu_llati", "level_code": "provincia_eclesiastica" },
        "child": { "code": "%[1]s_arch", "religion_code": "catolicisme_ritu_llati", "level_code": "arquebisbat_arxidiocesi" },
        "relation_type": "arquebisbat_arxidiocesi",
        "moderation_status": "publicat"
      },
      {
        "parent": { "code": "%[1]s_arch", "religion_code": "catolicisme_ritu_llati", "level_code": "arquebisbat_arxidiocesi" },
        "child": { "code": "%[1]s_child", "religion_code": "catolicisme_ritu_llati", "level_code": "arxiprestat_vicariat_forani" },
        "relation_type": "arxiprestat_vicariat_forani",
        "moderation_status": "publicat"
      },
      {
        "parent": { "code": "%[1]s_root", "religion_code": "catolicisme_ritu_llati", "level_code": "santa_seu" },
        "child": { "code": "%[1]s_prov", "religion_code": "catolicisme_ritu_llati", "level_code": "provincia_eclesiastica" },
        "relation_type": "provincia_eclesiastica",
        "moderation_status": "publicat"
      }
    ],
    "relacions_territorials": [],
    "relacions_arxius": []
  }
}`, prefix, suffix))

	f354U6R1ImportConfessionalPayload(t, app, database, session, payload)
	if got := len(f354U6R1ListEntitats(t, database, prefix)); got != 4 {
		t.Fatalf("s'esperaven 4 entitats importades pendents, got %d", got)
	}
	if got := len(f354U6R1ListHierarchy(t, database, prefix)); got != 3 {
		t.Fatalf("s'esperaven 3 relacions jerarquiques pendents, got %d", got)
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM usuaris_activitat WHERE usuari_id = ? AND estat = 'pendent' AND detalls = 'import' AND objecte_tipus IN ('entitat_religiosa','entitat_religiosa_relacio')", admin.ID); got != 7 {
		t.Fatalf("s'esperaven 7 activitats pendents d'import confessional, got %d", got)
	}

	jobID := submitAsyncBulkJobByTypeActionF311(t, app, session, "csrf_f35_4u6r1_all_"+suffix, "all", "approve")
	job := waitForAdminJobTerminal(t, database, jobID)
	if strings.TrimSpace(job.Status) != "done" {
		t.Fatalf("el job bulk all ha d'acabar done, got status=%s result=%s", job.Status, job.ResultJSON)
	}
	result := parseF354U6R1ModeracioJobResult(t, job.ResultJSON)
	if result.Errors != 0 || result.Updated != 7 || result.Processed != 7 || result.Targets != 7 {
		t.Fatalf("resultat bulk all inesperat: %+v", result)
	}

	for _, entity := range f354U6R1ListEntitats(t, database, prefix) {
		if entity.ModeracioEstat != "publicat" || !entity.ModeratedBy.Valid || !entity.ModeratedAt.Valid {
			t.Fatalf("l'entitat %s ha de quedar publicada i moderada: %+v", entity.Codi, entity)
		}
	}
	for _, rel := range f354U6R1ListHierarchy(t, database, prefix) {
		if rel.ModeracioEstat != "publicat" || !rel.ModeratedBy.Valid || !rel.ModeratedAt.Valid {
			t.Fatalf("la relacio %d ha de quedar publicada i moderada: %+v", rel.ID, rel)
		}
	}
	if got := countRows(t, database, "SELECT COUNT(*) AS n FROM usuaris_activitat WHERE usuari_id = ? AND estat = 'pendent' AND detalls = 'import' AND objecte_tipus IN ('entitat_religiosa','entitat_religiosa_relacio')", admin.ID); got != 0 {
		t.Fatalf("despres del bulk all no han de quedar activitats pendents, got %d", got)
	}
}

func TestF354U6R1BulkApproveEntityOnlyConsumesInitialHierarchyRelation(t *testing.T) {
	app, database, _, session := setupF354U6R1BulkAdmin(t, "test_f35_4u6r1_bulk_entity_only.sqlite3")
	suffix := time.Now().Format("150405000000000")
	prefix := "f35_4u6r1_ent_" + suffix
	payload := []byte(fmt.Sprintf(`{
  "schema": "cercagenealogica.confessional.v1",
  "exported_at": "2026-06-18T12:15:00Z",
  "source": { "app": "CercaGenealogica", "module": "confessional" },
  "items": {
    "entitats_religioses": [
      {
        "ref": { "code": "%[1]s_child", "religion_code": "catolicisme_ritu_llati", "level_code": "parroquia" },
        "name": "Parroquia F35-4U6R1 %[2]s",
        "religion_code": "catolicisme_ritu_llati",
        "level_code": "parroquia",
        "moderation_status": "publicat"
      },
      {
        "ref": { "code": "%[1]s_parent", "religion_code": "catolicisme_ritu_llati", "level_code": "bisbat_diocesi" },
        "name": "Bisbat F35-4U6R1 %[2]s",
        "religion_code": "catolicisme_ritu_llati",
        "level_code": "bisbat_diocesi",
        "moderation_status": "publicat"
      }
    ],
    "relacions_entitats": [
      {
        "parent": { "code": "%[1]s_parent", "religion_code": "catolicisme_ritu_llati", "level_code": "bisbat_diocesi" },
        "child": { "code": "%[1]s_child", "religion_code": "catolicisme_ritu_llati", "level_code": "parroquia" },
        "relation_type": "parroquia",
        "moderation_status": "publicat"
      }
    ],
    "relacions_territorials": [],
    "relacions_arxius": []
  }
}`, prefix, suffix))

	f354U6R1ImportConfessionalPayload(t, app, database, session, payload)
	jobID := submitAsyncBulkJobByTypeActionF311(t, app, session, "csrf_f35_4u6r1_entity_"+suffix, "entitat_religiosa", "approve")
	job := waitForAdminJobTerminal(t, database, jobID)
	if strings.TrimSpace(job.Status) != "done" {
		t.Fatalf("el job bulk entitat_religiosa ha d'acabar done, got status=%s result=%s", job.Status, job.ResultJSON)
	}
	result := parseF354U6R1ModeracioJobResult(t, job.ResultJSON)
	if result.Errors != 0 || result.Targets != 2 || result.Updated != 2 {
		t.Fatalf("resultat bulk entitat_religiosa inesperat: %+v", result)
	}
	for _, entity := range f354U6R1ListEntitats(t, database, prefix) {
		if entity.ModeracioEstat != "publicat" {
			t.Fatalf("l'entitat %s ha de quedar publicada, got %q", entity.Codi, entity.ModeracioEstat)
		}
	}
	for _, rel := range f354U6R1ListHierarchy(t, database, prefix) {
		if rel.ModeracioEstat != "publicat" {
			t.Fatalf("la relacio dependent ha de quedar publicada amb l'entitat, got %q", rel.ModeracioEstat)
		}
	}
}

func TestF354U6R1BulkApproveRelationOnlyFailsWhenEntitiesAreStillPending(t *testing.T) {
	app, database, _, session := setupF354U6R1BulkAdmin(t, "test_f35_4u6r1_bulk_relation_only.sqlite3")
	suffix := time.Now().Format("150405000000000")
	prefix := "f35_4u6r1_rel_" + suffix
	payload := []byte(fmt.Sprintf(`{
  "schema": "cercagenealogica.confessional.v1",
  "exported_at": "2026-06-18T12:30:00Z",
  "source": { "app": "CercaGenealogica", "module": "confessional" },
  "items": {
    "entitats_religioses": [
      {
        "ref": { "code": "%[1]s_child", "religion_code": "catolicisme_ritu_llati", "level_code": "parroquia" },
        "name": "Parroquia F35-4U6R1 %[2]s",
        "religion_code": "catolicisme_ritu_llati",
        "level_code": "parroquia",
        "moderation_status": "publicat"
      },
      {
        "ref": { "code": "%[1]s_parent", "religion_code": "catolicisme_ritu_llati", "level_code": "bisbat_diocesi" },
        "name": "Bisbat F35-4U6R1 %[2]s",
        "religion_code": "catolicisme_ritu_llati",
        "level_code": "bisbat_diocesi",
        "moderation_status": "publicat"
      }
    ],
    "relacions_entitats": [
      {
        "parent": { "code": "%[1]s_parent", "religion_code": "catolicisme_ritu_llati", "level_code": "bisbat_diocesi" },
        "child": { "code": "%[1]s_child", "religion_code": "catolicisme_ritu_llati", "level_code": "parroquia" },
        "relation_type": "parroquia",
        "moderation_status": "publicat"
      }
    ],
    "relacions_territorials": [],
    "relacions_arxius": []
  }
}`, prefix, suffix))

	f354U6R1ImportConfessionalPayload(t, app, database, session, payload)
	jobID := submitAsyncBulkJobByTypeActionF311(t, app, session, "csrf_f35_4u6r1_relation_"+suffix, "entitat_religiosa_relacio", "approve")
	job := waitForAdminJobTerminal(t, database, jobID)
	if strings.TrimSpace(job.Status) != "error" {
		t.Fatalf("el job bulk entitat_religiosa_relacio ha d'acabar error, got status=%s result=%s", job.Status, job.ResultJSON)
	}
	if !strings.Contains(job.ResultJSON, "l'entitat pare no esta publicada") {
		t.Fatalf("el resultat ha de donar un error clar de dependència, result=%s", job.ResultJSON)
	}
	for _, entity := range f354U6R1ListEntitats(t, database, prefix) {
		if entity.ModeracioEstat != "pendent" {
			t.Fatalf("cap entitat no s'ha d'aprovar parcialment, got %q per %s", entity.ModeracioEstat, entity.Codi)
		}
	}
	for _, rel := range f354U6R1ListHierarchy(t, database, prefix) {
		if rel.ModeracioEstat != "pendent" {
			t.Fatalf("la relacio ha de continuar pendent, got %q", rel.ModeracioEstat)
		}
	}
}

func TestF354U6R1BulkApproveEntityFailsWhenParentIsNotPublishedAndNotInBulk(t *testing.T) {
	app, database, admin, session := setupF354U6R1BulkAdmin(t, "test_f35_4u6r1_parent_absent.sqlite3")
	suffix := time.Now().Format("150405000000000")
	parentID := f354SSaveEntitat(t, database, "f35_4u6r1_abs_parent_"+suffix, "Pare rebutjat F35-4U6R1 "+suffix, "catolicisme_ritu_llati", "bisbat_diocesi", "rebutjat")
	childID := f354SSaveEntitat(t, database, "f35_4u6r1_abs_child_"+suffix, "Filla pendent F35-4U6R1 "+suffix, "catolicisme_ritu_llati", "parroquia", "pendent")
	if _, err := database.SaveEntitatReligiosaRelacio(&db.EntitatReligiosaRelacio{
		EntitatOrigenID: parentID,
		EntitatDestiID:  childID,
		TipusRelacio:    "parroquia",
		ModeracioEstat:  "pendent",
		CreatedBy:       sql.NullInt64{Int64: int64(admin.ID), Valid: true},
	}); err != nil {
		t.Fatalf("SaveEntitatReligiosaRelacio pending absent parent: %v", err)
	}

	jobID := submitAsyncBulkJobByTypeActionF311(t, app, session, "csrf_f35_4u6r1_absent_"+suffix, "entitat_religiosa", "approve")
	job := waitForAdminJobTerminal(t, database, jobID)
	if strings.TrimSpace(job.Status) != "error" {
		t.Fatalf("el job amb pare absent ha d'acabar error, got status=%s result=%s", job.Status, job.ResultJSON)
	}
	if !strings.Contains(job.ResultJSON, "no esta publicada ni aprovada en aquest bulk") {
		t.Fatalf("resultat sense error clar de pare absent: %s", job.ResultJSON)
	}
	child, err := database.GetEntitatReligiosa(childID)
	if err != nil || child == nil {
		t.Fatalf("GetEntitatReligiosa child: %v", err)
	}
	if child.ModeracioEstat != "pendent" {
		t.Fatalf("la filla ha de continuar pendent, got %q", child.ModeracioEstat)
	}
	rel := f353Z14FindRelation(t, database, parentID, childID)
	if rel == nil || rel.ModeracioEstat != "pendent" {
		t.Fatalf("la relacio dependent ha de continuar pendent: %+v", rel)
	}
}

func TestF354U6R1BulkApproveEntityFailsOnMultipleInitialParents(t *testing.T) {
	app, database, admin, session := setupF354U6R1BulkAdmin(t, "test_f35_4u6r1_duplicate_parents.sqlite3")
	suffix := time.Now().Format("150405000000000")
	parentA := f354SSaveEntitat(t, database, "f35_4u6r1_dup_parent_a_"+suffix, "Pare A F35-4U6R1 "+suffix, "catolicisme_ritu_llati", "bisbat_diocesi", "publicat")
	parentB := f354SSaveEntitat(t, database, "f35_4u6r1_dup_parent_b_"+suffix, "Pare B F35-4U6R1 "+suffix, "catolicisme_ritu_llati", "bisbat_diocesi", "publicat")
	childID := f354SSaveEntitat(t, database, "f35_4u6r1_dup_child_"+suffix, "Filla duplicada F35-4U6R1 "+suffix, "catolicisme_ritu_llati", "parroquia", "pendent")
	for _, parentID := range []int{parentA, parentB} {
		if _, err := database.SaveEntitatReligiosaRelacio(&db.EntitatReligiosaRelacio{
			EntitatOrigenID: parentID,
			EntitatDestiID:  childID,
			TipusRelacio:    "parroquia",
			ModeracioEstat:  "pendent",
			CreatedBy:       sql.NullInt64{Int64: int64(admin.ID), Valid: true},
		}); err != nil {
			t.Fatalf("SaveEntitatReligiosaRelacio duplicate parent=%d: %v", parentID, err)
		}
	}

	jobID := submitAsyncBulkJobByTypeActionF311(t, app, session, "csrf_f35_4u6r1_dup_"+suffix, "entitat_religiosa", "approve")
	job := waitForAdminJobTerminal(t, database, jobID)
	if strings.TrimSpace(job.Status) != "error" {
		t.Fatalf("el job amb múltiples pares ha d'acabar error, got status=%s result=%s", job.Status, job.ResultJSON)
	}
	if !strings.Contains(job.ResultJSON, "multiples relacions pare/filla inicials pendents") {
		t.Fatalf("resultat sense error clar de múltiples pares: %s", job.ResultJSON)
	}
	child, err := database.GetEntitatReligiosa(childID)
	if err != nil || child == nil {
		t.Fatalf("GetEntitatReligiosa child: %v", err)
	}
	if child.ModeracioEstat != "pendent" {
		t.Fatalf("la filla ha de continuar pendent, got %q", child.ModeracioEstat)
	}
	for _, parentID := range []int{parentA, parentB} {
		rel := f353Z14FindRelation(t, database, parentID, childID)
		if rel == nil || rel.ModeracioEstat != "pendent" {
			t.Fatalf("la relacio duplicada ha de continuar pendent parent=%d rel=%+v", parentID, rel)
		}
	}
}

func setupF354U6R1BulkAdmin(t *testing.T, dbFile string) (*core.App, db.DB, *db.User, *http.Cookie) {
	t.Helper()
	app, database := newTestAppForLogin(t, dbFile)
	admin := createTestUser(t, database, "f35_4u6r1_"+time.Now().Format("150405000000000"))
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_f35_4u6r1_"+time.Now().Format("150405000000000"))
	return app, database, admin, session
}

func f354U6R1ImportConfessionalPayload(t *testing.T, app *core.App, database db.DB, session *http.Cookie, payload []byte) {
	t.Helper()
	csrfToken, csrfCookie := extractCSRFContextFromImportExport(t, app, session)
	dryRunBody := f354U1DryRun(t, app, database, session, csrfCookie, csrfToken, payload)
	applyRR := f354U1Apply(t, app, session, csrfCookie, extractCSRFTokenFromHTML(t, dryRunBody), extractHiddenTextareaValue(t, dryRunBody, "payload_b64"))
	if applyRR.Code != http.StatusSeeOther {
		t.Fatalf("apply F35-4U6R1 status=%d body=%s", applyRR.Code, applyRR.Body.String())
	}
}

func f354U6R1ListEntitats(t *testing.T, database db.DB, prefix string) []db.EntitatReligiosa {
	t.Helper()
	rows, err := database.ListEntitatsReligioses()
	if err != nil {
		t.Fatalf("ListEntitatsReligioses: %v", err)
	}
	out := make([]db.EntitatReligiosa, 0)
	for _, row := range rows {
		if strings.HasPrefix(row.Codi, prefix) {
			out = append(out, row)
		}
	}
	return out
}

func f354U6R1ListHierarchy(t *testing.T, database db.DB, prefix string) []db.EntitatReligiosaRelacio {
	t.Helper()
	entities := f354U6R1ListEntitats(t, database, prefix)
	if len(entities) == 0 {
		return []db.EntitatReligiosaRelacio{}
	}
	entityIDs := map[int]struct{}{}
	for _, entity := range entities {
		entityIDs[entity.ID] = struct{}{}
	}
	rows, err := database.ListEntitatReligiosaRelacions()
	if err != nil {
		t.Fatalf("ListEntitatReligiosaRelacions: %v", err)
	}
	out := make([]db.EntitatReligiosaRelacio, 0)
	for _, row := range rows {
		if _, ok := entityIDs[row.EntitatOrigenID]; ok {
			if _, ok := entityIDs[row.EntitatDestiID]; ok {
				out = append(out, row)
			}
		}
	}
	return out
}

func parseF354U6R1ModeracioJobResult(t *testing.T, raw string) struct {
	Targets   int `json:"targets"`
	Processed int `json:"processed"`
	Updated   int `json:"updated"`
	Errors    int `json:"errors"`
} {
	t.Helper()
	var result struct {
		Targets   int `json:"targets"`
		Processed int `json:"processed"`
		Updated   int `json:"updated"`
		Errors    int `json:"errors"`
	}
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		t.Fatalf("result_json de moderacio bulk invàlid: %v raw=%s", err, raw)
	}
	return result
}
