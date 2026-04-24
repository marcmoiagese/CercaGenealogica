package integration

import (
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type f32131PrincipalCountingDB struct {
	db.DB
	listTranscripcionsCalls int
	listPersonesCalls       int
	listPersonesByIDs       int
	listPersonesByBook      int
}

func (d *f32131PrincipalCountingDB) ListTranscripcionsRaw(llibreID int, f db.TranscripcioFilter) ([]db.TranscripcioRaw, error) {
	d.listTranscripcionsCalls++
	return d.DB.ListTranscripcionsRaw(llibreID, f)
}

func (d *f32131PrincipalCountingDB) ListTranscripcioPersones(transcripcioID int) ([]db.TranscripcioPersonaRaw, error) {
	d.listPersonesCalls++
	return d.DB.ListTranscripcioPersones(transcripcioID)
}

func (d *f32131PrincipalCountingDB) ListTranscripcioPersonesByTranscripcioIDs(transcripcioIDs []int) (map[int][]db.TranscripcioPersonaRaw, error) {
	d.listPersonesByIDs++
	return d.DB.ListTranscripcioPersonesByTranscripcioIDs(transcripcioIDs)
}

func (d *f32131PrincipalCountingDB) ListTranscripcioPersonesByLlibreID(llibreID int) (map[int][]db.TranscripcioPersonaRaw, error) {
	d.listPersonesByBook++
	return d.DB.ListTranscripcioPersonesByLlibreID(llibreID)
}

func (d *f32131PrincipalCountingDB) GetMaxTranscripcioRawID() (int, error) {
	loader, ok := d.DB.(interface {
		GetMaxTranscripcioRawID() (int, error)
	})
	if !ok {
		return 0, nil
	}
	return loader.GetMaxTranscripcioRawID()
}

func TestTemplateImportPrincipalMatchUsesRuntimeBulkCandidatesSQLitePostgresF32131(t *testing.T) {
	modelJSON := `{
  "version": 1,
  "kind": "transcripcions_raw",
  "book_resolution": { "mode": "llibre_id", "column": "llibre_id" },
  "mapping": {
    "columns": [
      { "header": "llibre_id", "key": "llibre_id", "required": true, "map_to": [{ "target": "base.llibre_id" }] },
      { "header": "tipus_acte", "key": "tipus_acte", "required": true, "map_to": [{ "target": "base.tipus_acte" }] },
      { "header": "notes", "key": "notes", "map_to": [{ "target": "base.notes_marginals" }] },
      { "header": "batejat", "key": "batejat", "map_to": [{ "target": "person.batejat", "transform": [{ "op": "parse_person_from_nom" }] }] }
    ]
  },
  "policies": {
    "merge_existing": {
      "mode": "by_principal_person_if_book_indexed",
      "principal_roles": ["batejat"],
      "update_missing_only": true,
      "add_missing_people": true,
      "add_missing_attrs": true
    }
  }
}`

	for _, cfg := range loadSQLitePostgresAndMySQLConfigsForImportHistory(t) {
		cfg := cfg
		t.Run(cfg.Label, func(t *testing.T) {
			app, database := newTestAppForConfigOrSkipMySQL(t, cfg.Config)
			user, sessionID := createF7UserWithSession(t, database)
			ensureAdminPolicyForUser(t, database, user.ID)
			llibreID, _ := createF7LlibreWithPagina(t, database, user.ID)

			llibre, _ := database.GetLlibre(llibreID)
			llibre.IndexacioCompleta = true
			_ = database.UpdateLlibre(llibre)

			existingID, err := database.CreateTranscripcioRaw(&db.TranscripcioRaw{
				LlibreID:       llibreID,
				TipusActe:      "baptisme",
				ModeracioEstat: "pendent",
			})
			if err != nil {
				t.Fatalf("[%s] CreateTranscripcioRaw ha fallat: %v", cfg.Label, err)
			}
			if _, err := database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
				TranscripcioID: existingID,
				Rol:            "batejat",
				Nom:            "Joan",
				Cognom1:        "Garcia",
			}); err != nil {
				t.Fatalf("[%s] CreateTranscripcioPersona ha fallat: %v", cfg.Label, err)
			}

			templateID, err := database.CreateCSVImportTemplate(&db.CSVImportTemplate{
				Name:        "Template F32131 " + cfg.Label,
				OwnerUserID: sqlNullFromInt(user.ID),
				Visibility:  "private",
				ModelJSON:   modelJSON,
			})
			if err != nil || templateID == 0 {
				t.Fatalf("[%s] CreateCSVImportTemplate ha fallat: %v", cfg.Label, err)
			}

			countingDB := &f32131PrincipalCountingDB{DB: database}
			app.DB = countingDB

			req := buildImportGlobalRequest(t, sessionID, "csrf-f32131-"+cfg.Label, map[string]string{
				"model":       "template",
				"template_id": strconv.Itoa(templateID),
				"separator":   ",",
			}, strings.Join([]string{
				"llibre_id,tipus_acte,notes,batejat",
				strconv.Itoa(llibreID) + ",baptisme,Nota nova,Joan Garcia",
			}, "\n"))
			rr := httptest.NewRecorder()
			app.AdminImportRegistresGlobal(rr, req)
			if rr.Result().StatusCode != 303 {
				t.Fatalf("[%s] status inesperat: %d body=%s", cfg.Label, rr.Result().StatusCode, rr.Body.String())
			}

			updated, err := database.GetTranscripcioRaw(existingID)
			if err != nil {
				t.Fatalf("[%s] GetTranscripcioRaw ha fallat: %v", cfg.Label, err)
			}
			if updated.NotesMarginals != "Nota nova" {
				t.Fatalf("[%s] esperava merge per principal, notes=%q", cfg.Label, updated.NotesMarginals)
			}
			if countingDB.listTranscripcionsCalls == 0 {
				t.Fatalf("[%s] s'esperava càrrega de candidats de transcripcions al camí per principal", cfg.Label)
			}
			if countingDB.listPersonesCalls > 1 {
				t.Fatalf("[%s] el camí per principal no ha d'escalar fila-a-fila en càrrega d'existents: persones=%d", cfg.Label, countingDB.listPersonesCalls)
			}
			if cfg.Engine == "postgres" || cfg.Engine == "mysql" {
				if countingDB.listPersonesByBook == 0 {
					t.Fatalf("[%s] %s ha d'usar càrrega bulk per llibre al camí principal", cfg.Label, cfg.Engine)
				}
				if countingDB.listPersonesByIDs != 0 {
					t.Fatalf("[%s] %s no ha de recórrer a càrrega per IDs al camí principal: persones_by_ids=%d", cfg.Label, cfg.Engine, countingDB.listPersonesByIDs)
				}
			} else {
				if countingDB.listPersonesByIDs == 0 {
					t.Fatalf("[%s] SQLite ha de mantenir la càrrega bulk per IDs al camí principal", cfg.Label)
				}
			}
		})
	}
}
