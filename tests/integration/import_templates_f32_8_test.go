package integration

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestTemplateImportStrongDedupIndexedPageSQLitePostgresF328(t *testing.T) {
	for _, cfg := range loadSQLiteAndPostgresConfigsForImportHistory(t) {
		cfg := cfg
		t.Run(cfg.Label, func(t *testing.T) {
			app, database := newTestAppForConfig(t, cfg.Config)
			user, sessionID := createF7UserWithSession(t, database)
			ensureAdminPolicyForUser(t, database, user.ID)
			llibreID, paginaID := createF7LlibreWithPagina(t, database, user.ID)

			modelJSON := `{
  "version": 1,
  "kind": "transcripcions_raw",
  "book_resolution": { "mode": "llibre_id", "column": "llibre_id" },
  "mapping": {
    "columns": [
      { "header": "llibre_id", "key": "llibre_id", "required": true, "map_to": [{ "target": "base.llibre_id" }] },
      { "header": "tipus_acte", "key": "tipus_acte", "required": true, "map_to": [{ "target": "base.tipus_acte" }] },
      { "header": "pagina", "key": "pagina", "required": false,
        "map_to": [
          { "target": "base.num_pagina_text", "transform": [{ "op": "trim" }] },
          { "target": "attr.pagina_digital.text_with_quality", "transform": [{ "op": "trim" }, { "op": "default_quality_if_present" }] }
        ]
      },
      { "header": "cognoms", "key": "cognoms", "map_to": [{ "target": "person.batejat", "transform": [{ "op": "parse_person_from_cognoms" }] }] },
      { "header": "pare", "key": "pare", "map_to": [{ "target": "person.pare", "transform": [{ "op": "parse_person_from_nom" }] }] },
      { "header": "mare", "key": "mare", "map_to": [{ "target": "person.mare", "transform": [{ "op": "parse_person_from_nom" }] }] },
      { "header": "nascut", "key": "nascut", "map_to": [{ "target": "attr.data_naixement.date_or_text_with_quality", "transform": [{ "op": "parse_date_flexible_to_date_or_text_with_quality" }] }] },
      { "header": "acte", "key": "acte",
        "map_to": [
          { "target": "base.data_acte_iso_text_estat", "transform": [{ "op": "parse_date_flexible_to_base_data_acte" }] },
          { "target": "attr.data_bateig.date_or_text_with_quality", "transform": [{ "op": "parse_date_flexible_to_date_or_text_with_quality" }] }
        ]
      }
    ]
  },
  "policies": {
    "dedup": {
      "within_file": true,
      "key_columns": ["llibre_id","pagina","cognoms","pare","mare","nascut","acte"],
      "if_principal_name_missing_add_row_index": true
    },
    "merge_existing": {
      "mode": "by_strong_signature_if_page_indexed",
      "principal_roles": ["batejat"],
      "update_missing_only": true,
      "add_missing_people": true,
      "add_missing_attrs": true,
      "avoid_duplicate_rows_by_principal_name_per_book": true
    }
  }
}`
			templateID, err := database.CreateCSVImportTemplate(&db.CSVImportTemplate{
				Name:             "Template F328 " + cfg.Label,
				OwnerUserID:      sqlNullFromInt(user.ID),
				Visibility:       "private",
				DefaultSeparator: ",",
				ModelJSON:        modelJSON,
			})
			if err != nil || templateID == 0 {
				t.Fatalf("[%s] CreateCSVImportTemplate ha fallat: %v", cfg.Label, err)
			}

			existingID, err := database.CreateTranscripcioRaw(&db.TranscripcioRaw{
				LlibreID:       llibreID,
				PaginaID:       sql.NullInt64{Int64: int64(paginaID), Valid: true},
				NumPaginaText:  "1",
				TipusActe:      "baptisme",
				DataActeText:   "05/02/1890",
				DataActeISO:    sql.NullString{String: "1890-02-05", Valid: true},
				DataActeEstat:  "clar",
				ModeracioEstat: "pendent",
			})
			if err != nil {
				t.Fatalf("[%s] CreateTranscripcioRaw existent ha fallat: %v", cfg.Label, err)
			}
			if _, err := database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
				TranscripcioID: existingID,
				Rol:            "batejat",
				Nom:            "Joan",
				Cognom1:        "Garcia",
				Cognom2:        "Soler",
			}); err != nil {
				t.Fatalf("[%s] CreateTranscripcioPersona batejat ha fallat: %v", cfg.Label, err)
			}
			if _, err := database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
				TranscripcioID: existingID,
				Rol:            "pare",
				Nom:            "Pere",
				Cognom1:        "Garcia",
			}); err != nil {
				t.Fatalf("[%s] CreateTranscripcioPersona pare ha fallat: %v", cfg.Label, err)
			}
			if _, err := database.CreateTranscripcioPersona(&db.TranscripcioPersonaRaw{
				TranscripcioID: existingID,
				Rol:            "mare",
				Nom:            "Maria",
				Cognom1:        "Puig",
			}); err != nil {
				t.Fatalf("[%s] CreateTranscripcioPersona mare ha fallat: %v", cfg.Label, err)
			}
			if _, err := database.CreateTranscripcioAtribut(&db.TranscripcioAtributRaw{
				TranscripcioID: existingID,
				Clau:           "pagina_digital",
				TipusValor:     "text",
				ValorText:      "1",
				Estat:          "clar",
			}); err != nil {
				t.Fatalf("[%s] CreateTranscripcioAtribut pagina_digital ha fallat: %v", cfg.Label, err)
			}
			if _, err := database.CreateTranscripcioAtribut(&db.TranscripcioAtributRaw{
				TranscripcioID: existingID,
				Clau:           "data_bateig",
				TipusValor:     "date",
				ValorDate:      sql.NullString{String: "1890-02-05", Valid: true},
				Estat:          "clar",
			}); err != nil {
				t.Fatalf("[%s] CreateTranscripcioAtribut bateig ha fallat: %v", cfg.Label, err)
			}
			if _, err := database.CreateTranscripcioAtribut(&db.TranscripcioAtributRaw{
				TranscripcioID: existingID,
				Clau:           "data_naixement",
				TipusValor:     "date",
				ValorDate:      sql.NullString{String: "1890-02-01", Valid: true},
				Estat:          "clar",
			}); err != nil {
				t.Fatalf("[%s] CreateTranscripcioAtribut naixement ha fallat: %v", cfg.Label, err)
			}

			csvContent := strings.Join([]string{
				"llibre_id,tipus_acte,pagina,cognoms,pare,mare,nascut,acte",
				fmt.Sprintf("%d,baptisme,1,Garcia Soler Joan,Pere Garcia,Maria Puig,01/02/1890,05/02/1890", llibreID),
			}, "\n")
			req := buildImportGlobalRequest(t, sessionID, "csrf-f328-"+cfg.Label, map[string]string{
				"model":       "template",
				"template_id": strconv.Itoa(templateID),
				"separator":   ",",
			}, csvContent)
			rr := httptest.NewRecorder()
			app.AdminImportRegistresGlobal(rr, req)
			if rr.Result().StatusCode != http.StatusSeeOther {
				t.Fatalf("[%s] status inesperat: %d body=%s", cfg.Label, rr.Result().StatusCode, rr.Body.String())
			}
			_, failed := parseRedirectCounts(t, rr)
			if failed != 0 {
				t.Fatalf("[%s] el merge fort no ha de fallar, failed=%d", cfg.Label, failed)
			}
			registres, err := database.ListTranscripcionsRaw(llibreID, db.TranscripcioFilter{Limit: -1})
			if err != nil {
				t.Fatalf("[%s] ListTranscripcionsRaw ha fallat: %v", cfg.Label, err)
			}
			if len(registres) != 1 {
				t.Fatalf("[%s] la deduplicació forta ha de conservar 1 sol registre, got=%d", cfg.Label, len(registres))
			}
		})
	}
}
