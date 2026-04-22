package integration

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/cnf"
	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

type f3210CountingDB struct {
	db.DB
	listTranscripcionsRawCalls int
	listPersonesCalls          int
	listPersonesByIDs          int
	listAtributsCalls          int
	listAtributsByIDs          int
}

func (d *f3210CountingDB) ListTranscripcionsRaw(llibreID int, f db.TranscripcioFilter) ([]db.TranscripcioRaw, error) {
	d.listTranscripcionsRawCalls++
	return d.DB.ListTranscripcionsRaw(llibreID, f)
}

func (d *f3210CountingDB) ListTranscripcioPersones(transcripcioID int) ([]db.TranscripcioPersonaRaw, error) {
	d.listPersonesCalls++
	return d.DB.ListTranscripcioPersones(transcripcioID)
}

func (d *f3210CountingDB) ListTranscripcioPersonesByTranscripcioIDs(transcripcioIDs []int) (map[int][]db.TranscripcioPersonaRaw, error) {
	d.listPersonesByIDs++
	return d.DB.ListTranscripcioPersonesByTranscripcioIDs(transcripcioIDs)
}

func (d *f3210CountingDB) ListTranscripcioAtributs(transcripcioID int) ([]db.TranscripcioAtributRaw, error) {
	d.listAtributsCalls++
	return d.DB.ListTranscripcioAtributs(transcripcioID)
}

func (d *f3210CountingDB) ListTranscripcioAtributsByTranscripcioIDs(transcripcioIDs []int) (map[int][]db.TranscripcioAtributRaw, error) {
	d.listAtributsByIDs++
	return d.DB.ListTranscripcioAtributsByTranscripcioIDs(transcripcioIDs)
}

func TestTemplateImportStrongDedupUsesBulkExistingFetchSQLitePostgresF3210(t *testing.T) {
	for _, cfg := range loadSQLiteAndPostgresConfigsForImportHistory(t) {
		cfg := cfg
		t.Run(cfg.Label, func(t *testing.T) {
			app, database := newTestAppForConfig(t, cfg.Config)
			user, sessionID := createF7UserWithSession(t, database)
			ensureAdminPolicyForUser(t, database, user.ID)
			llibreID, paginaID := createF7LlibreWithPagina(t, database, user.ID)

			countingDB := &f3210CountingDB{DB: database}
			app.DB = countingDB

			templateID := createF3210Template(t, database, user.ID, cfg.Label)
			existingID := createF3210ExistingStrongBaptisme(t, database, llibreID, paginaID)

			req := buildImportGlobalRequest(t, sessionID, "csrf-f3210-bulk-"+cfg.Label, map[string]string{
				"model":       "template",
				"template_id": strconv.Itoa(templateID),
				"separator":   ",",
			}, strings.Join([]string{
				"llibre_id,tipus_acte,pagina,cognoms,pare,mare,nascut,acte",
				strconv.Itoa(llibreID) + ",baptisme,1,Garcia Soler Joan,Pere Garcia,Maria Puig,01/02/1890,05/02/1890",
			}, "\n"))
			rr := httptest.NewRecorder()
			app.AdminImportRegistresGlobal(rr, req)
			if rr.Code != http.StatusSeeOther {
				t.Fatalf("[%s] status inesperat: %d body=%s", cfg.Label, rr.Code, rr.Body.String())
			}
			_, failed := parseRedirectCounts(t, rr)
			if failed != 0 {
				t.Fatalf("[%s] el merge fort no ha de fallar, failed=%d", cfg.Label, failed)
			}
			registres, err := database.ListTranscripcionsRaw(llibreID, db.TranscripcioFilter{Limit: -1})
			if err != nil {
				t.Fatalf("[%s] ListTranscripcionsRaw ha fallat: %v", cfg.Label, err)
			}
			if len(registres) != 1 || registres[0].ID != existingID {
				t.Fatalf("[%s] el duplicat fort ha de fusionar amb l'existent, registres=%+v existingID=%d", cfg.Label, registres, existingID)
			}
			if countingDB.listTranscripcionsRawCalls == 0 {
				t.Fatalf("[%s] s'esperava almenys una lectura de transcripcions per al context fort", cfg.Label)
			}
			if countingDB.listPersonesByIDs == 0 || countingDB.listAtributsByIDs == 0 {
				t.Fatalf("[%s] el carregador d'existents ha d'usar càrrega bulk: persones_by_ids=%d atributs_by_ids=%d", cfg.Label, countingDB.listPersonesByIDs, countingDB.listAtributsByIDs)
			}
			if countingDB.listPersonesCalls > 1 || countingDB.listAtributsCalls > 1 {
				t.Fatalf("[%s] el carregador d'existents no ha d'anar fila a fila; només s'accepta el read únic del merge: persones=%d atributs=%d", cfg.Label, countingDB.listPersonesCalls, countingDB.listAtributsCalls)
			}
		})
	}
}

func TestTemplateImportDuplicateCheckMetricsSQLitePostgresF3210(t *testing.T) {
	writeDuplicateCheckRe := regexp.MustCompile(`write_duplicate_check_dur=([^ ]+)`)
	writeDurRe := regexp.MustCompile(`write_dur=([^ ]+)`)
	resolveDurRe := regexp.MustCompile(`resolve_dur=([^ ]+)`)

	for _, cfg := range loadSQLiteAndPostgresConfigsForImportHistory(t) {
		cfg := cfg
		t.Run(cfg.Label, func(t *testing.T) {
			app, database := newTestAppForConfig(t, cfg.Config)
			user, sessionID := createF7UserWithSession(t, database)
			ensureAdminPolicyForUser(t, database, user.ID)
			llibreID, _ := createF7LlibreWithPagina(t, database, user.ID)
			templateID := createF3210Template(t, database, user.ID, "metrics-"+cfg.Label)

			prevDBConfig := cnf.Config
			prevDBLogLevel := ""
			if cnf.Config == nil {
				cnf.Config = map[string]string{}
			} else {
				prevDBLogLevel = cnf.Config["LOG_LEVEL"]
			}
			cnf.Config["LOG_LEVEL"] = "debug"
			core.SetLogLevel("debug")
			defer func() {
				core.SetLogLevel("error")
				if prevDBConfig == nil {
					cnf.Config = nil
				} else {
					cnf.Config["LOG_LEVEL"] = prevDBLogLevel
				}
			}()

			buf, restore := captureStandardLog(t)
			defer restore()

			rows := []string{"llibre_id,tipus_acte,pagina,cognoms,pare,mare,nascut,acte"}
			for i := 0; i < 240; i++ {
				rows = append(rows, strings.Join([]string{
					strconv.Itoa(llibreID),
					"baptisme",
					"1",
					"Garcia Soler Joan" + strconv.Itoa(i),
					"Pere Garcia" + strconv.Itoa(i%17),
					"Maria Puig" + strconv.Itoa(i%19),
					strconv.Itoa(1+(i%28)) + "/02/1890",
					strconv.Itoa(1+(i%28)) + "/03/1890",
				}, ","))
			}

			req := buildImportGlobalRequest(t, sessionID, "csrf-f3210-metrics-"+cfg.Label, map[string]string{
				"model":       "template",
				"template_id": strconv.Itoa(templateID),
				"separator":   ",",
			}, strings.Join(rows, "\n"))
			rr := httptest.NewRecorder()
			app.AdminImportRegistresGlobal(rr, req)
			if rr.Code != http.StatusSeeOther {
				t.Fatalf("[%s] status inesperat: %d body=%s", cfg.Label, rr.Code, rr.Body.String())
			}

			logs := buf.String()
			var importLine string
			for _, line := range strings.Split(logs, "\n") {
				if strings.Contains(line, "registre import model=") && strings.Contains(line, "write_duplicate_check_dur=") {
					importLine = line
				}
			}
			if importLine == "" {
				t.Fatalf("[%s] no s'ha trobat el log d'import amb write_duplicate_check_dur: %s", cfg.Label, logs)
			}
			writeDuplicateCheckMatch := writeDuplicateCheckRe.FindStringSubmatch(importLine)
			writeDurMatch := writeDurRe.FindStringSubmatch(importLine)
			resolveDurMatch := resolveDurRe.FindStringSubmatch(importLine)
			if len(writeDuplicateCheckMatch) != 2 || len(writeDurMatch) != 2 || len(resolveDurMatch) != 2 {
				t.Fatalf("[%s] no s'han pogut parsejar les mètriques d'import: %s", cfg.Label, importLine)
			}
			writeDuplicateCheckDur, err := time.ParseDuration(writeDuplicateCheckMatch[1])
			if err != nil || writeDuplicateCheckDur <= 0 {
				t.Fatalf("[%s] write_duplicate_check_dur invàlid (%q): %v", cfg.Label, writeDuplicateCheckMatch[1], err)
			}
			if _, err := time.ParseDuration(writeDurMatch[1]); err != nil {
				t.Fatalf("[%s] write_dur invàlid (%q): %v", cfg.Label, writeDurMatch[1], err)
			}
			if _, err := time.ParseDuration(resolveDurMatch[1]); err != nil {
				t.Fatalf("[%s] resolve_dur invàlid (%q): %v", cfg.Label, resolveDurMatch[1], err)
			}
			t.Logf("[%s] rows=240 write_duplicate_check_dur=%s write_dur=%s resolve_dur=%s", cfg.Label, writeDuplicateCheckMatch[1], writeDurMatch[1], resolveDurMatch[1])
		})
	}
}

func createF3210Template(t *testing.T, database db.DB, userID int, label string) int {
	t.Helper()

	templateID, err := database.CreateCSVImportTemplate(&db.CSVImportTemplate{
		Name:             "Template F3210 " + label,
		OwnerUserID:      sqlNullFromInt(userID),
		Visibility:       "private",
		DefaultSeparator: ",",
		ModelJSON:        f3210TemplateModelJSON(),
	})
	if err != nil || templateID == 0 {
		t.Fatalf("[%s] CreateCSVImportTemplate ha fallat: %v", label, err)
	}
	return templateID
}

func createF3210ExistingStrongBaptisme(t *testing.T, database db.DB, llibreID, paginaID int) int {
	t.Helper()

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
		t.Fatalf("CreateTranscripcioRaw existent ha fallat: %v", err)
	}
	for _, p := range []db.TranscripcioPersonaRaw{
		{TranscripcioID: existingID, Rol: "batejat", Nom: "Joan", Cognom1: "Garcia", Cognom2: "Soler"},
		{TranscripcioID: existingID, Rol: "pare", Nom: "Pere", Cognom1: "Garcia"},
		{TranscripcioID: existingID, Rol: "mare", Nom: "Maria", Cognom1: "Puig"},
	} {
		persona := p
		if _, err := database.CreateTranscripcioPersona(&persona); err != nil {
			t.Fatalf("CreateTranscripcioPersona %s ha fallat: %v", persona.Rol, err)
		}
	}
	for _, attr := range []db.TranscripcioAtributRaw{
		{TranscripcioID: existingID, Clau: "pagina_digital", TipusValor: "text", ValorText: "1", Estat: "clar"},
		{TranscripcioID: existingID, Clau: "data_bateig", TipusValor: "date", ValorDate: sql.NullString{String: "1890-02-05", Valid: true}, Estat: "clar"},
		{TranscripcioID: existingID, Clau: "data_naixement", TipusValor: "date", ValorDate: sql.NullString{String: "1890-02-01", Valid: true}, Estat: "clar"},
	} {
		attribute := attr
		if _, err := database.CreateTranscripcioAtribut(&attribute); err != nil {
			t.Fatalf("CreateTranscripcioAtribut %s ha fallat: %v", attribute.Clau, err)
		}
	}
	return existingID
}

func f3210TemplateModelJSON() string {
	return `{
  "version": 1,
  "kind": "transcripcions_raw",
  "book_resolution": { "mode": "llibre_id", "column": "llibre_id" },
  "mapping": {
    "columns": [
      { "header": "llibre_id", "key": "llibre_id", "required": true, "map_to": [{ "target": "base.llibre_id" }] },
      { "header": "tipus_acte", "key": "tipus_acte", "required": true, "map_to": [{ "target": "base.tipus_acte" }] },
      { "header": "pagina", "key": "pagina",
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
}
