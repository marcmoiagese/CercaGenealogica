package integration

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/cnf"
	"github.com/marcmoiagese/CercaGenealogica/core"
)

func TestTemplateImportPersonaPersistMetricsSQLitePostgresF3212(t *testing.T) {
	writePersonaPersistRe := regexp.MustCompile(`write_persona_persist_dur=([^ ]+)`)
	writeTranscripcioInsertRe := regexp.MustCompile(`write_transcripcio_insert_dur=([^ ]+)`)
	writeBulkTranscripcioBatchesRe := regexp.MustCompile(`write_bulk_transcripcio_batches=([0-9]+)`)
	writeBulkPersonaBatchesRe := regexp.MustCompile(`write_bulk_persona_batches=([0-9]+)`)
	writeBulkLinksBatchesRe := regexp.MustCompile(`write_bulk_links_batches=([0-9]+)`)

	for _, cfg := range loadSQLiteAndPostgresConfigsForImportHistory(t) {
		cfg := cfg
		t.Run(cfg.Label, func(t *testing.T) {
			app, database := newTestAppForConfig(t, cfg.Config)
			user, sessionID := createF7UserWithSession(t, database)
			ensureAdminPolicyForUser(t, database, user.ID)
			llibreID, _ := createF7LlibreWithPagina(t, database, user.ID)
			templateID := createF3210Template(t, database, user.ID, "f3212-metrics-"+cfg.Label)

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

			req := buildImportGlobalRequest(t, sessionID, "csrf-f3212-metrics-"+cfg.Label, map[string]string{
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
				if strings.Contains(line, "registre import model=") && strings.Contains(line, "write_persona_persist_dur=") {
					importLine = line
				}
			}
			if importLine == "" {
				t.Fatalf("[%s] no s'ha trobat el log d'import amb write_persona_persist_dur: %s", cfg.Label, logs)
			}

			personaMatch := writePersonaPersistRe.FindStringSubmatch(importLine)
			transcripcioMatch := writeTranscripcioInsertRe.FindStringSubmatch(importLine)
			transcripcioBatchMatch := writeBulkTranscripcioBatchesRe.FindStringSubmatch(importLine)
			personaBatchMatch := writeBulkPersonaBatchesRe.FindStringSubmatch(importLine)
			linksBatchMatch := writeBulkLinksBatchesRe.FindStringSubmatch(importLine)
			if len(personaMatch) != 2 || len(transcripcioMatch) != 2 || len(transcripcioBatchMatch) != 2 || len(personaBatchMatch) != 2 || len(linksBatchMatch) != 2 {
				t.Fatalf("[%s] no s'han pogut parsejar les mètriques F32-12: %s", cfg.Label, importLine)
			}

			personaDur, err := time.ParseDuration(personaMatch[1])
			if err != nil || personaDur <= 0 {
				t.Fatalf("[%s] write_persona_persist_dur invàlid (%q): %v", cfg.Label, personaMatch[1], err)
			}
			if _, err := time.ParseDuration(transcripcioMatch[1]); err != nil {
				t.Fatalf("[%s] write_transcripcio_insert_dur invàlid (%q): %v", cfg.Label, transcripcioMatch[1], err)
			}
			transcripcioBatches, err := strconv.Atoi(transcripcioBatchMatch[1])
			if err != nil || transcripcioBatches <= 0 {
				t.Fatalf("[%s] write_bulk_transcripcio_batches invàlid (%q): %v", cfg.Label, transcripcioBatchMatch[1], err)
			}
			personaBatches, err := strconv.Atoi(personaBatchMatch[1])
			if err != nil || personaBatches <= 0 {
				t.Fatalf("[%s] write_bulk_persona_batches invàlid (%q): %v", cfg.Label, personaBatchMatch[1], err)
			}
			linksBatches, err := strconv.Atoi(linksBatchMatch[1])
			if err != nil || linksBatches <= 0 {
				t.Fatalf("[%s] write_bulk_links_batches invàlid (%q): %v", cfg.Label, linksBatchMatch[1], err)
			}

			if cfg.Engine == "postgres" {
				if transcripcioBatches != 2 || personaBatches != 1 || linksBatches != 1 {
					t.Fatalf("[%s] PostgreSQL hauria d'usar batching específic per taula amb 240 files, got trans=%d persones=%d atributs=%d", cfg.Label, transcripcioBatches, personaBatches, linksBatches)
				}
			} else {
				if transcripcioBatches != 240 || personaBatches != 720 || linksBatches != 720 {
					t.Fatalf("[%s] SQLite hauria de mantenir el camí fila-a-fila instrumentat, got trans=%d persones=%d atributs=%d", cfg.Label, transcripcioBatches, personaBatches, linksBatches)
				}
			}

			t.Logf("[%s] rows=240 write_persona_persist_dur=%s write_transcripcio_insert_dur=%s write_bulk_transcripcio_batches=%d write_bulk_persona_batches=%d write_bulk_links_batches=%d", cfg.Label, personaMatch[1], transcripcioMatch[1], transcripcioBatches, personaBatches, linksBatches)
		})
	}
}
