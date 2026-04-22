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
	"github.com/marcmoiagese/CercaGenealogica/db"
)

type f3211PageLookupCountingDB struct {
	db.DB
	listCalls     int
	getByIDCalls  int
	getByNumCalls int
}

func (d *f3211PageLookupCountingDB) ListLlibrePagines(llibreID int) ([]db.LlibrePagina, error) {
	d.listCalls++
	return d.DB.ListLlibrePagines(llibreID)
}

func (d *f3211PageLookupCountingDB) GetLlibrePaginaByID(id int) (*db.LlibrePagina, error) {
	d.getByIDCalls++
	return d.DB.GetLlibrePaginaByID(id)
}

func (d *f3211PageLookupCountingDB) GetLlibrePaginaByNum(llibreID, num int) (*db.LlibrePagina, error) {
	d.getByNumCalls++
	return d.DB.GetLlibrePaginaByNum(llibreID, num)
}

func TestTemplateImportPageLookupUsesCachedBookPagesSQLitePostgresF3211(t *testing.T) {
	for _, cfg := range loadSQLiteAndPostgresConfigsForImportHistory(t) {
		cfg := cfg
		t.Run(cfg.Label, func(t *testing.T) {
			app, database := newTestAppForConfig(t, cfg.Config)
			user, sessionID := createF7UserWithSession(t, database)
			ensureAdminPolicyForUser(t, database, user.ID)
			llibreID, _ := createF7LlibreWithPagina(t, database, user.ID)

			countingDB := &f3211PageLookupCountingDB{DB: database}
			app.DB = countingDB

			templateID := createF3210Template(t, database, user.ID, "f3211-count-"+cfg.Label)
			rows := []string{"llibre_id,tipus_acte,pagina,cognoms,pare,mare,nascut,acte"}
			for i := 0; i < 60; i++ {
				rows = append(rows, strings.Join([]string{
					strconv.Itoa(llibreID),
					"baptisme",
					"1",
					"Garcia Soler Joan" + strconv.Itoa(i),
					"Pere Garcia" + strconv.Itoa(i),
					"Maria Puig" + strconv.Itoa(i),
					strconv.Itoa(1+(i%28)) + "/02/1890",
					strconv.Itoa(1+(i%28)) + "/03/1890",
				}, ","))
			}

			req := buildImportGlobalRequest(t, sessionID, "csrf-f3211-count-"+cfg.Label, map[string]string{
				"model":       "template",
				"template_id": strconv.Itoa(templateID),
				"separator":   ",",
			}, strings.Join(rows, "\n"))
			rr := httptest.NewRecorder()
			app.AdminImportRegistresGlobal(rr, req)
			if rr.Code != http.StatusSeeOther {
				t.Fatalf("[%s] status inesperat: %d body=%s", cfg.Label, rr.Code, rr.Body.String())
			}
			if countingDB.listCalls != 1 {
				t.Fatalf("[%s] ListLlibrePagines s'hauria de resoldre un sol cop per llibre, got=%d", cfg.Label, countingDB.listCalls)
			}
			if countingDB.getByIDCalls != 0 || countingDB.getByNumCalls != 0 {
				t.Fatalf("[%s] no s'haurien d'usar lookups puntuals de pàgina, got byID=%d byNum=%d", cfg.Label, countingDB.getByIDCalls, countingDB.getByNumCalls)
			}
		})
	}
}

func TestTemplateImportPageLookupMetricsSQLitePostgresF3211(t *testing.T) {
	writePageLookupRe := regexp.MustCompile(`write_page_lookup_dur=([^ ]+)`)
	writeDurRe := regexp.MustCompile(`write_dur=([^ ]+)`)
	totalDurRe := regexp.MustCompile(`total_dur=([^ ]+)`)

	for _, cfg := range loadSQLiteAndPostgresConfigsForImportHistory(t) {
		cfg := cfg
		t.Run(cfg.Label, func(t *testing.T) {
			app, database := newTestAppForConfig(t, cfg.Config)
			user, sessionID := createF7UserWithSession(t, database)
			ensureAdminPolicyForUser(t, database, user.ID)
			llibreID, _ := createF7LlibreWithPagina(t, database, user.ID)
			templateID := createF3210Template(t, database, user.ID, "f3211-metrics-"+cfg.Label)

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

			req := buildImportGlobalRequest(t, sessionID, "csrf-f3211-metrics-"+cfg.Label, map[string]string{
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
				if strings.Contains(line, "registre import model=") && strings.Contains(line, "write_page_lookup_dur=") {
					importLine = line
				}
			}
			if importLine == "" {
				t.Fatalf("[%s] no s'ha trobat el log d'import amb write_page_lookup_dur: %s", cfg.Label, logs)
			}
			pageLookupMatch := writePageLookupRe.FindStringSubmatch(importLine)
			writeDurMatch := writeDurRe.FindStringSubmatch(importLine)
			totalDurMatch := totalDurRe.FindStringSubmatch(importLine)
			if len(pageLookupMatch) != 2 || len(writeDurMatch) != 2 || len(totalDurMatch) != 2 {
				t.Fatalf("[%s] no s'han pogut parsejar les mètriques d'import: %s", cfg.Label, importLine)
			}
			pageLookupDur, err := time.ParseDuration(pageLookupMatch[1])
			if err != nil || pageLookupDur <= 0 {
				t.Fatalf("[%s] write_page_lookup_dur invàlid (%q): %v", cfg.Label, pageLookupMatch[1], err)
			}
			if _, err := time.ParseDuration(writeDurMatch[1]); err != nil {
				t.Fatalf("[%s] write_dur invàlid (%q): %v", cfg.Label, writeDurMatch[1], err)
			}
			if _, err := time.ParseDuration(totalDurMatch[1]); err != nil {
				t.Fatalf("[%s] total_dur invàlid (%q): %v", cfg.Label, totalDurMatch[1], err)
			}
			t.Logf("[%s] rows=240 write_page_lookup_dur=%s write_dur=%s total_dur=%s", cfg.Label, pageLookupMatch[1], writeDurMatch[1], totalDurMatch[1])
		})
	}
}
