package integration

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type f32132BulkPersistCountingDB struct {
	db.DB
	bulkCalls          int
	createRawCalls     int
	createPersonaCalls int
	createAtributCalls int
}

func (d *f32132BulkPersistCountingDB) BulkCreateTranscripcioRawBundles(rows []db.TranscripcioRawImportBundle) (db.TranscripcioRawImportBulkResult, error) {
	d.bulkCalls++
	return d.DB.BulkCreateTranscripcioRawBundles(rows)
}

func (d *f32132BulkPersistCountingDB) CreateTranscripcioRaw(row *db.TranscripcioRaw) (int, error) {
	d.createRawCalls++
	return d.DB.CreateTranscripcioRaw(row)
}

func (d *f32132BulkPersistCountingDB) CreateTranscripcioPersona(row *db.TranscripcioPersonaRaw) (int, error) {
	d.createPersonaCalls++
	return d.DB.CreateTranscripcioPersona(row)
}

func (d *f32132BulkPersistCountingDB) CreateTranscripcioAtribut(row *db.TranscripcioAtributRaw) (int, error) {
	d.createAtributCalls++
	return d.DB.CreateTranscripcioAtribut(row)
}

func TestTemplateImportPersistsThroughRuntimeBulkBundlesSQLitePostgresF32132(t *testing.T) {
	for _, cfg := range loadSQLitePostgresAndMySQLConfigsForImportHistory(t) {
		cfg := cfg
		t.Run(cfg.Label, func(t *testing.T) {
			app, database := newTestAppForConfigOrSkipMySQL(t, cfg.Config)
			user, sessionID := createF7UserWithSession(t, database)
			ensureAdminPolicyForUser(t, database, user.ID)
			llibreID, _ := createF7LlibreWithPagina(t, database, user.ID)

			countingDB := &f32132BulkPersistCountingDB{DB: database}
			app.DB = countingDB

			templateID := createF3210Template(t, database, user.ID, "f32132-bulk-"+cfg.Label)
			rows := strings.Join([]string{
				"llibre_id,tipus_acte,pagina,cognoms,pare,mare,nascut,acte",
				strconv.Itoa(llibreID) + ",baptisme,1,Garcia Soler Joan,Pere Garcia,Maria Puig,01/02/1890,05/02/1890",
				strconv.Itoa(llibreID) + ",baptisme,1,Garcia Soler Pau,Pau Garcia,Anna Puig,02/02/1890,06/02/1890",
			}, "\n")

			req := buildImportGlobalRequest(t, sessionID, "csrf-f32132-"+cfg.Label, map[string]string{
				"model":       "template",
				"template_id": strconv.Itoa(templateID),
				"separator":   ",",
			}, rows)
			rr := httptest.NewRecorder()
			app.AdminImportRegistresGlobal(rr, req)
			if rr.Code != http.StatusSeeOther {
				t.Fatalf("[%s] status inesperat: %d body=%s", cfg.Label, rr.Code, rr.Body.String())
			}
			if countingDB.bulkCalls == 0 {
				t.Fatalf("[%s] el pipeline d'import ha de persistir per BulkCreateTranscripcioRawBundles", cfg.Label)
			}
			if countingDB.createRawCalls != 0 || countingDB.createPersonaCalls != 0 || countingDB.createAtributCalls != 0 {
				t.Fatalf("[%s] el pipeline d'import no ha de caure al camí fila-a-fila: raw=%d persones=%d atributs=%d", cfg.Label, countingDB.createRawCalls, countingDB.createPersonaCalls, countingDB.createAtributCalls)
			}
		})
	}
}
