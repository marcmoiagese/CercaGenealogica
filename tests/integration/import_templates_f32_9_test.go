package integration

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type f329CountingDB struct {
	db.DB
	listPersonesCalls int
	listPersonesByIDs int
	listAtributsCalls int
	listAtributsByIDs int
}

func (d *f329CountingDB) ListTranscripcioPersones(transcripcioID int) ([]db.TranscripcioPersonaRaw, error) {
	d.listPersonesCalls++
	return d.DB.ListTranscripcioPersones(transcripcioID)
}

func (d *f329CountingDB) ListTranscripcioPersonesByTranscripcioIDs(transcripcioIDs []int) (map[int][]db.TranscripcioPersonaRaw, error) {
	d.listPersonesByIDs++
	return d.DB.ListTranscripcioPersonesByTranscripcioIDs(transcripcioIDs)
}

func (d *f329CountingDB) ListTranscripcioAtributs(transcripcioID int) ([]db.TranscripcioAtributRaw, error) {
	d.listAtributsCalls++
	return d.DB.ListTranscripcioAtributs(transcripcioID)
}

func (d *f329CountingDB) ListTranscripcioAtributsByTranscripcioIDs(transcripcioIDs []int) (map[int][]db.TranscripcioAtributRaw, error) {
	d.listAtributsByIDs++
	return d.DB.ListTranscripcioAtributsByTranscripcioIDs(transcripcioIDs)
}

func TestRegistreImportSidefxUsesBulkIndexacioFetchSQLitePostgresF329(t *testing.T) {
	for _, cfg := range loadSQLiteAndPostgresConfigsForImportHistory(t) {
		cfg := cfg
		t.Run(cfg.Label, func(t *testing.T) {
			app, database := newTestAppForConfig(t, cfg.Config)
			user, sessionID := createF7UserWithSession(t, database)
			ensureAdminPolicyForUser(t, database, user.ID)
			llibreID, _ := createF7LlibreWithPagina(t, database, user.ID)

			countingDB := &f329CountingDB{DB: database}
			app.DB = countingDB

			req := buildImportGlobalRequest(t, sessionID, "csrf-f329-"+cfg.Label, map[string]string{
				"model":     "generic",
				"separator": ",",
			}, strings.Join([]string{
				"llibre_id,tipus_acte",
				strconv.Itoa(llibreID) + ",baptisme",
				strconv.Itoa(llibreID) + ",obit",
			}, "\n"))
			rr := httptest.NewRecorder()
			app.AdminImportRegistresGlobal(rr, req)
			if rr.Result().StatusCode != http.StatusSeeOther {
				t.Fatalf("[%s] status inesperat: %d body=%s", cfg.Label, rr.Result().StatusCode, rr.Body.String())
			}
			if countingDB.listPersonesByIDs == 0 || countingDB.listAtributsByIDs == 0 {
				t.Fatalf("[%s] el recalcul d'indexació ha d'usar càrrega bulk: persones_by_ids=%d atributs_by_ids=%d", cfg.Label, countingDB.listPersonesByIDs, countingDB.listAtributsByIDs)
			}
			if countingDB.listPersonesCalls != 0 || countingDB.listAtributsCalls != 0 {
				t.Fatalf("[%s] el recalcul d'indexació no ha d'usar càrrega fila-a-fila: persones=%d atributs=%d", cfg.Label, countingDB.listPersonesCalls, countingDB.listAtributsCalls)
			}
		})
	}
}

type f3212Fix2CountingDB struct {
	db.DB
	listPersonesCalls  int
	listPersonesByIDs  int
	listPersonesByBook int
	listAtributsCalls  int
	listAtributsByIDs  int
	listAtributsByBook int
}

func (d *f3212Fix2CountingDB) ListTranscripcioPersones(transcripcioID int) ([]db.TranscripcioPersonaRaw, error) {
	d.listPersonesCalls++
	return d.DB.ListTranscripcioPersones(transcripcioID)
}

func (d *f3212Fix2CountingDB) ListTranscripcioPersonesByTranscripcioIDs(transcripcioIDs []int) (map[int][]db.TranscripcioPersonaRaw, error) {
	d.listPersonesByIDs++
	return d.DB.ListTranscripcioPersonesByTranscripcioIDs(transcripcioIDs)
}

func (d *f3212Fix2CountingDB) ListTranscripcioPersonesByLlibreID(llibreID int) (map[int][]db.TranscripcioPersonaRaw, error) {
	d.listPersonesByBook++
	loader, ok := d.DB.(interface {
		ListTranscripcioPersonesByLlibreID(llibreID int) (map[int][]db.TranscripcioPersonaRaw, error)
	})
	if !ok {
		return nil, nil
	}
	return loader.ListTranscripcioPersonesByLlibreID(llibreID)
}

func (d *f3212Fix2CountingDB) ListTranscripcioAtributs(transcripcioID int) ([]db.TranscripcioAtributRaw, error) {
	d.listAtributsCalls++
	return d.DB.ListTranscripcioAtributs(transcripcioID)
}

func (d *f3212Fix2CountingDB) ListTranscripcioAtributsByTranscripcioIDs(transcripcioIDs []int) (map[int][]db.TranscripcioAtributRaw, error) {
	d.listAtributsByIDs++
	return d.DB.ListTranscripcioAtributsByTranscripcioIDs(transcripcioIDs)
}

func (d *f3212Fix2CountingDB) ListTranscripcioAtributsByLlibreID(llibreID int) (map[int][]db.TranscripcioAtributRaw, error) {
	d.listAtributsByBook++
	loader, ok := d.DB.(interface {
		ListTranscripcioAtributsByLlibreID(llibreID int) (map[int][]db.TranscripcioAtributRaw, error)
	})
	if !ok {
		return nil, nil
	}
	return loader.ListTranscripcioAtributsByLlibreID(llibreID)
}

func TestRegistreImportSidefxUsesBookScopedBulkFetchPostgresF3212Fix2(t *testing.T) {
	for _, cfg := range loadSQLiteAndPostgresConfigsForImportHistory(t) {
		if cfg.Engine != "postgres" {
			continue
		}
		cfg := cfg
		t.Run(cfg.Label, func(t *testing.T) {
			app, database := newTestAppForConfig(t, cfg.Config)
			user, sessionID := createF7UserWithSession(t, database)
			ensureAdminPolicyForUser(t, database, user.ID)
			llibreID, _ := createF7LlibreWithPagina(t, database, user.ID)

			countingDB := &f3212Fix2CountingDB{DB: database}
			app.DB = countingDB

			req := buildImportGlobalRequest(t, sessionID, "csrf-f3212-fix2-"+cfg.Label, map[string]string{
				"model":     "generic",
				"separator": ",",
			}, strings.Join([]string{
				"llibre_id,tipus_acte",
				strconv.Itoa(llibreID) + ",baptisme",
				strconv.Itoa(llibreID) + ",obit",
			}, "\n"))
			rr := httptest.NewRecorder()
			app.AdminImportRegistresGlobal(rr, req)
			if rr.Result().StatusCode != http.StatusSeeOther {
				t.Fatalf("[%s] status inesperat: %d body=%s", cfg.Label, rr.Result().StatusCode, rr.Body.String())
			}
			if countingDB.listPersonesByBook == 0 || countingDB.listAtributsByBook == 0 {
				t.Fatalf("[%s] PostgreSQL ha d'usar càrrega bulk per llibre: persones_by_book=%d atributs_by_book=%d", cfg.Label, countingDB.listPersonesByBook, countingDB.listAtributsByBook)
			}
			if countingDB.listPersonesByIDs != 0 || countingDB.listAtributsByIDs != 0 {
				t.Fatalf("[%s] PostgreSQL no hauria de recórrer a by_ids quan existeix el camí per llibre: persones_by_ids=%d atributs_by_ids=%d", cfg.Label, countingDB.listPersonesByIDs, countingDB.listAtributsByIDs)
			}
			if countingDB.listPersonesCalls != 0 || countingDB.listAtributsCalls != 0 {
				t.Fatalf("[%s] PostgreSQL no ha d'usar càrrega fila-a-fila: persones=%d atributs=%d", cfg.Label, countingDB.listPersonesCalls, countingDB.listAtributsCalls)
			}
		})
	}
}
