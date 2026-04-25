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
	listPersonesCalls  int
	listPersonesByIDs  int
	listPersonesByBook int
	listAtributsCalls  int
	listAtributsByIDs  int
	listAtributsByBook int
}

func (d *f329CountingDB) ListTranscripcioPersones(transcripcioID int) ([]db.TranscripcioPersonaRaw, error) {
	d.listPersonesCalls++
	return d.DB.ListTranscripcioPersones(transcripcioID)
}

func (d *f329CountingDB) ListTranscripcioPersonesByTranscripcioIDs(transcripcioIDs []int) (map[int][]db.TranscripcioPersonaRaw, error) {
	d.listPersonesByIDs++
	return d.DB.ListTranscripcioPersonesByTranscripcioIDs(transcripcioIDs)
}

func (d *f329CountingDB) ListTranscripcioPersonesByLlibreID(llibreID int) (map[int][]db.TranscripcioPersonaRaw, error) {
	d.listPersonesByBook++
	return d.DB.ListTranscripcioPersonesByLlibreID(llibreID)
}

func (d *f329CountingDB) ListTranscripcioAtributs(transcripcioID int) ([]db.TranscripcioAtributRaw, error) {
	d.listAtributsCalls++
	return d.DB.ListTranscripcioAtributs(transcripcioID)
}

func (d *f329CountingDB) ListTranscripcioAtributsByTranscripcioIDs(transcripcioIDs []int) (map[int][]db.TranscripcioAtributRaw, error) {
	d.listAtributsByIDs++
	return d.DB.ListTranscripcioAtributsByTranscripcioIDs(transcripcioIDs)
}

func (d *f329CountingDB) ListTranscripcioAtributsByLlibreID(llibreID int) (map[int][]db.TranscripcioAtributRaw, error) {
	d.listAtributsByBook++
	return d.DB.ListTranscripcioAtributsByLlibreID(llibreID)
}

func TestRegistreImportSidefxUsesBulkIndexacioFetchSQLitePostgresF329(t *testing.T) {
	for _, cfg := range loadSQLitePostgresAndMySQLConfigsForImportHistory(t) {
		cfg := cfg
		t.Run(cfg.Label, func(t *testing.T) {
			app, database := newTestAppForConfigOrSkipMySQL(t, cfg.Config)
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
			if countingDB.listPersonesCalls != 0 || countingDB.listAtributsCalls != 0 {
				t.Fatalf("[%s] el recalcul d'indexació no ha d'usar càrrega fila-a-fila: persones=%d atributs=%d", cfg.Label, countingDB.listPersonesCalls, countingDB.listAtributsCalls)
			}
			if cfg.Engine == "postgres" {
				if countingDB.listPersonesByIDs == 0 || countingDB.listAtributsByIDs == 0 {
					t.Fatalf("[%s] PostgreSQL ha d'usar càrrega bulk per IDs al sidefx: persones_by_ids=%d atributs_by_ids=%d", cfg.Label, countingDB.listPersonesByIDs, countingDB.listAtributsByIDs)
				}
				if countingDB.listPersonesByBook != 0 || countingDB.listAtributsByBook != 0 {
					t.Fatalf("[%s] PostgreSQL no hauria de carregar tot el llibre al sidefx quan ja té els IDs exactes: persones_by_book=%d atributs_by_book=%d", cfg.Label, countingDB.listPersonesByBook, countingDB.listAtributsByBook)
				}
			} else if cfg.Engine == "mysql" {
				if countingDB.listPersonesByBook == 0 || countingDB.listAtributsByBook == 0 {
					t.Fatalf("[%s] MySQL ha d'usar càrrega bulk per llibre: persones_by_book=%d atributs_by_book=%d", cfg.Label, countingDB.listPersonesByBook, countingDB.listAtributsByBook)
				}
				if countingDB.listPersonesByIDs != 0 || countingDB.listAtributsByIDs != 0 {
					t.Fatalf("[%s] MySQL no hauria de recórrer a by_ids quan el runtime específic per llibre és disponible: persones_by_ids=%d atributs_by_ids=%d", cfg.Label, countingDB.listPersonesByIDs, countingDB.listAtributsByIDs)
				}
			} else {
				if countingDB.listPersonesByIDs == 0 || countingDB.listAtributsByIDs == 0 {
					t.Fatalf("[%s] SQLite ha de mantenir la càrrega bulk per IDs: persones_by_ids=%d atributs_by_ids=%d", cfg.Label, countingDB.listPersonesByIDs, countingDB.listAtributsByIDs)
				}
			}
		})
	}
}

type f3212Fix2CountingDB struct {
	db.DB
	listStrongCandidatesCalls int
	listTranscripcionsCalls   int
	listPersonesCalls         int
	listPersonesByIDs         int
	listPersonesByBook        int
	listAtributsCalls         int
	listAtributsByIDs         int
	listAtributsByBook        int
}

func (d *f3212Fix2CountingDB) ListTranscripcionsRaw(llibreID int, f db.TranscripcioFilter) ([]db.TranscripcioRaw, error) {
	d.listTranscripcionsCalls++
	return d.DB.ListTranscripcionsRaw(llibreID, f)
}

func (d *f3212Fix2CountingDB) ListTranscripcioStrongMatchCandidates(bookID int, tipusActe, pageKey string) ([]db.TranscripcioRaw, map[int][]db.TranscripcioPersonaRaw, map[int][]db.TranscripcioAtributRaw, error) {
	d.listStrongCandidatesCalls++
	loader, ok := d.DB.(interface {
		ListTranscripcioStrongMatchCandidates(bookID int, tipusActe, pageKey string) ([]db.TranscripcioRaw, map[int][]db.TranscripcioPersonaRaw, map[int][]db.TranscripcioAtributRaw, error)
	})
	if !ok {
		return nil, nil, nil, nil
	}
	return loader.ListTranscripcioStrongMatchCandidates(bookID, tipusActe, pageKey)
}

func (d *f3212Fix2CountingDB) ListTranscripcioStrongMatchCandidatesUpToID(bookID int, tipusActe, pageKey string, maxExistingID int) ([]db.TranscripcioRaw, map[int][]db.TranscripcioPersonaRaw, map[int][]db.TranscripcioAtributRaw, error) {
	d.listStrongCandidatesCalls++
	loader, ok := d.DB.(interface {
		ListTranscripcioStrongMatchCandidatesUpToID(bookID int, tipusActe, pageKey string, maxExistingID int) ([]db.TranscripcioRaw, map[int][]db.TranscripcioPersonaRaw, map[int][]db.TranscripcioAtributRaw, error)
	})
	if !ok {
		return nil, nil, nil, nil
	}
	return loader.ListTranscripcioStrongMatchCandidatesUpToID(bookID, tipusActe, pageKey, maxExistingID)
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

func (d *f3212Fix2CountingDB) GetMaxTranscripcioRawID() (int, error) {
	loader, ok := d.DB.(interface {
		GetMaxTranscripcioRawID() (int, error)
	})
	if !ok {
		return 0, nil
	}
	return loader.GetMaxTranscripcioRawID()
}

func TestRegistreImportSidefxUsesIDScopedBulkFetchPostgresF3212Fix2(t *testing.T) {
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
			if countingDB.listPersonesByIDs == 0 || countingDB.listAtributsByIDs == 0 {
				t.Fatalf("[%s] PostgreSQL ha d'usar càrrega bulk per IDs al sidefx: persones_by_ids=%d atributs_by_ids=%d", cfg.Label, countingDB.listPersonesByIDs, countingDB.listAtributsByIDs)
			}
			if countingDB.listPersonesByBook != 0 || countingDB.listAtributsByBook != 0 {
				t.Fatalf("[%s] PostgreSQL no hauria de recórrer al carregador per llibre al sidefx quan ja té els IDs exactes: persones_by_book=%d atributs_by_book=%d", cfg.Label, countingDB.listPersonesByBook, countingDB.listAtributsByBook)
			}
			if countingDB.listPersonesCalls != 0 || countingDB.listAtributsCalls != 0 {
				t.Fatalf("[%s] PostgreSQL no ha d'usar càrrega fila-a-fila: persones=%d atributs=%d", cfg.Label, countingDB.listPersonesCalls, countingDB.listAtributsCalls)
			}
		})
	}
}

func TestTemplateImportStrongDedupUsesPageScopedCandidatesPostgresF3212Fix2(t *testing.T) {
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

			for pageNum := 2; pageNum <= 13; pageNum++ {
				if _, err := database.SaveLlibrePagina(&db.LlibrePagina{
					LlibreID:  llibreID,
					NumPagina: pageNum,
					Estat:     "indexada",
				}); err != nil {
					t.Fatalf("[%s] SaveLlibrePagina(%d) ha fallat: %v", cfg.Label, pageNum, err)
				}
			}

			for pageNum := 1; pageNum <= 12; pageNum++ {
				pagina, err := database.GetLlibrePaginaByNum(llibreID, pageNum)
				if err != nil || pagina == nil {
					t.Fatalf("[%s] GetLlibrePaginaByNum(%d) ha fallat: %v", cfg.Label, pageNum, err)
				}
				createF3210ExistingStrongBaptismeForPage(t, database, llibreID, pageNum, pagina.ID)
			}

			countingDB := &f3212Fix2CountingDB{DB: database}
			app.DB = countingDB
			templateID := createF3210Template(t, database, user.ID, "f3212-fix2-pages-"+cfg.Label)

			rows := []string{"llibre_id,tipus_acte,pagina,cognoms,pare,mare,nascut,acte"}
			for pageNum := 1; pageNum <= 12; pageNum++ {
				rows = append(rows, strings.Join([]string{
					strconv.Itoa(llibreID),
					"baptisme",
					strconv.Itoa(pageNum),
					"Garcia Soler Joan" + strconv.Itoa(100+pageNum),
					"Pere Garcia" + strconv.Itoa(100+pageNum),
					"Maria Puig" + strconv.Itoa(100+pageNum),
					"01/02/1890",
					"05/02/1890",
				}, ","))
			}

			req := buildImportGlobalRequest(t, sessionID, "csrf-f3212-fix2-pages-"+cfg.Label, map[string]string{
				"model":       "template",
				"template_id": strconv.Itoa(templateID),
				"separator":   ",",
			}, strings.Join(rows, "\n"))
			rr := httptest.NewRecorder()
			app.AdminImportRegistresGlobal(rr, req)
			if rr.Result().StatusCode != http.StatusSeeOther {
				t.Fatalf("[%s] status inesperat: %d body=%s", cfg.Label, rr.Result().StatusCode, rr.Body.String())
			}
			if countingDB.listStrongCandidatesCalls == 0 {
				t.Fatalf("[%s] PostgreSQL ha d'usar candidats forts acotats per pàgina", cfg.Label)
			}
			if countingDB.listTranscripcionsCalls > 1 {
				t.Fatalf("[%s] PostgreSQL no hauria d'usar ListTranscripcionsRaw ampli fora del read lateral esperat: %d", cfg.Label, countingDB.listTranscripcionsCalls)
			}
			if countingDB.listPersonesCalls != 0 || countingDB.listAtributsCalls != 0 {
				t.Fatalf("[%s] el camí acotat no ha d'anar fila-a-fila: persones=%d atributs=%d", cfg.Label, countingDB.listPersonesCalls, countingDB.listAtributsCalls)
			}
		})
	}
}
