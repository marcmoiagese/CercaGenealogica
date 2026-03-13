package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const benchmarkLlibresCount = 500

func BenchmarkLlibresBulkSQLite(b *testing.B) {
	benchmarkLlibresSQLite(b, true)
}

func BenchmarkLlibresGenericSQLite(b *testing.B) {
	benchmarkLlibresSQLite(b, false)
}

func benchmarkLlibresSQLite(b *testing.B, bulk bool) {
	tmpDir := b.TempDir()
	projectRoot := findProjectRoot(b)
	if err := os.Chdir(projectRoot); err != nil {
		b.Fatalf("no puc fer chdir a l'arrel del projecte (%s): %v", projectRoot, err)
	}
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dbPath := filepath.Join(tmpDir, fmt.Sprintf("bench_llibres_%t_%d.sqlite3", bulk, i))
		cfg := map[string]string{
			"DB_ENGINE": "sqlite",
			"DB_PATH":   dbPath,
			"RECREADB":  "true",
			"LOG_LEVEL": "silent",
		}
		dbInstance, err := db.NewDB(cfg)
		if err != nil {
			b.Fatalf("NewDB ha fallat: %v", err)
		}
		sqliteDB, ok := dbInstance.(*db.SQLite)
		if !ok {
			b.Fatalf("DB no es SQLite")
		}
		_, nivellID := seedBenchmarkPaisNivell(b, sqliteDB)
		mun := &db.Municipi{
			Nom:            "Municipi Bench",
			Tipus:          "municipi",
			Estat:          "actiu",
			ModeracioEstat: "pendent",
		}
		mun.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(nivellID), Valid: true}
		munID, err := sqliteDB.CreateMunicipi(mun)
		if err != nil {
			b.Fatalf("CreateMunicipi ha fallat: %v", err)
		}
		llibres := makeBenchmarkLlibres(benchmarkLlibresCount, munID)
		b.StartTimer()

		if bulk {
			if _, _, err := sqliteDB.BulkInsertLlibres(context.Background(), llibres); err != nil {
				b.Fatalf("BulkInsertLlibres ha fallat: %v", err)
			}
		} else {
			for idx := range llibres {
				l := llibres[idx]
				if _, err := sqliteDB.CreateLlibre(&l); err != nil {
					b.Fatalf("CreateLlibre ha fallat: %v", err)
				}
			}
		}

		b.StopTimer()
		sqliteDB.Close()
	}
}

func makeBenchmarkLlibres(count int, municipiID int) []db.Llibre {
	if count <= 0 {
		return nil
	}
	res := make([]db.Llibre, 0, count)
	for i := 0; i < count; i++ {
		res = append(res, db.Llibre{
			MunicipiID:     municipiID,
			TipusLlibre:    "baptismes",
			Cronologia:     fmt.Sprintf("1900-%d", 1900+i),
			CodiDigital:    fmt.Sprintf("CD-%d", i),
			ModeracioEstat: "pendent",
		})
	}
	return res
}
