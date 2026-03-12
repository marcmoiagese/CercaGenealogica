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

const benchmarkArxiusCount = 500

func BenchmarkArxiusBulkSQLite(b *testing.B) {
	benchmarkArxiusSQLite(b, true)
}

func BenchmarkArxiusGenericSQLite(b *testing.B) {
	benchmarkArxiusSQLite(b, false)
}

func benchmarkArxiusSQLite(b *testing.B, bulk bool) {
	tmpDir := b.TempDir()
	projectRoot := findProjectRoot(b)
	if err := os.Chdir(projectRoot); err != nil {
		b.Fatalf("no puc fer chdir a l'arrel del projecte (%s): %v", projectRoot, err)
	}
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dbPath := filepath.Join(tmpDir, fmt.Sprintf("bench_arxius_%t_%d.sqlite3", bulk, i))
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
		arxius := makeBenchmarkArxius(benchmarkArxiusCount, munID)
		b.StartTimer()

		if bulk {
			if _, _, err := sqliteDB.BulkInsertArxius(context.Background(), arxius); err != nil {
				b.Fatalf("BulkInsertArxius ha fallat: %v", err)
			}
		} else {
			for idx := range arxius {
				a := arxius[idx]
				if _, err := sqliteDB.CreateArxiu(&a); err != nil {
					b.Fatalf("CreateArxiu ha fallat: %v", err)
				}
			}
		}

		b.StopTimer()
		sqliteDB.Close()
	}
}

func makeBenchmarkArxius(count int, municipiID int) []db.Arxiu {
	if count <= 0 {
		return nil
	}
	res := make([]db.Arxiu, 0, count)
	munID := sql.NullInt64{Int64: int64(municipiID), Valid: municipiID > 0}
	for i := 0; i < count; i++ {
		res = append(res, db.Arxiu{
			Nom:            fmt.Sprintf("Arxiu Bench %d", i),
			Tipus:          "municipal",
			MunicipiID:     munID,
			ModeracioEstat: "pendent",
		})
	}
	return res
}
