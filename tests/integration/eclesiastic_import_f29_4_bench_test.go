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

const benchmarkArquebisbatCount = 500

func BenchmarkEclesiasticArquebisbatsBulkSQLite(b *testing.B) {
	benchmarkEclesiasticArquebisbatsSQLite(b, true)
}

func BenchmarkEclesiasticArquebisbatsGenericSQLite(b *testing.B) {
	benchmarkEclesiasticArquebisbatsSQLite(b, false)
}

func benchmarkEclesiasticArquebisbatsSQLite(b *testing.B, bulk bool) {
	tmpDir := b.TempDir()
	projectRoot := findProjectRoot(b)
	if err := os.Chdir(projectRoot); err != nil {
		b.Fatalf("no puc fer chdir a l'arrel del projecte (%s): %v", projectRoot, err)
	}
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dbPath := filepath.Join(tmpDir, fmt.Sprintf("bench_ecles_%t_%d.sqlite3", bulk, i))
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
		paisID, _ := seedBenchmarkPaisNivell(b, sqliteDB)
		entitats := makeBenchmarkArquebisbats(benchmarkArquebisbatCount, paisID)
		b.StartTimer()

		if bulk {
			if _, _, err := sqliteDB.BulkInsertArquebisbats(context.Background(), entitats); err != nil {
				b.Fatalf("BulkInsertArquebisbats ha fallat: %v", err)
			}
		} else {
			for idx := range entitats {
				e := entitats[idx]
				if _, err := sqliteDB.CreateArquebisbat(&e); err != nil {
					b.Fatalf("CreateArquebisbat ha fallat: %v", err)
				}
			}
		}

		b.StopTimer()
		sqliteDB.Close()
	}
}

func makeBenchmarkArquebisbats(count int, paisID int) []db.Arquebisbat {
	if count <= 0 {
		return nil
	}
	res := make([]db.Arquebisbat, 0, count)
	for i := 0; i < count; i++ {
		res = append(res, db.Arquebisbat{
			Nom:            fmt.Sprintf("Arquebisbat Bench %d", i),
			TipusEntitat:   "arquebisbat",
			PaisID:         sql.NullInt64{Int64: int64(paisID), Valid: paisID > 0},
			ModeracioEstat: "pendent",
		})
	}
	return res
}
