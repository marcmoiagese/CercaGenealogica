package integration

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const benchmarkMunicipiCount = 500

func BenchmarkTerritoriMunicipisBulkSQLite(b *testing.B) {
	benchmarkTerritoriMunicipisSQLite(b, true)
}

func BenchmarkTerritoriMunicipisGenericSQLite(b *testing.B) {
	benchmarkTerritoriMunicipisSQLite(b, false)
}

func benchmarkTerritoriMunicipisSQLite(b *testing.B, bulk bool) {
	tmpDir := b.TempDir()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dbPath := filepath.Join(tmpDir, fmt.Sprintf("bench_territori_%t_%d.sqlite3", bulk, i))
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
		municipis := makeBenchmarkMunicipis(benchmarkMunicipiCount, nivellID)
		b.StartTimer()

		if bulk {
			if _, _, err := sqliteDB.BulkInsertMunicipis(context.Background(), municipis); err != nil {
				b.Fatalf("BulkInsertMunicipis ha fallat: %v", err)
			}
		} else {
			for idx := range municipis {
				m := municipis[idx]
				if _, err := sqliteDB.CreateMunicipi(&m); err != nil {
					b.Fatalf("CreateMunicipi ha fallat: %v", err)
				}
			}
		}

		b.StopTimer()
		sqliteDB.Close()
	}
}

func seedBenchmarkPaisNivell(b *testing.B, database db.DB) (int, int) {
	b.Helper()
	pais := &db.Pais{
		CodiISO2:    "TB",
		CodiISO3:    "TBB",
		CodiPaisNum: "990",
	}
	paisID, err := database.CreatePais(pais)
	if err != nil {
		b.Fatalf("CreatePais ha fallat: %v", err)
	}
	nivell := &db.NivellAdministratiu{
		PaisID:         paisID,
		Nivel:          1,
		NomNivell:      "Test Bench",
		TipusNivell:    "pais",
		CodiOficial:    "TB-1",
		Estat:          "actiu",
		ModeracioEstat: "pendent",
	}
	nivellID, err := database.CreateNivell(nivell)
	if err != nil {
		b.Fatalf("CreateNivell ha fallat: %v", err)
	}
	return paisID, nivellID
}

func makeBenchmarkMunicipis(count int, nivellID int) []db.Municipi {
	if count <= 0 {
		return nil
	}
	res := make([]db.Municipi, 0, count)
	for i := 0; i < count; i++ {
		m := db.Municipi{
			Nom:            fmt.Sprintf("Municipi Bench %d", i),
			Tipus:          "municipi",
			CodiPostal:     fmt.Sprintf("B%04d", i),
			Estat:          "actiu",
			ModeracioEstat: "pendent",
		}
		m.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(nivellID), Valid: true}
		res = append(res, m)
	}
	return res
}
