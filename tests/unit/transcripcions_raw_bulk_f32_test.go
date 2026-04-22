package unit

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
	testcommon "github.com/marcmoiagese/CercaGenealogica/tests/common"
)

type f325BulkCreateDB interface {
	BulkCreateTranscripcioRawBundles([]db.TranscripcioRawImportBundle) (db.TranscripcioRawImportBulkResult, error)
}

func TestF325BulkCreateTranscripcioRawBundlesSQLitePostgres(t *testing.T) {
	ensureF325ProjectRoot(t)
	for _, cfg := range f325SQLiteAndPostgresConfigs(t) {
		cfg := cfg
		t.Run(cfg.Label, func(t *testing.T) {
			database, err := db.NewDB(cfg.Config)
			if err != nil {
				t.Fatalf("[%s] NewDB ha fallat: %v", cfg.Label, err)
			}
			defer database.Close()

			bulkDB, ok := database.(f325BulkCreateDB)
			if !ok {
				t.Fatalf("[%s] la DB no exposa BulkCreateTranscripcioRawBundles", cfg.Label)
			}

			llibreID := createF325Book(t, database, cfg.Label)
			bundles, expectedPersones, expectedAtributs := buildF325Bundles(llibreID, 430)
			result, err := bulkDB.BulkCreateTranscripcioRawBundles(bundles)
			if err != nil {
				t.Fatalf("[%s] BulkCreateTranscripcioRawBundles ha fallat: %v", cfg.Label, err)
			}
			if len(result.IDs) != len(bundles) || result.Metrics.Rows != len(bundles) {
				t.Fatalf("[%s] resultat bulk inesperat: ids=%d rows=%d bundles=%d", cfg.Label, len(result.IDs), result.Metrics.Rows, len(bundles))
			}
			if result.Metrics.Persones != expectedPersones || result.Metrics.Atributs != expectedAtributs {
				t.Fatalf("[%s] mètriques relacionades inesperades: %+v", cfg.Label, result.Metrics)
			}
			if result.Metrics.TranscripcioInsertDur <= 0 || result.Metrics.PersonaPersistDur <= 0 || result.Metrics.LinksPersistDur <= 0 || result.Metrics.CommitDur <= 0 {
				t.Fatalf("[%s] mètriques temporals buides: %+v", cfg.Label, result.Metrics)
			}

			registres, err := database.ListTranscripcionsRaw(llibreID, db.TranscripcioFilter{Limit: -1})
			if err != nil {
				t.Fatalf("[%s] ListTranscripcionsRaw ha fallat: %v", cfg.Label, err)
			}
			if len(registres) != len(bundles) {
				t.Fatalf("[%s] esperava %d registres persistits, got=%d", cfg.Label, len(bundles), len(registres))
			}

			for _, idx := range []int{0, len(result.IDs) / 2, len(result.IDs) - 1} {
				persones, err := database.ListTranscripcioPersones(result.IDs[idx])
				if err != nil {
					t.Fatalf("[%s] ListTranscripcioPersones(%d) ha fallat: %v", cfg.Label, result.IDs[idx], err)
				}
				if len(persones) != 2 {
					t.Fatalf("[%s] registre %d esperava 2 persones, got=%d", cfg.Label, result.IDs[idx], len(persones))
				}
				atributs, err := database.ListTranscripcioAtributs(result.IDs[idx])
				if err != nil {
					t.Fatalf("[%s] ListTranscripcioAtributs(%d) ha fallat: %v", cfg.Label, result.IDs[idx], err)
				}
				if len(atributs) != 2 {
					t.Fatalf("[%s] registre %d esperava 2 atributs, got=%d", cfg.Label, result.IDs[idx], len(atributs))
				}
			}
		})
	}
}

func ensureF325ProjectRoot(t *testing.T) {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("no puc obtenir directori actual: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			if err := os.Chdir(dir); err != nil {
				t.Fatalf("no puc fer chdir a l'arrel del projecte (%s): %v", dir, err)
			}
			return
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("no s'ha trobat go.mod a cap directori pare de %s", dir)
		}
		dir = parent
	}
}

func f325SQLiteAndPostgresConfigs(t *testing.T) []testcommon.TestDBConfig {
	t.Helper()

	raw := testcommon.LoadTestDBConfigs(t)
	out := make([]testcommon.TestDBConfig, 0, len(raw))
	for _, cfg := range raw {
		if cfg.Engine == "mysql" {
			continue
		}
		cfgCopy := map[string]string{}
		for k, v := range cfg.Config {
			cfgCopy[k] = v
		}
		cfgCopy["RECREADB"] = "true"
		if _, ok := cfgCopy["LOG_LEVEL"]; !ok {
			cfgCopy["LOG_LEVEL"] = "silent"
		}
		if cfg.Engine == "sqlite" {
			cfgCopy["DB_PATH"] = filepath.Join(t.TempDir(), "f325_"+cfg.Label+".sqlite3")
		}
		out = append(out, testcommon.TestDBConfig{
			Engine: cfg.Engine,
			Label:  cfg.Label,
			Config: cfgCopy,
		})
	}
	if len(out) == 0 {
		t.Fatalf("no hi ha configuracions SQLite/PostgreSQL disponibles per F32-5")
	}
	return out
}

func createF325Book(t *testing.T, database db.DB, label string) int {
	t.Helper()

	suffix := fmt.Sprintf("%s_%d", label, time.Now().UnixNano())
	municipiID, err := database.CreateMunicipi(&db.Municipi{
		Nom:            "Municipi F325 " + suffix,
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateMunicipi ha fallat: %v", err)
	}
	arquebisbatID, err := database.CreateArquebisbat(&db.Arquebisbat{
		Nom:            "Bisbat F325 " + suffix,
		TipusEntitat:   "bisbat",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateArquebisbat ha fallat: %v", err)
	}
	llibreID, err := database.CreateLlibre(&db.Llibre{
		ArquebisbatID:  arquebisbatID,
		MunicipiID:     municipiID,
		Titol:          "Llibre F325 " + suffix,
		Cronologia:     "1891",
		TipusLlibre:    "sacramental",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateLlibre ha fallat: %v", err)
	}
	return llibreID
}

func buildF325Bundles(llibreID, total int) ([]db.TranscripcioRawImportBundle, int, int) {
	bundles := make([]db.TranscripcioRawImportBundle, 0, total)
	totalPersones := 0
	totalAtributs := 0
	for i := 0; i < total; i++ {
		bundles = append(bundles, db.TranscripcioRawImportBundle{
			Transcripcio: db.TranscripcioRaw{
				LlibreID:       llibreID,
				NumPaginaText:  fmt.Sprintf("%d", 10+(i%25)),
				TipusActe:      "baptisme",
				DataActeText:   fmt.Sprintf("%02d/02/1891", 1+(i%28)),
				DataActeISO:    sql.NullString{String: fmt.Sprintf("1891-02-%02d", 1+(i%28)), Valid: true},
				DataActeEstat:  "clar",
				ModeracioEstat: "pendent",
			},
			Persones: []db.TranscripcioPersonaRaw{
				{
					Rol:      "batejat",
					Nom:      fmt.Sprintf("NomF325_%d", i),
					Cognom1:  fmt.Sprintf("CognomF325_%d", i),
					Cognom2:  "Soler",
					NomEstat: "clar",
				},
				{
					Rol:     "pare",
					Nom:     fmt.Sprintf("PareF325_%d", i),
					Cognom1: "Garcia",
				},
			},
			Atributs: []db.TranscripcioAtributRaw{
				{
					Clau:       "pagina_digital",
					TipusValor: "text",
					ValorText:  fmt.Sprintf("%d", 10+(i%25)),
					Estat:      "clar",
				},
				{
					Clau:       "ofici",
					TipusValor: "text",
					ValorText:  fmt.Sprintf("ofici_%d", i),
					Estat:      "clar",
				},
			},
		})
		totalPersones += 2
		totalAtributs += 2
	}
	return bundles, totalPersones, totalAtributs
}
