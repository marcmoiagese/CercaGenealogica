package unit

import (
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
	testcommon "github.com/marcmoiagese/CercaGenealogica/tests/common"
)

func TestF3212BulkCreateTranscripcioRawBundlesBatchMetricsSQLitePostgres(t *testing.T) {
	ensureF325ProjectRoot(t)
	for _, cfg := range f325SQLiteAndPostgresConfigs(t) {
		cfg := cfg
		t.Run(cfg.Label, func(t *testing.T) {
			database, err := openF325DatabaseForConfig(t, cfg)
			if err != nil {
				t.Fatalf("[%s] NewDB ha fallat: %v", cfg.Label, err)
			}
			defer database.Close()

			bulkDB, ok := database.(f325BulkCreateDB)
			if !ok {
				t.Fatalf("[%s] la DB no exposa BulkCreateTranscripcioRawBundles", cfg.Label)
			}

			llibreID := createF325Book(t, database, cfg.Label+"_f3212")
			bundles, expectedPersones, expectedAtributs := buildF325Bundles(llibreID, 430)
			result, err := bulkDB.BulkCreateTranscripcioRawBundles(bundles)
			if err != nil {
				t.Fatalf("[%s] BulkCreateTranscripcioRawBundles ha fallat: %v", cfg.Label, err)
			}
			if result.Metrics.Persones != expectedPersones || result.Metrics.Atributs != expectedAtributs {
				t.Fatalf("[%s] mètriques relacionades inesperades: %+v", cfg.Label, result.Metrics)
			}
			if result.Metrics.TranscripcioBatches <= 0 || result.Metrics.PersonaBatches <= 0 || result.Metrics.AtributBatches <= 0 {
				t.Fatalf("[%s] esperava comptadors de batch positius, got=%+v", cfg.Label, result.Metrics)
			}

			if cfg.Engine == "postgres" {
				if result.Metrics.TranscripcioBatches != 1 {
					t.Fatalf("[%s] esperava 1 batch bulk de transcripcions, got=%d", cfg.Label, result.Metrics.TranscripcioBatches)
				}
				if result.Metrics.PersonaBatches != 1 {
					t.Fatalf("[%s] esperava 1 batch bulk de persones, got=%d", cfg.Label, result.Metrics.PersonaBatches)
				}
				if result.Metrics.AtributBatches != 1 {
					t.Fatalf("[%s] esperava 1 batch bulk d'atributs, got=%d", cfg.Label, result.Metrics.AtributBatches)
				}
				return
			}

			if result.Metrics.TranscripcioBatches != len(bundles) {
				t.Fatalf("[%s] esperava batches de transcripcions fila-a-fila, got=%d want=%d", cfg.Label, result.Metrics.TranscripcioBatches, len(bundles))
			}
			if result.Metrics.PersonaBatches != expectedPersones {
				t.Fatalf("[%s] esperava batches de persones fila-a-fila, got=%d want=%d", cfg.Label, result.Metrics.PersonaBatches, expectedPersones)
			}
			if result.Metrics.AtributBatches != expectedAtributs {
				t.Fatalf("[%s] esperava batches d'atributs fila-a-fila, got=%d want=%d", cfg.Label, result.Metrics.AtributBatches, expectedAtributs)
			}
		})
	}
}

func openF325DatabaseForConfig(t *testing.T, cfg testcommon.TestDBConfig) (db.DB, error) {
	t.Helper()
	return db.NewDB(cfg.Config)
}
