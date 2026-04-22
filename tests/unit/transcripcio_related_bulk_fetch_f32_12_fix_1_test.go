package unit

import (
	"testing"
)

func TestF3212Fix1LargeRelatedBulkFetchSQLitePostgres(t *testing.T) {
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

			llibreID := createF325Book(t, database, cfg.Label+"_f3212_fix1")
			bundles, expectedPersones, expectedAtributs := buildF325Bundles(llibreID, 1300)
			result, err := bulkDB.BulkCreateTranscripcioRawBundles(bundles)
			if err != nil {
				t.Fatalf("[%s] BulkCreateTranscripcioRawBundles ha fallat: %v", cfg.Label, err)
			}
			if len(result.IDs) != len(bundles) {
				t.Fatalf("[%s] esperava %d IDs, got=%d", cfg.Label, len(bundles), len(result.IDs))
			}

			personesByID, err := database.ListTranscripcioPersonesByTranscripcioIDs(result.IDs)
			if err != nil {
				t.Fatalf("[%s] ListTranscripcioPersonesByTranscripcioIDs ha fallat: %v", cfg.Label, err)
			}
			atributsByID, err := database.ListTranscripcioAtributsByTranscripcioIDs(result.IDs)
			if err != nil {
				t.Fatalf("[%s] ListTranscripcioAtributsByTranscripcioIDs ha fallat: %v", cfg.Label, err)
			}

			totalPersones := 0
			totalAtributs := 0
			for _, id := range result.IDs {
				persones := personesByID[id]
				if len(persones) != 2 {
					t.Fatalf("[%s] transcripcio %d esperava 2 persones, got=%d", cfg.Label, id, len(persones))
				}
				atributs := atributsByID[id]
				if len(atributs) != 2 {
					t.Fatalf("[%s] transcripcio %d esperava 2 atributs, got=%d", cfg.Label, id, len(atributs))
				}
				totalPersones += len(persones)
				totalAtributs += len(atributs)
			}

			if totalPersones != expectedPersones {
				t.Fatalf("[%s] total persones bulk inesperat: got=%d want=%d", cfg.Label, totalPersones, expectedPersones)
			}
			if totalAtributs != expectedAtributs {
				t.Fatalf("[%s] total atributs bulk inesperat: got=%d want=%d", cfg.Label, totalAtributs, expectedAtributs)
			}
		})
	}
}
