package core

import (
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestApplyModeracioBulkRegistreDerivedSideEffectsDeletesSearchDocsF311(t *testing.T) {
	app, database := newModeracioBulkDiagnosticsApp(t)

	const registreID = 77
	if err := database.UpsertSearchDoc(&db.SearchDoc{
		EntityType:      "registre_raw",
		EntityID:        registreID,
		Published:       true,
		PersonFullNorm:  "joan pujo",
		PersonNomNorm:   "joan",
		CognomsCanon:    "pujol",
		PersonPhonetic:  "JN",
		CognomsPhonetic: "PJL",
	}); err != nil {
		t.Fatalf("UpsertSearchDoc ha fallat: %v", err)
	}

	app.applyModeracioBulkRegistreDerivedSideEffects(
		[]moderacioBulkRegistreState{{
			Reg: db.TranscripcioRaw{ID: registreID, ModeracioEstat: "publicat"},
		}},
		map[int]struct{}{registreID: {}},
		"rebutjat",
		map[moderacioBulkRegistreDemoKey][]int{},
		map[int][]int{},
	)

	doc, err := database.GetSearchDoc("registre_raw", registreID)
	if err == nil && doc != nil {
		t.Fatalf("search_doc de registre %d no s'ha eliminat", registreID)
	}
}
