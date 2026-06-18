package unit

import (
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func createTemplateImportTestBook(t *testing.T, database db.DB, suffix string) int {
	t.Helper()
	municipiID, err := database.CreateMunicipi(&db.Municipi{
		Nom:            "Municipi F354U10R3 " + suffix,
		Tipus:          "municipi",
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateMunicipi ha fallat: %v", err)
	}
	arquebisbatID, err := database.CreateArquebisbat(&db.Arquebisbat{
		Nom:            "Arquebisbat F354U10R3 " + suffix,
		TipusEntitat:   "bisbat",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateArquebisbat ha fallat: %v", err)
	}
	llibreID, err := database.CreateLlibre(&db.Llibre{
		ArquebisbatID:  arquebisbatID,
		MunicipiID:     municipiID,
		Titol:          "Llibre F354U10R3 " + suffix,
		Cronologia:     "1899",
		TipusLlibre:    "sacramental",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateLlibre ha fallat: %v", err)
	}
	return llibreID
}

func TestInsertAdminImportRunSQLiteReturnsIDF354U10R3(t *testing.T) {
	database := newTestSQLiteDB(t)
	defer database.Close()

	id, err := database.InsertAdminImportRun("transcripcions_templates", "ok", 0)
	if err != nil {
		t.Fatalf("InsertAdminImportRun ha fallat: %v", err)
	}
	if id <= 0 {
		t.Fatalf("InsertAdminImportRun hauria de retornar id > 0, got=%d", id)
	}
}

func TestPersistTemplatePendingMergeAllowsIdempotentSQLiteF354U10R3(t *testing.T) {
	database := newTestSQLiteDB(t)
	defer database.Close()

	llibreID := createTemplateImportTestBook(t, database, "idempotent")
	registre := &db.TranscripcioRaw{
		LlibreID:       llibreID,
		TipusActe:      "baptisme",
		DataActeEstat:  "clar",
		NotesMarginals: "mateix valor",
		ModeracioEstat: "pendent",
	}
	registreID, err := database.CreateTranscripcioRaw(registre)
	if err != nil {
		t.Fatalf("CreateTranscripcioRaw ha fallat: %v", err)
	}
	existing, err := database.GetTranscripcioRaw(registreID)
	if err != nil || existing == nil {
		t.Fatalf("GetTranscripcioRaw ha fallat: registre=%+v err=%v", existing, err)
	}

	if err := database.PersistTemplatePendingMerge(existing, nil, nil); err != nil {
		t.Fatalf("PersistTemplatePendingMerge idempotent no hauria de fallar: %v", err)
	}
	after, err := database.GetTranscripcioRaw(registreID)
	if err != nil || after == nil {
		t.Fatalf("GetTranscripcioRaw després ha fallat: registre=%+v err=%v", after, err)
	}
	if after.ID != registreID || after.NotesMarginals != "mateix valor" {
		t.Fatalf("registre inesperat després del merge idempotent: %+v", after)
	}
}

func TestPersistTemplatePendingMergeMissingTargetSQLiteF354U10R3(t *testing.T) {
	database := newTestSQLiteDB(t)
	defer database.Close()

	llibreID := createTemplateImportTestBook(t, database, "missing")
	err := database.PersistTemplatePendingMerge(&db.TranscripcioRaw{
		ID:             999999,
		LlibreID:       llibreID,
		TipusActe:      "baptisme",
		DataActeEstat:  "clar",
		ModeracioEstat: "pendent",
		NotesMarginals: "no existeix",
	}, nil, nil)
	if err == nil {
		t.Fatalf("s'esperava error per registre inexistent")
	}
	if !strings.Contains(err.Error(), "template merge target not found") {
		t.Fatalf("error inesperat per registre inexistent: %v", err)
	}
}
