package unit

import (
	"database/sql"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestLlibreURLStoresLinkedBook(t *testing.T) {
	cfg := newTestConfig()
	database, err := db.NewDB(cfg)
	if err != nil {
		t.Fatalf("db.NewDB: %v", err)
	}
	defer database.Close()

	municipi := &db.Municipi{
		Nom:            "Test municipi",
		Tipus:          "ciutat",
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	}
	municipiID, err := database.CreateMunicipi(municipi)
	if err != nil {
		t.Fatalf("CreateMunicipi: %v", err)
	}

	llibreA := &db.Llibre{
		MunicipiID:     municipiID,
		Titol:          "Llibre A",
		ModeracioEstat: "publicat",
	}
	llibreAID, err := database.CreateLlibre(llibreA)
	if err != nil {
		t.Fatalf("CreateLlibre A: %v", err)
	}

	llibreB := &db.Llibre{
		MunicipiID:     municipiID,
		Titol:          "Llibre B",
		ModeracioEstat: "publicat",
	}
	llibreBID, err := database.CreateLlibre(llibreB)
	if err != nil {
		t.Fatalf("CreateLlibre B: %v", err)
	}

	link := &db.LlibreURL{
		LlibreID:    llibreAID,
		LlibreRefID: sql.NullInt64{Int64: int64(llibreBID), Valid: true},
		URL:         "https://example.com/test",
	}
	if err := database.AddLlibreURL(link); err != nil {
		t.Fatalf("AddLlibreURL: %v", err)
	}

	links, err := database.ListLlibreURLs(llibreAID)
	if err != nil {
		t.Fatalf("ListLlibreURLs: %v", err)
	}
	if len(links) != 1 {
		t.Fatalf("esperava 1 enllaç, tinc %d", len(links))
	}
	got := links[0]
	if !got.LlibreRefID.Valid || int(got.LlibreRefID.Int64) != llibreBID {
		t.Fatalf("LlibreRefID inesperat: %+v", got.LlibreRefID)
	}
	if !got.LlibreRefTitol.Valid || got.LlibreRefTitol.String != "Llibre B" {
		t.Fatalf("LlibreRefTitol inesperat: %+v", got.LlibreRefTitol)
	}
}
