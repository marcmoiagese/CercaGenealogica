package integration

import (
	"database/sql"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
	testcommon "github.com/marcmoiagese/CercaGenealogica/tests/common"
)

func TestSearchDocsAndAdminClosure(t *testing.T) {
	configs := testcommon.LoadTestDBConfigs(t)
	for _, cfg := range configs {
		cfg := cfg
		t.Run(cfg.Label, func(t *testing.T) {
			database, err := db.NewDB(cfg.Config)
			if err != nil {
				t.Fatalf("no s'ha pogut inicialitzar DB: %v", err)
			}
			defer database.Close()

			paisID, err := database.CreatePais(&db.Pais{CodiISO2: "PP", CodiISO3: "PPA"})
			if err != nil {
				t.Fatalf("no s'ha pogut crear pais: %v", err)
			}
			nivellID, err := database.CreateNivell(&db.NivellAdministratiu{
				PaisID:          paisID,
				Nivel:           1,
				NomNivell:       "Nivell prova",
				TipusNivell:     "Regio",
				Estat:           "actiu",
				ModeracioEstat:  "publicat",
			})
			if err != nil {
				t.Fatalf("no s'ha pogut crear nivell: %v", err)
			}
			mun := &db.Municipi{
				Nom:            "Municipi prova",
				Tipus:          "poble",
				Estat:          "actiu",
				ModeracioEstat: "publicat",
			}
			mun.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(nivellID), Valid: true}
			munID, err := database.CreateMunicipi(mun)
			if err != nil {
				t.Fatalf("no s'ha pogut crear municipi: %v", err)
			}

			entries := []db.AdminClosureEntry{
				{DescendantMunicipiID: munID, AncestorType: "municipi", AncestorID: munID},
				{DescendantMunicipiID: munID, AncestorType: "nivell", AncestorID: nivellID},
				{DescendantMunicipiID: munID, AncestorType: "pais", AncestorID: paisID},
			}
			if err := database.ReplaceAdminClosure(munID, entries); err != nil {
				t.Fatalf("replace admin_closure fallat: %v", err)
			}
			rows, err := database.ListAdminClosure(munID)
			if err != nil {
				t.Fatalf("list admin_closure fallat: %v", err)
			}
			if len(rows) < 3 {
				t.Fatalf("esperava min 3 entrades admin_closure, obtingut %d", len(rows))
			}

			cognomID, err := database.UpsertCognom("Prova", "PROVA", "test", "", nil)
			if err != nil {
				t.Fatalf("no s'ha pogut crear cognom: %v", err)
			}
			foundID, err := database.FindCognomIDByKey("PROVA")
			if err != nil {
				t.Fatalf("find cognom per key fallat: %v", err)
			}
			if foundID != cognomID {
				t.Fatalf("cognom id inesperat: %d != %d", foundID, cognomID)
			}

			doc := &db.SearchDoc{
				EntityType:        "persona",
				EntityID:          123,
				Published:         true,
				MunicipiID:        sql.NullInt64{Int64: int64(munID), Valid: true},
				PersonNomNorm:     "joan",
				PersonCognomsNorm: "prova",
				PersonFullNorm:    "joan prova",
				PersonTokensNorm:  "joan prova",
				CognomsTokensNorm: "prova",
				PersonPhonetic:    "J500",
				CognomsPhonetic:   "P610",
				CognomsCanon:      "prova",
				AnyActe:           sql.NullInt64{Int64: 1900, Valid: true},
			}
			if err := database.UpsertSearchDoc(doc); err != nil {
				t.Fatalf("upsert search doc fallat: %v", err)
			}
			loaded, err := database.GetSearchDoc("persona", 123)
			if err != nil {
				t.Fatalf("get search doc fallat: %v", err)
			}
			if !loaded.Published || loaded.PersonFullNorm != "joan prova" {
				t.Fatalf("search doc carregat incorrecte")
			}

			doc.Published = false
			doc.PersonFullNorm = "joan prova nou"
			if err := database.UpsertSearchDoc(doc); err != nil {
				t.Fatalf("upsert search doc update fallat: %v", err)
			}
			loaded, err = database.GetSearchDoc("persona", 123)
			if err != nil {
				t.Fatalf("get search doc update fallat: %v", err)
			}
			if loaded.Published || loaded.PersonFullNorm != "joan prova nou" {
				t.Fatalf("search doc update incorrecte")
			}

			if err := database.DeleteSearchDoc("persona", 123); err != nil {
				t.Fatalf("delete search doc fallat: %v", err)
			}
			if _, err := database.GetSearchDoc("persona", 123); err == nil {
				t.Fatalf("esperava error després d'esborrar search doc")
			}
		})
	}
}
