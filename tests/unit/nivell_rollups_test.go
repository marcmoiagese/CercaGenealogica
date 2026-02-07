package unit

import (
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestNivellRollupsBasics(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	paisID, err := app.DB.CreatePais(&db.Pais{
		CodiISO2: "ZZ",
		CodiISO3: "ZZZ",
	})
	if err != nil {
		t.Fatalf("CreatePais: %v", err)
	}
	nivellID, err := app.DB.CreateNivell(&db.NivellAdministratiu{
		PaisID:         paisID,
		Nivel:          1,
		NomNivell:      "Prova",
		TipusNivell:    "comarca",
		Estat:          "actiu",
		ModeracioEstat: "publicat",
	})
	if err != nil {
		t.Fatalf("CreateNivell: %v", err)
	}

	if err := app.DB.ApplyNivellDemografiaDelta(nivellID, 1900, "natalitat", 2); err != nil {
		t.Fatalf("ApplyNivellDemografiaDelta: %v", err)
	}
	meta, err := app.DB.GetNivellDemografiaMeta(nivellID)
	if err != nil {
		t.Fatalf("GetNivellDemografiaMeta: %v", err)
	}
	if meta == nil || meta.TotalNatalitat != 2 {
		t.Fatalf("meta inesperada: %+v", meta)
	}
	rows, err := app.DB.ListNivellDemografiaAny(nivellID, 0, 0)
	if err != nil {
		t.Fatalf("ListNivellDemografiaAny: %v", err)
	}
	if len(rows) == 0 || rows[0].Any != 1900 {
		t.Fatalf("no s'ha trobat l'any 1900 a demografia")
	}

	nomID, err := app.DB.UpsertNom("Josep", "josep", "stats_auto", nil)
	if err != nil {
		t.Fatalf("UpsertNom: %v", err)
	}
	if err := app.DB.UpsertNomFreqNivellAny(nomID, nivellID, 1900, 3); err != nil {
		t.Fatalf("UpsertNomFreqNivellAny: %v", err)
	}
	if err := app.DB.UpsertNomFreqNivellTotal(nomID, nivellID, 3); err != nil {
		t.Fatalf("UpsertNomFreqNivellTotal: %v", err)
	}
	topNoms, err := app.DB.ListTopNomsByNivell(nivellID, 5)
	if err != nil {
		t.Fatalf("ListTopNomsByNivell: %v", err)
	}
	if len(topNoms) == 0 || topNoms[0].NomID != nomID {
		t.Fatalf("top noms inesperat: %+v", topNoms)
	}
	series, err := app.DB.ListNomSeriesByNivell(nivellID, nomID, "year")
	if err != nil {
		t.Fatalf("ListNomSeriesByNivell: %v", err)
	}
	found := false
	for _, item := range series {
		if item.AnyDoc == 1900 && item.Freq == 3 {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("serie de noms no conté 1900=3")
	}

	cognomID, err := app.DB.UpsertCognom("Garcia", "garcia", "stats_auto", "stats_auto", nil)
	if err != nil {
		t.Fatalf("UpsertCognom: %v", err)
	}
	if err := app.DB.ApplyCognomFreqNivellAnyDelta(cognomID, nivellID, 1900, 2); err != nil {
		t.Fatalf("ApplyCognomFreqNivellAnyDelta: %v", err)
	}
	if err := app.DB.UpsertCognomFreqNivellTotal(cognomID, nivellID, 2); err != nil {
		t.Fatalf("UpsertCognomFreqNivellTotal: %v", err)
	}
	topCognoms, err := app.DB.ListTopCognomsByNivell(nivellID, 5)
	if err != nil {
		t.Fatalf("ListTopCognomsByNivell: %v", err)
	}
	if len(topCognoms) == 0 || topCognoms[0].CognomID != cognomID {
		t.Fatalf("top cognoms inesperat: %+v", topCognoms)
	}
}
