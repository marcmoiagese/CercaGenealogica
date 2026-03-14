package integration

import (
	"database/sql"
	"strconv"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestResolveLlibresByCodesMultiDB(t *testing.T) {
	apps := newAppsForAllDBs(t)

	for _, env := range apps {
		env := env
		t.Run(env.Label, func(t *testing.T) {
			t.Helper()
			database := env.DB

			pais := &db.Pais{
				CodiISO2:    "ES",
				CodiISO3:    "ESP",
				CodiPaisNum: "724",
			}
			paisID, err := database.CreatePais(pais)
			if err != nil {
				t.Fatalf("CreatePais ha fallat: %v", err)
			}
			nivell := &db.NivellAdministratiu{
				PaisID:         paisID,
				Nivel:          1,
				NomNivell:      "Test nivell",
				TipusNivell:    "pais",
				Estat:          "actiu",
				ModeracioEstat: "pendent",
			}
			nivellID, err := database.CreateNivell(nivell)
			if err != nil {
				t.Fatalf("CreateNivell ha fallat: %v", err)
			}
			mun := &db.Municipi{
				Nom:            "Municipi Test",
				Tipus:          "municipi",
				Estat:          "actiu",
				ModeracioEstat: "pendent",
			}
			mun.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(nivellID), Valid: true}
			munID, err := database.CreateMunicipi(mun)
			if err != nil {
				t.Fatalf("CreateMunicipi ha fallat: %v", err)
			}
			entitat := &db.Arquebisbat{
				Nom:            "Bisbat Test",
				TipusEntitat:   "bisbat",
				PaisID:         sql.NullInt64{Int64: int64(paisID), Valid: true},
				ModeracioEstat: "pendent",
			}
			entID, err := database.CreateArquebisbat(entitat)
			if err != nil {
				t.Fatalf("CreateArquebisbat ha fallat: %v", err)
			}

			codiDigital := "123"
			codiFisic := "CF-" + strconv.FormatInt(time.Now().UnixNano(), 10)
			llibre := &db.Llibre{
				ArquebisbatID:  entID,
				MunicipiID:     munID,
				TipusLlibre:    "baptismes",
				Cronologia:     "1900-1910",
				CodiDigital:    codiDigital,
				CodiFisic:      codiFisic,
				ModeracioEstat: "pendent",
			}
			if _, err := database.CreateLlibre(llibre); err != nil {
				t.Fatalf("CreateLlibre ha fallat: %v", err)
			}

			rows, err := database.ResolveLlibresByCodes(munID, "baptismes", "1900-1910", []string{codiDigital}, []string{codiFisic})
			if err != nil {
				t.Fatalf("ResolveLlibresByCodes ha fallat: %v", err)
			}
			if len(rows) == 0 {
				t.Fatalf("ResolveLlibresByCodes esperava 1 fila, got 0")
			}
		})
	}
}
