package integration

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF352ConfessionalCRUDMultiDB(t *testing.T) {
	for _, env := range newConfessionalAppsForAllDBs(t) {
		t.Run(env.Label, func(t *testing.T) {
			suffix := fmt.Sprintf("%d", time.Now().UnixNano())
			religio := &db.ReligioConfessio{
				Nom:            "Religio F35-2 " + suffix,
				Descripcio:     "confessio de prova",
				Estat:          "actiu",
				Observacions:   "obs",
				ModeracioEstat: "publicat",
			}
			religioID, err := env.DB.SaveReligioConfessio(religio)
			if err != nil {
				t.Fatalf("SaveReligioConfessio: %v", err)
			}
			religio.Nom = "Religio F35-2 editada " + suffix
			if _, err := env.DB.SaveReligioConfessio(religio); err != nil {
				t.Fatalf("Update ReligioConfessio: %v", err)
			}
			gotReligio, err := env.DB.GetReligioConfessio(religioID)
			if err != nil {
				t.Fatalf("GetReligioConfessio: %v", err)
			}
			if gotReligio.Nom != religio.Nom {
				t.Fatalf("nom religio no actualitzat: got %q want %q", gotReligio.Nom, religio.Nom)
			}

			model := &db.ModelConfessional{
				Nom:                "Model F35-2 " + suffix,
				ReligioConfessioID: sql.NullInt64{Int64: int64(religioID), Valid: true},
				Descripcio:         "model de prova",
				AnyInici:           sql.NullInt64{Int64: 1900, Valid: true},
				Estat:              "actiu",
				ModeracioEstat:     "publicat",
			}
			modelID, err := env.DB.SaveModelConfessional(model)
			if err != nil {
				t.Fatalf("SaveModelConfessional: %v", err)
			}
			nivell := &db.NivellConfessional{
				ModelConfessionalID: modelID,
				Ordre:               1,
				NomNivell:           "Nivell F35-2 " + suffix,
				NomPlural:           "Nivells",
				TipusNivell:         "diocesi",
				Estat:               "actiu",
				ModeracioEstat:      "publicat",
			}
			nivellID, err := env.DB.SaveNivellConfessional(nivell)
			if err != nil {
				t.Fatalf("SaveNivellConfessional: %v", err)
			}
			entitat := &db.EntitatReligiosa{
				Nom:                  "Entitat F35-2 " + suffix,
				ReligioConfessioID:   sql.NullInt64{Int64: int64(religioID), Valid: true},
				ModelConfessionalID:  sql.NullInt64{Int64: int64(modelID), Valid: true},
				NivellConfessionalID: sql.NullInt64{Int64: int64(nivellID), Valid: true},
				TipusEntitat:         "diocesi",
				Estat:                "actiu",
				Web:                  "https://example.test",
				ModeracioEstat:       "publicat",
			}
			entitatID, err := env.DB.SaveEntitatReligiosa(entitat)
			if err != nil {
				t.Fatalf("SaveEntitatReligiosa: %v", err)
			}
			entitat.Nom = "Entitat F35-2 editada " + suffix
			if _, err := env.DB.SaveEntitatReligiosa(entitat); err != nil {
				t.Fatalf("Update EntitatReligiosa: %v", err)
			}
			gotEntitat, err := env.DB.GetEntitatReligiosa(entitatID)
			if err != nil {
				t.Fatalf("GetEntitatReligiosa: %v", err)
			}
			if gotEntitat.Nom != entitat.Nom {
				t.Fatalf("nom entitat no actualitzat: got %q want %q", gotEntitat.Nom, entitat.Nom)
			}

			if err := env.DB.DeleteReligioConfessio(religioID); !errors.Is(err, db.ErrUnsafeDelete) {
				t.Fatalf("DeleteReligioConfessio amb dependencies = %v, want ErrUnsafeDelete", err)
			}
			if err := env.DB.DeleteNivellConfessional(nivellID); !errors.Is(err, db.ErrUnsafeDelete) {
				t.Fatalf("DeleteNivellConfessional amb dependencies = %v, want ErrUnsafeDelete", err)
			}
			if err := env.DB.DeleteEntitatReligiosa(entitatID); err != nil {
				t.Fatalf("DeleteEntitatReligiosa: %v", err)
			}
			if err := env.DB.DeleteNivellConfessional(nivellID); err != nil {
				t.Fatalf("DeleteNivellConfessional: %v", err)
			}
			if err := env.DB.DeleteModelConfessional(modelID); err != nil {
				t.Fatalf("DeleteModelConfessional: %v", err)
			}
			if err := env.DB.DeleteReligioConfessio(religioID); err != nil {
				t.Fatalf("DeleteReligioConfessio: %v", err)
			}
		})
	}
}
