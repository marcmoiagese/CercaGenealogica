package integration

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestF353MunicipiEntitatReligiosaSchemaSQLite(t *testing.T) {
	_, database := newTestAppForLogin(t, "test_f35_3_municipi_entitat_religiosa.sqlite3")

	if !f351SQLiteTableExists(t, database, "municipi_entitat_religiosa") {
		t.Fatalf("taula municipi_entitat_religiosa no creada")
	}
	got := f351SQLiteColumns(t, database, "municipi_entitat_religiosa")
	for _, column := range []string{
		"id", "municipi_id", "nucli_id", "entitat_religiosa_id", "tipus_relacio",
		"any_inici", "any_fi", "observacions", "moderation_status", "created_at", "updated_at",
	} {
		if !got[column] {
			t.Fatalf("municipi_entitat_religiosa no te columna %s; columns=%v", column, got)
		}
	}
	for _, idx := range []string{
		"idx_municipi_entitat_religiosa_municipi",
		"idx_municipi_entitat_religiosa_nucli",
		"idx_municipi_entitat_religiosa_entitat",
	} {
		if !f351SQLiteIndexExists(t, database, idx) {
			t.Fatalf("index esperat no creat: %s", idx)
		}
	}
	fks := f351SQLiteFKs(t, database, "municipi_entitat_religiosa")
	for _, fk := range []string{
		"municipi_id->municipis",
		"nucli_id->municipis",
		"entitat_religiosa_id->entitat_religiosa",
	} {
		if !fks[fk] {
			t.Fatalf("FK esperada absent %s; fks=%v", fk, fks)
		}
	}
}

func TestF353MunicipiEntitatReligiosaSQLFilesAligned(t *testing.T) {
	root := findProjectRoot(t)
	for _, rel := range []string{"db/SQLite.sql", "db/PostgreSQL.sql", "db/MySQL.sql"} {
		body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("no s'ha pogut llegir %s: %v", rel, err)
		}
		src := string(body)
		for _, token := range []string{
			"CREATE TABLE IF NOT EXISTS municipi_entitat_religiosa",
			"municipi_id",
			"nucli_id",
			"entitat_religiosa_id",
			"tipus_relacio",
			"any_inici",
			"any_fi",
			"idx_municipi_entitat_religiosa_municipi",
			"idx_municipi_entitat_religiosa_nucli",
			"idx_municipi_entitat_religiosa_entitat",
			"entitat_religiosa",
		} {
			if !strings.Contains(src, token) {
				t.Fatalf("%s no conte token F35-3 %q", rel, token)
			}
		}
		for _, forbidden := range []string{
			"llibre_entitat_" + "religiosa",
			"arxiu_" + "entitat",
		} {
			if strings.Contains(src, forbidden) {
				t.Fatalf("%s introdueix relacio fora d'abast %s", rel, forbidden)
			}
		}
	}
}

func TestF353MunicipiEntitatReligiosaCRUDMultiDB(t *testing.T) {
	for _, env := range newAppsForAllDBs(t) {
		t.Run(env.Label, func(t *testing.T) {
			suffix := fmt.Sprintf("%d", time.Now().UnixNano())
			municipiID, err := env.DB.CreateMunicipi(&db.Municipi{
				Nom:            "Municipi F35-3 " + suffix,
				Tipus:          "municipi",
				Estat:          "actiu",
				ModeracioEstat: "publicat",
			})
			if err != nil {
				t.Fatalf("CreateMunicipi municipi: %v", err)
			}
			nucliID, err := env.DB.CreateMunicipi(&db.Municipi{
				Nom:            "Nucli F35-3 " + suffix,
				MunicipiID:     sql.NullInt64{Int64: int64(municipiID), Valid: true},
				Tipus:          "nucli",
				Estat:          "actiu",
				ModeracioEstat: "publicat",
			})
			if err != nil {
				t.Fatalf("CreateMunicipi nucli: %v", err)
			}
			entitatID, err := env.DB.SaveEntitatReligiosa(&db.EntitatReligiosa{
				Nom:            "Entitat rel F35-3 " + suffix,
				TipusEntitat:   "parroquia",
				Estat:          "actiu",
				ModeracioEstat: "publicat",
			})
			if err != nil {
				t.Fatalf("SaveEntitatReligiosa: %v", err)
			}
			rel := &db.MunicipiEntitatReligiosa{
				MunicipiID:         municipiID,
				NucliID:            sql.NullInt64{Int64: int64(nucliID), Valid: true},
				EntitatReligiosaID: entitatID,
				TipusRelacio:       "parroquia",
				AnyInici:           sql.NullInt64{Int64: 1850, Valid: true},
				Observacions:       "relacio temporal de prova",
				ModeracioEstat:     "publicat",
			}
			relID, err := env.DB.SaveMunicipiEntitatReligiosa(rel)
			if err != nil {
				t.Fatalf("SaveMunicipiEntitatReligiosa create: %v", err)
			}
			rel.AnyFi = sql.NullInt64{Int64: 1900, Valid: true}
			rel.TipusRelacio = "historica"
			if _, err := env.DB.SaveMunicipiEntitatReligiosa(rel); err != nil {
				t.Fatalf("SaveMunicipiEntitatReligiosa update: %v", err)
			}
			got, err := env.DB.GetMunicipiEntitatReligiosa(relID)
			if err != nil {
				t.Fatalf("GetMunicipiEntitatReligiosa: %v", err)
			}
			if got.TipusRelacio != "historica" || !got.AnyFi.Valid || got.AnyFi.Int64 != 1900 {
				t.Fatalf("relacio no actualitzada correctament: %+v", got)
			}
			list, err := env.DB.ListMunicipiEntitatsReligioses(municipiID)
			if err != nil {
				t.Fatalf("ListMunicipiEntitatsReligioses: %v", err)
			}
			found := false
			for _, item := range list {
				if item.ID == relID {
					found = true
				}
			}
			if !found {
				t.Fatalf("relacio %d no trobada al llistat per municipi", relID)
			}
			if err := env.DB.DeleteEntitatReligiosa(entitatID); !errors.Is(err, db.ErrUnsafeDelete) {
				t.Fatalf("DeleteEntitatReligiosa amb relacio = %v, want ErrUnsafeDelete", err)
			}
			if err := env.DB.DeleteMunicipiEntitatReligiosa(relID); err != nil {
				t.Fatalf("DeleteMunicipiEntitatReligiosa: %v", err)
			}
			if err := env.DB.DeleteEntitatReligiosa(entitatID); err != nil {
				t.Fatalf("DeleteEntitatReligiosa final: %v", err)
			}
		})
	}
}
