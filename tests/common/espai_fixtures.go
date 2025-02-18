package common

import (
	"fmt"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func CreateEspaiArbreFixture(t *testing.T, database db.DB, ownerID int) *db.EspaiArbre {
	t.Helper()

	arbre := &db.EspaiArbre{
		OwnerUserID: ownerID,
		Nom:         fmt.Sprintf("Arbre test %d", time.Now().UnixNano()),
		Visibility:  "private",
		Status:      "active",
	}
	if _, err := database.CreateEspaiArbre(arbre); err != nil {
		t.Fatalf("CreateEspaiArbre ha fallat: %v", err)
	}
	return arbre
}

func CreateEspaiPersonaFixture(t *testing.T, database db.DB, ownerID, arbreID int) *db.EspaiPersona {
	t.Helper()

	persona := &db.EspaiPersona{
		OwnerUserID: ownerID,
		ArbreID:     arbreID,
		Status:      "active",
	}
	if _, err := database.CreateEspaiPersona(persona); err != nil {
		t.Fatalf("CreateEspaiPersona ha fallat: %v", err)
	}
	return persona
}

func CreateEspaiGrupFixture(t *testing.T, database db.DB, ownerID int) *db.EspaiGrup {
	t.Helper()

	grup := &db.EspaiGrup{
		OwnerUserID: ownerID,
		Nom:         fmt.Sprintf("Grup test %d", time.Now().UnixNano()),
		Status:      "active",
	}
	if _, err := database.CreateEspaiGrup(grup); err != nil {
		t.Fatalf("CreateEspaiGrup ha fallat: %v", err)
	}
	return grup
}

func AddEspaiGrupMembreFixture(t *testing.T, database db.DB, groupID, userID int) *db.EspaiGrupMembre {
	t.Helper()

	membre := &db.EspaiGrupMembre{
		GrupID: groupID,
		UserID: userID,
		Role:   "member",
		Status: "active",
	}
	if _, err := database.AddEspaiGrupMembre(membre); err != nil {
		t.Fatalf("AddEspaiGrupMembre ha fallat: %v", err)
	}
	return membre
}
