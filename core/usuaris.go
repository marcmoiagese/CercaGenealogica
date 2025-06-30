package core

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type Usuari struct {
	ID          int
	Nom         string
	Cognoms     string
	Correu      string
	Contrasenya string
	DataCreacio time.Time
	Actiu       bool
}

func RegistrarUsuari(db db.DB, nom, cognoms, correu, contrassenya string) error {
	token := generateToken(32)
	query := `
        INSERT INTO usuaris (nom, cognoms, correu, contrasenya, actiu, data_creacio) 
        VALUES (?, ?, ?, ?, 0, datetime('now'))`
	_, err := db.Exec(query, nom, cognoms, correu, contrassenya)
	if err != nil {
		return err
	}

	log.Printf("Token d'activació: %s", token)

	// TODO: guardar token + expiració en una taula "tokens_activacio"
	// INSERT INTO tokens_activacio (usuari_correu, token, expira) VALUES (?, ?, ?)

	return nil
}

func ActivarUsuari(db db.DB, token string) error {
	// TODO: comprovar si token existeix i no ha expirat
	// SELECT usuari_correu FROM tokens WHERE token = ? AND expira > NOW()

	correu := "usuari@exemple.com" // Exemple temporal

	_, err := db.Exec("UPDATE usuaris SET actiu = 1 WHERE correu = ?", correu)
	return err
}

func generateToken(length int) string {
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	result := make([]byte, length)
	for i := range result {
		num, _ := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		result[i] = letters[num.Int64()]
	}
	return string(result)
}
