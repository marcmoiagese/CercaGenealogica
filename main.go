package main

import (
	"fmt"
	"log"

	"github.com/marcmoiagese/CercaGenealogica/cnf"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func main() {
	// Carregar configuració
	config, err := cnf.LoadConfig()
	if err != nil {
		log.Fatalf("Error carregant configuració: %v", err)
	}

	// Inicialitzar base de dades
	database, err := db.NewDatabase(*config) // Nota el * per desreferenciar
	if err != nil {
		log.Fatalf("Error inicialitzant base de dades: %v", err)
	}

	// Connectar a la base de dades
	if err := database.Connect(); err != nil {
		log.Fatalf("Error connectant a la base de dades: %v", err)
	}
	defer database.Close()

	// Executar migracions
	if err := database.Migrate(); err != nil {
		log.Fatalf("Error executant migracions: %v", err)
	}

	fmt.Println("Aplicació iniciada correctament!")
	// Aquí pots afegir més lògica de la teva aplicació
}
