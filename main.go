package main

import (
	"log"
	"net/http"

	"github.com/marcmoiagese/CercaGenealogica/cnf"
	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func main() {

	// Carrega configuració, inicia connexió BD...
	log.Println("Servidor iniciant-se...")

	config := cnf.LoadConfig("cnf/config.cfg")

	dbInstance, err := db.NewDB(config)
	if err != nil {
		log.Fatalf("Error inicialitzant BD: %v", err)
	}
	defer dbInstance.Close()

	// Aplica middleware a /static/
	http.HandleFunc("/static/", core.SecureHeaders(core.ServeStatic))

	// Rutes públiques
	http.HandleFunc("/", core.SecureHeaders(func(w http.ResponseWriter, r *http.Request) {
		core.RenderTemplate(w, "index", nil)
	}))

	http.HandleFunc("/inici", core.SecureHeaders(func(w http.ResponseWriter, r *http.Request) {
		core.RenderPrivateTemplate(w, "index-logedin", nil)
	}))

	log.Println("Servidor iniciat a http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
