package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"text/template"

	"github.com/marcmoiagese/CercaGenealogica/db"
	"github.com/marcmoiagese/CercaGenealogica/web/handlers"
)

func loadConfig() (engine, path string) {
	data, err := os.ReadFile("cnf/config.cfg")
	if err != nil {
		log.Fatal("No es pot llegir config.cfg")
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if strings.Contains(line, "DB_ENGINE=") {
			engine = strings.TrimPrefix(line, "DB_ENGINE=")
		}
		if strings.Contains(line, "DB_PATH=") {
			path = strings.TrimPrefix(line, "DB_PATH=")
		}
	}
	return
}

func main() {
	engine, _ := loadConfig()
	dbManager, err := db.GetDBManager(engine)
	if err != nil {
		log.Fatal(err)
	}
	err = dbManager.Init()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Inicialitza connexió SQL per passar als handlers
	sqlDB := dbManager.DB()

	// Rutes
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		tmpl, err := template.ParseFiles("web/templates/index.html")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		tmpl.Execute(w, nil)
	})

	http.HandleFunc("/cerca", handlers.CercaHandler(sqlDB))
	http.HandleFunc("/upload", handlers.UploadPageHandler())
	http.HandleFunc("/import", handlers.ImportHandler(sqlDB))

	// Serve static files
	http.Handle("/static/", handlers.StaticHandler())

	fmt.Println("Servidor corrent a http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
