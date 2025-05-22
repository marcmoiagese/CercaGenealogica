package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"text/template"

	"github.com/julienschmidt/httprouter"
	"github.com/marcmoiagese/CercaGenealogica/core/mon"
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
		line = strings.TrimSpace(line) // Eliminem espais inicials i finals
		if strings.HasPrefix(line, "DB_ENGINE=") {
			engine = strings.TrimPrefix(line, "DB_ENGINE=")
			engine = strings.TrimSpace(engine) // Netegem també el valor
		}
		if strings.HasPrefix(line, "DB_PATH=") {
			path = strings.TrimPrefix(line, "DB_PATH=")
			path = strings.TrimSpace(path) // Netegem també el valor
		}
	}
	return
}

// Adaptador per convertir httprouter.Handle a http.HandlerFunc
func adapt(fn httprouter.Handle) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fn(w, r, nil)
	}
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

	// Obtenim connexió SQL neta
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
	http.HandleFunc("/import", handlers.ImportHandler(dbManager))
	http.Handle("/static/", handlers.StaticHandler())
	http.HandleFunc("/pendents", handlers.PendentsHandler(dbManager))
	http.HandleFunc("/import-seleccionats", handlers.ImportarDuplicatsSeleccionatsHandler(dbManager))
	http.HandleFunc("/eliminar-seleccionats", handlers.EliminarDuplicatsSeleccionatsHandler(dbManager))
	http.HandleFunc("/mon", adapt(mon.ServePaisPage(dbManager)))

	fmt.Println("Servidor corrent a http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
