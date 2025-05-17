package main

import (
	"fmt"
	arquevisbats "go-fesme/modules/Importacio/Arquevisbats"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/julienschmidt/httprouter"

	"./core/cerca"
	"./db"
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
	engine, path := loadConfig()
	dbManager, err := db.GetDBManager(engine)
	if err != nil {
		log.Fatal(err)
	}
	err = dbManager.Init()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	router := httprouter.New()

	// Rutes web
	router.GET("/", func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		http.ServeFile(w, r, "web/templates/index.html")
	})
	router.POST("/import", arquevisbats.HandleImport(&dbManager))
	router.GET("/cerca", cerca.CercaHandler(dbManager.db))
	router.GET("/pendents", func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		dups, _ := dbManager.GetPossibleDuplicates()
		// TODO: render template amb dups
	})

	// Serve static
	fs := http.FileServer(http.Dir("web/static"))
	router.Handler("GET", "/static/*filepath", http.StripPrefix("/static/", fs))

	fmt.Println("Servidor corrent a http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", router))
}
