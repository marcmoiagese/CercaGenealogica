package main

import (
	"bufio"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func LoadConfig(path string) map[string]string {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("No s'ha pogut obrir el fitxer de configuració: %v", err)
	}
	defer file.Close()

	config := make(map[string]string)
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "=") {
			parts := strings.SplitN(line, "=", 2)
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			config[key] = value
		}
	}

	return config
}

func main() {
	config := LoadConfig("cnf/config.cfg")

	dbInstance, err := db.NewDB(config)
	if err != nil {
		log.Fatalf("Error inicialitzant BD: %v", err)
	}
	defer dbInstance.Close()

	fs := http.FileServer(http.Dir("static"))
	http.Handle("/static/", http.StripPrefix("/static/", fs))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		RenderTemplate(w, "index", nil)
	})

	http.HandleFunc("/inici", func(w http.ResponseWriter, r *http.Request) {
		RenderPrivateTemplate(w, "index-logedin", nil)
	})

	log.Println("Servidor iniciat a http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
