package main

import (
	"bufio"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/core"
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

// serveStatic – Serveix només fitxers CSS, JS, imatges...
func serveStatic(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path[len("/static/"):]

	// Evita llistat de carpetes
	if strings.HasSuffix(path, "/") || path == "" {
		http.Error(w, "Accés denegat", http.StatusForbidden)
		return
	}

	// Bloqueja extensions prohibides
	ext := filepath.Ext(path)
	switch ext {
	case ".go", ".sql", ".env", ".cfg", ".sh", ".php", ".txt":
		http.Error(w, "Accés denegat", http.StatusForbidden)
		return
	}

	// Serveix només fitxers existents
	http.ServeFile(w, r, filepath.Join("static", path))
}

func main() {
	config := LoadConfig("cnf/config.cfg")

	dbInstance, err := db.NewDB(config)
	if err != nil {
		log.Fatalf("Error inicialitzant BD: %v", err)
	}
	defer dbInstance.Close()

	// Carrega configuració, inicia connexió BD...
	log.Println("Servidor iniciant-se...")

	// Aplica middleware a /static/
	http.HandleFunc("/static/", core.secureHeaders(core.serveStatic))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		core.RenderTemplate(w, "index", nil)
	})

	http.HandleFunc("/inici", func(w http.ResponseWriter, r *http.Request) {
		core.RenderPrivateTemplate(w, "index-logedin", nil)
	})

	http.HandleFunc("/debug", func(w http.ResponseWriter, r *http.Request) {
		tmpl := core.Templates.Lookup("base.html")
		if tmpl == nil {
			http.Error(w, "Plantilla 'base.html' no existeix", http.StatusInternalServerError)
			return
		}

		err := tmpl.Execute(w, nil)
		if err != nil {
			http.Error(w, "Error executant la plantilla: "+err.Error(), http.StatusInternalServerError)
		}
	})

	log.Println("Servidor iniciat a http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
