package main

import (
	"log"
	"net/http"

	"github.com/marcmoiagese/CercaGenealogica/cnf"
	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

func applyMiddleware(fn http.HandlerFunc, middlewares ...func(http.HandlerFunc) http.HandlerFunc) http.HandlerFunc {
	for _, mw := range middlewares {
		fn = mw(fn)
	}
	return fn
}

func main() {

	core.InitWebServer()

	config := cnf.LoadConfig("cnf/config.cfg")

	dbInstance, err := db.NewDB(config)
	if err != nil {
		log.Fatalf("Error inicialitzant BD: %v", err)
	}
	defer dbInstance.Close()

	// Serveix recursos estàtics amb middleware de seguretat
	http.HandleFunc("/static/", applyMiddleware(core.ServeStatic, core.BlockIPs, core.RateLimit))
	//http.HandleFunc("/static/", core.SecureHeaders(core.BlockIPs(core.RateLimit(http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))))

	// Rutes HTML
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		core.RenderTemplate(w, "index.html", map[string]interface{}{
			"CSRFToken": "token-segon",
		})
	})

	http.HandleFunc("/inici", func(w http.ResponseWriter, r *http.Request) {
		core.RenderPrivateTemplate(w, "index-logedin", nil)
	})

	http.HandleFunc("/registre", core.RegistrarUsuari)

	http.HandleFunc("/condicions-us", func(w http.ResponseWriter, r *http.Request) {
		core.RenderTemplate(w, "condicions-us.html", map[string]interface{}{
			"DataActualitzacio": "Gener 2024",
		})
	})

	http.HandleFunc("/regenerar-token", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			core.MostrarFormulariRegenerarToken(w, r)
		} else if r.Method == "POST" {
			core.ProcessarRegenerarToken(w, r)
		}
	})

	http.HandleFunc("/activar", core.ActivarUsuariHTTP)

	log.Println("Servidor iniciat a http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
