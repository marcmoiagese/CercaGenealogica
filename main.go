package main

import (
	"log"
	"net/http"
	"time"

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
		if user, authenticated := core.VerificarSessio(r); authenticated {
			log.Printf("[auth] usuari %s ja autenticat, redirigint / -> /inici", user.Usuari)
			http.Redirect(w, r, "/inici", http.StatusSeeOther)
			return
		}
		core.RenderTemplate(w, r, "index.html", map[string]interface{}{
			"CSRFToken": "token-segon",
		})
	})

	http.HandleFunc("/inici", func(w http.ResponseWriter, r *http.Request) {
		// Verificar si l'usuari té una sessió vàlida
		user, authenticated := core.VerificarSessio(r)
		if !authenticated {
			// Redirigir a la pàgina principal si no té sessió
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}

		// Renderitzar la pàgina privada amb les dades de l'usuari
		core.RenderPrivateTemplate(w, r, "index-logedin.html", map[string]interface{}{
			"User": user,
		})
	})

	http.HandleFunc("/registre", core.RegistrarUsuari)

	http.HandleFunc("/login", core.IniciarSessio)
	http.HandleFunc("/logout", core.TancarSessio)

	http.HandleFunc("/condicions-us", func(w http.ResponseWriter, r *http.Request) {
		core.RenderTemplate(w, r, "condicions-us.html", map[string]interface{}{
			"DataActualitzacio": "Gener 2024",
		})
	})

	// Canvi d'idioma via ruta /{lang}/ amb cookie + redirect
	handleLang := func(lang string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			expiry := time.Now().Add(365 * 24 * time.Hour)
			http.SetCookie(w, &http.Cookie{
				Name:     "lang",
				Value:    lang,
				Expires:  expiry,
				Path:     "/",
				HttpOnly: false,
				SameSite: http.SameSiteLaxMode,
			})
			log.Printf("[lang] canvi a %s des de %s", lang, r.RemoteAddr)
			http.Redirect(w, r, "/", http.StatusSeeOther)
		}
	}
	http.HandleFunc("/cat/", handleLang("cat"))
	http.HandleFunc("/en/", handleLang("en"))
	http.HandleFunc("/oc/", handleLang("oc"))

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
