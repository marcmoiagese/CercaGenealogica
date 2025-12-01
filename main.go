package main

import (
	"log"
	"net/http"
	"os"
	"strings"
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

	configMap, err := cnf.LoadConfig("cnf/config.cfg")
	if err != nil {
		log.Fatalf("No s'ha pogut carregar config: %v", err)
	}

	appCfg, err := cnf.ParseConfig(configMap)
	if err != nil {
		log.Fatalf("Config invàlida: %v", err)
	}

	core.SetLogLevel(appCfg.LogLevel)
	core.LogLoadedTemplates()

	core.InitWebServer(configMap)

	dbInstance, err := db.NewDB(configMap)
	if err != nil {
		log.Fatalf("Error inicialitzant BD: %v", err)
	}
	app := core.NewApp(configMap, dbInstance)
	defer app.Close()

	// Serveix recursos estàtics amb middleware de seguretat
	http.HandleFunc("/static/", applyMiddleware(core.ServeStatic, core.BlockIPs, core.RateLimit))
	//http.HandleFunc("/static/", core.SecureHeaders(core.BlockIPs(core.RateLimit(http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))))

	// Rutes HTML
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if user, authenticated := app.VerificarSessio(r); authenticated {
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
		user, authenticated := app.VerificarSessio(r)
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

	http.HandleFunc("/registre", applyMiddleware(app.RegistrarUsuari, core.BlockIPs, core.RateLimit))

	http.HandleFunc("/login", applyMiddleware(app.IniciarSessio, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/logout", applyMiddleware(app.TancarSessio, core.BlockIPs))

	http.HandleFunc("/condicions-us", func(w http.ResponseWriter, r *http.Request) {
		core.RenderTemplate(w, r, "condicions-us.html", map[string]interface{}{
			"DataActualitzacio": "Gener 2024",
		})
	})

	// Canvi d'idioma via ruta /{lang}/ amb cookie + redirect
	handleLang := func(lang string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			expiry := time.Now().Add(365 * 24 * time.Hour)
			env := strings.ToLower(os.Getenv("ENVIRONMENT"))
			secure := true
			sameSite := http.SameSiteStrictMode
			if env == "development" {
				secure = r.TLS != nil
				sameSite = http.SameSiteLaxMode
			}
			http.SetCookie(w, &http.Cookie{
				Name:     "lang",
				Value:    lang,
				Expires:  expiry,
				Path:     "/",
				HttpOnly: false,
				SameSite: sameSite,
				Secure:   secure,
			})
			log.Printf("[lang] canvi a %s des de %s", lang, r.RemoteAddr)
			http.Redirect(w, r, "/", http.StatusSeeOther)
		}
	}
	http.HandleFunc("/cat/", handleLang("cat"))
	http.HandleFunc("/en/", handleLang("en"))
	http.HandleFunc("/oc/", handleLang("oc"))

	http.HandleFunc("/regenerar-token", func(w http.ResponseWriter, r *http.Request) {
		handler := func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" {
				app.MostrarFormulariRegenerarToken(w, r)
			} else if r.Method == "POST" {
				app.ProcessarRegenerarToken(w, r)
			}
		}
		applyMiddleware(handler, core.BlockIPs, core.RateLimit)(w, r)
	})

	http.HandleFunc("/activar", applyMiddleware(app.ActivarUsuariHTTP, core.BlockIPs))

	log.Println("Servidor iniciat a http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
