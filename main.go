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
	_ = dbInstance.EnsureDefaultPolicies()
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
		lang := core.ResolveLang(r)
		if pref := strings.TrimSpace(user.PreferredLang); pref != "" {
			lang = pref
		}
		canManageArxius := app.CanManageArxius(user)
		core.RenderPrivateTemplateLang(w, r, "index-logedin.html", lang, map[string]interface{}{
			"User":            user,
			"CanManageArxius": canManageArxius,
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
			secure := r.TLS != nil
			sameSite := http.SameSiteStrictMode
			if env == "development" {
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
			target := r.Header.Get("Referer")
			if target == "" {
				target = "/"
			}
			http.Redirect(w, r, target, http.StatusSeeOther)
		}
	}
	http.HandleFunc("/cat/", handleLang("cat"))
	http.HandleFunc("/en/", handleLang("en"))
	http.HandleFunc("/oc/", handleLang("oc"))

	http.HandleFunc("/api/check-availability", applyMiddleware(app.CheckAvailability, core.BlockIPs, core.RateLimit))

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
	http.HandleFunc("/recuperar", applyMiddleware(app.GestionarRecuperacio, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/perfil", applyMiddleware(app.Perfil, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/perfil/dades", applyMiddleware(app.ActualitzarPerfilDades, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/perfil/privacitat", applyMiddleware(app.ActualitzarPerfilPrivacitat, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/perfil/contrasenya", applyMiddleware(app.ActualitzarPerfilContrasenya, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/perfil/email-confirm", applyMiddleware(app.ConfirmarCanviEmail, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/perfil/email-revert", applyMiddleware(app.RevertirCanviEmail, core.BlockIPs, core.RateLimit))

	// Arxius (lectura per a tots els usuaris autenticats)
	http.HandleFunc("/arxius", applyMiddleware(app.ListArxius, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/arxius/", applyMiddleware(app.ShowArxiu, core.BlockIPs, core.RateLimit))

	// Admin països
	http.HandleFunc("/admin/paisos", applyMiddleware(app.AdminListPaisos, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/paisos/new", applyMiddleware(app.AdminNewPais, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/paisos/save", applyMiddleware(app.AdminSavePais, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/paisos/", func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/nivells") {
			switch {
			case strings.HasSuffix(r.URL.Path, "/nivells"):
				applyMiddleware(app.AdminListNivells, core.BlockIPs, core.RateLimit)(w, r)
			case strings.HasSuffix(r.URL.Path, "/nivells/new"):
				applyMiddleware(app.AdminNewNivell, core.BlockIPs, core.RateLimit)(w, r)
			default:
				http.NotFound(w, r)
			}
			return
		}
		if strings.HasSuffix(r.URL.Path, "/edit") {
			applyMiddleware(app.AdminEditPais, core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		http.NotFound(w, r)
	})
	http.HandleFunc("/admin/nivells", applyMiddleware(app.AdminListNivells, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/nivells/save", applyMiddleware(app.AdminSaveNivell, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/nivells/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/edit") {
			applyMiddleware(app.AdminEditNivell, core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if strings.Contains(r.URL.Path, "/noms/") && strings.HasSuffix(r.URL.Path, "/save") {
			applyMiddleware(app.AdminSaveNivellNomHistoric, core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		http.NotFound(w, r)
	})

	// Admin municipis
	http.HandleFunc("/admin/municipis", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/new"):
			applyMiddleware(app.AdminNewMunicipi, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/edit"):
			applyMiddleware(app.AdminEditMunicipi, core.BlockIPs, core.RateLimit)(w, r)
		case strings.Contains(r.URL.Path, "/codis/") && strings.HasSuffix(r.URL.Path, "/save"):
			applyMiddleware(app.AdminSaveCodiPostal, core.BlockIPs, core.RateLimit)(w, r)
		case strings.Contains(r.URL.Path, "/noms/") && strings.HasSuffix(r.URL.Path, "/save"):
			applyMiddleware(app.AdminSaveMunicipiNomHistoric, core.BlockIPs, core.RateLimit)(w, r)
		case strings.Contains(r.URL.Path, "/ecles/") && strings.HasSuffix(r.URL.Path, "/save"):
			applyMiddleware(app.AdminSaveMunicipiEcles, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/save"):
			applyMiddleware(app.AdminSaveMunicipi, core.BlockIPs, core.RateLimit)(w, r)
		default:
			applyMiddleware(app.AdminListMunicipis, core.BlockIPs, core.RateLimit)(w, r)
		}
	})

	// Entitats eclesiàstiques
	http.HandleFunc("/admin/eclesiastic", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/new"):
			applyMiddleware(app.AdminNewEclesiastic, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/edit"):
			applyMiddleware(app.AdminEditEclesiastic, core.BlockIPs, core.RateLimit)(w, r)
		default:
			applyMiddleware(app.AdminListEclesiastic, core.BlockIPs, core.RateLimit)(w, r)
		}
	})
	http.HandleFunc("/admin/eclesiastic/save", applyMiddleware(app.AdminSaveEclesiastic, core.BlockIPs, core.RateLimit))

	// Polítiques / permisos
	http.HandleFunc("/admin/politiques", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/new"):
			applyMiddleware(app.AdminNewPolitica, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/edit"):
			applyMiddleware(app.AdminEditPolitica, core.BlockIPs, core.RateLimit)(w, r)
		default:
			applyMiddleware(app.AdminListPolitiques, core.BlockIPs, core.RateLimit)(w, r)
		}
	})
	http.HandleFunc("/admin/politiques/save", applyMiddleware(app.AdminSavePolitica, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/politiques/assignacions", applyMiddleware(app.AdminAssignacionsPolitiques, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/politiques/assignar-usuari", applyMiddleware(app.AdminAssignarPoliticaUsuari, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/politiques/treure-usuari", applyMiddleware(app.AdminTreurePoliticaUsuari, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/politiques/assignar-grup", applyMiddleware(app.AdminAssignarPoliticaGrup, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/politiques/treure-grup", applyMiddleware(app.AdminTreurePoliticaGrup, core.BlockIPs, core.RateLimit))

	// Moderació
	http.HandleFunc("/admin/moderacio", applyMiddleware(app.AdminModeracioList, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/moderacio/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/aprovar"):
			applyMiddleware(app.AdminModeracioAprovar, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/rebutjar"):
			applyMiddleware(app.AdminModeracioRebutjar, core.BlockIPs, core.RateLimit)(w, r)
		default:
			http.NotFound(w, r)
		}
	})

	// Admin arxius
	http.HandleFunc("/admin/arxius", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			applyMiddleware(app.AdminCreateArxiu, core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		applyMiddleware(app.AdminListArxius, core.BlockIPs, core.RateLimit)(w, r)
	})
	http.HandleFunc("/admin/arxius/new", applyMiddleware(app.AdminNewArxiu, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/arxius/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case strings.HasSuffix(path, "/edit"):
			applyMiddleware(app.AdminEditArxiu, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(path, "/delete"):
			applyMiddleware(app.AdminDeleteArxiu, core.BlockIPs, core.RateLimit)(w, r)
		case strings.Contains(path, "/llibres/") && strings.HasSuffix(path, "/update"):
			applyMiddleware(app.AdminUpdateArxiuLlibre, core.BlockIPs, core.RateLimit)(w, r)
		case strings.Contains(path, "/llibres/") && strings.HasSuffix(path, "/delete"):
			applyMiddleware(app.AdminDeleteArxiuLlibre, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(path, "/llibres/add"):
			applyMiddleware(app.AdminAddArxiuLlibre, core.BlockIPs, core.RateLimit)(w, r)
		default:
			if r.Method == http.MethodPost {
				applyMiddleware(app.AdminUpdateArxiu, core.BlockIPs, core.RateLimit)(w, r)
			} else {
				applyMiddleware(app.AdminShowArxiu, core.BlockIPs, core.RateLimit)(w, r)
			}
		}
	})
	// Admin llibres
	http.HandleFunc("/admin/llibres", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/new"):
			applyMiddleware(app.AdminNewLlibre, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/edit"):
			applyMiddleware(app.AdminEditLlibre, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/pagines/save"):
			applyMiddleware(app.AdminSaveLlibrePagina, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/pagines"):
			applyMiddleware(app.AdminLlibrePagines, core.BlockIPs, core.RateLimit)(w, r)
		case strings.Contains(r.URL.Path, "/arxius/") && strings.HasSuffix(r.URL.Path, "/update"):
			applyMiddleware(app.AdminUpdateLlibreArxiu, core.BlockIPs, core.RateLimit)(w, r)
		case strings.Contains(r.URL.Path, "/arxius/") && strings.HasSuffix(r.URL.Path, "/delete"):
			applyMiddleware(app.AdminDeleteLlibreArxiu, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/arxius/add"):
			applyMiddleware(app.AdminAddLlibreArxiu, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/save"):
			applyMiddleware(app.AdminSaveLlibre, core.BlockIPs, core.RateLimit)(w, r)
		case r.URL.Path != "/admin/llibres":
			applyMiddleware(app.AdminShowLlibre, core.BlockIPs, core.RateLimit)(w, r)
		default:
			applyMiddleware(app.AdminListLlibres, core.BlockIPs, core.RateLimit)(w, r)
		}
	})
	http.HandleFunc("/admin/llibres/save", applyMiddleware(app.AdminSaveLlibre, core.BlockIPs, core.RateLimit))

	log.Println("Servidor iniciat a http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
