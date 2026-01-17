package main

import (
	"log"
	"net/http"
	"os"
	"strconv"
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

func extractID(path string) int {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for _, part := range parts {
		if id, err := strconv.Atoi(part); err == nil {
			return id
		}
	}
	return 0
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
	_ = dbInstance.EnsureDefaultPointsRules()
	app := core.NewApp(configMap, dbInstance)
	if err := app.EnsurePolicyGrants(); err != nil {
		log.Printf("[permissions] error assegurant grants per polítiques: %v", err)
	}
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

	http.HandleFunc("/inici", app.RequireLogin(func(w http.ResponseWriter, r *http.Request) {
		// Verificar si l'usuari té una sessió vàlida
		user, authenticated := app.VerificarSessio(r)
		if !authenticated || user == nil {
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
	}))

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
	http.HandleFunc("/perfil/credits/convert", applyMiddleware(app.RequireLogin(app.ConvertPointsToCredits), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/perfil/email-confirm", applyMiddleware(app.ConfirmarCanviEmail, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/perfil/email-revert", applyMiddleware(app.RevertirCanviEmail, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/ranking", applyMiddleware(app.Ranking, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/u/", applyMiddleware(app.PublicUserProfile, core.BlockIPs, core.RateLimit))

	// Cognoms
	http.HandleFunc("/cognoms", applyMiddleware(app.RequireLogin(app.CognomsList), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/cognoms/cerca", applyMiddleware(app.RequireLogin(app.SearchCognomsJSON), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/cognoms/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/cognoms/")
		parts := strings.Split(strings.Trim(path, "/"), "/")
		if len(parts) == 0 || parts[0] == "" {
			http.NotFound(w, r)
			return
		}
		if len(parts) >= 3 && parts[1] == "variants" && parts[2] == "suggest" && r.Method == http.MethodPost {
			applyMiddleware(app.RequireLogin(app.CognomSuggestVariant), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if len(parts) == 2 && parts[1] == "mapa" && r.Method == http.MethodGet {
			applyMiddleware(app.RequireLogin(app.CognomMapa), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if len(parts) == 1 && r.Method == http.MethodGet {
			applyMiddleware(app.RequireLogin(app.CognomDetall), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		http.NotFound(w, r)
	})
	http.HandleFunc("/api/cognoms/", applyMiddleware(app.RequireLogin(app.CognomHeatmapJSON), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/territori/municipis/suggest", applyMiddleware(app.RequireLogin(app.AdminMunicipisSuggest), core.BlockIPs, core.RateLimit))

	// Arxius (lectura per a tots els usuaris autenticats)
	http.HandleFunc("/arxius", applyMiddleware(app.ListArxius, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/arxius/", applyMiddleware(app.ShowArxiu, core.BlockIPs, core.RateLimit))

	// Media
	http.HandleFunc("/media/albums", applyMiddleware(app.RequireLogin(app.MediaAlbums), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/media/albums/new", applyMiddleware(app.RequireLogin(app.MediaAlbumNew), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/media/albums/", applyMiddleware(app.MediaAlbumDetail, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/media/items/", applyMiddleware(app.MediaItemRoute, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/media/dz/", applyMiddleware(app.MediaDeepZoom, core.BlockIPs, core.RateLimit))

	// Persones (bàsic: llista pública i creació/edició amb moderació)
	http.HandleFunc("/persones/cerca", applyMiddleware(app.AdminSearchPersonesJSON, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/persones", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			applyMiddleware(app.RequireLogin(app.ListPersonesPublic), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if r.Method == http.MethodPost {
			applyMiddleware(app.CreatePersona, core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		http.NotFound(w, r)
	})
	http.HandleFunc("/persones/new", applyMiddleware(app.PersonaForm, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/persones/save", applyMiddleware(app.PersonaSave, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/persones/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/registres") && r.Method == http.MethodGet {
			applyMiddleware(app.RequireLogin(app.PersonaRegistres), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/edit") && r.Method == http.MethodGet {
			applyMiddleware(app.PersonaForm, core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if r.Method == http.MethodGet {
			applyMiddleware(app.RequireLogin(app.PersonaDetall), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if r.Method == http.MethodPut || r.Method == http.MethodPost {
			applyMiddleware(app.UpdatePersona, core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		http.NotFound(w, r)
	})

	// Admin països
	http.HandleFunc("/admin/paisos", applyMiddleware(app.AdminListPaisos, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/paisos/new", applyMiddleware(app.AdminNewPais, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/paisos/save", applyMiddleware(app.AdminSavePais, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/paisos/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/edit") {
			applyMiddleware(app.AdminEditPais, core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		http.NotFound(w, r)
	})
	http.HandleFunc("/territori/paisos/", func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/nivells") {
			http.NotFound(w, r)
			return
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/nivells"):
			applyMiddleware(app.AdminListNivells, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/nivells/new"):
			applyMiddleware(app.AdminNewNivell, core.BlockIPs, core.RateLimit)(w, r)
		default:
			http.NotFound(w, r)
		}
	})
	http.HandleFunc("/territori/nivells", applyMiddleware(app.AdminListNivells, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/territori/nivells/save", applyMiddleware(app.AdminSaveNivell, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/territori/nivells/", func(w http.ResponseWriter, r *http.Request) {
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
	http.HandleFunc("/territori/municipis", func(w http.ResponseWriter, r *http.Request) {
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
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/delete"):
			applyMiddleware(app.AdminDeleteMunicipi, core.BlockIPs, core.RateLimit)(w, r)
		default:
			applyMiddleware(app.AdminListMunicipis, core.BlockIPs, core.RateLimit)(w, r)
		}
	})
	http.HandleFunc("/territori/municipis/", func(w http.ResponseWriter, r *http.Request) {
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
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/delete"):
			applyMiddleware(app.AdminDeleteMunicipi, core.BlockIPs, core.RateLimit)(w, r)
		default:
			if r.Method == http.MethodGet {
				parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
				if len(parts) >= 3 {
					if _, err := strconv.Atoi(parts[2]); err == nil {
						applyMiddleware(app.MunicipiPublic, core.BlockIPs, core.RateLimit)(w, r)
						return
					}
				}
			}
			applyMiddleware(app.AdminListMunicipis, core.BlockIPs, core.RateLimit)(w, r)
			return
		}
	})

	// Entitats eclesiàstiques
	http.HandleFunc("/territori/eclesiastic", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/new"):
			applyMiddleware(app.AdminNewEclesiastic, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/edit"):
			applyMiddleware(app.AdminEditEclesiastic, core.BlockIPs, core.RateLimit)(w, r)
		default:
			applyMiddleware(app.AdminListEclesiastic, core.BlockIPs, core.RateLimit)(w, r)
		}
	})
	http.HandleFunc("/territori/eclesiastic/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/new"):
			applyMiddleware(app.AdminNewEclesiastic, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/edit"):
			applyMiddleware(app.AdminEditEclesiastic, core.BlockIPs, core.RateLimit)(w, r)
		default:
			applyMiddleware(app.AdminListEclesiastic, core.BlockIPs, core.RateLimit)(w, r)
		}
	})
	http.HandleFunc("/territori/eclesiastic/save", applyMiddleware(app.AdminSaveEclesiastic, core.BlockIPs, core.RateLimit))

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
	http.HandleFunc("/admin/politiques/", func(w http.ResponseWriter, r *http.Request) {
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
	http.HandleFunc("/admin/politiques/grants/save", applyMiddleware(app.AdminSavePoliticaGrant, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/politiques/grants/delete", applyMiddleware(app.AdminDeletePoliticaGrant, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/politiques/assignacions", applyMiddleware(app.AdminAssignacionsPolitiques, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/politiques/assignar-usuari", applyMiddleware(app.AdminAssignarPoliticaUsuari, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/politiques/treure-usuari", applyMiddleware(app.AdminTreurePoliticaUsuari, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/politiques/assignar-grup", applyMiddleware(app.AdminAssignarPoliticaGrup, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/politiques/treure-grup", applyMiddleware(app.AdminTreurePoliticaGrup, core.BlockIPs, core.RateLimit))

	// Usuaris (administració)
	http.HandleFunc("/admin/usuaris", applyMiddleware(app.AdminListUsuaris, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/usuaris/actiu", applyMiddleware(app.AdminSetUserActive, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/usuaris/ban", applyMiddleware(app.AdminSetUserBanned, core.BlockIPs, core.RateLimit))

	// Regles de punts
	http.HandleFunc("/admin/punts/regles", applyMiddleware(app.AdminListPuntsRegles, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/punts/regles/new", applyMiddleware(app.AdminNewPuntsRegla, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/punts/regles/save", applyMiddleware(app.AdminSavePuntsRegla, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/punts/regles/recalc", applyMiddleware(app.AdminRecalcPunts, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/punts/regles/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/edit") {
			applyMiddleware(app.AdminEditPuntsRegla, core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		http.NotFound(w, r)
	})
	// Cognoms import/statistics
	http.HandleFunc("/admin/cognoms/import", applyMiddleware(app.AdminCognomsImport, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/cognoms/import/run", applyMiddleware(app.AdminCognomsImportRun, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/cognoms/stats/run", applyMiddleware(app.AdminCognomsStatsRun, core.BlockIPs, core.RateLimit))
	// Territori import/export
	http.HandleFunc("/admin/territori/import", applyMiddleware(app.AdminTerritoriImport, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/territori/import/run", applyMiddleware(app.AdminTerritoriImportRun, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/territori/export", applyMiddleware(app.AdminTerritoriExport, core.BlockIPs, core.RateLimit))
	// Entitats eclesiàstiques import/export
	http.HandleFunc("/admin/eclesiastic/import", applyMiddleware(app.AdminEclesiasticImport, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/eclesiastic/import/run", applyMiddleware(app.AdminEclesiasticImportRun, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/eclesiastic/export", applyMiddleware(app.AdminEclesiasticExport, core.BlockIPs, core.RateLimit))
	// Arxius import/export
	http.HandleFunc("/admin/arxius/import", applyMiddleware(app.AdminArxiusImport, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/arxius/import/run", applyMiddleware(app.AdminArxiusImportRun, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/arxius/export", applyMiddleware(app.AdminArxiusExport, core.BlockIPs, core.RateLimit))

	// Moderació
	http.HandleFunc("/moderacio", applyMiddleware(app.AdminModeracioList, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/moderacio/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/bulk"):
			applyMiddleware(app.AdminModeracioBulk, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/aprovar"):
			applyMiddleware(app.AdminModeracioAprovar, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/rebutjar"):
			applyMiddleware(app.AdminModeracioRebutjar, core.BlockIPs, core.RateLimit)(w, r)
		default:
			http.NotFound(w, r)
		}
	})
	http.HandleFunc("/admin/moderacio/media", applyMiddleware(app.AdminModeracioMediaList, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/moderacio/media/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/albums/") && strings.HasSuffix(r.URL.Path, "/approve"):
			applyMiddleware(app.AdminModeracioMediaAlbumApprove, core.BlockIPs, core.RateLimit)(w, r)
		case strings.Contains(r.URL.Path, "/albums/") && strings.HasSuffix(r.URL.Path, "/reject"):
			applyMiddleware(app.AdminModeracioMediaAlbumReject, core.BlockIPs, core.RateLimit)(w, r)
		case strings.Contains(r.URL.Path, "/items/") && strings.HasSuffix(r.URL.Path, "/approve"):
			applyMiddleware(app.AdminModeracioMediaItemApprove, core.BlockIPs, core.RateLimit)(w, r)
		case strings.Contains(r.URL.Path, "/items/") && strings.HasSuffix(r.URL.Path, "/reject"):
			applyMiddleware(app.AdminModeracioMediaItemReject, core.BlockIPs, core.RateLimit)(w, r)
		default:
			http.NotFound(w, r)
		}
	})

	// Admin arxius
	http.HandleFunc("/documentals/arxius", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			applyMiddleware(app.AdminCreateArxiu, core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		applyMiddleware(app.AdminListArxius, core.BlockIPs, core.RateLimit)(w, r)
	})
	http.HandleFunc("/documentals/arxius/new", applyMiddleware(app.AdminNewArxiu, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/documentals/arxius/", func(w http.ResponseWriter, r *http.Request) {
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
	http.HandleFunc("/documentals/llibres", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/indexar/draft") && r.Method == http.MethodPost:
			applyMiddleware(app.AdminIndexarDraft, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/indexar/clear") && r.Method == http.MethodPost:
			applyMiddleware(app.AdminClearIndexerDraft, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/indexar/commit") && r.Method == http.MethodPost:
			applyMiddleware(app.AdminCommitIndexer, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/indexar"):
			applyMiddleware(app.AdminIndexarLlibre, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/new"):
			applyMiddleware(app.AdminNewLlibre, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/importar/errors"):
			applyMiddleware(app.AdminDownloadImportErrors, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/importar"):
			applyMiddleware(app.AdminImportRegistresGlobalView, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/importar"):
			applyMiddleware(app.AdminImportRegistresGlobal, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/import"):
			applyMiddleware(app.AdminImportRegistresView, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/import/errors"):
			applyMiddleware(app.AdminDownloadImportErrors, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/export"):
			applyMiddleware(app.AdminExportRegistresLlibre, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/import"):
			applyMiddleware(app.AdminImportRegistresLlibre, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/indexacio"):
			applyMiddleware(app.AdminToggleIndexacioLlibre, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/indexacio/recalc"):
			applyMiddleware(app.AdminRecalcIndexacioLlibre, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/edit"):
			applyMiddleware(app.AdminEditLlibre, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/registres/nou"):
			applyMiddleware(app.AdminNewRegistre, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/registres") && r.Method == http.MethodPost:
			applyMiddleware(app.AdminCreateRegistre, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/registres"):
			applyMiddleware(app.AdminListRegistresLlibre, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/pagines/stats/save"):
			applyMiddleware(app.AdminUpdateLlibrePageStat, core.BlockIPs, core.RateLimit)(w, r)
		case strings.Contains(r.URL.Path, "/pagines"):
			http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(extractID(r.URL.Path))+"/indexar", http.StatusSeeOther)
		case strings.Contains(r.URL.Path, "/arxius/") && strings.HasSuffix(r.URL.Path, "/edit"):
			applyMiddleware(app.AdminEditLlibreArxiuLinks, core.BlockIPs, core.RateLimit)(w, r)
		case strings.Contains(r.URL.Path, "/arxius/") && strings.HasSuffix(r.URL.Path, "/update"):
			applyMiddleware(app.AdminUpdateLlibreArxiu, core.BlockIPs, core.RateLimit)(w, r)
		case strings.Contains(r.URL.Path, "/arxius/") && strings.HasSuffix(r.URL.Path, "/delete"):
			applyMiddleware(app.AdminDeleteLlibreArxiu, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/arxius/add"):
			applyMiddleware(app.AdminAddLlibreArxiu, core.BlockIPs, core.RateLimit)(w, r)
		case strings.Contains(r.URL.Path, "/urls/") && strings.HasSuffix(r.URL.Path, "/delete"):
			applyMiddleware(app.AdminDeleteLlibreURL, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/urls/add"):
			applyMiddleware(app.AdminAddLlibreURL, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/save"):
			applyMiddleware(app.AdminSaveLlibre, core.BlockIPs, core.RateLimit)(w, r)
		case r.URL.Path != "/documentals/llibres":
			applyMiddleware(app.AdminShowLlibre, core.BlockIPs, core.RateLimit)(w, r)
		default:
			applyMiddleware(app.AdminListLlibres, core.BlockIPs, core.RateLimit)(w, r)
		}
	})
	http.HandleFunc("/documentals/llibres/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/registres/purge") && r.Method == http.MethodPost:
			applyMiddleware(app.AdminPurgeLlibreRegistres, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/indexar/draft") && r.Method == http.MethodPost:
			applyMiddleware(app.AdminIndexarDraft, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/indexar/clear") && r.Method == http.MethodPost:
			applyMiddleware(app.AdminClearIndexerDraft, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/indexar/commit") && r.Method == http.MethodPost:
			applyMiddleware(app.AdminCommitIndexer, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/indexar"):
			applyMiddleware(app.AdminIndexarLlibre, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/new"):
			applyMiddleware(app.AdminNewLlibre, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/importar/errors"):
			applyMiddleware(app.AdminDownloadImportErrors, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/importar"):
			applyMiddleware(app.AdminImportRegistresGlobalView, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/importar"):
			applyMiddleware(app.AdminImportRegistresGlobal, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/import"):
			applyMiddleware(app.AdminImportRegistresView, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/import/errors"):
			applyMiddleware(app.AdminDownloadImportErrors, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/export"):
			applyMiddleware(app.AdminExportRegistresLlibre, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/import"):
			applyMiddleware(app.AdminImportRegistresLlibre, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/indexacio"):
			applyMiddleware(app.AdminToggleIndexacioLlibre, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/indexacio/recalc"):
			applyMiddleware(app.AdminRecalcIndexacioLlibre, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/edit"):
			applyMiddleware(app.AdminEditLlibre, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/registres/nou"):
			applyMiddleware(app.AdminNewRegistre, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/registres") && r.Method == http.MethodPost:
			applyMiddleware(app.AdminCreateRegistre, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/registres"):
			applyMiddleware(app.AdminListRegistresLlibre, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/pagines/stats/save"):
			applyMiddleware(app.AdminUpdateLlibrePageStat, core.BlockIPs, core.RateLimit)(w, r)
		case strings.Contains(r.URL.Path, "/pagines"):
			http.Redirect(w, r, "/documentals/llibres/"+strconv.Itoa(extractID(r.URL.Path))+"/indexar", http.StatusSeeOther)
		case strings.Contains(r.URL.Path, "/arxius/") && strings.HasSuffix(r.URL.Path, "/edit"):
			applyMiddleware(app.AdminEditLlibreArxiuLinks, core.BlockIPs, core.RateLimit)(w, r)
		case strings.Contains(r.URL.Path, "/arxius/") && strings.HasSuffix(r.URL.Path, "/update"):
			applyMiddleware(app.AdminUpdateLlibreArxiu, core.BlockIPs, core.RateLimit)(w, r)
		case strings.Contains(r.URL.Path, "/arxius/") && strings.HasSuffix(r.URL.Path, "/delete"):
			applyMiddleware(app.AdminDeleteLlibreArxiu, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/arxius/add"):
			applyMiddleware(app.AdminAddLlibreArxiu, core.BlockIPs, core.RateLimit)(w, r)
		case strings.Contains(r.URL.Path, "/urls/") && strings.HasSuffix(r.URL.Path, "/delete"):
			applyMiddleware(app.AdminDeleteLlibreURL, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/urls/add"):
			applyMiddleware(app.AdminAddLlibreURL, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/save"):
			applyMiddleware(app.AdminSaveLlibre, core.BlockIPs, core.RateLimit)(w, r)
		case r.URL.Path != "/documentals/llibres":
			applyMiddleware(app.AdminShowLlibre, core.BlockIPs, core.RateLimit)(w, r)
		default:
			applyMiddleware(app.AdminListLlibres, core.BlockIPs, core.RateLimit)(w, r)
		}
	})
	http.HandleFunc("/documentals/llibres/save", applyMiddleware(app.AdminSaveLlibre, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/documentals/pagines/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/media/link") && r.Method == http.MethodPost:
			applyMiddleware(app.AdminLinkMediaToPagina, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/media/unlink") && r.Method == http.MethodPost:
			applyMiddleware(app.AdminUnlinkMediaFromPagina, core.BlockIPs, core.RateLimit)(w, r)
		default:
			http.NotFound(w, r)
		}
	})
	http.HandleFunc("/documentals/registres/cercar/export", applyMiddleware(app.AdminExportRegistresGlobal, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/documentals/registres/cercar", applyMiddleware(app.AdminSearchRegistres, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/documentals/registres/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/marcar"):
			applyMiddleware(app.AdminSetRegistreMark, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/desmarcar"):
			applyMiddleware(app.AdminClearRegistreMark, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/inline"):
			applyMiddleware(app.AdminInlineUpdateRegistreField, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/persones") && r.Method == http.MethodGet:
			applyMiddleware(app.AdminListRegistrePersonesJSON, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/convertir"):
			applyMiddleware(app.AdminConvertRegistreToPersona, core.BlockIPs, core.RateLimit)(w, r)
		case strings.Contains(r.URL.Path, "/persones/") && strings.HasSuffix(r.URL.Path, "/enllacar"):
			applyMiddleware(app.AdminLinkPersonaToRaw, core.BlockIPs, core.RateLimit)(w, r)
		case strings.Contains(r.URL.Path, "/persones/") && strings.HasSuffix(r.URL.Path, "/desenllacar"):
			applyMiddleware(app.AdminUnlinkPersonaFromRaw, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/historial/revert"):
			applyMiddleware(app.AdminRevertRegistreChange, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/historial"):
			applyMiddleware(app.AdminRegistreHistory, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/estadistiques"):
			applyMiddleware(app.AdminRegistreStats, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/editar"):
			if r.Method == http.MethodPost {
				applyMiddleware(app.AdminUpdateRegistre, core.BlockIPs, core.RateLimit)(w, r)
			} else {
				applyMiddleware(app.AdminEditRegistre, core.BlockIPs, core.RateLimit)(w, r)
			}
		case strings.HasSuffix(r.URL.Path, "/delete"):
			applyMiddleware(app.AdminDeleteRegistre, core.BlockIPs, core.RateLimit)(w, r)
		default:
			applyMiddleware(app.AdminShowRegistre, core.BlockIPs, core.RateLimit)(w, r)
		}
	})

	log.Println("Servidor iniciat a http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
