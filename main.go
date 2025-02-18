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
	_ = dbInstance.EnsureDefaultAchievements()
	app := core.NewApp(configMap, dbInstance)
	core.SetPlatformSettingsStore(dbInstance)
	core.SetMaintenanceStore(dbInstance)
	if err := app.EnsurePolicyGrants(); err != nil {
		log.Printf("[permissions] error assegurant grants per polítiques: %v", err)
	}
	if err := app.EnsureSystemImportTemplates(); err != nil {
		log.Printf("[import-templates] error assegurant plantilles system: %v", err)
	}
	app.StartEspaiGrampsSyncWorker()
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
		user, ok := app.RequirePermissionKeyAnyScope(w, r, "home.view")
		if !ok || user == nil {
			return
		}

		// Renderitzar la pàgina privada amb les dades de l'usuari
		lang := core.ResolveLang(r)
		if pref := strings.TrimSpace(user.PreferredLang); pref != "" {
			lang = pref
		}
		canManageArxius := app.CanManageArxius(user)
		widgetStates, err := app.DashboardWidgetStates(user.ID, lang)
		if err != nil {
			log.Printf("[dashboard] error carregant widgets: %v", err)
		}
		activityWidget := app.DashboardActivityWidget(user.ID, lang, widgetStates["activity"].Settings)
		pointsWidget := app.DashboardPointsWidget(user.ID, lang, widgetStates["points"].Settings)
		core.RenderPrivateTemplateLang(w, r, "index-logedin.html", lang, map[string]interface{}{
			"User":            user,
			"CanManageArxius": canManageArxius,
			"WidgetStates":    widgetStates,
			"ActivityWidget":  activityWidget,
			"PointsWidget":    pointsWidget,
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
	http.HandleFunc("/transparencia", func(w http.ResponseWriter, r *http.Request) {
		app.TransparencyPublicPage(w, r)
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
	http.HandleFunc("/api/public/metrics", applyMiddleware(app.PublicMetricsAPI, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/public/espai/arbres/", applyMiddleware(app.EspaiPublicArbreAPI, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/dashboard/widgets", applyMiddleware(app.RequireLogin(app.DashboardWidgetsAPI), core.BlockIPs, core.RateLimit))

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
	http.HandleFunc("/espai", applyMiddleware(app.RequireLogin(app.EspaiPersonalOverview), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/persones/", applyMiddleware(app.RequireLogin(app.EspaiPersonaHandler), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/gedcom", applyMiddleware(app.RequireLogin(app.EspaiPersonalGedcom), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/gedcom/upload", applyMiddleware(app.RequireLogin(app.EspaiGedcomUpload), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/gedcom/reimport", applyMiddleware(app.RequireLogin(app.EspaiGedcomReimport), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/integracions", applyMiddleware(app.RequireLogin(app.EspaiPersonalIntegracions), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/integracions/gramps/connect", applyMiddleware(app.RequireLogin(app.EspaiGrampsConnect), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/integracions/gramps/sync", applyMiddleware(app.RequireLogin(app.EspaiGrampsSyncNow), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/notificacions/read", applyMiddleware(app.RequireLogin(app.EspaiNotificationsRead), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/notificacions/read-all", applyMiddleware(app.RequireLogin(app.EspaiNotificationsReadAll), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/notificacions/prefs", applyMiddleware(app.RequireLogin(app.EspaiNotificationsPrefs), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/privacitat/arbre", applyMiddleware(app.RequireLogin(app.EspaiPrivacyUpdateTree), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/privacitat/persona", applyMiddleware(app.RequireLogin(app.EspaiPrivacyUpdatePersona), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/coincidencies", applyMiddleware(app.RequireLogin(app.EspaiPersonalCoincidencies), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/coincidencies/decide", applyMiddleware(app.RequireLogin(app.EspaiCoincidenciesDecide), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/coincidencies/bulk", applyMiddleware(app.RequireLogin(app.EspaiCoincidenciesBulk), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/coincidencies/rebuild", applyMiddleware(app.RequireLogin(app.EspaiCoincidenciesRebuild), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/grups", applyMiddleware(app.RequireLogin(app.EspaiPersonalGrups), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/grups/create", applyMiddleware(app.RequireLogin(app.EspaiGrupsCreate), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/grups/invite", applyMiddleware(app.RequireLogin(app.EspaiGrupsInvite), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/grups/membre/accept", applyMiddleware(app.RequireLogin(app.EspaiGrupsAcceptInvite), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/grups/membre/decline", applyMiddleware(app.RequireLogin(app.EspaiGrupsDeclineInvite), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/grups/membre/update", applyMiddleware(app.RequireLogin(app.EspaiGrupsUpdateMember), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/grups/arbres/add", applyMiddleware(app.RequireLogin(app.EspaiGrupsAddTree), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/grups/arbres/remove", applyMiddleware(app.RequireLogin(app.EspaiGrupsRemoveTree), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/grups/conflictes/rebuild", applyMiddleware(app.RequireLogin(app.EspaiGrupsRebuildConflicts), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/grups/conflictes/resolve", applyMiddleware(app.RequireLogin(app.EspaiGrupsResolveConflict), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/espai/grups/arbre", applyMiddleware(app.RequireLogin(app.EspaiGrupsTreeView), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/espai/gedcom/imports", applyMiddleware(app.RequireLogin(app.EspaiGedcomImportsAPI), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/espai/gedcom/imports/", applyMiddleware(app.RequireLogin(app.EspaiGedcomImportDetailAPI), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/espai/coincidencies", applyMiddleware(app.RequireLogin(app.EspaiCoincidenciesAPI), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/perfil/achievements", applyMiddleware(app.RequireLogin(app.PerfilAchievementsAPI), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/perfil/achievements/", applyMiddleware(app.RequireLogin(app.PerfilAchievementsAPI), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/ranking", applyMiddleware(app.Ranking, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/u/bloc", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			applyMiddleware(app.RequireLogin(app.UserBlock), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		http.NotFound(w, r)
	})
	http.HandleFunc("/u/", applyMiddleware(app.PublicUserProfile, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/public/persones/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/arbre") && r.Method == http.MethodGet {
			applyMiddleware(app.PersonaPublicArbre, core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if r.Method == http.MethodGet {
			applyMiddleware(app.PersonaPublic, core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		http.NotFound(w, r)
	})
	http.HandleFunc("/public/espai/arbres/", applyMiddleware(app.EspaiPublicArbrePage, core.BlockIPs, core.RateLimit))

	// Cognoms
	http.HandleFunc("/cognoms", applyMiddleware(app.RequireLogin(app.CognomsList), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/cognoms/cerca", applyMiddleware(app.RequireLogin(app.SearchCognomsJSON), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/cognoms/merge", applyMiddleware(app.RequireLogin(app.CognomMergeSuggest), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/cerca-avancada", applyMiddleware(app.RequireLogin(app.AdvancedSearchPage), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/cognoms/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/cognoms/")
		parts := strings.Split(strings.Trim(path, "/"), "/")
		if len(parts) == 0 || parts[0] == "" {
			http.NotFound(w, r)
			return
		}
		if len(parts) >= 2 && parts[1] == "historial" && r.Method == http.MethodGet {
			applyMiddleware(app.RequireLogin(app.CognomWikiHistory), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if len(parts) >= 3 && parts[1] == "historial" && parts[2] == "revert" && r.Method == http.MethodPost {
			applyMiddleware(app.RequireLogin(app.CognomWikiRevert), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if len(parts) >= 2 && parts[1] == "compare" && r.Method == http.MethodGet {
			applyMiddleware(app.RequireLogin(app.CognomWikiHistory), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if len(parts) >= 2 && parts[1] == "revert" && r.Method == http.MethodPost {
			applyMiddleware(app.RequireLogin(app.CognomWikiRevert), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if len(parts) >= 2 && parts[1] == "estadistiques" && r.Method == http.MethodGet {
			applyMiddleware(app.RequireLogin(app.CognomWikiStats), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if len(parts) >= 2 && parts[1] == "marcar" && r.Method == http.MethodPost {
			applyMiddleware(app.RequireLogin(app.CognomWikiMark), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if len(parts) >= 2 && parts[1] == "desmarcar" && r.Method == http.MethodPost {
			applyMiddleware(app.RequireLogin(app.CognomWikiUnmark), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if len(parts) >= 2 && parts[1] == "proposar" && r.Method == http.MethodPost {
			applyMiddleware(app.RequireLogin(app.CognomProposeUpdate), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if len(parts) >= 3 && parts[1] == "historia" && parts[2] == "submit" && r.Method == http.MethodPost {
			applyMiddleware(app.RequireLogin(app.CognomSubmitHistoria), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if len(parts) >= 3 && parts[1] == "notes" && parts[2] == "submit" && r.Method == http.MethodPost {
			applyMiddleware(app.RequireLogin(app.CognomSubmitNotes), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if len(parts) >= 3 && parts[1] == "referencies" && parts[2] == "submit" && r.Method == http.MethodPost {
			applyMiddleware(app.RequireLogin(app.CognomSubmitReferencia), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if len(parts) >= 3 && parts[1] == "variants" && parts[2] == "suggest" && r.Method == http.MethodPost {
			applyMiddleware(app.RequireLogin(app.CognomSuggestVariant), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if len(parts) >= 3 && parts[1] == "merge" && parts[2] == "to" && r.Method == http.MethodPost {
			applyMiddleware(app.RequireLogin(app.CognomMergeSuggestTo), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if len(parts) >= 3 && parts[1] == "merge" && parts[2] == "from" && r.Method == http.MethodPost {
			applyMiddleware(app.RequireLogin(app.CognomMergeSuggestFrom), core.BlockIPs, core.RateLimit)(w, r)
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
	http.HandleFunc("/api/cognoms/variants/suggest", applyMiddleware(app.RequireLogin(app.CognomVariantsSuggestJSON), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/territori/paisos/suggest", applyMiddleware(app.RequireLogin(app.AdminPaisosSuggest), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/territori/nivell-admin/suggest", applyMiddleware(app.RequireLogin(app.AdminNivellAdministratiuSuggest), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/territori/nivells/suggest", applyMiddleware(app.RequireLogin(app.AdminNivellsSuggest), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/territori/municipis/suggest", applyMiddleware(app.RequireLogin(app.AdminMunicipisSuggest), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/territori/eclesiastic/suggest", applyMiddleware(app.RequireLogin(app.AdminEclesSuggest), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/territori/municipis/", applyMiddleware(app.MunicipiMapesAPI, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/municipis/", applyMiddleware(app.MunicipiMapesAPI, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/admin/municipis/", applyMiddleware(app.MunicipiDemografiaAdminAPI, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/admin/nivells/", applyMiddleware(app.NivellStatsAdminAPI, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/admin/kpis/general", applyMiddleware(app.AdminKPIsGeneralAPI, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/admin/control/kpis", applyMiddleware(app.AdminControlKPIsAPI, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/admin/control/health", applyMiddleware(app.AdminControlHealthAPI, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/admin/control/metrics", applyMiddleware(app.AdminControlMetricsAPI, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/admin/control/moderacio/summary", applyMiddleware(app.AdminControlModeracioSummaryAPI, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/admin/control/moderacio/jobs/", applyMiddleware(app.AdminControlModeracioJobStatus, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/admin/jobs", applyMiddleware(app.AdminJobsAPI, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/admin/jobs/", applyMiddleware(app.AdminJobsDetailAPI, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/admin/auditoria", applyMiddleware(app.AdminAuditAPI, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/arbre/expand", applyMiddleware(app.RequireLogin(app.ArbreExpandAPI), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/persones/", applyMiddleware(app.RequireLogin(app.PersonaArbreAPI), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/mapes/", applyMiddleware(app.MapesAPI, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/anecdotes/", applyMiddleware(app.AnecdotesAPI, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/anecdote_versions/", applyMiddleware(app.AnecdoteVersionsAPI, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/import-templates", applyMiddleware(app.RequireLogin(app.ImportTemplatesAPI), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/import-templates/", applyMiddleware(app.RequireLogin(app.ImportTemplatesAPI), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/events", applyMiddleware(app.EventsAPI, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/events/", applyMiddleware(app.EventsAPI, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/scopes/search", applyMiddleware(app.ScopeSearchAPI, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/search", applyMiddleware(app.RequireLogin(app.SearchAPI), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/documentals/arxius/suggest", applyMiddleware(app.RequireLogin(app.SearchArxiusSuggestJSON), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/documentals/llibres/suggest", applyMiddleware(app.RequireLogin(app.SearchLlibresSuggestJSON), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/documentals/registres/suggest", applyMiddleware(app.RequireLogin(app.SearchRegistresSuggestJSON), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/media/llibres/search", applyMiddleware(app.RequireLogin(app.MediaLlibresSearchJSON), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/media/llibres/pagines/suggest", applyMiddleware(app.RequireLogin(app.MediaLlibrePaginesSuggestJSON), core.BlockIPs, core.RateLimit))

	// Importador templates
	http.HandleFunc("/importador/plantilles", applyMiddleware(app.RequireLogin(app.ImportTemplatesRoute), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/importador/plantilles/", applyMiddleware(app.RequireLogin(app.ImportTemplatesRoute), core.BlockIPs, core.RateLimit))

	// Esdeveniments historics
	http.HandleFunc("/historia/events", applyMiddleware(app.EventsListPage, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/historia/events/nou", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			applyMiddleware(app.RequireLogin(app.EventHistoricNew), core.BlockIPs, core.RateLimit)(w, r)
		case http.MethodPost:
			applyMiddleware(app.RequireLogin(app.EventHistoricCreate), core.BlockIPs, core.RateLimit)(w, r)
		default:
			http.NotFound(w, r)
		}
	})
	http.HandleFunc("/historia/events/slug/", applyMiddleware(app.EventHistoricShowBySlug, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/historia/events/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/historial/revert") {
			if r.Method == http.MethodPost {
				applyMiddleware(app.RequireLogin(app.EventHistoricWikiRevert), core.BlockIPs, core.RateLimit)(w, r)
				return
			}
			http.NotFound(w, r)
			return
		}
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) >= 4 {
			switch parts[3] {
			case "editar":
				if r.Method == http.MethodGet {
					applyMiddleware(app.RequireLogin(app.EventHistoricEdit), core.BlockIPs, core.RateLimit)(w, r)
					return
				}
				if r.Method == http.MethodPost {
					applyMiddleware(app.RequireLogin(app.EventHistoricUpdate), core.BlockIPs, core.RateLimit)(w, r)
					return
				}
			case "historial":
				if r.Method == http.MethodGet {
					applyMiddleware(app.RequireLogin(app.EventHistoricWikiHistory), core.BlockIPs, core.RateLimit)(w, r)
					return
				}
			case "estadistiques":
				if r.Method == http.MethodGet {
					applyMiddleware(app.RequireLogin(app.EventHistoricWikiStats), core.BlockIPs, core.RateLimit)(w, r)
					return
				}
			case "marcar":
				if r.Method == http.MethodPost {
					applyMiddleware(app.RequireLogin(app.EventHistoricWikiMark), core.BlockIPs, core.RateLimit)(w, r)
					return
				}
			case "desmarcar":
				if r.Method == http.MethodPost {
					applyMiddleware(app.RequireLogin(app.EventHistoricWikiUnmark), core.BlockIPs, core.RateLimit)(w, r)
					return
				}
			}
		}
		if r.Method == http.MethodGet {
			applyMiddleware(app.EventHistoricShow, core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		http.NotFound(w, r)
	})

	// Missatges
	http.HandleFunc("/missatges", applyMiddleware(app.RequireLogin(app.MessagesInbox), core.BlockIPs, core.RateLimit))
	http.HandleFunc("/missatges/nou", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			applyMiddleware(app.RequireLogin(app.MessagesNew), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		http.NotFound(w, r)
	})
	http.HandleFunc("/missatges/fil/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/enviar"):
			if r.Method == http.MethodPost {
				applyMiddleware(app.RequireLogin(app.MessagesSend), core.BlockIPs, core.RateLimit)(w, r)
				return
			}
		case strings.HasSuffix(r.URL.Path, "/arxivar"):
			if r.Method == http.MethodPost {
				applyMiddleware(app.RequireLogin(app.MessagesArchive), core.BlockIPs, core.RateLimit)(w, r)
				return
			}
		case strings.HasSuffix(r.URL.Path, "/esborrar"):
			if r.Method == http.MethodPost {
				applyMiddleware(app.RequireLogin(app.MessagesDelete), core.BlockIPs, core.RateLimit)(w, r)
				return
			}
		case strings.HasSuffix(r.URL.Path, "/bloc"):
			if r.Method == http.MethodPost {
				applyMiddleware(app.RequireLogin(app.MessagesBlock), core.BlockIPs, core.RateLimit)(w, r)
				return
			}
		case strings.HasSuffix(r.URL.Path, "/carpeta"):
			if r.Method == http.MethodPost {
				applyMiddleware(app.RequireLogin(app.MessagesSetFolder), core.BlockIPs, core.RateLimit)(w, r)
				return
			}
		default:
			if r.Method == http.MethodGet {
				applyMiddleware(app.RequireLogin(app.MessagesThread), core.BlockIPs, core.RateLimit)(w, r)
				return
			}
		}
		http.NotFound(w, r)
	})

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
		if strings.HasSuffix(r.URL.Path, "/enllacar-dada") && r.Method == http.MethodPost {
			applyMiddleware(app.PersonaLinkField, core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/historial/revert") && r.Method == http.MethodPost {
			applyMiddleware(app.RequireLogin(app.PersonaWikiRevert), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/historial") && r.Method == http.MethodGet {
			applyMiddleware(app.RequireLogin(app.PersonaWikiHistory), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/estadistiques") && r.Method == http.MethodGet {
			applyMiddleware(app.RequireLogin(app.PersonaWikiStats), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/marcar") && r.Method == http.MethodPost {
			applyMiddleware(app.RequireLogin(app.PersonaWikiMark), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/desmarcar") && r.Method == http.MethodPost {
			applyMiddleware(app.RequireLogin(app.PersonaWikiUnmark), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/anecdotes/new") && r.Method == http.MethodGet {
			applyMiddleware(app.RequireLogin(app.PersonaAnecdoteForm), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/anecdotes") && r.Method == http.MethodPost {
			applyMiddleware(app.RequireLogin(app.PersonaAnecdoteCreate), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/registres") && r.Method == http.MethodGet {
			applyMiddleware(app.RequireLogin(app.PersonaRegistres), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/arbre") && r.Method == http.MethodGet {
			applyMiddleware(app.RequireLogin(app.PersonaArbre), core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/edit") && r.Method == http.MethodGet {
			applyMiddleware(app.PersonaForm, core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		if r.Method == http.MethodGet {
			if user, ok := app.VerificarSessio(r); ok && user != nil {
				applyMiddleware(app.RequireLogin(app.PersonaDetall), core.BlockIPs, core.RateLimit)(w, r)
				return
			}
			applyMiddleware(app.PersonaPublic, core.BlockIPs, core.RateLimit)(w, r)
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
	http.HandleFunc("/admin/nivells/rebuild", applyMiddleware(app.AdminNivellsRebuildPage, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/api/admin/nivells/rebuild/", applyMiddleware(app.NivellStatsAdminJobStatusAPI, core.BlockIPs, core.RateLimit))
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
		if r.Method == http.MethodGet {
			parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
			if len(parts) >= 3 {
				if _, err := strconv.Atoi(parts[2]); err == nil {
					applyMiddleware(app.NivellPublic, core.BlockIPs, core.RateLimit)(w, r)
					return
				}
			}
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
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/historial"):
			applyMiddleware(app.RequireLogin(app.MunicipiWikiHistory), core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/estadistiques"):
			applyMiddleware(app.RequireLogin(app.MunicipiWikiStats), core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/historial/revert"):
			applyMiddleware(app.RequireLogin(app.MunicipiWikiRevert), core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/marcar"):
			applyMiddleware(app.RequireLogin(app.MunicipiWikiMark), core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/desmarcar"):
			applyMiddleware(app.RequireLogin(app.MunicipiWikiUnmark), core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/historia"):
			applyMiddleware(app.MunicipiHistoriaPublic, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/events"):
			applyMiddleware(app.MunicipiEventsListPage, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/historia/aportar"):
			applyMiddleware(app.MunicipiHistoriaAportar, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/demografia"):
			applyMiddleware(app.MunicipiDemografiaPage, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/historia/general/save"):
			applyMiddleware(app.MunicipiHistoriaGeneralSave, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/historia/general/submit"):
			applyMiddleware(app.MunicipiHistoriaGeneralSubmit, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/historia/fets/new"):
			applyMiddleware(app.MunicipiHistoriaFetNew, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/historia/fets/") && strings.HasSuffix(r.URL.Path, "/save"):
			applyMiddleware(app.MunicipiHistoriaFetSave, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/historia/fets/") && strings.HasSuffix(r.URL.Path, "/submit"):
			applyMiddleware(app.MunicipiHistoriaFetSubmit, core.BlockIPs, core.RateLimit)(w, r)
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
			parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
			if len(parts) >= 4 && parts[3] == "anecdotes" {
				switch {
				case len(parts) == 4 && r.Method == http.MethodGet:
					applyMiddleware(app.MunicipiAnecdotesListPage, core.BlockIPs, core.RateLimit)(w, r)
					return
				case len(parts) == 5 && parts[4] == "new":
					applyMiddleware(app.RequireLogin(app.MunicipiAnecdoteNewPage), core.BlockIPs, core.RateLimit)(w, r)
					return
				case len(parts) == 5 && r.Method == http.MethodGet:
					applyMiddleware(app.MunicipiAnecdoteDetailPage, core.BlockIPs, core.RateLimit)(w, r)
					return
				case len(parts) == 5 && r.Method == http.MethodPost:
					applyMiddleware(app.RequireLogin(app.MunicipiAnecdoteDetailPage), core.BlockIPs, core.RateLimit)(w, r)
					return
				case len(parts) == 6 && parts[5] == "edit":
					applyMiddleware(app.RequireLogin(app.MunicipiAnecdoteEditPage), core.BlockIPs, core.RateLimit)(w, r)
					return
				}
			}
			if r.Method == http.MethodGet {
				if len(parts) >= 4 && parts[3] == "mapes" {
					switch {
					case len(parts) == 4:
						applyMiddleware(app.MunicipiMapesListPage, core.BlockIPs, core.RateLimit)(w, r)
						return
					case len(parts) == 5:
						applyMiddleware(app.MunicipiMapaViewPage, core.BlockIPs, core.RateLimit)(w, r)
						return
					case len(parts) == 6 && parts[5] == "editor":
						applyMiddleware(app.MunicipiMapaEditorPage, core.BlockIPs, core.RateLimit)(w, r)
						return
					}
				}
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
	http.HandleFunc("/admin/usuaris/revocar-sessions", applyMiddleware(app.AdminRevokeUserSessions, core.BlockIPs, core.RateLimit))

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
	// Achievements
	http.HandleFunc("/admin/achievements", applyMiddleware(app.AdminListAchievements, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/achievements/new", applyMiddleware(app.AdminNewAchievement, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/achievements/icons", applyMiddleware(app.AdminAchievementIcons, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/achievements/save", applyMiddleware(app.AdminSaveAchievement, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/achievements/recompute", applyMiddleware(app.AdminRecomputeAchievements, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/achievements/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/edit") {
			applyMiddleware(app.AdminEditAchievement, core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		http.NotFound(w, r)
	})
	// Control Center + marca pública
	http.HandleFunc("/admin/control", applyMiddleware(app.AdminControlCenter, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/control/widgets", applyMiddleware(app.AdminDashboardWidgetsPage, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/auditoria", applyMiddleware(app.AdminAuditPage, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/jobs", applyMiddleware(app.AdminJobsListPage, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/jobs/", applyMiddleware(app.AdminJobsShowPage, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/plataforma/config", applyMiddleware(app.AdminPlatformConfig, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/kpis", applyMiddleware(app.AdminKPIsPage, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/transparencia", applyMiddleware(app.AdminTransparencyPage, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/transparencia/settings", applyMiddleware(app.AdminTransparencySaveSettings, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/transparencia/contributors/save", applyMiddleware(app.AdminTransparencySaveContributor, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/transparencia/contributors/delete", applyMiddleware(app.AdminTransparencyDeleteContributor, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/manteniments", applyMiddleware(app.AdminListMaintenance, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/manteniments/new", applyMiddleware(app.AdminNewMaintenance, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/manteniments/save", applyMiddleware(app.AdminSaveMaintenance, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/manteniments/delete", applyMiddleware(app.AdminDeleteMaintenance, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/manteniments/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/edit") {
			applyMiddleware(app.AdminEditMaintenance, core.BlockIPs, core.RateLimit)(w, r)
			return
		}
		http.NotFound(w, r)
	})
	// Cognoms import/statistics
	http.HandleFunc("/admin/cognoms/import", applyMiddleware(app.AdminCognomsImport, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/cognoms/import/run", applyMiddleware(app.AdminCognomsImportRun, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/cognoms/stats/run", applyMiddleware(app.AdminCognomsStatsRun, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/cognoms/merge", applyMiddleware(app.AdminCognomsMerge, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/cognoms/merge/delete", applyMiddleware(app.AdminCognomsMergeDelete, core.BlockIPs, core.RateLimit))
	// Import/export centralitzat
	http.HandleFunc("/admin/import-export", applyMiddleware(app.AdminImportExport, core.BlockIPs, core.RateLimit))
	// Territori import/export
	http.HandleFunc("/admin/territori/import/run", applyMiddleware(app.AdminTerritoriImportRun, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/territori/export", applyMiddleware(app.AdminTerritoriExport, core.BlockIPs, core.RateLimit))
	// Entitats eclesiàstiques import/export
	http.HandleFunc("/admin/eclesiastic/import/run", applyMiddleware(app.AdminEclesiasticImportRun, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/eclesiastic/export", applyMiddleware(app.AdminEclesiasticExport, core.BlockIPs, core.RateLimit))
	// Arxius import/export
	http.HandleFunc("/admin/arxius/import/run", applyMiddleware(app.AdminArxiusImportRun, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/arxius/export", applyMiddleware(app.AdminArxiusExport, core.BlockIPs, core.RateLimit))
	// Llibres import/export (JSON)
	http.HandleFunc("/admin/llibres/import/run", applyMiddleware(app.AdminLlibresImportRun, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/llibres/export", applyMiddleware(app.AdminLlibresExport, core.BlockIPs, core.RateLimit))

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
		case strings.Contains(r.URL.Path, "/municipis/historia/general/"):
			applyMiddleware(app.AdminModeracioHistoriaGeneralPreview, core.BlockIPs, core.RateLimit)(w, r)
		case strings.Contains(r.URL.Path, "/municipis/historia/fets/"):
			applyMiddleware(app.AdminModeracioHistoriaFetPreview, core.BlockIPs, core.RateLimit)(w, r)
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
	http.HandleFunc("/admin/moderacio/mapes", applyMiddleware(app.AdminModeracioMapesList, core.BlockIPs, core.RateLimit))
	http.HandleFunc("/admin/moderacio/mapes/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/approve"):
			applyMiddleware(app.AdminModeracioMapesApprove, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(r.URL.Path, "/reject"):
			applyMiddleware(app.AdminModeracioMapesReject, core.BlockIPs, core.RateLimit)(w, r)
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
		case r.Method == http.MethodGet && strings.HasSuffix(path, "/historial"):
			applyMiddleware(app.ArxiuWikiHistory, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodGet && strings.HasSuffix(path, "/estadistiques"):
			applyMiddleware(app.ArxiuWikiStats, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/historial/revert"):
			applyMiddleware(app.ArxiuWikiRevert, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/marcar"):
			applyMiddleware(app.ArxiuWikiMark, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/desmarcar"):
			applyMiddleware(app.ArxiuWikiUnmark, core.BlockIPs, core.RateLimit)(w, r)
		case strings.HasSuffix(path, "/donacions"):
			applyMiddleware(app.ArxiuDonacionsRedirect, core.BlockIPs, core.RateLimit)(w, r)
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
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/historial"):
			applyMiddleware(app.LlibreWikiHistory, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/estadistiques"):
			applyMiddleware(app.LlibreWikiStats, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/historial/revert"):
			applyMiddleware(app.LlibreWikiRevert, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/marcar"):
			applyMiddleware(app.LlibreWikiMark, core.BlockIPs, core.RateLimit)(w, r)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/desmarcar"):
			applyMiddleware(app.LlibreWikiUnmark, core.BlockIPs, core.RateLimit)(w, r)
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
