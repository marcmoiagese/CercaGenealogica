package core

import "net/http"

func (a *App) EspaiPersonalOverview(w http.ResponseWriter, r *http.Request) {
	a.EspaiPersonalOverviewPage(w, r)
}

func (a *App) EspaiPersonalGedcom(w http.ResponseWriter, r *http.Request) {
	a.EspaiPersonalGedcomPage(w, r)
}

func (a *App) EspaiPersonalIntegracions(w http.ResponseWriter, r *http.Request) {
	a.EspaiPersonalIntegracionsPage(w, r)
}

func (a *App) EspaiPersonalCoincidencies(w http.ResponseWriter, r *http.Request) {
	a.EspaiPersonalCoincidenciesPage(w, r)
}

func (a *App) EspaiPersonalGrups(w http.ResponseWriter, r *http.Request) {
	a.EspaiPersonalGrupsPage(w, r)
}

func (a *App) renderEspaiPersonal(w http.ResponseWriter, r *http.Request, section string) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	RenderPrivateTemplate(w, r, "espai.html", map[string]interface{}{
		"SpaceSection": section,
		"SpaceState":   "empty",
	})
}
