package core

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func validatePaisCodes(p *db.Pais) string {
	if len(p.CodiISO2) != 2 || !isUpperAlpha(p.CodiISO2) {
		return "El codi ISO2 ha de tenir 2 lletres majúscules."
	}
	if len(p.CodiISO3) != 3 || !isUpperAlpha(p.CodiISO3) {
		return "El codi ISO3 ha de tenir 3 lletres majúscules."
	}
	return ""
}

func isUpperAlpha(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < 'A' || r > 'Z' {
			return false
		}
	}
	return true
}

func (a *App) AdminListPaisos(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permTerritory); !ok {
		return
	}
	paisos, err := a.DB.ListPaisos()
	if err != nil {
		http.Error(w, "Error obtenint països", http.StatusInternalServerError)
		return
	}
	RenderPrivateTemplate(w, r, "admin-paisos-list.html", map[string]interface{}{
		"Paisos":          paisos,
		"CanManageArxius": true,
	})
}

func (a *App) AdminNewPais(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permTerritory); !ok {
		return
	}
	RenderPrivateTemplate(w, r, "admin-paisos-form.html", map[string]interface{}{
		"Pais":            &db.Pais{},
		"IsNew":           true,
		"CanManageArxius": true,
	})
}

func (a *App) AdminEditPais(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permTerritory); !ok {
		return
	}
	id := extractID(r.URL.Path)
	pais, err := a.DB.GetPais(id)
	if err != nil || pais == nil {
		http.NotFound(w, r)
		return
	}
	RenderPrivateTemplate(w, r, "admin-paisos-form.html", map[string]interface{}{
		"Pais":            pais,
		"IsNew":           false,
		"CanManageArxius": true,
	})
}

func (a *App) AdminSavePais(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permTerritory); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin/paisos", http.StatusSeeOther)
		return
	}
	id, _ := strconv.Atoi(r.FormValue("id"))
	pais := &db.Pais{
		ID:          id,
		CodiISO2:    strings.ToUpper(strings.TrimSpace(r.FormValue("codi_iso2"))),
		CodiISO3:    strings.ToUpper(strings.TrimSpace(r.FormValue("codi_iso3"))),
		CodiPaisNum: strings.TrimSpace(r.FormValue("codi_pais_num")),
	}
	if errMsg := validatePaisCodes(pais); errMsg != "" {
		RenderPrivateTemplate(w, r, "admin-paisos-form.html", map[string]interface{}{
			"Pais":            pais,
			"IsNew":           id == 0,
			"Error":           errMsg,
			"CanManageArxius": true,
		})
		return
	}
	if errMsg := a.ensurePaisUnique(pais); errMsg != "" {
		RenderPrivateTemplate(w, r, "admin-paisos-form.html", map[string]interface{}{
			"Pais":            pais,
			"IsNew":           id == 0,
			"Error":           errMsg,
			"CanManageArxius": true,
		})
		return
	}
	var err error
	if pais.ID == 0 {
		_, err = a.DB.CreatePais(pais)
	} else {
		err = a.DB.UpdatePais(pais)
	}
	if err != nil {
		RenderPrivateTemplate(w, r, "admin-paisos-form.html", map[string]interface{}{
			"Pais":            pais,
			"IsNew":           id == 0,
			"Error":           "No s'ha pogut desar el país.",
			"CanManageArxius": true,
		})
		return
	}
	http.Redirect(w, r, "/admin/paisos", http.StatusSeeOther)
}

func (a *App) ensurePaisUnique(p *db.Pais) string {
	existents, err := a.DB.ListPaisos()
	if err != nil {
		return ""
	}
	for _, e := range existents {
		if p.ID != 0 && e.ID == p.ID {
			continue
		}
		if e.CodiISO2 == p.CodiISO2 || e.CodiISO3 == p.CodiISO3 || (p.CodiPaisNum != "" && e.CodiPaisNum == p.CodiPaisNum) {
			return "Ja existeix un país amb aquest codi."
		}
	}
	return ""
}
