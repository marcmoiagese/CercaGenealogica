package core

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
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
	user, ok := a.requirePermissionKeyAnyScope(w, r, permKeyTerritoriPaisosView)
	if !ok {
		return
	}
	scopeFilter := a.buildListScopeFilter(user.ID, permKeyTerritoriPaisosView, ScopePais)
	if !scopeFilter.hasGlobal && scopeFilter.isEmpty() {
		RenderPrivateTemplate(w, r, "admin-paisos-list.html", map[string]interface{}{
			"Paisos":          []db.Pais{},
			"CanCreatePais":   a.HasPermission(user.ID, permKeyTerritoriPaisosCreate, PermissionTarget{}),
			"CanEditPais":     map[int]bool{},
			"ShowPaisActions": false,
			"CanManageArxius": true,
			"User":            user,
		})
		return
	}
	paisos, err := a.DB.ListPaisos()
	if err != nil {
		http.Error(w, "Error obtenint països", http.StatusInternalServerError)
		return
	}
	if !scopeFilter.hasGlobal && len(scopeFilter.paisIDs) > 0 {
		allowed := map[int]struct{}{}
		for _, id := range scopeFilter.paisIDs {
			allowed[id] = struct{}{}
		}
		filtered := make([]db.Pais, 0, len(paisos))
		for _, pais := range paisos {
			if _, ok := allowed[pais.ID]; ok {
				filtered = append(filtered, pais)
			}
		}
		paisos = filtered
	}
	canCreatePais := a.HasPermission(user.ID, permKeyTerritoriPaisosCreate, PermissionTarget{})
	canEditPais := make(map[int]bool, len(paisos))
	showPaisActions := false
	for _, pais := range paisos {
		target := PermissionTarget{PaisID: intPtr(pais.ID)}
		canEdit := a.HasPermission(user.ID, permKeyTerritoriPaisosEdit, target)
		canEditPais[pais.ID] = canEdit
		if canEdit {
			showPaisActions = true
		}
	}
	RenderPrivateTemplate(w, r, "admin-paisos-list.html", map[string]interface{}{
		"Paisos":          paisos,
		"CanCreatePais":   canCreatePais,
		"CanEditPais":     canEditPais,
		"ShowPaisActions": showPaisActions,
		"CanManageArxius": true,
		"User":            user,
	})
}

func (a *App) AdminPaisosSuggest(w http.ResponseWriter, r *http.Request) {
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	perms := a.getPermissionsForUser(user.ID)
	allowAll := false
	if !a.hasAnyPermissionKey(user.ID, permKeyTerritoriPaisosView) {
		if !permPolicies(perms) {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		allowAll = true
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if len(query) < 1 {
		writeJSON(w, map[string]interface{}{"items": []interface{}{}})
		return
	}
	limit := 10
	if val := strings.TrimSpace(r.URL.Query().Get("limit")); val != "" {
		if v, err := strconv.Atoi(val); err == nil && v > 0 && v <= 25 {
			limit = v
		}
	}
	scopeFilter := listScopeFilter{}
	if !allowAll {
		scopeFilter = a.buildListScopeFilter(user.ID, permKeyTerritoriPaisosView, ScopePais)
		if !scopeFilter.hasGlobal && scopeFilter.isEmpty() {
			writeJSON(w, map[string]interface{}{"items": []interface{}{}})
			return
		}
	}
	paisos, err := a.DB.ListPaisos()
	if err != nil {
		writeJSON(w, map[string]interface{}{"items": []interface{}{}})
		return
	}
	if !allowAll && !scopeFilter.hasGlobal && len(scopeFilter.paisIDs) > 0 {
		allowed := map[int]struct{}{}
		for _, id := range scopeFilter.paisIDs {
			allowed[id] = struct{}{}
		}
		filtered := make([]db.Pais, 0, len(paisos))
		for _, pais := range paisos {
			if _, ok := allowed[pais.ID]; ok {
				filtered = append(filtered, pais)
			}
		}
		paisos = filtered
	}
	lang := ResolveLang(r)
	queryLower := strings.ToLower(query)
	items := make([]map[string]interface{}, 0, limit)
	for _, pais := range paisos {
		label := strings.TrimSpace(a.countryLabelFromISO(pais.CodiISO2, lang))
		iso2 := strings.TrimSpace(pais.CodiISO2)
		iso3 := strings.TrimSpace(pais.CodiISO3)
		match := strings.Contains(strings.ToLower(label), queryLower) ||
			strings.Contains(strings.ToLower(iso2), queryLower) ||
			strings.Contains(strings.ToLower(iso3), queryLower)
		if !match {
			continue
		}
		contextParts := []string{}
		if iso2 != "" {
			contextParts = append(contextParts, iso2)
		}
		if iso3 != "" {
			contextParts = append(contextParts, iso3)
		}
		items = append(items, map[string]interface{}{
			"id":      pais.ID,
			"nom":     label,
			"context": strings.Join(contextParts, " · "),
		})
		if len(items) >= limit {
			break
		}
	}
	writeJSON(w, map[string]interface{}{"items": items})
}

func (a *App) AdminNewPais(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyTerritoriPaisosCreate, PermissionTarget{}); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	countries := loadCountriesOptions(r)
	RenderPrivateTemplate(w, r, "admin-paisos-form.html", map[string]interface{}{
		"Pais":            &db.Pais{},
		"IsNew":           true,
		"Countries":       countries,
		"CanManageArxius": true,
		"User":            user,
	})
}

func (a *App) AdminEditPais(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	target := PermissionTarget{PaisID: intPtr(id)}
	if _, ok := a.requirePermissionKey(w, r, permKeyTerritoriPaisosEdit, target); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	pais, err := a.DB.GetPais(id)
	if err != nil || pais == nil {
		http.NotFound(w, r)
		return
	}
	countries := loadCountriesOptions(r)
	RenderPrivateTemplate(w, r, "admin-paisos-form.html", map[string]interface{}{
		"Pais":            pais,
		"IsNew":           false,
		"Countries":       countries,
		"CanManageArxius": true,
		"User":            user,
	})
}

func (a *App) AdminSavePais(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin/paisos", http.StatusSeeOther)
		return
	}
	id, _ := strconv.Atoi(r.FormValue("id"))
	permKey := permKeyTerritoriPaisosCreate
	if id != 0 {
		permKey = permKeyTerritoriPaisosEdit
	}
	target := PermissionTarget{}
	if id != 0 {
		target.PaisID = intPtr(id)
	}
	if _, ok := a.requirePermissionKey(w, r, permKey, target); !ok {
		return
	}
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
			"Countries":       loadCountriesOptions(r),
			"CanManageArxius": true,
		})
		return
	}
	if errMsg := a.ensurePaisUnique(pais); errMsg != "" {
		RenderPrivateTemplate(w, r, "admin-paisos-form.html", map[string]interface{}{
			"Pais":            pais,
			"IsNew":           id == 0,
			"Error":           errMsg,
			"Countries":       loadCountriesOptions(r),
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
			"Countries":       loadCountriesOptions(r),
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

type countryOption struct {
	ISO2  string
	ISO3  string
	Num   int
	Label string
}

func loadCountriesOptions(r *http.Request) []countryOption {
	lang := ResolveLang(r)
	content, err := os.ReadFile("static/json/countries.json")
	if err != nil {
		return nil
	}
	var raw []map[string]interface{}
	if err := json.Unmarshal(content, &raw); err != nil {
		return nil
	}
	opts := make([]countryOption, 0, len(raw))
	for _, c := range raw {
		iso2, _ := c["alpha2"].(string)
		iso3, _ := c["alpha3"].(string)
		var num int
		switch v := c["id"].(type) {
		case float64:
			num = int(v)
		case int:
			num = v
		}
		label := pickCountryLabel(c, lang)
		opts = append(opts, countryOption{ISO2: strings.ToUpper(iso2), ISO3: strings.ToUpper(iso3), Num: num, Label: label})
	}
	sort.Slice(opts, func(i, j int) bool { return opts[i].Label < opts[j].Label })
	return opts
}

func pickCountryLabel(m map[string]interface{}, lang string) string {
	l := strings.ToLower(strings.TrimSpace(lang))
	preferred := []string{l, "cat", "ca", "oc", "eu", "en", "es", "fr"}
	for _, k := range preferred {
		if v, ok := m[k]; ok {
			return fmt.Sprint(v)
		}
	}
	if v, ok := m["alpha3"]; ok {
		return fmt.Sprint(v)
	}
	return ""
}
