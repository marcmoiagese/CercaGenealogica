package core

import (
	"database/sql"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

var nivellEstats = map[string]bool{
	"actiu":    true,
	"inactiu":  true,
	"fusionat": true,
	"abolit":   true,
}

func (a *App) AdminListNivells(w http.ResponseWriter, r *http.Request) {
	paisID := extractID(r.URL.Path)
	if pid := strings.TrimSpace(r.URL.Query().Get("pais_id")); pid != "" {
		if v, err := strconv.Atoi(pid); err == nil {
			paisID = v
		}
	}
	user, ok := a.requirePermissionKeyAnyScope(w, r, permKeyTerritoriNivellsView)
	if !ok {
		return
	}
	perms := a.getPermissionsForUser(user.ID)
	scopeFilter := a.buildListScopeFilter(user.ID, permKeyTerritoriNivellsView, ScopePais)
	paisos, _ := a.DB.ListPaisos()
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
	niv, _ := strconv.Atoi(r.URL.Query().Get("nivel"))
	estat := strings.TrimSpace(r.URL.Query().Get("estat"))
	lang := ResolveLang(r)
	perPage := parseListPerPage(r.URL.Query().Get("per_page"))
	page := parseListPage(r.URL.Query().Get("page"))
	statusVals, statusExists := r.URL.Query()["status"]
	status := ""
	if statusExists {
		status = strings.TrimSpace(statusVals[0])
	} else {
		status = "publicat"
	}
	filter := db.NivellAdminFilter{
		PaisID: paisID,
		Nivel:  niv,
		Estat:  estat,
		Status: status,
	}
	filterKeys := []string{"nivel", "pais", "nom", "tipus", "codi", "parent", "anys", "estat", "status"}
	filterValues := map[string]string{}
	filterMatch := map[string]string{}
	for _, key := range filterKeys {
		paramKey := "f_" + key
		if val := strings.TrimSpace(r.URL.Query().Get(paramKey)); val != "" {
			filterValues[key] = val
			filterMatch[key] = strings.ToLower(val)
		}
	}
	filterOrder := []string{}
	if orderParam := strings.TrimSpace(r.URL.Query().Get("order")); orderParam != "" {
		for _, key := range strings.Split(orderParam, ",") {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			if _, ok := filterMatch[key]; ok {
				filterOrder = append(filterOrder, key)
			}
		}
	}
	if len(filterOrder) == 0 {
		for _, key := range filterKeys {
			if _, ok := filterMatch[key]; ok {
				filterOrder = append(filterOrder, key)
			}
		}
	} else {
		seen := map[string]bool{}
		for _, key := range filterOrder {
			seen[key] = true
		}
		for _, key := range filterKeys {
			if _, ok := filterMatch[key]; ok && !seen[key] {
				filterOrder = append(filterOrder, key)
			}
		}
	}
	if !scopeFilter.hasGlobal {
		if scopeFilter.isEmpty() {
			pagination := buildPagination(r, page, perPage, 0, "#nivellsTable")
			RenderPrivateTemplate(w, r, "admin-nivells-list.html", map[string]interface{}{
				"Nivells":           []db.NivellAdministratiu{},
				"Pais":              nil,
				"Paisos":            []db.Pais{},
				"Filter":            filter,
				"FilterValues":      filterValues,
				"FilterOrder":       strings.Join(filterOrder, ","),
				"CanManageArxius":   a.hasPerm(perms, permArxius),
				"CanCreateNivell":   false,
				"CanEditNivell":     map[int]bool{},
				"ShowNivellActions": false,
				"Page":              pagination.Page,
				"PerPage":           pagination.PerPage,
				"Total":             pagination.Total,
				"TotalPages":        pagination.TotalPages,
				"PageLinks":         pagination.Links,
				"PageSelectBase":    pagination.SelectBase,
				"PageAnchor":        pagination.Anchor,
				"User":              user,
			})
			return
		}
		filter.AllowedPaisIDs = scopeFilter.paisIDs
	}
	nivells := []db.NivellAdministratiu{}
	total := 0
	pagination := Pagination{}
	filtered := len(filterMatch) > 0
	if filtered {
		listFilter := filter
		allNivells, _ := a.DB.ListNivells(listFilter)
		for i := range allNivells {
			if allNivells[i].PaisISO2.Valid {
				allNivells[i].PaisLabel = a.countryLabelFromISO(allNivells[i].PaisISO2.String, lang)
			}
		}
		matches := make([]db.NivellAdministratiu, 0, len(allNivells))
		for _, nivell := range allNivells {
			match := true
			for _, key := range filterOrder {
				filterVal := filterMatch[key]
				if filterVal == "" {
					continue
				}
				value := strings.ToLower(nivellFilterValue(nivell, key, lang))
				if !strings.Contains(value, filterVal) {
					match = false
					break
				}
			}
			if match {
				matches = append(matches, nivell)
			}
		}
		total = len(matches)
		pagination = buildPagination(r, page, perPage, total, "#nivellsTable")
		start := pagination.Offset
		end := start + pagination.PerPage
		if start < 0 {
			start = 0
		}
		if start > total {
			start = total
		}
		if end > total {
			end = total
		}
		nivells = matches[start:end]
	} else {
		total, _ = a.DB.CountNivells(filter)
		pagination = buildPagination(r, page, perPage, total, "#nivellsTable")
		listFilter := filter
		listFilter.Limit = pagination.PerPage
		listFilter.Offset = pagination.Offset
		nivells, _ = a.DB.ListNivells(listFilter)
		for i := range nivells {
			if nivells[i].PaisISO2.Valid {
				nivells[i].PaisLabel = a.countryLabelFromISO(nivells[i].PaisISO2.String, lang)
			}
		}
	}
	canCreateNivell := false
	if paisID > 0 {
		canCreateNivell = a.HasPermission(user.ID, permKeyTerritoriNivellsCreate, PermissionTarget{PaisID: intPtr(paisID)})
	}
	canEditNivell := make(map[int]bool, len(nivells))
	showNivellActions := false
	for _, nivell := range nivells {
		target := PermissionTarget{PaisID: intPtr(nivell.PaisID)}
		canEdit := a.HasPermission(user.ID, permKeyTerritoriNivellsEdit, target)
		canEditNivell[nivell.ID] = canEdit
		if canEdit {
			showNivellActions = true
		}
	}
	var pais *db.Pais
	if paisID > 0 {
		pais, _ = a.DB.GetPais(paisID)
		if pais == nil {
			pais = &db.Pais{ID: paisID}
		}
	}
	RenderPrivateTemplate(w, r, "admin-nivells-list.html", map[string]interface{}{
		"Nivells":           nivells,
		"Pais":              pais,
		"Paisos":            paisos,
		"Filter":            filter,
		"FilterValues":      filterValues,
		"FilterOrder":       strings.Join(filterOrder, ","),
		"CanManageArxius":   a.hasPerm(perms, permArxius),
		"CanCreateNivell":   canCreateNivell,
		"CanEditNivell":     canEditNivell,
		"ShowNivellActions": showNivellActions,
		"Page":              pagination.Page,
		"PerPage":           pagination.PerPage,
		"Total":             pagination.Total,
		"TotalPages":        pagination.TotalPages,
		"PageLinks":         pagination.Links,
		"PageSelectBase":    pagination.SelectBase,
		"PageAnchor":        pagination.Anchor,
		"User":              user,
	})
}

func nivellYearsLabel(n db.NivellAdministratiu) string {
	start := ""
	end := ""
	if n.AnyInici.Valid {
		start = strconv.FormatInt(n.AnyInici.Int64, 10)
	}
	if n.AnyFi.Valid {
		end = strconv.FormatInt(n.AnyFi.Int64, 10)
	}
	if start != "" && end != "" {
		return start + " - " + end
	}
	return start + end
}

func nivellFilterValue(n db.NivellAdministratiu, key, lang string) string {
	switch key {
	case "nivel":
		if n.Nivel > 0 {
			return strconv.Itoa(n.Nivel)
		}
	case "pais":
		if n.PaisLabel != "" {
			return n.PaisLabel
		}
		if n.PaisISO2.Valid {
			return n.PaisISO2.String
		}
	case "nom":
		return n.NomNivell
	case "tipus":
		if n.TipusNivell != "" {
			return T(lang, "levels.types."+n.TipusNivell)
		}
	case "codi":
		return n.CodiOficial
	case "parent":
		if n.ParentNom.Valid {
			return n.ParentNom.String
		}
		return "-"
	case "anys":
		return nivellYearsLabel(n)
	case "estat":
		if n.Estat != "" {
			return T(lang, "levels.state."+n.Estat)
		}
	case "status":
		if n.ModeracioEstat != "" {
			return T(lang, "activity.status."+n.ModeracioEstat)
		}
	}
	return ""
}

func (a *App) AdminNewNivell(w http.ResponseWriter, r *http.Request) {
	returnURL := strings.TrimSpace(r.URL.Query().Get("return_to"))
	paisID := extractID(r.URL.Path)
	target := PermissionTarget{PaisID: intPtr(paisID)}
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriNivellsCreate, target)
	if !ok {
		return
	}
	pais, _ := a.DB.GetPais(paisID)
	parents, _ := a.DB.ListNivells(db.NivellAdminFilter{PaisID: paisID})
	paisLabel := ""
	if pais != nil {
		paisLabel = a.countryLabelFromISO(pais.CodiISO2, ResolveLang(r))
	}
	RenderPrivateTemplate(w, r, "admin-nivells-form.html", map[string]interface{}{
		"Nivell":          &db.NivellAdministratiu{PaisID: paisID, Estat: "actiu", ModeracioEstat: "pendent"},
		"Pais":            pais,
		"PaisLabel":       paisLabel,
		"Parents":         parents,
		"LevelTypes":      levelTypes(),
		"IsNew":           true,
		"ReturnURL":       returnURL,
		"CanManageArxius": true,
		"User":            user,
	})
}

func (a *App) AdminEditNivell(w http.ResponseWriter, r *http.Request) {
	returnURL := strings.TrimSpace(r.URL.Query().Get("return_to"))
	id := extractID(r.URL.Path)
	nivell, err := a.DB.GetNivell(id)
	if err != nil || nivell == nil {
		http.NotFound(w, r)
		return
	}
	target := PermissionTarget{PaisID: intPtr(nivell.PaisID)}
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriNivellsEdit, target)
	if !ok {
		return
	}
	pais, _ := a.DB.GetPais(nivell.PaisID)
	parents, _ := a.DB.ListNivells(db.NivellAdminFilter{PaisID: nivell.PaisID})
	nomsH, _ := a.DB.ListNomsHistorics("nivell_admin", nivell.ID)
	paisLabel := ""
	if pais != nil {
		paisLabel = a.countryLabelFromISO(pais.CodiISO2, ResolveLang(r))
	}
	RenderPrivateTemplate(w, r, "admin-nivells-form.html", map[string]interface{}{
		"Nivell":          nivell,
		"Pais":            pais,
		"PaisLabel":       paisLabel,
		"Parents":         parents,
		"NomsHistorics":   nomsH,
		"LevelTypes":      levelTypes(),
		"IsNew":           false,
		"ReturnURL":       returnURL,
		"CanManageArxius": true,
		"User":            user,
	})
}

func levelTypes() []string {
	return []string{
		"provincia",
		"districte",
		"subdistricte",
		"comtat",
		"municipi",
		"govern_local",
		"poble",
		"parroquia",
		"dependencia",
		"comunitat",
		"comunitat_autonoma",
		"vegueria",
		"localitat",
		"comuna",
		"barri",
		"regio",
		"estat",
		"ciutat_estatutaria",
		"ciutat",
		"ciutat_mercat",
		"raion",
		"republica_autonoma",
		"assentament",
		"governacio",
		"divisio",
		"subregio",
		"consell_unitari",
		"corporacio_municipal",
		"regio_autonoma",
		"districte_rural",
		"districte_urba",
		"subprefectura",
		"departament",
		"canto",
		"unitat_veinal",
		"prefectura",
		"prefectura_autonoma",
		"area_no_incorporada",
		"districte_electoral",
		"area_urbana",
		"area_rural",
		"corregiment",
		"vereda",
		"territori",
		"jefatura",
		"sector",
		"grupacio",
		"colina",
		"cantons",
		"comunitat_local",
		"districte_especial",
		"territori_no_organitzat",
		"regio_administrativa",
		"circumscripcio",
		"entitat_federal",
		"condomi",
		"ciutat_independent",
		"mancomunitat_serveis",
		"territori_equivalent",
		"concell",
		"post_administratiu",
		"area_censal",
		"reserva_indigena",
		"territori_organitzat_no_incorporat",
		"comarca",
		"ciutat_autonoma",
		"illa_autonoma",
		"aglomeracio_urbana",
		"area_especial",
		"collectivitat_ultramar",
		"regio_administrativa_especial",
		"mancomunitat",
		"poble_etnic_reserva",
		"districte_forestal",
		"burg",
		"vila_australia",
		"metropoli",
		"area_metropolitana",
		"area_govern_local",
		"comissio_serveis",
		"districte_millora",
	}
}

func parseNullInt(val string) sql.NullInt64 {
	var n sql.NullInt64
	if strings.TrimSpace(val) == "" {
		return n
	}
	if i, err := strconv.Atoi(val); err == nil {
		n.Int64 = int64(i)
		n.Valid = true
	}
	return n
}

func (a *App) AdminSaveNivell(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin/paisos", http.StatusSeeOther)
		return
	}
	id, _ := strconv.Atoi(r.FormValue("id"))
	permKey := permKeyTerritoriNivellsCreate
	if id != 0 {
		permKey = permKeyTerritoriNivellsEdit
	}
	paisID, _ := strconv.Atoi(r.FormValue("pais_id"))
	target := PermissionTarget{PaisID: intPtr(paisID)}
	user, ok := a.requirePermissionKey(w, r, permKey, target)
	if !ok {
		return
	}
	returnURL := strings.TrimSpace(r.FormValue("return_to"))
	nivel, _ := strconv.Atoi(r.FormValue("nivel"))
	parentID := parseNullInt(r.FormValue("parent_id"))
	anyInici := parseNullInt(r.FormValue("any_inici"))
	anyFi := parseNullInt(r.FormValue("any_fi"))
	estat := strings.TrimSpace(r.FormValue("estat"))
	nivell := &db.NivellAdministratiu{
		ID:             id,
		PaisID:         paisID,
		Nivel:          nivel,
		NomNivell:      strings.TrimSpace(r.FormValue("nom_nivell")),
		TipusNivell:    strings.TrimSpace(r.FormValue("tipus_nivell")),
		CodiOficial:    strings.TrimSpace(r.FormValue("codi_oficial")),
		Altres:         strings.TrimSpace(r.FormValue("altres")),
		ParentID:       parentID,
		AnyInici:       anyInici,
		AnyFi:          anyFi,
		Estat:          estat,
		CreatedBy:      sqlNullIntFromInt(user.ID),
		ModeracioEstat: "pendent",
		ModeratedBy:    sql.NullInt64{},
		ModeratedAt:    sql.NullTime{},
	}
	if errMsg := a.validateNivell(nivell); errMsg != "" {
		a.renderNivellFormError(w, r, nivell, errMsg, id == 0)
		return
	}
	if err := a.ensureNivellUnique(nivell); err != "" {
		a.renderNivellFormError(w, r, nivell, err, id == 0)
		return
	}
	var saveErr error
	if nivell.ID == 0 {
		var createdID int
		createdID, saveErr = a.DB.CreateNivell(nivell)
		if saveErr == nil {
			nivell.ID = createdID
			_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleNivellCreate, "crear", "nivell", &createdID, "pendent", nil, "")
		}
	} else {
		saveErr = a.DB.UpdateNivell(nivell)
		_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleNivellUpdate, "editar", "nivell", &id, "pendent", nil, "")
	}
	if saveErr != nil {
		a.renderNivellFormError(w, r, nivell, "No s'ha pogut desar el nivell administratiu.", id == 0)
		return
	}
	if returnURL != "" {
		http.Redirect(w, r, returnURL, http.StatusSeeOther)
	} else {
		http.Redirect(w, r, "/territori/paisos/"+strconv.Itoa(nivell.PaisID)+"/nivells", http.StatusSeeOther)
	}
}

func (a *App) validateNivell(n *db.NivellAdministratiu) string {
	if n.PaisID == 0 {
		return "Cal indicar el país."
	}
	if n.Nivel < 1 || n.Nivel > 7 {
		return "El nivell ha d'estar entre 1 i 7."
	}
	if strings.TrimSpace(n.NomNivell) == "" {
		return "El nom del nivell és obligatori."
	}
	if n.ParentID.Valid {
		parent, err := a.DB.GetNivell(int(n.ParentID.Int64))
		if err != nil || parent == nil || parent.PaisID != n.PaisID {
			return "El nivell pare ha de pertànyer al mateix país."
		}
		if parent.ID == n.ID {
			return "Un nivell no pot ser el seu propi pare."
		}
	}
	if n.Estat == "" {
		n.Estat = "actiu"
	}
	if !nivellEstats[n.Estat] {
		return "Estat no vàlid."
	}
	return ""
}

func (a *App) ensureNivellUnique(n *db.NivellAdministratiu) string {
	existents, err := a.DB.ListNivells(db.NivellAdminFilter{PaisID: n.PaisID, Nivel: n.Nivel})
	if err != nil {
		return ""
	}
	name := strings.TrimSpace(n.NomNivell)
	if name == "" {
		return ""
	}
	for _, e := range existents {
		if n.ID != 0 && e.ID == n.ID {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(e.NomNivell), name) {
			continue
		}
		if n.ParentID.Valid {
			if e.ParentID.Valid && e.ParentID.Int64 == n.ParentID.Int64 {
				return "Ja existeix un nivell amb aquest nom dins aquest pare."
			}
			continue
		}
		if !e.ParentID.Valid {
			return "Ja existeix un nivell amb aquest nom per al país."
		}
	}
	return ""
}

func (a *App) renderNivellFormError(w http.ResponseWriter, r *http.Request, n *db.NivellAdministratiu, msg string, isNew bool) {
	parents, _ := a.DB.ListNivells(db.NivellAdminFilter{PaisID: n.PaisID})
	nomsH, _ := a.DB.ListNomsHistorics("nivell_admin", n.ID)
	RenderPrivateTemplate(w, r, "admin-nivells-form.html", map[string]interface{}{
		"Nivell":          n,
		"Parents":         parents,
		"IsNew":           isNew,
		"Error":           msg,
		"NomsHistorics":   nomsH,
		"ReturnURL":       strings.TrimSpace(r.FormValue("return_to")),
		"CanManageArxius": true,
	})
}

func (a *App) AdminSaveNivellNomHistoric(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/territori/nivells", http.StatusSeeOther)
		return
	}
	nivID := extractID(r.URL.Path)
	if nivID == 0 {
		http.NotFound(w, r)
		return
	}
	nivell, err := a.DB.GetNivell(nivID)
	if err != nil || nivell == nil {
		http.NotFound(w, r)
		return
	}
	target := PermissionTarget{PaisID: intPtr(nivell.PaisID)}
	if _, ok := a.requirePermissionKey(w, r, permKeyTerritoriNivellsEdit, target); !ok {
		return
	}
	nhID, _ := strconv.Atoi(r.FormValue("nh_id"))
	nh := &db.NomHistoric{
		ID:                    nhID,
		EntitatTipus:          "nivell_admin",
		EntitatID:             nivID,
		Nom:                   strings.TrimSpace(r.FormValue("nom")),
		AnyInici:              parseNullInt(r.FormValue("any_inici")),
		AnyFi:                 parseNullInt(r.FormValue("any_fi")),
		PaisRegne:             strings.TrimSpace(r.FormValue("pais_regne")),
		DistribucioGeografica: strings.TrimSpace(r.FormValue("distribucio_geografica")),
		Font:                  strings.TrimSpace(r.FormValue("font")),
	}
	if nh.Nom == "" {
		http.Redirect(w, r, "/territori/nivells/"+strconv.Itoa(nivID)+"/edit?error=nomh", http.StatusSeeOther)
		return
	}
	_, _ = a.DB.SaveNomHistoric(nh)
	http.Redirect(w, r, "/territori/nivells/"+strconv.Itoa(nivID)+"/edit", http.StatusSeeOther)
}
