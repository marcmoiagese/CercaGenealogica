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
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	paisID := extractID(r.URL.Path)
	if pid := strings.TrimSpace(r.URL.Query().Get("pais_id")); pid != "" {
		if v, err := strconv.Atoi(pid); err == nil {
			paisID = v
		}
	}
	paisos, _ := a.DB.ListPaisos()
	niv, _ := strconv.Atoi(r.URL.Query().Get("nivel"))
	estat := strings.TrimSpace(r.URL.Query().Get("estat"))
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
	nivells, _ := a.DB.ListNivells(filter)
	for i := range nivells {
		if nivells[i].PaisISO2.Valid {
			nivells[i].PaisLabel = a.countryLabelFromISO(nivells[i].PaisISO2.String, ResolveLang(r))
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
		"Nivells":         nivells,
		"Pais":            pais,
		"Paisos":          paisos,
		"Filter":          filter,
		"CanManageArxius": a.hasPerm(a.getPermissionsForUser(user.ID), permArxius),
		"User":            user,
	})
}

func (a *App) AdminNewNivell(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permTerritory); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	returnURL := strings.TrimSpace(r.URL.Query().Get("return_to"))
	paisID := extractID(r.URL.Path)
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
	if _, _, ok := a.requirePermission(w, r, permTerritory); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	returnURL := strings.TrimSpace(r.URL.Query().Get("return_to"))
	id := extractID(r.URL.Path)
	nivell, err := a.DB.GetNivell(id)
	if err != nil || nivell == nil {
		http.NotFound(w, r)
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
	if _, _, ok := a.requirePermission(w, r, permTerritory); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/admin/paisos", http.StatusSeeOther)
		return
	}
	user, _ := a.VerificarSessio(r)
	id, _ := strconv.Atoi(r.FormValue("id"))
	paisID, _ := strconv.Atoi(r.FormValue("pais_id"))
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
	for _, e := range existents {
		if n.ID != 0 && e.ID == n.ID {
			continue
		}
		if strings.EqualFold(e.NomNivell, n.NomNivell) {
			return "Ja existeix un nivell amb aquest nom i nivell per al país."
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
	if _, _, ok := a.requirePermission(w, r, permTerritory); !ok {
		return
	}
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
