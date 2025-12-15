package core

import (
	"database/sql"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func (a *App) AdminListMunicipis(w http.ResponseWriter, r *http.Request) {
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	perms := a.getPermissionsForUser(user.ID)
	filter := db.MunicipiFilter{
		Text:  strings.TrimSpace(r.URL.Query().Get("q")),
		Estat: strings.TrimSpace(r.URL.Query().Get("estat")),
		Status: strings.TrimSpace(r.URL.Query().Get("status")),
	}
	if filter.Status == "" {
		filter.Status = "publicat"
	}
	if pid := strings.TrimSpace(r.URL.Query().Get("pais_id")); pid != "" {
		if v, err := strconv.Atoi(pid); err == nil {
			filter.PaisID = v
		}
	}
	if lid := strings.TrimSpace(r.URL.Query().Get("nivell_id")); lid != "" {
		if v, err := strconv.Atoi(lid); err == nil {
			filter.NivellID = v
		}
	}
	muns, _ := a.DB.ListMunicipis(filter)
	paisos, _ := a.DB.ListPaisos()
	var nivells []db.NivellAdministratiu
	if filter.PaisID > 0 {
		nivells, _ = a.DB.ListNivells(db.NivellAdminFilter{PaisID: filter.PaisID})
	} else {
		nivells, _ = a.DB.ListNivells(db.NivellAdminFilter{})
	}
	RenderPrivateTemplate(w, r, "admin-municipis-list.html", map[string]interface{}{
		"Municipis":       muns,
		"Filter":          filter,
		"Paisos":          paisos,
		"Nivells":         nivells,
		"CanManageArxius": a.hasPerm(perms, permArxius),
		"User":            user,
	})
}

func (a *App) AdminNewMunicipi(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permTerritory); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	paisos, _ := a.DB.ListPaisos()
	var (
		levels []db.NivellAdministratiu
		mun    = &db.Municipi{Estat: "actiu"}
	)
	if pid := strings.TrimSpace(r.URL.Query().Get("pais_id")); pid != "" {
		if v, err := strconv.Atoi(pid); err == nil {
			levels, _ = a.DB.ListNivells(db.NivellAdminFilter{PaisID: v})
			mun.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(v), Valid: true}
		}
	}
	// Si no hi ha filtre, carrega nivells del primer país per donar referència visual
	if len(levels) == 0 && len(paisos) > 0 {
		levels, _ = a.DB.ListNivells(db.NivellAdminFilter{PaisID: paisos[0].ID})
	}
	arquebisbats, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	RenderPrivateTemplate(w, r, "admin-municipis-form.html", map[string]interface{}{
		"Municipi":        mun,
		"Paisos":          paisos,
		"Levels":          levels,
		"Arquebisbats":    arquebisbats,
		"CodisPostals":    nil,
		"IsNew":           true,
		"CanManageArxius": true,
		"User":            user,
	})
}

func (a *App) AdminEditMunicipi(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permTerritory); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	id := extractID(r.URL.Path)
	mun, err := a.DB.GetMunicipi(id)
	if err != nil || mun == nil {
		http.NotFound(w, r)
		return
	}
	paisos, _ := a.DB.ListPaisos()
	var levels []db.NivellAdministratiu
	if pid := strings.TrimSpace(r.URL.Query().Get("pais_id")); pid != "" {
		if v, err := strconv.Atoi(pid); err == nil {
			mun.NivellAdministratiuID[0] = sql.NullInt64{Int64: int64(v), Valid: true}
		}
	}
	if mun.NivellAdministratiuID[0].Valid {
		levels, _ = a.DB.ListNivells(db.NivellAdminFilter{PaisID: int(mun.NivellAdministratiuID[0].Int64)})
	}
	codis, _ := a.DB.ListCodisPostals(mun.ID)
	ecles, _ := a.DB.ListArquebisbatMunicipis(mun.ID)
	arquebisbats, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	nomsH, _ := a.DB.ListNomsHistorics("municipi", mun.ID)
	var editEcles *db.ArquebisbatMunicipi
	if editParam := strings.TrimSpace(r.URL.Query().Get("edit_am")); editParam != "" {
		if editID, err := strconv.Atoi(editParam); err == nil {
			for _, el := range ecles {
				if el.ID == editID {
					editEcles = &el
					break
				}
			}
		}
	}
	RenderPrivateTemplate(w, r, "admin-municipis-form.html", map[string]interface{}{
		"Municipi":        mun,
		"Paisos":          paisos,
		"Levels":          levels,
		"CodisPostals":    codis,
		"Ecles":           ecles,
		"Arquebisbats":    arquebisbats,
		"NomsHistorics":   nomsH,
		"EditEcles":       editEcles,
		"IsNew":           false,
		"CanManageArxius": true,
		"User":            user,
	})
}

func parseNullFloat(val string) sql.NullFloat64 {
	var n sql.NullFloat64
	if strings.TrimSpace(val) == "" {
		return n
	}
	if f, err := strconv.ParseFloat(val, 64); err == nil {
		n.Valid = true
		n.Float64 = f
	}
	return n
}

func (a *App) AdminSaveMunicipi(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permTerritory); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/territori/municipis", http.StatusSeeOther)
		return
	}
	user, _ := a.VerificarSessio(r)
	id, _ := strconv.Atoi(r.FormValue("id"))
	parent := parseNullInt(r.FormValue("municipi_id"))
	m := &db.Municipi{
		ID:         id,
		Nom:        strings.TrimSpace(r.FormValue("nom")),
		MunicipiID: parent,
		Tipus:      strings.TrimSpace(r.FormValue("tipus")),
		CodiPostal: strings.TrimSpace(r.FormValue("codi_postal")),
		Latitud:    parseNullFloat(r.FormValue("latitud")),
		Longitud:   parseNullFloat(r.FormValue("longitud")),
		What3Words: strings.TrimSpace(r.FormValue("what3words")),
		Web:        strings.TrimSpace(r.FormValue("web")),
		Wikipedia:  strings.TrimSpace(r.FormValue("wikipedia")),
		Altres:     strings.TrimSpace(r.FormValue("altres")),
		Estat:      strings.TrimSpace(r.FormValue("estat")),
		CreatedBy:  sqlNullIntFromInt(user.ID),
		ModeracioEstat: "pendent",
		ModeratedBy: sql.NullInt64{},
		ModeratedAt: sql.NullTime{},
	}
	for i := 0; i < 7; i++ {
		field := strings.TrimSpace(r.FormValue("nivell_administratiu_id_" + strconv.Itoa(i+1)))
		if field != "" {
			m.NivellAdministratiuID[i] = parseNullInt(field)
		}
	}
	if m.Estat == "" {
		m.Estat = "actiu"
	}
	if errMsg := a.validateMunicipi(m); errMsg != "" {
		a.renderMunicipiFormError(w, r, m, errMsg, id == 0)
		return
	}
	if m.ID == 0 {
		createdID, err := a.DB.CreateMunicipi(m)
		if err != nil {
			a.renderMunicipiFormError(w, r, m, "No s'ha pogut crear el municipi.", true)
			return
		}
		m.ID = createdID
		_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleMunicipiCreate, "crear", "municipi", &createdID, "pendent", nil, "")
	} else {
		if err := a.DB.UpdateMunicipi(m); err != nil {
			a.renderMunicipiFormError(w, r, m, "No s'ha pogut actualitzar el municipi.", false)
			return
		}
		_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleMunicipiUpdate, "editar", "municipi", &id, "pendent", nil, "")
	}
	http.Redirect(w, r, "/territori/municipis", http.StatusSeeOther)
}

func (a *App) validateMunicipi(m *db.Municipi) string {
	if strings.TrimSpace(m.Nom) == "" {
		return "El nom és obligatori."
	}
	if m.Tipus == "" {
		return "El tipus és obligatori."
	}
	if m.MunicipiID.Valid && m.ID != 0 && m.MunicipiID.Int64 == int64(m.ID) {
		return "Un municipi no pot ser pare de si mateix."
	}
	return ""
}

func (a *App) renderMunicipiFormError(w http.ResponseWriter, r *http.Request, m *db.Municipi, msg string, isNew bool) {
	paisos, _ := a.DB.ListPaisos()
	var levels []db.NivellAdministratiu
	if m.NivellAdministratiuID[0].Valid {
		levels, _ = a.DB.ListNivells(db.NivellAdminFilter{PaisID: int(m.NivellAdministratiuID[0].Int64)})
	}
	var ecles []db.ArquebisbatMunicipi
	if !isNew && m.ID != 0 {
		ecles, _ = a.DB.ListArquebisbatMunicipis(m.ID)
	}
	arquebisbats, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	nomsH, _ := a.DB.ListNomsHistorics("municipi", m.ID)
	RenderPrivateTemplate(w, r, "admin-municipis-form.html", map[string]interface{}{
		"Municipi":        m,
		"Paisos":          paisos,
		"Levels":          levels,
		"CodisPostals":    nil,
		"Ecles":           ecles,
		"Arquebisbats":    arquebisbats,
		"NomsHistorics":   nomsH,
		"Error":           msg,
		"IsNew":           isNew,
		"CanManageArxius": true,
	})
}

func (a *App) AdminSaveCodiPostal(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permTerritory); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/territori/municipis", http.StatusSeeOther)
		return
	}
	munID := extractID(r.URL.Path)
	if munID == 0 {
		http.NotFound(w, r)
		return
	}
	_, err := a.DB.GetMunicipi(munID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	cpID, _ := strconv.Atoi(r.FormValue("cp_id"))
	cp := &db.CodiPostal{
		ID:         cpID,
		MunicipiID: munID,
		CodiPostal: strings.TrimSpace(r.FormValue("codi_postal")),
		Zona:       strings.TrimSpace(r.FormValue("zona")),
		Desde:      sql.NullString{String: strings.TrimSpace(r.FormValue("desde")), Valid: strings.TrimSpace(r.FormValue("desde")) != ""},
		Fins:       sql.NullString{String: strings.TrimSpace(r.FormValue("fins")), Valid: strings.TrimSpace(r.FormValue("fins")) != ""},
	}
	if cp.CodiPostal == "" {
		http.Redirect(w, r, "/territori/municipis/"+strconv.Itoa(munID)+"/edit?error=cp", http.StatusSeeOther)
		return
	}
	_, _ = a.DB.SaveCodiPostal(cp)
	http.Redirect(w, r, "/territori/municipis/"+strconv.Itoa(munID)+"/edit", http.StatusSeeOther)
}

func (a *App) AdminSaveMunicipiEcles(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permTerritory); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/territori/municipis", http.StatusSeeOther)
		return
	}
	munID := extractID(r.URL.Path)
	if munID == 0 {
		http.NotFound(w, r)
		return
	}
	_, err := a.DB.GetMunicipi(munID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	amID, _ := strconv.Atoi(r.FormValue("am_id"))
	arqID, _ := strconv.Atoi(r.FormValue("arquebisbat_id"))
	am := &db.ArquebisbatMunicipi{
		ID:            amID,
		MunicipiID:    munID,
		ArquebisbatID: arqID,
		AnyInici:      parseNullInt(r.FormValue("any_inici")),
		AnyFi:         parseNullInt(r.FormValue("any_fi")),
		Motiu:         strings.TrimSpace(r.FormValue("motiu")),
		Font:          strings.TrimSpace(r.FormValue("font")),
	}
	if am.ArquebisbatID == 0 {
		http.Redirect(w, r, "/territori/municipis/"+strconv.Itoa(munID)+"/edit?error=ecles", http.StatusSeeOther)
		return
	}
	_, _ = a.DB.SaveArquebisbatMunicipi(am)
	http.Redirect(w, r, "/territori/municipis/"+strconv.Itoa(munID)+"/edit", http.StatusSeeOther)
}

func (a *App) AdminSaveMunicipiNomHistoric(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permTerritory); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/territori/municipis", http.StatusSeeOther)
		return
	}
	munID := extractID(r.URL.Path)
	if munID == 0 {
		http.NotFound(w, r)
		return
	}
	_, err := a.DB.GetMunicipi(munID)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	nhID, _ := strconv.Atoi(r.FormValue("nh_id"))
	nh := &db.NomHistoric{
		ID:                    nhID,
		EntitatTipus:          "municipi",
		EntitatID:             munID,
		Nom:                   strings.TrimSpace(r.FormValue("nom")),
		AnyInici:              parseNullInt(r.FormValue("any_inici")),
		AnyFi:                 parseNullInt(r.FormValue("any_fi")),
		PaisRegne:             strings.TrimSpace(r.FormValue("pais_regne")),
		DistribucioGeografica: strings.TrimSpace(r.FormValue("distribucio_geografica")),
		Font:                  strings.TrimSpace(r.FormValue("font")),
	}
	if nh.Nom == "" {
		http.Redirect(w, r, "/territori/municipis/"+strconv.Itoa(munID)+"/edit?error=nomh", http.StatusSeeOther)
		return
	}
	_, _ = a.DB.SaveNomHistoric(nh)
	http.Redirect(w, r, "/territori/municipis/"+strconv.Itoa(munID)+"/edit", http.StatusSeeOther)
}
