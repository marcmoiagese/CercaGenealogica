package core

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

// PersonaRequest és un DTO senzill per a creació/edició via JSON.
type PersonaRequest struct {
	Nom            string `json:"nom"`
	Cognom1        string `json:"cognom1"`
	Cognom2        string `json:"cognom2"`
	Municipi       string `json:"municipi"`
	Llibre         string `json:"llibre"`
	Pagina         string `json:"pagina"`
	Ofici          string `json:"ofici"`
	Quinta         string `json:"quinta"`
	ModeracioMotiu string `json:"motiu"`
}

// Form per crear/editar persona (UI bàsica)
func (a *App) PersonaForm(w http.ResponseWriter, r *http.Request) {
	user, perms, ok := a.requirePermission(w, r, permCreatePerson)
	if !ok {
		return
	}
	canManageArxius := a.hasPerm(perms, permArxius)
	id := 0
	if r.URL.Path != "/persones/new" {
		id = extractID(r.URL.Path)
	}
	var p *db.Persona
	if id > 0 {
		p, _ = a.DB.GetPersona(id)
		if p == nil {
			http.NotFound(w, r)
			return
		}
		if p.CreatedBy.Valid && int(p.CreatedBy.Int64) != user.ID && !a.hasPerm(perms, func(pp db.PolicyPermissions) bool { return pp.CanEditAnyPerson }) {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
	}
	if p == nil {
		p = &db.Persona{}
	}
	RenderPrivateTemplate(w, r, "persona-form.html", map[string]interface{}{
		"Persona":           p,
		"IsNew":             id == 0,
		"User":              user,
		"CanManageArxius":   canManageArxius,
		"CanManagePolicies": perms.CanManagePolicies || perms.Admin,
		"CanModerate":       perms.CanModerate || perms.Admin,
	})
}

// Guarda persona des de formulari (crea pendent o marca pendent en edició)
func (a *App) PersonaSave(w http.ResponseWriter, r *http.Request) {
	user, perms, ok := a.requirePermission(w, r, permCreatePerson)
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	_ = r.ParseForm()
	id, _ := strconv.Atoi(r.FormValue("id"))
	nom := strings.TrimSpace(r.FormValue("nom"))
	cognom1 := strings.TrimSpace(r.FormValue("cognom1"))
	cognom2 := strings.TrimSpace(r.FormValue("cognom2"))
	if nom == "" {
		a.renderPersonaFormError(w, r, id, "El nom és obligatori.")
		return
	}
	if id > 0 {
		if existent, _ := a.DB.GetPersona(id); existent != nil {
			if existent.CreatedBy.Valid && int(existent.CreatedBy.Int64) != user.ID && !a.hasPerm(perms, func(pp db.PolicyPermissions) bool { return pp.CanEditAnyPerson }) {
				http.Error(w, "Forbidden", http.StatusForbidden)
				return
			}
		}
	}
	updatedBy := sql.NullInt64{Int64: int64(user.ID), Valid: true}
	if id == 0 {
		// En creació, updated_by = created_by
	} else if id > 0 {
		if existent, _ := a.DB.GetPersona(id); existent != nil && existent.UpdatedBy.Valid {
			updatedBy = existent.UpdatedBy
		}
	}
	createdBy := sql.NullInt64{Int64: int64(user.ID), Valid: true}
	if id > 0 {
		if existent, _ := a.DB.GetPersona(id); existent != nil && existent.CreatedBy.Valid {
			createdBy = existent.CreatedBy
		}
	}
	p := &db.Persona{
		ID:             id,
		Nom:            nom,
		Cognom1:        cognom1,
		Cognom2:        cognom2,
		Municipi:       r.FormValue("municipi"),
		Arquebisbat:    r.FormValue("arquevisbat"),
		Llibre:         r.FormValue("llibre"),
		Pagina:         r.FormValue("pagina"),
		Ofici:          r.FormValue("ofici"),
		Quinta:         r.FormValue("motiu"),
		ModeracioEstat: "pendent",
		ModeracioMotiu: r.FormValue("motiu"),
		CreatedBy:      createdBy,
		UpdatedBy:      updatedBy,
		DataNaixement:  sql.NullString{String: strings.TrimSpace(r.FormValue("data_naixement")), Valid: strings.TrimSpace(r.FormValue("data_naixement")) != ""},
		DataDefuncio:   sql.NullString{String: strings.TrimSpace(r.FormValue("data_defuncio")), Valid: strings.TrimSpace(r.FormValue("data_defuncio")) != ""},
	}
	if id == 0 {
		newID, err := a.DB.CreatePersona(p)
		if err != nil {
			a.renderPersonaFormError(w, r, id, "No s'ha pogut crear la persona.")
			return
		}
		_, _ = a.RegisterUserActivity(r.Context(), user.ID, rulePersonaCreate, "crear", "persona", &newID, "pendent", nil, "")
	} else {
		if err := a.DB.UpdatePersona(p); err != nil {
			a.renderPersonaFormError(w, r, id, "No s'ha pogut actualitzar la persona.")
			return
		}
		_, _ = a.RegisterUserActivity(r.Context(), user.ID, rulePersonaUpdate, "editar", "persona", &id, "pendent", nil, "")
	}
	http.Redirect(w, r, "/persones", http.StatusSeeOther)
}

// renderPersonaFormError retorna el formulari amb un missatge d'error reutilitzant l'estat
func (a *App) renderPersonaFormError(w http.ResponseWriter, r *http.Request, id int, msg string) {
	user := userFromContext(r)
	perms := db.PolicyPermissions{}
	if user != nil {
		perms = a.getPermissionsForUser(user.ID)
	}
	p := &db.Persona{}
	if id > 0 {
		if existent, _ := a.DB.GetPersona(id); existent != nil {
			p = existent
		}
	}
	RenderPrivateTemplate(w, r, "persona-form.html", map[string]interface{}{
		"Persona":           p,
		"IsNew":             id == 0,
		"Error":             msg,
		"User":              user,
		"CanManageArxius":   a.hasPerm(perms, permArxius),
		"CanManagePolicies": perms.CanManagePolicies || perms.Admin,
		"CanModerate":       perms.CanModerate || perms.Admin,
	})
}

// API pública: llista de persones publicades
func (a *App) ListPersonesPublic(w http.ResponseWriter, r *http.Request) {
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	perms := a.getPermissionsForUser(user.ID)
	canManageArxius := a.hasPerm(perms, permArxius)
	persones, err := a.DB.ListPersones(db.PersonaFilter{Estat: "publicat", Limit: 500})
	if err != nil {
		http.Error(w, "Error llistant persones", http.StatusInternalServerError)
		return
	}
	RenderPrivateTemplate(w, r, "persones-public.html", map[string]interface{}{
		"Persones":          persones,
		"User":              user,
		"CanManageArxius":   canManageArxius,
		"CanManagePolicies": perms.CanManagePolicies || perms.Admin,
		"CanModerate":       perms.CanModerate || perms.Admin,
	})
}

// Detall de persona (només publicades) per usuaris autenticats
func (a *App) PersonaDetall(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	user := userFromContext(r)
	perms := db.PolicyPermissions{}
	if user != nil {
		perms = a.getPermissionsForUser(user.ID)
	}
	p, err := a.DB.GetPersona(id)
	if err != nil || p == nil || p.ModeracioEstat != "publicat" {
		http.NotFound(w, r)
		return
	}
	RenderPrivateTemplate(w, r, "persona-detall.html", map[string]interface{}{
		"Persona":           p,
		"User":              user,
		"CanManageArxius":   a.hasPerm(perms, permArxius),
		"CanManagePolicies": perms.CanManagePolicies || perms.Admin,
		"CanModerate":       perms.CanModerate || perms.Admin,
		"Tab":               "detall",
	})
}

func (a *App) PersonaRegistres(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	user := userFromContext(r)
	perms := db.PolicyPermissions{}
	if user != nil {
		perms = a.getPermissionsForUser(user.ID)
	}
	p, err := a.DB.GetPersona(id)
	if err != nil || p == nil || p.ModeracioEstat != "publicat" {
		http.NotFound(w, r)
		return
	}
	tipus := strings.TrimSpace(r.URL.Query().Get("tipus"))
	rows, _ := a.DB.ListRegistresByPersona(id, tipus)
	type registreView struct {
		ID     int
		Tipus  string
		Rol    string
		Any    string
		Llibre string
		Pagina string
		Estat  string
	}
	var views []registreView
	for _, row := range rows {
		title := row.LlibreTitol.String
		if title == "" {
			title = row.LlibreNom.String
		}
		pagina := row.NumPaginaText
		if row.PaginaID.Valid {
			pagina = strconv.FormatInt(row.PaginaID.Int64, 10)
		}
		anyVal := ""
		if row.AnyDoc.Valid {
			anyVal = strconv.FormatInt(row.AnyDoc.Int64, 10)
		}
		views = append(views, registreView{
			ID:     row.RegistreID,
			Tipus:  row.TipusActe,
			Rol:    row.Rol,
			Any:    anyVal,
			Llibre: title,
			Pagina: pagina,
			Estat:  row.ModeracioEstat,
		})
	}
	RenderPrivateTemplate(w, r, "persona-registres.html", map[string]interface{}{
		"Persona":           p,
		"User":              user,
		"Registres":         views,
		"TipusOptions":      transcripcioTipusActe,
		"TipusSelected":     tipus,
		"CanManageArxius":   a.hasPerm(perms, permArxius),
		"CanManagePolicies": perms.CanManagePolicies || perms.Admin,
		"CanModerate":       perms.CanModerate || perms.Admin,
		"Tab":               "registres",
	})
}

// Creació de persona: qualsevol usuari amb permís can_create_person. Es guarda en estat pendent.
func (a *App) CreatePersona(w http.ResponseWriter, r *http.Request) {
	user, _, ok := a.requirePermission(w, r, permCreatePerson)
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	var req PersonaRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "JSON invàlid", http.StatusBadRequest)
		return
	}
	p := &db.Persona{
		Nom:            req.Nom,
		Cognom1:        req.Cognom1,
		Cognom2:        req.Cognom2,
		Municipi:       req.Municipi,
		Llibre:         req.Llibre,
		Pagina:         req.Pagina,
		Ofici:          req.Ofici,
		Quinta:         req.ModeracioMotiu,
		ModeracioEstat: "pendent",
		ModeracioMotiu: req.ModeracioMotiu,
		CreatedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
	}
	id, err := a.DB.CreatePersona(p)
	if err != nil {
		http.Error(w, "No s'ha pogut crear", http.StatusInternalServerError)
		return
	}
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, rulePersonaCreate, "crear", "persona", &id, "pendent", nil, "")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"id": id, "estat": p.ModeracioEstat})
}

// Edició de persona: es torna a estat pendent per revisió.
func (a *App) UpdatePersona(w http.ResponseWriter, r *http.Request) {
	user, perms, ok := a.requirePermission(w, r, permCreatePerson)
	if !ok {
		return
	}
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	id := extractID(r.URL.Path)
	var req PersonaRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "JSON invàlid", http.StatusBadRequest)
		return
	}
	existent, err := a.DB.GetPersona(id)
	if err != nil || existent == nil {
		http.NotFound(w, r)
		return
	}
	if existent.CreatedBy.Valid && int(existent.CreatedBy.Int64) != user.ID && !a.hasPerm(perms, func(pp db.PolicyPermissions) bool { return pp.CanEditAnyPerson }) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	existent.Nom = req.Nom
	existent.Cognom1 = req.Cognom1
	existent.Cognom2 = req.Cognom2
	existent.Municipi = req.Municipi
	existent.Llibre = req.Llibre
	existent.Pagina = req.Pagina
	existent.Ofici = req.Ofici
	existent.ModeracioEstat = "pendent"
	existent.ModeracioMotiu = req.ModeracioMotiu
	existent.Quinta = req.ModeracioMotiu
	existent.UpdatedBy = sql.NullInt64{Int64: int64(user.ID), Valid: true}
	existent.DataNaixement = sql.NullString{String: strings.TrimSpace(existent.DataNaixement.String), Valid: existent.DataNaixement.String != ""}
	existent.DataDefuncio = sql.NullString{String: strings.TrimSpace(existent.DataDefuncio.String), Valid: existent.DataDefuncio.String != ""}
	if err := a.DB.UpdatePersona(existent); err != nil {
		http.Error(w, "No s'ha pogut actualitzar", http.StatusInternalServerError)
		return
	}
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, rulePersonaUpdate, "editar", "persona", &id, "pendent", nil, "")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"id": id, "estat": existent.ModeracioEstat})
}
