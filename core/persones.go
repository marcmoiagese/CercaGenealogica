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
		if existent, _ := a.DB.GetPersona(id); existent != nil && existent.ModeracioEstat == "publicat" {
			after := *existent
			after.Nom = p.Nom
			after.Cognom1 = p.Cognom1
			after.Cognom2 = p.Cognom2
			after.Municipi = p.Municipi
			after.Arquebisbat = p.Arquebisbat
			after.Llibre = p.Llibre
			after.Pagina = p.Pagina
			after.Ofici = p.Ofici
			after.ModeracioEstat = "pendent"
			after.ModeracioMotiu = p.ModeracioMotiu
			after.Quinta = p.Quinta
			after.ModeratedBy = sql.NullInt64{}
			after.ModeratedAt = sql.NullTime{}
			after.UpdatedBy = updatedBy
			after.DataNaixement = p.DataNaixement
			after.DataDefuncio = p.DataDefuncio
			if existent.CreatedBy.Valid {
				after.CreatedBy = existent.CreatedBy
			}
			beforeJSON, _ := json.Marshal(existent)
			afterJSON, _ := json.Marshal(after)
			meta, err := buildWikiChangeMetadata(beforeJSON, afterJSON, 0)
			if err != nil {
				a.renderPersonaFormError(w, r, id, "No s'ha pogut preparar la proposta.")
				return
			}
			changeID, err := a.createWikiChange(&db.WikiChange{
				ObjectType:     "persona",
				ObjectID:       id,
				ChangeType:     "form",
				FieldKey:       "bulk",
				Metadata:       meta,
				ModeracioEstat: "pendent",
				ChangedBy:      sqlNullIntFromInt(user.ID),
			})
			if err != nil {
				a.renderPersonaFormError(w, r, id, "No s'ha pogut crear la proposta.")
				return
			}
			detail := "persona:" + strconv.Itoa(id)
			_, _ = a.RegisterUserActivity(r.Context(), user.ID, rulePersonaUpdate, "editar", "persona_canvi", &changeID, "pendent", nil, detail)
			http.Redirect(w, r, "/persones/"+strconv.Itoa(id)+"?pending=1", http.StatusSeeOther)
			return
		}
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
	lang := ResolveLang(r)
	p, err := a.DB.GetPersona(id)
	if err != nil || p == nil || p.ModeracioEstat != "publicat" {
		http.NotFound(w, r)
		return
	}
	fullName := strings.TrimSpace(strings.Join([]string{p.Nom, p.Cognom1, p.Cognom2}, " "))
	if fullName == "" {
		fullName = p.NomComplet
	}
	initials := ""
	if p.Nom != "" {
		r := []rune(strings.TrimSpace(p.Nom))
		if len(r) > 0 {
			initials += strings.ToUpper(string(r[0]))
		}
	}
	if p.Cognom1 != "" {
		r := []rune(strings.TrimSpace(p.Cognom1))
		if len(r) > 0 {
			initials += strings.ToUpper(string(r[0]))
		}
	}
	if initials == "" {
		initials = "?"
	}
	birthDate := ""
	if p.DataNaixement.Valid {
		birthDate = formatDateDisplay(p.DataNaixement.String)
	}
	baptismDate := ""
	if p.DataBateig.Valid {
		baptismDate = formatDateDisplay(p.DataBateig.String)
	}
	deathDate := ""
	if p.DataDefuncio.Valid {
		deathDate = formatDateDisplay(p.DataDefuncio.String)
	}
	lastUpdated := ""
	if p.UpdatedAt.Valid {
		lastUpdated = p.UpdatedAt.Time.Format("2006-01-02")
	} else if p.CreatedAt.Valid {
		lastUpdated = p.CreatedAt.Time.Format("2006-01-02")
	}
	totalFields := 0
	filledFields := 0
	addField := func(val string) {
		totalFields++
		if strings.TrimSpace(val) != "" {
			filledFields++
		}
	}
	addField(p.Nom)
	addField(p.Cognom1)
	addField(p.Cognom2)
	addField(p.Municipi)
	addField(p.Ofici)
	addField(p.Llibre)
	addField(p.Pagina)
	totalFields += 2
	if p.DataNaixement.Valid {
		filledFields++
	}
	if p.DataDefuncio.Valid {
		filledFields++
	}
	completesa := 0
	if totalFields > 0 {
		completesa = int(float64(filledFields) / float64(totalFields) * 100)
		if completesa > 100 {
			completesa = 100
		}
	}
	canEditPersona := false
	if user != nil {
		if perms.Admin || perms.CanEditAnyPerson {
			canEditPersona = true
		} else if p.CreatedBy.Valid && int(p.CreatedBy.Int64) == user.ID {
			canEditPersona = true
		}
	}
	markType := ""
	markPublic := true
	markOwn := false
	if user != nil {
		if marks, err := a.DB.ListWikiMarks("persona", []int{id}); err == nil {
			for _, mark := range marks {
				if mark.UserID == user.ID {
					markType = mark.Tipus
					markPublic = mark.IsPublic
					markOwn = true
					break
				}
			}
		}
	}
	wikiPending := strings.TrimSpace(r.URL.Query().Get("pending")) != ""
	type docView struct {
		ID     int
		Tipus  string
		Any    string
		Llibre string
		Pagina string
		Estat  string
	}
	type timelineEvent struct {
		Type        string
		FilterType  string
		Label       string
		Icon        string
		Date        string
		Title       string
		Source      string
		RegistreID  int
		RegistreAny string
	}
	type relationView struct {
		Role            string
		RoleLabel       string
		Name            string
		Municipi        string
		Ofici           string
		RegistreID      int
		RegistreTipus   string
		RegistreAny     string
		Llibre          string
		Linked          bool
		LinkedPersonaID int
	}
	type anecdoteView struct {
		ID       int
		Title    string
		Body     string
		Tag      string
		User     string
		Date     string
		Status   string
		Featured bool
	}
	var docs []docView
	var relacions []relationView
	var timeline []timelineEvent
	originMunicipi := ""
	originLlibre := ""
	originPagina := ""
	originRegistreID := 0
	originAny := ""
	var anecdotes []anecdoteView
	totalDocs := 0
	if rows, err := a.DB.ListRegistresByPersona(id, ""); err == nil {
		llibreCache := map[int]*db.Llibre{}
		munCache := map[int]string{}
		getMunicipiName := func(llibreID int) string {
			if llibreID <= 0 {
				return ""
			}
			if name, ok := munCache[llibreID]; ok {
				return name
			}
			llibre, ok := llibreCache[llibreID]
			if !ok {
				llibre, _ = a.DB.GetLlibre(llibreID)
				llibreCache[llibreID] = llibre
			}
			if llibre == nil || llibre.MunicipiID <= 0 {
				munCache[llibreID] = ""
				return ""
			}
			mun, err := a.DB.GetMunicipi(llibre.MunicipiID)
			if err != nil || mun == nil {
				munCache[llibreID] = ""
				return ""
			}
			name := strings.TrimSpace(mun.Nom)
			munCache[llibreID] = name
			return name
		}
		totalDocs = len(rows)
		if len(rows) > 0 {
			first := rows[0]
			originRegistreID = first.RegistreID
			if first.AnyDoc.Valid {
				originAny = strconv.FormatInt(first.AnyDoc.Int64, 10)
			}
			originLlibre = first.LlibreTitol.String
			if originLlibre == "" {
				originLlibre = first.LlibreNom.String
			}
			originPagina = first.NumPaginaText
			if first.PaginaID.Valid {
				originPagina = strconv.FormatInt(first.PaginaID.Int64, 10)
			}
		}
		for i, row := range rows {
			if i >= 6 {
				break
			}
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
			docs = append(docs, docView{
				ID:     row.RegistreID,
				Tipus:  row.TipusActe,
				Any:    anyVal,
				Llibre: title,
				Pagina: pagina,
				Estat:  row.ModeracioEstat,
			})
		}
		seenRel := map[string]bool{}
		seenEvent := map[string]bool{}
		getLabel := func(key, fallback string) string {
			if v := T(lang, key); v != "" && v != key {
				return v
			}
			return fallback
		}
		eventLabel := map[string]string{
			"naixement":   getLabel("records.field.data_naixement", "Naixement"),
			"baptisme":    getLabel("records.field.data_bateig", "Baptisme"),
			"defuncio":    getLabel("records.field.data_defuncio", "Defunció"),
			"enterrament": getLabel("records.field.data_enterrament", "Enterrament"),
			"matrimoni":   getLabel("records.field.data_matrimoni", "Matrimoni"),
			"confirmacio": getLabel("records.field.data_confirmacio", "Confirmació"),
		}
		eventIcon := map[string]string{
			"naixement":   "fa-baby",
			"baptisme":    "fa-water",
			"defuncio":    "fa-skull-crossbones",
			"enterrament": "fa-cross",
			"matrimoni":   "fa-ring",
			"confirmacio": "fa-star",
			"altre":       "fa-calendar",
		}
		addEvent := func(ev timelineEvent) {
			if strings.TrimSpace(ev.Date) == "" {
				return
			}
			key := ev.Type + "|" + ev.Date + "|" + strconv.Itoa(ev.RegistreID)
			if seenEvent[key] {
				return
			}
			seenEvent[key] = true
			timeline = append(timeline, ev)
		}
		acteDateFrom := func(reg *db.TranscripcioRaw) string {
			if reg == nil {
				return ""
			}
			if reg.DataActeISO.Valid {
				if formatted := formatDateDisplay(reg.DataActeISO.String); formatted != "" {
					return formatted
				}
			}
			raw := strings.TrimSpace(reg.DataActeText)
			if raw == "" {
				return ""
			}
			if formatted := formatDateDisplay(raw); formatted != "" {
				return formatted
			}
			return raw
		}
		for _, row := range rows {
			persones, err := a.DB.ListTranscripcioPersones(row.RegistreID)
			if err != nil {
				continue
			}
			attrs, _ := a.DB.ListTranscripcioAtributs(row.RegistreID)
			regRaw, _ := a.DB.GetTranscripcioRaw(row.RegistreID)
			title := row.LlibreTitol.String
			if title == "" {
				title = row.LlibreNom.String
			}
			anyVal := ""
			if row.AnyDoc.Valid {
				anyVal = strconv.FormatInt(row.AnyDoc.Int64, 10)
			}
			munName := getMunicipiName(row.LlibreID)
			tipus := normalizeRole(row.TipusActe)
			if originMunicipi == "" {
				if tipus == "baptisme" || tipus == "naixement" {
					originMunicipi = munName
				} else if originMunicipi == "" {
					originMunicipi = munName
				}
			}
			sourceType := ""
			if tipus != "" {
				key := "records.type." + tipus
				if label := T(lang, key); label != key {
					sourceType = label
				} else {
					sourceType = row.TipusActe
				}
			}
			source := "Registre #" + strconv.Itoa(row.RegistreID)
			if sourceType != "" {
				source += " · " + sourceType
			}
			if munName != "" {
				source += " · " + munName
			}
			baseEvent := func(eventType string, date string) {
				label := eventLabel[eventType]
				if label == "" {
					label = sourceType
				}
				icon := eventIcon[eventType]
				if icon == "" {
					icon = eventIcon["altre"]
				}
				filterType := eventType
				switch eventType {
				case "baptisme", "confirmacio":
					filterType = "sagrament"
				case "enterrament":
					filterType = "defuncio"
				}
				eventTitle := title
				if munName != "" {
					eventTitle = munName
				}
				addEvent(timelineEvent{
					Type:        eventType,
					FilterType:  filterType,
					Label:       label,
					Icon:        icon,
					Date:        date,
					Title:       eventTitle,
					Source:      source,
					RegistreID:  row.RegistreID,
					RegistreAny: anyVal,
				})
			}
			switch tipus {
			case "baptisme":
				if d := attrValueByKeysRaw(attrs,
					"data_naixement", "datanaixement", "naixement",
					"data_naixament", "datanaixament", "naixament",
					"nascut", "data_nascut", "datanascut",
				); d != "" {
					baseEvent("naixement", d)
				}
				date := attrValueByKeysRaw(attrs,
					"data_bateig", "databateig",
					"data_baptisme", "databaptisme",
					"bateig", "baptisme", "databapt", "data_bapt",
				)
				if date == "" {
					date = acteDateFrom(regRaw)
				}
				baseEvent("baptisme", date)
			case "obit", "defuncio":
				date := attrValueByKeysRaw(attrs, "data_defuncio", "datadefuncio", "defuncio")
				if date == "" {
					date = acteDateFrom(regRaw)
				}
				baseEvent("defuncio", date)
				if d := attrValueByKeysRaw(attrs, "data_enterrament", "enterrament", "data_enterr"); d != "" {
					baseEvent("enterrament", d)
				}
			case "matrimoni":
				date := attrValueByKeysRaw(attrs, "data_matrimoni", "datamatrimoni", "matrimoni", "data_casament", "casament")
				if date == "" {
					date = acteDateFrom(regRaw)
				}
				baseEvent("matrimoni", date)
			case "confirmacio":
				date := attrValueByKeysRaw(attrs, "data_confirmacio", "dataconfirmacio", "confirmacio")
				if date == "" {
					date = acteDateFrom(regRaw)
				}
				baseEvent("confirmacio", date)
			default:
				if date := acteDateFrom(regRaw); date != "" {
					baseEvent(tipus, date)
				}
			}
			for _, pr := range persones {
				if pr.PersonaID.Valid && int(pr.PersonaID.Int64) == id {
					continue
				}
				name := personDisplayName(pr)
				if name == "" {
					continue
				}
				role := strings.TrimSpace(pr.Rol)
				roleLabel := role
				normRole := normalizeRole(role)
				if normRole != "" {
					key := "records.role." + normRole
					if label := T(lang, key); label != key {
						roleLabel = label
					}
				}
				uniq := strconv.Itoa(row.RegistreID) + "|" + normRole + "|" + name
				if seenRel[uniq] {
					continue
				}
				seenRel[uniq] = true
				linkedID := 0
				if pr.PersonaID.Valid {
					linkedID = int(pr.PersonaID.Int64)
				}
				relacions = append(relacions, relationView{
					Role:            role,
					RoleLabel:       roleLabel,
					Name:            name,
					Municipi:        pr.MunicipiText,
					Ofici:           pr.OficiText,
					RegistreID:      row.RegistreID,
					RegistreTipus:   row.TipusActe,
					RegistreAny:     anyVal,
					Llibre:          title,
					Linked:          pr.PersonaID.Valid,
					LinkedPersonaID: linkedID,
				})
			}
		}
	}
	userID := 0
	if user != nil {
		userID = user.ID
	}
	if rows, err := a.DB.ListPersonaAnecdotes(id, userID); err == nil {
		for i, row := range rows {
			date := ""
			if row.CreatedAt.Valid {
				date = row.CreatedAt.Time.Format("2006-01-02")
			}
			userLabel := "usuari"
			if row.UserName.Valid && strings.TrimSpace(row.UserName.String) != "" {
				userLabel = strings.TrimSpace(row.UserName.String)
			}
			anecdotes = append(anecdotes, anecdoteView{
				ID:       row.ID,
				Title:    row.Title,
				Body:     row.Body,
				Tag:      row.Tag,
				User:     userLabel,
				Date:     date,
				Status:   row.Status,
				Featured: i == 0 && row.Status == "publicat",
			})
		}
	}
	if birthDate == "" {
		for _, ev := range timeline {
			if ev.Type == "naixement" && ev.Date != "" {
				birthDate = ev.Date
				break
			}
		}
	}
	if baptismDate == "" {
		for _, ev := range timeline {
			if ev.Type == "baptisme" && ev.Date != "" {
				baptismDate = ev.Date
				break
			}
		}
	}
	if deathDate == "" {
		for _, ev := range timeline {
			if ev.Type == "defuncio" && ev.Date != "" {
				deathDate = ev.Date
				break
			}
		}
	}
	locationLabel := strings.TrimSpace(p.Municipi)
	if locationLabel == "" {
		locationLabel = strings.TrimSpace(originMunicipi)
	}
	lifeRange := ""
	if birthDate != "" {
		lifeRange = birthDate
	}
	if deathDate != "" {
		if lifeRange != "" {
			lifeRange = lifeRange + " – " + deathDate
		} else {
			lifeRange = deathDate
		}
	}
	birthLabel := ""
	if birthDate != "" {
		birthLabel = birthDate
	}
	if locationLabel != "" {
		if birthLabel != "" {
			birthLabel += " · " + locationLabel
		} else {
			birthLabel = locationLabel
		}
	}
	deathLabel := ""
	if deathDate != "" {
		deathLabel = deathDate
	}
	RenderPrivateTemplate(w, r, "persona-detall.html", map[string]interface{}{
		"Persona":           p,
		"NomComplet":        fullName,
		"Initials":          initials,
		"BirthDate":         birthDate,
		"BaptismDate":       baptismDate,
		"DeathDate":         deathDate,
		"LifeRange":         lifeRange,
		"BirthLabel":        birthLabel,
		"DeathLabel":        deathLabel,
		"LastUpdated":       lastUpdated,
		"Completesa":        completesa,
		"CanEditPersona":    canEditPersona,
		"DocRegistres":      docs,
		"DocTotal":          totalDocs,
		"OriginMunicipi":    originMunicipi,
		"OriginLlibre":      originLlibre,
		"OriginPagina":      originPagina,
		"OriginRegistreID":  originRegistreID,
		"OriginAny":         originAny,
		"Relacions":         relacions,
		"TimelineEvents":    timeline,
		"Anecdotes":         anecdotes,
		"TipusOptions":      transcripcioTipusActe,
		"User":              user,
		"CanManageArxius":   a.hasPerm(perms, permArxius),
		"CanManagePolicies": perms.CanManagePolicies || perms.Admin,
		"CanModerate":       perms.CanModerate || perms.Admin,
		"MarkType":          markType,
		"MarkPublic":        markPublic,
		"MarkOwn":           markOwn,
		"WikiPending":       wikiPending,
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

func (a *App) PersonaAnecdoteForm(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	persona, err := a.DB.GetPersona(id)
	if err != nil || persona == nil {
		http.NotFound(w, r)
		return
	}
	fullName := strings.TrimSpace(strings.Join([]string{persona.Nom, persona.Cognom1, persona.Cognom2}, " "))
	if fullName == "" {
		fullName = persona.NomComplet
	}
	RenderPrivateTemplate(w, r, "persona-anecdotes-form.html", map[string]interface{}{
		"Persona":    persona,
		"NomComplet": fullName,
		"Form": map[string]string{
			"Title": "",
			"Tag":   "",
			"Body":  "",
		},
		"User": user,
	})
}

func (a *App) PersonaAnecdoteCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	persona, err := a.DB.GetPersona(id)
	if err != nil || persona == nil {
		http.NotFound(w, r)
		return
	}
	_ = r.ParseForm()
	title := strings.TrimSpace(r.FormValue("titol"))
	body := strings.TrimSpace(r.FormValue("text"))
	tag := strings.TrimSpace(r.FormValue("tag"))
	if title == "" || body == "" {
		fullName := strings.TrimSpace(strings.Join([]string{persona.Nom, persona.Cognom1, persona.Cognom2}, " "))
		if fullName == "" {
			fullName = persona.NomComplet
		}
		RenderPrivateTemplate(w, r, "persona-anecdotes-form.html", map[string]interface{}{
			"Persona":      persona,
			"NomComplet":   fullName,
			"ErrorMessage": "El títol i el text són obligatoris.",
			"Form": map[string]string{
				"Title": title,
				"Tag":   tag,
				"Body":  body,
			},
			"User": user,
		})
		return
	}
	perms := a.getPermissionsForUser(user.ID)
	status := "pendent"
	if perms.Admin || perms.CanModerate {
		status = "publicat"
	}
	anecdote := &db.PersonaAnecdote{
		PersonaID: id,
		UserID:    user.ID,
		Title:     title,
		Body:      body,
		Tag:       tag,
		Status:    status,
	}
	if _, err := a.DB.CreatePersonaAnecdote(anecdote); err != nil {
		http.Error(w, "No s'ha pogut desar l'anècdota", http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/persones/"+strconv.Itoa(id)+"#anecdotes", http.StatusSeeOther)
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
	if existent.ModeracioEstat == "publicat" {
		after := *existent
		after.Nom = req.Nom
		after.Cognom1 = req.Cognom1
		after.Cognom2 = req.Cognom2
		after.Municipi = req.Municipi
		after.Llibre = req.Llibre
		after.Pagina = req.Pagina
		after.Ofici = req.Ofici
		after.ModeracioEstat = "pendent"
		after.ModeracioMotiu = req.ModeracioMotiu
		after.Quinta = req.ModeracioMotiu
		after.ModeratedBy = sql.NullInt64{}
		after.ModeratedAt = sql.NullTime{}
		after.UpdatedBy = sql.NullInt64{Int64: int64(user.ID), Valid: true}
		after.DataNaixement = sql.NullString{String: strings.TrimSpace(after.DataNaixement.String), Valid: after.DataNaixement.String != ""}
		after.DataDefuncio = sql.NullString{String: strings.TrimSpace(after.DataDefuncio.String), Valid: after.DataDefuncio.String != ""}
		if existent.CreatedBy.Valid {
			after.CreatedBy = existent.CreatedBy
		}
		beforeJSON, _ := json.Marshal(existent)
		afterJSON, _ := json.Marshal(after)
		meta, err := buildWikiChangeMetadata(beforeJSON, afterJSON, 0)
		if err != nil {
			http.Error(w, "No s'ha pogut preparar la proposta", http.StatusInternalServerError)
			return
		}
		changeID, err := a.createWikiChange(&db.WikiChange{
			ObjectType:     "persona",
			ObjectID:       id,
			ChangeType:     "form",
			FieldKey:       "bulk",
			Metadata:       meta,
			ModeracioEstat: "pendent",
			ChangedBy:      sqlNullIntFromInt(user.ID),
		})
		if err != nil {
			http.Error(w, "No s'ha pogut crear la proposta", http.StatusInternalServerError)
			return
		}
		detail := "persona:" + strconv.Itoa(id)
		_, _ = a.RegisterUserActivity(r.Context(), user.ID, rulePersonaUpdate, "editar", "persona_canvi", &changeID, "pendent", nil, detail)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"id": id, "estat": "pendent", "pending": true, "change_id": changeID})
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
