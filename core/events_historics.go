package core

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const (
	eventHistoricMaxBytes = 2 << 20
)

type eventOption struct {
	Value string
	Label string
}

type eventImpactFormRow struct {
	ScopeType    string
	ScopeID      int
	ScopeLabel   string
	ImpacteTipus string
	Intensitat   int
	Notes        string
}

type eventFormState struct {
	Titol          string
	Tipus          string
	Resum          string
	Descripcio     string
	DataInici      string
	DataFi         string
	DataIniciAprox bool
	DataFiAprox    bool
	Precisio       string
	Fonts          string
	Impacts        []eventImpactFormRow
}

type eventListItem struct {
	ID         int
	Slug       string
	Title      string
	TypeLabel  string
	DateLabel  string
	Summary    string
	URL        string
	HasDate    bool
	DateYear   int
	DateDecade int
	Intensity  int
}

type eventImpactView struct {
	ScopeLabel     string
	ScopeTypeLabel string
	ImpactType     string
	Intensitat     int
	Notes          string
}

type eventScopeContext struct {
	Type  string
	ID    int
	Label string
	Back  string
}

type eventTimelineGroup struct {
	Label string
	Items []eventListItem
}

var eventHistoricTypes = []string{
	"guerra",
	"conflicte_local",
	"plaga",
	"pesta",
	"pandemia",
	"fam",
	"crisi_economica",
	"revolta",
	"incendi",
	"terratremol",
	"inundacio",
	"assassinat",
	"repressio",
	"migracio_massiva",
	"altres",
}

var eventHistoricScopeTypes = []string{
	"pais",
	"nivell_admin",
	"municipi",
	"entitat_eclesiastica",
}

var eventHistoricImpactTypes = []string{
	"directe",
	"indirecte",
	"transit",
	"rumor",
}

var eventHistoricPrecision = []string{
	"dia",
	"mes",
	"any",
	"decada",
}

func (a *App) EventsListPage(w http.ResponseWriter, r *http.Request) {
	a.renderEventsListPage(w, r, nil)
}

func (a *App) MunicipiEventsListPage(w http.ResponseWriter, r *http.Request) {
	munID := extractID(r.URL.Path)
	if munID <= 0 {
		http.NotFound(w, r)
		return
	}
	mun, err := a.DB.GetMunicipi(munID)
	if err != nil || mun == nil {
		http.NotFound(w, r)
		return
	}
	user, _ := a.VerificarSessio(r)
	perms := db.PolicyPermissions{}
	if user != nil {
		perms = a.getPermissionsForUser(user.ID)
	}
	canManageTerritory := user != nil && a.hasPerm(perms, permTerritory)
	canModerate := user != nil && a.hasPerm(perms, permModerate)
	if mun.ModeracioEstat != "" && mun.ModeracioEstat != "publicat" && !(canManageTerritory || canModerate) {
		http.NotFound(w, r)
		return
	}
	scope := &eventScopeContext{
		Type:  "municipi",
		ID:    mun.ID,
		Label: strings.TrimSpace(mun.Nom),
		Back:  fmt.Sprintf("/territori/municipis/%d", mun.ID),
	}
	a.renderEventsListPage(w, r, scope)
}

func (a *App) EventHistoricShow(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	if id <= 0 {
		http.NotFound(w, r)
		return
	}
	event, err := a.DB.GetEventHistoric(id)
	if err != nil || event == nil {
		http.NotFound(w, r)
		return
	}
	user, _ := a.VerificarSessio(r)
	perms := db.PolicyPermissions{}
	if user != nil {
		perms = a.getPermissionsForUser(user.ID)
	}
	canModerate := user != nil && a.hasPerm(perms, permModerate)
	ownsEvent := user != nil && event.CreatedBy.Valid && int(event.CreatedBy.Int64) == user.ID
	if !a.canViewEventHistoric(user, perms, event) {
		http.NotFound(w, r)
		return
	}

	lang := ResolveLang(r)
	impacts, _ := a.DB.ListEventImpacts(event.ID)
	impactViews := make([]eventImpactView, 0, len(impacts))
	for _, impact := range impacts {
		label, _ := a.resolveEventScopeLabel(impact.ScopeType, impact.ScopeID, lang)
		impactViews = append(impactViews, eventImpactView{
			ScopeLabel:     label,
			ScopeTypeLabel: eventScopeTypeLabel(lang, impact.ScopeType),
			ImpactType:     eventImpactTypeLabel(lang, impact.ImpacteTipus),
			Intensitat:     impact.Intensitat,
			Notes:          strings.TrimSpace(impact.Notes),
		})
	}

	canEdit := false
	if user != nil {
		if event.ModerationStatus == "publicat" {
			canEdit = true
		} else if canModerate || ownsEvent {
			canEdit = true
		}
	}
	markType := ""
	markPublic := true
	markOwn := false
	wikiPending := false
	token := ""
	if user != nil {
		if mark, err := a.DB.GetWikiMark("event_historic", event.ID, user.ID); err == nil && mark != nil {
			markType = mark.Tipus
			markPublic = mark.IsPublic
			markOwn = true
		}
		if changes, err := a.DB.ListWikiChanges("event_historic", event.ID); err == nil {
			for _, change := range filterVisibleWikiChanges(changes, user.ID, canModerate) {
				if change.ModeracioEstat == "pendent" {
					wikiPending = true
					break
				}
			}
		}
		token, _ = ensureCSRF(w, r)
	}
	data := map[string]interface{}{
		"Event":         event,
		"EventType":     eventTypeLabel(lang, event.Tipus),
		"EventDate":     eventDateLabel(*event),
		"Impacts":       impactViews,
		"ShowStatus":    event.ModerationStatus != "publicat",
		"CanEdit":       canEdit,
		"EditDisabled":  user != nil && !canEdit,
		"EditURL":       fmt.Sprintf("/historia/events/%d/editar", event.ID),
		"CanCreate":     user != nil,
		"StatusLabel":   eventModerationLabel(lang, event.ModerationStatus),
		"StatusPending": event.ModerationStatus == "pendent",
		"MarkType":      markType,
		"MarkPublic":    markPublic,
		"MarkOwn":       markOwn,
		"WikiPending":   wikiPending,
		"CSRFToken":     token,
	}
	if user != nil {
		RenderPrivateTemplate(w, r, "event-show.html", data)
		return
	}
	RenderTemplate(w, r, "event-show.html", data)
}

func (a *App) EventHistoricShowBySlug(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 4 {
		http.NotFound(w, r)
		return
	}
	slug := strings.TrimSpace(parts[3])
	if slug == "" {
		http.NotFound(w, r)
		return
	}
	event, err := a.DB.GetEventHistoricBySlug(slug)
	if err != nil || event == nil {
		http.NotFound(w, r)
		return
	}
	r.URL.Path = "/historia/events/" + strconv.Itoa(event.ID)
	a.EventHistoricShow(w, r)
}

func (a *App) EventHistoricNew(w http.ResponseWriter, r *http.Request) {
	user, _ := a.VerificarSessio(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	lang := ResolveLang(r)
	token, _ := ensureCSRF(w, r)
	form := eventFormState{}
	scopeType := strings.TrimSpace(r.URL.Query().Get("scope_type"))
	scopeID := parseFormInt(r.URL.Query().Get("scope_id"))
	if scopeType != "" && scopeID > 0 {
		label, _ := a.resolveEventScopeLabel(scopeType, scopeID, lang)
		form.Impacts = append(form.Impacts, eventImpactFormRow{
			ScopeType:    scopeType,
			ScopeID:      scopeID,
			ScopeLabel:   label,
			ImpacteTipus: "directe",
			Intensitat:   3,
		})
	}
	if len(form.Impacts) == 0 {
		form.Impacts = []eventImpactFormRow{{Intensitat: 3, ImpacteTipus: "directe"}}
	}
	data := map[string]interface{}{
		"CSRFToken":      token,
		"Form":           form,
		"EventTypes":     eventTypeOptions(lang),
		"ScopeTypes":     eventScopeTypeOptions(lang),
		"ImpactTypes":    eventImpactTypeOptions(lang),
		"PrecisionTypes": eventPrecisionOptions(lang),
		"IntensityOpts":  eventIntensityOptions(),
		"ErrorMessage":   "",
		"Ok":             false,
		"FormTitle":      T(lang, "events.form.title"),
		"FormSubtitle":   T(lang, "events.form.subtitle"),
		"SubmitLabel":    T(lang, "events.form.submit"),
		"BackURL":        "/historia/events",
		"FormAction":     "",
	}
	RenderPrivateTemplate(w, r, "event-form.html", data)
}

func (a *App) EventHistoricCreate(w http.ResponseWriter, r *http.Request) {
	user, _ := a.VerificarSessio(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, eventHistoricMaxBytes)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	lang := ResolveLang(r)
	form := buildEventFormState(r)
	if len(form.Impacts) == 0 {
		form.Impacts = []eventImpactFormRow{{Intensitat: 3, ImpacteTipus: "directe"}}
	}
	event := &db.EventHistoric{
		Titol:            strings.TrimSpace(form.Titol),
		Tipus:            strings.TrimSpace(form.Tipus),
		Resum:            strings.TrimSpace(form.Resum),
		Descripcio:       strings.TrimSpace(form.Descripcio),
		DataInici:        strings.TrimSpace(form.DataInici),
		DataFi:           strings.TrimSpace(form.DataFi),
		DataIniciAprox:   form.DataIniciAprox,
		DataFiAprox:      form.DataFiAprox,
		Precisio:         strings.TrimSpace(form.Precisio),
		Fonts:            strings.TrimSpace(form.Fonts),
		CreatedBy:        sqlNullIntFromInt(user.ID),
		ModerationStatus: "pendent",
	}
	if err := validateEventFormDates(event); err != nil {
		a.renderEventFormWithError(w, r, lang, form, err.Error(), 0, false)
		return
	}
	event.Slug = a.ensureUniqueEventSlug(event.Titol)
	impacts := buildEventImpacts(form, user.ID)
	if len(impacts) == 0 {
		a.renderEventFormWithError(w, r, lang, form, T(lang, "events.form.error.impacts"), 0, false)
		return
	}
	id, err := a.DB.CreateEventHistoric(event)
	if err != nil {
		a.renderEventFormWithError(w, r, lang, form, T(lang, "events.form.error.save"), 0, false)
		return
	}
	if err := a.DB.ReplaceEventImpacts(id, impacts); err != nil {
		Errorf("Error guardant impactes event %d: %v", id, err)
		a.renderEventFormWithError(w, r, lang, form, T(lang, "events.form.error.impacts"), 0, false)
		return
	}
	if id > 0 {
		detail := "event_historic:" + strconv.Itoa(id)
		_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleEventHistoricSubmit, "event_historic_submit", "event_historic", &id, "pendent", nil, detail)
	}
	http.Redirect(w, r, "/historia/events/"+strconv.Itoa(id)+"?created=1", http.StatusSeeOther)
}

func (a *App) EventHistoricEdit(w http.ResponseWriter, r *http.Request) {
	user, _ := a.VerificarSessio(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	id := extractID(r.URL.Path)
	if id <= 0 {
		http.NotFound(w, r)
		return
	}
	event, err := a.DB.GetEventHistoric(id)
	if err != nil || event == nil {
		http.NotFound(w, r)
		return
	}
	perms := a.getPermissionsForUser(user.ID)
	canModerate := a.hasPerm(perms, permModerate)
	ownsEvent := event.CreatedBy.Valid && int(event.CreatedBy.Int64) == user.ID
	if event.ModerationStatus != "publicat" && !(canModerate || ownsEvent) {
		http.NotFound(w, r)
		return
	}
	lang := ResolveLang(r)
	impacts, _ := a.DB.ListEventImpacts(event.ID)
	form := a.buildEventFormStateFromEvent(event, impacts, lang)
	token, _ := ensureCSRF(w, r)
	data := map[string]interface{}{
		"CSRFToken":      token,
		"Form":           form,
		"EventTypes":     eventTypeOptions(lang),
		"ScopeTypes":     eventScopeTypeOptions(lang),
		"ImpactTypes":    eventImpactTypeOptions(lang),
		"PrecisionTypes": eventPrecisionOptions(lang),
		"IntensityOpts":  eventIntensityOptions(),
		"ErrorMessage":   "",
		"Ok":             false,
		"FormTitle":      T(lang, "events.form.edit.title"),
		"FormSubtitle":   T(lang, "events.form.edit.subtitle"),
		"SubmitLabel":    T(lang, "events.form.edit.submit"),
		"BackURL":        fmt.Sprintf("/historia/events/%d", event.ID),
		"FormAction":     fmt.Sprintf("/historia/events/%d/editar", event.ID),
	}
	RenderPrivateTemplate(w, r, "event-form.html", data)
}

func (a *App) EventHistoricUpdate(w http.ResponseWriter, r *http.Request) {
	user, _ := a.VerificarSessio(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	id := extractID(r.URL.Path)
	if id <= 0 {
		http.NotFound(w, r)
		return
	}
	event, err := a.DB.GetEventHistoric(id)
	if err != nil || event == nil {
		http.NotFound(w, r)
		return
	}
	perms := a.getPermissionsForUser(user.ID)
	canModerate := a.hasPerm(perms, permModerate)
	ownsEvent := event.CreatedBy.Valid && int(event.CreatedBy.Int64) == user.ID
	if event.ModerationStatus != "publicat" && !(canModerate || ownsEvent) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, eventHistoricMaxBytes)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	lang := ResolveLang(r)
	form := buildEventFormState(r)
	if len(form.Impacts) == 0 {
		form.Impacts = []eventImpactFormRow{{Intensitat: 3, ImpacteTipus: "directe"}}
	}
	if err := validateEventFormDates(&db.EventHistoric{
		DataInici:      strings.TrimSpace(form.DataInici),
		DataFi:         strings.TrimSpace(form.DataFi),
		DataIniciAprox: form.DataIniciAprox,
		DataFiAprox:    form.DataFiAprox,
	}); err != nil {
		a.renderEventFormWithError(w, r, lang, form, err.Error(), id, true)
		return
	}
	impacts := buildEventImpacts(form, user.ID)
	if len(impacts) == 0 {
		a.renderEventFormWithError(w, r, lang, form, T(lang, "events.form.error.impacts"), id, true)
		return
	}

	if event.ModerationStatus == "publicat" {
		existingImpacts, _ := a.DB.ListEventImpacts(event.ID)
		beforeJSON, _ := buildEventHistoricSnapshot(event, existingImpacts)
		afterEvent := *event
		afterEvent.Titol = strings.TrimSpace(form.Titol)
		afterEvent.Tipus = strings.TrimSpace(form.Tipus)
		afterEvent.Resum = strings.TrimSpace(form.Resum)
		afterEvent.Descripcio = strings.TrimSpace(form.Descripcio)
		afterEvent.DataInici = strings.TrimSpace(form.DataInici)
		afterEvent.DataFi = strings.TrimSpace(form.DataFi)
		afterEvent.DataIniciAprox = form.DataIniciAprox
		afterEvent.DataFiAprox = form.DataFiAprox
		afterEvent.Precisio = strings.TrimSpace(form.Precisio)
		afterEvent.Fonts = strings.TrimSpace(form.Fonts)
		afterEvent.ModerationStatus = "pendent"
		afterEvent.ModerationNotes = ""
		afterEvent.ModeratedBy = sql.NullInt64{}
		afterEvent.ModeratedAt = sql.NullTime{}
		if err := db.ValidateEventHistoric(&afterEvent); err != nil {
			a.renderEventFormWithError(w, r, lang, form, err.Error(), id, true)
			return
		}
		afterJSON, _ := buildEventHistoricSnapshot(&afterEvent, impacts)
		meta, err := buildWikiChangeMetadata(beforeJSON, afterJSON, 0)
		if err != nil {
			a.renderEventFormWithError(w, r, lang, form, T(lang, "events.form.error.save"), id, true)
			return
		}
		if !a.ensureWikiChangeAllowed(w, r, lang) {
			return
		}
		changeID, err := a.createWikiChange(&db.WikiChange{
			ObjectType:     "event_historic",
			ObjectID:       id,
			ChangeType:     "form",
			FieldKey:       "bulk",
			Metadata:       meta,
			ModeracioEstat: "pendent",
			ChangedBy:      sqlNullIntFromInt(user.ID),
		})
		if err != nil {
			if _, msg, ok := a.wikiGuardrailInfo(lang, err); ok {
				a.renderEventFormWithError(w, r, lang, form, msg, id, true)
				return
			}
			a.renderEventFormWithError(w, r, lang, form, T(lang, "events.form.error.save"), id, true)
			return
		}
		detail := "event_historic:" + strconv.Itoa(id)
		_, _ = a.RegisterUserActivity(r.Context(), user.ID, "", "editar", "event_historic_canvi", &changeID, "pendent", nil, detail)
		http.Redirect(w, r, "/historia/events/"+strconv.Itoa(id)+"?pending=1", http.StatusSeeOther)
		return
	}

	updated := *event
	updated.Titol = strings.TrimSpace(form.Titol)
	updated.Tipus = strings.TrimSpace(form.Tipus)
	updated.Resum = strings.TrimSpace(form.Resum)
	updated.Descripcio = strings.TrimSpace(form.Descripcio)
	updated.DataInici = strings.TrimSpace(form.DataInici)
	updated.DataFi = strings.TrimSpace(form.DataFi)
	updated.DataIniciAprox = form.DataIniciAprox
	updated.DataFiAprox = form.DataFiAprox
	updated.Precisio = strings.TrimSpace(form.Precisio)
	updated.Fonts = strings.TrimSpace(form.Fonts)
	updated.ModerationStatus = "pendent"
	updated.ModerationNotes = ""
	updated.ModeratedBy = sql.NullInt64{}
	updated.ModeratedAt = sql.NullTime{}
	if err := a.DB.UpdateEventHistoric(&updated); err != nil {
		a.renderEventFormWithError(w, r, lang, form, T(lang, "events.form.error.save"), id, true)
		return
	}
	if err := a.DB.ReplaceEventImpacts(id, impacts); err != nil {
		Errorf("Error guardant impactes event %d: %v", id, err)
		a.renderEventFormWithError(w, r, lang, form, T(lang, "events.form.error.impacts"), id, true)
		return
	}
	http.Redirect(w, r, "/historia/events/"+strconv.Itoa(id)+"?updated=1", http.StatusSeeOther)
}

func (a *App) EventsAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) >= 3 {
		id, err := strconv.Atoi(parts[2])
		if err != nil || id <= 0 {
			http.NotFound(w, r)
			return
		}
		event, err := a.DB.GetEventHistoric(id)
		if err != nil || event == nil || event.ModerationStatus != "publicat" {
			http.NotFound(w, r)
			return
		}
		impacts, _ := a.DB.ListEventImpacts(id)
		lang := ResolveLang(r)
		impactViews := []map[string]interface{}{}
		for _, impact := range impacts {
			label, _ := a.resolveEventScopeLabel(impact.ScopeType, impact.ScopeID, lang)
			impactViews = append(impactViews, map[string]interface{}{
				"scope_type":    impact.ScopeType,
				"scope_id":      impact.ScopeID,
				"scope_label":   label,
				"impacte_tipus": impact.ImpacteTipus,
				"intensitat":    impact.Intensitat,
				"notes":         strings.TrimSpace(impact.Notes),
			})
		}
		writeJSON(w, map[string]interface{}{
			"event": map[string]interface{}{
				"id":         event.ID,
				"titol":      strings.TrimSpace(event.Titol),
				"slug":       strings.TrimSpace(event.Slug),
				"tipus":      strings.TrimSpace(event.Tipus),
				"resum":      strings.TrimSpace(event.Resum),
				"descripcio": strings.TrimSpace(event.Descripcio),
				"data_inici": strings.TrimSpace(event.DataInici),
				"data_fi":    strings.TrimSpace(event.DataFi),
				"precisio":   strings.TrimSpace(event.Precisio),
				"fonts":      strings.TrimSpace(event.Fonts),
			},
			"impacts": impactViews,
		})
		return
	}
	list, hasNext, page, perPage := a.fetchEventsList(r, nil)
	payload := make([]map[string]interface{}, 0, len(list))
	for _, item := range list {
		payload = append(payload, map[string]interface{}{
			"id":      item.ID,
			"title":   item.Title,
			"type":    item.TypeLabel,
			"date":    item.DateLabel,
			"url":     item.URL,
			"slug":    item.Slug,
			"summary": item.Summary,
		})
	}
	writeJSON(w, map[string]interface{}{
		"items":    payload,
		"page":     page,
		"per_page": perPage,
		"has_next": hasNext,
	})
}

func (a *App) ScopeSearchAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	scopeType := strings.TrimSpace(r.URL.Query().Get("type"))
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
	lang := ResolveLang(r)
	items := []map[string]interface{}{}
	switch scopeType {
	case "municipi":
		filter := db.MunicipiBrowseFilter{
			Text:   query,
			Status: "publicat",
			Limit:  limit,
		}
		rows, _ := a.DB.SuggestMunicipis(filter)
		for _, row := range rows {
			context := buildMunicipiSuggestContext(row, lang)
			levelIDs := make([]int, 7)
			for i := 0; i < 7; i++ {
				if row.LevelIDs[i].Valid {
					levelIDs[i] = int(row.LevelIDs[i].Int64)
				}
			}
			items = append(items, map[string]interface{}{
				"id":      row.ID,
				"label":   row.Nom,
				"context": context,
				"pais_id": row.PaisID,
				"nivells": levelIDs,
			})
		}
	case "nivell_admin":
		filter := db.NivellAdminFilter{
			Text:   query,
			Status: "publicat",
			Limit:  limit,
		}
		rows, _ := a.DB.ListNivells(filter)
		for _, row := range rows {
			label := strings.TrimSpace(row.NomNivell)
			if label == "" {
				continue
			}
			context := eventScopeTypeLabel(lang, "nivell_admin")
			items = append(items, map[string]interface{}{
				"id":      row.ID,
				"label":   label,
				"context": context,
			})
		}
	case "entitat_eclesiastica":
		filter := db.ArquebisbatFilter{
			Text:   query,
			Status: "publicat",
			Limit:  limit,
		}
		rows, _ := a.DB.ListArquebisbats(filter)
		for _, row := range rows {
			label := strings.TrimSpace(row.Nom)
			if label == "" {
				continue
			}
			context := strings.TrimSpace(row.TipusEntitat)
			if row.ParentNom.Valid {
				context = joinNonEmpty(context, strings.TrimSpace(row.ParentNom.String), " · ")
			}
			items = append(items, map[string]interface{}{
				"id":      row.ID,
				"label":   label,
				"context": context,
			})
		}
	case "pais":
		paisos, _ := a.DB.ListPaisos()
		queryLower := strings.ToLower(query)
		for _, pais := range paisos {
			label := a.countryLabelFromISO(pais.CodiISO2, lang)
			if label == "" {
				label = strings.ToUpper(strings.TrimSpace(pais.CodiISO2))
			}
			if !strings.Contains(strings.ToLower(label), queryLower) &&
				!strings.Contains(strings.ToLower(pais.CodiISO2), queryLower) &&
				!strings.Contains(strings.ToLower(pais.CodiISO3), queryLower) {
				continue
			}
			items = append(items, map[string]interface{}{
				"id":      pais.ID,
				"label":   label,
				"context": strings.ToUpper(strings.TrimSpace(pais.CodiISO2)),
			})
			if len(items) >= limit {
				break
			}
		}
	default:
		writeJSON(w, map[string]interface{}{"items": []interface{}{}})
		return
	}
	writeJSON(w, map[string]interface{}{"items": items})
}

func (a *App) renderEventsListPage(w http.ResponseWriter, r *http.Request, scope *eventScopeContext) {
	user, _ := a.VerificarSessio(r)
	list, hasNext, page, perPage := a.fetchEventsList(r, scope)
	lang := ResolveLang(r)

	filterQ := strings.TrimSpace(r.URL.Query().Get("q"))
	filterTipus := strings.TrimSpace(r.URL.Query().Get("tipus"))
	filterFrom := strings.TrimSpace(r.URL.Query().Get("from"))
	filterTo := strings.TrimSpace(r.URL.Query().Get("to"))
	filterView := strings.TrimSpace(r.URL.Query().Get("view"))
	filterGroup := strings.TrimSpace(r.URL.Query().Get("group"))
	filterOrder := normalizeEventOrder(r.URL.Query().Get("order"))
	filterWithDates := parseFormBool(r.URL.Query().Get("with_dates"))
	filterScopeType := strings.TrimSpace(r.URL.Query().Get("scope_type"))
	filterScopeID := parseFormInt(r.URL.Query().Get("scope_id"))
	filterScopeLabel := strings.TrimSpace(r.URL.Query().Get("scope_label"))
	filterImpactType := strings.TrimSpace(r.URL.Query().Get("impacte_tipus"))
	filterIntensitat := parseFormInt(r.URL.Query().Get("intensitat_min"))

	isTimeline := filterView == "timeline"
	if isTimeline && filterGroup == "" {
		filterGroup = "any"
	}
	if isTimeline && r.URL.Query().Get("with_dates") == "" {
		filterWithDates = true
		q := r.URL.Query()
		q.Set("with_dates", "1")
		r.URL.RawQuery = q.Encode()
	}

	if scope != nil {
		filterScopeType = scope.Type
		filterScopeID = scope.ID
		filterScopeLabel = scope.Label
	}
	if filterScopeLabel == "" && filterScopeType != "" && filterScopeID > 0 {
		if label, _ := a.resolveEventScopeLabel(filterScopeType, filterScopeID, lang); label != "" {
			filterScopeLabel = label
		}
	}

	values := url.Values{}
	if filterQ != "" {
		values.Set("q", filterQ)
	}
	if filterTipus != "" {
		values.Set("tipus", filterTipus)
	}
	if filterFrom != "" {
		values.Set("from", filterFrom)
	}
	if filterTo != "" {
		values.Set("to", filterTo)
	}
	if filterView != "" {
		values.Set("view", filterView)
	}
	if filterGroup != "" {
		values.Set("group", filterGroup)
	}
	if filterScopeType != "" {
		values.Set("scope_type", filterScopeType)
	}
	if filterScopeID > 0 {
		values.Set("scope_id", strconv.Itoa(filterScopeID))
	}
	if filterScopeLabel != "" {
		values.Set("scope_label", filterScopeLabel)
	}
	if filterImpactType != "" {
		values.Set("impacte_tipus", filterImpactType)
	}
	if filterIntensitat > 0 {
		values.Set("intensitat_min", strconv.Itoa(filterIntensitat))
	}
	if filterWithDates {
		values.Set("with_dates", "1")
	}
	if filterOrder != "" {
		values.Set("order", filterOrder)
	}
	if perPage != 25 {
		values.Set("per_page", strconv.Itoa(perPage))
	}
	pageBase := "/historia/events"
	if scope != nil && scope.Type == "municipi" && scope.ID > 0 {
		pageBase = fmt.Sprintf("/territori/municipis/%d/events", scope.ID)
	}
	if encoded := values.Encode(); encoded != "" {
		pageBase = pageBase + "?" + encoded
	} else {
		pageBase = pageBase + "?"
	}
	if isTimeline {
		list = a.populateEventListIntensity(list, filterScopeType, filterScopeID)
	}
	timelineGroups := []eventTimelineGroup{}
	if isTimeline {
		timelineGroups = buildEventTimelineGroups(list, filterGroup, lang)
	}
	data := map[string]interface{}{
		"Events":            list,
		"TimelineGroups":    timelineGroups,
		"IsTimeline":        isTimeline,
		"HasPrev":           page > 1,
		"HasNext":           hasNext,
		"PrevPage":          page - 1,
		"NextPage":          page + 1,
		"PageBase":          pageBase,
		"FilterQ":           filterQ,
		"FilterTipus":       filterTipus,
		"FilterFrom":        filterFrom,
		"FilterTo":          filterTo,
		"FilterView":        filterView,
		"FilterGroup":       filterGroup,
		"FilterOrder":       filterOrder,
		"FilterWithDates":   filterWithDates,
		"FilterScopeType":   filterScopeType,
		"FilterScopeID":     filterScopeID,
		"FilterScopeLabel":  filterScopeLabel,
		"FilterImpactType":  filterImpactType,
		"FilterIntensitat":  filterIntensitat,
		"EventTypes":        eventTypeOptions(lang),
		"ScopeTypes":        eventScopeTypeOptions(lang),
		"ImpactTypes":       eventImpactTypeOptions(lang),
		"IntensityOpts":     eventIntensityOptions(),
		"OrderOptions":      eventOrderOptions(lang),
		"TimelineGroupOpts": eventTimelineGroupOptions(lang),
		"CanCreate":         user != nil,
		"ScopeContext":      scope,
		"ScopeFixed":        scope != nil,
	}
	if user != nil {
		RenderPrivateTemplate(w, r, "events-list.html", data)
		return
	}
	RenderTemplate(w, r, "events-list.html", data)
}

func (a *App) fetchEventsList(r *http.Request, scope *eventScopeContext) ([]eventListItem, bool, int, int) {
	lang := ResolveLang(r)
	page := parseListPage(r.URL.Query().Get("page"))
	perPage := parseListPerPage(r.URL.Query().Get("per_page"))
	if perPage == 25 {
		if val := strings.TrimSpace(r.URL.Query().Get("page_size")); val != "" {
			perPage = parseListPerPage(val)
		}
	}
	offset := (page - 1) * perPage
	if offset < 0 {
		offset = 0
	}
	filter := db.EventHistoricFilter{
		Query:         strings.TrimSpace(r.URL.Query().Get("q")),
		Tipus:         strings.TrimSpace(r.URL.Query().Get("tipus")),
		Status:        "publicat",
		ImpacteTipus:  strings.TrimSpace(r.URL.Query().Get("impacte_tipus")),
		IntensitatMin: parseFormInt(r.URL.Query().Get("intensitat_min")),
		OnlyWithDates: parseFormBool(r.URL.Query().Get("with_dates")),
		OrderBy:       normalizeEventOrder(r.URL.Query().Get("order")),
		Limit:         perPage + 1,
		Offset:        offset,
	}
	if scope != nil {
		filter.ImpacteTipus = strings.TrimSpace(r.URL.Query().Get("impacte_tipus"))
		filter.IntensitatMin = parseFormInt(r.URL.Query().Get("intensitat_min"))
	}
	if from := strings.TrimSpace(r.URL.Query().Get("from")); from != "" {
		if parsed, err := time.Parse("2006-01-02", from); err == nil {
			filter.From = parsed
		}
	}
	if to := strings.TrimSpace(r.URL.Query().Get("to")); to != "" {
		if parsed, err := time.Parse("2006-01-02", to); err == nil {
			filter.To = parsed
		}
	}
	var rows []db.EventHistoric
	var err error
	scopeType := strings.TrimSpace(r.URL.Query().Get("scope_type"))
	scopeID := parseFormInt(r.URL.Query().Get("scope_id"))
	if scope != nil {
		scopeType = scope.Type
		scopeID = scope.ID
	}
	if scopeType != "" && scopeID > 0 {
		rows, err = a.DB.ListEventsByScope(scopeType, scopeID, filter)
	} else {
		rows, err = a.DB.ListEventsHistoric(filter)
	}
	if err != nil {
		Errorf("Error carregant events: %v", err)
		rows = []db.EventHistoric{}
	}
	hasNext := len(rows) > perPage
	if hasNext {
		rows = rows[:perPage]
	}
	list := make([]eventListItem, 0, len(rows))
	for _, row := range rows {
		hasDate := false
		year := 0
		decade := 0
		if parsed, ok := parseEventDateForTimeline(row); ok {
			hasDate = true
			year = parsed.Year()
			decade = (year / 10) * 10
		}
		item := eventListItem{
			ID:         row.ID,
			Slug:       strings.TrimSpace(row.Slug),
			Title:      strings.TrimSpace(row.Titol),
			TypeLabel:  eventTypeLabel(lang, row.Tipus),
			DateLabel:  eventDateLabel(row),
			Summary:    strings.TrimSpace(row.Resum),
			URL:        fmt.Sprintf("/historia/events/%d", row.ID),
			HasDate:    hasDate,
			DateYear:   year,
			DateDecade: decade,
		}
		if item.Summary == "" {
			item.Summary = summarizeHistoriaText(row.Descripcio, 240)
		}
		list = append(list, item)
	}
	return list, hasNext, page, perPage
}

func (a *App) renderEventFormWithError(w http.ResponseWriter, r *http.Request, lang string, form eventFormState, msg string, eventID int, isEdit bool) {
	token, _ := ensureCSRF(w, r)
	formTitle := T(lang, "events.form.title")
	formSubtitle := T(lang, "events.form.subtitle")
	submitLabel := T(lang, "events.form.submit")
	backURL := "/historia/events"
	formAction := ""
	if isEdit {
		formTitle = T(lang, "events.form.edit.title")
		formSubtitle = T(lang, "events.form.edit.subtitle")
		submitLabel = T(lang, "events.form.edit.submit")
		if eventID > 0 {
			backURL = fmt.Sprintf("/historia/events/%d", eventID)
			formAction = fmt.Sprintf("/historia/events/%d/editar", eventID)
		}
	}
	data := map[string]interface{}{
		"CSRFToken":      token,
		"Form":           form,
		"EventTypes":     eventTypeOptions(lang),
		"ScopeTypes":     eventScopeTypeOptions(lang),
		"ImpactTypes":    eventImpactTypeOptions(lang),
		"PrecisionTypes": eventPrecisionOptions(lang),
		"IntensityOpts":  eventIntensityOptions(),
		"ErrorMessage":   msg,
		"Ok":             false,
		"FormTitle":      formTitle,
		"FormSubtitle":   formSubtitle,
		"SubmitLabel":    submitLabel,
		"BackURL":        backURL,
		"FormAction":     formAction,
	}
	RenderPrivateTemplate(w, r, "event-form.html", data)
}

func buildEventFormState(r *http.Request) eventFormState {
	form := eventFormState{
		Titol:          strings.TrimSpace(r.FormValue("titol")),
		Tipus:          strings.TrimSpace(r.FormValue("tipus")),
		Resum:          strings.TrimSpace(r.FormValue("resum")),
		Descripcio:     strings.TrimSpace(r.FormValue("descripcio")),
		DataInici:      strings.TrimSpace(r.FormValue("data_inici")),
		DataFi:         strings.TrimSpace(r.FormValue("data_fi")),
		DataIniciAprox: r.FormValue("data_inici_aprox") != "",
		DataFiAprox:    r.FormValue("data_fi_aprox") != "",
		Precisio:       strings.TrimSpace(r.FormValue("precisio")),
		Fonts:          strings.TrimSpace(r.FormValue("fonts")),
	}
	scopeTypes := r.Form["impact_scope_type"]
	scopeIDs := r.Form["impact_scope_id"]
	scopeLabels := r.Form["impact_scope_label"]
	impactTypes := r.Form["impact_type"]
	intensitats := r.Form["impact_intensitat"]
	notes := r.Form["impact_notes"]
	max := maxLenStrings(scopeTypes, scopeIDs, impactTypes, intensitats, notes, scopeLabels)
	for i := 0; i < max; i++ {
		row := eventImpactFormRow{}
		if i < len(scopeTypes) {
			row.ScopeType = strings.TrimSpace(scopeTypes[i])
		}
		if i < len(scopeIDs) {
			row.ScopeID = parseFormInt(scopeIDs[i])
		}
		if i < len(scopeLabels) {
			row.ScopeLabel = strings.TrimSpace(scopeLabels[i])
		}
		if i < len(impactTypes) {
			row.ImpacteTipus = strings.TrimSpace(impactTypes[i])
		}
		if i < len(intensitats) {
			row.Intensitat = parseFormInt(intensitats[i])
		}
		if i < len(notes) {
			row.Notes = strings.TrimSpace(notes[i])
		}
		if row.ScopeType == "" && row.ScopeID == 0 && row.ImpacteTipus == "" && row.Notes == "" {
			continue
		}
		form.Impacts = append(form.Impacts, row)
	}
	return form
}

func (a *App) buildEventFormStateFromEvent(event *db.EventHistoric, impacts []db.EventHistoricImpact, lang string) eventFormState {
	form := eventFormState{}
	if event != nil {
		form = eventFormState{
			Titol:          strings.TrimSpace(event.Titol),
			Tipus:          strings.TrimSpace(event.Tipus),
			Resum:          strings.TrimSpace(event.Resum),
			Descripcio:     strings.TrimSpace(event.Descripcio),
			DataInici:      strings.TrimSpace(event.DataInici),
			DataFi:         strings.TrimSpace(event.DataFi),
			DataIniciAprox: event.DataIniciAprox,
			DataFiAprox:    event.DataFiAprox,
			Precisio:       strings.TrimSpace(event.Precisio),
			Fonts:          strings.TrimSpace(event.Fonts),
		}
	}
	for _, impact := range impacts {
		label, _ := a.resolveEventScopeLabel(impact.ScopeType, impact.ScopeID, lang)
		form.Impacts = append(form.Impacts, eventImpactFormRow{
			ScopeType:    strings.TrimSpace(impact.ScopeType),
			ScopeID:      impact.ScopeID,
			ScopeLabel:   label,
			ImpacteTipus: strings.TrimSpace(impact.ImpacteTipus),
			Intensitat:   impact.Intensitat,
			Notes:        strings.TrimSpace(impact.Notes),
		})
	}
	if len(form.Impacts) == 0 {
		form.Impacts = []eventImpactFormRow{{Intensitat: 3, ImpacteTipus: "directe"}}
	}
	return form
}

func buildEventImpacts(form eventFormState, userID int) []db.EventHistoricImpact {
	impacts := []db.EventHistoricImpact{}
	for _, row := range form.Impacts {
		if strings.TrimSpace(row.ScopeType) == "" || row.ScopeID <= 0 {
			continue
		}
		impactType := strings.TrimSpace(row.ImpacteTipus)
		if impactType == "" {
			impactType = "directe"
		}
		intensitat := row.Intensitat
		if intensitat == 0 {
			intensitat = 3
		}
		impacts = append(impacts, db.EventHistoricImpact{
			ScopeType:    strings.TrimSpace(row.ScopeType),
			ScopeID:      row.ScopeID,
			ImpacteTipus: impactType,
			Intensitat:   intensitat,
			Notes:        strings.TrimSpace(row.Notes),
			CreatedBy:    sqlNullIntFromInt(userID),
		})
	}
	return impacts
}

func validateEventFormDates(event *db.EventHistoric) error {
	if event == nil {
		return errors.New("event nil")
	}
	if event.DataInici != "" {
		if _, err := time.Parse("2006-01-02", event.DataInici); err != nil {
			return err
		}
	}
	if event.DataFi != "" {
		if _, err := time.Parse("2006-01-02", event.DataFi); err != nil {
			return err
		}
	}
	if event.DataInici != "" && event.DataFi != "" {
		start, _ := time.Parse("2006-01-02", event.DataInici)
		end, _ := time.Parse("2006-01-02", event.DataFi)
		if end.Before(start) {
			return errors.New("data_fi before data_inici")
		}
	}
	return nil
}

func eventTypeOptions(lang string) []eventOption {
	opts := make([]eventOption, 0, len(eventHistoricTypes))
	for _, val := range eventHistoricTypes {
		opts = append(opts, eventOption{Value: val, Label: eventTypeLabel(lang, val)})
	}
	return opts
}

func eventScopeTypeOptions(lang string) []eventOption {
	opts := make([]eventOption, 0, len(eventHistoricScopeTypes))
	for _, val := range eventHistoricScopeTypes {
		opts = append(opts, eventOption{Value: val, Label: eventScopeTypeLabel(lang, val)})
	}
	return opts
}

func eventImpactTypeOptions(lang string) []eventOption {
	opts := make([]eventOption, 0, len(eventHistoricImpactTypes))
	for _, val := range eventHistoricImpactTypes {
		opts = append(opts, eventOption{Value: val, Label: eventImpactTypeLabel(lang, val)})
	}
	return opts
}

func eventPrecisionOptions(lang string) []eventOption {
	opts := []eventOption{{Value: "", Label: T(lang, "common.all")}}
	for _, val := range eventHistoricPrecision {
		opts = append(opts, eventOption{Value: val, Label: eventPrecisionLabel(lang, val)})
	}
	return opts
}

func eventIntensityOptions() []eventOption {
	opts := []eventOption{}
	for i := 1; i <= 5; i++ {
		opts = append(opts, eventOption{Value: strconv.Itoa(i), Label: strconv.Itoa(i)})
	}
	return opts
}

func eventOrderOptions(lang string) []eventOption {
	return []eventOption{
		{Value: "", Label: T(lang, "events.order.default")},
		{Value: "data_desc", Label: T(lang, "events.order.data_desc")},
		{Value: "data_asc", Label: T(lang, "events.order.data_asc")},
		{Value: "intensitat_desc", Label: T(lang, "events.order.intensity_desc")},
		{Value: "recent", Label: T(lang, "events.order.recent")},
	}
}

func eventTimelineGroupOptions(lang string) []eventOption {
	return []eventOption{
		{Value: "any", Label: T(lang, "events.filters.timeline_group.any")},
		{Value: "decada", Label: T(lang, "events.filters.timeline_group.decade")},
	}
}

func eventTypeLabel(lang, typ string) string {
	if typ == "" {
		return ""
	}
	key := fmt.Sprintf("events.types.%s", typ)
	label := T(lang, key)
	if label == key {
		return typ
	}
	return label
}

func eventScopeTypeLabel(lang, scopeType string) string {
	if scopeType == "" {
		return ""
	}
	key := fmt.Sprintf("events.scope.%s", scopeType)
	label := T(lang, key)
	if label == key {
		return scopeType
	}
	return label
}

func eventImpactTypeLabel(lang, impactType string) string {
	if impactType == "" {
		return ""
	}
	key := fmt.Sprintf("events.impact.%s", impactType)
	label := T(lang, key)
	if label == key {
		return impactType
	}
	return label
}

func eventPrecisionLabel(lang, precision string) string {
	if precision == "" {
		return ""
	}
	key := fmt.Sprintf("events.precision.%s", precision)
	label := T(lang, key)
	if label == key {
		return precision
	}
	return label
}

func eventModerationLabel(lang, status string) string {
	if status == "" {
		return ""
	}
	key := fmt.Sprintf("events.status.%s", status)
	label := T(lang, key)
	if label == key {
		return status
	}
	return label
}

func eventDateLabel(e db.EventHistoric) string {
	start := eventDatePart(e.DataInici, e.Precisio, e.DataIniciAprox)
	end := eventDatePart(e.DataFi, e.Precisio, e.DataFiAprox)
	if start == "" && end == "" {
		return ""
	}
	if start == "" {
		return end
	}
	if end == "" || end == start {
		return start
	}
	return start + " - " + end
}

func eventDatePart(value, precision string, approx bool) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	label := value
	if parsed, err := time.Parse("2006-01-02", value); err == nil {
		switch precision {
		case "any":
			label = fmt.Sprintf("%d", parsed.Year())
		case "mes":
			label = parsed.Format("01/2006")
		case "decada":
			dec := (parsed.Year() / 10) * 10
			label = fmt.Sprintf("%ds", dec)
		default:
			label = parsed.Format("02/01/2006")
		}
	}
	if approx {
		return "~" + label
	}
	return label
}

func parseEventDateForTimeline(e db.EventHistoric) (time.Time, bool) {
	val := strings.TrimSpace(e.DataInici)
	if val == "" {
		val = strings.TrimSpace(e.DataFi)
	}
	if val == "" {
		return time.Time{}, false
	}
	parsed, err := time.Parse("2006-01-02", val)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}

func (a *App) ensureUniqueEventSlug(title string) string {
	base := slugifyEventTitle(title)
	slug := base
	for i := 2; i <= 50; i++ {
		if existing, err := a.DB.GetEventHistoricBySlug(slug); err == nil && existing != nil {
			slug = fmt.Sprintf("%s-%d", base, i)
			continue
		}
		break
	}
	return slug
}

func slugifyEventTitle(title string) string {
	val := strings.ToLower(strings.TrimSpace(title))
	val = stripDiacritics(val)
	var b strings.Builder
	lastDash := false
	for _, r := range val {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash && b.Len() > 0 {
			b.WriteByte('-')
			lastDash = true
		}
	}
	slug := strings.Trim(b.String(), "-")
	if slug == "" {
		return "event"
	}
	return slug
}

func (a *App) resolveEventScopeLabel(scopeType string, scopeID int, lang string) (string, string) {
	switch strings.TrimSpace(scopeType) {
	case "municipi":
		if mun, err := a.DB.GetMunicipi(scopeID); err == nil && mun != nil {
			return strings.TrimSpace(mun.Nom), ""
		}
	case "nivell_admin":
		if lvl, err := a.DB.GetNivell(scopeID); err == nil && lvl != nil {
			return strings.TrimSpace(lvl.NomNivell), ""
		}
	case "entitat_eclesiastica":
		if ent, err := a.DB.GetArquebisbat(scopeID); err == nil && ent != nil {
			return strings.TrimSpace(ent.Nom), ""
		}
	case "pais":
		paisos, _ := a.DB.ListPaisos()
		for _, pais := range paisos {
			if pais.ID == scopeID {
				return a.countryLabelFromISO(pais.CodiISO2, lang), ""
			}
		}
	}
	return "", ""
}

func buildMunicipiSuggestContext(row db.MunicipiSuggestRow, lang string) string {
	names := []string{}
	types := []string{}
	for i := 0; i < len(row.LevelNames); i++ {
		if row.LevelNames[i].Valid {
			names = append(names, strings.TrimSpace(row.LevelNames[i].String))
		} else {
			names = append(names, "")
		}
		if row.LevelTypes[i].Valid {
			labelKey := fmt.Sprintf("levels.types.%s", row.LevelTypes[i].String)
			label := T(lang, labelKey)
			if label == labelKey {
				label = strings.TrimSpace(row.LevelTypes[i].String)
			}
			types = append(types, label)
		} else {
			types = append(types, "")
		}
	}
	parts := []string{}
	if len(names) > 0 && names[0] != "" {
		parts = append(parts, names[0])
	}
	chain := []string{}
	for i := 1; i < len(names); i++ {
		if names[i] == "" {
			continue
		}
		label := names[i]
		if types[i] != "" {
			label = types[i] + ": " + names[i]
		}
		chain = append(chain, label)
	}
	if len(chain) > 0 {
		parts = append(parts, strings.Join(chain, " / "))
	}
	return strings.Join(parts, " - ")
}

func joinNonEmpty(a, b, sep string) string {
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	switch {
	case a != "" && b != "":
		return a + sep + b
	case a != "":
		return a
	case b != "":
		return b
	default:
		return ""
	}
}

func parseFormBool(val string) bool {
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "1", "true", "yes", "on", "si":
		return true
	default:
		return false
	}
}

func normalizeEventOrder(val string) string {
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "data_asc", "data_desc", "intensitat_desc", "recent":
		return strings.ToLower(strings.TrimSpace(val))
	default:
		return ""
	}
}

func buildEventTimelineGroups(items []eventListItem, group string, lang string) []eventTimelineGroup {
	group = strings.TrimSpace(group)
	if group == "" {
		group = "any"
	}
	order := []int{}
	groupMap := map[int][]eventListItem{}
	undated := []eventListItem{}
	for _, item := range items {
		if !item.HasDate {
			undated = append(undated, item)
			continue
		}
		key := item.DateYear
		if group == "decada" {
			key = item.DateDecade
		}
		if key == 0 {
			undated = append(undated, item)
			continue
		}
		if _, ok := groupMap[key]; !ok {
			order = append(order, key)
		}
		groupMap[key] = append(groupMap[key], item)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(order)))
	out := []eventTimelineGroup{}
	for _, key := range order {
		label := fmt.Sprintf("%d", key)
		if group == "decada" {
			label = fmt.Sprintf("%ds", key)
		}
		out = append(out, eventTimelineGroup{
			Label: label,
			Items: groupMap[key],
		})
	}
	if len(undated) > 0 {
		out = append(out, eventTimelineGroup{
			Label: T(lang, "events.timeline.undated"),
			Items: undated,
		})
	}
	return out
}

func (a *App) populateEventListIntensity(items []eventListItem, scopeType string, scopeID int) []eventListItem {
	if a == nil || a.DB == nil || len(items) == 0 {
		return items
	}
	scopeType = strings.TrimSpace(scopeType)
	for i := range items {
		maxIntensitat := 0
		impacts, err := a.DB.ListEventImpacts(items[i].ID)
		if err != nil {
			continue
		}
		for _, impact := range impacts {
			if scopeType != "" && scopeID > 0 {
				if impact.ScopeType != scopeType || impact.ScopeID != scopeID {
					continue
				}
			}
			if impact.Intensitat > maxIntensitat {
				maxIntensitat = impact.Intensitat
			}
		}
		items[i].Intensity = maxIntensitat
	}
	return items
}

func maxLenStrings(values ...[]string) int {
	max := 0
	for _, v := range values {
		if len(v) > max {
			max = len(v)
		}
	}
	return max
}
