package core

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type espaiPersonaLinkSuggestion struct {
	ID      int    `json:"id"`
	Nom     string `json:"nom"`
	Context string `json:"context"`
	URL     string `json:"url"`
}

func (a *App) EspaiPersonaLinkSearchAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if len(query) < 2 {
		writeJSON(w, map[string]interface{}{"items": []espaiPersonaLinkSuggestion{}})
		return
	}
	limit := 10
	if val := strings.TrimSpace(r.URL.Query().Get("limit")); val != "" {
		if v, err := strconv.Atoi(val); err == nil && v > 0 && v <= 25 {
			limit = v
		}
	}
	queryTokens := normalizeQueryTokens(query)
	filter := db.SearchQueryFilter{
		Query:         query,
		QueryNorm:     normalizeSearchText(query),
		QueryTokens:   queryTokens,
		QueryPhonetic: strings.Join(phoneticTokens(queryTokens), " "),
		Entity:        "persona",
		Page:          1,
		PageSize:      limit,
	}
	if len(queryTokens) > 0 {
		canon, variant := a.expandCognomTokens(queryTokens)
		filter.CanonTokens = canon
		filter.VariantTokens = variant
	}
	results, _, _, err := a.DB.SearchDocs(filter)
	if err != nil {
		http.Error(w, "No s'han pogut cercar persones", http.StatusInternalServerError)
		return
	}
	lang := ResolveLang(r)
	items := make([]espaiPersonaLinkSuggestion, 0, len(results))
	seen := map[int]struct{}{}
	for _, row := range results {
		if _, ok := seen[row.EntityID]; ok {
			continue
		}
		seen[row.EntityID] = struct{}{}
		persona, err := a.DB.GetPersona(row.EntityID)
		if err != nil || persona == nil {
			continue
		}
		status := strings.TrimSpace(persona.ModeracioEstat)
		if status != "" && status != "publicat" {
			continue
		}
		name := personaDisplayName(persona)
		items = append(items, espaiPersonaLinkSuggestion{
			ID:      persona.ID,
			Nom:     name,
			Context: a.buildPersonaLinkContext(persona.ID, lang),
			URL:     "/persones/" + strconv.Itoa(persona.ID),
		})
	}
	writeJSON(w, map[string]interface{}{"items": items})
}

func (a *App) buildPersonaLinkContext(personaID int, lang string) string {
	rows, err := a.DB.ListRegistresByPersona(personaID, "")
	if err != nil || len(rows) == 0 {
		return ""
	}
	selected := rows[0]
	for _, row := range rows {
		tipus := normalizeRole(row.TipusActe)
		if tipus == "naixement" || tipus == "baptisme" {
			selected = row
			break
		}
	}
	persones, _ := a.DB.ListTranscripcioPersones(selected.RegistreID)
	roleMap := personRoleMap(persones)
	appendUnique := func(list []string, name string) []string {
		name = strings.TrimSpace(name)
		if name == "" {
			return list
		}
		for _, existing := range list {
			if existing == name {
				return list
			}
		}
		return append(list, name)
	}
	father := firstNameByRoles(roleMap, []string{"pare", "paire", "parent", "pare_nuvi", "pare_novia", "parenuvi", "parenovia"})
	mother := firstNameByRoles(roleMap, []string{"mare", "maire", "mare_nuvi", "mare_novia", "marenuvi", "marenovia"})
	parentNames := []string{}
	parentNames = appendUnique(parentNames, father)
	parentNames = appendUnique(parentNames, mother)
	grandparentNames := []string{}
	grandparentSeen := map[string]bool{}
	appendGrandparents := func(names []string) {
		for _, name := range names {
			name = strings.TrimSpace(name)
			if name == "" || grandparentSeen[name] {
				continue
			}
			grandparentSeen[name] = true
			grandparentNames = append(grandparentNames, name)
		}
	}
	appendGrandparents(namesByRoles(roleMap, []string{"avi_patern", "avia_paterna"}))
	appendGrandparents(namesByRoles(roleMap, []string{"avi_matern", "avia_materna"}))
	if len(grandparentNames) == 0 {
		appendGrandparents(namesByRoles(roleMap, []string{"avi", "avia"}))
	}

	eventDate := ""
	attrs, _ := a.DB.ListTranscripcioAtributs(selected.RegistreID)
	tipusNorm := normalizeRole(selected.TipusActe)
	switch tipusNorm {
	case "baptisme":
		eventDate = attrValueByKeysRaw(attrs,
			"data_bateig", "databateig",
			"data_baptisme", "databaptisme",
			"bateig", "baptisme", "databapt", "data_bapt",
		)
	case "naixement":
		eventDate = attrValueByKeysRaw(attrs,
			"data_naixement", "datanaixement", "naixement",
			"data_naixament", "datanaixament", "naixament",
			"nascut", "data_nascut", "datanascut",
		)
	case "defuncio", "obit":
		eventDate = attrValueByKeysRaw(attrs,
			"data_defuncio", "datadefuncio", "defuncio",
			"data_obit", "obit",
		)
	case "matrimoni":
		eventDate = attrValueByKeysRaw(attrs,
			"data_matrimoni", "datamatrimoni", "matrimoni",
			"data_casament", "casament",
		)
	case "confirmacio":
		eventDate = attrValueByKeysRaw(attrs,
			"data_confirmacio", "dataconfirmacio", "confirmacio",
		)
	}
	if eventDate != "" {
		eventDate = formatDateDisplay(eventDate)
	}
	if eventDate == "" {
		raw := strings.TrimSpace(selected.DataActeText)
		if formatted := formatDateDisplay(raw); formatted != "" {
			eventDate = formatted
		} else {
			eventDate = raw
		}
	}

	parentLabel := T(lang, "records.detail.parents")
	if parentLabel == "records.detail.parents" {
		parentLabel = "Pares"
	}
	grandparentsLabel := T(lang, "records.detail.grandparents")
	if grandparentsLabel == "records.detail.grandparents" {
		grandparentsLabel = "Avis"
	}
	tipusLabel := strings.TrimSpace(selected.TipusActe)
	if tipusNorm != "" {
		if label := T(lang, "records.type."+tipusNorm); label != "records.type."+tipusNorm {
			tipusLabel = label
		}
	}
	eventLabel := strings.TrimSpace(tipusLabel)
	if eventLabel == "" {
		eventLabel = T(lang, "records.field.data_acte")
	}
	if eventLabel == "records.field.data_acte" {
		eventLabel = "Data"
	}

	contextParts := []string{}
	if len(parentNames) > 0 {
		contextParts = append(contextParts, fmt.Sprintf("%s: %s", parentLabel, strings.Join(parentNames, " / ")))
	}
	if len(grandparentNames) > 0 {
		contextParts = append(contextParts, fmt.Sprintf("%s: %s", grandparentsLabel, strings.Join(grandparentNames, " / ")))
	}
	if strings.TrimSpace(eventDate) != "" {
		contextParts = append(contextParts, fmt.Sprintf("%s: %s", eventLabel, eventDate))
	}
	return strings.TrimSpace(strings.Join(contextParts, " · "))
}

func (a *App) EspaiPersonaLinkCreate(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, T(ResolveLang(r), "error.csrf"), http.StatusBadRequest)
		return
	}
	redirectBase := espaiRedirectTarget(r, "/espai/persones-importades")
	lang := ResolveLang(r)

	personaID := parseFormInt(r.FormValue("persona_id"))
	targetID := parseFormInt(r.FormValue("target_id"))
	if personaID == 0 || targetID == 0 {
		http.Redirect(w, r, withQueryParams(redirectBase, map[string]string{"error": T(lang, "space.people_data.link.error")}), http.StatusSeeOther)
		return
	}
	persona, err := a.DB.GetEspaiPersona(personaID)
	if err != nil || persona == nil || persona.OwnerUserID != user.ID {
		http.Redirect(w, r, withQueryParams(redirectBase, map[string]string{"error": T(lang, "space.people_data.link.error")}), http.StatusSeeOther)
		return
	}
	target, err := a.DB.GetPersona(targetID)
	if err != nil || target == nil {
		http.Redirect(w, r, withQueryParams(redirectBase, map[string]string{"error": T(lang, "space.people_data.link.error")}), http.StatusSeeOther)
		return
	}
	if status := strings.TrimSpace(target.ModeracioEstat); status != "" && status != "publicat" {
		http.Redirect(w, r, withQueryParams(redirectBase, map[string]string{"error": T(lang, "space.people_data.link.error")}), http.StatusSeeOther)
		return
	}

	matched := false
	if links, err := a.DB.ListEspaiCoincidenciesByOwner(user.ID); err == nil {
		for _, link := range links {
			if link.PersonaID != persona.ID || strings.TrimSpace(link.TargetType) != "persona" {
				continue
			}
			if link.TargetID == targetID {
				matched = true
				if strings.TrimSpace(link.Status) != "accepted" {
					_ = a.DB.UpdateEspaiCoincidenciaStatus(link.ID, "accepted")
				}
				continue
			}
			if strings.TrimSpace(link.Status) == "accepted" {
				_ = a.DB.UpdateEspaiCoincidenciaStatus(link.ID, "rejected")
			}
		}
	}
	if !matched {
		_, err := a.DB.CreateEspaiCoincidencia(&db.EspaiCoincidencia{
			OwnerUserID: user.ID,
			ArbreID:     persona.ArbreID,
			PersonaID:   persona.ID,
			TargetType:  "persona",
			TargetID:    targetID,
			Status:      "accepted",
		})
		if err != nil {
			http.Redirect(w, r, withQueryParams(redirectBase, map[string]string{"error": err.Error()}), http.StatusSeeOther)
			return
		}
	}
	http.Redirect(w, r, withQueryParams(redirectBase, map[string]string{"notice": T(lang, "space.people_data.link.success")}), http.StatusSeeOther)
}
