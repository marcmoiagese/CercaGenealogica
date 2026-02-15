package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type moderacioItem struct {
	ID         int
	Type       string
	Nom        string
	Context    string
	ContextURL string
	Autor      string
	AutorURL   string
	AutorID    int
	Created    string
	CreatedAt  time.Time
	Motiu      string
	EditURL    string
	Status     string
}

type moderacioTypeCount struct {
	Type  string `json:"type"`
	Total int    `json:"total"`
}

type moderacioSummary struct {
	Total        int                  `json:"total"`
	SLA0_24h     int                  `json:"sla_0_24h"`
	SLA1_3d      int                  `json:"sla_1_3d"`
	SLA3Plus     int                  `json:"sla_3d_plus"`
	TopType      string               `json:"top_type"`
	TopTypeTotal int                  `json:"top_type_total"`
	ByType       []moderacioTypeCount `json:"by_type"`
}

type moderacioFilters struct {
	Type      string
	Status    string
	AgeBucket string
	UserID    int
	UserQuery string
}

const (
	moderacioAge0_24h = "0_24h"
	moderacioAge1_3d  = "1_3d"
	moderacioAge3Plus = "3d_plus"
)

func moderacioAgeBucket(createdAt time.Time, now time.Time) string {
	if createdAt.IsZero() {
		return ""
	}
	age := now.Sub(createdAt)
	if age <= 24*time.Hour {
		return moderacioAge0_24h
	}
	if age <= 72*time.Hour {
		return moderacioAge1_3d
	}
	return moderacioAge3Plus
}

func (a *App) buildModeracioItems(lang string, page, perPage int, user *db.User, canModerateAll bool, filters moderacioFilters) ([]moderacioItem, int, moderacioSummary) {
	var items []moderacioItem
	userCache := map[int]*db.User{}
	autorFromID := func(id sql.NullInt64) (string, string, int) {
		if !id.Valid {
			return "—", "", 0
		}
		uid := int(id.Int64)
		if cached, ok := userCache[uid]; ok {
			username := strings.TrimSpace(cached.Usuari)
			if username == "" {
				full := strings.TrimSpace(strings.TrimSpace(cached.Name) + " " + strings.TrimSpace(cached.Surname))
				if full != "" {
					username = full
				}
			}
			if username == "" {
				username = "—"
			}
			return username, "/u/" + strconv.Itoa(cached.ID), cached.ID
		}
		u, err := a.DB.GetUserByID(uid)
		if err != nil || u == nil {
			return "—", "", 0
		}
		userCache[uid] = u
		username := strings.TrimSpace(u.Usuari)
		if username == "" {
			full := strings.TrimSpace(strings.TrimSpace(u.Name) + " " + strings.TrimSpace(u.Surname))
			if full != "" {
				username = full
			}
		}
		if username == "" {
			username = "—"
		}
		return username, "/u/" + strconv.Itoa(u.ID), u.ID
	}

	statusFilter := strings.TrimSpace(filters.Status)
	statusAll := statusFilter == "" || statusFilter == "all"
	typeFilter := strings.TrimSpace(filters.Type)
	ageFilter := strings.TrimSpace(filters.AgeBucket)
	userQuery := strings.TrimSpace(filters.UserQuery)
	userID := filters.UserID
	now := time.Now()
	typeAllowed := func(objType string) bool {
		return typeFilter == "" || typeFilter == "all" || typeFilter == objType
	}
	statusAllowed := func(status string) bool {
		if statusAll {
			return true
		}
		return strings.TrimSpace(status) == statusFilter
	}
	userAllowed := func(item moderacioItem) bool {
		if userID > 0 {
			return item.AutorID == userID
		}
		if userQuery == "" {
			return true
		}
		return strings.Contains(strings.ToLower(item.Autor), strings.ToLower(strings.TrimPrefix(userQuery, "@")))
	}
	ageAllowed := func(createdAt time.Time) bool {
		if ageFilter == "" {
			return true
		}
		return moderacioAgeBucket(createdAt, now) == ageFilter
	}
	matchesFilters := func(item moderacioItem) bool {
		if !typeAllowed(item.Type) {
			return false
		}
		if !statusAllowed(item.Status) {
			return false
		}
		if !userAllowed(item) {
			return false
		}
		if !ageAllowed(item.CreatedAt) {
			return false
		}
		return true
	}

	canModerateHistoriaAny := canModerateAll
	if !canModerateAll && user != nil {
		canModerateHistoriaAny = a.hasAnyPermissionKey(user.ID, permKeyTerritoriMunicipisHistoriaModerate)
	}
	canModerateHistoriaItem := func(municipiID int) bool {
		if canModerateAll {
			return true
		}
		if user == nil || municipiID <= 0 {
			return false
		}
		target := a.resolveMunicipiTarget(municipiID)
		return a.HasPermission(user.ID, permKeyTerritoriMunicipisHistoriaModerate, target)
	}
	canModerateAnecdotesAny := canModerateAll
	if !canModerateAll && user != nil {
		canModerateAnecdotesAny = a.hasAnyPermissionKey(user.ID, permKeyTerritoriMunicipisAnecdotesModerate)
	}
	canModerateAnecdoteItem := func(municipiID int) bool {
		if canModerateAll {
			return true
		}
		if user == nil || municipiID <= 0 {
			return false
		}
		target := a.resolveMunicipiTarget(municipiID)
		return a.HasPermission(user.ID, permKeyTerritoriMunicipisAnecdotesModerate, target)
	}

	persones := []db.Persona{}
	arxius := []db.ArxiuWithCount{}
	llibres := []db.LlibreRow{}
	nivells := []db.NivellAdministratiu{}
	municipis := []db.MunicipiRow{}
	ents := []db.ArquebisbatRow{}
	variants := []db.CognomVariant{}
	referencies := []db.CognomReferencia{}
	mergeSuggestions := []db.CognomRedirectSuggestion{}
	events := []db.EventHistoric{}
	pendingChanges := []db.TranscripcioRawChange{}
	wikiChanges := []db.WikiChange{}
	if canModerateAll {
		if typeAllowed("persona") {
			status := ""
			if !statusAll {
				status = statusFilter
			}
			if pendents, err := a.DB.ListPersones(db.PersonaFilter{Estat: status}); err == nil {
				persones = pendents
			}
		}
		if typeAllowed("arxiu") {
			status := ""
			if !statusAll {
				status = statusFilter
			}
			if rows, err := a.DB.ListArxius(db.ArxiuFilter{Status: status}); err == nil {
				arxius = rows
			}
		}
		if typeAllowed("llibre") {
			status := ""
			if !statusAll {
				status = statusFilter
			}
			if rows, err := a.DB.ListLlibres(db.LlibreFilter{Status: status}); err == nil {
				llibres = rows
			}
		}
		if typeAllowed("nivell") {
			status := ""
			if !statusAll {
				status = statusFilter
			}
			if rows, err := a.DB.ListNivells(db.NivellAdminFilter{Status: status}); err == nil {
				nivells = rows
			}
		}
		if typeAllowed("municipi") {
			status := ""
			if !statusAll {
				status = statusFilter
			}
			if rows, err := a.DB.ListMunicipis(db.MunicipiFilter{Status: status}); err == nil {
				municipis = rows
			}
		}
		if typeAllowed("eclesiastic") {
			status := ""
			if !statusAll {
				status = statusFilter
			}
			if rows, err := a.DB.ListArquebisbats(db.ArquebisbatFilter{Status: status}); err == nil {
				ents = rows
			}
		}
		if typeAllowed("cognom_variant") {
			status := ""
			if !statusAll {
				status = statusFilter
			}
			if rows, err := a.DB.ListCognomVariants(db.CognomVariantFilter{Status: status}); err == nil {
				variants = rows
			}
		}
		if typeAllowed("cognom_referencia") {
			status := ""
			if !statusAll {
				status = statusFilter
			}
			if rows, err := a.DB.ListCognomReferencies(db.CognomReferenciaFilter{Status: status}); err == nil {
				referencies = rows
			}
		}
		if typeAllowed("cognom_merge") {
			status := ""
			if !statusAll {
				status = statusFilter
			}
			if rows, err := a.DB.ListCognomRedirectSuggestions(db.CognomRedirectSuggestionFilter{Status: status}); err == nil {
				mergeSuggestions = rows
			}
		}
		if typeAllowed("event_historic") {
			status := ""
			if !statusAll {
				status = statusFilter
			}
			if rows, err := a.DB.ListEventsHistoric(db.EventHistoricFilter{Status: status}); err == nil {
				events = rows
			}
		}
		if typeAllowed("registre_canvi") && (statusAll || statusFilter == "pendent") {
			if rows, err := a.DB.ListTranscripcioRawChangesPending(); err == nil {
				pendingChanges = rows
			}
		}
		if (typeAllowed("municipi_canvi") || typeAllowed("arxiu_canvi") || typeAllowed("llibre_canvi") || typeAllowed("persona_canvi") || typeAllowed("cognom_canvi") || typeAllowed("event_historic_canvi") || typeAllowed("wiki_canvi")) && (statusAll || statusFilter == "pendent") {
			if items, err := a.DB.ListWikiPending(0); err == nil {
				for _, item := range items {
					if !isValidWikiObjectType(item.ObjectType) {
						continue
					}
					change, err := a.DB.GetWikiChange(item.ChangeID)
					if err != nil || change == nil {
						continue
					}
					if change.ModeracioEstat != "pendent" {
						_ = a.DB.DequeueWikiPending(change.ID)
						continue
					}
					wikiChanges = append(wikiChanges, *change)
				}
			}
		}
	}
	historiaGeneral := []db.MunicipiHistoriaGeneralVersion{}
	historiaFets := []db.MunicipiHistoriaFetVersion{}
	if canModerateHistoriaAny && (statusAll || statusFilter == "pendent") && typeAllowed("municipi_historia_general") {
		if rows, _, err := a.DB.ListPendingMunicipiHistoriaGeneralVersions(0, 0); err == nil {
			for _, row := range rows {
				if canModerateHistoriaItem(row.MunicipiID) {
					historiaGeneral = append(historiaGeneral, row)
				}
			}
		}
	}
	if canModerateHistoriaAny && (statusAll || statusFilter == "pendent") && typeAllowed("municipi_historia_fet") {
		if rows, _, err := a.DB.ListPendingMunicipiHistoriaFetVersions(0, 0); err == nil {
			for _, row := range rows {
				if canModerateHistoriaItem(row.MunicipiID) {
					historiaFets = append(historiaFets, row)
				}
			}
		}
	}
	anecdotes := []db.MunicipiAnecdotariVersion{}
	if canModerateAnecdotesAny && (statusAll || statusFilter == "pendent") && typeAllowed("municipi_anecdota_version") {
		if rows, _, err := a.DB.ListPendingMunicipiAnecdotariVersions(0, 0); err == nil {
			for _, row := range rows {
				if canModerateAnecdoteItem(row.MunicipiID) {
					anecdotes = append(anecdotes, row)
				}
			}
		}
	}

	start := (page - 1) * perPage
	if start < 0 {
		start = 0
	}
	end := start + perPage
	index := 0
	typeCounts := map[string]int{}
	summary := moderacioSummary{}
	appendIfVisible := func(item moderacioItem) {
		if !matchesFilters(item) {
			return
		}
		summary.Total++
		typeCounts[item.Type]++
		switch moderacioAgeBucket(item.CreatedAt, now) {
		case moderacioAge0_24h:
			summary.SLA0_24h++
		case moderacioAge1_3d:
			summary.SLA1_3d++
		case moderacioAge3Plus:
			summary.SLA3Plus++
		}
		if index >= start && index < end {
			items = append(items, item)
		}
		index++
	}

	if canModerateAll {
		if typeAllowed("persona") {
			for _, p := range persones {
				created := ""
				var createdAt time.Time
				if p.CreatedAt.Valid {
					created = p.CreatedAt.Time.Format("2006-01-02 15:04")
					createdAt = p.CreatedAt.Time
				}
			context := strings.TrimSpace(fmt.Sprintf("%s %s", p.Llibre, p.Pagina))
			if context == "" {
				context = p.Municipi
			}
			autorNom, autorURL, autorID := autorFromID(p.CreatedBy)
			appendIfVisible(moderacioItem{
				ID:        p.ID,
				Type:      "persona",
				Nom:       strings.TrimSpace(strings.Join([]string{p.Nom, p.Cognom1, p.Cognom2}, " ")),
				Context:   context,
				Autor:     autorNom,
				AutorURL:  autorURL,
				AutorID:   autorID,
				Created:   created,
				CreatedAt: createdAt,
				Motiu:     p.ModeracioMotiu,
				EditURL:   fmt.Sprintf("/persones/%d?return_to=/moderacio", p.ID),
				Status:    p.ModeracioEstat,
			})
			}
		}

		if typeAllowed("arxiu") {
			for _, arow := range arxius {
				created := ""
				var createdAt time.Time
				if arow.CreatedAt.Valid {
					created = arow.CreatedAt.Time.Format("2006-01-02 15:04")
					createdAt = arow.CreatedAt.Time
				}
				autorNom, autorURL, autorID := autorFromID(arow.CreatedBy)
				appendIfVisible(moderacioItem{
					ID:        arow.ID,
					Type:      "arxiu",
					Nom:       arow.Nom,
					Context:   arow.Tipus,
					Autor:     autorNom,
					AutorURL:  autorURL,
					AutorID:   autorID,
					Created:   created,
					CreatedAt: createdAt,
					Motiu:     arow.ModeracioMotiu,
					EditURL:   fmt.Sprintf("/documentals/arxius/%d/edit?return_to=/moderacio", arow.ID),
					Status:    arow.ModeracioEstat,
				})
			}
		}

		if typeAllowed("llibre") {
			for _, l := range llibres {
				created := ""
				var createdAt time.Time
				if l.CreatedAt.Valid {
					created = l.CreatedAt.Time.Format("2006-01-02 15:04")
					createdAt = l.CreatedAt.Time
				}
				autorNom, autorURL, autorID := autorFromID(l.CreatedBy)
				appendIfVisible(moderacioItem{
					ID:        l.ID,
					Type:      "llibre",
					Nom:       l.Titol,
					Context:   l.NomEsglesia,
					Autor:     autorNom,
					AutorURL:  autorURL,
					AutorID:   autorID,
					Created:   created,
					CreatedAt: createdAt,
					Motiu:     l.ModeracioMotiu,
					EditURL:   fmt.Sprintf("/documentals/llibres/%d/edit?return_to=/moderacio", l.ID),
					Status:    l.ModeracioEstat,
				})
			}
		}

		if typeAllowed("nivell") {
			for _, n := range nivells {
				created := ""
				var createdAt time.Time
				if n.CreatedAt.Valid {
					created = n.CreatedAt.Time.Format("2006-01-02 15:04")
					createdAt = n.CreatedAt.Time
				}
				autorNom, autorURL, autorID := autorFromID(n.CreatedBy)
				appendIfVisible(moderacioItem{
					ID:        n.ID,
					Type:      "nivell",
					Nom:       n.NomNivell,
					Context:   fmt.Sprintf("Nivell %d", n.Nivel),
					Autor:     autorNom,
					AutorURL:  autorURL,
					AutorID:   autorID,
					Created:   created,
					CreatedAt: createdAt,
					Motiu:     n.ModeracioMotiu,
					EditURL:   fmt.Sprintf("/territori/nivells/%d/edit?return_to=/moderacio", n.ID),
					Status:    n.ModeracioEstat,
				})
			}
		}

		if typeAllowed("municipi") {
			for _, mrow := range municipis {
				created := ""
				var createdAt time.Time
				if mrow.CreatedAt.Valid {
					created = mrow.CreatedAt.Time.Format("2006-01-02 15:04")
					createdAt = mrow.CreatedAt.Time
				}
				autorNom, autorURL, autorID := autorFromID(mrow.CreatedBy)
				motiu := ""
				ctx := strings.TrimSpace(strings.Join([]string{mrow.PaisNom.String, mrow.ProvNom.String, mrow.Comarca.String}, " / "))
				appendIfVisible(moderacioItem{
					ID:        mrow.ID,
					Type:      "municipi",
					Nom:       mrow.Nom,
					Context:   ctx,
					Autor:     autorNom,
					AutorURL:  autorURL,
					AutorID:   autorID,
					Created:   created,
					CreatedAt: createdAt,
					Motiu:     motiu,
					EditURL:   fmt.Sprintf("/territori/municipis/%d/edit?return_to=/moderacio", mrow.ID),
					Status:    mrow.ModeracioEstat,
				})
			}
		}

		if typeAllowed("eclesiastic") {
			for _, row := range ents {
				created := ""
				var createdAt time.Time
				if row.CreatedAt.Valid {
					created = row.CreatedAt.Time.Format("2006-01-02 15:04")
					createdAt = row.CreatedAt.Time
				}
				autorNom, autorURL, autorID := autorFromID(row.CreatedBy)
				motiu := ""
				appendIfVisible(moderacioItem{
					ID:        row.ID,
					Type:      "eclesiastic",
					Nom:       row.Nom,
					Context:   row.TipusEntitat,
					Autor:     autorNom,
					AutorURL:  autorURL,
					AutorID:   autorID,
					Created:   created,
					CreatedAt: createdAt,
					Motiu:     motiu,
					EditURL:   fmt.Sprintf("/territori/eclesiastic/%d/edit?return_to=/moderacio", row.ID),
					Status:    row.ModeracioEstat,
				})
			}
		}
	}

	for _, row := range historiaGeneral {
		created := ""
		var createdAt time.Time
		if row.CreatedAt.Valid {
			created = row.CreatedAt.Time.Format("2006-01-02 15:04")
			createdAt = row.CreatedAt.Time
		}
		autorNom, autorURL, autorID := autorFromID(row.CreatedBy)
		nomParts := []string{T(lang, "municipi.history.general")}
		if strings.TrimSpace(row.MunicipiNom) != "" {
			nomParts = append(nomParts, strings.TrimSpace(row.MunicipiNom))
		}
		appendIfVisible(moderacioItem{
			ID:         row.ID,
			Type:       "municipi_historia_general",
			Nom:        strings.Join(nomParts, " · "),
			Context:    strings.TrimSpace(row.MunicipiNom),
			ContextURL: fmt.Sprintf("/territori/municipis/%d", row.MunicipiID),
			Autor:      autorNom,
			AutorURL:   autorURL,
			AutorID:    autorID,
			Created:    created,
			CreatedAt:  createdAt,
			Motiu:      row.ModerationNotes,
			EditURL:    fmt.Sprintf("/moderacio/municipis/historia/general/%d", row.ID),
			Status:     row.Status,
		})
	}

	for _, row := range historiaFets {
		created := ""
		var createdAt time.Time
		if row.CreatedAt.Valid {
			created = row.CreatedAt.Time.Format("2006-01-02 15:04")
			createdAt = row.CreatedAt.Time
		}
		autorNom, autorURL, autorID := autorFromID(row.CreatedBy)
		dateLabel := strings.TrimSpace(historiaDateLabel(row))
		nameParts := []string{}
		if dateLabel != "" {
			nameParts = append(nameParts, dateLabel)
		}
		if strings.TrimSpace(row.Titol) != "" {
			nameParts = append(nameParts, strings.TrimSpace(row.Titol))
		}
		if strings.TrimSpace(row.MunicipiNom) != "" {
			nameParts = append(nameParts, strings.TrimSpace(row.MunicipiNom))
		}
		appendIfVisible(moderacioItem{
			ID:         row.ID,
			Type:       "municipi_historia_fet",
			Nom:        strings.Join(nameParts, " · "),
			Context:    strings.TrimSpace(row.MunicipiNom),
			ContextURL: fmt.Sprintf("/territori/municipis/%d", row.MunicipiID),
			Autor:      autorNom,
			AutorURL:   autorURL,
			AutorID:    autorID,
			Created:    created,
			CreatedAt:  createdAt,
			Motiu:      row.ModerationNotes,
			EditURL:    fmt.Sprintf("/moderacio/municipis/historia/fets/%d", row.ID),
			Status:     row.Status,
		})
	}

	for _, row := range anecdotes {
		created := ""
		var createdAt time.Time
		if row.CreatedAt.Valid {
			created = row.CreatedAt.Time.Format("2006-01-02 15:04")
			createdAt = row.CreatedAt.Time
		}
		autorNom, autorURL, autorID := autorFromID(row.CreatedBy)
		tagLabel := strings.TrimSpace(row.Tag)
		if strings.TrimSpace(row.Tag) != "" {
			labelKey := "municipi.anecdotes.tags." + strings.TrimSpace(row.Tag)
			label := strings.TrimSpace(T(lang, labelKey))
			if label != "" && label != labelKey {
				tagLabel = label
			}
		}
		contextParts := []string{}
		if strings.TrimSpace(row.MunicipiNom) != "" {
			contextParts = append(contextParts, strings.TrimSpace(row.MunicipiNom))
		}
		if strings.TrimSpace(tagLabel) != "" {
			contextParts = append(contextParts, strings.TrimSpace(tagLabel))
		}
		name := strings.TrimSpace(row.Titol)
		if name == "" {
			name = T(lang, "municipi.anecdotes.title")
		}
		appendIfVisible(moderacioItem{
			ID:         row.ID,
			Type:       "municipi_anecdota_version",
			Nom:        name,
			Context:    strings.Join(contextParts, " · "),
			ContextURL: fmt.Sprintf("/territori/municipis/%d", row.MunicipiID),
			Autor:      autorNom,
			AutorURL:   autorURL,
			AutorID:    autorID,
			Created:    created,
			CreatedAt:  createdAt,
			Motiu:      row.ModerationNotes,
			EditURL:    fmt.Sprintf("/territori/municipis/%d/anecdotes/%d?version_id=%d", row.MunicipiID, row.ItemID, row.ID),
			Status:     row.Status,
		})
	}

	if canModerateAll {
		cognomCache := map[int]string{}
		for _, v := range variants {
			created := ""
			var createdAt time.Time
			if v.CreatedAt.Valid {
				created = v.CreatedAt.Time.Format("2006-01-02 15:04")
				createdAt = v.CreatedAt.Time
			}
			autorNom, autorURL, autorID := autorFromID(v.CreatedBy)
			forma := cognomCache[v.CognomID]
			if forma == "" {
				if c, err := a.DB.GetCognom(v.CognomID); err == nil && c != nil {
					forma = c.Forma
					cognomCache[v.CognomID] = forma
				}
			}
			context := strings.TrimSpace(fmt.Sprintf("%s → %s", forma, v.Variant))
			if context == "" {
				context = v.Variant
			}
			appendIfVisible(moderacioItem{
				ID:        v.ID,
				Type:      "cognom_variant",
				Nom:       v.Variant,
				Context:   context,
				Autor:     autorNom,
				AutorURL:  autorURL,
				AutorID:   autorID,
				Created:   created,
				CreatedAt: createdAt,
				Motiu:     v.ModeracioMotiu,
				EditURL:   fmt.Sprintf("/cognoms/%d", v.CognomID),
				Status:    v.ModeracioEstat,
			})
		}
		for _, ref := range referencies {
			created := ""
			var createdAt time.Time
			if ref.CreatedAt.Valid {
				created = ref.CreatedAt.Time.Format("2006-01-02 15:04")
				createdAt = ref.CreatedAt.Time
			}
			autorNom, autorURL, autorID := autorFromID(ref.CreatedBy)
			forma := cognomCache[ref.CognomID]
			if forma == "" {
				if c, err := a.DB.GetCognom(ref.CognomID); err == nil && c != nil {
					forma = c.Forma
					cognomCache[ref.CognomID] = forma
				}
			}
			context := strings.TrimSpace(forma)
			if context == "" {
				context = fmt.Sprintf("Cognom %d", ref.CognomID)
			}
			name := strings.TrimSpace(ref.Titol)
			if name == "" {
				name = strings.TrimSpace(ref.URL)
			}
			if name == "" {
				name = strings.TrimSpace(ref.Kind)
			}
			appendIfVisible(moderacioItem{
				ID:         ref.ID,
				Type:       "cognom_referencia",
				Nom:        name,
				Context:    context,
				ContextURL: fmt.Sprintf("/cognoms/%d", ref.CognomID),
				Autor:      autorNom,
				AutorURL:   autorURL,
				AutorID:    autorID,
				Created:    created,
				CreatedAt:  createdAt,
				Motiu:      ref.ModeracioMotiu,
				EditURL:    fmt.Sprintf("/cognoms/%d", ref.CognomID),
				Status:     ref.ModeracioEstat,
			})
		}
		for _, merge := range mergeSuggestions {
			created := ""
			var createdAt time.Time
			if merge.CreatedAt.Valid {
				created = merge.CreatedAt.Time.Format("2006-01-02 15:04")
				createdAt = merge.CreatedAt.Time
			}
			autorNom, autorURL, autorID := autorFromID(merge.CreatedBy)
			fromLabel := cognomCache[merge.FromCognomID]
			if fromLabel == "" {
				if c, err := a.DB.GetCognom(merge.FromCognomID); err == nil && c != nil {
					fromLabel = c.Forma
					cognomCache[merge.FromCognomID] = fromLabel
				}
			}
			toLabel := cognomCache[merge.ToCognomID]
			if toLabel == "" {
				if c, err := a.DB.GetCognom(merge.ToCognomID); err == nil && c != nil {
					toLabel = c.Forma
					cognomCache[merge.ToCognomID] = toLabel
				}
			}
			context := strings.TrimSpace(fmt.Sprintf("%s → %s", fromLabel, toLabel))
			if context == "" {
				context = fmt.Sprintf("Cognom %d → %d", merge.FromCognomID, merge.ToCognomID)
			}
			appendIfVisible(moderacioItem{
				ID:         merge.ID,
				Type:       "cognom_merge",
				Nom:        context,
				Context:    context,
				ContextURL: fmt.Sprintf("/cognoms/%d", merge.ToCognomID),
				Autor:      autorNom,
				AutorURL:   autorURL,
				AutorID:    autorID,
				Created:    created,
				CreatedAt:  createdAt,
				Motiu:      merge.Reason,
				EditURL:    fmt.Sprintf("/admin/cognoms/merge"),
				Status:     merge.ModeracioEstat,
			})
		}
	}

	if canModerateAll {
		for _, ev := range events {
			created := ""
			var createdAt time.Time
			if ev.CreatedAt.Valid {
				created = ev.CreatedAt.Time.Format("2006-01-02 15:04")
				createdAt = ev.CreatedAt.Time
			}
			autorNom, autorURL, autorID := autorFromID(ev.CreatedBy)
			contextParts := []string{}
			if label := eventTypeLabel(lang, ev.Tipus); label != "" {
				contextParts = append(contextParts, label)
			}
			if dateLabel := eventDateLabel(ev); dateLabel != "" {
				contextParts = append(contextParts, dateLabel)
			}
			context := strings.Join(contextParts, " · ")
			appendIfVisible(moderacioItem{
				ID:        ev.ID,
				Type:      "event_historic",
				Nom:       strings.TrimSpace(ev.Titol),
				Context:   context,
				Autor:     autorNom,
				AutorURL:  autorURL,
				AutorID:   autorID,
				Created:   created,
				CreatedAt: createdAt,
				Motiu:     ev.ModerationNotes,
				EditURL:   fmt.Sprintf("/historia/events/%d", ev.ID),
				Status:    ev.ModerationStatus,
			})
		}
	}

	if canModerateAll && typeAllowed("registre") {
		status := ""
		if !statusAll {
			status = statusFilter
		}
		const regChunk = 200
		offset := 0
		for {
			registres, err := a.DB.ListTranscripcionsRawGlobal(db.TranscripcioFilter{
				Status: status,
				Limit:  regChunk,
				Offset: offset,
			})
			if err != nil || len(registres) == 0 {
				break
			}
			for _, reg := range registres {
				autorNom, autorURL, autorID := autorFromID(reg.CreatedBy)
				created := ""
				var createdAt time.Time
				if !reg.CreatedAt.IsZero() {
					created = reg.CreatedAt.Format("2006-01-02 15:04")
					createdAt = reg.CreatedAt
				}
				contextParts := []string{}
				if reg.TipusActe != "" {
					contextParts = append(contextParts, reg.TipusActe)
				}
				if reg.DataActeText != "" {
					contextParts = append(contextParts, reg.DataActeText)
				} else if reg.AnyDoc.Valid {
					contextParts = append(contextParts, fmt.Sprintf("%d", reg.AnyDoc.Int64))
				}
				if reg.NumPaginaText != "" {
					contextParts = append(contextParts, reg.NumPaginaText)
				}
				appendIfVisible(moderacioItem{
					ID:        reg.ID,
					Type:      "registre",
					Nom:       fmt.Sprintf("Registre %d", reg.ID),
					Context:   strings.Join(contextParts, " · "),
					Autor:     autorNom,
					AutorURL:  autorURL,
					AutorID:   autorID,
					Created:   created,
					CreatedAt: createdAt,
					Motiu:     reg.ModeracioMotiu,
					EditURL:   fmt.Sprintf("/documentals/registres/%d/editar?return_to=/moderacio", reg.ID),
					Status:    reg.ModeracioEstat,
				})
			}
			if len(registres) < regChunk {
				break
			}
			offset += regChunk
		}
	}

	if canModerateAll && typeAllowed("registre_canvi") {
		for _, change := range pendingChanges {
			autorNom, autorURL, autorID := autorFromID(change.ChangedBy)
			created := ""
			var createdAt time.Time
			if !change.ChangedAt.IsZero() {
				created = change.ChangedAt.Format("2006-01-02 15:04")
				createdAt = change.ChangedAt
			}
			contextParts := []string{}
			if change.ChangeType != "" {
				contextParts = append(contextParts, change.ChangeType)
			}
			if change.FieldKey != "" {
				contextParts = append(contextParts, change.FieldKey)
			}
			context := strings.Join(contextParts, " · ")
			if context == "" {
				context = fmt.Sprintf("Canvi %d", change.ID)
			}
			appendIfVisible(moderacioItem{
				ID:        change.ID,
				Type:      "registre_canvi",
				Nom:       fmt.Sprintf("Registre %d", change.TranscripcioID),
				Context:   context,
				Autor:     autorNom,
				AutorURL:  autorURL,
				AutorID:   autorID,
				Created:   created,
				CreatedAt: createdAt,
				Motiu:     change.ModeracioMotiu,
				EditURL:   fmt.Sprintf("/documentals/registres/%d/editar?return_to=/moderacio", change.TranscripcioID),
				Status:    change.ModeracioEstat,
			})
		}
	}

	if canModerateAll && len(wikiChanges) > 0 {
		typeMap := map[string]string{
			"municipi":       "municipi_canvi",
			"arxiu":          "arxiu_canvi",
			"llibre":         "llibre_canvi",
			"persona":        "persona_canvi",
			"cognom":         "cognom_canvi",
			"event_historic": "event_historic_canvi",
		}
		municipiCache := map[int]string{}
		arxiuCache := map[int]string{}
		llibreCache := map[int]string{}
		personaCache := map[int]string{}
		cognomCache := map[int]string{}
		eventCache := map[int]string{}
		for _, change := range wikiChanges {
			objType := typeMap[change.ObjectType]
			if objType == "" {
				objType = "wiki_canvi"
			}
			if !typeAllowed(objType) {
				continue
			}
			autorNom, autorURL, autorID := autorFromID(change.ChangedBy)
			created := ""
			var createdAt time.Time
			if !change.ChangedAt.IsZero() {
				created = change.ChangedAt.Format("2006-01-02 15:04")
				createdAt = change.ChangedAt
			}
			contextParts := []string{}
			if change.ChangeType != "" {
				contextParts = append(contextParts, change.ChangeType)
			}
			if change.FieldKey != "" {
				contextParts = append(contextParts, change.FieldKey)
			}
			context := strings.Join(contextParts, " · ")
			if context == "" {
				context = fmt.Sprintf("Canvi %d", change.ID)
			}
			name := fmt.Sprintf("%s %d", change.ObjectType, change.ObjectID)
			editURL := ""
			contextURL := ""
			switch change.ObjectType {
			case "municipi":
				if cached, ok := municipiCache[change.ObjectID]; ok {
					name = cached
				} else if mun, err := a.DB.GetMunicipi(change.ObjectID); err == nil && mun != nil {
					name = mun.Nom
					municipiCache[change.ObjectID] = name
				}
				contextURL = fmt.Sprintf("/territori/municipis/%d", change.ObjectID)
				editURL = fmt.Sprintf("/territori/municipis/%d/historial?view=%d", change.ObjectID, change.ID)
			case "arxiu":
				if cached, ok := arxiuCache[change.ObjectID]; ok {
					name = cached
				} else if arxiu, err := a.DB.GetArxiu(change.ObjectID); err == nil && arxiu != nil {
					name = arxiu.Nom
					arxiuCache[change.ObjectID] = name
				}
				contextURL = fmt.Sprintf("/documentals/arxius/%d", change.ObjectID)
				editURL = fmt.Sprintf("/documentals/arxius/%d/historial?view=%d", change.ObjectID, change.ID)
			case "llibre":
				if cached, ok := llibreCache[change.ObjectID]; ok {
					name = cached
				} else if llibre, err := a.DB.GetLlibre(change.ObjectID); err == nil && llibre != nil {
					name = llibre.Titol
					llibreCache[change.ObjectID] = name
				}
				contextURL = fmt.Sprintf("/documentals/llibres/%d", change.ObjectID)
				editURL = fmt.Sprintf("/documentals/llibres/%d/historial?view=%d", change.ObjectID, change.ID)
			case "persona":
				if cached, ok := personaCache[change.ObjectID]; ok {
					name = cached
				} else if persona, err := a.DB.GetPersona(change.ObjectID); err == nil && persona != nil {
					name = strings.TrimSpace(strings.Join([]string{persona.Nom, persona.Cognom1, persona.Cognom2}, " "))
					personaCache[change.ObjectID] = name
				}
				contextURL = fmt.Sprintf("/persones/%d", change.ObjectID)
				editURL = fmt.Sprintf("/persones/%d/historial?view=%d", change.ObjectID, change.ID)
			case "cognom":
				if cached, ok := cognomCache[change.ObjectID]; ok {
					name = cached
				} else if cognom, err := a.DB.GetCognom(change.ObjectID); err == nil && cognom != nil {
					name = cognom.Forma
					cognomCache[change.ObjectID] = name
				}
				contextURL = fmt.Sprintf("/cognoms/%d", change.ObjectID)
				editURL = fmt.Sprintf("/cognoms/%d/historial?view=%d", change.ObjectID, change.ID)
			case "event_historic":
				if cached, ok := eventCache[change.ObjectID]; ok {
					name = cached
				} else if ev, err := a.DB.GetEventHistoric(change.ObjectID); err == nil && ev != nil {
					name = strings.TrimSpace(ev.Titol)
					eventCache[change.ObjectID] = name
				}
				contextURL = fmt.Sprintf("/historia/events/%d", change.ObjectID)
				editURL = fmt.Sprintf("/historia/events/%d/historial?view=%d", change.ObjectID, change.ID)
			}
			appendIfVisible(moderacioItem{
				ID:         change.ID,
				Type:       objType,
				Nom:        name,
				Context:    context,
				ContextURL: contextURL,
				Autor:      autorNom,
				AutorURL:   autorURL,
				AutorID:    autorID,
				Created:    created,
				CreatedAt:  createdAt,
				Motiu:      change.ModeracioMotiu,
				EditURL:    editURL,
				Status:     change.ModeracioEstat,
			})
		}
	}

	if summary.Total > 0 {
		byType := make([]moderacioTypeCount, 0, len(typeCounts))
		for key, count := range typeCounts {
			byType = append(byType, moderacioTypeCount{Type: key, Total: count})
		}
		sort.Slice(byType, func(i, j int) bool {
			if byType[i].Total == byType[j].Total {
				return byType[i].Type < byType[j].Type
			}
			return byType[i].Total > byType[j].Total
		})
		summary.ByType = byType
		summary.TopType = byType[0].Type
		summary.TopTypeTotal = byType[0].Total
	}

	return items, summary.Total, summary
}

func (a *App) firstPendingActivityTime(objectType string, objectID int) string {
	if acts, err := a.DB.ListActivityByObject(objectType, objectID, "pendent"); err == nil {
		for _, act := range acts {
			return act.CreatedAt.Format("2006-01-02 15:04")
		}
	}
	return "—"
}

func parseModeracioTime(val string) time.Time {
	if val == "" || val == "—" {
		return time.Time{}
	}
	t, err := time.Parse("2006-01-02 15:04", val)
	if err != nil {
		return time.Time{}
	}
	return t
}

func parseModeracioFilters(r *http.Request) (moderacioFilters, string) {
	filterType := strings.TrimSpace(r.URL.Query().Get("type"))
	filterStatus := strings.TrimSpace(r.URL.Query().Get("status"))
	if filterStatus == "" {
		filterStatus = "pendent"
	}
	filterAge := strings.TrimSpace(r.URL.Query().Get("age"))
	switch filterAge {
	case moderacioAge0_24h, moderacioAge1_3d, moderacioAge3Plus:
	default:
		filterAge = ""
	}
	userInput := strings.TrimSpace(r.URL.Query().Get("user"))
	filterUserID := 0
	filterUserQuery := strings.TrimSpace(userInput)
	if filterUserQuery != "" {
		filterUserQuery = strings.TrimPrefix(filterUserQuery, "@")
		if n, err := strconv.Atoi(filterUserQuery); err == nil && n > 0 {
			filterUserID = n
			filterUserQuery = ""
		}
	}
	return moderacioFilters{
		Type:      filterType,
		Status:    filterStatus,
		AgeBucket: filterAge,
		UserID:    filterUserID,
		UserQuery: filterUserQuery,
	}, userInput
}

func moderacioReturnWithFlag(path string, flag string) string {
	separator := "?"
	if strings.Contains(path, "?") {
		separator = "&"
	}
	return path + separator + flag + "=1"
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (a *App) requireModeracioUser(w http.ResponseWriter, r *http.Request) (*db.User, db.PolicyPermissions, bool, bool) {
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return nil, db.PolicyPermissions{}, false, false
	}
	*r = *a.withUser(r, user)
	perms, found := a.permissionsFromContext(r)
	if !found {
		perms = a.getPermissionsForUser(user.ID)
		*r = *a.withPermissions(r, perms)
	}
	canModerateAll := a.hasPerm(perms, permModerate)
	if canModerateAll || a.hasAnyPermissionKey(user.ID, permKeyTerritoriMunicipisHistoriaModerate) || a.hasAnyPermissionKey(user.ID, permKeyTerritoriMunicipisAnecdotesModerate) {
		return user, perms, canModerateAll, true
	}
	http.Error(w, "Forbidden", http.StatusForbidden)
	return user, perms, false, false
}

func (a *App) canModerateTerritoriObject(user *db.User, perms db.PolicyPermissions, objectType string, versionID int) bool {
	if user == nil {
		return false
	}
	if a.hasPerm(perms, permModerate) {
		return true
	}
	munID := 0
	switch objectType {
	case "municipi_historia_general":
		if id, err := a.DB.ResolveMunicipiIDByHistoriaGeneralVersionID(versionID); err == nil {
			munID = id
		}
	case "municipi_historia_fet":
		if id, err := a.DB.ResolveMunicipiIDByHistoriaFetVersionID(versionID); err == nil {
			munID = id
		}
	case "municipi_anecdota_version":
		if id, err := a.DB.ResolveMunicipiIDByAnecdotariVersionID(versionID); err == nil {
			munID = id
		}
	default:
		return false
	}
	if munID <= 0 {
		return false
	}
	target := a.resolveMunicipiTarget(munID)
	switch objectType {
	case "municipi_anecdota_version":
		return a.HasPermission(user.ID, permKeyTerritoriMunicipisAnecdotesModerate, target)
	default:
		return a.HasPermission(user.ID, permKeyTerritoriMunicipisHistoriaModerate, target)
	}
}

// Llista de persones pendents de moderació
func (a *App) AdminModeracioList(w http.ResponseWriter, r *http.Request) {
	user, perms, canModerateAll, ok := a.requireModeracioUser(w, r)
	if !ok {
		return
	}
	page := 1
	perPage := 25
	if val := strings.TrimSpace(r.URL.Query().Get("page")); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			page = n
		}
	}
	if val := strings.TrimSpace(r.URL.Query().Get("per_page")); val != "" {
		if n, err := strconv.Atoi(val); err == nil {
			switch n {
			case 10, 25, 50, 100:
				perPage = n
			}
		}
	}
	filters, userInput := parseModeracioFilters(r)
	filterType := filters.Type
	filterStatus := filters.Status
	filterAge := filters.AgeBucket
	pageItems, total, summary := a.buildModeracioItems(ResolveLang(r), page, perPage, user, canModerateAll, filters)
	totalPages := 1
	if total > 0 {
		totalPages = (total + perPage - 1) / perPage
	}
	if page > totalPages {
		page = totalPages
	}
	start := (page - 1) * perPage
	if start < 0 {
		start = 0
	}
	end := start + perPage
	if end > total {
		end = total
	}
	pageStart := 0
	pageEnd := 0
	if total > 0 {
		pageStart = start + 1
		pageEnd = end
	}
	pageValues := url.Values{}
	pageValues.Set("per_page", strconv.Itoa(perPage))
	if filterType != "" {
		pageValues.Set("type", filterType)
	}
	if filterStatus != "" {
		pageValues.Set("status", filterStatus)
	}
	if filterAge != "" {
		pageValues.Set("age", filterAge)
	}
	if userInput != "" {
		pageValues.Set("user", userInput)
	}
	pageBase := "/moderacio"
	if encoded := pageValues.Encode(); encoded != "" {
		pageBase += "?" + encoded
	}
	canManageArxius := a.hasPerm(perms, permArxius)
	isAdmin := a.hasPerm(perms, permAdmin)
	msg := ""
	okFlag := false
	if r.URL.Query().Get("ok") != "" {
		okFlag = true
		msg = T(ResolveLang(r), "moderation.success")
	} else if r.URL.Query().Get("err") != "" {
		msg = T(ResolveLang(r), "moderation.error")
	}
	RenderPrivateTemplate(w, r, "admin-moderacio-list.html", map[string]interface{}{
		"Persones":        pageItems,
		"CanModerate":     true,
		"CanManageArxius": canManageArxius,
		"IsAdmin":         isAdmin,
		"Msg":             msg,
		"Ok":              okFlag,
		"CanBulk":         canModerateAll,
		"User":            user,
		"Total":           total,
		"Page":            page,
		"PerPage":         perPage,
		"TotalPages":      totalPages,
		"HasPrev":         page > 1,
		"HasNext":         page < totalPages,
		"PrevPage":        page - 1,
		"NextPage":        page + 1,
		"PageStart":       pageStart,
		"PageEnd":         pageEnd,
		"PageBase":        pageBase,
		"FilterType":      filterType,
		"FilterStatus":    filterStatus,
		"FilterAge":       filterAge,
		"FilterUser":      userInput,
		"Summary":         summary,
		"ReturnTo":        r.URL.RequestURI(),
	})
}

func (a *App) applyModeracioAction(ctx context.Context, action string, objType string, id int, motiu string, userID int) error {
	switch action {
	case "approve":
		if err := a.updateModeracioObject(objType, id, "publicat", "", userID); err != nil {
			return err
		}
		if acts, err := a.DB.ListActivityByObject(objType, id, "pendent"); err == nil {
			for _, act := range acts {
				_ = a.ValidateActivity(act.ID, userID)
			}
		}
		_, _ = a.RegisterUserActivity(ctx, userID, ruleModeracioApprove, "moderar_aprovar", objType, &id, "validat", nil, "")
		if objType == "event_historic" {
			a.registerEventHistoricModerationActivity(ctx, id, "publicat", userID, "")
		}
	case "reject":
		if err := a.updateModeracioObject(objType, id, "rebutjat", motiu, userID); err != nil {
			return err
		}
		if acts, err := a.DB.ListActivityByObject(objType, id, "pendent"); err == nil {
			for _, act := range acts {
				_ = a.DB.UpdateUserActivityStatus(act.ID, "anulat", &userID)
			}
		}
		_, _ = a.RegisterUserActivity(ctx, userID, ruleModeracioReject, "moderar_rebutjar", objType, &id, "validat", nil, motiu)
		if objType == "event_historic" {
			a.registerEventHistoricModerationActivity(ctx, id, "rebutjat", userID, motiu)
		}
	default:
		return fmt.Errorf("acció no vàlida")
	}
	return nil
}

func (a *App) processModeracioBulkAll(ctx context.Context, action, bulkType, motiu string, userID int, update func(processed int, total int)) error {
	if update == nil {
		update = func(int, int) {}
	}
	processed := 0
	total := 0
	errCount := 0
	updateTotal := func(add int) {
		if add <= 0 {
			return
		}
		total += add
		update(processed, total)
	}
	apply := func(objType string, id int) {
		if err := a.applyModeracioAction(ctx, action, objType, id, motiu, userID); err != nil {
			Errorf("Moderacio massiva %s %s:%d ha fallat: %v", action, objType, id, err)
			errCount++
		}
		processed++
		update(processed, total)
	}

	wikiPendingByType := map[string][]int{}
	if items, err := a.DB.ListWikiPending(0); err == nil {
		for _, item := range items {
			if !isValidWikiObjectType(item.ObjectType) {
				continue
			}
			wikiPendingByType[item.ObjectType] = append(wikiPendingByType[item.ObjectType], item.ChangeID)
		}
	}

	types := []string{"persona", "arxiu", "llibre", "nivell", "municipi", "eclesiastic", "municipi_historia_general", "municipi_historia_fet", "municipi_anecdota_version", "event_historic", "registre", "cognom_variant", "cognom_referencia", "cognom_merge", "municipi_canvi", "arxiu_canvi", "llibre_canvi", "persona_canvi", "cognom_canvi", "event_historic_canvi"}
	if bulkType != "" && bulkType != "all" {
		types = []string{bulkType}
	}
	for _, objType := range types {
		switch objType {
		case "persona":
			if rows, err := a.DB.ListPersones(db.PersonaFilter{Estat: "pendent"}); err == nil {
				updateTotal(len(rows))
				for _, row := range rows {
					apply(objType, row.ID)
				}
			} else {
				errCount++
			}
		case "arxiu":
			if rows, err := a.DB.ListArxius(db.ArxiuFilter{Status: "pendent"}); err == nil {
				updateTotal(len(rows))
				for _, row := range rows {
					apply(objType, row.ID)
				}
			} else {
				errCount++
			}
		case "llibre":
			if rows, err := a.DB.ListLlibres(db.LlibreFilter{Status: "pendent"}); err == nil {
				updateTotal(len(rows))
				for _, row := range rows {
					apply(objType, row.ID)
				}
			} else {
				errCount++
			}
		case "nivell":
			if rows, err := a.DB.ListNivells(db.NivellAdminFilter{Status: "pendent"}); err == nil {
				updateTotal(len(rows))
				for _, row := range rows {
					apply(objType, row.ID)
				}
			} else {
				errCount++
			}
		case "municipi":
			if rows, err := a.DB.ListMunicipis(db.MunicipiFilter{Status: "pendent"}); err == nil {
				updateTotal(len(rows))
				for _, row := range rows {
					apply(objType, row.ID)
				}
			} else {
				errCount++
			}
		case "eclesiastic":
			if rows, err := a.DB.ListArquebisbats(db.ArquebisbatFilter{Status: "pendent"}); err == nil {
				updateTotal(len(rows))
				for _, row := range rows {
					apply(objType, row.ID)
				}
			} else {
				errCount++
			}
		case "cognom_variant":
			if rows, err := a.DB.ListCognomVariants(db.CognomVariantFilter{Status: "pendent"}); err == nil {
				updateTotal(len(rows))
				for _, row := range rows {
					apply(objType, row.ID)
				}
			} else {
				errCount++
			}
		case "cognom_referencia":
			if rows, err := a.DB.ListCognomReferencies(db.CognomReferenciaFilter{Status: "pendent"}); err == nil {
				updateTotal(len(rows))
				for _, row := range rows {
					apply(objType, row.ID)
				}
			} else {
				errCount++
			}
		case "cognom_merge":
			if rows, err := a.DB.ListCognomRedirectSuggestions(db.CognomRedirectSuggestionFilter{Status: "pendent"}); err == nil {
				updateTotal(len(rows))
				for _, row := range rows {
					apply(objType, row.ID)
				}
			} else {
				errCount++
			}
		case "municipi_historia_general":
			if rows, _, err := a.DB.ListPendingMunicipiHistoriaGeneralVersions(0, 0); err == nil {
				updateTotal(len(rows))
				for _, row := range rows {
					apply(objType, row.ID)
				}
			} else {
				errCount++
			}
		case "municipi_historia_fet":
			if rows, _, err := a.DB.ListPendingMunicipiHistoriaFetVersions(0, 0); err == nil {
				updateTotal(len(rows))
				for _, row := range rows {
					apply(objType, row.ID)
				}
			} else {
				errCount++
			}
		case "municipi_anecdota_version":
			if rows, _, err := a.DB.ListPendingMunicipiAnecdotariVersions(0, 0); err == nil {
				updateTotal(len(rows))
				for _, row := range rows {
					apply(objType, row.ID)
				}
			} else {
				errCount++
			}
		case "event_historic":
			if rows, err := a.DB.ListEventsHistoric(db.EventHistoricFilter{Status: "pendent"}); err == nil {
				updateTotal(len(rows))
				for _, row := range rows {
					apply(objType, row.ID)
				}
			} else {
				errCount++
			}
		case "municipi_canvi":
			ids := wikiPendingByType["municipi"]
			updateTotal(len(ids))
			for _, id := range ids {
				apply(objType, id)
			}
		case "arxiu_canvi":
			ids := wikiPendingByType["arxiu"]
			updateTotal(len(ids))
			for _, id := range ids {
				apply(objType, id)
			}
		case "llibre_canvi":
			ids := wikiPendingByType["llibre"]
			updateTotal(len(ids))
			for _, id := range ids {
				apply(objType, id)
			}
		case "persona_canvi":
			ids := wikiPendingByType["persona"]
			updateTotal(len(ids))
			for _, id := range ids {
				apply(objType, id)
			}
		case "cognom_canvi":
			ids := wikiPendingByType["cognom"]
			updateTotal(len(ids))
			for _, id := range ids {
				apply(objType, id)
			}
		case "event_historic_canvi":
			ids := wikiPendingByType["event_historic"]
			updateTotal(len(ids))
			for _, id := range ids {
				apply(objType, id)
			}
		case "registre":
			if totalCount, err := a.DB.CountTranscripcionsRawGlobal(db.TranscripcioFilter{Status: "pendent"}); err == nil {
				updateTotal(totalCount)
				const chunk = 200
				offset := 0
				for {
					rows, err := a.DB.ListTranscripcionsRawGlobal(db.TranscripcioFilter{
						Status: "pendent",
						Limit:  chunk,
						Offset: offset,
					})
					if err != nil {
						errCount++
						break
					}
					if len(rows) == 0 {
						break
					}
					for _, row := range rows {
						apply(objType, row.ID)
					}
					if len(rows) < chunk {
						break
					}
					offset += chunk
				}
			} else {
				errCount++
			}
		}
	}
	if errCount > 0 {
		return fmt.Errorf("errors: %d", errCount)
	}
	return nil
}

// Accions massives de moderació
func (a *App) AdminModeracioBulk(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permModerate); !ok {
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
	user, _ := a.VerificarSessio(r)
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/moderacio?err=1", http.StatusSeeOther)
		return
	}
	action := strings.TrimSpace(r.FormValue("bulk_action"))
	if action == "" {
		action = strings.TrimSpace(r.FormValue("action"))
	}
	scope := strings.TrimSpace(r.FormValue("bulk_scope"))
	if scope == "" {
		scope = "page"
	}
	bulkType := strings.TrimSpace(r.FormValue("bulk_type"))
	if bulkType == "" {
		bulkType = "all"
	}
	selected := r.Form["selected"]
	motiu := strings.TrimSpace(r.FormValue("bulk_reason"))
	perms := a.getPermissionsForUser(user.ID)
	isAdmin := a.hasPerm(perms, permAdmin)
	if scope == "all" && !isAdmin {
		http.Redirect(w, r, "/moderacio?err=1", http.StatusSeeOther)
		return
	}
	async := strings.TrimSpace(r.FormValue("async")) == "1" || strings.Contains(r.Header.Get("Accept"), "application/json")
	returnTo := strings.TrimSpace(r.FormValue("return_to"))
	if returnTo == "" {
		returnTo = "/moderacio"
	}
	if scope == "all" {
		if async {
			job := a.moderacioBulkStore().newJob(action, scope, bulkType)
			store := a.moderacioBulkStore()
			go func() {
				err := a.processModeracioBulkAll(context.Background(), action, bulkType, motiu, user.ID, func(processed int, total int) {
					store.setTotal(job.ID, total)
					store.setProcessed(job.ID, processed)
				})
				store.finish(job.ID, err)
			}()
			writeJSON(w, map[string]interface{}{"ok": true, "job_id": job.ID})
			return
		}
		if err := a.processModeracioBulkAll(r.Context(), action, bulkType, motiu, user.ID, nil); err != nil {
			http.Redirect(w, r, moderacioReturnWithFlag(returnTo, "err"), http.StatusSeeOther)
			return
		}
		http.Redirect(w, r, moderacioReturnWithFlag(returnTo, "ok"), http.StatusSeeOther)
		return
	}
	if len(selected) == 0 {
		http.Redirect(w, r, moderacioReturnWithFlag(returnTo, "err"), http.StatusSeeOther)
		return
	}
	errCount := 0
	for _, entry := range selected {
		parts := strings.SplitN(entry, ":", 2)
		if len(parts) != 2 {
			errCount++
			continue
		}
		objType := strings.TrimSpace(parts[0])
		id, err := strconv.Atoi(parts[1])
		if err != nil {
			errCount++
			continue
		}
		if err := a.applyModeracioAction(r.Context(), action, objType, id, motiu, user.ID); err != nil {
			Errorf("Moderacio massiva %s %s:%d ha fallat: %v", action, objType, id, err)
			errCount++
		}
	}
	if errCount > 0 {
		http.Redirect(w, r, moderacioReturnWithFlag(returnTo, "err"), http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, moderacioReturnWithFlag(returnTo, "ok"), http.StatusSeeOther)
}

// Aprovar persona
func (a *App) AdminModeracioAprovar(w http.ResponseWriter, r *http.Request) {
	user, perms, canModerateAll, ok := a.requireModeracioUser(w, r)
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
	id := extractID(r.URL.Path)
	objType := strings.TrimSpace(r.FormValue("object_type"))
	if objType == "" {
		objType = "persona"
	}
	_ = r.ParseForm()
	if !canModerateAll && !a.canModerateTerritoriObject(user, perms, objType, id) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if err := a.updateModeracioObject(objType, id, "publicat", "", user.ID); err != nil {
		Errorf("Moderacio aprovar %s:%d ha fallat: %v", objType, id, err)
		http.Redirect(w, r, "/moderacio?err=1", http.StatusSeeOther)
		return
	}
	if acts, err := a.DB.ListActivityByObject(objType, id, "pendent"); err == nil {
		for _, act := range acts {
			_ = a.ValidateActivity(act.ID, user.ID)
		}
	}
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleModeracioApprove, "moderar_aprovar", objType, &id, "validat", nil, "")
	if objType == "event_historic" {
		a.registerEventHistoricModerationActivity(r.Context(), id, "publicat", user.ID, "")
	}
	http.Redirect(w, r, "/moderacio?ok=1", http.StatusSeeOther)
}

// Rebutjar persona amb motiu
func (a *App) AdminModeracioRebutjar(w http.ResponseWriter, r *http.Request) {
	user, perms, canModerateAll, ok := a.requireModeracioUser(w, r)
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
	id := extractID(r.URL.Path)
	objType := strings.TrimSpace(r.FormValue("object_type"))
	if objType == "" {
		objType = "persona"
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/moderacio?err=1", http.StatusSeeOther)
		return
	}
	if !canModerateAll && !a.canModerateTerritoriObject(user, perms, objType, id) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	motiu := r.FormValue("motiu")
	if err := a.updateModeracioObject(objType, id, "rebutjat", motiu, user.ID); err != nil {
		Errorf("Moderacio rebutjar %s:%d ha fallat: %v", objType, id, err)
		http.Redirect(w, r, "/moderacio?err=1", http.StatusSeeOther)
		return
	}
	if acts, err := a.DB.ListActivityByObject(objType, id, "pendent"); err == nil {
		for _, act := range acts {
			_ = a.DB.UpdateUserActivityStatus(act.ID, "anulat", &user.ID)
		}
	}
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleModeracioReject, "moderar_rebutjar", objType, &id, "validat", nil, motiu)
	if objType == "event_historic" {
		a.registerEventHistoricModerationActivity(r.Context(), id, "rebutjat", user.ID, motiu)
	}
	http.Redirect(w, r, "/moderacio?ok=1", http.StatusSeeOther)
}

func (a *App) updateModeracioObject(objectType string, id int, estat, motiu string, moderatorID int) error {
	switch objectType {
	case "persona":
		before, _ := a.DB.GetPersona(id)
		if err := a.DB.UpdatePersonaModeracio(id, estat, motiu, moderatorID); err != nil {
			return err
		}
		if estat == "publicat" {
			if err := a.upsertSearchDocForPersonaID(id); err != nil {
				Errorf("SearchIndex persona %d: %v", id, err)
			}
		} else if before != nil && before.ModeracioEstat == "publicat" {
			if err := a.DB.DeleteSearchDoc("persona", id); err != nil {
				Errorf("SearchIndex delete persona %d: %v", id, err)
			}
		}
		return nil
	case "arxiu":
		return a.DB.UpdateArxiuModeracio(id, estat, motiu, moderatorID)
	case "llibre":
		return a.DB.UpdateLlibreModeracio(id, estat, motiu, moderatorID)
	case "municipi":
		return a.DB.UpdateMunicipiModeracio(id, estat, motiu, moderatorID)
	case "nivell":
		return a.DB.UpdateNivellModeracio(id, estat, motiu, moderatorID)
	case "eclesiastic":
		return a.DB.UpdateArquebisbatModeracio(id, estat, motiu, moderatorID)
	case "registre":
		reg, err := a.DB.GetTranscripcioRaw(id)
		if err != nil {
			return err
		}
		if reg == nil {
			return fmt.Errorf("registre no trobat")
		}
		oldStatus := reg.ModeracioEstat
		delta := demografiaDeltaFromStatus(oldStatus, estat)
		if delta == 0 {
			if err := a.DB.UpdateTranscripcioModeracio(id, estat, motiu, moderatorID); err != nil {
				return err
			}
		} else {
			llibre, err := a.loadLlibreForRegistre(reg)
			if err != nil || llibre == nil {
				if err := a.DB.UpdateTranscripcioModeracio(id, estat, motiu, moderatorID); err != nil {
					return err
				}
				persones, _ := a.DB.ListTranscripcioPersones(reg.ID)
				a.applyNomCognomDeltaForRegistre(reg, persones, delta)
			} else {
				munID, year, tipus, ok := demografiaDeltaFromRegistre(reg, llibre)
				if !ok {
					if err := a.DB.UpdateTranscripcioModeracio(id, estat, motiu, moderatorID); err != nil {
						return err
					}
					persones, _ := a.DB.ListTranscripcioPersones(reg.ID)
					a.applyNomCognomDeltaForRegistre(reg, persones, delta)
				} else {
					if err := a.DB.UpdateTranscripcioModeracioWithDemografia(id, estat, motiu, moderatorID, munID, year, tipus, delta); err != nil {
						return err
					}
					a.applyNivellDemografiaDeltaForMunicipi(munID, year, tipus, delta)
					persones, _ := a.DB.ListTranscripcioPersones(reg.ID)
					a.applyNomCognomDeltaForRegistre(reg, persones, delta)
				}
			}
		}
		if estat == "publicat" {
			if err := a.upsertSearchDocForRegistreID(reg.ID); err != nil {
				Errorf("SearchIndex registre %d: %v", reg.ID, err)
			}
		} else if oldStatus == "publicat" {
			if err := a.DB.DeleteSearchDoc("registre_raw", reg.ID); err != nil {
				Errorf("SearchIndex delete registre %d: %v", reg.ID, err)
			}
		}
		return nil
	case "registre_canvi":
		return a.moderateRegistreChange(id, estat, motiu, moderatorID)
	case "cognom_variant":
		return a.DB.UpdateCognomVariantModeracio(id, estat, motiu, moderatorID)
	case "cognom_referencia":
		return a.DB.UpdateCognomReferenciaModeracio(id, estat, motiu, moderatorID)
	case "cognom_merge":
		return a.moderateCognomMergeSuggestion(id, estat, motiu, moderatorID)
	case "event_historic":
		return a.DB.UpdateEventHistoricModeracio(id, estat, motiu, moderatorID)
	case "municipi_canvi":
		return a.moderateWikiChange(id, "municipi", estat, motiu, moderatorID)
	case "arxiu_canvi":
		return a.moderateWikiChange(id, "arxiu", estat, motiu, moderatorID)
	case "llibre_canvi":
		return a.moderateWikiChange(id, "llibre", estat, motiu, moderatorID)
	case "persona_canvi":
		return a.moderateWikiChange(id, "persona", estat, motiu, moderatorID)
	case "cognom_canvi":
		return a.moderateWikiChange(id, "cognom", estat, motiu, moderatorID)
	case "event_historic_canvi":
		return a.moderateWikiChange(id, "event_historic", estat, motiu, moderatorID)
	case "municipi_historia_general":
		return a.DB.SetMunicipiHistoriaGeneralStatus(id, estat, motiu, &moderatorID)
	case "municipi_historia_fet":
		return a.DB.SetMunicipiHistoriaFetStatus(id, estat, motiu, &moderatorID)
	case "municipi_anecdota_version":
		if estat == "publicat" {
			if err := a.DB.ApproveMunicipiAnecdotariVersion(id, moderatorID); err != nil {
				return err
			}
			if version, err := a.DB.GetMunicipiAnecdotariVersion(id); err == nil && version != nil {
				Infof("Anecdota aprovada version=%d item=%d municipi=%d moderator=%d", id, version.ItemID, version.MunicipiID, moderatorID)
			} else {
				Infof("Anecdota aprovada version=%d moderator=%d", id, moderatorID)
			}
			return nil
		}
		if estat == "rebutjat" {
			if err := a.DB.RejectMunicipiAnecdotariVersion(id, moderatorID, motiu); err != nil {
				return err
			}
			if version, err := a.DB.GetMunicipiAnecdotariVersion(id); err == nil && version != nil {
				Infof("Anecdota rebutjada version=%d item=%d municipi=%d moderator=%d", id, version.ItemID, version.MunicipiID, moderatorID)
			} else {
				Infof("Anecdota rebutjada version=%d moderator=%d", id, moderatorID)
			}
			return nil
		}
		return fmt.Errorf("estat desconegut")
	default:
		return fmt.Errorf("tipus desconegut")
	}
}

func (a *App) moderateCognomMergeSuggestion(id int, estat, motiu string, moderatorID int) error {
	sugg, err := a.DB.GetCognomRedirectSuggestion(id)
	if err != nil {
		return err
	}
	if sugg == nil {
		return fmt.Errorf("merge suggestion not found")
	}
	if estat == "publicat" {
		toID := sugg.ToCognomID
		if canonID, _, err := a.resolveCognomCanonicalID(toID); err == nil && canonID > 0 {
			toID = canonID
		}
		if sugg.FromCognomID != toID {
			var createdBy *int
			if sugg.CreatedBy.Valid {
				val := int(sugg.CreatedBy.Int64)
				createdBy = &val
			}
			if err := a.DB.SetCognomRedirect(sugg.FromCognomID, toID, createdBy, sugg.Reason); err != nil {
				return err
			}
		}
	}
	return a.DB.UpdateCognomRedirectSuggestionModeracio(id, estat, motiu, moderatorID)
}

func (a *App) registerEventHistoricModerationActivity(ctx context.Context, eventID int, status string, moderatorID int, reason string) {
	if eventID <= 0 {
		return
	}
	event, err := a.DB.GetEventHistoric(eventID)
	if err != nil || event == nil || !event.CreatedBy.Valid {
		return
	}
	authorID := int(event.CreatedBy.Int64)
	switch status {
	case "publicat":
		_, _ = a.RegisterUserActivity(ctx, authorID, ruleEventHistoricApprove, "event_historic_approve", "event_historic", &eventID, "validat", &moderatorID, "")
	case "rebutjat":
		_, _ = a.RegisterUserActivity(ctx, authorID, ruleEventHistoricReject, "event_historic_reject", "event_historic", &eventID, "validat", &moderatorID, reason)
	}
}

func (a *App) moderateRegistreChange(changeID int, estat, motiu string, moderatorID int) error {
	change, err := a.DB.GetTranscripcioRawChange(changeID)
	if err != nil {
		return err
	}
	if change == nil {
		return fmt.Errorf("canvi no trobat")
	}
	if err := a.DB.UpdateTranscripcioRawChangeModeracio(changeID, estat, motiu, moderatorID); err != nil {
		return err
	}
	if estat != "publicat" {
		a.updateRegistreChangeActivities(change.TranscripcioID, changeID, moderatorID, false)
		return nil
	}
	registre, err := a.DB.GetTranscripcioRaw(change.TranscripcioID)
	if err != nil || registre == nil {
		return fmt.Errorf("registre no trobat")
	}
	beforePersones, _ := a.DB.ListTranscripcioPersones(registre.ID)
	_, afterSnap := parseTranscripcioChangeMeta(*change)
	if afterSnap == nil {
		return fmt.Errorf("canvi sense dades")
	}
	after := *afterSnap
	after.Persones = append([]db.TranscripcioPersonaRaw(nil), afterSnap.Persones...)
	after.Atributs = append([]db.TranscripcioAtributRaw(nil), afterSnap.Atributs...)
	after.Raw.ID = registre.ID
	after.Raw.ModeracioEstat = "publicat"
	after.Raw.ModeratedBy = sqlNullIntFromInt(moderatorID)
	after.Raw.ModeratedAt = sql.NullTime{Time: time.Now(), Valid: true}
	after.Raw.ModeracioMotiu = motiu
	if !after.Raw.CreatedBy.Valid {
		after.Raw.CreatedBy = registre.CreatedBy
	}
	if err := a.DB.UpdateTranscripcioRaw(&after.Raw); err != nil {
		return err
	}
	_ = a.DB.DeleteTranscripcioPersones(registre.ID)
	for i := range after.Persones {
		after.Persones[i].TranscripcioID = registre.ID
		_, _ = a.DB.CreateTranscripcioPersona(&after.Persones[i])
	}
	_ = a.DB.DeleteTranscripcioAtributs(registre.ID)
	for i := range after.Atributs {
		after.Atributs[i].TranscripcioID = registre.ID
		_, _ = a.DB.CreateTranscripcioAtribut(&after.Atributs[i])
	}
	if registre.ModeracioEstat == "publicat" {
		a.applyDemografiaDeltaForRegistre(registre, -1)
	}
	if after.Raw.ModeracioEstat == "publicat" {
		a.applyDemografiaDeltaForRegistre(&after.Raw, 1)
	}
	if registre.ModeracioEstat == "publicat" {
		a.applyNomCognomDeltaForRegistre(registre, beforePersones, -1)
	}
	if after.Raw.ModeracioEstat == "publicat" {
		a.applyNomCognomDeltaForRegistre(&after.Raw, after.Persones, 1)
	}
	a.updateRegistreChangeActivities(change.TranscripcioID, changeID, moderatorID, true)
	if change.ChangeType == "revert" {
		if srcID := parseRevertSourceChangeID(change.Metadata); srcID > 0 {
			if srcChange, err := a.DB.GetTranscripcioRawChange(srcID); err == nil && srcChange != nil && srcChange.ChangedBy.Valid {
				changedByID := int(srcChange.ChangedBy.Int64)
				if acts, err := a.DB.ListActivityByObject("registre", change.TranscripcioID, "validat"); err == nil {
					for _, act := range acts {
						if act.UserID != changedByID || act.Action != "editar_registre" {
							continue
						}
						_ = a.DB.UpdateUserActivityStatus(act.ID, "anulat", &moderatorID)
						if act.Points != 0 {
							_ = a.DB.AddPointsToUser(act.UserID, -act.Points)
						}
						break
					}
				}
			}
		}
	}
	if err := a.upsertSearchDocForRegistreID(registre.ID); err != nil {
		Errorf("SearchIndex registre %d: %v", registre.ID, err)
	}
	_, _ = a.recalcLlibreIndexacioStats(registre.LlibreID)
	return nil
}

func (a *App) moderateWikiChange(changeID int, objectType string, estat, motiu string, moderatorID int) error {
	change, err := a.DB.GetWikiChange(changeID)
	if err != nil {
		return err
	}
	if change == nil {
		return fmt.Errorf("canvi no trobat")
	}
	if objectType != "" && change.ObjectType != objectType {
		return fmt.Errorf("tipus de canvi no coincideix")
	}
	if err := a.DB.UpdateWikiChangeModeracio(changeID, estat, motiu, moderatorID); err != nil {
		Errorf("WikiChangeModeracio failed change_id=%d object=%s object_id=%d err=%v", changeID, change.ObjectType, change.ObjectID, err)
		return err
	}
	if estat != "publicat" {
		return nil
	}
	if !isValidWikiObjectType(change.ObjectType) {
		return fmt.Errorf("tipus desconegut")
	}
	switch change.ObjectType {
	case "municipi":
		return a.applyWikiMunicipiChange(change, motiu, moderatorID)
	case "arxiu":
		return a.applyWikiArxiuChange(change, motiu, moderatorID)
	case "llibre":
		return a.applyWikiLlibreChange(change, motiu, moderatorID)
	case "persona":
		return a.applyWikiPersonaChange(change, motiu, moderatorID)
	case "cognom":
		return a.applyWikiCognomChange(change, motiu, moderatorID)
	case "event_historic":
		return a.applyWikiEventHistoricChange(change, motiu, moderatorID)
	default:
		return fmt.Errorf("tipus desconegut")
	}
}

func (a *App) updateRegistreChangeActivities(registreID, changeID, moderatorID int, validate bool) {
	acts, err := a.DB.ListActivityByObject("registre", registreID, "pendent")
	if err != nil {
		return
	}
	detailKey := fmt.Sprintf("change:%d", changeID)
	for _, act := range acts {
		if act.Details != "" && act.Details != detailKey {
			continue
		}
		if validate {
			_ = a.ValidateActivity(act.ID, moderatorID)
		} else {
			_ = a.CancelActivity(act.ID, moderatorID)
		}
	}
}

func parseRevertSourceChangeID(payload string) int {
	if strings.TrimSpace(payload) == "" {
		return 0
	}
	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(payload), &raw); err != nil {
		return 0
	}
	revertRaw, ok := raw["revert"]
	if !ok {
		return 0
	}
	revertMap, ok := revertRaw.(map[string]interface{})
	if !ok {
		return 0
	}
	val, ok := revertMap["source_change_id"]
	if !ok {
		return 0
	}
	switch v := val.(type) {
	case float64:
		return int(v)
	case int:
		return v
	case string:
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return 0
}
