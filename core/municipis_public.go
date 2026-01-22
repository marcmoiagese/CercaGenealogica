package core

import (
	"database/sql"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type municipiRecordColumn struct {
	Key        string
	Label      string
	Filterable bool
	IsStatus   bool
	IsActions  bool
}

type municipiRecordRow struct {
	ID             int
	ModeracioEstat string
	Cells          map[string]interface{}
}

func personByRoles(persones []db.TranscripcioPersonaRaw, roles ...string) *db.TranscripcioPersonaRaw {
	if len(persones) == 0 {
		return nil
	}
	for _, role := range roles {
		for i := range persones {
			if persones[i].Rol == role {
				return &persones[i]
			}
		}
	}
	return nil
}

func (a *App) MunicipiPublic(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	mun, err := a.DB.GetMunicipi(id)
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
	canManageArxius := user != nil && a.hasPerm(perms, permArxius)
	canManagePolicies := user != nil && (perms.CanManagePolicies || perms.Admin)
	munTarget := a.resolveMunicipiTarget(mun.ID)
	canViewMap := user != nil && a.HasPermission(user.ID, permKeyTerritoriMunicipisMapesView, munTarget)
	canCreateMap := user != nil && a.HasPermission(user.ID, permKeyTerritoriMunicipisMapesCreate, munTarget)

	if mun.ModeracioEstat != "" && mun.ModeracioEstat != "publicat" && !(canManageTerritory || canModerate) {
		http.NotFound(w, r)
		return
	}

	lang := ResolveLang(r)
	seen := make(map[int]bool)
	var levels []db.NivellAdministratiu
	for _, lvlID := range mun.NivellAdministratiuID {
		if !lvlID.Valid {
			continue
		}
		id := int(lvlID.Int64)
		if seen[id] {
			continue
		}
		seen[id] = true
		if lvl, err := a.DB.GetNivell(id); err == nil && lvl != nil {
			levels = append(levels, *lvl)
		}
	}
	sort.Slice(levels, func(i, j int) bool {
		return levels[i].Nivel < levels[j].Nivel
	})

	type levelView struct {
		Nivel int
		Nom   string
		Tipus string
	}
	var levelViews []levelView
	for _, lvl := range levels {
		labelKey := fmt.Sprintf("levels.types.%s", lvl.TipusNivell)
		label := T(lang, labelKey)
		if label == labelKey {
			label = lvl.TipusNivell
		}
		levelViews = append(levelViews, levelView{
			Nivel: lvl.Nivel,
			Nom:   lvl.NomNivell,
			Tipus: label,
		})
	}

	countryLabel := ""
	for _, lvl := range levels {
		if lvl.PaisISO2.Valid {
			countryLabel = a.countryLabelFromISO(lvl.PaisISO2.String, lang)
			break
		}
	}
	typeKey := fmt.Sprintf("municipis.type.%s", mun.Tipus)
	typeLabel := T(lang, typeKey)
	if typeLabel == typeKey {
		typeLabel = mun.Tipus
	}
	regionLabel := ""
	if len(levelViews) > 0 {
		regionLabel = levelViews[len(levelViews)-1].Nom
	}
	mapesData := map[string]interface{}{
		"municipi_id": mun.ID,
		"mapes_api":   fmt.Sprintf("/api/municipis/%d/mapes", mun.ID),
		"mapes_page":  fmt.Sprintf("/territori/municipis/%d/mapes", mun.ID),
	}

	historiaGeneral, historiaTimeline, historiaErr := a.DB.GetMunicipiHistoriaSummary(mun.ID)
	if historiaErr != nil {
		Errorf("Error carregant historia municipi %d: %v", mun.ID, historiaErr)
	}
	historiaSummary := ""
	if historiaGeneral != nil {
		if strings.TrimSpace(historiaGeneral.Resum) != "" {
			historiaSummary = strings.TrimSpace(historiaGeneral.Resum)
		} else {
			historiaSummary = summarizeHistoriaText(historiaGeneral.CosText, 260)
		}
	}
	historiaTimelineView := []map[string]string{}
	for _, item := range historiaTimeline {
		historiaTimelineView = append(historiaTimelineView, map[string]string{
			"date":  historiaDateLabel(item),
			"title": strings.TrimSpace(item.Titol),
			"resum": strings.TrimSpace(item.Resum),
		})
	}
	canAportarHistoria := user != nil && a.HasPermission(user.ID, permKeyTerritoriMunicipisHistoriaCreate, munTarget)

	recordsColumns := []municipiRecordColumn{
		{Key: "llibre", Label: T(lang, "records.search.table.book"), Filterable: true},
		{Key: "tipus", Label: T(lang, "records.table.type"), Filterable: true},
		{Key: "any", Label: T(lang, "records.table.year"), Filterable: true},
		{Key: "subjecte", Label: T(lang, "records.table.subject"), Filterable: true},
		{Key: "pare", Label: T(lang, "records.detail.father"), Filterable: true},
		{Key: "mare", Label: T(lang, "records.detail.mother"), Filterable: true},
		{Key: "ofici", Label: T(lang, "persons.col.ofici"), Filterable: true},
		{Key: "status", Label: T(lang, "records.table.status"), IsStatus: true},
		{Key: "actions", Label: T(lang, "records.table.actions"), IsActions: true},
	}
	recordsRows := []municipiRecordRow{}
	recordStatus := "publicat"
	if canManageArxius {
		recordStatus = ""
	}
	llibres, _ := a.DB.ListLlibres(db.LlibreFilter{
		MunicipiID: mun.ID,
		Status:     recordStatus,
	})
	for _, llibre := range llibres {
		registres, _ := a.DB.ListTranscripcionsRaw(llibre.ID, db.TranscripcioFilter{
			Status: recordStatus,
			Limit:  -1,
		})
		for _, reg := range registres {
			persones, _ := a.DB.ListTranscripcioPersones(reg.ID)
			primary := primaryPersonForTipus(reg.TipusActe, persones)
			father := personByRoles(persones, "pare", "pare_nuvi", "pare_novia")
			mother := personByRoles(persones, "mare", "mare_nuvi", "mare_novia")

			subjecte := ""
			ofici := ""
			if primary != nil {
				subjecte = personDisplayName(*primary)
				ofici = primary.OficiText
			}

			tipusKey := fmt.Sprintf("records.type.%s", reg.TipusActe)
			tipusLabel := T(lang, tipusKey)
			if tipusLabel == tipusKey {
				tipusLabel = reg.TipusActe
			}

			cells := map[string]interface{}{
				"llibre":   bookDisplayTitle(llibre.Llibre),
				"tipus":    tipusLabel,
				"any":      formatAny(reg.AnyDoc),
				"subjecte": subjecte,
				"pare":     personDisplayNameOrDash(father),
				"mare":     personDisplayNameOrDash(mother),
				"ofici":    ofici,
			}

			recordsRows = append(recordsRows, municipiRecordRow{
				ID:             reg.ID,
				ModeracioEstat: reg.ModeracioEstat,
				Cells:          cells,
			})
		}
	}

	status := "publicat"
	if canManageArxius {
		status = ""
	}
	arxius, _ := a.DB.ListArxius(db.ArxiuFilter{
		MunicipiID: mun.ID,
		Status:     status,
		Limit:      200,
	})

	data := map[string]interface{}{
		"Municipi":          mun,
		"Hierarchy":         levelViews,
		"CountryLabel":      countryLabel,
		"TypeLabel":         typeLabel,
		"RegionLabel":       regionLabel,
		"Arxius":            arxius,
		"RecordColumns":     recordsColumns,
		"RecordRows":        recordsRows,
		"User":              user,
		"CanManageArxius":   canManageArxius,
		"CanManagePolicies": canManagePolicies,
		"CanModerate":       canModerate,
		"CanManageTerritory": canManageTerritory,
		"CanViewMap":        canViewMap,
		"CanCreateMap":      canCreateMap,
		"ShowStatusBadge":   canManageTerritory || canModerate,
		"MapesData":         mapesData,
		"HistoriaGeneral":   historiaGeneral,
		"HistoriaGeneralSummary": historiaSummary,
		"HistoriaTimelineDestacat": historiaTimelineView,
		"CanAportarHistoria": canAportarHistoria,
	}
	if user != nil {
		RenderPrivateTemplate(w, r, "municipi-perfil-pro.html", data)
		return
	}
	RenderTemplate(w, r, "municipi-perfil-pro.html", data)
}

func formatAny(val sql.NullInt64) string {
	if val.Valid {
		return fmt.Sprintf("%d", val.Int64)
	}
	return ""
}

func personDisplayNameOrDash(p *db.TranscripcioPersonaRaw) string {
	if p == nil {
		return "-"
	}
	name := personDisplayName(*p)
	if name == "" {
		return "-"
	}
	return name
}

func bookDisplayTitle(llibre db.Llibre) string {
	if llibre.Titol != "" {
		return llibre.Titol
	}
	return llibre.NomEsglesia
}

func summarizeHistoriaText(text string, maxLen int) string {
	clean := strings.TrimSpace(text)
	if clean == "" {
		return ""
	}
	clean = strings.Join(strings.Fields(clean), " ")
	if maxLen <= 0 || len(clean) <= maxLen {
		return clean
	}
	cut := clean[:maxLen]
	if idx := strings.LastIndex(cut, " "); idx > maxLen/2 {
		cut = cut[:idx]
	}
	return strings.TrimSpace(cut) + "..."
}

func historiaDateLabel(item db.MunicipiHistoriaFetVersion) string {
	if strings.TrimSpace(item.DataDisplay) != "" {
		return strings.TrimSpace(item.DataDisplay)
	}
	if item.AnyInici.Valid && item.AnyFi.Valid {
		if item.AnyInici.Int64 == item.AnyFi.Int64 {
			return fmt.Sprintf("%d", item.AnyInici.Int64)
		}
		return fmt.Sprintf("%d-%d", item.AnyInici.Int64, item.AnyFi.Int64)
	}
	if item.AnyInici.Valid {
		return fmt.Sprintf("%d", item.AnyInici.Int64)
	}
	if item.AnyFi.Valid {
		return fmt.Sprintf("%d", item.AnyFi.Int64)
	}
	return ""
}
