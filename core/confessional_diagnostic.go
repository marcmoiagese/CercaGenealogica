package core

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const (
	confDiagnosticSeverityCritical = "critical"
	confDiagnosticSeverityWarning  = "warning"
	confDiagnosticSeverityInfo     = "info"

	confDiagnosticTypeMissingParent                = "missing_parent"
	confDiagnosticTypeIncompatibleRelation         = "incompatible_relation"
	confDiagnosticTypeMultipleParents              = "multiple_parents"
	confDiagnosticTypePendingRelationInconsistent  = "pending_relation_inconsistent"
	confDiagnosticTypeLocalEntityWithoutTerritory  = "local_entity_without_territory"
	confDiagnosticTypeMunicipalityWithoutLocalUnit = "municipality_without_local_entity"
	confDiagnosticTypeArchiveWithoutContext        = "archive_without_context"
	confDiagnosticTypeArchiveContextAmbiguous      = "archive_context_ambiguous"
	confDiagnosticTypePossibleDuplicate            = "possible_duplicate"
)

type confessionalDiagnosticFilter struct {
	ReligionCode string
	LevelCode    string
	Severity     string
	AlertType    string
	Query        string
}

type confessionalDiagnosticSummary struct {
	PublishedEntities             int
	PendingEntities               int
	PublishedHierarchyRelations   int
	PublishedTerritorialRelations int
	PublishedArchiveRelations     int
	CriticalAlerts                int
	WarningAlerts                 int
	InfoAlerts                    int
}

type confessionalDiagnosticAlert struct {
	Severity      string
	SeverityLabel string
	SeverityClass string
	Type          string
	TypeLabel     string
	Description   string
	Subject       string
	SubjectURL    string
	Context       string
	ContextURL    string
	ReligionCode  string
	LevelCode     string
}

type confessionalDiagnosticCoverageRow struct {
	ReligionCode         string
	ReligionLabel        string
	PublishedEntities    int
	HierarchyRelations   int
	TerritorialRelations int
	ArchiveRelations     int
	CriticalAlerts       int
	WarningAlerts        int
}

type confessionalDiagnosticOption struct {
	Value string
	Label string
}

type confessionalDiagnosticPageData struct {
	Filter              confessionalDiagnosticFilter
	Summary             confessionalDiagnosticSummary
	Alerts              []confessionalDiagnosticAlert
	CoverageRows        []confessionalDiagnosticCoverageRow
	SelectableReligions []ConfessionalReligionCatalogItem
	SelectableNivells   []ConfessionalLevelCatalogItem
	SeverityOptions     []confessionalDiagnosticOption
	AlertTypeOptions    []confessionalDiagnosticOption
}

func (a *App) AdminConfessionalDiagnostic(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriConfessionalDiagnosticView, PermissionTarget{})
	if !ok {
		return
	}

	lang := ResolveLangForUser(r, user.PreferredLang)
	filter := parseConfessionalDiagnosticFilter(r)

	allEntitats, _ := a.DB.ListEntitatsReligioses()
	allRelacionsEntitats, _ := a.DB.ListEntitatReligiosaRelacions()
	allRelacionsTerritori, _ := a.DB.ListMunicipiEntitatsReligioses(0)
	allRelacionsArxiu, _ := a.DB.ListArxiuEntitatsReligioses(0, 0, "")
	allMunicipis, _ := a.DB.ListMunicipis(db.MunicipiFilter{})
	allArxius, _ := a.DB.ListArxius(db.ArxiuFilter{Status: "publicat", Limit: -1})

	publishedEntitats := publishedEntitatsReligioses(allEntitats)
	publishedRelacionsEntitats := publishedEntitatReligiosaRelacions(allRelacionsEntitats)
	publishedRelacionsTerritori := publishedMunicipiEntitatsReligioses(allRelacionsTerritori)
	publishedRelacionsArxiu := publishedArxiuEntitatsReligiosesForDiagnostic(allRelacionsArxiu)
	publishedMunicipis := publishedMunicipisForDiagnostic(allMunicipis)

	diagnostic := a.buildConfessionalDiagnostic(lang, filter, allEntitats, publishedEntitats, allRelacionsEntitats, publishedRelacionsEntitats, allRelacionsTerritori, publishedRelacionsTerritori, allRelacionsArxiu, publishedRelacionsArxiu, publishedMunicipis, allArxius)
	diagnostic.Filter = filter
	diagnostic.SelectableReligions = ListSelectableConfessionalReligionCatalog()
	diagnostic.SelectableNivells = ListConfessionalLevelCatalog()
	diagnostic.SeverityOptions = confessionalDiagnosticSeverityOptions(lang)
	diagnostic.AlertTypeOptions = confessionalDiagnosticTypeOptions(lang)

	RenderPrivateTemplate(w, r, "admin-confessional-diagnostic.html", map[string]interface{}{
		"Diagnostic":            diagnostic,
		"SelectableReligions":   diagnostic.SelectableReligions,
		"SelectableNivells":     diagnostic.SelectableNivells,
		"ReligionCatalogLabels": confessionalReligionCatalogLabels(lang),
		"LevelCatalogLabels":    confessionalLevelCatalogLabels(lang),
		"User":                  user,
	})
}

func parseConfessionalDiagnosticFilter(r *http.Request) confessionalDiagnosticFilter {
	q := r.URL.Query()
	filter := confessionalDiagnosticFilter{
		ReligionCode: normalizeCatalogCode(q.Get("religio_confessio_codi")),
		LevelCode:    normalizeCatalogCode(q.Get("nivell_confessional_codi")),
		Severity:     normalizeConfessionalDiagnosticSeverity(q.Get("severity")),
		AlertType:    normalizeConfessionalDiagnosticType(q.Get("alert_type")),
		Query:        normalizeConfessionalSearchText(q.Get("q")),
	}
	if _, ok := GetConfessionalReligionCatalogByCode(filter.ReligionCode); filter.ReligionCode != "" && !ok {
		filter.ReligionCode = ""
	}
	if _, ok := GetConfessionalLevelCatalogByCode(filter.LevelCode); filter.LevelCode != "" && !ok {
		filter.LevelCode = ""
	}
	if filter.ReligionCode != "" && filter.LevelCode != "" {
		_, _, _, _, compatible := ConfessionalLevelCompatibleWithReligion(filter.ReligionCode, filter.LevelCode)
		if !compatible {
			filter.LevelCode = ""
		}
	}
	return filter
}

func normalizeConfessionalDiagnosticSeverity(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case confDiagnosticSeverityCritical, confDiagnosticSeverityWarning, confDiagnosticSeverityInfo:
		return strings.TrimSpace(strings.ToLower(raw))
	default:
		return ""
	}
}

func normalizeConfessionalDiagnosticType(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case confDiagnosticTypeMissingParent,
		confDiagnosticTypeIncompatibleRelation,
		confDiagnosticTypeMultipleParents,
		confDiagnosticTypePendingRelationInconsistent,
		confDiagnosticTypeLocalEntityWithoutTerritory,
		confDiagnosticTypeMunicipalityWithoutLocalUnit,
		confDiagnosticTypeArchiveWithoutContext,
		confDiagnosticTypeArchiveContextAmbiguous,
		confDiagnosticTypePossibleDuplicate:
		return strings.TrimSpace(strings.ToLower(raw))
	default:
		return ""
	}
}

func confessionalDiagnosticSeverityOptions(lang string) []confessionalDiagnosticOption {
	return []confessionalDiagnosticOption{
		{Value: "", Label: T(lang, "confessional.diagnostic.filter.all_severities")},
		{Value: confDiagnosticSeverityCritical, Label: T(lang, "confessional.diagnostic.severity.critical")},
		{Value: confDiagnosticSeverityWarning, Label: T(lang, "confessional.diagnostic.severity.warning")},
		{Value: confDiagnosticSeverityInfo, Label: T(lang, "confessional.diagnostic.severity.info")},
	}
}

func confessionalDiagnosticTypeOptions(lang string) []confessionalDiagnosticOption {
	types := []string{
		confDiagnosticTypeMissingParent,
		confDiagnosticTypeIncompatibleRelation,
		confDiagnosticTypeMultipleParents,
		confDiagnosticTypePendingRelationInconsistent,
		confDiagnosticTypeLocalEntityWithoutTerritory,
		confDiagnosticTypeMunicipalityWithoutLocalUnit,
		confDiagnosticTypeArchiveWithoutContext,
		confDiagnosticTypeArchiveContextAmbiguous,
		confDiagnosticTypePossibleDuplicate,
	}
	options := []confessionalDiagnosticOption{{Value: "", Label: T(lang, "confessional.diagnostic.filter.all_alert_types")}}
	for _, typ := range types {
		options = append(options, confessionalDiagnosticOption{
			Value: typ,
			Label: T(lang, "confessional.diagnostic.type."+typ),
		})
	}
	return options
}

func (a *App) buildConfessionalDiagnostic(lang string, filter confessionalDiagnosticFilter, allEntitats, publishedEntitats []db.EntitatReligiosa, allRelacionsEntitats, publishedRelacionsEntitats []db.EntitatReligiosaRelacio, allRelacionsTerritori, publishedRelacionsTerritori []db.MunicipiEntitatReligiosa, allRelacionsArxiu, publishedRelacionsArxiu []db.ArxiuEntitatReligiosa, publishedMunicipis []db.MunicipiRow, publishedArxius []db.ArxiuWithCount) confessionalDiagnosticPageData {
	entitatsByID := confessionalEntitatsByID(allEntitats)
	publishedEntitatsByID := confessionalEntitatsByID(publishedEntitats)
	religionLabels := confessionalReligionCatalogLabels(lang)
	levelLabels := confessionalLevelCatalogLabels(lang)
	alerts := make([]confessionalDiagnosticAlert, 0, 64)

	publishedParentsByChild := map[int][]db.EntitatReligiosaRelacio{}
	for _, rel := range publishedRelacionsEntitats {
		publishedParentsByChild[rel.EntitatDestiID] = append(publishedParentsByChild[rel.EntitatDestiID], rel)
	}

	publishedTerritoriByEntitat := map[int][]db.MunicipiEntitatReligiosa{}
	localCoverageByMunicipi := map[int]bool{}
	for _, rel := range publishedRelacionsTerritori {
		publishedTerritoriByEntitat[rel.EntitatReligiosaID] = append(publishedTerritoriByEntitat[rel.EntitatReligiosaID], rel)
		if entitat, ok := publishedEntitatsByID[rel.EntitatReligiosaID]; ok && confessionalDiagnosticIsLocalTerritorialEntity(entitat) {
			localCoverageByMunicipi[rel.MunicipiID] = true
			if rel.NucliID.Valid {
				localCoverageByMunicipi[int(rel.NucliID.Int64)] = true
			}
		}
	}

	publishedArxiuByArxiuID := map[int][]db.ArxiuEntitatReligiosa{}
	for _, rel := range publishedRelacionsArxiu {
		publishedArxiuByArxiuID[rel.ArxiuID] = append(publishedArxiuByArxiuID[rel.ArxiuID], rel)
	}

	for _, entity := range publishedEntitats {
		if !confessionalDiagnosticEntityMatchesScope(filter, entity) {
			continue
		}
		level, levelOK := GetConfessionalLevelCatalogByCode(entity.NivellConfessionalCodi)
		parents := publishedParentsByChild[entity.ID]
		if len(parents) == 0 {
			alert := confessionalDiagnosticAlert{
				Type:         confDiagnosticTypeMissingParent,
				TypeLabel:    T(lang, "confessional.diagnostic.type."+confDiagnosticTypeMissingParent),
				Subject:      entity.Nom,
				SubjectURL:   fmt.Sprintf("/confessional/entitats/%d", entity.ID),
				ReligionCode: entity.ReligioConfessioCodi,
				LevelCode:    entity.NivellConfessionalCodi,
				Context:      strings.TrimSpace(strings.Join([]string{religionLabels[entity.ReligioConfessioCodi], levelLabels[entity.NivellConfessionalCodi]}, " · ")),
			}
			switch {
			case levelOK && len(level.AllowedParentLevelCodes) > 0:
				alert.Severity = confDiagnosticSeverityCritical
				alert.Description = fmt.Sprintf(T(lang, "confessional.diagnostic.message.missing_parent.expected"), levelLabels[entity.NivellConfessionalCodi])
			case levelOK && len(level.AllowedParentLevelCodes) == 0:
				alert.Severity = confDiagnosticSeverityInfo
				alert.Description = fmt.Sprintf(T(lang, "confessional.diagnostic.message.missing_parent.root"), levelLabels[entity.NivellConfessionalCodi])
			default:
				alert.Severity = confDiagnosticSeverityWarning
				alert.Description = fmt.Sprintf(T(lang, "confessional.diagnostic.message.missing_parent.optional"), entity.NivellConfessionalCodi)
			}
			alerts = append(alerts, decorateConfessionalDiagnosticAlert(lang, alert))
		}
		if len(parents) > 1 {
			parentNames := make([]string, 0, len(parents))
			for _, rel := range parents {
				if parent, ok := publishedEntitatsByID[rel.EntitatOrigenID]; ok {
					parentNames = append(parentNames, parent.Nom)
				} else {
					parentNames = append(parentNames, strconv.Itoa(rel.EntitatOrigenID))
				}
			}
			alerts = append(alerts, decorateConfessionalDiagnosticAlert(lang, confessionalDiagnosticAlert{
				Severity:     confDiagnosticSeverityCritical,
				Type:         confDiagnosticTypeMultipleParents,
				TypeLabel:    T(lang, "confessional.diagnostic.type."+confDiagnosticTypeMultipleParents),
				Description:  fmt.Sprintf(T(lang, "confessional.diagnostic.message.multiple_parents"), strings.Join(parentNames, ", ")),
				Subject:      entity.Nom,
				SubjectURL:   fmt.Sprintf("/confessional/entitats/%d", entity.ID),
				Context:      strings.TrimSpace(strings.Join(parentNames, ", ")),
				ReligionCode: entity.ReligioConfessioCodi,
				LevelCode:    entity.NivellConfessionalCodi,
			}))
		}
		if confessionalDiagnosticIsLocalTerritorialEntity(entity) && len(publishedTerritoriByEntitat[entity.ID]) == 0 {
			parentName := "-"
			if len(parents) > 0 {
				if parent, ok := publishedEntitatsByID[parents[0].EntitatOrigenID]; ok {
					parentName = parent.Nom
				}
			}
			inferredMunicipi := confessionalDiagnosticInferredMunicipi(entity.Nom, publishedMunicipis)
			description := fmt.Sprintf(T(lang, "confessional.diagnostic.message.local_entity_without_territory"), levelLabels[entity.NivellConfessionalCodi])
			context := parentName
			if inferredMunicipi != "" {
				context = fmt.Sprintf("%s · %s: %s", parentName, T(lang, "confessional.diagnostic.context.inferred_municipality"), inferredMunicipi)
			}
			alerts = append(alerts, decorateConfessionalDiagnosticAlert(lang, confessionalDiagnosticAlert{
				Severity:     confDiagnosticSeverityWarning,
				Type:         confDiagnosticTypeLocalEntityWithoutTerritory,
				TypeLabel:    T(lang, "confessional.diagnostic.type."+confDiagnosticTypeLocalEntityWithoutTerritory),
				Description:  description,
				Subject:      entity.Nom,
				SubjectURL:   fmt.Sprintf("/confessional/entitats/%d", entity.ID),
				Context:      context,
				ContextURL:   fmt.Sprintf("/confessional/municipis-entitats/new?return_to=/confessional/entitats/%d", entity.ID),
				ReligionCode: entity.ReligioConfessioCodi,
				LevelCode:    entity.NivellConfessionalCodi,
			}))
		}
	}

	for _, rel := range publishedRelacionsEntitats {
		parent, parentOK := entitatsByID[rel.EntitatOrigenID]
		child, childOK := entitatsByID[rel.EntitatDestiID]
		if !confessionalDiagnosticRelationMatchesScope(filter, parent, parentOK, child, childOK) {
			continue
		}
		issues := make([]string, 0, 6)
		if !parentOK {
			issues = append(issues, T(lang, "confessional.diagnostic.message.issue.parent_missing"))
		}
		if !childOK {
			issues = append(issues, T(lang, "confessional.diagnostic.message.issue.child_missing"))
		}
		if parentOK && childOK {
			if parent.ModeracioEstat != "publicat" {
				issues = append(issues, T(lang, "confessional.diagnostic.message.issue.parent_unpublished"))
			}
			if child.ModeracioEstat != "publicat" {
				issues = append(issues, fmt.Sprintf(T(lang, "confessional.diagnostic.message.issue.child_status"), child.ModeracioEstat))
			}
			if parent.ID == child.ID {
				issues = append(issues, T(lang, "confessional.diagnostic.message.issue.self_relation"))
			}
			if err := validateConfessionalEntityRelation(&parent, &child); err != nil {
				issues = append(issues, err.Error())
			}
			if rel.TipusRelacio != "" && child.NivellConfessionalCodi != "" && rel.TipusRelacio != child.NivellConfessionalCodi {
				issues = append(issues, fmt.Sprintf(T(lang, "confessional.diagnostic.message.issue.relation_type"), rel.TipusRelacio, child.NivellConfessionalCodi))
			}
		}
		if confessionalRelationWouldCreateCycle(rel.EntitatOrigenID, rel.EntitatDestiID, publishedRelacionsEntitats, rel.ID) {
			issues = append(issues, T(lang, "confessional.diagnostic.message.issue.cycle"))
		}
		if len(issues) == 0 {
			continue
		}
		subject := fmt.Sprintf("%s -> %s", confessionalDiagnosticEntityLabel(parent, parentOK, rel.EntitatOrigenID), confessionalDiagnosticEntityLabel(child, childOK, rel.EntitatDestiID))
		subjectURL := ""
		if childOK {
			subjectURL = fmt.Sprintf("/confessional/entitats/%d", child.ID)
		}
		religionCode := ""
		levelCode := ""
		if childOK {
			religionCode = child.ReligioConfessioCodi
			levelCode = child.NivellConfessionalCodi
		} else if parentOK {
			religionCode = parent.ReligioConfessioCodi
			levelCode = parent.NivellConfessionalCodi
		}
		alerts = append(alerts, decorateConfessionalDiagnosticAlert(lang, confessionalDiagnosticAlert{
			Severity:     confDiagnosticSeverityCritical,
			Type:         confDiagnosticTypeIncompatibleRelation,
			TypeLabel:    T(lang, "confessional.diagnostic.type."+confDiagnosticTypeIncompatibleRelation),
			Description:  strings.Join(uniqueConfessionalDiagnosticStrings(issues), "; "),
			Subject:      subject,
			SubjectURL:   subjectURL,
			Context:      rel.TipusRelacio,
			ReligionCode: religionCode,
			LevelCode:    levelCode,
		}))
	}

	dependentByChild, duplicateDependentChildren := a.initialPendingConfessionalParentRelations(allEntitats, allRelacionsEntitats)
	for childID := range duplicateDependentChildren {
		child, ok := entitatsByID[childID]
		if !ok || !confessionalDiagnosticEntityMatchesScope(filter, child) {
			continue
		}
		alerts = append(alerts, decorateConfessionalDiagnosticAlert(lang, confessionalDiagnosticAlert{
			Severity:     confDiagnosticSeverityCritical,
			Type:         confDiagnosticTypePendingRelationInconsistent,
			TypeLabel:    T(lang, "confessional.diagnostic.type."+confDiagnosticTypePendingRelationInconsistent),
			Description:  T(lang, "confessional.diagnostic.message.pending_relation.duplicate"),
			Subject:      child.Nom,
			SubjectURL:   fmt.Sprintf("/confessional/entitats/%d", child.ID),
			ReligionCode: child.ReligioConfessioCodi,
			LevelCode:    child.NivellConfessionalCodi,
		}))
	}

	for _, rel := range allRelacionsEntitats {
		child, childOK := entitatsByID[rel.EntitatDestiID]
		parent, parentOK := entitatsByID[rel.EntitatOrigenID]
		if childOK && !confessionalDiagnosticEntityMatchesScope(filter, child) && !(parentOK && confessionalDiagnosticEntityMatchesScope(filter, parent)) {
			continue
		}
		if rel.ModeracioEstat == "publicat" && childOK && child.ModeracioEstat == "rebutjat" {
			alerts = append(alerts, decorateConfessionalDiagnosticAlert(lang, confessionalDiagnosticAlert{
				Severity:     confDiagnosticSeverityCritical,
				Type:         confDiagnosticTypePendingRelationInconsistent,
				TypeLabel:    T(lang, "confessional.diagnostic.type."+confDiagnosticTypePendingRelationInconsistent),
				Description:  T(lang, "confessional.diagnostic.message.pending_relation.published_rejected_child"),
				Subject:      confessionalDiagnosticEntityLabel(child, childOK, rel.EntitatDestiID),
				SubjectURL:   fmt.Sprintf("/confessional/entitats/%d", rel.EntitatDestiID),
				Context:      confessionalDiagnosticEntityLabel(parent, parentOK, rel.EntitatOrigenID),
				ReligionCode: child.ReligioConfessioCodi,
				LevelCode:    child.NivellConfessionalCodi,
			}))
		}
		if rel.ModeracioEstat != "pendent" || !childOK {
			continue
		}
		if child.ModeracioEstat == "publicat" {
			alerts = append(alerts, decorateConfessionalDiagnosticAlert(lang, confessionalDiagnosticAlert{
				Severity:     confDiagnosticSeverityCritical,
				Type:         confDiagnosticTypePendingRelationInconsistent,
				TypeLabel:    T(lang, "confessional.diagnostic.type."+confDiagnosticTypePendingRelationInconsistent),
				Description:  T(lang, "confessional.diagnostic.message.pending_relation.child_published"),
				Subject:      child.Nom,
				SubjectURL:   fmt.Sprintf("/confessional/entitats/%d", child.ID),
				Context:      confessionalDiagnosticEntityLabel(parent, parentOK, rel.EntitatOrigenID),
				ReligionCode: child.ReligioConfessioCodi,
				LevelCode:    child.NivellConfessionalCodi,
			}))
			continue
		}
		if child.ModeracioEstat == "rebutjat" {
			alerts = append(alerts, decorateConfessionalDiagnosticAlert(lang, confessionalDiagnosticAlert{
				Severity:     confDiagnosticSeverityCritical,
				Type:         confDiagnosticTypePendingRelationInconsistent,
				TypeLabel:    T(lang, "confessional.diagnostic.type."+confDiagnosticTypePendingRelationInconsistent),
				Description:  T(lang, "confessional.diagnostic.message.pending_relation.child_rejected"),
				Subject:      child.Nom,
				SubjectURL:   fmt.Sprintf("/confessional/entitats/%d", child.ID),
				Context:      confessionalDiagnosticEntityLabel(parent, parentOK, rel.EntitatOrigenID),
				ReligionCode: child.ReligioConfessioCodi,
				LevelCode:    child.NivellConfessionalCodi,
			}))
			continue
		}
		if hiddenRel, ok := dependentByChild[child.ID]; ok && hiddenRel.ID == rel.ID {
			if err := a.validateInitialConfessionalParentRelationForModeration(&rel, &child); err != nil {
				alerts = append(alerts, decorateConfessionalDiagnosticAlert(lang, confessionalDiagnosticAlert{
					Severity:     confDiagnosticSeverityCritical,
					Type:         confDiagnosticTypePendingRelationInconsistent,
					TypeLabel:    T(lang, "confessional.diagnostic.type."+confDiagnosticTypePendingRelationInconsistent),
					Description:  fmt.Sprintf(T(lang, "confessional.diagnostic.message.pending_relation.hidden_invalid"), err.Error()),
					Subject:      child.Nom,
					SubjectURL:   fmt.Sprintf("/confessional/entitats/%d", child.ID),
					Context:      confessionalDiagnosticEntityLabel(parent, parentOK, rel.EntitatOrigenID),
					ReligionCode: child.ReligioConfessioCodi,
					LevelCode:    child.NivellConfessionalCodi,
				}))
			}
		}
	}

	for _, municipi := range publishedMunicipis {
		if !confessionalDiagnosticMunicipiCoverageEligible(municipi) {
			continue
		}
		if filter.ReligionCode != "" || filter.LevelCode != "" {
			// Els buits de cobertura territorial sense religio assignable segura només es mostren en vista global.
			continue
		}
		if localCoverageByMunicipi[municipi.ID] {
			continue
		}
		alerts = append(alerts, decorateConfessionalDiagnosticAlert(lang, confessionalDiagnosticAlert{
			Severity:    confDiagnosticSeverityWarning,
			Type:        confDiagnosticTypeMunicipalityWithoutLocalUnit,
			TypeLabel:   T(lang, "confessional.diagnostic.type."+confDiagnosticTypeMunicipalityWithoutLocalUnit),
			Description: T(lang, "confessional.diagnostic.message.municipality_without_local_entity"),
			Subject:     municipi.Nom,
			SubjectURL:  "/confessional/municipis-entitats",
			Context:     municipi.Tipus,
			ContextURL:  "/confessional/municipis-entitats",
		}))
	}

	for _, arxiu := range publishedArxius {
		name := strings.TrimSpace(arxiu.Nom)
		if !confessionalDiagnosticLooksReligiousArchive(name) {
			continue
		}
		rels := publishedArxiuByArxiuID[arxiu.ID]
		if len(rels) == 0 {
			if filter.ReligionCode != "" || filter.LevelCode != "" {
				continue
			}
			alerts = append(alerts, decorateConfessionalDiagnosticAlert(lang, confessionalDiagnosticAlert{
				Severity:    confDiagnosticSeverityWarning,
				Type:        confDiagnosticTypeArchiveWithoutContext,
				TypeLabel:   T(lang, "confessional.diagnostic.type."+confDiagnosticTypeArchiveWithoutContext),
				Description: T(lang, "confessional.diagnostic.message.archive_without_context"),
				Subject:     name,
				SubjectURL:  fmt.Sprintf("/documentals/arxius/%d", arxiu.ID),
				Context:     T(lang, "confessional.diagnostic.context.no_religious_relation"),
				ContextURL:  fmt.Sprintf("/documentals/arxius/%d", arxiu.ID),
			}))
			continue
		}

		if confessionalDiagnosticLooksParishArchive(name) {
			hasLocal := false
			for _, rel := range rels {
				if entitat, ok := publishedEntitatsByID[rel.EntitatReligiosaID]; ok && confessionalDiagnosticIsLocalTerritorialEntity(entitat) {
					hasLocal = true
					break
				}
			}
			if !hasLocal && confessionalDiagnosticArchiveRelationsMatchScope(filter, rels, publishedEntitatsByID) {
				alerts = append(alerts, decorateConfessionalDiagnosticAlert(lang, confessionalDiagnosticAlert{
					Severity:    confDiagnosticSeverityWarning,
					Type:        confDiagnosticTypeArchiveContextAmbiguous,
					TypeLabel:   T(lang, "confessional.diagnostic.type."+confDiagnosticTypeArchiveContextAmbiguous),
					Description: T(lang, "confessional.diagnostic.message.archive_local_context_missing"),
					Subject:     name,
					SubjectURL:  fmt.Sprintf("/documentals/arxius/%d", arxiu.ID),
					Context:     confessionalDiagnosticArchiveRelationSummary(rels, publishedEntitatsByID),
					ContextURL:  fmt.Sprintf("/documentals/arxius/%d", arxiu.ID),
				}))
			}
		}

		if confessionalDiagnosticArchiveRelationIsAmbiguous(rels) && confessionalDiagnosticArchiveRelationsMatchScope(filter, rels, publishedEntitatsByID) {
			alerts = append(alerts, decorateConfessionalDiagnosticAlert(lang, confessionalDiagnosticAlert{
				Severity:    confDiagnosticSeverityWarning,
				Type:        confDiagnosticTypeArchiveContextAmbiguous,
				TypeLabel:   T(lang, "confessional.diagnostic.type."+confDiagnosticTypeArchiveContextAmbiguous),
				Description: T(lang, "confessional.diagnostic.message.archive_context_ambiguous"),
				Subject:     name,
				SubjectURL:  fmt.Sprintf("/documentals/arxius/%d", arxiu.ID),
				Context:     confessionalDiagnosticArchiveRelationSummary(rels, publishedEntitatsByID),
				ContextURL:  fmt.Sprintf("/documentals/arxius/%d", arxiu.ID),
			}))
		}
	}

	for _, duplicate := range confessionalDiagnosticDuplicateAlerts(lang, filter, publishedEntitats, publishedParentsByChild, religionLabels) {
		alerts = append(alerts, duplicate)
	}

	sort.Slice(alerts, func(i, j int) bool {
		if confessionalDiagnosticSeverityRank(alerts[i].Severity) != confessionalDiagnosticSeverityRank(alerts[j].Severity) {
			return confessionalDiagnosticSeverityRank(alerts[i].Severity) < confessionalDiagnosticSeverityRank(alerts[j].Severity)
		}
		if alerts[i].Type != alerts[j].Type {
			return alerts[i].Type < alerts[j].Type
		}
		return alerts[i].Subject < alerts[j].Subject
	})

	filteredAlerts := make([]confessionalDiagnosticAlert, 0, len(alerts))
	for _, alert := range alerts {
		if confessionalDiagnosticAlertMatchesFilter(filter, alert) {
			filteredAlerts = append(filteredAlerts, alert)
		}
	}

	summary := confessionalDiagnosticSummary{}
	for _, item := range allEntitats {
		if !confessionalDiagnosticEntityMatchesScope(filter, item) {
			continue
		}
		switch item.ModeracioEstat {
		case "publicat":
			summary.PublishedEntities++
		case "pendent":
			summary.PendingEntities++
		}
	}
	for _, rel := range publishedRelacionsEntitats {
		parent, parentOK := entitatsByID[rel.EntitatOrigenID]
		child, childOK := entitatsByID[rel.EntitatDestiID]
		if confessionalDiagnosticRelationMatchesScope(filter, parent, parentOK, child, childOK) {
			summary.PublishedHierarchyRelations++
		}
	}
	for _, rel := range publishedRelacionsTerritori {
		entitat, ok := entitatsByID[rel.EntitatReligiosaID]
		if ok && confessionalDiagnosticEntityMatchesScope(filter, entitat) {
			summary.PublishedTerritorialRelations++
		}
	}
	for _, rel := range publishedRelacionsArxiu {
		entitat, ok := entitatsByID[rel.EntitatReligiosaID]
		if ok && confessionalDiagnosticEntityMatchesScope(filter, entitat) {
			summary.PublishedArchiveRelations++
		}
	}
	for _, alert := range filteredAlerts {
		switch alert.Severity {
		case confDiagnosticSeverityCritical:
			summary.CriticalAlerts++
		case confDiagnosticSeverityWarning:
			summary.WarningAlerts++
		case confDiagnosticSeverityInfo:
			summary.InfoAlerts++
		}
	}

	coverageRows := confessionalDiagnosticCoverageRows(filter, publishedEntitats, publishedRelacionsEntitats, publishedRelacionsTerritori, publishedRelacionsArxiu, alerts, religionLabels)

	return confessionalDiagnosticPageData{
		Summary:      summary,
		Alerts:       filteredAlerts,
		CoverageRows: coverageRows,
	}
}

func publishedArxiuEntitatsReligiosesForDiagnostic(items []db.ArxiuEntitatReligiosa) []db.ArxiuEntitatReligiosa {
	out := make([]db.ArxiuEntitatReligiosa, 0, len(items))
	for _, item := range items {
		if item.ModeracioEstat == "publicat" && (item.Estat == "" || item.Estat == "actiu") {
			out = append(out, item)
		}
	}
	return out
}

func publishedMunicipisForDiagnostic(items []db.MunicipiRow) []db.MunicipiRow {
	out := make([]db.MunicipiRow, 0, len(items))
	for _, item := range items {
		if item.ModeracioEstat == "publicat" {
			out = append(out, item)
		}
	}
	return out
}

func confessionalDiagnosticEntityMatchesScope(filter confessionalDiagnosticFilter, entity db.EntitatReligiosa) bool {
	if filter.ReligionCode != "" && entity.ReligioConfessioCodi != filter.ReligionCode {
		return false
	}
	if filter.LevelCode != "" && entity.NivellConfessionalCodi != filter.LevelCode {
		return false
	}
	return true
}

func confessionalDiagnosticRelationMatchesScope(filter confessionalDiagnosticFilter, parent db.EntitatReligiosa, parentOK bool, child db.EntitatReligiosa, childOK bool) bool {
	if filter.ReligionCode == "" && filter.LevelCode == "" {
		return true
	}
	if childOK && confessionalDiagnosticEntityMatchesScope(filter, child) {
		return true
	}
	if parentOK && confessionalDiagnosticEntityMatchesScope(filter, parent) {
		return true
	}
	return false
}

func decorateConfessionalDiagnosticAlert(lang string, alert confessionalDiagnosticAlert) confessionalDiagnosticAlert {
	alert.SeverityLabel = T(lang, "confessional.diagnostic.severity."+alert.Severity)
	alert.SeverityClass = alert.Severity
	if strings.TrimSpace(alert.TypeLabel) == "" {
		alert.TypeLabel = T(lang, "confessional.diagnostic.type."+alert.Type)
	}
	return alert
}

func confessionalDiagnosticSeverityRank(severity string) int {
	switch severity {
	case confDiagnosticSeverityCritical:
		return 0
	case confDiagnosticSeverityWarning:
		return 1
	default:
		return 2
	}
}

func confessionalDiagnosticAlertMatchesFilter(filter confessionalDiagnosticFilter, alert confessionalDiagnosticAlert) bool {
	if filter.Severity != "" && alert.Severity != filter.Severity {
		return false
	}
	if filter.AlertType != "" && alert.Type != filter.AlertType {
		return false
	}
	if filter.ReligionCode != "" && alert.ReligionCode != "" && alert.ReligionCode != filter.ReligionCode {
		return false
	}
	if filter.LevelCode != "" && alert.LevelCode != "" && alert.LevelCode != filter.LevelCode {
		return false
	}
	if filter.Query == "" {
		return true
	}
	hay := normalizeConfessionalSearchComparable(strings.Join([]string{alert.TypeLabel, alert.Description, alert.Subject, alert.Context}, " "))
	for _, token := range strings.Fields(normalizeConfessionalSearchComparable(filter.Query)) {
		if !strings.Contains(hay, token) {
			return false
		}
	}
	return true
}

func confessionalDiagnosticEntityLabel(entity db.EntitatReligiosa, ok bool, fallbackID int) string {
	if ok {
		return entity.Nom
	}
	return fmt.Sprintf("#%d", fallbackID)
}

func confessionalDiagnosticIsLocalTerritorialEntity(entity db.EntitatReligiosa) bool {
	level, ok := GetConfessionalLevelCatalogByCode(entity.NivellConfessionalCodi)
	if !ok || !level.Active || !level.CanLinkMunicipi {
		return false
	}
	switch level.CategoryCode {
	case "territorial_local", "lloc_de_culte", "unitat_pastoral", "comunitat_religiosa":
		return true
	default:
		return false
	}
}

func confessionalDiagnosticMunicipiCoverageEligible(item db.MunicipiRow) bool {
	switch strings.TrimSpace(strings.ToLower(item.Tipus)) {
	case "municipi", "nucli", "nucli_urba", "nucli_rural", "entitat_poblacio":
		return true
	default:
		return strings.Contains(strings.ToLower(item.Tipus), "nucli")
	}
}

func confessionalDiagnosticLooksReligiousArchive(name string) bool {
	hay := strings.ToLower(strings.TrimSpace(name))
	for _, token := range []string{
		"parroquial", "parròquia", "parroquia", "diocesà", "diocesa", "diocesana", "diocesà",
		"arxidiocesà", "arxidiocesa", "arxidiocesana", "eclesiàstic", "eclesiastic",
		"església", "esglesia", "bisbat", "arquebisbat",
	} {
		if strings.Contains(hay, token) {
			return true
		}
	}
	return false
}

func confessionalDiagnosticLooksParishArchive(name string) bool {
	hay := strings.ToLower(strings.TrimSpace(name))
	for _, token := range []string{"parroquial", "parròquia", "parroquia", "església", "esglesia"} {
		if strings.Contains(hay, token) {
			return true
		}
	}
	return false
}

func confessionalDiagnosticArchiveRelationSummary(rels []db.ArxiuEntitatReligiosa, entitatsByID map[int]db.EntitatReligiosa) string {
	parts := make([]string, 0, len(rels))
	for _, rel := range rels {
		entitat := fmt.Sprintf("#%d", rel.EntitatReligiosaID)
		if item, ok := entitatsByID[rel.EntitatReligiosaID]; ok {
			entitat = item.Nom
		}
		parts = append(parts, strings.TrimSpace(entitat+" ("+rel.TipusRelacio+")"))
	}
	return strings.Join(parts, ", ")
}

func confessionalDiagnosticArchiveRelationIsAmbiguous(rels []db.ArxiuEntitatReligiosa) bool {
	if len(rels) <= 1 {
		return false
	}
	genericOnly := true
	for _, rel := range rels {
		switch rel.TipusRelacio {
		case "context_religios", "altres", "":
		default:
			genericOnly = false
		}
	}
	return genericOnly
}

func confessionalDiagnosticArchiveRelationsMatchScope(filter confessionalDiagnosticFilter, rels []db.ArxiuEntitatReligiosa, entitatsByID map[int]db.EntitatReligiosa) bool {
	if filter.ReligionCode == "" && filter.LevelCode == "" {
		return true
	}
	for _, rel := range rels {
		if entitat, ok := entitatsByID[rel.EntitatReligiosaID]; ok && confessionalDiagnosticEntityMatchesScope(filter, entitat) {
			return true
		}
	}
	return false
}

func confessionalDiagnosticDuplicateAlerts(lang string, filter confessionalDiagnosticFilter, publishedEntitats []db.EntitatReligiosa, parentsByChild map[int][]db.EntitatReligiosaRelacio, religionLabels map[string]string) []confessionalDiagnosticAlert {
	alerts := []confessionalDiagnosticAlert{}
	byCode := map[string][]db.EntitatReligiosa{}
	byName := map[string][]db.EntitatReligiosa{}
	byLooseName := map[string][]db.EntitatReligiosa{}
	for _, entity := range publishedEntitats {
		if !confessionalDiagnosticEntityMatchesScope(filter, entity) {
			continue
		}
		if code := strings.ToLower(strings.TrimSpace(entity.Codi)); code != "" {
			byCode[code] = append(byCode[code], entity)
		}
		if key := entity.ReligioConfessioCodi + "|" + confessionalDiagnosticStrictNameKey(entity.Nom); key != "|" {
			byName[key] = append(byName[key], entity)
		}
		parentKey := "0"
		if parents := parentsByChild[entity.ID]; len(parents) > 0 {
			parentKey = strconv.Itoa(parents[0].EntitatOrigenID)
		}
		if loose := confessionalDiagnosticLooseNameKey(entity.Nom); loose != "" {
			byLooseName[entity.ReligioConfessioCodi+"|"+parentKey+"|"+loose] = append(byLooseName[entity.ReligioConfessioCodi+"|"+parentKey+"|"+loose], entity)
		}
	}
	for code, group := range byCode {
		if len(group) < 2 {
			continue
		}
		alerts = append(alerts, decorateConfessionalDiagnosticAlert(lang, confessionalDiagnosticAlert{
			Severity:     confDiagnosticSeverityWarning,
			Type:         confDiagnosticTypePossibleDuplicate,
			TypeLabel:    T(lang, "confessional.diagnostic.type."+confDiagnosticTypePossibleDuplicate),
			Description:  fmt.Sprintf(T(lang, "confessional.diagnostic.message.duplicate.code"), strings.Join(confessionalDiagnosticEntityNames(group), ", ")),
			Subject:      code,
			SubjectURL:   fmt.Sprintf("/confessional/entitats/%d", group[0].ID),
			Context:      religionLabels[group[0].ReligioConfessioCodi],
			ReligionCode: group[0].ReligioConfessioCodi,
			LevelCode:    group[0].NivellConfessionalCodi,
		}))
	}
	for _, group := range byName {
		if len(group) < 2 {
			continue
		}
		alerts = append(alerts, decorateConfessionalDiagnosticAlert(lang, confessionalDiagnosticAlert{
			Severity:     confDiagnosticSeverityWarning,
			Type:         confDiagnosticTypePossibleDuplicate,
			TypeLabel:    T(lang, "confessional.diagnostic.type."+confDiagnosticTypePossibleDuplicate),
			Description:  fmt.Sprintf(T(lang, "confessional.diagnostic.message.duplicate.name"), strings.Join(confessionalDiagnosticEntityNames(group), ", ")),
			Subject:      group[0].Nom,
			SubjectURL:   fmt.Sprintf("/confessional/entitats/%d", group[0].ID),
			Context:      religionLabels[group[0].ReligioConfessioCodi],
			ReligionCode: group[0].ReligioConfessioCodi,
			LevelCode:    group[0].NivellConfessionalCodi,
		}))
	}
	for _, group := range byLooseName {
		if len(group) < 2 || confessionalDiagnosticAllSameName(group) {
			continue
		}
		alerts = append(alerts, decorateConfessionalDiagnosticAlert(lang, confessionalDiagnosticAlert{
			Severity:     confDiagnosticSeverityWarning,
			Type:         confDiagnosticTypePossibleDuplicate,
			TypeLabel:    T(lang, "confessional.diagnostic.type."+confDiagnosticTypePossibleDuplicate),
			Description:  fmt.Sprintf(T(lang, "confessional.diagnostic.message.duplicate.similar"), strings.Join(confessionalDiagnosticEntityNames(group), ", ")),
			Subject:      group[0].Nom,
			SubjectURL:   fmt.Sprintf("/confessional/entitats/%d", group[0].ID),
			Context:      religionLabels[group[0].ReligioConfessioCodi],
			ReligionCode: group[0].ReligioConfessioCodi,
			LevelCode:    group[0].NivellConfessionalCodi,
		}))
	}
	return alerts
}

func confessionalDiagnosticEntityNames(items []db.EntitatReligiosa) []string {
	names := make([]string, 0, len(items))
	for _, item := range items {
		names = append(names, item.Nom)
	}
	sort.Strings(names)
	return uniqueConfessionalDiagnosticStrings(names)
}

func confessionalDiagnosticStrictNameKey(name string) string {
	return normalizeConfessionalSearchComparable(name)
}

func confessionalDiagnosticLooseNameKey(name string) string {
	tokens := strings.Fields(normalizeConfessionalSearchComparable(name))
	if len(tokens) == 0 {
		return ""
	}
	stopWords := map[string]bool{
		"de": true, "del": true, "dels": true, "la": true, "el": true, "els": true, "les": true,
		"da": true, "do": true, "di": true, "du": true, "des": true, "of": true, "the": true,
	}
	kept := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if stopWords[token] {
			continue
		}
		kept = append(kept, token)
	}
	if len(kept) == 0 {
		return strings.Join(tokens, " ")
	}
	return strings.Join(kept, " ")
}

func confessionalDiagnosticAllSameName(items []db.EntitatReligiosa) bool {
	if len(items) < 2 {
		return true
	}
	first := confessionalDiagnosticStrictNameKey(items[0].Nom)
	for i := 1; i < len(items); i++ {
		if confessionalDiagnosticStrictNameKey(items[i].Nom) != first {
			return false
		}
	}
	return true
}

func uniqueConfessionalDiagnosticStrings(values []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func confessionalDiagnosticCoverageRows(filter confessionalDiagnosticFilter, publishedEntitats []db.EntitatReligiosa, publishedRelacionsEntitats []db.EntitatReligiosaRelacio, publishedRelacionsTerritori []db.MunicipiEntitatReligiosa, publishedRelacionsArxiu []db.ArxiuEntitatReligiosa, alerts []confessionalDiagnosticAlert, religionLabels map[string]string) []confessionalDiagnosticCoverageRow {
	rowsByReligion := map[string]*confessionalDiagnosticCoverageRow{}
	for _, religion := range ListSelectableConfessionalReligionCatalog() {
		if filter.ReligionCode != "" && filter.ReligionCode != religion.Code {
			continue
		}
		rowsByReligion[religion.Code] = &confessionalDiagnosticCoverageRow{
			ReligionCode:  religion.Code,
			ReligionLabel: religionLabels[religion.Code],
		}
	}
	for _, entity := range publishedEntitats {
		if !confessionalDiagnosticEntityMatchesScope(filter, entity) {
			continue
		}
		row := rowsByReligion[entity.ReligioConfessioCodi]
		if row == nil {
			continue
		}
		row.PublishedEntities++
	}
	entityByID := confessionalEntitatsByID(publishedEntitats)
	for _, rel := range publishedRelacionsEntitats {
		entity, ok := entityByID[rel.EntitatDestiID]
		if !ok || !confessionalDiagnosticEntityMatchesScope(filter, entity) {
			continue
		}
		if row := rowsByReligion[entity.ReligioConfessioCodi]; row != nil {
			row.HierarchyRelations++
		}
	}
	for _, rel := range publishedRelacionsTerritori {
		entity, ok := entityByID[rel.EntitatReligiosaID]
		if !ok || !confessionalDiagnosticEntityMatchesScope(filter, entity) {
			continue
		}
		if row := rowsByReligion[entity.ReligioConfessioCodi]; row != nil {
			row.TerritorialRelations++
		}
	}
	for _, rel := range publishedRelacionsArxiu {
		entity, ok := entityByID[rel.EntitatReligiosaID]
		if !ok || !confessionalDiagnosticEntityMatchesScope(filter, entity) {
			continue
		}
		if row := rowsByReligion[entity.ReligioConfessioCodi]; row != nil {
			row.ArchiveRelations++
		}
	}
	for _, alert := range alerts {
		row := rowsByReligion[alert.ReligionCode]
		if row == nil {
			continue
		}
		switch alert.Severity {
		case confDiagnosticSeverityCritical:
			row.CriticalAlerts++
		case confDiagnosticSeverityWarning:
			row.WarningAlerts++
		}
	}
	rows := make([]confessionalDiagnosticCoverageRow, 0, len(rowsByReligion))
	for _, row := range rowsByReligion {
		if row.PublishedEntities == 0 && row.HierarchyRelations == 0 && row.TerritorialRelations == 0 && row.ArchiveRelations == 0 && row.CriticalAlerts == 0 && row.WarningAlerts == 0 {
			continue
		}
		rows = append(rows, *row)
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].ReligionLabel < rows[j].ReligionLabel
	})
	return rows
}

func confessionalDiagnosticInferredMunicipi(name string, municipis []db.MunicipiRow) string {
	needle := normalizeConfessionalSearchComparable(name)
	matches := []string{}
	for _, municipi := range municipis {
		if !confessionalDiagnosticMunicipiCoverageEligible(municipi) {
			continue
		}
		label := normalizeConfessionalSearchComparable(municipi.Nom)
		if label == "" {
			continue
		}
		if strings.Contains(needle, label) {
			matches = append(matches, municipi.Nom)
		}
	}
	matches = uniqueConfessionalDiagnosticStrings(matches)
	if len(matches) == 1 {
		return matches[0]
	}
	return ""
}
