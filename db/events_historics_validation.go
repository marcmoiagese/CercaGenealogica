package db

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	eventHistoricResumMax = 500
	eventHistoricDescMax  = 5000
	eventHistoricFontsMax = 2000
)

var eventHistoricTipus = map[string]struct{}{
	"guerra":           {},
	"conflicte_local":  {},
	"plaga":            {},
	"pesta":            {},
	"pandemia":         {},
	"fam":              {},
	"crisi_economica":  {},
	"revolta":          {},
	"incendi":          {},
	"terratremol":      {},
	"inundacio":        {},
	"assassinat":       {},
	"repressio":        {},
	"migracio_massiva": {},
	"altres":           {},
}

var eventHistoricPrecisio = map[string]struct{}{
	"dia":    {},
	"mes":    {},
	"any":    {},
	"decada": {},
}

var eventHistoricModerationStatus = map[string]struct{}{
	"pendent":  {},
	"publicat": {},
	"rebutjat": {},
}

var eventHistoricScopeTypes = map[string]struct{}{
	"pais":                 {},
	"nivell_admin":         {},
	"municipi":             {},
	"entitat_eclesiastica": {},
}

var eventHistoricImpactTypes = map[string]struct{}{
	"directe":   {},
	"indirecte": {},
	"transit":   {},
	"rumor":     {},
}

func isEventHistoricScopeType(scopeType string) bool {
	_, ok := eventHistoricScopeTypes[scopeType]
	return ok
}

func ValidateEventHistoric(e *EventHistoric) error {
	if e == nil {
		return errors.New("event nil")
	}
	titol := strings.TrimSpace(e.Titol)
	if titol == "" {
		return fmt.Errorf("titol required")
	}
	slug := strings.TrimSpace(e.Slug)
	if slug == "" {
		return fmt.Errorf("slug required")
	}
	tipus := strings.TrimSpace(e.Tipus)
	if tipus == "" {
		return fmt.Errorf("tipus required")
	}
	if _, ok := eventHistoricTipus[tipus]; !ok {
		return fmt.Errorf("tipus invalid")
	}
	status := strings.TrimSpace(e.ModerationStatus)
	if status == "" {
		return fmt.Errorf("moderation_status required")
	}
	if _, ok := eventHistoricModerationStatus[status]; !ok {
		return fmt.Errorf("moderation_status invalid")
	}
	if utf8.RuneCountInString(strings.TrimSpace(e.Resum)) > eventHistoricResumMax {
		return fmt.Errorf("resum too long")
	}
	if utf8.RuneCountInString(strings.TrimSpace(e.Descripcio)) > eventHistoricDescMax {
		return fmt.Errorf("descripcio too long")
	}
	if utf8.RuneCountInString(strings.TrimSpace(e.Fonts)) > eventHistoricFontsMax {
		return fmt.Errorf("fonts too long")
	}
	precisio := strings.TrimSpace(e.Precisio)
	if precisio != "" {
		if _, ok := eventHistoricPrecisio[precisio]; !ok {
			return fmt.Errorf("precisio invalid")
		}
	}
	startRaw := strings.TrimSpace(e.DataInici)
	endRaw := strings.TrimSpace(e.DataFi)
	var start, end time.Time
	if startRaw != "" {
		parsed, err := time.Parse("2006-01-02", startRaw)
		if err != nil {
			return fmt.Errorf("data_inici invalid")
		}
		start = parsed
	}
	if endRaw != "" {
		parsed, err := time.Parse("2006-01-02", endRaw)
		if err != nil {
			return fmt.Errorf("data_fi invalid")
		}
		end = parsed
	}
	if !start.IsZero() && !end.IsZero() && end.Before(start) {
		return fmt.Errorf("data_fi before data_inici")
	}
	return nil
}

func ValidateEventHistoricImpact(i *EventHistoricImpact) error {
	if i == nil {
		return errors.New("impact nil")
	}
	scopeType := strings.TrimSpace(i.ScopeType)
	if scopeType == "" {
		return fmt.Errorf("scope_type required")
	}
	if _, ok := eventHistoricScopeTypes[scopeType]; !ok {
		return fmt.Errorf("scope_type invalid")
	}
	if i.ScopeID <= 0 {
		return fmt.Errorf("scope_id invalid")
	}
	impactType := strings.TrimSpace(i.ImpacteTipus)
	if impactType == "" {
		return fmt.Errorf("impacte_tipus required")
	}
	if _, ok := eventHistoricImpactTypes[impactType]; !ok {
		return fmt.Errorf("impacte_tipus invalid")
	}
	if i.Intensitat < 1 || i.Intensitat > 5 {
		return fmt.Errorf("intensitat invalid")
	}
	return nil
}
