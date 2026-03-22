package core

import (
	"sort"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

var moderacioWikiTypeMap = map[string]string{
	"municipi":       "municipi_canvi",
	"arxiu":          "arxiu_canvi",
	"llibre":         "llibre_canvi",
	"persona":        "persona_canvi",
	"cognom":         "cognom_canvi",
	"event_historic": "event_historic_canvi",
}

// Tipus moderats fora del panell i bulk principal (fluxos separats).
var moderacioOutOfBandTypes = map[string]string{
	"municipi_mapa_version": "admin/moderacio/mapes",
	"media_album":           "admin/moderacio/media",
	"media_item":            "admin/moderacio/media",
	"external_link":         "admin/external-links",
}

func moderacioOutOfBandTypeKeys() []string {
	keys := make([]string, 0, len(moderacioOutOfBandTypes))
	for key := range moderacioOutOfBandTypes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func formatModeracioOutOfBandTypes() string {
	keys := moderacioOutOfBandTypeKeys()
	if len(keys) == 0 {
		return ""
	}
	return strings.Join(keys, ",")
}

func resolveWikiChangeModeracioType(change db.WikiChange) (string, bool) {
	if !isValidWikiObjectType(change.ObjectType) {
		return "", false
	}
	objType := moderacioWikiTypeMap[change.ObjectType]
	if objType == "" {
		return "", false
	}
	return objType, true
}
