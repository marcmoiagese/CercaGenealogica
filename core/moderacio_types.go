package core

import (
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

func resolveWikiChangeModeracioType(change db.WikiChange) string {
	objType := strings.TrimSpace(change.ObjectType)
	if objType == "" {
		return ""
	}
	if !isValidWikiObjectType(objType) {
		return ""
	}
	return moderacioWikiTypeMap[strings.ToLower(objType)]
}
