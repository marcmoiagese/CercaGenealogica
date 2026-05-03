package core

import "github.com/marcmoiagese/CercaGenealogica/db"

func (a *App) canModerateWikiObject(user *db.User, objectType string, objectID int) bool {
	if user == nil {
		return false
	}
	model := a.newModeracioScopeModel(user, false)
	changeType := wikiObjectTypeToModeracioType(objectType)
	if changeType == "" {
		return false
	}
	return model.canModerateWikiChange(db.WikiChange{
		ObjectType: objectType,
		ObjectID:   objectID,
	}, changeType)
}

func wikiObjectTypeToModeracioType(objectType string) string {
	switch objectType {
	case "municipi":
		return "municipi_canvi"
	case "arxiu":
		return "arxiu_canvi"
	case "llibre":
		return "llibre_canvi"
	case "persona":
		return "persona_canvi"
	case "cognom":
		return "cognom_canvi"
	case "event_historic":
		return "event_historic_canvi"
	default:
		return ""
	}
}
