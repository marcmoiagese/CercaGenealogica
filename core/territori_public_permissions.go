package core

import "github.com/marcmoiagese/CercaGenealogica/db"

func (a *App) canModerateMunicipiPublic(user *db.User, target PermissionTarget) bool {
	if user == nil {
		return false
	}
	return a.HasAnyPermission(user.ID, []string{
		permKeyTerritoriMunicipisEdit,
		permKeyTerritoriMunicipisHistoriaModerate,
		permKeyTerritoriMunicipisAnecdotesModerate,
		permKeyTerritoriMunicipisMapesModerate,
	}, target)
}

func (a *App) canEditMunicipiPublic(user *db.User, target PermissionTarget) bool {
	return user != nil && a.HasPermission(user.ID, permKeyTerritoriMunicipisEdit, target)
}

func (a *App) canEditNivellPublic(user *db.User, target PermissionTarget) bool {
	return user != nil && a.HasPermission(user.ID, permKeyTerritoriNivellsEdit, target)
}
