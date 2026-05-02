package core

import "github.com/marcmoiagese/CercaGenealogica/db"

func (a *App) canModeratePersonesPublic(user *db.User) bool {
	return user != nil && a.HasPermission(user.ID, permKeyPersonesModerate, PermissionTarget{})
}

func (a *App) canModerateCognomsPublic(user *db.User) bool {
	return user != nil && a.HasPermission(user.ID, permKeyCognomsModerate, PermissionTarget{})
}

func (a *App) canModerateEventHistoricPublic(user *db.User, eventID int) bool {
	if user == nil {
		return false
	}
	if a.HasPermission(user.ID, permKeyEventsModerate, PermissionTarget{}) {
		return true
	}
	if eventID <= 0 {
		return false
	}
	impacts, err := a.DB.ListEventImpacts(eventID)
	if err != nil {
		Errorf("Error carregant impactes event historic %d per permisos: %v", eventID, err)
		return false
	}
	for _, impact := range impacts {
		if a.HasPermission(user.ID, permKeyEventsModerate, a.eventImpactPermissionTarget(impact)) {
			return true
		}
	}
	return false
}

func (a *App) canViewEventHistoricPublic(user *db.User, event *db.EventHistoric, canModerate bool) bool {
	if event == nil {
		return false
	}
	if event.ModerationStatus == "" || event.ModerationStatus == "publicat" {
		return true
	}
	if user == nil {
		return false
	}
	return canModerate || (event.CreatedBy.Valid && int(event.CreatedBy.Int64) == user.ID)
}

func (a *App) eventImpactPermissionTarget(impact db.EventHistoricImpact) PermissionTarget {
	switch impact.ScopeType {
	case "pais":
		return PermissionTarget{PaisID: intPtr(impact.ScopeID)}
	case "municipi":
		return a.resolveMunicipiTarget(impact.ScopeID)
	case "nivell_admin":
		target := PermissionTarget{NivellIDs: []int{impact.ScopeID}}
		if nivell, err := a.DB.GetNivell(impact.ScopeID); err == nil && nivell != nil && nivell.PaisID > 0 {
			target.PaisID = intPtr(nivell.PaisID)
		}
		return target
	case "entitat_eclesiastica":
		return PermissionTarget{EclesID: intPtr(impact.ScopeID)}
	default:
		return PermissionTarget{}
	}
}
