package core

import (
	"net/http"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func (a *App) mediaUserRoles(r *http.Request, user *db.User) (bool, bool) {
	if user == nil {
		return false, false
	}
	perms, found := a.permissionsFromContext(r)
	if !found {
		perms = a.getPermissionsForUser(user.ID)
	}
	return a.hasPerm(perms, permAdmin), a.hasPerm(perms, permModerate)
}

func (a *App) requireMediaView(w http.ResponseWriter, r *http.Request) (*db.User, bool) {
	return a.requirePermissionKeyAnyScope(w, r, permKeyMediaView)
}

func (a *App) requireMediaViewIfLogged(w http.ResponseWriter, r *http.Request) (*db.User, bool) {
	return a.requirePermissionKeyIfLogged(w, r, permKeyMediaView)
}

func (a *App) mediaEnsureUser(r *http.Request) *db.User {
	if r == nil {
		return nil
	}
	if user := userFromContext(r); user != nil {
		return user
	}
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		return nil
	}
	*r = *a.withUser(r, user)
	perms, found := a.permissionsFromContext(r)
	if !found {
		perms = a.getPermissionsForUser(user.ID)
		*r = *a.withPermissions(r, perms)
	}
	return user
}

func (a *App) mediaUserIsPrivileged(r *http.Request, user *db.User, album *db.MediaAlbum) bool {
	if album == nil {
		return false
	}
	if user != nil && album.OwnerUserID == user.ID {
		return true
	}
	isAdmin, isModerator := a.mediaUserRoles(r, user)
	return isAdmin || isModerator
}

func (a *App) mediaUserCanAccess(r *http.Request, user *db.User, album *db.MediaAlbum) bool {
	if album == nil {
		return false
	}
	if a.mediaUserIsPrivileged(r, user, album) {
		return true
	}
	if album.ModerationStatus != "approved" {
		return false
	}
	switch album.Visibility {
	case "public":
		return true
	case "registered":
		return user != nil
	case "restricted_group":
		if user == nil || !album.RestrictedGroupID.Valid {
			return false
		}
		return a.mediaUserInGroup(user.ID, int(album.RestrictedGroupID.Int64))
	case "custom_policy":
		if user == nil || !album.AccessPolicyID.Valid {
			return false
		}
		return a.mediaUserHasPolicy(user.ID, int(album.AccessPolicyID.Int64))
	case "admins_only":
		return false
	default:
		return false
	}
}

func (a *App) mediaUserCanAccessItem(r *http.Request, user *db.User, album *db.MediaAlbum, item *db.MediaItem) bool {
	if album == nil || item == nil {
		return false
	}
	if album.AlbumType == "achievement_icon" {
		return true
	}
	if a.mediaUserIsPrivileged(r, user, album) {
		return true
	}
	if !a.mediaUserCanAccess(r, user, album) {
		return false
	}
	return item.ModerationStatus == "approved"
}

func (a *App) mediaUserInGroup(userID int, groupID int) bool {
	if userID <= 0 || groupID <= 0 {
		return false
	}
	groups, err := a.DB.ListUserGroups(userID)
	if err != nil {
		return false
	}
	for _, g := range groups {
		if g.ID == groupID {
			return true
		}
	}
	return false
}

func (a *App) mediaUserHasPolicy(userID int, policyID int) bool {
	if userID <= 0 || policyID <= 0 {
		return false
	}
	policies, err := a.DB.ListUserPolitiques(userID)
	if err == nil {
		for _, p := range policies {
			if p.ID == policyID {
				return true
			}
		}
	}
	groups, err := a.DB.ListUserGroups(userID)
	if err != nil {
		return false
	}
	for _, g := range groups {
		groupPolicies, err := a.DB.ListGroupPolitiques(g.ID)
		if err != nil {
			continue
		}
		for _, p := range groupPolicies {
			if p.ID == policyID {
				return true
			}
		}
	}
	return false
}
