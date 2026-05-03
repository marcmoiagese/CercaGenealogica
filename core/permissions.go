package core

import (
	"context"
	"net/http"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type permContextKey string

const permissionsKey permContextKey = "permissions"

type userContextKey string

const userKey userContextKey = "user"

type unreadMessagesContextKey string

const unreadMessagesKey unreadMessagesContextKey = "unread_messages"

type effectiveAdminContextKey string

const effectiveAdminKey effectiveAdminContextKey = "effective_admin"

// PolicyPermissions is re-exported for convenience
type PolicyPermissions = db.PolicyPermissions

var adminPolicies = []string{"admin", "moderador", "confiança"}

func (a *App) getPermissionsForUser(userID int) db.PolicyPermissions {
	if userID == 0 || a.DB == nil {
		return db.PolicyPermissions{}
	}
	perms, err := a.DB.GetEffectivePoliticaPerms(userID)
	if err != nil {
		Errorf("error carregant permisos per usuari %d: %v", userID, err)
		return db.PolicyPermissions{}
	}
	if perms == (db.PolicyPermissions{}) {
		Debugf("usuari %d sense polítiques assignades; permisos mínims", userID)
	}
	return perms
}

func (a *App) permissionsFromContext(r *http.Request) (db.PolicyPermissions, bool) {
	if val := r.Context().Value(permissionsKey); val != nil {
		if p, ok := val.(db.PolicyPermissions); ok {
			return p, true
		}
	}
	return db.PolicyPermissions{}, false
}

func (a *App) withPermissions(r *http.Request, perms db.PolicyPermissions) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), permissionsKey, perms))
}

func (a *App) withEffectiveAdmin(r *http.Request, isAdmin bool) *http.Request {
	if r == nil {
		return r
	}
	return r.WithContext(context.WithValue(r.Context(), effectiveAdminKey, isAdmin))
}

func effectiveAdminFromContext(r *http.Request) (bool, bool) {
	if r == nil {
		return false, false
	}
	if val := r.Context().Value(effectiveAdminKey); val != nil {
		if isAdmin, ok := val.(bool); ok {
			return isAdmin, true
		}
	}
	return false, false
}

func (a *App) effectiveAdminForUser(userID int, _ db.PolicyPermissions) bool {
	if userID == 0 || a == nil {
		return false
	}
	snap, err := a.getPermissionSnapshot(userID)
	if err != nil {
		return false
	}
	return snap.isAdmin
}

func (a *App) withUser(r *http.Request, u *db.User) *http.Request {
	if u == nil {
		return r
	}
	return r.WithContext(context.WithValue(r.Context(), userKey, u))
}

func (a *App) withUnreadMessagesCount(r *http.Request, count int) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), unreadMessagesKey, count))
}

func userFromContext(r *http.Request) *db.User {
	if r == nil {
		return nil
	}
	if val := r.Context().Value(userKey); val != nil {
		if u, ok := val.(*db.User); ok {
			return u
		}
	}
	return nil
}

func unreadMessagesCountFromContext(r *http.Request) (int, bool) {
	if r == nil {
		return 0, false
	}
	if val := r.Context().Value(unreadMessagesKey); val != nil {
		if count, ok := val.(int); ok {
			return count, true
		}
	}
	return 0, false
}

func (a *App) ensureUnreadMessagesCount(r *http.Request, userID int) *http.Request {
	if r == nil || userID <= 0 {
		return r
	}
	if _, ok := unreadMessagesCountFromContext(r); ok {
		return r
	}
	count := 0
	if a.DB != nil {
		if n, err := a.DB.CountDMUnread(userID); err == nil {
			count = n
		} else {
			Errorf("Error comptant missatges pendents per usuari %d: %v", userID, err)
		}
	}
	return a.withUnreadMessagesCount(r, count)
}

// RequireLogin is a minimal guard without any specific permission.
func (a *App) RequireLogin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if user, ok := a.VerificarSessio(r); ok && user != nil {
			*r = *a.withUser(r, user)
			perms, found := a.permissionsFromContext(r)
			if !found {
				perms = a.getPermissionsForUser(user.ID)
				*r = *a.withPermissions(r, perms)
			}
			if _, found := effectiveAdminFromContext(r); !found {
				*r = *a.withEffectiveAdmin(r, a.effectiveAdminForUser(user.ID, perms))
			}
			*r = *a.ensureUnreadMessagesCount(r, user.ID)
			if _, found := permissionKeysFromContext(r); !found {
				*r = *a.withPermissionKeys(r, a.permissionKeysForUser(user.ID))
			}
			next(w, r)
			return
		}
		http.Redirect(w, r, "/login", http.StatusSeeOther)
	}
}

// Legacy helper still used in alguns handlers; basa en polítiques per nom.
func (a *App) requirePolicies(w http.ResponseWriter, r *http.Request, policies []string) (*db.User, bool) {
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return nil, false
	}
	r = a.withUser(r, user)
	if len(policies) == 0 {
		return user, true
	}
	has, err := a.DB.UserHasAnyPolicy(user.ID, policies)
	if err != nil || !has {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return nil, false
	}
	return user, true
}
