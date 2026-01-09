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

func (a *App) withUser(r *http.Request, u *db.User) *http.Request {
	if u == nil {
		return r
	}
	return r.WithContext(context.WithValue(r.Context(), userKey, u))
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

func (a *App) hasPerm(perms db.PolicyPermissions, check func(db.PolicyPermissions) bool) bool {
	if perms.Admin {
		return true
	}
	return check(perms)
}

func (a *App) requirePermission(w http.ResponseWriter, r *http.Request, check func(db.PolicyPermissions) bool) (*db.User, db.PolicyPermissions, bool) {
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return nil, db.PolicyPermissions{}, false
	}
	*r = *a.withUser(r, user)
	perms, found := a.permissionsFromContext(r)
	if !found {
		perms = a.getPermissionsForUser(user.ID)
		*r = *a.withPermissions(r, perms)
	}
	if !a.hasPerm(perms, check) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return user, perms, false
	}
	return user, perms, true
}

// Helpers
func permTerritory(p db.PolicyPermissions) bool { return p.CanManageTerritory }
func permEclesia(p db.PolicyPermissions) bool   { return p.CanManageEclesia }
func permArxius(p db.PolicyPermissions) bool    { return p.CanManageArchives }
func permModerate(p db.PolicyPermissions) bool  { return p.CanModerate }
func permPolicies(p db.PolicyPermissions) bool  { return p.CanManagePolicies }
func permAdmin(p db.PolicyPermissions) bool     { return p.Admin }
func permUsers(p db.PolicyPermissions) bool     { return p.CanManageUsers }
func permCreatePerson(p db.PolicyPermissions) bool {
	return p.CanCreatePerson || p.Admin
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
