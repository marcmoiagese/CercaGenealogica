package core

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const mapModerationObjectType = "municipi_mapa_version"

type mapModerationItem struct {
	Version    db.MunicipiMapaVersion
	Mapa       db.MunicipiMapa
	Municipi   db.Municipi
	AuthorName string
	AuthorURL  string
	CreatedAt  string
	ViewURL    string
}

func (a *App) requireMapModerationUser(w http.ResponseWriter, r *http.Request) (*db.User, db.PolicyPermissions, bool) {
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
	if !a.hasPerm(perms, permModerate) && !a.hasAnyPermissionKey(user.ID, permKeyTerritoriMunicipisMapesModerate) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return user, perms, false
	}
	return user, perms, true
}

func (a *App) canModerateMap(perms db.PolicyPermissions, user *db.User, munID int) bool {
	if user == nil {
		return false
	}
	if a.hasPerm(perms, permModerate) {
		return true
	}
	target := a.resolveMunicipiTarget(munID)
	return a.HasPermission(user.ID, permKeyTerritoriMunicipisMapesModerate, target)
}

func (a *App) AdminModeracioMapesList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user, perms, ok := a.requireMapModerationUser(w, r)
	if !ok {
		return
	}
	lang := resolveUserLang(r, user)

	msg := ""
	okFlag := false
	if r.URL.Query().Get("ok") != "" {
		okFlag = true
		msg = T(lang, "moderation.success")
	} else if r.URL.Query().Get("err") != "" {
		msg = T(lang, "moderation.error")
	}

	versions, err := a.DB.ListMunicipiMapaVersions(db.MunicipiMapaVersionFilter{
		Status: "pendent",
		Limit:  200,
	})
	if err != nil {
		Errorf("Error carregant moderacio mapes: %v", err)
		versions = []db.MunicipiMapaVersion{}
	}
	mapCache := map[int]*db.MunicipiMapa{}
	munCache := map[int]*db.Municipi{}
	userCache := map[int]*db.User{}
	resolveUser := func(id int) (string, string) {
		if id <= 0 {
			return "-", ""
		}
		if cached, ok := userCache[id]; ok {
			return userDisplayName(cached), "/u/" + strconv.Itoa(cached.ID)
		}
		u, err := a.DB.GetUserByID(id)
		if err != nil || u == nil {
			userCache[id] = nil
			return "-", ""
		}
		userCache[id] = u
		return userDisplayName(u), "/u/" + strconv.Itoa(u.ID)
	}

	items := []mapModerationItem{}
	for _, version := range versions {
		mapa, ok := mapCache[version.MapaID]
		if !ok {
			row, err := a.DB.GetMunicipiMapa(version.MapaID)
			if err != nil || row == nil {
				mapCache[version.MapaID] = nil
				continue
			}
			mapCache[version.MapaID] = row
			mapa = row
		}
		if mapa == nil {
			continue
		}
		mun, ok := munCache[mapa.MunicipiID]
		if !ok {
			row, err := a.DB.GetMunicipi(mapa.MunicipiID)
			if err != nil || row == nil {
				munCache[mapa.MunicipiID] = nil
				continue
			}
			munCache[mapa.MunicipiID] = row
			mun = row
		}
		if mun == nil {
			continue
		}
		if !a.canModerateMap(perms, user, mun.ID) {
			continue
		}
		authorID := 0
		if version.CreatedBy.Valid {
			authorID = int(version.CreatedBy.Int64)
		}
		authorName, authorURL := resolveUser(authorID)
		createdAt := ""
		if version.CreatedAt.Valid {
			createdAt = version.CreatedAt.Time.Format("02/01/2006 15:04")
		}
		viewURL := "/territori/municipis/" + strconv.Itoa(mun.ID) + "/mapes/" + strconv.Itoa(mapa.ID)
		items = append(items, mapModerationItem{
			Version:    version,
			Mapa:       *mapa,
			Municipi:   *mun,
			AuthorName: authorName,
			AuthorURL:  authorURL,
			CreatedAt:  createdAt,
			ViewURL:    viewURL,
		})
	}

	RenderPrivateTemplateLang(w, r, "admin-moderacio-mapes.html", lang, map[string]interface{}{
		"User": user,
		"Msg":  msg,
		"Ok":   okFlag,
		"Items": items,
	})
}

func (a *App) AdminModeracioMapesApprove(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	user, perms, ok := a.requireMapModerationUser(w, r)
	if !ok {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/admin/moderacio/mapes?err=1", http.StatusSeeOther)
		return
	}
	version, err := a.DB.GetMunicipiMapaVersion(id)
	if err != nil || version == nil {
		http.NotFound(w, r)
		return
	}
	mapa, err := a.DB.GetMunicipiMapa(version.MapaID)
	if err != nil || mapa == nil {
		http.NotFound(w, r)
		return
	}
	mun, err := a.DB.GetMunicipi(mapa.MunicipiID)
	if err != nil || mun == nil {
		http.NotFound(w, r)
		return
	}
	if !a.canModerateMap(perms, user, mun.ID) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	notes := strings.TrimSpace(r.FormValue("notes"))
	if err := a.DB.UpdateMunicipiMapaVersionStatus(id, "publicat", notes, user.ID); err != nil {
		Errorf("Moderacio mapa aprovar %d ha fallat: %v", id, err)
		http.Redirect(w, r, "/admin/moderacio/mapes?err=1", http.StatusSeeOther)
		return
	}
	if err := a.DB.UpdateMunicipiMapaCurrentVersion(version.MapaID, id); err != nil {
		Errorf("Moderacio mapa actualitzar current %d ha fallat: %v", id, err)
		http.Redirect(w, r, "/admin/moderacio/mapes?err=1", http.StatusSeeOther)
		return
	}
	if acts, err := a.DB.ListActivityByObject(mapModerationObjectType, id, "pendent"); err == nil {
		for _, act := range acts {
			_ = a.ValidateActivity(act.ID, user.ID)
		}
	}
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleMunicipiMapaApprove, "moderar_aprovar", mapModerationObjectType, &id, "validat", nil, "")
	http.Redirect(w, r, "/admin/moderacio/mapes?ok=1", http.StatusSeeOther)
}

func (a *App) AdminModeracioMapesReject(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	user, perms, ok := a.requireMapModerationUser(w, r)
	if !ok {
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/admin/moderacio/mapes?err=1", http.StatusSeeOther)
		return
	}
	version, err := a.DB.GetMunicipiMapaVersion(id)
	if err != nil || version == nil {
		http.NotFound(w, r)
		return
	}
	mapa, err := a.DB.GetMunicipiMapa(version.MapaID)
	if err != nil || mapa == nil {
		http.NotFound(w, r)
		return
	}
	mun, err := a.DB.GetMunicipi(mapa.MunicipiID)
	if err != nil || mun == nil {
		http.NotFound(w, r)
		return
	}
	if !a.canModerateMap(perms, user, mun.ID) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	notes := strings.TrimSpace(r.FormValue("notes"))
	if err := a.DB.UpdateMunicipiMapaVersionStatus(id, "rebutjat", notes, user.ID); err != nil {
		Errorf("Moderacio mapa rebutjar %d ha fallat: %v", id, err)
		http.Redirect(w, r, "/admin/moderacio/mapes?err=1", http.StatusSeeOther)
		return
	}
	if acts, err := a.DB.ListActivityByObject(mapModerationObjectType, id, "pendent"); err == nil {
		for _, act := range acts {
			_ = a.CancelActivity(act.ID, user.ID)
		}
	}
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleMunicipiMapaReject, "moderar_rebutjar", mapModerationObjectType, &id, "validat", nil, notes)
	http.Redirect(w, r, "/admin/moderacio/mapes?ok=1", http.StatusSeeOther)
}
