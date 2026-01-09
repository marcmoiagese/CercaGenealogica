package core

import (
	"database/sql"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

// CanManageArxius és un helper públic per saber si l'usuari pot gestionar arxius.
func (a *App) CanManageArxius(user *db.User) bool {
	if user == nil {
		return false
	}
	perms := a.getPermissionsForUser(user.ID)
	return a.hasPerm(perms, permArxius)
}

// Llistat públic (lectura) d'arxius per a usuaris logats
func (a *App) ListArxius(w http.ResponseWriter, r *http.Request) {
	user, authenticated := a.VerificarSessio(r)
	if !authenticated || user == nil {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	perms := a.getPermissionsForUser(user.ID)
	*r = *a.withPermissions(r, perms)
	canManage := a.hasPerm(perms, permArxius)
	isAdmin := a.hasPerm(perms, permAdmin)
	canManageTerritory := a.hasPerm(perms, permTerritory)
	canManageEclesia := a.hasPerm(perms, permEclesia)
	canModerate := a.hasPerm(perms, permModerate)
	canManageUsers := a.hasPerm(perms, permUsers)
	canManagePolicies := a.hasPerm(perms, permPolicies)
	filter := db.ArxiuFilter{
		Text:  strings.TrimSpace(r.URL.Query().Get("q")),
		Tipus: strings.TrimSpace(r.URL.Query().Get("tipus")),
		Acces: strings.TrimSpace(r.URL.Query().Get("acces")),
	}
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	if status == "" {
		status = "publicat"
	}
	filter.Status = status
	if v := strings.TrimSpace(r.URL.Query().Get("entitat_id")); v != "" {
		if id, err := strconv.Atoi(v); err == nil {
			filter.EntitatID = id
		}
	}
	arxius, _ := a.DB.ListArxius(filter)
	for i := range arxius {
		if rels, err := a.DB.ListArxiuLlibres(arxius[i].ID); err == nil {
			arxius[i].Llibres = len(rels)
		}
	}
	arquebisbats, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	RenderPrivateTemplate(w, r, "admin-arxius-list.html", map[string]interface{}{
		"Arxius":             arxius,
		"Filter":             filter,
		"CanManageArxius":    canManage,
		"ArxiusBasePath":     "/arxius",
		"Arquebisbats":       arquebisbats,
		"User":               user,
		"IsAdmin":            isAdmin,
		"CanManageTerritory": canManageTerritory,
		"CanManageEclesia":   canManageEclesia,
		"CanModerate":        canModerate,
		"CanManageUsers":     canManageUsers,
		"CanManagePolicies":  canManagePolicies,
	})
}

// Detall en lectura d'un arxiu
func (a *App) ShowArxiu(w http.ResponseWriter, r *http.Request) {
	user, authenticated := a.VerificarSessio(r)
	if !authenticated || user == nil {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	perms := a.getPermissionsForUser(user.ID)
	*r = *a.withPermissions(r, perms)
	canManage := a.hasPerm(perms, permArxius)
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	arxiu, err := a.DB.GetArxiu(id)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if !canManage && arxiu.ModeracioEstat != "publicat" {
		http.NotFound(w, r)
		return
	}
	llibres, _ := a.DB.ListArxiuLlibres(id)
	entNom := a.loadEntitatNom(arxiu)
	RenderPrivateTemplate(w, r, "admin-arxius-show.html", map[string]interface{}{
		"Arxiu":           arxiu,
		"Llibres":         llibres,
		"EntitatNom":      entNom,
		"CanManageArxius": canManage,
		"ArxiusBasePath":  "/arxius",
		"User":            user,
	})
}

// Admin: llistat d'arxius
func (a *App) AdminListArxius(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKeyAnyScope(w, r, permKeyDocumentalsArxiusView)
	if !ok {
		return
	}
	perms := a.getPermissionsForUser(user.ID)
	canManage := a.hasPerm(perms, permArxius)
	isAdmin := a.hasPerm(perms, permAdmin)
	canManageTerritory := a.hasPerm(perms, permTerritory)
	canManageEclesia := a.hasPerm(perms, permEclesia)
	canModerate := a.hasPerm(perms, permModerate)
	canManageUsers := a.hasPerm(perms, permUsers)
	canManagePolicies := a.hasPerm(perms, permPolicies)
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	if status == "" {
		status = "publicat"
	}
	scopeFilter := a.buildListScopeFilter(user.ID, permKeyDocumentalsArxiusView, ScopeArxiu)
	filter := db.ArxiuFilter{
		Text:   strings.TrimSpace(r.URL.Query().Get("q")),
		Tipus:  strings.TrimSpace(r.URL.Query().Get("tipus")),
		Acces:  strings.TrimSpace(r.URL.Query().Get("acces")),
		Status: status,
	}
	if v := strings.TrimSpace(r.URL.Query().Get("entitat_id")); v != "" {
		if id, err := strconv.Atoi(v); err == nil {
			filter.EntitatID = id
		}
	}
	if !scopeFilter.hasGlobal {
		if scopeFilter.isEmpty() {
			RenderPrivateTemplate(w, r, "admin-arxius-list.html", map[string]interface{}{
				"Arxius":             []db.ArxiuWithCount{},
				"Filter":             filter,
				"ArxiusBasePath":     "/documentals/arxius",
				"Arquebisbats":       []db.ArquebisbatRow{},
				"CanManageArxius":    canManage,
				"User":               user,
				"IsAdmin":            isAdmin,
				"CanManageTerritory": canManageTerritory,
				"CanManageEclesia":   canManageEclesia,
				"CanModerate":        canModerate,
				"CanManageUsers":     canManageUsers,
				"CanManagePolicies":  canManagePolicies,
			})
			return
		}
		filter.AllowedArxiuIDs = scopeFilter.arxiuIDs
		filter.AllowedMunicipiIDs = scopeFilter.municipiIDs
		filter.AllowedProvinciaIDs = scopeFilter.provinciaIDs
		filter.AllowedComarcaIDs = scopeFilter.comarcaIDs
		filter.AllowedPaisIDs = scopeFilter.paisIDs
		filter.AllowedEclesIDs = scopeFilter.eclesIDs
	}
	arxius, _ := a.DB.ListArxius(filter)
	for i := range arxius {
		if rels, err := a.DB.ListArxiuLlibres(arxius[i].ID); err == nil {
			arxius[i].Llibres = len(rels)
		}
	}
	arquebisbats, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	RenderPrivateTemplate(w, r, "admin-arxius-list.html", map[string]interface{}{
		"Arxius":             arxius,
		"Filter":             filter,
		"ArxiusBasePath":     "/documentals/arxius",
		"Arquebisbats":       arquebisbats,
		"CanManageArxius":    canManage,
		"User":               user,
		"IsAdmin":            isAdmin,
		"CanManageTerritory": canManageTerritory,
		"CanManageEclesia":   canManageEclesia,
		"CanModerate":        canModerate,
		"CanManageUsers":     canManageUsers,
		"CanManagePolicies":  canManagePolicies,
	})
}

func parseArxiuForm(r *http.Request) *db.Arxiu {
	_ = r.ParseForm()
	municipiID := sqlNullInt(r.FormValue("municipi_id"))
	entitatID := sqlNullInt(r.FormValue("entitat_eclesiastica_id"))
	return &db.Arxiu{
		Nom:                   strings.TrimSpace(r.FormValue("nom")),
		Tipus:                 strings.TrimSpace(r.FormValue("tipus")),
		Acces:                 strings.TrimSpace(r.FormValue("acces")),
		MunicipiID:            municipiID,
		EntitatEclesiasticaID: entitatID,
		Adreca:                strings.TrimSpace(r.FormValue("adreca")),
		Ubicacio:              strings.TrimSpace(r.FormValue("ubicacio")),
		Web:                   strings.TrimSpace(r.FormValue("web")),
		Notes:                 strings.TrimSpace(r.FormValue("notes")),
	}
}

func (a *App) renderArxiuForm(w http.ResponseWriter, r *http.Request, arxiu *db.Arxiu, isNew bool, errMsg string, user *db.User, returnURL string) {
	municipis, _ := a.DB.ListMunicipis(db.MunicipiFilter{})
	arquebisbats, _ := a.DB.ListArquebisbats(db.ArquebisbatFilter{})
	RenderPrivateTemplate(w, r, "admin-arxius-form.html", map[string]interface{}{
		"Arxiu":           arxiu,
		"IsNew":           isNew,
		"Error":           errMsg,
		"ReturnURL":       returnURL,
		"CanManageArxius": true,
		"Municipis":       municipis,
		"Arquebisbats":    arquebisbats,
		"User":            user,
	})
}

// Alta
func (a *App) AdminNewArxiu(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsArxiusCreate, PermissionTarget{})
	if !ok {
		return
	}
	returnURL := strings.TrimSpace(r.URL.Query().Get("return_to"))
	a.renderArxiuForm(w, r, &db.Arxiu{}, true, "", user, returnURL)
}

func (a *App) AdminCreateArxiu(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsArxiusCreate, PermissionTarget{})
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/documentals/arxius", http.StatusSeeOther)
		return
	}
	returnURL := strings.TrimSpace(r.FormValue("return_to"))
	arxiu := parseArxiuForm(r)
	if arxiu.Nom == "" || len(arxiu.Nom) < 3 {
		a.renderArxiuForm(w, r, arxiu, true, "El nom és obligatori (mínim 3 caràcters).", user, returnURL)
		return
	}
	arxiu.CreatedBy = sqlNullIntFromInt(user.ID)
	arxiu.ModeracioEstat = "pendent"
	arxiu.ModeratedBy = sql.NullInt64{}
	arxiu.ModeratedAt = sql.NullTime{}
	id, err := a.DB.CreateArxiu(arxiu)
	if err != nil {
		a.renderArxiuForm(w, r, arxiu, true, "No s'ha pogut crear l'arxiu.", user, returnURL)
		return
	}
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleArxiuCreate, "crear", "arxiu", &id, "pendent", nil, "")
	if returnURL != "" {
		http.Redirect(w, r, returnURL, http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/documentals/arxius/"+strconv.Itoa(id), http.StatusSeeOther)
}

// Edició
func (a *App) AdminEditArxiu(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	target := a.resolveArxiuTarget(id)
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsArxiusEdit, target)
	if !ok {
		return
	}
	arxiu, err := a.DB.GetArxiu(id)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	returnURL := strings.TrimSpace(r.URL.Query().Get("return_to"))
	a.renderArxiuForm(w, r, arxiu, false, "", user, returnURL)
}

func (a *App) AdminUpdateArxiu(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	target := a.resolveArxiuTarget(id)
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsArxiusEdit, target)
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/documentals/arxius/"+strconv.Itoa(id)+"/edit", http.StatusSeeOther)
		return
	}
	returnURL := strings.TrimSpace(r.FormValue("return_to"))
	arxiu := parseArxiuForm(r)
	arxiu.ID = id
	arxiu.ModeracioEstat = "pendent"
	arxiu.ModeratedBy = sql.NullInt64{}
	arxiu.ModeratedAt = sql.NullTime{}
	if err := a.DB.UpdateArxiu(arxiu); err != nil {
		a.renderArxiuForm(w, r, arxiu, false, "No s'ha pogut actualitzar.", user, returnURL)
		return
	}
	_, _ = a.RegisterUserActivity(r.Context(), user.ID, ruleArxiuUpdate, "editar", "arxiu", &id, "pendent", nil, "")
	if returnURL != "" {
		http.Redirect(w, r, returnURL, http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/documentals/arxius/"+strconv.Itoa(id), http.StatusSeeOther)
}

func (a *App) AdminDeleteArxiu(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	target := a.resolveArxiuTarget(id)
	if _, ok := a.requirePermissionKey(w, r, permKeyDocumentalsArxiusDelete, target); !ok {
		return
	}
	_ = a.DB.DeleteArxiu(id)
	http.Redirect(w, r, "/documentals/arxius", http.StatusSeeOther)
}

func (a *App) AdminShowArxiu(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	target := a.resolveArxiuTarget(id)
	user, ok := a.requirePermissionKey(w, r, permKeyDocumentalsArxiusView, target)
	if !ok {
		return
	}
	arxiu, err := a.DB.GetArxiu(id)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	llibres, _ := a.DB.ListArxiuLlibres(id)
	entNom := a.loadEntitatNom(arxiu)
	RenderPrivateTemplate(w, r, "admin-arxius-show.html", map[string]interface{}{
		"Arxiu":           arxiu,
		"Llibres":         llibres,
		"EntitatNom":      entNom,
		"CanManageArxius": true,
		"ArxiusBasePath":  "/documentals/arxius",
		"User":            user,
	})
}

func (a *App) AdminAddArxiuLlibre(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path)
	target := a.resolveArxiuTarget(id)
	if _, ok := a.requirePermissionKey(w, r, permKeyDocumentalsArxiusEdit, target); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/documentals/arxius/"+strconv.Itoa(id), http.StatusSeeOther)
		return
	}
	llibreID, _ := strconv.Atoi(r.FormValue("llibre_id"))
	signatura := strings.TrimSpace(r.FormValue("signatura"))
	urlOverride := strings.TrimSpace(r.FormValue("url_override"))
	if llibreID == 0 {
		http.Redirect(w, r, "/documentals/arxius/"+strconv.Itoa(id)+"?error=llibre", http.StatusSeeOther)
		return
	}
	if err := a.DB.AddArxiuLlibre(id, llibreID, signatura, urlOverride); err != nil {
		http.Redirect(w, r, "/documentals/arxius/"+strconv.Itoa(id)+"?error=dup", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/documentals/arxius/"+strconv.Itoa(id), http.StatusSeeOther)
}

func (a *App) AdminUpdateArxiuLlibre(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 5 {
		http.NotFound(w, r)
		return
	}
	arxiuID, _ := strconv.Atoi(parts[2])
	target := a.resolveArxiuTarget(arxiuID)
	if _, ok := a.requirePermissionKey(w, r, permKeyDocumentalsArxiusEdit, target); !ok {
		return
	}
	llibreID, _ := strconv.Atoi(parts[4])
	signatura := strings.TrimSpace(r.FormValue("signatura"))
	urlOverride := strings.TrimSpace(r.FormValue("url_override"))
	_ = a.DB.UpdateArxiuLlibre(arxiuID, llibreID, signatura, urlOverride)
	http.Redirect(w, r, "/documentals/arxius/"+strconv.Itoa(arxiuID), http.StatusSeeOther)
}

func (a *App) AdminDeleteArxiuLlibre(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 5 {
		http.NotFound(w, r)
		return
	}
	arxiuID, _ := strconv.Atoi(parts[2])
	target := a.resolveArxiuTarget(arxiuID)
	if _, ok := a.requirePermissionKey(w, r, permKeyDocumentalsArxiusEdit, target); !ok {
		return
	}
	llibreID, _ := strconv.Atoi(parts[4])
	_ = a.DB.DeleteArxiuLlibre(arxiuID, llibreID)
	http.Redirect(w, r, "/documentals/arxius/"+strconv.Itoa(arxiuID), http.StatusSeeOther)
}

// util
func extractID(path string) int {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i := len(parts) - 1; i >= 0; i-- {
		if id, err := strconv.Atoi(parts[i]); err == nil {
			return id
		}
	}
	return 0
}

func sqlNullInt(val string) (n sql.NullInt64) {
	if strings.TrimSpace(val) == "" {
		return
	}
	if i, err := strconv.Atoi(val); err == nil {
		n.Int64 = int64(i)
		n.Valid = true
	}
	return
}

func sqlNullIntFromInt(v int) sql.NullInt64 {
	return sql.NullInt64{Int64: int64(v), Valid: true}
}

func (a *App) loadEntitatNom(arxiu *db.Arxiu) string {
	if arxiu == nil || !arxiu.EntitatEclesiasticaID.Valid {
		return ""
	}
	if ent, err := a.DB.GetArquebisbat(int(arxiu.EntitatEclesiasticaID.Int64)); err == nil && ent != nil {
		return ent.Nom
	}
	return ""
}
