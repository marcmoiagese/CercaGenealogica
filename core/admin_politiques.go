package core

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type politicaGrantView struct {
	ID              int
	PermKey         string
	ScopeType       string
	ScopeID         int
	ScopeIDValid    bool
	IncludeChildren bool
}

type politicaGrantForm struct {
	ID              int
	PermKey         string
	ScopeType       string
	ScopeID         int
	IncludeChildren bool
}

func buildGrantViews(grants []db.PoliticaGrant) []politicaGrantView {
	res := make([]politicaGrantView, 0, len(grants))
	for _, g := range grants {
		view := politicaGrantView{
			ID:              g.ID,
			PermKey:         g.PermKey,
			ScopeType:       g.ScopeType,
			IncludeChildren: g.IncludeChildren,
		}
		if g.ScopeID.Valid {
			view.ScopeID = int(g.ScopeID.Int64)
			view.ScopeIDValid = true
		}
		res = append(res, view)
	}
	return res
}

func scopeLabelKeyMap() map[string]string {
	labels := map[string]string{}
	for _, opt := range scopeOptions() {
		labels[string(opt.Value)] = opt.LabelKey
	}
	return labels
}

func normalizePolicyTab(val string) string {
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "json":
		return "json"
	case "grants":
		return "grants"
	case "gui":
		return "gui"
	default:
		return "gui"
	}
}

func (a *App) politicaFormData(r *http.Request, pol *db.Politica, isNew bool, activeTab string, errMsg string, grantForm *politicaGrantForm) map[string]interface{} {
	if pol == nil {
		pol = &db.Politica{}
	}
	activeTab = normalizePolicyTab(activeTab)
	grants := []politicaGrantView{}
	if !isNew && pol.ID > 0 && a.DB != nil {
		if rows, err := a.DB.ListPoliticaGrants(pol.ID); err == nil {
			grants = buildGrantViews(rows)
		}
	}
	data := map[string]interface{}{
		"Politica":          pol,
		"IsNew":             isNew,
		"ActiveTab":         activeTab,
		"Grants":            grants,
		"PermissionCatalog": permissionCatalog(),
		"ScopeOptions":      scopeOptions(),
		"ScopeLabels":       scopeLabelKeyMap(),
		"CanManageArxius":   true,
		"CanManagePolicies": true,
	}
	if errMsg != "" {
		data["Error"] = errMsg
	}
	if grantForm != nil {
		data["GrantForm"] = grantForm
	}
	return data
}

func (a *App) AdminListPolitiques(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permPolicies); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	pols, err := a.DB.ListPolitiques()
	if err != nil {
		http.Error(w, "Error obtenint polítiques", http.StatusInternalServerError)
		return
	}
	RenderPrivateTemplate(w, r, "admin-politiques-list.html", map[string]interface{}{
		"Politiques":        pols,
		"CanManageArxius":   true,
		"CanManagePolicies": true,
		"User":              user,
	})
}

func (a *App) AdminNewPolitica(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permPolicies); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	data := a.politicaFormData(r, &db.Politica{}, true, "gui", "", nil)
	data["User"] = user
	RenderPrivateTemplate(w, r, "admin-politiques-form.html", data)
}

func (a *App) AdminEditPolitica(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permPolicies); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	id := extractID(r.URL.Path)
	pol, err := a.DB.GetPolitica(id)
	if err != nil || pol == nil {
		http.NotFound(w, r)
		return
	}
	data := a.politicaFormData(r, pol, false, r.URL.Query().Get("tab"), "", nil)
	data["User"] = user
	RenderPrivateTemplate(w, r, "admin-politiques-form.html", data)
}

func (a *App) AdminSavePolitica(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permPolicies); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Formulari invàlid", http.StatusBadRequest)
		return
	}
	id, _ := strconv.Atoi(r.FormValue("id"))
	name := strings.TrimSpace(r.FormValue("nom"))
	desc := strings.TrimSpace(r.FormValue("descripcio"))
	permsRaw := strings.TrimSpace(r.FormValue("permisos"))

	if name == "" {
		pol := &db.Politica{ID: id, Nom: name, Descripcio: desc, Permisos: permsRaw}
		RenderPrivateTemplate(w, r, "admin-politiques-form.html", a.politicaFormData(r, pol, id == 0, "gui", "El nom és obligatori", nil))
		return
	}

	// Validar JSON de permisos
	var parsed db.PolicyPermissions
	if permsRaw == "" {
		permsRaw = "{}"
	}
	if err := json.Unmarshal([]byte(permsRaw), &parsed); err != nil {
		pol := &db.Politica{ID: id, Nom: name, Descripcio: desc, Permisos: permsRaw}
		RenderPrivateTemplate(w, r, "admin-politiques-form.html", a.politicaFormData(r, pol, id == 0, "json", "JSON de permisos invàlid", nil))
		return
	}
	// Re-marshal per guardar net
	permsClean, _ := json.Marshal(parsed)
	p := &db.Politica{
		ID:         id,
		Nom:        name,
		Descripcio: desc,
		Permisos:   string(permsClean),
	}
	if _, err := a.DB.SavePolitica(p); err != nil {
		RenderPrivateTemplate(w, r, "admin-politiques-form.html", a.politicaFormData(r, p, id == 0, "gui", "No s'ha pogut desar la política", nil))
		return
	}
	_ = a.DB.BumpPolicyPermissionsVersion(p.ID)
	http.Redirect(w, r, "/admin/politiques", http.StatusSeeOther)
}

func (a *App) AdminSavePoliticaGrant(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permPolicies); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Formulari invàlid", http.StatusBadRequest)
		return
	}
	politicaID, _ := strconv.Atoi(r.FormValue("politica_id"))
	grantID, _ := strconv.Atoi(r.FormValue("grant_id"))
	permKey := strings.TrimSpace(r.FormValue("perm_key"))
	scopeTypeRaw := strings.TrimSpace(r.FormValue("scope_type"))
	scopeType, scopeOK := parseScopeType(scopeTypeRaw)
	includeChildren := r.FormValue("include_children") == "1"
	grantForm := &politicaGrantForm{
		ID:              grantID,
		PermKey:         permKey,
		ScopeType:       scopeTypeRaw,
		IncludeChildren: includeChildren,
	}

	pol, err := a.DB.GetPolitica(politicaID)
	if err != nil || pol == nil {
		http.NotFound(w, r)
		return
	}

	if permKey == "" || !isKnownPermissionKey(permKey) {
		RenderPrivateTemplate(w, r, "admin-politiques-form.html", a.politicaFormData(r, pol, false, "grants", "Permís invàlid", grantForm))
		return
	}
	if !scopeOK {
		RenderPrivateTemplate(w, r, "admin-politiques-form.html", a.politicaFormData(r, pol, false, "grants", "Àmbit invàlid", grantForm))
		return
	}
	grantForm.ScopeType = string(scopeType)

	scopeID := 0
	if scopeType != ScopeGlobal {
		scopeID, _ = strconv.Atoi(strings.TrimSpace(r.FormValue("scope_id")))
		if scopeID <= 0 {
			RenderPrivateTemplate(w, r, "admin-politiques-form.html", a.politicaFormData(r, pol, false, "grants", "ID d'àmbit obligatori", grantForm))
			return
		}
		grantForm.ScopeID = scopeID
		grantForm.ScopeType = string(scopeType)
	}

	if grantID > 0 {
		found := false
		if grants, err := a.DB.ListPoliticaGrants(politicaID); err == nil {
			for _, g := range grants {
				if g.ID == grantID {
					found = true
					break
				}
			}
		}
		if !found {
			RenderPrivateTemplate(w, r, "admin-politiques-form.html", a.politicaFormData(r, pol, false, "grants", "Grant no trobada", grantForm))
			return
		}
	}

	grant := &db.PoliticaGrant{
		ID:              grantID,
		PoliticaID:      politicaID,
		PermKey:         permKey,
		ScopeType:       string(scopeType),
		IncludeChildren: includeChildren,
	}
	if scopeType != ScopeGlobal {
		grant.ScopeID = sql.NullInt64{Int64: int64(scopeID), Valid: true}
	}

	if _, err := a.DB.SavePoliticaGrant(grant); err != nil {
		RenderPrivateTemplate(w, r, "admin-politiques-form.html", a.politicaFormData(r, pol, false, "grants", "No s'ha pogut desar el grant", grantForm))
		return
	}
	_ = a.DB.BumpPolicyPermissionsVersion(politicaID)
	http.Redirect(w, r, fmt.Sprintf("/admin/politiques/%d/edit?tab=grants", politicaID), http.StatusSeeOther)
}

func (a *App) AdminDeletePoliticaGrant(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := a.requirePermission(w, r, permPolicies); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Formulari invàlid", http.StatusBadRequest)
		return
	}
	politicaID, _ := strconv.Atoi(r.FormValue("politica_id"))
	grantID, _ := strconv.Atoi(r.FormValue("grant_id"))
	pol, err := a.DB.GetPolitica(politicaID)
	if err != nil || pol == nil {
		http.NotFound(w, r)
		return
	}
	if grantID <= 0 {
		RenderPrivateTemplate(w, r, "admin-politiques-form.html", a.politicaFormData(r, pol, false, "grants", "Grant invàlida", nil))
		return
	}
	found := false
	if grants, err := a.DB.ListPoliticaGrants(politicaID); err == nil {
		for _, g := range grants {
			if g.ID == grantID {
				found = true
				break
			}
		}
	}
	if !found {
		RenderPrivateTemplate(w, r, "admin-politiques-form.html", a.politicaFormData(r, pol, false, "grants", "Grant no trobada", nil))
		return
	}
	if err := a.DB.DeletePoliticaGrant(grantID); err != nil {
		RenderPrivateTemplate(w, r, "admin-politiques-form.html", a.politicaFormData(r, pol, false, "grants", "No s'ha pogut eliminar el grant", nil))
		return
	}
	_ = a.DB.BumpPolicyPermissionsVersion(politicaID)
	http.Redirect(w, r, fmt.Sprintf("/admin/politiques/%d/edit?tab=grants", politicaID), http.StatusSeeOther)
}
