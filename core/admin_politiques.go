package core

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
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
	ScopeLabel      string
}

type politicaGrantForm struct {
	ID              int
	PermKey         string
	ScopeType       string
	ScopeID         int
	IncludeChildren bool
	ScopeLabel      string
}

type policyGuiGrantGroup struct {
	TitleKey string
	Keys     []string
}

type policyModularJSON struct {
	Version int                     `json:"version"`
	Policy  policyModularJSONMeta   `json:"policy"`
	Grants  []policyModularJSONItem `json:"grants"`
}

type policyModularJSONMeta struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type policyModularJSONItem struct {
	PermKey         string `json:"perm_key"`
	ScopeType       string `json:"scope_type"`
	ScopeID         *int   `json:"scope_id"`
	IncludeChildren bool   `json:"include_children"`
}

func guiGrantGroups() []policyGuiGrantGroup {
	return []policyGuiGrantGroup{
		{
			TitleKey: "policies.gui.modular.group.general_view",
			Keys: []string{
				permKeyHomeView,
				permKeyMessagesView,
				permKeySearchAdvancedView,
				permKeyRankingView,
				permKeyPersonsView,
				permKeyCognomsView,
				permKeyMediaView,
				permKeyImportTemplatesView,
				permKeyEventsView,
			},
		},
		{
			TitleKey: "policies.gui.modular.group.wiki_actions",
			Keys: []string{
				permKeyWikiRevert,
			},
		},
		{
			TitleKey: "policies.gui.modular.group.moderation",
			Keys: []string{
				permKeyPersonesModerate,
				permKeyCognomsModerate,
				permKeyEventsModerate,
				permKeyMediaModerate,
			},
		},
		{
			TitleKey: "policies.gui.modular.group.documentals_view",
			Keys: []string{
				permKeyDocumentalsArxiusView,
				permKeyDocumentalsLlibresView,
				permKeyDocumentalsLlibresViewRegistres,
			},
		},
		{
			TitleKey: "policies.gui.modular.group.documentals_index",
			Keys: []string{
				permKeyDocumentalsRegistresEdit,
				permKeyDocumentalsRegistresEditInline,
				permKeyDocumentalsRegistresLinkPerson,
				permKeyDocumentalsRegistresConvertToPerson,
				permKeyDocumentalsLlibresImportCSV,
				permKeyDocumentalsLlibresBulkIndex,
			},
		},
		{
			TitleKey: "policies.gui.modular.group.territori_view",
			Keys: []string{
				permKeyTerritoriNivellsView,
				permKeyTerritoriMunicipisView,
				permKeyTerritoriEclesView,
			},
		},
		{
			TitleKey: "policies.gui.modular.group.admin_platform",
			Keys: []string{
				permKeyAdminUsersManage,
				permKeyAdminPoliciesManage,
				permKeyAdminAuditView,
				permKeyAdminJobsManage,
				permKeyAdminPlatformSettingsEdit,
				permKeyAdminMaintenanceManage,
				permKeyAdminAnalyticsView,
				permKeyAdminTransparencyManage,
			},
		},
		{
			TitleKey: "policies.gui.modular.group.admin_external",
			Keys: []string{
				permKeyAdminExternalSitesManage,
				permKeyAdminExternalLinksModerate,
			},
		},
	}
}

func guiGrantKeySet(groups []policyGuiGrantGroup) map[string]bool {
	keys := map[string]bool{}
	for _, group := range groups {
		for _, key := range group.Keys {
			if strings.TrimSpace(key) == "" {
				continue
			}
			keys[key] = true
		}
	}
	return keys
}

type grantScopeLabeler struct {
	app      *App
	lang     string
	pais     map[int]string
	nivell   map[int]string
	municipi map[int]string
	ecles    map[int]string
	arxiu    map[int]string
	llibre   map[int]string
}

func newGrantScopeLabeler(app *App, lang string) *grantScopeLabeler {
	return &grantScopeLabeler{
		app:      app,
		lang:     lang,
		pais:     map[int]string{},
		nivell:   map[int]string{},
		municipi: map[int]string{},
		ecles:    map[int]string{},
		arxiu:    map[int]string{},
		llibre:   map[int]string{},
	}
}

func (l *grantScopeLabeler) label(scopeType string, scopeID int) string {
	if l == nil || l.app == nil || l.app.DB == nil || scopeID <= 0 {
		return ""
	}
	scopeType = strings.TrimSpace(scopeType)
	switch scopeType {
	case string(ScopePais):
		if cached, ok := l.pais[scopeID]; ok {
			return cached
		}
		pais, err := l.app.DB.GetPais(scopeID)
		if err == nil && pais != nil {
			label := l.app.countryLabelFromISO(pais.CodiISO2, l.lang)
			l.pais[scopeID] = label
			return label
		}
	case string(ScopeNivell), string(ScopeProvincia), string(ScopeComarca):
		if cached, ok := l.nivell[scopeID]; ok {
			return cached
		}
		nivell, err := l.app.DB.GetNivell(scopeID)
		if err == nil && nivell != nil {
			label := strings.TrimSpace(nivell.NomNivell)
			if label == "" {
				label = strings.TrimSpace(nivell.TipusNivell)
			}
			l.nivell[scopeID] = label
			return label
		}
	case string(ScopeMunicipi):
		if cached, ok := l.municipi[scopeID]; ok {
			return cached
		}
		mun, err := l.app.DB.GetMunicipi(scopeID)
		if err == nil && mun != nil {
			label := strings.TrimSpace(mun.Nom)
			if label == "" {
				label = strings.TrimSpace(mun.Tipus)
			}
			l.municipi[scopeID] = label
			return label
		}
	case string(ScopeEcles):
		if cached, ok := l.ecles[scopeID]; ok {
			return cached
		}
		ent, err := l.app.DB.GetArquebisbat(scopeID)
		if err == nil && ent != nil {
			label := strings.TrimSpace(ent.Nom)
			l.ecles[scopeID] = label
			return label
		}
	case string(ScopeArxiu):
		if cached, ok := l.arxiu[scopeID]; ok {
			return cached
		}
		arxiu, err := l.app.DB.GetArxiu(scopeID)
		if err == nil && arxiu != nil {
			label := strings.TrimSpace(arxiu.Nom)
			l.arxiu[scopeID] = label
			return label
		}
	case string(ScopeLlibre):
		if cached, ok := l.llibre[scopeID]; ok {
			return cached
		}
		llibre, err := l.app.DB.GetLlibre(scopeID)
		if err == nil && llibre != nil {
			label := strings.TrimSpace(llibre.Titol)
			if label == "" {
				label = strings.TrimSpace(llibre.NomEsglesia)
			}
			l.llibre[scopeID] = label
			return label
		}
	}
	return ""
}

func buildGrantViews(grants []db.PoliticaGrant, labeler *grantScopeLabeler) []politicaGrantView {
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
			if labeler != nil {
				view.ScopeLabel = labeler.label(g.ScopeType, view.ScopeID)
			}
		}
		res = append(res, view)
	}
	return res
}

func exportPolicyModularJSON(pol *db.Politica, grants []db.PoliticaGrant) string {
	if pol == nil {
		pol = &db.Politica{}
	}
	items := make([]policyModularJSONItem, 0, len(grants))
	for _, g := range grants {
		item := policyModularJSONItem{
			PermKey:         strings.TrimSpace(g.PermKey),
			ScopeType:       strings.TrimSpace(g.ScopeType),
			IncludeChildren: g.IncludeChildren,
		}
		if g.ScopeID.Valid {
			scopeID := int(g.ScopeID.Int64)
			item.ScopeID = &scopeID
		}
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].PermKey != items[j].PermKey {
			return items[i].PermKey < items[j].PermKey
		}
		if items[i].ScopeType != items[j].ScopeType {
			return items[i].ScopeType < items[j].ScopeType
		}
		iID, jID := 0, 0
		if items[i].ScopeID != nil {
			iID = *items[i].ScopeID
		}
		if items[j].ScopeID != nil {
			jID = *items[j].ScopeID
		}
		if iID != jID {
			return iID < jID
		}
		return !items[i].IncludeChildren && items[j].IncludeChildren
	})
	doc := policyModularJSON{
		Version: 1,
		Policy: policyModularJSONMeta{
			Name:        pol.Nom,
			Description: pol.Descripcio,
		},
		Grants: items,
	}
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return "{\n  \"version\": 1,\n  \"policy\": {\"name\": \"\", \"description\": \"\"},\n  \"grants\": []\n}"
	}
	return string(out)
}

func parsePolicyModularJSON(input string, politicaID int) ([]db.PoliticaGrant, error) {
	raw := strings.TrimSpace(input)
	if raw == "" {
		return nil, fmt.Errorf("JSON buit")
	}
	for _, legacy := range []string{"per" + "misos", "Policy" + "Permissions", "Policy" + "Document", "CanManageUsers", "CanManagePolicies", "CanModerate", "Admin"} {
		if strings.Contains(raw, legacy) {
			return nil, fmt.Errorf("camp legacy no acceptat: %s", legacy)
		}
	}
	dec := json.NewDecoder(bytes.NewReader([]byte(raw)))
	dec.DisallowUnknownFields()
	var doc policyModularJSON
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("JSON invalid: %w", err)
	}
	var extra interface{}
	if err := dec.Decode(&extra); err == nil {
		return nil, fmt.Errorf("JSON invalid: contingut extra")
	}
	if doc.Version != 1 {
		return nil, fmt.Errorf("version ha de ser 1")
	}
	if doc.Grants == nil {
		return nil, fmt.Errorf("grants ha de ser un array")
	}
	grants := make([]db.PoliticaGrant, 0, len(doc.Grants))
	seen := map[string]bool{}
	for i, item := range doc.Grants {
		permKey := strings.TrimSpace(item.PermKey)
		if permKey == "" {
			return nil, fmt.Errorf("grants[%d].perm_key es obligatori", i)
		}
		if !isKnownPermissionKey(permKey) {
			return nil, fmt.Errorf("grants[%d].perm_key desconegut: %s", i, permKey)
		}
		scopeType, ok := parseScopeType(item.ScopeType)
		if !ok {
			return nil, fmt.Errorf("grants[%d].scope_type invalid: %s", i, item.ScopeType)
		}
		grant := db.PoliticaGrant{
			PoliticaID:      politicaID,
			PermKey:         permKey,
			ScopeType:       string(scopeType),
			IncludeChildren: item.IncludeChildren,
		}
		scopeID := 0
		if item.ScopeID != nil {
			scopeID = *item.ScopeID
		}
		if scopeType == ScopeGlobal {
			if scopeID != 0 {
				return nil, fmt.Errorf("grants[%d].scope_id ha de ser null o 0 per global", i)
			}
		} else {
			if scopeID <= 0 {
				return nil, fmt.Errorf("grants[%d].scope_id es obligatori per scope_type %s", i, scopeType)
			}
			grant.ScopeID = sql.NullInt64{Int64: int64(scopeID), Valid: true}
		}
		key := fmt.Sprintf("%s|%s|%d|%t", grant.PermKey, grant.ScopeType, scopeID, grant.IncludeChildren)
		if seen[key] {
			return nil, fmt.Errorf("grant duplicat a grants[%d]", i)
		}
		seen[key] = true
		grants = append(grants, grant)
	}
	return grants, nil
}

func scopeLabelKeyMap() map[string]string {
	labels := map[string]string{}
	for _, opt := range scopeOptions() {
		labels[string(opt.Value)] = opt.LabelKey
	}
	labels[string(ScopeProvincia)] = "policies.grants.scope.nivell"
	labels[string(ScopeComarca)] = "policies.grants.scope.nivell"
	labels[string(ScopeMunicipi)] = "policies.grants.scope.nivell"
	return labels
}

func normalizePolicyTab(val string) string {
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "grants":
		return "grants"
	case "json":
		return "json"
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
	lang := ResolveLang(r)
	guiGroups := guiGrantGroups()
	guiKeySet := guiGrantKeySet(guiGroups)
	grants := []politicaGrantView{}
	var grantRows []db.PoliticaGrant
	guiGrantState := map[string]bool{}
	if !isNew && pol.ID > 0 && a.DB != nil {
		if rows, err := a.DB.ListPoliticaGrants(pol.ID); err == nil {
			grantRows = rows
			labeler := newGrantScopeLabeler(a, lang)
			grants = buildGrantViews(rows, labeler)
			for _, g := range rows {
				if g.ScopeType != string(ScopeGlobal) {
					continue
				}
				if guiKeySet[g.PermKey] {
					guiGrantState[g.PermKey] = true
				}
			}
		} else {
			Errorf("No s'han pogut carregar grants de politica %d: %v", pol.ID, err)
			if errMsg == "" {
				errMsg = "No s'han pogut carregar els grants de la politica"
			}
		}
	}
	if grantForm != nil && grantForm.ScopeID > 0 && strings.TrimSpace(grantForm.ScopeLabel) == "" {
		labeler := newGrantScopeLabeler(a, lang)
		grantForm.ScopeLabel = labeler.label(grantForm.ScopeType, grantForm.ScopeID)
	}
	data := map[string]interface{}{
		"Politica":          pol,
		"IsNew":             isNew,
		"ActiveTab":         activeTab,
		"Grants":            grants,
		"PermissionCatalog": permissionCatalog(),
		"ScopeOptions":      scopeOptions(),
		"ScopeLabels":       scopeLabelKeyMap(),
		"GuiGrantGroups":    guiGroups,
		"GuiGrantState":     guiGrantState,
		"PolicyJSON":        exportPolicyModularJSON(pol, grantRows),
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
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminPoliciesManage, PermissionTarget{}); !ok {
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
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminPoliciesManage, PermissionTarget{}); !ok {
		return
	}
	user, _ := a.VerificarSessio(r)
	data := a.politicaFormData(r, &db.Politica{}, true, "gui", "", nil)
	data["User"] = user
	RenderPrivateTemplate(w, r, "admin-politiques-form.html", data)
}

func (a *App) AdminEditPolitica(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminPoliciesManage, PermissionTarget{}); !ok {
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
	if r.URL.Query().Get("ok") == "json_applied" {
		data["Notice"] = "JSON modular aplicat correctament"
	}
	data["User"] = user
	RenderPrivateTemplate(w, r, "admin-politiques-form.html", data)
}

func (a *App) AdminSavePolitica(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminPoliciesManage, PermissionTarget{}); !ok {
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
	activeTab := normalizePolicyTab(r.FormValue("active_tab"))

	if name == "" {
		pol := &db.Politica{ID: id, Nom: name, Descripcio: desc}
		RenderPrivateTemplate(w, r, "admin-politiques-form.html", a.politicaFormData(r, pol, id == 0, "gui", "El nom és obligatori", nil))
		return
	}

	p := &db.Politica{
		ID:         id,
		Nom:        name,
		Descripcio: desc,
	}
	savedID, err := a.DB.SavePolitica(p)
	if err != nil {
		RenderPrivateTemplate(w, r, "admin-politiques-form.html", a.politicaFormData(r, p, id == 0, "gui", "No s'ha pogut desar la política", nil))
		return
	}
	if savedID > 0 {
		p.ID = savedID
	}
	if activeTab == "gui" {
		guiKeys := guiGrantKeySet(guiGrantGroups())
		selectedGuiKeys := extractGuiGrantSelection(r, guiKeys)
		if err := a.syncPolicyGlobalGrants(p.ID, guiKeys, selectedGuiKeys); err != nil {
			Errorf("No s'han pogut sincronitzar grants GUI per la politica %d: %v", p.ID, err)
		}
	}
	if err := a.DB.BumpPermissionSnapshotVersion(p.ID); err != nil {
		Errorf("No s'ha pogut invalidar snapshot de politica %d: %v", p.ID, err)
	}
	http.Redirect(w, r, "/admin/politiques", http.StatusSeeOther)
}

func extractGuiGrantSelection(r *http.Request, keySet map[string]bool) map[string]bool {
	selected := map[string]bool{}
	if r == nil {
		return selected
	}
	for _, raw := range r.Form["grant_global"] {
		key := strings.TrimSpace(raw)
		if key == "" {
			continue
		}
		if !keySet[key] {
			continue
		}
		selected[key] = true
	}
	return selected
}

func (a *App) syncPolicyGlobalGrants(politicaID int, keySet map[string]bool, selected map[string]bool) error {
	if politicaID <= 0 || a == nil || a.DB == nil {
		return nil
	}
	grants, err := a.DB.ListPoliticaGrants(politicaID)
	if err != nil {
		return err
	}
	existingGlobal := map[string]db.PoliticaGrant{}
	for _, g := range grants {
		if g.ScopeType != string(ScopeGlobal) {
			continue
		}
		if !keySet[g.PermKey] {
			continue
		}
		existingGlobal[g.PermKey] = g
	}
	for key, g := range existingGlobal {
		if selected[key] {
			continue
		}
		if err := a.DB.DeletePoliticaGrant(g.ID); err != nil {
			return err
		}
	}
	for key := range selected {
		if !keySet[key] {
			continue
		}
		if _, ok := existingGlobal[key]; ok {
			continue
		}
		_, err := a.DB.SavePoliticaGrant(&db.PoliticaGrant{
			PoliticaID:      politicaID,
			PermKey:         key,
			ScopeType:       string(ScopeGlobal),
			ScopeID:         sql.NullInt64{},
			IncludeChildren: false,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *App) AdminSavePoliticaGrant(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminPoliciesManage, PermissionTarget{}); !ok {
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
		Errorf("No s'ha pogut desar grant politica=%d grant=%d: %v", politicaID, grantID, err)
		RenderPrivateTemplate(w, r, "admin-politiques-form.html", a.politicaFormData(r, pol, false, "grants", "No s'ha pogut desar el grant", grantForm))
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/admin/politiques/%d/edit?tab=grants", politicaID), http.StatusSeeOther)
}

func (a *App) AdminDeletePoliticaGrant(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminPoliciesManage, PermissionTarget{}); !ok {
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
		Errorf("No s'ha pogut eliminar grant politica=%d grant=%d: %v", politicaID, grantID, err)
		RenderPrivateTemplate(w, r, "admin-politiques-form.html", a.politicaFormData(r, pol, false, "grants", "No s'ha pogut eliminar el grant", nil))
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/admin/politiques/%d/edit?tab=grants", politicaID), http.StatusSeeOther)
}

func (a *App) AdminApplyPoliticaJSON(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminPoliciesManage, PermissionTarget{}); !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invalid", http.StatusBadRequest)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Formulari invalid", http.StatusBadRequest)
		return
	}
	politicaID, _ := strconv.Atoi(r.FormValue("politica_id"))
	pol, err := a.DB.GetPolitica(politicaID)
	if err != nil || pol == nil {
		http.NotFound(w, r)
		return
	}
	input := r.FormValue("policy_json")
	grants, err := parsePolicyModularJSON(input, politicaID)
	if err != nil {
		data := a.politicaFormData(r, pol, false, "json", err.Error(), nil)
		data["PolicyJSON"] = input
		RenderPrivateTemplate(w, r, "admin-politiques-form.html", data)
		return
	}
	if err := a.DB.ReplacePoliticaGrants(politicaID, grants); err != nil {
		Errorf("No s'ha pogut aplicar JSON modular politica=%d: %v", politicaID, err)
		data := a.politicaFormData(r, pol, false, "json", "No s'ha pogut aplicar el JSON modular", nil)
		data["PolicyJSON"] = input
		RenderPrivateTemplate(w, r, "admin-politiques-form.html", data)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/admin/politiques/%d/edit?tab=json&ok=json_applied", politicaID), http.StatusSeeOther)
}
