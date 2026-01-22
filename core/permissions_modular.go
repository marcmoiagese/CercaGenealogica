package core

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

// Catalog of permission keys for modular scopes.
const (
	permKeyAdminTerritoriImport = "admin.territori.import"
	permKeyAdminTerritoriExport = "admin.territori.export"
	permKeyAdminEclesImport     = "admin.eclesiastic.import"
	permKeyAdminEclesExport     = "admin.eclesiastic.export"
	permKeyAdminArxiusImport    = "admin.arxius.import"
	permKeyAdminArxiusExport    = "admin.arxius.export"
	permKeyAdminPuntsAdd        = "admin.punts.regles.add"
	permKeyAdminPuntsEdit       = "admin.punts.regles.edit"

	permKeyTerritoriPaisosView      = "territori.paisos.view"
	permKeyTerritoriPaisosCreate    = "territori.paisos.create"
	permKeyTerritoriPaisosEdit      = "territori.paisos.edit"
	permKeyTerritoriNivellsView     = "territori.nivells.view"
	permKeyTerritoriNivellsCreate   = "territori.nivells.create"
	permKeyTerritoriNivellsEdit     = "territori.nivells.edit"
	permKeyTerritoriMunicipisView   = "territori.municipis.view"
	permKeyTerritoriMunicipisCreate = "territori.municipis.create"
	permKeyTerritoriMunicipisEdit   = "territori.municipis.edit"
	permKeyTerritoriMunicipisMapesView     = "municipis.mapes.view"
	permKeyTerritoriMunicipisMapesCreate   = "municipis.mapes.create"
	permKeyTerritoriMunicipisMapesEdit     = "municipis.mapes.edit"
	permKeyTerritoriMunicipisMapesSubmit   = "municipis.mapes.submit"
	permKeyTerritoriMunicipisMapesModerate = "municipis.mapes.moderate"
	permKeyTerritoriMunicipisHistoriaCreate   = "municipis.historia.create"
	permKeyTerritoriMunicipisHistoriaEdit     = "municipis.historia.edit"
	permKeyTerritoriMunicipisHistoriaSubmit   = "municipis.historia.submit"
	permKeyTerritoriMunicipisHistoriaModerate = "municipis.historia.moderate"
	permKeyTerritoriEclesView       = "territori.eclesiastic.view"
	permKeyTerritoriEclesCreate     = "territori.eclesiastic.create"
	permKeyTerritoriEclesEdit       = "territori.eclesiastic.edit"
	permKeyTerritoriEclesImportJSON = "territori.eclesiastic.import_json"

	permKeyDocumentalsArxiusView   = "documentals.arxius.view"
	permKeyDocumentalsArxiusCreate = "documentals.arxius.create"
	permKeyDocumentalsArxiusEdit   = "documentals.arxius.edit"
	permKeyDocumentalsArxiusDelete = "documentals.arxius.delete"
	permKeyDocumentalsArxiusImport = "documentals.arxius.import"
	permKeyDocumentalsArxiusExport = "documentals.arxius.export"

	permKeyDocumentalsLlibresView          = "documentals.llibres.view"
	permKeyDocumentalsLlibresCreate        = "documentals.llibres.create"
	permKeyDocumentalsLlibresEdit          = "documentals.llibres.edit"
	permKeyDocumentalsLlibresDelete        = "documentals.llibres.delete"
	permKeyDocumentalsLlibresImport        = "documentals.llibres.import"
	permKeyDocumentalsLlibresExport        = "documentals.llibres.export"
	permKeyDocumentalsLlibresMarkIndexed   = "documentals.llibres.mark_indexed"
	permKeyDocumentalsLlibresRecalcIndex   = "documentals.llibres.recalc_index"
	permKeyDocumentalsLlibresExportCSV     = "documentals.llibres.export_csv"
	permKeyDocumentalsLlibresImportCSV     = "documentals.llibres.import_csv"
	permKeyDocumentalsLlibresViewRegistres = "documentals.llibres.view_registres"
	permKeyDocumentalsLlibresBulkIndex     = "documentals.llibres.bulk_index"

	permKeyDocumentalsRegistresEdit            = "documentals.registres.edit"
	permKeyDocumentalsRegistresEditInline      = "documentals.registres.edit_inline"
	permKeyDocumentalsRegistresLinkPerson      = "documentals.registres.link_person"
	permKeyDocumentalsRegistresConvertToPerson = "documentals.registres.convert_to_person"
)

var permissionCatalogKeys = []string{
	permKeyAdminTerritoriImport,
	permKeyAdminTerritoriExport,
	permKeyAdminEclesImport,
	permKeyAdminEclesExport,
	permKeyAdminArxiusImport,
	permKeyAdminArxiusExport,
	permKeyAdminPuntsAdd,
	permKeyAdminPuntsEdit,
	permKeyTerritoriPaisosView,
	permKeyTerritoriPaisosCreate,
	permKeyTerritoriPaisosEdit,
	permKeyTerritoriNivellsView,
	permKeyTerritoriNivellsCreate,
	permKeyTerritoriNivellsEdit,
	permKeyTerritoriMunicipisView,
	permKeyTerritoriMunicipisCreate,
	permKeyTerritoriMunicipisEdit,
	permKeyTerritoriMunicipisMapesView,
	permKeyTerritoriMunicipisMapesCreate,
	permKeyTerritoriMunicipisMapesEdit,
	permKeyTerritoriMunicipisMapesSubmit,
	permKeyTerritoriMunicipisMapesModerate,
	permKeyTerritoriMunicipisHistoriaCreate,
	permKeyTerritoriMunicipisHistoriaEdit,
	permKeyTerritoriMunicipisHistoriaSubmit,
	permKeyTerritoriMunicipisHistoriaModerate,
	permKeyTerritoriEclesView,
	permKeyTerritoriEclesCreate,
	permKeyTerritoriEclesEdit,
	permKeyTerritoriEclesImportJSON,
	permKeyDocumentalsArxiusView,
	permKeyDocumentalsArxiusCreate,
	permKeyDocumentalsArxiusEdit,
	permKeyDocumentalsArxiusDelete,
	permKeyDocumentalsArxiusImport,
	permKeyDocumentalsArxiusExport,
	permKeyDocumentalsLlibresView,
	permKeyDocumentalsLlibresCreate,
	permKeyDocumentalsLlibresEdit,
	permKeyDocumentalsLlibresDelete,
	permKeyDocumentalsLlibresImport,
	permKeyDocumentalsLlibresExport,
	permKeyDocumentalsLlibresMarkIndexed,
	permKeyDocumentalsLlibresRecalcIndex,
	permKeyDocumentalsLlibresExportCSV,
	permKeyDocumentalsLlibresImportCSV,
	permKeyDocumentalsLlibresViewRegistres,
	permKeyDocumentalsLlibresBulkIndex,
	permKeyDocumentalsRegistresEdit,
	permKeyDocumentalsRegistresEditInline,
	permKeyDocumentalsRegistresLinkPerson,
	permKeyDocumentalsRegistresConvertToPerson,
}

type scopeOption struct {
	Value    ScopeType
	LabelKey string
}

var permissionScopeOptions = []scopeOption{
	{Value: ScopeGlobal, LabelKey: "policies.grants.scope.global"},
	{Value: ScopePais, LabelKey: "policies.grants.scope.pais"},
	{Value: ScopeProvincia, LabelKey: "policies.grants.scope.provincia"},
	{Value: ScopeComarca, LabelKey: "policies.grants.scope.comarca"},
	{Value: ScopeMunicipi, LabelKey: "policies.grants.scope.municipi"},
	{Value: ScopeEcles, LabelKey: "policies.grants.scope.entitat_eclesiastica"},
	{Value: ScopeArxiu, LabelKey: "policies.grants.scope.arxiu"},
	{Value: ScopeLlibre, LabelKey: "policies.grants.scope.llibre"},
}

func permissionCatalog() []string {
	keys := make([]string, len(permissionCatalogKeys))
	copy(keys, permissionCatalogKeys)
	return keys
}

func scopeOptions() []scopeOption {
	opts := make([]scopeOption, len(permissionScopeOptions))
	copy(opts, permissionScopeOptions)
	return opts
}

func isKnownPermissionKey(key string) bool {
	key = strings.TrimSpace(key)
	if key == "" {
		return false
	}
	for _, known := range permissionCatalogKeys {
		if key == known {
			return true
		}
	}
	return false
}

var legacyTerritoryPermKeys = []string{
	permKeyTerritoriPaisosView,
	permKeyTerritoriPaisosCreate,
	permKeyTerritoriPaisosEdit,
	permKeyTerritoriNivellsView,
	permKeyTerritoriNivellsCreate,
	permKeyTerritoriNivellsEdit,
	permKeyTerritoriMunicipisView,
	permKeyTerritoriMunicipisCreate,
	permKeyTerritoriMunicipisEdit,
	permKeyTerritoriMunicipisMapesView,
	permKeyTerritoriMunicipisMapesCreate,
	permKeyTerritoriMunicipisMapesEdit,
	permKeyTerritoriMunicipisMapesSubmit,
	permKeyTerritoriMunicipisMapesModerate,
	permKeyTerritoriMunicipisHistoriaCreate,
	permKeyTerritoriMunicipisHistoriaEdit,
	permKeyTerritoriMunicipisHistoriaSubmit,
	permKeyTerritoriMunicipisHistoriaModerate,
	permKeyAdminTerritoriImport,
	permKeyAdminTerritoriExport,
}

var legacyEclesPermKeys = []string{
	permKeyTerritoriEclesView,
	permKeyTerritoriEclesCreate,
	permKeyTerritoriEclesEdit,
	permKeyTerritoriEclesImportJSON,
	permKeyAdminEclesImport,
	permKeyAdminEclesExport,
}

var legacyArchivePermKeys = []string{
	permKeyDocumentalsArxiusView,
	permKeyDocumentalsArxiusCreate,
	permKeyDocumentalsArxiusEdit,
	permKeyDocumentalsArxiusDelete,
	permKeyDocumentalsArxiusImport,
	permKeyDocumentalsArxiusExport,
	permKeyDocumentalsLlibresView,
	permKeyDocumentalsLlibresCreate,
	permKeyDocumentalsLlibresEdit,
	permKeyDocumentalsLlibresDelete,
	permKeyDocumentalsLlibresImport,
	permKeyDocumentalsLlibresExport,
	permKeyDocumentalsLlibresMarkIndexed,
	permKeyDocumentalsLlibresRecalcIndex,
	permKeyDocumentalsLlibresExportCSV,
	permKeyDocumentalsLlibresImportCSV,
	permKeyDocumentalsLlibresViewRegistres,
	permKeyDocumentalsLlibresBulkIndex,
	permKeyDocumentalsRegistresEdit,
	permKeyDocumentalsRegistresEditInline,
	permKeyDocumentalsRegistresLinkPerson,
	permKeyDocumentalsRegistresConvertToPerson,
	permKeyAdminArxiusImport,
	permKeyAdminArxiusExport,
}

var legacyPolicyPermKeys = []string{
	permKeyAdminPuntsAdd,
	permKeyAdminPuntsEdit,
}

type ScopeType string

const (
	ScopeGlobal    ScopeType = "global"
	ScopePais      ScopeType = "pais"
	ScopeProvincia ScopeType = "provincia"
	ScopeComarca   ScopeType = "comarca"
	ScopeMunicipi  ScopeType = "municipi"
	ScopeEcles     ScopeType = "entitat_eclesiastica"
	ScopeArxiu     ScopeType = "arxiu"
	ScopeLlibre    ScopeType = "llibre"
)

type PermissionTarget struct {
	PaisID      *int
	ProvinciaID *int
	ComarcaID   *int
	MunicipiID  *int
	EclesID     *int
	ArxiuID     *int
	LlibreID    *int
	ArxiuIDs    []int
}

type compiledGrant struct {
	scopeType       ScopeType
	scopeID         int
	includeChildren bool
}

type permissionSnapshot struct {
	isAdmin bool
	grants  map[string][]compiledGrant
}

type listScopeFilter struct {
	hasGlobal    bool
	arxiuIDs     []int
	llibreIDs    []int
	municipiIDs  []int
	provinciaIDs []int
	comarcaIDs   []int
	paisIDs      []int
	eclesIDs     []int
}

func (f listScopeFilter) isEmpty() bool {
	return len(f.arxiuIDs) == 0 && len(f.llibreIDs) == 0 && len(f.municipiIDs) == 0 &&
		len(f.provinciaIDs) == 0 && len(f.comarcaIDs) == 0 && len(f.paisIDs) == 0 && len(f.eclesIDs) == 0
}

type permCacheKey struct {
	userID  int
	version int
}

type permissionCacheEntry struct {
	snapshot  permissionSnapshot
	expiresAt time.Time
}

type permissionCache struct {
	mu      sync.RWMutex
	entries map[permCacheKey]permissionCacheEntry
}

const permissionCacheTTL = 10 * time.Minute

func newPermissionCache() *permissionCache {
	return &permissionCache{entries: make(map[permCacheKey]permissionCacheEntry)}
}

func (c *permissionCache) get(userID, version int) (permissionSnapshot, bool) {
	if c == nil {
		return permissionSnapshot{}, false
	}
	key := permCacheKey{userID: userID, version: version}
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok {
		return permissionSnapshot{}, false
	}
	if time.Now().After(entry.expiresAt) {
		c.mu.Lock()
		delete(c.entries, key)
		c.mu.Unlock()
		return permissionSnapshot{}, false
	}
	return entry.snapshot, true
}

func (c *permissionCache) set(userID, version int, snapshot permissionSnapshot) {
	if c == nil {
		return
	}
	key := permCacheKey{userID: userID, version: version}
	c.mu.Lock()
	for existing := range c.entries {
		if existing.userID == userID {
			delete(c.entries, existing)
		}
	}
	c.entries[key] = permissionCacheEntry{
		snapshot:  snapshot,
		expiresAt: time.Now().Add(permissionCacheTTL),
	}
	c.mu.Unlock()
}

func (a *App) permissionCache() *permissionCache {
	if a == nil {
		return nil
	}
	if a.permCache == nil {
		a.permCache = newPermissionCache()
	}
	return a.permCache
}

func (a *App) getPermissionSnapshot(userID int) (permissionSnapshot, error) {
	if a == nil || a.DB == nil || userID == 0 {
		return permissionSnapshot{grants: map[string][]compiledGrant{}}, nil
	}
	start := time.Now()
	version := 0
	if v, err := a.DB.GetUserPermissionsVersion(userID); err == nil {
		version = v
	}
	cache := a.permissionCache()
	if snap, ok := cache.get(userID, version); ok {
		Debugf("permisos cache hit user=%d version=%d in %s", userID, version, time.Since(start))
		return snap, nil
	}
	snap, err := a.buildPermissionSnapshot(userID)
	if err != nil {
		return permissionSnapshot{}, err
	}
	cache.set(userID, version, snap)
	Debugf("permisos cache miss user=%d version=%d in %s", userID, version, time.Since(start))
	return snap, nil
}

func (a *App) buildPermissionSnapshot(userID int) (permissionSnapshot, error) {
	snap := permissionSnapshot{
		grants: make(map[string][]compiledGrant),
	}
	policies, err := a.DB.ListUserPolitiques(userID)
	if err != nil {
		return snap, err
	}
	groups, err := a.DB.ListUserGroups(userID)
	if err != nil {
		return snap, err
	}
	for _, g := range groups {
		ps, err := a.DB.ListGroupPolitiques(g.ID)
		if err != nil {
			return snap, err
		}
		policies = append(policies, ps...)
	}
	byID := map[int]db.Politica{}
	for _, p := range policies {
		if p.ID <= 0 {
			continue
		}
		byID[p.ID] = p
	}
	for _, policy := range byID {
		if strings.EqualFold(policy.Nom, "admin") {
			snap.isAdmin = true
		}
		var perms db.PolicyPermissions
		permsLoaded := false
		if strings.TrimSpace(policy.Permisos) != "" {
			if err := json.Unmarshal([]byte(policy.Permisos), &perms); err == nil {
				permsLoaded = true
				if perms.Admin {
					snap.isAdmin = true
				}
			}
		}
		grants, err := a.DB.ListPoliticaGrants(policy.ID)
		if err != nil {
			return snap, err
		}
		if len(grants) > 0 {
			for _, g := range grants {
				addGrantFromDB(snap.grants, g)
			}
			continue
		}
		if !permsLoaded {
			continue
		}
		for _, key := range legacyPermKeys(perms) {
			addGlobalGrant(snap.grants, key)
		}
	}
	return snap, nil
}

func legacyPermKeys(perms db.PolicyPermissions) []string {
	if perms.Admin {
		keys := make([]string, len(permissionCatalogKeys))
		copy(keys, permissionCatalogKeys)
		return keys
	}
	keys := []string{}
	if perms.CanManageTerritory {
		keys = append(keys, legacyTerritoryPermKeys...)
	}
	if perms.CanManageEclesia {
		keys = append(keys, legacyEclesPermKeys...)
	}
	if perms.CanManageArchives {
		keys = append(keys, legacyArchivePermKeys...)
	}
	if perms.CanManagePolicies {
		keys = append(keys, legacyPolicyPermKeys...)
	}
	return keys
}

func addGlobalGrant(store map[string][]compiledGrant, permKey string) {
	addGrant(store, permKey, compiledGrant{scopeType: ScopeGlobal})
}

func addGrant(store map[string][]compiledGrant, permKey string, grant compiledGrant) {
	permKey = strings.TrimSpace(permKey)
	if permKey == "" {
		return
	}
	store[permKey] = append(store[permKey], grant)
}

func addGrantFromDB(store map[string][]compiledGrant, g db.PoliticaGrant) {
	permKey := strings.TrimSpace(g.PermKey)
	if permKey == "" {
		return
	}
	scopeType, ok := parseScopeType(g.ScopeType)
	if !ok {
		return
	}
	scopeID := 0
	if scopeType != ScopeGlobal {
		if !g.ScopeID.Valid {
			return
		}
		scopeID = int(g.ScopeID.Int64)
		if scopeID <= 0 {
			return
		}
	}
	addGrant(store, permKey, compiledGrant{
		scopeType:       scopeType,
		scopeID:         scopeID,
		includeChildren: g.IncludeChildren,
	})
}

func parseScopeType(raw string) (ScopeType, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "global":
		return ScopeGlobal, true
	case "pais":
		return ScopePais, true
	case "provincia":
		return ScopeProvincia, true
	case "comarca":
		return ScopeComarca, true
	case "municipi":
		return ScopeMunicipi, true
	case "entitat_eclesiastica":
		return ScopeEcles, true
	case "arxiu":
		return ScopeArxiu, true
	case "llibre":
		return ScopeLlibre, true
	default:
		return "", false
	}
}

func (t PermissionTarget) mostSpecificScope() ScopeType {
	switch {
	case t.LlibreID != nil:
		return ScopeLlibre
	case t.ArxiuID != nil || len(t.ArxiuIDs) > 0:
		return ScopeArxiu
	case t.MunicipiID != nil:
		return ScopeMunicipi
	case t.ComarcaID != nil:
		return ScopeComarca
	case t.ProvinciaID != nil:
		return ScopeProvincia
	case t.PaisID != nil:
		return ScopePais
	case t.EclesID != nil:
		return ScopeEcles
	default:
		return ""
	}
}

func (t PermissionTarget) idForScope(scope ScopeType) *int {
	switch scope {
	case ScopePais:
		return t.PaisID
	case ScopeProvincia:
		return t.ProvinciaID
	case ScopeComarca:
		return t.ComarcaID
	case ScopeMunicipi:
		return t.MunicipiID
	case ScopeEcles:
		return t.EclesID
	case ScopeArxiu:
		return t.ArxiuID
	case ScopeLlibre:
		return t.LlibreID
	default:
		return nil
	}
}

// HasPermission checks if the user has a permKey for the given target.
func (a *App) HasPermission(userID int, permKey string, target PermissionTarget) bool {
	snap, err := a.getPermissionSnapshot(userID)
	if err != nil {
		return false
	}
	if snap.isAdmin {
		return true
	}
	permKey = strings.TrimSpace(permKey)
	if permKey == "" {
		return false
	}
	grants := snap.grants[permKey]
	if len(grants) == 0 {
		return false
	}
	for _, g := range grants {
		if grantMatchesTarget(g, target) {
			return true
		}
	}
	return false
}

func (a *App) HasAnyPermission(userID int, permKeys []string, target PermissionTarget) bool {
	for _, key := range permKeys {
		if a.HasPermission(userID, key, target) {
			return true
		}
	}
	return false
}

func (a *App) hasAnyPermissionKey(userID int, permKey string) bool {
	snap, err := a.getPermissionSnapshot(userID)
	if err != nil {
		return false
	}
	if snap.isAdmin {
		return true
	}
	permKey = strings.TrimSpace(permKey)
	if permKey == "" {
		return false
	}
	grants := snap.grants[permKey]
	return len(grants) > 0
}

func grantMatchesTarget(grant compiledGrant, target PermissionTarget) bool {
	if grant.scopeType == ScopeGlobal {
		return true
	}
	if grant.scopeType == ScopeArxiu && len(target.ArxiuIDs) > 0 {
		for _, id := range target.ArxiuIDs {
			if id == grant.scopeID {
				if !grant.includeChildren && target.mostSpecificScope() != ScopeArxiu {
					continue
				}
				return true
			}
		}
	}
	targetID := target.idForScope(grant.scopeType)
	if targetID == nil {
		return false
	}
	if !grant.includeChildren && target.mostSpecificScope() != grant.scopeType {
		return false
	}
	return *targetID == grant.scopeID
}

func grantAppliesToListScope(grant compiledGrant, listScope ScopeType) bool {
	if grant.scopeType == ScopeGlobal {
		return true
	}
	if grant.scopeType == listScope {
		return true
	}
	if !grant.includeChildren {
		return false
	}
	switch listScope {
	case ScopeArxiu:
		switch grant.scopeType {
		case ScopePais, ScopeProvincia, ScopeComarca, ScopeMunicipi, ScopeEcles:
			return true
		default:
			return false
		}
	case ScopeLlibre:
		switch grant.scopeType {
		case ScopePais, ScopeProvincia, ScopeComarca, ScopeMunicipi, ScopeEcles, ScopeArxiu:
			return true
		default:
			return false
		}
	case ScopeMunicipi:
		switch grant.scopeType {
		case ScopePais, ScopeProvincia, ScopeComarca:
			return true
		default:
			return false
		}
	case ScopeComarca:
		switch grant.scopeType {
		case ScopePais, ScopeProvincia:
			return true
		default:
			return false
		}
	case ScopeProvincia:
		switch grant.scopeType {
		case ScopePais:
			return true
		default:
			return false
		}
	case ScopeEcles:
		switch grant.scopeType {
		case ScopePais:
			return true
		default:
			return false
		}
	default:
		return false
	}
}

func dedupeInts(items []int) []int {
	if len(items) < 2 {
		return items
	}
	seen := make(map[int]struct{}, len(items))
	out := make([]int, 0, len(items))
	for _, v := range items {
		if v <= 0 {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func (a *App) buildListScopeFilter(userID int, permKey string, listScope ScopeType) listScopeFilter {
	filter := listScopeFilter{}
	snap, err := a.getPermissionSnapshot(userID)
	if err != nil {
		return filter
	}
	if snap.isAdmin {
		filter.hasGlobal = true
		return filter
	}
	permKey = strings.TrimSpace(permKey)
	if permKey == "" {
		return filter
	}
	grants := snap.grants[permKey]
	for _, g := range grants {
		if g.scopeType == ScopeGlobal {
			filter.hasGlobal = true
			return filter
		}
		if !grantAppliesToListScope(g, listScope) {
			continue
		}
		switch g.scopeType {
		case ScopeArxiu:
			filter.arxiuIDs = append(filter.arxiuIDs, g.scopeID)
		case ScopeLlibre:
			filter.llibreIDs = append(filter.llibreIDs, g.scopeID)
		case ScopeMunicipi:
			filter.municipiIDs = append(filter.municipiIDs, g.scopeID)
		case ScopeProvincia:
			filter.provinciaIDs = append(filter.provinciaIDs, g.scopeID)
		case ScopeComarca:
			filter.comarcaIDs = append(filter.comarcaIDs, g.scopeID)
		case ScopePais:
			filter.paisIDs = append(filter.paisIDs, g.scopeID)
		case ScopeEcles:
			filter.eclesIDs = append(filter.eclesIDs, g.scopeID)
		}
	}
	filter.arxiuIDs = dedupeInts(filter.arxiuIDs)
	filter.llibreIDs = dedupeInts(filter.llibreIDs)
	filter.municipiIDs = dedupeInts(filter.municipiIDs)
	filter.provinciaIDs = dedupeInts(filter.provinciaIDs)
	filter.comarcaIDs = dedupeInts(filter.comarcaIDs)
	filter.paisIDs = dedupeInts(filter.paisIDs)
	filter.eclesIDs = dedupeInts(filter.eclesIDs)
	return filter
}

func (a *App) requirePermissionKey(w http.ResponseWriter, r *http.Request, permKey string, target PermissionTarget) (*db.User, bool) {
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return nil, false
	}
	*r = *a.withUser(r, user)
	perms, found := a.permissionsFromContext(r)
	if !found {
		perms = a.getPermissionsForUser(user.ID)
		*r = *a.withPermissions(r, perms)
	}
	if !a.HasPermission(user.ID, permKey, target) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return user, false
	}
	return user, true
}

func (a *App) requireAnyPermissionKey(w http.ResponseWriter, r *http.Request, permKeys []string, target PermissionTarget) (*db.User, bool) {
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return nil, false
	}
	*r = *a.withUser(r, user)
	perms, found := a.permissionsFromContext(r)
	if !found {
		perms = a.getPermissionsForUser(user.ID)
		*r = *a.withPermissions(r, perms)
	}
	if len(permKeys) == 0 || !a.HasAnyPermission(user.ID, permKeys, target) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return user, false
	}
	return user, true
}

func (a *App) requirePermissionKeyAnyScope(w http.ResponseWriter, r *http.Request, permKey string) (*db.User, bool) {
	user, ok := a.VerificarSessio(r)
	if !ok || user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return nil, false
	}
	*r = *a.withUser(r, user)
	perms, found := a.permissionsFromContext(r)
	if !found {
		perms = a.getPermissionsForUser(user.ID)
		*r = *a.withPermissions(r, perms)
	}
	if !a.hasAnyPermissionKey(user.ID, permKey) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return user, false
	}
	return user, true
}
