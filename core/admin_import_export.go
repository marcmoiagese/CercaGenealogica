package core

import (
	"net/http"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func (a *App) AdminImportExport(w http.ResponseWriter, r *http.Request) {
	permKeys := []string{
		permKeyAdminTerritoriImport,
		permKeyAdminTerritoriExport,
		permKeyAdminEclesImport,
		permKeyAdminEclesExport,
		permKeyDocumentalsArxiusImport,
		permKeyDocumentalsArxiusExport,
		permKeyDocumentalsLlibresImport,
		permKeyDocumentalsLlibresExport,
		permKeyTerritoriConfessionalImportExportView,
		permKeyTerritoriConfessionalImportExportExport,
		permKeyTerritoriConfessionalImportExportImport,
	}
	user, ok := a.requireAnyPermissionKey(w, r, permKeys, PermissionTarget{})
	if !ok {
		return
	}
	a.renderAdminImportExportPage(w, r, user, nil)
}

func (a *App) renderAdminImportExportPage(w http.ResponseWriter, r *http.Request, user *db.User, extra map[string]interface{}) {
	data := a.adminImportExportPageData(r, user)
	for key, value := range extra {
		data[key] = value
	}
	RenderPrivateTemplate(w, r, "admin-import-export.html", data)
}

func (a *App) adminImportExportPageData(r *http.Request, user *db.User) map[string]interface{} {
	canTerritoriImport := a.HasPermission(user.ID, permKeyAdminTerritoriImport, PermissionTarget{})
	canTerritoriExport := a.HasPermission(user.ID, permKeyAdminTerritoriExport, PermissionTarget{})
	canEclesImport := a.HasPermission(user.ID, permKeyAdminEclesImport, PermissionTarget{})
	canEclesExport := a.HasPermission(user.ID, permKeyAdminEclesExport, PermissionTarget{})
	canArxiusImport := a.HasPermission(user.ID, permKeyDocumentalsArxiusImport, PermissionTarget{})
	canArxiusExport := a.HasPermission(user.ID, permKeyDocumentalsArxiusExport, PermissionTarget{})
	canLlibresImport := a.HasPermission(user.ID, permKeyDocumentalsLlibresImport, PermissionTarget{})
	canLlibresExport := a.HasPermission(user.ID, permKeyDocumentalsLlibresExport, PermissionTarget{})
	canConfessionalView := a.HasPermission(user.ID, permKeyTerritoriConfessionalImportExportView, PermissionTarget{})
	canConfessionalImport := a.HasPermission(user.ID, permKeyTerritoriConfessionalImportExportImport, PermissionTarget{})
	canConfessionalExport := a.HasPermission(user.ID, permKeyTerritoriConfessionalImportExportExport, PermissionTarget{})

	activeTab := strings.TrimSpace(r.URL.Query().Get("tab"))
	activeTab = resolveImportExportTab(activeTab, map[string]bool{
		"territori":    canTerritoriImport || canTerritoriExport,
		"eclesiastic":  canEclesImport || canEclesExport,
		"confessional": canConfessionalView || canConfessionalImport || canConfessionalExport,
		"arxius":       canArxiusImport || canArxiusExport,
		"llibres":      canLlibresImport || canLlibresExport,
	})
	requestedSubtab := strings.TrimSpace(r.URL.Query().Get("subtab"))
	territoriSubtab := resolveImportExportSubtab("territori", requestedSubtab, canTerritoriImport, canTerritoriExport)
	eclesSubtab := resolveImportExportSubtab("eclesiastic", requestedSubtab, canEclesImport, canEclesExport)
	confessionalSubtab := resolveImportExportSubtab("confessional", requestedSubtab, canConfessionalImport, canConfessionalExport)
	arxiusSubtab := resolveImportExportSubtab("arxius", requestedSubtab, canArxiusImport, canArxiusExport)
	llibresSubtab := resolveImportExportSubtab("llibres", requestedSubtab, canLlibresImport, canLlibresExport)

	q := r.URL.Query()
	errMsg := ""
	if q.Get("err") != "" {
		errMsg = T(ResolveLang(r), "common.error")
	}

	data := map[string]interface{}{
		"ActiveTab":                      activeTab,
		"TerritoriSubtab":                territoriSubtab,
		"EclesSubtab":                    eclesSubtab,
		"ConfessionalSubtab":             confessionalSubtab,
		"ArxiusSubtab":                   arxiusSubtab,
		"LlibresSubtab":                  llibresSubtab,
		"CanTerritoriImport":             canTerritoriImport,
		"CanTerritoriExport":             canTerritoriExport,
		"CanEclesImport":                 canEclesImport,
		"CanEclesExport":                 canEclesExport,
		"CanConfessionalView":            canConfessionalView,
		"CanConfessionalImport":          canConfessionalImport,
		"CanConfessionalExport":          canConfessionalExport,
		"ConfessionalDryRunAvailable":    false,
		"CanArxiusImport":                canArxiusImport,
		"CanArxiusExport":                canArxiusExport,
		"CanLlibresImport":               canLlibresImport,
		"CanLlibresExport":               canLlibresExport,
		"ConfessionalImportRun":          q.Get("import") == "1" && activeTab == "confessional" && confessionalSubtab == "confessional-import",
		"ConfessionalEntitiesTotal":      parseIntQuery(q.Get("conf_entities_total")),
		"ConfessionalEntitiesCreated":    parseIntQuery(q.Get("conf_entities_created")),
		"ConfessionalEntitiesSkipped":    parseIntQuery(q.Get("conf_entities_skipped")),
		"ConfessionalHierarchyTotal":     parseIntQuery(q.Get("conf_hierarchy_total")),
		"ConfessionalHierarchyCreated":   parseIntQuery(q.Get("conf_hierarchy_created")),
		"ConfessionalHierarchySkipped":   parseIntQuery(q.Get("conf_hierarchy_skipped")),
		"ConfessionalTerritorialTotal":   parseIntQuery(q.Get("conf_territorial_total")),
		"ConfessionalTerritorialCreated": parseIntQuery(q.Get("conf_territorial_created")),
		"ConfessionalTerritorialSkipped": parseIntQuery(q.Get("conf_territorial_skipped")),
		"ConfessionalArchiveTotal":       parseIntQuery(q.Get("conf_archive_total")),
		"ConfessionalArchiveCreated":     parseIntQuery(q.Get("conf_archive_created")),
		"ConfessionalArchiveSkipped":     parseIntQuery(q.Get("conf_archive_skipped")),
	}

	data["TerritoriImportRun"] = q.Get("import") == "1" && activeTab == "territori" && territoriSubtab == "territori-import"
	if activeTab == "territori" {
		data["TerritoriMsg"] = errMsg
	} else {
		data["TerritoriMsg"] = ""
	}
	data["TerritoriCountriesCreated"] = parseIntQuery(q.Get("countries_created"))
	data["TerritoriLevelsTotal"] = parseIntQuery(q.Get("levels_total"))
	data["TerritoriLevelsCreated"] = parseIntQuery(q.Get("levels_created"))
	data["TerritoriLevelsSkipped"] = parseIntQuery(q.Get("levels_skipped"))
	data["TerritoriLevelsErrors"] = parseIntQuery(q.Get("levels_errors"))
	data["TerritoriMunicipisTotal"] = parseIntQuery(q.Get("municipis_total"))
	data["TerritoriMunicipisCreated"] = parseIntQuery(q.Get("municipis_created"))
	data["TerritoriMunicipisSkipped"] = parseIntQuery(q.Get("municipis_skipped"))
	data["TerritoriMunicipisErrors"] = parseIntQuery(q.Get("municipis_errors"))

	data["EclesImportRun"] = q.Get("import") == "1" && activeTab == "eclesiastic" && eclesSubtab == "eclesiastic-import"
	if activeTab == "eclesiastic" {
		data["EclesMsg"] = errMsg
	} else {
		data["EclesMsg"] = ""
	}
	data["EclesEntitatsTotal"] = parseIntQuery(q.Get("entitats_total"))
	data["EclesEntitatsCreated"] = parseIntQuery(q.Get("entitats_created"))
	data["EclesEntitatsSkipped"] = parseIntQuery(q.Get("entitats_skipped"))
	data["EclesEntitatsErrors"] = parseIntQuery(q.Get("entitats_errors"))
	data["EclesRelacionsTotal"] = parseIntQuery(q.Get("relacions_total"))
	data["EclesRelacionsCreated"] = parseIntQuery(q.Get("relacions_created"))
	data["EclesRelacionsSkipped"] = parseIntQuery(q.Get("relacions_skipped"))
	data["EclesRelacionsErrors"] = parseIntQuery(q.Get("relacions_errors"))

	if activeTab == "confessional" {
		data["ConfessionalMsg"] = errMsg
	} else {
		data["ConfessionalMsg"] = ""
	}

	data["ArxiusImportRun"] = q.Get("import") == "1" && activeTab == "arxius" && arxiusSubtab == "arxius-import"
	if activeTab == "arxius" {
		data["ArxiusMsg"] = errMsg
	} else {
		data["ArxiusMsg"] = ""
	}
	data["ArxiusTotal"] = parseIntQuery(q.Get("arxius_total"))
	data["ArxiusCreated"] = parseIntQuery(q.Get("arxius_created"))
	data["ArxiusSkipped"] = parseIntQuery(q.Get("arxius_skipped"))
	data["ArxiusErrors"] = parseIntQuery(q.Get("arxius_errors"))

	data["LlibresImportRun"] = q.Get("import") == "1" && activeTab == "llibres" && llibresSubtab == "llibres-import"
	if activeTab == "llibres" {
		data["LlibresMsg"] = errMsg
	} else {
		data["LlibresMsg"] = ""
	}
	data["LlibresTotal"] = parseIntQuery(q.Get("llibres_total"))
	data["LlibresCreated"] = parseIntQuery(q.Get("llibres_created"))
	data["LlibresSkipped"] = parseIntQuery(q.Get("llibres_skipped"))
	data["LlibresErrors"] = parseIntQuery(q.Get("llibres_errors"))

	return data
}

func resolveImportExportTab(requested string, available map[string]bool) string {
	if requested != "" && available[requested] {
		return requested
	}
	for _, candidate := range []string{"territori", "eclesiastic", "confessional", "arxius", "llibres"} {
		if available[candidate] {
			return candidate
		}
	}
	return ""
}

func resolveImportExportSubtab(prefix, requested string, canImport, canExport bool) string {
	importID := prefix + "-import"
	exportID := prefix + "-export"
	if requested == importID && canImport {
		return importID
	}
	if requested == exportID && canExport {
		return exportID
	}
	if canImport {
		return importID
	}
	if canExport {
		return exportID
	}
	return importID
}
