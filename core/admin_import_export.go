package core

import (
	"net/http"
	"strings"
)

func (a *App) AdminImportExport(w http.ResponseWriter, r *http.Request) {
	permKeys := []string{
		permKeyAdminTerritoriImport,
		permKeyAdminTerritoriExport,
		permKeyAdminEclesImport,
		permKeyAdminEclesExport,
		permKeyAdminArxiusImport,
		permKeyAdminArxiusExport,
		permKeyDocumentalsLlibresImport,
		permKeyDocumentalsLlibresExport,
	}
	user, ok := a.requireAnyPermissionKey(w, r, permKeys, PermissionTarget{})
	if !ok {
		return
	}
	canTerritoriImport := a.HasPermission(user.ID, permKeyAdminTerritoriImport, PermissionTarget{})
	canTerritoriExport := a.HasPermission(user.ID, permKeyAdminTerritoriExport, PermissionTarget{})
	canEclesImport := a.HasPermission(user.ID, permKeyAdminEclesImport, PermissionTarget{})
	canEclesExport := a.HasPermission(user.ID, permKeyAdminEclesExport, PermissionTarget{})
	canArxiusImport := a.HasPermission(user.ID, permKeyAdminArxiusImport, PermissionTarget{})
	canArxiusExport := a.HasPermission(user.ID, permKeyAdminArxiusExport, PermissionTarget{})
	canLlibresImport := a.HasPermission(user.ID, permKeyDocumentalsLlibresImport, PermissionTarget{})
	canLlibresExport := a.HasPermission(user.ID, permKeyDocumentalsLlibresExport, PermissionTarget{})

	activeTab := strings.TrimSpace(r.URL.Query().Get("tab"))
	activeTab = resolveImportExportTab(activeTab, map[string]bool{
		"territori":   canTerritoriImport || canTerritoriExport,
		"eclesiastic": canEclesImport || canEclesExport,
		"arxius":      canArxiusImport || canArxiusExport,
		"llibres":     canLlibresImport || canLlibresExport,
	})
	requestedSubtab := strings.TrimSpace(r.URL.Query().Get("subtab"))
	territoriSubtab := resolveImportExportSubtab("territori", requestedSubtab, canTerritoriImport, canTerritoriExport)
	eclesSubtab := resolveImportExportSubtab("eclesiastic", requestedSubtab, canEclesImport, canEclesExport)
	arxiusSubtab := resolveImportExportSubtab("arxius", requestedSubtab, canArxiusImport, canArxiusExport)
	llibresSubtab := resolveImportExportSubtab("llibres", requestedSubtab, canLlibresImport, canLlibresExport)

	q := r.URL.Query()
	errMsg := ""
	if q.Get("err") != "" {
		errMsg = T(ResolveLang(r), "common.error")
	}

	data := map[string]interface{}{
		"ActiveTab":          activeTab,
		"TerritoriSubtab":    territoriSubtab,
		"EclesSubtab":        eclesSubtab,
		"ArxiusSubtab":       arxiusSubtab,
		"LlibresSubtab":      llibresSubtab,
		"CanTerritoriImport": canTerritoriImport,
		"CanTerritoriExport": canTerritoriExport,
		"CanEclesImport":     canEclesImport,
		"CanEclesExport":     canEclesExport,
		"CanArxiusImport":    canArxiusImport,
		"CanArxiusExport":    canArxiusExport,
		"CanLlibresImport":   canLlibresImport,
		"CanLlibresExport":   canLlibresExport,
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

	RenderPrivateTemplate(w, r, "admin-import-export.html", data)
}

func resolveImportExportTab(requested string, available map[string]bool) string {
	if requested != "" && available[requested] {
		return requested
	}
	for _, candidate := range []string{"territori", "eclesiastic", "arxius", "llibres"} {
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
