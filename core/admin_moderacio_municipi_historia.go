package core

import (
	"net/http"
	"strconv"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func (a *App) AdminModeracioHistoriaGeneralPreview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user, perms, canModerateAll, ok := a.requireModeracioUser(w, r)
	if !ok {
		return
	}
	versionID := extractID(r.URL.Path)
	if versionID <= 0 {
		http.NotFound(w, r)
		return
	}
	version, err := a.DB.GetMunicipiHistoriaGeneralVersion(versionID)
	if err != nil || version == nil {
		http.NotFound(w, r)
		return
	}
	munID, err := a.DB.ResolveMunicipiIDByHistoriaGeneralVersionID(versionID)
	if err != nil || munID <= 0 {
		http.NotFound(w, r)
		return
	}
	if !canModerateAll && !a.canModerateHistoriaObject(user, perms, "municipi_historia_general", versionID) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	mun, err := a.DB.GetMunicipi(munID)
	if err != nil || mun == nil {
		http.NotFound(w, r)
		return
	}
	var current *db.MunicipiHistoriaGeneralVersion
	if historia, err := a.DB.GetMunicipiHistoriaByMunicipiID(munID); err == nil && historia != nil && historia.CurrentGeneralVersionID.Valid {
		currentID := int(historia.CurrentGeneralVersionID.Int64)
		if currentID > 0 {
			if row, err := a.DB.GetMunicipiHistoriaGeneralVersion(currentID); err == nil {
				current = row
			}
		}
	}
	lang := resolveUserLang(r, user)
	RenderPrivateTemplateLang(w, r, "admin-moderacio-historia-general.html", lang, map[string]interface{}{
		"User":        user,
		"Municipi":    mun,
		"Pending":     version,
		"Current":     current,
		"ObjectType":  "municipi_historia_general",
		"ReturnURL":   "/moderacio",
		"HistoriaURL": "/territori/municipis/" + strconv.Itoa(mun.ID) + "/historia",
	})
}

func (a *App) AdminModeracioHistoriaFetPreview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user, perms, canModerateAll, ok := a.requireModeracioUser(w, r)
	if !ok {
		return
	}
	versionID := extractID(r.URL.Path)
	if versionID <= 0 {
		http.NotFound(w, r)
		return
	}
	version, err := a.DB.GetMunicipiHistoriaFetVersion(versionID)
	if err != nil || version == nil {
		http.NotFound(w, r)
		return
	}
	munID, err := a.DB.ResolveMunicipiIDByHistoriaFetVersionID(versionID)
	if err != nil || munID <= 0 {
		http.NotFound(w, r)
		return
	}
	if !canModerateAll && !a.canModerateHistoriaObject(user, perms, "municipi_historia_fet", versionID) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	mun, err := a.DB.GetMunicipi(munID)
	if err != nil || mun == nil {
		http.NotFound(w, r)
		return
	}
	var current *db.MunicipiHistoriaFetVersion
	if fetRow, err := a.DB.GetMunicipiHistoriaFet(version.FetID); err == nil && fetRow != nil && fetRow.CurrentVersionID.Valid {
		currentID := int(fetRow.CurrentVersionID.Int64)
		if currentID > 0 {
			if row, err := a.DB.GetMunicipiHistoriaFetVersion(currentID); err == nil {
				current = row
			}
		}
	}
	pendingDate := historiaDateLabel(*version)
	currentDate := ""
	if current != nil {
		currentDate = historiaDateLabel(*current)
	}
	lang := resolveUserLang(r, user)
	RenderPrivateTemplateLang(w, r, "admin-moderacio-historia-fet.html", lang, map[string]interface{}{
		"User":         user,
		"Municipi":     mun,
		"Pending":      version,
		"Current":      current,
		"PendingDate":  pendingDate,
		"CurrentDate":  currentDate,
		"ObjectType":   "municipi_historia_fet",
		"ReturnURL":    "/moderacio",
		"HistoriaURL":  "/territori/municipis/" + strconv.Itoa(mun.ID) + "/historia",
	})
}
