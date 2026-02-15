package core

import (
	"net/http"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

var platformSettingFields = map[string]string{
	"brand_name":       "site.brand_name",
	"footer_tagline":   "site.footer_tagline",
	"contact_email":    "site.contact_email",
	"contact_location": "site.contact_location",
}

// AdminPlatformConfig permet editar la marca pública de la plataforma.
func (a *App) AdminPlatformConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	user, ok := a.requirePermissionKey(w, r, permKeyAdminPlatformSettingsEdit, PermissionTarget{})
	if !ok {
		return
	}
	if r.Method == http.MethodPost {
		a.adminPlatformConfigSave(w, r, user)
		return
	}
	a.renderPlatformConfig(w, r, user, "", false)
}

func (a *App) adminPlatformConfigSave(w http.ResponseWriter, r *http.Request, user *db.User) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Formulari invàlid", http.StatusBadRequest)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, "CSRF invàlid", http.StatusBadRequest)
		return
	}
	for field, key := range platformSettingFields {
		val := strings.TrimSpace(r.FormValue(field))
		if err := a.DB.UpsertPlatformSetting(key, val, user.ID); err != nil {
			Errorf("Error desant plataforma setting %s: %v", key, err)
			a.renderPlatformConfig(w, r, user, T(ResolveLang(r), "common.error"), false)
			return
		}
	}
	a.logAdminAudit(r, user.ID, auditActionPlatformUpdate, "platform", 0, nil)
	InvalidatePlatformSettingsCache()
	http.Redirect(w, r, "/admin/plataforma/config?ok=1", http.StatusSeeOther)
}

func (a *App) renderPlatformConfig(w http.ResponseWriter, r *http.Request, user *db.User, msg string, okMsg bool) {
	values := map[string]string{}
	items, err := a.DB.ListPlatformSettings()
	if err != nil {
		Errorf("Error carregant platform settings: %v", err)
	} else {
		for _, row := range items {
			values[row.Key] = row.Value
		}
	}
	if msg == "" {
		if r.URL.Query().Get("ok") != "" {
			msg = T(ResolveLang(r), "common.saved")
			okMsg = true
		} else if r.URL.Query().Get("err") != "" {
			msg = T(ResolveLang(r), "common.error")
		}
	}
	RenderPrivateTemplate(w, r, "admin-platform-config.html", map[string]interface{}{
		"User":   user,
		"Values": values,
		"Msg":    msg,
		"Ok":     okMsg,
	})
}
