package core

import (
	"net/http"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

// AdminControlCenter mostra el centre d'operacions unificat.
func (a *App) AdminControlCenter(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user, ok := a.requireAnyPermissionKey(w, r, []string{
		permKeyAdminAnalyticsView,
		permKeyAdminAuditView,
		permKeyAdminJobsManage,
		permKeyAdminPlatformSettingsEdit,
		permKeyAdminMaintenanceManage,
		permKeyAdminTransparencyManage,
	}, PermissionTarget{})
	if !ok {
		return
	}
	lang := ResolveLang(r)
	canPlatformSettings := a.HasPermission(user.ID, permKeyAdminPlatformSettingsEdit, PermissionTarget{})
	canMaintenance := a.HasPermission(user.ID, permKeyAdminMaintenanceManage, PermissionTarget{})
	canAnalytics := a.HasPermission(user.ID, permKeyAdminAnalyticsView, PermissionTarget{})
	canTransparency := a.HasPermission(user.ID, permKeyAdminTransparencyManage, PermissionTarget{})
	canJobs := a.HasPermission(user.ID, permKeyAdminJobsManage, PermissionTarget{})
	canAudit := a.HasPermission(user.ID, permKeyAdminAuditView, PermissionTarget{})
	recentAudit := []adminAuditView{}
	if canAudit {
		if rows, err := a.DB.ListAdminAudit(db.AdminAuditFilter{Limit: 5}); err == nil {
			recentAudit = buildAdminAuditViews(a, lang, rows, map[int]string{})
		}
	}
	RenderPrivateTemplate(w, r, "admin-control.html", map[string]interface{}{
		"User":                user,
		"CanPlatformSettings": canPlatformSettings,
		"CanMaintenance":      canMaintenance,
		"CanAnalytics":        canAnalytics,
		"CanTransparency":     canTransparency,
		"CanManageAdminJobs":  canJobs,
		"CanViewAdminAudit":   canAudit,
		"ModerationTypeLabels": map[string]string{
			"persona":                   T(lang, "moderation.type.persona"),
			"arxiu":                     T(lang, "moderation.type.arxiu"),
			"llibre":                    T(lang, "moderation.type.llibre"),
			"nivell":                    T(lang, "moderation.type.nivell"),
			"municipi":                  T(lang, "moderation.type.municipi"),
			"eclesiastic":               T(lang, "moderation.type.eclesiastic"),
			"municipi_mapa_version":     T(lang, "moderation.type.municipi_mapa_version"),
			"cognom_variant":            T(lang, "moderation.type.cognom_variant"),
			"cognom_referencia":         T(lang, "moderation.type.cognom_referencia"),
			"cognom_merge":              T(lang, "moderation.type.cognom_merge"),
			"event_historic":            T(lang, "moderation.type.event_historic"),
			"municipi_historia_general": T(lang, "moderation.type.municipi_historia_general"),
			"municipi_historia_fet":     T(lang, "moderation.type.municipi_historia_fet"),
			"municipi_anecdota_version": T(lang, "moderation.type.municipi_anecdota_version"),
			"registre":                  T(lang, "moderation.type.registre"),
			"registre_canvi":            T(lang, "moderation.type.registre_canvi"),
			"media_album":               T(lang, "moderation.type.media_album"),
			"media_item":                T(lang, "moderation.type.media_item"),
			"external_link":             T(lang, "moderation.type.external_link"),
			"municipi_canvi":            T(lang, "moderation.type.municipi_canvi"),
			"arxiu_canvi":               T(lang, "moderation.type.arxiu_canvi"),
			"llibre_canvi":              T(lang, "moderation.type.llibre_canvi"),
			"persona_canvi":             T(lang, "moderation.type.persona_canvi"),
			"cognom_canvi":              T(lang, "moderation.type.cognom_canvi"),
			"event_historic_canvi":      T(lang, "moderation.type.event_historic_canvi"),
		},
		"RecentAudit": recentAudit,
	})
}
