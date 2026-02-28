package core

import (
	"database/sql"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type externalSiteAdminView struct {
	Site           db.ExternalSite
	IconURL        string
	DomainsDisplay string
}

type externalLinkModerationView struct {
	ID          int
	PersonaID   int
	PersonaName string
	PersonaURL  string
	SiteLabel   string
	URL         string
	Title       string
	Meta        string
	CreatedAt   string
}

var externalSiteAccessModes = []string{"public", "account", "private", "premium", "mixed"}

func (a *App) AdminExternalSitesList(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKey(w, r, permKeyAdminExternalSitesManage, PermissionTarget{})
	if !ok {
		return
	}
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	sites, err := a.DB.ExternalSitesListAll()
	if err != nil {
		http.Error(w, "Error carregant els sites externs", http.StatusInternalServerError)
		return
	}
	views := make([]externalSiteAdminView, 0, len(sites))
	for _, site := range sites {
		views = append(views, externalSiteAdminView{
			Site:           site,
			IconURL:        externalSiteIconURL(&site),
			DomainsDisplay: externalSiteDomainsDisplay(site.Domains),
		})
	}
	lang := ResolveLang(r)
	msg := ""
	okMsg := false
	if r.URL.Query().Get("ok") != "" {
		msg = T(lang, "admin.external_sites.notice.saved")
		okMsg = true
	} else if r.URL.Query().Get("err") != "" {
		msg = T(lang, "admin.external_sites.error.save")
	}
	RenderPrivateTemplate(w, r, "admin-external-sites-list.html", map[string]interface{}{
		"User":  user,
		"Sites": views,
		"Msg":   msg,
		"Ok":    okMsg,
	})
}

func (a *App) AdminExternalSiteNew(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminExternalSitesManage, PermissionTarget{}); !ok {
		return
	}
	site := &db.ExternalSite{
		AccessMode: "mixed",
		IsActive:   true,
	}
	a.renderExternalSiteForm(w, r, site, true, "", "", "")
}

func (a *App) AdminExternalSiteEdit(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminExternalSitesManage, PermissionTarget{}); !ok {
		return
	}
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	site, err := a.findExternalSiteByID(id)
	if err != nil {
		http.Error(w, "Error carregant el site", http.StatusInternalServerError)
		return
	}
	if site == nil {
		http.NotFound(w, r)
		return
	}
	domainsInput := externalSiteDomainsInput(site.Domains)
	iconInput := ""
	if site.IconPath.Valid {
		iconInput = strings.TrimSpace(site.IconPath.String)
	}
	a.renderExternalSiteForm(w, r, site, false, domainsInput, iconInput, "")
}

func (a *App) AdminExternalSiteCreate(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminExternalSitesManage, PermissionTarget{}); !ok {
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
	site, domainsInput, iconInput, errMsg := a.parseExternalSiteForm(r, 0)
	if errMsg != "" {
		a.renderExternalSiteForm(w, r, site, true, domainsInput, iconInput, errMsg)
		return
	}
	if _, err := a.DB.ExternalSiteUpsert(site); err != nil {
		a.renderExternalSiteForm(w, r, site, true, domainsInput, iconInput, T(ResolveLang(r), "admin.external_sites.error.save"))
		return
	}
	http.Redirect(w, r, "/admin/external-sites?ok=1", http.StatusSeeOther)
}

func (a *App) AdminExternalSiteUpdate(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminExternalSitesManage, PermissionTarget{}); !ok {
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
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	site, domainsInput, iconInput, errMsg := a.parseExternalSiteForm(r, id)
	if errMsg != "" {
		a.renderExternalSiteForm(w, r, site, false, domainsInput, iconInput, errMsg)
		return
	}
	if _, err := a.DB.ExternalSiteUpsert(site); err != nil {
		a.renderExternalSiteForm(w, r, site, false, domainsInput, iconInput, T(ResolveLang(r), "admin.external_sites.error.save"))
		return
	}
	http.Redirect(w, r, "/admin/external-sites?ok=1", http.StatusSeeOther)
}

func (a *App) AdminExternalSiteToggle(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminExternalSitesManage, PermissionTarget{}); !ok {
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
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	if err := a.DB.ExternalSiteToggleActive(id); err != nil {
		http.Redirect(w, r, "/admin/external-sites?err=1", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/admin/external-sites?ok=1", http.StatusSeeOther)
}

func (a *App) AdminExternalLinksList(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKey(w, r, permKeyAdminExternalLinksModerate, PermissionTarget{})
	if !ok {
		return
	}
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	rows, err := a.DB.ExternalLinksListByStatus("pending")
	if err != nil {
		http.Error(w, "Error carregant els enllaços externs", http.StatusInternalServerError)
		return
	}
	lang := ResolveLang(r)
	items := make([]externalLinkModerationView, 0, len(rows))
	for _, row := range rows {
		items = append(items, externalLinkModerationView{
			ID:          row.ID,
			PersonaID:   row.PersonaID,
			PersonaName: externalLinkPersonaName(row),
			PersonaURL:  externalLinkPersonaURL(row.PersonaID),
			SiteLabel:   externalLinkSiteLabel(lang, row),
			URL:         strings.TrimSpace(row.URL),
			Title:       strings.TrimSpace(row.Title.String),
			Meta:        strings.TrimSpace(row.Meta.String),
			CreatedAt:   formatAuditTime(row.CreatedAt),
		})
	}
	msg := ""
	okMsg := false
	if r.URL.Query().Get("ok") != "" {
		msg = T(lang, "admin.external_links.notice.ok")
		okMsg = true
	} else if r.URL.Query().Get("err") != "" {
		msg = T(lang, "admin.external_links.error")
	}
	RenderPrivateTemplate(w, r, "admin-external-links-list.html", map[string]interface{}{
		"User":  user,
		"Links": items,
		"Msg":   msg,
		"Ok":    okMsg,
	})
}

func (a *App) AdminExternalLinkApprove(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminExternalLinksModerate, PermissionTarget{}); !ok {
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
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	if err := a.DB.ExternalLinkModerate(id, "approved"); err != nil {
		http.Redirect(w, r, "/admin/external-links?err=1", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/admin/external-links?ok=1", http.StatusSeeOther)
}

func (a *App) AdminExternalLinkReject(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyAdminExternalLinksModerate, PermissionTarget{}); !ok {
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
	id := extractID(r.URL.Path)
	if id == 0 {
		http.NotFound(w, r)
		return
	}
	if err := a.DB.ExternalLinkModerate(id, "rejected"); err != nil {
		http.Redirect(w, r, "/admin/external-links?err=1", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/admin/external-links?ok=1", http.StatusSeeOther)
}

func (a *App) renderExternalSiteForm(w http.ResponseWriter, r *http.Request, site *db.ExternalSite, isNew bool, domainsInput string, iconInput string, errMsg string) {
	accessMode := strings.TrimSpace(site.AccessMode)
	if accessMode == "" {
		site.AccessMode = "mixed"
	}
	data := map[string]interface{}{
		"Site":          site,
		"IsNew":         isNew,
		"DomainsInput":  domainsInput,
		"IconInput":     iconInput,
		"AccessModes":   externalSiteAccessModes,
		"IconOptions":   listExternalSiteIcons(),
		"IconPreview":   externalSiteIconURLFromInput(site.Slug, iconInput),
		"CanManageArxius": true,
	}
	if errMsg != "" {
		data["Error"] = errMsg
	}
	RenderPrivateTemplate(w, r, "admin-external-sites-form.html", data)
}

func (a *App) parseExternalSiteForm(r *http.Request, id int) (*db.ExternalSite, string, string, string) {
	lang := ResolveLang(r)
	if err := r.ParseForm(); err != nil {
		return &db.ExternalSite{ID: id}, "", "", T(lang, "common.error")
	}
	name := strings.TrimSpace(r.FormValue("name"))
	slug := strings.TrimSpace(r.FormValue("slug"))
	domainsInput := strings.TrimSpace(r.FormValue("domains"))
	accessMode := strings.TrimSpace(r.FormValue("access_mode"))
	iconInput := strings.TrimSpace(r.FormValue("icon_path"))
	isActive := r.FormValue("is_active") == "1"

	if slug == "" {
		slug = slugifyExternalSite(name)
		if slug == "" {
			slug = slugifyExternalSite(externalSiteFirstDomain(domainsInput))
		}
	}
	if name == "" || slug == "" {
		return &db.ExternalSite{
			ID:         id,
			Name:       name,
			Slug:       slug,
			AccessMode: accessMode,
			IsActive:   isActive,
		}, domainsInput, iconInput, T(lang, "admin.external_sites.error.required")
	}
	domains := db.ParseExternalDomains(domainsInput)
	if len(domains) == 0 || !externalDomainsValid(domains) {
		return &db.ExternalSite{
			ID:         id,
			Name:       name,
			Slug:       slug,
			AccessMode: accessMode,
			IsActive:   isActive,
		}, domainsInput, iconInput, T(lang, "admin.external_sites.error.invalid")
	}
	if !externalAccessModeValid(accessMode) {
		return &db.ExternalSite{
			ID:         id,
			Name:       name,
			Slug:       slug,
			AccessMode: accessMode,
			IsActive:   isActive,
		}, domainsInput, iconInput, T(lang, "admin.external_sites.error.invalid")
	}
	domainsValue := strings.Join(domains, "\n")
	iconValue := normalizeExternalSiteIcon(iconInput)
	site := &db.ExternalSite{
		ID:         id,
		Name:       name,
		Slug:       slug,
		Domains:    domainsValue,
		AccessMode: accessMode,
		IsActive:   isActive,
	}
	if iconValue != "" {
		site.IconPath = sql.NullString{String: iconValue, Valid: true}
	}
	return site, domainsValue, iconInput, ""
}

func (a *App) findExternalSiteByID(id int) (*db.ExternalSite, error) {
	sites, err := a.DB.ExternalSitesListAll()
	if err != nil {
		return nil, err
	}
	for _, site := range sites {
		if site.ID == id {
			found := site
			return &found, nil
		}
	}
	return nil, nil
}

func externalSiteIconURL(site *db.ExternalSite) string {
	if site == nil {
		return "/static/img/ext-sites/unknown.svg"
	}
	icon := ""
	if site.IconPath.Valid {
		icon = strings.TrimSpace(site.IconPath.String)
	}
	slug := strings.TrimSpace(site.Slug)
	if slug == "" {
		slug = "unknown"
	}
	if icon == "" {
		if slug == "unknown" {
			icon = "/static/img/ext-sites/unknown.svg"
		} else {
			icon = "/static/img/ext-sites/" + slug + ".svg"
		}
	} else if !strings.HasPrefix(icon, "/") && !strings.HasPrefix(icon, "http://") && !strings.HasPrefix(icon, "https://") {
		icon = "/" + icon
	}
	return icon
}

func externalSiteIconURLFromInput(slug, input string) string {
	input = strings.TrimSpace(input)
	if input != "" {
		icon := normalizeExternalSiteIcon(input)
		if icon != "" && !strings.HasPrefix(icon, "/") && !strings.HasPrefix(icon, "http://") && !strings.HasPrefix(icon, "https://") {
			icon = "/" + icon
		}
		if icon != "" {
			return icon
		}
	}
	site := &db.ExternalSite{Slug: slug}
	return externalSiteIconURL(site)
}

func externalSiteDomainsDisplay(raw string) string {
	domains := db.ParseExternalDomains(raw)
	if len(domains) == 0 {
		return ""
	}
	return strings.Join(domains, ", ")
}

func externalSiteDomainsInput(raw string) string {
	domains := db.ParseExternalDomains(raw)
	if len(domains) == 0 {
		return strings.TrimSpace(raw)
	}
	return strings.Join(domains, "\n")
}

func externalSiteFirstDomain(raw string) string {
	domains := db.ParseExternalDomains(raw)
	if len(domains) == 0 {
		return ""
	}
	return domains[0]
}

func externalDomainsValid(domains []string) bool {
	for _, domain := range domains {
		domain = strings.TrimSpace(domain)
		if domain == "" {
			return false
		}
		if strings.ContainsAny(domain, "/\\?#!@") {
			return false
		}
	}
	return true
}

func externalAccessModeValid(val string) bool {
	val = strings.TrimSpace(val)
	if val == "" {
		return false
	}
	for _, allowed := range externalSiteAccessModes {
		if val == allowed {
			return true
		}
	}
	return false
}

func slugifyExternalSite(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	raw = strings.ToLower(stripDiacritics(raw))
	var b strings.Builder
	lastDash := false
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
			lastDash = false
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		case r == '-' || r == '_' || r == '.' || r == ' ':
			if !lastDash && b.Len() > 0 {
				b.WriteByte('-')
				lastDash = true
			}
		default:
			continue
		}
	}
	return strings.Trim(b.String(), "-")
}

func normalizeExternalSiteIcon(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") || strings.HasPrefix(raw, "/") {
		return raw
	}
	if !strings.Contains(raw, "/") {
		if !strings.HasSuffix(raw, ".svg") {
			raw += ".svg"
		}
		return "static/img/ext-sites/" + raw
	}
	return raw
}

func listExternalSiteIcons() []string {
	entries, err := os.ReadDir("static/img/ext-sites")
	if err != nil {
		return nil
	}
	icons := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(strings.ToLower(name), ".svg") {
			continue
		}
		icons = append(icons, name)
	}
	sort.Strings(icons)
	return icons
}

func externalLinkPersonaName(row db.ExternalLinkAdminRow) string {
	if strings.TrimSpace(row.PersonaNomComplet.String) != "" {
		return strings.TrimSpace(row.PersonaNomComplet.String)
	}
	parts := []string{
		strings.TrimSpace(row.PersonaNom.String),
		strings.TrimSpace(row.PersonaCognom1.String),
		strings.TrimSpace(row.PersonaCognom2.String),
	}
	name := strings.TrimSpace(strings.Join(parts, " "))
	if name != "" {
		return name
	}
	return "—"
}

func externalLinkPersonaURL(personaID int) string {
	if personaID <= 0 {
		return ""
	}
	return "/persones/" + strconv.Itoa(personaID)
}

func externalLinkSiteLabel(lang string, row db.ExternalLinkAdminRow) string {
	slug := strings.TrimSpace(row.SiteSlug.String)
	name := strings.TrimSpace(row.SiteName.String)
	if name != "" {
		return name
	}
	if slug == "" || slug == "unknown" {
		return T(lang, "persons.external.site.unknown")
	}
	return slug
}
