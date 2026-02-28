package integration

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

type externalLinksAPIResponse struct {
	PersonaID int                  `json:"persona_id"`
	Groups    []externalLinksGroup `json:"groups"`
}

type externalLinksGroup struct {
	Site  externalLinksSite   `json:"site"`
	Items []externalLinksItem `json:"items"`
}

type externalLinksSite struct {
	Slug       string `json:"slug"`
	Name       string `json:"name"`
	AccessMode string `json:"access_mode"`
	IconURL    string `json:"icon_url"`
}

type externalLinksItem struct {
	URL   string `json:"url"`
	Count int    `json:"count"`
}

func TestExternalLinksPublicAPIAndUnknownSite(t *testing.T) {
	forEachArbreDB(t, func(t *testing.T, label string, app *core.App, database db.DB, _ string) {
		user, _ := createF7UserWithSession(t, database)
		personaID := createTestPersona(t, database, user.ID, "Test", "Externs")

		site := &db.ExternalSite{
			Slug:       "geneanet",
			Name:       "Geneanet",
			Domains:    "geneanet.org",
			AccessMode: "public",
			IsActive:   true,
		}
		if _, err := database.ExternalSiteUpsert(site); err != nil {
			t.Fatalf("ExternalSiteUpsert ha fallat (%s): %v", label, err)
		}

		linkID, err := database.ExternalLinkInsertPending(personaID, user.ID, "https://www.geneanet.org/individual/123", "Perfil Geneanet")
		if err != nil {
			t.Fatalf("ExternalLinkInsertPending known ha fallat (%s): %v", label, err)
		}
		if err := database.ExternalLinkModerate(linkID, "approved"); err != nil {
			t.Fatalf("ExternalLinkModerate known ha fallat (%s): %v", label, err)
		}

		unknownID, err := database.ExternalLinkInsertPending(personaID, user.ID, "https://unknown.example/person/1", "Perfil Desconegut")
		if err != nil {
			t.Fatalf("ExternalLinkInsertPending unknown ha fallat (%s): %v", label, err)
		}
		if err := database.ExternalLinkModerate(unknownID, "approved"); err != nil {
			t.Fatalf("ExternalLinkModerate unknown ha fallat (%s): %v", label, err)
		}

		resp := fetchExternalLinks(t, app, personaID)
		if resp.PersonaID != personaID {
			t.Fatalf("persona_id incorrecte (%s): got %d", label, resp.PersonaID)
		}
		if len(resp.Groups) != 2 {
			t.Fatalf("esperava 2 groups (%s), got %d", label, len(resp.Groups))
		}
		known := findExternalGroup(resp.Groups, "geneanet")
		if known == nil {
			t.Fatalf("no trobo group geneanet (%s)", label)
		}
		if known.Site.AccessMode != "public" {
			t.Fatalf("access_mode incorrecte (%s): %s", label, known.Site.AccessMode)
		}
		if len(known.Items) != 1 || known.Items[0].Count != 1 {
			t.Fatalf("items geneanet incorrectes (%s): %#v", label, known.Items)
		}

		unknown := findExternalGroup(resp.Groups, "unknown")
		if unknown == nil {
			t.Fatalf("no trobo group unknown (%s)", label)
		}
		if !strings.HasSuffix(unknown.Site.IconURL, "/static/img/ext-sites/unknown.svg") {
			t.Fatalf("icona unknown incorrecta (%s): %s", label, unknown.Site.IconURL)
		}
	})
}

func TestExternalLinksSubmitValidationAndDup(t *testing.T) {
	forEachArbreDB(t, func(t *testing.T, label string, app *core.App, database db.DB, _ string) {
		user, _ := createF7UserWithSession(t, database)
		personaID := createTestPersona(t, database, user.ID, "Test", "Submit")

		payload := map[string]string{"url": "https://example.com/profile/1", "title": "Test"}

		// Sense login
		req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/persones/%d/external-links", personaID), bytes.NewBufferString(`{"url":"https://example.com"}`))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		app.PersonesExternalLinksAPI(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("esperava 401 sense login (%s), got %d", label, rr.Code)
		}

		// Amb login però sense CSRF
		req = httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/persones/%d/external-links", personaID), bytes.NewBufferString(`{"url":"https://example.com"}`))
		req.Header.Set("Content-Type", "application/json")
		req.AddCookie(createSessionCookie(t, database, user.ID, "sess_ext_"+label))
		rr = httptest.NewRecorder()
		app.PersonesExternalLinksAPI(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("esperava 400 sense CSRF (%s), got %d", label, rr.Code)
		}

		// URL invàlida
		code, status := postExternalLinkJSON(t, app, personaID, user.ID, database, map[string]string{"url": "ftp://example.com", "title": "bad"})
		if code != http.StatusBadRequest || status != "invalid" {
			t.Fatalf("esperava invalid (%s), got %d/%s", label, code, status)
		}

		// OK
		code, status = postExternalLinkJSON(t, app, personaID, user.ID, database, payload)
		if code != http.StatusOK || status != "ok" {
			t.Fatalf("esperava ok (%s), got %d/%s", label, code, status)
		}
		rows, err := database.ExternalLinksListByPersona(personaID, "")
		if err != nil || len(rows) != 1 {
			t.Fatalf("esperava 1 link (%s), got %d err=%v", label, len(rows), err)
		}

		// Duplicat
		code, status = postExternalLinkJSON(t, app, personaID, user.ID, database, payload)
		if code != http.StatusOK || status != "dup" {
			t.Fatalf("esperava dup (%s), got %d/%s", label, code, status)
		}
		rows, _ = database.ExternalLinksListByPersona(personaID, "")
		if len(rows) != 1 {
			t.Fatalf("duplicat hauria de mantenir 1 link (%s), got %d", label, len(rows))
		}
	})
}

func TestAdminExternalSitesCRUD(t *testing.T) {
	forEachArbreDB(t, func(t *testing.T, label string, app *core.App, database db.DB, _ string) {
		user, _ := createF7UserWithSession(t, database)
		session := createSessionCookie(t, database, user.ID, "sess_admin_ext_"+label)
		csrf := "csrf_admin_ext_" + label

		form := newFormValues(map[string]string{
			"name":        "Geneanet",
			"slug":        "",
			"domains":     "geneanet.org",
			"access_mode": "public",
			"icon_path":   "geneanet.svg",
			"is_active":   "1",
			"csrf_token":  csrf,
		})
		req := httptest.NewRequest(http.MethodPost, "/admin/external-sites", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.AddCookie(session)
		req.AddCookie(csrfCookie(csrf))
		rr := httptest.NewRecorder()
		app.AdminExternalSiteCreate(rr, req)
		if rr.Code != http.StatusSeeOther {
			t.Fatalf("create site (%s) esperava 303, got %d", label, rr.Code)
		}

		sites, err := database.ExternalSitesListAll()
		if err != nil || len(sites) != 1 {
			t.Fatalf("ExternalSitesListAll (%s) err=%v len=%d", label, err, len(sites))
		}
		siteID := sites[0].ID

		form = newFormValues(map[string]string{
			"name":        "Geneanet Updated",
			"slug":        sites[0].Slug,
			"domains":     "geneanet.org",
			"access_mode": "private",
			"icon_path":   "geneanet.svg",
			"is_active":   "1",
			"csrf_token":  csrf,
		})
		req = httptest.NewRequest(http.MethodPost, fmt.Sprintf("/admin/external-sites/%d", siteID), strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.AddCookie(session)
		req.AddCookie(csrfCookie(csrf))
		rr = httptest.NewRecorder()
		app.AdminExternalSiteUpdate(rr, req)
		if rr.Code != http.StatusSeeOther {
			t.Fatalf("update site (%s) esperava 303, got %d", label, rr.Code)
		}

		sites, _ = database.ExternalSitesListAll()
		if sites[0].AccessMode != "private" {
			t.Fatalf("update access_mode (%s) esperava private, got %s", label, sites[0].AccessMode)
		}

		req = httptest.NewRequest(http.MethodPost, fmt.Sprintf("/admin/external-sites/%d/toggle", siteID), strings.NewReader("csrf_token="+csrf))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.AddCookie(session)
		req.AddCookie(csrfCookie(csrf))
		rr = httptest.NewRecorder()
		app.AdminExternalSiteToggle(rr, req)
		if rr.Code != http.StatusSeeOther {
			t.Fatalf("toggle site (%s) esperava 303, got %d", label, rr.Code)
		}
		sites, _ = database.ExternalSitesListAll()
		if sites[0].IsActive {
			t.Fatalf("toggle site (%s) hauria de ser inactiu", label)
		}
	})
}

func TestEspaiExternalLinksPermissions(t *testing.T) {
	forEachArbreDB(t, func(t *testing.T, label string, app *core.App, database db.DB, _ string) {
		userA, _ := createF7UserWithSession(t, database)
		userB, _ := createF7UserWithSession(t, database)

		arbreID, err := database.CreateEspaiArbre(&db.EspaiArbre{
			OwnerUserID: userA.ID,
			Nom:         "Arbre Test",
		})
		if err != nil {
			t.Fatalf("CreateEspaiArbre (%s) ha fallat: %v", label, err)
		}
		personaID, err := database.CreateEspaiPersona(&db.EspaiPersona{
			OwnerUserID: userA.ID,
			ArbreID:     arbreID,
			Nom:         sql.NullString{String: "Nom", Valid: true},
		})
		if err != nil || personaID == 0 {
			t.Fatalf("CreateEspaiPersona (%s) ha fallat: %v", label, err)
		}

		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/espai/persones/%d/external-links", personaID), nil)
		req.AddCookie(createSessionCookie(t, database, userB.ID, "sess_ext_forbid_"+label))
		rr := httptest.NewRecorder()
		app.RequireLogin(app.EspaiPersonaExternalLinksAPI)(rr, req)
		if rr.Code != http.StatusNotFound {
			t.Fatalf("esperava 404 per usuari aliè (%s), got %d", label, rr.Code)
		}
	})
}

func fetchExternalLinks(t *testing.T, app *core.App, personaID int) externalLinksAPIResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/persones/%d/external-links", personaID), nil)
	rr := httptest.NewRecorder()
	app.PersonesExternalLinksAPI(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET external links status %d", rr.Code)
	}
	var resp externalLinksAPIResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("Decode external links ha fallat: %v", err)
	}
	return resp
}

func findExternalGroup(groups []externalLinksGroup, slug string) *externalLinksGroup {
	for i := range groups {
		if groups[i].Site.Slug == slug {
			return &groups[i]
		}
	}
	return nil
}

func postExternalLinkJSON(t *testing.T, app *core.App, personaID int, userID int, database db.DB, payload map[string]string) (int, string) {
	t.Helper()
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/persones/%d/external-links", personaID), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	sessionID := fmt.Sprintf("sess_ext_post_%d", userID)
	req.AddCookie(createSessionCookie(t, database, userID, sessionID))
	csrf := fmt.Sprintf("csrf_ext_post_%d", userID)
	req.AddCookie(csrfCookie(csrf))
	req.Header.Set("X-CSRF-Token", csrf)
	rr := httptest.NewRecorder()
	app.PersonesExternalLinksAPI(rr, req)
	status := ""
	if rr.Body.Len() > 0 {
		var resp map[string]string
		if err := json.NewDecoder(rr.Body).Decode(&resp); err == nil {
			status = resp["status"]
		}
	}
	return rr.Code, status
}

func newFormValues(values map[string]string) url.Values {
	form := url.Values{}
	for key, value := range values {
		form.Set(key, value)
	}
	return form
}
