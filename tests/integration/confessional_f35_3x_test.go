package integration

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestF353XConfessionalCatalogIsHiddenFromNavigation(t *testing.T) {
	root := findProjectRoot(t)
	handlerBody := readProjectFileF353X(t, root, "core/admin_confessional.go")
	listBody := readProjectFileF353X(t, root, "templates/admin-confessional-list.html")
	formBody := readProjectFileF353X(t, root, "templates/admin-confessional-form.html")
	menuBody := readProjectFileF353X(t, root, "templates/layouts/menu-private.html")
	catalogBody := readProjectFileF353X(t, root, "core/confessional_catalog.go")

	for _, forbidden := range []string{
		`href="/confessional/religions"`,
		`href="/confessional/nivells"`,
		`confessional.menu.catalog_religions`,
		`confessional.menu.catalog_levels`,
		`eq .Data.Section.Kind "religio"`,
		`eq .Data.Section.Kind "nivell"`,
	} {
		if strings.Contains(menuBody+listBody, forbidden) {
			t.Fatalf("el cataleg base no ha de ser pantalla visible: %s", forbidden)
		}
	}
	for _, token := range []string{
		`href="/confessional/entitats"`,
		`href="/confessional/relacions-entitats"`,
		`href="/confessional/municipis-entitats"`,
		`confessionalCatalogSection(section.Kind)`,
		`http.Redirect(w, r, "/confessional/entitats"`,
	} {
		if !strings.Contains(menuBody+listBody+handlerBody, token) {
			t.Fatalf("falta contracte visible F35-3X: %s", token)
		}
	}
	for _, token := range []string{
		`name="religio_confessio_codi"`,
		`name="nivell_confessional_codi"`,
		`data-religion-code="{{ .ReligionCode }}"`,
		`syncConfessionalLevels`,
	} {
		if !strings.Contains(formBody, token) {
			t.Fatalf("el formulari d'entitats ha de conservar selectors de cataleg: %s", token)
		}
	}
	for _, token := range []string{
		"ListConfessionalReligionCatalog",
		"ListConfessionalLevelCatalog",
		"GetConfessionalReligionCatalogByCode",
		"GetConfessionalLevelCatalogByCode",
	} {
		if !strings.Contains(catalogBody+handlerBody, token) {
			t.Fatalf("el cataleg estatic ha de continuar disponible per selectors/validacio: %s", token)
		}
	}
}

func TestF353XCatalogRoutesRedirectToEntities(t *testing.T) {
	app, _ := newTestAppForLogin(t, "test_f35_3x_catalog_routes.sqlite3")

	for _, tc := range []struct {
		name    string
		path    string
		handler http.HandlerFunc
	}{
		{name: "root", path: "/confessional", handler: app.AdminConfessionalList},
		{name: "catalog_religions", path: "/confessional/religions", handler: app.AdminConfessionalSectionList},
		{name: "catalog_levels", path: "/confessional/nivells", handler: app.AdminConfessionalSectionList},
		{name: "legacy_catalog_religions", path: "/territori/confessional/religions", handler: app.AdminConfessionalSectionList},
		{name: "legacy_catalog_levels", path: "/territori/confessional/nivells", handler: app.AdminConfessionalSectionList},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			rr := httptest.NewRecorder()
			tc.handler(rr, req)
			if rr.Code != http.StatusSeeOther {
				t.Fatalf("%s status=%d, want 303", tc.name, rr.Code)
			}
			if got := rr.Header().Get("Location"); got != "/confessional/entitats" {
				t.Fatalf("%s Location=%q, want /confessional/entitats", tc.name, got)
			}
		})
	}
}

func readProjectFileF353X(t *testing.T, root, rel string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("no s'ha pogut llegir %s: %v", rel, err)
	}
	return string(body)
}
