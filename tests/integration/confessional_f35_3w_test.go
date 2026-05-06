package integration

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestF353WStaticCatalogHasNoActiveDBCRUDFlow(t *testing.T) {
	root := findProjectRoot(t)
	handlerBody := readProjectFileF353W(t, root, "core/admin_confessional.go")
	formBody := readProjectFileF353W(t, root, "templates/admin-confessional-form.html")
	listBody := readProjectFileF353W(t, root, "templates/admin-confessional-list.html")
	menuBody := readProjectFileF353W(t, root, "templates/layouts/menu-private.html")
	mainBody := readProjectFileF353W(t, root, "main.go")
	dbMotorBody := readProjectFileF353W(t, root, "db/motor.go")
	dbConfessionalBody := readProjectFileF353W(t, root, "db/confessional_per_motor.go")

	for _, token := range []string{
		"confessionalCatalogSection(kind)",
		"ListConfessionalReligionCatalog()",
		"ListConfessionalLevelCatalog()",
		"ReligionCatalogLabels",
		"LevelCatalogLabels",
	} {
		if !strings.Contains(handlerBody, token) {
			t.Fatalf("falta blindatge/cataleg F35-3W al handler: %s", token)
		}
	}
	for _, forbidden := range []string{
		"data.Religio = &db.ReligioConfessio",
		"data.Nivell = &db.NivellConfessional",
		"data.Religio, err = a.DB.GetReligioConfessio",
		"data.Nivell, err = a.DB.GetNivellConfessional",
		"SaveReligioConfessio(data.Religio)",
		"SaveNivellConfessional(data.Nivell)",
		"DeleteReligioConfessio(id)",
		"DeleteNivellConfessional(id)",
	} {
		if strings.Contains(handlerBody, forbidden) {
			t.Fatalf("el flux actiu encara exposa CRUD base: %s", forbidden)
		}
	}
	for _, forbidden := range []string{
		`href="/territori/confessional`,
		`action="/territori/confessional`,
		`name="kind" value="religio"`,
		`name="kind" value="nivell"`,
		`/confessional/religions/{{ .ID }}/edit`,
		`/confessional/nivells/{{ .ID }}/edit`,
	} {
		if strings.Contains(listBody+formBody+menuBody, forbidden) {
			t.Fatalf("la UI conserva ruta o accio CRUD base legacy: %s", forbidden)
		}
	}
	for _, forbidden := range []string{
		`href="/confessional/religions"`,
		`href="/confessional/nivells"`,
		`confessional.menu.catalog_religions`,
		`confessional.menu.catalog_levels`,
	} {
		if strings.Contains(listBody+menuBody, forbidden) {
			t.Fatalf("la UI no ha de mostrar pantalla de cataleg base: %s", forbidden)
		}
	}
	for _, token := range []string{
		`href="/confessional/entitats"`,
		`action="/confessional/save"`,
		`action="/confessional/delete"`,
		`/territori/confessional`,
		`compatibilitat historica`,
	} {
		if !strings.Contains(listBody+formBody+menuBody+mainBody, token) {
			t.Fatalf("falta ruta canonica o compatibilitat F35-3W: %s", token)
		}
	}
	for _, token := range []string{
		"Legacy confessional base catalogs",
		"Legacy level catalog rows",
		"Legacy religion/confession row access",
		"Legacy level row access",
	} {
		if !strings.Contains(dbMotorBody+dbConfessionalBody, token) {
			t.Fatalf("falta documentacio legacy DB F35-3W: %s", token)
		}
	}
	if strings.Contains(handlerBody, "ListNivellsConfessionals()") {
		t.Fatalf("el handler no ha de llegir nivells base de DB per selectors o cataleg")
	}
}

func TestF353WManipulatedBaseCatalogPostsAreBlocked(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3w_block_base_catalog_posts.sqlite3")
	admin := createTestUser(t, database, "f35_3w_admin_"+time.Now().Format("150405000000000"))
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_f35_3w_"+time.Now().Format("150405000000000"))

	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
		path    string
		values  url.Values
	}{
		{
			name:    "save_religio",
			handler: app.AdminSaveConfessional,
			path:    "/confessional/save",
			values:  url.Values{"kind": {"religio"}, "nom": {"Cristianisme manipulat"}, "estat": {"actiu"}},
		},
		{
			name:    "save_nivell",
			handler: app.AdminSaveConfessional,
			path:    "/confessional/save",
			values:  url.Values{"kind": {"nivell"}, "nom_nivell": {"Parroquia manipulada"}, "ordre": {"1"}, "estat": {"actiu"}},
		},
		{
			name:    "delete_religio",
			handler: app.AdminDeleteConfessional,
			path:    "/confessional/delete",
			values:  url.Values{"kind": {"religio"}, "id": {"1"}},
		},
		{
			name:    "delete_nivell",
			handler: app.AdminDeleteConfessional,
			path:    "/confessional/delete",
			values:  url.Values{"kind": {"nivell"}, "id": {"1"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.values.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.AddCookie(session)
			rr := httptest.NewRecorder()
			tc.handler(rr, req)
			if rr.Code != http.StatusNotFound {
				t.Fatalf("%s status=%d, want 404", tc.name, rr.Code)
			}
		})
	}

	if rows, err := database.Query("SELECT COUNT(*) AS n FROM religio_confessio"); err != nil || parseCountValue(t, rows[0]["n"]) != 0 {
		t.Fatalf("POST manipulat no ha d'escriure religions base: rows=%v err=%v", rows, err)
	}
	if rows, err := database.Query("SELECT COUNT(*) AS n FROM nivell_confessional"); err != nil || parseCountValue(t, rows[0]["n"]) != 0 {
		t.Fatalf("POST manipulat no ha d'escriure nivells base: rows=%v err=%v", rows, err)
	}
}

func readProjectFileF353W(t *testing.T, root, rel string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("no s'ha pogut llegir %s: %v", rel, err)
	}
	return string(body)
}
