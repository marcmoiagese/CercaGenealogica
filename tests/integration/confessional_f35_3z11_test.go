package integration

import (
	"strings"
	"testing"
)

func TestF353Z11ConfessionalTabsRemovedFromRenderedSections(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f35_3z11_tabs.sqlite3")
	session := f353YAdminSession(t, database, "z11_tabs")

	cases := []struct {
		name    string
		path    string
		handler func() string
		want    string
	}{
		{
			name: "entitats",
			path: "/confessional/entitats",
			handler: func() string {
				return f353YGet(t, app.AdminConfessionalSectionList, "/confessional/entitats", session)
			},
			want: `id="nivellsFilterForm"`,
		},
		{
			name: "navegacio",
			path: "/confessional/navegacio",
			handler: func() string {
				return f353YGet(t, app.AdminConfessionalNavigation, "/confessional/navegacio", session)
			},
			want: `id="confessional-q"`,
		},
		{
			name: "relacions_entitats",
			path: "/confessional/relacions-entitats",
			handler: func() string {
				return f353YGet(t, app.AdminConfessionalSectionList, "/confessional/relacions-entitats", session)
			},
			want: `class="taula taula-registres"`,
		},
		{
			name: "relacions_territorials",
			path: "/confessional/municipis-entitats",
			handler: func() string {
				return f353YGet(t, app.AdminConfessionalSectionList, "/confessional/municipis-entitats", session)
			},
			want: `class="taula taula-registres"`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := tc.handler()
			if strings.Contains(body, "confessional-tabs") {
				t.Fatalf("%s encara renderitza confessional-tabs; body=%s", tc.path, body)
			}
			if !strings.Contains(body, tc.want) {
				t.Fatalf("%s no renderitza el contingut propi esperat %q; body=%s", tc.path, tc.want, body)
			}
		})
	}
}

func TestF353Z11ConfessionalMenuIconsLinksAndNoInlineJS(t *testing.T) {
	root := findProjectRoot(t)
	menuBody := readProjectFileF353U(t, root, "templates/layouts/menu-private.html")
	listBody := readProjectFileF353U(t, root, "templates/admin-confessional-list.html")
	navBody := readProjectFileF353U(t, root, "templates/admin-confessional-navegacio.html")

	for _, token := range []string{
		`<i class="fas fa-place-of-worship"></i>`,
		`<h3>{{ t .Lang "confessional.menu.section" }}</h3>`,
		`href="/confessional/entitats"><i class="fas fa-sitemap"></i> {{ t .Lang "confessional.menu.entities" }}`,
		`href="/confessional/navegacio"><i class="fas fa-route"></i> {{ t .Lang "confessional.menu.navigation" }}`,
		`href="/confessional/relacions-entitats"><i class="fas fa-link"></i> {{ t .Lang "confessional.menu.entity_relations" }}`,
		`href="/confessional/municipis-entitats"><i class="fas fa-map-location-dot"></i> {{ t .Lang "confessional.menu.territorial_relations" }}`,
	} {
		if !strings.Contains(menuBody, token) {
			t.Fatalf("falta contracte d'icona/link al menu confessional: %s", token)
		}
	}
	for _, route := range []string{
		`/confessional/entitats`,
		`/confessional/navegacio`,
		`/confessional/relacions-entitats`,
		`/confessional/municipis-entitats`,
	} {
		if !strings.Contains(menuBody, route) {
			t.Fatalf("falta link confessional al menu principal: %s", route)
		}
	}
	if strings.Contains(listBody+navBody, "confessional-tabs") {
		t.Fatalf("els templates confessionals no han de conservar CSS ni nav confessional-tabs")
	}
	if strings.Contains(listBody, "<script>\n") || strings.Contains(navBody, "<script>\n") ||
		strings.Contains(listBody, "onclick=") || strings.Contains(navBody, "onclick=") ||
		strings.Contains(listBody, "onchange=") || strings.Contains(navBody, "onchange=") {
		t.Fatalf("F35-3Z11 no ha d'afegir JS inline")
	}
}
