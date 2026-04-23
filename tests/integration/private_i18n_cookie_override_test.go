package integration

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func setUserPreferredLang(t *testing.T, appUser *db.User, lang string, database db.DB) {
	t.Helper()
	appUser.PreferredLang = lang
	if err := database.UpdateUserProfile(appUser); err != nil {
		t.Fatalf("UpdateUserProfile ha fallat: %v", err)
	}
}

func TestPrivateSectionsRespectLangCookieOverPreferredLang(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_private_i18n_cookie_override.sqlite3")

	user := createTestUser(t, database, "private_i18n_admin")
	assignPolicyByName(t, database, user.ID, "admin")
	setUserPreferredLang(t, user, "cat", database)
	session := createSessionCookie(t, database, user.ID, "sess_private_i18n_admin")
	langCookie := &http.Cookie{Name: "lang", Value: "en", Path: "/"}

	cases := []struct {
		name        string
		path        string
		wantSnippet string
		handler     func(http.ResponseWriter, *http.Request)
	}{
		{
			name:        "messages",
			path:        "/missatges",
			wantSnippet: "<h1>Messages</h1>",
			handler:     app.MessagesInbox,
		},
		{
			name:        "media albums",
			path:        "/media/albums",
			wantSnippet: "<h1>Albums</h1>",
			handler:     app.MediaAlbums,
		},
		{
			name:        "media moderation",
			path:        "/admin/moderacio/media",
			wantSnippet: "<h1>Media moderation</h1>",
			handler:     app.AdminModeracioMediaList,
		},
		{
			name:        "map moderation",
			path:        "/admin/moderacio/mapes",
			wantSnippet: "<h1>Map moderation</h1>",
			handler:     app.AdminModeracioMapesList,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			req.AddCookie(session)
			req.AddCookie(langCookie)
			rr := httptest.NewRecorder()

			tc.handler(rr, req)
			if rr.Code != http.StatusOK {
				t.Fatalf("esperava 200, got %d", rr.Code)
			}
			body := rr.Body.String()
			if !strings.Contains(body, "<html lang=\"en\">") {
				t.Fatalf("la pagina hauria de renderitzar-se en angles")
			}
			if !strings.Contains(body, tc.wantSnippet) {
				t.Fatalf("la pagina hauria de contenir el text %q", tc.wantSnippet)
			}
		})
	}
}
