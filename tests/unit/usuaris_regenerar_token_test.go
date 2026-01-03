package unit

import (
	"fmt"
	"html/template"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

// MostrarFormulariRegenerarToken hauria de renderitzar la pàgina sense petar.
func TestMostrarFormulariRegenerarToken_OK(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	// Sobreescrivim les plantilles globals perquè "regenerar-token.html" existeixi
	oldTpl := core.Templates
	defer func() { core.Templates = oldTpl }()

	const tpl = `{{ define "regenerar-token.html" }}OK regenerar token{{ end }}`
	core.Templates = template.Must(template.New("regenerar-token.html").Parse(tpl))

	req := httptest.NewRequest(http.MethodGet, "/regenerar-token", nil)
	rr := httptest.NewRecorder()

	http.HandlerFunc(app.MostrarFormulariRegenerarToken).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("esperava 200 a MostrarFormulariRegenerarToken, tinc %d", rr.Code)
	}
}

// ProcessarRegenerarToken amb GET ha de redirigir al formulari.
func TestProcessarRegenerarToken_GetRedirects(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	req := httptest.NewRequest(http.MethodGet, "/regenerar-token", nil)
	rr := httptest.NewRecorder()

	http.HandlerFunc(app.ProcessarRegenerarToken).ServeHTTP(rr, req)

	resp := rr.Result()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("esperava redirect (303/303), tinc %d", resp.StatusCode)
	}
	loc := resp.Header.Get("Location")
	if loc != "/regenerar-token" {
		t.Errorf("esperava Location=/regenerar-token, tinc %q", loc)
	}
}

// ProcessarRegenerarToken amb POST però sense CSRF ha de donar Forbidden (entra a RegenerarTokenActivacio).
func TestProcessarRegenerarToken_PostInvalidCSRF(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	req := httptest.NewRequest(http.MethodPost, "/regenerar-token?email=test@example.com", nil)
	rr := httptest.NewRecorder()

	http.HandlerFunc(app.ProcessarRegenerarToken).ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("esperava 403 per CSRF invàlid, tinc %d", rr.Code)
	}
}

// CheckAvailability amb mètode incorrecte (GET) ha de retornar 405.
func TestCheckAvailability_InvalidMethod(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	req := httptest.NewRequest(http.MethodGet, "/check-availability", nil)
	rr := httptest.NewRecorder()

	http.HandlerFunc(app.CheckAvailability).ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("esperava 405 per mètode no permès, tinc %d", rr.Code)
	}
}

func TestRegenerarTokenActivacio_InvalidCSRF(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	req := httptest.NewRequest(http.MethodPost, "/regenerar-token?email=test@example.com", nil)
	// Sense cookie ni camp csrf_token
	rr := httptest.NewRecorder()

	http.HandlerFunc(app.RegenerarTokenActivacio).ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("esperava 403 per CSRF invàlid, tinc %d", rr.Code)
	}
}

func TestRegenerarTokenActivacio_MissingEmail(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	form := url.Values{}
	form.Set("csrf_token", "tok")
	req := httptest.NewRequest(http.MethodPost, "/regenerar-token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: "tok"})

	rr := httptest.NewRecorder()
	http.HandlerFunc(app.RegenerarTokenActivacio).ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("esperava 400 per email buit, tinc %d", rr.Code)
	}
}

func TestRegenerarTokenActivacio_UserNotFound(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	email := "noexisteix@example.com"
	form := url.Values{}
	form.Set("csrf_token", "tok")
	url := "/regenerar-token?email=" + url.QueryEscape(email)

	req := httptest.NewRequest(http.MethodPost, url, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: "tok"})

	rr := httptest.NewRecorder()
	http.HandlerFunc(app.RegenerarTokenActivacio).ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("esperava 404 per usuari no trobat, tinc %d", rr.Code)
	}
}

func TestRegenerarTokenActivacio_UserAlreadyActive(t *testing.T) {
	app := newTestApp(t)
	defer closeTestApp(t, app)

	email := "actiu@example.com"

	sqlite, ok := app.DB.(*db.SQLite)
	if !ok {
		t.Fatalf("DB no és SQLite en test")
	}

	user := &db.User{
		Usuari:        "actiu",
		Name:          "Usuari",
		Surname:       "Actiu",
		Email:         email,
		Password:      []byte("dummy"),
		DataNaixament: "1990-01-01",
		Active:        true,
	}
	if err := sqlite.InsertUser(user); err != nil {
		t.Fatalf("InsertUser ha fallat: %v", err)
	}

	form := url.Values{}
	form.Set("csrf_token", "tok")
	url := "/regenerar-token?email=" + url.QueryEscape(email)

	req := httptest.NewRequest(http.MethodPost, url, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: "tok"})

	rr := httptest.NewRecorder()
	http.HandlerFunc(app.RegenerarTokenActivacio).ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("esperava 400 per usuari ja actiu, tinc %d", rr.Code)
	}
}

// fakeDB implementa db.DB però només el que necessitem per a RegenerarTokenActivacio.
type fakeDB struct {
	users map[string]*db.User
}

func (f *fakeDB) Connect() error                                    { return nil }
func (f *fakeDB) Close()                                            {}
func (f *fakeDB) Exec(q string, args ...interface{}) (int64, error) { return 0, nil }
func (f *fakeDB) Query(q string, args ...interface{}) ([]map[string]interface{}, error) {
	return nil, nil
}

func (f *fakeDB) InsertUser(u *db.User) error {
	if f.users == nil {
		f.users = make(map[string]*db.User)
	}
	f.users[u.Email] = u
	return nil
}

func (f *fakeDB) SaveActivationToken(email, token string) error {
	// Per aquests tests no cal guardar res realment
	return nil
}

func (f *fakeDB) GetUserByEmail(email string) (*db.User, error) {
	if f.users == nil {
		return nil, fmt.Errorf("not found")
	}
	u, ok := f.users[email]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return u, nil
}

func (f *fakeDB) GetUserByID(id int) (*db.User, error) {
	if f.users == nil {
		return nil, fmt.Errorf("not found")
	}
	for _, u := range f.users {
		if u.ID == id {
			return u, nil
		}
	}
	return nil, fmt.Errorf("not found")
}

func (f *fakeDB) ExistsUserByUsername(username string) (bool, error) {
	if f.users == nil {
		return false, nil
	}
	for _, u := range f.users {
		if u.Usuari == username {
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeDB) ExistsUserByEmail(email string) (bool, error) {
	if f.users == nil {
		return false, nil
	}
	_, ok := f.users[email]
	return ok, nil
}

func (f *fakeDB) ActivateUser(token string) error { return nil }

func (f *fakeDB) AuthenticateUser(usernameOrEmail, password string) (*db.User, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *fakeDB) SaveSession(sessionID string, userID int, expiry string) error { return nil }
func (f *fakeDB) GetSessionUser(sessionID string) (*db.User, error) {
	return nil, fmt.Errorf("not found")
}
func (f *fakeDB) DeleteSession(sessionID string) error { return nil }
func (f *fakeDB) CreatePasswordReset(email, token, expiry, lang string) (bool, error) {
	return false, nil
}
func (f *fakeDB) GetPasswordReset(token string) (*db.PasswordReset, error) {
	return nil, fmt.Errorf("not implemented")
}
func (f *fakeDB) MarkPasswordResetUsed(id int) error { return nil }
func (f *fakeDB) UpdateUserPassword(userID int, passwordHash []byte) error {
	return nil
}
func (f *fakeDB) CreatePrivacyDefaults(userID int) error { return nil }
func (f *fakeDB) GetPrivacySettings(userID int) (*db.PrivacySettings, error) {
	return nil, fmt.Errorf("not implemented")
}
func (f *fakeDB) SavePrivacySettings(userID int, p *db.PrivacySettings) error { return nil }
func (f *fakeDB) UpdateUserProfile(u *db.User) error                          { return nil }
func (f *fakeDB) UpdateUserEmail(userID int, newEmail string) error           { return nil }
func (f *fakeDB) CreateEmailChange(userID int, newEmail, tokenConfirm, expConfirm, tokenRevert, expRevert, lang string) error {
	return nil
}
func (f *fakeDB) ConfirmEmailChange(token string) (*db.EmailChange, error) {
	return nil, fmt.Errorf("not implemented")
}
func (f *fakeDB) RevertEmailChange(token string) (*db.EmailChange, error) {
	return nil, fmt.Errorf("not implemented")
}
func (f *fakeDB) markEmailChangeConfirmed(id int) error { return nil }
func (f *fakeDB) markEmailChangeReverted(id int) error  { return nil }
func (f *fakeDB) UserHasAnyPolicy(userID int, policies []string) (bool, error) {
	return false, nil
}
func (f *fakeDB) EnsureDefaultPolicies() error    { return nil }
func (f *fakeDB) EnsureDefaultPointsRules() error { return nil }
func (f *fakeDB) ListArxius(filter db.ArxiuFilter) ([]db.ArxiuWithCount, error) {
	return nil, nil
}
func (f *fakeDB) GetArxiu(id int) (*db.Arxiu, error)   { return nil, nil }
func (f *fakeDB) CreateArxiu(a *db.Arxiu) (int, error) { return 0, nil }
func (f *fakeDB) UpdateArxiu(a *db.Arxiu) error        { return nil }
func (f *fakeDB) DeleteArxiu(id int) error             { return nil }
func (f *fakeDB) ListArxiuLlibres(arxiuID int) ([]db.ArxiuLlibreDetail, error) {
	return nil, nil
}
func (f *fakeDB) ListLlibreArxius(llibreID int) ([]db.ArxiuLlibreDetail, error) { return nil, nil }
func (f *fakeDB) AddArxiuLlibre(arxiuID, llibreID int, signatura, urlOverride string) error {
	return nil
}
func (f *fakeDB) UpdateArxiuLlibre(arxiuID, llibreID int, signatura, urlOverride string) error {
	return nil
}
func (f *fakeDB) DeleteArxiuLlibre(arxiuID, llibreID int) error { return nil }
func (f *fakeDB) ListLlibreURLs(llibreID int) ([]db.LlibreURL, error) {
	return nil, nil
}
func (f *fakeDB) AddLlibreURL(link *db.LlibreURL) error { return nil }
func (f *fakeDB) DeleteLlibreURL(id int) error          { return nil }
func (f *fakeDB) SearchLlibresSimple(q string, limit int) ([]db.LlibreSimple, error) {
	return nil, nil
}
func (f *fakeDB) ListLlibres(filter db.LlibreFilter) ([]db.LlibreRow, error) { return nil, nil }
func (f *fakeDB) GetLlibre(id int) (*db.Llibre, error)                       { return nil, nil }
func (f *fakeDB) CreateLlibre(l *db.Llibre) (int, error)                     { return 0, nil }
func (f *fakeDB) UpdateLlibre(l *db.Llibre) error                            { return nil }
func (f *fakeDB) ListLlibrePagines(llibreID int) ([]db.LlibrePagina, error)  { return nil, nil }
func (f *fakeDB) SaveLlibrePagina(p *db.LlibrePagina) (int, error)           { return 0, nil }
func (f *fakeDB) RecalcLlibrePagines(llibreID, total int) error              { return nil }
func (f *fakeDB) ListPaisos() ([]db.Pais, error)                             { return nil, nil }
func (f *fakeDB) GetPais(id int) (*db.Pais, error) {
	return nil, fmt.Errorf("not implemented")
}
func (f *fakeDB) CreatePais(p *db.Pais) (int, error) { return 0, nil }
func (f *fakeDB) UpdatePais(p *db.Pais) error        { return nil }
func (f *fakeDB) ListNivells(filt db.NivellAdminFilter) ([]db.NivellAdministratiu, error) {
	return nil, nil
}
func (f *fakeDB) GetNivell(id int) (*db.NivellAdministratiu, error) {
	return nil, fmt.Errorf("not implemented")
}
func (f *fakeDB) CreateNivell(n *db.NivellAdministratiu) (int, error) { return 0, nil }
func (f *fakeDB) UpdateNivell(n *db.NivellAdministratiu) error        { return nil }
func (f *fakeDB) ListMunicipis(filter db.MunicipiFilter) ([]db.MunicipiRow, error) {
	return nil, nil
}
func (f *fakeDB) GetMunicipi(id int) (*db.Municipi, error) { return nil, nil }
func (f *fakeDB) CreateMunicipi(m *db.Municipi) (int, error) {
	return 0, nil
}
func (f *fakeDB) UpdateMunicipi(m *db.Municipi) error { return nil }
func (f *fakeDB) ListCodisPostals(municipiID int) ([]db.CodiPostal, error) {
	return nil, nil
}
func (f *fakeDB) SaveCodiPostal(cp *db.CodiPostal) (int, error) { return 0, nil }
func (f *fakeDB) ListNomsHistorics(entitatTipus string, entitatID int) ([]db.NomHistoric, error) {
	return nil, nil
}
func (f *fakeDB) SaveNomHistoric(nh *db.NomHistoric) (int, error) { return 0, nil }
func (f *fakeDB) ListGroups() ([]db.Group, error)                 { return nil, nil }
func (f *fakeDB) ListArquebisbats(filt db.ArquebisbatFilter) ([]db.ArquebisbatRow, error) {
	return nil, nil
}
func (f *fakeDB) GetArquebisbat(id int) (*db.Arquebisbat, error)    { return nil, nil }
func (f *fakeDB) CreateArquebisbat(ae *db.Arquebisbat) (int, error) { return 0, nil }
func (f *fakeDB) UpdateArquebisbat(ae *db.Arquebisbat) error        { return nil }
func (f *fakeDB) ListArquebisbatMunicipis(munID int) ([]db.ArquebisbatMunicipi, error) {
	return nil, nil
}
func (f *fakeDB) SaveArquebisbatMunicipi(am *db.ArquebisbatMunicipi) (int, error) {
	return 0, nil
}
func (f *fakeDB) ListPolitiques() ([]db.Politica, error)                 { return nil, nil }
func (f *fakeDB) GetPolitica(id int) (*db.Politica, error)               { return nil, nil }
func (f *fakeDB) SavePolitica(p *db.Politica) (int, error)               { return 0, nil }
func (f *fakeDB) ListUserPolitiques(userID int) ([]db.Politica, error)   { return nil, nil }
func (f *fakeDB) AddUserPolitica(userID, politicaID int) error           { return nil }
func (f *fakeDB) RemoveUserPolitica(userID, politicaID int) error        { return nil }
func (f *fakeDB) ListGroupPolitiques(groupID int) ([]db.Politica, error) { return nil, nil }
func (f *fakeDB) AddGroupPolitica(groupID, politicaID int) error         { return nil }
func (f *fakeDB) RemoveGroupPolitica(groupID, politicaID int) error      { return nil }
func (f *fakeDB) GetEffectivePoliticaPerms(userID int) (db.PolicyPermissions, error) {
	return db.PolicyPermissions{}, nil
}
func (f *fakeDB) ListUsersAdmin() ([]db.UserAdminRow, error) { return nil, nil }
func (f *fakeDB) SetUserActive(userID int, active bool) error { return nil }
func (f *fakeDB) SetUserBanned(userID int, banned bool) error { return nil }

// Punts i activitat (no-op)
func (f *fakeDB) ListPointsRules() ([]db.PointsRule, error) { return nil, nil }
func (f *fakeDB) GetPointsRule(id int) (*db.PointsRule, error) {
	return nil, fmt.Errorf("not found")
}
func (f *fakeDB) GetPointsRuleByCode(code string) (*db.PointsRule, error) {
	return nil, fmt.Errorf("not found")
}
func (f *fakeDB) SavePointsRule(r *db.PointsRule) (int, error)       { return 0, nil }
func (f *fakeDB) InsertUserActivity(a *db.UserActivity) (int, error) { return 0, nil }
func (f *fakeDB) GetUserActivity(id int) (*db.UserActivity, error) {
	return nil, fmt.Errorf("not found")
}
func (f *fakeDB) UpdateUserActivityStatus(id int, status string, moderatedBy *int) error {
	return nil
}
func (f *fakeDB) ListUserActivityByUser(userID int, af db.ActivityFilter) ([]db.UserActivity, error) {
	return nil, nil
}
func (f *fakeDB) ListActivityByObject(objectType string, objectID int, status string) ([]db.UserActivity, error) {
	return nil, nil
}
func (f *fakeDB) AddPointsToUser(userID int, delta int) error { return nil }
func (f *fakeDB) GetUserPoints(userID int) (*db.UserPoints, error) {
	return &db.UserPoints{UserID: userID}, nil
}
func (f *fakeDB) RecalcUserPoints() error                              { return nil }
func (f *fakeDB) GetRanking(db.RankingFilter) ([]db.UserPoints, error) { return nil, nil }
func (f *fakeDB) CountRanking(db.RankingFilter) (int, error)           { return 0, nil }
func (f *fakeDB) ListUserGroups(userID int) ([]db.Group, error)        { return nil, nil }
func (f *fakeDB) ListPersones(filter db.PersonaFilter) ([]db.Persona, error) {
	return nil, nil
}
func (f *fakeDB) GetPersona(id int) (*db.Persona, error) { return nil, nil }
func (f *fakeDB) CreatePersona(p *db.Persona) (int, error) {
	return 0, nil
}
func (f *fakeDB) UpdatePersona(p *db.Persona) error { return nil }
func (f *fakeDB) UpdatePersonaModeracio(id int, estat, motiu string, moderatorID int) error {
	return nil
}
func (f *fakeDB) UpdateArxiuModeracio(id int, estat, motiu string, moderatorID int) error { return nil }
func (f *fakeDB) UpdateLlibreModeracio(id int, estat, motiu string, moderatorID int) error {
	return nil
}
func (f *fakeDB) UpdateNivellModeracio(id int, estat, motiu string, moderatorID int) error {
	return nil
}
func (f *fakeDB) UpdateMunicipiModeracio(id int, estat, motiu string, moderatorID int) error {
	return nil
}
func (f *fakeDB) UpdateArquebisbatModeracio(id int, estat, motiu string, moderatorID int) error {
	return nil
}
func (f *fakeDB) ListTranscripcionsRaw(llibreID int, ftr db.TranscripcioFilter) ([]db.TranscripcioRaw, error) {
	return nil, nil
}
func (f *fakeDB) ListTranscripcionsRawGlobal(ftr db.TranscripcioFilter) ([]db.TranscripcioRaw, error) {
	return nil, nil
}
func (f *fakeDB) CountTranscripcionsRaw(llibreID int, ftr db.TranscripcioFilter) (int, error) {
	return 0, nil
}
func (f *fakeDB) CountTranscripcionsRawGlobal(ftr db.TranscripcioFilter) (int, error) {
	return 0, nil
}
func (f *fakeDB) GetTranscripcioRaw(id int) (*db.TranscripcioRaw, error) {
	return nil, nil
}
func (f *fakeDB) CreateTranscripcioRaw(t *db.TranscripcioRaw) (int, error) {
	return 0, nil
}
func (f *fakeDB) UpdateTranscripcioRaw(t *db.TranscripcioRaw) error {
	return nil
}
func (f *fakeDB) DeleteTranscripcioRaw(id int) error {
	return nil
}
func (f *fakeDB) RecalcTranscripcionsRawPageStats(llibreID int) error {
	return nil
}
func (f *fakeDB) SetTranscripcionsRawPageStatsIndexacio(llibreID int, value int) error {
	return nil
}
func (f *fakeDB) ListTranscripcionsRawPageStats(llibreID int) ([]db.TranscripcioRawPageStat, error) {
	return nil, nil
}
func (f *fakeDB) UpdateTranscripcionsRawPageStat(stat *db.TranscripcioRawPageStat) error {
	return nil
}
func (f *fakeDB) DeleteTranscripcionsByLlibre(llibreID int) error {
	return nil
}
func (f *fakeDB) ListTranscripcioPersones(transcripcioID int) ([]db.TranscripcioPersonaRaw, error) {
	return nil, nil
}
func (f *fakeDB) CreateTranscripcioPersona(p *db.TranscripcioPersonaRaw) (int, error) {
	return 0, nil
}
func (f *fakeDB) LinkTranscripcioPersona(personaRawID int, personaID int, linkedBy int) error {
	return nil
}
func (f *fakeDB) UnlinkTranscripcioPersona(personaRawID int, linkedBy int) error {
	return nil
}
func (f *fakeDB) DeleteTranscripcioPersones(transcripcioID int) error {
	return nil
}
func (f *fakeDB) ListTranscripcioAtributs(transcripcioID int) ([]db.TranscripcioAtributRaw, error) {
	return nil, nil
}
func (f *fakeDB) CreateTranscripcioAtribut(a *db.TranscripcioAtributRaw) (int, error) {
	return 0, nil
}
func (f *fakeDB) DeleteTranscripcioAtributs(transcripcioID int) error {
	return nil
}
func (f *fakeDB) GetTranscripcioDraft(userID, llibreID int) (*db.TranscripcioDraft, error) {
	return nil, nil
}
func (f *fakeDB) SaveTranscripcioDraft(userID, llibreID int, payload string) error {
	return nil
}
func (f *fakeDB) DeleteTranscripcioDraft(userID, llibreID int) error {
	return nil
}
func (f *fakeDB) SearchPersones(filt db.PersonaSearchFilter) ([]db.PersonaSearchResult, error) {
	return nil, nil
}
func (f *fakeDB) ListRegistresByPersona(personaID int, tipus string) ([]db.PersonaRegistreRow, error) {
	return nil, nil
}
func (f *fakeDB) UpsertTranscripcioMark(m *db.TranscripcioRawMark) error {
	return nil
}
func (f *fakeDB) DeleteTranscripcioMark(transcripcioID, userID int) error {
	return nil
}
func (f *fakeDB) ListTranscripcioMarks(transcripcioIDs []int) ([]db.TranscripcioRawMark, error) {
	return nil, nil
}
func (f *fakeDB) UpdateTranscripcioModeracio(id int, estat, motiu string, moderatorID int) error {
	return nil
}
func (f *fakeDB) CreateTranscripcioRawChange(c *db.TranscripcioRawChange) (int, error) {
	return 0, nil
}
func (f *fakeDB) ListTranscripcioRawChanges(transcripcioID int) ([]db.TranscripcioRawChange, error) {
	return nil, nil
}
func (f *fakeDB) GetTranscripcioRawChange(id int) (*db.TranscripcioRawChange, error) {
	return nil, fmt.Errorf("GetTranscripcioRawChange not implemented in fakeDB")
}
func (f *fakeDB) ListTranscripcioRawChangesPending() ([]db.TranscripcioRawChange, error) {
	return nil, nil
}
func (f *fakeDB) UpdateTranscripcioRawChangeModeracio(id int, estat, motiu string, moderatorID int) error {
	return nil
}

func (f *fakeDB) HasLlibreDuplicate(municipiID int, tipus, cronologia, codiDigital, codiFisic string, excludeID int) (bool, error) {
	return false, nil
}
func (f *fakeDB) GetLlibresIndexacioStats(ids []int) (map[int]db.LlibreIndexacioStats, error) {
	return map[int]db.LlibreIndexacioStats{}, nil
}
func (f *fakeDB) UpsertLlibreIndexacioStats(stats *db.LlibreIndexacioStats) error {
	return nil
}
func (f *fakeDB) ListCognoms(q string, limit, offset int) ([]db.Cognom, error) {
	return nil, nil
}
func (f *fakeDB) GetCognom(id int) (*db.Cognom, error) { return nil, nil }
func (f *fakeDB) UpsertCognom(forma, key, origen, notes string, createdBy *int) (int, error) {
	return 0, nil
}
func (f *fakeDB) ListCognomVariants(filter db.CognomVariantFilter) ([]db.CognomVariant, error) {
	return nil, nil
}
func (f *fakeDB) ResolveCognomPublicatByForma(forma string) (int, string, bool, error) {
	return 0, "", false, nil
}
func (f *fakeDB) ListCognomFormesPublicades(cognomID int) ([]string, error) {
	return nil, nil
}
func (f *fakeDB) CreateCognomVariant(v *db.CognomVariant) (int, error) { return 0, nil }
func (f *fakeDB) UpdateCognomVariantModeracio(id int, estat, motiu string, moderatorID int) error {
	return nil
}
func (f *fakeDB) UpsertCognomFreqMunicipiAny(cognomID, municipiID, anyDoc, freq int) error {
	return nil
}
func (f *fakeDB) QueryCognomHeatmap(cognomID int, anyStart, anyEnd int) ([]db.CognomFreqRow, error) {
	return nil, nil
}
func (f *fakeDB) ListCognomImportRows(limit, offset int) ([]db.CognomImportRow, error) {
	return nil, nil
}
func (f *fakeDB) ListCognomStatsRows(limit, offset int) ([]db.CognomStatsRow, error) {
	return nil, nil
}

// Helper per crear una App amb fakeDB
func newFakeAppWithUsers(users map[string]*db.User) *core.App {
	fdb := &fakeDB{users: users}
	// Per aquests tests, la config ens és igual
	return core.NewApp(map[string]string{}, fdb)
}

func TestRegenerarTokenActivacio_Success(t *testing.T) {
	email := "nou@example.com"
	users := map[string]*db.User{
		email: {
			Email:  email,
			Active: false, // molt important: inactiu
		},
	}
	app := newFakeAppWithUsers(users)

	form := url.Values{}
	form.Set("csrf_token", "tok")
	urlStr := "/regenerar-token?email=" + url.QueryEscape(email)

	req := httptest.NewRequest(http.MethodPost, urlStr, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "cg_csrf", Value: "tok"})

	rr := httptest.NewRecorder()

	http.HandlerFunc(app.RegenerarTokenActivacio).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK && rr.Code != http.StatusFound {
		t.Fatalf("esperava 200 o 302 en el cas bo, tinc %d", rr.Code)
	}

	// Opcional: comprovar que a la resposta hi ha algun missatge de confirmació
	body := rr.Body.String()
	if !strings.Contains(body, "token") && !strings.Contains(body, "activació") {
		t.Logf("cos de resposta: %s", body)
	}
}
