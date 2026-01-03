package unit

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/core"
	"github.com/marcmoiagese/CercaGenealogica/db"
)

// fakeDBVerificar implementa db.DB però només fa servir GetSessionUser
// per aquests tests. La resta de mètodes tornen un error "not implemented".
type fakeDBVerificar struct {
	sessions map[string]*db.User
}

// --- Implementació de la interfície db.DB ---

func (f *fakeDBVerificar) Connect() error { return nil }
func (f *fakeDBVerificar) Close()         {}

func (f *fakeDBVerificar) Exec(query string, args ...interface{}) (int64, error) {
	return 0, errors.New("Exec not implemented in fakeDBVerificar")
}

func (f *fakeDBVerificar) Query(query string, args ...interface{}) ([]map[string]interface{}, error) {
	return nil, errors.New("Query not implemented in fakeDBVerificar")
}

func (f *fakeDBVerificar) InsertUser(user *db.User) error {
	return errors.New("InsertUser not implemented in fakeDBVerificar")
}

func (f *fakeDBVerificar) SaveActivationToken(email, token string) error {
	return errors.New("SaveActivationToken not implemented in fakeDBVerificar")
}

func (f *fakeDBVerificar) GetUserByEmail(email string) (*db.User, error) {
	return nil, errors.New("GetUserByEmail not implemented in fakeDBVerificar")
}

func (f *fakeDBVerificar) GetUserByID(id int) (*db.User, error) {
	for _, u := range f.sessions {
		if u.ID == id {
			return u, nil
		}
	}
	return nil, errors.New("not found")
}

func (f *fakeDBVerificar) ExistsUserByUsername(username string) (bool, error) {
	return false, errors.New("ExistsUserByUsername not implemented in fakeDBVerificar")
}

func (f *fakeDBVerificar) ExistsUserByEmail(email string) (bool, error) {
	return false, errors.New("ExistsUserByEmail not implemented in fakeDBVerificar")
}

func (f *fakeDBVerificar) ActivateUser(token string) error {
	return errors.New("ActivateUser not implemented in fakeDBVerificar")
}

func (f *fakeDBVerificar) AuthenticateUser(usernameOrEmail, password string) (*db.User, error) {
	return nil, errors.New("AuthenticateUser not implemented in fakeDBVerificar")
}

func (f *fakeDBVerificar) SaveSession(sessionID string, userID int, expiry string) error {
	return errors.New("SaveSession not implemented in fakeDBVerificar")
}

// Aquesta és l'única funció que ens importa per aquests tests
func (f *fakeDBVerificar) GetSessionUser(sessionID string) (*db.User, error) {
	u, ok := f.sessions[sessionID]
	if !ok {
		return nil, errors.New("session not found")
	}
	return u, nil
}

func (f *fakeDBVerificar) DeleteSession(sessionID string) error {
	return errors.New("DeleteSession not implemented in fakeDBVerificar")
}
func (f *fakeDBVerificar) CreatePasswordReset(email, token, expiry, lang string) (bool, error) {
	return false, errors.New("CreatePasswordReset not implemented in fakeDBVerificar")
}
func (f *fakeDBVerificar) GetPasswordReset(token string) (*db.PasswordReset, error) {
	return nil, errors.New("GetPasswordReset not implemented in fakeDBVerificar")
}
func (f *fakeDBVerificar) MarkPasswordResetUsed(id int) error {
	return errors.New("MarkPasswordResetUsed not implemented in fakeDBVerificar")
}
func (f *fakeDBVerificar) UpdateUserPassword(userID int, passwordHash []byte) error {
	return errors.New("UpdateUserPassword not implemented in fakeDBVerificar")
}
func (f *fakeDBVerificar) CreatePrivacyDefaults(userID int) error {
	return errors.New("CreatePrivacyDefaults not implemented in fakeDBVerificar")
}
func (f *fakeDBVerificar) GetPrivacySettings(userID int) (*db.PrivacySettings, error) {
	return nil, errors.New("GetPrivacySettings not implemented in fakeDBVerificar")
}
func (f *fakeDBVerificar) SavePrivacySettings(userID int, p *db.PrivacySettings) error {
	return errors.New("SavePrivacySettings not implemented in fakeDBVerificar")
}
func (f *fakeDBVerificar) UpdateUserProfile(u *db.User) error {
	return errors.New("UpdateUserProfile not implemented in fakeDBVerificar")
}
func (f *fakeDBVerificar) UpdateUserEmail(userID int, newEmail string) error {
	return errors.New("UpdateUserEmail not implemented in fakeDBVerificar")
}
func (f *fakeDBVerificar) ListUsersAdmin() ([]db.UserAdminRow, error) {
	return nil, errors.New("ListUsersAdmin not implemented in fakeDBVerificar")
}
func (f *fakeDBVerificar) SetUserActive(userID int, active bool) error {
	return errors.New("SetUserActive not implemented in fakeDBVerificar")
}
func (f *fakeDBVerificar) SetUserBanned(userID int, banned bool) error {
	return errors.New("SetUserBanned not implemented in fakeDBVerificar")
}
func (f *fakeDBVerificar) CreateEmailChange(userID int, newEmail, tokenConfirm, expConfirm, tokenRevert, expRevert, lang string) error {
	return errors.New("CreateEmailChange not implemented in fakeDBVerificar")
}
func (f *fakeDBVerificar) ConfirmEmailChange(token string) (*db.EmailChange, error) {
	return nil, errors.New("ConfirmEmailChange not implemented in fakeDBVerificar")
}
func (f *fakeDBVerificar) RevertEmailChange(token string) (*db.EmailChange, error) {
	return nil, errors.New("RevertEmailChange not implemented in fakeDBVerificar")
}
func (f *fakeDBVerificar) markEmailChangeConfirmed(id int) error {
	return errors.New("markEmailChangeConfirmed not implemented in fakeDBVerificar")
}
func (f *fakeDBVerificar) markEmailChangeReverted(id int) error {
	return errors.New("markEmailChangeReverted not implemented in fakeDBVerificar")
}
func (f *fakeDBVerificar) UserHasAnyPolicy(userID int, policies []string) (bool, error) {
	return false, nil
}
func (f *fakeDBVerificar) EnsureDefaultPolicies() error    { return nil }
func (f *fakeDBVerificar) EnsureDefaultPointsRules() error { return nil }
func (f *fakeDBVerificar) ListArxius(filter db.ArxiuFilter) ([]db.ArxiuWithCount, error) {
	return nil, nil
}
func (f *fakeDBVerificar) GetArxiu(id int) (*db.Arxiu, error)   { return nil, nil }
func (f *fakeDBVerificar) CreateArxiu(a *db.Arxiu) (int, error) { return 0, nil }
func (f *fakeDBVerificar) UpdateArxiu(a *db.Arxiu) error        { return nil }
func (f *fakeDBVerificar) DeleteArxiu(id int) error             { return nil }
func (f *fakeDBVerificar) ListArxiuLlibres(arxiuID int) ([]db.ArxiuLlibreDetail, error) {
	return nil, nil
}
func (f *fakeDBVerificar) ListLlibreArxius(llibreID int) ([]db.ArxiuLlibreDetail, error) {
	return nil, nil
}
func (f *fakeDBVerificar) AddArxiuLlibre(arxiuID, llibreID int, signatura, urlOverride string) error {
	return nil
}
func (f *fakeDBVerificar) UpdateArxiuLlibre(arxiuID, llibreID int, signatura, urlOverride string) error {
	return nil
}
func (f *fakeDBVerificar) DeleteArxiuLlibre(arxiuID, llibreID int) error { return nil }
func (f *fakeDBVerificar) ListLlibreURLs(llibreID int) ([]db.LlibreURL, error) {
	return nil, nil
}
func (f *fakeDBVerificar) AddLlibreURL(link *db.LlibreURL) error { return nil }
func (f *fakeDBVerificar) DeleteLlibreURL(id int) error          { return nil }
func (f *fakeDBVerificar) SearchLlibresSimple(q string, limit int) ([]db.LlibreSimple, error) {
	return nil, nil
}
func (f *fakeDBVerificar) ListLlibres(filter db.LlibreFilter) ([]db.LlibreRow, error) {
	return nil, nil
}
func (f *fakeDBVerificar) GetLlibre(id int) (*db.Llibre, error)                      { return nil, nil }
func (f *fakeDBVerificar) CreateLlibre(l *db.Llibre) (int, error)                    { return 0, nil }
func (f *fakeDBVerificar) UpdateLlibre(l *db.Llibre) error                           { return nil }
func (f *fakeDBVerificar) ListLlibrePagines(llibreID int) ([]db.LlibrePagina, error) { return nil, nil }
func (f *fakeDBVerificar) SaveLlibrePagina(p *db.LlibrePagina) (int, error)          { return 0, nil }
func (f *fakeDBVerificar) RecalcLlibrePagines(llibreID, total int) error             { return nil }
func (f *fakeDBVerificar) ListPaisos() ([]db.Pais, error)                            { return nil, nil }
func (f *fakeDBVerificar) GetPais(id int) (*db.Pais, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeDBVerificar) CreatePais(p *db.Pais) (int, error) { return 0, nil }
func (f *fakeDBVerificar) UpdatePais(p *db.Pais) error        { return nil }
func (f *fakeDBVerificar) ListNivells(filt db.NivellAdminFilter) ([]db.NivellAdministratiu, error) {
	return nil, nil
}
func (f *fakeDBVerificar) GetNivell(id int) (*db.NivellAdministratiu, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeDBVerificar) CreateNivell(n *db.NivellAdministratiu) (int, error) { return 0, nil }
func (f *fakeDBVerificar) UpdateNivell(n *db.NivellAdministratiu) error        { return nil }
func (f *fakeDBVerificar) ListMunicipis(filter db.MunicipiFilter) ([]db.MunicipiRow, error) {
	return nil, nil
}
func (f *fakeDBVerificar) GetMunicipi(id int) (*db.Municipi, error) { return nil, nil }
func (f *fakeDBVerificar) CreateMunicipi(m *db.Municipi) (int, error) {
	return 0, nil
}
func (f *fakeDBVerificar) UpdateMunicipi(m *db.Municipi) error { return nil }
func (f *fakeDBVerificar) ListCodisPostals(municipiID int) ([]db.CodiPostal, error) {
	return nil, nil
}
func (f *fakeDBVerificar) SaveCodiPostal(cp *db.CodiPostal) (int, error) { return 0, nil }
func (f *fakeDBVerificar) ListNomsHistorics(entitatTipus string, entitatID int) ([]db.NomHistoric, error) {
	return nil, nil
}
func (f *fakeDBVerificar) SaveNomHistoric(nh *db.NomHistoric) (int, error) { return 0, nil }
func (f *fakeDBVerificar) ListGroups() ([]db.Group, error)                 { return nil, nil }
func (f *fakeDBVerificar) ListArquebisbats(filt db.ArquebisbatFilter) ([]db.ArquebisbatRow, error) {
	return nil, nil
}
func (f *fakeDBVerificar) GetArquebisbat(id int) (*db.Arquebisbat, error) { return nil, nil }
func (f *fakeDBVerificar) CreateArquebisbat(ae *db.Arquebisbat) (int, error) {
	return 0, nil
}
func (f *fakeDBVerificar) UpdateArquebisbat(ae *db.Arquebisbat) error { return nil }
func (f *fakeDBVerificar) ListArquebisbatMunicipis(munID int) ([]db.ArquebisbatMunicipi, error) {
	return nil, nil
}
func (f *fakeDBVerificar) SaveArquebisbatMunicipi(am *db.ArquebisbatMunicipi) (int, error) {
	return 0, nil
}
func (f *fakeDBVerificar) ListPolitiques() ([]db.Politica, error)                 { return nil, nil }
func (f *fakeDBVerificar) GetPolitica(id int) (*db.Politica, error)               { return nil, nil }
func (f *fakeDBVerificar) SavePolitica(p *db.Politica) (int, error)               { return 0, nil }
func (f *fakeDBVerificar) ListUserPolitiques(userID int) ([]db.Politica, error)   { return nil, nil }
func (f *fakeDBVerificar) AddUserPolitica(userID, politicaID int) error           { return nil }
func (f *fakeDBVerificar) RemoveUserPolitica(userID, politicaID int) error        { return nil }
func (f *fakeDBVerificar) ListGroupPolitiques(groupID int) ([]db.Politica, error) { return nil, nil }
func (f *fakeDBVerificar) AddGroupPolitica(groupID, politicaID int) error         { return nil }
func (f *fakeDBVerificar) RemoveGroupPolitica(groupID, politicaID int) error      { return nil }
func (f *fakeDBVerificar) GetEffectivePoliticaPerms(userID int) (db.PolicyPermissions, error) {
	return db.PolicyPermissions{}, nil
}

// Punts i activitat (no-op)
func (f *fakeDBVerificar) ListPointsRules() ([]db.PointsRule, error) { return nil, nil }
func (f *fakeDBVerificar) GetPointsRule(id int) (*db.PointsRule, error) {
	return nil, fmt.Errorf("not found")
}
func (f *fakeDBVerificar) GetPointsRuleByCode(code string) (*db.PointsRule, error) {
	return nil, fmt.Errorf("not found")
}
func (f *fakeDBVerificar) SavePointsRule(r *db.PointsRule) (int, error)       { return 0, nil }
func (f *fakeDBVerificar) InsertUserActivity(a *db.UserActivity) (int, error) { return 0, nil }
func (f *fakeDBVerificar) GetUserActivity(id int) (*db.UserActivity, error) {
	return nil, fmt.Errorf("not found")
}
func (f *fakeDBVerificar) UpdateUserActivityStatus(id int, status string, moderatedBy *int) error {
	return nil
}
func (f *fakeDBVerificar) ListUserActivityByUser(userID int, af db.ActivityFilter) ([]db.UserActivity, error) {
	return nil, nil
}
func (f *fakeDBVerificar) ListActivityByObject(objectType string, objectID int, status string) ([]db.UserActivity, error) {
	return nil, nil
}
func (f *fakeDBVerificar) AddPointsToUser(userID int, delta int) error { return nil }
func (f *fakeDBVerificar) GetUserPoints(userID int) (*db.UserPoints, error) {
	return &db.UserPoints{UserID: userID}, nil
}
func (f *fakeDBVerificar) RecalcUserPoints() error                              { return nil }
func (f *fakeDBVerificar) GetRanking(db.RankingFilter) ([]db.UserPoints, error) { return nil, nil }
func (f *fakeDBVerificar) CountRanking(db.RankingFilter) (int, error)           { return 0, nil }
func (f *fakeDBVerificar) ListUserGroups(userID int) ([]db.Group, error)        { return nil, nil }
func (f *fakeDBVerificar) ListPersones(filter db.PersonaFilter) ([]db.Persona, error) {
	return nil, nil
}
func (f *fakeDBVerificar) GetPersona(id int) (*db.Persona, error) { return nil, nil }
func (f *fakeDBVerificar) CreatePersona(p *db.Persona) (int, error) {
	return 0, nil
}
func (f *fakeDBVerificar) UpdatePersona(p *db.Persona) error { return nil }
func (f *fakeDBVerificar) UpdatePersonaModeracio(id int, estat, motiu string, moderatorID int) error {
	return nil
}
func (f *fakeDBVerificar) UpdateArxiuModeracio(id int, estat, motiu string, moderatorID int) error {
	return nil
}
func (f *fakeDBVerificar) UpdateLlibreModeracio(id int, estat, motiu string, moderatorID int) error {
	return nil
}
func (f *fakeDBVerificar) UpdateNivellModeracio(id int, estat, motiu string, moderatorID int) error {
	return nil
}
func (f *fakeDBVerificar) UpdateMunicipiModeracio(id int, estat, motiu string, moderatorID int) error {
	return nil
}
func (f *fakeDBVerificar) UpdateArquebisbatModeracio(id int, estat, motiu string, moderatorID int) error {
	return nil
}
func (f *fakeDBVerificar) ListTranscripcionsRaw(llibreID int, ftr db.TranscripcioFilter) ([]db.TranscripcioRaw, error) {
	return nil, nil
}
func (f *fakeDBVerificar) ListTranscripcionsRawGlobal(ftr db.TranscripcioFilter) ([]db.TranscripcioRaw, error) {
	return nil, nil
}
func (f *fakeDBVerificar) CountTranscripcionsRaw(llibreID int, ftr db.TranscripcioFilter) (int, error) {
	return 0, nil
}
func (f *fakeDBVerificar) CountTranscripcionsRawGlobal(ftr db.TranscripcioFilter) (int, error) {
	return 0, nil
}
func (f *fakeDBVerificar) GetTranscripcioRaw(id int) (*db.TranscripcioRaw, error) {
	return nil, nil
}
func (f *fakeDBVerificar) CreateTranscripcioRaw(t *db.TranscripcioRaw) (int, error) {
	return 0, nil
}
func (f *fakeDBVerificar) UpdateTranscripcioRaw(t *db.TranscripcioRaw) error {
	return nil
}
func (f *fakeDBVerificar) DeleteTranscripcioRaw(id int) error {
	return nil
}
func (f *fakeDBVerificar) RecalcTranscripcionsRawPageStats(llibreID int) error {
	return nil
}
func (f *fakeDBVerificar) SetTranscripcionsRawPageStatsIndexacio(llibreID int, value int) error {
	return nil
}
func (f *fakeDBVerificar) ListTranscripcionsRawPageStats(llibreID int) ([]db.TranscripcioRawPageStat, error) {
	return nil, nil
}
func (f *fakeDBVerificar) UpdateTranscripcionsRawPageStat(stat *db.TranscripcioRawPageStat) error {
	return nil
}
func (f *fakeDBVerificar) DeleteTranscripcionsByLlibre(llibreID int) error {
	return nil
}
func (f *fakeDBVerificar) ListTranscripcioPersones(transcripcioID int) ([]db.TranscripcioPersonaRaw, error) {
	return nil, nil
}
func (f *fakeDBVerificar) CreateTranscripcioPersona(p *db.TranscripcioPersonaRaw) (int, error) {
	return 0, nil
}
func (f *fakeDBVerificar) LinkTranscripcioPersona(personaRawID int, personaID int, linkedBy int) error {
	return nil
}
func (f *fakeDBVerificar) UnlinkTranscripcioPersona(personaRawID int, linkedBy int) error {
	return nil
}
func (f *fakeDBVerificar) DeleteTranscripcioPersones(transcripcioID int) error {
	return nil
}
func (f *fakeDBVerificar) ListTranscripcioAtributs(transcripcioID int) ([]db.TranscripcioAtributRaw, error) {
	return nil, nil
}
func (f *fakeDBVerificar) CreateTranscripcioAtribut(a *db.TranscripcioAtributRaw) (int, error) {
	return 0, nil
}
func (f *fakeDBVerificar) DeleteTranscripcioAtributs(transcripcioID int) error {
	return nil
}
func (f *fakeDBVerificar) GetTranscripcioDraft(userID, llibreID int) (*db.TranscripcioDraft, error) {
	return nil, nil
}
func (f *fakeDBVerificar) SaveTranscripcioDraft(userID, llibreID int, payload string) error {
	return nil
}
func (f *fakeDBVerificar) DeleteTranscripcioDraft(userID, llibreID int) error {
	return nil
}
func (f *fakeDBVerificar) SearchPersones(filt db.PersonaSearchFilter) ([]db.PersonaSearchResult, error) {
	return nil, nil
}
func (f *fakeDBVerificar) ListRegistresByPersona(personaID int, tipus string) ([]db.PersonaRegistreRow, error) {
	return nil, nil
}
func (f *fakeDBVerificar) UpsertTranscripcioMark(m *db.TranscripcioRawMark) error {
	return nil
}
func (f *fakeDBVerificar) DeleteTranscripcioMark(transcripcioID, userID int) error {
	return nil
}
func (f *fakeDBVerificar) ListTranscripcioMarks(transcripcioIDs []int) ([]db.TranscripcioRawMark, error) {
	return nil, nil
}
func (f *fakeDBVerificar) UpdateTranscripcioModeracio(id int, estat, motiu string, moderatorID int) error {
	return nil
}
func (f *fakeDBVerificar) CreateTranscripcioRawChange(c *db.TranscripcioRawChange) (int, error) {
	return 0, nil
}
func (f *fakeDBVerificar) ListTranscripcioRawChanges(transcripcioID int) ([]db.TranscripcioRawChange, error) {
	return nil, nil
}
func (f *fakeDBVerificar) GetTranscripcioRawChange(id int) (*db.TranscripcioRawChange, error) {
	return nil, errors.New("GetTranscripcioRawChange not implemented in fakeDBVerificar")
}
func (f *fakeDBVerificar) ListTranscripcioRawChangesPending() ([]db.TranscripcioRawChange, error) {
	return nil, nil
}
func (f *fakeDBVerificar) UpdateTranscripcioRawChangeModeracio(id int, estat, motiu string, moderatorID int) error {
	return errors.New("UpdateTranscripcioRawChangeModeracio not implemented in fakeDBVerificar")
}

func (f *fakeDBVerificar) HasLlibreDuplicate(municipiID int, tipus, cronologia, codiDigital, codiFisic string, excludeID int) (bool, error) {
	return false, nil
}
func (f *fakeDBVerificar) GetLlibresIndexacioStats(ids []int) (map[int]db.LlibreIndexacioStats, error) {
	return map[int]db.LlibreIndexacioStats{}, nil
}
func (f *fakeDBVerificar) UpsertLlibreIndexacioStats(stats *db.LlibreIndexacioStats) error {
	return nil
}
func (f *fakeDBVerificar) ListCognoms(q string, limit, offset int) ([]db.Cognom, error) {
	return nil, nil
}
func (f *fakeDBVerificar) GetCognom(id int) (*db.Cognom, error) { return nil, nil }
func (f *fakeDBVerificar) UpsertCognom(forma, key, origen, notes string, createdBy *int) (int, error) {
	return 0, nil
}
func (f *fakeDBVerificar) ListCognomVariants(filter db.CognomVariantFilter) ([]db.CognomVariant, error) {
	return nil, nil
}
func (f *fakeDBVerificar) ResolveCognomPublicatByForma(forma string) (int, string, bool, error) {
	return 0, "", false, nil
}
func (f *fakeDBVerificar) ListCognomFormesPublicades(cognomID int) ([]string, error) {
	return nil, nil
}
func (f *fakeDBVerificar) CreateCognomVariant(v *db.CognomVariant) (int, error) { return 0, nil }
func (f *fakeDBVerificar) UpdateCognomVariantModeracio(id int, estat, motiu string, moderatorID int) error {
	return nil
}
func (f *fakeDBVerificar) UpsertCognomFreqMunicipiAny(cognomID, municipiID, anyDoc, freq int) error {
	return nil
}
func (f *fakeDBVerificar) QueryCognomHeatmap(cognomID int, anyStart, anyEnd int) ([]db.CognomFreqRow, error) {
	return nil, nil
}
func (f *fakeDBVerificar) ListCognomImportRows(limit, offset int) ([]db.CognomImportRow, error) {
	return nil, nil
}
func (f *fakeDBVerificar) ListCognomStatsRows(limit, offset int) ([]db.CognomStatsRow, error) {
	return nil, nil
}

// Crea una App només amb la fake DB per testejar VerificarSessio.
func newAppVerificar(dbFake db.DB) *core.App {
	// FIX: aquí només hi ha Config i DB, res de Templates
	return &core.App{
		Config: map[string]string{},
		DB:     dbFake,
	}
}

// Sense cap cookie de sessió → ha de tornar (nil, false)
func TestVerificarSessio_NoCookie_ReturnsFalse(t *testing.T) {
	fake := &fakeDBVerificar{sessions: map[string]*db.User{}}
	app := newAppVerificar(fake)

	req := httptest.NewRequest(http.MethodGet, "/", nil)

	user, ok := app.VerificarSessio(req)
	if ok {
		t.Fatalf("esperava ok=false sense cookie de sessió, tinc true")
	}
	if user != nil {
		t.Fatalf("esperava user=nil sense cookie de sessió, tinc %#v", user)
	}
}

// Cookie present però buida → també (nil, false)
func TestVerificarSessio_EmptyCookie_ReturnsFalse(t *testing.T) {
	fake := &fakeDBVerificar{sessions: map[string]*db.User{}}
	app := newAppVerificar(fake)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{
		Name:  "cg_session",
		Value: "",
	})

	user, ok := app.VerificarSessio(req)
	if ok {
		t.Fatalf("esperava ok=false amb cookie buida, tinc true")
	}
	if user != nil {
		t.Fatalf("esperava user=nil amb cookie buida, tinc %#v", user)
	}
}

// Cookie amb ID de sessió inexistent → (nil, false)
func TestVerificarSessio_InvalidSession_ReturnsFalse(t *testing.T) {
	fake := &fakeDBVerificar{sessions: map[string]*db.User{}}
	app := newAppVerificar(fake)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{
		Name:  "cg_session",
		Value: "sessio-inexistent",
	})

	user, ok := app.VerificarSessio(req)
	if ok {
		t.Fatalf("esperava ok=false amb sessió inexistent, tinc true")
	}
	if user != nil {
		t.Fatalf("esperava user=nil amb sessió inexistent, tinc %#v", user)
	}
}

// Sessió vàlida → ha de retornar l'usuari i ok=true
func TestVerificarSessio_ValidSession_ReturnsUser(t *testing.T) {
	const sessionID = "sessio-de-prova"

	expectedUser := &db.User{
		ID:      123,
		Usuari:  "testuser",
		Name:    "Test",
		Surname: "User",
		Email:   "test@example.com",
	}

	fake := &fakeDBVerificar{
		sessions: map[string]*db.User{
			sessionID: expectedUser,
		},
	}
	app := newAppVerificar(fake)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{
		Name:  "cg_session",
		Value: sessionID,
	})

	user, ok := app.VerificarSessio(req)
	if !ok {
		t.Fatalf("esperava ok=true amb sessió vàlida, tinc false")
	}
	if user == nil {
		t.Fatalf("esperava user != nil amb sessió vàlida")
	}
	if user.ID != expectedUser.ID {
		t.Fatalf("esperava ID usuari %d, tinc %d", expectedUser.ID, user.ID)
	}
	if user.Usuari != expectedUser.Usuari {
		t.Fatalf("esperava Usuari=%q, tinc %q", expectedUser.Usuari, user.Usuari)
	}
}
