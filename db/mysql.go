package db

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/crypto/bcrypt"
)

type MySQL struct {
	Host   string
	Port   string
	User   string
	Pass   string
	DBName string
	Conn   *sql.DB
	help   sqlHelper
}

func (d *MySQL) Connect() error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", d.User, d.Pass, d.Host, d.Port, d.DBName)
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("error connectant a MySQL: %w", err)
	}
	d.Conn = conn
	d.help = newSQLHelper(conn, "mysql", "NOW()")
	logInfof("Conectat a MySQL")
	return nil
}

func (d *MySQL) Close() {
	if d.Conn != nil {
		d.Conn.Close()
	}
}

func (d *MySQL) Exec(query string, args ...interface{}) (int64, error) {
	res, err := d.Conn.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	id, _ := res.LastInsertId()
	return id, nil
}

func (d *MySQL) Query(query string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := d.Conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		columns, _ := rows.Columns()
		scanArgs := make([]interface{}, len(columns))
		values := make([]interface{}, len(columns))

		for i := range values {
			scanArgs[i] = &values[i]
		}

		err := rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}
		results = append(results, row)
	}
	return results, nil
}

func (d *MySQL) InsertUser(user *User) error {
	return d.help.insertUser(user)
}

func (d *MySQL) GetUserByEmail(email string) (*User, error) {
	return d.help.getUserByEmail(email)
}

func (d *MySQL) GetUserByID(id int) (*User, error) {
	return d.help.getUserByID(id)
}

func (m *MySQL) SaveActivationToken(email, token string) error {
	return m.help.saveActivationToken(email, token)
}

func (d *MySQL) ExistsUserByUsername(username string) (bool, error) {
	return d.help.existsUserByUsername(username)
}

func (d *MySQL) ExistsUserByEmail(email string) (bool, error) {
	return d.help.existsUserByEmail(email)
}

func (d *MySQL) ActivateUser(token string) error {
	return d.help.activateUser(token)
}

func (d *MySQL) AuthenticateUser(usernameOrEmail, password string) (*User, error) {
	u, err := d.help.authenticateUser(usernameOrEmail, password)
	if err != nil {
		return nil, fmt.Errorf("usuari no trobat o no actiu")
	}
	if err := bcrypt.CompareHashAndPassword(u.Password, []byte(password)); err != nil {
		return nil, fmt.Errorf("contrasenya incorrecta")
	}
	return u, nil
}

// Gestió de sessions - adaptat a MySQL
func (d *MySQL) SaveSession(sessionID string, userID int, expiry string) error {
	if err := d.help.saveSession(sessionID, userID, expiry); err != nil {
		logErrorf("[MySQL] Error guardant sessió: %v", err)
		return err
	}
	return nil
}

func (d *MySQL) GetSessionUser(sessionID string) (*User, error) {
	return d.help.getSessionUser(sessionID)
}

func (d *MySQL) DeleteSession(sessionID string) error {
	return d.help.deleteSession(sessionID)
}
func (d *MySQL) ListUserGroups(userID int) ([]Group, error) {
	return d.help.listUserGroups(userID)
}

func (d *MySQL) CreatePasswordReset(email, token, expiry, lang string) (bool, error) {
	return d.help.createPasswordReset(email, token, expiry, lang)
}

func (d *MySQL) GetPasswordReset(token string) (*PasswordReset, error) {
	return d.help.getPasswordReset(token)
}

func (d *MySQL) MarkPasswordResetUsed(id int) error {
	return d.help.markPasswordResetUsed(id)
}

func (d *MySQL) UpdateUserPassword(userID int, passwordHash []byte) error {
	return d.help.updateUserPassword(userID, passwordHash)
}

func (d *MySQL) CreatePrivacyDefaults(userID int) error {
	return d.help.createPrivacyDefaults(userID)
}

func (d *MySQL) GetPrivacySettings(userID int) (*PrivacySettings, error) {
	return d.help.getPrivacySettings(userID)
}

func (d *MySQL) SavePrivacySettings(userID int, p *PrivacySettings) error {
	return d.help.savePrivacySettings(userID, p)
}

func (d *MySQL) UpdateUserProfile(u *User) error {
	return d.help.updateUserProfile(u)
}

func (d *MySQL) UpdateUserEmail(userID int, newEmail string) error {
	return d.help.updateUserEmail(userID, newEmail)
}

func (d *MySQL) CreateEmailChange(userID int, newEmail, tokenConfirm, expConfirm, tokenRevert, expRevert, lang string) error {
	return d.help.createEmailChange(userID, newEmail, tokenConfirm, expConfirm, tokenRevert, expRevert, lang)
}

func (d *MySQL) ConfirmEmailChange(token string) (*EmailChange, error) {
	return d.help.confirmEmailChange(token)
}

func (d *MySQL) RevertEmailChange(token string) (*EmailChange, error) {
	return d.help.revertEmailChange(token)
}

func (d *MySQL) markEmailChangeConfirmed(id int) error {
	return d.help.markEmailChangeConfirmed(id)
}

func (d *MySQL) markEmailChangeReverted(id int) error {
	return d.help.markEmailChangeReverted(id)
}

// Policies
func (d *MySQL) UserHasAnyPolicy(userID int, policies []string) (bool, error) {
	return d.help.userHasAnyPolicy(userID, policies)
}
func (d *MySQL) EnsureDefaultPolicies() error {
	return d.help.ensureDefaultPolicies()
}
func (d *MySQL) EnsureDefaultPointsRules() error {
	return d.help.ensureDefaultPointsRules()
}
func (d *MySQL) ListGroups() ([]Group, error) {
	return d.help.listGroups()
}
func (d *MySQL) ListPolitiques() ([]Politica, error) {
	return d.help.listPolitiques()
}
func (d *MySQL) GetPolitica(id int) (*Politica, error) {
	return d.help.getPolitica(id)
}
func (d *MySQL) SavePolitica(p *Politica) (int, error) {
	return d.help.savePolitica(p)
}
func (d *MySQL) ListUserPolitiques(userID int) ([]Politica, error) {
	return d.help.listUserPolitiques(userID)
}
func (d *MySQL) AddUserPolitica(userID, politicaID int) error {
	return d.help.addUserPolitica(userID, politicaID)
}
func (d *MySQL) RemoveUserPolitica(userID, politicaID int) error {
	return d.help.removeUserPolitica(userID, politicaID)
}
func (d *MySQL) ListGroupPolitiques(groupID int) ([]Politica, error) {
	return d.help.listGroupPolitiques(groupID)
}
func (d *MySQL) AddGroupPolitica(groupID, politicaID int) error {
	return d.help.addGroupPolitica(groupID, politicaID)
}
func (d *MySQL) RemoveGroupPolitica(groupID, politicaID int) error {
	return d.help.removeGroupPolitica(groupID, politicaID)
}
func (d *MySQL) GetEffectivePoliticaPerms(userID int) (PolicyPermissions, error) {
	return d.help.getEffectivePoliticaPerms(userID)
}

// Persones (moderació)
func (d *MySQL) ListPersones(f PersonaFilter) ([]Persona, error) {
	return d.help.listPersones(f)
}
func (d *MySQL) GetPersona(id int) (*Persona, error) {
	return d.help.getPersona(id)
}
func (d *MySQL) CreatePersona(p *Persona) (int, error) {
	return d.help.createPersona(p)
}
func (d *MySQL) UpdatePersona(p *Persona) error {
	return d.help.updatePersona(p)
}
func (d *MySQL) UpdatePersonaModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updatePersonaModeracio(id, estat, motiu, moderatorID)
}

// Paisos
func (d *MySQL) ListPaisos() ([]Pais, error) {
	return d.help.listPaisos()
}
func (d *MySQL) GetPais(id int) (*Pais, error) {
	return d.help.getPais(id)
}
func (d *MySQL) CreatePais(p *Pais) (int, error) {
	return d.help.createPais(p)
}
func (d *MySQL) UpdatePais(p *Pais) error {
	return d.help.updatePais(p)
}

// Nivells administratius
func (d *MySQL) ListNivells(f NivellAdminFilter) ([]NivellAdministratiu, error) {
	return d.help.listNivells(f)
}
func (d *MySQL) GetNivell(id int) (*NivellAdministratiu, error) {
	return d.help.getNivell(id)
}
func (d *MySQL) CreateNivell(n *NivellAdministratiu) (int, error) {
	return d.help.createNivell(n)
}
func (d *MySQL) UpdateNivell(n *NivellAdministratiu) error {
	return d.help.updateNivell(n)
}
func (d *MySQL) UpdateNivellModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateNivellModeracio(id, estat, motiu, moderatorID)
}

// Municipis
func (d *MySQL) ListMunicipis(f MunicipiFilter) ([]MunicipiRow, error) {
	return d.help.listMunicipis(f)
}
func (d *MySQL) GetMunicipi(id int) (*Municipi, error) {
	return d.help.getMunicipi(id)
}
func (d *MySQL) CreateMunicipi(m *Municipi) (int, error) {
	return d.help.createMunicipi(m)
}
func (d *MySQL) UpdateMunicipi(m *Municipi) error {
	return d.help.updateMunicipi(m)
}
func (d *MySQL) UpdateMunicipiModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateMunicipiModeracio(id, estat, motiu, moderatorID)
}
func (d *MySQL) ListCodisPostals(municipiID int) ([]CodiPostal, error) {
	return d.help.listCodisPostals(municipiID)
}
func (d *MySQL) SaveCodiPostal(cp *CodiPostal) (int, error) {
	return d.help.saveCodiPostal(cp)
}
func (d *MySQL) ListNomsHistorics(entitatTipus string, entitatID int) ([]NomHistoric, error) {
	return d.help.listNomsHistorics(entitatTipus, entitatID)
}
func (d *MySQL) SaveNomHistoric(nh *NomHistoric) (int, error) {
	return d.help.saveNomHistoric(nh)
}

// Entitats eclesiàstiques
func (d *MySQL) ListArquebisbats(f ArquebisbatFilter) ([]ArquebisbatRow, error) {
	return d.help.listArquebisbats(f)
}
func (d *MySQL) GetArquebisbat(id int) (*Arquebisbat, error) {
	return d.help.getArquebisbat(id)
}
func (d *MySQL) CreateArquebisbat(ae *Arquebisbat) (int, error) {
	return d.help.createArquebisbat(ae)
}
func (d *MySQL) UpdateArquebisbat(ae *Arquebisbat) error {
	return d.help.updateArquebisbat(ae)
}
func (d *MySQL) UpdateArquebisbatModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateArquebisbatModeracio(id, estat, motiu, moderatorID)
}
func (d *MySQL) ListArquebisbatMunicipis(munID int) ([]ArquebisbatMunicipi, error) {
	return d.help.listArquebisbatMunicipis(munID)
}
func (d *MySQL) SaveArquebisbatMunicipi(am *ArquebisbatMunicipi) (int, error) {
	return d.help.saveArquebisbatMunicipi(am)
}

// Arxius
func (d *MySQL) ListArxius(f ArxiuFilter) ([]ArxiuWithCount, error) {
	return d.help.listArxius(f)
}
func (d *MySQL) GetArxiu(id int) (*Arxiu, error) {
	return d.help.getArxiu(id)
}
func (d *MySQL) CreateArxiu(a *Arxiu) (int, error) {
	return d.help.createArxiu(a)
}
func (d *MySQL) UpdateArxiu(a *Arxiu) error {
	return d.help.updateArxiu(a)
}
func (d *MySQL) UpdateArxiuModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateArxiuModeracio(id, estat, motiu, moderatorID)
}
func (d *MySQL) DeleteArxiu(id int) error {
	return d.help.deleteArxiu(id)
}
func (d *MySQL) ListArxiuLlibres(arxiuID int) ([]ArxiuLlibreDetail, error) {
	return d.help.listArxiuLlibres(arxiuID)
}
func (d *MySQL) ListLlibreArxius(llibreID int) ([]ArxiuLlibreDetail, error) {
	return d.help.listLlibreArxius(llibreID)
}
func (d *MySQL) AddArxiuLlibre(arxiuID, llibreID int, signatura, urlOverride string) error {
	return d.help.addArxiuLlibre(arxiuID, llibreID, signatura, urlOverride)
}
func (d *MySQL) UpdateArxiuLlibre(arxiuID, llibreID int, signatura, urlOverride string) error {
	return d.help.updateArxiuLlibre(arxiuID, llibreID, signatura, urlOverride)
}
func (d *MySQL) DeleteArxiuLlibre(arxiuID, llibreID int) error {
	return d.help.deleteArxiuLlibre(arxiuID, llibreID)
}
func (d *MySQL) SearchLlibresSimple(q string, limit int) ([]LlibreSimple, error) {
	return d.help.searchLlibresSimple(q, limit)
}
func (d *MySQL) ListLlibres(f LlibreFilter) ([]LlibreRow, error) {
	return d.help.listLlibres(f)
}
func (d *MySQL) GetLlibre(id int) (*Llibre, error) {
	return d.help.getLlibre(id)
}
func (d *MySQL) CreateLlibre(l *Llibre) (int, error) {
	return d.help.createLlibre(l)
}
func (d *MySQL) UpdateLlibre(l *Llibre) error {
	return d.help.updateLlibre(l)
}
func (d *MySQL) UpdateLlibreModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateLlibreModeracio(id, estat, motiu, moderatorID)
}
func (d *MySQL) ListLlibrePagines(llibreID int) ([]LlibrePagina, error) {
	return d.help.listLlibrePagines(llibreID)
}
func (d *MySQL) SaveLlibrePagina(p *LlibrePagina) (int, error) {
	return d.help.saveLlibrePagina(p)
}
func (d *MySQL) RecalcLlibrePagines(llibreID, total int) error {
	return d.help.recalcLlibrePagines(llibreID, total)
}

func (d *MySQL) ListTranscripcionsRaw(llibreID int, f TranscripcioFilter) ([]TranscripcioRaw, error) {
	return d.help.listTranscripcionsRaw(llibreID, f)
}
func (d *MySQL) ListTranscripcionsRawGlobal(f TranscripcioFilter) ([]TranscripcioRaw, error) {
	return d.help.listTranscripcionsRawGlobal(f)
}
func (d *MySQL) CountTranscripcionsRaw(llibreID int, f TranscripcioFilter) (int, error) {
	return d.help.countTranscripcionsRaw(llibreID, f)
}
func (d *MySQL) CountTranscripcionsRawGlobal(f TranscripcioFilter) (int, error) {
	return d.help.countTranscripcionsRawGlobal(f)
}
func (d *MySQL) GetTranscripcioRaw(id int) (*TranscripcioRaw, error) {
	return d.help.getTranscripcioRaw(id)
}
func (d *MySQL) CreateTranscripcioRaw(t *TranscripcioRaw) (int, error) {
	return d.help.createTranscripcioRaw(t)
}
func (d *MySQL) UpdateTranscripcioRaw(t *TranscripcioRaw) error {
	return d.help.updateTranscripcioRaw(t)
}
func (d *MySQL) DeleteTranscripcioRaw(id int) error {
	return d.help.deleteTranscripcioRaw(id)
}
func (d *MySQL) ListTranscripcioPersones(transcripcioID int) ([]TranscripcioPersonaRaw, error) {
	return d.help.listTranscripcioPersones(transcripcioID)
}
func (d *MySQL) CreateTranscripcioPersona(p *TranscripcioPersonaRaw) (int, error) {
	return d.help.createTranscripcioPersona(p)
}
func (d *MySQL) DeleteTranscripcioPersones(transcripcioID int) error {
	return d.help.deleteTranscripcioPersones(transcripcioID)
}
func (d *MySQL) LinkTranscripcioPersona(personaRawID int, personaID int, linkedBy int) error {
	return d.help.linkTranscripcioPersona(personaRawID, personaID, linkedBy)
}
func (d *MySQL) UnlinkTranscripcioPersona(personaRawID int, linkedBy int) error {
	return d.help.unlinkTranscripcioPersona(personaRawID, linkedBy)
}
func (d *MySQL) ListTranscripcioAtributs(transcripcioID int) ([]TranscripcioAtributRaw, error) {
	return d.help.listTranscripcioAtributs(transcripcioID)
}
func (d *MySQL) CreateTranscripcioAtribut(a *TranscripcioAtributRaw) (int, error) {
	return d.help.createTranscripcioAtribut(a)
}
func (d *MySQL) DeleteTranscripcioAtributs(transcripcioID int) error {
	return d.help.deleteTranscripcioAtributs(transcripcioID)
}
func (d *MySQL) GetTranscripcioDraft(userID, llibreID int) (*TranscripcioDraft, error) {
	return d.help.getTranscripcioDraft(userID, llibreID)
}
func (d *MySQL) SaveTranscripcioDraft(userID, llibreID int, payload string) error {
	return d.help.saveTranscripcioDraft(userID, llibreID, payload)
}
func (d *MySQL) DeleteTranscripcioDraft(userID, llibreID int) error {
	return d.help.deleteTranscripcioDraft(userID, llibreID)
}
func (d *MySQL) SearchPersones(f PersonaSearchFilter) ([]PersonaSearchResult, error) {
	return d.help.searchPersones(f)
}
func (d *MySQL) ListRegistresByPersona(personaID int, tipus string) ([]PersonaRegistreRow, error) {
	return d.help.listRegistresByPersona(personaID, tipus)
}

// Punts i activitat
func (d *MySQL) ListPointsRules() ([]PointsRule, error) { return d.help.listPointsRules() }
func (d *MySQL) GetPointsRule(id int) (*PointsRule, error) {
	return d.help.getPointsRule(id)
}
func (d *MySQL) GetPointsRuleByCode(code string) (*PointsRule, error) {
	return d.help.getPointsRuleByCode(code)
}
func (d *MySQL) SavePointsRule(r *PointsRule) (int, error)       { return d.help.savePointsRule(r) }
func (d *MySQL) GetUserActivity(id int) (*UserActivity, error)   { return d.help.getUserActivity(id) }
func (d *MySQL) InsertUserActivity(a *UserActivity) (int, error) { return d.help.insertUserActivity(a) }
func (d *MySQL) UpdateUserActivityStatus(id int, status string, moderatedBy *int) error {
	return d.help.updateUserActivityStatus(id, status, moderatedBy)
}
func (d *MySQL) ListUserActivityByUser(userID int, f ActivityFilter) ([]UserActivity, error) {
	return d.help.listUserActivityByUser(userID, f)
}
func (d *MySQL) ListActivityByObject(objectType string, objectID int, status string) ([]UserActivity, error) {
	return d.help.listActivityByObject(objectType, objectID, status)
}
func (d *MySQL) AddPointsToUser(userID int, delta int) error {
	return d.help.addPointsToUser(userID, delta)
}
func (d *MySQL) GetUserPoints(userID int) (*UserPoints, error)    { return d.help.getUserPoints(userID) }
func (d *MySQL) RecalcUserPoints() error                          { return d.help.recalcUserPoints() }
func (d *MySQL) GetRanking(f RankingFilter) ([]UserPoints, error) { return d.help.getRanking(f) }
func (d *MySQL) CountRanking(f RankingFilter) (int, error)        { return d.help.countRanking(f) }
