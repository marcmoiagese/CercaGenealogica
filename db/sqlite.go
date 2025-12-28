package db

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/crypto/bcrypt"
)

type SQLite struct {
	Path string
	Conn *sql.DB
	help sqlHelper
}

func (d *SQLite) Connect() error {
	dsn := d.Path
	if !strings.HasPrefix(dsn, "file:") {
		dsn = "file:" + dsn
	}
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	dsn = dsn + sep + "_foreign_keys=1&_journal_mode=WAL&_busy_timeout=15000"
	conn, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return fmt.Errorf("error connectant a SQLite: %w", err)
	}
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)
	_, _ = conn.Exec("PRAGMA foreign_keys = ON")
	_, _ = conn.Exec("PRAGMA journal_mode = WAL")
	_, _ = conn.Exec("PRAGMA busy_timeout = 15000")
	_, _ = conn.Exec("PRAGMA synchronous = NORMAL")
	d.Conn = conn
	d.help = newSQLHelper(conn, "sqlite", "datetime('now')")
	logInfof("Conectat a SQLite")
	return nil
}

func (d *SQLite) Close() {
	if d.Conn != nil {
		d.Conn.Close()
	}
}

func (d *SQLite) Query(query string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := d.Conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Processa resultats
	columns, _ := rows.Columns()
	results := []map[string]interface{}{}

	for rows.Next() {
		scanArgs := make([]interface{}, len(columns))
		values := make([]interface{}, len(columns))

		for i := range values {
			scanArgs[i] = &values[i]
		}

		rows.Scan(scanArgs...)

		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}

		results = append(results, row)
	}

	return results, nil
}

func (d *SQLite) Exec(query string, args ...interface{}) (int64, error) {
	res, err := d.Conn.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	id, _ := res.LastInsertId()
	return id, nil
}

func (d *SQLite) InsertUser(user *User) error {
	if err := d.help.insertUser(user); err != nil {
		logErrorf("[SQLite] Error a InsertUser: %v", err)
		return err
	}
	return nil
}

func (d *SQLite) GetUserByEmail(email string) (*User, error) {
	return d.help.getUserByEmail(email)
}

func (d *SQLite) GetUserByID(id int) (*User, error) {
	return d.help.getUserByID(id)
}

func (d *SQLite) ExistsUserByUsername(username string) (bool, error) {
	return d.help.existsUserByUsername(username)
}

func (d *SQLite) ExistsUserByEmail(email string) (bool, error) {
	return d.help.existsUserByEmail(email)
}

func (s *SQLite) SaveActivationToken(email, token string) error {
	return s.help.saveActivationToken(email, token)
}

func (d *SQLite) ActivateUser(token string) error {
	return d.help.activateUser(token)
}

func (d *SQLite) AuthenticateUser(usernameOrEmail, password string) (*User, error) {
	u, err := d.help.authenticateUser(usernameOrEmail, password)
	if err != nil {
		return nil, fmt.Errorf("usuari no trobat o no actiu")
	}

	// Verificar contrasenya (assumim que està hashejada amb bcrypt)
	if err := bcrypt.CompareHashAndPassword(u.Password, []byte(password)); err != nil {
		return nil, fmt.Errorf("contrasenya incorrecta")
	}

	return u, nil
}

// Gestió de sessions - adaptat a l'estructura existent de la taula sessions
func (d *SQLite) SaveSession(sessionID string, userID int, expiry string) error {
	if err := d.help.saveSession(sessionID, userID, expiry); err != nil {
		logErrorf("[SQLite] Error guardant sessió: %v", err)
		return err
	}
	return nil
}

func (d *SQLite) GetSessionUser(sessionID string) (*User, error) {
	return d.help.getSessionUser(sessionID)
}

func (d *SQLite) DeleteSession(sessionID string) error {
	return d.help.deleteSession(sessionID)
}

func (d *SQLite) CreatePasswordReset(email, token, expiry, lang string) (bool, error) {
	return d.help.createPasswordReset(email, token, expiry, lang)
}

func (d *SQLite) GetPasswordReset(token string) (*PasswordReset, error) {
	return d.help.getPasswordReset(token)
}

func (d *SQLite) MarkPasswordResetUsed(id int) error {
	return d.help.markPasswordResetUsed(id)
}

func (d *SQLite) UpdateUserPassword(userID int, passwordHash []byte) error {
	return d.help.updateUserPassword(userID, passwordHash)
}

func (d *SQLite) CreatePrivacyDefaults(userID int) error {
	return d.help.createPrivacyDefaults(userID)
}

func (d *SQLite) GetPrivacySettings(userID int) (*PrivacySettings, error) {
	return d.help.getPrivacySettings(userID)
}

func (d *SQLite) SavePrivacySettings(userID int, p *PrivacySettings) error {
	return d.help.savePrivacySettings(userID, p)
}

func (d *SQLite) UpdateUserProfile(u *User) error {
	return d.help.updateUserProfile(u)
}

func (d *SQLite) UpdateUserEmail(userID int, newEmail string) error {
	return d.help.updateUserEmail(userID, newEmail)
}

func (d *SQLite) ListUsersAdmin() ([]UserAdminRow, error) {
	return d.help.listUsersAdmin()
}

func (d *SQLite) ListUsersAdminFiltered(filter UserAdminFilter) ([]UserAdminRow, error) {
	return d.help.listUsersAdminFiltered(filter)
}

func (d *SQLite) CountUsersAdmin(filter UserAdminFilter) (int, error) {
	return d.help.countUsersAdmin(filter)
}

func (d *SQLite) SetUserActive(userID int, active bool) error {
	return d.help.setUserActive(userID, active)
}

func (d *SQLite) SetUserBanned(userID int, banned bool) error {
	return d.help.setUserBanned(userID, banned)
}

func (d *SQLite) CreateEmailChange(userID int, newEmail, tokenConfirm, expConfirm, tokenRevert, expRevert, lang string) error {
	return d.help.createEmailChange(userID, newEmail, tokenConfirm, expConfirm, tokenRevert, expRevert, lang)
}

func (d *SQLite) ConfirmEmailChange(token string) (*EmailChange, error) {
	return d.help.confirmEmailChange(token)
}

func (d *SQLite) RevertEmailChange(token string) (*EmailChange, error) {
	return d.help.revertEmailChange(token)
}

func (d *SQLite) markEmailChangeConfirmed(id int) error {
	return d.help.markEmailChangeConfirmed(id)
}

func (d *SQLite) markEmailChangeReverted(id int) error {
	return d.help.markEmailChangeReverted(id)
}

// Policies
func (d *SQLite) UserHasAnyPolicy(userID int, policies []string) (bool, error) {
	return d.help.userHasAnyPolicy(userID, policies)
}
func (d *SQLite) EnsureDefaultPolicies() error {
	return d.help.ensureDefaultPolicies()
}
func (d *SQLite) EnsureDefaultPointsRules() error {
	return d.help.ensureDefaultPointsRules()
}
func (d *SQLite) ListGroups() ([]Group, error) {
	return d.help.listGroups()
}
func (d *SQLite) ListPolitiques() ([]Politica, error) {
	return d.help.listPolitiques()
}
func (d *SQLite) GetPolitica(id int) (*Politica, error) {
	return d.help.getPolitica(id)
}
func (d *SQLite) SavePolitica(p *Politica) (int, error) {
	return d.help.savePolitica(p)
}
func (d *SQLite) ListUserPolitiques(userID int) ([]Politica, error) {
	return d.help.listUserPolitiques(userID)
}
func (d *SQLite) AddUserPolitica(userID, politicaID int) error {
	return d.help.addUserPolitica(userID, politicaID)
}
func (d *SQLite) RemoveUserPolitica(userID, politicaID int) error {
	return d.help.removeUserPolitica(userID, politicaID)
}
func (d *SQLite) ListGroupPolitiques(groupID int) ([]Politica, error) {
	return d.help.listGroupPolitiques(groupID)
}
func (d *SQLite) AddGroupPolitica(groupID, politicaID int) error {
	return d.help.addGroupPolitica(groupID, politicaID)
}
func (d *SQLite) RemoveGroupPolitica(groupID, politicaID int) error {
	return d.help.removeGroupPolitica(groupID, politicaID)
}
func (d *SQLite) ListUserGroups(userID int) ([]Group, error) {
	return d.help.listUserGroups(userID)
}
func (d *SQLite) GetEffectivePoliticaPerms(userID int) (PolicyPermissions, error) {
	return d.help.getEffectivePoliticaPerms(userID)
}

// Persones (moderació)
func (d *SQLite) ListPersones(f PersonaFilter) ([]Persona, error) {
	return d.help.listPersones(f)
}
func (d *SQLite) GetPersona(id int) (*Persona, error) {
	return d.help.getPersona(id)
}
func (d *SQLite) CreatePersona(p *Persona) (int, error) {
	return d.help.createPersona(p)
}
func (d *SQLite) UpdatePersona(p *Persona) error {
	return d.help.updatePersona(p)
}
func (d *SQLite) UpdatePersonaModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updatePersonaModeracio(id, estat, motiu, moderatorID)
}

// Paisos
func (d *SQLite) ListPaisos() ([]Pais, error) {
	return d.help.listPaisos()
}
func (d *SQLite) GetPais(id int) (*Pais, error) {
	return d.help.getPais(id)
}
func (d *SQLite) CreatePais(p *Pais) (int, error) {
	return d.help.createPais(p)
}
func (d *SQLite) UpdatePais(p *Pais) error {
	return d.help.updatePais(p)
}

// Nivells administratius
func (d *SQLite) ListNivells(f NivellAdminFilter) ([]NivellAdministratiu, error) {
	return d.help.listNivells(f)
}
func (d *SQLite) GetNivell(id int) (*NivellAdministratiu, error) {
	return d.help.getNivell(id)
}
func (d *SQLite) CreateNivell(n *NivellAdministratiu) (int, error) {
	return d.help.createNivell(n)
}
func (d *SQLite) UpdateNivell(n *NivellAdministratiu) error {
	return d.help.updateNivell(n)
}
func (d *SQLite) UpdateNivellModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateNivellModeracio(id, estat, motiu, moderatorID)
}

// Municipis
func (d *SQLite) ListMunicipis(f MunicipiFilter) ([]MunicipiRow, error) {
	return d.help.listMunicipis(f)
}
func (d *SQLite) GetMunicipi(id int) (*Municipi, error) {
	return d.help.getMunicipi(id)
}
func (d *SQLite) CreateMunicipi(m *Municipi) (int, error) {
	return d.help.createMunicipi(m)
}
func (d *SQLite) UpdateMunicipi(m *Municipi) error {
	return d.help.updateMunicipi(m)
}
func (d *SQLite) UpdateMunicipiModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateMunicipiModeracio(id, estat, motiu, moderatorID)
}
func (d *SQLite) ListCodisPostals(municipiID int) ([]CodiPostal, error) {
	return d.help.listCodisPostals(municipiID)
}
func (d *SQLite) SaveCodiPostal(cp *CodiPostal) (int, error) {
	return d.help.saveCodiPostal(cp)
}
func (d *SQLite) ListNomsHistorics(entitatTipus string, entitatID int) ([]NomHistoric, error) {
	return d.help.listNomsHistorics(entitatTipus, entitatID)
}
func (d *SQLite) SaveNomHistoric(nh *NomHistoric) (int, error) {
	return d.help.saveNomHistoric(nh)
}

// Entitats eclesiàstiques
func (d *SQLite) ListArquebisbats(f ArquebisbatFilter) ([]ArquebisbatRow, error) {
	return d.help.listArquebisbats(f)
}
func (d *SQLite) GetArquebisbat(id int) (*Arquebisbat, error) {
	return d.help.getArquebisbat(id)
}
func (d *SQLite) CreateArquebisbat(ae *Arquebisbat) (int, error) {
	return d.help.createArquebisbat(ae)
}
func (d *SQLite) UpdateArquebisbat(ae *Arquebisbat) error {
	return d.help.updateArquebisbat(ae)
}
func (d *SQLite) UpdateArquebisbatModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateArquebisbatModeracio(id, estat, motiu, moderatorID)
}
func (d *SQLite) ListArquebisbatMunicipis(munID int) ([]ArquebisbatMunicipi, error) {
	return d.help.listArquebisbatMunicipis(munID)
}
func (d *SQLite) SaveArquebisbatMunicipi(am *ArquebisbatMunicipi) (int, error) {
	return d.help.saveArquebisbatMunicipi(am)
}

// Arxius
func (d *SQLite) ListArxius(f ArxiuFilter) ([]ArxiuWithCount, error) {
	return d.help.listArxius(f)
}
func (d *SQLite) GetArxiu(id int) (*Arxiu, error) {
	return d.help.getArxiu(id)
}
func (d *SQLite) CreateArxiu(a *Arxiu) (int, error) {
	return d.help.createArxiu(a)
}
func (d *SQLite) UpdateArxiu(a *Arxiu) error {
	return d.help.updateArxiu(a)
}
func (d *SQLite) UpdateArxiuModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateArxiuModeracio(id, estat, motiu, moderatorID)
}
func (d *SQLite) DeleteArxiu(id int) error {
	return d.help.deleteArxiu(id)
}
func (d *SQLite) ListArxiuLlibres(arxiuID int) ([]ArxiuLlibreDetail, error) {
	return d.help.listArxiuLlibres(arxiuID)
}
func (d *SQLite) ListLlibreArxius(llibreID int) ([]ArxiuLlibreDetail, error) {
	return d.help.listLlibreArxius(llibreID)
}

func (d *SQLite) ListLlibreURLs(llibreID int) ([]LlibreURL, error) {
	return d.help.listLlibreURLs(llibreID)
}

func (d *SQLite) AddLlibreURL(link *LlibreURL) error {
	return d.help.addLlibreURL(link)
}

func (d *SQLite) DeleteLlibreURL(id int) error {
	return d.help.deleteLlibreURL(id)
}
func (d *SQLite) AddArxiuLlibre(arxiuID, llibreID int, signatura, urlOverride string) error {
	return d.help.addArxiuLlibre(arxiuID, llibreID, signatura, urlOverride)
}
func (d *SQLite) UpdateArxiuLlibre(arxiuID, llibreID int, signatura, urlOverride string) error {
	return d.help.updateArxiuLlibre(arxiuID, llibreID, signatura, urlOverride)
}
func (d *SQLite) DeleteArxiuLlibre(arxiuID, llibreID int) error {
	return d.help.deleteArxiuLlibre(arxiuID, llibreID)
}
func (d *SQLite) SearchLlibresSimple(q string, limit int) ([]LlibreSimple, error) {
	return d.help.searchLlibresSimple(q, limit)
}
func (d *SQLite) ListLlibres(f LlibreFilter) ([]LlibreRow, error) {
	return d.help.listLlibres(f)
}
func (d *SQLite) GetLlibre(id int) (*Llibre, error) {
	return d.help.getLlibre(id)
}
func (d *SQLite) CreateLlibre(l *Llibre) (int, error) {
	return d.help.createLlibre(l)
}
func (d *SQLite) UpdateLlibre(l *Llibre) error {
	return d.help.updateLlibre(l)
}
func (d *SQLite) HasLlibreDuplicate(municipiID int, tipus, cronologia, codiDigital, codiFisic string, excludeID int) (bool, error) {
	return d.help.hasLlibreDuplicate(municipiID, tipus, cronologia, codiDigital, codiFisic, excludeID)
}
func (d *SQLite) GetLlibresIndexacioStats(ids []int) (map[int]LlibreIndexacioStats, error) {
	return d.help.getLlibresIndexacioStats(ids)
}
func (d *SQLite) UpsertLlibreIndexacioStats(stats *LlibreIndexacioStats) error {
	return d.help.upsertLlibreIndexacioStats(stats)
}
func (d *SQLite) UpdateLlibreModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateLlibreModeracio(id, estat, motiu, moderatorID)
}
func (d *SQLite) ListLlibrePagines(llibreID int) ([]LlibrePagina, error) {
	return d.help.listLlibrePagines(llibreID)
}
func (d *SQLite) SaveLlibrePagina(p *LlibrePagina) (int, error) {
	return d.help.saveLlibrePagina(p)
}
func (d *SQLite) RecalcLlibrePagines(llibreID, total int) error {
	return d.help.recalcLlibrePagines(llibreID, total)
}

func (d *SQLite) ListTranscripcionsRaw(llibreID int, f TranscripcioFilter) ([]TranscripcioRaw, error) {
	return d.help.listTranscripcionsRaw(llibreID, f)
}
func (d *SQLite) ListTranscripcionsRawGlobal(f TranscripcioFilter) ([]TranscripcioRaw, error) {
	return d.help.listTranscripcionsRawGlobal(f)
}
func (d *SQLite) CountTranscripcionsRaw(llibreID int, f TranscripcioFilter) (int, error) {
	return d.help.countTranscripcionsRaw(llibreID, f)
}
func (d *SQLite) CountTranscripcionsRawGlobal(f TranscripcioFilter) (int, error) {
	return d.help.countTranscripcionsRawGlobal(f)
}
func (d *SQLite) GetTranscripcioRaw(id int) (*TranscripcioRaw, error) {
	return d.help.getTranscripcioRaw(id)
}
func (d *SQLite) CreateTranscripcioRaw(t *TranscripcioRaw) (int, error) {
	return d.help.createTranscripcioRaw(t)
}
func (d *SQLite) UpdateTranscripcioRaw(t *TranscripcioRaw) error {
	return d.help.updateTranscripcioRaw(t)
}
func (d *SQLite) UpdateTranscripcioModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateTranscripcioModeracio(id, estat, motiu, moderatorID)
}
func (d *SQLite) DeleteTranscripcioRaw(id int) error {
	return d.help.deleteTranscripcioRaw(id)
}
func (d *SQLite) ListTranscripcionsRawPageStats(llibreID int) ([]TranscripcioRawPageStat, error) {
	return d.help.listTranscripcionsRawPageStats(llibreID)
}
func (d *SQLite) UpdateTranscripcionsRawPageStat(stat *TranscripcioRawPageStat) error {
	return d.help.updateTranscripcionsRawPageStat(stat)
}
func (d *SQLite) RecalcTranscripcionsRawPageStats(llibreID int) error {
	return d.help.recalcTranscripcionsRawPageStats(llibreID)
}
func (d *SQLite) SetTranscripcionsRawPageStatsIndexacio(llibreID int, value int) error {
	return d.help.setTranscripcionsRawPageStatsIndexacio(llibreID, value)
}

func (d *SQLite) DeleteTranscripcionsByLlibre(llibreID int) error {
	return d.help.deleteTranscripcionsByLlibre(llibreID)
}
func (d *SQLite) CreateTranscripcioRawChange(c *TranscripcioRawChange) (int, error) {
	return d.help.createTranscripcioRawChange(c)
}
func (d *SQLite) ListTranscripcioRawChanges(transcripcioID int) ([]TranscripcioRawChange, error) {
	return d.help.listTranscripcioRawChanges(transcripcioID)
}
func (d *SQLite) ListTranscripcioPersones(transcripcioID int) ([]TranscripcioPersonaRaw, error) {
	return d.help.listTranscripcioPersones(transcripcioID)
}
func (d *SQLite) CreateTranscripcioPersona(p *TranscripcioPersonaRaw) (int, error) {
	return d.help.createTranscripcioPersona(p)
}
func (d *SQLite) DeleteTranscripcioPersones(transcripcioID int) error {
	return d.help.deleteTranscripcioPersones(transcripcioID)
}
func (d *SQLite) LinkTranscripcioPersona(personaRawID int, personaID int, linkedBy int) error {
	return d.help.linkTranscripcioPersona(personaRawID, personaID, linkedBy)
}
func (d *SQLite) UnlinkTranscripcioPersona(personaRawID int, linkedBy int) error {
	return d.help.unlinkTranscripcioPersona(personaRawID, linkedBy)
}
func (d *SQLite) ListTranscripcioAtributs(transcripcioID int) ([]TranscripcioAtributRaw, error) {
	return d.help.listTranscripcioAtributs(transcripcioID)
}
func (d *SQLite) CreateTranscripcioAtribut(a *TranscripcioAtributRaw) (int, error) {
	return d.help.createTranscripcioAtribut(a)
}
func (d *SQLite) DeleteTranscripcioAtributs(transcripcioID int) error {
	return d.help.deleteTranscripcioAtributs(transcripcioID)
}
func (d *SQLite) GetTranscripcioDraft(userID, llibreID int) (*TranscripcioDraft, error) {
	return d.help.getTranscripcioDraft(userID, llibreID)
}
func (d *SQLite) SaveTranscripcioDraft(userID, llibreID int, payload string) error {
	return d.help.saveTranscripcioDraft(userID, llibreID, payload)
}
func (d *SQLite) DeleteTranscripcioDraft(userID, llibreID int) error {
	return d.help.deleteTranscripcioDraft(userID, llibreID)
}
func (d *SQLite) UpsertTranscripcioMark(m *TranscripcioRawMark) error {
	return d.help.upsertTranscripcioMark(m)
}
func (d *SQLite) DeleteTranscripcioMark(transcripcioID, userID int) error {
	return d.help.deleteTranscripcioMark(transcripcioID, userID)
}
func (d *SQLite) ListTranscripcioMarks(transcripcioIDs []int) ([]TranscripcioRawMark, error) {
	return d.help.listTranscripcioMarks(transcripcioIDs)
}
func (d *SQLite) SearchPersones(f PersonaSearchFilter) ([]PersonaSearchResult, error) {
	return d.help.searchPersones(f)
}
func (d *SQLite) ListRegistresByPersona(personaID int, tipus string) ([]PersonaRegistreRow, error) {
	return d.help.listRegistresByPersona(personaID, tipus)
}

// Punts i activitat
func (d *SQLite) ListPointsRules() ([]PointsRule, error) { return d.help.listPointsRules() }
func (d *SQLite) GetPointsRule(id int) (*PointsRule, error) {
	return d.help.getPointsRule(id)
}
func (d *SQLite) GetPointsRuleByCode(code string) (*PointsRule, error) {
	return d.help.getPointsRuleByCode(code)
}
func (d *SQLite) SavePointsRule(r *PointsRule) (int, error)     { return d.help.savePointsRule(r) }
func (d *SQLite) GetUserActivity(id int) (*UserActivity, error) { return d.help.getUserActivity(id) }
func (d *SQLite) InsertUserActivity(a *UserActivity) (int, error) {
	return d.help.insertUserActivity(a)
}
func (d *SQLite) UpdateUserActivityStatus(id int, status string, moderatedBy *int) error {
	return d.help.updateUserActivityStatus(id, status, moderatedBy)
}
func (d *SQLite) ListUserActivityByUser(userID int, f ActivityFilter) ([]UserActivity, error) {
	return d.help.listUserActivityByUser(userID, f)
}
func (d *SQLite) ListActivityByObject(objectType string, objectID int, status string) ([]UserActivity, error) {
	return d.help.listActivityByObject(objectType, objectID, status)
}
func (d *SQLite) AddPointsToUser(userID int, delta int) error {
	return d.help.addPointsToUser(userID, delta)
}
func (d *SQLite) GetUserPoints(userID int) (*UserPoints, error) { return d.help.getUserPoints(userID) }
func (d *SQLite) RecalcUserPoints() error                       { return d.help.recalcUserPoints() }
func (d *SQLite) GetRanking(f RankingFilter) ([]UserPoints, error) {
	return d.help.getRanking(f)
}
func (d *SQLite) CountRanking(f RankingFilter) (int, error) { return d.help.countRanking(f) }

// Cognoms
func (d *SQLite) ListCognoms(q string, limit, offset int) ([]Cognom, error) {
	return d.help.listCognoms(q, limit, offset)
}
func (d *SQLite) GetCognom(id int) (*Cognom, error) { return d.help.getCognom(id) }
func (d *SQLite) UpsertCognom(forma, key, origen, notes string, createdBy *int) (int, error) {
	return d.help.upsertCognom(forma, key, origen, notes, createdBy)
}
func (d *SQLite) ListCognomVariants(f CognomVariantFilter) ([]CognomVariant, error) {
	return d.help.listCognomVariants(f)
}
func (d *SQLite) ResolveCognomPublicatByForma(forma string) (int, string, bool, error) {
	return d.help.resolveCognomPublicatByForma(forma)
}
func (d *SQLite) ListCognomFormesPublicades(cognomID int) ([]string, error) {
	return d.help.listCognomFormesPublicades(cognomID)
}
func (d *SQLite) CreateCognomVariant(v *CognomVariant) (int, error) {
	return d.help.createCognomVariant(v)
}
func (d *SQLite) UpdateCognomVariantModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateCognomVariantModeracio(id, estat, motiu, moderatorID)
}
func (d *SQLite) UpsertCognomFreqMunicipiAny(cognomID, municipiID, anyDoc, freq int) error {
	return d.help.upsertCognomFreqMunicipiAny(cognomID, municipiID, anyDoc, freq)
}
func (d *SQLite) QueryCognomHeatmap(cognomID int, anyStart, anyEnd int) ([]CognomFreqRow, error) {
	return d.help.queryCognomHeatmap(cognomID, anyStart, anyEnd)
}

func (d *SQLite) ListCognomImportRows(limit, offset int) ([]CognomImportRow, error) {
	return d.help.listCognomImportRows(limit, offset)
}

func (d *SQLite) ListCognomStatsRows(limit, offset int) ([]CognomStatsRow, error) {
	return d.help.listCognomStatsRows(limit, offset)
}
