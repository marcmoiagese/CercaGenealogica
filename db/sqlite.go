package db

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

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

// Missatgeria interna
func (d *SQLite) GetOrCreateDMThread(userA, userB int) (*DMThread, error) {
	return d.help.getOrCreateDMThread(userA, userB)
}

func (d *SQLite) GetDMThreadByUsers(userA, userB int) (*DMThread, error) {
	return d.help.getDMThreadByUsers(userA, userB)
}

func (d *SQLite) GetDMThreadByID(threadID int) (*DMThread, error) {
	return d.help.getDMThreadByID(threadID)
}

func (d *SQLite) ListDMThreadsForUser(userID int, f DMThreadListFilter) ([]DMThreadListItem, error) {
	return d.help.listDMThreadsForUser(userID, f)
}

func (d *SQLite) CountDMUnread(userID int) (int, error) {
	return d.help.countDMUnread(userID)
}

func (d *SQLite) ListDMThreadFolders(userID int) ([]string, error) {
	return d.help.listDMThreadFolders(userID)
}

func (d *SQLite) SetDMThreadFolder(threadID, userID int, folder string) error {
	return d.help.setDMThreadFolder(threadID, userID, folder)
}

func (d *SQLite) ListDMMessages(threadID, limit, beforeID int) ([]DMMessage, error) {
	return d.help.listDMMessages(threadID, limit, beforeID)
}

func (d *SQLite) CreateDMMessage(threadID, senderID int, body string) (int, error) {
	return d.help.createDMMessage(threadID, senderID, body)
}

func (d *SQLite) UpdateDMThreadLastMessage(threadID, msgID int, at time.Time) error {
	return d.help.updateDMThreadLastMessage(threadID, msgID, at)
}

func (d *SQLite) MarkDMThreadRead(threadID, userID, lastMsgID int) error {
	return d.help.markDMThreadRead(threadID, userID, lastMsgID)
}

func (d *SQLite) SetDMThreadArchived(threadID, userID int, archived bool) error {
	return d.help.setDMThreadArchived(threadID, userID, archived)
}

func (d *SQLite) SoftDeleteDMThread(threadID, userID int) error {
	return d.help.softDeleteDMThread(threadID, userID)
}

func (d *SQLite) AddUserBlock(blockerID, blockedID int) error {
	return d.help.addUserBlock(blockerID, blockedID)
}

func (d *SQLite) RemoveUserBlock(blockerID, blockedID int) error {
	return d.help.removeUserBlock(blockerID, blockedID)
}

func (d *SQLite) IsUserBlocked(blockerID, blockedID int) (bool, error) {
	return d.help.isUserBlocked(blockerID, blockedID)
}

func (d *SQLite) UpdateUserProfile(u *User) error {
	return d.help.updateUserProfile(u)
}

func (d *SQLite) UpdateUserEmail(userID int, newEmail string) error {
	return d.help.updateUserEmail(userID, newEmail)
}

func (d *SQLite) ListDashboardWidgets(userID int) ([]DashboardWidgetConfig, error) {
	return d.help.listDashboardWidgets(userID)
}

func (d *SQLite) SaveDashboardWidgets(userID int, widgets []DashboardWidgetConfig) error {
	return d.help.saveDashboardWidgets(userID, widgets)
}

func (d *SQLite) ClearDashboardWidgets(userID int) error {
	return d.help.clearDashboardWidgets(userID)
}
func (d *SQLite) ListPlatformSettings() ([]PlatformSetting, error) {
	return d.help.listPlatformSettings()
}
func (d *SQLite) UpsertPlatformSetting(key, value string, updatedBy int) error {
	return d.help.upsertPlatformSetting(key, value, updatedBy)
}
func (d *SQLite) ListMaintenanceWindows() ([]MaintenanceWindow, error) {
	return d.help.listMaintenanceWindows()
}
func (d *SQLite) GetMaintenanceWindow(id int) (*MaintenanceWindow, error) {
	return d.help.getMaintenanceWindow(id)
}
func (d *SQLite) SaveMaintenanceWindow(w *MaintenanceWindow) (int, error) {
	return d.help.saveMaintenanceWindow(w)
}
func (d *SQLite) DeleteMaintenanceWindow(id int) error {
	return d.help.deleteMaintenanceWindow(id)
}
func (d *SQLite) GetActiveMaintenanceWindow(now time.Time) (*MaintenanceWindow, error) {
	return d.help.getActiveMaintenanceWindow(now)
}
func (d *SQLite) GetAdminKPIsGeneral() (*AdminKPIsGeneral, error) {
	return d.help.getAdminKPIsGeneral()
}
func (d *SQLite) CountUsersSince(since time.Time) (int, error) {
	return d.help.countUsersSince(since)
}
func (d *SQLite) ListTransparencySettings() ([]TransparencySetting, error) {
	return d.help.listTransparencySettings()
}
func (d *SQLite) UpsertTransparencySetting(key, value string, updatedBy int) error {
	return d.help.upsertTransparencySetting(key, value, updatedBy)
}
func (d *SQLite) ListTransparencyContributors(includePrivate bool) ([]TransparencyContributor, error) {
	return d.help.listTransparencyContributors(includePrivate)
}
func (d *SQLite) GetTransparencyContributor(id int) (*TransparencyContributor, error) {
	return d.help.getTransparencyContributor(id)
}
func (d *SQLite) SaveTransparencyContributor(c *TransparencyContributor) (int, error) {
	return d.help.saveTransparencyContributor(c)
}
func (d *SQLite) DeleteTransparencyContributor(id int) error {
	return d.help.deleteTransparencyContributor(id)
}
func (d *SQLite) InsertAdminImportRun(importType, status string, createdBy int) error {
	return d.help.insertAdminImportRun(importType, status, createdBy)
}
func (d *SQLite) CountAdminImportRunsSince(since time.Time) (AdminImportRunSummary, error) {
	return d.help.countAdminImportRunsSince(since)
}
func (d *SQLite) CreateAdminJob(job *AdminJob) (int, error) {
	return d.help.createAdminJob(job)
}
func (d *SQLite) UpdateAdminJobProgress(id int, progressDone, progressTotal int) error {
	return d.help.updateAdminJobProgress(id, progressDone, progressTotal)
}
func (d *SQLite) UpdateAdminJobStatus(id int, status, errorText, resultJSON string, finishedAt *time.Time) error {
	return d.help.updateAdminJobStatus(id, status, errorText, resultJSON, finishedAt)
}
func (d *SQLite) GetAdminJob(id int) (*AdminJob, error) {
	return d.help.getAdminJob(id)
}
func (d *SQLite) ListAdminJobs(filter AdminJobFilter) ([]AdminJob, error) {
	return d.help.listAdminJobs(filter)
}
func (d *SQLite) CountAdminJobs(filter AdminJobFilter) (int, error) {
	return d.help.countAdminJobs(filter)
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
func (d *SQLite) EnsureDefaultAchievements() error {
	return d.help.ensureDefaultAchievements()
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
func (d *SQLite) ListPoliticaGrants(politicaID int) ([]PoliticaGrant, error) {
	return d.help.listPoliticaGrants(politicaID)
}
func (d *SQLite) SavePoliticaGrant(g *PoliticaGrant) (int, error) {
	return d.help.savePoliticaGrant(g)
}
func (d *SQLite) DeletePoliticaGrant(id int) error {
	return d.help.deletePoliticaGrant(id)
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
func (d *SQLite) GetUserPermissionsVersion(userID int) (int, error) {
	return d.help.getUserPermissionsVersion(userID)
}
func (d *SQLite) BumpUserPermissionsVersion(userID int) error {
	return d.help.bumpUserPermissionsVersion(userID)
}

func (d *SQLite) BumpGroupPermissionsVersion(groupID int) error {
	return d.help.bumpGroupPermissionsVersion(groupID)
}

func (d *SQLite) BumpPolicyPermissionsVersion(politicaID int) error {
	return d.help.bumpPolicyPermissionsVersion(politicaID)
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
func (d *SQLite) ListPersonaFieldLinks(personaID int) ([]PersonaFieldLink, error) {
	return d.help.listPersonaFieldLinks(personaID)
}
func (d *SQLite) UpsertPersonaFieldLink(personaID int, fieldKey string, registreID int, userID int) error {
	return d.help.upsertPersonaFieldLink(personaID, fieldKey, registreID, userID)
}
func (d *SQLite) ListPersonaAnecdotes(personaID int, userID int) ([]PersonaAnecdote, error) {
	return d.help.listPersonaAnecdotes(personaID, userID)
}
func (d *SQLite) CreatePersonaAnecdote(a *PersonaAnecdote) (int, error) {
	return d.help.createPersonaAnecdote(a)
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

func (d *SQLite) CountNivells(f NivellAdminFilter) (int, error) {
	return d.help.countNivells(f)
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

func (d *SQLite) CountMunicipis(f MunicipiFilter) (int, error) {
	return d.help.countMunicipis(f)
}
func (d *SQLite) ListMunicipisBrowse(f MunicipiBrowseFilter) ([]MunicipiBrowseRow, error) {
	return d.help.listMunicipisBrowse(f)
}
func (d *SQLite) CountMunicipisBrowse(f MunicipiBrowseFilter) (int, error) {
	return d.help.countMunicipisBrowse(f)
}
func (d *SQLite) SuggestMunicipis(f MunicipiBrowseFilter) ([]MunicipiSuggestRow, error) {
	return d.help.suggestMunicipis(f)
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

func (d *SQLite) CountArquebisbats(f ArquebisbatFilter) (int, error) {
	return d.help.countArquebisbats(f)
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

func (d *SQLite) CountArxius(f ArxiuFilter) (int, error) {
	return d.help.countArxius(f)
}
func (d *SQLite) CountPaisos() (int, error) {
	return d.help.countPaisos()
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
func (d *SQLite) InsertArxiuDonacioClick(arxiuID int, userID *int) error {
	return d.help.insertArxiuDonacioClick(arxiuID, userID)
}
func (d *SQLite) CountArxiuDonacioClicks(arxiuID int) (int, error) {
	return d.help.countArxiuDonacioClicks(arxiuID)
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

func (d *SQLite) CountLlibres(f LlibreFilter) (int, error) {
	return d.help.countLlibres(f)
}
func (d *SQLite) CountIndexedRegistres(status string) (int, error) {
	return d.help.countIndexedRegistres(status)
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
func (d *SQLite) SearchLlibrePagines(llibreID int, query string, limit int) ([]LlibrePagina, error) {
	return d.help.searchLlibrePagines(llibreID, query, limit)
}
func (d *SQLite) GetLlibrePaginaByID(id int) (*LlibrePagina, error) {
	return d.help.getLlibrePaginaByID(id)
}
func (d *SQLite) GetLlibrePaginaByNum(llibreID, num int) (*LlibrePagina, error) {
	return d.help.getLlibrePaginaByNum(llibreID, num)
}
func (d *SQLite) SaveLlibrePagina(p *LlibrePagina) (int, error) {
	return d.help.saveLlibrePagina(p)
}
func (d *SQLite) RecalcLlibrePagines(llibreID, total int) error {
	return d.help.recalcLlibrePagines(llibreID, total)
}

// Media
func (d *SQLite) ListMediaAlbumsByOwner(userID int) ([]MediaAlbum, error) {
	return d.help.listMediaAlbumsByOwner(userID)
}
func (d *SQLite) ListMediaAlbumsByLlibre(llibreID int) ([]MediaAlbum, error) {
	return d.help.listMediaAlbumsByLlibre(llibreID)
}
func (d *SQLite) GetMediaAlbumByID(id int) (*MediaAlbum, error) {
	return d.help.getMediaAlbumByID(id)
}
func (d *SQLite) GetMediaAlbumByPublicID(publicID string) (*MediaAlbum, error) {
	return d.help.getMediaAlbumByPublicID(publicID)
}
func (d *SQLite) CreateMediaAlbum(a *MediaAlbum) (int, error) {
	return d.help.createMediaAlbum(a)
}
func (d *SQLite) ListMediaItemsByAlbum(albumID int) ([]MediaItem, error) {
	return d.help.listMediaItemsByAlbum(albumID)
}
func (d *SQLite) ListMediaItemsByAlbumType(albumType, status string) ([]MediaItem, error) {
	return d.help.listMediaItemsByAlbumType(albumType, status)
}
func (d *SQLite) GetMediaItemByID(id int) (*MediaItem, error) {
	return d.help.getMediaItemByID(id)
}
func (d *SQLite) GetMediaItemByPublicID(publicID string) (*MediaItem, error) {
	return d.help.getMediaItemByPublicID(publicID)
}
func (d *SQLite) CreateMediaItem(item *MediaItem) (int, error) {
	return d.help.createMediaItem(item)
}
func (d *SQLite) UpdateMediaItemDerivativesStatus(itemID int, status string) error {
	return d.help.updateMediaItemDerivativesStatus(itemID, status)
}
func (d *SQLite) ListMediaAlbumsByStatus(status string) ([]MediaAlbum, error) {
	return d.help.listMediaAlbumsByStatus(status)
}
func (d *SQLite) ListMediaItemsByStatus(status string) ([]MediaItem, error) {
	return d.help.listMediaItemsByStatus(status)
}
func (d *SQLite) UpdateMediaAlbumModeration(id int, status, visibility string, restrictedGroupID, accessPolicyID, creditCost, difficultyScore int, sourceType, notes string, moderatorID int) error {
	return d.help.updateMediaAlbumModeration(id, status, visibility, restrictedGroupID, accessPolicyID, creditCost, difficultyScore, sourceType, notes, moderatorID)
}
func (d *SQLite) UpdateMediaItemModeration(id int, status string, creditCost int, notes string, moderatorID int) error {
	return d.help.updateMediaItemModeration(id, status, creditCost, notes, moderatorID)
}
func (d *SQLite) GetUserCreditsBalance(userID int) (int, error) {
	return d.help.getUserCreditsBalance(userID)
}
func (d *SQLite) InsertUserCreditsLedger(entry *UserCreditsLedgerEntry) (int, error) {
	return d.help.insertUserCreditsLedger(entry)
}
func (d *SQLite) GetActiveMediaAccessGrant(userID, mediaItemID int) (*MediaAccessGrant, error) {
	return d.help.getActiveMediaAccessGrant(userID, mediaItemID)
}
func (d *SQLite) GetMediaAccessGrantByToken(token string) (*MediaAccessGrant, error) {
	return d.help.getMediaAccessGrantByToken(token)
}
func (d *SQLite) CreateMediaAccessGrant(grant *MediaAccessGrant) (int, error) {
	return d.help.createMediaAccessGrant(grant)
}
func (d *SQLite) InsertMediaAccessLog(entry *MediaAccessLog) (int, error) {
	return d.help.insertMediaAccessLog(entry)
}
func (d *SQLite) ListMediaItemLinksByPagina(paginaID int) ([]MediaItemPageLink, error) {
	return d.help.listMediaItemLinksByPagina(paginaID)
}
func (d *SQLite) ListMediaItemLinksByAlbum(albumID int) ([]MediaItemPageLink, error) {
	return d.help.listMediaItemLinksByAlbum(albumID)
}
func (d *SQLite) UpsertMediaItemPageLink(mediaItemID, llibreID, paginaID, pageOrder int, notes string) error {
	return d.help.upsertMediaItemPageLink(mediaItemID, llibreID, paginaID, pageOrder, notes)
}
func (d *SQLite) DeleteMediaItemPageLink(mediaItemID, paginaID int) error {
	return d.help.deleteMediaItemPageLink(mediaItemID, paginaID)
}
func (d *SQLite) CountMediaItemLinksByAlbum(albumID int) (map[int]int, error) {
	return d.help.countMediaItemLinksByAlbum(albumID)
}
func (d *SQLite) SearchMediaItems(query string, limit int) ([]MediaItemSearchRow, error) {
	return d.help.searchMediaItems(query, limit)
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
func (d *SQLite) CountTranscripcionsRawByPageValue(llibreID int, pageValue string) (int, error) {
	return d.help.countTranscripcionsRawByPageValue(llibreID, pageValue)
}

func (d *SQLite) ListTranscripcionsRawByPageValue(llibreID int, pageValue string) ([]TranscripcioRaw, error) {
	return d.help.listTranscripcionsRawByPageValue(llibreID, pageValue)
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
func (d *SQLite) UpdateTranscripcioModeracioWithDemografia(id int, estat, motiu string, moderatorID int, municipiID, year int, tipus string, delta int) error {
	return d.help.updateTranscripcioModeracioWithDemografia(id, estat, motiu, moderatorID, municipiID, year, tipus, delta)
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
func (d *SQLite) GetTranscripcioRawChange(id int) (*TranscripcioRawChange, error) {
	return d.help.getTranscripcioRawChange(id)
}
func (d *SQLite) ListTranscripcioRawChangesPending() ([]TranscripcioRawChange, error) {
	return d.help.listTranscripcioRawChangesPending()
}
func (d *SQLite) UpdateTranscripcioRawChangeModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateTranscripcioRawChangeModeracio(id, estat, motiu, moderatorID)
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
func (d *SQLite) GetWikiMark(objectType string, objectID int, userID int) (*WikiMark, error) {
	return d.help.getWikiMark(objectType, objectID, userID)
}
func (d *SQLite) UpsertWikiMark(m *WikiMark) error {
	return d.help.upsertWikiMark(m)
}
func (d *SQLite) DeleteWikiMark(objectType string, objectID int, userID int) error {
	return d.help.deleteWikiMark(objectType, objectID, userID)
}
func (d *SQLite) ListWikiMarks(objectType string, objectIDs []int) ([]WikiMark, error) {
	return d.help.listWikiMarks(objectType, objectIDs)
}
func (d *SQLite) IncWikiPublicCount(objectType string, objectID int, tipus string, delta int) error {
	return d.help.incWikiPublicCount(objectType, objectID, tipus, delta)
}
func (d *SQLite) GetWikiPublicCounts(objectType string, objectID int) (map[string]int, error) {
	return d.help.getWikiPublicCounts(objectType, objectID)
}
func (d *SQLite) CreateWikiChange(c *WikiChange) (int, error) {
	return d.help.createWikiChange(c)
}
func (d *SQLite) GetWikiChange(id int) (*WikiChange, error) {
	return d.help.getWikiChange(id)
}
func (d *SQLite) ListWikiChanges(objectType string, objectID int) ([]WikiChange, error) {
	return d.help.listWikiChanges(objectType, objectID)
}
func (d *SQLite) ListWikiChangesPending(objectType string, limit int) ([]WikiChange, error) {
	return d.help.listWikiChangesPending(objectType, limit)
}
func (d *SQLite) UpdateWikiChangeModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateWikiChangeModeracio(id, estat, motiu, moderatorID)
}
func (d *SQLite) EnqueueWikiPending(change *WikiChange) error {
	return d.help.enqueueWikiPending(change)
}
func (d *SQLite) DequeueWikiPending(changeID int) error {
	return d.help.dequeueWikiPending(changeID)
}
func (d *SQLite) ListWikiPending(limit int) ([]WikiPendingItem, error) {
	return d.help.listWikiPending(limit)
}
func (d *SQLite) CreateCSVImportTemplate(t *CSVImportTemplate) (int, error) {
	return d.help.createCSVImportTemplate(t)
}
func (d *SQLite) UpdateCSVImportTemplate(t *CSVImportTemplate) error {
	return d.help.updateCSVImportTemplate(t)
}
func (d *SQLite) GetCSVImportTemplate(id int) (*CSVImportTemplate, error) {
	return d.help.getCSVImportTemplate(id)
}
func (d *SQLite) ListCSVImportTemplates(filter CSVImportTemplateFilter) ([]CSVImportTemplate, error) {
	return d.help.listCSVImportTemplates(filter)
}
func (d *SQLite) DeleteCSVImportTemplate(id int) error {
	return d.help.deleteCSVImportTemplate(id)
}
func (d *SQLite) SearchPersones(f PersonaSearchFilter) ([]PersonaSearchResult, error) {
	return d.help.searchPersones(f)
}
func (d *SQLite) ListRegistresByPersona(personaID int, tipus string) ([]PersonaRegistreRow, error) {
	return d.help.listRegistresByPersona(personaID, tipus)
}
func (d *SQLite) GetPersonesByIDs(ids []int) (map[int]*Persona, error) {
	return d.help.getPersonesByIDs(ids)
}
func (d *SQLite) FindBestBaptismeTranscripcioForPersona(personaID int) (int, bool, error) {
	return d.help.findBestBaptismeTranscripcioForPersona(personaID)
}
func (d *SQLite) GetParentsFromTranscripcio(transcripcioID int) (int, int, error) {
	return d.help.getParentsFromTranscripcio(transcripcioID)
}

// Punts i activitat
func (d *SQLite) ListPointsRules() ([]PointsRule, error) { return d.help.listPointsRules() }
func (d *SQLite) GetPointsRule(id int) (*PointsRule, error) {
	return d.help.getPointsRule(id)
}
func (d *SQLite) GetPointsRuleByCode(code string) (*PointsRule, error) {
	return d.help.getPointsRuleByCode(code)
}
func (d *SQLite) SavePointsRule(r *PointsRule) (int, error) { return d.help.savePointsRule(r) }
func (d *SQLite) ListUserIDs(limit, offset int) ([]int, error) {
	return d.help.listUserIDs(limit, offset)
}
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

// Achievements
func (d *SQLite) ListAchievements() ([]Achievement, error) {
	return d.help.listAchievements()
}
func (d *SQLite) ListEnabledAchievements() ([]Achievement, error) {
	return d.help.listEnabledAchievements()
}
func (d *SQLite) GetAchievement(id int) (*Achievement, error) { return d.help.getAchievement(id) }
func (d *SQLite) GetAchievementByCode(code string) (*Achievement, error) {
	return d.help.getAchievementByCode(code)
}
func (d *SQLite) SaveAchievement(a *Achievement) (int, error) {
	return d.help.saveAchievement(a)
}
func (d *SQLite) AwardAchievement(userID, achievementID int, status, metaJSON string) (bool, error) {
	return d.help.awardAchievement(userID, achievementID, status, metaJSON)
}
func (d *SQLite) ListUserAchievements(userID int) ([]AchievementUserView, error) {
	return d.help.listUserAchievements(userID)
}
func (d *SQLite) ListUserShowcase(userID int) ([]AchievementShowcaseView, error) {
	return d.help.listUserShowcase(userID)
}
func (d *SQLite) SetUserShowcaseSlot(userID, achievementID, slot int) error {
	return d.help.setUserShowcaseSlot(userID, achievementID, slot)
}
func (d *SQLite) ClearUserShowcaseSlot(userID, slot int) error {
	return d.help.clearUserShowcaseSlot(userID, slot)
}
func (d *SQLite) IsAchievementEventActive(code string, at time.Time) (bool, error) {
	return d.help.isAchievementEventActive(code, at)
}
func (d *SQLite) CountUserActivities(f AchievementActivityFilter) (int, error) {
	return d.help.countUserActivities(f)
}
func (d *SQLite) CountUserActivitiesDistinctObject(f AchievementActivityFilter) (int, error) {
	return d.help.countUserActivitiesDistinctObject(f)
}
func (d *SQLite) SumUserActivityPoints(f AchievementActivityFilter) (int, error) {
	return d.help.sumUserActivityPoints(f)
}
func (d *SQLite) ListUserActivityDays(f AchievementActivityFilter) ([]time.Time, error) {
	return d.help.listUserActivityDays(f)
}

// Cognoms
func (d *SQLite) ListCognoms(q string, limit, offset int) ([]Cognom, error) {
	return d.help.listCognoms(q, limit, offset)
}
func (d *SQLite) GetCognom(id int) (*Cognom, error) { return d.help.getCognom(id) }
func (d *SQLite) FindCognomIDByKey(key string) (int, error) {
	return d.help.findCognomIDByKey(key)
}
func (d *SQLite) UpsertCognom(forma, key, origen, notes string, createdBy *int) (int, error) {
	return d.help.upsertCognom(forma, key, origen, notes, createdBy)
}
func (d *SQLite) UpdateCognom(c *Cognom) error {
	return d.help.updateCognom(c)
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

func (d *SQLite) GetCognomRedirect(fromID int) (*CognomRedirect, error) {
	return d.help.getCognomRedirect(fromID)
}

func (d *SQLite) ListCognomRedirects() ([]CognomRedirect, error) {
	return d.help.listCognomRedirects()
}

func (d *SQLite) ListCognomRedirectsByTo(toID int) ([]CognomRedirect, error) {
	return d.help.listCognomRedirectsByTo(toID)
}

func (d *SQLite) SetCognomRedirect(fromID, toID int, createdBy *int, reason string) error {
	return d.help.setCognomRedirect(fromID, toID, createdBy, reason)
}

func (d *SQLite) DeleteCognomRedirect(fromID int) error {
	return d.help.deleteCognomRedirect(fromID)
}

func (d *SQLite) CreateCognomRedirectSuggestion(s *CognomRedirectSuggestion) (int, error) {
	return d.help.createCognomRedirectSuggestion(s)
}

func (d *SQLite) GetCognomRedirectSuggestion(id int) (*CognomRedirectSuggestion, error) {
	return d.help.getCognomRedirectSuggestion(id)
}

func (d *SQLite) ListCognomRedirectSuggestions(f CognomRedirectSuggestionFilter) ([]CognomRedirectSuggestion, error) {
	return d.help.listCognomRedirectSuggestions(f)
}

func (d *SQLite) UpdateCognomRedirectSuggestionModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateCognomRedirectSuggestionModeracio(id, estat, motiu, moderatorID)
}

func (d *SQLite) CreateCognomReferencia(ref *CognomReferencia) (int, error) {
	return d.help.createCognomReferencia(ref)
}

func (d *SQLite) ListCognomReferencies(f CognomReferenciaFilter) ([]CognomReferencia, error) {
	return d.help.listCognomReferencies(f)
}

func (d *SQLite) UpdateCognomReferenciaModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateCognomReferenciaModeracio(id, estat, motiu, moderatorID)
}

// Cercador avançat
func (d *SQLite) UpsertSearchDoc(doc *SearchDoc) error {
	return d.help.upsertSearchDoc(doc)
}
func (d *SQLite) GetSearchDoc(entityType string, entityID int) (*SearchDoc, error) {
	return d.help.getSearchDoc(entityType, entityID)
}
func (d *SQLite) DeleteSearchDoc(entityType string, entityID int) error {
	return d.help.deleteSearchDoc(entityType, entityID)
}
func (d *SQLite) SearchDocs(filter SearchQueryFilter) ([]SearchDocRow, int, SearchFacets, error) {
	return d.help.searchDocs(filter)
}
func (d *SQLite) ReplaceAdminClosure(descendantMunicipiID int, entries []AdminClosureEntry) error {
	return d.help.replaceAdminClosure(descendantMunicipiID, entries)
}
func (d *SQLite) ListAdminClosure(descendantMunicipiID int) ([]AdminClosureEntry, error) {
	return d.help.listAdminClosure(descendantMunicipiID)
}
func (d *SQLite) UpsertCognomFreqMunicipiAny(cognomID, municipiID, anyDoc, freq int) error {
	return d.help.upsertCognomFreqMunicipiAny(cognomID, municipiID, anyDoc, freq)
}
func (d *SQLite) ApplyCognomFreqMunicipiAnyDelta(cognomID, municipiID, anyDoc, delta int) error {
	return d.help.applyCognomFreqMunicipiAnyDelta(cognomID, municipiID, anyDoc, delta)
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

func (d *SQLite) RebuildCognomStats(cognomID int) error {
	return d.help.rebuildCognomStats(cognomID)
}

func (d *SQLite) GetCognomStatsTotal(cognomID int) (*CognomStatsTotal, error) {
	return d.help.getCognomStatsTotal(cognomID)
}

func (d *SQLite) ListCognomStatsAny(cognomID int, from, to int) ([]CognomStatsAnyRow, error) {
	return d.help.listCognomStatsAny(cognomID, from, to)
}

func (d *SQLite) ListCognomStatsAnyDecade(cognomID int, from, to int) ([]CognomStatsAnyRow, error) {
	return d.help.listCognomStatsAnyDecade(cognomID, from, to)
}

func (d *SQLite) ListCognomStatsAncestor(cognomID int, ancestorType string, level, any, limit int) ([]CognomStatsAncestorRow, error) {
	return d.help.listCognomStatsAncestor(cognomID, ancestorType, level, any, limit)
}

func (d *SQLite) CountCognomStatsAncestorDistinct(cognomID int, ancestorType string, level, any int) (int, error) {
	return d.help.countCognomStatsAncestorDistinct(cognomID, ancestorType, level, any)
}

// Noms
func (d *SQLite) UpsertNom(forma, key, notes string, createdBy *int) (int, error) {
	return d.help.upsertNom(forma, key, notes, createdBy)
}
func (d *SQLite) GetNom(id int) (*Nom, error) { return d.help.getNom(id) }
func (d *SQLite) ResolveNomByForma(forma string) (int, string, bool, error) {
	return d.help.resolveNomByForma(forma)
}
func (d *SQLite) UpsertNomFreqMunicipiAny(nomID, municipiID, anyDoc, delta int) error {
	return d.help.upsertNomFreqMunicipiAny(nomID, municipiID, anyDoc, delta)
}
func (d *SQLite) UpsertNomFreqMunicipiTotal(nomID, municipiID, delta int) error {
	return d.help.upsertNomFreqMunicipiTotal(nomID, municipiID, delta)
}
func (d *SQLite) UpsertCognomFreqMunicipiTotal(cognomID, municipiID, delta int) error {
	return d.help.upsertCognomFreqMunicipiTotal(cognomID, municipiID, delta)
}
func (d *SQLite) ListTopNomsByMunicipi(municipiID, limit int) ([]NomTotalRow, error) {
	return d.help.listTopNomsByMunicipi(municipiID, limit)
}
func (d *SQLite) ListTopCognomsByMunicipi(municipiID, limit int) ([]CognomTotalRow, error) {
	return d.help.listTopCognomsByMunicipi(municipiID, limit)
}
func (d *SQLite) ListNomSeries(municipiID, nomID int, bucket string) ([]NomFreqRow, error) {
	return d.help.listNomSeries(municipiID, nomID, bucket)
}
func (d *SQLite) ListCognomSeries(municipiID, cognomID int, bucket string) ([]CognomFreqRow, error) {
	return d.help.listCognomSeries(municipiID, cognomID, bucket)
}
func (d *SQLite) CountNomTotalsByMunicipi(municipiID int) (int, error) {
	return d.help.countNomTotalsByMunicipi(municipiID)
}
func (d *SQLite) CountCognomTotalsByMunicipi(municipiID int) (int, error) {
	return d.help.countCognomTotalsByMunicipi(municipiID)
}
func (d *SQLite) ClearNomCognomStatsByMunicipi(municipiID int) error {
	return d.help.clearNomCognomStatsByMunicipi(municipiID)
}
func (d *SQLite) UpsertNomFreqNivellAny(nomID, nivellID, anyDoc, delta int) error {
	return d.help.upsertNomFreqNivellAny(nomID, nivellID, anyDoc, delta)
}
func (d *SQLite) UpsertNomFreqNivellTotal(nomID, nivellID, delta int) error {
	return d.help.upsertNomFreqNivellTotal(nomID, nivellID, delta)
}
func (d *SQLite) ApplyCognomFreqNivellAnyDelta(cognomID, nivellID, anyDoc, delta int) error {
	return d.help.upsertCognomFreqNivellAny(cognomID, nivellID, anyDoc, delta)
}
func (d *SQLite) UpsertCognomFreqNivellTotal(cognomID, nivellID, delta int) error {
	return d.help.upsertCognomFreqNivellTotal(cognomID, nivellID, delta)
}
func (d *SQLite) ListTopNomsByNivell(nivellID, limit int) ([]NomTotalRow, error) {
	return d.help.listTopNomsByNivell(nivellID, limit)
}
func (d *SQLite) ListTopCognomsByNivell(nivellID, limit int) ([]CognomTotalRow, error) {
	return d.help.listTopCognomsByNivell(nivellID, limit)
}
func (d *SQLite) ListNomSeriesByNivell(nivellID, nomID int, bucket string) ([]NomFreqRow, error) {
	return d.help.listNomSeriesByNivell(nivellID, nomID, bucket)
}
func (d *SQLite) ListCognomSeriesByNivell(nivellID, cognomID int, bucket string) ([]CognomFreqRow, error) {
	return d.help.listCognomSeriesByNivell(nivellID, cognomID, bucket)
}
func (d *SQLite) ClearNomCognomStatsByNivell(nivellID int) error {
	return d.help.clearNomCognomStatsByNivell(nivellID)
}
func (d *SQLite) RebuildNivellNomCognomStats(nivellID int) error {
	return d.help.rebuildNivellNomCognomStats(nivellID)
}

func (d *SQLite) ListMunicipiMapes(filter MunicipiMapaFilter) ([]MunicipiMapa, error) {
	return d.help.listMunicipiMapes(filter)
}
func (d *SQLite) GetMunicipiMapa(id int) (*MunicipiMapa, error) {
	return d.help.getMunicipiMapa(id)
}
func (d *SQLite) CreateMunicipiMapa(m *MunicipiMapa) (int, error) {
	return d.help.createMunicipiMapa(m)
}
func (d *SQLite) UpdateMunicipiMapa(m *MunicipiMapa) error {
	return d.help.updateMunicipiMapa(m)
}
func (d *SQLite) UpdateMunicipiMapaCurrentVersion(mapaID, versionID int) error {
	return d.help.updateMunicipiMapaCurrentVersion(mapaID, versionID)
}
func (d *SQLite) NextMunicipiMapaVersionNumber(mapaID int) (int, error) {
	return d.help.nextMunicipiMapaVersionNumber(mapaID)
}
func (d *SQLite) ListMunicipiMapaVersions(filter MunicipiMapaVersionFilter) ([]MunicipiMapaVersion, error) {
	return d.help.listMunicipiMapaVersions(filter)
}
func (d *SQLite) GetMunicipiMapaVersion(id int) (*MunicipiMapaVersion, error) {
	return d.help.getMunicipiMapaVersion(id)
}
func (d *SQLite) CreateMunicipiMapaVersion(v *MunicipiMapaVersion) (int, error) {
	return d.help.createMunicipiMapaVersion(v)
}
func (d *SQLite) SaveMunicipiMapaDraft(versionID int, jsonData, changelog string, expectedLock int) (int, error) {
	return d.help.saveMunicipiMapaDraft(versionID, jsonData, changelog, expectedLock)
}
func (d *SQLite) UpdateMunicipiMapaVersionStatus(id int, status, notes string, moderatorID int) error {
	return d.help.updateMunicipiMapaVersionStatus(id, status, notes, moderatorID)
}
func (d *SQLite) ResolveMunicipiIDByMapaID(mapaID int) (int, error) {
	return d.help.resolveMunicipiIDByMapaID(mapaID)
}
func (d *SQLite) ResolveMunicipiIDByMapaVersionID(versionID int) (int, error) {
	return d.help.resolveMunicipiIDByMapaVersionID(versionID)
}

func (d *SQLite) EnsureMunicipiHistoria(municipiID int) (*MunicipiHistoria, error) {
	return d.help.ensureMunicipiHistoria(municipiID)
}
func (d *SQLite) GetMunicipiHistoriaByMunicipiID(municipiID int) (*MunicipiHistoria, error) {
	return d.help.getMunicipiHistoriaByMunicipiID(municipiID)
}
func (d *SQLite) ResolveMunicipiIDByHistoriaGeneralVersionID(versionID int) (int, error) {
	return d.help.resolveMunicipiIDByHistoriaGeneralVersionID(versionID)
}
func (d *SQLite) ResolveMunicipiIDByHistoriaFetVersionID(versionID int) (int, error) {
	return d.help.resolveMunicipiIDByHistoriaFetVersionID(versionID)
}
func (d *SQLite) NextMunicipiHistoriaGeneralVersion(historiaID int) (int, error) {
	return d.help.nextMunicipiHistoriaGeneralVersion(historiaID)
}
func (d *SQLite) CreateMunicipiHistoriaGeneralDraft(historiaID int, createdBy int, baseFromCurrent bool) (int, error) {
	return d.help.createMunicipiHistoriaGeneralDraft(historiaID, createdBy, baseFromCurrent)
}
func (d *SQLite) GetMunicipiHistoriaGeneralVersion(id int) (*MunicipiHistoriaGeneralVersion, error) {
	return d.help.getMunicipiHistoriaGeneralVersion(id)
}
func (d *SQLite) UpdateMunicipiHistoriaGeneralDraft(v *MunicipiHistoriaGeneralVersion) error {
	return d.help.updateMunicipiHistoriaGeneralDraft(v)
}
func (d *SQLite) SetMunicipiHistoriaGeneralStatus(versionID int, status, notes string, moderatorID *int) error {
	return d.help.setMunicipiHistoriaGeneralStatus(versionID, status, notes, moderatorID)
}
func (d *SQLite) GetMunicipiHistoriaFet(id int) (*MunicipiHistoriaFet, error) {
	return d.help.getMunicipiHistoriaFet(id)
}
func (d *SQLite) CreateMunicipiHistoriaFet(municipiID int, createdBy int) (int, error) {
	return d.help.createMunicipiHistoriaFet(municipiID, createdBy)
}
func (d *SQLite) NextMunicipiHistoriaFetVersion(fetID int) (int, error) {
	return d.help.nextMunicipiHistoriaFetVersion(fetID)
}
func (d *SQLite) CreateMunicipiHistoriaFetDraft(fetID int, createdBy int, baseFromCurrent bool) (int, error) {
	return d.help.createMunicipiHistoriaFetDraft(fetID, createdBy, baseFromCurrent)
}
func (d *SQLite) GetMunicipiHistoriaFetVersion(id int) (*MunicipiHistoriaFetVersion, error) {
	return d.help.getMunicipiHistoriaFetVersion(id)
}
func (d *SQLite) UpdateMunicipiHistoriaFetDraft(v *MunicipiHistoriaFetVersion) error {
	return d.help.updateMunicipiHistoriaFetDraft(v)
}
func (d *SQLite) SetMunicipiHistoriaFetStatus(versionID int, status, notes string, moderatorID *int) error {
	return d.help.setMunicipiHistoriaFetStatus(versionID, status, notes, moderatorID)
}
func (d *SQLite) GetMunicipiHistoriaSummary(municipiID int) (*MunicipiHistoriaGeneralVersion, []MunicipiHistoriaFetVersion, error) {
	return d.help.getMunicipiHistoriaSummary(municipiID)
}
func (d *SQLite) ListMunicipiHistoriaTimeline(municipiID int, status string, limit, offset int, q string, anyFrom, anyTo *int) ([]MunicipiHistoriaFetVersion, int, error) {
	return d.help.listMunicipiHistoriaTimeline(municipiID, status, limit, offset, q, anyFrom, anyTo)
}
func (d *SQLite) ListPendingMunicipiHistoriaGeneralVersions(limit, offset int) ([]MunicipiHistoriaGeneralVersion, int, error) {
	return d.help.listPendingMunicipiHistoriaGeneralVersions(limit, offset)
}
func (d *SQLite) ListPendingMunicipiHistoriaFetVersions(limit, offset int) ([]MunicipiHistoriaFetVersion, int, error) {
	return d.help.listPendingMunicipiHistoriaFetVersions(limit, offset)
}

func (d *SQLite) GetMunicipiDemografiaMeta(municipiID int) (*MunicipiDemografiaMeta, error) {
	return d.help.getMunicipiDemografiaMeta(municipiID)
}
func (d *SQLite) ListMunicipiDemografiaAny(municipiID int, from, to int) ([]MunicipiDemografiaAny, error) {
	return d.help.listMunicipiDemografiaAny(municipiID, from, to)
}
func (d *SQLite) ListMunicipiDemografiaDecades(municipiID int, from, to int) ([]MunicipiDemografiaAny, error) {
	return d.help.listMunicipiDemografiaDecades(municipiID, from, to)
}
func (d *SQLite) ApplyMunicipiDemografiaDelta(municipiID, year int, tipus string, delta int) error {
	return d.help.applyMunicipiDemografiaDelta(municipiID, year, tipus, delta)
}
func (d *SQLite) ApplyMunicipiDemografiaDeltaTx(tx *sql.Tx, municipiID, year int, tipus string, delta int) error {
	return d.help.applyMunicipiDemografiaDeltaTx(tx, municipiID, year, tipus, delta)
}
func (d *SQLite) RebuildMunicipiDemografia(municipiID int) error {
	return d.help.rebuildMunicipiDemografia(municipiID)
}
func (d *SQLite) GetNivellDemografiaMeta(nivellID int) (*NivellDemografiaMeta, error) {
	return d.help.getNivellDemografiaMeta(nivellID)
}
func (d *SQLite) ListNivellDemografiaAny(nivellID int, from, to int) ([]NivellDemografiaAny, error) {
	return d.help.listNivellDemografiaAny(nivellID, from, to)
}
func (d *SQLite) ListNivellDemografiaDecades(nivellID int, from, to int) ([]NivellDemografiaAny, error) {
	return d.help.listNivellDemografiaDecades(nivellID, from, to)
}
func (d *SQLite) ApplyNivellDemografiaDelta(nivellID, year int, tipus string, delta int) error {
	return d.help.applyNivellDemografiaDelta(nivellID, year, tipus, delta)
}
func (d *SQLite) RebuildNivellDemografia(nivellID int) error {
	return d.help.rebuildNivellDemografia(nivellID)
}

func (d *SQLite) ListMunicipiAnecdotariPublished(municipiID int, f MunicipiAnecdotariFilter) ([]MunicipiAnecdotariVersion, int, error) {
	return d.help.listMunicipiAnecdotariPublished(municipiID, f)
}
func (d *SQLite) GetMunicipiAnecdotariPublished(itemID int) (*MunicipiAnecdotariVersion, error) {
	return d.help.getMunicipiAnecdotariPublished(itemID)
}
func (d *SQLite) ListMunicipiAnecdotariComments(itemID int, limit, offset int) ([]MunicipiAnecdotariComment, int, error) {
	return d.help.listMunicipiAnecdotariComments(itemID, limit, offset)
}
func (d *SQLite) CreateMunicipiAnecdotariItem(municipiID int, createdBy int) (int, error) {
	return d.help.createMunicipiAnecdotariItem(municipiID, createdBy)
}
func (d *SQLite) CreateMunicipiAnecdotariDraft(itemID int, createdBy int, baseFromCurrent bool) (int, error) {
	return d.help.createMunicipiAnecdotariDraft(itemID, createdBy, baseFromCurrent)
}
func (d *SQLite) GetMunicipiAnecdotariVersion(id int) (*MunicipiAnecdotariVersion, error) {
	return d.help.getMunicipiAnecdotariVersion(id)
}
func (d *SQLite) GetPendingMunicipiAnecdotariVersionByItemID(itemID int) (*MunicipiAnecdotariVersion, error) {
	return d.help.getPendingMunicipiAnecdotariVersionByItemID(itemID)
}
func (d *SQLite) UpdateMunicipiAnecdotariDraft(v *MunicipiAnecdotariVersion) error {
	return d.help.updateMunicipiAnecdotariDraft(v)
}
func (d *SQLite) SubmitMunicipiAnecdotariVersion(versionID int) error {
	return d.help.submitMunicipiAnecdotariVersion(versionID)
}
func (d *SQLite) ListPendingMunicipiAnecdotariVersions(limit, offset int) ([]MunicipiAnecdotariVersion, int, error) {
	return d.help.listPendingMunicipiAnecdotariVersions(limit, offset)
}
func (d *SQLite) ApproveMunicipiAnecdotariVersion(versionID int, moderatorID int) error {
	return d.help.approveMunicipiAnecdotariVersion(versionID, moderatorID)
}
func (d *SQLite) RejectMunicipiAnecdotariVersion(versionID int, moderatorID int, notes string) error {
	return d.help.rejectMunicipiAnecdotariVersion(versionID, moderatorID, notes)
}
func (d *SQLite) CreateMunicipiAnecdotariComment(itemID int, userID int, body string) (int, error) {
	return d.help.createMunicipiAnecdotariComment(itemID, userID, body)
}
func (d *SQLite) GetMunicipiAnecdotariLastCommentAt(userID int) (time.Time, error) {
	return d.help.getMunicipiAnecdotariLastCommentAt(userID)
}
func (d *SQLite) ResolveMunicipiIDByAnecdotariItemID(itemID int) (int, error) {
	return d.help.resolveMunicipiIDByAnecdotariItemID(itemID)
}
func (d *SQLite) ResolveMunicipiIDByAnecdotariVersionID(versionID int) (int, error) {
	return d.help.resolveMunicipiIDByAnecdotariVersionID(versionID)
}

// Esdeveniments historics
func (d *SQLite) CreateEventHistoric(e *EventHistoric) (int, error) {
	return d.help.createEventHistoric(e)
}

func (d *SQLite) GetEventHistoric(id int) (*EventHistoric, error) {
	return d.help.getEventHistoric(id)
}

func (d *SQLite) GetEventHistoricBySlug(slug string) (*EventHistoric, error) {
	return d.help.getEventHistoricBySlug(slug)
}

func (d *SQLite) UpdateEventHistoric(e *EventHistoric) error {
	return d.help.updateEventHistoric(e)
}

func (d *SQLite) ListEventsHistoric(filter EventHistoricFilter) ([]EventHistoric, error) {
	return d.help.listEventsHistoric(filter)
}

func (d *SQLite) UpdateEventHistoricModeracio(id int, estat, notes string, moderatorID int) error {
	return d.help.updateEventHistoricModeracio(id, estat, notes, moderatorID)
}

func (d *SQLite) ListEventImpacts(eventID int) ([]EventHistoricImpact, error) {
	return d.help.listEventImpacts(eventID)
}

func (d *SQLite) ReplaceEventImpacts(eventID int, impacts []EventHistoricImpact) error {
	return d.help.replaceEventImpacts(eventID, impacts)
}

func (d *SQLite) ListEventsByScope(scopeType string, scopeID int, filter EventHistoricFilter) ([]EventHistoric, error) {
	return d.help.listEventsByScope(scopeType, scopeID, filter)
}
