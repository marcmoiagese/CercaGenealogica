package db

import (
	"database/sql"
	"fmt"
	"time"

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

// Missatgeria interna
func (d *MySQL) GetOrCreateDMThread(userA, userB int) (*DMThread, error) {
	return d.help.getOrCreateDMThread(userA, userB)
}

func (d *MySQL) GetDMThreadByUsers(userA, userB int) (*DMThread, error) {
	return d.help.getDMThreadByUsers(userA, userB)
}

func (d *MySQL) GetDMThreadByID(threadID int) (*DMThread, error) {
	return d.help.getDMThreadByID(threadID)
}

func (d *MySQL) ListDMThreadsForUser(userID int, f DMThreadListFilter) ([]DMThreadListItem, error) {
	return d.help.listDMThreadsForUser(userID, f)
}

func (d *MySQL) CountDMUnread(userID int) (int, error) {
	return d.help.countDMUnread(userID)
}

func (d *MySQL) ListDMThreadFolders(userID int) ([]string, error) {
	return d.help.listDMThreadFolders(userID)
}

func (d *MySQL) SetDMThreadFolder(threadID, userID int, folder string) error {
	return d.help.setDMThreadFolder(threadID, userID, folder)
}

func (d *MySQL) ListDMMessages(threadID, limit, beforeID int) ([]DMMessage, error) {
	return d.help.listDMMessages(threadID, limit, beforeID)
}

func (d *MySQL) CreateDMMessage(threadID, senderID int, body string) (int, error) {
	return d.help.createDMMessage(threadID, senderID, body)
}

func (d *MySQL) UpdateDMThreadLastMessage(threadID, msgID int, at time.Time) error {
	return d.help.updateDMThreadLastMessage(threadID, msgID, at)
}

func (d *MySQL) MarkDMThreadRead(threadID, userID, lastMsgID int) error {
	return d.help.markDMThreadRead(threadID, userID, lastMsgID)
}

func (d *MySQL) SetDMThreadArchived(threadID, userID int, archived bool) error {
	return d.help.setDMThreadArchived(threadID, userID, archived)
}

func (d *MySQL) SoftDeleteDMThread(threadID, userID int) error {
	return d.help.softDeleteDMThread(threadID, userID)
}

func (d *MySQL) AddUserBlock(blockerID, blockedID int) error {
	return d.help.addUserBlock(blockerID, blockedID)
}

func (d *MySQL) RemoveUserBlock(blockerID, blockedID int) error {
	return d.help.removeUserBlock(blockerID, blockedID)
}

func (d *MySQL) IsUserBlocked(blockerID, blockedID int) (bool, error) {
	return d.help.isUserBlocked(blockerID, blockedID)
}

func (d *MySQL) UpdateUserProfile(u *User) error {
	return d.help.updateUserProfile(u)
}

func (d *MySQL) UpdateUserEmail(userID int, newEmail string) error {
	return d.help.updateUserEmail(userID, newEmail)
}

func (d *MySQL) ListDashboardWidgets(userID int) ([]DashboardWidgetConfig, error) {
	return d.help.listDashboardWidgets(userID)
}

func (d *MySQL) SaveDashboardWidgets(userID int, widgets []DashboardWidgetConfig) error {
	return d.help.saveDashboardWidgets(userID, widgets)
}

func (d *MySQL) ClearDashboardWidgets(userID int) error {
	return d.help.clearDashboardWidgets(userID)
}
func (d *MySQL) ListPlatformSettings() ([]PlatformSetting, error) {
	return d.help.listPlatformSettings()
}
func (d *MySQL) UpsertPlatformSetting(key, value string, updatedBy int) error {
	return d.help.upsertPlatformSetting(key, value, updatedBy)
}
func (d *MySQL) ListMaintenanceWindows() ([]MaintenanceWindow, error) {
	return d.help.listMaintenanceWindows()
}
func (d *MySQL) GetMaintenanceWindow(id int) (*MaintenanceWindow, error) {
	return d.help.getMaintenanceWindow(id)
}
func (d *MySQL) SaveMaintenanceWindow(w *MaintenanceWindow) (int, error) {
	return d.help.saveMaintenanceWindow(w)
}
func (d *MySQL) DeleteMaintenanceWindow(id int) error {
	return d.help.deleteMaintenanceWindow(id)
}
func (d *MySQL) GetActiveMaintenanceWindow(now time.Time) (*MaintenanceWindow, error) {
	return d.help.getActiveMaintenanceWindow(now)
}
func (d *MySQL) GetAdminKPIsGeneral() (*AdminKPIsGeneral, error) {
	return d.help.getAdminKPIsGeneral()
}
func (d *MySQL) CountUsersSince(since time.Time) (int, error) {
	return d.help.countUsersSince(since)
}
func (d *MySQL) ListTransparencySettings() ([]TransparencySetting, error) {
	return d.help.listTransparencySettings()
}
func (d *MySQL) UpsertTransparencySetting(key, value string, updatedBy int) error {
	return d.help.upsertTransparencySetting(key, value, updatedBy)
}
func (d *MySQL) ListTransparencyContributors(includePrivate bool) ([]TransparencyContributor, error) {
	return d.help.listTransparencyContributors(includePrivate)
}
func (d *MySQL) GetTransparencyContributor(id int) (*TransparencyContributor, error) {
	return d.help.getTransparencyContributor(id)
}
func (d *MySQL) SaveTransparencyContributor(c *TransparencyContributor) (int, error) {
	return d.help.saveTransparencyContributor(c)
}
func (d *MySQL) DeleteTransparencyContributor(id int) error {
	return d.help.deleteTransparencyContributor(id)
}
func (d *MySQL) InsertAdminImportRun(importType, status string, createdBy int) error {
	return d.help.insertAdminImportRun(importType, status, createdBy)
}
func (d *MySQL) CountAdminImportRunsSince(since time.Time) (AdminImportRunSummary, error) {
	return d.help.countAdminImportRunsSince(since)
}
func (d *MySQL) CreateAdminJob(job *AdminJob) (int, error) {
	return d.help.createAdminJob(job)
}
func (d *MySQL) UpdateAdminJobProgress(id int, progressDone, progressTotal int) error {
	return d.help.updateAdminJobProgress(id, progressDone, progressTotal)
}
func (d *MySQL) UpdateAdminJobStatus(id int, status, errorText, resultJSON string, finishedAt *time.Time) error {
	return d.help.updateAdminJobStatus(id, status, errorText, resultJSON, finishedAt)
}
func (d *MySQL) GetAdminJob(id int) (*AdminJob, error) {
	return d.help.getAdminJob(id)
}
func (d *MySQL) ListAdminJobs(filter AdminJobFilter) ([]AdminJob, error) {
	return d.help.listAdminJobs(filter)
}
func (d *MySQL) CountAdminJobs(filter AdminJobFilter) (int, error) {
	return d.help.countAdminJobs(filter)
}

func (d *MySQL) ListUsersAdmin() ([]UserAdminRow, error) {
	return d.help.listUsersAdmin()
}

func (d *MySQL) ListUsersAdminFiltered(filter UserAdminFilter) ([]UserAdminRow, error) {
	return d.help.listUsersAdminFiltered(filter)
}

func (d *MySQL) CountUsersAdmin(filter UserAdminFilter) (int, error) {
	return d.help.countUsersAdmin(filter)
}

func (d *MySQL) SetUserActive(userID int, active bool) error {
	return d.help.setUserActive(userID, active)
}

func (d *MySQL) SetUserBanned(userID int, banned bool) error {
	return d.help.setUserBanned(userID, banned)
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
func (d *MySQL) EnsureDefaultAchievements() error {
	return d.help.ensureDefaultAchievements()
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
func (d *MySQL) ListPoliticaGrants(politicaID int) ([]PoliticaGrant, error) {
	return d.help.listPoliticaGrants(politicaID)
}
func (d *MySQL) SavePoliticaGrant(g *PoliticaGrant) (int, error) {
	return d.help.savePoliticaGrant(g)
}
func (d *MySQL) DeletePoliticaGrant(id int) error {
	return d.help.deletePoliticaGrant(id)
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
func (d *MySQL) GetUserPermissionsVersion(userID int) (int, error) {
	return d.help.getUserPermissionsVersion(userID)
}
func (d *MySQL) BumpUserPermissionsVersion(userID int) error {
	return d.help.bumpUserPermissionsVersion(userID)
}

func (d *MySQL) BumpGroupPermissionsVersion(groupID int) error {
	return d.help.bumpGroupPermissionsVersion(groupID)
}

func (d *MySQL) BumpPolicyPermissionsVersion(politicaID int) error {
	return d.help.bumpPolicyPermissionsVersion(politicaID)
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
func (d *MySQL) ListPersonaFieldLinks(personaID int) ([]PersonaFieldLink, error) {
	return d.help.listPersonaFieldLinks(personaID)
}
func (d *MySQL) UpsertPersonaFieldLink(personaID int, fieldKey string, registreID int, userID int) error {
	return d.help.upsertPersonaFieldLink(personaID, fieldKey, registreID, userID)
}
func (d *MySQL) ListPersonaAnecdotes(personaID int, userID int) ([]PersonaAnecdote, error) {
	return d.help.listPersonaAnecdotes(personaID, userID)
}
func (d *MySQL) CreatePersonaAnecdote(a *PersonaAnecdote) (int, error) {
	return d.help.createPersonaAnecdote(a)
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

func (d *MySQL) CountNivells(f NivellAdminFilter) (int, error) {
	return d.help.countNivells(f)
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

func (d *MySQL) CountMunicipis(f MunicipiFilter) (int, error) {
	return d.help.countMunicipis(f)
}
func (d *MySQL) ListMunicipisBrowse(f MunicipiBrowseFilter) ([]MunicipiBrowseRow, error) {
	return d.help.listMunicipisBrowse(f)
}
func (d *MySQL) CountMunicipisBrowse(f MunicipiBrowseFilter) (int, error) {
	return d.help.countMunicipisBrowse(f)
}
func (d *MySQL) SuggestMunicipis(f MunicipiBrowseFilter) ([]MunicipiSuggestRow, error) {
	return d.help.suggestMunicipis(f)
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

func (d *MySQL) CountArquebisbats(f ArquebisbatFilter) (int, error) {
	return d.help.countArquebisbats(f)
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

func (d *MySQL) CountArxius(f ArxiuFilter) (int, error) {
	return d.help.countArxius(f)
}
func (d *MySQL) CountPaisos() (int, error) {
	return d.help.countPaisos()
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
func (d *MySQL) InsertArxiuDonacioClick(arxiuID int, userID *int) error {
	return d.help.insertArxiuDonacioClick(arxiuID, userID)
}
func (d *MySQL) CountArxiuDonacioClicks(arxiuID int) (int, error) {
	return d.help.countArxiuDonacioClicks(arxiuID)
}
func (d *MySQL) ListArxiuLlibres(arxiuID int) ([]ArxiuLlibreDetail, error) {
	return d.help.listArxiuLlibres(arxiuID)
}
func (d *MySQL) ListLlibreArxius(llibreID int) ([]ArxiuLlibreDetail, error) {
	return d.help.listLlibreArxius(llibreID)
}

func (d *MySQL) ListLlibreURLs(llibreID int) ([]LlibreURL, error) {
	return d.help.listLlibreURLs(llibreID)
}

func (d *MySQL) AddLlibreURL(link *LlibreURL) error {
	return d.help.addLlibreURL(link)
}

func (d *MySQL) DeleteLlibreURL(id int) error {
	return d.help.deleteLlibreURL(id)
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

func (d *MySQL) CountLlibres(f LlibreFilter) (int, error) {
	return d.help.countLlibres(f)
}
func (d *MySQL) CountIndexedRegistres(status string) (int, error) {
	return d.help.countIndexedRegistres(status)
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
func (d *MySQL) HasLlibreDuplicate(municipiID int, tipus, cronologia, codiDigital, codiFisic string, excludeID int) (bool, error) {
	return d.help.hasLlibreDuplicate(municipiID, tipus, cronologia, codiDigital, codiFisic, excludeID)
}
func (d *MySQL) GetLlibresIndexacioStats(ids []int) (map[int]LlibreIndexacioStats, error) {
	return d.help.getLlibresIndexacioStats(ids)
}
func (d *MySQL) UpsertLlibreIndexacioStats(stats *LlibreIndexacioStats) error {
	return d.help.upsertLlibreIndexacioStats(stats)
}
func (d *MySQL) UpdateLlibreModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateLlibreModeracio(id, estat, motiu, moderatorID)
}
func (d *MySQL) ListLlibrePagines(llibreID int) ([]LlibrePagina, error) {
	return d.help.listLlibrePagines(llibreID)
}
func (d *MySQL) SearchLlibrePagines(llibreID int, query string, limit int) ([]LlibrePagina, error) {
	return d.help.searchLlibrePagines(llibreID, query, limit)
}
func (d *MySQL) GetLlibrePaginaByID(id int) (*LlibrePagina, error) {
	return d.help.getLlibrePaginaByID(id)
}
func (d *MySQL) GetLlibrePaginaByNum(llibreID, num int) (*LlibrePagina, error) {
	return d.help.getLlibrePaginaByNum(llibreID, num)
}
func (d *MySQL) SaveLlibrePagina(p *LlibrePagina) (int, error) {
	return d.help.saveLlibrePagina(p)
}
func (d *MySQL) RecalcLlibrePagines(llibreID, total int) error {
	return d.help.recalcLlibrePagines(llibreID, total)
}

// Media
func (d *MySQL) ListMediaAlbumsByOwner(userID int) ([]MediaAlbum, error) {
	return d.help.listMediaAlbumsByOwner(userID)
}
func (d *MySQL) ListMediaAlbumsByLlibre(llibreID int) ([]MediaAlbum, error) {
	return d.help.listMediaAlbumsByLlibre(llibreID)
}
func (d *MySQL) GetMediaAlbumByID(id int) (*MediaAlbum, error) {
	return d.help.getMediaAlbumByID(id)
}
func (d *MySQL) GetMediaAlbumByPublicID(publicID string) (*MediaAlbum, error) {
	return d.help.getMediaAlbumByPublicID(publicID)
}
func (d *MySQL) CreateMediaAlbum(a *MediaAlbum) (int, error) {
	return d.help.createMediaAlbum(a)
}
func (d *MySQL) ListMediaItemsByAlbum(albumID int) ([]MediaItem, error) {
	return d.help.listMediaItemsByAlbum(albumID)
}
func (d *MySQL) ListMediaItemsByAlbumType(albumType, status string) ([]MediaItem, error) {
	return d.help.listMediaItemsByAlbumType(albumType, status)
}
func (d *MySQL) GetMediaItemByID(id int) (*MediaItem, error) {
	return d.help.getMediaItemByID(id)
}
func (d *MySQL) GetMediaItemByPublicID(publicID string) (*MediaItem, error) {
	return d.help.getMediaItemByPublicID(publicID)
}
func (d *MySQL) CreateMediaItem(item *MediaItem) (int, error) {
	return d.help.createMediaItem(item)
}
func (d *MySQL) UpdateMediaItemDerivativesStatus(itemID int, status string) error {
	return d.help.updateMediaItemDerivativesStatus(itemID, status)
}
func (d *MySQL) ListMediaAlbumsByStatus(status string) ([]MediaAlbum, error) {
	return d.help.listMediaAlbumsByStatus(status)
}
func (d *MySQL) ListMediaItemsByStatus(status string) ([]MediaItem, error) {
	return d.help.listMediaItemsByStatus(status)
}
func (d *MySQL) UpdateMediaAlbumModeration(id int, status, visibility string, restrictedGroupID, accessPolicyID, creditCost, difficultyScore int, sourceType, notes string, moderatorID int) error {
	return d.help.updateMediaAlbumModeration(id, status, visibility, restrictedGroupID, accessPolicyID, creditCost, difficultyScore, sourceType, notes, moderatorID)
}
func (d *MySQL) UpdateMediaItemModeration(id int, status string, creditCost int, notes string, moderatorID int) error {
	return d.help.updateMediaItemModeration(id, status, creditCost, notes, moderatorID)
}
func (d *MySQL) GetUserCreditsBalance(userID int) (int, error) {
	return d.help.getUserCreditsBalance(userID)
}
func (d *MySQL) InsertUserCreditsLedger(entry *UserCreditsLedgerEntry) (int, error) {
	return d.help.insertUserCreditsLedger(entry)
}
func (d *MySQL) GetActiveMediaAccessGrant(userID, mediaItemID int) (*MediaAccessGrant, error) {
	return d.help.getActiveMediaAccessGrant(userID, mediaItemID)
}
func (d *MySQL) GetMediaAccessGrantByToken(token string) (*MediaAccessGrant, error) {
	return d.help.getMediaAccessGrantByToken(token)
}
func (d *MySQL) CreateMediaAccessGrant(grant *MediaAccessGrant) (int, error) {
	return d.help.createMediaAccessGrant(grant)
}
func (d *MySQL) InsertMediaAccessLog(entry *MediaAccessLog) (int, error) {
	return d.help.insertMediaAccessLog(entry)
}
func (d *MySQL) ListMediaItemLinksByPagina(paginaID int) ([]MediaItemPageLink, error) {
	return d.help.listMediaItemLinksByPagina(paginaID)
}
func (d *MySQL) ListMediaItemLinksByAlbum(albumID int) ([]MediaItemPageLink, error) {
	return d.help.listMediaItemLinksByAlbum(albumID)
}
func (d *MySQL) UpsertMediaItemPageLink(mediaItemID, llibreID, paginaID, pageOrder int, notes string) error {
	return d.help.upsertMediaItemPageLink(mediaItemID, llibreID, paginaID, pageOrder, notes)
}
func (d *MySQL) DeleteMediaItemPageLink(mediaItemID, paginaID int) error {
	return d.help.deleteMediaItemPageLink(mediaItemID, paginaID)
}
func (d *MySQL) CountMediaItemLinksByAlbum(albumID int) (map[int]int, error) {
	return d.help.countMediaItemLinksByAlbum(albumID)
}
func (d *MySQL) SearchMediaItems(query string, limit int) ([]MediaItemSearchRow, error) {
	return d.help.searchMediaItems(query, limit)
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
func (d *MySQL) CountTranscripcionsRawByPageValue(llibreID int, pageValue string) (int, error) {
	return d.help.countTranscripcionsRawByPageValue(llibreID, pageValue)
}

func (d *MySQL) ListTranscripcionsRawByPageValue(llibreID int, pageValue string) ([]TranscripcioRaw, error) {
	return d.help.listTranscripcionsRawByPageValue(llibreID, pageValue)
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
func (d *MySQL) UpdateTranscripcioModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateTranscripcioModeracio(id, estat, motiu, moderatorID)
}
func (d *MySQL) UpdateTranscripcioModeracioWithDemografia(id int, estat, motiu string, moderatorID int, municipiID, year int, tipus string, delta int) error {
	return d.help.updateTranscripcioModeracioWithDemografia(id, estat, motiu, moderatorID, municipiID, year, tipus, delta)
}
func (d *MySQL) DeleteTranscripcioRaw(id int) error {
	return d.help.deleteTranscripcioRaw(id)
}
func (d *MySQL) ListTranscripcionsRawPageStats(llibreID int) ([]TranscripcioRawPageStat, error) {
	return d.help.listTranscripcionsRawPageStats(llibreID)
}
func (d *MySQL) UpdateTranscripcionsRawPageStat(stat *TranscripcioRawPageStat) error {
	return d.help.updateTranscripcionsRawPageStat(stat)
}
func (d *MySQL) RecalcTranscripcionsRawPageStats(llibreID int) error {
	return d.help.recalcTranscripcionsRawPageStats(llibreID)
}
func (d *MySQL) SetTranscripcionsRawPageStatsIndexacio(llibreID int, value int) error {
	return d.help.setTranscripcionsRawPageStatsIndexacio(llibreID, value)
}

func (d *MySQL) DeleteTranscripcionsByLlibre(llibreID int) error {
	return d.help.deleteTranscripcionsByLlibre(llibreID)
}
func (d *MySQL) CreateTranscripcioRawChange(c *TranscripcioRawChange) (int, error) {
	return d.help.createTranscripcioRawChange(c)
}
func (d *MySQL) ListTranscripcioRawChanges(transcripcioID int) ([]TranscripcioRawChange, error) {
	return d.help.listTranscripcioRawChanges(transcripcioID)
}
func (d *MySQL) GetTranscripcioRawChange(id int) (*TranscripcioRawChange, error) {
	return d.help.getTranscripcioRawChange(id)
}
func (d *MySQL) ListTranscripcioRawChangesPending() ([]TranscripcioRawChange, error) {
	return d.help.listTranscripcioRawChangesPending()
}
func (d *MySQL) UpdateTranscripcioRawChangeModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateTranscripcioRawChangeModeracio(id, estat, motiu, moderatorID)
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
func (d *MySQL) UpsertTranscripcioMark(m *TranscripcioRawMark) error {
	return d.help.upsertTranscripcioMark(m)
}
func (d *MySQL) DeleteTranscripcioMark(transcripcioID, userID int) error {
	return d.help.deleteTranscripcioMark(transcripcioID, userID)
}
func (d *MySQL) ListTranscripcioMarks(transcripcioIDs []int) ([]TranscripcioRawMark, error) {
	return d.help.listTranscripcioMarks(transcripcioIDs)
}
func (d *MySQL) GetWikiMark(objectType string, objectID int, userID int) (*WikiMark, error) {
	return d.help.getWikiMark(objectType, objectID, userID)
}
func (d *MySQL) UpsertWikiMark(m *WikiMark) error {
	return d.help.upsertWikiMark(m)
}
func (d *MySQL) DeleteWikiMark(objectType string, objectID int, userID int) error {
	return d.help.deleteWikiMark(objectType, objectID, userID)
}
func (d *MySQL) ListWikiMarks(objectType string, objectIDs []int) ([]WikiMark, error) {
	return d.help.listWikiMarks(objectType, objectIDs)
}
func (d *MySQL) IncWikiPublicCount(objectType string, objectID int, tipus string, delta int) error {
	return d.help.incWikiPublicCount(objectType, objectID, tipus, delta)
}
func (d *MySQL) GetWikiPublicCounts(objectType string, objectID int) (map[string]int, error) {
	return d.help.getWikiPublicCounts(objectType, objectID)
}
func (d *MySQL) CreateWikiChange(c *WikiChange) (int, error) {
	return d.help.createWikiChange(c)
}
func (d *MySQL) GetWikiChange(id int) (*WikiChange, error) {
	return d.help.getWikiChange(id)
}
func (d *MySQL) ListWikiChanges(objectType string, objectID int) ([]WikiChange, error) {
	return d.help.listWikiChanges(objectType, objectID)
}
func (d *MySQL) ListWikiChangesPending(objectType string, limit int) ([]WikiChange, error) {
	return d.help.listWikiChangesPending(objectType, limit)
}
func (d *MySQL) UpdateWikiChangeModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateWikiChangeModeracio(id, estat, motiu, moderatorID)
}
func (d *MySQL) EnqueueWikiPending(change *WikiChange) error {
	return d.help.enqueueWikiPending(change)
}
func (d *MySQL) DequeueWikiPending(changeID int) error {
	return d.help.dequeueWikiPending(changeID)
}
func (d *MySQL) ListWikiPending(limit int) ([]WikiPendingItem, error) {
	return d.help.listWikiPending(limit)
}
func (d *MySQL) CreateCSVImportTemplate(t *CSVImportTemplate) (int, error) {
	return d.help.createCSVImportTemplate(t)
}
func (d *MySQL) UpdateCSVImportTemplate(t *CSVImportTemplate) error {
	return d.help.updateCSVImportTemplate(t)
}
func (d *MySQL) GetCSVImportTemplate(id int) (*CSVImportTemplate, error) {
	return d.help.getCSVImportTemplate(id)
}
func (d *MySQL) ListCSVImportTemplates(filter CSVImportTemplateFilter) ([]CSVImportTemplate, error) {
	return d.help.listCSVImportTemplates(filter)
}
func (d *MySQL) DeleteCSVImportTemplate(id int) error {
	return d.help.deleteCSVImportTemplate(id)
}
func (d *MySQL) SearchPersones(f PersonaSearchFilter) ([]PersonaSearchResult, error) {
	return d.help.searchPersones(f)
}
func (d *MySQL) ListRegistresByPersona(personaID int, tipus string) ([]PersonaRegistreRow, error) {
	return d.help.listRegistresByPersona(personaID, tipus)
}
func (d *MySQL) GetPersonesByIDs(ids []int) (map[int]*Persona, error) {
	return d.help.getPersonesByIDs(ids)
}
func (d *MySQL) FindBestBaptismeTranscripcioForPersona(personaID int) (int, bool, error) {
	return d.help.findBestBaptismeTranscripcioForPersona(personaID)
}
func (d *MySQL) GetParentsFromTranscripcio(transcripcioID int) (int, int, error) {
	return d.help.getParentsFromTranscripcio(transcripcioID)
}

// Punts i activitat
func (d *MySQL) ListPointsRules() ([]PointsRule, error) { return d.help.listPointsRules() }
func (d *MySQL) GetPointsRule(id int) (*PointsRule, error) {
	return d.help.getPointsRule(id)
}
func (d *MySQL) GetPointsRuleByCode(code string) (*PointsRule, error) {
	return d.help.getPointsRuleByCode(code)
}
func (d *MySQL) SavePointsRule(r *PointsRule) (int, error) { return d.help.savePointsRule(r) }
func (d *MySQL) ListUserIDs(limit, offset int) ([]int, error) {
	return d.help.listUserIDs(limit, offset)
}
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

// Achievements
func (d *MySQL) ListAchievements() ([]Achievement, error) {
	return d.help.listAchievements()
}
func (d *MySQL) ListEnabledAchievements() ([]Achievement, error) {
	return d.help.listEnabledAchievements()
}
func (d *MySQL) GetAchievement(id int) (*Achievement, error) { return d.help.getAchievement(id) }
func (d *MySQL) GetAchievementByCode(code string) (*Achievement, error) {
	return d.help.getAchievementByCode(code)
}
func (d *MySQL) SaveAchievement(a *Achievement) (int, error) {
	return d.help.saveAchievement(a)
}
func (d *MySQL) AwardAchievement(userID, achievementID int, status, metaJSON string) (bool, error) {
	return d.help.awardAchievement(userID, achievementID, status, metaJSON)
}
func (d *MySQL) ListUserAchievements(userID int) ([]AchievementUserView, error) {
	return d.help.listUserAchievements(userID)
}
func (d *MySQL) ListUserShowcase(userID int) ([]AchievementShowcaseView, error) {
	return d.help.listUserShowcase(userID)
}
func (d *MySQL) SetUserShowcaseSlot(userID, achievementID, slot int) error {
	return d.help.setUserShowcaseSlot(userID, achievementID, slot)
}
func (d *MySQL) ClearUserShowcaseSlot(userID, slot int) error {
	return d.help.clearUserShowcaseSlot(userID, slot)
}
func (d *MySQL) IsAchievementEventActive(code string, at time.Time) (bool, error) {
	return d.help.isAchievementEventActive(code, at)
}
func (d *MySQL) CountUserActivities(f AchievementActivityFilter) (int, error) {
	return d.help.countUserActivities(f)
}
func (d *MySQL) CountUserActivitiesDistinctObject(f AchievementActivityFilter) (int, error) {
	return d.help.countUserActivitiesDistinctObject(f)
}
func (d *MySQL) SumUserActivityPoints(f AchievementActivityFilter) (int, error) {
	return d.help.sumUserActivityPoints(f)
}
func (d *MySQL) ListUserActivityDays(f AchievementActivityFilter) ([]time.Time, error) {
	return d.help.listUserActivityDays(f)
}

// Cognoms
func (d *MySQL) ListCognoms(q string, limit, offset int) ([]Cognom, error) {
	return d.help.listCognoms(q, limit, offset)
}
func (d *MySQL) GetCognom(id int) (*Cognom, error) { return d.help.getCognom(id) }
func (d *MySQL) FindCognomIDByKey(key string) (int, error) {
	return d.help.findCognomIDByKey(key)
}
func (d *MySQL) UpsertCognom(forma, key, origen, notes string, createdBy *int) (int, error) {
	return d.help.upsertCognom(forma, key, origen, notes, createdBy)
}
func (d *MySQL) UpdateCognom(c *Cognom) error {
	return d.help.updateCognom(c)
}
func (d *MySQL) ListCognomVariants(f CognomVariantFilter) ([]CognomVariant, error) {
	return d.help.listCognomVariants(f)
}
func (d *MySQL) ResolveCognomPublicatByForma(forma string) (int, string, bool, error) {
	return d.help.resolveCognomPublicatByForma(forma)
}
func (d *MySQL) ListCognomFormesPublicades(cognomID int) ([]string, error) {
	return d.help.listCognomFormesPublicades(cognomID)
}
func (d *MySQL) CreateCognomVariant(v *CognomVariant) (int, error) {
	return d.help.createCognomVariant(v)
}
func (d *MySQL) UpdateCognomVariantModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateCognomVariantModeracio(id, estat, motiu, moderatorID)
}

func (d *MySQL) GetCognomRedirect(fromID int) (*CognomRedirect, error) {
	return d.help.getCognomRedirect(fromID)
}

func (d *MySQL) ListCognomRedirects() ([]CognomRedirect, error) {
	return d.help.listCognomRedirects()
}

func (d *MySQL) ListCognomRedirectsByTo(toID int) ([]CognomRedirect, error) {
	return d.help.listCognomRedirectsByTo(toID)
}

func (d *MySQL) SetCognomRedirect(fromID, toID int, createdBy *int, reason string) error {
	return d.help.setCognomRedirect(fromID, toID, createdBy, reason)
}

func (d *MySQL) DeleteCognomRedirect(fromID int) error {
	return d.help.deleteCognomRedirect(fromID)
}

func (d *MySQL) CreateCognomRedirectSuggestion(s *CognomRedirectSuggestion) (int, error) {
	return d.help.createCognomRedirectSuggestion(s)
}

func (d *MySQL) GetCognomRedirectSuggestion(id int) (*CognomRedirectSuggestion, error) {
	return d.help.getCognomRedirectSuggestion(id)
}

func (d *MySQL) ListCognomRedirectSuggestions(f CognomRedirectSuggestionFilter) ([]CognomRedirectSuggestion, error) {
	return d.help.listCognomRedirectSuggestions(f)
}

func (d *MySQL) UpdateCognomRedirectSuggestionModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateCognomRedirectSuggestionModeracio(id, estat, motiu, moderatorID)
}

func (d *MySQL) CreateCognomReferencia(ref *CognomReferencia) (int, error) {
	return d.help.createCognomReferencia(ref)
}

func (d *MySQL) ListCognomReferencies(f CognomReferenciaFilter) ([]CognomReferencia, error) {
	return d.help.listCognomReferencies(f)
}

func (d *MySQL) UpdateCognomReferenciaModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateCognomReferenciaModeracio(id, estat, motiu, moderatorID)
}

// Cercador avançat
func (d *MySQL) UpsertSearchDoc(doc *SearchDoc) error { return d.help.upsertSearchDoc(doc) }
func (d *MySQL) GetSearchDoc(entityType string, entityID int) (*SearchDoc, error) {
	return d.help.getSearchDoc(entityType, entityID)
}
func (d *MySQL) DeleteSearchDoc(entityType string, entityID int) error {
	return d.help.deleteSearchDoc(entityType, entityID)
}
func (d *MySQL) SearchDocs(filter SearchQueryFilter) ([]SearchDocRow, int, SearchFacets, error) {
	return d.help.searchDocs(filter)
}
func (d *MySQL) ReplaceAdminClosure(descendantMunicipiID int, entries []AdminClosureEntry) error {
	return d.help.replaceAdminClosure(descendantMunicipiID, entries)
}
func (d *MySQL) ListAdminClosure(descendantMunicipiID int) ([]AdminClosureEntry, error) {
	return d.help.listAdminClosure(descendantMunicipiID)
}
func (d *MySQL) UpsertCognomFreqMunicipiAny(cognomID, municipiID, anyDoc, freq int) error {
	return d.help.upsertCognomFreqMunicipiAny(cognomID, municipiID, anyDoc, freq)
}
func (d *MySQL) ApplyCognomFreqMunicipiAnyDelta(cognomID, municipiID, anyDoc, delta int) error {
	return d.help.applyCognomFreqMunicipiAnyDelta(cognomID, municipiID, anyDoc, delta)
}
func (d *MySQL) QueryCognomHeatmap(cognomID int, anyStart, anyEnd int) ([]CognomFreqRow, error) {
	return d.help.queryCognomHeatmap(cognomID, anyStart, anyEnd)
}

func (d *MySQL) ListCognomImportRows(limit, offset int) ([]CognomImportRow, error) {
	return d.help.listCognomImportRows(limit, offset)
}

func (d *MySQL) ListCognomStatsRows(limit, offset int) ([]CognomStatsRow, error) {
	return d.help.listCognomStatsRows(limit, offset)
}

func (d *MySQL) RebuildCognomStats(cognomID int) error {
	return d.help.rebuildCognomStats(cognomID)
}

func (d *MySQL) GetCognomStatsTotal(cognomID int) (*CognomStatsTotal, error) {
	return d.help.getCognomStatsTotal(cognomID)
}

func (d *MySQL) ListCognomStatsAny(cognomID int, from, to int) ([]CognomStatsAnyRow, error) {
	return d.help.listCognomStatsAny(cognomID, from, to)
}

func (d *MySQL) ListCognomStatsAnyDecade(cognomID int, from, to int) ([]CognomStatsAnyRow, error) {
	return d.help.listCognomStatsAnyDecade(cognomID, from, to)
}

func (d *MySQL) ListCognomStatsAncestor(cognomID int, ancestorType string, level, any, limit int) ([]CognomStatsAncestorRow, error) {
	return d.help.listCognomStatsAncestor(cognomID, ancestorType, level, any, limit)
}

func (d *MySQL) CountCognomStatsAncestorDistinct(cognomID int, ancestorType string, level, any int) (int, error) {
	return d.help.countCognomStatsAncestorDistinct(cognomID, ancestorType, level, any)
}

// Noms
func (d *MySQL) UpsertNom(forma, key, notes string, createdBy *int) (int, error) {
	return d.help.upsertNom(forma, key, notes, createdBy)
}
func (d *MySQL) GetNom(id int) (*Nom, error) { return d.help.getNom(id) }
func (d *MySQL) ResolveNomByForma(forma string) (int, string, bool, error) {
	return d.help.resolveNomByForma(forma)
}
func (d *MySQL) UpsertNomFreqMunicipiAny(nomID, municipiID, anyDoc, delta int) error {
	return d.help.upsertNomFreqMunicipiAny(nomID, municipiID, anyDoc, delta)
}
func (d *MySQL) UpsertNomFreqMunicipiTotal(nomID, municipiID, delta int) error {
	return d.help.upsertNomFreqMunicipiTotal(nomID, municipiID, delta)
}
func (d *MySQL) UpsertCognomFreqMunicipiTotal(cognomID, municipiID, delta int) error {
	return d.help.upsertCognomFreqMunicipiTotal(cognomID, municipiID, delta)
}
func (d *MySQL) ListTopNomsByMunicipi(municipiID, limit int) ([]NomTotalRow, error) {
	return d.help.listTopNomsByMunicipi(municipiID, limit)
}
func (d *MySQL) ListTopCognomsByMunicipi(municipiID, limit int) ([]CognomTotalRow, error) {
	return d.help.listTopCognomsByMunicipi(municipiID, limit)
}
func (d *MySQL) ListNomSeries(municipiID, nomID int, bucket string) ([]NomFreqRow, error) {
	return d.help.listNomSeries(municipiID, nomID, bucket)
}
func (d *MySQL) ListCognomSeries(municipiID, cognomID int, bucket string) ([]CognomFreqRow, error) {
	return d.help.listCognomSeries(municipiID, cognomID, bucket)
}
func (d *MySQL) CountNomTotalsByMunicipi(municipiID int) (int, error) {
	return d.help.countNomTotalsByMunicipi(municipiID)
}
func (d *MySQL) CountCognomTotalsByMunicipi(municipiID int) (int, error) {
	return d.help.countCognomTotalsByMunicipi(municipiID)
}
func (d *MySQL) ClearNomCognomStatsByMunicipi(municipiID int) error {
	return d.help.clearNomCognomStatsByMunicipi(municipiID)
}
func (d *MySQL) UpsertNomFreqNivellAny(nomID, nivellID, anyDoc, delta int) error {
	return d.help.upsertNomFreqNivellAny(nomID, nivellID, anyDoc, delta)
}
func (d *MySQL) UpsertNomFreqNivellTotal(nomID, nivellID, delta int) error {
	return d.help.upsertNomFreqNivellTotal(nomID, nivellID, delta)
}
func (d *MySQL) ApplyCognomFreqNivellAnyDelta(cognomID, nivellID, anyDoc, delta int) error {
	return d.help.upsertCognomFreqNivellAny(cognomID, nivellID, anyDoc, delta)
}
func (d *MySQL) UpsertCognomFreqNivellTotal(cognomID, nivellID, delta int) error {
	return d.help.upsertCognomFreqNivellTotal(cognomID, nivellID, delta)
}
func (d *MySQL) ListTopNomsByNivell(nivellID, limit int) ([]NomTotalRow, error) {
	return d.help.listTopNomsByNivell(nivellID, limit)
}
func (d *MySQL) ListTopCognomsByNivell(nivellID, limit int) ([]CognomTotalRow, error) {
	return d.help.listTopCognomsByNivell(nivellID, limit)
}
func (d *MySQL) ListNomSeriesByNivell(nivellID, nomID int, bucket string) ([]NomFreqRow, error) {
	return d.help.listNomSeriesByNivell(nivellID, nomID, bucket)
}
func (d *MySQL) ListCognomSeriesByNivell(nivellID, cognomID int, bucket string) ([]CognomFreqRow, error) {
	return d.help.listCognomSeriesByNivell(nivellID, cognomID, bucket)
}
func (d *MySQL) ClearNomCognomStatsByNivell(nivellID int) error {
	return d.help.clearNomCognomStatsByNivell(nivellID)
}
func (d *MySQL) RebuildNivellNomCognomStats(nivellID int) error {
	return d.help.rebuildNivellNomCognomStats(nivellID)
}

func (d *MySQL) ListMunicipiMapes(filter MunicipiMapaFilter) ([]MunicipiMapa, error) {
	return d.help.listMunicipiMapes(filter)
}
func (d *MySQL) GetMunicipiMapa(id int) (*MunicipiMapa, error) {
	return d.help.getMunicipiMapa(id)
}
func (d *MySQL) CreateMunicipiMapa(m *MunicipiMapa) (int, error) {
	return d.help.createMunicipiMapa(m)
}
func (d *MySQL) UpdateMunicipiMapa(m *MunicipiMapa) error {
	return d.help.updateMunicipiMapa(m)
}
func (d *MySQL) UpdateMunicipiMapaCurrentVersion(mapaID, versionID int) error {
	return d.help.updateMunicipiMapaCurrentVersion(mapaID, versionID)
}
func (d *MySQL) NextMunicipiMapaVersionNumber(mapaID int) (int, error) {
	return d.help.nextMunicipiMapaVersionNumber(mapaID)
}
func (d *MySQL) ListMunicipiMapaVersions(filter MunicipiMapaVersionFilter) ([]MunicipiMapaVersion, error) {
	return d.help.listMunicipiMapaVersions(filter)
}
func (d *MySQL) GetMunicipiMapaVersion(id int) (*MunicipiMapaVersion, error) {
	return d.help.getMunicipiMapaVersion(id)
}
func (d *MySQL) CreateMunicipiMapaVersion(v *MunicipiMapaVersion) (int, error) {
	return d.help.createMunicipiMapaVersion(v)
}
func (d *MySQL) SaveMunicipiMapaDraft(versionID int, jsonData, changelog string, expectedLock int) (int, error) {
	return d.help.saveMunicipiMapaDraft(versionID, jsonData, changelog, expectedLock)
}
func (d *MySQL) UpdateMunicipiMapaVersionStatus(id int, status, notes string, moderatorID int) error {
	return d.help.updateMunicipiMapaVersionStatus(id, status, notes, moderatorID)
}
func (d *MySQL) ResolveMunicipiIDByMapaID(mapaID int) (int, error) {
	return d.help.resolveMunicipiIDByMapaID(mapaID)
}
func (d *MySQL) ResolveMunicipiIDByMapaVersionID(versionID int) (int, error) {
	return d.help.resolveMunicipiIDByMapaVersionID(versionID)
}

func (d *MySQL) EnsureMunicipiHistoria(municipiID int) (*MunicipiHistoria, error) {
	return d.help.ensureMunicipiHistoria(municipiID)
}
func (d *MySQL) GetMunicipiHistoriaByMunicipiID(municipiID int) (*MunicipiHistoria, error) {
	return d.help.getMunicipiHistoriaByMunicipiID(municipiID)
}
func (d *MySQL) ResolveMunicipiIDByHistoriaGeneralVersionID(versionID int) (int, error) {
	return d.help.resolveMunicipiIDByHistoriaGeneralVersionID(versionID)
}
func (d *MySQL) ResolveMunicipiIDByHistoriaFetVersionID(versionID int) (int, error) {
	return d.help.resolveMunicipiIDByHistoriaFetVersionID(versionID)
}
func (d *MySQL) NextMunicipiHistoriaGeneralVersion(historiaID int) (int, error) {
	return d.help.nextMunicipiHistoriaGeneralVersion(historiaID)
}
func (d *MySQL) CreateMunicipiHistoriaGeneralDraft(historiaID int, createdBy int, baseFromCurrent bool) (int, error) {
	return d.help.createMunicipiHistoriaGeneralDraft(historiaID, createdBy, baseFromCurrent)
}
func (d *MySQL) GetMunicipiHistoriaGeneralVersion(id int) (*MunicipiHistoriaGeneralVersion, error) {
	return d.help.getMunicipiHistoriaGeneralVersion(id)
}
func (d *MySQL) UpdateMunicipiHistoriaGeneralDraft(v *MunicipiHistoriaGeneralVersion) error {
	return d.help.updateMunicipiHistoriaGeneralDraft(v)
}
func (d *MySQL) SetMunicipiHistoriaGeneralStatus(versionID int, status, notes string, moderatorID *int) error {
	return d.help.setMunicipiHistoriaGeneralStatus(versionID, status, notes, moderatorID)
}
func (d *MySQL) GetMunicipiHistoriaFet(id int) (*MunicipiHistoriaFet, error) {
	return d.help.getMunicipiHistoriaFet(id)
}
func (d *MySQL) CreateMunicipiHistoriaFet(municipiID int, createdBy int) (int, error) {
	return d.help.createMunicipiHistoriaFet(municipiID, createdBy)
}
func (d *MySQL) NextMunicipiHistoriaFetVersion(fetID int) (int, error) {
	return d.help.nextMunicipiHistoriaFetVersion(fetID)
}
func (d *MySQL) CreateMunicipiHistoriaFetDraft(fetID int, createdBy int, baseFromCurrent bool) (int, error) {
	return d.help.createMunicipiHistoriaFetDraft(fetID, createdBy, baseFromCurrent)
}
func (d *MySQL) GetMunicipiHistoriaFetVersion(id int) (*MunicipiHistoriaFetVersion, error) {
	return d.help.getMunicipiHistoriaFetVersion(id)
}
func (d *MySQL) UpdateMunicipiHistoriaFetDraft(v *MunicipiHistoriaFetVersion) error {
	return d.help.updateMunicipiHistoriaFetDraft(v)
}
func (d *MySQL) SetMunicipiHistoriaFetStatus(versionID int, status, notes string, moderatorID *int) error {
	return d.help.setMunicipiHistoriaFetStatus(versionID, status, notes, moderatorID)
}
func (d *MySQL) GetMunicipiHistoriaSummary(municipiID int) (*MunicipiHistoriaGeneralVersion, []MunicipiHistoriaFetVersion, error) {
	return d.help.getMunicipiHistoriaSummary(municipiID)
}
func (d *MySQL) ListMunicipiHistoriaTimeline(municipiID int, status string, limit, offset int, q string, anyFrom, anyTo *int) ([]MunicipiHistoriaFetVersion, int, error) {
	return d.help.listMunicipiHistoriaTimeline(municipiID, status, limit, offset, q, anyFrom, anyTo)
}
func (d *MySQL) ListPendingMunicipiHistoriaGeneralVersions(limit, offset int) ([]MunicipiHistoriaGeneralVersion, int, error) {
	return d.help.listPendingMunicipiHistoriaGeneralVersions(limit, offset)
}
func (d *MySQL) ListPendingMunicipiHistoriaFetVersions(limit, offset int) ([]MunicipiHistoriaFetVersion, int, error) {
	return d.help.listPendingMunicipiHistoriaFetVersions(limit, offset)
}

func (d *MySQL) GetMunicipiDemografiaMeta(municipiID int) (*MunicipiDemografiaMeta, error) {
	return d.help.getMunicipiDemografiaMeta(municipiID)
}
func (d *MySQL) ListMunicipiDemografiaAny(municipiID int, from, to int) ([]MunicipiDemografiaAny, error) {
	return d.help.listMunicipiDemografiaAny(municipiID, from, to)
}
func (d *MySQL) ListMunicipiDemografiaDecades(municipiID int, from, to int) ([]MunicipiDemografiaAny, error) {
	return d.help.listMunicipiDemografiaDecades(municipiID, from, to)
}
func (d *MySQL) ApplyMunicipiDemografiaDelta(municipiID, year int, tipus string, delta int) error {
	return d.help.applyMunicipiDemografiaDelta(municipiID, year, tipus, delta)
}
func (d *MySQL) ApplyMunicipiDemografiaDeltaTx(tx *sql.Tx, municipiID, year int, tipus string, delta int) error {
	return d.help.applyMunicipiDemografiaDeltaTx(tx, municipiID, year, tipus, delta)
}
func (d *MySQL) RebuildMunicipiDemografia(municipiID int) error {
	return d.help.rebuildMunicipiDemografia(municipiID)
}
func (d *MySQL) GetNivellDemografiaMeta(nivellID int) (*NivellDemografiaMeta, error) {
	return d.help.getNivellDemografiaMeta(nivellID)
}
func (d *MySQL) ListNivellDemografiaAny(nivellID int, from, to int) ([]NivellDemografiaAny, error) {
	return d.help.listNivellDemografiaAny(nivellID, from, to)
}
func (d *MySQL) ListNivellDemografiaDecades(nivellID int, from, to int) ([]NivellDemografiaAny, error) {
	return d.help.listNivellDemografiaDecades(nivellID, from, to)
}
func (d *MySQL) ApplyNivellDemografiaDelta(nivellID, year int, tipus string, delta int) error {
	return d.help.applyNivellDemografiaDelta(nivellID, year, tipus, delta)
}
func (d *MySQL) RebuildNivellDemografia(nivellID int) error {
	return d.help.rebuildNivellDemografia(nivellID)
}

func (d *MySQL) ListMunicipiAnecdotariPublished(municipiID int, f MunicipiAnecdotariFilter) ([]MunicipiAnecdotariVersion, int, error) {
	return d.help.listMunicipiAnecdotariPublished(municipiID, f)
}
func (d *MySQL) GetMunicipiAnecdotariPublished(itemID int) (*MunicipiAnecdotariVersion, error) {
	return d.help.getMunicipiAnecdotariPublished(itemID)
}
func (d *MySQL) ListMunicipiAnecdotariComments(itemID int, limit, offset int) ([]MunicipiAnecdotariComment, int, error) {
	return d.help.listMunicipiAnecdotariComments(itemID, limit, offset)
}
func (d *MySQL) CreateMunicipiAnecdotariItem(municipiID int, createdBy int) (int, error) {
	return d.help.createMunicipiAnecdotariItem(municipiID, createdBy)
}
func (d *MySQL) CreateMunicipiAnecdotariDraft(itemID int, createdBy int, baseFromCurrent bool) (int, error) {
	return d.help.createMunicipiAnecdotariDraft(itemID, createdBy, baseFromCurrent)
}
func (d *MySQL) GetMunicipiAnecdotariVersion(id int) (*MunicipiAnecdotariVersion, error) {
	return d.help.getMunicipiAnecdotariVersion(id)
}
func (d *MySQL) GetPendingMunicipiAnecdotariVersionByItemID(itemID int) (*MunicipiAnecdotariVersion, error) {
	return d.help.getPendingMunicipiAnecdotariVersionByItemID(itemID)
}
func (d *MySQL) UpdateMunicipiAnecdotariDraft(v *MunicipiAnecdotariVersion) error {
	return d.help.updateMunicipiAnecdotariDraft(v)
}
func (d *MySQL) SubmitMunicipiAnecdotariVersion(versionID int) error {
	return d.help.submitMunicipiAnecdotariVersion(versionID)
}
func (d *MySQL) ListPendingMunicipiAnecdotariVersions(limit, offset int) ([]MunicipiAnecdotariVersion, int, error) {
	return d.help.listPendingMunicipiAnecdotariVersions(limit, offset)
}
func (d *MySQL) ApproveMunicipiAnecdotariVersion(versionID int, moderatorID int) error {
	return d.help.approveMunicipiAnecdotariVersion(versionID, moderatorID)
}
func (d *MySQL) RejectMunicipiAnecdotariVersion(versionID int, moderatorID int, notes string) error {
	return d.help.rejectMunicipiAnecdotariVersion(versionID, moderatorID, notes)
}
func (d *MySQL) CreateMunicipiAnecdotariComment(itemID int, userID int, body string) (int, error) {
	return d.help.createMunicipiAnecdotariComment(itemID, userID, body)
}
func (d *MySQL) GetMunicipiAnecdotariLastCommentAt(userID int) (time.Time, error) {
	return d.help.getMunicipiAnecdotariLastCommentAt(userID)
}
func (d *MySQL) ResolveMunicipiIDByAnecdotariItemID(itemID int) (int, error) {
	return d.help.resolveMunicipiIDByAnecdotariItemID(itemID)
}
func (d *MySQL) ResolveMunicipiIDByAnecdotariVersionID(versionID int) (int, error) {
	return d.help.resolveMunicipiIDByAnecdotariVersionID(versionID)
}

// Esdeveniments historics
func (d *MySQL) CreateEventHistoric(e *EventHistoric) (int, error) {
	return d.help.createEventHistoric(e)
}

func (d *MySQL) GetEventHistoric(id int) (*EventHistoric, error) {
	return d.help.getEventHistoric(id)
}

func (d *MySQL) GetEventHistoricBySlug(slug string) (*EventHistoric, error) {
	return d.help.getEventHistoricBySlug(slug)
}

func (d *MySQL) UpdateEventHistoric(e *EventHistoric) error {
	return d.help.updateEventHistoric(e)
}

func (d *MySQL) ListEventsHistoric(filter EventHistoricFilter) ([]EventHistoric, error) {
	return d.help.listEventsHistoric(filter)
}

func (d *MySQL) UpdateEventHistoricModeracio(id int, estat, notes string, moderatorID int) error {
	return d.help.updateEventHistoricModeracio(id, estat, notes, moderatorID)
}

func (d *MySQL) ListEventImpacts(eventID int) ([]EventHistoricImpact, error) {
	return d.help.listEventImpacts(eventID)
}

func (d *MySQL) ReplaceEventImpacts(eventID int, impacts []EventHistoricImpact) error {
	return d.help.replaceEventImpacts(eventID, impacts)
}

func (d *MySQL) ListEventsByScope(scopeType string, scopeID int, filter EventHistoricFilter) ([]EventHistoric, error) {
	return d.help.listEventsByScope(scopeType, scopeID, filter)
}
