package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	pq "github.com/lib/pq"
	"golang.org/x/crypto/bcrypt"
)

type PostgreSQL struct {
	Host   string
	Port   string
	User   string
	Pass   string
	DBName string
	Conn   *sql.DB
	help   sqlHelper
}

func (d *PostgreSQL) Connect() error {
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		d.Host, d.Port, d.User, d.Pass, d.DBName)

	conn, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return fmt.Errorf("error connectant a PostgreSQL: %w", err)
	}
	d.Conn = conn
	d.help = newSQLHelper(conn, "postgres", "NOW()")
	logInfof("Conectat a PostgreSQL")
	return nil
}

func (d *PostgreSQL) Close() {
	if d.Conn != nil {
		d.Conn.Close()
	}
}

func (d *PostgreSQL) Exec(query string, args ...interface{}) (int64, error) {
	res, err := d.Conn.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	id, _ := res.LastInsertId()
	return id, nil
}

func (d *PostgreSQL) Query(query string, args ...interface{}) ([]map[string]interface{}, error) {
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

		if err := rows.Scan(scanArgs...); err != nil {
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

func (d *PostgreSQL) InsertUser(user *User) error {
	return d.help.insertUser(user)
}

func (d *PostgreSQL) GetUserByEmail(email string) (*User, error) {
	return d.help.getUserByEmail(email)
}

func (d *PostgreSQL) GetUserByID(id int) (*User, error) {
	return d.help.getUserByID(id)
}

func (d *PostgreSQL) ExistsUserByUsername(username string) (bool, error) {
	return d.help.existsUserByUsername(username)
}

func (d *PostgreSQL) ExistsUserByEmail(email string) (bool, error) {
	return d.help.existsUserByEmail(email)
}

func (p *PostgreSQL) SaveActivationToken(email, token string) error {
	return p.help.saveActivationToken(email, token)
}

func (d *PostgreSQL) ActivateUser(token string) error {
	return d.help.activateUser(token)
}

func (d *PostgreSQL) AuthenticateUser(usernameOrEmail, password string) (*User, error) {
	u, err := d.help.authenticateUser(usernameOrEmail, password)
	if err != nil {
		return nil, fmt.Errorf("usuari no trobat o no actiu")
	}
	if err := bcrypt.CompareHashAndPassword(u.Password, []byte(password)); err != nil {
		return nil, fmt.Errorf("contrasenya incorrecta")
	}
	return u, nil
}

// Gestió de sessions - adaptat a PostgreSQL
func (d *PostgreSQL) SaveSession(sessionID string, userID int, expiry string) error {
	if err := d.help.saveSession(sessionID, userID, expiry); err != nil {
		logErrorf("[PostgreSQL] Error guardant sessió: %v", err)
		return err
	}
	return nil
}

func (d *PostgreSQL) GetSessionUser(sessionID string) (*User, error) {
	return d.help.getSessionUser(sessionID)
}

func (d *PostgreSQL) DeleteSession(sessionID string) error {
	return d.help.deleteSession(sessionID)
}
func (d *PostgreSQL) ListUserGroups(userID int) ([]Group, error) {
	return d.help.listUserGroups(userID)
}

func (d *PostgreSQL) CreatePasswordReset(email, token, expiry, lang string) (bool, error) {
	return d.help.createPasswordReset(email, token, expiry, lang)
}

func (d *PostgreSQL) GetPasswordReset(token string) (*PasswordReset, error) {
	return d.help.getPasswordReset(token)
}

func (d *PostgreSQL) MarkPasswordResetUsed(id int) error {
	return d.help.markPasswordResetUsed(id)
}

func (d *PostgreSQL) UpdateUserPassword(userID int, passwordHash []byte) error {
	return d.help.updateUserPassword(userID, passwordHash)
}

func (d *PostgreSQL) CreatePrivacyDefaults(userID int) error {
	return d.help.createPrivacyDefaults(userID)
}

func (d *PostgreSQL) GetPrivacySettings(userID int) (*PrivacySettings, error) {
	return d.help.getPrivacySettings(userID)
}

func (d *PostgreSQL) SavePrivacySettings(userID int, p *PrivacySettings) error {
	return d.help.savePrivacySettings(userID, p)
}

// Missatgeria interna
func (d *PostgreSQL) GetOrCreateDMThread(userA, userB int) (*DMThread, error) {
	return d.help.getOrCreateDMThread(userA, userB)
}

func (d *PostgreSQL) GetDMThreadByUsers(userA, userB int) (*DMThread, error) {
	return d.help.getDMThreadByUsers(userA, userB)
}

func (d *PostgreSQL) GetDMThreadByID(threadID int) (*DMThread, error) {
	return d.help.getDMThreadByID(threadID)
}

func (d *PostgreSQL) ListDMThreadsForUser(userID int, f DMThreadListFilter) ([]DMThreadListItem, error) {
	return d.help.listDMThreadsForUser(userID, f)
}

func (d *PostgreSQL) CountDMUnread(userID int) (int, error) {
	return d.help.countDMUnread(userID)
}

func (d *PostgreSQL) ListDMThreadFolders(userID int) ([]string, error) {
	return d.help.listDMThreadFolders(userID)
}

func (d *PostgreSQL) SetDMThreadFolder(threadID, userID int, folder string) error {
	return d.help.setDMThreadFolder(threadID, userID, folder)
}

func (d *PostgreSQL) ListDMMessages(threadID, limit, beforeID int) ([]DMMessage, error) {
	return d.help.listDMMessages(threadID, limit, beforeID)
}

func (d *PostgreSQL) CreateDMMessage(threadID, senderID int, body string) (int, error) {
	return d.help.createDMMessage(threadID, senderID, body)
}

func (d *PostgreSQL) UpdateDMThreadLastMessage(threadID, msgID int, at time.Time) error {
	return d.help.updateDMThreadLastMessage(threadID, msgID, at)
}

func (d *PostgreSQL) MarkDMThreadRead(threadID, userID, lastMsgID int) error {
	return d.help.markDMThreadRead(threadID, userID, lastMsgID)
}

func (d *PostgreSQL) SetDMThreadArchived(threadID, userID int, archived bool) error {
	return d.help.setDMThreadArchived(threadID, userID, archived)
}

func (d *PostgreSQL) SoftDeleteDMThread(threadID, userID int) error {
	return d.help.softDeleteDMThread(threadID, userID)
}

func (d *PostgreSQL) AddUserBlock(blockerID, blockedID int) error {
	return d.help.addUserBlock(blockerID, blockedID)
}

func (d *PostgreSQL) RemoveUserBlock(blockerID, blockedID int) error {
	return d.help.removeUserBlock(blockerID, blockedID)
}

func (d *PostgreSQL) IsUserBlocked(blockerID, blockedID int) (bool, error) {
	return d.help.isUserBlocked(blockerID, blockedID)
}

func (d *PostgreSQL) UpdateUserProfile(u *User) error {
	return d.help.updateUserProfile(u)
}

func (d *PostgreSQL) UpdateUserEmail(userID int, newEmail string) error {
	return d.help.updateUserEmail(userID, newEmail)
}

func (d *PostgreSQL) ListDashboardWidgets(userID int) ([]DashboardWidgetConfig, error) {
	return d.help.listDashboardWidgets(userID)
}

func (d *PostgreSQL) SaveDashboardWidgets(userID int, widgets []DashboardWidgetConfig) error {
	return d.help.saveDashboardWidgets(userID, widgets)
}

func (d *PostgreSQL) ClearDashboardWidgets(userID int) error {
	return d.help.clearDashboardWidgets(userID)
}
func (d *PostgreSQL) ListPlatformSettings() ([]PlatformSetting, error) {
	return d.help.listPlatformSettings()
}
func (d *PostgreSQL) UpsertPlatformSetting(key, value string, updatedBy int) error {
	return d.help.upsertPlatformSetting(key, value, updatedBy)
}
func (d *PostgreSQL) ListMaintenanceWindows() ([]MaintenanceWindow, error) {
	return d.help.listMaintenanceWindows()
}
func (d *PostgreSQL) GetMaintenanceWindow(id int) (*MaintenanceWindow, error) {
	return d.help.getMaintenanceWindow(id)
}
func (d *PostgreSQL) SaveMaintenanceWindow(w *MaintenanceWindow) (int, error) {
	return d.help.saveMaintenanceWindow(w)
}
func (d *PostgreSQL) DeleteMaintenanceWindow(id int) error {
	return d.help.deleteMaintenanceWindow(id)
}
func (d *PostgreSQL) GetActiveMaintenanceWindow(now time.Time) (*MaintenanceWindow, error) {
	return d.help.getActiveMaintenanceWindow(now)
}
func (d *PostgreSQL) GetAdminKPIsGeneral() (*AdminKPIsGeneral, error) {
	return d.help.getAdminKPIsGeneral()
}
func (d *PostgreSQL) CountUsersSince(since time.Time) (int, error) {
	return d.help.countUsersSince(since)
}
func (d *PostgreSQL) ListTransparencySettings() ([]TransparencySetting, error) {
	return d.help.listTransparencySettings()
}
func (d *PostgreSQL) UpsertTransparencySetting(key, value string, updatedBy int) error {
	return d.help.upsertTransparencySetting(key, value, updatedBy)
}
func (d *PostgreSQL) ListTransparencyContributors(includePrivate bool) ([]TransparencyContributor, error) {
	return d.help.listTransparencyContributors(includePrivate)
}
func (d *PostgreSQL) GetTransparencyContributor(id int) (*TransparencyContributor, error) {
	return d.help.getTransparencyContributor(id)
}
func (d *PostgreSQL) SaveTransparencyContributor(c *TransparencyContributor) (int, error) {
	return d.help.saveTransparencyContributor(c)
}
func (d *PostgreSQL) DeleteTransparencyContributor(id int) error {
	return d.help.deleteTransparencyContributor(id)
}
func (d *PostgreSQL) InsertAdminImportRun(importType, status string, createdBy int) error {
	return d.help.insertAdminImportRun(importType, status, createdBy)
}
func (d *PostgreSQL) CountAdminImportRunsSince(since time.Time) (AdminImportRunSummary, error) {
	return d.help.countAdminImportRunsSince(since)
}
func (d *PostgreSQL) CreateAdminJob(job *AdminJob) (int, error) {
	return d.help.createAdminJob(job)
}
func (d *PostgreSQL) UpdateAdminJobProgress(id int, progressDone, progressTotal int) error {
	return d.help.updateAdminJobProgress(id, progressDone, progressTotal)
}
func (d *PostgreSQL) UpdateAdminJobStatus(id int, status, phase, errorText, resultJSON string, finishedAt *time.Time) error {
	return d.help.updateAdminJobStatus(id, status, phase, errorText, resultJSON, finishedAt)
}
func (d *PostgreSQL) GetAdminJob(id int) (*AdminJob, error) {
	return d.help.getAdminJob(id)
}
func (d *PostgreSQL) ListAdminJobs(filter AdminJobFilter) ([]AdminJob, error) {
	return d.help.listAdminJobs(filter)
}
func (d *PostgreSQL) CountAdminJobs(filter AdminJobFilter) (int, error) {
	return d.help.countAdminJobs(filter)
}
func (d *PostgreSQL) CreateAdminJobTargets(jobID int, targets []AdminJobTarget) error {
	return d.help.createAdminJobTargets(jobID, targets)
}
func (d *PostgreSQL) ListAdminJobTargets(jobID int) ([]AdminJobTarget, error) {
	return d.help.listAdminJobTargets(jobID)
}
func (d *PostgreSQL) InsertAdminAudit(entry *AdminAuditEntry) (int, error) {
	return d.help.insertAdminAudit(entry)
}
func (d *PostgreSQL) ListAdminAudit(filter AdminAuditFilter) ([]AdminAuditEntry, error) {
	return d.help.listAdminAudit(filter)
}
func (d *PostgreSQL) CountAdminAudit(filter AdminAuditFilter) (int, error) {
	return d.help.countAdminAudit(filter)
}
func (d *PostgreSQL) ListAdminSessions(filter AdminSessionFilter) ([]AdminSessionRow, error) {
	return d.help.listAdminSessions(filter)
}
func (d *PostgreSQL) CountAdminSessions(filter AdminSessionFilter) (int, error) {
	return d.help.countAdminSessions(filter)
}
func (d *PostgreSQL) RevokeUserSessions(userID int) error {
	return d.help.revokeUserSessions(userID)
}

func (d *PostgreSQL) ListUsersAdmin() ([]UserAdminRow, error) {
	return d.help.listUsersAdmin()
}

func (d *PostgreSQL) ListUsersAdminFiltered(filter UserAdminFilter) ([]UserAdminRow, error) {
	return d.help.listUsersAdminFiltered(filter)
}

func (d *PostgreSQL) CountUsersAdmin(filter UserAdminFilter) (int, error) {
	return d.help.countUsersAdmin(filter)
}

func (d *PostgreSQL) SetUserActive(userID int, active bool) error {
	return d.help.setUserActive(userID, active)
}

func (d *PostgreSQL) SetUserBanned(userID int, banned bool) error {
	return d.help.setUserBanned(userID, banned)
}

func (d *PostgreSQL) CreateEmailChange(userID int, newEmail, tokenConfirm, expConfirm, tokenRevert, expRevert, lang string) error {
	return d.help.createEmailChange(userID, newEmail, tokenConfirm, expConfirm, tokenRevert, expRevert, lang)
}

func (d *PostgreSQL) ConfirmEmailChange(token string) (*EmailChange, error) {
	return d.help.confirmEmailChange(token)
}

func (d *PostgreSQL) RevertEmailChange(token string) (*EmailChange, error) {
	return d.help.revertEmailChange(token)
}

func (d *PostgreSQL) markEmailChangeConfirmed(id int) error {
	return d.help.markEmailChangeConfirmed(id)
}

func (d *PostgreSQL) markEmailChangeReverted(id int) error {
	return d.help.markEmailChangeReverted(id)
}

// Policies
func (d *PostgreSQL) UserHasAnyPolicy(userID int, policies []string) (bool, error) {
	return d.help.userHasAnyPolicy(userID, policies)
}
func (d *PostgreSQL) EnsureDefaultPolicies() error {
	return d.help.ensureDefaultPolicies()
}
func (d *PostgreSQL) EnsureDefaultPointsRules() error {
	return d.help.ensureDefaultPointsRules()
}
func (d *PostgreSQL) EnsureDefaultAchievements() error {
	return d.help.ensureDefaultAchievements()
}
func (d *PostgreSQL) ListGroups() ([]Group, error) {
	return d.help.listGroups()
}
func (d *PostgreSQL) CreateGroup(name, desc string) (int, error) {
	return d.help.createGroup(name, desc)
}
func (d *PostgreSQL) ListGroupMembers(groupID int) ([]UserAdminRow, error) {
	return d.help.listGroupMembers(groupID)
}
func (d *PostgreSQL) AddUserGroup(userID, groupID int) error {
	return d.help.addUserGroup(userID, groupID)
}
func (d *PostgreSQL) RemoveUserGroup(userID, groupID int) error {
	return d.help.removeUserGroup(userID, groupID)
}
func (d *PostgreSQL) ListPolitiques() ([]Politica, error) {
	return d.help.listPolitiques()
}
func (d *PostgreSQL) GetPolitica(id int) (*Politica, error) {
	return d.help.getPolitica(id)
}
func (d *PostgreSQL) SavePolitica(p *Politica) (int, error) {
	return d.help.savePolitica(p)
}
func (d *PostgreSQL) ListPoliticaGrants(politicaID int) ([]PoliticaGrant, error) {
	return d.help.listPoliticaGrants(politicaID)
}
func (d *PostgreSQL) SavePoliticaGrant(g *PoliticaGrant) (int, error) {
	return d.help.savePoliticaGrant(g)
}
func (d *PostgreSQL) DeletePoliticaGrant(id int) error {
	return d.help.deletePoliticaGrant(id)
}
func (d *PostgreSQL) ListUserPolitiques(userID int) ([]Politica, error) {
	return d.help.listUserPolitiques(userID)
}
func (d *PostgreSQL) AddUserPolitica(userID, politicaID int) error {
	return d.help.addUserPolitica(userID, politicaID)
}
func (d *PostgreSQL) RemoveUserPolitica(userID, politicaID int) error {
	return d.help.removeUserPolitica(userID, politicaID)
}
func (d *PostgreSQL) ListGroupPolitiques(groupID int) ([]Politica, error) {
	return d.help.listGroupPolitiques(groupID)
}
func (d *PostgreSQL) AddGroupPolitica(groupID, politicaID int) error {
	return d.help.addGroupPolitica(groupID, politicaID)
}
func (d *PostgreSQL) RemoveGroupPolitica(groupID, politicaID int) error {
	return d.help.removeGroupPolitica(groupID, politicaID)
}
func (d *PostgreSQL) GetEffectivePoliticaPerms(userID int) (PolicyPermissions, error) {
	return d.help.getEffectivePoliticaPerms(userID)
}
func (d *PostgreSQL) GetUserPermissionsVersion(userID int) (int, error) {
	return d.help.getUserPermissionsVersion(userID)
}
func (d *PostgreSQL) BumpUserPermissionsVersion(userID int) error {
	return d.help.bumpUserPermissionsVersion(userID)
}

func (d *PostgreSQL) BumpGroupPermissionsVersion(groupID int) error {
	return d.help.bumpGroupPermissionsVersion(groupID)
}

func (d *PostgreSQL) BumpPolicyPermissionsVersion(politicaID int) error {
	return d.help.bumpPolicyPermissionsVersion(politicaID)
}

// Persones (moderació)
func (d *PostgreSQL) ListPersones(f PersonaFilter) ([]Persona, error) {
	return d.help.listPersones(f)
}
func (d *PostgreSQL) CountPersones(f PersonaFilter) (int, error) {
	return d.help.countPersones(f)
}
func (d *PostgreSQL) GetPersona(id int) (*Persona, error) {
	return d.help.getPersona(id)
}
func (d *PostgreSQL) CreatePersona(p *Persona) (int, error) {
	return d.help.createPersona(p)
}
func (d *PostgreSQL) UpdatePersona(p *Persona) error {
	return d.help.updatePersona(p)
}
func (d *PostgreSQL) ListPersonaFieldLinks(personaID int) ([]PersonaFieldLink, error) {
	return d.help.listPersonaFieldLinks(personaID)
}
func (d *PostgreSQL) UpsertPersonaFieldLink(personaID int, fieldKey string, registreID int, userID int) error {
	return d.help.upsertPersonaFieldLink(personaID, fieldKey, registreID, userID)
}
func (d *PostgreSQL) ListPersonaAnecdotes(personaID int, userID int) ([]PersonaAnecdote, error) {
	return d.help.listPersonaAnecdotes(personaID, userID)
}
func (d *PostgreSQL) CreatePersonaAnecdote(a *PersonaAnecdote) (int, error) {
	return d.help.createPersonaAnecdote(a)
}
func (d *PostgreSQL) UpdatePersonaModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updatePersonaModeracio(id, estat, motiu, moderatorID)
}
func (d *PostgreSQL) ExternalSitesListActive() ([]ExternalSite, error) {
	return d.help.listExternalSitesActive()
}

func (d *PostgreSQL) ExternalSitesListAll() ([]ExternalSite, error) {
	return d.help.listExternalSitesAll()
}
func (d *PostgreSQL) ExternalSiteUpsert(site *ExternalSite) (int, error) {
	return d.help.upsertExternalSite(site)
}
func (d *PostgreSQL) ExternalSiteToggleActive(id int) error {
	return d.help.toggleExternalSiteActive(id)
}
func (d *PostgreSQL) ExternalLinksListByPersona(personaID int, statusFilter string) ([]ExternalLinkRow, error) {
	return d.help.listExternalLinksByPersona(personaID, statusFilter)
}

func (d *PostgreSQL) ExternalLinksListByStatus(status string) ([]ExternalLinkAdminRow, error) {
	return d.help.listExternalLinksByStatus(status)
}

func (d *PostgreSQL) CountExternalLinksByStatus(status string) (int, error) {
	return d.help.countExternalLinksByStatus(status)
}
func (d *PostgreSQL) ListExternalLinksAdmin(filter ExternalLinkAdminFilter) ([]ExternalLinkAdminRow, error) {
	return d.help.listExternalLinksAdmin(filter)
}
func (d *PostgreSQL) CountExternalLinksAdmin(filter ExternalLinkAdminFilter) (int, error) {
	return d.help.countExternalLinksAdmin(filter)
}
func (d *PostgreSQL) ExternalLinkInsertPending(personaID int, userID int, url, title string) (int, error) {
	return d.help.createExternalLinkPending(personaID, userID, url, title)
}
func (d *PostgreSQL) ExternalLinkModerate(id int, status string) error {
	return d.help.updateExternalLinkStatus(id, status)
}

// Paisos
func (d *PostgreSQL) ListPaisos() ([]Pais, error) {
	return d.help.listPaisos()
}
func (d *PostgreSQL) GetPais(id int) (*Pais, error) {
	return d.help.getPais(id)
}
func (d *PostgreSQL) CreatePais(p *Pais) (int, error) {
	return d.help.createPais(p)
}
func (d *PostgreSQL) UpdatePais(p *Pais) error {
	return d.help.updatePais(p)
}

// Nivells administratius
func (d *PostgreSQL) ListNivells(f NivellAdminFilter) ([]NivellAdministratiu, error) {
	return d.help.listNivells(f)
}

func (d *PostgreSQL) CountNivells(f NivellAdminFilter) (int, error) {
	return d.help.countNivells(f)
}
func (d *PostgreSQL) GetNivell(id int) (*NivellAdministratiu, error) {
	return d.help.getNivell(id)
}
func (d *PostgreSQL) CreateNivell(n *NivellAdministratiu) (int, error) {
	return d.help.createNivell(n)
}
func (d *PostgreSQL) UpdateNivell(n *NivellAdministratiu) error {
	return d.help.updateNivell(n)
}
func (d *PostgreSQL) UpdateNivellModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateNivellModeracio(id, estat, motiu, moderatorID)
}

// Municipis
func (d *PostgreSQL) ListMunicipis(f MunicipiFilter) ([]MunicipiRow, error) {
	return d.help.listMunicipis(f)
}

func (d *PostgreSQL) CountMunicipis(f MunicipiFilter) (int, error) {
	return d.help.countMunicipis(f)
}
func (d *PostgreSQL) ListMunicipisBrowse(f MunicipiBrowseFilter) ([]MunicipiBrowseRow, error) {
	return d.help.listMunicipisBrowse(f)
}
func (d *PostgreSQL) CountMunicipisBrowse(f MunicipiBrowseFilter) (int, error) {
	return d.help.countMunicipisBrowse(f)
}
func (d *PostgreSQL) DebugMunicipiBrowse(f MunicipiBrowseFilter) MunicipiBrowseDebugInfo {
	return d.help.debugMunicipiBrowse(f)
}
func (d *PostgreSQL) SuggestMunicipis(f MunicipiBrowseFilter) ([]MunicipiSuggestRow, error) {
	return d.help.suggestMunicipis(f)
}
func (d *PostgreSQL) GetMunicipi(id int) (*Municipi, error) {
	return d.help.getMunicipi(id)
}
func (d *PostgreSQL) CreateMunicipi(m *Municipi) (int, error) {
	return d.help.createMunicipi(m)
}
func (d *PostgreSQL) ResolveMunicipisByNames(names []string) ([]MunicipiResolveRow, error) {
	return d.help.resolveMunicipisByNames(names)
}
func (d *PostgreSQL) ResolveArquebisbatsByNames(names []string) ([]ArquebisbatResolveRow, error) {
	return d.help.resolveArquebisbatsByNames(names)
}
func (d *PostgreSQL) ResolveArxiusByNames(names []string) ([]ArxiuResolveRow, error) {
	return d.help.resolveArxiusByNames(names)
}
func (d *PostgreSQL) BulkInsertNivells(ctx context.Context, rows []NivellAdministratiu) ([]int, string, error) {
	if len(rows) == 0 {
		return nil, "postgres-copy", nil
	}
	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, "postgres-copy", err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
        CREATE TEMP TABLE tmp_nivells_import (
            import_seq INTEGER,
            pais_id INTEGER,
            nivel INTEGER,
            nom_nivell TEXT,
            tipus_nivell TEXT,
            codi_oficial TEXT,
            altres TEXT,
            parent_id INTEGER,
            any_inici INTEGER,
            any_fi INTEGER,
            estat TEXT,
            created_by INTEGER,
            moderation_status TEXT,
            moderated_by INTEGER,
            moderated_at TIMESTAMP,
            moderation_notes TEXT
        ) ON COMMIT DROP`)
	if err != nil {
		return nil, "postgres-copy", err
	}
	stmt, err := tx.PrepareContext(ctx, pq.CopyIn("tmp_nivells_import",
		"import_seq",
		"pais_id",
		"nivel",
		"nom_nivell",
		"tipus_nivell",
		"codi_oficial",
		"altres",
		"parent_id",
		"any_inici",
		"any_fi",
		"estat",
		"created_by",
		"moderation_status",
		"moderated_by",
		"moderated_at",
		"moderation_notes",
	))
	if err != nil {
		return nil, "postgres-copy", err
	}
	for i, n := range rows {
		if _, err := stmt.Exec(
			i,
			n.PaisID,
			n.Nivel,
			n.NomNivell,
			n.TipusNivell,
			n.CodiOficial,
			n.Altres,
			n.ParentID,
			n.AnyInici,
			n.AnyFi,
			n.Estat,
			n.CreatedBy,
			n.ModeracioEstat,
			n.ModeratedBy,
			n.ModeratedAt,
			n.ModeracioMotiu,
		); err != nil {
			_ = stmt.Close()
			return nil, "postgres-copy", err
		}
	}
	if _, err := stmt.Exec(); err != nil {
		_ = stmt.Close()
		return nil, "postgres-copy", err
	}
	if err := stmt.Close(); err != nil {
		return nil, "postgres-copy", err
	}
	rowsRes, err := tx.QueryContext(ctx, `
        INSERT INTO nivells_administratius (
            pais_id, nivel, nom_nivell, tipus_nivell, codi_oficial, altres, parent_id,
            any_inici, any_fi, estat, created_by, moderation_status, moderated_by, moderated_at, moderation_notes,
            created_at, updated_at
        )
        SELECT
            pais_id, nivel, nom_nivell, tipus_nivell, codi_oficial, altres, parent_id,
            any_inici, any_fi, estat, created_by, moderation_status, moderated_by, moderated_at, moderation_notes,
            NOW(), NOW()
        FROM tmp_nivells_import
        ORDER BY import_seq
        RETURNING id`)
	if err != nil {
		return nil, "postgres-copy", err
	}
	defer rowsRes.Close()
	ids := make([]int, 0, len(rows))
	for rowsRes.Next() {
		var id int
		if err := rowsRes.Scan(&id); err != nil {
			return nil, "postgres-copy", err
		}
		ids = append(ids, id)
	}
	if err := rowsRes.Err(); err != nil {
		return nil, "postgres-copy", err
	}
	if err := tx.Commit(); err != nil {
		return nil, "postgres-copy", err
	}
	return ids, "postgres-copy", nil
}
func (d *PostgreSQL) BulkInsertMunicipis(ctx context.Context, rows []Municipi) ([]int, string, error) {
	if len(rows) == 0 {
		return nil, "postgres-copy", nil
	}
	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, "postgres-copy", err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
        CREATE TEMP TABLE tmp_municipis_import (
            import_seq INTEGER,
            nom TEXT,
            municipi_id INTEGER,
            tipus TEXT,
            nivell_administratiu_id_1 INTEGER,
            nivell_administratiu_id_2 INTEGER,
            nivell_administratiu_id_3 INTEGER,
            nivell_administratiu_id_4 INTEGER,
            nivell_administratiu_id_5 INTEGER,
            nivell_administratiu_id_6 INTEGER,
            nivell_administratiu_id_7 INTEGER,
            codi_postal TEXT,
            latitud DOUBLE PRECISION,
            longitud DOUBLE PRECISION,
            what3words TEXT,
            web TEXT,
            wikipedia TEXT,
            altres TEXT,
            estat TEXT,
            created_by INTEGER,
            moderation_status TEXT,
            moderated_by INTEGER,
            moderated_at TIMESTAMP,
            moderation_notes TEXT
        ) ON COMMIT DROP`)
	if err != nil {
		return nil, "postgres-copy", err
	}
	stmt, err := tx.PrepareContext(ctx, pq.CopyIn("tmp_municipis_import",
		"import_seq",
		"nom",
		"municipi_id",
		"tipus",
		"nivell_administratiu_id_1",
		"nivell_administratiu_id_2",
		"nivell_administratiu_id_3",
		"nivell_administratiu_id_4",
		"nivell_administratiu_id_5",
		"nivell_administratiu_id_6",
		"nivell_administratiu_id_7",
		"codi_postal",
		"latitud",
		"longitud",
		"what3words",
		"web",
		"wikipedia",
		"altres",
		"estat",
		"created_by",
		"moderation_status",
		"moderated_by",
		"moderated_at",
		"moderation_notes",
	))
	if err != nil {
		return nil, "postgres-copy", err
	}
	for i, m := range rows {
		if _, err := stmt.Exec(
			i,
			m.Nom,
			m.MunicipiID,
			m.Tipus,
			m.NivellAdministratiuID[0],
			m.NivellAdministratiuID[1],
			m.NivellAdministratiuID[2],
			m.NivellAdministratiuID[3],
			m.NivellAdministratiuID[4],
			m.NivellAdministratiuID[5],
			m.NivellAdministratiuID[6],
			m.CodiPostal,
			m.Latitud,
			m.Longitud,
			m.What3Words,
			m.Web,
			m.Wikipedia,
			m.Altres,
			m.Estat,
			m.CreatedBy,
			m.ModeracioEstat,
			m.ModeratedBy,
			m.ModeratedAt,
			m.ModeracioMotiu,
		); err != nil {
			_ = stmt.Close()
			return nil, "postgres-copy", err
		}
	}
	if _, err := stmt.Exec(); err != nil {
		_ = stmt.Close()
		return nil, "postgres-copy", err
	}
	if err := stmt.Close(); err != nil {
		return nil, "postgres-copy", err
	}
	rowsRes, err := tx.QueryContext(ctx, `
        INSERT INTO municipis (
            nom, municipi_id, tipus,
            nivell_administratiu_id_1, nivell_administratiu_id_2, nivell_administratiu_id_3,
            nivell_administratiu_id_4, nivell_administratiu_id_5, nivell_administratiu_id_6, nivell_administratiu_id_7,
            codi_postal, latitud, longitud, what3words, web, wikipedia, altres, estat,
            created_by, moderation_status, moderated_by, moderated_at, moderation_notes,
            data_creacio, ultima_modificacio
        )
        SELECT
            nom, municipi_id, tipus,
            nivell_administratiu_id_1, nivell_administratiu_id_2, nivell_administratiu_id_3,
            nivell_administratiu_id_4, nivell_administratiu_id_5, nivell_administratiu_id_6, nivell_administratiu_id_7,
            codi_postal, latitud, longitud, what3words, web, wikipedia, altres, estat,
            created_by, moderation_status, moderated_by, moderated_at, moderation_notes,
            NOW(), NOW()
        FROM tmp_municipis_import
        ORDER BY import_seq
        RETURNING id`)
	if err != nil {
		return nil, "postgres-copy", err
	}
	defer rowsRes.Close()
	ids := make([]int, 0, len(rows))
	for rowsRes.Next() {
		var id int
		if err := rowsRes.Scan(&id); err != nil {
			return nil, "postgres-copy", err
		}
		ids = append(ids, id)
	}
	if err := rowsRes.Err(); err != nil {
		return nil, "postgres-copy", err
	}
	if err := tx.Commit(); err != nil {
		return nil, "postgres-copy", err
	}
	return ids, "postgres-copy", nil
}
func (d *PostgreSQL) BulkUpdateMunicipiParents(ctx context.Context, updates []MunicipiParentUpdate) (string, error) {
	if len(updates) == 0 {
		return "postgres-copy", nil
	}
	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return "postgres-copy", err
	}
	defer tx.Rollback()
	for i := 0; i < len(updates); i += bulkTerritoriBatchSize {
		end := i + bulkTerritoriBatchSize
		if end > len(updates) {
			end = len(updates)
		}
		batch := updates[i:end]
		query, args := buildBulkUpdateMunicipiParents(d.help.style, batch)
		if query == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return "postgres-copy", err
		}
	}
	if err := tx.Commit(); err != nil {
		return "postgres-copy", err
	}
	return "postgres-copy", nil
}
func (d *PostgreSQL) UpdateMunicipi(m *Municipi) error {
	return d.help.updateMunicipi(m)
}
func (d *PostgreSQL) UpdateMunicipiModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateMunicipiModeracio(id, estat, motiu, moderatorID)
}
func (d *PostgreSQL) ListCodisPostals(municipiID int) ([]CodiPostal, error) {
	return d.help.listCodisPostals(municipiID)
}
func (d *PostgreSQL) SaveCodiPostal(cp *CodiPostal) (int, error) {
	return d.help.saveCodiPostal(cp)
}
func (d *PostgreSQL) ListNomsHistorics(entitatTipus string, entitatID int) ([]NomHistoric, error) {
	return d.help.listNomsHistorics(entitatTipus, entitatID)
}
func (d *PostgreSQL) SaveNomHistoric(nh *NomHistoric) (int, error) {
	return d.help.saveNomHistoric(nh)
}

// Entitats eclesiàstiques
func (d *PostgreSQL) ListArquebisbats(f ArquebisbatFilter) ([]ArquebisbatRow, error) {
	return d.help.listArquebisbats(f)
}

func (d *PostgreSQL) CountArquebisbats(f ArquebisbatFilter) (int, error) {
	return d.help.countArquebisbats(f)
}
func (d *PostgreSQL) GetArquebisbat(id int) (*Arquebisbat, error) {
	return d.help.getArquebisbat(id)
}
func (d *PostgreSQL) CreateArquebisbat(ae *Arquebisbat) (int, error) {
	return d.help.createArquebisbat(ae)
}
func (d *PostgreSQL) BulkInsertArquebisbats(ctx context.Context, rows []Arquebisbat) ([]int, string, error) {
	if len(rows) == 0 {
		return nil, "postgres-copy", nil
	}
	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, "postgres-copy", err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
        CREATE TEMP TABLE tmp_arquebisbats_import (
            import_seq INTEGER,
            nom TEXT,
            tipus_entitat TEXT,
            pais_id INTEGER,
            nivell INTEGER,
            parent_id INTEGER,
            any_inici INTEGER,
            any_fi INTEGER,
            web TEXT,
            web_arxiu TEXT,
            web_wikipedia TEXT,
            territori TEXT,
            observacions TEXT,
            created_by INTEGER,
            moderation_status TEXT,
            moderated_by INTEGER,
            moderated_at TIMESTAMP,
            moderation_notes TEXT
        ) ON COMMIT DROP`)
	if err != nil {
		return nil, "postgres-copy", err
	}
	stmt, err := tx.PrepareContext(ctx, pq.CopyIn("tmp_arquebisbats_import",
		"import_seq",
		"nom",
		"tipus_entitat",
		"pais_id",
		"nivell",
		"parent_id",
		"any_inici",
		"any_fi",
		"web",
		"web_arxiu",
		"web_wikipedia",
		"territori",
		"observacions",
		"created_by",
		"moderation_status",
		"moderated_by",
		"moderated_at",
		"moderation_notes",
	))
	if err != nil {
		return nil, "postgres-copy", err
	}
	for i, a := range rows {
		if _, err := stmt.Exec(
			i,
			a.Nom,
			a.TipusEntitat,
			a.PaisID,
			a.Nivell,
			a.ParentID,
			a.AnyInici,
			a.AnyFi,
			a.Web,
			a.WebArxiu,
			a.WebWikipedia,
			a.Territori,
			a.Observacions,
			a.CreatedBy,
			a.ModeracioEstat,
			a.ModeratedBy,
			a.ModeratedAt,
			a.ModeracioMotiu,
		); err != nil {
			_ = stmt.Close()
			return nil, "postgres-copy", err
		}
	}
	if _, err := stmt.Exec(); err != nil {
		_ = stmt.Close()
		return nil, "postgres-copy", err
	}
	if err := stmt.Close(); err != nil {
		return nil, "postgres-copy", err
	}
	rowsRes, err := tx.QueryContext(ctx, `
        INSERT INTO arquebisbats (
            nom, tipus_entitat, pais_id, nivell, parent_id, any_inici, any_fi, web, web_arxiu, web_wikipedia,
            territori, observacions, created_by, moderation_status, moderated_by, moderated_at, moderation_notes,
            created_at, updated_at
        )
        SELECT
            nom, tipus_entitat, pais_id, nivell, parent_id, any_inici, any_fi, web, web_arxiu, web_wikipedia,
            territori, observacions, created_by, moderation_status, moderated_by, moderated_at, moderation_notes,
            NOW(), NOW()
        FROM tmp_arquebisbats_import
        ORDER BY import_seq
        RETURNING id`)
	if err != nil {
		return nil, "postgres-copy", err
	}
	defer rowsRes.Close()
	ids := make([]int, 0, len(rows))
	for rowsRes.Next() {
		var id int
		if err := rowsRes.Scan(&id); err != nil {
			return nil, "postgres-copy", err
		}
		ids = append(ids, id)
	}
	if err := rowsRes.Err(); err != nil {
		return nil, "postgres-copy", err
	}
	if err := tx.Commit(); err != nil {
		return nil, "postgres-copy", err
	}
	return ids, "postgres-copy", nil
}
func (d *PostgreSQL) UpdateArquebisbat(ae *Arquebisbat) error {
	return d.help.updateArquebisbat(ae)
}
func (d *PostgreSQL) UpdateArquebisbatModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateArquebisbatModeracio(id, estat, motiu, moderatorID)
}
func (d *PostgreSQL) BulkUpdateModeracioSimple(objectType, estat, motiu string, moderatorID int, ids []int) (int, error) {
	return d.help.bulkUpdateModeracioSimple(objectType, estat, motiu, moderatorID, ids)
}
func (d *PostgreSQL) ListArquebisbatMunicipis(munID int) ([]ArquebisbatMunicipi, error) {
	return d.help.listArquebisbatMunicipis(munID)
}
func (d *PostgreSQL) SaveArquebisbatMunicipi(am *ArquebisbatMunicipi) (int, error) {
	return d.help.saveArquebisbatMunicipi(am)
}
func (d *PostgreSQL) BulkInsertArquebisbatMunicipis(ctx context.Context, rows []ArquebisbatMunicipi) (string, error) {
	if len(rows) == 0 {
		return "postgres-copy", nil
	}
	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return "postgres-copy", err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
        CREATE TEMP TABLE tmp_arquebisbats_municipi_import (
            import_seq INTEGER,
            id_municipi INTEGER,
            id_arquevisbat INTEGER,
            any_inici INTEGER,
            any_fi INTEGER,
            motiu TEXT,
            font TEXT
        ) ON COMMIT DROP`)
	if err != nil {
		return "postgres-copy", err
	}
	stmt, err := tx.PrepareContext(ctx, pq.CopyIn("tmp_arquebisbats_municipi_import",
		"import_seq",
		"id_municipi",
		"id_arquevisbat",
		"any_inici",
		"any_fi",
		"motiu",
		"font",
	))
	if err != nil {
		return "postgres-copy", err
	}
	for i, r := range rows {
		if _, err := stmt.Exec(
			i,
			r.MunicipiID,
			r.ArquebisbatID,
			r.AnyInici,
			r.AnyFi,
			r.Motiu,
			r.Font,
		); err != nil {
			_ = stmt.Close()
			return "postgres-copy", err
		}
	}
	if _, err := stmt.Exec(); err != nil {
		_ = stmt.Close()
		return "postgres-copy", err
	}
	if err := stmt.Close(); err != nil {
		return "postgres-copy", err
	}
	if _, err := tx.ExecContext(ctx, `
        INSERT INTO arquebisbats_municipi (
            id_municipi, id_arquevisbat, any_inici, any_fi, motiu, font, created_at
        )
        SELECT
            id_municipi, id_arquevisbat, any_inici, any_fi, motiu, font, NOW()
        FROM tmp_arquebisbats_municipi_import
        ORDER BY import_seq`); err != nil {
		return "postgres-copy", err
	}
	if err := tx.Commit(); err != nil {
		return "postgres-copy", err
	}
	return "postgres-copy", nil
}

// Arxius
func (d *PostgreSQL) ListArxius(f ArxiuFilter) ([]ArxiuWithCount, error) {
	return d.help.listArxius(f)
}

func (d *PostgreSQL) CountArxius(f ArxiuFilter) (int, error) {
	return d.help.countArxius(f)
}
func (d *PostgreSQL) CountPaisos() (int, error) {
	return d.help.countPaisos()
}
func (d *PostgreSQL) GetArxiu(id int) (*Arxiu, error) {
	return d.help.getArxiu(id)
}
func (d *PostgreSQL) CreateArxiu(a *Arxiu) (int, error) {
	return d.help.createArxiu(a)
}
func (d *PostgreSQL) BulkInsertArxius(ctx context.Context, rows []Arxiu) ([]int, string, error) {
	if len(rows) == 0 {
		return nil, "postgres-copy", nil
	}
	d.help.ensureArxiuExtraColumns()
	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, "postgres-copy", err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
        CREATE TEMP TABLE tmp_arxius_import (
            import_seq INTEGER,
            nom TEXT,
            tipus TEXT,
            municipi_id INTEGER,
            entitat_eclesiastica_id INTEGER,
            adreca TEXT,
            ubicacio TEXT,
            what3words TEXT,
            web TEXT,
            acces TEXT,
            notes TEXT,
            accepta_donacions BOOLEAN,
            donacions_url TEXT,
            created_by INTEGER,
            moderation_status TEXT,
            moderated_by INTEGER,
            moderated_at TIMESTAMP,
            moderation_notes TEXT
        ) ON COMMIT DROP`)
	if err != nil {
		return nil, "postgres-copy", err
	}
	stmt, err := tx.PrepareContext(ctx, pq.CopyIn("tmp_arxius_import",
		"import_seq",
		"nom",
		"tipus",
		"municipi_id",
		"entitat_eclesiastica_id",
		"adreca",
		"ubicacio",
		"what3words",
		"web",
		"acces",
		"notes",
		"accepta_donacions",
		"donacions_url",
		"created_by",
		"moderation_status",
		"moderated_by",
		"moderated_at",
		"moderation_notes",
	))
	if err != nil {
		return nil, "postgres-copy", err
	}
	for i, a := range rows {
		if _, err := stmt.Exec(
			i,
			a.Nom,
			a.Tipus,
			a.MunicipiID,
			a.EntitatEclesiasticaID,
			a.Adreca,
			a.Ubicacio,
			a.What3Words,
			a.Web,
			a.Acces,
			a.Notes,
			a.AcceptaDonacions,
			a.DonacionsURL,
			a.CreatedBy,
			a.ModeracioEstat,
			a.ModeratedBy,
			a.ModeratedAt,
			a.ModeracioMotiu,
		); err != nil {
			_ = stmt.Close()
			return nil, "postgres-copy", err
		}
	}
	if _, err := stmt.Exec(); err != nil {
		_ = stmt.Close()
		return nil, "postgres-copy", err
	}
	if err := stmt.Close(); err != nil {
		return nil, "postgres-copy", err
	}
	rowsRes, err := tx.QueryContext(ctx, `
        INSERT INTO arxius (
            nom, tipus, municipi_id, entitat_eclesiastica_id, adreca, ubicacio, what3words, web, acces,
            notes, accepta_donacions, donacions_url, created_by, moderation_status, moderated_by, moderated_at,
            moderation_notes, created_at, updated_at
        )
        SELECT
            nom, tipus, municipi_id, entitat_eclesiastica_id, adreca, ubicacio, what3words, web, acces,
            notes, accepta_donacions, donacions_url, created_by, moderation_status, moderated_by, moderated_at,
            moderation_notes, NOW(), NOW()
        FROM tmp_arxius_import
        ORDER BY import_seq
        RETURNING id`)
	if err != nil {
		return nil, "postgres-copy", err
	}
	defer rowsRes.Close()
	ids := make([]int, 0, len(rows))
	for rowsRes.Next() {
		var id int
		if err := rowsRes.Scan(&id); err != nil {
			return nil, "postgres-copy", err
		}
		ids = append(ids, id)
	}
	if err := rowsRes.Err(); err != nil {
		return nil, "postgres-copy", err
	}
	if err := tx.Commit(); err != nil {
		return nil, "postgres-copy", err
	}
	return ids, "postgres-copy", nil
}
func (d *PostgreSQL) UpdateArxiu(a *Arxiu) error {
	return d.help.updateArxiu(a)
}
func (d *PostgreSQL) UpdateArxiuModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateArxiuModeracio(id, estat, motiu, moderatorID)
}
func (d *PostgreSQL) DeleteArxiu(id int) error {
	return d.help.deleteArxiu(id)
}
func (d *PostgreSQL) InsertArxiuDonacioClick(arxiuID int, userID *int) error {
	return d.help.insertArxiuDonacioClick(arxiuID, userID)
}
func (d *PostgreSQL) CountArxiuDonacioClicks(arxiuID int) (int, error) {
	return d.help.countArxiuDonacioClicks(arxiuID)
}
func (d *PostgreSQL) ListArxiuLlibres(arxiuID int) ([]ArxiuLlibreDetail, error) {
	return d.help.listArxiuLlibres(arxiuID)
}
func (d *PostgreSQL) ListLlibreArxius(llibreID int) ([]ArxiuLlibreDetail, error) {
	return d.help.listLlibreArxius(llibreID)
}
func (d *PostgreSQL) ListLlibreArxiusByLlibreIDs(llibreIDs []int) (map[int][]ArxiuLlibreDetail, error) {
	return d.help.listLlibreArxiusByLlibreIDs(llibreIDs)
}

func (d *PostgreSQL) ListLlibreURLs(llibreID int) ([]LlibreURL, error) {
	return d.help.listLlibreURLs(llibreID)
}

func (d *PostgreSQL) AddLlibreURL(link *LlibreURL) error {
	return d.help.addLlibreURL(link)
}

func (d *PostgreSQL) DeleteLlibreURL(id int) error {
	return d.help.deleteLlibreURL(id)
}
func (d *PostgreSQL) AddArxiuLlibre(arxiuID, llibreID int, signatura, urlOverride string) error {
	return d.help.addArxiuLlibre(arxiuID, llibreID, signatura, urlOverride)
}
func (d *PostgreSQL) UpdateArxiuLlibre(arxiuID, llibreID int, signatura, urlOverride string) error {
	return d.help.updateArxiuLlibre(arxiuID, llibreID, signatura, urlOverride)
}
func (d *PostgreSQL) DeleteArxiuLlibre(arxiuID, llibreID int) error {
	return d.help.deleteArxiuLlibre(arxiuID, llibreID)
}
func (d *PostgreSQL) SearchLlibresSimple(q string, limit int) ([]LlibreSimple, error) {
	return d.help.searchLlibresSimple(q, limit)
}
func (d *PostgreSQL) ListLlibres(f LlibreFilter) ([]LlibreRow, error) {
	return d.help.listLlibres(f)
}

func (d *PostgreSQL) CountLlibres(f LlibreFilter) (int, error) {
	return d.help.countLlibres(f)
}
func (d *PostgreSQL) CountIndexedRegistres(status string) (int, error) {
	return d.help.countIndexedRegistres(status)
}
func (d *PostgreSQL) GetLlibre(id int) (*Llibre, error) {
	return d.help.getLlibre(id)
}
func (d *PostgreSQL) GetLlibresByIDs(ids []int) (map[int]*Llibre, error) {
	return d.help.getLlibresByIDs(ids)
}
func (d *PostgreSQL) CreateLlibre(l *Llibre) (int, error) {
	return d.help.createLlibre(l)
}
func (d *PostgreSQL) BulkInsertLlibres(ctx context.Context, rows []Llibre) ([]int, string, error) {
	if len(rows) == 0 {
		return nil, "postgres-copy", nil
	}
	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, "postgres-copy", err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
        CREATE TEMP TABLE tmp_llibres_import (
            import_seq INTEGER,
            arquevisbat_id INTEGER,
            municipi_id INTEGER,
            nom_esglesia TEXT,
            codi_digital TEXT,
            codi_fisic TEXT,
            titol TEXT,
            tipus_llibre TEXT,
            cronologia TEXT,
            volum TEXT,
            abat TEXT,
            contingut TEXT,
            llengua TEXT,
            requeriments_tecnics TEXT,
            unitat_catalogacio TEXT,
            unitat_instalacio TEXT,
            pagines INTEGER,
            url_base TEXT,
            url_imatge_prefix TEXT,
            pagina TEXT,
            indexacio_completa BOOLEAN,
            created_by INTEGER,
            moderation_status TEXT,
            moderated_by INTEGER,
            moderated_at TIMESTAMP,
            moderation_notes TEXT
        ) ON COMMIT DROP`)
	if err != nil {
		return nil, "postgres-copy", err
	}
	stmt, err := tx.PrepareContext(ctx, pq.CopyIn("tmp_llibres_import",
		"import_seq",
		"arquevisbat_id",
		"municipi_id",
		"nom_esglesia",
		"codi_digital",
		"codi_fisic",
		"titol",
		"tipus_llibre",
		"cronologia",
		"volum",
		"abat",
		"contingut",
		"llengua",
		"requeriments_tecnics",
		"unitat_catalogacio",
		"unitat_instalacio",
		"pagines",
		"url_base",
		"url_imatge_prefix",
		"pagina",
		"indexacio_completa",
		"created_by",
		"moderation_status",
		"moderated_by",
		"moderated_at",
		"moderation_notes",
	))
	if err != nil {
		return nil, "postgres-copy", err
	}
	for i, l := range rows {
		var arquebisbat interface{}
		if l.ArquebisbatID > 0 {
			arquebisbat = l.ArquebisbatID
		}
		if _, err := stmt.Exec(
			i,
			arquebisbat,
			l.MunicipiID,
			l.NomEsglesia,
			l.CodiDigital,
			l.CodiFisic,
			l.Titol,
			l.TipusLlibre,
			l.Cronologia,
			l.Volum,
			l.Abat,
			l.Contingut,
			l.Llengua,
			l.Requeriments,
			l.UnitatCatalogacio,
			l.UnitatInstalacio,
			l.Pagines,
			l.URLBase,
			l.URLImatgePrefix,
			l.Pagina,
			l.IndexacioCompleta,
			l.CreatedBy,
			l.ModeracioEstat,
			l.ModeratedBy,
			l.ModeratedAt,
			l.ModeracioMotiu,
		); err != nil {
			_ = stmt.Close()
			return nil, "postgres-copy", err
		}
	}
	if _, err := stmt.Exec(); err != nil {
		_ = stmt.Close()
		return nil, "postgres-copy", err
	}
	if err := stmt.Close(); err != nil {
		return nil, "postgres-copy", err
	}
	rowsRes, err := tx.QueryContext(ctx, `
        INSERT INTO llibres (
            arquevisbat_id, municipi_id, nom_esglesia, codi_digital, codi_fisic, titol, tipus_llibre, cronologia,
            volum, abat, contingut, llengua, requeriments_tecnics, unitat_catalogacio, unitat_instalacio, pagines,
            url_base, url_imatge_prefix, pagina, indexacio_completa, created_by, moderation_status, moderated_by,
            moderated_at, moderation_notes, created_at, updated_at
        )
        SELECT
            arquevisbat_id, municipi_id, nom_esglesia, codi_digital, codi_fisic, titol, tipus_llibre, cronologia,
            volum, abat, contingut, llengua, requeriments_tecnics, unitat_catalogacio, unitat_instalacio, pagines,
            url_base, url_imatge_prefix, pagina, indexacio_completa, created_by, moderation_status, moderated_by,
            moderated_at, moderation_notes, NOW(), NOW()
        FROM tmp_llibres_import
        ORDER BY import_seq
        RETURNING id`)
	if err != nil {
		return nil, "postgres-copy", err
	}
	defer rowsRes.Close()
	ids := make([]int, 0, len(rows))
	for rowsRes.Next() {
		var id int
		if err := rowsRes.Scan(&id); err != nil {
			return nil, "postgres-copy", err
		}
		ids = append(ids, id)
	}
	if err := rowsRes.Err(); err != nil {
		return nil, "postgres-copy", err
	}
	if err := tx.Commit(); err != nil {
		return nil, "postgres-copy", err
	}
	return ids, "postgres-copy", nil
}
func (d *PostgreSQL) BulkInsertArxiuLlibres(ctx context.Context, rows []ArxiuLlibreLink) (string, error) {
	if len(rows) == 0 {
		return "postgres-copy", nil
	}
	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return "postgres-copy", err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
        CREATE TEMP TABLE tmp_arxius_llibres_import (
            import_seq INTEGER,
            arxiu_id INTEGER,
            llibre_id INTEGER,
            signatura TEXT,
            url_override TEXT
        ) ON COMMIT DROP`)
	if err != nil {
		return "postgres-copy", err
	}
	stmt, err := tx.PrepareContext(ctx, pq.CopyIn("tmp_arxius_llibres_import",
		"import_seq",
		"arxiu_id",
		"llibre_id",
		"signatura",
		"url_override",
	))
	if err != nil {
		return "postgres-copy", err
	}
	for i, link := range rows {
		if _, err := stmt.Exec(i, link.ArxiuID, link.LlibreID, link.Signatura, link.URLOverride); err != nil {
			_ = stmt.Close()
			return "postgres-copy", err
		}
	}
	if _, err := stmt.Exec(); err != nil {
		_ = stmt.Close()
		return "postgres-copy", err
	}
	if err := stmt.Close(); err != nil {
		return "postgres-copy", err
	}
	if _, err := tx.ExecContext(ctx, `
        INSERT INTO arxius_llibres (arxiu_id, llibre_id, signatura, url_override)
        SELECT arxiu_id, llibre_id, signatura, url_override
        FROM tmp_arxius_llibres_import
        ORDER BY import_seq`); err != nil {
		return "postgres-copy", err
	}
	if err := tx.Commit(); err != nil {
		return "postgres-copy", err
	}
	return "postgres-copy", nil
}
func (d *PostgreSQL) BulkInsertLlibreURLs(ctx context.Context, rows []LlibreURL) (string, error) {
	if len(rows) == 0 {
		return "postgres-copy", nil
	}
	d.help.ensureLlibreURLColumns()
	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return "postgres-copy", err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
        CREATE TEMP TABLE tmp_llibres_urls_import (
            import_seq INTEGER,
            llibre_id INTEGER,
            arxiu_id INTEGER,
            llibre_ref_id INTEGER,
            url TEXT,
            tipus TEXT,
            descripcio TEXT,
            created_by INTEGER
        ) ON COMMIT DROP`)
	if err != nil {
		return "postgres-copy", err
	}
	stmt, err := tx.PrepareContext(ctx, pq.CopyIn("tmp_llibres_urls_import",
		"import_seq",
		"llibre_id",
		"arxiu_id",
		"llibre_ref_id",
		"url",
		"tipus",
		"descripcio",
		"created_by",
	))
	if err != nil {
		return "postgres-copy", err
	}
	for i, link := range rows {
		if _, err := stmt.Exec(
			i,
			link.LlibreID,
			link.ArxiuID,
			link.LlibreRefID,
			link.URL,
			link.Tipus,
			link.Descripcio,
			link.CreatedBy,
		); err != nil {
			_ = stmt.Close()
			return "postgres-copy", err
		}
	}
	if _, err := stmt.Exec(); err != nil {
		_ = stmt.Close()
		return "postgres-copy", err
	}
	if err := stmt.Close(); err != nil {
		return "postgres-copy", err
	}
	if _, err := tx.ExecContext(ctx, `
        INSERT INTO llibres_urls (llibre_id, arxiu_id, llibre_ref_id, url, tipus, descripcio, created_by, created_at)
        SELECT llibre_id, arxiu_id, llibre_ref_id, url, tipus, descripcio, created_by, NOW()
        FROM tmp_llibres_urls_import
        ORDER BY import_seq`); err != nil {
		return "postgres-copy", err
	}
	if err := tx.Commit(); err != nil {
		return "postgres-copy", err
	}
	return "postgres-copy", nil
}
func (d *PostgreSQL) UpdateLlibre(l *Llibre) error {
	return d.help.updateLlibre(l)
}
func (d *PostgreSQL) HasLlibreDuplicate(municipiID int, tipus, cronologia, codiDigital, codiFisic string, excludeID int) (bool, error) {
	return d.help.hasLlibreDuplicate(municipiID, tipus, cronologia, codiDigital, codiFisic, excludeID)
}
func (d *PostgreSQL) ResolveLlibresByCodes(municipiID int, tipus, cronologia string, codiDigitals, codiFisics []string) ([]LlibreResolveRow, error) {
	if municipiID <= 0 {
		return nil, nil
	}
	tipus = strings.TrimSpace(tipus)
	cronologia = strings.TrimSpace(cronologia)
	if tipus == "" || cronologia == "" {
		return nil, nil
	}
	digital := make([]string, 0, len(codiDigitals))
	for _, code := range codiDigitals {
		code = strings.TrimSpace(code)
		if code == "" {
			continue
		}
		digital = append(digital, code)
	}
	fisic := make([]string, 0, len(codiFisics))
	for _, code := range codiFisics {
		code = strings.TrimSpace(code)
		if code == "" {
			continue
		}
		fisic = append(fisic, code)
	}
	if len(digital) == 0 && len(fisic) == 0 {
		return nil, nil
	}
	conds := make([]string, 0, 2)
	args := []interface{}{municipiID, tipus, cronologia}
	argIdx := 4
	if len(digital) > 0 {
		conds = append(conds, fmt.Sprintf("codi_digital = ANY($%d)", argIdx))
		args = append(args, pq.Array(digital))
		argIdx++
	}
	if len(fisic) > 0 {
		conds = append(conds, fmt.Sprintf("codi_fisic = ANY($%d)", argIdx))
		args = append(args, pq.Array(fisic))
		argIdx++
	}
	if len(conds) == 0 {
		return nil, nil
	}
	query := `
        SELECT id, COALESCE(codi_digital, ''), COALESCE(codi_fisic, '')
        FROM llibres
        WHERE municipi_id = $1 AND tipus_llibre = $2 AND cronologia = $3 AND (` + strings.Join(conds, " OR ") + `)`
	rows, err := d.Conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []LlibreResolveRow
	for rows.Next() {
		var row LlibreResolveRow
		if err := rows.Scan(&row.ID, &row.CodiDigital, &row.CodiFisic); err != nil {
			return nil, err
		}
		res = append(res, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

func (d *PostgreSQL) ResolveLlibresByPayload(rows []LlibreResolveCandidate) ([]LlibreResolveMatch, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	ctx := context.Background()
	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
        CREATE TEMP TABLE tmp_llibres_resolve (
            municipi_id INT NOT NULL,
            tipus_llibre TEXT NOT NULL,
            cronologia TEXT NOT NULL,
            codi_digital TEXT,
            codi_fisic TEXT
        ) ON COMMIT DROP`); err != nil {
		return nil, err
	}
	stmt, err := tx.PrepareContext(ctx, pq.CopyIn(
		"tmp_llibres_resolve",
		"municipi_id",
		"tipus_llibre",
		"cronologia",
		"codi_digital",
		"codi_fisic",
	))
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if row.MunicipiID <= 0 {
			continue
		}
		tipus := strings.TrimSpace(row.TipusLlibre)
		cronologia := strings.TrimSpace(row.Cronologia)
		if tipus == "" || cronologia == "" {
			continue
		}
		var digital interface{}
		if code := strings.TrimSpace(row.CodiDigital); code != "" {
			digital = code
		}
		var fisic interface{}
		if code := strings.TrimSpace(row.CodiFisic); code != "" {
			fisic = code
		}
		if digital == nil && fisic == nil {
			continue
		}
		if _, err := stmt.Exec(row.MunicipiID, tipus, cronologia, digital, fisic); err != nil {
			_ = stmt.Close()
			return nil, err
		}
	}
	if _, err := stmt.Exec(); err != nil {
		_ = stmt.Close()
		return nil, err
	}
	if err := stmt.Close(); err != nil {
		return nil, err
	}
	query := `
        SELECT DISTINCT l.id, l.municipi_id, l.tipus_llibre, l.cronologia,
               COALESCE(l.codi_digital, ''), COALESCE(l.codi_fisic, '')
        FROM llibres l
        JOIN tmp_llibres_resolve t
          ON l.municipi_id = t.municipi_id
         AND l.tipus_llibre = t.tipus_llibre
         AND l.cronologia = t.cronologia
         AND (
              (t.codi_digital IS NOT NULL AND t.codi_digital <> '' AND l.codi_digital = t.codi_digital)
           OR (t.codi_fisic IS NOT NULL AND t.codi_fisic <> '' AND l.codi_fisic = t.codi_fisic)
         )`
	rowsResult, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rowsResult.Close()
	var res []LlibreResolveMatch
	for rowsResult.Next() {
		var row LlibreResolveMatch
		if err := rowsResult.Scan(&row.ID, &row.MunicipiID, &row.TipusLlibre, &row.Cronologia, &row.CodiDigital, &row.CodiFisic); err != nil {
			return nil, err
		}
		res = append(res, row)
	}
	if err := rowsResult.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return res, nil
}
func (d *PostgreSQL) GetLlibresIndexacioStats(ids []int) (map[int]LlibreIndexacioStats, error) {
	return d.help.getLlibresIndexacioStats(ids)
}
func (d *PostgreSQL) UpsertLlibreIndexacioStats(stats *LlibreIndexacioStats) error {
	return d.help.upsertLlibreIndexacioStats(stats)
}
func (d *PostgreSQL) UpdateLlibreModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateLlibreModeracio(id, estat, motiu, moderatorID)
}
func (d *PostgreSQL) ListLlibrePagines(llibreID int) ([]LlibrePagina, error) {
	return d.help.listLlibrePagines(llibreID)
}
func (d *PostgreSQL) SearchLlibrePagines(llibreID int, query string, limit int) ([]LlibrePagina, error) {
	return d.help.searchLlibrePagines(llibreID, query, limit)
}
func (d *PostgreSQL) GetLlibrePaginaByID(id int) (*LlibrePagina, error) {
	return d.help.getLlibrePaginaByID(id)
}
func (d *PostgreSQL) GetLlibrePaginaByNum(llibreID, num int) (*LlibrePagina, error) {
	return d.help.getLlibrePaginaByNum(llibreID, num)
}
func (d *PostgreSQL) SaveLlibrePagina(p *LlibrePagina) (int, error) {
	return d.help.saveLlibrePagina(p)
}
func (d *PostgreSQL) RecalcLlibrePagines(llibreID, total int) error {
	return d.help.recalcLlibrePagines(llibreID, total)
}

// Media
func (d *PostgreSQL) ListMediaAlbumsByOwner(userID int) ([]MediaAlbum, error) {
	return d.help.listMediaAlbumsByOwner(userID)
}
func (d *PostgreSQL) ListMediaAlbumsByLlibre(llibreID int) ([]MediaAlbum, error) {
	return d.help.listMediaAlbumsByLlibre(llibreID)
}
func (d *PostgreSQL) GetMediaAlbumByID(id int) (*MediaAlbum, error) {
	return d.help.getMediaAlbumByID(id)
}
func (d *PostgreSQL) GetMediaAlbumByPublicID(publicID string) (*MediaAlbum, error) {
	return d.help.getMediaAlbumByPublicID(publicID)
}
func (d *PostgreSQL) CreateMediaAlbum(a *MediaAlbum) (int, error) {
	return d.help.createMediaAlbum(a)
}
func (d *PostgreSQL) ListMediaItemsByAlbum(albumID int) ([]MediaItem, error) {
	return d.help.listMediaItemsByAlbum(albumID)
}
func (d *PostgreSQL) ListMediaItemsByAlbumType(albumType, status string) ([]MediaItem, error) {
	return d.help.listMediaItemsByAlbumType(albumType, status)
}
func (d *PostgreSQL) GetMediaItemByID(id int) (*MediaItem, error) {
	return d.help.getMediaItemByID(id)
}
func (d *PostgreSQL) GetMediaItemByPublicID(publicID string) (*MediaItem, error) {
	return d.help.getMediaItemByPublicID(publicID)
}
func (d *PostgreSQL) CreateMediaItem(item *MediaItem) (int, error) {
	return d.help.createMediaItem(item)
}
func (d *PostgreSQL) UpdateMediaItemDerivativesStatus(itemID int, status string) error {
	return d.help.updateMediaItemDerivativesStatus(itemID, status)
}
func (d *PostgreSQL) ListMediaAlbumsByStatus(status string) ([]MediaAlbum, error) {
	return d.help.listMediaAlbumsByStatus(status)
}
func (d *PostgreSQL) ListMediaItemsByStatus(status string) ([]MediaItem, error) {
	return d.help.listMediaItemsByStatus(status)
}
func (d *PostgreSQL) CountMediaAlbumsByStatus(status string) (int, error) {
	return d.help.countMediaAlbumsByStatus(status)
}
func (d *PostgreSQL) CountMediaItemsByStatus(status string) (int, error) {
	return d.help.countMediaItemsByStatus(status)
}
func (d *PostgreSQL) ListMediaAlbumsModeracio(filter MediaModeracioFilter) ([]MediaAlbum, error) {
	return d.help.listMediaAlbumsModeracio(filter)
}
func (d *PostgreSQL) ListMediaItemsModeracio(filter MediaModeracioFilter) ([]MediaItem, error) {
	return d.help.listMediaItemsModeracio(filter)
}
func (d *PostgreSQL) CountMediaAlbumsModeracio(filter MediaModeracioFilter) (int, error) {
	return d.help.countMediaAlbumsModeracio(filter)
}
func (d *PostgreSQL) CountMediaItemsModeracio(filter MediaModeracioFilter) (int, error) {
	return d.help.countMediaItemsModeracio(filter)
}
func (d *PostgreSQL) UpdateMediaAlbumModeration(id int, status, visibility string, restrictedGroupID, accessPolicyID, creditCost, difficultyScore int, sourceType, notes string, moderatorID int) error {
	return d.help.updateMediaAlbumModeration(id, status, visibility, restrictedGroupID, accessPolicyID, creditCost, difficultyScore, sourceType, notes, moderatorID)
}
func (d *PostgreSQL) UpdateMediaItemModeration(id int, status string, creditCost int, notes string, moderatorID int) error {
	return d.help.updateMediaItemModeration(id, status, creditCost, notes, moderatorID)
}
func (d *PostgreSQL) GetUserCreditsBalance(userID int) (int, error) {
	return d.help.getUserCreditsBalance(userID)
}
func (d *PostgreSQL) InsertUserCreditsLedger(entry *UserCreditsLedgerEntry) (int, error) {
	return d.help.insertUserCreditsLedger(entry)
}
func (d *PostgreSQL) GetActiveMediaAccessGrant(userID, mediaItemID int) (*MediaAccessGrant, error) {
	return d.help.getActiveMediaAccessGrant(userID, mediaItemID)
}
func (d *PostgreSQL) GetMediaAccessGrantByToken(token string) (*MediaAccessGrant, error) {
	return d.help.getMediaAccessGrantByToken(token)
}
func (d *PostgreSQL) CreateMediaAccessGrant(grant *MediaAccessGrant) (int, error) {
	return d.help.createMediaAccessGrant(grant)
}
func (d *PostgreSQL) InsertMediaAccessLog(entry *MediaAccessLog) (int, error) {
	return d.help.insertMediaAccessLog(entry)
}
func (d *PostgreSQL) ListMediaItemLinksByPagina(paginaID int) ([]MediaItemPageLink, error) {
	return d.help.listMediaItemLinksByPagina(paginaID)
}
func (d *PostgreSQL) ListMediaItemLinksByAlbum(albumID int) ([]MediaItemPageLink, error) {
	return d.help.listMediaItemLinksByAlbum(albumID)
}
func (d *PostgreSQL) UpsertMediaItemPageLink(mediaItemID, llibreID, paginaID, pageOrder int, notes string) error {
	return d.help.upsertMediaItemPageLink(mediaItemID, llibreID, paginaID, pageOrder, notes)
}
func (d *PostgreSQL) DeleteMediaItemPageLink(mediaItemID, paginaID int) error {
	return d.help.deleteMediaItemPageLink(mediaItemID, paginaID)
}
func (d *PostgreSQL) CountMediaItemLinksByAlbum(albumID int) (map[int]int, error) {
	return d.help.countMediaItemLinksByAlbum(albumID)
}
func (d *PostgreSQL) SearchMediaItems(query string, limit int) ([]MediaItemSearchRow, error) {
	return d.help.searchMediaItems(query, limit)
}

func (d *PostgreSQL) ListTranscripcionsRaw(llibreID int, f TranscripcioFilter) ([]TranscripcioRaw, error) {
	return d.help.listTranscripcionsRaw(llibreID, f)
}
func (d *PostgreSQL) ListTranscripcionsRawGlobal(f TranscripcioFilter) ([]TranscripcioRaw, error) {
	return d.help.listTranscripcionsRawGlobal(f)
}
func (d *PostgreSQL) ListTranscripcionsRawByIDs(ids []int) ([]TranscripcioRaw, error) {
	return d.help.listTranscripcionsRawByIDs(ids)
}
func (d *PostgreSQL) CountTranscripcionsRaw(llibreID int, f TranscripcioFilter) (int, error) {
	return d.help.countTranscripcionsRaw(llibreID, f)
}
func (d *PostgreSQL) CountTranscripcionsRawGlobal(f TranscripcioFilter) (int, error) {
	return d.help.countTranscripcionsRawGlobal(f)
}
func (d *PostgreSQL) CountTranscripcionsRawByPageValue(llibreID int, pageValue string) (int, error) {
	return d.help.countTranscripcionsRawByPageValue(llibreID, pageValue)
}

func (d *PostgreSQL) ListTranscripcionsRawByPageValue(llibreID int, pageValue string) ([]TranscripcioRaw, error) {
	return d.help.listTranscripcionsRawByPageValue(llibreID, pageValue)
}
func (d *PostgreSQL) GetTranscripcioRaw(id int) (*TranscripcioRaw, error) {
	return d.help.getTranscripcioRaw(id)
}
func (d *PostgreSQL) CreateTranscripcioRaw(t *TranscripcioRaw) (int, error) {
	return d.help.createTranscripcioRaw(t)
}
func (d *PostgreSQL) UpdateTranscripcioRaw(t *TranscripcioRaw) error {
	return d.help.updateTranscripcioRaw(t)
}
func (d *PostgreSQL) UpdateTranscripcioModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateTranscripcioModeracio(id, estat, motiu, moderatorID)
}
func (d *PostgreSQL) UpdateTranscripcioModeracioWithDemografia(id int, estat, motiu string, moderatorID int, municipiID, year int, tipus string, delta int) error {
	return d.help.updateTranscripcioModeracioWithDemografia(id, estat, motiu, moderatorID, municipiID, year, tipus, delta)
}
func (d *PostgreSQL) BulkUpdateTranscripcioModeracio(estat, motiu string, moderatorID int, ids []int) (int, error) {
	return d.help.bulkUpdateTranscripcioModeracio(estat, motiu, moderatorID, ids)
}
func (d *PostgreSQL) BulkUpdateTranscripcioModeracioWithDemografia(estat, motiu string, moderatorID int, ids []int, municipiID, year int, tipus string, delta int) (int, error) {
	return d.help.bulkUpdateTranscripcioModeracioWithDemografia(estat, motiu, moderatorID, ids, municipiID, year, tipus, delta)
}
func (d *PostgreSQL) DeleteTranscripcioRaw(id int) error {
	return d.help.deleteTranscripcioRaw(id)
}
func (d *PostgreSQL) ListTranscripcionsRawPageStats(llibreID int) ([]TranscripcioRawPageStat, error) {
	return d.help.listTranscripcionsRawPageStats(llibreID)
}
func (d *PostgreSQL) UpdateTranscripcionsRawPageStat(stat *TranscripcioRawPageStat) error {
	return d.help.updateTranscripcionsRawPageStat(stat)
}
func (d *PostgreSQL) RecalcTranscripcionsRawPageStats(llibreID int) error {
	return d.help.recalcTranscripcionsRawPageStats(llibreID)
}
func (d *PostgreSQL) SetTranscripcionsRawPageStatsIndexacio(llibreID int, value int) error {
	return d.help.setTranscripcionsRawPageStatsIndexacio(llibreID, value)
}

func (d *PostgreSQL) DeleteTranscripcionsByLlibre(llibreID int) error {
	return d.help.deleteTranscripcionsByLlibre(llibreID)
}
func (d *PostgreSQL) CreateTranscripcioRawChange(c *TranscripcioRawChange) (int, error) {
	return d.help.createTranscripcioRawChange(c)
}
func (d *PostgreSQL) ListTranscripcioRawChanges(transcripcioID int) ([]TranscripcioRawChange, error) {
	return d.help.listTranscripcioRawChanges(transcripcioID)
}
func (d *PostgreSQL) GetTranscripcioRawChange(id int) (*TranscripcioRawChange, error) {
	return d.help.getTranscripcioRawChange(id)
}
func (d *PostgreSQL) ListTranscripcioRawChangesPending() ([]TranscripcioRawChange, error) {
	return d.help.listTranscripcioRawChangesPending()
}
func (d *PostgreSQL) ListTranscripcioRawChangesPendingFiltered(filter TranscripcioFilter) ([]TranscripcioRawChange, error) {
	return d.help.listTranscripcioRawChangesPendingFiltered(filter)
}
func (d *PostgreSQL) CountTranscripcioRawChangesPending() (int, error) {
	return d.help.countTranscripcioRawChangesPending()
}
func (d *PostgreSQL) CountTranscripcioRawChangesPendingScoped(filter TranscripcioFilter) (int, error) {
	return d.help.countTranscripcioRawChangesPendingScoped(filter)
}
func (d *PostgreSQL) UpdateTranscripcioRawChangeModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateTranscripcioRawChangeModeracio(id, estat, motiu, moderatorID)
}
func (d *PostgreSQL) ListTranscripcioPersones(transcripcioID int) ([]TranscripcioPersonaRaw, error) {
	return d.help.listTranscripcioPersones(transcripcioID)
}
func (d *PostgreSQL) ListTranscripcioPersonesByTranscripcioIDs(transcripcioIDs []int) (map[int][]TranscripcioPersonaRaw, error) {
	return d.help.listTranscripcioPersonesByTranscripcioIDs(transcripcioIDs)
}
func (d *PostgreSQL) CreateTranscripcioPersona(p *TranscripcioPersonaRaw) (int, error) {
	return d.help.createTranscripcioPersona(p)
}
func (d *PostgreSQL) DeleteTranscripcioPersones(transcripcioID int) error {
	return d.help.deleteTranscripcioPersones(transcripcioID)
}
func (d *PostgreSQL) LinkTranscripcioPersona(personaRawID int, personaID int, linkedBy int) error {
	return d.help.linkTranscripcioPersona(personaRawID, personaID, linkedBy)
}
func (d *PostgreSQL) UnlinkTranscripcioPersona(personaRawID int, linkedBy int) error {
	return d.help.unlinkTranscripcioPersona(personaRawID, linkedBy)
}
func (d *PostgreSQL) ListTranscripcioAtributs(transcripcioID int) ([]TranscripcioAtributRaw, error) {
	return d.help.listTranscripcioAtributs(transcripcioID)
}
func (d *PostgreSQL) CreateTranscripcioAtribut(a *TranscripcioAtributRaw) (int, error) {
	return d.help.createTranscripcioAtribut(a)
}
func (d *PostgreSQL) BulkCreateTranscripcioRawBundles(rows []TranscripcioRawImportBundle) (TranscripcioRawImportBulkResult, error) {
	return d.help.bulkCreateTranscripcioRawBundles(rows)
}
func (d *PostgreSQL) DeleteTranscripcioAtributs(transcripcioID int) error {
	return d.help.deleteTranscripcioAtributs(transcripcioID)
}
func (d *PostgreSQL) GetTranscripcioDraft(userID, llibreID int) (*TranscripcioDraft, error) {
	return d.help.getTranscripcioDraft(userID, llibreID)
}
func (d *PostgreSQL) SaveTranscripcioDraft(userID, llibreID int, payload string) error {
	return d.help.saveTranscripcioDraft(userID, llibreID, payload)
}
func (d *PostgreSQL) DeleteTranscripcioDraft(userID, llibreID int) error {
	return d.help.deleteTranscripcioDraft(userID, llibreID)
}
func (d *PostgreSQL) UpsertTranscripcioMark(m *TranscripcioRawMark) error {
	return d.help.upsertTranscripcioMark(m)
}
func (d *PostgreSQL) DeleteTranscripcioMark(transcripcioID, userID int) error {
	return d.help.deleteTranscripcioMark(transcripcioID, userID)
}
func (d *PostgreSQL) ListTranscripcioMarks(transcripcioIDs []int) ([]TranscripcioRawMark, error) {
	return d.help.listTranscripcioMarks(transcripcioIDs)
}
func (d *PostgreSQL) GetWikiMark(objectType string, objectID int, userID int) (*WikiMark, error) {
	return d.help.getWikiMark(objectType, objectID, userID)
}
func (d *PostgreSQL) UpsertWikiMark(m *WikiMark) error {
	return d.help.upsertWikiMark(m)
}
func (d *PostgreSQL) DeleteWikiMark(objectType string, objectID int, userID int) error {
	return d.help.deleteWikiMark(objectType, objectID, userID)
}
func (d *PostgreSQL) ListWikiMarks(objectType string, objectIDs []int) ([]WikiMark, error) {
	return d.help.listWikiMarks(objectType, objectIDs)
}
func (d *PostgreSQL) IncWikiPublicCount(objectType string, objectID int, tipus string, delta int) error {
	return d.help.incWikiPublicCount(objectType, objectID, tipus, delta)
}
func (d *PostgreSQL) GetWikiPublicCounts(objectType string, objectID int) (map[string]int, error) {
	return d.help.getWikiPublicCounts(objectType, objectID)
}
func (d *PostgreSQL) CreateWikiChange(c *WikiChange) (int, error) {
	return d.help.createWikiChange(c)
}
func (d *PostgreSQL) GetWikiChange(id int) (*WikiChange, error) {
	return d.help.getWikiChange(id)
}
func (d *PostgreSQL) ListWikiChanges(objectType string, objectID int) ([]WikiChange, error) {
	return d.help.listWikiChanges(objectType, objectID)
}
func (d *PostgreSQL) ListWikiChangesPending(objectType string, limit int) ([]WikiChange, error) {
	return d.help.listWikiChangesPending(objectType, limit)
}
func (d *PostgreSQL) UpdateWikiChangeModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateWikiChangeModeracio(id, estat, motiu, moderatorID)
}
func (d *PostgreSQL) EnqueueWikiPending(change *WikiChange) error {
	return d.help.enqueueWikiPending(change)
}
func (d *PostgreSQL) DequeueWikiPending(changeID int) error {
	return d.help.dequeueWikiPending(changeID)
}
func (d *PostgreSQL) ListWikiPending(limit int) ([]WikiPendingItem, error) {
	return d.help.listWikiPending(limit)
}
func (d *PostgreSQL) ListWikiPendingChanges(limit, offset int) ([]WikiChange, []int, error) {
	return d.help.listWikiPendingChanges(limit, offset)
}
func (d *PostgreSQL) CountWikiPendingChangesByType() (map[string]int, error) {
	return d.help.countWikiPendingChangesByType()
}
func (d *PostgreSQL) CountWikiPendingMunicipiChangesScoped(filter MunicipiScopeFilter) (int, error) {
	return d.help.countWikiPendingMunicipiChangesScoped(filter)
}
func (d *PostgreSQL) CountWikiPendingArxiuChangesScoped(filter ArxiuFilter) (int, error) {
	return d.help.countWikiPendingArxiuChangesScoped(filter)
}
func (d *PostgreSQL) CountWikiPendingLlibreChangesScoped(filter LlibreFilter) (int, error) {
	return d.help.countWikiPendingLlibreChangesScoped(filter)
}

// Espai personal
func (d *PostgreSQL) CreateEspaiArbre(a *EspaiArbre) (int, error) { return d.help.createEspaiArbre(a) }
func (d *PostgreSQL) UpdateEspaiArbre(a *EspaiArbre) error        { return d.help.updateEspaiArbre(a) }
func (d *PostgreSQL) DeleteEspaiArbre(ownerID, treeID int) error {
	return d.help.deleteEspaiArbre(ownerID, treeID)
}
func (d *PostgreSQL) GetEspaiArbre(id int) (*EspaiArbre, error) { return d.help.getEspaiArbre(id) }
func (d *PostgreSQL) ListEspaiArbresByOwner(ownerID int) ([]EspaiArbre, error) {
	return d.help.listEspaiArbresByOwner(ownerID)
}
func (d *PostgreSQL) ListEspaiArbresPublic() ([]EspaiArbre, error) {
	return d.help.listEspaiArbresPublic()
}
func (d *PostgreSQL) CreateEspaiFontImportacio(f *EspaiFontImportacio) (int, error) {
	return d.help.createEspaiFontImportacio(f)
}
func (d *PostgreSQL) UpdateEspaiFontImportacio(f *EspaiFontImportacio) error {
	return d.help.updateEspaiFontImportacio(f)
}
func (d *PostgreSQL) GetEspaiFontImportacio(id int) (*EspaiFontImportacio, error) {
	return d.help.getEspaiFontImportacio(id)
}
func (d *PostgreSQL) DeleteEspaiFontImportacio(id int) error {
	return d.help.deleteEspaiFontImportacio(id)
}
func (d *PostgreSQL) GetEspaiFontImportacioByChecksum(ownerID int, checksum string) (*EspaiFontImportacio, error) {
	return d.help.getEspaiFontImportacioByChecksum(ownerID, checksum)
}
func (d *PostgreSQL) ListEspaiFontsImportacioByOwner(ownerID int) ([]EspaiFontImportacio, error) {
	return d.help.listEspaiFontsImportacioByOwner(ownerID)
}
func (d *PostgreSQL) CreateEspaiImport(i *EspaiImport) (int, error) {
	return d.help.createEspaiImport(i)
}
func (d *PostgreSQL) UpdateEspaiImportStatus(id int, status string, errorText, summaryJSON string) error {
	return d.help.updateEspaiImportStatus(id, status, errorText, summaryJSON)
}
func (d *PostgreSQL) UpdateEspaiImportProgress(id int, done, total int) error {
	return d.help.updateEspaiImportProgress(id, done, total)
}
func (d *PostgreSQL) GetEspaiImport(id int) (*EspaiImport, error) { return d.help.getEspaiImport(id) }
func (d *PostgreSQL) GetLatestEspaiImportByFont(ownerID, fontID int) (*EspaiImport, error) {
	return d.help.getLatestEspaiImportByFont(ownerID, fontID)
}
func (d *PostgreSQL) ListEspaiImportsByOwner(ownerID int) ([]EspaiImport, error) {
	return d.help.listEspaiImportsByOwner(ownerID)
}
func (d *PostgreSQL) ListEspaiImportsByArbre(arbreID int) ([]EspaiImport, error) {
	return d.help.listEspaiImportsByArbre(arbreID)
}
func (d *PostgreSQL) ListEspaiImportsByStatus(status string, limit int) ([]EspaiImport, error) {
	return d.help.listEspaiImportsByStatus(status, limit)
}
func (d *PostgreSQL) DeleteEspaiImportsByArbre(arbreID int) error {
	return d.help.deleteEspaiImportsByArbre(arbreID)
}
func (d *PostgreSQL) CountEspaiImportsByFont(fontID int) (int, error) {
	return d.help.countEspaiImportsByFont(fontID)
}
func (d *PostgreSQL) ClearEspaiTreeData(arbreID int) error {
	return d.help.clearEspaiTreeData(arbreID)
}
func (d *PostgreSQL) CreateEspaiPersona(p *EspaiPersona) (int, error) {
	return d.help.createEspaiPersona(p)
}
func (d *PostgreSQL) UpdateEspaiPersona(p *EspaiPersona) error { return d.help.updateEspaiPersona(p) }
func (d *PostgreSQL) UpdateEspaiPersonaVisibility(id int, visibility string) error {
	return d.help.updateEspaiPersonaVisibility(id, visibility)
}
func (d *PostgreSQL) GetEspaiPersona(id int) (*EspaiPersona, error) {
	return d.help.getEspaiPersona(id)
}
func (d *PostgreSQL) ListEspaiPersonesByArbre(arbreID int) ([]EspaiPersona, error) {
	return d.help.listEspaiPersonesByArbre(arbreID)
}
func (d *PostgreSQL) ListEspaiPersonesByArbreQuery(arbreID int, query string, limit, offset int) ([]EspaiPersona, error) {
	return d.help.listEspaiPersonesByArbreQuery(arbreID, query, limit, offset)
}
func (d *PostgreSQL) CountEspaiPersonesByArbre(arbreID int) (int, int, error) {
	return d.help.countEspaiPersonesByArbre(arbreID)
}
func (d *PostgreSQL) CountEspaiPersonesByArbreQuery(arbreID int, query string) (int, error) {
	return d.help.countEspaiPersonesByArbreQuery(arbreID, query)
}
func (d *PostgreSQL) ListEspaiPersonesByOwnerFilters(ownerID int, name, tree, visibility string, limit, offset int) ([]EspaiPersonaTreeRow, error) {
	return d.help.listEspaiPersonesByOwnerFilters(ownerID, name, tree, visibility, limit, offset)
}
func (d *PostgreSQL) CountEspaiPersonesByOwnerFilters(ownerID int, name, tree, visibility string) (int, error) {
	return d.help.countEspaiPersonesByOwnerFilters(ownerID, name, tree, visibility)
}
func (d *PostgreSQL) ListEspaiPersonesByOwnerDataFilters(ownerID int, filter EspaiPersonaDataFilter, limit, offset int) ([]EspaiPersonaTreeRow, error) {
	return d.help.listEspaiPersonesByOwnerDataFilters(ownerID, filter, limit, offset)
}
func (d *PostgreSQL) CountEspaiPersonesByOwnerDataFilters(ownerID int, filter EspaiPersonaDataFilter) (int, error) {
	return d.help.countEspaiPersonesByOwnerDataFilters(ownerID, filter)
}
func (d *PostgreSQL) CreateEspaiRelacio(r *EspaiRelacio) (int, error) {
	return d.help.createEspaiRelacio(r)
}
func (d *PostgreSQL) ListEspaiRelacionsByArbre(arbreID int) ([]EspaiRelacio, error) {
	return d.help.listEspaiRelacionsByArbre(arbreID)
}
func (d *PostgreSQL) CountEspaiRelacionsByArbre(arbreID int) (int, error) {
	return d.help.countEspaiRelacionsByArbre(arbreID)
}
func (d *PostgreSQL) CountEspaiRelacionsByArbreType(arbreID int, relationType string) (int, error) {
	return d.help.countEspaiRelacionsByArbreType(arbreID, relationType)
}
func (d *PostgreSQL) CreateEspaiEvent(ev *EspaiEvent) (int, error) {
	return d.help.createEspaiEvent(ev)
}
func (d *PostgreSQL) ListEspaiEventsByPersona(personaID int) ([]EspaiEvent, error) {
	return d.help.listEspaiEventsByPersona(personaID)
}
func (d *PostgreSQL) DeleteEspaiEventsByArbreSource(arbreID int, source string) error {
	return d.help.deleteEspaiEventsByArbreSource(arbreID, source)
}
func (d *PostgreSQL) CreateEspaiCoincidencia(c *EspaiCoincidencia) (int, error) {
	return d.help.createEspaiCoincidencia(c)
}
func (d *PostgreSQL) UpdateEspaiCoincidenciaStatus(id int, status string) error {
	return d.help.updateEspaiCoincidenciaStatus(id, status)
}
func (d *PostgreSQL) GetEspaiCoincidencia(id int) (*EspaiCoincidencia, error) {
	return d.help.getEspaiCoincidencia(id)
}
func (d *PostgreSQL) GetEspaiCoincidenciaByTarget(ownerID, personaID int, targetType string, targetID int) (*EspaiCoincidencia, error) {
	return d.help.getEspaiCoincidenciaByTarget(ownerID, personaID, targetType, targetID)
}
func (d *PostgreSQL) ListEspaiCoincidenciesByOwner(ownerID int) ([]EspaiCoincidencia, error) {
	return d.help.listEspaiCoincidenciesByOwner(ownerID)
}
func (d *PostgreSQL) CreateEspaiCoincidenciaDecision(dec *EspaiCoincidenciaDecision) (int, error) {
	return d.help.createEspaiCoincidenciaDecision(dec)
}
func (d *PostgreSQL) ListEspaiCoincidenciaDecisions(coincidenciaID int) ([]EspaiCoincidenciaDecision, error) {
	return d.help.listEspaiCoincidenciaDecisions(coincidenciaID)
}
func (d *PostgreSQL) CreateEspaiIntegracioGramps(i *EspaiIntegracioGramps) (int, error) {
	return d.help.createEspaiIntegracioGramps(i)
}
func (d *PostgreSQL) UpdateEspaiIntegracioGramps(i *EspaiIntegracioGramps) error {
	return d.help.updateEspaiIntegracioGramps(i)
}
func (d *PostgreSQL) GetEspaiIntegracioGramps(id int) (*EspaiIntegracioGramps, error) {
	return d.help.getEspaiIntegracioGramps(id)
}
func (d *PostgreSQL) ListEspaiIntegracionsGrampsByOwner(ownerID int) ([]EspaiIntegracioGramps, error) {
	return d.help.listEspaiIntegracionsGrampsByOwner(ownerID)
}
func (d *PostgreSQL) ListEspaiIntegracionsGramps() ([]EspaiIntegracioGramps, error) {
	return d.help.listEspaiIntegracionsGramps()
}
func (d *PostgreSQL) CreateEspaiIntegracioGrampsLog(l *EspaiIntegracioGrampsLog) (int, error) {
	return d.help.createEspaiIntegracioGrampsLog(l)
}
func (d *PostgreSQL) ListEspaiIntegracioGrampsLogs(integracioID int, limit int) ([]EspaiIntegracioGrampsLog, error) {
	return d.help.listEspaiIntegracioGrampsLogs(integracioID, limit)
}
func (d *PostgreSQL) CreateEspaiNotification(n *EspaiNotification) (int, error) {
	return d.help.createEspaiNotification(n)
}
func (d *PostgreSQL) ListEspaiNotificationsByUser(userID int, status string, limit int) ([]EspaiNotification, error) {
	return d.help.listEspaiNotificationsByUser(userID, status, limit)
}
func (d *PostgreSQL) MarkEspaiNotificationRead(id int, userID int) error {
	return d.help.markEspaiNotificationRead(id, userID)
}
func (d *PostgreSQL) MarkEspaiNotificationsReadAll(userID int) error {
	return d.help.markEspaiNotificationsReadAll(userID)
}
func (d *PostgreSQL) GetEspaiNotificationPref(userID int) (*EspaiNotificationPref, error) {
	return d.help.getEspaiNotificationPref(userID)
}
func (d *PostgreSQL) UpsertEspaiNotificationPref(p *EspaiNotificationPref) error {
	return d.help.upsertEspaiNotificationPref(p)
}
func (d *PostgreSQL) CreateEspaiPrivacyAudit(a *EspaiPrivacyAudit) (int, error) {
	return d.help.createEspaiPrivacyAudit(a)
}
func (d *PostgreSQL) CreateEspaiGrup(g *EspaiGrup) (int, error) { return d.help.createEspaiGrup(g) }
func (d *PostgreSQL) GetEspaiGrup(id int) (*EspaiGrup, error)   { return d.help.getEspaiGrup(id) }
func (d *PostgreSQL) ListEspaiGrupsByOwner(ownerID int) ([]EspaiGrup, error) {
	return d.help.listEspaiGrupsByOwner(ownerID)
}
func (d *PostgreSQL) ListEspaiGrupsByUser(userID int) ([]EspaiGrup, error) {
	return d.help.listEspaiGrupsByUser(userID)
}
func (d *PostgreSQL) AddEspaiGrupMembre(m *EspaiGrupMembre) (int, error) {
	return d.help.addEspaiGrupMembre(m)
}
func (d *PostgreSQL) GetEspaiGrupMembre(grupID, userID int) (*EspaiGrupMembre, error) {
	return d.help.getEspaiGrupMembre(grupID, userID)
}
func (d *PostgreSQL) UpdateEspaiGrupMembre(m *EspaiGrupMembre) error {
	return d.help.updateEspaiGrupMembre(m)
}
func (d *PostgreSQL) ListEspaiGrupMembres(grupID int) ([]EspaiGrupMembre, error) {
	return d.help.listEspaiGrupMembres(grupID)
}
func (d *PostgreSQL) AddEspaiGrupArbre(a *EspaiGrupArbre) (int, error) {
	return d.help.addEspaiGrupArbre(a)
}
func (d *PostgreSQL) ListEspaiGrupArbres(grupID int) ([]EspaiGrupArbre, error) {
	return d.help.listEspaiGrupArbres(grupID)
}
func (d *PostgreSQL) UpdateEspaiGrupArbreStatus(grupID, arbreID int, status string) error {
	return d.help.updateEspaiGrupArbreStatus(grupID, arbreID, status)
}
func (d *PostgreSQL) CreateEspaiGrupConflicte(c *EspaiGrupConflicte) (int, error) {
	return d.help.createEspaiGrupConflicte(c)
}
func (d *PostgreSQL) UpdateEspaiGrupConflicteStatus(id int, status string, resolvedBy *int) error {
	return d.help.updateEspaiGrupConflicteStatus(id, status, resolvedBy)
}
func (d *PostgreSQL) ListEspaiGrupConflictes(grupID int) ([]EspaiGrupConflicte, error) {
	return d.help.listEspaiGrupConflictes(grupID)
}
func (d *PostgreSQL) CreateEspaiGrupCanvi(c *EspaiGrupCanvi) (int, error) {
	return d.help.createEspaiGrupCanvi(c)
}
func (d *PostgreSQL) ListEspaiGrupCanvis(grupID int, limit int) ([]EspaiGrupCanvi, error) {
	return d.help.listEspaiGrupCanvis(grupID, limit)
}
func (d *PostgreSQL) CreateCSVImportTemplate(t *CSVImportTemplate) (int, error) {
	return d.help.createCSVImportTemplate(t)
}
func (d *PostgreSQL) UpdateCSVImportTemplate(t *CSVImportTemplate) error {
	return d.help.updateCSVImportTemplate(t)
}
func (d *PostgreSQL) GetCSVImportTemplate(id int) (*CSVImportTemplate, error) {
	return d.help.getCSVImportTemplate(id)
}
func (d *PostgreSQL) ListCSVImportTemplates(filter CSVImportTemplateFilter) ([]CSVImportTemplate, error) {
	return d.help.listCSVImportTemplates(filter)
}
func (d *PostgreSQL) DeleteCSVImportTemplate(id int) error {
	return d.help.deleteCSVImportTemplate(id)
}
func (d *PostgreSQL) SearchPersones(f PersonaSearchFilter) ([]PersonaSearchResult, error) {
	return d.help.searchPersones(f)
}
func (d *PostgreSQL) ListRegistresByPersona(personaID int, tipus string) ([]PersonaRegistreRow, error) {
	return d.help.listRegistresByPersona(personaID, tipus)
}
func (d *PostgreSQL) GetPersonesByIDs(ids []int) (map[int]*Persona, error) {
	return d.help.getPersonesByIDs(ids)
}
func (d *PostgreSQL) FindBestBaptismeTranscripcioForPersona(personaID int) (int, bool, error) {
	return d.help.findBestBaptismeTranscripcioForPersona(personaID)
}
func (d *PostgreSQL) GetParentsFromTranscripcio(transcripcioID int) (int, int, error) {
	return d.help.getParentsFromTranscripcio(transcripcioID)
}

// Punts i activitat
func (d *PostgreSQL) ListPointsRules() ([]PointsRule, error) { return d.help.listPointsRules() }
func (d *PostgreSQL) GetPointsRule(id int) (*PointsRule, error) {
	return d.help.getPointsRule(id)
}
func (d *PostgreSQL) GetPointsRuleByCode(code string) (*PointsRule, error) {
	return d.help.getPointsRuleByCode(code)
}
func (d *PostgreSQL) SavePointsRule(r *PointsRule) (int, error) { return d.help.savePointsRule(r) }
func (d *PostgreSQL) ListUserIDs(limit, offset int) ([]int, error) {
	return d.help.listUserIDs(limit, offset)
}
func (d *PostgreSQL) GetUserActivity(id int) (*UserActivity, error) {
	return d.help.getUserActivity(id)
}
func (d *PostgreSQL) InsertUserActivity(a *UserActivity) (int, error) {
	return d.help.insertUserActivity(a)
}
func (d *PostgreSQL) BulkInsertUserActivities(ctx context.Context, rows []UserActivity) (string, error) {
	if len(rows) == 0 {
		return "postgres-batch", nil
	}
	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return "postgres-batch", err
	}
	defer tx.Rollback()
	for i := 0; i < len(rows); i += bulkActivityBatchSize {
		end := i + bulkActivityBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[i:end]
		query, args := buildBulkInsertUserActivities(d.help.style, d.help.nowFun, batch)
		if query == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return "postgres-batch", err
		}
	}
	if err := tx.Commit(); err != nil {
		return "postgres-batch", err
	}
	return "postgres-batch", nil
}
func (d *PostgreSQL) BulkUpdateUserActivityStatus(ids []int, status string, moderatedBy *int) error {
	return d.help.bulkUpdateUserActivityStatus(ids, status, moderatedBy)
}
func (d *PostgreSQL) UpdateUserActivityStatus(id int, status string, moderatedBy *int) error {
	return d.help.updateUserActivityStatus(id, status, moderatedBy)
}
func (d *PostgreSQL) ListUserActivityByUser(userID int, f ActivityFilter) ([]UserActivity, error) {
	return d.help.listUserActivityByUser(userID, f)
}
func (d *PostgreSQL) ListActivityByObject(objectType string, objectID int, status string) ([]UserActivity, error) {
	return d.help.listActivityByObject(objectType, objectID, status)
}
func (d *PostgreSQL) ListActivityByObjects(objectType string, objectIDs []int, status string) ([]UserActivity, error) {
	return d.help.listActivityByObjects(objectType, objectIDs, status)
}
func (d *PostgreSQL) AddPointsToUser(userID int, delta int) error {
	return d.help.addPointsToUser(userID, delta)
}
func (d *PostgreSQL) GetUserPoints(userID int) (*UserPoints, error) {
	return d.help.getUserPoints(userID)
}
func (d *PostgreSQL) RecalcUserPoints() error { return d.help.recalcUserPoints() }
func (d *PostgreSQL) GetRanking(f RankingFilter) ([]UserPoints, error) {
	return d.help.getRanking(f)
}
func (d *PostgreSQL) CountRanking(f RankingFilter) (int, error) {
	return d.help.countRanking(f)
}

// Achievements
func (d *PostgreSQL) ListAchievements() ([]Achievement, error) {
	return d.help.listAchievements()
}
func (d *PostgreSQL) ListEnabledAchievements() ([]Achievement, error) {
	return d.help.listEnabledAchievements()
}
func (d *PostgreSQL) GetAchievement(id int) (*Achievement, error) { return d.help.getAchievement(id) }
func (d *PostgreSQL) GetAchievementByCode(code string) (*Achievement, error) {
	return d.help.getAchievementByCode(code)
}
func (d *PostgreSQL) SaveAchievement(a *Achievement) (int, error) {
	return d.help.saveAchievement(a)
}
func (d *PostgreSQL) AwardAchievement(userID, achievementID int, status, metaJSON string) (bool, error) {
	return d.help.awardAchievement(userID, achievementID, status, metaJSON)
}
func (d *PostgreSQL) ListUserAchievements(userID int) ([]AchievementUserView, error) {
	return d.help.listUserAchievements(userID)
}
func (d *PostgreSQL) ListUserShowcase(userID int) ([]AchievementShowcaseView, error) {
	return d.help.listUserShowcase(userID)
}
func (d *PostgreSQL) SetUserShowcaseSlot(userID, achievementID, slot int) error {
	return d.help.setUserShowcaseSlot(userID, achievementID, slot)
}
func (d *PostgreSQL) ClearUserShowcaseSlot(userID, slot int) error {
	return d.help.clearUserShowcaseSlot(userID, slot)
}
func (d *PostgreSQL) IsAchievementEventActive(code string, at time.Time) (bool, error) {
	return d.help.isAchievementEventActive(code, at)
}
func (d *PostgreSQL) CountUserActivities(f AchievementActivityFilter) (int, error) {
	return d.help.countUserActivities(f)
}
func (d *PostgreSQL) CountUserActivitiesDistinctObject(f AchievementActivityFilter) (int, error) {
	return d.help.countUserActivitiesDistinctObject(f)
}
func (d *PostgreSQL) SumUserActivityPoints(f AchievementActivityFilter) (int, error) {
	return d.help.sumUserActivityPoints(f)
}
func (d *PostgreSQL) ListUserActivityDays(f AchievementActivityFilter) ([]time.Time, error) {
	return d.help.listUserActivityDays(f)
}

// Cognoms
func (d *PostgreSQL) ListCognoms(q string, limit, offset int) ([]Cognom, error) {
	return d.help.listCognoms(q, limit, offset)
}
func (d *PostgreSQL) GetCognom(id int) (*Cognom, error) { return d.help.getCognom(id) }
func (d *PostgreSQL) FindCognomIDByKey(key string) (int, error) {
	return d.help.findCognomIDByKey(key)
}
func (d *PostgreSQL) UpsertCognom(forma, key, origen, notes string, createdBy *int) (int, error) {
	return d.help.upsertCognom(forma, key, origen, notes, createdBy)
}
func (d *PostgreSQL) BulkEnsureCognoms(formsByKey map[string]string, origen, notes string, createdBy *int) (map[string]int, error) {
	return d.help.bulkEnsureCognoms(formsByKey, origen, notes, createdBy)
}
func (d *PostgreSQL) UpdateCognom(c *Cognom) error {
	return d.help.updateCognom(c)
}
func (d *PostgreSQL) ListCognomVariants(f CognomVariantFilter) ([]CognomVariant, error) {
	return d.help.listCognomVariants(f)
}
func (d *PostgreSQL) CountCognomVariants(f CognomVariantFilter) (int, error) {
	return d.help.countCognomVariants(f)
}
func (d *PostgreSQL) ResolveCognomPublicatByForma(forma string) (int, string, bool, error) {
	return d.help.resolveCognomPublicatByForma(forma)
}
func (d *PostgreSQL) ListCognomFormesPublicades(cognomID int) ([]string, error) {
	return d.help.listCognomFormesPublicades(cognomID)
}
func (d *PostgreSQL) CreateCognomVariant(v *CognomVariant) (int, error) {
	return d.help.createCognomVariant(v)
}
func (d *PostgreSQL) UpdateCognomVariantModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateCognomVariantModeracio(id, estat, motiu, moderatorID)
}

func (d *PostgreSQL) GetCognomRedirect(fromID int) (*CognomRedirect, error) {
	return d.help.getCognomRedirect(fromID)
}

func (d *PostgreSQL) ListCognomRedirects() ([]CognomRedirect, error) {
	return d.help.listCognomRedirects()
}

func (d *PostgreSQL) ListCognomRedirectsByTo(toID int) ([]CognomRedirect, error) {
	return d.help.listCognomRedirectsByTo(toID)
}

func (d *PostgreSQL) SetCognomRedirect(fromID, toID int, createdBy *int, reason string) error {
	return d.help.setCognomRedirect(fromID, toID, createdBy, reason)
}

func (d *PostgreSQL) DeleteCognomRedirect(fromID int) error {
	return d.help.deleteCognomRedirect(fromID)
}

func (d *PostgreSQL) CreateCognomRedirectSuggestion(s *CognomRedirectSuggestion) (int, error) {
	return d.help.createCognomRedirectSuggestion(s)
}

func (d *PostgreSQL) GetCognomRedirectSuggestion(id int) (*CognomRedirectSuggestion, error) {
	return d.help.getCognomRedirectSuggestion(id)
}

func (d *PostgreSQL) ListCognomRedirectSuggestions(f CognomRedirectSuggestionFilter) ([]CognomRedirectSuggestion, error) {
	return d.help.listCognomRedirectSuggestions(f)
}
func (d *PostgreSQL) CountCognomRedirectSuggestions(f CognomRedirectSuggestionFilter) (int, error) {
	return d.help.countCognomRedirectSuggestions(f)
}

func (d *PostgreSQL) UpdateCognomRedirectSuggestionModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateCognomRedirectSuggestionModeracio(id, estat, motiu, moderatorID)
}

func (d *PostgreSQL) CreateCognomReferencia(ref *CognomReferencia) (int, error) {
	return d.help.createCognomReferencia(ref)
}

func (d *PostgreSQL) ListCognomReferencies(f CognomReferenciaFilter) ([]CognomReferencia, error) {
	return d.help.listCognomReferencies(f)
}
func (d *PostgreSQL) CountCognomReferencies(f CognomReferenciaFilter) (int, error) {
	return d.help.countCognomReferencies(f)
}

func (d *PostgreSQL) UpdateCognomReferenciaModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateCognomReferenciaModeracio(id, estat, motiu, moderatorID)
}

// Cercador avançat
func (d *PostgreSQL) UpsertSearchDoc(doc *SearchDoc) error { return d.help.upsertSearchDoc(doc) }
func (d *PostgreSQL) BulkUpsertSearchDocs(docs []SearchDoc) error {
	return d.help.bulkUpsertSearchDocs(docs)
}
func (d *PostgreSQL) GetSearchDoc(entityType string, entityID int) (*SearchDoc, error) {
	return d.help.getSearchDoc(entityType, entityID)
}
func (d *PostgreSQL) DeleteSearchDoc(entityType string, entityID int) error {
	return d.help.deleteSearchDoc(entityType, entityID)
}
func (d *PostgreSQL) BulkDeleteSearchDocs(entityType string, entityIDs []int) error {
	return d.help.bulkDeleteSearchDocs(entityType, entityIDs)
}
func (d *PostgreSQL) SearchDocs(filter SearchQueryFilter) ([]SearchDocRow, int, SearchFacets, error) {
	return d.help.searchDocs(filter)
}
func (d *PostgreSQL) ReplaceAdminClosure(descendantMunicipiID int, entries []AdminClosureEntry) error {
	return d.help.replaceAdminClosure(descendantMunicipiID, entries)
}
func (d *PostgreSQL) ListAdminClosure(descendantMunicipiID int) ([]AdminClosureEntry, error) {
	return d.help.listAdminClosure(descendantMunicipiID)
}
func (d *PostgreSQL) UpsertCognomFreqMunicipiAny(cognomID, municipiID, anyDoc, freq int) error {
	return d.help.upsertCognomFreqMunicipiAny(cognomID, municipiID, anyDoc, freq)
}
func (d *PostgreSQL) ApplyCognomFreqMunicipiAnyDelta(cognomID, municipiID, anyDoc, delta int) error {
	return d.help.applyCognomFreqMunicipiAnyDelta(cognomID, municipiID, anyDoc, delta)
}
func (d *PostgreSQL) QueryCognomHeatmap(cognomID int, anyStart, anyEnd int) ([]CognomFreqRow, error) {
	return d.help.queryCognomHeatmap(cognomID, anyStart, anyEnd)
}

func (d *PostgreSQL) ListCognomImportRows(limit, offset int) ([]CognomImportRow, error) {
	return d.help.listCognomImportRows(limit, offset)
}

func (d *PostgreSQL) ListCognomStatsRows(limit, offset int) ([]CognomStatsRow, error) {
	return d.help.listCognomStatsRows(limit, offset)
}

func (d *PostgreSQL) RebuildCognomStats(cognomID int) error {
	return d.help.rebuildCognomStats(cognomID)
}

func (d *PostgreSQL) GetCognomStatsTotal(cognomID int) (*CognomStatsTotal, error) {
	return d.help.getCognomStatsTotal(cognomID)
}

func (d *PostgreSQL) ListCognomStatsAny(cognomID int, from, to int) ([]CognomStatsAnyRow, error) {
	return d.help.listCognomStatsAny(cognomID, from, to)
}

func (d *PostgreSQL) ListCognomStatsAnyDecade(cognomID int, from, to int) ([]CognomStatsAnyRow, error) {
	return d.help.listCognomStatsAnyDecade(cognomID, from, to)
}

func (d *PostgreSQL) ListCognomStatsAncestor(cognomID int, ancestorType string, level, any, limit int) ([]CognomStatsAncestorRow, error) {
	return d.help.listCognomStatsAncestor(cognomID, ancestorType, level, any, limit)
}

func (d *PostgreSQL) CountCognomStatsAncestorDistinct(cognomID int, ancestorType string, level, any int) (int, error) {
	return d.help.countCognomStatsAncestorDistinct(cognomID, ancestorType, level, any)
}

// Noms
func (d *PostgreSQL) UpsertNom(forma, key, notes string, createdBy *int) (int, error) {
	return d.help.upsertNom(forma, key, notes, createdBy)
}
func (d *PostgreSQL) BulkEnsureNoms(formsByKey map[string]string, notes string, createdBy *int) (map[string]int, error) {
	return d.help.bulkEnsureNoms(formsByKey, notes, createdBy)
}
func (d *PostgreSQL) GetNom(id int) (*Nom, error) { return d.help.getNom(id) }
func (d *PostgreSQL) ResolveNomByForma(forma string) (int, string, bool, error) {
	return d.help.resolveNomByForma(forma)
}
func (d *PostgreSQL) UpsertNomFreqMunicipiAny(nomID, municipiID, anyDoc, delta int) error {
	return d.help.upsertNomFreqMunicipiAny(nomID, municipiID, anyDoc, delta)
}
func (d *PostgreSQL) UpsertNomFreqMunicipiTotal(nomID, municipiID, delta int) error {
	return d.help.upsertNomFreqMunicipiTotal(nomID, municipiID, delta)
}
func (d *PostgreSQL) UpsertCognomFreqMunicipiTotal(cognomID, municipiID, delta int) error {
	return d.help.upsertCognomFreqMunicipiTotal(cognomID, municipiID, delta)
}
func (d *PostgreSQL) BulkApplyNomCognomStatsDeltas(deltas NomCognomStatsDeltas) error {
	return d.help.bulkApplyNomCognomStatsDeltas(deltas)
}
func (d *PostgreSQL) ListTopNomsByMunicipi(municipiID, limit int) ([]NomTotalRow, error) {
	return d.help.listTopNomsByMunicipi(municipiID, limit)
}
func (d *PostgreSQL) ListTopCognomsByMunicipi(municipiID, limit int) ([]CognomTotalRow, error) {
	return d.help.listTopCognomsByMunicipi(municipiID, limit)
}
func (d *PostgreSQL) ListNomSeries(municipiID, nomID int, bucket string) ([]NomFreqRow, error) {
	return d.help.listNomSeries(municipiID, nomID, bucket)
}
func (d *PostgreSQL) ListCognomSeries(municipiID, cognomID int, bucket string) ([]CognomFreqRow, error) {
	return d.help.listCognomSeries(municipiID, cognomID, bucket)
}
func (d *PostgreSQL) CountNomTotalsByMunicipi(municipiID int) (int, error) {
	return d.help.countNomTotalsByMunicipi(municipiID)
}
func (d *PostgreSQL) CountCognomTotalsByMunicipi(municipiID int) (int, error) {
	return d.help.countCognomTotalsByMunicipi(municipiID)
}
func (d *PostgreSQL) ClearNomCognomStatsByMunicipi(municipiID int) error {
	return d.help.clearNomCognomStatsByMunicipi(municipiID)
}
func (d *PostgreSQL) UpsertNomFreqNivellAny(nomID, nivellID, anyDoc, delta int) error {
	return d.help.upsertNomFreqNivellAny(nomID, nivellID, anyDoc, delta)
}
func (d *PostgreSQL) UpsertNomFreqNivellTotal(nomID, nivellID, delta int) error {
	return d.help.upsertNomFreqNivellTotal(nomID, nivellID, delta)
}
func (d *PostgreSQL) ApplyCognomFreqNivellAnyDelta(cognomID, nivellID, anyDoc, delta int) error {
	return d.help.upsertCognomFreqNivellAny(cognomID, nivellID, anyDoc, delta)
}
func (d *PostgreSQL) UpsertCognomFreqNivellTotal(cognomID, nivellID, delta int) error {
	return d.help.upsertCognomFreqNivellTotal(cognomID, nivellID, delta)
}
func (d *PostgreSQL) ListTopNomsByNivell(nivellID, limit int) ([]NomTotalRow, error) {
	return d.help.listTopNomsByNivell(nivellID, limit)
}
func (d *PostgreSQL) ListTopCognomsByNivell(nivellID, limit int) ([]CognomTotalRow, error) {
	return d.help.listTopCognomsByNivell(nivellID, limit)
}
func (d *PostgreSQL) ListNomSeriesByNivell(nivellID, nomID int, bucket string) ([]NomFreqRow, error) {
	return d.help.listNomSeriesByNivell(nivellID, nomID, bucket)
}
func (d *PostgreSQL) ListCognomSeriesByNivell(nivellID, cognomID int, bucket string) ([]CognomFreqRow, error) {
	return d.help.listCognomSeriesByNivell(nivellID, cognomID, bucket)
}
func (d *PostgreSQL) ClearNomCognomStatsByNivell(nivellID int) error {
	return d.help.clearNomCognomStatsByNivell(nivellID)
}
func (d *PostgreSQL) RebuildNivellNomCognomStats(nivellID int) error {
	return d.help.rebuildNivellNomCognomStats(nivellID)
}

func (d *PostgreSQL) ListMunicipiMapes(filter MunicipiMapaFilter) ([]MunicipiMapa, error) {
	return d.help.listMunicipiMapes(filter)
}
func (d *PostgreSQL) GetMunicipiMapa(id int) (*MunicipiMapa, error) {
	return d.help.getMunicipiMapa(id)
}
func (d *PostgreSQL) CreateMunicipiMapa(m *MunicipiMapa) (int, error) {
	return d.help.createMunicipiMapa(m)
}
func (d *PostgreSQL) UpdateMunicipiMapa(m *MunicipiMapa) error {
	return d.help.updateMunicipiMapa(m)
}
func (d *PostgreSQL) UpdateMunicipiMapaCurrentVersion(mapaID, versionID int) error {
	return d.help.updateMunicipiMapaCurrentVersion(mapaID, versionID)
}
func (d *PostgreSQL) NextMunicipiMapaVersionNumber(mapaID int) (int, error) {
	return d.help.nextMunicipiMapaVersionNumber(mapaID)
}
func (d *PostgreSQL) ListMunicipiMapaVersions(filter MunicipiMapaVersionFilter) ([]MunicipiMapaVersion, error) {
	return d.help.listMunicipiMapaVersions(filter)
}
func (d *PostgreSQL) CountMunicipiMapaVersionsScoped(filter MunicipiMapaVersionFilter, scope MunicipiScopeFilter) (int, error) {
	return d.help.countMunicipiMapaVersionsScoped(filter, scope)
}
func (d *PostgreSQL) GetMunicipiMapaVersion(id int) (*MunicipiMapaVersion, error) {
	return d.help.getMunicipiMapaVersion(id)
}
func (d *PostgreSQL) CreateMunicipiMapaVersion(v *MunicipiMapaVersion) (int, error) {
	return d.help.createMunicipiMapaVersion(v)
}
func (d *PostgreSQL) SaveMunicipiMapaDraft(versionID int, jsonData, changelog string, expectedLock int) (int, error) {
	return d.help.saveMunicipiMapaDraft(versionID, jsonData, changelog, expectedLock)
}
func (d *PostgreSQL) UpdateMunicipiMapaVersionStatus(id int, status, notes string, moderatorID int) error {
	return d.help.updateMunicipiMapaVersionStatus(id, status, notes, moderatorID)
}
func (d *PostgreSQL) ResolveMunicipiIDByMapaID(mapaID int) (int, error) {
	return d.help.resolveMunicipiIDByMapaID(mapaID)
}
func (d *PostgreSQL) ResolveMunicipiIDByMapaVersionID(versionID int) (int, error) {
	return d.help.resolveMunicipiIDByMapaVersionID(versionID)
}

func (d *PostgreSQL) EnsureMunicipiHistoria(municipiID int) (*MunicipiHistoria, error) {
	return d.help.ensureMunicipiHistoria(municipiID)
}
func (d *PostgreSQL) GetMunicipiHistoriaByMunicipiID(municipiID int) (*MunicipiHistoria, error) {
	return d.help.getMunicipiHistoriaByMunicipiID(municipiID)
}
func (d *PostgreSQL) ResolveMunicipiIDByHistoriaGeneralVersionID(versionID int) (int, error) {
	return d.help.resolveMunicipiIDByHistoriaGeneralVersionID(versionID)
}
func (d *PostgreSQL) ResolveMunicipiIDByHistoriaFetVersionID(versionID int) (int, error) {
	return d.help.resolveMunicipiIDByHistoriaFetVersionID(versionID)
}
func (d *PostgreSQL) NextMunicipiHistoriaGeneralVersion(historiaID int) (int, error) {
	return d.help.nextMunicipiHistoriaGeneralVersion(historiaID)
}
func (d *PostgreSQL) CreateMunicipiHistoriaGeneralDraft(historiaID int, createdBy int, baseFromCurrent bool) (int, error) {
	return d.help.createMunicipiHistoriaGeneralDraft(historiaID, createdBy, baseFromCurrent)
}
func (d *PostgreSQL) GetMunicipiHistoriaGeneralVersion(id int) (*MunicipiHistoriaGeneralVersion, error) {
	return d.help.getMunicipiHistoriaGeneralVersion(id)
}
func (d *PostgreSQL) UpdateMunicipiHistoriaGeneralDraft(v *MunicipiHistoriaGeneralVersion) error {
	return d.help.updateMunicipiHistoriaGeneralDraft(v)
}
func (d *PostgreSQL) SetMunicipiHistoriaGeneralStatus(versionID int, status, notes string, moderatorID *int) error {
	return d.help.setMunicipiHistoriaGeneralStatus(versionID, status, notes, moderatorID)
}
func (d *PostgreSQL) GetMunicipiHistoriaFet(id int) (*MunicipiHistoriaFet, error) {
	return d.help.getMunicipiHistoriaFet(id)
}
func (d *PostgreSQL) CreateMunicipiHistoriaFet(municipiID int, createdBy int) (int, error) {
	return d.help.createMunicipiHistoriaFet(municipiID, createdBy)
}
func (d *PostgreSQL) NextMunicipiHistoriaFetVersion(fetID int) (int, error) {
	return d.help.nextMunicipiHistoriaFetVersion(fetID)
}
func (d *PostgreSQL) CreateMunicipiHistoriaFetDraft(fetID int, createdBy int, baseFromCurrent bool) (int, error) {
	return d.help.createMunicipiHistoriaFetDraft(fetID, createdBy, baseFromCurrent)
}
func (d *PostgreSQL) GetMunicipiHistoriaFetVersion(id int) (*MunicipiHistoriaFetVersion, error) {
	return d.help.getMunicipiHistoriaFetVersion(id)
}
func (d *PostgreSQL) UpdateMunicipiHistoriaFetDraft(v *MunicipiHistoriaFetVersion) error {
	return d.help.updateMunicipiHistoriaFetDraft(v)
}
func (d *PostgreSQL) SetMunicipiHistoriaFetStatus(versionID int, status, notes string, moderatorID *int) error {
	return d.help.setMunicipiHistoriaFetStatus(versionID, status, notes, moderatorID)
}
func (d *PostgreSQL) GetMunicipiHistoriaSummary(municipiID int) (*MunicipiHistoriaGeneralVersion, []MunicipiHistoriaFetVersion, error) {
	return d.help.getMunicipiHistoriaSummary(municipiID)
}
func (d *PostgreSQL) ListMunicipiHistoriaTimeline(municipiID int, status string, limit, offset int, q string, anyFrom, anyTo *int) ([]MunicipiHistoriaFetVersion, int, error) {
	return d.help.listMunicipiHistoriaTimeline(municipiID, status, limit, offset, q, anyFrom, anyTo)
}
func (d *PostgreSQL) ListPendingMunicipiHistoriaGeneralVersions(limit, offset int) ([]MunicipiHistoriaGeneralVersion, int, error) {
	return d.help.listPendingMunicipiHistoriaGeneralVersions(limit, offset)
}
func (d *PostgreSQL) ListPendingMunicipiHistoriaFetVersions(limit, offset int) ([]MunicipiHistoriaFetVersion, int, error) {
	return d.help.listPendingMunicipiHistoriaFetVersions(limit, offset)
}
func (d *PostgreSQL) CountPendingMunicipiHistoriaGeneralVersionsScoped(filter MunicipiScopeFilter) (int, error) {
	return d.help.countPendingMunicipiHistoriaGeneralVersionsScoped(filter)
}
func (d *PostgreSQL) CountPendingMunicipiHistoriaFetVersionsScoped(filter MunicipiScopeFilter) (int, error) {
	return d.help.countPendingMunicipiHistoriaFetVersionsScoped(filter)
}

func (d *PostgreSQL) GetMunicipiDemografiaMeta(municipiID int) (*MunicipiDemografiaMeta, error) {
	return d.help.getMunicipiDemografiaMeta(municipiID)
}
func (d *PostgreSQL) ListMunicipiDemografiaAny(municipiID int, from, to int) ([]MunicipiDemografiaAny, error) {
	return d.help.listMunicipiDemografiaAny(municipiID, from, to)
}
func (d *PostgreSQL) ListMunicipiDemografiaDecades(municipiID int, from, to int) ([]MunicipiDemografiaAny, error) {
	return d.help.listMunicipiDemografiaDecades(municipiID, from, to)
}
func (d *PostgreSQL) ApplyMunicipiDemografiaDelta(municipiID, year int, tipus string, delta int) error {
	return d.help.applyMunicipiDemografiaDelta(municipiID, year, tipus, delta)
}
func (d *PostgreSQL) ApplyMunicipiDemografiaDeltaTx(tx *sql.Tx, municipiID, year int, tipus string, delta int) error {
	return d.help.applyMunicipiDemografiaDeltaTx(tx, municipiID, year, tipus, delta)
}
func (d *PostgreSQL) RebuildMunicipiDemografia(municipiID int) error {
	return d.help.rebuildMunicipiDemografia(municipiID)
}
func (d *PostgreSQL) GetNivellDemografiaMeta(nivellID int) (*NivellDemografiaMeta, error) {
	return d.help.getNivellDemografiaMeta(nivellID)
}
func (d *PostgreSQL) ListNivellDemografiaAny(nivellID int, from, to int) ([]NivellDemografiaAny, error) {
	return d.help.listNivellDemografiaAny(nivellID, from, to)
}
func (d *PostgreSQL) ListNivellDemografiaDecades(nivellID int, from, to int) ([]NivellDemografiaAny, error) {
	return d.help.listNivellDemografiaDecades(nivellID, from, to)
}
func (d *PostgreSQL) ApplyNivellDemografiaDelta(nivellID, year int, tipus string, delta int) error {
	return d.help.applyNivellDemografiaDelta(nivellID, year, tipus, delta)
}
func (d *PostgreSQL) BulkApplyPositiveDemografiaDeltas(municipis []DemografiaDelta, nivells []DemografiaDelta) error {
	return d.help.bulkApplyPositiveDemografiaDeltas(municipis, nivells)
}
func (d *PostgreSQL) RebuildNivellDemografia(nivellID int) error {
	return d.help.rebuildNivellDemografia(nivellID)
}

func (d *PostgreSQL) ListMunicipiAnecdotariPublished(municipiID int, f MunicipiAnecdotariFilter) ([]MunicipiAnecdotariVersion, int, error) {
	return d.help.listMunicipiAnecdotariPublished(municipiID, f)
}
func (d *PostgreSQL) GetMunicipiAnecdotariPublished(itemID int) (*MunicipiAnecdotariVersion, error) {
	return d.help.getMunicipiAnecdotariPublished(itemID)
}
func (d *PostgreSQL) ListMunicipiAnecdotariComments(itemID int, limit, offset int) ([]MunicipiAnecdotariComment, int, error) {
	return d.help.listMunicipiAnecdotariComments(itemID, limit, offset)
}
func (d *PostgreSQL) CreateMunicipiAnecdotariItem(municipiID int, createdBy int) (int, error) {
	return d.help.createMunicipiAnecdotariItem(municipiID, createdBy)
}
func (d *PostgreSQL) CreateMunicipiAnecdotariDraft(itemID int, createdBy int, baseFromCurrent bool) (int, error) {
	return d.help.createMunicipiAnecdotariDraft(itemID, createdBy, baseFromCurrent)
}
func (d *PostgreSQL) GetMunicipiAnecdotariVersion(id int) (*MunicipiAnecdotariVersion, error) {
	return d.help.getMunicipiAnecdotariVersion(id)
}
func (d *PostgreSQL) GetPendingMunicipiAnecdotariVersionByItemID(itemID int) (*MunicipiAnecdotariVersion, error) {
	return d.help.getPendingMunicipiAnecdotariVersionByItemID(itemID)
}
func (d *PostgreSQL) UpdateMunicipiAnecdotariDraft(v *MunicipiAnecdotariVersion) error {
	return d.help.updateMunicipiAnecdotariDraft(v)
}
func (d *PostgreSQL) SubmitMunicipiAnecdotariVersion(versionID int) error {
	return d.help.submitMunicipiAnecdotariVersion(versionID)
}
func (d *PostgreSQL) ListPendingMunicipiAnecdotariVersions(limit, offset int) ([]MunicipiAnecdotariVersion, int, error) {
	return d.help.listPendingMunicipiAnecdotariVersions(limit, offset)
}
func (d *PostgreSQL) CountPendingMunicipiAnecdotariVersionsScoped(filter MunicipiScopeFilter) (int, error) {
	return d.help.countPendingMunicipiAnecdotariVersionsScoped(filter)
}
func (d *PostgreSQL) ApproveMunicipiAnecdotariVersion(versionID int, moderatorID int) error {
	return d.help.approveMunicipiAnecdotariVersion(versionID, moderatorID)
}
func (d *PostgreSQL) RejectMunicipiAnecdotariVersion(versionID int, moderatorID int, notes string) error {
	return d.help.rejectMunicipiAnecdotariVersion(versionID, moderatorID, notes)
}
func (d *PostgreSQL) CreateMunicipiAnecdotariComment(itemID int, userID int, body string) (int, error) {
	return d.help.createMunicipiAnecdotariComment(itemID, userID, body)
}
func (d *PostgreSQL) GetMunicipiAnecdotariLastCommentAt(userID int) (time.Time, error) {
	return d.help.getMunicipiAnecdotariLastCommentAt(userID)
}
func (d *PostgreSQL) ResolveMunicipiIDByAnecdotariItemID(itemID int) (int, error) {
	return d.help.resolveMunicipiIDByAnecdotariItemID(itemID)
}
func (d *PostgreSQL) ResolveMunicipiIDByAnecdotariVersionID(versionID int) (int, error) {
	return d.help.resolveMunicipiIDByAnecdotariVersionID(versionID)
}

// Esdeveniments historics
func (d *PostgreSQL) CreateEventHistoric(e *EventHistoric) (int, error) {
	return d.help.createEventHistoric(e)
}

func (d *PostgreSQL) GetEventHistoric(id int) (*EventHistoric, error) {
	return d.help.getEventHistoric(id)
}

func (d *PostgreSQL) GetEventHistoricBySlug(slug string) (*EventHistoric, error) {
	return d.help.getEventHistoricBySlug(slug)
}

func (d *PostgreSQL) UpdateEventHistoric(e *EventHistoric) error {
	return d.help.updateEventHistoric(e)
}

func (d *PostgreSQL) ListEventsHistoric(filter EventHistoricFilter) ([]EventHistoric, error) {
	return d.help.listEventsHistoric(filter)
}
func (d *PostgreSQL) CountEventsHistoric(filter EventHistoricFilter) (int, error) {
	return d.help.countEventsHistoric(filter)
}

func (d *PostgreSQL) UpdateEventHistoricModeracio(id int, estat, notes string, moderatorID int) error {
	return d.help.updateEventHistoricModeracio(id, estat, notes, moderatorID)
}

func (d *PostgreSQL) ListEventImpacts(eventID int) ([]EventHistoricImpact, error) {
	return d.help.listEventImpacts(eventID)
}

func (d *PostgreSQL) ReplaceEventImpacts(eventID int, impacts []EventHistoricImpact) error {
	return d.help.replaceEventImpacts(eventID, impacts)
}

func (d *PostgreSQL) ListEventsByScope(scopeType string, scopeID int, filter EventHistoricFilter) ([]EventHistoric, error) {
	return d.help.listEventsByScope(scopeType, scopeID, filter)
}
