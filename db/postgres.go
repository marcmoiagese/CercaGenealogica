package db

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
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

func (d *PostgreSQL) UpdateUserProfile(u *User) error {
	return d.help.updateUserProfile(u)
}

func (d *PostgreSQL) UpdateUserEmail(userID int, newEmail string) error {
	return d.help.updateUserEmail(userID, newEmail)
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
func (d *PostgreSQL) ListGroups() ([]Group, error) {
	return d.help.listGroups()
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
func (d *PostgreSQL) GetPersona(id int) (*Persona, error) {
	return d.help.getPersona(id)
}
func (d *PostgreSQL) CreatePersona(p *Persona) (int, error) {
	return d.help.createPersona(p)
}
func (d *PostgreSQL) UpdatePersona(p *Persona) error {
	return d.help.updatePersona(p)
}
func (d *PostgreSQL) UpdatePersonaModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updatePersonaModeracio(id, estat, motiu, moderatorID)
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
func (d *PostgreSQL) SuggestMunicipis(f MunicipiBrowseFilter) ([]MunicipiSuggestRow, error) {
	return d.help.suggestMunicipis(f)
}
func (d *PostgreSQL) GetMunicipi(id int) (*Municipi, error) {
	return d.help.getMunicipi(id)
}
func (d *PostgreSQL) CreateMunicipi(m *Municipi) (int, error) {
	return d.help.createMunicipi(m)
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
func (d *PostgreSQL) UpdateArquebisbat(ae *Arquebisbat) error {
	return d.help.updateArquebisbat(ae)
}
func (d *PostgreSQL) UpdateArquebisbatModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateArquebisbatModeracio(id, estat, motiu, moderatorID)
}
func (d *PostgreSQL) ListArquebisbatMunicipis(munID int) ([]ArquebisbatMunicipi, error) {
	return d.help.listArquebisbatMunicipis(munID)
}
func (d *PostgreSQL) SaveArquebisbatMunicipi(am *ArquebisbatMunicipi) (int, error) {
	return d.help.saveArquebisbatMunicipi(am)
}

// Arxius
func (d *PostgreSQL) ListArxius(f ArxiuFilter) ([]ArxiuWithCount, error) {
	return d.help.listArxius(f)
}

func (d *PostgreSQL) CountArxius(f ArxiuFilter) (int, error) {
	return d.help.countArxius(f)
}
func (d *PostgreSQL) GetArxiu(id int) (*Arxiu, error) {
	return d.help.getArxiu(id)
}
func (d *PostgreSQL) CreateArxiu(a *Arxiu) (int, error) {
	return d.help.createArxiu(a)
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
func (d *PostgreSQL) ListArxiuLlibres(arxiuID int) ([]ArxiuLlibreDetail, error) {
	return d.help.listArxiuLlibres(arxiuID)
}
func (d *PostgreSQL) ListLlibreArxius(llibreID int) ([]ArxiuLlibreDetail, error) {
	return d.help.listLlibreArxius(llibreID)
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
func (d *PostgreSQL) GetLlibre(id int) (*Llibre, error) {
	return d.help.getLlibre(id)
}
func (d *PostgreSQL) CreateLlibre(l *Llibre) (int, error) {
	return d.help.createLlibre(l)
}
func (d *PostgreSQL) UpdateLlibre(l *Llibre) error {
	return d.help.updateLlibre(l)
}
func (d *PostgreSQL) HasLlibreDuplicate(municipiID int, tipus, cronologia, codiDigital, codiFisic string, excludeID int) (bool, error) {
	return d.help.hasLlibreDuplicate(municipiID, tipus, cronologia, codiDigital, codiFisic, excludeID)
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
func (d *PostgreSQL) GetLlibrePaginaByID(id int) (*LlibrePagina, error) {
	return d.help.getLlibrePaginaByID(id)
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
func (d *PostgreSQL) CountTranscripcionsRaw(llibreID int, f TranscripcioFilter) (int, error) {
	return d.help.countTranscripcionsRaw(llibreID, f)
}
func (d *PostgreSQL) CountTranscripcionsRawGlobal(f TranscripcioFilter) (int, error) {
	return d.help.countTranscripcionsRawGlobal(f)
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
func (d *PostgreSQL) UpdateTranscripcioRawChangeModeracio(id int, estat, motiu string, moderatorID int) error {
	return d.help.updateTranscripcioRawChangeModeracio(id, estat, motiu, moderatorID)
}
func (d *PostgreSQL) ListTranscripcioPersones(transcripcioID int) ([]TranscripcioPersonaRaw, error) {
	return d.help.listTranscripcioPersones(transcripcioID)
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
func (d *PostgreSQL) SearchPersones(f PersonaSearchFilter) ([]PersonaSearchResult, error) {
	return d.help.searchPersones(f)
}
func (d *PostgreSQL) ListRegistresByPersona(personaID int, tipus string) ([]PersonaRegistreRow, error) {
	return d.help.listRegistresByPersona(personaID, tipus)
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
func (d *PostgreSQL) GetUserActivity(id int) (*UserActivity, error) {
	return d.help.getUserActivity(id)
}
func (d *PostgreSQL) InsertUserActivity(a *UserActivity) (int, error) {
	return d.help.insertUserActivity(a)
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

// Cognoms
func (d *PostgreSQL) ListCognoms(q string, limit, offset int) ([]Cognom, error) {
	return d.help.listCognoms(q, limit, offset)
}
func (d *PostgreSQL) GetCognom(id int) (*Cognom, error) { return d.help.getCognom(id) }
func (d *PostgreSQL) UpsertCognom(forma, key, origen, notes string, createdBy *int) (int, error) {
	return d.help.upsertCognom(forma, key, origen, notes, createdBy)
}
func (d *PostgreSQL) ListCognomVariants(f CognomVariantFilter) ([]CognomVariant, error) {
	return d.help.listCognomVariants(f)
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
func (d *PostgreSQL) UpsertCognomFreqMunicipiAny(cognomID, municipiID, anyDoc, freq int) error {
	return d.help.upsertCognomFreqMunicipiAny(cognomID, municipiID, anyDoc, freq)
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
