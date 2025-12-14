package db

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/crypto/bcrypt"
)

type SQLite struct {
	Path string
	Conn *sql.DB
	help sqlHelper
}

func (d *SQLite) Connect() error {
	conn, err := sql.Open("sqlite3", d.Path)
	if err != nil {
		return fmt.Errorf("error connectant a SQLite: %w", err)
	}
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
func (d *SQLite) UpdatePersonaModeracio(id int, estat, motiu string) error {
	return d.help.updatePersonaModeracio(id, estat, motiu)
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
func (d *SQLite) DeleteArxiu(id int) error {
	return d.help.deleteArxiu(id)
}
func (d *SQLite) ListArxiuLlibres(arxiuID int) ([]ArxiuLlibreDetail, error) {
	return d.help.listArxiuLlibres(arxiuID)
}
func (d *SQLite) ListLlibreArxius(llibreID int) ([]ArxiuLlibreDetail, error) {
	return d.help.listLlibreArxius(llibreID)
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
func (d *SQLite) ListLlibrePagines(llibreID int) ([]LlibrePagina, error) {
	return d.help.listLlibrePagines(llibreID)
}
func (d *SQLite) SaveLlibrePagina(p *LlibrePagina) (int, error) {
	return d.help.saveLlibrePagina(p)
}
func (d *SQLite) RecalcLlibrePagines(llibreID, total int) error {
	return d.help.recalcLlibrePagines(llibreID, total)
}
