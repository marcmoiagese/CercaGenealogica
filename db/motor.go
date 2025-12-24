package db

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"
)

type DB interface {
	Connect() error
	Close()
	Exec(query string, args ...interface{}) (int64, error)
	Query(query string, args ...interface{}) ([]map[string]interface{}, error)
	InsertUser(user *User) error
	SaveActivationToken(email, token string) error
	GetUserByEmail(email string) (*User, error)
	GetUserByID(id int) (*User, error)
	ExistsUserByUsername(username string) (bool, error)
	ExistsUserByEmail(email string) (bool, error)
	ActivateUser(token string) error
	AuthenticateUser(usernameOrEmail, password string) (*User, error)
	SaveSession(sessionID string, userID int, expiry string) error
	GetSessionUser(sessionID string) (*User, error)
	DeleteSession(sessionID string) error
	ListUserGroups(userID int) ([]Group, error)
	CreatePasswordReset(email, token, expiry, lang string) (bool, error)
	GetPasswordReset(token string) (*PasswordReset, error)
	MarkPasswordResetUsed(id int) error
	UpdateUserPassword(userID int, passwordHash []byte) error
	CreatePrivacyDefaults(userID int) error
	GetPrivacySettings(userID int) (*PrivacySettings, error)
	SavePrivacySettings(userID int, p *PrivacySettings) error
	UpdateUserProfile(u *User) error
	UpdateUserEmail(userID int, newEmail string) error
	CreateEmailChange(userID int, newEmail, tokenConfirm, expConfirm, tokenRevert, expRevert, lang string) error
	ConfirmEmailChange(token string) (*EmailChange, error)
	RevertEmailChange(token string) (*EmailChange, error)
	// Policies
	UserHasAnyPolicy(userID int, policies []string) (bool, error)
	EnsureDefaultPolicies() error
	ListGroups() ([]Group, error)
	ListPolitiques() ([]Politica, error)
	GetPolitica(id int) (*Politica, error)
	SavePolitica(p *Politica) (int, error)
	ListUserPolitiques(userID int) ([]Politica, error)
	AddUserPolitica(userID, politicaID int) error
	RemoveUserPolitica(userID, politicaID int) error
	ListGroupPolitiques(groupID int) ([]Politica, error)
	AddGroupPolitica(groupID, politicaID int) error
	RemoveGroupPolitica(groupID, politicaID int) error
	GetEffectivePoliticaPerms(userID int) (PolicyPermissions, error)
	EnsureDefaultPointsRules() error
	// Punts i activitat
	ListPointsRules() ([]PointsRule, error)
	GetPointsRule(id int) (*PointsRule, error)
	GetPointsRuleByCode(code string) (*PointsRule, error)
	SavePointsRule(r *PointsRule) (int, error)
	GetUserActivity(id int) (*UserActivity, error)
	InsertUserActivity(a *UserActivity) (int, error)
	UpdateUserActivityStatus(id int, status string, moderatedBy *int) error
	ListUserActivityByUser(userID int, f ActivityFilter) ([]UserActivity, error)
	ListActivityByObject(objectType string, objectID int, status string) ([]UserActivity, error)
	AddPointsToUser(userID int, delta int) error
	GetUserPoints(userID int) (*UserPoints, error)
	RecalcUserPoints() error
	GetRanking(f RankingFilter) ([]UserPoints, error)
	CountRanking(f RankingFilter) (int, error)

	// Persones (moderació)
	ListPersones(filter PersonaFilter) ([]Persona, error)
	GetPersona(id int) (*Persona, error)
	CreatePersona(p *Persona) (int, error)
	UpdatePersona(p *Persona) error
	UpdatePersonaModeracio(id int, estat, motiu string, moderatorID int) error
	UpdateArxiuModeracio(id int, estat, motiu string, moderatorID int) error
	UpdateLlibreModeracio(id int, estat, motiu string, moderatorID int) error
	UpdateNivellModeracio(id int, estat, motiu string, moderatorID int) error
	UpdateMunicipiModeracio(id int, estat, motiu string, moderatorID int) error
	UpdateArquebisbatModeracio(id int, estat, motiu string, moderatorID int) error
	UpdateTranscripcioModeracio(id int, estat, motiu string, moderatorID int) error
	// Arxius CRUD
	ListArxius(filter ArxiuFilter) ([]ArxiuWithCount, error)
	GetArxiu(id int) (*Arxiu, error)
	CreateArxiu(a *Arxiu) (int, error)
	UpdateArxiu(a *Arxiu) error
	DeleteArxiu(id int) error
	ListArxiuLlibres(arxiuID int) ([]ArxiuLlibreDetail, error)
	ListLlibreArxius(llibreID int) ([]ArxiuLlibreDetail, error)
	AddArxiuLlibre(arxiuID, llibreID int, signatura, urlOverride string) error
	UpdateArxiuLlibre(arxiuID, llibreID int, signatura, urlOverride string) error
	DeleteArxiuLlibre(arxiuID, llibreID int) error
	SearchLlibresSimple(q string, limit int) ([]LlibreSimple, error)
	ListLlibres(filter LlibreFilter) ([]LlibreRow, error)
	GetLlibre(id int) (*Llibre, error)
	CreateLlibre(l *Llibre) (int, error)
	UpdateLlibre(l *Llibre) error
	HasLlibreDuplicate(municipiID int, tipus, cronologia, codiDigital, codiFisic string, excludeID int) (bool, error)
	GetLlibresIndexacioStats(ids []int) (map[int]LlibreIndexacioStats, error)
	UpsertLlibreIndexacioStats(stats *LlibreIndexacioStats) error
	ListLlibrePagines(llibreID int) ([]LlibrePagina, error)
	SaveLlibrePagina(p *LlibrePagina) (int, error)
	RecalcLlibrePagines(llibreID, total int) error
	// Transcripcions RAW
	ListTranscripcionsRaw(llibreID int, f TranscripcioFilter) ([]TranscripcioRaw, error)
	ListTranscripcionsRawGlobal(f TranscripcioFilter) ([]TranscripcioRaw, error)
	CountTranscripcionsRaw(llibreID int, f TranscripcioFilter) (int, error)
	CountTranscripcionsRawGlobal(f TranscripcioFilter) (int, error)
	GetTranscripcioRaw(id int) (*TranscripcioRaw, error)
	CreateTranscripcioRaw(t *TranscripcioRaw) (int, error)
	UpdateTranscripcioRaw(t *TranscripcioRaw) error
	DeleteTranscripcioRaw(id int) error
	ListTranscripcionsRawPageStats(llibreID int) ([]TranscripcioRawPageStat, error)
	UpdateTranscripcionsRawPageStat(stat *TranscripcioRawPageStat) error
	RecalcTranscripcionsRawPageStats(llibreID int) error
	DeleteTranscripcionsByLlibre(llibreID int) error
	CreateTranscripcioRawChange(c *TranscripcioRawChange) (int, error)
	ListTranscripcioPersones(transcripcioID int) ([]TranscripcioPersonaRaw, error)
	CreateTranscripcioPersona(p *TranscripcioPersonaRaw) (int, error)
	DeleteTranscripcioPersones(transcripcioID int) error
	LinkTranscripcioPersona(personaRawID int, personaID int, linkedBy int) error
	UnlinkTranscripcioPersona(personaRawID int, linkedBy int) error
	ListTranscripcioAtributs(transcripcioID int) ([]TranscripcioAtributRaw, error)
	CreateTranscripcioAtribut(a *TranscripcioAtributRaw) (int, error)
	DeleteTranscripcioAtributs(transcripcioID int) error
	GetTranscripcioDraft(userID, llibreID int) (*TranscripcioDraft, error)
	SaveTranscripcioDraft(userID, llibreID int, payload string) error
	DeleteTranscripcioDraft(userID, llibreID int) error
	UpsertTranscripcioMark(m *TranscripcioRawMark) error
	DeleteTranscripcioMark(transcripcioID, userID int) error
	ListTranscripcioMarks(transcripcioIDs []int) ([]TranscripcioRawMark, error)
	SearchPersones(f PersonaSearchFilter) ([]PersonaSearchResult, error)
	ListRegistresByPersona(personaID int, tipus string) ([]PersonaRegistreRow, error)

	// Paisos
	ListPaisos() ([]Pais, error)
	GetPais(id int) (*Pais, error)
	CreatePais(p *Pais) (int, error)
	UpdatePais(p *Pais) error

	// Nivells administratius
	ListNivells(f NivellAdminFilter) ([]NivellAdministratiu, error)
	GetNivell(id int) (*NivellAdministratiu, error)
	CreateNivell(n *NivellAdministratiu) (int, error)
	UpdateNivell(n *NivellAdministratiu) error

	// Municipis
	ListMunicipis(f MunicipiFilter) ([]MunicipiRow, error)
	GetMunicipi(id int) (*Municipi, error)
	CreateMunicipi(m *Municipi) (int, error)
	UpdateMunicipi(m *Municipi) error
	ListCodisPostals(municipiID int) ([]CodiPostal, error)
	SaveCodiPostal(cp *CodiPostal) (int, error)
	ListNomsHistorics(entitatTipus string, entitatID int) ([]NomHistoric, error)
	SaveNomHistoric(nh *NomHistoric) (int, error)

	// Entitats eclesiàstiques
	ListArquebisbats(f ArquebisbatFilter) ([]ArquebisbatRow, error)
	GetArquebisbat(id int) (*Arquebisbat, error)
	CreateArquebisbat(ae *Arquebisbat) (int, error)
	UpdateArquebisbat(ae *Arquebisbat) error
	ListArquebisbatMunicipis(munID int) ([]ArquebisbatMunicipi, error)
	SaveArquebisbatMunicipi(am *ArquebisbatMunicipi) (int, error)
}

// Tipus comú d'usuari al paquet `db`
type User struct {
	ID            int
	Usuari        string
	Name          string
	Surname       string
	Email         string
	Password      []byte
	DataNaixament string
	Active        bool
	CreatedAt     string
	Pais          string
	Estat         string
	Provincia     string
	Poblacio      string
	CodiPostal    string
	Address       string
	Employment    string
	Profession    string
	Phone         string
	PreferredLang string
	SpokenLangs   string
}

type PasswordReset struct {
	ID     int
	UserID int
	Email  string
	Lang   string
}

type PrivacySettings struct {
	UserID                  int
	NomVisibility           string
	CognomsVisibility       string
	EmailVisibility         string
	BirthVisibility         string
	PaisVisibility          string
	EstatVisibility         string
	ProvinciaVisibility     string
	PoblacioVisibility      string
	PostalVisibility        string
	AddressVisibility       string
	EmploymentVisibility    string
	ProfessionVisibility    string
	PhoneVisibility         string
	PreferredLangVisibility string
	SpokenLangsVisibility   string
	ShowActivity            bool
	ProfilePublic           bool
	NotifyEmail             bool
	AllowContact            bool
}

// Regles de punts / activitat
type PointsRule struct {
	ID          int
	Code        string
	Name        string
	Description string
	Points      int
	Active      bool
	CreatedAt   time.Time
}

type UserActivity struct {
	ID          int
	UserID      int
	RuleID      sql.NullInt64
	Action      string
	ObjectType  string
	ObjectID    sql.NullInt64
	Points      int
	Status      string
	ModeratedBy sql.NullInt64
	Details     string
	CreatedAt   time.Time
}

type UserPoints struct {
	UserID              int
	Total               int
	UltimaActualitzacio time.Time
}

type ActivityFilter struct {
	Status     string
	ObjectType string
	Limit      int
	Offset     int
	From       time.Time
	To         time.Time
}

type RankingFilter struct {
	PreferredLang string
	Limit         int
	Offset        int
	PublicOnly    bool
}

type Pais struct {
	ID          int
	CodiISO2    string
	CodiISO3    string
	CodiPaisNum string
}

type Politica struct {
	ID         int
	Nom        string
	Descripcio string
	Permisos   string
}

type PolicyPermissions struct {
	Admin              bool `json:"admin"`
	CanManageUsers     bool `json:"can_manage_users"`
	CanManageTerritory bool `json:"can_manage_territory"`
	CanManageEclesia   bool `json:"can_manage_eclesiastic"`
	CanManageArchives  bool `json:"can_manage_archives"`
	CanCreatePerson    bool `json:"can_create_person"`
	CanEditAnyPerson   bool `json:"can_edit_any_person"`
	CanModerate        bool `json:"can_moderate"`
	CanManagePolicies  bool `json:"can_manage_policies"`
}

type Group struct {
	ID         int
	Nom        string
	Descripcio string
}

type Persona struct {
	ID             int
	Nom            string
	Cognom1        string
	Cognom2        string
	Municipi       string
	Arquebisbat    string
	NomComplet     string
	Pagina         string
	Llibre         string
	Quinta         string
	DataNaixement  sql.NullString
	DataBateig     sql.NullString
	DataDefuncio   sql.NullString
	Ofici          string
	EstatCivil     string
	ModeracioEstat string
	ModeracioMotiu string
	CreatedBy      sql.NullInt64
	CreatedAt      sql.NullTime
	UpdatedAt      sql.NullTime
	UpdatedBy      sql.NullInt64
	ModeratedBy    sql.NullInt64
	ModeratedAt    sql.NullTime
}

type PersonaFilter struct {
	Estat string
	Limit int
}

type NivellAdministratiu struct {
	ID             int
	PaisID         int
	PaisISO2       sql.NullString
	PaisLabel      string
	Nivel          int
	NomNivell      string
	TipusNivell    string
	CodiOficial    string
	Altres         string
	ParentID       sql.NullInt64
	ParentNom      sql.NullString
	AnyInici       sql.NullInt64
	AnyFi          sql.NullInt64
	Estat          string
	CreatedBy      sql.NullInt64
	ModeracioEstat string
	ModeracioMotiu string
	ModeratedBy    sql.NullInt64
	ModeratedAt    sql.NullTime
}

type NivellAdminFilter struct {
	PaisID int
	Nivel  int
	Estat  string
	Status string
}

type Municipi struct {
	ID                    int
	Nom                   string
	MunicipiID            sql.NullInt64
	Tipus                 string
	NivellAdministratiuID [7]sql.NullInt64
	CodiPostal            string
	Latitud               sql.NullFloat64
	Longitud              sql.NullFloat64
	What3Words            string
	Web                   string
	Wikipedia             string
	Altres                string
	Estat                 string
	CreatedBy             sql.NullInt64
	ModeracioEstat        string
	ModeracioMotiu        string
	ModeratedBy           sql.NullInt64
	ModeratedAt           sql.NullTime
}

type MunicipiRow struct {
	ID             int
	Nom            string
	Tipus          string
	Estat          string
	CodiPostal     string
	PaisNom        sql.NullString
	ProvNom        sql.NullString
	Comarca        sql.NullString
	ModeracioEstat string
}

type MunicipiFilter struct {
	Text     string
	Estat    string
	PaisID   int
	NivellID int
	Status   string
}

type CodiPostal struct {
	ID         int
	MunicipiID int
	CodiPostal string
	Zona       string
	Desde      sql.NullString
	Fins       sql.NullString
}

type Arquebisbat struct {
	ID             int
	Nom            string
	TipusEntitat   string
	PaisID         sql.NullInt64
	Nivell         sql.NullInt64
	ParentID       sql.NullInt64
	AnyInici       sql.NullInt64
	AnyFi          sql.NullInt64
	Web            string
	WebArxiu       string
	WebWikipedia   string
	Territori      string
	Observacions   string
	CreatedBy      sql.NullInt64
	ModeracioEstat string
	ModeracioMotiu string
	ModeratedBy    sql.NullInt64
	ModeratedAt    sql.NullTime
}

type ArquebisbatRow struct {
	ID             int
	Nom            string
	TipusEntitat   string
	PaisNom        sql.NullString
	Nivell         sql.NullInt64
	ParentNom      sql.NullString
	AnyInici       sql.NullInt64
	AnyFi          sql.NullInt64
	ModeracioEstat string
}

type ArquebisbatFilter struct {
	Text   string
	PaisID int
	Status string
}

type ArquebisbatMunicipi struct {
	ID            int
	MunicipiID    int
	ArquebisbatID int
	AnyInici      sql.NullInt64
	AnyFi         sql.NullInt64
	Motiu         string
	Font          string
	NomEntitat    string
}

type EmailChange struct {
	ID           int
	UserID       int
	OldEmail     string
	NewEmail     string
	TokenConfirm string
	ExpConfirm   string
	TokenRevert  string
	ExpRevert    string
	Lang         string
	Confirmed    bool
	Reverted     bool
}

// Arxius
type Arxiu struct {
	ID                    int
	Nom                   string
	Tipus                 string
	MunicipiID            sql.NullInt64
	EntitatEclesiasticaID sql.NullInt64
	Adreca                string
	Ubicacio              string
	Web                   string
	Acces                 string
	Notes                 string
	CreatedBy             sql.NullInt64
	ModeracioEstat        string
	ModeracioMotiu        string
	ModeratedBy           sql.NullInt64
	ModeratedAt           sql.NullTime
}

type ArxiuFilter struct {
	Text       string
	Tipus      string
	Acces      string
	EntitatID  int
	MunicipiID int
	PaisID     int
	Limit      int
	Offset     int
	Status     string
}

type ArxiuWithCount struct {
	Arxiu
	MunicipiNom sql.NullString
	EntitatNom  sql.NullString
	Llibres     int
}

type ArxiuLlibreDetail struct {
	ArxiuID     int
	LlibreID    int
	Titol       string
	NomEsglesia string
	Cronologia  string
	Municipi    sql.NullString
	ArxiuNom    sql.NullString
	Signatura   sql.NullString
	URLOverride sql.NullString
	Pagines     sql.NullInt64
}

type LlibreSimple struct {
	ID          int
	Titol       string
	NomEsglesia string
	Cronologia  string
	Municipi    sql.NullString
}

type Llibre struct {
	ID                int
	ArquebisbatID     int
	MunicipiID        int
	NomEsglesia       string
	CodiDigital       string
	CodiFisic         string
	Titol             string
	TipusLlibre       string
	Cronologia        string
	Volum             string
	Abat              string
	Contingut         string
	Llengua           string
	Requeriments      string
	UnitatCatalogacio string
	UnitatInstalacio  string
	Pagines           sql.NullInt64
	URLBase           string
	URLImatgePrefix   string
	Pagina            string
	IndexacioCompleta bool
	CreatedBy         sql.NullInt64
	ModeracioEstat    string
	ModeracioMotiu    string
	ModeratedBy       sql.NullInt64
	ModeratedAt       sql.NullTime
}

type LlibreRow struct {
	Llibre
	ArquebisbatNom sql.NullString
	MunicipiNom    sql.NullString
}

type LlibreIndexacioStats struct {
	LlibreID       int
	TotalRegistres int
	TotalCamps     int
	CampsEmplenats int
	Percentatge    int
	UpdatedAt      time.Time
}

type TranscripcioRawPageStat struct {
	ID             int
	LlibreID       int
	PaginaID       sql.NullInt64
	NumPaginaText  string
	TipusPagina    string
	Exclosa        int
	IndexacioCompleta int
	DuplicadaDe    sql.NullString
	TotalRegistres int
	ComputedAt     sql.NullTime
}

type LlibreFilter struct {
	Text          string
	ArquebisbatID int
	MunicipiID    int
	ArxiuID       int
	ArxiuTipus    string
	Status        string
}

type LlibrePagina struct {
	ID        int
	LlibreID  int
	NumPagina int
	Estat     string
	IndexedAt sql.NullString
	IndexedBy sql.NullInt64
	Notes     string
}

type TranscripcioRaw struct {
	ID                         int
	LlibreID                   int
	PaginaID                   sql.NullInt64
	NumPaginaText              string
	PosicioPagina              sql.NullInt64
	TipusActe                  string
	AnyDoc                     sql.NullInt64
	DataActeText               string
	DataActeISO                sql.NullString
	DataActeEstat              string
	TranscripcioLiteral        string
	NotesMarginals             string
	ObservacionsPaleografiques string
	ModeracioEstat             string
	ModeratedBy                sql.NullInt64
	ModeratedAt                sql.NullTime
	ModeracioMotiu             string
	CreatedBy                  sql.NullInt64
	CreatedAt                  time.Time
	UpdatedAt                  time.Time
}

type TranscripcioPersonaRaw struct {
	ID              int
	TranscripcioID  int
	Rol             string
	Nom             string
	NomEstat        string
	Cognom1         string
	Cognom1Estat    string
	Cognom2         string
	Cognom2Estat    string
	Sexe            string
	SexeEstat       string
	EdatText        string
	EdatEstat       string
	EstatCivilText  string
	EstatCivilEstat string
	MunicipiText    string
	MunicipiEstat   string
	OficiText       string
	OficiEstat      string
	CasaNom         string
	CasaEstat       string
	PersonaID       sql.NullInt64
	LinkedBy        sql.NullInt64
	LinkedAt        sql.NullTime
	Notes           string
}

type TranscripcioAtributRaw struct {
	ID             int
	TranscripcioID int
	Clau           string
	TipusValor     string
	ValorText      string
	ValorInt       sql.NullInt64
	ValorDate      sql.NullString
	ValorBool      sql.NullBool
	Estat          string
	Notes          string
}

type TranscripcioDraft struct {
	ID        int
	LlibreID  int
	UserID    int
	Payload   string
	UpdatedAt time.Time
}

type TranscripcioRawMark struct {
	ID             int
	TranscripcioID int
	UserID         int
	Tipus          string
	IsPublic       bool
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type TranscripcioRawChange struct {
	ID             int
	TranscripcioID int
	ChangeType     string
	FieldKey       string
	OldValue       string
	NewValue       string
	Metadata       string
	ChangedBy      sql.NullInt64
	ChangedAt      time.Time
}

type PersonaSearchFilter struct {
	Query    string
	Nom      string
	Cognom1  string
	Cognom2  string
	Municipi string
	AnyMin   int
	AnyMax   int
	Limit    int
}

type PersonaSearchResult struct {
	ID            int
	Nom           string
	Cognom1       string
	Cognom2       string
	Municipi      string
	DataNaixement sql.NullString
	DataBateig    sql.NullString
	DataDefuncio  sql.NullString
	Ofici         string
	EstatCivil    string
}

type PersonaRegistreRow struct {
	RegistreID     int
	PersonaRawID   int
	LlibreID       int
	LlibreTitol    sql.NullString
	LlibreNom      sql.NullString
	TipusActe      string
	AnyDoc         sql.NullInt64
	DataActeText   string
	PaginaID       sql.NullInt64
	NumPaginaText  string
	PosicioPagina  sql.NullInt64
	Rol            string
	ModeracioEstat string
}

type TranscripcioFilter struct {
	LlibreID    int
	TipusActe   string
	AnyDoc      int
	PaginaID    int
	Status      string
	Qualitat    string
	Search      string
	UseFullText bool
	Limit       int
	Offset      int
}

type NomHistoric struct {
	ID                    int
	EntitatTipus          string
	EntitatID             int
	Nom                   string
	AnyInici              sql.NullInt64
	AnyFi                 sql.NullInt64
	PaisRegne             string
	DistribucioGeografica string
	Font                  string
}

// Funció principal per obtenir una connexió i recrear BD si cal
func NewDB(config map[string]string) (DB, error) {
	var dbInstance DB
	engine := config["DB_ENGINE"]

	switch engine {
	case "sqlite":
		dbInstance = &SQLite{Path: config["DB_PATH"]}
	case "postgres":
		dbInstance = &PostgreSQL{
			Host:   config["DB_HOST"],
			Port:   config["DB_PORT"],
			User:   config["DB_USR"],
			Pass:   config["DB_PASS"],
			DBName: config["DB_NAME"],
		}
	case "mysql":
		dbInstance = &MySQL{
			Host:   config["DB_HOST"],
			Port:   config["DB_PORT"],
			User:   config["DB_USR"],
			Pass:   config["DB_PASS"],
			DBName: config["DB_NAME"],
		}
	default:
		return nil, fmt.Errorf("motor de BD desconegut: %s", engine)
	}

	// Connectem primer
	if err := dbInstance.Connect(); err != nil {
		return nil, err
	}

	// Si cal, recrearem la BD
	if config["RECREADB"] == "true" {
		sqlFile := getSQLFilePath(engine)
		if err := CreateDatabaseFromSQL(sqlFile, engine, dbInstance); err != nil {
			return nil, fmt.Errorf("error recreant BD amb %s: %v", engine, err)
		}
	}

	return dbInstance, nil
}

// Obtenir el path del fitxer SQL segons el motor
func getSQLFilePath(engine string) string {
	switch engine {
	case "sqlite":
		return "db/SQLite.sql"
	case "postgres":
		return "db/PostgreSQL.sql"
	case "mysql":
		return "db/MySQL.sql"
	default:
		return ""
	}
}

// Funció genèrica per executar totes les sentències SQL d'un fitxer
// Funció genèrica per executar totes les sentències SQL d'un fitxer
func CreateDatabaseFromSQL(sqlFile, engine string, db DB) error {
	logInfof("Recreant BD des de: %s", sqlFile)
	data, err := os.ReadFile(sqlFile)
	if err != nil {
		return fmt.Errorf("no s'ha pogut llegir el fitxer SQL: %w", err)
	}

	raw := string(data)

	// 1) Elimina línies de comentari i línies buides,
	//    però conserva el SQL que vingui després en altres línies
	var b strings.Builder
	for _, line := range strings.Split(raw, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") || trimmed == "" {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	cleanSQL := b.String()

	// 2) Separa per ';' i neteja espais. (Semicolons al final del statement)
	parts := strings.Split(cleanSQL, ";")

	// 3) Escollir com començar la transacció segons el motor
	beginStmt := "BEGIN"
	switch engine {
	case "sqlite":
		beginStmt = "BEGIN IMMEDIATE"
	case "postgres", "mysql":
		beginStmt = "BEGIN"
	default:
		beginStmt = "BEGIN"
	}

	if _, err := db.Exec(beginStmt); err != nil {
		return fmt.Errorf("no s’ha pogut començar transacció: %w", err)
	}
	defer func() {
		// en cas d’error, el caller retornarà; aquí fem un ROLLBACK best-effort
		_, _ = db.Exec("ROLLBACK")
	}()

	// 4) Activar FKs només per SQLite (PRAGMA és específic de SQLite)
	if engine == "sqlite" {
		if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
			return fmt.Errorf("error activant foreign_keys: %w", err)
		}
	}

	// 5) Executa cada statement
	for _, stmt := range parts {
		q := strings.TrimSpace(stmt)
		if q == "" {
			continue
		}
		// Evita BEGIN/COMMIT del fitxer, si n’hi hagués
		low := strings.ToLower(q)
		if low == "begin" || low == "commit" || strings.HasPrefix(low, "begin ") || strings.HasPrefix(low, "commit ") {
			continue
		}

		if _, err := db.Exec(q); err != nil {
			// Mostra un tros de l’SQL per facilitar el debug
			snip := q
			if len(snip) > 120 {
				snip = snip[:120] + " ..."
			}
			return fmt.Errorf("error executant '%s': %w", snip, err)
		}
	}

	// 6) Commit final
	if _, err := db.Exec("COMMIT"); err != nil {
		return fmt.Errorf("error fent COMMIT: %w", err)
	}

	logInfof("BD recreada correctament")
	return nil
}
