package db

import "database/sql"

type EspaiArbre struct {
	ID          int
	OwnerUserID int
	Nom         string
	Descripcio  sql.NullString
	Visibility  string
	Status      string
	CreatedAt   sql.NullTime
	UpdatedAt   sql.NullTime
}

type EspaiFontImportacio struct {
	ID               int
	OwnerUserID      int
	SourceType       string
	Nom              sql.NullString
	OriginalFilename sql.NullString
	StoragePath      sql.NullString
	ChecksumSHA256   sql.NullString
	SizeBytes        sql.NullInt64
	CreatedAt        sql.NullTime
	UpdatedAt        sql.NullTime
}

type EspaiImport struct {
	ID            int
	OwnerUserID   int
	ArbreID       int
	FontID        sql.NullInt64
	ImportType    string
	Status        string
	ProgressTotal int
	ProgressDone  int
	SummaryJSON   sql.NullString
	ErrorText     sql.NullString
	StartedAt     sql.NullTime
	FinishedAt    sql.NullTime
	CreatedAt     sql.NullTime
	UpdatedAt     sql.NullTime
}

type EspaiPersona struct {
	ID            int
	OwnerUserID   int
	ArbreID       int
	ExternalID    sql.NullString
	Nom           sql.NullString
	Cognom1       sql.NullString
	Cognom2       sql.NullString
	NomComplet    sql.NullString
	Sexe          sql.NullString
	DataNaixement sql.NullString
	DataDefuncio  sql.NullString
	LlocNaixement sql.NullString
	LlocDefuncio  sql.NullString
	Notes         sql.NullString
	Visibility    string
	Status        string
	CreatedAt     sql.NullTime
	UpdatedAt     sql.NullTime
}

type EspaiPrivacyAudit struct {
	ID             int
	OwnerUserID    int
	ArbreID        int
	PersonaID      sql.NullInt64
	Action         string
	FromVisibility sql.NullString
	ToVisibility   sql.NullString
	IP             sql.NullString
	CreatedAt      sql.NullTime
}

type EspaiRelacio struct {
	ID              int
	ArbreID         int
	PersonaID       int
	RelatedPersonaID int
	RelationType    string
	Notes           sql.NullString
	CreatedAt       sql.NullTime
	UpdatedAt       sql.NullTime
}

type EspaiCoincidencia struct {
	ID          int
	OwnerUserID int
	ArbreID     int
	PersonaID   int
	TargetType  string
	TargetID    int
	Score       sql.NullFloat64
	ReasonJSON  sql.NullString
	Status      string
	CreatedAt   sql.NullTime
	UpdatedAt   sql.NullTime
}

type EspaiCoincidenciaDecision struct {
	ID             int
	CoincidenciaID int
	Decision       string
	DecidedBy      sql.NullInt64
	Notes          sql.NullString
	CreatedAt      sql.NullTime
}

type EspaiIntegracioGramps struct {
	ID          int
	OwnerUserID int
	ArbreID     int
	BaseURL     string
	Username    sql.NullString
	Token       sql.NullString
	Status      string
	LastSyncAt  sql.NullTime
	LastError   sql.NullString
	CreatedAt   sql.NullTime
	UpdatedAt   sql.NullTime
}

type EspaiIntegracioGrampsLog struct {
	ID           int
	IntegracioID int
	Status       string
	Message      sql.NullString
	CreatedAt    sql.NullTime
}

type EspaiNotification struct {
	ID         int
	UserID     int
	Kind       string
	Title      sql.NullString
	Body       sql.NullString
	URL        sql.NullString
	Status     string
	ObjectType sql.NullString
	ObjectID   sql.NullInt64
	GroupID    sql.NullInt64
	TreeID     sql.NullInt64
	DedupeKey  sql.NullString
	CreatedAt  sql.NullTime
	ReadAt     sql.NullTime
}

type EspaiNotificationPref struct {
	UserID    int
	Freq      string
	TypesJSON sql.NullString
	UpdatedAt sql.NullTime
}

type EspaiGrup struct {
	ID          int
	OwnerUserID int
	Nom         string
	Descripcio  sql.NullString
	Status      string
	CreatedAt   sql.NullTime
	UpdatedAt   sql.NullTime
}

type EspaiGrupMembre struct {
	ID        int
	GrupID    int
	UserID    int
	Role      string
	Status    string
	JoinedAt  sql.NullTime
	CreatedAt sql.NullTime
}

type EspaiGrupArbre struct {
	ID        int
	GrupID    int
	ArbreID   int
	Status    string
	CreatedAt sql.NullTime
}

type EspaiGrupConflicte struct {
	ID           int
	GrupID       int
	ArbreID      int
	ConflictType string
	ObjectID     sql.NullInt64
	Status       string
	Summary      sql.NullString
	DetailsJSON  sql.NullString
	ResolvedAt   sql.NullTime
	ResolvedBy   sql.NullInt64
	CreatedAt    sql.NullTime
	UpdatedAt    sql.NullTime
}

type EspaiGrupCanvi struct {
	ID         int
	GrupID     int
	ActorID    sql.NullInt64
	Action     string
	ObjectType sql.NullString
	ObjectID   sql.NullInt64
	PayloadJSON sql.NullString
	CreatedAt  sql.NullTime
}
