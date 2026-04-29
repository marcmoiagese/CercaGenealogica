package core

import (
	"database/sql"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type TerritoriImportPlan struct {
	Payload            territoriExportPayload
	UserID             int
	Engine             string
	BulkInserter       territoriBulkInserter
	HasBulkInserter    bool
	PaisByISO2         map[string]db.Pais
	LevelKeyMap        map[string]int
	ExistingLevelsByID map[int]db.NivellAdministratiu
	ActivityRuleNivell territoriImportActivityRule
	ActivityRuleMun    territoriImportActivityRule
}

type TerritoriImportPersistResult struct {
	CountriesCreated int
	LevelsTotal      int
	LevelsCreated    int
	LevelsSkipped    int
	LevelsErrors     int
	MunicipisTotal   int
	MunicipisCreated int
	MunicipisSkipped int
	MunicipisErrors  int
	ParentErrors     int
	BulkModeLevels   string
	BulkModeMunicipis string
	BulkModeParents   string
	Sidefx            TerritoriImportSidefxPlan
}

type TerritoriImportSidefxPlan struct {
	UserID               int
	AffectedMunicipiIDs  []int
	AffectedLevelIDs     []int
	PendingActivities    []db.UserActivity
	ActivityCount        int
	ClosureErrors        int
	RebuildErrors        int
	ActivityMode         string
}

type TerritoriImportMetrics struct {
	ParseDur                   time.Duration
	PrepDur                    time.Duration
	CountriesDur               time.Duration
	LevelsBuildDur             time.Duration
	LevelsPersistDur           time.Duration
	MunicipisExistingLookupDur time.Duration
	MunicipisBuildDur          time.Duration
	MunicipisPersistDur        time.Duration
	ParentsBuildDur            time.Duration
	ParentsPersistDur          time.Duration
	SidefxClosureDur           time.Duration
	SidefxRebuildDemografiaDur time.Duration
	SidefxRebuildNomCognomDur  time.Duration
	SidefxActivitiesDur        time.Duration
	SidefxAchievementsDur      time.Duration
	TotalDur                   time.Duration
}

type territoriImportActivityRule struct {
	ruleID sql.NullInt64
	points int
}
