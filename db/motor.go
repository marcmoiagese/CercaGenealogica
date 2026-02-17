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
	ListDashboardWidgets(userID int) ([]DashboardWidgetConfig, error)
	SaveDashboardWidgets(userID int, widgets []DashboardWidgetConfig) error
	ClearDashboardWidgets(userID int) error
	ListPlatformSettings() ([]PlatformSetting, error)
	UpsertPlatformSetting(key, value string, updatedBy int) error
	ListMaintenanceWindows() ([]MaintenanceWindow, error)
	GetMaintenanceWindow(id int) (*MaintenanceWindow, error)
	SaveMaintenanceWindow(w *MaintenanceWindow) (int, error)
	DeleteMaintenanceWindow(id int) error
	GetActiveMaintenanceWindow(now time.Time) (*MaintenanceWindow, error)
	GetAdminKPIsGeneral() (*AdminKPIsGeneral, error)
	CountUsersSince(since time.Time) (int, error)
	ListTransparencySettings() ([]TransparencySetting, error)
	UpsertTransparencySetting(key, value string, updatedBy int) error
	ListTransparencyContributors(includePrivate bool) ([]TransparencyContributor, error)
	GetTransparencyContributor(id int) (*TransparencyContributor, error)
	SaveTransparencyContributor(c *TransparencyContributor) (int, error)
	DeleteTransparencyContributor(id int) error
	InsertAdminImportRun(importType, status string, createdBy int) error
	CountAdminImportRunsSince(since time.Time) (AdminImportRunSummary, error)
	CreateAdminJob(job *AdminJob) (int, error)
	UpdateAdminJobProgress(id int, progressDone, progressTotal int) error
	UpdateAdminJobStatus(id int, status, errorText, resultJSON string, finishedAt *time.Time) error
	GetAdminJob(id int) (*AdminJob, error)
	ListAdminJobs(filter AdminJobFilter) ([]AdminJob, error)
	CountAdminJobs(filter AdminJobFilter) (int, error)
	InsertAdminAudit(entry *AdminAuditEntry) (int, error)
	ListAdminAudit(filter AdminAuditFilter) ([]AdminAuditEntry, error)
	CountAdminAudit(filter AdminAuditFilter) (int, error)
	ListAdminSessions(filter AdminSessionFilter) ([]AdminSessionRow, error)
	CountAdminSessions(filter AdminSessionFilter) (int, error)
	RevokeUserSessions(userID int) error
	// Missatgeria interna
	GetOrCreateDMThread(userA, userB int) (*DMThread, error)
	GetDMThreadByUsers(userA, userB int) (*DMThread, error)
	GetDMThreadByID(threadID int) (*DMThread, error)
	ListDMThreadsForUser(userID int, f DMThreadListFilter) ([]DMThreadListItem, error)
	CountDMUnread(userID int) (int, error)
	ListDMThreadFolders(userID int) ([]string, error)
	SetDMThreadFolder(threadID, userID int, folder string) error
	ListDMMessages(threadID, limit, beforeID int) ([]DMMessage, error)
	CreateDMMessage(threadID, senderID int, body string) (int, error)
	UpdateDMThreadLastMessage(threadID, msgID int, at time.Time) error
	MarkDMThreadRead(threadID, userID, lastMsgID int) error
	SetDMThreadArchived(threadID, userID int, archived bool) error
	SoftDeleteDMThread(threadID, userID int) error
	AddUserBlock(blockerID, blockedID int) error
	RemoveUserBlock(blockerID, blockedID int) error
	IsUserBlocked(blockerID, blockedID int) (bool, error)
	// Admin users
	ListUsersAdmin() ([]UserAdminRow, error)
	SetUserActive(userID int, active bool) error
	SetUserBanned(userID int, banned bool) error
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
	ListPoliticaGrants(politicaID int) ([]PoliticaGrant, error)
	SavePoliticaGrant(g *PoliticaGrant) (int, error)
	DeletePoliticaGrant(id int) error
	ListUserPolitiques(userID int) ([]Politica, error)
	AddUserPolitica(userID, politicaID int) error
	RemoveUserPolitica(userID, politicaID int) error
	ListGroupPolitiques(groupID int) ([]Politica, error)
	AddGroupPolitica(groupID, politicaID int) error
	RemoveGroupPolitica(groupID, politicaID int) error
	GetEffectivePoliticaPerms(userID int) (PolicyPermissions, error)
	GetUserPermissionsVersion(userID int) (int, error)
	BumpUserPermissionsVersion(userID int) error
	BumpGroupPermissionsVersion(groupID int) error
	BumpPolicyPermissionsVersion(politicaID int) error
	EnsureDefaultPointsRules() error
	EnsureDefaultAchievements() error
	// Punts i activitat
	ListPointsRules() ([]PointsRule, error)
	GetPointsRule(id int) (*PointsRule, error)
	GetPointsRuleByCode(code string) (*PointsRule, error)
	SavePointsRule(r *PointsRule) (int, error)
	ListUserIDs(limit, offset int) ([]int, error)
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
	// Achievements
	ListAchievements() ([]Achievement, error)
	ListEnabledAchievements() ([]Achievement, error)
	GetAchievement(id int) (*Achievement, error)
	GetAchievementByCode(code string) (*Achievement, error)
	SaveAchievement(a *Achievement) (int, error)
	AwardAchievement(userID, achievementID int, status, metaJSON string) (bool, error)
	ListUserAchievements(userID int) ([]AchievementUserView, error)
	ListUserShowcase(userID int) ([]AchievementShowcaseView, error)
	SetUserShowcaseSlot(userID, achievementID, slot int) error
	ClearUserShowcaseSlot(userID, slot int) error
	IsAchievementEventActive(code string, at time.Time) (bool, error)
	CountUserActivities(f AchievementActivityFilter) (int, error)
	CountUserActivitiesDistinctObject(f AchievementActivityFilter) (int, error)
	SumUserActivityPoints(f AchievementActivityFilter) (int, error)
	ListUserActivityDays(f AchievementActivityFilter) ([]time.Time, error)
	// Cognoms
	ListCognoms(q string, limit, offset int) ([]Cognom, error)
	GetCognom(id int) (*Cognom, error)
	FindCognomIDByKey(key string) (int, error)
	UpsertCognom(forma, key, origen, notes string, createdBy *int) (int, error)
	UpdateCognom(c *Cognom) error
	ListCognomVariants(f CognomVariantFilter) ([]CognomVariant, error)
	CreateCognomVariant(v *CognomVariant) (int, error)
	UpdateCognomVariantModeracio(id int, estat, motiu string, moderatorID int) error
	// Cognoms redirects (alias -> canònic)
	GetCognomRedirect(fromID int) (*CognomRedirect, error)
	ListCognomRedirects() ([]CognomRedirect, error)
	ListCognomRedirectsByTo(toID int) ([]CognomRedirect, error)
	SetCognomRedirect(fromID, toID int, createdBy *int, reason string) error
	DeleteCognomRedirect(fromID int) error
	// Cognoms merge suggestions (moderables)
	CreateCognomRedirectSuggestion(s *CognomRedirectSuggestion) (int, error)
	GetCognomRedirectSuggestion(id int) (*CognomRedirectSuggestion, error)
	ListCognomRedirectSuggestions(f CognomRedirectSuggestionFilter) ([]CognomRedirectSuggestion, error)
	UpdateCognomRedirectSuggestionModeracio(id int, estat, motiu string, moderatorID int) error
	// Cognoms referències (moderables)
	CreateCognomReferencia(ref *CognomReferencia) (int, error)
	ListCognomReferencies(f CognomReferenciaFilter) ([]CognomReferencia, error)
	UpdateCognomReferenciaModeracio(id int, estat, motiu string, moderatorID int) error
	// Cerca avançada
	UpsertSearchDoc(doc *SearchDoc) error
	GetSearchDoc(entityType string, entityID int) (*SearchDoc, error)
	DeleteSearchDoc(entityType string, entityID int) error
	SearchDocs(filter SearchQueryFilter) ([]SearchDocRow, int, SearchFacets, error)
	ReplaceAdminClosure(descendantMunicipiID int, entries []AdminClosureEntry) error
	ListAdminClosure(descendantMunicipiID int) ([]AdminClosureEntry, error)
	UpsertCognomFreqMunicipiAny(cognomID, municipiID, anyDoc, freq int) error
	QueryCognomHeatmap(cognomID int, anyStart, anyEnd int) ([]CognomFreqRow, error)
	ListCognomImportRows(limit, offset int) ([]CognomImportRow, error)
	ListCognomStatsRows(limit, offset int) ([]CognomStatsRow, error)
	RebuildCognomStats(cognomID int) error
	GetCognomStatsTotal(cognomID int) (*CognomStatsTotal, error)
	ListCognomStatsAny(cognomID int, from, to int) ([]CognomStatsAnyRow, error)
	ListCognomStatsAnyDecade(cognomID int, from, to int) ([]CognomStatsAnyRow, error)
	ListCognomStatsAncestor(cognomID int, ancestorType string, level, any, limit int) ([]CognomStatsAncestorRow, error)
	CountCognomStatsAncestorDistinct(cognomID int, ancestorType string, level, any int) (int, error)
	ResolveCognomPublicatByForma(forma string) (int, string, bool, error)
	ListCognomFormesPublicades(cognomID int) ([]string, error)
	// Noms
	UpsertNom(forma, key, notes string, createdBy *int) (int, error)
	GetNom(id int) (*Nom, error)
	ResolveNomByForma(forma string) (int, string, bool, error)
	UpsertNomFreqMunicipiAny(nomID, municipiID, anyDoc, delta int) error
	UpsertNomFreqMunicipiTotal(nomID, municipiID, delta int) error
	ApplyCognomFreqMunicipiAnyDelta(cognomID, municipiID, anyDoc, delta int) error
	UpsertCognomFreqMunicipiTotal(cognomID, municipiID, delta int) error
	ListTopNomsByMunicipi(municipiID, limit int) ([]NomTotalRow, error)
	ListTopCognomsByMunicipi(municipiID, limit int) ([]CognomTotalRow, error)
	ListNomSeries(municipiID, nomID int, bucket string) ([]NomFreqRow, error)
	ListCognomSeries(municipiID, cognomID int, bucket string) ([]CognomFreqRow, error)
	CountNomTotalsByMunicipi(municipiID int) (int, error)
	CountCognomTotalsByMunicipi(municipiID int) (int, error)
	ClearNomCognomStatsByMunicipi(municipiID int) error
	UpsertNomFreqNivellAny(nomID, nivellID, anyDoc, delta int) error
	UpsertNomFreqNivellTotal(nomID, nivellID, delta int) error
	ApplyCognomFreqNivellAnyDelta(cognomID, nivellID, anyDoc, delta int) error
	UpsertCognomFreqNivellTotal(cognomID, nivellID, delta int) error
	ListTopNomsByNivell(nivellID, limit int) ([]NomTotalRow, error)
	ListTopCognomsByNivell(nivellID, limit int) ([]CognomTotalRow, error)
	ListNomSeriesByNivell(nivellID, nomID int, bucket string) ([]NomFreqRow, error)
	ListCognomSeriesByNivell(nivellID, cognomID int, bucket string) ([]CognomFreqRow, error)
	ClearNomCognomStatsByNivell(nivellID int) error
	RebuildNivellNomCognomStats(nivellID int) error
	// Mapes municipi
	ListMunicipiMapes(filter MunicipiMapaFilter) ([]MunicipiMapa, error)
	GetMunicipiMapa(id int) (*MunicipiMapa, error)
	CreateMunicipiMapa(m *MunicipiMapa) (int, error)
	UpdateMunicipiMapa(m *MunicipiMapa) error
	UpdateMunicipiMapaCurrentVersion(mapaID, versionID int) error
	NextMunicipiMapaVersionNumber(mapaID int) (int, error)
	CreateMunicipiMapaVersion(v *MunicipiMapaVersion) (int, error)
	ListMunicipiMapaVersions(filter MunicipiMapaVersionFilter) ([]MunicipiMapaVersion, error)
	GetMunicipiMapaVersion(id int) (*MunicipiMapaVersion, error)
	SaveMunicipiMapaDraft(versionID int, jsonData, changelog string, expectedLock int) (int, error)
	UpdateMunicipiMapaVersionStatus(id int, status, notes string, moderatorID int) error
	ResolveMunicipiIDByMapaID(mapaID int) (int, error)
	ResolveMunicipiIDByMapaVersionID(versionID int) (int, error)

	// Historia municipi
	EnsureMunicipiHistoria(municipiID int) (*MunicipiHistoria, error)
	GetMunicipiHistoriaByMunicipiID(municipiID int) (*MunicipiHistoria, error)
	ResolveMunicipiIDByHistoriaGeneralVersionID(versionID int) (int, error)
	ResolveMunicipiIDByHistoriaFetVersionID(versionID int) (int, error)
	NextMunicipiHistoriaGeneralVersion(historiaID int) (int, error)
	CreateMunicipiHistoriaGeneralDraft(historiaID int, createdBy int, baseFromCurrent bool) (int, error)
	GetMunicipiHistoriaGeneralVersion(id int) (*MunicipiHistoriaGeneralVersion, error)
	UpdateMunicipiHistoriaGeneralDraft(v *MunicipiHistoriaGeneralVersion) error
	SetMunicipiHistoriaGeneralStatus(versionID int, status, notes string, moderatorID *int) error
	GetMunicipiHistoriaFet(id int) (*MunicipiHistoriaFet, error)
	CreateMunicipiHistoriaFet(municipiID int, createdBy int) (int, error)
	NextMunicipiHistoriaFetVersion(fetID int) (int, error)
	CreateMunicipiHistoriaFetDraft(fetID int, createdBy int, baseFromCurrent bool) (int, error)
	GetMunicipiHistoriaFetVersion(id int) (*MunicipiHistoriaFetVersion, error)
	UpdateMunicipiHistoriaFetDraft(v *MunicipiHistoriaFetVersion) error
	SetMunicipiHistoriaFetStatus(versionID int, status, notes string, moderatorID *int) error
	GetMunicipiHistoriaSummary(municipiID int) (*MunicipiHistoriaGeneralVersion, []MunicipiHistoriaFetVersion, error)
	ListMunicipiHistoriaTimeline(municipiID int, status string, limit, offset int, q string, anyFrom, anyTo *int) ([]MunicipiHistoriaFetVersion, int, error)
	ListPendingMunicipiHistoriaGeneralVersions(limit, offset int) ([]MunicipiHistoriaGeneralVersion, int, error)
	ListPendingMunicipiHistoriaFetVersions(limit, offset int) ([]MunicipiHistoriaFetVersion, int, error)

	// Demografia municipi
	GetMunicipiDemografiaMeta(municipiID int) (*MunicipiDemografiaMeta, error)
	ListMunicipiDemografiaAny(municipiID int, from, to int) ([]MunicipiDemografiaAny, error)
	ListMunicipiDemografiaDecades(municipiID int, from, to int) ([]MunicipiDemografiaAny, error)
	ApplyMunicipiDemografiaDelta(municipiID, year int, tipus string, delta int) error
	ApplyMunicipiDemografiaDeltaTx(tx *sql.Tx, municipiID, year int, tipus string, delta int) error
	RebuildMunicipiDemografia(municipiID int) error
	GetNivellDemografiaMeta(nivellID int) (*NivellDemografiaMeta, error)
	ListNivellDemografiaAny(nivellID int, from, to int) ([]NivellDemografiaAny, error)
	ListNivellDemografiaDecades(nivellID int, from, to int) ([]NivellDemografiaAny, error)
	ApplyNivellDemografiaDelta(nivellID, year int, tipus string, delta int) error
	RebuildNivellDemografia(nivellID int) error

	// Anecdotari municipi
	ListMunicipiAnecdotariPublished(municipiID int, f MunicipiAnecdotariFilter) ([]MunicipiAnecdotariVersion, int, error)
	GetMunicipiAnecdotariPublished(itemID int) (*MunicipiAnecdotariVersion, error)
	ListMunicipiAnecdotariComments(itemID int, limit, offset int) ([]MunicipiAnecdotariComment, int, error)
	CreateMunicipiAnecdotariItem(municipiID int, createdBy int) (int, error)
	CreateMunicipiAnecdotariDraft(itemID int, createdBy int, baseFromCurrent bool) (int, error)
	GetMunicipiAnecdotariVersion(id int) (*MunicipiAnecdotariVersion, error)
	GetPendingMunicipiAnecdotariVersionByItemID(itemID int) (*MunicipiAnecdotariVersion, error)
	UpdateMunicipiAnecdotariDraft(v *MunicipiAnecdotariVersion) error
	SubmitMunicipiAnecdotariVersion(versionID int) error
	ListPendingMunicipiAnecdotariVersions(limit, offset int) ([]MunicipiAnecdotariVersion, int, error)
	ApproveMunicipiAnecdotariVersion(versionID int, moderatorID int) error
	RejectMunicipiAnecdotariVersion(versionID int, moderatorID int, notes string) error
	CreateMunicipiAnecdotariComment(itemID int, userID int, body string) (int, error)
	GetMunicipiAnecdotariLastCommentAt(userID int) (time.Time, error)
	ResolveMunicipiIDByAnecdotariItemID(itemID int) (int, error)
	ResolveMunicipiIDByAnecdotariVersionID(versionID int) (int, error)

	// Esdeveniments historics
	CreateEventHistoric(e *EventHistoric) (int, error)
	GetEventHistoric(id int) (*EventHistoric, error)
	GetEventHistoricBySlug(slug string) (*EventHistoric, error)
	UpdateEventHistoric(e *EventHistoric) error
	ListEventsHistoric(filter EventHistoricFilter) ([]EventHistoric, error)
	UpdateEventHistoricModeracio(id int, estat, notes string, moderatorID int) error
	ListEventImpacts(eventID int) ([]EventHistoricImpact, error)
	ReplaceEventImpacts(eventID int, impacts []EventHistoricImpact) error
	ListEventsByScope(scopeType string, scopeID int, filter EventHistoricFilter) ([]EventHistoric, error)

	// Persones (moderació)
	ListPersones(filter PersonaFilter) ([]Persona, error)
	GetPersona(id int) (*Persona, error)
	CreatePersona(p *Persona) (int, error)
	UpdatePersona(p *Persona) error
	ListPersonaFieldLinks(personaID int) ([]PersonaFieldLink, error)
	UpsertPersonaFieldLink(personaID int, fieldKey string, registreID int, userID int) error
	// Anecdotari persona
	ListPersonaAnecdotes(personaID int, userID int) ([]PersonaAnecdote, error)
	CreatePersonaAnecdote(a *PersonaAnecdote) (int, error)
	UpdatePersonaModeracio(id int, estat, motiu string, moderatorID int) error
	UpdateArxiuModeracio(id int, estat, motiu string, moderatorID int) error
	UpdateLlibreModeracio(id int, estat, motiu string, moderatorID int) error
	UpdateNivellModeracio(id int, estat, motiu string, moderatorID int) error
	UpdateMunicipiModeracio(id int, estat, motiu string, moderatorID int) error
	UpdateArquebisbatModeracio(id int, estat, motiu string, moderatorID int) error
	UpdateTranscripcioModeracio(id int, estat, motiu string, moderatorID int) error
	UpdateTranscripcioModeracioWithDemografia(id int, estat, motiu string, moderatorID int, municipiID, year int, tipus string, delta int) error
	// Arxius CRUD
	ListArxius(filter ArxiuFilter) ([]ArxiuWithCount, error)
	CountArxius(filter ArxiuFilter) (int, error)
	CountPaisos() (int, error)
	GetArxiu(id int) (*Arxiu, error)
	CreateArxiu(a *Arxiu) (int, error)
	UpdateArxiu(a *Arxiu) error
	DeleteArxiu(id int) error
	InsertArxiuDonacioClick(arxiuID int, userID *int) error
	CountArxiuDonacioClicks(arxiuID int) (int, error)
	ListArxiuLlibres(arxiuID int) ([]ArxiuLlibreDetail, error)
	ListLlibreArxius(llibreID int) ([]ArxiuLlibreDetail, error)
	AddArxiuLlibre(arxiuID, llibreID int, signatura, urlOverride string) error
	UpdateArxiuLlibre(arxiuID, llibreID int, signatura, urlOverride string) error
	DeleteArxiuLlibre(arxiuID, llibreID int) error
	ListLlibreURLs(llibreID int) ([]LlibreURL, error)
	AddLlibreURL(link *LlibreURL) error
	DeleteLlibreURL(id int) error
	SearchLlibresSimple(q string, limit int) ([]LlibreSimple, error)
	ListLlibres(filter LlibreFilter) ([]LlibreRow, error)
	CountLlibres(filter LlibreFilter) (int, error)
	CountIndexedRegistres(status string) (int, error)
	GetLlibre(id int) (*Llibre, error)
	CreateLlibre(l *Llibre) (int, error)
	UpdateLlibre(l *Llibre) error
	HasLlibreDuplicate(municipiID int, tipus, cronologia, codiDigital, codiFisic string, excludeID int) (bool, error)
	GetLlibresIndexacioStats(ids []int) (map[int]LlibreIndexacioStats, error)
	UpsertLlibreIndexacioStats(stats *LlibreIndexacioStats) error
	ListLlibrePagines(llibreID int) ([]LlibrePagina, error)
	SearchLlibrePagines(llibreID int, query string, limit int) ([]LlibrePagina, error)
	GetLlibrePaginaByID(id int) (*LlibrePagina, error)
	GetLlibrePaginaByNum(llibreID, num int) (*LlibrePagina, error)
	SaveLlibrePagina(p *LlibrePagina) (int, error)
	RecalcLlibrePagines(llibreID, total int) error
	// Media
	ListMediaAlbumsByOwner(userID int) ([]MediaAlbum, error)
	ListMediaAlbumsByLlibre(llibreID int) ([]MediaAlbum, error)
	GetMediaAlbumByID(id int) (*MediaAlbum, error)
	GetMediaAlbumByPublicID(publicID string) (*MediaAlbum, error)
	CreateMediaAlbum(a *MediaAlbum) (int, error)
	ListMediaItemsByAlbum(albumID int) ([]MediaItem, error)
	ListMediaItemsByAlbumType(albumType, status string) ([]MediaItem, error)
	GetMediaItemByID(id int) (*MediaItem, error)
	GetMediaItemByPublicID(publicID string) (*MediaItem, error)
	CreateMediaItem(item *MediaItem) (int, error)
	UpdateMediaItemDerivativesStatus(itemID int, status string) error
	ListMediaAlbumsByStatus(status string) ([]MediaAlbum, error)
	ListMediaItemsByStatus(status string) ([]MediaItem, error)
	UpdateMediaAlbumModeration(id int, status, visibility string, restrictedGroupID, accessPolicyID, creditCost, difficultyScore int, sourceType, notes string, moderatorID int) error
	UpdateMediaItemModeration(id int, status string, creditCost int, notes string, moderatorID int) error
	// Media credits + grants
	GetUserCreditsBalance(userID int) (int, error)
	InsertUserCreditsLedger(entry *UserCreditsLedgerEntry) (int, error)
	GetActiveMediaAccessGrant(userID, mediaItemID int) (*MediaAccessGrant, error)
	GetMediaAccessGrantByToken(token string) (*MediaAccessGrant, error)
	CreateMediaAccessGrant(grant *MediaAccessGrant) (int, error)
	InsertMediaAccessLog(entry *MediaAccessLog) (int, error)
	// Media links to pages
	ListMediaItemLinksByPagina(paginaID int) ([]MediaItemPageLink, error)
	ListMediaItemLinksByAlbum(albumID int) ([]MediaItemPageLink, error)
	UpsertMediaItemPageLink(mediaItemID, llibreID, paginaID, pageOrder int, notes string) error
	DeleteMediaItemPageLink(mediaItemID, paginaID int) error
	CountMediaItemLinksByAlbum(albumID int) (map[int]int, error)
	SearchMediaItems(query string, limit int) ([]MediaItemSearchRow, error)
	// Transcripcions RAW
	ListTranscripcionsRaw(llibreID int, f TranscripcioFilter) ([]TranscripcioRaw, error)
	ListTranscripcionsRawGlobal(f TranscripcioFilter) ([]TranscripcioRaw, error)
	CountTranscripcionsRaw(llibreID int, f TranscripcioFilter) (int, error)
	CountTranscripcionsRawGlobal(f TranscripcioFilter) (int, error)
	CountTranscripcionsRawByPageValue(llibreID int, pageValue string) (int, error)
	ListTranscripcionsRawByPageValue(llibreID int, pageValue string) ([]TranscripcioRaw, error)
	GetTranscripcioRaw(id int) (*TranscripcioRaw, error)
	CreateTranscripcioRaw(t *TranscripcioRaw) (int, error)
	UpdateTranscripcioRaw(t *TranscripcioRaw) error
	DeleteTranscripcioRaw(id int) error
	ListTranscripcionsRawPageStats(llibreID int) ([]TranscripcioRawPageStat, error)
	UpdateTranscripcionsRawPageStat(stat *TranscripcioRawPageStat) error
	RecalcTranscripcionsRawPageStats(llibreID int) error
	SetTranscripcionsRawPageStatsIndexacio(llibreID int, value int) error
	DeleteTranscripcionsByLlibre(llibreID int) error
	CreateTranscripcioRawChange(c *TranscripcioRawChange) (int, error)
	GetTranscripcioRawChange(id int) (*TranscripcioRawChange, error)
	ListTranscripcioRawChanges(transcripcioID int) ([]TranscripcioRawChange, error)
	ListTranscripcioRawChangesPending() ([]TranscripcioRawChange, error)
	UpdateTranscripcioRawChangeModeracio(id int, estat, motiu string, moderatorID int) error
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
	// Wiki
	GetWikiMark(objectType string, objectID int, userID int) (*WikiMark, error)
	UpsertWikiMark(m *WikiMark) error
	DeleteWikiMark(objectType string, objectID int, userID int) error
	ListWikiMarks(objectType string, objectIDs []int) ([]WikiMark, error)
	IncWikiPublicCount(objectType string, objectID int, tipus string, delta int) error
	GetWikiPublicCounts(objectType string, objectID int) (map[string]int, error)
	CreateWikiChange(c *WikiChange) (int, error)
	GetWikiChange(id int) (*WikiChange, error)
	ListWikiChanges(objectType string, objectID int) ([]WikiChange, error)
	ListWikiChangesPending(objectType string, limit int) ([]WikiChange, error)
	UpdateWikiChangeModeracio(id int, estat, motiu string, moderatorID int) error
	EnqueueWikiPending(change *WikiChange) error
	DequeueWikiPending(changeID int) error
	ListWikiPending(limit int) ([]WikiPendingItem, error)
	// CSV import templates
	CreateCSVImportTemplate(t *CSVImportTemplate) (int, error)
	UpdateCSVImportTemplate(t *CSVImportTemplate) error
	GetCSVImportTemplate(id int) (*CSVImportTemplate, error)
	ListCSVImportTemplates(filter CSVImportTemplateFilter) ([]CSVImportTemplate, error)
	DeleteCSVImportTemplate(id int) error
	SearchPersones(f PersonaSearchFilter) ([]PersonaSearchResult, error)
	ListRegistresByPersona(personaID int, tipus string) ([]PersonaRegistreRow, error)
	GetPersonesByIDs(ids []int) (map[int]*Persona, error)
	FindBestBaptismeTranscripcioForPersona(personaID int) (int, bool, error)
	GetParentsFromTranscripcio(transcripcioID int) (int, int, error)

	// Paisos
	ListPaisos() ([]Pais, error)
	GetPais(id int) (*Pais, error)
	CreatePais(p *Pais) (int, error)
	UpdatePais(p *Pais) error

	// Nivells administratius
	ListNivells(f NivellAdminFilter) ([]NivellAdministratiu, error)
	CountNivells(f NivellAdminFilter) (int, error)
	GetNivell(id int) (*NivellAdministratiu, error)
	CreateNivell(n *NivellAdministratiu) (int, error)
	UpdateNivell(n *NivellAdministratiu) error

	// Municipis
	ListMunicipis(f MunicipiFilter) ([]MunicipiRow, error)
	CountMunicipis(f MunicipiFilter) (int, error)
	ListMunicipisBrowse(f MunicipiBrowseFilter) ([]MunicipiBrowseRow, error)
	CountMunicipisBrowse(f MunicipiBrowseFilter) (int, error)
	SuggestMunicipis(f MunicipiBrowseFilter) ([]MunicipiSuggestRow, error)
	GetMunicipi(id int) (*Municipi, error)
	CreateMunicipi(m *Municipi) (int, error)
	UpdateMunicipi(m *Municipi) error
	ListCodisPostals(municipiID int) ([]CodiPostal, error)
	SaveCodiPostal(cp *CodiPostal) (int, error)
	ListNomsHistorics(entitatTipus string, entitatID int) ([]NomHistoric, error)
	SaveNomHistoric(nh *NomHistoric) (int, error)

	// Entitats eclesiàstiques
	ListArquebisbats(f ArquebisbatFilter) ([]ArquebisbatRow, error)
	CountArquebisbats(f ArquebisbatFilter) (int, error)
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

type DashboardWidgetConfig struct {
	WidgetID     string
	Order        int
	Hidden       bool
	SettingsJSON string
}

type PlatformSetting struct {
	Key       string
	Value     string
	UpdatedBy sql.NullInt64
	UpdatedAt sql.NullTime
}

type MaintenanceWindow struct {
	ID          int
	Title       string
	Message     string
	Severity    string
	ShowFrom    string
	StartsAt    string
	EndsAt      string
	CTALabel    string
	CTAURL      string
	IsEnabled   bool
	Dismissible bool
	CreatedBy   sql.NullInt64
	UpdatedBy   sql.NullInt64
	CreatedAt   string
	UpdatedAt   string
}

type AdminKPIsGeneral struct {
	TotalUsers             int
	ActiveUsers            int
	ContributorUsers       int
	ValidatedContributions int
}

type AdminImportRunSummary struct {
	Ok    int
	Error int
}

type AdminJob struct {
	ID            int
	Kind          string
	Status        string
	ProgressTotal int
	ProgressDone  int
	PayloadJSON   string
	ResultJSON    string
	ErrorText     string
	StartedAt     sql.NullTime
	FinishedAt    sql.NullTime
	CreatedAt     sql.NullTime
	UpdatedAt     sql.NullTime
	CreatedBy     sql.NullInt64
}

type AdminJobFilter struct {
	Kind      string
	Status    string
	CreatedBy int
	Limit     int
	Offset    int
}

type AdminAuditEntry struct {
	ID           int
	ActorID      sql.NullInt64
	Action       string
	ObjectType   string
	ObjectID     sql.NullInt64
	MetadataJSON string
	IP           string
	CreatedAt    sql.NullTime
}

type AdminAuditFilter struct {
	Action     string
	ActorID    int
	ObjectType string
	Limit      int
	Offset     int
}

type AdminSessionRow struct {
	ID           int
	UserID       int
	Username     string
	Nom          string
	Cognoms      string
	CreatedAt    sql.NullTime
	ExpiresAt    sql.NullTime
	LastAccessAt sql.NullTime
	Revoked      bool
}

type AdminSessionFilter struct {
	UserID     int
	ActiveOnly bool
	Limit      int
	Offset     int
}

type TransparencySetting struct {
	Key       string
	Value     string
	UpdatedBy sql.NullInt64
	UpdatedAt sql.NullTime
}

type TransparencyContributor struct {
	ID          int
	Name        string
	Type        string
	Description string
	Amount      sql.NullFloat64
	Currency    string
	URL         string
	IsPublic    bool
	SortOrder   int
	CreatedBy   sql.NullInt64
	UpdatedBy   sql.NullInt64
	CreatedAt   string
	UpdatedAt   string
}

type UserAdminRow struct {
	ID        int
	Usuari    string
	Nom       string
	Cognoms   string
	Email     string
	CreatedAt string
	LastLogin string
	Active    bool
	Banned    bool
}

type UserAdminFilter struct {
	UserID int
	Query  string
	Active *bool
	Banned *bool
	Limit  int
	Offset int
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

type DMThread struct {
	ID            int
	UserLowID     int
	UserHighID    int
	CreatedAt     sql.NullTime
	LastMessageAt sql.NullTime
	LastMessageID sql.NullInt64
}

type DMThreadState struct {
	ThreadID          int
	UserID            int
	LastReadMessageID sql.NullInt64
	Archived          bool
	Muted             bool
	Deleted           bool
	UpdatedAt         sql.NullTime
}

type DMMessage struct {
	ID        int
	ThreadID  int
	SenderID  int
	Body      string
	CreatedAt sql.NullTime
}

type DMThreadListFilter struct {
	ThreadID int
	Archived *bool
	Deleted  *bool
	Folder   *string
	Limit    int
	Offset   int
}

type DMThreadListItem struct {
	ThreadID             int
	OtherUserID          int
	ThreadCreatedAt      sql.NullTime
	LastMessageID        sql.NullInt64
	LastMessageAt        sql.NullTime
	LastMessageBody      string
	LastMessageSenderID  sql.NullInt64
	LastMessageCreatedAt sql.NullTime
	LastReadMessageID    sql.NullInt64
	Folder               string
	Archived             bool
	Muted                bool
	Deleted              bool
	Unread               bool
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

type Achievement struct {
	ID              int
	Code            string
	Name            string
	Description     string
	Rarity          string
	Visibility      string
	Domain          string
	IsEnabled       bool
	IsRepeatable    bool
	IconMediaItemID sql.NullInt64
	RuleJSON        string
	CreatedAt       sql.NullTime
	UpdatedAt       sql.NullTime
}

type AchievementUser struct {
	ID            int
	UserID        int
	AchievementID int
	AwardedAt     sql.NullTime
	Status        string
	MetaJSON      sql.NullString
}

type AchievementUserView struct {
	AchievementID   int
	Code            string
	Name            string
	Description     string
	Rarity          string
	Visibility      string
	Domain          string
	IconMediaItemID sql.NullInt64
	IconPublicID    sql.NullString
	AwardedAt       sql.NullTime
	Status          string
	MetaJSON        sql.NullString
}

type AchievementShowcase struct {
	UserID        int
	AchievementID int
	Slot          int
	CreatedAt     sql.NullTime
}

type AchievementShowcaseView struct {
	Slot            int
	AchievementID   int
	Code            string
	Name            string
	Description     string
	Rarity          string
	Visibility      string
	Domain          string
	IconMediaItemID sql.NullInt64
	IconPublicID    sql.NullString
	AwardedAt       sql.NullTime
	Status          string
	MetaJSON        sql.NullString
}

type ActivityFilter struct {
	Status     string
	ObjectType string
	Limit      int
	Offset     int
	From       time.Time
	To         time.Time
}

type AchievementActivityFilter struct {
	UserID      int
	RuleCodes   []string
	Actions     []string
	ObjectTypes []string
	Statuses    []string
	From        time.Time
	To          time.Time
}

type RankingFilter struct {
	PreferredLang string
	Limit         int
	Offset        int
	PublicOnly    bool
}

type Cognom struct {
	ID        int
	Forma     string
	Key       string
	Origen    string
	Notes     string
	CreatedBy sql.NullInt64
	CreatedAt sql.NullTime
	UpdatedAt sql.NullTime
}

type Nom struct {
	ID        int
	Forma     string
	Key       string
	Notes     string
	CreatedBy sql.NullInt64
	CreatedAt sql.NullTime
	UpdatedAt sql.NullTime
}

type CognomVariant struct {
	ID             int
	CognomID       int
	Variant        string
	Key            string
	Llengua        string
	AnyInici       sql.NullInt64
	AnyFi          sql.NullInt64
	PaisID         sql.NullInt64
	MunicipiID     sql.NullInt64
	ModeracioEstat string
	ModeracioMotiu string
	ModeratedBy    sql.NullInt64
	ModeratedAt    sql.NullTime
	CreatedBy      sql.NullInt64
	CreatedAt      sql.NullTime
	UpdatedAt      sql.NullTime
}

type CognomVariantFilter struct {
	CognomID int
	Status   string
	Q        string
	Limit    int
	Offset   int
}

type CognomRedirect struct {
	FromCognomID int
	ToCognomID   int
	Reason       string
	CreatedBy    sql.NullInt64
	CreatedAt    sql.NullTime
}

type CognomRedirectSuggestion struct {
	ID             int
	FromCognomID   int
	ToCognomID     int
	Reason         string
	ModeracioEstat string
	ModeracioMotiu string
	ModeratedBy    sql.NullInt64
	ModeratedAt    sql.NullTime
	CreatedBy      sql.NullInt64
	CreatedAt      sql.NullTime
}

type CognomRedirectSuggestionFilter struct {
	Status       string
	FromCognomID int
	ToCognomID   int
	Limit        int
	Offset       int
}

type CognomReferencia struct {
	ID             int
	CognomID       int
	Kind           string
	RefID          sql.NullInt64
	URL            string
	Titol          string
	Descripcio     string
	Pagina         string
	ModeracioEstat string
	ModeracioMotiu string
	ModeratedBy    sql.NullInt64
	ModeratedAt    sql.NullTime
	CreatedBy      sql.NullInt64
	CreatedAt      sql.NullTime
}

type CognomReferenciaFilter struct {
	CognomID int
	Status   string
	Limit    int
	Offset   int
}

type CognomFreqRow struct {
	MunicipiID  int
	MunicipiNom sql.NullString
	Latitud     sql.NullFloat64
	Longitud    sql.NullFloat64
	AnyDoc      int
	Freq        int
}

type CognomImportRow struct {
	Cognom1      sql.NullString
	Cognom1Estat sql.NullString
	Cognom2      sql.NullString
	Cognom2Estat sql.NullString
}

type CognomStatsRow struct {
	Cognom1      sql.NullString
	Cognom1Estat sql.NullString
	Cognom2      sql.NullString
	Cognom2Estat sql.NullString
	AnyDoc       sql.NullInt64
	MunicipiID   sql.NullInt64
}

type CognomStatsTotal struct {
	CognomID        int
	TotalPersones   int
	TotalAparicions int
	UpdatedAt       sql.NullTime
}

type CognomStatsAnyRow struct {
	CognomID int
	Any      int
	Total    int
}

type CognomStatsAncestorRow struct {
	CognomID     int
	AncestorType string
	AncestorID   int
	Any          int
	Total        int
	Label        string
	Level        int
}

type NomFreqRow struct {
	NomID      int
	MunicipiID int
	AnyDoc     int
	Freq       int
}

type NomTotalRow struct {
	NomID      int
	MunicipiID int
	TotalFreq  int
	Forma      string
}

type CognomTotalRow struct {
	CognomID   int
	MunicipiID int
	TotalFreq  int
	Forma      string
}

type MunicipiMapa struct {
	ID               int
	MunicipiID       int
	GroupType        string
	Title            string
	PeriodLabel      string
	PeriodStart      sql.NullInt64
	PeriodEnd        sql.NullInt64
	Topic            string
	CurrentVersionID sql.NullInt64
	CreatedBy        sql.NullInt64
	CreatedAt        sql.NullTime
	UpdatedAt        sql.NullTime
}

type MunicipiMapaFilter struct {
	MunicipiID int
	GroupType  string
	CreatedBy  int
	Limit      int
	Offset     int
}

type MunicipiMapaVersion struct {
	ID              int
	MapaID          int
	Version         int
	Status          string
	JSONData        string
	Changelog       string
	LockVersion     int
	CreatedBy       sql.NullInt64
	CreatedAt       sql.NullTime
	ModeratedBy     sql.NullInt64
	ModeratedAt     sql.NullTime
	ModerationNotes string
}

type MunicipiMapaVersionFilter struct {
	MapaID    int
	Status    string
	CreatedBy int
	Limit     int
	Offset    int
}

type MunicipiHistoria struct {
	ID                      int
	MunicipiID              int
	CurrentGeneralVersionID sql.NullInt64
	CreatedAt               sql.NullTime
	UpdatedAt               sql.NullTime
}

type MunicipiHistoriaGeneralVersion struct {
	ID              int
	HistoriaID      int
	MunicipiID      int
	MunicipiNom     string
	Version         int
	Titol           string
	Resum           string
	CosText         string
	TagsJSON        string
	Status          string
	ModerationNotes string
	LockVersion     int
	CreatedBy       sql.NullInt64
	CreatedAt       sql.NullTime
	UpdatedAt       sql.NullTime
	ModeratedBy     sql.NullInt64
	ModeratedAt     sql.NullTime
}

type MunicipiHistoriaFet struct {
	ID               int
	MunicipiID       int
	CurrentVersionID sql.NullInt64
	CreatedAt        sql.NullTime
	UpdatedAt        sql.NullTime
}

type MunicipiHistoriaFetVersion struct {
	ID              int
	FetID           int
	MunicipiID      int
	MunicipiNom     string
	Version         int
	AnyInici        sql.NullInt64
	AnyFi           sql.NullInt64
	DataInici       string
	DataFi          string
	DataDisplay     string
	Titol           string
	Resum           string
	CosText         string
	TagsJSON        string
	FontsJSON       string
	Status          string
	ModerationNotes string
	LockVersion     int
	CreatedBy       sql.NullInt64
	CreatedAt       sql.NullTime
	UpdatedAt       sql.NullTime
	ModeratedBy     sql.NullInt64
	ModeratedAt     sql.NullTime
}

type MunicipiHistoriaVersionFilter struct {
	MunicipiID int
	Status     string
	Limit      int
	Offset     int
}

type MunicipiDemografiaAny struct {
	MunicipiID int
	Any        int
	Natalitat  int
	Matrimonis int
	Defuncions int
	UpdatedAt  sql.NullTime
}

type MunicipiDemografiaMeta struct {
	MunicipiID      int
	AnyMin          sql.NullInt64
	AnyMax          sql.NullInt64
	TotalNatalitat  int
	TotalMatrimonis int
	TotalDefuncions int
	UpdatedAt       sql.NullTime
}

type NivellDemografiaAny struct {
	NivellID   int
	Any        int
	Natalitat  int
	Matrimonis int
	Defuncions int
	UpdatedAt  sql.NullTime
}

type NivellDemografiaMeta struct {
	NivellID        int
	AnyMin          sql.NullInt64
	AnyMax          sql.NullInt64
	TotalNatalitat  int
	TotalMatrimonis int
	TotalDefuncions int
	UpdatedAt       sql.NullTime
}

type DemografiaQueueItem struct {
	ID          int
	MunicipiID  int
	Tipus       string
	Any         int
	Delta       int
	Source      string
	SourceID    string
	CreatedAt   sql.NullTime
	ProcessedAt sql.NullTime
}

type MunicipiAnecdotariItem struct {
	ID               int
	MunicipiID       int
	CurrentVersionID sql.NullInt64
	CreatedBy        sql.NullInt64
	CreatedAt        sql.NullTime
	UpdatedAt        sql.NullTime
}

type MunicipiAnecdotariVersion struct {
	ID              int
	ItemID          int
	MunicipiID      int
	MunicipiNom     string
	Version         int
	Status          string
	Titol           string
	Tag             string
	DataRef         string
	Text            string
	FontURL         string
	ModerationNotes string
	LockVersion     int
	CreatedBy       sql.NullInt64
	CreatedAt       sql.NullTime
	UpdatedAt       sql.NullTime
	ModeratedBy     sql.NullInt64
	ModeratedAt     sql.NullTime
}

type MunicipiAnecdotariComment struct {
	ID        int
	ItemID    int
	UserID    int
	Body      string
	CreatedAt sql.NullTime
}

type MunicipiAnecdotariFilter struct {
	Tag    string
	Query  string
	Status string
	Limit  int
	Offset int
}

type EventHistoric struct {
	ID               int
	Titol            string
	Slug             string
	Tipus            string
	Resum            string
	Descripcio       string
	DataInici        string
	DataFi           string
	DataIniciAprox   bool
	DataFiAprox      bool
	Precisio         string
	Fonts            string
	CreatedBy        sql.NullInt64
	ModerationStatus string
	ModeratedBy      sql.NullInt64
	ModeratedAt      sql.NullTime
	ModerationNotes  string
	CreatedAt        sql.NullTime
	UpdatedAt        sql.NullTime
}

type EventHistoricImpact struct {
	ID           int
	EventID      int
	ScopeType    string
	ScopeID      int
	ImpacteTipus string
	Intensitat   int
	Notes        string
	CreatedBy    sql.NullInt64
	CreatedAt    sql.NullTime
	UpdatedAt    sql.NullTime
}

type EventHistoricFilter struct {
	Query         string
	Tipus         string
	Status        string
	ImpacteTipus  string
	IntensitatMin int
	OnlyWithDates bool
	OrderBy       string
	From          time.Time
	To            time.Time
	Limit         int
	Offset        int
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

type PoliticaGrant struct {
	ID              int
	PoliticaID      int
	PermKey         string
	ScopeType       string
	ScopeID         sql.NullInt64
	IncludeChildren bool
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
	ID                int
	Nom               string
	Cognom1           string
	Cognom2           string
	Municipi          string
	MunicipiNaixement string
	MunicipiDefuncio  string
	Arquebisbat       string
	NomComplet        string
	Pagina            string
	Llibre            string
	Quinta            string
	DataNaixement     sql.NullString
	DataBateig        sql.NullString
	DataDefuncio      sql.NullString
	Ofici             string
	EstatCivil        string
	ModeracioEstat    string
	ModeracioMotiu    string
	CreatedBy         sql.NullInt64
	CreatedAt         sql.NullTime
	UpdatedAt         sql.NullTime
	UpdatedBy         sql.NullInt64
	ModeratedBy       sql.NullInt64
	ModeratedAt       sql.NullTime
}

type PersonaFieldLink struct {
	ID         int
	PersonaID  int
	FieldKey   string
	RegistreID int
	CreatedBy  sql.NullInt64
	CreatedAt  sql.NullTime
}

type PersonaFilter struct {
	Estat string
	Limit int
}

type PersonaAnecdote struct {
	ID        int
	PersonaID int
	UserID    int
	UserName  sql.NullString
	Title     string
	Body      string
	Tag       string
	Status    string
	CreatedAt sql.NullTime
	UpdatedAt sql.NullTime
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
	CreatedAt      sql.NullTime
	ModeracioEstat string
	ModeracioMotiu string
	ModeratedBy    sql.NullInt64
	ModeratedAt    sql.NullTime
}

type NivellAdminFilter struct {
	PaisID         int
	Nivel          int
	Text           string
	Estat          string
	Status         string
	Limit          int
	Offset         int
	AllowedPaisIDs []int
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
	PaisID         sql.NullInt64
	ProvinciaID    sql.NullInt64
	ComarcaID      sql.NullInt64
	PaisNom        sql.NullString
	ProvNom        sql.NullString
	Comarca        sql.NullString
	ModeracioEstat string
	CreatedBy      sql.NullInt64
	CreatedAt      sql.NullTime
}

type MunicipiFilter struct {
	Text                string
	Estat               string
	PaisID              int
	NivellID            int
	Status              string
	Limit               int
	Offset              int
	AllowedMunicipiIDs  []int
	AllowedProvinciaIDs []int
	AllowedComarcaIDs   []int
	AllowedNivellIDs    []int
	AllowedPaisIDs      []int
}

type MunicipiBrowseFilter struct {
	Text                string
	Estat               string
	Status              string
	PaisID              int
	MunicipiID          int
	NivellID            int
	Tipus               string
	LevelIDs            [7]int
	Sort                string
	SortDir             string
	Limit               int
	Offset              int
	AllowedMunicipiIDs  []int
	AllowedProvinciaIDs []int
	AllowedComarcaIDs   []int
	AllowedNivellIDs    []int
	AllowedPaisIDs      []int
}

type MunicipiBrowseRow struct {
	ID                int
	Nom               string
	Tipus             string
	Estat             string
	CodiPostal        string
	ModeracioEstat    string
	LevelIDs          [7]sql.NullInt64
	LevelNames        [7]sql.NullString
	Latitud           sql.NullFloat64
	Longitud          sql.NullFloat64
	RegistresTotal    int64
	RegistresIndexats int64
}

type MunicipiSuggestRow struct {
	ID         int
	Nom        string
	Tipus      string
	PaisID     int
	LevelIDs   [7]sql.NullInt64
	LevelNames [7]sql.NullString
	LevelTypes [7]sql.NullString
	Latitud    sql.NullFloat64
	Longitud   sql.NullFloat64
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
	PaisID         int
	PaisNom        sql.NullString
	Nivell         sql.NullInt64
	ParentNom      sql.NullString
	AnyInici       sql.NullInt64
	AnyFi          sql.NullInt64
	ModeracioEstat string
	CreatedBy      sql.NullInt64
	CreatedAt      sql.NullTime
}

type ArquebisbatFilter struct {
	Text            string
	PaisID          int
	Status          string
	Limit           int
	Offset          int
	AllowedEclesIDs []int
	AllowedPaisIDs  []int
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
	What3Words            string
	Web                   string
	Acces                 string
	Notes                 string
	AcceptaDonacions      bool
	DonacionsURL          string
	CreatedBy             sql.NullInt64
	CreatedAt             sql.NullTime
	ModeracioEstat        string
	ModeracioMotiu        string
	ModeratedBy           sql.NullInt64
	ModeratedAt           sql.NullTime
}

type ArxiuFilter struct {
	Text                string
	Tipus               string
	Acces               string
	EntitatID           int
	MunicipiID          int
	PaisID              int
	Limit               int
	Offset              int
	Status              string
	AllowedArxiuIDs     []int
	AllowedMunicipiIDs  []int
	AllowedProvinciaIDs []int
	AllowedComarcaIDs   []int
	AllowedNivellIDs    []int
	AllowedPaisIDs      []int
	AllowedEclesIDs     []int
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
	TipusLlibre string
	NomEsglesia string
	Cronologia  string
	Municipi    sql.NullString
	ArxiuNom    sql.NullString
	Signatura   sql.NullString
	URLOverride sql.NullString
	Pagines     sql.NullInt64
}

type LlibreURL struct {
	ID             int
	LlibreID       int
	ArxiuID        sql.NullInt64
	LlibreRefID    sql.NullInt64
	ArxiuNom       sql.NullString
	LlibreRefTitol sql.NullString
	URL            string
	Tipus          sql.NullString
	Descripcio     sql.NullString
	CreatedBy      sql.NullInt64
	CreatedAt      sql.NullTime
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
	CreatedAt         sql.NullTime
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
	ID                int
	LlibreID          int
	PaginaID          sql.NullInt64
	NumPaginaText     string
	TipusPagina       string
	Exclosa           int
	IndexacioCompleta int
	DuplicadaDe       sql.NullString
	TotalRegistres    int
	ComputedAt        sql.NullTime
}

type LlibreFilter struct {
	Text                string
	Cronologia          string
	ArquebisbatID       int
	MunicipiID          int
	ArxiuID             int
	ArxiuTipus          string
	TipusLlibre         string
	Status              string
	Limit               int
	Offset              int
	AllowedLlibreIDs    []int
	AllowedArxiuIDs     []int
	AllowedMunicipiIDs  []int
	AllowedProvinciaIDs []int
	AllowedComarcaIDs   []int
	AllowedNivellIDs    []int
	AllowedPaisIDs      []int
	AllowedEclesIDs     []int
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

// Media
type MediaAlbum struct {
	ID                int
	PublicID          string
	Title             string
	Description       string
	AlbumType         string
	OwnerUserID       int
	LlibreID          sql.NullInt64
	ModerationStatus  string
	Visibility        string
	RestrictedGroupID sql.NullInt64
	AccessPolicyID    sql.NullInt64
	CreditCost        int
	DifficultyScore   int
	SourceType        string
	ModeratedBy       sql.NullInt64
	ModeratedAt       sql.NullTime
	ModerationNotes   string
	ItemsCount        int
}

type MediaItem struct {
	ID                 int
	PublicID           string
	AlbumID            int
	Title              string
	OriginalFilename   string
	MimeType           string
	ByteSize           int64
	Width              int
	Height             int
	ChecksumSHA256     string
	StorageKeyOriginal string
	ThumbPath          string
	DerivativesStatus  string
	ModerationStatus   string
	ModeratedBy        sql.NullInt64
	ModeratedAt        sql.NullTime
	ModerationNotes    string
	CreditCost         int
}

type MediaItemPage struct {
	ID          int
	MediaItemID int
	LlibreID    sql.NullInt64
	PaginaID    sql.NullInt64
	PageOrder   int
	Notes       string
}

type MediaItemPageLink struct {
	ID                     int
	MediaItemID            int
	LlibreID               sql.NullInt64
	PaginaID               sql.NullInt64
	NumPagina              sql.NullInt64
	MediaItemPublicID      string
	MediaItemTitle         string
	MediaItemThumbPath     string
	MediaItemStatus        string
	AlbumID                int
	AlbumPublicID          string
	AlbumTitle             string
	AlbumOwnerUserID       int
	AlbumModerationStatus  string
	AlbumVisibility        string
	AlbumRestrictedGroupID sql.NullInt64
	AlbumAccessPolicyID    sql.NullInt64
	PageOrder              int
	Notes                  string
}

type MediaItemSearchRow struct {
	MediaItemID            int
	MediaItemPublicID      string
	MediaItemTitle         string
	MediaItemThumb         string
	MediaItemStatus        string
	AlbumID                int
	AlbumPublicID          string
	AlbumTitle             string
	AlbumOwnerUserID       int
	AlbumStatus            string
	AlbumVisibility        string
	AlbumRestrictedGroupID sql.NullInt64
	AlbumAccessPolicyID    sql.NullInt64
}

type UserCreditsLedgerEntry struct {
	ID        int
	UserID    int
	Delta     int
	Reason    string
	RefType   sql.NullString
	RefID     sql.NullInt64
	CreatedAt time.Time
}

type MediaAccessGrant struct {
	ID           int
	UserID       int
	MediaItemID  int
	GrantToken   string
	ExpiresAt    time.Time
	CreditsSpent int
	CreatedAt    time.Time
}

type MediaAccessLog struct {
	ID           int
	UserID       int
	MediaItemID  int
	AccessType   string
	CreditsSpent int
	CreatedAt    time.Time
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

type SearchDoc struct {
	ID                    int
	EntityType            string
	EntityID              int
	Published             bool
	MunicipiID            sql.NullInt64
	ArxiuID               sql.NullInt64
	LlibreID              sql.NullInt64
	EntitatEclesiasticaID sql.NullInt64
	DataActe              sql.NullString
	AnyActe               sql.NullInt64
	PersonNomNorm         string
	PersonCognomsNorm     string
	PersonFullNorm        string
	PersonTokensNorm      string
	CognomsTokensNorm     string
	PersonPhonetic        string
	CognomsPhonetic       string
	CognomsCanon          string
}

type SearchQueryFilter struct {
	Query                 string
	QueryNorm             string
	QueryPhonetic         string
	QueryTokens           []string
	CanonTokens           []string
	VariantTokens         []string
	Name                  string
	Surname1              string
	Surname2              string
	NameNorm              string
	SurnameNorm           string
	NameTokens            []string
	SurnameTokens         []string
	SurnameTokens1        []string
	SurnameTokens2        []string
	Father                string
	Mother                string
	Partner               string
	FatherTokens          []string
	MotherTokens          []string
	PartnerTokens         []string
	Exact                 bool
	OnlySurnameDirect     bool
	Entity                string
	AncestorType          string
	AncestorID            int
	EntitatEclesiasticaID int
	ArxiuID               int
	LlibreID              int
	DateFrom              string
	DateTo                string
	AnyFrom               int
	AnyTo                 int
	TipusActe             string
	Page                  int
	PageSize              int
	Sort                  string
	IncludeUnpublished    bool
}

type SearchDocRow struct {
	SearchDoc
	TipusActe sql.NullString
	Score     int
}

type SearchFacets struct {
	EntityType map[string]int
	TipusActe  map[string]int
}

type AdminClosureEntry struct {
	DescendantMunicipiID int
	AncestorType         string
	AncestorID           int
}

type TranscripcioPersonaRaw struct {
	ID                 int
	TranscripcioID     int
	Rol                string
	Nom                string
	NomEstat           string
	Cognom1            string
	Cognom1Estat       string
	Cognom2            string
	Cognom2Estat       string
	CognomSoltera      string
	CognomSolteraEstat string
	Sexe               string
	SexeEstat          string
	EdatText           string
	EdatEstat          string
	EstatCivilText     string
	EstatCivilEstat    string
	MunicipiText       string
	MunicipiEstat      string
	OficiText          string
	OficiEstat         string
	CasaNom            string
	CasaEstat          string
	PersonaID          sql.NullInt64
	LinkedBy           sql.NullInt64
	LinkedAt           sql.NullTime
	Notes              string
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
	ModeracioEstat string
	ModeratedBy    sql.NullInt64
	ModeratedAt    sql.NullTime
	ModeracioMotiu string
	ChangedBy      sql.NullInt64
	ChangedAt      time.Time
}

type WikiMark struct {
	ID         int
	ObjectType string
	ObjectID   int
	UserID     int
	Tipus      string
	IsPublic   bool
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type WikiChange struct {
	ID             int
	ObjectType     string
	ObjectID       int
	ChangeType     string
	FieldKey       string
	OldValue       string
	NewValue       string
	Metadata       string
	ModeracioEstat string
	ModeratedBy    sql.NullInt64
	ModeratedAt    sql.NullTime
	ModeracioMotiu string
	ChangedBy      sql.NullInt64
	ChangedAt      time.Time
}

type WikiPendingItem struct {
	ChangeID   int
	ObjectType string
	ObjectID   int
	ChangedAt  time.Time
	ChangedBy  sql.NullInt64
	CreatedAt  time.Time
}

type CSVImportTemplate struct {
	ID               int
	Name             string
	Description      string
	OwnerUserID      sql.NullInt64
	Visibility       string
	DefaultSeparator string
	ModelJSON        string
	Signature        string
	CreatedAt        sql.NullTime
	UpdatedAt        sql.NullTime
}

type CSVImportTemplateFilter struct {
	OwnerUserID   int
	IncludePublic bool
	Query         string
	Limit         int
	Offset        int
}

type PersonaSearchFilter struct {
	Query               string
	Nom                 string
	Cognom1             string
	Cognom2             string
	Municipi            string
	AnyMin              int
	AnyMax              int
	Limit               int
	UseCognomDictionary bool
	ExpandedCognoms     []string
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
		if config["RECREADB"] == "true" {
			path := strings.TrimSpace(config["DB_PATH"])
			if path != "" {
				if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
					logInfof("No s'ha pogut eliminar el fitxer SQLite %s: %v", path, err)
				}
			}
		}
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
	} else {
		ready, err := baseSchemaReady(engine, dbInstance)
		if err != nil {
			return nil, fmt.Errorf("error comprovant esquema BD (%s): %v", engine, err)
		}
		if !ready {
			return nil, fmt.Errorf("la BD no te esquema base; cal RECREADB=true o executar %s manualment", getSQLFilePath(engine))
		}
	}
	if err := ensureMapTables(engine, dbInstance); err != nil {
		return nil, fmt.Errorf("error assegurant taules mapes (%s): %v", engine, err)
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
	if err := resetDatabase(engine, db); err != nil {
		return fmt.Errorf("error netejant BD (%s): %w", engine, err)
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
	var parts []string
	if engine == "postgres" {
		parts = splitSQLStatements(cleanSQL)
	} else {
		parts = strings.Split(cleanSQL, ";")
	}

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
			if engine == "mysql" {
				handled, fixErr := handleMySQLMissingIndexColumn(db, q, err)
				if handled {
					if fixErr != nil {
						return fmt.Errorf("error arreglant index MySQL: %w", fixErr)
					}
					continue
				}
			}
			if shouldIgnoreSQLError(engine, q, err) {
				continue
			}
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

func resetDatabase(engine string, db DB) error {
	if db == nil {
		return nil
	}
	switch engine {
	case "postgres":
		rows, err := db.Query("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
		if err != nil {
			return err
		}
		for _, row := range rows {
			name := strings.TrimSpace(stringFromRowValue(rowValueByKey(row, "tablename")))
			if name == "" {
				continue
			}
			stmt := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", quoteIdent(engine, name))
			if _, err := db.Exec(stmt); err != nil {
				return err
			}
		}
		typeRows, err := db.Query("SELECT t.typname FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace WHERE n.nspname = 'public' AND t.typtype = 'c'")
		if err != nil {
			return err
		}
		for _, row := range typeRows {
			name := strings.TrimSpace(stringFromRowValue(rowValueByKey(row, "typname")))
			if name == "" {
				continue
			}
			typeStmt := fmt.Sprintf("DROP TYPE IF EXISTS %s CASCADE", quoteIdent(engine, name))
			if _, err := db.Exec(typeStmt); err != nil {
				return err
			}
		}
	case "mysql":
		_, _ = db.Exec("SET FOREIGN_KEY_CHECKS=0")
		rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()")
		if err != nil {
			_, _ = db.Exec("SET FOREIGN_KEY_CHECKS=1")
			return err
		}
		for _, row := range rows {
			name := strings.TrimSpace(stringFromRowValue(rowValueByKey(row, "table_name")))
			if name == "" {
				continue
			}
			stmt := fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdent(engine, name))
			if _, err := db.Exec(stmt); err != nil {
				_, _ = db.Exec("SET FOREIGN_KEY_CHECKS=1")
				return err
			}
		}
		_, _ = db.Exec("SET FOREIGN_KEY_CHECKS=1")
	}
	return nil
}

func splitSQLStatements(sql string) []string {
	var stmts []string
	var b strings.Builder
	inSingle := false
	inDouble := false
	inDollar := false
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if inDollar {
			if ch == '$' && i+1 < len(sql) && sql[i+1] == '$' {
				inDollar = false
				b.WriteByte(ch)
				b.WriteByte('$')
				i++
				continue
			}
			b.WriteByte(ch)
			continue
		}
		if inSingle {
			if ch == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					b.WriteByte(ch)
					b.WriteByte('\'')
					i++
					continue
				}
				inSingle = false
			}
			b.WriteByte(ch)
			continue
		}
		if inDouble {
			if ch == '"' {
				if i+1 < len(sql) && sql[i+1] == '"' {
					b.WriteByte(ch)
					b.WriteByte('"')
					i++
					continue
				}
				inDouble = false
			}
			b.WriteByte(ch)
			continue
		}
		if ch == '$' && i+1 < len(sql) && sql[i+1] == '$' {
			inDollar = true
			b.WriteByte(ch)
			b.WriteByte('$')
			i++
			continue
		}
		if ch == '\'' {
			inSingle = true
			b.WriteByte(ch)
			continue
		}
		if ch == '"' {
			inDouble = true
			b.WriteByte(ch)
			continue
		}
		if ch == ';' {
			stmt := strings.TrimSpace(b.String())
			if stmt != "" {
				stmts = append(stmts, stmt)
			}
			b.Reset()
			continue
		}
		b.WriteByte(ch)
	}
	last := strings.TrimSpace(b.String())
	if last != "" {
		stmts = append(stmts, last)
	}
	return stmts
}

func handleMySQLMissingIndexColumn(db DB, stmt string, err error) (bool, error) {
	if err == nil || db == nil {
		return false, nil
	}
	if !strings.Contains(err.Error(), "Key column 'llibre_ref_id' doesn't exist") {
		return false, nil
	}
	low := strings.ToLower(strings.TrimSpace(stmt))
	if !strings.HasPrefix(low, "create index") || !strings.Contains(low, "idx_llibres_urls_llibre_ref") {
		return false, nil
	}
	if _, addErr := db.Exec("ALTER TABLE llibres_urls ADD COLUMN llibre_ref_id INT UNSIGNED NULL"); addErr != nil {
		if !strings.Contains(addErr.Error(), "Duplicate column name") {
			return true, addErr
		}
	}
	if _, idxErr := db.Exec("CREATE INDEX idx_llibres_urls_llibre_ref ON llibres_urls(llibre_ref_id)"); idxErr != nil {
		if shouldIgnoreSQLError("mysql", "CREATE INDEX idx_llibres_urls_llibre_ref ON llibres_urls(llibre_ref_id)", idxErr) {
			return true, nil
		}
		return true, idxErr
	}
	return true, nil
}

func shouldIgnoreSQLError(engine, stmt string, err error) bool {
	if err == nil {
		return false
	}
	low := strings.ToLower(strings.TrimSpace(stmt))
	switch engine {
	case "mysql":
		if !strings.Contains(err.Error(), "Duplicate key name") {
			return false
		}
		if strings.HasPrefix(low, "create index") {
			return true
		}
		if strings.HasPrefix(low, "alter table") && strings.Contains(low, " add index") {
			return true
		}
	case "postgres":
		if strings.Contains(low, "create extension") && strings.Contains(err.Error(), "pg_extension_name_index") {
			return true
		}
		if strings.Contains(low, "create extension") && strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return true
		}
	}
	return false
}

func quoteIdent(engine, ident string) string {
	switch engine {
	case "mysql":
		return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
	default:
		return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
	}
}

func stringFromRowValue(val interface{}) string {
	if val == nil {
		return ""
	}
	switch v := val.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
}

func rowValueByKey(row map[string]interface{}, key string) interface{} {
	if row == nil {
		return nil
	}
	if val, ok := row[key]; ok {
		return val
	}
	keyLower := strings.ToLower(key)
	for k, v := range row {
		if strings.ToLower(k) == keyLower {
			return v
		}
	}
	return nil
}

func ensureMapTables(engine string, db DB) error {
	if db == nil {
		return nil
	}
	statements := []string{}
	switch engine {
	case "sqlite":
		statements = []string{
			`CREATE TABLE IF NOT EXISTS municipi_mapes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    municipi_id INTEGER NOT NULL REFERENCES municipis(id) ON DELETE CASCADE,
    group_type TEXT NOT NULL CHECK(group_type IN ('actual','historic','community')),
    title TEXT NOT NULL,
    period_label TEXT,
    period_start INTEGER,
    period_end INTEGER,
    topic TEXT,
    current_version_id INTEGER,
    created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);`,
			`CREATE TABLE IF NOT EXISTS municipi_mapa_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mapa_id INTEGER NOT NULL REFERENCES municipi_mapes(id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('draft','pendent','publicat','rebutjat')) DEFAULT 'draft',
    data_json TEXT NOT NULL,
    changelog TEXT NOT NULL DEFAULT '',
    lock_version INTEGER NOT NULL DEFAULT 0,
    created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderated_at TIMESTAMP,
    moderation_notes TEXT,
    UNIQUE (mapa_id, version)
);`,
			`CREATE INDEX IF NOT EXISTS idx_municipi_mapes_municipi_group ON municipi_mapes(municipi_id, group_type);`,
			`CREATE INDEX IF NOT EXISTS idx_municipi_mapes_updated ON municipi_mapes(municipi_id, updated_at DESC);`,
			`CREATE INDEX IF NOT EXISTS idx_municipi_mapa_versions_status ON municipi_mapa_versions(status, created_at ASC);`,
			`CREATE INDEX IF NOT EXISTS idx_municipi_mapa_versions_mapa_status ON municipi_mapa_versions(mapa_id, status);`,
		}
	case "postgres":
		statements = []string{
			`CREATE TABLE IF NOT EXISTS municipi_mapes (
    id SERIAL PRIMARY KEY,
    municipi_id INTEGER NOT NULL REFERENCES municipis(id) ON DELETE CASCADE,
    group_type TEXT NOT NULL CHECK(group_type IN ('actual','historic','community')),
    title TEXT NOT NULL,
    period_label TEXT,
    period_start INTEGER,
    period_end INTEGER,
    topic TEXT,
    current_version_id INTEGER,
    created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);`,
			`CREATE TABLE IF NOT EXISTS municipi_mapa_versions (
    id SERIAL PRIMARY KEY,
    mapa_id INTEGER NOT NULL REFERENCES municipi_mapes(id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('draft','pendent','publicat','rebutjat')) DEFAULT 'draft',
    data_json TEXT NOT NULL,
    changelog TEXT NOT NULL DEFAULT '',
    lock_version INTEGER NOT NULL DEFAULT 0,
    created_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    moderated_by INTEGER REFERENCES usuaris(id) ON DELETE SET NULL,
    moderated_at TIMESTAMP WITHOUT TIME ZONE,
    moderation_notes TEXT,
    UNIQUE (mapa_id, version)
);`,
			`CREATE INDEX IF NOT EXISTS idx_municipi_mapes_municipi_group ON municipi_mapes(municipi_id, group_type);`,
			`CREATE INDEX IF NOT EXISTS idx_municipi_mapes_updated ON municipi_mapes(municipi_id, updated_at DESC);`,
			`CREATE INDEX IF NOT EXISTS idx_municipi_mapa_versions_status ON municipi_mapa_versions(status, created_at ASC);`,
			`CREATE INDEX IF NOT EXISTS idx_municipi_mapa_versions_mapa_status ON municipi_mapa_versions(mapa_id, status);`,
		}
	case "mysql":
		statements = []string{
			`CREATE TABLE IF NOT EXISTS municipi_mapes (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    municipi_id INT UNSIGNED NOT NULL,
    group_type ENUM('actual','historic','community') NOT NULL,
    title VARCHAR(255) NOT NULL,
    period_label VARCHAR(64) NULL,
    period_start INT NULL,
    period_end INT NULL,
    topic VARCHAR(64) NULL,
    current_version_id INT UNSIGNED NULL,
    created_by INT UNSIGNED NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_municipi_mapes_municipi_group (municipi_id, group_type),
    INDEX idx_municipi_mapes_updated (municipi_id, updated_at),
    FOREIGN KEY (municipi_id) REFERENCES municipis(id) ON DELETE CASCADE,
    FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
			`CREATE TABLE IF NOT EXISTS municipi_mapa_versions (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    mapa_id INT UNSIGNED NOT NULL,
    version INT UNSIGNED NOT NULL,
    status ENUM('draft','pendent','publicat','rebutjat') DEFAULT 'draft',
    data_json LONGTEXT NOT NULL,
    changelog TEXT NOT NULL,
    lock_version INT UNSIGNED NOT NULL DEFAULT 0,
    created_by INT UNSIGNED NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    moderated_by INT UNSIGNED NULL,
    moderated_at DATETIME,
    moderation_notes TEXT,
    UNIQUE KEY idx_municipi_mapa_versions_unique (mapa_id, version),
    INDEX idx_municipi_mapa_versions_status (status, created_at),
    INDEX idx_municipi_mapa_versions_mapa_status (mapa_id, status),
    FOREIGN KEY (mapa_id) REFERENCES municipi_mapes(id) ON DELETE CASCADE,
    FOREIGN KEY (created_by) REFERENCES usuaris(id) ON DELETE SET NULL,
    FOREIGN KEY (moderated_by) REFERENCES usuaris(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		}
	}
	for _, stmt := range statements {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func baseSchemaReady(engine string, db DB) (bool, error) {
	return tableExists(engine, db, "usuaris")
}

func tableExists(engine string, db DB, table string) (bool, error) {
	table = strings.TrimSpace(table)
	if table == "" {
		return false, nil
	}
	var query string
	switch engine {
	case "postgres":
		query = "SELECT 1 FROM information_schema.tables WHERE table_name = $1"
	case "mysql":
		query = "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?"
	default:
		query = "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?"
	}
	rows, err := db.Query(query, table)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}
