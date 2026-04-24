package db

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type TemplateImportStrongMatchRequest struct {
	BookID         int
	TipusActe      string
	PageKey        string
	SnapshotMaxID  int
	PrincipalRoles []string
}

type TemplateImportStrongMatchResult struct {
	Transcripcions                   []TranscripcioRaw
	PersonesByTranscripcioID         map[int][]TranscripcioPersonaRaw
	AtributsByTranscripcioID         map[int][]TranscripcioAtributRaw
	PreparedPersonesByTranscripcioID map[int]map[string]*TranscripcioPersonaRaw
	PreparedAtributsByTranscripcioID map[int]map[string]*TranscripcioAtributRaw
	PreparedMatchIDsByKey            map[string]int
	ExactContextMatch                bool
}

type TemplateImportPrincipalMatchRequest struct {
	BookID        int
	SnapshotMaxID int
}

type TemplateImportPrincipalMatchResult struct {
	Transcripcions           []TranscripcioRaw
	PersonesByTranscripcioID map[int][]TranscripcioPersonaRaw
}

type TemplateImportPageResolution struct {
	CanonicalKey string
	Indexed      bool
	Resolved     bool
}

type TemplateImportPageResolver interface {
	ResolveIncoming(bookID int, row *TranscripcioRaw, attrs map[string]*TranscripcioAtributRaw) (TemplateImportPageResolution, error)
	ResolveExisting(bookID int, row *TranscripcioRaw, attrs []TranscripcioAtributRaw) (string, error)
}

type TemplateImportRuntime interface {
	Engine() string
	NewPageResolver() TemplateImportPageResolver
	ExistingSnapshotMaxID() (int, error)
	LoadStrongMatchCandidates(req TemplateImportStrongMatchRequest) (TemplateImportStrongMatchResult, error)
	LoadPrincipalMatchCandidates(req TemplateImportPrincipalMatchRequest) (TemplateImportPrincipalMatchResult, error)
	BulkCreateBundles(rows []TranscripcioRawImportBundle) (TranscripcioRawImportBulkResult, error)
	CreateBundle(row TranscripcioRawImportBundle) (TranscripcioRawImportBulkResult, error)
	LoadPersonesByLlibreID(llibreID int, transcripcioIDs []int) (map[int][]TranscripcioPersonaRaw, error)
	LoadAtributsByLlibreID(llibreID int, transcripcioIDs []int) (map[int][]TranscripcioAtributRaw, error)
}

type templateImportRuntimeBase struct {
	database DB
}

type sqliteTemplateImportRuntime struct {
	templateImportRuntimeBase
}

type postgresTemplateImportRuntime struct {
	templateImportRuntimeBase
	strongSnapshots map[string]*postgresTemplateImportStrongSnapshot
}

type mysqlTemplateImportRuntime struct {
	templateImportRuntimeBase
}

type genericTemplateImportRuntime struct {
	templateImportRuntimeBase
}

type templateBookPageLookup struct {
	byNum            map[int]*LlibrePagina
	byID             map[int]*LlibrePagina
	resolvedByKey    map[string]TemplateImportPageResolution
	resolvedByPageID map[int]TemplateImportPageResolution
}

type templateCachedPageResolver struct {
	database DB
	books    map[int]*templateBookPageLookup
	byID     map[int]*LlibrePagina
	missing  map[int]struct{}
}

type sqliteTemplateImportPageResolver struct {
	*templateCachedPageResolver
}

type postgresTemplateImportPageResolver struct {
	*templateCachedPageResolver
}

type mysqlTemplateImportPageResolver struct {
	*templateCachedPageResolver
}

type genericTemplateImportPageResolver struct {
	*templateCachedPageResolver
}

func TemplateImportRuntimeFor(database DB) TemplateImportRuntime {
	base := templateImportRuntimeBase{database: database}
	switch engine := base.Engine(); engine {
	case "postgres":
		return &postgresTemplateImportRuntime{templateImportRuntimeBase: base}
	case "sqlite":
		return sqliteTemplateImportRuntime{templateImportRuntimeBase: base}
	case "mysql":
		return mysqlTemplateImportRuntime{templateImportRuntimeBase: base}
	default:
		return genericTemplateImportRuntime{templateImportRuntimeBase: base}
	}
}

func (r templateImportRuntimeBase) Engine() string {
	if r.database == nil {
		return "generic"
	}
	engine := strings.ToLower(strings.TrimSpace(r.database.Engine()))
	if engine == "" {
		return "generic"
	}
	return engine
}

func (r templateImportRuntimeBase) ExistingSnapshotMaxID() (int, error) {
	if r.database == nil {
		return 0, nil
	}
	return r.database.GetMaxTranscripcioRawID()
}

func (r templateImportRuntimeBase) BulkCreateBundles(rows []TranscripcioRawImportBundle) (TranscripcioRawImportBulkResult, error) {
	if r.database == nil {
		return TranscripcioRawImportBulkResult{}, fmt.Errorf("template import bulk create unsupported for engine=%s", r.Engine())
	}
	return r.database.BulkCreateTranscripcioRawBundles(rows)
}

func (r templateImportRuntimeBase) CreateBundle(row TranscripcioRawImportBundle) (TranscripcioRawImportBulkResult, error) {
	res := TranscripcioRawImportBulkResult{
		Metrics: TranscripcioRawImportBulkMetrics{
			Rows:     1,
			Persones: len(row.Persones),
			Atributs: len(row.Atributs),
		},
	}
	if r.database == nil {
		return res, fmt.Errorf("template import create unsupported for engine=%s", r.Engine())
	}
	raw := row.Transcripcio
	start := time.Now()
	id, err := r.database.CreateTranscripcioRaw(&raw)
	res.Metrics.TranscripcioInsertDur += time.Since(start)
	if err != nil || id == 0 {
		return res, err
	}
	res.IDs = []int{id}
	for i := range row.Persones {
		persona := row.Persones[i]
		persona.TranscripcioID = id
		start = time.Now()
		_, _ = r.database.CreateTranscripcioPersona(&persona)
		res.Metrics.PersonaPersistDur += time.Since(start)
	}
	for i := range row.Atributs {
		attr := row.Atributs[i]
		attr.TranscripcioID = id
		start = time.Now()
		_, _ = r.database.CreateTranscripcioAtribut(&attr)
		res.Metrics.LinksPersistDur += time.Since(start)
	}
	return res, nil
}

func (r sqliteTemplateImportRuntime) NewPageResolver() TemplateImportPageResolver {
	return sqliteTemplateImportPageResolver{templateCachedPageResolver: newTemplateCachedPageResolver(r.database)}
}

func (r postgresTemplateImportRuntime) NewPageResolver() TemplateImportPageResolver {
	return postgresTemplateImportPageResolver{templateCachedPageResolver: newTemplateCachedPageResolver(r.database)}
}

func (r mysqlTemplateImportRuntime) NewPageResolver() TemplateImportPageResolver {
	return mysqlTemplateImportPageResolver{templateCachedPageResolver: newTemplateCachedPageResolver(r.database)}
}

func (r genericTemplateImportRuntime) NewPageResolver() TemplateImportPageResolver {
	return genericTemplateImportPageResolver{templateCachedPageResolver: newTemplateCachedPageResolver(r.database)}
}

func (r sqliteTemplateImportRuntime) LoadStrongMatchCandidates(req TemplateImportStrongMatchRequest) (TemplateImportStrongMatchResult, error) {
	return genericStrongMatchCandidates(r.database, req)
}

func (r mysqlTemplateImportRuntime) LoadStrongMatchCandidates(req TemplateImportStrongMatchRequest) (TemplateImportStrongMatchResult, error) {
	return scopedStrongMatchCandidates(r.database, req)
}

func (r genericTemplateImportRuntime) LoadStrongMatchCandidates(req TemplateImportStrongMatchRequest) (TemplateImportStrongMatchResult, error) {
	return genericStrongMatchCandidates(r.database, req)
}

func (r *postgresTemplateImportRuntime) LoadStrongMatchCandidates(req TemplateImportStrongMatchRequest) (TemplateImportStrongMatchResult, error) {
	result := emptyStrongMatchResult()
	if r.database == nil || req.BookID <= 0 || req.SnapshotMaxID == 0 {
		return result, nil
	}
	pageKey := strings.TrimSpace(req.PageKey)
	tipusActe := strings.TrimSpace(req.TipusActe)
	if pageKey == "" || tipusActe == "" {
		return result, nil
	}
	snapshot, err := r.strongSnapshot(req.BookID, req.SnapshotMaxID)
	if err != nil {
		return result, err
	}
	result.PreparedMatchIDsByKey = snapshot.strongMatchIDsForRequest(pageKey, tipusActe, req.PrincipalRoles)
	result.ExactContextMatch = true
	return result, nil
}

func (r sqliteTemplateImportRuntime) LoadPrincipalMatchCandidates(req TemplateImportPrincipalMatchRequest) (TemplateImportPrincipalMatchResult, error) {
	return genericPrincipalMatchCandidates(r.database, req)
}

func (r mysqlTemplateImportRuntime) LoadPrincipalMatchCandidates(req TemplateImportPrincipalMatchRequest) (TemplateImportPrincipalMatchResult, error) {
	return bookScopedPrincipalMatchCandidates(r.database, req)
}

func (r genericTemplateImportRuntime) LoadPrincipalMatchCandidates(req TemplateImportPrincipalMatchRequest) (TemplateImportPrincipalMatchResult, error) {
	return genericPrincipalMatchCandidates(r.database, req)
}

func (r *postgresTemplateImportRuntime) LoadPrincipalMatchCandidates(req TemplateImportPrincipalMatchRequest) (TemplateImportPrincipalMatchResult, error) {
	result := TemplateImportPrincipalMatchResult{
		PersonesByTranscripcioID: map[int][]TranscripcioPersonaRaw{},
	}
	if r.database == nil || req.BookID <= 0 || req.SnapshotMaxID == 0 {
		return result, nil
	}
	snapshot, err := r.strongSnapshot(req.BookID, req.SnapshotMaxID)
	if err != nil {
		return result, err
	}
	result.Transcripcions = append(result.Transcripcions, snapshot.transcripcions...)
	result.PersonesByTranscripcioID = snapshot.personesForIDs(snapshot.ids)
	return result, nil
}

func (r *postgresTemplateImportRuntime) CreateBundle(row TranscripcioRawImportBundle) (TranscripcioRawImportBulkResult, error) {
	return createBundleThroughBulk(r, row)
}

func (r mysqlTemplateImportRuntime) CreateBundle(row TranscripcioRawImportBundle) (TranscripcioRawImportBulkResult, error) {
	return createBundleThroughBulk(r, row)
}

func (r sqliteTemplateImportRuntime) LoadPersonesByLlibreID(llibreID int, transcripcioIDs []int) (map[int][]TranscripcioPersonaRaw, error) {
	return genericRelatedPersonesByIDs(r.database, transcripcioIDs)
}

func (r mysqlTemplateImportRuntime) LoadPersonesByLlibreID(llibreID int, transcripcioIDs []int) (map[int][]TranscripcioPersonaRaw, error) {
	return relatedPersonesByLlibreID(r.database, llibreID, transcripcioIDs)
}

func (r genericTemplateImportRuntime) LoadPersonesByLlibreID(llibreID int, transcripcioIDs []int) (map[int][]TranscripcioPersonaRaw, error) {
	return genericRelatedPersonesByIDs(r.database, transcripcioIDs)
}

func (r postgresTemplateImportRuntime) LoadPersonesByLlibreID(llibreID int, transcripcioIDs []int) (map[int][]TranscripcioPersonaRaw, error) {
	return relatedPersonesByLlibreID(r.database, llibreID, transcripcioIDs)
}

func (r sqliteTemplateImportRuntime) LoadAtributsByLlibreID(llibreID int, transcripcioIDs []int) (map[int][]TranscripcioAtributRaw, error) {
	return genericRelatedAtributsByIDs(r.database, transcripcioIDs)
}

func (r mysqlTemplateImportRuntime) LoadAtributsByLlibreID(llibreID int, transcripcioIDs []int) (map[int][]TranscripcioAtributRaw, error) {
	return relatedAtributsByLlibreID(r.database, llibreID, transcripcioIDs)
}

func (r genericTemplateImportRuntime) LoadAtributsByLlibreID(llibreID int, transcripcioIDs []int) (map[int][]TranscripcioAtributRaw, error) {
	return genericRelatedAtributsByIDs(r.database, transcripcioIDs)
}

func (r postgresTemplateImportRuntime) LoadAtributsByLlibreID(llibreID int, transcripcioIDs []int) (map[int][]TranscripcioAtributRaw, error) {
	return relatedAtributsByLlibreID(r.database, llibreID, transcripcioIDs)
}

func createBundleThroughBulk(runtime interface {
	Engine() string
	BulkCreateBundles(rows []TranscripcioRawImportBundle) (TranscripcioRawImportBulkResult, error)
}, row TranscripcioRawImportBundle) (TranscripcioRawImportBulkResult, error) {
	res, err := runtime.BulkCreateBundles([]TranscripcioRawImportBundle{row})
	if err != nil {
		return res, err
	}
	if len(res.IDs) != 1 || res.IDs[0] == 0 {
		return res, fmt.Errorf("template import create unsupported for engine=%s", runtime.Engine())
	}
	return res, nil
}

func relatedPersonesByLlibreID(database DB, llibreID int, transcripcioIDs []int) (map[int][]TranscripcioPersonaRaw, error) {
	if database == nil {
		return map[int][]TranscripcioPersonaRaw{}, nil
	}
	rows, err := database.ListTranscripcioPersonesByLlibreID(llibreID)
	if err != nil {
		return nil, err
	}
	return filterPersonaMapsByIDs(rows, transcripcioIDs), nil
}

func relatedAtributsByLlibreID(database DB, llibreID int, transcripcioIDs []int) (map[int][]TranscripcioAtributRaw, error) {
	if database == nil {
		return map[int][]TranscripcioAtributRaw{}, nil
	}
	rows, err := database.ListTranscripcioAtributsByLlibreID(llibreID)
	if err != nil {
		return nil, err
	}
	return filterAtributMapsByIDs(rows, transcripcioIDs), nil
}

func newTemplateCachedPageResolver(database DB) *templateCachedPageResolver {
	if database == nil {
		return nil
	}
	return &templateCachedPageResolver{
		database: database,
		books:    map[int]*templateBookPageLookup{},
		byID:     map[int]*LlibrePagina{},
		missing:  map[int]struct{}{},
	}
}

func (r sqliteTemplateImportPageResolver) ResolveIncoming(bookID int, row *TranscripcioRaw, attrs map[string]*TranscripcioAtributRaw) (TemplateImportPageResolution, error) {
	return r.templateCachedPageResolver.ResolveIncoming(bookID, row, attrs)
}

func (r postgresTemplateImportPageResolver) ResolveIncoming(bookID int, row *TranscripcioRaw, attrs map[string]*TranscripcioAtributRaw) (TemplateImportPageResolution, error) {
	return r.templateCachedPageResolver.ResolveIncoming(bookID, row, attrs)
}

func (r mysqlTemplateImportPageResolver) ResolveIncoming(bookID int, row *TranscripcioRaw, attrs map[string]*TranscripcioAtributRaw) (TemplateImportPageResolution, error) {
	return r.templateCachedPageResolver.ResolveIncoming(bookID, row, attrs)
}

func (r genericTemplateImportPageResolver) ResolveIncoming(bookID int, row *TranscripcioRaw, attrs map[string]*TranscripcioAtributRaw) (TemplateImportPageResolution, error) {
	return r.templateCachedPageResolver.ResolveIncoming(bookID, row, attrs)
}

func (r sqliteTemplateImportPageResolver) ResolveExisting(bookID int, row *TranscripcioRaw, attrs []TranscripcioAtributRaw) (string, error) {
	return r.templateCachedPageResolver.ResolveExisting(bookID, row, attrs)
}

func (r postgresTemplateImportPageResolver) ResolveExisting(bookID int, row *TranscripcioRaw, attrs []TranscripcioAtributRaw) (string, error) {
	return r.templateCachedPageResolver.ResolveExisting(bookID, row, attrs)
}

func (r mysqlTemplateImportPageResolver) ResolveExisting(bookID int, row *TranscripcioRaw, attrs []TranscripcioAtributRaw) (string, error) {
	return r.templateCachedPageResolver.ResolveExisting(bookID, row, attrs)
}

func (r genericTemplateImportPageResolver) ResolveExisting(bookID int, row *TranscripcioRaw, attrs []TranscripcioAtributRaw) (string, error) {
	return r.templateCachedPageResolver.ResolveExisting(bookID, row, attrs)
}

func (r *templateCachedPageResolver) ResolveIncoming(bookID int, row *TranscripcioRaw, attrs map[string]*TranscripcioAtributRaw) (TemplateImportPageResolution, error) {
	pageKey := templateImportLogicalPageKey(row, attrs)
	pageKeyNorm := normalizeTemplatePageResolutionKey(pageKey)
	if row != nil && row.PaginaID.Valid {
		if resolution, ok := r.lookupPageResolutionByID(bookID, int(row.PaginaID.Int64)); ok {
			if pageKey == "" && resolution.CanonicalKey != "" {
				resolution.CanonicalKey = resolution.CanonicalKey
			}
			return resolution, nil
		}
		page, err := r.lookupPageByID(bookID, int(row.PaginaID.Int64))
		if err == nil && page != nil {
			if pageKey == "" && page.NumPagina > 0 {
				pageKey = strconv.Itoa(page.NumPagina)
			}
			resolution := TemplateImportPageResolution{
				CanonicalKey: normalizeTemplatePageResolutionKey(pageKey),
				Indexed:      page.LlibreID == bookID && page.Estat == "indexada",
				Resolved:     true,
			}
			r.rememberPageResolutionByID(bookID, int(row.PaginaID.Int64), resolution)
			if pageKeyNorm != "" {
				r.rememberPageResolutionByKey(bookID, pageKeyNorm, resolution)
			}
			return resolution, nil
		}
	}
	if pageKey == "" {
		return TemplateImportPageResolution{}, nil
	}
	if resolution, ok := r.lookupPageResolutionByKey(bookID, pageKeyNorm); ok {
		return resolution, nil
	}
	pageNum, ok := parseTemplateImportPositiveInt(pageKey)
	if !ok {
		resolution := TemplateImportPageResolution{
			CanonicalKey: pageKeyNorm,
			Indexed:      false,
			Resolved:     false,
		}
		r.rememberPageResolutionByKey(bookID, pageKeyNorm, resolution)
		return resolution, nil
	}
	page, err := r.lookupPageByNum(bookID, pageNum)
	if err != nil || page == nil {
		resolution := TemplateImportPageResolution{
			CanonicalKey: pageKeyNorm,
			Indexed:      false,
			Resolved:     false,
		}
		r.rememberPageResolutionByKey(bookID, pageKeyNorm, resolution)
		return resolution, err
	}
	resolution := TemplateImportPageResolution{
		CanonicalKey: strconv.Itoa(page.NumPagina),
		Indexed:      page.Estat == "indexada",
		Resolved:     true,
	}
	r.rememberPageResolutionByKey(bookID, pageKeyNorm, resolution)
	if page.ID > 0 {
		r.rememberPageResolutionByID(bookID, page.ID, resolution)
	}
	return resolution, nil
}

func (r *templateCachedPageResolver) ResolveExisting(bookID int, row *TranscripcioRaw, attrs []TranscripcioAtributRaw) (string, error) {
	attrsByKey := map[string]*TranscripcioAtributRaw{}
	for i := range attrs {
		attrsByKey[attrs[i].Clau] = &attrs[i]
	}
	pageKey := templateImportLogicalPageKey(row, attrsByKey)
	if pageKey != "" || row == nil || !row.PaginaID.Valid {
		return pageKey, nil
	}
	lookupBookID := bookID
	if lookupBookID <= 0 {
		lookupBookID = row.LlibreID
	}
	page, err := r.lookupPageByID(lookupBookID, int(row.PaginaID.Int64))
	if err != nil || page == nil || page.NumPagina <= 0 {
		return "", err
	}
	return strconv.Itoa(page.NumPagina), nil
}

func (r *templateCachedPageResolver) loadBook(bookID int) (*templateBookPageLookup, error) {
	if r == nil || r.database == nil || bookID <= 0 {
		return nil, nil
	}
	if pages, ok := r.books[bookID]; ok {
		return pages, nil
	}
	rows, err := r.database.ListLlibrePagines(bookID)
	if err != nil {
		return nil, err
	}
	pages := &templateBookPageLookup{
		byNum:            map[int]*LlibrePagina{},
		byID:             map[int]*LlibrePagina{},
		resolvedByKey:    map[string]TemplateImportPageResolution{},
		resolvedByPageID: map[int]TemplateImportPageResolution{},
	}
	for i := range rows {
		page := rows[i]
		pageCopy := page
		if pageCopy.ID > 0 {
			pages.byID[pageCopy.ID] = &pageCopy
			r.byID[pageCopy.ID] = &pageCopy
			delete(r.missing, pageCopy.ID)
			pages.resolvedByPageID[pageCopy.ID] = TemplateImportPageResolution{
				CanonicalKey: strconv.Itoa(pageCopy.NumPagina),
				Indexed:      pageCopy.Estat == "indexada",
				Resolved:     true,
			}
		}
		if pageCopy.NumPagina > 0 {
			pages.byNum[pageCopy.NumPagina] = &pageCopy
			pages.resolvedByKey[strconv.Itoa(pageCopy.NumPagina)] = TemplateImportPageResolution{
				CanonicalKey: strconv.Itoa(pageCopy.NumPagina),
				Indexed:      pageCopy.Estat == "indexada",
				Resolved:     true,
			}
		}
	}
	r.books[bookID] = pages
	return pages, nil
}

func (r *templateCachedPageResolver) getBook(bookID int) (*templateBookPageLookup, error) {
	if r == nil || bookID <= 0 {
		return nil, nil
	}
	return r.loadBook(bookID)
}

func (r *templateCachedPageResolver) lookupPageResolutionByKey(bookID int, pageKey string) (TemplateImportPageResolution, bool) {
	pages, err := r.getBook(bookID)
	if err != nil || pages == nil {
		return TemplateImportPageResolution{}, false
	}
	resolution, ok := pages.resolvedByKey[normalizeTemplatePageResolutionKey(pageKey)]
	return resolution, ok
}

func (r *templateCachedPageResolver) rememberPageResolutionByKey(bookID int, pageKey string, resolution TemplateImportPageResolution) {
	pages, err := r.getBook(bookID)
	if err != nil || pages == nil {
		return
	}
	normKey := normalizeTemplatePageResolutionKey(pageKey)
	if normKey != "" {
		pages.resolvedByKey[normKey] = resolution
	}
	if resolution.Resolved && resolution.CanonicalKey != "" {
		pages.resolvedByKey[normalizeTemplatePageResolutionKey(resolution.CanonicalKey)] = resolution
	}
}

func (r *templateCachedPageResolver) lookupPageResolutionByID(bookID, pageID int) (TemplateImportPageResolution, bool) {
	if pageID <= 0 {
		return TemplateImportPageResolution{}, false
	}
	pages, err := r.getBook(bookID)
	if err != nil || pages == nil {
		return TemplateImportPageResolution{}, false
	}
	resolution, ok := pages.resolvedByPageID[pageID]
	return resolution, ok
}

func (r *templateCachedPageResolver) rememberPageResolutionByID(bookID, pageID int, resolution TemplateImportPageResolution) {
	if pageID <= 0 {
		return
	}
	pages, err := r.getBook(bookID)
	if err != nil || pages == nil {
		return
	}
	pages.resolvedByPageID[pageID] = resolution
	if resolution.Resolved && resolution.CanonicalKey != "" {
		pages.resolvedByKey[normalizeTemplatePageResolutionKey(resolution.CanonicalKey)] = resolution
	}
}

func (r *templateCachedPageResolver) lookupPageByNum(bookID, pageNum int) (*LlibrePagina, error) {
	if r != nil {
		if pages, err := r.loadBook(bookID); err != nil {
			return nil, err
		} else if pages != nil {
			return pages.byNum[pageNum], nil
		}
	}
	if r == nil || r.database == nil {
		return nil, nil
	}
	return r.database.GetLlibrePaginaByNum(bookID, pageNum)
}

func (r *templateCachedPageResolver) lookupPageByID(bookID, pageID int) (*LlibrePagina, error) {
	if pageID <= 0 {
		return nil, nil
	}
	if r != nil {
		if page, ok := r.byID[pageID]; ok {
			return page, nil
		}
		if _, missing := r.missing[pageID]; missing {
			return nil, nil
		}
		if pages, err := r.loadBook(bookID); err != nil {
			return nil, err
		} else if pages != nil {
			if page, ok := pages.byID[pageID]; ok {
				return page, nil
			}
		}
	}
	if r == nil || r.database == nil {
		return nil, nil
	}
	page, err := r.database.GetLlibrePaginaByID(pageID)
	if err != nil || page == nil {
		if r != nil && err == nil {
			r.missing[pageID] = struct{}{}
		}
		return page, err
	}
	if r != nil {
		r.byID[pageID] = page
		delete(r.missing, pageID)
	}
	return page, nil
}

func genericStrongMatchCandidates(database DB, req TemplateImportStrongMatchRequest) (TemplateImportStrongMatchResult, error) {
	result := emptyStrongMatchResult()
	if database == nil || req.BookID <= 0 {
		return result, nil
	}
	tipusActe := strings.TrimSpace(req.TipusActe)
	if tipusActe == "" || req.SnapshotMaxID == 0 {
		return result, nil
	}
	trans, err := database.ListTranscripcionsRaw(req.BookID, TranscripcioFilter{TipusActe: tipusActe, Limit: -1})
	if err != nil {
		return result, err
	}
	filtered := filterTranscripcionsBySnapshot(trans, req.SnapshotMaxID)
	result.Transcripcions = filtered
	ids := transcripcioIDs(filtered)
	if len(ids) == 0 {
		return result, nil
	}
	personesByID, err := database.ListTranscripcioPersonesByTranscripcioIDs(ids)
	if err != nil {
		return result, err
	}
	atributsByID, err := database.ListTranscripcioAtributsByTranscripcioIDs(ids)
	if err != nil {
		return result, err
	}
	result.PersonesByTranscripcioID = ensurePersonaMaps(personesByID)
	result.AtributsByTranscripcioID = ensureAtributMaps(atributsByID)
	return result, nil
}

func scopedStrongMatchCandidates(database DB, req TemplateImportStrongMatchRequest) (TemplateImportStrongMatchResult, error) {
	result := emptyStrongMatchResult()
	if database == nil || req.BookID <= 0 || req.SnapshotMaxID == 0 {
		return result, nil
	}
	pageKey := strings.TrimSpace(req.PageKey)
	tipusActe := strings.TrimSpace(req.TipusActe)
	if pageKey == "" || tipusActe == "" {
		return result, nil
	}
	trans, personesByID, atributsByID, err := database.ListTranscripcioStrongMatchCandidatesUpToID(req.BookID, tipusActe, pageKey, req.SnapshotMaxID)
	if err != nil {
		return result, err
	}
	result.Transcripcions = filterTranscripcionsBySnapshot(trans, req.SnapshotMaxID)
	result.PersonesByTranscripcioID = ensurePersonaMaps(personesByID)
	result.AtributsByTranscripcioID = ensureAtributMaps(atributsByID)
	return result, nil
}

func genericPrincipalMatchCandidates(database DB, req TemplateImportPrincipalMatchRequest) (TemplateImportPrincipalMatchResult, error) {
	result := TemplateImportPrincipalMatchResult{
		PersonesByTranscripcioID: map[int][]TranscripcioPersonaRaw{},
	}
	if database == nil || req.BookID <= 0 || req.SnapshotMaxID == 0 {
		return result, nil
	}
	trans, err := database.ListTranscripcionsRaw(req.BookID, TranscripcioFilter{Limit: -1})
	if err != nil {
		return result, err
	}
	filtered := filterTranscripcionsBySnapshot(trans, req.SnapshotMaxID)
	result.Transcripcions = filtered
	ids := transcripcioIDs(filtered)
	if len(ids) == 0 {
		return result, nil
	}
	personesByID, err := database.ListTranscripcioPersonesByTranscripcioIDs(ids)
	if err != nil {
		return result, err
	}
	result.PersonesByTranscripcioID = ensurePersonaMaps(personesByID)
	return result, nil
}

func bookScopedPrincipalMatchCandidates(database DB, req TemplateImportPrincipalMatchRequest) (TemplateImportPrincipalMatchResult, error) {
	result := TemplateImportPrincipalMatchResult{
		PersonesByTranscripcioID: map[int][]TranscripcioPersonaRaw{},
	}
	if database == nil || req.BookID <= 0 || req.SnapshotMaxID == 0 {
		return result, nil
	}
	trans, err := database.ListTranscripcionsRaw(req.BookID, TranscripcioFilter{Limit: -1})
	if err != nil {
		return result, err
	}
	result.Transcripcions = filterTranscripcionsBySnapshot(trans, req.SnapshotMaxID)
	ids := transcripcioIDs(result.Transcripcions)
	if len(ids) == 0 {
		return result, nil
	}
	personesByID, err := database.ListTranscripcioPersonesByLlibreID(req.BookID)
	if err != nil {
		return result, err
	}
	result.PersonesByTranscripcioID = filterPersonaMapsByIDs(personesByID, ids)
	return result, nil
}

func genericRelatedPersonesByIDs(database DB, transcripcioIDs []int) (map[int][]TranscripcioPersonaRaw, error) {
	if database == nil {
		return map[int][]TranscripcioPersonaRaw{}, nil
	}
	rows, err := database.ListTranscripcioPersonesByTranscripcioIDs(transcripcioIDs)
	if err != nil {
		return nil, err
	}
	return ensurePersonaMaps(rows), nil
}

func genericRelatedAtributsByIDs(database DB, transcripcioIDs []int) (map[int][]TranscripcioAtributRaw, error) {
	if database == nil {
		return map[int][]TranscripcioAtributRaw{}, nil
	}
	rows, err := database.ListTranscripcioAtributsByTranscripcioIDs(transcripcioIDs)
	if err != nil {
		return nil, err
	}
	return ensureAtributMaps(rows), nil
}

func genericDBStrongMatchCandidates(database DB, bookID int, tipusActe, pageKey string, maxExistingID int) ([]TranscripcioRaw, map[int][]TranscripcioPersonaRaw, map[int][]TranscripcioAtributRaw, error) {
	result, err := genericStrongMatchCandidates(database, TemplateImportStrongMatchRequest{
		BookID:        bookID,
		TipusActe:     tipusActe,
		PageKey:       pageKey,
		SnapshotMaxID: maxExistingID,
	})
	if err != nil {
		return nil, map[int][]TranscripcioPersonaRaw{}, map[int][]TranscripcioAtributRaw{}, err
	}
	return result.Transcripcions, result.PersonesByTranscripcioID, result.AtributsByTranscripcioID, nil
}

func templateImportLogicalPageKey(row *TranscripcioRaw, attrs map[string]*TranscripcioAtributRaw) string {
	if attr := attrs["pagina_digital"]; attr != nil {
		if value := templateImportAttrComparableValue(attr); value != "" {
			return value
		}
	}
	if row != nil && strings.TrimSpace(row.NumPaginaText) != "" {
		return strings.TrimSpace(row.NumPaginaText)
	}
	return ""
}

func templateImportAttrComparableValue(attr *TranscripcioAtributRaw) string {
	if attr == nil {
		return ""
	}
	return strings.TrimSpace(attr.ValorText)
}

func normalizeTemplatePageResolutionKey(pageKey string) string {
	return strings.TrimSpace(pageKey)
}

func emptyStrongMatchResult() TemplateImportStrongMatchResult {
	return TemplateImportStrongMatchResult{
		PersonesByTranscripcioID:         map[int][]TranscripcioPersonaRaw{},
		AtributsByTranscripcioID:         map[int][]TranscripcioAtributRaw{},
		PreparedPersonesByTranscripcioID: map[int]map[string]*TranscripcioPersonaRaw{},
		PreparedAtributsByTranscripcioID: map[int]map[string]*TranscripcioAtributRaw{},
		PreparedMatchIDsByKey:            map[string]int{},
	}
}

type postgresTemplateImportStrongSnapshot struct {
	bookID                  int
	snapshotMaxID           int
	ids                     []int
	transcripcions          []TranscripcioRaw
	transcripcioByID        map[int]TranscripcioRaw
	personesByID            map[int][]TranscripcioPersonaRaw
	atributsByID            map[int][]TranscripcioAtributRaw
	preparedPersonesByID    map[int]map[string]*TranscripcioPersonaRaw
	preparedAtributsByID    map[int]map[string]*TranscripcioAtributRaw
	strongMatchKeysByPolicy map[string]map[int]string
	strongMatchIDsByRequest map[string]map[string]int
	strongIDsByContext      map[string][]int
	pageNumsByPaginaID      map[int]int
}

func (r *postgresTemplateImportRuntime) strongSnapshot(bookID, snapshotMaxID int) (*postgresTemplateImportStrongSnapshot, error) {
	if r == nil || r.database == nil || bookID <= 0 || snapshotMaxID == 0 {
		return nil, nil
	}
	if r.strongSnapshots == nil {
		r.strongSnapshots = map[string]*postgresTemplateImportStrongSnapshot{}
	}
	cacheKey := postgresTemplateImportSnapshotKey(bookID, snapshotMaxID)
	if snapshot, ok := r.strongSnapshots[cacheKey]; ok && snapshot != nil {
		return snapshot, nil
	}
	snapshot, err := r.loadStrongSnapshot(bookID, snapshotMaxID)
	if err != nil {
		return nil, err
	}
	r.strongSnapshots[cacheKey] = snapshot
	return snapshot, nil
}

func (r *postgresTemplateImportRuntime) loadStrongSnapshot(bookID, snapshotMaxID int) (*postgresTemplateImportStrongSnapshot, error) {
	trans, err := r.database.ListTranscripcionsRaw(bookID, TranscripcioFilter{Limit: -1})
	if err != nil {
		return nil, err
	}
	filtered := filterTranscripcionsBySnapshot(trans, snapshotMaxID)
	ids := transcripcioIDs(filtered)
	personesByID, err := r.database.ListTranscripcioPersonesByLlibreID(bookID)
	if err != nil {
		return nil, err
	}
	atributsByID, err := r.database.ListTranscripcioAtributsByLlibreID(bookID)
	if err != nil {
		return nil, err
	}
	pages, err := r.database.ListLlibrePagines(bookID)
	if err != nil {
		return nil, err
	}
	pageNumsByPaginaID := make(map[int]int, len(pages))
	for _, page := range pages {
		if page.ID > 0 && page.NumPagina > 0 {
			pageNumsByPaginaID[page.ID] = page.NumPagina
		}
	}
	filteredPersones := filterPersonaMapsByIDs(personesByID, ids)
	filteredAtributs := filterAtributMapsByIDs(atributsByID, ids)
	preparedPersonesByID := make(map[int]map[string]*TranscripcioPersonaRaw, len(filteredPersones))
	for id, rows := range filteredPersones {
		byRole := make(map[string]*TranscripcioPersonaRaw, len(rows))
		for i := range rows {
			byRole[rows[i].Rol] = &rows[i]
		}
		preparedPersonesByID[id] = byRole
	}
	preparedAtributsByID := make(map[int]map[string]*TranscripcioAtributRaw, len(filteredAtributs))
	for id, rows := range filteredAtributs {
		byKey := make(map[string]*TranscripcioAtributRaw, len(rows))
		for i := range rows {
			byKey[rows[i].Clau] = &rows[i]
		}
		preparedAtributsByID[id] = byKey
	}
	transcripcioByID := make(map[int]TranscripcioRaw, len(filtered))
	strongIDsByContext := map[string][]int{}
	for _, row := range filtered {
		if row.ID <= 0 {
			continue
		}
		transcripcioByID[row.ID] = row
		contextKeys := postgresStrongContextKeysForExisting(&row, filteredAtributs[row.ID], pageNumsByPaginaID)
		tipusActe := normalizePostgresStrongContextPart(row.TipusActe)
		if tipusActe == "" {
			continue
		}
		for _, pageKey := range contextKeys {
			contextKey := postgresStrongContextKey(tipusActe, pageKey)
			strongIDsByContext[contextKey] = append(strongIDsByContext[contextKey], row.ID)
		}
	}
	return &postgresTemplateImportStrongSnapshot{
		bookID:                  bookID,
		snapshotMaxID:           snapshotMaxID,
		ids:                     ids,
		transcripcions:          filtered,
		transcripcioByID:        transcripcioByID,
		personesByID:            filteredPersones,
		atributsByID:            filteredAtributs,
		preparedPersonesByID:    preparedPersonesByID,
		preparedAtributsByID:    preparedAtributsByID,
		strongMatchKeysByPolicy: map[string]map[int]string{},
		strongMatchIDsByRequest: map[string]map[string]int{},
		strongIDsByContext:      strongIDsByContext,
		pageNumsByPaginaID:      pageNumsByPaginaID,
	}, nil
}

func (s *postgresTemplateImportStrongSnapshot) lookupStrongContextIDs(pageKey, tipusActe string) []int {
	if s == nil {
		return nil
	}
	tipusActe = normalizePostgresStrongContextPart(tipusActe)
	if tipusActe == "" {
		return nil
	}
	pageKeys := postgresStrongContextKeysForIncoming(pageKey)
	if len(pageKeys) == 0 {
		return nil
	}
	merged := make([]int, 0)
	seen := map[int]struct{}{}
	for _, key := range pageKeys {
		for _, id := range s.strongIDsByContext[postgresStrongContextKey(tipusActe, key)] {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			merged = append(merged, id)
		}
	}
	return merged
}

func (s *postgresTemplateImportStrongSnapshot) transcripcionsForIDs(ids []int) []TranscripcioRaw {
	if s == nil || len(ids) == 0 {
		return nil
	}
	rows := make([]TranscripcioRaw, 0, len(ids))
	for _, id := range normalizePositiveUniqueIDs(ids) {
		if row, ok := s.transcripcioByID[id]; ok {
			rows = append(rows, row)
		}
	}
	return rows
}

func (s *postgresTemplateImportStrongSnapshot) personesForIDs(ids []int) map[int][]TranscripcioPersonaRaw {
	if s == nil {
		return map[int][]TranscripcioPersonaRaw{}
	}
	return filterPersonaMapsByIDs(s.personesByID, ids)
}

func (s *postgresTemplateImportStrongSnapshot) atributsForIDs(ids []int) map[int][]TranscripcioAtributRaw {
	if s == nil {
		return map[int][]TranscripcioAtributRaw{}
	}
	return filterAtributMapsByIDs(s.atributsByID, ids)
}

func (s *postgresTemplateImportStrongSnapshot) preparedPersonesForIDs(ids []int) map[int]map[string]*TranscripcioPersonaRaw {
	if s == nil {
		return map[int]map[string]*TranscripcioPersonaRaw{}
	}
	filtered := map[int]map[string]*TranscripcioPersonaRaw{}
	for _, id := range normalizePositiveUniqueIDs(ids) {
		if persones, ok := s.preparedPersonesByID[id]; ok {
			filtered[id] = persones
		}
	}
	return filtered
}

func (s *postgresTemplateImportStrongSnapshot) preparedAtributsForIDs(ids []int) map[int]map[string]*TranscripcioAtributRaw {
	if s == nil {
		return map[int]map[string]*TranscripcioAtributRaw{}
	}
	filtered := map[int]map[string]*TranscripcioAtributRaw{}
	for _, id := range normalizePositiveUniqueIDs(ids) {
		if atributs, ok := s.preparedAtributsByID[id]; ok {
			filtered[id] = atributs
		}
	}
	return filtered
}

func (s *postgresTemplateImportStrongSnapshot) strongMatchKeysForPolicy(principalRoles []string) map[int]string {
	if s == nil {
		return map[int]string{}
	}
	if s.strongMatchKeysByPolicy == nil {
		s.strongMatchKeysByPolicy = map[string]map[int]string{}
	}
	policyKey := postgresStrongMatchPolicyKey(principalRoles)
	if keys, ok := s.strongMatchKeysByPolicy[policyKey]; ok && keys != nil {
		return keys
	}
	keys := make(map[int]string, len(s.ids))
	for _, id := range s.ids {
		row, ok := s.transcripcioByID[id]
		if !ok {
			continue
		}
		matchKey := postgresStrongMatchKeyForRow(&row, s.preparedPersonesByID[id], s.preparedAtributsByID[id], principalRoles)
		if matchKey != "" {
			keys[id] = matchKey
		}
	}
	s.strongMatchKeysByPolicy[policyKey] = keys
	return keys
}

func (s *postgresTemplateImportStrongSnapshot) strongMatchIDsForContext(ids []int, principalRoles []string) map[string]int {
	if s == nil {
		return map[string]int{}
	}
	keysByID := s.strongMatchKeysForPolicy(principalRoles)
	out := map[string]int{}
	for _, id := range normalizePositiveUniqueIDs(ids) {
		if matchKey, ok := keysByID[id]; ok && matchKey != "" {
			if _, exists := out[matchKey]; !exists {
				out[matchKey] = id
			}
		}
	}
	return out
}

func (s *postgresTemplateImportStrongSnapshot) strongMatchIDsForRequest(pageKey, tipusActe string, principalRoles []string) map[string]int {
	if s == nil {
		return map[string]int{}
	}
	if s.strongMatchIDsByRequest == nil {
		s.strongMatchIDsByRequest = map[string]map[string]int{}
	}
	cacheKey := postgresStrongContextRequestCacheKey(pageKey, tipusActe, principalRoles)
	if cached, ok := s.strongMatchIDsByRequest[cacheKey]; ok {
		return cached
	}
	ids := s.lookupStrongContextIDs(pageKey, tipusActe)
	prepared := s.strongMatchIDsForContext(ids, principalRoles)
	s.strongMatchIDsByRequest[cacheKey] = prepared
	return prepared
}

func postgresTemplateImportSnapshotKey(bookID, snapshotMaxID int) string {
	return strconv.Itoa(bookID) + ":" + strconv.Itoa(snapshotMaxID)
}

func postgresStrongContextKey(tipusActe, pageKey string) string {
	return normalizePostgresStrongContextPart(tipusActe) + "|" + normalizePostgresStrongContextPart(pageKey)
}

func postgresStrongContextRequestCacheKey(pageKey, tipusActe string, principalRoles []string) string {
	return postgresStrongContextKey(tipusActe, pageKey) + "\x1e" + postgresStrongMatchPolicyKey(principalRoles)
}

func normalizePostgresStrongContextPart(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func postgresStrongContextKeysForIncoming(pageKey string) []string {
	pageKey = strings.TrimSpace(pageKey)
	if pageKey == "" {
		return nil
	}
	keys := []string{normalizePostgresStrongContextPart(pageKey)}
	if pageNum, err := strconv.Atoi(pageKey); err == nil && pageNum > 0 {
		numeric := strconv.Itoa(pageNum)
		if numeric != keys[0] {
			keys = append(keys, numeric)
		}
	}
	return keys
}

func postgresStrongContextKeysForExisting(row *TranscripcioRaw, attrs []TranscripcioAtributRaw, pageNumsByPaginaID map[int]int) []string {
	keys := make([]string, 0, 3)
	seen := map[string]struct{}{}
	add := func(value string) {
		value = normalizePostgresStrongContextPart(value)
		if value == "" {
			return
		}
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		keys = append(keys, value)
	}
	if row != nil {
		add(row.NumPaginaText)
		if row.PaginaID.Valid {
			if pageNum, ok := pageNumsByPaginaID[int(row.PaginaID.Int64)]; ok && pageNum > 0 {
				add(strconv.Itoa(pageNum))
			}
		}
	}
	for _, attr := range attrs {
		if attr.Clau != "pagina_digital" {
			continue
		}
		if value := strings.TrimSpace(attr.ValorText); value != "" {
			add(value)
		}
	}
	return keys
}

func filterTranscripcionsBySnapshot(rows []TranscripcioRaw, snapshotMaxID int) []TranscripcioRaw {
	if len(rows) == 0 {
		return nil
	}
	res := make([]TranscripcioRaw, 0, len(rows))
	for _, row := range rows {
		if row.ID <= 0 {
			continue
		}
		if snapshotMaxID >= 0 && row.ID > snapshotMaxID {
			continue
		}
		res = append(res, row)
	}
	return res
}

func transcripcioIDs(rows []TranscripcioRaw) []int {
	ids := make([]int, 0, len(rows))
	for _, row := range rows {
		if row.ID > 0 {
			ids = append(ids, row.ID)
		}
	}
	return normalizePositiveUniqueIDs(ids)
}

func ensurePersonaMaps(rows map[int][]TranscripcioPersonaRaw) map[int][]TranscripcioPersonaRaw {
	if rows == nil {
		return map[int][]TranscripcioPersonaRaw{}
	}
	return rows
}

func ensureAtributMaps(rows map[int][]TranscripcioAtributRaw) map[int][]TranscripcioAtributRaw {
	if rows == nil {
		return map[int][]TranscripcioAtributRaw{}
	}
	return rows
}

func filterPersonaMapsByIDs(rows map[int][]TranscripcioPersonaRaw, ids []int) map[int][]TranscripcioPersonaRaw {
	filtered := map[int][]TranscripcioPersonaRaw{}
	rows = ensurePersonaMaps(rows)
	if len(ids) == 0 {
		return filtered
	}
	for _, id := range normalizePositiveUniqueIDs(ids) {
		if persones, ok := rows[id]; ok {
			filtered[id] = persones
		}
	}
	return filtered
}

func filterAtributMapsByIDs(rows map[int][]TranscripcioAtributRaw, ids []int) map[int][]TranscripcioAtributRaw {
	filtered := map[int][]TranscripcioAtributRaw{}
	rows = ensureAtributMaps(rows)
	if len(ids) == 0 {
		return filtered
	}
	for _, id := range normalizePositiveUniqueIDs(ids) {
		if atributs, ok := rows[id]; ok {
			filtered[id] = atributs
		}
	}
	return filtered
}

func parseTemplateImportPositiveInt(value string) (int, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	n, err := strconv.Atoi(value)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}
