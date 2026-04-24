package db

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type TemplateImportStrongMatchRequest struct {
	BookID        int
	TipusActe     string
	PageKey       string
	SnapshotMaxID int
}

type TemplateImportStrongMatchResult struct {
	Transcripcions           []TranscripcioRaw
	PersonesByTranscripcioID map[int][]TranscripcioPersonaRaw
	AtributsByTranscripcioID map[int][]TranscripcioAtributRaw
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
		return postgresTemplateImportRuntime{templateImportRuntimeBase: base}
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
	return genericStrongMatchCandidates(r.database, req)
}

func (r genericTemplateImportRuntime) LoadStrongMatchCandidates(req TemplateImportStrongMatchRequest) (TemplateImportStrongMatchResult, error) {
	return genericStrongMatchCandidates(r.database, req)
}

func (r postgresTemplateImportRuntime) LoadStrongMatchCandidates(req TemplateImportStrongMatchRequest) (TemplateImportStrongMatchResult, error) {
	result := emptyStrongMatchResult()
	if r.database == nil || req.BookID <= 0 || req.SnapshotMaxID == 0 {
		return result, nil
	}
	pageKey := strings.TrimSpace(req.PageKey)
	tipusActe := strings.TrimSpace(req.TipusActe)
	if pageKey == "" || tipusActe == "" {
		return result, nil
	}
	trans, personesByID, atributsByID, err := r.database.ListTranscripcioStrongMatchCandidatesUpToID(req.BookID, tipusActe, pageKey, req.SnapshotMaxID)
	if err != nil {
		return result, err
	}
	result.Transcripcions = filterTranscripcionsBySnapshot(trans, req.SnapshotMaxID)
	result.PersonesByTranscripcioID = ensurePersonaMaps(personesByID)
	result.AtributsByTranscripcioID = ensureAtributMaps(atributsByID)
	return result, nil
}

func (r sqliteTemplateImportRuntime) LoadPrincipalMatchCandidates(req TemplateImportPrincipalMatchRequest) (TemplateImportPrincipalMatchResult, error) {
	return genericPrincipalMatchCandidates(r.database, req)
}

func (r mysqlTemplateImportRuntime) LoadPrincipalMatchCandidates(req TemplateImportPrincipalMatchRequest) (TemplateImportPrincipalMatchResult, error) {
	return genericPrincipalMatchCandidates(r.database, req)
}

func (r genericTemplateImportRuntime) LoadPrincipalMatchCandidates(req TemplateImportPrincipalMatchRequest) (TemplateImportPrincipalMatchResult, error) {
	return genericPrincipalMatchCandidates(r.database, req)
}

func (r postgresTemplateImportRuntime) LoadPrincipalMatchCandidates(req TemplateImportPrincipalMatchRequest) (TemplateImportPrincipalMatchResult, error) {
	result := TemplateImportPrincipalMatchResult{
		PersonesByTranscripcioID: map[int][]TranscripcioPersonaRaw{},
	}
	if r.database == nil || req.BookID <= 0 || req.SnapshotMaxID == 0 {
		return result, nil
	}
	trans, err := r.database.ListTranscripcionsRaw(req.BookID, TranscripcioFilter{Limit: -1})
	if err != nil {
		return result, err
	}
	result.Transcripcions = filterTranscripcionsBySnapshot(trans, req.SnapshotMaxID)
	ids := transcripcioIDs(result.Transcripcions)
	if len(ids) == 0 {
		return result, nil
	}
	personesByID, err := r.database.ListTranscripcioPersonesByLlibreID(req.BookID)
	if err != nil {
		return result, err
	}
	result.PersonesByTranscripcioID = filterPersonaMapsByIDs(personesByID, ids)
	return result, nil
}

func (r postgresTemplateImportRuntime) CreateBundle(row TranscripcioRawImportBundle) (TranscripcioRawImportBulkResult, error) {
	res, err := r.BulkCreateBundles([]TranscripcioRawImportBundle{row})
	if err != nil {
		return res, err
	}
	if len(res.IDs) != 1 || res.IDs[0] == 0 {
		return res, fmt.Errorf("template import create unsupported for engine=%s", r.Engine())
	}
	return res, nil
}

func (r sqliteTemplateImportRuntime) LoadPersonesByLlibreID(llibreID int, transcripcioIDs []int) (map[int][]TranscripcioPersonaRaw, error) {
	return genericRelatedPersonesByIDs(r.database, transcripcioIDs)
}

func (r mysqlTemplateImportRuntime) LoadPersonesByLlibreID(llibreID int, transcripcioIDs []int) (map[int][]TranscripcioPersonaRaw, error) {
	return genericRelatedPersonesByIDs(r.database, transcripcioIDs)
}

func (r genericTemplateImportRuntime) LoadPersonesByLlibreID(llibreID int, transcripcioIDs []int) (map[int][]TranscripcioPersonaRaw, error) {
	return genericRelatedPersonesByIDs(r.database, transcripcioIDs)
}

func (r postgresTemplateImportRuntime) LoadPersonesByLlibreID(llibreID int, transcripcioIDs []int) (map[int][]TranscripcioPersonaRaw, error) {
	if r.database == nil {
		return map[int][]TranscripcioPersonaRaw{}, nil
	}
	rows, err := r.database.ListTranscripcioPersonesByLlibreID(llibreID)
	if err != nil {
		return nil, err
	}
	return filterPersonaMapsByIDs(rows, transcripcioIDs), nil
}

func (r sqliteTemplateImportRuntime) LoadAtributsByLlibreID(llibreID int, transcripcioIDs []int) (map[int][]TranscripcioAtributRaw, error) {
	return genericRelatedAtributsByIDs(r.database, transcripcioIDs)
}

func (r mysqlTemplateImportRuntime) LoadAtributsByLlibreID(llibreID int, transcripcioIDs []int) (map[int][]TranscripcioAtributRaw, error) {
	return genericRelatedAtributsByIDs(r.database, transcripcioIDs)
}

func (r genericTemplateImportRuntime) LoadAtributsByLlibreID(llibreID int, transcripcioIDs []int) (map[int][]TranscripcioAtributRaw, error) {
	return genericRelatedAtributsByIDs(r.database, transcripcioIDs)
}

func (r postgresTemplateImportRuntime) LoadAtributsByLlibreID(llibreID int, transcripcioIDs []int) (map[int][]TranscripcioAtributRaw, error) {
	if r.database == nil {
		return map[int][]TranscripcioAtributRaw{}, nil
	}
	rows, err := r.database.ListTranscripcioAtributsByLlibreID(llibreID)
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
		PersonesByTranscripcioID: map[int][]TranscripcioPersonaRaw{},
		AtributsByTranscripcioID: map[int][]TranscripcioAtributRaw{},
	}
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
