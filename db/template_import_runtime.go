package db

import (
	"fmt"
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

type TemplateImportRuntime interface {
	Engine() string
	ListBookPages(bookID int) ([]LlibrePagina, error)
	GetBookPageByID(pageID int) (*LlibrePagina, error)
	GetBookPageByNum(bookID, pageNum int) (*LlibrePagina, error)
	ExistingSnapshotMaxID() (int, error)
	LoadStrongMatchCandidates(req TemplateImportStrongMatchRequest) (TemplateImportStrongMatchResult, error)
	LoadPrincipalMatchCandidates(req TemplateImportPrincipalMatchRequest) (TemplateImportPrincipalMatchResult, error)
	BulkCreateBundles(rows []TranscripcioRawImportBundle) (TranscripcioRawImportBulkResult, error)
	CreateBundle(row TranscripcioRawImportBundle) (TranscripcioRawImportBulkResult, error)
	LoadPersonesByLlibreID(llibreID int, transcripcioIDs []int) (map[int][]TranscripcioPersonaRaw, error)
	LoadAtributsByLlibreID(llibreID int, transcripcioIDs []int) (map[int][]TranscripcioAtributRaw, error)
}

type templateImportSnapshotMaxProvider interface {
	GetMaxTranscripcioRawID() (int, error)
}

type templateImportBundleCreator interface {
	BulkCreateTranscripcioRawBundles([]TranscripcioRawImportBundle) (TranscripcioRawImportBulkResult, error)
}

type templateImportStrongMatchCandidateLoader interface {
	ListTranscripcioStrongMatchCandidates(bookID int, tipusActe, pageKey string) ([]TranscripcioRaw, map[int][]TranscripcioPersonaRaw, map[int][]TranscripcioAtributRaw, error)
}

type templateImportStrongMatchCandidateSnapshotLoader interface {
	ListTranscripcioStrongMatchCandidatesUpToID(bookID int, tipusActe, pageKey string, maxExistingID int) ([]TranscripcioRaw, map[int][]TranscripcioPersonaRaw, map[int][]TranscripcioAtributRaw, error)
}

type templateImportRelatedByBookLoader interface {
	ListTranscripcioPersonesByLlibreID(llibreID int) (map[int][]TranscripcioPersonaRaw, error)
	ListTranscripcioAtributsByLlibreID(llibreID int) (map[int][]TranscripcioAtributRaw, error)
}

type templateImportRuntimeAdapter struct {
	database DB
}

func TemplateImportRuntimeFor(database DB) TemplateImportRuntime {
	return templateImportRuntimeAdapter{database: database}
}

func (r templateImportRuntimeAdapter) Engine() string {
	switch r.database.(type) {
	case *SQLite:
		return "sqlite"
	case *PostgreSQL:
		return "postgres"
	case *MySQL:
		return "mysql"
	default:
		return "generic"
	}
}

func (r templateImportRuntimeAdapter) ListBookPages(bookID int) ([]LlibrePagina, error) {
	if r.database == nil {
		return nil, nil
	}
	return r.database.ListLlibrePagines(bookID)
}

func (r templateImportRuntimeAdapter) GetBookPageByID(pageID int) (*LlibrePagina, error) {
	if r.database == nil {
		return nil, nil
	}
	return r.database.GetLlibrePaginaByID(pageID)
}

func (r templateImportRuntimeAdapter) GetBookPageByNum(bookID, pageNum int) (*LlibrePagina, error) {
	if r.database == nil {
		return nil, nil
	}
	return r.database.GetLlibrePaginaByNum(bookID, pageNum)
}

func (r templateImportRuntimeAdapter) ExistingSnapshotMaxID() (int, error) {
	if provider, ok := r.database.(templateImportSnapshotMaxProvider); ok {
		return provider.GetMaxTranscripcioRawID()
	}
	return 0, nil
}

func (r templateImportRuntimeAdapter) LoadStrongMatchCandidates(req TemplateImportStrongMatchRequest) (TemplateImportStrongMatchResult, error) {
	result := TemplateImportStrongMatchResult{
		PersonesByTranscripcioID: map[int][]TranscripcioPersonaRaw{},
		AtributsByTranscripcioID: map[int][]TranscripcioAtributRaw{},
	}
	if r.database == nil || req.BookID <= 0 {
		return result, nil
	}
	tipusActe := strings.TrimSpace(req.TipusActe)
	if tipusActe == "" || req.SnapshotMaxID == 0 {
		return result, nil
	}
	pageKey := strings.TrimSpace(req.PageKey)
	if req.SnapshotMaxID > 0 && pageKey != "" {
		if loader, ok := r.database.(templateImportStrongMatchCandidateSnapshotLoader); ok {
			trans, personesByID, atributsByID, err := loader.ListTranscripcioStrongMatchCandidatesUpToID(req.BookID, tipusActe, pageKey, req.SnapshotMaxID)
			if err == nil && len(trans) > 0 {
				result.Transcripcions = trans
				result.PersonesByTranscripcioID = ensurePersonaMaps(personesByID)
				result.AtributsByTranscripcioID = ensureAtributMaps(atributsByID)
				return result, nil
			}
		}
	}
	if pageKey != "" {
		if loader, ok := r.database.(templateImportStrongMatchCandidateLoader); ok {
			trans, personesByID, atributsByID, err := loader.ListTranscripcioStrongMatchCandidates(req.BookID, tipusActe, pageKey)
			if err == nil && len(trans) > 0 {
				filtered := filterTranscripcionsBySnapshot(trans, req.SnapshotMaxID)
				result.Transcripcions = filtered
				result.PersonesByTranscripcioID = ensurePersonaMaps(personesByID)
				result.AtributsByTranscripcioID = ensureAtributMaps(atributsByID)
				return result, nil
			}
		}
	}
	trans, err := r.database.ListTranscripcionsRaw(req.BookID, TranscripcioFilter{TipusActe: tipusActe, Limit: -1})
	if err != nil {
		return result, err
	}
	filtered := filterTranscripcionsBySnapshot(trans, req.SnapshotMaxID)
	result.Transcripcions = filtered
	ids := transcripcioIDs(filtered)
	if len(ids) == 0 {
		return result, nil
	}
	personesByID, err := r.database.ListTranscripcioPersonesByTranscripcioIDs(ids)
	if err != nil {
		return result, err
	}
	atributsByID, err := r.database.ListTranscripcioAtributsByTranscripcioIDs(ids)
	if err != nil {
		return result, err
	}
	result.PersonesByTranscripcioID = ensurePersonaMaps(personesByID)
	result.AtributsByTranscripcioID = ensureAtributMaps(atributsByID)
	return result, nil
}

func (r templateImportRuntimeAdapter) LoadPrincipalMatchCandidates(req TemplateImportPrincipalMatchRequest) (TemplateImportPrincipalMatchResult, error) {
	result := TemplateImportPrincipalMatchResult{
		PersonesByTranscripcioID: map[int][]TranscripcioPersonaRaw{},
	}
	if r.database == nil || req.BookID <= 0 {
		return result, nil
	}
	if req.SnapshotMaxID == 0 {
		return result, nil
	}
	trans, err := r.database.ListTranscripcionsRaw(req.BookID, TranscripcioFilter{Limit: -1})
	if err != nil {
		return result, err
	}
	filtered := filterTranscripcionsBySnapshot(trans, req.SnapshotMaxID)
	result.Transcripcions = filtered
	ids := transcripcioIDs(filtered)
	if len(ids) == 0 {
		return result, nil
	}
	personesByID, err := r.database.ListTranscripcioPersonesByTranscripcioIDs(ids)
	if err != nil {
		return result, err
	}
	result.PersonesByTranscripcioID = ensurePersonaMaps(personesByID)
	return result, nil
}

func (r templateImportRuntimeAdapter) BulkCreateBundles(rows []TranscripcioRawImportBundle) (TranscripcioRawImportBulkResult, error) {
	if creator, ok := r.database.(templateImportBundleCreator); ok {
		return creator.BulkCreateTranscripcioRawBundles(rows)
	}
	return TranscripcioRawImportBulkResult{}, fmt.Errorf("template import bulk create unsupported for engine=%s", r.Engine())
}

func (r templateImportRuntimeAdapter) CreateBundle(row TranscripcioRawImportBundle) (TranscripcioRawImportBulkResult, error) {
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

func (r templateImportRuntimeAdapter) LoadPersonesByLlibreID(llibreID int, transcripcioIDs []int) (map[int][]TranscripcioPersonaRaw, error) {
	if loader, ok := r.database.(templateImportRelatedByBookLoader); ok {
		personesByID, err := loader.ListTranscripcioPersonesByLlibreID(llibreID)
		if err == nil {
			return ensurePersonaMaps(personesByID), nil
		}
	}
	if r.database == nil {
		return map[int][]TranscripcioPersonaRaw{}, nil
	}
	personesByID, err := r.database.ListTranscripcioPersonesByTranscripcioIDs(transcripcioIDs)
	if err != nil {
		return nil, err
	}
	return ensurePersonaMaps(personesByID), nil
}

func (r templateImportRuntimeAdapter) LoadAtributsByLlibreID(llibreID int, transcripcioIDs []int) (map[int][]TranscripcioAtributRaw, error) {
	if loader, ok := r.database.(templateImportRelatedByBookLoader); ok {
		atributsByID, err := loader.ListTranscripcioAtributsByLlibreID(llibreID)
		if err == nil {
			return ensureAtributMaps(atributsByID), nil
		}
	}
	if r.database == nil {
		return map[int][]TranscripcioAtributRaw{}, nil
	}
	atributsByID, err := r.database.ListTranscripcioAtributsByTranscripcioIDs(transcripcioIDs)
	if err != nil {
		return nil, err
	}
	return ensureAtributMaps(atributsByID), nil
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
