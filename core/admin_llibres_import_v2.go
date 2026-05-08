package core

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type llibresImportPayloadV2 struct {
	Schema string                `json:"schema"`
	Items  llibresImportItemsV2  `json:"items"`
	Source llibresImportSourceV2 `json:"source"`
}

type llibresImportSourceV2 struct {
	App    string `json:"app"`
	Module string `json:"module"`
}

type llibresImportItemsV2 struct {
	Llibres []llibreImportRecordV2 `json:"llibres"`
}

type llibreImportRecordV2 struct {
	Code             string
	Title            string
	ChurchName       string
	BookType         string
	Chronology       string
	Volume           string
	Abbot            string
	Content          string
	Language         string
	Requirements     string
	CatalogUnit      string
	InstallationUnit string
	PageCount        *int
	PageEntries      []llibreImportPageV2
	URLBase          string
	URLImagePrefix   string
	DefaultPage      string
	IndexedComplete  bool
	DigitalCode      string
	PhysicalCode     string
	SourceSystem     string
	ExternalID       string
	ExternalCode     string
	Municipality     llibreImportMunicipalityV2
	Archives         []llibreImportArchiveRefV2
	URLs             []llibreImportURLRefV2
	Legacy           *llibreImportLegacyV2
}

type llibreImportMunicipalityV2 struct {
	Name        string `json:"name"`
	CountryISO2 string `json:"country_iso2"`
}

type llibreImportArchiveRefV2 struct {
	ArchiveCode      string
	ArchiveName      string
	RelationType     string
	Principal        bool
	PreferredDisplay bool
	Signatura        string
	URLOverride      string
	SourceSystem     string
	ExternalID       string
	ExternalCode     string
	Notes            string
}

type llibreImportURLRefV2 struct {
	URL         string
	Type        string
	Description string
	ArchiveCode string
	ArchiveName string
}

type llibreImportPageV2 struct {
	PageNumber     int    `json:"page_number"`
	CanonicalLabel string `json:"canonical_label"`
}

type llibreImportLegacyV2 struct {
	OldID            int    `json:"old_id"`
	ArquebisbatNom   string `json:"arquebisbat_nom"`
	ArquebisbatTipus string `json:"arquebisbat_tipus"`
}

type llibreImportDiagnosticsV2 struct {
	Total               int                 `json:"total"`
	CreatedBooks        int                 `json:"created_books"`
	UpdatedPendingBooks int                 `json:"updated_pending_books"`
	ExistingBooks       int                 `json:"existing_books"`
	CreatedArchiveLinks int                 `json:"created_archive_links"`
	SkippedArchiveLinks int                 `json:"skipped_archive_links"`
	CreatedURLs         int                 `json:"created_urls"`
	SkippedURLs         int                 `json:"skipped_urls"`
	CreatedPages        int                 `json:"created_pages"`
	SkippedPages        int                 `json:"skipped_pages"`
	ErrorsTotal         int                 `json:"errors_total"`
	ErrorsByReason      map[string]int      `json:"errors_by_reason"`
	WarningsByReason    map[string]int      `json:"warnings_by_reason"`
	FirstErrors         []string            `json:"first_errors"`
	Targets             []db.AdminJobTarget `json:"-"`
}

func detectLlibresImportSchema(raw []byte) string {
	var head struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(raw, &head); err != nil {
		return ""
	}
	return strings.TrimSpace(head.Schema)
}

func (r *llibreImportRecordV2) UnmarshalJSON(data []byte) error {
	type alias struct {
		Code                  string                     `json:"code"`
		Title                 string                     `json:"title"`
		ChurchName            string                     `json:"church_name"`
		BookType              string                     `json:"book_type"`
		Chronology            string                     `json:"chronology"`
		Volume                string                     `json:"volume"`
		Abbot                 string                     `json:"abbot"`
		Content               string                     `json:"content"`
		Language              string                     `json:"language"`
		Requirements          string                     `json:"requirements"`
		TechnicalRequirements string                     `json:"technical_requirements"`
		CatalogUnit           string                     `json:"catalog_unit"`
		InstallationUnit      string                     `json:"installation_unit"`
		URLBase               string                     `json:"url_base"`
		BaseURL               string                     `json:"base_url"`
		URLImagePrefix        string                     `json:"url_image_prefix"`
		ImageURLPrefix        string                     `json:"image_url_prefix"`
		DefaultPage           string                     `json:"default_page"`
		Page                  string                     `json:"page"`
		IndexedComplete       bool                       `json:"indexed_complete"`
		CompleteIndexation    bool                       `json:"complete_indexation"`
		DigitalCode           string                     `json:"digital_code"`
		PhysicalCode          string                     `json:"physical_code"`
		SourceSystem          string                     `json:"source_system"`
		ExternalID            string                     `json:"external_id"`
		ExternalCode          string                     `json:"external_code"`
		Municipality          llibreImportMunicipalityV2 `json:"municipality"`
		Archives              []llibreImportArchiveRefV2 `json:"archives"`
		URLs                  []llibreImportURLRefV2     `json:"urls"`
		Legacy                *llibreImportLegacyV2      `json:"legacy"`
	}
	var aux alias
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	r.Code = strings.TrimSpace(aux.Code)
	r.Title = strings.TrimSpace(aux.Title)
	r.ChurchName = strings.TrimSpace(aux.ChurchName)
	r.BookType = strings.TrimSpace(aux.BookType)
	r.Chronology = strings.TrimSpace(aux.Chronology)
	r.Volume = strings.TrimSpace(aux.Volume)
	r.Abbot = strings.TrimSpace(aux.Abbot)
	r.Content = strings.TrimSpace(aux.Content)
	r.Language = strings.TrimSpace(aux.Language)
	r.Requirements = strings.TrimSpace(aux.Requirements)
	if r.Requirements == "" {
		r.Requirements = strings.TrimSpace(aux.TechnicalRequirements)
	}
	r.CatalogUnit = strings.TrimSpace(aux.CatalogUnit)
	r.InstallationUnit = strings.TrimSpace(aux.InstallationUnit)
	r.URLBase = strings.TrimSpace(aux.URLBase)
	if r.URLBase == "" {
		r.URLBase = strings.TrimSpace(aux.BaseURL)
	}
	r.URLImagePrefix = strings.TrimSpace(aux.URLImagePrefix)
	if r.URLImagePrefix == "" {
		r.URLImagePrefix = strings.TrimSpace(aux.ImageURLPrefix)
	}
	r.DefaultPage = strings.TrimSpace(aux.DefaultPage)
	if r.DefaultPage == "" {
		r.DefaultPage = strings.TrimSpace(aux.Page)
	}
	r.IndexedComplete = aux.IndexedComplete || aux.CompleteIndexation
	r.DigitalCode = strings.TrimSpace(aux.DigitalCode)
	r.PhysicalCode = strings.TrimSpace(aux.PhysicalCode)
	r.SourceSystem = strings.TrimSpace(aux.SourceSystem)
	r.ExternalID = strings.TrimSpace(aux.ExternalID)
	r.ExternalCode = strings.TrimSpace(aux.ExternalCode)
	r.Municipality = aux.Municipality
	r.Municipality.Name = strings.TrimSpace(r.Municipality.Name)
	r.Municipality.CountryISO2 = strings.ToUpper(strings.TrimSpace(r.Municipality.CountryISO2))
	r.Archives = aux.Archives
	r.URLs = aux.URLs
	r.Legacy = aux.Legacy

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if pagesRaw, ok := raw["pages"]; ok && len(bytes.TrimSpace(pagesRaw)) > 0 {
		trimmed := bytes.TrimSpace(pagesRaw)
		switch trimmed[0] {
		case '[':
			if err := json.Unmarshal(trimmed, &r.PageEntries); err != nil {
				return err
			}
		default:
			var pageCount int
			if err := json.Unmarshal(trimmed, &pageCount); err == nil && pageCount > 0 {
				r.PageCount = &pageCount
			}
		}
	}
	return nil
}

func (r *llibreImportArchiveRefV2) UnmarshalJSON(data []byte) error {
	type alias struct {
		ArchiveCode         string `json:"archive_code"`
		ArchiveName         string `json:"archive_name"`
		RelationType        string `json:"relation_type"`
		Principal           bool   `json:"principal"`
		PreferredDisplay    bool   `json:"preferred_display"`
		PreferredForDisplay bool   `json:"preferred_for_display"`
		Signature           string `json:"signature"`
		Signatura           string `json:"signatura"`
		URLOverride         string `json:"url_override"`
		SourceSystem        string `json:"source_system"`
		ExternalID          string `json:"external_id"`
		ExternalCode        string `json:"external_code"`
		Notes               string `json:"notes"`
	}
	var aux alias
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	r.ArchiveCode = strings.TrimSpace(aux.ArchiveCode)
	r.ArchiveName = strings.TrimSpace(aux.ArchiveName)
	r.RelationType = strings.TrimSpace(aux.RelationType)
	r.Principal = aux.Principal
	r.PreferredDisplay = aux.PreferredDisplay || aux.PreferredForDisplay
	r.Signatura = strings.TrimSpace(aux.Signatura)
	if r.Signatura == "" {
		r.Signatura = strings.TrimSpace(aux.Signature)
	}
	r.URLOverride = strings.TrimSpace(aux.URLOverride)
	r.SourceSystem = strings.TrimSpace(aux.SourceSystem)
	r.ExternalID = strings.TrimSpace(aux.ExternalID)
	r.ExternalCode = strings.TrimSpace(aux.ExternalCode)
	r.Notes = strings.TrimSpace(aux.Notes)
	return nil
}

func (r *llibreImportURLRefV2) UnmarshalJSON(data []byte) error {
	type alias struct {
		URL         string `json:"url"`
		Type        string `json:"type"`
		Description string `json:"description"`
		ArchiveCode string `json:"archive_code"`
		ArchiveName string `json:"archive_name"`
	}
	var aux alias
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	r.URL = strings.TrimSpace(aux.URL)
	r.Type = strings.TrimSpace(aux.Type)
	r.Description = strings.TrimSpace(aux.Description)
	r.ArchiveCode = strings.TrimSpace(aux.ArchiveCode)
	r.ArchiveName = strings.TrimSpace(aux.ArchiveName)
	return nil
}

func (a *App) runLlibresImportV2(w http.ResponseWriter, r *http.Request, user *db.User, returnTo string, rawPayload []byte, start time.Time) {
	var payload llibresImportPayloadV2
	if err := json.Unmarshal(rawPayload, &payload); err != nil || strings.TrimSpace(payload.Schema) != "cercagenealogica.llibres.v2" {
		a.logAdminImportRunDetailed(r, "llibres", adminImportStatusError, user.ID, &adminImportJobDetail{
			Payload:    map[string]interface{}{"import_type": "llibres", "import_format": "v2"},
			Result:     map[string]interface{}{"status": adminImportStatusError, "errors_by_reason": map[string]int{"invalid_payload": 1}},
			StartedAt:  start,
			FinishedAt: time.Now(),
		})
		http.Redirect(w, r, withQueryParams(returnTo, map[string]string{"err": "1"}), http.StatusSeeOther)
		return
	}
	diag := &llibreImportDiagnosticsV2{
		Total:            len(payload.Items.Llibres),
		ErrorsByReason:   map[string]int{},
		WarningsByReason: map[string]int{},
		FirstErrors:      []string{},
	}
	addError := func(reason, detail string) {
		diag.ErrorsTotal++
		diag.ErrorsByReason[reason]++
		if detail != "" && len(diag.FirstErrors) < 10 {
			diag.FirstErrors = append(diag.FirstErrors, detail)
		}
	}
	municipalityRows, err := a.resolveLlibresImportMunicipisV2(payload.Items.Llibres)
	if err != nil {
		addError("database_error", err.Error())
	}
	archiveCodeRows, archiveNameRows, err := a.resolveLlibresImportArxiusV2(payload.Items.Llibres)
	if err != nil {
		addError("database_error", err.Error())
	}

	for _, record := range payload.Items.Llibres {
		if err := a.importSingleLlibreV2(r, user, record, municipalityRows, archiveCodeRows, archiveNameRows, diag); err != nil {
			addError("database_error", err.Error())
		}
	}

	status := adminImportStatusOK
	if diag.ErrorsTotal > 0 {
		status = adminImportStatusError
	}
	a.logAdminImportRunDetailed(r, "llibres", status, user.ID, &adminImportJobDetail{
		Payload: map[string]interface{}{
			"import_type":   "llibres",
			"import_format": "v2",
			"schema":        payload.Schema,
			"total":         diag.Total,
		},
		Result: map[string]interface{}{
			"status":                status,
			"total":                 diag.Total,
			"created_books":         diag.CreatedBooks,
			"updated_pending_books": diag.UpdatedPendingBooks,
			"existing_books":        diag.ExistingBooks,
			"created_archive_links": diag.CreatedArchiveLinks,
			"skipped_archive_links": diag.SkippedArchiveLinks,
			"created_urls":          diag.CreatedURLs,
			"skipped_urls":          diag.SkippedURLs,
			"created_pages":         diag.CreatedPages,
			"skipped_pages":         diag.SkippedPages,
			"errors_total":          diag.ErrorsTotal,
			"errors_by_reason":      diag.ErrorsByReason,
			"warnings_by_reason":    diag.WarningsByReason,
			"first_errors":          diag.FirstErrors,
		},
		Targets:       diag.Targets,
		ProgressTotal: diag.Total,
		ProgressDone:  diag.Total - diag.ErrorsTotal,
		StartedAt:     start,
		FinishedAt:    time.Now(),
	})
	redirect := withQueryParams(returnTo, map[string]string{
		"import":                "1",
		"llibres_total":         strconv.Itoa(diag.Total),
		"llibres_created":       strconv.Itoa(diag.CreatedBooks),
		"llibres_existing":      strconv.Itoa(diag.ExistingBooks),
		"llibres_errors":        strconv.Itoa(diag.ErrorsTotal),
		"archive_links":         strconv.Itoa(diag.CreatedArchiveLinks),
		"archive_links_skipped": strconv.Itoa(diag.SkippedArchiveLinks),
	})
	http.Redirect(w, r, redirect, http.StatusSeeOther)
}

func (a *App) resolveLlibresImportMunicipisV2(records []llibreImportRecordV2) (map[string][]db.MunicipiResolveRow, error) {
	namesSet := map[string]struct{}{}
	for _, row := range records {
		if row.Municipality.Name != "" {
			namesSet[strings.ToLower(row.Municipality.Name)] = struct{}{}
		}
	}
	if len(namesSet) == 0 {
		return map[string][]db.MunicipiResolveRow{}, nil
	}
	names := make([]string, 0, len(namesSet))
	for name := range namesSet {
		names = append(names, name)
	}
	rows, err := a.DB.ResolveMunicipisByNames(names)
	if err != nil {
		return nil, err
	}
	out := map[string][]db.MunicipiResolveRow{}
	for _, row := range rows {
		key := normalizeKey(row.Nom)
		out[key] = append(out[key], row)
	}
	return out, nil
}

func (a *App) resolveLlibresImportArxiusV2(records []llibreImportRecordV2) (map[string][]db.ArxiuResolveRow, map[string][]db.ArxiuResolveRow, error) {
	codeSet := map[string]struct{}{}
	nameSet := map[string]struct{}{}
	for _, row := range records {
		for _, archive := range row.Archives {
			if archive.ArchiveCode != "" {
				codeSet[strings.ToLower(archive.ArchiveCode)] = struct{}{}
			}
			if archive.ArchiveName != "" {
				nameSet[strings.ToLower(archive.ArchiveName)] = struct{}{}
			}
		}
	}
	codeMap := map[string][]db.ArxiuResolveRow{}
	if len(codeSet) > 0 {
		codes := make([]string, 0, len(codeSet))
		for code := range codeSet {
			codes = append(codes, code)
		}
		rows, err := a.DB.ResolveArxiusByCodes(codes)
		if err != nil {
			return nil, nil, err
		}
		for _, row := range rows {
			key := normalizeKey(row.Codi)
			codeMap[key] = append(codeMap[key], row)
		}
	}
	nameMap := map[string][]db.ArxiuResolveRow{}
	if len(nameSet) > 0 {
		names := make([]string, 0, len(nameSet))
		for name := range nameSet {
			names = append(names, name)
		}
		rows, err := a.DB.ResolveArxiusByNames(names)
		if err != nil {
			return nil, nil, err
		}
		for _, row := range rows {
			key := normalizeKey(row.Nom)
			nameMap[key] = append(nameMap[key], row)
		}
	}
	return codeMap, nameMap, nil
}

func (a *App) importSingleLlibreV2(r *http.Request, user *db.User, record llibreImportRecordV2, municipis map[string][]db.MunicipiResolveRow, archiveCodes, archiveNames map[string][]db.ArxiuResolveRow, diag *llibreImportDiagnosticsV2) error {
	municipiID, reason := resolveLlibresImportMunicipiV2(record, municipis)
	if reason != "" {
		diag.ErrorsTotal++
		diag.ErrorsByReason[reason]++
		if len(diag.FirstErrors) < 10 {
			diag.FirstErrors = append(diag.FirstErrors, buildLlibresImportV2ErrorDetail(record, reason))
		}
		return nil
	}
	resolvedArchives, reason := resolveLlibresImportArxiusRefsV2(record, archiveCodes, archiveNames)
	if reason != "" {
		diag.ErrorsTotal++
		diag.ErrorsByReason[reason]++
		if len(diag.FirstErrors) < 10 {
			diag.FirstErrors = append(diag.FirstErrors, buildLlibresImportV2ErrorDetail(record, reason))
		}
		return nil
	}
	llibre, matchType, ambiguous, err := a.resolveExistingLlibreImportV2(record, municipiID, resolvedArchives)
	if err != nil {
		diag.ErrorsTotal++
		diag.ErrorsByReason["database_error"]++
		if len(diag.FirstErrors) < 10 {
			diag.FirstErrors = append(diag.FirstErrors, err.Error())
		}
		return nil
	}
	if ambiguous {
		diag.ErrorsTotal++
		diag.ErrorsByReason["book_duplicate_ambiguous"]++
		if len(diag.FirstErrors) < 10 {
			diag.FirstErrors = append(diag.FirstErrors, buildLlibresImportV2ErrorDetail(record, "book_duplicate_ambiguous"))
		}
		return nil
	}
	if llibre == nil {
		newBook := buildLlibreImportV2Record(record, municipiID, user.ID)
		newID, err := a.DB.CreateLlibre(&newBook)
		if err != nil {
			return err
		}
		diag.CreatedBooks++
		diag.Targets = append(diag.Targets, db.AdminJobTarget{ObjectType: "llibre", ObjectID: newID})
		llibre = &newBook
		llibre.ID = newID
	} else if llibre.ModeracioEstat == "publicat" {
		if a.llibreImportV2NeedsPublishedChange(llibre, record, resolvedArchives) {
			diag.ErrorsTotal++
			diag.ErrorsByReason["published_book_requires_moderated_change"]++
			if len(diag.FirstErrors) < 10 {
				diag.FirstErrors = append(diag.FirstErrors, buildLlibresImportV2ErrorDetail(record, "published_book_requires_moderated_change"))
			}
			return nil
		}
		diag.ExistingBooks++
		_ = matchType
		return nil
	} else {
		updated := mergePendingLlibreFromImportV2(llibre, record, municipiID, user.ID)
		if updated {
			if err := a.DB.UpdateLlibre(llibre); err != nil {
				return err
			}
			diag.UpdatedPendingBooks++
			diag.Targets = append(diag.Targets, db.AdminJobTarget{ObjectType: "llibre", ObjectID: llibre.ID})
		} else {
			diag.ExistingBooks++
		}
	}
	createdLinks, skippedLinks, err := a.applyLlibreImportV2ArchiveLinks(llibre, resolvedArchives, user.ID)
	if err != nil {
		return err
	}
	diag.CreatedArchiveLinks += createdLinks
	diag.SkippedArchiveLinks += skippedLinks
	createdURLs, skippedURLs, err := a.applyLlibreImportV2URLs(llibre.ID, record.URLs, resolvedArchives, user.ID, diag)
	if err != nil {
		return err
	}
	diag.CreatedURLs += createdURLs
	diag.SkippedURLs += skippedURLs
	createdPages, skippedPages, err := a.applyLlibreImportV2Pages(llibre.ID, record.PageEntries)
	if err != nil {
		return err
	}
	diag.CreatedPages += createdPages
	diag.SkippedPages += skippedPages
	_ = matchType
	return nil
}

type llibreImportResolvedArchiveV2 struct {
	Ref llibreImportArchiveRefV2
	DB  db.ArxiuResolveRow
}

func resolveLlibresImportMunicipiV2(record llibreImportRecordV2, municipalities map[string][]db.MunicipiResolveRow) (int, string) {
	if strings.TrimSpace(record.Municipality.Name) == "" {
		return 0, "municipality_missing"
	}
	rows := municipalities[normalizeKey(record.Municipality.Name)]
	if len(rows) == 0 {
		return 0, "municipality_not_found"
	}
	if len(rows) == 1 {
		return rows[0].ID, ""
	}
	filtered := make([]db.MunicipiResolveRow, 0, len(rows))
	if iso2 := strings.ToUpper(strings.TrimSpace(record.Municipality.CountryISO2)); iso2 != "" {
		for _, row := range rows {
			if row.ISO2.Valid && strings.EqualFold(strings.TrimSpace(row.ISO2.String), iso2) {
				filtered = append(filtered, row)
			}
		}
	}
	if len(filtered) == 1 {
		return filtered[0].ID, ""
	}
	if len(filtered) > 1 {
		return 0, "municipality_ambiguous"
	}
	return 0, "municipality_ambiguous"
}

func resolveLlibresImportArxiusRefsV2(record llibreImportRecordV2, codeMap, nameMap map[string][]db.ArxiuResolveRow) ([]llibreImportResolvedArchiveV2, string) {
	if len(record.Archives) == 0 {
		return nil, "archive_missing"
	}
	resolved := make([]llibreImportResolvedArchiveV2, 0, len(record.Archives))
	seen := map[int]struct{}{}
	for _, archive := range record.Archives {
		if archive.ArchiveCode == "" && archive.ArchiveName == "" {
			return nil, "archive_code_missing"
		}
		rows := []db.ArxiuResolveRow{}
		if archive.ArchiveCode != "" {
			rows = codeMap[normalizeKey(archive.ArchiveCode)]
			if len(rows) == 0 {
				return nil, "archive_not_found"
			}
		} else {
			rows = nameMap[normalizeKey(archive.ArchiveName)]
			if len(rows) == 0 {
				return nil, "archive_not_found"
			}
		}
		if len(rows) != 1 {
			return nil, "archive_ambiguous"
		}
		if _, ok := seen[rows[0].ID]; ok {
			continue
		}
		seen[rows[0].ID] = struct{}{}
		resolved = append(resolved, llibreImportResolvedArchiveV2{Ref: archive, DB: rows[0]})
	}
	if len(resolved) == 0 {
		return nil, "archive_missing"
	}
	return resolved, ""
}

func (a *App) resolveExistingLlibreImportV2(record llibreImportRecordV2, municipiID int, archives []llibreImportResolvedArchiveV2) (*db.Llibre, string, bool, error) {
	if ids, err := a.resolveLlibreIDsByStableRefsV2(record); err != nil {
		return nil, "", false, err
	} else if len(ids) == 1 {
		llibre, err := a.DB.GetLlibre(ids[0])
		return llibre, "stable_ref", false, err
	} else if len(ids) > 1 {
		return nil, "stable_ref", true, nil
	}
	if ids, err := a.resolveLlibreIDsByArchiveExternalRefsV2(archives); err != nil {
		return nil, "", false, err
	} else if len(ids) == 1 {
		llibre, err := a.DB.GetLlibre(ids[0])
		return llibre, "archive_external_ref", false, err
	} else if len(ids) > 1 {
		return nil, "archive_external_ref", true, nil
	}
	if ids, err := a.resolveLlibreIDsByFallbackV2(record, municipiID); err != nil {
		return nil, "", false, err
	} else if len(ids) == 1 {
		llibre, err := a.DB.GetLlibre(ids[0])
		return llibre, "fallback", false, err
	} else if len(ids) > 1 {
		return nil, "fallback", true, nil
	}
	return nil, "", false, nil
}

func (a *App) resolveLlibreIDsByStableRefsV2(record llibreImportRecordV2) ([]int, error) {
	ids := map[int]struct{}{}
	type queryDef struct {
		sql  string
		args []interface{}
	}
	queries := make([]queryDef, 0, 5)
	if code := strings.TrimSpace(record.Code); code != "" {
		queries = append(queries, queryDef{
			sql:  `SELECT id FROM llibres WHERE codi = ?`,
			args: []interface{}{code},
		})
	}
	if source := strings.TrimSpace(record.SourceSystem); source != "" {
		if externalID := strings.TrimSpace(record.ExternalID); externalID != "" {
			queries = append(queries, queryDef{
				sql:  `SELECT id FROM llibres WHERE source_system = ? AND external_id = ?`,
				args: []interface{}{source, externalID},
			})
		}
		if externalCode := strings.TrimSpace(record.ExternalCode); externalCode != "" {
			queries = append(queries, queryDef{
				sql:  `SELECT id FROM llibres WHERE source_system = ? AND external_code = ?`,
				args: []interface{}{source, externalCode},
			})
		}
	}
	for _, q := range queries {
		rows, err := a.DB.Query(formatSQLForDB(a.DB, q.sql), q.args...)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			if id := rowInt(row, "id"); id > 0 {
				ids[id] = struct{}{}
			}
		}
	}
	return mapKeysInt(ids), nil
}

func (a *App) resolveLlibreIDsByArchiveExternalRefsV2(archives []llibreImportResolvedArchiveV2) ([]int, error) {
	ids := map[int]struct{}{}
	for _, archive := range archives {
		ref := archive.Ref
		if ref.SourceSystem == "" {
			continue
		}
		var rows []map[string]interface{}
		var err error
		if ref.ExternalID != "" {
			query := formatSQLForDB(a.DB, `
                SELECT llibre_id
                FROM arxius_llibres
                WHERE arxiu_id = ? AND source_system = ? AND external_id = ?`)
			rows, err = a.DB.Query(query, archive.DB.ID, ref.SourceSystem, ref.ExternalID)
		} else if ref.ExternalCode != "" {
			query := formatSQLForDB(a.DB, `
                SELECT llibre_id
                FROM arxius_llibres
                WHERE arxiu_id = ? AND source_system = ? AND external_code = ?`)
			rows, err = a.DB.Query(query, archive.DB.ID, ref.SourceSystem, ref.ExternalCode)
		} else {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			if id := rowInt(row, "llibre_id"); id > 0 {
				ids[id] = struct{}{}
			}
		}
	}
	return mapKeysInt(ids), nil
}

func (a *App) resolveLlibreIDsByFallbackV2(record llibreImportRecordV2, municipiID int) ([]int, error) {
	clauses := []string{"municipi_id = ?"}
	args := []interface{}{municipiID}
	if record.BookType != "" {
		clauses = append(clauses, "tipus_llibre = ?")
		args = append(args, record.BookType)
	}
	if record.Chronology != "" {
		clauses = append(clauses, "cronologia = ?")
		args = append(args, record.Chronology)
	}
	if record.Title != "" {
		clauses = append(clauses, "titol = ?")
		args = append(args, record.Title)
	}
	query := formatSQLForDB(a.DB, `SELECT id FROM llibres WHERE `+strings.Join(clauses, " AND "))
	rows, err := a.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	ids := make([]int, 0, len(rows))
	for _, row := range rows {
		if id := rowInt(row, "id"); id > 0 {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 && record.ChurchName != "" && record.CatalogUnit != "" {
		query = formatSQLForDB(a.DB, `
            SELECT id FROM llibres
            WHERE municipi_id = ? AND tipus_llibre = ? AND cronologia = ? AND nom_esglesia = ? AND unitat_catalogacio = ?`)
		rows, err = a.DB.Query(query, municipiID, record.BookType, record.Chronology, record.ChurchName, record.CatalogUnit)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			if id := rowInt(row, "id"); id > 0 {
				ids = append(ids, id)
			}
		}
	}
	return dedupeIntSlice(ids), nil
}

func buildLlibreImportV2Record(record llibreImportRecordV2, municipiID, userID int) db.Llibre {
	pagines := sql.NullInt64{}
	if record.PageCount != nil && *record.PageCount > 0 {
		pagines = sql.NullInt64{Int64: int64(*record.PageCount), Valid: true}
	}
	return db.Llibre{
		MunicipiID:        municipiID,
		NomEsglesia:       record.ChurchName,
		Codi:              record.Code,
		CodiDigital:       record.DigitalCode,
		CodiFisic:         record.PhysicalCode,
		SourceSystem:      record.SourceSystem,
		ExternalID:        record.ExternalID,
		ExternalCode:      record.ExternalCode,
		Titol:             record.Title,
		TipusLlibre:       record.BookType,
		Cronologia:        record.Chronology,
		Volum:             record.Volume,
		Abat:              record.Abbot,
		Contingut:         record.Content,
		Llengua:           record.Language,
		Requeriments:      record.Requirements,
		UnitatCatalogacio: record.CatalogUnit,
		UnitatInstalacio:  record.InstallationUnit,
		Pagines:           pagines,
		URLBase:           record.URLBase,
		URLImatgePrefix:   record.URLImagePrefix,
		Pagina:            record.DefaultPage,
		IndexacioCompleta: record.IndexedComplete,
		CreatedBy:         sqlNullIntFromInt(userID),
		ModeracioEstat:    "pendent",
	}
}

func mergePendingLlibreFromImportV2(existing *db.Llibre, record llibreImportRecordV2, municipiID, userID int) bool {
	updated := false
	desired := buildLlibreImportV2Record(record, municipiID, userID)
	if existing.MunicipiID != desired.MunicipiID {
		existing.MunicipiID = desired.MunicipiID
		updated = true
	}
	assignString := func(dst *string, val string) {
		if *dst != val {
			*dst = val
			updated = true
		}
	}
	assignString(&existing.NomEsglesia, desired.NomEsglesia)
	assignString(&existing.Codi, desired.Codi)
	assignString(&existing.CodiDigital, desired.CodiDigital)
	assignString(&existing.CodiFisic, desired.CodiFisic)
	assignString(&existing.SourceSystem, desired.SourceSystem)
	assignString(&existing.ExternalID, desired.ExternalID)
	assignString(&existing.ExternalCode, desired.ExternalCode)
	assignString(&existing.Titol, desired.Titol)
	assignString(&existing.TipusLlibre, desired.TipusLlibre)
	assignString(&existing.Cronologia, desired.Cronologia)
	assignString(&existing.Volum, desired.Volum)
	assignString(&existing.Abat, desired.Abat)
	assignString(&existing.Contingut, desired.Contingut)
	assignString(&existing.Llengua, desired.Llengua)
	assignString(&existing.Requeriments, desired.Requeriments)
	assignString(&existing.UnitatCatalogacio, desired.UnitatCatalogacio)
	assignString(&existing.UnitatInstalacio, desired.UnitatInstalacio)
	assignString(&existing.URLBase, desired.URLBase)
	assignString(&existing.URLImatgePrefix, desired.URLImatgePrefix)
	assignString(&existing.Pagina, desired.Pagina)
	if existing.IndexacioCompleta != desired.IndexacioCompleta {
		existing.IndexacioCompleta = desired.IndexacioCompleta
		updated = true
	}
	if existing.Pagines != desired.Pagines {
		existing.Pagines = desired.Pagines
		updated = true
	}
	return updated
}

func (a *App) applyLlibreImportV2ArchiveLinks(llibre *db.Llibre, archives []llibreImportResolvedArchiveV2, userID int) (int, int, error) {
	existing, err := a.currentLlibreArxiuLinks(llibre.ID)
	if err != nil {
		return 0, 0, err
	}
	currentByArchive := map[int]db.ArxiuLlibreLink{}
	for _, link := range existing {
		currentByArchive[link.ArxiuID] = link
	}
	desiredLinks := make([]db.ArxiuLlibreLink, 0, len(archives))
	created := 0
	skipped := 0
	for _, archive := range archives {
		current, exists := currentByArchive[archive.DB.ID]
		link := current
		if !exists {
			link = db.ArxiuLlibreLink{
				ArxiuID:   archive.DB.ID,
				LlibreID:  llibre.ID,
				CreatedBy: sqlNullIntFromInt(userID),
			}
			created++
		} else {
			skipped++
		}
		link.LlibreID = llibre.ID
		link.TipusRelacio = strings.TrimSpace(archive.Ref.RelationType)
		link.Signatura = strings.TrimSpace(archive.Ref.Signatura)
		link.URLOverride = strings.TrimSpace(archive.Ref.URLOverride)
		link.SourceSystem = strings.TrimSpace(archive.Ref.SourceSystem)
		link.ExternalID = strings.TrimSpace(archive.Ref.ExternalID)
		link.ExternalCode = strings.TrimSpace(archive.Ref.ExternalCode)
		link.Notes = strings.TrimSpace(archive.Ref.Notes)
		link.Principal = archive.Ref.Principal
		link.PreferitVisualitzacio = archive.Ref.PreferredDisplay
		link.Estat = "actiu"
		link.ModeracioEstat = llibre.ModeracioEstat
		link.UpdatedBy = sqlNullIntFromInt(userID)
		desiredLinks = append(desiredLinks, link)
	}
	merged := append(existing[:0:0], existing...)
	for _, link := range desiredLinks {
		merged = append(merged, link)
	}
	merged = normalizeLlibreArxiuLinks(merged)
	for i := range merged {
		if err := a.saveLlibreArxiuLink(&merged[i]); err != nil {
			return 0, 0, err
		}
	}
	return created, skipped, nil
}

func (a *App) applyLlibreImportV2URLs(llibreID int, urls []llibreImportURLRefV2, archives []llibreImportResolvedArchiveV2, userID int, diag *llibreImportDiagnosticsV2) (int, int, error) {
	if len(urls) == 0 {
		return 0, 0, nil
	}
	existing, err := a.DB.ListLlibreURLs(llibreID)
	if err != nil {
		return 0, 0, err
	}
	archiveCodeToID := map[string]int{}
	archiveNameToID := map[string]int{}
	for _, archive := range archives {
		if archive.Ref.ArchiveCode != "" {
			archiveCodeToID[normalizeKey(archive.Ref.ArchiveCode)] = archive.DB.ID
		}
		if archive.Ref.ArchiveName != "" {
			archiveNameToID[normalizeKey(archive.Ref.ArchiveName)] = archive.DB.ID
		}
		archiveCodeToID[normalizeKey(archive.DB.Codi)] = archive.DB.ID
		archiveNameToID[normalizeKey(archive.DB.Nom)] = archive.DB.ID
	}
	seen := map[string]struct{}{}
	for _, row := range existing {
		key := llibreImportURLKey(llibreID, row)
		seen[key] = struct{}{}
	}
	created := 0
	skipped := 0
	for _, link := range urls {
		if link.URL == "" || !(strings.HasPrefix(link.URL, "http://") || strings.HasPrefix(link.URL, "https://")) {
			diag.WarningsByReason["url_invalid"]++
			skipped++
			continue
		}
		arxiuID := sql.NullInt64{}
		if link.ArchiveCode != "" {
			if id, ok := archiveCodeToID[normalizeKey(link.ArchiveCode)]; ok {
				arxiuID = sql.NullInt64{Int64: int64(id), Valid: true}
			} else {
				diag.WarningsByReason["url_archive_not_found"]++
				skipped++
				continue
			}
		} else if link.ArchiveName != "" {
			if id, ok := archiveNameToID[normalizeKey(link.ArchiveName)]; ok {
				arxiuID = sql.NullInt64{Int64: int64(id), Valid: true}
			} else {
				diag.WarningsByReason["url_archive_not_found"]++
				skipped++
				continue
			}
		}
		row := db.LlibreURL{
			LlibreID:    llibreID,
			ArxiuID:     arxiuID,
			URL:         link.URL,
			Tipus:       sqlNullString(link.Type),
			Descripcio:  sqlNullString(link.Description),
			LlibreRefID: sql.NullInt64{},
			CreatedBy:   sqlNullIntFromInt(userID),
		}
		key := llibreImportURLKey(llibreID, row)
		if _, ok := seen[key]; ok {
			diag.WarningsByReason["url_duplicate"]++
			skipped++
			continue
		}
		if err := a.DB.AddLlibreURL(&row); err != nil {
			return created, skipped, err
		}
		seen[key] = struct{}{}
		created++
	}
	return created, skipped, nil
}

func llibreImportURLKey(llibreID int, row db.LlibreURL) string {
	arxiuID := 0
	if row.ArxiuID.Valid {
		arxiuID = int(row.ArxiuID.Int64)
	}
	typ := ""
	if row.Tipus.Valid {
		typ = strings.TrimSpace(row.Tipus.String)
	}
	return fmt.Sprintf("%d|%d|%s|%s", llibreID, arxiuID, strings.TrimSpace(row.URL), typ)
}

func (a *App) applyLlibreImportV2Pages(llibreID int, pages []llibreImportPageV2) (int, int, error) {
	created := 0
	skipped := 0
	for _, page := range pages {
		if page.PageNumber <= 0 {
			skipped++
			continue
		}
		existing, err := a.DB.GetLlibrePaginaByNum(llibreID, page.PageNumber)
		if err == nil && existing != nil {
			skipped++
			continue
		}
		entry := &db.LlibrePagina{
			LlibreID:  llibreID,
			NumPagina: page.PageNumber,
			Estat:     "pendent",
			Notes:     strings.TrimSpace(page.CanonicalLabel),
		}
		if _, err := a.DB.SaveLlibrePagina(entry); err != nil {
			return created, skipped, err
		}
		created++
	}
	return created, skipped, nil
}

func (a *App) llibreImportV2NeedsPublishedChange(existing *db.Llibre, record llibreImportRecordV2, archives []llibreImportResolvedArchiveV2) bool {
	if a.llibreImportV2NeedsPublishedBookFieldChange(existing, record) {
		return true
	}
	if changed, err := a.llibreImportV2NeedsPublishedArchiveLinkChange(existing, archives); err != nil || changed {
		return true
	}
	if changed, err := a.llibreImportV2NeedsPublishedURLChange(existing.ID, record.URLs, archives); err != nil || changed {
		return true
	}
	if changed, err := a.llibreImportV2NeedsPublishedPageChange(existing.ID, record.PageEntries); err != nil || changed {
		return true
	}
	return false
}

func (a *App) llibreImportV2NeedsPublishedBookFieldChange(existing *db.Llibre, record llibreImportRecordV2) bool {
	desired := buildLlibreImportV2Record(record, existing.MunicipiID, 0)
	return existing.Codi != desired.Codi ||
		existing.CodiDigital != desired.CodiDigital ||
		existing.CodiFisic != desired.CodiFisic ||
		existing.SourceSystem != desired.SourceSystem ||
		existing.ExternalID != desired.ExternalID ||
		existing.ExternalCode != desired.ExternalCode ||
		existing.Titol != desired.Titol ||
		existing.NomEsglesia != desired.NomEsglesia ||
		existing.TipusLlibre != desired.TipusLlibre ||
		existing.Cronologia != desired.Cronologia ||
		existing.Volum != desired.Volum ||
		existing.Abat != desired.Abat ||
		existing.Contingut != desired.Contingut ||
		existing.Llengua != desired.Llengua ||
		existing.Requeriments != desired.Requeriments ||
		existing.UnitatCatalogacio != desired.UnitatCatalogacio ||
		existing.UnitatInstalacio != desired.UnitatInstalacio ||
		existing.Pagines != desired.Pagines ||
		existing.URLBase != desired.URLBase ||
		existing.URLImatgePrefix != desired.URLImatgePrefix ||
		existing.Pagina != desired.Pagina ||
		existing.IndexacioCompleta != desired.IndexacioCompleta
}

func (a *App) llibreImportV2NeedsPublishedArchiveLinkChange(existing *db.Llibre, archives []llibreImportResolvedArchiveV2) (bool, error) {
	current, err := a.currentLlibreArxiuLinks(existing.ID)
	if err != nil {
		return false, err
	}
	desired, err := a.buildDesiredLlibreImportV2ArchiveLinks(existing, archives, 0)
	if err != nil {
		return false, err
	}
	return !llibreImportV2ArchiveLinksEqual(current, desired), nil
}

func (a *App) buildDesiredLlibreImportV2ArchiveLinks(llibre *db.Llibre, archives []llibreImportResolvedArchiveV2, userID int) ([]db.ArxiuLlibreLink, error) {
	existing, err := a.currentLlibreArxiuLinks(llibre.ID)
	if err != nil {
		return nil, err
	}
	currentByArchive := map[int]db.ArxiuLlibreLink{}
	for _, link := range existing {
		currentByArchive[link.ArxiuID] = link
	}
	desiredLinks := make([]db.ArxiuLlibreLink, 0, len(archives))
	for _, archive := range archives {
		current, exists := currentByArchive[archive.DB.ID]
		link := current
		if !exists {
			link = db.ArxiuLlibreLink{
				ArxiuID:  archive.DB.ID,
				LlibreID: llibre.ID,
			}
			if userID > 0 {
				link.CreatedBy = sqlNullIntFromInt(userID)
			}
		}
		link.LlibreID = llibre.ID
		link.TipusRelacio = strings.TrimSpace(archive.Ref.RelationType)
		link.Signatura = strings.TrimSpace(archive.Ref.Signatura)
		link.URLOverride = strings.TrimSpace(archive.Ref.URLOverride)
		link.SourceSystem = strings.TrimSpace(archive.Ref.SourceSystem)
		link.ExternalID = strings.TrimSpace(archive.Ref.ExternalID)
		link.ExternalCode = strings.TrimSpace(archive.Ref.ExternalCode)
		link.Notes = strings.TrimSpace(archive.Ref.Notes)
		link.Principal = archive.Ref.Principal
		link.PreferitVisualitzacio = archive.Ref.PreferredDisplay
		link.Estat = "actiu"
		link.ModeracioEstat = llibre.ModeracioEstat
		if userID > 0 {
			link.UpdatedBy = sqlNullIntFromInt(userID)
		}
		desiredLinks = append(desiredLinks, link)
	}
	merged := append(existing[:0:0], existing...)
	for _, link := range desiredLinks {
		merged = append(merged, link)
	}
	return normalizeLlibreArxiuLinks(merged), nil
}

func llibreImportV2ArchiveLinksEqual(aLinks, bLinks []db.ArxiuLlibreLink) bool {
	left := normalizeLlibreArxiuLinks(append([]db.ArxiuLlibreLink(nil), aLinks...))
	right := normalizeLlibreArxiuLinks(append([]db.ArxiuLlibreLink(nil), bLinks...))
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !llibreImportV2ArchiveLinkEqual(left[i], right[i]) {
			return false
		}
	}
	return true
}

func llibreImportV2ArchiveLinkEqual(a, b db.ArxiuLlibreLink) bool {
	return a.ArxiuID == b.ArxiuID &&
		a.LlibreID == b.LlibreID &&
		strings.TrimSpace(a.TipusRelacio) == strings.TrimSpace(b.TipusRelacio) &&
		a.Principal == b.Principal &&
		a.PreferitVisualitzacio == b.PreferitVisualitzacio &&
		strings.TrimSpace(a.Signatura) == strings.TrimSpace(b.Signatura) &&
		strings.TrimSpace(a.URLOverride) == strings.TrimSpace(b.URLOverride) &&
		strings.TrimSpace(a.SourceSystem) == strings.TrimSpace(b.SourceSystem) &&
		strings.TrimSpace(a.ExternalID) == strings.TrimSpace(b.ExternalID) &&
		strings.TrimSpace(a.ExternalCode) == strings.TrimSpace(b.ExternalCode) &&
		strings.TrimSpace(a.Notes) == strings.TrimSpace(b.Notes) &&
		strings.TrimSpace(a.Estat) == strings.TrimSpace(b.Estat) &&
		strings.TrimSpace(a.ModeracioEstat) == strings.TrimSpace(b.ModeracioEstat)
}

func (a *App) llibreImportV2NeedsPublishedURLChange(llibreID int, urls []llibreImportURLRefV2, archives []llibreImportResolvedArchiveV2) (bool, error) {
	if len(urls) == 0 {
		return false, nil
	}
	existing, err := a.DB.ListLlibreURLs(llibreID)
	if err != nil {
		return false, err
	}
	archiveCodeToID := map[string]int{}
	archiveNameToID := map[string]int{}
	for _, archive := range archives {
		if archive.Ref.ArchiveCode != "" {
			archiveCodeToID[normalizeKey(archive.Ref.ArchiveCode)] = archive.DB.ID
		}
		if archive.Ref.ArchiveName != "" {
			archiveNameToID[normalizeKey(archive.Ref.ArchiveName)] = archive.DB.ID
		}
		archiveCodeToID[normalizeKey(archive.DB.Codi)] = archive.DB.ID
		archiveNameToID[normalizeKey(archive.DB.Nom)] = archive.DB.ID
	}
	seen := map[string]struct{}{}
	for _, row := range existing {
		seen[llibreImportURLKey(llibreID, row)] = struct{}{}
	}
	for _, link := range urls {
		if link.URL == "" || !(strings.HasPrefix(link.URL, "http://") || strings.HasPrefix(link.URL, "https://")) {
			continue
		}
		arxiuID := sql.NullInt64{}
		if link.ArchiveCode != "" {
			id, ok := archiveCodeToID[normalizeKey(link.ArchiveCode)]
			if !ok {
				continue
			}
			arxiuID = sql.NullInt64{Int64: int64(id), Valid: true}
		} else if link.ArchiveName != "" {
			id, ok := archiveNameToID[normalizeKey(link.ArchiveName)]
			if !ok {
				continue
			}
			arxiuID = sql.NullInt64{Int64: int64(id), Valid: true}
		}
		row := db.LlibreURL{
			LlibreID:    llibreID,
			ArxiuID:     arxiuID,
			URL:         strings.TrimSpace(link.URL),
			Tipus:       sqlNullString(link.Type),
			Descripcio:  sqlNullString(link.Description),
			LlibreRefID: sql.NullInt64{},
		}
		if _, ok := seen[llibreImportURLKey(llibreID, row)]; !ok {
			return true, nil
		}
	}
	return false, nil
}

func (a *App) llibreImportV2NeedsPublishedPageChange(llibreID int, pages []llibreImportPageV2) (bool, error) {
	for _, page := range pages {
		if page.PageNumber <= 0 {
			continue
		}
		existing, err := a.DB.GetLlibrePaginaByNum(llibreID, page.PageNumber)
		if err != nil {
			return false, err
		}
		if existing == nil {
			return true, nil
		}
	}
	return false, nil
}

func buildLlibresImportV2ErrorDetail(record llibreImportRecordV2, reason string) string {
	label := strings.TrimSpace(record.Code)
	if label == "" {
		label = strings.TrimSpace(record.Title)
	}
	if label == "" {
		label = strings.TrimSpace(record.ChurchName)
	}
	detail := reason + ":" + label
	switch reason {
	case "archive_not_found", "archive_ambiguous", "archive_code_missing", "archive_missing":
		parts := make([]string, 0, len(record.Archives))
		for _, archive := range record.Archives {
			archiveLabel := strings.TrimSpace(archive.ArchiveCode)
			if archiveLabel == "" {
				archiveLabel = strings.TrimSpace(archive.ArchiveName)
			}
			if archiveLabel != "" {
				parts = append(parts, archiveLabel)
			}
		}
		if len(parts) > 0 {
			detail += " archives=" + strings.Join(parts, ",")
		}
	case "municipality_missing", "municipality_not_found", "municipality_ambiguous":
		if name := strings.TrimSpace(record.Municipality.Name); name != "" {
			detail += " municipality=" + name
			if iso2 := strings.TrimSpace(record.Municipality.CountryISO2); iso2 != "" {
				detail += "/" + iso2
			}
		}
	}
	return detail
}

func mapKeysInt(values map[int]struct{}) []int {
	out := make([]int, 0, len(values))
	for id := range values {
		out = append(out, id)
	}
	return dedupeIntSlice(out)
}
