package core

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type templateImportModel struct {
	RecordType          string
	PresetCode          string
	BookMode            string
	BookColumn          string
	CronologiaNormalize bool
	AmbiguityPolicy     string
	ScopeFilters        bool
	ContextFilters      []string
	BaseDefaults        map[string]string
	Mapping             []templateColumn
	Policies            templatePolicies
	NameOrder           string
	DateFormat          string
	Quality             templateQualityConfig
}

type templatePolicies struct {
	ModerationStatus                     string
	DedupWithin                          bool
	DedupKeyFields                       []string
	DedupAddRowIndexWhenPrincipalMissing bool
	MergeMode                            string
	PrincipalRoles                       []string
	UpdateMissingOnly                    bool
	AddMissingPeople                     bool
	AddMissingAttrs                      bool
	AvoidDuplicatePrincipal              bool
}

type templateColumn struct {
	Header    string
	Aliases   []string
	Required  bool
	Key       string
	MapTo     []templateMapTo
	Condition *templateCondition
	Index     int
	KeyNorm   string
}

type templateCondition struct {
	Expr string
	Then templateBranch
	Else *templateBranch
}

type templateBranch struct {
	MapTo      []templateMapTo
	Transforms []templateTransform
}

type templateMapTo struct {
	Target     string
	Transforms []templateTransform
	Condition  *templateInlineCondition
}

type templateInlineCondition struct {
	Op   string
	Args map[string]interface{}
}

type templateTransform struct {
	Name         string
	Value        string
	Args         map[string]interface{}
	Kind         string
	DefaultValue string
	SelectRight  bool
	Regex        *regexp.Regexp
	RegexGroup   int
	MapValues    map[string]string
}

type templateRowContext struct {
	plan   *templateRowContextPlan
	values []string
}

type templateRowContextPlan struct {
	HeaderRefs map[string]int
	ColumnRefs map[string]int
}

const templateImportCreateBatchSize = 500

type templatePendingCreate struct {
	RowNum int
	BookID int
	Bundle db.TranscripcioRawImportBundle
}

type templateMatchBuildCache struct {
	normalizedParts map[string]string
	loweredParts    map[string]string
	personKeys      map[string]string
}

func (ctx templateRowContext) HeaderValue(key string) string {
	value, _ := ctx.LookupHeaderValue(key)
	return value
}

func (ctx templateRowContext) ColumnValue(key string) string {
	value, _ := ctx.LookupColumnValue(key)
	return value
}

func (ctx templateRowContext) LookupHeaderValue(key string) (string, bool) {
	if ctx.plan == nil {
		return "", false
	}
	idx, ok := ctx.plan.HeaderRefs[key]
	if !ok || idx < 0 || idx >= len(ctx.values) {
		return "", false
	}
	return ctx.values[idx], true
}

func (ctx templateRowContext) LookupColumnValue(key string) (string, bool) {
	if ctx.plan == nil {
		return "", false
	}
	idx, ok := ctx.plan.ColumnRefs[key]
	if !ok || idx < 0 || idx >= len(ctx.values) {
		return "", false
	}
	return ctx.values[idx], true
}

func (a *App) RunCSVTemplateImport(template *db.CSVImportTemplate, reader io.Reader, sep rune, userID int, ctx importContext, fixedBookID int) csvImportResult {
	start := time.Now()
	result := csvImportResult{
		Debug: newCSVImportDebugMetrics("template", "global"),
	}
	if fixedBookID > 0 {
		result.Debug.Scope = "book"
	}
	if template == nil {
		result.Failed = 1
		result.Errors = append(result.Errors, importErrorEntry{Row: 0, Reason: "plantilla invàlida"})
		result.Debug.finalize(len(result.BookIDs), time.Since(start))
		return result
	}
	if sep == 0 {
		sep = ','
	}
	parseStart := time.Now()
	model, err := parseTemplateImportModel(template.ModelJSON)
	parseElapsed := time.Since(parseStart)
	result.Debug.addParseModel(parseElapsed)
	result.Debug.addParse(parseElapsed)
	if err != nil {
		result.Failed = 1
		result.Errors = append(result.Errors, importErrorEntry{Row: 0, Reason: "model de plantilla invàlid"})
		result.Debug.finalize(len(result.BookIDs), time.Since(start))
		return result
	}
	parseStart = time.Now()
	if err := validateTemplateImportModel(model); err != nil {
		parseElapsed = time.Since(parseStart)
		result.Debug.addParseValidation(parseElapsed)
		result.Debug.addParse(parseElapsed)
		result.Failed = 1
		result.Errors = append(result.Errors, importErrorEntry{Row: 0, Reason: err.Error()})
		result.Debug.finalize(len(result.BookIDs), time.Since(start))
		return result
	}
	parseElapsed = time.Since(parseStart)
	result.Debug.addParseValidation(parseElapsed)
	result.Debug.addParse(parseElapsed)
	compileTemplateImportModel(model)
	if debugModel := templateImportDebugModel(model); debugModel != "" {
		result.Debug.Model = debugModel
	}
	parseCfg := buildTemplateParseConfig(model)
	parseCfg.Metrics = &result.Debug

	csvReader := csv.NewReader(reader)
	csvReader.Comma = sep
	csvReader.TrimLeadingSpace = true
	parseStart = time.Now()
	headers, err := csvReader.Read()
	parseElapsed = time.Since(parseStart)
	result.Debug.addParseHeaderRead(parseElapsed)
	result.Debug.addParse(parseElapsed)
	if err != nil {
		result.Failed = 1
		result.Errors = append(result.Errors, importErrorEntry{Row: 0, Reason: "capçalera CSV invàlida"})
		result.Debug.finalize(len(result.BookIDs), time.Since(start))
		return result
	}

	headerIndex := map[string]int{}
	parseStart = time.Now()
	for i, h := range headers {
		headerIndex[normalizeCSVHeader(h)] = i
	}
	if model.PresetCode == "generic_v1" && len(model.Mapping) == 0 {
		model.Mapping = buildGenericTemplateColumns(headers)
	}
	for i := range model.Mapping {
		model.Mapping[i].Index = resolveTemplateColumnIndex(model.Mapping[i], headerIndex)
		model.Mapping[i].KeyNorm = normalizeCSVHeader(firstNonEmpty(model.Mapping[i].Key, model.Mapping[i].Header))
		if model.Mapping[i].Required && model.Mapping[i].Index == -1 {
			result.Failed = 1
			result.Errors = append(result.Errors, importErrorEntry{Row: 0, Reason: "falta la columna " + model.Mapping[i].Header})
			result.Debug.addParse(time.Since(parseStart))
			result.Debug.finalize(len(result.BookIDs), time.Since(start))
			return result
		}
	}
	parseElapsed = time.Since(parseStart)
	result.Debug.addParseHeaderPrepare(parseElapsed)
	result.Debug.addParse(parseElapsed)
	rowContextPlan := buildTemplateRowContextPlan(model.Mapping, headerIndex)

	resolveStart := time.Now()
	bookInfoByKey, bookInfoByID := a.prepareBookLookups(model, ctx, fixedBookID)
	result.Debug.addResolve(time.Since(resolveStart))
	if fixedBookID > 0 {
		if _, ok := bookInfoByID[fixedBookID]; !ok {
			result.Failed = 1
			result.Errors = append(result.Errors, importErrorEntry{Row: 0, Reason: "llibre no trobat"})
			result.Debug.finalize(len(result.BookIDs), time.Since(start))
			return result
		}
	}

	seen := map[string]int{}
	seenMatchByContext := map[string]map[string]int{}
	existingByContext := map[string]map[string]int{}
	pendingCreates := make([]templatePendingCreate, 0, templateImportCreateBatchSize)
	importRuntime := db.TemplateImportRuntimeFor(a.DB)
	pageResolver := importRuntime.NewPageResolver()
	matchBuildCache := newTemplateMatchBuildCache()
	existingSnapshotMaxID := 0
	if maxID, err := importRuntime.ExistingSnapshotMaxID(); err == nil && maxID > 0 {
		existingSnapshotMaxID = maxID
	}
	flushPendingCreates := func() {
		pendingCreates = a.flushTemplatePendingCreates(pendingCreates, &result, importRuntime)
	}
	rowNum := 1
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		rowNum++
		if err != nil {
			result.Failed++
			result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: "error llegint fila"})
			continue
		}
		result.Debug.incRows()
		parseStart = time.Now()
		rowCtx := buildTemplateRowContext(rowContextPlan, record)
		parseElapsed = time.Since(parseStart)
		result.Debug.addParseRowContext(parseElapsed)
		result.Debug.addParse(parseElapsed)
		resolveStart = time.Now()
		bookID, bookInfo, bookErr := resolveTemplateBookID(model, rowCtx, bookInfoByKey, bookInfoByID, fixedBookID)
		result.Debug.addResolve(time.Since(resolveStart))
		if bookErr != "" {
			result.Failed++
			result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: bookErr})
			continue
		}

		t := db.TranscripcioRaw{
			LlibreID:       bookID,
			ModeracioEstat: pickTemplateModerationStatus(model),
			CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
		}
		applyBaseDefaults(&t, model.BaseDefaults)
		persones := map[string]*db.TranscripcioPersonaRaw{}
		atributs := map[string]*db.TranscripcioAtributRaw{}
		mappedValues := map[string]string{}

		parseStart = time.Now()
		for _, col := range model.Mapping {
			if col.Index < 0 || col.Index >= len(rowCtx.values) {
				continue
			}
			rawVal := rowCtx.values[col.Index]
			if rawVal == "" && !columnHasDefault(col) {
				continue
			}
			applyTemplateColumn(col, rawVal, rowCtx, &t, persones, atributs, mappedValues, parseCfg)
		}
		parseElapsed = time.Since(parseStart)
		result.Debug.addParseColumns(parseElapsed)
		result.Debug.addParse(parseElapsed)

		if model.Policies.DedupWithin && len(model.Policies.DedupKeyFields) > 0 {
			duplicateStart := time.Now()
			key := buildTemplateDedupKeyWithCache(matchBuildCache, model.Policies.DedupKeyFields, rowCtx, mappedValues)
			if key != "" {
				if model.Policies.DedupAddRowIndexWhenPrincipalMissing && !principalPersonHasName(persones, model.Policies.PrincipalRoles) {
					key += "|row:" + strconv.Itoa(rowNum)
				}
				if firstRow, ok := seen[key]; ok {
					result.Debug.addWriteDuplicateCheck(time.Since(duplicateStart))
					result.Failed++
					fields := map[string]string{"duplicate_row": strconv.Itoa(firstRow)}
					result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: "registre duplicat", Fields: fields})
					continue
				}
				seen[key] = rowNum
			}
			result.Debug.addWriteDuplicateCheck(time.Since(duplicateStart))
		}

		matchKey := ""
		matchContextKey := ""
		matchMode := model.Policies.MergeMode
		switch matchMode {
		case "by_strong_signature_if_page_indexed":
			pageLookupStart := time.Now()
			pageKey, pageIndexed := a.templateIndexedPageKeyWithResolver(pageResolver, bookID, &t, atributs)
			result.Debug.addWritePageLookup(time.Since(pageLookupStart))
			if pageIndexed {
				duplicateStart := time.Now()
				matchKey = buildTemplateStrongMatchKeyWithCache(matchBuildCache, &t, persones, atributs, model.Policies)
				if matchKey != "" {
					matchContextKey = "strong|" + strconv.Itoa(bookID) + "|" + normalizeTemplateMatchPartWithCache(matchBuildCache, pageKey) + "|" + normalizeTemplateMatchPartWithCache(matchBuildCache, t.TipusActe)
					if model.Policies.AvoidDuplicatePrincipal {
						if firstRow, ok := templateSeenMatchRow(seenMatchByContext, matchContextKey, matchKey); ok {
							result.Debug.addWriteDuplicateCheck(time.Since(duplicateStart))
							result.Failed++
							fields := map[string]string{"duplicate_row": strconv.Itoa(firstRow)}
							result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: "registre duplicat", Fields: fields})
							continue
						}
					}
				}
				result.Debug.addWriteDuplicateCheck(time.Since(duplicateStart))
			}
		case "by_principal_person_if_book_indexed":
			if !bookInfo.Indexed {
				break
			}
			duplicateStart := time.Now()
			matchKey = principalPersonKey(persones, model.Policies.PrincipalRoles)
			if matchKey != "" && model.Policies.AvoidDuplicatePrincipal {
				matchContextKey = "principal|" + strconv.Itoa(bookID)
				if firstRow, ok := templateSeenMatchRow(seenMatchByContext, matchContextKey, matchKey); ok {
					result.Debug.addWriteDuplicateCheck(time.Since(duplicateStart))
					result.Failed++
					fields := map[string]string{"duplicate_row": strconv.Itoa(firstRow)}
					result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: "registre duplicat", Fields: fields})
					continue
				}
			}
			if matchKey != "" && matchContextKey == "" {
				matchContextKey = "principal|" + strconv.Itoa(bookID)
			}
			result.Debug.addWriteDuplicateCheck(time.Since(duplicateStart))
		}

		if matchKey != "" && matchContextKey != "" {
			existingMap := existingByContext[matchContextKey]
			if existingMap == nil {
				resolveStart = time.Now()
				if matchMode == "by_strong_signature_if_page_indexed" {
					existingMap = a.loadExistingByStrongMatchWithPageResolverSnapshot(importRuntime, pageResolver, bookID, &t, atributs, model.Policies, existingSnapshotMaxID)
				} else {
					existingMap = a.loadExistingByPrincipal(importRuntime, bookID, model.Policies.PrincipalRoles, existingSnapshotMaxID)
				}
				result.Debug.addResolve(time.Since(resolveStart))
				existingByContext[matchContextKey] = existingMap
			}
			if existingID, ok := existingMap[matchKey]; ok {
				writeStart := time.Now()
				updated, okUpdate := a.mergeTemplateRow(existingID, &t, persones, atributs, model.Policies)
				result.Debug.addWrite(time.Since(writeStart))
				if okUpdate {
					result.Updated++
					result.markBook(bookID)
					if matchContextKey != "" && matchKey != "" {
						templateRememberSeenMatch(seenMatchByContext, matchContextKey, matchKey, rowNum)
					}
					if updated {
						existingMap[matchKey] = existingID
					}
					continue
				}
			}
		}

		writePrepareStart := time.Now()
		t.DataActeEstat = normalizeDataActeEstat(t.DataActeEstat)
		if t.DataActeEstat == "" {
			t.DataActeEstat = "clar"
		}
		if !validTipusActe(t.TipusActe) {
			result.Debug.addWritePrepare(time.Since(writePrepareStart))
			result.Failed++
			result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: "tipus_acte invàlid"})
			continue
		}
		result.Debug.addWritePrepare(time.Since(writePrepareStart))
		personaResolveStart := time.Now()
		personesRows := make([]db.TranscripcioPersonaRaw, 0, len(persones))
		for _, p := range persones {
			if isEmptyPerson(p) {
				continue
			}
			personesRows = append(personesRows, *p)
		}
		atributRows := make([]db.TranscripcioAtributRaw, 0, len(atributs))
		for _, attr := range atributs {
			if isEmptyAttr(attr) {
				continue
			}
			atributRows = append(atributRows, *attr)
		}
		result.Debug.addWritePersonaResolve(time.Since(personaResolveStart))
		pendingCreates = append(pendingCreates, templatePendingCreate{
			RowNum: rowNum,
			BookID: bookID,
			Bundle: db.TranscripcioRawImportBundle{
				Transcripcio: t,
				Persones:     personesRows,
				Atributs:     atributRows,
			},
		})
		if matchContextKey != "" && matchKey != "" {
			templateRememberSeenMatch(seenMatchByContext, matchContextKey, matchKey, rowNum)
		}
		if len(pendingCreates) >= templateImportCreateBatchSize {
			flushPendingCreates()
		}
	}
	flushPendingCreates()
	result.Debug.finalize(len(result.BookIDs), time.Since(start))
	return result
}

func (a *App) flushTemplatePendingCreates(pending []templatePendingCreate, result *csvImportResult, runtime db.TemplateImportRuntime) []templatePendingCreate {
	if len(pending) == 0 || result == nil {
		return pending[:0]
	}
	if runtime != nil {
		bundles := make([]db.TranscripcioRawImportBundle, len(pending))
		for i := range pending {
			bundles[i] = pending[i].Bundle
		}
		bulkResult, err := runtime.BulkCreateBundles(bundles)
		if err == nil && len(bulkResult.IDs) == len(pending) {
			result.Debug.addWriteBulkBatch(len(pending))
			result.Debug.addWriteBulkStatementBatches(
				bulkResult.Metrics.TranscripcioBatches,
				bulkResult.Metrics.PersonaBatches,
				bulkResult.Metrics.AtributBatches,
			)
			result.Debug.addWriteTranscripcioInsert(bulkResult.Metrics.TranscripcioInsertDur)
			result.Debug.addWritePersonaPersist(bulkResult.Metrics.PersonaPersistDur)
			result.Debug.addWriteLinksPersist(bulkResult.Metrics.LinksPersistDur)
			result.Debug.addWriteCommit(bulkResult.Metrics.CommitDur)
			for i := range pending {
				result.Created++
				result.markBook(pending[i].BookID)
			}
			return pending[:0]
		}
		result.Debug.addWriteBulkFallback()
	}
	for i := range pending {
		a.createTemplatePendingRow(pending[i], result)
	}
	return pending[:0]
}

func (a *App) createTemplatePendingRow(row templatePendingCreate, result *csvImportResult) {
	if result == nil {
		return
	}
	runtime := db.TemplateImportRuntimeFor(a.DB)
	createResult, err := runtime.CreateBundle(row.Bundle)
	result.Debug.addWriteTranscripcioInsert(createResult.Metrics.TranscripcioInsertDur)
	result.Debug.addWritePersonaPersist(createResult.Metrics.PersonaPersistDur)
	result.Debug.addWriteLinksPersist(createResult.Metrics.LinksPersistDur)
	if err != nil || len(createResult.IDs) == 0 || createResult.IDs[0] == 0 {
		result.Failed++
		reason := "no s'ha pogut crear el registre"
		if err != nil {
			reason = fmt.Sprintf("no s'ha pogut crear el registre: %v", err)
		}
		result.Errors = append(result.Errors, importErrorEntry{Row: row.RowNum, Reason: reason})
		return
	}
	result.Created++
	result.markBook(row.BookID)
}

func parseTemplateImportModel(modelJSON string) (*templateImportModel, error) {
	modelJSON = strings.TrimSpace(modelJSON)
	if modelJSON == "" {
		return nil, fmt.Errorf("model empty")
	}
	var root map[string]interface{}
	if err := json.Unmarshal([]byte(modelJSON), &root); err != nil {
		return nil, err
	}
	model := &templateImportModel{
		BookMode:        "llibre_id",
		BookColumn:      "llibre_id",
		AmbiguityPolicy: "fail",
		BaseDefaults:    map[string]string{},
		Policies: templatePolicies{
			ModerationStatus:  "pendent",
			UpdateMissingOnly: true,
			AddMissingPeople:  true,
			AddMissingAttrs:   true,
		},
	}
	if meta, ok := root["metadata"].(map[string]interface{}); ok {
		if v := asString(meta["record_type"]); v != "" {
			model.RecordType = v
		}
		if v := asString(meta["preset_code"]); v != "" {
			model.PresetCode = v
		}
	}
	if v := asString(root["record_type"]); v != "" {
		model.RecordType = v
	}
	if v := asString(root["preset_code"]); v != "" {
		model.PresetCode = v
	}
	if v := asString(root["name_order"]); v != "" {
		model.NameOrder = v
	}
	if v := asString(root["date_format"]); v != "" {
		model.DateFormat = v
	}
	if quality, ok := root["quality"].(map[string]interface{}); ok {
		model.Quality.Labels = asBool(quality["labels"])
		if markers, ok := quality["markers"].(map[string]interface{}); ok {
			model.Quality.Markers = map[string]string{}
			for key, val := range markers {
				model.Quality.Markers[key] = strings.TrimSpace(asString(val))
			}
		}
	}
	if book, ok := root["book_resolution"].(map[string]interface{}); ok {
		if v := asString(book["mode"]); v != "" {
			model.BookMode = v
		}
		if v := asString(book["column"]); v != "" {
			model.BookColumn = v
		}
		if v := asBool(book["cronologia_normalize"]); v {
			model.CronologiaNormalize = v
		}
		if v := asBool(book["normalize_cronologia"]); v {
			model.CronologiaNormalize = v
		}
		if v := asString(book["ambiguity_policy"]); v != "" {
			model.AmbiguityPolicy = v
		}
		if v := asBool(book["scope_filters"]); v {
			model.ScopeFilters = v
		}
		if v := asStringSlice(book["context_filters"]); len(v) > 0 {
			model.ContextFilters = v
		}
	}
	if defaults, ok := root["base_defaults"].(map[string]interface{}); ok {
		for key, val := range defaults {
			model.BaseDefaults[key] = asString(val)
		}
	}
	if mapping, ok := root["mapping"].(map[string]interface{}); ok {
		if cols, ok := mapping["columns"].([]interface{}); ok {
			model.Mapping = parseTemplateColumns(cols)
		}
	}
	if policies, ok := root["policies"].(map[string]interface{}); ok {
		model.Policies.ModerationStatus = firstNonEmpty(asString(policies["moderation_status"]), model.Policies.ModerationStatus)
		if dedup, ok := policies["dedup"].(map[string]interface{}); ok {
			model.Policies.DedupWithin = asBool(dedup["within_file"])
			model.Policies.DedupKeyFields = append(model.Policies.DedupKeyFields, asStringSlice(dedup["key_fields"])...)
			model.Policies.DedupKeyFields = append(model.Policies.DedupKeyFields, asStringSlice(dedup["key_columns"])...)
			model.Policies.DedupAddRowIndexWhenPrincipalMissing = asBool(dedup["if_principal_name_missing_add_row_index"])
		}
		if merge, ok := policies["merge_existing"].(map[string]interface{}); ok {
			model.Policies.MergeMode = asString(merge["mode"])
			model.Policies.PrincipalRoles = asStringSlice(merge["principal_roles"])
			if v, ok := merge["update_missing_only"]; ok {
				model.Policies.UpdateMissingOnly = asBool(v)
			}
			if v, ok := merge["add_missing_people"]; ok {
				model.Policies.AddMissingPeople = asBool(v)
			}
			if v, ok := merge["add_missing_attrs"]; ok {
				model.Policies.AddMissingAttrs = asBool(v)
			}
			if v, ok := merge["avoid_duplicate_rows_by_principal_name_per_book"]; ok {
				model.Policies.AvoidDuplicatePrincipal = asBool(v)
			}
		}
	}
	return model, nil
}

func parseTemplateColumns(raw []interface{}) []templateColumn {
	cols := make([]templateColumn, 0, len(raw))
	for _, item := range raw {
		colMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		col := templateColumn{
			Header:   asString(colMap["header"]),
			Aliases:  asStringSlice(colMap["aliases"]),
			Required: asBool(colMap["required"]),
			Key:      asString(colMap["key"]),
			MapTo:    parseTemplateMapTo(colMap["map_to"]),
			Index:    -1,
		}
		if cond, ok := colMap["condition"].(map[string]interface{}); ok {
			col.Condition = parseTemplateCondition(cond)
		}
		cols = append(cols, col)
	}
	return cols
}

func buildGenericTemplateColumns(headers []string) []templateColumn {
	cols := make([]templateColumn, 0, len(headers))
	for _, header := range headers {
		col := parseCSVHeader(header)
		target := ""
		switch col.Kind {
		case "base":
			target = "base." + col.Field
		case "person":
			target = "person." + col.Role + "." + col.Field
		case "attr":
			attrType := strings.TrimSpace(col.AttrType)
			if attrType == "" {
				attrType = "text"
			}
			target = "attr." + col.AttrKey + "." + attrType
		}
		if target == "" {
			continue
		}
		cols = append(cols, templateColumn{
			Header: strings.TrimSpace(header),
			Key:    strings.TrimSpace(header),
			MapTo:  []templateMapTo{{Target: target}},
			Index:  -1,
		})
	}
	return cols
}

func parseTemplateCondition(cond map[string]interface{}) *templateCondition {
	if cond == nil {
		return nil
	}
	thenMap := fromMap(cond["then"])
	elseMap := fromMap(cond["else"])
	out := &templateCondition{
		Expr: asString(cond["expr"]),
		Then: templateBranch{
			MapTo:      parseTemplateMapTo(thenMap["map_to"]),
			Transforms: parseTemplateTransforms(thenMap["transforms"]),
		},
	}
	if elseMap != nil {
		branch := templateBranch{
			MapTo:      parseTemplateMapTo(elseMap["map_to"]),
			Transforms: parseTemplateTransforms(elseMap["transforms"]),
		}
		out.Else = &branch
	}
	return out
}

func parseTemplateMapTo(raw interface{}) []templateMapTo {
	list, ok := raw.([]interface{})
	if !ok {
		return nil
	}
	res := make([]templateMapTo, 0, len(list))
	for _, item := range list {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		entry := templateMapTo{
			Target:     asString(m["target"]),
			Transforms: parseTemplateTransforms(m["transforms"]),
		}
		if len(entry.Transforms) == 0 {
			entry.Transforms = parseTemplateTransforms(m["transform"])
		}
		if cond, ok := m["condition"].(map[string]interface{}); ok {
			entry.Condition = &templateInlineCondition{
				Op:   asString(cond["op"]),
				Args: asMap(cond["args"]),
			}
		}
		res = append(res, entry)
	}
	return res
}

func parseTemplateTransforms(raw interface{}) []templateTransform {
	list, ok := raw.([]interface{})
	if !ok {
		return nil
	}
	out := make([]templateTransform, 0, len(list))
	for _, item := range list {
		switch t := item.(type) {
		case string:
			out = append(out, templateTransform{Name: t})
		case map[string]interface{}:
			name := firstNonEmpty(asString(t["name"]), asString(t["op"]))
			tr := templateTransform{
				Name:  name,
				Value: firstNonEmpty(asString(t["value"]), asString(t["arg"])),
				Args:  asMap(t["args"]),
			}
			out = append(out, tr)
		}
	}
	return out
}

func compileTemplateTransforms(transforms []templateTransform) {
	for i := range transforms {
		kind := strings.TrimSpace(strings.ToLower(transforms[i].Name))
		transforms[i].Kind = kind
		switch kind {
		case "set_default":
			transforms[i].DefaultValue = firstNonEmpty(transforms[i].Value, asString(transforms[i].Args["value"]))
		case "map_values":
			transforms[i].MapValues = asMapString(transforms[i].Args)
		case "regex_extract":
			pattern := asString(transforms[i].Args["pattern"])
			transforms[i].RegexGroup = intFromInterface(transforms[i].Args["group"], 1)
			if pattern != "" {
				if re, err := regexp.Compile(pattern); err == nil {
					transforms[i].Regex = re
				}
			}
		case "split_couple_i":
			selectSide := strings.ToLower(firstNonEmpty(asString(transforms[i].Args["select"]), transforms[i].Value))
			transforms[i].SelectRight = selectSide == "right"
		}
	}
}

func compileTemplateImportModel(model *templateImportModel) {
	if model == nil {
		return
	}
	for i := range model.Mapping {
		compileTemplateTransformsInColumn(&model.Mapping[i])
	}
}

func compileTemplateTransformsInColumn(col *templateColumn) {
	if col == nil {
		return
	}
	for i := range col.MapTo {
		compileTemplateTransforms(col.MapTo[i].Transforms)
	}
	if col.Condition != nil {
		compileTemplateTransforms(col.Condition.Then.Transforms)
		for i := range col.Condition.Then.MapTo {
			compileTemplateTransforms(col.Condition.Then.MapTo[i].Transforms)
		}
		if col.Condition.Else != nil {
			compileTemplateTransforms(col.Condition.Else.Transforms)
			for i := range col.Condition.Else.MapTo {
				compileTemplateTransforms(col.Condition.Else.MapTo[i].Transforms)
			}
		}
	}
}

func resolveTemplateColumnIndex(col templateColumn, headers map[string]int) int {
	if idx, ok := headers[normalizeCSVHeader(col.Header)]; ok {
		return idx
	}
	for _, alias := range col.Aliases {
		if idx, ok := headers[normalizeCSVHeader(alias)]; ok {
			return idx
		}
	}
	return -1
}

func buildTemplateRowContextPlan(cols []templateColumn, headers map[string]int) *templateRowContextPlan {
	plan := &templateRowContextPlan{
		HeaderRefs: make(map[string]int, len(headers)),
		ColumnRefs: make(map[string]int, len(cols)),
	}
	for key, idx := range headers {
		plan.HeaderRefs[key] = idx
	}
	for _, col := range cols {
		if col.Index < 0 {
			continue
		}
		keyNorm := col.KeyNorm
		if keyNorm == "" {
			keyNorm = normalizeCSVHeader(col.Header)
		}
		if keyNorm == "" {
			continue
		}
		plan.ColumnRefs[keyNorm] = col.Index
	}
	return plan
}

func buildTemplateRowContext(plan *templateRowContextPlan, record []string) templateRowContext {
	values := make([]string, len(record))
	for i := range record {
		values[i] = strings.TrimSpace(record[i])
	}
	return templateRowContext{plan: plan, values: values}
}

type bookInfo struct {
	ID      int
	Indexed bool
}

func (a *App) prepareBookLookups(model *templateImportModel, ctx importContext, fixedBookID int) (map[string]bookInfo, map[int]bookInfo) {
	bookByKey := map[string]bookInfo{}
	bookByID := map[int]bookInfo{}
	filter := db.LlibreFilter{}
	if model.ScopeFilters || len(model.ContextFilters) > 0 {
		if ctx.MunicipiID != 0 {
			filter.MunicipiID = ctx.MunicipiID
		}
		if ctx.ArxiuID != 0 {
			filter.ArxiuID = ctx.ArxiuID
		}
	}
	llibres, _ := a.DB.ListLlibres(filter)
	for _, l := range llibres {
		bookByID[l.ID] = bookInfo{ID: l.ID, Indexed: l.IndexacioCompleta}
		if model.BookMode == "cronologia_lookup" {
			key := normalizeCronologia(l.Cronologia)
			if key == "" {
				continue
			}
			if existing, ok := bookByKey[key]; ok {
				if existing.ID != l.ID {
					bookByKey[key] = bookInfo{ID: -1}
				}
			} else {
				bookByKey[key] = bookInfo{ID: l.ID, Indexed: l.IndexacioCompleta}
			}
		}
	}
	if fixedBookID > 0 {
		if _, ok := bookByID[fixedBookID]; !ok {
			if llibre, err := a.DB.GetLlibre(fixedBookID); err == nil && llibre != nil {
				bookByID[fixedBookID] = bookInfo{ID: llibre.ID, Indexed: llibre.IndexacioCompleta}
			}
		}
	}
	return bookByKey, bookByID
}

func resolveTemplateBookID(model *templateImportModel, rowCtx templateRowContext, byKey map[string]bookInfo, byID map[int]bookInfo, fixedBookID int) (int, bookInfo, string) {
	if fixedBookID > 0 {
		info := byID[fixedBookID]
		if info.ID == 0 {
			return 0, bookInfo{}, "llibre no trobat"
		}
		if model.BookMode == "llibre_id" && model.BookColumn != "" {
			val := rowCtx.HeaderValue(normalizeCSVHeader(model.BookColumn))
			if val != "" {
				if id, err := strconv.Atoi(val); err == nil && id != fixedBookID {
					return 0, bookInfo{}, "llibre_id no coincideix"
				}
			}
		}
		return fixedBookID, info, ""
	}
	switch model.BookMode {
	case "cronologia_lookup":
		raw := rowCtx.HeaderValue(normalizeCSVHeader(model.BookColumn))
		if strings.TrimSpace(raw) == "" {
			return 0, bookInfo{}, "llibre buit"
		}
		key := raw
		if model.CronologiaNormalize {
			key = normalizeCronologia(key)
		}
		info, ok := byKey[key]
		if !ok || info.ID == 0 {
			return 0, bookInfo{}, "llibre no trobat: " + raw
		}
		if info.ID < 0 {
			return 0, bookInfo{}, "llibre ambigu: " + raw
		}
		return info.ID, info, ""
	default:
		raw := rowCtx.HeaderValue(normalizeCSVHeader(model.BookColumn))
		id, err := strconv.Atoi(raw)
		if err != nil || id == 0 {
			return 0, bookInfo{}, "llibre_id obligatori"
		}
		info, ok := byID[id]
		if !ok {
			return 0, bookInfo{}, "llibre no trobat"
		}
		return id, info, ""
	}
}

func applyTemplateColumn(col templateColumn, rawValue string, rowCtx templateRowContext, t *db.TranscripcioRaw, persones map[string]*db.TranscripcioPersonaRaw, atributs map[string]*db.TranscripcioAtributRaw, mappedValues map[string]string, parseCfg templateParseConfig) {
	if t == nil {
		return
	}
	applyMapTo := col.MapTo
	preTransforms := []templateTransform{}
	if col.Condition != nil {
		conditionStart := time.Now()
		ok := evalTemplateCondition(col.Condition.Expr, rawValue, rowCtx)
		if parseCfg.Metrics != nil {
			parseCfg.Metrics.addParseCondition(time.Since(conditionStart))
		}
		if ok {
			applyMapTo = col.Condition.Then.MapTo
			preTransforms = col.Condition.Then.Transforms
		} else if col.Condition.Else != nil {
			applyMapTo = col.Condition.Else.MapTo
			preTransforms = col.Condition.Else.Transforms
		} else {
			return
		}
	}
	for _, entry := range applyMapTo {
		if entry.Target == "" {
			continue
		}
		if entry.Condition != nil {
			conditionStart := time.Now()
			ok := evalInlineCondition(entry.Condition, rowCtx)
			if parseCfg.Metrics != nil {
				parseCfg.Metrics.addParseCondition(time.Since(conditionStart))
			}
			if !ok {
				continue
			}
		}
		value := rawValue
		extras := map[string]string{}
		if len(preTransforms) > 0 {
			transformStart := time.Now()
			value, extras = applyTemplateTransforms(value, preTransforms, parseCfg)
			if parseCfg.Metrics != nil {
				parseCfg.Metrics.addParseTransform(time.Since(transformStart))
			}
		}
		personMode, personFound := "", false
		if len(entry.Transforms) > 0 {
			transformStart := time.Now()
			val, ex, mode, found := applyTemplateTransformsWithPerson(value, entry.Transforms, parseCfg)
			value = val
			for k, v := range ex {
				extras[k] = v
			}
			personMode = mode
			personFound = found
			if parseCfg.Metrics != nil {
				parseCfg.Metrics.addParseTransform(time.Since(transformStart))
			}
		}
		if personFound && isPersonRoleTarget(entry.Target) {
			role := strings.TrimPrefix(entry.Target, "person.")
			role = strings.Split(role, ".")[0]
			var p *db.TranscripcioPersonaRaw
			switch personMode {
			case "nom_v2":
				p = buildPersonFromNomV2WithConfig(value, role, parseCfg)
			case "nom_v2_maternal_first":
				p = swapPersonCognoms(buildPersonFromNomV2WithConfig(value, role, parseCfg))
			case "cognoms_v2":
				p = buildPersonFromCognomsV2WithConfig(value, role, parseCfg)
			case "cognoms_v2_maternal_first":
				p = swapPersonCognoms(buildPersonFromCognomsV2WithConfig(value, role, parseCfg))
			case "nom":
				p = buildPersonFromNomWithConfig(value, role, parseCfg)
			default:
				p = buildPersonFromCognomsWithConfig(value, role, parseCfg)
			}
			if p != nil {
				persones[role] = mergePerson(persones[role], p)
			}
		} else {
			applyTemplateTarget(entry.Target, value, extras, t, persones, atributs, parseCfg)
		}
		if mappedValues != nil {
			mappedValues[entry.Target] = value
		}
	}
}

func swapPersonCognoms(p *db.TranscripcioPersonaRaw) *db.TranscripcioPersonaRaw {
	if p == nil {
		return nil
	}
	if strings.TrimSpace(p.Cognom1) == "" || strings.TrimSpace(p.Cognom2) == "" {
		return p
	}
	p.Cognom1, p.Cognom2 = p.Cognom2, p.Cognom1
	p.Cognom1Estat, p.Cognom2Estat = p.Cognom2Estat, p.Cognom1Estat
	return p
}

func applyTemplateTransforms(value string, transforms []templateTransform, parseCfg templateParseConfig) (string, map[string]string) {
	extras := map[string]string{}
	for _, tr := range transforms {
		kind := tr.Kind
		if kind == "" {
			kind = strings.TrimSpace(strings.ToLower(tr.Name))
		}
		switch kind {
		case "trim":
			value = strings.TrimSpace(value)
		case "lower":
			value = strings.ToLower(value)
		case "strip_diacritics":
			value = stripDiacritics(value)
		case "normalize_cronologia":
			value = normalizeCronologia(value)
		case "parse_ddmmyyyy_to_iso":
			iso, estat := parseDateToISOWithConfig(value, parseCfg)
			value = iso
			if estat != "" {
				extras["date_estat"] = estat
			}
		case "parse_int_nullable":
			if n, err := strconv.Atoi(strings.TrimSpace(value)); err == nil {
				value = strconv.Itoa(n)
			} else {
				value = ""
			}
		case "parse_marriage_order_int_nullable":
			if order, ok := parseMarriageOrder(value); ok {
				value = strconv.Itoa(order)
			} else {
				value = ""
			}
		case "strip_marriage_order_text":
			value = stripMarriageOrderText(value)
		case "parse_date_flexible_to_base_data_acte", "parse_date_flexible_to_date_or_text_with_quality":
			if strings.TrimSpace(value) != "" {
				extras["date_text"] = strings.TrimSpace(value)
			}
			iso, textRaw, estat := parseFlexibleDateWithConfig(value, parseCfg)
			if iso != "" {
				value = iso
			} else if textRaw != "" {
				value = textRaw
			} else {
				value = ""
			}
			if estat != "" {
				extras["date_estat"] = estat
			}
		case "split_couple_i":
			left, right := splitCouple(value)
			if tr.SelectRight {
				value = right
			} else {
				value = left
			}
		case "set_default":
			if strings.TrimSpace(value) == "" {
				value = tr.DefaultValue
			}
		case "map_values":
			if mapped, ok := tr.MapValues[value]; ok {
				value = mapped
			}
		case "regex_extract":
			if tr.Regex != nil {
				matches := tr.Regex.FindStringSubmatch(value)
				if tr.RegexGroup >= 0 && tr.RegexGroup < len(matches) {
					value = matches[tr.RegexGroup]
				}
			}
		case "extract_parenthetical_last":
			value = extractParentheticalLast(value)
		case "extract_parenthetical_all":
			value = strings.Join(extractParentheticalAll(value), "; ")
		case "strip_parentheticals":
			value = stripParentheticals(value)
		case "default_quality_if_present":
			if strings.TrimSpace(value) != "" {
				extras["quality"] = "clar"
			}
		}
	}
	return value, extras
}

func applyTemplateTransformsWithPerson(value string, transforms []templateTransform, parseCfg templateParseConfig) (string, map[string]string, string, bool) {
	extras := map[string]string{}
	for _, tr := range transforms {
		kind := tr.Kind
		if kind == "" {
			kind = strings.TrimSpace(strings.ToLower(tr.Name))
		}
		switch kind {
		case "parse_person_from_cognoms", "parse_person_from_cognoms_marcmoia_v2", "parse_person_from_cognoms_marcmoia_v2_maternal_first":
			mode := "cognoms"
			if kind == "parse_person_from_cognoms_marcmoia_v2" {
				mode = "cognoms_v2"
			}
			if kind == "parse_person_from_cognoms_marcmoia_v2_maternal_first" {
				mode = "cognoms_v2_maternal_first"
			}
			return value, extras, mode, true
		case "parse_person_from_nom", "parse_person_from_nom_marcmoia_v2", "parse_person_from_nom_marcmoia_v2_maternal_first":
			mode := "nom"
			if kind == "parse_person_from_nom_marcmoia_v2" {
				mode = "nom_v2"
			}
			if kind == "parse_person_from_nom_marcmoia_v2_maternal_first" {
				mode = "nom_v2_maternal_first"
			}
			return value, extras, mode, true
		default:
			val, ex := applyTemplateTransforms(value, []templateTransform{tr}, parseCfg)
			value = val
			for k, v := range ex {
				extras[k] = v
			}
		}
	}
	return value, extras, "", false
}

func isPersonRoleTarget(target string) bool {
	if !strings.HasPrefix(target, "person.") {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(target, "person."), ".")
	return len(parts) == 1 || parts[1] == ""
}

func applyTemplateTarget(target string, value string, extras map[string]string, t *db.TranscripcioRaw, persones map[string]*db.TranscripcioPersonaRaw, atributs map[string]*db.TranscripcioAtributRaw, parseCfg templateParseConfig) {
	target = strings.TrimSpace(target)
	if target == "" || t == nil {
		return
	}
	if strings.HasPrefix(target, "base.") {
		field := strings.TrimPrefix(target, "base.")
		applyBaseTarget(field, value, extras, t)
		return
	}
	if strings.HasPrefix(target, "person.") {
		applyPersonTarget(strings.TrimPrefix(target, "person."), value, extras, persones, parseCfg)
		return
	}
	if strings.HasPrefix(target, "attr.") {
		applyAttrTarget(strings.TrimPrefix(target, "attr."), value, extras, atributs, parseCfg)
		return
	}
}

func applyBaseTarget(field string, value string, extras map[string]string, t *db.TranscripcioRaw) {
	field = strings.TrimSpace(field)
	baseField := field
	var fieldType string
	if parts := strings.Split(field, "."); len(parts) > 1 {
		baseField = parts[0]
		fieldType = parts[1]
	}
	switch baseField {
	case "llibre_id":
		if id, err := strconv.Atoi(value); err == nil {
			t.LlibreID = id
		}
	case "pagina_id":
		if id, err := strconv.Atoi(value); err == nil {
			t.PaginaID = sql.NullInt64{Int64: int64(id), Valid: true}
		}
	case "num_pagina_text":
		t.NumPaginaText = value
	case "posicio_pagina":
		t.PosicioPagina = parseIntNull(value)
	case "tipus_acte":
		t.TipusActe = value
	case "any_doc":
		if fieldType == "int_nullable" {
			t.AnyDoc = parseIntNull(value)
		} else {
			t.AnyDoc = parseIntNull(value)
		}
	case "data_acte_text":
		t.DataActeText = value
	case "data_acte_iso":
		t.DataActeISO = parseNullString(value)
		if estat := extras["date_estat"]; estat != "" {
			t.DataActeEstat = estat
		}
	case "data_acte_estat":
		t.DataActeEstat = value
	case "data_acte_iso_text_estat":
		if text := extras["date_text"]; text != "" {
			t.DataActeText = text
		}
		if value != "" {
			if isISODate(value) {
				t.DataActeISO = parseNullString(value)
			} else {
				t.DataActeText = value
			}
		}
		if estat := extras["date_estat"]; estat != "" {
			t.DataActeEstat = estat
		}
	case "transcripcio_literal":
		t.TranscripcioLiteral = value
	case "notes_marginals":
		t.NotesMarginals = value
	case "observacions_paleografiques":
		t.ObservacionsPaleografiques = value
	case "moderation_status":
		if strings.TrimSpace(value) != "" {
			t.ModeracioEstat = value
		}
	}
}

func normalizeDataActeEstat(value string) string {
	value = normalizeQualityStatus(value)
	switch value {
	case "clar", "dubtos", "incomplet", "illegible", "no_consta":
		return value
	default:
		return ""
	}
}

func applyPersonTarget(field string, value string, extras map[string]string, persones map[string]*db.TranscripcioPersonaRaw, parseCfg templateParseConfig) {
	parts := strings.Split(field, ".")
	if len(parts) < 1 {
		return
	}
	role := parts[0]
	if role == "" {
		return
	}
	if len(parts) == 1 {
		if strings.TrimSpace(value) == "" {
			return
		}
		p := buildPersonFromCognomsWithConfig(value, role, parseCfg)
		if p == nil {
			return
		}
		persones[role] = mergePerson(persones[role], p)
		return
	}
	fieldName := parts[1]
	p := persones[role]
	if p == nil {
		p = &db.TranscripcioPersonaRaw{Rol: role}
		persones[role] = p
	}
	switch fieldName {
	case "nom":
		value, extras = applyTemplateQualityToPersonField(value, extras, parseCfg)
		p.Nom = value
		if extras["quality"] != "" {
			p.NomEstat = extras["quality"]
		}
	case "cognom1":
		value, extras = applyTemplateQualityToPersonField(value, extras, parseCfg)
		p.Cognom1 = value
		if extras["quality"] != "" {
			p.Cognom1Estat = extras["quality"]
		}
	case "cognom2":
		value, extras = applyTemplateQualityToPersonField(value, extras, parseCfg)
		p.Cognom2 = value
		if extras["quality"] != "" {
			p.Cognom2Estat = extras["quality"]
		}
	case "cognom_soltera":
		value, extras = applyTemplateQualityToPersonField(value, extras, parseCfg)
		p.CognomSoltera = value
		if extras["quality"] != "" {
			p.CognomSolteraEstat = extras["quality"]
		}
	case "ofici_text_with_quality":
		p.OficiText = value
		if extras["quality"] != "" {
			p.OficiEstat = extras["quality"]
		}
	case "municipi_text_with_quality":
		p.MunicipiText = value
		if extras["quality"] != "" {
			p.MunicipiEstat = extras["quality"]
		}
	default:
		applyPersonField(p, fieldName, value)
	}
}

func applyTemplateQualityToPersonField(value string, extras map[string]string, parseCfg templateParseConfig) (string, map[string]string) {
	if extras == nil {
		extras = map[string]string{}
	}
	cleaned, qual := extractQualityWithConfig(value, parseCfg)
	if cleaned != "" || qual != "" {
		value = cleaned
	}
	if extras["quality"] == "" && qual != "" {
		extras["quality"] = qual
	}
	return value, extras
}

func applyAttrTarget(field string, value string, extras map[string]string, atributs map[string]*db.TranscripcioAtributRaw, parseCfg templateParseConfig) {
	parts := strings.Split(field, ".")
	if len(parts) == 0 {
		return
	}
	key := parts[0]
	if key == "" {
		return
	}
	attrType := "text"
	if len(parts) > 1 && parts[1] != "" {
		attrType = parts[1]
	}
	attr := atributs[key]
	if attr == nil {
		attr = &db.TranscripcioAtributRaw{Clau: key}
		atributs[key] = attr
	}
	switch attrType {
	case "int", "int_nullable":
		attr.TipusValor = "int"
		attr.ValorInt = parseIntNull(value)
	case "date":
		attr.TipusValor = "date"
		attr.ValorDate = parseNullString(value)
	case "bool":
		attr.TipusValor = "bool"
		l := strings.ToLower(strings.TrimSpace(value))
		if l == "1" || l == "true" || l == "si" || l == "yes" || l == "on" {
			attr.ValorBool = sql.NullBool{Bool: true, Valid: true}
		} else if l != "" {
			attr.ValorBool = sql.NullBool{Bool: false, Valid: true}
		}
	case "estat":
		attr.Estat = value
	case "date_or_text_with_quality":
		if isISODate(value) {
			attr.TipusValor = "date"
			attr.ValorDate = parseNullString(value)
		} else {
			attr.TipusValor = "text"
			attr.ValorText = value
		}
		if estat := extras["date_estat"]; estat != "" {
			attr.Estat = estat
		} else if value != "" {
			_, estat := parseDateToISOWithConfig(value, parseCfg)
			if estat != "" {
				attr.Estat = estat
			}
		}
	case "text_with_quality":
		attr.TipusValor = "text"
		attr.ValorText = value
		if extras["quality"] != "" {
			attr.Estat = extras["quality"]
		}
	default:
		attr.TipusValor = "text"
		attr.ValorText = value
	}
}

func buildTemplateDedupKey(fields []string, rowCtx templateRowContext, mapped map[string]string) string {
	return buildTemplateDedupKeyWithCache(nil, fields, rowCtx, mapped)
}

func buildTemplateDedupKeyWithCache(cache *templateMatchBuildCache, fields []string, rowCtx templateRowContext, mapped map[string]string) string {
	parts := make([]string, 0, len(fields))
	for _, key := range fields {
		if key == "" {
			continue
		}
		norm := normalizeCSVHeader(key)
		if val, ok := rowCtx.LookupColumnValue(norm); ok {
			parts = append(parts, val)
			continue
		}
		if val, ok := rowCtx.LookupHeaderValue(norm); ok {
			parts = append(parts, val)
			continue
		}
		if val, ok := mapped[key]; ok {
			parts = append(parts, val)
			continue
		}
	}
	if len(parts) == 0 {
		return ""
	}
	for i, p := range parts {
		parts[i] = normalizeTemplateLowerPartWithCache(cache, p)
	}
	return strings.Join(parts, "|")
}

func templateSeenMatchRow(seenByContext map[string]map[string]int, contextKey, matchKey string) (int, bool) {
	if contextKey == "" || matchKey == "" {
		return 0, false
	}
	rows := seenByContext[contextKey]
	if rows == nil {
		return 0, false
	}
	firstRow, ok := rows[matchKey]
	return firstRow, ok
}

func templateRememberSeenMatch(seenByContext map[string]map[string]int, contextKey, matchKey string, rowNum int) {
	if contextKey == "" || matchKey == "" {
		return
	}
	rows := seenByContext[contextKey]
	if rows == nil {
		rows = map[string]int{}
		seenByContext[contextKey] = rows
	}
	rows[matchKey] = rowNum
}

func evalTemplateCondition(expr string, value string, rowCtx templateRowContext) bool {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return strings.TrimSpace(value) != ""
	}
	lower := strings.ToLower(expr)
	if lower == "not_empty" {
		return strings.TrimSpace(value) != ""
	}
	if strings.Contains(expr, "==") || strings.Contains(expr, "!=") {
		op := "=="
		if strings.Contains(expr, "!=") {
			op = "!="
		}
		parts := strings.Split(expr, op)
		if len(parts) != 2 {
			return false
		}
		left := strings.TrimSpace(parts[0])
		right := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
		leftVal := value
		if strings.HasPrefix(strings.ToLower(left), "column:") {
			ref := strings.TrimSpace(left[7:])
			if ref != "" {
				leftVal, _ = rowCtx.LookupColumnValue(normalizeCSVHeader(ref))
				if leftVal == "" {
					leftVal = rowCtx.HeaderValue(normalizeCSVHeader(ref))
				}
			}
		}
		if op == "==" {
			return leftVal == right
		}
		return leftVal != right
	}
	return strings.TrimSpace(value) != ""
}

func evalInlineCondition(cond *templateInlineCondition, rowCtx templateRowContext) bool {
	if cond == nil {
		return true
	}
	switch strings.ToLower(cond.Op) {
	case "not_empty":
		ref := asString(cond.Args["column"])
		if ref == "" {
			return false
		}
		val := rowCtx.ColumnValue(normalizeCSVHeader(ref))
		if val == "" {
			val = rowCtx.HeaderValue(normalizeCSVHeader(ref))
		}
		return strings.TrimSpace(val) != ""
	case "equals":
		ref := asString(cond.Args["column"])
		expected := asString(cond.Args["value"])
		val := rowCtx.ColumnValue(normalizeCSVHeader(ref))
		if val == "" {
			val = rowCtx.HeaderValue(normalizeCSVHeader(ref))
		}
		return val == expected
	default:
		return true
	}
}

func (a *App) loadExistingByPrincipal(runtime db.TemplateImportRuntime, bookID int, roles []string, snapshotMaxID int) map[string]int {
	existingMap := map[string]int{}
	if runtime == nil {
		return existingMap
	}
	candidates, _ := runtime.LoadPrincipalMatchCandidates(db.TemplateImportPrincipalMatchRequest{
		BookID:        bookID,
		SnapshotMaxID: snapshotMaxID,
	})
	for _, tr := range candidates.Transcripcions {
		personesExistents, ok := candidates.PersonesByTranscripcioID[tr.ID]
		if !ok {
			personesExistents, _ = a.DB.ListTranscripcioPersones(tr.ID)
		}
		for _, p := range personesExistents {
			if len(roles) > 0 && !stringInSlice(p.Rol, roles) {
				continue
			}
			nameKey := normalizeNameKey(p.Nom, p.Cognom1, p.Cognom2)
			if nameKey == "" {
				continue
			}
			if _, exists := existingMap[nameKey]; !exists {
				existingMap[nameKey] = tr.ID
			}
			break
		}
	}
	return existingMap
}

func (a *App) loadExistingByStrongMatch(bookID int, incoming *db.TranscripcioRaw, incomingAttrs map[string]*db.TranscripcioAtributRaw, policies templatePolicies) map[string]int {
	return a.loadExistingByStrongMatchWithPageResolver(nil, bookID, incoming, incomingAttrs, policies)
}

func (a *App) loadExistingByStrongMatchWithPageResolver(pageResolver db.TemplateImportPageResolver, bookID int, incoming *db.TranscripcioRaw, incomingAttrs map[string]*db.TranscripcioAtributRaw, policies templatePolicies) map[string]int {
	return a.loadExistingByStrongMatchWithPageResolverSnapshot(db.TemplateImportRuntimeFor(a.DB), pageResolver, bookID, incoming, incomingAttrs, policies, -1)
}

func (a *App) loadExistingByStrongMatchWithPageResolverSnapshot(runtime db.TemplateImportRuntime, pageResolver db.TemplateImportPageResolver, bookID int, incoming *db.TranscripcioRaw, incomingAttrs map[string]*db.TranscripcioAtributRaw, policies templatePolicies, snapshotMaxID int) map[string]int {
	existingMap := map[string]int{}
	if incoming == nil {
		return existingMap
	}
	pageKey := templateLogicalPageKey(incoming, incomingAttrs)
	if pageKey == "" || strings.TrimSpace(incoming.TipusActe) == "" {
		return existingMap
	}
	if snapshotMaxID == 0 {
		return existingMap
	}
	matchBuildCache := newTemplateMatchBuildCache()
	pageKeyNorm := normalizeTemplateMatchPartWithCache(matchBuildCache, pageKey)
	candidates, _ := runtime.LoadStrongMatchCandidates(db.TemplateImportStrongMatchRequest{
		BookID:        bookID,
		TipusActe:     incoming.TipusActe,
		PageKey:       pageKey,
		SnapshotMaxID: snapshotMaxID,
	})
	attrsByTranscripcioID := candidates.AtributsByTranscripcioID
	personesByTranscripcioID := candidates.PersonesByTranscripcioID
	trans := candidates.Transcripcions
	if len(trans) > 0 {
		for _, tr := range trans {
			if tr.ID <= 0 || (snapshotMaxID >= 0 && tr.ID > snapshotMaxID) {
				continue
			}
			attrsExistents, okAttrs := attrsByTranscripcioID[tr.ID]
			if !okAttrs {
				attrsExistents, _ = a.DB.ListTranscripcioAtributs(tr.ID)
			}
			if normalizeTemplateMatchPartWithCache(matchBuildCache, a.templateLogicalPageKeyForExistingWithResolver(pageResolver, bookID, &tr, attrsExistents)) != pageKeyNorm {
				continue
			}
			personesExistentsRows, okPersones := personesByTranscripcioID[tr.ID]
			if !okPersones {
				personesExistentsRows, _ = a.DB.ListTranscripcioPersones(tr.ID)
			}
			personesExistents := map[string]*db.TranscripcioPersonaRaw{}
			for i := range personesExistentsRows {
				personesExistents[personesExistentsRows[i].Rol] = &personesExistentsRows[i]
			}
			attrsByKey := map[string]*db.TranscripcioAtributRaw{}
			for i := range attrsExistents {
				attrsByKey[attrsExistents[i].Clau] = &attrsExistents[i]
			}
			matchKey := buildTemplateStrongMatchKeyWithCache(matchBuildCache, &tr, personesExistents, attrsByKey, policies)
			if matchKey == "" {
				continue
			}
			if _, exists := existingMap[matchKey]; !exists {
				existingMap[matchKey] = tr.ID
			}
		}
		return existingMap
	}
	return existingMap
}

func (a *App) templateIndexedPageKey(bookID int, t *db.TranscripcioRaw, attrs map[string]*db.TranscripcioAtributRaw) (string, bool) {
	return a.templateIndexedPageKeyWithResolver(db.TemplateImportRuntimeFor(a.DB).NewPageResolver(), bookID, t, attrs)
}

func (a *App) templateIndexedPageKeyWithResolver(pageResolver db.TemplateImportPageResolver, bookID int, t *db.TranscripcioRaw, attrs map[string]*db.TranscripcioAtributRaw) (string, bool) {
	if pageResolver == nil {
		return templateLogicalPageKey(t, attrs), false
	}
	resolution, err := pageResolver.ResolveIncoming(bookID, t, attrs)
	if err != nil {
		return templateLogicalPageKey(t, attrs), false
	}
	if resolution.Resolved && resolution.CanonicalKey != "" {
		return resolution.CanonicalKey, resolution.Indexed
	}
	return templateLogicalPageKey(t, attrs), resolution.Indexed
}

func (a *App) templateLogicalPageKeyForExisting(t *db.TranscripcioRaw, attrs []db.TranscripcioAtributRaw) string {
	return a.templateLogicalPageKeyForExistingWithResolver(db.TemplateImportRuntimeFor(a.DB).NewPageResolver(), 0, t, attrs)
}

func (a *App) templateLogicalPageKeyForExistingWithResolver(pageResolver db.TemplateImportPageResolver, bookID int, t *db.TranscripcioRaw, attrs []db.TranscripcioAtributRaw) string {
	attrsByKey := map[string]*db.TranscripcioAtributRaw{}
	for i := range attrs {
		attrsByKey[attrs[i].Clau] = &attrs[i]
	}
	pageKey := templateLogicalPageKey(t, attrsByKey)
	if pageKey != "" || pageResolver == nil {
		return pageKey
	}
	resolved, err := pageResolver.ResolveExisting(bookID, t, attrs)
	if err != nil {
		return ""
	}
	return resolved
}

func templateLogicalPageKey(t *db.TranscripcioRaw, attrs map[string]*db.TranscripcioAtributRaw) string {
	if attr := attrs["pagina_digital"]; attr != nil {
		if value := templateAttrComparableValue(attr); value != "" {
			return value
		}
	}
	if t != nil && strings.TrimSpace(t.NumPaginaText) != "" {
		return strings.TrimSpace(t.NumPaginaText)
	}
	return ""
}

func buildTemplateStrongMatchKey(t *db.TranscripcioRaw, persones map[string]*db.TranscripcioPersonaRaw, attrs map[string]*db.TranscripcioAtributRaw, policies templatePolicies) string {
	return buildTemplateStrongMatchKeyWithCache(nil, t, persones, attrs, policies)
}

func buildTemplateStrongMatchKeyWithCache(cache *templateMatchBuildCache, t *db.TranscripcioRaw, persones map[string]*db.TranscripcioPersonaRaw, attrs map[string]*db.TranscripcioAtributRaw, policies templatePolicies) string {
	if t == nil {
		return ""
	}
	principalKey := templateStrongPrincipalKeyWithCache(cache, persones, policies.PrincipalRoles)
	if principalKey == "" {
		return ""
	}
	signals := []string{"principal:" + principalKey}
	extraCount := 0
	seenSignals := map[string]struct{}{}
	addExtra := func(kind, value string) {
		value = normalizeTemplateMatchPartWithCache(cache, value)
		if value == "" {
			return
		}
		signal := kind + ":" + value
		if _, exists := seenSignals[signal]; exists {
			return
		}
		seenSignals[signal] = struct{}{}
		signals = append(signals, signal)
		extraCount++
	}
	if t.DataActeISO.Valid {
		addExtra("data_acte", t.DataActeISO.String)
	} else {
		addExtra("data_acte", t.DataActeText)
	}
	switch strings.ToLower(strings.TrimSpace(t.TipusActe)) {
	case "baptisme":
		for _, key := range []string{"data_bateig", "data_naixement", "data_defuncio", "casat"} {
			if attr := attrs[key]; attr != nil {
				addExtra("attr:"+key, templateAttrComparableValue(attr))
			}
		}
		for _, role := range []string{"pare", "mare", "avi_patern", "avia_paterna", "avi_matern", "avia_materna", "padri", "padrina"} {
			if p := persones[role]; p != nil {
				addExtra("person:"+role, templateStrongPersonKeyWithCache(cache, p))
			}
		}
	default:
		attrKeys := make([]string, 0, len(attrs))
		for key := range attrs {
			if key == "pagina_digital" {
				continue
			}
			attrKeys = append(attrKeys, key)
		}
		sort.Strings(attrKeys)
		for _, key := range attrKeys {
			addExtra("attr:"+key, templateAttrComparableValue(attrs[key]))
		}
		principalRoles := map[string]struct{}{}
		for _, role := range policies.PrincipalRoles {
			role = strings.TrimSpace(role)
			if role != "" {
				principalRoles[role] = struct{}{}
			}
		}
		if len(principalRoles) == 0 {
			principalRoles["batejat"] = struct{}{}
			principalRoles["persona_principal"] = struct{}{}
		}
		roleKeys := make([]string, 0, len(persones))
		for role := range persones {
			if _, skip := principalRoles[role]; skip {
				continue
			}
			roleKeys = append(roleKeys, role)
		}
		sort.Strings(roleKeys)
		for _, role := range roleKeys {
			addExtra("person:"+role, templateStrongPersonKeyWithCache(cache, persones[role]))
		}
	}
	if extraCount < 2 {
		return ""
	}
	return strings.Join(signals, "|")
}

func templateStrongPrincipalKey(persones map[string]*db.TranscripcioPersonaRaw, roles []string) string {
	return templateStrongPrincipalKeyWithCache(nil, persones, roles)
}

func templateStrongPrincipalKeyWithCache(cache *templateMatchBuildCache, persones map[string]*db.TranscripcioPersonaRaw, roles []string) string {
	if len(roles) == 0 {
		roles = []string{"batejat", "persona_principal"}
	}
	for _, role := range roles {
		if key := templateStrongPersonKeyWithCache(cache, persones[role]); key != "" {
			return key
		}
	}
	return ""
}

func templateStrongPersonKey(p *db.TranscripcioPersonaRaw) string {
	return templateStrongPersonKeyWithCache(nil, p)
}

func templateStrongPersonKeyWithCache(cache *templateMatchBuildCache, p *db.TranscripcioPersonaRaw) string {
	if p == nil {
		return ""
	}
	cacheKey := ""
	if cache != nil {
		cacheKey = p.Nom + "\x00" + p.Cognom1 + "\x00" + p.Cognom2
		if key, ok := cache.personKeys[cacheKey]; ok {
			return key
		}
	}
	nom := normalizeTemplateMatchPartWithCache(cache, p.Nom)
	cognom1 := normalizeTemplateMatchPartWithCache(cache, p.Cognom1)
	cognom2 := normalizeTemplateMatchPartWithCache(cache, p.Cognom2)
	if nom == "" || (cognom1 == "" && cognom2 == "") {
		if cache != nil {
			cache.personKeys[cacheKey] = ""
		}
		return ""
	}
	key := nom + "|" + cognom1 + "|" + cognom2
	if cache != nil {
		cache.personKeys[cacheKey] = key
	}
	return key
}

func templateAttrComparableValue(attr *db.TranscripcioAtributRaw) string {
	if attr == nil {
		return ""
	}
	if attr.ValorDate.Valid {
		return attr.ValorDate.String
	}
	if attr.ValorInt.Valid {
		return strconv.FormatInt(attr.ValorInt.Int64, 10)
	}
	if attr.ValorBool.Valid {
		if attr.ValorBool.Bool {
			return "true"
		}
		return "false"
	}
	return attr.ValorText
}

func normalizeTemplateMatchPart(value string) string {
	return normalizeTemplateMatchPartWithCache(nil, value)
}

func normalizeTemplateMatchPartWithCache(cache *templateMatchBuildCache, value string) string {
	rawValue := value
	if cache != nil {
		if normalized, ok := cache.normalizedParts[rawValue]; ok {
			return normalized
		}
	}
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) >= 10 && isISODate(value[:10]) {
		value = value[:10]
		if cache != nil {
			cache.normalizedParts[rawValue] = value
		}
		return value
	}
	value = stripDiacritics(value)
	value = strings.Join(strings.Fields(value), " ")
	if cache != nil {
		cache.normalizedParts[rawValue] = value
	}
	return value
}

func normalizeTemplateLowerPartWithCache(cache *templateMatchBuildCache, value string) string {
	if cache != nil {
		if lowered, ok := cache.loweredParts[value]; ok {
			return lowered
		}
	}
	lowered := strings.ToLower(strings.TrimSpace(value))
	if cache != nil {
		cache.loweredParts[value] = lowered
	}
	return lowered
}

func newTemplateMatchBuildCache() *templateMatchBuildCache {
	return &templateMatchBuildCache{
		normalizedParts: map[string]string{},
		loweredParts:    map[string]string{},
		personKeys:      map[string]string{},
	}
}

func parseStrictPositiveInt(value string) (int, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return 0, false
		}
	}
	n, err := strconv.Atoi(value)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}

func (a *App) mergeTemplateRow(existingID int, t *db.TranscripcioRaw, persones map[string]*db.TranscripcioPersonaRaw, atributs map[string]*db.TranscripcioAtributRaw, policies templatePolicies) (bool, bool) {
	existing, err := a.DB.GetTranscripcioRaw(existingID)
	if err != nil || existing == nil {
		return false, false
	}
	updated := updateExistingTranscripcio(existing, t, policies.UpdateMissingOnly)
	if updated {
		_ = a.DB.UpdateTranscripcioRaw(existing)
	}
	if policies.AddMissingPeople {
		personesExistents, _ := a.DB.ListTranscripcioPersones(existingID)
		personKeys := map[string]bool{}
		for _, p := range personesExistents {
			personKeys[personKey(&p)] = true
		}
		for _, p := range persones {
			if isEmptyPerson(p) {
				continue
			}
			key := personKey(p)
			if personKeys[key] {
				continue
			}
			p.TranscripcioID = existingID
			_, _ = a.DB.CreateTranscripcioPersona(p)
			personKeys[key] = true
		}
	}
	if policies.AddMissingAttrs {
		attrsExistents, _ := a.DB.ListTranscripcioAtributs(existingID)
		attrKeys := map[string]bool{}
		for _, a := range attrsExistents {
			attrKeys[a.Clau] = true
		}
		for _, attr := range atributs {
			if isEmptyAttr(attr) {
				continue
			}
			if attrKeys[attr.Clau] {
				continue
			}
			attr.TranscripcioID = existingID
			_, _ = a.DB.CreateTranscripcioAtribut(attr)
			attrKeys[attr.Clau] = true
		}
	}
	return updated, true
}

func updateExistingTranscripcio(existing *db.TranscripcioRaw, incoming *db.TranscripcioRaw, missingOnly bool) bool {
	if existing == nil || incoming == nil {
		return false
	}
	incoming.DataActeEstat = normalizeDataActeEstat(incoming.DataActeEstat)
	updated := false
	if (!missingOnly) || existing.NumPaginaText == "" {
		if incoming.NumPaginaText != "" {
			existing.NumPaginaText = incoming.NumPaginaText
			updated = true
		}
	}
	if (!missingOnly) || !existing.PosicioPagina.Valid {
		if incoming.PosicioPagina.Valid {
			existing.PosicioPagina = incoming.PosicioPagina
			updated = true
		}
	}
	if (!missingOnly) || !existing.AnyDoc.Valid {
		if incoming.AnyDoc.Valid {
			existing.AnyDoc = incoming.AnyDoc
			updated = true
		}
	}
	if (!missingOnly) || existing.DataActeText == "" {
		if incoming.DataActeText != "" {
			existing.DataActeText = incoming.DataActeText
			updated = true
		}
	}
	if (!missingOnly) || !existing.DataActeISO.Valid {
		if incoming.DataActeISO.Valid {
			existing.DataActeISO = incoming.DataActeISO
			updated = true
		}
	}
	if (!missingOnly) || existing.DataActeEstat == "" || existing.DataActeEstat == "no_consta" {
		if incoming.DataActeEstat != "" {
			existing.DataActeEstat = incoming.DataActeEstat
			updated = true
		}
	}
	if (!missingOnly) || existing.TranscripcioLiteral == "" {
		if incoming.TranscripcioLiteral != "" {
			existing.TranscripcioLiteral = incoming.TranscripcioLiteral
			updated = true
		}
	}
	if (!missingOnly) || existing.NotesMarginals == "" {
		if incoming.NotesMarginals != "" {
			existing.NotesMarginals = incoming.NotesMarginals
			updated = true
		}
	}
	if (!missingOnly) || existing.ObservacionsPaleografiques == "" {
		if incoming.ObservacionsPaleografiques != "" {
			existing.ObservacionsPaleografiques = incoming.ObservacionsPaleografiques
			updated = true
		}
	}
	return updated
}

func principalPersonKey(persones map[string]*db.TranscripcioPersonaRaw, roles []string) string {
	if len(persones) == 0 {
		return ""
	}
	if len(roles) == 0 {
		roles = []string{"batejat", "persona_principal"}
	}
	for _, role := range roles {
		if p := persones[role]; p != nil {
			return normalizeNameKey(p.Nom, p.Cognom1, p.Cognom2)
		}
	}
	return ""
}

func principalPersonHasName(persones map[string]*db.TranscripcioPersonaRaw, roles []string) bool {
	if len(persones) == 0 {
		return false
	}
	if len(roles) == 0 {
		roles = []string{"batejat", "persona_principal"}
	}
	for _, role := range roles {
		if p := persones[role]; p != nil && strings.TrimSpace(p.Nom) != "" {
			return true
		}
	}
	return false
}

func pickTemplateModerationStatus(model *templateImportModel) string {
	if model == nil {
		return "pendent"
	}
	if model.BaseDefaults["moderation_status"] != "" {
		return model.BaseDefaults["moderation_status"]
	}
	if model.Policies.ModerationStatus != "" {
		return model.Policies.ModerationStatus
	}
	return "pendent"
}

func applyBaseDefaults(t *db.TranscripcioRaw, defaults map[string]string) {
	if t == nil || defaults == nil {
		return
	}
	if v := defaults["tipus_acte"]; v != "" {
		t.TipusActe = v
	}
	if v := defaults["moderation_status"]; v != "" {
		t.ModeracioEstat = v
	}
	if v := defaults["data_acte_estat"]; v != "" {
		t.DataActeEstat = v
	}
}

func templateImportDebugModel(model *templateImportModel) string {
	if model == nil {
		return ""
	}
	switch model.PresetCode {
	case "generic_v1":
		return "generic"
	case "baptismes_marcmoia", "baptismes_marcmoia_v2":
		return "template:" + model.PresetCode
	default:
		return ""
	}
}

func mergePerson(base *db.TranscripcioPersonaRaw, incoming *db.TranscripcioPersonaRaw) *db.TranscripcioPersonaRaw {
	if base == nil {
		return incoming
	}
	if base.Nom == "" {
		base.Nom = incoming.Nom
	}
	if base.Cognom1 == "" {
		base.Cognom1 = incoming.Cognom1
	}
	if base.Cognom2 == "" {
		base.Cognom2 = incoming.Cognom2
	}
	if base.CognomSoltera == "" {
		base.CognomSoltera = incoming.CognomSoltera
	}
	if base.NomEstat == "" {
		base.NomEstat = incoming.NomEstat
	}
	if base.Cognom1Estat == "" {
		base.Cognom1Estat = incoming.Cognom1Estat
	}
	if base.Cognom2Estat == "" {
		base.Cognom2Estat = incoming.Cognom2Estat
	}
	if base.CognomSolteraEstat == "" {
		base.CognomSolteraEstat = incoming.CognomSolteraEstat
	}
	if base.Notes == "" {
		base.Notes = incoming.Notes
	}
	if base.MunicipiText == "" {
		base.MunicipiText = incoming.MunicipiText
	}
	return base
}

func columnHasDefault(col templateColumn) bool {
	for _, entry := range col.MapTo {
		for _, tr := range entry.Transforms {
			if strings.ToLower(tr.Name) == "set_default" {
				return true
			}
		}
	}
	if col.Condition != nil {
		if branchHasDefault(col.Condition.Then) {
			return true
		}
		if col.Condition.Else != nil && branchHasDefault(*col.Condition.Else) {
			return true
		}
	}
	return false
}

func branchHasDefault(branch templateBranch) bool {
	for _, entry := range branch.MapTo {
		for _, tr := range entry.Transforms {
			if strings.ToLower(tr.Name) == "set_default" {
				return true
			}
		}
	}
	for _, tr := range branch.Transforms {
		if strings.ToLower(tr.Name) == "set_default" {
			return true
		}
	}
	return false
}

func asString(val interface{}) string {
	if val == nil {
		return ""
	}
	switch v := val.(type) {
	case string:
		return strings.TrimSpace(v)
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func asBool(val interface{}) bool {
	switch v := val.(type) {
	case bool:
		return v
	case string:
		v = strings.ToLower(strings.TrimSpace(v))
		return v == "1" || v == "true" || v == "yes" || v == "si"
	case float64:
		return v != 0
	case int:
		return v != 0
	default:
		return false
	}
}

func asStringSlice(val interface{}) []string {
	list := []string{}
	switch v := val.(type) {
	case []interface{}:
		for _, item := range v {
			if s := asString(item); s != "" {
				list = append(list, s)
			}
		}
	case []string:
		for _, s := range v {
			if strings.TrimSpace(s) != "" {
				list = append(list, strings.TrimSpace(s))
			}
		}
	case string:
		for _, part := range strings.Split(v, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				list = append(list, part)
			}
		}
	}
	return list
}

func asMap(val interface{}) map[string]interface{} {
	if v, ok := val.(map[string]interface{}); ok {
		return v
	}
	return map[string]interface{}{}
}

func asMapString(val map[string]interface{}) map[string]string {
	if val == nil {
		return nil
	}
	out := map[string]string{}
	for k, v := range val {
		out[k] = asString(v)
	}
	return out
}

func fromMap(val interface{}) map[string]interface{} {
	if m, ok := val.(map[string]interface{}); ok {
		return m
	}
	return nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func intFromInterface(val interface{}, fallback int) int {
	switch v := val.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case string:
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return n
		}
	}
	return fallback
}

func stringInSlice(val string, list []string) bool {
	for _, item := range list {
		if item == val {
			return true
		}
	}
	return false
}
