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
	RecordType             string
	PresetCode             string
	BookMode               string
	BookColumn             string
	BookCodeColumn         string
	BookSourceSystem       string
	BookSourceColumn       string
	BookExternalIDColumn   string
	BookExternalCodeColumn string
	BookArchiveCodeColumn  string
	BookChronologyColumn   string
	BookTypeColumn         string
	BookTitleColumn        string
	BookChurchNameColumn   string
	BookURLColumn          string
	BookBaseURLColumn      string
	BookURLOverrideColumn  string
	MunicipalityContext    string
	CronologiaNormalize    bool
	AmbiguityPolicy        string
	ScopeFilters           bool
	ContextFilters         []string
	BaseDefaults           map[string]string
	Mapping                []templateColumn
	Policies               templatePolicies
	NameOrder              string
	DateFormat             string
	Quality                templateQualityConfig
}

type templatePolicies struct {
	ModerationStatus                     string
	DedupWithin                          bool
	DedupKeyFields                       []string
	DedupKeyStrategy                     string
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

type templateDedupKeyFieldSource int

const (
	templateDedupKeyFieldSourceMissing templateDedupKeyFieldSource = iota
	templateDedupKeyFieldSourceColumn
	templateDedupKeyFieldSourceHeader
	templateDedupKeyFieldSourceMapped
)

type templateDedupKeyFieldPlan struct {
	RawKey  string
	NormKey string
	Source  templateDedupKeyFieldSource
	Index   int
}

type templateDedupKeyPlan struct {
	Strategy string
	Fields   []templateDedupKeyFieldPlan
}

const templateImportCreateBatchSize = 500

type templatePendingCreate struct {
	RowNum int
	BookID int
	Bundle db.TranscripcioRawImportBundle
}

type templateMergeOutcome struct {
	Accepted        bool
	Changed         bool
	RecordID        int
	ChangeID        int
	CreatedProposal bool
}

type templateDuplicateCheckBlockMetrics struct {
	Key                         string
	BookID                      int
	PageKey                     string
	TipusActe                   string
	SnapshotMaxID               int
	IncomingRowsCount           int
	PageIndexed                 bool
	PageResolverUsed            bool
	StrongMatchEnabled          bool
	RuntimeLoadCandidatesCalled bool
	ReasonIfNotCalled           string
	FallbackPath                string
	PageLookupDur               time.Duration
	BuildMatchKeyDur            time.Duration
	ExistingLoadDur             time.Duration
	CompareDur                  time.Duration
	TotalBlockDuplicateCheckDur time.Duration
	SkipLogged                  bool
}

type templateDuplicateCheckRunMetrics struct {
	Enabled                             bool
	Engine                              string
	RuntimeType                         string
	Model                               string
	Scope                               string
	TemplateID                          int
	Books                               int
	Rows                                int
	RuntimeCallsCount                   int
	FallbackCallsCount                  int
	StrongSnapshotLogLinesExpectedCount int
	Blocks                              map[string]*templateDuplicateCheckBlockMetrics
}

func newTemplateDuplicateCheckRunMetrics(enabled bool, engine, runtimeType, model, scope string, templateID, books int) *templateDuplicateCheckRunMetrics {
	return &templateDuplicateCheckRunMetrics{
		Enabled:     enabled,
		Engine:      engine,
		RuntimeType: runtimeType,
		Model:       model,
		Scope:       scope,
		TemplateID:  templateID,
		Books:       books,
		Blocks:      map[string]*templateDuplicateCheckBlockMetrics{},
	}
}

func (m *templateDuplicateCheckRunMetrics) logStart() {
	_ = m
}

func (m *templateDuplicateCheckRunMetrics) block(key string, bookID int, pageKey, tipusActe string, snapshotMaxID int) *templateDuplicateCheckBlockMetrics {
	if m == nil || !m.Enabled {
		return nil
	}
	block := m.Blocks[key]
	if block == nil {
		block = &templateDuplicateCheckBlockMetrics{
			Key:           key,
			BookID:        bookID,
			PageKey:       pageKey,
			TipusActe:     tipusActe,
			SnapshotMaxID: snapshotMaxID,
		}
		m.Blocks[key] = block
	}
	if bookID > 0 {
		block.BookID = bookID
	}
	if pageKey != "" {
		block.PageKey = pageKey
	}
	if tipusActe != "" {
		block.TipusActe = tipusActe
	}
	if snapshotMaxID != 0 {
		block.SnapshotMaxID = snapshotMaxID
	}
	block.IncomingRowsCount++
	return block
}

func (m *templateDuplicateCheckRunMetrics) logSkipRuntimeLoadStrongCandidates(block *templateDuplicateCheckBlockMetrics, reason string, runtime db.TemplateImportRuntime, fallbackPath string) {
	if m == nil || !m.Enabled || block == nil || block.SkipLogged {
		return
	}
	_ = runtime
	block.SkipLogged = true
	block.ReasonIfNotCalled = reason
	if fallbackPath != "" {
		block.FallbackPath = fallbackPath
	}
}

func (m *templateDuplicateCheckRunMetrics) logFallbackPath(reason string, dur time.Duration, rows, bookID int) {
	if m == nil || !m.Enabled {
		return
	}
	_, _, _ = dur, rows, bookID
	_ = reason
	m.FallbackCallsCount++
}

func (m *templateDuplicateCheckRunMetrics) logBlocksAndSummary(rows int) {
	if m == nil || !m.Enabled {
		return
	}
	m.Rows = rows
	totalDur := time.Duration(0)
	pageLookupDur := time.Duration(0)
	dedupWithinOnlyDur := time.Duration(0)
	buildKeysDur := time.Duration(0)
	pageIndexedFalseDur := time.Duration(0)
	blocks := make([]*templateDuplicateCheckBlockMetrics, 0, len(m.Blocks))
	for _, block := range m.Blocks {
		if block == nil {
			continue
		}
		blocks = append(blocks, block)
		totalDur += block.TotalBlockDuplicateCheckDur
		pageLookupDur += block.PageLookupDur
		buildKeysDur += block.BuildMatchKeyDur
		if block.ReasonIfNotCalled == "dedup_within_only" {
			dedupWithinOnlyDur += block.TotalBlockDuplicateCheckDur
		}
		if block.ReasonIfNotCalled == "pageIndexed=false" {
			pageIndexedFalseDur += block.TotalBlockDuplicateCheckDur
		}
	}
	Debugf(
		"duplicate_check_summary total_dur=%s runtime_calls_count=%d fallback_calls_count=%d strong_snapshot_log_lines_expected_count=%d blocks=%d rows=%d",
		totalDur,
		m.RuntimeCallsCount,
		m.FallbackCallsCount,
		m.StrongSnapshotLogLinesExpectedCount,
		len(m.Blocks),
		m.Rows,
	)
	if !IsImportProfileEnabled() {
		return
	}
	Debugf(
		"duplicate_check_breakdown group_book_page_dur=%s dedup_within_only_dur=%s build_keys_dur=%s page_indexed_false_dur=%s runtime_calls_count=%d fallback_calls_count=%d blocks=%d rows=%d",
		pageLookupDur,
		dedupWithinOnlyDur,
		buildKeysDur,
		pageIndexedFalseDur,
		m.RuntimeCallsCount,
		m.FallbackCallsCount,
		len(m.Blocks),
		m.Rows,
	)
	sort.Slice(blocks, func(i, j int) bool {
		if blocks[i].TotalBlockDuplicateCheckDur == blocks[j].TotalBlockDuplicateCheckDur {
			return blocks[i].BookID < blocks[j].BookID
		}
		return blocks[i].TotalBlockDuplicateCheckDur > blocks[j].TotalBlockDuplicateCheckDur
	})
	limit := 10
	if len(blocks) < limit {
		limit = len(blocks)
	}
	for i := 0; i < limit; i++ {
		block := blocks[i]
		Debugf(
			"duplicate_check_top_slow_block rank=%d book_id=%d incoming_rows_count=%d reason=%q page_key=%q tipus_acte=%q build_match_key_dur=%s existing_load_dur=%s compare_dur=%s total_block_duplicate_check_dur=%s",
			i+1,
			block.BookID,
			block.IncomingRowsCount,
			block.ReasonIfNotCalled,
			block.PageKey,
			block.TipusActe,
			block.BuildMatchKeyDur,
			block.ExistingLoadDur,
			block.CompareDur,
			block.TotalBlockDuplicateCheckDur,
		)
	}
}

type templateMatchBuildCache struct {
	normalizedParts map[string]string
	loweredParts    map[string]string
	personKeys      map[string]string
}

type templateDedupKeyProfileMetrics struct {
	BookID                  int
	Rows                    int
	MatchKeysGenerated      int
	PeopleProcessed         int
	AtributsProcessed       int
	NonEmptyFields          int
	EmptyFields             int
	NormalizationsTotal     int
	NormalizationsUnique    int
	FieldExtractDur         time.Duration
	StringNormalizeDur      time.Duration
	DateParseDur            time.Duration
	PeopleBuildDur          time.Duration
	AttrsPeopleIterateDur   time.Duration
	FinalAssemblyDur        time.Duration
	TotalBuildMatchKeyDur   time.Duration
	FieldsProcessed         int
	NormalizeCacheHits      int
	NormalizeCacheMisses    int
	RepeatedRowBuilds       int
	RepeatedSubcomponentHit int
	MaxNormalizedLen        int
	TotalNormalizedLen      int
}

type templateDedupKeyProfiler struct {
	enabledBooks map[int]struct{}
	byBook       map[int]*templateDedupKeyProfileMetrics
}

func newTemplateDedupKeyProfiler(bookIDs ...int) *templateDedupKeyProfiler {
	profiler := &templateDedupKeyProfiler{
		enabledBooks: map[int]struct{}{},
		byBook:       map[int]*templateDedupKeyProfileMetrics{},
	}
	for _, bookID := range bookIDs {
		if bookID > 0 {
			profiler.enabledBooks[bookID] = struct{}{}
		}
	}
	return profiler
}

func (p *templateDedupKeyProfiler) metricsForBook(bookID int) *templateDedupKeyProfileMetrics {
	if p == nil {
		return nil
	}
	if len(p.enabledBooks) > 0 {
		if _, ok := p.enabledBooks[bookID]; !ok {
			return nil
		}
	}
	if bookID <= 0 {
		return nil
	}
	metrics := p.byBook[bookID]
	if metrics == nil {
		metrics = &templateDedupKeyProfileMetrics{BookID: bookID}
		p.byBook[bookID] = metrics
	}
	return metrics
}

func (p *templateDedupKeyProfiler) logDebug(bookIDs []int) {
	if p == nil || len(p.byBook) == 0 || len(bookIDs) == 0 {
		return
	}
	for _, bookID := range bookIDs {
		metrics := p.byBook[bookID]
		if metrics == nil {
			continue
		}
		avgNormalizedLen := 0.0
		if metrics.NonEmptyFields > 0 {
			avgNormalizedLen = float64(metrics.TotalNormalizedLen) / float64(metrics.NonEmptyFields)
		}
		Debugf(
			"duplicate_check_dedup_key_profile book_id=%d rows=%d match_keys_generated=%d people_processed=%d atributs_processed=%d nonempty_fields=%d empty_fields=%d normalizations_total=%d normalizations_unique=%d field_extract_dur=%s string_normalize_dur=%s date_parse_dur=%s people_build_dur=%s iterate_atributs_persones_dur=%s final_key_assembly_dur=%s total_build_match_key_dur=%s fields_processed=%d normalize_cache_hits=%d normalize_cache_misses=%d repeated_row_builds=%d repeated_subcomponent_hits=%d max_normalized_len=%d avg_normalized_len=%.2f",
			metrics.BookID,
			metrics.Rows,
			metrics.MatchKeysGenerated,
			metrics.PeopleProcessed,
			metrics.AtributsProcessed,
			metrics.NonEmptyFields,
			metrics.EmptyFields,
			metrics.NormalizationsTotal,
			metrics.NormalizationsUnique,
			metrics.FieldExtractDur,
			metrics.StringNormalizeDur,
			metrics.DateParseDur,
			metrics.PeopleBuildDur,
			metrics.AttrsPeopleIterateDur,
			metrics.FinalAssemblyDur,
			metrics.TotalBuildMatchKeyDur,
			metrics.FieldsProcessed,
			metrics.NormalizeCacheHits,
			metrics.NormalizeCacheMisses,
			metrics.RepeatedRowBuilds,
			metrics.RepeatedSubcomponentHit,
			metrics.MaxNormalizedLen,
			avgNormalizedLen,
		)
	}
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

func newTemplateDedupKeyPlan(rowPlan *templateRowContextPlan, policies templatePolicies) *templateDedupKeyPlan {
	if rowPlan == nil || len(policies.DedupKeyFields) == 0 {
		return nil
	}
	plan := &templateDedupKeyPlan{
		Strategy: policies.DedupKeyStrategy,
		Fields:   make([]templateDedupKeyFieldPlan, 0, len(policies.DedupKeyFields)),
	}
	for _, key := range policies.DedupKeyFields {
		if key == "" {
			continue
		}
		norm := normalizeCSVHeader(key)
		field := templateDedupKeyFieldPlan{
			RawKey:  key,
			NormKey: norm,
			Source:  templateDedupKeyFieldSourceMapped,
			Index:   -1,
		}
		if idx, ok := rowPlan.ColumnRefs[norm]; ok {
			field.Source = templateDedupKeyFieldSourceColumn
			field.Index = idx
		} else if idx, ok := rowPlan.HeaderRefs[norm]; ok {
			field.Source = templateDedupKeyFieldSourceHeader
			field.Index = idx
		}
		plan.Fields = append(plan.Fields, field)
	}
	if len(plan.Fields) == 0 {
		return nil
	}
	return plan
}

func (a *App) RunCSVTemplateImport(template *db.CSVImportTemplate, reader io.Reader, sep rune, userID int, ctx importContext, fixedBookID int) csvImportResult {
	start := time.Now()
	if IsPostgresStagingProfileEnabled() {
		db.ResetPostgresTemplateImportStagingProfile()
	}
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
	dedupKeyPlan := newTemplateDedupKeyPlan(rowContextPlan, model.Policies)

	resolveStart := time.Now()
	bookLookup := a.prepareBookLookups(model, ctx, fixedBookID)
	result.Debug.addResolve(time.Since(resolveStart))
	if fixedBookID > 0 {
		if _, ok := bookLookup.byID[fixedBookID]; !ok {
			result.Failed = 1
			result.Errors = append(result.Errors, importErrorEntry{Row: 0, Reason: "book_not_found", Fields: map[string]string{"fixed_book_id": strconv.Itoa(fixedBookID)}})
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
	duplicateCheckRun := newTemplateDuplicateCheckRunMetrics(
		result.Debug.Enabled && strings.EqualFold(importRuntime.Engine(), "postgres"),
		importRuntime.Engine(),
		fmt.Sprintf("%T", importRuntime),
		result.Debug.Model,
		result.Debug.Scope,
		template.ID,
		len(bookLookup.byID),
	)
	duplicateCheckRun.logStart()
	postgresDebug := result.Debug.Enabled && strings.EqualFold(importRuntime.Engine(), "postgres")
	importProfileDebug := postgresDebug && IsImportProfileEnabled()
	var writePrepareBreakdown *templateWritePrepareBreakdown
	var importPhaseGaps *templateImportPhaseGapMetrics
	if importProfileDebug {
		writePrepareBreakdown = &templateWritePrepareBreakdown{}
		importPhaseGaps = &templateImportPhaseGapMetrics{}
		result.WritePrepareBreakdown = writePrepareBreakdown
		result.ImportPhaseGaps = importPhaseGaps
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
		bookID, bookInfo, bookErr := resolveTemplateBookID(model, rowCtx, bookLookup)
		result.Debug.addResolve(time.Since(resolveStart))
		if bookErr != nil {
			result.Failed++
			result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: bookErr.Code, Fields: bookErr.Fields(rowNum)})
			continue
		}

		t := db.TranscripcioRaw{
			LlibreID:       bookID,
			ModeracioEstat: "pendent",
			CreatedBy:      sql.NullInt64{Int64: int64(userID), Valid: true},
		}
		applyBaseDefaults(&t, model.BaseDefaults)
		enforceTemplateImportedPending(&t, userID)
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
		parseColumnsEnd := time.Now()
		lastDuplicateEnd := time.Time{}

		if model.Policies.DedupWithin && len(model.Policies.DedupKeyFields) > 0 {
			duplicateStart := time.Now()
			block := duplicateCheckRun.block(
				"dedup|"+strconv.Itoa(bookID)+"|"+strings.Join(model.Policies.DedupKeyFields, ","),
				bookID,
				"",
				"",
				existingSnapshotMaxID,
			)
			if block != nil {
				block.PageIndexed = false
				block.PageResolverUsed = false
				block.StrongMatchEnabled = false
				block.ReasonIfNotCalled = "dedup_within_only"
			}
			buildStart := time.Now()
			key := buildTemplateDedupKeyWithPlan(matchBuildCache, dedupKeyPlan, rowCtx, mappedValues, nil)
			buildDur := time.Since(buildStart)
			if block != nil {
				block.BuildMatchKeyDur += buildDur
			}
			if key != "" {
				compareStart := time.Now()
				if model.Policies.DedupAddRowIndexWhenPrincipalMissing && !principalPersonHasName(persones, model.Policies.PrincipalRoles) {
					key += "|row:" + strconv.Itoa(rowNum)
				}
				if firstRow, ok := seen[key]; ok {
					if block != nil {
						block.CompareDur += time.Since(compareStart)
						block.TotalBlockDuplicateCheckDur += time.Since(duplicateStart)
					}
					result.Debug.addWriteDuplicateCheck(time.Since(duplicateStart))
					result.Failed++
					fields := map[string]string{"duplicate_row": strconv.Itoa(firstRow)}
					result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: "registre duplicat", Fields: fields})
					continue
				}
				if block != nil {
					block.CompareDur += time.Since(compareStart)
				}
				seen[key] = rowNum
			}
			if block != nil {
				block.TotalBlockDuplicateCheckDur += time.Since(duplicateStart)
			}
			result.Debug.addWriteDuplicateCheck(time.Since(duplicateStart))
			lastDuplicateEnd = time.Now()
		}

		matchKey := ""
		matchContextKey := ""
		matchMode := model.Policies.MergeMode
		duplicateBlock := (*templateDuplicateCheckBlockMetrics)(nil)
		duplicateBlockKey := ""
		switch matchMode {
		case "by_strong_signature_if_page_indexed":
			pageLookupStart := time.Now()
			pageKey, pageIndexed := a.templateIndexedPageKeyWithResolver(pageResolver, bookID, &t, atributs)
			pageLookupDur := time.Since(pageLookupStart)
			result.Debug.addWritePageLookup(pageLookupDur)
			duplicateBlockKey = "strong|" + strconv.Itoa(bookID) + "|" + pageKey + "|" + strings.TrimSpace(t.TipusActe)
			duplicateBlock = duplicateCheckRun.block(duplicateBlockKey, bookID, pageKey, strings.TrimSpace(t.TipusActe), existingSnapshotMaxID)
			if duplicateBlock != nil {
				duplicateBlock.PageIndexed = pageIndexed
				duplicateBlock.PageResolverUsed = true
				duplicateBlock.StrongMatchEnabled = true
				duplicateBlock.PageLookupDur += pageLookupDur
				duplicateBlock.TotalBlockDuplicateCheckDur += pageLookupDur
			}
			if pageIndexed {
				duplicateStart := time.Now()
				buildStart := time.Now()
				matchKey = buildTemplateStrongMatchKeyWithCache(matchBuildCache, &t, persones, atributs, model.Policies)
				if duplicateBlock != nil {
					duplicateBlock.BuildMatchKeyDur += time.Since(buildStart)
				}
				if matchKey != "" {
					matchContextKey = "strong|" + strconv.Itoa(bookID) + "|" + normalizeTemplateMatchPartWithCache(matchBuildCache, pageKey) + "|" + normalizeTemplateMatchPartWithCache(matchBuildCache, t.TipusActe)
					if model.Policies.AvoidDuplicatePrincipal {
						compareStart := time.Now()
						if firstRow, ok := templateSeenMatchRow(seenMatchByContext, matchContextKey, matchKey); ok {
							if duplicateBlock != nil {
								duplicateBlock.CompareDur += time.Since(compareStart)
								duplicateBlock.TotalBlockDuplicateCheckDur += time.Since(duplicateStart)
							}
							result.Debug.addWriteDuplicateCheck(time.Since(duplicateStart))
							result.Failed++
							fields := map[string]string{"duplicate_row": strconv.Itoa(firstRow)}
							result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: "registre duplicat", Fields: fields})
							continue
						}
						if duplicateBlock != nil {
							duplicateBlock.CompareDur += time.Since(compareStart)
						}
					}
				} else if duplicateBlock != nil {
					duplicateCheckRun.logSkipRuntimeLoadStrongCandidates(duplicateBlock, `matchKey=""`, importRuntime, "")
				}
				if duplicateBlock != nil {
					duplicateBlock.TotalBlockDuplicateCheckDur += time.Since(duplicateStart)
				}
				result.Debug.addWriteDuplicateCheck(time.Since(duplicateStart))
				lastDuplicateEnd = time.Now()
			} else if duplicateBlock != nil {
				duplicateCheckRun.logSkipRuntimeLoadStrongCandidates(duplicateBlock, "pageIndexed=false", importRuntime, "")
				lastDuplicateEnd = time.Now()
			}
		case "by_principal_person_if_book_indexed":
			duplicateBlockKey = "principal|" + strconv.Itoa(bookID)
			duplicateBlock = duplicateCheckRun.block(duplicateBlockKey, bookID, "", "", existingSnapshotMaxID)
			if duplicateBlock != nil {
				duplicateBlock.PageIndexed = bookInfo.Indexed
				duplicateBlock.PageResolverUsed = false
				duplicateBlock.StrongMatchEnabled = false
				duplicateBlock.FallbackPath = "principal_match_mode"
			}
			if !bookInfo.Indexed {
				if duplicateBlock != nil {
					duplicateCheckRun.logSkipRuntimeLoadStrongCandidates(duplicateBlock, "bookInfo.Indexed=false", importRuntime, "principal_match_mode")
				}
				break
			}
			duplicateStart := time.Now()
			buildStart := time.Now()
			matchKey = principalPersonKey(persones, model.Policies.PrincipalRoles)
			if duplicateBlock != nil {
				duplicateBlock.BuildMatchKeyDur += time.Since(buildStart)
			}
			if matchKey != "" && model.Policies.AvoidDuplicatePrincipal {
				matchContextKey = "principal|" + strconv.Itoa(bookID)
				compareStart := time.Now()
				if firstRow, ok := templateSeenMatchRow(seenMatchByContext, matchContextKey, matchKey); ok {
					if duplicateBlock != nil {
						duplicateBlock.CompareDur += time.Since(compareStart)
						duplicateBlock.TotalBlockDuplicateCheckDur += time.Since(duplicateStart)
					}
					result.Debug.addWriteDuplicateCheck(time.Since(duplicateStart))
					result.Failed++
					fields := map[string]string{"duplicate_row": strconv.Itoa(firstRow)}
					result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: "registre duplicat", Fields: fields})
					continue
				}
				if duplicateBlock != nil {
					duplicateBlock.CompareDur += time.Since(compareStart)
				}
			}
			if matchKey != "" && matchContextKey == "" {
				matchContextKey = "principal|" + strconv.Itoa(bookID)
			} else if matchKey == "" && duplicateBlock != nil {
				duplicateCheckRun.logSkipRuntimeLoadStrongCandidates(duplicateBlock, `matchKey=""`, importRuntime, "principal_match_mode")
			}
			if duplicateBlock != nil {
				duplicateBlock.TotalBlockDuplicateCheckDur += time.Since(duplicateStart)
			}
			result.Debug.addWriteDuplicateCheck(time.Since(duplicateStart))
			lastDuplicateEnd = time.Now()
		}

		if matchKey != "" && matchContextKey != "" {
			existingMap := existingByContext[matchContextKey]
			if existingMap == nil {
				resolveStart = time.Now()
				if matchMode == "by_strong_signature_if_page_indexed" {
					if duplicateBlock != nil {
						duplicateBlock.RuntimeLoadCandidatesCalled = true
						duplicateCheckRun.RuntimeCallsCount++
						duplicateCheckRun.StrongSnapshotLogLinesExpectedCount++
						Debugf(
							"duplicate_check calling_runtime_load_strong_candidates book_id=%d page_key=%q tipus_acte=%q snapshot_max_id=%d",
							bookID,
							duplicateBlock.PageKey,
							duplicateBlock.TipusActe,
							existingSnapshotMaxID,
						)
					}
					existingLoadStart := time.Now()
					existingMap = a.loadExistingByStrongMatchWithPageResolverSnapshot(importRuntime, pageResolver, bookID, &t, atributs, model.Policies, existingSnapshotMaxID)
					if duplicateBlock != nil {
						existingLoadDur := time.Since(existingLoadStart)
						duplicateBlock.ExistingLoadDur += existingLoadDur
						duplicateBlock.TotalBlockDuplicateCheckDur += existingLoadDur
					}
				} else {
					existingLoadStart := time.Now()
					existingMap = a.loadExistingByPrincipal(importRuntime, bookID, model.Policies.PrincipalRoles, existingSnapshotMaxID)
					existingLoadDur := time.Since(existingLoadStart)
					if duplicateBlock != nil {
						duplicateBlock.ExistingLoadDur += existingLoadDur
						duplicateBlock.TotalBlockDuplicateCheckDur += existingLoadDur
					}
					duplicateCheckRun.logFallbackPath("principal_match_mode", existingLoadDur, 1, bookID)
				}
				result.Debug.addResolve(time.Since(resolveStart))
				existingByContext[matchContextKey] = existingMap
			} else if duplicateBlock != nil && !duplicateBlock.RuntimeLoadCandidatesCalled {
				duplicateBlock.ReasonIfNotCalled = "existing_map_cached"
			}
			compareStart := time.Now()
			if existingID, ok := existingMap[matchKey]; ok {
				if duplicateBlock != nil {
					duplicateBlock.CompareDur += time.Since(compareStart)
				}
				writeStart := time.Now()
				outcome := a.mergeTemplateRow(existingID, &t, persones, atributs, model.Policies)
				result.Debug.addWrite(time.Since(writeStart))
				if outcome.Accepted {
					result.Updated++
					result.markBook(bookID)
					result.addUpdatedRegistre(existingID)
					if outcome.ChangeID > 0 {
						result.addChangeProposal(outcome.ChangeID)
					}
					if matchContextKey != "" && matchKey != "" {
						templateRememberSeenMatch(seenMatchByContext, matchContextKey, matchKey, rowNum)
					}
					if outcome.Changed {
						existingMap[matchKey] = existingID
					}
					continue
				}
			} else if duplicateBlock != nil {
				duplicateBlock.CompareDur += time.Since(compareStart)
			}
		}

		writePrepareStart := time.Now()
		if importPhaseGaps != nil {
			importPhaseGaps.ParseToWritePrepareGap += writePrepareStart.Sub(parseColumnsEnd)
			if !lastDuplicateEnd.IsZero() {
				importPhaseGaps.DuplicateBeforeWritePrepareCount++
			}
		}
		t.DataActeEstat = normalizeDataActeEstat(t.DataActeEstat)
		if t.DataActeEstat == "" {
			t.DataActeEstat = "clar"
		}
		enforceTemplateImportedPending(&t, userID)
		if !validTipusActe(t.TipusActe) {
			result.Debug.addWritePrepare(time.Since(writePrepareStart))
			result.Failed++
			result.Errors = append(result.Errors, importErrorEntry{Row: rowNum, Reason: "tipus_acte invàlid"})
			continue
		}
		result.Debug.addWritePrepare(time.Since(writePrepareStart))
		personaResolveStart := time.Now()
		if importPhaseGaps != nil && !lastDuplicateEnd.IsZero() {
			importPhaseGaps.DuplicateCheckToInsertsGap += personaResolveStart.Sub(lastDuplicateEnd)
		}
		preallocStart := time.Now()
		personesRows := make([]db.TranscripcioPersonaRaw, 0, len(persones))
		atributRows := make([]db.TranscripcioAtributRaw, 0, len(atributs))
		if writePrepareBreakdown != nil {
			writePrepareBreakdown.PreallocDur += time.Since(preallocStart)
		}
		personesBuildStart := time.Now()
		for _, p := range persones {
			if isEmptyPerson(p) {
				continue
			}
			personesRows = append(personesRows, *p)
		}
		atributsBuildStart := time.Now()
		for _, attr := range atributs {
			if isEmptyAttr(attr) {
				continue
			}
			atributRows = append(atributRows, *attr)
		}
		if writePrepareBreakdown != nil {
			writePrepareBreakdown.BuildPersonesBatchDur += atributsBuildStart.Sub(personesBuildStart)
			writePrepareBreakdown.BuildLinksBatchDur += time.Since(atributsBuildStart)
			writePrepareBreakdown.PrepareMapsSlicesDur += time.Since(personaResolveStart)
			writePrepareBreakdown.TranscripcionsCount++
			writePrepareBreakdown.PersonesCount += len(personesRows)
			writePrepareBreakdown.LinksCount += len(atributRows)
			writePrepareBreakdown.AtributsCount += len(atributRows)
		}
		result.Debug.addWritePersonaResolve(time.Since(personaResolveStart))
		transBatchStart := time.Now()
		pendingCreates = append(pendingCreates, templatePendingCreate{
			RowNum: rowNum,
			BookID: bookID,
			Bundle: db.TranscripcioRawImportBundle{
				Transcripcio: t,
				Persones:     personesRows,
				Atributs:     atributRows,
			},
		})
		if writePrepareBreakdown != nil {
			writePrepareBreakdown.BuildTranscripcionsBatchDur += time.Since(transBatchStart)
		}
		if matchContextKey != "" && matchKey != "" {
			templateRememberSeenMatch(seenMatchByContext, matchContextKey, matchKey, rowNum)
		}
	}
	plan := buildTemplateImportPlan(pendingCreates)
	persister := newTemplateImportPersister(importRuntime)
	persister.Persist(plan, TemplateImportPersistOptions{
		App:     a,
		Result:  &result,
		Runtime: importRuntime,
	})
	a.finalizeTemplateImportSideEffects(ctx, template, model, headers, userID, fixedBookID, start, &result)
	duplicateCheckRun.logBlocksAndSummary(result.Debug.Rows)
	result.WriteCompletedAt = time.Now()
	result.Debug.finalize(len(result.BookIDs), time.Since(start))
	return result
}

func (a *App) flushTemplatePendingCreates(pending []templatePendingCreate, result *csvImportResult, runtime db.TemplateImportRuntime) []templatePendingCreate {
	if len(pending) == 0 || result == nil {
		return pending[:0]
	}
	if result.WritePrepareBreakdown != nil {
		result.WritePrepareBreakdown.Batches++
	}
	if runtime != nil {
		preallocStart := time.Now()
		bundles := make([]db.TranscripcioRawImportBundle, len(pending))
		if result.WritePrepareBreakdown != nil {
			result.WritePrepareBreakdown.PreallocDur += time.Since(preallocStart)
		}
		for i := range pending {
			transStart := time.Now()
			bundles[i].Transcripcio = pending[i].Bundle.Transcripcio
			if result.WritePrepareBreakdown != nil {
				result.WritePrepareBreakdown.BuildTranscripcionsBatchDur += time.Since(transStart)
			}
			personesStart := time.Now()
			bundles[i].Persones = pending[i].Bundle.Persones
			if result.WritePrepareBreakdown != nil {
				result.WritePrepareBreakdown.BuildPersonesBatchDur += time.Since(personesStart)
			}
			linksStart := time.Now()
			bundles[i].Atributs = pending[i].Bundle.Atributs
			if result.WritePrepareBreakdown != nil {
				result.WritePrepareBreakdown.BuildLinksBatchDur += time.Since(linksStart)
			}
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
				result.addCreatedRegistre(bulkResult.IDs[i])
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
	result.addCreatedRegistre(createResult.IDs[0])
}

func enforceTemplateImportedPending(row *db.TranscripcioRaw, userID int) {
	if row == nil {
		return
	}
	row.ModeracioEstat = "pendent"
	row.ModeratedBy = sql.NullInt64{}
	row.ModeratedAt = sql.NullTime{}
	row.ModeracioMotiu = ""
	if userID > 0 && !row.CreatedBy.Valid {
		row.CreatedBy = sql.NullInt64{Int64: int64(userID), Valid: true}
	}
}

func (a *App) finalizeTemplateImportSideEffects(ctx importContext, template *db.CSVImportTemplate, model *templateImportModel, headers []string, userID int, fixedBookID int, startedAt time.Time, result *csvImportResult) {
	if a == nil || a.DB == nil || result == nil || template == nil {
		return
	}
	result.PendingActivityCount = a.recordTemplateImportPendingActivities(ctx, userID, result)
	a.logTemplateImportRun(ctx, template, model, headers, userID, fixedBookID, startedAt, result)
}

func (a *App) recordTemplateImportPendingActivities(ctx importContext, userID int, result *csvImportResult) int {
	if a == nil || a.DB == nil || result == nil || userID <= 0 {
		return 0
	}
	objectIDs := append([]int{}, result.CreatedRegistreIDs...)
	objectIDs = append(objectIDs, result.UpdatedRegistreIDs...)
	objectIDs = uniquePositiveInts(objectIDs)
	if len(objectIDs) == 0 {
		return 0
	}
	existingActivities, err := a.DB.ListActivityByObjects("registre", objectIDs, "pendent")
	existingSet := map[int]struct{}{}
	if err != nil {
		Errorf("template import pending activity lookup failed: %v", err)
		return 0
	}
	for _, act := range existingActivities {
		if act.ObjectID.Valid && act.ObjectID.Int64 > 0 {
			existingSet[int(act.ObjectID.Int64)] = struct{}{}
		}
	}
	rows := make([]db.UserActivity, 0, len(objectIDs))
	now := time.Now()
	for _, objectID := range objectIDs {
		if _, exists := existingSet[objectID]; exists {
			continue
		}
		rows = append(rows, db.UserActivity{
			UserID:     userID,
			Action:     "import_template",
			ObjectType: "registre",
			ObjectID:   sql.NullInt64{Int64: int64(objectID), Valid: true},
			Status:     "pendent",
			Details:    "template_import",
			CreatedAt:  now,
		})
	}
	if len(rows) == 0 {
		return 0
	}
	insertCtx := ctx.RequestContext()
	inserted := 0
	_, err = a.DB.BulkInsertUserActivities(insertCtx, rows)
	if err != nil {
		for i := range rows {
			if _, insertErr := a.DB.InsertUserActivity(&rows[i]); insertErr == nil {
				inserted++
			}
		}
	} else {
		inserted = len(rows)
	}
	if inserted <= 0 {
		return 0
	}
	a.EvaluateAchievementsForUser(insertCtx, userID, AchievementTrigger{CreatedAt: now})
	a.logAntiAbuseSignals(userID, now)
	return inserted
}

func (a *App) logTemplateImportRun(ctx importContext, template *db.CSVImportTemplate, model *templateImportModel, headers []string, userID int, fixedBookID int, startedAt time.Time, result *csvImportResult) {
	if a == nil || a.DB == nil || result == nil || template == nil {
		return
	}
	status := adminImportStatusOK
	if result.Failed > 0 {
		status = adminImportStatusError
	}
	errorsByReason := csvImportErrorsByReason(result.Errors)
	payload := map[string]interface{}{
		"import_type":   "transcripcions_templates",
		"module":        "templates",
		"template_id":   template.ID,
		"template_name": strings.TrimSpace(template.Name),
		"record_type":   templateImportRecordType(model),
		"headers":       append([]string(nil), headers...),
		"row_count":     result.Debug.Rows,
		"book_ids":      sortedKeysFromIntSet(result.BookIDs),
	}
	if fixedBookID > 0 {
		payload["scope"] = "book"
		payload["fixed_book_id"] = fixedBookID
	} else {
		payload["scope"] = "global"
	}
	if ctx.MunicipiID > 0 {
		payload["municipi_id"] = ctx.MunicipiID
	}
	if ctx.ArxiuID > 0 {
		payload["arxiu_id"] = ctx.ArxiuID
	}
	resultMap := map[string]interface{}{
		"status":                 status,
		"created":                result.Created,
		"updated":                result.Updated,
		"failed":                 result.Failed,
		"created_target_count":   len(uniquePositiveInts(result.CreatedRegistreIDs)),
		"updated_target_count":   len(uniquePositiveInts(result.UpdatedRegistreIDs)),
		"pending_activity_count": result.PendingActivityCount,
		"errors_by_reason":       errorsByReason,
	}
	targets, createdTargets, updatedTargets := templateImportAdminTargets(template.ID, result)
	detail := &adminImportJobDetail{
		Payload:       payload,
		Result:        resultMap,
		Targets:       targets,
		ProgressTotal: templateImportMaxInt(result.Debug.Rows, result.Created+result.Updated+result.Failed),
		ProgressDone:  result.Created + result.Updated,
		StartedAt:     startedAt,
		FinishedAt:    time.Now(),
	}
	logResult := a.logAdminImportRunDetailedResult(ctx.Request, "transcripcions_templates", status, userID, detail)
	result.ImportRunID = logResult.ImportRunID
	result.AdminJobID = logResult.AdminJobID
	result.AuditID = logResult.AuditID
	result.CreatedTargetCount = createdTargets
	result.UpdatedTargetCount = updatedTargets
}

func templateImportRecordType(model *templateImportModel) string {
	if model == nil {
		return "transcripcions_raw"
	}
	if strings.TrimSpace(model.RecordType) != "" {
		return strings.TrimSpace(model.RecordType)
	}
	return "transcripcions_raw"
}

func csvImportErrorsByReason(entries []importErrorEntry) map[string]int {
	out := map[string]int{}
	for _, entry := range entries {
		key := strings.TrimSpace(entry.Reason)
		if key == "" {
			key = "unknown"
		}
		out[key]++
	}
	return out
}

func templateImportAdminTargets(templateID int, result *csvImportResult) ([]db.AdminJobTarget, int, int) {
	if result == nil {
		return nil, 0, 0
	}
	targets := make([]db.AdminJobTarget, 0, 1+len(result.BookIDs)+len(result.CreatedRegistreIDs)+len(result.UpdatedRegistreIDs)+len(result.ChangeProposalIDs))
	seen := map[string]struct{}{}
	appendTarget := func(objectType string, objectID int) {
		if strings.TrimSpace(objectType) == "" || objectID <= 0 {
			return
		}
		key := objectType + ":" + strconv.Itoa(objectID)
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		targets = append(targets, db.AdminJobTarget{ObjectType: objectType, ObjectID: objectID})
	}
	if templateID > 0 {
		appendTarget("csv_import_template", templateID)
	}
	for _, bookID := range sortedKeysFromIntSet(result.BookIDs) {
		appendTarget("llibre", bookID)
	}
	createdTargets := 0
	for _, id := range uniquePositiveInts(result.CreatedRegistreIDs) {
		appendTarget("registre", id)
		createdTargets++
	}
	updatedTargets := 0
	for _, id := range uniquePositiveInts(result.UpdatedRegistreIDs) {
		appendTarget("registre", id)
		updatedTargets++
	}
	for _, id := range uniquePositiveInts(result.ChangeProposalIDs) {
		appendTarget("registre_canvi", id)
	}
	return targets, createdTargets, updatedTargets
}

func sortedKeysFromIntSet(values map[int]struct{}) []int {
	if len(values) == 0 {
		return nil
	}
	out := make([]int, 0, len(values))
	for id := range values {
		if id > 0 {
			out = append(out, id)
		}
	}
	sort.Ints(out)
	return out
}

func uniquePositiveInts(values []int) []int {
	if len(values) == 0 {
		return nil
	}
	seen := map[int]struct{}{}
	out := make([]int, 0, len(values))
	for _, id := range values {
		if id <= 0 {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	sort.Ints(out)
	return out
}

func templateImportMaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
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
		BookMode:             "llibre_id",
		BookColumn:           "llibre_id",
		BookChronologyColumn: "llibre_id",
		AmbiguityPolicy:      "fail",
		BaseDefaults:         map[string]string{},
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
			model.BookChronologyColumn = v
		}
		model.BookCodeColumn = asString(book["book_code_column"])
		model.BookSourceSystem = asString(book["source_system"])
		model.BookSourceColumn = asString(book["source_system_column"])
		model.BookExternalIDColumn = asString(book["external_id_column"])
		model.BookExternalCodeColumn = asString(book["external_code_column"])
		model.BookArchiveCodeColumn = asString(book["archive_code_column"])
		if v := asString(book["chronology_column"]); v != "" {
			model.BookChronologyColumn = v
		}
		model.BookTypeColumn = asString(book["book_type_column"])
		model.BookTitleColumn = asString(book["title_column"])
		model.BookChurchNameColumn = asString(book["church_name_column"])
		model.BookURLColumn = asString(book["url_column"])
		model.BookBaseURLColumn = asString(book["base_url_column"])
		model.BookURLOverrideColumn = asString(book["url_override_column"])
		model.MunicipalityContext = asString(book["municipality_context"])
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
			model.Policies.DedupKeyStrategy = asString(dedup["key_strategy"])
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

type templateBookResolveError struct {
	Code    string
	FieldsM map[string]string
}

func (e *templateBookResolveError) Fields(rowNum int) map[string]string {
	fields := map[string]string{}
	if e == nil {
		return fields
	}
	for key, value := range e.FieldsM {
		if strings.TrimSpace(value) == "" {
			continue
		}
		fields[key] = value
	}
	if rowNum > 0 {
		fields["row"] = strconv.Itoa(rowNum)
	}
	return fields
}

type templateScopedBook struct {
	Row   db.LlibreRow
	Info  bookInfo
	Links []db.ArxiuLlibreDetail
}

type templateBookLookupState struct {
	app             *App
	model           *templateImportModel
	ctx             importContext
	fixedBookID     int
	byID            map[int]bookInfo
	scopedBooks     []templateScopedBook
	archiveByCode   map[string]*db.Arxiu
	archiveLinks    map[int][]db.ArxiuLlibreDetail
	stableRefCache  map[string]templateResolveResult
	urlsByBook      map[int][]db.LlibreURL
	municipalityOK  bool
	municipalityErr *templateBookResolveError
	archiveOK       bool
	archiveErr      *templateBookResolveError
}

type templateResolveResult struct {
	ID   int
	Info bookInfo
	Err  *templateBookResolveError
}

func (a *App) prepareBookLookups(model *templateImportModel, ctx importContext, fixedBookID int) *templateBookLookupState {
	state := &templateBookLookupState{
		app:            a,
		model:          model,
		ctx:            ctx,
		fixedBookID:    fixedBookID,
		byID:           map[int]bookInfo{},
		archiveByCode:  map[string]*db.Arxiu{},
		archiveLinks:   map[int][]db.ArxiuLlibreDetail{},
		stableRefCache: map[string]templateResolveResult{},
		urlsByBook:     map[int][]db.LlibreURL{},
	}
	filter := db.LlibreFilter{}
	if ctx.MunicipiID != 0 {
		filter.MunicipiID = ctx.MunicipiID
	}
	if ctx.ArxiuID != 0 {
		filter.ArxiuID = ctx.ArxiuID
	}
	shouldLoadScoped := filter.MunicipiID != 0 || filter.ArxiuID != 0 || model.ScopeFilters || len(model.ContextFilters) > 0 || model.BookMode == "cronologia_lookup"
	if shouldLoadScoped {
		llibres, _ := a.DB.ListLlibres(filter)
		for _, l := range llibres {
			state.byID[l.ID] = bookInfo{ID: l.ID, Indexed: l.IndexacioCompleta}
			state.scopedBooks = append(state.scopedBooks, templateScopedBook{
				Row:  l,
				Info: bookInfo{ID: l.ID, Indexed: l.IndexacioCompleta},
			})
		}
		if len(state.scopedBooks) > 0 {
			ids := make([]int, 0, len(state.scopedBooks))
			for _, item := range state.scopedBooks {
				ids = append(ids, item.Row.ID)
			}
			if linksByBook, err := a.DB.ListLlibreArxiusByLlibreIDs(ids); err == nil {
				for i := range state.scopedBooks {
					state.scopedBooks[i].Links = linksByBook[state.scopedBooks[i].Row.ID]
				}
			}
		}
	}
	if fixedBookID > 0 {
		if _, ok := state.byID[fixedBookID]; !ok {
			if llibre, err := a.DB.GetLlibre(fixedBookID); err == nil && llibre != nil {
				state.byID[fixedBookID] = bookInfo{ID: llibre.ID, Indexed: llibre.IndexacioCompleta}
			}
		}
	}
	return state
}

func resolveTemplateBookID(model *templateImportModel, rowCtx templateRowContext, state *templateBookLookupState) (int, bookInfo, *templateBookResolveError) {
	if state == nil {
		return 0, bookInfo{}, &templateBookResolveError{Code: "book_not_found"}
	}
	if state.fixedBookID > 0 {
		info := state.byID[state.fixedBookID]
		if info.ID == 0 {
			return 0, bookInfo{}, &templateBookResolveError{Code: "book_not_found", FieldsM: map[string]string{"fixed_book_id": strconv.Itoa(state.fixedBookID)}}
		}
		if model.BookMode == "llibre_id" && model.BookColumn != "" {
			column := normalizeCSVHeader(model.BookColumn)
			val, ok := rowCtx.LookupHeaderValue(column)
			if ok {
				val = strings.TrimSpace(val)
			}
			if ok && val != "" {
				id, err := strconv.Atoi(val)
				if err != nil || id <= 0 {
					return 0, bookInfo{}, &templateBookResolveError{Code: "book_id_invalid", FieldsM: map[string]string{"column": model.BookColumn, "value": val, "llibre_id": val, "fixed_book_id": strconv.Itoa(state.fixedBookID)}}
				}
				if id != state.fixedBookID {
					return 0, bookInfo{}, &templateBookResolveError{Code: "book_id_mismatch", FieldsM: map[string]string{"column": model.BookColumn, "value": val, "fixed_book_id": strconv.Itoa(state.fixedBookID), "llibre_id": val}}
				}
			}
		}
		return state.fixedBookID, info, nil
	}
	switch strings.TrimSpace(model.BookMode) {
	case "cronologia_lookup":
		return state.resolveByChronology(rowCtx)
	case "v2_lookup":
		return state.resolveByV2(rowCtx)
	default:
		column := normalizeCSVHeader(model.BookColumn)
		raw, ok := rowCtx.LookupHeaderValue(column)
		if !ok {
			return 0, bookInfo{}, &templateBookResolveError{Code: "book_resolution_missing_column", FieldsM: map[string]string{"column": model.BookColumn}}
		}
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return 0, bookInfo{}, &templateBookResolveError{Code: "book_resolution_missing_value", FieldsM: map[string]string{"column": model.BookColumn}}
		}
		id, err := strconv.Atoi(raw)
		if err != nil || id <= 0 {
			return 0, bookInfo{}, &templateBookResolveError{Code: "book_id_invalid", FieldsM: map[string]string{"column": model.BookColumn, "value": raw, "llibre_id": raw}}
		}
		info, ok := state.byID[id]
		if !ok {
			var found bool
			info, found = state.loadBookInfo(id)
			if !found {
				return 0, bookInfo{}, &templateBookResolveError{Code: "book_not_found", FieldsM: map[string]string{"book_id": strconv.Itoa(id)}}
			}
		}
		return id, info, nil
	}
}

func (s *templateBookLookupState) resolveByChronology(rowCtx templateRowContext) (int, bookInfo, *templateBookResolveError) {
	raw := s.rowValue(rowCtx, firstNonEmpty(s.model.BookChronologyColumn, s.model.BookColumn))
	if strings.TrimSpace(raw) == "" {
		return 0, bookInfo{}, &templateBookResolveError{Code: "book_resolution_missing_column", FieldsM: map[string]string{"column": firstNonEmpty(s.model.BookChronologyColumn, s.model.BookColumn)}}
	}
	return s.resolveScopedCandidates(rowCtx, raw, func(item templateScopedBook) bool {
		return s.normalizeChronology(item.Row.Cronologia) == s.normalizeChronology(raw)
	})
}

func (s *templateBookLookupState) resolveByV2(rowCtx templateRowContext) (int, bookInfo, *templateBookResolveError) {
	if id, info, err, ok := s.resolveByBookCode(rowCtx); ok {
		return id, info, err
	}
	if id, info, err, ok := s.resolveByStableRef(rowCtx); ok {
		return id, info, err
	}
	if id, info, err, ok := s.resolveByArchiveStableRef(rowCtx); ok {
		return id, info, err
	}
	if id, info, err, ok := s.resolveByArchiveDigitalOrURL(rowCtx); ok {
		return id, info, err
	}
	if id, info, err, ok := s.resolveByChronologyWithContext(rowCtx); ok {
		return id, info, err
	}
	if id, info, err, ok := s.resolveByMetadata(rowCtx); ok {
		return id, info, err
	}
	if id, info, err, ok := s.resolveByChurchChronology(rowCtx); ok {
		return id, info, err
	}
	if id, info, err, ok := s.resolveByMunicipalityChronology(rowCtx); ok {
		return id, info, err
	}
	return 0, bookInfo{}, &templateBookResolveError{Code: "book_not_found"}
}

func (s *templateBookLookupState) resolveByBookCode(rowCtx templateRowContext) (int, bookInfo, *templateBookResolveError, bool) {
	code := strings.TrimSpace(s.rowValue(rowCtx, s.model.BookCodeColumn))
	if code == "" {
		return 0, bookInfo{}, nil, false
	}
	if len(s.scopedBooks) > 0 {
		id, info, err := s.resolveScopedCandidates(rowCtx, code, func(item templateScopedBook) bool {
			return strings.EqualFold(strings.TrimSpace(item.Row.Codi), code)
		})
		return id, info, err, true
	}
	id, info, err := s.resolveGlobalStableRef("code|"+strings.ToLower(code), db.LlibreStableRef{Codi: code}, map[string]string{"book_code": code}, "book_code_not_found")
	return id, info, err, true
}

func (s *templateBookLookupState) resolveByStableRef(rowCtx templateRowContext) (int, bookInfo, *templateBookResolveError, bool) {
	sourceSystem := s.resolveSourceSystem(rowCtx)
	externalID := strings.TrimSpace(s.rowValue(rowCtx, s.model.BookExternalIDColumn))
	externalCode := strings.TrimSpace(s.rowValue(rowCtx, s.model.BookExternalCodeColumn))
	if externalID == "" && externalCode == "" {
		return 0, bookInfo{}, nil, false
	}
	if sourceSystem == "" {
		return 0, bookInfo{}, &templateBookResolveError{Code: "external_ref_missing_source_system", FieldsM: map[string]string{"external_id": externalID, "external_code": externalCode}}, true
	}
	if len(s.scopedBooks) > 0 {
		id, info, err := s.resolveScopedCandidates(rowCtx, firstNonEmpty(externalID, externalCode), func(item templateScopedBook) bool {
			if !strings.EqualFold(strings.TrimSpace(item.Row.SourceSystem), sourceSystem) {
				return false
			}
			if externalID != "" && strings.EqualFold(strings.TrimSpace(item.Row.ExternalID), externalID) {
				return true
			}
			if externalCode != "" && strings.EqualFold(strings.TrimSpace(item.Row.ExternalCode), externalCode) {
				return true
			}
			return false
		})
		return id, info, err, true
	}
	id, info, err := s.resolveGlobalStableRef(
		"stable|"+sourceSystem+"|"+externalID+"|"+externalCode,
		db.LlibreStableRef{SourceSystem: sourceSystem, ExternalID: externalID, ExternalCode: externalCode},
		map[string]string{"source_system": sourceSystem, "external_id": externalID, "external_code": externalCode},
		"book_not_found",
	)
	return id, info, err, true
}

func (s *templateBookLookupState) resolveByArchiveStableRef(rowCtx templateRowContext) (int, bookInfo, *templateBookResolveError, bool) {
	archive, archiveErr := s.resolveArchive(rowCtx)
	sourceSystem := s.resolveSourceSystem(rowCtx)
	externalID := strings.TrimSpace(s.rowValue(rowCtx, s.model.BookExternalIDColumn))
	externalCode := strings.TrimSpace(s.rowValue(rowCtx, s.model.BookExternalCodeColumn))
	if archive == nil && strings.TrimSpace(s.rowValue(rowCtx, s.model.BookArchiveCodeColumn)) == "" && s.ctx.ArxiuID == 0 {
		return 0, bookInfo{}, nil, false
	}
	if externalID == "" && externalCode == "" {
		return 0, bookInfo{}, nil, false
	}
	if archiveErr != nil {
		return 0, bookInfo{}, archiveErr, true
	}
	if sourceSystem == "" {
		return 0, bookInfo{}, &templateBookResolveError{Code: "external_ref_missing_source_system", FieldsM: map[string]string{"archive_code": strings.TrimSpace(s.rowValue(rowCtx, s.model.BookArchiveCodeColumn)), "external_id": externalID, "external_code": externalCode}}, true
	}
	id, info, err := s.resolveArchiveCandidates(archive.ID, rowCtx, firstNonEmpty(externalID, externalCode), func(link db.ArxiuLlibreDetail) bool {
		if !strings.EqualFold(strings.TrimSpace(link.SourceSystem.String), sourceSystem) {
			return false
		}
		if externalID != "" && strings.EqualFold(strings.TrimSpace(link.ExternalID.String), externalID) {
			return true
		}
		if externalCode != "" && strings.EqualFold(strings.TrimSpace(link.ExternalCode.String), externalCode) {
			return true
		}
		return false
	})
	return id, info, err, true
}

func (s *templateBookLookupState) resolveByArchiveDigitalOrURL(rowCtx templateRowContext) (int, bookInfo, *templateBookResolveError, bool) {
	archive, archiveErr := s.resolveArchive(rowCtx)
	if archive == nil && strings.TrimSpace(s.rowValue(rowCtx, s.model.BookArchiveCodeColumn)) == "" && s.ctx.ArxiuID == 0 {
		return 0, bookInfo{}, nil, false
	}
	if archiveErr != nil {
		return 0, bookInfo{}, archiveErr, true
	}
	digitalCode := strings.TrimSpace(s.rowValue(rowCtx, s.model.BookExternalCodeColumn))
	if digitalCode != "" {
		id, info, err := s.resolveArchiveCandidates(archive.ID, rowCtx, digitalCode, func(link db.ArxiuLlibreDetail) bool {
			if strings.EqualFold(strings.TrimSpace(link.ExternalCode.String), digitalCode) {
				return true
			}
			if strings.EqualFold(strings.TrimSpace(link.Signatura.String), digitalCode) {
				return true
			}
			return false
		})
		return id, info, err, true
	}
	targetURL := firstNonEmpty(
		strings.TrimSpace(s.rowValue(rowCtx, s.model.BookURLOverrideColumn)),
		strings.TrimSpace(s.rowValue(rowCtx, s.model.BookBaseURLColumn)),
		strings.TrimSpace(s.rowValue(rowCtx, s.model.BookURLColumn)),
	)
	if targetURL == "" {
		return 0, bookInfo{}, nil, false
	}
	id, info, err := s.resolveArchiveCandidates(archive.ID, rowCtx, targetURL, func(link db.ArxiuLlibreDetail) bool {
		if normalizeTemplateURLKey(link.URLOverride.String) == normalizeTemplateURLKey(targetURL) {
			return true
		}
		urls := s.urlsForBook(link.LlibreID)
		for _, item := range urls {
			if normalizeTemplateURLKey(item.URL) == normalizeTemplateURLKey(targetURL) {
				return true
			}
		}
		book := s.bookRow(link.LlibreID)
		if book != nil && normalizeTemplateURLKey(book.URLBase) == normalizeTemplateURLKey(targetURL) {
			return true
		}
		return false
	})
	return id, info, err, true
}

func (s *templateBookLookupState) resolveByChronologyWithContext(rowCtx templateRowContext) (int, bookInfo, *templateBookResolveError, bool) {
	raw := strings.TrimSpace(s.rowValue(rowCtx, firstNonEmpty(s.model.BookChronologyColumn, s.model.BookColumn)))
	if raw == "" {
		return 0, bookInfo{}, nil, false
	}
	archive, archiveErr := s.resolveArchive(rowCtx)
	if archiveErr != nil {
		return 0, bookInfo{}, archiveErr, true
	}
	if archive == nil {
		return 0, bookInfo{}, nil, false
	}
	if err := s.validateMunicipalityContext(rowCtx); err != nil {
		return 0, bookInfo{}, err, true
	}
	id, info, err := s.resolveScopedCandidates(rowCtx, raw, func(item templateScopedBook) bool {
		if !s.matchesMunicipality(item.Row) {
			return false
		}
		if s.normalizeChronology(item.Row.Cronologia) != s.normalizeChronology(raw) {
			return false
		}
		return s.bookHasArchive(item, archive.ID)
	})
	return id, info, err, true
}

func (s *templateBookLookupState) resolveByMetadata(rowCtx templateRowContext) (int, bookInfo, *templateBookResolveError, bool) {
	title := strings.TrimSpace(s.rowValue(rowCtx, s.model.BookTitleColumn))
	bookType := strings.TrimSpace(s.rowValue(rowCtx, s.model.BookTypeColumn))
	if title == "" || bookType == "" {
		return 0, bookInfo{}, nil, false
	}
	archive, archiveErr := s.resolveArchive(rowCtx)
	if archiveErr != nil {
		return 0, bookInfo{}, archiveErr, true
	}
	if err := s.validateMunicipalityContext(rowCtx); err != nil {
		return 0, bookInfo{}, err, true
	}
	id, info, err := s.resolveScopedCandidates(rowCtx, title, func(item templateScopedBook) bool {
		if !s.matchesMunicipality(item.Row) {
			return false
		}
		if !strings.EqualFold(strings.TrimSpace(item.Row.TipusLlibre), bookType) {
			return false
		}
		if normalizeTemplateLookupToken(item.Row.Titol) != normalizeTemplateLookupToken(title) {
			return false
		}
		if archive != nil && !s.bookHasArchive(item, archive.ID) {
			return false
		}
		return true
	})
	return id, info, err, true
}

func (s *templateBookLookupState) resolveByChurchChronology(rowCtx templateRowContext) (int, bookInfo, *templateBookResolveError, bool) {
	church := strings.TrimSpace(s.rowValue(rowCtx, s.model.BookChurchNameColumn))
	bookType := strings.TrimSpace(s.rowValue(rowCtx, s.model.BookTypeColumn))
	chronology := strings.TrimSpace(s.rowValue(rowCtx, firstNonEmpty(s.model.BookChronologyColumn, s.model.BookColumn)))
	if church == "" || bookType == "" || chronology == "" {
		return 0, bookInfo{}, nil, false
	}
	archive, archiveErr := s.resolveArchive(rowCtx)
	if archiveErr != nil {
		return 0, bookInfo{}, archiveErr, true
	}
	if err := s.validateMunicipalityContext(rowCtx); err != nil {
		return 0, bookInfo{}, err, true
	}
	id, info, err := s.resolveScopedCandidates(rowCtx, chronology, func(item templateScopedBook) bool {
		if !s.matchesMunicipality(item.Row) {
			return false
		}
		if !strings.EqualFold(strings.TrimSpace(item.Row.TipusLlibre), bookType) {
			return false
		}
		if s.normalizeChronology(item.Row.Cronologia) != s.normalizeChronology(chronology) {
			return false
		}
		if normalizeTemplateLookupToken(item.Row.NomEsglesia) != normalizeTemplateLookupToken(church) {
			return false
		}
		if archive != nil && !s.bookHasArchive(item, archive.ID) {
			return false
		}
		return true
	})
	return id, info, err, true
}

func (s *templateBookLookupState) resolveByMunicipalityChronology(rowCtx templateRowContext) (int, bookInfo, *templateBookResolveError, bool) {
	chronology := strings.TrimSpace(s.rowValue(rowCtx, firstNonEmpty(s.model.BookChronologyColumn, s.model.BookColumn)))
	if chronology == "" {
		return 0, bookInfo{}, nil, false
	}
	if err := s.validateMunicipalityContext(rowCtx); err != nil {
		return 0, bookInfo{}, err, true
	}
	id, info, err := s.resolveScopedCandidates(rowCtx, chronology, func(item templateScopedBook) bool {
		return s.matchesMunicipality(item.Row) && s.normalizeChronology(item.Row.Cronologia) == s.normalizeChronology(chronology)
	})
	return id, info, err, true
}

func (s *templateBookLookupState) resolveScopedCandidates(rowCtx templateRowContext, raw string, match func(item templateScopedBook) bool) (int, bookInfo, *templateBookResolveError) {
	candidates := make([]templateScopedBook, 0, 2)
	for _, item := range s.scopedBooks {
		if match(item) {
			candidates = append(candidates, item)
		}
	}
	fields := s.rowDebugFields(rowCtx)
	fields["book_value"] = raw
	if len(candidates) == 0 {
		return 0, bookInfo{}, &templateBookResolveError{Code: "book_not_found", FieldsM: fields}
	}
	if len(candidates) > 1 {
		return 0, bookInfo{}, &templateBookResolveError{Code: "book_ambiguous", FieldsM: fields}
	}
	return candidates[0].Row.ID, candidates[0].Info, nil
}

func (s *templateBookLookupState) resolveGlobalStableRef(cacheKey string, ref db.LlibreStableRef, fields map[string]string, notFoundCode string) (int, bookInfo, *templateBookResolveError) {
	if cached, ok := s.stableRefCache[cacheKey]; ok {
		return cached.ID, cached.Info, cached.Err
	}
	llibre, err := s.app.DB.ResolveLlibreByStableRef(ref)
	if err != nil {
		res := templateResolveResult{Err: &templateBookResolveError{Code: "book_not_found", FieldsM: fields}}
		s.stableRefCache[cacheKey] = res
		return 0, bookInfo{}, res.Err
	}
	if llibre == nil {
		res := templateResolveResult{Err: &templateBookResolveError{Code: notFoundCode, FieldsM: fields}}
		s.stableRefCache[cacheKey] = res
		return 0, bookInfo{}, res.Err
	}
	info := bookInfo{ID: llibre.ID, Indexed: llibre.IndexacioCompleta}
	s.byID[llibre.ID] = info
	res := templateResolveResult{ID: llibre.ID, Info: info}
	s.stableRefCache[cacheKey] = res
	return llibre.ID, info, nil
}

func (s *templateBookLookupState) resolveArchiveCandidates(arxiuID int, rowCtx templateRowContext, raw string, match func(link db.ArxiuLlibreDetail) bool) (int, bookInfo, *templateBookResolveError) {
	links := s.linksForArchive(arxiuID)
	candidateIDs := map[int]bookInfo{}
	for _, link := range links {
		if match(link) {
			if info, ok := s.byID[link.LlibreID]; ok {
				candidateIDs[link.LlibreID] = info
				continue
			}
			info, ok := s.loadBookInfo(link.LlibreID)
			if !ok {
				continue
			}
			candidateIDs[link.LlibreID] = info
		}
	}
	fields := s.rowDebugFields(rowCtx)
	fields["book_value"] = raw
	if len(candidateIDs) == 0 {
		return 0, bookInfo{}, &templateBookResolveError{Code: "book_not_found", FieldsM: fields}
	}
	if len(candidateIDs) > 1 {
		return 0, bookInfo{}, &templateBookResolveError{Code: "book_ambiguous", FieldsM: fields}
	}
	for id, info := range candidateIDs {
		if info.ID == 0 {
			info.ID = id
		}
		s.byID[id] = info
		return id, info, nil
	}
	return 0, bookInfo{}, &templateBookResolveError{Code: "book_not_found", FieldsM: fields}
}

func (s *templateBookLookupState) resolveArchive(rowCtx templateRowContext) (*db.Arxiu, *templateBookResolveError) {
	archiveCode := strings.TrimSpace(s.rowValue(rowCtx, s.model.BookArchiveCodeColumn))
	if archiveCode == "" && s.ctx.ArxiuID > 0 {
		if s.archiveOK {
			return s.archiveByCode["__ctx__"], nil
		}
		if s.archiveErr != nil {
			return nil, s.archiveErr
		}
		row, err := s.app.DB.GetArxiu(s.ctx.ArxiuID)
		if err != nil || row == nil {
			s.archiveErr = &templateBookResolveError{Code: "archive_not_found", FieldsM: map[string]string{"arxiu_id": strconv.Itoa(s.ctx.ArxiuID)}}
			return nil, s.archiveErr
		}
		s.archiveOK = true
		s.archiveByCode["__ctx__"] = row
		return row, nil
	}
	if archiveCode == "" {
		return nil, nil
	}
	key := normalizeTemplateLookupToken(archiveCode)
	if row, ok := s.archiveByCode[key]; ok {
		return row, nil
	}
	arxius, _ := s.app.DB.ListArxius(db.ArxiuFilter{})
	for _, item := range arxius {
		if normalizeTemplateLookupToken(item.Codi) == key {
			row := item.Arxiu
			s.archiveByCode[key] = &row
			return &row, nil
		}
	}
	return nil, &templateBookResolveError{Code: "archive_not_found", FieldsM: map[string]string{"archive_code": archiveCode}}
}

func (s *templateBookLookupState) linksForArchive(arxiuID int) []db.ArxiuLlibreDetail {
	if links, ok := s.archiveLinks[arxiuID]; ok {
		return links
	}
	links, _ := s.app.DB.ListArxiuLlibres(arxiuID)
	s.archiveLinks[arxiuID] = links
	return links
}

func (s *templateBookLookupState) urlsForBook(bookID int) []db.LlibreURL {
	if urls, ok := s.urlsByBook[bookID]; ok {
		return urls
	}
	urls, _ := s.app.DB.ListLlibreURLs(bookID)
	s.urlsByBook[bookID] = urls
	return urls
}

func (s *templateBookLookupState) bookRow(bookID int) *db.LlibreRow {
	for i := range s.scopedBooks {
		if s.scopedBooks[i].Row.ID == bookID {
			return &s.scopedBooks[i].Row
		}
	}
	return nil
}

func (s *templateBookLookupState) bookHasArchive(item templateScopedBook, arxiuID int) bool {
	for _, link := range item.Links {
		if link.ArxiuID == arxiuID {
			return true
		}
	}
	return false
}

func (s *templateBookLookupState) loadBookInfo(bookID int) (bookInfo, bool) {
	if info, ok := s.byID[bookID]; ok && info.ID != 0 {
		return info, true
	}
	if row := s.bookRow(bookID); row != nil {
		info := bookInfo{ID: row.ID, Indexed: row.IndexacioCompleta}
		s.byID[bookID] = info
		return info, true
	}
	llibre, err := s.app.DB.GetLlibre(bookID)
	if err != nil || llibre == nil {
		return bookInfo{}, false
	}
	info := bookInfo{ID: llibre.ID, Indexed: llibre.IndexacioCompleta}
	s.byID[bookID] = info
	return info, true
}

func (s *templateBookLookupState) municipalityContextMode() string {
	switch strings.TrimSpace(strings.ToLower(s.model.MunicipalityContext)) {
	case "", "import_context":
		return "import_context"
	case "required":
		return "required"
	case "none":
		return "none"
	default:
		return "invalid"
	}
}

func (s *templateBookLookupState) validateMunicipalityContext(rowCtx templateRowContext) *templateBookResolveError {
	if s.municipalityContextMode() == "none" {
		return nil
	}
	if s.ctx.MunicipiID == 0 {
		return &templateBookResolveError{Code: "municipality_context_missing", FieldsM: s.rowDebugFields(rowCtx)}
	}
	if s.municipalityOK {
		return nil
	}
	if s.municipalityErr != nil {
		return s.municipalityErr
	}
	row, err := s.app.DB.GetMunicipi(s.ctx.MunicipiID)
	if err != nil || row == nil {
		s.municipalityErr = &templateBookResolveError{Code: "municipality_not_found", FieldsM: map[string]string{"municipi_id": strconv.Itoa(s.ctx.MunicipiID)}}
		return s.municipalityErr
	}
	s.municipalityOK = true
	return nil
}

func (s *templateBookLookupState) resolveSourceSystem(rowCtx templateRowContext) string {
	return strings.ToLower(strings.TrimSpace(firstNonEmpty(s.rowValue(rowCtx, s.model.BookSourceColumn), s.model.BookSourceSystem)))
}

func (s *templateBookLookupState) matchesMunicipality(book db.LlibreRow) bool {
	if s.municipalityContextMode() == "none" {
		return true
	}
	return book.MunicipiID == s.ctx.MunicipiID
}

func (s *templateBookLookupState) rowValue(rowCtx templateRowContext, column string) string {
	column = strings.TrimSpace(column)
	if column == "" {
		return ""
	}
	return rowCtx.HeaderValue(normalizeCSVHeader(column))
}

func (s *templateBookLookupState) normalizeChronology(raw string) string {
	if !s.model.CronologiaNormalize {
		return strings.TrimSpace(raw)
	}
	return normalizeCronologia(raw)
}

func (s *templateBookLookupState) rowDebugFields(rowCtx templateRowContext) map[string]string {
	fields := map[string]string{}
	if s.ctx.MunicipiID > 0 {
		fields["municipi_id"] = strconv.Itoa(s.ctx.MunicipiID)
	}
	if s.ctx.ArxiuID > 0 {
		fields["arxiu_id"] = strconv.Itoa(s.ctx.ArxiuID)
	}
	if archiveCode := strings.TrimSpace(s.rowValue(rowCtx, s.model.BookArchiveCodeColumn)); archiveCode != "" {
		fields["archive_code"] = archiveCode
	}
	if sourceSystem := strings.TrimSpace(s.resolveSourceSystem(rowCtx)); sourceSystem != "" {
		fields["source_system"] = sourceSystem
	}
	if externalCode := strings.TrimSpace(s.rowValue(rowCtx, s.model.BookExternalCodeColumn)); externalCode != "" {
		fields["external_code"] = externalCode
	}
	if externalID := strings.TrimSpace(s.rowValue(rowCtx, s.model.BookExternalIDColumn)); externalID != "" {
		fields["external_id"] = externalID
	}
	return fields
}

func normalizeTemplateLookupToken(raw string) string {
	return normalizeCSVHeader(raw)
}

func normalizeTemplateURLKey(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	return strings.TrimRight(raw, "/")
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
	plan := newTemplateDedupKeyPlan(rowCtx.plan, templatePolicies{DedupKeyFields: fields})
	if plan == nil || len(plan.Fields) == 0 {
		return buildTemplateDedupKeyLegacyWithCache(cache, fields, rowCtx, mapped)
	}
	return buildTemplateDedupKeyWithPlan(cache, plan, rowCtx, mapped, nil)
}

func buildTemplateDedupKeyWithPlan(cache *templateMatchBuildCache, plan *templateDedupKeyPlan, rowCtx templateRowContext, mapped map[string]string, profile *templateDedupKeyProfileMetrics) string {
	if plan == nil || len(plan.Fields) == 0 {
		return ""
	}
	var builder strings.Builder
	segments := 0
	if profile != nil {
		profile.Rows++
	}
	for _, field := range plan.Fields {
		fieldLookupStart := time.Now()
		value, ok := templateDedupKeyFieldValue(field, rowCtx, mapped)
		if profile != nil {
			profile.FieldExtractDur += time.Since(fieldLookupStart)
		}
		if !ok {
			continue
		}
		if profile != nil {
			profile.FieldsProcessed++
		}
		normalizeStart := time.Now()
		value = normalizeTemplateLowerPartProfiled(cache, value, profile)
		if profile != nil {
			profile.StringNormalizeDur += time.Since(normalizeStart)
			if value == "" {
				profile.EmptyFields++
			} else {
				profile.NonEmptyFields++
				valueLen := len(value)
				profile.TotalNormalizedLen += valueLen
				if valueLen > profile.MaxNormalizedLen {
					profile.MaxNormalizedLen = valueLen
				}
			}
		}
		assembleStart := time.Now()
		if segments > 0 {
			builder.WriteByte('|')
		}
		builder.WriteString(value)
		segments++
		if profile != nil {
			profile.FinalAssemblyDur += time.Since(assembleStart)
		}
	}
	if segments == 0 {
		return ""
	}
	if profile != nil {
		profile.MatchKeysGenerated++
	}
	return builder.String()
}

func templateDedupKeyFieldValue(field templateDedupKeyFieldPlan, rowCtx templateRowContext, mapped map[string]string) (string, bool) {
	switch field.Source {
	case templateDedupKeyFieldSourceColumn, templateDedupKeyFieldSourceHeader:
		if field.Index < 0 || field.Index >= len(rowCtx.values) {
			return "", false
		}
		return rowCtx.values[field.Index], true
	case templateDedupKeyFieldSourceMapped:
		if mapped == nil {
			return "", false
		}
		val, ok := mapped[field.RawKey]
		return val, ok
	default:
		return "", false
	}
}

func normalizeTemplateLowerPartProfiled(cache *templateMatchBuildCache, value string, profile *templateDedupKeyProfileMetrics) string {
	if profile != nil {
		profile.NormalizationsTotal++
	}
	if cache != nil {
		if lowered, ok := cache.loweredParts[value]; ok {
			if profile != nil {
				profile.NormalizeCacheHits++
				profile.RepeatedSubcomponentHit++
			}
			return lowered
		}
		if profile != nil {
			profile.NormalizeCacheMisses++
			profile.NormalizationsUnique++
		}
	}
	return normalizeTemplateLowerPartWithCache(cache, value)
}

func buildTemplateDedupKeyLegacyWithCache(cache *templateMatchBuildCache, fields []string, rowCtx templateRowContext, mapped map[string]string) string {
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
		if runtime != nil && runtime.Engine() == "postgres" {
			Debugf("duplicate_check strong_snapshot engine=postgres strategy=context-first book_id=%d page_key=%q tipus_acte=%q snapshot_max_id=%d candidate_transcripcio_ids_count=0 loaded_transcripcions_count=0 loaded_persones_count=0 loaded_atributs_count=0 query_candidates_dur=0s query_transcripcions_dur=0s query_persones_dur=0s query_atributs_dur=0s total_context_snapshot_dur=0s", bookID, pageKey, strings.TrimSpace(incoming.TipusActe), snapshotMaxID)
		}
		return existingMap
	}
	matchBuildCache := newTemplateMatchBuildCache()
	pageKeyNorm := normalizeTemplateMatchPartWithCache(matchBuildCache, pageKey)
	candidates, _ := runtime.LoadStrongMatchCandidates(db.TemplateImportStrongMatchRequest{
		BookID:         bookID,
		TipusActe:      incoming.TipusActe,
		PageKey:        pageKey,
		SnapshotMaxID:  snapshotMaxID,
		PrincipalRoles: policies.PrincipalRoles,
	})
	attrsByTranscripcioID := candidates.AtributsByTranscripcioID
	personesByTranscripcioID := candidates.PersonesByTranscripcioID
	preparedAtributsByTranscripcioID := candidates.PreparedAtributsByTranscripcioID
	preparedPersonesByTranscripcioID := candidates.PreparedPersonesByTranscripcioID
	preparedMatchIDsByKey := candidates.PreparedMatchIDsByKey
	exactContextMatch := candidates.ExactContextMatch
	trans := candidates.Transcripcions
	if exactContextMatch && len(preparedMatchIDsByKey) > 0 {
		return preparedMatchIDsByKey
	}
	if len(trans) > 0 {
		for _, tr := range trans {
			if tr.ID <= 0 || (snapshotMaxID >= 0 && tr.ID > snapshotMaxID) {
				continue
			}
			attrsExistents, okAttrs := attrsByTranscripcioID[tr.ID]
			if !okAttrs {
				attrsExistents, _ = a.DB.ListTranscripcioAtributs(tr.ID)
			}
			if !exactContextMatch && normalizeTemplateMatchPartWithCache(matchBuildCache, a.templateLogicalPageKeyForExistingWithResolver(pageResolver, bookID, &tr, attrsExistents)) != pageKeyNorm {
				continue
			}
			personesExistentsRows, okPersones := personesByTranscripcioID[tr.ID]
			if !okPersones {
				personesExistentsRows, _ = a.DB.ListTranscripcioPersones(tr.ID)
			}
			personesExistents := preparedPersonesByTranscripcioID[tr.ID]
			if personesExistents == nil {
				personesExistents = map[string]*db.TranscripcioPersonaRaw{}
				for i := range personesExistentsRows {
					personesExistents[personesExistentsRows[i].Rol] = &personesExistentsRows[i]
				}
			}
			attrsByKey := preparedAtributsByTranscripcioID[tr.ID]
			if attrsByKey == nil {
				attrsByKey = map[string]*db.TranscripcioAtributRaw{}
				for i := range attrsExistents {
					attrsByKey[attrsExistents[i].Clau] = &attrsExistents[i]
				}
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

func (a *App) mergeTemplateRow(existingID int, t *db.TranscripcioRaw, persones map[string]*db.TranscripcioPersonaRaw, atributs map[string]*db.TranscripcioAtributRaw, policies templatePolicies) templateMergeOutcome {
	existing, err := a.DB.GetTranscripcioRaw(existingID)
	if err != nil || existing == nil {
		return templateMergeOutcome{}
	}
	beforePersones, _ := a.DB.ListTranscripcioPersones(existingID)
	beforeAtributs, _ := a.DB.ListTranscripcioAtributs(existingID)
	userID := 0
	if t != nil && t.CreatedBy.Valid {
		userID = int(t.CreatedBy.Int64)
	}
	afterRaw := *existing
	changed := updateExistingTranscripcio(&afterRaw, t, policies.UpdateMissingOnly)
	enforceTemplateImportedPending(&afterRaw, userID)
	afterPersones, addedPeople := mergeTemplatePeoplePreview(beforePersones, persones, policies.AddMissingPeople)
	afterAtributs, addedAttrs := mergeTemplateAttrsPreview(beforeAtributs, atributs, policies.AddMissingAttrs)
	changed = changed || addedPeople || addedAttrs
	if !changed {
		return templateMergeOutcome{Accepted: true, RecordID: existingID}
	}
	if strings.EqualFold(strings.TrimSpace(existing.ModeracioEstat), "publicat") {
		changeID, err := a.createTemplateImportChangeProposal(existingID, existing, beforePersones, beforeAtributs, afterRaw, afterPersones, afterAtributs, userID)
		if err != nil || changeID == 0 {
			return templateMergeOutcome{}
		}
		return templateMergeOutcome{
			Accepted:        true,
			Changed:         true,
			RecordID:        existingID,
			ChangeID:        changeID,
			CreatedProposal: true,
		}
	}
	if err := a.persistTemplatePendingMerge(existing, &afterRaw, afterPersones, afterAtributs); err != nil {
		return templateMergeOutcome{}
	}
	return templateMergeOutcome{
		Accepted: true,
		Changed:  true,
		RecordID: existingID,
	}
}

func mergeTemplatePeoplePreview(existing []db.TranscripcioPersonaRaw, incoming map[string]*db.TranscripcioPersonaRaw, addMissing bool) ([]db.TranscripcioPersonaRaw, bool) {
	out := append([]db.TranscripcioPersonaRaw(nil), existing...)
	if !addMissing {
		return out, false
	}
	personKeys := map[string]bool{}
	for _, p := range existing {
		personKeys[personKey(&p)] = true
	}
	added := false
	for _, p := range incoming {
		if isEmptyPerson(p) {
			continue
		}
		key := personKey(p)
		if personKeys[key] {
			continue
		}
		copyP := *p
		out = append(out, copyP)
		personKeys[key] = true
		added = true
	}
	return out, added
}

func mergeTemplateAttrsPreview(existing []db.TranscripcioAtributRaw, incoming map[string]*db.TranscripcioAtributRaw, addMissing bool) ([]db.TranscripcioAtributRaw, bool) {
	out := append([]db.TranscripcioAtributRaw(nil), existing...)
	if !addMissing {
		return out, false
	}
	attrKeys := map[string]bool{}
	for _, attr := range existing {
		attrKeys[attr.Clau] = true
	}
	added := false
	for _, attr := range incoming {
		if isEmptyAttr(attr) {
			continue
		}
		if attrKeys[attr.Clau] {
			continue
		}
		copyAttr := *attr
		out = append(out, copyAttr)
		attrKeys[attr.Clau] = true
		added = true
	}
	return out, added
}

func (a *App) persistTemplatePendingMerge(existing *db.TranscripcioRaw, afterRaw *db.TranscripcioRaw, afterPersones []db.TranscripcioPersonaRaw, afterAtributs []db.TranscripcioAtributRaw) error {
	if a == nil || a.DB == nil || existing == nil || afterRaw == nil {
		return fmt.Errorf("template merge persistence unavailable")
	}
	afterRaw.ID = existing.ID
	afterRaw.CreatedBy = existing.CreatedBy
	for i := range afterAtributs {
		afterAtributs[i].TranscripcioID = existing.ID
	}
	for i := range afterPersones {
		afterPersones[i].TranscripcioID = existing.ID
	}
	return a.DB.PersistTemplatePendingMerge(afterRaw, afterPersones, afterAtributs)
}

func (a *App) createTemplateImportChangeProposal(existingID int, beforeRaw *db.TranscripcioRaw, beforePersones []db.TranscripcioPersonaRaw, beforeAtributs []db.TranscripcioAtributRaw, afterRaw db.TranscripcioRaw, afterPersones []db.TranscripcioPersonaRaw, afterAtributs []db.TranscripcioAtributRaw, userID int) (int, error) {
	if a == nil || a.DB == nil || beforeRaw == nil {
		return 0, fmt.Errorf("template change proposal unavailable")
	}
	afterRaw.ID = existingID
	afterRaw.CreatedBy = beforeRaw.CreatedBy
	meta := map[string]interface{}{
		"source": "template_import",
		"before": map[string]interface{}{
			"raw":      beforeRaw,
			"persones": beforePersones,
			"atributs": beforeAtributs,
		},
		"after": map[string]interface{}{
			"raw":      afterRaw,
			"persones": afterPersones,
			"atributs": afterAtributs,
		},
	}
	metaJSON, _ := json.Marshal(meta)
	return a.DB.CreateTranscripcioRawChange(&db.TranscripcioRawChange{
		TranscripcioID: existingID,
		ChangeType:     "template_import",
		FieldKey:       "bulk",
		OldValue:       "",
		NewValue:       "",
		Metadata:       string(metaJSON),
		ModeracioEstat: "pendent",
		ChangedBy:      sqlNullIntFromInt(userID),
	})
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

func applyBaseDefaults(t *db.TranscripcioRaw, defaults map[string]string) {
	if t == nil || defaults == nil {
		return
	}
	if v := defaults["tipus_acte"]; v != "" {
		t.TipusActe = v
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
