package core

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

var (
	parseMarriageOrderParenRe     = regexp.MustCompile(`\(\s*(\d+)\s*[rnt]?\s*\)`)
	parseMarriageOrderOrdinalRe   = regexp.MustCompile(`\b(\d+)\s*(r|n|t)\b`)
	parseMarriageOrderMatAfterRe  = regexp.MustCompile(`matrimoni\s*(\d+)`)
	parseMarriageOrderMatBeforeRe = regexp.MustCompile(`\b(\d+)\s*(?:r|n|t)?\s*matrimoni\b`)
)

type templatePersonBuildProfiler struct {
	Enabled               bool
	TotalCalls            int
	PersonsAttempted      int
	PersonsCreated        int
	PersonsDiscardedEmpty int
	AtributsProcessed     int
	FieldsEmpty           int
	FieldsNonEmpty        int
	CacheHits             int
	CacheMisses           int
	NormalizationsTotal   int
	FullNameSplits        int
	RoleAssignments       int
	StructBuilds          int
	AttributeBuilds       int
	ValidationCalls       int
	FieldLookupDur        time.Duration
	CacheLookupDur        time.Duration
	StringNormalizeDur    time.Duration
	SplitParseDur         time.Duration
	RoleAssignDur         time.Duration
	StructBuildDur        time.Duration
	AtributsDur           time.Duration
	ValidationDur         time.Duration
	TotalDur              time.Duration
	normalizeSeen         map[string]struct{}
	roleStats             map[string]*templatePersonBuildRoleStats
}

type templatePersonBuildRoleStats struct {
	Parser      string
	Role        string
	Calls       int
	CacheHits   int
	CacheMisses int
	TotalDur    time.Duration
}

type templatePersonBuildCallMetrics struct {
	Parser             string
	Role               string
	Attempted          bool
	Created            bool
	DiscardedEmpty     bool
	AtributsProcessed  int
	FieldsEmpty        int
	FieldsNonEmpty     int
	CacheHit           bool
	CacheMiss          bool
	Normalizations     int
	FullNameSplit      bool
	RoleAssigned       bool
	StructBuilt        bool
	AttributeBuilt     bool
	ValidationCalled   bool
	FieldLookupDur     time.Duration
	CacheLookupDur     time.Duration
	StringNormalizeDur time.Duration
	SplitParseDur      time.Duration
	RoleAssignDur      time.Duration
	StructBuildDur     time.Duration
	AtributsDur        time.Duration
	ValidationDur      time.Duration
	TotalDur           time.Duration
}

func newTemplatePersonBuildProfiler(enabled bool) *templatePersonBuildProfiler {
	if !enabled {
		return nil
	}
	return &templatePersonBuildProfiler{
		Enabled:       true,
		normalizeSeen: map[string]struct{}{},
		roleStats:     map[string]*templatePersonBuildRoleStats{},
	}
}

func (p *templatePersonBuildProfiler) addNormalization(value string) {
	if p == nil || !p.Enabled {
		return
	}
	p.NormalizationsTotal++
	if value == "" {
		return
	}
	p.normalizeSeen[value] = struct{}{}
}

func (p *templatePersonBuildProfiler) addCall(call templatePersonBuildCallMetrics) {
	if p == nil || !p.Enabled {
		return
	}
	p.TotalCalls++
	if call.Attempted {
		p.PersonsAttempted++
	}
	if call.Created {
		p.PersonsCreated++
	}
	if call.DiscardedEmpty {
		p.PersonsDiscardedEmpty++
	}
	p.AtributsProcessed += call.AtributsProcessed
	p.FieldsEmpty += call.FieldsEmpty
	p.FieldsNonEmpty += call.FieldsNonEmpty
	if call.CacheHit {
		p.CacheHits++
	}
	if call.CacheMiss {
		p.CacheMisses++
	}
	if call.FullNameSplit {
		p.FullNameSplits++
	}
	if call.RoleAssigned {
		p.RoleAssignments++
	}
	if call.StructBuilt {
		p.StructBuilds++
	}
	if call.AttributeBuilt {
		p.AttributeBuilds++
	}
	if call.ValidationCalled {
		p.ValidationCalls++
	}
	p.FieldLookupDur += call.FieldLookupDur
	p.CacheLookupDur += call.CacheLookupDur
	p.StringNormalizeDur += call.StringNormalizeDur
	p.SplitParseDur += call.SplitParseDur
	p.RoleAssignDur += call.RoleAssignDur
	p.StructBuildDur += call.StructBuildDur
	p.AtributsDur += call.AtributsDur
	p.ValidationDur += call.ValidationDur
	p.TotalDur += call.TotalDur
	statKey := call.Parser + "\x00" + call.Role
	stat := p.roleStats[statKey]
	if stat == nil {
		stat = &templatePersonBuildRoleStats{Parser: call.Parser, Role: call.Role}
		p.roleStats[statKey] = stat
	}
	stat.Calls++
	if call.CacheHit {
		stat.CacheHits++
	}
	if call.CacheMiss {
		stat.CacheMisses++
	}
	stat.TotalDur += call.TotalDur
}

func (p *templatePersonBuildProfiler) logDebug() {
	if p == nil || !p.Enabled {
		return
	}
	repeated := p.NormalizationsTotal - len(p.normalizeSeen)
	if repeated < 0 {
		repeated = 0
	}
	Debugf(
		"parse_person_build_summary total_calls=%d persons_attempted=%d persons_created=%d persons_discarded_empty=%d atributs_processed=%d fields_empty=%d fields_nonempty=%d cache_hits=%d cache_misses=%d normalizations_total=%d normalizations_unique=%d normalizations_repeated=%d full_name_splits=%d role_assignments=%d struct_builds=%d attribute_builds=%d validation_calls=%d field_lookup_dur=%s cache_lookup_dur=%s string_normalize_dur=%s split_parse_dur=%s role_assign_dur=%s struct_build_dur=%s atributs_dur=%s validation_dur=%s total_dur=%s",
		p.TotalCalls,
		p.PersonsAttempted,
		p.PersonsCreated,
		p.PersonsDiscardedEmpty,
		p.AtributsProcessed,
		p.FieldsEmpty,
		p.FieldsNonEmpty,
		p.CacheHits,
		p.CacheMisses,
		p.NormalizationsTotal,
		len(p.normalizeSeen),
		repeated,
		p.FullNameSplits,
		p.RoleAssignments,
		p.StructBuilds,
		p.AttributeBuilds,
		p.ValidationCalls,
		p.FieldLookupDur,
		p.CacheLookupDur,
		p.StringNormalizeDur,
		p.SplitParseDur,
		p.RoleAssignDur,
		p.StructBuildDur,
		p.AtributsDur,
		p.ValidationDur,
		p.TotalDur,
	)
	stats := make([]*templatePersonBuildRoleStats, 0, len(p.roleStats))
	for _, stat := range p.roleStats {
		stats = append(stats, stat)
	}
	sort.Slice(stats, func(i, j int) bool {
		if stats[i].TotalDur == stats[j].TotalDur {
			if stats[i].Parser == stats[j].Parser {
				return stats[i].Role < stats[j].Role
			}
			return stats[i].Parser < stats[j].Parser
		}
		return stats[i].TotalDur > stats[j].TotalDur
	})
	limit := 8
	if len(stats) < limit {
		limit = len(stats)
	}
	for i := 0; i < limit; i++ {
		stat := stats[i]
		Debugf(
			"parse_person_build_top rank=%d parser=%q role=%q calls=%d cache_hits=%d cache_misses=%d total_dur=%s",
			i+1,
			stat.Parser,
			stat.Role,
			stat.Calls,
			stat.CacheHits,
			stat.CacheMisses,
			stat.TotalDur,
		)
	}
}

func parseFlexibleDateV2(raw string) (string, string, string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", ""
	}
	if strings.Contains(raw, "¿") {
		return "", strings.TrimSpace(raw), "no_consta"
	}
	if strings.Contains(raw, "?") {
		return "", strings.TrimSpace(raw), "dubtos"
	}
	normalized := strings.ReplaceAll(raw, ".", "/")
	parts := strings.Split(normalized, "/")
	if len(parts) != 3 {
		return "", strings.TrimSpace(raw), "incomplet"
	}
	dayStr := strings.TrimSpace(parts[0])
	monthStr := strings.TrimSpace(parts[1])
	yearStr := strings.TrimSpace(parts[2])
	if dayStr == "" || monthStr == "" || yearStr == "" {
		return "", strings.TrimSpace(raw), "incomplet"
	}
	if !isDigits(dayStr) || !isDigits(monthStr) || !isDigits(yearStr) {
		return "", strings.TrimSpace(raw), "incomplet"
	}
	day, _ := strconv.Atoi(dayStr)
	month, _ := strconv.Atoi(monthStr)
	year, _ := strconv.Atoi(yearStr)
	if len(yearStr) == 2 {
		year += 1900
	}
	if len(yearStr) < 2 || len(yearStr) > 4 {
		return "", strings.TrimSpace(raw), "incomplet"
	}
	if !validDate(day, month, year) {
		return "", strings.TrimSpace(raw), "incomplet"
	}
	return fmt.Sprintf("%04d-%02d-%02d", year, month, day), "", "clar"
}

func parseFlexibleDateWithConfig(raw string, cfg templateParseConfig) (string, string, string) {
	start := time.Now()
	defer func() {
		if cfg.Metrics != nil {
			cfg.Metrics.addParseDate(time.Since(start))
		}
	}()
	if cfg.Caches != nil {
		if cached, ok := cfg.Caches.date[raw]; ok && cached.Loaded {
			return cached.ISO, cached.Text, cached.Estat
		}
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", ""
	}
	cleaned, qual := extractQualityWithConfig(raw, cfg)
	cleaned = strings.TrimSpace(cleaned)
	if cleaned == "" {
		if cfg.Caches != nil {
			cfg.Caches.date[raw] = templateDateCacheEntry{Text: "", Estat: qual, Loaded: true}
		}
		return "", "", qual
	}
	format := normalizeTemplateDateFormat(cfg.DateFormat)
	iso, ok := parseDateByFormat(cleaned, format)
	if ok {
		estat := mergeQualityStatus(qual, "clar")
		if cfg.Caches != nil {
			cfg.Caches.date[raw] = templateDateCacheEntry{ISO: iso, Text: "", Estat: estat, Loaded: true}
		}
		return iso, "", estat
	}
	if qual == "" {
		qual = "incomplet"
	}
	if cfg.Caches != nil {
		cfg.Caches.date[raw] = templateDateCacheEntry{ISO: "", Text: cleaned, Estat: qual, Loaded: true}
	}
	return "", cleaned, qual
}

func parseDateToISOWithConfig(raw string, cfg templateParseConfig) (string, string) {
	iso, _, estat := parseFlexibleDateWithConfig(raw, cfg)
	return iso, estat
}

func parseDateByFormat(raw, format string) (string, bool) {
	normalized := strings.ReplaceAll(raw, ".", "/")
	normalized = strings.ReplaceAll(normalized, "-", "/")
	parts := strings.Split(normalized, "/")
	if len(parts) != 3 {
		return "", false
	}
	p1 := strings.TrimSpace(parts[0])
	p2 := strings.TrimSpace(parts[1])
	p3 := strings.TrimSpace(parts[2])
	if p1 == "" || p2 == "" || p3 == "" {
		return "", false
	}
	if !isDigits(p1) || !isDigits(p2) || !isDigits(p3) {
		return "", false
	}
	day, month, year := 0, 0, 0
	switch format {
	case "mm/dd":
		month, _ = strconv.Atoi(p1)
		day, _ = strconv.Atoi(p2)
		year, _ = strconv.Atoi(p3)
	case "iso":
		year, _ = strconv.Atoi(p1)
		month, _ = strconv.Atoi(p2)
		day, _ = strconv.Atoi(p3)
	default:
		day, _ = strconv.Atoi(p1)
		month, _ = strconv.Atoi(p2)
		year, _ = strconv.Atoi(p3)
	}
	if year < 100 {
		year += 1900
	}
	if !validDate(day, month, year) {
		return "", false
	}
	return fmt.Sprintf("%04d-%02d-%02d", year, month, day), true
}

func validDate(day, month, year int) bool {
	if day <= 0 || month <= 0 || month > 12 || year <= 0 {
		return false
	}
	t := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	return t.Day() == day && int(t.Month()) == month && t.Year() == year
}

func isDigits(val string) bool {
	for _, r := range val {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func isISODate(val string) bool {
	if len(val) != 10 {
		return false
	}
	for i, r := range val {
		switch i {
		case 4, 7:
			if r != '-' {
				return false
			}
		default:
			if r < '0' || r > '9' {
				return false
			}
		}
	}
	return true
}

func extractParentheticalAll(raw string) []string {
	raw = strings.TrimSpace(raw)
	var extras []string
	for {
		start := strings.Index(raw, "(")
		end := strings.Index(raw, ")")
		if start == -1 || end == -1 || end < start {
			break
		}
		extra := strings.TrimSpace(raw[start+1 : end])
		if extra != "" {
			extras = append(extras, extra)
		}
		raw = strings.TrimSpace(raw[:start] + " " + raw[end+1:])
	}
	return extras
}

func extractParentheticalLast(raw string) string {
	all := extractParentheticalAll(raw)
	if len(all) == 0 {
		return ""
	}
	return all[len(all)-1]
}

func stripParentheticals(raw string) string {
	raw = strings.TrimSpace(raw)
	for {
		start := strings.Index(raw, "(")
		end := strings.Index(raw, ")")
		if start == -1 || end == -1 || end < start {
			break
		}
		raw = strings.TrimSpace(raw[:start] + " " + raw[end+1:])
	}
	return strings.Join(strings.Fields(raw), " ")
}

func buildPersonFromCognomsV2(raw, role string) *db.TranscripcioPersonaRaw {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	main := stripParentheticals(raw)
	extras := extractParentheticalAll(raw)
	tokens := strings.Fields(main)
	clean := make([]string, 0, len(tokens))
	quals := make([]string, 0, len(tokens))
	for _, tok := range tokens {
		tokClean, qual := cleanToken(tok)
		if tokClean != "" {
			clean = append(clean, tokClean)
			quals = append(quals, qual)
		}
	}
	var cognom1, cognom2, nom string
	var cognom1Qual, cognom2Qual, nomQual string
	if len(clean) >= 1 {
		consumed := 0
		cognom1, cognom1Qual, consumed = consumeSurnameFromStart(clean, quals)
		restTokens := clean[consumed:]
		restQuals := quals[consumed:]
		if len(restTokens) == 1 {
			nom = restTokens[0]
			nomQual = restQuals[0]
		} else if len(restTokens) >= 2 {
			consumed2 := 0
			cognom2, cognom2Qual, consumed2 = consumeSurnameFromStart(restTokens, restQuals)
			nameTokens := restTokens[consumed2:]
			if len(nameTokens) > 0 {
				nom = strings.Join(nameTokens, " ")
				nomQual = mergeQuality(restQuals[consumed2:]...)
			}
		}
	}
	notes, municipi := splitParentheticalNotes(extras)
	munText, munQual := cleanFreeText(municipi)
	p := &db.TranscripcioPersonaRaw{
		Rol:          role,
		Nom:          nom,
		Cognom1:      cognom1,
		Cognom2:      cognom2,
		NomEstat:     defaultQuality(nom, nomQual),
		Cognom1Estat: defaultQuality(cognom1, cognom1Qual),
		Cognom2Estat: defaultQuality(cognom2, cognom2Qual),
		Notes:        notes,
	}
	if munText != "" {
		p.MunicipiText = munText
		p.MunicipiEstat = defaultQuality(munText, munQual)
	}
	return p
}

func buildPersonFromCognomsWithConfig(raw, role string, cfg templateParseConfig) *db.TranscripcioPersonaRaw {
	return buildTemplatePersonWithConfig(raw, role, "cognoms_v1", cfg, buildPersonFromCognomsProfiled)
}

func buildPersonFromNomV2(raw, role string) *db.TranscripcioPersonaRaw {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	main := stripParentheticals(raw)
	extras := extractParentheticalAll(raw)
	tokens := strings.Fields(main)
	clean := make([]string, 0, len(tokens))
	quals := make([]string, 0, len(tokens))
	for _, tok := range tokens {
		tokClean, qual := cleanToken(tok)
		if tokClean != "" {
			clean = append(clean, tokClean)
			quals = append(quals, qual)
		}
	}
	var cognom1, cognom2, nom string
	var cognom1Qual, cognom2Qual, nomQual string
	if len(clean) >= 1 {
		surname2, surname2Qual, consumed2 := consumeSurnameFromEnd(clean, quals)
		restTokens := clean[:len(clean)-consumed2]
		restQuals := quals[:len(quals)-consumed2]
		if len(restTokens) <= 1 {
			nom = strings.Join(restTokens, " ")
			if len(restQuals) > 0 {
				nomQual = mergeQuality(restQuals...)
			}
			cognom1 = surname2
			cognom1Qual = surname2Qual
		} else {
			cognom2 = surname2
			cognom2Qual = surname2Qual
			surname1, surname1Qual, consumed1 := consumeSurnameFromEnd(restTokens, restQuals)
			cognom1 = surname1
			cognom1Qual = surname1Qual
			nameTokens := restTokens[:len(restTokens)-consumed1]
			if len(nameTokens) > 0 {
				nom = strings.Join(nameTokens, " ")
				nomQual = mergeQuality(restQuals[:len(restQuals)-consumed1]...)
			}
		}
	}
	notes, municipi := splitParentheticalNotes(extras)
	munText, munQual := cleanFreeText(municipi)
	p := &db.TranscripcioPersonaRaw{
		Rol:          role,
		Nom:          nom,
		Cognom1:      cognom1,
		Cognom2:      cognom2,
		NomEstat:     defaultQuality(nom, nomQual),
		Cognom1Estat: defaultQuality(cognom1, cognom1Qual),
		Cognom2Estat: defaultQuality(cognom2, cognom2Qual),
		Notes:        notes,
	}
	if munText != "" {
		p.MunicipiText = munText
		p.MunicipiEstat = defaultQuality(munText, munQual)
	}
	return p
}

func buildPersonFromNomWithConfig(raw, role string, cfg templateParseConfig) *db.TranscripcioPersonaRaw {
	return buildTemplatePersonWithConfig(raw, role, "nom_v1", cfg, buildPersonFromNomProfiled)
}

func buildPersonFromCognomsV2WithConfig(raw, role string, cfg templateParseConfig) *db.TranscripcioPersonaRaw {
	return buildTemplatePersonWithConfig(raw, role, "cognoms_v2", cfg, buildPersonFromCognomsV2Profiled)
}

func buildPersonFromNomV2WithConfig(raw, role string, cfg templateParseConfig) *db.TranscripcioPersonaRaw {
	return buildTemplatePersonWithConfig(raw, role, "nom_v2", cfg, buildPersonFromNomV2Profiled)
}

type templatePersonBuildFunc func(raw, role string, cfg templateParseConfig, call *templatePersonBuildCallMetrics) *db.TranscripcioPersonaRaw

func buildTemplatePersonWithConfig(raw, role, parser string, cfg templateParseConfig, build templatePersonBuildFunc) *db.TranscripcioPersonaRaw {
	start := time.Now()
	call := templatePersonBuildCallMetrics{Parser: parser, Role: role}
	defer func() {
		call.TotalDur = time.Since(start)
		if cfg.Metrics != nil {
			cfg.Metrics.addParsePersonBuild(call.TotalDur)
		}
		if cfg.PersonProfiler != nil {
			cfg.PersonProfiler.addCall(call)
		}
	}()
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		call.DiscardedEmpty = true
		return nil
	}
	call.Attempted = true
	cacheStart := time.Now()
	if cached, ok := templateCachedPersonLookup(cfg, parser, trimmed); ok {
		call.CacheLookupDur += time.Since(cacheStart)
		call.CacheHit = true
		return templatePersonFromCacheForRole(cached, role, &call)
	}
	call.CacheLookupDur += time.Since(cacheStart)
	call.CacheMiss = true
	p := build(trimmed, role, cfg, &call)
	templateStoreCachedPerson(cfg, parser, trimmed, p)
	if p == nil {
		call.DiscardedEmpty = true
		return nil
	}
	call.Created = true
	return p
}

func templateCachedPersonLookup(cfg templateParseConfig, parser, raw string) (templatePersonCacheEntry, bool) {
	if cfg.Caches == nil {
		return templatePersonCacheEntry{}, false
	}
	entry, ok := cfg.Caches.person[templatePersonCacheKey{
		Parser: parser,
		Flavor: cfg.PersonCacheFlavor,
		Raw:    raw,
	}]
	return entry, ok && entry.Loaded
}

func templateStoreCachedPerson(cfg templateParseConfig, parser, raw string, p *db.TranscripcioPersonaRaw) {
	if cfg.Caches == nil {
		return
	}
	entry := templatePersonCacheEntry{Loaded: true}
	if p != nil {
		entry.Person = *p
		entry.Person.Rol = ""
		entry.HasPerson = true
	}
	cfg.Caches.person[templatePersonCacheKey{
		Parser: parser,
		Flavor: cfg.PersonCacheFlavor,
		Raw:    raw,
	}] = entry
}

func templatePersonFromCacheForRole(entry templatePersonCacheEntry, role string, call *templatePersonBuildCallMetrics) *db.TranscripcioPersonaRaw {
	if !entry.HasPerson {
		return nil
	}
	assignStart := time.Now()
	cp := entry.Person
	cp.Rol = role
	if call != nil {
		call.RoleAssigned = true
		call.RoleAssignDur += time.Since(assignStart)
		templateProfilePersonFields(&cp, call)
	}
	return &cp
}

func buildPersonFromCognomsProfiled(raw, role string, _ templateParseConfig, call *templatePersonBuildCallMetrics) *db.TranscripcioPersonaRaw {
	p := buildPersonFromCognoms(raw, role)
	templateProfilePersonFields(p, call)
	if p != nil {
		call.StructBuilt = true
		call.RoleAssigned = true
	}
	return p
}

func buildPersonFromNomProfiled(raw, role string, _ templateParseConfig, call *templatePersonBuildCallMetrics) *db.TranscripcioPersonaRaw {
	p := buildPersonFromNom(raw, role)
	templateProfilePersonFields(p, call)
	if p != nil {
		call.StructBuilt = true
		call.RoleAssigned = true
	}
	return p
}

func buildPersonFromCognomsV2Profiled(raw, role string, cfg templateParseConfig, call *templatePersonBuildCallMetrics) *db.TranscripcioPersonaRaw {
	if isDefaultQualityConfig(cfg.Quality) && !cfg.Quality.Labels {
		return buildPersonFromCognomsV2Core(raw, role, false, cfg, call)
	}
	return buildPersonFromCognomsV2Core(raw, role, true, cfg, call)
}

func buildPersonFromNomV2Profiled(raw, role string, cfg templateParseConfig, call *templatePersonBuildCallMetrics) *db.TranscripcioPersonaRaw {
	if isDefaultQualityConfig(cfg.Quality) && !cfg.Quality.Labels {
		return buildPersonFromNomV2Core(raw, role, false, cfg, call)
	}
	return buildPersonFromNomV2Core(raw, role, true, cfg, call)
}

func buildPersonFromCognomsV2Core(raw, role string, useConfig bool, cfg templateParseConfig, call *templatePersonBuildCallMetrics) *db.TranscripcioPersonaRaw {
	splitStart := time.Now()
	main, extras := splitParentheticals(raw)
	call.SplitParseDur += time.Since(splitStart)
	call.FullNameSplit = true
	globalQual := ""
	if useConfig {
		qualStart := time.Now()
		main, globalQual = stripQualityLabel(main, cfg.Quality)
		if globalQual == "" && len(extras) > 0 {
			if status := mapQualityLabel(extras[len(extras)-1]); status != "" {
				globalQual = status
				extras = extras[:len(extras)-1]
			}
		}
		call.ValidationDur += time.Since(qualStart)
		call.ValidationCalled = true
	}
	clean, quals := templateProfiledCleanTokens(main, useConfig, cfg, call)
	var cognom1, cognom2, nom string
	var cognom1Qual, cognom2Qual, nomQual string
	parseStart := time.Now()
	if len(clean) >= 1 {
		consumed := 0
		cognom1, cognom1Qual, consumed = consumeSurnameFromStart(clean, quals)
		restTokens := clean[consumed:]
		restQuals := quals[consumed:]
		if len(restTokens) == 1 {
			nom = restTokens[0]
			nomQual = restQuals[0]
		} else if len(restTokens) >= 2 {
			consumed2 := 0
			cognom2, cognom2Qual, consumed2 = consumeSurnameFromStart(restTokens, restQuals)
			nameTokens := restTokens[consumed2:]
			if len(nameTokens) > 0 {
				nom = strings.Join(nameTokens, " ")
				nomQual = mergeQuality(restQuals[consumed2:]...)
			}
		}
	}
	call.SplitParseDur += time.Since(parseStart)
	if globalQual != "" {
		validateStart := time.Now()
		if nom != "" {
			nomQual = mergeQualityStatus(nomQual, globalQual)
		}
		if cognom1 != "" {
			cognom1Qual = mergeQualityStatus(cognom1Qual, globalQual)
		}
		if cognom2 != "" {
			cognom2Qual = mergeQualityStatus(cognom2Qual, globalQual)
		}
		call.ValidationDur += time.Since(validateStart)
		call.ValidationCalled = true
	}
	attrStart := time.Now()
	notes, municipi := splitParentheticalNotes(extras)
	var munText, munQual string
	if useConfig {
		munText, munQual = cleanFreeTextWithConfig(municipi, cfg)
	} else {
		munText, munQual = cleanFreeText(municipi)
	}
	call.AtributsDur += time.Since(attrStart)
	assembleStart := time.Now()
	p := &db.TranscripcioPersonaRaw{
		Rol:          role,
		Nom:          nom,
		Cognom1:      cognom1,
		Cognom2:      cognom2,
		NomEstat:     defaultQuality(nom, nomQual),
		Cognom1Estat: defaultQuality(cognom1, cognom1Qual),
		Cognom2Estat: defaultQuality(cognom2, cognom2Qual),
		Notes:        notes,
	}
	if munText != "" {
		p.MunicipiText = munText
		p.MunicipiEstat = defaultQuality(munText, munQual)
	}
	call.StructBuildDur += time.Since(assembleStart)
	call.StructBuilt = true
	call.RoleAssigned = true
	templateProfilePersonFields(p, call)
	return p
}

func buildPersonFromNomV2Core(raw, role string, useConfig bool, cfg templateParseConfig, call *templatePersonBuildCallMetrics) *db.TranscripcioPersonaRaw {
	splitStart := time.Now()
	main, extras := splitParentheticals(raw)
	call.SplitParseDur += time.Since(splitStart)
	call.FullNameSplit = true
	globalQual := ""
	if useConfig {
		qualStart := time.Now()
		main, globalQual = stripQualityLabel(main, cfg.Quality)
		if globalQual == "" && len(extras) > 0 {
			if status := mapQualityLabel(extras[len(extras)-1]); status != "" {
				globalQual = status
				extras = extras[:len(extras)-1]
			}
		}
		call.ValidationDur += time.Since(qualStart)
		call.ValidationCalled = true
	}
	clean, quals := templateProfiledCleanTokens(main, useConfig, cfg, call)
	var cognom1, cognom2, nom string
	var cognom1Qual, cognom2Qual, nomQual string
	parseStart := time.Now()
	if len(clean) >= 1 {
		nom = clean[0]
		nomQual = quals[0]
		restTokens := clean[1:]
		restQuals := quals[1:]
		if len(restTokens) == 1 {
			cognom1 = restTokens[0]
			cognom1Qual = restQuals[0]
		} else if len(restTokens) >= 2 {
			consumed1 := 0
			cognom1, cognom1Qual, consumed1 = consumeSurnameFromEnd(restTokens, restQuals)
			if consumed1 == 0 {
				consumed1 = 1
				cognom1 = restTokens[len(restTokens)-1]
				cognom1Qual = restQuals[len(restQuals)-1]
			}
			if consumed1 < len(restTokens) {
				nameTokens := restTokens[:len(restTokens)-consumed1]
				if len(nameTokens) > 0 {
					cognom2 = strings.Join(nameTokens, " ")
					cognom2Qual = mergeQuality(restQuals[:len(restQuals)-consumed1]...)
				}
			}
		}
	}
	call.SplitParseDur += time.Since(parseStart)
	if globalQual != "" {
		validateStart := time.Now()
		if nom != "" {
			nomQual = mergeQualityStatus(nomQual, globalQual)
		}
		if cognom1 != "" {
			cognom1Qual = mergeQualityStatus(cognom1Qual, globalQual)
		}
		if cognom2 != "" {
			cognom2Qual = mergeQualityStatus(cognom2Qual, globalQual)
		}
		call.ValidationDur += time.Since(validateStart)
		call.ValidationCalled = true
	}
	attrStart := time.Now()
	notes, municipi := splitParentheticalNotes(extras)
	var munText, munQual string
	if useConfig {
		munText, munQual = cleanFreeTextWithConfig(municipi, cfg)
	} else {
		munText, munQual = cleanFreeText(municipi)
	}
	call.AtributsDur += time.Since(attrStart)
	assembleStart := time.Now()
	p := &db.TranscripcioPersonaRaw{
		Rol:          role,
		Nom:          nom,
		Cognom1:      cognom1,
		Cognom2:      cognom2,
		NomEstat:     defaultQuality(nom, nomQual),
		Cognom1Estat: defaultQuality(cognom1, cognom1Qual),
		Cognom2Estat: defaultQuality(cognom2, cognom2Qual),
		Notes:        notes,
	}
	if munText != "" {
		p.MunicipiText = munText
		p.MunicipiEstat = defaultQuality(munText, munQual)
	}
	call.StructBuildDur += time.Since(assembleStart)
	call.StructBuilt = true
	call.RoleAssigned = true
	templateProfilePersonFields(p, call)
	return p
}

func templateProfiledCleanTokens(main string, useConfig bool, cfg templateParseConfig, call *templatePersonBuildCallMetrics) ([]string, []string) {
	splitStart := time.Now()
	tokens := strings.Fields(main)
	call.SplitParseDur += time.Since(splitStart)
	clean := make([]string, 0, len(tokens))
	quals := make([]string, 0, len(tokens))
	for _, tok := range tokens {
		normStart := time.Now()
		var tokClean, qual string
		if useConfig {
			tokClean, qual = cleanTokenWithConfig(tok, cfg.Quality)
		} else {
			tokClean, qual = cleanToken(tok)
		}
		call.StringNormalizeDur += time.Since(normStart)
		call.Normalizations++
		if cfg.PersonProfiler != nil {
			cfg.PersonProfiler.addNormalization(tokClean)
		}
		if tokClean != "" {
			clean = append(clean, tokClean)
			quals = append(quals, qual)
		}
	}
	return clean, quals
}

func templateProfilePersonFields(p *db.TranscripcioPersonaRaw, call *templatePersonBuildCallMetrics) {
	if p == nil || call == nil {
		return
	}
	for _, val := range []string{p.Nom, p.Cognom1, p.Cognom2, p.MunicipiText, p.Notes} {
		if strings.TrimSpace(val) == "" {
			call.FieldsEmpty++
		} else {
			call.FieldsNonEmpty++
		}
	}
}

func splitParentheticalNotes(extras []string) (string, string) {
	if len(extras) == 0 {
		return "", ""
	}
	if len(extras) == 1 {
		return extras[0], extras[0]
	}
	notes := strings.Join(extras[:len(extras)-1], "; ")
	return notes, extras[len(extras)-1]
}

func cleanFreeText(raw string) (string, string) {
	if raw == "" {
		return "", ""
	}
	qual := ""
	if strings.Contains(raw, "¿") {
		qual = "no_consta"
	} else if strings.Contains(raw, "?") {
		qual = "dubtos"
	}
	text := strings.ReplaceAll(raw, "¿", "")
	text = strings.ReplaceAll(text, "?", "")
	text = strings.TrimSpace(text)
	return text, qual
}

func cleanTokenWithConfig(token string, cfg templateQualityConfig) (string, string) {
	if token == "" {
		return "", ""
	}
	cleaned, qual := stripQualityMarkers(token, cfg)
	cleaned = strings.Trim(cleaned, " ,.;:")
	return cleaned, qual
}

func cleanFreeTextWithConfig(raw string, cfg templateParseConfig) (string, string) {
	if raw == "" {
		return "", ""
	}
	text, qual := extractQualityWithConfig(raw, cfg)
	return strings.TrimSpace(text), qual
}

func parseMarriageOrder(raw string) (int, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	lower := strings.ToLower(raw)
	if match := parseMarriageOrderParenRe.FindStringSubmatch(lower); len(match) > 1 {
		if n, err := strconv.Atoi(match[1]); err == nil {
			return n, true
		}
	}
	if match := parseMarriageOrderOrdinalRe.FindStringSubmatch(lower); len(match) > 1 {
		if n, err := strconv.Atoi(match[1]); err == nil {
			return n, true
		}
	}
	if match := parseMarriageOrderMatAfterRe.FindStringSubmatch(lower); len(match) > 1 {
		if n, err := strconv.Atoi(match[1]); err == nil {
			return n, true
		}
	}
	if match := parseMarriageOrderMatBeforeRe.FindStringSubmatch(lower); len(match) > 1 {
		if n, err := strconv.Atoi(match[1]); err == nil {
			return n, true
		}
	}
	return 0, false
}

func stripMarriageOrderText(raw string) string {
	out := raw
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`\(\s*\d+\s*[rnt]?\s*\)`),
		regexp.MustCompile(`\bmatrimoni\s*\d+\b`),
		regexp.MustCompile(`\b\d+\s*(?:r|n|t)?\s*matrimoni\b`),
		regexp.MustCompile(`\b\d+\s*(?:r|n|t)\b`),
	}
	for _, re := range patterns {
		out = re.ReplaceAllString(out, "")
	}
	out = strings.ReplaceAll(out, ":", " ")
	out = strings.ReplaceAll(out, "-", " ")
	out = strings.Join(strings.Fields(out), " ")
	return strings.Trim(out, " ,;")
}
