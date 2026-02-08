package core

import (
	"strings"
)

type templateQualityConfig struct {
	Labels  bool
	Markers map[string]string
}

type templateParseConfig struct {
	DateFormat string
	Quality    templateQualityConfig
}

var defaultTemplateQualityMarkers = map[string]string{
	"dubtos":   "?",
	"no_consta": "¿",
}

var qualityRank = map[string]int{
	"":          0,
	"clar":      1,
	"dubtos":    2,
	"incomplet": 3,
	"illegible": 4,
	"no_consta": 5,
}

func normalizeTemplateDateFormat(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	switch raw {
	case "mm/dd", "mmdd", "mmddyyyy", "mdy":
		return "mm/dd"
	case "iso", "yyyy-mm-dd", "yyyy/mm/dd":
		return "iso"
	default:
		return "dd/mm"
	}
}

func normalizeTemplateQualityConfig(cfg templateQualityConfig) templateQualityConfig {
	markers := map[string]string{}
	if cfg.Markers != nil {
		for key, val := range cfg.Markers {
			markers[key] = strings.TrimSpace(val)
		}
	}
	for key, val := range defaultTemplateQualityMarkers {
		if _, ok := markers[key]; !ok {
			markers[key] = val
		}
	}
	return templateQualityConfig{
		Labels:  cfg.Labels,
		Markers: markers,
	}
}

func buildTemplateParseConfig(model *templateImportModel) templateParseConfig {
	cfg := templateParseConfig{}
	if model != nil {
		cfg.DateFormat = model.DateFormat
		cfg.Quality = model.Quality
	}
	cfg.DateFormat = normalizeTemplateDateFormat(cfg.DateFormat)
	cfg.Quality = normalizeTemplateQualityConfig(cfg.Quality)
	return cfg
}

func isDefaultQualityConfig(cfg templateQualityConfig) bool {
	cfg = normalizeTemplateQualityConfig(cfg)
	if cfg.Labels {
		return false
	}
	return cfg.Markers["dubtos"] == "?" &&
		cfg.Markers["no_consta"] == "¿" &&
		cfg.Markers["incomplet"] == "" &&
		cfg.Markers["illegible"] == ""
}

func mergeQualityStatus(values ...string) string {
	best := ""
	bestRank := 0
	for _, raw := range values {
		val := normalizeQualityStatus(raw)
		rank := qualityRank[val]
		if rank > bestRank {
			bestRank = rank
			best = val
		}
	}
	return best
}

func normalizeQualityStatus(raw string) string {
	normalized := normalizeQualityToken(raw)
	switch normalized {
	case "clar", "clara", "clear":
		return "clar"
	case "dubtos":
		return "dubtos"
	case "incomplet", "incomplete":
		return "incomplet"
	case "illegible", "ilegible":
		return "illegible"
	case "noconsta":
		return "no_consta"
	default:
		return strings.TrimSpace(raw)
	}
}

func normalizeQualityToken(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	raw = strings.Trim(raw, "[](){}.,;:!?\"'")
	raw = strings.ReplaceAll(raw, "·", "")
	raw = strings.ReplaceAll(raw, "_", "")
	raw = strings.ReplaceAll(raw, "-", "")
	raw = stripDiacritics(raw)
	raw = strings.ReplaceAll(raw, " ", "")
	return raw
}

func mapQualityLabel(raw string) string {
	switch normalizeQualityToken(raw) {
	case "clar", "clara", "clear":
		return "clar"
	case "dubtos":
		return "dubtos"
	case "incomplet", "incomplete":
		return "incomplet"
	case "illegible", "ilegible":
		return "illegible"
	case "noconsta":
		return "no_consta"
	default:
		return ""
	}
}

func stripQualityMarkers(raw string, cfg templateQualityConfig) (string, string) {
	if raw == "" {
		return raw, ""
	}
	status := ""
	cleaned := raw
	cfg = normalizeTemplateQualityConfig(cfg)
	for _, key := range []string{"no_consta", "illegible", "incomplet", "dubtos"} {
		marker := cfg.Markers[key]
		if marker == "" {
			continue
		}
		if strings.Contains(cleaned, marker) {
			cleaned = strings.ReplaceAll(cleaned, marker, "")
			status = mergeQualityStatus(status, key)
		}
	}
	cleaned = strings.TrimSpace(cleaned)
	return cleaned, status
}

func stripQualityLabel(raw string, cfg templateQualityConfig) (string, string) {
	if !cfg.Labels {
		return raw, ""
	}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return raw, ""
	}
	if idx := strings.LastIndex(trimmed, "("); idx != -1 && strings.HasSuffix(trimmed, ")") {
		label := strings.TrimSpace(trimmed[idx+1 : len(trimmed)-1])
		if status := mapQualityLabel(label); status != "" {
			return strings.TrimSpace(trimmed[:idx]), status
		}
	}
	if idx := strings.LastIndex(trimmed, "["); idx != -1 && strings.HasSuffix(trimmed, "]") {
		label := strings.TrimSpace(trimmed[idx+1 : len(trimmed)-1])
		if status := mapQualityLabel(label); status != "" {
			return strings.TrimSpace(trimmed[:idx]), status
		}
	}
	tokens := strings.Fields(trimmed)
	if len(tokens) >= 2 {
		if normalizeQualityToken(tokens[len(tokens)-2]) == "no" && normalizeQualityToken(tokens[len(tokens)-1]) == "consta" {
			return strings.TrimSpace(strings.Join(tokens[:len(tokens)-2], " ")), "no_consta"
		}
	}
	if len(tokens) > 0 {
		if status := mapQualityLabel(tokens[len(tokens)-1]); status != "" {
			return strings.TrimSpace(strings.Join(tokens[:len(tokens)-1], " ")), status
		}
	}
	return raw, ""
}

func extractQuality(raw string, cfg templateQualityConfig) (string, string) {
	cleaned, label := stripQualityLabel(raw, cfg)
	cleaned, marker := stripQualityMarkers(cleaned, cfg)
	return cleaned, mergeQualityStatus(label, marker)
}
