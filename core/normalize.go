package core

import (
	"strings"
	"unicode"
)

// NormalizeCognomKey genera una clau normalitzada per deduplicar cognoms.
func NormalizeCognomKey(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	s = strings.ToLower(s)
	s = stripDiacritics(s)
	s = strings.ReplaceAll(s, "’", "")
	s = strings.ReplaceAll(s, "'", "")
	s = strings.ReplaceAll(s, "-", " ")
	s = strings.ReplaceAll(s, ".", " ")
	s = strings.ReplaceAll(s, ",", " ")
	s = strings.Join(strings.Fields(s), " ")
	s = strings.ReplaceAll(s, " ", "")
	return strings.ToUpper(s)
}

// NormalizeNomKey genera una clau normalitzada per deduplicar noms.
func NormalizeNomKey(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	s = strings.ToLower(s)
	s = stripDiacritics(s)
	s = strings.ReplaceAll(s, "’", "")
	s = strings.ReplaceAll(s, "'", "")
	s = strings.ReplaceAll(s, "-", " ")
	s = strings.ReplaceAll(s, ".", " ")
	s = strings.ReplaceAll(s, ",", " ")
	s = strings.Join(strings.Fields(s), " ")
	s = strings.ReplaceAll(s, " ", "")
	return strings.ToUpper(s)
}

func sanitizeCognomLiteral(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "\"'“”«»")
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.ContainsAny(value, "0123456789") {
		return ""
	}
	if strings.ContainsAny(value, "()[]{}") {
		return ""
	}
	for _, r := range value {
		if unicode.IsLetter(r) || r == ' ' || r == '-' || r == '\'' || r == '’' || r == '·' || r == '.' {
			continue
		}
		return ""
	}
	value = strings.Join(strings.Fields(value), " ")
	if len([]rune(value)) < 2 {
		return ""
	}
	return value
}

func sanitizeNomLiteral(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "\"'“”«»")
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.ContainsAny(value, "0123456789") {
		return ""
	}
	if strings.ContainsAny(value, "()[]{}") {
		return ""
	}
	for _, r := range value {
		if unicode.IsLetter(r) || r == ' ' || r == '-' || r == '\'' || r == '’' || r == '·' {
			continue
		}
		return ""
	}
	value = strings.Join(strings.Fields(value), " ")
	if len([]rune(value)) < 2 {
		return ""
	}
	return value
}
