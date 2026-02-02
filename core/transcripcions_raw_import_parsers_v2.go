package core

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

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
			cognom2 = restTokens[0]
			cognom2Qual = restQuals[0]
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

func splitParentheticalNotes(extras []string) (string, string) {
	if len(extras) == 0 {
		return "", ""
	}
	if len(extras) == 1 {
		return "", extras[0]
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

func parseMarriageOrder(raw string) (int, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	lower := strings.ToLower(raw)
	reParen := regexp.MustCompile(`\(\s*(\d+)\s*[rnt]?\s*\)`)
	if match := reParen.FindStringSubmatch(lower); len(match) > 1 {
		if n, err := strconv.Atoi(match[1]); err == nil {
			return n, true
		}
	}
	reOrdinal := regexp.MustCompile(`\b(\d+)\s*(r|n|t)\b`)
	if match := reOrdinal.FindStringSubmatch(lower); len(match) > 1 {
		if n, err := strconv.Atoi(match[1]); err == nil {
			return n, true
		}
	}
	reMatAfter := regexp.MustCompile(`matrimoni\s*(\d+)`)
	if match := reMatAfter.FindStringSubmatch(lower); len(match) > 1 {
		if n, err := strconv.Atoi(match[1]); err == nil {
			return n, true
		}
	}
	reMatBefore := regexp.MustCompile(`\b(\d+)\s*(?:r|n|t)?\s*matrimoni\b`)
	if match := reMatBefore.FindStringSubmatch(lower); len(match) > 1 {
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
