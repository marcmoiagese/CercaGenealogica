package core

import "strings"

func extractSurnameCandidate(q string) string {
	q = strings.TrimSpace(q)
	if q == "" {
		return ""
	}
	parts := strings.Fields(q)
	if len(parts) == 0 {
		return ""
	}
	cand := parts[len(parts)-1]
	return strings.Trim(cand, " ,.;:")
}
