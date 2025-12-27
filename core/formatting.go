package core

import (
	"strings"
	"time"
)

func formatCronologiaDisplay(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	if strings.Contains(trimmed, ".") || strings.Contains(trimmed, "/") {
		return trimmed
	}
	if t, err := time.Parse("2006-01-02", trimmed); err == nil {
		return t.Format("02/01/2006")
	}
	return trimmed
}
