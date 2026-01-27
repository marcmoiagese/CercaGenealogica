package core

import (
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func summarizeHistoriaText(text string, max int) string {
	clean := strings.TrimSpace(text)
	if clean == "" || max <= 0 {
		return ""
	}
	clean = strings.Join(strings.Fields(clean), " ")
	runes := []rune(clean)
	if len(runes) <= max {
		return clean
	}
	if max <= 3 {
		return string(runes[:max])
	}
	trimmed := strings.TrimSpace(string(runes[:max-3]))
	if trimmed == "" {
		return ""
	}
	return trimmed + "..."
}

func historiaDateLabel(item db.MunicipiHistoriaFetVersion) string {
	if label := strings.TrimSpace(item.DataDisplay); label != "" {
		return label
	}
	start := strings.TrimSpace(item.DataInici)
	end := strings.TrimSpace(item.DataFi)
	if start != "" || end != "" {
		if start == "" {
			return end
		}
		if end == "" || start == end {
			return start
		}
		return start + "-" + end
	}
	startYear := ""
	endYear := ""
	if item.AnyInici.Valid {
		startYear = strconv.FormatInt(item.AnyInici.Int64, 10)
	}
	if item.AnyFi.Valid {
		endYear = strconv.FormatInt(item.AnyFi.Int64, 10)
	}
	if startYear != "" || endYear != "" {
		if startYear == "" {
			return endYear
		}
		if endYear == "" || startYear == endYear {
			return startYear
		}
		return startYear + "-" + endYear
	}
	return "-"
}
