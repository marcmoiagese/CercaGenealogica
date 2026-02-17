package core

import (
	"net/http"
	"strconv"
	"strings"
)

type PageLink struct {
	Label   string
	URL     string
	Current bool
	IsNav   bool
}

type Pagination struct {
	Page       int
	PerPage    int
	Total      int
	TotalPages int
	Offset     int
	RangeStart int
	RangeEnd   int
	Links      []PageLink
	SelectBase string
	Anchor     string
}

func parseListPerPage(val string) int {
	switch strings.TrimSpace(val) {
	case "1":
		return 1
	case "5":
		return 5
	case "10":
		return 10
	case "25":
		return 25
	case "50":
		return 50
	case "100":
		return 100
	default:
		return 25
	}
}

func parseListPage(val string) int {
	if n, err := strconv.Atoi(strings.TrimSpace(val)); err == nil && n > 0 {
		return n
	}
	return 1
}

func buildPagination(r *http.Request, page, perPage, total int, anchor string) Pagination {
	if perPage <= 0 {
		perPage = 25
	}
	if page <= 0 {
		page = 1
	}
	totalPages := 1
	if total > 0 && perPage > 0 {
		totalPages = (total + perPage - 1) / perPage
	}
	if totalPages < 1 {
		totalPages = 1
	}
	if page > totalPages {
		page = totalPages
	}
	offset := (page - 1) * perPage
	if offset < 0 {
		offset = 0
	}
	rangeStart := 0
	rangeEnd := 0
	if total > 0 {
		rangeStart = offset + 1
		if rangeStart > total {
			rangeStart = total
		}
		rangeEnd = offset + perPage
		if rangeEnd > total {
			rangeEnd = total
		}
	}

	selectQuery := cloneValues(r.URL.Query())
	selectQuery.Del("page")
	selectQuery.Del("per_page")
	selectBase := r.URL.Path
	if len(selectQuery) > 0 {
		selectBase = selectBase + "?" + selectQuery.Encode() + "&per_page="
	} else {
		selectBase = selectBase + "?per_page="
	}

	pageQuery := cloneValues(r.URL.Query())
	pageQuery.Del("page")
	pageQuery.Set("per_page", strconv.Itoa(perPage))
	links := []PageLink{}
	addLink := func(label string, target int, current bool, isNav bool) {
		q := cloneValues(pageQuery)
		q.Set("page", strconv.Itoa(target))
		url := r.URL.Path + "?" + q.Encode()
		if anchor != "" {
			url += anchor
		}
		links = append(links, PageLink{Label: label, URL: url, Current: current, IsNav: isNav})
	}
	if totalPages > 1 {
		if page > 1 {
			addLink("<<", 1, false, true)
			addLink("<", page-1, false, true)
		}
		windowSize := 10
		start := 1
		end := totalPages
		if totalPages > windowSize {
			half := windowSize / 2
			start = page - half
			if start < 1 {
				start = 1
			}
			end = start + windowSize - 1
			if end > totalPages {
				end = totalPages
				start = end - windowSize + 1
			}
		}
		for i := start; i <= end; i++ {
			addLink(strconv.Itoa(i), i, i == page, false)
		}
		if page < totalPages {
			addLink(">", page+1, false, true)
			addLink(">>", totalPages, false, true)
		}
	}
	return Pagination{
		Page:       page,
		PerPage:    perPage,
		Total:      total,
		TotalPages: totalPages,
		Offset:     offset,
		RangeStart: rangeStart,
		RangeEnd:   rangeEnd,
		Links:      links,
		SelectBase: selectBase,
		Anchor:     anchor,
	}
}
