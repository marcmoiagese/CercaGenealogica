package db

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
)

var ErrInvalidExternalURL = errors.New("invalid external url")

type ExternalSite struct {
	ID         int
	Slug       string
	Name       string
	Domains    string
	IconPath   sql.NullString
	AccessMode string
	IsActive   bool
	CreatedAt  sql.NullTime
	UpdatedAt  sql.NullTime
}

type ExternalLink struct {
	ID              int
	PersonaID       int
	SiteID          sql.NullInt64
	URL             string
	URLNorm         string
	Title           sql.NullString
	Meta            sql.NullString
	Status          string
	CreatedByUserID sql.NullInt64
	CreatedAt       sql.NullTime
	UpdatedAt       sql.NullTime
}

type ExternalLinkRow struct {
	ExternalLink
	SiteSlug       sql.NullString
	SiteName       sql.NullString
	SiteIconPath   sql.NullString
	SiteAccessMode sql.NullString
}

type ExternalLinkAdminRow struct {
	ExternalLinkRow
	PersonaNom        sql.NullString
	PersonaCognom1    sql.NullString
	PersonaCognom2    sql.NullString
	PersonaNomComplet sql.NullString
}

func ParseExternalDomains(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var list []string
	if strings.HasPrefix(raw, "[") {
		_ = json.Unmarshal([]byte(raw), &list)
	}
	if len(list) == 0 {
		list = strings.FieldsFunc(raw, func(r rune) bool {
			return r == '\n' || r == ',' || r == ';'
		})
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(list))
	for _, item := range list {
		d := strings.ToLower(strings.TrimSpace(item))
		if d == "" {
			continue
		}
		d = strings.TrimPrefix(d, "http://")
		d = strings.TrimPrefix(d, "https://")
		d = strings.TrimPrefix(d, "//")
		d = strings.TrimSuffix(d, "/")
		d = strings.TrimPrefix(d, "www.")
		d = strings.TrimPrefix(d, "*.")
		d = strings.TrimPrefix(d, ".")
		if d == "" {
			continue
		}
		if _, ok := seen[d]; ok {
			continue
		}
		seen[d] = struct{}{}
		out = append(out, d)
	}
	return out
}

func MatchExternalDomain(host, domain string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	domain = strings.ToLower(strings.TrimSpace(domain))
	if host == "" || domain == "" {
		return false
	}
	if host == domain {
		return true
	}
	return strings.HasSuffix(host, "."+domain)
}

func NormalizeExternalURL(raw string) (string, string, string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", "", ErrInvalidExternalURL
	}
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", "", "", ErrInvalidExternalURL
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", "", "", ErrInvalidExternalURL
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "" {
		return "", "", "", ErrInvalidExternalURL
	}
	port := parsed.Port()
	if (scheme == "http" && port == "80") || (scheme == "https" && port == "443") {
		port = ""
	}
	hostForURL := host
	if port != "" {
		hostForURL = host + ":" + port
	}
	parsed.Scheme = "https"
	parsed.Host = hostForURL
	parsed.Fragment = ""

	path := strings.TrimRight(parsed.Path, "/")
	if path == "/" {
		path = ""
	}
	parsed.Path = path

	query := parsed.Query()
	for key := range query {
		if isExternalTrackingParam(key) {
			query.Del(key)
		}
	}
	parsed.RawQuery = query.Encode()

	clean := parsed.String()
	if clean == "" {
		return "", "", "", ErrInvalidExternalURL
	}
	return clean, clean, host, nil
}

func isExternalTrackingParam(key string) bool {
	key = strings.ToLower(strings.TrimSpace(key))
	if key == "" {
		return false
	}
	if strings.HasPrefix(key, "utm_") {
		return true
	}
	switch key {
	case "fbclid", "gclid", "igshid", "mc_cid", "mc_eid":
		return true
	default:
		return false
	}
}
