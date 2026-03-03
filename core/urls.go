package core

import (
	"net/http"
	"net/url"
	"strings"
)

// PublicBaseURL retorna la base pública configurada (sense slash final).
func PublicBaseURL(cfg map[string]string) string {
	if cfg == nil {
		return ""
	}
	raw := strings.TrimSpace(cfg["PUBLIC_BASE_URL"])
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return ""
	}
	u.RawQuery = ""
	u.Fragment = ""
	u.Path = strings.TrimRight(u.Path, "/")
	return u.String()
}

// PublicBaseOrigin retorna l'origen (scheme + host) de la base pública.
func PublicBaseOrigin(cfg map[string]string) string {
	base := PublicBaseURL(cfg)
	if base == "" {
		return ""
	}
	u, err := url.Parse(base)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return ""
	}
	return strings.ToLower(u.Scheme) + "://" + strings.ToLower(u.Host)
}

// BuildPublicURL construeix un URL absolut usant config o el request actual.
func BuildPublicURL(cfg map[string]string, r *http.Request, path string) string {
	base := PublicBaseURL(cfg)
	if base == "" && r != nil {
		scheme := requestScheme(r)
		host := requestHost(r)
		if scheme != "" && host != "" {
			base = scheme + "://" + host
		}
	}
	if base == "" {
		return ""
	}
	base = strings.TrimRight(base, "/")
	if path == "" {
		return base
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return base + path
}
