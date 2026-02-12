package core

import "net/url"

func withQueryParams(base string, params map[string]string) string {
	if base == "" || len(params) == 0 {
		return base
	}
	u, err := url.Parse(base)
	if err != nil {
		return base
	}
	q := u.Query()
	for key, value := range params {
		if key == "" {
			continue
		}
		q.Set(key, value)
	}
	u.RawQuery = q.Encode()
	return u.String()
}
