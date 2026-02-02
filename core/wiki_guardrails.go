package core

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const (
	defaultWikiChangeRateLimit = 0.5  // 30 per minut
	defaultWikiChangeRateBurst = 30.0 // burst de 30
	defaultWikiMarkRateLimit   = 1.0  // 60 per minut
	defaultWikiMarkRateBurst   = 60.0 // burst de 60

	defaultWikiMetaMaxBytes     = 64 * 1024
	defaultWikiPendingPerUser   = 10
	defaultWikiPendingPerObject = 200
)

var (
	errWikiMetadataTooLarge   = errors.New("wiki metadata too large")
	errWikiPendingUserLimit   = errors.New("wiki pending per user limit")
	errWikiPendingObjectLimit = errors.New("wiki pending per object limit")
	errWikiRateLimited        = errors.New("wiki rate limited")
)

func (a *App) wikiGuardrailInfo(lang string, err error) (int, string, bool) {
	switch {
	case errors.Is(err, errWikiRateLimited):
		return http.StatusTooManyRequests, T(lang, "wiki.guardrail.rate"), true
	case errors.Is(err, errWikiMetadataTooLarge):
		return http.StatusBadRequest, T(lang, "wiki.guardrail.meta"), true
	case errors.Is(err, errWikiPendingUserLimit):
		return http.StatusBadRequest, T(lang, "wiki.guardrail.pending_user"), true
	case errors.Is(err, errWikiPendingObjectLimit):
		return http.StatusBadRequest, T(lang, "wiki.guardrail.pending_object"), true
	default:
		return 0, "", false
	}
}

func (a *App) ensureWikiChangeAllowed(w http.ResponseWriter, r *http.Request, lang string) bool {
	if a == nil || r == nil {
		return false
	}
	if !a.allowWikiChange(r) {
		http.Error(w, T(lang, "wiki.guardrail.rate"), http.StatusTooManyRequests)
		return false
	}
	return true
}

func (a *App) ensureWikiMarkAllowed(w http.ResponseWriter, r *http.Request, lang string) bool {
	if a == nil || r == nil {
		return false
	}
	if !a.allowWikiMark(r) {
		http.Error(w, T(lang, "wiki.guardrail.rate"), http.StatusTooManyRequests)
		return false
	}
	return true
}

func (a *App) allowWikiChange(r *http.Request) bool {
	rate, burst := a.wikiChangeRateLimit()
	return allowRouteLimit(r, "/wiki/change", rate, burst)
}

func (a *App) allowWikiMark(r *http.Request) bool {
	rate, burst := a.wikiMarkRateLimit()
	return allowRouteLimit(r, "/wiki/mark", rate, burst)
}

func (a *App) wikiChangeRateLimit() (float64, float64) {
	rate := configFloat(a.Config, "WIKI_CHANGE_RATE", defaultWikiChangeRateLimit)
	burst := configFloat(a.Config, "WIKI_CHANGE_BURST", defaultWikiChangeRateBurst)
	return rate, burst
}

func (a *App) wikiMarkRateLimit() (float64, float64) {
	rate := configFloat(a.Config, "WIKI_MARK_RATE", defaultWikiMarkRateLimit)
	burst := configFloat(a.Config, "WIKI_MARK_BURST", defaultWikiMarkRateBurst)
	return rate, burst
}

func (a *App) wikiMetaMaxBytes() int {
	return configInt(a.Config, "WIKI_META_MAX_BYTES", defaultWikiMetaMaxBytes)
}

func (a *App) wikiPendingPerUser() int {
	return configInt(a.Config, "WIKI_PENDING_PER_USER", defaultWikiPendingPerUser)
}

func (a *App) wikiPendingPerObject() int {
	return configInt(a.Config, "WIKI_PENDING_PER_OBJECT", defaultWikiPendingPerObject)
}

func (a *App) applyWikiChangeGuardrails(change *db.WikiChange) error {
	if change == nil {
		return fmt.Errorf("canvi buit")
	}
	metaLimit := a.wikiMetaMaxBytes()
	if metaLimit > 0 && len([]byte(change.Metadata)) > metaLimit {
		return errWikiMetadataTooLarge
	}
	state := strings.TrimSpace(change.ModeracioEstat)
	if state == "" {
		state = "pendent"
	}
	if state != "pendent" {
		return nil
	}
	if perObj := a.wikiPendingPerObject(); perObj > 0 {
		count, err := a.countWikiPending(change.ObjectType, change.ObjectID, 0)
		if err != nil {
			return err
		}
		if count >= perObj {
			return errWikiPendingObjectLimit
		}
	}
	if perUser := a.wikiPendingPerUser(); perUser > 0 && change.ChangedBy.Valid {
		count, err := a.countWikiPending(change.ObjectType, change.ObjectID, int(change.ChangedBy.Int64))
		if err != nil {
			return err
		}
		if count >= perUser {
			return errWikiPendingUserLimit
		}
	}
	return nil
}

func (a *App) countWikiPending(objectType string, objectID int, userID int) (int, error) {
	if a == nil || a.DB == nil {
		return 0, fmt.Errorf("db no disponible")
	}
	if strings.TrimSpace(objectType) == "" || objectID <= 0 {
		return 0, fmt.Errorf("objecte invàlid")
	}
	query := `SELECT COUNT(*) AS n FROM wiki_canvis WHERE object_type = ? AND object_id = ? AND moderation_status = 'pendent'`
	args := []interface{}{objectType, objectID}
	if userID > 0 {
		query += " AND changed_by = ?"
		args = append(args, userID)
	}
	rows, err := a.DB.Query(query, args...)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return parseCountValue(rows[0]["n"])
}

func parseCountValue(raw interface{}) (int, error) {
	switch v := raw.(type) {
	case int:
		return v, nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	case float64:
		return int(v), nil
	case []byte:
		n, err := strconv.Atoi(string(v))
		if err != nil {
			return 0, err
		}
		return n, nil
	case string:
		n, err := strconv.Atoi(v)
		if err != nil {
			return 0, err
		}
		return n, nil
	default:
		return 0, fmt.Errorf("tipus inesperat per count: %T", raw)
	}
}

func configInt(cfg map[string]string, key string, def int) int {
	if cfg == nil {
		return def
	}
	raw, ok := cfg[key]
	if !ok {
		return def
	}
	val, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || val <= 0 {
		return def
	}
	return val
}

func configFloat(cfg map[string]string, key string, def float64) float64 {
	if cfg == nil {
		return def
	}
	raw, ok := cfg[key]
	if !ok {
		return def
	}
	val, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || val <= 0 {
		return def
	}
	return val
}
