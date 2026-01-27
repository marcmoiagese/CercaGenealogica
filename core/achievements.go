package core

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type AchievementRuleFilters struct {
	RuleCodes   []string `json:"rule_codes,omitempty"`
	Actions     []string `json:"actions,omitempty"`
	ObjectTypes []string `json:"object_types,omitempty"`
	Status      []string `json:"status,omitempty"`
}

type AchievementRule struct {
	Type      string                 `json:"type"`
	Filters   AchievementRuleFilters `json:"filters,omitempty"`
	EventCode string                 `json:"event_code,omitempty"`
	Threshold int                    `json:"threshold,omitempty"`
	Window    string                 `json:"window,omitempty"`
	MinDays   int                    `json:"min_days,omitempty"`
	MinRatio  float64                `json:"min_ratio,omitempty"`
}

type AchievementTrigger struct {
	ActivityID int
	RuleCode   string
	Action     string
	ObjectType string
	ObjectID   int
	Status     string
	CreatedAt  time.Time
}

type AchievementsService struct {
	DB         db.DB
	Candidates []db.Achievement
	DryRun     bool
}

func NewAchievementsService(database db.DB) *AchievementsService {
	return &AchievementsService{DB: database}
}

func (a *App) EvaluateAchievementsForUser(ctx context.Context, userID int, trigger AchievementTrigger) {
	svc := NewAchievementsService(a.DB)
	if a.achievementCache != nil {
		candidates, err := a.achievementCache.candidatesForTrigger(a.DB, trigger)
		if err != nil {
			Errorf("Achievements cache error user=%d: %v", userID, err)
		} else {
			svc.Candidates = candidates
		}
	}
	if _, err := svc.EvaluateForUser(ctx, userID, trigger); err != nil {
		Errorf("Achievements evaluate error user=%d: %v", userID, err)
	}
}

func (s *AchievementsService) EvaluateForUser(ctx context.Context, userID int, trigger AchievementTrigger) ([]db.Achievement, error) {
	if userID <= 0 {
		return nil, errors.New("user_id invalid")
	}
	achievements := s.Candidates
	if achievements == nil {
		var err error
		achievements, err = s.DB.ListEnabledAchievements()
		if err != nil {
			return nil, err
		}
	}
	earned, err := s.DB.ListUserAchievements(userID)
	if err != nil {
		return nil, err
	}
	earnedSet := map[int]bool{}
	for _, item := range earned {
		earnedSet[item.AchievementID] = true
	}
	refTime := trigger.CreatedAt
	if refTime.IsZero() {
		refTime = time.Now()
	}
	awarded := []db.Achievement{}
	for _, ach := range achievements {
		if !ach.IsRepeatable && earnedSet[ach.ID] {
			continue
		}
		rule, err := parseAchievementRule(ach.RuleJSON)
		if err != nil {
			continue
		}
		if rule.EventCode != "" {
			active, err := s.DB.IsAchievementEventActive(rule.EventCode, refTime)
			if err != nil || !active {
				continue
			}
		}
		ok, meta, err := s.evaluateRuleForUser(userID, rule, refTime)
		if err != nil || !ok {
			continue
		}
		if meta == nil {
			meta = map[string]interface{}{}
		}
		meta["rule_type"] = rule.Type
		meta["achievement_code"] = ach.Code
		meta["evaluated_at"] = time.Now().Format(time.RFC3339)
		if trigger.ActivityID > 0 {
			meta["activity_id"] = trigger.ActivityID
		}
		if trigger.RuleCode != "" {
			meta["rule_code"] = trigger.RuleCode
		}
		if trigger.Action != "" {
			meta["action"] = trigger.Action
		}
		if trigger.ObjectType != "" {
			meta["object_type"] = trigger.ObjectType
		}
		if trigger.ObjectID > 0 {
			meta["object_id"] = trigger.ObjectID
		}
		if trigger.Status != "" {
			meta["status"] = trigger.Status
		}
		metaJSON, _ := json.Marshal(meta)
		if !s.DryRun {
			inserted, err := s.DB.AwardAchievement(userID, ach.ID, "active", string(metaJSON))
			if err != nil || !inserted {
				continue
			}
		}
		awarded = append(awarded, ach)
	}
	_ = ctx
	return awarded, nil
}

func (s *AchievementsService) GetUserAchievements(userID int) ([]db.AchievementUserView, error) {
	return s.DB.ListUserAchievements(userID)
}

func (s *AchievementsService) GetUserShowcase(userID int) ([]db.AchievementShowcaseView, error) {
	return s.DB.ListUserShowcase(userID)
}

func (s *AchievementsService) evaluateRuleForUser(userID int, rule AchievementRule, refTime time.Time) (bool, map[string]interface{}, error) {
	filter := db.AchievementActivityFilter{
		UserID:      userID,
		RuleCodes:   rule.Filters.RuleCodes,
		Actions:     rule.Filters.Actions,
		ObjectTypes: rule.Filters.ObjectTypes,
		Statuses:    rule.Filters.Status,
	}
	switch rule.Type {
	case "count":
		if rule.Threshold <= 0 {
			return false, nil, errors.New("threshold invalid")
		}
		total, err := s.DB.CountUserActivities(filter)
		if err != nil {
			return false, nil, err
		}
		meta := map[string]interface{}{
			"count":     total,
			"threshold": rule.Threshold,
		}
		return total >= rule.Threshold, meta, nil
	case "sum_points":
		if rule.Threshold <= 0 {
			return false, nil, errors.New("threshold invalid")
		}
		total, err := s.DB.SumUserActivityPoints(filter)
		if err != nil {
			return false, nil, err
		}
		meta := map[string]interface{}{
			"points":    total,
			"threshold": rule.Threshold,
		}
		return total >= rule.Threshold, meta, nil
	case "burst_count":
		if rule.Threshold <= 0 {
			return false, nil, errors.New("threshold invalid")
		}
		window, err := parseAchievementWindow(rule.Window)
		if err != nil {
			return false, nil, err
		}
		filter.From = refTime.Add(-window)
		filter.To = refTime
		total, err := s.DB.CountUserActivities(filter)
		if err != nil {
			return false, nil, err
		}
		meta := map[string]interface{}{
			"count":     total,
			"threshold": rule.Threshold,
			"window":    rule.Window,
		}
		return total >= rule.Threshold, meta, nil
	case "streak_days":
		if rule.MinDays <= 0 {
			return false, nil, errors.New("min_days invalid")
		}
		days, err := s.DB.ListUserActivityDays(filter)
		if err != nil {
			return false, nil, err
		}
		maxStreak := maxConsecutiveDays(days)
		meta := map[string]interface{}{
			"streak":  maxStreak,
			"minDays": rule.MinDays,
		}
		return maxStreak >= rule.MinDays, meta, nil
	case "count_distinct":
		if rule.Threshold <= 0 {
			return false, nil, errors.New("threshold invalid")
		}
		total, err := s.DB.CountUserActivitiesDistinctObject(filter)
		if err != nil {
			return false, nil, err
		}
		meta := map[string]interface{}{
			"distinct_count": total,
			"threshold":      rule.Threshold,
		}
		return total >= rule.Threshold, meta, nil
	case "ratio_approved":
		if rule.MinRatio <= 0 || rule.MinRatio > 1 {
			return false, nil, errors.New("min_ratio invalid")
		}
		approvedStatuses := rule.Filters.Status
		if len(approvedStatuses) == 0 {
			approvedStatuses = []string{"validat"}
		}
		approvedFilter := filter
		approvedFilter.Statuses = approvedStatuses
		totalFilter := filter
		totalFilter.Statuses = nil
		total, err := s.DB.CountUserActivities(totalFilter)
		if err != nil {
			return false, nil, err
		}
		approved, err := s.DB.CountUserActivities(approvedFilter)
		if err != nil {
			return false, nil, err
		}
		if rule.Threshold > 0 && total < rule.Threshold {
			meta := map[string]interface{}{
				"approved":  approved,
				"total":     total,
				"ratio":     0.0,
				"min_ratio": rule.MinRatio,
				"min_total": rule.Threshold,
			}
			return false, meta, nil
		}
		ratio := 0.0
		if total > 0 {
			ratio = float64(approved) / float64(total)
		}
		meta := map[string]interface{}{
			"approved":  approved,
			"total":     total,
			"ratio":     ratio,
			"min_ratio": rule.MinRatio,
			"min_total": rule.Threshold,
		}
		return total > 0 && ratio >= rule.MinRatio, meta, nil
	default:
		return false, nil, errors.New("invalid rule type")
	}
}

func parseAchievementRule(ruleJSON string) (AchievementRule, error) {
	var rule AchievementRule
	dec := json.NewDecoder(strings.NewReader(ruleJSON))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rule); err != nil {
		return rule, err
	}
	rule.Type = strings.ToLower(strings.TrimSpace(rule.Type))
	rule.Filters.RuleCodes = normalizeStringList(rule.Filters.RuleCodes)
	rule.Filters.Actions = normalizeStringList(rule.Filters.Actions)
	rule.Filters.ObjectTypes = normalizeStringList(rule.Filters.ObjectTypes)
	rule.Filters.Status = normalizeStringList(rule.Filters.Status)
	rule.EventCode = strings.TrimSpace(rule.EventCode)
	switch rule.Type {
	case "count", "sum_points":
		if rule.Threshold <= 0 {
			return rule, errors.New("threshold invalid")
		}
	case "count_distinct":
		if rule.Threshold <= 0 {
			return rule, errors.New("threshold invalid")
		}
	case "ratio_approved":
		if rule.MinRatio <= 0 || rule.MinRatio > 1 {
			return rule, errors.New("min_ratio invalid")
		}
		if rule.Threshold < 0 {
			return rule, errors.New("threshold invalid")
		}
	case "burst_count":
		if rule.Threshold <= 0 {
			return rule, errors.New("threshold invalid")
		}
		if _, err := parseAchievementWindow(rule.Window); err != nil {
			return rule, err
		}
	case "streak_days":
		if rule.MinDays <= 0 {
			return rule, errors.New("min_days invalid")
		}
	default:
		return rule, errors.New("rule type invalid")
	}
	return rule, nil
}

func parseAchievementWindow(window string) (time.Duration, error) {
	switch strings.TrimSpace(window) {
	case "24h":
		return 24 * time.Hour, nil
	case "48h":
		return 48 * time.Hour, nil
	case "7d":
		return 7 * 24 * time.Hour, nil
	default:
		return 0, errors.New("window invalid")
	}
}

func normalizeStringList(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, val := range values {
		val = strings.TrimSpace(val)
		if val == "" {
			continue
		}
		if !seen[val] {
			seen[val] = true
			out = append(out, val)
		}
	}
	return out
}

func maxConsecutiveDays(days []time.Time) int {
	if len(days) == 0 {
		return 0
	}
	maxStreak := 1
	current := 1
	for i := 1; i < len(days); i++ {
		prev := days[i-1]
		next := days[i]
		if sameDay(prev.AddDate(0, 0, 1), next) {
			current++
		} else {
			current = 1
		}
		if current > maxStreak {
			maxStreak = current
		}
	}
	return maxStreak
}

func sameDay(a, b time.Time) bool {
	return a.Year() == b.Year() && a.Month() == b.Month() && a.Day() == b.Day()
}

func (a *App) PerfilAchievementsAPI(w http.ResponseWriter, r *http.Request) {
	base := strings.TrimPrefix(r.URL.Path, "/api/perfil/achievements")
	base = strings.Trim(base, "/")
	user := userFromContext(r)
	if user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	svc := NewAchievementsService(a.DB)
	switch {
	case base == "":
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		items, err := svc.GetUserAchievements(user.ID)
		if err != nil {
			http.Error(w, "failed to load", http.StatusInternalServerError)
			return
		}
		showcase, err := svc.GetUserShowcase(user.ID)
		if err != nil {
			http.Error(w, "failed to load", http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]interface{}{
			"items":    buildAchievementPayload(items),
			"showcase": buildShowcasePayload(showcase),
		})
	case base == "showcase":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var payload struct {
			AchievementID int    `json:"achievement_id"`
			Slot          int    `json:"slot"`
			CSRFToken     string `json:"csrf_token"`
		}
		if err := decodeAchievementJSON(w, r, &payload); err != nil {
			http.Error(w, "invalid payload", http.StatusBadRequest)
			return
		}
		token := readCSRFToken(r, payload.CSRFToken)
		if !validateCSRF(r, token) {
			http.Error(w, "invalid csrf", http.StatusBadRequest)
			return
		}
		if payload.Slot <= 0 {
			http.Error(w, "invalid slot", http.StatusBadRequest)
			return
		}
		if payload.AchievementID <= 0 {
			if err := a.DB.ClearUserShowcaseSlot(user.ID, payload.Slot); err != nil {
				http.Error(w, "failed to clear", http.StatusInternalServerError)
				return
			}
			writeJSON(w, map[string]interface{}{"ok": true})
			return
		}
		items, err := svc.GetUserAchievements(user.ID)
		if err != nil {
			http.Error(w, "failed to load", http.StatusInternalServerError)
			return
		}
		hasAchievement := false
		for _, item := range items {
			if item.AchievementID == payload.AchievementID {
				hasAchievement = true
				break
			}
		}
		if !hasAchievement {
			http.Error(w, "achievement not owned", http.StatusForbidden)
			return
		}
		if err := a.DB.SetUserShowcaseSlot(user.ID, payload.AchievementID, payload.Slot); err != nil {
			http.Error(w, "failed to save", http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]interface{}{"ok": true})
	default:
		http.NotFound(w, r)
	}
}

func decodeAchievementJSON(w http.ResponseWriter, r *http.Request, payload interface{}) error {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	dec := json.NewDecoder(r.Body)
	return dec.Decode(payload)
}

func buildAchievementPayload(items []db.AchievementUserView) []map[string]interface{} {
	payload := make([]map[string]interface{}, 0, len(items))
	for _, item := range items {
		entry := map[string]interface{}{
			"id":          item.AchievementID,
			"code":        item.Code,
			"name":        item.Name,
			"description": item.Description,
			"rarity":      item.Rarity,
			"visibility":  item.Visibility,
			"domain":      item.Domain,
			"status":      item.Status,
		}
		if item.AwardedAt.Valid {
			entry["awarded_at"] = item.AwardedAt.Time.Format(time.RFC3339)
		}
		if item.MetaJSON.Valid && strings.TrimSpace(item.MetaJSON.String) != "" {
			var meta interface{}
			if err := json.Unmarshal([]byte(item.MetaJSON.String), &meta); err == nil {
				entry["meta"] = meta
			}
		}
		if item.IconPublicID.Valid && strings.TrimSpace(item.IconPublicID.String) != "" {
			entry["icon_url"] = "/media/items/" + item.IconPublicID.String + "/thumb"
		}
		payload = append(payload, entry)
	}
	return payload
}

func buildShowcasePayload(items []db.AchievementShowcaseView) []map[string]interface{} {
	payload := make([]map[string]interface{}, 0, len(items))
	for _, item := range items {
		entry := map[string]interface{}{
			"slot":        item.Slot,
			"id":          item.AchievementID,
			"code":        item.Code,
			"name":        item.Name,
			"description": item.Description,
			"rarity":      item.Rarity,
			"visibility":  item.Visibility,
			"domain":      item.Domain,
			"status":      item.Status,
		}
		if item.AwardedAt.Valid {
			entry["awarded_at"] = item.AwardedAt.Time.Format(time.RFC3339)
		}
		if item.MetaJSON.Valid && strings.TrimSpace(item.MetaJSON.String) != "" {
			var meta interface{}
			if err := json.Unmarshal([]byte(item.MetaJSON.String), &meta); err == nil {
				entry["meta"] = meta
			}
		}
		if item.IconPublicID.Valid && strings.TrimSpace(item.IconPublicID.String) != "" {
			entry["icon_url"] = "/media/items/" + item.IconPublicID.String + "/thumb"
		}
		payload = append(payload, entry)
	}
	return payload
}
