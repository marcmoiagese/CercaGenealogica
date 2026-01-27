package core

import (
	"strings"
	"sync"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type achievementCache struct {
	mu         sync.RWMutex
	loaded     bool
	items      []db.Achievement
	globals    []db.Achievement
	byRuleCode map[string][]db.Achievement
}

func newAchievementCache() *achievementCache {
	return &achievementCache{
		byRuleCode: make(map[string][]db.Achievement),
	}
}

func (c *achievementCache) invalidate() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.loaded = false
	c.items = nil
	c.globals = nil
	c.byRuleCode = make(map[string][]db.Achievement)
	c.mu.Unlock()
}

func (c *achievementCache) ensureLoaded(database db.DB) error {
	if c == nil || database == nil {
		return nil
	}
	c.mu.RLock()
	loaded := c.loaded
	c.mu.RUnlock()
	if loaded {
		return nil
	}
	return c.load(database)
}

func (c *achievementCache) load(database db.DB) error {
	if database == nil {
		return nil
	}
	achievements, err := database.ListEnabledAchievements()
	if err != nil {
		return err
	}
	globals := make([]db.Achievement, 0, len(achievements))
	byRuleCode := make(map[string][]db.Achievement)
	for _, ach := range achievements {
		rule, err := parseAchievementRule(ach.RuleJSON)
		if err != nil || len(rule.Filters.RuleCodes) == 0 {
			globals = append(globals, ach)
			continue
		}
		for _, code := range rule.Filters.RuleCodes {
			if code == "" {
				continue
			}
			byRuleCode[code] = append(byRuleCode[code], ach)
		}
	}
	c.mu.Lock()
	c.items = achievements
	c.globals = globals
	c.byRuleCode = byRuleCode
	c.loaded = true
	c.mu.Unlock()
	return nil
}

func (c *achievementCache) candidatesForTrigger(database db.DB, trigger AchievementTrigger) ([]db.Achievement, error) {
	if c == nil {
		return nil, nil
	}
	if err := c.ensureLoaded(database); err != nil {
		return nil, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.loaded {
		return nil, nil
	}
	code := strings.TrimSpace(trigger.RuleCode)
	if code == "" {
		items := make([]db.Achievement, len(c.items))
		copy(items, c.items)
		return items, nil
	}
	candidates := make([]db.Achievement, 0, len(c.globals)+len(c.byRuleCode[code]))
	seen := make(map[int]bool)
	for _, ach := range c.globals {
		if seen[ach.ID] {
			continue
		}
		seen[ach.ID] = true
		candidates = append(candidates, ach)
	}
	for _, ach := range c.byRuleCode[code] {
		if seen[ach.ID] {
			continue
		}
		seen[ach.ID] = true
		candidates = append(candidates, ach)
	}
	return candidates, nil
}
