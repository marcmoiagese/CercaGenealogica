package core

import (
	"encoding/json"
	"os"
	"strings"
	"sync"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

// App encapsula dependències compartides per evitar reobrir recursos per petició.
type App struct {
	Config map[string]string
	DB     db.DB
	Mail   MailConfig
	permCache           *permissionCache
	llibreTargetCache   *targetCache
	arxiuTargetCache    *targetCache
	municipiTargetCache *targetCache
	achievementCache    *achievementCache
	searchIndexOnce     sync.Once
}

func NewApp(cfg map[string]string, database db.DB) *App {
	return &App{
		Config:              cfg,
		DB:                  database,
		Mail:                NewMailConfig(cfg),
		permCache:           newPermissionCache(),
		llibreTargetCache:   newTargetCache(targetCacheTTL, llibreTargetCacheMax),
		arxiuTargetCache:    newTargetCache(targetCacheTTL, arxiuTargetCacheMax),
		municipiTargetCache: newTargetCache(targetCacheTTL, municipiTargetCacheMax),
		achievementCache:    newAchievementCache(),
	}
}

func (a *App) Close() {
	if a.DB != nil {
		a.DB.Close()
	}
}

type countryLabelCache struct {
	raw    []map[string]interface{}
	labels map[string]map[string]string
	loaded bool
}

var clCache countryLabelCache

func (a *App) loadCountryLabels() {
	if clCache.loaded {
		return
	}
	content, err := os.ReadFile("static/json/countries.json")
	if err != nil {
		clCache.loaded = true
		return
	}
	var raw []map[string]interface{}
	if err := json.Unmarshal(content, &raw); err != nil {
		clCache.loaded = true
		return
	}
	clCache.raw = raw
	clCache.labels = make(map[string]map[string]string)
	clCache.loaded = true
}

// countryLabelFromISO retorna el nom del país (segons idioma) a partir del codi ISO2.
func (a *App) countryLabelFromISO(iso2 string, lang string) string {
	a.loadCountryLabels()
	if clCache.raw == nil {
		return iso2
	}
	iso2 = strings.ToUpper(strings.TrimSpace(iso2))
	if iso2 == "" {
		return ""
	}
	key := strings.ToLower(strings.TrimSpace(lang))
	if key == "" {
		key = "en"
	}
	if lbls, ok := clCache.labels[key]; ok {
		if v, ok := lbls[iso2]; ok {
			return v
		}
	}
	lbls := make(map[string]string)
	for _, c := range clCache.raw {
		code, _ := c["alpha2"].(string)
		code = strings.ToUpper(strings.TrimSpace(code))
		if code == "" {
			continue
		}
		lbls[code] = pickCountryLabel(c, key)
	}
	clCache.labels[key] = lbls
	if v, ok := lbls[iso2]; ok {
		return v
	}
	return iso2
}
