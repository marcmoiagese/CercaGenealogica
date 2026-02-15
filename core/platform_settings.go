package core

import (
	"strings"
	"sync"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type PlatformSettings struct {
	BrandName       string
	FooterTagline   string
	ContactEmail    string
	ContactLocation string
}

type platformSettingsStore struct {
	mu     sync.RWMutex
	db     db.DB
	loaded bool
	values map[string]string
}

var platformStore platformSettingsStore

func SetPlatformSettingsStore(database db.DB) {
	platformStore.mu.Lock()
	platformStore.db = database
	platformStore.loaded = false
	platformStore.values = nil
	platformStore.mu.Unlock()
}

func InvalidatePlatformSettingsCache() {
	platformStore.mu.Lock()
	platformStore.loaded = false
	platformStore.mu.Unlock()
}

func GetPlatformSettings(lang string) PlatformSettings {
	values := platformSettingsSnapshot()
	brand := strings.TrimSpace(values["site.brand_name"])
	if brand == "" {
		brand = T(lang, "app.brand")
	}
	tagline := strings.TrimSpace(values["site.footer_tagline"])
	if tagline == "" {
		tagline = T(lang, "footer.tagline")
	}
	email := strings.TrimSpace(values["site.contact_email"])
	if email == "" {
		email = T(lang, "footer.contact.email")
	}
	location := strings.TrimSpace(values["site.contact_location"])
	if location == "" {
		location = T(lang, "footer.contact.location")
	}
	return PlatformSettings{
		BrandName:       brand,
		FooterTagline:   tagline,
		ContactEmail:    email,
		ContactLocation: location,
	}
}

func platformSettingsSnapshot() map[string]string {
	platformStore.mu.RLock()
	if platformStore.loaded {
		snapshot := copyPlatformSettings(platformStore.values)
		platformStore.mu.RUnlock()
		return snapshot
	}
	platformStore.mu.RUnlock()

	platformStore.mu.Lock()
	defer platformStore.mu.Unlock()
	if platformStore.loaded {
		return copyPlatformSettings(platformStore.values)
	}
	values := map[string]string{}
	if platformStore.db != nil {
		if rows, err := platformStore.db.ListPlatformSettings(); err == nil {
			for _, row := range rows {
				values[row.Key] = row.Value
			}
		}
	}
	platformStore.values = values
	platformStore.loaded = true
	return copyPlatformSettings(values)
}

func copyPlatformSettings(values map[string]string) map[string]string {
	if values == nil {
		return map[string]string{}
	}
	out := make(map[string]string, len(values))
	for k, v := range values {
		out[k] = v
	}
	return out
}
