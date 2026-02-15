package core

import (
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const maintenanceCacheTTL = 30 * time.Second
const maintenanceTimeLayout = "2006-01-02 15:04"

type MaintenanceBanner struct {
	ID          int
	Title       string
	Message     string
	Severity    string
	CTALabel    string
	CTAURL      string
	Dismissible bool
	State       string
	StartsAt    string
	EndsAt      string
}

type maintenanceStoreState struct {
	mu      sync.RWMutex
	db      db.DB
	cache   *MaintenanceBanner
	cacheAt time.Time
}

var maintenanceStore maintenanceStoreState

func SetMaintenanceStore(database db.DB) {
	maintenanceStore.mu.Lock()
	maintenanceStore.db = database
	maintenanceStore.cache = nil
	maintenanceStore.cacheAt = time.Time{}
	maintenanceStore.mu.Unlock()
}

func InvalidateMaintenanceCache() {
	maintenanceStore.mu.Lock()
	maintenanceStore.cache = nil
	maintenanceStore.cacheAt = time.Time{}
	maintenanceStore.mu.Unlock()
}

func GetMaintenanceBanner(now time.Time) *MaintenanceBanner {
	maintenanceStore.mu.RLock()
	if maintenanceStore.db == nil {
		maintenanceStore.mu.RUnlock()
		return nil
	}
	if !maintenanceStore.cacheAt.IsZero() && now.Sub(maintenanceStore.cacheAt) < maintenanceCacheTTL {
		banner := maintenanceStore.cache
		maintenanceStore.mu.RUnlock()
		return banner
	}
	maintenanceStore.mu.RUnlock()

	maintenanceStore.mu.Lock()
	defer maintenanceStore.mu.Unlock()
	if maintenanceStore.db == nil {
		return nil
	}
	if !maintenanceStore.cacheAt.IsZero() && now.Sub(maintenanceStore.cacheAt) < maintenanceCacheTTL {
		return maintenanceStore.cache
	}
	window, err := maintenanceStore.db.GetActiveMaintenanceWindow(now)
	if err != nil {
		Errorf("[maintenance] error carregant finestra activa: %v", err)
		maintenanceStore.cache = nil
		maintenanceStore.cacheAt = now
		return nil
	}
	banner := buildMaintenanceBanner(window, now)
	maintenanceStore.cache = banner
	maintenanceStore.cacheAt = now
	return banner
}

func buildMaintenanceBanner(window *db.MaintenanceWindow, now time.Time) *MaintenanceBanner {
	if window == nil {
		return nil
	}
	title := strings.TrimSpace(window.Title)
	message := strings.TrimSpace(window.Message)
	if title == "" && message == "" {
		return nil
	}
	severity := strings.ToLower(strings.TrimSpace(window.Severity))
	if severity != "warning" && severity != "critical" && severity != "info" {
		severity = "info"
	}
	state := "active"
	if startsAt, err := parseMaintenanceTime(window.StartsAt); err == nil {
		if now.Before(startsAt) {
			state = "scheduled"
		}
	}
	ctaLabel := strings.TrimSpace(window.CTALabel)
	ctaURL := strings.TrimSpace(window.CTAURL)
	if ctaLabel == "" || ctaURL == "" {
		ctaLabel = ""
		ctaURL = ""
	}
	return &MaintenanceBanner{
		ID:          window.ID,
		Title:       title,
		Message:     message,
		Severity:    severity,
		CTALabel:    ctaLabel,
		CTAURL:      ctaURL,
		Dismissible: window.Dismissible,
		State:       state,
		StartsAt:    maintenanceDisplayTime(window.StartsAt),
		EndsAt:      maintenanceDisplayTime(window.EndsAt),
	}
}

func maintenanceDisplayTime(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	raw = strings.Replace(raw, "T", " ", 1)
	if len(raw) >= 16 {
		return raw[:16]
	}
	return raw
}

func maintenanceInputValue(raw string) string {
	display := maintenanceDisplayTime(raw)
	if display == "" {
		return ""
	}
	return strings.Replace(display, " ", "T", 1)
}

func maintenanceInputValueFromTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.Format("2006-01-02T15:04")
}

func normalizeMaintenanceTime(value string) (string, time.Time, error) {
	parsed, err := parseMaintenanceTime(value)
	if err != nil {
		return "", time.Time{}, err
	}
	return parsed.Format(maintenanceTimeLayout), parsed, nil
}

func parseMaintenanceTime(value string) (time.Time, error) {
	val := strings.TrimSpace(value)
	if val == "" {
		return time.Time{}, errors.New("data buida")
	}
	val = strings.Replace(val, "T", " ", 1)
	layouts := []string{
		maintenanceTimeLayout,
		"2006-01-02 15:04:05",
		time.RFC3339,
		time.RFC3339Nano,
	}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, val); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, errors.New("data invalida")
}
