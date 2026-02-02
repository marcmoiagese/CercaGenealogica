package core

import (
	"fmt"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

var wikiObjectTypes = map[string]struct{}{
	"municipi":       {},
	"arxiu":          {},
	"llibre":         {},
	"persona":        {},
	"cognom":         {},
	"event_historic": {},
}

func isValidWikiObjectType(objectType string) bool {
	_, ok := wikiObjectTypes[strings.ToLower(strings.TrimSpace(objectType))]
	return ok
}

func (a *App) createWikiChange(change *db.WikiChange) (int, error) {
	if change == nil {
		return 0, fmt.Errorf("canvi buit")
	}
	if !isValidWikiObjectType(change.ObjectType) {
		return 0, fmt.Errorf("tipus wiki invàlid")
	}
	if change.ObjectID <= 0 {
		return 0, fmt.Errorf("object_id invàlid")
	}
	if err := a.applyWikiChangeGuardrails(change); err != nil {
		return 0, err
	}
	start := time.Now()
	id, err := a.DB.CreateWikiChange(change)
	if err != nil {
		userID := 0
		if change.ChangedBy.Valid {
			userID = int(change.ChangedBy.Int64)
		}
		Errorf("WikiChangeCreate failed object=%s object_id=%d user_id=%d err=%v", change.ObjectType, change.ObjectID, userID, err)
		return 0, err
	}
	elapsed := time.Since(start)
	if elapsed > time.Second {
		Debugf("WikiChangeCreate slow object=%s object_id=%d dur=%s", change.ObjectType, change.ObjectID, elapsed)
	}
	return id, nil
}

func (a *App) updateWikiPublicCounts(objectType string, objectID int, prev *db.WikiMark, newType string, newPublic bool) error {
	if a == nil || a.DB == nil {
		return fmt.Errorf("app invàlida")
	}
	deltas := []struct {
		tipus string
		delta int
	}{}
	if prev != nil && prev.IsPublic {
		if !newPublic {
			deltas = append(deltas, struct {
				tipus string
				delta int
			}{tipus: prev.Tipus, delta: -1})
		} else if strings.TrimSpace(prev.Tipus) != strings.TrimSpace(newType) {
			deltas = append(deltas, struct {
				tipus string
				delta int
			}{tipus: prev.Tipus, delta: -1})
			deltas = append(deltas, struct {
				tipus string
				delta int
			}{tipus: newType, delta: 1})
		}
	} else if newPublic {
		deltas = append(deltas, struct {
			tipus string
			delta int
		}{tipus: newType, delta: 1})
	}
	for _, d := range deltas {
		if strings.TrimSpace(d.tipus) == "" || d.delta == 0 {
			continue
		}
		if err := a.DB.IncWikiPublicCount(objectType, objectID, d.tipus, d.delta); err != nil {
			return err
		}
	}
	return nil
}
