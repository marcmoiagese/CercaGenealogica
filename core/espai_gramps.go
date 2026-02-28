package core

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const (
	grampsDefaultSyncIntervalMinutes = 60
	grampsDefaultBackoffMinutes      = 5
	grampsDefaultHTTPTimeoutSeconds  = 20
	grampsMaxLogsPerIntegration      = 5
)

type grampsSyncStore struct {
	mu       sync.Mutex
	locks    map[int]struct{}
	failures map[int]int
	next     map[int]time.Time
}

var grampsSync = newGrampsSyncStore()

func newGrampsSyncStore() *grampsSyncStore {
	return &grampsSyncStore{
		locks:    map[int]struct{}{},
		failures: map[int]int{},
		next:     map[int]time.Time{},
	}
}

func (s *grampsSyncStore) tryLock(id int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.locks[id]; ok {
		return false
	}
	s.locks[id] = struct{}{}
	return true
}

func (s *grampsSyncStore) unlock(id int) {
	s.mu.Lock()
	delete(s.locks, id)
	s.mu.Unlock()
}

func (s *grampsSyncStore) shouldSkip(id int, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if next, ok := s.next[id]; ok && now.Before(next) {
		return true
	}
	return false
}

func (s *grampsSyncStore) recordFailure(id int, base time.Duration) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	fail := s.failures[id] + 1
	if fail < 1 {
		fail = 1
	}
	if fail > 6 {
		fail = 6
	}
	s.failures[id] = fail
	wait := base * time.Duration(1<<uint(fail-1))
	if wait > 2*time.Hour {
		wait = 2 * time.Hour
	}
	next := time.Now().Add(wait)
	s.next[id] = next
	return next
}

func (s *grampsSyncStore) recordSuccess(id int) {
	s.mu.Lock()
	delete(s.failures, id)
	delete(s.next, id)
	s.mu.Unlock()
}

type grampsConfig struct {
	SyncInterval time.Duration
	Backoff      time.Duration
	Timeout      time.Duration
}

func (a *App) grampsConfig() grampsConfig {
	interval := parseIntDefault(a.Config["ESP_GRAMPS_SYNC_INTERVAL_MINUTES"], grampsDefaultSyncIntervalMinutes)
	if interval <= 0 {
		interval = grampsDefaultSyncIntervalMinutes
	}
	backoff := parseIntDefault(a.Config["ESP_GRAMPS_SYNC_BACKOFF_MINUTES"], grampsDefaultBackoffMinutes)
	if backoff <= 0 {
		backoff = grampsDefaultBackoffMinutes
	}
	timeout := parseIntDefault(a.Config["ESP_GRAMPS_HTTP_TIMEOUT_SECONDS"], grampsDefaultHTTPTimeoutSeconds)
	if timeout <= 0 {
		timeout = grampsDefaultHTTPTimeoutSeconds
	}
	return grampsConfig{
		SyncInterval: time.Duration(interval) * time.Minute,
		Backoff:      time.Duration(backoff) * time.Minute,
		Timeout:      time.Duration(timeout) * time.Second,
	}
}

func (a *App) StartEspaiGrampsSyncWorker() {
	cfg := a.grampsConfig()
	if cfg.SyncInterval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(cfg.SyncInterval)
		defer ticker.Stop()
		for range ticker.C {
			_ = a.syncAllGrampsIntegrations(context.Background(), false)
		}
	}()
}

func (a *App) EspaiPersonalIntegracionsPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}

	integracions, _ := a.DB.ListEspaiIntegracionsGrampsByOwner(user.ID)
	logsByID := map[int][]db.EspaiIntegracioGrampsLog{}
	for _, integ := range integracions {
		if logs, err := a.DB.ListEspaiIntegracioGrampsLogs(integ.ID, grampsMaxLogsPerIntegration); err == nil {
			logsByID[integ.ID] = logs
		}
	}
	trees, _ := a.DB.ListEspaiArbresByOwner(user.ID)
	treesByID := map[int]db.EspaiArbre{}
	for _, t := range trees {
		treesByID[t.ID] = t
	}

	spaceState := "ready"
	if len(integracions) == 0 {
		spaceState = "empty"
	}

	RenderPrivateTemplate(w, r, "espai.html", map[string]interface{}{
		"SpaceSection":       "integracions",
		"SpaceState":         spaceState,
		"GrampsIntegrations": integracions,
		"GrampsLogsByID":     logsByID,
		"GrampsTrees":        trees,
		"GrampsTreesByID":    treesByID,
		"UploadError":        strings.TrimSpace(r.URL.Query().Get("error")),
		"UploadNotice":       strings.TrimSpace(r.URL.Query().Get("notice")),
	})
}

func (a *App) EspaiGrampsConnect(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, T(ResolveLang(r), "error.csrf"), http.StatusBadRequest)
		return
	}
	baseURL := strings.TrimSpace(r.FormValue("base_url"))
	username := strings.TrimSpace(r.FormValue("username"))
	token := strings.TrimSpace(r.FormValue("token"))
	integrationID := parseFormInt(r.FormValue("integration_id"))
	if baseURL == "" {
		http.Redirect(w, r, "/espai/integracions?error="+urlQueryEscape(T(ResolveLang(r), "space.gramps.error.missing_fields")), http.StatusSeeOther)
		return
	}
	baseURL = strings.TrimRight(baseURL, "/")

	arbreID := parseFormInt(r.FormValue("arbre_id"))
	treeName := strings.TrimSpace(r.FormValue("tree_name"))
	tree, err := a.ensureEspaiArbre(user.ID, arbreID, treeName, "Gramps")
	if err != nil {
		http.Redirect(w, r, "/espai/integracions?error="+urlQueryEscape(err.Error()), http.StatusSeeOther)
		return
	}

	var integration *db.EspaiIntegracioGramps
	if integrationID > 0 {
		if existing, err := a.DB.GetEspaiIntegracioGramps(integrationID); err == nil && existing != nil && existing.OwnerUserID == user.ID {
			integration = existing
		} else {
			http.Redirect(w, r, "/espai/integracions?error="+urlQueryEscape(T(ResolveLang(r), "space.gramps.error.not_found")), http.StatusSeeOther)
			return
		}
	}
	if existing, err := a.DB.ListEspaiIntegracionsGrampsByOwner(user.ID); err == nil {
		for i := range existing {
			if existing[i].ArbreID == tree.ID {
				integration = &existing[i]
				break
			}
		}
	}
	if integration == nil && token == "" {
		http.Redirect(w, r, "/espai/integracions?error="+urlQueryEscape(T(ResolveLang(r), "space.gramps.error.missing_fields")), http.StatusSeeOther)
		return
	}

	encToken := ""
	if token != "" {
		encToken, err = a.encryptGrampsToken(token)
		if err != nil {
			http.Redirect(w, r, "/espai/integracions?error="+urlQueryEscape(err.Error()), http.StatusSeeOther)
			return
		}
	}
	tokenForPing := token
	if tokenForPing == "" && integration != nil {
		if integration.Token.Valid {
			if decrypted, err := a.decryptGrampsToken(integration.Token); err == nil {
				tokenForPing = decrypted
			}
		}
	}
	if tokenForPing == "" {
		http.Redirect(w, r, "/espai/integracions?error="+urlQueryEscape(T(ResolveLang(r), "space.gramps.error.missing_token")), http.StatusSeeOther)
		return
	}
	if integration != nil {
		integration.BaseURL = baseURL
		integration.ArbreID = tree.ID
		integration.Username = sqlNullString(username)
		if encToken != "" {
			integration.Token = sql.NullString{String: encToken, Valid: true}
		}
		integration.Status = "connected"
		integration.LastError = sql.NullString{}
		if err := a.DB.UpdateEspaiIntegracioGramps(integration); err != nil {
			http.Redirect(w, r, "/espai/integracions?error="+urlQueryEscape(err.Error()), http.StatusSeeOther)
			return
		}
	} else {
		integration = &db.EspaiIntegracioGramps{
			OwnerUserID: user.ID,
			ArbreID:     tree.ID,
			BaseURL:     baseURL,
			Username:    sqlNullString(username),
			Token:       sql.NullString{String: encToken, Valid: true},
			Status:      "connected",
		}
		if _, err := a.DB.CreateEspaiIntegracioGramps(integration); err != nil {
			http.Redirect(w, r, "/espai/integracions?error="+urlQueryEscape(err.Error()), http.StatusSeeOther)
			return
		}
	}

	client := a.newGrampsClient(baseURL, username, tokenForPing)
	if err := client.Ping(r.Context()); err != nil {
		_ = a.updateGrampsIntegrationError(integration, err.Error())
		http.Redirect(w, r, "/espai/integracions?error="+urlQueryEscape(err.Error()), http.StatusSeeOther)
		return
	}

	if _, err := a.queueGrampsImport(user.ID, tree.ID); err != nil {
		http.Redirect(w, r, "/espai/integracions?error="+urlQueryEscape(err.Error()), http.StatusSeeOther)
		return
	}

	http.Redirect(w, r, "/espai/integracions?notice="+urlQueryEscape(T(ResolveLang(r), "space.gramps.notice.queued")), http.StatusSeeOther)
}

func (a *App) EspaiGrampsSyncNow(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		http.Error(w, T(ResolveLang(r), "error.csrf"), http.StatusBadRequest)
		return
	}
	integID := parseFormInt(r.FormValue("integration_id"))
	if integID == 0 {
		http.Redirect(w, r, "/espai/integracions?error="+urlQueryEscape(T(ResolveLang(r), "space.gramps.error.not_found")), http.StatusSeeOther)
		return
	}
	integ, err := a.DB.GetEspaiIntegracioGramps(integID)
	if err != nil || integ == nil || integ.OwnerUserID != user.ID {
		http.Redirect(w, r, "/espai/integracions?error="+urlQueryEscape(T(ResolveLang(r), "space.gramps.error.not_found")), http.StatusSeeOther)
		return
	}
	if _, err := a.queueGrampsImport(user.ID, integ.ArbreID); err != nil {
		http.Redirect(w, r, "/espai/integracions?error="+urlQueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/espai/integracions?notice="+urlQueryEscape(T(ResolveLang(r), "space.gramps.notice.queued")), http.StatusSeeOther)
}

func (a *App) syncAllGrampsIntegrations(ctx context.Context, force bool) error {
	integracions, err := a.DB.ListEspaiIntegracionsGramps()
	if err != nil {
		return err
	}
	for i := range integracions {
		_ = a.syncGrampsIntegration(ctx, &integracions[i], force)
	}
	return nil
}

func (a *App) syncGrampsIntegration(ctx context.Context, integ *db.EspaiIntegracioGramps, force bool) error {
	if integ == nil {
		return errors.New("integracio no trobada")
	}
	if integ.Status == "disabled" {
		return nil
	}
	now := time.Now()
	if !force && grampsSync.shouldSkip(integ.ID, now) {
		return nil
	}
	if !grampsSync.tryLock(integ.ID) {
		return nil
	}
	defer grampsSync.unlock(integ.ID)

	token, err := a.decryptGrampsToken(integ.Token)
	if err != nil {
		return a.updateGrampsIntegrationError(integ, err.Error())
	}

	client := a.newGrampsClient(integ.BaseURL, integ.Username.String, token)
	if err := client.Ping(ctx); err != nil {
		return a.updateGrampsIntegrationError(integ, err.Error())
	}

	people, err := client.FetchPeople(ctx)
	if err != nil {
		return a.updateGrampsIntegrationError(integ, err.Error())
	}
	families, _ := client.FetchFamilies(ctx)

	existing, _ := a.DB.ListEspaiPersonesByArbre(integ.ArbreID)
	extMap := map[string]int{}
	existingByID := map[int]db.EspaiPersona{}
	for _, p := range existing {
		existingByID[p.ID] = p
		if p.ExternalID.Valid {
			extMap[strings.TrimSpace(p.ExternalID.String)] = p.ID
		}
	}

	noteCache := map[string]string{}
	noteTextFor := func(handles []string) string {
		if len(handles) == 0 {
			return ""
		}
		parts := []string{}
		for _, h := range handles {
			h = strings.TrimSpace(h)
			if h == "" {
				continue
			}
			if cached, ok := noteCache[h]; ok {
				if cached != "" {
					parts = append(parts, cached)
				}
				continue
			}
			text, err := client.FetchNote(ctx, h)
			if err != nil {
				noteCache[h] = ""
				continue
			}
			text = normalizeGrampsNote(text)
			noteCache[h] = text
			if text != "" {
				parts = append(parts, text)
			}
		}
		if len(parts) == 0 {
			return ""
		}
		return strings.TrimSpace(strings.Join(parts, " "))
	}

	personEventRefs := map[int][]grampsEventRef{}
	personDetailCache := map[string]grampsPerson{}
	personsByID := map[int]*db.EspaiPersona{}
	createdPersons := 0
	updatedPersons := 0

	mergeNull := func(dst *sql.NullString, val string) bool {
		val = strings.TrimSpace(val)
		if val == "" {
			return false
		}
		if !dst.Valid || strings.TrimSpace(dst.String) != val {
			*dst = sql.NullString{String: val, Valid: true}
			return true
		}
		return false
	}

	for _, p := range people {
		if p.Sex == "" && p.Handle != "" {
			if cached, ok := personDetailCache[p.Handle]; ok {
				if cached.Handle != "" {
					p = mergeGrampsPersonFallback(p, cached)
				}
			} else if detail, err := client.FetchPerson(ctx, p.Handle); err == nil {
				personDetailCache[p.Handle] = detail
				p = mergeGrampsPersonFallback(p, detail)
			} else {
				personDetailCache[p.Handle] = grampsPerson{}
			}
		}
		keys := []string{}
		if p.Handle != "" {
			keys = append(keys, "gramps:"+p.Handle)
		}
		if p.GrampsID != "" {
			keys = append(keys, "gramps:"+p.GrampsID)
		}
		if len(keys) == 0 {
			continue
		}
		foundID := 0
		for _, key := range keys {
			if id, ok := extMap[key]; ok {
				foundID = id
				break
			}
		}
		surnameParts := p.SurnameParts
		if len(surnameParts) == 0 && p.Surname != "" {
			surnameParts = []string{p.Surname}
		}
		cognom1 := ""
		cognom2 := ""
		if len(surnameParts) > 0 {
			cognom1 = surnameParts[0]
		}
		if len(surnameParts) > 1 {
			cognom2 = surnameParts[1]
		}
		fullSurname := strings.TrimSpace(p.SurnameFull)
		if fullSurname == "" && len(surnameParts) > 0 {
			fullSurname = strings.TrimSpace(strings.Join(surnameParts, " "))
		}
		fullName := strings.TrimSpace(strings.Join([]string{p.GivenName, fullSurname}, " "))
		notesText := noteTextFor(p.NoteHandles)

		if foundID > 0 {
			existingPersona := existingByID[foundID]
			changed := false
			if mergeNull(&existingPersona.Nom, p.GivenName) {
				changed = true
			}
			if mergeNull(&existingPersona.Cognom1, cognom1) {
				changed = true
			}
			if mergeNull(&existingPersona.Cognom2, cognom2) {
				changed = true
			}
			if mergeNull(&existingPersona.NomComplet, fullName) {
				changed = true
			}
			if mergeNull(&existingPersona.Sexe, p.Sex) {
				changed = true
			}
			if mergeNull(&existingPersona.DataNaixement, p.BirthDate) {
				changed = true
			}
			if mergeNull(&existingPersona.DataDefuncio, p.DeathDate) {
				changed = true
			}
			if mergeNull(&existingPersona.Notes, notesText) {
				changed = true
			}
			if existingPersona.HasMedia != p.HasMedia {
				existingPersona.HasMedia = p.HasMedia
				changed = true
			}
			if changed {
				_ = a.DB.UpdateEspaiPersona(&existingPersona)
				updatedPersons++
			}
			_ = a.upsertSearchDocForEspaiPersonaID(existingPersona.ID)
			personCopy := existingPersona
			personsByID[foundID] = &personCopy
			personEventRefs[foundID] = append(personEventRefs[foundID], p.EventRefs...)
			for _, key := range keys {
				extMap[key] = foundID
			}
			continue
		}

		primaryKey := keys[0]
		person := &db.EspaiPersona{
			OwnerUserID:   integ.OwnerUserID,
			ArbreID:       integ.ArbreID,
			ExternalID:    sqlNullString(primaryKey),
			Nom:           sqlNullString(p.GivenName),
			Cognom1:       sqlNullString(cognom1),
			Cognom2:       sqlNullString(cognom2),
			NomComplet:    sqlNullString(fullName),
			Sexe:          sqlNullString(p.Sex),
			DataNaixement: sqlNullString(p.BirthDate),
			DataDefuncio:  sqlNullString(p.DeathDate),
			Notes:         sqlNullString(notesText),
			HasMedia:      p.HasMedia,
			Status:        "active",
		}
		if _, err := a.DB.CreateEspaiPersona(person); err != nil {
			continue
		}
		_ = a.upsertSearchDocForEspaiPersonaID(person.ID)
		for _, key := range keys {
			extMap[key] = person.ID
		}
		personsByID[person.ID] = person
		personEventRefs[person.ID] = append(personEventRefs[person.ID], p.EventRefs...)
		createdPersons++
	}

	relationsCreated := 0
	relations, _ := a.DB.ListEspaiRelacionsByArbre(integ.ArbreID)
	relSet := map[string]struct{}{}
	for _, rel := range relations {
		key := fmt.Sprintf("%d:%d:%s", rel.PersonaID, rel.RelatedPersonaID, rel.RelationType)
		relSet[key] = struct{}{}
	}

	for _, fam := range families {
		fatherID := resolveGrampsPersonID(extMap, fam.FatherID)
		motherID := resolveGrampsPersonID(extMap, fam.MotherID)
		if fatherID > 0 && motherID > 0 {
			relationsCreated += a.createEspaiRelationIfMissing(relSet, integ.ArbreID, fatherID, motherID, "spouse")
			relationsCreated += a.createEspaiRelationIfMissing(relSet, integ.ArbreID, motherID, fatherID, "spouse")
		}
		if len(fam.EventRefs) > 0 {
			if fatherID > 0 {
				personEventRefs[fatherID] = append(personEventRefs[fatherID], fam.EventRefs...)
			}
			if motherID > 0 {
				personEventRefs[motherID] = append(personEventRefs[motherID], fam.EventRefs...)
			}
		}
		for _, child := range fam.Children {
			childID := resolveGrampsPersonID(extMap, child)
			if childID == 0 {
				continue
			}
			if fatherID > 0 {
				relationsCreated += a.createEspaiRelationIfMissing(relSet, integ.ArbreID, childID, fatherID, "father")
			}
			if motherID > 0 {
				relationsCreated += a.createEspaiRelationIfMissing(relSet, integ.ArbreID, childID, motherID, "mother")
			}
		}
	}

	_ = a.DB.DeleteEspaiEventsByArbreSource(integ.ArbreID, "gramps")
	eventCache := map[string]grampsEvent{}
	placeCache := map[string]string{}
	eventsCreated := 0
	pendingPersonaUpdates := map[int]*db.EspaiPersona{}
	eventKeySet := map[string]struct{}{}
	getEvent := func(ref string) (grampsEvent, bool) {
		ref = strings.TrimSpace(ref)
		if ref == "" {
			return grampsEvent{}, false
		}
		if cached, ok := eventCache[ref]; ok {
			return cached, true
		}
		ev, err := client.FetchEvent(ctx, ref)
		if err != nil {
			return grampsEvent{}, false
		}
		if ev.Place == "" && ev.PlaceRef != "" {
			if cachedPlace, ok := placeCache[ev.PlaceRef]; ok {
				ev.Place = cachedPlace
			} else if name, err := client.FetchPlaceName(ctx, ev.PlaceRef); err == nil {
				ev.Place = name
				placeCache[ev.PlaceRef] = name
			}
		}
		eventCache[ref] = ev
		return ev, true
	}
	for personID, refs := range personEventRefs {
		for _, ref := range refs {
			refID := strings.TrimSpace(ref.Ref)
			if refID == "" {
				continue
			}
			eventKey := fmt.Sprintf("%d:%s", personID, refID)
			if _, ok := eventKeySet[eventKey]; ok {
				continue
			}
			eventKeySet[eventKey] = struct{}{}
			ev, ok := getEvent(refID)
			if !ok {
				continue
			}
			evType := normalizeGrampsEventType(ev.Type)
			event := &db.EspaiEvent{
				ArbreID:     integ.ArbreID,
				PersonaID:   personID,
				ExternalID:  sqlNullString("gramps:" + refID),
				EventType:   evType,
				EventRole:   sqlNullString(ref.Role),
				EventDate:   sqlNullString(ev.Date),
				EventPlace:  sqlNullString(ev.Place),
				Description: sqlNullString(ev.Description),
				Source:      sqlNullString("gramps"),
			}
			if _, err := a.DB.CreateEspaiEvent(event); err == nil {
				eventsCreated++
			}
			person := personsByID[personID]
			if person == nil {
				continue
			}
			updated := false
			switch evType {
			case "naixement":
				if !person.DataNaixement.Valid && ev.Date != "" {
					person.DataNaixement = sqlNullString(ev.Date)
					updated = true
				}
				if !person.LlocNaixement.Valid && ev.Place != "" {
					person.LlocNaixement = sqlNullString(ev.Place)
					updated = true
				}
			case "defuncio", "enterrament":
				if !person.DataDefuncio.Valid && ev.Date != "" {
					person.DataDefuncio = sqlNullString(ev.Date)
					updated = true
				}
				if !person.LlocDefuncio.Valid && ev.Place != "" {
					person.LlocDefuncio = sqlNullString(ev.Place)
					updated = true
				}
			}
			if updated {
				pendingPersonaUpdates[personID] = person
			}
		}
	}
	for _, person := range pendingPersonaUpdates {
		_ = a.DB.UpdateEspaiPersona(person)
		_ = a.upsertSearchDocForEspaiPersonaID(person.ID)
	}

	integ.Status = "connected"
	integ.LastSyncAt = sql.NullTime{Time: time.Now(), Valid: true}
	integ.LastError = sql.NullString{}
	if err := a.DB.UpdateEspaiIntegracioGramps(integ); err != nil {
		return err
	}
	grampsSync.recordSuccess(integ.ID)
	summary := fmt.Sprintf("Importades %d persones (%d actualitzades), %d relacions, %d esdeveniments", createdPersons, updatedPersons, relationsCreated, eventsCreated)
	_, _ = a.DB.CreateEspaiIntegracioGrampsLog(&db.EspaiIntegracioGrampsLog{
		IntegracioID: integ.ID,
		Status:       "success",
		Message:      sqlNullString(summary),
	})
	return nil
}

func (a *App) createEspaiRelationIfMissing(relSet map[string]struct{}, arbreID, fromID, toID int, relType string) int {
	key := fmt.Sprintf("%d:%d:%s", fromID, toID, relType)
	if _, ok := relSet[key]; ok {
		return 0
	}
	if _, err := a.DB.CreateEspaiRelacio(&db.EspaiRelacio{
		ArbreID:         arbreID,
		PersonaID:       fromID,
		RelatedPersonaID: toID,
		RelationType:    relType,
	}); err == nil {
		relSet[key] = struct{}{}
		return 1
	}
	return 0
}

func (a *App) updateGrampsIntegrationError(integ *db.EspaiIntegracioGramps, message string) error {
	if integ == nil {
		return errors.New(message)
	}
	integ.Status = "error"
	integ.LastError = sqlNullString(message)
	_ = a.DB.UpdateEspaiIntegracioGramps(integ)
	grampsSync.recordFailure(integ.ID, a.grampsConfig().Backoff)
	_, _ = a.DB.CreateEspaiIntegracioGrampsLog(&db.EspaiIntegracioGrampsLog{
		IntegracioID: integ.ID,
		Status:       "error",
		Message:      sqlNullString(message),
	})
	a.notifyEspaiGrampsError(integ, message)
	return errors.New(message)
}

func (a *App) ensureEspaiArbre(ownerID, arbreID int, treeName string, prefix string) (*db.EspaiArbre, error) {
	if arbreID > 0 {
		existing, err := a.DB.GetEspaiArbre(arbreID)
		if err == nil && existing != nil && existing.OwnerUserID == ownerID {
			return existing, nil
		}
	}
	if strings.TrimSpace(treeName) == "" {
		treeName = fmt.Sprintf("%s %s", prefix, time.Now().Format("2006-01-02 15:04"))
	}
	tree := &db.EspaiArbre{
		OwnerUserID: ownerID,
		Nom:         treeName,
		Visibility:  "private",
		Status:      "active",
	}
	if _, err := a.DB.CreateEspaiArbre(tree); err != nil {
		return nil, err
	}
	return tree, nil
}

func (a *App) encryptGrampsToken(token string) (string, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return "", errors.New(T("cat", "space.gramps.error.missing_fields"))
	}
	key, err := a.grampsSecretKey()
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	ciphertext := gcm.Seal(nil, nonce, []byte(token), nil)
	blob := append(nonce, ciphertext...)
	return base64.StdEncoding.EncodeToString(blob), nil
}

func (a *App) decryptGrampsToken(val sql.NullString) (string, error) {
	if !val.Valid || strings.TrimSpace(val.String) == "" {
		return "", errors.New(T("cat", "space.gramps.error.missing_token"))
	}
	key, err := a.grampsSecretKey()
	if err != nil {
		return "", err
	}
	raw, err := base64.StdEncoding.DecodeString(val.String)
	if err != nil {
		return "", errors.New(T("cat", "space.gramps.error.decrypt"))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	if len(raw) < gcm.NonceSize() {
		return "", errors.New(T("cat", "space.gramps.error.decrypt"))
	}
	nonce := raw[:gcm.NonceSize()]
	ciphertext := raw[gcm.NonceSize():]
	plain, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", errors.New(T("cat", "space.gramps.error.decrypt"))
	}
	return string(plain), nil
}

func (a *App) grampsSecretKey() ([]byte, error) {
	raw := strings.TrimSpace(a.Config["ESP_GRAMPS_SECRET"])
	if raw == "" {
		return nil, errors.New(T("cat", "space.gramps.error.secret"))
	}
	if strings.HasPrefix(raw, "base64:") {
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(raw, "base64:"))
		if err != nil || len(decoded) < 32 {
			return nil, errors.New(T("cat", "space.gramps.error.secret"))
		}
		return decoded[:32], nil
	}
	sum := sha256.Sum256([]byte(raw))
	return sum[:], nil
}

type grampsClient struct {
	baseURL     string
	user        string
	password    string
	token       string
	accessToken string
	client      *http.Client
}

func (a *App) newGrampsClient(baseURL, user, token string) *grampsClient {
	cfg := a.grampsConfig()
	user = strings.TrimSpace(user)
	token = strings.TrimSpace(token)
	return &grampsClient{
		baseURL:  strings.TrimRight(baseURL, "/"),
		user:     user,
		password: func() string {
			if user != "" {
				return token
			}
			return ""
		}(),
		token: func() string {
			if user == "" {
				return token
			}
			return ""
		}(),
		client: &http.Client{Timeout: cfg.Timeout},
	}
}

func (c *grampsClient) Ping(ctx context.Context) error {
	endpoints := []string{"/api/health", "/api/v1/health"}
	var lastErr error
	for _, ep := range endpoints {
		if err := c.getOK(ctx, ep); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	apiEndpoints := []string{"/api/people", "/api/people/", "/api/v1/people", "/api/v1/people/"}
	for _, ep := range apiEndpoints {
		if err := c.getOK(ctx, ep); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	if lastErr != nil {
		return lastErr
	}
	return errors.New("no s'ha pogut validar la connexió amb Gramps")
}

func (c *grampsClient) getOK(ctx context.Context, endpoint string) error {
	req, err := c.newRequest(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: %w", endpoint, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return errors.New("auth invalid per Gramps")
	}
	return fmt.Errorf("Gramps resposta %d per %s", resp.StatusCode, endpoint)
}

func (c *grampsClient) FetchPeople(ctx context.Context) ([]grampsPerson, error) {
	items, err := c.fetchList(ctx, []string{
		"/api/people?pagesize=1000&extend=all",
		"/api/people/?pagesize=1000&extend=all",
		"/api/v1/people?pagesize=1000&extend=all",
		"/api/v1/people/?pagesize=1000&extend=all",
		"/api/people",
		"/api/people/",
		"/api/v1/people",
		"/api/v1/people/",
	})
	if err != nil {
		return nil, err
	}
	out := []grampsPerson{}
	for _, item := range items {
		person := parseGrampsPerson(item)
		if person.GrampsID != "" || person.Handle != "" {
			out = append(out, person)
		}
	}
	return out, nil
}

func (c *grampsClient) FetchFamilies(ctx context.Context) ([]grampsFamily, error) {
	items, err := c.fetchList(ctx, []string{
		"/api/families?pagesize=1000&extend=all",
		"/api/families/?pagesize=1000&extend=all",
		"/api/v1/families?pagesize=1000&extend=all",
		"/api/v1/families/?pagesize=1000&extend=all",
		"/api/families",
		"/api/families/",
		"/api/v1/families",
		"/api/v1/families/",
	})
	if err != nil {
		return nil, err
	}
	out := []grampsFamily{}
	for _, item := range items {
		fam := parseGrampsFamily(item)
		if fam.FatherID != "" || fam.MotherID != "" || len(fam.Children) > 0 {
			out = append(out, fam)
		}
	}
	return out, nil
}

func (c *grampsClient) FetchPerson(ctx context.Context, handle string) (grampsPerson, error) {
	handle = strings.TrimSpace(handle)
	if handle == "" {
		return grampsPerson{}, errors.New("person handle buit")
	}
	endpoints := []string{
		"/api/people/" + url.PathEscape(handle) + "?extend=all",
		"/api/people/" + url.PathEscape(handle) + "/?extend=all",
		"/api/v1/people/" + url.PathEscape(handle) + "?extend=all",
		"/api/v1/people/" + url.PathEscape(handle) + "/?extend=all",
		"/api/people/" + url.PathEscape(handle),
		"/api/people/" + url.PathEscape(handle) + "/",
		"/api/v1/people/" + url.PathEscape(handle),
		"/api/v1/people/" + url.PathEscape(handle) + "/",
	}
	item, err := c.fetchItem(ctx, endpoints)
	if err != nil {
		return grampsPerson{}, err
	}
	person := parseGrampsPerson(item)
	if person.Handle == "" {
		person.Handle = handle
	}
	return person, nil
}

func (c *grampsClient) FetchEvent(ctx context.Context, handle string) (grampsEvent, error) {
	handle = strings.TrimSpace(handle)
	if handle == "" {
		return grampsEvent{}, errors.New("event handle buit")
	}
	endpoints := []string{
		"/api/events/" + url.PathEscape(handle),
		"/api/events/" + url.PathEscape(handle) + "/",
		"/api/v1/events/" + url.PathEscape(handle),
		"/api/v1/events/" + url.PathEscape(handle) + "/",
	}
	item, err := c.fetchItem(ctx, endpoints)
	if err != nil {
		return grampsEvent{}, err
	}
	ev := parseGrampsEvent(item)
	if ev.Handle == "" {
		ev.Handle = handle
	}
	return ev, nil
}

func (c *grampsClient) FetchNote(ctx context.Context, handle string) (string, error) {
	handle = strings.TrimSpace(handle)
	if handle == "" {
		return "", errors.New("note handle buit")
	}
	endpoints := []string{
		"/api/notes/" + url.PathEscape(handle),
		"/api/notes/" + url.PathEscape(handle) + "/",
		"/api/v1/notes/" + url.PathEscape(handle),
		"/api/v1/notes/" + url.PathEscape(handle) + "/",
	}
	item, err := c.fetchItem(ctx, endpoints)
	if err != nil {
		return "", err
	}
	text := ""
	if textMap, ok := item["text"].(map[string]interface{}); ok {
		text = lookupString(textMap, "string", "value", "text")
	}
	if text == "" {
		text = lookupString(item, "text", "value")
	}
	return text, nil
}

func (c *grampsClient) FetchPlaceName(ctx context.Context, handle string) (string, error) {
	handle = strings.TrimSpace(handle)
	if handle == "" {
		return "", errors.New("place handle buit")
	}
	endpoints := []string{
		"/api/places/" + url.PathEscape(handle),
		"/api/places/" + url.PathEscape(handle) + "/",
		"/api/v1/places/" + url.PathEscape(handle),
		"/api/v1/places/" + url.PathEscape(handle) + "/",
	}
	item, err := c.fetchItem(ctx, endpoints)
	if err != nil {
		return "", err
	}
	if nameMap, ok := item["name"].(map[string]interface{}); ok {
		if name := lookupString(nameMap, "value", "name", "text"); name != "" {
			return name, nil
		}
	}
	if name := lookupString(item, "name", "value"); name != "" {
		return name, nil
	}
	return "", errors.New("place name buit")
}

func (c *grampsClient) fetchList(ctx context.Context, endpoints []string) ([]map[string]interface{}, error) {
	for _, ep := range endpoints {
		req, err := c.newRequest(ctx, http.MethodGet, ep, nil)
		if err != nil {
			continue
		}
		resp, err := c.client.Do(req)
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return nil, errors.New("auth invalid per Gramps")
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			continue
		}
		items, err := parseGrampsList(body)
		if err == nil && len(items) > 0 {
			return items, nil
		}
	}
	return nil, errors.New("no s'han pogut carregar dades de Gramps")
}

func (c *grampsClient) fetchItem(ctx context.Context, endpoints []string) (map[string]interface{}, error) {
	for _, ep := range endpoints {
		req, err := c.newRequest(ctx, http.MethodGet, ep, nil)
		if err != nil {
			continue
		}
		resp, err := c.client.Do(req)
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return nil, errors.New("auth invalid per Gramps")
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			continue
		}
		item, err := parseGrampsItem(body)
		if err == nil && item != nil {
			return item, nil
		}
	}
	return nil, errors.New("no s'han pogut carregar dades de Gramps")
}

func (c *grampsClient) newRequest(ctx context.Context, method, endpoint string, body io.Reader) (*http.Request, error) {
	base := strings.TrimRight(c.baseURL, "/")
	endpoint = normalizeGrampsEndpoint(base, endpoint)
	target := base + endpoint
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return nil, err
	}
	if c.user != "" {
		if err := c.ensureAccessToken(ctx); err != nil {
			return nil, err
		}
		if c.accessToken != "" {
			req.Header.Set("Authorization", "Bearer "+c.accessToken)
		} else if c.password != "" {
			req.SetBasicAuth(c.user, c.password)
		}
	} else if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	req.Header.Set("Accept", "application/json")
	return req, nil
}

func normalizeGrampsEndpoint(baseURL, endpoint string) string {
	baseURL = strings.TrimRight(baseURL, "/")
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "/"
	}
	if strings.HasSuffix(baseURL, "/api/v1") {
		if strings.HasPrefix(endpoint, "/api/v1") {
			endpoint = strings.TrimPrefix(endpoint, "/api/v1")
		} else if strings.HasPrefix(endpoint, "/api/") {
			endpoint = strings.TrimPrefix(endpoint, "/api")
		}
	} else if strings.HasSuffix(baseURL, "/api") {
		if strings.HasPrefix(endpoint, "/api/") {
			endpoint = strings.TrimPrefix(endpoint, "/api")
		}
	}
	if endpoint == "" {
		return "/"
	}
	if !strings.HasPrefix(endpoint, "/") {
		endpoint = "/" + endpoint
	}
	return endpoint
}

func (c *grampsClient) ensureAccessToken(ctx context.Context) error {
	if c.user == "" {
		return nil
	}
	if c.accessToken != "" {
		return nil
	}
	if c.password == "" {
		return errors.New("credencials Gramps incompletes")
	}
	payload := map[string]string{
		"username": c.user,
		"password": c.password,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	endpoints := []string{"/api/token/", "/api/token", "/api/v1/token/", "/api/v1/token"}
	var lastErr error
	for _, ep := range endpoints {
		req, err := c.newTokenRequest(ctx, ep, bytes.NewReader(body))
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := c.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return errors.New("auth invalid per Gramps")
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			lastErr = fmt.Errorf("Gramps resposta %d per %s", resp.StatusCode, ep)
			continue
		}
		if readErr != nil {
			lastErr = readErr
			continue
		}
		var tokenResp map[string]interface{}
		if err := json.Unmarshal(data, &tokenResp); err != nil {
			lastErr = err
			continue
		}
		if access, ok := tokenResp["access_token"].(string); ok && strings.TrimSpace(access) != "" {
			c.accessToken = strings.TrimSpace(access)
			return nil
		}
		lastErr = errors.New("token no trobat en resposta Gramps")
	}
	if lastErr != nil {
		return lastErr
	}
	return errors.New("no s'ha pogut validar la connexió amb Gramps")
}

func (c *grampsClient) newTokenRequest(ctx context.Context, endpoint string, body io.Reader) (*http.Request, error) {
	base := strings.TrimRight(c.baseURL, "/")
	endpoint = normalizeGrampsEndpoint(base, endpoint)
	target := base + endpoint
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

type grampsPerson struct {
	GrampsID         string
	Handle           string
	FamilyHandles    []string
	ParentFamilyRefs []string
	GivenName        string
	Surname          string
	SurnameParts     []string
	SurnameFull      string
	Sex              string
	BirthDate        string
	DeathDate        string
	NoteHandles      []string
	EventRefs        []grampsEventRef
	HasMedia         bool
}

type grampsEventRef struct {
	Ref  string
	Role string
}

type grampsEvent struct {
	Handle      string
	Type        string
	Date        string
	Place       string
	PlaceRef    string
	Description string
}

type grampsFamily struct {
	FatherID   string
	MotherID   string
	Children   []string
	EventRefs  []grampsEventRef
}

func parseGrampsList(body []byte) ([]map[string]interface{}, error) {
	var raw interface{}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	switch val := raw.(type) {
	case []interface{}:
		return mapSlice(val), nil
	case map[string]interface{}:
		for _, key := range []string{"data", "items", "results"} {
			if arr, ok := val[key].([]interface{}); ok {
				return mapSlice(arr), nil
			}
		}
	}
	return nil, errors.New("format JSON desconegut")
}

func parseGrampsItem(body []byte) (map[string]interface{}, error) {
	var raw interface{}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	switch val := raw.(type) {
	case map[string]interface{}:
		for _, key := range []string{"data", "item", "result"} {
			if m, ok := val[key].(map[string]interface{}); ok {
				return m, nil
			}
		}
		return val, nil
	case []interface{}:
		if len(val) == 0 {
			return nil, errors.New("llista buida")
		}
		if m, ok := val[0].(map[string]interface{}); ok {
			return m, nil
		}
	}
	return nil, errors.New("format JSON desconegut")
}

func mapSlice(input []interface{}) []map[string]interface{} {
	out := []map[string]interface{}{}
	for _, item := range input {
		if m, ok := item.(map[string]interface{}); ok {
			out = append(out, m)
		}
	}
	return out
}

func resolveGrampsPersonID(extMap map[string]int, ref string) int {
	ref = strings.TrimSpace(ref)
	if ref == "" || extMap == nil {
		return 0
	}
	if strings.HasPrefix(ref, "gramps:") {
		if id, ok := extMap[ref]; ok {
			return id
		}
		ref = strings.TrimPrefix(ref, "gramps:")
	}
	if id, ok := extMap["gramps:"+ref]; ok {
		return id
	}
	return 0
}

func parseGrampsPerson(m map[string]interface{}) grampsPerson {
	person := grampsPerson{}
	person.GrampsID = lookupString(m, "gramps_id", "grampsId")
	person.Handle = lookupString(m, "handle", "id", "person_id")
	if person.Handle == "" && person.GrampsID != "" {
		person.Handle = person.GrampsID
	}
	nameMap := lookupMap(m, "primary_name", "name")
	given := lookupString(m, "first_name", "given", "given_name")
	surname := lookupString(m, "surname", "last_name", "family_name")
	if nameMap != nil {
		if given == "" {
			given = lookupString(nameMap, "first", "given", "first_name")
		}
		if surname == "" {
			surname = lookupString(nameMap, "surname", "last_name", "family_name")
		}
		if full := lookupString(nameMap, "name", "full"); (given == "" || surname == "") && full != "" {
			given, surname = parseNameParts(full)
		}
	}
	if (given == "" || surname == "") && lookupString(m, "name") != "" {
		given, surname = parseNameParts(lookupString(m, "name"))
	}
	person.GivenName = given
	person.Surname = surname
	surnameParts := extractSurnameParts(nameMap, m)
	if len(surnameParts) == 0 && surname != "" {
		surnameParts = []string{surname}
	}
	person.SurnameParts = surnameParts
	if len(surnameParts) > 0 {
		person.Surname = surnameParts[0]
		person.SurnameFull = strings.TrimSpace(strings.Join(surnameParts, " "))
	} else {
		person.SurnameFull = strings.TrimSpace(surname)
	}

	sex := lookupString(m, "gender", "sex", "gender_type", "sex_type", "genderType", "sexType")
	if sex == "" {
		if raw, ok := m["gender"]; ok {
			sex = extractGrampsGenderValue(raw)
		} else if raw, ok := m["sex"]; ok {
			sex = extractGrampsGenderValue(raw)
		}
	}
	if sex == "" {
		if genderMap := lookupMap(m, "gender", "sex"); genderMap != nil {
			sex = extractGrampsGenderValue(genderMap)
		}
	}
	person.Sex = normalizeGrampsSexValue(sex)

	person.BirthDate = lookupDate(m, "birth_date", "birth", "birth_event", "birthDate")
	person.DeathDate = lookupDate(m, "death_date", "death", "death_event", "deathDate")
	person.FamilyHandles = extractReferenceIDs(m["family_list"])
	if len(person.FamilyHandles) == 0 {
		person.FamilyHandles = extractReferenceIDs(m["family_handle_list"])
	}
	person.ParentFamilyRefs = extractReferenceIDs(m["parent_family_list"])
	person.NoteHandles = extractReferenceIDs(m["note_list"])
	person.EventRefs = extractEventRefs(firstNonNil(m, "event_ref_list", "event_ref", "event_list"))
	if len(person.EventRefs) == 0 {
		person.EventRefs = []grampsEventRef{}
	}
	addEventRef := func(val interface{}, role string) {
		ref := extractReferenceID(val)
		if ref == "" {
			return
		}
		person.EventRefs = append(person.EventRefs, grampsEventRef{Ref: ref, Role: role})
	}
	addEventRef(m["birth_ref"], "birth")
	addEventRef(m["death_ref"], "death")
	addEventRef(m["baptism_ref"], "baptism")
	addEventRef(m["christening_ref"], "baptism")
	addEventRef(m["burial_ref"], "burial")
	addEventRef(m["confirmation_ref"], "confirmation")
	addEventRef(m["confirm_ref"], "confirmation")
	addEventRef(m["primary_event_ref"], "primary")
	person.HasMedia = hasGrampsMedia(m["media_list"])
	return person
}

func parseGrampsFamily(m map[string]interface{}) grampsFamily {
	fam := grampsFamily{}
	fam.FatherID = lookupString(m, "father_handle", "father_id", "father")
	if fam.FatherID == "" {
		fam.FatherID = extractReferenceID(m["father"])
	}
	fam.MotherID = lookupString(m, "mother_handle", "mother_id", "mother")
	if fam.MotherID == "" {
		fam.MotherID = extractReferenceID(m["mother"])
	}
	if childrenRaw, ok := m["child_ref_list"]; ok {
		fam.Children = append(fam.Children, extractReferenceIDs(childrenRaw)...)
	} else if childrenRaw, ok := m["children"]; ok {
		fam.Children = append(fam.Children, extractReferenceIDs(childrenRaw)...)
	} else if childrenRaw, ok := m["child_list"]; ok {
		fam.Children = append(fam.Children, extractReferenceIDs(childrenRaw)...)
	}
	fam.EventRefs = extractEventRefs(firstNonNil(m, "event_ref_list", "event_ref", "event_list"))
	if len(fam.EventRefs) == 0 {
		fam.EventRefs = []grampsEventRef{}
	}
	addFamilyEventRef := func(val interface{}, role string) {
		ref := extractReferenceID(val)
		if ref == "" {
			return
		}
		fam.EventRefs = append(fam.EventRefs, grampsEventRef{Ref: ref, Role: role})
	}
	addFamilyEventRef(m["marriage_ref"], "marriage")
	addFamilyEventRef(m["event_ref"], "")
	return fam
}

func parseGrampsEvent(m map[string]interface{}) grampsEvent {
	ev := grampsEvent{}
	ev.Handle = lookupString(m, "handle", "id", "gramps_id")
	ev.Type = lookupString(m, "type", "event_type")
	if ev.Type == "" {
		if typeMap := lookupMap(m, "type"); typeMap != nil {
			ev.Type = lookupString(typeMap, "string", "value", "name")
		}
	}
	if dateRaw, ok := m["date"]; ok {
		ev.Date = dateFromValue(dateRaw)
	}
	if ev.Date == "" {
		ev.Date = dateFromValue(firstNonNil(m, "dateval", "date_text", "date_val", "value", "text"))
	}
	ev.Description = lookupString(m, "description", "desc", "note")
	if placeRaw, ok := m["place"]; ok {
		if placeMap, ok := placeRaw.(map[string]interface{}); ok {
			if nameMap, ok := placeMap["name"].(map[string]interface{}); ok {
				ev.Place = lookupString(nameMap, "value", "name", "text")
			}
			if ev.Place == "" {
				ev.Place = lookupString(placeMap, "name", "value", "text")
			}
			ev.PlaceRef = lookupString(placeMap, "gramps_id", "id", "handle", "ref")
			if ev.PlaceRef == "" {
				ev.PlaceRef = extractReferenceID(placeMap)
			}
		} else if s, ok := placeRaw.(string); ok {
			ev.PlaceRef = strings.TrimSpace(s)
		}
	}
	return ev
}

func lookupString(m map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if val, ok := m[key]; ok {
			if s, ok := val.(string); ok {
				return strings.TrimSpace(s)
			}
			if f, ok := val.(fmt.Stringer); ok {
				return strings.TrimSpace(f.String())
			}
		}
	}
	return ""
}

func firstNonNil(m map[string]interface{}, keys ...string) interface{} {
	for _, key := range keys {
		if val, ok := m[key]; ok {
			return val
		}
	}
	return nil
}

func lookupInt(m map[string]interface{}, keys ...string) int {
	for _, key := range keys {
		if val, ok := m[key]; ok {
			switch v := val.(type) {
			case int:
				return v
			case int64:
				return int(v)
			case float64:
				return int(v)
			case json.Number:
				if i, err := v.Int64(); err == nil {
					return int(i)
				}
			case string:
				if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
					return n
				}
			}
		}
	}
	return 0
}

func lookupMap(m map[string]interface{}, keys ...string) map[string]interface{} {
	for _, key := range keys {
		if val, ok := m[key].(map[string]interface{}); ok {
			return val
		}
	}
	return nil
}

func lookupDate(m map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if val, ok := m[key]; ok {
			if s := dateFromValue(val); s != "" {
				return s
			}
		}
	}
	return ""
}

func dateFromValue(val interface{}) string {
	return normalizeGrampsDateValue(val)
}

func normalizeGrampsDateValue(val interface{}) string {
	switch v := val.(type) {
	case string:
		return normalizeGrampsDateString(v)
	case map[string]interface{}:
		if s := lookupString(v, "text", "date", "dateval", "date_val", "value", "date_text"); s != "" {
			return normalizeGrampsDateString(s)
		}
		year := lookupInt(v, "year", "y")
		month := lookupInt(v, "month", "m")
		day := lookupInt(v, "day", "d")
		if year == 0 && month == 0 && day == 0 {
			return ""
		}
		base := formatGrampsDateParts(day, month, year)
		mod := strings.ToLower(strings.TrimSpace(lookupString(v, "modifier", "mod", "qualifier", "quality")))
		return applyGrampsDateModifier(base, mod)
	default:
		return ""
	}
}

func normalizeGrampsDateString(raw string) string {
	val := strings.TrimSpace(raw)
	if val == "" {
		return ""
	}
	upper := strings.ToUpper(val)
	if rest, ok := stripGrampsDatePrefix(val, upper, []string{"ABT ", "ABOUT ", "CIRCA ", "CA ", "CAL ", "EST ", "ESTIMATED "}); ok {
		return "~" + rest
	}
	if rest, ok := stripGrampsDatePrefix(val, upper, []string{"BEF ", "BEFORE "}); ok {
		return "<" + rest
	}
	if rest, ok := stripGrampsDatePrefix(val, upper, []string{"AFT ", "AFTER "}); ok {
		return ">" + rest
	}
	return val
}

func stripGrampsDatePrefix(raw, upper string, prefixes []string) (string, bool) {
	for _, prefix := range prefixes {
		if strings.HasPrefix(upper, prefix) {
			rest := strings.TrimSpace(raw[len(prefix):])
			return rest, rest != ""
		}
	}
	return "", false
}

func formatGrampsDateParts(day, month, year int) string {
	if year <= 0 {
		return ""
	}
	dayStr := "??"
	monthStr := "??"
	if day > 0 {
		dayStr = fmt.Sprintf("%02d", day)
	}
	if month > 0 {
		monthStr = fmt.Sprintf("%02d", month)
	}
	if dayStr == "??" && monthStr == "??" {
		return strconv.Itoa(year)
	}
	return fmt.Sprintf("%s/%s/%d", dayStr, monthStr, year)
}

func applyGrampsDateModifier(base, mod string) string {
	if base == "" {
		return ""
	}
	switch mod {
	case "about", "abt", "approx", "approximate", "estimated", "est", "calc", "calculated", "circa", "c", "ca":
		if strings.HasPrefix(base, "~") {
			return base
		}
		return "~" + base
	case "before", "bef", "lt", "<":
		if strings.HasPrefix(base, "<") {
			return base
		}
		return "<" + base
	case "after", "aft", "gt", ">":
		if strings.HasPrefix(base, ">") {
			return base
		}
		return ">" + base
	default:
		return base
	}
}

func extractReferenceID(val interface{}) string {
	switch v := val.(type) {
	case string:
		return strings.TrimSpace(v)
	case map[string]interface{}:
		if s := lookupString(v, "gramps_id", "id", "handle", "ref", "person"); s != "" {
			return s
		}
	}
	return ""
}

func extractReferenceIDs(val interface{}) []string {
	out := []string{}
	switch v := val.(type) {
	case []interface{}:
		for _, entry := range v {
			if id := extractReferenceID(entry); id != "" {
				out = append(out, id)
			}
		}
	case []string:
		for _, entry := range v {
			if id := strings.TrimSpace(entry); id != "" {
				out = append(out, id)
			}
		}
	}
	return out
}

func extractGrampsGenderValue(val interface{}) string {
	switch v := val.(type) {
	case string:
		return strings.TrimSpace(v)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.Itoa(int(v))
	case map[string]interface{}:
		if s := lookupString(v, "string", "value", "name", "gender", "sex", "type", "text", "code"); s != "" {
			return s
		}
		if raw, ok := v["value"]; ok {
			if s := extractGrampsGenderValue(raw); s != "" {
				return s
			}
		}
		if raw, ok := v["code"]; ok {
			if s := extractGrampsGenderValue(raw); s != "" {
				return s
			}
		}
		if raw, ok := v["type"]; ok {
			if s := extractGrampsGenderValue(raw); s != "" {
				return s
			}
		}
	}
	return ""
}

func normalizeGrampsToken(val string) string {
	val = strings.ToLower(strings.TrimSpace(val))
	if val == "" {
		return ""
	}
	replacer := strings.NewReplacer(
		"á", "a",
		"à", "a",
		"ä", "a",
		"â", "a",
		"ã", "a",
		"å", "a",
		"é", "e",
		"è", "e",
		"ë", "e",
		"ê", "e",
		"í", "i",
		"ì", "i",
		"ï", "i",
		"î", "i",
		"ó", "o",
		"ò", "o",
		"ö", "o",
		"ô", "o",
		"õ", "o",
		"ú", "u",
		"ù", "u",
		"ü", "u",
		"û", "u",
		"ç", "c",
		"ñ", "n",
		"ý", "y",
		"ÿ", "y",
	)
	return replacer.Replace(val)
}

func normalizeGrampsSexValue(raw string) string {
	val := normalizeGrampsToken(raw)
	if val == "" {
		return ""
	}
	switch val {
	case "1", "m", "male":
		return "male"
	case "2", "f", "female":
		return "female"
	case "0", "u", "unknown", "unk", "desconegut", "desconeguda", "desconocido", "desconocida", "inconnu", "inconnue", "ignot", "ignota", "na", "n/a":
		return ""
	}
	maleHints := []string{
		"masc",
		"mascul",
		"hombre",
		"home",
		"homme",
		"uomo",
		"maschio",
		"varon",
		"varo",
		"mann",
		"mannlich",
		"erkek",
	}
	for _, hint := range maleHints {
		if strings.Contains(val, hint) {
			return "male"
		}
	}
	femaleHints := []string{
		"fem",
		"femen",
		"mujer",
		"dona",
		"donna",
		"femme",
		"frau",
		"kadin",
	}
	for _, hint := range femaleHints {
		if strings.Contains(val, hint) {
			return "female"
		}
	}
	return ""
}

func extractSurnameParts(nameMap map[string]interface{}, root map[string]interface{}) []string {
	parts := []string{}
	parseList := func(val interface{}) {
		switch list := val.(type) {
		case []interface{}:
			for _, entry := range list {
				if m, ok := entry.(map[string]interface{}); ok {
					if s := lookupString(m, "surname", "value", "name"); s != "" {
						parts = append(parts, s)
					}
				} else if s, ok := entry.(string); ok && strings.TrimSpace(s) != "" {
					parts = append(parts, strings.TrimSpace(s))
				}
			}
		case []string:
			for _, entry := range list {
				if s := strings.TrimSpace(entry); s != "" {
					parts = append(parts, s)
				}
			}
		}
	}
	if nameMap != nil {
		parseList(nameMap["surname_list"])
	}
	if len(parts) == 0 && root != nil {
		parseList(root["surname_list"])
	}
	if len(parts) == 0 && nameMap != nil {
		if s := lookupString(nameMap, "surname", "last_name", "family_name"); s != "" {
			parts = append(parts, s)
		}
	}
	return parts
}

func mergeGrampsPersonFallback(base grampsPerson, detail grampsPerson) grampsPerson {
	if base.Sex == "" {
		base.Sex = detail.Sex
	}
	if base.GivenName == "" {
		base.GivenName = detail.GivenName
	}
	if base.Surname == "" {
		base.Surname = detail.Surname
	}
	if base.SurnameFull == "" {
		base.SurnameFull = detail.SurnameFull
	}
	if len(base.SurnameParts) == 0 && len(detail.SurnameParts) > 0 {
		base.SurnameParts = detail.SurnameParts
	}
	if base.BirthDate == "" {
		base.BirthDate = detail.BirthDate
	}
	if base.DeathDate == "" {
		base.DeathDate = detail.DeathDate
	}
	if len(base.NoteHandles) == 0 && len(detail.NoteHandles) > 0 {
		base.NoteHandles = detail.NoteHandles
	}
	if len(base.EventRefs) == 0 && len(detail.EventRefs) > 0 {
		base.EventRefs = detail.EventRefs
	}
	if !base.HasMedia && detail.HasMedia {
		base.HasMedia = true
	}
	if len(base.FamilyHandles) == 0 && len(detail.FamilyHandles) > 0 {
		base.FamilyHandles = detail.FamilyHandles
	}
	if len(base.ParentFamilyRefs) == 0 && len(detail.ParentFamilyRefs) > 0 {
		base.ParentFamilyRefs = detail.ParentFamilyRefs
	}
	return base
}

func extractEventRefs(val interface{}) []grampsEventRef {
	refs := []grampsEventRef{}
	switch list := val.(type) {
	case []interface{}:
		for _, entry := range list {
			switch item := entry.(type) {
			case map[string]interface{}:
				ref := lookupString(item, "ref", "handle", "gramps_id", "id")
				if ref == "" {
					if raw, ok := item["ref"]; ok {
						ref = extractReferenceID(raw)
					}
				}
				if ref == "" {
					ref = extractReferenceID(item)
				}
				role := lookupString(item, "role")
				if ref != "" {
					refs = append(refs, grampsEventRef{Ref: ref, Role: role})
				}
			case string:
				ref := strings.TrimSpace(item)
				if ref != "" {
					refs = append(refs, grampsEventRef{Ref: ref})
				}
			}
		}
	case []string:
		for _, entry := range list {
			if ref := strings.TrimSpace(entry); ref != "" {
				refs = append(refs, grampsEventRef{Ref: ref})
			}
		}
	}
	return refs
}

func hasGrampsMedia(val interface{}) bool {
	switch list := val.(type) {
	case []interface{}:
		return len(list) > 0
	case []string:
		return len(list) > 0
	default:
		return false
	}
}

func parseNameParts(name string) (string, string) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", ""
	}
	if strings.Contains(name, "/") {
		parts := strings.Split(name, "/")
		given := strings.TrimSpace(parts[0])
		surname := ""
		if len(parts) > 1 {
			surname = strings.TrimSpace(parts[1])
		}
		return given, surname
	}
	fields := strings.Fields(name)
	if len(fields) <= 1 {
		return name, ""
	}
	return strings.Join(fields[:len(fields)-1], " "), fields[len(fields)-1]
}

func normalizeGrampsEventType(val string) string {
	val = strings.ToLower(strings.TrimSpace(val))
	val = strings.ReplaceAll(val, "_", " ")
	val = strings.ReplaceAll(val, "-", " ")
	val = strings.TrimSpace(val)
	if strings.Contains(val, "marriage") {
		return "matrimoni"
	}
	if strings.Contains(val, "birth") {
		return "naixement"
	}
	if strings.Contains(val, "bapt") {
		return "baptisme"
	}
	if strings.Contains(val, "death") {
		return "defuncio"
	}
	if strings.Contains(val, "burial") {
		return "enterrament"
	}
	if strings.Contains(val, "confirm") {
		return "confirmacio"
	}
	if strings.Contains(val, "resid") {
		return "residencia"
	}
	if strings.Contains(val, "occup") {
		return "feina"
	}
	switch val {
	case "birth", "naixement", "naixament":
		return "naixement"
	case "baptism", "baptisme", "bateig":
		return "baptisme"
	case "death", "defuncio", "obit":
		return "defuncio"
	case "burial", "enterrament", "sepultura":
		return "enterrament"
	case "marriage", "matrimoni", "wedding":
		return "matrimoni"
	case "confirmation", "confirmacio":
		return "confirmacio"
	case "residence", "residencia":
		return "residencia"
	case "occupation", "ocupacio", "feina", "treball":
		return "feina"
	default:
		return "altre"
	}
}

var grampsNoteURLRegex = regexp.MustCompile(`(?i)#\s*[^\n]*https?://\S+`)

func normalizeGrampsNote(text string) string {
	text = strings.ReplaceAll(text, "\n", " ")
	text = grampsNoteURLRegex.ReplaceAllString(text, " ")
	text = strings.Join(strings.Fields(text), " ")
	return strings.TrimSpace(text)
}
