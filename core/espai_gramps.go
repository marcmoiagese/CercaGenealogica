package core

import (
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
	if baseURL == "" || token == "" {
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

	encToken, err := a.encryptGrampsToken(token)
	if err != nil {
		http.Redirect(w, r, "/espai/integracions?error="+urlQueryEscape(err.Error()), http.StatusSeeOther)
		return
	}

	client := a.newGrampsClient(baseURL, username, token)
	if err := client.Ping(r.Context()); err != nil {
		http.Redirect(w, r, "/espai/integracions?error="+urlQueryEscape(err.Error()), http.StatusSeeOther)
		return
	}

	integration := &db.EspaiIntegracioGramps{
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

	if err := a.syncGrampsIntegration(r.Context(), integration, true); err != nil {
		http.Redirect(w, r, "/espai/integracions?error="+urlQueryEscape(err.Error()), http.StatusSeeOther)
		return
	}

	http.Redirect(w, r, "/espai/integracions?notice="+urlQueryEscape(T(ResolveLang(r), "space.gramps.notice.connected")), http.StatusSeeOther)
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
	if err := a.syncGrampsIntegration(r.Context(), integ, true); err != nil {
		http.Redirect(w, r, "/espai/integracions?error="+urlQueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/espai/integracions?notice="+urlQueryEscape(T(ResolveLang(r), "space.gramps.notice.synced")), http.StatusSeeOther)
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
	for _, p := range existing {
		if p.ExternalID.Valid {
			extMap[strings.TrimSpace(p.ExternalID.String)] = p.ID
		}
	}

	createdPersons := 0
	for _, p := range people {
		if p.ExternalID == "" {
			continue
		}
		extID := "gramps:" + p.ExternalID
		if _, ok := extMap[extID]; ok {
			continue
		}
		person := &db.EspaiPersona{
			OwnerUserID:   integ.OwnerUserID,
			ArbreID:       integ.ArbreID,
			ExternalID:    sqlNullString(extID),
			Nom:           sqlNullString(p.GivenName),
			Cognom1:       sqlNullString(p.Surname),
			NomComplet:    sqlNullString(strings.TrimSpace(strings.Join([]string{p.GivenName, p.Surname}, " "))),
			Sexe:          sqlNullString(p.Sex),
			DataNaixement: sqlNullString(p.BirthDate),
			DataDefuncio:  sqlNullString(p.DeathDate),
			Status:        "active",
		}
		if _, err := a.DB.CreateEspaiPersona(person); err != nil {
			continue
		}
		extMap[extID] = person.ID
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
		fatherID := extMap["gramps:"+fam.FatherID]
		motherID := extMap["gramps:"+fam.MotherID]
		if fatherID > 0 && motherID > 0 {
			relationsCreated += a.createEspaiRelationIfMissing(relSet, integ.ArbreID, fatherID, motherID, "spouse")
			relationsCreated += a.createEspaiRelationIfMissing(relSet, integ.ArbreID, motherID, fatherID, "spouse")
		}
		for _, child := range fam.Children {
			childID := extMap["gramps:"+child]
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

	integ.Status = "connected"
	integ.LastSyncAt = sql.NullTime{Time: time.Now(), Valid: true}
	integ.LastError = sql.NullString{}
	if err := a.DB.UpdateEspaiIntegracioGramps(integ); err != nil {
		return err
	}
	grampsSync.recordSuccess(integ.ID)
	summary := fmt.Sprintf("Importades %d persones, %d relacions", createdPersons, relationsCreated)
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
	baseURL string
	user    string
	token   string
	client  *http.Client
}

func (a *App) newGrampsClient(baseURL, user, token string) *grampsClient {
	cfg := a.grampsConfig()
	return &grampsClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		user:    strings.TrimSpace(user),
		token:   token,
		client:  &http.Client{Timeout: cfg.Timeout},
	}
}

func (c *grampsClient) Ping(ctx context.Context) error {
	endpoints := []string{"/api/health", "/api/v1/health"}
	for _, ep := range endpoints {
		if err := c.getOK(ctx, ep); err == nil {
			return nil
		}
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
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return errors.New("auth invalid per Gramps")
	}
	return fmt.Errorf("Gramps resposta %d", resp.StatusCode)
}

func (c *grampsClient) FetchPeople(ctx context.Context) ([]grampsPerson, error) {
	items, err := c.fetchList(ctx, []string{"/api/people", "/api/people/", "/api/v1/people", "/api/v1/people/"})
	if err != nil {
		return nil, err
	}
	out := []grampsPerson{}
	for _, item := range items {
		person := parseGrampsPerson(item)
		if person.ExternalID != "" {
			out = append(out, person)
		}
	}
	return out, nil
}

func (c *grampsClient) FetchFamilies(ctx context.Context) ([]grampsFamily, error) {
	items, err := c.fetchList(ctx, []string{"/api/families", "/api/families/", "/api/v1/families", "/api/v1/families/"})
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

func (c *grampsClient) newRequest(ctx context.Context, method, endpoint string, body io.Reader) (*http.Request, error) {
	target := strings.TrimRight(c.baseURL, "/") + endpoint
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return nil, err
	}
	if c.user != "" {
		req.SetBasicAuth(c.user, c.token)
	} else if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	req.Header.Set("Accept", "application/json")
	return req, nil
}

type grampsPerson struct {
	ExternalID string
	GivenName  string
	Surname    string
	Sex        string
	BirthDate  string
	DeathDate  string
}

type grampsFamily struct {
	FatherID string
	MotherID string
	Children []string
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

func mapSlice(input []interface{}) []map[string]interface{} {
	out := []map[string]interface{}{}
	for _, item := range input {
		if m, ok := item.(map[string]interface{}); ok {
			out = append(out, m)
		}
	}
	return out
}

func parseGrampsPerson(m map[string]interface{}) grampsPerson {
	person := grampsPerson{}
	person.ExternalID = lookupString(m, "gramps_id", "id", "handle", "person_id")
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

	sex := lookupString(m, "gender", "sex")
	switch strings.ToUpper(strings.TrimSpace(sex)) {
	case "M", "MALE":
		person.Sex = "male"
	case "F", "FEMALE":
		person.Sex = "female"
	default:
		person.Sex = ""
	}

	person.BirthDate = lookupDate(m, "birth_date", "birth", "birth_event", "birthDate")
	person.DeathDate = lookupDate(m, "death_date", "death", "death_event", "deathDate")
	return person
}

func parseGrampsFamily(m map[string]interface{}) grampsFamily {
	fam := grampsFamily{}
	fam.FatherID = extractReferenceID(m["father"])
	if fam.FatherID == "" {
		fam.FatherID = lookupString(m, "father_id")
	}
	fam.MotherID = extractReferenceID(m["mother"])
	if fam.MotherID == "" {
		fam.MotherID = lookupString(m, "mother_id")
	}
	if childrenRaw, ok := m["children"]; ok {
		fam.Children = append(fam.Children, extractReferenceIDs(childrenRaw)...)
	}
	return fam
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
	switch v := val.(type) {
	case string:
		return strings.TrimSpace(v)
	case map[string]interface{}:
		for _, key := range []string{"date", "date_val", "value", "text"} {
			if s, ok := v[key].(string); ok && strings.TrimSpace(s) != "" {
				return strings.TrimSpace(s)
			}
		}
	}
	return ""
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
