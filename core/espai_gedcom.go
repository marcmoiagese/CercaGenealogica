package core

import (
	"bufio"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const (
	gedcomDefaultRoot        = "./data/espai/gedcom"
	gedcomDefaultMaxUploadMB = 50
)

type gedcomConfig struct {
	Root           string
	MaxUploadBytes int64
}

type gedcomImportSummary struct {
	Persons       int      `json:"persons"`
	Families      int      `json:"families"`
	Relations     int      `json:"relations"`
	Warnings      []string `json:"warnings,omitempty"`
	Errors        []string `json:"errors,omitempty"`
	WarningsTotal int      `json:"warnings_total,omitempty"`
	ErrorsTotal   int      `json:"errors_total,omitempty"`
}

type gedcomPerson struct {
	ID         string
	GivenName  string
	Surname    string
	Sex        string
	BirthDate  string
	DeathDate  string
	FullName   string
}

type gedcomFamily struct {
	ID       string
	Husband  string
	Wife     string
	Children []string
}

type gedcomParseResult struct {
	Persons  []gedcomPerson
	Families []gedcomFamily
	Warnings []string
	Errors   []string
}

func (a *App) gedcomConfig() gedcomConfig {
	cfg := gedcomConfig{}
	root := strings.TrimSpace(a.Config["GEDCOM_ROOT"])
	if root == "" {
		root = gedcomDefaultRoot
	}
	maxMB := parseIntDefault(a.Config["GEDCOM_MAX_UPLOAD_MB"], gedcomDefaultMaxUploadMB)
	if maxMB <= 0 {
		maxMB = gedcomDefaultMaxUploadMB
	}
	cfg.Root = root
	cfg.MaxUploadBytes = int64(maxMB) * 1024 * 1024
	return cfg
}

func (a *App) EspaiPersonalGedcomPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}

	imports, _ := a.DB.ListEspaiImportsByOwner(user.ID)
	fonts, _ := a.DB.ListEspaiFontsImportacioByOwner(user.ID)
	trees, _ := a.DB.ListEspaiArbresByOwner(user.ID)

	fontsByID := map[int]db.EspaiFontImportacio{}
	for _, f := range fonts {
		fontsByID[f.ID] = f
	}
	treesByID := map[int]db.EspaiArbre{}
	for _, t := range trees {
		treesByID[t.ID] = t
	}

	summaries := map[int]gedcomImportSummary{}
	for _, imp := range imports {
		if imp.SummaryJSON.Valid {
			var sum gedcomImportSummary
			if err := json.Unmarshal([]byte(imp.SummaryJSON.String), &sum); err == nil {
				summaries[imp.ID] = sum
			}
		}
	}

	var selectedImport *db.EspaiImport
	var selectedSummary *gedcomImportSummary
	if importID := parseFormInt(r.URL.Query().Get("import_id")); importID > 0 {
		for _, imp := range imports {
			if imp.ID == importID {
				tmp := imp
				selectedImport = &tmp
				if sum, ok := summaries[imp.ID]; ok {
					selectedSummary = &sum
				}
				break
			}
		}
	}

	spaceState := "ready"
	if len(imports) == 0 {
		spaceState = "empty"
	}

	cfg := a.gedcomConfig()
	RenderPrivateTemplate(w, r, "espai.html", map[string]interface{}{
		"SpaceSection":    "gedcom",
		"SpaceState":      spaceState,
		"GEDCOMImports":   imports,
		"GEDCOMFontsByID": fontsByID,
		"GEDCOMTrees":     trees,
		"GEDCOMTreesByID": treesByID,
		"GEDCOMSummaries": summaries,
		"SelectedImport":  selectedImport,
		"SelectedSummary": selectedSummary,
		"GEDCOMMaxMB":     cfg.MaxUploadBytes / (1024 * 1024),
		"UploadError":     strings.TrimSpace(r.URL.Query().Get("error")),
		"UploadNotice":    strings.TrimSpace(r.URL.Query().Get("notice")),
	})
}

func (a *App) EspaiGedcomUpload(w http.ResponseWriter, r *http.Request) {
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
	importRec, notice, err := a.handleGedcomUpload(w, r, user.ID)
	if err != nil {
		target := "/espai/gedcom?error=" + urlQueryEscape(err.Error())
		http.Redirect(w, r, target, http.StatusSeeOther)
		return
	}
	if notice != "" {
		target := "/espai/gedcom?notice=" + urlQueryEscape(notice)
		http.Redirect(w, r, target, http.StatusSeeOther)
		return
	}
	target := "/espai/gedcom"
	if importRec != nil {
		target = fmt.Sprintf("/espai/gedcom?import_id=%d", importRec.ID)
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}

func (a *App) EspaiGedcomReimport(w http.ResponseWriter, r *http.Request) {
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
	importID := parseFormInt(r.FormValue("import_id"))
	if importID == 0 {
		http.Redirect(w, r, "/espai/gedcom?error="+urlQueryEscape(T(ResolveLang(r), "space.gedcom.error.missing_import")), http.StatusSeeOther)
		return
	}
	importRec, err := a.reimportGedcomFont(user.ID, importID, ResolveLang(r))
	if err != nil {
		http.Redirect(w, r, "/espai/gedcom?error="+urlQueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/espai/gedcom?import_id=%d", importRec.ID), http.StatusSeeOther)
}

func (a *App) EspaiGedcomImportsAPI(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		imports, err := a.DB.ListEspaiImportsByOwner(user.ID)
		if err != nil {
			http.Error(w, "No s'han pogut carregar els imports", http.StatusInternalServerError)
			return
		}
		payload := map[string]interface{}{
			"ok":    true,
			"items": imports,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(payload)
	case http.MethodPost:
		if !validateCSRF(r, r.FormValue("csrf_token")) {
			http.Error(w, T(ResolveLang(r), "error.csrf"), http.StatusBadRequest)
			return
		}
		importRec, notice, err := a.handleGedcomUpload(w, r, user.ID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		payload := map[string]interface{}{
			"ok":     true,
			"notice": notice,
		}
		if importRec != nil {
			payload["id"] = importRec.ID
			payload["status"] = importRec.Status
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(payload)
	default:
		http.NotFound(w, r)
	}
}

func (a *App) EspaiGedcomImportDetailAPI(w http.ResponseWriter, r *http.Request) {
	user := userFromContext(r)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/espai/gedcom/imports/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	importID, err := strconv.Atoi(parts[0])
	if err != nil || importID <= 0 {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 2 && parts[1] == "reimport" {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		if !validateCSRF(r, r.FormValue("csrf_token")) {
			http.Error(w, T(ResolveLang(r), "error.csrf"), http.StatusBadRequest)
			return
		}
		importRec, err := a.reimportGedcomFont(user.ID, importID, ResolveLang(r))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		payload := map[string]interface{}{
			"ok":     true,
			"id":     importRec.ID,
			"status": importRec.Status,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(payload)
		return
	}
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	importRec, err := a.DB.GetEspaiImport(importID)
	if err != nil || importRec == nil || importRec.OwnerUserID != user.ID {
		http.NotFound(w, r)
		return
	}
	payload := map[string]interface{}{
		"ok":   true,
		"item": importRec,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

func (a *App) handleGedcomUpload(w http.ResponseWriter, r *http.Request, ownerID int) (*db.EspaiImport, string, error) {
	cfg := a.gedcomConfig()
	r.Body = http.MaxBytesReader(w, r.Body, cfg.MaxUploadBytes)
	if err := r.ParseMultipartForm(cfg.MaxUploadBytes); err != nil {
		return nil, "", errors.New(T(ResolveLang(r), "space.gedcom.error.too_large"))
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		return nil, "", errors.New(T(ResolveLang(r), "space.gedcom.error.missing_file"))
	}
	defer file.Close()

	ext := strings.ToLower(filepath.Ext(header.Filename))
	if ext != ".ged" && ext != ".gedcom" {
		return nil, "", errors.New(T(ResolveLang(r), "space.gedcom.error.invalid_ext"))
	}

	treeID := parseFormInt(r.FormValue("arbre_id"))
	treeName := strings.TrimSpace(r.FormValue("tree_name"))
	var tree *db.EspaiArbre
	if treeID > 0 {
		existing, err := a.DB.GetEspaiArbre(treeID)
		if err == nil && existing != nil && existing.OwnerUserID == ownerID {
			tree = existing
		}
	}
	if tree == nil {
		if treeName == "" {
			treeName = fmt.Sprintf("GEDCOM %s", time.Now().Format("2006-01-02 15:04"))
		}
		newTree := &db.EspaiArbre{
			OwnerUserID: ownerID,
			Nom:         treeName,
			Visibility:  "private",
			Status:      "active",
		}
		if _, err := a.DB.CreateEspaiArbre(newTree); err != nil {
			return nil, "", err
		}
		tree = newTree
	}

	safeName := sanitizeFilename(header.Filename)
	if safeName == "" {
		safeName = "gedcom.ged"
	}
	userDir := filepath.Join(cfg.Root, fmt.Sprintf("%d", ownerID))
	if err := os.MkdirAll(userDir, 0o755); err != nil {
		return nil, "", err
	}
	targetPath := filepath.Join(userDir, fmt.Sprintf("%d_%s", time.Now().UnixNano(), safeName))
	out, err := os.Create(targetPath)
	if err != nil {
		return nil, "", err
	}
	defer out.Close()

	hasher := sha256.New()
	writer := io.MultiWriter(out, hasher)
	size, err := io.Copy(writer, file)
	if err != nil {
		_ = os.Remove(targetPath)
		return nil, "", err
	}
	checksum := hex.EncodeToString(hasher.Sum(nil))

	if existing, err := a.DB.GetEspaiFontImportacioByChecksum(ownerID, checksum); err == nil && existing != nil {
		_ = os.Remove(targetPath)
		if imp, err := a.DB.GetLatestEspaiImportByFont(ownerID, existing.ID); err == nil && imp != nil {
			return imp, T(ResolveLang(r), "space.gedcom.notice.duplicate"), nil
		}
		return nil, T(ResolveLang(r), "space.gedcom.notice.duplicate"), nil
	}

	font := &db.EspaiFontImportacio{
		OwnerUserID:      ownerID,
		SourceType:       "gedcom",
		OriginalFilename: sqlNullString(header.Filename),
		StoragePath:      sqlNullString(targetPath),
		ChecksumSHA256:   sqlNullString(checksum),
		SizeBytes:        sql.NullInt64{Int64: size, Valid: size > 0},
	}
	if _, err := a.DB.CreateEspaiFontImportacio(font); err != nil {
		_ = os.Remove(targetPath)
		return nil, "", err
	}

	importRec := &db.EspaiImport{
		OwnerUserID: ownerID,
		ArbreID:     tree.ID,
		FontID:      sql.NullInt64{Int64: int64(font.ID), Valid: true},
		ImportType:  "gedcom",
		Status:      "queued",
	}
	if _, err := a.DB.CreateEspaiImport(importRec); err != nil {
		return nil, "", err
	}

	if err := a.processGedcomImport(importRec, targetPath); err != nil {
		_ = a.DB.UpdateEspaiImportStatus(importRec.ID, "error", err.Error(), "")
		return importRec, "", err
	}

	return importRec, "", nil
}

func (a *App) reimportGedcomFont(ownerID, importID int, lang string) (*db.EspaiImport, error) {
	imp, err := a.DB.GetEspaiImport(importID)
	if err != nil || imp == nil || imp.OwnerUserID != ownerID {
		return nil, errors.New(T(lang, "space.gedcom.error.not_found"))
	}
	if !imp.FontID.Valid {
		return nil, errors.New(T(lang, "space.gedcom.error.missing_font"))
	}
	font, err := a.DB.GetEspaiFontImportacio(int(imp.FontID.Int64))
	if err != nil || font == nil {
		return nil, errors.New(T(lang, "space.gedcom.error.missing_font"))
	}
	if !font.StoragePath.Valid {
		return nil, errors.New(T(lang, "space.gedcom.error.missing_file"))
	}
	importRec := &db.EspaiImport{
		OwnerUserID: ownerID,
		ArbreID:     imp.ArbreID,
		FontID:      imp.FontID,
		ImportType:  "gedcom",
		Status:      "queued",
	}
	if _, err := a.DB.CreateEspaiImport(importRec); err != nil {
		return nil, err
	}
	if err := a.processGedcomImport(importRec, font.StoragePath.String); err != nil {
		_ = a.DB.UpdateEspaiImportStatus(importRec.ID, "error", err.Error(), "")
		return importRec, err
	}
	return importRec, nil
}

func (a *App) processGedcomImport(importRec *db.EspaiImport, path string) error {
	if importRec == nil {
		return fmt.Errorf("import record missing")
	}
	_ = a.DB.UpdateEspaiImportStatus(importRec.ID, "parsing", "", "")
	parseResult, err := parseGEDCOMFile(path)
	if err != nil {
		return err
	}
	_ = a.DB.UpdateEspaiImportStatus(importRec.ID, "normalizing", "", "")

	personIDs := map[string]int{}
	relationsCount := 0
	warnings := append([]string{}, parseResult.Warnings...)

	for _, p := range parseResult.Persons {
		person := &db.EspaiPersona{
			OwnerUserID:   importRec.OwnerUserID,
			ArbreID:       importRec.ArbreID,
			ExternalID:    sqlNullString(p.ID),
			Nom:           sqlNullString(p.GivenName),
			Cognom1:       sqlNullString(p.Surname),
			NomComplet:    sqlNullString(p.FullName),
			Sexe:          sqlNullString(p.Sex),
			DataNaixement: sqlNullString(p.BirthDate),
			DataDefuncio:  sqlNullString(p.DeathDate),
			Status:        "active",
		}
		if _, err := a.DB.CreateEspaiPersona(person); err != nil {
			warnings = appendWarning(warnings, fmt.Sprintf("No s'ha pogut crear persona %s", p.ID))
			continue
		}
		personIDs[p.ID] = person.ID
	}

	_ = a.DB.UpdateEspaiImportStatus(importRec.ID, "persisted", "", "")

	for _, fam := range parseResult.Families {
		husbID := personIDs[fam.Husband]
		wifeID := personIDs[fam.Wife]
		if husbID > 0 && wifeID > 0 {
			if _, err := a.DB.CreateEspaiRelacio(&db.EspaiRelacio{
				ArbreID:      importRec.ArbreID,
				PersonaID:    husbID,
				RelatedPersonaID: wifeID,
				RelationType: "spouse",
			}); err == nil {
				relationsCount++
			}
			if _, err := a.DB.CreateEspaiRelacio(&db.EspaiRelacio{
				ArbreID:      importRec.ArbreID,
				PersonaID:    wifeID,
				RelatedPersonaID: husbID,
				RelationType: "spouse",
			}); err == nil {
				relationsCount++
			}
		}
		for _, child := range fam.Children {
			childID := personIDs[child]
			if childID == 0 {
				continue
			}
			if husbID > 0 {
				if _, err := a.DB.CreateEspaiRelacio(&db.EspaiRelacio{
					ArbreID:      importRec.ArbreID,
					PersonaID:    childID,
					RelatedPersonaID: husbID,
					RelationType: "father",
				}); err == nil {
					relationsCount++
				}
			}
			if wifeID > 0 {
				if _, err := a.DB.CreateEspaiRelacio(&db.EspaiRelacio{
					ArbreID:      importRec.ArbreID,
					PersonaID:    childID,
					RelatedPersonaID: wifeID,
					RelationType: "mother",
				}); err == nil {
					relationsCount++
				}
			}
		}
	}

	summary := gedcomImportSummary{
		Persons:       len(parseResult.Persons),
		Families:      len(parseResult.Families),
		Relations:     relationsCount,
		Warnings:      warnings,
		WarningsTotal: len(warnings),
		Errors:        parseResult.Errors,
		ErrorsTotal:   len(parseResult.Errors),
	}
	summaryJSON := ""
	if b, err := json.Marshal(summary); err == nil {
		summaryJSON = string(b)
	}
	_ = a.DB.UpdateEspaiImportProgress(importRec.ID, summary.Persons+summary.Relations, summary.Persons+summary.Relations)
	if err := a.DB.UpdateEspaiImportStatus(importRec.ID, "done", "", summaryJSON); err != nil {
		return err
	}
	if _, err := a.rebuildEspaiCoincidenciesForArbre(importRec.OwnerUserID, importRec.ArbreID); err != nil {
		Errorf("Espai coincidencies rebuild arbre %d: %v", importRec.ArbreID, err)
	}
	return nil
}

func parseGEDCOMFile(path string) (*gedcomParseResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024), 1024*1024)

	result := &gedcomParseResult{}
	var currentPerson *gedcomPerson
	var currentFamily *gedcomFamily
	currentEvent := ""
	lineNum := 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		lineNum++
		if line == "" {
			continue
		}
		if lineNum == 1 && !strings.HasPrefix(line, "0 HEAD") {
			result.Errors = append(result.Errors, "Missing GEDCOM header")
			return result, fmt.Errorf("invalid GEDCOM header")
		}
		if strings.HasPrefix(line, "0 ") {
			if currentPerson != nil {
				result.Persons = append(result.Persons, *currentPerson)
				currentPerson = nil
			}
			if currentFamily != nil {
				result.Families = append(result.Families, *currentFamily)
				currentFamily = nil
			}
			currentEvent = ""
			fields := strings.Fields(line)
			if len(fields) >= 3 && fields[2] == "INDI" {
				currentPerson = &gedcomPerson{ID: trimGedcomID(fields[1])}
				continue
			}
			if len(fields) >= 3 && fields[2] == "FAM" {
				currentFamily = &gedcomFamily{ID: trimGedcomID(fields[1])}
				continue
			}
			continue
		}
		if currentPerson != nil {
			if strings.HasPrefix(line, "1 NAME") {
				name := strings.TrimSpace(strings.TrimPrefix(line, "1 NAME"))
				given, surname := parseGedcomName(name)
				currentPerson.GivenName = given
				currentPerson.Surname = surname
				currentPerson.FullName = strings.TrimSpace(strings.Join([]string{given, surname}, " "))
				continue
			}
			if strings.HasPrefix(line, "1 SEX") {
				sex := strings.TrimSpace(strings.TrimPrefix(line, "1 SEX"))
				switch strings.ToUpper(sex) {
				case "M":
					currentPerson.Sex = "male"
				case "F":
					currentPerson.Sex = "female"
				default:
					currentPerson.Sex = "unknown"
				}
				continue
			}
			if strings.HasPrefix(line, "1 BIRT") {
				currentEvent = "BIRT"
				continue
			}
			if strings.HasPrefix(line, "1 DEAT") {
				currentEvent = "DEAT"
				continue
			}
			if strings.HasPrefix(line, "1 ") {
				currentEvent = ""
				continue
			}
			if strings.HasPrefix(line, "2 DATE") && currentEvent != "" {
				dateVal := strings.TrimSpace(strings.TrimPrefix(line, "2 DATE"))
				if currentEvent == "BIRT" {
					currentPerson.BirthDate = dateVal
				} else if currentEvent == "DEAT" {
					currentPerson.DeathDate = dateVal
				}
			}
			continue
		}
		if currentFamily != nil {
			if strings.HasPrefix(line, "1 HUSB") {
				currentFamily.Husband = trimGedcomID(strings.TrimSpace(strings.TrimPrefix(line, "1 HUSB")))
				continue
			}
			if strings.HasPrefix(line, "1 WIFE") {
				currentFamily.Wife = trimGedcomID(strings.TrimSpace(strings.TrimPrefix(line, "1 WIFE")))
				continue
			}
			if strings.HasPrefix(line, "1 CHIL") {
				child := trimGedcomID(strings.TrimSpace(strings.TrimPrefix(line, "1 CHIL")))
				if child != "" {
					currentFamily.Children = append(currentFamily.Children, child)
				}
				continue
			}
		}
	}
	if currentPerson != nil {
		result.Persons = append(result.Persons, *currentPerson)
	}
	if currentFamily != nil {
		result.Families = append(result.Families, *currentFamily)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func parseGedcomName(name string) (string, string) {
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
	return name, ""
}

func trimGedcomID(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "@")
	raw = strings.TrimSuffix(raw, "@")
	return raw
}

func appendWarning(warnings []string, msg string) []string {
	if msg == "" {
		return warnings
	}
	if len(warnings) >= 20 {
		return warnings
	}
	return append(warnings, msg)
}

func urlQueryEscape(val string) string {
	return strings.ReplaceAll(url.QueryEscape(val), "+", "%20")
}
