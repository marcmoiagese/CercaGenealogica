package core

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

const (
	confessionalImportExportSchema   = "cercagenealogica.confessional.v1"
	confessionalImportExportMaxBytes = 2 << 20
)

type confessionalExportPayload struct {
	Schema     string                   `json:"schema"`
	ExportedAt string                   `json:"exported_at"`
	Source     confessionalExportSource `json:"source"`
	Items      confessionalExportItems  `json:"items"`
}

type confessionalExportSource struct {
	App    string `json:"app"`
	Module string `json:"module"`
}

type confessionalExportItems struct {
	EntitatsReligioses    []confessionalExportEntityRecord        `json:"entitats_religioses"`
	RelacionsEntitats     []confessionalExportHierarchyRelation   `json:"relacions_entitats"`
	RelacionsTerritorials []confessionalExportTerritorialRelation `json:"relacions_territorials"`
	RelacionsArxius       []confessionalExportArchiveRelation     `json:"relacions_arxius"`
}

type confessionalEntityRef struct {
	Code         string `json:"code"`
	ReligionCode string `json:"religion_code"`
	LevelCode    string `json:"level_code"`
	Name         string `json:"name,omitempty"`
}

type confessionalMunicipalityRef struct {
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	CountryISO2 string   `json:"country_iso2,omitempty"`
	ParentNames []string `json:"parent_names,omitempty"`
}

type confessionalArchiveRef struct {
	Name         string                       `json:"name"`
	Type         string                       `json:"type,omitempty"`
	Municipality *confessionalMunicipalityRef `json:"municipality,omitempty"`
}

type confessionalExportEntityRecord struct {
	Ref              confessionalEntityRef `json:"ref"`
	Name             string                `json:"name"`
	ReligionCode     string                `json:"religion_code"`
	LevelCode        string                `json:"level_code"`
	Type             string                `json:"type,omitempty"`
	SpecificType     string                `json:"specific_type,omitempty"`
	StartsYear       *int                  `json:"starts_year,omitempty"`
	EndsYear         *int                  `json:"ends_year,omitempty"`
	State            string                `json:"state,omitempty"`
	Web              string                `json:"web,omitempty"`
	Wikipedia        string                `json:"wikipedia,omitempty"`
	Territory        string                `json:"territory,omitempty"`
	Description      string                `json:"description,omitempty"`
	Observations     string                `json:"observations,omitempty"`
	ModerationStatus string                `json:"moderation_status,omitempty"`
}

type confessionalExportHierarchyRelation struct {
	Parent           confessionalEntityRef `json:"parent"`
	Child            confessionalEntityRef `json:"child"`
	RelationType     string                `json:"relation_type"`
	StartsYear       *int                  `json:"starts_year,omitempty"`
	EndsYear         *int                  `json:"ends_year,omitempty"`
	Observations     string                `json:"observations,omitempty"`
	ModerationStatus string                `json:"moderation_status,omitempty"`
}

type confessionalExportTerritorialRelation struct {
	Entity           confessionalEntityRef        `json:"entity"`
	Municipality     confessionalMunicipalityRef  `json:"municipality"`
	Nucleus          *confessionalMunicipalityRef `json:"nucleus,omitempty"`
	RelationType     string                       `json:"relation_type"`
	StartsYear       *int                         `json:"starts_year,omitempty"`
	EndsYear         *int                         `json:"ends_year,omitempty"`
	Observations     string                       `json:"observations,omitempty"`
	ModerationStatus string                       `json:"moderation_status,omitempty"`
}

type confessionalExportArchiveRelation struct {
	Entity           confessionalEntityRef  `json:"entity"`
	Archive          confessionalArchiveRef `json:"archive"`
	RelationType     string                 `json:"relation_type"`
	StartsYear       *int                   `json:"starts_year,omitempty"`
	EndsYear         *int                   `json:"ends_year,omitempty"`
	Observations     string                 `json:"observations,omitempty"`
	State            string                 `json:"state,omitempty"`
	ModerationStatus string                 `json:"moderation_status,omitempty"`
}

type confessionalImportViewPlan struct {
	Performed bool
	CanApply  bool
	Errors    []string
	Warnings  []string
	Conflicts []string

	EntityCreates     []string
	EntityExisting    []string
	HierarchyCreates  []string
	HierarchyExisting []string
	TerritoryCreates  []string
	TerritoryExisting []string
	ArchiveCreates    []string
	ArchiveExisting   []string

	EntityCreateCount      int
	EntityExistingCount    int
	HierarchyCreateCount   int
	HierarchyExistingCount int
	TerritoryCreateCount   int
	TerritoryExistingCount int
	ArchiveCreateCount     int
	ArchiveExistingCount   int

	PayloadB64          string
	IncludeNonPublished bool
}

type confessionalImportEntityCreate struct {
	RefKey string
	Entity db.EntitatReligiosa
	Label  string
}

type confessionalImportHierarchyCreate struct {
	ParentRefKey string
	ChildRefKey  string
	RelationType string
	StartsYear   sql.NullInt64
	EndsYear     sql.NullInt64
	Observations string
	Status       string
	Label        string
}

type confessionalImportTerritoryCreate struct {
	EntityRefKey string
	MunicipiID   int
	NucliID      sql.NullInt64
	RelationType string
	StartsYear   sql.NullInt64
	EndsYear     sql.NullInt64
	Observations string
	Status       string
	Label        string
}

type confessionalImportArchiveCreate struct {
	EntityRefKey string
	ArxiuID      int
	RelationType string
	StartsYear   sql.NullInt64
	EndsYear     sql.NullInt64
	Observations string
	State        string
	Status       string
	Label        string
}

type confessionalImportPlan struct {
	View              confessionalImportViewPlan
	ExistingEntityIDs map[string]int
	EntityCreates     []confessionalImportEntityCreate
	HierarchyCreates  []confessionalImportHierarchyCreate
	TerritoryCreates  []confessionalImportTerritoryCreate
	ArchiveCreates    []confessionalImportArchiveCreate
}

func (a *App) AdminConfessionalExport(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requirePermissionKey(w, r, permKeyTerritoriConfessionalImportExportExport, PermissionTarget{}); !ok {
		return
	}
	includeNonPublished := r.URL.Query().Get("include_non_published") == "1"
	includeHierarchy := r.URL.Query().Get("include_hierarchy") != "0"
	includeTerritory := r.URL.Query().Get("include_territorial") != "0"
	includeArchives := r.URL.Query().Get("include_archives") != "0"
	religionCode := normalizeCatalogCode(strings.TrimSpace(r.URL.Query().Get("religio_confessio_codi")))
	levelCode := normalizeCatalogCode(strings.TrimSpace(r.URL.Query().Get("nivell_confessional_codi")))

	allEntitats, err := a.DB.ListEntitatsReligioses()
	if err != nil {
		http.NotFound(w, r)
		return
	}
	allHierarchy, _ := a.DB.ListEntitatReligiosaRelacions()
	allTerritory, _ := a.DB.ListMunicipiEntitatsReligioses(0)
	allArchiveRelations, _ := a.DB.ListArxiuEntitatsReligioses(0, 0, "")
	allMunicipiRows, _ := a.DB.ListMunicipis(db.MunicipiFilter{})
	allArxius, _ := a.DB.ListArxius(db.ArxiuFilter{Limit: -1})

	municipisByID := a.confessionalMunicipisByID(allMunicipiRows)
	arxiusByID := map[int]db.ArxiuWithCount{}
	for _, row := range allArxius {
		arxiusByID[row.ID] = row
	}

	exportedEntities := make([]db.EntitatReligiosa, 0)
	entityRefs := map[int]confessionalEntityRef{}
	for _, entity := range allEntitats {
		if !includeNonPublished && entity.ModeracioEstat != "publicat" {
			continue
		}
		if religionCode != "" && entity.ReligioConfessioCodi != religionCode {
			continue
		}
		if levelCode != "" && entity.NivellConfessionalCodi != levelCode {
			continue
		}
		exportedEntities = append(exportedEntities, entity)
		entityRefs[entity.ID] = confessionalEntityRefFromEntity(entity)
	}
	sort.Slice(exportedEntities, func(i, j int) bool {
		return entityRefs[exportedEntities[i].ID].Code < entityRefs[exportedEntities[j].ID].Code
	})

	payload := confessionalExportPayload{
		Schema:     confessionalImportExportSchema,
		ExportedAt: time.Now().Format(time.RFC3339),
		Source: confessionalExportSource{
			App:    "CercaGenealogica",
			Module: "confessional",
		},
	}
	for _, entity := range exportedEntities {
		payload.Items.EntitatsReligioses = append(payload.Items.EntitatsReligioses, confessionalExportEntityRecord{
			Ref:              confessionalEntityRefFromEntity(entity),
			Name:             entity.Nom,
			ReligionCode:     entity.ReligioConfessioCodi,
			LevelCode:        entity.NivellConfessionalCodi,
			Type:             strings.TrimSpace(entity.TipusEntitat),
			SpecificType:     strings.TrimSpace(entity.TipusEspecific),
			StartsYear:       confIntPtr(entity.AnyInici),
			EndsYear:         confIntPtr(entity.AnyFi),
			State:            strings.TrimSpace(entity.Estat),
			Web:              strings.TrimSpace(entity.Web),
			Wikipedia:        strings.TrimSpace(entity.WebWikipedia),
			Territory:        strings.TrimSpace(entity.Territori),
			Description:      strings.TrimSpace(entity.Descripcio),
			Observations:     strings.TrimSpace(entity.Observacions),
			ModerationStatus: strings.TrimSpace(entity.ModeracioEstat),
		})
	}

	if includeHierarchy {
		for _, rel := range allHierarchy {
			if !includeNonPublished && rel.ModeracioEstat != "publicat" {
				continue
			}
			parentRef, okParent := entityRefs[rel.EntitatOrigenID]
			childRef, okChild := entityRefs[rel.EntitatDestiID]
			if !okParent || !okChild {
				continue
			}
			payload.Items.RelacionsEntitats = append(payload.Items.RelacionsEntitats, confessionalExportHierarchyRelation{
				Parent:           parentRef,
				Child:            childRef,
				RelationType:     strings.TrimSpace(rel.TipusRelacio),
				StartsYear:       confIntPtr(rel.AnyInici),
				EndsYear:         confIntPtr(rel.AnyFi),
				Observations:     strings.TrimSpace(rel.Observacions),
				ModerationStatus: strings.TrimSpace(rel.ModeracioEstat),
			})
		}
	}

	if includeTerritory {
		for _, rel := range allTerritory {
			if !includeNonPublished && rel.ModeracioEstat != "publicat" {
				continue
			}
			entityRef, ok := entityRefs[rel.EntitatReligiosaID]
			if !ok {
				continue
			}
			municipi, ok := municipisByID[rel.MunicipiID]
			if !ok {
				continue
			}
			record := confessionalExportTerritorialRelation{
				Entity:           entityRef,
				Municipality:     a.confessionalMunicipalityRef(municipi, municipisByID),
				RelationType:     strings.TrimSpace(rel.TipusRelacio),
				StartsYear:       confIntPtr(rel.AnyInici),
				EndsYear:         confIntPtr(rel.AnyFi),
				Observations:     strings.TrimSpace(rel.Observacions),
				ModerationStatus: strings.TrimSpace(rel.ModeracioEstat),
			}
			if rel.NucliID.Valid {
				if nucli, ok := municipisByID[int(rel.NucliID.Int64)]; ok {
					ref := a.confessionalMunicipalityRef(nucli, municipisByID)
					record.Nucleus = &ref
				}
			}
			payload.Items.RelacionsTerritorials = append(payload.Items.RelacionsTerritorials, record)
		}
	}

	if includeArchives {
		for _, rel := range allArchiveRelations {
			if !includeNonPublished && rel.ModeracioEstat != "publicat" {
				continue
			}
			entityRef, ok := entityRefs[rel.EntitatReligiosaID]
			if !ok {
				continue
			}
			arxiu, ok := arxiusByID[rel.ArxiuID]
			if !ok {
				continue
			}
			payload.Items.RelacionsArxius = append(payload.Items.RelacionsArxius, confessionalExportArchiveRelation{
				Entity:           entityRef,
				Archive:          a.confessionalArchiveRef(arxiu, municipisByID),
				RelationType:     strings.TrimSpace(rel.TipusRelacio),
				StartsYear:       confIntPtr(rel.AnyInici),
				EndsYear:         confIntPtr(rel.AnyFi),
				Observations:     strings.TrimSpace(rel.Observacions),
				State:            strings.TrimSpace(rel.Estat),
				ModerationStatus: strings.TrimSpace(rel.ModeracioEstat),
			})
		}
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename=confessional-export.json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(payload)
}

func (a *App) AdminConfessionalImportDryRun(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriConfessionalImportExportImport, PermissionTarget{})
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseMultipartForm(confessionalImportExportMaxBytes); err != nil {
		a.renderConfessionalImportExportResult(w, r, user, confessionalImportViewPlan{
			Performed: true,
			Errors:    []string{T(ResolveLang(r), "confessional.io.error.invalid_json")},
		}, T(ResolveLang(r), "common.error"))
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		a.renderConfessionalImportExportResult(w, r, user, confessionalImportViewPlan{
			Performed: true,
			Errors:    []string{T(ResolveLang(r), "common.error")},
		}, T(ResolveLang(r), "common.error"))
		return
	}
	payloadBytes, err := readConfessionalImportPayloadBytes(r)
	if err != nil {
		a.renderConfessionalImportExportResult(w, r, user, confessionalImportViewPlan{
			Performed: true,
			Errors:    []string{err.Error()},
		}, "")
		return
	}
	plan := a.buildConfessionalImportPlan(payloadBytes, r.FormValue("include_non_published") == "1")
	a.renderConfessionalImportExportResult(w, r, user, plan.View, "")
}

func (a *App) AdminConfessionalImportApply(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requirePermissionKey(w, r, permKeyTerritoriConfessionalImportExportImport, PermissionTarget{})
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		a.renderConfessionalImportExportResult(w, r, user, confessionalImportViewPlan{
			Performed: true,
			Errors:    []string{T(ResolveLang(r), "common.error")},
		}, T(ResolveLang(r), "common.error"))
		return
	}
	if !validateCSRF(r, r.FormValue("csrf_token")) {
		a.renderConfessionalImportExportResult(w, r, user, confessionalImportViewPlan{
			Performed: true,
			Errors:    []string{T(ResolveLang(r), "common.error")},
		}, T(ResolveLang(r), "common.error"))
		return
	}
	rawPayload, err := base64.StdEncoding.DecodeString(strings.TrimSpace(r.FormValue("payload_b64")))
	if err != nil {
		a.renderConfessionalImportExportResult(w, r, user, confessionalImportViewPlan{
			Performed: true,
			Errors:    []string{T(ResolveLang(r), "confessional.io.error.invalid_json")},
		}, "")
		return
	}
	plan := a.buildConfessionalImportPlan(rawPayload, r.FormValue("include_non_published") == "1")
	if len(plan.View.Errors) > 0 || !plan.View.CanApply {
		a.renderConfessionalImportExportResult(w, r, user, plan.View, "")
		return
	}

	entityIDs := make(map[string]int, len(plan.ExistingEntityIDs)+len(plan.EntityCreates))
	for key, id := range plan.ExistingEntityIDs {
		entityIDs[key] = id
	}
	entityCreated, entitySkipped := 0, 0
	for _, item := range plan.EntityCreates {
		entity := item.Entity
		entity.CreatedBy = sqlNullIntFromInt(user.ID)
		entity.UpdatedBy = sqlNullIntFromInt(user.ID)
		if entity.ModeracioEstat == "publicat" {
			entity.ModeratedBy = sqlNullIntFromInt(user.ID)
			entity.ModeratedAt = sql.NullTime{Time: time.Now(), Valid: true}
		}
		newID, err := a.DB.SaveEntitatReligiosa(&entity)
		if err != nil {
			a.renderConfessionalImportExportResult(w, r, user, plan.View, fmt.Sprintf("Import confessional: %s", err))
			return
		}
		entityIDs[item.RefKey] = newID
		entityCreated++
	}
	entitySkipped = plan.View.EntityExistingCount

	hierarchyCreated, hierarchySkipped := 0, plan.View.HierarchyExistingCount
	for _, item := range plan.HierarchyCreates {
		rel := &db.EntitatReligiosaRelacio{
			EntitatOrigenID: entityIDs[item.ParentRefKey],
			EntitatDestiID:  entityIDs[item.ChildRefKey],
			TipusRelacio:    item.RelationType,
			AnyInici:        item.StartsYear,
			AnyFi:           item.EndsYear,
			Observacions:    item.Observations,
			ModeracioEstat:  item.Status,
			CreatedBy:       sqlNullIntFromInt(user.ID),
			UpdatedBy:       sqlNullIntFromInt(user.ID),
		}
		if item.Status == "publicat" {
			rel.ModeratedBy = sqlNullIntFromInt(user.ID)
			rel.ModeratedAt = sql.NullTime{Time: time.Now(), Valid: true}
		}
		if _, err := a.DB.SaveEntitatReligiosaRelacio(rel); err != nil {
			a.renderConfessionalImportExportResult(w, r, user, plan.View, fmt.Sprintf("Import confessional: %s", err))
			return
		}
		hierarchyCreated++
	}

	territoryCreated, territorySkipped := 0, plan.View.TerritoryExistingCount
	for _, item := range plan.TerritoryCreates {
		rel := &db.MunicipiEntitatReligiosa{
			MunicipiID:         item.MunicipiID,
			NucliID:            item.NucliID,
			EntitatReligiosaID: entityIDs[item.EntityRefKey],
			TipusRelacio:       item.RelationType,
			AnyInici:           item.StartsYear,
			AnyFi:              item.EndsYear,
			Observacions:       item.Observations,
			ModeracioEstat:     item.Status,
			CreatedBy:          sqlNullIntFromInt(user.ID),
			UpdatedBy:          sqlNullIntFromInt(user.ID),
		}
		if item.Status == "publicat" {
			rel.ModeratedBy = sqlNullIntFromInt(user.ID)
			rel.ModeratedAt = sql.NullTime{Time: time.Now(), Valid: true}
		}
		if _, err := a.DB.SaveMunicipiEntitatReligiosa(rel); err != nil {
			a.renderConfessionalImportExportResult(w, r, user, plan.View, fmt.Sprintf("Import confessional: %s", err))
			return
		}
		territoryCreated++
	}

	archiveCreated, archiveSkipped := 0, plan.View.ArchiveExistingCount
	for _, item := range plan.ArchiveCreates {
		rel := &db.ArxiuEntitatReligiosa{
			ArxiuID:            item.ArxiuID,
			EntitatReligiosaID: entityIDs[item.EntityRefKey],
			TipusRelacio:       item.RelationType,
			AnyInici:           item.StartsYear,
			AnyFi:              item.EndsYear,
			Observacions:       item.Observations,
			Estat:              item.State,
			ModeracioEstat:     item.Status,
			CreatedBy:          sqlNullIntFromInt(user.ID),
			UpdatedBy:          sqlNullIntFromInt(user.ID),
		}
		if item.Status == "publicat" {
			rel.ModeratedBy = sqlNullIntFromInt(user.ID)
			rel.ModeratedAt = sql.NullTime{Time: time.Now(), Valid: true}
		}
		if _, err := a.DB.SaveArxiuEntitatReligiosa(rel); err != nil {
			a.renderConfessionalImportExportResult(w, r, user, plan.View, fmt.Sprintf("Import confessional: %s", err))
			return
		}
		archiveCreated++
	}

	http.Redirect(w, r, withQueryParams("/admin/import-export?tab=confessional&subtab=confessional-import", map[string]string{
		"import":                   "1",
		"conf_entities_total":      strconv.Itoa(plan.View.EntityCreateCount + plan.View.EntityExistingCount),
		"conf_entities_created":    strconv.Itoa(entityCreated),
		"conf_entities_skipped":    strconv.Itoa(entitySkipped),
		"conf_hierarchy_total":     strconv.Itoa(plan.View.HierarchyCreateCount + plan.View.HierarchyExistingCount),
		"conf_hierarchy_created":   strconv.Itoa(hierarchyCreated),
		"conf_hierarchy_skipped":   strconv.Itoa(hierarchySkipped),
		"conf_territorial_total":   strconv.Itoa(plan.View.TerritoryCreateCount + plan.View.TerritoryExistingCount),
		"conf_territorial_created": strconv.Itoa(territoryCreated),
		"conf_territorial_skipped": strconv.Itoa(territorySkipped),
		"conf_archive_total":       strconv.Itoa(plan.View.ArchiveCreateCount + plan.View.ArchiveExistingCount),
		"conf_archive_created":     strconv.Itoa(archiveCreated),
		"conf_archive_skipped":     strconv.Itoa(archiveSkipped),
	}), http.StatusSeeOther)
}

func (a *App) renderConfessionalImportExportResult(w http.ResponseWriter, r *http.Request, user *db.User, plan confessionalImportViewPlan, msg string) {
	a.renderAdminImportExportPage(w, r, user, map[string]interface{}{
		"ActiveTab":                   "confessional",
		"ConfessionalSubtab":          "confessional-import",
		"ConfessionalDryRunAvailable": true,
		"ConfessionalDryRun":          plan,
		"ConfessionalMsg":             msg,
	})
}

func readConfessionalImportPayloadBytes(r *http.Request) ([]byte, error) {
	file, header, err := r.FormFile("import_file")
	if err != nil {
		return nil, fmt.Errorf("%s", T(ResolveLang(r), "confessional.io.error.invalid_json"))
	}
	defer file.Close()
	if header != nil && header.Size > confessionalImportExportMaxBytes {
		return nil, fmt.Errorf("%s", T(ResolveLang(r), "confessional.io.error.file_too_large"))
	}
	lr := io.LimitReader(file, confessionalImportExportMaxBytes+1)
	body, err := io.ReadAll(lr)
	if err != nil {
		return nil, fmt.Errorf("%s", T(ResolveLang(r), "confessional.io.error.invalid_json"))
	}
	if len(body) == 0 || len(body) > confessionalImportExportMaxBytes {
		return nil, fmt.Errorf("%s", T(ResolveLang(r), "confessional.io.error.file_too_large"))
	}
	return body, nil
}

func (a *App) buildConfessionalImportPlan(payloadBytes []byte, includeNonPublished bool) *confessionalImportPlan {
	view := confessionalImportViewPlan{
		Performed:           true,
		IncludeNonPublished: includeNonPublished,
		PayloadB64:          base64.StdEncoding.EncodeToString(payloadBytes),
	}
	plan := &confessionalImportPlan{
		View:              view,
		ExistingEntityIDs: map[string]int{},
	}

	var payload confessionalExportPayload
	dec := json.NewDecoder(bytes.NewReader(payloadBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&payload); err != nil {
		plan.View.Errors = append(plan.View.Errors, T(defaultLang, "confessional.io.error.invalid_json"))
		return plan
	}
	if payload.Schema != confessionalImportExportSchema {
		plan.View.Errors = append(plan.View.Errors, T(defaultLang, "confessional.io.error.unsupported_version"))
		return plan
	}

	allEntitats, _ := a.DB.ListEntitatsReligioses()
	allHierarchy, _ := a.DB.ListEntitatReligiosaRelacions()
	allTerritory, _ := a.DB.ListMunicipiEntitatsReligioses(0)
	allArchiveRelations, _ := a.DB.ListArxiuEntitatsReligioses(0, 0, "")
	allMunicipiRows, _ := a.DB.ListMunicipis(db.MunicipiFilter{})
	allArxius, _ := a.DB.ListArxius(db.ArxiuFilter{Limit: -1})

	municipisByID := a.confessionalMunicipisByID(allMunicipiRows)
	municipiIndex := a.confessionalMunicipalityIndex(municipisByID)
	arxiuIndex := a.confessionalArchiveIndex(allArxius, municipisByID)
	entityExact, entityCode, entityExactDup := confessionalEntityIndexes(allEntitats)

	payloadEntities := map[string]confessionalExportEntityRecord{}
	entityModels := map[string]db.EntitatReligiosa{}

	for _, item := range payload.Items.EntitatsReligioses {
		if !includeNonPublished && strings.TrimSpace(item.ModerationStatus) != "" && item.ModerationStatus != "publicat" {
			continue
		}
		ref := confessionalNormalizeEntityRef(item.Ref)
		if ref.Code == "" || ref.ReligionCode == "" || ref.LevelCode == "" || strings.TrimSpace(item.Name) == "" {
			plan.View.Errors = append(plan.View.Errors, fmt.Sprintf("%s: %s", T(defaultLang, "confessional.io.error.unresolved_reference"), confessionalEntityRefLabel(ref)))
			continue
		}
		_, _, religionOK, levelOK, compatible := ConfessionalLevelCompatibleWithReligion(ref.ReligionCode, ref.LevelCode)
		if !religionOK || !levelOK || !compatible {
			plan.View.Errors = append(plan.View.Errors, fmt.Sprintf("%s: %s", T(defaultLang, "confessional.io.error.invalid_catalog"), confessionalEntityRefLabel(ref)))
			continue
		}
		refKey := confessionalEntityRefKey(ref)
		if _, exists := payloadEntities[refKey]; exists {
			plan.View.Errors = append(plan.View.Errors, fmt.Sprintf("%s: %s", T(defaultLang, "confessional.io.error.duplicate_json"), confessionalEntityRefLabel(ref)))
			continue
		}
		payloadEntities[refKey] = item
		status := strings.TrimSpace(item.ModerationStatus)
		if status == "" {
			status = "publicat"
		}
		if !includeNonPublished && status != "publicat" {
			status = "publicat"
		}
		state := strings.TrimSpace(item.State)
		if state == "" {
			state = "actiu"
		}
		entityModels[refKey] = db.EntitatReligiosa{
			Codi:                   ref.Code,
			Nom:                    strings.TrimSpace(item.Name),
			ReligioConfessioCodi:   ref.ReligionCode,
			NivellConfessionalCodi: ref.LevelCode,
			TipusEntitat:           strings.TrimSpace(item.Type),
			TipusEspecific:         strings.TrimSpace(item.SpecificType),
			AnyInici:               confNullIntFromPtr(item.StartsYear),
			AnyFi:                  confNullIntFromPtr(item.EndsYear),
			Estat:                  state,
			Web:                    strings.TrimSpace(item.Web),
			WebWikipedia:           strings.TrimSpace(item.Wikipedia),
			Territori:              strings.TrimSpace(item.Territory),
			Descripcio:             strings.TrimSpace(item.Description),
			Observacions:           strings.TrimSpace(item.Observations),
			ModeracioEstat:         status,
		}
	}

	refKeys := make([]string, 0, len(payloadEntities))
	for key := range payloadEntities {
		refKeys = append(refKeys, key)
	}
	sort.Strings(refKeys)
	for _, refKey := range refKeys {
		item := payloadEntities[refKey]
		ref := confessionalNormalizeEntityRef(item.Ref)
		if entityExactDup[refKey] {
			plan.View.Errors = append(plan.View.Errors, fmt.Sprintf("%s: %s", T(defaultLang, "confessional.io.error.unresolved_reference"), confessionalEntityRefLabel(ref)))
			continue
		}
		if existing, ok := entityExact[refKey]; ok {
			plan.ExistingEntityIDs[refKey] = existing.ID
			plan.View.EntityExisting = append(plan.View.EntityExisting, confessionalEntityRefLabel(ref))
			if diff := confessionalEntityDiff(existing, entityModels[refKey]); diff != "" {
				plan.View.Conflicts = append(plan.View.Conflicts, fmt.Sprintf("%s: %s", confessionalEntityRefLabel(ref), diff))
			}
			continue
		}
		if codeMatches := entityCode[ref.Code]; len(codeMatches) > 0 {
			plan.View.Errors = append(plan.View.Errors, fmt.Sprintf("%s: %s", T(defaultLang, "confessional.io.error.code_conflict"), confessionalEntityRefLabel(ref)))
			continue
		}
		create := entityModels[refKey]
		create.CreatedBy = sqlNullIntFromInt(1)
		create.UpdatedBy = sqlNullIntFromInt(1)
		plan.EntityCreates = append(plan.EntityCreates, confessionalImportEntityCreate{
			RefKey: refKey,
			Entity: create,
			Label:  confessionalEntityRefLabel(ref),
		})
		plan.View.EntityCreates = append(plan.View.EntityCreates, confessionalEntityRefLabel(ref))
	}
	plan.View.EntityCreateCount = len(plan.View.EntityCreates)
	plan.View.EntityExistingCount = len(plan.View.EntityExisting)

	knownRefs := map[string]db.EntitatReligiosa{}
	for refKey, entity := range entityModels {
		knownRefs[refKey] = entity
	}
	for refKey, entity := range entityExact {
		if _, ok := knownRefs[refKey]; !ok {
			knownRefs[refKey] = entity
		}
	}

	existingHierarchyKeys := map[string]struct{}{}
	hierarchyGraph := map[string]map[string]bool{}
	for _, rel := range allHierarchy {
		if !includeNonPublished && rel.ModeracioEstat != "publicat" {
			continue
		}
		parent, okParent := entityIDToRefKey(rel.EntitatOrigenID, allEntitats)
		child, okChild := entityIDToRefKey(rel.EntitatDestiID, allEntitats)
		if !okParent || !okChild {
			continue
		}
		key := confessionalHierarchyKey(parent, child, rel.TipusRelacio, rel.AnyInici, rel.AnyFi)
		existingHierarchyKeys[key] = struct{}{}
		if hierarchyGraph[parent] == nil {
			hierarchyGraph[parent] = map[string]bool{}
		}
		hierarchyGraph[parent][child] = true
	}
	seenHierarchy := map[string]struct{}{}
	for _, rel := range payload.Items.RelacionsEntitats {
		if !includeNonPublished && strings.TrimSpace(rel.ModerationStatus) != "" && rel.ModerationStatus != "publicat" {
			continue
		}
		parentRef := confessionalNormalizeEntityRef(rel.Parent)
		childRef := confessionalNormalizeEntityRef(rel.Child)
		parentKey := confessionalEntityRefKey(parentRef)
		childKey := confessionalEntityRefKey(childRef)
		parent, okParent := knownRefs[parentKey]
		child, okChild := knownRefs[childKey]
		if !okParent || !okChild {
			plan.View.Errors = append(plan.View.Errors, fmt.Sprintf("%s: %s -> %s", T(defaultLang, "confessional.io.error.unresolved_reference"), confessionalEntityRefLabel(parentRef), confessionalEntityRefLabel(childRef)))
			continue
		}
		if err := validateConfessionalEntityRelation(&parent, &child); err != nil {
			plan.View.Errors = append(plan.View.Errors, fmt.Sprintf("%s: %s -> %s", T(defaultLang, "confessional.io.error.invalid_relation"), confessionalEntityRefLabel(parentRef), confessionalEntityRefLabel(childRef)))
			continue
		}
		if parentKey == childKey || confessionalGraphReachable(hierarchyGraph, childKey, parentKey) {
			plan.View.Errors = append(plan.View.Errors, fmt.Sprintf("%s: %s -> %s", T(defaultLang, "confessional.io.error.cycle"), confessionalEntityRefLabel(parentRef), confessionalEntityRefLabel(childRef)))
			continue
		}
		key := confessionalHierarchyKey(parentKey, childKey, strings.TrimSpace(rel.RelationType), confNullIntFromPtr(rel.StartsYear), confNullIntFromPtr(rel.EndsYear))
		if _, ok := seenHierarchy[key]; ok {
			plan.View.Errors = append(plan.View.Errors, fmt.Sprintf("%s: %s -> %s", T(defaultLang, "confessional.io.error.duplicate_json"), confessionalEntityRefLabel(parentRef), confessionalEntityRefLabel(childRef)))
			continue
		}
		seenHierarchy[key] = struct{}{}
		label := fmt.Sprintf("%s -> %s", confessionalEntityRefLabel(parentRef), confessionalEntityRefLabel(childRef))
		if _, ok := existingHierarchyKeys[key]; ok {
			plan.View.HierarchyExisting = append(plan.View.HierarchyExisting, label)
			continue
		}
		if hierarchyGraph[parentKey] == nil {
			hierarchyGraph[parentKey] = map[string]bool{}
		}
		hierarchyGraph[parentKey][childKey] = true
		status := strings.TrimSpace(rel.ModerationStatus)
		if status == "" {
			status = "publicat"
		}
		plan.HierarchyCreates = append(plan.HierarchyCreates, confessionalImportHierarchyCreate{
			ParentRefKey: parentKey,
			ChildRefKey:  childKey,
			RelationType: strings.TrimSpace(rel.RelationType),
			StartsYear:   confNullIntFromPtr(rel.StartsYear),
			EndsYear:     confNullIntFromPtr(rel.EndsYear),
			Observations: strings.TrimSpace(rel.Observations),
			Status:       status,
			Label:        label,
		})
		plan.View.HierarchyCreates = append(plan.View.HierarchyCreates, label)
	}
	plan.View.HierarchyCreateCount = len(plan.View.HierarchyCreates)
	plan.View.HierarchyExistingCount = len(plan.View.HierarchyExisting)

	existingTerritoryKeys := map[string]struct{}{}
	for _, rel := range allTerritory {
		if !includeNonPublished && rel.ModeracioEstat != "publicat" {
			continue
		}
		entityKey, ok := entityIDToRefKey(rel.EntitatReligiosaID, allEntitats)
		if !ok {
			continue
		}
		key := confessionalTerritoryKey(entityKey, rel.MunicipiID, rel.NucliID, rel.TipusRelacio, rel.AnyInici, rel.AnyFi)
		existingTerritoryKeys[key] = struct{}{}
	}
	seenTerritory := map[string]struct{}{}
	for _, rel := range payload.Items.RelacionsTerritorials {
		if !includeNonPublished && strings.TrimSpace(rel.ModerationStatus) != "" && rel.ModerationStatus != "publicat" {
			continue
		}
		entityRef := confessionalNormalizeEntityRef(rel.Entity)
		entityKey := confessionalEntityRefKey(entityRef)
		if _, ok := knownRefs[entityKey]; !ok {
			plan.View.Errors = append(plan.View.Errors, fmt.Sprintf("%s: %s", T(defaultLang, "confessional.io.error.unresolved_reference"), confessionalEntityRefLabel(entityRef)))
			continue
		}
		municipiID, err := confessionalResolveMunicipalityRef(rel.Municipality, municipiIndex)
		if err != nil {
			plan.View.Errors = append(plan.View.Errors, err.Error())
			continue
		}
		nucliID := sql.NullInt64{}
		if rel.Nucleus != nil {
			resolvedNucliID, err := confessionalResolveMunicipalityRef(*rel.Nucleus, municipiIndex)
			if err != nil {
				plan.View.Errors = append(plan.View.Errors, err.Error())
				continue
			}
			nucli, ok := municipisByID[resolvedNucliID]
			if !ok || !nucli.MunicipiID.Valid || int(nucli.MunicipiID.Int64) != municipiID {
				plan.View.Errors = append(plan.View.Errors, fmt.Sprintf("%s: %s", T(defaultLang, "confessional.io.error.unresolved_reference"), rel.Nucleus.Name))
				continue
			}
			nucliID = sql.NullInt64{Int64: int64(resolvedNucliID), Valid: true}
		}
		key := confessionalTerritoryKey(entityKey, municipiID, nucliID, strings.TrimSpace(rel.RelationType), confNullIntFromPtr(rel.StartsYear), confNullIntFromPtr(rel.EndsYear))
		label := fmt.Sprintf("%s -> %s", confessionalEntityRefLabel(entityRef), rel.Municipality.Name)
		if _, ok := seenTerritory[key]; ok {
			plan.View.Errors = append(plan.View.Errors, fmt.Sprintf("%s: %s", T(defaultLang, "confessional.io.error.duplicate_json"), label))
			continue
		}
		seenTerritory[key] = struct{}{}
		if _, ok := existingTerritoryKeys[key]; ok {
			plan.View.TerritoryExisting = append(plan.View.TerritoryExisting, label)
			continue
		}
		status := strings.TrimSpace(rel.ModerationStatus)
		if status == "" {
			status = "publicat"
		}
		plan.TerritoryCreates = append(plan.TerritoryCreates, confessionalImportTerritoryCreate{
			EntityRefKey: entityKey,
			MunicipiID:   municipiID,
			NucliID:      nucliID,
			RelationType: strings.TrimSpace(rel.RelationType),
			StartsYear:   confNullIntFromPtr(rel.StartsYear),
			EndsYear:     confNullIntFromPtr(rel.EndsYear),
			Observations: strings.TrimSpace(rel.Observations),
			Status:       status,
			Label:        label,
		})
		plan.View.TerritoryCreates = append(plan.View.TerritoryCreates, label)
	}
	plan.View.TerritoryCreateCount = len(plan.View.TerritoryCreates)
	plan.View.TerritoryExistingCount = len(plan.View.TerritoryExisting)

	existingArchiveKeys := map[string]struct{}{}
	for _, rel := range allArchiveRelations {
		if !includeNonPublished && rel.ModeracioEstat != "publicat" {
			continue
		}
		entityKey, ok := entityIDToRefKey(rel.EntitatReligiosaID, allEntitats)
		if !ok {
			continue
		}
		key := confessionalArchiveRelationKey(entityKey, rel.ArxiuID, rel.TipusRelacio, rel.AnyInici, rel.AnyFi)
		existingArchiveKeys[key] = struct{}{}
	}
	seenArchive := map[string]struct{}{}
	for _, rel := range payload.Items.RelacionsArxius {
		if !includeNonPublished && strings.TrimSpace(rel.ModerationStatus) != "" && rel.ModerationStatus != "publicat" {
			continue
		}
		entityRef := confessionalNormalizeEntityRef(rel.Entity)
		entityKey := confessionalEntityRefKey(entityRef)
		if _, ok := knownRefs[entityKey]; !ok {
			plan.View.Errors = append(plan.View.Errors, fmt.Sprintf("%s: %s", T(defaultLang, "confessional.io.error.unresolved_reference"), confessionalEntityRefLabel(entityRef)))
			continue
		}
		arxiuID, err := confessionalResolveArchiveRef(rel.Archive, arxiuIndex)
		if err != nil {
			plan.View.Errors = append(plan.View.Errors, err.Error())
			continue
		}
		key := confessionalArchiveRelationKey(entityKey, arxiuID, strings.TrimSpace(rel.RelationType), confNullIntFromPtr(rel.StartsYear), confNullIntFromPtr(rel.EndsYear))
		label := fmt.Sprintf("%s -> %s", confessionalEntityRefLabel(entityRef), rel.Archive.Name)
		if _, ok := seenArchive[key]; ok {
			plan.View.Errors = append(plan.View.Errors, fmt.Sprintf("%s: %s", T(defaultLang, "confessional.io.error.duplicate_json"), label))
			continue
		}
		seenArchive[key] = struct{}{}
		if _, ok := existingArchiveKeys[key]; ok {
			plan.View.ArchiveExisting = append(plan.View.ArchiveExisting, label)
			continue
		}
		status := strings.TrimSpace(rel.ModerationStatus)
		if status == "" {
			status = "publicat"
		}
		state := strings.TrimSpace(rel.State)
		if state == "" {
			state = "actiu"
		}
		plan.ArchiveCreates = append(plan.ArchiveCreates, confessionalImportArchiveCreate{
			EntityRefKey: entityKey,
			ArxiuID:      arxiuID,
			RelationType: strings.TrimSpace(rel.RelationType),
			StartsYear:   confNullIntFromPtr(rel.StartsYear),
			EndsYear:     confNullIntFromPtr(rel.EndsYear),
			Observations: strings.TrimSpace(rel.Observations),
			State:        state,
			Status:       status,
			Label:        label,
		})
		plan.View.ArchiveCreates = append(plan.View.ArchiveCreates, label)
	}
	plan.View.ArchiveCreateCount = len(plan.View.ArchiveCreates)
	plan.View.ArchiveExistingCount = len(plan.View.ArchiveExisting)
	plan.View.CanApply = len(plan.View.Errors) == 0
	return plan
}

func confessionalEntityIndexes(all []db.EntitatReligiosa) (map[string]db.EntitatReligiosa, map[string][]db.EntitatReligiosa, map[string]bool) {
	exact := map[string]db.EntitatReligiosa{}
	codeMap := map[string][]db.EntitatReligiosa{}
	dup := map[string]bool{}
	for _, entity := range all {
		ref := confessionalEntityRefFromEntity(entity)
		key := confessionalEntityRefKey(ref)
		if key != "" {
			if _, exists := exact[key]; exists {
				dup[key] = true
			} else {
				exact[key] = entity
			}
		}
		if ref.Code != "" {
			codeMap[ref.Code] = append(codeMap[ref.Code], entity)
		}
	}
	return exact, codeMap, dup
}

func (a *App) confessionalMunicipisByID(rows []db.MunicipiRow) map[int]*db.Municipi {
	res := map[int]*db.Municipi{}
	for _, row := range rows {
		full, err := a.DB.GetMunicipi(row.ID)
		if err != nil || full == nil {
			continue
		}
		res[row.ID] = full
	}
	return res
}

func (a *App) confessionalMunicipalityRef(m *db.Municipi, all map[int]*db.Municipi) confessionalMunicipalityRef {
	levelISO := a.levelISOMap()
	ref := confessionalMunicipalityRef{
		Name:        strings.TrimSpace(m.Nom),
		Type:        strings.TrimSpace(m.Tipus),
		CountryISO2: municipiISO2(m, levelISO),
	}
	parentNames := make([]string, 0)
	cur := m
	seen := map[int]bool{}
	for cur != nil && cur.MunicipiID.Valid {
		pid := int(cur.MunicipiID.Int64)
		if seen[pid] {
			break
		}
		seen[pid] = true
		parent := all[pid]
		if parent == nil {
			break
		}
		parentNames = append(parentNames, strings.TrimSpace(parent.Nom))
		cur = parent
	}
	if len(parentNames) > 0 {
		ref.ParentNames = parentNames
	}
	return ref
}

func (a *App) confessionalArchiveRef(row db.ArxiuWithCount, municipis map[int]*db.Municipi) confessionalArchiveRef {
	ref := confessionalArchiveRef{
		Name: strings.TrimSpace(row.Nom),
		Type: strings.TrimSpace(row.Tipus),
	}
	if row.MunicipiID.Valid {
		if municipi, ok := municipis[int(row.MunicipiID.Int64)]; ok {
			munRef := a.confessionalMunicipalityRef(municipi, municipis)
			ref.Municipality = &munRef
		}
	}
	return ref
}

func (a *App) confessionalMunicipalityIndex(all map[int]*db.Municipi) map[string][]int {
	index := map[string][]int{}
	for id, municipi := range all {
		ref := a.confessionalMunicipalityRef(municipi, all)
		key := confessionalMunicipalityRefKey(ref)
		index[key] = append(index[key], id)
	}
	return index
}

func (a *App) confessionalArchiveIndex(rows []db.ArxiuWithCount, municipis map[int]*db.Municipi) map[string][]int {
	index := map[string][]int{}
	for _, row := range rows {
		ref := a.confessionalArchiveRef(row, municipis)
		key := confessionalArchiveRefKey(ref)
		index[key] = append(index[key], row.ID)
	}
	return index
}

func confessionalResolveMunicipalityRef(ref confessionalMunicipalityRef, index map[string][]int) (int, error) {
	key := confessionalMunicipalityRefKey(confessionalNormalizeMunicipalityRef(ref))
	ids := index[key]
	if len(ids) == 1 {
		return ids[0], nil
	}
	if len(ids) > 1 {
		return 0, fmt.Errorf("%s: %s", T(defaultLang, "confessional.io.error.unresolved_reference"), ref.Name)
	}
	return 0, fmt.Errorf("%s: %s", T(defaultLang, "confessional.io.error.unresolved_reference"), ref.Name)
}

func confessionalResolveArchiveRef(ref confessionalArchiveRef, index map[string][]int) (int, error) {
	key := confessionalArchiveRefKey(confessionalNormalizeArchiveRef(ref))
	ids := index[key]
	if len(ids) == 1 {
		return ids[0], nil
	}
	return 0, fmt.Errorf("%s: %s", T(defaultLang, "confessional.io.error.unresolved_reference"), ref.Name)
}

func confessionalNormalizeEntityRef(ref confessionalEntityRef) confessionalEntityRef {
	ref.Code = normalizeCatalogCode(ref.Code)
	ref.ReligionCode = normalizeCatalogCode(ref.ReligionCode)
	ref.LevelCode = normalizeCatalogCode(ref.LevelCode)
	ref.Name = strings.TrimSpace(ref.Name)
	return ref
}

func confessionalNormalizeMunicipalityRef(ref confessionalMunicipalityRef) confessionalMunicipalityRef {
	ref.Name = strings.TrimSpace(ref.Name)
	ref.Type = strings.TrimSpace(ref.Type)
	ref.CountryISO2 = strings.ToUpper(strings.TrimSpace(ref.CountryISO2))
	for i := range ref.ParentNames {
		ref.ParentNames[i] = strings.TrimSpace(ref.ParentNames[i])
	}
	return ref
}

func confessionalNormalizeArchiveRef(ref confessionalArchiveRef) confessionalArchiveRef {
	ref.Name = strings.TrimSpace(ref.Name)
	ref.Type = strings.TrimSpace(ref.Type)
	if ref.Municipality != nil {
		m := confessionalNormalizeMunicipalityRef(*ref.Municipality)
		ref.Municipality = &m
	}
	return ref
}

func confessionalEntityRefFromEntity(entity db.EntitatReligiosa) confessionalEntityRef {
	return confessionalNormalizeEntityRef(confessionalEntityRef{
		Code:         entity.Codi,
		ReligionCode: entity.ReligioConfessioCodi,
		LevelCode:    entity.NivellConfessionalCodi,
		Name:         entity.Nom,
	})
}

func confessionalEntityRefKey(ref confessionalEntityRef) string {
	ref = confessionalNormalizeEntityRef(ref)
	if ref.Code == "" || ref.ReligionCode == "" || ref.LevelCode == "" {
		return ""
	}
	return strings.Join([]string{ref.Code, ref.ReligionCode, ref.LevelCode}, "|")
}

func confessionalMunicipalityRefKey(ref confessionalMunicipalityRef) string {
	ref = confessionalNormalizeMunicipalityRef(ref)
	return strings.Join([]string{
		strings.ToLower(ref.Name),
		strings.ToLower(ref.Type),
		strings.ToLower(ref.CountryISO2),
		strings.ToLower(strings.Join(ref.ParentNames, ">")),
	}, "|")
}

func confessionalArchiveRefKey(ref confessionalArchiveRef) string {
	ref = confessionalNormalizeArchiveRef(ref)
	municipiKey := ""
	if ref.Municipality != nil {
		municipiKey = confessionalMunicipalityRefKey(*ref.Municipality)
	}
	return strings.Join([]string{
		strings.ToLower(ref.Name),
		strings.ToLower(ref.Type),
		municipiKey,
	}, "|")
}

func confessionalHierarchyKey(parentKey, childKey, relationType string, startsYear, endsYear sql.NullInt64) string {
	return strings.Join([]string{
		parentKey,
		childKey,
		strings.TrimSpace(relationType),
		confNullIntKey(startsYear),
		confNullIntKey(endsYear),
	}, "|")
}

func confessionalTerritoryKey(entityKey string, municipiID int, nucliID sql.NullInt64, relationType string, startsYear, endsYear sql.NullInt64) string {
	return strings.Join([]string{
		entityKey,
		strconv.Itoa(municipiID),
		confNullIntKey(nucliID),
		strings.TrimSpace(relationType),
		confNullIntKey(startsYear),
		confNullIntKey(endsYear),
	}, "|")
}

func confessionalArchiveRelationKey(entityKey string, arxiuID int, relationType string, startsYear, endsYear sql.NullInt64) string {
	return strings.Join([]string{
		entityKey,
		strconv.Itoa(arxiuID),
		strings.TrimSpace(relationType),
		confNullIntKey(startsYear),
		confNullIntKey(endsYear),
	}, "|")
}

func confNullIntKey(v sql.NullInt64) string {
	if !v.Valid {
		return ""
	}
	return strconv.FormatInt(v.Int64, 10)
}

func confNullIntFromPtr(v *int) sql.NullInt64 {
	if v == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(*v), Valid: true}
}

func confIntPtr(v sql.NullInt64) *int {
	if !v.Valid {
		return nil
	}
	n := int(v.Int64)
	return &n
}

func confessionalEntityRefLabel(ref confessionalEntityRef) string {
	ref = confessionalNormalizeEntityRef(ref)
	if ref.Name != "" {
		return ref.Name + " (" + ref.Code + ")"
	}
	return ref.Code
}

func confessionalEntityDiff(existing, imported db.EntitatReligiosa) string {
	diff := make([]string, 0)
	if strings.TrimSpace(existing.Nom) != strings.TrimSpace(imported.Nom) {
		diff = append(diff, "nom")
	}
	if strings.TrimSpace(existing.TipusEntitat) != strings.TrimSpace(imported.TipusEntitat) {
		diff = append(diff, "tipus")
	}
	if strings.TrimSpace(existing.TipusEspecific) != strings.TrimSpace(imported.TipusEspecific) {
		diff = append(diff, "tipus_especific")
	}
	if existing.AnyInici != imported.AnyInici {
		diff = append(diff, "any_inici")
	}
	if existing.AnyFi != imported.AnyFi {
		diff = append(diff, "any_fi")
	}
	if strings.TrimSpace(existing.Estat) != strings.TrimSpace(imported.Estat) {
		diff = append(diff, "estat")
	}
	if strings.TrimSpace(existing.Web) != strings.TrimSpace(imported.Web) {
		diff = append(diff, "web")
	}
	if strings.TrimSpace(existing.WebWikipedia) != strings.TrimSpace(imported.WebWikipedia) {
		diff = append(diff, "web_wikipedia")
	}
	if strings.TrimSpace(existing.Territori) != strings.TrimSpace(imported.Territori) {
		diff = append(diff, "territori")
	}
	if strings.TrimSpace(existing.Descripcio) != strings.TrimSpace(imported.Descripcio) {
		diff = append(diff, "descripcio")
	}
	if strings.TrimSpace(existing.Observacions) != strings.TrimSpace(imported.Observacions) {
		diff = append(diff, "observacions")
	}
	if strings.TrimSpace(existing.ModeracioEstat) != strings.TrimSpace(imported.ModeracioEstat) {
		diff = append(diff, "moderacio")
	}
	return strings.Join(diff, ", ")
}

func confessionalGraphReachable(graph map[string]map[string]bool, start, target string) bool {
	if start == target {
		return true
	}
	seen := map[string]bool{}
	queue := []string{start}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if seen[cur] {
			continue
		}
		seen[cur] = true
		for next := range graph[cur] {
			if next == target {
				return true
			}
			queue = append(queue, next)
		}
	}
	return false
}

func entityIDToRefKey(id int, all []db.EntitatReligiosa) (string, bool) {
	for _, entity := range all {
		if entity.ID == id {
			key := confessionalEntityRefKey(confessionalEntityRefFromEntity(entity))
			return key, key != ""
		}
	}
	return "", false
}
