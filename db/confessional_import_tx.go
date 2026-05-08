package db

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	ConfessionalImportTxStageAfterEntities  = "after_entities"
	ConfessionalImportTxStageAfterHierarchy = "after_hierarchy"
	ConfessionalImportTxStageAfterTerritory = "after_territory"
)

// ConfessionalImportTxTestHook permet forçar errors controlats en tests
// d'integració per verificar rollback real del flux transaccional.
var ConfessionalImportTxTestHook func(stage string) error

type confessionalImportTxOps struct {
	engine             string
	begin              func() (*sql.Tx, error)
	loadEntities       func(tx *sql.Tx) ([]EntitatReligiosa, error)
	loadHierarchy      func(tx *sql.Tx) ([]EntitatReligiosaRelacio, error)
	loadTerritory      func(tx *sql.Tx) ([]MunicipiEntitatReligiosa, error)
	loadArchive        func(tx *sql.Tx) ([]ArxiuEntitatReligiosa, error)
	insertEntity       func(tx *sql.Tx, entity *EntitatReligiosa) (int, error)
	insertHierarchy    func(tx *sql.Tx, rel *EntitatReligiosaRelacio) error
	insertTerritory    func(tx *sql.Tx, rel *MunicipiEntitatReligiosa) error
	insertArchive      func(tx *sql.Tx, rel *ArxiuEntitatReligiosa) error
	loadMunicipiParent func(tx *sql.Tx, municipiID int) (sql.NullInt64, error)
	archiveExists      func(tx *sql.Tx, arxiuID int) (bool, error)
}

func sqliteApplyConfessionalImportPlanTx(d *SQLite, plan *ConfessionalImportTxPlan) (*ConfessionalImportTxResult, error) {
	q := sqliteConfessionalQueries()
	ops := confessionalImportTxOps{
		engine:        "sqlite",
		begin:         func() (*sql.Tx, error) { return d.Conn.BeginTx(context.Background(), nil) },
		loadEntities:  func(tx *sql.Tx) ([]EntitatReligiosa, error) { return confessionalListEntitatsTx(tx, q) },
		loadHierarchy: func(tx *sql.Tx) ([]EntitatReligiosaRelacio, error) { return confessionalListEntitatRelacionsTx(tx, q) },
		loadTerritory: func(tx *sql.Tx) ([]MunicipiEntitatReligiosa, error) { return confessionalListTerritoryTx(tx, q) },
		loadArchive:   func(tx *sql.Tx) ([]ArxiuEntitatReligiosa, error) { return confessionalListArchiveTxSQLite(tx) },
		insertEntity: func(tx *sql.Tx, entity *EntitatReligiosa) (int, error) {
			return confessionalInsertEntitatTxSQLite(tx, entity)
		},
		insertHierarchy: func(tx *sql.Tx, rel *EntitatReligiosaRelacio) error {
			return confessionalInsertHierarchyTxSQLite(tx, rel)
		},
		insertTerritory: func(tx *sql.Tx, rel *MunicipiEntitatReligiosa) error {
			return confessionalInsertTerritoryTxSQLite(tx, rel)
		},
		insertArchive: func(tx *sql.Tx, rel *ArxiuEntitatReligiosa) error {
			return confessionalInsertArchiveTxSQLite(tx, rel)
		},
		loadMunicipiParent: func(tx *sql.Tx, municipiID int) (sql.NullInt64, error) {
			return confessionalLoadMunicipiParentTxSQLite(tx, municipiID)
		},
		archiveExists: func(tx *sql.Tx, arxiuID int) (bool, error) {
			return confessionalArchiveExistsTxSQLite(tx, arxiuID)
		},
	}
	return confessionalApplyImportPlanTx(ops, plan)
}

func postgresApplyConfessionalImportPlanTx(d *PostgreSQL, plan *ConfessionalImportTxPlan) (*ConfessionalImportTxResult, error) {
	q := postgresConfessionalQueries()
	ops := confessionalImportTxOps{
		engine:        "postgres",
		begin:         func() (*sql.Tx, error) { return d.Conn.BeginTx(context.Background(), nil) },
		loadEntities:  func(tx *sql.Tx) ([]EntitatReligiosa, error) { return confessionalListEntitatsTx(tx, q) },
		loadHierarchy: func(tx *sql.Tx) ([]EntitatReligiosaRelacio, error) { return confessionalListEntitatRelacionsTx(tx, q) },
		loadTerritory: func(tx *sql.Tx) ([]MunicipiEntitatReligiosa, error) { return confessionalListTerritoryTx(tx, q) },
		loadArchive:   func(tx *sql.Tx) ([]ArxiuEntitatReligiosa, error) { return confessionalListArchiveTxPostgres(tx) },
		insertEntity: func(tx *sql.Tx, entity *EntitatReligiosa) (int, error) {
			return confessionalInsertEntitatTxPostgres(tx, entity)
		},
		insertHierarchy: func(tx *sql.Tx, rel *EntitatReligiosaRelacio) error {
			return confessionalInsertHierarchyTxPostgres(tx, rel)
		},
		insertTerritory: func(tx *sql.Tx, rel *MunicipiEntitatReligiosa) error {
			return confessionalInsertTerritoryTxPostgres(tx, rel)
		},
		insertArchive: func(tx *sql.Tx, rel *ArxiuEntitatReligiosa) error {
			return confessionalInsertArchiveTxPostgres(tx, rel)
		},
		loadMunicipiParent: func(tx *sql.Tx, municipiID int) (sql.NullInt64, error) {
			return confessionalLoadMunicipiParentTxPostgres(tx, municipiID)
		},
		archiveExists: func(tx *sql.Tx, arxiuID int) (bool, error) {
			return confessionalArchiveExistsTxPostgres(tx, arxiuID)
		},
	}
	return confessionalApplyImportPlanTx(ops, plan)
}

func mysqlApplyConfessionalImportPlanTx(d *MySQL, plan *ConfessionalImportTxPlan) (*ConfessionalImportTxResult, error) {
	q := mysqlConfessionalQueries()
	ops := confessionalImportTxOps{
		engine:        "mysql",
		begin:         func() (*sql.Tx, error) { return d.Conn.BeginTx(context.Background(), nil) },
		loadEntities:  func(tx *sql.Tx) ([]EntitatReligiosa, error) { return confessionalListEntitatsTx(tx, q) },
		loadHierarchy: func(tx *sql.Tx) ([]EntitatReligiosaRelacio, error) { return confessionalListEntitatRelacionsTx(tx, q) },
		loadTerritory: func(tx *sql.Tx) ([]MunicipiEntitatReligiosa, error) { return confessionalListTerritoryTx(tx, q) },
		loadArchive:   func(tx *sql.Tx) ([]ArxiuEntitatReligiosa, error) { return confessionalListArchiveTxMySQL(tx) },
		insertEntity: func(tx *sql.Tx, entity *EntitatReligiosa) (int, error) {
			return confessionalInsertEntitatTxMySQL(tx, entity)
		},
		insertHierarchy: func(tx *sql.Tx, rel *EntitatReligiosaRelacio) error {
			return confessionalInsertHierarchyTxMySQL(tx, rel)
		},
		insertTerritory: func(tx *sql.Tx, rel *MunicipiEntitatReligiosa) error {
			return confessionalInsertTerritoryTxMySQL(tx, rel)
		},
		insertArchive: func(tx *sql.Tx, rel *ArxiuEntitatReligiosa) error {
			return confessionalInsertArchiveTxMySQL(tx, rel)
		},
		loadMunicipiParent: func(tx *sql.Tx, municipiID int) (sql.NullInt64, error) {
			return confessionalLoadMunicipiParentTxMySQL(tx, municipiID)
		},
		archiveExists: func(tx *sql.Tx, arxiuID int) (bool, error) {
			return confessionalArchiveExistsTxMySQL(tx, arxiuID)
		},
	}
	return confessionalApplyImportPlanTx(ops, plan)
}

func confessionalApplyImportPlanTx(ops confessionalImportTxOps, plan *ConfessionalImportTxPlan) (_ *ConfessionalImportTxResult, err error) {
	if plan == nil {
		return nil, fmt.Errorf("pla d'import confessional invàlid")
	}
	tx, err := ops.begin()
	if err != nil {
		return nil, WrapSQLError(SQLErrorContext{Engine: ops.engine, Component: "confessional_import_tx", Op: "begin_apply_confessional_import_tx"}, err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		if rbErr := tx.Rollback(); rbErr != nil && rbErr != sql.ErrTxDone && err == nil {
			err = WrapSQLError(SQLErrorContext{Engine: ops.engine, Component: "confessional_import_tx", Op: "rollback_apply_confessional_import_tx"}, rbErr)
		}
	}()

	entities, err := ops.loadEntities(tx)
	if err != nil {
		return nil, err
	}
	hierarchy, err := ops.loadHierarchy(tx)
	if err != nil {
		return nil, err
	}
	territory, err := ops.loadTerritory(tx)
	if err != nil {
		return nil, err
	}
	archiveRelations, err := ops.loadArchive(tx)
	if err != nil {
		return nil, err
	}

	entityByCode := map[string]EntitatReligiosa{}
	entityKeyByID := map[int]string{}
	for _, entity := range entities {
		entityByCode[strings.TrimSpace(entity.Codi)] = entity
		entityKeyByID[entity.ID] = confImportEntityKey(entity.Codi, entity.ReligioConfessioCodi, entity.NivellConfessionalCodi)
	}

	entityIDs := make(map[string]int, len(plan.ExistingEntityIDs)+len(plan.EntityCreates))
	for key, id := range plan.ExistingEntityIDs {
		entityIDs[key] = id
	}
	for key, id := range plan.ExistingEntityIDs {
		if _, ok := entityKeyByID[id]; !ok {
			return nil, fmt.Errorf("rollback complet: l'entitat referenciada %q ja no existeix abans d'aplicar", key)
		}
	}

	hierarchySet := map[string]bool{}
	for _, rel := range hierarchy {
		parentKey, okParent := entityKeyByID[rel.EntitatOrigenID]
		childKey, okChild := entityKeyByID[rel.EntitatDestiID]
		if !okParent || !okChild {
			continue
		}
		hierarchySet[confImportHierarchyKey(parentKey, childKey, rel.TipusRelacio, rel.AnyInici, rel.AnyFi)] = true
	}
	territorySet := map[string]bool{}
	for _, rel := range territory {
		entityKey, ok := entityKeyByID[rel.EntitatReligiosaID]
		if !ok {
			continue
		}
		territorySet[confImportTerritoryKey(entityKey, rel.MunicipiID, rel.NucliID, rel.TipusRelacio, rel.AnyInici, rel.AnyFi)] = true
	}
	archiveSet := map[string]bool{}
	for _, rel := range archiveRelations {
		entityKey, ok := entityKeyByID[rel.EntitatReligiosaID]
		if !ok {
			continue
		}
		archiveSet[confImportArchiveKey(entityKey, rel.ArxiuID, rel.TipusRelacio, rel.AnyInici, rel.AnyFi)] = true
	}

	result := &ConfessionalImportTxResult{
		CreatedEntityIDs:     map[string]int{},
		CreatedHierarchyKeys: []string{},
		CreatedTerritoryKeys: []string{},
		CreatedArchiveKeys:   []string{},
	}
	for _, item := range plan.EntityCreates {
		code := strings.TrimSpace(item.Entity.Codi)
		existing, exists := entityByCode[code]
		if exists {
			existingKey := confImportEntityKey(existing.Codi, existing.ReligioConfessioCodi, existing.NivellConfessionalCodi)
			if existingKey != item.RefKey {
				return nil, fmt.Errorf("rollback complet: conflicte de codi a %s", item.Label)
			}
			if diff := confImportEntityDiff(existing, item.Entity); diff != "" {
				return nil, fmt.Errorf("rollback complet: l'entitat %s ja existeix amb diferències a %s", item.Label, diff)
			}
			entityIDs[item.RefKey] = existing.ID
			result.EntitiesSkipped++
			continue
		}
		entity := item.Entity
		newID, err := ops.insertEntity(tx, &entity)
		if err != nil {
			return nil, fmt.Errorf("rollback complet: no s'ha pogut crear l'entitat %s: %w", item.Label, err)
		}
		entityIDs[item.RefKey] = newID
		entityByCode[code] = entity
		entityKeyByID[newID] = item.RefKey
		result.EntitiesCreated++
		result.CreatedEntityIDs[item.RefKey] = newID
	}
	if err := confessionalImportRunTestHook(ConfessionalImportTxStageAfterEntities); err != nil {
		return nil, fmt.Errorf("rollback complet: %w", err)
	}

	for _, item := range plan.HierarchyCreates {
		parentID, okParent := entityIDs[item.ParentRefKey]
		childID, okChild := entityIDs[item.ChildRefKey]
		if !okParent || !okChild {
			return nil, fmt.Errorf("rollback complet: la relació jeràrquica %s referencia entitats no disponibles", item.Label)
		}
		if _, ok := entityKeyByID[parentID]; !ok {
			return nil, fmt.Errorf("rollback complet: l'entitat pare de %s ja no existeix", item.Label)
		}
		if _, ok := entityKeyByID[childID]; !ok {
			return nil, fmt.Errorf("rollback complet: l'entitat filla de %s ja no existeix", item.Label)
		}
		key := confImportHierarchyKey(item.ParentRefKey, item.ChildRefKey, item.RelationType, item.StartsYear, item.EndsYear)
		if hierarchySet[key] {
			result.HierarchySkipped++
			continue
		}
		rel := &EntitatReligiosaRelacio{
			EntitatOrigenID: parentID,
			EntitatDestiID:  childID,
			TipusRelacio:    item.RelationType,
			AnyInici:        item.StartsYear,
			AnyFi:           item.EndsYear,
			Observacions:    item.Observations,
			ModeracioEstat:  item.Status,
			CreatedBy:       sql.NullInt64{Int64: int64(plan.ActorUserID), Valid: plan.ActorUserID > 0},
			UpdatedBy:       sql.NullInt64{Int64: int64(plan.ActorUserID), Valid: plan.ActorUserID > 0},
		}
		if item.Status == "publicat" {
			rel.ModeratedBy = sql.NullInt64{Int64: int64(plan.ActorUserID), Valid: plan.ActorUserID > 0}
			rel.ModeratedAt = sql.NullTime{Time: time.Now(), Valid: true}
		}
		if err := ops.insertHierarchy(tx, rel); err != nil {
			return nil, fmt.Errorf("rollback complet: no s'ha pogut crear la relació jeràrquica %s: %w", item.Label, err)
		}
		hierarchySet[key] = true
		result.HierarchyCreated++
		result.CreatedHierarchyKeys = append(result.CreatedHierarchyKeys, key)
	}
	if err := confessionalImportRunTestHook(ConfessionalImportTxStageAfterHierarchy); err != nil {
		return nil, fmt.Errorf("rollback complet: %w", err)
	}

	for _, item := range plan.TerritoryCreates {
		entityID, ok := entityIDs[item.EntityRefKey]
		if !ok {
			return nil, fmt.Errorf("rollback complet: la relació territorial %s referencia una entitat no disponible", item.Label)
		}
		if _, ok := entityKeyByID[entityID]; !ok {
			return nil, fmt.Errorf("rollback complet: l'entitat de %s ja no existeix", item.Label)
		}
		if _, err := ops.loadMunicipiParent(tx, item.MunicipiID); err != nil {
			return nil, fmt.Errorf("rollback complet: el municipi de %s ja no existeix", item.Label)
		}
		if item.NucliID.Valid {
			parentID, err := ops.loadMunicipiParent(tx, int(item.NucliID.Int64))
			if err != nil {
				return nil, fmt.Errorf("rollback complet: el nucli de %s ja no existeix", item.Label)
			}
			if !parentID.Valid || int(parentID.Int64) != item.MunicipiID {
				return nil, fmt.Errorf("rollback complet: el nucli de %s ja no pertany al municipi esperat", item.Label)
			}
		}
		key := confImportTerritoryKey(item.EntityRefKey, item.MunicipiID, item.NucliID, item.RelationType, item.StartsYear, item.EndsYear)
		if territorySet[key] {
			result.TerritorySkipped++
			continue
		}
		rel := &MunicipiEntitatReligiosa{
			MunicipiID:         item.MunicipiID,
			NucliID:            item.NucliID,
			EntitatReligiosaID: entityID,
			TipusRelacio:       item.RelationType,
			AnyInici:           item.StartsYear,
			AnyFi:              item.EndsYear,
			Observacions:       item.Observations,
			ModeracioEstat:     item.Status,
			CreatedBy:          sql.NullInt64{Int64: int64(plan.ActorUserID), Valid: plan.ActorUserID > 0},
			UpdatedBy:          sql.NullInt64{Int64: int64(plan.ActorUserID), Valid: plan.ActorUserID > 0},
		}
		if item.Status == "publicat" {
			rel.ModeratedBy = sql.NullInt64{Int64: int64(plan.ActorUserID), Valid: plan.ActorUserID > 0}
			rel.ModeratedAt = sql.NullTime{Time: time.Now(), Valid: true}
		}
		if err := ops.insertTerritory(tx, rel); err != nil {
			return nil, fmt.Errorf("rollback complet: no s'ha pogut crear la relació territorial %s: %w", item.Label, err)
		}
		territorySet[key] = true
		result.TerritoryCreated++
		result.CreatedTerritoryKeys = append(result.CreatedTerritoryKeys, key)
	}
	if err := confessionalImportRunTestHook(ConfessionalImportTxStageAfterTerritory); err != nil {
		return nil, fmt.Errorf("rollback complet: %w", err)
	}

	for _, item := range plan.ArchiveCreates {
		entityID, ok := entityIDs[item.EntityRefKey]
		if !ok {
			return nil, fmt.Errorf("rollback complet: la relació arxiu-entitat %s referencia una entitat no disponible", item.Label)
		}
		if _, ok := entityKeyByID[entityID]; !ok {
			return nil, fmt.Errorf("rollback complet: l'entitat de %s ja no existeix", item.Label)
		}
		exists, err := ops.archiveExists(tx, item.ArxiuID)
		if err != nil {
			return nil, fmt.Errorf("rollback complet: no s'ha pogut revalidar l'arxiu de %s: %w", item.Label, err)
		}
		if !exists {
			return nil, fmt.Errorf("rollback complet: l'arxiu de %s ja no existeix", item.Label)
		}
		key := confImportArchiveKey(item.EntityRefKey, item.ArxiuID, item.RelationType, item.StartsYear, item.EndsYear)
		if archiveSet[key] {
			result.ArchiveSkipped++
			continue
		}
		rel := &ArxiuEntitatReligiosa{
			ArxiuID:            item.ArxiuID,
			EntitatReligiosaID: entityID,
			TipusRelacio:       item.RelationType,
			AnyInici:           item.StartsYear,
			AnyFi:              item.EndsYear,
			Observacions:       item.Observations,
			Estat:              item.State,
			ModeracioEstat:     item.Status,
			CreatedBy:          sql.NullInt64{Int64: int64(plan.ActorUserID), Valid: plan.ActorUserID > 0},
			UpdatedBy:          sql.NullInt64{Int64: int64(plan.ActorUserID), Valid: plan.ActorUserID > 0},
		}
		if item.Status == "publicat" {
			rel.ModeratedBy = sql.NullInt64{Int64: int64(plan.ActorUserID), Valid: plan.ActorUserID > 0}
			rel.ModeratedAt = sql.NullTime{Time: time.Now(), Valid: true}
		}
		if err := ops.insertArchive(tx, rel); err != nil {
			return nil, fmt.Errorf("rollback complet: no s'ha pogut crear la relació arxiu-entitat %s: %w", item.Label, err)
		}
		archiveSet[key] = true
		result.ArchiveCreated++
		result.CreatedArchiveKeys = append(result.CreatedArchiveKeys, key)
	}

	if err := tx.Commit(); err != nil {
		return nil, WrapSQLError(SQLErrorContext{Engine: ops.engine, Component: "confessional_import_tx", Op: "commit_apply_confessional_import_tx"}, err)
	}
	committed = true
	return result, nil
}

func confessionalListEntitatsTx(tx *sql.Tx, q confessionalQueries) ([]EntitatReligiosa, error) {
	rows, err := tx.QueryContext(context.Background(), q.listEntitats)
	if err != nil {
		return nil, WrapSQLError(SQLErrorContext{Engine: q.engine, Component: "confessional_import_tx", Op: "list_entitats_apply_confessional_import_tx", Object: "entitat_religiosa"}, err)
	}
	defer rows.Close()
	out := make([]EntitatReligiosa, 0)
	for rows.Next() {
		item, err := scanEntitat(rows)
		if err != nil {
			return nil, WrapSQLError(SQLErrorContext{Engine: q.engine, Component: "confessional_import_tx", Op: "scan_entitats_apply_confessional_import_tx", Object: "entitat_religiosa"}, err)
		}
		out = append(out, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, WrapSQLError(SQLErrorContext{Engine: q.engine, Component: "confessional_import_tx", Op: "rows_entitats_apply_confessional_import_tx", Object: "entitat_religiosa"}, err)
	}
	return out, nil
}

func confessionalListEntitatRelacionsTx(tx *sql.Tx, q confessionalQueries) ([]EntitatReligiosaRelacio, error) {
	rows, err := tx.QueryContext(context.Background(), q.listEntitatRelacions)
	if err != nil {
		return nil, WrapSQLError(SQLErrorContext{Engine: q.engine, Component: "confessional_import_tx", Op: "list_hierarchy_apply_confessional_import_tx", Object: "entitat_religiosa_relacio"}, err)
	}
	defer rows.Close()
	out := make([]EntitatReligiosaRelacio, 0)
	for rows.Next() {
		item, err := scanEntitatRelacio(rows)
		if err != nil {
			return nil, WrapSQLError(SQLErrorContext{Engine: q.engine, Component: "confessional_import_tx", Op: "scan_hierarchy_apply_confessional_import_tx", Object: "entitat_religiosa_relacio"}, err)
		}
		out = append(out, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, WrapSQLError(SQLErrorContext{Engine: q.engine, Component: "confessional_import_tx", Op: "rows_hierarchy_apply_confessional_import_tx", Object: "entitat_religiosa_relacio"}, err)
	}
	return out, nil
}

func confessionalListTerritoryTx(tx *sql.Tx, q confessionalQueries) ([]MunicipiEntitatReligiosa, error) {
	rows, err := tx.QueryContext(context.Background(), q.listRelacionsAll)
	if err != nil {
		return nil, WrapSQLError(SQLErrorContext{Engine: q.engine, Component: "confessional_import_tx", Op: "list_territory_apply_confessional_import_tx", Object: "municipi_entitat_religiosa"}, err)
	}
	defer rows.Close()
	out := make([]MunicipiEntitatReligiosa, 0)
	for rows.Next() {
		item, err := scanRelacio(rows)
		if err != nil {
			return nil, WrapSQLError(SQLErrorContext{Engine: q.engine, Component: "confessional_import_tx", Op: "scan_territory_apply_confessional_import_tx", Object: "municipi_entitat_religiosa"}, err)
		}
		out = append(out, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, WrapSQLError(SQLErrorContext{Engine: q.engine, Component: "confessional_import_tx", Op: "rows_territory_apply_confessional_import_tx", Object: "municipi_entitat_religiosa"}, err)
	}
	return out, nil
}

func confessionalListArchiveTxSQLite(tx *sql.Tx) ([]ArxiuEntitatReligiosa, error) {
	return confessionalListArchiveTx(tx, "sqlite", `SELECT id, arxiu_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, COALESCE(observacions, ''), estat, moderation_status, COALESCE(moderation_notes, ''), created_by, updated_by, moderated_by, moderated_at, created_at, updated_at FROM arxiu_entitat_religiosa ORDER BY arxiu_id, entitat_religiosa_id, any_inici, id`)
}

func confessionalListArchiveTxPostgres(tx *sql.Tx) ([]ArxiuEntitatReligiosa, error) {
	return confessionalListArchiveTx(tx, "postgres", `SELECT id, arxiu_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, COALESCE(observacions, ''), estat, moderation_status, COALESCE(moderation_notes, ''), created_by, updated_by, moderated_by, moderated_at, created_at, updated_at FROM arxiu_entitat_religiosa ORDER BY arxiu_id, entitat_religiosa_id, any_inici, id`)
}

func confessionalListArchiveTxMySQL(tx *sql.Tx) ([]ArxiuEntitatReligiosa, error) {
	return confessionalListArchiveTx(tx, "mysql", `SELECT id, arxiu_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, COALESCE(observacions, ''), estat, moderation_status, COALESCE(moderation_notes, ''), created_by, updated_by, moderated_by, moderated_at, created_at, updated_at FROM arxiu_entitat_religiosa ORDER BY arxiu_id, entitat_religiosa_id, any_inici, id`)
}

func confessionalListArchiveTx(tx *sql.Tx, engine, query string) ([]ArxiuEntitatReligiosa, error) {
	rows, err := tx.QueryContext(context.Background(), query)
	if err != nil {
		return nil, WrapSQLError(SQLErrorContext{Engine: engine, Component: "confessional_import_tx", Op: "list_archive_apply_confessional_import_tx", Object: "arxiu_entitat_religiosa"}, err)
	}
	defer rows.Close()
	out := make([]ArxiuEntitatReligiosa, 0)
	for rows.Next() {
		var rel ArxiuEntitatReligiosa
		if err := scanArxiuEntitatReligiosa(rows, &rel); err != nil {
			return nil, WrapSQLError(SQLErrorContext{Engine: engine, Component: "confessional_import_tx", Op: "scan_archive_apply_confessional_import_tx", Object: "arxiu_entitat_religiosa"}, err)
		}
		out = append(out, rel)
	}
	if err := rows.Err(); err != nil {
		return nil, WrapSQLError(SQLErrorContext{Engine: engine, Component: "confessional_import_tx", Op: "rows_archive_apply_confessional_import_tx", Object: "arxiu_entitat_religiosa"}, err)
	}
	return out, nil
}

func confessionalInsertEntitatTxSQLite(tx *sql.Tx, entity *EntitatReligiosa) (int, error) {
	res, err := tx.ExecContext(context.Background(), `INSERT INTO entitat_religiosa (codi, nom, religio_confessio_codi, nivell_confessional_codi, religio_confessio_id, model_confessional_id, nivell_confessional_id, pais_id, parent_id, tipus_entitat, tipus_especific, any_inici, any_fi, estat, web, web_wikipedia, territori, descripcio, observacions, moderation_status, moderation_notes, created_by, updated_by, moderated_by, moderated_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))`,
		nullableStringArg(entity.Codi), entity.Nom, nullableStringArg(entity.ReligioConfessioCodi), nullableStringArg(entity.NivellConfessionalCodi), entity.ReligioConfessioID, entity.ModelConfessionalID, entity.NivellConfessionalID, entity.PaisID, entity.ParentID, entity.TipusEntitat, entity.TipusEspecific, entity.AnyInici, entity.AnyFi, entity.Estat, entity.Web, entity.WebWikipedia, entity.Territori, entity.Descripcio, entity.Observacions, entity.ModeracioEstat, entity.ModeracioMotiu, entity.CreatedBy, entity.UpdatedBy, entity.ModeratedBy, entity.ModeratedAt)
	if err != nil {
		return 0, WrapSQLError(SQLErrorContext{Engine: "sqlite", Component: "confessional_import_tx", Op: "insert_entitat_apply_confessional_import_tx", Object: "entitat_religiosa"}, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, WrapSQLError(SQLErrorContext{Engine: "sqlite", Component: "confessional_import_tx", Op: "last_insert_entitat_apply_confessional_import_tx", Object: "entitat_religiosa"}, err)
	}
	entity.ID = int(id)
	return entity.ID, nil
}

func confessionalInsertEntitatTxPostgres(tx *sql.Tx, entity *EntitatReligiosa) (int, error) {
	if err := tx.QueryRowContext(context.Background(), `INSERT INTO entitat_religiosa (codi, nom, religio_confessio_codi, nivell_confessional_codi, religio_confessio_id, model_confessional_id, nivell_confessional_id, pais_id, parent_id, tipus_entitat, tipus_especific, any_inici, any_fi, estat, web, web_wikipedia, territori, descripcio, observacions, moderation_status, moderation_notes, created_by, updated_by, moderated_by, moderated_at, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, NOW(), NOW()) RETURNING id`,
		nullableStringArg(entity.Codi), entity.Nom, nullableStringArg(entity.ReligioConfessioCodi), nullableStringArg(entity.NivellConfessionalCodi), entity.ReligioConfessioID, entity.ModelConfessionalID, entity.NivellConfessionalID, entity.PaisID, entity.ParentID, entity.TipusEntitat, entity.TipusEspecific, entity.AnyInici, entity.AnyFi, entity.Estat, entity.Web, entity.WebWikipedia, entity.Territori, entity.Descripcio, entity.Observacions, entity.ModeracioEstat, entity.ModeracioMotiu, entity.CreatedBy, entity.UpdatedBy, entity.ModeratedBy, entity.ModeratedAt).Scan(&entity.ID); err != nil {
		return 0, WrapSQLError(SQLErrorContext{Engine: "postgres", Component: "confessional_import_tx", Op: "insert_entitat_apply_confessional_import_tx", Object: "entitat_religiosa"}, err)
	}
	return entity.ID, nil
}

func confessionalInsertEntitatTxMySQL(tx *sql.Tx, entity *EntitatReligiosa) (int, error) {
	res, err := tx.ExecContext(context.Background(), `INSERT INTO entitat_religiosa (codi, nom, religio_confessio_codi, nivell_confessional_codi, religio_confessio_id, model_confessional_id, nivell_confessional_id, pais_id, parent_id, tipus_entitat, tipus_especific, any_inici, any_fi, estat, web, web_wikipedia, territori, descripcio, observacions, moderation_status, moderation_notes, created_by, updated_by, moderated_by, moderated_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())`,
		nullableStringArg(entity.Codi), entity.Nom, nullableStringArg(entity.ReligioConfessioCodi), nullableStringArg(entity.NivellConfessionalCodi), entity.ReligioConfessioID, entity.ModelConfessionalID, entity.NivellConfessionalID, entity.PaisID, entity.ParentID, entity.TipusEntitat, entity.TipusEspecific, entity.AnyInici, entity.AnyFi, entity.Estat, entity.Web, entity.WebWikipedia, entity.Territori, entity.Descripcio, entity.Observacions, entity.ModeracioEstat, entity.ModeracioMotiu, entity.CreatedBy, entity.UpdatedBy, entity.ModeratedBy, entity.ModeratedAt)
	if err != nil {
		return 0, WrapSQLError(SQLErrorContext{Engine: "mysql", Component: "confessional_import_tx", Op: "insert_entitat_apply_confessional_import_tx", Object: "entitat_religiosa"}, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, WrapSQLError(SQLErrorContext{Engine: "mysql", Component: "confessional_import_tx", Op: "last_insert_entitat_apply_confessional_import_tx", Object: "entitat_religiosa"}, err)
	}
	entity.ID = int(id)
	return entity.ID, nil
}

func confessionalInsertHierarchyTxSQLite(tx *sql.Tx, rel *EntitatReligiosaRelacio) error {
	_, err := tx.ExecContext(context.Background(), `INSERT INTO entitat_religiosa_relacio (entitat_origen_id, entitat_desti_id, tipus_relacio, any_inici, any_fi, font_id, observacions, moderation_status, moderation_notes, created_by, updated_by, moderated_by, moderated_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))`,
		rel.EntitatOrigenID, rel.EntitatDestiID, rel.TipusRelacio, rel.AnyInici, rel.AnyFi, rel.FontID, rel.Observacions, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt)
	return confImportWrapExec("sqlite", "insert_hierarchy_apply_confessional_import_tx", "entitat_religiosa_relacio", err)
}

func confessionalInsertHierarchyTxPostgres(tx *sql.Tx, rel *EntitatReligiosaRelacio) error {
	_, err := tx.ExecContext(context.Background(), `INSERT INTO entitat_religiosa_relacio (entitat_origen_id, entitat_desti_id, tipus_relacio, any_inici, any_fi, font_id, observacions, moderation_status, moderation_notes, created_by, updated_by, moderated_by, moderated_at, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW(), NOW())`,
		rel.EntitatOrigenID, rel.EntitatDestiID, rel.TipusRelacio, rel.AnyInici, rel.AnyFi, rel.FontID, rel.Observacions, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt)
	return confImportWrapExec("postgres", "insert_hierarchy_apply_confessional_import_tx", "entitat_religiosa_relacio", err)
}

func confessionalInsertHierarchyTxMySQL(tx *sql.Tx, rel *EntitatReligiosaRelacio) error {
	_, err := tx.ExecContext(context.Background(), `INSERT INTO entitat_religiosa_relacio (entitat_origen_id, entitat_desti_id, tipus_relacio, any_inici, any_fi, font_id, observacions, moderation_status, moderation_notes, created_by, updated_by, moderated_by, moderated_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())`,
		rel.EntitatOrigenID, rel.EntitatDestiID, rel.TipusRelacio, rel.AnyInici, rel.AnyFi, rel.FontID, rel.Observacions, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt)
	return confImportWrapExec("mysql", "insert_hierarchy_apply_confessional_import_tx", "entitat_religiosa_relacio", err)
}

func confessionalInsertTerritoryTxSQLite(tx *sql.Tx, rel *MunicipiEntitatReligiosa) error {
	_, err := tx.ExecContext(context.Background(), `INSERT INTO municipi_entitat_religiosa (municipi_id, nucli_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, observacions, moderation_status, moderation_notes, created_by, updated_by, moderated_by, moderated_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))`,
		rel.MunicipiID, rel.NucliID, rel.EntitatReligiosaID, rel.TipusRelacio, rel.AnyInici, rel.AnyFi, rel.Observacions, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt)
	return confImportWrapExec("sqlite", "insert_territory_apply_confessional_import_tx", "municipi_entitat_religiosa", err)
}

func confessionalInsertTerritoryTxPostgres(tx *sql.Tx, rel *MunicipiEntitatReligiosa) error {
	_, err := tx.ExecContext(context.Background(), `INSERT INTO municipi_entitat_religiosa (municipi_id, nucli_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, observacions, moderation_status, moderation_notes, created_by, updated_by, moderated_by, moderated_at, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW(), NOW())`,
		rel.MunicipiID, rel.NucliID, rel.EntitatReligiosaID, rel.TipusRelacio, rel.AnyInici, rel.AnyFi, rel.Observacions, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt)
	return confImportWrapExec("postgres", "insert_territory_apply_confessional_import_tx", "municipi_entitat_religiosa", err)
}

func confessionalInsertTerritoryTxMySQL(tx *sql.Tx, rel *MunicipiEntitatReligiosa) error {
	_, err := tx.ExecContext(context.Background(), `INSERT INTO municipi_entitat_religiosa (municipi_id, nucli_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, observacions, moderation_status, moderation_notes, created_by, updated_by, moderated_by, moderated_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())`,
		rel.MunicipiID, rel.NucliID, rel.EntitatReligiosaID, rel.TipusRelacio, rel.AnyInici, rel.AnyFi, rel.Observacions, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt)
	return confImportWrapExec("mysql", "insert_territory_apply_confessional_import_tx", "municipi_entitat_religiosa", err)
}

func confessionalInsertArchiveTxSQLite(tx *sql.Tx, rel *ArxiuEntitatReligiosa) error {
	_, err := tx.ExecContext(context.Background(), `INSERT INTO arxiu_entitat_religiosa (arxiu_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, observacions, estat, moderation_status, moderation_notes, created_by, updated_by, moderated_by, moderated_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))`,
		rel.ArxiuID, rel.EntitatReligiosaID, rel.TipusRelacio, rel.AnyInici, rel.AnyFi, rel.Observacions, rel.Estat, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt)
	return confImportWrapExec("sqlite", "insert_archive_apply_confessional_import_tx", "arxiu_entitat_religiosa", err)
}

func confessionalInsertArchiveTxPostgres(tx *sql.Tx, rel *ArxiuEntitatReligiosa) error {
	_, err := tx.ExecContext(context.Background(), `INSERT INTO arxiu_entitat_religiosa (arxiu_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, observacions, estat, moderation_status, moderation_notes, created_by, updated_by, moderated_by, moderated_at, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW(), NOW())`,
		rel.ArxiuID, rel.EntitatReligiosaID, rel.TipusRelacio, rel.AnyInici, rel.AnyFi, rel.Observacions, rel.Estat, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt)
	return confImportWrapExec("postgres", "insert_archive_apply_confessional_import_tx", "arxiu_entitat_religiosa", err)
}

func confessionalInsertArchiveTxMySQL(tx *sql.Tx, rel *ArxiuEntitatReligiosa) error {
	_, err := tx.ExecContext(context.Background(), `INSERT INTO arxiu_entitat_religiosa (arxiu_id, entitat_religiosa_id, tipus_relacio, any_inici, any_fi, observacions, estat, moderation_status, moderation_notes, created_by, updated_by, moderated_by, moderated_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())`,
		rel.ArxiuID, rel.EntitatReligiosaID, rel.TipusRelacio, rel.AnyInici, rel.AnyFi, rel.Observacions, rel.Estat, rel.ModeracioEstat, rel.ModeracioMotiu, rel.CreatedBy, rel.UpdatedBy, rel.ModeratedBy, rel.ModeratedAt)
	return confImportWrapExec("mysql", "insert_archive_apply_confessional_import_tx", "arxiu_entitat_religiosa", err)
}

func confessionalLoadMunicipiParentTxSQLite(tx *sql.Tx, municipiID int) (sql.NullInt64, error) {
	return confessionalLoadMunicipiParentTx(tx, "sqlite", `SELECT municipi_id FROM municipis WHERE id = ?`, municipiID)
}

func confessionalLoadMunicipiParentTxPostgres(tx *sql.Tx, municipiID int) (sql.NullInt64, error) {
	return confessionalLoadMunicipiParentTx(tx, "postgres", `SELECT municipi_id FROM municipis WHERE id = $1`, municipiID)
}

func confessionalLoadMunicipiParentTxMySQL(tx *sql.Tx, municipiID int) (sql.NullInt64, error) {
	return confessionalLoadMunicipiParentTx(tx, "mysql", `SELECT municipi_id FROM municipis WHERE id = ?`, municipiID)
}

func confessionalLoadMunicipiParentTx(tx *sql.Tx, engine, query string, municipiID int) (sql.NullInt64, error) {
	var parentID sql.NullInt64
	if err := tx.QueryRowContext(context.Background(), query, municipiID).Scan(&parentID); err != nil {
		if err == sql.ErrNoRows {
			return sql.NullInt64{}, err
		}
		return sql.NullInt64{}, WrapSQLError(SQLErrorContext{Engine: engine, Component: "confessional_import_tx", Op: "load_municipi_apply_confessional_import_tx", Object: "municipi", ObjectID: municipiID}, err)
	}
	return parentID, nil
}

func confessionalArchiveExistsTxSQLite(tx *sql.Tx, arxiuID int) (bool, error) {
	return confessionalArchiveExistsTx(tx, "sqlite", `SELECT 1 FROM arxius WHERE id = ?`, arxiuID)
}

func confessionalArchiveExistsTxPostgres(tx *sql.Tx, arxiuID int) (bool, error) {
	return confessionalArchiveExistsTx(tx, "postgres", `SELECT 1 FROM arxius WHERE id = $1`, arxiuID)
}

func confessionalArchiveExistsTxMySQL(tx *sql.Tx, arxiuID int) (bool, error) {
	return confessionalArchiveExistsTx(tx, "mysql", `SELECT 1 FROM arxius WHERE id = ?`, arxiuID)
}

func confessionalArchiveExistsTx(tx *sql.Tx, engine, query string, arxiuID int) (bool, error) {
	var marker int
	if err := tx.QueryRowContext(context.Background(), query, arxiuID).Scan(&marker); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, WrapSQLError(SQLErrorContext{Engine: engine, Component: "confessional_import_tx", Op: "load_arxiu_apply_confessional_import_tx", Object: "arxiu", ObjectID: arxiuID}, err)
	}
	return true, nil
}

func confImportWrapExec(engine, op, object string, err error) error {
	if err == nil {
		return nil
	}
	return WrapSQLError(SQLErrorContext{Engine: engine, Component: "confessional_import_tx", Op: op, Object: object}, err)
}

func confessionalImportRunTestHook(stage string) error {
	if ConfessionalImportTxTestHook == nil {
		return nil
	}
	return ConfessionalImportTxTestHook(stage)
}

func confImportEntityKey(code, religionCode, levelCode string) string {
	return strings.Join([]string{
		strings.TrimSpace(code),
		strings.TrimSpace(religionCode),
		strings.TrimSpace(levelCode),
	}, "|")
}

func confImportHierarchyKey(parentKey, childKey, relationType string, startsYear, endsYear sql.NullInt64) string {
	return strings.Join([]string{parentKey, childKey, strings.TrimSpace(relationType), confImportNullIntKey(startsYear), confImportNullIntKey(endsYear)}, "|")
}

func confImportTerritoryKey(entityKey string, municipiID int, nucliID sql.NullInt64, relationType string, startsYear, endsYear sql.NullInt64) string {
	return strings.Join([]string{entityKey, strconv.Itoa(municipiID), confImportNullIntKey(nucliID), strings.TrimSpace(relationType), confImportNullIntKey(startsYear), confImportNullIntKey(endsYear)}, "|")
}

func confImportArchiveKey(entityKey string, arxiuID int, relationType string, startsYear, endsYear sql.NullInt64) string {
	return strings.Join([]string{entityKey, strconv.Itoa(arxiuID), strings.TrimSpace(relationType), confImportNullIntKey(startsYear), confImportNullIntKey(endsYear)}, "|")
}

func confImportNullIntKey(v sql.NullInt64) string {
	if !v.Valid {
		return ""
	}
	return strconv.FormatInt(v.Int64, 10)
}

func confImportEntityDiff(existing, imported EntitatReligiosa) string {
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
