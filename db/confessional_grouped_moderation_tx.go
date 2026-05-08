package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

type confessionalWikiChangeMeta struct {
	Before         json.RawMessage `json:"before"`
	After          json.RawMessage `json:"after"`
	SourceChangeID int             `json:"source_change_id,omitempty"`
}

func buildConfessionalInitialWikiMetadata(snapshot *EntitatReligiosa) (string, error) {
	if snapshot == nil {
		return "", fmt.Errorf("entitat religiosa invalida")
	}
	afterJSON, err := json.Marshal(snapshot)
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(confessionalWikiChangeMeta{
		Before: nil,
		After:  afterJSON,
	})
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func sqliteApproveEntitatReligiosaWithInitialParentTx(d *SQLite, entityID, relationID int, motiu string, moderatorID int) error {
	q := sqliteConfessionalQueries()
	ctx := context.Background()
	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return confessionalWrap(q, "begin_approve_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}
	defer func() { _ = tx.Rollback() }()

	entity, err := scanEntitat(tx.QueryRowContext(ctx, q.getEntitat, entityID))
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("l'entitat religiosa no existeix")
		}
		return confessionalWrap(q, "load_entity_approve_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}
	if entity.ModeracioEstat != "pendent" {
		return fmt.Errorf("la filla ja no esta pendent")
	}

	relation, err := scanEntitatRelacio(tx.QueryRowContext(ctx, q.getEntitatRelacio, relationID))
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("la relacio pare/filla inicial no existeix")
		}
		return confessionalWrap(q, "load_relation_approve_entitat_religiosa_initial_parent_tx", "entitat_religiosa_relacio", relationID, err)
	}
	if relation.ModeracioEstat != "pendent" {
		return fmt.Errorf("la relacio pare/filla inicial ja no esta pendent")
	}
	if relation.EntitatDestiID != entityID {
		return fmt.Errorf("la relacio dependent no apunta a la filla esperada")
	}

	parent, err := scanEntitat(tx.QueryRowContext(ctx, q.getEntitat, relation.EntitatOrigenID))
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("l'entitat pare no existeix")
		}
		return confessionalWrap(q, "load_parent_approve_entitat_religiosa_initial_parent_tx", "entitat_religiosa", relation.EntitatOrigenID, err)
	}
	if parent.ModeracioEstat != "publicat" {
		return fmt.Errorf("l'entitat pare ja no esta publicada")
	}

	if _, err := tx.ExecContext(ctx, `UPDATE entitat_religiosa_relacio SET moderation_status=?, moderation_notes=?, moderated_by=?, moderated_at=datetime('now'), updated_at=datetime('now') WHERE id=?`, "publicat", motiu, moderatorID, relationID); err != nil {
		return confessionalWrap(q, "approve_relation_entitat_religiosa_initial_parent_tx", "entitat_religiosa_relacio", relationID, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE entitat_religiosa SET moderation_status=?, moderation_notes=?, moderated_by=?, moderated_at=datetime('now'), updated_at=datetime('now') WHERE id=?`, "publicat", motiu, moderatorID, entityID); err != nil {
		return confessionalWrap(q, "approve_entity_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}

	entity, err = scanEntitat(tx.QueryRowContext(ctx, q.getEntitat, entityID))
	if err != nil {
		return confessionalWrap(q, "reload_entity_approve_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}

	var publishedWikiCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM wiki_canvis WHERE object_type = ? AND object_id = ? AND moderation_status = 'publicat'`, "entitat_religiosa", entityID).Scan(&publishedWikiCount); err != nil {
		return confessionalWrap(q, "count_published_wiki_entitat_religiosa_initial_parent_tx", "wiki_canvis", entityID, err)
	}
	if publishedWikiCount > 0 {
		return fmt.Errorf("la wiki inicial publicada ja existeix per a l'entitat religiosa %d", entityID)
	}

	metadata, err := buildConfessionalInitialWikiMetadata(entity)
	if err != nil {
		return confessionalWrap(q, "build_wiki_entitat_religiosa_initial_parent_tx", "wiki_canvis", entityID, err)
	}
	moderatedBy := sql.NullInt64{Int64: int64(moderatorID), Valid: moderatorID > 0}
	if _, err := tx.ExecContext(ctx, `INSERT INTO wiki_canvis (object_type, object_id, change_type, field_key, old_value, new_value, metadata, moderation_status, moderated_by, moderated_at, moderation_notes, changed_by, changed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?, ?, datetime('now'))`, "entitat_religiosa", entityID, "create", "*", "", "", metadata, "publicat", moderatedBy, "", entity.CreatedBy); err != nil {
		return confessionalWrap(q, "insert_wiki_entitat_religiosa_initial_parent_tx", "wiki_canvis", entityID, err)
	}

	if err := tx.Commit(); err != nil {
		return confessionalWrap(q, "commit_approve_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}
	return nil
}

func sqliteRejectEntitatReligiosaWithInitialParentTx(d *SQLite, entityID, relationID int, motiu string, moderatorID int) error {
	q := sqliteConfessionalQueries()
	ctx := context.Background()
	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return confessionalWrap(q, "begin_reject_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}
	defer func() { _ = tx.Rollback() }()

	entity, err := scanEntitat(tx.QueryRowContext(ctx, q.getEntitat, entityID))
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("l'entitat religiosa no existeix")
		}
		return confessionalWrap(q, "load_entity_reject_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}
	if entity.ModeracioEstat != "pendent" {
		return fmt.Errorf("la filla ja no esta pendent")
	}

	relation, err := scanEntitatRelacio(tx.QueryRowContext(ctx, q.getEntitatRelacio, relationID))
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("la relacio pare/filla inicial no existeix")
		}
		return confessionalWrap(q, "load_relation_reject_entitat_religiosa_initial_parent_tx", "entitat_religiosa_relacio", relationID, err)
	}
	if relation.ModeracioEstat != "pendent" {
		return fmt.Errorf("la relacio pare/filla inicial ja no esta pendent")
	}
	if relation.EntitatDestiID != entityID {
		return fmt.Errorf("la relacio dependent no apunta a la filla esperada")
	}
	if _, err := scanEntitat(tx.QueryRowContext(ctx, q.getEntitat, relation.EntitatOrigenID)); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("l'entitat pare no existeix")
		}
		return confessionalWrap(q, "load_parent_reject_entitat_religiosa_initial_parent_tx", "entitat_religiosa", relation.EntitatOrigenID, err)
	}

	if _, err := tx.ExecContext(ctx, `UPDATE entitat_religiosa_relacio SET moderation_status=?, moderation_notes=?, moderated_by=?, moderated_at=datetime('now'), updated_at=datetime('now') WHERE id=?`, "rebutjat", motiu, moderatorID, relationID); err != nil {
		return confessionalWrap(q, "reject_relation_entitat_religiosa_initial_parent_tx", "entitat_religiosa_relacio", relationID, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE entitat_religiosa SET moderation_status=?, moderation_notes=?, moderated_by=?, moderated_at=datetime('now'), updated_at=datetime('now') WHERE id=?`, "rebutjat", motiu, moderatorID, entityID); err != nil {
		return confessionalWrap(q, "reject_entity_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}

	if err := tx.Commit(); err != nil {
		return confessionalWrap(q, "commit_reject_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}
	return nil
}

func postgresApproveEntitatReligiosaWithInitialParentTx(d *PostgreSQL, entityID, relationID int, motiu string, moderatorID int) error {
	q := postgresConfessionalQueries()
	ctx := context.Background()
	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return confessionalWrap(q, "begin_approve_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}
	defer func() { _ = tx.Rollback() }()

	entity, err := scanEntitat(tx.QueryRowContext(ctx, q.getEntitat, entityID))
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("l'entitat religiosa no existeix")
		}
		return confessionalWrap(q, "load_entity_approve_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}
	if entity.ModeracioEstat != "pendent" {
		return fmt.Errorf("la filla ja no esta pendent")
	}

	relation, err := scanEntitatRelacio(tx.QueryRowContext(ctx, q.getEntitatRelacio, relationID))
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("la relacio pare/filla inicial no existeix")
		}
		return confessionalWrap(q, "load_relation_approve_entitat_religiosa_initial_parent_tx", "entitat_religiosa_relacio", relationID, err)
	}
	if relation.ModeracioEstat != "pendent" {
		return fmt.Errorf("la relacio pare/filla inicial ja no esta pendent")
	}
	if relation.EntitatDestiID != entityID {
		return fmt.Errorf("la relacio dependent no apunta a la filla esperada")
	}

	parent, err := scanEntitat(tx.QueryRowContext(ctx, q.getEntitat, relation.EntitatOrigenID))
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("l'entitat pare no existeix")
		}
		return confessionalWrap(q, "load_parent_approve_entitat_religiosa_initial_parent_tx", "entitat_religiosa", relation.EntitatOrigenID, err)
	}
	if parent.ModeracioEstat != "publicat" {
		return fmt.Errorf("l'entitat pare ja no esta publicada")
	}

	if _, err := tx.ExecContext(ctx, `UPDATE entitat_religiosa_relacio SET moderation_status=$1, moderation_notes=$2, moderated_by=$3, moderated_at=NOW(), updated_at=NOW() WHERE id=$4`, "publicat", motiu, moderatorID, relationID); err != nil {
		return confessionalWrap(q, "approve_relation_entitat_religiosa_initial_parent_tx", "entitat_religiosa_relacio", relationID, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE entitat_religiosa SET moderation_status=$1, moderation_notes=$2, moderated_by=$3, moderated_at=NOW(), updated_at=NOW() WHERE id=$4`, "publicat", motiu, moderatorID, entityID); err != nil {
		return confessionalWrap(q, "approve_entity_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}

	entity, err = scanEntitat(tx.QueryRowContext(ctx, q.getEntitat, entityID))
	if err != nil {
		return confessionalWrap(q, "reload_entity_approve_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}

	var publishedWikiCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM wiki_canvis WHERE object_type = $1 AND object_id = $2 AND moderation_status = 'publicat'`, "entitat_religiosa", entityID).Scan(&publishedWikiCount); err != nil {
		return confessionalWrap(q, "count_published_wiki_entitat_religiosa_initial_parent_tx", "wiki_canvis", entityID, err)
	}
	if publishedWikiCount > 0 {
		return fmt.Errorf("la wiki inicial publicada ja existeix per a l'entitat religiosa %d", entityID)
	}

	metadata, err := buildConfessionalInitialWikiMetadata(entity)
	if err != nil {
		return confessionalWrap(q, "build_wiki_entitat_religiosa_initial_parent_tx", "wiki_canvis", entityID, err)
	}
	moderatedBy := sql.NullInt64{Int64: int64(moderatorID), Valid: moderatorID > 0}
	if _, err := tx.ExecContext(ctx, `INSERT INTO wiki_canvis (object_type, object_id, change_type, field_key, old_value, new_value, metadata, moderation_status, moderated_by, moderated_at, moderation_notes, changed_by, changed_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), $10, $11, NOW())`, "entitat_religiosa", entityID, "create", "*", "", "", metadata, "publicat", moderatedBy, "", entity.CreatedBy); err != nil {
		return confessionalWrap(q, "insert_wiki_entitat_religiosa_initial_parent_tx", "wiki_canvis", entityID, err)
	}

	if err := tx.Commit(); err != nil {
		return confessionalWrap(q, "commit_approve_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}
	return nil
}

func postgresRejectEntitatReligiosaWithInitialParentTx(d *PostgreSQL, entityID, relationID int, motiu string, moderatorID int) error {
	q := postgresConfessionalQueries()
	ctx := context.Background()
	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return confessionalWrap(q, "begin_reject_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}
	defer func() { _ = tx.Rollback() }()

	entity, err := scanEntitat(tx.QueryRowContext(ctx, q.getEntitat, entityID))
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("l'entitat religiosa no existeix")
		}
		return confessionalWrap(q, "load_entity_reject_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}
	if entity.ModeracioEstat != "pendent" {
		return fmt.Errorf("la filla ja no esta pendent")
	}

	relation, err := scanEntitatRelacio(tx.QueryRowContext(ctx, q.getEntitatRelacio, relationID))
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("la relacio pare/filla inicial no existeix")
		}
		return confessionalWrap(q, "load_relation_reject_entitat_religiosa_initial_parent_tx", "entitat_religiosa_relacio", relationID, err)
	}
	if relation.ModeracioEstat != "pendent" {
		return fmt.Errorf("la relacio pare/filla inicial ja no esta pendent")
	}
	if relation.EntitatDestiID != entityID {
		return fmt.Errorf("la relacio dependent no apunta a la filla esperada")
	}
	if _, err := scanEntitat(tx.QueryRowContext(ctx, q.getEntitat, relation.EntitatOrigenID)); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("l'entitat pare no existeix")
		}
		return confessionalWrap(q, "load_parent_reject_entitat_religiosa_initial_parent_tx", "entitat_religiosa", relation.EntitatOrigenID, err)
	}

	if _, err := tx.ExecContext(ctx, `UPDATE entitat_religiosa_relacio SET moderation_status=$1, moderation_notes=$2, moderated_by=$3, moderated_at=NOW(), updated_at=NOW() WHERE id=$4`, "rebutjat", motiu, moderatorID, relationID); err != nil {
		return confessionalWrap(q, "reject_relation_entitat_religiosa_initial_parent_tx", "entitat_religiosa_relacio", relationID, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE entitat_religiosa SET moderation_status=$1, moderation_notes=$2, moderated_by=$3, moderated_at=NOW(), updated_at=NOW() WHERE id=$4`, "rebutjat", motiu, moderatorID, entityID); err != nil {
		return confessionalWrap(q, "reject_entity_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}

	if err := tx.Commit(); err != nil {
		return confessionalWrap(q, "commit_reject_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}
	return nil
}

func mysqlApproveEntitatReligiosaWithInitialParentTx(d *MySQL, entityID, relationID int, motiu string, moderatorID int) error {
	q := mysqlConfessionalQueries()
	ctx := context.Background()
	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return confessionalWrap(q, "begin_approve_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}
	defer func() { _ = tx.Rollback() }()

	entity, err := scanEntitat(tx.QueryRowContext(ctx, q.getEntitat, entityID))
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("l'entitat religiosa no existeix")
		}
		return confessionalWrap(q, "load_entity_approve_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}
	if entity.ModeracioEstat != "pendent" {
		return fmt.Errorf("la filla ja no esta pendent")
	}

	relation, err := scanEntitatRelacio(tx.QueryRowContext(ctx, q.getEntitatRelacio, relationID))
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("la relacio pare/filla inicial no existeix")
		}
		return confessionalWrap(q, "load_relation_approve_entitat_religiosa_initial_parent_tx", "entitat_religiosa_relacio", relationID, err)
	}
	if relation.ModeracioEstat != "pendent" {
		return fmt.Errorf("la relacio pare/filla inicial ja no esta pendent")
	}
	if relation.EntitatDestiID != entityID {
		return fmt.Errorf("la relacio dependent no apunta a la filla esperada")
	}

	parent, err := scanEntitat(tx.QueryRowContext(ctx, q.getEntitat, relation.EntitatOrigenID))
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("l'entitat pare no existeix")
		}
		return confessionalWrap(q, "load_parent_approve_entitat_religiosa_initial_parent_tx", "entitat_religiosa", relation.EntitatOrigenID, err)
	}
	if parent.ModeracioEstat != "publicat" {
		return fmt.Errorf("l'entitat pare ja no esta publicada")
	}

	if _, err := tx.ExecContext(ctx, `UPDATE entitat_religiosa_relacio SET moderation_status=?, moderation_notes=?, moderated_by=?, moderated_at=NOW(), updated_at=NOW() WHERE id=?`, "publicat", motiu, moderatorID, relationID); err != nil {
		return confessionalWrap(q, "approve_relation_entitat_religiosa_initial_parent_tx", "entitat_religiosa_relacio", relationID, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE entitat_religiosa SET moderation_status=?, moderation_notes=?, moderated_by=?, moderated_at=NOW(), updated_at=NOW() WHERE id=?`, "publicat", motiu, moderatorID, entityID); err != nil {
		return confessionalWrap(q, "approve_entity_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}

	entity, err = scanEntitat(tx.QueryRowContext(ctx, q.getEntitat, entityID))
	if err != nil {
		return confessionalWrap(q, "reload_entity_approve_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}

	var publishedWikiCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM wiki_canvis WHERE object_type = ? AND object_id = ? AND moderation_status = 'publicat'`, "entitat_religiosa", entityID).Scan(&publishedWikiCount); err != nil {
		return confessionalWrap(q, "count_published_wiki_entitat_religiosa_initial_parent_tx", "wiki_canvis", entityID, err)
	}
	if publishedWikiCount > 0 {
		return fmt.Errorf("la wiki inicial publicada ja existeix per a l'entitat religiosa %d", entityID)
	}

	metadata, err := buildConfessionalInitialWikiMetadata(entity)
	if err != nil {
		return confessionalWrap(q, "build_wiki_entitat_religiosa_initial_parent_tx", "wiki_canvis", entityID, err)
	}
	moderatedBy := sql.NullInt64{Int64: int64(moderatorID), Valid: moderatorID > 0}
	if _, err := tx.ExecContext(ctx, `INSERT INTO wiki_canvis (object_type, object_id, change_type, field_key, old_value, new_value, metadata, moderation_status, moderated_by, moderated_at, moderation_notes, changed_by, changed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), ?, ?, NOW())`, "entitat_religiosa", entityID, "create", "*", "", "", metadata, "publicat", moderatedBy, "", entity.CreatedBy); err != nil {
		return confessionalWrap(q, "insert_wiki_entitat_religiosa_initial_parent_tx", "wiki_canvis", entityID, err)
	}

	if err := tx.Commit(); err != nil {
		return confessionalWrap(q, "commit_approve_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}
	return nil
}

func mysqlRejectEntitatReligiosaWithInitialParentTx(d *MySQL, entityID, relationID int, motiu string, moderatorID int) error {
	q := mysqlConfessionalQueries()
	ctx := context.Background()
	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return confessionalWrap(q, "begin_reject_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}
	defer func() { _ = tx.Rollback() }()

	entity, err := scanEntitat(tx.QueryRowContext(ctx, q.getEntitat, entityID))
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("l'entitat religiosa no existeix")
		}
		return confessionalWrap(q, "load_entity_reject_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}
	if entity.ModeracioEstat != "pendent" {
		return fmt.Errorf("la filla ja no esta pendent")
	}

	relation, err := scanEntitatRelacio(tx.QueryRowContext(ctx, q.getEntitatRelacio, relationID))
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("la relacio pare/filla inicial no existeix")
		}
		return confessionalWrap(q, "load_relation_reject_entitat_religiosa_initial_parent_tx", "entitat_religiosa_relacio", relationID, err)
	}
	if relation.ModeracioEstat != "pendent" {
		return fmt.Errorf("la relacio pare/filla inicial ja no esta pendent")
	}
	if relation.EntitatDestiID != entityID {
		return fmt.Errorf("la relacio dependent no apunta a la filla esperada")
	}
	if _, err := scanEntitat(tx.QueryRowContext(ctx, q.getEntitat, relation.EntitatOrigenID)); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("l'entitat pare no existeix")
		}
		return confessionalWrap(q, "load_parent_reject_entitat_religiosa_initial_parent_tx", "entitat_religiosa", relation.EntitatOrigenID, err)
	}

	if _, err := tx.ExecContext(ctx, `UPDATE entitat_religiosa_relacio SET moderation_status=?, moderation_notes=?, moderated_by=?, moderated_at=NOW(), updated_at=NOW() WHERE id=?`, "rebutjat", motiu, moderatorID, relationID); err != nil {
		return confessionalWrap(q, "reject_relation_entitat_religiosa_initial_parent_tx", "entitat_religiosa_relacio", relationID, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE entitat_religiosa SET moderation_status=?, moderation_notes=?, moderated_by=?, moderated_at=NOW(), updated_at=NOW() WHERE id=?`, "rebutjat", motiu, moderatorID, entityID); err != nil {
		return confessionalWrap(q, "reject_entity_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}

	if err := tx.Commit(); err != nil {
		return confessionalWrap(q, "commit_reject_entitat_religiosa_initial_parent_tx", "entitat_religiosa", entityID, err)
	}
	return nil
}
