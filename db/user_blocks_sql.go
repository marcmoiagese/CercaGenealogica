package db

import (
	"database/sql"
	"errors"
	"fmt"
)

func (h sqlHelper) addUserBlock(blockerID, blockedID int) error {
	if blockerID <= 0 || blockedID <= 0 {
		return fmt.Errorf("invalid user ids")
	}
	if blockerID == blockedID {
		return fmt.Errorf("same user")
	}
	stmt := `INSERT INTO user_blocks (blocker_id, blocked_id, created_at) VALUES (?, ?, ` + h.nowFun + `)`
	switch h.style {
	case "postgres":
		stmt += " ON CONFLICT (blocker_id, blocked_id) DO NOTHING"
	case "mysql":
		stmt = `INSERT IGNORE INTO user_blocks (blocker_id, blocked_id, created_at) VALUES (?, ?, ` + h.nowFun + `)`
	default:
		stmt = `INSERT OR IGNORE INTO user_blocks (blocker_id, blocked_id, created_at) VALUES (?, ?, ` + h.nowFun + `)`
	}
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, blockerID, blockedID)
	return err
}

func (h sqlHelper) removeUserBlock(blockerID, blockedID int) error {
	if blockerID <= 0 || blockedID <= 0 {
		return fmt.Errorf("invalid user ids")
	}
	stmt := formatPlaceholders(h.style, `DELETE FROM user_blocks WHERE blocker_id = ? AND blocked_id = ?`)
	_, err := h.db.Exec(stmt, blockerID, blockedID)
	return err
}

func (h sqlHelper) isUserBlocked(blockerID, blockedID int) (bool, error) {
	if blockerID <= 0 || blockedID <= 0 {
		return false, fmt.Errorf("invalid user ids")
	}
	stmt := formatPlaceholders(h.style, `SELECT 1 FROM user_blocks WHERE blocker_id = ? AND blocked_id = ? LIMIT 1`)
	var flag int
	if err := h.db.QueryRow(stmt, blockerID, blockedID).Scan(&flag); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
