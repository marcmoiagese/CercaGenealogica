package db

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

type dmThreadScanner interface {
	Scan(dest ...interface{}) error
}

func scanDMThread(scanner dmThreadScanner) (DMThread, error) {
	var t DMThread
	if err := scanner.Scan(
		&t.ID,
		&t.UserLowID,
		&t.UserHighID,
		&t.CreatedAt,
		&t.LastMessageAt,
		&t.LastMessageID,
	); err != nil {
		return t, err
	}
	return t, nil
}

func (h sqlHelper) ensureDMThreadState(tx *sql.Tx, threadID, userID int) error {
	if threadID <= 0 || userID <= 0 {
		return errors.New("invalid thread/user")
	}
	stmt := `INSERT INTO dm_thread_state (thread_id, user_id, archived, muted, deleted, updated_at)
        VALUES (?, ?, 0, 0, 0, ` + h.nowFun + `)`
	switch h.style {
	case "postgres":
		stmt += " ON CONFLICT (thread_id, user_id) DO NOTHING"
	case "mysql":
		stmt = `INSERT IGNORE INTO dm_thread_state (thread_id, user_id, archived, muted, deleted, updated_at)
            VALUES (?, ?, 0, 0, 0, ` + h.nowFun + `)`
	default:
		stmt = `INSERT OR IGNORE INTO dm_thread_state (thread_id, user_id, archived, muted, deleted, updated_at)
            VALUES (?, ?, 0, 0, 0, ` + h.nowFun + `)`
	}
	stmt = formatPlaceholders(h.style, stmt)
	_, err := tx.Exec(stmt, threadID, userID)
	return err
}

func (h sqlHelper) getOrCreateDMThread(userA, userB int) (*DMThread, error) {
	if userA <= 0 || userB <= 0 {
		return nil, fmt.Errorf("invalid user ids")
	}
	if userA == userB {
		return nil, fmt.Errorf("same user")
	}
	low := userA
	high := userB
	if low > high {
		low, high = high, low
	}
	tx, err := h.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	insertStmt := `INSERT INTO dm_threads (user_low_id, user_high_id, created_at)
        VALUES (?, ?, ` + h.nowFun + `)`
	switch h.style {
	case "postgres":
		insertStmt += " ON CONFLICT (user_low_id, user_high_id) DO NOTHING"
	case "mysql":
		insertStmt = `INSERT IGNORE INTO dm_threads (user_low_id, user_high_id, created_at)
            VALUES (?, ?, ` + h.nowFun + `)`
	default:
		insertStmt = `INSERT OR IGNORE INTO dm_threads (user_low_id, user_high_id, created_at)
            VALUES (?, ?, ` + h.nowFun + `)`
	}
	insertStmt = formatPlaceholders(h.style, insertStmt)
	if _, err := tx.Exec(insertStmt, low, high); err != nil {
		return nil, err
	}

	selectStmt := formatPlaceholders(h.style, `SELECT id, user_low_id, user_high_id, created_at, last_message_at, last_message_id
        FROM dm_threads WHERE user_low_id = ? AND user_high_id = ?`)
	thread, err := scanDMThread(tx.QueryRow(selectStmt, low, high))
	if err != nil {
		return nil, err
	}
	if err := h.ensureDMThreadState(tx, thread.ID, low); err != nil {
		return nil, err
	}
	if err := h.ensureDMThreadState(tx, thread.ID, high); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &thread, nil
}

func (h sqlHelper) getDMThreadByUsers(userA, userB int) (*DMThread, error) {
	if userA <= 0 || userB <= 0 {
		return nil, fmt.Errorf("invalid user ids")
	}
	if userA == userB {
		return nil, fmt.Errorf("same user")
	}
	low := userA
	high := userB
	if low > high {
		low, high = high, low
	}
	stmt := formatPlaceholders(h.style, `SELECT id, user_low_id, user_high_id, created_at, last_message_at, last_message_id
        FROM dm_threads WHERE user_low_id = ? AND user_high_id = ?`)
	thread, err := scanDMThread(h.db.QueryRow(stmt, low, high))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &thread, nil
}

func (h sqlHelper) getDMThreadByID(threadID int) (*DMThread, error) {
	if threadID <= 0 {
		return nil, fmt.Errorf("invalid thread id")
	}
	stmt := formatPlaceholders(h.style, `SELECT id, user_low_id, user_high_id, created_at, last_message_at, last_message_id
        FROM dm_threads WHERE id = ?`)
	thread, err := scanDMThread(h.db.QueryRow(stmt, threadID))
	if err != nil {
		return nil, err
	}
	return &thread, nil
}

func (h sqlHelper) listDMThreadsForUser(userID int, f DMThreadListFilter) ([]DMThreadListItem, error) {
	if userID <= 0 {
		return nil, fmt.Errorf("invalid user")
	}
	query := `
        SELECT t.id, t.user_low_id, t.user_high_id, t.created_at, t.last_message_at, t.last_message_id,
               s.last_read_message_id, s.archived, s.muted, s.deleted, s.updated_at, s.folder,
               m.sender_id, m.body, m.created_at
        FROM dm_threads t
        JOIN dm_thread_state s ON s.thread_id = t.id AND s.user_id = ?
        LEFT JOIN dm_messages m ON m.id = t.last_message_id`
	args := []interface{}{userID}
	clauses := []string{}
	if f.ThreadID > 0 {
		clauses = append(clauses, "t.id = ?")
		args = append(args, f.ThreadID)
	}
	if f.Archived != nil {
		clauses = append(clauses, "s.archived = ?")
		args = append(args, *f.Archived)
	}
	if f.Deleted != nil {
		clauses = append(clauses, "s.deleted = ?")
		args = append(args, *f.Deleted)
	} else {
		clauses = append(clauses, "s.deleted = ?")
		args = append(args, false)
	}
	if f.Folder != nil {
		if *f.Folder == "" {
			clauses = append(clauses, "(s.folder IS NULL OR s.folder = '')")
		} else {
			clauses = append(clauses, "s.folder = ?")
			args = append(args, *f.Folder)
		}
	}
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY t.last_message_at DESC, t.id DESC"
	if f.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, f.Limit)
		if f.Offset > 0 {
			query += " OFFSET ?"
			args = append(args, f.Offset)
		}
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := []DMThreadListItem{}
	for rows.Next() {
		var item DMThreadListItem
		var userLow int
		var userHigh int
		var archivedRaw interface{}
		var mutedRaw interface{}
		var deletedRaw interface{}
		var folderRaw sql.NullString
		var lastMessageBody sql.NullString
		var stateUpdatedAt sql.NullTime
		if err := rows.Scan(
			&item.ThreadID,
			&userLow,
			&userHigh,
			&item.ThreadCreatedAt,
			&item.LastMessageAt,
			&item.LastMessageID,
			&item.LastReadMessageID,
			&archivedRaw,
			&mutedRaw,
			&deletedRaw,
			&stateUpdatedAt,
			&folderRaw,
			&item.LastMessageSenderID,
			&lastMessageBody,
			&item.LastMessageCreatedAt,
		); err != nil {
			return nil, err
		}
		if userID == userLow {
			item.OtherUserID = userHigh
		} else {
			item.OtherUserID = userLow
		}
		item.Archived = parseBoolValue(archivedRaw)
		item.Muted = parseBoolValue(mutedRaw)
		item.Deleted = parseBoolValue(deletedRaw)
		if folderRaw.Valid {
			item.Folder = strings.TrimSpace(folderRaw.String)
		}
		if lastMessageBody.Valid {
			item.LastMessageBody = lastMessageBody.String
		}
		item.Unread = false
		if item.LastMessageID.Valid {
			if !item.LastReadMessageID.Valid || item.LastReadMessageID.Int64 < item.LastMessageID.Int64 {
				item.Unread = true
			}
		}
		res = append(res, item)
	}
	return res, nil
}

func (h sqlHelper) countDMUnread(userID int) (int, error) {
	if userID <= 0 {
		return 0, fmt.Errorf("invalid user")
	}
	query := `
        SELECT COUNT(1)
        FROM dm_threads t
        JOIN dm_thread_state s ON s.thread_id = t.id AND s.user_id = ?
        LEFT JOIN dm_messages m ON m.id = t.last_message_id
        WHERE s.deleted = ?
          AND s.archived = ?
          AND t.last_message_id IS NOT NULL
          AND (s.last_read_message_id IS NULL OR s.last_read_message_id < t.last_message_id)
          AND (m.sender_id IS NULL OR m.sender_id <> ?)`
	query = formatPlaceholders(h.style, query)
	var count int
	if err := h.db.QueryRow(query, userID, false, false, userID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (h sqlHelper) listDMThreadFolders(userID int) ([]string, error) {
	if userID <= 0 {
		return nil, fmt.Errorf("invalid user")
	}
	query := `
        SELECT DISTINCT folder
        FROM dm_thread_state
        WHERE user_id = ?
          AND deleted = ?
          AND folder IS NOT NULL
          AND folder <> ''
        ORDER BY folder ASC`
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, userID, false)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	folders := []string{}
	for rows.Next() {
		var folder sql.NullString
		if err := rows.Scan(&folder); err != nil {
			return nil, err
		}
		if folder.Valid {
			name := strings.TrimSpace(folder.String)
			if name != "" {
				folders = append(folders, name)
			}
		}
	}
	return folders, nil
}

func (h sqlHelper) setDMThreadFolder(threadID, userID int, folder string) error {
	if threadID <= 0 || userID <= 0 {
		return fmt.Errorf("invalid thread/user")
	}
	folder = strings.TrimSpace(folder)
	stmt := `UPDATE dm_thread_state SET folder = ?, updated_at = ` + h.nowFun + ` WHERE thread_id = ? AND user_id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, folder, threadID, userID)
	return err
}

func (h sqlHelper) listDMMessages(threadID, limit, beforeID int) ([]DMMessage, error) {
	if threadID <= 0 {
		return nil, fmt.Errorf("invalid thread")
	}
	query := `SELECT id, thread_id, sender_id, body, created_at FROM dm_messages WHERE thread_id = ?`
	args := []interface{}{threadID}
	if beforeID > 0 {
		query += " AND id < ?"
		args = append(args, beforeID)
	}
	query += " ORDER BY id DESC"
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	query = formatPlaceholders(h.style, query)
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []DMMessage
	for rows.Next() {
		var msg DMMessage
		if err := rows.Scan(&msg.ID, &msg.ThreadID, &msg.SenderID, &msg.Body, &msg.CreatedAt); err != nil {
			return nil, err
		}
		res = append(res, msg)
	}
	return res, nil
}

func (h sqlHelper) createDMMessage(threadID, senderID int, body string) (int, error) {
	if threadID <= 0 || senderID <= 0 {
		return 0, fmt.Errorf("invalid thread/sender")
	}
	stmt := `INSERT INTO dm_messages (thread_id, sender_id, body, created_at) VALUES (?, ?, ?, ` + h.nowFun + `)`
	if h.style == "postgres" {
		stmt += " RETURNING id"
	}
	stmt = formatPlaceholders(h.style, stmt)
	body = strings.TrimSpace(body)
	if h.style == "postgres" {
		var id int
		if err := h.db.QueryRow(stmt, threadID, senderID, body).Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}
	res, err := h.db.Exec(stmt, threadID, senderID, body)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil {
		return int(id), nil
	}
	return 0, nil
}

func (h sqlHelper) updateDMThreadLastMessage(threadID, msgID int, at time.Time) error {
	if threadID <= 0 {
		return fmt.Errorf("invalid thread")
	}
	if msgID <= 0 {
		return fmt.Errorf("invalid message")
	}
	if at.IsZero() {
		at = time.Now()
	}
	stmt := `UPDATE dm_threads SET last_message_id = ?, last_message_at = ? WHERE id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, msgID, at, threadID)
	return err
}

func (h sqlHelper) markDMThreadRead(threadID, userID, lastMsgID int) error {
	if threadID <= 0 || userID <= 0 {
		return fmt.Errorf("invalid thread/user")
	}
	if lastMsgID <= 0 {
		return fmt.Errorf("invalid message")
	}
	stmt := `UPDATE dm_thread_state
        SET last_read_message_id = CASE
                WHEN last_read_message_id IS NULL OR last_read_message_id < ? THEN ?
                ELSE last_read_message_id END,
            updated_at = ` + h.nowFun + `
        WHERE thread_id = ? AND user_id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, lastMsgID, lastMsgID, threadID, userID)
	return err
}

func (h sqlHelper) setDMThreadArchived(threadID, userID int, archived bool) error {
	if threadID <= 0 || userID <= 0 {
		return fmt.Errorf("invalid thread/user")
	}
	stmt := `UPDATE dm_thread_state SET archived = ?, updated_at = ` + h.nowFun + ` WHERE thread_id = ? AND user_id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, archived, threadID, userID)
	return err
}

func (h sqlHelper) softDeleteDMThread(threadID, userID int) error {
	if threadID <= 0 || userID <= 0 {
		return fmt.Errorf("invalid thread/user")
	}
	stmt := `UPDATE dm_thread_state SET deleted = 1, updated_at = ` + h.nowFun + ` WHERE thread_id = ? AND user_id = ?`
	stmt = formatPlaceholders(h.style, stmt)
	_, err := h.db.Exec(stmt, threadID, userID)
	return err
}
