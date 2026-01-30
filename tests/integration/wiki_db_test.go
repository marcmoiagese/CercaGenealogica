package integration

import (
	"database/sql"
	"strconv"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestWikiDBChangesAndMarks(t *testing.T) {
	_, database := newTestAppForLogin(t, "test_wiki_db.sqlite3")

	user := createTestUser(t, database, "wiki_db_user_"+strconv.FormatInt(time.Now().UnixNano(), 10))

	change := &db.WikiChange{
		ObjectType:     "persona",
		ObjectID:       42,
		ChangeType:     "form",
		FieldKey:       "nom",
		OldValue:       "A",
		NewValue:       "B",
		ModeracioEstat: "pendent",
		ChangedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
	}
	changeID, err := database.CreateWikiChange(change)
	if err != nil || changeID == 0 {
		t.Fatalf("CreateWikiChange ha fallat: %v", err)
	}

	changes, err := database.ListWikiChanges("persona", 42)
	if err != nil {
		t.Fatalf("ListWikiChanges ha fallat: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("esperava 1 canvi, got %d", len(changes))
	}
	if changes[0].ModeracioEstat != "pendent" {
		t.Fatalf("estat esperat pendent, got %s", changes[0].ModeracioEstat)
	}

	pending, err := database.ListWikiPending(0)
	if err != nil {
		t.Fatalf("ListWikiPending ha fallat: %v", err)
	}
	if !wikiPendingContains(pending, changeID) {
		t.Fatalf("change %d no apareix a la cua de pendents", changeID)
	}

	if err := database.UpdateWikiChangeModeracio(changeID, "publicat", "", user.ID); err != nil {
		t.Fatalf("UpdateWikiChangeModeracio ha fallat: %v", err)
	}
	updated, err := database.GetWikiChange(changeID)
	if err != nil || updated == nil {
		t.Fatalf("GetWikiChange ha fallat: %v", err)
	}
	if updated.ModeracioEstat != "publicat" {
		t.Fatalf("estat esperat publicat, got %s", updated.ModeracioEstat)
	}
	if !updated.ModeratedBy.Valid || int(updated.ModeratedBy.Int64) != user.ID {
		t.Fatalf("moderated_by inesperat: %+v", updated.ModeratedBy)
	}

	pending, err = database.ListWikiPending(0)
	if err != nil {
		t.Fatalf("ListWikiPending ha fallat: %v", err)
	}
	if wikiPendingContains(pending, changeID) {
		t.Fatalf("change %d encara és a la cua de pendents", changeID)
	}

	change2 := &db.WikiChange{
		ObjectType:     "persona",
		ObjectID:       42,
		ChangeType:     "form",
		FieldKey:       "nom",
		OldValue:       "B",
		NewValue:       "C",
		ModeracioEstat: "pendent",
		ChangedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
	}
	changeID2, err := database.CreateWikiChange(change2)
	if err != nil || changeID2 == 0 {
		t.Fatalf("CreateWikiChange segona ha fallat: %v", err)
	}
	if err := database.UpdateWikiChangeModeracio(changeID2, "rebutjat", "", user.ID); err != nil {
		t.Fatalf("UpdateWikiChangeModeracio rebutjar ha fallat: %v", err)
	}
	pending, err = database.ListWikiPending(0)
	if err != nil {
		t.Fatalf("ListWikiPending ha fallat: %v", err)
	}
	if wikiPendingContains(pending, changeID2) {
		t.Fatalf("change rebutjat %d encara és a la cua", changeID2)
	}

	mark := db.WikiMark{
		ObjectType: "persona",
		ObjectID:   42,
		UserID:     user.ID,
		Tipus:      "interes",
		IsPublic:   true,
	}
	if err := database.UpsertWikiMark(&mark); err != nil {
		t.Fatalf("UpsertWikiMark ha fallat: %v", err)
	}
	if err := database.UpsertWikiMark(&mark); err != nil {
		t.Fatalf("UpsertWikiMark idempotent ha fallat: %v", err)
	}
	count := countRows(t, database, "SELECT COUNT(*) AS n FROM wiki_marques WHERE object_type = ? AND object_id = ? AND user_id = ?", "persona", 42, user.ID)
	if count != 1 {
		t.Fatalf("esperava 1 marca, got %d", count)
	}
	if err := database.DeleteWikiMark("persona", 42, user.ID); err != nil {
		t.Fatalf("DeleteWikiMark ha fallat: %v", err)
	}
	if err := database.DeleteWikiMark("persona", 42, user.ID); err != nil {
		t.Fatalf("DeleteWikiMark idempotent ha fallat: %v", err)
	}
	count = countRows(t, database, "SELECT COUNT(*) AS n FROM wiki_marques WHERE object_type = ? AND object_id = ? AND user_id = ?", "persona", 42, user.ID)
	if count != 0 {
		t.Fatalf("esperava 0 marques, got %d", count)
	}
}

func TestWikiPendingQueueOrder(t *testing.T) {
	_, database := newTestAppForLogin(t, "test_wiki_pending_order.sqlite3")

	user := createTestUser(t, database, "wiki_pending_user_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	change1 := &db.WikiChange{
		ObjectType:     "cognom",
		ObjectID:       11,
		ChangeType:     "form",
		FieldKey:       "origen",
		OldValue:       "",
		NewValue:       "X",
		ModeracioEstat: "pendent",
		ChangedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
	}
	id1, err := database.CreateWikiChange(change1)
	if err != nil {
		t.Fatalf("CreateWikiChange 1 ha fallat: %v", err)
	}

	change2 := &db.WikiChange{
		ObjectType:     "cognom",
		ObjectID:       11,
		ChangeType:     "form",
		FieldKey:       "notes",
		OldValue:       "",
		NewValue:       "Y",
		ModeracioEstat: "pendent",
		ChangedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
	}
	id2, err := database.CreateWikiChange(change2)
	if err != nil {
		t.Fatalf("CreateWikiChange 2 ha fallat: %v", err)
	}

	pending, err := database.ListWikiPending(0)
	if err != nil {
		t.Fatalf("ListWikiPending ha fallat: %v", err)
	}
	if len(pending) < 2 {
		t.Fatalf("esperava 2 pendents, got %d", len(pending))
	}
	if pending[0].ChangeID != id2 {
		t.Fatalf("ordre pendent incorrecte, esperava %d primer i he rebut %d", id2, pending[0].ChangeID)
	}
	if pending[1].ChangeID != id1 {
		t.Fatalf("ordre pendent incorrecte, esperava %d segon i he rebut %d", id1, pending[1].ChangeID)
	}
}

func TestWikiPendingBulkList(t *testing.T) {
	_, database := newTestAppForLogin(t, "test_wiki_pending_bulk.sqlite3")

	user := createTestUser(t, database, "wiki_bulk_user_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	for i := 0; i < 25; i++ {
		change := &db.WikiChange{
			ObjectType:     "persona",
			ObjectID:       77,
			ChangeType:     "form",
			FieldKey:       "nom",
			OldValue:       "A",
			NewValue:       "B",
			ModeracioEstat: "pendent",
			ChangedBy:      sql.NullInt64{Int64: int64(user.ID), Valid: true},
		}
		if _, err := database.CreateWikiChange(change); err != nil {
			t.Fatalf("CreateWikiChange bulk ha fallat: %v", err)
		}
	}

	changes, err := database.ListWikiChanges("persona", 77)
	if err != nil {
		t.Fatalf("ListWikiChanges bulk ha fallat: %v", err)
	}
	if len(changes) != 25 {
		t.Fatalf("esperava 25 canvis, got %d", len(changes))
	}

	pending, err := database.ListWikiPending(0)
	if err != nil {
		t.Fatalf("ListWikiPending bulk ha fallat: %v", err)
	}
	if len(pending) < 25 {
		t.Fatalf("esperava >=25 pendents, got %d", len(pending))
	}
}

func wikiPendingContains(items []db.WikiPendingItem, changeID int) bool {
	for _, item := range items {
		if item.ChangeID == changeID {
			return true
		}
	}
	return false
}
