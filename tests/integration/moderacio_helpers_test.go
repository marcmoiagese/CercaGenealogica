package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func createNonAdminTestUser(t *testing.T, database db.DB, username string) *db.User {
	t.Helper()
	_ = createTestUser(t, database, fmt.Sprintf("seed_admin_%d", time.Now().UnixNano()))
	return createTestUser(t, database, username)
}
