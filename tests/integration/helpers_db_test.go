package integration

import (
	"strconv"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

// countActiveSessions torna quantes sessions NO revocades (revocat = 0)
// hi ha a la taula sessions. Es fa servir només en tests.
func countActiveSessions(t *testing.T, database db.DB) int {
	t.Helper()

	rows, err := database.Query("SELECT COUNT(*) AS n FROM sessions WHERE revocat = 0")
	if err != nil {
		t.Fatalf("no s'ha pogut comptar sessions actives: %v", err)
	}

	if len(rows) == 0 {
		return 0
	}

	raw := rows[0]["n"]
	switch v := raw.(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case float64:
		return int(v)
	case []byte:
		s := string(v)
		n, err := strconv.Atoi(s)
		if err != nil {
			t.Fatalf("no s'ha pogut convertir COUNT(*)=%q a int: %v", s, err)
		}
		return n
	case string:
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("no s'ha pogut convertir COUNT(*)=%q a int: %v", v, err)
		}
		return n
	default:
		t.Fatalf("tipus inesperat per COUNT(*): %T (%v)", raw, raw)
	}

	return 0
}
