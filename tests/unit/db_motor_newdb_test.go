package unit

import (
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestNewDB_UnknownEngine(t *testing.T) {
	cfg := map[string]string{
		"DB_ENGINE": "no-such-engine",
	}

	if _, err := db.NewDB(cfg); err == nil {
		t.Fatalf("esperava error per motor de BD desconegut")
	}
}
