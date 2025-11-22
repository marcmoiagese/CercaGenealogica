package unit

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// Test de connexió SQLite in-memory per validar que el driver funciona.
// Més endavant, es pot ampliar per aplicar l'esquema definit a SQLite.sql
// i provar consultes bàsiques.
func TestSQLiteInMemoryConnection(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("no s'ha pogut obrir sqlite in-memory: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatalf("no s'ha pogut fer ping a la BD sqlite in-memory: %v", err)
	}
}
