package core

import (
	"strings"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestWikiObjectTypeWhitelist(t *testing.T) {
	valid := []string{"municipi", "arxiu", "llibre", "persona", "cognom"}
	for _, v := range valid {
		if !isValidWikiObjectType(v) {
			t.Fatalf("object_type %q hauria de ser valid", v)
		}
	}
	if isValidWikiObjectType("registre_raw") {
		t.Fatalf("object_type no permes ha de ser rebutjat")
	}
}

func TestBuildCountWikiPendingQueryFormatsPostgresPlaceholders(t *testing.T) {
	query, args := buildCountWikiPendingQuery(&db.PostgreSQL{}, "municipi", 308, 12)
	if len(args) != 3 {
		t.Fatalf("args esperats=3, got=%d", len(args))
	}
	if strings.Contains(query, "? AND") {
		t.Fatalf("la query postgres no ha de conservar '? AND': %s", query)
	}
	for _, invalid := range []string{"WHERE AND", "FROM wiki_canvis AND"} {
		if strings.Contains(query, invalid) {
			t.Fatalf("query postgres invalida: conte %q: %s", invalid, query)
		}
	}
	for _, want := range []string{"object_type = $1", "object_id = $2", "changed_by = $3"} {
		if !strings.Contains(query, want) {
			t.Fatalf("query postgres no conte %q: %s", want, query)
		}
	}
}

func TestBuildCountWikiPendingQueryKeepsSQLitePlaceholders(t *testing.T) {
	query, args := buildCountWikiPendingQuery(&db.SQLite{}, "municipi", 308, 0)
	if len(args) != 2 {
		t.Fatalf("args esperats=2, got=%d", len(args))
	}
	if !strings.Contains(query, "object_type = ? AND object_id = ?") {
		t.Fatalf("query sqlite ha de conservar placeholders '?': %s", query)
	}
	if strings.Contains(query, "changed_by") {
		t.Fatalf("query sense user_id no ha d'afegir filtre changed_by: %s", query)
	}
}
