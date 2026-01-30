package core

import "testing"

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
