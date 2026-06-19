package core

import (
	"database/sql"
	"testing"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func TestConfessionalResolveMunicipalityRefDedupesSameCandidateID(t *testing.T) {
	ref := confessionalMunicipalityRef{Name: "Municipi dedupe", Type: "municipi"}
	normalized := confessionalNormalizeMunicipalityRef(ref)
	key := confessionalMunicipalityRefKey(normalized)
	nameTypeCountryKey := confessionalMunicipalityNameTypeCountryKey(normalized)

	lookup := confessionalMunicipalityLookup{
		All: map[int]*db.Municipi{
			7: {
				ID:             7,
				Nom:            "Municipi dedupe",
				Tipus:          "municipi",
				MunicipiID:     sql.NullInt64{},
				ModeracioEstat: "publicat",
			},
		},
		Exact:             map[string][]int{key: {7, 7}},
		ByNameTypeCountry: map[string][]int{nameTypeCountryKey: {7, 7}},
		ByNameType:        map[string][]int{},
		ByName:            map[string][]int{},
	}

	id, err := confessionalResolveMunicipalityRef(ref, lookup, confessionalRefContext{})
	if err != nil {
		t.Fatalf("confessionalResolveMunicipalityRef() error inesperat: %v", err)
	}
	if id != 7 {
		t.Fatalf("confessionalResolveMunicipalityRef() = %d, esperava 7", id)
	}
}

func TestConfessionalUniqueCandidateIDsPreservesOrder(t *testing.T) {
	got := confessionalUniqueCandidateIDs([]int{4, 4, 2, 4, 2, 9})
	want := []int{4, 2, 9}
	if len(got) != len(want) {
		t.Fatalf("len(confessionalUniqueCandidateIDs()) = %d, esperava %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("confessionalUniqueCandidateIDs()[%d] = %d, esperava %d (%v)", i, got[i], want[i], got)
		}
	}
}
