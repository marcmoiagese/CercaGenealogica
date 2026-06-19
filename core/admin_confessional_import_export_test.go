package core

import (
	"database/sql"
	"strings"
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

func TestConfessionalUniqueCandidateIDsReturnsNilForEmptyInput(t *testing.T) {
	if got := confessionalUniqueCandidateIDs(nil); got != nil {
		t.Fatalf("confessionalUniqueCandidateIDs(nil) = %v, esperava nil", got)
	}
	if got := confessionalUniqueCandidateIDs([]int{}); got != nil {
		t.Fatalf("confessionalUniqueCandidateIDs([]) = %v, esperava nil", got)
	}
}

func TestConfessionalUniqueCandidateIDsReturnsOriginalSliceWithoutDuplicates(t *testing.T) {
	ids := []int{4, 2, 9}
	got := confessionalUniqueCandidateIDs(ids)
	if len(got) != len(ids) {
		t.Fatalf("len(confessionalUniqueCandidateIDs()) = %d, esperava %d", len(got), len(ids))
	}
	if &got[0] != &ids[0] {
		t.Fatalf("confessionalUniqueCandidateIDs() ha de reutilitzar el slice original quan no hi ha duplicats")
	}
}

func TestConfessionalParseMunicipalityAltresExtractsContext(t *testing.T) {
	meta := confessionalParseMunicipalityAltres("idescat_codi=430385 03; municipi_idescat=430385; categoria=capital; comarca=Baix Camp; provincia=Tarragona")
	if meta.IdescatCodi != "430385 03" || meta.MunicipiIdescat != "430385" {
		t.Fatalf("confessionalParseMunicipalityAltres() ha de conservar codis Idescat, got %+v", meta)
	}
	if meta.Categoria != "capital" || meta.Comarca != "Baix Camp" || meta.Provincia != "Tarragona" {
		t.Fatalf("confessionalParseMunicipalityAltres() ha d'extreure categoria/comarca/provincia, got %+v", meta)
	}
}

func TestConfessionalMunicipalityCandidateLabelIncludesTerritorialContext(t *testing.T) {
	lookup := confessionalMunicipalityLookup{
		All: map[int]*db.Municipi{
			7: {
				ID:             7,
				Nom:            "Cambrils",
				Tipus:          "poble",
				Altres:         "idescat_codi=430385 03; municipi_idescat=430385; comarca=Baix Camp; provincia=Tarragona",
				ModeracioEstat: "publicat",
			},
		},
		LevelISO: map[int]string{},
		Levels:   map[int]db.NivellAdministratiu{},
	}
	got := confessionalMunicipalityCandidateLabel(lookup.All[7], lookup)
	for _, token := range []string{"municipi_idescat=430385", "comarca=Baix Camp", "provincia=Tarragona"} {
		if !strings.Contains(got, token) {
			t.Fatalf("confessionalMunicipalityCandidateLabel() ha d'incloure %q, got %q", token, got)
		}
	}
}

func TestConfessionalMunicipalityPathLabelStopsOnCycle(t *testing.T) {
	all := map[int]*db.Municipi{
		1: {
			ID:         1,
			Nom:        "Municipi A",
			MunicipiID: sql.NullInt64{Int64: 2, Valid: true},
		},
		2: {
			ID:         2,
			Nom:        "Municipi B",
			MunicipiID: sql.NullInt64{Int64: 1, Valid: true},
		},
	}

	got := confessionalMunicipalityPathLabel(all[1], all)
	if got == "" {
		t.Fatalf("confessionalMunicipalityPathLabel() no hauria de quedar buit amb un cicle curt")
	}
	if got != "Municipi B > Municipi A" {
		t.Fatalf("confessionalMunicipalityPathLabel() = %q, esperava un path truncat segur", got)
	}
}
