package core

import (
	"strings"
	"testing"
)

func TestModeracioBulkAllowedTypesHavePermKey(t *testing.T) {
	for _, objType := range moderacioBulkAllowedTypes {
		spec, ok := moderacioTypeSpecs[objType]
		if !ok {
			t.Fatalf("moderacioTypeSpecs missing for %s", objType)
		}
		if strings.TrimSpace(spec.PermKey) == "" {
			t.Fatalf("moderacio type %s missing PermKey", objType)
		}
		if strings.TrimSpace(string(spec.ListScope)) == "" {
			t.Fatalf("moderacio type %s missing ListScope", objType)
		}
	}
}
