package db

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateConfessionalEntityPairPreservesWrappedErrors(t *testing.T) {
	previousHook := ValidateConfessionalEntityRelationHook
	defer func() {
		ValidateConfessionalEntityRelationHook = previousHook
	}()

	var errTestConfessionalRelation = errors.New("test confessional relation invalid")
	ValidateConfessionalEntityRelationHook = func(parent, child *EntitatReligiosa) error {
		return errTestConfessionalRelation
	}

	parent := &EntitatReligiosa{ID: 1, Nom: "Parent"}
	child := &EntitatReligiosa{ID: 2, Nom: "Child"}

	err := validateConfessionalEntityPair(parent, child)
	if err == nil {
		t.Fatal("validateConfessionalEntityPair returned nil, want wrapped error")
	}
	if !errors.Is(err, ErrInvalidReference) {
		t.Fatalf("errors.Is(err, ErrInvalidReference)=false; err=%v", err)
	}
	if !errors.Is(err, errTestConfessionalRelation) {
		t.Fatalf("errors.Is(err, errTestConfessionalRelation)=false; err=%v", err)
	}
	if msg := err.Error(); !strings.Contains(msg, "invalid reference") || !strings.Contains(msg, errTestConfessionalRelation.Error()) {
		t.Fatalf("wrapped error message lacks context: %q", msg)
	}
}
