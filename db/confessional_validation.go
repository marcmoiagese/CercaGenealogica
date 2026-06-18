package db

import (
	"database/sql"
	"fmt"
)

// ValidateConfessionalEntityRelationHook lets the higher-level confessional
// module inject the canonical parent/child validation without an import cycle.
var ValidateConfessionalEntityRelationHook func(parent, child *EntitatReligiosa) error

type confessionalEntityLookup interface {
	QueryRow(query string, args ...interface{}) *sql.Row
}

func validateConfessionalEntityParent(queryer confessionalEntityLookup, q confessionalQueries, entity *EntitatReligiosa) error {
	if entity == nil || !entity.ParentID.Valid || entity.ParentID.Int64 <= 0 || ValidateConfessionalEntityRelationHook == nil {
		return nil
	}
	if entity.ID > 0 && entity.ParentID.Int64 == int64(entity.ID) {
		return fmt.Errorf("%w: entitat religiosa parent equals child", ErrInvalidReference)
	}
	parent, err := loadConfessionalEntityForValidation(queryer, q, int(entity.ParentID.Int64))
	if err != nil {
		return err
	}
	if parent == nil {
		return fmt.Errorf("%w: entitat religiosa parent missing", ErrInvalidReference)
	}
	return validateConfessionalEntityPair(parent, entity)
}

func validateConfessionalEntityRelationIDs(queryer confessionalEntityLookup, q confessionalQueries, parentID, childID int) error {
	if parentID <= 0 || childID <= 0 || ValidateConfessionalEntityRelationHook == nil {
		return nil
	}
	parent, err := loadConfessionalEntityForValidation(queryer, q, parentID)
	if err != nil {
		return err
	}
	if parent == nil {
		return fmt.Errorf("%w: entitat religiosa parent missing", ErrInvalidReference)
	}
	child, err := loadConfessionalEntityForValidation(queryer, q, childID)
	if err != nil {
		return err
	}
	if child == nil {
		return fmt.Errorf("%w: entitat religiosa child missing", ErrInvalidReference)
	}
	return validateConfessionalEntityPair(parent, child)
}

func validateConfessionalEntityPair(parent, child *EntitatReligiosa) error {
	if ValidateConfessionalEntityRelationHook == nil {
		return nil
	}
	if err := ValidateConfessionalEntityRelationHook(parent, child); err != nil {
		return fmt.Errorf("%w: %s", ErrInvalidReference, err.Error())
	}
	return nil
}

func loadConfessionalEntityForValidation(queryer confessionalEntityLookup, q confessionalQueries, id int) (*EntitatReligiosa, error) {
	item, err := scanEntitat(queryer.QueryRow(q.getEntitat, id))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, confessionalWrap(q, "get_entitat_religiosa_validation", "entitat_religiosa", id, err)
	}
	return item, nil
}
