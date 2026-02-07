package core

import (
	"sort"
	"strings"
)

func (a *App) listNivellAncestorsForMunicipi(munID int) []int {
	if munID <= 0 {
		return nil
	}
	entries, err := a.DB.ListAdminClosure(munID)
	if err != nil {
		Errorf("Error carregant admin_closure municipi %d: %v", munID, err)
		return nil
	}
	seen := map[int]struct{}{}
	ids := make([]int, 0, len(entries))
	for _, entry := range entries {
		if strings.TrimSpace(entry.AncestorType) != "nivell" {
			continue
		}
		if entry.AncestorID <= 0 {
			continue
		}
		if _, ok := seen[entry.AncestorID]; ok {
			continue
		}
		seen[entry.AncestorID] = struct{}{}
		ids = append(ids, entry.AncestorID)
	}
	sort.Ints(ids)
	return ids
}

func (a *App) applyNivellDemografiaDeltaForMunicipi(munID, year int, tipus string, delta int) {
	if munID <= 0 || year <= 0 || delta == 0 {
		return
	}
	nivells := a.listNivellAncestorsForMunicipi(munID)
	for _, nivellID := range nivells {
		if err := a.DB.ApplyNivellDemografiaDelta(nivellID, year, tipus, delta); err != nil {
			Errorf("Error actualitzant demografia nivell %d (mun=%d): %v", nivellID, munID, err)
		}
	}
}
