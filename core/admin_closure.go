package core

import (
	"strconv"
	"strings"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

func (a *App) rebuildAdminClosureForMunicipi(mun *db.Municipi) {
	if mun == nil || mun.ID <= 0 {
		return
	}
	entries := a.buildAdminClosureEntries(mun)
	if err := a.DB.ReplaceAdminClosure(mun.ID, entries); err != nil {
		Errorf("Error rebuild admin_closure municipi %d: %v", mun.ID, err)
	}
}

func (a *App) rebuildAdminClosureAll() error {
	muns, err := a.DB.ListMunicipis(db.MunicipiFilter{})
	if err != nil {
		return err
	}
	for i := range muns {
		mun, err := a.DB.GetMunicipi(muns[i].ID)
		if err != nil || mun == nil {
			continue
		}
		a.rebuildAdminClosureForMunicipi(mun)
	}
	return nil
}

func (a *App) buildAdminClosureEntries(mun *db.Municipi) []db.AdminClosureEntry {
	entries := []db.AdminClosureEntry{}
	if mun == nil || mun.ID <= 0 {
		return entries
	}
	seen := map[string]struct{}{}
	add := func(typ string, id int) {
		if id <= 0 || strings.TrimSpace(typ) == "" {
			return
		}
		key := typ + ":" + strconv.Itoa(id)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		entries = append(entries, db.AdminClosureEntry{
			DescendantMunicipiID: mun.ID,
			AncestorType:         typ,
			AncestorID:           id,
		})
	}
	add("municipi", mun.ID)
	paisID := 0
	for _, level := range mun.NivellAdministratiuID {
		if !level.Valid || level.Int64 <= 0 {
			continue
		}
		lvlID := int(level.Int64)
		add("nivell", lvlID)
		if paisID == 0 {
			if lvl, err := a.DB.GetNivell(lvlID); err == nil && lvl != nil && lvl.PaisID > 0 {
				paisID = lvl.PaisID
			}
		}
	}
	if paisID > 0 {
		add("pais", paisID)
	}
	return entries
}
