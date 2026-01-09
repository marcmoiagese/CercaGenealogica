package core

import (
	"sync"
	"time"
)

const (
	targetCacheTTL         = 10 * time.Minute
	llibreTargetCacheMax   = 10000
	arxiuTargetCacheMax    = 5000
	municipiTargetCacheMax = 5000
)

type targetCacheEntry struct {
	target    PermissionTarget
	expiresAt time.Time
}

type targetCache struct {
	mu         sync.RWMutex
	entries    map[int]targetCacheEntry
	ttl        time.Duration
	maxEntries int
}

func newTargetCache(ttl time.Duration, maxEntries int) *targetCache {
	return &targetCache{entries: make(map[int]targetCacheEntry), ttl: ttl, maxEntries: maxEntries}
}

func (c *targetCache) get(id int) (PermissionTarget, bool) {
	if c == nil {
		return PermissionTarget{}, false
	}
	c.mu.RLock()
	entry, ok := c.entries[id]
	c.mu.RUnlock()
	if !ok {
		return PermissionTarget{}, false
	}
	if time.Now().After(entry.expiresAt) {
		c.mu.Lock()
		delete(c.entries, id)
		c.mu.Unlock()
		return PermissionTarget{}, false
	}
	return clonePermissionTarget(entry.target), true
}

func (c *targetCache) set(id int, target PermissionTarget) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.maxEntries > 0 && len(c.entries) >= c.maxEntries {
		for key := range c.entries {
			delete(c.entries, key)
			break
		}
	}
	c.entries[id] = targetCacheEntry{
		target:    clonePermissionTarget(target),
		expiresAt: time.Now().Add(c.ttl),
	}
	c.mu.Unlock()
}

func clonePermissionTarget(t PermissionTarget) PermissionTarget {
	clone := t
	if t.ArxiuIDs != nil {
		clone.ArxiuIDs = append([]int(nil), t.ArxiuIDs...)
	}
	return clone
}

func intPtr(val int) *int {
	if val == 0 {
		return nil
	}
	v := val
	return &v
}

func dedupeIntSlice(values []int) []int {
	if len(values) < 2 {
		return values
	}
	seen := make(map[int]struct{}, len(values))
	out := make([]int, 0, len(values))
	for _, v := range values {
		if v <= 0 {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func (a *App) ensureTargetCaches() {
	if a == nil {
		return
	}
	if a.llibreTargetCache == nil {
		a.llibreTargetCache = newTargetCache(targetCacheTTL, llibreTargetCacheMax)
	}
	if a.arxiuTargetCache == nil {
		a.arxiuTargetCache = newTargetCache(targetCacheTTL, arxiuTargetCacheMax)
	}
	if a.municipiTargetCache == nil {
		a.municipiTargetCache = newTargetCache(targetCacheTTL, municipiTargetCacheMax)
	}
}

func (a *App) loadLlibreTargetFast(llibreID int) (PermissionTarget, bool) {
	if a == nil || a.DB == nil || llibreID <= 0 {
		return PermissionTarget{}, false
	}
	query := `
        SELECT l.municipi_id AS municipi_id,
               l.arquevisbat_id AS ecles_id,
               m.nivell_administratiu_id_3 AS provincia_id,
               m.nivell_administratiu_id_4 AS comarca_id,
               na1.pais_id AS pais_id,
               al.arxiu_id AS arxiu_id
        FROM llibres l
        LEFT JOIN municipis m ON m.id = l.municipi_id
        LEFT JOIN nivells_administratius na1 ON na1.id = m.nivell_administratiu_id_1
        LEFT JOIN arxius_llibres al ON al.llibre_id = l.id
        WHERE l.id = ?`
	query = formatSQLForDB(a.DB, query)
	rows, err := a.DB.Query(query, llibreID)
	if err != nil || len(rows) == 0 {
		return PermissionTarget{}, false
	}
	target := PermissionTarget{LlibreID: intPtr(llibreID)}
	for _, row := range rows {
		if target.MunicipiID == nil {
			if v := rowInt(row, "municipi_id"); v > 0 {
				target.MunicipiID = intPtr(v)
			}
		}
		if target.ProvinciaID == nil {
			if v := rowInt(row, "provincia_id"); v > 0 {
				target.ProvinciaID = intPtr(v)
			}
		}
		if target.ComarcaID == nil {
			if v := rowInt(row, "comarca_id"); v > 0 {
				target.ComarcaID = intPtr(v)
			}
		}
		if target.PaisID == nil {
			if v := rowInt(row, "pais_id"); v > 0 {
				target.PaisID = intPtr(v)
			}
		}
		if target.EclesID == nil {
			if v := rowInt(row, "ecles_id"); v > 0 {
				target.EclesID = intPtr(v)
			}
		}
		if v := rowInt(row, "arxiu_id"); v > 0 {
			target.ArxiuIDs = append(target.ArxiuIDs, v)
		}
	}
	target.ArxiuIDs = dedupeIntSlice(target.ArxiuIDs)
	if len(target.ArxiuIDs) == 1 {
		target.ArxiuID = intPtr(target.ArxiuIDs[0])
	}
	return target, true
}

func (a *App) loadArxiuTargetFast(arxiuID int) (PermissionTarget, bool) {
	if a == nil || a.DB == nil || arxiuID <= 0 {
		return PermissionTarget{}, false
	}
	query := `
        SELECT a.municipi_id AS municipi_id,
               a.entitat_eclesiastica_id AS ecles_id,
               m.nivell_administratiu_id_3 AS provincia_id,
               m.nivell_administratiu_id_4 AS comarca_id,
               na1.pais_id AS pais_id
        FROM arxius a
        LEFT JOIN municipis m ON m.id = a.municipi_id
        LEFT JOIN nivells_administratius na1 ON na1.id = m.nivell_administratiu_id_1
        WHERE a.id = ?`
	query = formatSQLForDB(a.DB, query)
	rows, err := a.DB.Query(query, arxiuID)
	if err != nil || len(rows) == 0 {
		return PermissionTarget{}, false
	}
	target := PermissionTarget{ArxiuID: intPtr(arxiuID), ArxiuIDs: []int{arxiuID}}
	row := rows[0]
	if v := rowInt(row, "municipi_id"); v > 0 {
		target.MunicipiID = intPtr(v)
	}
	if v := rowInt(row, "provincia_id"); v > 0 {
		target.ProvinciaID = intPtr(v)
	}
	if v := rowInt(row, "comarca_id"); v > 0 {
		target.ComarcaID = intPtr(v)
	}
	if v := rowInt(row, "pais_id"); v > 0 {
		target.PaisID = intPtr(v)
	}
	if v := rowInt(row, "ecles_id"); v > 0 {
		target.EclesID = intPtr(v)
	}
	return target, true
}

func (a *App) resolveLlibreTarget(llibreID int) PermissionTarget {
	if llibreID <= 0 {
		return PermissionTarget{}
	}
	start := time.Now()
	a.ensureTargetCaches()
	if t, ok := a.llibreTargetCache.get(llibreID); ok {
		Debugf("permisos target cache hit llibre=%d in %s", llibreID, time.Since(start))
		return t
	}
	target := PermissionTarget{LlibreID: intPtr(llibreID)}
	if a == nil || a.DB == nil {
		return target
	}
	if fastTarget, ok := a.loadLlibreTargetFast(llibreID); ok {
		target = fastTarget
	} else {
		if llibre, err := a.DB.GetLlibre(llibreID); err == nil && llibre != nil {
			if llibre.MunicipiID > 0 {
				target.MunicipiID = intPtr(llibre.MunicipiID)
				a.fillTerritoryFromMunicipi(&target, llibre.MunicipiID)
			}
			if llibre.ArquebisbatID > 0 {
				target.EclesID = intPtr(llibre.ArquebisbatID)
			}
		}
		if rels, err := a.DB.ListLlibreArxius(llibreID); err == nil {
			for _, rel := range rels {
				if rel.ArxiuID > 0 {
					target.ArxiuIDs = append(target.ArxiuIDs, rel.ArxiuID)
				}
			}
		}
		target.ArxiuIDs = dedupeIntSlice(target.ArxiuIDs)
		if len(target.ArxiuIDs) == 1 {
			target.ArxiuID = intPtr(target.ArxiuIDs[0])
		}
	}
	if target.MunicipiID == nil && len(target.ArxiuIDs) > 0 {
		arxiuTarget := a.resolveArxiuTarget(target.ArxiuIDs[0])
		if target.MunicipiID == nil {
			target.MunicipiID = arxiuTarget.MunicipiID
		}
		if target.PaisID == nil {
			target.PaisID = arxiuTarget.PaisID
		}
		if target.ProvinciaID == nil {
			target.ProvinciaID = arxiuTarget.ProvinciaID
		}
		if target.ComarcaID == nil {
			target.ComarcaID = arxiuTarget.ComarcaID
		}
		if target.EclesID == nil {
			target.EclesID = arxiuTarget.EclesID
		}
		if target.ArxiuID == nil && arxiuTarget.ArxiuID != nil {
			target.ArxiuID = arxiuTarget.ArxiuID
		}
	}
	a.llibreTargetCache.set(llibreID, target)
	Debugf("permisos target cache miss llibre=%d in %s", llibreID, time.Since(start))
	return target
}

func (a *App) resolveArxiuTarget(arxiuID int) PermissionTarget {
	if arxiuID <= 0 {
		return PermissionTarget{}
	}
	start := time.Now()
	a.ensureTargetCaches()
	if t, ok := a.arxiuTargetCache.get(arxiuID); ok {
		Debugf("permisos target cache hit arxiu=%d in %s", arxiuID, time.Since(start))
		return t
	}
	target := PermissionTarget{ArxiuID: intPtr(arxiuID), ArxiuIDs: []int{arxiuID}}
	if a == nil || a.DB == nil {
		return target
	}
	if fastTarget, ok := a.loadArxiuTargetFast(arxiuID); ok {
		target = fastTarget
	} else {
		arxiu, err := a.DB.GetArxiu(arxiuID)
		if err == nil && arxiu != nil {
			if arxiu.MunicipiID.Valid {
				munID := int(arxiu.MunicipiID.Int64)
				target.MunicipiID = intPtr(munID)
				a.fillTerritoryFromMunicipi(&target, munID)
			}
			if arxiu.EntitatEclesiasticaID.Valid {
				target.EclesID = intPtr(int(arxiu.EntitatEclesiasticaID.Int64))
			}
		}
	}
	a.arxiuTargetCache.set(arxiuID, target)
	Debugf("permisos target cache miss arxiu=%d in %s", arxiuID, time.Since(start))
	return target
}

func (a *App) resolveMunicipiTarget(municipiID int) PermissionTarget {
	if municipiID <= 0 {
		return PermissionTarget{}
	}
	start := time.Now()
	a.ensureTargetCaches()
	if t, ok := a.municipiTargetCache.get(municipiID); ok {
		Debugf("permisos target cache hit municipi=%d in %s", municipiID, time.Since(start))
		return t
	}
	target := PermissionTarget{MunicipiID: intPtr(municipiID)}
	a.fillTerritoryFromMunicipi(&target, municipiID)
	a.municipiTargetCache.set(municipiID, target)
	Debugf("permisos target cache miss municipi=%d in %s", municipiID, time.Since(start))
	return target
}

func (a *App) fillTerritoryFromMunicipi(target *PermissionTarget, municipiID int) {
	if target == nil || municipiID <= 0 || a == nil || a.DB == nil {
		return
	}
	mun, err := a.DB.GetMunicipi(municipiID)
	if err != nil || mun == nil {
		return
	}
	if target.MunicipiID == nil {
		target.MunicipiID = intPtr(municipiID)
	}
	if target.ProvinciaID == nil && mun.NivellAdministratiuID[2].Valid {
		target.ProvinciaID = intPtr(int(mun.NivellAdministratiuID[2].Int64))
	}
	if target.ComarcaID == nil && mun.NivellAdministratiuID[3].Valid {
		target.ComarcaID = intPtr(int(mun.NivellAdministratiuID[3].Int64))
	}
	if target.PaisID == nil {
		nivellID := 0
		for _, val := range mun.NivellAdministratiuID {
			if val.Valid {
				nivellID = int(val.Int64)
				break
			}
		}
		if nivellID > 0 {
			if nivell, err := a.DB.GetNivell(nivellID); err == nil && nivell != nil && nivell.PaisID > 0 {
				target.PaisID = intPtr(nivell.PaisID)
			}
		}
	}
}
